from __future__ import annotations

from datetime import date, datetime, timezone
import json
from pathlib import Path

import duckdb
import pytest

from skypoints_data_engineering.persistence import initialize_schema, open_database


TABLE_NAMES = {
    "pipeline_run",
    "input_artifact",
    "run_input",
    "raw_member_record",
    "member_record_outcome",
    "member_issue",
    "raw_redemption_payload",
    "redemption_transaction_outcome",
    "redemption_issue",
    "run_reconciliation",
}
VIEW_NAMES = {
    "members_aus",
    "members_ind",
    "members_usa",
    "curated_redemptions",
}
FIXED_INGESTED_AT = datetime(2026, 11, 24, 15, 30, tzinfo=timezone.utc)


def _initialize(connection: duckdb.DuckDBPyConnection) -> None:
    initialize_schema(connection)


def _insert_run(connection: duckdb.DuckDBPyConnection, run_id: str = "run-1") -> None:
    connection.execute(
        """
        INSERT INTO pipeline_run
        (run_id, started_at_utc, status, as_of_date)
        VALUES (?, ?, 'running', ?)
        """,
        [run_id, FIXED_INGESTED_AT, date(2026, 11, 24)],
    )


def _insert_artifact_and_input(
    connection: duckdb.DuckDBPyConnection,
    artifact_id: str,
    *,
    run_id: str = "run-1",
    source_kind: str = "member",
    source_identity: str | None = None,
    filename: str | None = None,
) -> None:
    if source_identity is None:
        source_identity = artifact_id.upper()
    if filename is None:
        filename = f"{source_identity}.csv"
    connection.execute(
        """
        INSERT INTO input_artifact
        (artifact_id, source_kind, source_identity, content_sha256, schema_manifest)
        VALUES (?, ?, ?, ?, '{}'::JSON)
        """,
        [artifact_id, source_kind, source_identity, artifact_id.ljust(64, "0")[:64]],
    )
    connection.execute(
        """
        INSERT INTO run_input
        (run_id, artifact_id, original_filename, ingested_at_utc, input_status)
        VALUES (?, ?, ?, ?, 'succeeded')
        """,
        [run_id, artifact_id, filename, FIXED_INGESTED_AT],
    )


def _insert_raw_member(
    connection: duckdb.DuckDBPyConnection,
    artifact_id: str,
    raw_record_id: str,
    *,
    row_number: int,
    source_code: str,
    source_row: dict[str, object] | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO raw_member_record
        (raw_record_id, artifact_id, source_code, sheet_name, sheet_name_key,
         source_row_number, schema_fingerprint, raw_values)
        VALUES (?, ?, ?, NULL, '', ?, ?, ?::JSON)
        """,
        [
            raw_record_id,
            artifact_id,
            source_code,
            row_number,
            "schema-" + artifact_id,
            json.dumps(source_row or {"Name": "Elena", "ID": "1"}),
        ],
    )


def _insert_member_outcome(
    connection: duckdb.DuckDBPyConnection,
    artifact_id: str,
    raw_record_id: str,
    *,
    run_id: str = "run-1",
    member_name: str | None = "Elena",
    source_member_id: str | None = "1",
    disposition: str = "accepted",
    routed_country_code: str | None = None,
) -> None:
    connection.execute(
        """
        INSERT INTO member_record_outcome
        (run_id, artifact_id, raw_record_id, member_name, source_member_id,
         disposition, routed_country_code)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            artifact_id,
            raw_record_id,
            member_name,
            source_member_id,
            disposition,
            routed_country_code,
        ],
    )


def _insert_redemption_parent(
    connection: duckdb.DuckDBPyConnection,
    artifact_id: str,
    payload_index: int,
    *,
    run_id: str = "run-1",
    member_id: str | None = "223457",
    feed_date: str = "20240115",
    children: int = 1,
    disposition: str = "accepted",
) -> None:
    payload = {"member_id": member_id, "feed_date": feed_date, "redemptions": []}
    connection.execute(
        """
        INSERT INTO raw_redemption_payload
        (run_id, artifact_id, payload_index, payload_representation,
         raw_payload_json, parent_member_id, parent_feed_date_raw,
         parsed_feed_date, transaction_count, disposition)
        VALUES (?, ?, ?, 'mapping_json', ?::JSON, ?, ?::JSON, ?, ?, ?)
        """,
        [
            run_id,
            artifact_id,
            payload_index,
            json.dumps(payload),
            member_id,
            json.dumps(feed_date),
            date(2024, 1, 15),
            children,
            disposition,
        ],
    )


def _insert_transaction(
    connection: duckdb.DuckDBPyConnection,
    artifact_id: str,
    payload_index: int,
    transaction_index: int,
    *,
    run_id: str = "run-1",
    transaction_id: str = "RX10091",
    disposition: str = "accepted",
    matched_member: tuple[str, str] | None = None,
) -> None:
    child = {
        "txn_id": transaction_id,
        "txn_date": "20240110",
        "partner": "AeroLink",
        "miles_redeemed": 12000,
        "status": "COMPLETED",
    }
    connection.execute(
        """
        INSERT INTO redemption_transaction_outcome
        (run_id, artifact_id, payload_index, transaction_index, transaction_id,
         transaction_date_raw, parsed_transaction_date, partner,
         miles_redeemed_raw, miles_redeemed, status, raw_transaction,
         disposition, matched_member_run_id, matched_member_raw_record_id)
        VALUES (?, ?, ?, ?, ?, ?::JSON, ?, ?, ?::JSON, ?, ?, ?::JSON, ?, ?, ?)
        """,
        [
            run_id,
            artifact_id,
            payload_index,
            transaction_index,
            transaction_id,
            json.dumps("20240110"),
            date(2024, 1, 10),
            "AeroLink",
            json.dumps(12000),
            12000,
            "COMPLETED",
            json.dumps(child),
            disposition,
            matched_member[0] if matched_member else None,
            matched_member[1] if matched_member else None,
        ],
    )


def test_schema_initializes_expected_relations_and_types() -> None:
    with open_database() as connection:
        initialize_schema(connection)

        relations = connection.execute(
            """
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
        tables = {name for name, kind in relations if kind == "BASE TABLE"}
        views = {name for name, kind in relations if kind == "VIEW"}

        assert tables == TABLE_NAMES
        assert views == VIEW_NAMES

        columns = {
            row[0]: row[1]
            for row in connection.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'member_record_outcome'
                """
            ).fetchall()
        }
        assert columns["member_name"] == "VARCHAR"
        assert columns["source_member_id"] == "VARCHAR"
        assert columns["enrollment_date"] == "DATE"
        assert columns["post_code"] == "INTEGER"
        assert columns["age"] == "INTEGER"
        assert columns["stale_member"] == "BOOLEAN"
        assert columns["disposition"] == "VARCHAR"

        run_columns = {
            row[0]: row[1]
            for row in connection.execute(
                """
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'pipeline_run'
                """
            ).fetchall()
        }
        assert run_columns["started_at_utc"] == "TIMESTAMP WITH TIME ZONE"
        assert run_columns["as_of_date"] == "DATE"


def test_schema_initialization_is_idempotent() -> None:
    with open_database() as connection:
        initialize_schema(connection)
        initialize_schema(connection)
        assert connection.execute("SELECT count(*) FROM pipeline_run").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM information_schema.views WHERE table_name LIKE 'members_%'").fetchone() == (3,)


def test_open_database_closes_only_connections_it_owns(tmp_path: Path) -> None:
    database_path = tmp_path / "foundation.duckdb"
    with open_database(database_path) as owned_connection:
        initialize_schema(owned_connection)
        assert owned_connection.execute("SELECT 1").fetchone() == (1,)

    with pytest.raises(duckdb.ConnectionException):
        owned_connection.execute("SELECT 1")

    external_connection = duckdb.connect(":memory:")
    with open_database(external_connection) as accepted_connection:
        assert accepted_connection is external_connection
    assert external_connection.execute("SELECT 1").fetchone() == (1,)
    external_connection.close()


def test_foreign_keys_and_nullable_composite_member_reference() -> None:
    with open_database() as connection:
        initialize_schema(connection)
        _insert_run(connection)
        with pytest.raises(duckdb.ConstraintException):
            _insert_run(connection)
        _insert_artifact_and_input(connection, "member-aus", source_identity="AUS")
        _insert_artifact_and_input(
            connection,
            "redemption-feed",
            source_kind="redemption",
            source_identity="synthetic-feed",
            filename="fixture.json",
        )
        _insert_raw_member(connection, "member-aus", "member-row-1", row_number=1, source_code="AUS")
        _insert_member_outcome(connection, "member-aus", "member-row-1")
        _insert_redemption_parent(connection, "redemption-feed", 1)

        # Both NULLs are the valid unmatched-reference representation.
        _insert_transaction(connection, "redemption-feed", 1, 1)

        with pytest.raises(duckdb.ConstraintException):
            connection.execute(
                """
                INSERT INTO raw_member_record
                (raw_record_id, artifact_id, source_code, sheet_name_key,
                 source_row_number, schema_fingerprint, raw_values)
                VALUES ('orphan', 'missing-artifact', 'AUS', '', 1, 'schema', '{}'::JSON)
                """
            )

        with pytest.raises(duckdb.ConstraintException):
            _insert_transaction(
                connection,
                "redemption-feed",
                1,
                2,
                matched_member=("missing-run", "missing-member"),
            )

        # A partially populated composite FK is rejected by the paired-null check.
        with pytest.raises(duckdb.ConstraintException):
            connection.execute(
                """
                UPDATE redemption_transaction_outcome
                SET matched_member_run_id = 'run-1'
                WHERE artifact_id = 'redemption-feed' AND transaction_index = 1
                """
            )


def test_duplicate_business_identifiers_are_not_global_database_keys() -> None:
    with open_database() as connection:
        initialize_schema(connection)
        _insert_run(connection)
        for artifact_id, source in (("member-aus", "AUS"), ("member-ind", "IND")):
            _insert_artifact_and_input(
                connection,
                artifact_id,
                source_identity=source,
                filename=f"{source}.csv",
            )

        # Same names and local IDs across artifacts are retained independently.
        for artifact_id, source, row_id in (
            ("member-aus", "AUS", "aus-1"),
            ("member-aus", "AUS", "aus-2"),
            ("member-ind", "IND", "ind-1"),
        ):
            row = 1 if row_id.endswith("1") else 2
            _insert_raw_member(connection, artifact_id, row_id, row_number=row, source_code=source)

        _insert_member_outcome(
            connection, "member-aus", "aus-1", member_name="Duplicate", source_member_id="1",
            disposition="quarantined", routed_country_code="AUS",
        )
        _insert_member_outcome(
            connection, "member-aus", "aus-2", member_name="Duplicate", source_member_id="1",
            disposition="quarantined", routed_country_code="AUS",
        )
        _insert_member_outcome(
            connection, "member-ind", "ind-1", member_name="Duplicate", source_member_id="1",
            disposition="accepted", routed_country_code="IND",
        )
        assert connection.execute(
            "SELECT count(*) FROM member_record_outcome WHERE member_name = 'Duplicate'"
        ).fetchone() == (3,)
        assert connection.execute("SELECT count(*) FROM members_aus").fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM members_ind WHERE member_name = 'Duplicate'"
        ).fetchone() == (1,)

        _insert_artifact_and_input(
            connection,
            "redemption-feed",
            source_kind="redemption",
            source_identity="synthetic-feed",
            filename="fixture.json",
        )
        _insert_redemption_parent(connection, "redemption-feed", 1, children=2)
        _insert_redemption_parent(connection, "redemption-feed", 2, children=1)
        # Same transaction ID within a run is data, not a database uniqueness key.
        _insert_transaction(
            connection, "redemption-feed", 1, 1,
            transaction_id="DUPLICATE", disposition="quarantined",
        )
        _insert_transaction(
            connection, "redemption-feed", 1, 2,
            transaction_id="DUPLICATE", disposition="quarantined",
        )
        _insert_transaction(
            connection, "redemption-feed", 2, 1,
            transaction_id="DUPLICATE", disposition="warning",
        )
        assert connection.execute(
            "SELECT count(*) FROM redemption_transaction_outcome WHERE transaction_id = 'DUPLICATE'"
        ).fetchone() == (3,)


def test_curated_views_filter_quarantined_rows_and_keep_parent_fields() -> None:
    with open_database() as connection:
        initialize_schema(connection)
        _insert_run(connection)
        for artifact_id, source, filename in (
            ("aus", "AUS", "AUS.xlsx"),
            ("ind", "IND", "IND.csv"),
            ("usa", "USA", "USA.csv"),
        ):
            _insert_artifact_and_input(
                connection,
                artifact_id,
                source_identity=source,
                filename=filename,
            )
            _insert_raw_member(connection, artifact_id, f"{source}-1", row_number=1, source_code=source)
            _insert_member_outcome(
                connection,
                artifact_id,
                f"{source}-1",
                disposition="warning" if source == "IND" else "accepted",
                routed_country_code=source,
            )
        _insert_raw_member(connection, "aus", "AUS-2", row_number=2, source_code="AUS")
        _insert_member_outcome(
            connection,
            "aus",
            "AUS-2",
            member_name="Quarantined",
            disposition="quarantined",
            routed_country_code="AUS",
        )

        assert connection.execute("SELECT count(*) FROM members_aus").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM members_ind").fetchone() == (1,)
        assert connection.execute("SELECT count(*) FROM members_usa").fetchone() == (1,)
        assert connection.execute(
            "SELECT count(*) FROM members_aus WHERE member_name = 'Quarantined'"
        ).fetchone() == (0,)

        _insert_artifact_and_input(
            connection,
            "redemption-feed",
            source_kind="redemption",
            source_identity="synthetic-feed",
            filename="fixture.json",
        )
        _insert_redemption_parent(connection, "redemption-feed", 1, children=2)
        _insert_transaction(connection, "redemption-feed", 1, 1, disposition="warning")
        _insert_transaction(connection, "redemption-feed", 1, 2, disposition="quarantined")

        rows = connection.execute(
            "SELECT member_id, feed_date, txn_id, partner FROM curated_redemptions"
        ).fetchall()
        assert rows == [("223457", '"20240115"', "RX10091", "AeroLink")]


def test_failed_view_creation_rolls_back_schema_initialization() -> None:
    with open_database() as connection:
        connection.execute("CREATE TABLE members_usa (sentinel INTEGER)")

        with pytest.raises(duckdb.CatalogException):
            initialize_schema(connection)

        existing = connection.execute("SELECT * FROM members_usa").fetchall()
        assert existing == []
        tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main' AND table_type = 'BASE TABLE'
                """
            ).fetchall()
        }
        assert tables == {"members_usa"}
