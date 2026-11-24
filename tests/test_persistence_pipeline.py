from datetime import date, datetime, timezone
from dataclasses import replace
import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from skypoints_data_engineering.ingestion import ingest_active_sources
from skypoints_data_engineering.persistence import (
    PersistenceIntegrityError,
    _redemption_content_hash,
    create_pipeline_run,
    finish_pipeline_run,
    initialize_schema,
    open_database,
    persist_member_results,
    persist_redemption_results,
    register_input_artifact,
)
from skypoints_data_engineering.redemptions import process_redemption_payloads
from skypoints_data_engineering.transforms import TransformationIssue, transform_batch
from skypoints_data_engineering.validation import validate_batch


ROOT = Path(__file__).resolve().parents[1]
RAW_DATA = ROOT / "data" / "raw"
ASSESSMENT_FIXTURE = ROOT / "tests" / "fixtures" / "redemptions_assessment_example.json"
AS_OF = date(2026, 11, 24)
INGESTED = datetime(2026, 11, 24, 15, 30, tzinfo=timezone.utc)


def _setup_run(connection: duckdb.DuckDBPyConnection, run_id: str = "run-1") -> None:
    create_pipeline_run(connection, run_id, as_of_date=AS_OF, started_at_utc=INGESTED)


def _member_result(source_file: str):
    batch = next(
        batch for batch in ingest_active_sources(RAW_DATA, ingested_at_utc=INGESTED)
        if batch.source_file == source_file
    )
    validation = validate_batch(batch, as_of_date=AS_OF)
    transformation = transform_batch(validation, as_of_date=AS_OF)
    return batch, validation, transformation


def _seed_synthetic_member_reference(connection: duckdb.DuckDBPyConnection) -> tuple[str, str]:
    """Create an explicitly synthetic profile; never derived from data/raw/ records."""
    content = b"synthetic-member-fixture-v1"
    artifact_id = register_input_artifact(
        connection, "run-1", source_kind="member", source_identity="SYNTHETIC_TEST_ONLY",
        content_sha256=hashlib.sha256(content).hexdigest(),
        schema_manifest={"fixture": "synthetic-only", "source": "not data/raw"},
        original_filename="synthetic_member_fixture.json", ingested_at_utc=INGESTED,
    )
    raw_id = "synthetic-member-record-test-only"
    connection.execute(
        """INSERT INTO raw_member_record
           (raw_record_id, artifact_id, source_code, sheet_name, sheet_name_key,
            source_row_number, schema_fingerprint, raw_values)
           VALUES (?, ?, 'SYNTHETIC_TEST_ONLY', NULL, '', 1, 'synthetic-v1',
                   '{\"fixture\":\"synthetic-only\"}'::JSON)""",
        [raw_id, artifact_id],
    )
    connection.execute(
        """INSERT INTO member_record_outcome
           (run_id, artifact_id, raw_record_id, member_name, source_member_id, disposition)
           VALUES ('run-1', ?, ?, 'Synthetic Test Member', 'SYNTH-MEMBER-001', 'accepted')""",
        [artifact_id, raw_id],
    )
    return "run-1", raw_id


def _redemption_result(payloads: object, *, mapping: dict[str, str] | None = None):
    return process_redemption_payloads(
        payloads,
        source_name="synthetic-redemption.json",
        batch_id="fixture-batch",
        member_key_mapping=mapping,
    )


def test_redemption_hash_is_order_independent_stable_and_nested_type_safe() -> None:
    first = _redemption_result({"member_id": "M1", "feed_date": "bad", "extra": {"a": 1, "b": 2}})
    reordered = _redemption_result({"extra": {"b": 2, "a": 1}, "feed_date": "bad", "member_id": "M1"})
    assert _redemption_content_hash(first) == _redemption_content_hash(reordered)
    assert _redemption_content_hash(first) == _redemption_content_hash(first)

    as_datetime = _redemption_result({"member_id": "M1", "feed_date": datetime(2024, 1, 15)})
    as_datetime_again = _redemption_result({"member_id": "M1", "feed_date": datetime(2024, 1, 15)})
    as_datetime_string = _redemption_result({"member_id": "M1", "feed_date": "2024-01-15T00:00:00"})
    as_date = _redemption_result({"member_id": "M1", "feed_date": date(2024, 1, 15)})
    as_date_string = _redemption_result({"member_id": "M1", "feed_date": "2024-01-15"})
    assert _redemption_content_hash(as_datetime) == _redemption_content_hash(as_datetime_again)
    assert _redemption_content_hash(as_datetime) != _redemption_content_hash(as_datetime_string)
    assert _redemption_content_hash(as_date) != _redemption_content_hash(as_date_string)


def test_redemption_hash_distinguishes_text_from_bytes_and_rejects_unsupported_values() -> None:
    text = _redemption_result('{"member_id":"M1"}')
    raw_bytes = _redemption_result(b'{"member_id":"M1"}')
    assert _redemption_content_hash(text) != _redemption_content_hash(raw_bytes)

    unsupported = _redemption_result({"member_id": object()})
    with pytest.raises(ValueError, match="Unsupported redemption payload value type: object"):
        _redemption_content_hash(unsupported)


def test_member_pipeline_persists_raw_outcomes_issues_and_reconciliation() -> None:
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        batch, validation, transformation = _member_result("AUS.xlsx")

        artifact_id = persist_member_results(
            connection, "run-1", batch, validation, transformation,
        )

        assert connection.execute(
            "SELECT count(*) FROM raw_member_record WHERE artifact_id = ?", [artifact_id]
        ).fetchone() == (3,)
        assert connection.execute(
            "SELECT count(*) FROM member_record_outcome WHERE run_id = 'run-1' AND artifact_id = ?",
            [artifact_id],
        ).fetchone() == (3,)
        assert connection.execute(
            "SELECT count(*) FROM member_issue WHERE run_id = 'run-1' AND raw_record_id IN "
            "(SELECT raw_record_id FROM member_record_outcome WHERE artifact_id = ?)",
            [artifact_id],
        ).fetchone()[0] >= len(validation.issues)
        manifest = connection.execute(
            "SELECT schema_manifest::VARCHAR FROM input_artifact WHERE artifact_id = ?",
            [artifact_id],
        ).fetchone()[0]
        assert "Date of Birth" in manifest
        assert connection.execute(
            """SELECT count(*) FROM raw_member_record
               WHERE artifact_id = ?
                 AND json_extract_string(raw_values, '$.\"Date of Enrollment\"') = '2021-13-13'""",
            [artifact_id],
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT disposition FROM member_record_outcome WHERE run_id = 'run-1' AND artifact_id = ? AND member_name = 'Mike'",
            [artifact_id],
        ).fetchone() == ("warning",)
        assert connection.execute(
            "SELECT count(*) FROM members_aus WHERE run_id = 'run-1' AND artifact_id = ?",
            [artifact_id],
        ).fetchone() == (2,)
        assert connection.execute(
            """SELECT read_count, accepted_count, warned_count, quarantined_count,
                      transformed_count, raw_persisted_count, curated_persisted_count
               FROM run_reconciliation WHERE run_id = 'run-1' AND artifact_id = ?
                 AND scope = 'member_record'""",
            [artifact_id],
        ).fetchone() == (3, 1, 1, 1, 2, 3, 2)


def test_multiple_artifacts_reprocess_and_retry_preserve_history_without_duplicates() -> None:
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        batches = [
            _member_result("AUS.xlsx"),
            _member_result("IND.csv"),
            _member_result("USA.csv"),
        ]
        artifact_ids = [
            persist_member_results(connection, "run-1", batch, validation, transformed)
            for batch, validation, transformed in batches
        ]
        finish_pipeline_run(connection, "run-1", status="succeeded", finished_at_utc=INGESTED)
        persist_member_results(connection, "run-1", *batches[0])

        create_pipeline_run(
            connection, "run-2", as_of_date=AS_OF, started_at_utc=INGESTED,
            reprocess_of_run_id="run-1",
        )
        persist_member_results(connection, "run-2", *batches[0])

        assert connection.execute(
            "SELECT count(*) FROM raw_member_record WHERE artifact_id = ?", [artifact_ids[0]]
        ).fetchone() == (3,)
        assert connection.execute(
            "SELECT count(*) FROM member_record_outcome WHERE artifact_id = ?", [artifact_ids[0]]
        ).fetchone() == (6,)
        assert connection.execute(
            "SELECT count(*) FROM run_input WHERE artifact_id = ?", [artifact_ids[0]]
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT count(*) FROM run_reconciliation WHERE artifact_id = ? AND scope = 'member_record'",
            [artifact_ids[0]],
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT count(*) FROM members_ind WHERE run_id = 'run-1' AND artifact_id = ?",
            [artifact_ids[1]],
        ).fetchone() == (len(batches[1][2].curated_records),)
        assert connection.execute(
            "SELECT count(*) FROM members_usa WHERE run_id = 'run-1' AND artifact_id = ?",
            [artifact_ids[2]],
        ).fetchone() == (len(batches[2][2].curated_records),)


@pytest.mark.parametrize("deleted_relation", ["member_record_outcome", "member_issue", "run_reconciliation"])
def test_succeeded_member_retry_rejects_missing_persisted_state(deleted_relation: str) -> None:
    batch, validation, transformation = _member_result("AUS.xlsx")
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        artifact_id = persist_member_results(connection, "run-1", batch, validation, transformation)
        if deleted_relation == "member_record_outcome":
            connection.execute(
                "DELETE FROM member_issue WHERE run_id = 'run-1' AND raw_record_id IN (SELECT raw_record_id FROM raw_member_record WHERE artifact_id = ?)",
                [artifact_id],
            )
        connection.execute(
            f"DELETE FROM {deleted_relation} WHERE run_id = 'run-1' AND artifact_id = ?" if deleted_relation != "member_issue"
            else "DELETE FROM member_issue WHERE run_id = 'run-1' AND raw_record_id IN (SELECT raw_record_id FROM raw_member_record WHERE artifact_id = ?)",
            [artifact_id],
        )
        with pytest.raises(PersistenceIntegrityError, match="Succeeded member input|Succeeded input"):
            persist_member_results(connection, "run-1", batch, validation, transformation)


def test_succeeded_redemption_retry_rejects_missing_outcome_and_reconciliation() -> None:
    result = _redemption_result(ASSESSMENT_FIXTURE.read_text(encoding="utf-8"))
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        artifact_id = persist_redemption_results(
            connection, "run-1", result, ingested_at_utc=INGESTED,
        )
        connection.execute("DELETE FROM redemption_issue WHERE artifact_id = ?", [artifact_id])
        connection.execute(
            "DELETE FROM redemption_transaction_outcome WHERE artifact_id = ? AND transaction_index = 1",
            [artifact_id],
        )
        with pytest.raises(PersistenceIntegrityError, match="Succeeded redemption input"):
            persist_redemption_results(
                connection, "run-1", result, ingested_at_utc=INGESTED,
            )


def test_routing_findings_are_persisted_with_routing_category() -> None:
    batch, validation, transformation = _member_result("IND.csv")
    issue = TransformationIssue(
        validation.outcomes[0].record, "COUNTRY_ROUTE_UNMAPPED_SOURCE",
        "Synthetic routing finding for persistence category coverage.",
    )
    transformation = replace(transformation, issues=(issue,))
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        persist_member_results(connection, "run-1", batch, validation, transformation)
        assert connection.execute(
            "SELECT issue_category FROM member_issue WHERE rule_code = 'COUNTRY_ROUTE_UNMAPPED_SOURCE'"
        ).fetchone() == ("routing",)


def test_redemption_pipeline_retains_malformed_parents_children_and_unmatched_rows() -> None:
    payloads = [
        "{",  # Original malformed text is retained as the parent payload.
        {
            "member_id": "NO-MATCH",
            "feed_date": "20240115",
            "redemptions": [
                {"txn_id": "BAD", "txn_date": "20240230", "miles_redeemed": "many"},
                "malformed-child",
            ],
        },
    ]
    result = _redemption_result(payloads)
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        artifact_id = persist_redemption_results(
            connection, "run-1", result, ingested_at_utc=INGESTED,
        )

        assert connection.execute(
            "SELECT count(*) FROM raw_redemption_payload WHERE run_id = 'run-1' AND artifact_id = ?",
            [artifact_id],
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT raw_payload_text FROM raw_redemption_payload WHERE artifact_id = ? AND payload_index = 1",
            [artifact_id],
        ).fetchone() == ("{",)
        assert connection.execute(
            "SELECT count(*) FROM redemption_transaction_outcome WHERE artifact_id = ?",
            [artifact_id],
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT count(*) FROM redemption_issue WHERE artifact_id = ? AND issue_category = 'parent'",
            [artifact_id],
        ).fetchone()[0] >= 1
        assert connection.execute(
            "SELECT count(*) FROM redemption_issue WHERE artifact_id = ? AND issue_category = 'transaction'",
            [artifact_id],
        ).fetchone()[0] >= 3
        assert connection.execute(
            "SELECT count(*) FROM curated_redemptions WHERE artifact_id = ?",
            [artifact_id],
        ).fetchone() == (0,)
        assert connection.execute(
            """SELECT read_count, quarantined_count, raw_persisted_count
               FROM run_reconciliation WHERE artifact_id = ? AND scope = 'redemption_parent'""",
            [artifact_id],
        ).fetchone() == (2, 1, 2)
        assert connection.execute(
            """SELECT read_count, warned_count, quarantined_count, transformed_count,
                      unmatched_count, raw_persisted_count, curated_persisted_count
               FROM run_reconciliation WHERE artifact_id = ? AND scope = 'redemption_transaction'""",
            [artifact_id],
        ).fetchone() == (2, 0, 2, 2, 2, 2, 0)


def test_assessment_redemption_fixture_persists_two_queryable_rows() -> None:
    raw_text = ASSESSMENT_FIXTURE.read_text(encoding="utf-8")
    result = _redemption_result(raw_text)
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        artifact_id = persist_redemption_results(
            connection, "run-1", result, ingested_at_utc=INGESTED,
        )
        rows = connection.execute(
            """SELECT member_id, feed_date, txn_id, txn_date, partner, miles_redeemed, status
               FROM curated_redemptions WHERE artifact_id = ? ORDER BY transaction_index""",
            [artifact_id],
        ).fetchall()
        assert rows == [
            ("223457", '"20240115"', "RX10091", '"20240110"', "AeroLink", 12000, "COMPLETED"),
            ("223457", '"20240115"', "RX10092", '"20240113"', "SkyPoints", 5000, "PENDING"),
        ]
        assert connection.execute(
            "SELECT count(*) FROM redemption_issue WHERE artifact_id = ? AND rule_code = 'MEMBER_REFERENCE_UNMATCHED'",
            [artifact_id],
        ).fetchone() == (2,)
        assert connection.execute(
            """SELECT read_count, warned_count, quarantined_count, transformed_count,
                      unmatched_count, raw_persisted_count, curated_persisted_count
               FROM run_reconciliation WHERE run_id = 'run-1' AND artifact_id = ?
                 AND scope = 'redemption_transaction'""",
            [artifact_id],
        ).fetchone() == (2, 2, 0, 2, 2, 2, 2)
        assert connection.execute(
            "SELECT input_status FROM run_input WHERE run_id = 'run-1' AND artifact_id = ?",
            [artifact_id],
        ).fetchone() == ("succeeded",)


def test_redemption_retry_and_new_run_are_idempotent_and_keep_both_histories() -> None:
    raw_text = ASSESSMENT_FIXTURE.read_text(encoding="utf-8")
    result = _redemption_result(raw_text)
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        artifact_id = persist_redemption_results(
            connection, "run-1", result, ingested_at_utc=INGESTED,
        )
        persist_redemption_results(
            connection, "run-1", result, ingested_at_utc=INGESTED,
        )
        create_pipeline_run(
            connection, "run-2", as_of_date=AS_OF, started_at_utc=INGESTED,
            reprocess_of_run_id="run-1",
        )
        persist_redemption_results(
            connection, "run-2", result, ingested_at_utc=INGESTED,
        )

        assert connection.execute(
            "SELECT count(*) FROM raw_redemption_payload WHERE artifact_id = ?", [artifact_id]
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT count(*) FROM redemption_transaction_outcome WHERE artifact_id = ?",
            [artifact_id],
        ).fetchone() == (4,)
        assert connection.execute(
            "SELECT count(*) FROM redemption_issue WHERE artifact_id = ?",
            [artifact_id],
        ).fetchone() == (4,)
        assert connection.execute(
            "SELECT count(*) FROM run_reconciliation WHERE artifact_id = ?",
            [artifact_id],
        ).fetchone() == (4,)
        assert connection.execute(
            "SELECT count(*) FROM run_input WHERE artifact_id = ?", [artifact_id]
        ).fetchone() == (2,)


def test_confirmed_member_reference_requires_explicit_nonquarantined_locator() -> None:
    fixture = json.loads(ASSESSMENT_FIXTURE.read_text(encoding="utf-8"))
    result = _redemption_result(fixture, mapping={"223457": "confirmed-profile-key"})
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        member_reference = _seed_synthetic_member_reference(connection)
        artifact_id = persist_redemption_results(
            connection, "run-1", result, ingested_at_utc=INGESTED,
            confirmed_member_references={"confirmed-profile-key": member_reference},
        )
        assert connection.execute(
            "SELECT DISTINCT matched_member_run_id, matched_member_raw_record_id "
            "FROM redemption_transaction_outcome WHERE artifact_id = ?",
            [artifact_id],
        ).fetchall() == [member_reference]
        assert connection.execute(
            """SELECT a.source_identity, r.original_filename, o.source_member_id
               FROM member_record_outcome o
               JOIN raw_member_record rr USING (artifact_id, raw_record_id)
               JOIN input_artifact a USING (artifact_id)
               JOIN run_input r ON r.artifact_id = a.artifact_id AND r.run_id = o.run_id
               WHERE o.run_id = ? AND o.raw_record_id = ?""",
            list(member_reference),
        ).fetchone() == ("SYNTHETIC_TEST_ONLY", "synthetic_member_fixture.json", "SYNTH-MEMBER-001")
        assert connection.execute(
            "SELECT count(*) FROM redemption_issue WHERE artifact_id = ? AND rule_code = 'MEMBER_REFERENCE_UNMATCHED'",
            [artifact_id],
        ).fetchone() == (0,)


def test_failed_persistence_rolls_back_data_and_records_failed_input(monkeypatch) -> None:
    batch, validation, transformation = _member_result("IND.csv")
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)

        def fail_reconciliation(*_args, **_kwargs):
            raise RuntimeError("injected persistence failure")

        monkeypatch.setattr(
            "skypoints_data_engineering.persistence._write_member_reconciliation",
            fail_reconciliation,
        )
        with pytest.raises(RuntimeError, match="injected persistence failure"):
            persist_member_results(connection, "run-1", batch, validation, transformation)

        assert connection.execute("SELECT count(*) FROM raw_member_record").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM member_record_outcome").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM member_issue").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM run_reconciliation").fetchone() == (0,)
        assert connection.execute(
            "SELECT input_status, failure_detail FROM run_input"
        ).fetchone()[0] == "failed"
        monkeypatch.undo()
        persisted_id = persist_member_results(connection, "run-1", batch, validation, transformation)
        assert connection.execute(
            "SELECT input_status FROM run_input WHERE artifact_id = ?", [persisted_id]
        ).fetchone() == ("succeeded",)


def test_failed_redemption_persistence_rolls_back_parent_child_and_issue_rows(monkeypatch) -> None:
    import skypoints_data_engineering.persistence as persistence

    result = _redemption_result(ASSESSMENT_FIXTURE.read_text(encoding="utf-8"))
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)

        def fail_issue(*_args, **_kwargs):
            raise RuntimeError("injected redemption persistence failure")

        monkeypatch.setattr(persistence, "_insert_redemption_issue", fail_issue)
        with pytest.raises(RuntimeError, match="injected redemption persistence failure"):
            persist_redemption_results(
                connection, "run-1", result, ingested_at_utc=INGESTED,
            )

        assert connection.execute("SELECT count(*) FROM raw_redemption_payload").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM redemption_transaction_outcome").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM redemption_issue").fetchone() == (0,)
        assert connection.execute("SELECT count(*) FROM run_reconciliation").fetchone() == (0,)
        assert connection.execute("SELECT input_status FROM run_input").fetchone() == ("failed",)


def test_run_completion_and_reprocessing_metadata_are_explicit() -> None:
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        finish_pipeline_run(connection, "run-1", status="succeeded", finished_at_utc=INGESTED)
        create_pipeline_run(
            connection, "run-2", as_of_date=AS_OF, started_at_utc=INGESTED,
            reprocess_of_run_id="run-1",
        )
        assert connection.execute(
            "SELECT status, finished_at_utc IS NOT NULL FROM pipeline_run WHERE run_id = 'run-1'"
        ).fetchone() == ("succeeded", True)
        assert connection.execute(
            "SELECT reprocess_of_run_id FROM pipeline_run WHERE run_id = 'run-2'"
        ).fetchone() == ("run-1",)


def test_artifact_registration_separates_content_identity_from_run_identity() -> None:
    with open_database() as connection:
        initialize_schema(connection)
        _setup_run(connection)
        artifact_id = register_input_artifact(
            connection,
            "run-1",
            source_kind="redemption",
            source_identity="fixture-feed",
            content_sha256="A" * 64,
            schema_manifest={"columns": ["member_id", "redemptions"]},
            original_filename="fixture.json",
            ingested_at_utc=INGESTED,
        )
        repeated_id = register_input_artifact(
            connection,
            "run-1",
            source_kind="redemption",
            source_identity="fixture-feed",
            content_sha256="a" * 64,
            schema_manifest={"columns": ["member_id", "redemptions"]},
            original_filename="fixture.json",
            ingested_at_utc=datetime(2026, 11, 25, tzinfo=timezone.utc),
        )
        other_content_id = register_input_artifact(
            connection,
            "run-1",
            source_kind="redemption",
            source_identity="fixture-feed",
            content_sha256="b" * 64,
            schema_manifest={"columns": ["member_id", "redemptions"]},
            original_filename="fixture-2.json",
            ingested_at_utc=INGESTED,
        )

        assert artifact_id == repeated_id
        assert artifact_id != other_content_id
        assert connection.execute(
            "SELECT input_status, ingested_at_utc FROM run_input WHERE run_id = 'run-1' AND artifact_id = ?",
            [artifact_id],
        ).fetchone() == ("pending", INGESTED)
        assert connection.execute(
            "SELECT count(*) FROM input_artifact WHERE source_identity = 'fixture-feed'"
        ).fetchone() == (2,)
        with pytest.raises(ValueError, match="inputs are incomplete"):
            finish_pipeline_run(connection, "run-1", status="succeeded", finished_at_utc=INGESTED)
