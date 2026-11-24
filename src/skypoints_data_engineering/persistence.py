"""DuckDB schema initialization and connection lifecycle helpers.

This module owns only DuckDB-specific connection and schema concerns. Pipeline
ingestion, validation, transformation, and redemption functions remain in-memory
and persistence-agnostic.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Iterator, Mapping
from uuid import NAMESPACE_URL, uuid5

import duckdb

from .ingestion import IngestionBatch, RawRecord
from .redemptions import RedemptionDisposition, RedemptionResult
from .source_dates import parse_source_date
from .transforms import TransformationResult
from .validation import Disposition, ValidationResult


class PersistenceIntegrityError(RuntimeError):
    """Raised when a succeeded input no longer matches its persisted evidence."""


_SCHEMA_DDL = (
    """
    CREATE TABLE IF NOT EXISTS pipeline_run (
        run_id VARCHAR PRIMARY KEY,
        started_at_utc TIMESTAMPTZ NOT NULL,
        finished_at_utc TIMESTAMPTZ,
        status VARCHAR NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
        as_of_date DATE NOT NULL,
        reprocess_of_run_id VARCHAR,
        FOREIGN KEY (reprocess_of_run_id) REFERENCES pipeline_run(run_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS input_artifact (
        artifact_id VARCHAR PRIMARY KEY,
        source_kind VARCHAR NOT NULL CHECK (source_kind IN ('member', 'redemption')),
        source_identity VARCHAR NOT NULL,
        content_sha256 VARCHAR NOT NULL CHECK (length(content_sha256) = 64),
        schema_manifest JSON NOT NULL,
        UNIQUE (source_kind, source_identity, content_sha256)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_input (
        run_id VARCHAR NOT NULL,
        artifact_id VARCHAR NOT NULL,
        original_filename VARCHAR NOT NULL,
        feed_generated_at_utc TIMESTAMPTZ,
        ingested_at_utc TIMESTAMPTZ NOT NULL,
        input_status VARCHAR NOT NULL
            CHECK (input_status IN ('pending', 'running', 'succeeded', 'failed')),
        failure_detail VARCHAR,
        PRIMARY KEY (run_id, artifact_id),
        FOREIGN KEY (run_id) REFERENCES pipeline_run(run_id),
        FOREIGN KEY (artifact_id) REFERENCES input_artifact(artifact_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_member_record (
        raw_record_id VARCHAR PRIMARY KEY,
        artifact_id VARCHAR NOT NULL,
        source_code VARCHAR NOT NULL,
        sheet_name VARCHAR,
        sheet_name_key VARCHAR NOT NULL,
        source_row_number BIGINT NOT NULL CHECK (source_row_number >= 1),
        schema_fingerprint VARCHAR NOT NULL,
        raw_values JSON NOT NULL,
        UNIQUE (artifact_id, raw_record_id),
        UNIQUE (artifact_id, source_code, sheet_name_key, source_row_number),
        FOREIGN KEY (artifact_id) REFERENCES input_artifact(artifact_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS member_record_outcome (
        run_id VARCHAR NOT NULL,
        artifact_id VARCHAR NOT NULL,
        raw_record_id VARCHAR NOT NULL,
        member_name VARCHAR,
        source_member_id VARCHAR,
        enrollment_date DATE,
        last_flight_date DATE,
        tier_code VARCHAR,
        agent_name VARCHAR,
        state VARCHAR,
        country VARCHAR,
        post_code INTEGER,
        dob DATE,
        active_member VARCHAR,
        age INTEGER,
        stale_member BOOLEAN,
        routed_country_code VARCHAR,
        routing_method VARCHAR,
        disposition VARCHAR NOT NULL
            CHECK (disposition IN ('accepted', 'warning', 'quarantined')),
        PRIMARY KEY (run_id, raw_record_id),
        FOREIGN KEY (run_id, artifact_id) REFERENCES run_input(run_id, artifact_id),
        FOREIGN KEY (artifact_id, raw_record_id)
            REFERENCES raw_member_record(artifact_id, raw_record_id),
        CHECK (member_name IS NULL OR length(member_name) <= 255),
        CHECK (source_member_id IS NULL OR length(source_member_id) <= 18),
        CHECK (tier_code IS NULL OR length(tier_code) <= 5),
        CHECK (agent_name IS NULL OR length(agent_name) <= 255),
        CHECK (state IS NULL OR length(state) <= 5),
        CHECK (country IS NULL OR length(country) <= 5),
        CHECK (post_code IS NULL OR post_code BETWEEN -99999 AND 99999),
        CHECK (active_member IS NULL OR length(active_member) <= 1)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS member_issue (
        run_id VARCHAR NOT NULL,
        raw_record_id VARCHAR NOT NULL,
        issue_ordinal INTEGER NOT NULL CHECK (issue_ordinal >= 1),
        issue_category VARCHAR NOT NULL
            CHECK (issue_category IN ('validation', 'transformation', 'routing')),
        rule_code VARCHAR NOT NULL,
        severity VARCHAR NOT NULL CHECK (severity IN ('warning', 'error')),
        field_name VARCHAR,
        detail VARCHAR NOT NULL,
        PRIMARY KEY (run_id, raw_record_id, issue_ordinal),
        FOREIGN KEY (run_id, raw_record_id)
            REFERENCES member_record_outcome(run_id, raw_record_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS raw_redemption_payload (
        run_id VARCHAR NOT NULL,
        artifact_id VARCHAR NOT NULL,
        payload_index BIGINT NOT NULL CHECK (payload_index >= 1),
        payload_representation VARCHAR NOT NULL
            CHECK (payload_representation IN ('text', 'bytes_hex', 'mapping_json')),
        raw_payload_text VARCHAR,
        raw_payload_hex VARCHAR,
        raw_payload_json JSON,
        parent_member_id VARCHAR,
        parent_feed_date_raw JSON,
        parsed_feed_date DATE,
        transaction_count BIGINT NOT NULL CHECK (transaction_count >= 0),
        disposition VARCHAR NOT NULL
            CHECK (disposition IN ('accepted', 'warning', 'quarantined')),
        PRIMARY KEY (run_id, artifact_id, payload_index),
        FOREIGN KEY (run_id, artifact_id) REFERENCES run_input(run_id, artifact_id),
        CHECK (
            (payload_representation = 'text'
                AND raw_payload_text IS NOT NULL
                AND raw_payload_hex IS NULL
                AND raw_payload_json IS NULL)
            OR
            (payload_representation = 'bytes_hex'
                AND raw_payload_text IS NULL
                AND raw_payload_hex IS NOT NULL
                AND raw_payload_json IS NULL)
            OR
            (payload_representation = 'mapping_json'
                AND raw_payload_text IS NULL
                AND raw_payload_hex IS NULL
                AND raw_payload_json IS NOT NULL)
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS redemption_transaction_outcome (
        run_id VARCHAR NOT NULL,
        artifact_id VARCHAR NOT NULL,
        payload_index BIGINT NOT NULL,
        transaction_index BIGINT NOT NULL CHECK (transaction_index >= 1),
        transaction_id VARCHAR,
        transaction_date_raw JSON,
        parsed_transaction_date DATE,
        partner VARCHAR,
        miles_redeemed_raw JSON,
        miles_redeemed DECIMAL(38, 0),
        status VARCHAR,
        raw_transaction JSON NOT NULL,
        disposition VARCHAR NOT NULL
            CHECK (disposition IN ('accepted', 'warning', 'quarantined')),
        matched_member_run_id VARCHAR,
        matched_member_raw_record_id VARCHAR,
        PRIMARY KEY (run_id, artifact_id, payload_index, transaction_index),
        FOREIGN KEY (run_id, artifact_id, payload_index)
            REFERENCES raw_redemption_payload(run_id, artifact_id, payload_index),
        FOREIGN KEY (matched_member_run_id, matched_member_raw_record_id)
            REFERENCES member_record_outcome(run_id, raw_record_id),
        CHECK (
            (matched_member_run_id IS NULL AND matched_member_raw_record_id IS NULL)
            OR
            (matched_member_run_id IS NOT NULL AND matched_member_raw_record_id IS NOT NULL)
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS redemption_issue (
        issue_id UUID PRIMARY KEY,
        run_id VARCHAR NOT NULL,
        artifact_id VARCHAR NOT NULL,
        payload_index BIGINT NOT NULL,
        transaction_index BIGINT CHECK (transaction_index >= 1),
        issue_category VARCHAR NOT NULL CHECK (issue_category IN ('parent', 'transaction')),
        rule_code VARCHAR NOT NULL,
        severity VARCHAR NOT NULL CHECK (severity IN ('warning', 'error')),
        field_name VARCHAR,
        detail VARCHAR NOT NULL,
        FOREIGN KEY (run_id, artifact_id, payload_index)
            REFERENCES raw_redemption_payload(run_id, artifact_id, payload_index),
        FOREIGN KEY (run_id, artifact_id, payload_index, transaction_index)
            REFERENCES redemption_transaction_outcome(
                run_id, artifact_id, payload_index, transaction_index
            ),
        CHECK (
            (issue_category = 'parent' AND transaction_index IS NULL)
            OR
            (issue_category = 'transaction' AND transaction_index IS NOT NULL)
        )
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_reconciliation (
        run_id VARCHAR NOT NULL,
        artifact_id VARCHAR NOT NULL,
        scope VARCHAR NOT NULL
            CHECK (scope IN ('member_record', 'redemption_parent', 'redemption_transaction')),
        read_count BIGINT NOT NULL CHECK (read_count >= 0),
        accepted_count BIGINT NOT NULL CHECK (accepted_count >= 0),
        warned_count BIGINT NOT NULL CHECK (warned_count >= 0),
        quarantined_count BIGINT NOT NULL CHECK (quarantined_count >= 0),
        transformed_count BIGINT NOT NULL CHECK (transformed_count >= 0),
        unmatched_count BIGINT NOT NULL CHECK (unmatched_count >= 0),
        raw_persisted_count BIGINT NOT NULL CHECK (raw_persisted_count >= 0),
        curated_persisted_count BIGINT NOT NULL CHECK (curated_persisted_count >= 0),
        PRIMARY KEY (run_id, artifact_id, scope),
        FOREIGN KEY (run_id, artifact_id) REFERENCES run_input(run_id, artifact_id),
        CHECK (accepted_count + warned_count + quarantined_count = read_count),
        CHECK (transformed_count <= read_count),
        CHECK (unmatched_count <= read_count),
        CHECK (curated_persisted_count <= read_count),
        CHECK (scope <> 'redemption_parent' OR curated_persisted_count = 0),
        CHECK (scope <> 'redemption_parent' OR transformed_count = 0),
        CHECK (scope = 'redemption_transaction' OR unmatched_count = 0)
    )
    """,
)


_VIEW_DDL = (
    """
    CREATE OR REPLACE VIEW members_aus AS
    SELECT * FROM member_record_outcome
    WHERE disposition <> 'quarantined' AND routed_country_code = 'AUS'
    """,
    """
    CREATE OR REPLACE VIEW members_ind AS
    SELECT * FROM member_record_outcome
    WHERE disposition <> 'quarantined' AND routed_country_code = 'IND'
    """,
    """
    CREATE OR REPLACE VIEW members_usa AS
    SELECT * FROM member_record_outcome
    WHERE disposition <> 'quarantined' AND routed_country_code = 'USA'
    """,
    """
    CREATE OR REPLACE VIEW curated_redemptions AS
    SELECT
        t.run_id,
        t.artifact_id,
        t.payload_index,
        t.transaction_index,
        p.parent_member_id AS member_id,
        p.parent_feed_date_raw AS feed_date,
        p.parsed_feed_date,
        t.transaction_id AS txn_id,
        t.transaction_date_raw AS txn_date,
        t.parsed_transaction_date,
        t.partner,
        t.miles_redeemed_raw,
        t.miles_redeemed,
        t.status,
        t.matched_member_run_id,
        t.matched_member_raw_record_id
    FROM redemption_transaction_outcome AS t
    JOIN raw_redemption_payload AS p
      ON p.run_id = t.run_id
     AND p.artifact_id = t.artifact_id
     AND p.payload_index = t.payload_index
    WHERE t.disposition <> 'quarantined'
    """,
)


@contextmanager
def open_database(
    database: str | Path | duckdb.DuckDBPyConnection = ":memory:",
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Yield a DuckDB connection and close it only when opened here.

    ``database`` may be a database path (or ``":memory:"``), in which case
    this context manager owns and closes the connection, or an existing
    ``DuckDBPyConnection``, whose lifecycle remains with its caller.
    """

    if isinstance(database, duckdb.DuckDBPyConnection):
        yield database
        return

    connection = duckdb.connect(str(database))
    try:
        yield connection
    finally:
        connection.close()


def initialize_schema(connection: duckdb.DuckDBPyConnection) -> None:
    """Create the approved relations and curated views transactionally.

    Repeated calls are safe for a database initialized by this version. The
    initializer creates missing relations, leaves existing tables unchanged,
    and replaces the four views with their approved definitions. It does not
    perform schema migrations.
    """

    connection.execute("BEGIN TRANSACTION")
    try:
        for statement in _SCHEMA_DDL:
            connection.execute(statement)
        for statement in _VIEW_DDL:
            connection.execute(statement)
        connection.execute("COMMIT")
    except BaseException as exc:
        try:
            connection.execute("ROLLBACK")
        except duckdb.Error as rollback_error:
            raise rollback_error from exc
        raise


_MEMBER_SOURCE_FIELDS: Mapping[str, Mapping[str, str | None]] = {
    "AUS": {
        "member_name": "Member Name", "source_member_id": "Unique ID",
        "enrollment_date": "Date of Enrollment", "last_flight_date": "Date of Flight",
        "tier_code": "Tier Type", "agent_name": None, "state": None,
        "country": None, "post_code": None, "dob": "Date of Birth",
        "active_member": None, "date_format": "iso",
    },
    "IND": {
        "member_name": "Name", "source_member_id": "ID",
        "enrollment_date": "EnrollmentDate", "last_flight_date": "Flight Date",
        "tier_code": "TierCode", "agent_name": None, "state": None,
        "country": None, "post_code": None, "dob": "DOB",
        "active_member": None, "date_format": "slash",
    },
    "USA": {
        "member_name": "Name", "source_member_id": "ID",
        "enrollment_date": "EnrollmentDate", "last_flight_date": "FlightDate",
        "tier_code": "TierCode", "agent_name": None, "state": None,
        "country": None, "post_code": None, "dob": None,
        "active_member": None, "date_format": "compact",
    },
}
_MEMBER_DATE_COLUMNS = ("enrollment_date", "last_flight_date", "dob")


def create_pipeline_run(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    *,
    as_of_date: date,
    started_at_utc: datetime | None = None,
    reprocess_of_run_id: str | None = None,
) -> None:
    """Create a run, allowing an identical run-ID retry without rewriting it.

    ``run_id`` is an execution identity and must be new for intentional
    reprocessing. A repeated ID is accepted only when its immutable date and
    reprocess reference agree with the existing row.
    """

    if not run_id:
        raise ValueError("run_id must be non-empty")
    _require_date(as_of_date, "as_of_date")
    started = _utc_timestamp(started_at_utc or datetime.now(timezone.utc), "started_at_utc")
    with _transaction(connection):
        connection.execute(
            """INSERT INTO pipeline_run
               (run_id, started_at_utc, status, as_of_date, reprocess_of_run_id)
               VALUES (?, ?, 'running', ?, ?) ON CONFLICT (run_id) DO NOTHING""",
            [run_id, started, as_of_date, reprocess_of_run_id],
        )
        existing = connection.execute(
            "SELECT as_of_date, reprocess_of_run_id FROM pipeline_run WHERE run_id = ?",
            [run_id],
        ).fetchone()
        if existing != (as_of_date, reprocess_of_run_id):
            raise ValueError(f"run_id {run_id!r} already exists with different run metadata")


def finish_pipeline_run(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    *,
    status: str,
    finished_at_utc: datetime | None = None,
) -> None:
    """Finish a run as succeeded or failed; repeated same-status calls are safe."""

    if status not in {"succeeded", "failed"}:
        raise ValueError("finished run status must be 'succeeded' or 'failed'")
    finished = _utc_timestamp(finished_at_utc or datetime.now(timezone.utc), "finished_at_utc")
    with _transaction(connection):
        current = connection.execute(
            "SELECT status FROM pipeline_run WHERE run_id = ?", [run_id]
        ).fetchone()
        if current is None:
            raise ValueError(f"Unknown run_id: {run_id}")
        if current[0] == status:
            return
        if current[0] != "running":
            raise ValueError(f"Cannot change completed run {run_id!r} from {current[0]!r} to {status!r}")
        if status == "succeeded":
            incomplete = connection.execute(
                """SELECT count(*) FROM run_input
                   WHERE run_id = ? AND input_status <> 'succeeded'""",
                [run_id],
            ).fetchone()[0]
            if incomplete:
                raise ValueError("Cannot succeed a run while one or more inputs are incomplete")
        connection.execute(
            "UPDATE pipeline_run SET status = ?, finished_at_utc = ? WHERE run_id = ?",
            [status, finished, run_id],
        )


def register_input_artifact(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    *,
    source_kind: str,
    source_identity: str,
    content_sha256: str,
    schema_manifest: Mapping[str, object],
    original_filename: str,
    ingested_at_utc: datetime,
    feed_generated_at_utc: datetime | None = None,
) -> str:
    """Register content identity and associate it with a run in pending state.

    The artifact ID is derived exactly from source kind, source identity, and
    SHA-256. Re-registering identical metadata is safe and leaves an existing
    run-input status and first-ingested timestamp untouched.
    """

    if source_kind not in {"member", "redemption"}:
        raise ValueError("source_kind must be 'member' or 'redemption'")
    if not source_identity or not original_filename:
        raise ValueError("source_identity and original_filename must be non-empty")
    content_sha256 = content_sha256.lower()
    _validate_sha256(content_sha256)
    ingested = _utc_timestamp(ingested_at_utc, "ingested_at_utc")
    feed_generated = (
        _utc_timestamp(feed_generated_at_utc, "feed_generated_at_utc")
        if feed_generated_at_utc is not None else None
    )
    manifest = _json_text(schema_manifest)
    artifact_id = _artifact_id(source_kind, source_identity, content_sha256)
    with _transaction(connection):
        _require_running_run(connection, run_id)
        _ensure_artifact(
            connection, artifact_id, source_kind, source_identity, content_sha256, manifest,
        )
        existing = connection.execute(
            "SELECT original_filename, feed_generated_at_utc FROM run_input WHERE run_id = ? AND artifact_id = ?",
            [run_id, artifact_id],
        ).fetchone()
        if existing is None:
            connection.execute(
                """INSERT INTO run_input
                   (run_id, artifact_id, original_filename, feed_generated_at_utc,
                    ingested_at_utc, input_status)
                   VALUES (?, ?, ?, ?, ?, 'pending')""",
                [run_id, artifact_id, original_filename, feed_generated, ingested],
            )
        elif existing != (original_filename, feed_generated):
            raise ValueError("Existing run_input metadata differs from registration request")
    return artifact_id


def persist_member_results(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    batch: IngestionBatch,
    validation_result: ValidationResult,
    transformation_result: TransformationResult,
    *,
    feed_generated_at_utc: datetime | None = None,
) -> str:
    """Persist one complete member artifact's raw rows, outcomes, issues and counts.

    The artifact ID is derived from the source kind, source code and exact file
    SHA-256. Each artifact operation is atomic. Repeating it for the same run
    and inputs is idempotent; a new run preserves a new outcome history while
    reusing immutable raw rows.
    """

    _check_member_inputs(batch, validation_result, transformation_result)
    _check_run_as_of_date(connection, run_id, transformation_result.as_of_date)
    content_sha256 = batch.source_sha256.lower()
    _validate_sha256(content_sha256)
    ingested = _utc_timestamp(batch.ingested_at_utc, "ingested_at_utc")
    feed_generated = (
        _utc_timestamp(feed_generated_at_utc, "feed_generated_at_utc")
        if feed_generated_at_utc is not None else None
    )
    artifact_id = _artifact_id("member", batch.source_code, content_sha256)
    manifest = _member_schema_manifest(batch)
    if _completed_input_exists(
        connection, run_id, artifact_id, "member", batch.source_code,
        content_sha256, manifest, batch.source_file, feed_generated,
    ):
        _verify_member_persistence(
            connection, run_id, artifact_id, batch, validation_result,
            transformation_result,
        )
        return artifact_id
    try:
        with _transaction(connection):
            _register_input(
                connection, run_id, artifact_id, "member", batch.source_code,
                content_sha256, manifest, batch.source_file,
                ingested, feed_generated,
            )
            raw_ids: dict[int, str] = {}
            for record in batch.records:
                raw_id = _raw_member_id(artifact_id, record)
                raw_ids[id(record)] = raw_id
                connection.execute(
                    """INSERT INTO raw_member_record
                       (raw_record_id, artifact_id, source_code, sheet_name, sheet_name_key,
                        source_row_number, schema_fingerprint, raw_values)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?::JSON)
                       ON CONFLICT (raw_record_id) DO NOTHING""",
                    [raw_id, artifact_id, record.source_code, record.sheet_name,
                     record.sheet_name or "", record.source_row_number,
                     record.schema_fingerprint, _json_text(record.raw_values)],
                )

            curated_by_record = {
                id(item.record): item for item in transformation_result.curated_records
            }
            transform_issues: dict[int, list[object]] = {}
            for issue in transformation_result.issues:
                transform_issues.setdefault(id(issue.record), []).append(issue)

            final_dispositions: list[str] = []
            for outcome in validation_result.outcomes:
                record = outcome.record
                raw_id = raw_ids[id(record)]
                curated = curated_by_record.get(id(record))
                issues = transform_issues.get(id(record), [])
                disposition = outcome.disposition.value
                if disposition == "accepted" and issues:
                    disposition = "warning"
                final_dispositions.append(disposition)
                canonical = _member_values(record)
                connection.execute(
                    """INSERT INTO member_record_outcome
                       (run_id, artifact_id, raw_record_id, member_name, source_member_id,
                        enrollment_date, last_flight_date, tier_code, agent_name, state, country,
                        post_code, dob, active_member, age, stale_member, routed_country_code,
                        routing_method, disposition)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT (run_id, raw_record_id) DO NOTHING""",
                    [run_id, artifact_id, raw_id, *canonical,
                     curated.age if curated else None,
                     curated.stale_member if curated else None,
                     curated.country_code if curated else None,
                     curated.country_routing_method if curated else None,
                     disposition],
                )
                all_issues: list[tuple[str, object]] = [
                    ("validation", issue) for issue in outcome.issues
                ] + [("routing", issue) for issue in issues]
                for ordinal, (category, issue) in enumerate(all_issues, start=1):
                    connection.execute(
                        """INSERT INTO member_issue
                           (run_id, raw_record_id, issue_ordinal, issue_category,
                            rule_code, severity, field_name, detail)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                           ON CONFLICT (run_id, raw_record_id, issue_ordinal) DO NOTHING""",
                        [run_id, raw_id, ordinal, category, issue.rule_code,
                         issue.severity.value if category == "validation" else "warning",
                         issue.field_name if category == "validation" else None,
                         issue.detail],
                    )

            connection.execute(
                "UPDATE run_input SET input_status = 'succeeded', failure_detail = NULL WHERE run_id = ? AND artifact_id = ?",
                [run_id, artifact_id],
            )
            _write_member_reconciliation(
                connection, run_id, artifact_id, len(batch.records), final_dispositions,
                len(transformation_result.curated_records),
            )
    except Exception as exc:
        _record_failed_input(
            connection, run_id, artifact_id, "member", batch.source_code,
            content_sha256, manifest, batch.source_file,
            ingested, feed_generated, exc,
        )
        raise
    return artifact_id


def persist_redemption_results(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    result: RedemptionResult,
    *,
    source_identity: str | None = None,
    original_filename: str | None = None,
    ingested_at_utc: datetime | None = None,
    feed_generated_at_utc: datetime | None = None,
    source_bytes: bytes | None = None,
    confirmed_member_references: Mapping[str, tuple[str, str]] | None = None,
) -> str:
    """Persist redemption parents, children, issues and reconciliation atomically.

    Supply ``source_bytes`` to identify/hash the exact original file bytes. For
    in-memory JSON only, a deterministic typed representation of the payload
    collection is hashed; it is not represented as a producer-file hash.
    ``confirmed_member_references`` maps the parser's explicit matched key to
    a persisted ``(run_id, raw_record_id)`` locator. No join is inferred.
    """

    if not result.payload_outcomes:
        raise ValueError("result must contain at least one parent payload")
    _check_redemption_result(result)
    source_name = result.payload_outcomes[0].provenance.source_name
    identity = source_identity or source_name
    filename = original_filename or source_name
    if not identity or not filename:
        raise ValueError("source_identity and original_filename must be non-empty")
    if any(p.provenance.source_name != source_name for p in result.payload_outcomes):
        raise ValueError("one persistence call must contain one source_name")
    ingested = _utc_timestamp(ingested_at_utc or datetime.now(timezone.utc), "ingested_at_utc")
    feed_generated = (
        _utc_timestamp(feed_generated_at_utc, "feed_generated_at_utc")
        if feed_generated_at_utc is not None else None
    )
    digest = hashlib.sha256(source_bytes).hexdigest() if source_bytes is not None else _redemption_content_hash(result)
    artifact_id = _artifact_id("redemption", identity, digest)
    _validate_sha256(digest)
    manifest = _redemption_schema_manifest(result)
    references = confirmed_member_references or {}
    if _completed_input_exists(
        connection, run_id, artifact_id, "redemption", identity, digest,
        manifest, filename, feed_generated,
    ):
        _verify_redemption_persistence(connection, run_id, artifact_id, result, references)
        return artifact_id

    transactions_by_parent: dict[int, list[object]] = {}
    for outcome in result.transaction_outcomes:
        transactions_by_parent.setdefault(outcome.record.provenance.payload_index, []).append(outcome)
    try:
        with _transaction(connection):
            _register_input(
                connection, run_id, artifact_id, "redemption", identity, digest,
                manifest, filename, ingested, feed_generated,
            )
            for parent in result.payload_outcomes:
                pindex = parent.provenance.payload_index
                representation, raw_text, raw_hex, raw_json = _redemption_raw_columns(parent.raw_payload)
                children = transactions_by_parent.get(pindex, [])
                connection.execute(
                    """INSERT INTO raw_redemption_payload
                       (run_id, artifact_id, payload_index, payload_representation,
                        raw_payload_text, raw_payload_hex, raw_payload_json, parent_member_id,
                        parent_feed_date_raw, parsed_feed_date, transaction_count, disposition)
                       VALUES (?, ?, ?, ?, ?, ?, ?::JSON, ?, ?::JSON, ?, ?, ?)
                       ON CONFLICT (run_id, artifact_id, payload_index) DO NOTHING""",
                    [run_id, artifact_id, pindex, representation, raw_text, raw_hex,
                     raw_json, _sql_text(parent.parent_member_id),
                     _json_or_null(parent.parent_feed_date), parent.parsed_feed_date,
                     parent.transaction_count, parent.disposition.value],
                )
                for child in children:
                    item = child.record
                    tindex = item.provenance.transaction_index
                    if tindex is None:
                        raise ValueError("transaction outcome is missing transaction_index")
                    reference = None
                    if item.matched_member_key is not None:
                        reference = references.get(item.matched_member_key)
                        if reference is not None:
                            _require_confirmed_member_reference(connection, reference)
                    disposition = child.disposition.value
                    unmatched_already_recorded = any(
                        issue.rule_code == "MEMBER_REFERENCE_UNMATCHED" for issue in child.issues
                    )
                    if item.matched_member_key is not None and reference is None and not unmatched_already_recorded:
                        disposition = "warning" if disposition == "accepted" else disposition
                    connection.execute(
                        """INSERT INTO redemption_transaction_outcome
                           (run_id, artifact_id, payload_index, transaction_index, transaction_id,
                            transaction_date_raw, parsed_transaction_date, partner,
                            miles_redeemed_raw, miles_redeemed, status, raw_transaction,
                            disposition, matched_member_run_id, matched_member_raw_record_id)
                           VALUES (?, ?, ?, ?, ?, ?::JSON, ?, ?, ?::JSON, ?, ?, ?::JSON, ?, ?, ?)
                           ON CONFLICT (run_id, artifact_id, payload_index, transaction_index) DO NOTHING""",
                        [run_id, artifact_id, pindex, tindex,
                         _sql_text(item.transaction_id), _json_or_null(item.transaction_date),
                         item.parsed_transaction_date, _sql_text(item.partner),
                         _json_or_null(item.miles_redeemed), _integer_miles(item.miles_redeemed),
                         _sql_text(item.status), _json_text(item.raw_transaction), disposition,
                         reference[0] if reference else None, reference[1] if reference else None],
                    )
                    for ordinal, issue in enumerate(child.issues, start=1):
                        _insert_redemption_issue(
                            connection, run_id, artifact_id, pindex, tindex, "transaction",
                            ordinal, issue.rule_code, issue.severity.value, issue.field_name,
                            issue.detail,
                        )
                    if item.matched_member_key is not None and reference is None and not unmatched_already_recorded:
                        _insert_redemption_issue(
                            connection, run_id, artifact_id, pindex, tindex, "transaction",
                            len(child.issues) + 1, "MEMBER_REFERENCE_UNMATCHED", "warning", "member_id",
                            "No persisted member-record locator was supplied for the explicitly matched key.",
                        )
                for ordinal, issue in enumerate(parent.issues, start=1):
                    _insert_redemption_issue(
                        connection, run_id, artifact_id, pindex, None, "parent", ordinal,
                        issue.rule_code, issue.severity.value, issue.field_name, issue.detail,
                    )

            connection.execute(
                "UPDATE run_input SET input_status = 'succeeded', failure_detail = NULL WHERE run_id = ? AND artifact_id = ?",
                [run_id, artifact_id],
            )
            _write_redemption_reconciliation(
                connection, run_id, artifact_id, result, references,
            )
    except Exception as exc:
        _record_failed_input(
            connection, run_id, artifact_id, "redemption", identity, digest,
            manifest, filename, ingested, feed_generated, exc,
        )
        raise
    return artifact_id


@contextmanager
def _transaction(connection: duckdb.DuckDBPyConnection) -> Iterator[None]:
    connection.execute("BEGIN TRANSACTION")
    try:
        yield
        connection.execute("COMMIT")
    except BaseException as exc:
        try:
            connection.execute("ROLLBACK")
        except duckdb.Error as rollback_error:
            raise rollback_error from exc
        raise


def _register_input(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    source_kind: str,
    source_identity: str,
    content_sha256: str,
    schema_manifest: str,
    filename: str,
    ingested_at_utc: datetime,
    feed_generated_at_utc: datetime | None,
) -> None:
    _require_running_run(connection, run_id)
    _ensure_artifact(
        connection, artifact_id, source_kind, source_identity, content_sha256, schema_manifest,
    )

    existing = connection.execute(
        "SELECT original_filename, feed_generated_at_utc, ingested_at_utc FROM run_input WHERE run_id = ? AND artifact_id = ?",
        [run_id, artifact_id],
    ).fetchone()
    if existing is None:
        connection.execute(
            """INSERT INTO run_input
               (run_id, artifact_id, original_filename, feed_generated_at_utc,
                ingested_at_utc, input_status, failure_detail)
               VALUES (?, ?, ?, ?, ?, 'running', NULL)""",
            [run_id, artifact_id, filename, feed_generated_at_utc, ingested_at_utc],
        )
    else:
        if existing[0] != filename or existing[1] != feed_generated_at_utc:
            raise ValueError("Retry metadata differs from the existing run_input row")
        connection.execute(
            """UPDATE run_input SET input_status = 'running', failure_detail = NULL
               WHERE run_id = ? AND artifact_id = ?""",
            [run_id, artifact_id],
        )


def _ensure_artifact(
    connection: duckdb.DuckDBPyConnection,
    artifact_id: str,
    source_kind: str,
    source_identity: str,
    content_sha256: str,
    schema_manifest: str,
) -> None:
    connection.execute(
        """INSERT INTO input_artifact
           (artifact_id, source_kind, source_identity, content_sha256, schema_manifest)
           VALUES (?, ?, ?, ?, ?::JSON) ON CONFLICT (artifact_id) DO NOTHING""",
        [artifact_id, source_kind, source_identity, content_sha256, schema_manifest],
    )
    artifact = connection.execute(
        "SELECT source_kind, source_identity, content_sha256, schema_manifest::VARCHAR FROM input_artifact WHERE artifact_id = ?",
        [artifact_id],
    ).fetchone()
    if artifact != (source_kind, source_identity, content_sha256, schema_manifest):
        raise ValueError(f"Artifact identity collision or metadata mismatch for {artifact_id!r}")


def _record_failed_input(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    source_kind: str,
    source_identity: str,
    content_sha256: str,
    manifest: str,
    filename: str,
    ingested_at_utc: datetime,
    feed_generated_at_utc: datetime | None,
    failure: Exception,
) -> None:
    """Record failure metadata separately after the data transaction rolls back."""

    try:
        with _transaction(connection):
            prior = connection.execute(
                "SELECT input_status FROM run_input WHERE run_id = ? AND artifact_id = ?",
                [run_id, artifact_id],
            ).fetchone()
            if prior is not None and prior[0] == "succeeded":
                return
            _register_input(
                connection, run_id, artifact_id, source_kind, source_identity,
                content_sha256, manifest, filename, ingested_at_utc,
                feed_generated_at_utc,
            )
            connection.execute(
                """UPDATE run_input SET input_status = 'failed', failure_detail = ?
                   WHERE run_id = ? AND artifact_id = ?""",
                [f"{type(failure).__name__}: {failure}", run_id, artifact_id],
            )
    except Exception as recording_error:
        raise recording_error from failure


def _completed_input_exists(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    source_kind: str,
    source_identity: str,
    content_sha256: str,
    manifest: str,
    filename: str,
    feed_generated_at_utc: datetime | None,
) -> bool:
    row = connection.execute(
        """SELECT i.source_kind, i.source_identity, i.content_sha256,
                  i.schema_manifest::VARCHAR, r.original_filename,
                  r.feed_generated_at_utc, r.input_status
           FROM input_artifact i JOIN run_input r USING (artifact_id)
           WHERE r.run_id = ? AND r.artifact_id = ?""",
        [run_id, artifact_id],
    ).fetchone()
    if row is None:
        return False
    expected = (source_kind, source_identity, content_sha256, manifest, filename, feed_generated_at_utc)
    if row[:6] != expected:
        raise ValueError("Existing run_input/artifact metadata differs from the retry")
    return row[6] == "succeeded"


def _verify_member_persistence(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    batch: IngestionBatch,
    validation_result: ValidationResult,
    transformation_result: TransformationResult,
) -> None:
    expected_raw = {
        _raw_member_id(artifact_id, record) for record in batch.records
    }
    actual_raw = {row[0] for row in connection.execute(
        "SELECT raw_record_id FROM raw_member_record WHERE artifact_id = ?", [artifact_id]
    ).fetchall()}
    expected_outcomes = {(run_id, raw_id) for raw_id in expected_raw}
    actual_outcomes = set(connection.execute(
        "SELECT run_id, raw_record_id FROM member_record_outcome WHERE run_id = ? AND artifact_id = ?",
        [run_id, artifact_id],
    ).fetchall())
    expected_issues = []
    for outcome in validation_result.outcomes:
        raw_id = _raw_member_id(artifact_id, outcome.record)
        issues = [("validation", issue) for issue in outcome.issues]
        issues.extend(("routing", issue) for issue in transformation_result.issues
                      if issue.record is outcome.record)
        for ordinal, (category, issue) in enumerate(issues, start=1):
            expected_issues.append((run_id, raw_id, ordinal, category, issue.rule_code,
                                    issue.severity.value if category == "validation" else "warning",
                                    issue.field_name if category == "validation" else None,
                                    issue.detail))
    actual_issues = connection.execute(
        """SELECT run_id, raw_record_id, issue_ordinal, issue_category, rule_code,
                  severity, field_name, detail FROM member_issue
           WHERE run_id = ? AND raw_record_id IN
             (SELECT raw_record_id FROM raw_member_record WHERE artifact_id = ?)""",
        [run_id, artifact_id],
    ).fetchall()
    if (actual_raw != expected_raw or actual_outcomes != expected_outcomes
            or sorted(actual_issues) != sorted(expected_issues)):
        raise PersistenceIntegrityError(
            f"Succeeded member input {run_id}/{artifact_id} is missing or has inconsistent raw, outcome, or issue rows"
        )
    _verify_member_reconciliation(connection, run_id, artifact_id)


def _verify_redemption_persistence(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    result: RedemptionResult,
    references: Mapping[str, tuple[str, str]],
) -> None:
    expected_parents = {parent.provenance.payload_index for parent in result.payload_outcomes}
    actual_parents = {row[0] for row in connection.execute(
        "SELECT payload_index FROM raw_redemption_payload WHERE run_id = ? AND artifact_id = ?",
        [run_id, artifact_id],
    ).fetchall()}
    expected_children = {
        (outcome.record.provenance.payload_index, outcome.record.provenance.transaction_index)
        for outcome in result.transaction_outcomes
    }
    actual_children = set(connection.execute(
        "SELECT payload_index, transaction_index FROM redemption_transaction_outcome WHERE run_id = ? AND artifact_id = ?",
        [run_id, artifact_id],
    ).fetchall())
    expected_issue_count = sum(len(item.issues) for item in result.payload_outcomes)
    expected_issue_count += sum(len(item.issues) for item in result.transaction_outcomes)
    expected_issue_count += sum(
        1 for item in result.transaction_outcomes
        if item.record.matched_member_key is not None
        and item.record.matched_member_key not in references
        and not any(issue.rule_code == "MEMBER_REFERENCE_UNMATCHED" for issue in item.issues)
    )
    actual_issue_count = connection.execute(
        "SELECT count(*) FROM redemption_issue WHERE run_id = ? AND artifact_id = ?",
        [run_id, artifact_id],
    ).fetchone()[0]
    if (actual_parents != expected_parents or actual_children != expected_children
            or actual_issue_count != expected_issue_count):
        raise PersistenceIntegrityError(
            f"Succeeded redemption input {run_id}/{artifact_id} is missing or has inconsistent parent, transaction, or issue rows"
        )
    _verify_redemption_reconciliation(connection, run_id, artifact_id)


def _verify_member_reconciliation(
    connection: duckdb.DuckDBPyConnection, run_id: str, artifact_id: str,
) -> None:
    rows = connection.execute(
        """SELECT disposition, count(*) FROM member_record_outcome
           WHERE run_id = ? AND artifact_id = ? GROUP BY disposition""",
        [run_id, artifact_id],
    ).fetchall()
    counts = {name: count for name, count in rows}
    read_count = sum(counts.values())
    raw_count = connection.execute(
        "SELECT count(*) FROM raw_member_record WHERE artifact_id = ?", [artifact_id]
    ).fetchone()[0]
    curated_count = connection.execute(
        "SELECT count(*) FROM member_record_outcome WHERE run_id = ? AND artifact_id = ? AND disposition <> 'quarantined' AND routed_country_code IN ('AUS','IND','USA')",
        [run_id, artifact_id],
    ).fetchone()[0]
    expected = (read_count, counts.get("accepted", 0), counts.get("warning", 0),
                counts.get("quarantined", 0), read_count - counts.get("quarantined", 0),
                0, raw_count, curated_count)
    _assert_reconciliation(connection, run_id, artifact_id, "member_record", expected)


def _verify_redemption_reconciliation(
    connection: duckdb.DuckDBPyConnection, run_id: str, artifact_id: str,
) -> None:
    parent_rows = connection.execute(
        "SELECT disposition, count(*) FROM raw_redemption_payload WHERE run_id = ? AND artifact_id = ? GROUP BY disposition",
        [run_id, artifact_id],
    ).fetchall()
    parent_counts = {name: count for name, count in parent_rows}
    parent_read = sum(parent_counts.values())
    parent_raw = parent_read
    _assert_reconciliation(connection, run_id, artifact_id, "redemption_parent", (
        parent_read, parent_counts.get("accepted", 0), parent_counts.get("warning", 0),
        parent_counts.get("quarantined", 0), 0, 0, parent_raw, 0,
    ))
    child_rows = connection.execute(
        "SELECT disposition, count(*) FROM redemption_transaction_outcome WHERE run_id = ? AND artifact_id = ? GROUP BY disposition",
        [run_id, artifact_id],
    ).fetchall()
    child_counts = {name: count for name, count in child_rows}
    child_read = sum(child_counts.values())
    unmatched = connection.execute(
        "SELECT count(DISTINCT (payload_index, transaction_index)) FROM redemption_issue WHERE run_id = ? AND artifact_id = ? AND rule_code = 'MEMBER_REFERENCE_UNMATCHED'",
        [run_id, artifact_id],
    ).fetchone()[0]
    curated = connection.execute(
        "SELECT count(*) FROM curated_redemptions WHERE run_id = ? AND artifact_id = ?",
        [run_id, artifact_id],
    ).fetchone()[0]
    _assert_reconciliation(connection, run_id, artifact_id, "redemption_transaction", (
        child_read, child_counts.get("accepted", 0), child_counts.get("warning", 0),
        child_counts.get("quarantined", 0), child_read, unmatched, child_read, curated,
    ))


def _assert_reconciliation(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    scope: str,
    expected: tuple[int, ...],
) -> None:
    actual = connection.execute(
        """SELECT read_count, accepted_count, warned_count, quarantined_count,
                  transformed_count, unmatched_count, raw_persisted_count,
                  curated_persisted_count FROM run_reconciliation
           WHERE run_id = ? AND artifact_id = ? AND scope = ?""",
        [run_id, artifact_id, scope],
    ).fetchone()
    if actual != expected:
        raise PersistenceIntegrityError(
            f"Succeeded input {run_id}/{artifact_id} has inconsistent {scope} reconciliation: expected {expected}, found {actual}"
        )


def _artifact_id(source_kind: str, source_identity: str, content_sha256: str) -> str:
    digest = hashlib.sha256(f"{source_identity}\0{content_sha256}".encode("utf-8")).hexdigest()
    return f"{source_kind}-{source_identity.lower()}-{digest}"


def _validate_sha256(value: str) -> None:
    if len(value) != 64 or any(character not in "0123456789abcdef" for character in value.lower()):
        raise ValueError("content_sha256 must be a 64-character hexadecimal SHA-256 digest")


def _raw_member_id(artifact_id: str, record: RawRecord) -> str:
    sheet_key = record.sheet_name or ""
    coordinate = f"{artifact_id}\0{record.source_code}\0{sheet_key}\0{record.source_row_number}"
    return hashlib.sha256(coordinate.encode("utf-8")).hexdigest()


def _member_schema_manifest(batch: IngestionBatch) -> str:
    return _json_text({
        "schemas": [
            {
                "sheet_name": schema.sheet_name,
                "columns": list(schema.columns),
                "schema_fingerprint": schema.schema_fingerprint,
            }
            for schema in batch.schemas
        ],
        "column_presence_by_sheet": [
            {"sheet_name": schema.sheet_name, "columns": list(schema.columns)}
            for schema in batch.schemas
        ],
    })


def _redemption_schema_manifest(result: RedemptionResult) -> str:
    return _json_text({
        "payload_count": len(result.payload_outcomes),
        "representations": sorted({
            _payload_representation(parent.raw_payload)
            for parent in result.payload_outcomes
        }),
    })


def _check_member_inputs(
    batch: IngestionBatch,
    validation_result: ValidationResult,
    transformation_result: TransformationResult,
) -> None:
    if validation_result.batch_id != batch.batch_id:
        raise ValueError("validation result does not belong to the supplied batch")
    if transformation_result.validation_result != validation_result:
        raise ValueError("transformation result does not belong to the supplied validation result")
    if transformation_result.as_of_date != validation_result.as_of_date:
        raise ValueError("validation and transformation as_of_date values differ")
    if not batch.source_file or not batch.source_code or not batch.batch_id:
        raise ValueError("member batch source file, source code, and batch ID must be populated")
    if len(validation_result.outcomes) != len(batch.records) or any(
        outcome.record is not record
        for outcome, record in zip(validation_result.outcomes, batch.records)
    ):
        raise ValueError("validation outcomes must account for batch records in source order")
    for record in batch.records:
        if (
            record.source_code != batch.source_code
            or record.source_file != batch.source_file
            or record.source_sha256 != batch.source_sha256
            or record.batch_id != batch.batch_id
            or record.ingested_at_utc != batch.ingested_at_utc
        ):
            raise ValueError("raw record provenance does not match its ingestion batch")
    curated_ids = [id(item.record) for item in transformation_result.curated_records]
    expected_ids = [
        id(outcome.record) for outcome in validation_result.outcomes
        if outcome.disposition is not Disposition.QUARANTINED
    ]
    if curated_ids != expected_ids:
        raise ValueError("transformation output must contain each non-quarantined record exactly once")


def _check_redemption_result(result: RedemptionResult) -> None:
    expected_parent_indexes = list(range(1, len(result.payload_outcomes) + 1))
    actual_parent_indexes = [item.provenance.payload_index for item in result.payload_outcomes]
    if actual_parent_indexes != expected_parent_indexes:
        raise ValueError("redemption parent payload indexes must be unique, ordered, and one-based")
    transactions_by_parent: dict[int, list[object]] = {}
    for outcome in result.transaction_outcomes:
        record = outcome.record
        payload_index = record.provenance.payload_index
        if payload_index < 1 or payload_index > len(result.payload_outcomes):
            raise ValueError("redemption transaction references an unknown parent payload")
        parent = result.payload_outcomes[payload_index - 1]
        parent_provenance = parent.provenance
        if (
            outcome.parent_outcome is not parent
            or record.provenance.source_name != parent_provenance.source_name
            or record.provenance.batch_id != parent_provenance.batch_id
            or record.provenance.payload_index != parent_provenance.payload_index
            or record.provenance.transaction_index is None
        ):
            raise ValueError("redemption transaction provenance does not match its parent")
        transactions_by_parent.setdefault(payload_index, []).append(outcome)
    for outcomes in transactions_by_parent.values():
        indexes = [item.record.provenance.transaction_index for item in outcomes]
        if indexes != list(range(1, len(indexes) + 1)):
            raise ValueError("child transaction indexes must be unique, ordered, and one-based")
    for parent in result.payload_outcomes:
        if len(transactions_by_parent.get(parent.provenance.payload_index, [])) != parent.transaction_count:
            raise ValueError("parent transaction_count does not match supplied child outcomes")


def _member_values(record: RawRecord) -> list[object]:
    try:
        mapping = _MEMBER_SOURCE_FIELDS[record.source_code]
    except KeyError as exc:
        raise ValueError(f"Unsupported member source code: {record.source_code}") from exc

    result: list[object] = []
    for canonical_name in (
        "member_name", "source_member_id", "enrollment_date", "last_flight_date",
        "tier_code", "agent_name", "state", "country", "post_code", "dob",
        "active_member",
    ):
        source_column = mapping[canonical_name]
        value = record.raw_values.get(source_column) if source_column is not None else None
        if canonical_name in _MEMBER_DATE_COLUMNS:
            value = _parse_member_date(value, mapping["date_format"])
        elif canonical_name == "post_code":
            value = _integer_post_code(value)
        elif value is not None:
            value = _sql_text(value)
        result.append(value)
    return result


def _parse_member_date(value: object, date_format: object) -> date | None:
    if value is None or (isinstance(value, str) and (not value.strip() or value.strip().upper() == "NULL")):
        return None
    parsed, _reason = parse_source_date(value, str(date_format))
    return parsed


def _integer_post_code(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.strip().isdigit():
        return int(value.strip())
    return None


def _write_member_reconciliation(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    read_count: int,
    dispositions: list[str],
    transformed_count: int,
) -> None:
    # Counts used for the persisted reconciliation are read back from committed
    # candidate rows inside this transaction, not trusted from caller objects.
    disposition_counts = dict(connection.execute(
        """SELECT disposition, count(*) FROM member_record_outcome
           WHERE run_id = ? AND artifact_id = ? GROUP BY disposition""",
        [run_id, artifact_id],
    ).fetchall())
    read_count = sum(disposition_counts.values())
    transformed_count = read_count - disposition_counts.get("quarantined", 0)
    raw_count = connection.execute(
        "SELECT count(*) FROM raw_member_record WHERE artifact_id = ?", [artifact_id]
    ).fetchone()[0]
    curated_count = connection.execute(
        """SELECT count(*) FROM member_record_outcome
           WHERE run_id = ? AND artifact_id = ? AND disposition <> 'quarantined'
             AND routed_country_code IN ('AUS', 'IND', 'USA')""",
        [run_id, artifact_id],
    ).fetchone()[0]
    _upsert_reconciliation(
        connection, run_id, artifact_id, "member_record", read_count,
        disposition_counts.get("accepted", 0), disposition_counts.get("warning", 0),
        disposition_counts.get("quarantined", 0), transformed_count, 0,
        raw_count, curated_count,
    )


def _write_redemption_reconciliation(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    result: RedemptionResult,
    references: Mapping[str, tuple[str, str]],
) -> None:
    parents = result.payload_outcomes
    children = result.transaction_outcomes
    unmatched_count = connection.execute(
        """SELECT count(DISTINCT (payload_index, transaction_index))
           FROM redemption_issue WHERE run_id = ? AND artifact_id = ?
             AND rule_code = 'MEMBER_REFERENCE_UNMATCHED'""",
        [run_id, artifact_id],
    ).fetchone()[0]
    parent_raw_count = connection.execute(
        "SELECT count(*) FROM raw_redemption_payload WHERE run_id = ? AND artifact_id = ?",
        [run_id, artifact_id],
    ).fetchone()[0]
    child_raw_count = connection.execute(
        """SELECT count(*) FROM redemption_transaction_outcome
           WHERE run_id = ? AND artifact_id = ?""",
        [run_id, artifact_id],
    ).fetchone()[0]
    child_curated_count = connection.execute(
        """SELECT count(*) FROM curated_redemptions
           WHERE run_id = ? AND artifact_id = ?""",
        [run_id, artifact_id],
    ).fetchone()[0]
    parent_counts = dict(connection.execute(
        """SELECT disposition, count(*) FROM raw_redemption_payload
           WHERE run_id = ? AND artifact_id = ? GROUP BY disposition""",
        [run_id, artifact_id],
    ).fetchall())
    parent_read = sum(parent_counts.values())
    _upsert_reconciliation(
        connection, run_id, artifact_id, "redemption_parent", parent_read,
        parent_counts.get("accepted", 0), parent_counts.get("warning", 0),
        parent_counts.get("quarantined", 0),
        0, 0, parent_raw_count, 0,
    )
    child_counts = dict(connection.execute(
        """SELECT disposition, count(*) FROM redemption_transaction_outcome
           WHERE run_id = ? AND artifact_id = ? GROUP BY disposition""",
        [run_id, artifact_id],
    ).fetchall())
    child_read = sum(child_counts.values())
    _upsert_reconciliation(
        connection, run_id, artifact_id, "redemption_transaction", child_read,
        child_counts.get("accepted", 0), child_counts.get("warning", 0),
        child_counts.get("quarantined", 0), child_read, unmatched_count,
        child_raw_count, child_curated_count,
    )


def _upsert_reconciliation(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    scope: str,
    read_count: int,
    accepted_count: int,
    warned_count: int,
    quarantined_count: int,
    transformed_count: int,
    unmatched_count: int,
    raw_persisted_count: int,
    curated_persisted_count: int,
) -> None:
    connection.execute(
        """INSERT INTO run_reconciliation
           (run_id, artifact_id, scope, read_count, accepted_count, warned_count,
            quarantined_count, transformed_count, unmatched_count, raw_persisted_count,
            curated_persisted_count)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (run_id, artifact_id, scope) DO UPDATE SET
             read_count = excluded.read_count,
             accepted_count = excluded.accepted_count,
             warned_count = excluded.warned_count,
             quarantined_count = excluded.quarantined_count,
             transformed_count = excluded.transformed_count,
             unmatched_count = excluded.unmatched_count,
             raw_persisted_count = excluded.raw_persisted_count,
             curated_persisted_count = excluded.curated_persisted_count""",
        [run_id, artifact_id, scope, read_count, accepted_count, warned_count,
         quarantined_count, transformed_count, unmatched_count, raw_persisted_count,
         curated_persisted_count],
    )


def _insert_redemption_issue(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    payload_index: int,
    transaction_index: int | None,
    category: str,
    ordinal: int,
    rule_code: str,
    severity: str,
    field_name: str | None,
    detail: str,
) -> None:
    locator = f"{run_id}|{artifact_id}|{payload_index}|{transaction_index}|{ordinal}"
    issue_id = str(uuid5(NAMESPACE_URL, locator))
    connection.execute(
        """INSERT INTO redemption_issue
           (issue_id, run_id, artifact_id, payload_index, transaction_index,
            issue_category, rule_code, severity, field_name, detail)
           VALUES (?::UUID, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (issue_id) DO NOTHING""",
        [issue_id, run_id, artifact_id, payload_index, transaction_index,
         category, rule_code, severity, field_name, detail],
    )


def _require_confirmed_member_reference(
    connection: duckdb.DuckDBPyConnection,
    reference: tuple[str, str],
) -> None:
    if len(reference) != 2 or not all(reference):
        raise ValueError("confirmed member reference must be a (run_id, raw_record_id) pair")
    row = connection.execute(
        """SELECT disposition FROM member_record_outcome
           WHERE run_id = ? AND raw_record_id = ?""",
        list(reference),
    ).fetchone()
    if row is None:
        raise ValueError(f"Confirmed member reference does not exist: {reference!r}")
    if row[0] == "quarantined":
        raise ValueError("Cannot persist a redemption link to a quarantined member outcome")


def _redemption_raw_columns(payload: object) -> tuple[str, str | None, str | None, str | None]:
    representation = _payload_representation(payload)
    if representation == "text":
        return representation, str(payload), None, None
    if representation == "bytes_hex":
        return representation, None, bytes(payload).hex(), None
    return representation, None, None, _typed_payload_json(payload)


def _payload_representation(payload: object) -> str:
    if isinstance(payload, str):
        return "text"
    if isinstance(payload, bytes):
        return "bytes_hex"
    return "mapping_json"


def _redemption_content_hash(result: RedemptionResult) -> str:
    parts = []
    for parent in result.payload_outcomes:
        payload = parent.raw_payload
        if isinstance(payload, bytes):
            part: object = ["bytes", payload.hex()]
        elif isinstance(payload, str):
            part = ["text", payload]
        else:
            part = ["mapping", _typed_value(payload)]
        parts.append(part)
    encoded = json.dumps(parts, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _json_or_null(value: object) -> str:
    return _json_text(value)


def _json_text(value: object) -> str:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
            allow_nan=False, default=_json_default,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"Value cannot be represented in persisted JSON: {exc}") from exc


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return {"$skypoints_type": "datetime", "value": value.isoformat(), "fold": value.fold}
    if isinstance(value, date):
        return {"$skypoints_type": "date", "value": value.isoformat()}
    if isinstance(value, bytes):
        return {"$skypoints_type": "bytes", "hex": value.hex()}
    raise TypeError(f"Unsupported JSON value type: {type(value).__name__}")


def _typed_payload_json(value: object) -> str:
    """Serialize mapping payloads with an explicit, versioned Python-type envelope."""
    return _json_text({
        "$skypoints_encoding": "typed-json-v1",
        "value": _typed_value(value),
    })


def _typed_value(value: object, active: set[int] | None = None) -> object:
    """Return a deterministic, recursively type-tagged representation for hashing."""
    if value is None:
        return ["none"]
    if isinstance(value, bool):
        return ["bool", value]
    if isinstance(value, int):
        return ["int", str(value)]
    if isinstance(value, float):
        if not (float("-inf") < value < float("inf")):
            raise ValueError("Non-finite floats are unsupported in redemption payload mappings")
        return ["float", value.hex()]
    if isinstance(value, str):
        return ["str", value]
    if isinstance(value, bytes):
        return ["bytes", value.hex()]
    if isinstance(value, datetime):
        return ["datetime", value.isoformat(), value.fold]
    if isinstance(value, date):
        return ["date", value.isoformat()]
    if not isinstance(value, (Mapping, list, tuple)):
        raise ValueError(
            f"Unsupported redemption payload value type: {type(value).__name__}"
        )

    active = active if active is not None else set()
    identity = id(value)
    if identity in active:
        raise ValueError("Cyclic redemption payload mappings or sequences are unsupported")
    active.add(identity)
    try:
        if isinstance(value, Mapping):
            items = [(_typed_value(key, active), _typed_value(item, active)) for key, item in value.items()]
            items.sort(key=lambda pair: json.dumps(pair[0], sort_keys=True, separators=(",", ":")))
            return ["mapping", items]
        tag = "tuple" if isinstance(value, tuple) else "list"
        return [tag, [_typed_value(item, active) for item in value]]
    finally:
        active.remove(identity)


def _sql_text(value: object) -> str | None:
    return None if value is None else str(value)


def _integer_miles(value: object) -> int | None:
    return value if type(value) is int else None


def _require_date(value: object, name: str) -> None:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError(f"{name} must be a calendar date")


def _utc_timestamp(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(timezone.utc)


def _check_run_as_of_date(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    as_of_date: date,
) -> None:
    _require_date(as_of_date, "as_of_date")
    row = connection.execute("SELECT as_of_date FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
    if row is None:
        raise ValueError(f"Unknown run_id: {run_id}")
    if row[0] != as_of_date:
        raise ValueError("pipeline run as_of_date must match transformation as_of_date")


def _require_running_run(connection: duckdb.DuckDBPyConnection, run_id: str) -> None:
    row = connection.execute("SELECT status FROM pipeline_run WHERE run_id = ?", [run_id]).fetchone()
    if row is None:
        raise ValueError(f"Unknown run_id: {run_id}")
    if row[0] != "running":
        raise ValueError(f"Cannot add input to {row[0]!r} pipeline run {run_id!r}")
