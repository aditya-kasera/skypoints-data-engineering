"""End-to-end local assessment demonstration runner."""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from datetime import date, datetime, timezone
from pathlib import Path
import sys
from typing import Sequence
from uuid import uuid4

import duckdb

from .ingestion import ingest_active_sources
from .persistence import (
    create_pipeline_run,
    finish_pipeline_run,
    initialize_schema,
    open_database,
    persist_member_results,
    persist_redemption_results,
)
from .redemptions import process_redemption_payloads
from .transforms import transform_batch
from .validation import validate_batch


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_DEFAULT_DATA_DIR = _PROJECT_ROOT / "data" / "raw"
_DEFAULT_REDEMPTION_FIXTURE = _PROJECT_ROOT / "tests" / "fixtures" / "redemptions_assessment_example.json"
_DEFAULT_DATABASE = _PROJECT_ROOT / "build" / "skypoints.duckdb"


@dataclass(frozen=True)
class DemoSummary:
    """Persisted counts and status for one local demo execution."""

    run_id: str
    status: str
    as_of_date: date
    database_path: Path
    artifact_lines: tuple[str, ...]
    member_counts: tuple[int, int, int]
    country_counts: tuple[tuple[str, int], ...]
    redemption_parent_counts: tuple[int, int, int, int]
    redemption_transaction_counts: tuple[int, int, int, int, int]
    persistence_counts: tuple[int, int, int, int, int]
    reconciliation_ok: bool


def run_demo(
    *,
    as_of_date: date,
    database_path: str | Path = _DEFAULT_DATABASE,
    data_dir: str | Path = _DEFAULT_DATA_DIR,
    redemption_fixture: str | Path = _DEFAULT_REDEMPTION_FIXTURE,
) -> DemoSummary:
    """Run all supplied member adapters and the illustrative JSON fixture.

    A fresh execution ID is generated for every invocation. Repeating a demo
    creates a new auditable run while reusing content-level artifact identity.
    The explicit ``as_of_date`` is used for validation and transformations.
    """
    if not isinstance(as_of_date, date) or isinstance(as_of_date, datetime):
        raise TypeError("as_of_date must be an explicit datetime.date")
    db_path = Path(database_path).expanduser().resolve()
    input_dir = Path(data_dir).expanduser().resolve()
    fixture_path = Path(redemption_fixture).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = f"demo-{uuid4().hex}"
    started = datetime.now(timezone.utc)

    with open_database(db_path) as connection:
        initialize_schema(connection)
        create_pipeline_run(
            connection, run_id, as_of_date=as_of_date, started_at_utc=started,
        )
        try:
            batches = ingest_active_sources(input_dir, ingested_at_utc=started)
            for batch in batches:
                validation = validate_batch(batch, as_of_date=as_of_date)
                transformation = transform_batch(validation, as_of_date=as_of_date)
                persist_member_results(
                    connection, run_id, batch, validation, transformation,
                )

            if not fixture_path.is_file():
                raise FileNotFoundError(f"Illustrative redemption fixture not found: {fixture_path}")
            source_bytes = fixture_path.read_bytes()
            try:
                redemption_text = source_bytes.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ValueError(f"Redemption fixture is not UTF-8: {fixture_path}") from exc
            redemption_result = process_redemption_payloads(
                redemption_text, source_name=fixture_path.name,
            )
            persist_redemption_results(
                connection, run_id, redemption_result,
                source_identity=fixture_path.name,
                original_filename=fixture_path.name,
                ingested_at_utc=started,
                source_bytes=source_bytes,
            )

            reconciliation_ok = _reconciliation_matches_database(connection, run_id)
            if not reconciliation_ok:
                raise RuntimeError("Persisted reconciliation does not match database rows")
            summary = _read_summary(connection, run_id, as_of_date, db_path, reconciliation_ok)
            finish_pipeline_run(
                connection, run_id, status="succeeded", finished_at_utc=datetime.now(timezone.utc),
            )
            return replace(summary, status="succeeded")
        except Exception:
            finish_pipeline_run(
                connection, run_id, status="failed", finished_at_utc=datetime.now(timezone.utc),
            )
            raise


def _reconciliation_matches_database(
    connection: duckdb.DuckDBPyConnection, run_id: str,
) -> bool:
    """Check every artifact/scope reconciliation row against persisted rows."""
    artifacts = connection.execute(
        """SELECT r.artifact_id, a.source_kind FROM run_input r
           JOIN input_artifact a USING (artifact_id) WHERE r.run_id = ?""",
        [run_id],
    ).fetchall()
    expected_keys: set[tuple[str, str]] = set()
    for artifact_id, source_kind in artifacts:
        if source_kind == "member":
            expected_keys.add((artifact_id, "member_record"))
            expected = _member_reconciliation_values(connection, run_id, artifact_id)
            if not _reconciliation_row_matches(connection, run_id, artifact_id, "member_record", expected):
                return False
        else:
            for scope in ("redemption_parent", "redemption_transaction"):
                expected_keys.add((artifact_id, scope))
                expected = _redemption_reconciliation_values(connection, run_id, artifact_id, scope)
                if not _reconciliation_row_matches(connection, run_id, artifact_id, scope, expected):
                    return False
    found_keys = set(connection.execute(
        "SELECT artifact_id, scope FROM run_reconciliation WHERE run_id = ?", [run_id],
    ).fetchall())
    return found_keys == expected_keys


def _member_reconciliation_values(
    connection: duckdb.DuckDBPyConnection, run_id: str, artifact_id: str,
) -> tuple[int, ...]:
    rows = connection.execute(
        """SELECT disposition, count(*) FROM member_record_outcome
           WHERE run_id = ? AND artifact_id = ? GROUP BY disposition""",
        [run_id, artifact_id],
    ).fetchall()
    count = dict(rows)
    read = sum(count.values())
    raw = connection.execute(
        "SELECT count(*) FROM raw_member_record WHERE artifact_id = ?", [artifact_id],
    ).fetchone()[0]
    curated = connection.execute(
        """SELECT count(*) FROM member_record_outcome WHERE run_id = ? AND artifact_id = ?
           AND disposition <> 'quarantined' AND routed_country_code IN ('AUS','IND','USA')""",
        [run_id, artifact_id],
    ).fetchone()[0]
    return (read, count.get("accepted", 0), count.get("warning", 0),
            count.get("quarantined", 0), read - count.get("quarantined", 0), 0, raw, curated)


def _redemption_reconciliation_values(
    connection: duckdb.DuckDBPyConnection, run_id: str, artifact_id: str, scope: str,
) -> tuple[int, ...]:
    if scope == "redemption_parent":
        grouped = connection.execute(
            """SELECT disposition, count(*) FROM raw_redemption_payload
               WHERE run_id = ? AND artifact_id = ? GROUP BY disposition""",
            [run_id, artifact_id],
        ).fetchall()
        counts = dict(grouped)
        read = sum(counts.values())
        return (read, counts.get("accepted", 0), counts.get("warning", 0),
                counts.get("quarantined", 0), 0, 0, read, 0)
    grouped = connection.execute(
        """SELECT disposition, count(*) FROM redemption_transaction_outcome
           WHERE run_id = ? AND artifact_id = ? GROUP BY disposition""",
        [run_id, artifact_id],
    ).fetchall()
    counts = dict(grouped)
    read = sum(counts.values())
    unmatched = connection.execute(
        """SELECT count(DISTINCT (payload_index, transaction_index)) FROM redemption_issue
           WHERE run_id = ? AND artifact_id = ? AND rule_code = 'MEMBER_REFERENCE_UNMATCHED'""",
        [run_id, artifact_id],
    ).fetchone()[0]
    curated = connection.execute(
        "SELECT count(*) FROM curated_redemptions WHERE run_id = ? AND artifact_id = ?",
        [run_id, artifact_id],
    ).fetchone()[0]
    return (read, counts.get("accepted", 0), counts.get("warning", 0),
            counts.get("quarantined", 0), read, unmatched, read, curated)


def _reconciliation_row_matches(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    artifact_id: str,
    scope: str,
    expected: tuple[int, ...],
) -> bool:
    actual = connection.execute(
        """SELECT read_count, accepted_count, warned_count, quarantined_count,
                  transformed_count, unmatched_count, raw_persisted_count,
                  curated_persisted_count FROM run_reconciliation
           WHERE run_id = ? AND artifact_id = ? AND scope = ?""",
        [run_id, artifact_id, scope],
    ).fetchone()
    return actual == expected


def _read_summary(
    connection: duckdb.DuckDBPyConnection,
    run_id: str,
    as_of_date: date,
    database_path: Path,
    reconciliation_ok: bool,
) -> DemoSummary:
    artifacts = connection.execute(
        """SELECT i.source_kind, r.original_filename, i.artifact_id,
                  CASE WHEN i.source_kind = 'member' THEN
                    (SELECT count(*) FROM raw_member_record m WHERE m.artifact_id = i.artifact_id)
                  ELSE (SELECT count(*) FROM raw_redemption_payload p
                        WHERE p.run_id = r.run_id AND p.artifact_id = i.artifact_id) END
           FROM run_input r JOIN input_artifact i USING (artifact_id)
           WHERE r.run_id = ? ORDER BY i.source_kind, r.original_filename""",
        [run_id],
    ).fetchall()
    artifact_lines = tuple(
        f"{kind}: {filename} artifact={artifact_id} records={count}"
        for kind, filename, artifact_id, count in artifacts
    )
    member_counts = connection.execute(
        """SELECT count(*) FILTER (WHERE disposition = 'accepted'),
                  count(*) FILTER (WHERE disposition = 'warning'),
                  count(*) FILTER (WHERE disposition = 'quarantined')
           FROM member_record_outcome WHERE run_id = ?""",
        [run_id],
    ).fetchone()
    country_counts = tuple((code, connection.execute(
        f"SELECT count(*) FROM members_{code.lower()} WHERE run_id = ?", [run_id],
    ).fetchone()[0]) for code in ("AUS", "IND", "USA"))
    parent_counts = connection.execute(
        """SELECT count(*) FILTER (WHERE disposition = 'accepted'),
                  count(*) FILTER (WHERE disposition = 'warning'),
                  count(*) FILTER (WHERE disposition = 'quarantined'), count(*)
           FROM raw_redemption_payload WHERE run_id = ?""",
        [run_id],
    ).fetchone()
    transaction_counts = connection.execute(
        """SELECT count(*) FILTER (WHERE t.disposition = 'accepted'),
                  count(*) FILTER (WHERE t.disposition = 'warning'),
                  count(*) FILTER (WHERE t.disposition = 'quarantined'), count(*),
                  count(*) FILTER (WHERE EXISTS (
                    SELECT 1 FROM redemption_issue i WHERE i.run_id = t.run_id
                      AND i.artifact_id = t.artifact_id AND i.payload_index = t.payload_index
                      AND i.transaction_index = t.transaction_index
                      AND i.rule_code = 'MEMBER_REFERENCE_UNMATCHED'))
           FROM redemption_transaction_outcome t WHERE t.run_id = ?""",
        [run_id],
    ).fetchone()
    persistence_counts = connection.execute(
        """SELECT
             (SELECT count(*) FROM raw_member_record m JOIN input_artifact a USING (artifact_id)
              JOIN run_input r USING (artifact_id) WHERE r.run_id = ? AND a.source_kind = 'member'),
             (SELECT count(*) FROM member_record_outcome WHERE run_id = ? AND disposition <> 'quarantined'),
             (SELECT count(*) FROM raw_redemption_payload WHERE run_id = ?),
             (SELECT count(*) FROM redemption_transaction_outcome WHERE run_id = ?),
             (SELECT count(*) FROM curated_redemptions WHERE run_id = ?)""",
        [run_id] * 5,
    ).fetchone()
    status = connection.execute(
        "SELECT status FROM pipeline_run WHERE run_id = ?", [run_id],
    ).fetchone()[0]
    return DemoSummary(
        run_id, status, as_of_date, database_path, artifact_lines,
        tuple(member_counts), country_counts, tuple(parent_counts),
        tuple(transaction_counts), tuple(persistence_counts), reconciliation_ok,
    )


def format_summary(summary: DemoSummary) -> str:
    """Format concise, database-derived run counts for terminal output."""
    member_accepted, member_warned, member_quarantined = summary.member_counts
    parent_accepted, parent_warned, parent_quarantined, parent_total = summary.redemption_parent_counts
    txn_accepted, txn_warned, txn_quarantined, txn_total, unmatched = summary.redemption_transaction_counts
    raw_members, curated_members, raw_parents, raw_transactions, curated_transactions = summary.persistence_counts
    lines = [
        f"Run: {summary.run_id} ({summary.status})",
        f"As of: {summary.as_of_date.isoformat()}",
        "Input artifacts:",
        *(f"  {line}" for line in summary.artifact_lines),
        f"Members accepted/warned/quarantined: {member_accepted}/{member_warned}/{member_quarantined}",
        "Country curated: " + ", ".join(f"{code}={count}" for code, count in summary.country_counts),
        f"Redemption parents accepted/warned/quarantined/total: {parent_accepted}/{parent_warned}/{parent_quarantined}/{parent_total}",
        f"Transactions accepted/warned/quarantined/unmatched/total: {txn_accepted}/{txn_warned}/{txn_quarantined}/{unmatched}/{txn_total}",
        f"Persisted raw members/curated members/raw parents/raw transactions/curated transactions: {raw_members}/{curated_members}/{raw_parents}/{raw_transactions}/{curated_transactions}",
        f"Reconciliation: {'OK' if summary.reconciliation_ok else 'MISMATCH'}",
        f"Database: {summary.database_path}",
    ]
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point; errors are concise and result in a non-zero status."""
    parser = argparse.ArgumentParser(prog="skypoints-data-engineering")
    subparsers = parser.add_subparsers(dest="command", required=True)
    demo = subparsers.add_parser("run-demo", help="run the complete local assessment demo")
    demo.add_argument("--as-of-date", required=True, type=date.fromisoformat, metavar="YYYY-MM-DD")
    demo.add_argument("--database", type=Path, default=_DEFAULT_DATABASE)
    demo.add_argument("--data-dir", type=Path, default=_DEFAULT_DATA_DIR)
    demo.add_argument("--redemption-fixture", type=Path, default=_DEFAULT_REDEMPTION_FIXTURE)
    args = parser.parse_args(argv)
    try:
        summary = run_demo(
            as_of_date=args.as_of_date,
            database_path=args.database,
            data_dir=args.data_dir,
            redemption_fixture=args.redemption_fixture,
        )
    except Exception as exc:
        print(f"SkyPoints demo failed: {exc}", file=sys.stderr)
        return 1
    print(format_summary(summary))
    return 0 if summary.status == "succeeded" and summary.reconciliation_ok else 1
