from datetime import date
import hashlib
from pathlib import Path
import re

import duckdb

from skypoints_data_engineering.runner import main, run_demo


ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
FIXTURE = ROOT / "tests" / "fixtures" / "redemptions_assessment_example.json"
AS_OF = date(2024, 1, 15)


def _raw_hashes() -> dict[str, str]:
    return {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(RAW.iterdir()) if path.is_file()
    }


def test_runner_executes_complete_pipeline_and_repeat_creates_new_run_history(tmp_path, capsys) -> None:
    database = tmp_path / "demo.duckdb"
    before = _raw_hashes()
    args = [
        "run-demo", "--as-of-date", AS_OF.isoformat(), "--database", str(database),
        "--data-dir", str(RAW), "--redemption-fixture", str(FIXTURE),
    ]

    assert main(args) == 0
    first_output = capsys.readouterr().out
    first_run = re.search(r"Run: (demo-[0-9a-f]+) \(succeeded\)", first_output).group(1)
    assert "AUS.xlsx" in first_output and "IND.csv" in first_output and "USA.csv" in first_output
    assert "Transactions accepted/warned/quarantined/unmatched/total: 0/2/0/2/2" in first_output
    assert "Reconciliation: OK" in first_output
    assert str(database.resolve()) in first_output

    assert main(args) == 0
    second_output = capsys.readouterr().out
    second_run = re.search(r"Run: (demo-[0-9a-f]+) \(succeeded\)", second_output).group(1)
    assert second_run != first_run
    assert _raw_hashes() == before

    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute(
            "SELECT count(*) FROM pipeline_run WHERE status = 'succeeded' AND as_of_date = ?",
            [AS_OF],
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT count(*) FROM run_input WHERE run_id = ? AND input_status = 'succeeded'",
            [first_run],
        ).fetchone() == (4,)
        assert connection.execute(
            "SELECT count(*) FROM member_record_outcome WHERE run_id = ?",
            [first_run],
        ).fetchone() == (9,)
        assert connection.execute(
            "SELECT count(*) FROM raw_member_record",
        ).fetchone() == (9,)
        assert connection.execute(
            "SELECT count(*) FROM members_aus WHERE run_id = ?", [first_run],
        ).fetchone()[0] > 0
        assert connection.execute(
            "SELECT count(*) FROM members_ind WHERE run_id = ?", [first_run],
        ).fetchone()[0] > 0
        assert connection.execute(
            "SELECT count(*) FROM members_usa WHERE run_id = ?", [first_run],
        ).fetchone()[0] > 0
        assert connection.execute(
            "SELECT count(*) FROM raw_redemption_payload WHERE run_id = ?", [first_run],
        ).fetchone() == (1,)
        assert connection.execute(
            "SELECT count(*) FROM redemption_transaction_outcome WHERE run_id = ?", [first_run],
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT count(*) FROM curated_redemptions WHERE run_id = ?", [first_run],
        ).fetchone() == (2,)
        assert connection.execute(
            "SELECT count(*) FROM run_reconciliation WHERE run_id = ?", [first_run],
        ).fetchone() == (5,)


def test_run_demo_uses_explicit_reference_date(tmp_path) -> None:
    summary = run_demo(
        as_of_date=date(2020, 2, 29), database_path=tmp_path / "explicit.duckdb",
        data_dir=RAW, redemption_fixture=FIXTURE,
    )
    assert summary.as_of_date == date(2020, 2, 29)
    assert summary.status == "succeeded"
    assert summary.reconciliation_ok
    with duckdb.connect(str(summary.database_path), read_only=True) as connection:
        assert connection.execute(
            "SELECT as_of_date FROM pipeline_run WHERE run_id = ?", [summary.run_id],
        ).fetchone() == (date(2020, 2, 29),)


def test_cli_failure_returns_nonzero_and_records_failed_run(tmp_path, capsys) -> None:
    database = tmp_path / "failure.duckdb"
    result = main([
        "run-demo", "--as-of-date", "2024-01-15", "--database", str(database),
        "--data-dir", str(tmp_path / "missing-inputs"), "--redemption-fixture", str(FIXTURE),
    ])
    captured = capsys.readouterr()
    assert result == 1
    assert "SkyPoints demo failed:" in captured.err
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute(
            "SELECT status FROM pipeline_run",
        ).fetchone() == ("failed",)


def test_cli_requires_iso_as_of_date(tmp_path, capsys) -> None:
    import pytest

    with pytest.raises(SystemExit) as raised:
        main([
            "run-demo", "--as-of-date", "20240115", "--database", str(tmp_path / "bad-date.duckdb"),
        ])
    assert raised.value.code == 2
    assert "YYYY-MM-DD" in capsys.readouterr().err
