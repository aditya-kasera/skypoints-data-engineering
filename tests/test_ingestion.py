from datetime import datetime, timedelta, timezone
import hashlib
from pathlib import Path

import pytest

from skypoints_data_engineering.ingestion import (
    IngestionError,
    ingest_active_sources,
    ingest_source_file,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA = REPOSITORY_ROOT / "data" / "raw"
FIXED_UTC = datetime(2026, 11, 24, 15, 30, tzinfo=timezone.utc)


def test_ingests_exact_active_inventory_with_complete_provenance() -> None:
    batches = ingest_active_sources(RAW_DATA, ingested_at_utc=FIXED_UTC)

    assert [(batch.source_file, batch.source_code) for batch in batches] == [
        ("AUS.xlsx", "AUS"),
        ("IND.csv", "IND"),
        ("USA.csv", "USA"),
    ]
    assert [len(batch.records) for batch in batches] == [3, 3, 3]

    for batch in batches:
        source_bytes = (RAW_DATA / batch.source_file).read_bytes()
        assert batch.source_sha256 == hashlib.sha256(source_bytes).hexdigest()
        assert batch.ingested_at_utc == FIXED_UTC
        assert batch.batch_id.startswith(f"member-{batch.source_code.lower()}-")
        assert len(batch.schema_fingerprint) == 64
        assert len(batch.schemas) >= 1
        assert [record.source_row_number for record in batch.records] == [1, 2, 3]
        for record in batch.records:
            assert record.source_file == batch.source_file
            assert record.source_code == batch.source_code
            assert record.batch_id == batch.batch_id
            assert record.source_sha256 == batch.source_sha256
            assert record.ingested_at_utc == FIXED_UTC
            assert len(record.schema_fingerprint) == 64

    assert batches[0].records[0].sheet_name == "Sheet1"
    assert batches[1].records[0].sheet_name is None
    assert batches[2].records[0].sheet_name is None


def test_raw_values_are_not_date_parsed_or_null_normalized() -> None:
    aus, ind, usa = ingest_active_sources(RAW_DATA, ingested_at_utc=FIXED_UTC)

    assert aus.records[0].raw_values["Date of Birth"] == "NULL"
    assert aus.records[1].raw_values["Date of Enrollment"] == "2021-13-13"
    assert ind.records[0].raw_values["DOB"] == "12/1/1998"
    assert usa.records[0].raw_values["EnrollmentDate"] == "6152022"
    assert usa.records[1].raw_values["EnrollmentDate"] == "1052022"
    assert usa.records[1].raw_values["FlightDate"] == "1152022"


def test_batch_id_is_content_stable_and_staged_filename_is_utc() -> None:
    path = RAW_DATA / "IND.csv"
    first = ingest_source_file(path, ingested_at_utc=FIXED_UTC)
    offset_timestamp = datetime(2026, 11, 24, 17, 30, tzinfo=timezone(timedelta(hours=2)))
    second = ingest_source_file(path, ingested_at_utc=offset_timestamp)

    assert first.batch_id == second.batch_id
    assert first.source_sha256 == second.source_sha256
    assert first.ingested_at_utc == second.ingested_at_utc == FIXED_UTC
    assert first.staged_filename == "member_IND_20261124T153000Z_" + first.source_sha256[:12] + ".csv"
    assert second.staged_filename == first.staged_filename


def test_ingestion_rejects_naive_operational_timestamps() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        ingest_active_sources(RAW_DATA, ingested_at_utc=datetime(2026, 11, 24, 15, 30))


def test_only_configured_active_filenames_are_supported(tmp_path: Path) -> None:
    unsupported = tmp_path / "IND.xlsx"
    unsupported.touch()

    with pytest.raises(IngestionError, match="Unsupported source filename"):
        ingest_source_file(unsupported, ingested_at_utc=FIXED_UTC)


def test_missing_active_file_fails_with_filename(tmp_path: Path) -> None:
    with pytest.raises(IngestionError, match="AUS.xlsx"):
        ingest_active_sources(tmp_path, ingested_at_utc=FIXED_UTC)
