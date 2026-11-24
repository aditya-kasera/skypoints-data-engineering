"""Read the three active member source files without standardizing their values."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Mapping

import pandas as pd


ACTIVE_SOURCE_CODES: Mapping[str, str] = {
    "AUS.xlsx": "AUS",
    "IND.csv": "IND",
    "USA.csv": "USA",
}


class IngestionError(RuntimeError):
    """Raised when an active source cannot be read as a raw batch."""


@dataclass(frozen=True)
class SourceSchema:
    """Ordered source columns and their stable schema fingerprint."""

    sheet_name: str | None
    columns: tuple[str, ...]
    schema_fingerprint: str


@dataclass(frozen=True)
class RawRecord:
    """A source record with enough metadata to locate its original value."""

    source_code: str
    source_file: str
    source_row_number: int
    sheet_name: str | None
    source_sha256: str
    batch_id: str
    schema_fingerprint: str
    ingested_at_utc: datetime
    raw_values: Mapping[str, object]


@dataclass(frozen=True)
class IngestionBatch:
    """An in-memory raw batch; ingestion does not write or alter source files."""

    source_code: str
    source_file: str
    source_sha256: str
    batch_id: str
    ingested_at_utc: datetime
    schemas: tuple[SourceSchema, ...]
    records: tuple[RawRecord, ...]

    @property
    def schema_fingerprint(self) -> str:
        """Fingerprint the ordered schemas in this file (one per workbook sheet)."""

        payload = [
            {
                "sheet_name": schema.sheet_name,
                "columns": schema.columns,
                "schema_fingerprint": schema.schema_fingerprint,
            }
            for schema in self.schemas
        ]
        return _sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))

    @property
    def staged_filename(self) -> str:
        """Return the proposed staged name without renaming or writing a file."""

        timestamp = self.ingested_at_utc.strftime("%Y%m%dT%H%M%SZ")
        extension = Path(self.source_file).suffix.lower()
        return f"member_{self.source_code}_{timestamp}_{self.source_sha256[:12]}{extension}"


def ingest_active_sources(
    data_dir: str | Path,
    *,
    ingested_at_utc: datetime | None = None,
) -> tuple[IngestionBatch, ...]:
    """Read exactly the three configured active inputs in deterministic order.

    ``source_row_number`` is a one-based data-record ordinal, excluding the
    header row. The optional timestamp is intended for reproducible callers and
    tests; when omitted, a timezone-aware UTC timestamp is generated once for
    the complete ingestion call.
    """

    directory = Path(data_dir)
    timestamp = _as_utc(ingested_at_utc or datetime.now(timezone.utc))
    missing = [name for name in ACTIVE_SOURCE_CODES if not (directory / name).is_file()]
    if missing:
        raise IngestionError(f"Missing active source file(s) in {directory}: {', '.join(missing)}")

    return tuple(
        ingest_source_file(directory / file_name, ingested_at_utc=timestamp)
        for file_name in ACTIVE_SOURCE_CODES
    )


def ingest_source_file(
    source_path: str | Path,
    *,
    ingested_at_utc: datetime | None = None,
) -> IngestionBatch:
    """Read one active source using its format-specific raw adapter.

    Values and source headers are retained as read. This function does not
    parse dates, rename fields, assign country, or apply validation rules.
    """

    path = Path(source_path)
    source_file = path.name
    source_code = ACTIVE_SOURCE_CODES.get(source_file)
    if source_code is None:
        raise IngestionError(f"Unsupported source filename: {source_file}")
    if not path.is_file():
        raise IngestionError(f"Source file does not exist: {path}")

    timestamp = _as_utc(ingested_at_utc or datetime.now(timezone.utc))
    source_sha256 = _hash_file(path)
    batch_id = _batch_id(source_code, source_sha256)

    try:
        if path.suffix.lower() == ".csv":
            schemas, records = _read_csv(path)
        else:
            schemas, records = _read_workbook(path)
    except Exception as exc:
        if isinstance(exc, IngestionError):
            raise
        raise IngestionError(f"Unable to read {source_file}: {exc}") from exc

    raw_records = tuple(
        RawRecord(
            source_code=source_code,
            source_file=source_file,
            source_row_number=row_number,
            sheet_name=sheet_name,
            source_sha256=source_sha256,
            batch_id=batch_id,
            schema_fingerprint=schema_fingerprint,
            ingested_at_utc=timestamp,
            raw_values=values,
        )
        for sheet_name, row_number, schema_fingerprint, values in records
    )
    return IngestionBatch(
        source_code=source_code,
        source_file=source_file,
        source_sha256=source_sha256,
        batch_id=batch_id,
        ingested_at_utc=timestamp,
        schemas=tuple(schemas),
        records=raw_records,
    )


def _read_csv(path: Path) -> tuple[list[SourceSchema], list[tuple[str | None, int, str, dict[str, object]]]]:
    frame = pd.read_csv(
        path,
        dtype=str,
        encoding="utf-8-sig",
        keep_default_na=False,
        skip_blank_lines=False,
    )
    columns = tuple(str(column) for column in frame.columns)
    schema_fingerprint = _fingerprint_schema(None, columns)
    records = [
        (None, row_number, schema_fingerprint, dict(zip(columns, values)))
        for row_number, values in enumerate(frame.itertuples(index=False, name=None), start=1)
    ]
    return [SourceSchema(None, columns, schema_fingerprint)], records


def _read_workbook(
    path: Path,
) -> tuple[list[SourceSchema], list[tuple[str | None, int, str, dict[str, object]]]]:
    schemas: list[SourceSchema] = []
    records: list[tuple[str | None, int, str, dict[str, object]]] = []
    with pd.ExcelFile(path) as workbook:
        for sheet_name in workbook.sheet_names:
            frame = pd.read_excel(
                workbook,
                sheet_name=sheet_name,
                dtype=object,
                keep_default_na=False,
            )
            columns = tuple(str(column) for column in frame.columns)
            schema_fingerprint = _fingerprint_schema(sheet_name, columns)
            schemas.append(SourceSchema(sheet_name, columns, schema_fingerprint))
            records.extend(
                (sheet_name, row_number, schema_fingerprint, dict(zip(columns, values)))
                for row_number, values in enumerate(frame.itertuples(index=False, name=None), start=1)
            )
    return schemas, records


def _fingerprint_schema(sheet_name: str | None, columns: tuple[str, ...]) -> str:
    payload = {"sheet_name": sheet_name, "columns": columns}
    return _sha256(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _batch_id(source_code: str, source_sha256: str) -> str:
    digest = _sha256(f"{source_code}\0{source_sha256}".encode("utf-8"))
    return f"member-{source_code.lower()}-{digest}"


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("ingested_at_utc must be timezone-aware")
    return value.astimezone(timezone.utc)
