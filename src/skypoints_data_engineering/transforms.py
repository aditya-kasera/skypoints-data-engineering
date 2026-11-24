"""Deterministic member derivations and local demonstration country routing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Mapping

from .ingestion import RawRecord
from .source_dates import parse_source_date
from .validation import Disposition, RecordOutcome, ValidationResult


FILENAME_SAMPLE_ADAPTER = "FILENAME_SAMPLE_ADAPTER"
COUNTRY_BY_SOURCE_FILE: Mapping[str, str] = {
    "AUS.xlsx": "AUS",
    "IND.csv": "IND",
    "USA.csv": "USA",
}


@dataclass(frozen=True)
class TransformationIssue:
    """A transformation/routing warning retaining its source record."""

    record: RawRecord
    rule_code: str
    detail: str


@dataclass(frozen=True)
class CuratedRecord:
    """Derived fields plus original raw and validation lineage."""

    record: RawRecord
    validation_outcome: RecordOutcome
    age: int | None
    stale_member: bool | None
    country_code: str | None
    country_routing_method: str | None
    issues: tuple[TransformationIssue, ...]


@dataclass(frozen=True)
class TransformationResult:
    """Curated rows and all original validation outcomes for one batch."""

    validation_result: ValidationResult
    as_of_date: date
    curated_records: tuple[CuratedRecord, ...]
    issues: tuple[TransformationIssue, ...]

    @property
    def quarantined_outcomes(self) -> tuple[RecordOutcome, ...]:
        """Retain quarantined records while excluding them from curated rows."""

        return tuple(
            outcome for outcome in self.validation_result.outcomes
            if outcome.disposition is Disposition.QUARANTINED
        )


_DATE_FIELDS_BY_SOURCE: Mapping[str, tuple[str | None, str, str]] = {
    "AUS": ("Date of Birth", "Date of Flight", "iso"),
    "IND": ("DOB", "Flight Date", "slash"),
    "USA": (None, "FlightDate", "compact"),
}


def calculate_age(dob: date | None, *, as_of_date: date) -> int | None:
    """Calculate birthday-aware age using March 1 for non-leap Feb. 29 birthdays.

    The March 1 rule in non-leap years is a deterministic implementation
    assumption. It is not specified by the assessment and requires business
    confirmation. Future DOBs and missing DOBs produce ``None``.
    """

    _require_calendar_date(as_of_date, "as_of_date")
    if dob is None:
        return None
    _require_calendar_date(dob, "dob")
    if dob > as_of_date:
        return None
    try:
        birthday_this_year = date(as_of_date.year, dob.month, dob.day)
    except ValueError:
        # A Feb. 29 birthday in a non-leap year is observed on March 1 here.
        birthday_this_year = date(as_of_date.year, 3, 1)
    return as_of_date.year - dob.year - (as_of_date < birthday_this_year)


def calculate_stale_member(last_flight_date: date | None, *, as_of_date: date) -> bool | None:
    """Return true only beyond 90 days; missing, invalid, or future dates are null."""

    _require_calendar_date(as_of_date, "as_of_date")
    if last_flight_date is None:
        return None
    _require_calendar_date(last_flight_date, "last_flight_date")
    if last_flight_date > as_of_date:
        return None
    return (as_of_date - last_flight_date).days > 90


def transform_batch(
    validation_result: ValidationResult,
    *,
    as_of_date: date,
) -> TransformationResult:
    """Derive member fields and demo-route non-quarantined rows.

    The explicit date must match the date used for validation. The result keeps
    the complete original ``ValidationResult``; quarantined outcomes therefore
    remain traceable even though only non-quarantined rows are curated.
    """

    if not isinstance(as_of_date, date) or isinstance(as_of_date, datetime):
        raise TypeError("as_of_date must be an explicit datetime.date value")
    if validation_result.as_of_date != as_of_date:
        raise ValueError("as_of_date must match the date used by validate_batch")

    transformed: list[CuratedRecord] = []
    all_issues: list[TransformationIssue] = []
    for outcome in validation_result.outcomes:
        if outcome.disposition is Disposition.QUARANTINED:
            continue

        record = outcome.record
        try:
            dob_column, flight_column, date_format = _DATE_FIELDS_BY_SOURCE[record.source_code]
        except KeyError as exc:
            raise ValueError(f"Unsupported source code for transformations: {record.source_code}") from exc

        dob = _record_date(record, dob_column, date_format)
        last_flight = _record_date(record, flight_column, date_format)
        country_code, route_method, routing_issues = _route_record(record)
        all_issues.extend(routing_issues)
        transformed.append(
            CuratedRecord(
                record=record,
                validation_outcome=outcome,
                age=calculate_age(dob, as_of_date=as_of_date),
                stale_member=calculate_stale_member(last_flight, as_of_date=as_of_date),
                country_code=country_code,
                country_routing_method=route_method,
                issues=tuple(routing_issues),
            )
        )

    return TransformationResult(
        validation_result=validation_result,
        as_of_date=as_of_date,
        curated_records=tuple(transformed),
        issues=tuple(all_issues),
    )


def _record_date(record: RawRecord, column: str | None, date_format: str) -> date | None:
    if column is None or column not in record.raw_values:
        return None
    value = record.raw_values[column]
    if value is None or (isinstance(value, str) and value.strip().upper() in {"", "NULL"}):
        return None
    parsed, _reason = parse_source_date(value, date_format)
    return parsed


def _require_calendar_date(value: date, field_name: str) -> None:
    if not isinstance(value, date) or isinstance(value, datetime):
        raise TypeError(f"{field_name} must be a calendar date, not a datetime")


def _route_record(
    record: RawRecord,
) -> tuple[str | None, str | None, list[TransformationIssue]]:
    country_code = COUNTRY_BY_SOURCE_FILE.get(record.source_file)
    if country_code is None:
        issue = TransformationIssue(
            record,
            "COUNTRY_ROUTE_UNMAPPED_SOURCE",
            f"No local sample country route is configured for {record.source_file!r}.",
        )
        return None, None, [issue]
    if country_code != record.source_code:
        issue = TransformationIssue(
            record,
            "COUNTRY_ROUTE_SOURCE_MISMATCH",
            f"Filename {record.source_file!r} maps to {country_code}, but provenance source code is {record.source_code!r}.",
        )
        return None, None, [issue]
    return country_code, FILENAME_SAMPLE_ADAPTER, []
