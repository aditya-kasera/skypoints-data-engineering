"""Validate raw source batches without changing or discarding their records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Mapping

from .ingestion import IngestionBatch, RawRecord
from .source_dates import parse_source_date


class Disposition(str, Enum):
    """Validation disposition for one source record."""

    ACCEPTED = "accepted"
    WARNING = "warning"
    QUARANTINED = "quarantined"


class IssueSeverity(str, Enum):
    """Severity used to derive a record's validation disposition."""

    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class ValidationIssue:
    """A rule finding tied to the untouched source record and its provenance."""

    record: RawRecord
    rule_code: str
    severity: IssueSeverity
    detail: str
    field_name: str | None = None


@dataclass(frozen=True)
class RecordOutcome:
    """Disposition and findings for one original raw record."""

    record: RawRecord
    disposition: Disposition
    issues: tuple[ValidationIssue, ...]


@dataclass(frozen=True)
class ValidationResult:
    """All row outcomes and issues for one input batch."""

    batch_id: str
    as_of_date: date
    outcomes: tuple[RecordOutcome, ...]

    @property
    def issues(self) -> tuple[ValidationIssue, ...]:
        """Return issues in source-record and rule-evaluation order."""

        return tuple(issue for outcome in self.outcomes for issue in outcome.issues)


@dataclass(frozen=True)
class _SourceFields:
    member_name: str
    member_id: str
    enrollment_date: str
    flight_date: str
    date_of_birth: str | None
    tier_code: str
    date_format: str


_FIELDS_BY_SOURCE: Mapping[str, _SourceFields] = {
    "AUS": _SourceFields(
        "Member Name", "Unique ID", "Date of Enrollment", "Date of Flight",
        "Date of Birth", "Tier Type", "iso",
    ),
    "IND": _SourceFields(
        "Name", "ID", "EnrollmentDate", "Flight Date", "DOB", "TierCode", "slash",
    ),
    "USA": _SourceFields(
        "Name", "ID", "EnrollmentDate", "FlightDate", None, "TierCode", "compact",
    ),
}


def validate_batch(batch: IngestionBatch, *, as_of_date: date) -> ValidationResult:
    """Validate required fields, scoped key anomalies, and source date values.

    ``as_of_date`` is mandatory so future-date checks never depend on the
    machine clock. Duplicate Member Name and source ID checks are confined to
    this one batch: duplicate names are quarantined, while duplicate source IDs
    are warnings. The function returns every input record, retaining its
    original values and provenance in both outcome and issue objects.
    """

    if not isinstance(as_of_date, date) or isinstance(as_of_date, datetime):
        raise TypeError("as_of_date must be an explicit datetime.date value")

    try:
        fields = _FIELDS_BY_SOURCE[batch.source_code]
    except KeyError as exc:
        raise ValueError(f"Unsupported source code for validation: {batch.source_code}") from exc

    issues_by_index: list[list[ValidationIssue]] = [[] for _ in batch.records]
    for index, record in enumerate(batch.records):
        _validate_record(record, fields, as_of_date, issues_by_index[index])

    _add_duplicate_issues(
        batch.records, fields.member_id, "DUPLICATE_SOURCE_MEMBER_ID",
        IssueSeverity.WARNING, issues_by_index,
    )
    _add_duplicate_issues(
        batch.records, fields.member_name, "DUPLICATE_MEMBER_NAME",
        IssueSeverity.ERROR, issues_by_index,
    )

    outcomes = []
    for record, issues in zip(batch.records, issues_by_index):
        if any(issue.severity is IssueSeverity.ERROR for issue in issues):
            disposition = Disposition.QUARANTINED
        elif issues:
            disposition = Disposition.WARNING
        else:
            disposition = Disposition.ACCEPTED
        outcomes.append(RecordOutcome(record, disposition, tuple(issues)))

    return ValidationResult(batch.batch_id, as_of_date, tuple(outcomes))


def _validate_record(
    record: RawRecord,
    fields: _SourceFields,
    as_of_date: date,
    issues: list[ValidationIssue],
) -> None:
    _validate_required(record, fields.member_name, "MEMBER_NAME_REQUIRED", issues)
    _validate_required(record, fields.member_id, "MEMBER_ID_REQUIRED", issues)
    _validate_required(record, fields.enrollment_date, "ENROLLMENT_DATE_REQUIRED", issues)

    _validate_length(record, fields.member_name, 255, issues)
    _validate_length(record, fields.member_id, 18, issues)
    _validate_length(record, fields.tier_code, 5, issues)

    for column, field_name, required in (
        (fields.enrollment_date, "enrollment_date", True),
        (fields.flight_date, "flight_date", False),
        (fields.date_of_birth, "date_of_birth", False),
    ):
        if column is None or column not in record.raw_values:
            # Required column absence was reported by _validate_required.
            # Optional columns absent from a source schema are not null values.
            continue

        value = record.raw_values[column]
        if _is_missing(value):
            if required:
                # _validate_required already emitted the row-level missing-value issue.
                continue
            else:
                _add_issue(
                    record, issues, "OPTIONAL_DATE_MISSING", IssueSeverity.WARNING,
                    f"Optional {field_name} is blank.", column,
                )
            continue

        if field_name == "date_of_birth" and isinstance(value, str) and value.strip().upper() == "NULL":
            _add_issue(
                record, issues, "OPTIONAL_DATE_MISSING", IssueSeverity.WARNING,
                "DOB contains the source literal NULL marker.", column,
            )
            continue

        parsed, reason = parse_source_date(value, fields.date_format)
        if parsed is None:
            severity = IssueSeverity.ERROR if required else IssueSeverity.WARNING
            code = "AMBIGUOUS_REQUIRED_DATE" if reason == "ambiguous" and required else (
                "AMBIGUOUS_OPTIONAL_DATE" if reason == "ambiguous" else "INVALID_DATE"
            )
            _add_issue(
                record, issues, code, severity,
                f"{field_name} value {value!r} is {reason} under the documented {record.source_code} adapter format.",
                column,
            )
        elif parsed > as_of_date:
            if field_name == "date_of_birth":
                _add_issue(
                    record, issues, "FUTURE_DOB", IssueSeverity.ERROR,
                    f"Date of birth {parsed.isoformat()} is after as_of_date {as_of_date.isoformat()}.",
                    column,
                )
            else:
                _add_issue(
                    record, issues, "FUTURE_MEMBER_DATE", IssueSeverity.WARNING,
                    f"{field_name} {parsed.isoformat()} is after as_of_date {as_of_date.isoformat()}; flagged pending source-owner confirmation.",
                    column,
                )


def _validate_required(
    record: RawRecord,
    column: str,
    rule_code: str,
    issues: list[ValidationIssue],
) -> None:
    if column not in record.raw_values:
        _add_issue(
            record, issues, "REQUIRED_COLUMN_ABSENT", IssueSeverity.ERROR,
            f"Required source column {column!r} is absent.", column,
        )
    elif _is_missing(record.raw_values[column]):
        _add_issue(
            record, issues, rule_code, IssueSeverity.ERROR,
            f"Required field {column!r} is blank.", column,
        )


def _validate_length(
    record: RawRecord,
    column: str,
    maximum: int,
    issues: list[ValidationIssue],
) -> None:
    if column not in record.raw_values or _is_missing(record.raw_values[column]):
        return
    value = _as_text(record.raw_values[column])
    if len(value) > maximum:
        _add_issue(
            record, issues, "FIELD_TOO_LONG", IssueSeverity.ERROR,
            f"Field {column!r} has length {len(value)}; maximum is {maximum}.", column,
        )


def _add_duplicate_issues(
    records: tuple[RawRecord, ...],
    column: str,
    rule_code: str,
    severity: IssueSeverity,
    issues_by_index: list[list[ValidationIssue]],
) -> None:
    grouped: dict[str, list[int]] = {}
    for index, record in enumerate(records):
        if column not in record.raw_values or _is_missing(record.raw_values[column]):
            continue
        value = _as_text(record.raw_values[column])
        grouped.setdefault(value, []).append(index)

    for value, indexes in grouped.items():
        if len(indexes) < 2:
            continue
        for index in indexes:
            record = records[index]
            _add_issue(
                record, issues_by_index[index], rule_code, severity,
                f"Value {value!r} occurs {len(indexes)} times in this source batch.", column,
            )


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    try:
        # Handles NaN/NaT from tabular readers without adding a dependency.
        missing = value != value
        return bool(missing)
    except (TypeError, ValueError):
        return False


def _as_text(value: object) -> str:
    return str(value)


def _add_issue(
    record: RawRecord,
    issues: list[ValidationIssue],
    rule_code: str,
    severity: IssueSeverity,
    detail: str,
    field_name: str | None,
) -> None:
    issues.append(ValidationIssue(record, rule_code, severity, detail, field_name))
