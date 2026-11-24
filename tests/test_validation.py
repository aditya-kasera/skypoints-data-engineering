from dataclasses import replace
from datetime import date
from pathlib import Path

from skypoints_data_engineering.ingestion import ingest_active_sources
from skypoints_data_engineering.validation import (
    Disposition,
    IssueSeverity,
    validate_batch,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA = REPOSITORY_ROOT / "data" / "raw"
AS_OF_DATE = date(2026, 11, 24)


def test_validation_returns_one_provenance_linked_outcome_per_raw_record() -> None:
    batches = ingest_active_sources(RAW_DATA)

    for batch in batches:
        result = validate_batch(batch, as_of_date=AS_OF_DATE)
        assert result.batch_id == batch.batch_id
        assert len(result.outcomes) == len(batch.records)
        assert all(outcome.record is raw for outcome, raw in zip(result.outcomes, batch.records))
        for issue in result.issues:
            assert issue.record in batch.records
            assert issue.record.source_file == batch.source_file
            assert issue.record.source_sha256 == batch.source_sha256
            assert issue.record.batch_id == batch.batch_id


def test_required_invalid_and_ambiguous_enrollment_dates_are_quarantined() -> None:
    aus, _ind, usa = ingest_active_sources(RAW_DATA)

    aus_result = validate_batch(aus, as_of_date=AS_OF_DATE)
    assert aus_result.outcomes[1].disposition is Disposition.QUARANTINED
    assert "INVALID_DATE" in {issue.rule_code for issue in aus_result.outcomes[1].issues}

    usa_result = validate_batch(usa, as_of_date=AS_OF_DATE)
    assert usa_result.outcomes[1].disposition is Disposition.QUARANTINED
    ambiguous_issue = next(
        issue for issue in usa_result.outcomes[1].issues
        if issue.rule_code == "AMBIGUOUS_REQUIRED_DATE"
    )
    assert ambiguous_issue.severity is IssueSeverity.ERROR
    assert ambiguous_issue.record.raw_values["EnrollmentDate"] == "1052022"
    assert sum(outcome.disposition is Disposition.QUARANTINED for outcome in usa_result.outcomes) == 1


def test_optional_date_issues_warn_and_source_absence_is_not_a_null_value() -> None:
    aus, _ind, usa = ingest_active_sources(RAW_DATA)

    aus_result = validate_batch(aus, as_of_date=AS_OF_DATE)
    assert aus_result.outcomes[0].disposition is Disposition.WARNING
    assert "OPTIONAL_DATE_MISSING" in {issue.rule_code for issue in aus_result.outcomes[0].issues}

    usa_result = validate_batch(usa, as_of_date=AS_OF_DATE)
    assert not any(issue.field_name in {"DOB", "Date of Birth"} for issue in usa_result.issues)
    assert "Date of Birth" not in usa.records[0].raw_values


def test_duplicate_keys_are_reported_only_within_the_supplied_batch() -> None:
    aus, ind, usa = ingest_active_sources(RAW_DATA)
    repeated_ind_records = tuple(
        replace(record, raw_values={**record.raw_values, "Name": "Same Name", "ID": "shared-local-id"})
        for record in ind.records[:2]
    )
    repeated_ind = replace(ind, records=repeated_ind_records)

    same_batch_result = validate_batch(repeated_ind, as_of_date=AS_OF_DATE)
    duplicate_names = [
        issue for issue in same_batch_result.issues
        if issue.rule_code == "DUPLICATE_MEMBER_NAME"
    ]
    assert len(duplicate_names) == 2
    assert all(issue.severity is IssueSeverity.ERROR for issue in duplicate_names)
    duplicate_ids = [
        issue for issue in same_batch_result.issues
        if issue.rule_code == "DUPLICATE_SOURCE_MEMBER_ID"
    ]
    assert len(duplicate_ids) == 2
    assert all(issue.severity is IssueSeverity.WARNING for issue in duplicate_ids)
    assert all(outcome.disposition is Disposition.QUARANTINED for outcome in same_batch_result.outcomes)
    assert len(same_batch_result.outcomes) == len(repeated_ind.records)
    assert all(outcome.record in repeated_ind.records for outcome in same_batch_result.outcomes)

    repeated_id_records = tuple(
        replace(record, raw_values={**record.raw_values, "ID": "shared-local-id"})
        for record in ind.records[:2]
    )
    repeated_id_result = validate_batch(
        replace(ind, records=repeated_id_records), as_of_date=AS_OF_DATE,
    )
    assert all(outcome.disposition is Disposition.WARNING for outcome in repeated_id_result.outcomes)
    assert {issue.rule_code for issue in repeated_id_result.issues} == {"DUPLICATE_SOURCE_MEMBER_ID"}

    # Mike occurs in both files, but one batch is validated at a time and no
    # cross-source identity or name uniqueness comparison is performed.
    aus_mike = replace(aus, records=(aus.records[0],))
    usa_mike = replace(usa, records=(usa.records[2],))
    for batch in (replace(aus_mike, records=(aus_mike.records[0],)), usa_mike):
        result = validate_batch(batch, as_of_date=AS_OF_DATE)
        assert not any(issue.rule_code == "DUPLICATE_MEMBER_NAME" for issue in result.issues)


def test_absent_and_blank_required_values_are_distinguished_and_retained() -> None:
    _aus, ind, _usa = ingest_active_sources(RAW_DATA)
    source = ind.records[0]
    absent_values = dict(source.raw_values)
    del absent_values["ID"]
    absent_values["Name"] = "   "
    synthetic_record = replace(source, raw_values=absent_values)
    synthetic_batch = replace(ind, records=(synthetic_record,))

    outcome = validate_batch(synthetic_batch, as_of_date=AS_OF_DATE).outcomes[0]
    assert outcome.disposition is Disposition.QUARANTINED
    assert outcome.record is synthetic_record
    issue_codes = {issue.rule_code for issue in outcome.issues}
    assert "REQUIRED_COLUMN_ABSENT" in issue_codes
    assert "MEMBER_NAME_REQUIRED" in issue_codes
    assert "ID" not in outcome.record.raw_values
    assert outcome.record.raw_values["Name"] == "   "


def test_blank_required_enrollment_value_has_one_quarantine_issue() -> None:
    _aus, ind, _usa = ingest_active_sources(RAW_DATA)
    record = ind.records[0]
    synthetic_record = replace(
        record,
        raw_values={**record.raw_values, "EnrollmentDate": ""},
    )
    outcome = validate_batch(
        replace(ind, records=(synthetic_record,)), as_of_date=AS_OF_DATE,
    ).outcomes[0]

    enrollment_issues = [issue for issue in outcome.issues if issue.field_name == "EnrollmentDate"]
    assert outcome.disposition is Disposition.QUARANTINED
    assert len(enrollment_issues) == 1
    assert enrollment_issues[0].rule_code == "ENROLLMENT_DATE_REQUIRED"


def test_overlength_required_field_is_quarantined_without_mutating_raw_value() -> None:
    _aus, ind, _usa = ingest_active_sources(RAW_DATA)
    record = ind.records[0]
    long_identifier = "x" * 19
    synthetic_record = replace(record, raw_values={**record.raw_values, "ID": long_identifier})
    synthetic_batch = replace(ind, records=(synthetic_record,))

    outcome = validate_batch(synthetic_batch, as_of_date=AS_OF_DATE).outcomes[0]
    assert outcome.disposition is Disposition.QUARANTINED
    assert any(issue.rule_code == "FIELD_TOO_LONG" and issue.field_name == "ID" for issue in outcome.issues)
    assert outcome.record.raw_values["ID"] == long_identifier


def test_future_dob_quarantines_but_future_enrollment_and_flight_warn() -> None:
    _aus, ind, _usa = ingest_active_sources(RAW_DATA)
    record = ind.records[0]

    future_dob = replace(record, raw_values={**record.raw_values, "DOB": "12/1/2030"})
    dob_outcome = validate_batch(
        replace(ind, records=(future_dob,)), as_of_date=AS_OF_DATE,
    ).outcomes[0]
    assert dob_outcome.disposition is Disposition.QUARANTINED
    assert "FUTURE_DOB" in {issue.rule_code for issue in dob_outcome.issues}
    assert dob_outcome.record.raw_values["DOB"] == "12/1/2030"

    future_member_dates = replace(
        record,
        raw_values={
            **record.raw_values,
            "EnrollmentDate": "12/2/2030",
            "Flight Date": "12/3/2030",
        },
    )
    date_outcome = validate_batch(
        replace(ind, records=(future_member_dates,)), as_of_date=AS_OF_DATE,
    ).outcomes[0]
    assert date_outcome.disposition is Disposition.WARNING
    assert sum(issue.rule_code == "FUTURE_MEMBER_DATE" for issue in date_outcome.issues) == 2
