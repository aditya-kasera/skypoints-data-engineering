from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from skypoints_data_engineering.ingestion import IngestionBatch, RawRecord, ingest_active_sources
from skypoints_data_engineering.transforms import (
    FILENAME_SAMPLE_ADAPTER,
    calculate_age,
    calculate_stale_member,
    transform_batch,
)
from skypoints_data_engineering.validation import Disposition, ValidationResult, validate_batch


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA = REPOSITORY_ROOT / "data" / "raw"
AS_OF_DATE = date(2026, 11, 24)


def test_age_respects_birthdays_on_before_and_after_reference_date() -> None:
    as_of = date(2026, 6, 15)

    assert calculate_age(date(1990, 6, 14), as_of_date=as_of) == 36
    assert calculate_age(date(1990, 6, 15), as_of_date=as_of) == 36
    assert calculate_age(date(1990, 6, 16), as_of_date=as_of) == 35


def test_age_handles_february_29_under_documented_march_first_assumption() -> None:
    leap_as_of_before = date(2024, 2, 28)
    leap_as_of_on = date(2024, 2, 29)
    non_leap_before = date(2023, 2, 28)
    non_leap_on_assumed_date = date(2023, 3, 1)

    assert calculate_age(date(2000, 2, 29), as_of_date=leap_as_of_before) == 23
    assert calculate_age(date(2000, 2, 29), as_of_date=leap_as_of_on) == 24
    assert calculate_age(date(2000, 2, 29), as_of_date=non_leap_before) == 22
    assert calculate_age(date(2000, 2, 29), as_of_date=non_leap_on_assumed_date) == 23
    assert calculate_age(None, as_of_date=non_leap_on_assumed_date) is None
    assert calculate_age(date(2030, 1, 1), as_of_date=AS_OF_DATE) is None
    with pytest.raises(TypeError, match="calendar date"):
        calculate_age(date(2000, 1, 1), as_of_date=datetime(2026, 11, 24))


@pytest.mark.parametrize(
    ("days_since_flight", "expected"),
    [(89, False), (90, False), (91, True)],
)
def test_stale_member_uses_strict_greater_than_90_days(
    days_since_flight: int,
    expected: bool,
) -> None:
    flight_date = AS_OF_DATE - timedelta(days=days_since_flight)
    assert calculate_stale_member(flight_date, as_of_date=AS_OF_DATE) is expected


def test_stale_member_returns_none_for_missing_or_future_dates() -> None:
    assert calculate_stale_member(None, as_of_date=AS_OF_DATE) is None
    assert calculate_stale_member(AS_OF_DATE + timedelta(days=1), as_of_date=AS_OF_DATE) is None


def _single_record_result(
    batch: IngestionBatch,
    changes: dict[str, object],
) -> tuple[RawRecord, ValidationResult]:
    original = batch.records[0]
    changed = replace(original, raw_values={**original.raw_values, **changes})
    one_row_batch = replace(batch, records=(changed,))
    return changed, validate_batch(one_row_batch, as_of_date=AS_OF_DATE)


def test_missing_and_invalid_dob_or_flight_produce_null_derivations() -> None:
    aus, ind, usa = ingest_active_sources(RAW_DATA)

    aus_result = validate_batch(aus, as_of_date=AS_OF_DATE)
    aus_output = transform_batch(aus_result, as_of_date=AS_OF_DATE)
    missing_dob = next(row for row in aus_output.curated_records if row.record.source_row_number == 1)
    assert missing_dob.age is None  # AUS literal NULL DOB

    _usa_record, usa_result = _single_record_result(usa, {})
    usa_output = transform_batch(usa_result, as_of_date=AS_OF_DATE)
    assert usa_output.curated_records[0].age is None  # DOB column absent from USA

    _bad_dob, bad_dob_result = _single_record_result(ind, {"DOB": "not-a-date"})
    bad_dob_output = transform_batch(bad_dob_result, as_of_date=AS_OF_DATE)
    assert bad_dob_output.curated_records[0].age is None

    _missing_flight, missing_flight_result = _single_record_result(ind, {"Flight Date": ""})
    missing_flight_output = transform_batch(missing_flight_result, as_of_date=AS_OF_DATE)
    assert missing_flight_output.curated_records[0].stale_member is None

    _bad_flight, bad_flight_result = _single_record_result(ind, {"Flight Date": "2022-99-99"})
    bad_flight_output = transform_batch(bad_flight_result, as_of_date=AS_OF_DATE)
    assert bad_flight_output.curated_records[0].stale_member is None


def test_future_flight_warning_is_retained_and_stale_is_null() -> None:
    _aus, ind, _usa = ingest_active_sources(RAW_DATA)
    future_date = AS_OF_DATE + timedelta(days=1)
    future = f"{future_date.month}/{future_date.day}/{future_date.year}"
    _record, validation_result = _single_record_result(ind, {"Flight Date": future})

    output = transform_batch(validation_result, as_of_date=AS_OF_DATE)
    curated = output.curated_records[0]
    assert curated.stale_member is None
    future_issue = next(
        issue for issue in curated.validation_outcome.issues
        if issue.rule_code == "FUTURE_MEMBER_DATE"
    )
    assert future_issue.record is curated.record
    assert future_issue.field_name == "Flight Date"


def test_country_routing_uses_only_exact_filename_mappings_with_provenance() -> None:
    batches = ingest_active_sources(RAW_DATA)
    routed: dict[str, str] = {}

    for batch in batches:
        result = transform_batch(validate_batch(batch, as_of_date=AS_OF_DATE), as_of_date=AS_OF_DATE)
        for row in result.curated_records:
            routed[row.record.source_file] = row.country_code
            assert row.country_routing_method == FILENAME_SAMPLE_ADAPTER
            assert row.record.source_code == row.country_code

    assert routed == {"AUS.xlsx": "AUS", "IND.csv": "IND", "USA.csv": "USA"}


def test_unknown_filename_is_not_routed_and_emits_provenance_linked_issue() -> None:
    _aus, ind, _usa = ingest_active_sources(RAW_DATA)
    original = ind.records[0]
    unknown_file_record = replace(original, source_file="MEX.csv")
    batch_result = validate_batch(replace(ind, records=(unknown_file_record,)), as_of_date=AS_OF_DATE)

    output = transform_batch(batch_result, as_of_date=AS_OF_DATE)
    assert output.curated_records[0].country_code is None
    assert output.curated_records[0].country_routing_method is None
    issue = output.issues[0]
    assert issue.rule_code == "COUNTRY_ROUTE_UNMAPPED_SOURCE"
    assert issue.record is unknown_file_record


def test_quarantined_records_are_excluded_but_raw_and_validation_lineage_remain() -> None:
    _aus, _ind, usa = ingest_active_sources(RAW_DATA)
    validation_result = validate_batch(usa, as_of_date=AS_OF_DATE)
    output = transform_batch(validation_result, as_of_date=AS_OF_DATE)

    assert output.validation_result is validation_result
    assert len(output.quarantined_outcomes) == 1
    quarantined = output.quarantined_outcomes[0]
    assert quarantined.disposition is Disposition.QUARANTINED
    assert quarantined.record in usa.records
    assert all(row.record is not quarantined.record for row in output.curated_records)
    assert len(output.curated_records) + len(output.quarantined_outcomes) == len(usa.records)


def test_warning_outcomes_and_raw_values_are_retained_without_mutation() -> None:
    aus, _ind, _usa = ingest_active_sources(RAW_DATA)
    original = aus.records[0]
    raw_values_before = dict(original.raw_values)
    validation_result = validate_batch(aus, as_of_date=AS_OF_DATE)
    output = transform_batch(validation_result, as_of_date=AS_OF_DATE)

    warning_row = next(row for row in output.curated_records if row.record is original)
    assert warning_row.validation_outcome.disposition is Disposition.WARNING
    assert warning_row.validation_outcome.issues
    assert original.raw_values == raw_values_before
    assert warning_row.record is original


def test_transform_is_deterministic_and_requires_validation_reference_date() -> None:
    _aus, ind, _usa = ingest_active_sources(RAW_DATA)
    validation_result = validate_batch(ind, as_of_date=AS_OF_DATE)

    first = transform_batch(validation_result, as_of_date=AS_OF_DATE)
    second = transform_batch(validation_result, as_of_date=AS_OF_DATE)
    assert first == second
    with pytest.raises(ValueError, match="match the date used"):
        transform_batch(validation_result, as_of_date=AS_OF_DATE + timedelta(days=1))
    with pytest.raises(TypeError, match="explicit datetime.date"):
        transform_batch(validation_result, as_of_date=datetime(2026, 11, 24))
