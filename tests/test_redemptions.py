from copy import deepcopy
import json
from pathlib import Path

import pytest

from skypoints_data_engineering.redemptions import (
    RedemptionDisposition,
    RedemptionIssueSeverity,
    process_redemption_payloads,
)


REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURE = REPOSITORY_ROOT / "tests" / "fixtures" / "redemptions_assessment_example.json"


def _fixture_text() -> str:
    return FIXTURE.read_text(encoding="utf-8")


def _fixture_object() -> dict[str, object]:
    return json.loads(_fixture_text())


def test_assessment_example_flattens_two_transactions_and_keeps_parent_fields() -> None:
    raw_text = _fixture_text()
    result = process_redemption_payloads(
        raw_text,
        source_name="tests/fixtures/redemptions_assessment_example.json",
        batch_id="synthetic-assessment-example",
    )

    assert len(result.payload_outcomes) == 1
    assert len(result.transaction_outcomes) == 2
    first, second = [outcome.record for outcome in result.transaction_outcomes]
    assert (first.parent_member_id, first.parent_feed_date) == ("223457", "20240115")
    assert (first.transaction_id, first.transaction_date, first.partner) == (
        "RX10091", "20240110", "AeroLink",
    )
    assert (first.miles_redeemed, first.status) == (12000, "COMPLETED")
    assert (second.transaction_id, second.transaction_date, second.partner) == (
        "RX10092", "20240113", "SkyPoints",
    )
    assert (second.miles_redeemed, second.status) == (5000, "PENDING")
    assert first.parsed_feed_date.isoformat() == "2024-01-15"
    assert first.parsed_transaction_date.isoformat() == "2024-01-10"
    assert first.raw_payload == raw_text
    assert first.provenance.source_name == "tests/fixtures/redemptions_assessment_example.json"
    assert first.provenance.batch_id == "synthetic-assessment-example"
    assert first.provenance.payload_index == 1
    assert first.provenance.transaction_index == 1
    assert first.raw_transaction == _fixture_object()["redemptions"][0]
    assert all(
        outcome.disposition is RedemptionDisposition.WARNING
        for outcome in result.transaction_outcomes
    )  # No confirmed profile mapping was supplied.
    assert len([issue for issue in result.issues if issue.rule_code == "MEMBER_REFERENCE_UNMATCHED"]) == 2


def test_ordered_payload_collection_and_empty_redemptions_array() -> None:
    payload_one = _fixture_object()
    payload_two = {"member_id": "M-EMPTY", "feed_date": "20240201", "redemptions": []}

    result = process_redemption_payloads([payload_one, payload_two], source_name="synthetic-batch")

    assert [item.provenance.payload_index for item in result.payload_outcomes] == [1, 2]
    assert [item.transaction_count for item in result.payload_outcomes] == [2, 0]
    assert len(result.transaction_outcomes) == 2
    assert result.payload_outcomes[1].disposition is RedemptionDisposition.ACCEPTED
    assert result.payload_outcomes[1].raw_payload == payload_two


@pytest.mark.parametrize(
    ("payload", "expected_code"),
    [
        ("{", "JSON_SYNTAX_INVALID"),
        ("NaN", "JSON_SYNTAX_INVALID"),
        ("[]", "JSON_TOP_LEVEL_INVALID"),
        ({}, "PARENT_FIELD_REQUIRED"),
        ({"member_id": 223457, "feed_date": "20240115", "redemptions": []}, "PARENT_FIELD_TYPE_INVALID"),
        ({"member_id": "223457", "feed_date": "20240230", "redemptions": []}, "PARENT_DATE_INVALID"),
        ({"member_id": "223457", "feed_date": "20240115"}, "REDEMPTIONS_REQUIRED"),
        ({"member_id": "223457", "feed_date": "20240115", "redemptions": {}}, "REDEMPTIONS_TYPE_INVALID"),
    ],
)
def test_parent_syntax_structure_and_required_fields_are_reported(
    payload: object,
    expected_code: str,
) -> None:
    result = process_redemption_payloads(payload, source_name="synthetic-invalid-parent")

    assert len(result.payload_outcomes) == 1
    outcome = result.payload_outcomes[0]
    assert outcome.disposition is RedemptionDisposition.QUARANTINED
    assert expected_code in {issue.rule_code for issue in outcome.issues}
    assert outcome.raw_payload == payload
    assert result.payload_outcomes[0].provenance.source_name == "synthetic-invalid-parent"


def test_invalid_transaction_object_fields_dates_and_miles_are_retained() -> None:
    payload = {
        "member_id": "M-TEST",
        "feed_date": "20240115",
        "redemptions": [
            {
                "txn_id": "BAD-DATE",
                "txn_date": "20240230",
                "partner": "Partner X",
                "miles_redeemed": 12.5,
                "status": "UNSPECIFIED-STATUS",
            },
            {"txn_date": 20240110, "partner": "", "miles_redeemed": True},
            "not-an-object",
        ],
    }
    before = deepcopy(payload)

    result = process_redemption_payloads(payload, source_name="synthetic-malformed-transactions")

    assert len(result.transaction_outcomes) == 3
    assert all(
        outcome.disposition is RedemptionDisposition.QUARANTINED
        for outcome in result.transaction_outcomes
    )
    issue_codes = {issue.rule_code for issue in result.issues}
    assert "TRANSACTION_DATE_INVALID" in issue_codes
    assert "MILES_NOT_INTEGER" in issue_codes
    assert "TRANSACTION_FIELD_REQUIRED" in issue_codes
    assert "TRANSACTION_FIELD_TYPE_INVALID" in issue_codes
    assert "TRANSACTION_OBJECT_INVALID" in issue_codes
    assert payload == before
    for outcome in result.transaction_outcomes:
        assert outcome.record.raw_payload == before
        assert outcome.record.provenance.source_name == "synthetic-malformed-transactions"
        assert all(issue.provenance == outcome.record.provenance for issue in outcome.issues)
    # No complete status/partner domain is invented.
    assert result.transaction_outcomes[0].record.status == "UNSPECIFIED-STATUS"
    assert not any(issue.rule_code in {"STATUS_UNKNOWN", "PARTNER_UNKNOWN"} for issue in result.issues)


def test_invalid_parent_keeps_and_quarantines_each_child_transaction() -> None:
    payload = _fixture_object()
    payload["feed_date"] = "20240230"

    result = process_redemption_payloads(payload, source_name="synthetic-invalid-parent-with-children")

    assert result.payload_outcomes[0].disposition is RedemptionDisposition.QUARANTINED
    assert len(result.transaction_outcomes) == 2
    assert all(row.disposition is RedemptionDisposition.QUARANTINED for row in result.transaction_outcomes)
    assert all(row.parent_outcome is result.payload_outcomes[0] for row in result.transaction_outcomes)
    assert all(row.record.raw_payload == payload for row in result.transaction_outcomes)
    assert all(
        any(issue.rule_code == "PARENT_PAYLOAD_INVALID" for issue in row.issues)
        for row in result.transaction_outcomes
    )


@pytest.mark.parametrize("miles", ["12000", 12000.0, True, None])
def test_miles_must_be_a_json_integer(miles: object) -> None:
    payload = _fixture_object()
    payload["redemptions"] = [
        {**payload["redemptions"][0], "miles_redeemed": miles}
    ]

    result = process_redemption_payloads(payload, member_key_mapping={"223457": "SYNTH-MEMBER-001"})

    outcome = result.transaction_outcomes[0]
    assert outcome.disposition is RedemptionDisposition.QUARANTINED
    assert any(issue.rule_code == "MILES_NOT_INTEGER" for issue in outcome.issues)
    assert outcome.record.miles_redeemed == miles


@pytest.mark.parametrize("miles", [0, -100])
def test_non_positive_integer_miles_are_retained_as_warnings(miles: int) -> None:
    payload = _fixture_object()
    payload["redemptions"] = [
        {**payload["redemptions"][0], "miles_redeemed": miles}
    ]
    result = process_redemption_payloads(payload, member_key_mapping={"223457": "SYNTH-MEMBER-001"})

    outcome = result.transaction_outcomes[0]
    assert outcome.disposition is RedemptionDisposition.WARNING
    assert any(
        issue.rule_code == "NON_POSITIVE_MILES"
        and issue.severity is RedemptionIssueSeverity.WARNING
        for issue in outcome.issues
    )
    assert outcome.record.miles_redeemed == miles


def test_duplicate_transaction_ids_quarantine_all_matches_within_one_call() -> None:
    first = _fixture_object()
    second = _fixture_object()
    second["member_id"] = "M-OTHER"
    second["redemptions"] = [
        {**second["redemptions"][0], "txn_id": "RX10091"}
    ]

    result = process_redemption_payloads([first, second], source_name="synthetic-duplicate-batch")

    assert len(result.transaction_outcomes) == 3
    duplicate_outcomes = [
        outcome for outcome in result.transaction_outcomes
        if any(issue.rule_code == "DUPLICATE_TRANSACTION_ID" for issue in outcome.issues)
    ]
    assert len(duplicate_outcomes) == 2
    assert all(outcome.disposition is RedemptionDisposition.QUARANTINED for outcome in duplicate_outcomes)
    assert [outcome.record.provenance.payload_index for outcome in duplicate_outcomes] == [1, 2]
    assert sum(payload.transaction_count for payload in result.payload_outcomes) == 3
    assert len(result.transaction_outcomes) == 3

    separate_calls = [
        process_redemption_payloads(payload, source_name="separate-call")
        for payload in (first, second)
    ]
    assert all(
        not any(issue.rule_code == "DUPLICATE_TRANSACTION_ID" for issue in result.issues)
        for result in separate_calls
    )


def test_member_reference_stays_unmatched_without_explicit_confirmed_mapping() -> None:
    result = process_redemption_payloads(
        _fixture_text(),
        source_name="tests/fixtures/redemptions_assessment_example.json",
    )

    for outcome in result.transaction_outcomes:
        assert outcome.record.parent_member_id == "223457"
        assert outcome.record.matched_member_key is None
        issue = next(issue for issue in outcome.issues if issue.rule_code == "MEMBER_REFERENCE_UNMATCHED")
        assert issue.provenance == outcome.record.provenance

    # These supplied local source IDs do not match the illustrative JSON member ID.
    local_source_ids_only = {"1": "AUS-local-1", "2": "IND-local-2", "3": "USA-local-3"}
    still_unmatched = process_redemption_payloads(
        _fixture_text(),
        member_key_mapping=local_source_ids_only,
    )
    assert all(row.record.matched_member_key is None for row in still_unmatched.transaction_outcomes)
    assert all(
        any(issue.rule_code == "MEMBER_REFERENCE_UNMATCHED" for issue in row.issues)
        for row in still_unmatched.transaction_outcomes
    )


def test_successful_member_join_uses_only_explicit_synthetic_mapping() -> None:
    # This mapping is synthetic test data, not a mapping observed in supplied member files.
    synthetic_confirmed_mapping = {"223457": "SYNTH-MEMBER-001"}
    result = process_redemption_payloads(
        _fixture_text(),
        source_name="tests/fixtures/redemptions_assessment_example.json",
        member_key_mapping=synthetic_confirmed_mapping,
    )

    assert all(
        outcome.record.matched_member_key == "SYNTH-MEMBER-001"
        for outcome in result.transaction_outcomes
    )
    assert not any(issue.rule_code == "MEMBER_REFERENCE_UNMATCHED" for issue in result.issues)


def test_repeated_processing_is_deterministic_and_parent_issue_provenance_is_preserved() -> None:
    malformed_parent = {"member_id": "M-1", "feed_date": "20240115", "redemptions": [None]}
    first = process_redemption_payloads(
        [malformed_parent], source_name="synthetic-repeat", batch_id="fixed-batch",
    )
    second = process_redemption_payloads(
        [malformed_parent], source_name="synthetic-repeat", batch_id="fixed-batch",
    )

    assert first == second
    child = first.transaction_outcomes[0]
    assert child.disposition is RedemptionDisposition.QUARANTINED
    assert not child.parent_outcome.issues  # Parent is valid; malformed child findings are transaction-scoped.
    issue = next(issue for issue in child.issues if issue.rule_code == "TRANSACTION_OBJECT_INVALID")
    assert issue.raw_payload == malformed_parent
    assert issue.raw_transaction is None
    assert issue.provenance == child.record.provenance
