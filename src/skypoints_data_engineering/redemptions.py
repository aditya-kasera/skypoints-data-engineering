"""Validate and flatten nested redemption JSON payloads in memory."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from datetime import date
from enum import Enum
import json
from typing import TypeAlias

from .source_dates import parse_source_date


JsonPayload: TypeAlias = str | bytes | Mapping[str, object]


class RedemptionDisposition(str, Enum):
    """Disposition for a parent payload or flattened transaction."""

    ACCEPTED = "accepted"
    WARNING = "warning"
    QUARANTINED = "quarantined"


class RedemptionIssueSeverity(str, Enum):
    """Severity for a structured redemption finding."""

    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class RedemptionProvenance:
    """Position of an input payload and optional child transaction."""

    source_name: str
    payload_index: int
    batch_id: str | None
    transaction_index: int | None = None


@dataclass(frozen=True)
class RedemptionIssue:
    """Issue carrying the raw parent and child values needed for review."""

    provenance: RedemptionProvenance
    rule_code: str
    severity: RedemptionIssueSeverity
    detail: str
    raw_payload: object
    raw_transaction: object | None = None
    field_name: str | None = None


@dataclass(frozen=True)
class RedemptionPayloadOutcome:
    """Parent-level parse/validation result, including its untouched payload."""

    provenance: RedemptionProvenance
    raw_payload: object
    parent_member_id: object | None
    parent_feed_date: object | None
    parsed_feed_date: date | None
    transaction_count: int
    disposition: RedemptionDisposition
    issues: tuple[RedemptionIssue, ...]


@dataclass(frozen=True)
class FlattenedRedemption:
    """One child transaction with parent fields and raw source lineage retained."""

    provenance: RedemptionProvenance
    parent_member_id: object | None
    parent_feed_date: object | None
    transaction_id: object | None
    transaction_date: object | None
    partner: object | None
    miles_redeemed: object | None
    status: object | None
    parsed_feed_date: date | None
    parsed_transaction_date: date | None
    matched_member_key: str | None
    raw_payload: object
    raw_transaction: object


@dataclass(frozen=True)
class RedemptionOutcome:
    """Validation disposition for one array element, including malformed ones."""

    record: FlattenedRedemption
    parent_outcome: RedemptionPayloadOutcome
    disposition: RedemptionDisposition
    issues: tuple[RedemptionIssue, ...]


@dataclass(frozen=True)
class RedemptionResult:
    """All parent payloads and child outcomes from one deterministic call."""

    payload_outcomes: tuple[RedemptionPayloadOutcome, ...]
    transaction_outcomes: tuple[RedemptionOutcome, ...]

    @property
    def issues(self) -> tuple[RedemptionIssue, ...]:
        """Return parent issues followed by transaction issues in input order."""

        return tuple(
            issue
            for payload in self.payload_outcomes
            for issue in payload.issues
        ) + tuple(
            issue
            for outcome in self.transaction_outcomes
            for issue in outcome.issues
        )


@dataclass
class _PendingTransaction:
    record: FlattenedRedemption
    parent_outcome: RedemptionPayloadOutcome
    issues: list[RedemptionIssue]


def process_redemption_payloads(
    payloads: JsonPayload | Sequence[JsonPayload],
    *,
    source_name: str = "in-memory",
    batch_id: str | None = None,
    member_key_mapping: Mapping[str, str] | None = None,
) -> RedemptionResult:
    """Parse and flatten one JSON payload or an ordered collection of payloads.

    Duplicate transaction IDs are scoped to this function call. A provided
    ``member_key_mapping`` must map explicitly confirmed redemption member IDs
    to canonical member profile keys; it is never inferred from member data.
    The illustrative PDF JSON is not a supplied production feed.
    """

    if not source_name:
        raise ValueError("source_name must be non-empty")
    if member_key_mapping is not None:
        for member_id, profile_key in member_key_mapping.items():
            if not isinstance(member_id, str) or not member_id.strip():
                raise ValueError("member_key_mapping keys must be non-empty strings")
            if not isinstance(profile_key, str) or not profile_key.strip():
                raise ValueError("member_key_mapping values must be non-empty strings")

    if isinstance(payloads, (str, bytes, Mapping)):
        items = (payloads,)
    elif isinstance(payloads, Sequence):
        items = tuple(payloads)
    else:
        raise TypeError("payloads must be one JSON payload or an ordered sequence of payloads")

    payload_outcomes: list[RedemptionPayloadOutcome] = []
    pending: list[_PendingTransaction] = []
    for payload_index, payload in enumerate(items, start=1):
        raw_payload = deepcopy(payload)
        parent_outcome, children = _process_parent(
            payload,
            raw_payload,
            source_name=source_name,
            batch_id=batch_id,
            payload_index=payload_index,
            member_key_mapping=member_key_mapping,
        )
        payload_outcomes.append(parent_outcome)
        pending.extend(children)

    _flag_duplicate_transaction_ids(pending)
    transaction_outcomes = tuple(_finalize_transaction(item) for item in pending)
    return RedemptionResult(tuple(payload_outcomes), transaction_outcomes)


def _process_parent(
    payload: JsonPayload,
    raw_payload: object,
    *,
    source_name: str,
    batch_id: str | None,
    payload_index: int,
    member_key_mapping: Mapping[str, str] | None,
) -> tuple[RedemptionPayloadOutcome, list[_PendingTransaction]]:
    provenance = RedemptionProvenance(source_name, payload_index, batch_id)
    issues: list[RedemptionIssue] = []
    try:
        if isinstance(payload, (str, bytes)):
            parsed = json.loads(payload, parse_constant=_reject_nonstandard_constant)
        elif isinstance(payload, Mapping):
            parsed = deepcopy(payload)
        else:
            parsed = None
            _issue(
                issues, provenance, "JSON_INPUT_TYPE_INVALID", RedemptionIssueSeverity.ERROR,
                "Input payload must be a JSON string, bytes, or object.", raw_payload,
            )
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
        parsed = None
        _issue(
            issues, provenance, "JSON_SYNTAX_INVALID", RedemptionIssueSeverity.ERROR,
            f"JSON payload cannot be parsed: {exc.msg if isinstance(exc, json.JSONDecodeError) else str(exc)}",
            raw_payload,
        )

    if parsed is None and not issues:
        _issue(
            issues, provenance, "JSON_TOP_LEVEL_INVALID", RedemptionIssueSeverity.ERROR,
            "Top-level JSON value must be an object.", raw_payload,
        )
    if parsed is not None and not isinstance(parsed, Mapping):
        _issue(
            issues, provenance, "JSON_TOP_LEVEL_INVALID", RedemptionIssueSeverity.ERROR,
            "Top-level JSON value must be an object.", raw_payload,
        )
        parsed = None

    if parsed is None:
        parent = RedemptionPayloadOutcome(
            provenance, raw_payload, None, None, None, 0,
            RedemptionDisposition.QUARANTINED, tuple(issues),
        )
        return parent, []

    member_id = parsed.get("member_id")
    if not isinstance(member_id, str) or not member_id.strip():
        code = "PARENT_FIELD_REQUIRED" if "member_id" not in parsed or member_id == "" else "PARENT_FIELD_TYPE_INVALID"
        _issue(
            issues, provenance, code, RedemptionIssueSeverity.ERROR,
            "member_id must be a non-empty string.", raw_payload, field_name="member_id",
        )

    feed_date = parsed.get("feed_date")
    parsed_feed_date: date | None = None
    if not isinstance(feed_date, str):
        code = "PARENT_FIELD_REQUIRED" if "feed_date" not in parsed else "PARENT_FIELD_TYPE_INVALID"
        _issue(
            issues, provenance, code, RedemptionIssueSeverity.ERROR,
            "feed_date must be an eight-digit YYYYMMDD string.", raw_payload, field_name="feed_date",
        )
    else:
        parsed_feed_date, reason = parse_source_date(feed_date, "basic_iso")
        if parsed_feed_date is None:
            _issue(
                issues, provenance, "PARENT_DATE_INVALID", RedemptionIssueSeverity.ERROR,
                f"feed_date {feed_date!r} is not a valid YYYYMMDD calendar date ({reason}).",
                raw_payload, field_name="feed_date",
            )

    children_value = parsed.get("redemptions")
    if "redemptions" not in parsed:
        _issue(
            issues, provenance, "REDEMPTIONS_REQUIRED", RedemptionIssueSeverity.ERROR,
            "Required redemptions array is absent.", raw_payload, field_name="redemptions",
        )
        children: list[object] = []
    elif not isinstance(children_value, list):
        _issue(
            issues, provenance, "REDEMPTIONS_TYPE_INVALID", RedemptionIssueSeverity.ERROR,
            "redemptions must be an array.", raw_payload, field_name="redemptions",
        )
        children = []
    else:
        children = children_value

    parent_outcome = RedemptionPayloadOutcome(
        provenance=provenance,
        raw_payload=raw_payload,
        parent_member_id=member_id,
        parent_feed_date=feed_date,
        parsed_feed_date=parsed_feed_date,
        transaction_count=len(children),
        disposition=_disposition(issues),
        issues=tuple(issues),
    )

    transactions = [
        _process_transaction(
            child,
            raw_payload,
            parent_outcome,
            transaction_index=index,
            member_key_mapping=member_key_mapping,
        )
        for index, child in enumerate(children, start=1)
    ]
    return parent_outcome, transactions


def _process_transaction(
    raw_transaction: object,
    raw_payload: object,
    parent: RedemptionPayloadOutcome,
    *,
    transaction_index: int,
    member_key_mapping: Mapping[str, str] | None,
) -> _PendingTransaction:
    provenance = RedemptionProvenance(
        parent.provenance.source_name,
        parent.provenance.payload_index,
        parent.provenance.batch_id,
        transaction_index,
    )
    issues: list[RedemptionIssue] = []
    txn = raw_transaction if isinstance(raw_transaction, Mapping) else {}
    if not isinstance(raw_transaction, Mapping):
        _issue(
            issues, provenance, "TRANSACTION_OBJECT_INVALID", RedemptionIssueSeverity.ERROR,
            "Each redemptions array element must be an object.", raw_payload, raw_transaction,
        )

    txn_id = _required_string(txn, "txn_id", raw_payload, raw_transaction, provenance, issues)
    txn_date = txn.get("txn_date")
    parsed_txn_date: date | None = None
    if not isinstance(txn_date, str):
        code = "TRANSACTION_FIELD_REQUIRED" if "txn_date" not in txn else "TRANSACTION_FIELD_TYPE_INVALID"
        _issue(
            issues, provenance, code, RedemptionIssueSeverity.ERROR,
            "txn_date must be an eight-digit YYYYMMDD string.", raw_payload, raw_transaction, "txn_date",
        )
    else:
        parsed_txn_date, reason = parse_source_date(txn_date, "basic_iso")
        if parsed_txn_date is None:
            _issue(
                issues, provenance, "TRANSACTION_DATE_INVALID", RedemptionIssueSeverity.ERROR,
                f"txn_date {txn_date!r} is not a valid YYYYMMDD calendar date ({reason}).",
                raw_payload, raw_transaction, "txn_date",
            )

    partner = _required_string(txn, "partner", raw_payload, raw_transaction, provenance, issues)
    miles = txn.get("miles_redeemed")
    if "miles_redeemed" not in txn:
        _issue(
            issues, provenance, "TRANSACTION_FIELD_REQUIRED", RedemptionIssueSeverity.ERROR,
            "Required transaction field 'miles_redeemed' is absent.", raw_payload, raw_transaction,
            "miles_redeemed",
        )
    elif type(miles) is not int:
        _issue(
            issues, provenance, "MILES_NOT_INTEGER", RedemptionIssueSeverity.ERROR,
            "miles_redeemed must be represented as a JSON integer.", raw_payload, raw_transaction,
            "miles_redeemed",
        )
    elif miles <= 0:
        _issue(
            issues, provenance, "NON_POSITIVE_MILES", RedemptionIssueSeverity.WARNING,
            "Zero or negative miles are retained and flagged pending reversal/zero-value rules.",
            raw_payload, raw_transaction, "miles_redeemed",
        )

    status = _required_string(txn, "status", raw_payload, raw_transaction, provenance, issues)

    if any(issue.severity is RedemptionIssueSeverity.ERROR for issue in parent.issues):
        parent_valid = False
    else:
        parent_valid = True
    member_id = parent.parent_member_id
    matched_member_key: str | None = None
    if isinstance(member_id, str) and member_id.strip():
        if member_key_mapping is not None:
            matched_member_key = member_key_mapping.get(member_id)
        if matched_member_key is None:
            _issue(
                issues, provenance, "MEMBER_REFERENCE_UNMATCHED", RedemptionIssueSeverity.WARNING,
                "No explicitly supplied confirmed member-key mapping matched this member_id.",
                raw_payload, raw_transaction, "member_id",
            )

    record = FlattenedRedemption(
        provenance=provenance,
        parent_member_id=member_id,
        parent_feed_date=parent.parent_feed_date,
        transaction_id=txn_id,
        transaction_date=txn_date,
        partner=partner,
        miles_redeemed=miles if "miles_redeemed" in txn else None,
        status=status,
        parsed_feed_date=parent.parsed_feed_date,
        parsed_transaction_date=parsed_txn_date,
        matched_member_key=matched_member_key,
        raw_payload=deepcopy(raw_payload),
        raw_transaction=deepcopy(raw_transaction),
    )
    pending = _PendingTransaction(record, parent, issues)
    if not parent_valid:
        pending.issues.append(
            RedemptionIssue(
                provenance=provenance,
                rule_code="PARENT_PAYLOAD_INVALID",
                severity=RedemptionIssueSeverity.ERROR,
                detail="Parent payload validation failed; child transaction is retained but quarantined.",
                raw_payload=deepcopy(raw_payload),
                raw_transaction=deepcopy(raw_transaction),
            )
        )
    return pending


def _required_string(
    values: Mapping[str, object],
    field_name: str,
    raw_payload: object,
    raw_transaction: object,
    provenance: RedemptionProvenance,
    issues: list[RedemptionIssue],
) -> str | None:
    value = values.get(field_name)
    if not isinstance(value, str):
        code = "TRANSACTION_FIELD_REQUIRED" if field_name not in values else "TRANSACTION_FIELD_TYPE_INVALID"
        _issue(
            issues, provenance, code, RedemptionIssueSeverity.ERROR,
            f"{field_name} must be a non-empty string.", raw_payload, raw_transaction, field_name,
        )
        return None
    if not value.strip():
        _issue(
            issues, provenance, "TRANSACTION_FIELD_REQUIRED", RedemptionIssueSeverity.ERROR,
            f"{field_name} must be a non-empty string.", raw_payload, raw_transaction, field_name,
        )
        return value
    return value


def _flag_duplicate_transaction_ids(pending: list[_PendingTransaction]) -> None:
    positions: dict[str, list[int]] = {}
    for index, transaction in enumerate(pending):
        transaction_id = transaction.record.transaction_id
        if isinstance(transaction_id, str) and transaction_id.strip():
            positions.setdefault(transaction_id, []).append(index)
    for transaction_id, indexes in positions.items():
        if len(indexes) < 2:
            continue
        for index in indexes:
            transaction = pending[index]
            transaction.issues.append(
                RedemptionIssue(
                    provenance=transaction.record.provenance,
                    rule_code="DUPLICATE_TRANSACTION_ID",
                    severity=RedemptionIssueSeverity.ERROR,
                    detail=(
                        f"Transaction ID {transaction_id!r} occurs {len(indexes)} times "
                        "in this processing call."
                    ),
                    raw_payload=deepcopy(transaction.record.raw_payload),
                    raw_transaction=deepcopy(transaction.record.raw_transaction),
                    field_name="txn_id",
                )
            )


def _finalize_transaction(pending: _PendingTransaction) -> RedemptionOutcome:
    return RedemptionOutcome(
        record=pending.record,
        parent_outcome=pending.parent_outcome,
        disposition=_disposition(pending.issues),
        issues=tuple(pending.issues),
    )


def _disposition(issues: Sequence[RedemptionIssue]) -> RedemptionDisposition:
    if any(issue.severity is RedemptionIssueSeverity.ERROR for issue in issues):
        return RedemptionDisposition.QUARANTINED
    if issues:
        return RedemptionDisposition.WARNING
    return RedemptionDisposition.ACCEPTED


def _issue(
    issues: list[RedemptionIssue],
    provenance: RedemptionProvenance,
    rule_code: str,
    severity: RedemptionIssueSeverity,
    detail: str,
    raw_payload: object,
    raw_transaction: object | None = None,
    field_name: str | None = None,
) -> None:
    issues.append(
        RedemptionIssue(
            provenance=provenance,
            rule_code=rule_code,
            severity=severity,
            detail=detail,
            raw_payload=deepcopy(raw_payload),
            raw_transaction=deepcopy(raw_transaction),
            field_name=field_name,
        )
    )


def _reject_nonstandard_constant(value: str) -> None:
    raise ValueError(f"Non-standard JSON constant {value!r} is not permitted")
