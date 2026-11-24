"""Shared strict parsers for source date representations."""

from __future__ import annotations

from datetime import date, datetime
import re


def parse_source_date(value: object, date_format: str) -> tuple[date | None, str]:
    """Return a parsed calendar date and ``valid``, ``invalid``, or ``ambiguous``.

    Supported format names match the three active adapters: ``iso`` for AUS,
    ``slash`` for IND, and ``compact`` for USA. Compact dates enumerate
    one- and two-digit month/day splits with a four-digit year.
    """

    if isinstance(value, datetime):
        return value.date(), "valid"
    if isinstance(value, date):
        return value, "valid"

    text = str(value)
    if date_format == "iso":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            return None, "invalid"
        try:
            return date.fromisoformat(text), "valid"
        except ValueError:
            return None, "invalid"
    if date_format == "slash":
        try:
            return datetime.strptime(text, "%m/%d/%Y").date(), "valid"
        except ValueError:
            return None, "invalid"
    if date_format == "compact":
        return _parse_compact_date(text)
    raise ValueError(f"Unknown date format: {date_format}")


def _parse_compact_date(text: str) -> tuple[date | None, str]:
    if not re.fullmatch(r"\d{7,8}", text):
        return None, "invalid"
    candidates: set[date] = set()
    for month_digits in (1, 2):
        for day_digits in (1, 2):
            if len(text) != month_digits + day_digits + 4:
                continue
            month_text = text[:month_digits]
            day_text = text[month_digits : month_digits + day_digits]
            year_text = text[month_digits + day_digits :]
            try:
                candidates.add(date(int(year_text), int(month_text), int(day_text)))
            except ValueError:
                continue
    if len(candidates) == 1:
        return next(iter(candidates)), "valid"
    if len(candidates) > 1:
        return None, "ambiguous"
    return None, "invalid"
