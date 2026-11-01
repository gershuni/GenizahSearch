"""Numbers the FJMS catalogue records as free text: lines, columns, copy dates.

The catalogue's line and column counts are text (``catalog.NumRow`` /
``NumColumn``): ``13``, ``19-21``, ``22,25``, ``16–31``, ``8-9-12``,
``15 (-16)``, ``5-``. ``parse_number_text`` reads them into intervals by
the owner's ruling D7 of docs/plans/CATALOGUE_CRITERIA_PLAN.md, and
``scripts/migrate_fjms_criteria.py`` stores the result in the sidecar's
``criteria_numbers`` table, so a search never parses text.

Copy dates are numbers already (``CopyDate``, ``CopyToDate``); ``copy_span``
is the per-record span rule of D18, mirrored in SQL by the criteria code.
"""
from __future__ import annotations

import re

# Every dash the catalogue uses as a range mark; parentheses only wrap a
# number ("15 (-16)").
_NORMALIZE = str.maketrans({
    "‒": "-", "–": "-", "—": "-", "−": "-",
    "(": " ", ")": " ",
})
_SOFT_HYPHEN = "­"
_NUMBER = re.compile(r"\d+")


def parse_number_text(text) -> list[tuple[int, int]]:
    """The intervals a line or column text records, sorted, without repeats.

    - Parts are separated by commas; each part is one value or a range.
    - Numbers joined by a dash (any dash) are one interval from the
      smallest to the largest: ``19-21`` -> 19..21, ``8-9-12`` -> 8..12,
      ``22-20`` -> 20..22, ``15 (-16)`` -> 15..16.
    - Numbers in a part without a dash are values of their own.
    - ``0`` is no value (the Lieberman and Danzig catalogues use it for
      blank), also as a range end: ``0-25`` -> 25.
    - Soft hyphens are dropped; a trailing dash or comma is ignored
      (``5-`` -> 5, ``7,3,`` -> 3 and 7).
    - Numbers over 80 are kept as written.
    """
    if text is None:
        return []
    cleaned = str(text).replace(_SOFT_HYPHEN, "").translate(_NORMALIZE)
    intervals = set()
    for part in cleaned.split(","):
        numbers = [int(n) for n in _NUMBER.findall(part)]
        numbers = [n for n in numbers if n > 0]
        if not numbers:
            continue
        if "-" in part:
            intervals.add((min(numbers), max(numbers)))
        else:
            intervals.update((n, n) for n in numbers)
    return sorted(intervals)


def _positive_year(value):
    try:
        year = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return year if year > 0 else None


def copy_span(copy_date, copy_to_date):
    """The (from, to) years one catalogue record dates its copy to, or None.

    D18: ``CopyDate`` must be a positive year (``0``, ``-99``, empty = no
    date); a ``CopyToDate`` after it extends the span; one before it
    (``1027`` / ``8``) or missing leaves the start year alone. A record
    with only a ``CopyToDate`` has no date.
    """
    start = _positive_year(copy_date)
    if start is None:
        return None
    end = _positive_year(copy_to_date)
    if end is None or end < start:
        end = start
    return start, end


def span_label(span) -> str:
    """'944–1100' for a span, '944' for one year, '' for None."""
    if not span:
        return ""
    start, end = span
    return f"{start}–{end}" if end != start else str(start)
