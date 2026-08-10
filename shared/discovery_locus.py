# -*- coding: utf-8 -*-
"""Reference locus: turning a stored work-side offset into a citable place.

An identification today names a work and stops (*תלמוד בבלי, שבת*). This module
turns `discovery_evidence.w_start`/`w_end` -- offsets into the reference work's
NORMALIZED Hebrew-letter stream (`norm_stream`), never into raw text -- into the
place inside that work a scholar would cite (*שבת יד ע"א--טו ע"ב*).

Everything here is PURE: no I/O, no database, no filesystem. The offline builder
(`scripts/build_work_divisions.py`) supplies the per-work unit table; the bake
supplies the intervals. That split is what makes every rule below directly
mutation-testable.

MASKING. Reference corpora are named only as "M-source" / "R-source". A canonical
work's SECTION LABEL (בראשית יב, שבת יד ע"א) is a universal scholarly citation and
ships; the source-manuscript provenance field carried in the same header does NOT,
and `parse_canonical_header` drops it by construction rather than by convention.
"""
from __future__ import annotations

import re
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple

__all__ = [
    "LocusAddress",
    "parse_canonical_header",
    "units_for_span",
    "compress_pieces",
    "render_ranges",
    "select_locus_work",
    "RANGE_SEP",
    "PIECE_SEP",
]

#: Between the two ends of one continuous run (en dash, the citation convention).
RANGE_SEP = "–"
#: Between two runs that are NOT adjacent -- the visible evidence of a gap.
PIECE_SEP = "; "

# `##division, פרק N, פסוק|משנה|הלכה N | <provenance>##`
# The division field is absent in per-tractate files, where the file IS the tractate.
_ADDR_RE = re.compile(
    r"^(?:(?P<div>[^,]+?),\s*)?"
    r"פרק\s+(?P<chapter>[^,|]+?)\s*"
    r"(?:,\s*(?P<kind>פסוק|משנה|הלכה)\s+(?P<sub>[^,|]+?)\s*)?$"
)
#: Edition apparatus, not part of a citation.
_GIRSA_RE = re.compile(r"\s*\(גרסה\)\s*$")


class LocusAddress(NamedTuple):
    """One parsed address. `sub` is the verse / mishnah / halakhah, if the header has one."""

    division: str          #: book or tractate; "" when the file is already per-tractate
    chapter: str           #: Hebrew numeral, verbatim from the source -- never re-rendered
    sub: str               #: Hebrew numeral, or "" when the header carries no finer field
    sub_kind: str          #: "פסוק" | "משנה" | "הלכה" | ""


def parse_canonical_header(inner: str) -> Optional[LocusAddress]:
    """Parse one `##...##` header body into an address, dropping provenance.

    `inner` is the text BETWEEN the `##` delimiters.

    Provenance is removed by cutting at the first ``|`` -- deliberately NOT by
    subtracting the provenance-extraction regex used elsewhere in the pipeline, which
    matches the ENTIRE header (it exists to pull the provenance value out of a whole
    header) and would therefore delete the address along with it. That mistake yields
    a silent zero-unit map: every span collapses onto one nameless unit and the
    failure looks like "this work has no divisions" rather than a parse bug.

    Returns None when the body carries no chapter, which is correct rather than
    exceptional: a work whose header does not address is simply not addressable.
    """
    body = inner.split("|", 1)[0].strip()
    match = _ADDR_RE.match(body)
    if match is None:
        return None
    return LocusAddress(
        division=(match.group("div") or "").strip(),
        chapter=_GIRSA_RE.sub("", (match.group("chapter") or "").strip()),
        sub=_GIRSA_RE.sub("", (match.group("sub") or "").strip()),
        sub_kind=(match.group("kind") or "").strip(),
    )


def units_for_span(unit_starts: Sequence[int], start: int, end: int) -> Tuple[int, int]:
    """Half-open stream interval ``[start, end)`` -> inclusive ``(lo_ord, hi_ord)``.

    `unit_starts` is the ascending start offset of every unit of ONE work, so the
    ordinal is the index into it.

    The high end probes ``end - 1``, not ``end``: a span ending exactly on a unit
    boundary stops *before* that unit, and citing the unit it never reaches is the
    single easiest way to publish a confidently wrong reference.
    """
    if not unit_starts:
        raise ValueError("a work with no units cannot carry a locus")
    if end < start:
        raise ValueError(f"span end {end} precedes start {start}")
    lo = _floor_index(unit_starts, start)
    hi = _floor_index(unit_starts, max(start, end - 1))
    return lo, max(lo, hi)


def _floor_index(starts: Sequence[int], offset: int) -> int:
    """Index of the last unit starting at or before `offset` (0 when it precedes all)."""
    low, high = 0, len(starts)
    while low < high:
        mid = (low + high) // 2
        if starts[mid] <= offset:
            low = mid + 1
        else:
            high = mid
    return max(low - 1, 0)


def compress_pieces(pieces: Iterable[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """Merge unit intervals into display runs -- BY CITATION ADJACENCY ONLY.

    Two pieces join only when they overlap or are immediate successors in unit
    ordinal. They are never joined because they are close in characters.

    This is the most dangerous rule in the feature and the reason it is stated
    negatively. Alignments that sit a few dozen characters apart still routinely
    skip a short unit; rendering ``2`` and ``4`` as ``2-4`` tells a reader the
    fragment witnesses unit 3, which it does not. The exact-unit index would keep
    answering searches correctly the whole time, so the search results and the
    printed citation would disagree with no error anywhere -- silent, and only
    findable by reading the manuscript.
    """
    ordered = sorted(set(pieces))
    if not ordered:
        return []
    for lo, hi in ordered:
        if lo > hi:
            raise ValueError(f"piece ({lo}, {hi}) has its ends reversed")
    merged: List[List[int]] = [list(ordered[0])]
    for lo, hi in ordered[1:]:
        if lo <= merged[-1][1] + 1:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return [(lo, hi) for lo, hi in merged]


def render_ranges(ranges: Sequence[Tuple[int, int]], labels: Sequence[str]) -> str:
    """Runs of unit ordinals -> the displayed citation, e.g. ``ב–יא; טו–לב``.

    Labels are emitted verbatim from the source's own Hebrew numerals. Nothing here
    wraps the result in brackets or parentheses: the surface envelope rejects a
    bracketed numeric pair, and because that guard covers the WHOLE envelope a single
    bad string costs the reader the entire page rather than one row.
    """
    out = []
    for lo, hi in ranges:
        if not (0 <= lo < len(labels)) or not (0 <= hi < len(labels)):
            raise IndexError(f"range ({lo}, {hi}) falls outside {len(labels)} units")
        out.append(labels[lo] if lo == hi else f"{labels[lo]}{RANGE_SEP}{labels[hi]}")
    return PIECE_SEP.join(out)


def select_locus_work(
    matched_letters_by_work: Dict[str, int],
    display_work_id: str,
) -> Optional[str]:
    """Pick the ONE coordinate space an identification's address is rendered in.

    A composition can sit in the corpus twice -- once per reference corpus -- under a
    single canonical identity, and the two copies are addressed in different systems
    (one by folio, the other by chapter). Those are not interconvertible, so one
    rendered address must come from exactly one of them.

    The order is: **most matched letters wins; ties to the display work; then lowest
    work id.** Deliberately NOT "the display work whenever it has anything": measured
    over the real duplicates, the non-display member carries more matched text in 9 of
    15 cases, so a display-first rule would discard the better address on most of
    them. Equally, filtering to the display work alone would delete a valid address
    outright whenever only the other member has evidence.

    Returns None when no member carries any matched text -- fail closed, show no
    place, rather than invent one.
    """
    candidates = {w: n for w, n in matched_letters_by_work.items() if n and n > 0}
    if not candidates:
        return None
    return min(
        candidates,
        key=lambda w: (-candidates[w], w != display_work_id, w),
    )
