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

import bisect
import re
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple

__all__ = [
    "LocusAddress",
    "parse_canonical_header",
    "units_for_span",
    "compress_pieces",
    "split_at_citation_breaks",
    "citation_seq_for_daf",
    "render_ranges",
    "select_locus_work",
    "stream_offset_for_raw",
    "heb_numeral",
    "parse_unit_numeral",
    "amud_ordinal",
    "AMUD_MAX",
    "sefaria_daf",
    "daf_label_he",
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


def stream_offset_for_raw(offsets: Sequence[int], raw_pos: int) -> int:
    """Raw (NFC) character position -> offset in the normalized letter stream.

    `offsets` is the second element of `norm_stream()`: ascending, one entry per
    stream character, holding that character's index in the NFC source. Every call
    site in the pipeline writes ``stream, _ = norm_stream(...)`` and throws it away;
    it is the only bridge between the two coordinate systems, and the reason the
    remaining work here is a bisect rather than a re-alignment.

    Answers "the first stream character at or after `raw_pos`", which is what a unit
    START wants. A raw position past the last letter yields ``len(offsets)`` -- the
    half-open end of the stream, not an error, because a division whose text is all
    punctuation legitimately begins where the stream ends.
    """
    return bisect.bisect_left(offsets, raw_pos)


#: Additive gematria. No thousands and no geresh: nothing in this corpus's citation
#: labels needs either, and inventing forms the editions do not use would make a
#: rendered citation unrecognisable to the reader it is for.
_HEB_ONES = ("", "א", "ב", "ג", "ד", "ה", "ו", "ז", "ח", "ט")
_HEB_TENS = ("", "י", "כ", "ל", "מ", "נ", "ס", "ע", "פ", "צ")
_HEB_HUNDREDS = ("", "ק", "ר", "ש", "ת", "תק", "תר", "תש", "תת", "תתק")
_HEB_VALUE = {
    "א": 1, "ב": 2, "ג": 3, "ד": 4, "ה": 5, "ו": 6, "ז": 7, "ח": 8, "ט": 9,
    "י": 10, "כ": 20, "ל": 30, "מ": 40, "נ": 50, "ס": 60, "ע": 70, "פ": 80, "צ": 90,
    "ק": 100, "ר": 200, "ש": 300, "ת": 400,
    "ך": 20, "ם": 40, "ן": 50, "ף": 80, "ץ": 90,
}
#: Folded before the round-trip check, so a label written with a final letter is
#: still recognised as the numeral it denotes (ך is 20 exactly as כ is).
_FINAL_FOLD = str.maketrans({"ך": "כ", "ם": "מ", "ן": "נ", "ף": "פ", "ץ": "צ"})


def heb_numeral(value: int) -> str:
    """1 -> 'א', 15 -> 'טו', 176 -> 'קעו'. For addresses that arrive as integers.

    15 and 16 are written טו/טז, never יה/יו: those spell the Name. This is not a
    stylistic preference -- an edition does not print יה for 15, so a citation that
    did would not match the page the reader turns to.
    """
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError(f"not a citable numeral: {value!r}")
    if value > 999:
        raise ValueError(f"{value} is outside the range these citations use")
    hundreds, rest = divmod(value, 100)
    if rest in (15, 16):
        return _HEB_HUNDREDS[hundreds] + ("טו" if rest == 15 else "טז")
    tens, ones = divmod(rest, 10)
    return _HEB_HUNDREDS[hundreds] + _HEB_TENS[tens] + _HEB_ONES[ones]


def parse_unit_numeral(label: str) -> Optional[int]:
    """'קעו' -> 176, and '176' -> 176. None when the label is not a numeral.

    Additive gematria, tolerant of the punctuation an edition puts round a numeral
    (geresh, quotes, spaces). Used to SORT unit labels, which is where it earns its
    keep: marker order in an edition is not always citation order, so the unit table
    must be ordered by the number each label denotes.

    DECIMAL LABELS ARE REAL. 173 of the corpus's 8,736 daf markers spell the folio in
    ASCII digits rather than letters, across 95 distinct labels ('17', '120', '159').
    A Hebrew-only parser drops those markers, and dropping a marker does not lose a
    citation -- it silently WIDENS the neighbouring one over the text the dropped
    marker was there to divide.

    WELL-FORMED IS THE LOAD-BEARING WORD. Summing every Hebrew letter present -- the
    obvious implementation, and the one the probe tree uses -- reads a WORD as a
    number: הקדמה sums to 154, and an unnumbered opening section would sort into the
    middle of the tractate between daf 153 and 155, where nothing about it looks
    wrong. So the value must ROUND-TRIP: a label is a numeral only if re-rendering
    its own sum reproduces it exactly.

    That test is exact rather than heuristic, which matters, because the near-miss
    rules are not enough. Requiring non-increasing letter values rejects הקדמה
    (5, 100) but accepts עמוד (70, 40, 6, 4 = 120), and עמוד is a word this corpus
    actually uses. Under round-trip, 120 renders קכ, which is not עמוד, so it is
    refused -- while קעו, טו, טז and תתקצט all reproduce themselves and pass.
    """
    text = (label or "").strip()
    digits = [ch for ch in text if ch.isascii() and ch.isdigit()]
    if digits:
        # Mixed script is not a numeral in either system -- refuse rather than pick.
        if any(ch in _HEB_VALUE for ch in text):
            return None
        value = int("".join(digits))
        return value if 1 <= value <= 999 else None
    letters = [ch.translate(_FINAL_FOLD) for ch in text if ch in _HEB_VALUE]
    if not letters:
        return None
    value = sum(_HEB_VALUE[ch] for ch in letters)
    try:
        canonical = heb_numeral(value)
    except ValueError:
        return None
    return value if canonical == "".join(letters) else None


#: Columns per folio, by foliation. Two for a Bavli daf (recto/verso); FOUR for the
#: Yerushalmi, which is printed two columns to the side.
AMUD_MAX = 4
_AMUD_LETTERS = ("א", "ב", "ג", "ד")


def amud_ordinal(label: str) -> int:
    """'א' -> 1 ... 'ד' -> 4; 0 when the label names no column.

    Not two-valued, and this is measured rather than defensive: of the corpus's
    8,736 inline daf markers, 772 -- 8.8% -- carry ג or ד. Those are Yerushalmi
    folios, printed four columns to the leaf, so ע"ג and ע"ד are ordinary citations
    there. A recto/verso model does not merely mislabel them; it has nowhere to put
    them, so they are dropped, and a dropped marker silently widens its neighbour
    over the text it was there to divide.
    """
    first = (label or "").strip()[:1]
    return _AMUD_LETTERS.index(first) + 1 if first in _AMUD_LETTERS else 0


def sefaria_daf(index: int) -> Tuple[int, int]:
    """Sefaria's flat Talmud section index -> (daf, amud), amud 1 = ע\"א.

    Sefaria addresses a tractate as one flat sequence in which index 1 is the
    non-existent 1a, so 3 is 2a -- where every Bavli tractate actually begins. The
    conversion is uniform: ``daf = (n + 1) // 2``, recto on odd.

    It is uniform in the strong sense, checked rather than assumed: across the 36
    staged Tosafot works the recovered last daf lands on or inside the real tractate
    length every time, with zero overshoots (Bava Batra 176a of 176, Shabbat 157b of
    157, Ketubot 112b of 112, Yoma 88a of 88). Works that begin later -- Tosafot on
    Keritot at index 17, on Megillah at 4 -- need no correction: that IS where that
    commentary starts. The per-work offset table the plan budgeted for is not needed,
    and building one would encode 59 chances to be wrong in place of one formula that
    an external oracle already confirms.

    The 25 staged Rif works index their OWN foliation from 1 = 1a, which the same
    formula gives. Their labels must say so; see `daf_label_he`.
    """
    if not isinstance(index, int) or isinstance(index, bool) or index < 1:
        raise ValueError(f"not a Sefaria section index: {index!r}")
    return (index + 1) // 2, 2 - (index % 2)


def daf_label_he(daf: int, amud: int, prefix: str = "") -> str:
    """(14, 1) -> 'יד ע\"א'. `prefix` names a foliation that is not the tractate's.

    Columns run 1..4: two for a Bavli daf, four for a Yerushalmi folio.

    The prefix exists for one reason. A Rif work is paginated by the Rif's own
    folios, which run about a third of the tractate's, so `ברכות ג ע\"א` rendered from
    a Rif index points a reader at Bavli Berakhot 3a -- a real page, in the right
    tractate, that is not the text. A wrong citation that resolves is worse than one
    that does not, so the foliation is named in the label itself rather than left to
    a column heading the reader may not be looking at.
    """
    if not isinstance(amud, int) or isinstance(amud, bool) or not 1 <= amud <= AMUD_MAX:
        raise ValueError(f"a folio has {AMUD_MAX} columns at most, not column {amud!r}")
    label = f'{heb_numeral(daf)} ע"{_AMUD_LETTERS[amud - 1]}'
    return f"{prefix} {label}" if prefix else label


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


def split_at_citation_breaks(
    lo_ord: int, hi_ord: int, citation_seq: Sequence[Optional[int]]
) -> List[Tuple[int, int]]:
    """Split one unit range wherever the CITATION stops running forwards.

    `citation_seq[i]` is unit i's position in its own citation system, as a dense
    integer whose successor is +1: ``daf * 2 + amud - 1`` for a two-sided folio,
    ``daf * 4 + amud - 1`` for a Yerushalmi leaf, the chapter number for a chapter
    table. ``None`` means "this unit has no citation position", which can never be
    adjacent to anything.

    TABLE ORDER IS NOT CITATION ORDER, and assuming otherwise is the defect this
    whole module is most exposed to. The units of a work are ordered by where they
    sit in the stream, because that is what the bisect needs. In the Talmud editions
    that order is the edition's own arrangement, which follows the chapters -- while
    the printed daf markers follow the printed foliation. Where the two disagree the
    marker sequence steps BACKWARDS: measured, the daf sequence rises monotonically
    in only 41 of 87 marker-bearing works, Bavli Berakhot alone inverts 11 times
    (<דף טז, עמ' א> is immediately followed by <דף טו, עמ' ב>), and Menachot jumps
    31 folios back at צד ע"א -> סג ע"ב.

    Merging on table adjacency across such a step prints a descending citation. On
    the real shipped data that is 27 spans crossing an inversion and 17 rendering
    visibly backwards -- `בבלי מנחות צד ע"א–סג ע"ב`. Backwards is the LUCKY case: it
    is at least visible. The same step in the other direction yields a forward-
    looking range that quietly claims forty folios the fragment never touches.
    """
    if hi_ord < lo_ord:
        raise ValueError(f"range ({lo_ord}, {hi_ord}) has its ends reversed")
    runs: List[Tuple[int, int]] = []
    start = lo_ord
    for i in range(lo_ord, hi_ord):
        here, nxt = citation_seq[i], citation_seq[i + 1]
        if here is None or nxt is None or nxt != here + 1:
            runs.append((start, i))
            start = i + 1
    runs.append((start, hi_ord))
    return runs


def compress_pieces(
    pieces: Iterable[Tuple[int, int]],
    citation_seq: Optional[Sequence[Optional[int]]] = None,
) -> List[Tuple[int, int]]:
    """Merge unit intervals into display runs -- BY CITATION ADJACENCY ONLY.

    Two pieces join only when they overlap or are immediate successors. They are
    never joined because they are close in characters.

    This is the most dangerous rule in the feature and the reason it is stated
    negatively. Alignments that sit a few dozen characters apart still routinely
    skip a short unit; rendering ``2`` and ``4`` as ``2-4`` tells a reader the
    fragment witnesses unit 3, which it does not. The exact-unit index would keep
    answering searches correctly the whole time, so the search results and the
    printed citation would disagree with no error anywhere -- silent, and only
    findable by reading the manuscript.

    Pass `citation_seq` for any family whose table order is not its citation order
    (see `split_at_citation_breaks`); successorship is then required in BOTH, and
    each merged run is guaranteed to be a real forward run of citations. Omit it
    only where the two orders are proven identical -- which holds for the Bible and
    Sefaria chapter tables, whose labels are strictly increasing by construction,
    and not for the Talmud daf tables.
    """
    ordered = sorted(set(pieces))
    if not ordered:
        return []
    for lo, hi in ordered:
        if lo > hi:
            raise ValueError(f"piece ({lo}, {hi}) has its ends reversed")
    if citation_seq is not None:
        ordered = sorted(
            run for lo, hi in ordered for run in split_at_citation_breaks(lo, hi, citation_seq)
        )
    merged: List[List[int]] = [list(ordered[0])]
    for lo, hi in ordered[1:]:
        adjacent = lo <= merged[-1][1] + 1
        if adjacent and citation_seq is not None and lo == merged[-1][1] + 1:
            prev, here = citation_seq[merged[-1][1]], citation_seq[lo]
            adjacent = prev is not None and here is not None and here == prev + 1
        if adjacent:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return [(lo, hi) for lo, hi in merged]


def citation_seq_for_daf(
    folios: Sequence[Tuple[Optional[int], int]], columns_per_folio: int = 2
) -> List[Optional[int]]:
    """[(daf, amud)] -> the dense citation positions `compress_pieces` wants.

    ``columns_per_folio`` is 2 for a Bavli daf and 4 for a Yerushalmi leaf. A unit
    whose folio could not be read is ``None``, so nothing merges across it: an
    unreadable label is a break in the citation, not a licence to bridge one.
    """
    if columns_per_folio not in (2, AMUD_MAX):
        raise ValueError(f"a folio has 2 or {AMUD_MAX} columns, not {columns_per_folio}")
    out: List[Optional[int]] = []
    for daf, amud in folios:
        if daf is None or not 1 <= amud <= columns_per_folio:
            out.append(None)
        else:
            out.append(daf * columns_per_folio + amud - 1)
    return out


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
