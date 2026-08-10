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
import json
import re
import unicodedata
from array import array
from typing import Dict, Iterable, List, NamedTuple, Optional, Sequence, Tuple

__all__ = [
    "LocusAddress",
    "parse_canonical_header",
    "units_for_span",
    "MIN_UNIT_OVERLAP",
    "compress_pieces",
    "split_at_citation_breaks",
    "citation_runs",
    "citation_seq_for_daf",
    "render_ranges",
    "select_locus_work",
    "norm_stream",
    "stream_offset_for_raw",
    "heb_numeral",
    "parse_unit_numeral",
    "amud_ordinal",
    "AMUD_MAX",
    "sefaria_daf",
    "daf_label_he",
    "RANGE_SEP",
    "PIECE_SEP",
    "label_segments",
    "shorten_range_tail",
    "RefAlignment",
    "RefSpanProjectionError",
    "parse_ref_span_alignments",
    "select_primary_alignment",
    "merge_witnessed_spans",
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


#: א..ת. Finals fold to their base so a word-final letter matches its medial form.
_STREAM_MIN, _STREAM_MAX = 0x05D0, 0x05EA


def norm_stream(text: str) -> Tuple[str, "array[int]"]:
    """The reference corpus's coordinate system: (letter stream, offset map).

    Space-free Hebrew base letters, finals folded, everything else -- nikud,
    cantillation, the Judeo-Arabic upper dot, punctuation, brackets, digits, Latin,
    whitespace -- dropped as a separator. `offsets[i]` is the NFC-text index of
    stream character i, which is the bridge `stream_offset_for_raw` bisects.

    PORTED, NOT IMPORTED, and that is a deliberate trade. The pipeline's copy lives
    in the research tree, which is gitignored and unversioned, so importing it would
    make every stored offset in the product depend on a file no commit can pin. The
    cost is that the two can drift apart silently, and a drift here does not raise --
    it moves every offset in the corpus at once. `tests/test_discovery_locus.py`
    therefore diffs this implementation against the pipeline's whenever the research
    tree is present, and pins fixed vectors when it is not.
    """
    nfc = unicodedata.normalize("NFC", text)
    out: List[str] = []
    offsets: "array[int]" = array("i")
    for index, ch in enumerate(nfc):
        folded = ch.translate(_FINAL_FOLD)
        if _STREAM_MIN <= ord(folded) <= _STREAM_MAX:
            out.append(folded)
            offsets.append(index)
    return "".join(out), offsets


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


#: A first unit touched by fewer letters than this was CLIPPED, not witnessed.
#:
#: The scholar audit's case: a span opening 24 letters before משנה ו begins earned
#: `פרק ג, משנה ה–ו` where `משנה ו` was the answer. Any one-character touch bought a
#: citation, so the reader was told the fragment witnesses a sub-unit it barely
#: grazes.
#:
#: ABSOLUTE, not a fraction of the unit, and that is measured rather than preferred.
#: Unit extents differ by two orders of magnitude across the families -- a liturgical
#: section runs 617 letters at the median against 5,126 for a Yerushalmi halakhah --
#: so 5% of a unit is 31 letters in one family and 256 in another, which is larger
#: than the median stored span. On the real rows a 5%-of-unit rule discards first
#: units holding up to 1,115 letters of aligned text, and drops more than half the
#: whole span on 902 rows; the absolute rule drops more than half on none. Whether a
#: clip is a witness is a property of the clip, not of what it clips.
#:
#: 30 rather than 25 is a margin, and the cost of the margin is stated rather than
#: hidden: 25 is the MINIMUM that closes the audited case, 30 fires on 11,903 of
#: 234,060 real rows (5.09%) against 9,076 at 25, and takes the rows where the dropped
#: unit held a quarter or more of the span from 43 to 74. The ceiling is just above:
#: at 35 the rule starts removing units holding the MAJORITY of a span, at 100 it does
#: so on 1,442 rows.
MIN_UNIT_OVERLAP = 30


def units_for_span(
    unit_starts: Sequence[int], start: int, end: int,
    min_overlap: int = MIN_UNIT_OVERLAP,
) -> Tuple[int, int]:
    """Half-open stream interval ``[start, end)`` -> inclusive ``(lo_ord, hi_ord)``.

    `unit_starts` is the ascending start offset of every unit of ONE work, so the
    ordinal is the index into it.

    The high end probes ``end - 1``, not ``end``: a span ending exactly on a unit
    boundary stops *before* that unit, and citing the unit it never reaches is the
    single easiest way to publish a confidently wrong reference.

    THE TWO BOUNDARY RULES ARE NOT THE SAME RULE and must not be merged. The ``end -
    1`` probe refuses a unit the span does not reach AT ALL; `min_overlap` refuses a
    first unit the span reaches and barely touches. Pass ``min_overlap=0`` for raw
    geometry.

    The threshold applies at the START only. The end side has a measured twin -- 14.7%
    of boundary-crossing spans touch their last unit with under 25 letters, and one
    real row renders `סא ע"א–ע"ב` off a single letter of the verso -- but the ruling
    that produced this rule was about the start, so the end is left alone rather than
    quietly widened. Two consequences worth knowing before anyone adds it: the never-
    empty guarantee below stops being free (measured, exactly one real two-unit span
    is under the threshold at BOTH ends), and an end-side sliver is a genuine if tiny
    touch, whereas a start-side one is often the tail of the previous unit.
    """
    if not unit_starts:
        raise ValueError("a work with no units cannot carry a locus")
    if end < start:
        raise ValueError(f"span end {end} precedes start {start}")
    if min_overlap < 0:
        raise ValueError(f"a minimum overlap cannot be negative: {min_overlap!r}")
    lo = _floor_index(unit_starts, start)
    hi = max(lo, _floor_index(unit_starts, max(start, end - 1)))
    if min_overlap and lo < hi:
        # `lo < hi` carries three guarantees at once, which is why it is the whole
        # condition: `unit_starts[lo + 1]` exists, the span runs past it (so the first
        # unit's covered extent is exactly this difference -- no stream length and no
        # extent table needed), and `lo + 1 <= hi`, so the rule can NEVER empty a
        # range. A span lying entirely inside one unit is untouched by construction,
        # however short it is; the shortest stored span in the corpus is 36 letters
        # and it keeps its address.
        if unit_starts[lo + 1] - max(start, unit_starts[lo]) < min_overlap:
            lo += 1
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


def citation_runs(
    pieces: Iterable[Tuple[int, int]], citation_seq: Sequence[Optional[int]]
) -> Tuple[List[Tuple[int, int]], List[int]]:
    """Unit-ordinal ranges -> the runs of CITATION POSITIONS they witness.

    Returns ``(runs, unplaced)``: merged inclusive runs in citation space, and the
    ordinals of any units that carry no citation position, which are reported rather
    than dropped so the caller can decide (they must never be silently swallowed).

    THE INDEX IS IN ORDINAL SPACE; THE CITATION IS NOT. An ordinal identifies a
    place in the table, and a table can visit the same folio twice -- 46 of 87
    marker-bearing works do, 495 repeated units in all, because the edition's
    arrangement revisits a folio the printed foliation only numbers once. Rendering
    from ordinals then hands the reader `נז ע"א; נו ע"ב–נז ע"א; נו ע"ב`: honest about
    the pieces, and unreadable as a citation.

    A reader asks which folios the fragment witnesses. Two table units on the same
    folio are ONE answer to that question, so the render folds to the set of
    positions touched and merges what is genuinely consecutive there -- `נו ע"ב–נז
    ע"א`. Nothing is lost: `discovery_locus_piece` still stores the exact ordinals,
    so the part filter keeps answering at full resolution while the printed citation
    reads the way a citation reads.
    """
    positions = set()
    unplaced: List[int] = []
    for lo, hi in pieces:
        if hi < lo:
            raise ValueError(f"piece ({lo}, {hi}) has its ends reversed")
        for ordinal in range(lo, hi + 1):
            place = citation_seq[ordinal]
            if place is None:
                unplaced.append(ordinal)
            else:
                positions.add(place)
    runs: List[Tuple[int, int]] = []
    for place in sorted(positions):
        if runs and place == runs[-1][1] + 1:
            runs[-1] = (runs[-1][0], place)
        else:
            runs.append((place, place))
    return runs, sorted(set(unplaced))


def _label_at(labels, key: int) -> str:
    """Look a label up by ordinal or by citation position, refusing a miss.

    `labels` is a list when the caller renders from ordinals and a mapping when it
    renders from citation positions. A negative key is refused explicitly: on a list
    it would silently wrap round and cite the wrong end of the work.
    """
    if key < 0:
        raise IndexError(f"{key} is not a label key")
    try:
        return labels[key]
    except (KeyError, IndexError):
        raise IndexError(f"no label for {key}") from None


def label_segments(label: str) -> List[str]:
    """Split a compound label on `, ` -- but NEVER inside brackets.

    A Judeo-Arabic section label can be `1. [פ, מ, לא, לו, ליא]`: a section number
    followed by the manuscripts that witness it. Those commas are inside the witness
    list and are not part boundaries, so a plain `split(", ")` would cut the list up
    and then the range renderer would elide half of it as a shared prefix.

    Only a bracket that actually CLOSES protects anything. Real headings arrive
    unbalanced -- `הקדמה למסכת אבות [שמונה פרקים` is one -- and a running-depth
    counter would let that stray `[` suppress every separator to the end of the
    label, so the page part would fuse to the section name and never shorten.
    """
    protected = [False] * len(label)
    stack: List[Tuple[str, int]] = []
    pairs = {")": "(", "]": "[", "}": "{"}
    for index, ch in enumerate(label):
        if ch in "([{":
            stack.append((ch, index))
        elif ch in ")]}" and stack and stack[-1][0] == pairs[ch]:
            _, opened = stack.pop()
            for position in range(opened, index + 1):
                protected[position] = True

    parts, current = [], []
    index = 0
    while index < len(label):
        if not protected[index] and label.startswith(", ", index):
            parts.append("".join(current))
            current = []
            index += 2
            continue
        current.append(label[index])
        index += 1
    parts.append("".join(current))
    return [p for p in parts if p != ""] or [label]


def shorten_range_tail(head: str, tail: str) -> str:
    """The tail of a range, with whatever it already shares with the head removed.

    `עמ' 43–עמ' 47` is not a page range anyone writes; `עמ' 43–47` is. The same rule
    covers every family at once, because a repeated leading part is the normal shape
    of a compound label: `פרק ג, משנה ה–פרק ג, משנה ז` shortens to `פרק ג, משנה ה–ז`,
    and `יד ע"א–יד ע"ב` to `יד ע"א–ע"ב`.

    Two stages, and the second is CONDITIONAL on the first having consumed
    everything before it. Whole leading parts go first; then, only if the two labels
    now differ in their very first remaining part, a shared leading word inside that
    part goes too. Without that condition `פרק ג, משנה ה` and `פרק ד, משנה ב` would
    lose the `משנה` from the tail while still disagreeing about the chapter, and the
    reader would be told the range ends at `פרק ד, ב` -- a different address.

    Never returns empty: two identical labels are not a range, and a caller that
    hands us one gets the tail back whole rather than a dangling dash.
    """
    head_parts, tail_parts = label_segments(head), label_segments(tail)
    shared = 0
    while (shared < min(len(head_parts), len(tail_parts))
           and head_parts[shared] == tail_parts[shared]):
        shared += 1
    rest = tail_parts[shared:]
    if not rest:
        return tail
    if shared == len(head_parts) - 1:
        head_words, tail_words = head_parts[shared].split(), rest[0].split()
        common = 0
        while (common < min(len(head_words), len(tail_words)) - 1
               and head_words[common] == tail_words[common]):
            common += 1
        if common:
            rest = [" ".join(tail_words[common:])] + rest[1:]
    return ", ".join(p for p in rest if p) or tail


def render_ranges(ranges: Sequence[Tuple[int, int]], labels) -> str:
    """Runs -> the displayed citation, e.g. ``ב–יא; טו–לב``.

    `labels` is indexed by whatever space `ranges` is in: a sequence keyed by unit
    ordinal, or a mapping keyed by citation position (see `citation_runs`).

    Labels are emitted verbatim from the source's own Hebrew numerals; the only thing
    done to them is dropping a repetition the reader does not need (see
    `shorten_range_tail`). Nothing here wraps the result in brackets or parentheses:
    the surface envelope rejects a bracketed numeric pair, and because that guard
    covers the WHOLE envelope a single bad string costs the reader the entire page
    rather than one row.
    """
    out = []
    for lo, hi in ranges:
        if hi < lo:
            raise ValueError(f"range ({lo}, {hi}) has its ends reversed")
        head, tail = _label_at(labels, lo), _label_at(labels, hi)
        out.append(head if lo == hi
                   else f"{head}{RANGE_SEP}{shorten_range_tail(head, tail)}")
    return PIECE_SEP.join(out)


class RefSpanProjectionError(RuntimeError):
    """Fail-closed error reading a match row's paired page/work spans."""


class RefAlignment(NamedTuple):
    """ONE dual-side alignment, exactly as the producer paired it.

    `page_start`/`page_end` index the manuscript page's normalized stream;
    `w_start`/`w_end` index the reference work's. The pairing is the producer's, not
    something computed here.
    """

    page_start: int
    page_end: int
    w_start: int
    w_end: int


def parse_ref_span_alignments(ref_spans_json_str: Optional[str]) -> List[RefAlignment]:
    """EVERY dual-side alignment a match row carries, in the producer's own order.

    The bake has until now kept one alignment per row -- the largest page-side extent
    -- because an evidence row has room for one dual-side span. Measured over all
    381,341 real rows: **86,724 (22.7%) carry more than one**, and **83,998 (22.0%)
    carry more than one DISTINCT work-side span**. Those are the rows whose evidence
    genuinely sits in several places, and a citation that names only one of them is
    not merely coarse, it is wrong about where the fragment is.

    Order is the producer's, deliberately. Sorting here would silently change which
    alignment `select_primary_alignment` returns on a tie, and that function's
    tie-break is frozen.

    RAISES rather than skipping. An entry missing one side cannot be dropped: dropping
    it changes which entry wins, and the row still comes out carrying plausible
    offsets, so nothing downstream can see that it happened. Measured: 0 of 381,341
    real rows are malformed, so this path is a guard against a future producer, not a
    tolerance for the present one.

    MASKING: reads the four integer fields only. The `cigar` alignment string is
    reference-text-derived; it is never read, stored, or logged.
    """
    if not ref_spans_json_str:
        return []
    try:
        entries = json.loads(ref_spans_json_str)
    except ValueError as exc:
        raise RefSpanProjectionError(
            f"ref_spans_json is not parseable JSON -- refusing to guess a work-side "
            f"offset ({type(exc).__name__})"
        ) from exc
    if not entries:
        return []
    out: List[RefAlignment] = []
    for entry in entries:
        try:
            out.append(RefAlignment(int(entry["p0"]), int(entry["p1"]),
                                    int(entry["rg0"]), int(entry["rg1"])))
        except (KeyError, TypeError, ValueError) as exc:
            raise RefSpanProjectionError(
                "a ref_spans_json entry lacks a complete dual-side span "
                "(p0/p1/rg0/rg1) -- refusing to select among incomplete pairs "
                f"({type(exc).__name__})"
            ) from exc
    return out


def select_primary_alignment(
    alignments: Sequence[RefAlignment],
) -> Optional[RefAlignment]:
    """The ONE alignment a single evidence row carries. This rule is FROZEN.

    Largest page-side extent, tie-broken `page_start` ASC, `page_end` ASC, `w_start`
    ASC, `w_end` ASC -- a total order over integers, so the answer does not depend on
    the order the entries arrived in.

    Verified against the producer rather than against itself: this selection
    reproduces one of the producer's own evidence rows on 381,341 of 381,341 rows.
    Changing it would move the work-side offsets of the whole shipped asset.
    """
    best, best_key = None, None
    for item in alignments:
        key = (-(item.page_end - item.page_start), item.page_start, item.page_end,
               item.w_start, item.w_end)
        if best_key is None or key < best_key:
            best, best_key = item, key
    return best


def merge_witnessed_spans(
    spans: Iterable[Tuple[int, int]]
) -> List[Tuple[int, int]]:
    """Half-open work-side spans -> the same witnessed text, without repetition.

    Merges spans that OVERLAP OR TOUCH, and never spans with a gap between them. The
    distinction is the whole point and it is not the same rule as the citation's:

      * merging `[100,200)` with `[150,300)` into `[100,300)` loses nothing -- every
        offset in the union is witnessed by one of the two.
      * merging `[100,200)` with `[201,300)` would claim offset 200, which nothing
        witnesses. That is the fabrication the never-bridge-a-gap rule forbids.

    Needed because the producer records overlapping CANDIDATE alignments: measured
    over the real rows, consecutive distinct work spans overlap 48,011 times and nest
    12,519 times, and the merge folds 65,048 of 506,011 alignments (12.9%), taking
    rows with more than one piece from 22.0% to 12.8%.

    WHAT THIS IS AND IS NOT, measured rather than assumed. It does NOT fix a wrong
    citation: over the 5,664 real rows where the merge folds something and the work has
    a unit table, the rendered citation changes on **0 of them**, because
    `citation_runs` already folds overlapping pieces to the set of citation positions
    they touch. So this is storage hygiene. It belongs in the contract anyway --
    `locus_piece_count` is a published number recomputed from the pieces, and without
    the merge it overstates scatter on every one of those 5,664 rows.

    This is an interval normalization, not a citation decision. The citation's own
    rule -- adjacency in unit ordinal, never in characters -- still applies afterwards
    and is enforced separately by `compress_pieces` and `citation_runs`.
    """
    ordered = sorted((int(a), int(b)) for a, b in spans)
    out: List[Tuple[int, int]] = []
    for start, end in ordered:
        if end < start:
            raise ValueError(f"span ({start}, {end}) has its ends reversed")
        if out and start <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], end))
        else:
            out.append((start, end))
    return out


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
