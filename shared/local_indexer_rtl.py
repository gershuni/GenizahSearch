"""Pure, fitz-free RTL text-reconstruction helpers for the LOCAL PDF extractor.

Phase 102 (D-F13/D-F14). These helpers operate on already-parsed glyph-trace
dicts (originating from PyMuPDF ``page.get_text("rawdict")`` in Plan 02) and
contain NO ``import fitz`` / ``import pymupdf`` so they are independently
unit-testable on committed glyph-trace JSON fixtures (Codex LOW-11).

The unit a helper consumes is a RICHER glyph record (REVIEWS HIGH-4/HIGH-5)::

    {"c": "מ", "bbox": [x0, y0, x1, y1], "font": "...", "size": 11.0,
     "span_id": int,           # monotonic id of the originating rawdict span
     "original_order": int}    # index in ORIGINAL rawdict reading order (NOT x-sorted)

bbox floats are PDF points; x increases left -> right. ``span_id`` and
``original_order`` are annotated by Plan 02 while flattening spans. The de-space
hysteresis uses span_id/font boundaries as mid-gap corroboration (HIGH-5), and
the reorder segments by ``original_order`` + center-x jumps (NOT a destructive
global x-sort, which would reverse RTL letters / destroy the Meiri jump signal —
HIGH-4).

Pipeline (D-05): original glyphs -> de-space to word units (bbox unions, NO
synthetic spaces) -> reorder units (RTL-gated) -> per-line string -> bracket fix
-> punctuation normalize.
"""

from __future__ import annotations

import re
import statistics
import unicodedata

# De-space threshold (2026-05-31 revision — edge-gap, NOT center-gap-vs-median).
# A word boundary is the inter-glyph WHITESPACE (edge-to-edge: next.x0 - prev.x1)
# exceeding this fraction of the font size. The corpus is bimodal in edge-gap /
# font-size space: intra-word + letter-spacing tracking clusters <= ~0.2*em,
# word spaces sit at ~0.6-1.3*em, with a stable valley at ~0.4-0.5*em (verified
# across 90 books / 300K+ inter-glyph gaps). 0.45 sits in that valley — safely
# below the word-space floor (~0.53*em, p5) yet well above letter-spacing. The
# OLD metric (center-x to center-x vs 1.8x per-line median) conflated letter
# WIDTH with spacing, so wide letters (מ/ש/ה) were shattered off justified words
# and the median drifted inside letter-spaced runs; edge-gap is width-invariant.
#
# PER-LINE OTSU VALLEY (2026-05-31, second iteration). A single global fraction
# cannot separate every book: word-spaces are ~0.3*em in tightly-set modern
# books (Ravitzky), ~0.7*em in normal books, while letter-spaced HEADINGS track
# letters at ~0.5*em — these overlap, so any fixed threshold either MERGES tight
# words or SHATTERS headings. But within a single line the intra-gap and word-gap
# clusters are always cleanly bimodal; only the valley LOCATION moves. So the
# threshold is found per line by 1-D Otsu (the value minimizing within-cluster
# variance of the normalized edge gaps), clipped + bounded:
#   * outlier clip: gaps above _GAP_OUTLIER_CAP (column breaks) are pinned to the
#     cap so they always split but don't drag the valley up.
#   * spread guard: if the line's gaps span < _GAP_MIN_SPREAD it is unimodal
#     (one word, or a uniformly letter-spaced single word) -> no internal split.
#   * the valley is bounded to [_GAP_MIN_FRACTION, _GAP_MAX_FRACTION] of the font
#     size so noise can't split letters and a real word-space always splits.
_GAP_OUTLIER_CAP = 1.2      # x font size — clip column-break/figure gaps
_GAP_MIN_SPREAD = 0.12      # x font size — below this the line is one cluster
_GAP_MIN_FRACTION = 0.12    # x font size — valley floor (intra noise never splits)
_GAP_MAX_FRACTION = 1.10    # x font size — valley ceiling (big gaps always split)

# Secondary boundary signal (2026-05-31 b): use a *clean* embedded space glyph as
# a word boundary when the Otsu edge-gap test can't see the word-space (zero-WIDTH
# space glyphs in tightly-set headings/citations). Gated locally so letter-spacing
# (a space between EVERY letter) is NOT mistaken for word-spaces. Kill-switch for
# A/B measurement and emergency revert; production default is ON.
_SPACE_BOUNDARY_ENABLED = True

# Reorder (adapted from Meiri _normalize_span_dir).
MAX_BACKWARD_JUMP = 15.0

# Bracket tables — copied verbatim from pdf_to_docx.py:643-650 (F-C).
_BRACKET_PAIRS = [("(", ")"), ("[", "]"), ("{", "}")]
_MIRROR_OF = {o: c for o, c in _BRACKET_PAIRS}
_MIRROR_OF.update({c: o for o, c in _BRACKET_PAIRS})
_CLOSERS = {c for _, c in _BRACKET_PAIRS}
_BRACKETS = set(_MIRROR_OF.keys())


# ---------------------------------------------------------------------------
# Primitives.
# ---------------------------------------------------------------------------
def rtl_ratio(text: str) -> float:
    """Fraction of RTL chars (bidi class R/AL/AN) among alpha chars.

    Mirrors ``shared/local_indexer._rtl_ratio``. The per-helper LTR gate is
    ``rtl_ratio(text) <= 0.4`` (LTR / numeric / empty / mixed-LTR pass through).
    """
    alpha = [c for c in text if c.isalpha()]
    if not alpha:
        return 0.0
    rtl = sum(1 for c in alpha if unicodedata.bidirectional(c) in ("R", "AL", "AN"))
    return rtl / len(alpha)


def _is_nikud(cp: int) -> bool:
    """True for Hebrew combining marks — nikud points AND cantillation te'amim.

    Uses the Unicode general category ``Mn`` (Mark, nonspacing) instead of the
    old fixed range 0x05B0-0x05C7. That range was wrong in both directions: it
    INCLUDED the spacing punctuation maqaf (U+05BE, ``Pd``), paseq (U+05C0),
    sof-pasuq (U+05C3) and nun-hafukha (U+05C6) — which must read as base glyphs
    (treating the maqaf as a vowel mark corrupted ranges like ``סב־סג``) — and
    EXCLUDED the cantillation te'amim at 0x0591-0x05AF (``Mn``), which let
    accents pollute the gap math on vocalized/biblical text. ``Mn`` classifies
    all of them correctly (2026-05-31 de-space revision).
    """
    return unicodedata.category(chr(cp)) == "Mn"


def _center_x(glyph: dict) -> float:
    bbox = glyph["bbox"]
    return (bbox[0] + bbox[2]) / 2.0


def _glyph_char(glyph: dict) -> str:
    return glyph.get("c", "") or ""


def _is_nikud_glyph(glyph: dict) -> bool:
    c = _glyph_char(glyph)
    return bool(c) and _is_nikud(ord(c[0]))


def _bbox_union(glyphs: list[dict]) -> list[float]:
    xs0 = [g["bbox"][0] for g in glyphs]
    ys0 = [g["bbox"][1] for g in glyphs]
    xs1 = [g["bbox"][2] for g in glyphs]
    ys1 = [g["bbox"][3] for g in glyphs]
    return [min(xs0), min(ys0), max(xs1), max(ys1)]


def _line_y_center(glyphs: list[dict]) -> float:
    return statistics.median([(g["bbox"][1] + g["bbox"][3]) / 2.0 for g in glyphs])


def _glyph_size(glyph: dict) -> float:
    sz = glyph.get("size")
    if sz:
        return float(sz)
    bbox = glyph["bbox"]
    return float(bbox[3] - bbox[1])


# ---------------------------------------------------------------------------
# D-02: baseline / font-size-derived line grouping (NOT a fixed y_tol=2.5).
# ---------------------------------------------------------------------------
def group_lines_by_baseline(raw_lines: list[dict]) -> list[list[dict]]:
    """Group rawdict line dicts into visual rows by a baseline/font-size tol.

    The tolerance is derived from the median size of BASE glyphs (excluding
    nikud combining marks and superscript footnote refs — glyphs whose size is
    < 0.7 x the line median size), so a vocalized Hebrew line is kept as one
    row (not split by its lower nikud marks) and a superscript footnote ref is
    not merged into a neighbouring row.

    Returns a list of rows; each row is a list of the original line dicts whose
    baselines fall within tolerance of each other.
    """
    if not raw_lines:
        return []

    # Global base-text size = median of NON-NIKUD glyph sizes across all lines.
    # Tolerance is derived from this (D-02 — baseline from base glyphs only), so a
    # fitz-split nikud-only row merges back into its base line rather than the
    # tiny nikud size shrinking the tolerance.
    non_nikud_sizes = [
        _glyph_size(c)
        for ln in raw_lines
        for sp in ln.get("spans", [])
        for c in sp.get("chars", [])
        if not _is_nikud_glyph(c)
    ]
    all_sizes = non_nikud_sizes or [
        _glyph_size(c)
        for ln in raw_lines
        for sp in ln.get("spans", [])
        for c in sp.get("chars", [])
    ]
    if not all_sizes:
        return [[ln] for ln in raw_lines]
    global_median = statistics.median(all_sizes)
    tolerance = 0.6 * global_median

    measured: list[tuple[float, dict]] = []  # (baseline_y, line)
    for ln in raw_lines:
        glyphs = [c for sp in ln.get("spans", []) for c in sp.get("chars", [])]
        if not glyphs:
            continue
        # base glyphs for the baseline: not nikud, not tiny superscript
        # (size < 0.7 x the global base size).
        base = [
            g for g in glyphs
            if not _is_nikud_glyph(g) and _glyph_size(g) >= 0.7 * global_median
        ]
        ref = base or glyphs
        baseline_y = statistics.median([g["bbox"][3] for g in ref])  # bottom edge
        measured.append((baseline_y, ln))

    if not measured:
        return []

    measured.sort(key=lambda t: t[0])
    rows: list[list[dict]] = [[measured[0][1]]]
    row_baseline = measured[0][0]
    for baseline_y, ln in measured[1:]:
        if abs(baseline_y - row_baseline) <= tolerance:
            rows[-1].append(ln)
        else:
            rows.append([ln])
            row_baseline = baseline_y
    return rows


# ---------------------------------------------------------------------------
# D-04 / D-05 / M3: adaptive de-space -> word-unit bbox-unions.
# ---------------------------------------------------------------------------
def _word_gap_fraction(norm_gaps: list[float]) -> float:
    """Per-line word-boundary threshold as a fraction of the font size.

    ``norm_gaps`` are consecutive base-glyph EDGE gaps already divided by the
    leading glyph's font size. Returns the fraction F such that an edge gap >
    F * font_size is a word boundary. Found by 1-D Otsu on the gaps (clipped +
    bounded — see the _GAP_* constants). Lines whose gaps are too uniform to be
    bimodal (one word, or a single uniformly letter-spaced word) return
    _GAP_MAX_FRACTION, i.e. effectively "do not split internally".
    """
    if not norm_gaps:
        return _GAP_MAX_FRACTION
    vals = sorted(min(max(g, 0.0), _GAP_OUTLIER_CAP) for g in norm_gaps)
    n = len(vals)
    if n < 2 or (vals[-1] - vals[0]) < _GAP_MIN_SPREAD:
        return _GAP_MAX_FRACTION  # unimodal: no intra/inter split on this line
    # 1-D Otsu: pick the split maximizing between-cluster variance.
    prefix = [0.0] * (n + 1)
    for i, v in enumerate(vals):
        prefix[i + 1] = prefix[i] + v
    best_thr, best_var = _GAP_MAX_FRACTION, -1.0
    for k in range(1, n):
        w0, w1 = k, n - k
        mean0 = prefix[k] / w0
        mean1 = (prefix[n] - prefix[k]) / w1
        between = w0 * w1 * (mean1 - mean0) ** 2
        if between > best_var:
            best_var = between
            best_thr = (vals[k - 1] + vals[k]) / 2.0
    return min(max(best_thr, _GAP_MIN_FRACTION), _GAP_MAX_FRACTION)


def _ltr_word_units(line_glyphs: list[dict]) -> list[dict]:
    """LTR pass-through: split on whitespace glyphs, preserve emission order.

    LTR letters are NEVER reordered. Each unit carries the original member glyph
    order joined verbatim, a bbox union, and original_order = min of members.
    """
    units: list[dict] = []
    current: list[dict] = []

    def _flush() -> None:
        if current:
            text = "".join(_glyph_char(g) for g in current)
            if text:
                units.append({
                    "text": text,
                    "bbox": _bbox_union(current),
                    "original_order": min(g.get("original_order", i)
                                          for i, g in enumerate(current)),
                })
        current.clear()

    for g in sorted(line_glyphs, key=lambda x: x.get("original_order", 0)):
        if _glyph_char(g).strip() == "":  # whitespace glyph -> boundary
            _flush()
        else:
            current.append(g)
    _flush()
    return units


def _is_ltr_base(glyph: dict) -> bool:
    """True for a glyph that reads LEFT-to-right: digits, Latin, and the numeric
    separators that ride inside a number/Latin token (so ``1977`` / ``194-212`` /
    ``p20`` reverse as one block, not per-digit). Bidi classes L (Latin),
    EN (European digit), ES/ET/CS (numeric +-, %, ,.: separators)."""
    c = _glyph_char(glyph)
    if not c:
        return False
    return unicodedata.bidirectional(c[0]) in ("L", "EN", "ES", "ET", "CS")


def _order_unit_text_rtl(members: list[dict]) -> str:
    """Build a word unit's text in correct R->L consonant order (M3).

    Order the unit's BASE consonants by DESCENDING center-x (right-to-left),
    keeping each nikud combining mark attached to its base consonant (a mark
    belongs to the nearest base by center-x; emitted immediately AFTER its base).
    A word whose glyphs were emitted visual-LTR (logically-first consonant at the
    HIGHEST center-x, logically-last at the lowest) thus reads correctly.

    Embedded LEFT-to-right runs (digits / Latin — e.g. a year ``1977`` or a page
    range ``194-212`` fused into the unit) are then flipped back to ascending
    center-x: the standard bidi "reverse embedded level run" step. Without it the
    blanket descending sort reverses numbers (``1977`` -> ``7791``). A run must
    contain at least one digit/Latin glyph to flip (lone neutral separators stay
    in their RTL slot).
    """
    bases = [g for g in members if not _is_nikud_glyph(g)]
    marks = [g for g in members if _is_nikud_glyph(g)]
    if not bases:
        return "".join(_glyph_char(g) for g in members)

    # Attach each nikud mark to the nearest base consonant by center-x.
    attached: dict[int, list[dict]] = {id(b): [] for b in bases}
    for m in marks:
        mx = _center_x(m)
        nearest = min(bases, key=lambda b: abs(_center_x(b) - mx))
        attached[id(nearest)].append(m)

    # M3: order base consonants by DESCENDING center-x (right-to-left reading).
    bases_rtl = sorted(bases, key=_center_x, reverse=True)

    # Re-flip maximal LTR runs (digits/Latin) back to ascending so embedded
    # numbers read correctly (1977 must not become 7791). A run only flips if it
    # carries a real digit/Latin glyph, not just separators.
    n = len(bases_rtl)
    i = 0
    while i < n:
        if _is_ltr_base(bases_rtl[i]):
            j = i
            while j + 1 < n and _is_ltr_base(bases_rtl[j + 1]):
                j += 1
            run = bases_rtl[i:j + 1]
            if any(unicodedata.bidirectional(_glyph_char(g)[0]) in ("L", "EN")
                   for g in run if _glyph_char(g)):
                bases_rtl[i:j + 1] = list(reversed(run))
            i = j + 1
        else:
            i += 1

    out: list[str] = []
    for b in bases_rtl:
        out.append(_glyph_char(b))
        for m in sorted(attached[id(b)], key=lambda g: g.get("original_order", 0)):
            out.append(_glyph_char(m))
    return "".join(out)


def _is_space_glyph(glyph: dict) -> bool:
    """True for an emitted whitespace glyph (a boundary HINT, not real text)."""
    c = _glyph_char(glyph)
    return bool(c) and c.strip() == ""


def despace_line_to_word_units(line_glyphs: list[dict]) -> list[dict]:
    """Letter-spacing de-collapse -> word-unit bbox-unions (D-04/D-05/M3).

    Consumes the richer glyph record. Does NOT sort glyphs by x up front (that
    destroys original_order and can reverse RTL letters). For an RTL line:

      1. nikud combining marks (Unicode ``Mn``) ride along inside their base's
         accumulating unit but are excluded from the gap math (a mark between
         two consonants must not read as a boundary — D-04/D-06).
      2. the primary boundary signal is EDGE-TO-EDGE whitespace between
         consecutive base glyphs in ascending-x order (next.x0 - prev.x1), NOT
         center-to-center. Center distance conflated letter width with spacing
         and shattered wide letters off justified words (2026-05-31 revision).
      3. word boundary when edge_gap > gap_fraction * font_size, where
         gap_fraction is the per-line Otsu valley of the normalized edge gaps
         (see _word_gap_fraction) — it adapts to each line's own intra/inter
         bimodal spacing.
      4. SECONDARY boundary signal = an embedded space GLYPH (U+0020) that is a
         *clean* word-space (2026-05-31 b). Some PDFs encode the inter-word space
         as a zero-WIDTH space glyph (the gap collapses to ~0 em, so the Otsu gap
         test in (3) cannot see it — e.g. tightly-set headings/citations where
         `פרנץ רוזנצווייג ושמואל` renders as one run). The space glyph IS reliable
         there. But it cannot be used unconditionally: justified Hebrew encodes
         letter-spacing as a literal space between EVERY letter, so a space marks
         letter-spacing as often as a word break. The two are told apart LOCALLY:
         a real word-space has NO space glyph at either immediately-adjacent
         inter-base position, whereas a letter-spaced space always does (its
         neighbours are spaced too). So a space glyph forces a boundary ONLY when
         neither adjacent inter-base position also carries a space. This is purely
         additive — a line with no space glyphs splits exactly as it did on (3).
      5. each word unit's text is built by DESCENDING center-x (M3), nikud kept
         on its base; bbox = union of members; original_order = min of members.

    Units are returned sorted by ascending original_order (rawdict emission
    order) so the downstream reorder (reorder_word_units_rtl) can re-segment by
    original_order + x-jumps. Synthetic zero-bbox space glyphs are NEVER created
    (Codex HIGH-3 / D-05): pure-space glyphs are dropped from the unit text and
    serve only as the corroborating boundary hint described in point 4.
    """
    text = "".join(_glyph_char(g) for g in line_glyphs)
    if rtl_ratio(text) <= 0.4:
        return _ltr_word_units(line_glyphs)

    content = [g for g in line_glyphs if _glyph_char(g).strip() != ""]
    if len(content) < 2:
        if not content:
            return []
        return [{
            "text": _order_unit_text_rtl(content),
            "bbox": _bbox_union(content),
            "original_order": min(g.get("original_order", 0) for g in content),
        }]

    # Visual sequence (ascending center-x) of ALL glyphs, so embedded space
    # glyphs can be read as inter-base boundary hints (point 4) while nikud rides
    # along inside its base's unit and is excluded from the gap math.
    vis_all = sorted(line_glyphs, key=_center_x)

    # Pass 1: base-glyph sequence + a parallel space_before[] flag (a space glyph
    # sits between base i-1 and base i). Nikud and empty glyphs are skipped here.
    bases_seq: list[dict] = []
    space_before: list[bool] = []
    pending_space = False
    for g in vis_all:
        if _is_nikud_glyph(g):
            continue
        if _is_space_glyph(g):
            pending_space = True
            continue
        if not _glyph_char(g):
            continue
        bases_seq.append(g)
        space_before.append(pending_space)
        pending_space = False

    if len(bases_seq) < 2:
        return [{
            "text": _order_unit_text_rtl(content),
            "bbox": _bbox_union(content),
            "original_order": min(g.get("original_order", 0) for g in content),
        }]

    # Per-line word-gap threshold via 1-D Otsu on the base-glyph EDGE gaps (each
    # normalized by the leading glyph's font size). This adapts to the line's own
    # bimodal intra/inter spacing — tight modern books (~0.3*em word-spaces),
    # normal books (~0.7*em) and letter-spaced headings (~0.5*em tracking, words
    # >0.9*em) each get a valley in the right place where a fixed fraction cannot.
    norm_gaps = [
        (bases_seq[i + 1]["bbox"][0] - bases_seq[i]["bbox"][2])
        / (_glyph_size(bases_seq[i]) or _glyph_size(bases_seq[i + 1]) or 1.0)
        for i in range(len(bases_seq) - 1)
    ]
    gap_fraction = _word_gap_fraction(norm_gaps)

    # Boundary decision per inter-base position i (1 .. len-1): the Otsu edge-gap
    # test OR a clean word-space glyph (point 4). space_before[i] is the space
    # between base i-1 and base i; its left/right neighbours are positions i-1 and
    # i+1 (a leading space at index 0 is not an inter-base position).
    n = len(bases_seq)
    boundary = [False] * n
    for i in range(1, n):
        edge_gap = bases_seq[i]["bbox"][0] - bases_seq[i - 1]["bbox"][2]
        font_size = _glyph_size(bases_seq[i - 1]) or _glyph_size(bases_seq[i]) or 1.0
        gap_boundary = edge_gap > gap_fraction * font_size
        sp_left = space_before[i - 1] if i - 1 >= 1 else False
        sp_right = space_before[i + 1] if i + 1 < n else False
        clean_space = (
            _SPACE_BOUNDARY_ENABLED
            and space_before[i]
            and not (sp_left or sp_right)
        )
        boundary[i] = gap_boundary or clean_space

    # Pass 2: walk visual order, splitting bases at boundary positions; nikud
    # rides into the current unit (re-attached to its base by _order_unit_text_rtl).
    units_visual: list[list[dict]] = []
    current: list[dict] = []
    base_idx = -1
    for g in vis_all:
        if _is_space_glyph(g) or not _glyph_char(g):
            continue
        if _is_nikud_glyph(g):
            current.append(g)
            continue
        base_idx += 1
        if base_idx >= 1 and boundary[base_idx] and current:
            units_visual.append(current)
            current = [g]
        else:
            current.append(g)
    if current:
        units_visual.append(current)

    units = [{
        "text": _order_unit_text_rtl(members),
        "bbox": _bbox_union(members),
        "original_order": min(g.get("original_order", 0) for g in members),
    } for members in units_visual if members]

    units.sort(key=lambda u: u["original_order"])
    return units


def line_text_from_word_units(units: list[dict]) -> str:
    return " ".join(u["text"] for u in units)


# ---------------------------------------------------------------------------
# Task 3: RTL word-unit reorder (original_order + x-jumps) + bracket / punct.
# ---------------------------------------------------------------------------
def _unit_center_x(unit: dict) -> float:
    bbox = unit["bbox"]
    return (bbox[0] + bbox[2]) / 2.0


def reorder_word_units_rtl(units: list[dict], line_text: str) -> list[dict]:
    """RTL-gated word-unit reorder, adapted from Meiri ``_normalize_span_dir``.

    Iterates units in ORIGINAL reading order (sorted by ``original_order`` — NOT
    by x), tracking center-x direction jumps; it does NOT do a destructive global
    x-sort (HIGH-4). Reorders whole UNITS only — the intra-unit letter order was
    already fixed in de-space (M3).

      * LTR guard FIRST: ``rtl_ratio(line_text) <= 0.4`` -> return units unchanged.
      * Start a new segment when the unit jumps FORWARD (this_center > prev_center)
        or jumps BACKWARD by more than MAX_BACKWARD_JUMP; else extend the segment.
      * One segment -> return unchanged. Else sort segments right-to-left by max
        center-x and flatten; re-reverse embedded digit-only unit runs so
        reference/footnote numbers keep their LTR digit order (F-A).
    """
    if rtl_ratio(line_text) <= 0.4:
        return units
    if len(units) < 2:
        return units

    ordered = sorted(units, key=lambda u: u["original_order"])
    segments: list[list[dict]] = [[ordered[0]]]
    for i in range(1, len(ordered)):
        prev_c = _unit_center_x(ordered[i - 1])
        this_c = _unit_center_x(ordered[i])
        dx = this_c - prev_c
        if dx > 0 or dx < -MAX_BACKWARD_JUMP:
            segments.append([ordered[i]])
        else:
            segments[-1].append(ordered[i])

    if len(segments) == 1:
        return units

    segments.sort(key=lambda seg: -max(_unit_center_x(u) for u in seg))

    flat = [u for seg in segments for u in seg]

    # Re-reverse runs of digit-only units (LTR numbers inside RTL text — F-A).
    def _is_digit_unit(u: dict) -> bool:
        t = u["text"].strip()
        return bool(t) and t.isdigit()

    i = 0
    while i < len(flat):
        if _is_digit_unit(flat[i]):
            j = i
            while j + 1 < len(flat) and _is_digit_unit(flat[j + 1]):
                j += 1
            if j > i:
                flat[i:j + 1] = list(reversed(flat[i:j + 1]))
            i = j + 1
        else:
            i += 1
    return flat


def fix_visual_brackets_rtl(line_text: str) -> str:
    """Mirror reversed bracket pairs back to logical order (F-C).

    Adapts Meiri ``_fix_visual_brackets`` to string granularity (post-reorder we
    no longer hold per-char dicts). RTL-gated; LTR text is untouched. Brackets in
    the reading-order string are paired sequentially; a (closer, matching-opener)
    pair is the visual-encoding signature and gets mirrored.
    """
    if rtl_ratio(line_text) <= 0.4:
        return line_text
    chars = list(line_text)
    bracket_idx = [i for i, ch in enumerate(chars) if ch in _BRACKETS]
    k = 0
    while k + 1 < len(bracket_idx):
        ai, bi = bracket_idx[k], bracket_idx[k + 1]
        a, b = chars[ai], chars[bi]
        if a in _CLOSERS and b not in _CLOSERS and _MIRROR_OF[a] == b:
            chars[ai] = _MIRROR_OF[a]
            chars[bi] = _MIRROR_OF[b]
        k += 2
    return "".join(chars)


# F-B: collapse a spurious space before ASCII punctuation AND Hebrew sof-pasuq
# (U+05C3 ׃) / maqaf (U+05BE ־). The class is NOT ASCII-only.
_PUNCT_SPACING_RE = re.compile(r"\s+([.,;:!?)־׃])")


def normalize_punctuation_spacing(line_text: str) -> str:
    """Collapse a whitespace run immediately preceding punctuation (F-B).

    Covers ASCII ``.,;:!?)`` AND the Hebrew punctuation codepoints maqaf
    (U+05BE ־) and sof pasuq (U+05C3 ׃). Conservative — only collapses a
    whitespace run that immediately precedes one of these marks.
    """
    return _PUNCT_SPACING_RE.sub(r"\1", line_text)
