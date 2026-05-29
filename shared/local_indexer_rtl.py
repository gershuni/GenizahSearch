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

# ---------------------------------------------------------------------------
# Hebrew range constants (PATTERNS.md "Shared Patterns").
# ---------------------------------------------------------------------------
_HEBREW_BLOCK_LO = 0x0590  # RTL gate: full Hebrew block incl. nikud
_HEBREW_BLOCK_HI = 0x07BF
_NIKUD_LO = 0x05B0  # combining marks — EXCLUDE from gap math (D-04/D-06)
_NIKUD_HI = 0x05C7

# Hebrew punctuation (F-B normalize targets): maqaf U+05BE, sof pasuq U+05C3.
_HEBREW_MAQAF = "־"      # ־
_HEBREW_SOF_PASUQ = "׃"  # ׃

# De-space thresholds (D-04 hysteresis — NO Otsu, that is deferred).
_HARD_GAP_MULT = 1.8   # gap > 1.8 x median -> always a word boundary
_MID_GAP_MULT = 1.15   # 1.15x < gap <= 1.8x -> break only if corroborated
_LONG_TOKEN_LETTERS = 12  # abnormal-long accumulating unit -> corroboration hint

# Reorder (adapted from Meiri _normalize_span_dir).
MAX_BACKWARD_JUMP = 15.0

# Bracket tables — copied verbatim from pdf_to_docx.py:643-650 (F-C).
_BRACKET_PAIRS = [("(", ")"), ("[", "]"), ("{", "}")]
_MIRROR_OF = {o: c for o, c in _BRACKET_PAIRS}
_MIRROR_OF.update({c: o for o, c in _BRACKET_PAIRS})
_CLOSERS = {c for _, c in _BRACKET_PAIRS}
_BRACKETS = set(_MIRROR_OF.keys())

_PUNCT = set(".,;:!?)\"'" + _HEBREW_MAQAF + _HEBREW_SOF_PASUQ)


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
    return _NIKUD_LO <= cp <= _NIKUD_HI


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


def _order_unit_text_rtl(members: list[dict]) -> str:
    """Build a word unit's text in correct R->L consonant order (M3).

    Order the unit's BASE consonants by DESCENDING center-x (right-to-left),
    keeping each nikud combining mark attached to its base consonant (a mark
    belongs to the nearest base by center-x; emitted immediately AFTER its base).
    A word whose glyphs were emitted visual-LTR (logically-first consonant at the
    HIGHEST center-x, logically-last at the lowest) thus reads correctly.
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
    out: list[str] = []
    for b in bases_rtl:
        out.append(_glyph_char(b))
        for m in sorted(attached[id(b)], key=lambda g: g.get("original_order", 0)):
            out.append(_glyph_char(m))
    return "".join(out)


def despace_line_to_word_units(line_glyphs: list[dict]) -> list[dict]:
    """Adaptive letter-spacing de-collapse -> word-unit bbox-unions (D-04/D-05/M3).

    Consumes the richer glyph record. Does NOT sort glyphs by x up front (that
    destroys original_order and can reverse RTL letters). For an RTL line:

      1. metric glyphs = glyphs EXCLUDING nikud (a mark between two consonants
         must not read as a boundary — D-04/D-06).
      2. gaps measured on center-x of consecutive metric glyphs (ascending x).
      3. HARD break: gap > 1.8 x median_gap.
         MID break: 1.15x < gap <= 1.8x, but only if corroborated by an embedded
         space glyph, punctuation, a span_id/font boundary (HIGH-5), or an
         abnormally long accumulating unit.
      4. each word unit's text is built by DESCENDING center-x (M3), nikud kept
         on its base; bbox = union of members; original_order = min of members.

    Units are returned sorted by ascending original_order (rawdict emission
    order) so the downstream reorder (reorder_word_units_rtl) can re-segment by
    original_order + x-jumps. Synthetic zero-bbox space glyphs are NEVER created
    (Codex HIGH-3 / D-05): pure-space glyphs are dropped from text and used only
    as boundary hints.
    """
    text = "".join(_glyph_char(g) for g in line_glyphs)
    if rtl_ratio(text) <= 0.4:
        return _ltr_word_units(line_glyphs)

    space_glyphs = [g for g in line_glyphs if _glyph_char(g) == " "]
    content = [g for g in line_glyphs if _glyph_char(g).strip() != ""]
    if len(content) < 2:
        if not content:
            return []
        return [{
            "text": _order_unit_text_rtl(content),
            "bbox": _bbox_union(content),
            "original_order": min(g.get("original_order", 0) for g in content),
        }]

    # Visual sequence (ascending center-x) for boundary detection. Nikud rides
    # along inside its base's accumulating unit but is excluded from gap math.
    ordered = sorted(content, key=_center_x)
    metric = [g for g in ordered if not _is_nikud_glyph(g)]
    base_centers = [_center_x(g) for g in metric]
    base_gaps = [base_centers[i + 1] - base_centers[i]
                 for i in range(len(base_centers) - 1)]
    if not base_gaps:
        return [{
            "text": _order_unit_text_rtl(content),
            "bbox": _bbox_union(content),
            "original_order": min(g.get("original_order", 0) for g in content),
        }]
    median_gap = statistics.median(base_gaps)
    hard = _HARD_GAP_MULT * median_gap
    mid = _MID_GAP_MULT * median_gap

    def _space_between(lo: float, hi: float) -> bool:
        return any(lo < _center_x(s) < hi for s in space_glyphs)

    def _base_letter_count(unit: list[dict]) -> int:
        return sum(1 for g in unit if not _is_nikud_glyph(g)
                   and _HEBREW_BLOCK_LO <= ord(_glyph_char(g)[0]) <= _HEBREW_BLOCK_HI)

    units_visual: list[list[dict]] = []
    current: list[dict] = []
    prev_base: dict | None = None
    for g in ordered:
        if _is_nikud_glyph(g):
            current.append(g)
            continue
        if prev_base is None:
            current.append(g)
            prev_base = g
            continue
        gap = _center_x(g) - _center_x(prev_base)
        boundary = False
        if gap > hard:
            boundary = True
        elif gap > mid:
            corroborated = (
                _space_between(_center_x(prev_base), _center_x(g))
                or _glyph_char(prev_base) in _PUNCT
                or _glyph_char(g) in _PUNCT
                or g.get("span_id") != prev_base.get("span_id")
                or g.get("font") != prev_base.get("font")
                or _base_letter_count(current) > _LONG_TOKEN_LETTERS
            )
            boundary = corroborated
        if boundary:
            units_visual.append(current)
            current = [g]
        else:
            current.append(g)
        prev_base = g
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
