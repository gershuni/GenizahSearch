"""Unit tests for shared/local_indexer_rtl.py (Phase 102-01).

Drive the pure RTL reconstruction helpers from committed glyph-trace JSON
fixtures (bbox-dependent — text-only fixtures cannot pin these — Codex LOW-11).
"""
import json
import os

import pytest

from shared import local_indexer_rtl as rtl

FIXTURE_DIR = os.path.join(
    os.path.dirname(__file__), "fixtures", "local_indexer", "glyph_traces"
)

ALL_FIXTURES = [
    "letter_spaced_line.json",
    "letter_spaced_reversed_line.json",
    "rtl_running_header.json",
    "ltr_latin_line.json",
    "undersplit_line.json",
    "overmerge_line.json",
    "intra_word_visual_ltr.json",
]


def load(name):
    with open(os.path.join(FIXTURE_DIR, name), encoding="utf-8") as fh:
        return json.load(fh)


def line_glyphs(fixture, line_idx=0):
    ln = fixture["lines"][line_idx]
    return [c for sp in ln.get("spans", []) for c in sp.get("chars", [])]


# ---------------------------------------------------------------------------
# Task 1 — fixtures present + richer contract + classification + grouping.
# ---------------------------------------------------------------------------
def test_all_fixtures_present():
    for name in ALL_FIXTURES:
        assert os.path.exists(os.path.join(FIXTURE_DIR, name)), name


@pytest.mark.parametrize("name", ALL_FIXTURES)
def test_glyphs_carry_richer_contract(name):
    fx = load(name)
    for li in range(len(fx["lines"])):
        for gl in line_glyphs(fx, li):
            assert "span_id" in gl, name
            assert "original_order" in gl, name
            assert "bbox" in gl and len(gl["bbox"]) == 4, name


def test_rtl_ratio_classifies():
    assert rtl.rtl_ratio("מלחמת") > 0.4
    assert rtl.rtl_ratio("Northwest") <= 0.4
    assert rtl.rtl_ratio("") == 0.0


def test_word_gap_fraction_otsu_valley():
    # Per-line Otsu valley adapts to each line's bimodal intra/inter spacing.
    big = rtl._GAP_MAX_FRACTION
    lo, hi = rtl._GAP_MIN_FRACTION, rtl._GAP_MAX_FRACTION
    # Tight book (Ravitzky): intra ~0.0, word ~0.3 -> valley LOW, between them.
    tight = rtl._word_gap_fraction([0.0, 0.0, 0.0, 0.32, 0.0, 0.0, 0.35, 0.0, 0.0])
    assert lo <= tight < 0.32
    # Normal book: intra ~0.07, word ~0.7 -> valley in the middle.
    normal = rtl._word_gap_fraction([0.07, 0.05, 0.06, 0.70, 0.05, 0.72, 0.06])
    assert 0.07 < normal < 0.70
    # Letter-spaced heading: tracking ~0.5, words ~1.2 -> valley HIGH (> tracking).
    heading = rtl._word_gap_fraction([0.5, 0.48, 0.5, 1.2, 0.49, 0.5, 1.25])
    assert heading > 0.5
    # Single tight word (all touching) -> unimodal -> no internal split.
    assert rtl._word_gap_fraction([0.0, 0.0, 0.0, 0.0]) == big
    # Uniformly letter-spaced single word (all ~0.5, low spread) -> no split.
    assert rtl._word_gap_fraction([0.5, 0.49, 0.51, 0.5, 0.5]) == big
    assert rtl._word_gap_fraction([]) == big
    # A genuine word-gap-scale line with a column-break outlier: the valley sits
    # below the (large) word gaps so they split, and the outlier always splits.
    val = rtl._word_gap_fraction([0.05, 0.05, 0.8, 0.05, 0.85, 14.0])
    assert 0.05 < val < 0.8
    # Bounds are ordered (sanity).
    assert lo < hi <= rtl._GAP_OUTLIER_CAP


def test_is_nikud_and_center_x():
    # nikud points + cantillation te'amim are combining marks (Mn).
    assert rtl._is_nikud(0x05B0) and rtl._is_nikud(0x05C7)  # sheva, qamats qatan
    assert rtl._is_nikud(0x0591)  # etnahta — a te'am the OLD 0x05B0-0x05C7 missed
    assert not rtl._is_nikud(ord("א"))
    # Spacing punctuation in the Hebrew block must NOT count as a combining mark,
    # or it corrupts ordering of ranges like סב־סג (2026-05-31 Mn fix).
    assert not rtl._is_nikud(0x05BE)  # maqaf ־
    assert not rtl._is_nikud(0x05C3)  # sof pasuq ׃
    assert rtl._center_x({"bbox": [10.0, 0.0, 20.0, 5.0]}) == 15.0


@pytest.mark.parametrize("name", [
    "letter_spaced_line.json", "letter_spaced_reversed_line.json",
    "rtl_running_header.json", "ltr_latin_line.json",
    "undersplit_line.json", "overmerge_line.json",
])
def test_single_line_fixtures_group_to_one_row(name):
    fx = load(name)
    assert len(rtl.group_lines_by_baseline(fx["lines"])) == 1


def test_intra_word_fixture_groups_to_two_rows():
    fx = load("intra_word_visual_ltr.json")
    assert len(rtl.group_lines_by_baseline(fx["lines"])) == 2


def test_vocalized_line_stays_one_row():
    # A base line + a fitz-split nikud-only "line" at a near baseline must merge.
    base = {"bbox": [100, 40, 160, 52], "spans": [{"font": "David", "size": 11.0,
            "chars": [
                {"c": "א", "bbox": [100, 40, 107, 52], "size": 11.0,
                 "span_id": 0, "original_order": 0},
                {"c": "ב", "bbox": [108, 40, 115, 52], "size": 11.0,
                 "span_id": 0, "original_order": 1}]}]}
    nikud = {"bbox": [101, 52, 110, 57], "spans": [{"font": "David", "size": 5.0,
             "chars": [
                {"c": "ָ", "bbox": [101, 52, 105, 57], "size": 5.0,
                 "span_id": 1, "original_order": 2}]}]}
    rows = rtl.group_lines_by_baseline([base, nikud])
    assert len(rows) == 1


def test_superscript_footnote_ref_not_merged():
    base = {"bbox": [100, 40, 160, 52], "spans": [{"font": "David", "size": 11.0,
            "chars": [
                {"c": "א", "bbox": [100, 40, 107, 52], "size": 11.0,
                 "span_id": 0, "original_order": 0}]}]}
    sup = {"bbox": [161, 39, 166, 45], "spans": [{"font": "David", "size": 6.0,
           "chars": [
                {"c": "1", "bbox": [161, 39, 166, 45], "size": 6.0,
                 "span_id": 1, "original_order": 1}]}]}
    rows = rtl.group_lines_by_baseline([base, sup])
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# Task 2 — adaptive de-space -> word units (D-04/D-05/M3).
# ---------------------------------------------------------------------------
def _despaced_text(name, line_idx=0):
    fx = load(name)
    units = rtl.despace_line_to_word_units(line_glyphs(fx, line_idx))
    return rtl.line_text_from_word_units(units)


def test_despace_letter_spaced_line():
    fx = load("letter_spaced_line.json")
    assert _despaced_text("letter_spaced_line.json") == fx["expected_despaced"]


def test_despace_undersplit_splits_by_edge_gap():
    # Two words with realistic spacing split by the edge-gap rule; the embedded
    # space glyph is ignored (justified Hebrew puts spaces between letters too).
    fx = load("undersplit_line.json")
    assert _despaced_text("undersplit_line.json") == fx["expected_despaced"]


def test_despace_overmerge_splits_by_edge_gap():
    # Three words in SEPARATE spans with realistic word spacing split into 3
    # units by the edge-gap rule (the span boundary is no longer consulted).
    fx = load("overmerge_line.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx))
    text = rtl.line_text_from_word_units(units)
    assert text == fx["expected_despaced"]
    assert len(units) == 3
    assert "ישנואצליוסף" not in text  # must NOT over-merge (spike residual)


def test_letter_spaced_separate_spans_do_not_shatter():
    # Regression (2026-05-31): the production bug was letter-spaced Hebrew where
    # PyMuPDF emits each glyph as its own span. The OLD center-gap + span/font
    # corroboration shattered such words into single letters ("פירוש" ->
    # "פירו ש"). Edge-gap ignores the span boundary: with intra-letter tracking
    # of ~3pt (< 0.45*11 = 4.95) the word stays whole.
    glyphs = [
        {"c": "ש", "bbox": [330, 40, 337, 52], "size": 11.0, "span_id": 0,
         "font": "David", "original_order": 0},
        {"c": "ל", "bbox": [320, 40, 327, 52], "size": 11.0, "span_id": 1,
         "font": "David", "original_order": 1},
        {"c": "ו", "bbox": [310, 40, 317, 52], "size": 11.0, "span_id": 2,
         "font": "David", "original_order": 2},
        {"c": "ם", "bbox": [300, 40, 307, 52], "size": 11.0, "span_id": 3,
         "font": "David", "original_order": 3},
    ]
    units = rtl.despace_line_to_word_units(glyphs)
    assert len(units) == 1
    assert units[0]["text"] == "שלום"


def test_despace_intra_word_visual_ltr_m3():
    fx = load("intra_word_visual_ltr.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx, 0))
    assert len(units) == 1
    assert units[0]["text"] == "שלום"   # correct R->L (descending center-x)
    assert units[0]["text"] != "םולש"   # NOT the ascending-x order


def test_intra_word_m3_center_x_authoring_rule():
    # ם must have the SMALLEST center-x and ש the LARGEST of the four consonants
    # (proving the descending-x sort yields "שלום").
    fx = load("intra_word_visual_ltr.json")
    glyphs = line_glyphs(fx, 0)
    by_char = {g["c"]: rtl._center_x(g) for g in glyphs}
    consonant_cx = {c: by_char[c] for c in "שלום"}
    assert min(consonant_cx, key=consonant_cx.get) == "ם"
    assert max(consonant_cx, key=consonant_cx.get) == "ש"


def test_despace_nikud_stays_attached_to_base():
    fx = load("intra_word_visual_ltr.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx, 1))
    assert len(units) == 1
    text = units[0]["text"]
    holam = "ֹ"
    assert holam in text
    # The combining mark must immediately FOLLOW its base consonant ו.
    assert text.index(holam) == text.index("ו") + 1


def test_despace_ltr_passthrough_untouched():
    fx = load("ltr_latin_line.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx))
    assert [u["text"] for u in units] == ["Northwest", "Semitic", "Dictionary"]
    assert rtl.line_text_from_word_units(units) == fx["expected_despaced"]


def test_despace_units_carry_original_order_and_real_bbox():
    fx = load("letter_spaced_line.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx))
    for u in units:
        assert "original_order" in u
        bx = u["bbox"]
        assert bx[2] > bx[0] and bx[3] > bx[1]  # non-zero area


def test_nikud_between_consonants_no_boundary():
    # A nikud mark sitting between two consonants must not split a word.
    glyphs = [
        {"c": "ש", "bbox": [320, 40, 327, 52], "size": 11.0, "span_id": 0,
         "original_order": 0},
        {"c": "ָ", "bbox": [320, 40, 324, 52], "size": 11.0, "span_id": 0,
         "original_order": 1},
        {"c": "ל", "bbox": [312, 40, 319, 52], "size": 11.0, "span_id": 0,
         "original_order": 2},
        {"c": "ו", "bbox": [304, 40, 311, 52], "size": 11.0, "span_id": 0,
         "original_order": 3},
        {"c": "ם", "bbox": [296, 40, 303, 52], "size": 11.0, "span_id": 0,
         "original_order": 4},
    ]
    units = rtl.despace_line_to_word_units(glyphs)
    assert len(units) == 1


def test_zero_width_space_glyph_forces_boundary():
    # Regression (2026-05-31 b / N1): tightly-set headings/citations encode the
    # inter-word space as a ZERO-WIDTH space glyph — the edge gap collapses to ~0
    # so the Otsu test cannot see it (e.g. "פרנץ רוזנצווייג ושמואל" rendered as one
    # run). The space glyph IS the only reliable boundary there. Two tight words
    # with a single clean space glyph between them must split into 2 units.
    glyphs = [
        {"c": "ב", "bbox": [10, 40, 20, 52], "size": 11.0, "original_order": 0},
        {"c": "א", "bbox": [21, 40, 31, 52], "size": 11.0, "original_order": 1},
        {"c": " ", "bbox": [31, 40, 33, 52], "size": 11.0, "original_order": 2},
        {"c": "ד", "bbox": [33, 40, 43, 52], "size": 11.0, "original_order": 3},
        {"c": "ג", "bbox": [44, 40, 54, 52], "size": 11.0, "original_order": 4},
        {"c": "ה", "bbox": [55, 40, 65, 52], "size": 11.0, "original_order": 5},
    ]
    assert len(rtl.despace_line_to_word_units(glyphs)) == 2
    # The kill-switch proves it is the space glyph (not the ~0 gap) doing the work.
    rtl._SPACE_BOUNDARY_ENABLED = False
    try:
        assert len(rtl.despace_line_to_word_units(glyphs)) == 1
    finally:
        rtl._SPACE_BOUNDARY_ENABLED = True


def test_letter_spaced_run_spaces_suppressed():
    # Regression (2026-05-31 b / N1): justified Hebrew encodes letter-spacing as a
    # space between EVERY letter. A space glyph must NOT split such a run, else a
    # heading like "ה כ ו כ ב י ם" shatters. The space signal is gated locally:
    # a space whose immediate neighbour position also carries a space is treated
    # as letter-spacing, not a word break. A single letter-spaced word (space +
    # uniform ~0.45*em tracking between every letter) stays ONE unit.
    glyphs = [
        {"c": "ם", "bbox": [10, 40, 17, 52], "size": 11.0, "original_order": 0},
        {"c": " ", "bbox": [18, 40, 21, 52], "size": 11.0, "original_order": 1},
        {"c": "ו", "bbox": [22, 40, 29, 52], "size": 11.0, "original_order": 2},
        {"c": " ", "bbox": [30, 40, 33, 52], "size": 11.0, "original_order": 3},
        {"c": "ל", "bbox": [34, 40, 41, 52], "size": 11.0, "original_order": 4},
        {"c": " ", "bbox": [42, 40, 45, 52], "size": 11.0, "original_order": 5},
        {"c": "ש", "bbox": [46, 40, 53, 52], "size": 11.0, "original_order": 6},
    ]
    units = rtl.despace_line_to_word_units(glyphs)
    assert len(units) == 1
    assert units[0]["text"] == "שלום"


# ---------------------------------------------------------------------------
# Task 3 — reorder + bracket fix + punctuation normalize.
# ---------------------------------------------------------------------------
def _reordered_text(name):
    fx = load(name)
    units = rtl.despace_line_to_word_units(line_glyphs(fx))
    reordered = rtl.reorder_word_units_rtl(units, fx["expected_reordered"])
    return rtl.line_text_from_word_units(reordered)


def test_reorder_reversed_line():
    fx = load("letter_spaced_reversed_line.json")
    # de-space yields the reversed string; reorder fixes it (D-05).
    assert _despaced_text("letter_spaced_reversed_line.json") == fx["expected_despaced"]
    assert _reordered_text("letter_spaced_reversed_line.json") == fx["expected_reordered"]


def test_reorder_running_header():
    fx = load("rtl_running_header.json")
    assert _reordered_text("rtl_running_header.json") == fx["expected_reordered"]


def test_reorder_ltr_identity():
    fx = load("ltr_latin_line.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx))
    out = rtl.reorder_word_units_rtl(units, "Northwest Semitic Dictionary")
    assert out == units  # LTR no-regression: returned unchanged


def test_reorder_intra_word_identity():
    fx = load("intra_word_visual_ltr.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx, 0))
    out = rtl.reorder_word_units_rtl(units, "שלום")
    assert rtl.line_text_from_word_units(out) == "שלום"


def test_reorder_invariant_to_input_shuffle():
    # Proves reorder segments by original_order, not array position / x-sort.
    fx = load("letter_spaced_reversed_line.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx))
    shuffled = list(reversed(units))
    a = rtl.reorder_word_units_rtl(units, fx["expected_reordered"])
    b = rtl.reorder_word_units_rtl(shuffled, fx["expected_reordered"])
    assert [u["text"] for u in a] == [u["text"] for u in b]


def _digit_unit(visual_string):
    # Build a one-unit member list laid out visually left-to-right at 6pt pitch.
    return [
        {"c": ch, "bbox": [i * 6, 0.0, i * 6 + 5, 10.0], "size": 10.0,
         "original_order": i}
        for i, ch in enumerate(visual_string)
    ]


def test_order_unit_keeps_embedded_ltr_run_ascending():
    # Regression (2026-05-31 b / N3): the blanket descending-center-x sort reversed
    # embedded numbers (a year 1977 -> 7791). _order_unit_text_rtl must re-flip the
    # LTR run (digits / Latin / numeric separators) back to ascending.
    assert rtl._order_unit_text_rtl(_digit_unit("1977")) == "1977"
    assert rtl._order_unit_text_rtl(_digit_unit("194-256")) == "194-256"
    assert rtl._order_unit_text_rtl(_digit_unit("p20")) == "p20"
    assert rtl._order_unit_text_rtl(_digit_unit("2003.")) == "2003."
    # Hebrew consonants must STILL reverse (visual ם-ו-ל-ש -> logical שלום).
    assert rtl._order_unit_text_rtl(_digit_unit("םולש")) == "שלום"


def test_year_in_rtl_line_not_reversed():
    # End-to-end: a Hebrew word followed by a year keeps the year readable.
    def g(c, x):
        return {"c": c, "bbox": [x, 0.0, x + 5, 10.0], "size": 10.0,
                "original_order": 0}
    line = []
    for o, (c, x) in enumerate([("1", 0), ("9", 6), ("7", 12), ("7", 18),
                                (" ", 24), ("ם", 31), ("י", 37), ("ל", 43),
                                ("ש", 49), ("ו", 55), ("ר", 61), ("י", 67)]):
        gl = g(c, x)
        gl["original_order"] = o
        line.append(gl)
    raw = "".join(x["c"] for x in line)
    units = rtl.reorder_word_units_rtl(rtl.despace_line_to_word_units(line), raw)
    assert rtl.line_text_from_word_units(units) == "ירושלים 1977"


def test_fix_visual_brackets_rtl():
    # Reversed visual storage ")טקסט(" -> logical "(טקסט)".
    assert rtl.fix_visual_brackets_rtl(")טקסט(") == "(טקסט)"
    # LTR text untouched.
    assert rtl.fix_visual_brackets_rtl("(text)") == "(text)"


def test_normalize_punctuation_spacing_ascii():
    assert rtl.normalize_punctuation_spacing("אופנים .") == "אופנים."


def test_normalize_punctuation_spacing_hebrew_sof_pasuq():
    assert rtl.normalize_punctuation_spacing("דבר ׃") == "דבר׃"  # U+05C3


def test_normalize_punctuation_spacing_hebrew_maqaf():
    # maqaf U+05BE — collapse the space immediately before it.
    assert rtl.normalize_punctuation_spacing("בן ־ אדם") == "בן־ אדם"


# ---------------------------------------------------------------------------
# Real-PDF regression fixtures (2026-05-31 edge-gap + Mn de-space revision).
# Glyph traces captured from a real letter-spaced/justified book
# ("איגרות הרמב״ם - שילת.pdf"); guard the exact shatter + maqaf-range bugs that
# the synthetic fixtures could not (their geometry was hand-authored).
# ---------------------------------------------------------------------------
REAL_FIXTURES = [
    "real_pirush_hamishna.json",
    "real_hakdamot_lpirush.json",
    "real_maqaf_range.json",
    "real_otzar_heading.json",      # aggressive letter-spacing — must NOT shatter
    "real_ravitzky_tight.json",     # tight ~0.3*em word-spaces — must NOT merge
    "real_dusiach_packed_names.json",  # N1: zero-width word-spaces — must split
    "real_dusiach_year.json",          # N3: embedded year must NOT digit-reverse
]


def _full_pipeline(glyphs):
    raw = "".join(g.get("c", "") for g in glyphs)
    units = rtl.despace_line_to_word_units(glyphs)
    units = rtl.reorder_word_units_rtl(units, raw)
    s = rtl.line_text_from_word_units(units)
    s = rtl.fix_visual_brackets_rtl(s)
    s = rtl.normalize_punctuation_spacing(s)
    return s


@pytest.mark.parametrize("name", REAL_FIXTURES)
def test_real_pdf_despace_regression(name):
    fx = load(name)
    out = _full_pipeline(line_glyphs(fx))
    for needle in fx.get("must_contain", []):
        assert needle in out, f"{name}: expected {needle!r} in {out!r}"
    for needle in fx.get("must_not_contain", []):
        assert needle not in out, f"{name}: {needle!r} must NOT appear in {out!r}"
