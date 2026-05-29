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


def test_is_nikud_and_center_x():
    assert rtl._is_nikud(0x05B0) and rtl._is_nikud(0x05C7)
    assert not rtl._is_nikud(ord("א"))
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


def test_despace_undersplit_splits_via_hysteresis():
    fx = load("undersplit_line.json")
    assert _despaced_text("undersplit_line.json") == fx["expected_despaced"]


def test_despace_overmerge_splits_via_span_boundary():
    fx = load("overmerge_line.json")
    units = rtl.despace_line_to_word_units(line_glyphs(fx))
    text = rtl.line_text_from_word_units(units)
    assert text == fx["expected_despaced"]
    # The split is driven by the span/font boundary, NOT the 1.8x threshold:
    # all word gaps are below the hard multiplier, yet we still get 3 units.
    assert len(units) == 3
    assert "ישנואצליוסף" not in text  # must NOT over-merge


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
