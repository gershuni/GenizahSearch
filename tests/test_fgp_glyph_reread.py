# -*- coding: utf-8 -*-
"""GS-276 re-reading (plan docs/plans/FGP_PDF_GLYPH_REREAD_PLAN.md; rules FGP_TEXT_PIPELINE_RULES.md section 9):
the name registry, the presentation-form normalisation, the ToUnicode reader, the shape rule, the map policy, and
the checks C2, C3 and C5 - each with a case built to fail."""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts import fgp_glyph_reread as g  # noqa: E402
from scripts.fgp_glyph_registry import form_of, text_of_name  # noqa: E402


# --- registry ---------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("name, text", [
    ("nunfinal", "ן"), ("finalnun", "ן"), ("kaffinal", "ך"), ("hyphenminus", "-"),
    ("vavholam", "וֹ"), ("tavdages", "תּ"), ("shinshindot", "שׁ"),
    ("shinwithdageshandsindot", "שּׂ"), ("alefisolated", "ا"), ("lamwithalefisolated", "لا"),
    ("kasratanisolated", "ٍ"), ("vavyod", "ױ"), ("arabicquestionmark", "؟"),
])
def test_registry_resolves_the_names_met_in_fgp_fonts(name, text):
    assert text_of_name(name) == text


@pytest.mark.parametrize("name", [".notdef", "space", "xmarkbld", "dotnosp", "afii62760", "floppy3", None, ""])
def test_registry_does_not_guess(name):
    if name in ("xmarkbld", "dotnosp", "afii62760", "floppy3"):
        pytest.importorskip("fontTools")  # these reach the Adobe Glyph List fallback; CI's lock file lacks fontTools
    assert form_of(name) is None


def test_arabic_form_is_the_presentation_character():
    assert form_of("alefisolated") == "ﺍ" and form_of("lamwithalefisolated") == "ﻻ"


# --- normalisation of decoded presentation forms ------------------------------------------------------------------

def test_normalize_presentation_changes_only_presentation_forms():
    assert g.normalize_presentation("וֹש ﺍ café") == "וֹש ا café"


@pytest.mark.parametrize("text, ok", [("ן", True), ("וֹ", True), ("ا", True), ("(", False),
                                      ("◌", False), ("-", False), ("a", False), ("", False),
                                      ("ּ", True), ("ְ", True)])  # a point alone: kept
def test_shape_alone_certifies_only_hebrew_or_arabic_letters_and_marks(text, ok):
    assert g.shape_may_certify(text) is ok


def test_typefaces_keeps_every_family_that_ties_in_any_order():
    arial, times = (next(f for f, fam in g.REFERENCE_FONTS.items() if fam == name) for name in ("Arial", "Times New Roman"))
    hits = [[(arial, 0x2D)], [(times, 0x2D)]]
    assert g.typefaces(hits) == g.typefaces(reversed(hits)) == frozenset({"Arial", "Times New Roman"})
    assert g.typefaces(hits + [[(arial, 0x2E)]]) == frozenset({"Arial"})
    assert g.typefaces([[], []]) == frozenset()


# --- name rule and tie rule (owner, 2026-10-28) -----------------------------------------------------------------------

def _certify(monkeypatch, glyphs, names):
    """certify_font over made-up outlines: ``glyphs`` = {code: (outline, [(reference file, code point)])}."""
    outlines = {f"g{code}": o for code, (o, _) in glyphs.items()}
    monkeypatch.setattr(g, "canon_outline", lambda tt, gname: outlines.get(gname))
    ref = {"index": {o: hits for o, hits in glyphs.values() if hits}}
    return g.certify_font(object(), {code: f"g{code}" for code in glyphs}, sorted(glyphs), names, ref)


def test_name_rule_reads_an_unmatched_glyph_by_its_name(monkeypatch):
    pytest.importorskip("fontTools")  # uniF020 and finalkafwithsheva reach the Adobe Glyph List fallback; CI lacks it
    out = _certify(monkeypatch, {1: ((("a",),), []), 2: ((("b",),), []), 3: ((("c",),), []), 4: ((("d",),), [])},
                   {1: "memfinal", 2: "vavholam", 3: "uniF020", 4: "finalkafwithsheva"})
    assert (out[1]["status"], out[1]["text"], out[1]["form"]) == ("certified by name", "ם", "ם")
    # a pointed letter: the presentation form in the copy, letter + point in the text
    assert (out[2]["status"], out[2]["text"], out[2]["form"]) == ("certified by name", "וֹ", "וֹ")
    assert (out[3]["status"], out[3]["text"]) == ("unmatched", None)  # a private-use reading is never applied
    assert (out[4]["status"], out[4]["text"]) == ("unmatched", None)  # no one-character form


def test_name_rule_needs_the_name_to_agree_with_any_shape_the_outline_matches(monkeypatch):
    pytest.importorskip("fontTools")  # "period" reaches the Adobe Glyph List fallback; CI's lock file lacks fontTools
    out = _certify(monkeypatch, {1: ((("a",),), [("david.ttf", 0x05D0)]),        # three David glyphs make the
                                 4: ((("e",),), [("david.ttf", 0x05D1)]),        # typeface David
                                 5: ((("f",),), [("david.ttf", 0x05D2)]),
                                 2: ((("b",),), [("times.ttf", 0x2E)]),         # a Times period, named period
                                 3: ((("c",),), [("times.ttf", 0x6F)])},        # a Times "o", named samekh
                   {1: "alef", 2: "period", 3: "samekh"})
    assert out[1]["status"] == "certified"
    assert (out[2]["status"], out[2]["text"]) == ("certified by name", ".")
    assert (out[3]["status"], out[3]["text"]) == ("other face", None)


@pytest.mark.parametrize("interps, plain", [
    (["ָ", "ׇ"], "ָ"), (["ע", "ﬠ"], "ע"), (["נ", ""], "נ"),
    (["׆", "נ", ""], "נ"),
    (["ה", "ח"], None),              # two plain letters
    (["ע", "e"], None),              # a reading that is no variant of the letter
    (["e", "е"], None),              # no Hebrew at all
    (["ּ"], None),                   # nothing tied
])
def test_tie_rule_reads_the_plain_letter_beside_only_its_variants(interps, plain):
    assert g.plain_of(interps) == plain


def test_tie_rule_in_certification(monkeypatch):
    out = _certify(monkeypatch, {1: ((("a",),), [("david.ttf", 0x05D0)]),
                                 2: ((("q",),), [("david.ttf", 0x05B8), ("david.ttf", 0x05C7)]),
                                 3: ((("h",),), [("david.ttf", 0x05D4), ("david.ttf", 0x05D7)])},
                   {1: "alef"})
    assert (out[2]["status"], out[2]["text"], out[2]["form"]) == ("certified by plain form", "ָ", "ָ")
    assert (out[3]["status"], out[3]["text"]) == ("ambiguous", None)


def test_a_name_reading_is_withdrawn_where_the_shape_is_certified_as_something_else():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE decisions (sha256, objid, code, status, text, form, outline)")
    db.executemany("INSERT INTO decisions VALUES (?,?,?,?,?,?,?)", [
        ("a", 1, 1, "certified by name", "ם", "ם", "X"),
        ("b", 1, 1, "certified by shape", "ן", "ן", "X"),          # the same outline certified as another letter
        ("a", 1, 2, "certified by name", "ֹ", "ֹ", "Y"),
        ("c", 1, 2, "certified by name", "ׂ", "ׂ", "Y"),           # two names for one outline: each kept
        ("a", 1, 3, "certified by name", "ס", "ס", "Z"),
        ("d", 1, 3, "certified", "ס", "ס", "Z"),                   # agrees: kept
    ])
    assert g.withdraw_contradicted_names(db) == 1
    assert db.execute("SELECT status, text FROM decisions WHERE sha256='a' AND code=1").fetchone() == (
        "name contradicts a shape certified elsewhere", None)
    assert db.execute("SELECT count(*) FROM decisions WHERE status = 'certified by name'").fetchone()[0] == 3


def test_font_maps_applies_a_name_reading_but_never_over_the_producers_map():
    db = _decisions([
        ("s", 9, 4, None, "certified by name", "ם", 0, None, None, "ם"),        # no map: added
        ("s", 9, 5, None, "certified by plain form", "ָ", 0, None, None, "ָ"),  # tie rule: added
        ("s", 7, 6, None, "certified by name", "ן", 1, "ך", 1, "ן"),             # the map's own entry kept, no conflict
    ])
    maps, conflicts = g.font_maps("s", db)
    assert maps == {9: {4: "ם", 5: "ָ"}} and conflicts == []


# --- ToUnicode reading and writing -----------------------------------------------------------------------------------

def test_parse_tounicode_reads_bfchar_and_both_bfrange_forms():
    cmap = (b"2 beginbfchar\n<04> <05DA>\n<09> <05DF>\nendbfchar\n"
            b"1 beginbfrange\n<20> <22> <0030>\nendbfrange\n"
            b"1 beginbfrange\n<41> <42> [<05D0> <05D1>]\nendbfrange\n")
    assert g.parse_tounicode(cmap) == {4: "ך", 9: "ן", 0x20: "0", 0x21: "1", 0x22: "2",
                                       0x41: "א", 0x42: "ב"}


def test_tounicode_round_trip():
    m = {4: "ך", 0x23: "וֹ"}
    assert g.parse_tounicode(g.tounicode_cmap(m)) == m


# --- map policy (font_maps) ------------------------------------------------------------------------------------------

def _decisions(rows):
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE decisions (sha256, objid, code, n, std, name, glyph, name_text, face, interps, refs, status,"
               " text, has_tounicode, existing, in_existing, outline, form)")
    for r in rows:
        db.execute("INSERT INTO decisions (sha256, objid, code, std, status, text, has_tounicode, existing, in_existing,"
                   " form) VALUES (?,?,?,?,?,?,?,?,?,?)", r)
    return db


def test_font_maps_policy():
    db = _decisions([
        ("s", 9, 4, None, "certified", "ך", 0, None, None, "ך"),            # no map: added
        ("s", 9, 5, "י", "certified", "י", 0, None, None, "י"),         # already decoded: left out
        ("s", 9, 6, None, "unmatched", None, 0, None, None, None),                      # uncertified: left out
        ("s", 7, 35, None, "certified by shape", "ְ", 1, None, 0, "ְ"),       # missing from a map: added
        ("s", 7, 40, "(", "certified by shape", ")", 1, "(", 1, ")"),                   # shape never overrides a map
        ("s", 7, 41, "ב", "certified", "בּ", 1, "ב", 1, "בּ"),  # name+shape contradict: conflict
    ])
    maps, conflicts = g.font_maps("s", db)
    assert maps == {9: {4: "ך"}, 7: {35: "ְ"}}
    assert [(c["objid"], c["code"]) for c in conflicts] == [(7, 41)]


# --- C2 ----------------------------------------------------------------------------------------------------------------

def _reading(rows, skipped=(), guard=()):
    # glyph ids as traced_extract makes them: "<page>|<origin, font, size, box>|<ordinal>"
    return {"rows": [{"page": 1, "block": 0, "row": i, "rtl": True, "text": t,
                      "glyphs": [(f"1|{gid}", c) for gid, c in gl]} for i, (t, gl) in enumerate(rows)],
            "skipped": list(skipped), "guard": list(guard)}


U_ROWS = [("שלו\x04 בית", [("a", "ש"), ("b", "ל"), ("c", "ו"), ("d", "\x04"),
                                                        ("e", "ב"), ("f", "י"), ("g", "ת")]),
          ("אב", [("h", "א"), ("i", "ב")])]
R_GOOD = [("שלוך בית", [("a", "ש"), ("b", "ל"), ("c", "ו"),
                                                          ("d", "ך"), ("e", "ב"), ("f", "י"), ("g", "ת")]),
          ("אב", [("h", "א"), ("i", "ב")])]


def test_c2_passes_a_pure_decoding_change():
    res = g.c2_check(_reading(U_ROWS), _reading(R_GOOD))
    assert res["reasons"] == [] and res["d"] == 1 and res["rows_changed"] == 1


def test_c2_fails_an_unrelated_deletion():
    bad = [(R_GOOD[0][0].replace("י", ""), R_GOOD[0][1]), R_GOOD[1]]
    assert any("beyond the decoded" in r for r in g.c2_check(_reading(U_ROWS), _reading(bad))["reasons"])


def test_c2_fails_reordered_intact_words():
    bad = [("בית שלוך", R_GOOD[0][1]), R_GOOD[1]]
    assert any("moved" in r for r in g.c2_check(_reading(U_ROWS), _reading(bad))["reasons"])


def test_c2_fails_a_changed_row_without_a_decoded_glyph():
    bad = [R_GOOD[0], ("בא", R_GOOD[1][1])]
    assert any("without a decoded glyph" in r for r in g.c2_check(_reading(U_ROWS), _reading(bad))["reasons"])


def test_c2_fails_a_point_detached_into_another_row():
    u = [U_ROWS[0], ("אָב", [("h", "א"), ("m", "ָ"), ("i", "ב")])]
    r = [(R_GOOD[0][0] + "ָ", R_GOOD[0][1] + [("m", "ָ")]), R_GOOD[1]]
    assert "rows grouped differently" in g.c2_check(_reading(u), _reading(r))["reasons"]


def test_c2_fails_a_missing_page():
    assert "pages skipped differently" in g.c2_check(_reading(U_ROWS), _reading(R_GOOD, skipped=[2]))["reasons"]


def test_c2_fails_a_guard_fallback_on_a_decoded_page():
    reasons = g.c2_check(_reading(U_ROWS), _reading(R_GOOD, guard=[1]))["reasons"]
    assert any("blocks text" in r for r in reasons)


def test_c2_counts_a_bracket_pair_as_one_class():
    # the RTL bracket fix may swap a fallback "(" into ")": still the same class
    u = [("נוי)", [("a", "נ"), ("b", "ו"), ("c", "י"), ("d", "(")])]
    r = [("נוין", [("a", "נ"), ("b", "ו"), ("c", "י"), ("d", "ן")])]
    assert g.c2_check(_reading(u), _reading(r))["reasons"] == []


# --- C3 ----------------------------------------------------------------------------------------------------------------

def test_c3_agrees_and_finds_a_decoding_error():
    ok = g.c3_compare("שלו\x04 רב", "שלום רב",
                      "שלום רב")
    assert ok["judged"] == ok["agree"] == ok["weight"] > 0 and not ok["decode_errors"]
    bad = g.c3_compare("שלו\x04 רב", "שלון רב",
                       "שלום רב")
    assert bad["agree"] == 0 and len(bad["decode_errors"]) == 1


# --- C5 ----------------------------------------------------------------------------------------------------------------

def test_c5_fails_a_run_that_recovers_nothing():
    assert g.c5_verdict({"recovered": 0, "held": 0, "unresolved": 1000}, 1000)["passed"] is False


def test_c5_thresholds():
    assert g.c5_verdict({"recovered": 600, "held": 200, "unresolved": 200}, 1000)["passed"] is True
    assert g.c5_verdict({"recovered": 490, "held": 400, "unresolved": 110}, 1000)["passed"] is False
    assert g.c5_verdict({"recovered": 700, "held": 0, "unresolved": 300}, 1000)["passed"] is False


# --- scope from gate 3 (step 6) ---------------------------------------------------------------------------------------

def _tr(tid, status="identical", replaced=0, src="pdf_rtl", unit=("u", 1)):
    return {"today_id": tid, "status": status, "text_replaced": replaced, "today_text_source": src,
            "unit_table": unit[0], "unit_id": unit[1], "reader": "docx"}


def test_scope_is_every_kept_pdf_row_with_its_repaired_text():
    rows = [_tr(1), _tr(2, status="retired"), _tr(3, replaced=1), _tr(4, src="docx"), _tr(5, src="original"),
            _tr(None), _tr(6)]
    today = {1: ("a\x04", "x/1.pdf"), 2: ("b", "x/2.pdf"), 3: ("c", "x/3.pdf"), 4: ("d", "x/4.pdf"),
             5: ("e", "x/5.pdf"), 6: ("f", None)}
    scope = g.scope_from(rows, today, {1: "aך"})
    assert [(r["id"], r["s0"], r["s"], r["text_source"]) for r in scope] == [(1, "a\x04", "aך", "pdf_rtl"),
                                                                             (5, "e", "e", "original")]


def test_benchmark_is_every_replaced_pdf_row_with_a_word_text():
    rows = [_tr(1, replaced=1), _tr(2, replaced=1, unit=("u", 2)), _tr(3, replaced=1, src="docx"), _tr(4)]
    today = {i: ("t", f"x/{i}.pdf") for i in range(1, 5)}
    bench = g.bench_from(rows, today, {("u", 1): "word text", ("u", 2): ""})
    assert [(b["id"], b["word_text"]) for b in bench] == [(1, "word text")]


# --- number ranges (owner, 2026-10-27) ------------------------------------------------------------------------------

def _rd(rows, page_norms=None):
    rd = _reading(rows)
    for row in rd["rows"]:
        row["norm"] = row["text"]
    rd["page_norms"] = page_norms or [(1, "\n".join(t for t, _ in rows))]
    rd["guard"] = []
    return rd


def test_repair_ranges_restores_the_untouched_order():
    u = _rd([("עמ' 12\x1625", [("a", "ע"), ("b", "מ"), ("c", "'"), ("d", "1"), ("e", "2"),
                                         ("f", "\x16"), ("g", "2"), ("h", "5")])])
    r = _rd([("עמ' 25-12", [("a", "ע"), ("b", "מ"), ("c", "'"), ("d", "1"), ("e", "2"),
                                      ("f", "-"), ("g", "2"), ("h", "5")])])
    assert g.repair_ranges(u, r) == 1
    assert r["rows"][0]["text"] == "עמ' 12-25" and r["norm"] == "עמ' 12-25"
    assert g.c2_check(u, r)["reasons"] == []


def test_repair_ranges_leaves_a_hyphen_between_words_alone():
    u = _rd([("כתבי\x16היד", [("a", "כ"), ("f", "\x16"), ("h", "ד")])])
    r = _rd([("כתבי-היד", [("a", "כ"), ("f", "-"), ("h", "ד")])])
    assert g.repair_ranges(u, r) == 0


def test_reassemble_reproduces_the_joined_text():
    r = _rd([("א", [("a", "א")]), ("ב", [("b", "ב")])])
    assert g.reassemble(r) == "א\nב"


def test_reset_restores_the_paths_and_environment_of_import(tmp_path, monkeypatch):
    before = (g.WORK, g.PDF_ROOT, os.environ.get("FGP_GLYPH_REREAD_WORK"), os.environ.get("FGP_GLYPH_REREAD_PDF_ROOT"))
    g.use_work_dir(str(tmp_path / "w"), str(tmp_path / "p"))
    g.set_inputs([], [], {}, [])
    assert g.CENSUS_DB.startswith(str(tmp_path)) and os.environ["FGP_GLYPH_REREAD_WORK"] == str(tmp_path / "w")
    g.reset()
    assert (g.WORK, g.PDF_ROOT, os.environ.get("FGP_GLYPH_REREAD_WORK"),
            os.environ.get("FGP_GLYPH_REREAD_PDF_ROOT")) == before
    assert g._INPUTS is None and g.CENSUS_DB == os.path.join(g.WORK, "census.db")


def test_approved_exception_patterns():
    # owner, 2026-10-28: a change of direction ships as a recorded exception; digits or Latin reordered stays held
    assert "other characters moved: the row changed direction" in g.APPROVED_PATTERNS
    assert "other characters moved: digits or Latin reordered" not in g.APPROVED_PATTERNS
    assert not any(p.startswith("C3") or p.startswith("C1") for p in g.APPROVED_PATTERNS)
