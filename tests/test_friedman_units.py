"""Reading order, unit cutting and conservation gates of the Friedman unit recovery.

Synthetic glyph layouts only: no dictionary text is committed as a fixture.
"""
from __future__ import annotations

import json
import sys
from types import SimpleNamespace

import pytest

from scripts import friedman_units as fu
from scripts import prepare_friedman_dictionary as prep

H = 700.0
BOLD = ("Hadassah-Bold", 13.5)
REG = ("Hadassah-Regular", 11.0)
GUIDE = ("Hadassah-Regular", 9.0)
HE = ("FrankRuehl", 11.5)
LOC = ("FrankRuehl", 9.5)
NUM = ("FrankRuehl", 12.0)
SHELF = ("TimesNewRomanPSMT", 7.5)
ARABIC = ("TimesNewRomanPSMT", 10.0)
ARABIC_BOLD = ("TimesNewRomanPS-BoldMT", 12.0)
BULLET = ("Wingdings-Regular", 10.0)
DIAMOND = ("Wingdings-Regular", 11.0)
ROMAN = ("TimesNewRomanPS-BoldMT", 10.5)
MARK = ("ArialUnicodeMS", 11.5)
HIDDEN = ("TimesNewRomanPSMT", 0.8)
WIDTH = 6.0
GAP = ("gap", 3.0)


def visual(items, y=200.0, x_right=400.0):
    """Lay glyphs out leftwards from the right margin, in the VISUAL right-to-left order.

    ("ltr", text, style) is a run read left to right (its visual order is the text order);
    ("mark", ch, style) sits over the previous base glyph; ("gap", w) is empty space;
    ("space", style) is a whitespace glyph with no room around it, as the diacritic font
    carries inside words. The returned glyphs are in content order sorted by font and x,
    so a correct result can only come from the positions.
    """
    glyphs = []
    x = x_right
    last_base = None
    for item in items:
        if item[0] == "gap":
            x -= item[1]
            continue
        if item[0] == "space":
            glyphs.append({"c": " ", "x0": x - 1.0, "x1": x, "style": item[1]})
            x -= 1.0
            continue
        if item[0] == "mark":
            # Zero-advance, at the origin the producer gives it: the diacritic font's combining
            # marks print 0.31 em LEFT of their origin, which therefore sits past the base's
            # right edge, inside the letter before it; a point prints to the right of its origin.
            centre = (last_base["x0"] + last_base["x1"]) / 2
            origin = centre + (0.31 if "̀" <= item[1] <= "ͯ" else -0.15) * item[2][1]
            glyphs.append({"c": item[1], "x0": origin, "x1": origin, "style": item[2]})
            continue
        text, style = (item[1], item[2]) if item[0] == "ltr" else item
        order = reversed(text) if item[0] == "ltr" else text
        for ch in order:
            glyphs.append({"c": ch, "x0": x - WIDTH, "x1": x, "style": style})
            last_base = glyphs[-1]
            x -= WIDTH
    glyphs.sort(key=lambda g: (g["style"], g["x0"]))
    spans = []
    for g in glyphs:
        char = {"c": g["c"], "bbox": [g["x0"], y, g["x1"], y + 12]}
        if spans and spans[-1]["font"] == g["style"][0] and spans[-1]["size"] == g["style"][1]:
            spans[-1]["chars"].append(char)
        else:
            spans.append({"font": g["style"][0], "size": g["style"][1], "chars": [char]})
    x0 = min(g["x0"] for g in glyphs)
    return {"bbox": [x0, y, x_right, y + 12], "spans": spans}


def block(*lines):
    return {"type": 0, "bbox": [min(ln["bbox"][0] for ln in lines), lines[0]["bbox"][1],
                                max(ln["bbox"][2] for ln in lines), lines[-1]["bbox"][3]], "lines": list(lines)}


def words(*parts, style=REG):
    items = []
    for i, part in enumerate(parts):
        if i:
            items.append(GAP)
        items.append(part if isinstance(part, tuple) else (part, style))
    return items


def locator(prefix, number, tail=")."):
    return [(prefix, LOC), ("ltr", number, LOC), (tail, LOC)]


class Page:
    rect = SimpleNamespace(height=H)

    def __init__(self, *blocks):
        self.blocks = list(blocks)

    def get_text(self, kind, sort=False):
        assert kind == "rawdict"
        return {"blocks": self.blocks}


class Doc:
    def __init__(self, pages):
        self.pages = pages

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def __iter__(self):
        return iter(self.pages)


def head(text, style=BOLD):
    """A vocalised head: a vowel over every letter, in the diacritic font."""
    items = []
    for ch in text:
        items.append((ch, style))
        items.append(("mark", "ַ", MARK))
    return items


def page_one():
    """Letter page: heading, an entry, a ❖ sub-entry, a bold complement, a binyan, an entry that runs on."""
    letter = block(visual([("א", BOLD), GAP, ("(", ARABIC_BOLD), ("أ", ARABIC_BOLD), (")", ARABIC_BOLD)], y=90))
    entry = block(visual([*head("אבד"), GAP, *words("נעשה", "קבוע."), GAP, ("y", BULLET), GAP,
                          *words("אלשוק", "אלמתזיד"), GAP, ("׳הכיסופים׳", HE), GAP,
                          ("(ח", LOC), ("ltr", "40", LOC), (",", LOC), GAP, ("שורה", LOC), GAP, ("ltr", "3", LOC),
                          (").", LOC)], y=120))
    subentry = block(visual([("v", DIAMOND), GAP, *words("לא", "אבד", style=BOLD), GAP, ("מוכרח.", REG), GAP,
                             ("y", BULLET), GAP, ("פקאל", REG), GAP, ("׳אמר׳", HE), GAP, *locator("(ח", "81", ")")], y=140))
    complement = block(visual([("-ה", BOLD), GAP, ("אהב", REG), GAP, ("y", BULLET), GAP, ("עשק", REG), GAP,
                               ("׳אהב׳", HE), GAP, *locator("(ב", "7")], y=160))
    binyan = block(visual([("ltr", "V", ROMAN), GAP, ("ציית", REG), GAP, ("y", BULLET), GAP, ("יגב", REG), GAP,
                           ("׳חייב׳", HE), GAP, *locator("(ב", "5", ")")], y=180))
    continuing = block(visual([("אגל", BOLD), GAP, ("בגלל", REG), GAP, ("y", BULLET), GAP, ("צאק", REG), GAP,
                               ("ו", HIDDEN)], y=200))
    number = block(visual([("ltr", "[1]", NUM)], y=640))
    return Page(number, continuing, binyan, complement, subentry, entry, letter)


def page_two(label="]2["):
    """The running head shares a block with the first body line; a ❖ opens a line inside a
    block; a Roman numeral is a line fragment of its own beside the Hebrew line."""
    guide = visual([("אגל", GUIDE), GAP, ("(أجل)", ARABIC)], y=60)
    continuation = visual([("צדרה", REG), GAP, ("׳הצטער׳", HE), GAP, *locator("(ח", "19")], y=92)
    inner = visual([("v", DIAMOND), GAP, ("של", BOLD), GAP, ("y", BULLET), GAP, ("מרכב", REG), GAP, ("׳אנייה׳", HE),
                    GAP, *locator("(ג", "2")], y=112)
    hebrew = visual([("בהא", REG), GAP, *words("נתמך", "בו.")], y=140, x_right=401)
    numeral = visual([("ltr", "V", ROMAN)], y=144, x_right=418)  # lower, inside the line's height
    number = block(visual([("ltr", label, NUM)], y=500))
    return Page(block(hebrew), number, block(guide, continuation, inner), block(numeral))


@pytest.fixture
def archive(tmp_path, monkeypatch):
    html = " ".join(f"LoadPDF('{name}'" for name in prep.SOURCE_IDS).encode()
    monkeypatch.setattr(prep, "download", lambda url: html if url == prep.SITE_URL else b"%PDF-fake-source")
    prep.fetch(tmp_path)
    return tmp_path


def opener(pages):
    return lambda path: Doc(pages if path.name == "1.pdf" else [])


def test_logical_line_rebuilds_reading_order_from_positions():
    line = visual([("לא", HE), GAP, ("(ח", LOC), ("ltr", "24", LOC), (",", LOC), GAP, ("שורה", LOC), GAP,
                   ("ltr", "46–45", LOC), (")", LOC), GAP, ("ltr", "TS Misc. 28.190", SHELF)])
    assert fu.text_of(fu.block_glyphs(block(line))) == "לא (ח24, שורה 45–46) TS Misc. 28.190"
    line = visual([("עמ'", HE), GAP, ("ltr", "613", HE), GAP, ("y", BULLET), GAP, ("ויסכנהא", REG)])
    assert fu.text_of(fu.block_glyphs(block(line))) == "עמ' 613 • ויסכנהא", "the bullet is not a Latin letter"


def test_marks_follow_their_letter_and_diacritic_font_spaces_vanish():
    line = visual([("ג", BOLD), ("mark", "̇", ("LucidaSansUnicode", 12.5)), ("space", MARK), ("אז", BOLD),
                   GAP, ("קומוס", REG), GAP, ("ו", HIDDEN)])
    glyphs = fu.block_glyphs(block(line))
    assert fu.text_of(glyphs) == "ג̇אז קומוס"
    assert fu.segments_of(glyphs) == [["head", "ג̇אז "], ["ja", "קומוס"]]
    assert fu.parse_head(fu.segments_of(glyphs))[0]["keys"] == ["גאז"]
    line = visual([("א", BOLD), ("mark", "ﱠ", ("TimesNewRomanPS-BoldMT", 12.0)), ("ול", BOLD), GAP, ("החשוב", REG)])
    segments = fu.segments_of(fu.block_glyphs(block(line)))
    assert segments[0] == ["head", "אﱠול "], "a shadda ligature is a mark"
    assert fu.parse_head(segments)[0]["keys"] == ["אול"], "and it does not split the key"


def test_a_dot_whose_origin_lies_in_the_previous_letter_attaches_under_its_ink():
    """The book's own geometry (1.pdf page 1, ``פעל/ד̇לך``): the dot's origin, 346.95, lies in
    the slash's box (346.64-351.48), 0.31 pt right of ד (340.28-346.64); its ink is 0.31 em left."""
    spans = [{"font": "BHWWPV+Hadassah-Bold", "size": 13.5, "chars": [
                 {"c": "ל", "bbox": [351.48, 295, 357.84, 309]}, {"c": "/", "bbox": [346.64, 295, 351.48, 309]},
                 {"c": "ד", "bbox": [340.28, 295, 346.64, 309]}, {"c": "ל", "bbox": [333.92, 295, 340.28, 309]}]},
             {"font": "SWLUTN+LucidaSansUnicode", "size": 12.5, "chars": [
                 {"c": "̇", "bbox": [346.95, 295.88, 346.95, 308.38], "origin": [346.95, 304.91]}]}]
    line = {"bbox": [333.92, 295, 357.84, 309], "spans": spans}
    assert fu.text_of(fu.block_glyphs(block(line))) == "ל/ד̇ל"
    assert fu.mark_anchor("SWLUTN+LucidaSansUnicode", 12.5, "̇", 346.95) == pytest.approx(346.95 - 0.307 * 12.5)
    assert fu.mark_anchor("MVHFNZ+ArialUnicodeMS", 11.5, "َ", 412.34) == pytest.approx(412.34 + 0.15 * 11.5)
    assert fu.mark_anchor("JLZSXF+FrankRuehl", 11.5, "ּ", 100.0) == pytest.approx(100.0 - 0.18 * 11.5), "the dagesh by its position"
    assert fu.mark_anchor("Unknown", 10.0, "̇", 5.0) == 5.0, "an unmeasured font keeps the origin"


@pytest.mark.parametrize("font,size,char,expected", [
    ("ABCDEF+Hadassah-Bold", 13.5, "א", "head"), ("Hadassah-Regular", 11.0, "א", "ja"),
    ("FrankRuehl", 11.5, "א", "he"), ("FrankRuehl", 9.5, "(", "loc"), ("TimesNewRomanPSMT", 7.5, "T", "shelf"),
    ("TimesNewRomanPSMT", 12.0, "ع", "foreign"), ("TimesNewRomanPS-BoldMT", 10.5, "V", "binyan"),
    ("TimesNewRomanPS-BoldMT", 10.5, "/", "binyan"), ("TimesNewRomanPS-BoldMT", 12.0, "أ", "foreign"),
    ("Wingdings-Regular", 10.0, "y", "bullet"), ("Wingdings-Regular", 11.0, "", "subentry"),
    ("Wingdings-Regular", 11.0, "q", "other"), ("ArialUnicodeMS", 11.5, "َ", "mark"),
    ("ArialUnicodeMS", 11.5, " ", "space"), ("TimesNewRomanPSMT", 0.8, "ו", "hidden"), ("ArialMT", 11.5, "x", "other")])
def test_glyph_roles_follow_the_book_typography(font, size, char, expected):
    assert fu.classify_char(font, size, char) == expected


def test_head_variants_root_lemma_alternatives_and_notes():
    head, binyan, end, flags = fu.parse_head([["head", "אַבַּדּ: לא אַבַּדּ (=לא בֻּדּ) "], ["ja", "אין מנוס"]])
    assert (head["root"], head["lemma"], head["notes"]) == ("אבד", "לא אבד", ["=לא בד"])
    assert head["keys"] == ["לא אבד", "אבד", "לא בד"] and binyan is None and end == 1 and flags == []
    head, binyan, end, flags = fu.parse_head([["head", "אבי "], ["binyan", "V"], ["head", " ען פעל/דלך "], ["ja", "סירב"]])
    assert (head["root"], head["alternatives"], head["keys"]) == ("אבי", ["ען פעל", "דלך"], ["אבי"])
    assert head["frames"] == ["ען פעל", "דלך"] and binyan == "V" and end == 3
    assert flags == ["binyan_in_head", "complement_lemma"]
    head, binyan, end, flags = fu.parse_head([["subentry", "❖ "], ["head", "אבריסם/אברזם "], ["ja", "סרט משי."]])
    assert head["root"] == "אבריסם / אברזם" and head["keys"] == ["אבריסמ", "אברזמ"] and binyan is None
    head, binyan, end, flags = fu.parse_head([["head", "גרב "], ["binyan", "II"], ["head", "/"], ["binyan", "I"], ["ja", "התמיה"]])
    assert (head["root"], binyan, end, flags) == ("גרב", "II/I", 4, ["compound_binyan"])
    head, binyan, end, flags = fu.parse_head([["head", "("], ["binyan", "IV"], ["head", "/"], ["binyan", "I"],
                                              ["head", ") אויס מנה "], ["ja", "התייאשו"]])
    assert (head["root"], head["lemma"], binyan) == (None, "אויס מנה", "(IV/I)") and "compound_binyan" in flags
    assert fu.parse_head([["binyan", "II"], ["ja", "חיזק."]]) == (None, "II", 1, [])
    head, binyan, end, flags = fu.parse_head([["head", "("], ["binyan", "IV"], ["he", "/"], ["binyan", "I"],
                                              ["head", ") אויס "], ["ja", "התייאשו"]])
    assert (head["lemma"], binyan, flags) == ("אויס", "(IV/I)", ["compound_binyan"]), "a slash in the text font"
    head, binyan, end, flags = fu.parse_head([["head", "איס "], ["binyan", "I"], ["head", " -הא "], ["ja", "התייאש"]])
    assert (head["root"], head["lemma"], head["keys"], flags) == ("איס", "-הא", ["איס"], ["binyan_in_head", "complement_lemma"])
    head, binyan, end, flags = fu.parse_head([["head", "צדף "], ["binyan", "VIII"], ["head", " לה "], ["ja", "הזדמן לו"]])
    assert head["keys"] == ["צדפ"] and head["frames"] == ["לה"] and flags == ["binyan_in_head", "complement_lemma"]
    assert fu.parse_head([["head", "אַן "], ["ja", "ש-"]])[0]["keys"] == ["אנ"], "an entry for the particle itself"
    assert fu.parse_head([["subentry", "❖ "], ["head", "עליה "], ["ja", "נגדו"]], "subentry")[0]["keys"] == []
    head = fu.parse_head([["head", "ג̇אז: מَרْ̇גאן ג̇אז (<זא̇ג) "], ["ja", "קומוס"]])[0]
    assert head["root_dotted"] == ["ג̇אז"] and head["keys"] == ["מרגאנ גאז", "גאז", "זאג"], "the variant spelling is a key"
    head = fu.parse_head([["head", "בני "], ["binyan", "I"], ["head", " עَלَיْהא "], ["ja", "התבסס"]])[0]
    assert head["keys"] == ["בני"] and head["frames"] == ["עליהא"] and head["root_dotted"] == ["בני"]
    head = fu.parse_head([["head", "ג̇לב/ג̇לף "], ["ja", "סוג"]])[0]
    assert head["root_dotted"] == ["ג̇לב", "ג̇לף"]
    head, binyan, end, flags = fu.parse_head([["binyan", "V"], ["head", "(בִהא) "], ["ja", "ציית"]])
    assert head["keys"] == [] and head["notes"] == ["בהא"] and flags == ["no_head_key"]


def test_quotations_split_at_bullets_and_closed_locators():
    gloss, examples, flags = fu.parse_body([
        ["ja", "בעניין. "], ["he", "ראה בלאו, מילון, עמ׳ 42 "], ["head", "חין "], ["bullet", "• "],
        ["ja", "כתאבה מן אגל "], ["he", "'מכתבו' "], ["loc", "(ח31, שורה 21). "],
        ["ja", "כתבנא כותב "], ["he", "'כתבנו' "], ["loc", "(שם, ע\"ב, שורה "], ["ltr", "5"], ["loc", "). "],
        ["bullet", "• "], ["ja", "קאל "], ["foreign", "عن "], ["he", "'אמר' "]])
    assert gloss["definition"] == "בעניין." and gloss["references"].startswith("ראה בלאו") and gloss["xrefs"] == ["חין"]
    assert [ex["quote"] for ex in examples] == ["כתאבה מן אגל", "כתבנא כותב", "קאל"]
    assert examples[0]["locator"] == "(ח31, שורה 21)." and examples[2]["locator"] == ""
    assert examples[2]["foreign"] == "عن"
    assert flags == ["bold_inside_body", "foreign_text", "other_font", "quotation_without_locator"]


def test_units_kinds_parents_continuation_and_running_head(archive):
    result = fu.recover(archive, opener([page_one(), page_two()]))
    units = result["units"]
    assert [(u["id"], u["kind"], u["parent"]) for u in units] == [
        ("f000001", "entry", None), ("f000002", "subentry", "f000001"), ("f000003", "subentry", "f000001"),
        ("f000004", "binyan", "f000001"), ("f000005", "entry", None), ("f000006", "subentry", "f000005"),
        ("f000007", "binyan", "f000005")]
    first = units[0]
    assert first["head"]["plain"] == "אבד" and first["head"]["keys"] == ["אבד"] and first["letter"] == "א"
    assert first["gloss"]["definition"] == "נעשה קבוע."
    assert first["examples"] == [{"quote": "אלשוק אלמתזיד", "translation": "׳הכיסופים׳", "locator": "(ח40, שורה 3).",
                                  "foreign": "", "xrefs": [], "other": ""}]
    assert units[1]["head"]["keys"] == ["לא אבד"] and units[1]["flags"] == []
    assert units[2]["head"]["plain"] == "-ה" and units[2]["flags"] == ["complement_head", "complement_only"]
    assert units[2]["head"]["keys"] == []
    assert units[3]["binyan"] == "V" and units[3]["head"] is None and units[3]["flags"] == []
    carried = units[4]
    assert carried["text"] == "אגל בגלל • צאק צדרה ׳הצטער׳ (ח19)."
    assert [(loc["pdf_page"], loc["printed_page"], loc["paragraph"]) for loc in carried["locations"]] == [(1, "1", 4), (2, "2", 0)]
    assert carried["flags"] == ["continued_across_page"]
    assert units[5]["head"]["keys"] == ["של"] and units[5]["text"] == "❖ של • מרכב ׳אנייה׳ (ג2)."
    assert units[6]["text"] == "V בהא נתמך בו." and units[6]["flags"] == ["no_quotations"]
    pages = result["pages"]
    assert pages[0]["letter_heading"] == "א (أ)" and pages[0]["printed_page"] == "1" and pages[0]["hidden_chars"] == 1
    assert pages[0]["running_head"] is None and pages[0]["running_head_matches"] is False
    assert pages[1]["running_head"] == "אגל (أجل)" and pages[1]["running_head_matches"] is True
    assert pages[1]["units"] == ["f000005", "f000006", "f000007"] and pages[1]["page_number_chars"] == 3
    assert result["printed_pages"] == {"first": 1, "last": 2, "count": 2, "missing": [], "duplicates": 0, "unlabelled": 0}
    assert (result["running_head_matches"], result["pages_with_running_head"]) == (1, 1)


def test_bold_or_numeral_at_line_start_opens_a_unit_only_after_a_short_line(archive):
    full = visual([("אבד", BOLD), GAP, *words("מילה", "אחת", "ועוד", "מילה", "ועוד", "מילה", "ועוד", "מילה")], y=120)
    short = visual([*words("סוף.")], y=132, x_right=400)
    bold_after_full = visual([("אבי", BOLD), GAP, ("ראה.", REG)], y=144)
    page = Page(block(full, bold_after_full), block(visual([("[1]", NUM)], y=640)))
    units = fu.recover(archive, opener([page]))["units"]
    assert len(units) == 1 and units[0]["flags"] == ["bold_inside_body", "head_at_line_start", "no_quotations"]
    page = Page(block(full, short, bold_after_full), block(visual([("[1]", NUM)], y=640)))
    units = fu.recover(archive, opener([page]))["units"]
    assert [u["head"]["plain"] for u in units] == ["אבד", "אבי"] and units[1]["flags"] == ["no_quotations", "split_from_block"]


def test_diamond_stored_as_its_own_line_fragment_opens_the_subentry(archive):
    diamond = visual([("v", DIAMOND)], y=123, x_right=418)  # the extractor's own line, lower than the text
    text = visual([("(", BOLD), ("ltr", "I", ROMAN), ("/", HE), ("ltr", "IV", ROMAN), (")", BOLD), GAP, ("אויס", BOLD),
                   GAP, ("התייאשו", REG)], y=120, x_right=401)  # the book prints the group left to right: (IV/I)
    entry = visual([("אבד", BOLD), GAP, ("נעשה.", REG)], y=100)
    page = Page(block(entry, text), block(diamond), block(visual([("[1]", NUM)], y=640)))
    units = fu.recover(archive, opener([page]))["units"]
    assert [(u["kind"], u["parent"], u["binyan"]) for u in units] == [("entry", None, None), ("subentry", "f000001", "(IV/I)")]
    assert units[1]["text"] == "❖ (IV/I) אויס התייאשו" and units[1]["head"]["keys"] == ["אויס"]


def test_braced_entry_and_narrow_bold_space(archive):
    """A ``{`` before the bold head still opens an entry; a narrow gap with a blank glyph is a space."""
    first = visual([("אבד", BOLD), GAP, ("נעשה.", REG)], y=100)
    braced = visual([("{", HE), ("באלה", BOLD), GAP, ("דאגה", REG)], y=120)
    narrow = visual([("מא", BOLD), ("gap", 0.5), ("space", LOC), ("gap", 0.5), ("תציע", BOLD), GAP, ("טובה", REG)], y=140)
    page = Page(block(first), block(braced), block(narrow), block(visual([("[1]", NUM)], y=640)))
    units = fu.recover(archive, opener([page]))["units"]
    assert [(u["kind"], u["head"]["plain"]) for u in units] == [("entry", "אבד"), ("entry", "באלה"), ("entry", "מא תציע")]
    assert units[1]["flags"] == ["braced_head", "no_quotations"] and units[1]["text"] == "{באלה דאגה"
    assert units[2]["head"]["keys"] == ["מא תציע"]


def test_plural_marker_and_colon_after_numeral():
    head = fu.parse_head([["head", "אנפצאל ר' אנפצאלאת "], ["ja", "תירוץ."]])[0]
    assert head["alternatives"] == ["אנפצאל", "אנפצאלאת"] and head["keys"] == ["אנפצאל", "אנפצאלאת"]
    assert head["root_source"] == "whole_head" and head["root_candidate"] == "אנפצאל"
    head, binyan, _, _ = fu.parse_head([["head", "כלו "], ["binyan", "I"], ["head", ": לא כלות מנה/מנהא "], ["ja", "איחול"]])
    assert (head["root"], head["lemma"], binyan, head["root_source"]) == ("כלו", "לא כלות מנה / מנהא", "I", "numeral")
    assert head["frames"] == ["מנהא"] and head["keys"] == ["לא כלות מנה", "כלו"]
    head = fu.parse_head([["head", "אך, אכת "], ["ja", "אח."]])[0]
    assert head["alternatives"] == ["אך", "אכת"] and head["root_source"] == "whole_head"
    head = fu.parse_head([["head", "איצי ("], ["foreign", "<"], ["head", "איצא) "], ["ja", "גם."]])[0]
    assert head["root"] == "איצי" and head["root_source"] == "single_word" and head["root_candidate"] is None
    assert head["text"] == "איצי (<איצא)" and head["keys"] == ["איצי", "איצא"], "the bracket's reading is a key"


def test_head_goes_on_past_a_note_in_the_text_font_and_keys_its_forms():
    """The book sets a note inside the head's bracket in the text font: the forms it carries (a
    variant spelling, a plural, the singular of a plural) are keys, and the head is not cut."""
    segments = [["head", "קרטאץ ("], ["ja", "<קרטאס"], ["head", ") "], ["ja", "נייר."]]
    head, _, end, _ = fu.parse_head(segments)
    assert head["text"] == "קרטאץ (<קרטאס)" and head["keys"] == ["קרטאצ", "קרטאס"] and end == 3
    segments = [["head", "דבש ("], ["ja", "ר׳ אדבש) דבש. "], ["bullet", "• "], ["ja", "קאל"]]
    head, _, end, _ = fu.parse_head(segments)
    assert head["keys"] == ["דבש", "אדבש"] and end == 2
    assert segments[1:3] == [["ja", "ר׳ אדבש)"], ["ja", "דבש. "]], "the body starts after the bracket; no glyph is lost"
    head, _, end, _ = fu.parse_head([["head", "סנוג̇ ("], ["ja", "ריבוי של סנג̇ה̈) "], ["head", "ר׳ סנוג̇את "], ["ja", "מאזניים"]])
    assert head["keys"] == ["סנוג", "סנוגאת", "סנגה"] and end == 3, "a bold plural after the bracket is still the head"
    segments = [["subentry", "❖ "], ["head", "מא ("], ["ja", "ולא רק "], ["head", "לא) בד ממא יפעל "], ["ja", "אין מנוס"]]
    head, _, end, _ = fu.parse_head(segments, "subentry")
    assert head["text"] == "מא (ולא רק לא) בד ממא יפעל" and head["keys"] == ["מא בד ממא"] and end == 4
    assert len(segments) == 5, "a bold closing bracket is not split: the bold after it is the head"
    head, _, end, _ = fu.parse_head([["head", "אד̇ן ("], ["ja", "ולא רק פי) הרשה לו "], ["bullet", "• "], ["ja", "אדנת"]])
    assert head["keys"] == ["אדנ"] and head["notes"] == ["ולא רק פי"] and end == 2, "a note without forms keys nothing"
    head, _, end, _ = fu.parse_head([["head", "אד̇ן ("], ["ja", "a note that never closes " * 5], ["ja", "x"]])
    assert head["keys"] == ["אדנ"] and end == 1, "an unclosed bracket cuts the head as before"
    assert fu.parse_head([["head", "אד̇ן ("], ["bullet", "• "], ["ja", "קאל) "]])[2] == 1, "a quotation is never swallowed"


def test_placeholders_leave_the_key_and_alternatives_split_at_the_correction_mark():
    assert fu.parse_head([["head", "ען כלפה פעל "], ["ja", "x"]])[0]["keys"] == ["ענ כלפה"]
    assert fu.parse_head([["head", "מכתוב כדא "], ["ja", "x"]])[0]["keys"] == ["מכתוב"]
    assert fu.parse_head([["head", "לאגל דלך/לאגל פעל "], ["ja", "x"]])[0]["keys"] == ["לאגל דלכ", "לאגל"]
    assert fu.parse_head([["head", "פעל "], ["ja", "עשה"]])[0]["keys"] == ["פעל"], "the verb's own entry keeps its word"
    assert fu.parse_head([["head", "בצט<בסט. "], ["ja", "x"]])[0]["keys"] == ["בצט", "בסט"]
    assert fu.parse_head([["head", "מוכוד<מאכוד בהא "], ["ja", "x"]])[0]["keys"] == ["מוכוד", "מאכוד בהא"]
    head = fu.parse_head([["head", "קצר: מא (א)קצר מעה פיהא "], ["ja", "x"]])[0]
    assert head["keys"] == ["מא קצר מעה פיהא", "קצר", "מא אקצר מעה פיהא"] and head["notes"] == [], "optional letters key both ways"
    assert fu.is_frame("ליפעל")
    head = fu.parse_head([["head", "וכל "], ["binyan", "II"], ["head", " -ה באן יפעל/ליפעל "], ["ja", "x"]])[0]
    assert head["keys"] == ["וכל"] and head["frames"] == ["-ה באן יפעל", "ליפעל"]


def test_punctuation_in_another_font_between_two_runs_of_one_role_stays_in_that_role():
    line = visual([("אלואחדה", REG), GAP, ("[", REG), ("...", HE), ("]", REG), GAP, ("הו", REG)])
    assert fu.segments_of(fu.block_glyphs(block(line))) == [["ja", "אלואחדה [...] הו"]], "the dots are not a translation"
    line = visual([("פילי", BOLD), (",", HE), GAP, ("פיליא/פיליה", BOLD), GAP, ("מטבע", REG)])
    segments = fu.segments_of(fu.block_glyphs(block(line)))
    assert segments == [["head", "פילי, פיליא/פיליה "], ["ja", "מטבע"]]
    assert fu.parse_head(segments)[0]["keys"] == ["פילי", "פיליא", "פיליה"]


@pytest.mark.parametrize("form,frame", [("-הא", True), ("עליה", True), ("אן יפעל", True), ("הא עליה", True),
                                        ("לה", True), ("קלבה", False), ("מן אגל", False), ("לא אבד", False)])
def test_valency_frames_are_not_forms(form, frame):
    assert fu.is_frame(form) is frame


def test_line_start_bold_after_a_cross_reference_phrase_is_downgraded(archive):
    full = visual([("אבד", BOLD), GAP, *words("מילה", "אחת", "ועוד", "מילה", "ועוד", "מילה", "ועוד.", "ראה", "הערך")], y=120)
    wrapped = visual([("אבי", BOLD), (".", REG)], y=132)
    page = Page(block(full, wrapped), block(visual([("[1]", NUM)], y=640)))
    units = fu.recover(archive, opener([page]))["units"]
    assert len(units) == 1 and units[0]["flags"] == ["bold_inside_body", "no_quotations", "xref_at_line_start"]


def test_repeated_printed_page_is_recorded_not_recut(archive):
    result = fu.recover(archive, opener([page_one(), page_two(), page_two(label="[2]")]))
    assert len(result["units"]) == 7 and result["pages"][2]["duplicate_of"] == 1 and result["pages"][2]["units"] == []
    assert result["printed_pages"]["duplicates"] == 1


@pytest.fixture
def built(archive, monkeypatch):
    monkeypatch.setitem(sys.modules, "fitz", SimpleNamespace(open=opener([page_one(), page_two()])))
    fu.build(archive)
    return archive


def test_full_build_verify_and_show(built):
    assert fu.verify(built) == {"verified": True, "units": 7, "pages": 2, "printed_pages": 2, "status": fu.STATUS}
    report = prep.read_json(built / "candidate" / "units_report.json")
    assert report["by_kind"] == {"entry": 2, "subentry": 3, "binyan": 2} and report["quotations"] == 6
    assert report["flags"] == {"complement_head": 1, "complement_only": 1, "continued_across_page": 1, "no_quotations": 1}
    found = fu.show(built, "לא אבד")
    assert [u["id"] for u in found] == ["f000002"] and found[0]["head"]["root"] == "לא אבד"
    assert fu.show(built, "f000004")[0]["binyan"] == "V"


def _rewrite(directory, mutate):
    path = directory / "candidate" / "units.json"
    result = json.loads(path.read_text(encoding="utf-8"))
    mutate(result)
    prep.write_json(path, result)
    report_path = directory / "candidate" / "units_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["units_sha256"] = prep.sha256(path.read_bytes())
    prep.write_json(report_path, report)


def test_verify_fails_on_lost_glyphs(built):
    _rewrite(built, lambda r: r["units"][4]["locations"].pop())
    with pytest.raises(ValueError, match="Glyphs lost"):
        fu.verify(built)


def test_verify_fails_when_keys_do_not_follow_the_normalizer(built):
    _rewrite(built, lambda r: r["units"][0]["head"]["keys"].append("אחר"))
    with pytest.raises(ValueError, match="normalizer"):
        fu.verify(built)


def test_verify_fails_on_edited_units_or_sources(built):
    path = built / "candidate" / "units.json"
    with path.open("ab") as stream:
        stream.write(b"\n")
    with pytest.raises(ValueError, match="checksum mismatch"):
        fu.verify(built)
    _rewrite(built, lambda r: None)
    assert fu.verify(built)["verified"]
    with (built / "download" / "1.pdf").open("ab") as stream:
        stream.write(b"changed")
    with pytest.raises(ValueError, match="Source checksum mismatch"):
        fu.verify(built)


def test_verify_fails_on_a_stale_report(built):
    report_path = built / "candidate" / "units_report.json"
    report = json.loads(report_path.read_text(encoding="utf-8"))
    report["units"] = 5
    prep.write_json(report_path, report)
    with pytest.raises(ValueError, match="Report differs"):
        fu.verify(built)
