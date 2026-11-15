# -*- coding: utf-8 -*-
"""shared/ja_roots.py: a typed root's dots and stems, shared by Blau's and Friedman's lookups.

Pinned: a stem label is read as every stem number it names, Friedman's noisy labels included
("IV/II", "II(<IV)", "IV)>I)", "(" names none); a typed stem is a stem only when written as one (a
Roman numeral I to X or a number 1 to 10, beside a root with a Hebrew letter); the dot above however
it is written (moved here from shared/blau_service.py, which still exports it).
"""
from __future__ import annotations

import pytest

from types import SimpleNamespace

from shared import blau_service
from shared.ja_roots import (
    BORROW_BLAU, BORROW_FRIEDMAN, BORROW_LANE, STEM_NUMERALS, BorrowedRoot, borrowed_roots, dot_pattern, dots_agree,
    head_begins_with, ja_root_of_arabic, parse_stem, root_and_stem, stem_label, stem_numbers, stem_query, with_final_letters,
)


@pytest.mark.parametrize("label, numbers", [
    ("I", (1,)), ("VIII", (8,)), ("X", (10,)), ("IX", (9,)), ("VII", (7,)),
    # Friedman's labels as recovered from the PDFs: every numeral they name.
    ("IV/II", (2, 4)), ("II(<IV)", (2, 4)), ("IV)>I)", (1, 4)), ("IV/II/I", (1, 2, 4)), ("(<IV)", (4,)),
    ("III.", (3,)), ("I:", (1,)), ("VVI", (5, 6)), ("VIII(V)", (5, 8)),
    # Blau's binyan_shift: both sides.
    ("IV > I", (1, 4)), ("I >", (1,)), ("> V", (5,)), ("VI = V", (5, 6)), ("II , IV", (2, 4)),
    # Nothing to read.
    ("(", ()), ("", ()), (None, ()), ("-", ()),
])
def test_a_stem_label_names_every_stem_number_in_it(label, numbers):
    assert stem_numbers(label) == numbers


def test_stem_labels_and_numbers_round_trip():
    assert [stem_label(n) for n in range(1, 11)] == list(STEM_NUMERALS)
    assert stem_label(0) == stem_label(11) == ""
    for n in range(1, 11):
        assert stem_numbers(stem_label(n)) == (n,)
        assert parse_stem(stem_label(n)) == parse_stem(str(n)) == parse_stem(stem_label(n).lower()) == n


@pytest.mark.parametrize("token", ["", "0", "11", "XI", "XX", "IIII", "VV", "א", "x1", "١٠", "1.5", "-1"])
def test_a_token_that_is_not_a_stem_is_zero(token):
    assert parse_stem(token) == 0


@pytest.mark.parametrize("text, expected", [
    ("דלל X", ("דלל", 10)), ("דלל 10", ("דלל", 10)), ("דלל x", ("דלל", 10)), ("  דלל   IV ", ("דלל", 4)),
    ("ד'לל X", ("ד'לל", 10)), ("ד̇לל 8", ("ד̇לל", 8)),
    ("X דלל", ("דלל", 10)), ("1 דלל", ("דלל", 1)),        # the stem typed first, as a right-to-left box may
    ("ד̇לך ו I", ("ד̇לך ו", 1)),                          # a root named by two words
    # Not a stem lookup:
    ("דלל", ("", 0)), ("X", ("", 0)), ("10", ("", 0)), ("דלל XI", ("", 0)), ("דלל 11", ("", 0)),
    ("קאל לי", ("", 0)), ("abc X", ("", 0)), ("X 10", ("", 0)), ("דלל 10 X", ("", 0)), ("", ("", 0)),
])
def test_a_root_with_a_typed_stem(text, expected):
    assert root_and_stem(text) == expected


def test_stem_query_is_what_the_box_holds():
    assert stem_query("ד̇לל", 10) == "ד̇לל X" and stem_query("דלל", 0) == "דלל" and stem_query("דלל", 12) == "דלל"
    assert root_and_stem(stem_query("ד̇לל", 4)) == ("ד̇לל", 4)


def test_the_dot_however_it_is_written_and_blau_still_exports_it():
    assert dot_pattern("ד'לל") == dot_pattern("ד׳לל") == dot_pattern("ד̇לל") == "ד*לל"
    assert dot_pattern("ר'") == "ר" and dot_pattern("דלל X") == "דלל"
    assert dots_agree("ד*לל", "ד̇לל") and not dots_agree("ד*לל", "דלל") and not dots_agree("ד*לל", "abc")
    assert blau_service.dot_pattern is dot_pattern and blau_service.DOT_LETTERS == frozenset("גדטצתכ")


# The roots Blau's tab borrows from Friedman and Lane (owner, 2026-11-11).

@pytest.mark.parametrize("name, root_key, ja", [
    ("قول", "قWل", "קול"), ("سلم", "سلم", "סלם"),
    # The second letter a dot marks, as Blau spells it: ج ג̇ and غ ג, ذ ד̇, ظ ט̇, ض צ̇, ث ת̇, خ כ̇.
    ("جوب", "جWب", "ג̇וב"), ("غلب", "غلب", "גלב"), ("ذكر", "ذكر", "ד̇כר"), ("ظلم", "ظلم", "ט̇לם"),
    ("ضرب", "ضرب", "צ̇רב"), ("ثقل", "ثقل", "ת̇קל"), ("خبر", "خبر", "כ̇בר"),
    # Every hamza seat is א, alef maqsura י, ta marbuta ה.
    ("رأى", "رءW", "ראי"), ("جيأ", "جWء", "ג̇יא"), ("سئل", "سءل", "סאל"), ("بؤس", "بءس", "באס"),
    ("مرة", "", "מרה"),
    # A doubled root Lane names with two letters (اب under ءبب) is written with three, as Blau does.
    ("اب", "ءبب", "אבב"), ("غل", "غلل", "גלל"), ("اب", "ءبو", "אב"), ("خص", "خصص", "כ̇צץ"),
    ("ضن", "ضنن", "צ̇נן"), ("نخ", "نخخ", "נכ̇ך̇"),
    # Not a weak root, whose key has W for و or ي twice (ذا under ذWW).
    ("ذا", "ذWW", "ד̇א"),
    # Marks dropped; nothing left is nothing.
    ("قَوْل", "", "קול"), ("", "", ""), ("abc", "", ""),
])
def test_an_arabic_root_in_hebrew_letters_as_blau_spells_a_root(name, root_key, ja):
    assert ja_root_of_arabic(name, root_key) == ja


def test_a_lookup_key_is_written_with_its_final_letters():
    assert with_final_letters("סלאמ") == "סלאם" and with_final_letters("כתאב אלמלכ") == "כתאב אלמלך"
    assert with_final_letters("יגי") == "יגי" and with_final_letters("") == ""


def _friedman(*matches, stem=0):
    """Friedman's matches as (root, via, matched[, unit kind, unit head]): by default an entry headed
    by the matched key."""
    def unit(kind="entry", head=None, matched=""):
        return SimpleNamespace(kind=kind, head_text=matched if head is None else head)
    return SimpleNamespace(stem=stem, matches=tuple(
        SimpleNamespace(root=m[0], via=m[1], matched=m[2], unit=unit(*m[3:], matched=m[2])) for m in matches))


def _lane(*roots):
    return SimpleNamespace(roots=tuple(SimpleNamespace(name=name, root_key=key) for name, key in roots))


def test_borrowed_roots_are_friedmans_in_its_order_then_lanes_first():
    friedman = _friedman(("קול", "index", "קאל (קול)"), ("ג̇יא", "head", "יגי"), ("סלאם", "stem", "סלאמ"),
                         ("ג̇לו", "index", "גאל (ג̇לו)"))
    lane = _lane(("قول", "قWل"), ("قيل", "قWل"))
    assert borrowed_roots(friedman, lane) == (
        BorrowedRoot("קול", BORROW_FRIEDMAN, "קאל (קול)"),      # the index form as the book prints it
        BorrowedRoot("ג̇יא", BORROW_FRIEDMAN, "יגי (ג̇יא)"),     # the word, and its entry's root
        BorrowedRoot("סלאם", BORROW_FRIEDMAN, "סלאם"),          # the word is the root: the root alone
        BorrowedRoot("ג̇לו", BORROW_FRIEDMAN, "גאל (ג̇לו)"),
        BorrowedRoot("קול", BORROW_LANE, "قول"),                 # Lane's first root only
    )


def test_each_root_once_per_dictionary_its_dots_telling_two_roots_apart():
    friedman = _friedman(("קול", "index", "קאל (קול)"), ("קול", "head", "קול"), ("ג̇לל", "index", "גל (ג̇לל)"),
                         ("גלל", "index", "גל (גלל)"))
    assert [(r.root, r.evidence) for r in borrowed_roots(friedman, None)] == [
        ("קול", "קאל (קול)"), ("ג̇לל", "גל (ג̇לל)"), ("גלל", "גל (גלל)")]


def _blau(*cards, stem=0):
    return SimpleNamespace(stem=stem, cards=tuple(
        SimpleNamespace(name=name, root_label=label, matched_key=matched, borrowed=borrowed)
        for name, label, matched, borrowed in cards))


def test_friedman_s_tab_borrows_blau_s_own_entries_then_lane_s_root():
    """Owner, 2026-11-11: Friedman's tab borrows from Blau and Lane. Blau's OWN cards only (a card Blau
    borrowed came from Friedman or Lane already), by the entry's first printed name; the evidence is
    what Blau matched and the entry, or the entry alone."""
    blau = _blau(("כון", "כון", "יכון", ()), ("ד̇לל | דאלה̈", "ד̇לל", "ד̇לל | דאלה̈", ()),
                 ("קול", "קול", "קול", (("lane", "قول"),)))
    assert borrowed_roots(lane=_lane(("كون", "كWن")), blau=blau) == (
        BorrowedRoot("כון", BORROW_BLAU, "יכון (כון)"),
        BorrowedRoot("ד̇לל", BORROW_BLAU, "ד̇לל | דאלה̈"),
        BorrowedRoot("כון", BORROW_LANE, "كون"),
    )
    assert borrowed_roots(blau=_blau(("כון", "כון", "כון", ()), stem=1)) == (), "a typed stem borrows nothing"
    assert borrowed_roots(blau=SimpleNamespace(hits=())) == (), "an expression answer has no cards"


def test_only_an_entry_the_word_heads_and_the_index_lend_a_friedman_root():
    """Owner, 2026-11-11: אלעלמא borrowed דרס, חצל and חוז into Blau's tab. Friedman matched עלמא in a
    stem paragraph of דרס ("עِלْמאً"), a sub-entry of חצל and the entry "חוז עِלْמאً": phrases whose
    root is the verb's, the word its complement. Only an entry whose head begins with the word or
    its base form, and the book's index of forms, give the word's root."""
    friedman = _friedman(("עאלם", "head", "אלעלמא", "subentry", "אלעُלَמאُ (ז״ל)"),
                         ("דרס", "stem", "עלמא", "binyan", "עِלْמאً"),
                         ("חצל", "stem", "עלמא", "subentry", "עِלْמאً"),
                         ("חוז", "stem", "עלמא", "entry", "חוז עِלْמאً"),
                         ("קול", "stem", "קול", "entry", "קَוْל ר׳ אَקאוِיל"),
                         ("ג̇לו", "index", "גאל (ג̇לו)", "binyan", "-הא"))
    assert borrowed_roots(friedman, None) == (BorrowedRoot("קול", BORROW_FRIEDMAN, "קול"),
                                              BorrowedRoot("ג̇לו", BORROW_FRIEDMAN, "גאל (ג̇לו)")), (
        "the entry headed by the base form, and the index whatever unit it opened")
    assert head_begins_with("קَוْל ר׳ אَקאוِיל", "קול") and not head_begins_with("חוז עِלْמאً", "עלמא")


def test_a_card_or_match_found_by_verb_form_is_never_borrowed():
    """PR 3: both dictionaries run the same generated forms, so a root one found by verb form is no
    second dictionary's word for it; each tab shows its own."""
    friedman = _friedman(("דלל", "verb_form", "אסתדלו (דלל X)"), ("קול", "index", "קאל (קול)"))
    assert borrowed_roots(friedman, None) == (BorrowedRoot("קול", BORROW_FRIEDMAN, "קאל (קול)"),)
    blau = SimpleNamespace(stem=0, cards=(
        SimpleNamespace(name="דלל", root_label="דלל", matched_key="", borrowed=(), verb_form="אסתדלו (דלל X)"),
        SimpleNamespace(name="כון", root_label="כון", matched_key="יכון", borrowed=(), verb_form="")))
    assert borrowed_roots(blau=blau) == (BorrowedRoot("כון", BORROW_BLAU, "יכון (כון)"),)


def test_nothing_is_borrowed_from_a_typed_stem_an_expression_a_root_of_several_words_or_no_dictionary():
    assert borrowed_roots(_friedman(("קול", "chosen", "קול"), stem=1), None) == ()
    assert borrowed_roots(SimpleNamespace(hits=()), None) == (), "an expression answer has no matches"
    assert borrowed_roots(_friedman(("קול ו", "head", "קול ו"), ("", "head", "קול")), None) == ()
    assert borrowed_roots(None, None) == () and borrowed_roots(None, _lane()) == ()
    assert borrowed_roots(None, _lane(("abc", ""))) == (), "a root with no Arabic letter"
