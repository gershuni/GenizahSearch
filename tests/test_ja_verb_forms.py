# -*- coding: utf-8 -*-
"""shared/ja_verb_forms.py: the forms a verb stem of a three-letter root takes, and which listed stem a
written word belongs to (the private dictionary lookup plan, section 4.2; owner, 2026-11-11: used to
open a root's card on the word's stem). Synthetic roots of every class the plan names, no dictionary
text; every form is on the lookup's keys (``normalize_ja``: finals folded).
"""
from __future__ import annotations

import pytest

from shared.blau_normalize import candidate_stems
from shared.ja_roots import StemPair
from shared.ja_verb_forms import (
    KINDS, MAX_SUGGESTIONS, MAX_VERB_FORM_CARDS, MIN_LETTERS, PREFIXED, VERB_FORMS_VERSION, hit_kind, parse_kinds,
    stem_forms, suggested_roots, switch_kinds, verb_form_cards, verb_form_index, word_stem, word_stems,
)


@pytest.mark.parametrize("stem, forms", [
    (1, ["כתב", "כתבת", "יכתב", "יכתבונ", "כאתב", "מכתוב", "כאתבה"]),
    (2, ["כתב", "מכתב", "תכתיב"]),
    (3, ["כאתב", "יכאתב", "מכאתב"]),
    (4, ["אכתב", "יכתב", "מכתב", "אכתאב"]),
    (5, ["תכתב", "יתכתב", "מתכתב"]),
    (6, ["תכאתב", "יתכאתב", "מתכאתב"]),
    (7, ["אנכתב", "ינכתב", "מנכתב", "אנכתאב"]),
    (8, ["אכתתב", "יכתתב", "מכתתב", "אכתתאב"]),
    (9, ["אכתב", "יכתב"]),
    (10, ["אסתכתב", "יסתכתב", "מסתכתב", "אסתכתאב", "אסתכתבו"]),
])
def test_the_table_for_a_sound_root(stem, forms):
    generated = stem_forms("כתב", stem)
    assert set(forms) <= generated, sorted(set(forms) - generated)


def test_the_stems_that_differ_do_not_share_their_marks():
    assert "אסתכתב" not in stem_forms("כתב", 1) and "תכאתב" not in stem_forms("כתב", 5)
    assert "אנכתב" not in stem_forms("כתב", 10) and "כאתב" not in stem_forms("כתב", 2)


@pytest.mark.parametrize("root, stem, forms", [
    # Doubled: the contracted form beside the full one (a form under three letters is dropped).
    ("דלל", 10, ["אסתדל", "אסתדלל", "אסתדלו", "יסתדל"]),
    ("קרר", 10, ["אסתקר", "אסתקרר"]),
    # Hollow: קאל, קל-, יקול / יקיל / יקאל, קאיל, X אסתגאב and its verbal noun.
    ("קול", 1, ["קאל", "קלת", "יקול", "יקיל", "יקאל", "קאיל"]),
    ("גוב", 10, ["אסתגאב", "אסתגאבה"]),
    # Final weak: the last letter as א, י, ו or dropped.
    ("גני", 10, ["אסתגנא", "אסתגנית", "אסתגנו"]),
    # First ו: the imperfect of I without it, VIII אתצל.
    ("וצל", 1, ["יצל"]),
    ("וצל", 8, ["אתצל"]),
    # First א: VIII אתכד, IV written with one א.
    ("אכד", 8, ["אתכד"]),
    ("אכד", 4, ["אכד"]),
    # Stem VIII assimilation: ז -> זד, צ and ט -> ט, ד and ת merge.
    ("זחמ", 8, ["אזדחמ"]),
    ("צלח", 8, ["אצטלח"]),
    ("דעו", 8, ["אדעי", "אדעו"]),
])
def test_weak_roots_give_every_variant_beside_the_regular_form(root, stem, forms):
    generated = stem_forms(root, stem)
    assert set(forms) <= generated, sorted(set(forms) - generated)


def test_no_form_is_shorter_than_three_letters():
    assert all(len(form) >= MIN_LETTERS for stem in range(1, 11) for form in stem_forms("דלל", stem))
    assert "דל" not in stem_forms("דלל", 1) and "קל" not in stem_forms("קול", 1)


@pytest.mark.parametrize("root, stem", [("כתאב", 1), ("כת", 1), ("כתב", 0), ("כתב", 11), ("", 1), ("כ ת", 1)])
def test_a_root_of_other_than_three_letters_or_a_stem_outside_one_to_ten_has_none(root, stem):
    assert stem_forms(root, stem) == frozenset()


@pytest.mark.parametrize("word, root, listed, stem", [
    ("אסתקר", "קרר", (1, 2, 4, 5, 10), 10),        # the owner's report: X, not I
    ("טאלבתה", "טלב", (1, 3, 7), 3),               # טאלבת, its pronoun peeled: III
    ("ויכון", "כונ", (1, 3, 5), 1),
    ("ואסתגאב", "גוב", (1, 3, 4, 6, 10), 10),      # the conjunction peeled by candidate_stems
    ("קאל", "קול", (1, 2, 3, 4, 7), 1),
    ("קרר", "קרר", (1, 2, 4, 5, 10), 0),           # the root itself is I and II: none chosen
    ("טאלב", "טלב", (1, 3, 7), 0),                 # the participle of I and the perfect of III
    ("אסתקר", "קרר", (1, 2, 4), 0),                # X is not listed: never a stem the root lacks
    ("כתאב", "כתב", (1, 2), 0),
])
def test_the_word_s_stem_is_the_one_listed_stem_that_holds_it(word, root, listed, stem):
    assert word_stem(candidate_stems(word), root, listed) == stem


def test_the_gate_s_mutation_switches_each_take_their_rule_out():
    """Plan section 6.5: the mutation runs need each rule switchable; the lookup uses the defaults."""
    assert "קאל" in stem_forms("קול", 1) and "קאל" not in stem_forms("קול", 1, weak=False)
    assert "אסתדל" in stem_forms("דלל", 10) and "אסתדל" not in stem_forms("דלל", 10, weak=False)
    assert "אזדחמ" in stem_forms("זחמ", 8) and "אזדחמ" not in stem_forms("זחמ", 8, assimilation=False)
    assert "אזתחמ" in stem_forms("זחמ", 8, assimilation=False)
    assert "דל" not in stem_forms("דלל", 1) and "דל" in stem_forms("דלל", 1, min_letters=2)


def test_the_verb_form_index_maps_every_form_to_the_pairs_that_give_it():
    dll_x = StemPair("דלל", "דלל", 10, "u10", "e1")
    dll_i = StemPair("דלל", "דלל", 1, "u1", "e1")
    noun = StemPair("כתאב", "כתאב", 1, "u2", "e2")   # not three letters: no forms
    index = verb_form_index([dll_x, dll_i, noun])
    assert index["אסתדלו"] == (dll_x,) and index["דלל"] == (dll_i,)
    assert not any(noun in pairs for pairs in index.values())
    assert "דל" not in index and "דל" in verb_form_index([dll_i], min_letters=2)


def test_word_stems_names_every_listed_stem_that_holds_the_word():
    assert word_stems(candidate_stems("קרר"), "קרר", (10, 2, 1, 2)) == (1, 2)
    assert word_stems([], "קרר", (1, 2)) == () and word_stems([""], "קרר", (1,)) == ()
    assert VERB_FORMS_VERSION == 2


@pytest.mark.parametrize("root, stem, kept, dropped", [
    # Owner, 2026-11-11: תלתה ("three") was read as stem VIII of להו, its last letter dropped.
    ("להו", 8, ["תלתהי", "ילתהו", "אלתהא"], ["תלתה", "ילתה", "אלתה"]),
    ("רמי", 1, ["רמת", "רמוא", "ירמו", "ירמונ", "ירמי"], ["ירמ", "תרמ"]),
    ("קרא", 10, ["אסתקרא", "אסתקרי", "יסתקרו"], ["אסתקר", "יסתקר"]),
    ("לקי", 3, ["לאקו", "ילאקי"], ["ילאק"]),
])
def test_a_verb_whose_final_weak_letter_is_dropped_takes_an_ending(root, stem, kept, dropped):
    """Rules version 2: the final weak letter dropped only before an ending (רמת, ירמו), never bare
    (ירמ: the short form after לם, which mostly spells another word). ``bare_alone`` is version 1."""
    forms, old = stem_forms(root, stem), stem_forms(root, stem, bare_alone=True)
    assert set(kept) <= forms, sorted(set(kept) - forms)
    assert not set(dropped) & forms, sorted(set(dropped) & forms)
    assert set(dropped) <= old, "version 1 had them"


def test_a_doubled_root_keeps_its_contracted_form_without_an_ending():
    assert {"אסתדל", "יסתדל", "אסתקר"} <= stem_forms("דלל", 10) | stem_forms("קרר", 10)
    assert "יחי" in stem_forms("חיי", 1), "doubled and final weak: the contracted form stays"


# ---------------------------------------------------------------------------
# The cards found by verb form (plan 4.4 and 6.1; PR 3)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("stem, own, length, kind", [
    (10, True, 6, "A"), (3, True, 4, "A"), (8, True, 3, "A"),          # own key, a prefixed stem: any length
    (1, True, 4, "B"), (4, True, 5, "B"), (2, True, 4, "B"),           # own key, I/II/IV, four letters or more
    (1, True, 3, "C"), (2, True, 3, "C"), (4, True, 3, "C"),           # three letters
    (10, False, 6, "D"), (5, False, 3, "D"),                           # a peeled key, a prefixed stem
    (1, False, 4, "E"), (4, False, 6, "E"),                            # peeled, I/II/IV
])
def test_the_kind_of_a_hit_is_the_plan_s(stem, own, length, kind):
    assert hit_kind(stem, own, length) == kind


def test_the_prefixed_stems_and_the_kinds_are_the_plan_s():
    assert PREFIXED == {3, 5, 6, 7, 8, 9, 10} and KINDS == ("A", "B", "C", "D", "E")
    assert MAX_VERB_FORM_CARDS == 3


@pytest.mark.parametrize("text, kinds", [
    (None, set()), ("", set()), ("A,B,D", {"A", "B", "D"}), ("a d", {"A", "D"}), (" b ; e ", {"B", "E"}),
    ("ABCDE", set(KINDS)), ("X, Y, 1", set()), ("A,A", {"A"}),
])
def test_a_switch_names_the_kinds_it_shows(text, kinds):
    assert parse_kinds(text) == kinds


def test_a_switch_is_its_default_while_unset_and_none_when_set_empty(monkeypatch):
    """Both services read their switches through ``switch_kinds`` (one rule, not two copies)."""
    monkeypatch.delenv("JA_TEST_KINDS", raising=False)
    assert switch_kinds("JA_TEST_KINDS", "A,D") == {"A", "D"}
    monkeypatch.setenv("JA_TEST_KINDS", "")
    assert switch_kinds("JA_TEST_KINDS", "A,D") == frozenset()
    monkeypatch.setenv("JA_TEST_KINDS", "b")
    assert switch_kinds("JA_TEST_KINDS", "A,D") == {"B"}


def test_the_suggested_roots_are_the_cards_roots_of_a_kind_shown_once_each_and_bounded():
    """What both tabs draw as "Other possible roots": in card order, not an entry already shown, of a
    kind switched on, one chip per (root with its dots, stem), at most ``MAX_SUGGESTIONS``."""
    index = verb_form_index([_pair("דלל", 10, "e-dll"), _pair("ד̇לל", 10, "e-dhll"), _pair("קרב", 1, "e-qrb"),
                             _pair("דלל", 10, "e-dll-2")])
    keys = ["אסתדלו"]
    assert suggested_roots(keys, index, {"A"}) == (("דלל", 10), ("ד̇לל", 10)), "dots apart, one chip per root"
    assert suggested_roots(keys, index, {"A"}, shown={"e-dll"}) == (("ד̇לל", 10), ("דלל", 10))
    assert suggested_roots(keys, index, {"A"}, shown={"e-dll", "e-dll-2"}) == (("ד̇לל", 10),)
    assert suggested_roots(keys, index, {"B", "D"}) == (), "a kind not shown"
    assert suggested_roots(keys, index, ()) == () and suggested_roots(keys, {}, {"A"}) == ()
    assert suggested_roots(keys, index, {"A"}, limit=1) == (("דלל", 10),)
    many = {"אסתדלו": [_pair(f"ר{c}ב", 10, f"e{c}") for c in "גדהזחטכמנסעפצקש"]}
    assert [root for root, _stem in suggested_roots(keys, many, {"A"})] == ["רגב", "רדב", "רהב", "רזב", "רחב"]
    assert MAX_SUGGESTIONS == 5


def _pair(root, stem, entry, unit="u"):
    return StemPair(root, root.replace("̇", ""), stem, unit, entry)


def test_the_cards_come_in_the_plan_s_order_one_per_entry():
    """Plan 4.4: the word's own key before a peeled one, a prefixed stem before I, II and IV, a longer
    form before a shorter; one card per entry, its kind its best hit's."""
    index = verb_form_index([_pair("דלל", 10, "e-dll"), _pair("קרב", 1, "e-qrb"), _pair("סתדל", 1, "e-other"),
                             _pair("דלל", 1, "e-dll")])
    peeled_first = verb_form_cards(["ואסתדלו", "אסתדלו"], index)
    assert [(c.entry_id, c.kind, c.stems) for c in peeled_first] == [("e-dll", "D", (10,))]
    cards = verb_form_cards(["תדלל", "דלל"], {"תדלל": [_pair("קרב", 1, "e-own-short")],
                                             "דלל": [_pair("דלל", 1, "e-peeled")]})
    assert [c.entry_id for c in cards] == ["e-own-short", "e-peeled"], "the word's own key first"
    mixed = verb_form_cards(["אכתב"], {"אכתב": [_pair("כתב", 4, "e-iv"), _pair("כתב", 9, "e-ix")]})
    assert [(c.entry_id, c.kind) for c in mixed] == [("e-ix", "A"), ("e-iv", "B")], "a prefixed stem first"
    longer = verb_form_cards(["אסתדלו", "סתדלו"], {"סתדלו": [_pair("דלל", 10, "e-short")],
                                                  "אסתדלו": [_pair("דלל", 1, "e-long")]})
    assert [c.entry_id for c in longer] == ["e-long", "e-short"], "own key outranks the stem"
    same = verb_form_cards(["תדאלל", "דאלל"], {"תדאלל": [_pair("דלל", 6, "e")], "דאלל": [_pair("דלל", 3, "e")]})
    assert [(c.entry_id, c.kind, c.stems) for c in same] == [("e", "A", (3, 6))], "one entry, one card"
    assert verb_form_cards([], index) == [] and verb_form_cards(["", "זזזז"], index) == []


def test_a_card_opens_on_one_stem_only_and_says_how_it_was_reached():
    """The stem a card may open on is the one listed stem the word fits (as ``word_stem``); the evidence
    is the form with its final letter, the root as printed and the stems."""
    [x] = verb_form_cards(["אסתדלו"], verb_form_index([_pair("ד̇לל", 10, "e"), _pair("ד̇לל", 1, "e")]))
    assert (x.stem, x.form, x.evidence()) == (10, "אסתדלו", "אסתדלו (ד̇לל X)")
    [two] = verb_form_cards(["טאלב"], verb_form_index([_pair("טלב", 1, "e"), _pair("טלב", 3, "e")]))
    assert (two.stems, two.stem, two.evidence()) == ((1, 3), 0, "טאלב (טלב I, III)")
    [final] = verb_form_cards(["יסתעמלונ"], verb_form_index([_pair("עמל", 10, "e")]))
    assert final.evidence() == "יסתעמלון (עמל X)", "the form written with its final letter"
    [dup] = verb_form_cards(["אסתדלו"], {"אסתדלו": [_pair("דלל", 10, "e", "u1"), _pair("דלל", 10, "e", "u2")]})
    assert dup.hits == ((10, True, 6),), "two paragraphs of one stem are one hit"
