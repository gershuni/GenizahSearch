# -*- coding: utf-8 -*-
"""scripts/measure_dictionary_stems.py: the gate of PR 2 (the private dictionary lookup plan, section 6).

Its pure functions on hand-made pairs, then a whole run on the synthetic sidecars the service tests
build (no dictionary text): the answer key is Friedman's index of forms, a card is right when it names
the index's root, the order is plan 4.4's, the baseline's wrong-only is split, the quotations' misses
are explained, and every mutation run moves a number.
"""
from __future__ import annotations

from collections import Counter

import pytest

from scripts import measure_dictionary_stems as gate
from scripts.build_friedman_sidecar import write_sidecar
from shared import friedman_service as fs
from shared.ja_roots import StemPair
from tests.test_blau_service import _build, _with_stems
from tests.test_friedman_service import VIEWER, stem_units


def _pair(root, stem, entry="e", unit="u"):
    return StemPair(root, root, stem, unit, entry)


def test_a_word_s_keys_are_its_own_then_its_clitics_peeled():
    assert gate.word_keys("ואסתגאב")[:2] == ["ואסתגאב", "אסתגאב"]
    assert gate.word_keys("") == [] and gate.word_keys("abc") == []


def test_cards_come_in_plan_4_4_s_order():
    """The word's own key before a peeled one; a prefixed stem before I, II, IV; a longer form first."""
    index = gate.restricted_index([_pair("קרר", 10, "x"), _pair("סתק", 1, "i"), _pair("וסת", 1, "w")],
                                  {"אסתקר", "סתקר", "וסתק"})
    assert {k: [p.entry_id for p in v] for k, v in index.items()} == {"אסתקר": ["x"]}
    own_vs_peeled = gate.cards_for(["ואסתקר", "אסתקר"], {"ואסתקר": [_pair("וסת", 1, "own")],
                                                          "אסתקר": [_pair("קרר", 10, "peeled")]})
    assert [c.entry_id for c in own_vs_peeled] == ["own", "peeled"]
    same = gate.cards_for(["תקאתל"], {"תקאתל": [_pair("קתל", 1, "k"), _pair("קתל", 6, "k")]})
    assert [(c.entry_id, sorted(h[0] for h in c.hits)) for c in same] == [("k", [1, 6])], "one entry, one card"
    two = gate.cards_for(["אקבל"], {"אקבל": [_pair("קבל", 1, "first-seen"), _pair("אקב", 10, "prefixed")]})
    assert [c.entry_id for c in two] == ["prefixed", "first-seen"]


def test_root_letters_and_shapes():
    assert gate.has_root_letters("מכתוב", "כתב") and not gate.has_root_letters("מכתוב", "קרב")
    assert gate.has_root_letters("קאל", "קול") is True, "the weak letter may be gone"
    assert gate.has_root_letters("ראית", "ראי") is None, "one strong letter: cannot tell"
    assert gate.shape("מכאתבת", "כתב") == "מ1א23ת" and gate.shape("כלאמ", "כלמ") == "12א3", "on keys: finals folded"


def test_scoring_counts_reach_first_wrong_only_and_nothing():
    # אסתקר: X of קרר, right. תקאתל: VI of קתל, right. אקבל: I of קבל (right) and, first, the prefixed X
    # of a root אקב the index does not give (wrong). יקתל: I of קתל, but the index says קטל: wrong only.
    answers = {"אסתקר": {("קרר", "קרר")}, "תקאתל": {("קתל", "קתל")}, "זזז": {("זזז", "זזז")},
               "אקבל": {("קבל", "קבל")}, "יקתל": {("קטל", "קטל")}}
    index = {**gate.restricted_index([_pair("קרר", 10, "x"), _pair("קתל", 6, "k"), _pair("קבל", 1, "q"),
                                      _pair("קתל", 1, "k")], {"אסתקר", "תקאתל", "אקבל", "זזז", "יקתל"})}
    index["אקבל"] = index["אקבל"] + [_pair("אקב", 10, "a")]

    def right(card, roots):
        return any(card.root_key == key for _dotted, key in roots)
    result = gate.score_forms(answers, index, right, lambda roots: ("זזז", "זזז") not in roots)
    assert {k: result["all"][k] for k in ("n", "reach", "first", "wrong_only", "nothing")} == \
        {"n": 5, "reach": 3, "first": 2, "wrong_only": 1, "nothing": 1}
    assert result["listed"]["n"] == 4 and result["listed"]["nothing"] == 0
    assert result["right_share_by"]["stem"] == {"1": 50.0, "10": 50.0, "6": 100.0}


def test_the_baseline_s_wrong_only_is_split_by_why():
    answers = {"כתאב": {("כתב", "כתב")}, "אכרג": {("כרג", "כרג")}, "קול": {("קול", "קול")}}
    cards = {"כתאב": [("כתאב", "כתאב", False)],   # a noun entered under its own spelling
             "אכרג": [("דכל", "דכל", True)],      # through a sub-unit's head
             "קול": [("קלל", "קלל", False)]}      # another root
    result = gate.score_baseline(answers, lambda form: cards[form], lambda c, roots: any(c[1] == k for _d, k in roots),
                                 lambda _roots: True)
    assert result["all"]["wrong_only"] == 3
    assert result["wrong_only_split"] == {"entry_filed_under_the_word_itself": 1, "through_a_sub_unit_head": 1,
                                          "another_root": 1}


def test_a_quotation_miss_is_explained():
    pair = _pair("כתב", 1)
    items = [(pair, "כתב אליה", {}),                          # held: the perfect
             (pair, "ומכתוב", {}),                            # held: the passive participle, conjunction peeled
             (pair, "אסתכתב", {"under_a_sub_entry": True}),  # another stem (X) of the root
             (pair, "מכאתבת", {}),                            # a word of the root no rule generates
             (pair, "גדא", {"column_disagrees_with_head": True}),
             (_pair("קול", 1), "קאל", {}),                   # held: the hollow perfect
             (_pair("ראי", 1), "בל", {})]                    # a root of one strong letter
    result = gate.quotation_coverage(items, {"כתב": {1}})
    assert (result["quotations"], result["held"]) == (7, 3)
    assert result["miss_reasons"] == {"other_stem": 1, "root_word_not_generated": 1, "no_root_word": 1,
                                      "weak_root_unknown": 1}
    assert result["miss_flags"] == {"under_a_sub_entry": 1, "column_disagrees_with_head": 1}
    assert result["missing_shapes"] == {"מ1א23ת": 1}


def test_the_offer_rate_counts_words_with_a_card():
    index = gate.restricted_index([_pair("קרר", 10)], {"אסתקר"})
    assert gate.offer_rate({"אסתקר", "זזז"}, index) == {
        "words": 2, "offered": 1, "offered_pct": 50.0, "cards_per_offered_word": 1.0, "one_root_pct_of_offered": 100.0}


@pytest.fixture
def sidecars(tmp_path, monkeypatch):
    """Friedman's stem units with an index of three forms (one generated by a listed stem, one by a
    stem the entry does not list, one of no listed root), and Blau's stem roots."""
    def row(form, root):
        return {"form": form, "form_key": fs.normalize_ja(form), "line": form, "pdf_page": 1, "printed_page": "969",
                "url": f"{VIEWER}Value_Keys.pdf#page=1",
                "roots": [{"text": root, "dotted": root, "key": fs.normalize_ja(root), "alternatives": 1}]}
    friedman = tmp_path / "friedman.db"
    write_sidecar(stem_units(), {"index_version": 1, "rows": [row("אסתדלו", "דלל"), row("אנדל", "דלל"),
                                                                row("ינקרב", "קרב"), row("מגאז", "גוז")]},
                  {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, friedman)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    return str(friedman), str(_build(tmp_path, _with_stems()))


def test_a_whole_run_on_synthetic_sidecars(sidecars):
    result = gate.measure(*sidecars)
    assert result["answer_key"]["forms"] == 4 and result["verb_forms_version"] == 2
    friedman, blau = result["friedman"], result["blau"]
    # אסתדלו is X of דלל, listed in both; אנדל is VII of דלל, listed in Blau only; ינקרב is VII of קרב, listed
    # nowhere (Friedman and Blau list V only); מגאז's root is in neither.
    assert friedman["forms"]["all"]["reach"] == 1 and friedman["forms"]["listed"]["n"] == 3
    assert blau["forms"]["all"]["reach"] == 2 and blau["forms"]["listed"]["n"] == 3
    assert friedman["forms"]["all"]["nothing"] == 3
    for name in ("friedman", "blau"):
        assert result["build"][name]["keys"] > 0 and result["build"][name]["seconds"] >= 0
        assert set(result["mutations"][name]) == {"weak_rules_off", "two_letter_forms_on", "listed_pair_filter_off",
                                                  "stem_viii_set"}
    # The filter off lets every root take every stem: ינקרב (VII of קרב) is then reached in both.
    assert result["mutations"]["friedman"]["listed_pair_filter_off"]["reach"] == 3
    assert result["mutations"]["blau"]["listed_pair_filter_off"]["reach"] == 3
    summary = gate.summary(result)
    assert "== friedman" in summary and "== blau" in summary and "mutation listed_pair_filter_off" in summary


def test_the_baseline_is_today_s_lookup_without_the_cards_found_by_verb_form(sidecars, monkeypatch):
    """Blau's baseline is the lookup as it was before the verb forms: the cards they find are the
    treatment the gate measures, so BLAU_VERB_FORM_KINDS (A, B and D by default) must not reach it.
    אסתדלו reaches nothing in Blau by itself, and דלל X only by its verb form."""
    from shared import blau_service
    monkeypatch.delenv(blau_service.ENV_VERB_FORM_KINDS, raising=False)
    defaults = gate.measure(*sidecars)["blau"]["baseline"]
    monkeypatch.setenv(blau_service.ENV_VERB_FORM_KINDS, "")
    off = gate.measure(*sidecars)["blau"]["baseline"]
    monkeypatch.setenv(blau_service.ENV_VERB_FORM_KINDS, "ABCDE")
    every_kind = gate.measure(*sidecars)["blau"]["baseline"]
    assert defaults == off == every_kind


def test_the_mutation_runs_can_fail_the_rules(sidecars, monkeypatch):
    """With the weak-root rules off, דלל's contracted X form is no longer generated: reach falls."""
    healthy = gate.measure(*sidecars)["friedman"]["forms"]["all"]["reach"]
    original = gate.stem_forms

    def no_weak(root, stem, **rules):
        return original(root, stem, **{**rules, "weak": False})
    monkeypatch.setattr(gate, "stem_forms", no_weak)
    words = Counter(gate.word_keys("אסתדל"))
    assert words and gate.measure(*sidecars)["friedman"]["forms"]["all"]["reach"] <= healthy
