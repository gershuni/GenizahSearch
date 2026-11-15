# -*- coding: utf-8 -*-
"""scripts/draw_dictionary_stems_sample.py: the graded click sample's draw (the private dictionary lookup
plan, section 6.1). Pinned: which printed words may be drawn, the quota rule (a word counts in both
dictionaries, drawing stops when every graded kind is full), what the grader sees (no dictionary, no
kind), and the cards read off the real lookups on the synthetic sidecars the service tests build.
"""
from __future__ import annotations

import json
import random

import pytest

from scripts import draw_dictionary_stems_sample as sample
from scripts.build_friedman_sidecar import write_sidecar
from shared import friedman_service as fs
from shared.blau_service import BlauService
from tests.test_blau_service import _build, _with_stems
from tests.test_friedman_service import stem_units


@pytest.mark.parametrize("token, word", [
    ("ואלקול,", "ואלקול"), ("תלתה.", "תלתה"), ("(קאל)", "קאל"), ("ג'מיע", "ג'מיע"),
    ("[אל]קול", ""), ("קא(?)ל", ""), ("אל..", "אל"), ("12", ""), ("abc", ""), ("…", ""),
])
def test_a_printed_token_is_cleaned_as_a_reader_clicks_it_and_editorial_marks_leave_it_out(token, word):
    assert sample.clean_word(token) == word


def test_function_words_and_short_words_are_not_drawn():
    assert sample.eligible("אסתדלו") and sample.eligible("ואלקול")
    assert not sample.eligible("פי") and not sample.eligible("אלי") and not sample.eligible("מן")
    assert not sample.eligible("") and not sample.eligible("קל")


def test_occurrences_and_context():
    texts = [{"tokens": ["פי", "אסתדלו", "[אל]קול", "קאל", "אן"]}, {"tokens": ["טאלבתה"]}]
    assert sample.occurrences(texts) == [(0, 1), (0, 3), (1, 0)]
    assert sample.context(texts[0]["tokens"], 3, width=2) == ("אסתדלו [אל]קול", "אן")
    assert sample.context(texts[0]["tokens"], 0, width=8) == ("", "אסתדלו [אל]קול קאל אן")


def _card(root, kind, stems=(10,)):
    return {"entry": root, "root": root, "stems": list(stems), "stem": stems[0] if len(stems) == 1 else 0,
            "kind": kind, "form": "x"}


def test_the_draw_fills_every_quota_and_a_word_counts_in_both_dictionaries():
    words = ["אאאא", "בבבב", "גגגג", "דדדד", "הההה", "וווו"]
    texts = [{"source": 1, "pgpid": 7, "sys_id": "990001", "tokens": words}]
    plan = {"אאאא": {"blau": [_card("דלל", "A")], "friedman": [_card("דלל", "A")], "own": {"blau": 2, "friedman": 0}},
            "בבבב": {"blau": [_card("קרב", "B", (1, 2))], "friedman": []},
            "גגגג": {"blau": [_card("קרב", "A")], "friedman": []},   # Blau's A full: not kept
            "דדדד": {"blau": [], "friedman": [_card("כתב", "D"), _card("קרב", "A")]},
            "הההה": {"blau": [], "friedman": []},
            "וווו": {"blau": [_card("ספר", "D")], "friedman": [_card("ספר", "B")]}}
    order = [(0, n) for n in range(len(words))]
    items, counts, drawn = sample.draw(order, texts, lambda w: plan[w], quota=1)
    assert [i["word"] for i in items] == ["אאאא", "בבבב", "דדדד", "וווו"]
    assert counts == {("blau", "A"): 1, ("friedman", "A"): 1, ("blau", "B"): 1, ("friedman", "D"): 1,
                      ("blau", "D"): 1, ("friedman", "B"): 1}
    assert drawn == 6 and items[0]["kinds"] == {"blau": "A", "friedman": "A"}
    assert items[2]["kinds"] == {"friedman": "D"}, "a word's kind is its first card's"
    assert items[0]["link"] == sample.BROWSE_URL + "990001" and items[0]["id"] == "s0001"
    assert items[0]["own"] == {"blau": 2, "friedman": 0} and set(items[0]["cards"]) == {"blau", "friedman"}, (
        "how many cards the word itself reached, for the split the plan reports")
    assert items[1]["own"] == {} and items[1]["cards"]["friedman"] == []
    stopped, _counts, at = sample.draw(order + [(0, 0)] * 5, texts, lambda w: plan[w], quota=1)
    assert at == 6, "drawing stops once every quota is full"


def test_both_dictionaries_roots_are_graded_once_each_with_the_union_of_stems():
    cards = {"blau": [_card("דלל", "A", (10,)), _card("ד̇לל", "B", (1, 4))],
             "friedman": [_card("דלל", "A", (1, 10)), _card("קרב", "D", (5,))]}
    assert sample.roots_to_grade(cards) == [{"root": "דלל", "stems": [1, 10]}, {"root": "ד̇לל", "stems": [1, 4]},
                                            {"root": "קרב", "stems": [5]}]


def test_the_grader_sees_no_dictionary_no_kind_and_no_link():
    item = {"id": "s0003", "word": "אסתדלו", "left": "קאל", "right": "פיה", "link": "https://x", "kinds": {"blau": "A"},
            "cards": {"blau": [_card("דלל", "A")]}, "roots": [{"root": "דלל", "stems": [10]}]}
    graded = sample.grading_item(item, lambda root, stem: [f"gloss of {root} {stem}"])
    assert graded == {"id": "s0003", "word": "אסתדלו", "before": "קאל", "after": "פיה", "roots": [
        {"rid": "s0003-r1", "root": "דלל", "stems": [{"stem": "X", "glosses": ["gloss of דלל 10"]}]}]}
    text = json.dumps(graded, ensure_ascii=False)
    assert "blau" not in text and "friedman" not in text and "kind" not in text and "https" not in text
    assert [len(b) for b in sample.batches(list(range(60)), 25)] == [25, 25, 10]


def test_the_cards_are_read_off_the_lookups_on_synthetic_sidecars(tmp_path, monkeypatch):
    blau = BlauService(str(_build(tmp_path / "b", _with_stems())))
    assert sample.blau_cards(blau, "אסתדלו") == [
        {"entry": "blau-e-000110", "root": "דלל", "stems": [10], "stem": 10, "kind": "A", "form": "אסתדלו"}]
    assert sample.blau_cards(blau, "ידלל")[0]["kind"] == "B"
    assert sample.blau_cards(blau, "דלל") == [], "the word's own entry is no card found by verb form"
    path = tmp_path / "friedman.db"
    write_sidecar(stem_units(), {"index_version": 1, "rows": []},
                  {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    friedman = fs.FriedmanService(str(path))
    assert sample.friedman_cards(friedman, "ואסתדלו") == [
        {"entry": "f000101", "root": "דלל", "stems": [10], "stem": 10, "kind": "D", "form": "אסתדלו"}]
    glosses = sample.stem_glosses(blau, friedman, "דלל", 10, {})
    assert glosses and all(isinstance(g, str) for g in glosses)
    blau.close()


def test_the_seeded_order_is_reproducible():
    order = [(0, n) for n in range(50)]
    a, b = list(order), list(order)
    random.Random(sample.SEED).shuffle(a)
    random.Random(sample.SEED).shuffle(b)
    assert a == b and a != order
