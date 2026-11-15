# -*- coding: utf-8 -*-
"""scripts/score_dictionary_stems_sample.py: the pass rule of the graded click sample (the private
dictionary lookup plan, section 6.1, decided 2026-11-11): at most 5 wrong-only words in 100 gradable,
cannot-tell at most 25% of those drawn, the stem opened only when right in 85% of the root-right words;
the second review replaces the grader's verdict."""
from __future__ import annotations

import json

import pytest

from scripts import score_dictionary_stems_sample as score
from shared.ja_roots import dot_pattern


def _item(n, cards, kind="A", own=0):
    """One drawn word with Blau cards [(root, stems)], graded later by root."""
    roots = []
    for root, stems in cards:
        if root not in [r["root"] for r in roots]:
            roots.append({"root": root, "stems": list(stems)})
    return {"id": f"s{n:04d}", "kinds": {"blau": kind},
            "cards": {"blau": [{"root": r, "stems": list(s), "stem": s[0] if len(s) == 1 else 0, "kind": kind}
                               for r, s in cards], "friedman": []},
            "own": {"blau": own, "friedman": 0}, "roots": roots}


def _answer(item, *verdicts):
    return {"id": item["id"], "roots": [{"rid": f"{item['id']}-r{n}", "root": v[0], "stem": v[1], "note": ""}
                                        for n, v in enumerate(verdicts, 1)]}


def test_wrong_only_cannot_tell_first_right_and_the_stem():
    right = _item(1, [("קול", (1,))])
    second = _item(2, [("קלל", (1,)), ("קול", (1, 4))])
    wrong = _item(3, [("להו", (8,))])
    mixed = _item(4, [("דרר", (1,)), ("דרי", (1,))])
    unclear = _item(5, [("עפו", (1, 3))])
    answers = [_answer(right, ("right", "I")), _answer(second, ("wrong", ""), ("right", "IV")),
               _answer(wrong, ("wrong", "")), _answer(mixed, ("wrong", ""), ("cannot_tell", "")),
               _answer(unclear, ("cannot_tell", ""))]
    t = score.score([right, second, wrong, mixed, unclear], answers)["blau"]["A"]
    assert (t["drawn"], t["gradable"], t["cannot_tell"], t["wrong_only"]) == (5, 4, 1, 2), (
        "no root right and not all cannot-tell: wrong-only (the plan's 'none of whose roots is right')")
    assert (t["first_right"], t["root_right"], t["stem_right"], t["stem_whole"]) == (1, 2, 1, 1)
    assert t["stem_guessed"] == 1 and t["stem_right_pct"] == 100.0 and not t["passes"], (
        "a word that fits several stems opens on the whole entry: not counted for or against the stem")


@pytest.mark.parametrize("wrong, cannot, passes", [(5, 0, True), (6, 0, False), (0, 33, True), (0, 34, False)])
def test_the_pass_rule(wrong, cannot, passes):
    """At most 5 wrong-only in 100 gradable; cannot-tell at most 25% of those drawn (33 of 133)."""
    items, answers = [], []
    for n in range(100 + cannot):
        item = _item(n, [("כתב", (10,))])
        verdict = ("cannot_tell", "") if n >= 100 else (("wrong", "") if n < wrong else ("right", "X"))
        items.append(item)
        answers.append(_answer(item, verdict))
    t = score.score(items, answers)["blau"]["A"]
    assert t["passes"] is passes and t["gradable"] == 100


@pytest.mark.parametrize("stem_right, opens", [(85, True), (84, False)])
def test_a_kind_opens_on_the_stem_only_when_it_is_right_in_85_percent(stem_right, opens):
    items, answers = [], []
    for n in range(100):
        item = _item(n, [("כתב", (10,))])
        items.append(item)
        answers.append(_answer(item, ("right", "X" if n < stem_right else "I")))
    t = score.score(items, answers)["blau"]["A"]
    assert t["passes"] and t["opens_on_stem"] is opens


def test_the_second_review_replaces_the_grader_and_ungraded_words_wait():
    wrong = _item(1, [("להו", (8,))])
    pending = _item(2, [("כתב", (10,))])
    answers = [_answer(wrong, ("wrong", ""))]
    before = score.score([wrong, pending], answers)
    assert before["blau"]["A"]["wrong_only"] == 1 and before["blau"]["A"]["drawn"] == 1, "an ungraded word waits"
    after = score.score([wrong, pending], answers, {"s0001-r1": {"root": "right", "stem": "VIII"}})
    assert after["blau"]["A"]["wrong_only"] == 0 and after["blau"]["A"]["stem_right"] == 1


def test_the_split_by_whether_the_dictionary_also_found_the_word_itself():
    a, b = _item(1, [("כתב", (10,))], own=2), _item(2, [("כתב", (10,))])
    t = score.score([a, b], [_answer(a, ("wrong", "")), _answer(b, ("right", "X"))])["blau"]["A"]
    assert t["also_found_itself"]["wrong_only"] == 1 and t["found_only_by_verb_form"]["wrong_only"] == 0


def test_kinds_are_scored_apart_and_the_wilson_bound():
    a, b = _item(1, [("כתב", (10,))], kind="A"), _item(2, [("כתב", (1,))], kind="B")
    result = score.score([a, b], [_answer(a, ("right", "X")), _answer(b, ("wrong", ""))])
    assert result["blau"]["A"]["wrong_only"] == 0 and result["blau"]["B"]["wrong_only"] == 1
    assert result["blau"]["D"]["drawn"] == 0 and not result["blau"]["D"]["passes"]
    assert score.wilson_upper(5, 100) == pytest.approx(0.112, abs=0.001)
    assert score.wilson_upper(0, 0) == 1.0


def test_load_reads_the_answers_but_not_a_rejected_one(tmp_path):
    item = _item(1, [("כתב", (10,))])
    (tmp_path / "sample.jsonl").write_text(json.dumps(item, ensure_ascii=False) + "\n", encoding="utf-8")
    answers = tmp_path / "grading" / "answers"
    answers.mkdir(parents=True)
    (answers / "batch_01.json").write_text(json.dumps({"items": [_answer(item, ("right", "X"))]}), encoding="utf-8")
    (answers / "batch_01.rejected-1.json").write_text(json.dumps({"items": [_answer(item, ("wrong", ""))]}),
                                                      encoding="utf-8")
    sample, got, review, excluded = score.load(tmp_path)
    assert [a["roots"][0]["root"] for a in got] == ["right"] and review == {} and excluded == {}
    (tmp_path / "excluded.json").write_text(json.dumps({"s0001": "Hebrew"}), encoding="utf-8")
    assert score.load(tmp_path)[3] == {"s0001": "Hebrew"}


def _two(n, blau, friedman, kind="B"):
    """A word with Blau's and Friedman's cards [(root, stems)]."""
    item = _item(n, blau, kind=kind)
    item["kinds"]["friedman"] = kind
    item["cards"]["friedman"] = [{"root": r, "stems": list(s), "stem": s[0] if len(s) == 1 else 0, "kind": kind}
                                 for r, s in friedman]
    for root, stems in friedman:
        if root not in [r["root"] for r in item["roots"]]:
            item["roots"].append({"root": root, "stems": list(stems)})
    return item


def test_a_right_root_from_either_dictionary_is_a_win_unless_strict():
    """Recall counts more than precision: ندري got דרר from Friedman and דרי from Blau;
    the reader sees both tabs, so the word is a win in both. Strict scoring keeps each dictionary apart."""
    word = _two(1, [("דרי", (1,))], [("דרר", (1, 4))])
    answers = [_answer(word, ("right", "I"), ("wrong", ""))]
    pooled = score.score([word], answers)
    assert pooled["friedman"]["B"]["wrong_only"] == 0 and pooled["blau"]["B"]["wrong_only"] == 0
    strict = score.score([word], answers, pooled=False)
    assert strict["friedman"]["B"]["wrong_only"] == 1 and strict["blau"]["B"]["wrong_only"] == 0
    assert pooled["friedman"]["B"]["stem_guessed"] == 0, "the stem counts only from the dictionary's own right card"


def test_a_pooled_word_waits_for_the_other_dictionary_s_roots_to_be_graded():
    """Pooled, a word is judged on every root proposed for it; while one the other dictionary proposed
    has no verdict yet, the word waits (it is not counted wrong-only). Strict scoring needs only the
    dictionary's own roots."""
    word = _two(1, [("דרי", (1,))], [("דרר", (1, 4))])
    graded = {dot_pattern("דרי"): {"root": "wrong", "stem": ""}}
    assert score.judge(word, "blau", graded) is None, "דרר (Friedman's) is not graded yet"
    strict = score.judge(word, "blau", graded, pooled=False)
    assert strict is not None and strict["wrong_only"]
    graded[dot_pattern("דרר")] = {"root": "right", "stem": "I"}
    pooled = score.judge(word, "blau", graded)
    assert pooled is not None and not pooled["wrong_only"]


def test_hebrew_and_aramaic_words_are_left_out():
    """Hebrew and Aramaic words are irrelevant (``excluded.json``)."""
    hebrew, arabic = _item(1, [("נהו", (1,))]), _item(2, [("כתב", (10,))])
    answers = [_answer(hebrew, ("wrong", "")), _answer(arabic, ("right", "X"))]
    result = score.score([hebrew, arabic], answers, excluded={"s0001": "Hebrew"})
    assert result["blau"]["A"]["drawn"] == 1 and result["blau"]["A"]["wrong_only"] == 0
    assert result["items"] == 1 and result["excluded"] == 1
