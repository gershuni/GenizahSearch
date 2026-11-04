# -*- coding: utf-8 -*-
"""scripts/measure_arabic_readings.py: the pure parts the Arabic-layer measurement rests on.

An undotted letter gives both readings and a marked one gives one (except ג, whose
mark means ج in Blau and غ in many editions); the frequency ranking puts the common
reading first and the broken rankings do not; the root keys of Lane, Lisān and a
peeled word meet; the gate fails on a broken result. No dictionary or corpus text
is needed: the counts and roots below are made up.
"""
from __future__ import annotations

from collections import Counter

from scripts.measure_arabic_readings import (
    GATE,
    buckwalter_to_arabic,
    form_roots,
    gate,
    ja_letters,
    rank_readings,
    readings,
    root_key,
    skeleton,
    strip_roots,
)


def test_an_undotted_letter_is_read_both_ways_and_a_marked_one_once():
    assert readings("דלך") == ["دلك", "دلخ", "ذلك", "ذلخ"], "plain letters first, the last letter varying fastest"
    assert readings("ד'לך") == ["ذلك", "ذلخ"]
    assert readings("ד\u0307לך") == ["ذلك", "ذلخ"]
    assert readings("ר'גל") == readings("רגל"), "an apostrophe after ר is an abbreviation, not a dot"
    assert set(readings("ג'יר")) == {"جير", "غير"}, "a mark on ג says only 'not plain'"
    assert ja_letters("אלמד׳") == [("א", False), ("ל", False), ("מ", False), ("ד", True)]


def test_a_final_alef_may_be_alef_maqsura_and_the_skeleton_folds_what_judeo_arabic_does_not_write():
    assert "موسي" in readings("מוסא")
    assert skeleton("شيء") == "شي" and skeleton("على") == "علي" and skeleton("مدينة") == "مدينه"
    assert skeleton("أ\u064eي\u0652ض\u064bا") == "ايضا" and skeleton("ذ\u0670ل\u0650ك\u064e") == "ذلك"


def test_frequency_puts_the_common_reading_first_and_the_broken_rankings_do_not():
    counts = Counter({"ذلك": 82941, "دلك": 25, "غير": 23889, "جير": 9})
    assert [r for r, _ in rank_readings("דלך", counts)] == ["ذلك", "دلك"]
    assert [r for r, _ in rank_readings("דלך", counts, "plain")] == ["دلك"]
    assert [r for r, _ in rank_readings("דלך", counts, "reversed")] == ["دلك", "ذلك"]
    assert [r for r, _ in rank_readings("גיר", counts, "dotted")] == ["جير"]
    assert rank_readings("קקק", counts) == [], "an unattested word shows nothing"


def test_root_keys_meet_across_the_dictionaries():
    assert root_key(buckwalter_to_arabic("Ax*")) == root_key("أخذ") == "ءخذ", "Lane writes initial hamza as A"
    assert root_key(buckwalter_to_arabic("A^bw")) == root_key("أبو")
    assert root_key("قال") == root_key("قول") == "قWل"
    assert root_key(buckwalter_to_arabic("$b")) == root_key("شبب"), "Lane writes a geminate root with two letters"


def test_peeling_prefers_the_proclitic_to_an_inner_letter():
    roots = {root_key(r) for r in ("بيت", "لبت", "كتب", "بقي", "وبق")}
    assert strip_roots("لبيت", roots)[0] == root_key("بيت")
    assert strip_roots("والكتاب", roots)[0] == root_key("كتب")
    assert strip_roots("وبقي", roots)[0] == root_key("بقي")
    assert strip_roots("زززز", roots) == []


def test_method_v2_reads_the_plural_alif_and_backs_off_to_the_stem():
    counts = Counter({"حضروا": 50, "تنفذ": 40})
    assert rank_readings("חצ'רו", counts) == [], "v1: Judeo-Arabic writes حضروا without its alif"
    assert rank_readings("חצ'רו", counts, version="v2")[0][0] == "حضروا"
    assert rank_readings("ותנפדה", counts, version="v2")[0][0] == "وتنفذه", "the stem تنفذ is counted"
    assert rank_readings("ותנפדה", counts) == []


def test_method_v3_finds_the_root_through_lanes_headwords():
    forms = {"سيد": {root_key("سود")}, "لسان": {root_key("لسن")}, "سان": {root_key("سنو")}}
    assert form_roots("سيدي", forms, Counter()) == [root_key("سود")]
    counts = Counter({"لسان": 1500, "سان": 3})
    assert form_roots("ولسان", forms, Counter(), counts)[0] == root_key("لسن"), "the common stem wins the tie"
    assert form_roots("زززز", forms, Counter()) == []


def _result(p1_right, p1_wrong, plain_right, p2_right):
    def counter(right, wrong):
        return {"right": right, "wrong": wrong, "none": 1000 - right - wrong}
    return {"P1_root_against_blau": {"frequency": counter(p1_right, p1_wrong), "plain": counter(plain_right, 100)},
            "P2_dot_against_edition": {"frequency": counter(p2_right, 0)}}


def test_the_gate_passes_a_good_result_and_fails_each_broken_one():
    assert gate(_result(750, 150, 600, 900), "frequency") == []
    low_dot = int(GATE["G1_p2_dot_agreement_min"] * 1000) - 1
    assert any(f.startswith("G1") for f in gate(_result(750, 150, 600, low_dot), "frequency"))
    assert any(f.startswith("G2") for f in gate(_result(620, 150, 600, 900), "frequency"))
    assert any(f.startswith("G3") for f in gate(_result(600, 350, 500, 900), "frequency"))
