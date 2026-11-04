# -*- coding: utf-8 -*-
"""scripts/measure_arabic_readings.py: the pure parts the Arabic-layer measurement rests on.

The frequency ranking puts the common reading first and the broken rankings do not; the
root finders peel what they should; the gate fails on a broken result. The readings and
keys themselves are tests/test_arabic_normalize.py's. No dictionary or corpus text is
needed: the counts and roots below are made up.
"""
from __future__ import annotations

from collections import Counter

import pytest

from scripts.measure_arabic_readings import (
    GATE,
    form_roots,
    gate,
    iter_pages,
    DEV_DOCUMENTS_FILE,
    lane_attribute,
    load_dev_documents,
    plural_alif_dropped,
    rank_readings,
    strip_roots,
)
from shared.arabic_normalize import buckwalter_to_arabic, ja_letters, root_key


def test_p2_compares_a_plural_alif_reading_letter_for_letter_with_the_page():
    """חצ'רו read حضروا: the alif v2 adds is not on the page, so the marked צ is still letter 1."""
    page = ja_letters("חצ'רו")
    assert plural_alif_dropped("حضروا", page) == "حضرو"
    assert plural_alif_dropped("حضرو", page) == "حضرو"
    assert plural_alif_dropped("حضرا", ja_letters("חצ'רא")) == "حضرا", "only after a final ו"


def test_the_development_documents_are_pinned_not_drawn(tmp_path):
    """A later seed or a newer pgp.db must not re-admit the documents the method was shaped on."""
    dev = load_dev_documents(str(DEV_DOCUMENTS_FILE))
    assert len(dev) == 300 and all(isinstance(i, int) for i in dev)
    empty = tmp_path / "dev.json"
    empty.write_text('{"pgpids": []}', encoding="utf-8")
    with pytest.raises(ValueError):
        load_dev_documents(str(empty))


def test_pages_of_one_manuscript_keep_their_own_headers(tmp_path):
    """The grading rows name the page, not only the manuscript: 990001_1 and 990001_2 differ."""
    text = tmp_path / "t.txt"
    text.write_text("==> 990001_1 <==\nאלף\n==> 990001_2 <==\nבית\n==> 990002_1 <==\nגימל\n", encoding="utf-8")
    pages = list(iter_pages(str(text), {"990001"}))
    assert [(s, h) for s, h, _ in pages] == [("990001", "990001_1"), ("990001", "990001_2")]


def test_lane_attributes_are_decoded_before_buckwalter():
    """``&amp;`` is Buckwalter's ؤ in XML; read raw it would be four letters."""
    name = lane_attribute('<div2 type="root" n="&amp;mr &amp;c.">', "n")
    assert name == "&mr &c."
    assert buckwalter_to_arabic(name.split()[0]) == "ؤمر"
    assert lane_attribute('<div2 type="root">', "n") == ""


def test_frequency_puts_the_common_reading_first_and_the_broken_rankings_do_not():
    counts = Counter({"ذلك": 82941, "دلك": 25, "غير": 23889, "جير": 9})
    assert [r for r, _ in rank_readings("דלך", counts)] == ["ذلك", "دلك"]
    assert [r for r, _ in rank_readings("דלך", counts, "plain")] == ["دلك"]
    assert [r for r, _ in rank_readings("דלך", counts, "reversed")] == ["دلك", "ذلك"]
    assert [r for r, _ in rank_readings("גיר", counts, "dotted")] == ["جير"]
    assert rank_readings("קקק", counts) == [], "an unattested word shows nothing"


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
