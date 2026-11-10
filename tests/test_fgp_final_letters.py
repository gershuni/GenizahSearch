# -*- coding: utf-8 -*-
"""GS-276: final letters the PDF font left as control characters, restored row by row (gate 3, rules §8).

``scripts/fgp_final_letters.py`` infers each row's code-to-letter mapping from a vocabulary; gate 3 measures it
on the rows that exist both ways (check T10) and records the mapping of every row that keeps today's text; gate 4
applies it to the content and the PDF page split it carries.
"""
from __future__ import annotations

import collections
import json

from scripts import fgp_final_letters as fl
from scripts import fgp_gate3_text_pipeline as g3
from scripts import fgp_gate4_chooser_validation as g4

M, N = "\x04", "\x18"      # Or.1081 J10 (row 4185): U+0004 was final mem, U+0018 final nun
ROW_4185 = (f"01 עבדהא אברהי{M} אלמקי{M}\n02 בסנבאט\n03 עבד הדרת הנגידות תכו{N} משרתה לנצח ינהי אלי סאמיהא\n"
            f"04 אנה מקי{M} בסנבאט וענדה אולאד ועאילה\n05 ואלכל מרצא וקד קצרת ידה א{N} ינפק עליה{M}")
CLEAN_4185 = ROW_4185.replace(M, "ם").replace(N, "ן")
VOCAB = collections.Counter({"אברהים": 40, "אלמקים": 5, "מקים": 9, "עליהם": 30, "תכון": 6, "אן": 400,
                             "אברהין": 1, "מקין": 1})


def test_a_rows_codes_are_read_off_its_words():
    decisions = fl.assign(fl.code_votes(ROW_4185, VOCAB))
    assert decisions[M]["letter"] == "ם" and decisions[M]["applied"]
    # the nun has one word of two letters or more ("תכו"); "א" is too short to vote
    assert decisions[N]["letter"] == "ן" and not decisions[N]["applied"], "one vote is below the bar"
    mapping = fl.mapping_of(decisions)
    assert mapping == {M: "ם"}
    repaired = fl.apply_mapping(ROW_4185, mapping)
    assert "אברהים אלמקים" in repaired and N in repaired, "a code below the bar stays as typed"


def test_a_code_inside_words_is_left_alone():
    text = f"של{M}ום של{M}ום אברהי{N}"
    votes = fl.code_votes(text, VOCAB)
    assert votes[M][2] == 2 and votes[M][1] == 0
    assert M not in fl.assign(votes), "inside-word codes are vowel points or dots, never a final letter"


def test_two_codes_do_not_share_a_letter_when_the_votes_allow_otherwise():
    vocab = collections.Counter({"אבם": 3, "אבן": 2, "גדם": 1, "גדן": 1, "הום": 5, "זחן": 5})
    text = f"אב{M} גד{N} הו{M} זח{N} אב{N} גד{N}"
    d = fl.assign(fl.code_votes(text, vocab))
    assert d[M]["letter"] == "ם" and d[N]["letter"] == "ן"


def test_the_measurement_leaves_each_rows_own_words_out():
    """With only the row's own clean text as vocabulary, nothing may be judged right: no leak."""
    vocab = fl.build_vocabulary([CLEAN_4185])
    C = fl.evaluate([(ROW_4185, CLEAN_4185)], vocab)
    assert C["judged"] == 0
    C = fl.evaluate([(ROW_4185, CLEAN_4185)], fl.build_vocabulary([CLEAN_4185, "אברהים אברהים אלמקים מקים עליהם"]))
    assert C["judged"] == 1 and C["right"] == 1


def test_build_vocabulary_skips_texts_with_codes_and_counts_each_text_once():
    v = fl.build_vocabulary(["שלום עולם", "שלום עולם", f"שלו{M}"])
    assert v == collections.Counter({"שלום": 1, "עולם": 1})


def _row(tid, status="identical", replaced=0, unit=("dbo_UnitTranscription", 1)):
    return {"today_id": tid, "row_key": f"today:{tid}", "status": status, "text_replaced": replaced,
            "unit_table": unit[0], "unit_id": unit[1]}


def test_gate3_step_repairs_kept_rows_and_measures_replaced_ones():
    clean_words = "אברהים אלמקים מקים עליהם " * 3
    unit_texts = {("dbo_UnitTranscription", 9): {"text": CLEAN_4185 + "\n" + clean_words},
                  ("dbo_UnitTranscription", 8): {"text": clean_words}}
    today = {1: (ROW_4185, None), 2: (ROW_4185, None), 3: ("שלום", None)}
    rows = [_row(1), _row(2, "accepted", 1, ("dbo_UnitTranscription", 9)), _row(3)]
    C = collections.Counter()
    out = g3.final_letter_step(rows, unit_texts, today, C)
    assert [r["today_id"] for r in out] == [1]
    assert json.loads(out[0]["mapping"]) == {"U+0004": "ם"} and "אברהים" in out[0]["text"]
    assert C["final_letters.kept_rows_with_codes"] == 1 and C["final_letters.codes_applied"] == 1
    assert C["final_letters.eval.pairs"] == 1 and C["final_letters.today_rows_with_codes"] == 2


def _t10(counts):
    checks = g3.run_checks([], [], collections.Counter(counts), {}, "d", None, True)
    return {c["name"]: c for c in checks}["T10_final_letters_accurate"]["passed"]


def test_t10_vouches_only_for_a_measured_inference():
    assert _t10({}) == 1, "nothing applied, nothing to vouch for"
    assert _t10({"final_letters.codes_applied": 5, "final_letters.eval.judged": 999,
                 "final_letters.eval.right": 999}) == 0, "too few judged"
    assert _t10({"final_letters.codes_applied": 5, "final_letters.eval.judged": 2000,
                 "final_letters.eval.right": 1980}) == 0, "99.0% is below the floor"
    assert _t10({"final_letters.codes_applied": 5, "final_letters.eval.judged": 2000,
                 "final_letters.eval.right": 1995}) == 1


def test_gate4_applies_the_mapping_to_the_text_and_its_page_split():
    sections = json.dumps([{"page_num": 1, "text": f"אברהי{M}"}, {"page_num": 2, "text": f"עליה{M} א{N}"}],
                          ensure_ascii=False)
    text, secs = g4.apply_final_letters(f"אברהי{M}\nעליה{M} א{N}", sections, {M: "ם"})
    assert text == f"אברהים\nעליהם א{N}"
    assert [s["text"] for s in json.loads(secs)] == ["אברהים", f"עליהם א{N}"]
    assert g4.apply_final_letters(None, None, {M: "ם"}) == (None, None)
