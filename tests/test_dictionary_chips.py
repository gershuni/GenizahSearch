# -*- coding: utf-8 -*-
"""web/components/blau_panel.py ``keep_chips``: the chips of the other possible roots stay while the
reader presses them one by one (owner, 2026-11-11: a press must not take the other chips away, so the
reader can check each root in turn)."""
from __future__ import annotations

from dataclasses import replace

from shared.blau_service import empty_lookup as blau_empty
from shared.friedman_service import empty_lookup as friedman_empty
from web.components import blau_panel

CHIPS = (("דרר", 0), ("דרי", 1))
OTHER = (("דור", 0),)


def _word(word, blau=(), friedman=()):
    return replace(blau_empty(word), suggestions=blau), replace(friedman_empty(word), suggestions=friedman)


def test_a_new_word_remembers_its_chips_and_shows_its_own_answer():
    state = {}
    blau, friedman = _word("נדרי", CHIPS, OTHER)
    got_blau, got_friedman, chips = blau_panel.keep_chips(state, "נדרי", "word", blau, friedman)
    assert (got_blau, got_friedman) == (blau, friedman) and chips == ("נדרי", "")
    assert state == {"word": "נדרי", "blau": CHIPS, "friedman": OTHER, "chosen": ""}


def test_a_chip_pressed_keeps_the_word_s_chips_and_marks_what_is_shown():
    state = {}
    blau_panel.keep_chips(state, "נדרי", "word", *_word("נדרי", CHIPS, OTHER))
    pressed_blau, pressed_friedman = _word("דרי I")   # a root with a stem: no chips of its own
    blau, friedman, chips = blau_panel.keep_chips(state, "דרי I", blau_panel.SUGGEST_HOW, pressed_blau, pressed_friedman)
    assert blau.suggestions == CHIPS and friedman.suggestions == OTHER and chips == ("נדרי", "דרי I")
    assert blau.word == "דרי I", "the answer is the chip's; only its chips are the word's"
    # A stem button on that card keeps them too; nothing shown now is a chip.
    _b, _f, chips = blau_panel.keep_chips(state, "דרי IV", blau_panel.STEM_HOW, *_word("דרי IV"))
    assert chips == ("נדרי", "דרי IV") and _b.suggestions == CHIPS
    # The word's own chip: back to the word, nothing marked.
    _b, _f, chips = blau_panel.keep_chips(state, "נדרי", blau_panel.SUGGEST_HOW, *_word("נדרי", CHIPS, OTHER))
    assert chips == ("נדרי", "") and _b.suggestions == CHIPS


def test_another_word_replaces_the_chips_and_a_word_without_chips_clears_them():
    state = {}
    blau_panel.keep_chips(state, "נדרי", "word", *_word("נדרי", CHIPS))
    _b, _f, chips = blau_panel.keep_chips(state, "אסתדלו", "word", *_word("אסתדלו"))
    assert chips == ("", "") and state["blau"] == ()
    _b, _f, chips = blau_panel.keep_chips(state, "דלל X", blau_panel.STEM_HOW, *_word("דלל X"))
    assert chips == ("", "") and _b.suggestions == (), "a stem press with no chips remembered changes nothing"
    _b, _f, chips = blau_panel.keep_chips(state, "תקרב", "box", *_word("תקרב", (), OTHER))
    assert chips == ("תקרב", "") and state["friedman"] == OTHER, "Friedman's chips alone are remembered too"


def test_an_expression_or_no_dictionary_is_left_as_it_is():
    state = {"word": "נדרי", "blau": CHIPS, "friedman": (), "chosen": ""}
    _b, _f, chips = blau_panel.keep_chips(state, "דרי I", blau_panel.SUGGEST_HOW, None, None)
    assert (_b, _f, chips) == (None, None, ("נדרי", "דרי I"))
    assert blau_panel.SUGGEST_HOW == "suggestion"
    assert blau_panel.lookup_event(False, None, None, blau_panel.SUGGEST_HOW)["how"] == "suggestion"
