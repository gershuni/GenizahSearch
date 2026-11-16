# -*- coding: utf-8 -*-
"""What the dictionary panels show a reader is never an internal name or lookup key (2026-11-16):
no match-details tooltip, keys with their final letters, and no match that needs both a
spelling the word lacks and a form the entry lacks (ממלוך -> ממלכה̈)."""
from __future__ import annotations

from pathlib import Path

from shared.blau_normalize import stacked_variant

REPO = Path(__file__).resolve().parent.parent


def test_a_spelling_change_and_an_entry_change_together_are_another_word():
    assert stacked_variant((('mater', 'source_suffix'),))
    assert stacked_variant((('prefix', 'interchange', 'source_suffix'), ('mater', 'source_prefix')))
    assert not stacked_variant((('mater',),)), 'a possible spelling alone stays'
    assert not stacked_variant((('suffix', 'source_suffix'),)), 'an entry change alone stays'
    assert not stacked_variant((('mater', 'source_suffix'), ('suffix',))), 'one plain way in is enough'
    assert not stacked_variant(())


def test_both_lookups_never_add_such_matches():
    blau = (REPO / 'shared' / 'blau_service.py').read_text(encoding='utf-8')
    assert "return stacked_variant(matching_paths(word, _text(row['raw']), candidate))" in blau
    assert blau.count('not other_word(row, candidate)') == 2, 'Blau never makes them a card'
    assert blau.count('stacked_variant(') == 1
    friedman = (REPO / 'shared' / 'friedman_service.py').read_text(encoding='utf-8')
    assert 'if stacked_variant(evidence):\n                return' in friedman, 'Friedman never adds them'


def test_no_match_details_tooltip():
    """How a match was reached is not explained to the reader (owner, 2026-11-16): the internal steps
    are not a reliable account, and the results speak for themselves."""
    panel = (REPO / 'web' / 'components' / 'expression_panel.py').read_text(encoding='utf-8')
    body = panel.split('def render_match_details', 1)[1].split('\ndef ', 1)[0]
    assert 'dict-match-details' not in panel and '.tooltip(' not in body


def test_keys_are_shown_with_final_letters():
    friedman = (REPO / 'web' / 'components' / 'friedman_panel.py').read_text(encoding='utf-8')
    blau = (REPO / 'web' / 'components' / 'blau_panel.py').read_text(encoding='utf-8')
    panel = (REPO / 'web' / 'components' / 'expression_panel.py').read_text(encoding='utf-8')
    assert '_rtl(with_final_letters(match.matched))' in friedman
    assert '_rtl(with_final_letters(candidate))' in blau
    assert "spelling = with_final_letters(" in panel
