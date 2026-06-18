# -*- coding: utf-8 -*-
"""TDD RED — word-level model + compose mapping for the Joins Lab builder.

Asserts the COMPOSED query string produced by:
  build_side_query(lines_state, variants, page_position)  [new word-model shape]
  -> compose(side_query)
  -> query_str

The new lines_state shape (per plan 118-06):
  [
    {
      'words': [
        {'term': str, 'mods': dict, 'gap_to_next_word': int},
        ...
      ],
      'line_start': bool,
      'line_end': bool,
      'gap_to_next_line': int,
    },
    ...
  ]

Tests:
  1. Single line, one word, no mods -> term only
  2. Single line, two words, gap 2, first word has prefix -> '#<w1> [2] <w2>'
  3. One line with line_start=True -> leading | on first token
  4. Two lines with a line gap -> [|N] between them
  5. negation/plene/wildcard per word hoist exactly as _apply_modifiers_to_term
  6. Empty model -> build_side_query returns None
  7. Line with multiple words and gap=0 -> words joined with space (no [N])
  8. Two words, gap 1, second word has negation
  9. line_end=True -> trailing | on last token
"""
from __future__ import annotations

import pytest

from shared.joins_lab import compose


def _get_build_side_query():
    """Import build_side_query inside test body (RED guard: import deferred so
    collection passes even if not yet implemented)."""
    from web.components.joins_builder import build_side_query
    return build_side_query


# ---------------------------------------------------------------------------
# Helper: make a single-word line dict
# ---------------------------------------------------------------------------

def _word(term, mods=None, gap_to_next_word=0):
    return {'term': term, 'mods': mods or {}, 'gap_to_next_word': gap_to_next_word}


def _line(words, line_start=False, line_end=False, gap_to_next_line=0):
    return {
        'words': words,
        'line_start': line_start,
        'line_end': line_end,
        'gap_to_next_line': gap_to_next_line,
    }


# ---------------------------------------------------------------------------
# Test 1: Single line, one word, no mods -> term only
# ---------------------------------------------------------------------------

def test_single_word_no_mods():
    """Single line with one word and no mods returns the bare term."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום')])]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q == 'שלום', f"Expected 'שלום', got {q!r}"


# ---------------------------------------------------------------------------
# Test 2: Two words, gap 2, first word has prefix
# ---------------------------------------------------------------------------

def test_two_words_gap_prefix():
    """Two words in a line with gap=2 and prefix on first: '#שלום [2] עליכם'."""
    bsq = _get_build_side_query()
    lines = [
        _line([
            _word('שלום', mods={'prefix': True}, gap_to_next_word=2),
            _word('עליכם'),
        ])
    ]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q == '#שלום [2] עליכם', f"Expected '#שלום [2] עליכם', got {q!r}"


# ---------------------------------------------------------------------------
# Test 3: line_start=True -> leading | on first token
# ---------------------------------------------------------------------------

def test_line_start_adds_pipe():
    """line_start=True on a single-word line adds | to the first token."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום')], line_start=True)]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q is not None
    # The first token must start with |
    tokens = q.split()
    assert tokens[0].startswith('|'), f"Expected leading | token, got {q!r}"


# ---------------------------------------------------------------------------
# Test 4: Two lines with a line gap -> [|N] between them
# ---------------------------------------------------------------------------

def test_two_lines_with_gap():
    """Two lines with gap_to_next_line=3 emits [|3] between them."""
    bsq = _get_build_side_query()
    lines = [
        _line([_word('שלום')], gap_to_next_line=3),
        _line([_word('עליכם')]),
    ]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q is not None
    assert '[|3]' in q, f"Expected [|3] line-gap marker, got {q!r}"
    assert 'שלום' in q
    assert 'עליכם' in q


# ---------------------------------------------------------------------------
# Test 5: negation/plene/wildcard per word hoist
# ---------------------------------------------------------------------------

def test_negation_hoist():
    """Negation mod: term becomes '-שלום'."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום', mods={'negation': True})])]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q == '-שלום', f"Expected '-שלום', got {q!r}"


def test_plene_hoist():
    """Plene mod: term becomes '%שלום'."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום', mods={'plene': True})])]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q == '%שלום', f"Expected '%שלום', got {q!r}"


def test_wildcard_prefix_hoist():
    """wildcard_prefix mod: term becomes '*שלום'."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום', mods={'wildcard_prefix': True})])]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q == '*שלום', f"Expected '*שלום', got {q!r}"


def test_wildcard_suffix_hoist():
    """wildcard_suffix mod: term becomes 'שלום*'."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום', mods={'wildcard_suffix': True})])]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q == 'שלום*', f"Expected 'שלום*', got {q!r}"


# ---------------------------------------------------------------------------
# Test 6: Empty model -> build_side_query returns None
# ---------------------------------------------------------------------------

def test_empty_model_returns_none():
    """All-empty words -> build_side_query returns None."""
    bsq = _get_build_side_query()
    lines = [_line([_word(''), _word('   ')])]
    result = bsq(lines, False, None)
    assert result is None, f"Expected None for empty model, got {result!r}"


def test_empty_lines_returns_none():
    """Empty list -> build_side_query returns None."""
    bsq = _get_build_side_query()
    result = bsq([], False, None)
    assert result is None


# ---------------------------------------------------------------------------
# Test 7: Multiple words with gap=0 -> joined with space (no [N] marker)
# ---------------------------------------------------------------------------

def test_two_words_gap_zero():
    """Two words with gap=0 -> joined with a single space (no [N] marker)."""
    bsq = _get_build_side_query()
    lines = [
        _line([
            _word('שלום', gap_to_next_word=0),
            _word('עולם'),
        ])
    ]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert '[' not in q, f"Expected no gap marker for gap=0, got {q!r}"
    assert 'שלום' in q and 'עולם' in q


# ---------------------------------------------------------------------------
# Test 8: Two words, gap 1, second word has negation
# ---------------------------------------------------------------------------

def test_two_words_second_negation():
    """Two words with gap=1; second word is negated: 'ראובן [1] -שמעון'."""
    bsq = _get_build_side_query()
    lines = [
        _line([
            _word('ראובן', gap_to_next_word=1),
            _word('שמעון', mods={'negation': True}),
        ])
    ]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert '[1]' in q, f"Expected [1] word-gap marker, got {q!r}"
    assert '-שמעון' in q, f"Expected negated second word, got {q!r}"


# ---------------------------------------------------------------------------
# Test 9: line_end=True -> trailing | on last token
# ---------------------------------------------------------------------------

def test_line_end_adds_pipe():
    """line_end=True adds | to the last token of the line."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום')], line_end=True)]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q is not None
    tokens = q.split()
    assert tokens[-1].endswith('|'), f"Expected trailing | on last token, got {q!r}"


# ---------------------------------------------------------------------------
# Test 10: Two lines with line_start on first, line_end on second
# ---------------------------------------------------------------------------

def test_two_lines_anchors():
    """Two lines: first has line_start, second has line_end.
    Result: '|שלום [|1] עליכם|'
    """
    bsq = _get_build_side_query()
    lines = [
        _line([_word('שלום')], line_start=True, gap_to_next_line=1),
        _line([_word('עליכם')], line_end=True),
    ]
    side = bsq(lines, False, None)
    assert side is not None
    q, _, _ = compose(side)
    assert q is not None
    assert '[|1]' in q, f"Expected [|1], got {q!r}"
    # First token has leading |
    assert q.startswith('|'), f"Expected leading |, got {q!r}"
    # Last token has trailing |
    tokens = q.split()
    assert tokens[-1].endswith('|'), f"Expected trailing |, got {q!r}"


# ---------------------------------------------------------------------------
# Test 11: page_position propagated correctly
# ---------------------------------------------------------------------------

def test_page_position_start_propagated():
    """page_position='start' is passed through to compose (no ValueError with non-empty row)."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום')])]
    side = bsq(lines, False, 'start')
    assert side is not None
    assert side.page_position == 'start'
    q, _, pp = compose(side)
    assert q is not None
    assert pp == 'start'


# ---------------------------------------------------------------------------
# Test 12: variants=True flows into SideQuery
# ---------------------------------------------------------------------------

def test_variants_propagated():
    """variants=True is correctly set on the returned SideQuery."""
    bsq = _get_build_side_query()
    lines = [_line([_word('שלום')])]
    side = bsq(lines, True, None)
    assert side is not None
    assert side.variants is True
