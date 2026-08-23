# -*- coding: utf-8 -*-
"""The composition snippet marker contract.

PR #325 workflow review: the parallels xlsx export now renders ``*...*`` as
red+bold runs, which turns a literal ``*`` in manuscript text from a cosmetic
oddity into a rendering bug -- one stray marker restyles the rest of the cell.
The chunk/composition builder was the only highlighter in search_engine.py not
sanitizing it.
"""
from shared.search_engine import build_marked_composition_fragment as build


def test_the_matched_span_is_wrapped_in_markers():
    out = build('abcdefghij', 3, 6)
    assert out == 'abc*def*ghij'
    assert out.count('*') == 2


def test_a_literal_asterisk_outside_the_span_is_neutralized():
    """Two markers only: the export splits on '*', so a third would style
    everything after it as matched."""
    out = build('a*b matched tail', 4, 11)
    assert out.count('*') == 2
    assert '*matched*' in out


def test_a_literal_asterisk_inside_the_span_is_neutralized():
    out = build('before mat*ch after', 7, 13)
    assert out.count('*') == 2
    assert out.startswith('before *mat ch*')


def test_asterisks_on_both_sides_are_all_neutralized():
    out = build('*x* hit *y*', 4, 7)
    assert out.count('*') == 2
    assert '*hit*' in out


def test_padding_is_bounded_and_clamped_at_both_ends():
    content = 'z' * 500
    out = build(content, 250, 260, pad=10)
    # 10 left + 10 span + 10 right + 2 markers
    assert len(out) == 32
    edge = build('abc', 0, 3, pad=10)
    assert edge == '*abc*'


def test_empty_content_yields_empty_string():
    assert build('', 0, 0) == ''
    assert build(None, 0, 0) == ''


# =========================================================================
# mark_word_highlights: the chunk-path SOURCE-context builder (round 6).
# Same extraction rationale as the fragment builder above: the only caller
# runs inside a full Tantivy composition search.
# =========================================================================
from shared.search_engine import mark_word_highlights


def test_word_spans_are_wrapped_in_markers():
    assert mark_word_highlights('abc def gh', [(4, 7)]) == 'abc *def* gh'


def test_multiple_spans_keep_their_own_words():
    assert mark_word_highlights('aa bb cc', [(0, 2), (6, 8)]) == '*aa* bb *cc*'


def test_a_literal_asterisk_in_the_pasted_source_is_neutralized():
    """The user's pasted text can carry a footnote '*'; left in place it
    toggles the xlsx rich-text state and styles the rest of the cell."""
    out = mark_word_highlights('x* hit y', [(3, 6)])
    assert out.count('*') == 2
    assert '*hit*' in out


def test_neutralization_preserves_the_span_offsets():
    """.replace('*', ' ') is 1:1 by design -- a literal '*' BEFORE the span
    must not shift which word gets highlighted. A 'fix' that deletes the
    asterisk instead of spacing it would highlight the wrong letters."""
    out = mark_word_highlights('** hit y', [(3, 6)])
    assert '*hit*' in out
    assert out == '   *hit* y'


def test_no_spans_still_neutralizes_literals():
    assert mark_word_highlights('a*b', []) == 'a b'


def test_the_source_context_builder_routes_through_the_helper():
    """No executed test reaches the composition loop without a Tantivy
    index, so pin the call site: the source-context builder must route
    through the tested helper, not a re-inlined marker loop."""
    from pathlib import Path
    engine = (Path(__file__).parent.parent / 'shared' /
              'search_engine.py').read_text(encoding='utf-8')
    assert 'mark_word_highlights(original_snippet, highlights)' in engine
