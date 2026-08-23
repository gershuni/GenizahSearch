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
