"""SearchEngine._cross_page_spans: matches across a page break of a whole-manuscript doc.

For a multi-word Literal query an aggregate (system / part) doc contributes only
the matches that cross a page break; the regex runs only in windows around the
breaks that pass a necessary pre-check (first term before the break, last term
after it). Real-index gate (2026-09-30, HEAD vs working engine, uncapped, 10
phrases): every cross-page row of the old path kept, 23 -> 97 cross-page rows
(the old path gave only each manuscript's FIRST match).
"""
import json

import pytest

import shared.search_engine as se
from shared.variants import VariantManager

T1, T2 = "תעודדו", "תענגו"  # תעודדו תענגו
X = "עמוד"                                                        # עמוד


@pytest.fixture(scope="module")
def eng():
    e = se.SearchEngine.__new__(se.SearchEngine)
    e.var_mgr = VariantManager()
    return e


def _doc(pages):
    """Join pages with '\\n' the way _add_continuous_document does; return (content, boundaries)."""
    text, bounds, cur = [], [], 0
    for i, p in enumerate(pages):
        start = cur
        text.append(p)
        cur += len(p)
        if i != len(pages) - 1:
            text.append("\n")
            cur += 1
        bounds.append({"uid": f"p{i + 1}", "start": start, "end": cur})
    return "".join(text), bounds


def _spans(eng, pages, query=f"{T1} {T2}", gap=0, strip=True, window=None):
    content, bounds = _doc(pages)
    terms = query.split()
    rx = eng.build_regex_pattern(terms, "literal", gap)
    window = window or 3 * len(query) + 30 * gap + 64
    got = eng._cross_page_spans(rx, content, bounds, terms[0], terms[-1], window, gap, strip)
    return content, [content[s:e] for s, e in got]


@pytest.mark.parametrize("gap", [0, 1])
def test_a_match_across_a_break_is_found_in_both_search_paths(eng, gap):
    # Three breaks, so gap 0 takes the joined single-pass path.
    pages = [f"{X} {X}", f"{X} {T1}", f"{T2} {X}", f"{X}"]
    _content, texts = _spans(eng, pages, gap=gap)
    assert texts == [f"{T1}\n{T2}"]


def test_a_match_inside_one_page_is_not_returned(eng):
    # The page doc returns it; the aggregate adds only matches across a break.
    _content, texts = _spans(eng, [f"{X} {T1} {T2}", f"{X} {X}", f"{X}"])
    assert texts == []


def test_every_crossing_is_returned_not_only_the_first(eng):
    pages = [f"{X} {T1}", f"{T2} {X}", f"{X} {T1}", f"{T2} {X}"]
    _content, texts = _spans(eng, pages)
    assert texts == [f"{T1}\n{T2}", f"{T1}\n{T2}"]


def test_bracketed_text_maps_back_to_the_exact_original_span(eng):
    # The match starts at the first LETTER, so the opening bracket stays outside the
    # highlight and the inner one inside -- the same span the bracket-free text had.
    pages = [f"{X} [{T1[:2]}]{T1[2:]}", f"{T2}] {X}", f"{X}"]
    content, texts = _spans(eng, pages)
    assert texts == [f"{T1[:2]}]{T1[2:]}\n{T2}"]


def test_the_pre_check_does_not_reject_tolerated_marks_inside_a_word(eng):
    marked = T1[:2] + "̇" + T1[2:]      # a combining dot inside the first word
    _content, texts = _spans(eng, [f"{X} {marked}", f"{T2} {X}", f"{X}"])
    assert texts == [f"{marked}\n{T2}"]


def test_a_match_over_a_one_word_page_is_returned_once(eng):
    # Short middle page: the match straddles two breaks.
    q = f"{T1} {X} {T2}"
    _content, texts = _spans(eng, [f"{X} {T1}", X, f"{T2} {X}"], query=q)
    assert texts == [f"{T1}\n{X}\n{T2}"]


def test_the_pre_check_needs_the_first_term_before_and_the_last_after(eng):
    # Both terms near the break but on the wrong sides: no match can cross it.
    _content, texts = _spans(eng, [f"{X} {T2}", f"{T1} {X}", f"{X}"])
    assert texts == []


def test_no_breaks_no_spans(eng):
    rx = eng.build_regex_pattern([T1, T2], "literal", 0)
    assert eng._cross_page_spans(rx, f"{T1} {T2}", [{"start": 0, "end": 13}], T1, T2, 100, 0, True) == []
