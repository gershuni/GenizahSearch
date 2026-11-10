"""SearchEngine._cross_page_spans: matches across a page break of a whole-manuscript doc.

For a multi-word Literal query an aggregate (system / part) doc contributes only
the matches that cross a page break; the regex runs only in windows around the
breaks that pass a necessary pre-check (first term before the break, last term
after it). Real-index gate (2026-09-30, HEAD vs working engine, uncapped, 10
phrases): every cross-page row of the old path kept, 23 -> 97 cross-page rows
(the old path gave only each manuscript's FIRST match).

The window is counted in words, not characters (2026-10-01): a fixed character
window lost a crossing after a long run of dots or lacuna brackets at a page edge.
"""
import random

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


def _side_words(terms, gap):
    # As execute_search computes it.
    return (len(terms) - 1) * (gap + 1)


def _spans(eng, pages, query=f"{T1} {T2}", gap=0, strip=True, side_words=None):
    content, bounds = _doc(pages)
    terms = query.split()
    rx = eng.build_regex_pattern(terms, "literal", gap)
    side_words = side_words or _side_words(terms, gap)
    got = eng._cross_page_spans(rx, content, bounds, terms[0], terms[-1], side_words, gap, strip)
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


@pytest.mark.parametrize("gap", [0, 1])
def test_a_long_non_word_run_at_a_page_end_does_not_hide_a_crossing(eng, gap):
    # Codex review 2026-10-01: 100 dots after the first word put it outside the
    # old 79-character window. 333 V0.8 pages end with a non-word run over 60.
    pages = [f"{X} {T1}" + "." * 100, f"{T2} {X}", f"{X}"]
    _content, texts = _spans(eng, pages, gap=gap)
    assert texts == [T1 + "." * 100 + "\n" + T2]


@pytest.mark.parametrize("dots", [57, 58, 59, 60, 61, 62])
def test_a_probe_edge_inside_the_first_word_does_not_hide_a_crossing(eng, dots):
    # The first 64-character probe before the break ends inside T1: that partial
    # chunk must not be taken as the window start.
    pages = [f"{X} {T1}" + "." * dots, f"{T2} {X}", f"{X}"]
    _content, texts = _spans(eng, pages)
    assert texts == [T1 + "." * dots + "\n" + T2]


def test_lacuna_lines_at_a_page_start_do_not_hide_a_crossing(eng):
    lacuna = "[\n" * 40
    pages = [f"{X} {T1}", lacuna + f"{T2} {X}", f"{X}"]
    _content, texts = _spans(eng, pages, strip=True)
    assert texts == [T1 + "\n" + lacuna + T2]


def test_gap_words_of_any_length_stay_inside_the_window(eng):
    long_word = "ש" * 300
    pages = [f"{X} {T1} {long_word}", f"{T2} {X}", f"{X}"]
    _content, texts = _spans(eng, pages, gap=1)
    assert texts == [f"{T1} {long_word}\n{T2}"]


def test_window_edges_never_fall_inside_a_long_chunk():
    # A chunk longer than the first 256-character probe: the edge is the chunk's
    # real start / end, not the probe's.
    long_chunk = "ב" * 300
    left = f"{X} {long_chunk}\n"
    assert se._left_window_start(left, len(left), 1) == len(X) + 1
    right = f"{long_chunk} {X}"
    assert se._right_window_end(right, 0, 1) == len(long_chunk)


def _ref_chunks(text):
    """Plain reference: (start, end) of every whitespace chunk holding a word char."""
    import re
    return [(m.start(), m.end()) for m in re.finditer(r"\S+", text)
            if re.search(r"[\w֐-׿']", m.group())]


def test_window_edges_match_a_plain_reference():
    rnd = random.Random(7)
    pieces = [X, T1, "ש" * 300, "[", "]", "....", "." * 400, "־", "'", "[א", "1"]
    inside = 0
    for _ in range(8000):
        text = "".join(rnd.choice(pieces) + rnd.choice([" ", "\n", "  ", ""]) for _w in range(rnd.randint(0, 12)))
        cut = rnd.randint(0, len(text))
        words = rnd.randint(1, 4)
        left = [c for c in _ref_chunks(text[:cut])]
        want_lo = left[-words][0] if len(left) >= words else 0
        right = _ref_chunks(text[cut:])
        want_hi = cut + right[words - 1][1] if len(right) >= words else len(text)
        # The reference chunks text[:cut] / text[cut:] on their own, so a chunk
        # running across the cut counts on both sides; the engine only cuts at a
        # page break, where text[cut - 1] is a newline.
        if 0 < cut < len(text) and not text[cut - 1].isspace() and not text[cut].isspace():
            continue
        assert se._left_window_start(text, cut, words) == want_lo, (text, cut, words)
        assert se._right_window_end(text, cut, words) == want_hi, (text, cut, words)
        inside += 0 < want_lo < cut or cut < want_hi < len(text)
    assert inside > 400  # edges strictly inside the text, not only at its ends


def test_the_word_window_finds_exactly_what_the_whole_text_finds(eng):
    """Randomised: the window result equals the same function over the whole text."""
    rnd = random.Random(20261001)
    vocab = [T1, T2, X, "[", "]", "[ ]", "....", "." * 90, "[" * 3, T1 + T2, "ו" + T1, "־", "'"]
    seps = [" ", "\n", "  ", " \n "]
    checked = 0
    for _ in range(400):
        pages = []
        for _p in range(rnd.randint(2, 6)):
            words = [rnd.choice(vocab) for _w in range(rnd.randint(1, 7))]
            pages.append("".join(w + rnd.choice(seps) for w in words).strip() or X)
        terms = rnd.choice([[T1, T2], [T1, X, T2], [X, T2], [T1, X]])
        gap = rnd.choice([0, 0, 1, 2])
        strip = rnd.random() < 0.8
        content, bounds = _doc(pages)
        rx = eng.build_regex_pattern(terms, "literal", gap)
        whole = eng._cross_page_spans(rx, content, bounds, terms[0], terms[-1], len(content) + 1, gap, strip)
        windowed = eng._cross_page_spans(rx, content, bounds, terms[0], terms[-1],
                                         _side_words(terms, gap), gap, strip)
        assert windowed == whole, (pages, terms, gap, strip)
        checked += bool(whole)
    assert checked > 40  # the sample really contains crossings


