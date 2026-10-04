"""execute_search builds each hit's snippets from the match it already has.

It used to re-run the full regex twice more per hit (once per snippet), which was
~10 s of a 50,000-candidate desktop search. These tests pin that the shortcut is
output-identical to the old per-snippet re-search, and that progress ticks (the
desktop worker's pause/cancel checkpoint + a cross-thread Qt signal) stay bounded.
"""

import re

from shared import search_engine as se
from shared.search_engine import SearchEngine


def _engine():
    return SearchEngine.__new__(SearchEngine)


TEXTS = [
    "שלום עליכם\nוברכה שלום רב",
    "x" * 200 + " שלום " + "y\n" * 50,
    "שלום",
    "a * b שלום * c\nd",
]


def test_highlight_pair_matches_old_highlight_calls():
    eng = _engine()
    regex = re.compile("שלום")
    for text in TEXTS:
        span = regex.search(text).span()
        assert eng._highlight_pair(text, span) == (
            eng.highlight(text, regex, False),
            eng.highlight(text, regex, True),
        )
        assert eng._highlight_pair(text, span) == (
            eng._highlight_by_span(text, span, False),
            eng._highlight_by_span(text, span, True),
        )


def test_highlight_pair_empty_span_is_none():
    assert _engine()._highlight_pair("abc", None) == (None, None)


def test_progress_tick_interval_is_bounded():
    # 50,000 candidates must not mean 10,000 cross-thread signals again, and a
    # cancel must still be seen within a few hundred hits.
    assert 50 <= se._PROGRESS_TICK_EVERY <= 1000
