"""Patterns the two regex engines read differently must not lose matches.

The desktop and the web process match with the ``regex`` module; the candidate
query is built from stdlib ``re``'s parse. ``x{e<=1}`` is a fuzzy constraint in
``regex`` and literal text in ``re``; ``[[:alpha:]]`` is a POSIX class in
``regex`` and a nested-set warning in ``re``. Such patterns must get no
prefilter (every document is a candidate), so the matcher decides.

Drives the real entry point, ``SearchEngine.execute_search(..., 'Regex')``, on
the main index and on My Library.
"""
import re
import warnings

import pytest

from shared.indexer import build_main_schema
from shared.local_indexer import build_local_schema
from shared.search_regex import compile as compile_search_regex

from test_regex_mode_lossless import _engine, _index

DOCS = {
    "fuzzy_sub": "ברכה שלוס עליכם",     # one substitution from שלום
    "fuzzy_del": "ברכה שלו עליכם",      # one deletion
    "exact": "ברכה שלום עליכם",
    "yerushalayim": "בירושלים עיר",
    "other": "טקסט אחר לגמרי",
}

DIVERGENT = [
    "(?:שלום){e<=1}",
    "שלום{e<=1}",
    "ש[[:alpha:]]ום",
    "ירו[[:alpha:]]לים",
    # A POSIX class later in the set: re gives no warning, and its parse needs ']ום'.
    "ש[a[:alpha:]]ום",
    "ש[^[:digit:]]ום",
]


def _expected(pattern):
    # What the in-process matcher accepts (the regex module), on the stored text.
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        rx = compile_search_regex(pattern, re.IGNORECASE)
    return {uid for uid, text in DOCS.items() if rx.search(text)}


@pytest.mark.parametrize("pattern", DIVERGENT)
@pytest.mark.parametrize("scope", ["genizah", "local"])
def test_engine_divergent_pattern_keeps_matches(pattern, scope):
    if scope == "genizah":
        eng = _engine(main=_index(DOCS, build_main_schema()))
    else:
        eng = _engine(local=_index(DOCS, build_local_schema(), local=True))
    expected = _expected(pattern)
    assert len(expected) >= 1
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        found = {r["uid"] for r in eng.execute_search(pattern, "Regex", 0, corpus_scope=scope)}
    assert found == expected
