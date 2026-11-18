"""shared/near_spellings.py: the catalogue search's near spellings (plan §4.7).

Hebrew: one ו or י added or dropped, the Responsa search's %word. Latin: the text search's Fuzzy
edits, from five letters only. Anything else: none.
"""
import itertools

import pytest
from rapidfuzz.distance import OSA

import shared.near_spellings as ns
from shared.responsa import expand_plene_defective


# --- Hebrew: plene and defective ------------------------------------------------------

@pytest.mark.parametrize("word", ["כתובה", "תפילין", "גט", "ירושלימ", "קדיש"])
def test_hebrew_near_spellings_are_responsas_plene_and_defective(word):
    assert ns.near_spellings(word) == frozenset(expand_plene_defective(word))


@pytest.mark.parametrize("word, near", [
    ("כתובה", "כתבה"), ("תפילינ", "תפלינ"), ("ירושלימ", "ירשלימ"), ("גט", "גיט"), ("סעדיה", "סעדה"),
])
def test_hebrew_reaches_its_other_spelling(word, near):
    assert near in ns.near_spellings(word)


@pytest.mark.parametrize("word, far", [
    ("כתובה", "כתיבה"),     # two changes: writing, not a marriage contract
    ("כתובה", "כתב"),       # two letters dropped
    ("כתובה", "הכתובה"),    # a prefix is the prefix search's job, not a near spelling
    ("שלומ", "שלומ".replace("ש", "ס")),  # another letter
])
def test_hebrew_never_changes_more_than_one_vav_or_yod(word, far):
    assert far not in ns.near_spellings(word)


# --- Latin: edits from five letters ----------------------------------------------------

@pytest.mark.parametrize("length, d", [(0, 0), (3, 0), (4, 0), (5, 1), (8, 1), (9, 2), (12, 2), (15, 2),
                                       (16, 0), (1000, 0)])
def test_the_latin_edit_budget_follows_the_length(length, d):
    assert ns.latin_distance("a" * length) == d


def _brute_force(word, d, alphabet):
    out = set()
    for n in range(max(1, len(word) - d), len(word) + d + 1):
        for chars in itertools.product(alphabet, repeat=n):
            s = "".join(chars)
            if OSA.distance(word, s) <= d:
                out.add(s)
    return out


@pytest.mark.parametrize("word", ["abcab", "ccbab", "abcabcabc"])
def test_latin_near_spellings_are_exactly_the_strings_within_the_distance(word, monkeypatch):
    monkeypatch.setattr(ns, "LATIN_LETTERS", "abc")
    expected = _brute_force(word, ns.latin_distance(word), "abc") if len(word) < 9 else None
    got = ns.near_spellings(word)
    if expected is not None:
        assert got == expected
    else:
        # Nine letters, two edits: every string is within two edits, and the near ones are found.
        assert all(OSA.distance(word, w) <= 2 for w in got)
        assert {"abcabcab", "abcabcabcc", "bacabcabc", "abcabcacb", "abcbcab"} <= got


@pytest.mark.parametrize("word, near", [
    ("saadia", "saadya"), ("saadia", "saadiah"), ("fustat", "fostat"),
    ("ketubba", "ketuba"), ("ketubba", "ketubbah"), ("maimonides", "maimonide"),
])
def test_latin_reaches_its_transliterations(word, near):
    assert near in ns.near_spellings(word)


@pytest.mark.parametrize("word", ["wine", "ab", "a1b2", "abג", "مكتوب", "1234"])
def test_short_mixed_or_other_script_words_are_only_themselves(word):
    assert ns.near_spellings(word) == frozenset({word})


def test_a_long_word_costs_nothing():
    # The API takes a 1,000-character query: two edits of it would be millions of strings.
    import time
    word = "a" * 1000
    started = time.perf_counter()
    assert ns.near_spellings(word) == frozenset({word})
    assert time.perf_counter() - started < 0.05
    assert len(ns.near_spellings("abcdefghijklmno")) < 400_000     # the longest that gets any


def test_nothing_is_cached_here():
    assert not hasattr(ns.near_spellings, "cache_info")


def test_the_distance_ranks_one_edit_before_two():
    assert ns.distance("saadia", "saadia") == 0
    assert ns.distance("saadia", "saadya") == 1
    assert ns.distance("saadia", "sadya") == 2
