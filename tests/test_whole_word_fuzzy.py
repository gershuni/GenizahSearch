"""Fuzzy = near spellings, whole words (owner 2026-10-01; prefixes 2026-10-02).

A near spelling is a whole word within a few edits of the query word -- a letter added,
dropped or replaced, or two neighbours swapped (OSA, as Tantivy's fuzzy query counts):
none below 3 letters, one for 3-4, two from 5. "It's not bad that we include ושלום in
fuzzy. It's fuzzy." -- a prefix letter is an edit like any other; ושלומות, three away,
is not a match, and neither is a near spelling found inside a longer word.

Before this, Fuzzy retrieved only the exact word (its "term"~1 is a phrase slop, not an
edit distance) and verified it with the 8,000-form Variants alternation as a substring.

Unit tests of the pieces, then end to end through ``SearchEngine.execute_search`` on a
tiny main-schema index.
"""
import gc
import itertools
import json
import os
import re
import time
from unittest.mock import patch

import pytest
from rapidfuzz.distance import OSA

tantivy = pytest.importorskip("tantivy")

from shared.config import Config  # noqa: E402
from shared.indexer import Indexer  # noqa: E402
from shared.search_regex import SearchBudgetExceeded, search_budget  # noqa: E402
from shared.search_tokenizer import register_search_tokenizers  # noqa: E402
from shared.text_normalize import strip_search_diacritics  # noqa: E402
from shared.variants import VariantManager  # noqa: E402
import shared.search_engine as se  # noqa: E402

W = "שלום"            # 4 letters: one edit
NEAR = "שלוס"         # one substitution
SWAP = "שולם"         # two neighbours swapped: one edit under OSA, two under Levenshtein
PREFIXED = "ושלום"    # one added letter
FAR = "ושלומות"       # three edits
ON = "עליכם"          # 5 letters: two edits
MARK = "̇"       # a combining dot (Judeo-Arabic)


# --- the near spellings -----------------------------------------------------------

@pytest.mark.parametrize("term, distance", [("על", 0), ("של", 0), ("שלו", 1), (W, 1), (ON, 2), ("ירושלים", 2)])
def test_the_edit_budget_follows_the_length_of_the_word(term, distance):
    assert se._fuzzy_distance(term) == distance


def _brute_force(term, d, alphabet):
    out = set()
    for n in range(max(1, len(term) - d), len(term) + d + 1):
        for chars in itertools.product(alphabet, repeat=n):
            s = "".join(chars)
            if OSA.distance(term, s) <= d:
                out.add(s)
    return out


@pytest.mark.parametrize("term", ["אבג", "אבגא", "אבגאב", "גגבאב"])
def test_near_spellings_are_exactly_the_strings_within_the_distance(term, monkeypatch):
    # A three-letter alphabet makes the brute force small, two edits included.
    monkeypatch.setattr(se, "_HEB_LETTERS", "אבג")
    se._near_spellings.cache_clear()
    try:
        assert se._near_spellings(term) == _brute_force(term, se._fuzzy_distance(term), "אבג")
    finally:
        se._near_spellings.cache_clear()


def test_a_swap_of_two_neighbours_is_one_edit():
    assert SWAP in se._near_spellings(W)
    assert PREFIXED in se._near_spellings(W) and NEAR in se._near_spellings(W)
    assert FAR not in se._near_spellings(W)


@pytest.mark.parametrize("term", ["AND", "[שלום", "שלום2", "abc"])
def test_a_word_that_is_not_plain_hebrew_letters_is_matched_as_typed(term):
    assert se._near_spellings(term) is None
    assert se._near_spelling_tiers(term) == (frozenset({term}),)


def test_tiers_split_the_near_spellings_by_distance():
    tiers = se._near_spelling_tiers(ON)
    assert tiers[0] == {ON} and frozenset().union(*tiers) == se._near_spellings(ON)
    assert all(OSA.distance(ON, f) == d for d, tier in enumerate(tiers) for f in tier)


def test_every_char_the_word_regex_joins_is_in_the_quick_lookup_set():
    joined = {chr(c) for c in range(0x110000) if se._FUZZY_FOLD_RE.match(chr(c))}
    assert joined == set(se._FUZZY_HELD_CHARS)


# --- the matcher ------------------------------------------------------------------

def _found(query, text, gap=0):
    m = se._fuzzy_matcher(query.split(), gap).search(text)
    return None if m is None else text[m.start():m.end()]


@pytest.mark.parametrize("text, found", [
    (f"אבג {W} דהו", W),
    (f"אבג {NEAR} דהו", NEAR),
    (f"אבג {SWAP} דהו", SWAP),
    (f"אבג {PREFIXED} דהו", PREFIXED),          # the owner's ruling: it's fuzzy
    (f"אבג {FAR} דהו", None),                   # three edits
    (f"אבג ו{FAR} דהו", None),
    (f"אבג {W}ים דהו", None),                   # שלוםים: two edits, one allowed
    (f"אבג ש[לום דהו", "ש[לום"),               # a lacuna inside the word
    (f"אבג ש{MARK}לוס דהו", f"ש{MARK}לוס"),     # a mark inside a near spelling
    (f'אבג ש"לום דהו', 'ש"לום'),               # a quote inside
    (f"אבג [{NEAR}] דהו", NEAR),                # brackets at the edges are not the word
    (f"אבג {W}־טוב", W),                   # maqaf separates words (as for Exact)
    (f"אבג שָלוֹם דהו", None),                  # nikud: not joined across (as Exact, Variants)
    (f"אבג {W}ְים דהו", None),                  # ...but a word it sits in goes on past it
    ("אבג", None),
])
def test_a_single_word_is_a_whole_near_spelling(text, found):
    assert _found(W, text) == found


def test_occurrences_come_in_text_order_from_pos():
    text = f"שלם אבג {W} דהו"
    m = se._fuzzy_matcher([W], 0)
    first = m.search(text)
    assert text[first.start():first.end()] == "שלם"
    second = m.search(text, first.start() + 1)
    assert text[second.start():second.end()] == W
    assert m.search(text, second.start() + 1) is None
    # A pos inside a word does not make its tail a word.
    assert m.search(f"ו{W}", 1) is None


def test_endpos_bounds_the_search():
    text = f"אבג {W} דהו"
    assert se._fuzzy_matcher([W], 0).search(text, 0, text.index(W) + 2) is None


@pytest.mark.parametrize("text, gap, found", [
    (f"{W} {ON}", 0, f"{W} {ON}"),
    (f"{NEAR} עליכמ", 0, f"{NEAR} עליכמ"),
    (f"{PREFIXED} לכם", 0, f"{PREFIXED} לכם"),     # עליכם -> לכם: two edits
    (f"{W} אבג {ON}", 0, None),
    (f"{W} אבג {ON}", 1, f"{W} אבג {ON}"),
    (f"{W}־{ON}", 0, None),                   # maqaf is no separator for a phrase
    (f"{W} [{ON}", 0, f"{W} [{ON}"),               # a lacuna bracket is
    (f"{W} {ON}{ON}", 0, None),
    (f"ו{FAR} {ON}", 0, None),
    (f"{W} {ON}ים", 0, f"{W} {ON}ים"),            # עליכםים: two edits from עליכם
    (f"{W} {ON}ימים", 0, None),
])
def test_a_phrase_is_near_spellings_in_order(text, gap, found):
    assert _found(f"{W} {ON}", text, gap) == found


def test_with_a_gap_a_later_word_is_tried_when_the_further_one_is_not_a_near_spelling():
    # The gap is greedy (one word between first), as the regex's {0,gap} is: here the
    # word after one gap word is not a near spelling, the adjacent one is.
    assert _found(f"{W} {ON}", f"{W} {ON} אבגדהוזח", 1) == f"{W} {ON}"
    # ...and a phrase whose last word continues into a longer word is passed over.
    assert _found(f"{W} {ON}", f"{W} עליכמ{ON}", 0) is None
    # Both fit: the greedy gap's span, as Exact's and Variants' regex report it.
    assert _found(f"{W} {ON}", f"{W} {ON} {ON}", 1) == f"{W} {ON} {ON}"
    # The greedy choice's last word goes on past a nikud sign (not a whole word):
    # the adjacent choice is taken instead of giving up.
    assert _found(f"{W} {ON}", f"{W} {ON} {ON}ְא", 1) == f"{W} {ON}"


def test_a_word_not_of_hebrew_letters_is_matched_as_typed_in_a_phrase():
    assert _found(f"{W} AB", f"{NEAR} ab") == f"{NEAR} ab"     # IGNORECASE, as Exact
    assert _found(f"{W} AB", f"{NEAR} abc") is not None        # Latin neighbours are lenient (as Exact)


def test_the_closest_spelling_on_a_page_is_preferred():
    text = f"לכם אבג עליכמ דהו {ON} זחט עליכן"     # two edits, one, none, one
    m = se._fuzzy_matcher([ON], 0)
    first = m.search(text)
    assert text[first.start():first.end()] == "לכם"
    best = m.closest(text, first, lambda _m: True)
    assert text[best.start():best.end()] == ON
    # The accept test still decides: without the word itself, the first one-edit word.
    best = m.closest(text, first, lambda x: text[x.start():x.end()] != ON)
    assert text[best.start():best.end()] == "עליכמ"


def test_a_rows_pattern_marks_the_near_spellings_on_its_page():
    m = se._fuzzy_matcher([W], 0)
    text = f"אבג {NEAR} דהו ש[לום זחט {FAR}"
    rx = re.compile(m.pattern_for(text))
    assert {text[x.start():x.end()] for x in rx.finditer(text)} >= {NEAR, "ש[לום"}
    assert rx.search(f"אבג {ON}") is None
    # No near spelling: the words as typed.
    assert m.pattern_for("אבג דהו") == m.pattern
    assert re.compile(m.pattern).search(W)


def test_a_search_obeys_the_api_time_budget():
    # Compiled patterns check the deadline on every call; the walk must too. (Leaving
    # the budget raises as well, so the raise is caught inside it.)
    m = se._fuzzy_matcher([W], 0)
    raised_inside = False
    try:
        with search_budget(0.01):
            time.sleep(0.1)                     # Windows' monotonic clock ticks every 15 ms
            try:
                m.search(f"אבג {W}")
            except SearchBudgetExceeded:
                raised_inside = True
    except SearchBudgetExceeded:
        pass
    assert raised_inside


# --- end to end -------------------------------------------------------------------

PAGES = {
    "tier2": f"אבג לכם דהו",                 # two edits from עליכם (indexed first)
    "tier1": f"אבג עליכמ דהו",               # one edit
    "exact": f"אבג {W} דהו",
    "near": f"אבג {NEAR} דהו",
    "near_br": f"אבג {NEAR}[ דהו",          # token שלוס[
    "near_mark": f"אבג ש{MARK}לוס דהו",      # found through content_search
    "prefixed": f"אבג {PREFIXED} דהו",
    "far": f"אבג {FAR} דהו",
    "both": f"שלם אבג דהו {W} זחט",          # an earlier one-edit word, then the word itself
    "start": f"{NEAR} אבג דהו",
}
CROSS = [("c1", f"אבג {NEAR}"), ("c2", f"עליכמ דהו")]      # שלוס | עליכמ across a break


def _schema():
    b = tantivy.SchemaBuilder()
    b.add_text_field("unique_id", stored=True)
    b.add_text_field("content", stored=True, tokenizer_name="hebword")
    for f in ("content_head", "content_tail", "line_starts", "line_ends"):
        b.add_text_field(f, stored=False, tokenizer_name="whitespace")
    b.add_text_field("content_search", stored=False, tokenizer_name="hebword")
    for f in ("source", "full_header", "shelfmark", "scope", "boundaries"):
        b.add_text_field(f, stored=True)
    return b.build()


class _Meta:
    def get_display_data(self, header, source):
        return {"shelfmark": header, "title": "", "img": "1", "source": source,
                "id": header, "library_code": ""}

    def parse_full_id_components(self, header):
        return {"sys_id": None, "ie_id": None, "p_num": None, "fl_id": None}


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    root = tmp_path_factory.mktemp("fuzzy")
    db = os.path.join(str(root), "tantivy_db")
    os.makedirs(db)
    idx = tantivy.Index(_schema(), path=db)
    register_search_tokenizers(idx)
    w = idx.writer(heap_size=50_000_000, num_threads=1)

    def add(uid, text, scope, bounds=""):
        w.add_document(tantivy.Document(
            unique_id=uid, content=text, content_search=strip_search_diacritics(text),
            source="V0.8", full_header=uid, shelfmark=uid, scope=scope, boundaries=bounds,
            **Indexer._extract_position_fields(text)))

    for uid, text in list(PAGES.items()) + CROSS:
        add(uid, text, "page")
    text = CROSS[0][1] + "\n" + CROSS[1][1]
    cut = len(CROSS[0][1]) + 1
    add("sys:c", text, "system", json.dumps([
        {"uid": "c1", "p_num": 1, "full_header": "c1", "source": "V0.8", "sys_id": "99", "start": 0, "end": cut},
        {"uid": "c2", "p_num": 2, "full_header": "c2", "source": "V0.8", "sys_id": "99", "start": cut,
         "end": len(text)}], ensure_ascii=False))
    w.commit()
    w.wait_merging_threads()
    w = idx = None
    with patch.object(Config, "INDEX_DIR", str(root)):
        eng = se.SearchEngine(_Meta(), VariantManager(), worker_mode=True, open_local=False)
    yield eng
    eng = None
    gc.collect()


def _rows(engine, query, **kw):
    return {r["uid"]: r for r in engine.execute_search(query, "fuzzy", 0, corpus_scope="genizah", **kw)}


def test_near_spellings_are_found_and_others_are_not(engine):
    rows = _rows(engine, W)
    assert {"exact", "near", "near_br", "near_mark", "prefixed", "both", "start"} <= set(rows)
    assert "far" not in rows


def test_a_row_marks_its_own_near_spelling_and_highlights_with_its_own_pattern(engine):
    row = _rows(engine, W)["near"]
    assert f"*{NEAR}*" in row["raw_file_hl"]
    # The pattern a viewer re-marks the page with finds this page's spelling; the
    # words-as-typed pattern every row used to carry would mark nothing here.
    assert re.search(row["highlight_pattern"], row["full_text"]).group() == NEAR


def test_a_row_shows_the_word_itself_over_an_earlier_near_spelling(engine):
    assert f"*{W}*" in _rows(engine, W)["both"]["raw_file_hl"]


def test_one_edit_ranks_above_two(engine):
    # One result allowed: retrieval's distance scores decide which page it is. (The
    # word itself already ranks first by its own clause's BM25.)
    with patch.object(Config, "SEARCH_LIMIT", 1):
        assert set(_rows(engine, ON)) <= {"tier1", "c2"}
        assert set(_rows(engine, W)) <= {"exact", "both"}


def test_a_phrase_across_a_page_break_is_found(engine):
    rows = _rows(engine, f"{W} {ON}")
    assert "c1" in rows and rows["c1"].get("cross_page")
    assert re.search(rows["c1"]["highlight_pattern"], rows["c1"]["full_text"])


def test_a_position_search_retrieves_near_spellings_at_the_position(engine):
    rows = _rows(engine, W, text_position="start")
    assert "start" in rows and "near" not in rows


def test_an_index_without_the_folded_field_still_finds_near_spellings(engine):
    real = engine._has_content_search
    engine._has_content_search = False
    try:
        rows = _rows(engine, W)
        assert {"near", "near_br", "prefixed"} <= set(rows)
    finally:
        engine._has_content_search = real


def test_fuzzy_computes_no_variant_forms(engine, monkeypatch):
    calls = []
    monkeypatch.setattr(engine, "_get_or_compute_variants", lambda *a: calls.append(a))
    _rows(engine, W)
    assert calls == []


def test_the_my_library_search_verifies_with_near_spellings(engine, monkeypatch):
    got = {}

    def fake(query_str, mode, gap, regex=None, **kw):
        got["regex"] = regex
        return []

    monkeypatch.setattr(engine, "_query_local_index", fake)
    monkeypatch.setattr(engine, "local_searcher", object(), raising=False)
    engine.execute_search(W, "fuzzy", 0, corpus_scope="local")
    assert isinstance(got["regex"], se._FuzzyMatcher)


class _Doc:
    def __init__(self, **fields):
        self._f = fields

    def get_first(self, name):
        return self._f.get(name)


def test_a_my_library_row_highlights_with_its_own_pattern(engine):
    m = se._fuzzy_matcher([W], 0)
    row = engine._build_local_result_dict(
        _Doc(unique_id="u", full_header="99_LOCAL_P1_F1", content=f"אבג {NEAR} דהו"), 1.0,
        regex=m, pattern_str=m.pattern)
    assert re.search(row["highlight_pattern"], row["full_text"]).group() == NEAR


def test_composition_keeps_its_own_fuzzy_regex(engine):
    # Composition calls build_regex_pattern directly; its Fuzzy branch is unchanged.
    assert not isinstance(engine.build_regex_pattern([W], "fuzzy", 0), se._FuzzyMatcher)
