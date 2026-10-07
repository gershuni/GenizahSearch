# -*- coding: utf-8 -*-
"""Every word of a composition text must be searched by some window, and a
run that searched less than the user asked for must say so.

Both composition engines split the source text into word windows and query
the index once per window:

* the standard engine (SearchEngine.search_composition_logic) slides a window
  of `chunk_size` words with stride 1;
* Lab Mode (LabEngine.lab_composition_search) strides by half a window and
  never queries a window under four words.

These tests call the REAL engine methods with only the index mocked, and
record which words each issued query actually contained, or which documents
survived the result filters.
"""
from __future__ import annotations

import re
from unittest.mock import MagicMock

import pytest

# Distinct Hebrew two-letter words, so a window's words map back to indices.
_LETTERS = [chr(c) for c in range(0x05D0, 0x05EB) if chr(c) not in 'ךםןףץ']
_WORDS = [a + b for a in _LETTERS for b in _LETTERS if a != b][:60]


def _text(n: int) -> str:
    return ' '.join(_WORDS[:n])


def _codes(result):
    return [n.get('code') for n in (result.get('composition_notices') or [])]


def _notice(result, code):
    for n in result.get('composition_notices') or []:
        if n.get('code') == code:
            return n
    return None


# ---------------------------------------------------------------------------
# Engine builders (__init__ bypassed; same shape as tests/test_comp_corpus_scope.py)
# ---------------------------------------------------------------------------

def _standard_engine():
    from genizah_core import SearchEngine

    engine = SearchEngine.__new__(SearchEngine)
    engine.index = MagicMock(name='genizah_index')
    engine.index.parse_query.return_value = MagicMock(name='q')
    hits = MagicMock()
    hits.hits = []
    engine.searcher = MagicMock(name='genizah_searcher')
    engine.searcher.search.return_value = hits
    engine.local_index = None
    engine.local_searcher = None
    engine._my_library_tab_ref = None
    engine._has_content_search = False
    engine.build_tantivy_query = MagicMock(return_value='content:x')
    engine.build_regex_pattern = MagicMock(return_value=re.compile('x'))
    engine._load_browse_map = MagicMock(return_value={})
    return engine


def _standard_windows(engine):
    out = []
    for call in engine.build_tantivy_query.call_args_list:
        out.append([_WORDS.index(w) for w in list(call.args[0])])
    return out


def _lab_engine(monkeypatch, *, weak=False, hit_windows=None, score=300.0):
    """Real LabEngine, index mocked. Identity fingerprint: every word is its
    own fingerprint token, so a query names exactly the words of its window.

    hit_windows=None: no window returns a hit. Otherwise a set of 0-based
    query ordinals that return ONE hit on document UID1 scoring `score`;
    'all' makes every query hit."""
    import genizah_core
    from genizah_core import LabEngine, LabSettings

    monkeypatch.setattr(genizah_core, 'text_to_fingerprint',
                        lambda text, freq_map=None: text)
    engine = LabEngine.__new__(LabEngine)
    engine.settings = LabSettings()
    engine.settings.comp_min_score = 1
    engine.settings.min_should_match = 0 if hit_windows is not None else 50
    engine.dynamic_rank_map = None
    engine._filter_match_count = 0
    engine._is_phrase_statistically_weak = lambda _t: weak
    engine.lab_index = MagicMock(name='lab_index')
    engine.lab_index.parse_query.return_value = MagicMock(name='lab_q')
    one_hit = MagicMock()
    one_hit.hits = [(1.0, 'addr')]
    no_hit = MagicMock()
    no_hit.hits = []
    calls = {'n': 0}

    def search(_q, _limit):
        i = calls['n']
        calls['n'] += 1
        if hit_windows == 'all' or (hit_windows and i in hit_windows):
            return one_hit
        return no_hit

    engine.lab_searcher = MagicMock(name='lab_searcher')
    engine.lab_searcher.search.side_effect = search
    engine.lab_searcher.doc.return_value = {
        'content': ['xx yy'], 'unique_id': ['UID1'],
        'full_header': ['HDR'], 'source': ['V0.8']}
    engine._calculate_match_metrics = (
        lambda content, fp_list, chunk_text, freq_map=None:
        (score, [{'fp': fp_list[0], 'start': 0, 'end': 2, 'word': 'xx'}], (0, 0)))
    engine.local_lab_searcher = None
    engine._local_lab_index = None
    engine.local_lab_searcher_stale = False
    engine._check_local_lab_freshness = MagicMock(return_value=True)
    engine._current_lab_weights_hash = MagicMock(return_value='h')
    engine._my_library_tab_ref = None
    return engine


def _lab_windows(engine):
    """Word-index lists of every window Lab Mode actually QUERIED."""
    out = []
    for call in engine.lab_index.parse_query.call_args_list:
        core = call.args[0].split(') AND (source:')[0]
        out.append([_WORDS.index(w) for w in re.findall(r':([^\s)]+)', core)])
    return out


# ---------------------------------------------------------------------------
# Standard engine
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('chunk_size', [2, 3, 5, 7, 12, 20])
@pytest.mark.parametrize('n', list(range(2, 26)))
def test_standard_engine_searches_every_word(n, chunk_size):
    engine = _standard_engine()
    engine.search_composition_logic(_text(n), chunk_size, float('inf'), 'exact')
    windows = _standard_windows(engine)
    covered = set().union(*map(set, windows)) if windows else set()
    assert covered == set(range(n)), (
        f'{n} words at chunk_size={chunk_size}: words {sorted(set(range(n)) - covered)} '
        f'were never searched')
    assert all(len(w) == min(chunk_size, n) for w in windows)


def test_standard_engine_short_text_is_one_whole_text_window_with_a_notice():
    engine = _standard_engine()
    result = engine.search_composition_logic(_text(3), 5, float('inf'), 'exact')
    assert _standard_windows(engine) == [[0, 1, 2]]
    assert result.get('effective_chunk_size') == 3
    assert result.get('composition_notices') == [{
        'code': 'text_shorter_than_chunk_size',
        'words': 3, 'chunk_size': 5, 'effective_chunk_size': 3,
    }]


def test_standard_engine_full_length_text_is_unchanged():
    engine = _standard_engine()
    result = engine.search_composition_logic(_text(8), 5, float('inf'), 'exact')
    assert result.get('composition_notices') == []
    assert result.get('effective_chunk_size') == 5
    assert _standard_windows(engine) == [list(range(i, i + 5)) for i in range(4)]


@pytest.mark.parametrize('n', [0, 1])
def test_standard_engine_under_two_words_is_refused_explicitly(n):
    engine = _standard_engine()
    text = _text(n) if n else '... ,'
    result = engine.search_composition_logic(text, 5, float('inf'), 'exact')
    assert _standard_windows(engine) == []
    assert result['main'] == [] and result['filtered'] == []
    assert _notice(result, 'text_too_short') == {
        'code': 'text_too_short', 'words': n, 'minimum': 2}


@pytest.mark.parametrize('chunk_size', [0, 1])
def test_standard_engine_reports_a_chunk_size_below_two(chunk_size):
    # The desktop spin box has no lower bound today; a 0 or 1 is raised to 2
    # and reported, never used silently.
    engine = _standard_engine()
    result = engine.search_composition_logic(_text(6), chunk_size, float('inf'), 'exact')
    assert _notice(result, 'chunk_size_raised') == {
        'code': 'chunk_size_raised', 'chunk_size': chunk_size, 'effective_chunk_size': 2}
    assert all(len(w) == 2 for w in _standard_windows(engine))


def test_standard_engine_lowers_an_unreachable_min_chunk_matches():
    # 3 words -> one window; "at least 3 matching chunks" can never be met.
    engine = _standard_engine()
    result = engine.search_composition_logic(
        _text(3), 5, float('inf'), 'exact', min_boundary_matches=3)
    assert _notice(result, 'min_chunk_matches_lowered') == {
        'code': 'min_chunk_matches_lowered', 'min_chunk_matches': 3, 'windows': 1}


# ---------------------------------------------------------------------------
# Lab Mode: coverage
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('chunk_size', [None, 4, 5, 6, 8, 10, 12, 15])
@pytest.mark.parametrize('n', list(range(4, 41)))
def test_lab_engine_searches_every_word(monkeypatch, n, chunk_size):
    engine = _lab_engine(monkeypatch)
    engine.lab_composition_search(_text(n), mode='variants', chunk_size=chunk_size)
    windows = _lab_windows(engine)
    covered = set().union(*map(set, windows)) if windows else set()
    assert covered == set(range(n)), (
        f'Lab Mode, {n} words at chunk_size={chunk_size}: words '
        f'{sorted(set(range(n)) - covered)} were never searched')


def test_lab_engine_tail_window_is_end_anchored(monkeypatch):
    # 12 words at chunk_size 10, step 5: at HEAD the only window is 0-9.
    engine = _lab_engine(monkeypatch)
    engine.lab_composition_search(_text(12), mode='variants', chunk_size=10)
    assert _lab_windows(engine) == [list(range(0, 10)), list(range(2, 12))]


@pytest.mark.parametrize('chunk_size', [2, 3])
def test_lab_engine_small_chunk_size_still_searches(monkeypatch, chunk_size):
    engine = _lab_engine(monkeypatch)
    result = engine.lab_composition_search(_text(10), mode='variants', chunk_size=chunk_size)
    windows = _lab_windows(engine)
    assert windows, 'Lab Mode issued no query at all'
    assert set().union(*map(set, windows)) == set(range(10))
    assert all(len(w) == 4 for w in windows)
    assert _notice(result, 'chunk_size_raised') == {
        'code': 'chunk_size_raised', 'chunk_size': chunk_size, 'effective_chunk_size': 4}


def test_lab_engine_three_words_says_why_nothing_was_searched(monkeypatch):
    engine = _lab_engine(monkeypatch)
    result = engine.lab_composition_search(_text(3), mode='variants', chunk_size=5)
    assert _lab_windows(engine) == []
    assert _notice(result, 'text_too_short') == {
        'code': 'text_too_short', 'words': 3, 'minimum': 4}


def test_lab_engine_weak_only_window_is_not_reported_as_searched(monkeypatch):
    # The one whole-text window is skipped as statistically weak before any
    # query, so the result must not say "searched as one chunk".
    engine = _lab_engine(monkeypatch, weak=True)
    result = engine.lab_composition_search(_text(4), mode='variants', chunk_size=5)
    assert _lab_windows(engine) == []
    assert _codes(result) == ['text_too_common']


# ---------------------------------------------------------------------------
# Lab Mode: results must not change for texts HEAD already searched fully
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('chunk_size, n', [(5, 10), (4, 9), (None, 30), (10, 24)])
def test_lab_tail_window_keeps_the_short_search_rule(monkeypatch, chunk_size, n):
    # These lengths have exactly 3 stride windows, so Lab applies its
    # short-search rule (keep a document scoring >= 250). The extra tail
    # window makes 4 queries; it must not switch the run to the long-search
    # rule (drop a single-hit document scoring < 1000) and lose this match.
    engine = _lab_engine(monkeypatch, hit_windows={0}, score=300.0)
    result = engine.lab_composition_search(_text(n), mode='variants', chunk_size=chunk_size)
    assert len(result['main']) == 1, 'a document the run returned before the fix was dropped'


def test_lab_min_chunk_matches_is_lowered_to_what_the_text_can_give(monkeypatch):
    # The web and desktop set Min. chunk matches to 3 when Lab Mode is turned
    # on. A 6-word text at chunk size 5 has 2 windows; every window matches.
    engine = _lab_engine(monkeypatch, hit_windows='all', score=300.0)
    result = engine.lab_composition_search(
        _text(6), mode='variants', chunk_size=5, min_boundary_matches=3)
    assert len(result['main']) == 1
    assert _notice(result, 'min_chunk_matches_lowered') == {
        'code': 'min_chunk_matches_lowered', 'min_chunk_matches': 3, 'windows': 2}


# ---------------------------------------------------------------------------
# The cap counts DISTINCT chunk texts (Codex design review K-12): a result's
# chunk count counts distinct chunk texts, so a text that repeats a phrase
# offers fewer chunks to match than it has windows.
# ---------------------------------------------------------------------------

def test_standard_min_chunk_cap_counts_distinct_chunk_texts():
    # w0 w1 w2 w0 w1 w2 at chunk 3: four windows, three different texts.
    engine = _standard_engine()
    text = ' '.join(_WORDS[i] for i in (0, 1, 2, 0, 1, 2))
    result = engine.search_composition_logic(text, 3, float('inf'), 'exact', min_boundary_matches=4)
    assert _notice(result, 'min_chunk_matches_lowered') == {
        'code': 'min_chunk_matches_lowered', 'min_chunk_matches': 4, 'windows': 3}


def test_lab_min_chunk_cap_counts_distinct_chunk_texts(monkeypatch):
    # The same four words twice at chunk 4: Lab searches windows 0, 2 and 4,
    # and windows 0 and 4 hold the same words. A document matching every window
    # has 2 distinct chunks, so "at least 3" must be lowered to 2, not kept.
    engine = _lab_engine(monkeypatch, hit_windows='all', score=300.0)
    text = ' '.join(_WORDS[i] for i in (0, 1, 2, 3, 0, 1, 2, 3))
    result = engine.lab_composition_search(text, mode='variants', chunk_size=4, min_boundary_matches=3)
    assert _notice(result, 'min_chunk_matches_lowered') == {
        'code': 'min_chunk_matches_lowered', 'min_chunk_matches': 3, 'windows': 2}
    assert len(result['main']) == 1


def test_eval_chunk_retriever_counts_composition_notices():
    # Codex design review K-13: a query shorter than chunk_size runs as one
    # whole-text window; the evaluation must be able to say how many did AND
    # at which size each ran -- a two-word and a three-word query at chunk 5
    # are two different searches. The notices are the real ones the engine
    # returns (plan_windows), not hand-written dicts.
    import importlib.util
    import json
    from pathlib import Path

    from shared.composition_windows import plan_windows
    from shared.retrieval_adapters import ChunkRetriever

    class _Engine:
        def search_composition_logic(self, text, chunk_size, max_freq, mode):
            plan = plan_windows(len(text.split()), chunk_size)
            return {'main': [], 'composition_notices': list(plan.notices)}

    retriever = ChunkRetriever(engine=_Engine(), chunk_size=5)
    for text in ('a b', 'a b c d e f', 'a b c'):
        retriever.retrieve(text)
    assert retriever.notice_counts == {'text_shorter_than_chunk_size': 2}
    assert retriever.effective_size_counts == {'text_shorter_than_chunk_size': {2: 1, 3: 1}}
    assert retriever.config_id == ChunkRetriever(engine=None, chunk_size=5).config_id

    # The evaluation script's summary and printout keep the breakdown, and
    # the summary still goes into the ledger (json, sort_keys).
    spec = importlib.util.spec_from_file_location(
        '_eval_methods', Path(__file__).resolve().parents[1] / 'scripts' / 'eval_methods.py')
    eval_methods = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(eval_methods)
    summary, lines = eval_methods.composition_notice_report(retriever)
    assert summary == {
        'composition_notices': {'text_shorter_than_chunk_size': 2},
        'composition_notice_effective_sizes': {'text_shorter_than_chunk_size': {2: 1, 3: 1}},
    }
    assert any('{2: 1, 3: 1}' in line for line in lines), lines
    json.dumps(summary, sort_keys=True)
    assert eval_methods.composition_notice_report(object()) == ({}, [])
