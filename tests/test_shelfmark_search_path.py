"""The Shelfmark search path: indexed catalogue, same answers, no worker for a lookup.

A public-API ``mode=shelfmark`` search took 10 s or more on most calls (PostHog,
2026-11-03: 262 of 437 requests in 14 days, 56 timed out), and the website's
Shelfmark mode 3 s at the median. Two causes, both fixed here:

* ``MetadataManager.search_by_meta`` normalized every one of the 255,723 catalogue
  records on every search (3-5 s). The catalogue is now normalized once into
  ``_ShelfmarkSearchIndex``, which narrows a query to the records that can match;
  the old loop's rules (``_shelfmark_matches``) decide over those. The index must
  give the loop's answer, so it is checked against the loop here, on a synthetic
  catalogue built to hit every rule and on the real corpus.
* every search, a shelfmark lookup included, went to a research worker: a
  subprocess that loads the catalogue (~6 s unless one was started ahead) and
  serves one search, behind a FIFO queue with one slot by default. ``IsolatedEngine``
  now resolves a Title/Shelfmark search's manuscripts in the server process and runs
  a lookup (``INLINE_METADATA_ROWS`` or fewer) in the calling thread; a broader
  search still goes to a worker, with its manuscripts, so the worker skips the
  catalogue too.
"""
import itertools
import threading
import time
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from shared.metadata_manager import (
    MetadataManager,
    _ShelfmarkSearchIndex,
    _shelfmark_forms,
    _shelfmark_matches,
    _shelfmark_query,
)
from shared.search_engine import SearchEngine
from web import research_jobs
from web.research_jobs import INLINE_METADATA_ROWS, IsolatedEngine


# ---------------------------------------------------------------- the synthetic catalogue

def _synthetic_shelfmarks():
    """Shelfmarks shaped like every library's, dense enough that the rules' edge cases
    occur: numeric prefixes across a dot (12.1 vs 12.10 vs 12.100), the same number
    in several series, dotless and slashed forms, letters after digits."""
    marks = []
    for n in (1, 2, 10, 12, 120, 123, 1234):
        for sub in (1, 2, 10, 12, 19, 100, 123, 2000):
            marks.append(f'T-S {n}.{sub}')
            marks.append(f'T-S NS {n}.{sub}')
        marks.append(f'T-S K {n}.{n}')
        marks.append(f'T-S Ar.{n}.{n + 1}')
        marks.append(f'T-S AS {n}.{n}')
        marks.append(f'ENA {n}.{n}')
        marks.append(f'ENA NS {n}.{n}')
        marks.append(f'MS heb. d.{n}/{n}')
        marks.append(f'Ms. heb. c. {n}.{n}')
        marks.append(f'Ms. EVR II A {n}')
        marks.append(f'Ms. EVR II A {n}, 1')
        marks.append(f'Mosseri IV.{n}')
        marks.append(f'Moss. VIII,{n}.1')
        marks.append(f'Halper {n}')
        marks.append(f'Or. 5{n}')
        marks.append(f'GASTER {n}.1')
        marks.append(f'L {n}')
        marks.append(f'Ms. Genizah {n}')
        marks.append(f'F {n}')
        marks.append(f'{n}')
        marks.append(f'T-S 12.{n}a')
        marks.append(f'ts12{n}')
    marks += ['Cambridge University Library Ms. T-S 12.319', 'T-S 12.319', 'T S 12 123', '-', '.',
              'Ms. Heb. 577.10.6=4', 'JTS ENA 2727.5', 'ENA 2727.5']
    return marks


QUERIES = [
    'T-S 12.123', 'T-S K 12.12', 'T-S NS 12.1', 'T-S Ar.12.13', 'ENA 2727.5', 'ENA NS 12.12',
    'MS Heb c 12', 'MS. Heb. d.12', 'Ms. EVR II A 12', 'Mosseri IV.12', 'Halper 12', 'Or. 512',
    'Gaster 1234', 'T-S 12', 'ts12123', 'T S 12 123', 'T-S NS 12', 'T-S NS 120.2', 'ms heb d 12/12',
    'T-S AS 1.1', '12.1', '12', 'L 1', 'ENA', 't', 'T-S 12.1', 'T-S 12.10', 'ts12.1', 'ts121',
    'T-S 12.123a', 'EVR II A 12, 1', 'Ms. Heb. 577.10.6=4', 'ts', 'x', 'zzz 999', 'T-S 12.123.',
    '  T-S 12.123  ', 'T-S', 'Ar.12', '5', '', ' ', '-', '.', '...', '* ', '12.',
]


def _bank(marks):
    return {f'99{i:08d}': {'shelfmark': mark, 'title': '', 'library_code': 'X', 'call_numbers_raw': [mark]}
            for i, mark in enumerate(marks)}


def _manager(bank, nli_cache=None, loaded=True):
    mm = MetadataManager.__new__(MetadataManager)
    mm.csv_bank = bank
    mm._csv_bank_loaded = loaded
    mm.nli_cache = nli_cache if nli_cache is not None else {}
    mm._shelfmark_index = None
    mm._shelfmark_search_index = None
    mm._nli_shelfmark_forms = {}
    return mm


@pytest.fixture(scope='module')
def bank():
    return _bank(_synthetic_shelfmarks())


# ---------------------------------------------------------------- the index gives the loop's answer

def test_every_rule_is_exercised_by_the_synthetic_queries(bank):
    """The equivalence test means something only if each matching rule fires for
    some (query, record) pair AND the candidate families each contribute."""
    mm = _manager(bank)
    fired = set()
    for q in QUERIES:
        query = _shelfmark_query(q, mm._normalize_shelfmark)
        q_norm, q_normalized, q_nodots, q_tokens = query
        for data in bank.values():
            val_norm, val_normalized, val_nodots, val_tokens = _shelfmark_forms(data['shelfmark'], mm._normalize_shelfmark)
            if val_norm == q_norm:
                fired.add('exact')
            elif val_normalized == q_normalized:
                fired.add('normalized')
            elif val_nodots == q_nodots:
                fired.add('dotless')
            elif q_normalized and val_normalized.startswith(q_normalized) and (
                    len(val_normalized) == len(q_normalized)
                    or val_normalized[len(q_normalized)] == '.'
                    or not val_normalized[len(q_normalized)].isdigit()):
                fired.add('prefix')
            elif _shelfmark_matches((val_norm, val_normalized, val_nodots, val_tokens), query):
                fired.add('tokens')
    assert fired == {'exact', 'normalized', 'dotless', 'prefix', 'tokens'}


def test_index_and_full_scan_agree_on_every_query(bank):
    mm = _manager(bank)
    for q in QUERIES:
        query = _shelfmark_query(q, mm._normalize_shelfmark)
        assert mm._shelfmark_search_catalogue(query) == mm._shelfmark_scan_catalogue(query), q
    assert mm._shelfmark_search_index is not None       # the index path ran


def test_search_by_meta_agrees_with_the_loop_including_the_nli_cache(bank):
    nli = {'NLIONLY': {'shelfmark': 'T-S 12.123', 'title': ''},        # not in the catalogue
           next(iter(bank)): {'shelfmark': 'Updated 7.7', 'title': ''}}  # a catalogue record, updated
    mm = _manager(bank, nli_cache=nli)
    expected = _manager(bank, nli_cache=nli, loaded=False)   # loaded=False: the loop, every record
    for q in ('T-S 12.123', 'ts12123', 'Updated 7.7', 'updated', 'T-S 12'):
        with patch('shared.shelfmark_bridge.lookup_cudl', lambda text: None):
            assert sorted(mm.search_by_meta(q, 'shelfmark')) == sorted(expected.search_by_meta(q, 'shelfmark')), q
    assert expected._shelfmark_search_index is None
    assert 'NLIONLY' in mm.search_by_meta('T-S 12.123', 'shelfmark')
    assert mm.search_by_meta('Updated 7.7', 'shelfmark') == [next(iter(bank))]


def test_index_candidates_miss_nothing_the_rules_match(bank):
    """Candidate generation is the only new logic: the rules' matches are among
    the candidates for every query, so a rule can never be starved of a record."""
    mm = _manager(bank)
    index = _ShelfmarkSearchIndex(bank, mm._normalize_shelfmark)
    for q in QUERIES:
        query = _shelfmark_query(q, mm._normalize_shelfmark)
        if not query[0] or not query[1] or not query[3]:
            continue                                     # degenerate: the loop runs, not the index
        matching = {sid for sid, data in bank.items()
                    if _shelfmark_matches(_shelfmark_forms(data['shelfmark'], mm._normalize_shelfmark), query)}
        assert matching <= index.candidates(query), q


def test_a_broken_candidate_family_is_caught(bank, monkeypatch):
    """The equivalence test can fail: drop one family of candidates and it does."""
    mm = _manager(bank)
    index = mm._shelfmark_search_index_for(bank)
    monkeypatch.setattr(index, 'nodot_keys', [])
    monkeypatch.setattr(index, 'nodot_ids', [])
    query = _shelfmark_query('ts12123', mm._normalize_shelfmark)
    assert mm._shelfmark_search_catalogue(query) != mm._shelfmark_scan_catalogue(query)


# ---------------------------------------------------------------- when the index is (not) used

def test_the_catalogue_is_not_scanned_once_indexed(bank, monkeypatch):
    mm = _manager(bank)
    expected = sorted(_manager(bank, loaded=False).search_by_meta('T-S 12.123', 'shelfmark'))   # the loop
    assert len(expected) >= 4            # T-S 12.123, T S 12 123, ts12123, T-S 12.123a, T-S NS 12.123 ...
    assert mm.search_by_meta('T-S 12.123', 'shelfmark')          # builds the index
    monkeypatch.setattr(mm, '_shelfmark_scan_catalogue',
                        lambda query: pytest.fail('the catalogue was scanned'))
    assert sorted(mm.search_by_meta('T-S 12.123', 'shelfmark')) == expected


def test_a_loading_catalogue_is_scanned_not_indexed(bank):
    mm = _manager(bank, loaded=False)
    assert mm.search_by_meta('T-S 12.123', 'shelfmark')
    assert mm._shelfmark_search_index is None


def test_degenerate_queries_are_scanned_not_indexed(bank, monkeypatch):
    mm = _manager(bank)
    monkeypatch.setattr(mm, '_shelfmark_search_index_for',
                        lambda csv_bank: pytest.fail('an index was asked for'))
    for q in ('', '   ', '-', '.', '...'):
        mm._shelfmark_search_catalogue(_shelfmark_query(q, mm._normalize_shelfmark))


def test_a_reloaded_or_replaced_catalogue_gets_a_new_index(bank):
    mm = _manager(bank)
    assert mm.search_by_meta('T-S 12.123', 'shelfmark')
    first = mm._shelfmark_search_index
    mm.csv_bank = dict(bank)                                      # replaced
    mm.csv_bank['NEW'] = {'shelfmark': 'T-S 12.123', 'title': ''}
    assert 'NEW' in mm.search_by_meta('T-S 12.123', 'shelfmark')
    assert mm._shelfmark_search_index is not first
    mm.csv_bank['NEW2'] = {'shelfmark': 'T-S 12.123', 'title': ''}   # grown in place
    assert 'NEW2' in mm.search_by_meta('T-S 12.123', 'shelfmark')


def test_two_threads_build_one_index(bank):
    mm = _manager(bank)
    seen = []

    def search():
        seen.append(sorted(mm.search_by_meta('T-S 12.123', 'shelfmark')))
    threads = [threading.Thread(target=search) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len({tuple(s) for s in seen}) == 1


def test_nli_cache_forms_follow_a_changed_shelfmark(bank):
    mm = _manager(bank, nli_cache={'N1': {'shelfmark': 'Alpha 1', 'title': ''}})
    assert mm.search_by_meta('Alpha 1', 'shelfmark') == ['N1']
    mm.nli_cache['N1'] = {'shelfmark': 'Beta 2', 'title': ''}
    assert mm.search_by_meta('Alpha 1', 'shelfmark') == []
    assert mm.search_by_meta('Beta 2', 'shelfmark') == ['N1']


def test_title_search_is_a_substring_match_as_before(bank):
    bank = dict(bank)
    bank['T1'] = {'shelfmark': 'X 1', 'title': 'Sefer ha-Mitzvot'}
    mm = _manager(bank, nli_cache={'T2': {'shelfmark': '', 'title': 'the mitzvot of'}})
    assert sorted(mm.search_by_meta('MITZVOT', 'title')) == ['T1', 'T2']
    assert mm.search_by_meta('mitzvot of', 'title') == ['T2']


# ---------------------------------------------------------------- the engine takes resolved manuscripts

def _engine(meta_mgr):
    with patch.object(SearchEngine, '__init__', lambda self, *a, **kw: None):
        engine = SearchEngine.__new__(SearchEngine)
    engine.meta_mgr = meta_mgr
    engine.searcher = None
    engine.index = None
    return engine


def test_execute_search_with_resolved_ids_does_not_search_the_catalogue():
    meta_mgr = MagicMock()
    meta_mgr.search_by_meta.side_effect = AssertionError('the catalogue was searched again')
    meta_mgr.get_meta_for_id.side_effect = lambda sid: (f'T-S {sid}', 'title')
    meta_mgr.get_library_for_id.return_value = 'CUL'
    rows = _engine(meta_mgr).execute_search('T-S 12.123', 'Shelfmark', 0, metadata_sys_ids=['2', '1'])
    assert [r['display']['shelfmark'] for r in rows] == ['T-S 1', 'T-S 2']
    assert all(r['metadata_only'] for r in rows)


def test_metadata_search_ids_resolves_as_execute_metadata_search_did():
    meta_mgr = MagicMock()
    meta_mgr.search_by_meta.return_value = ['3', '1', '2']
    engine = _engine(meta_mgr)
    assert engine.metadata_search_ids('T-S 12', 'Shelfmark') == ['3', '1', '2']
    meta_mgr.search_by_meta.assert_called_with('T-S 12', 'shelfmark')
    assert engine.metadata_search_ids('x', 'Title', restrict_sys_ids={'1', '2'}) == ['1', '2']
    meta_mgr.search_by_meta.assert_called_with('x', 'title')
    assert engine.metadata_search_ids('*', 'Shelfmark', restrict_sys_ids={'b', 'a'}) == ['a', 'b']
    assert engine.metadata_search_ids('', 'Shelfmark', restrict_sys_ids={'b', 'a'}) == ['a', 'b']


# ---------------------------------------------------------------- the website and the API skip the worker

class _Fake:
    """An engine as IsolatedEngine sees it: a catalogue that is loaded, and
    execute_search that records where it ran and, like the engine's metadata
    search, asks its progress callback per manuscript and answers a cancel
    (InterruptedError from that callback) with the rows it had."""
    def __init__(self, ids, row_seconds=0.0):
        self.ids = list(ids)
        self.row_seconds = row_seconds
        self.meta_mgr = SimpleNamespace(_csv_bank_loaded=True)
        self.calls = []

    def metadata_search_ids(self, query_str, mode, restrict_sys_ids=None):
        self.calls.append(('resolve', query_str, mode, restrict_sys_ids))
        return self.ids

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None,
                       responsa_options=None, restrict_sys_ids=None, text_position=None,
                       corpus_scope='all', phase_callback=None, preview_callback=None, ids_only=False,
                       metadata_sys_ids=None):
        self.calls.append(('run', threading.current_thread().name, metadata_sys_ids))
        rows = []
        try:
            for i, sid in enumerate(metadata_sys_ids or []):
                if progress_callback:
                    progress_callback(i, len(metadata_sys_ids))
                if self.row_seconds:
                    time.sleep(self.row_seconds)
                rows.append({'display': {'id': sid}})
        except InterruptedError:
            rows.append({'display': {'id': 'stopped'}})
        return rows


def _no_queue(monkeypatch):
    monkeypatch.setattr(research_jobs, 'get_queue',
                        lambda: pytest.fail('a worker was asked for'))


@pytest.mark.parametrize('mode', ['Shelfmark', 'Title'])
def test_a_lookup_runs_in_the_calling_thread_without_a_worker(monkeypatch, mode):
    _no_queue(monkeypatch)
    fake = _Fake(['1', '2'])
    ticks = []
    rows = IsolatedEngine(fake, 'search').execute_search('T-S 12.123', mode, 0, progress_callback=lambda *v: ticks.append(v),
                                                         restrict_sys_ids={'1', '2', '3'})
    assert [r['display']['id'] for r in rows] == ['1', '2']
    assert fake.calls == [('resolve', 'T-S 12.123', mode, {'1', '2', '3'}),
                          ('run', threading.current_thread().name, ['1', '2'])]
    assert ticks == [(0, 2), (1, 2)]                 # the caller's progress still hears the engine


# What the worker-wait loop enforces on a worker, the inline path enforces on itself
# (Codex review of PR #468, round 1).

def test_an_inline_lookup_stops_at_its_deadline(monkeypatch):
    from shared.search_regex import SearchBudgetExceeded, search_budget
    _no_queue(monkeypatch)
    fake = _Fake([str(i) for i in range(20)], row_seconds=0.02)
    with pytest.raises(SearchBudgetExceeded):
        IsolatedEngine(fake, 'search').controlled(seconds=0.05).execute_search('T-S', 'Shelfmark', 0)
    assert fake.calls[-1][0] == 'run'
    with pytest.raises(SearchBudgetExceeded), search_budget(0.05):   # the API's budget, as a context
        IsolatedEngine(fake, 'search').execute_search('T-S', 'Shelfmark', 0)


def test_an_inline_lookup_stops_when_cancelled(monkeypatch):
    _no_queue(monkeypatch)
    fake = _Fake(['1', '2', '3'])
    cancel = threading.Event()
    cancel.set()
    with pytest.raises(InterruptedError):
        IsolatedEngine(fake, 'search').controlled(cancel=cancel).execute_search('T-S', 'Shelfmark', 0)
    assert fake.calls[-1][0] == 'run'                 # it ran, was stopped at its first check, and raised


def test_an_inline_lookup_keeps_the_website_time_limit(monkeypatch):
    from shared.config import Config
    _no_queue(monkeypatch)
    monkeypatch.setattr(Config, 'WEB_SEARCH_TIME_LIMIT', 0.05)
    fake = _Fake([str(i) for i in range(20)], row_seconds=0.02)
    rows = IsolatedEngine(fake, 'search').execute_search('T-S', 'Shelfmark', 0)
    assert rows[-1]['display']['id'] == 'stopped' and len(rows) < 20
    assert research_jobs.consume_time_limit_stop() is True
    assert research_jobs.consume_time_limit_stop() is False
    monkeypatch.setattr(Config, 'WEB_SEARCH_TIME_LIMIT', 0)         # no limit
    assert len(IsolatedEngine(fake, 'search').execute_search('T-S', 'Shelfmark', 0)) == 20
    assert research_jobs.consume_time_limit_stop() is False


def test_the_inline_ceiling_is_a_lookup_not_a_listing(monkeypatch):
    _no_queue(monkeypatch)
    fake = _Fake([str(i) for i in range(INLINE_METADATA_ROWS)])
    assert len(IsolatedEngine(fake, 'search').execute_search('T-S', 'Shelfmark', 0)) == INLINE_METADATA_ROWS
    assert 1 < INLINE_METADATA_ROWS <= 200


WORKER_ROWS = [{'display': {'id': 'from-the-worker'}}]


def _stub_queue(monkeypatch):
    """A queue whose worker answers with the manuscripts it was handed, or with
    WORKER_ROWS when it was handed none (it resolved them itself, as before)."""
    submitted = []

    def submit(payload):
        submitted.append(payload)
        handed = payload['arguments'].get('metadata_sys_ids')
        rows = [{'display': {'id': sid}} for sid in handed] if handed is not None else WORKER_ROWS
        future = Future()
        future.set_result({'value': rows, 'cutoff': {}, 'time_limit': False})
        return SimpleNamespace(future=future, status='Searching', progress=(0, 0),
                               preview=None, preview_sequence=0)
    queue = SimpleNamespace(submit=submit, position=lambda job: 0, cancel=lambda job: None)
    monkeypatch.setattr(research_jobs, 'get_queue', lambda: queue)
    return submitted


def test_a_broad_search_goes_to_a_worker_with_its_manuscripts(monkeypatch):
    submitted = _stub_queue(monkeypatch)
    ids = [str(i) for i in range(INLINE_METADATA_ROWS + 1)]
    fake = _Fake(ids)
    rows = IsolatedEngine(fake, 'search').execute_search('T-S', 'Shelfmark', 0)
    assert len(rows) == len(ids)
    assert fake.calls == [('resolve', 'T-S', 'Shelfmark', None)]       # not run here
    assert submitted[0]['method'] == 'execute_search'
    assert submitted[0]['arguments']['metadata_sys_ids'] == ids
    assert submitted[0]['arguments']['mode'] == 'Shelfmark'


def test_a_text_search_still_goes_to_a_worker_untouched(monkeypatch):
    submitted = _stub_queue(monkeypatch)
    fake = _Fake(['1'])
    monkeypatch.setattr(research_jobs, 'INLINE_METADATA_ROWS', 10**6)
    assert IsolatedEngine(fake, 'search').execute_search('abc', 'literal', 0) == WORKER_ROWS
    assert 'metadata_sys_ids' not in submitted[-1]['arguments']
    assert fake.calls == []


def test_a_catalogue_still_loading_leaves_the_worker_to_resolve(monkeypatch):
    submitted = _stub_queue(monkeypatch)
    fake = _Fake(['1'])
    fake.meta_mgr._csv_bank_loaded = False
    assert IsolatedEngine(fake, 'search').execute_search('T-S 12.123', 'Shelfmark', 0) == WORKER_ROWS
    assert fake.calls == []
    assert 'metadata_sys_ids' not in submitted[-1]['arguments']


def test_an_engine_without_the_resolver_is_left_alone(monkeypatch):
    submitted = _stub_queue(monkeypatch)

    class Bare:
        def execute_search(self, query_str, mode, gap, progress_callback=None):
            raise AssertionError('ran in process')
    assert IsolatedEngine(Bare(), 'search').execute_search('T-S 12.123', 'Shelfmark', 0) == WORKER_ROWS
    assert 'metadata_sys_ids' not in submitted[-1]['arguments']


def test_the_api_search_route_hands_metadata_modes_to_the_engine_wrapper():
    """web/search_api.py runs every search through api_engine(IsolatedEngine): the
    inline path above is the API's path too, with search_mode_value untouched."""
    source = open('web/search_api.py', encoding='utf-8').read()
    assert "'shelfmark': 'Shelfmark'" in source
    assert 'worker_searcher = api_engine(state.searcher, request, core_timeout)' in source
    assert 'posthog_search_mode_value = req.search_mode' in source


# ---------------------------------------------------------------- the real corpus

@pytest.fixture(scope='module')
def real_manager(tmp_path_factory):
    from tests.shelfmark_corpus import MIN_ROWS, load_real_manager
    mm = load_real_manager(tmp_path_factory.mktemp('cudl'))
    if len(mm.csv_bank) < MIN_ROWS:
        pytest.skip(f'libraries.csv has {len(mm.csv_bank)} rows: the corpus is not here')
    return mm


REAL_QUERIES = ['T-S 12.123', 'ENA 2727.5', 'MS Heb c 57', 'Ms. EVR II A 1501', 'ts12123', 'T-S NS 120.2']


def test_real_corpus_index_agrees_with_the_loop(real_manager):
    mm = real_manager
    for q in REAL_QUERIES:
        query = _shelfmark_query(q, mm._normalize_shelfmark)
        via_index = mm._shelfmark_search_catalogue(query)
        assert via_index, q                                   # a real manuscript each
        assert via_index == mm._shelfmark_scan_catalogue(query), q


def test_real_corpus_lookup_is_fast_once_indexed(real_manager):
    import time
    mm = real_manager
    mm.search_by_meta('T-S 12.123', 'shelfmark')               # builds the index
    started = time.perf_counter()
    for q in itertools.islice(itertools.cycle(REAL_QUERIES), 12):
        mm.search_by_meta(q, 'shelfmark')
    per_lookup = (time.perf_counter() - started) / 12
    # 3.5-4.8 s each before (one normalization per record per search); generous
    # for a loaded CI runner, and an order of magnitude under the old cost.
    assert per_lookup < 0.3, per_lookup
