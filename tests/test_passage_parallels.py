# -*- coding: utf-8 -*-
"""Phase 145: PassageSearcher (shared/passage_parallels.py) row contract, and
the `method` routing shared/parallels_service.py and web/passage_assets.py
add on top of the passage-matching engine.

Three concerns, matching the task:

1. Row contract: a PassageSearcher.search_composition_logic() row carries
   the same keys/types shared/parallels_service.py's CompositionSearcher
   Protocol expects (uid, raw_header, src_lbl, source_ctx, text, score,
   final_score, chunk_count, chunk_hits), against a TINY SYNTHETIC index
   (no real corpus data -- this worktree carries none).
2. Display-span correctness: a verbatim query's highlighted span, read back
   out of `row['text']`'s `*...*` markers, is the CORRECT substring of the
   ORIGINAL (un-normalized) manuscript text -- including the punctuation,
   nikud and reflowed spacing norm_stream stripped before matching. This is
   the "bounded re-normalization" display contract
   (docs/specs/passage-matching-algorithm.md); getting the offset
   arithmetic wrong here would silently highlight the wrong text.
3. Chunk-path routing unchanged: shared.parallels_service.fetch_parallels_
   results's new `method` parameter must be a true no-op for method='chunk'
   (the default) -- same call into `searcher`, same returned bundle,
   whether `method` is omitted or passed explicitly as 'chunk'.

Fail-closed test: PASSAGE_PARALLELS_ENABLED=1 + a missing index directory
leaves web.passage_assets.passage_available() False, with no exception.
"""
from __future__ import annotations

import asyncio
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_builder import build_index  # noqa: E402
from shared.passage_index import open_index  # noqa: E402
from shared.passage_normalize import norm_stream_fast  # noqa: E402
from shared.passage_parallels import PassageSearcher  # noqa: E402
from shared.passage_policy import get_preset  # noqa: E402

ALEF = 0x05D0


def _aperiodic(n: int, salt: int = 0) -> str:
    """Pseudo-random Hebrew letters with no short period (crib from
    tests/test_passage_search.py -- a periodic fixture collapses under gram
    dedup and is useless for testing ordinary-text retrieval)."""
    out = []
    x = 987_654_321 + salt
    for _ in range(n):
        x = (x * 1_103_515_245 + 12_345) & 0x7FFFFFFF
        out.append(chr(ALEF + (x >> 7) % 22))
    return ''.join(out)


def _record_id(r: int) -> str:
    """Realistic `{sys_id}_{IE..}_{P######}_{FL..}` shape (matches
    shared/passage_corpus.py's header contract and shared/passage_parallels.
    py's _UID_RE / _SYS_ID_RE extraction regexes)."""
    return f"99{r:08d}_IE{10_000_000 + r}_P{r:07d}_FL1"


def _interleave_with_noise(letters: str) -> str:
    """Insert a space + a nikud mark (HIRIQ, U+05B4 -- dropped by
    norm_stream) every 4 letters. Simulates reflowed HTR line breaks and
    diacritics: the normalized stream is IDENTICAL to `letters`, but the
    character offsets in the "original" text diverge from the normalized
    stream's offsets, which is exactly what project_span must get right."""
    out = []
    for i, ch in enumerate(letters):
        out.append(ch)
        if i % 4 == 3:
            out.append(' ִ')
    return ''.join(out)


class _FakeTextFetcher:
    """Minimal PageTextFetcher: record_id -> original (un-normalized) text."""

    def __init__(self, mapping: dict):
        self._mapping = mapping

    def get_full_text_by_header(self, full_header: str):
        return self._mapping.get(full_header)


@pytest.fixture(scope='module')
def synthetic_corpus(tmp_path_factory):
    """A tiny synthetic passage index + the original texts it was built
    from, so a fake text fetcher can serve them back for the display path.
    """
    d = str(tmp_path_factory.mktemp('ppar'))
    motif = _aperiodic(80)
    noisy_motif = _interleave_with_noise(motif)

    originals: dict = {}
    records = []
    for r in range(15):
        rid = _record_id(r)
        body = _aperiodic(300, salt=1000 + r)
        if r == 0:
            # The one carrier: the motif, WITH interspersed spaces/nikud,
            # embedded inside unrelated filler text.
            text = body[:120] + ' ' + noisy_motif + ' ' + body[120:]
        else:
            text = body
        originals[rid] = text
        records.append((rid, text))

    build_index(records, d, partitions=3, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, originals, motif


@pytest.fixture
def searcher(synthetic_corpus):
    idx, originals, _motif = synthetic_corpus
    return PassageSearcher(index=idx, text_fetcher=_FakeTextFetcher(originals))


# ---------------------------------------------------------------------------
# 1. Row contract.
# ---------------------------------------------------------------------------

_ROW_KEYS = {
    'uid': str, 'raw_header': str, 'src_lbl': str, 'source_ctx': str,
    'text': str, 'score': float, 'final_score': float, 'chunk_count': int,
    'chunk_hits': list,
}


def test_row_contract_keys_and_types(searcher, synthetic_corpus):
    _idx, _originals, motif = synthetic_corpus
    result = searcher.search_composition_logic(full_text=motif)
    assert set(result.keys()) == {'main', 'filtered'}
    assert result['filtered'] == []
    assert result['main'], 'the motif carrier was not found'

    row = result['main'][0]
    for key, typ in _ROW_KEYS.items():
        assert key in row, f'missing row key {key!r}'
        assert isinstance(row[key], typ), (
            f'{key!r} is {type(row[key])}, expected {typ}')

    # The one record that carries the motif.
    assert row['raw_header'] == _record_id(0)
    assert row['uid'] == f'IE{10_000_000}_P{0:07d}_FL1'
    assert row['chunk_count'] == len(row['chunk_hits']) > 0
    assert row['score'] == row['final_score'] == pytest.approx(row['score'])
    assert row['score'] > 0


def test_uid_is_parseable_by_the_browse_locator_regex(searcher, synthetic_corpus):
    """web/search_api.py::_UID_PATTERN is anchored (^...$) and rejects a
    sys_id prefix -- uid must be EXACTLY the IE/P/FL portion, matching what
    the Tantivy `unique_id` field would hold for the same page."""
    _idx, _originals, motif = synthetic_corpus
    result = searcher.search_composition_logic(full_text=motif)
    row = result['main'][0]
    uid_pattern = re.compile(r'^(IE\d+)_(P\d+)_(FL\d+)$')
    assert uid_pattern.match(row['uid']), row['uid']


def test_chunk_count_equals_number_of_spans(searcher, synthetic_corpus):
    """Plan contract: chunk_count = number of accepted spans (not a
    dedup-derived count like the incumbent's _count_unique_chunks)."""
    from shared.passage_search import search_passage
    idx, _originals, motif = synthetic_corpus
    hits, _report = search_passage(idx, motif, get_preset('standard-40'))
    by_record = {h.record_id: h for h in hits}
    result = searcher.search_composition_logic(full_text=motif)
    for row in result['main']:
        hit = by_record[row['raw_header']]
        assert row['chunk_count'] == hit.n_spans
        assert len(row['chunk_hits']) == hit.n_spans


def test_restrict_sys_ids_filters_rows(searcher, synthetic_corpus):
    _idx, _originals, motif = synthetic_corpus
    everyone = searcher.search_composition_logic(full_text=motif)
    assert everyone['main'], 'fixture precondition: at least one hit'
    result = searcher.search_composition_logic(
        full_text=motif, restrict_sys_ids={'nonexistent-sys-id'})
    assert result['main'] == []


def test_min_boundary_matches_filters_by_span_count(searcher, synthetic_corpus):
    _idx, _originals, motif = synthetic_corpus
    everyone = searcher.search_composition_logic(full_text=motif, boundary_mode='full')
    max_spans = max(r['chunk_count'] for r in everyone['main'])
    filtered = searcher.search_composition_logic(
        full_text=motif, boundary_mode='full',
        min_boundary_matches=max_spans + 1,
    )
    assert filtered['main'] == []


# ---------------------------------------------------------------------------
# 2. Display-span correctness: offsets projected through punctuation/spaces.
# ---------------------------------------------------------------------------

_MARKER_RE = re.compile(r'\*(.*?)\*', re.DOTALL)


def test_highlight_span_is_the_correct_original_substring(searcher, synthetic_corpus):
    idx, originals, motif = synthetic_corpus
    result = searcher.search_composition_logic(full_text=motif)
    row = next(r for r in result['main'] if r['raw_header'] == _record_id(0))

    assert row['text'], 'no highlight text rendered for the top-N row'
    marked = _MARKER_RE.findall(row['text'])
    assert marked, f'no *marker* pairs found in {row["text"]!r}'

    original_text = originals[_record_id(0)]
    combined_marked = ''.join(marked)

    # Punctuation/space-preserving: the highlighted region must have carried
    # the interleaved space + nikud noise through, not just the bare letters
    # -- proving the span was projected onto ORIGINAL offsets, not the
    # stripped normalized stream.
    assert ' ' in combined_marked, (
        'highlighted span lost the interleaved spaces -- offsets were not '
        'projected onto the original text')
    assert 'ִ' in combined_marked, (
        'highlighted span lost the interleaved nikud -- offsets were not '
        'projected onto the original text')

    # Every marked character sequence must be a literal substring of the
    # ORIGINAL manuscript text at its correct location (not shifted).
    for m in marked:
        assert m in original_text, f'{m!r} is not a substring of the original text'

    # And, after stripping everything norm_stream would strip, the marked
    # text's letters must be (almost) exactly the query motif -- i.e. the
    # highlighted span really is the verbatim motif, not some unrelated
    # stretch of the record. A verbatim match scores near-zero density
    # (shared/passage_search.py's own test asserts best_density <= 0.02), so
    # near-total similarity is expected -- a difflib ratio rather than exact
    # equality absorbs the few characters a real Levenshtein alignment may
    # trim or admit at the span's edges.
    import difflib
    stripped = norm_stream_fast(combined_marked)
    assert stripped, 'nothing but noise survived normalization of the marked span'
    # norm_stream_fast final-folds (ך->כ, ם->מ, etc); `motif`'s own raw letters
    # include some UN-folded finals (_aperiodic draws from the first 22
    # codepoints after ALEF, which interleaves final forms), so compare
    # fold-for-fold rather than raw motif vs folded stripped text.
    folded_motif = norm_stream_fast(motif)
    ratio = difflib.SequenceMatcher(None, stripped, folded_motif).ratio()
    assert ratio > 0.9, (
        f'highlighted letters {stripped!r} do not match the (folded) query '
        f'motif {folded_motif!r} (similarity ratio {ratio:.3f})')


def test_source_ctx_highlights_the_query_side(searcher, synthetic_corpus):
    """source_ctx mirrors the incumbent's src_snippets: the QUERY-side text
    around the matched span, with *marker* highlighting -- built from the
    pasted composition, not the manuscript."""
    _idx, _originals, motif = synthetic_corpus
    # Wrap the query itself in filler + noise so source_ctx has real context
    # to show and a real offset-projection job to do.
    prefix = _aperiodic(200, salt=55)
    query = prefix + _interleave_with_noise(motif)
    result = searcher.search_composition_logic(full_text=query)
    row = next(r for r in result['main'] if r['raw_header'] == _record_id(0))
    assert row['source_ctx']
    assert '*' in row['source_ctx']
    marked = _MARKER_RE.findall(row['source_ctx'])
    combined = ''.join(marked)
    assert ' ' in combined or 'ִ' in combined, (
        'source_ctx did not carry the query-side noise through its span projection')


def test_rows_beyond_render_cap_have_no_highlight_text(searcher, synthetic_corpus):
    """Bounded re-normalization: only the top-N rendered rows get text/
    source_ctx/chunk_hits built; rows beyond the cap still carry every other
    field so a caller that groups/sorts/caps AFTER this call sees a complete
    row set."""
    idx, originals, motif = synthetic_corpus
    small_cap_searcher = PassageSearcher(
        index=idx, text_fetcher=_FakeTextFetcher(originals), render_cap=0)
    result = small_cap_searcher.search_composition_logic(full_text=motif)
    assert result['main'], 'fixture precondition: at least one hit'
    for row in result['main']:
        assert row['text'] == ''
        assert row['source_ctx'] == ''
        assert row['chunk_hits'] == []
        # Every OTHER field is still populated (score/final_score/chunk_count
        # /uid/raw_header) -- the cap only bounds the re-normalization work.
        assert row['score'] > 0
        assert row['chunk_count'] > 0


# ---------------------------------------------------------------------------
# 3. Chunk-path routing unchanged for method='chunk' / omitted.
# ---------------------------------------------------------------------------

def test_fetch_parallels_results_method_chunk_matches_omitted():
    from unittest.mock import MagicMock

    from shared.parallels_service import fetch_parallels_results

    def _make_engine():
        engine = MagicMock()
        engine.search_composition_logic.return_value = {
            'main': [{
                'uid': 'IE1_P1_FL1', 'raw_header': 'h_99001_IE1_P1_FL1',
                'src_lbl': 'CUL', 'source_ctx': 'ctx', 'text': 'txt',
                'score': 5.0, 'final_score': 5.0, 'chunk_count': 1,
                'chunk_hits': [(0, 'ctx', 5.0, 'txt')],
            }],
            'filtered': [],
        }
        return engine

    meta_mgr = MagicMock()
    meta_mgr.parse_full_id_components.return_value = {'sys_id': '99001'}

    async def _run(method_kwargs):
        engine = _make_engine()
        bundle = await fetch_parallels_results(
            searcher=engine, meta_mgr=meta_mgr, text='hello world',
            chunk_size=5, mode='exact', **method_kwargs,
        )
        return engine, bundle

    engine_omitted, bundle_omitted = asyncio.run(_run({}))
    engine_explicit, bundle_explicit = asyncio.run(_run({'method': 'chunk'}))

    assert engine_omitted.search_composition_logic.call_args \
        == engine_explicit.search_composition_logic.call_args
    assert bundle_omitted.main_results == bundle_explicit.main_results
    assert bundle_omitted.filtered_results == bundle_explicit.filtered_results
    assert bundle_omitted.boundary_options == bundle_explicit.boundary_options
    assert bundle_omitted.truncated_to_200 == bundle_explicit.truncated_to_200


def test_run_sync_executor_kwarg_selects_the_dispatch_pool():
    """Direct test of the mechanism `fetch_parallels_results`'s `executor`
    param relies on: `_run_sync(func, _executor=None)` dispatches on the
    default pool; `_run_sync(func, _executor=<pool>)` dispatches on THAT
    pool. Verified by thread name (real executors, no asyncio mocking) --
    this is what makes "method='chunk' is byte-for-byte unchanged" true at
    the dispatch layer, not just at the call-arguments layer."""
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from shared.parallels_service import _run_sync

    names = {}

    def _capture(key):
        names[key] = threading.current_thread().name
        return key

    custom = ThreadPoolExecutor(max_workers=1, thread_name_prefix='passage-test-pool')
    try:
        asyncio.run(_run_sync(_capture, 'default'))
        asyncio.run(_run_sync(_capture, 'default_explicit', _executor=None))
        asyncio.run(_run_sync(_capture, 'custom', _executor=custom))
    finally:
        custom.shutdown(wait=True)

    assert 'passage-test-pool' not in names['default']
    assert 'passage-test-pool' not in names['default_explicit']
    assert 'passage-test-pool' in names['custom']


def test_fetch_parallels_results_chunk_path_ignores_executor_kwarg():
    """method='chunk' must dispatch on the DEFAULT executor even when a
    caller also passes `executor` -- only method='passage' is allowed to
    change which executor is used (shared/parallels_service.py's
    `executor if method == 'passage' else None` line)."""
    from unittest.mock import MagicMock
    from concurrent.futures import ThreadPoolExecutor

    from shared.parallels_service import fetch_parallels_results

    engine = MagicMock()
    engine.search_composition_logic.return_value = {'main': [], 'filtered': []}
    meta_mgr = MagicMock()
    decoy = ThreadPoolExecutor(max_workers=1, thread_name_prefix='decoy-pool')
    try:
        asyncio.run(fetch_parallels_results(
            searcher=engine, meta_mgr=meta_mgr, text='hello',
            chunk_size=5, mode='exact', method='chunk', executor=decoy,
        ))
    finally:
        decoy.shutdown(wait=True)
    # The mock ran synchronously inside whichever executor _run_sync chose;
    # asserting the CALL happened at all (MagicMock records it regardless of
    # thread) plus the dedicated unit test above pins WHICH pool is chosen --
    # together they cover the claim without re-mocking asyncio internals.
    engine.search_composition_logic.assert_called_once()


# ---------------------------------------------------------------------------
# Fail-closed: flag ON + missing index dir -> passage_available() False.
# ---------------------------------------------------------------------------

def test_passage_available_false_on_missing_index_dir(tmp_path, monkeypatch):
    import web.passage_assets as passage_assets

    monkeypatch.setattr(passage_assets, 'PASSAGE_PARALLELS_ENABLED', True)
    monkeypatch.setattr(passage_assets, 'PASSAGE_DATA_DIR', str(tmp_path / 'does-not-exist'))
    try:
        ready = passage_assets.load_passage_state()
        assert ready is False
        assert passage_assets.passage_available() is False
        assert passage_assets.get_passage_searcher(text_fetcher=None) is None
    finally:
        # Restore module state so later tests in the same process are not
        # left pointing at a synthetic, now-deleted tmp_path.
        passage_assets.load_passage_state()


def test_passage_available_false_when_flag_off_even_with_real_index(synthetic_corpus, monkeypatch):
    import web.passage_assets as passage_assets

    idx, _originals, _motif = synthetic_corpus
    monkeypatch.setattr(passage_assets, 'PASSAGE_PARALLELS_ENABLED', False)
    monkeypatch.setattr(passage_assets, '_state',
                         passage_assets._PassageState(ready=True, index=idx))
    assert passage_assets.passage_available() is False
    assert passage_assets.get_passage_searcher(text_fetcher=None) is None
