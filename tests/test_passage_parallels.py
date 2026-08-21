# -*- coding: utf-8 -*-
"""Phase 145: PassageSearcher (shared/passage_parallels.py) row contract, plus
fixes from the post-implementation adversarial review + real-data smoke test.

Sections, matching the review findings:

1. Row contract: a PassageSearcher.search_composition_logic() row carries
   the same keys/types shared/parallels_service.py's CompositionSearcher
   Protocol expects, against a TINY SYNTHETIC index (no real corpus data --
   this worktree carries none).
2. Display-span correctness: a verbatim query's highlighted span, read back
   out of `row['text']`'s `*...*` markers, is the CORRECT substring of the
   ORIGINAL (un-normalized) manuscript text -- including punctuation, nikud
   and reflowed spacing norm_stream stripped before matching.
3. Finding #1 ("THE BIG ONE"): rendered rows == kept rows, mutation-proven --
   the test in this section is written to FAIL under the old raw-hit-rank
   rendering and PASS under the group-cap fix; both directions were verified
   by hand in this session (see the commit message for the exact revert
   used).
4. Finding #2: boundary_mode other than 'full' raises ValueError.
5. Finding #3: filter_text parity with SearchEngine.search_composition_logic.
6. Finding #4: chunk_index is comparable across records (query-side ordinal,
   not a per-record-local counter).
7. Finding #5: a literal '*' in manuscript text does not produce a spurious
   marker.
8. Finding #6: shared.search_engine.SearchEngine.get_full_text_by_header
   rejects a header outside the closed character set before ever touching
   Tantivy.
9. Executor dispatch (finding #8): `fetch_parallels_results`'s `executor`
   kwarg alone decides dispatch now (no more `method` parameter).

Fail-closed test: PASSAGE_PARALLELS_ENABLED=1 + a missing index directory
leaves web.passage_assets.passage_available() False, with no exception.
"""
from __future__ import annotations

import asyncio
import difflib
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from shared.passage_builder import build_index  # noqa: E402
from shared.passage_index import open_index  # noqa: E402
from shared.passage_normalize import norm_stream_fast  # noqa: E402
from shared.passage_parallels import PassageSearcher, _extract_sys_id  # noqa: E402
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


def _grouped_record_id(group: int, page: int) -> str:
    """Same sys_id ('99' + an 8-digit group number) across every `page`
    within one `group` -- distinct IE/P per page -- so multiple records can
    share a sys_id GROUP for the cap tests."""
    return f"99{group:08d}_IE{30_000_000 + group * 10 + page}_P{page:07d}_FL1"


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
    Only ONE record (r==0) carries the motif.
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


@pytest.fixture(scope='module')
def grouped_corpus(tmp_path_factory):
    """4 distinct sys_id GROUPS x 3 pages each (12 records total), every
    page carrying the SAME motif so all 12 match the same query -- enough
    raw hits (12) spread across few enough groups (4) to exercise the
    GROUP-cap rule (not a raw-hit-rank rule) at a render_cap in between.
    """
    d = str(tmp_path_factory.mktemp('ppar_grouped'))
    motif = _aperiodic(80, salt=777)
    originals: dict = {}
    records = []
    for group in range(4):
        for page in range(3):
            rid = _grouped_record_id(group, page)
            body = _aperiodic(200, salt=5000 + group * 10 + page)
            text = body[:50] + ' ' + motif + ' ' + body[50:]
            originals[rid] = text
            records.append((rid, text))

    build_index(records, d, partitions=2, apply_hygiene=False)
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


# ---------------------------------------------------------------------------
# 3. Finding #1 ("THE BIG ONE"): rendered rows == kept rows.
#
# Mutation-proof note: this test was run against the PRE-fix code (raw-rank
# rendering: `for rank, hit in enumerate(hits): ... if rank < self.render_cap:
# render(...)`, returning ALL hits unconditionally) in this session and
# failed with an empty `row['text']` on a kept-but-unrendered row; it was
# then run against the fixed code (group-cap applied internally, only
# surviving rows returned) and passed. Both directions verified by hand
# before this file was committed.
# ---------------------------------------------------------------------------

def test_render_cap_renders_exactly_the_kept_rows_not_a_raw_rank_subset(grouped_corpus):
    """Before the fix, PassageSearcher rendered highlights for the first
    `render_cap` HITS by RAW RANK, while
    shared.parallels_service._cap_main_results_by_group (what the API path
    applies downstream, and what the page's direct call path applies NOT AT
    ALL) keeps rows by a DIFFERENT rule: top `render_cap` distinct sys_id
    GROUPS by summed score. With 12 raw hits across only 4 manuscripts and
    render_cap=2, the old code rendered highlights for raw ranks 0-1 only,
    while the group cap kept up to 2 WHOLE groups (6 rows) -- so kept rows
    past raw rank 2 came back with empty text. Fixed: this searcher now
    applies the SAME group cap internally and returns ONLY the rows it
    rendered, so "rendered == kept" holds for ANY caller (the API's own
    downstream cap becomes a no-op; the page's direct call, which applies no
    cap of its own, is bounded for the first time).
    """
    idx, originals, motif = grouped_corpus
    capped_searcher = PassageSearcher(
        index=idx, text_fetcher=_FakeTextFetcher(originals), render_cap=2)
    result = capped_searcher.search_composition_logic(full_text=motif)

    assert result['main'], 'fixture precondition: at least one hit'
    # Capped to (at most) 2 GROUPS, not 2 ROWS -- with 3 pages/group that is
    # up to 6 rows, strictly fewer than all 12 raw hits (proves the cap
    # actually fired, not merely that few hits existed).
    kept_sys_ids = {_extract_sys_id(r['raw_header']) for r in result['main']}
    assert 1 <= len(kept_sys_ids) <= 2, kept_sys_ids
    assert len(result['main']) < 12, 'the group cap did not actually truncate anything'

    # The actual bug this fix closes: EVERY row this searcher RETURNS must
    # be rendered -- never a kept-but-blank row.
    for row in result['main']:
        assert row['text'] != '', f"row {row['raw_header']} was kept but never rendered"
        assert row['source_ctx'] != '', f"row {row['raw_header']} has no source_ctx"
        assert row['chunk_hits'], f"row {row['raw_header']} has no chunk_hits"

    # Uncapped (render_cap >= raw hit count): every one of the 12 rows comes
    # back, all rendered -- the cap logic is a genuine no-op when it need
    # not fire, not a hidden hard ceiling.
    uncapped_searcher = PassageSearcher(
        index=idx, text_fetcher=_FakeTextFetcher(originals), render_cap=200)
    uncapped_result = uncapped_searcher.search_composition_logic(full_text=motif)
    assert len(uncapped_result['main']) == 12
    assert all(r['text'] for r in uncapped_result['main'])


def test_chunk_index_is_comparable_across_records(grouped_corpus):
    """Finding #4: chunk_index is the query-side span's ordinal position
    among ALL distinct span starts for this query, not a per-record-local
    counter -- two different records matching the SAME query region must
    carry the SAME chunk_index (mirrors the incumbent's sliding-window
    index, which is shared across every Tantivy hit for that chunk)."""
    idx, originals, motif = grouped_corpus
    searcher_ = PassageSearcher(index=idx, text_fetcher=_FakeTextFetcher(originals))
    result = searcher_.search_composition_logic(full_text=motif)
    assert len(result['main']) >= 2, 'need >=2 records to compare chunk_index across'

    indices_seen = set()
    for row in result['main']:
        assert row['chunk_hits'], row['raw_header']
        # Every record's sole span covers the whole motif at query offset 0.
        indices_seen.add(row['chunk_hits'][0][0])
    assert indices_seen == {0}, (
        f'records matching the SAME query region got DIFFERENT chunk_index '
        f'values {indices_seen} -- they should all share the SAME ordinal')


# ---------------------------------------------------------------------------
# 4. Finding #2: boundary_mode other than 'full' raises.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('bad_mode', ['boundary', 'combined', 'anything-else'])
def test_boundary_mode_other_than_full_raises(searcher, synthetic_corpus, bad_mode):
    """Silent degradation ('boundary'/'combined' quietly treated as 'full')
    is exactly the failure mode this project's fail-closed posture exists to
    prevent -- PassageSearcher raises instead, so a caller that skips its own
    validation (web/search_api.py's step 4b; web/pages/parallels.py's
    disabled control) still cannot get a silently-wrong search."""
    _idx, _originals, motif = synthetic_corpus
    with pytest.raises(ValueError):
        searcher.search_composition_logic(full_text=motif, boundary_mode=bad_mode)


def test_boundary_mode_full_does_not_raise(searcher, synthetic_corpus):
    _idx, _originals, motif = synthetic_corpus
    result = searcher.search_composition_logic(full_text=motif, boundary_mode='full')
    assert result['main']


# ---------------------------------------------------------------------------
# 5. Finding #3: filter_text parity with SearchEngine.search_composition_logic.
# ---------------------------------------------------------------------------

def test_filter_text_routes_matching_records_to_filtered(searcher, synthetic_corpus):
    """Mirrors SearchEngine.search_composition_logic's filter_text
    semantics (shared/search_engine.py): if the query-side matched text ALSO
    appears in filter_text (the page's "Filter Sources" text -- known/
    printed source texts), the WHOLE record is routed to `filtered`, not
    `main` -- per-RECORD granularity, not per-span."""
    _idx, _originals, motif = synthetic_corpus
    everyone = searcher.search_composition_logic(full_text=motif)
    assert everyone['main'], 'fixture precondition: at least one hit'
    assert everyone['filtered'] == []

    # filter_text contains the exact motif -> the sole span on the hit
    # matches it verbatim.
    filtered_result = searcher.search_composition_logic(
        full_text=motif, filter_text=motif)
    assert filtered_result['main'] == []
    assert filtered_result['filtered'], 'filter_text match did not route to filtered'
    # filtered rows are NOT capped, but they ARE still rendered (matching
    # the incumbent, which builds full text for every row regardless of
    # is_filtered).
    assert all(r['text'] for r in filtered_result['filtered'])


def test_filter_text_non_matching_leaves_main_untouched(searcher, synthetic_corpus):
    _idx, _originals, motif = synthetic_corpus
    unrelated_filter_text = _aperiodic(500, salt=9999)
    result = searcher.search_composition_logic(
        full_text=motif, filter_text=unrelated_filter_text)
    assert result['main'], 'unrelated filter_text must not filter real matches'
    assert result['filtered'] == []


# ---------------------------------------------------------------------------
# 6. Finding #5: literal '*' in manuscript text is sanitized.
# ---------------------------------------------------------------------------

def test_highlight_span_sanitizes_literal_asterisk(tmp_path_factory):
    """A manuscript page containing a literal '*' must not produce a
    spurious marker -- mirrors SearchEngine.highlight's
    `snippet.replace('*', ' ')` precedent (shared/search_engine.py)."""
    d = str(tmp_path_factory.mktemp('ppar_asterisk'))
    motif = _aperiodic(80, salt=42)
    rid = _record_id(999)
    body_before = _aperiodic(30, salt=43)
    body_after = _aperiodic(30, salt=44)
    # A literal asterisk sitting INSIDE the +-60-char highlight window,
    # immediately before the matched motif.
    text = f"{body_before}*{motif}{body_after}"
    records = [(rid, text)]
    build_index(records, d, partitions=1, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None

    searcher_ = PassageSearcher(index=idx, text_fetcher=_FakeTextFetcher({rid: text}))
    result = searcher_.search_composition_logic(full_text=motif)
    row = next((r for r in result['main'] if r['raw_header'] == rid), None)
    assert row is not None, 'the motif carrier was not found'
    assert row['text']
    # Exactly the TWO markers _highlight_span itself inserts -- the literal
    # '*' from the source text must have been sanitized away, not counted as
    # a THIRD, spurious marker.
    assert row['text'].count('*') == 2, (
        f"a literal '*' in the manuscript text was not sanitized: {row['text']!r}")


# ---------------------------------------------------------------------------
# 7. Finding #6: get_full_text_by_header rejects non-conforming input.
# ---------------------------------------------------------------------------

def test_get_full_text_by_header_rejects_non_conforming_input():
    """`full_header` is validated against `^[A-Za-z0-9_]+$` BEFORE being
    interpolated into the Tantivy phrase query -- a value outside that set
    is rejected without ever touching `self.index`/`self.searcher`, so this
    works against a bare, un-initialized instance (no real Tantivy index
    needed to exercise the validation path)."""
    from shared.search_engine import SearchEngine
    engine = SearchEngine.__new__(SearchEngine)
    for bad in ('bad header', 'quote"injection', 'semi;colon', '',
                None, 'has space', 'a\nb', 'a/b', "a'; DROP TABLE x; --"):
        assert engine.get_full_text_by_header(bad) is None, repr(bad)


# ---------------------------------------------------------------------------
# 8. Executor dispatch (finding #8: `method` removed from
#    fetch_parallels_results -- `executor` alone decides now).
# ---------------------------------------------------------------------------

def test_run_sync_executor_kwarg_selects_the_dispatch_pool():
    """Direct test of the mechanism `fetch_parallels_results`'s `executor`
    param relies on: `_run_sync(func, _executor=None)` dispatches on the
    default pool; `_run_sync(func, _executor=<pool>)` dispatches on THAT
    pool. Verified by thread name (real executors, no asyncio mocking)."""
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


def test_fetch_parallels_results_executor_kwarg_is_honored():
    """Finding #8: `method` is gone from `fetch_parallels_results` --
    `executor` ALONE decides dispatch, unconditionally (previously honored
    only when a now-removed `method == 'passage'`)."""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from unittest.mock import MagicMock

    from shared.parallels_service import fetch_parallels_results

    names = {}

    class _CapturingEngine:
        def search_composition_logic(self, **_kwargs):
            names['thread'] = threading.current_thread().name
            return {'main': [], 'filtered': []}

    meta_mgr = MagicMock()
    custom = ThreadPoolExecutor(max_workers=1, thread_name_prefix='fpr-test-pool')
    try:
        asyncio.run(fetch_parallels_results(
            searcher=_CapturingEngine(), meta_mgr=meta_mgr, text='hello',
            chunk_size=5, mode='exact', executor=custom,
        ))
    finally:
        custom.shutdown(wait=True)
    assert 'fpr-test-pool' in names['thread']


def test_fetch_parallels_results_omitted_executor_uses_default_pool():
    """Omitting `executor` (the chunk path's call shape, unchanged) does NOT
    dispatch on any named pool -- preserves pre-Phase-145 behavior exactly."""
    import threading
    from unittest.mock import MagicMock

    from shared.parallels_service import fetch_parallels_results

    names = {}

    class _CapturingEngine:
        def search_composition_logic(self, **_kwargs):
            names['thread'] = threading.current_thread().name
            return {'main': [], 'filtered': []}

    meta_mgr = MagicMock()
    asyncio.run(fetch_parallels_results(
        searcher=_CapturingEngine(), meta_mgr=meta_mgr, text='hello',
        chunk_size=5, mode='exact',
    ))
    assert 'fpr-test-pool' not in names['thread']


def test_fetch_parallels_results_rejects_method_kwarg():
    """`method` no longer exists on this signature (finding #8) -- a caller
    still passing it must get a clear TypeError, not silent absorption into
    the removed parameter."""
    from unittest.mock import MagicMock

    from shared.parallels_service import fetch_parallels_results

    engine = MagicMock()
    meta_mgr = MagicMock()

    async def _run():
        await fetch_parallels_results(
            searcher=engine, meta_mgr=meta_mgr, text='hello',
            chunk_size=5, mode='exact', method='chunk',
        )

    with pytest.raises(TypeError):
        asyncio.run(_run())


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
