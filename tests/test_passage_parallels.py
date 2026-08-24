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
    # 'dropped_text_lookup_failures' (finding #16(b)) is ADDITIVE -- existing
    # consumers that only read 'main'/'filtered' (shared/parallels_service.py
    # via .get()) are unaffected by its presence.
    assert set(result.keys()) == {
        'main', 'filtered', 'dropped_text_lookup_failures',
        # PR #324 round 3, both additive for the same reason:
        'duplicate_photography_demoted', 'query_report',
        # PR #324 round 5: the group-cap flag, previously discarded.
        'truncated_to_200',
    }
    assert result['filtered'] == []
    assert result['dropped_text_lookup_failures'] == 0
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


def test_filtered_bucket_is_group_capped_too(grouped_corpus):
    """Finding #16(a): the incumbent's OWN filtered bucket is documented as
    "typically small" (driven by max_freq) and left uncapped on that
    assumption -- passage's filtered bucket is driven by filter_text
    substring matches instead, which has no such size guarantee. It must be
    capped by the SAME group-cap rule as main, not left to grow to the
    verify_cap ceiling (3,000) with one live Tantivy lookup each."""
    idx, originals, motif = grouped_corpus
    capped_searcher = PassageSearcher(
        index=idx, text_fetcher=_FakeTextFetcher(originals), render_cap=2)
    # filter_text == the motif itself -> every one of the 12 hits' sole span
    # matches it, so ALL 12 would land in `filtered` if it were uncapped.
    result = capped_searcher.search_composition_logic(
        full_text=motif, filter_text=motif)
    assert result['main'] == []
    assert result['filtered'], 'fixture precondition: at least one filtered hit'

    kept_sys_ids = {_extract_sys_id(r['raw_header']) for r in result['filtered']}
    assert 1 <= len(kept_sys_ids) <= 2, kept_sys_ids
    assert len(result['filtered']) < 12, (
        'the filtered bucket was not actually capped -- all 12 hits came back')
    # And "rendered == kept" holds for filtered rows too.
    for row in result['filtered']:
        assert row['text'], f"filtered row {row['raw_header']} was kept but never rendered"
        assert row['chunk_hits']


# ---------------------------------------------------------------------------
# Finding #16(b): a failed text lookup drops-and-counts the row, never a
# silent blank one.
# ---------------------------------------------------------------------------

class _FlakyTextFetcher:
    """Like _FakeTextFetcher, but returns None for one chosen record_id --
    simulating a failed display-text lookup on an otherwise-healthy search."""

    def __init__(self, mapping: dict, fail_for: str):
        self._mapping = mapping
        self._fail_for = fail_for

    def get_full_text_by_header(self, full_header: str):
        if full_header == self._fail_for:
            return None
        return self._mapping.get(full_header)


def test_failed_text_lookup_drops_and_counts_the_row_not_blank(grouped_corpus):
    """Exercises the FULL render path (real synthetic index, real
    search_passage call, real group cap) with a text_fetcher that fails for
    exactly one record: that row must be ABSENT from the output (never a
    blank-text row masquerading as a real result) and counted in
    dropped_text_lookup_failures."""
    idx, originals, motif = grouped_corpus
    failing_record_id = _grouped_record_id(0, 0)
    flaky_fetcher = _FlakyTextFetcher(originals, fail_for=failing_record_id)
    searcher_ = PassageSearcher(index=idx, text_fetcher=flaky_fetcher, render_cap=200)

    result = searcher_.search_composition_logic(full_text=motif)

    all_raw_headers = {r['raw_header'] for r in result['main']} | \
        {r['raw_header'] for r in result['filtered']}
    assert failing_record_id not in all_raw_headers, (
        'the row with a failed text lookup must be DROPPED, not returned blank')
    # No blank rows anywhere -- every returned row (main or filtered) is
    # fully rendered, per "rendered == kept."
    for row in result['main'] + result['filtered']:
        assert row['text'] != '', f"row {row['raw_header']} has blank text"

    assert result['dropped_text_lookup_failures'] == 1, (
        f"expected exactly 1 dropped row, got "
        f"{result['dropped_text_lookup_failures']}")
    # Without the fix, this record would have appeared in `main` with
    # text=='' -- one fewer row overall is the visible, counted difference.
    assert len(result['main']) == 11


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


def test_load_passage_state_flag_off_never_touches_the_filesystem(tmp_path, monkeypatch):
    """Codex review finding #14: the flag must be checked FIRST, before
    anything else runs. open_index() memory-maps multi-GB files and reads
    real bytes during its own validation -- unconditionally calling it
    (with only a LATER, separate passage_available() check ANDing the flag
    in) defeats the flag's entire operational purpose on a memory-
    constrained production host. This test proves open_index is never even
    CALLED when the flag is off, regardless of what sits at PASSAGE_DATA_DIR
    -- not merely that the end result happens to read as unavailable.

    Records calls in a list rather than raising from the probe: an earlier
    version of this test raised instead, and load_passage_state's own
    `except Exception` around the open_index call silently swallowed that
    raise -- both "never called" (the fix) and "called, then its exception
    was caught" (the bug) produced the identical ready=False outcome, so a
    raising probe could not actually tell the two apart. Verified in this
    session: reverting the fix (removing the flag-first check) made THIS
    version of the test fail (calls list non-empty) while the raising
    version still passed -- the raising version was proven vacuous first.
    """
    import web.passage_assets as passage_assets
    import shared.passage_index as passage_index_module

    calls: list = []

    def _record_call(*args, **kwargs):
        calls.append((args, kwargs))
        raise RuntimeError('open_index reached (test probe) -- see calls list')

    monkeypatch.setattr(passage_index_module, 'open_index', _record_call)
    monkeypatch.setattr(passage_assets, 'PASSAGE_PARALLELS_ENABLED', False)

    # A "real-looking" directory -- present, non-empty, with a manifest --
    # so a pass here proves the flag check happens before ANY inspection of
    # what is at PASSAGE_DATA_DIR, not merely that a missing directory
    # degrades gracefully.
    fake_index_dir = tmp_path / 'real-looking-index'
    fake_index_dir.mkdir()
    (fake_index_dir / 'manifest.json').write_text('{}', encoding='utf-8')
    monkeypatch.setattr(passage_assets, 'PASSAGE_DATA_DIR', str(fake_index_dir))

    ready = passage_assets.load_passage_state()
    assert calls == [], f'open_index was called {len(calls)} time(s) while the flag was off'
    assert ready is False
    assert passage_assets.passage_available() is False
    assert passage_assets.get_passage_searcher(text_fetcher=None) is None


# ---------------------------------------------------------------------------
# PR #324 round 3, P1 pair: the discarded QueryReport, and the never-invoked
# duplicate-photography hygiene.
# ---------------------------------------------------------------------------

def test_query_report_is_returned_not_discarded(searcher, synthetic_corpus):
    """QueryReport's own contract says it 'ships in the result envelope -- a
    truncated search that does not say so is a correctness defect'. The
    searcher bound it to `_report` and threw it away."""
    _idx, originals, motif = synthetic_corpus
    result = searcher.search_composition_logic(full_text=motif)
    rep = result.get('query_report')
    assert rep, 'query_report missing from the searcher return dict'
    assert rep['policy_id'] == searcher.policy.policy_id
    assert rep['query_letters'] > 0
    assert 'verify_truncated' in rep and 'candidates_truncated' in rep


def test_a_truncated_search_says_so(grouped_corpus):
    """With verify_cap=1 the verifier MUST truncate on a multi-candidate
    query, and the report must carry that fact out. Uses grouped_corpus (12
    records all carrying the motif -> 12 candidates against a cap of 1);
    synthetic_corpus has ONE carrier, so nothing there can ever truncate."""
    from shared.passage_policy import PassagePolicy

    idx, originals, motif = grouped_corpus
    tight = PassageSearcher(
        index=idx, text_fetcher=_FakeTextFetcher(originals),
        policy=PassagePolicy(name='tight-rt', verify_cap=1))
    result = tight.search_composition_logic(full_text=motif)
    assert result['query_report']['verify_truncated'] is True, (
        'the verify cap fired but the report does not say so'
    )


def _lines(n_lines: int, salt: int, width: int = 20) -> list:
    return [_aperiodic(width, salt=salt * 100 + i) for i in range(n_lines)]


@pytest.fixture(scope='module')
def dup_corpus(tmp_path_factory):
    """Three multi-LINE records (line breaks are what the detector reads):

      dupA, dupB -- byte-identical 6-line text: the same physical page
                    photographed under two catalogue records.
      ctrl       -- shares 3 of its 8 lines with the query (a genuine
                    parallel quoting the same passage), everything else
                    different: line agreement 3/8 = 0.375 < 0.60, must KEEP.
    """
    d = str(tmp_path_factory.mktemp('ppar_dup'))
    page_lines = _lines(6, salt=41)
    shared = page_lines[:3]                    # 60 letters -- the query
    ctrl_lines = shared + _lines(5, salt=97)   # 3 shared of 8

    ids = {'a': f"99{1:08d}_IE{77_000_001}_P{1:07d}_FL1",
           'b': f"99{2:08d}_IE{77_000_002}_P{1:07d}_FL1",
           'c': f"99{3:08d}_IE{77_000_003}_P{1:07d}_FL1"}
    originals = {ids['a']: '\n'.join(page_lines),
                 ids['b']: '\n'.join(page_lines),
                 ids['c']: '\n'.join(ctrl_lines)}
    build_index(sorted(originals.items()), d, partitions=2,
                apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, originals, '\n'.join(shared), ids


def test_duplicate_photography_is_demoted_not_deleted(dup_corpus):
    idx, originals, query, ids = dup_corpus
    s = PassageSearcher(index=idx, text_fetcher=_FakeTextFetcher(originals))
    result = s.search_composition_logic(full_text=query)

    main_ids = {r['raw_header'] for r in result['main']}
    filt = [r for r in result['filtered']
            if r.get('filter_reason') == 'duplicate_photography']

    # Exactly one of the two photographs survives in main...
    assert len({ids['a'], ids['b']} & main_ids) == 1, (
        f'expected exactly one photograph of the page in main, got '
        f'{sorted(main_ids)}'
    )
    # ...the other is DEMOTED (reachable, rendered) -- never deleted.
    assert len(filt) == 1 and result['duplicate_photography_demoted'] == 1
    assert filt[0]['text'], 'the demoted row must stay rendered'
    # The genuine parallel that merely QUOTES the same passage is kept:
    # its line agreement is 3/8, far under the 0.60 threshold.
    assert ids['c'] in main_ids, (
        'a genuine parallel sharing only the quoted lines was wrongly '
        'demoted as duplicate photography'
    )


# ---------------------------------------------------------------------------
# Owner ruling 2026-08-23: the web GUI searches at widest-40 (ds=1.8).
# ---------------------------------------------------------------------------

def test_the_web_surface_searches_at_widest_40(synthetic_corpus, monkeypatch):
    """Decided on two live GUI case studies graded row-by-row by the owner:
    the default (standard-40) surfaced 13 of his 28 verified manuscripts on
    the Yom Shabbaton query; widest-40 (density_scale 1.8) surfaced 26 -- and
    2.0 is the measured cliff. The web surface must opt in EXPLICITLY while
    DEFAULT_POLICY stays standard-40 for evaluation tooling, so this pins the
    wiring in web/passage_assets.py::get_passage_searcher, not the library
    default."""
    import web.passage_assets as pa

    idx, originals, _motif = synthetic_corpus
    monkeypatch.setattr(pa, 'PASSAGE_PARALLELS_ENABLED', True)
    monkeypatch.setattr(pa, '_state', pa._PassageState(ready=True, index=idx))

    s = pa.get_passage_searcher(_FakeTextFetcher(originals))
    assert s is not None
    assert s.policy.name == 'widest-40'
    assert s.policy.density_scale == 1.8

    # And the library default did NOT silently move with it.
    from shared.passage_policy import DEFAULT_POLICY
    assert DEFAULT_POLICY.name == 'standard-40'


# ---------------------------------------------------------------------------
# PR #324 round 5, P1 pair: restriction must precede the caps, and the
# group-cap truncation flag must survive to the caller.
# ---------------------------------------------------------------------------

def test_restriction_is_applied_before_the_caps(grouped_corpus):
    """restrict_sys_ids used to filter HITS, after candidate_cap and
    verify_cap were spent globally -- so on a common text, out-of-set
    candidates consumed the caps and in-set witnesses came back as false
    negatives that look exactly like absence of evidence.

    grouped_corpus: 4 sys_id groups x 3 pages, every page carrying the same
    motif. With verify_cap=1 only ONE candidate is ever verified; unless the
    restriction reaches the engine, that one slot goes to the globally
    strongest candidate, which is almost surely not in the restricted set.
    """
    from shared.passage_policy import PassagePolicy

    idx, originals, motif = grouped_corpus
    tight = PassageSearcher(
        index=idx, text_fetcher=_FakeTextFetcher(originals),
        policy=PassagePolicy(name='tight-restrict', verify_cap=1))

    # The set the caller restricts to: group 3 only.
    want_sys = _grouped_record_id(3, 0).split('_', 1)[0]

    unrestricted = tight.search_composition_logic(full_text=motif)
    took_slot = unrestricted['main'][0]['raw_header'].split('_', 1)[0]
    assert took_slot != want_sys, (
        'fixture cannot observe the defect: the globally strongest candidate '
        'is already in the restricted set'
    )

    res = tight.search_composition_logic(full_text=motif,
                                         restrict_sys_ids={want_sys})
    got = {r['raw_header'].split('_', 1)[0] for r in res['main']}
    assert got == {want_sys}, (
        f'restricted search returned {sorted(got) or "nothing"} -- the caps '
        f'were spent on records the caller excluded'
    )
    assert res['query_report']['candidates_restricted'] > 0, (
        'the engine did not count the restriction -- it is still filtering '
        'hits after the caps'
    )


def test_group_cap_truncation_reaches_the_return_dict(grouped_corpus):
    """The flag was computed and DISCARDED: the API path re-applied the same
    cap to an already-capped list, saw no truncation, and a >render_cap query
    silently looked complete."""
    idx, originals, motif = grouped_corpus
    s = PassageSearcher(index=idx, text_fetcher=_FakeTextFetcher(originals),
                        render_cap=2)   # 4 groups match -> truncation
    res = s.search_composition_logic(full_text=motif)
    assert res['truncated_to_200'] is True
    untr = PassageSearcher(index=idx,
                           text_fetcher=_FakeTextFetcher(originals),
                           render_cap=200).search_composition_logic(
        full_text=motif)
    assert untr['truncated_to_200'] is False


def test_service_ORs_the_searcher_truncation_flag():
    """fetch_parallels_results re-caps an already-capped list, so its own
    flag is False; the searcher's flag must carry through the bundle."""
    import asyncio

    from shared import parallels_service as ps

    class _Searcher:
        def search_composition_logic(self, *a, **k):
            return {'main': [{'uid': 'a', 'raw_header': '111_IE1_P000001_FL1',
                              'src_lbl': '', 'source_ctx': '', 'text': 't',
                              'score': 9, 'final_score': 9, 'chunk_count': 1,
                              'chunk_hits': []}],
                    'filtered': [], 'truncated_to_200': True}

    class _MM:
        def parse_full_id_components(self, uid):
            return None

    bundle = asyncio.run(ps.fetch_parallels_results(
        searcher=_Searcher(), meta_mgr=_MM(), text='t',
        chunk_size=3, mode='exact'))
    assert bundle.truncated_to_200 is True, (
        "the searcher's truncation flag was dropped between the return dict "
        'and the bundle'
    )


def test_the_web_width_control_reaches_the_policy(synthetic_corpus, monkeypatch):
    """Owner ruling 2026-08-23 (letter-level controls): the page's Match-width
    select passes a preset name into get_passage_searcher; an unknown name
    must raise, never silently fall back to a different width."""
    import web.passage_assets as pa

    idx, originals, _motif = synthetic_corpus
    monkeypatch.setattr(pa, 'PASSAGE_PARALLELS_ENABLED', True)
    monkeypatch.setattr(pa, '_state', pa._PassageState(ready=True, index=idx))

    s = pa.get_passage_searcher(_FakeTextFetcher(originals), preset='wide-40')
    assert s.policy.name == 'wide-40' and s.policy.density_scale == 1.3

    with pytest.raises(Exception):
        pa.get_passage_searcher(_FakeTextFetcher(originals), preset='slider-17')


# ---------------------------------------------------------------------------
# Owner ruling 2026-08-23 (Birkat Hamazon session): the page path is UNCAPPED.
# Display batching pages it; the export layer bounds it. The engine's group
# cap hid 299 of 497 found manuscripts from both surfaces.
# ---------------------------------------------------------------------------

def test_render_cap_zero_returns_every_group_fully_rendered(grouped_corpus):
    idx, originals, motif = grouped_corpus   # 4 sys_id groups, all matching
    capped = PassageSearcher(index=idx,
                             text_fetcher=_FakeTextFetcher(originals),
                             render_cap=2).search_composition_logic(
        full_text=motif)
    uncapped = PassageSearcher(index=idx,
                               text_fetcher=_FakeTextFetcher(originals),
                               render_cap=0).search_composition_logic(
        full_text=motif)

    def groups(res):
        return {r['raw_header'].split('_', 1)[0] for r in res['main']}

    assert len(groups(capped)) == 2 and capped['truncated_to_200'] is True
    assert len(groups(uncapped)) == 4, 'uncapped must return every group'
    assert uncapped['truncated_to_200'] is False, (
        'nothing is truncated by definition when the cap is off'
    )
    # rendered == kept must hold over the FULL set, not just the first 200:
    # a blank row past the old cap boundary would be the silent-blank defect
    # (finding #1) reintroduced at scale.
    assert all(r['text'] for r in uncapped['main']), (
        'an uncapped row was returned unrendered'
    )


def test_page_requests_the_uncapped_searcher(synthetic_corpus, monkeypatch):
    """The page passes render_cap=0; the API path keeps the searcher default
    (200-group envelope contract). Both pinned, because a future refactor
    that drops the page's kwarg silently reinstates the hidden-manuscripts
    behaviour the owner measured and ruled out."""
    import web.passage_assets as pa

    idx, originals, _motif = synthetic_corpus
    monkeypatch.setattr(pa, 'PASSAGE_PARALLELS_ENABLED', True)
    monkeypatch.setattr(pa, '_state', pa._PassageState(ready=True, index=idx))

    api_default = pa.get_passage_searcher(_FakeTextFetcher(originals))
    from shared.parallels_service import PARALLELS_GROUP_CAP
    assert api_default.render_cap == PARALLELS_GROUP_CAP

    page_style = pa.get_passage_searcher(_FakeTextFetcher(originals),
                                         render_cap=0)
    assert page_style.render_cap == 0

    src = open('web/pages/parallels.py', encoding='utf-8').read()
    # Pin the CALL, not the file: an explanatory comment also contains the
    # substring 'render_cap=0', so a bare `in src` stayed green when the
    # actual kwarg was removed (mutation-caught in this session).
    call_at = src.index('passage_searcher = get_passage_searcher(')
    call = src[call_at:src.index(')', call_at) + 1]
    assert 'render_cap=0' in call, (
        "the page's get_passage_searcher CALL no longer passes render_cap=0 "
        "-- found manuscripts are hidden from display AND export again"
    )


# ---------------------------------------------------------------------------
# Anchor-evidence tier at the searcher level (spec section 10.4).
# ---------------------------------------------------------------------------

@pytest.fixture(scope='module')
def anchor_corpus(tmp_path_factory):
    """One verbatim carrier of the reference plus 'translation' records that
    share only three scattered 14-letter collocations with it -- the class
    the anchor tier exists to surface."""
    d = str(tmp_path_factory.mktemp('ppar_anchor'))
    reference = _aperiodic(400, salt=31)
    snippets = [reference[40:54], reference[180:194], reference[320:334]]
    originals: dict = {}
    records = []
    for r in range(8):
        rid = _record_id(90 + r)
        body = _aperiodic(360, salt=7000 + r)
        if r == 0:
            text = body[:100] + ' ' + reference + ' ' + body[100:]
        elif r < 4:
            text = (body[:90] + snippets[0] + body[90:200] + snippets[1]
                    + body[200:300] + snippets[2] + body[300:])
        else:
            text = body
        originals[rid] = text
        records.append((rid, text))
    build_index(records, d, partitions=2, apply_hygiene=False)
    idx = open_index(d)
    assert idx is not None
    return idx, originals, reference


def test_anchor_rows_are_labelled_scaled_and_last(anchor_corpus):
    from shared.passage_policy import get_preset
    idx, originals, reference = anchor_corpus
    s = PassageSearcher(index=idx, text_fetcher=_FakeTextFetcher(originals),
                        policy=get_preset('anchor-sweep-40'))
    out = s.search_composition_logic(reference)
    rows = out['main']
    span_rows = [r for r in rows if r['match_tier'] == 'span']
    anchor_rows = [r for r in rows if r['match_tier'] == 'anchor']
    assert span_rows and anchor_rows
    # The report says the tier ran and how many records it added.
    assert out['query_report']['anchor_tier_enabled'] is True
    assert out['query_report']['anchor_records'] == len(anchor_rows)
    # Anchor row scores sort BELOW every span row under the page's plain
    # score sort -- the tiers must never interleave (different units).
    assert min(r['score'] for r in span_rows) > \
        max(r['score'] for r in anchor_rows)
    for r in anchor_rows:
        assert r['src_lbl'] == 'עדות עוגן (anchor evidence)'
        assert r['anchor_codes'] >= 8
        assert r['score'] == pytest.approx(r['anchor_codes'] / 1000.0)
        # Rendered like any row: highlighted windows on both sides.
        assert r['source_ctx'] and r['text']
    # Span rows keep the incumbent shape untouched.
    for r in span_rows:
        assert r['src_lbl'] == ''
        assert r['anchor_codes'] == 0


def test_default_policy_rows_carry_no_anchor_tier(anchor_corpus):
    idx, originals, reference = anchor_corpus
    s = PassageSearcher(index=idx, text_fetcher=_FakeTextFetcher(originals))
    out = s.search_composition_logic(reference)
    assert out['query_report']['anchor_tier_enabled'] is False
    assert all(r['match_tier'] == 'span' for r in out['main'])
