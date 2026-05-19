# -*- coding: utf-8 -*-
"""Regression tests for the export payload size cap.

Background (2026-05-18 forensic capture): a single user's
``app.storage.user['export_search_payload']`` held a 31,572-result list
weighing 498 MB. NiceGUI rehydrated that into RAM on every session load,
producing the ~5 GB Python-heap growth that drove the P1 web memory leak.
The fix caps every export-storage mutator at ``_EXPORT_RESULTS_CAP``
(5,000 results) and emits ``truncated`` / ``total_count`` metadata so
downstream UX can advise the user.

These tests assert the cap is applied on all four write paths:
  - set_search_export
  - update_search_export_results
  - set_parallels_export (both ``results`` and ``filtered`` lists)
  - update_parallels_export_filtered

Pattern matches tests/test_export_state_selection.py: monkeypatch
``web.safe_storage.app`` with an instance-isolated SimpleNamespace stub
whose ``storage.user`` is a fresh dict (Phase 88 Refinement 6).
"""
from types import SimpleNamespace
import json
import sys



def _make_stub(initial_storage: dict):
    return SimpleNamespace(storage=SimpleNamespace(user=initial_storage))


def _result(i: int) -> dict:
    """Build a result dict roughly matching the production shape so the cap
    is exercised against realistic data (uid keys, etc.)."""
    return {
        'uid': f'u{i}',
        'display': {'shelfmark': f'T-S 12.{i}', 'id': str(i)},
        'snippet': f'match {i}',
        'full_text': 'x' * 100,
        'score': 0.5,
    }


def _json_size(value) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode('utf-8'))


# ---------------------------------------------------------------------------
# set_search_export
# ---------------------------------------------------------------------------

def test_set_search_export_caps_oversized_list(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    big = [_result(i) for i in range(10_000)]
    export_state.set_search_export(results=big, query='ה*')

    payload = storage['export_search_payload']
    assert len(payload['results']) == export_state._EXPORT_RESULTS_CAP
    assert payload['truncated'] is True
    assert payload['total_count'] == 10_000
    # The cap must keep the first N (relevance-ordered), not a random slice.
    assert payload['results'][0]['uid'] == 'u0'
    assert payload['results'][-1]['uid'] == f'u{export_state._EXPORT_RESULTS_CAP - 1}'


def test_set_search_export_passes_small_list_through(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    small = [_result(i) for i in range(100)]
    export_state.set_search_export(results=small, query='foo')

    payload = storage['export_search_payload']
    assert len(payload['results']) == 100
    assert payload['truncated'] is False
    assert payload['total_count'] == 100
    assert 'full_text' not in payload['results'][0]
    # SEED-002 uid-only: full_text_excerpt is no longer kept on compacted rows.
    assert 'full_text_excerpt' not in payload['results'][0]


def test_set_search_export_handles_empty_and_non_list(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    export_state.set_search_export(results=[], query='')
    p = storage['export_search_payload']
    assert p['results'] == []
    assert p['truncated'] is False
    assert p['total_count'] == 0

    # Non-list defensive degradation (e.g., if a caller ever passes None).
    export_state.set_search_export(results=None, query='')  # type: ignore[arg-type]
    p = storage['export_search_payload']
    assert p['results'] == []
    assert p['truncated'] is False
    assert p['total_count'] == 0


def test_set_search_export_strips_heavy_text_fields_even_for_few_results(monkeypatch):
    """A small result count can still be huge when rows carry full manuscripts."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    rows = []
    for i in range(35):
        row = _result(i)
        row['full_text'] = 'x' * 500_000
        row['raw_file_hl'] = 'y' * 500_000
        rows.append(row)

    export_state.set_search_export(results=rows, query='foo')

    payload = storage['export_search_payload']
    assert len(payload['results']) == 35
    assert _json_size(payload) < 100_000
    assert all('full_text' not in r for r in payload['results'])
    assert all('raw_file_hl' not in r for r in payload['results'])
    # SEED-002 uid-only: full_text_excerpt no longer kept (was 500-char prefix
    # in ed6f89c4; rehydrate via Tantivy at export time instead).
    assert all('full_text_excerpt' not in r for r in payload['results'])


# ---------------------------------------------------------------------------
# update_search_export_results
# ---------------------------------------------------------------------------

def test_update_search_export_results_caps_and_preserves_other_fields(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    # Establish a baseline payload via the helper (caps to 100, untruncated).
    export_state.set_search_export(
        results=[_result(i) for i in range(100)],
        query='original-query',
        mode='responsa',
        gap=3,
        filters={'library': 'CUL'},
        warnings=['hello'],
        selected_uids=['u5'],
    )

    # Re-write with an oversized list (e.g. refinement undo).
    big = [_result(i) for i in range(10_000)]
    export_state.update_search_export_results(big)

    payload = storage['export_search_payload']
    assert len(payload['results']) == export_state._EXPORT_RESULTS_CAP
    assert payload['truncated'] is True
    assert payload['total_count'] == 10_000
    # All other envelope fields must be preserved.
    assert payload['query'] == 'original-query'
    assert payload['mode'] == 'responsa'
    assert payload['gap'] == 3
    assert payload['filters'] == {'library': 'CUL'}
    assert payload['warnings'] == ['hello']
    assert payload['selected_uids'] == ['u5']
    assert 'full_text' not in payload['results'][0]


def test_update_search_export_results_noops_when_payload_missing(monkeypatch):
    from web import export_state

    # Empty storage -- update with nothing to update should be a silent no-op,
    # not a fresh write (matches the existing Phase 88 D-11 contract).
    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    export_state.update_search_export_results([_result(i) for i in range(10_000)])
    assert 'export_search_payload' not in storage


# ---------------------------------------------------------------------------
# set_parallels_export
# ---------------------------------------------------------------------------

def test_set_parallels_export_caps_both_lists_independently(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    big_results = [_result(i) for i in range(8_000)]
    small_filtered = [_result(i) for i in range(50)]
    export_state.set_parallels_export(results=big_results, filtered=small_filtered)

    payload = storage['export_parallels_payload']
    assert len(payload['results']) == export_state._EXPORT_RESULTS_CAP
    assert len(payload['filtered']) == 50
    assert payload['truncated'] is True  # OR of the two
    assert payload['total_count'] == 8_000
    assert payload['filtered_total_count'] == 50


def test_set_parallels_export_strips_full_text_and_caps_chunk_hits(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    row = _result(1)
    row.update({
        'full_text': 'x' * 500_000,
        'content': 'y' * 500_000,
        'source_ctx': 's' * 10_000,
        'text': 't' * 10_000,
        'chunk_hits': [(0, 'a' * 5000, 10, 'b' * 5000)] * 200,
    })

    export_state.set_parallels_export(results=[row], filtered=[])

    stored = storage['export_parallels_payload']['results'][0]
    assert 'full_text' not in stored
    assert 'content' not in stored
    assert len(stored['source_ctx']) == export_state._PARALLELS_TEXT_STORAGE_CHARS
    assert len(stored['text']) == export_state._PARALLELS_TEXT_STORAGE_CHARS
    # SEED-002 uid-only: chunk_hits is no longer kept on compacted parallels rows.
    # Display fields rehydrate via meta_mgr at export time; chunk_hits were the
    # heaviest field in the legacy row schema (up to 100 KB worst case).
    assert 'chunk_hits' not in stored


def test_compact_parallels_result_rows_for_live_state():
    from web import export_state

    row = _result(1)
    row.update({'full_text': 'x' * 500_000, 'content': 'y' * 500_000})

    compacted = export_state.compact_parallels_result_rows([row])

    assert len(compacted) == 1
    assert 'full_text' not in compacted[0]
    assert 'content' not in compacted[0]


def test_compact_nicegui_export_storage_rewrites_legacy_payloads(tmp_path):
    from web import export_state

    storage_dir = tmp_path / '.nicegui'
    storage_dir.mkdir()
    legacy_payload = {
        'export_search_payload': {
            'results': [_result(1) | {'full_text': 'x' * 500_000, 'raw_file_hl': 'y' * 500_000}],
            'query': 'foo',
        }
    }
    storage_file = storage_dir / 'storage-user-legacy.json'
    storage_file.write_text(json.dumps(legacy_payload, ensure_ascii=False), encoding='utf-8')
    loaded_user = {
        'export_search_payload': {
            'results': [_result(2) | {'full_text': 'z' * 500_000}],
            'query': 'bar',
        }
    }
    storage = SimpleNamespace(path=storage_dir, _users={'loaded': loaded_user})

    summary = export_state.compact_nicegui_export_storage(storage)

    assert summary['files_compacted'] == 1
    assert summary['loaded_users_compacted'] == 1
    assert storage_file.stat().st_size < 20_000
    rewritten = json.loads(storage_file.read_text(encoding='utf-8'))
    stored_row = rewritten['export_search_payload']['results'][0]
    assert 'full_text' not in stored_row
    assert 'raw_file_hl' not in stored_row
    assert 'full_text' not in loaded_user['export_search_payload']['results'][0]


def test_set_parallels_export_clean_when_both_small(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    export_state.set_parallels_export(
        results=[_result(i) for i in range(10)],
        filtered=[_result(i) for i in range(5)],
    )
    payload = storage['export_parallels_payload']
    assert payload['truncated'] is False
    assert payload['total_count'] == 10
    assert payload['filtered_total_count'] == 5


# ---------------------------------------------------------------------------
# update_parallels_export_filtered
# ---------------------------------------------------------------------------

def test_update_parallels_export_filtered_caps_and_preserves_truncated_flag(monkeypatch):
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    # Initial state: results truncated, filtered small.
    export_state.set_parallels_export(
        results=[_result(i) for i in range(10_000)],  # capped, truncated=True
        filtered=[_result(i) for i in range(50)],
        meta={'source_text': 'x'},
    )
    assert storage['export_parallels_payload']['truncated'] is True

    # Patch filtered with an oversized list.
    export_state.update_parallels_export_filtered([_result(i) for i in range(7_000)])

    payload = storage['export_parallels_payload']
    assert len(payload['filtered']) == export_state._EXPORT_RESULTS_CAP
    assert payload['filtered_total_count'] == 7_000
    # truncated stays True (OR-merge with prior state).
    assert payload['truncated'] is True
    # meta is preserved (copy-on-update).
    assert payload['meta'] == {'source_text': 'x'}


def test_update_parallels_export_filtered_or_merges_from_clean_to_truncated(monkeypatch):
    """If the initial payload was clean but the patched list is oversized,
    truncated must flip True."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    export_state.set_parallels_export(
        results=[_result(i) for i in range(10)],   # clean
        filtered=[_result(i) for i in range(10)],  # clean
    )
    assert storage['export_parallels_payload']['truncated'] is False

    export_state.update_parallels_export_filtered([_result(i) for i in range(8_000)])
    assert storage['export_parallels_payload']['truncated'] is True
    assert storage['export_parallels_payload']['filtered_total_count'] == 8_000


# ---------------------------------------------------------------------------
# Module-level constant sanity
# ---------------------------------------------------------------------------

def test_cap_constant_is_reasonable():
    """If someone edits the constant in a way that defeats the purpose, fail
    loudly. 5K covers realistic export volumes; anything above ~20K starts to
    reproduce the original leak shape, anything below 1K starts breaking
    legitimate large exports."""
    from web import export_state
    assert 1000 <= export_state._EXPORT_RESULTS_CAP <= 20_000


def test_cap_results_helper_returns_correct_shape():
    from web import export_state
    capped, truncated, original = export_state._cap_results([_result(i) for i in range(3)])
    assert truncated is False
    assert original == 3
    assert len(capped) == 3

    capped, truncated, original = export_state._cap_results(
        [_result(i) for i in range(export_state._EXPORT_RESULTS_CAP + 1)]
    )
    assert truncated is True
    assert original == export_state._EXPORT_RESULTS_CAP + 1
    assert len(capped) == export_state._EXPORT_RESULTS_CAP

    # Non-list input.
    capped, truncated, original = export_state._cap_results(None)
    assert capped == [] and truncated is False and original == 0

    capped, truncated, original = export_state._cap_results('not-a-list')
    assert capped == [] and truncated is False and original == 0


# ---------------------------------------------------------------------------
# SEED-002: uid-only row schema invariants
# ---------------------------------------------------------------------------

def test_search_export_row_has_only_uid_keys(monkeypatch):
    """SEED-002: compacted search rows MUST be a subset of
    {uid, sort_score, snippet, match_terms}. All other fields
    (display, full_text, full_text_excerpt, raw_file_hl, content, score)
    are dropped — display fields rehydrate via meta_mgr at export time,
    full_text rehydrates via Tantivy."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    full_shape = [{
        'uid': '9912345678901234_IE1_P1_FL1',
        'sort_score': 0.95,
        'snippet': 'a *match* here',
        'match_terms': ['match', 'here'],
        # Fields that must be dropped:
        'display': {'shelfmark': 'T-S 12.345', 'title': 't', 'id': '99...', 'library_code': 'CUL'},
        'full_text': 'x' * 1000,
        'raw_file_hl': 'y' * 1000,
        'content': 'z' * 1000,
        'score': 0.5,  # search-side `score`, not parallels score
        'raw_header': 'header_991234',
    }]

    export_state.set_search_export(results=full_shape, query='foo')

    row = storage['export_search_payload']['results'][0]
    allowed = {'uid', 'sort_score', 'snippet', 'match_terms'}
    assert set(row.keys()) <= allowed
    # Explicit negative assertions for every field that must be stripped.
    for forbidden in ('display', 'full_text', 'full_text_excerpt',
                      'raw_file_hl', 'content', 'score', 'raw_header'):
        assert forbidden not in row, f"{forbidden} must not survive compaction"
    # Kept fields preserve values verbatim.
    assert row['uid'] == '9912345678901234_IE1_P1_FL1'
    assert row['sort_score'] == 0.95
    assert row['snippet'] == 'a *match* here'
    assert row['match_terms'] == ['match', 'here']


def test_parallels_export_row_keeps_safe_allowlist(monkeypatch):
    """SEED-002: compacted parallels rows MUST be a subset of
    {uid, sort_score, score, snippet, match_terms, source_ctx, text, raw_header}.

    CRITICAL #3 invariant: `score` and `raw_header` are INTENTIONALLY KEPT
    because (a) live parallels UI reads them at 13 sites in
    web/pages/parallels.py and (b) shared/search_serializer.py:691 sums
    `score` into the public /api/parallels aggregate_score. Dropping either
    field would silently break the UI or collapse aggregate_score to 0.0."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    full_shape = [{
        'uid': 'uid_a',
        'sort_score': 0.5,
        'score': 85.3,
        'snippet': 'snippet text',
        'match_terms': ['x', 'y'],
        'source_ctx': 'ctx',
        'text': 'manuscript text',
        'raw_header': 'header_9911111111111111_IE1_P3',
        # Fields that must be dropped:
        'display': {'shelfmark': 'T-S 1', 'title': 't', 'id': '9911', 'library_code': 'CUL'},
        'full_text': 'x' * 1000,
        'raw_file_hl': 'y' * 1000,
        'content': 'z' * 1000,
        'chunk_hits': [(0, 'a' * 1000, 10, 'b' * 1000)] * 50,
    }]

    export_state.set_parallels_export(results=full_shape, filtered=[])

    row = storage['export_parallels_payload']['results'][0]
    allowed = {'uid', 'sort_score', 'score', 'snippet', 'match_terms',
               'source_ctx', 'text', 'raw_header'}
    assert set(row.keys()) <= allowed
    # Critical retention invariants: score AND raw_header MUST be kept.
    assert row.get('score') == 85.3, "score MUST be kept (parallels UI + aggregate_score)"
    assert row.get('raw_header') == 'header_9911111111111111_IE1_P3', \
        "raw_header MUST be kept (parallels UI sys_id extraction)"
    # Explicit negative assertions for fields that must be stripped.
    for forbidden in ('chunk_hits', 'display', 'full_text', 'raw_file_hl', 'content'):
        assert forbidden not in row, f"{forbidden} must not survive compaction"


def test_per_row_bytes_drops_to_under_2kb():
    """SEED-002: per-row sys.getsizeof of dict + values must drop dramatically
    once display + full_text are stripped. Pre-fix worst case was ~22 KB per
    row (full Hebrew transcription + display dict + chunk_hits etc.); post-fix
    a representative row with a typical 500-char search snippet lands under
    2 KB. (Hebrew chars are 2 bytes each in CPython's PEP-393 storage, so a
    500-char snippet itself is ~1100 bytes including str overhead.)
    """
    from web import export_state

    row = {
        'uid': '9' * 32,  # uid 32 chars
        'sort_score': 0.95,
        # Typical search snippet — 500 chars (representative of production;
        # matches the legacy 500-char excerpt cap that ed6f89c4 used).
        'snippet': 'א' * 400 + '*ב*' * 30,
        'match_terms': ['אבל', 'אמר', 'דרש', 'תני', 'הא'],
        # Extra fields that should be stripped (the bulk of the legacy row).
        'display': {'shelfmark': 'T-S 12.345', 'title': 'huge title' * 100},
        'full_text': 'x' * 50_000,
        'raw_file_hl': 'y' * 50_000,
        'content': 'z' * 50_000,
    }
    pre_strip_bytes = sys.getsizeof(row) + sum(sys.getsizeof(v) for v in row.values())

    compacted, _changed = export_state._compact_search_result_row(row)
    total = sys.getsizeof(compacted) + sum(sys.getsizeof(v) for v in compacted.values())

    # Post-strip MUST be dramatically smaller than pre-strip (the whole point
    # of SEED-002). With a 500-char Hebrew snippet we expect ~1.5 KB.
    assert total < 2048, f"per-row bytes = {total}, expected < 2048"
    # And the reduction ratio sanity-check: pre-fix bytes >> post-fix bytes.
    assert pre_strip_bytes > 10 * total, (
        f"compaction must produce >10x reduction; got "
        f"pre={pre_strip_bytes} bytes, post={total} bytes"
    )


def test_5000_row_payload_well_under_pre_fix_ceiling(monkeypatch):
    """SEED-002: a fully capped 5000-row search payload should be dramatically
    smaller than the pre-fix 110 MB worst case. With realistic Hebrew snippets
    (~500 chars each, 2-byte UTF-8 per char) we expect ~5-7 MB for the JSON
    payload — a >15x reduction from the pre-fix ceiling.

    Note: 2-byte UTF-8 Hebrew chars make a 490-char snippet ~980 JSON bytes
    by themselves; 5000 rows pushes the floor near 5 MB regardless of how
    aggressively we strip metadata. The target is OOM-of-magnitude reduction
    from 110 MB, not absolute KB-scale (uid-only IS the floor).
    """
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    rows = [{
        'uid': f'9912345678901234_IE{i}_P1_FL1',
        'sort_score': 0.95 - (i * 1e-5),
        'snippet': 'א' * 400 + '*ב*' * 30,
        'match_terms': ['אבל', 'אמר', 'דרש', 'תני', 'הא'],
    } for i in range(5000)]
    export_state.set_search_export(results=rows, query='ה*')

    payload = storage['export_search_payload']
    size = _json_size(payload)
    # Hard ceiling: must be at least 10x smaller than pre-fix 110 MB.
    assert size < 11 * 1024 * 1024, f"5000-row payload = {size} bytes, expected < 11 MB"
    assert len(payload['results']) == 5000


def test_field_strip_invariants_still_hold(monkeypatch):
    """SEED-002 must preserve the ed6f89c4 invariants: heavyweight text
    fields (full_text, raw_file_hl, content) are stripped regardless of
    the compaction shape. The uid-only allowlist subsumes these — verify
    that none of the three keys is present in the stored row (we no
    longer expect full_text_excerpt either)."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    row = {
        'uid': 'u1',
        'sort_score': 0.5,
        'snippet': 'small',
        'match_terms': [],
        'full_text': 'x' * 500_000,
        'raw_file_hl': 'y' * 500_000,
        'content': 'z' * 500_000,
    }
    export_state.set_search_export(results=[row], query='foo')

    stored = storage['export_search_payload']['results'][0]
    assert 'full_text' not in stored
    assert 'raw_file_hl' not in stored
    assert 'content' not in stored
    # SEED-002 post-fix: no excerpt field either; rehydrate via Tantivy.
    assert 'full_text_excerpt' not in stored
