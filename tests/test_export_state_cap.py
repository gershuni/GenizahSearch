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
    """SEED-002 fixup: heavy fields stripped; chunk_hits IS kept but capped.

    Initial SEED-002 (2a7440d6) dropped chunk_hits entirely; that broke the
    public /api/export/parallels/json matches[] array. Restored with the
    ed6f89c4 cap (100 entries x 1000 chars/string)."""
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
    # chunk_hits is kept (needed by public API serializer) but capped.
    assert len(stored['chunk_hits']) == export_state._PARALLELS_CHUNK_HITS_CAP
    # Each kept tuple's text-strings are individually capped.
    assert len(stored['chunk_hits'][0][1]) == export_state._PARALLELS_CHUNK_TEXT_STORAGE_CHARS
    assert len(stored['chunk_hits'][0][3]) == export_state._PARALLELS_CHUNK_TEXT_STORAGE_CHARS


def test_compact_parallels_result_rows_for_live_state():
    from web import export_state

    row = _result(1)
    row.update({'full_text': 'x' * 500_000, 'content': 'y' * 500_000})

    compacted = export_state.compact_parallels_result_rows([row])

    assert len(compacted) == 1
    assert 'full_text' not in compacted[0]
    assert 'content' not in compacted[0]


def test_compact_parallels_preserves_live_ui_metadata():
    """SEED-002 fixup round 2 (Codex catch): compact_parallels_result_rows()
    is used at web/pages/parallels.py:2338-2339 to overwrite the LIVE
    main_results / filtered_results BEFORE they land in p_state.results.
    The UI reads these 6 small scalars from p_state.results:
      - final_score / has_boundary_matches / boundary_quality /
        boundary_match_count at parallels.py:3124-3127 (boost badge +
        boundary chips on each per-match row)
      - filter_reason / is_text_filtered at parallels.py:3063-3068 (specific
        reason chip on filtered manuscript groups)
    The original SEED-002 commit + round-1 fixup BOTH dropped these fields,
    which would have silently degraded the parallels UI in production
    (boost badges gone, generic 'Filtered' label everywhere). This test
    locks the contract so they survive compaction."""
    from web import export_state

    row = {
        'uid': 'IE188433865_P1_FL1',
        'raw_header': '99001234567890 IE188433865 P1 FL1',
        'sort_score': 0.7,
        'score': 85.3,
        'snippet': '',
        'match_terms': [],
        'source_ctx': 'src',
        'text': 'manuscript',
        # Live-UI scalars that MUST survive compaction:
        'final_score': 2150.0,           # combined-mode boosted score (parallels.py:3124)
        'has_boundary_matches': True,    # parallels.py:3125
        'boundary_quality': 0.82,        # parallels.py:3126
        'boundary_match_count': 3,       # parallels.py:3127
        'filter_reason': 'high_frequency',  # parallels.py:3063-3067
        'is_text_filtered': True,        # parallels.py:3068
        'is_filtered': True,             # parallels.py:3070 (self-audit catch)
    }

    compacted = export_state.compact_parallels_result_rows([row])
    assert len(compacted) == 1
    kept = compacted[0]

    # Round-1 contract still holds: identity + body fields are present.
    assert kept['score'] == 85.3
    assert kept['raw_header'] == '99001234567890 IE188433865 P1 FL1'

    # Round-2 contract: live-UI scalars survive compaction so the parallels
    # UI keeps rendering boost badges and specific filter reasons.
    assert kept['final_score'] == 2150.0
    assert kept['has_boundary_matches'] is True
    assert kept['boundary_quality'] == 0.82
    assert kept['boundary_match_count'] == 3
    assert kept['filter_reason'] == 'high_frequency'
    assert kept['is_text_filtered'] is True
    # Self-audit (post-Codex-round-2): is_filtered is the coarse
    # "filtered for some reason" flag; the chip-rendering fall-through
    # at parallels.py:3070-3072 makes the field functionally redundant
    # with the post-loop default, but locking the contract prevents
    # future drift if non-display callers ever depend on the flag.
    assert kept['is_filtered'] is True


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

def test_search_export_row_keeps_identity_allowlist(monkeypatch):
    """SEED-002 fixup (2026-05-19): compacted search rows MUST be a subset of
    {uid, sys_id, sort_score, snippet, match_terms, raw_header}. The display
    dict, full_text, full_text_excerpt, raw_file_hl, content, and search-side
    `score` are dropped — display fields rehydrate via meta_mgr at export
    time, full_text rehydrates via Tantivy.

    Fixup history: the original SEED-002 commit (2a7440d6) dropped raw_header
    too, which made downstream display rehydration fail for production text-
    search uids (``IE..._P..._FL...`` carry no sys_id digits). The fixup
    re-adds raw_header to the allowlist AND synthesizes a top-level sys_id
    from display.id / raw_header / uid at compaction time."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    # Production-shape uid (no sys_id digits) + raw_header carrying the sys_id.
    full_shape = [{
        'uid': 'IE188433865_P1_FL1',
        'sort_score': 0.95,
        'snippet': 'a *match* here',
        'match_terms': ['match', 'here'],
        'raw_header': '99001234567890 IE188433865 P1 FL1',
        # Fields that must be dropped:
        'display': {'shelfmark': 'T-S 12.345', 'title': 't', 'id': '99001234567890', 'library_code': 'CUL'},
        'full_text': 'x' * 1000,
        'raw_file_hl': 'y' * 1000,
        'content': 'z' * 1000,
        'score': 0.5,  # search-side `score`, not parallels score
    }]

    export_state.set_search_export(results=full_shape, query='foo')

    row = storage['export_search_payload']['results'][0]
    allowed = {'uid', 'sys_id', 'sort_score', 'snippet', 'match_terms', 'raw_header'}
    assert set(row.keys()) <= allowed
    # Explicit negative assertions for every field that must be stripped.
    for forbidden in ('display', 'full_text', 'full_text_excerpt',
                      'raw_file_hl', 'content', 'score'):
        assert forbidden not in row, f"{forbidden} must not survive compaction"
    # Kept fields preserve values verbatim.
    assert row['uid'] == 'IE188433865_P1_FL1'
    assert row['sort_score'] == 0.95
    assert row['snippet'] == 'a *match* here'
    assert row['match_terms'] == ['match', 'here']
    assert row['raw_header'] == '99001234567890 IE188433865 P1 FL1'
    # sys_id synthesized at compaction time from display.id (canonical channel).
    assert row['sys_id'] == '99001234567890'


def test_search_export_metadata_only_row_keeps_sys_id(monkeypatch):
    """SEED-002 fixup regression: metadata-only rows (Title/Shelfmark search
    via genizah_core._execute_metadata_search) have uid='' AND raw_header=''.
    Only ``display.id`` carries the sys_id. The compactor MUST hoist that
    sys_id into a top-level field so the resolver can find it post-compaction.
    Without this, every metadata-only export collapses to 'Unknown'."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    metadata_only_row = [{
        'uid': '',
        'raw_header': '',
        'sort_score': 0.0,
        'snippet': '',
        'match_terms': [],
        'display': {
            'shelfmark': 'T-S NS 329.96',
            'title': 'Synthetic FJMS row',
            'id': '99800000000000123',  # 18-digit synthetic sys_id (Phase 85)
            'library_code': 'CUL',
        },
    }]

    export_state.set_search_export(results=metadata_only_row, query='T-S NS')

    row = storage['export_search_payload']['results'][0]
    assert row['sys_id'] == '99800000000000123', \
        "metadata-only sys_id must survive compaction via display.id hoist"
    # The display dict itself is still dropped (heavy field).
    assert 'display' not in row


def test_search_export_row_synthesizes_sys_id_from_raw_header(monkeypatch):
    """Production text-search rows have display.id populated, but a defensive
    path: when a caller passes a row with no display dict but a valid
    raw_header, the compactor must still synthesize sys_id."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    rows = [{
        'uid': 'IE188433865_P1_FL1',
        'raw_header': '99001234567890 IE188433865 P1 FL1',
        'sort_score': 0.7,
        'snippet': '',
        'match_terms': [],
    }]
    export_state.set_search_export(results=rows, query='q')
    row = storage['export_search_payload']['results'][0]
    assert row['sys_id'] == '99001234567890'


def test_parallels_export_row_keeps_safe_allowlist(monkeypatch):
    """SEED-002 fixup: compacted parallels rows MUST be a subset of
    {uid, sys_id, sort_score, score, snippet, match_terms, source_ctx, text,
    raw_header, chunk_hits}.

    Retention invariants:
      - `score` and `raw_header` are kept (live parallels UI reads them at
        13 sites in web/pages/parallels.py; shared/search_serializer.py:691
        sums score into /api/parallels aggregate_score).
      - `chunk_hits` is kept (capped) because shared/search_serializer.py:828
        reads it to build the public /api/export/parallels/json matches[]
        array; dropping it collapses every row to one degenerate match.
      - `sys_id` is synthesized at compaction time so display rehydration
        works for both text-search and metadata-only rows post-compaction.

    Fields dropped: display, full_text, raw_file_hl, content."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    full_shape = [{
        'uid': 'IE188433865_P3_FL1',
        'sort_score': 0.5,
        'score': 85.3,
        'snippet': 'snippet text',
        'match_terms': ['x', 'y'],
        'source_ctx': 'ctx',
        'text': 'manuscript text',
        'raw_header': '99001111111111111 IE188433865 P3 FL1',
        # chunk_hits: 50 entries, each with 1000-char strings at idx 1 and 3.
        'chunk_hits': [(i, 'a' * 1000, 10 + i, 'b' * 1000) for i in range(50)],
        # Fields that must be dropped:
        'display': {'shelfmark': 'T-S 1', 'title': 't', 'id': '99001111111111111', 'library_code': 'CUL'},
        'full_text': 'x' * 1000,
        'raw_file_hl': 'y' * 1000,
        'content': 'z' * 1000,
    }]

    export_state.set_parallels_export(results=full_shape, filtered=[])

    row = storage['export_parallels_payload']['results'][0]
    allowed = {
        'uid', 'sys_id', 'sort_score', 'score', 'snippet', 'match_terms',
        'source_ctx', 'text', 'raw_header', 'chunk_hits',
        # Round-2 live-UI scalars (web/pages/parallels.py:3063-3071, 3124-3127).
        'final_score', 'has_boundary_matches', 'boundary_quality',
        'boundary_match_count', 'filter_reason', 'is_text_filtered',
        'is_filtered',
    }
    assert set(row.keys()) <= allowed
    # Critical retention invariants.
    assert row.get('score') == 85.3, "score MUST be kept (parallels UI + aggregate_score)"
    assert row.get('raw_header') == '99001111111111111 IE188433865 P3 FL1', \
        "raw_header MUST be kept (parallels UI sys_id extraction)"
    assert row.get('sys_id') == '99001111111111111', \
        "sys_id MUST be synthesized at compaction time"
    # chunk_hits preserved (capped); the underlying serializer needs it.
    assert isinstance(row.get('chunk_hits'), list)
    assert len(row['chunk_hits']) == 50, \
        "50 chunk_hits is under the 100-entry cap; all must survive"
    # Explicit negative assertions for fields that must be stripped.
    for forbidden in ('display', 'full_text', 'raw_file_hl', 'content'):
        assert forbidden not in row, f"{forbidden} must not survive compaction"


def test_parallels_export_chunk_hits_capped_at_100_entries(monkeypatch):
    """SEED-002 fixup: chunk_hits over the 100-entry cap is truncated to 100
    and each entry's strings are capped to 1000 chars (matching ed6f89c4)."""
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    chunk_hits = [(i, 'a' * 5000, i, 'b' * 5000) for i in range(250)]
    rows = [{
        'uid': 'IE1_P1_FL1',
        'raw_header': '99001234567890 IE1 P1 FL1',
        'sort_score': 0.5,
        'score': 10.0,
        'snippet': '',
        'match_terms': [],
        'source_ctx': '',
        'text': '',
        'chunk_hits': chunk_hits,
    }]
    export_state.set_parallels_export(results=rows, filtered=[])

    stored = storage['export_parallels_payload']['results'][0]
    assert len(stored['chunk_hits']) == export_state._PARALLELS_CHUNK_HITS_CAP
    # Each kept entry has strings capped to _PARALLELS_CHUNK_TEXT_STORAGE_CHARS.
    first = stored['chunk_hits'][0]
    assert len(first[1]) == export_state._PARALLELS_CHUNK_TEXT_STORAGE_CHARS
    assert len(first[3]) == export_state._PARALLELS_CHUNK_TEXT_STORAGE_CHARS


def test_per_row_bytes_drops_to_under_2kb():
    """SEED-002 fixup: per-row sys.getsizeof of dict + values stays under 2KB
    even with the fixup's added identity fields (raw_header ~50 bytes, sys_id
    ~16 bytes). Pre-fix worst case was ~22 KB per row; post-fix lands ~1.5KB.
    (Hebrew chars are 2 bytes each in CPython's PEP-393 storage, so a 500-char
    snippet itself is ~1100 bytes including str overhead.)
    """
    from web import export_state

    row = {
        'uid': 'IE188433865_P1_FL1',  # production text-search shape
        'raw_header': '99001234567890 IE188433865 P1 FL1',
        'sort_score': 0.95,
        # Typical search snippet — 500 chars (representative of production;
        # matches the legacy 500-char excerpt cap that ed6f89c4 used).
        'snippet': 'א' * 400 + '*ב*' * 30,
        'match_terms': ['אבל', 'אמר', 'דרש', 'תני', 'הא'],
        # Extra fields that should be stripped (the bulk of the legacy row).
        'display': {'shelfmark': 'T-S 12.345', 'title': 'huge title' * 100, 'id': '99001234567890'},
        'full_text': 'x' * 50_000,
        'raw_file_hl': 'y' * 50_000,
        'content': 'z' * 50_000,
    }
    pre_strip_bytes = sys.getsizeof(row) + sum(sys.getsizeof(v) for v in row.values())

    compacted, _changed = export_state._compact_search_result_row(row)
    total = sys.getsizeof(compacted) + sum(sys.getsizeof(v) for v in compacted.values())

    # Post-strip MUST be dramatically smaller than pre-strip. With a 500-char
    # Hebrew snippet + ~50-byte raw_header + ~16-byte sys_id we expect ~1.5 KB.
    assert total < 2048, f"per-row bytes = {total}, expected < 2048"
    # And the reduction ratio sanity-check: pre-fix bytes >> post-fix bytes.
    assert pre_strip_bytes > 10 * total, (
        f"compaction must produce >10x reduction; got "
        f"pre={pre_strip_bytes} bytes, post={total} bytes"
    )
    # Sanity: the identity fields landed.
    assert compacted['sys_id'] == '99001234567890'
    assert compacted['raw_header'] == '99001234567890 IE188433865 P1 FL1'


def test_5000_row_payload_well_under_pre_fix_ceiling(monkeypatch):
    """SEED-002 fixup: a fully capped 5000-row search payload is dramatically
    smaller than the pre-fix 110 MB worst case. With realistic Hebrew snippets
    (~500 chars each, 2-byte UTF-8 per char) and the fixup's small identity
    fields (raw_header ~50B + sys_id ~16B per row = 330 KB / 5000 rows total),
    we expect ~5-7 MB for the JSON payload — still a >15x reduction from the
    pre-fix ceiling.

    Note: 2-byte UTF-8 Hebrew chars make a 490-char snippet ~980 JSON bytes
    by themselves; 5000 rows pushes the floor near 5 MB regardless of how
    aggressively we strip metadata. The target is OOM-of-magnitude reduction
    from 110 MB, not absolute KB-scale.
    """
    from web import export_state

    storage: dict = {}
    monkeypatch.setattr('web.safe_storage.app', _make_stub(storage))

    rows = [{
        # Production text-search uid shape (no sys_id digits).
        'uid': f'IE{i}_P1_FL1',
        'raw_header': f'99{i:013d}0 IE{i} P1 FL1',
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
    # sys_id was synthesized from raw_header by the compactor for every row.
    assert all(r.get('sys_id') for r in payload['results'])


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
