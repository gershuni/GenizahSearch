"""Round-trip tests for search snapshot helpers (Phase 74, D-18)."""
from unittest.mock import patch

import pytest


def _make_storage():
    """Create a simple dict that mimics app.storage.user."""
    return {}


def test_persist_and_restore_round_trip():
    """runtime_only fields are pristine; restorable fields survive."""
    storage = _make_storage()
    tab_storage = {}
    # Phase 87 (B3 fix): user-storage reads/writes now route through
    # web.safe_storage; tab storage remains direct in search_state.py.
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = tab_storage
        mock_safe_app.storage.user = storage

        from web.pages.search_state import (
            SearchUIState,
            persist_search_snapshot,
            restore_search_snapshot,
        )

        state = SearchUIState()
        state.results = [{'display': {'id': 'abc'}}]
        state.printed_filter = 'hide_printed'
        state.is_running = True       # runtime_only - must NOT survive
        state.expanded_index = 3      # runtime_only - must NOT survive

        persist_search_snapshot(state)
        assert storage['search_results'] == [{'display': {'id': 'abc'}}]

        fresh_state = SearchUIState()
        fresh_state.is_running = False
        fresh_state.expanded_index = None
        restore_search_snapshot(fresh_state)

        assert fresh_state.results == [{'display': {'id': 'abc'}}]
        assert fresh_state.printed_filter == 'hide_printed'
        assert fresh_state.is_running is False
        assert fresh_state.expanded_index is None


def test_clear_snapshot_wipes_all_keys():
    """clear_search_snapshot removes snapshot keys it owns.

    Bootstrap-input keys (search_query, search_mode) are NOT owned by the
    helper and MUST survive the clear - they are owned by the bootstrap path
    (review-revision: Codex HIGH #1). The helper only resets true
    page-state snapshot keys (search_results, domain_exclusions, etc.).
    """
    storage = {
        'search_query': 'survives',  # bootstrap-input - NOT cleared by helper
        'search_mode': 'exact',       # bootstrap-input - NOT cleared by helper
        'search_results': [{'id': 'x'}],
        'domain_exclusions': ['foo'],
        'search_snapshot_schema_version': 1,
    }
    tab_storage = {'search_active_snapshot': {'version': 1, 'results': [{'id': 'tab'}]}}
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = tab_storage
        mock_safe_app.storage.user = storage
        from web.pages.search_state import clear_search_snapshot
        clear_search_snapshot()
        # Snapshot fields are reset to safe defaults:
        assert storage.get('search_results') == []
        assert storage.get('domain_exclusions') == []
        # Bootstrap-input keys are NOT touched by the helper:
        assert storage.get('search_query') == 'survives'
        assert storage.get('search_mode') == 'exact'


def test_missing_stamp_adopts_legacy_payload():
    """Pre-Phase-74 snapshots (no version stamp) are adopted, not wiped.

    Regression guard for Codex 74-CODEX-REVIEW2.md #1: returning users'
    search_results / domain_exclusions must survive the first post-upgrade
    load rather than being silently cleared.
    """
    storage = {
        'search_results': [{'display': {'id': 'legacy'}}],
        'domain_exclusions': ['foo'],
        # No 'search_snapshot_schema_version' key -> pre-74 snapshot.
    }
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = {}
        mock_safe_app.storage.user = storage
        from web.pages.search_state import SearchUIState, restore_search_snapshot

        state = SearchUIState()
        restore_search_snapshot(state)

        assert state.results == [{'display': {'id': 'legacy'}}]
        assert state.domain_exclusions == {'foo'}
        # Stamp adopted to current so the next load treats it as current.
        assert storage.get('search_snapshot_schema_version') == 1


def test_clear_search_filters_preserves_live_search_state():
    """clear_search_filters resets pre-search filter keys ONLY.

    Regression guard for Codex 74-CODEX-REVIEW2.md #3: the Advanced 'Clear All'
    button must not wipe search_results / domain_exclusions / refinement chain
    currently on screen.
    """
    storage = {
        # Live search state - MUST survive:
        'search_results': [{'display': {'id': 'keep'}}],
        'domain_exclusions': ['live_excl'],
        'search_printed_filter': 'hide_printed',
        'search_refinement_chain': [{'query': 'x'}],
        'search_exclusion_sources': [{'type': 'list', 'id': 1}],
        'search_all_terms_filter': True,
        # Filter keys - MUST be cleared:
        'search_filter_domains': ['d1', 'd2'],
        'search_filter_authors': [42],
        'search_filter_include_mode': False,
        'search_filter_date_from': 1000,
        'search_filter_width_min': 5.0,
        'search_filter_measurement_material': ['Parchment'],
    }
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = {}
        mock_safe_app.storage.user = storage
        from web.pages.search_state import clear_search_filters

        clear_search_filters()

        # Live search state preserved:
        assert storage['search_results'] == [{'display': {'id': 'keep'}}]
        assert storage['domain_exclusions'] == ['live_excl']
        assert storage['search_printed_filter'] == 'hide_printed'
        assert storage['search_refinement_chain'] == [{'query': 'x'}]
        assert storage['search_exclusion_sources'] == [{'type': 'list', 'id': 1}]
        assert storage['search_all_terms_filter'] is True
        # Filter keys reset:
        assert storage['search_filter_domains'] == []
        assert storage['search_filter_authors'] == []
        assert storage['search_filter_include_mode'] is True
        assert storage['search_filter_date_from'] is None
        assert storage['search_filter_width_min'] is None
        assert storage['search_filter_measurement_material'] == []


def test_stale_version_discards_snapshot():
    """Snapshot with wrong version stamp is silently discarded.

    2026-05-12: restore_search_snapshot now reads the version stamp via
    web.safe_storage.safe_user_get (Codex HIGH finding fix), so the mock
    must intercept that module's ``app`` import as well.
    """
    storage = {
        'search_results': [{'display': {'id': 'old'}}],
        'domain_exclusions': ['stale'],
        'search_snapshot_schema_version': 999,
    }
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = {}
        mock_safe_app.storage.user = storage
        from web.pages.search_state import SearchUIState, restore_search_snapshot

        state = SearchUIState()
        restore_search_snapshot(state)
        # Default, not restored - stale version was discarded.
        assert state.results == []


def test_restore_prefers_the_runs_snapshot_over_legacy_user_results():
    """The snapshot the run in the address left wins over the compacted, shared user
    storage copy (web/search_runs.py)."""
    from web import search_runs
    storage = {
        'search_results': [{'display': {'id': 'legacy'}}],
        'search_snapshot_schema_version': 1,
    }
    search_runs.clear()
    search_runs.finish_run('browser-1', 'code', {
        'version': 1,
        'results': [{'display': {'id': 'run'}}],
        'printed_filter': 'only_printed',
        'domain_exclusions': ['foo'],
        'search_refinement_chain': [],
        'search_exclusion_sources': [],
    })
    try:
        with patch('web.pages.search_state.app') as mock_app, \
             patch('web.safe_storage.app') as mock_safe_app, \
             patch('web.pages.search_state._browser_id', lambda: 'browser-1'):
            mock_app.storage.user = storage
            mock_safe_app.storage.user = storage
            from web.pages.search_state import SearchUIState, restore_search_snapshot

            state = SearchUIState()
            restore_search_snapshot(state, 'code')
            assert state.results == [{'display': {'id': 'run'}}]
            assert state.printed_filter == 'only_printed'
            assert state.domain_exclusions == {'foo'}

            other = SearchUIState()
            restore_search_snapshot(other)              # no run in the address
            assert other.results == [{'display': {'id': 'legacy'}}]
    finally:
        search_runs.clear()


@pytest.mark.parametrize('path', ['run snapshot', 'user snapshot'])
def test_a_restored_result_set_brings_its_responsa_downgrade_back(path):
    """The count line shows the downgrade from search_state: a back-navigation or a
    reload installed the rows without it (Codex review of PR #433)."""
    from web import search_runs
    rows = [{'display': {'id': 'r'}, 'responsa_warning': 'Spelling variants disabled'}]
    storage = {'search_snapshot_schema_version': 1}
    search_runs.clear()
    if path == 'run snapshot':
        search_runs.finish_run('browser-1', 'code', {
            'version': 1, 'results': rows, 'search_refinement_chain': [],
            'search_exclusion_sources': []})
    else:
        storage['search_results'] = rows
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app, \
         patch('web.pages.search_state._browser_id', lambda: 'browser-1'):
        mock_app.storage.user = storage
        mock_safe_app.storage.user = storage
        from web.pages.search_state import SearchUIState, restore_search_snapshot

        state = SearchUIState()
        restore_search_snapshot(state, 'code')
        search_runs.clear()

        assert state.results == rows
        assert state.responsa_warning == 'Spelling variants disabled'


def test_restore_falls_back_to_compact_user_snapshot_when_tab_missing():
    """Compact user snapshot must still restore visible results if tab snapshot is absent."""
    storage = {
        'search_results': [{'display': {'id': 'user'}}],
        'search_printed_filter': 'hide_printed',
        'domain_exclusions': ['bar'],
        'search_snapshot_schema_version': 1,
    }
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = {}
        mock_safe_app.storage.user = storage
        from web.pages.search_state import SearchUIState, restore_search_snapshot

        state = SearchUIState()
        restore_search_snapshot(state)

        assert state.results == [{'display': {'id': 'user'}}]
        assert state.printed_filter == 'hide_printed'
        assert state.domain_exclusions == {'bar'}


def test_search_history_compacts_embedded_results():
    """History entries must not persist heavyweight results."""
    storage = {'session_persistence_enabled': True, 'search_history_limit': 20}
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_app.storage.tab = {}
        mock_safe_app.storage.user = storage
        from web.pages.search_state import add_to_search_history

        add_to_search_history(
            query='abc',
            result_count=3,
            mode='exact',
            params={},
            state_snapshot={'results': [{'id': 'heavy'}], 'printed_filter': 'all'},
        )

        assert storage['search_history'][0]['state'] == {'printed_filter': 'all'}


def test_compact_result_rows_strips_heavy_text_and_keeps_excerpt():
    from web.pages.search_state import compact_result_rows

    compacted = compact_result_rows([{
        'display': {'id': 'abc', 'full_text': 'nested', 'content': 'nested-heavy'},
        'full_text': 'x' * 1000,
        'raw_file_hl': 'y' * 1000,
        'content': 'z' * 1000,
        'snippet': 'small',
    }])

    row = compacted[0]
    assert 'full_text' not in row
    assert 'raw_file_hl' not in row
    assert 'content' not in row
    assert row['full_text_excerpt'] == 'x' * 500
    assert 'full_text' not in row['display']
    assert 'content' not in row['display']


@pytest.mark.parametrize('path, rows, partial', [
    ('run snapshot', 1, True),
    ('user snapshot', 1, True),
    ('user snapshot', 3, False),      # it keeps 2 rows here: completing reads the rest
])
def test_a_restored_search_keeps_its_partial_state(path, rows, partial):
    """Codex round 3 on PR #442: a star search that left crossings out by its own rule is
    "+" that completing cannot fix (result_partial). A restored search keeps that, or
    Search within would run it again with every candidate -- unless the snapshot dropped
    rows, which completing does read."""
    from web import search_runs
    from web.pages import search_state as ss
    storage = {'search_snapshot_schema_version': 1}
    search_runs.clear()
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app, \
         patch('web.pages.search_state._browser_id', lambda: 'browser-1'), \
         patch.object(ss, '_SEARCH_ACTIVE_USER_FALLBACK_LIMIT', 2):
        mock_app.storage.user = storage
        mock_safe_app.storage.user = storage
        state = ss.SearchUIState()
        state.results = [{'display': {'id': f'M{i}'}} for i in range(rows)]
        state.result_count_capped = True
        state.result_partial = True
        if path == 'run snapshot':
            search_runs.finish_run('browser-1', 'code', dict(ss.search_run_snapshot(state),
                                                             search_exclusion_sources=[]))
        else:
            ss.persist_search_snapshot(state)
            search_runs.clear()
        restored = ss.SearchUIState()
        ss.restore_search_snapshot(restored, 'code')
        search_runs.clear()
    assert restored.result_count_capped is True
    assert restored.result_partial is partial


def test_the_run_snapshot_drops_the_partial_state_with_its_rows():
    from web.pages import search_state as ss
    state = ss.SearchUIState()
    state.result_partial = True
    state.results = [{'display': {'id': 'M'}}]
    assert ss.search_run_snapshot(state)['result_partial'] is True
    state.results = [{'display': {'id': f'M{i}'}} for i in range(1001)]
    snapshot = ss.search_run_snapshot(state)
    assert snapshot['result_partial'] is False and snapshot['result_count_capped'] is True


def test_clearing_the_snapshot_drops_the_partial_state():
    storage = {'search_result_partial': True}
    with patch('web.pages.search_state.app') as mock_app, \
         patch('web.safe_storage.app') as mock_safe_app:
        mock_app.storage.user = storage
        mock_safe_app.storage.user = storage
        from web.pages.search_state import clear_search_snapshot
        clear_search_snapshot()
    assert 'search_result_partial' not in storage
