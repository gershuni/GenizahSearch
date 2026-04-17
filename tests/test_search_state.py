"""Round-trip tests for search snapshot helpers (Phase 74, D-18)."""
from unittest.mock import patch


def _make_storage():
    """Create a simple dict that mimics app.storage.user."""
    return {}


def test_persist_and_restore_round_trip():
    """runtime_only fields are pristine; restorable fields survive."""
    storage = _make_storage()
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage

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
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage
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
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage
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
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage
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
    """Snapshot with wrong version stamp is silently discarded."""
    storage = {
        'search_results': [{'display': {'id': 'old'}}],
        'domain_exclusions': ['stale'],
        'search_snapshot_schema_version': 999,
    }
    with patch('web.pages.search_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.search_state import SearchUIState, restore_search_snapshot

        state = SearchUIState()
        restore_search_snapshot(state)
        # Default, not restored - stale version was discarded.
        assert state.results == []
