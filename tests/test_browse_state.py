"""Tests for browse snapshot helpers (Phase 74 + 74-CODEX-REVIEW2 fixes)."""
from unittest.mock import patch


def test_missing_stamp_adopts_legacy_payload():
    """Pre-Phase-74 snapshots (no version stamp) are adopted, not wiped.

    Regression guard for Codex 74-CODEX-REVIEW2.md #1: returning users'
    browse_position / reading_desk_state must survive the first post-upgrade
    load rather than being silently cleared.
    """
    storage = {
        'browse_position': {'sys_id': '123', 'p_num': 2, 'shelfmark': 'T-S 12.1', 'volume_ie': None},
        'reading_desk_state': {'entries': [{'sys_id': '123', 'shelfmark': 'T-S 12.1'}], 'pgpid': None, 'selected_sources': {}},
        # No 'browse_snapshot_schema_version' key -> pre-74 snapshot.
    }
    with patch('web.pages.browse_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.browse_state import BrowseState, restore_browse_snapshot

        pos, desk = restore_browse_snapshot(BrowseState())

        assert pos is not None
        assert pos['sys_id'] == '123'
        assert desk is not None
        assert desk['entries'] == [{'sys_id': '123', 'shelfmark': 'T-S 12.1'}]
        # Stamp adopted so subsequent loads see a current version.
        assert storage.get('browse_snapshot_schema_version') == 1


def test_stale_version_wipes_snapshot():
    """Non-zero mismatched stamp is treated as stale (existing behavior)."""
    storage = {
        'browse_position': {'sys_id': 'old'},
        'reading_desk_state': {'entries': [{'sys_id': 'old'}]},
        'browse_snapshot_schema_version': 999,
    }
    with patch('web.pages.browse_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.browse_state import BrowseState, restore_browse_snapshot

        pos, desk = restore_browse_snapshot(BrowseState())

        assert pos is None
        assert desk is None
        assert 'browse_position' not in storage
        assert 'reading_desk_state' not in storage


def test_clear_snapshot_keep_position_preserves_position():
    """clear_browse_snapshot(keep_position=True) drops only reading_desk_state.

    Regression guard for Codex 74-CODEX-REVIEW2.md #2: exiting joined view /
    clearing stale desk on explicit ?sys_id= navigation must not wipe the
    user's last single-page position.
    """
    storage = {
        'browse_position': {'sys_id': '123', 'p_num': 4},
        'reading_desk_state': {'entries': [{'sys_id': '123'}]},
        'browse_snapshot_schema_version': 1,
    }
    with patch('web.pages.browse_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.browse_state import clear_browse_snapshot

        clear_browse_snapshot(keep_position=True)

        assert storage.get('browse_position') == {'sys_id': '123', 'p_num': 4}
        assert storage.get('browse_snapshot_schema_version') == 1
        assert 'reading_desk_state' not in storage


def test_clear_snapshot_default_wipes_everything():
    """Default clear_browse_snapshot() wipes all keys including the stamp."""
    storage = {
        'browse_position': {'sys_id': '123'},
        'reading_desk_state': {'entries': []},
        'browse_snapshot_schema_version': 1,
    }
    with patch('web.pages.browse_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.browse_state import clear_browse_snapshot

        clear_browse_snapshot()

        assert 'browse_position' not in storage
        assert 'reading_desk_state' not in storage
        assert 'browse_snapshot_schema_version' not in storage


def test_persist_round_trip():
    """persist_browse_snapshot writes position + desk; restore reads them back."""
    storage = {'session_persistence_enabled': True}
    with patch('web.pages.browse_state.app') as mock_app:
        mock_app.storage.user = storage
        from web.pages.browse_state import (
            BrowseState, persist_browse_snapshot, restore_browse_snapshot,
        )

        class _Page:
            p_num = 3
            shelfmark = 'T-S 13.1'

        state = BrowseState()
        state.sys_id = '456'
        state.volume_ie = None
        state.view_joined = True
        state.reading_desk_entries = [{'sys_id': '456', 'shelfmark': 'T-S 13.1'}]
        state.joined_pgpid = 99
        state.reading_desk_selected_sources = {'456': 0}

        persist_browse_snapshot(state, page=_Page())

        pos, desk = restore_browse_snapshot(BrowseState())
        assert pos is not None and pos['sys_id'] == '456' and pos['p_num'] == 3
        assert desk is not None and desk['pgpid'] == 99
        assert desk['entries'] == [{'sys_id': '456', 'shelfmark': 'T-S 13.1'}]
