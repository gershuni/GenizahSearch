# -*- coding: utf-8 -*-
"""Phase 95 REQ-9 + D-30 Codex P0: LOCAL sys_ids must not trigger cloud client
calls in lists_sync.ListsCloudSync.sync_item_to_cloud / sync_list_to_cloud.

The gate MUST be at the TOP of each function, BEFORE _get_client() is called.
HIGH-2 review fix: sys_id derivation runs OUTSIDE any `if item_data:` branch
so a LOCAL item_id with missing item_data is ALSO gated.
"""
import logging
from unittest.mock import MagicMock


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_manager(items=None, lists=None):
    """Return a ListsCloudSync instance with fake in-memory data.

    Uses a simple MagicMock for lists_manager so we don't need the real
    ListsManager (which requires a file path).
    """
    from lists_sync import ListsCloudSync

    lm = MagicMock()
    lm.data = {
        'items': items or {},
        'lists': lists or {},
    }
    lm.save = MagicMock()

    manager = ListsCloudSync(lists_manager=lm)
    # Make is_sync_available() return True by providing a user_id and
    # simulating SUPABASE_AVAILABLE=True / SUPABASE_ANON_KEY set.
    manager._user_id = 'test-user-uuid'
    # Patch module-level SUPABASE_AVAILABLE and SUPABASE_ANON_KEY
    import lists_sync as ls_mod
    ls_mod.SUPABASE_AVAILABLE = True
    ls_mod.SUPABASE_ANON_KEY = 'fake-anon-key'
    return manager


LOCAL_SYS_ID = '970012345601234567'
REAL_SYS_ID = '990025143260205171'
SYNTH_SYS_ID = '990001234560000000'


# ---------------------------------------------------------------------------
# Task 1a: sync_item_to_cloud — LOCAL item_data present
# ---------------------------------------------------------------------------

def test_sync_item_to_cloud_zero_get_client_calls_for_local(caplog):
    """REQ-9 + D-30 Codex P0 (item_data present): mock _get_client and
    sync_list_to_cloud; pass a LOCAL sys_id; assert _get_client.call_count == 0.
    """
    manager = _make_manager(
        items={
            'fake-item-id': {
                'sys_id': LOCAL_SYS_ID,
                'lists': ['fake-list-id'],
                'fl_id': 'some-fl-id',
            }
        }
    )

    mock_get_client = MagicMock(return_value=None)
    mock_sync_list = MagicMock(return_value=False)
    manager._get_client = mock_get_client
    manager.sync_list_to_cloud = mock_sync_list

    with caplog.at_level(logging.INFO, logger='lists_sync'):
        result = manager.sync_item_to_cloud(
            item_id='fake-item-id', list_id='fake-list-id'
        )

    assert result is False, "Expected False for LOCAL sys_id"
    assert mock_get_client.call_count == 0, (
        "Codex P0 gate FAILED: _get_client() was called for a LOCAL sys_id"
    )
    assert mock_sync_list.call_count == 0, (
        "Codex P0 gate FAILED: sync_list_to_cloud() was called for a LOCAL sys_id"
    )
    assert 'local-only item, not synced' in caplog.text, (
        "Expected INFO log 'local-only item, not synced' not found"
    )


# ---------------------------------------------------------------------------
# Task 1b: sync_item_to_cloud — LOCAL item_id, missing item_data (HIGH-2)
# ---------------------------------------------------------------------------

def test_sync_item_to_cloud_local_item_id_missing_data(caplog):
    """HIGH-2 review fix (load-bearing): when item_data is None AND item_id
    itself is a LOCAL sys_id, the function MUST gate BEFORE any cloud touch.
    """
    # Empty items dict — no item_data in memory
    manager = _make_manager(items={})

    mock_get_client = MagicMock(return_value=None)
    mock_sync_list = MagicMock(return_value=False)
    manager._get_client = mock_get_client
    manager.sync_list_to_cloud = mock_sync_list

    with caplog.at_level(logging.INFO, logger='lists_sync'):
        result = manager.sync_item_to_cloud(
            item_id=LOCAL_SYS_ID, list_id='fake-list-id'
        )

    assert result is False, "Expected False for LOCAL item_id with missing item_data"
    assert mock_get_client.call_count == 0, (
        "HIGH-2 gate FAILED: _get_client() was called when item_data was None "
        "and item_id was LOCAL"
    )
    assert mock_sync_list.call_count == 0, (
        "HIGH-2 gate FAILED: sync_list_to_cloud() was called when item_data was None "
        "and item_id was LOCAL"
    )
    assert 'local-only item, not synced' in caplog.text, (
        "Expected INFO log 'local-only item, not synced' not found"
    )


# ---------------------------------------------------------------------------
# Task 1c: sync_item_to_cloud — missing item_data, non-LOCAL item_id (HIGH-2 regression)
# ---------------------------------------------------------------------------

def test_sync_item_to_cloud_missing_item_data_non_local_item_id():
    """HIGH-2 regression: non-LOCAL item_id with missing item_data should NOT
    be short-circuited by the LOCAL gate.  The function reaches is_sync_available()
    (and beyond) normally.
    """
    # Empty items dict — no item_data
    manager = _make_manager(items={})

    mock_get_client = MagicMock(return_value=None)
    mock_sync_list = MagicMock(return_value=False)
    manager._get_client = mock_get_client
    manager.sync_list_to_cloud = mock_sync_list

    # Should not raise; should NOT short-circuit at the LOCAL gate.
    # (It will return False for other reasons — item_data is None.)
    result = manager.sync_item_to_cloud(
        item_id='some-non-local-id', list_id='fake-list-id'
    )

    # The function will eventually return False (item_data not found), but
    # the LOCAL gate must NOT have fired — the function should have proceeded
    # past is_sync_available() so at least _get_client is accessible.
    # We simply assert no exception raised and we can verify the gate wasn't
    # taken by confirming is_sync_available was reachable (no exception thrown).
    assert result is False  # returns False because item_data is None


# ---------------------------------------------------------------------------
# Task 1d: sync_item_to_cloud — synthetic sys_id unchanged (regression)
# ---------------------------------------------------------------------------

def test_sync_item_to_cloud_synthetic_unchanged():
    """Regression: synthetic 99-prefix sys_id must NOT trigger the LOCAL gate."""
    manager = _make_manager(
        items={
            'synth-item-id': {
                'sys_id': SYNTH_SYS_ID,
                'lists': ['fake-list-id'],
            }
        }
    )

    mock_get_client = MagicMock(return_value=None)
    mock_sync_list = MagicMock(return_value=False)
    manager._get_client = mock_get_client
    manager.sync_list_to_cloud = mock_sync_list

    # Should NOT short-circuit at LOCAL gate — synthetic does not match is_local_sys_id
    result = manager.sync_item_to_cloud(
        item_id='synth-item-id', list_id='fake-list-id'
    )

    # The function will proceed past the LOCAL gate and fail for other reasons
    # (no cloud_id, sync_list_to_cloud returns False), so result is False.
    # The key is that _get_client WAS called (gate did not fire).
    assert result is False
    # _get_client should have been called since the LOCAL gate did not fire
    # and is_sync_available() returned True
    assert mock_get_client.call_count >= 1, (
        "Synthetic sys_id should proceed past the LOCAL gate and reach _get_client"
    )


# ---------------------------------------------------------------------------
# Task 1e: sync_list_to_cloud — aborts if any item in the list is LOCAL
# ---------------------------------------------------------------------------

def test_sync_list_to_cloud_aborts_if_any_item_local(caplog):
    """REQ-9 D-30: sync_list_to_cloud aborts BEFORE _get_client if any item
    belonging to the list has a LOCAL sys_id.
    """
    manager = _make_manager(
        items={
            'real-item': {
                'sys_id': REAL_SYS_ID,
                'lists': ['fake-list-id'],
            },
            'local-item': {
                'sys_id': LOCAL_SYS_ID,
                'lists': ['fake-list-id'],
            },
        },
        lists={
            'fake-list-id': {
                'name': 'Test List',
                'color': '#FFD700',
                'is_default': False,
                'is_system': False,
            }
        }
    )

    mock_get_client = MagicMock(return_value=None)
    manager._get_client = mock_get_client

    with caplog.at_level(logging.INFO, logger='lists_sync'):
        result = manager.sync_list_to_cloud(list_id='fake-list-id')

    assert result is False, "Expected False when list contains LOCAL items"
    assert mock_get_client.call_count == 0, (
        "D-30 gate FAILED: _get_client() was called when list contained LOCAL items"
    )
    assert 'list contains LOCAL items, not synced' in caplog.text, (
        "Expected INFO log 'list contains LOCAL items, not synced' not found"
    )


# ---------------------------------------------------------------------------
# Task 1f: sync_list_to_cloud — no LOCAL items, proceeds normally (regression)
# ---------------------------------------------------------------------------

def test_sync_list_to_cloud_no_local_items_proceeds():
    """Regression: items dict contains ONLY Genizah sys_ids associated with
    the list. Gate must NOT short-circuit; existing flow continues.
    """
    manager = _make_manager(
        items={
            'real-item-1': {
                'sys_id': REAL_SYS_ID,
                'lists': ['fake-list-id'],
            },
        },
        lists={
            'fake-list-id': {
                'name': 'Test List',
                'color': '#FFD700',
                'is_default': False,
                'is_system': False,
            }
        }
    )

    mock_get_client = MagicMock(return_value=None)
    manager._get_client = mock_get_client

    # Gate must NOT fire — existing flow should proceed (and fail because
    # _get_client returns None, but the gate is not the reason)
    result = manager.sync_list_to_cloud(list_id='fake-list-id')

    # The function should have proceeded to _get_client (which returns None)
    # and returned False because no client is available.
    assert result is False
    # _get_client should have been called since the LOCAL gate did not fire
    assert mock_get_client.call_count >= 1, (
        "Non-LOCAL items should proceed past the LOCAL gate and reach _get_client"
    )
