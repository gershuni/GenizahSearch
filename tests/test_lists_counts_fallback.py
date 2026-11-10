# -*- coding: utf-8 -*-
"""Phase 92.2 Reviews MUST-FIX 1 behavioral tests for _resolve_list_item_count.

3 tests verifying the None-vs-empty-dict semantics:
- counts=None  -> triggers legacy per-list _get_list_item_count() fallback
- counts={}    -> returns 0 WITHOUT triggering legacy fallback (valid batched empty result)
- counts={5:3} -> returns 3

These cases all concern a NON-recent list. W3 (2026-06-23) added a recent-list
routing branch that ALWAYS resolves through _get_list_item_count (its items live in
recent_items, not list_items), so the mocked manager must declare it is not the recent
list — otherwise Mock's auto-attribute makes _is_recent_list() return a truthy Mock and
every list takes the recent branch. The recent-list routing itself is covered by
tests/test_recently_viewed_bugs.py.
"""

from unittest.mock import Mock
from web.components.project_tree import _resolve_list_item_count


def _regular_list_mgr() -> Mock:
    """A manager mock whose lists are NOT the recent system list."""
    mgr = Mock()
    mgr._is_recent_list.return_value = False
    return mgr


def test_counts_none_triggers_legacy():
    """counts=None means 'no batched result — use legacy per-list fetch'."""
    mgr = _regular_list_mgr()
    mgr._get_list_item_count.return_value = 9

    assert _resolve_list_item_count("5", mgr, None) == 9
    mgr._get_list_item_count.assert_called_once_with("5")


def test_counts_empty_dict_returns_zero_without_legacy():
    """counts={} is a VALID batched result (user has no items).
    Must NOT trigger legacy fallback (Reviews MUST-FIX 1).
    """
    mgr = _regular_list_mgr()
    mgr._get_list_item_count.return_value = 9

    assert _resolve_list_item_count("5", mgr, {}) == 0
    mgr._get_list_item_count.assert_not_called()


def test_counts_populated_returns_value():
    """counts={5: 3} returns 3 for list_id '5'."""
    mgr = _regular_list_mgr()
    assert _resolve_list_item_count("5", mgr, {5: 3}) == 3
