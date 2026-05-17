# -*- coding: utf-8 -*-
"""Phase 92.2 Reviews MUST-FIX 1 behavioral tests for _resolve_list_item_count.

3 tests verifying the None-vs-empty-dict semantics:
- counts=None  -> triggers legacy per-list _get_list_item_count() fallback
- counts={}    -> returns 0 WITHOUT triggering legacy fallback (valid batched empty result)
- counts={5:3} -> returns 3
"""

from unittest.mock import Mock
from web.components.project_tree import _resolve_list_item_count


def test_counts_none_triggers_legacy():
    """counts=None means 'no batched result — use legacy per-list fetch'."""
    mgr = Mock()
    mgr._get_list_item_count.return_value = 9

    assert _resolve_list_item_count("5", mgr, None) == 9
    mgr._get_list_item_count.assert_called_once_with("5")


def test_counts_empty_dict_returns_zero_without_legacy():
    """counts={} is a VALID batched result (user has no items).
    Must NOT trigger legacy fallback (Reviews MUST-FIX 1).
    """
    mgr = Mock()
    mgr._get_list_item_count.return_value = 9

    assert _resolve_list_item_count("5", mgr, {}) == 0
    mgr._get_list_item_count.assert_not_called()


def test_counts_populated_returns_value():
    """counts={5: 3} returns 3 for list_id '5'."""
    mgr = Mock()
    assert _resolve_list_item_count("5", mgr, {5: 3}) == 3
