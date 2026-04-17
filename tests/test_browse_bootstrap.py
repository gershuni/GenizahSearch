"""Precedence tests for browse bootstrap (Phase 74, D-19).

Stubs in Plan 74-01; helper implementation arrives in Plan 74-02.
"""
import pytest


def _import_helper():
    try:
        from web.browse_bootstrap import resolve_browse_bootstrap
    except ImportError:
        pytest.skip("resolve_browse_bootstrap not yet implemented (Plan 74-02)")
    return resolve_browse_bootstrap


def test_explicit_sys_id_beats_saved_position():
    """Case (a): explicit sys_id in URL beats saved browse_position (no matching desk)."""
    resolve_browse_bootstrap = _import_helper()
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id='003750',
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,
        saved_position={'sys_id': '000001', 'p_num': 3, 'shelfmark': 'T-S 1.1', 'volume_ie': None},
    )
    assert result['action'] == 'sys_id'
    assert result['sys_id'] == '003750'


def test_blank_browse_restores_saved_position():
    """Case (b): no URL params - restore from browse_position."""
    resolve_browse_bootstrap = _import_helper()
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id=None,
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,
        saved_position={'sys_id': '003750', 'p_num': 3, 'shelfmark': 'T-S 12.1', 'volume_ie': None},
    )
    assert result['action'] == 'restore_position'
    assert result['p_num'] == 3


def test_reading_desk_restore_wins_over_position():
    """Case (c): saved reading desk takes priority over browse_position."""
    resolve_browse_bootstrap = _import_helper()
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id=None,
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk={'entries': [{'sys_id': '003750', 'shelfmark': 'T-S 12.1'}]},
        saved_position={'sys_id': '999999', 'p_num': 2, 'shelfmark': 'T-S Old', 'volume_ie': None},
    )
    assert result['action'] == 'restore_desk'
    assert result.get('restore_desk') is True
