"""Precedence tests for browse bootstrap (Phase 74, D-19).

Mirrors tests/test_search_bootstrap.py. The resolver is a pure function -
no fixtures, no mocks, no NiceGUI imports. Inputs are plain dicts;
output is a dispatch dict the caller uses to schedule load_page().
"""
from web.browse_bootstrap import resolve_browse_bootstrap


def test_explicit_sys_id_beats_saved_position():
    """Case (a): explicit sys_id in URL beats saved browse_position (no matching desk)."""
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
    assert result['sys_id'] == '003750'


def test_reading_desk_restore_wins_over_position():
    """Case (c): saved reading desk takes priority over browse_position (blank URL)."""
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


def test_explicit_sys_id_matching_desk_restores_desk():
    """Language-switch: sys_id in URL matches a reading-desk entry -> restore desk."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id='003750',
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk={'entries': [{'sys_id': '003750', 'shelfmark': 'T-S 12.1'}]},
        saved_position=None,
    )
    assert result['action'] == 'restore_desk'


def test_no_context_no_action():
    """Blank /browse with no saved state: action=none."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id=None,
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,
        saved_position=None,
    )
    assert result['action'] == 'none'


def test_fl_id_trumps_everything():
    """fl_id in URL wins over both desk and position."""
    result = resolve_browse_bootstrap(
        initial_fl_id='T-S 12.1.1r',
        initial_sys_id='999',
        initial_page=5,
        pending_shelfmark=None,
        saved_reading_desk={'entries': [{'sys_id': '003750'}]},
        saved_position={'sys_id': '000001', 'p_num': 3, 'shelfmark': 'T-S 1.1', 'volume_ie': None},
    )
    assert result['action'] == 'fl_id'
    assert result['fl_id'] == 'T-S 12.1.1r'


# Codex MEDIUM #7: side-effect coverage for volume_ie + shelfmark pass-through.
# The dispatch in browse.py restore_position branch must re-apply
# state.shelfmark_query and volume_ie from saved_position. These tests assert
# the resolver's result dict carries those values forward.

def test_restore_position_passes_shelfmark_and_volume_ie():
    """restore_position result includes saved shelfmark AND volume_ie."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id=None,
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,
        saved_position={
            'sys_id': '003750',
            'p_num': 3,
            'shelfmark': 'T-S 12.1',
            'volume_ie': 'IE12345',
        },
    )
    assert result['action'] == 'restore_position'
    assert result['shelfmark'] == 'T-S 12.1'
    assert result['volume_ie'] == 'IE12345'


def test_restore_position_handles_none_volume_ie():
    """Saved position with volume_ie=None returns volume_ie=None in result."""
    result = resolve_browse_bootstrap(
        initial_fl_id=None,
        initial_sys_id=None,
        initial_page=1,
        pending_shelfmark=None,
        saved_reading_desk=None,
        saved_position={
            'sys_id': '003750',
            'p_num': 1,
            'shelfmark': 'T-S 12.1',
            'volume_ie': None,
        },
    )
    assert result['action'] == 'restore_position'
    assert result['volume_ie'] is None
    # (caller's live-code volume validation is tested by the manual D-22 web
    # smoke - the resolver is pure and does not query get_volumes_for_sys_id.)
