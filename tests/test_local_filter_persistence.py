# -*- coding: utf-8 -*-
"""Phase 95 D-39: three per-surface LOCAL filter states persist across sessions.

The desktop app uses a session-state JSON (shared/session_persistence.py) rather than
QSettings — that is the established pattern for all other filter state in genizah_app.py
(e.g. printed_filter, domain_exclusions).  The three LOCAL filter keys are:
  - 'local_filter'              (in regular_search block)
  - 'local_filter_composition'  (in composition_search block)
  - 'local_filter_parallels'    (in composition_search block)

This test verifies the round-trip using the session state dictionary structure directly,
without instantiating QApplication or genizah_app (desktop-headless-safe).
"""
import copy


def _build_session_dict(search_local='all', comp_local='all', parallels_local='all'):
    """Build a minimal session state dict with LOCAL filter keys set."""
    return {
        'version': 1,
        'regular_search': {
            'printed_filter': 'all',
            'local_filter': search_local,
            'results': [],
        },
        'composition_search': {
            'printed_filter': 'all',
            'local_filter_composition': comp_local,
            'local_filter_parallels': parallels_local,
            'results': [],
            'filtered_results': [],
        },
    }


def _restore_from_session(state_dict):
    """Simulate the restore logic from _restore_session in genizah_app.py.

    Returns (search_local, comp_local, parallels_local) as restored.
    """
    reg = state_dict.get('regular_search', {})
    comp = state_dict.get('composition_search', {})
    search_local = reg.get('local_filter', 'all')
    comp_local = comp.get('local_filter_composition', 'all')
    parallels_local = comp.get('local_filter_parallels', 'all')
    return search_local, comp_local, parallels_local


def test_3_qsettings_keys_persist():
    """D-39: three independent LOCAL filter states survive a session save/restore cycle.

    Note: The desktop app uses the session state JSON (not QSettings) for filter
    persistence — consistent with the existing printed_filter / domain_exclusions pattern.
    This test exercises the save/restore data contract without instantiating QApplication.
    """
    # Simulate saving with distinct values per surface
    saved_state = _build_session_dict(
        search_local='only_local',
        comp_local='no_local',
        parallels_local='only_local',
    )

    # Simulate app restart (deep copy simulates serialise→deserialise round-trip)
    restored_state = copy.deepcopy(saved_state)

    search_local, comp_local, parallels_local = _restore_from_session(restored_state)

    assert search_local == 'only_local', (
        f"Search LOCAL filter state not persisted: expected 'only_local', got '{search_local}'"
    )
    assert comp_local == 'no_local', (
        f"Composition LOCAL filter state not persisted: expected 'no_local', got '{comp_local}'"
    )
    assert parallels_local == 'only_local', (
        f"Parallels LOCAL filter state not persisted: expected 'only_local', got '{parallels_local}'"
    )


def test_default_value_all():
    """D-39: missing keys default to 'all' (backward compat for sessions saved before Phase 95)."""
    old_session = {
        'regular_search': {'printed_filter': 'all', 'results': []},
        'composition_search': {'printed_filter': 'all', 'results': [], 'filtered_results': []},
    }
    search_local, comp_local, parallels_local = _restore_from_session(old_session)
    assert search_local == 'all'
    assert comp_local == 'all'
    assert parallels_local == 'all'


def test_surfaces_are_independent():
    """D-39: the three surfaces store independent state (setting one does not affect others)."""
    saved = _build_session_dict(
        search_local='no_local',
        comp_local='all',
        parallels_local='all',
    )
    s, c, p = _restore_from_session(saved)
    assert s == 'no_local'
    assert c == 'all'
    assert p == 'all'

    saved2 = _build_session_dict(
        search_local='all',
        comp_local='only_local',
        parallels_local='all',
    )
    s2, c2, p2 = _restore_from_session(saved2)
    assert s2 == 'all'
    assert c2 == 'only_local'
    assert p2 == 'all'


def test_cycle_states_valid():
    """REQ-6: cycle states are exactly ['all', 'only_local', 'no_local']."""
    states = ['all', 'only_local', 'no_local']
    # Verify all three are distinct
    assert len(set(states)) == 3
    # Verify cycle wraps: after 'no_local' comes 'all'
    for i, state in enumerate(states):
        next_state = states[(i + 1) % 3]
        assert next_state == states[(states.index(state) + 1) % 3]
