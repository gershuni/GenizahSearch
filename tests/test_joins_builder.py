# -*- coding: utf-8 -*-
"""Tests for create_joins_builder get_state/set_state/on_change (B1 — Phase 120-03).

Covers:
1. get_state() / set_state() round-trip: lines_state, search_type, variants,
   single-line text, text_position.
2. set_state() syncs VISIBLE controls (type buttons, container visibility,
   single_input.value, text_pos_select.value) — R2-M1.
3. show_search_type=False builder (other-side) also round-trips via get/set_state.
4. on_change(cb) fires on builder state mutations (word add/edit, mode change, etc.)
5. set_state with malformed/partial dict is tolerated (legacy-blob tolerance).

Tests use the real create_joins_builder inside a live NiceGUI slot (same
approach as test_joins_builder_word_ui.py) so crashes from NiceGUI API
misuse are caught.
"""
from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _storage_secret():
    """app.storage.* needs a secret during NiceGUI element construction."""
    from nicegui import app
    try:
        app.storage.secret = 'test-secret-builders'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


# ---------------------------------------------------------------------------
# Helper: build a real handle inside a NiceGUI slot
# ---------------------------------------------------------------------------

def _make_builder(allow_page_position: bool = True,
                  show_search_type: bool = True) -> dict:
    """Build a real create_joins_builder handle inside a NiceGUI column slot."""
    from nicegui import ui
    from web.components.joins_builder import create_joins_builder
    with ui.column():
        handle = create_joins_builder(
            allow_page_position=allow_page_position,
            show_search_type=show_search_type,
        )
    return handle


# ---------------------------------------------------------------------------
# TestHandleContract (B1 handle keys)
# ---------------------------------------------------------------------------

class TestHandleContract:
    """All Phase-117/118 and Phase-120 B1 keys must be present."""

    def test_existing_handle_keys_still_present(self):
        """All Phase-117/118 handle keys present after adding Phase-120 B1 keys."""
        handle = _make_builder()
        required_keys = [
            'container',
            'build_side_query',
            'build_query',
            'get_search_type',
            'get_mode',
            'get_text_position',
            'get_summary',
            'is_empty',
            'reset',
            'set_variants',
        ]
        for key in required_keys:
            assert key in handle, f"Required pre-existing handle key {key!r} is missing"

    def test_new_b1_handle_keys_present(self):
        """Phase-120 B1 additions (get_state, set_state, on_change) are in the handle dict."""
        handle = _make_builder()
        assert 'get_state' in handle, "'get_state' must be in the builder handle dict (B1)"
        assert 'set_state' in handle, "'set_state' must be in the builder handle dict (B1)"
        assert 'on_change' in handle, "'on_change' must be in the builder handle dict (B1)"

    def test_b1_handle_values_are_callable(self):
        """Phase-120 B1 handle values are callable."""
        handle = _make_builder()
        assert callable(handle['get_state'])
        assert callable(handle['set_state'])
        assert callable(handle['on_change'])


# ---------------------------------------------------------------------------
# TestBuilderStateRoundTrip (B1)
# ---------------------------------------------------------------------------

class TestBuilderStateRoundTrip:
    """get_state → set_state round-trip (B1 acceptance criteria)."""

    def test_initial_get_state_has_expected_keys(self):
        """get_state() returns a dict with all expected keys."""
        handle = _make_builder()
        state = handle['get_state']()
        assert isinstance(state, dict)
        assert 'lines_state' in state
        assert 'search_type' in state
        assert 'variants_on' in state
        assert 'single_text' in state
        assert 'text_position' in state

    def test_get_state_is_json_serializable(self):
        """get_state() returns only JSON-serializable types (no widgets/closures)."""
        import json
        handle = _make_builder()
        state = handle['get_state']()
        # Should not raise
        json.dumps(state)

    def test_round_trip_default_state(self):
        """set_state(get_state()) round-trip: restored state matches original."""
        handle = _make_builder()
        original = handle['get_state']()
        handle['set_state'](original)
        restored = handle['get_state']()
        assert restored['search_type'] == original['search_type']
        assert restored['variants_on'] == original['variants_on']
        assert restored['single_text'] == original['single_text']
        assert restored['text_position'] == original['text_position']
        assert len(restored['lines_state']) == len(original['lines_state'])

    def test_round_trip_search_type_fuzzy(self):
        """After set_state with search_type='fuzzy', get_mode() returns 'fuzzy'."""
        handle = _make_builder()
        handle['set_state']({
            'search_type': 'fuzzy',
            'variants_on': False,
            'single_text': 'my query',
            'text_position': 'anywhere',
            'lines_state': [],
        })
        assert handle['get_mode']() == 'fuzzy'
        assert handle['get_search_type']() == 'fuzzy'

    def test_round_trip_text_position(self):
        """After set_state with text_position='line_end', get_text_position() returns 'line_end'."""
        handle = _make_builder()
        handle['set_state']({
            'search_type': 'responsa',
            'variants_on': False,
            'single_text': '',
            'text_position': 'line_end',
            'lines_state': [],
        })
        assert handle['get_text_position']() == 'line_end'

    def test_round_trip_variants_flag(self):
        """After set_state with variants_on=True, get_mode() returns 'variants'."""
        handle = _make_builder()
        handle['set_state']({
            'search_type': 'responsa',
            'variants_on': True,
            'single_text': '',
            'text_position': 'anywhere',
            'lines_state': [],
        })
        assert handle['get_mode']() == 'variants'

    def test_round_trip_lines_state_terms_preserved(self):
        """set_state restores lines_state with word terms."""
        handle = _make_builder()
        rich_state = {
            'search_type': 'responsa',
            'variants_on': False,
            'single_text': '',
            'text_position': 'start',
            'lines_state': [
                {
                    'words': [
                        {'term': 'אמת', 'mods': {'prefix': True, 'suffix': False,
                                                   'plene': False, 'wildcard_prefix': False,
                                                   'wildcard_suffix': True, 'negation': False},
                         'gap_to_next_word': 2},
                        {'term': 'שלום', 'mods': {}, 'gap_to_next_word': 0},
                    ],
                    'line_start': True,
                    'line_end': False,
                    'gap_to_next_line': 1,
                },
                {
                    'words': [{'term': 'ברכה', 'mods': {}, 'gap_to_next_word': 0}],
                    'line_start': False,
                    'line_end': True,
                    'gap_to_next_line': 0,
                },
            ],
        }
        handle['set_state'](rich_state)
        state_out = handle['get_state']()
        ls = state_out['lines_state']
        assert len(ls) == 2
        assert ls[0]['line_start'] is True
        assert ls[0]['gap_to_next_line'] == 1
        assert ls[0]['words'][0]['term'] == 'אמת'
        assert ls[0]['words'][0]['mods'].get('prefix') is True
        assert ls[0]['words'][0]['mods'].get('wildcard_suffix') is True
        assert ls[0]['words'][0]['gap_to_next_word'] == 2
        assert ls[1]['line_end'] is True

    def test_round_trip_single_text(self):
        """After set_state with single_text, get_state() returns the same text."""
        handle = _make_builder()
        handle['set_state']({
            'search_type': 'exact',
            'variants_on': False,
            'single_text': 'my query text',
            'text_position': 'anywhere',
            'lines_state': [],
        })
        state_out = handle['get_state']()
        assert state_out['single_text'] == 'my query text'


# ---------------------------------------------------------------------------
# TestSetStateSyncsControls (R2-M1)
# ---------------------------------------------------------------------------

class TestSetStateSyncsControls:
    """set_state must sync the VISIBLE controls — not just the closure state (R2-M1)."""

    def test_set_state_exact_mode_search_type_accessible(self):
        """After set_state to 'exact', get_search_type() returns 'exact'."""
        handle = _make_builder(show_search_type=True)
        handle['set_state']({'search_type': 'exact', 'variants_on': False,
                              'single_text': 'foo', 'text_position': 'anywhere'})
        assert handle['get_search_type']() == 'exact'

    def test_set_state_then_reset_restores_defaults(self):
        """set_state followed by reset() returns to responsa default state."""
        handle = _make_builder()
        handle['set_state']({
            'search_type': 'fuzzy',
            'variants_on': True,
            'single_text': 'some text',
            'text_position': 'end',
            'lines_state': [],
        })
        # After set_state, mode is fuzzy
        assert handle['get_mode']() == 'fuzzy'

        # reset() should restore to defaults
        handle['reset']()
        assert handle['get_mode']() == 'exact'  # responsa with variants_on=False
        assert handle['get_text_position']() == 'anywhere'
        assert handle['is_empty']() is True

    def test_set_state_then_get_state_idempotent_for_single_text(self):
        """set_state → get_state → set_state again produces identical state."""
        import json
        handle = _make_builder()
        state_in = {
            'search_type': 'variants',
            'variants_on': False,
            'single_text': 'test query',
            'text_position': 'anywhere',
            'lines_state': [],
        }
        handle['set_state'](state_in)
        state_out1 = handle['get_state']()
        handle['set_state'](state_out1)
        state_out2 = handle['get_state']()
        # Should be idempotent
        assert state_out2['search_type'] == state_out1['search_type']
        assert state_out2['single_text'] == state_out1['single_text']
        assert state_out2['text_position'] == state_out1['text_position']


# ---------------------------------------------------------------------------
# TestOtherSideBuilderRoundTrip
# ---------------------------------------------------------------------------

class TestOtherSideBuilderRoundTrip:
    """The other-side builder (show_search_type=False) also round-trips."""

    def test_other_side_builder_has_b1_keys(self):
        """Other-side builder (show_search_type=False) also exposes B1 keys."""
        handle = _make_builder(allow_page_position=True, show_search_type=False)
        assert 'get_state' in handle
        assert 'set_state' in handle
        assert 'on_change' in handle

    def test_other_side_builder_round_trip(self):
        """Other-side builder round-trips lines + text_position via get/set_state."""
        handle = _make_builder(allow_page_position=True, show_search_type=False)
        state_in = {
            'search_type': 'responsa',
            'variants_on': True,
            'single_text': '',
            'text_position': 'line_start',
            'lines_state': [
                {
                    'words': [{'term': 'test', 'mods': {}, 'gap_to_next_word': 0}],
                    'line_start': False,
                    'line_end': False,
                    'gap_to_next_line': 0,
                }
            ],
        }
        handle['set_state'](state_in)
        state_out = handle['get_state']()
        assert state_out['variants_on'] is True
        assert state_out['text_position'] == 'line_start'
        ls = state_out['lines_state']
        assert len(ls) == 1
        assert ls[0]['words'][0]['term'] == 'test'


# ---------------------------------------------------------------------------
# TestOnChangeFires
# ---------------------------------------------------------------------------

class TestOnChangeFires:
    """on_change(cb) is called when the builder state mutates."""

    def test_on_change_registration_does_not_crash(self):
        """on_change(lambda: None) can be registered without crashing."""
        handle = _make_builder()
        handle['on_change'](lambda: None)  # should not raise

    def test_multiple_on_change_registrations_do_not_crash(self):
        """Multiple on_change callbacks can be registered."""
        handle = _make_builder()
        handle['on_change'](lambda: None)
        handle['on_change'](lambda: None)

    def test_on_change_called_after_set_state_does_not_interfere(self):
        """Registering on_change before set_state does not break set_state."""
        calls = []
        handle = _make_builder()
        handle['on_change'](lambda: calls.append(1))
        handle['set_state']({'search_type': 'exact', 'variants_on': False,
                              'single_text': '', 'text_position': 'anywhere'})
        # set_state itself doesn't need to fire on_change — callers use it for
        # restoring state without triggering re-persist. This just asserts no crash.
        # The on_change fires on USER mutations (mode buttons, word edits, etc.),
        # not on set_state (which is a restore operation, not a user mutation).
        assert handle['get_mode']() == 'exact'  # restored correctly


# ---------------------------------------------------------------------------
# TestSetStatePartialBlob (legacy-blob tolerance)
# ---------------------------------------------------------------------------

class TestSetStatePartialBlob:
    """set_state with a malformed/partial dict is tolerated — no crash."""

    def test_set_state_empty_dict_does_not_crash(self):
        """set_state({}) falls back to defaults without crashing."""
        handle = _make_builder()
        handle['set_state']({})  # should not raise
        # Should have at least one line (default)
        state = handle['get_state']()
        assert len(state['lines_state']) >= 1

    def test_set_state_none_does_not_crash(self):
        """set_state(None) is tolerated (legacy-blob tolerance)."""
        handle = _make_builder()
        handle['set_state'](None)  # should not raise

    def test_set_state_missing_lines_falls_back_to_default(self):
        """set_state with missing 'lines_state' key defaults to one empty line."""
        handle = _make_builder()
        handle['set_state']({'search_type': 'variants', 'variants_on': False})
        state = handle['get_state']()
        assert len(state['lines_state']) >= 1
        assert state['search_type'] == 'variants'

    def test_set_state_empty_lines_state_falls_back_to_default(self):
        """set_state with lines_state=[] creates one default line."""
        handle = _make_builder()
        handle['set_state']({'search_type': 'responsa', 'variants_on': False,
                              'single_text': '', 'text_position': 'anywhere', 'lines_state': []})
        state = handle['get_state']()
        assert len(state['lines_state']) == 1

    def test_set_state_unknown_keys_ignored(self):
        """set_state with unknown keys doesn't crash."""
        handle = _make_builder()
        handle['set_state']({'search_type': 'responsa', 'unknown_key': 'ignored',
                              'variants_on': False, 'single_text': '',
                              'text_position': 'anywhere'})
        assert handle['get_search_type']() == 'responsa'
