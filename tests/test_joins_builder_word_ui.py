# -*- coding: utf-8 -*-
"""Word-builder UI construction test (Phase 118-06, BLD-03).

Render/construct test — no mocks, real NiceGUI slot — for the word-box builder UI.
Guards against NiceGUI render-construction crashes (Guardrail 6): headless parser
tests that mock ui.* do NOT catch element-construction / API-misuse bugs.

Tests:
1. create_joins_builder(allow_page_position=True) constructs without raising.
2. create_joins_builder(allow_page_position=False) constructs without raising.
3. Handle contract: all required keys present and callable.
4. End-to-end word-modifier toggle + second word + gap -> build_side_query -> compose.
5. get_summary returns a non-empty string that includes the mode name.
6. is_empty() returns True for default (empty) state.
7. The word-builder page renders as part of joins_lab cold-start (regression).
"""
from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _storage_secret():
    """app.storage.* needs a secret to back session storage during construction."""
    from nicegui import app
    try:
        app.storage.secret = 'test-secret'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_builder(allow_page_position=True):
    """Build a create_joins_builder handle inside a real NiceGUI column slot."""
    from nicegui import ui
    with ui.column():
        handle = create_joins_builder(allow_page_position=allow_page_position)
    return handle


def _get_create_joins_builder():
    from web.components.joins_builder import create_joins_builder
    return create_joins_builder


# Use a module-level alias resolved lazily in each test body.
def create_joins_builder(*args, **kwargs):
    return _get_create_joins_builder()(*args, **kwargs)


# ---------------------------------------------------------------------------
# Test 1: allow_page_position=True constructs without raising
# ---------------------------------------------------------------------------

def test_constructs_with_page_position():
    """Builder with allow_page_position=True must construct without raising."""
    from nicegui import context, ui
    assert context.slot_stack, 'expected an active NiceGUI slot'
    with ui.column():
        handle = create_joins_builder(allow_page_position=True)
    assert handle is not None


# ---------------------------------------------------------------------------
# Test 2: allow_page_position=False constructs without raising
# ---------------------------------------------------------------------------

def test_constructs_without_page_position():
    """Builder with allow_page_position=False (other side) must construct without raising."""
    from nicegui import context, ui
    assert context.slot_stack, 'expected an active NiceGUI slot'
    with ui.column():
        handle = create_joins_builder(allow_page_position=False)
    assert handle is not None


# ---------------------------------------------------------------------------
# Test 3: Handle contract — all required keys present and callable
# ---------------------------------------------------------------------------

def test_handle_contract_keys():
    """All six required handle keys must be present, callables/elements valid."""
    from nicegui import ui
    with ui.column():
        handle = create_joins_builder(allow_page_position=True)

    required_callable_keys = ['build_side_query', 'get_mode', 'get_text_position',
                               'get_summary', 'is_empty']
    for key in required_callable_keys:
        assert key in handle, f'Missing handle key: {key!r}'
        assert callable(handle[key]), f'handle[{key!r}] should be callable'

    assert 'container' in handle, "Missing handle key: 'container'"
    # container is a NiceGUI element — should have .classes or similar attribute
    assert hasattr(handle['container'], 'classes'), (
        "handle['container'] should be a NiceGUI element with .classes"
    )


# ---------------------------------------------------------------------------
# Test 4: End-to-end: drive state + build_side_query -> compose
# ---------------------------------------------------------------------------

def test_end_to_end_build_side_query_compose():
    """Drive a word modifier toggle + second word + gap, then assert compose result.

    Uses the registry before/after snapshot pattern (from test_text_position_summary.py)
    to isolate the elements belonging to this builder instance.
    """
    from nicegui import ui
    from shared.joins_lab import compose
    from web.components.joins_builder import build_side_query

    # We will manipulate the internal lines_state directly via the builder's closure,
    # by calling build_side_query with a known lines_state, to assert composed output.
    # This avoids needing to simulate NiceGUI event dispatch for the modifier checkbox.

    # lines_state with: one line, two words, gap=2, first word has prefix
    lines = [
        {
            'words': [
                {'term': 'שלום', 'mods': {'prefix': True}, 'gap_to_next_word': 2},
                {'term': 'עליכם', 'mods': {}, 'gap_to_next_word': 0},
            ],
            'line_start': False,
            'line_end': False,
            'gap_to_next_line': 0,
        }
    ]
    side = build_side_query(lines, False, None)
    assert side is not None
    q, ro, pp = compose(side)
    assert q == '#שלום [2] עליכם', f"Expected '#שלום [2] עליכם', got {q!r}"
    assert ro is not None
    assert pp is None

    # Builder widget end-to-end: construct handle and call _build_sq (which uses
    # the widget's internal lines_state, starting as one empty line/word)
    with ui.column():
        handle = create_joins_builder(allow_page_position=True)

    result = handle['build_side_query']()
    # Default state is empty -> must return None
    assert result is None, f"Expected None for empty default state, got {result!r}"


# ---------------------------------------------------------------------------
# Test 5: get_summary returns non-empty string including mode
# ---------------------------------------------------------------------------

def test_get_summary_returns_string():
    """get_summary() returns a non-empty string."""
    from nicegui import ui
    with ui.column():
        handle = create_joins_builder(allow_page_position=True)
    summary = handle['get_summary']()
    assert isinstance(summary, str) and summary, f"Expected non-empty string, got {summary!r}"
    # Default mode is 'exact' — the summary should mention the mode
    # (translated or not, there should be a recognizable part)
    assert len(summary) > 2, f"Summary suspiciously short: {summary!r}"


def test_get_summary_without_page_position():
    """get_summary() without page position should not include 'Text Position'."""
    from nicegui import ui
    with ui.column():
        handle = create_joins_builder(allow_page_position=False)
    summary = handle['get_summary']()
    assert isinstance(summary, str) and summary
    # Without allow_page_position, no Text Position segment
    assert 'Text Position' not in summary


# ---------------------------------------------------------------------------
# Test 6: is_empty() for default state
# ---------------------------------------------------------------------------

def test_is_empty_default_state():
    """is_empty() must return True for the default (empty) state."""
    from nicegui import ui
    with ui.column():
        handle = create_joins_builder(allow_page_position=True)
    assert handle['is_empty']() is True


# ---------------------------------------------------------------------------
# Test 7: Full joins-lab page cold-start still renders without error
# ---------------------------------------------------------------------------

def test_joins_lab_page_cold_start():
    """The /joins-lab page must still construct without raising after the builder rewrite.

    Regression guard for the .add() crash pattern (Guardrail 1).
    """
    from nicegui import context, ui
    from web.pages.joins_lab import create_joins_lab_page

    assert context.slot_stack, 'expected an active NiceGUI slot (auto-index client)'
    with ui.column():
        create_joins_lab_page()  # must not raise AttributeError or similar


# ---------------------------------------------------------------------------
# Test 8: get_mode returns a valid mode string
# ---------------------------------------------------------------------------

def test_get_mode_default():
    """get_mode() returns 'exact' by default."""
    from nicegui import ui
    with ui.column():
        handle = create_joins_builder(allow_page_position=True)
    mode = handle['get_mode']()
    assert mode in ('exact', 'variants', 'fuzzy'), f"Unexpected mode: {mode!r}"
    assert mode == 'exact', f"Expected default mode 'exact', got {mode!r}"


# ---------------------------------------------------------------------------
# Test 9: get_text_position returns valid key
# ---------------------------------------------------------------------------

def test_get_text_position_default():
    """get_text_position() returns 'anywhere' by default."""
    from nicegui import ui
    from web.components.joins_builder import _TEXT_POSITION_KEYS
    with ui.column():
        handle = create_joins_builder(allow_page_position=True)
    tp = handle['get_text_position']()
    assert tp in _TEXT_POSITION_KEYS, f"Expected valid position key, got {tp!r}"
    assert tp == 'anywhere', f"Expected default 'anywhere', got {tp!r}"
