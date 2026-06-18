"""Phase 118 UAT follow-ups:
  - a page-level "New Search" button (restart_alt) clears the query + results
    while keeping the anchor (parity with /search);
  - the builder handle exposes reset() that returns it to one empty line / Exact
    / Anywhere;
  - the lone word box is full-width (flex-basis 100%) so its placeholder is never
    clipped;
  - a mode/syntax info popup (help_outline) is present next to the Mode buttons.

Helpers scope every query to the builder's OWN subtree — the shared NiceGUI
auto-index client accumulates elements across tests in a session.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from web.components.joins_builder import create_joins_builder


@pytest.fixture(autouse=True)
def _storage_secret():
    from nicegui import app
    try:
        app.storage.secret = 'test-secret'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


def _walk(el):
    for slot in el.slots.values():
        for child in slot.children:
            yield child
            yield from _walk(child)


def _has_class(el, cls):
    return cls in ' '.join(getattr(el, '_classes', []) or [])


def _has_class_ancestor(el, cls):
    slot = getattr(el, 'parent_slot', None)
    p = slot.parent if slot else None
    while p is not None:
        if _has_class(p, cls):
            return True
        slot = getattr(p, 'parent_slot', None)
        p = slot.parent if slot else None
    return False


def _build():
    """Construct the builder in a throwaway column; return (handle, root_column)."""
    from nicegui import ui
    col = ui.column()
    with col:
        handle = create_joins_builder(allow_page_position=True)
    return handle, col


def _word_inputs(root):
    from nicegui import ui
    return [el for el in _walk(root) if isinstance(el, ui.input) and _has_class(el, 'jl-word-rtl')]


def _fire_click(btn):
    for listener in list(btn._event_listeners.values()):
        if getattr(listener, 'type', None) == 'click':
            listener.handler()
            return True
    return False


def _fire_term_change(inp, text):
    """Fire the word input's model-value listener (NiceGUI normalizes the event
    name to 'update:modelValue') with a stub event carrying .args."""
    for listener in list(inp._event_listeners.values()):
        if 'modelValue' in (getattr(listener, 'type', '') or ''):
            listener.handler(SimpleNamespace(args=text))
            return True
    return False


# ---------------------------------------------------------------------------
# Builder reset()
# ---------------------------------------------------------------------------

class TestBuilderReset:
    def test_reset_returns_to_clean_defaults(self):
        from nicegui import ui
        handle, root = _build()

        # Populate: type a word, add a second word, switch to Fuzzy, set Text Position
        inputs = _word_inputs(root)
        assert _fire_term_change(inputs[0], 'אמר')
        assert not handle['is_empty']()

        add_word = [
            el for el in _walk(root) if isinstance(el, ui.button)
            and el._props.get('icon') == 'add' and _has_class_ancestor(el, 'jl-words-row')
        ]
        assert _fire_click(add_word[0])
        assert len(_word_inputs(root)) >= 2

        from web.translations import tr
        fuzzy_btns = [el for el in _walk(root) if isinstance(el, ui.button) and el.text == tr('Fuzzy')]
        assert fuzzy_btns and _fire_click(fuzzy_btns[0])
        assert handle['get_mode']() == 'fuzzy'

        selects = [el for el in _walk(root) if isinstance(el, ui.select)]
        assert selects, 'Text Position select expected'
        selects[0].value = 'start'  # programmatic .value fires on_value_change
        assert handle['get_text_position']() == 'start'

        # Reset
        handle['reset']()

        assert handle['is_empty']() is True
        assert handle['get_mode']() == 'exact'
        assert handle['get_text_position']() == 'anywhere'
        assert len(_word_inputs(root)) == 1, 'reset collapses back to one word box'

    def test_reset_handle_exposed_for_both_sides(self):
        from nicegui import ui
        with ui.column():
            anchor = create_joins_builder(allow_page_position=True)
        with ui.column():
            other = create_joins_builder(allow_page_position=False)
        assert callable(anchor.get('reset'))
        assert callable(other.get('reset'))
        # Other side has no Text Position select; reset() must not raise.
        other['reset']()


# ---------------------------------------------------------------------------
# Wider lone word box + info popup
# ---------------------------------------------------------------------------

def test_lone_word_box_is_full_width():
    handle, root = _build()
    inputs = _word_inputs(root)
    assert len(inputs) == 1
    col_style = dict(getattr(inputs[0].parent_slot.parent, '_style', {}) or {})
    # flex-basis 100% guarantees the placeholder is never clipped
    assert col_style.get('flex') == '1 1 100%', f'lone box should be full-width, got {col_style}'


def test_mode_info_popup_present():
    from nicegui import ui
    handle, root = _build()
    info_btns = [
        el for el in _walk(root) if isinstance(el, ui.button)
        and el._props.get('icon') == 'help_outline'
    ]
    assert info_btns, 'expected a help_outline info button next to the Mode buttons'


# ---------------------------------------------------------------------------
# Page-level New Search button
# ---------------------------------------------------------------------------

def test_page_has_new_search_button():
    from nicegui import context, ui
    from web.pages.joins_lab import create_joins_lab_page

    assert context.slot_stack, 'expected an active NiceGUI slot'
    with ui.column() as page_root:
        create_joins_lab_page()

    reset_btns = [
        el for el in _walk(page_root) if isinstance(el, ui.button)
        and el._props.get('icon') == 'restart_alt'
    ]
    assert reset_btns, 'expected a restart_alt "New Search" button on the page'
