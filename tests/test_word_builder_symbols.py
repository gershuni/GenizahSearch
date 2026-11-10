"""Phase 118 UAT follow-ups on the word builder:
  - modifier symbols beneath a word box are REMOVED when the modifier is unchecked
    (read the checkbox's synced .value; an uncheck must clear the symbol);
  - a lone word gets a WIDE box with the explanatory placeholder;
  - once a second word is added the boxes trim to word-width and the placeholder is
    dropped.

Helpers scope every query to the builder's OWN subtree — the shared NiceGUI
auto-index client accumulates elements across tests in a session, so a registry-wide
scan would see other tests' boxes.
"""

from __future__ import annotations

import pytest

from web.components.joins_builder import _WORD_MOD_TABLE, create_joins_builder


@pytest.fixture(autouse=True)
def _storage_secret():
    from nicegui import app
    try:
        app.storage.secret = 'test-secret'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


def _build_container():
    from nicegui import ui
    col = ui.column()
    with col:
        create_joins_builder(allow_page_position=True)
    return col


def _walk(el):
    for slot in el.slots.values():
        for child in slot.children:
            yield child
            yield from _walk(child)


def _has_class(el, cls):
    return cls in ' '.join(getattr(el, '_classes', []) or [])


def _word_inputs(root):
    from nicegui import ui
    return [el for el in _walk(root) if isinstance(el, ui.input) and _has_class(el, 'jl-word-rtl')]


def _symbol_labels(root, symbol):
    from nicegui import ui
    return [
        el for el in _walk(root)
        if isinstance(el, ui.label) and el.text == symbol
        and 'primary-600' in str(dict(getattr(el, '_style', {}) or {}))
    ]


def _fire_click(btn):
    for listener in list(btn._event_listeners.values()):
        if getattr(listener, 'type', None) == 'click':
            listener.handler()
            return True
    return False


def test_symbol_removed_when_modifier_unchecked():
    from nicegui import ui
    root = _build_container()
    symbols = [s for _, s, _, _ in _WORD_MOD_TABLE]
    cbs = [el for el in _walk(root) if isinstance(el, ui.checkbox)
           and any(el.text.startswith(s) for s in symbols)]
    assert cbs, 'no modifier checkboxes found'
    cb = cbs[0]
    sym = cb.text.split()[0]

    assert len(_symbol_labels(root, sym)) == 0
    cb.set_value(True)              # check -> symbol appears
    assert len(_symbol_labels(root, sym)) == 1
    cb.set_value(False)            # uncheck -> symbol MUST be removed
    assert len(_symbol_labels(root, sym)) == 0


def test_single_word_box_is_wide_with_placeholder():
    root = _build_container()
    inputs = _word_inputs(root)
    assert len(inputs) == 1, 'default line should have exactly one word box'
    assert (inputs[0]._props.get('placeholder') or '').strip(), 'lone box needs a placeholder'
    col_style = dict(getattr(inputs[0].parent_slot.parent, '_style', {}) or {})
    assert col_style.get('min-width') == '280px' or 'flex' in col_style, \
        f'lone word box should be wide, got {col_style}'


def test_added_word_boxes_are_narrow_without_placeholder():
    from nicegui import ui
    root = _build_container()
    add_word = [
        el for el in _walk(root) if isinstance(el, ui.button)
        and el._props.get('icon') == 'add' and _has_class_ancestor(el, 'jl-words-row')
    ]
    assert add_word, '+ Add word button not found inside words row'
    assert _fire_click(add_word[0])

    inputs = _word_inputs(root)
    assert len(inputs) >= 2, 'expected 2+ word boxes after Add word'
    for inp in inputs:
        assert not (inp._props.get('placeholder') or '').strip(), \
            'multi-word boxes should drop the placeholder'
        col_style = dict(getattr(inp.parent_slot.parent, '_style', {}) or {})
        assert col_style.get('max-width') == '180px', \
            f'multi-word box should be narrow, got {col_style}'


def _parent_of(el):
    """The element's parent, or None once the chain ends OR leaves this test's
    own tree.

    `Element._parent_slot` is a WEAKREF, so walking up past our throwaway root
    reaches an ambient slot owned by whatever ran before us; if it has been
    collected, `.parent_slot` raises. A collected ancestor cannot carry the
    class we are looking for. Same guard as
    tests/test_joins_lab_new_search_reset.py — see the note there.
    """
    try:
        slot = getattr(el, 'parent_slot', None)
    except RuntimeError:
        return None
    return slot.parent if slot else None


def _has_class_ancestor(el, cls):
    p = _parent_of(el)
    while p is not None:
        if _has_class(p, cls):
            return True
        p = _parent_of(p)
    return False
