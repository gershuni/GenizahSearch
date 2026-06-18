"""D-Q1 redesign: the segmented search-type selector.

Responsa-style (default) is the structured builder with a Variants toggle; the
Exact/Variants/Fuzzy/Regex types collapse to a single free-text line that runs
the STANDARD search path (so Fuzzy is real edit-distance and Regex a real regex,
fixing CR HIGH-7). build_query() returns a unified descriptor the page consumes.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from web.components.joins_builder import create_joins_builder
from web.translations import tr


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


def _build(show_search_type=True):
    from nicegui import ui
    col = ui.column()
    with col:
        handle = create_joins_builder(
            allow_page_position=True, show_search_type=show_search_type
        )
    return handle, col


def _type_button(root, label_key):
    from nicegui import ui
    want = tr(label_key)
    return [el for el in _walk(root) if isinstance(el, ui.button) and el.text == want]


def _fire_click(btn):
    for listener in list(btn._event_listeners.values()):
        if getattr(listener, 'type', None) == 'click':
            listener.handler()
            return True
    return False


def _single_input(root):
    from nicegui import ui
    return [el for el in _walk(root) if isinstance(el, ui.input) and _has_class(el, 'jl-single-rtl')]


def _word_inputs(root):
    from nicegui import ui
    return [el for el in _walk(root) if isinstance(el, ui.input) and _has_class(el, 'jl-word-rtl')]


def _fire_model_change(el, value):
    for listener in list(el._event_listeners.values()):
        if 'modelValue' in (getattr(listener, 'type', '') or ''):
            listener.handler(SimpleNamespace(args=value))
            return True
    return False


# ---------------------------------------------------------------------------

def test_default_type_is_responsa():
    handle, root = _build()
    assert handle['get_search_type']() == 'responsa'
    assert handle['build_query']()['kind'] == 'responsa'


def test_switch_to_exact_single_line():
    handle, root = _build()
    btns = _type_button(root, 'Exact')
    assert btns and _fire_click(btns[0])
    assert handle['get_search_type']() == 'exact'

    # empty until the single-line box has text
    assert handle['is_empty']() is True
    si = _single_input(root)
    assert len(si) == 1
    assert _fire_model_change(si[0], 'אמר רבי')
    assert handle['is_empty']() is False

    q = handle['build_query']()
    assert q == {'kind': 'simple', 'mode': 'exact', 'query': 'אמר רבי'}


def test_regex_type_maps_to_engine_Regex():
    handle, root = _build()
    assert _fire_click(_type_button(root, 'Regex')[0])
    si = _single_input(root)
    assert _fire_model_change(si[0], 'אמ.*רבי')
    q = handle['build_query']()
    assert q['kind'] == 'simple'
    assert q['mode'] == 'Regex'  # capital R — the engine checks the literal


def test_fuzzy_type_is_simple_not_responsa():
    handle, root = _build()
    assert _fire_click(_type_button(root, 'Fuzzy')[0])
    si = _single_input(root)
    assert _fire_model_change(si[0], 'עקיבא')
    q = handle['build_query']()
    assert q == {'kind': 'simple', 'mode': 'fuzzy', 'query': 'עקיבא'}


def test_variants_checkbox_sets_side_variants():
    from nicegui import ui
    handle, root = _build()
    # type a structured word so build_side_query yields a non-empty SideQuery
    wi = _word_inputs(root)
    assert _fire_model_change(wi[0], 'אמר')
    # find + check the Variants checkbox
    cbs = [el for el in _walk(root) if isinstance(el, ui.checkbox) and el.text == tr('Variants')]
    assert cbs, 'Variants checkbox expected'
    cbs[0].set_value(True)
    q = handle['build_query']()
    assert q['kind'] == 'responsa'
    assert q['side'] is not None and q['side'].variants is True


def test_single_input_distinct_class_from_word_boxes():
    handle, root = _build()
    si = _single_input(root)
    assert len(si) == 1
    # the single-line box must NOT carry jl-word-rtl (keeps structured-box helpers clean)
    assert not _has_class(si[0], 'jl-word-rtl')


def test_other_side_has_no_type_selector():
    handle, root = _build(show_search_type=False)
    assert handle['get_search_type']() == 'responsa'
    # none of the single-line type buttons should be present
    for key in ('Exact', 'Fuzzy', 'Regex'):
        assert not _type_button(root, key), f'{key} button must be absent on the other side'


def test_reset_from_single_line_restores_responsa():
    handle, root = _build()
    assert _fire_click(_type_button(root, 'Fuzzy')[0])
    assert _fire_model_change(_single_input(root)[0], 'עקיבא')
    assert handle['is_empty']() is False

    handle['reset']()
    assert handle['get_search_type']() == 'responsa'
    assert handle['is_empty']() is True
    assert handle['build_query']()['kind'] == 'responsa'
