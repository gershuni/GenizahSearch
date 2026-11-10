"""Regression: Text Position selection must not crash the search/summary path.

Phase 118 hotfix. The Text Position ``ui.select`` uses dict options
(``{key: label}``). Its handler stored the raw Quasar ``update:model-value``
payload (``e.args``), which for a dict-options select is the option OBJECT
``{'label': ..., 'value': ...}`` — a dict. That dict landed in
``text_position_state['value']``; then ``_get_summary`` did
``_TEXT_POSITION_LABEL_KEYS.get(tp, tp)`` and blew up with::

    TypeError: unhashable type: 'dict'

the moment a user picked "Start of text" / "Start of line" and ran a search
(``execute_joins_search`` -> ``_collapse_builder(get_summary())``). It also
silently broke ``_build_sq`` (``tp in ('start','end')`` is False for a dict, so
page_position was wrongly dropped).

Guarded two ways: the handler now stores the element's normalized ``.value`` (the
option KEY), and ``_get_text_position`` runs everything through
``_coerce_text_position``. Both layers covered below.
"""

from __future__ import annotations

import pytest

from web.components.joins_builder import (
    _TEXT_POSITION_KEYS,
    _coerce_text_position,
    create_joins_builder,
)


@pytest.fixture(autouse=True)
def _storage_secret():
    from nicegui import app
    try:
        app.storage.secret = 'test-secret'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


# --- pure coercion guard (fast, no NiceGUI render) -------------------------

class TestCoerceTextPosition:
    def test_quasar_option_object_maps_to_key(self):
        assert _coerce_text_position({'label': 'Start of text', 'value': 'start'}) == 'start'

    def test_quasar_object_line_start(self):
        assert _coerce_text_position({'label': 'Line starts', 'value': 'line_start'}) == 'line_start'

    def test_plain_key_passes_through(self):
        for k in _TEXT_POSITION_KEYS:
            assert _coerce_text_position(k) == k

    def test_unknown_string_falls_back_to_anywhere(self):
        assert _coerce_text_position('garbage') == 'anywhere'

    def test_dict_without_value_falls_back(self):
        assert _coerce_text_position({'bogus': 1}) == 'anywhere'

    def test_result_is_always_hashable_str(self):
        for v in ['start', {'value': 'end'}, {'x': 1}, 'nope', 'line_end']:
            out = _coerce_text_position(v)
            assert isinstance(out, str)
            # the exact failing op in _get_summary — must not raise
            {out: 1}.get(out)


# --- end-to-end: drive the real select, then read summary ------------------

def _build_and_get_select():
    """Build the widget and return (handle, its own Text Position select).

    The auto-index client's element registry accumulates across the whole test
    session, so snapshot ids before/after THIS build to grab the select that
    belongs to this builder (not a stale one from an earlier parametrization).
    """
    from nicegui import context, ui

    before = set(context.client.elements.keys())
    with ui.column():
        handle = create_joins_builder(allow_page_position=True)
    new = [
        el for eid, el in context.client.elements.items()
        if eid not in before and isinstance(el, ui.select)
        and isinstance(getattr(el, 'options', None), dict)
        and set(el.options.keys()) == set(_TEXT_POSITION_KEYS)
    ]
    assert len(new) == 1, f'expected exactly one Text Position select, found {len(new)}'
    return handle, new[0]


@pytest.mark.parametrize('key', ['start', 'end', 'line_start', 'line_end', 'anywhere'])
def test_selecting_position_then_summary_does_not_crash(key):
    handle, sel = _build_and_get_select()
    sel.value = key  # programmatic set fires on_value_change -> stores the KEY

    tp = handle['get_text_position']()
    assert tp == key
    assert isinstance(tp, str)

    # The exact crash site: building the collapsed summary must not raise.
    summary = handle['get_summary']()
    assert isinstance(summary, str) and summary
