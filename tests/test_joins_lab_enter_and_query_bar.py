"""Phase 118 UAT follow-ups:
  - Enter in a word box runs the search (parity with the Run Search button);
  - the collapsed bar shows the ACTUAL composed responsa query string(s), anchor
    side first then the other side after ' || ', each quoted.
"""

from __future__ import annotations

import pytest

from web.components.joins_builder import create_joins_builder
from web.pages.joins_lab import build_collapsed_query_text


@pytest.fixture(autouse=True)
def _storage_secret():
    from nicegui import app
    try:
        app.storage.secret = 'test-secret'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


class TestBuildCollapsedQueryText:
    def test_anchor_and_other_side(self):
        assert build_collapsed_query_text('אמר [|3] %רבי', '*עקיבא') == '"אמר [|3] %רבי" || "*עקיבא"'

    def test_anchor_only(self):
        assert build_collapsed_query_text('שלום', None) == '"שלום"'

    def test_empty(self):
        assert build_collapsed_query_text(None, None) == ''
        assert build_collapsed_query_text('', '') == ''

    def test_other_only_edge(self):
        assert build_collapsed_query_text(None, 'עקיבא') == '"עקיבא"'


class TestEnterToSearch:
    def _word_inputs(self, on_submit):
        from nicegui import context, ui
        before = set(context.client.elements.keys())
        with ui.column():
            create_joins_builder(allow_page_position=True, on_submit=on_submit)
        return [
            el for eid, el in context.client.elements.items()
            if eid not in before and isinstance(el, ui.input)
            and 'jl-word-rtl' in ' '.join(getattr(el, '_classes', []) or [])
        ]

    def test_keydown_enter_listener_present_when_on_submit(self):
        inputs = self._word_inputs(on_submit=lambda: None)
        assert inputs
        types = [getattr(li, 'type', None) for li in inputs[0]._event_listeners.values()]
        assert any('keydown' in (t or '') for t in types), f'no keydown.enter listener, got {types}'

    def test_enter_invokes_on_submit(self):
        fired = {'n': 0}

        def on_submit(*_a, **_k):
            fired['n'] += 1

        inputs = self._word_inputs(on_submit=on_submit)
        # fire the keydown.enter listener handler directly
        for li in inputs[0]._event_listeners.values():
            if 'keydown' in (getattr(li, 'type', '') or ''):
                li.handler()
                break
        assert fired['n'] == 1

    def test_no_enter_listener_without_on_submit(self):
        inputs = self._word_inputs(on_submit=None)
        assert inputs
        types = [getattr(li, 'type', None) for li in inputs[0]._event_listeners.values()]
        assert not any('keydown' in (t or '') for t in types)
