"""Phase 118 UAT follow-ups: inline Advanced options (#1) + other-side in the
collapsed summary bar (#2), plus the Combine-mode dict-payload guard.

Covers:
  - _coerce_combine_mode: Quasar dict-options payload -> 'AND'/'OR' key.
  - build_collapsed_summary: collapsed bar text includes the other-side segment
    only when the other side is enabled AND has content.
  - Advanced options render INLINE (no ui.expansion) so they are never hidden.
"""

from __future__ import annotations

import pytest

from web.pages.joins_lab import (
    _coerce_combine_mode,
    build_collapsed_summary,
    create_joins_lab_page,
)


@pytest.fixture(autouse=True)
def _storage_secret():
    from nicegui import app
    try:
        app.storage.secret = 'test-secret'  # noqa: S105 (test-only)
    except Exception:
        pass
    yield


class TestCoerceCombineMode:
    def test_quasar_object_or(self):
        assert _coerce_combine_mode({'label': 'Widen (OR)', 'value': 'OR'}) == 'OR'

    def test_quasar_object_and(self):
        assert _coerce_combine_mode({'label': 'Narrow (AND)', 'value': 'AND'}) == 'AND'

    def test_plain_keys_pass_through(self):
        assert _coerce_combine_mode('AND') == 'AND'
        assert _coerce_combine_mode('OR') == 'OR'

    def test_unknown_and_dict_fallback_to_and(self):
        assert _coerce_combine_mode('garbage') == 'AND'
        assert _coerce_combine_mode({'no': 'value'}) == 'AND'

    def test_result_always_hashable_str(self):
        for v in ['AND', 'OR', {'value': 'OR'}, {'x': 1}, 'nope']:
            out = _coerce_combine_mode(v)
            assert isinstance(out, str)
            assert out in ('AND', 'OR')


class TestBuildCollapsedSummary:
    ANCHOR = 'Exact · 2 lines · Text Position: Anywhere'

    def test_other_side_disabled_returns_anchor_only(self):
        assert build_collapsed_summary(self.ANCHOR, False, 'Exact · 1 line', 'AND') == self.ANCHOR

    def test_other_side_enabled_but_no_summary_returns_anchor_only(self):
        assert build_collapsed_summary(self.ANCHOR, True, None, 'AND') == self.ANCHOR
        assert build_collapsed_summary(self.ANCHOR, True, '', 'OR') == self.ANCHOR

    def test_other_side_enabled_appends_segment(self):
        out = build_collapsed_summary(self.ANCHOR, True, 'Exact · 1 line', 'OR')
        assert self.ANCHOR in out
        assert 'Exact · 1 line' in out
        assert '⇄' in out  # anchor/other-side separator

    def test_combine_dict_payload_does_not_crash_summary(self):
        # Even if a raw Quasar dict leaks in as combine_mode, the summary must build.
        out = build_collapsed_summary(self.ANCHOR, True, 'Exact · 1 line', {'value': 'OR'})
        assert isinstance(out, str) and self.ANCHOR in out


class TestAdvancedOptionsInline:
    def test_no_expansion_advanced_options_are_inline(self):
        """#1: Advanced options must not be hidden inside a ui.expansion."""
        from nicegui import context, ui

        before = set(context.client.elements.keys())
        with ui.column():
            create_joins_lab_page()
        new_els = [
            el for eid, el in context.client.elements.items() if eid not in before
        ]
        expansions = [el for el in new_els if isinstance(el, ui.expansion)]
        assert not expansions, (
            f'Advanced options must render inline (#1) — found {len(expansions)} '
            f'ui.expansion element(s) on the page'
        )

    def test_other_side_checkbox_present(self):
        """The other-side toggle is built at page render (inline, not deferred)."""
        from nicegui import context, ui

        before = set(context.client.elements.keys())
        with ui.column():
            create_joins_lab_page()
        new_checkboxes = [
            el for eid, el in context.client.elements.items()
            if eid not in before and isinstance(el, ui.checkbox)
        ]
        assert new_checkboxes, 'expected the inline option checkboxes to be rendered'
