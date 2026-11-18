# -*- coding: utf-8 -*-
"""The results header's Filter Results and Export buttons, driven through the real /search page
(NiceGUI User simulation; no index, no network). Owner, 2026-11-18: inside one "More actions"
menu the filter panel and the exports were too hidden, so each is a button of its own; Filter
Results is on (primary) while its panel is open and off (the theme's secondary text) otherwise."""
from __future__ import annotations

import os

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'results-header-test-secret-0123456789abcdef')

from tests.test_web_variant_settings_per_visitor import (  # noqa: E402,F401  (server is a fixture)
    _element, _fire, run, server,
)


def _filters_panel(user):
    """The Filter Results panel: the column holding its shelfmark field."""
    from nicegui import ui

    def holds_shelfmark_field(e):
        return 'py-3' in e.classes and any(
            isinstance(d, ui.input) and d.props.get('placeholder') == 'Filter by shelfmark'
            for d in e.descendants())
    return _element(user, ui.column, holds_shelfmark_field)


@pytest.mark.usefixtures('server')
def test_filter_results_opens_its_panel_and_the_exports_are_one_menu():
    from nicegui import ui
    seen = {}

    def state(user, btn):
        return (_filters_panel(user).visible, 'btn-off' in btn.classes, btn.props.get('color'))

    async def driver(a):
        await a.open('/search')
        btn = _element(a, ui.button, lambda e: 'results-filter-btn' in e.classes)
        seen['tooltip_label'] = btn.props.get('aria-label')
        seen['closed'] = state(a, btn)
        _fire(a, btn, 'click')
        seen['open'] = state(a, btn)
        _fire(a, btn, 'click')
        seen['closed_again'] = state(a, btn)
        export = _element(a, ui.button, lambda e: 'results-export-btn' in e.classes)
        with a._client:
            seen['exports'] = [e.default_slot.children[0].text if e.default_slot.children else None
                               for e in export.descendants() if isinstance(e, ui.menu_item)]
            seen['export_label'] = export.props.get('aria-label')
            seen['more_menu'] = any(isinstance(e, ui.button) and e.props.get('icon') == 'more_vert'
                                    for e in a._client.elements.values())

    run(driver, count=1)
    assert seen['tooltip_label'] == 'Filter Results'
    assert seen['closed'] == (False, True, None), seen
    assert seen['open'] == (True, False, 'primary'), seen
    assert seen['closed_again'] == (False, True, None), seen
    assert seen['export_label'] == 'Export'
    assert seen['more_menu'] is False
    assert len(seen['exports']) == 3, seen['exports']
