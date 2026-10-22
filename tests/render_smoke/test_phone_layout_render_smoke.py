# -*- coding: utf-8 -*-
"""Render-smoke: /browse carries the phone layout's controls (owner, 2026-10-22).

The long header-card and control-bar buttons fold to their icons on a phone
through ``browse-icon-collapse`` (text kept in the tree, named by aria-label,
the rule the icon smoke test states), and the image panel's slider row sits
behind a ``tune`` button that toggles ``is-open`` on it; the page CSS shows
that button under 640px only. Harness: ``test_browse_button_icons_render_smoke``.
"""
from __future__ import annotations

from nicegui import ui

from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE,
    _all_elements,
    _button,
    _button_with_tooltip,
    _click,
    _open_browse,
    _run_browse,
)

FOLDING = ('Search for Parallels', 'Show Metadata', 'Show Full Manuscript', 'Edit', 'Send Comment')


def test_the_long_buttons_fold_to_named_icons_on_a_phone():
    async def driver(user):
        await _open_browse(user, lambda texts: {'Edit', 'Send Comment'} <= set(texts))
        for text in FOLDING:
            [button] = _button(user, text)
            assert button.props.get('icon'), text
            assert 'browse-icon-collapse' in button.classes, text
            assert button.props.get('aria-label') == text, (text, button.props)

    _run_browse(OPEN_PAGE, driver)


def test_the_image_adjustments_sit_behind_a_toggle():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        toggle = _button_with_tooltip(user, 'Image adjustments')
        assert 'browse-adjust-toggle' in toggle.classes
        assert toggle.props.get('aria-label') == 'Image adjustments'
        [row] = [e for e in _all_elements(user) if isinstance(e, ui.row) and 'browse-image-adjust' in e.classes]
        assert 'is-open' not in row.classes
        _click(user, toggle)
        assert 'is-open' in row.classes
        _click(user, toggle)
        assert 'is-open' not in row.classes

    _run_browse(OPEN_PAGE, driver)
