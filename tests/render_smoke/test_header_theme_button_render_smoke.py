# -*- coding: utf-8 -*-
"""Render-smoke: the header's theme button switches light <-> dark only (2026-11-22).

It used to cycle light -> parchment -> dark, three clicks around. Now a click goes
light -> dark -> light, and parchment (a light theme) goes to dark. The icon and
tooltip say what the next click does. Parchment stays in Settings and the sidebar.
``web/header_theme.py``; harness of ``test_home_teaser_render_smoke.py``.
"""
from __future__ import annotations

import asyncio
import re

from nicegui import ui
from nicegui.testing.user_interaction import UserInteraction

from tests.render_smoke.test_home_teaser_render_smoke import _marked_element, _run_home_smoke
from web.header_theme import (
    HEADER_THEME_MARKER, header_next_theme, header_theme_icon,
)
from web.safe_storage import safe_user_get

_DATA_THEME = re.compile(r'.*getAttribute\("data-theme"\).*', re.S)


def test_next_theme_is_light_or_dark_only():
    assert header_next_theme('light') == 'dark'
    assert header_next_theme('parchment') == 'dark'
    assert header_next_theme('dark') == 'light'
    assert header_next_theme(None) == 'dark'
    assert header_theme_icon('light') == 'dark_mode'
    assert header_theme_icon('parchment') == 'dark_mode'
    assert header_theme_icon('dark') == 'light_mode'


def _tooltip_text(btn) -> str:
    tips = [c for c in btn.default_slot.children if isinstance(c, ui.tooltip)]
    assert len(tips) == 1
    return tips[0].text


def _drive(lang: str, dark_label: str, light_label: str):
    shown = {'theme': 'light'}  # what this tab's <body data-theme> says

    async def driver(user):
        user.javascript_rules[_DATA_THEME] = lambda _m: shown['theme']
        await user.open('/about')
        btn = _marked_element(user, HEADER_THEME_MARKER)
        assert btn is not None, 'no header theme button'

        async def click():
            with user._client:
                UserInteraction(user, {btn}, None).click()
            await asyncio.sleep(0.3)
            with user._client:
                shown['theme'] = safe_user_get('theme', 'light')
            return shown['theme']

        # light: the button offers dark
        assert btn.props['icon'] == 'dark_mode'
        assert _tooltip_text(btn) == dark_label
        assert btn.props['aria-label'] == dark_label

        assert await click() == 'dark'
        assert btn.props['icon'] == 'light_mode'
        assert _tooltip_text(btn) == light_label
        assert await click() == 'light'
        assert btn.props['icon'] == 'dark_mode'
        assert _tooltip_text(btn) == dark_label

        # a reader in parchment (chosen in Settings) goes to dark
        shown['theme'] = 'parchment'
        assert await click() == 'dark'
        assert btn.props['icon'] == 'light_mode'

    _run_home_smoke(driver, lang=lang)


def test_header_button_alternates_light_and_dark_english():
    _drive('en', 'Switch to the dark theme', 'Switch to the light theme')


def test_header_button_alternates_light_and_dark_hebrew():
    _drive('he', 'מעבר לערכת הנושא הכהה', 'מעבר לערכת הנושא הבהירה')

