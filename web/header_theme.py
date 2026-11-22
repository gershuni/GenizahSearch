# -*- coding: utf-8 -*-
"""The header's theme button: light and dark only.

The button in the site header switches between the light and the dark theme.
Parchment counts as a light theme there, so a click from parchment goes to dark.
Parchment is still chosen in Settings and in the sidebar's theme switcher.

The button's icon and tooltip say what the next click does. The other theme
switchers call ``sync_header_theme_button`` so the button stays right after
they change the theme.
"""
from __future__ import annotations

from nicegui import ElementFilter, ui

from web.translations import tr

HEADER_THEME_MARKER = 'theme-btn-header'


def header_next_theme(theme: str | None) -> str:
    """The theme a header click goes to: dark from light or parchment, light from dark."""
    return 'light' if theme == 'dark' else 'dark'


def header_theme_icon(theme: str | None) -> str:
    """The icon of the theme the next click goes to."""
    return 'light_mode' if header_next_theme(theme) == 'light' else 'dark_mode'


def header_theme_label(theme: str | None) -> str:
    """The tooltip and aria-label: what the next click does."""
    if header_next_theme(theme) == 'light':
        return tr('Switch to the light theme')
    return tr('Switch to the dark theme')


def sync_header_theme_button(theme: str) -> None:
    """Point this page's header button at the theme after ``theme``."""
    icon = header_theme_icon(theme)
    label = header_theme_label(theme)
    for btn in ElementFilter(kind=ui.button, marker=HEADER_THEME_MARKER, local_scope=False):
        btn.props['icon'] = icon
        btn.props['aria-label'] = label
        btn.update()
        for child in btn.default_slot.children:
            if isinstance(child, ui.tooltip):
                child.text = label
