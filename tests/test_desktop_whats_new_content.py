# -*- coding: utf-8 -*-
"""The desktop What's New bar follows its CONTENT, not the app version.

9.6.0 has no desktop What's New (owner, 2026-10-07). The bar used to show
whenever the saved ``whats_new_seen`` differed from APP_VERSION, so every
version bump showed the previous release's bar again. It now compares with
``WHATS_NEW_CONTENT_VERSION``: whoever dismissed that content sees nothing
after an update, and whoever never dismissed it still sees it.

Runs the real GenizahGUI methods on a stub window (no QApplication).
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest import mock

import genizah_app
from desktop.update_ui import WHATS_NEW_CONTENT_VERSION


def _window():
    return SimpleNamespace(whats_new_bar=mock.Mock())


def test_content_dismissed_before_an_update_is_not_shown_again():
    window = _window()
    genizah_app.GenizahGUI._maybe_show_whats_new(
        window, {'whats_new_seen': WHATS_NEW_CONTENT_VERSION})
    window.whats_new_bar.show_whats_new.assert_not_called()


def test_content_never_dismissed_is_shown():
    for cfg in ({}, {'whats_new_seen': '9.4.0'}):
        window = _window()
        genizah_app.GenizahGUI._maybe_show_whats_new(window, cfg)
        window.whats_new_bar.show_whats_new.assert_called_once_with(WHATS_NEW_CONTENT_VERSION)


def test_dismissing_saves_the_content_version_not_the_app_version():
    with mock.patch.object(genizah_app, 'save_app_config') as save:
        genizah_app.GenizahGUI.on_whats_new_dismissed(SimpleNamespace())
    save.assert_called_once_with({'whats_new_seen': WHATS_NEW_CONTENT_VERSION})
