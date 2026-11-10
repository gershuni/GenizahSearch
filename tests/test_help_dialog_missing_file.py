# -*- coding: utf-8 -*-
"""HelpDialog when Help.html cannot be loaded (owner, 2026-10-18).

The old built-in help texts (SEARCH/COMP/BROWSE/SETTINGS_HELP_HTML and the
English get_*_help_text methods) were months out of date and were removed.
A missing or unreadable Help.html now shows a short translated notice that
points to the website's Help, in both languages.
"""
from __future__ import annotations

import pytest
from PyQt6.QtWidgets import QApplication

import desktop.settings_dialogs as sd
from shared.genizah_translations import TRANSLATIONS

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])

_TITLE = "The Help file could not be loaded."
_LINK = ("Reinstall the application, or read the Help at "
         "<a href='https://genizahsearch.com/help'>genizahsearch.com/help</a>.")


@pytest.mark.parametrize("source", [None, "Z:/no/such/Help.html"])
def test_missing_help_file_shows_the_notice_with_the_website_link(source, monkeypatch):
    monkeypatch.setattr(sd, "tr", lambda s: s)
    dlg = sd.HelpDialog(None, "Help", source_path=source, lang="en")
    try:
        html = dlg.text.toHtml()
        assert _TITLE in dlg.text.toPlainText()
        assert "genizahsearch.com/help" in dlg.text.toPlainText()
        assert "https://genizahsearch.com/help" in html
    finally:
        dlg.deleteLater()


def test_the_notice_is_translated_and_keeps_its_link():
    assert TRANSLATIONS[_TITLE]
    he = TRANSLATIONS[_LINK]
    assert "<a href='https://genizahsearch.com/help'>genizahsearch.com/help</a>" in he


def test_the_old_built_in_help_texts_are_gone():
    import genizah_app
    for key in ("SEARCH_HELP_HTML", "COMP_HELP_HTML", "BROWSE_HELP_HTML", "SETTINGS_HELP_HTML"):
        assert key not in TRANSLATIONS
    for name in ("get_search_help_text", "get_comp_help_text", "get_browse_help_text",
                 "get_settings_help_text", "_build_help_fallback_html"):
        assert not hasattr(genizah_app.GenizahGUI, name)
