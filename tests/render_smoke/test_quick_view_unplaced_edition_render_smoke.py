# -*- coding: utf-8 -*-
"""Render-smoke: Quick View on a scholarly-transcription hit whose page could not be identified.

Such a hit claims the page Quick View opens on (web/pages/search_results.py::_hit_scope_version): its edition
is shown there, chosen in the version chooser, though the page filter leaves it out, and the actions read the
text on screen. Find Parallels searched the page's automatic text instead (Codex review of #33).

Drives the real dialog over the stubs of test_quick_view_without_automatic_text_render_smoke.py: T-S NS 243.146
page 2 has an automatic text, and its one FGP edition is on folio 1r, so the page filter leaves it out on page 2.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

from nicegui import ui

from tests.render_smoke.test_quick_view_without_automatic_text_render_smoke import (
    FGP_TEXT, HTR_TEXT, SID, _button, _click, _edition_shown, _elements, _labels, _result, _run, _version_label, _wait_until,
)
from web.pages import search_results
from web.translations import using_language

PAGE = '/_render_smoke/quick-view-unplaced-edition'

# An FGP hit on T-S NS 243.146 whose page could not be identified, opened on page 2.
UNPLACED = dict(_result(SID, '2', snippet='*אֵל* מְשוך בְטלה', full_text=HTR_TEXT),
                edition={'kind': 'fgp', 'version': 'fgp:22952', 'sys_id': SID, 'located': False},
                page_identified=False)


@ui.page(PAGE)
def _quick_view_page():
    search_state = SimpleNamespace(displayed_results=[UNPLACED], title_translations={}, printed_ids=set(),
                                   translation_data={})
    with using_language('en'):
        search_results.open_advanced_dialog(search_state, SimpleNamespace(), None, UNPLACED)


def _text_button(user, text):
    return [b for b in _elements(user) if isinstance(b, ui.button) and b.text == text]


def test_quick_view_shows_the_edition_and_find_parallels_searches_it():
    async def driver(user):
        await user.open(PAGE)
        await _wait_until(lambda: _edition_shown(user) and _text_button(user, 'Find Parallels'))
        assert _edition_shown(user), (_version_label(user), sorted(_labels(user)))
        assert any('Page not identified' in label for label in _labels(user)), sorted(_labels(user))

        went = []
        [find] = _text_button(user, 'Find Parallels')
        with patch.object(ui.navigate, 'to', side_effect=went.append):
            _click(user, find)
        [url] = went
        text = parse_qs(urlparse(url).query)['text'][0]
        assert 'מְשוך' in text, text[:120]
        assert 'שנייה' not in text, "searched the page's automatic text, not the edition on screen"
        # The 2,000-character cut is on the text, not its encoding (six characters a letter):
        # the whole edition (under 2,000 characters) goes, not its first ~330.
        assert text.strip() == FGP_TEXT.strip() and len(text) > 400, len(text)

    _run(driver)


def test_find_parallels_while_editing_sends_the_editors_text():
    """Edit mode: the textarea's text as typed, not the page Edit started from (Codex review of
    #33)."""
    async def driver(user):
        await user.open(PAGE)
        await _wait_until(lambda: _edition_shown(user) and _button(user, 'Edit'))
        with patch('web.auth_state.GlobalAuthState.is_logged_in', return_value=True):
            _click(user, _button(user, 'Edit')[0])

            def editor():   # the text's editor, which starts from the page's text (not the notes)
                return [e for e in _elements(user) if isinstance(e, ui.textarea) and e.value == HTR_TEXT]
            await _wait_until(editor)
            [area] = editor()
            with user._client:
                area.set_value('my edit of the page')
            await _wait_until(lambda: _text_button(user, 'Find Parallels'))
            went = []
            [find] = _text_button(user, 'Find Parallels')
            with patch.object(ui.navigate, 'to', side_effect=went.append):
                _click(user, find)
        [url] = went
        assert parse_qs(urlparse(url).query)['text'] == ['my edit of the page']

    _run(driver)
