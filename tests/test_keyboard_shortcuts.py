# -*- coding: utf-8 -*-
"""The website's keyboard shortcuts and their card (web/keyboard_shortcuts.py, 2026-11-05).

The card lists only shortcuts that exist: for each row, the script that handles its
key is read here. The browse page's keys follow the page's direction and work on a
Hebrew layout; none fires with Ctrl/Alt/Cmd (Ctrl+F used to toggle full screen).
"""
from __future__ import annotations

import re
from pathlib import Path

from shared.genizah_translations import TRANSLATIONS
from web import keyboard_shortcuts as ks

REPO = Path(ks.__file__).resolve().parents[1]
SHORTCUTS_JS = (REPO / 'web' / 'static' / 'js' / 'shortcuts.js').read_text(encoding='utf-8')
BLAU_JS = (REPO / 'web' / 'static' / 'js' / 'blau_lookup.js').read_text(encoding='utf-8')
BROWSE = (REPO / 'web' / 'pages' / 'browse.py').read_text(encoding='utf-8')
SEARCH = (REPO / 'web' / 'pages' / 'search.py').read_text(encoding='utf-8')
HOME = (REPO / 'web' / 'pages' / 'home.py').read_text(encoding='utf-8')
MAIN = (REPO / 'web' / 'main.py').read_text(encoding='utf-8')


def _browse_key_script() -> str:
    start = BROWSE.index('# Keyboard shortcuts (listed on the card')
    return BROWSE[start:BROWSE.index("'''.replace('__BROWSE_RTL__'", start)]


def test_every_string_on_the_card_has_its_hebrew():
    for text in ks.strings(dictionaries=True):
        assert TRANSLATIONS.get(text, '').strip(), text


def test_the_page_arrows_follow_the_page_direction():
    def keys(rtl):
        rows = dict((text, k) for _, section in ks.sections(rtl, False) for k, text in section)
        return rows['Next Page'], rows['Previous Page']

    assert keys(rtl=True) == (('←',), ('→',))
    assert keys(rtl=False) == (('→',), ('←',))
    script = _browse_key_script()
    assert "if (e.key === 'ArrowLeft') action = rtl ? 'next' : 'prev';" in script
    assert "else if (e.key === 'ArrowRight') action = rtl ? 'prev' : 'next';" in script
    assert ".replace('__BROWSE_RTL__', 'true' if is_rtl() else 'false')" in BROWSE


def test_every_browse_row_on_the_card_has_its_key_in_the_page_script():
    script = _browse_key_script()
    for action, key_test in (('zoom-in', "e.key === '+'"), ('zoom-out', "e.key === '-'"),
                             ('zoom-reset', "e.code === 'Digit0'"), ('fullscreen', "e.code === 'KeyF'")):
        assert key_test in script, action
        assert f"action = '{action}'" in script, action
        assert f'data-action="{action}"' in BROWSE, f'no button carries data-action="{action}"'


def test_the_browse_keys_leave_the_browser_s_own_shortcuts_alone():
    script = _browse_key_script()
    for guard in ('if (e.ctrlKey || e.altKey || e.metaKey) return;',
                  # Codex, PR #484: with the shortcuts card (or any dialog or menu) open, F, 0
                  # and the arrows must not reach the manuscript behind it.
                  "if (document.querySelector('.q-dialog, .q-menu')) return;"):
        assert guard in script, guard
        assert script.index(guard) < script.index("action = 'fullscreen'")


def test_the_buttons_tooltips_name_their_keys():
    for snippet in ("tooltip(f'{tr(\"Zoom in\")} (+)')", "tooltip(f'{tr(\"Zoom out\")} (−)')",
                    "tooltip(f'{tr(\"Reset View\")} (0)')", "tooltip(f'{tr(\"Fullscreen Image\")} (F)')",
                    "tooltip(f'{tr(\"Next Page\")} (", "tooltip(f'{tr(\"Previous Page\")} ("):
        assert snippet in BROWSE, snippet


def test_question_mark_and_slash_are_handled_on_every_page():
    assert f"var TRIGGER = '.{ks.TRIGGER_CLASS}';" in SHORTCUTS_JS
    assert f"var EVENT = '{ks.SHORTCUT_EVENT}';" in SHORTCUTS_JS
    assert f"var PRIMARY = '[{ks.PRIMARY_SEARCH_ATTR}]';" in SHORTCUTS_JS
    assert "event.key === '?' || (event.code === 'Slash' && event.shiftKey)" in SHORTCUTS_JS
    assert "event.key === '/' || event.code === 'Slash'" in SHORTCUTS_JS
    # Never while typing in a box, never over a dialog, never with a modifier.
    assert 'if (inABox(event.target) || inABox(document.activeElement)) return;' in SHORTCUTS_JS
    assert "if (document.querySelector('.q-dialog, .q-menu')) return;" in SHORTCUTS_JS
    assert 'event.ctrlKey || event.altKey || event.metaKey' in SHORTCUTS_JS
    # The layout loads the script together with the trigger the card opens from (Codex, PR
    # #484: shared, it ran on the layout-less routes, where "?" found no trigger).
    layout = MAIN[MAIN.index('def create_layout('):]
    assert 'ui.add_head_html(_shortcuts_head_html())' in layout
    assert 'ui.add_head_html(_shortcuts_head_html(), shared=True)' not in MAIN
    assert "shortcuts_trigger = ui.element('div').classes(f'{TRIGGER_CLASS} hidden')" in MAIN
    assert 'shortcuts_trigger.on(SHORTCUT_EVENT, lambda _e: open_shortcuts_card())' in MAIN


def test_slash_finds_the_page_s_own_search_box():
    """Quasar puts the attribute on the <input> itself (2026-11-05: the script looked
    inside it and fell back to the header box on /search)."""
    assert "primary.tagName === 'INPUT'" in SHORTCUTS_JS
    assert re.search(r"'outlined dense clearable data-ux-enter=search_results data-no-loading-bar'\s*"
                     r"' data-gs-primary-search'", SEARCH)
    assert "'outlined rounded dense data-gs-primary-search'" in HOME


def test_the_dictionary_rows_match_the_key_script():
    """D or M tapped then a click, Alt + a click, selected words + D, Esc (owner, 2026-11-05)."""
    assert "event.code === 'KeyD' || event.code === 'KeyM'" in BLAU_JS
    assert "var viaAlt = event.altKey" in BLAU_JS and "if (event.key === 'Escape') {" in BLAU_JS
    with_dict = [k for _, rows in ks.sections(False, True) for k, _ in rows]
    without = [k for _, rows in ks.sections(False, False) for k, _ in rows]
    for keys in (('D', 'or', 'M', 'then', 'click'), ('Alt', 'click'), ('Select words', 'D'), ('Esc',)):
        assert keys in with_dict and keys not in without, keys


def test_no_plus_beside_or_and_then():
    """"D or M then click" is pressed one after the other, not together."""
    assert set(ks.CONNECTORS) <= set(ks.PLAIN_WORDS)
    src = (REPO / 'web' / 'keyboard_shortcuts.py').read_text(encoding='utf-8')
    assert 'if i and key not in CONNECTORS and keys[i - 1] not in CONNECTORS:' in src


def test_the_space_rows_match_the_search_page_script():
    assert "if (e.key !== ' ') return;" in SEARCH
    assert 'var delta = e.shiftKey ? -inner.clientHeight : inner.clientHeight;' in SEARCH


def test_head_html_loads_the_script_versioned_by_its_content(monkeypatch, tmp_path):
    line = ks.head_html()
    assert line.startswith('<script src="/static/js/shortcuts.js?v=') and ' defer>' in line
    monkeypatch.setattr(ks, '_JS_PATH', tmp_path / 'missing.js')
    assert ks.head_html() == ''
