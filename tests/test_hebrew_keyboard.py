# -*- coding: utf-8 -*-
"""The on-screen Hebrew keyboard (web/hebrew_keyboard.py, web/static/js/hebrew_keyboard.js).

Part 1 pins the panel: every letter and final form once, finals beside their
letter, no ``?`` key (it switches the mode at the start of the box), labels
translated. Part 2 runs the script in Chromium on a plain page: a key types at
the cursor and replaces a selection, backspace takes a letter with its marks, the
box hears an ``input`` event per key, and the use is reported once per attempt.
"""
from __future__ import annotations

import html as html_lib
import re
from pathlib import Path

import pytest

from shared.genizah_translations import TRANSLATIONS
from web import hebrew_keyboard as hk

ROOT = Path(__file__).resolve().parents[1]
JS = ROOT / 'web' / 'static' / 'js' / 'hebrew_keyboard.js'
SEARCH_PAGE = ROOT / 'web' / 'pages' / 'search.py'


def _ident(text):
    return text


# --- Part 1: the panel -------------------------------------------------------

def test_every_letter_and_final_once():
    letters = [c for row in hk.LETTER_ROWS for c in row]
    assert sorted(letters) == sorted('אבגדהוזחטיכךלמםנןסעפףצץקרשת')
    assert len(letters) == len(set(letters)) == 27


def test_each_final_follows_its_letter():
    letters = [c for row in hk.LETTER_ROWS for c in row]
    for final, base in hk.FINALS.items():
        assert letters.index(final) == letters.index(base) + 1, final


def test_rows_fit_a_phone():
    assert all(len(row) <= 10 for row in hk.LETTER_ROWS)


def test_no_mode_prefix_keys():
    # '?' and a space switch the mode at the start of the box (search.py
    # _shortcut_prefixes); a newcomer would take '?' for a wildcard.
    for prefix in ('?', '=', '~', '/', '$', '#', 'R'):
        assert prefix not in hk.all_characters()


def test_panel_markup_has_every_key_as_a_button():
    markup = hk.panel_html(_ident)
    chars = [html_lib.unescape(c) for c in re.findall(r'data-hk-char="([^"]*)"', markup)]
    assert chars == [c for row in hk.LETTER_ROWS for c in row] + [' '] + [c for c, _ in hk.SYMBOL_KEYS]
    assert 'data-hk-action="backspace"' in markup
    assert markup.count('<button type="button"') == len(chars) + 1
    assert '<script' not in markup  # the page loads the script (BUG-B in search.py)
    assert 'Final form of כ' in markup


def test_labels_are_translated():
    labels = ['Hebrew keyboard', 'Final form of {}', 'Space', 'Delete'] + [t for _, t in hk.SYMBOL_KEYS]
    missing = [label for label in labels if label not in TRANSLATIONS]
    assert not missing


def test_head_html_versions_the_script():
    head = hk.head_html()
    assert re.search(r'/static/js/hebrew_keyboard\.js\?v=[0-9a-f]{10}', head)
    assert '.gs-hk-key' in head


def test_used_payload_never_raises():
    assert hk.used_payload({'detail': '{"used": true}'}) == {'used': True}
    assert hk.used_payload({'detail': 'not json'}) == {}
    assert hk.used_payload(None) == {}
    assert hk.used_payload('[1]') == {}


def test_search_page_wires_it():
    src = SEARCH_PAGE.read_text(encoding='utf-8')
    assert 'hebrew_keyboard.TARGET_ATTR' in src
    assert 'hebrew_keyboard.head_html()' in src
    assert "'hebrew_keyboard': _hebrew_keyboard_used" in src


# --- Part 2: the script in a browser ----------------------------------------

_PAGE = '''<!doctype html><html><head><meta charset="utf-8"></head><body>
<input id="box" {target} dir="rtl" value="">
<div id="panel" {panel} data-hk-field="main_search" data-hk-layout="alphabetical">{keys}</div>
<script>
window.inputs = 0; window.used = 0; window.captured = [];
window.posthog = {{capture: function (n, p) {{ window.captured.push([n, p]); }}}};
document.getElementById('box').addEventListener('input', function () {{ window.inputs++; }});
document.getElementById('panel').addEventListener('{event}', function () {{ window.used++; }});
</script>
<script>{js}</script>
</body></html>'''


@pytest.fixture(scope='module')
def page():
    playwright = pytest.importorskip('playwright.sync_api')
    with playwright.sync_playwright() as p:
        browser = p.chromium.launch()
        pg = browser.new_page()
        pg.set_content(_PAGE.format(
            target=hk.TARGET_ATTR, panel=hk.PANEL_ATTR, event=hk.USED_EVENT,
            keys=hk.panel_html(_ident), js=JS.read_text(encoding='utf-8')))
        yield pg
        browser.close()


def _reset(page, value='', caret=None):
    page.evaluate('''([v, c]) => {
        const b = document.getElementById('box'); b.value = v; b.focus();
        const at = c === null ? v.length : c; b.setSelectionRange(at, at);
        window.inputs = 0; window.used = 0; window.captured = [];
        window.gsHebrewKeyboard.reset();
    }''', [value, caret])


def _key(page, char):
    page.click(f'[data-hk-char="{char}"]')


def _box(page):
    return page.evaluate("document.getElementById('box').value")


def test_keys_type_at_the_cursor_and_keep_focus(page):
    _reset(page, 'שם', caret=1)
    _key(page, 'ל')
    _key(page, 'ו')
    assert _box(page) == 'שלום'
    assert page.evaluate('document.activeElement.id') == 'box'
    assert page.evaluate('window.inputs') == 2


def test_a_key_replaces_the_selection(page):
    _reset(page, 'אבגד')
    page.evaluate("document.getElementById('box').setSelectionRange(1, 3)")
    _key(page, 'ת')
    assert _box(page) == 'אתד'


def test_space_and_symbols(page):
    _reset(page, 'ר')
    _key(page, "'")
    page.click('[data-hk-char=" "]')
    _key(page, '*')
    assert _box(page) == "ר' *"


def test_backspace_takes_a_letter_with_its_marks(page):
    _reset(page, 'אד̇')  # ד with a dot above
    page.click('[data-hk-action="backspace"]')
    assert _box(page) == 'א'
    page.click('[data-hk-action="backspace"]')
    page.click('[data-hk-action="backspace"]')  # empty: nothing happens
    assert _box(page) == ''


def test_backspace_deletes_a_selection(page):
    _reset(page, 'אבגד')
    page.evaluate("document.getElementById('box').setSelectionRange(0, 2)")
    page.click('[data-hk-action="backspace"]')
    assert _box(page) == 'גד'


def test_use_reported_once_per_attempt_without_characters(page):
    _reset(page)
    for c in 'שבת':
        _key(page, c)
    assert page.evaluate('window.used') == 1
    captured = page.evaluate('window.captured')
    assert captured == [['hebrew_keyboard_used', {'field': 'main_search', 'layout': 'alphabetical'}]]
    page.evaluate('window.gsHebrewKeyboard.reset()')
    _key(page, 'א')
    assert page.evaluate('window.used') == 2


def test_an_untouched_box_types_at_the_end(page):
    # A restored query with the cursor at 0 and the box never clicked: the end.
    page.goto('about:blank')
    page.set_content(_PAGE.format(
        target=hk.TARGET_ATTR, panel=hk.PANEL_ATTR, event=hk.USED_EVENT,
        keys=hk.panel_html(_ident), js=JS.read_text(encoding='utf-8')))
    page.evaluate("const b = document.getElementById('box'); b.value = 'שב'; b.setSelectionRange(0, 0);")
    page.dispatch_event('[data-hk-char="ת"]', 'click')
    assert _box(page) == 'שבת'


def test_keyboard_activation_types(page):
    _reset(page, 'א')
    page.focus('[data-hk-char="ב"]')
    page.keyboard.press('Enter')
    page.keyboard.press('Space')
    assert _box(page) == 'אבב'
