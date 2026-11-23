# -*- coding: utf-8 -*-
"""Render-smoke: the head of Browse's text, and the reading text's size (owner, 2026-11-23).

* The version chooser's row (line numbers, A- A+, the dictionary switch, then the chooser) heads the
  text pane. It sat under the text: below the fold at 1280x800, after the whole text on a phone.
* The "V0.8" badge in the page's toolbar is gone: it named the automatic transcription's model even
  while an edition was on screen.
* A- / A+ (web/components/text_size.py): one size per visitor, a CSS variable the transcriptions'
  font sizes are multiplied by (Browse's page and Full Manuscript View, the Reading Desk), steps
  of the desktop app's (9pt to 40pt by 2pt of its 16: 9/16 to 2.5 by 0.125). The Reading Desk's texts
  have the buttons too.

Harnesses: ``test_browse_button_icons_render_smoke`` (Browse) and
``test_reading_desk_document_render_smoke`` (the desk).
"""
from __future__ import annotations

from nicegui import ui

from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE,
    _all_elements,
    _click,
    _open_browse,
    _run_browse,
    _wait_until,
)
from web.components import text_size
from web.pages import browse
from web.pages import browse_desk_document


def test_the_sizes_are_the_desktop_apps_steps():
    assert text_size.clamp(1) == 1.0 and text_size.clamp('1.125') == 1.125
    assert text_size.clamp(9) == 2.5 and text_size.clamp(0.1) == 0.5625     # 40pt and 9pt of 16 (Codex #108)
    assert text_size.clamp(1.03) == 1.0 and text_size.clamp(1.05) == 1.0625  # to the nearest whole point
    # down from the default as the desktop goes: 14, 12, 10, then its floor, 9
    sizes, size = [], 1.0
    for _ in range(5):
        size = text_size.clamp(size - text_size.STEP)
        sizes.append(size)
    assert sizes == [0.875, 0.75, 0.625, 0.5625, 0.5625]
    assert text_size.clamp(0.5625 + text_size.STEP) == 0.6875                # 11pt, as the desktop from 9
    # anything else is the default: a step below the page's sizes (owner, 2026-11-23: a bit smaller)
    assert text_size.DEFAULT == 0.875
    assert text_size.clamp(None) == 0.875 and text_size.clamp('x') == 0.875 and text_size.clamp(float('nan')) == 0.875
    assert text_size.scaled('1.2rem') == 'calc(1.2rem * var(--gs-text-scale, 1))'
    assert text_size.head_style(1.25) == '<style>:root { --gs-text-scale: 1.25; }</style>'


def test_the_texts_are_sized_by_the_readers_scale():
    styles = browse.VIEWER_STYLES
    for px in (17, 18, 19, 20):
        assert f'font-size: calc({px}px * var(--gs-text-scale, 1)) !important' in styles, px
    assert 'calc(1.2rem * var(--gs-text-scale, 1))' in browse_desk_document._TEXT_STYLE


def _row(user):
    rows = [e for e in _all_elements(user) if 'browse-text-head' in e.classes]
    return rows[0] if rows else None


def test_the_version_row_heads_the_text_and_the_badge_is_gone():
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        await _wait_until(lambda: _row(user) is not None)
        row = _row(user)
        siblings = list(row.parent_slot.children)
        texts = [i for i, el in enumerate(siblings)
                 if any(isinstance(d, ui.html) and 'line-numbered-text' in (d.content or '') for d in el.descendants())]
        seen['order'] = (siblings.index(row), texts)
        seen['in_row'] = sorted(b.props.get('aria-label') or b.props.get('icon') for b in row.descendants()
                                if isinstance(b, ui.button) and ('gs-text-size-btn' in b.classes
                                                                 or b.props.get('icon') == 'format_list_numbered'))
        seen['badge'] = [e for e in _all_elements(user) if 'source-badge' in e.classes]

    _run_browse(OPEN_PAGE, driver)
    row_at, text_at = seen['order']
    assert text_at and row_at < text_at[0], seen['order']             # the row first, then the text
    assert seen['in_row'] == ['Larger text', 'Smaller text', 'format_list_numbered']
    assert seen['badge'] == []


def test_the_line_numbers_change_the_text_in_place():
    """Owner, 2026-11-23: the line numbers' button made the window jump up. The text was drawn again from
    nothing: the pane was empty for a moment, the page lost its height and the browser pulled a short
    page's scroll up. Now the text's element stays and its content changes."""
    seen = {}

    def _texts(user):
        return [d for d in _all_elements(user) if isinstance(d, ui.html)
                and any(cls in (d.content or '') for cls in ('transcription-text', 'line-numbered-text'))]

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        await _wait_until(lambda: _row(user) is not None and _texts(user))
        [before] = _texts(user)
        seen['numbered'] = 'line-numbered-text' in before.content
        [button] = [b for b in _row(user).descendants() if isinstance(b, ui.button)
                    and b.props.get('icon') == 'format_list_numbered']
        _click(user, button)
        [after] = _texts(user)
        seen['same'] = after is before
        seen['numbered_after'] = 'line-numbered-text' in after.content
        _click(user, button)                                           # back as it was, for the visitor
        seen['numbered_again'] = 'line-numbered-text' in _texts(user)[0].content

    _run_browse(OPEN_PAGE, driver)
    assert seen['same']
    assert seen['numbered_after'] != seen['numbered']
    assert seen['numbered_again'] == seen['numbered']


def test_a_plus_and_a_minus_keep_the_visitors_size():
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        await _wait_until(lambda: _row(user) is not None)
        buttons = {b.props.get('aria-label'): b for b in _row(user).descendants() if 'gs-text-size-btn' in b.classes}
        with user._client:
            seen['start'] = text_size.current()
        _click(user, buttons['Larger text'])
        with user._client:
            seen['one_up'] = text_size.current()
        for _ in range(20):
            _click(user, buttons['Larger text'])
        with user._client:
            seen['top'] = text_size.current()
        _click(user, buttons['Smaller text'])
        with user._client:
            seen['one_down'] = text_size.current()

    _run_browse(OPEN_PAGE, driver)
    assert seen['start'] == 0.875                                     # a step below the page's sizes
    assert seen['one_up'] == 1.0
    assert seen['top'] == 2.5                                          # no further than the desktop's 40pt
    assert seen['one_down'] == 2.375


def test_the_reading_desk_texts_have_a_minus_and_a_plus(tmp_path):
    from tests.render_smoke.test_reading_desk_document_render_smoke import FRAGMENTS, _run as run_desk
    from tests.test_reading_desk_document import DOC
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)                                         # a hand-built desk: texts by fragment
        await _wait_until(lambda: [e for e in _all_elements(user) if 'rd-text-head' in e.classes])
        [head] = [e for e in _all_elements(user) if 'rd-text-head' in e.classes]
        seen['plain'] = sorted(b.props.get('aria-label') for b in head.descendants() if 'gs-text-size-btn' in b.classes)
        enter(FRAGMENTS, DOC)                                          # a document's desk: its own text panel
        await _wait_until(lambda: any('rd-document-mode' in e.classes for e in _all_elements(user)))
        heads = [e for e in _all_elements(user) if 'rd-text-head' in e.classes]
        seen['document'] = [sorted(b.props.get('aria-label') for b in h.descendants() if 'gs-text-size-btn' in b.classes)
                            for h in heads]

    run_desk(tmp_path, driver)
    assert seen['plain'] == ['Larger text', 'Smaller text']
    assert seen['document'] == [['Larger text', 'Smaller text']]


def test_a_search_deep_link_rewrites_the_choosers_one_tooltip():
    """The chooser is a button with its tooltip, "Version". A search's deep link that showed the version
    holding the phrase says so in that tooltip: one tooltip, its text changed, never a second (Codex #108)."""
    import asyncio

    from nicegui import core
    from nicegui.client import Client
    from nicegui.testing.general import prepare_simulation

    from tests.test_version_selector_marks_legend import GENUZOT, TEXT
    from web.components.version_selector import create_version_selector
    from web.translations import tr

    prepare_simulation()
    tips = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        for name, phrase in (('browsing', None), ('search', 'משפט ייחודי בלבד')):
            with Client(ui.page(f'/_chooser_tooltip_probe_{name}')) as client:
                create_version_selector('990000000000017', 1, TEXT + '\nמשפט ייחודי בלבד',
                                        all_sources=[GENUZOT], must_contain=phrase)
            await asyncio.sleep(0.4)
            [chooser] = [e for e in client.elements.values() if e.props.get('data-testid') == 'version-chooser']
            # its own, and any aimed at it from outside (NiceGUI's ``.tooltip()`` adds one by the element's id)
            mine = set(map(id, chooser.descendants()))
            tips[name] = [t.text for t in client.elements.values() if isinstance(t, ui.tooltip)
                          and (id(t) in mine or t.props.get('target') == f'#{chooser.html_id}')]
            with client:
                words[name] = tr('Version') if phrase is None else tr('showing the version containing your search')

    words = {}
    asyncio.run(_run())
    assert tips['browsing'] == [words['browsing']]
    assert tips['search'] == [words['search']]


def test_the_texts_tools_are_by_the_text():
    """Owner, 2026-11-23: by the text, a copy of it with the citation "How to cite" gives the page; the print
    menu, its "this page" now three (image and text, image, text) beside every page's text; Edit; Send
    Comment. The green card: the shelfmark copied in one click, before Share."""
    from types import SimpleNamespace as _NS
    from unittest.mock import patch as _patch

    from web import clipboard
    seen, copied, said = {}, [], []

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        await _wait_until(lambda: _row(user) is not None)
        row = _row(user)
        seen['tools'] = [b.props.get('data-testid') for b in row.descendants() if isinstance(b, ui.button)
                         and b.props.get('data-testid') in ('browse-copy-text', 'browse-print', 'browse-edit')]
        [prt] = [b for b in row.descendants() if b.props.get('data-testid') == 'browse-print']
        seen['print'] = [(' '.join(d.text for d in m.descendants() if isinstance(d, ui.item_section) and d.text),
                          [ls.js_handler for ls in m._event_listeners.values() if ls.type == 'click' and ls.js_handler])
                         for m in prt.descendants() if isinstance(m, ui.menu_item)]
        [copy] = [b for b in row.descendants() if b.props.get('data-testid') == 'browse-copy-text']

        async def record(text, **kwargs):
            copied.append((text, kwargs.get('success_message')))
            return True

        # through the site's copy (web/clipboard.py), which says "copied" only when the browser did
        with _patch.object(browse, 'copy_text_to_clipboard', side_effect=record):
            _click(user, copy)
            await _wait_until(lambda: len(copied) == 1)
        # the shelfmark is written in the click itself, in the browser (a phone's browser refuses a write
        # after a round trip: Codex on #115); only the browser's answer comes back, for the toast (the card as
        # it is now: the page is drawn again once its enrichment comes)
        [card_copy] = [e for e in _all_elements(user) if e.props.get('data-testid') == 'browse-copy-shelfmark']
        [listener] = [ls for ls in card_copy._event_listeners.values() if ls.type == 'click']
        seen['card_copy'] = (card_copy.props.get('aria-label'), listener.js_handler)
        with user._client, _patch.object(clipboard.ui, 'notify', side_effect=lambda m, **k: said.append((m, k))):
            listener.handler(_NS(args=True))
            listener.handler(_NS(args=False))

    _run_browse(OPEN_PAGE, driver)
    assert seen['tools'] == ['browse-copy-text', 'browse-print', 'browse-edit']
    labels = [label for label, _js in seen['print']]
    assert labels == ['Print this page image and text', 'Print this page image', 'Print this page text',
                      'Print all manuscript pages (3)']
    js = dict(seen['print'])
    assert js['Print this page image and text'] == [browse._print_js()]
    assert "classList.add('gs-print-image')" in js['Print this page image'][0]
    assert "classList.add('gs-print-text')" in js['Print this page text'][0]
    # the text on screen, then the page's citation
    [(text, message)] = copied
    shown, citation = text.split('\n\n', 1)
    assert shown == OPEN_PAGE.text and 'T-S 12.001' in citation and message == 'Text copied'
    name, js = seen['card_copy']
    assert name == 'Copy shelfmark'
    assert js == f'async () => emit(await {clipboard.clipboard_write_js("T-S 12.001")})'
    assert [(m, k.get('type')) for m, k in said] == [
        ('Copied', 'positive'), ('Copy failed. Select the text and copy it manually.', 'negative')]


def test_the_print_rules_print_a_part_alone():
    styles = browse.VIEWER_STYLES
    assert 'body.gs-print-image .browse-print-text-body { display: none !important; }' in styles
    assert 'body.gs-print-text .viewer-panels > .browse-image-card { display: none !important; }' in styles
    # an image hidden by Toggle Image comes back for its print (Codex on #115)
    assert 'body.gs-print-image .viewer-panels > .browse-image-card { display: block !important; }' in styles
    assert "window.addEventListener('afterprint'" in browse._print_js('image')
    # the transcription's credit goes with its text (owner, 2026-11-23)
    assert 'body.gs-print-image .browse-print-text-credit { display: none !important; }' in styles


def test_an_image_print_has_no_service_warning_and_no_transcription_credit():
    """Owner, 2026-11-23, on a printed image: "Service not available" printed above it, and MiDRASH's
    credit below it. The warning is never on paper; the transcription's credit is in its own block, which
    an image printed alone leaves out, and the site's line stays outside it."""
    import tests.render_smoke.test_browse_button_icons_render_smoke as icons
    from unittest.mock import patch as _patch

    real_service = icons._service

    def not_ready(page):
        svc = real_service(page)
        svc.is_ready = False
        return svc

    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        elements = _all_elements(user)
        seen['warning'] = [e.classes for e in elements if 'browse-service-warning' in e.classes]
        seen['credit'] = [e.content for e in elements
                          if 'browse-print-text-credit' in (getattr(e, 'content', '') or '')]

    with _patch.object(icons, '_service', side_effect=not_ready):
        _run_browse(OPEN_PAGE, driver)
    [warning] = seen['warning']
    assert 'print-hide' in warning
    [credit] = seen['credit']
    text_block, after = credit.split('<div class="browse-print-text-credit">', 1)[1].split('</div></div>', 1)
    assert 'MiDRASH' in text_block and 'Retrieved from' not in text_block
    assert 'Retrieved from' in after


def test_a_catalogue_only_record_offers_no_text_alone_to_print():
    """Codex on #115: a catalogue-only record has no text to read, so its print menu (on the toolbar) has
    "Print this page text" disabled: its sheet would have been "No text available"."""
    from dataclasses import replace
    page = replace(OPEN_PAGE, sys_id='990001234560000000', text='')   # shared/synthetic_sys_id.py
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: True)
        await _wait_until(lambda: any(e.props.get('data-testid') == 'browse-print' for e in _all_elements(user)))
        [prt] = [e for e in _all_elements(user) if e.props.get('data-testid') == 'browse-print']
        seen['entries'] = [(' '.join(d.text for d in m.descendants() if isinstance(d, ui.item_section) and d.text),
                            'disable' in m.props)
                           for m in prt.descendants() if isinstance(m, ui.menu_item)]
        [more] = [e for e in _all_elements(user) if e.props.get('data-testid') == 'browse-tools-more']
        seen['phone'] = [' '.join(d.text for d in m.descendants() if isinstance(d, ui.item_section) and d.text)
                         for m in more.descendants() if isinstance(m, ui.menu_item)]

    _run_browse(page, driver)
    entries = dict(seen['entries'])
    assert entries['Print this page text'] is True, seen['entries']
    assert entries['Print this page image and text'] is False
    # the phone's ⋮, its only print menu: the entries that apply -- this page has no image to print alone
    prints = [label for label in seen['phone'] if label.startswith('Print')]
    assert prints == ['Print this page image and text', 'Print all manuscript pages (3)'], prints


def test_a_catalogue_only_record_with_an_image_prints_it_alone_from_the_phones_menu():
    """Codex on #115: on a phone the ⋮ is a catalogue-only record's only print menu; it lacked the image alone."""
    from dataclasses import replace
    from tests.render_smoke.test_browse_button_icons_render_smoke import SWITCH_PAGE
    page = replace(SWITCH_PAGE, sys_id='990001234560000000', text='')   # Cambridge's images, no text
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: True)
        await _wait_until(lambda: any(e.props.get('data-testid') == 'browse-tools-more' for e in _all_elements(user)))
        [more] = [e for e in _all_elements(user) if e.props.get('data-testid') == 'browse-tools-more']
        seen['phone'] = [(' '.join(d.text for d in m.descendants() if isinstance(d, ui.item_section) and d.text),
                          [ls.js_handler for ls in m._event_listeners.values() if ls.type == 'click' and ls.js_handler])
                         for m in more.descendants() if isinstance(m, ui.menu_item)]

    _run_browse(page, driver)
    prints = {label: js for label, js in seen['phone'] if label.startswith('Print')}
    assert list(prints) == ['Print this page image and text', 'Print this page image',
                            'Print all manuscript pages (3)'], list(prints)
    assert prints['Print this page image'] == [browse._print_js('image')]


def test_while_the_page_is_edited_its_print_stays_on_the_toolbar():
    """Codex on #115: the text's line is drawn in view mode only, so in edit mode the print menu, every page's
    print with it, had gone from the page. It stays on the toolbar while the page is edited: "this page"
    disabled (the editor would print), every page's print as before."""
    seen = {}

    def prints(user):
        return [e for e in _all_elements(user) if e.props.get('data-testid') == 'browse-print']

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [edit] = [e for e in _all_elements(user) if e.props.get('data-testid') == 'browse-edit']
        _click(user, edit)
        await _wait_until(lambda: not any(e.props.get('data-testid') == 'browse-copy-text' for e in _all_elements(user))
                          and prints(user))
        [prt] = prints(user)
        seen['entries'] = [(' '.join(d.text for d in m.descendants() if isinstance(d, ui.item_section) and d.text),
                            'disable' in m.props)
                           for m in prt.descendants() if isinstance(m, ui.menu_item)]

    _run_browse(OPEN_PAGE, driver)
    entries = dict(seen['entries'])
    assert entries['Print all manuscript pages (3)'] is False, seen['entries']
    assert entries['Print this page image and text'] is True and entries['Print this page text'] is True
