# -*- coding: utf-8 -*-
"""Render-smoke: Browse's top on a phone (owner, 2026-11-23).

At 375x812 the image began 897px down, under a 310px green card (library and shelfmark on three
lines, the title, a row of buttons, the place on the shelf) and a 254px toolbar of unnamed icons.
Now, on a phone (the CSS: ``browse-wide-only`` hidden, ``browse-phone-only`` shown, below 641px):

* the green card is the shelfmark (the library in a span the phone hides), a ⋮ and the whole title
  on a line cut by the CSS (a tap shows it all); its links and buttons are in the ⋮, which also holds
  Add to Puzzle at every width (its button left the card);
* the page is chosen from a list (the box and its Go folded to an arrow read as forward);
* View All keeps its words, "All pages" / "One page";
* the toolbar's other buttons are in a second ⋮, each entry clicking the hidden button it stands for
  (``data-tool``), or doing the same itself; a wider screen keeps them all in sight.

Harness: ``test_browse_button_icons_render_smoke``. What the CSS does with these classes is measured
in the browser (the PR description).
"""
from __future__ import annotations

import re
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from nicegui import ui

from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE,
    _all_elements,
    _button,
    _buttons,
    _external_images,
    _open_browse,
    _service,
    _wait_until,
)
from tests.render_smoke.test_home_teaser_render_smoke import _run_home_smoke
from web.pages import browse

LONG_TITLE = 'A letter of the Gaon to the community of Fustat about the debts of the cantor'
PAGE = replace(OPEN_PAGE, library_name='Cambridge University Library', title=LONG_TITLE)


def _run(page, driver, svc=None, lang='en'):
    """``_run_browse`` of the icons test, with the service handed in (its calls are asserted)."""
    svc = svc or _service(page)
    crossref = MagicMock()
    crossref.is_available.return_value = False
    with patch.object(browse, 'get_service', return_value=svc), \
            patch('shared.nli_crossref_service.get_nli_crossref_service', return_value=crossref), \
            patch('web.components.image_resolution.resolve_external_images', side_effect=_external_images(page)), \
            patch.object(browse, 'load_connected_fragments_into', AsyncMock()), \
            patch.object(browse.GlobalAuthState, 'is_logged_in', return_value=True), \
            patch.object(browse.GlobalAuthState, 'get_user_id', return_value=None):
        _run_home_smoke(driver, lang=lang)


def _testid(user, testid):
    return [e for e in _all_elements(user) if e.props.get('data-testid') == testid]


def _entries(button):
    """A ⋮ button's menu entries: (text, classes, js handlers)."""
    out = []
    for item in button.descendants():
        if isinstance(item, ui.menu_item):
            text = ' '.join(d.text for d in item.descendants() if isinstance(d, ui.item_section) and d.text)
            js = [ls.js_handler for ls in item._event_listeners.values() if ls.type == 'click' and ls.js_handler]
            out.append((text, set(item.classes), js, dict(item.props)))
    return out


def _card(user):
    [card] = [e for e in _all_elements(user) if 'browse-header-card' in e.classes]
    return card


def test_the_green_card_is_the_shelfmark_and_the_whole_title_with_the_rest_in_its_menu():
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        card = _card(user)
        [heading] = [d for d in card.descendants() if isinstance(d, browse._BrowseHeading)]
        seen['heading'] = heading.content
        [title] = [d for d in card.descendants() if 'browse-header-title' in d.classes]
        tips = [t.text for t in _all_elements(user) if isinstance(t, ui.tooltip)
                and t.props.get('target') == f'#{title.html_id}']
        seen['tip_bounds'] = [(t.props.get('max-width'), t.props.get('max-height')) for t in _all_elements(user)
                              if isinstance(t, ui.tooltip) and t.props.get('target') == f'#{title.html_id}']
        taps = [ls.js_handler for ls in title._event_listeners.values() if ls.type == 'click']
        seen['title'] = (title.text, tips, taps)
        # the card's inline controls: a wide screen's only
        inline = [b for b in card.descendants() if isinstance(b, ui.button) and b.props.get('icon') in
                  ('search', 'info', 'star_border', 'star', 'share', 'extension')]
        seen['inline'] = sorted((b.props.get('icon'), 'browse-wide-only' in b.classes) for b in inline)
        links = [d for d in card.descendants() if isinstance(d, ui.link) and 'Ktiv' in
                 ' '.join(x.text for x in d.descendants() if isinstance(x, ui.label))]
        seen['ktiv_link_wide'] = all('browse-wide-only' in d.classes for d in links) and bool(links)
        [more] = _testid(user, 'browse-header-more')
        seen['more'] = ('browse-phone-only' in more.classes, more.props.get('aria-label'))
        seen['entries'] = [(text, 'browse-phone-only' in cls, props.get('href', '')) for text, cls, _js, props in
                           _entries(more)]

    _run(PAGE, driver)
    content = seen['heading']
    # the screen reader's line as before (both spans in one h2); a phone hides the library's
    assert re.search(r'<h2 [^>]*><span class="browse-h2-lib">Cambridge University Library, </span>'
                     r'<span class="browse-h2-sm">T-S 12.001</span></h2>', content), content
    title, tips, taps = seen['title']
    assert title == LONG_TITLE and tips == [LONG_TITLE]                 # whole: not cut at five words
    # ... in a box of bounded size; and the title's row narrower than the title at every width (owner,
    # 2026-11-23: at half the screen a long title stretched the card and the page to ~12,800px)
    assert seen['tip_bounds'] == [('min(640px, 90vw)', '40vh')]
    assert '\n    .browse-header-center { min-width: 0; }\n' in browse.VIEWER_STYLES
    assert taps == [browse._TITLE_TAP_JS]
    # no puzzle button on the card; every other button there is a wide screen's
    # share in sight on a phone too, copying the shelfmark before it (owner, 2026-11-23)
    assert seen['inline'] == [('info', True), ('search', True), ('share', False), ('star_border', True)]
    assert seen['ktiv_link_wide']
    assert seen['more'] == (True, 'More actions')                       # a phone's only (the puzzle is on the toolbar)
    entries = seen['entries']
    assert [t for t, _phone, _h in entries] == ['Ktiv', 'Search for Parallels', 'Show Metadata', 'Add to List']
    assert [t for t, phone, _h in entries if not phone] == []
    assert entries[0][2].startswith('https://www.nli.org.il/')          # a link: it opens in the browser


def test_every_piece_of_the_card_has_its_place_on_a_phone():
    """A phone orders the card's line: the shelfmark (an Oxford part's badge beside it), ⋮, then the title.
    A piece the phone rules do not place takes order 0 and goes first (Codex #107: the Oxford badge did)."""
    seen = {}
    placed = {'browse-header-h2': 1, 'browse-header-part': 1, 'browse-header-more': 2, 'browse-header-tool': 2,
              'browse-header-titlebox': 3}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [center] = [d for d in _card(user).descendants() if 'browse-header-center' in d.classes]
        # what shows in the row: tooltips open out of it
        seen['pieces'] = [(next((c for c in placed if c in el.classes), None), 'browse-wide-only' in el.classes)
                          for el in center.default_slot.children if not isinstance(el, ui.tooltip)]

    _run(replace(PAGE, oxford_part_label='part 6'), driver)
    pieces = seen['pieces']
    assert ('browse-header-part', False) in pieces
    assert [p for p in pieces if p[0] is None and not p[1]] == [], pieces
    phone = browse.VIEWER_STYLES.split('@media (max-width: 640px) {')[1]
    for cls, order in placed.items():
        assert re.search(rf'\.browse-header-center > \.{cls} \{{ order: {order};', phone), cls


def test_the_cards_words_are_short_and_its_names_whole():
    # Owner, 2026-11-23: "Parallels Search" and "Metadata" on the card; the metadata button's name says
    # the action (it contains its words); CUDL for the Cambridge Digital Library, its whole name a tooltip
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        card = _card(user)
        seen['buttons'] = sorted((b.text, b.props.get('aria-label')) for b in card.descendants()
                                 if isinstance(b, ui.button) and b.text)

    _run(PAGE, driver)
    assert seen['buttons'] == [('Metadata', 'Show Metadata'), ('Parallels Search', 'Parallels Search')]
    assert browse._SHORT_LIBRARY_LABELS['Cambridge Digital Library'] == 'CUDL'


def test_the_page_is_chosen_from_a_list():
    svc = _service(PAGE)
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [select] = [e for e in _all_elements(user) if isinstance(e, ui.select) and 'browse-page-select' in e.classes]
        seen['options'] = (select.options, select.value, select.props.get('aria-label'))
        seen['go'] = [b for b in _buttons(user) if b.text == 'Go' and b.props.get('icon') != 'search']
        svc.get_browse_page.reset_mock()
        with user.client:
            select.value = 2
        await _wait_until(lambda: svc.get_browse_page.called)
        seen['asked'] = [c.kwargs.get('p_num') for c in svc.get_browse_page.call_args_list]

    _run(PAGE, driver, svc)
    assert seen['options'] == ({1: '1', 2: '2', 3: '3'}, 1, 'Page')
    assert seen['go'] == []                                             # the page's Go and its arrow are gone
    assert 2 in seen['asked']


def test_view_all_keeps_its_words():
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'All pages' in texts)
        [button] = _button(user, 'All pages')
        tips = [t.text for t in _all_elements(user) if isinstance(t, ui.tooltip)
                and t.props.get('target') == f'#{button.html_id}']
        seen['all'] = (button.props.get('icon'), set(button.classes), button.props.get('aria-label'), tips)

    _run(PAGE, driver)
    icon, classes, label, tips = seen['all']
    assert icon == 'view_agenda' and label == 'All pages' and tips == ['Show Full Manuscript']
    assert 'browse-icon-wide' in classes and 'browse-icon-collapse' not in classes   # a phone drops the icon


def test_the_toolbar_menu_stands_in_for_the_buttons_a_phone_hides():
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [more] = _testid(user, 'browse-tools-more')
        seen['more'] = ('browse-phone-only' in more.classes, more.props.get('aria-label'))
        seen['entries'] = [(text, js) for text, _cls, js, _props in _entries(more)]
        tools = {}
        for e in _all_elements(user):
            tool = e.props.get('data-tool')
            if tool:
                tools[tool] = 'browse-wide-only' in e.classes or 'browse-joins-slot' in e.classes
        seen['tools'] = tools
        hidden = [b for b in _buttons(user) if b.props.get('icon') in ('print', 'library_add', 'star_border', 'extension')
                  and 'browse-controls-card' in {c for a in b.ancestors() for c in a.classes}]
        seen['hidden'] = sorted((b.props.get('icon'), 'browse-wide-only' in b.classes) for b in hidden)
        # by the text, on every width: copy, print, edit, send a comment (owner, 2026-11-23)
        [head] = [e for e in _all_elements(user) if 'browse-text-head' in e.classes]
        seen['by_text'] = sorted((b.props.get('icon'), 'browse-wide-only' in b.classes) for b in head.descendants()
                                 if isinstance(b, ui.button) and b.props.get('icon') in
                                 ('content_copy', 'print', 'edit', 'comment', 'add_comment', 'chat'))

    _run(PAGE, driver)
    assert seen['more'] == (True, 'More actions')
    texts = [t for t, _js in seen['entries']]
    # print, edit and send a comment are by the text, in sight on a phone too (owner, 2026-11-23)
    assert texts == ['Add to List', 'Toggle Image', 'View Comments', 'Joined Fragments', 'Add to Reading Desk',
                     'Add to Puzzle']
    # each entry that clicks a hidden button names one that is there, and hidden on a phone
    clicked = []
    for _text, js in seen['entries']:
        for handler in js:
            clicked += re.findall(r'data-tool=\\"(\w+)\\"', handler)
    assert sorted(set(clicked)) == ['image', 'joins', 'notes']
    assert all(seen['tools'].get(tool) for tool in clicked), seen['tools']
    # the print, Reading Desk and puzzle buttons are a wide screen's (the menu does their work itself);
    # Add to Puzzle is on the toolbar, beside Print (owner, 2026-11-23)
    assert seen['hidden'] == [('extension', True), ('library_add', True), ('star_border', True)]
    assert [icon for icon, wide in seen['by_text'] if wide] == []
    assert {'content_copy', 'print', 'edit'} <= {icon for icon, _w in seen['by_text']}


def test_the_phone_rules_ship_with_the_page_and_the_header():
    styles = browse.VIEWER_STYLES
    # the computed identifications are their icon on the toolbar, "New" a dot; less room above the
    # lookup (owner, 2026-11-23)
    assert ('.browse-discovery-slot .gs-discovery .q-btn__content > .block { font-size: 0; width: 0; '
            'overflow: hidden; }') in styles
    assert '.browse-title-wrap { position: absolute !important; }' in styles
    # no credit under an image that could not be shown (owner, 2026-11-23; manuscript_viewer.js hides it)
    assert '.browse-image-credit[hidden] { display: none !important; }' in styles
    assert '.main-content:has(> .browse-root) { padding-top: 8px !important; }' in styles
    assert '@media not all and (max-width: 640px) {\n        .browse-phone-only { display: none !important; }' in styles
    phone = styles.split('@media (max-width: 640px) {')[1]
    for rule in ('.browse-wide-only { display: none !important; }', '.browse-h2-lib { position: absolute; width: 1px; height: 1px;',
                 '.browse-shelf-place { display: none !important; }',
                 '.browse-joins-slot:not(:has(.ring-2)) { display: none !important; }'):
        assert rule in phone, rule
    # the library is hidden from sight only: display: none would drop it from the heading a screen reader
    # reads (Codex #107)
    assert '.browse-h2-lib { display: none' not in styles
    css = Path(__file__).resolve().parents[2].joinpath('web', 'static', 'common.css').read_text(encoding='utf-8')
    block = css[css.index('@media (max-width: 640px) {\n    .app-header > .nicegui-row'):]
    block = block[:block.index('\n}\n')]
    assert '.app-header .header-search:not(.is-open) .header-search-toggle { display: inline-flex !important; }' in block
    assert '.app-header .header-search:not(.is-open) .header-search-field { display: none !important; }' in block


def test_the_hebrew_words():
    seen = {}

    async def driver(user):
        await _open_browse(user, lambda texts: 'כל העמודים' in texts)
        seen['all'] = bool(_button(user, 'כל העמודים'))

    _run(PAGE, driver, lang='he')
    assert seen['all']
