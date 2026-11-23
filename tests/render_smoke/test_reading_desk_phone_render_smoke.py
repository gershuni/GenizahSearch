# -*- coding: utf-8 -*-
"""Render-smoke: the Reading Desk on a phone (owner, 2026-11-23).

Below 900px the desk's two panes stood side by side at half the width each (about 170px at
375px): images and texts were cut off. Now an Image | Text switch above the panes (shown below
900px only) chooses the one pane a phone shows, full width and the screen's height:

* the switch moves a class on the panes (``rd-show-text``), nothing is drawn again, and the choice
  is kept with the desk's others (``__phone_pane__``), so a redrawn desk shows the same pane;
* the CSS hides the other pane there and stacks the cards (``browse_desk_images.PHONE_CSS``);
* the desk's header and toolbar buttons give up their words for their icons below 600px, each
  keeping its name; the box to add a shelfmark takes a line of its own.

Harness: ``test_reading_desk_document_render_smoke`` (the real /browse, the desk entered as the
joins dialog enters it). The CSS's effect is measured in the browser (the PR description).
"""
from __future__ import annotations

from unittest.mock import patch

from nicegui import ui
from nicegui.client import Client

from tests.render_smoke.test_browse_button_icons_render_smoke import _all_elements, _click, _wait_until
from tests.render_smoke.test_reading_desk_document_render_smoke import FRAGMENTS, SID_A, _run
from web.pages import browse, browse_state
from web.pages import browse_desk_images as di
from web.safe_storage import safe_user_get, safe_user_set


def _one(user, cls):
    found = [e for e in _all_elements(user) if cls in e.classes]
    return found[0] if found else None


def _switch(user):
    found = [e for e in _all_elements(user) if e.props.get('data-testid') == 'rd-phone-pane']
    return found[0] if found else None


def test_the_phone_switch_shows_one_pane_and_keeps_the_choice(tmp_path):
    saved, seen = [], {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None and _one(user, 'rd-text-card') is not None)
        switch = _switch(user)
        panels = _one(user, 'reading-desk-panels')
        seen['start'] = (switch.options, switch.value, 'rd-show-text' in panels.classes,
                         'rd-phone-only' in switch.parent_slot.parent.classes,
                         _one(user, 'rd-image-card') is not None)
        card = _one(user, 'rd-text-card')
        with user._client:
            switch.value = 'text'
        await _wait_until(lambda: 'rd-show-text' in panels.classes)
        seen['text'] = ('rd-show-text' in panels.classes, _one(user, 'rd-text-card') is card,
                        saved[-1].get(di.KEY_PHONE_PANE) if saved else None)
        # the desk drawn again (another image view): the pane chosen stays
        [view] = [e for e in _all_elements(user) if isinstance(e, ui.toggle) and 'rd-image-view' in e.classes]
        with user._client:
            view.value = di.VIEW_ALL
        await _wait_until(lambda: _one(user, 'rd-text-card') is not card)
        panels = _one(user, 'reading-desk-panels')
        seen['again'] = ('rd-show-text' in panels.classes, _switch(user).value)
        with user._client:
            _switch(user).value = 'image'
        await _wait_until(lambda: 'rd-show-text' not in panels.classes)
        seen['back'] = 'rd-show-text' in panels.classes

    _run(tmp_path, driver, saved=saved)
    assert seen['start'] == ({'image': 'Image', 'text': 'Text'}, 'image', False, True, True)
    assert seen['text'] == (True, True, 'text')    # a class moved, the text side not drawn again
    assert seen['again'] == (True, 'text')
    assert seen['back'] is False


def test_the_desks_buttons_keep_their_names_as_icons(tmp_path):
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        buttons = [b for b in _all_elements(user) if isinstance(b, ui.button)
                   and b.text in ('Back to Page View', 'Open in Puzzle', 'Add', 'Add from List')]
        seen['buttons'] = sorted((b.text, 'browse-icon-collapse' in b.classes, b.props.get('aria-label')) for b in buttons)
        seen['box'] = _one(user, 'rd-add-box') is not None

    _run(tmp_path, driver)
    assert seen['buttons'] == [('Add', True, 'Add'), ('Add from List', True, 'Add from List'),
                               ('Back to Page View', True, 'Back to Page View'),
                               ('Open in Puzzle', True, 'Open in Puzzle')]
    assert seen['box']
    # (Pick from the shelf, absent here without a shelf tree, gets the same class: checked in the browser.)


def test_the_phone_rules_ship_with_the_desk():
    css = di.PHONE_CSS
    assert '@media not all and (max-width: 900px) { .rd-phone-only { display: none !important; } }' in css
    narrow = css.split('@media (max-width: 900px) {')[1]
    for rule in ('.reading-desk-panels:not(.rd-show-text) > .rd-text-card { display: none !important; }',
                 '.reading-desk-panels.rd-show-text > .rd-image-card { display: none !important; }',
                 '.reading-desk-panels { flex-direction: column !important; min-height: 0 !important; }'):
        assert rule in narrow, rule
    assert di.phone_pane({}) == 'image' and di.phone_pane({di.KEY_PHONE_PANE: 'text'}) == 'text'
    assert di.phone_pane({di.KEY_PHONE_PANE: 'nonsense'}) == 'image'


def test_the_switch_changes_the_pane_in_the_browser_and_keeps_the_readers_place(tmp_path):
    """Owner, 2026-11-23: "Text" sometimes threw the page to its top. The switch changes the class in the
    browser at once and keeps the switch where it was on the screen (or at its top, the reader having
    scrolled into the pane); the server's change follows and says the same."""
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        seen['js'] = [ls.js_handler for ls in _switch(user)._event_listeners.values()
                      if ls.type == 'update:modelValue' and ls.js_handler]       # NiceGUI keeps it camelCase

    _run(tmp_path, driver)
    assert seen['js'] == [di.PANE_SWITCH_JS]
    js = di.PANE_SWITCH_JS
    assert "panels.classList.toggle('rd-show-text', value === 'text')" in js
    assert 'window.scrollBy(0, sw.getBoundingClientRect().top - Math.max(before, 0))' in js


def test_the_thumbnails_are_squares_scrolled_sideways_on_a_narrow_screen():
    """Owner, 2026-11-23: two or three bigger squares, scrolled sideways (they were 68px slivers)."""
    narrow = di.IMAGES_CSS.split('@media (max-width: 900px) {')[1]
    for rule in ('--rd-square: min(calc((100vw - 48px) / 2.5), 180px);',
                 '.rd-filmstrip .rd-thumb { flex: 0 0 auto; width: var(--rd-square); }',
                 '.rd-filmstrip .rd-thumb img { width: 100%; height: auto; aspect-ratio: 1 / 1; }',
                 'height: calc(var(--rd-square) + 40px) !important;', 'flex-direction: row !important; flex-wrap: nowrap;'):
        assert rule in narrow, rule
    # the image's wrapper fills the thumbnail: centred, it shrank to nothing and the image with it (0x0)
    assert 'display: flex; flex-direction: column; align-items: stretch; text-align: center;' in di.IMAGES_CSS


def _back_button(user):
    found = [e for e in _all_elements(user) if e.props.get('data-testid') == 'desk-return']
    return found[0] if found else None


def test_a_shelfmark_clicked_in_the_desk_leaves_a_way_back_to_it(tmp_path):
    """Owner, 2026-11-23: a click on a shelfmark in the desk opened it in Browse, and the desk was gone.
    The desk is kept; Browse offers "Back to the Reading Desk (n)", which brings it back as it was."""
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        seen['in_desk'] = _back_button(user)
        names = [e for e in _all_elements(user) if isinstance(e, ui.label) and e.text == 'Frag B'
                 and any(ls.type == 'click' for ls in e._event_listeners.values())]
        if not names:      # the image on show is Frag A's: its name is the one to click
            names = [e for e in _all_elements(user) if isinstance(e, ui.label) and e.text == 'Frag A'
                     and any(ls.type == 'click' for ls in e._event_listeners.values())]
        _click(user, names[0])
        await _wait_until(lambda: _back_button(user) is not None)
        seen['left'] = (_switch(user), _back_button(user).text)
        with user._client:
            seen['kept'] = [e['sys_id'] for e in browse_state.desk_to_return()['entries']]
        _click(user, _back_button(user))
        await _wait_until(lambda: _switch(user) is not None)
        seen['back'] = _back_button(user)
        with user._client:
            seen['kept_after'] = browse_state.desk_to_return()

    _run(tmp_path, driver)
    assert seen['in_desk'] is None
    assert seen['left'] == (None, 'Back to the Reading Desk (2)')
    assert seen['kept'] == [f['document_id'] for f in FRAGMENTS]
    assert seen['back'] is None and seen['kept_after'] is None


def test_back_to_page_view_closes_the_desk_with_no_way_back(tmp_path):
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        [arrow] = [b for b in _all_elements(user) if isinstance(b, ui.button) and b.text == 'Back to Page View']
        _click(user, arrow)
        await _wait_until(lambda: _switch(user) is None)
        seen['button'] = _back_button(user)

    _run(tmp_path, driver)
    assert seen['button'] is None


def _leave_by_a_shelfmark(user):
    names = [e for e in _all_elements(user) if isinstance(e, ui.label) and e.text in ('Frag A', 'Frag B')
             and any(ls.type == 'click' for ls in e._event_listeners.values())]
    _click(user, names[0])


def test_the_way_back_shows_when_the_shelfmark_found_nothing(tmp_path):
    """Codex on #109: a shelfmark clicked in the desk that finds nothing leaves the page on its error (or
    welcome) screen, drawn before anything else returned: the way back is drawn first, so it is there too."""
    states, seen = [], {}

    class Recording(browse.BrowseState):
        def __init__(self):
            super().__init__()
            states.append(self)

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        [state] = states
        _leave_by_a_shelfmark(user)
        # the shelfmark's search finished (it draws the page it found): then its failure is drawn
        await _wait_until(lambda: _back_button(user) is not None and not state.is_loading
                          and state.current_page is not None and state.current_page.shelfmark == 'Frag C')
        redraw = [b for b in _all_elements(user) if isinstance(b, ui.button)
                  and ({b.text, b.props.get('aria-label')} & {'Show Metadata', 'Hide Metadata'})]
        state.error, state.current_page = 'Nothing found', None
        _click(user, redraw[0])                                         # the page drawn again, on its error
        await _wait_until(lambda: any(isinstance(e, ui.label) and e.text == 'Nothing found'
                                      for e in _all_elements(user)))
        seen['error_screen'] = not any('browse-header-card' in e.classes for e in _all_elements(user))
        seen['back'] = _back_button(user) is not None
        # and it brings the desk back from there: no page to draw it around, its first fragment's is loaded
        # (Codex on #109: the desk came back unseen, the screen still the error's)
        _click(user, _back_button(user))
        await _wait_until(lambda: _switch(user) is not None)
        seen['desk'] = ([e.get('sys_id') for e in state.reading_desk_entries], state.current_page.sys_id,
                        _back_button(user))

    with patch.object(browse, 'BrowseState', Recording):
        _run(tmp_path, driver)
    assert seen['error_screen'] is True
    assert seen['back'] is True
    assert seen['desk'] == ([f['document_id'] for f in FRAGMENTS], FRAGMENTS[0]['document_id'], None)


def test_a_reader_who_keeps_nothing_between_sessions_still_has_the_way_back(tmp_path):
    """Codex on #109: with "Save search state between sessions" off the desk left is kept on the page only,
    never in the reader's storage, as the desk itself."""
    seen = {}

    async def driver(user, enter):
        with user._client:
            safe_user_set('session_persistence_enabled', False)
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        _leave_by_a_shelfmark(user)
        await _wait_until(lambda: _back_button(user) is not None)
        with user._client:
            seen['stored'] = safe_user_get(browse_state.DESK_LEFT_KEY)
        _click(user, _back_button(user))
        await _wait_until(lambda: _switch(user) is not None)
        seen['back'] = _back_button(user)

    _run(tmp_path, driver)
    assert seen['stored'] is None
    assert seen['back'] is None


def test_an_embedded_viewer_offers_no_way_back(tmp_path):
    """Codex on #109: an embedded viewer (?embed=1, the discovery and Atlas previews) neither reads nor
    restores the reader's Browse state: with a desk kept, it shows no way back, and the desk stays kept."""
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        _leave_by_a_shelfmark(user)
        await _wait_until(lambda: _back_button(user) is not None)
        await user.open(f'/browse?sys_id={SID_A}&embed=1')
        await _wait_until(lambda: _one(user, 'viewer-panels') is not None)
        seen['embedded'] = _back_button(user)
        with user._client:
            seen['kept'] = browse_state.desk_to_return() is not None

    _run(tmp_path, driver)
    assert seen['embedded'] is None
    assert seen['kept'] is True


def test_a_desk_that_does_not_come_back_stays_kept(tmp_path):
    """Codex on #109: the way back read the kept desk destructively; a desk whose fragments could not be
    read again was lost with its button. It is forgotten once the desk is built again, not before: one that
    fails leaves the page as it was, the desk still kept and the way back still offered."""
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        _leave_by_a_shelfmark(user)
        await _wait_until(lambda: _back_button(user) is not None)
        service = browse.get_service()
        real = service.get_full_manuscript.side_effect
        service.get_full_manuscript.side_effect = RuntimeError('the library did not answer')
        _click(user, _back_button(user))
        await _wait_until(lambda: _back_button(user) is not None and _switch(user) is None)
        with user._client:
            seen['kept'] = browse_state.desk_to_return() is not None
        service.get_full_manuscript.side_effect = real               # the library answers again
        _click(user, _back_button(user))
        await _wait_until(lambda: _switch(user) is not None)
        with user._client:
            seen['after'] = (_back_button(user), browse_state.desk_to_return())

    _run(tmp_path, driver)
    assert seen['kept'] is True
    assert seen['after'] == (None, None)


def test_a_desk_opened_in_an_embedded_viewer_leaves_the_kept_one(tmp_path):
    """Codex on #109: a desk opened inside an embedded viewer (its Add to Reading Desk) forgot the reader's
    kept desk; an embedded viewer touches none of the reader's Browse state."""
    seen = {}

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        _leave_by_a_shelfmark(user)
        await _wait_until(lambda: _back_button(user) is not None)
        await user.open(f'/browse?sys_id={SID_A}&embed=1')
        await _wait_until(lambda: _one(user, 'viewer-panels') is not None)
        enter(FRAGMENTS, None)                                          # a desk opened in the embedded viewer
        await _wait_until(lambda: _switch(user) is not None)
        with user._client:
            seen['kept'] = browse_state.desk_to_return() is not None

    _run(tmp_path, driver)
    assert seen['kept'] is True


def test_leaving_by_a_shelfmark_brings_the_way_back_into_sight(tmp_path):
    """Owner, 2026-11-23, on a phone: after a shelfmark clicked in the desk "it didn't have a way back". It
    had, 402px above the screen: the page stayed scrolled where the desk had been. Leaving the desk by a
    shelfmark takes the page to its top, where the button is."""
    sent, states, seen = [], [], {}
    real = Client.run_javascript

    def spy(client, code, *args, **kwargs):
        result = real(client, code, *args, **kwargs)
        sent.append(code)                                               # sent: it did not raise
        return result

    class Recording(browse.BrowseState):
        def __init__(self):
            super().__init__()
            states.append(self)

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        [state] = states
        with patch.object(Client, 'run_javascript', spy):
            _leave_by_a_shelfmark(user)
            # and the search went on to the manuscript it found (the first try raised there and stopped it)
            await _wait_until(lambda: _back_button(user) is not None and state.current_page is not None
                              and state.current_page.shelfmark == 'Frag C')
        seen['top'] = [c for c in sent if c == di.TO_THE_TOP_JS]

    with patch.object(browse, 'BrowseState', Recording):
        _run(tmp_path, driver)
    assert seen['top'] == [di.TO_THE_TOP_JS]
    assert 'window.scrollTo({top: 0' in di.TO_THE_TOP_JS


def test_back_from_an_empty_result_with_no_page_to_load_keeps_the_desk(tmp_path):
    """Codex on #109: back from the error screen the desk is rebuilt (and forgets the kept one), then its
    first fragment's page is loaded; when that page does not come either, the desk is kept again with its
    way back, and the next try brings it."""
    states, seen = [], {}

    class Recording(browse.BrowseState):
        def __init__(self):
            super().__init__()
            states.append(self)

    async def driver(user, enter):
        enter(FRAGMENTS, None)
        await _wait_until(lambda: _switch(user) is not None)
        [state] = states
        _leave_by_a_shelfmark(user)
        await _wait_until(lambda: _back_button(user) is not None and not state.is_loading
                          and state.current_page is not None and state.current_page.shelfmark == 'Frag C')
        redraw = [b for b in _all_elements(user) if isinstance(b, ui.button)
                  and ({b.text, b.props.get('aria-label')} & {'Show Metadata', 'Hide Metadata'})]
        state.error, state.current_page = 'Nothing found', None
        _click(user, redraw[0])
        await _wait_until(lambda: _back_button(user) is not None and _one(user, 'browse-header-card') is None)
        service = browse.get_service()
        real = service.get_browse_page.side_effect
        service.get_browse_page.side_effect = lambda *a, **k: None          # the page does not come,
        service.get_metadata_only_browse_page.return_value = None           # nor its catalogue record
        _click(user, _back_button(user))
        # the click's load ran (its own error replaced the forced one) and ended
        await _wait_until(lambda: state.error != 'Nothing found' and not state.is_loading)
        seen['after_failure'] = (state.view_joined, _back_button(user) is not None)
        with user._client:
            seen['kept'] = browse_state.desk_to_return(state) is not None
        service.get_browse_page.side_effect = real
        _click(user, _back_button(user))
        await _wait_until(lambda: _switch(user) is not None and state.current_page is not None)
        seen['desk'] = [e.get('sys_id') for e in state.reading_desk_entries]

    with patch.object(browse, 'BrowseState', Recording):
        _run(tmp_path, driver)
    assert seen['after_failure'] == (False, True)                    # off the desk, the way back offered
    assert seen['kept'] is True
    assert seen['desk'] == [f['document_id'] for f in FRAGMENTS]
