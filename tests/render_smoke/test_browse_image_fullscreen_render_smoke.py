# -*- coding: utf-8 -*-
"""Render-smoke: /browse keeps the image full screen across Prev/Next (2026-11-19).

The full screen (F) was a class the browser put on the image card, so a page
turn -- which rebuilds the page and with it the card -- dropped the reader out
of it. The state is now the page's and the card is drawn with it. Harness: the
stubbed browse page of ``test_browse_button_icons_render_smoke`` (its service
answers every page turn with the same three-page fragment, which is enough: the
page is rebuilt either way).
"""
from __future__ import annotations

from nicegui import ui

from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE,
    _buttons,
    _click,
    _open_browse,
    _run_browse,
    _wait_until,
)
from tests.render_smoke.test_browse_zoom_render_smoke import _image_markup
from web.pages import browse


def _action_button(user, action):
    [button] = [b for b in _buttons(user) if b.props.get('data-action') == action]
    return button


def _image_card(user):
    """The card holding the image's Fullscreen button."""
    element = _action_button(user, 'fullscreen')
    while not isinstance(element, ui.card):
        element = element.parent_slot.parent
    return element


def test_the_image_stays_full_screen_on_the_next_page():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Show Metadata' in texts)
        card = _image_card(user)
        assert 'fullscreen-mode' not in card.classes
        assert 'width=400' in _image_markup(user), 'the stub page draws an NLI image with its preview'

        _click(user, _action_button(user, 'fullscreen'))
        assert 'fullscreen-mode' in card.classes

        # Next (what the arrow keys click): a new card, still full screen.
        _click(user, _action_button(user, 'next'))
        await _wait_until(lambda: _image_card(user) is not card and 'Show Metadata' in [b.text for b in _buttons(user)])
        rebuilt = _image_card(user)
        assert rebuilt is not card, 'the page was not rebuilt'
        assert 'fullscreen-mode' in rebuilt.classes

        # ...and drawn with the full image at once: the 400 px preview showed
        # small in the middle of the screen before the full one (2026-11-20).
        assert 'width=400' not in _image_markup(user)

        # F / Esc (both click the button) leave it, and the next page stays out.
        _click(user, _action_button(user, 'fullscreen'))
        assert 'fullscreen-mode' not in rebuilt.classes
        _click(user, _action_button(user, 'next'))
        await _wait_until(lambda: _image_card(user) is not rebuilt and 'Show Metadata' in [b.text for b in _buttons(user)])
        assert _image_card(user) is not rebuilt, 'the page was not rebuilt'
        assert 'fullscreen-mode' not in _image_card(user).classes
        assert 'width=400' in _image_markup(user), 'out of full screen the preview comes first, as before'

    _run_browse(OPEN_PAGE, driver)


def test_esc_leaves_through_the_button():
    """Esc is the page's keydown handler clicking the Fullscreen button, so the
    state follows; no listener of its own that only drops the class."""
    with open(browse.__file__, encoding='utf-8') as f:
        source = f.read()
    esc = source.index("if (e.key === 'Escape')", source.index("// Esc leaves the image's full screen"))
    click = "document.querySelector('[data-action=\"fullscreen\"]')?.click();"
    assert source.index(click, esc) - esc < 200, 'Esc must click the Fullscreen button'
    # Before the loading and text-box guards, after the dialog guard (a dialog's Esc is its own).
    assert source.index(".q-dialog, .q-menu')) return;") < esc < source.index("querySelector('.browse-loading')) return;")
    # The class is never set in the browser alone (that is what a page turn lost).
    assert "classList.toggle('fullscreen-mode')" not in source


def test_the_full_screen_image_fits_the_window():
    """The image box takes what the toolbar leaves, and the image's max-height
    reaches it through ui.html's wrapper div (auto height: a tall folio ran
    off the bottom of the screen, 2026-11-20). Measured in a browser: a
    2000x3179 folio drew 1400x2225 in a 1400x900 window, now 500x795."""
    css = browse.VIEWER_STYLES
    card = css[css.index('.fullscreen-mode {'):]
    card = card[:card.index('}')]
    for rule in ('inset: 0 !important', 'display: flex !important', 'flex-direction: column !important',
                 'padding: 0 !important'):
        assert rule in card, rule
    # The rule after the card's (a phone rule earlier in the sheet starts the same way).
    box = css[css.index('.fullscreen-mode .image-container {', css.index('.fullscreen-mode {')):]
    box = box[:box.index('}')]
    assert 'flex: 1 1 0 !important' in box and 'min-height: 0 !important' in box
    assert '100vh' not in box, 'a 100vh box under the toolbar runs off the screen'
    assert '.fullscreen-mode .image-container .img-loading-container > div { display: contents; }' in css
