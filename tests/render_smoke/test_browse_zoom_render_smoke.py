# -*- coding: utf-8 -*-
"""Render-smoke: the /browse zoom buttons answer in the browser (2026-11-03).

A click on + or - carries no server handler -- the button's ``js_handler``
calls the viewer, which applies the transform at once -- and the page hears
the level afterwards through the ``browse_zoom`` event, so a page it rebuilds
draws the image at that level. Harness: the stubbed browse page of
``test_browse_button_icons_render_smoke``. The browser side (the transform
itself) is ``tests/test_manuscript_viewer_js.py``.
"""
from __future__ import annotations

from unittest.mock import patch

from nicegui import events, ui

from tests.render_smoke.test_browse_button_icons_render_smoke import (
    OPEN_PAGE,
    _all_elements,
    _button,
    _button_with_tooltip,
    _buttons,
    _click,
    _open_browse,
    _run_browse,
    _texts,
    _wait_until,
)
from web.pages import browse


def _action_button(user, action):
    [button] = [b for b in _buttons(user) if b.props.get('data-action') == action]
    return button


def _click_listeners(element):
    return [l for l in element._event_listeners.values() if l.element_id == element.id and l.type == 'click']


def _image_markup(user):
    """The inline transform the page drew the main image with."""
    found = [e.content for e in _all_elements(user) if isinstance(e, ui.html) and 'class="zoomable-image"' in e.content]
    assert found, 'no image markup on the page'
    return found[0]


def _zoom_labels(user):
    return [e.text for e in _all_elements(user) if isinstance(e, ui.label) and 'zoom-level-label' in e.classes]


def _tell_the_page(user, payload):
    """Deliver a ``browse_zoom`` event the way the browser's emitEvent would."""
    layout = user._client.layout
    [listener] = [l for l in layout._event_listeners.values() if l.type == 'browse_zoom']
    with user._client:
        args = events.GenericEventArguments(sender=layout, client=user._client, args=payload)
        events.handle_event(listener.handler, args)


def _rebuild(user):
    """Show / Hide Metadata rebuilds the page content, image markup included."""
    toggles = [b for b in _buttons(user) if b.text in ('Show Metadata', 'Hide Metadata')]
    assert len(toggles) == 1, [b.text for b in toggles]
    _click(user, toggles[0])


def test_the_zoom_buttons_carry_no_server_click_handler():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        for action, call in (('zoom-in', "browseZoom('in')"), ('zoom-out', "browseZoom('out')")):
            button = _action_button(user, action)
            [listener] = _click_listeners(button)
            assert listener.handler is None, f'{action} still goes through the server'
            assert call in listener.js_handler, listener.js_handler
            assert button.props.get('aria-label'), action
        # Reset View applies in the browser first, then reaches the server for the sliders and the state.
        reset = _action_button(user, 'zoom-reset')
        [listener] = _click_listeners(reset)
        assert listener.handler is not None
        assert 'v.reset()' in listener.js_handler and 'emit(e)' in listener.js_handler
        assert _button_with_tooltip(user, 'Zoom in (+)') is _action_button(user, 'zoom-in')

    _run_browse(OPEN_PAGE, driver)


def test_the_page_hears_the_level_and_a_rebuilt_page_keeps_it():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Show Metadata' in texts)
        assert 'scale(1.0)' in _image_markup(user)
        assert _zoom_labels(user) == ['100%']

        with patch.object(ui, 'run_javascript') as run_js:
            _tell_the_page(user, {'scale': 1.5, 'seq': 7})
        run_js.assert_not_called()  # the level is already applied in the browser
        _rebuild(user)
        await _wait_until(lambda: 'scale(1.5)' in _image_markup(user))
        assert _zoom_labels(user) == ['150%']
        # The image says which report it followed, so the viewer can tell a
        # rebuild drawn before its latest level (Codex on #471); the number never goes back.
        assert 'data-zoom-seq="7"' in _image_markup(user)
        _tell_the_page(user, {'scale': 1.5, 'seq': 3})
        _tell_the_page(user, {'scale': 1.5, 'seq': 'x'})
        _rebuild(user)
        await _wait_until(lambda: 'Show Metadata' in _texts(user))
        assert 'data-zoom-seq="7"' in _image_markup(user)

        # Clamped to the viewer's range, and a broken payload changes nothing.
        _tell_the_page(user, {'scale': 50})
        _rebuild(user)
        await _wait_until(lambda: 'scale(8.0)' in _image_markup(user))
        assert _zoom_labels(user) == ['800%']
        _tell_the_page(user, {'scale': 'abc'})
        _tell_the_page(user, {})
        _tell_the_page(user, 'not a dict')
        _rebuild(user)
        await _wait_until(lambda: 'Show Metadata' in _texts(user))
        assert 'scale(1.0)' in _image_markup(user), 'a payload that is not a finite number is the fit level'

    _run_browse(OPEN_PAGE, driver)


def test_reset_view_and_rotation_never_push_a_scale():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Show Metadata' in texts)
        _tell_the_page(user, {'scale': 2.25})
        _rebuild(user)
        await _wait_until(lambda: 'scale(2.25)' in _image_markup(user))

        # A rotation goes to the viewers as a rotation alone: the scale stays the browser's.
        with patch.object(ui, 'run_javascript') as run_js:
            _click(user, _button_with_tooltip(user, 'Rotate Left'))
        sent = ' '.join(str(c.args[0]) for c in run_js.call_args_list)
        assert 'setRotation(270)' in sent
        assert 'scale' not in sent and 'update(' not in sent, sent
        _rebuild(user)
        await _wait_until(lambda: 'rotate(270deg)' in _image_markup(user))
        assert 'scale(2.25)' in _image_markup(user), 'the rotation left the level alone'

        # Reset View: both viewers reset in the browser, and the page's own state with them.
        with patch.object(ui, 'run_javascript') as run_js:
            _click(user, _action_button(user, 'zoom-reset'))
        sent = ' '.join(str(c.args[0]) for c in run_js.call_args_list)
        assert 'v.reset()' in sent
        _rebuild(user)
        await _wait_until(lambda: 'scale(1.0)' in _image_markup(user))
        assert 'rotate(0deg)' in _image_markup(user)
        assert _zoom_labels(user) == ['100%']

    _run_browse(OPEN_PAGE, driver)


def test_the_fullscreen_editor_toolbar_zooms_in_the_browser_too():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [edit] = _button(user, 'Edit')
        _click(user, edit)
        await _wait_until(lambda: 'Submit' in _texts(user))
        _click(user, _button_with_tooltip(user, 'Fullscreen Edit'))
        await _wait_until(lambda: _texts(user).count('Submit') >= 2)
        zoom_in = [b for b in _buttons(user) if b.props.get('aria-label') == 'Zoom in']
        assert len(zoom_in) == 2, 'the main toolbar and the fullscreen editor'
        for button in zoom_in:
            [listener] = _click_listeners(button)
            assert listener.handler is None and "browseZoom('in')" in listener.js_handler
        assert _zoom_labels(user) == ['100%', '100%']
        # The editor's image is drawn with the page's level and the shared viewer takes it over.
        found = [e.content for e in _all_elements(user) if isinstance(e, ui.html) and 'fs-zoomable-image' in e.content]
        assert found and 'scale(1.0)' in found[0]

    _run_browse(OPEN_PAGE, driver)


def test_the_page_wires_the_shared_viewer_once():
    """The browser side this file cannot run: the script tag, the event bridge
    and the fullscreen editor's viewer are in the page's head and its JS."""
    styles = browse.VIEWER_STYLES
    assert '/static/manuscript_viewer.js?v=' in styles and '__VIEWER_JS_VERSION__' not in styles
    assert "emitEvent('browse_zoom', { scale: viewer.state.scale, seq: viewer.reportSeq || 0 })" in styles
    assert 'onChange: window.browseZoomChanged' in styles
    assert "if (action === 'in') viewer.zoomIn();" in styles
