# -*- coding: utf-8 -*-
"""Render-smoke: free rotation on /browse and in Quick View (2026-11-18).

The /browse rotation slider sat in the image toolbar and showed only from 1440px
(2026-10-22), so a laptop or a phone had the 90° buttons alone. It now sits in the
image-adjustment row behind ``tune`` at every width, runs -180..180, and the 90°
buttons, the other slider and Reset Image all keep it showing the image's angle.
Quick View gets the same slider. Harnesses:
``test_browse_button_icons_render_smoke`` and
``test_quick_view_without_automatic_text_render_smoke``.
"""
from __future__ import annotations

import time
from unittest.mock import patch

from nicegui import ui

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
from tests.render_smoke.test_browse_zoom_render_smoke import _image_markup, _rebuild, _tell_the_page
from tests.render_smoke import test_quick_view_without_automatic_text_render_smoke as qv
from web.pages import browse, search_results


def _sliders(user, cls):
    return [e for e in _all_elements(user) if isinstance(e, ui.slider) and cls in e.classes]


def _ancestor_classes(element):
    found, parent = set(), element.parent_slot.parent if element.parent_slot else None
    while parent is not None:
        found.update(parent.classes)
        parent = parent.parent_slot.parent if parent.parent_slot else None
    return found


def _set(user, slider, value):
    """The reader moves the slider (the client's update:model-value reaching the server)."""
    with user._client:
        slider.value = value


def _sent(run_js):
    return ' '.join(str(c.args[0]) for c in run_js.call_args_list)


def _angle_labels(user):
    return [e.text for e in _all_elements(user) if isinstance(e, ui.label) and 'browse-rotate-angle' in e.classes]


# --- /browse ---------------------------------------------------------------------------------

def test_the_slider_sits_behind_tune_at_every_width():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [slider] = _sliders(user, 'browse-rotate-slider')
        assert (slider.props['min'], slider.props['max'], slider.props['step']) == (-180, 180, 1)
        assert slider.value == 0 and slider.props.get('aria-label') == 'Rotation'
        ancestors = _ancestor_classes(slider)
        assert 'browse-image-adjust' in ancestors and 'browse-image-tools' not in ancestors
        assert _angle_labels(user) == ['0°']
        # It turns the image in the browser while it moves, before the server hears it.
        # (NiceGUI stores `.on('update:model-value')` as `update:modelValue`.)
        live = [l for l in slider._event_listeners.values()
                if l.type in ('update:model-value', 'update:modelValue') and 'setRotation' in (l.js_handler or '')]
        assert live and 'setRotation(angle)' in live[0].js_handler

    _run_browse(OPEN_PAGE, driver)

    # No width hides it any more (it was `display: none` below 1440px and on a phone).
    src = open(browse.__file__, encoding='utf-8').read()
    css = src[src.index('<style>'):src.index('</style>', src.index('<style>'))]
    for line in css.splitlines():
        assert not ('browse-rotate-slider' in line and 'display: none' in line), line


def test_the_page_shortcuts_leave_a_focused_slider_its_arrow_keys():
    """Codex on #45: an arrow key on the rotation slider must not also turn the folio. Quasar
    already stops the slider's arrow keydowns (measured in a browser, 2026-11-18: none reached
    a document listener); the page's shortcut handler also returns for a slider target, so the
    rule does not rest on Quasar's internals. Focus lands on the slider's inner track, hence
    `closest`."""
    src = open(browse.__file__, encoding='utf-8').read()
    handler = src[src.index("document.addEventListener('keydown', function(e) {"):]
    handler = handler[:handler.index('</script>')]
    # Only the arrows: + - 0 F are not the slider's, so they still zoom, reset and go full
    # screen while a slider has focus (Codex on #45, round 2).
    guard = ("if ((e.key === 'ArrowLeft' || e.key === 'ArrowRight')\n"
             "                    && e.target.closest && e.target.closest('.q-slider, [role=\"slider\"]')) return;")
    assert guard in handler.replace('\r\n', '\n')
    assert handler.index("e.target.closest('.q-slider") < handler.index("if (e.key === 'ArrowLeft') action"), \
        'the guard comes before the arrows'


def test_moving_the_slider_turns_the_image_and_a_rebuilt_page_keeps_the_angle():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Show Metadata' in texts)
        _tell_the_page(user, {'scale': 2.25})
        [slider] = _sliders(user, 'browse-rotate-slider')
        with patch.object(ui, 'run_javascript') as run_js:
            _set(user, slider, -7)
        sent = _sent(run_js)
        assert 'setRotation(-7)' in sent and 'scale' not in sent, sent
        assert _angle_labels(user) == ['-7°']
        _rebuild(user)
        await _wait_until(lambda: 'rotate(-7deg)' in _image_markup(user))
        assert 'scale(2.25)' in _image_markup(user), 'the rotation left the level alone'
        [slider] = _sliders(user, 'browse-rotate-slider')
        assert slider.value == -7

    _run_browse(OPEN_PAGE, driver)


def test_the_ninety_degree_buttons_move_the_slider_with_one_push_each():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [slider] = _sliders(user, 'browse-rotate-slider')
        _set(user, slider, 10)
        for tooltip, angle in (('Rotate Left', -80), ('Rotate Left', -170), ('Rotate Left', 100),
                               ('Rotate Right', -170)):
            with patch.object(ui, 'run_javascript') as run_js:
                _click(user, _button_with_tooltip(user, tooltip))
            assert slider.value == angle, (tooltip, slider.value)
            assert _sent(run_js).count('setRotation(') == 1, _sent(run_js)
            assert f'setRotation({angle})' in _sent(run_js)

    _run_browse(OPEN_PAGE, driver)


def test_reset_image_zeroes_the_rotation_too():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [slider] = _sliders(user, 'browse-rotate-slider')
        _set(user, slider, 25)
        with patch.object(ui, 'run_javascript') as run_js:
            _click(user, _button_with_tooltip(user, 'Reset Image'))
        assert slider.value == 0
        sent = _sent(run_js)
        assert 'resetAdjustments()' in sent and 'setRotation(0)' in sent and 'scale' not in sent, sent

    _run_browse(OPEN_PAGE, driver)


def test_the_fullscreen_editor_has_the_slider_and_the_two_stay_together():
    async def driver(user):
        await _open_browse(user, lambda texts: 'Edit' in texts)
        [edit] = _button(user, 'Edit')
        _click(user, edit)
        await _wait_until(lambda: 'Submit' in _texts(user))
        _click(user, _button_with_tooltip(user, 'Fullscreen Edit'))
        await _wait_until(lambda: _texts(user).count('Submit') >= 2)
        sliders = _sliders(user, 'browse-rotate-slider')
        assert len(sliders) == 2, 'the image panel and the fullscreen editor'
        main, editor = sliders
        _set(user, editor, 33)
        assert main.value == 33
        rotate_left = [b for b in _buttons(user) if b.props.get('icon') == 'rotate_left']
        assert len(rotate_left) == 2
        _click(user, rotate_left[1])
        assert (main.value, editor.value) == (-57, -57)
        found = [e.content for e in _all_elements(user) if isinstance(e, ui.html) and 'fs-zoomable-image' in e.content]
        assert found

    _run_browse(OPEN_PAGE, driver)


# --- Quick View ------------------------------------------------------------------------------

def _qv_slider(user):
    found = [e for e in qv._elements(user) if isinstance(e, ui.slider) and 'adv-rotate-slider' in e.classes]
    return found[0] if found else None


def test_quick_view_has_the_slider_and_its_buttons_go_through_it():
    async def driver(user):
        with patch('shared.nli_circuit_breaker.is_open', return_value=False):
            await user.open(f'{qv.PAGE}?case=hit')
            await qv._wait_until(lambda: _qv_slider(user) is not None)
            slider = _qv_slider(user)
            assert (slider.props['min'], slider.props['max'], slider.value) == (-180, 180, 0)
            assert 'adv-image-adjust' in _ancestor_classes(slider)

            with patch.object(ui, 'run_javascript') as run_js:
                [left] = qv._button(user, 'Rotate Left')
                qv._click(user, left)
            assert slider.value == -90 and 'advViewer.setRotation(-90)' in _sent(run_js)

            with user._client:
                slider.value = 4
            with patch.object(ui, 'run_javascript') as run_js:
                [right] = qv._button(user, 'Rotate Right')
                qv._click(user, right)
            assert slider.value == 94 and 'advViewer.setRotation(94)' in _sent(run_js)

            with patch.object(ui, 'run_javascript') as run_js:
                [reset] = qv._button(user, 'Reset View')
                qv._click(user, reset)
            assert slider.value == 0
            assert 'advViewer.reset()' in _sent(run_js)

    qv._run(driver)


def _qv_image(user):
    found = [e.content for e in qv._elements(user) if isinstance(e, ui.html) and 'adv-zoomable-image' in (e.content or '')]
    return found[0] if found else ''


def test_quick_view_keeps_the_angle_through_its_redraws_and_resets_it_for_another_result():
    """Codex on #45: the dialog redraws its image and slider for full screen, edit mode, a
    folio or a late page count; each redraw drew them upright, dropping the reader's angle."""
    async def driver(user):
        with patch('shared.nli_circuit_breaker.is_open', return_value=False):
            await user.open(f'{qv.PAGE}?case=hit,no-edition')
            await qv._wait_until(lambda: _qv_slider(user) is not None)
            with user._client:
                _qv_slider(user).value = 30

            [fullscreen] = qv._button(user, 'Fullscreen')
            qv._click(user, fullscreen)
            await qv._wait_until(lambda: _qv_slider(user) is None and 'adv-zoomable-image' in qv._html(user))
            assert 'rotate(30deg)' in _qv_image(user), 'the full-screen redraw kept the angle'
            # Its own rotate button keeps the dialog's angle too (it has no slider).
            with patch.object(ui, 'run_javascript') as run_js:
                [right] = qv._button(user, 'Rotate Right')
                qv._click(user, right)
            assert 'advViewer.setRotation(120)' in _sent(run_js)

            [back] = qv._button(user, 'Exit Fullscreen')
            qv._click(user, back)
            await qv._wait_until(lambda: _qv_slider(user) is not None)
            assert _qv_slider(user).value == 120
            assert 'rotate(120deg)' in _qv_image(user)

            # Another result starts upright.
            [nxt] = qv._button(user, 'Next')
            qv._click(user, nxt)
            await qv._wait_until(lambda: _qv_slider(user) is not None and 'rotate(0deg)' in _qv_image(user))
            assert _qv_slider(user).value == 0

    qv._run(driver)


def _slow_external(sys_id):
    time.sleep(1.0)
    return {'sys_id': sys_id, 'external_provider': '', 'attribution': 'Cambridge University Library',
            'cambridge_images': [{'url': 'https://cudl.lib.cam.ac.uk/iiif/MS-TS-NS-00243-00146/canvas/1'},
                                 {'url': 'https://cudl.lib.cam.ac.uk/iiif/MS-TS-NS-00243-00146/canvas/2'}],
            'cambridge_alignment': {'verdict': 'aligned'}}


def test_a_library_image_that_comes_late_keeps_the_readers_angle():
    async def driver(user):
        with patch.object(search_results, 'quick_view_external_images', _slow_external), \
                patch.object(search_results, 'QUICK_VIEW_EXTERNAL_WAIT_S', 0.2), \
                patch('shared.nli_circuit_breaker.is_open', return_value=False):
            await user.open(f'{qv.PAGE}?case=hit')
            await qv._wait_until(lambda: _qv_slider(user) is not None and 'nli_image_by_sysid' in qv._html(user))
            assert 'rotate(0deg)' in qv._html(user)
            with user._client:
                _qv_slider(user).value = 12
            await qv._wait_until(lambda: 'cambridge_image' in qv._html(user))
            assert 'cambridge_image' in qv._html(user)
            assert 'rotate(12deg)' in qv._html(user)

    qv._run(driver)
