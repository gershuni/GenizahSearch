# -*- coding: utf-8 -*-
"""The /browse image zoom: multiplicative steps, clamps, and client-side clicks (2026-11-03).

PostHog's 30 days to 2026-11-03 held 50 rage clicks by 19 readers on /browse's
+ button (9 by 8 on -, 4 by 3 on reset), the only real rage-click target on the
site: each click added a linear 0.25 and went through a server round-trip. Now
a click multiplies the level by ZOOM_FACTOR, in the browser, and the page hears
the level afterwards. The arithmetic lives twice -- ``web/pages/browse_state.py``
for the page's state, ``web/static/manuscript_viewer.js`` for the browser -- and
this file keeps the two in step by reading the sources, as
``tests/test_phone_layout_rules.py`` does. ``tests/test_manuscript_viewer_js.py``
runs the viewer's own arithmetic under node; the render-smoke counterpart is
``tests/render_smoke/test_browse_zoom_render_smoke.py``.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from web.pages.browse_state import (
    DOUBLE_CLICK_ZOOM, ZOOM_FACTOR, ZOOM_MAX, ZOOM_MIN, clamp_zoom, zoom_percent, zoom_step,
)

REPO = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (REPO / rel).read_text(encoding='utf-8')


def test_a_click_multiplies_the_level():
    assert zoom_step(1.0, +1) == pytest.approx(1.5)
    assert zoom_step(1.5, +1) == pytest.approx(2.25)
    assert zoom_step(1.0, -1) == pytest.approx(1 / 1.5)
    # In and back out lands where it started, at every level in the range.
    for level in (0.25, 0.5, 1.0, 1.5, 3.0, 5.0):
        assert zoom_step(zoom_step(level, +1), -1) == pytest.approx(level)


def test_five_clicks_reach_the_top_of_the_range():
    """The old 0.25 steps needed 27 clicks from 100% to 759%; now five, and the sixth stops at the cap."""
    levels, level = [], 1.0
    for _ in range(6):
        level = zoom_step(level, +1)
        levels.append(level)
    assert levels[:5] == pytest.approx([1.5, 2.25, 3.375, 5.0625, 7.59375])
    assert levels[5] == ZOOM_MAX == 8.0


def test_the_range_is_closed_at_both_ends():
    assert ZOOM_MIN == 0.25
    assert zoom_step(ZOOM_MAX, +1) == ZOOM_MAX
    assert zoom_step(ZOOM_MIN, -1) == ZOOM_MIN
    assert zoom_step(0.3, -1) == ZOOM_MIN
    assert clamp_zoom(50) == ZOOM_MAX
    assert clamp_zoom(0) == ZOOM_MIN
    assert clamp_zoom(-3) == ZOOM_MIN
    assert clamp_zoom('2') == 2.0


@pytest.mark.parametrize('bad', ['abc', None, float('nan'), float('inf'), float('-inf'), [1.5]])
def test_what_is_not_a_finite_number_is_the_fit_level(bad):
    """The level arrives from the browser; a broken payload must never leave a NaN in the page's state."""
    assert clamp_zoom(bad) == 1.0


def test_the_toolbar_figure_rounds():
    assert zoom_percent(1.0) == 100
    assert zoom_percent(1.5) == 150
    assert zoom_percent(3.375) == 338
    assert zoom_percent(1 / 1.5) == 67
    assert zoom_percent(7.59375) == 759


def _js_number(js: str, name: str) -> float:
    m = re.search(r'^const %s = ([\d.]+);$' % name, js, re.M)
    assert m, f'{name} is not a top-level const in manuscript_viewer.js'
    return float(m.group(1))


def test_the_viewer_js_carries_the_same_numbers():
    js = _read('web/static/manuscript_viewer.js')
    assert _js_number(js, 'ZOOM_FACTOR') == ZOOM_FACTOR
    assert _js_number(js, 'MIN_ZOOM') == ZOOM_MIN
    assert _js_number(js, 'MAX_ZOOM') == ZOOM_MAX
    assert _js_number(js, 'DOUBLE_CLICK_ZOOM') == DOUBLE_CLICK_ZOOM


def test_the_viewer_steps_multiply_and_anchor_at_the_pointer():
    js = _read('web/static/manuscript_viewer.js')
    # A click multiplies, and is reported at once (the wheel and the pinch wait for the pause).
    assert 'zoomIn: function() { this.zoomBy(ZOOM_FACTOR, undefined, undefined, true); }' in js
    assert 'zoomOut: function() { this.zoomBy(1 / ZOOM_FACTOR, undefined, undefined, true); }' in js
    assert 'const maxZoom = options.maxZoom || MAX_ZOOM;' in js
    # No linear step is left anywhere: the wheel, the pinch and the buttons all clamp through clampZoom.
    assert 'zoomStep' not in js
    assert not re.search(r'this\.state\.scale [+-] (?:zoomStep|delta|0\.\d+)', js), 'a linear step is back'
    wheel = js[js.index('onWheel: function'):js.index('_changed: function')]
    assert 'this.zoomBy(factor, e.clientX, e.clientY)' in wheel
    assert 'Math.max(1 / ZOOM_FACTOR, Math.min(ZOOM_FACTOR, factor))' in wheel, 'one wheel event is at most one click'
    assert 'this.el.ondblclick = this.onDoubleClick.bind(this);' in js
    assert "this.el.style.touchAction = 'manipulation';" in js, "the browser's own double-tap zoom must stay out of it"
    toggle = js[js.index('toggleZoomAt: function'):js.index('onDoubleClick: function')]
    assert 'if (this.state.scale > 1.01) {' in toggle and 'this.fit();' in toggle
    assert 'this.zoomBy(DOUBLE_CLICK_ZOOM / this.state.scale, px, py, true);' in toggle
    # A double-tap goes through the same toggle, and keeps the click pair from the browser.
    end = js[js.index('onTouchEnd: function'):js.index('applyTransform: function')]
    assert 'this.toggleZoomAt(tap.x, tap.y);' in end and 'e.preventDefault();' in end
    assert "this.el.addEventListener('touchend', this._touch.end, { passive: false });" in js


def test_the_browse_buttons_zoom_in_the_browser_and_keep_their_hooks():
    browse = _read('web/pages/browse.py')
    js = _read('web/static/manuscript_viewer.js')
    # Client-side clicks, on the buttons the keyboard handler finds by data-action.
    zoom_in = [line for line in browse.splitlines() if 'data-action="zoom-in"' in line]
    zoom_out = [line for line in browse.splitlines() if 'data-action="zoom-out"' in line]
    assert len(zoom_in) == 2 and len(zoom_out) == 2, 'the button and the key that clicks it'
    [button_in] = [line for line in zoom_in if 'ui.button' in line]
    [button_out] = [line for line in zoom_out if 'ui.button' in line]
    assert ".on('click', js_handler=ZOOM_IN_JS)" in button_in and 'on_click=' not in button_in
    assert ".on('click', js_handler=ZOOM_OUT_JS)" in button_out and 'on_click=' not in button_out
    assert "document.querySelector('[data-action=\"zoom-in\"]')?.click();" in browse
    assert "document.querySelector('[data-action=\"zoom-out\"]')?.click();" in browse
    assert 'on_click=zoom_in' not in browse and 'on_click=zoom_out' not in browse
    # Both toolbars (the main one and the fullscreen editor's) use the same handlers.
    assert browse.count("js_handler=ZOOM_IN_JS") == 2
    assert browse.count("js_handler=ZOOM_OUT_JS") == 2
    assert browse.count("js_handler=ZOOM_RESET_JS") == 2
    # The page hears the level through one event, and a rotation never pushes a scale.
    assert "ui.on('browse_zoom', _zoom_changed)" in browse
    assert "emitEvent('browse_zoom', { scale: viewer.state.scale, seq: viewer.reportSeq || 0 })" in browse
    # Every image the page draws says which report it followed, so the viewer
    # can tell a rebuild drawn before its latest level (Codex on #471).
    assert browse.count('data-zoom-seq="{int(state.zoom_seq)}"') == 2
    assert 'drawnSeq < (this.reportSeq || 0)' in js
    transform = browse[browse.index('def update_image_transform'):browse.index('async def _submit_correction_now')]
    assert 'setRotation(' in transform and 'scale' not in transform.split('"""')[2]
    # A page change starts the viewer from the whole page again.
    assert 'window.manuscriptViewer.fit();' in browse
    # The fullscreen editor's image is the shared viewer too, not an inline copy.
    assert "imageSelector: '#fs-zoomable-image'" in browse and 'onWheel: function' not in browse
    # The <script> URL is stamped, so a page built after a deploy never runs an hour-old cached viewer.
    assert 'manuscript_viewer.js?v=__VIEWER_JS_VERSION__' in browse
    assert "'''.replace('__VIEWER_JS_VERSION__', _viewer_js_version())" in browse
