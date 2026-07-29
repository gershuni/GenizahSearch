# -*- coding: utf-8 -*-
"""Atlas mobile-gesture guards (2026-07-29).

Mobile is the atlas's PRIMARY audience — at the time of writing, ~68% of /atlas
pageviews and 972 of 1,447 people, driven by the homepage/Facebook announcement —
and mobile users complained about the page. Two defects caused it:

1. **Scroll trap.** ``web/pages/atlas.py`` reserved a FLAT 720px canvas on every
   viewport. On a phone that is taller than the visible area, and because the
   renderer sets ``touch-action`` on the canvas to own pan/pinch, the canvas
   swallowed every vertical swipe — users could not scroll past the map.
2. **Ghost taps.** Handing vertical panning back to the browser (``pan-y``) means
   the browser CLAIMS a one-finger swipe and fires ``pointercancel``, often
   before the finger travelled the 2px that sets ``dragMoved``. Treating that as
   a tap would focus a cluster under the user's finger on every page scroll.

These are static/structural guards over the renderer + page source: the renderer
is browser-only (Canvas 2D, PointerEvents, Fullscreen API) with no JS test
harness in this repo, so the invariants are pinned at the source level where a
regression would otherwise be invisible until it reached real phones.
"""

import pathlib

import web.pages.atlas as atlas_page

_JS = (
    pathlib.Path(atlas_page.__file__).parent.parent / "static" / "js" / "atlas_decode.js"
)
_JS_SRC = _JS.read_text(encoding="utf-8")
_PAGE_SRC = pathlib.Path(atlas_page.__file__).read_text(encoding="utf-8")


# ---------------------------------------------------------------------------
# 1. Gesture ownership must be conditional, never unconditionally 'none'.
# ---------------------------------------------------------------------------

def test_touch_action_is_applied_through_a_single_helper():
    assert "function applyTouchAction()" in _JS_SRC, (
        "gesture ownership must live in one helper so every call site "
        "(init / fullscreen / resize) stays in sync"
    )


def test_touch_action_is_never_set_unconditionally_to_none():
    # The pre-fix line was a bare `canvas.style.touchAction = 'none';` at attach
    # time. Any unconditional assignment re-creates the scroll trap on phones.
    assert "canvas.style.touchAction = 'none'" not in _JS_SRC, (
        "touch-action must not be hard-set to 'none' — phones outside fullscreen "
        "need 'pan-y' so a one-finger swipe scrolls the page"
    )


def test_narrow_screens_get_pan_y_and_immersive_gets_none():
    body = _JS_SRC.split("function applyTouchAction()", 1)[1][:600]
    assert "state.narrow" in body, "the gate must consult state.narrow"
    assert "_isFs()" in body, "fullscreen must re-take full gesture ownership"
    assert "'pan-y'" in body, "narrow + non-fullscreen must yield pan-y"
    assert "'none'" in body, "desktop/fullscreen must yield none"


def test_touch_action_resyncs_on_fullscreen_and_resize():
    # Entering fullscreen on a phone must restore 'none' (no page to scroll), and
    # a rotate/resize can cross the narrow threshold in either direction.
    fs_block = _JS_SRC.split("function _onFsChange()", 1)[1][:600]
    assert "applyTouchAction()" in fs_block, (
        "fullscreen enter/exit must re-derive gesture ownership"
    )
    resize_blocks = _JS_SRC.split("addEventListener('resize'")
    assert any("applyTouchAction()" in b[:400] for b in resize_blocks[1:]), (
        "a resize/orientation change must re-derive gesture ownership"
    )


# ---------------------------------------------------------------------------
# 2. A canceled pointer must never be treated as a tap.
# ---------------------------------------------------------------------------

def test_pointercancel_does_not_take_the_tap_path():
    assert "function endPointer(ev, canceled)" in _JS_SRC, (
        "endPointer must accept a `canceled` flag to distinguish pointerup from "
        "pointercancel"
    )
    assert "!dragMoved && !canceled" in _JS_SRC, (
        "the tap branch must exclude canceled pointers — otherwise every "
        "browser-claimed scroll gesture focuses a cluster under the finger"
    )


def test_pointercancel_passes_canceled_true_and_pointerup_false():
    assert "endPointer(ev, true)" in _JS_SRC, "pointercancel must pass canceled=true"
    assert "endPointer(ev, false)" in _JS_SRC, "pointerup must pass canceled=false"


# ---------------------------------------------------------------------------
# 3. The reserved height must cap against the viewport (and stay CLS-safe).
# ---------------------------------------------------------------------------

def test_reserved_height_caps_against_the_viewport():
    css = atlas_page._ATLAS_CANVAS_HEIGHT_CSS
    assert css.startswith("height: min("), (
        f"height must be a min() reservation, not a flat value; got {css!r}"
    )
    assert f"{atlas_page._ATLAS_CANVAS_HEIGHT_PX}px" in css, "desktop cap must survive"
    assert "vh)" in css, "must cap against the viewport so phones can scroll past"


def test_reserved_height_uses_plain_vh_not_dvh():
    # NiceGUI parses .style() into a dict keyed by property name, so a
    # `vh`-then-`dvh` progressive-enhancement PAIR cannot survive (the dict keeps
    # only the last value). Shipping `dvh` alone is unparseable on iOS < 15.4:
    # the declaration is dropped and the reserved box collapses.
    css = atlas_page._ATLAS_CANVAS_HEIGHT_CSS
    assert "dvh" not in css, (
        "dvh has no surviving fallback through NiceGUI's style dict — use plain vh"
    )
    assert css.count("height:") == 1, (
        "exactly one height declaration survives NiceGUI's style dict; emitting "
        "two silently drops the first"
    )


def test_page_no_longer_reserves_a_flat_pixel_height():
    # Both the box and the canvas must go through the shared responsive constant.
    assert "_ATLAS_CANVAS_HEIGHT_PX}px;" not in _PAGE_SRC, (
        "no element may reserve the flat 720px height any more — that is the "
        "mobile scroll trap"
    )
    assert _PAGE_SRC.count("_ATLAS_CANVAS_HEIGHT_CSS") >= 3, (
        "the box and the canvas must both use the shared responsive height "
        "(definition + 2 usages)"
    )


def test_fullscreen_css_still_overrides_the_reserved_height():
    # The inline min() must lose to the :fullscreen rule, or fullscreen on a
    # phone would stay capped at the inline height instead of filling the screen.
    assert "height:100vh !important" in _JS_SRC, (
        "the :fullscreen rule must force the box to fill the screen over the "
        "inline reserved height"
    )
    assert ":fullscreen>canvas{height:100% !important;}" in _JS_SRC, (
        "the :fullscreen rule must force the canvas to fill the box"
    )
