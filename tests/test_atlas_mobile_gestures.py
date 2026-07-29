# -*- coding: utf-8 -*-
"""Atlas mobile-gesture + height-cap guards (2026-07-29).

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

Both the geometry cap and the gesture gate hang off ONE signal — the
touch-primary media query ``(hover: none) and (pointer: coarse)``. That is the
load-bearing decision here, because the first cut of the fix got the signal wrong
twice and code review caught both:

* **Height-gated** (a bare inline ``min(720px, 60vh)``) silently shrank DESKTOP:
  ``60vh`` < 720px for any viewport under 1200px tall, so 1920x1080 rendered a
  648px atlas and a 1366x768 laptop a 461px one — i.e. nearly every desktop,
  while the change claimed desktop was untouched.
* **Width-gated** (``state.narrow``, canvas width <= 640px) missed LANDSCAPE
  phones and tablets — the worst geometry of all, a 720px box over a ~390px-tall
  viewport — leaving them in the exact trap being fixed.

The tests below therefore pin the SIGNAL, not just the behaviour, so neither
wrong gate can creep back in.

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

_TOUCH_QUERY = "(hover: none) and (pointer: coarse)"


# ---------------------------------------------------------------------------
# 1. Gesture ownership must be conditional, and gated on TOUCH — not width.
# ---------------------------------------------------------------------------

def test_touch_action_is_applied_through_a_single_helper():
    assert "function applyTouchAction()" in _JS_SRC, (
        "gesture ownership must live in one helper so every call site "
        "(init / fullscreen / resize) stays in sync"
    )


def test_touch_action_is_never_set_unconditionally_to_none():
    # The pre-fix line was a bare `canvas.style.touchAction = 'none';` at attach
    # time. Any unconditional assignment re-creates the scroll trap.
    assert "canvas.style.touchAction = 'none'" not in _JS_SRC, (
        "touch-action must not be hard-set to 'none' — touch devices outside "
        "fullscreen need 'pan-y' so a one-finger swipe scrolls the page"
    )


def test_gesture_gate_uses_the_touch_primary_media_query():
    assert "function _isTouchPrimary()" in _JS_SRC, (
        "the gesture gate must derive from a touch-primary probe"
    )
    assert _TOUCH_QUERY in _JS_SRC, (
        f"the gesture gate must use the standard touch-primary query {_TOUCH_QUERY!r}"
    )


def test_gesture_gate_does_not_regress_to_the_narrow_width_proxy():
    # REGRESSION GUARD (review finding): state.narrow is the focus-panel
    # breakpoint (canvas width <= 640px). Using it as the touch proxy left
    # landscape phones and tablets trapped.
    body = _JS_SRC.split("function applyTouchAction()", 1)[1][:400]
    assert "state.narrow" not in body, (
        "applyTouchAction must NOT gate on state.narrow — that is a width "
        "breakpoint, so landscape phones and tablets keep the scroll trap"
    )
    assert "_isTouchPrimary()" in body, "the gate must consult the touch-primary probe"
    assert "_isFs()" in body, "fullscreen must re-take full gesture ownership"
    assert "'pan-y'" in body, "touch + non-fullscreen must yield pan-y"
    assert "'none'" in body, "pointer devices / fullscreen must yield none"


def test_touch_action_resyncs_on_fullscreen_and_resize():
    # Entering fullscreen must restore 'none' (no page to scroll); attaching a
    # trackpad to a tablet can flip `pointer: coarse` and comes with a resize.
    fs_block = _JS_SRC.split("function _onFsChange()", 1)[1][:700]
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
# 3. The height cap must be touch-gated — and must not touch desktop.
# ---------------------------------------------------------------------------

def test_inline_reservation_stays_a_flat_pixel_height():
    # REGRESSION GUARD (review finding): a viewport-relative INLINE height applies
    # to every device, shrinking desktop layouts under 1200px tall. The cap must
    # come from the touch-gated stylesheet rule instead.
    css = atlas_page._ATLAS_CANVAS_HEIGHT_CSS
    assert css == f"height: {atlas_page._ATLAS_CANVAS_HEIGHT_PX}px;", (
        f"the inline reservation must stay a flat pixel height so pointer "
        f"layouts are untouched; got {css!r}"
    )
    assert "vh" not in css and "dvh" not in css, (
        "no viewport unit inline — it would shrink desktop too (1920x1080 -> "
        "648px, 1366x768 -> 461px)"
    )


def test_height_cap_is_gated_on_touch_primary_not_on_width_or_height():
    style = atlas_page._ATLAS_HEIGHT_CAP_STYLE
    assert _TOUCH_QUERY in style, (
        f"the height cap must be gated on {_TOUCH_QUERY!r} — the population with "
        f"the swipe-trap problem"
    )
    assert "max-width" not in style and "max-height" not in style, (
        "the cap must NOT be gated on a width/height breakpoint: width misses "
        "landscape phones and tablets, height shrinks short desktop windows"
    )
    assert f"min({atlas_page._ATLAS_CANVAS_HEIGHT_PX}px" in style, (
        "the cap must never exceed the desktop reservation"
    )
    assert "vh)" in style and "dvh" not in style, (
        "cap against the viewport with plain vh — dvh is unparseable on "
        "iOS < 15.4, which drops the declaration and collapses the box"
    )
    assert "!important" in style, (
        "the rule must beat the inline reservation, which otherwise always wins"
    )


def test_height_cap_uses_a_class_selector_so_fullscreen_still_wins():
    # atlas_decode.js's `.atlas-fs-box:fullscreen` rules (0-2-0 / 0-2-1, also
    # !important) must out-specify the cap, or entering fullscreen on a phone
    # would stay capped instead of filling the screen. An #id selector (1-0-0)
    # would invert that and break the escape hatch.
    style = atlas_page._ATLAS_HEIGHT_CAP_STYLE
    assert f".{atlas_page._ATLAS_HEIGHT_CAP_CLASS}" in style, (
        "the cap must use its class selector"
    )
    assert "#atlas-canvas" not in style, (
        "an #id selector (1-0-0) would out-specify the :fullscreen rules and "
        "break fullscreen on phones — keep the cap at class specificity"
    )


def test_both_box_and_canvas_carry_the_cap_class():
    assert _PAGE_SRC.count("_ATLAS_HEIGHT_CAP_CLASS") >= 3, (
        "the reserved box and the canvas must both carry the cap class "
        "(definition + 2 usages), or they can disagree on height"
    )


def test_cap_style_is_injected_into_head():
    assert "ui.add_head_html(_ATLAS_HEIGHT_CAP_STYLE)" in _PAGE_SRC, (
        "the cap must be in <head> so it applies at first paint and the CLS "
        "reservation still holds"
    )


def test_fullscreen_css_still_overrides_the_reserved_height():
    assert "height:100vh !important" in _JS_SRC, (
        "the :fullscreen rule must force the box to fill the screen over the "
        "inline reserved height"
    )
    assert ":fullscreen>canvas{height:100% !important;}" in _JS_SRC, (
        "the :fullscreen rule must force the canvas to fill the box"
    )
