# SEED-017 — Joins-Lab / Compare viewer: Rotate + Fullscreen (audit #10)

> Source: 2026-06-23 product-quality audit, finding **#10** (CONFIRMED, MEDIUM).
> Register: `.planning/audit-2026-06-23-product-quality/MASTER.md`.
> Decision gate (ANSWERED 2026-06-23): *"ONLY #10 — add Rotate + Fullscreen to the
> Lab/Compare viewer. NOT the reset/fit icon (#6), NOT desktop sorting (#18), NOT the
> puzzle-toolbar icons (#17), NOT the others."* → single-item viewer-control parity seed.

## Problem (finding #10)

The web **Joins-Lab anchor pane** and the **Compare modal** both render images via
`web/components/anchor_viewer.py::AnchorViewer`, whose toolbar has **zoom out / % / zoom in /
reset only** (anchor_viewer.py:611-636). Two sibling viewers in the same codebase already offer
rotate + fullscreen:

- `web/pages/browse.py:4022-4034` — rotate-left, rotation slider, rotate-right, reset, fullscreen.
- `desktop/viewers.py:682-726` — desktop ResultDialog/Browse viewer: rotate + fullscreen.

So a scholar inspecting a fragment in the Lab/Compare can zoom but **cannot rotate** a
sideways scan or go **fullscreen** — capabilities they have everywhere else.

## Scope (precisely #10 — web only)

Bring `AnchorViewer` to parity by adding **Rotate Left**, **Rotate Right**, and **Fullscreen**
to its toolbar. This automatically reaches BOTH consumers:
- the main Joins-Lab anchor pane (`web/pages/joins_lab.py`), and
- the Compare modal's two panes (`web/components/compare_modal.py`, which builds `AnchorViewer`).

**Scope expanded after UAT (2026-06-25, user request):** desktop Joins Lab is now IN scope too.
- **Web reset icon:** switched `fit_screen` → `/browse`'s `restart_alt` "Reset View" (#6 taste choice
  the user explicitly asked for).
- **Web rotate-left:** signed accumulation (-90), not `(x-90)%360`=270, so the CSS transition animates
  90° left instead of a 270° clockwise spin (UAT: "270 right seems odd").
- **Desktop** (`desktop/join_workbench.py`): added Rotate Left / Rotate Right / Reset / Fullscreen to
  BOTH the main workbench anchor pane AND the CompareDialog panes, using the same controls as the
  ResultDialog viewer (glyphs ↺ ↻ ↩ ⛶) — NOT the brightness/contrast/gamma sliders. Rotation via a new
  `_rotated_pixmap` helper (QTransform on the cached pixmap); Fullscreen reuses
  `desktop.viewers.FullscreenImageWindow` (the ResultDialog fullscreen window).

**Still out of scope (logged):** the rotation **slider** browse.py carries — buttons match the desktop
viewer and are the minimal "Rotate" parity; a slider adds toolbar width + re-render sync cost for little gain.

## Why the JS layer is already ready

`web/static/manuscript_viewer.js::createManuscriptViewer` already exposes `update(scale, rotation)`,
`rotateLeft/rotateRight`, `state.rotation`, and `reset()` (which zeroes rotation). AnchorViewer's
per-instance registry (`window.__msViewers[vid]`, SEED-010) already drives `mv.update(zoom, rotation)`.
So the only missing pieces are **Python-side rotation state + toolbar buttons + a fullscreen toggle**.

## Plan (all in `web/components/anchor_viewer.py`)

1. **State:** add `self._rotation: int = 0` in `__init__`.
2. **`_apply_transform()`** (rename of `_apply_zoom`): push BOTH `self._zoom` and `self._rotation`
   to `mv.update(zoom, rotation)`; the no-`mv` fallback transform must also include
   `rotate({rotation}deg)` (previously dropped rotation). Callers: `zoom_in/out/reset`, the new
   `rotate_*`.
3. **`rotate_left()` / `rotate_right()`:** `self._rotation = (self._rotation ∓ 90) % 360` →
   `_apply_transform()`.
4. **`zoom_reset()`:** also set `self._rotation = 0` (JS `mv.reset()` already zeroes it; sync Python).
5. **`update_content(direction != 0)`:** reset `self._rotation = 0` alongside the existing
   `self._zoom = 1.0` so rotation clears on folio change (matches the re-rendered `<img>` which
   carries no rotation).
6. **`toggle_fullscreen()`:** native Fullscreen API on this viewer's `.anchor-image-pane` wrapper
   (image + controls bar). Native fullscreen **escapes the Compare dialog's stacking/transform
   context** (a `position:fixed` overlay would be trapped inside the maximized `ui.dialog`) and ESC
   exits natively. Transcription stays out of fullscreen (full-bleed image, like /browse).
7. **Markup:** wrap `image_container` + the controls bar in
   `ui.element('div').classes('anchor-image-pane')` so the fullscreen target carries both. Existing
   CSS (`.anchor-viewer-container .image-container`, `.anchor-controls-bar`) is descendant/class-based
   → unaffected by the extra wrapper.
8. **CSS** in `_VIEWER_HEAD`: `.anchor-image-pane:fullscreen { … }` + `:fullscreen .image-container`
   override (fill viewport, square corners) + `:fullscreen .anchor-controls-bar` (no rounded corners).
9. **Buttons:** add `rotate_left`, `rotate_right`, `fullscreen` Material icons to the toolbar's right
   group, same `flat round dense` + `aria-label` + tooltip pattern as the zoom buttons. Strings
   `tr("Rotate left")`, `tr("Rotate right")`, `tr("Fullscreen")` — all already in
   `genizah_translations.TRANSLATIONS` (HE: סובב שמאלה / סובב ימינה / מסך מלא), no English leak.

## HIGH-2 invariant (must hold)

No `handleImageError`, no `fetchFlIdsFromManifest`, no `iiif.nli.org.il` introduced. The fullscreen
JS only toggles `requestFullscreen()/exitFullscreen()` on a viewer-scoped DOM node.

## Tests (`tests/test_anchor_viewer.py`)

- rotation arithmetic: initial 0; rotate_right → 90; rotate_left → 270; 4×right wraps to 0.
- `zoom_reset` zeroes rotation; folio-change (`update_content` direction≠0) zeroes rotation.
- source/`_VIEWER_HEAD` assertions: `rotate_left`/`rotate_right`/`fullscreen` icons present;
  `.anchor-image-pane` wrapper present; `:fullscreen` CSS present; HIGH-2 still holds
  (`_VIEWER_HEAD` has no `handleImageError` / `iiif.nli.org.il`).

## Verification

Targeted suite (`test_anchor_viewer.py`, `test_compare_modal.py`) + ruff + Codex code-review of the
diff, then PR. Manual: live Joins-Lab anchor pane + Compare modal — rotate ±90 works on both panes
independently, fullscreen on each pane escapes the modal, ESC exits.
