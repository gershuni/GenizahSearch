---
phase: quick-260322-jtk
plan: 01
type: execute
wave: 1
depends_on: []
files_modified:
  - shared/background_removal.py
  - web/api.py
  - web/pages/puzzle.py
autonomous: false
requirements: [CLICK-BG-REMOVE]

must_haves:
  truths:
    - "User can click a point on a canvas fragment to remove connected background from that seed"
    - "Multiple clicks are additive -- each removes another area/color"
    - "User can undo each click-removal step individually via undo button"
    - "Works for any background color (brown, gray, cream, blue) -- not color-specific"
  artifacts:
    - path: "shared/background_removal.py"
      provides: "flood_fill_remove() function -- edge-aware flood fill from seed point"
      exports: ["flood_fill_remove"]
    - path: "web/api.py"
      provides: "POST /api/puzzle_click_remove endpoint accepting image bytes + (x,y) seed"
    - path: "web/pages/puzzle.py"
      provides: "Eraser mode toggle button, JS click handler sending seed to server, undo stack"
  key_links:
    - from: "web/pages/puzzle.py (JS click handler)"
      to: "/api/puzzle_click_remove"
      via: "POST with canvas image bytes + x,y coordinates"
      pattern: "puzzle_click_remove"
    - from: "web/api.py (puzzle_click_remove)"
      to: "shared/background_removal.py"
      via: "flood_fill_remove(image_bytes, x, y, tolerance)"
      pattern: "flood_fill_remove"
    - from: "web/pages/puzzle.py (JS undo)"
      to: "client-side _bgUndoStack array"
      via: "pop previous data URL, replace image src"
      pattern: "_bgUndoStack"
---

<objective>
Add interactive click-to-remove background removal to the puzzle page. User clicks a point on a fragment image, server applies edge-aware flood fill from that seed point to make connected same-color pixels transparent. Multiple clicks are additive. Each step is individually undoable via a client-side undo stack.

Purpose: Current auto bg removal fails on brown/gray backgrounds (BL manuscripts on brown backing, Oxford full-brown scans). Instead of trying to auto-detect every possible background, let the user click what to remove.

Output: New `flood_fill_remove()` in background_removal.py, new API endpoint, eraser mode in puzzle toolbar with undo.
</objective>

<execution_context>
@~/.claude/get-shit-done/workflows/execute-plan.md
@~/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@shared/background_removal.py
@web/api.py (lines 856-935 for puzzle_process pattern)
@web/pages/puzzle.py (lines 32-55 for JS global, 300-447 for _loadImageWithFallbacks, 1795-1810 for _toggleFragmentBg, 3060-3127 for toolbar row 1 buttons, 3288-3370 for toolbar row 2 sliders)
</context>

<tasks>

<task type="auto">
  <name>Task 1: Add flood_fill_remove() to background_removal.py + API endpoint</name>
  <files>shared/background_removal.py, web/api.py</files>
  <action>
**In shared/background_removal.py**, add a new function `flood_fill_remove(image_bytes, seed_x, seed_y, tolerance=32)`:

1. Open image from bytes as RGBA (convert if RGB, preserving existing alpha).
2. Get the color at (seed_x, seed_y) from the RGB channels. This is the target color.
3. Implement a BFS/queue-based flood fill starting from (seed_x, seed_y):
   - Use a visited set (or boolean array same size as image) to track processed pixels.
   - For each pixel, compute color distance (Euclidean RGB distance) from target color.
   - If distance <= tolerance AND pixel is not already transparent (alpha > 0), mark for removal and add 4-connected neighbors to queue.
   - Edge-aware: the tolerance parameter controls how far colors can drift from the seed. A value of 32 is good default for scanning backgrounds.
4. Set alpha=0 for all pixels marked for removal.
5. Apply a 1px feather on removal edges: for each removed pixel that has a non-removed neighbor, set neighbor alpha to min(neighbor_alpha, 128). This prevents harsh jagged edges.
6. Return RGBA PNG bytes.

Use NumPy for the pixel array and a Python deque for BFS. The image is 800px so performance is fine without optimization.

Key details:
- Input image may already be RGBA with some transparent pixels (from prior click-removes or auto bg removal). Preserve existing transparency -- only add new transparency.
- If seed point is already transparent (alpha=0), return image unchanged.
- Clamp seed_x, seed_y to image bounds.

**In web/api.py**, add a new endpoint after the existing `puzzle_process` endpoint (~line 935):

```python
async def puzzle_click_remove(request: Request):
```

Route: POST `/api/puzzle_click_remove` with query params `x` (int), `y` (int), `tolerance` (int, default 32).
Body: image bytes (PNG or JPEG).
No auth token needed (same session, no caching of results -- this is ephemeral per-click).

Implementation:
- Read x, y, tolerance from query params. Clamp tolerance to 10-80.
- Read body bytes (max 10MB, same as puzzle_process).
- Validate image (PIL open+verify, same pattern as puzzle_process).
- Call `flood_fill_remove(raw_bytes, x, y, tolerance)`.
- Return Response with RGBA PNG bytes, content-type image/png.
- Apply same rate limiting as puzzle_process (`_check_puzzle_rate_limit`).

Register the route using `@app.post('/api/puzzle_click_remove')` decorator inside the `init_api_routes()` function, following the same pattern as `puzzle_process` at line 855.
  </action>
  <verify>
    <automated>python -c "from shared.background_removal import flood_fill_remove; import io; from PIL import Image; img = Image.new('RGB', (100, 100), (200, 180, 160)); buf = io.BytesIO(); img.save(buf, 'PNG'); result = flood_fill_remove(buf.getvalue(), 50, 50, 32); rimg = Image.open(io.BytesIO(result)); assert rimg.mode == 'RGBA'; import numpy as np; arr = np.array(rimg); assert arr[:,:,3].mean() < 50; print('flood_fill_remove works: solid color fully removed')"</automated>
  </verify>
  <done>flood_fill_remove() removes connected same-color region from seed point. API endpoint accepts POST with image bytes + coordinates, returns processed RGBA PNG.</done>
</task>

<task type="auto">
  <name>Task 2: Add eraser mode toggle + undo stack to puzzle page JS and toolbar</name>
  <files>web/pages/puzzle.py</files>
  <action>
**JS changes in PUZZLE_CANVAS_JS** (the window.puzzleCanvas object):

Add new properties to the global object initialization (after line 56, after `_extensionBannerDismissed: false,`):
```javascript
_eraserMode: false,
_bgUndoStacks: {},   // per-fragment undo stacks: { fragmentKey: [dataURL, dataURL, ...] }
```

Add new methods to window.puzzleCanvas:

1. `toggleEraserMode(enable)`:
   - Set `this._eraserMode = enable`.
   - If enabling: change canvas cursor to 'crosshair', disable object dragging (`this.canvas.selection = false; for each obj: obj.selectable = false, obj.evented = false` -- but keep click events).
   - If disabling: restore cursor to 'default', re-enable object interaction.
   - When eraser mode is on, attach a canvas `mouse:down` handler that:
     a. Gets click coordinates relative to the canvas viewport transform.
     b. Finds which fragment image the click landed on (use `canvas.findTarget(event)` or iterate fragments checking bounding boxes).
     c. If a fragment is found:
        - Get the fragment's current image as a data URL (via `obj.toDataURL()`).
        - Push it onto `this._bgUndoStacks[key]` (create array if needed) -- this is the "before" state for undo.
        - Convert click coordinates from canvas space to the fragment's local image pixel coordinates (account for obj.left, obj.top, obj.scaleX, obj.scaleY, obj.angle, obj.flipX, viewport transform). Use Fabric.js `fabric.util.transformPoint` or the object's `toLocalPoint` method.
        - Convert the fragment to a blob (via canvas `toBlob` or dataURL-to-blob).
        - POST to `/api/puzzle_click_remove?x={localX}&y={localY}&tolerance={tolerance}` with the blob as body.
        - On success: create a new Image from the response blob, replace the Fabric object's image source using `obj.setSrc(newBlobUrl, callback)` where callback calls `canvas.requestRenderAll()`.
        - On error: pop the undo stack entry (rollback).
     d. Show a brief visual indicator (e.g., small circle at click point that fades).

2. `undoClickRemove()`:
   - Get the currently selected fragment key. If no selection, get the last fragment that was click-removed.
   - Pop the last entry from `this._bgUndoStacks[key]`.
   - If entry exists: restore it via `obj.setSrc(poppedDataURL, callback)`.
   - If stack is empty: notify (no more undo steps).

3. `getEraserUndoCount()`:
   - Returns the total undo steps available (sum of all stacks), for UI badge display.

**Coordinate transform detail**: The click event gives canvas-space coordinates. To get the pixel coordinates in the original image:
- Use `obj.toLocalPoint(new fabric.Point(canvasX, canvasY), 'left', 'top')` to get coordinates relative to the object's top-left.
- Divide by scaleX/scaleY to get pixel coordinates in the source image.
- Clamp to [0, obj.width) and [0, obj.height).

**Tolerance**: Read from a new property `this._eraserTolerance` (default 32). The toolbar will provide a small slider.

**Python toolbar changes** in `create_puzzle_page()`:

After the crop controls section (after line ~3109 `ui.separator`), add an eraser section:

```python
ui.separator().props('vertical').style('height: 20px')

# Click-to-remove eraser
eraser_btn = ui.button(tr('Eraser'), icon='auto_fix_high', on_click=lambda: _toggle_eraser()).props(
    'dense flat dark size=sm'
).tooltip(tr('Click on background to remove it'))
eraser_undo_btn = ui.button(icon='undo', on_click=lambda: ui.run_javascript(
    'window.puzzleCanvas.undoClickRemove()'
)).props('dense flat dark round size=sm color=warning').tooltip(tr('Undo last removal'))
eraser_undo_btn.set_visibility(False)
```

Add `_toggle_eraser()` function:
- Toggle eraser mode via `ui.run_javascript('window.puzzleCanvas.toggleEraserMode(!window.puzzleCanvas._eraserMode)')`.
- Toggle button appearance (highlight when active, e.g., add/remove `color=amber` prop).
- Show/hide undo button.
- When eraser mode turns off, restore normal canvas interaction.

**In toolbar row 2** (sliders area, after threshold slider ~line 3370), add a small eraser tolerance slider:
```python
eraser_tolerance_label = ui.label('').classes('text-grey-5 text-xs')
eraser_tolerance_slider = ui.slider(min=10, max=80, value=32, step=2).props('dense dark').style('width: 80px')
```
The slider should be visible only when eraser mode is active. On change, update `window.puzzleCanvas._eraserTolerance`.

**Important integration details:**
- Eraser mode and crop mode are mutually exclusive. When entering eraser mode, exit crop mode first (and vice versa).
- When eraser mode is active, disable normal drag/selection so clicks go to the eraser handler, not to object selection.
- After each successful click-remove, emit a `puzzle-change` custom event so auto-save detects the modification.
- The undo stack is purely client-side (array of data URLs). No server state. This means undo is instant.
- When a fragment is removed from canvas (deleted), clean up its undo stack entry.

**Hebrew translations**: Add these to the tr() calls:
- 'Eraser' / 'מחק רקע'
- 'Click on background to remove it' / 'לחצו על הרקע להסרתו'
- 'Undo last removal' / 'בטל הסרה אחרונה'
  </action>
  <verify>
    <automated>python -c "from web.pages.puzzle import PUZZLE_CANVAS_JS; assert 'toggleEraserMode' in PUZZLE_CANVAS_JS; assert 'undoClickRemove' in PUZZLE_CANVAS_JS; assert '_bgUndoStacks' in PUZZLE_CANVAS_JS; assert 'puzzle_click_remove' in PUZZLE_CANVAS_JS; print('All eraser JS methods and API reference present')"</automated>
  </verify>
  <done>Eraser toggle button in toolbar activates click-to-remove mode. Clicking on a fragment sends seed coordinates to server, receives processed image, updates canvas. Undo button pops previous state from client-side stack. Eraser tolerance slider controls flood fill sensitivity.</done>
</task>

<task type="checkpoint:human-verify" gate="blocking">
  <what-built>Interactive click-to-remove background removal with undo stack</what-built>
  <how-to-verify>
    1. Open puzzle page: http://localhost:8080/puzzle
    2. Add a fragment (any shelfmark, e.g., a BL manuscript with brown backing like "Or 5557B")
    3. Click the "Eraser" button in toolbar -- cursor should change to crosshair
    4. Click on a brown/gray background area of the fragment -- that connected region should become transparent
    5. Click another background area -- additional area removed (additive)
    6. Click the undo button -- last removal should revert
    7. Click undo again -- previous removal should also revert
    8. Test on a blue-mat CUL image (e.g., "T-S 12.1") -- clicking blue areas should remove them
    9. Test edge case: clicking on actual manuscript text/parchment should remove very little (tolerance limits spread)
    10. Verify eraser mode and crop mode are mutually exclusive
  </how-to-verify>
  <resume-signal>Type "approved" or describe issues</resume-signal>
</task>

</tasks>

<verification>
- flood_fill_remove() correctly removes connected same-color region from any seed point
- API endpoint returns RGBA PNG with the clicked region made transparent
- Eraser mode toggle works, changes cursor, disables drag
- Click coordinates correctly transform from canvas space to image pixel space
- Undo stack restores previous image state without server roundtrip
- Multiple clicks are additive (each adds more transparency)
- Eraser and crop modes are mutually exclusive
</verification>

<success_criteria>
User can interactively remove any background color by clicking on it, with per-step undo. Works for brown, gray, cream, blue, or any other scanning background.
</success_criteria>

<output>
After completion, create `.planning/quick/260322-jtk-brown-bg-removal-open-issues-md/260322-jtk-SUMMARY.md`
</output>
