# Phase 49: Web Canvas - Research

**Researched:** 2026-03-16
**Domain:** Fabric.js canvas in NiceGUI web app, fragment manipulation parity with desktop
**Confidence:** HIGH

## Summary

Phase 49 brings the fragment puzzle canvas to the web app using Fabric.js, embedded in a NiceGUI page at `/puzzle`. The desktop implementation (Phase 48) establishes the interaction model: drag, rotate, flip, resize, folio navigation, threshold adjustment, 6 background modes, crop mode, and keyboard shortcuts. The web version must achieve manipulation parity using Fabric.js native controls rather than custom handle logic.

The project already has established patterns for embedding complex JavaScript in NiceGUI pages (see `window.manuscriptViewer` in browse.py), server-side image proxy routes (web/api.py), and shared services (puzzle_image_service.py, nli_crossref_service.py). The main technical challenge is bridging Fabric.js state back to Python for session persistence and coordinating async image loading through the existing server-side pipeline.

**Primary recommendation:** Use Fabric.js v6.x (stable, well-documented CDN availability) loaded via `ui.add_head_html('<script>')`, with a `window.puzzleCanvas` global object pattern matching the existing `window.manuscriptViewer` approach. Server-side session state via `app.storage.tab` for per-tab puzzle persistence. Single `/api/puzzle_image` endpoint wrapping existing `PuzzleImageService`.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- Dedicated `/puzzle` page (like Browse, Search) -- NOT a panel or dialog
- Nav menu link alongside Search, Browse, Lists
- Layout: toolbar rows at top (shelfmark input, controls, sliders) + Fabric.js canvas filling remaining space
- Desktop browsers only for v1 -- no mobile/touch adaptation
- "Add to Puzzle" button on: Browse page, search result expanded view, Personal Lists
- Click navigates to `/puzzle?add=sys_id,fl_id` -- fragment auto-added to existing canvas
- If puzzle already has fragments, they're preserved (singleton-like behavior via session state)
- Shelfmark autocomplete input on /puzzle page itself (same as desktop)
- Use Fabric.js **native** transform handles (built-in corner rotation, edge resize) -- NOT custom handles matching desktop
- Multi-select: **both** Shift+click AND Ctrl+click work
- **Wheel always zooms canvas** (not fragment resize) -- change from desktop behavior
- Pan: click-drag on empty canvas
- Custom right-click context menu on canvas (preventDefault browser menu): Flip H/V, Delete, Threshold
- Fragment resize: via Fabric.js edge handles + toolbar scale slider (10%-400%)
- Full keyboard parity: Delete/Backspace to remove, arrow keys 1px, Shift+arrows 10px, R/Shift+R rotate 1 deg
- All 6 background modes matching desktop: dark gray (#333), black, white, checkerboard, light table (#F5F0E0), grid (50px)
- Session state (fragments, positions, rotations, scales, flip states, thresholds) persisted in session
- Spinner placeholder on canvas while image loads
- Per-fragment bg toggle: stripped vs original image
- Snap guides priority: nice-to-have, not blocking
- Single endpoint: `GET /api/puzzle_image?fl_id=X&threshold=Y&size=Z`
- Default image size: 800px
- If contour-based snapping is too complex, fall back to bounding-box or skip

### Claude's Discretion
- Canvas library choice (Fabric.js confirmed as primary candidate, but Claude may evaluate fit)
- Exact NiceGUI integration pattern (ui.element, ui.run_javascript, custom component)
- Toolbar button icons and exact layout within rows
- Session state storage mechanism (sessionStorage vs server-side)
- Batch loading concurrency limit
- Snap guide contour detection approach (or whether to fall back to bounding box)

### Deferred Ideas (OUT OF SCOPE)
- Save/load puzzle arrangements -- Phase 50 (Join Documents)
- Recto/verso toggle -- Phase 51
- Community publish -- Phase 52
- "Load known join" from FJMS join groups -- Phase 52
- Undo/redo -- deferred enhancement (CANV-09)
- Z-order layer panel -- deferred enhancement (CANV-10)
- Mobile/touch support -- future enhancement
- Desktop wheel behavior change -- cross-phase fix noted during discussion
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| CANV-07 | User can navigate folios (next/prev) within a fragment's shelfmark | Desktop folio nav pattern established (genizah_app.py:4028-4068), `nli_crossref_service.get_folio_images()` provides ordered folio list with fl_id and label. Web needs server-side folio list endpoint + JS prev/next that swaps Fabric.js image. |
| CANV-08 | User can see snap guides when aligning fragments | Fabric.js has `snapAngle`/`snapThreshold` built-in for rotation. Edge/center alignment requires custom `object:moving` handler computing edge proximity + rendering temporary Line objects. SnappyRect pattern well-documented. Nice-to-have priority per user. |
| PLAT-01 | Puzzle works in the web app (NiceGUI + Fabric.js) | Fabric.js v6.x via CDN, `window.puzzleCanvas` global JS object, `/api/puzzle_image` endpoint wrapping `PuzzleImageService`, `app.storage.tab` for session state, `/puzzle` page with `@ui.page` decorator. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| Fabric.js | 6.4.3 (latest 6.x stable) | HTML5 canvas with object model, built-in transforms | Industry standard for interactive canvas; native rotation/resize handles, SVG support, object serialization |
| NiceGUI | existing (project) | Web framework | Already in use; provides `ui.run_javascript`, `app.storage.tab`, `@ui.page` |
| PuzzleImageService | existing (shared/) | IIIF fetch + bg removal + disk cache | Already built in Phase 47; reuse via API endpoint |
| NliCrossrefService | existing (shared/) | Folio list for navigation | `get_folio_images(sys_id)` returns ordered list with fl_id + labels |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| Pillow | existing | Server-side image processing | Background removal (already in shared/background_removal.py) |
| FastAPI Response | existing | API endpoint responses | Image bytes + PNG/JPEG content-type |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| Fabric.js 6.x | Fabric.js 7.x | v7 is newer but less documented, potential breaking changes from v6 ecosystem; 6.x is stable and well-tested |
| Fabric.js | Konva.js | Konva is simpler but lacks built-in rotation handles and has weaker object serialization; Fabric.js better matches the manipulation needs |
| `app.storage.tab` | sessionStorage (client-side) | sessionStorage survives page reload but not accessible from Python; `app.storage.tab` is server-side per-tab dict, persists across page navigations within NiceGUI but cleared on connection loss. Hybrid approach recommended. |

**CDN Installation (in `ui.add_head_html`):**
```html
<script src="https://cdn.jsdelivr.net/npm/fabric@6.4.3/dist/index.min.js"></script>
```

**Why Fabric.js 6.x over 7.x:** Fabric.js 7.0.0 was released December 2024 with breaking changes. The 6.x line (6.4.3) has extensive documentation, community examples, and stable CDN builds. The v7 line is still maturing (7.1.0 as of late 2024). For a production research tool, 6.x is the safer choice.

## Architecture Patterns

### Recommended Project Structure
```
web/
  pages/
    puzzle.py              # @ui.page('/puzzle') — main puzzle page
  components/
    puzzle_canvas.py       # Fabric.js wrapper: JS code, Python bridge functions
web/api.py                 # Add /api/puzzle_image, /api/puzzle_folios endpoints
shared/
  puzzle_image_service.py  # Already exists — reuse
  puzzle_model.py          # Already exists — PuzzleFragment/PuzzleDocument
  nli_crossref_service.py  # Already exists — get_folio_images()
```

### Pattern 1: NiceGUI + Fabric.js Bridge (Primary Architecture)

**What:** A `window.puzzleCanvas` global JavaScript object manages all Fabric.js state client-side. Python communicates via `ui.run_javascript()` for commands and `ui.on('puzzle_event', handler)` for callbacks.

**When to use:** All canvas interactions.

**Architecture:**
```
Python (NiceGUI)                    JavaScript (Browser)
┌──────────────┐                    ┌─────────────────────┐
│ puzzle.py    │ ──ui.run_js()───→  │ window.puzzleCanvas │
│              │                    │   .addFragment()    │
│              │ ←─emit event────   │   .removeFragment() │
│              │                    │   .getState()       │
│ api.py       │ ←─fetch()───────   │   .setBackground()  │
│  /puzzle_img │ ──image bytes──→   │   .flipH/V()        │
│  /puzzle_fol │ ──folio JSON──→    │   .navigateFolio()  │
└──────────────┘                    └─────────────────────┘
```

**Example (JS global object — follows `window.manuscriptViewer` pattern from browse.py):**
```javascript
window.puzzleCanvas = {
    canvas: null,
    fragments: {},  // key -> fabric.Image
    bgMode: 0,
    BG_COLORS: ['#333333', '#000000', '#FFFFFF', 'checker', '#F5F0E0', 'grid'],

    init: function(canvasId) {
        this.canvas = new fabric.Canvas(canvasId, {
            backgroundColor: '#333333',
            selection: true,       // multi-select via drag
            preserveObjectStacking: true,
        });
        this.setupEvents();
        this.setupKeyboard();
    },

    addFragment: async function(key, imageUrl, state) {
        // Show spinner placeholder, fetch image, create fabric.Image
        const img = await fabric.Image.fromURL(imageUrl);
        img.set({
            left: state.x, top: state.y,
            angle: state.rotation,
            scaleX: state.scale * (state.flip_h ? -1 : 1),
            scaleY: state.scale * (state.flip_v ? -1 : 1),
            hasControls: true,
            hasBorders: true,
        });
        this.canvas.add(img);
        this.fragments[key] = img;
    },

    getState: function() {
        // Serialize all fragment positions for session persistence
        const state = {};
        for (const [key, obj] of Object.entries(this.fragments)) {
            state[key] = {
                x: obj.left, y: obj.top,
                rotation: obj.angle,
                scale: Math.abs(obj.scaleX),
                flip_h: obj.scaleX < 0,
                flip_v: obj.scaleY < 0,
            };
        }
        return JSON.stringify(state);
    },

    setupEvents: function() {
        // Wheel zoom (not item resize per user decision)
        this.canvas.on('mouse:wheel', (opt) => {
            const delta = opt.e.deltaY;
            let zoom = this.canvas.getZoom();
            zoom *= 0.999 ** delta;
            zoom = Math.min(Math.max(0.1, zoom), 10);
            this.canvas.zoomToPoint({ x: opt.e.offsetX, y: opt.e.offsetY }, zoom);
            opt.e.preventDefault();
            opt.e.stopPropagation();
        });
    }
};
```

### Pattern 2: Server-Side Image API

**What:** Single endpoint wraps existing `PuzzleImageService.resolve_fragment_image()`.

**Example (in web/api.py):**
```python
@app.get('/api/puzzle_image')
def puzzle_image(fl_id: str, threshold: float = 30.0, size: int = 800, processed: bool = True):
    """Serve processed/original fragment image for puzzle canvas."""
    from shared.puzzle_image_service import get_puzzle_image_service
    service = get_puzzle_image_service()
    image_bytes = service.resolve_fragment_image(
        fl_id=fl_id, size=size, threshold=threshold, processed=processed
    )
    if image_bytes is None:
        return Response(content="Image not found", status_code=404)
    content_type = 'image/png' if processed else 'image/jpeg'
    return Response(
        content=image_bytes,
        media_type=content_type,
        headers={"Cache-Control": "public, max-age=3600"}
    )
```

### Pattern 3: Session State via app.storage.tab

**What:** NiceGUI's `app.storage.tab` provides a server-side dict per browser tab. Use it to persist puzzle state across page navigations within the same tab.

**Key behavior:**
- Persists when user navigates `/puzzle` -> `/browse` -> `/puzzle` (same tab)
- Lost on page reload (WebSocket reconnection creates new tab storage)
- Lost on tab close

**Example:**
```python
# Save state (called periodically or on navigation away)
async def save_puzzle_state():
    state_json = await ui.run_javascript('window.puzzleCanvas.getState()')
    app.storage.tab['puzzle_state'] = state_json
    app.storage.tab['puzzle_fragments'] = fragment_metadata  # Python-side metadata

# Restore state (on page load)
def restore_puzzle_state():
    saved = app.storage.tab.get('puzzle_state')
    if saved:
        ui.run_javascript(f'window.puzzleCanvas.restoreState({saved})')
```

**Limitation:** `app.storage.tab` is cleared on page reload. For true session persistence across reloads, need to combine with `sessionStorage` on the client side. Hybrid approach: save to both `app.storage.tab` (for Python access) and `sessionStorage` (for reload survival). On page load, check `sessionStorage` first.

### Pattern 4: Folio Navigation Endpoint

**What:** Server endpoint returns ordered folio list for a sys_id, enabling prev/next navigation.

**Example:**
```python
@app.get('/api/puzzle_folios/{sys_id}')
def puzzle_folios(sys_id: str):
    """Get folio list for a manuscript (ordered by leaf/side)."""
    from shared.nli_crossref_service import get_nli_crossref_service
    service = get_nli_crossref_service()
    folios = service.get_folio_images(sys_id)
    return [{'fl_id': f.get('fl_id', ''), 'label': f.get('folio_label', '')}
            for f in folios]
```

### Anti-Patterns to Avoid
- **Sending full image bytes via WebSocket:** NiceGUI's WebSocket has payload limits. Always serve images via HTTP GET endpoint, reference by URL in Fabric.js `fromURL()`.
- **Polling for state sync:** Don't poll Python for state changes. Use Fabric.js events (`object:modified`, `object:moving`) to push state to Python only when needed (e.g., before navigation).
- **Storing large state in `app.storage.tab`:** Keep puzzle state small (fragment metadata + positions). Never store image bytes in session storage.
- **Custom rotation/resize handles:** User explicitly chose Fabric.js native handles. Don't replicate desktop's custom `_hit_handle` logic.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Canvas object model | Custom DOM manipulation | Fabric.js `fabric.Canvas` + `fabric.Image` | Object selection, transform handles, serialization all built-in |
| Image rotation/resize handles | Custom corner hit detection | Fabric.js native `hasControls: true` | Fabric handles corner rotation + edge resize natively |
| Canvas zoom | Custom viewport transform math | `canvas.zoomToPoint()` | Built-in zoom with anchor point |
| Multi-select | Custom selection tracking | Fabric.js `selection: true` + `ActiveSelection` | Built-in rubber band + Shift/Ctrl click |
| Pan | Custom scroll manipulation | Fabric.js viewport transform | `canvas.relativePan()` or manual `viewportTransform` |
| Image CORS proxy | Client-side IIIF fetch | Server-side `/api/puzzle_image` endpoint | CORS restrictions on NLI/Cambridge IIIF; server proxy avoids all CORS issues |
| Background removal | Client-side canvas pixel manipulation | Server-side `shared/background_removal.py` | Already built, tested, cached to disk |
| Shelfmark autocomplete | Custom dropdown | NiceGUI `ui.input` with `autocomplete` + server search | NiceGUI has native autocomplete support |

**Key insight:** The server-side image pipeline (fetch + bg removal + cache) already exists and works. The web canvas should treat it as a black box, requesting processed images by URL. All heavy lifting happens server-side; Fabric.js just displays and manipulates the resulting images.

## Common Pitfalls

### Pitfall 1: CORS on IIIF Images
**What goes wrong:** Fabric.js `fromURL()` with cross-origin IIIF URLs fails silently or throws tainted canvas errors.
**Why it happens:** NLI/Cambridge IIIF servers may not set `Access-Control-Allow-Origin` headers.
**How to avoid:** Always load images through the server proxy (`/api/puzzle_image?fl_id=X`), never directly from IIIF URLs. This is already the design.
**Warning signs:** Images appear but canvas `toDataURL()` fails, or images don't load at all.

### Pitfall 2: Fabric.js Flip Implementation
**What goes wrong:** Setting `scaleX = -1` flips the image but also moves its position unexpectedly.
**Why it happens:** Fabric.js applies scale from the object's origin point. Negative scale mirrors around the origin.
**How to avoid:** Use `flipX`/`flipY` properties instead of negative scale values. Fabric.js has dedicated flip properties: `obj.set({ flipX: true })`.
**Warning signs:** Fragment jumps to unexpected position after flip.

### Pitfall 3: NiceGUI WebSocket Timing
**What goes wrong:** `ui.run_javascript()` called before page DOM is ready; canvas element doesn't exist yet.
**Why it happens:** NiceGUI renders server-side, then hydrates client-side. JS runs too early.
**How to avoid:** Use `ui.timer(0.1, init_canvas, once=True)` or `client.on_connect()` to delay canvas initialization until DOM is ready.
**Warning signs:** "Cannot read property of null" errors in browser console.

### Pitfall 4: Session State Loss on Navigation
**What goes wrong:** User navigates to Browse, comes back to /puzzle, canvas is empty.
**Why it happens:** NiceGUI rebuilds the page DOM on each navigation. Fabric.js canvas and all objects are destroyed.
**How to avoid:** Save full state to `app.storage.tab` before navigation (or periodically). On page load, restore from storage. Use `beforeunload` event as backup.
**Warning signs:** Canvas resets to empty after any navigation away and back.

### Pitfall 5: Image Loading Race Conditions
**What goes wrong:** Multiple images requested simultaneously; some fail or load in wrong order; placeholders not cleaned up.
**Why it happens:** Fabric.js `fromURL()` is async. Concurrent loads can interleave.
**How to avoid:** Track pending loads by fragment key. On completion, check if fragment still exists (user may have deleted it). Use Promise.allSettled for batch loads.
**Warning signs:** Spinner stays forever, duplicate images appear, or images appear for deleted fragments.

### Pitfall 6: Fabric.js v6 CDN Global
**What goes wrong:** Using `import { Canvas } from 'fabric'` syntax in inline `<script>` tag.
**Why it happens:** CDN script tag exposes `fabric` as a global (UMD build), not as ES module.
**How to avoid:** Use `new fabric.Canvas(...)`, not `new Canvas(...)`. The CDN build sets `window.fabric` global.
**Warning signs:** "Canvas is not defined" or "fabric is not defined" errors.

### Pitfall 7: Context Menu Browser Default
**What goes wrong:** Right-click shows browser's default context menu instead of custom Flip/Delete menu.
**Why it happens:** Need to preventDefault on the canvas element's contextmenu event.
**How to avoid:** Add `canvas.upperCanvasEl.addEventListener('contextmenu', e => e.preventDefault())` and handle via Fabric.js `mouse:down` with `button === 3`.
**Warning signs:** Browser's "Save image as..." menu appears instead of custom menu.

## Code Examples

### Fabric.js Canvas Initialization (CDN pattern)
```javascript
// Source: Fabric.js official docs + browse.py manuscriptViewer pattern
window.puzzleCanvas = {
    canvas: null,

    init: function(canvasId) {
        this.canvas = new fabric.Canvas(canvasId, {
            backgroundColor: '#333333',
            selection: true,
            preserveObjectStacking: true,
            stopContextMenu: true,    // prevents browser context menu
            fireRightClick: true,     // enables right-click events
        });

        // Wheel zoom (user decision: wheel = canvas zoom only)
        this.canvas.on('mouse:wheel', function(opt) {
            var delta = opt.e.deltaY;
            var zoom = this.getZoom();
            zoom *= 0.999 ** delta;
            zoom = Math.min(Math.max(0.05, zoom), 10);
            this.zoomToPoint({ x: opt.e.offsetX, y: opt.e.offsetY }, zoom);
            opt.e.preventDefault();
            opt.e.stopPropagation();
        });

        // Pan on empty canvas drag
        var panning = false;
        this.canvas.on('mouse:down', function(opt) {
            if (!opt.target) {
                panning = true;
                this.setCursor('grabbing');
                this.lastPosX = opt.e.clientX;
                this.lastPosY = opt.e.clientY;
            }
        });
        this.canvas.on('mouse:move', function(opt) {
            if (panning) {
                var vpt = this.viewportTransform;
                vpt[4] += opt.e.clientX - this.lastPosX;
                vpt[5] += opt.e.clientY - this.lastPosY;
                this.requestRenderAll();
                this.lastPosX = opt.e.clientX;
                this.lastPosY = opt.e.clientY;
            }
        });
        this.canvas.on('mouse:up', function() {
            panning = false;
            this.setCursor('default');
        });
    }
};
```

### Adding a Fragment Image via URL
```javascript
// Source: Fabric.js fabric.Image.fromURL pattern
addFragment: function(key, imageUrl, x, y, rotation, scale, flipH, flipV) {
    // Show loading placeholder
    var placeholder = new fabric.Text('Loading...', {
        left: x, top: y,
        fontSize: 14, fill: '#888',
        selectable: false, evented: false,
    });
    this.canvas.add(placeholder);

    fabric.Image.fromURL(imageUrl, {crossOrigin: 'anonymous'}).then(function(img) {
        img.set({
            left: x, top: y,
            angle: rotation,
            scaleX: scale,
            scaleY: scale,
            flipX: flipH,
            flipY: flipV,
            hasControls: true,
            hasBorders: true,
            cornerSize: 12,
            transparentCorners: false,
        });
        this.canvas.remove(placeholder);
        this.canvas.add(img);
        this.fragments[key] = img;
        this.canvas.setActiveObject(img);
        this.canvas.requestRenderAll();
    }.bind(this));
}
```

### Keyboard Shortcuts
```javascript
// Source: User decisions - full parity with desktop
setupKeyboard: function() {
    document.addEventListener('keydown', function(e) {
        if (!this.canvas) return;
        var active = this.canvas.getActiveObject();
        if (!active) return;

        var step = e.shiftKey ? 10 : 1;
        switch(e.key) {
            case 'Delete':
            case 'Backspace':
                this.removeSelected();
                e.preventDefault();
                break;
            case 'ArrowLeft':
                active.set('left', active.left - step);
                e.preventDefault();
                break;
            case 'ArrowRight':
                active.set('left', active.left + step);
                e.preventDefault();
                break;
            case 'ArrowUp':
                active.set('top', active.top - step);
                e.preventDefault();
                break;
            case 'ArrowDown':
                active.set('top', active.top + step);
                e.preventDefault();
                break;
            case 'r':
                active.rotate((active.angle || 0) + 1);
                break;
            case 'R':
                active.rotate((active.angle || 0) - 1);
                break;
        }
        this.canvas.requestRenderAll();
    }.bind(this));
}
```

### NiceGUI Page Pattern
```python
# Source: Existing page patterns in web/main.py
@ui.page('/puzzle')
def puzzle_page_route(add: str = None):
    """Fragment Puzzle page.

    Args:
        add: Optional 'sys_id,fl_id' to auto-add a fragment on load
    """
    set_current_page('/puzzle')
    ui.add_head_html(META_TAGS)
    ui.add_head_html(COMMON_STYLES)
    ui.add_head_html(FABRIC_JS_CDN)
    ui.add_head_html(apply_theme_immediately())

    content = create_layout()
    with content:
        from web.pages.puzzle import create_puzzle_page
        create_puzzle_page(initial_add=add)
```

### Snap Guides (Bounding Box Approach)
```javascript
// Source: Fabric.js community pattern (SnappyRect / alignment guides)
// Simplified bounding-box edge snapping
setupSnapGuides: function() {
    var SNAP_THRESHOLD = 8;
    var guidelines = [];

    this.canvas.on('object:moving', function(e) {
        // Remove old guidelines
        guidelines.forEach(g => this.remove(g));
        guidelines = [];

        var moving = e.target;
        var movingBounds = moving.getBoundingRect();

        this.getObjects().forEach(function(obj) {
            if (obj === moving || obj === e.target) return;
            var bounds = obj.getBoundingRect();

            // Left edge alignment
            if (Math.abs(movingBounds.left - bounds.left) < SNAP_THRESHOLD) {
                moving.set('left', bounds.left + (moving.left - movingBounds.left));
                var line = new fabric.Line(
                    [bounds.left, 0, bounds.left, this.height],
                    { stroke: '#00FFFF', strokeDashArray: [4, 4], selectable: false, evented: false }
                );
                guidelines.push(line);
                this.add(line);
            }
            // Right edge alignment
            if (Math.abs(movingBounds.left + movingBounds.width - bounds.left - bounds.width) < SNAP_THRESHOLD) {
                var rightEdge = bounds.left + bounds.width;
                moving.set('left', rightEdge - movingBounds.width + (moving.left - movingBounds.left));
                var line = new fabric.Line(
                    [rightEdge, 0, rightEdge, this.height],
                    { stroke: '#00FFFF', strokeDashArray: [4, 4], selectable: false, evented: false }
                );
                guidelines.push(line);
                this.add(line);
            }
            // ... similarly for top, bottom, center-H, center-V
        }.bind(this));

        this.requestRenderAll();
    });

    this.canvas.on('object:modified', function() {
        guidelines.forEach(g => this.remove(g));
        guidelines = [];
        this.requestRenderAll();
    });
}
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Fabric.js v5 (`fabric.Image.fromURL(url, callback)`) | Fabric.js v6 (`fabric.Image.fromURL(url, options).then()`) | v6 (2024) | Promise-based API replaces callbacks |
| `new fabric.Canvas('id')` (v5) | Same in v6 CDN | v6 | CDN global `fabric` unchanged |
| Fabric.js `fabric.Image.fromURL` with CORS | Same but needs `crossOrigin: 'anonymous'` option | Ongoing | Must set crossOrigin even for same-origin proxy URLs |

**Deprecated/outdated:**
- Fabric.js v5 callback-based API: Still works in v5 CDN but v6 uses Promises
- `fabric.StaticCanvas`: Still available but `fabric.Canvas` is needed for interactivity

## Open Questions

1. **Fabric.js v6 vs v7 CDN availability**
   - What we know: v6.4.3 is well-established on CDN; v7.1.0 exists but is newer
   - What's unclear: Whether v7 CDN build works as global `fabric` or requires ES module
   - Recommendation: Use v6.4.3 (`fabric@6.4.3/dist/index.min.js`). Upgrade to v7 later if needed.

2. **app.storage.tab reload behavior**
   - What we know: `app.storage.tab` is cleared when WebSocket disconnects (page reload)
   - What's unclear: Exact timing -- does NiceGUI reconnect and preserve tab storage on soft navigation?
   - Recommendation: Dual-write to both `app.storage.tab` (Python access) and `sessionStorage` (reload survival). On load, restore from `sessionStorage` if `app.storage.tab` is empty.

3. **Fabric.js Image RGBA PNG transparency**
   - What we know: Background-removed images are RGBA PNGs. Fabric.js should render transparency correctly.
   - What's unclear: Whether Fabric.js handles transparent PNG clicks correctly (hit detection on transparent pixels)
   - Recommendation: Test with real bg-removed images. If clicks fall through transparent areas, set `perPixelTargetFind: true` on images (Fabric.js built-in).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | existing `pytest.ini` or `pyproject.toml` |
| Quick run command | `pytest tests/test_puzzle_image_service.py tests/test_puzzle_model.py -x` |
| Full suite command | `pytest tests/ -x` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CANV-07 | Folio navigation returns correct next/prev FL IDs | unit | `pytest tests/test_web_puzzle_api.py::test_folio_navigation -x` | No -- Wave 0 |
| CANV-08 | Snap guides appear on edge alignment | manual-only | Visual verification in browser | N/A (JS-only) |
| PLAT-01 | Puzzle page loads, image endpoint returns bytes | unit + smoke | `pytest tests/test_web_puzzle_api.py::test_puzzle_image_endpoint -x` | No -- Wave 0 |
| PLAT-01 | Fabric.js canvas initializes and accepts images | manual-only | Visual verification in browser | N/A (JS-only) |

### Sampling Rate
- **Per task commit:** `pytest tests/test_puzzle_image_service.py tests/test_puzzle_model.py tests/test_web_puzzle_api.py -x`
- **Per wave merge:** `pytest tests/ -x`
- **Phase gate:** Full suite green + manual visual verification of canvas interactions

### Wave 0 Gaps
- [ ] `tests/test_web_puzzle_api.py` -- covers CANV-07 (folio endpoint), PLAT-01 (image endpoint)
- [ ] No new framework install needed (pytest already in place)
- [ ] JS-side interactions (CANV-08, canvas manipulation) require manual testing only

## Sources

### Primary (HIGH confidence)
- Fabric.js official docs: https://fabricjs.com/docs/getting-started/installing/ -- CDN setup, Canvas API
- Fabric.js GitHub releases: https://github.com/fabricjs/fabric.js/releases -- version 7.1.0 latest, 6.4.3 stable
- Project codebase: `web/pages/browse.py` lines 558-676 -- `window.manuscriptViewer` JS bridge pattern
- Project codebase: `web/api.py` lines 129-247 -- IIIF image proxy route pattern
- Project codebase: `shared/puzzle_image_service.py` -- full image pipeline (Phase 47)
- Project codebase: `shared/nli_crossref_service.py:get_folio_images()` -- folio navigation data
- Project codebase: `genizah_app.py` lines 2565-4200 -- desktop puzzle implementation (Phase 48)
- NiceGUI docs: https://nicegui.io/documentation/storage -- `app.storage.tab` behavior

### Secondary (MEDIUM confidence)
- Fabric.js snap guide patterns: https://hackernoon.com/mastering-object-snapping-in-fabricjs-introducing-the-snappyrect-class -- SnappyRect approach
- Fabric.js alignment guidelines discussion: https://github.com/fabricjs/fabric.js/discussions/10033 -- v6 alignment

### Tertiary (LOW confidence)
- Fabric.js v7 ESM vs UMD CDN build format -- not verified; sticking with v6.x to avoid risk

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- Fabric.js well-established, NiceGUI patterns already proven in codebase
- Architecture: HIGH -- follows existing patterns (manuscriptViewer, api.py proxy, shared services)
- Pitfalls: HIGH -- CORS, WebSocket timing, session state issues are well-documented in project history
- Snap guides: MEDIUM -- community patterns exist but not built-in; bounding-box approach is straightforward

**Research date:** 2026-03-16
**Valid until:** 2026-04-16 (stable domain, no fast-moving dependencies)
