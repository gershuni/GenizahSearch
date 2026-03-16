# Phase 49: Web Canvas - Context

**Gathered:** 2026-03-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Fabric.js canvas embedded in the NiceGUI web app at a dedicated `/puzzle` page, providing full manipulation parity with the desktop QGraphicsScene puzzle (Phase 48). Users can add fragments by shelfmark or from browse/search/lists, drag/rotate/flip/resize them, navigate folios, and see snap guides. Desktop browsers only for v1. No save/load (Phase 50), no recto/verso (Phase 51), no community (Phase 52).

Requirements: CANV-07 (folio navigation), CANV-08 (snap guides), PLAT-01 (web app).

</domain>

<decisions>
## Implementation Decisions

### Page Layout & Navigation
- Dedicated `/puzzle` page (like Browse, Search) — NOT a panel or dialog
- Nav menu link alongside Search, Browse, Lists
- Layout: toolbar rows at top (shelfmark input, controls, sliders) + Fabric.js canvas filling remaining space
- Desktop browsers only for v1 — no mobile/touch adaptation

### Entry Points ("Add to Puzzle")
- "Add to Puzzle" button on: Browse page, search result expanded view, Personal Lists
- Click navigates to `/puzzle?add=sys_id,fl_id` — fragment auto-added to existing canvas
- If puzzle already has fragments, they're preserved (singleton-like behavior via session state)
- Shelfmark autocomplete input on /puzzle page itself (same as desktop)

### Fabric.js Controls & Interaction
- Use Fabric.js **native** transform handles (built-in corner rotation, edge resize) — NOT custom handles matching desktop
- Multi-select: **both** Shift+click AND Ctrl+click work
- **Wheel always zooms canvas** (not fragment resize) — this is a **change from desktop behavior too** (desktop should be updated to match: wheel = canvas zoom only, resize via slider/handles only)
- Pan: click-drag on empty canvas
- Custom right-click context menu on canvas (preventDefault browser menu): Flip H/V, Delete, Threshold
- Fragment resize: via Fabric.js edge handles + toolbar scale slider (10%-400%)

### Keyboard Shortcuts
- Full parity with desktop: Delete/Backspace to remove, arrow keys to nudge 1px, Shift+arrows for 10px, R/Shift+R for rotate ±1°
- Canvas must have focus for shortcuts to work

### Background Modes
- All 6 modes matching desktop: dark gray (#333), black, white, checkerboard, light table (#F5F0E0), grid (50px)
- Cycle button in toolbar
- Default: dark gray

### Session State Persistence
- Puzzle state (fragments, positions, rotations, scales, flip states, thresholds) persisted in browser session (sessionStorage or server-side session)
- Navigating away and back restores the canvas exactly
- Lost on tab close (save/load is Phase 50)

### Loading UX
- Spinner placeholder on canvas while image loads from server API
- Replace with actual Fabric.js image object when ready

### Background Toggle
- Per-fragment toggle: stripped (bg-removed) vs original image — same as desktop
- Fetch original on demand if not cached

### Snap Guides (CANV-08) — Nice-to-Have
- **Priority: nice-to-have, not blocking.** Desktop works fine without them. Don't over-invest.
- Visual: thin cyan dashed lines extending across canvas when edges align
- Behavior: magnetic with ~5-10px threshold. Alt to temporarily disable.
- Alignment: edge-to-edge, ideally aligned to visible parchment contour (after bg removal) rather than bounding box
- **If contour-based snapping is too complex, fall back to bounding-box snapping or skip entirely**

### Image Pipeline (API)
- Single endpoint: `GET /api/puzzle_image?fl_id=X&threshold=Y&size=Z`
- Returns processed PNG (RGBA, bg removed) — or original JPEG if `processed=false`
- Server calls `PuzzleImageService.resolve_fragment_image()` — same code path as desktop
- Disk cache shared with desktop (same cache directory)
- Default image size: 800px (same as desktop)
- Batch loading: parallel requests for multi-fragment adds (Claude's discretion on concurrency)

### Claude's Discretion
- Canvas library choice (Fabric.js vs Konva.js vs other — based on NiceGUI JS bridge fit)
- Exact NiceGUI integration pattern (ui.element, ui.run_javascript, custom component)
- Toolbar button icons and exact layout within rows
- Session state storage mechanism (sessionStorage vs server-side)
- Batch loading concurrency limit
- Snap guide contour detection approach (or whether to fall back to bounding box)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 47-48 Deliverables (Foundation + Desktop)
- `shared/puzzle_model.py` — PuzzleFragment/PuzzleDocument dataclasses with JSON roundtrip
- `shared/puzzle_image_service.py` — IIIF fetch, background removal, disk cache. `resolve_fragment_image(fl_id, size, threshold, processed)`
- `shared/puzzle_service.py` — joins.db CRUD (save/load/list/delete), fragment reverse lookup
- `shared/background_removal.py` — HSV bg removal engine, `remove_background(bytes, threshold)`

### Desktop Implementation (reference for parity)
- `genizah_app.py` lines 2565-2998 — `PuzzleFragmentItem` (drag, rotate, flip, resize, crop, multi-select)
- `genizah_app.py` lines 3000-3158 — `PuzzleCanvasView` (pan, zoom, 6 background modes)
- `genizah_app.py` lines 3160-4200+ — `PuzzleCanvasWindow` (toolbar, shelfmark autocomplete, fragment management, folio navigation, threshold control)
- `gui_threads.py` lines 904-963 — `PuzzleImageLoaderThread`, `PuzzleMetaLoaderThread`

### Existing Web Patterns
- `web/api.py` lines 129-247 — IIIF image proxy routes (follow same pattern for puzzle endpoint)
- `web/services.py` — `get_thumbnail_url()`, `get_full_image_url()`, `build_iiif_image_url()`
- `web/pages/search.py` — Example of complex NiceGUI page with JS bridge (`ui.run_javascript`)
- `web/components/joins_panel.py` — Web fragment joins UI, `fetch_connected_fragments()` for join group loading

### Research (Phase 47)
- `.planning/research/PITFALLS.md` — CORS, DPI, edge quality, WebSocket payload pitfalls
- `.planning/research/ARCHITECTURE.md` — Component boundaries, data flow patterns

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/puzzle_image_service.py` — Server-side image pipeline (fetch + bg removal + cache) works identically for web API endpoint
- `shared/puzzle_model.py` — PuzzleFragment/PuzzleDocument dataclasses for session state serialization
- `web/api.py` IIIF proxy routes — Pattern template for `/api/puzzle_image` endpoint
- `web/components/joins_panel.py` — `fetch_connected_fragments()` for "Add from Joins" feature
- `genizah_core.py` — `normalize_shelfmark()`, `MetadataManager` for shelfmark autocomplete on web
- `shared/nli_crossref_service.py:get_folio_images()` — Folio list with labels for navigation

### Established Patterns
- NiceGUI JS bridge: `ui.run_javascript()` with `window.X = { init, update, reset }` for complex client-side components
- API routes: `@app.get('/api/...')` in web/api.py with proper error handling and content-type headers
- Page structure: `web/pages/X.py` with `@ui.page('/X')` decorator, header/nav imported from components
- Session state: `app.storage.tab` or `app.storage.browser` for per-tab/per-browser persistence (NiceGUI built-in)

### Integration Points
- `web/api.py` — Add `/api/puzzle_image` endpoint
- `web/pages/` — New `web/pages/puzzle.py` page
- `web/components/` — Possibly `web/components/puzzle_canvas.py` for the Fabric.js wrapper component
- Nav menu (in `web/components/header.py` or equivalent) — Add "Puzzle" link
- Browse page — Add "Add to Puzzle" button
- Search result expanded view — Add "Add to Puzzle" button
- Lists page — Add "Add to Puzzle" option

</code_context>

<specifics>
## Specific Ideas

- FJMS puzzle (screenshot shared) as visual reference — fragments stripped from backgrounds, freely positioned on dark canvas
- Fabric.js native handles are acceptable — don't need to match desktop's custom white circles
- Wheel-zoom is the web standard — user explicitly wants desktop changed to match this too
- Snap guides should be best-effort: contour-based if feasible, bounding-box fallback, or skip entirely. "Don't go out of your way for snap guides if it does not work"

</specifics>

<deferred>
## Deferred Ideas

- Save/load puzzle arrangements — Phase 50 (Join Documents)
- Recto/verso toggle — Phase 51
- Community publish — Phase 52
- "Load known join" from FJMS join groups — Phase 52
- Undo/redo — deferred enhancement (CANV-09)
- Z-order layer panel — deferred enhancement (CANV-10)
- Mobile/touch support — future enhancement
- **Desktop wheel behavior change** — Change desktop PuzzleFragmentItem to NOT resize on scroll wheel (wheel = canvas zoom only). This is a cross-phase fix noted during Phase 49 discussion.

</deferred>

---

*Phase: 49-web-canvas*
*Context gathered: 2026-03-16*
