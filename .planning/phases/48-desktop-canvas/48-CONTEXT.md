# Phase 48: Desktop Canvas - Context

**Gathered:** 2026-03-16
**Status:** Ready for planning

<domain>
## Phase Boundary

QGraphicsScene-based puzzle widget in the PyQt6 desktop app. Users can add fragments by shelfmark (with autocomplete) or from browse/search/lists, then drag, rotate (corner handle), flip, resize them on a dark canvas with background-removed images. Separate QMainWindow, single-instance. No save/load (Phase 50), no recto/verso (Phase 51), no community (Phase 52).

</domain>

<decisions>
## Implementation Decisions

### Fragment Interaction
- Free movement by default, hold Shift to snap to grid
- Rotation via corner handle drag (like image editors), NOT slider/buttons
- Multi-select: Ctrl+click to select multiple. Move/rotate/flip applies to all selected.
- Flip: Both toolbar buttons AND right-click context menu
- Resize: Mouse scroll wheel on fragment + toolbar scale slider (10%-400%)

### Canvas Layout & Entry
- **Separate window** (QMainWindow), not a tab. Like the Reading Desk pattern.
- **Single instance**: "Add to Puzzle" adds fragment to existing window (or opens if closed)
- **Entry points for "Add to Puzzle" button**: Browse page, ResultDialog, AND Personal Lists
- Puzzle window also has shelfmark input with **autocomplete** + ability to pick from known FJMS joins and personal lists
- Shelfmark autocomplete uses existing `normalize_shelfmark()` → `_shelf_to_sys` lookup

### Fragment Properties (Top Toolbar)
- **Minimal info**: Shelfmark + folio label for selected fragment
- **Controls**: Flip H/V buttons, threshold slider (per-fragment), folio prev/next, delete fragment, scale slider
- Per-fragment threshold: each fragment stores its own bg_removal_threshold (already in data model)
- Scale slider: 10%-400% in toolbar for precise control (in addition to mouse wheel)
- Folio navigation: auto re-process with fragment's saved threshold. If cached, instant.

### Canvas Background & Navigation
- Toggle between dark gray (#333) and checkerboard. Default dark gray.
- Full zoom/pan at view level: Ctrl+wheel to zoom canvas, hand-drag to pan. Fragments still independently movable on top.

### Claude's Discretion
- Exact corner handle visual design (small circles? squares? rotation cursor?)
- Selection visual (dashed border? handles? glow?)
- Toolbar button icons and layout
- Grid snap increment size
- How multi-select group transform center is calculated
- Async image loading thread design (can reuse ImageLoaderThread pattern)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 47 Deliverables (Foundation)
- `shared/puzzle_model.py` — PuzzleFragment/PuzzleDocument dataclasses with JSON roundtrip
- `shared/puzzle_image_service.py` — IIIF fetch, background removal, disk cache. `resolve_fragment_image(fl_id, size, threshold, processed)`
- `shared/puzzle_service.py` — joins.db CRUD (save/load/list/delete), fragment reverse lookup
- `shared/background_removal.py` — HSV bg removal engine, `remove_background(bytes, threshold)`

### Existing Desktop Patterns
- `genizah_app.py` lines 1391-1573 — `ZoomableScrollArea` (QGraphicsScene/View with zoom, pan, rotation)
- `genizah_app.py` lines 1575-1950 — `ManuscriptViewerWidget` (image loading, source switching)
- `genizah_app.py` lines 8611-8623 — Tab/window creation pattern
- `gui_threads.py` — `ImageLoaderThread` pattern for async image fetch
- `genizah_core.py` — `normalize_shelfmark()`, MetadataManager for shelfmark→sys_id resolution

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `ZoomableScrollArea` — QGraphicsScene/View wrapper. Reuse for canvas-level zoom/pan. But need per-fragment items (not single pixmap).
- `ImageLoaderThread` — Async image fetch pattern. Reuse for loading fragment images in background.
- `_shelf_to_sys` / `normalize_shelfmark()` — Shelfmark autocomplete lookup.
- `ResultDialog` — Already has buttons row; add "Add to Puzzle" alongside existing buttons.

### Established Patterns
- QGraphicsScene items: `QGraphicsPixmapItem` for images. Need custom subclass for draggable/rotatable/flippable.
- Tab creation: `create_X_tab()` returning QWidget. Here: `create_puzzle_window()` returning QMainWindow.
- Right-click context menu: Already used in ZoomableScrollArea (Copy/Save).
- Signal/slot for inter-widget communication (image loaded → display).

### Integration Points
- Browse page: Add "Add to Puzzle" button next to existing buttons
- ResultDialog: Add "Add to Puzzle" button in the button row
- Personal Lists: Add "Add to Puzzle" option per list item
- All three call a shared method: `GenizahSearchApp.add_to_puzzle(sys_id, shelfmark, folio_label, fl_id)`

</code_context>

<specifics>
## Specific Ideas

- FJMS puzzle (screenshot shared earlier) as visual reference — fragments stripped from backgrounds, freely positioned on dark canvas
- Corner handle rotation like image editors (Photoshop, GIMP free transform)
- Single puzzle window reused across all "Add to Puzzle" entry points (singleton pattern)
- Autocomplete shelfmark input + pick from FJMS joins + personal lists in the puzzle window

</specifics>

<deferred>
## Deferred Ideas

- Save/load puzzle arrangements — Phase 50 (Join Documents)
- Recto/verso toggle — Phase 51
- Community publish — Phase 52
- "Load known join" from FJMS join groups — Phase 52
- Undo/redo — deferred enhancement (CANV-09)
- Z-order layer panel — deferred enhancement (CANV-10)
- Snap guides between fragments — Phase 49 (CANV-08)
- Oxford image loading via parts JSON — future enhancement

</deferred>

---

*Phase: 48-desktop-canvas*
*Context gathered: 2026-03-16*
