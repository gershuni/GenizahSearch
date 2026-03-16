# Phase 50: Join Documents - Context

**Gathered:** 2026-03-16
**Status:** Ready for planning

<domain>
## Phase Boundary

Researchers can save puzzle arrangements as persistent join documents in joins.db, manage multiple saved documents via a side panel, and export composite images for publication. Both web and desktop apps. No recto/verso (Phase 51), no community publish (Phase 52).

Requirements: JDOC-01 (save), JDOC-02 (load), JDOC-03 (positions/state preserved), JDOC-04 (composite export), JDOC-05 (metadata).

</domain>

<decisions>
## Implementation Decisions

### Save/Load Model: Scratch Pad + Save As
- Canvas starts as an **unnamed scratch pad** — auto-recovered on crash but not saved to documents list
- User clicks **"Save Join"** to persist — prompted for title and optional notes
- Once saved, all further changes **auto-save** to that document (every meaningful change: add/remove fragment, drag end, rotate end, flip, folio change)
- **"New Puzzle"** button clears canvas back to empty scratch pad
- **Loading** a saved document replaces the current canvas. If scratch pad has unsaved work, prompt "Save current work first?" with [Save Join] / [Discard] / [Cancel]. If already editing a saved document, it's already auto-saved — load new doc immediately.

### Save Dialog
- Title **auto-suggested from shelfmarks** joined by ' + ' (e.g. "T-S 12.1 + T-S 13.5"), editable
- Optional multi-line **notes** field (reasoning, observations, references)
- **No join type field** — every puzzle join is physical by definition. Drop `join_type` from the model or hardcode to 'physical'.

### Document Management: Side Panel
- **Collapsible side panel** (left side) showing saved documents list
- Each item shows: **title, thumbnail preview** of the composite arrangement, **fragment shelfmarks**, last-edited date
- **Click to load** — replaces canvas (with save prompt if scratch pad active)
- **Double-click title to rename** inline
- **Delete button** per item with confirmation dialog ("Delete T-S 12.1 + T-S 13.5?" [Delete] [Cancel])
- Sorted by last-edited date (most recent first)

### Composite Image Export
- **Full-resolution IIIF** images re-fetched for export (not the ~800px canvas previews)
- **Transparent PNG** (RGBA) — fragments on transparent background for publication flexibility
- **Auto-cropped to content bounds** with small margin — no wasted empty space
- Progress indicator during export (fetching full-res images takes a few seconds)
- Export button in toolbar

### Metadata
- Title + notes only (no join type classification)
- Editable in a **details section** at the bottom of the side panel after saving
- Fragment list displayed read-only in details section (shelfmark + folio label per fragment)
- Changes auto-save

### Claude's Discretion
- Thumbnail generation approach (render from cached images or snapshot canvas)
- Thumbnail cache strategy and size
- Exact side panel width and collapse animation
- Scratch pad recovery mechanism (sessionStorage, temp row in joins.db, or app.storage.tab)
- Export compositing approach (Pillow on server for both apps, or canvas.toDataURL for web)
- Progress indicator style during export
- Exact auto-crop margin size

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 47-49 Deliverables (Foundation + Canvas)
- `shared/puzzle_model.py` — PuzzleFragment/PuzzleDocument dataclasses with JSON roundtrip. NOTE: `join_type` field exists but should be dropped or hardcoded to 'physical'.
- `shared/puzzle_service.py` — joins.db CRUD: `save_document`, `load_document`, `list_documents`, `delete_document`, `list_documents_for_fragment`. Schema has `join_documents` and `join_document_fragments` tables.
- `shared/puzzle_image_service.py` — IIIF fetch, background removal, disk cache. `resolve_fragment_image(fl_id, size, threshold, processed)` — use with full-res size param for export.
- `shared/background_removal.py` — HSV bg removal engine, `remove_background(bytes, threshold)`

### Desktop Implementation
- `genizah_app.py` lines 2565-2998 — `PuzzleFragmentItem` (drag, rotate, flip, resize, crop)
- `genizah_app.py` lines 3000-3158 — `PuzzleCanvasView` (pan, zoom, background modes)
- `genizah_app.py` lines 3160-4200+ — `PuzzleCanvasWindow` (toolbar, shelfmark autocomplete, fragment management)

### Web Implementation
- `web/pages/puzzle.py` — Fabric.js canvas page with session state persistence
- `web/api.py` — IIIF image proxy routes (pattern for puzzle image endpoints)

### Requirements
- `.planning/REQUIREMENTS.md` — JDOC-01 through JDOC-05 requirements

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `PuzzleService` — Full CRUD already implemented. `save_document()` and `load_document()` handle JSON serialization of fragments. `list_documents()` returns sorted list. `delete_document()` with CASCADE.
- `PuzzleDocument.to_json()` / `from_json()` — Roundtrip serialization already works.
- `PuzzleImageService.resolve_fragment_image()` — Accepts `size` parameter. Pass full IIIF resolution for export compositing.
- Web `app.storage.tab` — Used for scratch pad session persistence (already in use for canvas state in Phase 49).

### Established Patterns
- Desktop `QDockWidget` — For collapsible side panels (already used in Qt apps). Alternative: `QSplitter` with collapsible left panel.
- Web NiceGUI `ui.left_drawer` — Built-in collapsible drawer component. Natural fit for side panel.
- Desktop `QListWidget` with custom item widgets — For document list with thumbnails.
- Web `ui.card` inside drawer — For document list items.

### Integration Points
- Desktop `PuzzleCanvasWindow` — Add side panel (QDockWidget or QSplitter), Save Join button, New Puzzle button, Export button to toolbar
- Web `puzzle.py` — Add left drawer with document list, Save/New/Export buttons to toolbar row
- `shared/puzzle_service.py` — May need schema migration to drop `join_type` CHECK constraint, or just keep it defaulting to 'physical'
- `shared/puzzle_model.py` — Simplify: remove `join_type` or default to 'physical'

</code_context>

<specifics>
## Specific Ideas

- Scratch pad model inspired by user insight: "Users will want to just play with it" — experiments don't clutter saved list
- Auto-save after explicit save addresses: "autosave will help them recover if they forgot to save"
- Distinguish save vs publish: save is personal/local (this phase), publish is community (Phase 52)
- Every puzzle join is physical by definition — no need for join type classification
- Thumbnail previews in side panel make it visual and easy to identify documents at a glance

</specifics>

<deferred>
## Deferred Ideas

- **Publish for community review** — Phase 52 (Community + Integration). "Save" is personal, "Publish" makes it visible to others.
- Recto/verso toggle — Phase 51
- Undo/redo — deferred enhancement (CANV-09)
- Z-order layer panel — deferred enhancement (CANV-10)

</deferred>

---

*Phase: 50-join-documents*
*Context gathered: 2026-03-16*
