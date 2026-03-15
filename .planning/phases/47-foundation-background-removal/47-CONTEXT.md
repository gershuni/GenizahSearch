# Phase 47: Foundation + Background Removal - Context

**Gathered:** 2026-03-15
**Status:** Ready for planning

<domain>
## Phase Boundary

Shared data model for puzzle state (PuzzleDocument/PuzzleFragment), joins.db SQLite sidecar for persistence, and an HSV background removal engine that strips solid-color library scanning backgrounds from IIIF manuscript images. This phase delivers the foundation — no canvas UI yet (Phase 48/49).

</domain>

<decisions>
## Implementation Decisions

### Data Model
- Fragment identity: both sys_id + folio_label (canonical) and FL ID (cached for fast NLI image loading). FL IDs available for all images.
- PuzzleDocument contains multiple PuzzleFragments, each storing: sys_id, folio_label, fl_id, position (x, y), rotation (degrees), scale, flip_h, flip_v, bg_removal_threshold
- joins.db SQLite sidecar follows established pattern (pgp.db, fjms_enrichment.db) — singleton service, graceful degradation, thread-safe
- Metadata is source of truth; processed images cached locally for fast reload
- FJMS join groups can pre-populate a puzzle ("load known join" option) — informational link, not required

### Image Pipeline
- One shared Python module (Pillow + NumPy) for background removal — web calls server-side via API, desktop calls directly. Same code, same results.
- IIIF images proxied through existing web/api.py patterns
- Default ~1200px images for canvas interaction, user can toggle to full resolution when needed. Full-res only for final composite export.
- Auto-process on fragment add: fetch image → remove background → show stripped result. Takes 1-3 seconds.
- User can toggle between stripped and original view
- User can adjust threshold slider

### Testing Strategy
- Collect 2-3 sample test images from each major library (NLI, Cambridge, JTS, Manchester, Oxford)
- Visual preview tool: simple page/window showing original vs stripped side-by-side with threshold slider for interactive tuning
- Manual eyeball review for quality across libraries — no automated pixel checks needed for v1

### Claude's Discretion
- HSV color space ranges for each background type (blue, green, grid paper, white)
- Alpha channel handling and edge smoothing approach
- Cache directory structure and cleanup policy
- Exact joins.db schema column types and indexes

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Background Removal Research
- `.planning/research/STACK.md` — Stack recommendations: Pillow+NumPy, no OpenCV
- `.planning/research/FEATURES.md` — Feature landscape, background removal as foundational requirement
- `.planning/research/ARCHITECTURE.md` — Component boundaries, data flow patterns
- `.planning/research/PITFALLS.md` — CORS, DPI, edge quality, WebSocket payload pitfalls

### Existing Service Patterns
- `shared/nli_crossref_service.py` — SQLite sidecar service template (singleton, graceful degradation, thread-safe, `_find_project_root()`)
- `shared/reading_desk_model.py` — Dataclass model patterns for multi-fragment state

### Image Loading
- `web/api.py` (lines 129-247) — IIIF image proxy routes (NLI, Cambridge, etc.)
- `web/services.py` (lines 124-135) — `get_thumbnail_url()`, `get_full_image_url()`, `build_iiif_image_url()`
- `shared/nli_crossref_service.py:get_folio_images()` — Folio navigation with recto/verso labels

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `shared/nli_crossref_service.py` — Template for joins.db service (copy init pattern, singleton, graceful degradation)
- `web/api.py` IIIF proxy routes — Already proxy NLI/Cambridge/Manchester/Oxford/JTS images. New bg-removal endpoint can follow same pattern.
- `shared/nli_crossref_service.py:get_folio_images()` — Returns sorted folio images with labels (1r, 1v, etc.) — reuse for puzzle fragment folio navigation
- `shared/reading_desk_model.py` — Dataclass pattern for multi-entry state (ReadingDeskEntry, ReadingDeskState)

### Established Patterns
- SQLite sidecar: `file:{path}?mode=ro`, `check_same_thread=False`, auto-detect project root
- Service singleton: `get_X_service()` + `reset_X_service()` for test isolation
- Graceful degradation: `is_available()` check, empty results on missing sidecar
- Web JS bridge: `ui.run_javascript()` with `window.X = { init, update, reset }` pattern

### Integration Points
- `web/api.py` — Add `/api/puzzle_image/{fl_id}` endpoint for bg-removed images
- `shared/` directory — New `shared/puzzle_service.py` and `shared/background_removal.py`
- `web/pages/` — Future puzzle page will import the service
- Desktop `genizah_app.py` — Future puzzle widget will import the service directly

</code_context>

<specifics>
## Specific Ideas

- FJMS puzzle (screenshot shared) as visual reference — fragments stripped from solid backgrounds, freely positioned on dark canvas
- Library backgrounds are consistently solid colors: Cambridge = blue, JTS = grid paper, NLI = various solids. All high-contrast against parchment.
- User emphasized: "Background removal is basic — without it the puzzle won't work"

</specifics>

<deferred>
## Deferred Ideas

- "Load known join" from FJMS join groups — Phase 52 (Community + Integration) scope
- DPI auto-calibration from IIIF physicalScale metadata — deferred, manual resize is baseline
- Alpha feathering for smooth edges — deferred to future enhancement

</deferred>

---

*Phase: 47-foundation-background-removal*
*Context gathered: 2026-03-15*
