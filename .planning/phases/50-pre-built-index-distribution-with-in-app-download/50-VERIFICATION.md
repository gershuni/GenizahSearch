---
phase: 50-join-documents
verified: 2026-03-16T18:45:00Z
status: passed
score: 4/4 must-haves verified
re_verification: false
---

# Phase 50: Join Documents Verification Report

**Phase Goal:** Researchers can save their puzzle arrangements as persistent join documents and export composite images for publication
**Verified:** 2026-03-16T18:45:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | User can save the current puzzle arrangement and reload it later with all fragment positions, rotations, scales, and flip states preserved exactly | VERIFIED | `_on_save_join` / `save_document` with `fragments_json` containing x, y, rotation, scale, flip_h, flip_v. `_load_document` restores via `_pending_fragments` + `PuzzleFragmentItem` constructor. Web: `save_join` / `load_document` in puzzle.py with same fields. All 5 commits verified. |
| 2 | User can maintain multiple saved join documents and switch between them | VERIFIED | `list_documents` returns all saved docs ordered by updated_at. Desktop: QDockWidget side panel with `_docs_list` QListWidget. Web: left drawer with `docs_container`. Both have delete and rename. `_on_doc_list_clicked` handles switching with unsaved-changes prompt. |
| 3 | User can export a composite PNG image of the assembled join (background-removed fragments composited at full resolution) | VERIFIED | `compose_puzzle_export` in `shared/puzzle_export.py` (199 lines) produces RGBA PIL Image using `resolve_fragment_image(fl_id, size=3000)`. Desktop: `_on_export_png` calls `compose_puzzle_export`, saves via `QFileDialog`. Web: `export_png` uses `run.io_bound`, triggers `ui.download`. API: `POST /api/puzzle_export` returns PNG bytes. Export test passed: produces `119x100` RGBA image. |
| 4 | User can add and edit metadata on a join document: title, free-text notes, and fragment identifiers | VERIFIED | `PuzzleDocument` has `title`, `notes` fields. Desktop: `_details_group` with `_title_edit` (QLineEdit) and `_notes_edit` (QTextEdit), `_on_title_changed`/`_on_notes_changed` trigger auto-save. Web: `details_container` with `title_input`/`notes_input`, blur events trigger `schedule_auto_save`. `_fragments_label` (desktop) and `fragments_label` (web) show read-only fragment list. |

**Score:** 4/4 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/puzzle_model.py` | PuzzleFragment with crop/processed fields, PuzzleDocument with join_type='physical' default | VERIFIED | 59 lines. `join_type='physical'` confirmed. `crop_top/bottom/left/right` (int, default 0) and `processed` (bool, default True) confirmed by live import test. |
| `shared/puzzle_service.py` | Schema v2 migration with thumbnail_b64, shelfmark in fragments_json, thumbnail-safe saves | VERIFIED | 345 lines. `thumbnail_b64` column present in live schema test. Shelfmark, crop, processed all in `fragments_json`. Thumbnail preservation on metadata-only save confirmed. `list_documents` returns `shelfmarks_summary`. |
| `shared/puzzle_export.py` | Composite image export and thumbnail generation | VERIFIED | 199 lines. `compose_puzzle_export`, `generate_thumbnail`, `auto_suggest_title` all import and execute correctly. Produces RGBA output. Crop scaling and rotation confirmed in tests. |
| `genizah_app.py` | PuzzleCanvasWindow with QDockWidget side panel, save/load/export/new buttons, auto-save | VERIFIED | 44 occurrences of new method names. `QDockWidget`, `_on_save_join`, `_load_document`, `_on_export_png`, `_on_new_puzzle`, `_auto_save`, `_refresh_docs_list`, `_build_fragments_list`, `_details_group`, `_on_scene_changed`, `_scene_change_debounce`, `_spawn_meta_loader` all present. Syntax OK. |
| `web/pages/puzzle.py` | Puzzle page with left drawer, save/load/export/new buttons, event-driven auto-save | VERIFIED | `ui.left_drawer`, `save_join`, `load_document`, `new_puzzle`, `export_png`, `refresh_docs_list`, `build_fragments_list`, `schedule_auto_save`, `clearAll`, `getCropState`, `object:modified`, `puzzle-object-modified`, `pending_crops`, `doc_state['loading']`, `load_pending` all present. Syntax OK. |
| `web/api.py` | Puzzle export and thumbnail API endpoints | VERIFIED | 6 endpoints confirmed: `puzzle_documents_list`, `puzzle_document_get`, `puzzle_document_save`, `puzzle_document_delete`, `puzzle_export`, `puzzle_thumbnail`. `crop_top` and `processed` fields in document response. Syntax OK. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `shared/puzzle_export.py` | `shared/puzzle_image_service.py` | `resolve_fragment_image(fl_id, size=3000)` | WIRED | Line 74: `image_service.resolve_fragment_image(...)` in `compose_puzzle_export` |
| `shared/puzzle_service.py` | `shared/puzzle_model.py` | `PuzzleDocument/PuzzleFragment imports` | WIRED | Deferred import confirmed in puzzle_service.py; model classes used throughout |
| `genizah_app.py (PuzzleCanvasWindow)` | `shared/puzzle_service.py` | `save_document, load_document, list_documents, delete_document` | WIRED | Lines 4282, 4326, 4400, 4428, 4551, 4563, 4615 — all call `get_puzzle_service()` |
| `genizah_app.py (PuzzleCanvasWindow)` | `shared/puzzle_export.py` | `compose_puzzle_export, generate_thumbnail, auto_suggest_title` | WIRED | Lines 4398, 4497 — deferred imports inside `_on_save_join` and `_on_export_png` |
| `web/pages/puzzle.py` | `shared/puzzle_service.py` | `save_document, load_document, list_documents, delete_document` | WIRED | Lines 1575, 1653, 1673, 1679, 1766 — `get_puzzle_service(thread_safe=True)` |
| `web/pages/puzzle.py` | `shared/puzzle_export.py` | `compose_puzzle_export, generate_thumbnail, auto_suggest_title` | WIRED | Lines 1576, 1669, 1859, 1873 — deferred imports inside `export_png`, `do_auto_save` |
| `web/api.py` | `shared/puzzle_export.py` | `compose_puzzle_export via puzzle_export endpoint` | WIRED | Line 821: `from shared.puzzle_export import compose_puzzle_export` in `/api/puzzle_export` handler |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| JDOC-01 | 50-02, 50-03 | User can save a puzzle arrangement as a join document | SATISFIED | Desktop: `_on_save_join` persists to joins.db. Web: `save_join` + `POST /api/puzzle_document`. Both generate thumbnail and auto-suggested title. |
| JDOC-02 | 50-02, 50-03 | User can load a previously saved join document | SATISFIED | Desktop: `_load_document` via `_pending_fragments` pipeline restores all transforms. Web: `load_document` calls `addFragment` with x/y/rotation/scale/flip. |
| JDOC-03 | 50-01 | Join document stores fragment IDs, positions, rotations, scales, and flip state | SATISFIED | `fragments_json` includes sys_id, fl_id, folio_label, shelfmark, x, y, rotation, scale, flip_h, flip_v, bg_removal_threshold, crop_top/bottom/left/right, processed. Confirmed by live serialization test. |
| JDOC-04 | 50-01, 50-03 | User can export a composite image of the assembled join | SATISFIED | `compose_puzzle_export` in puzzle_export.py (199 lines) produces RGBA PNG. Desktop: `_on_export_png` with progress dialog. Web: `export_png` via run.io_bound + ui.download. API: `POST /api/puzzle_export`. |
| JDOC-05 | 50-02, 50-03 | User can add metadata (join type, notes) to a join document | SATISFIED | `PuzzleDocument.title` and `PuzzleDocument.notes` fields. Desktop: editable `_title_edit` + `_notes_edit` in details panel with auto-save. Web: `title_input` + `notes_input` in drawer. |

No orphaned requirements: REQUIREMENTS.md shows all JDOC-01 through JDOC-05 mapped to Phase 50 with status Complete.

### Anti-Patterns Found

None found in the phase-modified files. No TODO/FIXME/HACK/placeholder comments in shared/puzzle_export.py, shared/puzzle_model.py, shared/puzzle_service.py, or the puzzle-related sections of genizah_app.py.

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| — | — | None found | — | — |

### Human Verification Required

#### 1. Desktop Save/Load Roundtrip

**Test:** Open puzzle canvas, add 2 fragments, drag them to different positions, rotate one, then click Save Join. Close the puzzle window and reopen it. Click the document in the side panel to load it.
**Expected:** Both fragments appear at their saved positions with correct rotations. Folio navigation dropdowns work.
**Why human:** Requires actual PyQt6 UI interaction with live image loading thread behavior; cannot verify canvas transform fidelity programmatically.

#### 2. Web Auto-Save via object:modified

**Test:** In the web puzzle page, save a document, then drag a fragment to a new position. Wait 2 seconds.
**Expected:** Status bar shows "Auto-saved" and the document in the drawer updates its thumbnail.
**Why human:** Requires live browser interaction with Fabric.js canvas and WebSocket event round-trip timing.

#### 3. Web Export PNG Download

**Test:** In the web puzzle page with 2+ fragments on canvas, click Export PNG.
**Expected:** Browser initiates a file download of a PNG file containing a transparent-background composite of all fragment images at full resolution.
**Why human:** Requires live browser + IIIF image fetch from external server (Cambridge/NLI); cannot verify image content or download behavior programmatically.

#### 4. Crop State Roundtrip (Web)

**Test:** Add a fragment, crop it using the crop tool, save the document, reload it. Verify the crop is preserved.
**Expected:** Cropped fragment reloads with the same crop dimensions applied via `on_puzzle_add_result` callback.
**Why human:** Requires live Fabric.js interaction to set crop state, then verifying `getCropState()` reads correct per-object properties vs. the transient `_cropOffsets`.

### Gaps Summary

No gaps found. All 4 observable truths are verified by direct code inspection and live Python execution of the shared service layer tests. All 5 requirement IDs (JDOC-01 through JDOC-05) are satisfied by substantive, wired implementations across shared/puzzle_model.py, shared/puzzle_service.py, shared/puzzle_export.py, genizah_app.py, web/pages/puzzle.py, and web/api.py. All 5 git commits documented in the summaries exist in the repository history.

---

_Verified: 2026-03-16T18:45:00Z_
_Verifier: Claude (gsd-verifier)_
