# Plan Review Request — Phase 47: Foundation + Background Removal

You are a senior software architect reviewing an implementation plan before execution. Your job is to find gaps, risks, over-engineering, under-engineering, and logical errors. Be direct and specific.

---

## Project Background

**GenizahSearch** is a research platform for the Cairo Genizah (~400,000 manuscript fragments). It has:
- A **NiceGUI web app** and a **PyQt6 desktop app** (both must be maintained)
- **Tantivy search** over ~217,000 manuscript records
- **SQLite sidecars** for read-only scholarly data (pgp.db 147MB, fjms_enrichment.db 941MB, nli_crossref.db)
- **IIIF image loading** from NLI, Cambridge, Manchester, JTS, Oxford
- **Supabase** for community features (auth, corrections, lists)
- Shared core in `genizah_core.py` (~8,300 lines), shared services in `shared/` directory

The platform has shipped 10 milestones (v1 through v6.5.0) with ~161 plans executed.

## Current Milestone: v7.0.0 Fragment Puzzle

**Goal:** Visual jigsaw tool for assembling physical joins from manuscript fragment images. Researchers take 2+ fragment images, strip their backgrounds, and position them on a canvas to reconstruct the original manuscript page.

**6 phases planned (47-52):**
1. **Phase 47: Foundation + Background Removal** ← THIS IS WHAT YOU'RE REVIEWING
2. Phase 48: Desktop Canvas (QGraphicsScene)
3. Phase 49: Web Canvas (Fabric.js + NiceGUI)
4. Phase 50: Join Documents (save/load/export)
5. Phase 51: Recto/Verso
6. Phase 52: Community + Integration

**23 requirements total.** Phase 47 covers: BGRM-01, BGRM-02, BGRM-03.

## Phase 47 Requirements

| ID | Description |
|----|-------------|
| **BGRM-01** | Fragment images are automatically stripped of solid-color backgrounds (parchment shape visible) |
| **BGRM-02** | User can toggle between stripped and original image view |
| **BGRM-03** | User can adjust the background removal threshold |

## Phase 47 Success Criteria

1. A fragment image from NLI/Cambridge/Manchester can be loaded and its solid-color background removed, revealing the parchment shape with transparent surroundings
2. User can toggle between the stripped (transparent background) and original rectangular image
3. User can adjust the removal sensitivity threshold and see the mask update
4. PuzzleDocument/PuzzleFragment data model serializes and deserializes fragment positions, rotations, scales, and flip states correctly (roundtrip test)
5. joins.db SQLite sidecar schema is created and follows the established sidecar pattern

## User Decisions (Locked — Cannot Change)

- **No OpenCV.** Pillow + NumPy only for background removal.
- **Shared Python module** — web calls server-side via API, desktop calls directly. Same code, same results.
- **Fragment identity**: sys_id + folio_label (canonical) + FL ID (cached). FL IDs available for all images.
- **joins.db** follows established sidecar pattern (singleton, graceful degradation, thread-safe). First **read-write** sidecar in the project.
- **Metadata is source of truth**; processed images cached locally for fast reload.
- **Auto-process** background removal on fragment add (1-3 seconds).
- **Testing**: Sample images from each major library, visual preview tool with threshold slider, manual eyeball review.
- **Default image size**: ~1200px for canvas, user can toggle to full-res.

## Technical Context

- **Library backgrounds**: Solid colors — Cambridge blue, JTS grid paper, NLI gray, Oxford cream. All high-contrast against parchment.
- **Pillow HSV**: `Image.convert('HSV')` scales all channels 0-255 (NOT 0-360). `putalpha()` applies L-mode mask as alpha.
- **Morphological cleanup**: `ImageFilter.MinFilter(3)` (erode) + `ImageFilter.MaxFilter(5)` (dilate) — no OpenCV needed.
- **Existing sidecar pattern**: `shared/nli_crossref_service.py` — singleton, `_find_project_root()`, graceful degradation, `is_available()`.
- **Key difference**: joins.db is read-write (WAL mode, explicit commits) unlike all existing read-only sidecars.

---

## THE THREE PLANS TO REVIEW

### Plan 47-01: Data Model + joins.db Sidecar (Wave 1, autonomous)

**Objective:** PuzzleDocument/PuzzleFragment dataclasses + joins.db sidecar service.

**Task 1: PuzzleDocument/PuzzleFragment data model with JSON roundtrip**
- Files: `shared/puzzle_model.py`, `tests/test_puzzle_model.py`
- TDD with 6 tests: field defaults, roundtrip serialization, empty doc, join types, non-default values
- PuzzleFragment fields: sys_id, folio_label, fl_id, x, y, rotation, scale, flip_h, flip_v, bg_removal_threshold (default 30.0)
- PuzzleDocument fields: id (UUID), title, notes, join_type ('physical'/'content'/'uncertain'), fragments list, created_at, updated_at
- Serialization: `to_json()` via `dataclasses.asdict()` + `json.dumps()`, `from_json()` via `json.loads()` + constructor

**Task 2: joins.db sidecar service with CRUD operations**
- Files: `shared/puzzle_service.py`, `tests/test_puzzle_service.py`
- TDD with 7 tests: schema creation, CRUD (create+load, list, update, delete), graceful degradation, singleton
- Schema: `join_documents` table (id, title, notes, join_type with CHECK constraint, fragments_json, created_at, updated_at) + `meta` table (schema_version)
- Follows `nli_crossref_service.py` pattern but READ-WRITE: no `?mode=ro`, WAL mode, `CREATE TABLE IF NOT EXISTS`, explicit commits
- Singleton: `get_puzzle_service()` / `reset_puzzle_service()`

### Plan 47-02: Background Removal Engine (Wave 1, autonomous, parallel with 01)

**Objective:** HSV-based background removal using Pillow + NumPy.

**Task 1: Add Pillow to requirements.txt**
- Pillow is NOT currently installed (confirmed). NumPy 2.4.3 is installed but undeclared.
- Add both to requirements.txt, install.

**Task 2: Background removal engine with synthetic test images**
- Files: `shared/background_removal.py`, `tests/test_background_removal.py`
- TDD with 8 tests: blue/green/gray backgrounds, threshold effect, safety check, original preserved, RGBA PNG output, corner detection
- Algorithm: Load image → convert to HSV → sample corners for bg color (median of 20x20 blocks from 4 corners) → Euclidean distance mask → morphological cleanup (MinFilter/MaxFilter) → putalpha → save RGBA PNG
- Safety: if foreground < 10% of pixels, skip removal (return original as RGBA)
- Constants: DEFAULT_THRESHOLD=30.0, CORNER_SAMPLE_SIZE=20, MIN_FOREGROUND_RATIO=0.10
- All tests use synthetic images (Pillow-generated) — no external fixtures

### Plan 47-03: Visual Preview Tool + Quality Checkpoint (Wave 2, depends on 01+02, NOT autonomous)

**Objective:** Interactive PyQt6 preview tool for testing bg removal on real IIIF images.

**Task 1: Interactive background removal preview tool**
- File: `scripts/preview_background_removal.py`
- PyQt6 window: split view (original left, stripped right), FL ID input, sample image buttons (~8 hardcoded FL IDs from NLI/Cambridge/Manchester/Oxford), QSlider for threshold (5-150, default 30), toggle button, info panel (detected bg color, foreground ratio, processing time)
- Imports from `shared/background_removal.py` and `web/services.py` for IIIF URLs
- Dev tool only — functional, not polished

**Task 2: Human verification checkpoint (blocking)**
- Run the preview tool, test across libraries
- Verify: backgrounds stripped, threshold slider works, toggle works, edge quality acceptable
- Resume signal: "approved" or describe issues

---

## What I Want You to Review

1. **Completeness**: Do these 3 plans fully deliver the phase requirements (BGRM-01/02/03) and all 5 success criteria? Any gaps?

2. **Correctness**: Is the technical approach sound?
   - HSV corner-sampling + Euclidean distance + morphological cleanup for solid-color backgrounds
   - Pillow's HSV 0-255 scale handling
   - Safety check (10% foreground minimum)
   - joins.db as first read-write sidecar (WAL, explicit commits)

3. **Risk Assessment**: What could go wrong?
   - Background removal quality on real images (not just synthetic)
   - Dark parchment on dark backgrounds
   - Grid paper backgrounds (JTS) vs solid color assumption
   - Performance at 1200px
   - Edge cases in the data model

4. **Over/Under-engineering**:
   - Is the data model too complex or too simple for what's coming in phases 48-52?
   - Is 6+7+8 = 21 tests the right amount?
   - Is the preview tool the right level of effort for a testing tool?

5. **Wave structure**: Plans 01 and 02 run in parallel (Wave 1), Plan 03 depends on both (Wave 2). Is this correct? Any hidden dependencies?

6. **Downstream impact**: Will phases 48-52 (canvas, documents, community) be able to build on this foundation cleanly? Any missing abstractions or APIs they'll need?

Be specific. If something is fine, say so briefly. Focus your time on things that are wrong or risky.
