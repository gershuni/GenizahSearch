---
phase: 47-foundation-background-removal
verified: 2026-03-16T04:00:00Z
status: passed
score: 16/16 must-haves verified
re_verification: false
---

# Phase 47: Foundation + Background Removal Verification Report

**Phase Goal:** Researchers have a shared data model for puzzle state and a working background removal engine that isolates parchment from solid-color library scanning backgrounds
**Verified:** 2026-03-16T04:00:00Z
**Status:** passed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | PuzzleDocument with fragments serializes to JSON and deserializes back identically | VERIFIED | `to_json`/`from_json` wired in `shared/puzzle_model.py`; `test_roundtrip_serialization` passes |
| 2 | joins.db is created on first use with WAL mode, busy_timeout, and correct schema including fragment index table | VERIFIED | `PRAGMA journal_mode=WAL`, `PRAGMA busy_timeout=5000`, `join_document_fragments` table all present in `shared/puzzle_service.py`; `test_schema_creation` passes |
| 3 | A join document can be saved and loaded with all fragment state preserved | VERIFIED | `save_document`/`load_document` fully implemented; `test_create_and_load_document` passes |
| 4 | Concurrent writes from 3 threads all succeed without database locked errors | VERIFIED | `threading.Lock` on `_write_lock` + `thread_safe=True` + `PRAGMA busy_timeout=5000`; `test_concurrent_writes` passes |
| 5 | Fragment lookup by fl_id or sys_id returns the correct document IDs | VERIFIED | `list_documents_for_fragment` with `fl_id` and `sys_id` branches; tests `test_list_documents_for_fragment` and `test_list_documents_for_fragment_by_sys_id` pass |
| 6 | A solid-color background image is converted to RGBA PNG with transparent background | VERIFIED | `remove_background()` returns PNG bytes with `putalpha(mask)`; `test_solid_blue_background_removed`, `test_solid_green_background_removed` pass |
| 7 | Adjusting threshold changes how much background is removed | VERIFIED | `threshold` parameter in `remove_background` signature; `test_threshold_affects_mask` confirms different opaque pixel counts |
| 8 | Over-aggressive removal (>95% transparent) returns original image as safety fallback | VERIFIED | `if foreground_ratio >= min_foreground_ratio: rgba.putalpha(mask)` — skips removal when below threshold; `test_safety_check_preserves_content` passes |
| 9 | Corner sampling detects the dominant background color automatically | VERIFIED | `detect_background_color` samples all 4 corners; `test_detect_background_color_from_corners` passes |
| 10 | Low-saturation backgrounds (gray, cream) are handled correctly using value-only distance | VERIFIED | `if bg_saturation < LOW_SATURATION_THRESHOLD: diff = np.abs(...)` value-only branch; `test_solid_gray_background_removed` and `test_low_saturation_cream_background` pass |
| 11 | resolve_fragment_image fetches a IIIF image, applies background removal, and returns processed bytes | VERIFIED | `PuzzleImageService.resolve_fragment_image` calls `_fetch_iiif_image` then `remove_background`; `test_resolve_returns_processed_bytes` passes |
| 12 | Processed images are cached to disk with a deterministic key of (fl_id, size, threshold) | VERIFIED | Cache filename `{safe_id}_{size}_{threshold:.1f}.png`; `test_resolve_caches_to_disk` and `test_cache_path_includes_all_components` pass |
| 13 | Changing threshold produces a new cache entry, not a stale result | VERIFIED | Different threshold yields different filename; `test_resolve_different_threshold_different_cache` confirms 2 distinct files and 2 fetches |
| 14 | invalidate_cache clears entries for a specific fl_id, optionally filtered by threshold | VERIFIED | `invalidate_cache(fl_id, threshold=None)` uses glob pattern; `test_invalidate_cache_specific_threshold` and `test_invalidate_cache_all_thresholds` pass |
| 15 | Cache location follows platform convention: LOCALAPPDATA on Windows, project cache dir otherwise | VERIFIED | `_get_default_cache_dir` checks `os.environ.get('LOCALAPPDATA')`; `test_cache_path_platform_default` passes |
| 16 | Preview tool provides interactive threshold slider, toggle between stripped/original, and imports from shared modules only | VERIFIED | `QSlider`, `_toggle_btn`, `from shared.puzzle_image_service import`, `from shared.background_removal import` all present; no `from web` imports; user-approved checkpoint in Plan 04 summary |

**Score:** 16/16 truths verified

---

## Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/puzzle_model.py` | PuzzleDocument and PuzzleFragment dataclasses with JSON roundtrip | VERIFIED | 52 lines; `PuzzleFragment` (10 fields incl. `bg_removal_threshold`), `PuzzleDocument` (7 fields), `to_json`/`from_json` |
| `shared/puzzle_service.py` | joins.db sidecar service with CRUD, concurrency, fragment index | VERIFIED | 305 lines; `PuzzleService`, `get_puzzle_service`, `reset_puzzle_service`, WAL + Lock + FK pragmas, all CRUD methods |
| `tests/test_puzzle_model.py` | Data model roundtrip tests | VERIFIED | 6 tests covering fields, defaults, roundtrip, join types — all pass |
| `tests/test_puzzle_service.py` | joins.db CRUD + concurrency + fragment index tests | VERIFIED | 11 tests covering schema, CRUD, degradation, singleton, concurrency, fragment index — all pass |
| `shared/background_removal.py` | HSV-based background removal engine with low-saturation fallback | VERIFIED | 112 lines; `remove_background`, `detect_background_color`, `create_mask`, `DEFAULT_THRESHOLD=30.0`, `MIN_FOREGROUND_RATIO=0.05`, `LOW_SATURATION_THRESHOLD=30` |
| `tests/test_background_removal.py` | Unit tests with synthetic test images including low-saturation cases | VERIFIED | 11 tests covering solid colors, low-saturation fallback, threshold, safety, output format — all pass |
| `requirements.txt` | Pillow and numpy declared | VERIFIED | Lines 13-14: `Pillow`, `numpy` |
| `shared/puzzle_image_service.py` | Shared image resolver/cache for fragment puzzle images | VERIFIED | 193 lines; `PuzzleImageService`, `resolve_fragment_image`, `get_cache_path`, `invalidate_cache`, singleton pattern, LOCALAPPDATA logic, module-level convenience functions |
| `tests/test_puzzle_image_service.py` | Tests for image resolution, caching, and invalidation | VERIFIED | 10 tests covering cache paths, resolve, caching, threshold/size isolation, invalidation, original mode — all pass |
| `scripts/preview_background_removal.py` | Interactive visual preview tool | VERIFIED | 401 lines; PyQt6 window, `QSlider`, `_toggle_btn`, checkerboard compositing, info panel, imports from `shared/` only — syntax valid, checkpoint approved |

---

## Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `shared/puzzle_service.py` | `shared/puzzle_model.py` | `from shared.puzzle_model import PuzzleDocument, PuzzleFragment` | WIRED | Line 23 |
| `shared/puzzle_service.py` | joins.db (SQLite) | `PRAGMA journal_mode=WAL` | WIRED | Line 89; WAL mode confirmed |
| `shared/background_removal.py` | `Pillow Image.convert('HSV')` | HSV color space conversion | WIRED | Line 94: `hsv_img = img.convert('HSV')` |
| `shared/background_removal.py` | `Image.putalpha` | alpha channel application from mask | WIRED | Line 106: `rgba.putalpha(mask)` |
| `shared/puzzle_image_service.py` | `shared/background_removal.py` | `from shared.background_removal import remove_background, DEFAULT_THRESHOLD` | WIRED | Line 22 |
| `shared/puzzle_image_service.py` | NLI IIIF (requests) | `_fetch_iiif_image` via `requests.get` | WIRED | Lines 138-150; `NLI_IIIF_BASE` duplicated (not from `web/services.py`, per Finding 8) |
| `scripts/preview_background_removal.py` | `shared/puzzle_image_service.py` | `from shared.puzzle_image_service import PuzzleImageService` | WIRED | Line 38 |
| `scripts/preview_background_removal.py` | `shared/background_removal.py` | `from shared.background_removal import remove_background, detect_background_color, DEFAULT_THRESHOLD, LOW_SATURATION_THRESHOLD` | WIRED | Lines 39-42 |

**No orphaned modules.** All shared modules are fully wired to each other and to tests.

---

## Requirements Coverage

| Requirement | Source Plan(s) | Description | Status | Evidence |
|-------------|---------------|-------------|--------|----------|
| BGRM-01 | 47-02, 47-03, 47-04 | Fragment images are automatically stripped of solid-color backgrounds (parchment shape visible) | SATISFIED | `remove_background()` strips solid-color bg via HSV segmentation; pipeline wired through `PuzzleImageService.resolve_fragment_image`; preview tool validated on real images |
| BGRM-02 | 47-01, 47-03, 47-04 | User can toggle between stripped and original image view | SATISFIED | `resolve_fragment_image(processed=False)` returns raw JPEG; preview tool has toggle button; `test_resolve_original_mode` passes |
| BGRM-03 | 47-02, 47-03, 47-04 | User can adjust the background removal threshold | SATISFIED | `threshold` parameter in `remove_background` and `resolve_fragment_image`; `PuzzleFragment.bg_removal_threshold` persists per-fragment threshold; preview tool has `QSlider` (5-150) |

**No orphaned requirements.** All 3 BGRM requirements from REQUIREMENTS.md are satisfied.

---

## Anti-Patterns Found

No blockers or warnings detected.

Scan of modified files (`shared/puzzle_model.py`, `shared/puzzle_service.py`, `shared/background_removal.py`, `shared/puzzle_image_service.py`, `scripts/preview_background_removal.py`):
- No TODO/FIXME/PLACEHOLDER comments
- No `return null`/`return {}`/`return []` stub patterns (graceful degradation returns correctly typed empties as intended behavior)
- No console.log-only handlers
- No unimplemented stubs

---

## Human Verification Required

### 1. Visual background removal quality across library types

**Test:** Run `python scripts/preview_background_removal.py`, select samples from the dropdown, adjust threshold slider
**Expected:** Parchment edges visible through transparency (checkerboard pattern); threshold slider visibly changes removal aggressiveness
**Why human:** Visual quality of alpha mask edges cannot be assessed programmatically
**Status:** APPROVED — checkpoint verified by user on 2026-03-16. CUL (NLI IIIF), AIU (NLI IIIF), Manchester (LUNA direct), and Cambridge (direct IIIF) all confirmed working. Optimal threshold for CUL noted as ~115 (higher than default 30).

---

## Test Suite Results

```
38 passed in 0.63s
  - test_puzzle_model.py:     6/6 passed
  - test_puzzle_service.py:  11/11 passed
  - test_background_removal.py: 11/11 passed
  - test_puzzle_image_service.py: 10/10 passed
```

All commits verified in git log:
- `90d9d138` feat(47-01): PuzzleDocument/PuzzleFragment data model
- `dd6a205a` feat(47-01): PuzzleService with joins.db CRUD + fragment index
- `b1a7eb3b` chore(47-02): add Pillow and numpy to requirements.txt
- `917e829f` test(47-02): failing tests for background removal engine (TDD RED)
- `1a2725df` feat(47-02): HSV background removal engine with low-saturation fallback
- `474ecd73` test(47-03): failing tests for puzzle image service (TDD RED)
- `401a1937` feat(47-03): puzzle image service with IIIF fetch + bg removal + disk cache
- `299abd43` feat(47-04): interactive background removal preview tool

---

## Summary

Phase 47 fully achieves its goal. All 16 observable truths are verified. All 10 artifacts exist, are substantive (not stubs), and are wired. All 3 BGRM requirements are satisfied with evidence. The 38-test suite passes in 0.63 seconds. The human verification checkpoint was completed and approved on 2026-03-16 confirming visual quality across 4 library source types (CUL, AIU, Manchester, Cambridge).

The shared foundation is ready for downstream canvas phases (Phase 48 desktop canvas, Phase 49 web canvas).

---

_Verified: 2026-03-16T04:00:00Z_
_Verifier: Claude (gsd-verifier)_
