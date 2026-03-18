---
phase: quick
plan: 260318-kk1
subsystem: puzzle
tags: [puzzle, external-libraries, manchester, oxford, jts, cambridge, image-loading, desktop, web]
dependency_graph:
  requires: []
  provides:
    - PuzzleFragment.image_url/external_provider/page_index fields
    - PuzzleImageService._fetch_direct_url for non-NLI IIIF
    - /api/puzzle_ext_image endpoint
    - /api/puzzle_folios images_ext fallback
  affects:
    - shared/puzzle_model.py
    - shared/puzzle_image_service.py
    - shared/puzzle_service.py
    - shared/puzzle_export.py
    - gui_threads.py
    - genizah_app.py
    - web/pages/puzzle.py
    - web/pages/browse.py
    - web/api.py
tech_stack:
  added: []
  patterns:
    - External IIIF canvas URL fetching (_fetch_direct_url)
    - Provider-dispatch helper (_fetch_provider_image)
    - URL-based cache key for non-NLI images
    - page:N URL format for browse-to-puzzle deep link
key_files:
  created: []
  modified:
    - shared/puzzle_model.py
    - shared/puzzle_image_service.py
    - shared/puzzle_service.py
    - shared/puzzle_export.py
    - gui_threads.py
    - genizah_app.py
    - web/pages/puzzle.py
    - web/pages/browse.py
    - web/api.py
    - tests/test_puzzle_model.py
    - tests/test_puzzle_service.py
    - tests/test_puzzle_image_service.py
    - tests/test_puzzle_export.py
    - tests/test_puzzle_web_api.py
decisions:
  - External library images use /api/puzzle_ext_image (not extension); extension only needed for NLI
  - Cache key for external images uses safe_filename(url[:120]) to avoid filesystem issues
  - _fetch_provider_image helper delegates to existing cambridge/manchester/jts/oxford proxy endpoints
  - browse-to-puzzle deep link uses page:N format (not fl_id) for external library pages
  - Folio index matching uses label fallback when fl_id empty (non-NLI)
metrics:
  duration: ~45min
  completed: "2026-03-18"
  tasks_completed: 4
  files_changed: 14
---

# Quick Task 260318-kk1 Summary

Fix puzzle image loading for non-NLI libraries (Manchester, Oxford, JTS, Cambridge) by extending the fragment data model, image service, desktop/web canvas pipelines, and persistence layer to support direct IIIF canvas URL fetching.

## What Was Built

External library images (Manchester, Oxford, JTS, Cambridge) could not be added to the puzzle canvas because all image resolution paths assumed NLI FL IDs. These libraries use IIIF canvas URLs stored in `enrich_metadata` `images_ext`, not FL IDs. All image paths now support both NLI FL IDs and direct canvas URLs.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Extend desktop puzzle to load non-NLI images | 761d929e | shared/puzzle_model.py, puzzle_image_service.py, gui_threads.py, genizah_app.py |
| 2 | Fix web puzzle image loading for non-NLI libraries | 1eb81ba5 | web/api.py, web/pages/puzzle.py |
| 3 | Fix save/persist, export, and browse-to-puzzle | 69cc3ba7 | puzzle_service.py, puzzle_export.py, puzzle.py, browse.py, genizah_app.py |
| 4 | Add tests for external fragment support | 4e6bb2e3 | 5 test files (447 lines added) |

## Changes Made

### shared/puzzle_model.py
Added three new fields to `PuzzleFragment` with backward-compat defaults:
```python
image_url: str = ''          # Direct IIIF canvas URL (empty for NLI)
external_provider: str = ''  # 'cambridge', 'manchester', 'oxford', 'jts', or ''
page_index: int = -1         # 0-based page index into images_ext (-1 = use fl_id)
```

### shared/puzzle_image_service.py
- `resolve_fragment_image()` accepts `image_url=''` parameter; uses URL-based cache key (`safe_filename(url[:120])`) when fl_id is empty
- Added `_fetch_direct_url(image_url, size)`: appends `/full/{size},/0/default.jpg` to canvas base URLs, or uses URL as-is if it already contains `/full/`
- Updated module-level `resolve_fragment_image()` convenience function to forward `image_url`

### gui_threads.py
- `PuzzleMetaLoaderThread.run()`: three-tier fallback: NLI `images_nli` -> external `images_ext` -> NLI `fetch_nli_data`. External entries emit folio dicts with `image_url`/`page_index`/`external_provider`
- `PuzzleImageLoaderThread`: accepts `image_url=''` param, forwards to `resolve_fragment_image`, emits `fl_id or image_url` as emit ID

### genizah_app.py
- `add_fragment()`: accepts `image_url`, `external_provider`, `page_index`; passes them to `PuzzleFragment` and `PuzzleImageLoaderThread`
- `_on_meta_resolved()`: passes external fields from folio entry to `add_fragment`
- `_flip_recto_verso()` / `_flip_entire_puzzle()`: folio matching by label when fl_id empty; update all external fields after navigation; pass `image_url` to loader
- `_load_document()`: passes `image_url` to `PuzzleImageLoaderThread`
- `add_to_puzzle()`: passes external fields from cached folio list

### web/api.py
- Added `_fetch_provider_image(provider, sys_id, page)` helper delegating to existing proxy endpoints
- Added `GET /api/puzzle_ext_image` endpoint: fetch raw bytes via provider proxy, apply BG removal, cache processed result with `{provider}_{sys_id}_page{N}` cache key
- `puzzle_folios` endpoint: falls through to `enrich_metadata images_ext` when NLI manifest returns empty

### web/pages/puzzle.py
- `_resolve_folios()`: NLI manifest first, then `enrich_metadata images_ext` fallback
- `_add_fragment_by_sys_id()`: routes to `/api/puzzle_ext_image` for external_provider; populates `pending_fragment_meta` with external fields
- `_loadImageWithFallbacks()` JS: step 0 for `external_provider` images using `/api/puzzle_ext_image`; fallback to direct `image_url` if proxy fails
- `navigateFolio()` JS: propagates `external_provider`/`page_index`/`image_url` in `folioMeta`
- `loadFolios()` JS: matches by label when fl_id empty (non-NLI folio navigation)
- `load_document()`: uses `/api/puzzle_ext_image` URL + full `js_meta` for non-NLI fragments; `puzzle_meta` includes external fields
- `build_fragments_list()`: `PuzzleFragment` construction includes `image_url`/`external_provider`/`page_index`
- `auto_add()`: parses `page:N` format; handles external_provider case

### web/pages/browse.py
- `add_to_puzzle()`: passes `page:N` format when viewing external library images (active_source != 'nli')

### shared/puzzle_service.py
- `save_document()`: includes `image_url`/`external_provider`/`page_index` in `fragments_json` serialization

### shared/puzzle_export.py
- `compose_puzzle_export()`: passes `image_url` to `resolve_fragment_image`; external fragments now render in export/thumbnail/publish

## Tests Added (70 pass, 3 pre-existing failures)

- `test_puzzle_model.py`: 4 new tests (defaults, roundtrip, backward compat, NLI defaults)
- `test_puzzle_service.py`: 3 new tests (external save/load, mixed NLI+external, old DB backward compat)
- `test_puzzle_image_service.py`: 5 new tests (image_url fetch, caching, None return, URL construction)
- `test_puzzle_export.py`: 2 new tests (external fragment renders, image_url passed to service)
- `test_puzzle_web_api.py`: 12 new tests (folios fallback, ext_image endpoint, auto_add parsing)

## Deviations from Plan

None — plan executed exactly as written.

## Self-Check: PASSED

All key files exist on disk. All 4 task commits verified in git log:
- 761d929e: feat(quick-260318-kk1): extend desktop puzzle to load non-NLI images
- 1eb81ba5: feat(quick-260318-kk1): fix web puzzle image loading for non-NLI libraries
- 69cc3ba7: feat(quick-260318-kk1): fix save/persist, export, and browse-to-puzzle for external fragments
- 4e6bb2e3: test(quick-260318-kk1): add tests for external fragment support

70 tests pass. 3 pre-existing failures unrelated to this task.
