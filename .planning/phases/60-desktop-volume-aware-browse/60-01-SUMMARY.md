---
phase: 60-desktop-volume-aware-browse
plan: 01
subsystem: desktop-ui
tags: [pyqt6, iiif, multi-ie, volume-browse, qthread]

requires:
  - phase: 58-ie-volume-infrastructure
    provides: ie_volume_map.json, get_volumes_for_sys_id(), get_volume_pages(), volume-aware get_browse_page()
  - phase: 59-web-volume-browse
    provides: web volume selector pattern, volume_ie state management approach

provides:
  - Desktop Browse tab volume selector (QComboBox) for multi-IE manuscripts
  - resolve_volume_suffix() centralized helper for IE-to-suffix mapping
  - fetch_volume_manifest() lightweight manifest fetch for volume switches
  - Suffix-aware fetch_iiif_manifest() with separate _iiif_manifest_cache
  - VolumeManifestThread for async image refresh on volume switch
  - Search-to-Browse IE propagation via raw_header parsing
  - ResultDialog volume_ie state with cross-manuscript reset

affects: [desktop-browse, search-browse-integration, result-dialog]

tech-stack:
  added: []
  patterns: [volume_ie state tracking, generation-guarded async image refresh, blockSignals combo pattern]

key-files:
  created: []
  modified:
    - genizah_core.py
    - genizah_app.py
    - gui_threads.py

key-decisions:
  - "Separate _iiif_manifest_cache for non-primary suffixes to protect nli_cache"
  - "Lightweight VolumeManifestThread instead of re-running full enrichment on volume switch"
  - "Auto-switch to NLI source on volume change since external images are per-shelfmark"

patterns-established:
  - "Volume state absorption: browse_render_page auto-detects IE from page data for multi-IE manuscripts"
  - "Generation guard with (sid, volume_ie, gen) tuple for rejecting stale async results"

requirements-completed: [DSK-01, DSK-02, DSK-03]

duration: 29min
completed: 2026-03-31
---

# Phase 60 Plan 01: Desktop Volume-Aware Browse Summary

**Desktop PyQt6 browse parity with web: volume selector, suffix-aware IIIF, search-to-browse IE propagation for 3,193 multi-IE manuscripts**

## Performance

- **Duration:** 29 min
- **Started:** 2026-03-31T17:16:34Z
- **Completed:** 2026-03-31T17:45:56Z
- **Tasks:** 5 (of 6; Task 6 is manual verification)
- **Files modified:** 3

## Accomplishments
- Volume selector QComboBox in Browse nav bar shows "Volume N (M pages)" for multi-IE manuscripts
- Suffix-aware IIIF manifest fetching with separate cache prevents nli_cache pollution
- Search-to-Browse propagates IE from result header for correct volume landing
- ResultDialog tracks volume_ie with cross-manuscript reset
- All manifest fetches run on worker threads (no UI freezing)
- Auto-switches to NLI image source on volume change for manuscripts with external images

## Task Commits

All tasks committed atomically in a single commit:

1. **Task 1: Suffix parameter + resolve_volume_suffix** - `849c28b2` (feat)
2. **Task 2: Track volume_ie state** - `849c28b2` (feat)
3. **Task 3: Volume selector QComboBox** - `849c28b2` (feat)
4. **Task 4: Volume-aware images** - `849c28b2` (feat)
5. **Task 5: Search/ResultDialog propagation** - `849c28b2` (feat)

## Files Created/Modified
- `genizah_core.py` - resolve_volume_suffix(), fetch_iiif_manifest(suffix=), _iiif_manifest_cache, fetch_volume_manifest(), enrich_metadata(suffix=)
- `genizah_app.py` - current_browse_volume_ie state, combo_browse_volume UI, _on_browse_volume_changed(), _refresh_browse_images_for_volume(), _on_volume_manifest_loaded(), volume_ie in all get_browse_page() calls, open_result_in_browse IE extraction, ResultDialog current_volume_ie
- `gui_threads.py` - EnrichMetadataThread suffix parameter, VolumeManifestThread class

## Decisions Made
- Separate `_iiif_manifest_cache[(sys_id, suffix)]` to avoid polluting canonical `nli_cache` with secondary IE data
- Lightweight `VolumeManifestThread` for volume switches avoids re-running full enrichment (MARC, FJMS, crossref)
- Auto-switch viewer to NLI source on volume change because external images (Cambridge, Manchester, JTS) are per-shelfmark, not per-IE
- Volume selector hidden during View All mode to avoid confusion (View All is manuscript-scoped)
- Generation guard captures `(sid, volume_ie, gen)` tuple to reject stale async results from rapid switching

## Deviations from Plan

None - plan executed exactly as written. Both HIGH reviewer concerns (UI thread blocking, missing image refresh trigger) were already addressed in the revised plan.

## Issues Encountered
None

## User Setup Required
None - no external service configuration required.

## Known Stubs
None - all functionality is fully wired.

## Next Phase Readiness
- Desktop volume-aware browse is complete and matches web functionality
- Phase 61 (polish/edge cases) can proceed if needed
- Manual verification (Task 6) should be performed with EVR ARAB I 2939 (3 volumes) and single-IE manuscripts

## Self-Check: PASSED

- genizah_core.py: FOUND
- genizah_app.py: FOUND
- gui_threads.py: FOUND
- SUMMARY: FOUND
- Commit 849c28b2: FOUND
- resolve_volume_suffix importable: OK
- VolumeManifestThread importable: OK
- EnrichMetadataThread has suffix param: OK
- All 3 files compile: OK
- 192+ tests pass: OK

---
*Phase: 60-desktop-volume-aware-browse*
*Completed: 2026-03-31*
