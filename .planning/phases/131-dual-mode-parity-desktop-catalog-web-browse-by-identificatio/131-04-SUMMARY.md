---
phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio
plan: "04"
subsystem: ui
tags: [nicegui, library-filter, dual-mode, catalog, browse, fjms, safe_storage]

# Dependency graph
requires:
  - phase: 131
    plan: "01"
    provides: "Wave-0 RED tests for catalog dual-mode library filter"
  - phase: 131
    plan: "02"
    provides: "get_browse_library_facets instance method on FjmsService; library_mode kwarg on get_browse_results"
  - phase: 131
    plan: "03"
    provides: "desktop catalog dual-mode parity (sibling, non-overlapping files)"
  - phase: 130
    provides: "web /search dual-mode model: mode toggle, {mode,codes} dict shape, pluralized keys, migration, consume_incoming_filters"
provides:
  - "web Browse-by-Identification catalog library filter at dual-mode parity with web /search"
  - "consume_incoming_filters() accepts {mode,codes} dict backward-compatibly (bare list still -> show_only)"
  - "catalog->search handoff preserves mode: Hide arrives at /search as Hide"
  - "ALWAYS-true-facet shortlist via fjms.get_browse_library_facets(state.meta_mgr.get_library_for_id) -- no page-local fallback"
affects: [131-05, web-parallels, search-api]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Dict-shape persistence: catalog_library_filter stored as {mode, codes} never bare list"
    - "True-facet source: ALWAYS fjms.get_browse_library_facets instance method, callable mapper, no page-local fallback"
    - "D-06 3-branch migration: isinstance(raw, list)->show_only, isinstance(raw, dict)->read mode+codes, else->hide/[]"
    - "Mode-aware JS Apply guard: show_only blocks empty, hide allows empty (hide-nothing = show-all)"

key-files:
  created: []
  modified:
    - web/components/filter_panel.py
    - web/pages/catalog_browse.py

key-decisions:
  - "ALWAYS-true-facets: catalog is paginated (50/page) so page-local counts miss off-page libraries; use fjms.get_browse_library_facets unconditionally (Codex R3 F1)"
  - "Full-corpus callable mapper: state.meta_mgr.get_library_for_id passed directly as sys_id_to_library= (bound method object, NOT a dict or page-local _resolve_all)"
  - "No-count fallback only on facet-query failure: shortlist renders without counts when try/except catches the io_bound call; no other fallback path"
  - "Handoff dict shape: _build_incoming_filters carries {mode, codes} not bare list so catalog Hide->search preserves Hide (Codex R1 HIGH #3)"

patterns-established:
  - "catLib* JS namespace: separate from lib* (search) namespace so both pages coexist without collision"
  - "Mode-reset on toggle (D-04): catLibFilterSetMode unchecks all rows and re-runs Apply-enable"

requirements-completed: [DMF-08, DMF-10, DMF-12, DMF-13]

# Metrics
duration: 15min
completed: 2026-06-30
---

# Phase 131 Plan 04: Web Catalog Dual-Mode Library Filter Summary

**Catalog Browse-by-Identification gets full dual-mode (Show-only/Hide) library filter with ALWAYS-true facet counts from fjms.get_browse_library_facets, dict-shape persistence, mode-preserving catalog->search handoff, and real Phase-130 pluralized button keys**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-06-30T13:00Z
- **Completed:** 2026-06-30T13:09:07Z
- **Tasks:** 3 (Task 3 was verify+pass — no code changes needed)
- **Files modified:** 2

## Accomplishments

- `consume_incoming_filters()` in `filter_panel.py` now accepts both `{mode, codes}` dict (new) and bare-list (legacy show_only) — a catalog Hide selection arrives at `/search` as Hide, not Show-only
- Catalog library dialog fully redesigned: mode toggle (D-03/D-04), text-search input, sort-by-count/A-Z (DMF-12), ALWAYS-true-facet shortlist from `fjms.get_browse_library_facets` (callable full-corpus mapper, no page-local fallback), expand-all A-Z section using `library_codes_with_manuscripts()` (DMF-13)
- 3-state button uses REAL Phase-130 pluralized keys: neutral/show-only/hide states keyed on `current_library_mode`
- Hide mode threaded server-side via `library_mode=` kwarg on `fjms.get_browse_results`
- All persistence uses dict shape `{'mode', 'codes'}` — NEVER a bare list (both apply + clear sites)
- D-06 3-branch migration in restore block handles legacy list, new dict, and garbage inputs
- SEED-023 PGP/Editions filter args to `fjms.get_browse_results` unchanged and composing correctly

## Task Commits

1. **Task 1: consume_incoming_filters dict branch** - `7213e49a` (feat)
2. **Task 2+3: catalog dual-mode dialog + guard verify** - `2fd729d1` (feat)

## Files Created/Modified

- `web/components/filter_panel.py` — Added dict branch to `consume_incoming_filters()` for `{mode,codes}` handoff from catalog; bare-list legacy preserved as show_only
- `web/pages/catalog_browse.py` — Full dual-mode overhaul: imports, restore migration, mode cell, facets cell, JS extensions, 3-state button, redesigned dialog, dict-persist apply, clear sites, mode-kwarg in fetch, facet-fetch in refresh_results, {mode,codes} handoff

## Decisions Made

- ALWAYS-true-facets: the catalog is paginated (PAGE_SIZE=50) so page-local counts miss off-page libraries even with NO filters active; `fjms.get_browse_library_facets` is called unconditionally on every `refresh_results` — not just when the dialog is opened
- Full-corpus callable: `state.meta_mgr.get_library_for_id` passed as the `sys_id_to_library=` bound method directly (a callable), matching the Codex R3 F3 contract; NOT a page-local `_resolve_all` dict
- Facet try/except: wraps the `io_bound` call, leaves `current_library_facets['value'] = {}` on failure; dialog renders shortlist without counts in that degraded path — no other fallback path exists (Codex R3 F1)
- `_build_incoming_filters` now builds `{'mode': ..., 'codes': [...]}` instead of a bare list; `consume_incoming_filters` handles both shapes for full backward compatibility

## Deviations from Plan

None — plan executed exactly as written. All acceptance criteria met on first pass.

## Issues Encountered

None.

## Known Stubs

None — all data paths are wired. True-facet counts come from the DB; the no-count path is only on facet-query failure (try/except).

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes. All new filter paths sanitize via `sanitize_library_codes()` + inline `c != 'LOCAL'` guard + `mode` validation. Persistence routes through `web/safe_storage.py` chokepoint only.

## Self-Check: PASSED

- `web/components/filter_panel.py` exists and modified
- `web/pages/catalog_browse.py` exists and modified
- Task 1 commit `7213e49a` found in git log
- Task 2+3 commit `2fd729d1` found in git log
- 28 tests pass: `test_catalog_dual_mode_library_filter.py` (15) + guards (13)
- SEED-023 `test_catalog_availability_filter.py` (4) pass
- `test_fjms_browse_library_mode.py` (18) pass
- `python -m ruff check web/pages/catalog_browse.py web/components/filter_panel.py` clean

## Next Phase Readiness

- Plan 05 (web `/parallels` library filter) can proceed — this plan does not touch `/parallels`
- The `consume_incoming_filters` fix is backward-compatible; existing callers unaffected
- Render-smoke verification (live browser) deferred to human UAT: open `/catalog`, open library filter dialog, confirm true-facet counts, toggle Hide/Show-only, sort, text-search, Apply, reload, "Search in these results" carries mode

---
*Phase: 131-dual-mode-parity-desktop-catalog-web-browse-by-identificatio*
*Completed: 2026-06-30*
