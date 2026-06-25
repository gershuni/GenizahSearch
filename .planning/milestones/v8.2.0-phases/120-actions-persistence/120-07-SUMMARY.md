---
phase: 120-actions-persistence
plan: "07"
subsystem: joins-lab-compare-workbench
tags: [joins-lab, compare, browse-in-compare, set-as-anchor, image-prefetch, vs-toggle, off-loop, metadata-prefetch, catalog-dialog, SEED-008]
dependency_graph:
  requires: ["120-04", "120-06"]
  provides: ["D-07", "D-08", "D-09", "D-10", "D-12"]
  affects: [web/pages/joins_lab.py, web/components/compare_modal.py, web/components/candidate_grid.py, web/components/catalog_dialog.py, genizah_translations.py]
tech_stack:
  added: []
  patterns: [run.io_bound-off-loop, SEED-008-RuntimeError-guard, asyncio.ensure_future-fire-and-forget, js_handler-window.open-new-tab, anchor-generation-guard, bounded-prefetch-pool, rich-resolver-path]
key_files:
  created: []
  modified:
    - web/pages/joins_lab.py
    - web/components/compare_modal.py
    - web/components/candidate_grid.py
    - web/components/catalog_dialog.py
    - genizah_translations.py
    - tests/test_compare_modal.py
    - tests/test_joins_lab.py
decisions:
  - "Browse-in-Compare opens via js_handler window.open (new tab) — NOT ui.navigate.to which navigates the current client session"
  - "Candidate-pane Browse button is rebuilt on each _fill_candidate flip (URL changes with candidate); anchor-pane is static"
  - "_metadata_prefetcher_sync is a named sync def passed as metadata_prefetcher= to create_compare_modal; dispatched via run.io_bound inside _on_show (R2-H3 — never called directly on event loop)"
  - "Image prefetch uses RICH resolver path (get_service().get_browse_page + resolve_external_images + resolve_image_url), NOT executor.get_browse_page which is a narrow text dict (M3)"
  - "on_candidate_change callback lets compare_modal notify joins_lab of candidate flips so adjacent prefetch fires from _fill_candidate without polling"
  - "VS probe calls svc.get_suggestions (METHOD — L1) not a free-function import; probe result bool drives set_visibility (not disable)"
metrics:
  duration: "~2.5 hours (continuation session)"
  completed: "2026-06-21"
  tasks_completed: 3
  files_changed: 7
  tests_added: 46
---

# Phase 120 Plan 07: D-07/D-08/D-09/D-10/D-12 Compare+Workbench Enhancements Summary

**One-liner:** Set-as-Anchor workbench pivot + Browse-in-Compare per-pane buttons + off-loop FJMS/PGP info buttons (R2-H3 prefetched catalog_detail skips sync fetch on open) + bounded RICH-resolver image prefetch + VS toggle hide probe.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | D-07 Set-as-Anchor + D-08 Browse-in-Compare + D-09 off-loop metadata info buttons | `003b8061` | compare_modal.py, candidate_grid.py, catalog_dialog.py, joins_lab.py, genizah_translations.py, tests |
| 2 | D-10 Compare image prefetch — bounded 5-slot off-loop pool via RICH resolver | `cc86647e` | joins_lab.py, compare_modal.py, tests |
| 3 | D-12/L1 Hide VS toggle when anchor has zero VS look-alikes | `dddde638` | joins_lab.py, tests |

## What Was Built

### Task 1 — Set-as-Anchor (D-07) + Browse-in-Compare (D-08) + Info Buttons (D-09/H3/R2-H3)

**D-07 Set-as-Anchor:**
- `candidate_grid.py`: `on_set_as_anchor` + `on_add_as_join` params added to both `_create_candidate_card` and `create_candidate_grid`; push_pin icon button per card fires `on_set_as_anchor(cand.sys_id)`
- `joins_lab.py`: `_on_set_as_anchor(sys_id)` dispatches `asyncio.ensure_future(load_anchor(sys_id))` (async load_anchor, sync callback compatible); passed as `on_set_as_anchor=_on_set_as_anchor` to `create_candidate_grid`
- Existing `load_anchor` re-anchor flow clears `_triage` per Phase-119 D-11

**D-08 Browse-in-Compare:**
- `compare_modal.py`: anchor pane header gets a static `open_in_new` icon button built from `build_browse_url(anchor_cand)` via `js_handler='() => { window.open(url, "_blank"); }'`
- Candidate pane shelfmark row (`_cand_shelfmark_row_ref`) is cleared and rebuilt in `_fill_candidate` on each candidate flip so the Browse URL stays in sync
- Both buttons carry `aria-label=` (UI-SPEC §4 icon-only exception)
- No `ui.navigate.to` — opens exclusively in a new tab via `js_handler` (T-119-09 propagation rule)

**D-09/H3/R2-H3 Compare Info Buttons:**
- `catalog_dialog.py`: `show_catalog_dialog` extended with `catalog_detail=None` kwarg (R2-H3) — when provided, skips the internal `fjms_service.get_catalog_detail()` synchronous SQLite call; backward-compatible for Browse callers
- `compare_modal.py`: `metadata_prefetcher: Optional[Callable] = None` param added; `_anchor_info_row_ref` + `_cand_info_row_ref` hidden rows per pane; `_populate_pane_info_row(row_el, sys_id, shelfmark, meta)` helper builds FJMS Catalog + PGP/Bibliography chip buttons from prefetched meta (disabled when no data)
- `_on_show` uses `asyncio.gather(run.io_bound(metadata_prefetcher, anchor_sid), run.io_bound(metadata_prefetcher, cand_sid))` to fetch both panes concurrently off-loop; then populates the info rows; full SEED-008 guard wraps `_on_show` body
- `joins_lab.py`: `_metadata_prefetcher_sync(sys_id)` calls `get_fjms_service(thread_safe=True).get_bibliography()` + `.get_catalog_detail()`; passed as `metadata_prefetcher=_metadata_prefetcher_sync` to `create_compare_modal`

**Translations added (Phase 120-07 block):**
- Set as Anchor / הגדר כעוגן
- Pivot the workbench: make this fragment the new anchor / הפוך קטע זה לעוגן החדש
- Open anchor in Browse (new tab) / פתח עוגן בדפדפן (לשונית חדשה)
- Open candidate in Browse (new tab) / פתח מועמד בדפדפן (לשונית חדשה)
- FJMS Catalog / קטלוג FJMS
- View FJMS catalog data for this fragment / הצג נתוני קטלוג FJMS עבור קטע זה
- No FJMS catalog data for this fragment / אין נתוני קטלוג FJMS עבור קטע זה
- PGP / Bibliography / PGP / ביבליוגרפיה
- View PGP and bibliography data for this fragment / הצג נתוני PGP וביבליוגרפיה עבור קטע זה
- No PGP data for this fragment / אין נתוני PGP עבור קטע זה

### Task 2 — Compare Image Prefetch (D-10/M3)

- `joins_lab.py`: `_PREFETCH_SLOTS = 5` constant; `_prefetch_cache: dict`, `_prefetch_running: set`, `_prefetch_anchor_gen: dict` state
- `_prefetch_image_sync(sys_id)` sync function uses RICH resolver path: `get_service().get_browse_page(sys_id)` + `resolve_external_images(sys_id)` + `resolve_image_url(...)` — returns proxy img_url (no executor.get_browse_page — M3)
- `_prefetch_one(sys_id, my_gen)` async coroutine: SEED-008 guard + 2-point generation check (before+after await); stores to `_prefetch_cache`, removes from `_prefetch_running`
- `_schedule_image_prefetch(center_idx)` schedules offsets -2,-1,+1,+2, bounded to 5 concurrent, skips cached/in-flight
- `load_anchor` bumps `_prefetch_anchor_gen`, clears `_prefetch_cache` and `_prefetch_running` on re-anchor
- `_open_compare` fires initial prefetch for the opened candidate + passes `on_candidate_change=_schedule_image_prefetch`
- `compare_modal.py`: `on_candidate_change: Optional[Callable] = None` param; `_fill_candidate` calls `on_candidate_change(_state['idx'])` on each flip

### Task 3 — Hide VS Toggle (D-12/L1)

- `joins_lab.py`: `_probe_vs_data_and_update_toggle(anchor_sid, my_anchor_gen)` async coroutine: dispatches `run_vs_probe` (sync closure calling `svc.get_suggestions(anchor_sid, 1)` as a METHOD — L1) via `run.io_bound`; 2-point generation guard; `vs_el.set_visibility(bool(probe_result))`; SEED-008 guarded
- `load_anchor` fires `asyncio.ensure_future(_probe_vs_data_and_update_toggle(sys_id, _my_anchor_gen))` at end of anchor swap
- Empty probe → toggle HIDDEN (not disabled — absent from flex row, no placeholder); non-empty → visible (Phase-119 contract)

## Test Results

```
236 passed (test_compare_modal.py + test_joins_lab.py + test_joins_lab_off_loop.py +
           test_no_server_side_stop_propagation.py + test_no_raw_storage_access.py)
ruff: All checks passed on all 7 modified files
Phase-87 multitenant invariant: CLEAN (allowlist stays [])
```

New tests added: 46 (25 in Task 1, 11 in Task 2, 10 in Task 3)

## Deviations from Plan

None — plan executed exactly as written. All three tasks follow the plan's specified patterns (off-loop discipline, SEED-008, generation guards, RICH resolver path, js_handler for new-tab navigation, set_visibility for hide vs disable).

## Known Stubs

None — all three features are fully wired end-to-end. The Compare info buttons (D-09) are rendered disabled when the off-loop metadata fetch returns no data for a fragment, which is the correct graceful-degradation behavior for fragments without FJMS catalog entries.

## Threat Flags

No new network endpoints introduced. All image prefetch traffic routes through the existing proxy (`resolve_image_url`) + Phase-98 NLI circuit breaker. The metadata prefetcher accesses only local SQLite via `get_fjms_service(thread_safe=True)`. Browse-in-Compare opens via `js_handler` + `window.open` (client-side only — no server-side URL handling).

## Self-Check: PASSED

- [x] web/components/compare_modal.py: build_browse_url present; aria-label on Browse buttons; _populate_pane_info_row defined; metadata_prefetcher + on_candidate_change params; SEED-008 in _on_show; _cand_shelfmark_row_ref rebuilt in _fill_candidate
- [x] web/components/candidate_grid.py: on_set_as_anchor + on_add_as_join params; push_pin button
- [x] web/components/catalog_dialog.py: catalog_detail=None param; skips get_catalog_detail when provided
- [x] web/pages/joins_lab.py: _on_set_as_anchor; _metadata_prefetcher_sync; _prefetch_image_sync (RICH path); _prefetch_one (SEED-008+gen); _schedule_image_prefetch (bounded); _probe_vs_data_and_update_toggle (L1 method, set_visibility); load_anchor fires all three probes/clears
- [x] genizah_translations.py: Phase 120-07 block (10 new strings)
- [x] tests/test_compare_modal.py: TestBrowseInCompare (5) + TestCompareInfoButtons (9) + TestCatalogDialogPrefetchParam (2)
- [x] tests/test_joins_lab.py: TestSetAsAnchor (5) + TestMetadataPrefetcher (6) + TestImagePrefetch (11) + TestVSToggleHide (10) = 32
- [x] Commits 003b8061, cc86647e, dddde638 exist in git log
- [x] 236 targeted tests GREEN
- [x] ruff clean on all 7 modified files
- [x] test_no_raw_storage_access.py: CLEAN (Phase-87 allowlist stays [])
- [x] test_no_server_side_stop_propagation.py: CLEAN (no Python-side stop_propagation)
