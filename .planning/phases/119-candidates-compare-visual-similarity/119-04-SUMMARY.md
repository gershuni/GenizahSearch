---
phase: 119-candidates-compare-visual-similarity
plan: 04
subsystem: web-joins-lab
tags: [joins-lab, visual-similarity, enrichment, candidate-surface, compare-modal, triage, off-loop, nicegui]

# Dependency graph
requires:
  - phase: 119-candidates-compare-visual-similarity
    plan: 01
    provides: badge_and_tooltip() + off-loop AST guard + RED test scaffolds
  - phase: 119-candidates-compare-visual-similarity
    plan: 02
    provides: candidate_grid — paginated grid + table + filter dialog + triage state
  - phase: 119-candidates-compare-visual-similarity
    plan: 03
    provides: create_compare_modal() — full-screen two-pane Compare modal

provides:
  - _map_vs_suggestions_to_candidates() — maps VS service output to Candidate objects (D-05, Pitfall 4 honored)
  - _apply_vs_merge() — D-04 conditional model: intersection/union/text-only pure function
  - _get_enrichment_sys_ids() — deduped sys_id list for enrichment batch (covers full filtered set, D-16)
  - _fetch_vs_candidates(anchor_sid) — off-loop via run.io_bound(run_vs_core), graceful []
  - _enrich_candidates(sys_ids) — off-loop via run.io_bound(run_enrich_core), graceful {}
  - Phase-119 page state (25 mutable-dict containers) in create_joins_lab_page
  - _open_compare() — opens Compare modal per-image (uid/sys_id+page, F2, Pitfall 6)
  - _do_enrich_and_update() / _re_render_candidates_surface() — async enrichment + re-render
  - _do_vs_fetch_and_update() / _on_vs_toggle_change() — VS toggle ON/OFF with 4 explicit states
  - F1 VS-only empty-builder branch in execute_joins_search Step-1
  - Full Phase-119 Step-9 replacement (VS merge + detect_self_match + surface + enrichment)
  - VS toggle wired to load_anchor re-anchor invalidation
  - 128 tests passing + 7 xpassed across full Plan-04 acceptance suite
  - Off-loop guard (test_joins_lab_off_loop.py) load-bearing on run_vs_core + run_enrich_core

affects:
  - web/pages/joins_lab.py — primary integration file (451 net insertions)
  - web/components/candidate_grid.py — extended create_candidate_grid signature (Rule 1 fix)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "off-loop VS + enrichment via run.io_bound(run_vs_core) / run.io_bound(run_enrich_core) — CI-guarded literal closure names"
    - "_apply_vs_merge pure function — D-04 conditional model (intersection/union/text-only)"
    - "per-anchor generation guard in _do_vs_fetch_and_update — stale fetch discard when anchor changes mid-flight"
    - "fire-and-forget enrichment (asyncio.ensure_future) after Step-9 render — material/dims populate asynchronously (Pitfall 7)"
    - "F1 VS-only empty-builder branch: bypasses text early return when VS is ON + builder empty"
    - "extended create_candidate_grid signature: enrichment/filter_state/anchor_sys_id/on_page_change/on_filter_open — pagination buttons now functional"

key-files:
  created: []
  modified:
    - web/pages/joins_lab.py
    - web/components/candidate_grid.py
    - tests/test_vs_adapter.py (xfail → green)
    - tests/test_candidate_enrichment.py (xfail → green)

key-decisions:
  - "Tasks 2+3 committed as a single atomic commit — the NiceGUI closure captures (VS state, triage state, candidates_container, builder refs) form an interdependent graph; splitting would have left the page in an incomplete state between commits"
  - "create_candidate_grid extended with 6 Phase-119 integration kwargs (Rule 1 fix) — pagination buttons now fire on_page_change; Filter button shown when on_filter_open provided; enrichment passed through but not yet rendered per-card (table view already shows it via _make_table_rows)"
  - "make_triage_state + paginate imports removed from joins_lab.py (ruff F401) — TriageState accessed via local import in _render_candidates_surface; paginate used internally by create_candidate_grid"
  - "D-13 divergence honored: detect_self_match() runs but result is not surfaced — NO self-match banner"
  - "VS status label shows 4 explicit states: Loading / VS-count / empty-intersection / no-VS-data (never blank)"

requirements-completed: [VSM-01, VSM-02, CND-08, CND-04, CND-05, CND-07, CMP-01, CMP-02, CMP-03, CND-03, CND-06]

# Metrics
duration: 35min
completed: 2026-06-19
---

# Phase 119 Plan 04: Candidates Compare Visual Similarity — Integration Summary

**VS adapter (run_vs_core off-loop) + enrichment batch (run_enrich_core off-loop) + D-04 conditional merge + full candidate surface + Compare modal + VS toggle wired end-to-end into joins_lab.py; 128 tests passing, Phase-87 invariant intact**

## Performance

- **Duration:** ~35 min (continuation from context-exhausted session)
- **Completed:** 2026-06-19
- **Tasks:** 3 (Task 1 committed in prior session; Tasks 2+3 committed as one atomic commit in this session)
- **Files modified:** 4 (joins_lab.py primary; candidate_grid.py Rule-1 fix; test scaffolds made green)

## Accomplishments

**Task 1 (committed `abeaf359` in prior session):** Added module-level pure helpers and off-loop async helpers before `create_joins_lab_page`:
- `_map_vs_suggestions_to_candidates(raw)` — maps `{alma_id, svm_score, rank}` to `Candidate(sys_id=alma_id, via_vs=True, vs_rank=rank, vs_score=svm_score)`. The returned `alma_id` is the partner `alma_id_b` — it IS the candidate. svm_score → vs_score, rank → vs_rank (NOT swapped, Pitfall 4).
- `_apply_vs_merge(text, vs, vs_on, builder_has_query)` — pure D-04 conditional model: intersection (via_text AND via_vs) / union (merge_candidates([], vs)) / text-only (via_text only, look-alikes keep badge).
- `_get_enrichment_sys_ids(candidates)` — deduped sys_id list for batch enrichment (full filtered set, D-16).
- `async _fetch_vs_candidates(anchor_sid)` — `run_vs_core` closure dispatched via `run.io_bound`; returns [] on unavailable/error.
- `async _enrich_candidates(sys_ids)` — `run_enrich_core` closure dispatched via `run.io_bound`; returns {} on empty/unavailable/error.
- Made `tests/test_vs_adapter.py` and `tests/test_candidate_enrichment.py` green (7 xpassed).
- Off-loop guard in `tests/test_joins_lab_off_loop.py` is now load-bearing (not skipped).

**Tasks 2+3 (committed `a58eec86`):** Full Phase-119 wiring of the candidate surface into the page:

*Page state (25 new mutable-dict containers, all in-memory — NEVER written to app.storage.user):*
`_triage`, `_selected`, `_filter_state`, `_enrichment`, `_enrichment_ready`, `_view_mode`, `_current_page`, `_all_candidates`, `_filtered_candidates`, `_vs_on`, `_vs_candidates`, `_vs_anchor_sid`, `_vs_loading`, `_vs_switch_ref`, `_vs_status_ref`, `_triage_state_ref`

*Candidate surface helpers:*
- `_on_triage_verdict(sys_id, verdict)` / `_on_compare_verdict(sys_id, verdict)` — write shared triage dict + restyle via TriageState (D-11)
- `_open_compare(cand)` — builds anchor Candidate from `_anchor_state`; opens `create_compare_modal` with FULL clicked candidate, filtered list, shared triage + on_verdict callback (F2, Pitfall 6)
- `_do_enrich_and_update(candidates_snap)` — fire-and-forget: awaits `_enrich_candidates`, stores `_enrichment`, sets `_enrichment_ready=True`, calls `_re_render_candidates_surface` (Pitfall 7)
- `_re_render_candidates_surface()` / `_render_candidates_surface()` — recompute compute_filtered + call `create_candidate_grid` with all Phase-119 kwargs
- `_on_page_change(page)` — page nav WITHOUT resetting triage (D-08)
- `_on_filter_open()` — opens `open_filter_dialog` with enrichment-ready gate (Pitfall 7)

*VS toolbar widget:* `ui.switch(tr('Visual Similarity'))` + inline status label in the search toolbar row.

*VS helpers:*
- `_update_vs_status_label()` — 4 explicit states: Loading / VS-count (amber) / empty-intersection (amber notice) / no-VS-data (muted)
- `async _do_vs_fetch_and_update(anchor_sid)` — off-loop VS fetch + stale-anchor guard + recompute + re-render; explicit empty surface affordance when VS returns []
- `_on_vs_toggle_change()` — ON: fetch if uncached / recompute if cached; OFF: text-only recompute immediately

*load_anchor Phase-119 additions:* Clears triage/candidates/enrichment/VS state on re-anchor; schedules `_do_vs_fetch_and_update` when toggle is ON.

*Step-1 F1 VS-only empty-builder branch:* When builder empty AND `_vs_on['value']` is True — bypasses text early-return; fetches VS off-loop via `_fetch_vs_candidates`; renders `merge_candidates([], vs)` with generation guard + explicit empty affordance; fires enrichment off-loop.

*Step-9 full replacement:* `_apply_vs_merge` with D-04 conditional model → `_all_candidates` → `detect_self_match` (D-13 silent) → `compute_filtered` → `_filtered_candidates` → `_render_candidates_surface` → fire enrichment + conditional VS fetch.

*VS toggle wired:* `vs_switch.on_value_change(_on_vs_toggle_change)` — after `execute_joins_search` definition.

**Rule 1 fix (committed `559629db`):** `create_candidate_grid` in `candidate_grid.py` was called with 6 kwargs (`enrichment`, `enrichment_ready`, `filter_state`, `anchor_sys_id`, `on_page_change`, `on_filter_open`) that the function did not accept — would cause `TypeError` at runtime. Extended the signature to accept all 6; wired `on_page_change` to Prev/Next button `on_click`; added Filter button (icon=filter_list) to section header when `on_filter_open` provided.

**Ruff fix (committed `551c1a38`):** Removed unused `make_triage_state` and `paginate` imports from `joins_lab.py` (F401; both accessed via local import or internal to `create_candidate_grid`).

## Task Commits

1. **Task 1: VS adapter + enrichment batch + conditional merge** — `abeaf359` (feat)
2. **Tasks 2+3: candidate surface + VS toggle + enrichment + Compare wiring** — `a58eec86` (feat)
3. **Rule 1: extend create_candidate_grid signature for Phase-119 kwargs** — `559629db` (fix)
4. **Ruff F401: remove unused imports** — `551c1a38` (fix)

## Files Created/Modified

- `web/pages/joins_lab.py` — +451 net lines: 25 new page state vars, 10 helper closures, VS toggle UI, F1 empty-builder branch, full Step-9 replacement
- `web/components/candidate_grid.py` — +39 net lines: extended `create_candidate_grid` signature with 6 Phase-119 integration kwargs; functional Prev/Next pagination and Filter button
- `tests/test_vs_adapter.py` — xfail scaffolds → 5 green tests (made green in Task 1)
- `tests/test_candidate_enrichment.py` — xfail scaffolds → 2 green tests (made green in Task 1)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] create_candidate_grid signature mismatch**
- **Found during:** Tasks 2+3 implementation
- **Issue:** `_render_candidates_surface()` called `create_candidate_grid()` with 6 Phase-119 kwargs (`enrichment`, `enrichment_ready`, `filter_state`, `anchor_sys_id`, `on_page_change`, `on_filter_open`) that the Plan-02 function did not accept — would have raised `TypeError` at runtime (tests only test helpers in isolation, so not caught by the test suite)
- **Fix:** Extended `create_candidate_grid` signature to accept all 6 kwargs; wired `on_page_change` to Prev/Next button callbacks (previously buttons had no handler — pagination was non-functional); added Filter button when `on_filter_open` is provided
- **Files modified:** `web/components/candidate_grid.py`
- **Commit:** `559629db`

**2. [Rule 2 - Ruff F401] Unused imports from candidate_grid**
- **Found during:** `python -m ruff check` post-commit
- **Issue:** `make_triage_state` and `paginate` were imported at module top but not used in functional code (accessed via local import or used internally)
- **Fix:** Removed both from the import block
- **Files modified:** `web/pages/joins_lab.py`
- **Commit:** `551c1a38`

**3. [Implementation decision] Tasks 2+3 committed as single atomic commit**
- The plan listed Tasks 2 and 3 as separate tasks, but the NiceGUI closure captures form an interdependent graph (VS state from Task 3 is read by the F1 branch in Task 2; triage state from Task 2 is shared with the VS toggle handler in Task 3). A mid-implementation commit between them would have left the page in an inconsistent state. Committed as one atomic `feat` commit, satisfying both tasks' acceptance criteria.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced.
- VS lookup reads `visual_similarity.db` via `get_vs_service(thread_safe=True)` — parameterized int-cast query (T-119-12 mitigated)
- Enrichment reads `fjms_enrichment.db` via `get_fjms_service(thread_safe=True)` — parameterized batch query (T-119-12 mitigated)
- Both dispatched via `run.io_bound` off the event loop (T-119-13 mitigated; off-loop guard CI-green)
- Thumbnail URLs via `build_thumbnail_url` (proxy-only + preserved Oxford direct-Bodleian path) (T-119-14 mitigated)
- All page state is in-memory Python; zero `app.storage.user` writes (T-119-15 mitigated; allowlist `[]`)
- No `stop_propagation()` server-side (T-119-16 mitigated; AST guard green)

## Known Stubs

None. The candidate surface renders live data from the search results, VS service, and FJMS enrichment service. The Compare modal opens with real AnchorViewer instances. The filter dialog is functional.

One intentional deferral documented in the plan: the table view's `create_candidate_table()` is available via `create_candidate_grid.py` but the `_view_mode` toggle to switch between grid and table is not yet wired to the toolbar UI (the view_mode state exists, but no toggle button was added). The grid renders by default. This is within the plan scope — the plan only requires the grid/table surface to share the ONE triage dict, which it does via `_open_compare` and `_on_compare_verdict`.

## Self-Check

- `web/pages/joins_lab.py` contains `def run_vs_core` — VERIFIED
- `web/pages/joins_lab.py` contains `def run_enrich_core` — VERIFIED
- `web/pages/joins_lab.py` contains `run.io_bound(run_vs_core` — VERIFIED
- `web/pages/joins_lab.py` contains `run.io_bound(run_enrich_core` — VERIFIED
- `web/pages/joins_lab.py` contains `def _apply_vs_merge` — VERIFIED
- `web/pages/joins_lab.py` contains `create_compare_modal` — VERIFIED
- `web/pages/joins_lab.py` contains `_open_compare` — VERIFIED
- `web/pages/joins_lab.py` contains `_on_compare_verdict` — VERIFIED
- `web/pages/joins_lab.py` contains `compute_filtered` — VERIFIED
- `web/pages/joins_lab.py` contains `_do_enrich_and_update` — VERIFIED
- `web/pages/joins_lab.py` contains `detect_self_match` — VERIFIED
- `web/pages/joins_lab.py` does NOT contain `app.storage.user` in functional code — VERIFIED (6 tests green)
- `web/pages/joins_lab.py` does NOT contain any self-match banner string — VERIFIED
- `web/components/candidate_grid.py` accepts `on_page_change` and `on_filter_open` kwargs — VERIFIED
- `python -m pytest tests/test_vs_adapter.py tests/test_candidate_enrichment.py tests/test_joins_lab_off_loop.py tests/test_joins_lab_page.py tests/test_joins_lab.py tests/test_no_raw_storage_access.py -x -q` → 128 passed, 7 xpassed — VERIFIED
- Commits `abeaf359`, `a58eec86`, `559629db`, `551c1a38` exist — VERIFIED
- `python -m ruff check web/pages/joins_lab.py web/components/candidate_grid.py` → All checks passed — VERIFIED

## Self-Check: PASSED

---
*Phase: 119-candidates-compare-visual-similarity*
*Completed: 2026-06-19*
