---
phase: 119-candidates-compare-visual-similarity
plan: 03
subsystem: web-ui
tags: [joins-lab, compare-modal, anchor-viewer, triage, visual-similarity, nicegui]

# Dependency graph
requires:
  - phase: 119-candidates-compare-visual-similarity
    plan: 01
    provides: badge_and_tooltip() pure helper in shared/joins_lab.py
  - phase: 117-vertical-spine
    provides: AnchorViewer (web/components/anchor_viewer.py) — both Compare panes reuse it

provides:
  - create_compare_modal() factory in web/components/compare_modal.py — full-screen two-pane Compare dialog with flip-through + verdict auto-advance
  - create_compare_state() — headless testable state factory (no NiceGUI render harness)
  - _find_candidate_idx() — per-image candidate lookup (uid OR (sys_id, page), NOT sys_id alone)
  - step_candidate() / step_pane_page() / record_verdict() — headless-testable pure helpers
  - 19-test green suite in tests/test_compare_modal.py (CMP-01/02/03 + F2 Pitfall-6 case)

affects:
  - 119-04 (Wave 2 — joins_lab page wires create_compare_modal from grid card / table row)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "create_compare_state() + pure step/record helpers — headless testable, no NiceGUI render harness needed (mirrors AnchorViewer._resolve_off_loop pattern)"
    - "_find_candidate_idx by uid then (sys_id, page) — per-image identity, NOT sys_id-only (D-02 / Pitfall 6)"
    - "Two fresh AnchorViewer instances in one dialog — independent page state per pane (Pitfall 3); window._msViewerLoaded idempotency guard covers dynamic construction"
    - "Source-assertion tests validate forbidden strings (inject_viewer_assets, app.storage.user, p-3/gap-3) are absent from production code"

key-files:
  created:
    - web/components/compare_modal.py
    - tests/test_compare_modal.py (rewritten from RED scaffold to GREEN)
  modified: []

key-decisions:
  - "Headless helpers (create_compare_state, step_candidate, step_pane_page, record_verdict) factored as public module-level functions so tests exercise logic without a live NiceGUI event loop — mirrors AnchorViewer._resolve_off_loop headless-test pattern"
  - "is_size_mismatch imported from web.components.candidate_grid (ONE shared 1.4 formula — D-15); not reimplemented in compare_modal.py"
  - "Source assertions written as test cases to catch forbidden patterns (not just comments); docstrings must not mention the forbidden strings since tests do full-string search"
  - "_find_candidate_idx fallback is index 0 (not -1 or exception) to keep modal openable even when the exact candidate is not found"

patterns-established:
  - "source-assertion tests: check for absent forbidden patterns via pathlib.Path.read_text (catches both code and docstring mentions — production docstrings must be kept clean)"

requirements-completed: [CMP-01, CMP-02, CMP-03, VSM-02]

# Metrics
duration: 18min
completed: 2026-06-19
---

# Phase 119 Plan 03: Compare Modal Summary

**Full-screen two-pane Compare modal with two independent AnchorViewer panes, per-image candidate lookup, flip-through wrap-around, Y/?/N verdict auto-advance, 👁 badge + size-mismatch warning; all CMP-01/02/03 + F2 Pitfall-6 tests green**

## Performance

- **Duration:** ~18 min
- **Started:** 2026-06-19T08:10:00Z
- **Completed:** 2026-06-19T08:28:00Z
- **Tasks:** 2
- **Files modified:** 2 (1 created, 1 rewritten from RED scaffold)

## Accomplishments

- Created `web/components/compare_modal.py` — `create_compare_modal(anchor_cand, initial_candidate, filtered_candidates, triage, on_verdict, enrichment, on_close) -> ui.dialog` factory implementing:
  - `ui.dialog().props('maximized persistent')` full-screen modal with header bar (`var(--bg-header)`), two panes separated by `border-right: 2px solid var(--border-light)`, and sticky verdict bar (`var(--bg-tertiary)`)
  - TWO independent fresh `AnchorViewer` instances (anchor pane left, candidate pane right) — per-pane independent folio navigation (Pitfall 3 honored: no shared page state)
  - `_find_candidate_idx()` per-image lookup by uid then `(sys_id, page)` tuple — NOT by sys_id alone (D-02 / Pitfall 6: same sys_id can appear on multiple folios)
  - `_step(delta)` flip-through with wrap-around (parity desktop `step(delta)`:3741)
  - `_record_verdict(verdict)` → calls `on_verdict(sys_id, verdict)` + `_step(1)` auto-advance (D-03)
  - `_fill_candidate(cand)` rebuilds candidate pane: shelfmark label, badge row (`badge_and_tooltip()` for 👁 icon + size-mismatch badge when `is_size_mismatch` ratio > 1.4), fresh `AnchorViewer` instance
  - `is_size_mismatch` imported from `web.components.candidate_grid` — single shared 1.4 formula (D-15)
  - All strings via `tr()`; no `p-3`/`gap-3`; no `inject_viewer_assets` call; no `app.storage.user`; no server-side `stop_propagation`

- Rewrote `tests/test_compare_modal.py` from RED scaffold (5 xfail) to 19 passing tests:
  - CMP-01: `create_compare_state()` captures `anchor_sys_id` and correct `current_candidate`
  - CMP-02: `step_pane_page()` moves anchor/candidate pages independently
  - CMP-03: `record_verdict()` writes `triage[sys_id]`, calls `on_verdict` callback once, auto-advances
  - CMP-03 wrap-around: verdict at last → wraps to first; `step(-1)` from first → last
  - **F2 (Pitfall 6):** two candidates sharing `sys_id` on pages 5 and 6 → lookup by page-6 candidate returns index 2 (NOT index 1 which is the page-5 entry)
  - Source assertions: `inject_viewer_assets`, `app.storage.user`, `p-3`/`gap-3`, `stop_propagation()` absent; `is_size_mismatch` imported from `candidate_grid`

## Task Commits

Each task was committed atomically:

1. **Task 1: Compare modal shell + two independent AnchorViewer panes** — `b47106ee` (feat)
2. **Task 2: Flip-through navigation + verdict auto-advance + badges + tests** — `c6e7578f` (feat)

## Files Created/Modified

- `web/components/compare_modal.py` — NEW: 433 lines — full-screen Compare modal factory + headless helpers
- `tests/test_compare_modal.py` — REWRITTEN: 5 xfail → 19 passing tests (RED→GREEN)

## Decisions Made

- **Headless helpers as public module-level functions:** `create_compare_state`, `step_candidate`, `step_pane_page`, `record_verdict` are public and exercise all logic headlessly without a NiceGUI runtime — mirrors Phase-117 `AnchorViewer._resolve_off_loop` headless-test pattern. This prevents test coupling to NiceGUI internals.
- **`is_size_mismatch` imported, not reimplemented:** Single formula in `candidate_grid.py`; imported into `compare_modal.py` to keep exactly ONE 1.4 threshold (D-15 requirement). Tests assert this import exists.
- **Source assertions check full file text (not AST):** Simpler and catches both code and comments. Production docstrings must therefore not mention forbidden strings. Updated the file docstring accordingly.
- **_find_candidate_idx fallback is 0:** When the initial candidate is not found in the filtered list, the modal opens on the first candidate rather than raising an exception. Logged as WARNING.

## Deviations from Plan

None — plan executed exactly as written. Both tasks completed in order with all acceptance criteria satisfied.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced.
- Images load exclusively via `AnchorViewer` → per-provider proxy + Phase-98 NLI circuit breaker. No direct IIIF URLs constructed in this file.
- Compare state is a local mutable dict; `on_verdict` is the only persistence hook (calls back to the page-level triage dict).
- Shelfmarks/tooltips rendered via `ui.label`/`ui.icon` (auto-escaped; no `.html()` of raw candidate text).

## Known Stubs

None. The component is wired: `create_compare_modal()` opens a fully functional Compare modal. Plan 04 wires the `on_compare` callback from grid cards/table rows to open it.

## Self-Check

- `web/components/compare_modal.py` exists and contains `def create_compare_modal` — VERIFIED
- `web/components/compare_modal.py` contains `AnchorViewer(` twice — VERIFIED (grep: 2 instantiations)
- `web/components/compare_modal.py` contains `border-right: 2px solid var(--border-light)` — VERIFIED
- `web/components/compare_modal.py` contains `var(--bg-header)` — VERIFIED
- `web/components/compare_modal.py` does NOT contain `inject_viewer_assets` — VERIFIED (test passes)
- `web/components/compare_modal.py` does NOT contain `app.storage.user` — VERIFIED (test passes)
- `tests/test_compare_modal.py` — 19 passed, 0 failed, 0 xfail — VERIFIED
- Commits `b47106ee` and `c6e7578f` exist — VERIFIED (`git log`)
- `tests/test_no_raw_storage_access.py` green — VERIFIED (6 passed)
- `tests/test_joins_lab_off_loop.py` green — VERIFIED (10 passed, 2 skipped)

## Self-Check: PASSED

## Next Plan Readiness

- Wave 1 (Plan 119-03) complete: `create_compare_modal()` importable and testable
- Wave 2 (Plan 119-04) can wire `on_compare=lambda cand: modal.open()` in `joins_lab.py` by calling `create_compare_modal()` from grid card / table row
- The off-loop guard tests will go live once Plan 119-04 adds VS lookup + enrichment call sites to `web/pages/joins_lab.py`

---
*Phase: 119-candidates-compare-visual-similarity*
*Completed: 2026-06-19*
