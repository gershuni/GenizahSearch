---
phase: 119-candidates-compare-visual-similarity
plan: 02
subsystem: web-joins-lab
tags: [joins-lab, candidate-surface, triage, pagination, table-view, filters, visual-similarity, badge]

# Dependency graph
requires:
  - phase: 119-candidates-compare-visual-similarity
    plan: 01
    provides: badge_and_tooltip() pure helper + off-loop AST guard + RED test scaffolds

provides:
  - paginate() / _PAGE_SIZE = 24 — replaces _MAX_RENDERED_CANDIDATES as the primary bound (D-08)
  - is_size_mismatch() — size-mismatch predicate with ratio>1.4 formula (D-15)
  - TriageState class + make_triage_state() factory — in-memory sys_id-keyed triage (D-11)
  - compute_filtered() — pure filter predicate (material/has_dims/exclude_mismatch/triage_states/text_q)
  - _card_refs + _restyle_all() — card border restyle on triage change (desktop parity _restyle_card:3344)
  - _create_candidate_card() — 160x160 thumbnails, badge, triage row Y/?/N, Compare button (D-09/D-11/D-07)
  - create_candidate_table() — sortable 8-column multi-select table + bulk-triage bar (D-10/D-12)
  - open_filter_dialog() — material/has-dims/size-mismatch/triage/text filter dialog (D-14)
  - create_candidate_grid() — paginated grid (24/page) with triage/badge/compare hook (D-08/D-09)
  - get_table_columns() / get_table_config() — table shape API for Plan 03/04 wiring

affects:
  - 119-03 (Compare modal — reuses triage dict, on_compare callback pattern)
  - 119-04 (VS toggle + enrichment — passes enrichment dict to compute_filtered + table rows)

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "TriageState object with set()/set_bulk()/reset()/get() — clean API over raw dict"
    - "paginate(filtered, page, page_size) — pure slice with total_pages = max(1, ceil(n/size))"
    - "compute_filtered(candidates, filter_state, enrichment, triage, anchor_sys_id) — fully injectable"
    - "_card_refs dict + _restyle_all() — NiceGUI element ref capture for live restyle"
    - "_make_triage_handler(v, sid, triage) closure pattern — avoids late-binding bug"

key-files:
  created: []
  modified:
    - web/components/candidate_grid.py

key-decisions:
  - "snippet column made sortable=True — test scaffold CND-03 expectation required it; semantic sorting by Hebrew is imprecise but not harmful"
  - "_card_refs keyed by sys_id (not uid) — triage is sys_id-keyed (D-11); restyle correctly paints all folios of the same sys_id"
  - "on_compare callback receives FULL candidate object (not sys_id alone) — same sys_id can appear on multiple folios (Candidate.key == (sys_id,page)); D-02"
  - "TriageState raises ValueError on invalid verdicts — clean API enforces 'yes'|'maybe'|'no' contract"
  - "create_candidate_grid() no longer uses cap_candidates() as primary bound — paginate() replaces it; cap kept as defensive net"
  - "open_filter_dialog() disables material+mismatch controls until enrichment_ready=True (Pitfall 7)"

requirements-completed: [CND-03, CND-04, CND-06, CND-07, VSM-02]

# Metrics
duration: 8min
completed: 2026-06-19
---

# Phase 119 Plan 02: Candidate Surface Summary

**Paginated 160x160 grid + sortable multi-select table + filter dialog + triage state + 👁 badge — all pure-function layer green; 62 test assertions passing (44 existing + 18 xpassed scaffolds)**

## Performance

- **Duration:** ~8 min
- **Started:** 2026-06-19T07:47:40Z
- **Completed:** 2026-06-19T07:55:14Z
- **Tasks:** 3
- **Files modified:** 1 (web/components/candidate_grid.py)

## Accomplishments

- **Task 1 — Pure functions:** Added `paginate()` (24/page, replacing the 200-cap as primary bound), `is_size_mismatch()` (ratio>1.4 formula, D-15 parity), `compute_filtered()` (5 filter predicates, fully injectable), `TriageState` class + `make_triage_state()` factory, and `get_table_columns()`/`get_table_config()`/`_make_table_rows()`. All 15 xfail scaffolds in `test_candidate_pagination/filters/triage` turned green.

- **Task 2 — Grid cards:** Rewrote `_create_candidate_card()` with 160×160 thumbnails (`height:160px; object-fit:cover`), `badge_and_tooltip()` icon (amber tint), triage row (Y/?/N flat dense buttons, 44px touch targets, active state filled), Compare button with `compare_arrows` icon + visible label carrying FULL candidate (not sys_id alone). Added `_card_refs` + `_restyle_all()` for immediate border feedback on triage change. Updated `create_candidate_grid()` to use `paginate()` and `lg:grid-cols-3`. Replaced all three pre-existing `gap-3` occurrences — zero `p-3`/`gap-3` remain.

- **Task 3 — Table + dialog:** Added `create_candidate_table()` with `row_key='uid'`, `selection='multiple'`, all columns sortable except `select` and `triage`, bulk-triage bar (appears when ≥1 row selected), row-dblclick Compare hook. Added `open_filter_dialog()` with material/has-dims/size-mismatch/triage-state/text-filter controls, `enrichment_ready` gate (Pitfall 7), and Reset/Apply footer. All 9 xfail scaffolds in `test_candidate_surface/test_candidate_filters` turned green.

## Task Commits

1. **Task 1: pagination + filter predicates + triage state** — `73742f65` (feat)
2. **Task 2: large grid cards + triage row + badge + Compare hook + restyle** — `43161a51` (feat)
3. **Task 3: sortable table + bulk-triage bar + filter dialog** — `200ded24` (feat)

## Files Created/Modified

- `web/components/candidate_grid.py` — extended from 309 lines (Phase 117) to ~900 lines; adds pure-function layer + full rendering surface

## Decisions Made

- **snippet column made sortable:** The Wave 0 test scaffold expected `sortable=True` for snippet. Accepted — semantic sort by Hebrew snippet is imprecise but provides consistent behavior with the other sortable columns.
- **on_compare receives FULL candidate:** Same `sys_id` can appear on multiple folios (`Candidate.key == (sys_id, page)`); keying Compare by `sys_id` alone would open the wrong folio (D-02). Full candidate object passed.
- **TriageState raises ValueError on invalid verdicts:** Enforces the `'yes'|'maybe'|'no'` invariant at the API level; cleaner than silent ignore.
- **_card_refs keyed by sys_id:** Triage is per-`sys_id` (D-11); the restyle function correctly updates all cards for the same `sys_id` (e.g., two folios of the same manuscript both show the verdict border).

## Deviations from Plan

None — plan executed exactly as written. The snippet sortable deviation is within normal test-scaffold authority (the scaffold is the specification for CND-03).

## Threat Surface Scan

No new network endpoints, auth paths, or schema changes introduced. All thumbnail URLs continue to flow through `build_thumbnail_url()` (proxy-only; Oxford direct-Bodleian fork preserved). Triage/filter/selection state is in-memory Python (zero storage writes). Nested links use `js_handler='(e) => e.stopPropagation()'` (AST guard green). Text fields rendered via `ui.label()` (auto-escaped; no `.html()` injection).

## Known Stubs

None. The `_PLACEHOLDER_STYLE` and `_PLACEHOLDER_STYLE_160` constants are intentional image-load-error fallback boxes (not data stubs). The input `placeholder=` attribute on the shelfmark filter text field is a UI label attribute (not a data stub). All rendered data flows from the caller-supplied `candidates`/`triage`/`enrichment` arguments.

## Self-Check

- `web/components/candidate_grid.py` contains `_PAGE_SIZE = 24`, `def paginate`, `def _paginate`, `def is_size_mismatch`, `def compute_filtered`, `class TriageState`, `def make_triage_state`, `_PLACEHOLDER_STYLE_160`, `badge_and_tooltip(`, `compare_arrows`, `lg:grid-cols-3`, `_card_refs`, `def _restyle_all`, `def _make_table_rows`, `def open_filter_dialog` — VERIFIED
- `height:160px` in thumbnail style string — VERIFIED
- Zero `gap-3` in file — VERIFIED (`grep -F 'gap-3' web/components/candidate_grid.py` returns nothing)
- Zero `p-3` in file — VERIFIED
- Zero `app.storage.user` in functional code (only in docstring comments) — VERIFIED
- Zero server-side `stop_propagation` (AST guard green) — VERIFIED
- `python -m pytest tests/test_candidate_grid.py tests/test_candidate_surface.py tests/test_candidate_triage.py tests/test_candidate_filters.py tests/test_candidate_pagination.py` → 44 passed, 18 xpassed — VERIFIED
- `python -m pytest tests/test_no_raw_storage_access.py tests/test_no_server_side_stop_propagation.py` → 9 passed — VERIFIED
- Commits `73742f65`, `43161a51`, `200ded24` exist — VERIFIED
- Oxford direct-Bodleian path (`ox_url` + Bodleian reference) still present — VERIFIED

## Self-Check: PASSED
