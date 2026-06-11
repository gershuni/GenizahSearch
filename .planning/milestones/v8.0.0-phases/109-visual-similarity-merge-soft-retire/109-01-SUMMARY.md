---
phase: 109-visual-similarity-merge-soft-retire
plan: "01"
subsystem: desktop/join_workbench
tags: [visual-similarity, adapter, i18n, tdd, pure-functions]
dependency_graph:
  requires: []
  provides:
    - "_normalize_vs_row shim in desktop/join_workbench.py"
    - "Phase-109 i18n keys in genizah_translations.py"
    - "Wave-0 VS adapter test file tests/test_join_workbench_vs.py"
  affects:
    - "desktop/join_workbench.py — Plan 02 will call _normalize_vs_row"
    - "tests/test_join_workbench_vs.py — Plan 02 adds test_load_visual_candidates_parity"
tech_stack:
  added: []
  patterns:
    - "Pure module-level shim function (Qt-free, testable in CI)"
    - "TDD RED→GREEN: test file before implementation"
    - "Shelfmark fallback: str(alma_id) when no csv_bank metadata (review #5)"
key_files:
  created:
    - tests/test_join_workbench_vs.py
  modified:
    - desktop/join_workbench.py
    - genizah_translations.py
decisions:
  - "_normalize_vs_row named without _vs_ prefix to pass D-18 no_private AST guard"
  - "Shelfmark fallback to str(alma_id) ensures CandidateCard never renders blank bold label"
  - "No '✎ text' badge key added (review #6 — text-only cards render UNBADGED per CONTEXT ✎text RESOLVED)"
  - "True D-14a parity test deferred to Plan 02 (test lands with _load_visual_candidates code)"
metrics:
  duration: "~8 minutes"
  completed: "2026-06-07"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 3
---

# Phase 109 Plan 01: Wave-0 VS Adapter Foundation Summary

Pure VS-dict-to-Candidate adapter shim with shelfmark fallback + 3 Wave-0 tests + Phase-109 i18n keys, all Qt-free.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Wave-0 VS adapter test file (RED) | `eba75d00` | tests/test_join_workbench_vs.py |
| 2 | `_normalize_vs_row` shim (GREEN) | `be03fdce` | desktop/join_workbench.py |
| 3 | Phase-109 i18n keys | `d7f7d06d` | genizah_translations.py |

## What Was Built

**Task 1 — Wave-0 test file (RED):** Created `tests/test_join_workbench_vs.py` with 3 pure tests:
- `test_vs_adapter_maps_fields` (JWB-12a): full field-mapping through shim + normalizer
- `test_vs_adapter_shelfmark_fallback` (review #5): shim fallback to `str(alma_id)` when no metadata
- `test_visual_source_greyed_when_no_vs` (JWB-12g): anchor absent → `has_suggestions=False` → empty list

The `tmp_vs_db` fixture is copied verbatim from `tests/test_visual_similarity.py` (alma_id_a=100 with 5 rows). File was committed as intentional RED — `_normalize_vs_row` not yet defined.

**Task 2 — `_normalize_vs_row` shim (GREEN):** Added pure module-level function to `desktop/join_workbench.py` at line 208 (before `candidate_to_result_dict`). Key behaviors:
- `alma_id` → `display.id` (for `_r_sid` in normalize_candidate)
- `rank` → `vs_rank` (normalize_candidate reads `"vs_rank"`, NOT `"rank"`)
- `svm_score` passthrough (key name already matches)
- `_via_vs = True` (underscore sentinel — normalize_candidate reads `res.get("_via_vs")`)
- `img = None` → `page_of()` returns None → `Candidate.page = None` (VS is manuscript-level)
- **Shelfmark fallback:** `shelfmark or str(alma_id)` — `_EnrichWorker` does NOT fill shelfmark (verified :1530-1598), and `CandidateCard` uses `c.shelfmark` directly with no fallback (verified :1668)
- Optional `shelfmark`, `title`, `library_code` params — Plan 02 will pass from `csv_bank`
- Named `_normalize_vs_row` (NOT `_vs_*`) to pass the D-18 no_private AST guard

All 3 VS tests pass GREEN; `test_join_workbench_no_private.py` passes; ruff clean.

**Task 3 — Phase-109 i18n keys:** Appended `TRANSLATIONS.update({...})` block to `genizah_translations.py` with 6 new EN+HE keys:
- `"  ★ both"` → `"  ★ שניהם"` (provenance badge, leading 2 spaces)
- `"  ⊙ VS"` → `"  ⊙ דמיון"` (provenance badge, leading 2 spaces)
- `"Text"` → `"טקסט"` (source selector radio label)
- `"Visual look-alikes loaded"` → `"דמיון חיצוני נטען"`
- `"No visual similarity data for this manuscript"` → `"אין נתוני דמיון חיצוני עבור כתב יד זה"`
- `"VS rank"` → `"דירוג דמיון"`

No `"  ✎ text"` key added (review #6 — text-only candidates render UNBADGED per CONTEXT.md ✎text RESOLVED note). Pre-registered Phase-108 keys not duplicated. i18n guard (`test_join_workbench_i18n.py`) passes green.

## Verification Results

```
tests/test_join_workbench_vs.py::test_vs_adapter_maps_fields PASSED
tests/test_join_workbench_vs.py::test_vs_adapter_shelfmark_fallback PASSED
tests/test_join_workbench_vs.py::test_visual_source_greyed_when_no_vs PASSED
tests/test_join_workbench_i18n.py: 4 passed
tests/test_join_workbench_no_private.py: 2 passed
ruff check desktop/join_workbench.py genizah_translations.py tests/test_join_workbench_vs.py: All checks passed!
```

Total: 9 tests across 3 guard suites — all GREEN.

## Deviations from Plan

None — plan executed exactly as written.

The plan correctly notes that the true D-14a parity invariant (`test_load_visual_candidates_parity`) will land in Plan 02 alongside `_load_visual_candidates`. This plan's scope is the pure shim + Wave-0 tests only.

## Known Stubs

None. `_normalize_vs_row` is complete. The optional `shelfmark`/`title`/`library_code` params have working defaults (fallback to `str(alma_id)` for shelfmark) — Plan 02 will pass real values from `csv_bank`.

## Threat Flags

No new network or auth surface introduced. Plan 01 adds only pure dict-transform logic and test infrastructure. The shelfmark fallback (T-109-09 mitigation) is implemented correctly.

## Self-Check

**Commits exist:**
- `eba75d00` (test file) — verified via `git log`
- `be03fdce` (shim) — verified via `git log`
- `d7f7d06d` (i18n keys) — verified via `git log`

**Files exist:**
- `tests/test_join_workbench_vs.py` — created
- `desktop/join_workbench.py` — modified (added `_normalize_vs_row` at line 208)
- `genizah_translations.py` — modified (Phase-109 block appended)

**Guard tests green:** VS tests 3/3, i18n 4/4, no_private 2/2.

## Self-Check: PASSED
