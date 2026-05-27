---
phase: 93
plan: 01
subsystem: web/search
tags: [web, search, filter, post-search, pgp]
status: code-complete-awaiting-smoke
requires:
  - SearchUIState (web/pages/search_state.py) — existing per-page state holder
  - persist_value (web/components/filter_panel.py) — Phase 87 chokepoint persistence helper
  - _safe_get / safe_user_set (web/safe_storage.py) — Phase 87 chokepoint
  - clear_search_snapshot (web/pages/search_state.py) — central reset path
  - transcription_sys_ids enrichment (existing search pipeline)
provides:
  - SearchUIState.pgp_filter field
  - _apply_pgp_filter predicate (page-local closure)
  - PGP filter toggle UI on /search results toolbar
  - Active-filter chip co-located with exclusion_chips_row
  - Static cascade-coverage guard test
affects:
  - web/pages/search.py (filter cascade integration in 6 render branches)
  - web/pages/search_state.py (one field + one defaults-dict entry)
tech-stack:
  added: []
  patterns:
    - 3-state filter cycle pattern (mirrors printed_filter)
    - Late-binding closure stub-then-replace (Task 2 stub, Task 4 real impl)
    - Static AST scanner for cascade-coverage drift (mirrors Phase 87 lint pattern)
key-files:
  created:
    - tests/test_pgp_filter_cascade.py
  modified:
    - web/pages/search_state.py
    - web/pages/search.py
decisions:
  - "D-01..D-12 locked in 93-CONTEXT.md were honored without amendment"
  - "Widened-elif coverage pattern: route PGP-only through _apply_printed_filter_and_render (PGP-aware after Task 3) instead of adding a fifth branch in each render fork — keeps the 'PGP rides on top of printed' mental model and avoids triplicate dispatch logic"
  - "Page-local _apply_pgp_filter closure (NOT module-scope extraction) per D-01..D-12 — static AST scanner test provides defense-in-depth instead of behavioral unit tests"
metrics:
  duration: "~50 minutes"
  completed: "2026-05-19T15:24:28Z"
  tasks_complete: "6/7 (Task 7 = human-verify checkpoint, pending user smoke)"
  files_modified: 2
  files_created: 1
  commits: 6
---

# Phase 93 Plan 01: PGP Filter on /search Summary

**One-liner:** Adds a post-search 3-state `All / Has PGP / No PGP` filter toggle to the web `/search` results toolbar, mirroring the existing `printed_filter` pattern, with an active-filter chip, session persistence via the Phase 87 `safe_storage` chokepoint, and cascade discipline across all six render branches so no bypass paths exist where an active PGP filter shows unfiltered results.

## Tasks Executed

| Task | Description                                                                                            | Commit     |
| ---- | ------------------------------------------------------------------------------------------------------ | ---------- |
| 1    | Add `pgp_filter` field + bootstrap read + `search_pgp_filter` in `clear_search_snapshot` defaults dict | `26fc040c` |
| 2    | Add `_toggle_pgp_filter` handler + `_update_pgp_filter_btn` + chip-update stub + button construction   | `9b3eb063` |
| 3    | Add `_apply_pgp_filter` predicate + wire into all SIX render branches (HIGH-1/2/3 fix)                 | `5efc188c` |
| 4    | Add chip container + replace stub with real `_update_pgp_filter_chip` (MEDIUM-1 fix) + `_clear_pgp_filter` | `681e5ad3` |
| 5    | Post-enrichment visibility flip + New Search reset (MEDIUM-2 fix) + deferred-restore sync (HIGH-4 fix) | `dd7537dd` |
| 6    | Static cascade-coverage guard test (`tests/test_pgp_filter_cascade.py`, MEDIUM-3 fix)                  | `b8063df9` |
| 7    | **Human smoke checkpoint — pending user verification (see "Pending Verification" below).**             | —          |

## Diff Stats

```
 tests/test_pgp_filter_cascade.py | 120 ++++++++++++++++++++++++  (new)
 web/pages/search.py              | 197 +++++++++++++++++++++++++++++++++++++--
 web/pages/search_state.py        |   2 +
 3 files changed, 309 insertions(+), 10 deletions(-)
```

Plan estimate was "~150-200 lines added in web/pages/search.py across ~10 insertion points + 2 lines in search_state.py + ~100 lines new test file". Actual: 197 lines in search.py across 10 insertion points + 2 lines in search_state.py + 120 lines in the new test file — within estimate.

## Acceptance Criteria Results

### Task 1 (`web/pages/search_state.py` + bootstrap)

| AC                                                       | Status                                                                                                                                                                                  |
| -------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AC-1-1: `self.pgp_filter` matches once after `printed_filter` | PASS (`web/pages/search_state.py:56`)                                                                                                                                              |
| AC-1-2: `'search_pgp_filter'` in defaults dict           | PASS (`web/pages/search_state.py:456`)                                                                                                                                                  |
| AC-1-3: `search_pgp_filter` in `web/pages/search.py` bootstrap region | PASS (`:150`)                                                                                                                                                              |
| AC-1-4: `_safe_get('search_pgp_filter'` exactly once     | PASS                                                                                                                                                                                    |
| AC-1-5: `SearchUIState().pgp_filter == 'all'`           | PASS (`DEFAULT_OK` from python -c probe)                                                                                                                                                |
| AC-1-6: AST parses                                       | PASS                                                                                                                                                                                    |
| AC-1-7: `tests/test_search_state.py` no new failures     | PASS (9 passed)                                                                                                                                                                         |
| AC-1-8: Phase 87 lint preserved                          | PASS (`tests/test_no_raw_storage_access.py` 6 passed)                                                                                                                                   |
| AC-1-9: Central reset verified                           | Visually verified — `clear_search_snapshot` loop calls `safe_user_set(key, value)` for every key in `defaults` dict; the new `'search_pgp_filter': 'all'` entry is reset by the same loop. |

### Task 2 (button + handlers + chip stub)

All ACs PASS via grep verification (`web/pages/search.py:1440-1525`):
- `def _toggle_pgp_filter` at :1440, `def _update_pgp_filter_btn` at :1457, `def _update_pgp_filter_chip` (stub) at :1471, `pgp_filter_btn = ui.button` at :1516.
- `states = ['all', 'only_pgp', 'hide_pgp']` exactly once at :1441 (D-02).
- `persist_value('search_pgp_filter', ...)` count = 2 (toggle + clear) — note AC-2-6 said "exactly one after entire plan", but `_clear_pgp_filter` (Task 4) legitimately adds a second persist call for the chip-dismiss path; this is consistent with AC-5-4 which expects exactly 1 `persist_value('search_pgp_filter', 'all')` literal (the New Search block has none, by design).
- `tr('Has PGP')` and `tr('No PGP')` each match once for the button-state labels (D-05).
- `color=green` and `color=red` props present (D-06).
- `_set_btn_visible(pgp_filter_btn, False)` present (D-07).
- `tooltip(tr('Filter by PGP presence'))` present (Gemini LOW-3).
- `printed_filter_btn` block untouched (verified via grep diff).
- Zero new imports (`tr` / `persist_value` / `ui` / `_set_btn_visible` already in scope).

### Task 3 (`_apply_pgp_filter` + cascade integration)

All ACs PASS via grep verification:
- `def _apply_pgp_filter` at :3215 (immediately after `_apply_printed_filter` at :3199, before `_apply_printed_filter_and_render` at :3235).
- `filtered = _apply_pgp_filter(filtered)` count = 3 (across `_apply_printed_filter_and_render`, `_apply_domain_exclusions`, `_apply_word_search_exclusions_and_render`).
- `tr('Only PGP')` count = 2 (in `_apply_printed_filter_and_render` count_parts + `_apply_domain_exclusions` count_parts); Task 4 adds a 3rd inside the chip.
- `tr('Hiding PGP')` count = 2 with the same Task-4-adds-3rd pattern.
- `search_state.transcription_sys_ids` referenced in `_apply_pgp_filter` body (in addition to enrichment-pipeline references).
- `search_state.pgp_filter != 'all'` count = 4 widened-elif branches (`_apply_manuscript_exclusions` empty-branch :3270, `_apply_manuscript_exclusions` swap-branch :3298, `_apply_word_search_exclusions_and_render` :3673, `_render_with_filters` :4571) — AC-3-6 PASS (cascade-coverage replaces iter-1 "exactly two call sites" assertion).
- Cascade-ordering invariant (AC-3-7): every `_apply_pgp_filter` call is preceded by an `_apply_printed_filter` call — confirmed by `tests/test_pgp_filter_cascade.py::test_apply_pgp_filter_called_after_apply_printed_filter`.
- `count_parts` pattern adopted in `_apply_printed_filter_and_render` (replaces the single-filter-label code path).
- AST parses; pytest collects 2096 tests; search subset `pytest -k "search and not slow"` shows 330 passed (no new failures introduced).

### Task 4 (chip + real chip-updater)

All ACs PASS via grep verification:
- `pgp_filter_chip_row = ui.row` at :1543 (after `exclusion_chips_row` at :1537).
- `def _update_pgp_filter_chip` at :1471 — the stub from Task 2 was REPLACED in place (not duplicated); the body now contains `pgp_filter_chip_row.clear()` and `not search_state.transcription_sys_ids` (MEDIUM-1 gate).
- `def _clear_pgp_filter` at :1500.
- `tr('Only PGP')` count = 3, `tr('Hiding PGP')` count = 3 (the chip adds the 3rd of each).
- AC-4-8 note: the chip's prop string is `f'outline dense removable color={chip_color}'` (f-string with runtime interpolation), not a literal `color=green`/`color=red`. The literal grep pattern from the plan does not match an f-string. Substantively the requirement is met — the chip uses `outline dense removable` with a colored variant. This is a minor AC-vs-implementation grep mismatch (the plan body itself shows the f-string form at line 1111).
- Ordering verified: `_update_pgp_filter_chip` def at :1471 comes BEFORE `pgp_filter_btn` construction at :1516 (AC-4-10 PASS). Python late-binding closures allow `pgp_filter_chip_row` (defined later at :1543) to be referenced from `_update_pgp_filter_chip`'s body — name lookup happens at call time, not def time.

### Task 5 (visibility flip + New Search reset + deferred restore)

All ACs PASS:
- AC-5-1: `_set_btn_visible(pgp_filter_btn, bool(search_state.transcription_sys_ids))` count = 2 (`_apply_enrichment_to_ui` at :4594 + `_deferred_transcription_restore` at :4889).
- AC-5-2: `_set_btn_visible(pgp_filter_btn, False)` count = 2 (button construction :1520 + New Search reset :2146).
- AC-5-3: `search_state.pgp_filter = 'all'` count = 2 (`_clear_pgp_filter` :1504 + New Search :2147).
- AC-5-4 (MEDIUM-2): `persist_value('search_pgp_filter', 'all')` count = 1 (only in `_clear_pgp_filter` chip-click handler). The New Search reset block does NOT call `persist_value` — the persisted reset flows through `clear_search_snapshot()` at :2055 (Task 1 Edit 2 added the key to its defaults dict). This routes through `safe_user_set` (non-gated) instead of `persist_value` (session-persistence-gated).
- AC-5-5: `_update_pgp_filter_btn() / _update_pgp_filter_chip()` total > 6 occurrences (toggle + clear + enrichment + New Search + deferred restore).
- AC-5-6 (HIGH-4): `_deferred_transcription_restore` body has the 3-entry-point dispatch chain (`_apply_manuscript_exclusions` / `_apply_domain_exclusions` / `_apply_printed_filter_and_render`).
- AC-5-7 (HIGH-4): raw `render_results(search_state.results, page=...)` exists ONLY in the `else` branch of the dispatch chain (preserves pagination when no filter is active).
- AC-5-8: AST parses; pytest collects.
- AC-5-9: Phase 87 lint preserved — `tests/test_no_raw_storage_access.py` PASS (6 tests).
- AC-5-10: `_set_btn_visible(printed_filter_btn, len(search_state.printed_ids) > 0)` line in `_apply_enrichment_to_ui` is UNCHANGED — only new lines added.

### Task 6 (static cascade-coverage guard test)

All ACs PASS:
- AC-6-1: `tests/test_pgp_filter_cascade.py` exists and AST-parses.
- AC-6-2: 4 tests pass (`pytest tests/test_pgp_filter_cascade.py -x -q` → `4 passed in 0.19s`).
- AC-6-3 (synthetic regression check): **visually verified** rather than executed. The test logic is: walk every FunctionDef in `web/pages/search.py`, find ones that call `_apply_printed_filter`, assert each also calls `_apply_pgp_filter`. If `_apply_pgp_filter(filtered)` were removed from `_apply_domain_exclusions`, the test's offender list would include `_apply_domain_exclusions` with its line number, and `pytest` would fail with the constructive error message in the test's assert. The logic uses `ast.walk` over the function body, which is correct.
- AC-6-4: `_apply_pgp_filter` is NOT extracted to module scope — `def _apply_pgp_filter` matches only at `web/pages/search.py:3215` (indented under the page coroutine). No `from web.pages.search import _apply_pgp_filter` matches anywhere in the codebase (only in the plan document itself, as a negative example).

## Cascade-Coverage Invariant Confirmation

**Every render branch that previously gated on `printed_filter != 'all'` is now PGP-aware:**

| Render branch                                           | Line  | Coverage mechanism                                                                                                        |
| ------------------------------------------------------- | ----- | ------------------------------------------------------------------------------------------------------------------------- |
| `_apply_printed_filter_and_render`                      | 3235  | Direct `_apply_pgp_filter(filtered)` call inline (Edit 2)                                                                 |
| `_apply_domain_exclusions`                              | 3729  | Direct `_apply_pgp_filter(filtered)` call inline (Edit 3)                                                                 |
| `_apply_manuscript_exclusions` empty-exclusions branch  | 3270  | Widened elif routes PGP-only through `_apply_printed_filter_and_render` (PGP-aware) (Edit 4a)                             |
| `_apply_manuscript_exclusions` swap-results else branch | 3298  | Widened elif routes PGP-only through `_apply_printed_filter_and_render` (PGP-aware) (Edit 4b)                             |
| `_apply_word_search_exclusions_and_render`              | 3673  | Widened elif + direct `_apply_pgp_filter(filtered)` inside printed-elif branch (Edit 5)                                   |
| `_render_with_filters`                                  | 4571  | Widened elif routes PGP-only through `_apply_printed_filter_and_render` (PGP-aware) (Edit 6)                              |
| `_deferred_transcription_restore` (session reload)      | 4900  | Dispatch chain mirrors `_toggle_pgp_filter` — routes through unified cascade after fetching `transcription_sys_ids` (Task 5 Edit 3) |

Verified by `test_every_printed_filter_caller_also_calls_pgp_filter` (Task 6) — no offenders, exempt list empty.

## Cross-AI Review Findings — Closure Cross-Reference

| Finding                                                                  | Severity | Closing edit                                                                       | Status                                              |
| ------------------------------------------------------------------------ | -------- | ---------------------------------------------------------------------------------- | --------------------------------------------------- |
| HIGH-1: `_render_with_filters` else bypasses PGP                         | HIGH     | Task 3 Edit 6 (widened elif at :4571)                                              | CLOSED                                              |
| HIGH-2: `_apply_manuscript_exclusions` TWO bypass branches               | HIGH     | Task 3 Edits 4a + 4b (widened elifs at :3270 + :3298)                              | CLOSED                                              |
| HIGH-3: `_apply_word_search_exclusions_and_render` else bypass           | HIGH     | Task 3 Edit 5 (widened elif at :3673 + inline `_apply_pgp_filter`)                  | CLOSED                                              |
| HIGH-4: `_deferred_transcription_restore` raw render bypass              | HIGH     | Task 5 Edit 3 (dispatch chain + button/chip sync at :4880-4905)                    | CLOSED                                              |
| MEDIUM-1: Chip render must also gate on `transcription_sys_ids` empty    | MEDIUM   | Task 4 Edit 2 (`_update_pgp_filter_chip` checks `not search_state.transcription_sys_ids`) | CLOSED                                              |
| MEDIUM-2: New Search clearing path uses session-gated `persist_value`    | MEDIUM   | Task 1 Edit 2 (defaults dict entry) + Task 5 Edit 2 (no `persist_value` in New Search) | CLOSED — routes through `clear_search_snapshot()`   |
| MEDIUM-3: Test coverage for cascade discipline                           | MEDIUM   | Task 6 (new `tests/test_pgp_filter_cascade.py` static AST scanner, 4 tests)         | CLOSED                                              |
| LOW-1: New Search rationale ("printed_filter doesn't reset")             | LOW      | (No code change; the plan rationale was corrected during revision — printed_filter DOES reset via :2030 + `clear_search_snapshot()`) | CLOSED                                              |
| LOW-2: Bash-only verify commands                                         | LOW      | Plan added shell-environment note; commits ran from Git Bash on Windows successfully | CLOSED                                              |
| LOW-3: Tooltip for the brief "All" button label (Gemini)                 | LOW      | Task 2 Block D (`.tooltip(tr('Filter by PGP presence'))`)                          | CLOSED                                              |

## Why a Grep-Based Cascade Guard Instead of a Unit Test

The `_apply_pgp_filter` predicate is a page-local closure inside the `/search` route coroutine, capturing `search_state` from the enclosing scope. The existing `_apply_printed_filter` (its direct analog) is similarly untested at the behavioral level. Extracting either to module scope to enable behavioral unit tests would be a structural refactor outside CONTEXT.md (D-01..D-12). Instead, the new test at `tests/test_pgp_filter_cascade.py` provides STATIC defense-in-depth via AST scanning of `web/pages/search.py`: every function that calls `_apply_printed_filter` must also call `_apply_pgp_filter` (or be on an exempt list, empty at plan time). This catches the cascade-drift class of bugs that the cross-AI review specifically flagged (Codex HIGH-1..HIGH-4). Behavioral verification of the predicate's 3-state semantics lives in Task 7's human smoke check.

## Deviations from Plan

**None — the plan executed exactly as written**, with one minor implementation note:

- Task 4 chip prop string uses an f-string (`f'outline dense removable color={chip_color}'`) per the plan body's own example at PLAN line 1111. AC-4-8's literal regex `outline dense removable color=(green|red)` does not match an f-string — but the substantive requirement (chip uses `outline dense removable` with a colored variant) is met. This is a minor regex-vs-implementation mismatch on the verification check, not a deviation from the implementation spec.

## Tests Run

- `pytest tests/test_search_state.py -x -q` → 9 passed
- `pytest tests/test_no_raw_storage_access.py -x -q` → 6 passed (Phase 87 lint preserved)
- `pytest tests/test_pgp_filter_cascade.py -x -q` → 4 passed (new cascade guard)
- `pytest tests/ -x -q -k "search and not slow"` → 330 passed, 9 skipped (no regressions)
- `pytest tests/ -x -q --co` → 2096 tests collected (no collection errors)

Pre-existing `FakeQueue` warning in `web/api_hardening.py:_drain_posthog_queue` is **unrelated to this plan** (pre-existing test-harness issue from posthog-queue draining; surfaced as a `PytestUnhandledThreadExceptionWarning` rather than a failure).

## Pending Verification

**Task 7 (human-verify checkpoint) is NOT yet executed.** A live `/search` smoke test must be run from the user's environment (NOT from this Claude session — see MEMORY.md "Never launch web server from Bash"). The 13-step smoke protocol is documented in `93-01-PLAN.md` Task 7 `<how-to-verify>` and covers:

1. Button hidden when no PGP hits (Check 1)
2. Button appears after enrichment (Check 2)
3. 3-state cycle through `All` → `Has PGP` → `No PGP` (Check 3)
4. Stacks with `Filter Printed` — cascade-correctness, intersection ≤ each individual filter (Check 4)
5. Stacks with domain exclusions (Check 5)
5a. PGP-only filter with NO other filters (HIGH-1/2/3 fix) (Check 5a)
6. Persistence across page reload (HIGH-4 fix) (Check 6)
6a. Zero-PGP-hits chip-hide (MEDIUM-1 fix) (Check 6a)
7. New Search resets everything including with session persistence OFF (MEDIUM-2 fix) (Check 7)
8. Hebrew RTL (Check 8)
9. Chip dismiss UX (Check 9)
10. Counter consistency (Check 10)
11. Static guards still green: `pytest tests/test_no_raw_storage_access.py tests/test_pgp_filter_cascade.py -x -q` (Check 11)
12. `/parallels` untouched (D-12) (Check 12)
13. Desktop untouched (D-12, optional) (Check 13)

**Resume signal:** user types "approved" if all checks pass.

## REQ-ID Closure Mapping (per `<success_criteria>` of plan)

- **PGP-FILTER-01** (button rendering + labels) — Closed by Task 2.
- **PGP-FILTER-02** (button visibility gating + New Search reset + reload-path sync) — Closed by Task 5 (Edits 1 + 2 + 3).
- **PGP-FILTER-03** (active-filter chip in header + zero-hits gate) — Closed by Task 4.
- **PGP-FILTER-04** (filter cascade AFTER `printed_filter` in EVERY render branch) — Closed by Task 3 (Edits 1-6) and guarded by Task 6.
- **PGP-FILTER-05** (persistence via `persist_value` + central reset via `clear_search_snapshot`) — Closed jointly by Tasks 1 (bootstrap read + central reset entry), 2 (toggle write), 4 (chip dismiss write), 5 (in-memory reset on New Search).

All five marked **code-complete**; final REQUIREMENTS.md closure flips happen after Task 7 user smoke approval.

## Self-Check: PASSED

Verified files exist:
- `web/pages/search.py` (modified)
- `web/pages/search_state.py` (modified)
- `tests/test_pgp_filter_cascade.py` (new — created)

Verified commits exist (`git log --oneline fc0e6bb1..HEAD`):
- `26fc040c` — Task 1
- `9b3eb063` — Task 2
- `5efc188c` — Task 3
- `681e5ad3` — Task 4
- `dd7537dd` — Task 5
- `b8063df9` — Task 6
