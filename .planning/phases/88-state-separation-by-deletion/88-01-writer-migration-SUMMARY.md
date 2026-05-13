---
phase: 88-state-separation-by-deletion
plan: 01
subsystem: web
tags: [multitenant, app-storage, export-state, refactor, phase-88, path-b]

# Dependency graph
requires:
  - phase: 87-foundations
    provides: web/safe_storage.py chokepoint + ensure_session_uuid bootstrap + lint scanner
provides:
  - All 13 writer sites for the 10 deleted-in-88-03 AppState fields migrated to local variables
  - source_text fold-in (D-13) wired at bootstrap snapshot-restore and legacy storage paths in parallels.py
  - Every set_parallels_export(...) call in web/pages/parallels.py classified bucket (b) — positive export with source_text in meta — per Refinement 3
  - Singleton mirror writes are gone; AppState fields are now write-orphaned and ready for deletion in Plan 88-03
affects: [88-02-export-state-rewrite, 88-03-appstate-deletion-and-enforcement]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Local-variable threading through export_state setter/updater/clearer calls (Phase 88 chokepoint discipline)"
    - "Underscore-prefixed scratch locals (_results, _query, _gap, _filters_applied, etc.) signal scoped-export payload, no consumer beyond immediate call"
    - "source_text fold-in into set_parallels_export meta dict eliminates legacy app.storage.user['parallels_source_text'] fallback dependency"

key-files:
  created: []
  modified:
    - "web/pages/search.py (5 writer-site clusters migrated to locals; 25 state.X = value assignments deleted)"
    - "web/pages/search_results.py (2 writer sites migrated to locals; 2 state.X = value assignments deleted)"
    - "web/pages/parallels.py (4 writer-site clusters migrated to locals + D-13 source_text fold-in at bootstrap + legacy paths; 9 state.parallels_* assignments deleted)"

key-decisions:
  - "Used `_X` underscore-prefixed locals matching the kwarg name on export_state calls (per CONTEXT.md Claude's Discretion)"
  - "Site 1b (legacy bootstrap) always populates meta with explicit `{'source_text': _legacy_source_text}` (even when empty string) so every set_parallels_export(...) call in parallels.py is bucket (b) positive-export per Refinement 3 — never bucket (c) annotated meta=None"
  - "ParallelsState class has no source_text attribute; Site 1 snapshot path reads source_text from the _active_snapshot tab-storage dict (which carries it from _persist_active_snapshot at line 237)"
  - "safe_user_set('parallels_source_text', text) at line 457 retained as-is per plan (legacy UI persistence writer); the corresponding reader-side fallback in web/api.py is deleted in Plan 88-02"
  - "safe_user_set('parallels_results'/'filtered') legacy persistence writes at lines 2341-2344 retained as-is — these are not export_state writes (they exist for page-reload UI persistence)"

patterns-established:
  - "Per Refinement 3 audit: classify every set_parallels_export call into bucket (a) clear-export, (b) positive with source_text, or (c) explicitly annotated meta=None — pattern enforces no silent source_text loss in the export envelope"
  - "Plan-boundary green discipline (D-05): AppState fields remain physically present on the class until Plan 88-03 deletes them, so tests that read state.X in fixtures continue to work byte-unchanged"

requirements-completed: [STATE-02]

# Metrics
duration: 14min
completed: 2026-05-13
---

# Phase 88 Plan 01: Writer Migration Summary

**13 AppState singleton-write sites across search.py / search_results.py / parallels.py migrated to local variables threaded through web/export_state setter/updater/clearer calls, with D-13 source_text fold-in at parallels.py bootstrap paths and Refinement 3 audit confirming every set_parallels_export call is bucket (b) positive-export with source_text in meta.**

## Performance

- **Duration:** ~14 min (executor wall-clock; includes 3:48 full-suite pytest run)
- **Started:** 2026-05-13T15:36:00Z (worktree spawn)
- **Completed:** 2026-05-13T15:50:09Z
- **Tasks:** 4 (3 file-modification tasks + 1 plan-boundary verification gate)
- **Files modified:** 3 source files; 36 state.X assignments deleted; 96 insertions / 127 deletions net (refactor with comment trimming)

## Accomplishments

- **search.py (5 writer-site clusters):** Sites 1 (`_reset_search`), 2 (`toggle_select_all`), 3 (history-restore), 4 (partial-results cancel), 5 (happy-path enrichment) all migrated. 25 `state.X = value` lines deleted. 4 `set_search_export(...)` / `clear_search_export()` / `update_search_export_selection(...)` calls preserved with identical-value semantics via locals.
- **search_results.py (2 writer sites):** Site 1 (post-display-filter sync at line 126) and Site 2 (per-row toggle at line 377) migrated. 2 `state.X = value` lines deleted; 2 `update_search_export_*` calls preserved.
- **parallels.py (4 writer-site clusters + D-13 fold-in):** Sites 1 (snapshot-restore), 1b (legacy bootstrap), 2 (history-restore), 3 (`_reset_parallels`), 4 (search-completion) all migrated. 9 `state.parallels_* = value` lines deleted. D-13 source_text fold-in landed at sites 1 (from `_active_snapshot.get('source_text')`) and 1b (from `safe_user_get('parallels_source_text')`). Refinement 3 audit: all 4 `set_parallels_export(...)` calls in the file are bucket (b) positive-export with `source_text` in meta; `clear_parallels_export()` in Site 3 is bucket (a) clear path.
- **Plan-boundary green:** Full pytest passes at 1880 passed / 20 skipped (vs Phase 87 close baseline 1879 passed / 20 skipped — net +1 from Phase 87→88 elsewhere in the tree, not from this plan). Ruff clean. check_docs clean.
- **Zero writer sites missed across web/:** Scoped grep `^\s*state\.(10 fields)\s*=` in `web/` returns 0 matches.
- **AppState class shape unchanged:** All 10 fields still declared in `web/state.py:AppState.init()` (grep returns 10). Tests that write to state.X in fixtures continue to work byte-unchanged per D-05.
- **export_state ABI unchanged:** All 7 setter/updater/clearer functions still defined in `web/export_state.py` with identical signatures.

## Task Commits

Each task was committed atomically with `--no-verify` (parallel worktree mode):

1. **Task 1: Migrate search.py writer sites (5 clusters)** — `7c2370ba` (refactor)
2. **Task 2: Migrate search_results.py writer sites (2 sites)** — `d377a822` (refactor)
3. **Task 3: Migrate parallels.py writer sites + source_text fold-in (D-13)** — `b1a28799` (refactor)
4. **Task 4: Plan-boundary green verification** — no source changes; verification gate (pytest 1880 passed, ruff clean, check_docs clean)

**Plan metadata:** committed via final docs commit (this SUMMARY.md).

## Files Created/Modified

- `web/pages/search.py` — 5 writer-site clusters at lines ~2064 (reset), ~2083 (toggle_select_all), ~3794 (history-restore), ~4114 (partial-cancel), ~4204 (happy-path) migrated to underscore-prefixed locals; 25 state.X assignments removed; export_state calls preserved verbatim with kwargs sourced from locals.
- `web/pages/search_results.py` — 2 writer sites at lines ~126 (post-filter sync) and ~377 (per-row toggle) migrated; 2 state.X assignments removed; `state` import retained (11 other state.X usages remain in file).
- `web/pages/parallels.py` — 4 writer-site clusters + D-13 source_text fold-in. Site 1 (snapshot-restore) builds `_snapshot_meta = {'source_text': _snapshot_source_text}` from tab-storage `_active_snapshot.get('source_text')`. Site 1b (legacy bootstrap) builds `_bootstrap_meta = {'source_text': _legacy_source_text}` from `safe_user_get('parallels_source_text')` — always populates meta (never None) to keep bucket (b) classification. Sites 2 and 4 rename `state.parallels_search_meta` to local `_parallels_search_meta`. Site 3 (`_reset_parallels`) deletes 3 `state.parallels_* = ...` clears; `clear_parallels_export()` retained.

## Decisions Made

- **Local-variable naming convention:** Underscore-prefixed (`_results`, `_query`, `_mode`, `_gap`, `_filters_applied`, `_warnings`, `_selected_uids`, `_parallels_search_meta`, `_snapshot_meta`, `_bootstrap_meta`) signals "scratch variable for export payload, no consumer beyond immediate call." Matches CONTEXT.md Claude's Discretion + per-call-site kwarg names.
- **Site 1b meta is always bucket (b), never bucket (c):** Even when `_legacy_source_text` is empty string, the plan-text guidance allowed `_bootstrap_meta = None` for empty-source-text case. Refinement 3 strict reading prefers all calls to be bucket (b); upgraded the implementation to always populate `_bootstrap_meta = {'source_text': _legacy_source_text}` (explicit empty-string when legacy storage lost source_text). This eliminates the bucket-(c) edge case entirely, satisfying the strictest reading of "no unannotated meta=None with non-empty results."
- **Site 1 source_text source:** `ParallelsState` class (defined at parallels.py:156) has no `source_text` field. The `_active_snapshot` tab-storage dict (built at `_persist_active_snapshot`, line 237) IS the canonical source — uses `text_input.value` when present, falls back to `decoded_text`. Site 1 reads from `_active_snapshot.get('source_text', '') or ''`.
- **Refinement 3 audit confirmed:** All 4 `set_parallels_export(` calls in parallels.py are bucket (b). Manual visual inspection via Python regex print matches the expected shape.

## Deviations from Plan

**Total deviations:** 1 minor strengthening of plan-text guidance, NO scope creep.

### Auto-fixed Issues

**1. [Rule 2 - Critical correctness] Site 1b meta always-populated instead of None-when-empty**
- **Found during:** Task 3 audit step (Refinement 3 walk of `set_parallels_export(` calls)
- **Issue:** Plan text for Site 1b read: `_bootstrap_meta = {'source_text': _legacy_source_text} if _legacy_source_text else None`. This means a legacy session entering the `_legacy_results is not None` branch with empty source_text would produce `meta=None` while passing non-empty results — bucket (c) per Refinement 3. The bucket-(c) annotation would also be required.
- **Fix:** Changed to always populate `_bootstrap_meta = {'source_text': _legacy_source_text}` (even when empty string). Now every set_parallels_export(...) in parallels.py is unambiguously bucket (b) — positive export with source_text in meta. Updated the comment to document this strengthening.
- **Files modified:** web/pages/parallels.py (Site 1b, lines ~302-310)
- **Verification:** Python regex audit of all 4 `set_parallels_export(` calls shows `meta=_snapshot_meta`, `meta=_bootstrap_meta`, `meta=_parallels_search_meta` x2 — all dict-valued non-None.
- **Committed in:** b1a28799 (Task 3 commit, after intermediate Edit)

**Impact on plan:** This is a strengthening, not a regression. The plan text allowed the bucket-(c) fallback; the implementation prefers the strictest bucket-(b) classification. No scope creep, no test changes, no semantic difference in any consumer (api.py readers consume `meta.get('source_text', '')` either way).

## Issues Encountered

- **Pytest harness teardown warning:** `AttributeError: 'FakeQueue' object has no attribute 'get'` in `web/api_hardening.py:552` (_drain_posthog_queue thread). This is a teardown-order warning in the test harness (FakeQueue is replaced after test scope), NOT a test failure. Unrelated to Plan 88-01 changes; pre-existing behavior. Test count 1880 passed / 20 skipped is the authoritative pass signal.
- **Verification grep ambiguity:** Plan's `(set|update|clear)_search_export\(` regex only matches 4 calls (not 5) because `update_search_export_selection(` doesn't end at `_export\(` literal. The 5 wired call sites are all present (4 set/clear + 1 update_selection); the regex was imprecise but the intent is met. Documented as a verification-tool nuance, not a defect in the migration.
- **check_docs Windows console:** Initial run of `python scripts/check_docs.py` failed with `UnicodeEncodeError` on emoji output. Ran with `PYTHONIOENCODING=utf-8` and confirmed clean: "All checks passed! Documentation is healthy." Unrelated to this plan.

## User Setup Required

None — refactor only. No environment variables, no dashboard configuration. Zero user-visible behavior change (per success_criteria).

## Next Phase Readiness

Plan 88-02 (export_state rewrite + test rewrite + _TEST_BACKEND removal) is unblocked. Specifically:

- **Plan 88-02 can now delete the singleton mirror writes safely** — no caller in web/ writes to the 10 AppState fields anymore.
- **Plan 88-02 will delete the reader-side `safe_user_get('parallels_source_text', '')` fallback in web/api.py** — Site 1 and Site 1b in parallels.py now fold source_text into the per-session export meta, so the fallback is genuinely dead after 88-02 lands.
- **Plan 88-02 will rewrite the 4 export-related test files** to monkeypatch `web.safe_storage.app` directly and drop the `_TEST_BACKEND` shim. Tests currently write to `state.X` in fixtures; AppState class shape is unchanged so they pass byte-identical, but they will be cleaned up in 88-02.
- **Plan 88-03 can then delete the 10 AppState fields** with the static AST/grep guard in place — zero writers means zero data-loss risk.

No blockers, no carry-over.

## Self-Check: PASSED

Verifying claims before returning:

**Created files:**
- `.planning/phases/88-state-separation-by-deletion/88-01-writer-migration-SUMMARY.md`: FOUND

**Modified files (per task commits):**
- `web/pages/search.py`: FOUND (verified in 7c2370ba)
- `web/pages/search_results.py`: FOUND (verified in d377a822)
- `web/pages/parallels.py`: FOUND (verified in b1a28799)

**Commit hashes verified present in git log:**
- 7c2370ba: FOUND (Task 1)
- d377a822: FOUND (Task 2)
- b1a28799: FOUND (Task 3)

**Acceptance criteria verified at end of Task 4:**
- pytest: 1880 passed, 20 skipped (above 1879 Phase 87 baseline)
- ruff check .: All checks passed!
- check_docs.py: All checks passed!
- scoped grep `^\s*state\.(10 fields)\s*=` in web/: 0 matches
- Broader grep `state\.(10 fields)\s*=[^=]` in web/: 0 matches
- All 4 set_parallels_export calls in parallels.py: bucket (b) (meta dict with source_text key)

---
*Phase: 88-state-separation-by-deletion*
*Plan: 01 (writer migration)*
*Completed: 2026-05-13*
