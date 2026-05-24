---
phase: "96"
plan: "09"
iteration: 7
subsystem: desktop-my-library
tags: [fix, polish, codex-review, local-browse, session-restore, search-history]
dependency_graph:
  requires: [96-09-SUMMARY-FIX6.md]
  provides: [phase-96-closeout-ready]
  affects: [desktop/my_library_tab.py, shared/local_indexer.py, desktop/result_dialog.py, genizah_app.py]
tech_stack:
  patterns: [canonical-filepath-keying, post-restore-callback, html-escape-before-markup, session-persistence]
key_files:
  modified:
    - desktop/my_library_tab.py
    - shared/local_indexer.py
    - desktop/result_dialog.py
    - genizah_app.py
    - docs/OPEN_ISSUES.md
  created:
    - tests/test_local_nav_codex_fix7.py
decisions:
  - "P1.1: replaced 300ms QTimer with explicit notify_session_restored() callback from _restore_session finally block — eliminates timing race regardless of startup duration"
  - "P1.2: emit canonical filepath from local_indexer (not basename) and key _leaf_by_path by canonical path — O(1) lookup, no duplicate-basename collision"
  - "P1.3: html.escape() before newline/bold substitutions in _htmlify — matches Browse panel pattern established in iteration 6"
  - "P1.4: current_browse_sid = None sentinel at browse_load() entry — prevents stale LOCAL identity on failed Genizah lookups"
  - "F1: corpus_scope stored in search_params dict alongside mode_index/gap — restored by _restore_regular_search_from_state"
  - "F2: local_browse_sys_id + local_browse_p_num in top-level session state — _restore_local_browse deferred 400ms, silently skips if file no longer indexed"
metrics:
  duration: "~45 minutes"
  completed: "2026-05-24T17:34:00Z"
  tasks_completed: 6
  files_modified: 6
---

# Phase 96 Plan 09: My Library Polish — Iteration 7 (Codex P1 + Features) Summary

Closes Phase 96. Final Codex-prescribed polish pass: 4 P1 fixes + 2 user-requested features.
All items from the 96-09-CODEX-REVIEW.md P1 list addressed. D-F6 persistence bug resolved.

## What Was Done

### P1.1 — Restore Timing Race Fixed (D-F6 closed)

**Root cause (confirmed by Codex):** `_refresh_folder_list_ui()` scheduled
`_auto_select_first_folder` via `QTimer.singleShot(300, ...)` at `__init__`
time. `on_startup_finished` fires seconds later; `_restore_session` fires 200ms
after that. The 300ms timer from init always fired before session restore,
populating the tree with an empty opt-out set and overwriting the persisted
checkboxes on next close.

**Fix:** Removed the `QTimer.singleShot` from `_refresh_folder_list_ui()`.
Added `MyLibraryTab.notify_session_restored()` which calls
`_auto_select_first_folder()` directly. `GenizahGUI._restore_session()` calls
`my_library_tab.notify_session_restored()` in its `finally` block, guaranteeing
the auto-select fires only after opt-outs are loaded — regardless of startup
duration.

### P1.2 — Duplicate Basename Status Updates Fixed

**Root cause:** `local_indexer._file_finished_cb` emitted
`os.path.basename(filepath)`. `update_file_status()` did a linear scan looking
for a matching basename. Two folders containing `scan.pdf` → wrong row updated.

**Fix:** `local_indexer._file_finished_cb` now emits `_canonical_filepath(filepath)`.
`_UnifiedFileTreeWidget.update_file_status()` does a direct `_leaf_by_path.get(filepath)`
O(1) lookup; falls back to basename scan only for legacy callers. `_on_file_finished`
parameter renamed `filename` → `filepath` accordingly.

### P1.3 — ResultDialog HTML Injection Fixed

**Root cause:** `ResultDialog._htmlify()` applied `\n → <br>` and `*...*→<b>`
without first escaping HTML entities. File content containing literal `<` / `>`
/ `&` could inject markup into the QTextBrowser widget.

**Fix:** Added `html.escape(text)` as the first step in `_htmlify()`, before all
substitutions. Matches the `_open_local_browse_page` pattern introduced in
iteration 6. Bold markers (`*...*`) are unaffected — `*` has no HTML special
meaning and `html.escape()` leaves it unchanged.

### P1.4 — Stale Browse LOCAL Identity Fixed

**Root cause:** `browse_load()` only set `current_browse_sid = sid` on the
success path (line ~22808). Failed resolution paths (shelfmark not found, FL
not found) returned early without clearing `current_browse_sid`, leaving the
previous LOCAL sys_id in place. `_is_browsing_local()` would then return `True`
while the Browse UI showed Genizah chrome, causing LOCAL button dispatch on
Genizah manuscripts.

**Fix:** Added `self.current_browse_sid = None` at the entry of `browse_load()`,
after the empty-input guard. The real sys_id is set only after successful
resolution.

### Feature 1 — Search History Captures + Restores Corpus Selection

`_add_regular_search_to_history()` now stores `corpus_scope` in `search_params`
alongside `mode_index`, `gap`, etc. `_restore_regular_search_from_state()` reads
it back and updates both `_search_corpus_scope` and `corpus_scope_combo` (with
`blockSignals` to avoid spurious search triggers). History entries from before
this change gracefully default to `'genizah'` on restore.

### Feature 2 — Session Resume for LOCAL Browse State

`_save_session()` persists `local_browse_sys_id` and `local_browse_p_num` when
`_is_browsing_local()` is True. `_restore_session()` reads them back and
schedules `_restore_local_browse()` at 400ms. The restore function:
- Validates the sys_id is a LOCAL sys_id via `is_local_sys_id()`
- Calls `get_local_browse_page()` to verify the file is still indexed
- Silently skips if not found (file removed from index between sessions)
- Calls `_open_local_browse_page()` to restore the Browse panel at the saved page

`local_browse_sys_id` is also added to the `has_data` check so the restore
prompt triggers even when only LOCAL browse state was saved.

## Tests Added

`tests/test_local_nav_codex_fix7.py` — 12 new tests:

| Test | Covers |
|------|--------|
| `test_notify_session_restored_method_exists` | P1.1: method exists |
| `test_refresh_folder_list_ui_no_singleshot_300` | P1.1: no 300ms timer in _refresh_folder_list_ui |
| `test_local_indexer_emits_canonical_not_basename` | P1.2: local_indexer uses _canonical_filepath |
| `test_update_file_status_direct_lookup` | P1.2: update_file_status uses .get() |
| `test_htmlify_escapes_angle_brackets` | P1.3: < > & escaped |
| `test_htmlify_highlight_markers_still_work` | P1.3: bold markers survive |
| `test_htmlify_newlines_become_br` | P1.3: newlines still work |
| `test_browse_load_clears_sid_before_resolution` | P1.4: None sentinel before real sid |
| `test_add_regular_search_history_includes_corpus_scope` | F1: history saves corpus_scope |
| `test_restore_regular_search_from_state_applies_corpus_scope` | F1: restore applies it |
| `test_save_session_persists_local_browse_fields` | F2: session saves sys_id + p_num |
| `test_restore_session_restores_local_browse` | F2: session restore reads them back |

Full suite: **2580 passed, 24 skipped, 4 xfailed** (unchanged from pre-fix).

## OPEN_ISSUES.md Updates

- **D-F6**: Flipped from `Deferred` to `Fixed (2026-05-24, Phase 96-09 iter-7)` with
  root-cause description and fix approach documented.
- **D-F8** (new): Codex P3 page-block matching fragility — deferred to v7.15+ as
  known limitation. Works in practice; refactor candidate.
- **D-F9** (new): `_UnifiedFileTreeWidget` UI-thread filesystem walk — deferred to
  perf phase.
- **D-F10** (new): View All renderer path consolidation — deferred to v7.15+.

## Deviations from Plan

None. All 6 items (4 Codex P1 + 2 features) implemented as specified. The 3 Codex
"SKIP" items (P3 page-block matching, contract docs, LocalBrowsePage dataclass,
UI-thread walk, renderer consolidation) were documented as D-F8/D-F9/D-F10 in
OPEN_ISSUES.md rather than fixed.

## Commits

| Hash | Description |
|------|-------------|
| `19586ddd` | fix(96-09): P1.3 — HTML-escape LOCAL file content in ResultDialog._htmlify |
| `2683f1d2` | fix(96-09): P1.2 — emit canonical filepath for file-status updates (+ P1.1) |
| `e80422bc` | fix(96-09): P1.4 + F1 + F2 — browse identity, history corpus, LOCAL browse session |
| `3b299e44` | test(96-09): add fix-7 regression tests; update OPEN_ISSUES.md |

## Self-Check

- [x] All 4 commits exist in git log
- [x] Full test suite: 2580 passed, 24 skipped, 4 xfailed — no regressions
- [x] ruff clean on all modified files
- [x] iteration 4 regression tests (test_local_nav_codex_fix4.py) still pass
- [x] OPEN_ISSUES.md D-F6 marked Fixed
- [x] No STATE.md / ROADMAP.md modified (per objective)

## Self-Check: PASSED
