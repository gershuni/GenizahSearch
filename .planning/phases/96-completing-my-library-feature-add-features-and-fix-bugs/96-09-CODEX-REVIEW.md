# Codex Cross-AI Code Review — Phase 96 (post iteration 6)

## Verdict: One More Polish Needed

Core navigation model is fit. Four issues must be addressed before closing Phase 96.

## Strengths

- Core sparse-page navigation is directionally right: `p_num`, `internal_index`, `current_idx`, `max_p_num` preserve physical page numbers while supporting dense prev/next navigation.
- LOCAL result shape normalization tracks Genizah hit conventions (`p_num/img/display.img/highlight_pattern`).
- Opt-outs are query-time preferences, not destructive index state — right architecture.
- Reusing Browse prev/next + View All buttons avoids a parallel LOCAL nav UI.
- Test set covers important engine-level regressions: sparse pages, no-wrap, regex hit filtering, opt-out filtering, redundant button removal.

## Problems (must fix)

### P1 — Restore timing race is real
`MyLibraryTab.__init__` schedules folder auto-select at 300ms via `_refresh_folder_list_ui()`, but `_restore_session()` is scheduled LATER from `on_startup_finished()` after background startup. The tree can populate before `_local_file_optouts` is restored → checkbox commit ERASES restored opt-outs for displayed files. See `desktop/my_library_tab.py:1234` and `genizah_app.py:3067`. **This is the persistence bug we deferred.**

### P1 — Unified tree status updates wrong for duplicate basenames
Worker emits only `os.path.basename(filepath)`. `update_file_status()` updates the first matching basename in `_leaf_by_path`. Two folders containing `scan.pdf` → wrong row updated. See `shared/local_indexer.py:867` and `desktop/my_library_tab.py:335`. Fix: emit canonical filepath, not basename; key `_leaf_by_path` by canonical path.

### P2 — Browse LOCAL/Genizah dispatch can retain stale LOCAL identity
`_is_browsing_local()` keys off `current_browse_sid`. `browse_load()` hides LOCAL controls before resolving but does not clear `current_browse_sid` until successful resolution. Early returns leave buttons dispatching as LOCAL while chrome says Genizah. See `genizah_app.py:18956` and `genizah_app.py:22681`. Fix: clear `current_browse_sid` at start of `browse_load()`, set it only on successful resolution.

### P1 — ResultDialog LOCAL rendering not HTML-safe
`load_local_page()` routes LOCAL file text through `_htmlify()`, but `_htmlify()` does not escape `<`, `>`, `&`; only newlines + bold markers. Browse panel escapes correctly; ResultDialog does not. See `desktop/result_dialog.py:1951` + `2445`. Fix: route LOCAL text through `html.escape()` before `_htmlify()`, or add escape branch in `_htmlify` for LOCAL.

### P3 — View All page-block matching is fragile
One `<p>` per page → separate QTextBlocks is correct Qt mechanics. But `_mark_blocks_for_pages()` uses SUBSTRING matching — a page containing the separator text can be tagged as a separator block. See `genizah_app.py:18861` and `desktop/widgets/line_number_text_edit.py:317`. Fix: tag blocks by index/sentinel rather than text-content matching (e.g. set `userState` directly during HTML emission).

### P3 — Contract docs inconsistent
`get_local_browse_page()` docs say `current_idx` is 0-based but return value is 1-based; `internal_index` is the 0-based value. Code uses correctly but docs mislead. See `genizah_core.py:9328` and `9450`. Fix: update docstring.

## Optimality — Top 3 refactors

1. Replace loose-dict return from `get_local_browse_page()` with a `LocalBrowsePage` dataclass: `physical_p_num`, `ordinal_1based`, `ordinal_0based`, `total_indexed_pages`, `max_physical_p_num`.
2. `_UnifiedFileTreeWidget` should be data-driven from indexed DB rows where possible; move filesystem discovery to worker or cancellable async scan. Current `populate_for_folder()` recursively walks + `expandAll()`s on UI thread.
3. Collapse LOCAL View All rendering into one canonical path. `_aggregate_local_pages_with_separators()` / `_get_local_full_text_for_sys_id()` are now mostly vestigial while the actual renderer hand-builds `<p>` blocks.

## Test coverage gaps

Critical blind spots:
- 200-page cap dialog (added iteration 6)
- Browse shared-button dispatch after LOCAL→Genizah failure
- File-path label visibility (added iteration 5)
- Duplicate basename status updates
- Restore/autoselect timing
- ResultDialog LOCAL HTML escaping

## Phase Close Readiness

**One more polish needed.** Fix the 4 P1 items (restore timing race, duplicate-basename status, stale Browse LOCAL state, ResultDialog HTML escaping) before closing Phase 96.
