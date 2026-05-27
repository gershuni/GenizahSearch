---
phase: 96
plan: "09-fix2"
subsystem: desktop-my-library
tags: [uat-bugs, polish, fix-iteration-2]
key-files:
  modified:
    - desktop/my_library_tab.py
    - desktop/result_dialog.py
    - genizah_app.py
    - genizah_core.py
    - shared/local_indexer.py
    - tests/test_my_library_tab.py
decisions:
  - "Bug #3 root: ALL nav buttons (not just btn_res_prev/next) needed autoDefault=False; compact and full header page buttons also intercept Enter"
  - "Bug #2 root: timing race — tree populated before _restore_session fired; 300ms deferred auto-select resolves it"
  - "Bug #1 root: Pages/Status columns were always blank on startup because no prior scan data was loaded; new get_file_status_for_folder() API fills them"
  - "Bug #4 root: ResultDialog.load_result used parse_full_id_components(raw_header='') giving p=1 always; added LOCAL fallback reading data['img']"
  - "Bug #5 root: browse_load() called _show_local_browse_controls(False) but not browse_open_file_btn.setVisible(False); the open-file button has a separate hide path"
---

# Phase 96 Plan 09 Fix Iteration 2 Summary

**One-liner:** Re-fixed 5 UAT bugs where prior 96-09 agent's fixes were incomplete — root causes were: timing race (opt-outs), missing autoDefault on compact nav buttons (spinner), missing img field (Browse page), browse_load not hiding open-file button.

## What Was Different From the Prior Agent's Fix

### Bug #1 (UX — unified tree looks like split view)

**Prior agent:** Added `_UnifiedFileTreeWidget` and wired `_on_file_finished` to update it. The tree structure was correct.

**Actual remaining problem:** Pages and Status columns were always **blank** on startup. The tree only got populated with Pages/Status during a live scan (via `_on_file_finished` signals). When the user opened the app after a previous scan, the tree showed filenames and checkboxes but zero data in columns 1 and 2 — making it look useless / broken.

**This fix:**
- Added `LocalIndexer.get_file_status_for_folder(folder_path)` — queries `local_files` SQLite table for `page_count` and `extraction_status` of all indexed files in that folder.
- Updated `_UnifiedFileTreeWidget.populate_for_folder` to call this API and thread the results into `_populate_node`, which pre-populates Pages + Status columns (including error colours) immediately on tree load.
- Also added deferred auto-select of the first folder (300ms) so the tree is populated on startup without requiring user interaction.

### Bug #2 (persistence — opt-outs lost on close+reopen)

**Prior agent:** Added `flush_pending()` call in `closeEvent`. The save/restore code was correct.

**Actual remaining problem:** Timing race. The `_refresh_folder_list_ui()` call during `__init__` added items to the folder list, which caused Qt to auto-select the first item (or the user saw the tree and clicked it). `populate_for_folder` then read `self._app._local_file_optouts` which was still an **empty set** — `_restore_session` fires 200ms later. All files appeared checked (no opt-outs). On the next close, `_commit_changes()` saved the "all checked" state, wiping the real opt-outs.

**This fix:**
- `_refresh_folder_list_ui` now schedules `_auto_select_first_folder` via `QTimer.singleShot(300, ...)` instead of triggering selection immediately. The 300ms fires after `_restore_session` (200ms) has loaded `_local_file_optouts`. When the tree populates at 300ms, checkboxes correctly reflect the saved opt-out state.
- `_auto_select_first_folder` only sets row 0 if no item is currently selected (avoids overriding a user click in the 300ms window).

### Bug #3 (spinner Enter — page jumps by +1 or wraps)

**Prior agent:** Set `autoDefault=False` on `btn_res_prev` and `btn_res_next`, and added `spin_page.setFocus()` after loading a LOCAL hit.

**Actual remaining problem:** Two additional navigation buttons also had `autoDefault=True` (the default for `QPushButton` in a `QDialog`):
- `self.btn_compact_pg_next` / `self.btn_compact_pg_prev` (compact bar, lines 126-139)
- `btn_pg_next` / `btn_pg_prev` (full header nav, lines 246-248, local variables)

When Enter fired `editingFinished` → `load_page(target=39)`, the Enter key event also propagated to the next default button (e.g., `btn_compact_pg_next` → `load_page(offset=+1)` → page 40). For out-of-range values, the spinbox clamped the value to max, then the +1 step produced the "bizarre wraparound" the user observed.

**This fix:** Set `autoDefault=False` on all four nav buttons. Enter is now consumed exclusively by `spin_page.editingFinished`.

**Additional fix (ResultDialog page on open):** `load_result` extracted page `p` from `parse_full_id_components(raw_header)`. For LOCAL hits `raw_header` is `''` so `ids['p_num']` is always None → `p = 1`. Added a LOCAL fallback: when `current_sys_id` is a LOCAL sys_id, read `p` from `data.get('img') or data.get('p_num')`.

### Bug #4 (Browse opens at page 1 for LOCAL hits)

**Prior agent:** Fixed `_open_local_browse` to use `int(hit_p)` (handling string p_num). But `_build_local_result_dict` did NOT populate the `img` field.

**Actual remaining problem (identified by user):** The result dict lacked `'img'`. The `_open_local_browse` code at line 18753 reads `res.get('p_num')` which IS populated, so that path works. But `ResultDialog.load_result` reads `data.get('img')` via the LOCAL fallback (added in this fix). Without `img` in the dict, the fallback fell through to `p_num` (also now read), so both paths needed the field.

**This fix:** Added `"img": p_num` to the dict returned by `_build_local_result_dict`. This mirrors Genizah hits (genizah_core.py:5138: `'img': p_num`).

### Bug #5 (open-file button persists when switching to Genizah ms)

**Prior agent:** Added `browse_open_file_btn.setVisible(False)` inside `open_result_in_browse` (the non-LOCAL branch at line 18643). Also called `_show_local_browse_controls(False)`.

**Actual remaining problem:** `browse_load()` is called from many other paths (prev/next folio buttons, direct Browse tab input, session restore, etc.) — **27 call sites** in genizah_app.py. `browse_load()` called `_show_local_browse_controls(False)` but did NOT hide `browse_open_file_btn`. The `browse_open_file_btn` has a separate hide path that was missing from `browse_load()`.

**This fix:** Added `browse_open_file_btn.setVisible(False)` inside `browse_load()` alongside the existing `_show_local_browse_controls(False)` call. This covers all 27 call sites at once.

## Commits

| Hash | Description |
|------|-------------|
| 25f43763 | fix(96-09): bug #1+#2 — unified tree shows prior scan status; deferred auto-select fixes opt-out persistence |
| 2909e44d | fix(96-09): bug #3 — set autoDefault=False on all nav buttons; fix LOCAL page on ResultDialog open |
| f06476bb | fix(96-09): bug #4 — add 'img': p_num to _build_local_result_dict |
| 90e30815 | fix(96-09): bug #5 — hide 'Open file' button in browse_load (Genizah ms path) |

## Test Results

- 2560 passed, 24 skipped, 4 xfailed — all green
- `tests/test_my_library_tab.py`: updated `QTableWidget` → `QTreeWidget` assertion; added `get_file_status_for_folder` mock
- ruff: all checks passed on changed files

## Self-Check: PASSED

- `desktop/my_library_tab.py` modified: confirmed
- `desktop/result_dialog.py` modified: confirmed
- `genizah_app.py` modified: confirmed
- `genizah_core.py` modified: confirmed
- `shared/local_indexer.py` modified: confirmed
- All 4 fix commits exist in git log
