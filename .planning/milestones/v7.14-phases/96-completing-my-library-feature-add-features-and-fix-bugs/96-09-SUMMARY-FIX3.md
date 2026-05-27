---
phase: 96
plan: "09-fix3"
subsystem: desktop-my-library
tags: [uat-bugs, polish, fix-iteration-3]
key-files:
  modified:
    - desktop/result_dialog.py
    - genizah_app.py
    - genizah_core.py
    - docs/OPEN_ISSUES.md
decisions:
  - "Fix 1 root: blanket findChildren(QPushButton) loop is maintenance-free; iteration 2 missed btn_view_transcription ('Browse') and ~24 other buttons"
  - "Fix 2 root: btn_b_all (Genizah View All) was not disabled when switching to LOCAL browse; Browse opened from a Genizah ms left btn_b_all enabled; clicking it on a LOCAL file triggered browse_load_all() -> 'Could not load full text' then browse_load_page() on toggle-off -> 'Page not found'"
  - "Fix 3 root: 'img' key was present in top-level dict but absent from display sub-dict; render loop reads meta = res['display'], so meta.get('img') returned ''"
  - "Bug #2 persistence deferred by user — documented as D-F6 in OPEN_ISSUES.md"
---

# Phase 96 Plan 09 Fix Iteration 3 Summary

**One-liner:** Fixed Enter-opens-Browse (blanket autoDefault=False), View-All regression (disable btn_b_all for LOCAL), and Img column blank for LOCAL hits (add img to display dict).

## Worktree Base Correction

The worktree's HEAD was at `61294a66` (v7.14.0 release) rather than `ca51c944` (iteration 2 merge). The worktree_branch_check `git reset --hard ca51c944` was applied at startup to restore the correct base. All fixes in this iteration are applied on top of `ca51c944`.

## What Was Different From Prior Iterations

### Fix 1 — Spinner Enter still opens Browse tab (P1)

**Prior agent (iteration 2):** Set `autoDefault(False)` on 6 buttons: `btn_res_prev`, `btn_res_next`, `btn_compact_pg_prev`, `btn_compact_pg_next`, local `btn_pg_prev`, local `btn_pg_next`.

**Actual remaining problem:** `btn_view_transcription` ("Browse" button) and ~24 other `QPushButton` instances in `ResultDialog` still had `autoDefault=True` (Qt default in QDialog). When the user typed a page number and pressed Enter, `spin_page.editingFinished` fired `load_page(target=N)` correctly, BUT the Enter key event also propagated to the default button — `btn_view_transcription` was the first visible/enabled QPushButton in the action_row that accepted Enter → `open_full_transcription()` fired → opened the Browse tab and closed the dialog.

The "skipping/wrong direction" navigation symptom was caused by the same mechanism: Enter simultaneously triggered both `editingFinished` (correct page jump) and `btn_view_transcription` (opened Browse / disrupted the view), making navigation appear erratic.

**This fix:** Added a post-`setLayout()` loop:
```python
for _btn in self.findChildren(QPushButton):
    _btn.setAutoDefault(False)
```
This covers all current and future buttons in the dialog, including the 24+ that iteration 2 missed. The `setLayout()` call must precede `findChildren()` for the widget tree to be fully built.

### Fix 2 — View All button shows "דף לא נמצא" (P1, regression from iteration 2)

**Prior agent (iteration 2):** Added `browse_open_file_btn.setVisible(False)` inside `browse_load()`. This change was correct but unrelated to the View All regression.

**Actual root cause:** The View All regression was pre-existing (existed before iteration 2) but was surfaced by iteration 2's Bug #4 fix which made LOCAL browse actually work — once users could successfully browse LOCAL files, they started clicking View All on them.

Flow:
1. User browses a Genizah manuscript → `browse_render_page()` calls `btn_b_all.setEnabled(True)` at line 22500
2. User opens a LOCAL search result in Browse → `_open_local_browse_page()` or `_open_local_browse()` is called — neither called `btn_b_all.setEnabled(False)`, so btn_b_all remained enabled
3. User clicks "View All" → `toggle_browse_view_all(True)` → `browse_load_all()` → `get_full_manuscript(LOCAL_SYS_ID)` → returns `[]` → warning dialog → btn_b_all stays checked
4. User dismisses warning, sees btn_b_all is checked, clicks again to un-toggle → `toggle_browse_view_all(False)` → `browse_load_page()` → `get_browse_page(LOCAL_SYS_ID)` → returns `None` → "דף לא נמצא"

**This fix:** In both `_open_local_browse` (view-all path) and `_open_local_browse_page`, added disable+uncheck of `btn_b_all`. LOCAL files have their own view-toggle (`btn_local_browse_view_toggle`); the Genizah-level `btn_b_all` is meaningless for LOCAL files. When a Genizah manuscript is next loaded, `browse_render_page()` re-enables it normally.

### Fix 3 — LOCAL hits show blank Img column in search results (P3)

**Root cause:** The search results render loop at `genizah_app.py:16642` does `meta = res['display']`, then at line 16678: `QTableWidgetItem(str(meta.get('img', '')))`. The `display` sub-dict for LOCAL hits was:
```python
{"id": sys_id, "source": "LOCAL", "library_code": "LOCAL", "shelfmark": shelfmark}
```
The `"img": p_num` key was present in the top-level result dict (added by iteration 2) but NOT in the `display` sub-dict where the render loop reads it. Genizah hits include `img` inside `display` via `get_display_data()`.

**This fix:** Added `"img": p_num` inside the `display` dict in `_build_local_result_dict`.

### Bug #2 — Persistence (deferred, user-accepted)

Per user direction: no fix attempted. Added D-F6 entry to `docs/OPEN_ISSUES.md` with root hypothesis and future fix path.

## Commits

| Hash | Description |
|------|-------------|
| afe4911b | fix(96-09): iteration 3 — Enter opens Browse; View All regression; Img column for LOCAL |

## Test Results

- 2560 passed, 24 skipped, 4 xfailed — all green (identical to iteration 2 baseline)
- ruff: all checks passed on changed files

## Self-Check: PASSED

- `desktop/result_dialog.py` modified with blanket autoDefault loop: confirmed
- `genizah_app.py` modified with btn_b_all disable in both LOCAL browse paths: confirmed
- `genizah_core.py` modified with img in display dict: confirmed
- `docs/OPEN_ISSUES.md` modified with D-F6 deferral entry: confirmed
- Commit afe4911b exists in git log: confirmed
