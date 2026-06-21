---
phase: 120-actions-persistence
plan: "06"
subsystem: joins-lab-actions
tags: [joins-lab, add-to-list, export, login-gate, off-loop, multitenant, csv, xlsx]
dependency_graph:
  requires: ["120-04", "120-05"]
  provides: ["ACT-03"]
  affects: [web/pages/joins_lab.py, web/components/candidate_grid.py, genizah_translations.py]
tech_stack:
  added: []
  patterns: [run.io_bound-off-loop, SEED-008-RuntimeError-guard, late-bind-ref, login-gate-GlobalAuthState, openpyxl-xlsx, utf-8-sig-csv]
key_files:
  created: []
  modified:
    - web/pages/joins_lab.py
    - web/components/candidate_grid.py
    - genizah_translations.py
    - tests/test_joins_lab.py
    - tests/render_smoke/test_joins_lab_render_smoke.py
decisions:
  - "Export is a PERSISTENT toolbar button (joins_lab.py toolbar row), NOT inside the bulk bar — visible in both Grid and Table view (R2-H2)"
  - "Export operates on _filtered_candidates (full filtered/sorted set), NOT _selected (table checkbox selection) — R2-H2 invariant"
  - "fetch_export_text_batch defined as a flat top-level sync def (not a nested factory closure) so the off-loop AST guard sees it passed directly to run.io_bound"
  - "Add-to-List uses asyncio.ensure_future(_open_list_picker()) inside the sync _on_add_to_list_click so the logged-in async path does not block the event loop"
  - "Both Task 1 and Task 2 committed in a single atomic commit (cb72063e) due to continuation context; TDD RED/GREEN gates were not separately staged"
metrics:
  duration: "~2 hours (split across context window, continued from prior session)"
  completed: "2026-06-21"
  tasks_completed: 2
  files_changed: 5
  tests_added: 25
---

# Phase 120 Plan 06: ACT-03 Add-to-List + Export Summary

**One-liner:** Login-gated Add-to-List picker dispatching add_list_item off-loop per selected candidate + flat 10-column CSV/XLSX export of the full filtered set with batched off-loop transcription text fetch.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | ACT-03/D-05 Add-to-List login-gate + picker | `cb72063e` | joins_lab.py, candidate_grid.py, genizah_translations.py, tests |
| 2 | ACT-03/D-06 Export flat CSV/XLSX off-loop batched | `cb72063e` | joins_lab.py, genizah_translations.py, tests |

## What Was Built

### Task 1 — Add-to-List (D-05)

`_on_add_to_list_click()` in `web/pages/joins_lab.py`:
- Sync function — compatible with the bulk action bar's `on_click` callback
- **Anonymous path:** opens a compact login-gate dialog with "Sign in to add candidates to a list" and Cancel/Sign-in buttons; `create_login_dialog().open()` on Sign-in (D-18 pattern); returns immediately, does NOT call `add_list_item`
- **Logged-in path:** `asyncio.ensure_future(_open_list_picker())` — async inner coroutine that:
  - Fetches `get_user_lists(user_id)` + `get_list_item_counts()` simultaneously via `asyncio.gather(run.io_bound(...), run.io_bound(...))`
  - Renders a single-level list-picker sub-dialog with list names + item counts
  - On list click: inner `_pick_list` coroutine iterates `_filtered_candidates` (filtering to `_selected` set), dispatches `add_list_item` off-loop via `run.io_bound` per candidate, shows success notify or inline error
  - SEED-008 (D-20): `except RuntimeError: return` wraps the whole body

`create_candidate_table()` in `web/components/candidate_grid.py`:
- New optional `on_add_to_list: Optional[Callable] = None` parameter
- "Add to List" button (`playlist_add` icon) rendered in the bulk action bar alongside "Add to Puzzle" when callback provided
- Bilingual tooltip: "Sign in to add candidates to a list" via `tr()`

New imports in `joins_lab.py`: `csv`, `io`, `build_image_url_for_row`, `add_list_item`, `get_list_item_counts`, `get_user_lists`.

### Task 2 — Export (D-06)

`_export_candidates(fmt: str)` in `web/pages/joins_lab.py`:
- **R2-H2:** Snapshots `_filtered_candidates` (NOT `_selected`) at click time — export is NEVER selection-scoped
- Caps at `_EXPORT_CANDIDATE_CAP = 500` (aligns with `SEARCH_API_FUZZY_MAX_LIMIT`)
- Shows inline progress card with cancel button; resets `_export_cancel_ref['value'] = False` on start
- **Off-loop text fetch:** `fetch_export_text_batch(batch_items)` — flat top-level sync def passed DIRECTLY to `run.io_bound` (satisfies the `test_joins_lab_off_loop.py` AST guard); calls `executor.get_browse_page(cand.sys_id, p_num=cand.page)` per candidate; `cand.page` is `None` for VS-only candidates (→ first text page, A1 assumption); text capped at `_EXPORT_TEXT_CAP = 4000`
- **10-column export** (UI-SPEC §7): Shelfmark, Library, Title, Triage (Y/?/N/—), Score, Material, Dimensions, Page, Transcription (page), Image URL
- `build_image_url_for_row(c.sys_id, library_code=library_code, img_page=c.page)` for Image URL column
- **CSV:** `io.StringIO` + `csv.writer` + `utf-8-sig` BOM encode (Excel-compatible)
- **XLSX:** `openpyxl` (late import to avoid optional-dep cost at module load); single-sheet "Candidates"
- `ui.download(content, filename=..., media_type=...)` to trigger download
- SEED-008 (D-20): outer `try/except RuntimeError: return`
- **Late-bind:** `_export_ref['fn'] = _export_candidates` after definition (matching `_submit_ref` pattern)

**Export toolbar button** (persistent, above `candidates_container`, visible in BOTH Grid and Table view):
- `ui.button('Export', icon='download')` with dropdown menu (CSV / Excel (XLSX))
- `_export_btn.on('click', _export_menu.open)` — opens Quasar auto-close menu
- Menu items dispatch via `asyncio.ensure_future(_export_ref['fn']('csv'/'xlsx'))`

New constants: `_EXPORT_CANDIDATE_CAP = 500`, `_EXPORT_TEXT_CAP = 4000`, `_EXPORT_BATCH_SIZE = 10`.

### Translations Added

Phase 120-06 block in `genizah_translations.py`:
- Add to List / הוסף לרשימה לבחירה
- Sign in to add candidates to a list / כניסה להוספת מועמדים לרשימה
- Add N candidates to list: / הוסף N מועמדים לרשימה:
- {N} candidates added to "{list_name}" / {N} מועמדים נוספו לרשימה "{list_name}"
- Could not add to list. Check your connection. / לא ניתן להוסיף לרשימה. בדוק את החיבור.
- No lists found. Create a list first. / לא נמצאו רשימות. צור רשימה תחילה.
- Export / ייצוא
- Export candidates to CSV or Excel / ייצוא מועמדים ל-CSV או Excel
- CSV, Excel (XLSX) / (same labels)
- Exporting the first 500 candidates. / מייצא את 500 המועמדים הראשונים.
- Preparing export… / מכין ייצוא…
- fragments fetched / פריטים נטענו
- Export failed. Check your connection and try again. / ייצוא נכשל. בדוק את החיבור ונסה שנית.
- Retry / נסה שנית
- No candidates to export / אין מועמדים לייצוא
- Column headers: Shelfmark/מספר מדף, Library/ספרייה, Title/כותרת, Triage/סינון, Score/ציון, Material/חומר, Dimensions/מידות, Page/עמוד, Transcription (page)/תעתיק (עמוד), Image URL/כתובת תמונה

## Test Results

```
115 passed (tests/test_joins_lab.py — includes 8 TestAddToList + 15 TestExport cases)
34 passed, 1 skipped (test_joins_lab_off_loop.py + render_smoke/test_joins_lab_render_smoke.py)
6 passed (test_no_raw_storage_access.py — Phase-87 allowlist stays [])
ruff: All checks passed on all 5 modified files
```

## Deviations from Plan

### Pragmatic Deviations

**1. [Rule 3 - Continuation context] Task 1 and Task 2 committed atomically**
- **Found during:** Continuation session resuming after context-window boundary
- **Issue:** The prior session had already written both Task 1 and Task 2 code into `joins_lab.py` together; the tests were written in this session as a batch. Separating them into distinct commits at this point would require reverting and re-staging which is higher risk than committing cleanly.
- **Fix:** Single combined commit `cb72063e` covers both tasks; the TDD RED gate for Task 2 was not separately staged (no failing-tests-first commit exists)
- **TDD Gate Compliance:** See below

**2. [Rule 1 - Bug] Renamed `_make_text_batch_fetcher` factory to flat `fetch_export_text_batch`**
- **Found during:** Task 2 initial implementation (prior session)
- **Issue:** The factory/closure pattern `_make_text_batch_fetcher(batch)` returned an inner `_fetch_batch` function, but the off-loop AST guard (`test_joins_lab_off_loop.py`) requires the sync function to be passed DIRECTLY as the first positional arg to `run.io_bound` — a closure returned from a factory is not seen as the direct arg
- **Fix:** Replaced with a flat top-level sync def `fetch_export_text_batch(batch_items)` defined inside `_export_candidates`, passed directly: `await run.io_bound(fetch_export_text_batch, batch)`
- **Files modified:** `web/pages/joins_lab.py`

## TDD Gate Compliance

Task 2 (`tdd="true"` in plan frontmatter) — TDD gate compliance:

- **RED commit:** NOT separately staged (deviation — continuation session context)
- **GREEN commit:** `cb72063e` contains both failing-test specification AND passing implementation together
- **REFACTOR:** None needed

Warning: the `test(120-06): ` RED commit is missing from git log. The implementation is correct and all 15 TestExport cases pass, but the TDD gate sequence (RED→GREEN separate commits) was not observed due to context-window continuation constraints.

## Known Stubs

None — Add-to-List and Export are both fully wired end-to-end. The `build_image_url_for_row` call for Image URL column may return an empty string for candidates without a mappable provider (graceful degradation, not a stub).

## Threat Flags

No new network endpoints introduced. `add_list_item` follows the existing authenticated Supabase write path (Phase-92 RLS — `list_items INSERT TO authenticated`); the login gate prevents any anonymous write attempt. Export is a client-side file download only (no server-side file storage, no new I/O surface).

## Self-Check: PASSED

- [x] web/pages/joins_lab.py modified — `_on_add_to_list_click`, `_export_candidates`, `fetch_export_text_batch`, `_export_ref` all present
- [x] web/components/candidate_grid.py modified — `on_add_to_list` parameter and "Add to List" button present
- [x] genizah_translations.py modified — 'Add to List', 'Export' keys present
- [x] tests/test_joins_lab.py modified — TestAddToList (8 cases) + TestExport (15 cases) present
- [x] tests/render_smoke/test_joins_lab_render_smoke.py modified — test_anon_add_list_gate present
- [x] Commit cb72063e exists in git log
- [x] All 115 targeted tests pass GREEN
- [x] test_joins_lab_off_loop.py: 34 passed, 1 skipped (off-loop discipline maintained)
- [x] test_no_raw_storage_access.py: CLEAN (Phase-87 allowlist stays [])
- [x] ruff clean on all 5 modified files
