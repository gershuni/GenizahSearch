---
phase: 96
plan: "08"
subsystem: desktop-ui
tags: [phase-96, my-library, navigation, browse-panel, result-dialog, new-2]
dependency_graph:
  requires: [96-03, 96-05, 96-07]
  provides: [NEW-2-end-to-end]
  affects:
    - desktop/result_dialog.py
    - genizah_app.py
tech_stack:
  added: []
  patterns:
    - "Dispatch-at-top-of-load_page: is_local_sys_id check before cancel_image_thread()"
    - "View-All/Per-Page toggle with _local_browse_view_mode state persisted in session JSON"
    - "apply_line_numbered_text + html.escape: gutter preserved + injection prevented for LOCAL content"
    - "Engine next_prev=offset nav: blank-page skip handled by engine primitive not cur+offset arithmetic"
key_files:
  modified:
    - desktop/result_dialog.py
    - genizah_app.py
decisions:
  - "dispatch-at-top-of-load_page: is_local_sys_id dispatch placed BEFORE cancel_image_thread() per W9 (LOCAL has no IIIF image thread to cancel)"
  - "View-All/Per-Page toggle button placed in nav_bar after browse_open_file_btn (after existing LOCAL chrome) — hidden by default, shown only when LOCAL content is loaded"
  - "local_browse_view_mode persisted at TOP LEVEL of session JSON (cross-surface key, mirrors local_file_optouts from 96-04) — NOT nested in regular_search or composition_search"
  - "import html as _html_mod inside methods to avoid conflict with local variable named 'html' at line 8246 in genizah_app.py (a local variable in an unrelated method)"
  - "cur + offset in docstring comment only (explains what was replaced per Codex MEDIUM #7); actual code uses next_prev=offset"
metrics:
  duration: "~8 minutes"
  completed: "2026-05-24T11:05:57Z"
  tasks_completed: 3
  files_changed: 2
---

# Phase 96 Plan 08: LOCAL Navigation End-to-End (NEW-2) Summary

ResultDialog dispatches LOCAL hits to `load_local_page` (engine primitive call + pinned widget identifiers); Browse panel gains View-All/Per-Page toggle with labeled page/chunk separators, four new per-page nav widgets, and html.escape-safe rendering.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add load_local_page dispatch in ResultDialog (pinned identifiers) | 665744dd | desktop/result_dialog.py |
| 2a | Add _aggregate_local_pages_with_separators helper | d4be5da7 | genizah_app.py |
| 2b | Create Browse-panel per-page nav widgets + View-All/Per-Page toggle | 4cbd86f4 | genizah_app.py |
| Checkpoint | Human verification (pending) | — | — |

## What Was Built

### Task 1 — ResultDialog LOCAL Dispatch (load_local_page)

`load_page` now dispatches to `load_local_page` via `is_local_sys_id(self.current_sys_id)` check inserted BEFORE `cancel_image_thread()`. The dispatch uses a try/except ImportError fallback so Genizah path is preserved on import failure.

`load_local_page` sibling method:
- Calls `self.searcher.get_local_browse_page(self.current_sys_id, p_num=p_arg, next_prev=offset)` — the engine primitive shipped by plan 96-03
- On None return (boundary) → disables `btn_compact_pg_prev` or `btn_compact_pg_next` per D-12 (no wrap)
- Renders via `apply_line_numbered_text(self.text_ms, self._htmlify(text), source_text=text, is_html=True)` — preserves v7.12.0 gutter AND HTML-escapes via `_htmlify` (W11 + Codex HIGH #4)
- Skips `cancel_image_thread()` — LOCAL has no IIIF image thread (W9)
- All PINNED identifiers used directly per 96-08-WIRING-NOTES.md (BLOCKER 2 closure)

### Task 2a — View-All Separator Helper

`_aggregate_local_pages_with_separators(pages, is_pdf, lang)` module-level helper:
- `is_pdf=True` → label `page` / `דף`; `is_pdf=False` → label `chunk` / `מקטע`
- `lang='he'` → Hebrew labels; else English
- First page has no leading separator; empty pages silently skipped
- `_get_local_full_text_for_sys_id` now detects file extension + reads `CURRENT_LANG` and delegates to this helper

### Task 2b — Browse-Panel Per-Page Widgets + Toggle

**4 new widgets created** (W12 closure — none existed before):
- `self.btn_local_browse_prev` — prev page/chunk; disabled at boundary (D-12)
- `self.btn_local_browse_next` — next page/chunk; disabled at boundary (D-12)
- `self.lbl_local_browse_page` — label shows "page N / M" or "chunk N / M" or Hebrew equivalents
- `self.btn_local_browse_view_toggle` — toggles View-All ↔ Per-Page; label updates to reflect current mode

**New methods:**
- `_show_local_browse_controls(visible)` — visibility helper; called from `_open_local_browse_page`, `_open_local_browse` (view-all path), and `browse_load` (hides when Genizah content loaded)
- `_open_local_browse_page(sys_id, p_num, hit_data)` — single-page render; `html.escape` before `\n→<br>`; `apply_line_numbered_text` on `self.browse_text`; sets `_local_browse_current_sys_id`/`_local_browse_current_p_num`
- `_on_local_browse_nav(offset)` — uses `get_local_browse_page(..., next_prev=offset)` not `cur+offset` arithmetic (Codex MEDIUM #7)
- `_toggle_local_browse_view_mode()` — flips mode, saves session, re-renders via `_open_local_browse`

**Closures:**
- Codex MEDIUM #5: `_open_local_browse` derives `initial_p` from `res.get('p_num')` not hard-coded 1
- Codex MEDIUM #6: both view-all AND per-page paths set `_local_browse_current_sys_id` for toggle round-trip
- Codex MEDIUM #7: nav uses engine primitive's `next_prev=offset`, not `cur+offset` arithmetic
- Codex HIGH #4: `import html as _html_mod; _html_mod.escape(text)` before `\n→<br>` in `_open_local_browse_page` and view-all path
- W11: both render paths use `apply_line_numbered_text` on `self.browse_text` — v7.12.0 gutter preserved
- W12: 4 new widgets CREATED (did not exist before)
- BLOCKER 2: all pinned identifiers (`text_ms`, `btn_compact_pg_*`, `spin_page`, `lbl_total`, `current_p_num`, `current_internal_idx`, `current_sys_id`) used directly — no hasattr chains

**Session JSON persistence:**
- `local_browse_view_mode` key at TOP LEVEL of session JSON (cross-surface, mirrors `local_file_optouts` pattern from 96-04)
- Restored in `_restore_session` with default `'per_page'` for pre-Phase-96 sessions

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Module-level `import html` conflicts with local variable at line 8246**
- **Found during:** Task 2b ruff check
- **Issue:** `genizah_app.py` has a local variable named `html` at line 8246 (an unrelated method builds an HTML string). A module-level `import html` produced F401 (unused in plan-visible code path) and F811 (redefinition).
- **Fix:** Removed the module-level `import html`. Instead used `import html as _html_mod` inline inside the two methods that need it (`_open_local_browse_page` and `_open_local_browse` view-all path).
- **Files modified:** genizah_app.py
- **Commit:** d4be5da7 (removal) / 4cbd86f4 (inline import)

**2. [Rule 1 - Note] Codex MEDIUM #7 verify check false-positive on docstring**
- The plan's acceptance criteria check for `'cur + offset' not in body` would fail because the docstring of `_on_local_browse_nav` contains "The previous `cur + offset` arithmetic broke..." as an explanatory comment. The actual implementation correctly uses `next_prev=offset`. This is a documentation-only occurrence — the code is correct.

## Human Verify Checkpoint Pending

The checkpoint asks for visual verification of:
1. ResultDialog prev/next on LOCAL PDF (pages) and DOCX/TXT (chunks)
2. Browse panel View-All/Per-Page toggle with labeled separators
3. Codex HIGH #4: HTML special chars in file content rendered as literal text
4. Codex MEDIUM #5: Browse panel opens at clicked hit's page number
5. Codex MEDIUM #6: Toggle round-trip works (per-page → all → per-page)
6. Line-number gutter visible in both result dialog and browse panel (W11)
7. Session persistence of view mode across restart
8. RTL check: Hebrew separator labels when UI lang = HE

Any issues surfaced during human verification are eligible for the NEW-3 freestyle polish wave (plan 96-09).

## Test Results

| Suite | Result |
|-------|--------|
| `tests/test_local_nav_page_chunk.py` (4 tests) | 4 PASSED |
| `tests/test_local_browse_panel.py` (10 tests) | 10 PASSED |
| `tests/test_result_dialog_local_button_removed.py` (2 tests) | 2 PASSED |
| `tests/test_web_library_options_no_local.py` | PASSED |
| `tests/test_no_raw_storage_access.py` | PASSED |
| `python -m ruff check desktop/result_dialog.py genizah_app.py` | Clean |

## Known Stubs

None — all functionality is fully wired. The human-verify checkpoint will confirm the render paths work end-to-end in the live UI.

## Threat Flags

None — this plan adds no new network endpoints, auth paths, file access patterns, or schema changes. File content rendered in Qt widgets (not a browser context), and is HTML-escaped before insertion (Codex HIGH #4).

## Self-Check: PASSED

- desktop/result_dialog.py: FOUND (`load_local_page` at grep match 2272)
- genizah_app.py: FOUND (`_aggregate_local_pages_with_separators`, `_open_local_browse_page`, `_toggle_local_browse_view_mode`, `_on_local_browse_nav`, `_show_local_browse_controls` all present)
- commit 665744dd: FOUND
- commit d4be5da7: FOUND
- commit 4cbd86f4: FOUND
