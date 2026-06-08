---
phase: 109-visual-similarity-merge-soft-retire
plan: "12"
subsystem: desktop/corrections_ui + join_workbench + genizah_app
tags: [visual-similarity, pick-mode, reroute, soft-retire, join-workbench, gap-closure]

# Dependency graph
requires:
  - phase: 109-08
    provides: "tr('find joins in joins lab') pre-seeded in genizah_translations.py"
  - phase: 109-10
    provides: "G-07 Browse + ResultDialog VS buttons removed; open_joins_workbench plain call verified"
  - phase: 109-11
    provides: "join_workbench.py triage/folio/hint edits; wave boundary clear"
provides:
  - "JoinsDialog VS button rerouted: 🔗 icon + tr('find joins in joins lab') tooltip"
  - "_show_vs_picker opens Workbench PLAIN (no source='visual', no pick_callback) + closes JoinsDialog"
  - "pick-callback machinery in join_workbench.py retained but MARKED REMOVABLE (D-11)"
  - "_enrich_vs_suggestions + _on_vs_fetch_complete marked removable (D-11, zero callers confirmed)"
  - "_show_vs_dialog marker refreshed: 'no live caller' + 'pending parity sign-off' (D-11)"
  - "Unreachability proof: grep '_on_vs_fetch_complete' genizah_app.py returns exactly 1 line"
affects:
  - 109-13  # re-UAT (human-verify) that flips the removability markers live

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "G-08 reversal pattern: plain Workbench open + dialog close; user creates join inside the Lab"
    - "D-11 one-cycle soft-retire: mark-removable-in-comments without deleting any code"
    - "Unreachability proof via zero-caller orphan chain: grep(_on_vs_fetch_complete)=1 line"

key-files:
  created: []
  modified:
    - corrections_ui.py
    - desktop/join_workbench.py
    - genizah_app.py
    - tests/test_join_workbench_vs.py

key-decisions:
  - "G-08 reverses G-05: _show_vs_picker drops source='visual' + pick_callback; no pre-fetch needed"
  - "self.close() abandons in-progress JoinsDialog intentionally — user continues in the Lab"
  - "_on_vs_pick retained (marked removable) but loses its only caller (G-08.3)"
  - "Unreachability expressed via zero-caller orphan (_on_vs_fetch_complete) rather than direct grep of _show_vs_dialog — the orphaned helper is the TRUE proof"

patterns-established:
  - "Static scan test uses src.find('def method_name') + next '\n    def' to bound method body — headless, no Qt"
  - "Removable-marker comments: placed at EACH site (module-level helper, init, pick button block, set/clear methods)"

requirements-completed: [JWB-12]

# Metrics
duration: 4min
completed: 2026-06-08
---

# Phase 109 Plan 12: JoinsDialog VS Button Rerouted PLAIN + Pick-Mode Soft-Retire Summary

**JoinsDialog VS button now opens the Workbench PLAIN (🔗 + 'find joins in joins lab'; no pick-back, no source='visual') then closes itself; pick-callback machinery marked removable; _show_vs_dialog confirmed to have no live caller via zero-caller orphan chain (D-11, all tests green).**

## Performance

- **Duration:** ~4 min
- **Started:** 2026-06-08T02:16:44Z
- **Completed:** 2026-06-08T02:20:56Z
- **Tasks:** 2 (Task 1 TDD RED+GREEN; Task 2 comments-only)
- **Files modified:** 4

## Accomplishments

- Changed `btn_vs_pick` icon 🔍 → 🔗 and tooltip from composite "Visual Similarity — Pick a partner in the Join Lab" to `tr("find joins in joins lab")`
- Rewrote `_show_vs_picker` docstring and last line: `open_joins_workbench(res)` PLAIN + `self.close()` (G-08.2)
- `_on_vs_pick` retained with G-08 removable-marker comment (loses its only caller, D-11)
- Added `MARKED REMOVABLE` comments at all 4 pick-callback machinery sites in `desktop/join_workbench.py`
- Marked `_enrich_vs_suggestions` and `_on_vs_fetch_complete` removable in `genizah_app.py` (orphaned helpers, zero callers)
- Refreshed `_show_vs_dialog` marker: "no live caller" (G-07 + G-08) + "pending parity sign-off" (D-14b not yet cleared)
- Unreachability proof: `grep -n "_on_vs_fetch_complete" genizah_app.py` returns exactly ONE line (the def)
- Added `test_joinsdialog_opens_plain_and_closes` (static scan; confirmed RED before implementation, then GREEN)
- Full 45-test gate passes: test_join_workbench_vs + test_join_workbench_i18n + test_join_workbench_no_private + test_visual_similarity_dialog + test_join_workbench_construct

## Task Commits

Each task was committed atomically:

1. **Task 1 RED: add failing test test_joinsdialog_opens_plain_and_closes** - `46d7b357` (test)
2. **Task 1 GREEN: reroute JoinsDialog VS button to plain Join-Lab open + close (G-08)** - `fce3de3d` (feat)
3. **Task 2: mark pick-callback machinery + orphaned VS helpers removable (D-11 G-08)** - `498ed517` (feat)

**Plan metadata:** (docs commit follows)

## Files Created/Modified

- `corrections_ui.py` — btn_vs_pick icon/tooltip updated; _show_vs_picker rewrote to plain open + close; _on_vs_pick marked removable
- `desktop/join_workbench.py` — MARKED REMOVABLE comments at 4 machinery sites: _invoke_pick, _pick_callback init, pick button block, set_pick_callback/clear_pick_callback section header
- `genizah_app.py` — _enrich_vs_suggestions + _on_vs_fetch_complete marked removable; _show_vs_dialog marker refreshed to "no live caller" + "pending parity sign-off"
- `tests/test_join_workbench_vs.py` — test_joinsdialog_opens_plain_and_closes (static scan, Plan 12)

## Decisions Made

- Static scan test binds method body using `src.find("def _show_vs_picker")` + next `"\n    def"` — keeps headless without Qt construction
- `_show_vs_dialog` marker avoids "fully unreferenced" (false — orphaned helper still names it); uses "no live caller" (TRUE via zero-caller orphan chain)
- Comment in `_show_vs_dialog` avoids the literal string `_on_vs_fetch_complete` to keep the unreachability grep clean at exactly ONE hit

## Deviations from Plan

One minor discovery during implementation: the plan's `_show_vs_dialog` marker template mentioned `_on_vs_fetch_complete` by name in the comment, but doing so would add a second grep hit and break the acceptance criterion. Adjusted the phrasing to "the orphaned VS-dialog-fetch helper above" — logically equivalent, grep-clean. No behavioral or structural deviation.

## Known Stubs

None. All functionality is wired:
- btn_vs_pick correctly uses the 🔗 icon and `tr("find joins in joins lab")` tooltip
- _show_vs_picker calls `open_joins_workbench(res)` PLAIN with no args and `self.close()` after
- All machinery (pick-callback, _show_vs_dialog, orphaned helpers) is retained with explicit markers

## Threat Flags

No new threats — desktop UI refinement only. The plain `open_joins_workbench(res)` call reuses the existing public path; no new I/O, network, auth, or input-parsing surface.

## Self-Check: PASSED

- `corrections_ui.py` btn_vs_pick icon is `"🔗"`: CONFIRMED (line 3443)
- `corrections_ui.py` btn_vs_pick tooltip is `tr("find joins in joins lab")`: CONFIRMED (line 3445)
- `corrections_ui.py` `_show_vs_picker` has no `pick_callback=` and no `source="visual"`: CONFIRMED
- `corrections_ui.py` `_show_vs_picker` calls `self.close()`: CONFIRMED (line ~4779)
- `corrections_ui.py` `_on_vs_pick` defined with removable-marker comment: CONFIRMED
- `tests/test_join_workbench_vs.py` contains `test_joinsdialog_opens_plain_and_closes`: CONFIRMED
- `desktop/join_workbench.py` has 4 MARKED REMOVABLE comments: CONFIRMED (grep count=4)
- `genizah_app.py` `_show_vs_dialog` marker contains "no live caller": CONFIRMED (line 4769)
- `genizah_app.py` `_show_vs_dialog` marker contains "pending parity sign-off": CONFIRMED (line 4775)
- `genizah_app.py` "fully unreferenced" does NOT appear: CONFIRMED
- `grep -n "_on_vs_fetch_complete" genizah_app.py` returns exactly 1 line (def only): CONFIRMED
- Task commits 46d7b357, fce3de3d, 498ed517 exist: CONFIRMED
- `python -m pytest tests/test_join_workbench_vs.py tests/test_join_workbench_i18n.py tests/test_join_workbench_no_private.py tests/test_visual_similarity_dialog.py tests/test_join_workbench_construct.py -q` → 45 passed: CONFIRMED
- `python -m ruff check corrections_ui.py desktop/join_workbench.py genizah_app.py` → All checks passed: CONFIRMED
- `python -c "import ast; ast.parse(open('corrections_ui.py',...).read()); ast.parse(open('genizah_app.py',...).read())"` → no syntax error: CONFIRMED
