---
phase: 109-visual-similarity-merge-soft-retire
plan: "03"
subsystem: desktop/join_workbench
tags: [visual-similarity, reroute, source-selector, deprecation, soft-retire]
dependency_graph:
  requires:
    - "Plan 01: _normalize_vs_row shim + Phase-109 i18n keys"
    - "Plan 02: set_source()/apply_source() pending-source aware on JoinWorkbenchWindow"
  provides:
    - "open_joins_workbench(res, source=) with source='visual'/'combined'/'text', narrow logged except (D-10, review #8c)"
    - "Browse 'Visual similarity' rerouted to Workbench Visual source (D-10)"
    - "ResultDialog 'Search visual similarity' rerouted to Workbench Visual source + closes dialog (D-10)"
    - "'pending parity sign-off' deprecation marker on _show_vs_dialog normal-mode path (D-11, review #8b)"
    - "109-HUMAN-UAT.md scaffold with 7 parity scenarios — status: partial, awaiting Hillel sign-off (D-14b)"
  affects:
    - "Future cleanup phase: physical deletion of _show_vs_dialog normal-mode code (after D-14b sign-off)"
    - "Phase 110: JSA wiring may call open_joins_workbench; source= param is forward-compatible"
tech_stack:
  added: []
  patterns:
    - "source= param on open_joins_workbench: narrow (RuntimeError, AttributeError) logged except — not bare except (review #8c)"
    - "Reroute shape: replace entire body with res-dict construction + open_joins_workbench(res, source='visual')"
    - "Deprecation marker: 'DEPRECATED — pending parity sign-off' comment block before docstring (D-11/D-14 gate wording)"
key_files:
  created:
    - ".planning/phases/109-visual-similarity-merge-soft-retire/109-HUMAN-UAT.md"
  modified:
    - "genizah_app.py"
    - "desktop/result_dialog.py"
key-decisions:
  - "Reroute replaces entire body (drops local-DB/cache/server chain + _show_vs_dialog call); Workbench fetches VS itself"
  - "source= param defaults to 'text' so all existing callers of open_joins_workbench are unaffected"
  - "Deprecation marker phrased 'pending parity sign-off' (review #8b) to match D-14 gate; not 'REMOVED' or 'DO NOT USE'"
  - "Unused 'import json' in result_dialog.py removed after reroute eliminated all json.loads() calls (Rule 1 auto-fix)"
requirements-completed: [JWB-12]

# Metrics
duration: ~15min
completed: "2026-06-07"
---

# Phase 109 Plan 03: Reroute + Deprecation Marker Summary

**CODE-COMPLETE: Both desktop VS entry points (Browse + ResultDialog) now open the Join Workbench with the Visual source auto-loaded; open_joins_workbench gains a source= param with a narrow logged except; the old normal-mode _show_vs_dialog is marked "pending parity sign-off" — human parity UAT (D-14b) PENDING.**

## Performance

- **Duration:** ~15 min
- **Started:** 2026-06-07
- **Completed:** 2026-06-07
- **Tasks:** 2 auto + 1 checkpoint:human-verify (D-14b pending)
- **Files modified:** 3

## Accomplishments

- `open_joins_workbench(res, source="text")` now accepts an optional `source` parameter, applying `set_source(source)` after `set_anchor(res)` (D-01 ordering). The except is narrowed to `(RuntimeError, AttributeError)` + `logger.warning(...)` (review #8c — never swallow-all).
- `_browse_view_visual_similarity` body replaced: drops the local-DB/cache/server VS fetch chain and `_show_vs_dialog` call; now builds a res dict and calls `self.open_joins_workbench(res, source="visual")`.
- `_rd_search_visual_similarity` body replaced: drops `parent._show_vs_dialog/parent._enrich_vs_suggestions` chain; builds res dict, calls `app.open_joins_workbench(res, source="visual")`, then `self.close()`.
- `_show_vs_dialog` gains a "DEPRECATED — pending parity sign-off" comment block (D-11, review #8b); pick-mode `on_pick` branch and `_vs_open_joins_with_partner` untouched (D-12, SC#2).
- Automated parity gate (D-14a: `test_load_visual_candidates_parity`) confirmed GREEN.
- `109-HUMAN-UAT.md` scaffold created with 7 parity scenarios (D-14b, all PENDING).

## Task Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | source= param + reroute both entry points | `f98f5114` | genizah_app.py, desktop/result_dialog.py |
| 2 | Deprecation marker on _show_vs_dialog | `1ab99c88` | genizah_app.py |
| 1 fix | Remove unused json import (ruff F401) | `353c8415` | desktop/result_dialog.py |
| 3 | 109-HUMAN-UAT.md scaffold (checkpoint setup) | `db6bbcab` | .planning/phases/109-visual-similarity-merge-soft-retire/109-HUMAN-UAT.md |

## Files Created/Modified

- `genizah_app.py` — `open_joins_workbench` gains `source=` param; `_browse_view_visual_similarity` rerouted; `_show_vs_dialog` gains deprecation marker
- `desktop/result_dialog.py` — `_rd_search_visual_similarity` rerouted; `import json` removed (now unused)
- `.planning/phases/109-visual-similarity-merge-soft-retire/109-HUMAN-UAT.md` — UAT scaffold (status: partial)

## Decisions Made

- Reroute replaces the entire body of each VS entry-point method (not a conditional wrapper). The Workbench fetches VS candidates itself via `_load_visual_candidates`; the old local-DB/cache/server chain is gone from the entry points.
- `source=` defaults to `"text"` so all ~10 existing callers of `open_joins_workbench` in the codebase are unaffected.
- Deprecation marker uses "pending parity sign-off" wording (review #8b) to match the D-14 gate condition exactly — so the marker is self-documenting about when it becomes live.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed unused `import json` in result_dialog.py (ruff F401)**
- **Found during:** Task 1 (reroute _rd_search_visual_similarity)
- **Issue:** The reroute removed the `urllib.request + json.loads()` server-fallback chain, leaving `import json` unused; `python -m ruff check` reported F401.
- **Fix:** Removed `import json` from the file's imports.
- **Files modified:** desktop/result_dialog.py
- **Verification:** `python -m ruff check genizah_app.py desktop/result_dialog.py` → "All checks passed!"
- **Committed in:** `353c8415`

---

**Total deviations:** 1 auto-fixed (Rule 1 — import left orphaned by reroute)
**Impact on plan:** Necessary housekeeping; no scope creep.

## Human Gate: PENDING (D-14b)

The deprecation marker on `_show_vs_dialog` (D-11) is only considered LIVE after Hillel's manual parity sign-off (D-14b). The `109-HUMAN-UAT.md` file lists 7 scenarios:

1. Browse "Visual similarity" opens Workbench (not old orange dialog)
2. ResultDialog "Search visual similarity" opens Workbench + closes dialog
3. Four actions work on VS candidate cards (Browse / Puzzle / Add-to-List / Add-as-Join)
4. Reused-window re-anchor reloads VS for the new anchor
5. No-VS anchor: Visual + Combined greyed out; pane stays on Text
6. Performance: first 20-card page renders promptly; paging responsive
7. JoinsDialog pick-mode still works (D-12 / SC#2)

**Once Hillel approves all 7 scenarios:**
- Update `109-HUMAN-UAT.md` frontmatter: `status: complete`, `parity_sign_off: APPROVED`
- The deprecation marker on `_show_vs_dialog` is then live (D-11 flips)

## Verification Results

```
python -m pytest tests/test_join_workbench_vs.py::test_load_visual_candidates_parity -x
→ 1 passed (D-14a automated parity gate: GREEN)

python -m pytest tests/test_join_workbench_vs.py tests/test_join_workbench_no_private.py
        tests/test_join_workbench_i18n.py tests/test_visual_similarity_dialog.py
        tests/test_join_workbench_construct.py -x
→ 24 passed (all targeted phase tests: GREEN)

python -m ruff check genizah_app.py desktop/result_dialog.py
→ All checks passed!
```

## Known Stubs

None — this plan's code changes are fully wired. The pending UAT (D-14b) is a human sign-off gate, not a code stub.

## Threat Flags

No new network or auth surface. The reroute uses the public `open_joins_workbench` path (D-18: no `_vs_*` private calls on the rerouted bodies). The VS source fetches from the same local `visual_similarity.db` SQLite sidecar as before.

## Self-Check

**Commits exist:**
- `f98f5114` — Task 1: reroute + source param
- `1ab99c88` — Task 2: deprecation marker
- `353c8415` — Rule 1 fix: remove unused json import
- `db6bbcab` — Task 3: UAT scaffold

**Files modified:**
- `genizah_app.py` — confirmed (reroute body + source param + deprecation marker)
- `desktop/result_dialog.py` — confirmed (reroute body + import removed)
- `.planning/phases/109-visual-similarity-merge-soft-retire/109-HUMAN-UAT.md` — created

**Guard tests:**
- test_load_visual_candidates_parity: PASSED
- All 24 targeted phase tests: PASSED
- ruff: clean

## Self-Check: PASSED

---
*Phase: 109-visual-similarity-merge-soft-retire*
*Plan: 03*
*Completed: 2026-06-07*
