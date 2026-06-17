---
phase: 111-telemetry-foundation
plan: 03
subsystem: testing
tags: [ast, ci-guard, posthog, telemetry, privacy, static-analysis]

# Dependency graph
requires:
  - phase: 111-telemetry-foundation plan 02
    provides: "desktop/telemetry.py as the sole legitimate importer of shared.posthog_server"
provides:
  - "tests/test_telemetry_no_direct_posthog.py: PRIV-03 chokepoint AST guard (6 tests)"
  - "Structural CI invariant: no desktop/*.py except desktop/telemetry.py (by resolved path) may reach the PostHog transport"
affects: [112, 113, 114, 115, 116]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Resolved-path exemption: path.resolve() == CHOKEPOINT (not basename), so desktop/widgets/telemetry.py would still be scanned"
    - "Static AST guard: stdlib ast/pathlib only, no desktop/ module imports at test time (PyQt6 safe)"
    - "Absolute invariant: no YAML allowlist — only ONE file is ever permitted, no exceptions"

key-files:
  created:
    - tests/test_telemetry_no_direct_posthog.py
  modified: []

key-decisions:
  - "No YAML allowlist (unlike Phase 87 safe_storage guard) — the PRIV-03 invariant is absolute: exactly one file, forever"
  - "Exemption by RESOLVED PATH not basename — prevents future desktop/widgets/telemetry.py from being silently skipped (REVIEWS LOW / T-111-19)"
  - "PRIV-03 delivered in Phase 111 (3 phases ahead of its Phase-116 slot) — Phase 116 plan must REFERENCE this guard, not re-implement it"
  - "Zero new pip deps: stdlib ast + pathlib + textwrap only; no import of desktop/ at test time"

patterns-established:
  - "AST guard pattern: _PosthogAccessVisitor with parent-tracking (_seen_inner_ids) to avoid double-reporting, mirrors _StorageAccessVisitor from test_no_raw_storage_access.py"
  - "Synthetic violation tests: prove the scanner is not vacuous by exercising all three import forms + bare/attribute call forms"

requirements-completed: [PRIV-06, PRIV-03]

# Metrics
duration: 2min
completed: 2026-06-14
---

# Phase 111 Plan 03: PRIV-03 AST Guard Summary

**Static AST guard enforcing that desktop/telemetry.py is the ONLY file under desktop/ permitted to import shared.posthog_server or call enqueue_event — 6 tests covering production scan, positive control, bare/aliased/from-shared import forms, and resolved-path exemption**

## Performance

- **Duration:** 2 min
- **Started:** 2026-06-14T09:52:30Z
- **Completed:** 2026-06-14T09:54:17Z
- **Tasks:** 1
- **Files modified:** 1

## Accomplishments

- Created `tests/test_telemetry_no_direct_posthog.py` (305 lines) — the PRIV-03 structural CI guard mirroring the Phase 87 `test_no_raw_storage_access.py` pattern
- Scanner detects all bypass forms: `import shared.posthog_server [as ph]`, `from shared.posthog_server import ...`, `from shared import posthog_server [as ph]`, bare `enqueue_event(...)` call, and `ph.enqueue_event(...)` attribute call
- Exemption uses `path.resolve() == CHOKEPOINT` (resolved-path comparison), not `path.name == 'telemetry.py'` (basename) — T-111-19 / REVIEWS LOW addressed
- All 44 telemetry tests pass together (38 from Phase 02 + 6 new); ruff clean

## Task Commits

1. **Task 1: PRIV-03 AST guard** - `a376890b` (test)

**Plan metadata:** follows

## Files Created/Modified

- `tests/test_telemetry_no_direct_posthog.py` (305 lines, created) — `_PosthogAccessVisitor` AST visitor (Import/ImportFrom/Call/Attribute detection with parent-tracking), `test_no_direct_posthog_outside_chokepoint` (production scan), `test_chokepoint_itself_does_import_posthog` (positive control), `test_lint_rejects_synthetic_violation` (bare call), `test_lint_detects_aliased_import_call` (aliased import + Attribute call), `test_lint_detects_from_shared_import_alias` (REVIEWS LOW form), `test_skip_is_by_resolved_path_not_basename` (path-not-basename guard)

## Decisions Made

- No YAML allowlist: the rule is absolute — only `desktop/telemetry.py`, no exceptions, no justification path. Unlike Phase 87 which needed a transitional allowlist for safe_storage migration, PRIV-03 ships into a clean state (only telemetry.py touches posthog_server).
- Resolved-path exemption is intentional and documented: basename-only exemption would silently skip a future `desktop/widgets/telemetry.py`. The `test_skip_is_by_resolved_path_not_basename` test pins this invariant.
- PRIV-03 delivered early: formally Phase 116 per REQUIREMENTS.md traceability table, but shipping in Phase 111 because the chokepoint exists now. Phase 116 plan should reference this file rather than re-implementing the guard.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Phase 111 (telemetry-foundation) is now COMPLETE: 3/3 plans done
- Plans 01 (posthog_server extensions), 02 (desktop/telemetry.py chokepoint), and 03 (PRIV-03 CI guard) all committed
- Phase 112 (first-run consent prompt) can begin — `desktop/telemetry.py::show_first_run_prompt()` stub is ready
- Phase 113 (crash hooks) can begin — `desktop/telemetry.py::install_exception_hooks()` stub is ready
- CI guard is active: any Phase 112-115 PR that accidentally imports `shared.posthog_server` directly from a new desktop/ file will fail `test_no_direct_posthog_outside_chokepoint`

## Known Stubs

None — this plan created only a test file; no production stubs introduced.

## Threat Surface Scan

No new threat surface. `tests/test_telemetry_no_direct_posthog.py` is a pure static AST test with no network, auth, file-write, or schema changes. It only reads `.py` files from `desktop/` using `pathlib.Path.read_text`.

## Phase 116 Provenance Note

PRIV-03 is listed in REQUIREMENTS.md as a Phase 116 deliverable. This guard was shipped in Phase 111-03 because:
1. The chokepoint (`desktop/telemetry.py`) already exists and is the only legitimate importer
2. Phases 112-115 will add new desktop/ files that could accidentally introduce violations
3. Early shipping gives CI coverage for the entire milestone, not just the final phase

**Action for Phase 116 plan:** Replace the PRIV-03 planned task with: "Verify `tests/test_telemetry_no_direct_posthog.py` remains green after Phases 112-115 additions — PRIV-03 guard was delivered in Phase 111-03."

## Self-Check: PASSED

- `tests/test_telemetry_no_direct_posthog.py` exists: FOUND (305 lines)
- Commit `a376890b` (Task 1): present in git log - VERIFIED
- 6 tests pass (`pytest tests/test_telemetry_no_direct_posthog.py -q`): 6 passed in 0.44s - VERIFIED
- ruff clean on test file: All checks passed - VERIFIED
- Cross-check with Phase 02 tests: 44 passed together - VERIFIED
- `test_no_direct_posthog_outside_chokepoint` scans real tree (zero violations): PASS
- `test_chokepoint_itself_does_import_posthog` (positive control): PASS
- `test_lint_rejects_synthetic_violation` (bare call detected): PASS
- `test_lint_detects_aliased_import_call` (aliased import + attribute call): PASS
- `test_lint_detects_from_shared_import_alias` (REVIEWS LOW form): PASS
- `test_skip_is_by_resolved_path_not_basename` (resolved-path not basename): PASS

---
*Phase: 111-telemetry-foundation*
*Completed: 2026-06-14*
