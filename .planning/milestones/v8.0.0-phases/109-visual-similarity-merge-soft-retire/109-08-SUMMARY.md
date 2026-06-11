---
phase: 109-visual-similarity-merge-soft-retire
plan: "08"
subsystem: ui
tags: [i18n, translations, hebrew, rtl, join-workbench, visual-similarity]

# Dependency graph
requires:
  - phase: 109-visual-similarity-merge-soft-retire
    provides: Phase-109 gap-closure (G-04 toggle + G-05 pick) keys already seeded in TRANSLATIONS
provides:
  - Four EN+HE translation keys for gap-round-3 (G-06 eye badge, G-13 hint line, G-13 combined empty, G-08 link tooltip)
  - Static i18n guard test (GAP_ROUND_3_KEYS + test_gap_round_3_keys_in_translations) in tests/test_join_workbench_i18n.py
affects:
  - 109-09
  - 109-11
  - 109-12

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Pre-seed translation keys ahead of implementation plans to prevent merge conflicts on genizah_translations.py"
    - "i18n guard test asserts all gap-round keys resolve in TRANSLATIONS before implementation plans call them"

key-files:
  created: []
  modified:
    - genizah_translations.py
    - tests/test_join_workbench_i18n.py

key-decisions:
  - "All new VS strings use חזותי (NOT חיצוני) per the G-01 precedent"
  - "Existing G-04/G-05 keys untouched — G-08 retires their callers but keys remain in TRANSLATIONS"
  - "G-08.1 tooltip key seeded here even though corrections_ui.py is not AST-scanned by the guard, so the guard pins it explicitly"

patterns-established:
  - "Phase-109 convention: pre-seed EN+HE keys in a dedicated plan before any implementation plan calls them"
  - "GAP_ROUND_3_KEYS constant + named test mirrors the PHASE_107_HOST_KEYS pattern in the same guard file"

requirements-completed: [JWB-12]

# Metrics
duration: 5min
completed: 2026-06-08
---

# Phase 109 Plan 08: Gap-Round-3 i18n Pre-seed Summary

**Four EN+HE translation keys for G-06 eye badge / G-13 hint+empty message / G-08 link tooltip pre-seeded in genizah_translations.py with חזותי (not חיצוני), and pinned by a new static guard test**

## Performance

- **Duration:** ~5 min
- **Started:** 2026-06-08T01:55:00Z
- **Completed:** 2026-06-08T01:57:00Z
- **Tasks:** 1
- **Files modified:** 2

## Accomplishments

- Appended a new `# === Phase 109 gap-closure round 3 ===` TRANSLATIONS.update block after line 4022 of `genizah_translations.py` with all four gap-round-3 EN+HE keys
- All new HE values use חזותי (never חיצוני), consistent with G-01 precedent
- Existing G-04/G-05 keys (`No look-alikes match this search`, `Select as partner`, `Pick a partner in the Join Lab`) preserved untouched
- Added `GAP_ROUND_3_KEYS` constant and `test_gap_round_3_keys_in_translations` to `tests/test_join_workbench_i18n.py`
- `python -m pytest tests/test_join_workbench_i18n.py -q` passes all 5 tests (new test included)
- `python -m ruff check genizah_translations.py tests/test_join_workbench_i18n.py` clean

## Task Commits

Each task was committed atomically:

1. **Task 1: Append four gap-round-3 EN+HE keys + assert them in i18n guard** - `cacede85` (feat)

**Plan metadata:** (docs commit follows)

## Files Created/Modified

- `genizah_translations.py` - New `# === Phase 109 gap-closure round 3 ===` TRANSLATIONS.update block with four keys: "visual similarity", "Turn off Visual Similarity to see more results", "No look-alikes match this search — turn off Visual Similarity to see all results", "find joins in joins lab"
- `tests/test_join_workbench_i18n.py` - `GAP_ROUND_3_KEYS` constant and `test_gap_round_3_keys_in_translations` test function added

## Decisions Made

None — followed plan as specified. All EN keys, HE values, and test structure taken verbatim from 109-08-PLAN.md.

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None — no external service configuration required.

## Next Phase Readiness

- Implementation plans 109-09, 109-11, 109-12 can now call `tr("visual similarity")`, `tr("Turn off Visual Similarity to see more results")`, `tr("No look-alikes match this search — turn off Visual Similarity to see all results")`, and `tr("find joins in joins lab")` without editing `genizah_translations.py`
- The i18n guard `tests/test_join_workbench_i18n.py` stays green at every wave boundary

---
*Phase: 109-visual-similarity-merge-soft-retire*
*Completed: 2026-06-08*
