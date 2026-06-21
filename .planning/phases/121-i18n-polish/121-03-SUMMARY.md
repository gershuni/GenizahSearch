---
phase: 121-i18n-polish
plan: "03"
subsystem: i18n / RTL verification
tags: [i18n, rtl, uat, checklist, human-checkpoint, sc2-acceptance]
dependency_graph:
  requires: ["121-01"]
  provides: ["121-HE-UAT-CHECKLIST.md", "SC#2-sign-off"]
  affects: ["SC#2 acceptance gate", "v8.2.0 release readiness"]
tech_stack:
  added: []
  patterns: ["Per-surface UAT checklist with PASS/FAIL line items", "Human sign-off gate"]
key_files:
  created:
    - .planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md
  modified:
    - .planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md (sign-off block filled)
decisions:
  - "Checklist covers all 8 Joins Lab surfaces (D-01b) + the compare counter RTL regression guard (outer-scroll fix from Phase 120)"
  - "Sign-off block includes per-surface results table + overall PASS/FAIL + date for Hillel's sign-off"
  - "Drift-fix entry-point check explicitly tests the 'פתח במעבדת הצירופים' corrected label on /lists (Plan 01 fix)"
  - "Inline string/icon fixes applied during UAT pass (bfc658fa, 1a8c9aca); image/zoom defect deferred as SEED-010 (ea4140f9)"
metrics:
  duration: "20min"
  completed_date: "2026-06-21"
  tasks_completed: 2
  tasks_total: 2
  files_created: 1
  files_modified: 1
---

# Phase 121 Plan 03: HE-Mode RTL UAT Checklist Summary

**One-liner:** Per-surface HE-mode RTL UAT checklist for all 8 Joins Lab surfaces — authored, executed by Hillel, and signed off PASS (2026-06-21); SC#2 acceptance gate met; inline glossary + icon fixes applied; image/zoom defect deferred as SEED-010.

## Status: COMPLETE

Both tasks complete. SC#2 is met: the per-surface HE-mode RTL checklist exists, was executed against the live web app by Hillel, and is signed off overall PASS.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Author per-surface HE-mode RTL UAT checklist artifact | d04a8554 | `.planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md` |
| 2 | Hillel runs the HE-mode RTL UAT and signs off | (see inline fix commits + this SUMMARY commit) | `.planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md` (sign-off filled) |

### Inline fixes committed during the UAT pass (not planned — applied during Task 2)

| Commit | Description |
|--------|-------------|
| bfc658fa | HE-mode UAT glossary + wording corrections: "Add as Join" הוסף כחיבור→הוסף כצירוף (duplicate key override fix); "View in Browse" צפה בדפדוף→עיין בכתב יד; known-join confirm dialog body reworded to a question form |
| 1a8c9aca | web/components/joins_builder.py per-word modifier button icon tune→settings (gear icon matches "click the gear icon" tooltip and desktop ⚙) |
| ea4140f9 | Logged Joins Lab image-resolution + zoom bug as SEED-010 (language-independent; untestable during NLI outage; deferred to dedicated cloud-branch fix) |

## Sign-off Result

- **Overall: PASS** (conditional — see below)
- **Signed by:** Hillel
- **Date:** 2026-06-21

**Condition:** The i18n/RTL surfaces are accepted and SC#2 is met. All 8 surfaces pass. Inline
string and icon fixes were applied and committed during the pass. An image-resolution + zoom
defect was identified (CUDL/Oxford images not resolving consistently; zoom dead when image fails
to load) — this defect is language-independent and was untestable during an NLI outage. It has
been explicitly deferred as SEED-010 (out of i18n scope) with a docs/OPEN_ISSUES.md P2 row.

## Deviations from Plan

### Auto-fixed Issues (Inline during UAT pass — Rule 1/Rule 2)

**1. [Rule 1 - Bug] Three HE-value corrections in genizah_translations.py**
- **Found during:** Task 2 (UAT pass — Candidate Grid + Compare surfaces)
- **Issue:** "Add as Join" had a duplicate key where the last definition (הוסף כחיבור) overrode the correct earlier value (הוסף כצירוף); "View in Browse" used a less-precise translation; known-join confirm dialog was a statement, not a question.
- **Fix:** Corrected all three in `web/genizah_translations.py`
- **Files modified:** `web/genizah_translations.py`
- **Commit:** bfc658fa

**2. [Rule 1 - Bug] Modifier button icon mismatch in joins_builder.py**
- **Found during:** Task 2 (UAT pass — Query Builder surface)
- **Issue:** Per-word modifier button used `icon='tune'` but its tooltip said "click the gear icon (⚙)" — icon did not match the instruction; inconsistent with the desktop ⚙ button.
- **Fix:** Changed `icon='tune'` to `icon='settings'` (gear icon) in `web/components/joins_builder.py`
- **Files modified:** `web/components/joins_builder.py`
- **Commit:** 1a8c9aca

**3. [Deferred — SEED-010] Image-resolution + zoom defect**
- **Found during:** Task 2 (UAT pass)
- **Issue:** CUDL/Oxford images not resolving consistently across anchor/grid/Compare; zoom dead when image fails to load.
- **Disposition:** Language-independent (not an i18n defect); untestable during NLI outage at time of UAT. Per project memory `feedback_seed_midphase_fixes_to_cloud`: logged as SEED-010 and docs/OPEN_ISSUES.md P2 row. Not fixed inline.
- **Commit recording deferral:** ea4140f9

## Known Stubs

None — this plan produces only a verification checklist document and records a human sign-off.

## Threat Flags

None — this plan produces a markdown checklist only; no new production code path, no untrusted input, no new endpoints.

## Self-Check: PASSED

- [x] `.planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md` created and sign-off block filled
- [x] All 8 surfaces have PASS in the sign-off table
- [x] Overall PASS signed by Hillel, dated 2026-06-21
- [x] Inline fixes committed: bfc658fa (3 HE string corrections), 1a8c9aca (gear icon)
- [x] SEED-010 deferral documented: ea4140f9
- [x] Task 1 commit d04a8554 exists
- [x] SC#2 acceptance gate: met
