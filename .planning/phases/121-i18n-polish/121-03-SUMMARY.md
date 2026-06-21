---
phase: 121-i18n-polish
plan: "03"
subsystem: i18n / RTL verification
tags: [i18n, rtl, uat, checklist, human-checkpoint]
dependency_graph:
  requires: ["121-01"]
  provides: ["121-HE-UAT-CHECKLIST.md"]
  affects: ["SC#2 acceptance gate"]
tech_stack:
  added: []
  patterns: ["Per-surface UAT checklist with PASS/FAIL line items"]
key_files:
  created:
    - .planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md
  modified: []
decisions:
  - "Checklist covers all 8 Joins Lab surfaces (D-01b) + the compare counter RTL regression guard (outer-scroll fix from Phase 120)"
  - "Sign-off block includes per-surface results table + overall PASS/FAIL + date for Hillel's sign-off"
  - "Drift-fix entry-point check explicitly tests the 'פתח במעבדת הצירופים' corrected label on /lists (Plan 01 fix)"
metrics:
  duration: "5min"
  completed_date: "2026-06-21"
  tasks_completed: 1
  tasks_total: 2
  files_created: 1
  files_modified: 0
---

# Phase 121 Plan 03: HE-Mode RTL UAT Checklist Summary

**One-liner:** Per-surface HE-mode RTL UAT checklist for all 8 Joins Lab surfaces with PASS/FAIL line items, LTR-counter regression guard, outer-scroll regression guard, and drift-fix entry-point check — paused at human sign-off checkpoint (SC#2 acceptance gate).

## Status: PAUSED AT CHECKPOINT (Task 2 — Human Verify)

Task 1 (checklist authoring) is complete and committed. Task 2 (Hillel's live HE-mode UAT pass) is a blocking human checkpoint and cannot be auto-approved.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Author per-surface HE-mode RTL UAT checklist artifact | d04a8554 | `.planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md` |

## Awaiting

**Task 2:** Hillel runs the HE-mode RTL UAT checklist against the live web app and fills in the Sign-off block. SC#2 is the load-bearing acceptance gate for the i18n polish phase.

## Deviations from Plan

None — plan executed exactly as written for Task 1.

## Known Stubs

None — this plan produces only a verification checklist document.

## Threat Flags

None — this plan produces a markdown checklist only; no new production code path, no untrusted input, no new endpoints.

## Self-Check: PASSED

- [x] `.planning/phases/121-i18n-polish/121-HE-UAT-CHECKLIST.md` created and verified
- [x] Automated verify command passed: "OK checklist authored"
- [x] Commit d04a8554 exists
- [x] 8 surfaces covered (Anchor, Builder, Grid, Table, Compare, Known-Joins, Dialogs, Entry Points)
- [x] LTR prev/next counter regression guard present (Surface 5, E2)
- [x] Outer-scroll regression guard present (Surface 5, E5)
- [x] Drift-fix entry-point check present (Surface 8, H4: 'פתח במעבדת הצירופים')
- [x] Sign-off block with per-surface results table and overall PASS/FAIL present
