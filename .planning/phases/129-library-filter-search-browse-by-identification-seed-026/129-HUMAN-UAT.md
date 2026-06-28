---
status: issues_found
phase: 129-library-filter-search-browse-by-identification-seed-026
source: [129-VERIFICATION.md]
started: 2026-06-28T16:57:22Z
updated: 2026-06-28T17:30:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. Web search filter — visual / RTL smoke test
expected: In the Hebrew UI, the "סינון לפי ספרייה" (Filter by library) control appears beside the PGP/Printed buttons. Selecting one or more libraries narrows the result list (over the full set, not just the visible page), the per-library facet counts render correctly with 0-match libraries hidden, the active selection shows as a removable Hebrew chip, the results count shows the "(מסנן ספרייה)" indicator, and clicking the chip × restores the full result set. No English leak under Hebrew.
result: ISSUES — (GAP-A) the filter button does not render; (GAP-B) clicking "Filter by Domains" opens both menus; (GAP-C) wrong control type — must be a checkbox dialog like Filter by Domains so users can hide specific libraries; (GAP-D) chips appear in the pre-search "search only in…" bar instead of the post-search filter area.

### 2. Web Browse-by-Identification — push-down total correctness
expected: On the catalog (Browse-by-Identification) page, selecting a library from the new dropdown-checklist changes the total count correctly (reflects the full filtered set, not just the current page), pagination stays correct, the filter composes with the SEED-023 PGP/Editions filters (combining them ANDs correctly), per-code chips are removable, and clearing restores the unfiltered total. Labels render in Hebrew under the Hebrew UI.
result: ISSUES — (GAP-E) must use a checkbox dialog, not the dropdown; (GAP-F) "Search in these results" does not work when libraries are selected (selection dropped / button disabled). Push-down total/pagination not re-confirmed due to control issues.

### 3. Desktop catalog — visual smoke test
expected: On the desktop catalog Browse-by-Identification view, the library-filter QPushButton opens a Hebrew-labeled QMenu of checkable libraries with NO "My Library" / LOCAL option. Selecting e.g. CUL narrows the catalog results (correct total/pagination), the active selection appears as a Hebrew chip with per-code remove + clear-all, and removing restores results. Existing desktop search-results library/shelfmark filtering is unaffected.
result: ISSUES — (GAP-G) must use checkboxes in the tab, not a QMenu; (GAP-H) "search within these results" must compose with the library filter.

## Summary

total: 3
passed: 0
issues: 3
pending: 0
skipped: 0
blocked: 0

## Gaps

All 8 gaps (GAP-A .. GAP-H) are documented with root cause + remediation map in `129-VERIFICATION.md` (Gaps section). They route to gap-closure planning (`/gsd:plan-phase 129 --gaps`).
