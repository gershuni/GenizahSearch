---
status: partial
phase: 129-library-filter-search-browse-by-identification-seed-026
source: [129-VERIFICATION.md]
started: 2026-06-28T16:57:22Z
updated: 2026-06-28T22:10:00Z
round: 2 (re-smoke after gap-closure plans 129-05/06/07)
---

## Current Test

[awaiting human re-smoke of the redesigned checkbox-dialog UX]

## History

Round 1 (2026-06-28) found 8 UX gaps (GAP-A..GAP-H) — menu/dropdown control, misplaced chips,
broken "search within results". Closed by gap-closure plans 129-05/06/07 (menu/dropdown → checkbox
dialog mirroring "Filter by Domains"; post-search chips; search-within now threads the library
selection). Both code-review gates (internal + Codex CODE, converged APPROVE) passed; 64 tests green.

## Tests

### 1. Web /search — checkbox dialog, chip placement, no menu collision
expected: After a search returning multiple libraries, the "סינון לפי ספרייה" button is VISIBLE (GAP-A). Clicking it opens a CHECKBOX DIALOG (not a dropdown menu) like "Filter by Domains" (GAP-C). Apply is disabled when all boxes are unchecked; "Select all" re-enables it (all-unchecked guard). Selecting e.g. CUL + Apply narrows results over the full set; a removable chip appears in the POST-search filter row (not the pre-search "search only in…" bar) (GAP-D); chip × restores all. The "Filter by Domains" button opens ONLY the domain dialog (GAP-B). No English under Hebrew.
result: [pending]

### 2. Web Browse-by-Identification — dialog, push-down, search-within, no parallels leak
expected: The catalog library control is a CHECKBOX DIALOG (GAP-E). Selecting libraries changes the total correctly (push-down, composes with PGP/Editions). "Search in these results" is ENABLED and carries the library selection to /search (chip visible, results narrowed) (GAP-F); reloading /search persists it. Navigating catalog→parallels with a library selected does NOT silently leak the library filter into a later fresh /search (WR-01 fix). No English under Hebrew.
result: [pending]

### 3. Desktop catalog — dialog, search-within, recompute preserves library
expected: The desktop catalog library control is a CHECKBOX DIALOG (not a QMenu), with NO "My Library"/LOCAL option (GAP-G); OK disabled at zero-checked. Selecting CUL narrows the catalog; a removable Hebrew chip appears; removing restores. "Search in these results" / "Parallels in these results" carry the library scope to the search tab, where it shows as a removable "Library: …" chip (GAP-H); removing a DIFFERENT (e.g. domain) chip afterward PRESERVES the library restriction (FilterCountWorker recompute). No English under Hebrew.
result: [pending]

## Summary

total: 3
passed: 0
issues: 0
pending: 3
skipped: 0
blocked: 0

## Gaps

Round-1 gaps GAP-A..GAP-H are all CLOSED by code evidence (see 129-VERIFICATION.md re-verification).
Awaiting human re-smoke confirmation of the 3 surfaces above.
