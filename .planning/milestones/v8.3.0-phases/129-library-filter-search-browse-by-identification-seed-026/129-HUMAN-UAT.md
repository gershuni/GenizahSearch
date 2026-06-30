---
status: partial
phase: 129-library-filter-search-browse-by-identification-seed-026
source: [129-VERIFICATION.md]
started: 2026-06-28T16:57:22Z
updated: 2026-06-29T00:00:00Z
round: 3 (chips → button-state redesign; commit f8cb048b)
---

## Current Test

[awaiting human re-smoke of the chips→button-state redesign]

Round-3 change (user feedback "the chips can be a lot and don't really help"):
per-library chips were REMOVED on all three surfaces. The filter state now lives
on the filter control itself — when a strict subset of the relevant libraries is
shown, the button turns RED and reads "Filter Libraries (shown/total)"
(e.g. "(12/14)"); otherwise it is the neutral "Filter by library" / "All Libraries".
On the desktop SEARCH tab (which has no library button) the per-code chips collapse
to a single removable "Library (N)" summary chip.

## History

Round 1 (2026-06-28) found 8 UX gaps (GAP-A..GAP-H) — menu/dropdown control, misplaced chips,
broken "search within results". Closed by gap-closure plans 129-05/06/07 (menu/dropdown → checkbox
dialog mirroring "Filter by Domains"; post-search chips; search-within now threads the library
selection). Both code-review gates (internal + Codex CODE, converged APPROVE) passed; 64 tests green.

## Tests

### 1. Web /search — checkbox dialog, button-state, no menu collision
expected: After a search returning multiple libraries, the "סינון לפי ספרייה" button is VISIBLE as soon as results render (GAP-A). Clicking it opens a CHECKBOX DIALOG (not a dropdown menu) like "Filter by Domains" (GAP-C). Apply is disabled when all boxes are unchecked; "Select all" re-enables it (all-unchecked guard). Selecting e.g. a subset + Apply narrows results over the full set; the button turns RED and reads "סינון ספריות (shown/total)" — there are NO chips (round-3 redesign). Re-opening + Select All + Apply restores all and reverts the button to neutral. The "Filter by Domains" button opens ONLY the domain dialog (GAP-B). No English under Hebrew.
result: [pending]

### 2. Web Browse-by-Identification — dialog, push-down, search-within, no parallels leak
expected: The catalog library control is a CHECKBOX DIALOG (GAP-E). Selecting libraries changes the total correctly (push-down, composes with PGP/Editions); the button turns RED + "Filter Libraries (shown/total)" — no chips. "Search in these results" is ENABLED and carries the library selection to /search (results narrowed, search button shows its RED state) (GAP-F); reloading /search persists it. Navigating catalog→parallels with a library selected does NOT silently leak the library filter into a later fresh /search (WR-01 fix). No English under Hebrew.
result: [pending]

### 3. Desktop catalog — dialog, button-state, search-within, recompute preserves library
expected: The desktop catalog library control is a CHECKBOX DIALOG (not a QMenu) with a "Select None" button, NO "My Library"/LOCAL option, alphabetical order (GAP-G); OK disabled at zero-checked. Selecting a subset narrows the catalog; the catalog library button turns RED + "Filter Libraries (shown/total)" — no chips; Clear-all / Search-in-results stay enabled. "Search in these results" / "Parallels in these results" carry the library scope to the search tab, where it shows as a SINGLE removable "Library (N)" summary chip (GAP-H); removing a DIFFERENT (e.g. domain) chip afterward PRESERVES the library restriction (FilterCountWorker recompute). No English under Hebrew.
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
