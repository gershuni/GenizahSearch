---
phase: 27-domain-classifications
verified: 2026-02-13T18:30:00Z
status: passed
score: 21/21 must-haves verified
re_verification: true
previous_status: passed
uat_gaps_addressed: 3
---

# Phase 27: Domain Classifications RE-VERIFICATION Report

**Phase Goal:** Users can see what subject a manuscript belongs to and filter search results by domain

**Status:** PASSED - All UAT gaps closed

## Re-Verification Context

- **Initial Verification:** 2026-02-13T04:15:00Z (PASSED - 15/15 truths)
- **UAT Testing:** 2026-02-13T12:15:00Z (3 major gaps found)
- **Gap Closure:** Plans 27-04 (web), 27-05 (desktop) executed
- **Re-Verification:** 2026-02-13T18:30:00Z

The initial verification confirmed all technical requirements. UAT testing revealed UX issues requiring redesign from pre-search dropdown to post-search dynamic filtering.

## UAT Gaps - All CLOSED

### Gap 1: Web dropdown flattens hierarchy
- **Old:** Pre-search ui.select dropdown (flat)
- **New:** Post-search button+dialog with checkbox tree
- **Evidence:** domain_select removed (0 matches), _open_domain_filter_dialog() at line 1651

### Gap 2: Filtering should be post-search dynamic
- **Old:** Pre-search domain selection
- **New:** Post-search domain collection, instant client-side filtering
- **Evidence Web:** Lines 1959-1976 (collection), 1798-1829 (filtering)
- **Evidence Desktop:** Lines 14138-14164 (collection), 13739-13779 (filtering)

### Gap 3: Desktop needs exclude mode
- **Old:** Select-to-include with _selected_domains
- **New:** All-checked-by-default, uncheck-to-exclude with _domain_exclusions
- **Evidence:** _selected_domains removed (0 matches), _domain_exclusions added (10 matches)

## Observable Truths Verified: 21/21

### Original Truths (Still Work): 6/6
1. Web browse shows domain links
2. Desktop browse shows domain links
3. Web domain link navigates to search
4. Desktop domain link switches to search tab
5. Language-aware display
6. Parent/child deduplication

### Gap Closure Truths (New): 15/15
7. Web Domains button appears post-search
8. Web dialog shows result-specific checkbox tree
9. Web all checked by default
10. Web instant filtering without re-searching
11. Web button shows exclusion count
12. Web exclusions persisted
13. Desktop button enabled post-search
14. Desktop dialog shows result-specific domains
15. Desktop all checked by default
16. Desktop instant row hiding
17. Desktop label shows exclusion count
18. Desktop exclusions remembered
19. Web standalone browse removed
20. Desktop standalone browse removed
21. Hierarchy preserved in dialogs

## Requirements Coverage: 4/4

- DOM-01: Domain badges on browse - SATISFIED
- DOM-02: Filter by domain - SATISFIED
- DOM-03: Hierarchy preserved - SATISFIED
- DOM-04: Both apps - SATISFIED

## Key Implementation Changes

### Removed
- domain_select dropdown (web)
- _selected_domains state (desktop)
- _execute_domain_browse (both apps)
- Pre-search domain filtering

### Added
- Post-search domain collection via get_domains_for_sys_ids batch lookup
- Domains button (hidden/disabled until search with domain data)
- Modal checkbox tree dialog showing only result-specific domains
- All-checked-by-default exclude pattern
- Client-side instant filtering
- Domain exclusion persistence

## Edge Cases Verified

All edge cases handled in both apps:
- No domain data: button hidden/disabled
- All excluded: non-domain results still visible
- Remembered exclusions: re-applied on new search
- Cancel dialog: no state change
- Browse navigation: exclusions cleared
- Empty query: standalone browse removed

## Assessment

**Phase Goal ACHIEVED** - Users can see domain classifications and filter by domain with superior UX pattern.

Gap closure successfully addressed all UAT issues.

---

_Re-Verified: 2026-02-13T18:30:00Z by Claude (gsd-verifier)_
