---
phase: 26-scientific-joins
verified: 2026-02-12T06:30:19Z
status: passed
score: 7/7 must-haves verified
re_verification:
  previous_status: passed
  previous_score: 6/6
  previous_date: 2026-02-12T12:00:00Z
  gaps_closed:
    - "Multi-group deduplication with aggregated metadata (gap from UAT)"
  gaps_remaining: []
  regressions: []
---

# Phase 26: Scientific Joins Re-Verification Report

**Phase Goal:** Users can see scholarly join groups with attribution and navigate between related fragments
**Verified:** 2026-02-12T06:30:19Z
**Status:** PASSED
**Re-verification:** Yes — after gap closure from UAT

## Re-Verification Summary

**Previous verification (2026-02-12T12:00:00Z):** 6/6 truths verified, status: passed

**UAT Gap Found:** When a manuscript belongs to multiple FJMS join groups, duplicates appeared in desktop and web lost richer metadata (join_type) due to first-encountered-wins deduplication.

**Gap Closure (Plan 26-02):** Implemented GROUP BY + GROUP_CONCAT deduplication at service level (get_join_group()), aggregating all distinct scholar names and join types into lists. Both web and desktop now display comma-separated scholars and types.

**Current verification:** 7/7 truths verified (added 1 new truth for multi-group behavior), status: passed

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | When viewing a manuscript that has FJMS join data, the user sees other join group members in the Related Fragments panel | ✓ VERIFIED | web/components/joins_panel.py:171-218 — FJMS joins merged into fetch_connected_fragments via fjms_svc.get_join_group() |
| 2 | Each FJMS join entry shows the scholar name who identified the join | ✓ VERIFIED | web/components/joins_panel.py:214 stores comma-joined scholar_names; corrections_ui.py:3540 displays in created_by_username |
| 3 | Each FJMS join entry shows the join type (Physical Join, Codex Join, etc.) | ✓ VERIFIED | web/components/joins_panel.py:211 stores comma-joined join_types in relationship_type |
| 4 | The user can click an FJMS join member to navigate to that fragment page | ✓ VERIFIED | web/pages/browse.py:2019-2040 renders clickable links; genizah_app.py:6488-6641 adds menu actions |
| 5 | FJMS joins appear in the Related Fragments panel in both web and desktop apps | ✓ VERIFIED | Web: joins_panel.py + browse.py; Desktop: genizah_app.py + corrections_ui.py |
| 6 | FJMS joins coexist with existing user joins and PGP joins without duplication | ✓ VERIFIED | Deduplication in web (L194-195), desktop (L6620, L3569) |
| 7 | When a manuscript belongs to multiple FJMS join groups, each partner appears once with ALL contributing scholars and join types aggregated | ✓ VERIFIED (NEW) | shared/fjms_service.py:203-252 — GROUP BY AlmaId with GROUP_CONCAT(DISTINCT). Tests pass: test_get_join_group_deduplicates_across_groups, test_get_join_group_aggregates_scholars, test_get_join_group_aggregates_join_types, test_fjms_multi_group_shows_all_scholars, test_get_fjms_joins_multi_group_aggregated. Full suite: 507 passed. |

**Score:** 7/7 truths verified

### Required Artifacts

All artifacts verified at 3 levels (exists, substantive, wired):

- ✓ shared/fjms_service.py — GROUP BY + GROUP_CONCAT deduplication (L203-252), _split_concat helper (L196-201)
- ✓ web/components/joins_panel.py — Comma-join aggregated fields (L211,214)
- ✓ corrections_ui.py — Desktop comma-join aggregated fields (L3538,3540)
- ✓ tests/test_fjms_service.py — 4 new dedup/aggregation tests (L314-357), 31/31 pass
- ✓ tests/test_fjms_joins_integration.py — 2 new multi-group tests (L320, L347), 9/9 pass

### Key Links

All key links verified as WIRED:

- ✓ web/components/joins_panel.py → shared/fjms_service.py (L173-176, receives aggregated lists)
- ✓ corrections_ui.py → shared/fjms_service.py (L3497-3502, receives aggregated lists)
- ✓ genizah_app.py → shared/fjms_service.py (L6488-6641, receives deduplicated results)

### Requirements Coverage

All 5 requirements SATISFIED:

- ✓ JOIN-01: FJMS join group members visible (Truth 1)
- ✓ JOIN-02: Scholar attribution displayed (Truth 2, enhanced for multi-group in 26-02)
- ✓ JOIN-03: Join type displayed (Truth 3, enhanced for multi-group in 26-02)
- ✓ JOIN-04: Navigation works (Truth 4)
- ✓ JOIN-05: Both apps integrated (Truth 5)

### Gap Closure

**UAT Gap (Test 3):** Multi-group manuscripts showed duplicates (desktop) and lost metadata (web)

**Root Cause:** get_join_group() returned raw duplicate rows without deduplication

**Fix (Plan 26-02):**
1. Service: GROUP BY + GROUP_CONCAT in shared/fjms_service.py (commit 7e03329)
2. Consumers: Comma-join aggregated lists in web/desktop (commit fa5dd6c)
3. Tests: 6 new tests covering multi-group scenarios

**Gap Status:** ✅ CLOSED

**Verification:** 507 tests pass, no regressions, commits verified

### Anti-Patterns

No blockers detected. All error handling present, no stubs, no TODOs in implementation.

## Summary

Phase 26 goal ACHIEVED with gap closure complete.

- Initial (26-01): 6/6 truths verified
- UAT: 1 gap found (multi-group deduplication)
- Gap closure (26-02): 7/7 truths verified, gap closed
- Final: All requirements satisfied, 507 tests pass, ready for Phase 27

---

_Verified: 2026-02-12T06:30:19Z_
_Verifier: Claude (gsd-verifier)_
