---
phase: 26-scientific-joins
verified: 2026-02-12T12:00:00Z
status: passed
score: 6/6 must-haves verified
re_verification: false
---

# Phase 26: Scientific Joins Verification Report

**Phase Goal:** Users can see scholarly join groups with attribution and navigate between related fragments
**Verified:** 2026-02-12T12:00:00Z
**Status:** passed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | When viewing a manuscript that has FJMS join data, the user sees other join group members in the Related Fragments panel | ✓ VERIFIED | `web/components/joins_panel.py:171-218` — FJMS joins merged into fetch_connected_fragments via fjms_svc.get_join_group(), added to formatted_joins with source='FJMS' |
| 2 | Each FJMS join entry shows the scholar name who identified the join | ✓ VERIFIED | `web/components/joins_panel.py:214` stores scholar_name in formatted_joins; `web/pages/browse.py:2036-2038` renders scholar name with italic gray text; `corrections_ui.py:3540` stores in created_by_username field |
| 3 | Each FJMS join entry shows the join type (Physical Join, Codex Join, etc.) | ✓ VERIFIED | `web/components/joins_panel.py:211` stores join_type in relationship_type; `web/pages/browse.py:2030-2035` displays via rel_label mapping with fallback to raw value; `corrections_ui.py:3538` includes relationship_type |
| 4 | The user can click an FJMS join member to navigate to that fragment's page | ✓ VERIFIED | `web/pages/browse.py:2019-2040` renders fragments as clickable links; `genizah_app.py:6488-6641` adds FJMS entries to joins_menu with _navigate_to_joined_fragment action |
| 5 | FJMS joins appear in the Related Fragments panel in both web and desktop apps | ✓ VERIFIED | Web: `web/components/joins_panel.py:171-218` + `web/pages/browse.py:2026-2038`; Desktop: `genizah_app.py:6488-6641` (dropdown) + `corrections_ui.py:3490-3548` (JoinsDialog) |
| 6 | FJMS joins coexist with existing user joins and PGP joins without duplication | ✓ VERIFIED | `web/components/joins_panel.py:194-195` deduplicates via fragments_upper check; `genizah_app.py:6620` deduplicates via existing_upper set; `corrections_ui.py:3569` deduplicates in _merge_fjms_joins_into_display |

**Score:** 6/6 truths verified

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `web/components/joins_panel.py` | FJMS join data merged into fetch_connected_fragments | ✓ VERIFIED | Lines 171-218: FJMS merge block imports get_fjms_service, calls get_join_group(document_id), resolves shelfmarks, deduplicates, adds to formatted_joins with source='FJMS', scholar_name, relationship_type fields |
| `web/pages/browse.py` | FJMS scholar and join type display in Related Fragments panel | ✓ VERIFIED | Lines 1992-2038: Extracts scholar_name into frag_info_map (L1998, L2001, L2003), renders purple FJMS badge (L2026-2027), displays scholar name in italic gray (L2036-2038), shows join type via rel_label (L2030-2035) |
| `genizah_app.py` | FJMS joins in desktop Browse tab dropdown menu | ✓ VERIFIED | Lines 6488-6641: Two integration points - (1) Standalone fallback when no other joins (L6488-6541), (2) Merge into existing dropdown (L6610-6641). Both call get_fjms_service().get_join_group(), resolve shelfmarks, deduplicate, add [FJMS] prefix labels with scholar_name and join_type |
| `corrections_ui.py` | FJMS joins in desktop JoinsDialog | ✓ VERIFIED | Lines 3490-3548: _get_fjms_joins() method follows same tuple pattern as _get_pgp_joins, returns (shelfmarks, joins, details). Lines 3550-3589: _merge_fjms_joins_into_display() deduplicates and merges. Called in load_joins (L3558), refresh_from_corrections (L3770), refresh_display (L3798) |
| `tests/test_fjms_joins_integration.py` | Integration tests for FJMS joins in both apps | ✓ VERIFIED | 315 lines, 7 test functions covering: merge behavior, deduplication with user joins, scholar name preservation, graceful degradation when unavailable, desktop dialog structure, self-join skipping. All tests passing (pytest output: 7 passed, 13 warnings in 1.92s) |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| `web/components/joins_panel.py` | `shared/fjms_service.py` | `get_fjms_service().get_join_group(sys_id)` | ✓ WIRED | L173 imports get_fjms_service from web.fjms_service (which re-exports from shared), L174-176 calls get_fjms_service(thread_safe=True).get_join_group(document_id), results processed L177-216 |
| `genizah_app.py` | `shared/fjms_service.py` | `get_fjms_service().get_join_group(sys_id)` | ✓ WIRED | L6488 imports from shared.fjms_service, L6489 calls get_fjms_service(), L6492 calls get_join_group(document_id). Second call L6610-6613 follows same pattern. Results used to build dropdown menu entries |
| `corrections_ui.py` | `shared/fjms_service.py` | `get_fjms_service().get_join_group(sys_id)` | ✓ WIRED | L3497 imports from shared.fjms_service, L3498 calls get_fjms_service(), L3502 calls get_join_group(self.document_id), results processed L3505-3545 into join entries with scholar attribution |

### Requirements Coverage

| Requirement | Status | Supporting Evidence |
|-------------|--------|---------------------|
| **JOIN-01**: User can see FJMS join group members in the Related Fragments panel | ✓ SATISFIED | Truth 1 verified — FJMS joins merged into fetch_connected_fragments and displayed in both apps |
| **JOIN-02**: Join display includes scholar attribution (who identified the join) | ✓ SATISFIED | Truth 2 verified — scholar_name extracted from fjms_svc.get_join_group(), stored in formatted_joins, rendered in both web and desktop |
| **JOIN-03**: Join display includes join type (Physical Join, Codex Join, etc.) | ✓ SATISFIED | Truth 3 verified — join_type stored in relationship_type field, displayed via label mapping in browse.py and corrections_ui table |
| **JOIN-04**: User can navigate to other fragments in a join group | ✓ SATISFIED | Truth 4 verified — Web renders clickable links, desktop adds menu actions with _navigate_to_joined_fragment |
| **JOIN-05**: FJMS joins integrated in Related Fragments panel in both apps | ✓ SATISFIED | Truth 5 verified — Full integration in web (joins_panel + browse.py) and desktop (genizah_app + corrections_ui) |

### Anti-Patterns Found

No blocker anti-patterns detected. Files scanned: `web/components/joins_panel.py`, `web/pages/browse.py`, `genizah_app.py`, `corrections_ui.py`.

- No TODO/FIXME/PLACEHOLDER comments related to FJMS
- No console.log-only implementations
- No empty return statements or stub handlers
- All FJMS merge blocks include proper error handling (try/except with debug logging)
- Deduplication implemented at all three merge points (web, desktop dropdown, desktop dialog)

### Human Verification Required

No human verification needed. All truths are programmatically verifiable and have been confirmed via:
- Code inspection showing complete implementation
- Integration tests covering all merge scenarios (7/7 passing)
- Wiring verification showing proper calls to FjmsService
- No visual-only or real-time behavior requirements

### Gaps Summary

No gaps found. All 6 observable truths verified, all 5 artifacts pass existence/substantive/wired checks, all 3 key links wired, all 5 requirements satisfied. Phase goal achieved.

---

_Verified: 2026-02-12T12:00:00Z_
_Verifier: Claude (gsd-verifier)_
