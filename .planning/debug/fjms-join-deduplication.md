---
status: diagnosed
trigger: "Investigate the intra-FJMS join deduplication bug in Phase 26"
created: 2026-02-12T00:00:00Z
updated: 2026-02-12T00:10:00Z
---

## Current Focus

hypothesis: CONFIRMED - Service returns raw duplicates, UI layers handle inconsistently
test: Traced data flow and checked actual database
expecting: Service-level fix required
next_action: Return structured diagnosis

## Symptoms

expected: Each partner fragment appears exactly once with richest metadata (prefer entries with join_type)
actual:
  - Web: Deduplicates but keeps FIRST entry (may lose join_type)
  - Desktop: No deduplication, shows all duplicates
errors: None (logic bug)
reproduction: Navigate to sys_id 990001663820205171 (Ms. Evr. Antonin B 492), member of groups 7311 (no type) and 30000 (Physical Join)
started: Unknown, introduced in Phase 26 FJMS integration

## Eliminated

## Evidence

- timestamp: 2026-02-12T00:10:00Z
  checked: Database query for sys_id 990001663820205171
  found: Manuscript belongs to TWO join groups: 7311 (JoinType=None) and 30000 (JoinType="Physical Join"). Both groups have same 3 members.
  implication: Service returns 4 rows (2 partners × 2 groups), causing duplicates

- timestamp: 2026-02-12T00:15:00Z
  checked: fjms_service.py get_join_group() method (lines 196-232)
  found: Query returns ALL members from ALL groups WITHOUT deduplication. Just raw SQL join.
  implication: Service-level deduplication not happening

- timestamp: 2026-02-12T00:20:00Z
  checked: web/components/joins_panel.py lines 171-218
  found: Deduplicates by shelfmark.upper() in fragments_upper set (line 194). Uses set.add() so FIRST occurrence wins.
  implication: If group 7311 entry (no type) comes first, group 30000 entry (with type) is silently dropped

- timestamp: 2026-02-12T00:25:00Z
  checked: corrections_ui.py _get_fjms_joins() lines 3490-3548
  found: NO intra-FJMS deduplication at all. Each member from each group is added to lists.
  implication: Desktop shows duplicate partners (one per group)

- timestamp: 2026-02-12T00:30:00Z
  checked: corrections_ui.py _merge_fjms_joins_into_display() lines 3567-3612
  found: Deduplicates fragments by existing_fragments_upper (line 3569) and join pairs by existing_pairs (lines 3596-3611), but ONLY against user/PGP joins, NOT against other FJMS entries
  implication: If _get_fjms_joins returns duplicates, they all get added

## Resolution

root_cause: |
  fjms_service.py get_join_group() uses a simple SQL query that returns ALL rows
  from ALL join groups. When a manuscript belongs to multiple groups (common:
  20,088 manuscripts have joins, some in 25+ groups), the same partner appears
  multiple times with different metadata (join_type, scholar_name, etc).

  Neither UI layer handles intra-FJMS deduplication correctly:
  - Web (joins_panel.py:171-218): Deduplicates by shelfmark but keeps FIRST
    entry encountered. If a group without join_type comes first, richer
    metadata from later groups is silently lost.
  - Desktop (corrections_ui.py:3490-3548): NO intra-FJMS deduplication at all.
    All entries from all groups are returned, causing duplicate partners in UI.

fix: |
  Service-level deduplication in get_join_group() to return each partner ONCE
  with richest available metadata. Preference order:
  1. Prefer entries with join_type over NULL
  2. Among typed entries, prefer earlier group_id (arbitrary but stable)

  This eliminates duplicates at the source, ensuring both UIs work correctly
  without needing UI-layer deduplication logic.

verification: |
  1. Unit test: Multi-group manuscript (990001663820205171) returns 2 partners,
     not 4, each with "Physical Join" type
  2. Web UI: No loss of join_type metadata
  3. Desktop UI: No duplicate partners displayed
  4. Integration tests still pass

files_changed:
  - shared/fjms_service.py (get_join_group method - add GROUP BY with MAX logic)
  - tests/test_fjms_service.py (add test_get_join_group_multi_group_deduplication)
  - tests/test_fjms_joins_integration.py (add multi-group test cases)
