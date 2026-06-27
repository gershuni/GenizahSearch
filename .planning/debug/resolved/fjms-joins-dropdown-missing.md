---
status: diagnosed
trigger: "Investigate why FJMS joins do NOT appear in the desktop app's joins dropdown, even though they DO appear in the JoinsDialog."
created: 2026-02-15T00:00:00Z
updated: 2026-02-15T00:55:00Z
---

## Current Focus

hypothesis: Dropdown and dialog use different code paths to fetch FJMS joins
test: Compare _update_joins_dropdown vs JoinsDialog FJMS fetching logic
expecting: Find missing FJMS service call in dropdown path
next_action: Search for _update_joins_dropdown and _get_fjms_joins methods

## Symptoms

expected: FJMS joins appear in joins dropdown menu with [FJMS] prefix
actual: FJMS joins appear in JoinsDialog but NOT in dropdown menu
errors: None reported
reproduction: Open desktop app, view joins dropdown vs JoinsDialog
started: Issue reported after Phase 26 (commit 9f293ba) implementation

## Eliminated

## Evidence

- timestamp: 2026-02-15T00:10:00Z
  checked: genizah_app.py:_update_joins_dropdown (lines 6816-7043)
  found: Method has three code paths: (1) No joins → fallback to PGP+FJMS (lines 6845-6873, 6874-6915), (2) Has user joins → merge FJMS (lines 6996-7036), (3) Early returns on lines 6870 and 6913
  implication: FJMS fallback path exists but has early return

- timestamp: 2026-02-15T00:15:00Z
  checked: genizah_app.py lines 6874-6915 (FJMS fallback block)
  found: Line 6913 has `return` statement inside FJMS fallback block when fjms_valid list has items
  implication: When FJMS fallback finds joins, it displays them and returns, preventing merge logic from running

- timestamp: 2026-02-15T00:20:00Z
  checked: corrections_ui.py:JoinsDialog._display_connected_data (lines 4237-4350)
  found: Line 4340 calls _merge_fjms_joins_into_display AFTER displaying user joins, no early return
  implication: JoinsDialog always merges FJMS joins, even when user joins exist

- timestamp: 2026-02-15T00:25:00Z
  checked: Logic flow comparison
  found: Dropdown has two separate paths (fallback OR merge) with early returns. Dialog has one path (always merge FJMS after user joins).
  implication: Dropdown code structure prevents FJMS merging when user joins exist

## Resolution

root_cause: Key name mismatch in genizah_app.py FJMS dropdown code (both fallback and merge paths). The fjms_service.get_join_group() returns dicts with plural keys 'join_types' (list) and 'scholar_names' (list), but the dropdown code tries to access singular keys 'join_type' and 'scholar_name' at lines 6904-6909 (fallback) and 7026-7031 (merge). These .get() calls return empty strings, so no join type or scholar name appears in labels. While this doesn't prevent entries from appearing, if there's ANY other issue (like filtering), nothing shows. The JoinsDialog works because corrections_ui.py:3538-3540 uses the correct plural key names 'join_types' and 'scholar_names' with proper list handling.

- timestamp: 2026-02-15T00:30:00Z
  checked: Re-reading lines 6996-7036 (FJMS merge when user joins exist)
  found: Lines 6996-7036 show FJMS merge code that calls fjms_svc.get_join_group(document_id) and filters/displays results
  implication: Code to merge FJMS with user joins EXISTS and looks correct. If FJMS joins aren't appearing, either: (1) get_join_group returns empty, (2) all FJMS members get filtered out, or (3) an exception is caught at line 7035

- timestamp: 2026-02-15T00:35:00Z
  checked: shared/fjms_service.py:get_join_group return structure (lines 355-404)
  found: Returns list of dicts with keys: alma_id, join_group_ids, scholar_names (LIST), join_types (LIST), comment
  implication: get_join_group returns LISTS for scholar_names and join_types

- timestamp: 2026-02-15T00:40:00Z
  checked: genizah_app.py lines 7026-7031 (FJMS merge display code)
  found: Code accesses member.get('join_type', '') and member.get('scholar_name', '') (SINGULAR keys)
  implication: MISMATCH! Service returns 'join_types' (plural list) and 'scholar_names' (plural list), but dropdown merge code expects 'join_type' (singular) and 'scholar_name' (singular)

- timestamp: 2026-02-15T00:45:00Z
  checked: genizah_app.py lines 6904-6909 (FJMS fallback display code)
  found: Same issue - accesses member.get('join_type', '') and member.get('scholar_name', '')
  implication: Both fallback AND merge code have wrong key names. But user reports fallback works (JoinsDialog shows FJMS). Need to check JoinsDialog code.

- timestamp: 2026-02-15T00:50:00Z
  checked: corrections_ui.py:_get_fjms_joins lines 3534-3542
  found: Line 3538 accesses member.get('join_types', []) (PLURAL) and joins with ', '.join(). Line 3540 accesses member.get('scholar_names', []) (PLURAL) and joins.
  implication: JoinsDialog uses CORRECT plural key names. Dropdown code uses WRONG singular key names. This explains why JoinsDialog works but dropdown doesn't!

fix: Change genizah_app.py lines 6904-6909 and 7026-7031 to access 'join_types' and 'scholar_names' (plural) and join lists with ', '.join(), matching corrections_ui.py implementation at lines 3538-3540.

verification: Test with a manuscript that has both user joins and FJMS joins. FJMS entries should appear in dropdown with join type and scholar name labels. Also test standalone FJMS fallback (no user joins).

files_changed:
  - genizah_app.py (lines 6904-6909 and 7026-7031)
