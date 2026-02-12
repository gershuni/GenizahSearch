---
status: diagnosed
trigger: "Investigate this UAT issue for Phase 26 (Scientific Joins): When a fragment appears in both PGP and FJMS join sources, the user sees only one entry with the PGP badge (blue). They want to see both source badges (PGP blue + FJMS purple) on the single deduplicated entry."
created: 2026-02-12T00:00:00Z
updated: 2026-02-12T00:15:00Z
---

## Current Focus

hypothesis: CONFIRMED - FJMS merge logic drops duplicate entries entirely instead of merging source attributions
test: Complete - Read both web and desktop implementations
expecting: CONFIRMED - Found exact dedup locations and single-source data structure
next_action: Return diagnosis with fix recommendations

## Symptoms

expected: When fragment appears in both PGP and FJMS, show single entry with BOTH badges (PGP blue + FJMS purple)
actual: User sees only one entry with PGP badge (blue), FJMS attribution is lost
errors: None reported
reproduction: Search for fragment that exists in both PGP and FJMS join sources
started: Phase 26 UAT testing

## Eliminated

## Evidence

- timestamp: 2026-02-12T00:05:00Z
  checked: web/components/joins_panel.py lines 171-218 (FJMS merge block)
  found: Line 194-195 implements deduplication - if resolved_shelfmark.upper() in fragments_upper, continue (skips entire FJMS entry)
  implication: When a fragment exists in PGP, FJMS entry is dropped completely including source attribution

- timestamp: 2026-02-12T00:06:00Z
  checked: web/components/joins_panel.py lines 434-453 (relationship_map building)
  found: Lines 444-447 build relationship_map with single source per fragment - if frag_a not in map, then add. First source wins.
  implication: Each fragment can only have ONE source in the map. Second source never gets added.

- timestamp: 2026-02-12T00:07:00Z
  checked: web/components/joins_panel.py lines 539-542 (badge rendering)
  found: Lines 539-542 render badge based on relationship_map[frag]['source'] - single source value, renders single badge
  implication: UI only designed to show one badge per fragment. relationship_map structure doesn't support multiple sources.

- timestamp: 2026-02-12T00:08:00Z
  checked: corrections_ui.py lines 3596-3609 (desktop FJMS dedup)
  found: Lines 3597-3609 deduplicate FJMS joins against existing_pairs - if pair already exists, skip FJMS join
  implication: Desktop has same deduplication behavior - FJMS join dropped if fragment already present from user/PGP

- timestamp: 2026-02-12T00:10:00Z
  checked: corrections_ui.py lines 3613-3642 and 3953-3989 (desktop source display)
  found: Desktop displays source in table column 3. Line 3632 shows 'FJMS' in purple, line 3973 shows 'PGP' in green. Each row = one source.
  implication: Desktop also displays only ONE source per join row. Same issue as web - when dedup happens, source attribution lost.

- timestamp: 2026-02-12T00:12:00Z
  checked: Desktop table structure
  found: Desktop shows joins as table rows (fragment_a, fragment_b, relationship_type, SOURCE, created_by, created_at). Source is single column value.
  implication: Desktop would need similar fix - either allow duplicate rows with different sources, or aggregate sources into single cell

## Resolution

root_cause: Three-part architectural issue preventing dual badges:

**Part 1: Pipeline Stage Deduplication (web/components/joins_panel.py)**
  - Line 154-156: PGP merge drops duplicates from user joins
  - Line 193-195: FJMS merge drops duplicates from user/PGP joins
  - When fragment appears in both PGP and FJMS, FJMS entry is completely discarded (never reaches formatted_joins list)

**Part 2: Single-Source Data Structure**
  - Lines 162-169: PGP creates join entry with source='PGP'
  - Lines 207-216: FJMS creates join entry with source='FJMS'
  - formatted_joins is a flat list where each entry has ONE source field
  - When fragment appears in both, only one entry exists (PGP wins due to merge order)

**Part 3: Display Layer Limitation**
  - Lines 444-447: relationship_map[fragment] = {'type': ..., 'source': ...} - single source per fragment
  - Lines 539-542: Badge rendering reads single source value and renders one badge
  - No code path to aggregate multiple sources for same fragment

**Concrete Example:**
Fragment "T-S 10J5.1" appears in:
  - PGP document #3750 → formatted_joins gets entry with source='PGP'
  - FJMS join group #42 → Line 194 sees "T-S 10J5.1" already in fragments_upper → continue (dropped)
  - Result: Only PGP badge shown, FJMS attribution lost

fix: Two-stage fix required:

**Stage 1: Aggregate sources instead of dropping duplicates**
  - Line 193-195: Instead of `continue`, check if this fragment already has a join entry in formatted_joins
  - If yes, append FJMS to a sources list on that entry
  - If no, create new entry
  - Change formatted_joins entries from source='X' to sources=['X'] or sources=['X', 'Y']

**Stage 2: Update display to show multiple badges**
  - Lines 444-447: When building relationship_map, aggregate sources from all join entries for same fragment
  - Lines 539-542: Loop through sources list and render multiple badges

**Alternative simpler approach (RECOMMENDED):**
  - Don't deduplicate FJMS at merge stage - allow duplicate fragment entries with different sources
  - Aggregate sources at display time (lines 444-447) when building relationship_map
  - This keeps formatted_joins simple (each entry = one source) but merges at UI layer
  - Web: Change relationship_map from {'source': str} to {'sources': list[str]}
  - Desktop: Either allow duplicate rows OR concatenate sources in single cell

**Implementation Details:**

Web (web/components/joins_panel.py):
1. Line 193-195: Remove the `continue` statement - let FJMS entries be added even if fragment exists
2. Lines 444-447: Change relationship_map building to aggregate sources:
   ```python
   if frag_a not in relationship_map:
       relationship_map[frag_a] = {'type': rel_type, 'sources': [source], 'scholar_name': scholar_name}
   else:
       if source not in relationship_map[frag_a]['sources']:
           relationship_map[frag_a]['sources'].append(source)
   ```
3. Lines 539-542: Loop through sources list and render multiple badges:
   ```python
   for src in rel_info.get('sources', []):
       if src == 'FJMS':
           ui.badge('FJMS').props('color=purple outline dense').classes('text-xs')
       elif src != 'user':
           ui.badge(src).props('color=blue outline dense').classes('text-xs')
   ```

Desktop (corrections_ui.py):
1. Lines 3597-3611: Remove deduplication OR
2. Lines 3632, 3973: Concatenate sources when duplicate pairs detected:
   - Check if pair exists, if yes, update existing row's source column to "PGP, FJMS" instead of inserting new row

verification: Test with fragment that appears in both PGP document and FJMS join group
files_changed:
  - web/components/joins_panel.py (lines 193-195, 444-447, 539-542)
  - corrections_ui.py (lines 3597-3642 or alternative concatenation approach)
