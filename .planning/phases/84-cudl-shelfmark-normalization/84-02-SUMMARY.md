---
phase: 84-cudl-shelfmark-normalization
plan: "02"
subsystem: shared
tags: [normalization, cudl, shelfmark, alias-index, mosseri, ambiguity-exclusion]
dependency_graph:
  requires:
    - shared/shelfmark_bridge.py (cudl_normalize, _is_collision_key from Plan 01)
  provides:
    - shared/shelfmark_bridge.build_alias_index (strict ambiguity-exclusion policy)
    - shared/shelfmark_bridge.lookup_cudl (two-tier: slug + forward-label)
    - shared/shelfmark_bridge._index_key_for_label (module-level, reusable by Plan 03)
    - reports/cudl_alias_collisions.csv (written at every build_alias_index call)
  affects:
    - Plan 84-03 (shelfmark_to_cudl_label reuses _index_key_for_label)
    - Plan 84-04 (wiring build_alias_index into MetadataManager._load_csv_bank)
    - Plan 84-05 (integration test suite uses build_alias_index against real csv_bank)
tech_stack:
  added: []
  patterns:
    - Builder-dict ambiguity detection (collect claims -> materialize unambiguous only)
    - Module-level helper function for single source of truth (_index_key_for_label)
    - Optional test-injectable path parameter (report_path) for diagnostic artifact isolation
    - Late import inside function body to break genizah_core <-> shelfmark_bridge cycle
key_files:
  created:
    - tests/test_shelfmark_bridge_ambiguity.py
  modified:
    - shared/shelfmark_bridge.py
decisions:
  - "Strict ambiguity-exclusion: keys mapping to >1 distinct sys_id are excluded entirely (fail-loud over silent wrong-answer) — Codex HIGH #2"
  - "_index_key_for_label promoted to module level (not nested in build_alias_index) so Plan 03's shelfmark_to_cudl_label can call the same transform — Round 3 Codex HIGH #1"
  - "report_path parameter added to build_alias_index() so unit tests write to tmp_path — Round 3 Codex MEDIUM"
  - "Mosseri forward path uses _index_key_for_label(construct_mosseri_cudl_label(variant)); generic CUL path uses cudl_normalize(variant) directly"
  - "Same sys_id contributing multiple normalized-key variants is NOT ambiguous (all point to same row); only different sys_ids on same key triggers exclusion"
metrics:
  duration: "~12 minutes"
  completed: "2026-05-06"
  tasks_completed: 2
  tasks_total: 2
  files_created: 1
  files_modified: 1
---

# Phase 84 Plan 02: Mosseri Alias Index + Ambiguity Exclusion Policy Summary

**One-liner:** build_alias_index() with strict Codex HIGH #2 ambiguity-exclusion (builder dict, single-sys_id gate) + module-level _index_key_for_label (Round 3 Codex HIGH #1 single source of truth) + lookup_cudl() two-tier implementation (slug + forward-label) + 5 unit tests with tmp_path report injection (Round 3 Codex MEDIUM).

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Implement build_alias_index() + _index_key_for_label + lookup_cudl() | 8a84b24d | shared/shelfmark_bridge.py |
| 2 | Add ambiguity policy unit tests (use tmp_path report) | 77f06b4a | tests/test_shelfmark_bridge_ambiguity.py |

## What Was Built

### shared/shelfmark_bridge.py (modified)

Three stubs replaced with real implementations:

**`_index_key_for_label(label)`** — NEW module-level function (Round 3 Codex HIGH #1). Converts a forward CUDL label (`MS-MOSSERI-III-00027-O`) into the index key form (`mosseriiii27o`) by: stripping the leading `MS` segment, collapsing zfill padding from numeric segments, then running through `cudl_normalize`. Must be module-level because Plan 03's `shelfmark_to_cudl_label` and `lookup_cudl` extension both need the same transform.

**`build_alias_index(csv_bank, report_path=None)`** — replaces the stub (was `pass`). Uses the builder-dict pattern (Codex HIGH #2):
1. Walks all CUL and Mosseri rows in `csv_bank`
2. For Mosseri variants: calls `construct_mosseri_cudl_label(variant)` (late import from genizah_core), then `_index_key_for_label()` to get the index key
3. For all CUL/Mosseri variants: also indexes `cudl_normalize(variant)` directly (generic path)
4. Collects all `(sys_id, shelfmark)` claims per key into a `defaultdict(set)`
5. Materializes ONLY keys with exactly 1 distinct sys_id into `_CUDL_ALIAS_INDEX`
6. Writes excluded keys to `reports/cudl_alias_collisions.csv` (or `report_path` if supplied)
7. Emits one `WARNING` log line: `"alias index built: N keys, M ambiguous keys excluded"`

**`_write_alias_collision_report(ambiguous, report_path=None)`** — new helper. Writes `key,sys_ids,shelfmarks` CSV. OSError-safe (debug-level log on failure, no raise).

**`lookup_cudl(classmark)`** — replaces the stub. Two-tier lookup:
- Tier 1: `cudl_normalize(classmark)` — covers CUDL slug inputs like `mosseriiii27o`
- Tier 2: `_index_key_for_label(classmark)` — covers forward-label inputs like `MS-MOSSERI-III-00027-O`
Returns `None` if both tiers miss OR if the key was excluded as ambiguous.

### tests/test_shelfmark_bridge_ambiguity.py (new)

5 unit tests in `TestAmbiguityExclusion`:
1. `test_two_distinct_sys_ids_same_key_are_excluded` — verifies lookup_cudl returns None and key appears in report
2. `test_same_sys_id_multiple_paths_is_not_ambiguous` — verifies single-sys_id multi-variant still resolves
3. `test_collision_report_header` — verifies CSV header is `key,sys_ids,shelfmarks`
4. `test_three_way_ambiguity_excluded` — 3-way collision scenario excluded + in report
5. `test_real_reports_dir_not_mutated` — regression: reports/cudl_alias_collisions.csv unchanged by test run

All 5 tests pass. Every `build_alias_index()` call uses `report_path=tmp_path / 'collisions.csv'` (Round 3 Codex MEDIUM).

## Deviations from Plan

None — plan executed exactly as written. All three Codex findings addressed: HIGH #2 (ambiguity exclusion), Round 3 HIGH #1 (module-level _index_key_for_label), Round 3 MEDIUM (report_path parameter).

## Known Stubs

| Stub | File | Line | Reason |
|------|------|------|--------|
| `shelfmark_to_cudl_label()` | shared/shelfmark_bridge.py | ~280 | Plan 03 implements reverse-map (browse CUDL link) |

The `lookup_cudl()` stub from Plan 01 is now fully implemented. `shelfmark_to_cudl_label()` remains a stub per plan scope.

## Threat Flags

None. This plan modifies a pure-function normalization module and adds unit tests. No network endpoints, no auth paths, no Supabase writes, no schema changes.

## Self-Check: PASSED

- `shared/shelfmark_bridge.py` imports successfully: CONFIRMED
- `_index_key_for_label` is module-level (not nested): CONFIRMED
- `build_alias_index` has `report_path` parameter: CONFIRMED
- `_index_key_for_label('MS-MOSSERI-III-00027-O') == 'mosseriiii27o'`: CONFIRMED
- Mosseri slug lookup `lookup_cudl('mosseriiii27o')` returns correct sys_id: CONFIRMED
- Forward-label lookup `lookup_cudl('MS-MOSSERI-III-00027-O')` returns correct sys_id: CONFIRMED
- Ambiguous key (2 sys_ids) returns None from lookup_cudl: CONFIRMED
- Ambiguous key appears in collision report: CONFIRMED
- Same-sys_id multi-variant resolves correctly: CONFIRMED
- 5 unit tests pass: CONFIRMED
- Commit `8a84b24d` exists: CONFIRMED
- Commit `77f06b4a` exists: CONFIRMED
- No modifications to genizah_core.py, web/pages/browse.py, shared/nli_crossref_service.py: CONFIRMED
