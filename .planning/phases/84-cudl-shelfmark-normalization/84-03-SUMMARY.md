---
phase: 84-cudl-shelfmark-normalization
plan: "03"
subsystem: shared
tags: [normalization, cudl, shelfmark, or-patterns, numeric-collapse, forward-lookup, allowlist]
dependency_graph:
  requires:
    - shared/shelfmark_bridge.py (cudl_normalize, _index_key_for_label, _is_collision_key, build_alias_index from Plans 01+02)
    - reports/leading_zero_collisions.csv (D-06 gate file from Plan 01)
  provides:
    - shared/shelfmark_bridge._collapse_numeric_runs (NORM-02 Or.-only numeric-collapse)
    - shared/shelfmark_bridge.build_alias_index (extended with Or.-collapse path, Codex MEDIUM #5)
    - shared/shelfmark_bridge.lookup_cudl (3-tier cascade: slug + forward-label + Or.-collapse)
    - shared/shelfmark_bridge.shelfmark_to_cudl_label (conservative allowlist, Codex HIGH #3)
    - shared/shelfmark_bridge._SUPPORTED_CUDL_PATTERNS (documented allowlist constant)
  affects:
    - Plan 84-04 (wiring shelfmark_to_cudl_label into browse.py + 3 other call sites)
    - Plan 84-05 (integration test suite uses all bridge functions)
tech_stack:
  added: []
  patterns:
    - Or.-only gate for numeric-collapse (Codex MEDIUM #5 defense-in-depth)
    - 3-tier lookup cascade (normalize slug -> forward-label -> Or.-collapse)
    - Conservative forward lookup with documented _SUPPORTED_CUDL_PATTERNS allowlist
    - Single source of truth: Mosseri branch delegates to module-level _index_key_for_label
key_files:
  created: []
  modified:
    - shared/shelfmark_bridge.py
decisions:
  - "_collapse_numeric_runs gates on 3+ dot-separated digit parts so 2-group runs like '48.211' are unchanged (matches CUDL behavior)"
  - "Or.-only gate in build_alias_index uses base_key.startswith('or') and base_key[2].isdigit() — mirrors same check in lookup_cudl tier 3 for symmetry"
  - "shelfmark_to_cudl_label uses _SUPPORTED_CUDL_PATTERNS (Or., T-S, Add.) not a generic CUL fallback — avoids routing browse.py to 404s on unknown subcollections (Codex HIGH #3)"
  - "Mosseri branch in shelfmark_to_cudl_label delegates entirely to _index_key_for_label(construct_mosseri_cudl_label(shelfmark)) — no inline zfill loop (Round 3 Codex HIGH #1)"
  - "lookup_cudl body is EXTENDED not replaced — Plan 02's tier 2 forward-label fallback preserved (Round 3 Codex HIGH #2)"
metrics:
  duration: "~10 minutes"
  completed: "2026-05-06"
  tasks_completed: 1
  tasks_total: 1
  files_created: 0
  files_modified: 1
---

# Phase 84 Plan 03: Or.-Pattern Indexing + Leading-Zero Collision Exclusion + Conservative shelfmark_to_cudl_label Summary

**One-liner:** Or.-only numeric-collapse (_collapse_numeric_runs gated to ^or\d variants), 3-tier lookup_cudl cascade preserving Plan 02's forward-label fallback, and conservative shelfmark_to_cudl_label with _SUPPORTED_CUDL_PATTERNS allowlist delegating Mosseri to module-level _index_key_for_label.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Or.-only numeric-collapse + 3-tier lookup_cudl + conservative shelfmark_to_cudl_label | 289ae0fe | shared/shelfmark_bridge.py |

## What Was Built

### shared/shelfmark_bridge.py (modified)

Four additions replacing the Plan 01/02 stubs and extending existing functions:

**`_NUMERIC_RUN_RE`** — new module-level compiled regex matching dot-separated numeric runs, used by `_collapse_numeric_runs`.

**`_collapse_numeric_runs(s: str) -> str`** — new function (NORM-02). Collapses dot-separated numeric runs into a single concatenated number when there are 3+ dot groups: `or1080.1.1` → `or1080.11`, `or1080.5.30` → `or1080.530`. Two-group runs like `tsar48.211` are unchanged. Per Codex MEDIUM #5, callers gate to Cambridge Or. variants only.

**`build_alias_index()` extended** — after the generic CUL/Mosseri path, adds the Or.-only numeric-collapse path gated on `base_key.startswith('or') and base_key[2].isdigit()` (Codex MEDIUM #5). Collapsed aliases excluded from `_COLLISION_KEYS` are safe; the strict single-sys_id ambiguity policy from Plan 02 still catches any Or. collapsed key that would collide across fragments.

**`lookup_cudl()` extended to 3-tier cascade** (Round 3 Codex HIGH #2 — extend, not replace):
- Tier 1: `cudl_normalize(classmark)` — plain CUDL slug inputs
- Tier 2: `_index_key_for_label(classmark)` — forward-label `MS-MOSSERI-III-00027-O` style (Plan 02, preserved)
- Tier 3: `_collapse_numeric_runs(k1)` — Or.-only numeric-collapse retry (Plan 03, Or.-gated)

**`_SUPPORTED_CUDL_PATTERNS`** — new module-level constant documenting the allowlist: `Or.` (IGNORECASE), `T-S`, `Add.` (Codex HIGH #3).

**`shelfmark_to_cudl_label(shelfmark)` implemented** (was a stub returning None):
- Mosseri shelfmarks: calls `construct_mosseri_cudl_label(shelfmark)` then delegates to `_index_key_for_label(mosseri_label)` — single source of truth, no inline zfill loop (Round 3 Codex HIGH #1)
- Allowlist patterns (Or., T-S, Add.): passes through `cudl_normalize`; Or. numeric forms additionally apply `_collapse_numeric_runs` (Codex HIGH #2 Round 2)
- Uncertain forms (Halper, Yevr., ENA-MS, etc.): returns `None` so callers keep v7.10 `.replace(' ', '-')` fallback (Codex HIGH #3)

## Verify Results

All 14 acceptance criteria assertions pass:
- `_index_key_for_label('MS-MOSSERI-III-00027-O') == 'mosseriiii27o'` (Round 3 HIGH #1)
- `_collapse_numeric_runs('or1080.1.1') == 'or1080.11'`
- `_collapse_numeric_runs('tsar48.211') == 'tsar48.211'` (2-group unchanged)
- `lookup_cudl('or1080j15')` → sys_id 990010 (Or. letter-suffix)
- `lookup_cudl('or1080.1.1')` → sys_id 990011 (tier-3 collapse)
- `lookup_cudl('or1080.11')` → sys_id 990011 (direct slug)
- `lookup_cudl('MS-MOSSERI-III-00027-O')` → sys_id 990012 (Plan 02 tier-2 preserved)
- `shelfmark_to_cudl_label('Moss. III,27O') == 'mosseriiii27o'` (Mosseri delegation)
- `shelfmark_to_cudl_label('Or. 1080 J 15') == 'or1080j15'`
- `shelfmark_to_cudl_label('Or. 1080.1.1') == 'or1080.11'` (numeric-collapse in forward direction)
- `shelfmark_to_cudl_label('T-S Ar. 48.211') == 'tsar48.211'`
- `shelfmark_to_cudl_label('Add. 863, 2') == 'add863.2'`
- `shelfmark_to_cudl_label('Halper 331') is None` (not in allowlist)
- `shelfmark_to_cudl_label('Yevr. III B 1093') is None`, `shelfmark_to_cudl_label('ENA-MS 2956') is None`
- Codex MEDIUM #5: `or9999.11` in index, `xz1.23` NOT in index (Or.-only gate)

All 5 Plan 02 ambiguity unit tests still pass.

## Deviations from Plan

None — plan executed exactly as written. All Round 3 Codex findings addressed: HIGH #1 (single Mosseri transform), HIGH #2 (extend not replace lookup_cudl), HIGH #3 (conservative allowlist), MEDIUM #5 (Or.-only gate).

## Known Stubs

None. All stubs from Plans 01-03 have been implemented. The bridge module is complete pending Plan 04 wiring into runtime call sites.

## Threat Flags

None. This plan modifies a pure-function normalization module only. No network endpoints, no auth paths, no Supabase writes, no schema changes.

## Self-Check: PASSED

- `shared/shelfmark_bridge.py` modified and importable: CONFIRMED
- `_collapse_numeric_runs` defined at module level: CONFIRMED
- `_SUPPORTED_CUDL_PATTERNS` defined at module level: CONFIRMED
- `shelfmark_to_cudl_label` fully implemented (not a stub): CONFIRMED
- `lookup_cudl` has 3-tier cascade with _index_key_for_label AND _collapse_numeric_runs: CONFIRMED
- Plan 03 verify script exits 0 with ALL VERIFY CHECKS PASSED: CONFIRMED
- 5 existing ambiguity tests still pass: CONFIRMED
- Commit `289ae0fe` exists: CONFIRMED
- No file deletions: CONFIRMED
- No modifications to genizah_core.py, web/pages/browse.py, shared/nli_crossref_service.py: CONFIRMED
