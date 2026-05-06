---
phase: 84-cudl-shelfmark-normalization
plan: "01"
subsystem: shared
tags: [normalization, cudl, shelfmark, audit, bridge]
dependency_graph:
  requires: []
  provides:
    - shared/shelfmark_bridge.py (cudl_normalize, _normalize_without_zero_collapse, _BUILTIN_COLLISION_KEYS)
    - reports/leading_zero_collisions.csv (D-06 gate file)
    - reports/cudl_full_normalization_collisions.csv (transparency dump)
  affects:
    - Plan 84-02 (build_alias_index implementation)
    - Plan 84-03 (shelfmark_to_cudl_label + _COLLISION_KEYS wiring)
    - Plan 84-04 (wiring bridge into 4 call sites)
tech_stack:
  added: []
  patterns:
    - Pure-function normalization module (no side effects at import time)
    - Audit-only sibling helper (_normalize_without_zero_collapse) for delta isolation
    - Built-in safety net set (_BUILTIN_COLLISION_KEYS) independent of external CSV
key_files:
  created:
    - shared/shelfmark_bridge.py
    - scripts/audit_leading_zero_collisions.py
    - reports/leading_zero_collisions.csv
    - reports/cudl_full_normalization_collisions.csv
  modified: []
decisions:
  - "Ported normalize() from scripts/scan_cudl_orphans.py VERBATIM as cudl_normalize() — no rule changes"
  - "Added _normalize_without_zero_collapse() as audit-only sibling to isolate leading-zero delta (Codex MEDIUM #4)"
  - "reports/ directory is in .gitignore as /Reports (case-insensitive on Windows); report files force-added with git add -f to satisfy plan artifact requirement"
  - "Audit result: 0 leading-zero delta collisions — gate file is header-only; no keys need exclusion for Plan 03"
  - "_BUILTIN_COLLISION_KEYS starts empty; will be populated if future audit runs find production collisions (Gemini LOW)"
metrics:
  duration: "~8 minutes"
  completed: "2026-05-06"
  tasks_completed: 2
  tasks_total: 2
  files_created: 4
  files_modified: 0
---

# Phase 84 Plan 01: CUDL Shelfmark Bridge Foundation + Leading-Zero Audit Summary

**One-liner:** Bridge module skeleton with cudl_normalize() ported verbatim from scan_cudl_orphans.py, plus delta-isolation audit confirming 0 leading-zero collision risks across 140K CUL rows.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Create shared/shelfmark_bridge.py skeleton | ec02cb4e | shared/shelfmark_bridge.py |
| 2 | Create audit script and run collision reports | 047d323a | scripts/audit_leading_zero_collisions.py, reports/leading_zero_collisions.csv, reports/cudl_full_normalization_collisions.csv |

## What Was Built

### shared/shelfmark_bridge.py

New module providing the CUDL normalization foundation for Phase 84. Key exports:

- **`cudl_normalize(s)`** — full NORM-03 normalization: lowercase, whitespace/hyphen/quote removal, slash/comma→dot, dot-adjacent-to-letter removal, leading-zero stripping. Ported verbatim from `scripts/scan_cudl_orphans.py:37-58`. Four example assertions all pass:
  - `T-S Ar. 48.211` → `tsar48.211`
  - `T-S F 8/002` → `tsf8.2`
  - `Add. 863, 2` → `add863.2`
  - `T-S NS 329/0014` → `tsns329.14`

- **`_normalize_without_zero_collapse(s)`** — audit-only sibling; identical to `cudl_normalize` but omits the two leading-zero `re.sub` lines. Used by the audit script to isolate the delta.

- **`_BUILTIN_COLLISION_KEYS`** — hardcoded empty set (Gemini LOW safety net). Runtime checks `_is_collision_key(key)` combine this with the dynamically-loaded `_COLLISION_KEYS`.

- **Stubs:** `lookup_cudl()`, `build_alias_index()`, `load_collision_keys()`, `shelfmark_to_cudl_label()` — all return None/pass for now. Plans 02/03/04 implement them.

- **`NUM_RE`** — exported for `scripts/scan_cudl_orphans.py` re-import (D-08 site 4, one source of truth).

### scripts/audit_leading_zero_collisions.py

One-shot audit walking 140,170 CUL rows in libraries.csv. Algorithm:
1. For every variant, compute both `full_key = cudl_normalize(v)` and `base_key = _normalize_without_zero_collapse(v)`.
2. A "delta collision" is a `full_key` that maps to 2+ distinct sys_ids WHERE the base-level buckets would NOT already cover that same set (i.e. zero-collapse introduces the merge).

**Audit results (2026-05-06):**
- CUL rows processed: 140,170
- Distinct full-normalized keys: 271,464
- Full-normalization collisions: 529 (transparency dump)
- **Leading-zero DELTA collisions: 0** (gate file is header-only)

### Report Files

- `reports/leading_zero_collisions.csv` — gate file. Header-only (0 rows). Plan 03 loads this via `load_collision_keys()` to enforce D-06; since it is empty, no keys are excluded from the alias index.
- `reports/cudl_full_normalization_collisions.csv` — transparency dump. 529 rows of full-normalization collisions (mostly due to slash/comma/dot rules, not zero-collapse). NOT a runtime gate.

## Deviations from Plan

### [Rule 3 - Blocking] Force-add report files ignored by .gitignore

- **Found during:** Task 2 commit
- **Issue:** `.gitignore` line 28 has `/Reports` which Windows resolves case-insensitively as `/reports`, blocking `git add reports/*.csv`.
- **Fix:** Used `git add -f reports/leading_zero_collisions.csv reports/cudl_full_normalization_collisions.csv` to force-add the plan-specified artifacts.
- **Files modified:** None (git config only)
- **Commit:** 047d323a

## Known Stubs

| Stub | File | Line | Reason |
|------|------|------|--------|
| `build_alias_index()` | shared/shelfmark_bridge.py | ~140 | Plan 02 implements Mosseri + CUL alias walk |
| `shelfmark_to_cudl_label()` | shared/shelfmark_bridge.py | ~157 | Plan 03 implements reverse-map (browse CUDL link) |
| `lookup_cudl()` (always returns None) | shared/shelfmark_bridge.py | ~121 | Works once build_alias_index() is implemented in Plan 02 |

These stubs are intentional per plan scope. No plan goal is blocked — this plan's goal is the foundation module + audit only.

## Threat Flags

None. This plan creates a pure-function normalization module and an offline audit script. No network endpoints, no auth paths, no schema changes, no Supabase writes.

## Self-Check: PASSED

- `shared/shelfmark_bridge.py` exists and is importable: FOUND
- `scripts/audit_leading_zero_collisions.py` exists: FOUND
- `reports/leading_zero_collisions.csv` exists with correct header: FOUND
- `reports/cudl_full_normalization_collisions.csv` exists with correct header: FOUND
- Commit `ec02cb4e` exists: FOUND
- Commit `047d323a` exists: FOUND
- No modifications to `genizah_core.py`, `web/pages/browse.py`, `shared/nli_crossref_service.py`, `scripts/scan_cudl_orphans.py`, `libraries.csv`: CONFIRMED
