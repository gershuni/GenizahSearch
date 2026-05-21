---
phase: 95
plan: "02"
subsystem: shared-helpers
tags: [local-sys-id, parser-generalization, library-codes, phase-95, wave-1]
dependency_graph:
  requires: [95-01]
  provides: [shared/local_sys_id.py, genizah_core.py:parse_header_smart+parse_full_id_components, LIBRARY_CODES.LOCAL, Config.LOCAL_INDEX_DIR]
  affects: [genizah_core.py, genizah_translations.py, shared/local_sys_id.py]
tech_stack:
  added: [shared/local_sys_id.py]
  patterns: [hashlib.sha256 % 10**8 for 8-digit IDs, os.path.normcase+Path.resolve for canonical paths, regex alternation (?:99|97) for prefix broadening]
key_files:
  created:
    - shared/local_sys_id.py
    - tests/test_local_sys_id_namespace.py (stub -> green)
    - tests/test_canonical_filepath.py (stub -> green)
    - tests/test_local_sys_id_parser_compat.py (stub -> green)
  modified:
    - genizah_core.py (parse_header_smart, parse_full_id_components, LIBRARY_CODES, Config)
    - genizah_translations.py (LIBRARY_CODES_HE)
decisions:
  - "Used regex alternation (?:99|97) instead of centralized extract_sys_id helper — smaller blast radius, per CONTEXT D-13 planner note"
  - "Added test_synthetic_sys_id.py to LOCAL TestNoIntCoercion ALLOWLIST — it contains pattern references in docstring comments, not executable coercion"
  - "MetadataManager.__new__ bypass works for parser tests — parse_header_smart and parse_full_id_components are pure regex methods needing no __init__ state"
metrics:
  duration: "842 seconds (~14 min)"
  completed: "2026-05-21"
  tasks_completed: 4
  tasks_total: 4
  files_created: 4
  files_modified: 2
---

# Phase 95 Plan 02: LOCAL sys_id Helper + Parser Generalization Summary

**One-liner:** 97-prefix LOCAL sys_id pure-function helper module + `parse_header_smart`/`parse_full_id_components` regex broadening for D-13 Codex P0 + `LIBRARY_CODES['LOCAL']` EN+HE + `Config.LOCAL_INDEX_DIR` paths.

## Tasks Completed

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Create shared/local_sys_id.py helper module | 3774b707 | shared/local_sys_id.py |
| 2 | Implement green tests for namespace + canonical filepath | 571e4a02 | tests/test_local_sys_id_namespace.py, tests/test_canonical_filepath.py |
| 3 | Generalize parse_header_smart + parse_full_id_components for 97-prefix | c6340de6 | genizah_core.py, tests/test_local_sys_id_parser_compat.py |
| 4 | Extend LIBRARY_CODES with LOCAL entry + Config paths | 28a608ef | genizah_core.py, genizah_translations.py |

## Artifacts Delivered

### shared/local_sys_id.py

Five public/private helpers (123 lines, pure-function, no Qt/Tantivy):

- `is_local_sys_id(s)` — 97-prefix + 18-digit length discriminator; no suffix (unlike synthetic-99's `000000` suffix). Mirrors `shared/synthetic_sys_id.py:is_synthetic_sys_id` control flow exactly.
- `_canonical_filepath(p)` — `Path.resolve(strict=False)` + `os.path.normcase()` for Windows case/separator normalization (D-42).
- `_machine_id()` — `sha256(hostname) % 10**8` = exactly 8 decimal digits (D-19 Codex fix).
- `_content_hash(canonical, slot=0)` — `sha256(path) % 10**8` per slot, slot arg enables collision retry up to slot=7 (D-19).
- `generate_local_sys_id(filepath, slot=0)` — `"97" + machine_id(8) + content_hash(8)` = 18-digit string.

### genizah_core.py parser changes (D-13 Codex P0)

Two regex broadenings applied:

- `parse_header_smart` line 3642: `r'(99\d{8,})'` → `r'((?:99|97)\d{8,})'`
- `parse_full_id_components` line 3662: same broadening
- `parse_full_id_components`: D-34 `_F(\d{3,5})` fallback added after `IE\d+` match — sets `ie_id = f"F{file_id}"` for LOCAL full_headers that have no IE component.

### LIBRARY_CODES extension (D-13)

- `genizah_core.py:LIBRARY_CODES['LOCAL'] = 'My Library'`
- `genizah_translations.py:LIBRARY_CODES_HE['LOCAL'] = 'הספרייה שלי'`
- `genizah_core.py:Config.LOCAL_INDEX_DIR = os.path.join(INDEX_DIR, "LocalIndex")` (D-14)
- `genizah_core.py:Config.LOCAL_LAB_INDEX_DIR = os.path.join(INDEX_DIR, "LocalLabIndex")` (D-14)

### Tests turned GREEN

- `tests/test_local_sys_id_namespace.py`: 31 tests — golden cases, real Alma negatives, synthetic-99 negatives, general negatives, namespace disjoint, full libraries.csv ~255K-row scan (MEDIUM-3 fix: removed 1000-row cap), machine_id/content_hash digit guarantees, generate format, TestNoIntCoercion AST lint.
- `tests/test_canonical_filepath.py`: 6 tests — drive-letter casing, separator normalization, relative→absolute, missing-file no-raise, UNC path (Windows-only), junction idempotent (Windows-only admin-gated skip).
- `tests/test_local_sys_id_parser_compat.py`: 7 tests — parse_header_smart LOCAL + synthetic regression, parse_full_id_components LOCAL (D-34 ie_id=F0042) + synthetic regression + edge cases.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 2 - Missing critical] TestNoIntCoercion ALLOWLIST needed test_synthetic_sys_id.py**
- **Found during:** Task 2 test run
- **Issue:** `tests/test_synthetic_sys_id.py` contains `int(sys_id)` text in docstring/comment lines (not executable code). The LOCAL TestNoIntCoercion AST scan flagged these comment lines as violations.
- **Fix:** Added `tests/test_synthetic_sys_id.py` to the LOCAL ALLOWLIST — same rationale as that file's own allowlist including itself.
- **Files modified:** `tests/test_local_sys_id_namespace.py`
- **Commit:** 571e4a02

**2. [Rule 1 - Bug] Unused _canonical_filepath import in test_local_sys_id_namespace.py**
- **Found during:** Task 2 ruff check
- **Issue:** `_canonical_filepath` was imported in `test_local_sys_id_namespace.py` but only used in `test_canonical_filepath.py`.
- **Fix:** Removed unused import; ruff clean.
- **Files modified:** `tests/test_local_sys_id_namespace.py`
- **Commit:** 571e4a02

## Test Results

```
tests/test_local_sys_id_namespace.py   31 passed
tests/test_canonical_filepath.py        6 passed (2 Windows-only skippable)
tests/test_local_sys_id_parser_compat.py  7 passed
Full suite: 2371 passed, 28 skipped, 2 xfailed, 27 Wave-0 stubs (other plans)
```

The 27 failures are all `NotImplementedError` Wave-0 stubs from plans 95-03 through 95-09 — they existed before this plan and are unchanged.

## Known Stubs

None in files created/modified by this plan. All five helpers in `shared/local_sys_id.py` are fully implemented. All three test files are fully implemented (zero `raise NotImplementedError` remaining).

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes at trust boundaries introduced by this plan. `shared/local_sys_id.py` is pure-function (stdlib only: hashlib, os, socket, pathlib). The `LIBRARY_CODES['LOCAL']` addition is a dict entry — web-consumer guard (D-46) is handled in Plan 95-09.

## Self-Check: PASSED

| Item | Status |
|------|--------|
| shared/local_sys_id.py | FOUND |
| tests/test_local_sys_id_namespace.py | FOUND |
| tests/test_canonical_filepath.py | FOUND |
| tests/test_local_sys_id_parser_compat.py | FOUND |
| commit 3774b707 (Task 1) | FOUND |
| commit 571e4a02 (Task 2) | FOUND |
| commit c6340de6 (Task 3) | FOUND |
| commit 28a608ef (Task 4) | FOUND |
