---
phase: 106-joins-lab-shared-core-pure-logic-service-adapters-unit-tests
plan: "01"
subsystem: shared-core
tags: [joins-lab, domain-model, protocol, compose, tdd, pure-logic]
dependency_graph:
  requires: []
  provides:
    - shared/joins_lab.py BuilderRow/SideQuery/Candidate/MergeResult frozen dataclasses
    - shared/joins_lab.py SearchExecutor Protocol
    - shared/joins_lab.py normalize_candidate() single dict->Candidate source of truth
    - shared/joins_lab.py page_of() single page extractor
    - shared/joins_lab.py compose() line-break query composition with page-anchor 3-tuple
    - tests/test_joins_lab.py TestCompose/TestPageOf/TestNormalize/TestStaticImport
  affects:
    - Plans 02 and 03 (append functions to shared/joins_lab.py)
    - Phase 107 desktop JWB (injects SearchExecutor backed by SearchEngine + MetadataManager)
tech_stack:
  added: []
  patterns:
    - frozen dataclasses (first frozen=True usage in shared/ outside fist_cudl_bridge.py)
    - typing.Protocol with @runtime_checkable (first Protocol usage in codebase)
    - TDD RED/GREEN with class-based pytest
key_files:
  created:
    - shared/joins_lab.py
    - tests/test_joins_lab.py
  modified: []
decisions:
  - "F401-strict import block: only `from dataclasses import dataclass`, `import re`, `from typing import Optional, Protocol, runtime_checkable` — no `field`, no `from genizah_core import`, no `import dataclasses` (Plan 02 adds the last when it needs dataclasses.replace())"
  - "Docstring wording: 'No Qt bindings. No direct database connections.' avoids triggering the TestStaticImport substring guard while preserving the architectural constraint declaration"
  - "FakeSearchExecutor added at module level in test file (plain class, not MagicMock) per house pattern"
  - "TestDataclasses added to exercise Candidate and MergeResult constructors directly, satisfying ruff F401 on those imports"
metrics:
  duration: "~25 minutes"
  completed: "2026-06-03"
  tasks_completed: 3
  tasks_total: 3
  files_created: 2
  files_modified: 0
  tests_added: 28
  lines_added: 357  # shared/joins_lab.py
---

# Phase 106 Plan 01: Joins Lab Foundation — Domain Model + compose() Summary

Frozen dataclasses (BuilderRow/SideQuery/Candidate/MergeResult), SearchExecutor Protocol, normalize_candidate() dict→Candidate normalizer, page_of() extractor, and compose() RTL line-break query composer with page-anchor 3-tuple — all F401-clean and TDD-tested.

## What Was Built

`shared/joins_lab.py` (357 lines) — the foundational module for the Joins Lab shared core:

- **4 frozen dataclasses** following the `shared/fist_cudl_bridge.py` pattern: `BuilderRow` (term + line_start/end + gap_to_next), `SideQuery` (rows tuple + variants + page_position), `Candidate` (flat provenance fields + `key` property), `MergeResult` (candidates tuple + note).
- **SearchExecutor Protocol** (`@runtime_checkable`) — first `typing.Protocol` usage in the codebase. Exactly 4 methods: `execute_search`, `get_browse_page`, `get_meta_for_id`, `get_library_for_id`. The `corpus_scope` default is `"all"` matching the live engine; callers pass `"genizah"` explicitly.
- **`normalize_candidate()`** — single `dict→Candidate` source of truth. Reads `score` via `.get("score")` defaulting to `None` (line-break results carry no score — Codex VERIFIED). Page extraction delegated to `page_of()`.
- **`page_of()`** — `display.img` via `_to_int` then `_P0*(\d+)` uid regex fallback then `None`. VS-sourced `uid="{sid}|vs"` returns `None` (correct dedup key).
- **`compose()`** (SC#1) — RTL line-break query composition. Leading `|` = `line_start` (right edge in Hebrew). Trailing `|` = `line_end`. `[|N]` between rows for gap. Returns `(query_str, responsa_options, page_position)` 3-tuple. `page_position` passes through to `execute_search(text_position=...)` unchanged (R-01: ONE call, post-filter in `_execute_line_break_search`). Raises `ValueError` when `page_position='start'` and first row is empty, or `page_position='end'` and last row is empty. Returns `(None, None, None)` for benign all-empty input (no page anchor).

`tests/test_joins_lab.py` (28 tests across 7 classes):
- `TestPageOf` (4 tests), `TestNormalize` (4 tests), `TestDataclasses` (2 tests), `TestProtocol` (1 test)
- `TestCompose` (12 tests including round-trip via `_parse_line_break_query`, RTL pipe, all ValueError cases)
- `TestStaticImport` (5 tests: AST walk + substring guards + importability)

## Commits

| Hash | Type | Description |
|------|------|-------------|
| `20cb4367` | test | RED: failing tests for domain model + compose() + static guards |
| `865ab318` | feat | GREEN: shared/joins_lab.py implementation (domain model + compose()) |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Module docstring triggered TestStaticImport substring guards**

- **Found during:** Task 3 (GREEN, first test run of TestStaticImport)
- **Issue:** The module docstring contained the literal string "No PyQt." and "No direct sqlite3.connect." — the `test_no_pyside_or_qt_substring` test asserts `"PyQt" not in src` and `test_no_sqlite3_connect` asserts `"sqlite3.connect" not in src` (these catch dynamic import-string usage). The docstring was not a dynamic import but still triggered the guards.
- **Fix:** Changed docstring to "No Qt bindings. No direct database connections." — preserves the architectural constraint declaration without triggering the static guards.
- **Files modified:** `shared/joins_lab.py` (docstring only)
- **Commit:** `865ab318` (same feat commit — caught before first commit of the implementation)

**2. [Rule 2 - Missing functionality] `Candidate` and `MergeResult` unused in test file**

- **Found during:** Task 3 ruff check
- **Issue:** The plan's action specified importing `Candidate` and `MergeResult` by name in the test file, but neither was directly used in the test body — both are used indirectly via `normalize_candidate()` which returns `Candidate`. Ruff F401 flagged both as unused imports.
- **Fix:** Added `TestDataclasses` class with 2 tests that directly construct `Candidate` and `MergeResult` instances, satisfying F401 while adding useful coverage.
- **Files modified:** `tests/test_joins_lab.py`
- **Commit:** `865ab318` (same commit)

## Known Stubs

None — all data flows are implemented. `normalize_candidate()` returns real field values from the engine result dict, not placeholders. `compose()` generates real query syntax.

## Threat Flags

No new network endpoints, auth paths, file access patterns, or schema changes introduced. The only new surface is `compose()` which emits pure string output from trusted in-process input — covered by T-106-01 (accept) in the plan's threat model.

## Self-Check: PASSED

- `shared/joins_lab.py` exists: FOUND
- `tests/test_joins_lab.py` exists: FOUND
- Commit `20cb4367` exists: FOUND (RED phase)
- Commit `865ab318` exists: FOUND (GREEN phase)
- `pytest tests/test_joins_lab.py -x -q` → 28 passed
- `python -m ruff check shared/joins_lab.py tests/test_joins_lab.py` → All checks passed
- `grep -c '@dataclass(frozen=True)' shared/joins_lab.py` → 4
- `grep -q 'from dataclasses import dataclass$' shared/joins_lab.py` → OK (no `field`)
- `grep -q 'from genizah_core import' shared/joins_lab.py` → non-zero (no genizah_core import)
- `grep -q '^import dataclasses$' shared/joins_lab.py` → non-zero (not present — Plan 02 adds it)
- `grep -q 'corpus_scope: str = "all"' shared/joins_lab.py` → OK
- Line count: 357 (> min_lines: 150)
