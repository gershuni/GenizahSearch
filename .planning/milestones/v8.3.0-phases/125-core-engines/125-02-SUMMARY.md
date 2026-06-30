---
phase: 125-core-engines
plan: "02"
subsystem: shared/engines
tags: [extraction, lab-settings, god-file-decomposition, facade-shim, guard-01]
dependency_graph:
  requires: ["125-01"]
  provides: ["shared/lab_settings.py", "genizah_core.LabSettings facade shim"]
  affects: ["125-03 (LabEngine extraction — now unblocked)", "125-04 (SearchEngine extraction)"]
tech_stack:
  added: []
  patterns: ["same-object re-export facade shim", "module-level LOGGER", "skip-until-exists guard becomes live"]
key_files:
  created:
    - shared/lab_settings.py
  modified:
    - genizah_core.py
    - tests/test_no_back_edges_core.py
decisions:
  - "Derived LOGGER routing update (bare logging.getLogger → module LOGGER) as per PATTERNS.md — behavior identical, logger family genizah.shared.lab_settings"
  - "No tantivy guard needed — LabSettings is stdlib-only (json/logging/os/shared.config)"
  - "No _tr() needed — LabSettings has no tr() calls"
metrics:
  duration: "33m"
  completed_date: "2026-06-26"
  tasks_completed: 1
  files_changed: 3
---

# Phase 125 Plan 02: LabSettings Extraction Summary

**One-liner:** stdlib-only `LabSettings` (Lab Mode scoring weights, JSON persistence) extracted from genizah_core.py into shared/lab_settings.py behind a permanent same-object re-export shim.

## What Was Done

Extracted the `LabSettings` class (~139 lines) from genizah_core.py into `shared/lab_settings.py`. The class is stdlib-only (json, logging, os) and depends only on `shared.config.Config`. This is the first engine-cluster move in Phase 125, proving the extraction recipe on a low-risk target and unblocking LabEngine (125-03).

### Task 1: Create shared/lab_settings.py + genizah_core facade shim + identity tests

**Step A — shared/lab_settings.py:**
- Created with `# -*- coding: utf-8 -*-` header (no BOM — verified `b'# -'` first bytes)
- Module docstring in shared/codicological.py style (Phase 125 provenance)
- Imports: `json`, `logging`, `os`, `from shared.config import Config` (derived from body — exact set)
- `LOGGER = logging.getLogger("genizah." + __name__)` at module level
- LabSettings class body copied verbatim; two bare `logging.getLogger(__name__).warning(...)` calls in `load()` and `save()` updated to use module `LOGGER` (behavior identical — same logger family)

**Step B — genizah_core.py facade shim:**
- Removed the inline `class LabSettings` definition (lines 492-630)
- Added to the Phase-125 facade block:
  ```python
  # Phase 125: lab_settings extracted — permanent compat facade (v8.3.0)
  from shared.lab_settings import LabSettings  # noqa: F401
  ```

**Step C — Identity and standalone tests:**
- Added `test_lab_settings_identity` and `test_lab_settings_standalone_import` to `tests/test_no_back_edges_core.py` after the Phase 124 indexer tests
- GUARD-01 registry entry for `shared/lab_settings.py` (pre-grown in 125-01 with skip-until-exists guard) is now live — file exists, back-edge scan is enforcing

## Verification

```
# Back-edge + identity tests
GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/test_no_back_edges_core.py -k "lab_settings or back_edge" -q
→ 35 passed, 2 skipped

# LabSettings consumer tests
python -m pytest tests/test_comp_corpus_scope.py tests/test_lab_composition_chunk_hits.py -q
→ 32 passed

# Full bulk suite (non-GUI, excluding test_search_api_v2 pre-existing env failures)
GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -m "not gui and not render_smoke" --ignore=tests/test_search_api_v2.py -q
→ 4752 passed, 28 skipped, 61 deselected, 3 xfailed, 26 xpassed — ZERO new failures

# Pre-existing env failures (unchanged, test_search_api_v2 real-index)
→ 6 failed (state.searcher None in test env) — same as base, not caused by this plan

# Ruff
python -m ruff check shared/lab_settings.py genizah_core.py tests/test_no_back_edges_core.py
→ All checks passed!
```

## Runtime Verification

```python
import shared.lab_settings, genizah_core
assert shared.lab_settings.LabSettings is genizah_core.LabSettings  # PASS
s = shared.lab_settings.LabSettings()
assert hasattr(s, 'candidate_limit')  # PASS — candidate_limit: 5000
```

## Acceptance Criteria

- [x] shared/lab_settings.py exists, no BOM (`b'# -'` first bytes), `# -*- coding: utf-8 -*-` header, `LOGGER = logging.getLogger("genizah." + __name__)`, full LabSettings class
- [x] genizah_core.py has no `class LabSettings` definition; contains `from shared.lab_settings import LabSettings  # noqa: F401`
- [x] test_lab_settings_identity passes: `genizah_core.LabSettings is shared.lab_settings.LabSettings`
- [x] test_lab_settings_standalone_import passes (LabSettings() instantiates with candidate_limit)
- [x] GUARD-01 parametrized back-edge scan green with shared/lab_settings.py now present
- [x] ruff clean on all 3 touched files (noqa shim intact)
- [x] test_comp_corpus_scope.py + test_lab_composition_chunk_hits.py GREEN

## Deviations from Plan

None — plan executed exactly as written. The two `logging.getLogger(__name__).warning(...)` calls were updated to `LOGGER.warning(...)` as directed in the plan (Step A, PATTERNS.md note).

## Threat Flags

None. Pure mechanical class move (stdlib-only config class). No new network endpoints, auth paths, schema changes, or trust boundaries.

## Known Stubs

None.

## Commits

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | LabSettings extraction + facade shim + identity tests | 478959ec | shared/lab_settings.py (new), genizah_core.py, tests/test_no_back_edges_core.py |

## Self-Check: PASSED

- `shared/lab_settings.py` exists and contains `class LabSettings`
- `478959ec` commit exists in git log
- No unexpected file deletions in commit
- genizah_core.LabSettings is shared.lab_settings.LabSettings (verified at runtime)
- GUARD-01 live for shared/lab_settings.py — no module-level genizah_core imports found
