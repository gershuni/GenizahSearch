---
phase: 63-ci-dependency-pinning
plan: 01
subsystem: ci-infrastructure
tags: [ci, ruff, linting, github-actions]
dependency_graph:
  requires: []
  provides: [ci-workflow, ruff-config, lint-baseline]
  affects: [.github/workflows/ci.yml, ruff.toml]
tech_stack:
  added: [ruff@0.15.10]
  patterns: [explicit-re-export]
key_files:
  created:
    - .github/workflows/ci.yml
    - ruff.toml
  modified:
    - genizah_app.py
    - web/pages/parallels.py
    - web/supabase_client.py
    - web/fjms_service.py
    - web/document_service.py
    - web/corrections_service.py
    - web/nli_crossref_service.py
    - web/export_service.py
    - shared/__init__.py
    - scripts/analyze_char_merges.py
    - tests/e2e/conftest.py
  deleted:
    - .github/workflows/docs-check.yml
decisions:
  - "Used explicit re-export syntax (X as X) instead of noqa for shim modules"
  - "Fixed F821 _par_show_trans by checking p_state.translation_data directly"
  - "Removed duplicate reset_client (F811) keeping first definition"
metrics:
  duration: 329s
  completed: 2026-04-14T16:36:34Z
  tasks_completed: 2
  tasks_total: 2
  files_changed: 108
---

# Phase 63 Plan 01: CI Workflow & Ruff Baseline Summary

GitHub Actions CI workflow with ruff linting (E9/F401/F811/F821 scoped ruleset) and 267 violations fixed to establish zero-violation baseline across 105 source files.

## Tasks Completed

| # | Task | Commit | Key Files |
|---|------|--------|-----------|
| 1 | Create ruff.toml and CI workflow | 506ec1e7 | ruff.toml, .github/workflows/ci.yml |
| 2 | Fix all ruff violations | 552bef52 | 105 files (252 auto-fixed + 15 manual fixes) |

## What Was Built

### CI Workflow (.github/workflows/ci.yml)
- Two jobs: `lint-and-docs` (Ubuntu, ruff check + check_docs.py) and `tests` (matrix: Ubuntu 3.10 + Windows 3.11)
- Tests job depends on lint-and-docs (fast-fail)
- Triggers on push to master-main and all pull_requests
- ruff pinned to 0.15.10, pytest installed separately as CI-only dev tool
- Installs from requirements.txt (upgraded to requirements-lock.txt in Plan 02)

### Ruff Configuration (ruff.toml)
- Scoped ruleset: E9 (syntax errors), F401 (unused imports), F811 (redefined unused), F821 (undefined name)
- line-length = 120 (future-friendly default, not enforced)
- Excludes: .claude, .git, venv, __pycache__, extension, dist, build

### Violation Fixes (267 total)
- 252 F401 auto-fixed by `ruff check --fix`
- 5 shim re-exports restored with explicit `X as X` syntax (ruff gutted backward-compat shims)
- 1 F821 real bug fixed: `_par_show_trans` undefined in parallels.py render scope
- 1 F811 duplicate `reset_client` removed from supabase_client.py
- 3 genuinely unused imports manually removed (Indexer, Border/Side/get_column_letter)
- 3 noqa comments for availability-check imports in try/except blocks
- 2 explicit re-exports in shared/__init__.py

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Restored 5 shim re-exports gutted by ruff auto-fix**
- **Found during:** Task 2 (test verification)
- **Issue:** `ruff check --fix` removed imports from backward-compatibility shim modules (web/fjms_service.py, web/document_service.py, web/corrections_service.py, web/nli_crossref_service.py, web/export_service.py) because they appeared "unused" -- but they were re-exports consumed by other modules
- **Fix:** Restored all re-exports using explicit `X as X` syntax which ruff recognizes as intentional re-exports
- **Files modified:** web/fjms_service.py, web/document_service.py, web/corrections_service.py, web/nli_crossref_service.py, web/export_service.py
- **Commit:** 552bef52

**2. [Rule 1 - Bug] Fixed F821 undefined name `_par_show_trans` in parallels.py**
- **Found during:** Task 2
- **Issue:** Variable `_par_show_trans` was defined in an async function scope (line 2246) but referenced in a different render function scope (line 3142) -- a real NameError bug that would crash at runtime
- **Fix:** Replaced `_par_show_trans and get_language() == 'he' and sys_id and p_state.translation_data` with `p_state.translation_data and get_language() == 'he' and sys_id` -- translation_data is only populated when show_translations was True, so the check is equivalent
- **Files modified:** web/pages/parallels.py
- **Commit:** 552bef52

## Verification Results

- `ruff check .` exits 0 (zero violations)
- `pytest tests/` -- 1066 passed, 9 skipped, 1 warning (gotrue deprecation, Phase 64 scope)
- `python scripts/check_docs.py` -- pre-existing cp1255 encoding error on Windows (emoji in output), will work on Ubuntu CI runner

## Known Stubs

None.

## Self-Check: PASSED
