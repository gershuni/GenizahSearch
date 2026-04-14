---
phase: 63-ci-dependency-pinning
plan: 02
subsystem: dependency-pinning
tags: [dependencies, pip, lockfile, ci, documentation]
dependency_graph:
  requires: [ci-workflow, ruff-config, lint-baseline]
  provides: [pinned-deps, lockfile, dep-docs]
  affects: [requirements.txt, requirements-lock.txt, .github/workflows/ci.yml, docs/guides/DEVELOPER_GUIDE.md]
tech_stack:
  added: []
  patterns: [two-file-pinning]
key_files:
  created:
    - requirements-lock.txt
  modified:
    - requirements.txt
    - .github/workflows/ci.yml
    - docs/guides/DEVELOPER_GUIDE.md
  deleted: []
decisions:
  - "Used pip freeze from validated dev environment (1066 tests passing) as lock file source"
  - "Documented cross-platform lock file limitation in DEVELOPER_GUIDE.md"
  - "Replaced flake8 reference with ruff in Useful Commands section"
metrics:
  duration: 147s
  completed: 2026-04-14T16:43:35Z
  tasks_completed: 2
  tasks_total: 2
  files_changed: 4
---

# Phase 63 Plan 02: Dependency Pinning & Lock File Summary

All 14 direct dependencies pinned to exact versions, full transitive lock file (115 packages) generated, CI updated to install from lock file, and DEVELOPER_GUIDE.md documented with dependency upgrade workflow and linting instructions.

## Tasks Completed

| # | Task | Commit | Key Files |
|---|------|--------|-----------|
| 1 | Pin dependencies and update CI to use lock file | 74160829 | requirements.txt, requirements-lock.txt, .github/workflows/ci.yml |
| 2 | Document dependency upgrade workflow in DEVELOPER_GUIDE.md | 37cda5aa | docs/guides/DEVELOPER_GUIDE.md |

## What Was Built

### Pinned Dependencies (requirements.txt)
All 14 direct dependencies pinned with exact `==` versions:
- PyQt6==6.10.2, tantivy==0.25.1, requests==2.32.5, tqdm==4.67.3, colorama==0.4.6, openpyxl==3.1.5, python-docx==1.2.0, nicegui==3.8.0, supabase==2.28.0, gotrue==2.12.4, python-dotenv==1.2.2, keyring==25.7.0, Pillow==12.1.1, numpy==2.4.3

### Lock File (requirements-lock.txt)
Full `pip freeze` output with 115 packages (14 direct + 101 transitive), all with exact `==` pins. Generated from the validated development environment where 1066 tests pass.

### CI Update (.github/workflows/ci.yml)
Changed tests job from `pip install -r requirements.txt` to `pip install -r requirements-lock.txt` for fully reproducible CI builds.

### Documentation (docs/guides/DEVELOPER_GUIDE.md)
- **Dependency Management** section: adding/upgrading deps, two-file strategy table
- **Linting** section: ruff check commands, current ruleset, CI enforcement
- **Dev Tools** note: pytest and ruff documented as CI-only dev tools
- **Known Limitations** note: cross-platform lock file generation caveat
- Replaced `flake8` reference with `ruff check .` in Useful Commands
- Updated last-updated date to 2026-04-14

## Deviations from Plan

None - plan executed exactly as written.

## Verification Results

- `grep -c "==" requirements.txt` returns 14
- `grep -c "==" requirements-lock.txt` returns 115
- `.github/workflows/ci.yml` contains `pip install -r requirements-lock.txt`
- `pytest tests/` -- 1066 passed, 9 skipped, 1 warning (gotrue deprecation)
- DEVELOPER_GUIDE.md contains all required sections (Dependency Management, Linting, Known Limitations, CI-only dev tools)
- No flake8 references remain in DEVELOPER_GUIDE.md

## Known Stubs

None.

## Self-Check: PASSED
