---
phase: 65-repo-hygiene
plan: 01
subsystem: web
tags: [refactoring, monkey-patch, version-guard, dependency-pinning]
dependency_graph:
  requires: []
  provides: [framework-patches-module, packaging-dependency]
  affects: [web-startup]
tech_stack:
  added: [packaging]
  patterns: [per-patch-version-guard, extract-to-module]
key_files:
  created:
    - web/framework_patches.py
  modified:
    - web/main.py
    - requirements.txt
decisions:
  - Independent version guards per patch using packaging.version.Version
  - WARNING-level logging on unexpected failures for supported NiceGUI versions
  - Font-display middleware stays in main.py (Starlette middleware, not a monkey-patch)
metrics:
  duration: 111s
  completed: "2026-04-15T00:59:09Z"
  tasks: 2
  files: 3
---

# Phase 65 Plan 01: Extract Framework Patches Summary

Isolated 2 NiceGUI monkey-patches (ESM is_file guard, HTML lang attribute) into web/framework_patches.py with independent per-patch version guards using packaging.version.Version, WARNING-level failure logging, and pinned packaging==26.0 dependency.

## Task Results

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Add packaging to requirements.txt | e1bec365 | requirements.txt |
| 2 | Create web/framework_patches.py with version guards | 2886fe80 | web/framework_patches.py, web/main.py |

## Verification Results

1. `python -c "from web.framework_patches import apply_all_patches"` -- OK
2. `grep -c "_NV > _V" web/framework_patches.py` -- 2 (one per patch)
3. `grep "def _patch_" web/main.py` -- 0 matches (patches removed)
4. `grep "packaging==" requirements.txt` -- packaging==26.0
5. `pytest tests/ -x -q` -- 1067 passed, 8 skipped (baseline maintained)

## Decisions Made

1. **Independent version guards**: Each patch has its own `_NV > _V('3.8.0')` check. Both share the same threshold today but can diverge independently when one gets fixed upstream before the other.
2. **Font-display middleware stays in main.py**: It is a Starlette HTTP middleware, not a NiceGUI monkey-patch. Moving it would conflate concerns.
3. **WARNING not ERROR on failure**: Supported-version failures log at WARNING so they surface in production logs without triggering error alerting.

## Deviations from Plan

None -- plan executed exactly as written.

## Known Stubs

None.
