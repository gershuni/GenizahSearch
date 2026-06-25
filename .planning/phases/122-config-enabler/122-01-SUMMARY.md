---
phase: 122-config-enabler
plan: "01"
subsystem: shared-config
tags:
  - refactor
  - extraction
  - import-cycle
  - guard
  - stdlib-only
dependency_graph:
  requires: []
  provides:
    - shared/config.py (Config leaf module, stdlib-only)
    - tests/test_no_back_edges_core.py (GUARD-01 + CONFIG-01 permanent guard)
  affects:
    - genizah_core.py (permanent re-export facade)
    - shared/session_persistence.py (retargeted import, D-02)
tech_stack:
  added:
    - shared/config.py (new stdlib-only leaf module)
    - tests/test_no_back_edges_core.py (new AST guard test)
  patterns:
    - "# noqa: F401 re-export shim (GUARD-04, permanent facade)"
    - "scope-aware AST traversal for import-time compound statements"
    - "parametrized registry (EXTRACTED_MODULES) growing each phase"
key_files:
  created:
    - shared/config.py
    - tests/test_no_back_edges_core.py
  modified:
    - genizah_core.py
    - shared/session_persistence.py
decisions:
  - "D-02: retarget session_persistence.py:32 from genizah_core to shared.config"
  - "D-03: full class move (not constants-only)"
  - "D-04: identity re-export (shared.config.Config is genizah_core.Config)"
  - "D-05: load-time makedirs side effect preserved verbatim"
  - "D-06: per-file ruff review only, never repo-wide --fix"
  - "Codex BLOCKER #1: BASE_DIR uses dirname(dirname(abspath(__file__))) in shared/config.py"
  - "Auto-fix (Rule 1): removed now-unused import sys from genizah_core.py after Config extraction"
  - "Scope-aware AST traversal (not flat iter_child_nodes): descends If/Try/With/For/While/Match/ClassDef but stops at FunctionDef/AsyncFunctionDef"
metrics:
  duration: "~20 minutes"
  completed: "2026-06-25"
  tasks_completed: 2
  tasks_total: 2
  files_created: 2
  files_modified: 2
---

# Phase 122 Plan 01: Config Enabler — Extract Config + GUARD-01 Summary

**One-liner:** Config class extracted from genizah_core.py to stdlib-only shared/config.py with permanent re-export facade (noqa: F401 shim) and scope-aware GUARD-01 AST back-edge guard.

## What Was Built

### Task 1: Move Config to shared/config.py + facade shim + session_persistence retarget

Created `shared/config.py` as a new stdlib-only leaf module containing the full `Config` class extracted verbatim from `genizah_core.py:2295-2426`, with one intentional change per Codex BLOCKER #1: the non-frozen `else:` branch's `BASE_DIR` computation was changed from `os.path.dirname(os.path.abspath(__file__))` to `os.path.dirname(os.path.dirname(os.path.abspath(__file__)))` to climb `shared/` up to the repo root, preserving `FILE_V8`, `LIBRARIES_CSV`, and other path-derived attributes.

Deleted the original `class Config:` block from `genizah_core.py` and replaced the section header with a tombstone comment. Added the permanent re-export shim `from shared.config import Config  # noqa: F401` in the `from shared.*` imports block.

Retargeted `shared/session_persistence.py:32` from `from genizah_core import Config` to `from shared.config import Config` (D-02).

### Task 2: GUARD-01 back-edge guard + CONFIG-01 identity test

Created `tests/test_no_back_edges_core.py` with:
- `EXTRACTED_MODULES = ["shared/config.py"]` registry (grows each phase of v8.3.0)
- `_has_module_level_genizah_core_import()`: scope-aware traversal descending import-time compound statements (If/Try/With/For/While/Match/ClassDef + handlers) but NOT FunctionDef/AsyncFunctionDef bodies; never uses bare `ast.walk()`
- `test_no_module_level_genizah_core_import[shared/config.py]`: GUARD-01 parametrized
- `test_config_identity()`: CONFIG-01 — asserts `shared.config.Config is genizah_core.Config`
- `test_config_paths_resolve_to_repo_root()`: Codex BLOCKER #1 — asserts BASE_DIR/FILE_V8/LIBRARIES_CSV resolve to repo root
- `test_guard_catches_top_level_guarded_import()`: scope-aware guard unit test (Codex HIGH #2)
- `test_guard_ignores_lazy_function_body_import()`: lazy-import exclusion unit test (Codex HIGH #2)

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Removed now-unused `import sys` from genizah_core.py**
- **Found during:** Task 1, per-file ruff check
- **Issue:** After extracting the Config class (which used `sys.frozen`, `sys.executable`, `sys._MEIPASS`), `import sys` at line 7 in `genizah_core.py` became unused (F401). The only remaining reference to `sys` in the file was a local variable `sys = re.search(...)` in a parse function — not the module.
- **Fix:** Removed `import sys` from `genizah_core.py` imports block.
- **Files modified:** `genizah_core.py`
- **Commit:** 31979cac (included in Task 1 atomic commit)

## Gate Results

### Quick Smoke Gate
Command: `PYTHONUTF8=1 pytest tests/test_no_back_edges_core.py tests/test_history_no_result_snapshots.py tests/test_local_filter_persistence.py -x -q`
Result: **13 passed in 0.39s** — GREEN

### Full Phase Gate
Command: `PYTHONUTF8=1 pytest tests/ -m "not gui and not render_smoke" -q --timeout=120`
Result: **SEGFAULT at ~51% progress (exit code 139)**

The segfault is a **pre-existing known issue** documented in project memory: "pytest tests/ aborts on a PyQt6 headless segfault; CI uses marker-based `gui-tests` split (NOT `-n auto`, OOMs on Tantivy); add new dialog tests to `_GUI_TEST_FILES` in conftest." The crash occurs after ~2550 tests due to Qt process-global state accumulation on headless Windows runners. This is NOT related to this phase's changes (which are pure stdlib-only Python import refactoring with no Qt code).

**Partial verification performed:**
- First ~50% of full suite (all tests up to `test_local_optout_persistence.py`): all PASSED before segfault
- Targeted post-crash tests (188 tests across Config-critical and AST-guard files): 188 passed, 6 skipped
- All 5 tests in `test_no_back_edges_core.py`: PASSED
- Existing AST guard tests (`test_no_raw_storage_access.py`, `test_no_server_side_stop_propagation.py`): PASSED
- Config import chain tests (`test_history_no_result_snapshots.py`, `test_local_filter_persistence.py`): PASSED
- My-library Config monkeypatch tests (`test_my_library_tab_*.py`): PASSED (19 tests)

### Inline Acceptance Checks (all GREEN)
- `python -c "import genizah_core, shared.config; assert shared.config.Config is genizah_core.Config"` — OK
- `python -c "from genizah_core import Config; print(Config)"` → `<class 'shared.config.Config'>` — OK
- Path resolution (Codex BLOCKER #1): `Config.BASE_DIR == repo_root`, `FILE_V8 == repo_root/Transcriptions.txt`, `LIBRARIES_CSV == repo_root/libraries.csv` — OK
- `python -c "import shared.session_persistence as s; print(s.HISTORY_FILE)"` → non-empty path — OK
- `python -m ruff check genizah_core.py shared/config.py shared/session_persistence.py` → All checks passed
- `grep -n "^class Config:" genizah_core.py` → NOT FOUND (original deleted)
- `grep -n "from shared.config import Config" genizah_core.py` → line 64 with `# noqa: F401` intact
- No bare `ast.walk(` call in `tests/test_no_back_edges_core.py`

## Known Stubs

None — this is a pure refactor with no UI rendering or data display involved.

## Self-Check

**Created files exist:**
- `shared/config.py`: FOUND
- `tests/test_no_back_edges_core.py`: FOUND

**Commits exist:**
- `31979cac`: refactor(122): extract Config + facade shim + session_persistence retarget — FOUND
- `3048b9e0`: test(122): install GUARD-01 back-edge guard + CONFIG-01 identity test — FOUND

## Self-Check: PASSED
