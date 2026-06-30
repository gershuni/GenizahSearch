# Phase 122: Config Enabler - Research

**Researched:** 2026-06-25
**Domain:** Python import architecture / class extraction / AST guard authoring
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** GUARD-01 uses the strict, extracted-only assertion (ROADMAP SC#3 authoritative over REQUIREMENTS GUARD-01): a `shared/` module extracted this milestone may not `import genizah_core` at module level AT ALL. Test is parametrized over a registry of milestone-extracted `shared/` modules that grows each phase. Phase 122 registry = `{shared/config.py}` only. Pre-existing back-edges (`shared/exclusion_service.py:17`) are OUT of scope — do NOT add them to the registry in Phase 122.

- **D-02:** Retarget `shared/session_persistence.py:32` from `from genizah_core import Config` to `from shared.config import Config` in Phase 122.

- **D-03:** Full move of the entire `Config` class (not constants-only) to `shared/config.py`.

- **D-04:** `genizah_core.Config` = re-export of the same class object; a test asserts `shared.config.Config is genizah_core.Config` (identity, not a copy). `genizah_core.py` stays a permanent facade; `# noqa: F401` shim preserved.

- **D-05:** Only the self-contained `Config` class + stdlib imports (`os`, `sys`, `ctypes`) travel to `shared/config.py`. The class-body load-time side effect (`os.makedirs(INDEX_DIR, exist_ok=True)` in the class body) must run identically on `import shared.config`.

- **D-06:** Verification gate = full existing pytest suite green. Ruff is per-file review only on the extraction commit — never repo-wide `ruff --fix` (it would strip the `# noqa: F401` re-export shim).

### Claude's Discretion

- Internal mechanics of `tests/test_no_back_edges_core.py`: AST-walk implementation, how the extracted-module registry is represented, and the exact parametrization shape — as long as D-01 holds.
- Exact shim comment wording and import-line placement in `genizah_core.py`.
- Whether the CONFIG-01 identity assertion lives in a new test file or an existing core test.

### Deferred Ideas (OUT OF SCOPE)

None — discussion stayed strictly within phase scope.

</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CONFIG-01 | `Config` defined in `shared/config.py`; `genizah_core.Config` re-exports same class object; all existing callers including `shared/session_persistence.py` work unchanged | Full importer enumeration in §RQ-1; identity-test pattern in §RQ-3 |
| GUARD-01 | Permanent AST back-edge guard installed: no extracted `shared/` module imports `genizah_core` at module level; parametrized registry grows each phase | AST guard pattern in §RQ-3; confirmed clean at Phase 122 in §RQ-6 |
| GUARD-02 | Zero behavior change — full existing pytest suite green at phase boundary | Test invocation in §RQ-5; specific anchors in §RQ-5 |
| GUARD-03 | Source-scanning/AST tests retargeted before deletion — no deletion in Phase 122, but test list documented | Named tests listed in Architecture section |
| GUARD-04 | `genizah_core.py` stays permanent facade; `# noqa: F401` shim preserved | Shim recipe in §RQ-4; ruff.toml F401 selection confirmed |

</phase_requirements>

---

## Summary

Phase 122 is a pure Python class extraction with an extremely narrow code surface: move the
`Config` class (lines 2295–2426 in `genizah_core.py`) to a new `shared/config.py`, install a
one-line re-export shim in `genizah_core.py`, retarget one module-level importer
(`shared/session_persistence.py:32`), and add two test files — the GUARD-01 AST back-edge
guard and the CONFIG-01 identity assertion. No external packages are installed; no behavior
changes; no user-visible changes.

The critical property that makes this safe is the **import direction**: `shared/config.py` is
a stdlib-only leaf (`os`, `sys`, `ctypes`) with no imports from `genizah_core`. After the
move, `genizah_core.py` adds `from shared.config import Config  # noqa: F401` near the top,
which is the correct, cycle-free direction (`genizah_core → shared.config`). All 30+ existing
callers that do `from genizah_core import Config` continue to work through the facade with
zero modification — except `shared/session_persistence.py`, which is retargeted as D-02 to
eliminate the only module-level back-edge that would be in scope.

The cycle-pivot significance: `JoinsManager.JOINS_FILE` (line 10812) and
`ListsManager.LISTS_FILE` (line 11345) reference `Config.INDEX_DIR` at **class-definition
time** (as class-body attributes, not method bodies). Once `Config` lives in `shared/config.py`
and `genizah_core.py` imports it at the top via the shim, these class definitions evaluate
correctly. Phases 123–125, which extract `JoinsManager`, `ListsManager`, `VariantManager`,
etc., can then do `from shared.config import Config` at their own module level without any
import cycle.

**Primary recommendation:** Implement as one atomic commit: copy Config to `shared/config.py`,
add the facade shim to `genizah_core.py`, retarget `session_persistence.py`, add two test
files. Run per-file ruff review on the changed files only.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| `Config` class definition | `shared/config.py` (after move) | `genizah_core.py` (permanent facade) | leaf module, stdlib-only, no app-tier dependency |
| `Config` re-export compatibility | `genizah_core.py` (facade) | — | permanent backward-compat shim for 30+ callers |
| Session persistence `Config` import | `shared/session_persistence.py` → `shared.config` | — | D-02: direct import, no facade needed post-retarget |
| GUARD-01 enforcement | `tests/test_no_back_edges_core.py` | CI | AST scan of extracted-this-milestone registry |
| CONFIG-01 identity assertion | `tests/test_no_back_edges_core.py` or new `tests/test_config_identity.py` | — | Claude's discretion which file |

---

## Standard Stack

### Core (no new external packages — this phase is stdlib-only)

| Module | Version | Purpose | Notes |
|--------|---------|---------|-------|
| `ast` | stdlib | AST walking for GUARD-01 guard | [VERIFIED: Python stdlib] |
| `pathlib` | stdlib | File enumeration in guard test | [VERIFIED: Python stdlib] |
| `pytest` | already installed | Test runner | [VERIFIED: existing test suite] |
| `os`, `sys`, `ctypes` | stdlib | Used inside Config class body | [VERIFIED: existing code] |

No external packages are installed in Phase 122. The Package Legitimacy Audit section is
omitted per the protocol (no external packages).

---

## Architecture Patterns

### Research Question 1 (RQ-1): Full `Config` Importer Enumeration

Verified by live grep of the repository tree (2026-06-25).

#### Module-level importers — `from genizah_core import Config` at file top scope

| File | Line | Import Form | Classification |
|------|------|-------------|----------------|
| `shared/session_persistence.py` | 32 | `from genizah_core import Config` | **(b) MUST retarget** (D-02) — `Config.INDEX_DIR` used at module level on line 40 |
| `web/api.py` | 17 | `from genizah_core import Config` | (a) facade covers — no change needed |
| `desktop/viewers.py` | 18 | `from genizah_core import Config, get_logger, tr` | (a) facade covers |
| `desktop/image_loader.py` | 10 | `from genizah_core import Config, MetadataManager, get_logger` | (a) facade covers |
| `desktop/my_library_tab.py` | 69 | `from genizah_core import Config, tr, CURRENT_LANG` | (a) facade covers |
| `build_index.py` | 17 | `from genizah_core import Indexer, MetadataManager, VariantManager, LabEngine, Config` | (a) facade covers |
| `genizah_app.py` | 34 | `from genizah_core import Config, MetadataManager, ...` (long list) | (a) facade covers |

#### Lazy / function-body importers — `from genizah_core import Config` inside function

| File | Line | Context | Classification |
|------|------|---------|----------------|
| `desktop/telemetry.py` | 1239 | Inside `_enable_faulthandler()` (comment: "lazy — avoids circular at module level") | (a) lazy, facade covers |
| `web/stats_service.py` | 78, 184 | Inside method bodies | (a) lazy, facade covers |

#### `import genizah_core` + attribute-access pattern

| File | Lines | Attribute Used | Classification |
|------|-------|----------------|----------------|
| `desktop/join_workbench.py` | 819, 847 | `genizah_core.Config.HTTP_HEADERS` (line 847) | (a) facade covers via `genizah_core.Config` re-export |
| `desktop/consent_dialog.py` | 25 | `genizah_core.save_app_config`, `genizah_core.CURRENT_LANG` | (a) no Config access here |

#### Test-file importers (inside test functions / fixtures — lazy scope)

All test files listed below import inside test functions or fixtures, not at module level.
They all work through the `genizah_core.Config` facade without modification.

| File | Lines | Notes |
|------|-------|-------|
| `tests/conftest.py` | 224 | Inside fixture body — facade covers |
| `tests/test_generate_synthetic_rows.py` | 51, 74, 96, 701 | Inside test functions |
| `tests/test_local_lab_invalidation.py` | 514 | Inside test function |
| `tests/test_my_library_tab_progress_phases.py` | 38 | Inside fixture |
| `tests/test_my_library_tab_prior_status_cache.py` | 33 | Inside fixture |
| `tests/test_my_library_tab_skip_suppresses_rescan.py` | 50 | Inside fixture |
| `tests/test_native_crash.py` | 35 | Inside test |
| `tests/test_my_library_tab_reset_guard.py` | 43 | Inside fixture |
| `tests/test_recovery_scan_runs_cleanup.py` | 43 | Inside fixture |
| `tests/test_unified_tree_async_populate.py` | 318 | Inside test |

#### `patch.object(genizah_core.Config, ...)` pattern — monkeypatching tests

These tests patch Config attributes via `patch.object`. Because `genizah_core.Config` IS
`shared.config.Config` (same object — D-04), patching `genizah_core.Config.LOCAL_INDEX_DIR`
mutates the same class object that `shared.config.Config` points to. **No change needed** —
identity re-export guarantees patch propagation.

| File | Lines |
|------|-------|
| `tests/test_local_index_open_fallback.py` | 59, 88, 102 |
| `tests/test_local_reload_after_refresh.py` | 99–223 |
| `tests/conftest.py` | 225–226 (monkeypatch.setattr on Config) |

**Summary:** Exactly ONE file requires retargeting (D-02): `shared/session_persistence.py:32`.
All other callers work unchanged through the `genizah_core.Config` re-export facade.
`patch.object(genizah_core.Config, ...)` tests work identically because of object identity.

---

### Research Question 2 (RQ-2): Config Class Symbol Surface

**Verified by reading `genizah_core.py:2292–2426` directly (2026-06-25).**

The `Config` class spans lines 2295–2426. The class body contains:

#### Helper methods (inside class body — travel with the class)
- `_pick_writable_dir(primary, fallback)` — lines 2298–2316, stdlib only (`os`)
- `_get_documents_dir()` — lines 2318–2339, imports `ctypes.wintypes` inside the method body

#### Class-level attributes (evaluated at import time)
All resolved using only stdlib modules (`os`, `sys`):
- `BASE_DIR`, `INTERNAL_DIR` — depend on `sys.frozen`, `sys.executable`, `sys._MEIPASS`
- `FILE_V8`, `FILE_V7` — `os.path.join(BASE_DIR, ...)`
- `_PORTABLE_INDEX_PATH`, `_APPDATA_PATH`, `_LEGACY_PATH` — `os.path.join(...)`, `os.getenv(...)`
- `INDEX_DIR` — chosen from the three paths above, with `if os.path.exists(...)` checks
- **Load-time side effect (D-05):** `os.makedirs(INDEX_DIR, exist_ok=True)` at lines 2371–2376
- `REPORTS_DIR` — calls `_pick_writable_dir(...)` and `_get_documents_dir()`
- `IMAGE_CACHE_DIR`, `CACHE_META`, `CACHE_NLI`, `CONFIG_FILE`, `SESSION_FILE`, `LANGUAGE_FILE`, `BROWSE_MAP`, `LOG_FILE` — all `os.path.join(INDEX_DIR, ...)`
- `LAB_DIR`, `LAB_INDEX_DIR`, `LAB_CONFIG_FILE`, `LAB_WEIGHTS_FILE`, `LAB_LOG_FILE`
- `LOCAL_INDEX_DIR`, `LOCAL_LAB_INDEX_DIR`
- `LIBRARIES_CSV`, `OXFORD_DB`, `HELP_FILE` — use `INTERNAL_DIR`
- Constants: `SEARCH_LIMIT`, `VARIANT_GEN_LIMIT`, `REGEX_VARIANTS_LIMIT`, `WORD_TOKEN_PATTERN`, `MAX_EXPANDED_TERMS`, `NLI_IIIF_BASE`, `USER_AGENT`, `HTTP_HEADERS`

#### Static method
- `resource_path(relative_path)` — returns `os.path.join(Config.INTERNAL_DIR, relative_path)` via `Config.INTERNAL_DIR`

#### External module-level dependencies in Config's class body
Config references ONLY stdlib names (`os`, `sys`) and `ctypes.wintypes` (imported lazily inside `_get_documents_dir`). It references NO names defined elsewhere in `genizah_core.py` at class-definition time.

#### Forward-reference check (does anything use Config before line 2295?)
`Config` is referenced before line 2295 in `genizah_core.py` (e.g., `text_to_fingerprint` at line 340 uses `Config.WORD_TOKEN_PATTERN`, `LabSettings` at lines 603–991 uses `Config.*` extensively). However, ALL of these references are inside **function/method bodies** — they are evaluated at call time, not at module-import time. The exception is:

- `JoinsManager.JOINS_FILE = os.path.join(Config.INDEX_DIR, ...)` at line 10812 — class-level attribute, evaluated when Python processes the `class JoinsManager:` statement.
- `ListsManager.LISTS_FILE = os.path.join(Config.INDEX_DIR, ...)` at line 11345 — same pattern.

Both `JoinsManager` and `ListsManager` are defined AFTER `Config` (at lines 10812 and 11345, both > 2295), so the forward-reference is NOT a problem when `genizah_core.py` is loaded. After the move, `genizah_core.py` will import `Config` from `shared.config` at the top, making `Config` available before any class body in `genizah_core.py` is evaluated.

**Conclusion:** `shared/config.py` needs ONLY:
```python
import os
import sys
# ctypes is imported lazily inside _get_documents_dir — no top-level import needed
```
No other imports are required. The class body is self-contained.

---

### Research Question 3 (RQ-3): Canonical AST Back-Edge Guard Pattern

**Verified by reading the following test files:**
- `tests/test_no_raw_storage_access.py` (Phase 87 — most complete pattern)
- `tests/test_no_server_side_stop_propagation.py` (simpler pattern, directly applicable)
- `tests/test_wr01_open_local_browse_page_ast.py` (source-read pattern)

#### Idiomatic pattern this codebase uses

```python
# Standard pattern (from test_no_server_side_stop_propagation.py + test_no_raw_storage_access.py):
import ast
import pathlib

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

def test_no_<violation>():
    offenders = []
    for py_file in SOME_DIR.rglob("*.py"):
        try:
            source = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source)
        except (SyntaxError, OSError):
            continue
        for node in ast.walk(tree):
            if <condition>:
                offenders.append(f"  {py_file.relative_to(REPO_ROOT)}:{node.lineno}")
    assert not offenders, "..."
```

Key features observed:
1. Uses `ast.walk` (not `ast.NodeVisitor`) for simple, non-hierarchical scans
2. Catches `(SyntaxError, OSError)` gracefully — never hard-fails on unparseable files
3. Uses `pathlib.Path` for file enumeration (`rglob("*.py")`)
4. Builds an `offenders` list, single `assert not offenders` at the end
5. Error message names the violation, the correct alternative, and the offending locations
6. Files from `WEB_DIR` or `REPO_ROOT / "shared"` scanned by directory
7. NOT registered in `_GUI_TEST_FILES` in conftest.py (no Qt, no event dispatch) — runs in main suite

#### Recommended shape for `tests/test_no_back_edges_core.py`

The guard needs: (1) a registry of extracted modules, (2) parametrized over each one, (3)
AST-scans each file for module-level `import genizah_core` or `from genizah_core import ...`.

```python
# tests/test_no_back_edges_core.py
"""GUARD-01: no extracted shared/ module may import genizah_core at module level.

The EXTRACTED_MODULES registry grows each phase of the v8.3.0 decomposition:
  Phase 122: shared/config.py
  Phase 123: shared/variants.py, shared/codicological.py, shared/responsa.py, ...
  Phase 124: shared/metadata_manager.py, shared/indexer.py
  Phase 125: shared/search_engine.py, shared/lab_engine.py, shared/lab_settings.py
"""
import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Registry: add one entry per phase as modules are extracted.
# Phase 122: config only.
EXTRACTED_MODULES = [
    "shared/config.py",
]


def _has_module_level_genizah_core_import(source: str) -> list[int]:
    """Return line numbers of module-level genizah_core imports.

    Only module-level (depth-0) ImportFrom/Import nodes are scanned.
    Lazy imports inside function/method bodies are intentional and allowed.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    violations = []
    # Only top-level statements (direct children of Module) are module-level
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "genizah_core":
            violations.append(node.lineno)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "genizah_core" or alias.name.startswith("genizah_core."):
                    violations.append(node.lineno)
    return violations


@pytest.mark.parametrize("rel_path", EXTRACTED_MODULES)
def test_no_module_level_genizah_core_import(rel_path):
    """GUARD-01 strict: extracted shared/ module must not import genizah_core at module level."""
    path = REPO_ROOT / rel_path
    assert path.exists(), f"Extracted module {rel_path} not found — was it created?"
    source = path.read_text(encoding="utf-8")
    violations = _has_module_level_genizah_core_import(source)
    assert not violations, (
        f"{rel_path} imports genizah_core at module level on lines {violations}. "
        "GUARD-01 violation: extracted shared/ modules must be import-cycle-free. "
        "Use lazy imports inside method bodies if genizah_core symbols are needed, "
        "or retarget to the shared/ module that owns the symbol."
    )
```

Key design decisions:
- Uses `ast.iter_child_nodes(tree)` to scan ONLY module-level statements — function-body lazy imports (e.g., `shared/local_indexer.py:3154`) are NOT flagged
- Parametrized over `EXTRACTED_MODULES` list — adding a new extracted module automatically enters the scan
- The registry is a plain list constant at the top of the file — visible and auditable
- NOT in `_GUI_TEST_FILES` (no Qt) — runs in the main suite

#### CONFIG-01 identity test (D-04)

This is small enough to live in a dedicated file or alongside the GUARD-01 test:

```python
def test_config_identity():
    """CONFIG-01: genizah_core.Config is the same class object as shared.config.Config."""
    import shared.config
    import genizah_core
    assert shared.config.Config is genizah_core.Config, (
        "genizah_core.Config is not the same object as shared.config.Config. "
        "The re-export shim in genizah_core.py must be: "
        "from shared.config import Config  # noqa: F401"
    )
```

Claude's discretion: this can live in `test_no_back_edges_core.py` or in a separate
`tests/test_config_identity.py` — the planner should pick one location.

---

### Research Question 4 (RQ-4): The v7.9 Extraction Recipe (Proven Precedent)

**Verified by reading `genizah_app.py:63–73` and `desktop/viewers.py:1–22`, `desktop/puzzle.py:1–20`.**

The v7.9 recipe used for `desktop/puzzle.py`, `desktop/viewers.py`, etc.:

#### Step 1 — New module header
```python
"""[ClassName] extracted from genizah_app.py (v7.9 decomposition)."""
# ... stdlib + third-party imports ...
from genizah_core import Config, get_logger, tr  # consumers of the new module
# ... class/function definitions ...
```

#### Step 2 — Re-export shim in the source module
In `genizah_app.py`, immediately after or near the original class/function location, add:
```python
from desktop.puzzle import PuzzleFragmentItem, PuzzleCanvasView, ...  # noqa: F401
```

The `# noqa: F401` suppresses ruff's F401 "unused import" warning because the names
ARE used — just not in this file after the extraction; the shim exists for callers that
still `from genizah_app import PuzzleCanvasView`.

#### Adaptation for `genizah_core.py` → `shared/config.py`

The same recipe, adapted:

**`shared/config.py` (new file):**
```python
"""Config class extracted from genizah_core.py (v8.3.0 decomposition)."""
import os
import sys

class Config:
    """Static paths and limits used by the application and by bundled binaries."""
    # ... (full class body copied verbatim from genizah_core.py:2295–2426) ...
```

**`genizah_core.py` (add near existing top-of-file imports, after the `shared.` imports block):**
```python
from shared.config import Config  # noqa: F401 — permanent compat facade (v8.3.0)
```

The `# noqa: F401` is ESSENTIAL. Without it, `ruff` (which selects F401 in `ruff.toml`) would
flag this as an unused import if ruff can't see all use sites through the try/except or
because it doesn't trace through re-export patterns. The comment explains it's permanent.

#### Why `genizah_core.py` keeps the shim permanently

`genizah_core.py` is a permanent compatibility facade (GUARD-04 + D-04 + §7 Q5 adjudication).
Unlike the desktop shims in `genizah_app.py` (which get deleted in Phase 127), the
`genizah_core.py` shims NEVER get deleted — the web and desktop both import from it.

#### Placement in `genizah_core.py`

The import should go in the imports block at the top of the file, after the existing
`from shared.nli_circuit_breaker import ...` and `from shared.search_tokenizer import ...`
lines (lines 53–63), to be consistent with the established `genizah_core → shared-leaf`
import direction. The `Config` class definition at line 2295 is NOT deleted in Phase 122
(it stays as the authoritative definition until the next refactor; in this phase we are
adding a new definition in `shared/config.py` and making `genizah_core.Config` point to it).

Wait — note: the re-export shim makes `genizah_core.Config` point to `shared.config.Config`.
The ORIGINAL class at line 2295 in `genizah_core.py` must be DELETED, OR the import shim
must come AFTER the class definition (so the shim's name binding overrides the class).

**Critical ordering detail:** Python module execution is sequential. If `genizah_core.py`
defines `class Config:` at line 2295 AND also has `from shared.config import Config # noqa: F401`
at line 60, the IMPORT at line 60 runs first, binding `Config` to `shared.config.Config`.
Then the `class Config:` at line 2295 REBINDS `Config` to a new, different class object.
Result: `genizah_core.Config is not shared.config.Config` — CONFIG-01 FAILS.

**Correct approach:** Either:
- (A) Delete the original `class Config:` block from `genizah_core.py` and put the shim at the top — this makes `genizah_core.Config` point to `shared.config.Config`. [RECOMMENDED]
- (B) Keep the original class, but put the shim AFTER the class — the shim overwrites the name. This is confusing and fragile.

Option (A) is the clean implementation: copy the class to `shared/config.py`, delete the class from `genizah_core.py`, add the shim at the top. This is the same pattern as the v7.9 desktop extraction (the original class in `genizah_app.py` was deleted from there and replaced with the import shim).

**Recommended placement in `genizah_core.py`**: Add the shim line to the imports block, near the other `from shared.*` imports (lines 53–63), e.g.:

```python
from shared.nli_circuit_breaker import (...)   # existing line ~53
from shared.search_tokenizer import register_search_tokenizers  # existing line ~63
from shared.config import Config  # noqa: F401 — permanent compat facade (v8.3.0)
```

Then delete the `class Config:` block (lines 2292–2426) from `genizah_core.py`.

---

### Research Question 5 (RQ-5): Test Suite Execution Reality

**Verified by reading `tests/conftest.py`, `pyproject.toml`, and project memory.**

#### Test suite facts (from conftest.py and project memory)

- **Total test files:** 320 `test_*.py` files
- **PyQt6 segfault hazard:** Tests that construct a real `QApplication` and dispatch widget events accumulate process-global Qt state — causes SIGSEGV after ~3000 prior tests on headless runners. Mitigation: marker-based split (`-m "not gui"` main job; `-m gui` dedicated job).
- **GUI test files** (in `_GUI_TEST_FILES`): `test_telemetry_consent_ux.py`, `test_seed022_desktop_badge.py`, `test_catalog_availability_filter.py` — do NOT add `test_no_back_edges_core.py` here (no Qt).
- **CI skip list** (GITHUB_ACTIONS=true): `test_my_library_tab*.py`, `test_unified_tree_async_populate.py`, `test_folder_walk_worker.py`, `test_local_optout_persistence.py`, `test_recovery_scan_runs_cleanup.py`, `test_disk_headroom.py`, `test_join_workbench_construct.py`, `test_pdf_image_controller.py`, `test_pdf_page_renderer.py`, `test_phase_97_2_sqlite_vs_tantivy_consistency.py`.
- **Render smoke tests** (`tests/render_smoke/`): separate job, `-m render_smoke`.
- **Scale tests**: disabled by default, `--run-scale` to enable.
- **`-n auto` is OOM-dangerous** on this repo — Tantivy loads its index per worker; run bare `pytest` or `-n 2` max.

#### Correct test invocation for "full suite green" on Windows

**Quick smoke (pre-commit, < 60s) — Config extraction specific:**
```
PYTHONUTF8=1 pytest tests/test_history_no_result_snapshots.py tests/test_no_back_edges_core.py tests/test_local_filter_persistence.py -x -q
```

**Main suite (non-GUI, non-render-smoke, non-scale) — phase gate:**
```
PYTHONUTF8=1 pytest tests/ -m "not gui and not render_smoke" -x -q
```

**On Windows, note:** `PYTHONUTF8=1` is needed because `check_docs.py` crashes on emoji under cp1255; the same applies to test output with Hebrew strings.

**For the executor on this Windows machine** (not CI):
- Do NOT set `GITHUB_ACTIONS=true` — that would skip the `test_my_library_tab_*.py` files which test Config monkeypatching (D-14 pattern in conftest.py).
- Do NOT use `-n auto`.
- Headless Qt: set `QT_QPA_PLATFORM=offscreen` if running GUI-marked tests locally without a display.

#### Test files most likely to exercise the Config import path

| Test File | Config Interaction | Priority |
|-----------|-------------------|---------|
| `tests/test_history_no_result_snapshots.py` | Imports `shared.session_persistence` → uses `HISTORY_FILE` derived from `Config.INDEX_DIR` | HIGH — directly exercises D-02 retarget |
| `tests/test_local_filter_persistence.py` | Imports `shared.session_persistence` functions | HIGH |
| `tests/test_local_optout_persistence.py` | `from shared.session_persistence import ...` | HIGH (CI-skipped on GITHUB_ACTIONS) |
| `tests/conftest.py` fixture `temp_local_index_dir` | `from genizah_core import Config; monkeypatch.setattr(Config, ...)` | HIGH — tests the identity-patch guarantee |
| `tests/test_local_reload_after_refresh.py` | `patch.object(genizah_core.Config, ...)` | HIGH |
| `tests/test_local_index_open_fallback.py` | `patch.object(genizah_core.Config, ...)` | HIGH |
| `tests/test_no_back_edges_core.py` | NEW — the GUARD-01 test itself | HIGH |

---

### Research Question 6 (RQ-6): `shared/config.py` Cleanliness and Circular Import Analysis

**Verified by examining all imports in the `Config` class body.**

#### `shared/config.py` imports needed

```python
import os
import sys
```

`ctypes` and `ctypes.wintypes` are imported inside `_get_documents_dir()` (method body, not
class body), so they do NOT appear in the module-level imports of `shared/config.py`.

No other imports are required. The class body is entirely stdlib-only.

#### Circular import risk analysis

```
shared/config.py → [no imports from genizah_core or any project module]
genizah_core.py → from shared.config import Config
```

This is the correct, cycle-free direction:
- `shared.config` is a LEAF — it imports nothing from the project
- `genizah_core` → `shared.config` is a SAFE directed edge (same as `genizah_core` → `shared.nli_circuit_breaker`)

No circular import risk in any direction:
- `shared/session_persistence.py` after retarget: `from shared.config import Config` — `shared.config` is a leaf, no cycle
- `genizah_core.py` after adding shim: `from shared.config import Config` — `shared.config` doesn't import `genizah_core`, no cycle

#### GUARD-01 trivially passes at install time

`shared/config.py` has no `import genizah_core` or `from genizah_core import ...` at any
level. The GUARD-01 test scanning only top-level (`ast.iter_child_nodes`) will find zero
violations immediately upon installation.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Detecting module-level vs. function-level imports | Custom scope-tracker | `ast.iter_child_nodes(ast.parse(source))` — only yields top-level nodes | Python AST Module has exactly the top-level statements as its direct children |
| Checking if a re-export is "the same object" | Custom metadata comparison | `shared.config.Config is genizah_core.Config` — Python `is` identity check | Direct identity check is the canonical Python way; no need for `id()` or `__name__` comparison |
| Suppressing F401 for re-exports | Blanket `# noqa` | `# noqa: F401` on the specific import line | ruff.toml selects F401 specifically; inline noqa suppresses just that rule per-line without disabling others |

**Key insight:** The AST guard pattern is already proven in this codebase (4 existing AST guard tests). Reuse `ast.iter_child_nodes` (not `ast.walk`) to get ONLY module-level statements — this is the precise, non-over-scanning approach.

---

## Common Pitfalls

### Pitfall 1: Shim Placed Before Original Class Definition (Name Collision)
**What goes wrong:** If `genizah_core.py` has `from shared.config import Config  # noqa: F401` at line 60 AND `class Config:` at line 2295, Python evaluates the import first, then the class definition REBINDS `Config` to a new object. `genizah_core.Config` is the new local class, NOT the `shared.config.Config`. CONFIG-01 identity test fails.
**Why it happens:** Forgetting to delete the original class definition from `genizah_core.py`.
**How to avoid:** The extraction is: (1) copy class to `shared/config.py`, (2) DELETE the class block from `genizah_core.py`, (3) add import shim at top. The class must be GONE from `genizah_core.py`; the facade line is the only `Config` binding.
**Warning signs:** `test_config_identity` fails with "not the same object."

### Pitfall 2: `ruff --fix` Stripping the `# noqa: F401` Shim
**What goes wrong:** Running `ruff check --fix` (or `ruff format --fix`) repo-wide removes `# noqa: F401` comments silently. The next lint pass then flags the import as unused, and `ruff --fix` removes it. `from genizah_core import Config` stops working — all callers break.
**Why it happens:** `ruff --fix` is aggressive with unused imports.
**How to avoid:** Per D-06, per-file ruff review only: `ruff check genizah_core.py` — review output manually; do NOT run `ruff check . --fix`.
**Warning signs:** `ruff check genizah_core.py` shows the shim line; it should pass (the `# noqa: F401` suppresses it). If the shim line disappeared, a `--fix` was run.

### Pitfall 3: `monkeypatch.setattr(genizah_core.Config, ...)` Stops Working
**What goes wrong:** If for any reason `genizah_core.Config` is NOT the same object as `shared.config.Config`, then `monkeypatch.setattr(genizah_core.Config, "LOCAL_INDEX_DIR", ...)` patches one object while the code under test reads from the other. Tests appear to pass (no error) but the patching has no effect.
**Why it happens:** The class definition was not deleted from `genizah_core.py` (Pitfall 1), or the shim was placed in the wrong order.
**How to avoid:** Run `test_config_identity` first. Then run the `temp_local_index_dir` fixture-dependent tests.
**Warning signs:** `test_local_reload_after_refresh` or `test_local_index_open_fallback` pass when run alone but fail when the session has already imported the patched path.

### Pitfall 4: `_get_documents_dir` ctypes import fails silently on non-Windows
**What goes wrong:** `ctypes.wintypes` raises `ImportError` on Linux. The class body has `try/except Exception: pass` around it — so it degrades gracefully. But if the outer try block is removed during copying, Linux CI breaks.
**Why it happens:** Accidentally removing the try/except when copying the class body.
**How to avoid:** Copy the class body verbatim. Verify `_get_documents_dir()` still has the try/except block.
**Warning signs:** CI fails on Linux with `AttributeError: module 'ctypes' has no attribute 'wintypes'`.

### Pitfall 5: GUARD-01 test scans lazy imports (false positives)
**What goes wrong:** If the guard uses `ast.walk` instead of `ast.iter_child_nodes`, it will also find `from genizah_core import ...` inside function bodies (e.g., `shared/local_indexer.py:3154`). Those are intentional lazy imports and would cause false failures.
**Why it happens:** `ast.walk` traverses ALL nodes at all depths.
**How to avoid:** Use `ast.iter_child_nodes(tree)` which yields ONLY direct children of the Module node — i.e., top-level statements only. Function-body imports are nested inside FunctionDef → body → and are NOT direct children of Module.
**Warning signs:** GUARD-01 test fails for modules that clearly don't import genizah_core at module level.

---

## Code Examples

### Complete `shared/config.py` (verified from source)

The entire class body (lines 2295–2426 in genizah_core.py) travels verbatim. Only the file
header changes:

```python
# shared/config.py
"""Config class extracted from genizah_core.py (v8.3.0 decomposition)."""
# Source: genizah_core.py:2292-2426
import os
import sys


# ==============================================================================
#  CONFIG CLASS (EXE Compatible)
# ==============================================================================
class Config:
    """Static paths and limits used by the application and by bundled binaries."""
    # ... (full class body from genizah_core.py:2295-2426, verbatim) ...
```

### The re-export shim in `genizah_core.py`

Add after existing `from shared.*` imports (lines 53–63):

```python
from shared.config import Config  # noqa: F401 — permanent compat facade (v8.3.0)
```

### The D-02 retarget in `shared/session_persistence.py`

Change line 32 from:
```python
from genizah_core import Config
```
to:
```python
from shared.config import Config
```

No other changes in `session_persistence.py`. Line 40 (`HISTORY_FILE = os.path.join(Config.INDEX_DIR, ...)`) is unchanged — it evaluates `Config.INDEX_DIR` at import time regardless of which module `Config` came from.

---

## State of the Art

| Old Approach | Current Approach | Impact |
|--------------|-----------------|--------|
| `Config` defined in `genizah_core.py` | `Config` in `shared/config.py` (leaf module) | Enables all Phase 123–125 core extractions without import cycles |
| No back-edge guard | `tests/test_no_back_edges_core.py` (permanent) | Structural invariant enforced at every future phase boundary |
| `shared/session_persistence.py` imports via `genizah_core` | Imports directly from `shared.config` | Removes the one real module-level back-edge from `shared/` to `genizah_core` |

---

## Assumptions Log

> All claims in this research were verified directly against the live codebase tree (2026-06-25) or are Python stdlib behavior.

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `Config` class body has no runtime dependency on any non-stdlib name from `genizah_core.py` | RQ-2 | LOW — verified by reading lines 2295–2426; all names are `os`, `sys`, literal constants |
| A2 | `ast.iter_child_nodes(ast.parse(source))` yields only module-level statements, not function-body statements | RQ-3 | LOW — documented Python stdlib behavior; `Module` node's direct children are top-level statements |
| A3 | The `patch.object(genizah_core.Config, ...)` pattern in tests will work after the move because of object identity | RQ-1 | LOW — Python's `monkeypatch.setattr` mutates the object in place; same object means same mutation |

---

## Open Questions

1. **Where should the CONFIG-01 identity test live?**
   - What we know: Claude's discretion (CONTEXT.md). Two options: (a) add `test_config_identity` function to `tests/test_no_back_edges_core.py`, or (b) create `tests/test_config_identity.py`.
   - What's unclear: Whether co-locating guard + identity test in one file aids discoverability.
   - Recommendation: Co-locate in `tests/test_no_back_edges_core.py` — fewer files, same purpose (structural integrity of the extraction).

2. **Should the `class Config:` section comment be preserved in `genizah_core.py`?**
   - What we know: The section header `# CONFIG CLASS (EXE Compatible)` at line 2292–2293 will lose its target after deletion. A short tombstone comment is conventional.
   - Recommendation: Replace the section with a one-line comment: `# Config is now in shared/config.py — imported above via the re-export shim.`

---

## Environment Availability

This phase is a pure code/config change. No external tools, services, or runtimes beyond the existing Python 3.10+ environment and pytest are required. Environment availability check: SKIPPED (no external dependencies).

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing, no version bump needed) |
| Config file | `pyproject.toml` (`[tool.pytest.ini_options]`) |
| Quick run command | `PYTHONUTF8=1 pytest tests/test_no_back_edges_core.py tests/test_history_no_result_snapshots.py -x -q` |
| Full suite command | `PYTHONUTF8=1 pytest tests/ -m "not gui and not render_smoke" -x -q` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| CONFIG-01 | `shared.config.Config is genizah_core.Config` (identity) | unit | `pytest tests/test_no_back_edges_core.py::test_config_identity -x` | Wave 0 |
| GUARD-01 | `shared/config.py` has no module-level `import genizah_core` | AST static | `pytest tests/test_no_back_edges_core.py -x` | Wave 0 |
| GUARD-02 | Full existing pytest suite green | integration | `PYTHONUTF8=1 pytest tests/ -m "not gui and not render_smoke" -x -q` | Existing |
| GUARD-03 | No source-scanning tests broken (none are deleted this phase) | structural | Covered by GUARD-02 full suite run | Existing |
| GUARD-04 | `from genizah_core import Config` still resolves | import smoke | `python -c "from genizah_core import Config; print(Config)"` | Inline check |
| D-02 | `session_persistence` imports `Config` from `shared.config`, not `genizah_core` | unit | `pytest tests/test_history_no_result_snapshots.py tests/test_local_filter_persistence.py -x` | Existing |

### Sampling Rate

- **Per commit:** `PYTHONUTF8=1 pytest tests/test_no_back_edges_core.py tests/test_history_no_result_snapshots.py tests/test_local_filter_persistence.py -x -q`
- **Phase gate:** `PYTHONUTF8=1 pytest tests/ -m "not gui and not render_smoke" -x -q` — must be green before closing phase

### Wave 0 Gaps

- [ ] `tests/test_no_back_edges_core.py` — covers GUARD-01 + CONFIG-01 (NEW)
- (No other new test infrastructure needed — all other anchors use existing tests)

---

## Security Domain

This phase makes no network calls, handles no user input, introduces no cryptography, and
makes no authentication changes. It is a pure in-process Python class relocation.
`security_enforcement` does not apply to this phase. ASVS categories: not applicable.

---

## Sources

### Primary (HIGH confidence)
- Live source read: `genizah_core.py:2292–2426` — Config class body, verified 2026-06-25
- Live grep: all `from genizah_core import.*Config` in repo, verified 2026-06-25
- Live read: `shared/session_persistence.py:28–40` — module-level Config usage confirmed
- Live read: `tests/test_no_raw_storage_access.py`, `tests/test_no_server_side_stop_propagation.py` — AST guard pattern confirmed
- Live read: `tests/conftest.py:1–260` — GUI split, CI skip list, `_GUI_TEST_FILES` confirmed
- Live read: `ruff.toml` — F401 selection confirmed
- Live read: `pyproject.toml` — pytest config confirmed
- Live read: `genizah_app.py:63–73`, `desktop/viewers.py:1–22` — v7.9 extraction recipe confirmed

### Secondary (MEDIUM confidence)
- `.planning/seeds/SEED-020-decomposition-map.md` §7 — Codex review corrections; import topology analysis
- `.planning/phases/122-config-enabler/122-CONTEXT.md` — locked decisions D-01..D-06

---

## Metadata

**Confidence breakdown:**
- Config class symbol surface: HIGH — read from live source
- Importer enumeration: HIGH — live grep of entire repo
- AST guard pattern: HIGH — read from existing test files in repo
- v7.9 shim recipe: HIGH — read from live `genizah_app.py` and `desktop/viewers.py`
- Test invocation: HIGH — read from `conftest.py` and project memory
- Circular import analysis: HIGH — Python import semantics, verified in source

**Research date:** 2026-06-25
**Valid until:** This is a static refactor with no external dependencies — findings remain valid until the codebase changes.
