# Phase 122: Config Enabler - Pattern Map

**Mapped:** 2026-06-25
**Files analyzed:** 4 (2 created, 2 modified)
**Analogs found:** 4 / 4

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `shared/config.py` (CREATE) | config / leaf module | none (class body evaluated at import) | `shared/nli_circuit_breaker.py` (header), `shared/search_tokenizer.py` (header) | role-match — both are stdlib-only shared leaf modules with module-level singleton setup |
| `genizah_core.py` (MODIFY — delete class, add shim) | compatibility facade | import re-export | `genizah_app.py:69–73` — v7.9 `# noqa: F401` re-export shims for `desktop.*` | exact — same re-export shim pattern, same `# noqa: F401` convention |
| `shared/session_persistence.py` (MODIFY — line 32 only) | service | CRUD / file-I/O | itself (one-line import retarget from `genizah_core` → `shared.config`) | n/a — one-line change |
| `tests/test_no_back_edges_core.py` (CREATE) | test / AST static guard | n/a | `tests/test_no_server_side_stop_propagation.py` (simpler pattern) + `tests/test_no_raw_storage_access.py` (parametrize + offenders list) | exact — same AST guard idiom used in both analogs |

---

## Pattern Assignments

### `shared/config.py` (CREATE — config leaf module)

**Analogs:** `shared/nli_circuit_breaker.py` (module docstring + stdlib-only header convention), `shared/search_tokenizer.py` (dependency-light leaf rationale in docstring)

**Module header pattern** (`shared/nli_circuit_breaker.py` lines 1–7, `shared/search_tokenizer.py` lines 1–8):
```python
"""Config class extracted from genizah_core.py (v8.3.0 decomposition).

Stdlib-only leaf module — no imports from genizah_core or any project module.
Both the web process and the desktop app import Config from here directly
(web and desktop via the genizah_core.py permanent facade, shared/ modules
directly from shared.config).
"""
import os
import sys
```

**Class body:** copy verbatim from `genizah_core.py:2292–2426`. The full class body is self-contained (only `os`, `sys`, and `ctypes.wintypes` imported lazily inside `_get_documents_dir`). Key sections of the class body (for the planner's reference — do not alter these; copy verbatim):

- Helper staticmethods `_pick_writable_dir` and `_get_documents_dir` (lines 2298–2339) — travel inside the class body
- Class-level path attributes `BASE_DIR`, `INTERNAL_DIR`, `INDEX_DIR`, `REPORTS_DIR`, etc. (lines 2341–2420)
- Load-time side effect (D-05 — MUST be preserved): `os.makedirs(INDEX_DIR, exist_ok=True)` at lines 2371–2376, inside a `try/except` block
- Constants `SEARCH_LIMIT`, `VARIANT_GEN_LIMIT`, `HTTP_HEADERS`, etc. (lines 2412–2420)
- `@staticmethod resource_path(...)` (lines 2422–2425)

**Tombstone comment pattern for `genizah_core.py`** (replaces the section header at lines 2292–2293 after class deletion):
```python
# Config is now in shared/config.py — imported above via the re-export shim.
```

---

### `genizah_core.py` (MODIFY — delete class body, add re-export shim)

**Analog:** `genizah_app.py` lines 69–73 — the v7.9 `# noqa: F401` re-export shims

**v7.9 shim pattern** (`genizah_app.py:69–73`):
```python
from desktop.viewers import ZoomableScrollArea, FullscreenImageWindow, ManuscriptViewerWidget, _make_scrollable_row, _generate_oxford_dynamic_url  # noqa: F401
from desktop.puzzle import PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow  # noqa: F401
from desktop.vs_cache import DesktopVSCache, VSFetchThread, VSDownloadThread  # noqa: F401
```

**Adapted shim for `genizah_core.py`** — place after the existing `from shared.*` imports (lines 53–63):
```python
from shared.config import Config  # noqa: F401 — permanent compat facade (v8.3.0)
```

**Existing `from shared.*` import block** (`genizah_core.py:50–63`) for placement context:
```python
from shared.nli_circuit_breaker import (
    is_open as _nli_circuit_is_open,
    record_failure as _nli_record_failure,
    record_success as _nli_record_success,
    NLI_CONNECT_TIMEOUT,
    NLI_IIIF_READ_TIMEOUT,
    NLI_MARC_READ_TIMEOUT,
)
# SEED-006: dependency-light hebword tokenizer registration
from shared.search_tokenizer import register_search_tokenizers
# Phase 122: Config extracted to shared/config.py — permanent compat facade (v8.3.0)
from shared.config import Config  # noqa: F401
```

**Critical:** After adding the shim, delete the original `class Config:` block at lines 2295–2426 from `genizah_core.py`. The tombstone comment replaces the section header at lines 2292–2293. If the class body is NOT deleted, Python will rebind `Config` to the local class at line 2295+, breaking the identity assertion (Pitfall 1 in RESEARCH.md).

---

### `shared/session_persistence.py` (MODIFY — line 32 only)

**Change:** One line retarget (D-02). No structural pattern needed — the surrounding file structure is unchanged.

**Before** (`shared/session_persistence.py:32`):
```python
from genizah_core import Config
```

**After:**
```python
from shared.config import Config
```

Line 40 (`HISTORY_FILE = os.path.join(Config.INDEX_DIR, ...)`) is unchanged — it evaluates `Config.INDEX_DIR` at import time. Because `Config` is the same class object after the move (D-04), the evaluation is identical.

---

### `tests/test_no_back_edges_core.py` (CREATE — AST static guard + identity test)

**Primary analog:** `tests/test_no_server_side_stop_propagation.py` (simplest complete pattern: `ast.walk`, offenders list, `assert not offenders`, `(SyntaxError, OSError)` catch)

**Secondary analog:** `tests/test_no_raw_storage_access.py` (demonstrates `pathlib.Path(__file__).resolve().parent.parent` for REPO_ROOT, `rglob("*.py")`, `assert not offenders` with multi-line message)

**File docstring pattern** (from `test_no_server_side_stop_propagation.py` lines 1–19 — the "origin + violation + correct alternative" format):
```python
"""GUARD-01: no extracted shared/ module may import genizah_core at module level.

The EXTRACTED_MODULES registry grows each phase of the v8.3.0 decomposition:
  Phase 122: shared/config.py
  Phase 123: (add entries here as modules are extracted)
  ...

Only module-level imports are scanned (ast.iter_child_nodes — direct children of
Module node only). Lazy imports inside function/method bodies are intentional
and are NOT flagged.
"""
```

**Imports + REPO_ROOT pattern** (from `test_no_server_side_stop_propagation.py:21–23` and `test_no_raw_storage_access.py:25–32`):
```python
import ast
import pathlib

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
```

**Registry constant** (Claude's discretion per CONTEXT.md — plain list, grows each phase):
```python
# Registry: add one entry per phase as modules are extracted (v8.3.0 decomposition).
# Phase 122: Config only.
EXTRACTED_MODULES = [
    "shared/config.py",
]
```

**AST helper function** — uses `ast.iter_child_nodes` (NOT `ast.walk`) to scan only module-level statements. This is the critical difference from the simpler `test_no_server_side_stop_propagation.py` pattern, which uses `ast.walk` because it needs all-depth scanning. For GUARD-01 we need depth-0 only (Pitfall 5 in RESEARCH.md):
```python
def _has_module_level_genizah_core_import(source: str) -> list[int]:
    """Return line numbers of module-level genizah_core imports.

    Uses ast.iter_child_nodes (NOT ast.walk) — yields only direct children of
    the Module node, i.e., top-level statements. Function-body lazy imports
    (e.g., shared/local_indexer.py) are intentional and must not be flagged.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []
    violations = []
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "genizah_core":
            violations.append(node.lineno)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name == "genizah_core" or alias.name.startswith("genizah_core."):
                    violations.append(node.lineno)
    return violations
```

**Parametrized guard test** (pattern from `test_no_raw_storage_access.py` offenders + `test_file_actions.py` `@pytest.mark.parametrize` simple list form):
```python
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

**CONFIG-01 identity test** (co-located in the same file per RESEARCH.md open question recommendation — fewer files, same structural-integrity purpose):
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

**Do NOT register in `_GUI_TEST_FILES`** in `tests/conftest.py` — this test has no Qt dependency and must run in the main suite. (Confirmed: `test_no_raw_storage_access.py` and `test_no_server_side_stop_propagation.py` are also not in `_GUI_TEST_FILES`.)

---

## Shared Patterns

### `# noqa: F401` Re-export Shim Convention
**Source:** `genizah_app.py:69–73` (v7.9 extraction shims)
**Apply to:** The `from shared.config import Config  # noqa: F401` line in `genizah_core.py`

The `# noqa: F401` suppresses ruff's "unused import" warning. Per D-06, do NOT run `ruff check . --fix` — that strips inline `noqa` comments. Run `ruff check genizah_core.py` (per-file, manual review) after the edit.

### AST Guard: `(SyntaxError, OSError)` Catch
**Source:** `tests/test_no_server_side_stop_propagation.py:52–55`
**Apply to:** Any file-reading loop in `tests/test_no_back_edges_core.py`

```python
try:
    source = py_file.read_text(encoding="utf-8")
    tree = ast.parse(source)
except (SyntaxError, OSError):
    continue
```

For the parametrized test the read is direct (not in a loop), so use `path.read_text(encoding="utf-8")` directly — the file existence is asserted first.

### AST Guard: `pathlib.Path(__file__).resolve().parent.parent` for REPO_ROOT
**Source:** `tests/test_no_raw_storage_access.py:32`
**Apply to:** `tests/test_no_back_edges_core.py`

```python
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
```

### UTF-8 File Reading
**Source:** `tests/test_no_raw_storage_access.py:369`, `tests/test_no_server_side_stop_propagation.py:53`
**Apply to:** All file reads in the new test

```python
source = py_file.read_text(encoding="utf-8")
```

---

## No Analog Found

None — all four files have direct analogs in the codebase.

---

## Metadata

**Analog search scope:** `tests/`, `shared/`, `desktop/`, `genizah_app.py`, `genizah_core.py`
**Files scanned (read):** 8 source files + CONTEXT.md + RESEARCH.md
**Pattern extraction date:** 2026-06-25

**Critical ordering note for executor (from RESEARCH.md Pitfall 1):**
The extraction is a single atomic commit:
1. Copy `Config` class body to `shared/config.py` (new file)
2. Delete the `class Config:` block (lines 2295–2426) from `genizah_core.py`
3. Add `from shared.config import Config  # noqa: F401` to `genizah_core.py` imports block (after line 63)
4. Replace the section header comment at lines 2292–2293 with tombstone comment
5. Retarget `shared/session_persistence.py:32`
6. Create `tests/test_no_back_edges_core.py`

Steps 2 and 3 are interdependent — if either is omitted, CONFIG-01 identity test fails.
