# Phase 106: Joins Lab Shared Core — Pattern Map

**Mapped:** 2026-06-03
**Files analyzed:** 2 (shared/joins_lab.py, tests/test_joins_lab.py)
**Analogs found:** 2 / 2

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/joins_lab.py` | service + domain model | request-response (pure + adapter) | `shared/refinement.py` (pure functions + dataclass) + `shared/visual_similarity_service.py` (singleton pattern) | exact (split analog — refinement for shape, VS for singleton) |
| `tests/test_joins_lab.py` | test | batch (unit coverage of all logic units) | `tests/test_refinement.py` (MockSearcher + class-based + Hebrew fixtures) | exact |

---

## Pattern Assignments

### `shared/joins_lab.py` (service + domain model, pure + adapter)

**Primary analog:** `shared/refinement.py` (pure functions, dataclass, no I/O dependency)
**Secondary analog:** `shared/visual_similarity_service.py` (module-level singleton)
**Frozen-dataclass analog:** `shared/fist_cudl_bridge.py:80-97` (only `frozen=True` usage in `shared/`)

---

#### Imports pattern

Source: `shared/refinement.py` lines 1-22 (pure shared module, no I/O, stdlib only):

```python
# -*- coding: utf-8 -*-
"""
<one-paragraph module docstring — statelessness contract, what the module contains>
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Optional
```

Source: `shared/fist_cudl_bridge.py` lines 34-44 (frozen dataclass module):

```python
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple
```

**For `shared/joins_lab.py`, the imports block will be:**

```python
# -*- coding: utf-8 -*-
"""
Joins Lab shared core — pure domain logic + SearchExecutor adapter contract.

Provides: BuilderRow / SideQuery / Candidate frozen dataclasses, compose(),
cross_side_membership(), dedup_candidates(), merge_candidates(),
detect_self_match(), page_of(), snippet_html(), snippet_plain().

No PyQt. No direct sqlite3.connect. All data access via SearchExecutor
adapter or the existing shared services (visual_similarity_service,
fjms_service).
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Optional, Protocol, runtime_checkable

from genizah_core import (
    _parse_line_break_query,
    _query_has_brackets,
    _strip_brackets,
)
```

**Note:** `typing.Protocol` has NO existing usage in this codebase — `shared/joins_lab.py` introduces it for the first time. The import is `from typing import Protocol, runtime_checkable` (stdlib, no install needed, Python 3.8+).

---

#### Frozen dataclass pattern

Source: `shared/fist_cudl_bridge.py` lines 80-97 — the ONLY `@dataclass(frozen=True)` in `shared/`:

```python
@dataclass(frozen=True)
class InventoryRecord:
    """A single FIST inventory row resolved by the bridge.

    Fields:
      inventory_id    -- FIST dbo_Inventory.InventoryId (int, opaque)
      fist_shelfmark  -- FIST dbo_Inventory.Shelfmark verbatim
      has_alma        -- True iff dbo_InventoryAlma row exists
      title_heb       -- dbo_UnitCatalogRec.Title or None
      genizah_title   -- dbo_UnitCatalogRec.GenizahTitleText or None
    """
    inventory_id: int
    fist_shelfmark: str
    has_alma: bool
    title_heb: Optional[str] = None
    genizah_title: Optional[str] = None
```

**Apply to** `BuilderRow`, `SideQuery`, `Candidate`, and the merge result in `shared/joins_lab.py`:
- All required fields first (no default), then optional fields with defaults (`= None`, `= False`, `= 0`, `= field(default_factory=...)`).
- One-paragraph docstring + per-field inline comments (`--` style matches bridge convention).
- `frozen=True` means no mutation after construction — tuples for collections (e.g., `rows: tuple[BuilderRow, ...]` in `SideQuery`).

**D-09 (additive-extensible) implementation pattern** — copy from `shared/refinement.py:49-54` (`from_dict` ignoring unknown keys):

```python
@classmethod
def from_dict(cls, d: dict) -> RefinementStep:
    """Construct from dict, ignoring unknown keys and runtime fields."""
    _skip = {'_result_uids'}
    known = {k: v for k, v in d.items() if k in cls.__dataclass_fields__ and k not in _skip}
    return cls(**known)
```

---

#### SearchExecutor Protocol pattern

`typing.Protocol` does not yet exist in this codebase. The new module introduces it.
House style for the closest thing — an injected mock with the same method signatures — is `MockSearcher` in `tests/test_refinement.py` lines 21-32:

```python
class MockSearcher:
    """Mock search engine that returns predetermined results per call."""
    def __init__(self, results_by_call):
        self.results_by_call = results_by_call  # list of list[dict]
        self.call_count = 0
        self.calls = []

    def execute_search(self, query, mode, gap, **kwargs):
        self.calls.append((query, mode, gap, kwargs))
        result = self.results_by_call[self.call_count] if self.call_count < len(self.results_by_call) else []
        self.call_count += 1
        return result
```

**The production Protocol** (new pattern, no existing analog) follows the RESEARCH.md verified signature. Use `@runtime_checkable` so tests can do `isinstance(fake, SearchExecutor)` for sanity checks:

```python
@runtime_checkable
class SearchExecutor(Protocol):
    def execute_search(
        self,
        query_str: str,
        mode: str,
        gap: int,
        progress_callback=None,
        exclude_words=None,
        responsa_options: dict | None = None,
        restrict_sys_ids: set | None = None,
        text_position: str | None = None,
        corpus_scope: str = "genizah",
    ) -> list[dict]: ...

    def get_browse_page(
        self,
        sys_id: str,
        p_num: int | None = None,
        next_prev: int = 0,
        absolute_index: int | None = None,
        allow_cross: bool = False,
        volume_ie: str | None = None,
    ) -> dict | None: ...

    def get_meta_for_id(self, sys_id: str) -> tuple[str, str]: ...
    def get_library_for_id(self, sys_id: str) -> str: ...
```

---

#### Pure-function core pattern

Source: `shared/refinement.py` lines 62-80 (module-level pure functions with type hints, docstrings, `list[T]` style):

```python
def needs_mode_labels(chain: list[RefinementStep]) -> bool:
    """Return True if chain has steps with different modes (show mode badges)."""
    if len(chain) < 2:
        return False
    return len(set(s.mode for s in chain)) > 1


def compute_effective_restrict(
    filter_restrict: set | None,
    refinement_restrict: set | None,
) -> set | None:
    """Merge filter and refinement restrict sets.

    Contract (explicit None vs empty-set semantics):
    - Both None -> None (no restriction at all)
    - One None, one set -> return the set (could be empty)
    - Both sets -> return intersection (could be empty)
    """
    ...
```

**Apply to** all six logic units in `shared/joins_lab.py`: `compose()`, `resolve_other_side_pages()`, `cross_side_membership()`, `dedup_candidates()`, `merge_candidates()`, `detect_self_match()`, `page_of()`, `snippet_html()`, `snippet_plain()`. Each function: module-level, no `self`, full type hints, docstring with contract notes, returns typed value.

---

#### Module-level singleton pattern

Source: `shared/visual_similarity_service.py` lines 306-327:

```python
# ── Singleton ─────────────────────────────────────────────────────

_vs_instance = None
_vs_lock = threading.Lock()


def get_vs_service(thread_safe: bool = True) -> VisualSimilarityService:
    """Get or create the default VisualSimilarityService singleton."""
    global _vs_instance
    with _vs_lock:
        if _vs_instance is None:
            _vs_instance = VisualSimilarityService(thread_safe=thread_safe)
        return _vs_instance


def reset_vs_service():
    """Reset the singleton VisualSimilarityService instance."""
    global _vs_instance
    with _vs_lock:
        if _vs_instance is not None:
            _vs_instance.close()
        _vs_instance = None
```

**Note for `shared/joins_lab.py`:** The module has no stateful service class that requires a singleton — all logic units are pure functions or take an injected `SearchExecutor`. There is no `get_joins_lab_service()` analog needed. The singleton pattern does NOT apply here. Reference only for the import guard that the module uses `get_vs_service()` / `get_fjms_service()` (never instantiates sqlite3 itself).

---

#### Module-level docstring convention

Source: `shared/document_service.py` lines 1-28 (complete function-listing docstring):

```python
# -*- coding: utf-8 -*-
"""
Document Service for PGP document-fragment relationships.

This module provides PgpService class and module-level functions for accessing
PGP document data from the local pgp.db SQLite sidecar:
- get_document_for_fragment(sys_id) -> dict | None
- get_fragments_for_document(pgpid) -> list[dict]
...

All functions handle errors gracefully, returning None or empty lists
rather than raising exceptions. When the sidecar database is missing,
the service degrades gracefully (is_available() returns False).
...
"""
```

**Apply to `shared/joins_lab.py`:** List all six units + the Protocol + the dataclasses. Include the constraint: "No PyQt. No direct sqlite3.connect." This matches the static import guard (SC#6) as documentation.

---

#### Error handling convention

Source: `shared/visual_similarity_service.py` lines 108-128 (`get_suggestions`):

```python
def get_suggestions(self, sys_id: str, limit: int = 200) -> list:
    if not self._conn:
        return []
    try:
        alma_id = int(sys_id)
    except (ValueError, TypeError):
        return []

    try:
        cursor = self._conn.execute(...)
        rows = cursor.fetchall()
        return [...]
    except Exception as e:
        logger.error(f"get_suggestions error for {sys_id}: {e}")
        return []
```

**Apply to `shared/joins_lab.py`:** Pure functions with no I/O do not need try/except. The only place where errors should be swallowed-with-log is any function that calls `execute_search` or `get_browse_page` via the `SearchExecutor` — guard with try/except and return `[]` / `False` on failure. Compose / dedup / merge / snippet are pure and should raise on contract violations (e.g., `ValueError` for invalid `page_position`).

---

### `tests/test_joins_lab.py` (test, unit coverage)

**Primary analog:** `tests/test_refinement.py` (local MockSearcher + class-based TestX + Hebrew strings + imports from `shared.*`)

**Secondary analog:** `tests/test_bracket_search.py` (Hebrew RTL strings in fixtures, class-based `TestX`, import from `genizah_core`)

---

#### File header and imports pattern

Source: `tests/test_refinement.py` lines 1-14:

```python
# -*- coding: utf-8 -*-
"""Tests for shared/refinement.py — RefinementStep dataclass and chain helpers."""

from shared.refinement import (
    RefinementStep,
    compute_effective_restrict,
    needs_mode_labels,
    truncate_chain,
    replay_chain,
    scope_signature,
    enrich_snippet_with_chain_terms,
    compute_all_terms_filter,
)
```

**Apply to `tests/test_joins_lab.py`:** Single `# -*- coding: utf-8 -*-` header, one-line docstring, explicit named imports from `shared.joins_lab`. No `import *`. No conftest.py — the project has none.

---

#### Local mock/fake class pattern

Source: `tests/test_refinement.py` lines 20-32 (`MockSearcher`):

```python
class MockSearcher:
    """Mock search engine that returns predetermined results per call."""
    def __init__(self, results_by_call):
        self.results_by_call = results_by_call  # list of list[dict]
        self.call_count = 0
        self.calls = []

    def execute_search(self, query, mode, gap, **kwargs):
        self.calls.append((query, mode, gap, kwargs))
        result = self.results_by_call[self.call_count] if self.call_count < len(self.results_by_call) else []
        self.call_count += 1
        return result
```

Source: `tests/test_fjms_joins_integration.py` lines 90-103 (`mock_meta_mgr` — a `MagicMock` with real method bound):

```python
@pytest.fixture
def mock_meta_mgr():
    """Create a mock metadata manager that resolves sys_ids to shelfmarks."""
    meta = MagicMock()

    def get_meta_for_id(sys_id):
        mapping = {
            "SYS001": ("T-S 12.100", "Title A"),
            "SYS002": ("T-S 12.200", "Title B"),
        }
        return mapping.get(sys_id, ("Unknown", ""))

    meta.get_meta_for_id = get_meta_for_id
    return meta
```

**Apply to `tests/test_joins_lab.py`:** Define `FakeSearchExecutor` as a plain class (NOT `MagicMock`) at module level, above the test classes. Follow the `MockSearcher` pattern exactly: constructor takes canned data, methods record calls in `self.calls`, return canned data keyed by lookup. The exact design from RESEARCH.md matches the house pattern:

```python
class FakeSearchExecutor:
    """Test double for SearchExecutor Protocol — canned results, call recording."""

    def __init__(self, results=None, browse_pages=None, meta=None, library=None):
        self._results = results or []
        self._browse_pages = browse_pages or {}  # (sys_id, p_num) -> dict
        self._meta = meta or {}                  # sys_id -> (shelfmark, title)
        self._library = library or {}            # sys_id -> library_code
        self.calls = []                          # list for assertions

    def execute_search(self, query_str, mode, gap, **kwargs) -> list[dict]:
        self.calls.append(("execute_search", query_str, kwargs))
        return self._results

    def get_browse_page(self, sys_id, p_num=None, **kwargs) -> dict | None:
        self.calls.append(("get_browse_page", sys_id, p_num))
        return self._browse_pages.get((sys_id, p_num))

    def get_meta_for_id(self, sys_id) -> tuple[str, str]:
        return self._meta.get(sys_id, ("Unknown", ""))

    def get_library_for_id(self, sys_id) -> str:
        return self._library.get(sys_id, "")
```

---

#### Module-level helper function pattern

Source: `tests/test_refinement.py` lines 34-36:

```python
def _make_results(*sys_ids):
    """Helper: create mock result list from sys_id strings."""
    return [{'display': {'id': sid}, 'uid': f'uid_{sid}'} for sid in sys_ids]
```

**Apply to `tests/test_joins_lab.py`:** Define `_make_result(sys_id, page, ...)` as a module-level helper that constructs a realistic result dict matching the verified shape (RESEARCH.md `Result Dict Shape` section). Hebrew shelfmarks in fixture data are acceptable and encouraged (see TestEnrichSnippet pattern below).

---

#### Hebrew string fixture pattern

Source: `tests/test_refinement.py` lines 265-270 (`TestEnrichSnippet`):

```python
class TestEnrichSnippet:
    def test_enrich_marks_earlier_terms(self):
        chain = [RefinementStep('שלום', 'exact'), RefinementStep('רמבם', 'exact')]
        snippet = 'בשם שלום ורמבם *כמא* בתורה'
        result = enrich_snippet_with_chain_terms(snippet, chain, 'כמא')
        assert '*שלום*' in result
```

Source: `tests/test_bracket_search.py` lines 59-60:

```python
def test_hebrew_with_bracket(self):
    assert _query_has_brackets("]הנתשנ") is True
```

**Apply to `tests/test_joins_lab.py`:** Use literal Hebrew strings (UTF-8 source file, `# -*- coding: utf-8 -*-` header ensures this). For `TestCompose`, use a real Hebrew term: `BuilderRow(term="שהדותא", line_start=True)` to verify the RTL leading-`|` orientation from RESEARCH.md Pitfall 1. For `TestSnippet`, use a Hebrew text block with the target term somewhere in the middle to verify centering.

---

#### Class-based test grouping pattern

Source: `tests/test_refinement.py` lines 43-84 and throughout:

```python
class TestRefinementStepCreation:
    def test_refinement_step_create(self):
        step = RefinementStep('rambam', 'exact')
        assert step.query == 'rambam'
        assert step.mode == 'exact'
        ...

class TestComputeEffectiveRestrict:
    def test_effective_restrict_both_none(self):
        ...
```

Source: `tests/test_bracket_search.py` lines 17-76 (class per logical group):

```python
class TestAddBracketVariants:
    """Test _add_bracket_variants helper."""

class TestQueryHasBrackets:
    """Test _query_has_brackets helper."""
```

**Apply to `tests/test_joins_lab.py`:** One class per SC# / logical unit:

```
TestCompose          — SC#1: compose() round-trip, RTL, page_position
TestResolveOtherSide — SC#2 pure sub-function
TestCrossSide        — SC#2: AND/OR membership with FakeSearchExecutor
TestDedup            — SC#3: dedup_candidates
TestMerge            — SC#4: merge_candidates provenance ordering
TestSelfMatch        — SC#5: detect_self_match
TestPageOf           — SC#5: page_of helper
TestSnippet          — SC#5: snippet_html / snippet_plain
TestStaticImport     — SC#6: no PyQt, no fist_data
```

No `pytest.fixture` for the fake — just construct `FakeSearchExecutor(...)` inline per test. This matches `test_refinement.py`'s inline `MockSearcher([results1, results2])` pattern.

---

#### Static import guard test pattern

Source: `tests/test_no_raw_storage_access.py` (AST-based guard — closest analog to SC#6):

The project uses AST-inspection tests for static code-property guards. SC#6 should follow the same approach:

```python
class TestStaticImport:
    def test_no_pyqt_import(self):
        """shared/joins_lab.py must not import any Qt binding."""
        import ast, pathlib
        src = pathlib.Path("shared/joins_lab.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                names = [a.name for a in node.names] if isinstance(node, ast.Import) else [node.module or ""]
                for name in names:
                    assert not (name or "").startswith(("PyQt6", "PyQt5", "PySide6")), \
                        f"Qt import found: {name}"

    def test_no_fist_data_direct_connect(self):
        """shared/joins_lab.py must not sqlite3.connect to fist_data paths."""
        import pathlib
        src = pathlib.Path("shared/joins_lab.py").read_text(encoding="utf-8")
        assert "fist_data" not in src, "Direct fist_data path reference found"
```

---

## Shared Patterns

### `typing.Protocol` — first use in codebase
**Source:** No existing analog. New pattern introduced by Phase 106.
**Apply to:** `SearchExecutor` class in `shared/joins_lab.py`.
**Convention decision:** Use `@runtime_checkable` so tests can use `isinstance()` checks.
**Warning:** Do NOT add `@runtime_checkable` to a Protocol that has non-method members — it only checks method presence, not signatures. For `SearchExecutor` (methods only), it is safe.

### Frozen dataclasses with default fields
**Source:** `shared/fist_cudl_bridge.py:80-97` (`InventoryRecord`)
**Apply to:** `BuilderRow`, `SideQuery`, `Candidate` in `shared/joins_lab.py`
**Key rule:** Required fields (no default) must come before optional fields (with default) — Python dataclass inheritance ordering.

### Module-level pure helpers for `genizah_core` imports
**Source:** `tests/test_bracket_search.py` lines 10-14 (imports `_query_has_brackets`, `_strip_brackets` directly from `genizah_core`):
```python
from genizah_core import (
    _add_bracket_variants,
    _query_has_brackets,
    _strip_brackets,
)
```
**Apply to:** Both `shared/joins_lab.py` (production use) and `tests/test_joins_lab.py` (direct import in SC#1 round-trip tests, i.e., `from genizah_core import _parse_line_break_query`). The project has precedent for importing private (`_`-prefixed) module-level functions from `genizah_core` directly in tests and shared modules.

### `# -*- coding: utf-8 -*-` encoding header
**Source:** Every `shared/*.py` and `tests/test_*.py` file in this codebase.
**Apply to:** Both `shared/joins_lab.py` and `tests/test_joins_lab.py`. Required for Hebrew string literals.

### `from __future__ import annotations`
**Source:** `shared/refinement.py:17`, `shared/fist_cudl_bridge.py:34`, `shared/parallels_service.py:36`
**Apply to:** `shared/joins_lab.py` — enables `list[X]` and `X | Y` union syntax on Python 3.9 / 3.10 without runtime cost.

### Graceful degradation for missing services
**Source:** `shared/visual_similarity_service.py:107-109`:
```python
def get_suggestions(self, sys_id: str, limit: int = 200) -> list:
    if not self._conn:
        return []
```
**Apply to:** Any `shared/joins_lab.py` function that calls `get_vs_service()` or `get_fjms_service()`. Check `svc.is_available()` before querying; return `[]` or `None` if unavailable. Do NOT raise.

---

## No Analog Found

| File | Role | Data Flow | Reason |
|------|------|-----------|--------|
| `typing.Protocol` usage in `shared/joins_lab.py` | structural type | — | No existing `Protocol` usage anywhere in the codebase; `shared/joins_lab.py` introduces it for the first time |

---

## Metadata

**Analog search scope:** `shared/*.py` (35 files), `tests/test_*.py` (~100+ files)
**Key files read:**
- `shared/visual_similarity_service.py` — singleton pattern, module structure, graceful degradation
- `shared/puzzle_model.py` — dataclass convention (non-frozen; contrast with frozen)
- `shared/refinement.py` — pure-function module shape, dataclass with defaults, module-level helpers
- `shared/fist_cudl_bridge.py` — ONLY `@dataclass(frozen=True)` in `shared/` (lines 80-97)
- `shared/parallels_service.py` — `from __future__ import annotations`, dataclass header style
- `shared/document_service.py` — full function-listing docstring convention
- `tests/test_refinement.py` — MockSearcher pattern, class-based tests, Hebrew strings, module-level helpers
- `tests/test_fjms_joins_integration.py` — mock_meta_mgr MagicMock pattern, multi-class test file
- `tests/test_visual_similarity.py` — fixture + function-level test style (contrast with class-based)
- `tests/test_bracket_search.py` — Hebrew strings, direct `genizah_core._*` import, class-based
- `tests/test_puzzle_service.py` — class-based, `TestSchemaCreation` / `TestCRUD` grouping
**Pattern extraction date:** 2026-06-03
