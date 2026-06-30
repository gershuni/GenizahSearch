# Phase 125: Core Engines — Pattern Map

**Mapped:** 2026-06-26
**Files analyzed:** 7 (3 new shared modules + 4 modified files)
**Analogs found:** 7 / 7

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `shared/lab_settings.py` | config/service | CRUD (JSON persistence) | `shared/codicological.py` (stdlib-only, no tantivy guard) | role-match |
| `shared/lab_engine.py` | service | request-response + event-driven | `shared/indexer.py` (tantivy guard + `_tr()` + DI constructor) | exact |
| `shared/search_engine.py` | service | request-response + CRUD + streaming | `shared/indexer.py` + `shared/metadata_manager.py` (tantivy guard + DI + pre-cluster helpers) | exact |
| `genizah_core.py` (facade shims) | config | — | existing Phase 122–124 shim blocks at lines 61–114 | exact |
| `tests/test_no_back_edges_core.py` | test | — | existing parametrized identity test pattern (lines 148–531) | exact |
| `tests/test_seed011_composition_dedup.py` | test | — | `tests/test_local_post_dedup_merge.py` (structural AST/source scan pattern) | role-match |
| `genizah_core.py` (SEED-011 dedup) | service | request-response | existing `search_composition_logic` + `lab_composition_search` loops at lines 4906–5070 / 1549–1735 | exact (in-place refactor) |

---

## Pattern Assignments

### `shared/lab_settings.py` (config, CRUD)

**Analog:** `shared/codicological.py` (lines 1–17) and `shared/lists_manager.py` (lines 1–36)

**File header pattern** (`shared/codicological.py` lines 1–17):
```python
# -*- coding: utf-8 -*-
"""Codicological unit management for Oxford/Neubauer manuscripts.

Phase 123: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import CodicologicalManager`` callers continue working.
"""

import json
import logging
import os
import re

from shared.config import Config

LOGGER = logging.getLogger("genizah." + __name__)
```

**Note for LabSettings:** No `_tr()` helper needed (LabSettings has NO user-visible strings that need translation). No tantivy guard needed (LabSettings is stdlib-only: `json`, `os`). The `logging.getLogger("genizah." + __name__)` line IS required.

**LabSettings class body** (`genizah_core.py` lines 491–629 — copy verbatim). The class begins:
```python
class LabSettings:
    """Manages configuration for the Lab Mode, including scoring weights."""
    def __init__(self):
        self.custom_variants = {}
        self.candidate_limit = 5000
        ...
        self.load()

    def load(self):
        if os.path.exists(Config.LAB_CONFIG_FILE):
            try:
                ...
            except Exception as e:
                logging.getLogger(__name__).warning(
                    'Failed to load lab config from %s: %s', Config.LAB_CONFIG_FILE, e
                )

    def save(self):
        try:
            ...
        except Exception as e:
            logging.getLogger(__name__).warning(
                'Failed to save lab config to %s: %s', Config.LAB_CONFIG_FILE, e
            )
```

**IMPORTANT:** The existing `LabSettings.load()` / `save()` use `logging.getLogger(__name__)` (bare `__name__`). After extraction the module-level `LOGGER = logging.getLogger("genizah." + __name__)` exists, so update the two method-level calls to use `LOGGER` instead, for consistency.

---

### `shared/lab_engine.py` (service, request-response + event-driven)

**Analog:** `shared/indexer.py` (lines 1–60) — tantivy guard + `_tr()` + module-level LOGGER + DI constructor.

**File header + tantivy guard pattern** (`shared/indexer.py` lines 1–35):
```python
# -*- coding: utf-8 -*-
"""Tantivy index construction and browse-map assembly.

Phase 124: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import Indexer`` callers continue working.
"""

import logging
import os
...

try:
    import tantivy
except ImportError:
    # GUARD-02 (zero behavior change): at base, genizah_core guarded its
    # `import tantivy` and raised this friendly message. The Phase 124 facade
    # shim `from shared.indexer import Indexer` now executes BEFORE genizah_core's
    # own guard, so this module must raise the identical ImportError — otherwise a
    # missing-tantivy install surfaces a raw ModuleNotFoundError. Plain (untranslated)
    # to match the first guard that fired at base genizah_core.py.
    raise ImportError("Tantivy library missing. Please install it.")

LOGGER = logging.getLogger("genizah." + __name__)
```

**`_tr()` helper pattern** (`shared/indexer.py` lines 38–48 and `shared/lists_manager.py` lines 25–35):
```python
def _tr(text: str) -> str:
    """Translate text if current language is Hebrew.

    Mirrors genizah_core.tr() — lazy import of CURRENT_LANG inside the
    function body so we always see the live value (Pitfall 2 of Phase 123).
    GUARD-01-safe: the import is function-body-only, not module-level.
    """
    from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text
```

**LabEngine imports needed:** `from shared.lab_settings import LabSettings` (no back-edge to genizah_core at module level). Any lazy imports to genizah_core helpers (e.g., `text_to_fingerprint`, `_is_phrase_statistically_weak`) go inside method bodies as GUARD-01-safe lazy imports, or those helpers move into `shared/search_engine.py` and LabEngine imports them from there.

**LabEngine class body:** `genizah_core.py` lines 634–~2046 (copy intact). Constructor starts:
```python
class LabEngine:
    LAB_FINGERPRINT_FIELD = "fingerprint"
    NGRAM_SIZE = 3

    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        self.settings = LabSettings()
        self.lab_index = None
        self.lab_searcher = None
        self.lab_index_needs_rebuild = False
        self.dynamic_rank_map = None
        # CR-02 FIX: LOCAL LAB side-index attributes — mirror SearchEngine so
        # LabEngine.lab_composition_search can query LOCAL LAB hits in LAB mode.
        ...
```

**CR-01/CR-02 preservation:** Do NOT add `self._my_library_tab_ref = None` to LabEngine.__init__. The existing `getattr(self, "_my_library_tab_ref", None)` guards at lines 1678 and ~1408 are intentional — LabEngine instances never have this attribute in production (it lives only on SearchEngine). Keep the guards verbatim.

---

### `shared/search_engine.py` (service, request-response + CRUD + streaming)

**Analog:** `shared/indexer.py` (lines 1–60) + `shared/metadata_manager.py` (lines 1–35)

**File header** (follow `shared/indexer.py` docstring style):
```python
# -*- coding: utf-8 -*-
"""Full-text search execution, browse utilities, and LOCAL index management.

Phase 125: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import SearchEngine`` callers continue working.
"""
```

**Tantivy guard** (identical string — enforced by `tests/test_missing_tantivy.py`):
```python
try:
    import tantivy
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")
```

**LOGGER line** (`shared/metadata_manager.py` line 31 / `shared/indexer.py` line 35):
```python
LOGGER = logging.getLogger("genizah." + __name__)
```

**`_tr()` helper:** Same pattern as `shared/indexer.py` lines 38–48. Required because SearchEngine methods call `tr()` for user-visible status messages.

**Pre-cluster that MUST precede the class body** (all from `genizah_core.py` — copy in this order):

1. **`_LAST_RESPONSA_DOWNGRADE` cluster** (genizah_core.py lines 126–183):
```python
_LAST_RESPONSA_DOWNGRADE = threading.local()          # line 126
_LAST_RESPONSA_DOWNGRADE_META = threading.local()     # line 131
def _set_last_responsa_downgrade(message: str) -> None: ...   # line 134
def _consume_last_responsa_downgrade() -> Optional[str]: ...  # line 143
def _set_last_responsa_downgrade_meta(meta: dict) -> None: ...  # line 160
def _consume_last_responsa_downgrade_meta() -> Optional[dict]: ...  # line 171
```

2. **`RRF_K` constant** (genizah_core.py line 2453):
```python
RRF_K = 60
```

3. **RESPONSA REGEX HELPERS block** (genizah_core.py lines 2252–2358):
```python
def _make_flex_spacing_pattern(term: str) -> str: ...   # line 2256
def _build_wildcard_regex(component: dict) -> str: ...  # line 2270
def _add_bracket_variants(term: str) -> list: ...       # line 2321
def _query_has_brackets(query_str: str) -> bool: ...    # line 2346
def _strip_brackets(text: str) -> str: ...              # line 2356
```
Note: `_SOFIT_TO_NORMAL` is referenced inside `_build_wildcard_regex` — it lives in `shared/responsa.py`. Import it: `from shared.responsa import _SOFIT_TO_NORMAL`.

4. **SEED-006 compat gate helpers** (genizah_core.py lines 2361–2444):
```python
def _index_has_field(index, field_name: str) -> bool: ...          # line 2361
def content_search_staleness_messages(genizah_present, local_present): ...  # line 2385
MARK_TOLERANT_INSERTER = '[̀-ͯ"'׳״‘’]*'  # line 2428
def make_mark_tolerant_pattern(escaped_term: str) -> str: ...      # line 2431
```

5. **`_count_unique_chunks`** (genizah_core.py line 466):
```python
def _count_unique_chunks(chunk_hits):
    """Count distinct source-chunk contents from a chunk_hits list. ..."""
    return len({
        hit[1]
        for hit in (chunk_hits or ())
        if isinstance(hit, (tuple, list)) and len(hit) > 1 and hit[1]
    })
```

**SearchEngine class body** (genizah_core.py lines 2456–~5945 — copy intact). Constructor starts:
```python
class SearchEngine:
    """Run searches, build queries, and provide browsing utilities."""
    _shared_browse_map = None        # class-level singleton — moves with the class
    _browse_map_lock = threading.Lock()

    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        ...
        self._my_library_tab_ref: weakref.ref | None = None
        ...
```

**CORE-13 DI interface — `attach_my_library_tab`** (genizah_core.py lines 2493–2500):
```python
def attach_my_library_tab(self, tab) -> None:
    """Phase 97 R-01: attach a weakref to the MyLibraryTab for is_searchable gate.
    ...
    """
    self._my_library_tab_ref = weakref.ref(tab)
```
The `tab` argument is duck-typed: only `getattr(tab, "is_searchable", True)` is ever called on the resolved weakref. No Protocol ABC needed. No `shared/` → desktop import.

**BrowseMap class-level cache** (genizah_core.py lines 3166–3167, inside SearchEngine class):
```python
_shared_browse_map = None
_browse_map_lock = threading.Lock()
```
These class-level attrs move with the class body — zero migration code needed. The facade identity (`genizah_core.SearchEngine is shared.search_engine.SearchEngine`) means any code referencing `SearchEngine._shared_browse_map` continues to work.

---

### `genizah_core.py` — Phase 125 facade shim block

**Analog:** Existing Phase 122–124 shim block at genizah_core.py lines 61–114

**Pattern to copy** (lines 101–114 — the Phase 124 shims are the most recent reference):
```python
# Phase 124: metadata_manager extracted — permanent compat facade (v8.3.0)
from shared.metadata_manager import (  # noqa: F401
    _NLI_CACHE_MAX_ENTRIES,
    _BoundedLRUCache,
    MARC_FUTURE_TIMEOUT,
    NLI_IIIF_FUTURE_TIMEOUT,
    EXTERNAL_IIIF_HTTP_TIMEOUT,
    MetadataManager,
    _get_crossref_service,
    _get_fjms_service,
    _parse_cudl_label,
)
# Phase 124: indexer extracted — permanent compat facade (v8.3.0)
from shared.indexer import Indexer  # noqa: F401
```

**Phase 125 shims to add** (after line 114, or replacing the inline definitions that move):

```python
# Phase 125: lab_settings extracted — permanent compat facade (v8.3.0)
from shared.lab_settings import LabSettings  # noqa: F401

# Phase 125: lab_engine extracted — permanent compat facade (v8.3.0)
from shared.lab_engine import LabEngine  # noqa: F401

# Phase 125: search_engine extracted — permanent compat facade (v8.3.0)
from shared.search_engine import (  # noqa: F401
    # SearchEngine class
    SearchEngine,
    # Pre-cluster: RRF + Responsa regex helpers
    RRF_K,
    _make_flex_spacing_pattern,
    _build_wildcard_regex,
    _add_bracket_variants,
    _query_has_brackets,
    _strip_brackets,
    # SEED-006 compat gate helpers
    _index_has_field,
    content_search_staleness_messages,
    MARK_TOLERANT_INSERTER,
    make_mark_tolerant_pattern,
    # Composition helper
    _count_unique_chunks,
    # Responsa downgrade thread-local channel (6 names)
    _LAST_RESPONSA_DOWNGRADE,
    _LAST_RESPONSA_DOWNGRADE_META,
    _set_last_responsa_downgrade,
    _consume_last_responsa_downgrade,
    _set_last_responsa_downgrade_meta,
    _consume_last_responsa_downgrade_meta,
)
```

**TIMING:** The `_LAST_RESPONSA_DOWNGRADE` cluster currently lives at genizah_core.py lines 126–183 (before the class). When SearchEngine moves (125d), these inline definitions are REPLACED by the facade import. The `RRF_K = 60` line at 2453 and the RESPONSA REGEX HELPERS block at 2252–2358 are similarly replaced.

---

### `tests/test_no_back_edges_core.py` — registry extension to 13 entries

**Analog:** Same file, `EXTRACTED_MODULES` list at lines 31–42.

**Current state** (10 entries, Phase 124):
```python
EXTRACTED_MODULES = [
    "shared/config.py",
    "shared/browse_map_utils.py",
    "shared/text_normalize.py",
    "shared/variants.py",
    "shared/responsa.py",
    "shared/codicological.py",
    "shared/joins_manager.py",
    "shared/lists_manager.py",
    "shared/metadata_manager.py",
    "shared/indexer.py",             # Phase 124
]
```

**Phase 125 additions** (append after `"shared/indexer.py"`):
```python
    "shared/lab_settings.py",        # Phase 125b
    "shared/lab_engine.py",          # Phase 125c
    "shared/search_engine.py",       # Phase 125d
```

**Identity tests to add** — follow the exact same pattern as `test_metadata_manager_identity` (lines 465–503) and `test_indexer_identity` (lines 510–531):

```python
# Phase 125: lab_settings (CORE-11)
def test_lab_settings_identity():
    """CORE-11: genizah_core.LabSettings is the same class as shared.lab_settings.LabSettings."""
    import shared.lab_settings
    import genizah_core
    assert shared.lab_settings.LabSettings is genizah_core.LabSettings, (
        "genizah_core.LabSettings is not the same object as shared.lab_settings.LabSettings. "
        "The re-export shim must be: from shared.lab_settings import LabSettings  # noqa: F401"
    )

def test_lab_settings_standalone_import():
    """CORE-11 smoke: shared.lab_settings can be imported and LabSettings instantiates."""
    import shared.lab_settings
    assert hasattr(shared.lab_settings, 'LabSettings')
    # Smoke: LabSettings instantiates (loads config or uses defaults)
    settings = shared.lab_settings.LabSettings()
    assert settings is not None
    assert hasattr(settings, 'candidate_limit')

# Phase 125: lab_engine (CORE-12)
def test_lab_engine_identity():
    """CORE-12: genizah_core.LabEngine is the same class as shared.lab_engine.LabEngine."""
    import shared.lab_engine
    import genizah_core
    assert shared.lab_engine.LabEngine is genizah_core.LabEngine, (
        "genizah_core.LabEngine is not the same object as shared.lab_engine.LabEngine. "
        "The re-export shim must be: from shared.lab_engine import LabEngine  # noqa: F401"
    )

# Phase 125: search_engine (CORE-10)
def test_search_engine_identity():
    """CORE-10: genizah_core.SearchEngine is the same class as shared.search_engine.SearchEngine."""
    import shared.search_engine
    import genizah_core
    assert shared.search_engine.SearchEngine is genizah_core.SearchEngine, (
        "genizah_core.SearchEngine is not the same object as shared.search_engine.SearchEngine. "
        "The re-export shim must be: from shared.search_engine import SearchEngine  # noqa: F401"
    )
    # Also verify pre-cluster names
    assert shared.search_engine.RRF_K is genizah_core.RRF_K
    assert shared.search_engine._count_unique_chunks is genizah_core._count_unique_chunks
    assert shared.search_engine._set_last_responsa_downgrade is genizah_core._set_last_responsa_downgrade
    assert shared.search_engine.content_search_staleness_messages is genizah_core.content_search_staleness_messages
    assert shared.search_engine._index_has_field is genizah_core._index_has_field
```

---

### `tests/test_seed011_composition_dedup.py` (new test)

**Analog:** `tests/test_local_post_dedup_merge.py` — structural source-scan test pattern.

**Pattern** — mock `build_tantivy_query` / `build_regex_pattern` and count calls per outer-loop iteration:

```python
# From test_local_post_dedup_merge.py style — use a call-counter mock
from unittest.mock import patch, MagicMock

def test_search_composition_logic_no_double_prep():
    """SEED-011: build_tantivy_query called once per chunk-index, not once per index."""
    # Patch build_tantivy_query + build_regex_pattern on SearchEngine
    # Supply a 3-chunk input, corpus_scope='all' (both Genizah + LOCAL loops active)
    # Assert build_tantivy_query call count == 2 * n_chunks
    # (one genizah-flavor + one local-flavor per chunk, not 2 * n_chunks * 2)
    ...

def test_lab_composition_search_no_double_prep():
    """SEED-011: LAB chunk-plan precomputed once, not once per LAB index."""
    # Patch text_to_fingerprint and count calls vs chunks
    # Supply n chunks, corpus_scope='all' (both Genizah LAB + LOCAL LAB active)
    # Assert text_to_fingerprint called n times, not 2*n
    ...
```

---

### `genizah_core.py` — SEED-011 ChunkPlan refactor (PREP-01)

**Analog:** The existing double-prep loops at genizah_core.py lines 4906–5070 (`search_composition_logic`) and 1549–1735 (`lab_composition_search`) — in-place refactor, not a new file.

**ChunkPlan dataclass style** (follow `shared/responsa.py` lines 71–90 and `shared/joins_lab.py` lines 28–44):
```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class _ChunkPlan:
    """Per-chunk precomputed plan for composition search.

    Computed once in the outer chunk loop, shared by the Genizah and LOCAL
    index passes (SEED-011 double-prep dedup). The two query strings differ
    because the LOCAL pass applies diacritic folding (SEED-006 M1).
    """
    genizah_query_str: str          # build_tantivy_query(chunk, mode, content_search_field=_cs_field)
    local_query_str: str            # build_tantivy_query(folded_chunk, mode) — diacritic-folded
    compiled_regex: object          # compiled re.Pattern from build_regex_pattern(chunk, mode, 0)
    is_text_filtered: bool          # filter_text match pre-check
```

For the LAB fingerprint plan (SEED-011 Finding 2), a simpler named tuple or dataclass:
```python
@dataclass
class _LabChunkPlan:
    """Per-chunk fingerprint plan shared by Genizah LAB and LOCAL LAB passes."""
    fp_str: str
    fp_list: list
    needed_unique_fps: set
    core_query: str                 # " OR ".join([f'{target_field}:{t}' for t in fp_str.split()])
    # final_query_str is NOT shared — Genizah LAB adds source boost, LOCAL LAB uses core_query only
```

**Placement:** Define both dataclasses at module level in genizah_core.py (before LabEngine / SearchEngine class definitions, or inline at the top of the method — module-level is preferred for testability and matches project style).

After 125d (SearchEngine move), both dataclasses move to `shared/search_engine.py` pre-cluster and are shimmed in genizah_core facade.

---

## Shared Patterns

### Extraction Recipe (proven across Phases 122–124)

**Source:** `shared/indexer.py` (Phase 124 most recent extraction)
**Apply to:** All 3 new shared modules

The 7-step recipe (from RESEARCH.md "Architecture Patterns"):
1. Copy class body intact with correct `# -*- coding: utf-8 -*-` header (NO BOM)
2. Add tantivy guard if the module imports tantivy — IDENTICAL error string: `"Tantivy library missing. Please install it."`
3. `LOGGER = logging.getLogger("genizah." + __name__)` at module level
4. `_tr()` helper (lazy CURRENT_LANG) if any method uses `tr()`
5. Per-file `ruff check` after copying (catches F401 unused imports)
6. Add facade shim in genizah_core.py: `from shared.<module> import <Class>  # noqa: F401`
7. Identity test: `assert genizah_core.X is shared.X_module.X`

### Logging Pattern

**Source:** `shared/metadata_manager.py` line 31 / `shared/indexer.py` line 35 / `shared/lists_manager.py` line 22
**Apply to:** Every new module in Phase 125

```python
LOGGER = logging.getLogger("genizah." + __name__)
```

`__name__` will resolve to `shared.lab_settings`, `shared.lab_engine`, `shared.search_engine` — all prefixed with `"genizah."`.

### `_tr()` Lazy Helper

**Source:** `shared/indexer.py` lines 38–48 (verbatim copy)
**Apply to:** `shared/lab_engine.py`, `shared/search_engine.py` (if any method calls `tr()`)

```python
def _tr(text: str) -> str:
    """Translate text if current language is Hebrew.

    Mirrors genizah_core.tr() — lazy import of CURRENT_LANG inside the
    function body so we always see the live value (Pitfall 2 of Phase 123).
    GUARD-01-safe: the import is function-body-only, not module-level.
    """
    from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text
```

### GUARD-01 Identity Test Pattern

**Source:** `tests/test_no_back_edges_core.py` lines 465–531 (Phase 124 examples)
**Apply to:** New identity tests for LabSettings, LabEngine, SearchEngine

```python
def test_<module>_identity():
    """CORE-XX: genizah_core.<Name> is the same class as shared.<module>.<Name>."""
    import shared.<module>
    import genizah_core
    assert shared.<module>.<Name> is genizah_core.<Name>, (
        "genizah_core.<Name> is not the same object as shared.<module>.<Name>. "
        "The re-export shim must be: from shared.<module> import <Name>  # noqa: F401"
    )

def test_<module>_standalone_import():
    """CORE-XX smoke: shared.<module> can be imported and <Name> has expected API."""
    import shared.<module>
    assert hasattr(shared.<module>, '<Name>')
    # Smoke: instantiate / check key attrs
    ...
```

### Tantivy Guard Pattern

**Source:** `shared/indexer.py` lines 17–26
**Apply to:** `shared/lab_engine.py`, `shared/search_engine.py` (both import tantivy)

```python
try:
    import tantivy
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")
```

**Critical:** The error string must match EXACTLY — `tests/test_missing_tantivy.py` checks for this string. Do not vary the capitalization or punctuation.

### GUARD-03 Source-Scan Retarget Pattern

**Source:** `tests/test_local_post_dedup_merge.py` / `tests/test_audit_2026_06_23_guards.py` (current failing tests)
**Apply to:** 5 test files that must be retargeted when engine code moves

For each of the 5 GUARD-03 files, the retarget is:
- Tests reading `genizah_core.py` for `SearchEngine` methods → retarget to `shared/search_engine.py`
- Tests reading `genizah_core.py` for `LabEngine` methods → retarget to `shared/lab_engine.py`
- Tests using `encoding="utf-8"` → keep `"utf-8"` (after BOM fix, this works again; do NOT change to `"utf-8-sig"`)

The path-replacement pattern (from each test file's `read_text("genizah_core.py")`):
```python
# Before (current):
src = pathlib.Path("genizah_core.py").read_text(encoding="utf-8")

# After (post-extraction — example for SearchEngine method):
src = (pathlib.Path(__file__).parent.parent / "shared" / "search_engine.py").read_text(encoding="utf-8")
```

---

## No Analog Found

All files have close matches. No entries in this section.

---

## Key Extraction Hazards (for planner)

### Hazard A: BOM Fix Must Be First (Wave 0 / Commit 1)

**Location:** genizah_core.py byte offset 0–2
**Fix:** Strip `\xef\xbb\xbf` from the raw bytes. Do NOT change test files' `encoding=` args.
**Effect:** Turns all 7 currently-red tests green before any other code changes.

### Hazard B: `_LAST_RESPONSA_DOWNGRADE` cluster lives at module-level, NOT in SearchEngine

**Location:** genizah_core.py lines 126–183
**Move to:** `shared/search_engine.py` pre-cluster (before the class body)
**Facade:** All 6 names shimmed in genizah_core.py

### Hazard C: `_count_unique_chunks` lives before `LabEngine` (line 466) — used by BOTH engines

**Location:** genizah_core.py line 466
**Move to:** `shared/search_engine.py` pre-cluster
**Facade:** shimmed in genizah_core.py
**Note:** LabEngine's `lab_composition_search` uses it; since LabEngine will import from `shared.search_engine` (where `_count_unique_chunks` lives), this creates a dependency: `shared/lab_engine.py` → `shared/search_engine.py`. This is acceptable (no cycle).

### Hazard D: `_SOFIT_TO_NORMAL` referenced in `_build_wildcard_regex`

**Location:** genizah_core.py line 2299 (inside `_build_wildcard_regex`)
**Resolution:** `from shared.responsa import _SOFIT_TO_NORMAL` at module level in `shared/search_engine.py` — no cycle (responsa has no search_engine import).

### Hazard E: `shared/local_indexer.py` may lazy-import `_index_has_field` from genizah_core

**Action:** Grep `shared/local_indexer.py` for `_index_has_field`. If found, retarget to `from shared.search_engine import _index_has_field` in the SearchEngine extraction commit (125d).

### Hazard F: LabEngine extraction order — LabSettings first

**Reason:** `LabEngine.__init__` (genizah_core.py line 642) calls `self.settings = LabSettings()`. After extraction, `shared/lab_engine.py` must `from shared.lab_settings import LabSettings` — so LabSettings must exist first. Plan order: 125b (LabSettings) → 125c (LabEngine) → 125d (SearchEngine).

---

## Metadata

**Analog search scope:** `shared/`, `tests/`, `genizah_core.py`
**Files scanned:** 12 source files read; 8 grep searches
**Pattern extraction date:** 2026-06-26
