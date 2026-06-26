# Phase 124: Core Metadata & Index — Pattern Map

**Mapped:** 2026-06-26
**Files analyzed:** 5 (2 created, 3 modified)
**Analogs found:** 5 / 5

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `shared/metadata_manager.py` | service | request-response + CRUD | `shared/codicological.py` + `shared/joins_manager.py` | exact (same Phase 123 recipe) |
| `shared/indexer.py` | service + utility | batch + file-I/O | `shared/responsa.py` + `shared/lists_manager.py` | exact (same Phase 123 recipe, needs `_tr()`) |
| `genizah_core.py` (modify) | config/facade | — | current `genizah_core.py` shim block (lines 65-103) | exact (extend existing pattern) |
| `tests/test_no_back_edges_core.py` (modify) | test | — | existing identity/standalone tests in same file | exact |
| `tests/test_desktop_folio_navigation.py` (modify) | test | — | `genizah_core_source` fixture in same file (lines 46-51) | exact |

---

## Pattern Assignments

### `shared/metadata_manager.py` (service, request-response + CRUD)

**Primary analog:** `shared/codicological.py` (heavy `shared/` outbound deps, no `_tr()` needed)
**Secondary analog:** `shared/joins_manager.py` (module-level singletons + lazy service accessors)

**Module header pattern** — copy verbatim from `shared/codicological.py` lines 1-17:
```python
# -*- coding: utf-8 -*-
"""NLI metadata fetch, IIIF/MARC enrichment, and persistent caching.

Phase 124: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import MetadataManager`` callers continue working.
"""

import logging
import os
import re
import threading
import pickle
import json
import requests
import xml.etree.ElementTree as ET
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed

from shared.config import Config
from shared.nli_circuit_breaker import (
    is_open as _nli_circuit_is_open,
    record_failure as _nli_record_failure,
    record_success as _nli_record_success,
    NLI_CONNECT_TIMEOUT,
    NLI_IIIF_READ_TIMEOUT,
    NLI_MARC_READ_TIMEOUT,
)
from shared.codicological import CodicologicalManager
from shared.browse_map_utils import normalize_shelfmark

LOGGER = logging.getLogger("genizah." + __name__)
```

**Key rule:** `LOGGER = logging.getLogger("genizah." + __name__)` — NOT `logging.getLogger(__name__)`. Pitfall 7 from RESEARCH.md. `__name__` = `shared.metadata_manager`, so the full logger name becomes `genizah.shared.metadata_manager`, which propagates correctly through the `genizah` tree.
Source: `shared/codicological.py` line 17, `shared/joins_manager.py` line 22, `shared/lists_manager.py` line 22.

**No `_tr()` needed** — `MetadataManager` has no `tr()` calls (confirmed in RESEARCH Q1). Do not add `_tr()`.

**Module-level singletons + lazy accessor pattern** (from `genizah_core.py` lines 2278-2309 — copy verbatim):
```python
# ==============================================================================
#  NLI CROSSREF SIDECAR (lazy accessor for local image resolution)
# ==============================================================================
_nli_crossref_svc = None

def _get_crossref_service():
    """Lazy accessor for the NLI crossref sidecar service (desktop use)."""
    global _nli_crossref_svc
    if _nli_crossref_svc is None:
        try:
            from shared.nli_crossref_service import NliCrossrefService
            _nli_crossref_svc = NliCrossrefService(thread_safe=True)
        except Exception as e:
            LOGGER.warning('Failed to initialize NLI crossref service: %s', e)
    return _nli_crossref_svc


# ==============================================================================
#  FJMS SIDECAR (lazy accessor for bibliography/catalog enrichment)
# ==============================================================================
_fjms_svc = None

def _get_fjms_service():
    """Lazy accessor for the FJMS enrichment sidecar service."""
    global _fjms_svc
    if _fjms_svc is None:
        try:
            from shared.fjms_service import FjmsService
            _fjms_svc = FjmsService(thread_safe=True)
        except Exception as e:
            LOGGER.warning('Failed to initialize FJMS service: %s', e)
    return _fjms_svc
```
These are process-level singletons — they MUST live in `shared/metadata_manager.py` where the MetadataManager methods call them (RESEARCH Pitfall 6). Do not leave them in genizah_core.py.

**Full extraction block** — copy `genizah_core.py` lines 2100-4139 verbatim:
- `_CUDL_LABEL_RE` (line 2100)
- `_parse_cudl_label` (lines 2103-2132)
- `_BRIDGE_IMPORT_WARNED` + `_warn_bridge_import_failed` (lines 2171-2179) — NOTE: these are currently at lines 2171-2179 in genizah_core.py (after the logger section), NOT right after `_parse_cudl_label`. The RESEARCH pre-cluster list is accurate but the items are not all contiguous; copy each item to the new file in logical order.
- `_nli_crossref_svc` + `_get_crossref_service` (lines 2281-2292)
- `_fjms_svc` + `_get_fjms_service` (lines 2298-2309)
- `_NLI_CACHE_MAX_ENTRIES` (line 2332)
- `class _BoundedLRUCache` (lines 2335-2413)
- `MARC_FUTURE_TIMEOUT`, `NLI_IIIF_FUTURE_TIMEOUT`, `EXTERNAL_IIIF_HTTP_TIMEOUT` (lines 2421-2423)
- `class MetadataManager` (lines 2426-4139)

**LOGGER reference in pre-cluster:** `_warn_bridge_import_failed` calls `LOGGER` (line 2178). In genizah_core.py `LOGGER` is defined at line 2168 — it precedes this function. In `shared/metadata_manager.py`, `LOGGER = logging.getLogger("genizah." + __name__)` must appear before `_warn_bridge_import_failed`. Arrange module-level items so LOGGER is declared before any function that uses it.

**Lazy imports inside method bodies** — keep all three patterns verbatim (already lazy in genizah_core.py):
```python
# Inside method bodies only — NOT at module level (GUARD-01 safe):
from shared.synthetic_sys_id import is_synthetic_sys_id
from shared.nli_crossref_service import NliCrossrefService      # inside _get_crossref_service
from shared.fjms_service import FjmsService                     # inside _get_fjms_service
from shared.shelfmark_bridge import lookup_cudl, build_alias_index
from shared.nli_crossref_service import classify_cambridge_alignment
import csv                                                        # inside _load_csv_bank
```

---

### `shared/indexer.py` (service + utility, batch + file-I/O)

**Primary analog:** `shared/lists_manager.py` (has `_tr()` helper, module-level imports from `shared/`)
**Secondary analog:** `shared/responsa.py` (has `_tr()` helper, proven pattern)

**Module header pattern** — copy from `shared/lists_manager.py` lines 1-22:
```python
# -*- coding: utf-8 -*-
"""Tantivy index construction and browse-map assembly.

Phase 124: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import Indexer`` callers continue working.
"""

import logging
import os
import re
import shutil
import json
import pickle
from collections import defaultdict

import tantivy

from shared.config import Config
from shared.search_tokenizer import register_search_tokenizers
from shared.text_normalize import strip_search_diacritics
from shared.browse_map_utils import dedupe_browse_map

LOGGER = logging.getLogger("genizah." + __name__)
```

**`_tr()` helper pattern** (from `shared/lists_manager.py` lines 25-35 AND `shared/responsa.py` lines 19-29 — both identical):
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
Also add `from genizah_translations import TRANSLATIONS` at module level (required by `_tr()`). See `shared/lists_manager.py` line 18 and `shared/responsa.py` line 14.

Replace the single `tr(...)` call in `create_index` (genizah_core.py line 4319) with `_tr(...)`.

**`_strip_brackets` inline copy** (from RESEARCH Pitfall 4 — mirrors `genizah_core.py` line 4690):
```python
def _strip_brackets(text: str) -> str:
    """Remove all square brackets from *text*. Mirrors genizah_core._strip_brackets."""
    return text.replace('[', '').replace(']', '')
```
Place this before `class Indexer` in the new file. The genizah_core.py copy at line 4690 is NOT removed (it is still used by `SearchEngine`).

**Full extraction block** — copy `genizah_core.py` lines 4143-4583 verbatim (the `class Indexer` body), then substitute `tr(` → `_tr(` for the single occurrence in `create_index`.

---

### `genizah_core.py` (modify — extend existing shim block)

**Analog:** current shim block at `genizah_core.py` lines 65-103 (Phase 122/123 shims).

**Exact shim block to append** after line 103 (`from shared.lists_manager import ListsManager  # noqa: F401`):
```python
# Phase 124: metadata_manager extracted — permanent compat facade (v8.3.0)
from shared.metadata_manager import (  # noqa: F401
    _NLI_CACHE_MAX_ENTRIES,
    _BoundedLRUCache,
    MARC_FUTURE_TIMEOUT,
    NLI_IIIF_FUTURE_TIMEOUT,
    EXTERNAL_IIIF_HTTP_TIMEOUT,
    MetadataManager,
)
# Phase 124: indexer extracted — permanent compat facade (v8.3.0)
from shared.indexer import Indexer  # noqa: F401
```

**What to DELETE from genizah_core.py** after inserting the shims: the full extraction blocks — lines 2100-4139 (MetadataManager pre-cluster + class) and lines 4143-4583 (Indexer class). Keep `_strip_brackets` at line 4690 (it remains in genizah_core for `SearchEngine` use).

**The `_BRIDGE_IMPORT_WARNED` / `_warn_bridge_import_failed` pair at lines 2171-2179** moves to `shared/metadata_manager.py`. After deletion from genizah_core, it is NOT re-exported via facade (no external importers — RESEARCH Q5 confirms these are private helpers). The `LOGGER` reference at line 2179 must resolve in the new file from `shared/metadata_manager.py`'s own `LOGGER`.

**Run per-file ruff only — never `ruff --fix` repo-wide:**
```bash
python -m ruff check shared/metadata_manager.py shared/indexer.py genizah_core.py
```

---

### `tests/test_no_back_edges_core.py` (modify — grow registry + add tests)

**Analog:** existing identity + standalone test pairs in the same file (lines 160-456).

**EXTRACTED_MODULES registry extension** (lines 30-39 of the file — add two entries):
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
    "shared/metadata_manager.py",   # Phase 124 — add this
    "shared/indexer.py",             # Phase 124 — add this
]
```

**Identity test pattern** — copy structure from `test_codicological_identity` (lines 388-405) and `test_lists_manager_identity` (lines 436-445). The Phase 124 versions:
```python
# ---------------------------------------------------------------------------
# Phase 124: metadata_manager (CORE-08)
# ---------------------------------------------------------------------------

def test_metadata_manager_identity():
    """CORE-08: genizah_core.MetadataManager is the same class as shared.metadata_manager.MetadataManager."""
    import shared.metadata_manager
    import genizah_core

    assert shared.metadata_manager.MetadataManager is genizah_core.MetadataManager, (
        "genizah_core.MetadataManager is not the same object as "
        "shared.metadata_manager.MetadataManager. "
        "The re-export shim must be: from shared.metadata_manager import MetadataManager  # noqa: F401"
    )
    assert shared.metadata_manager._BoundedLRUCache is genizah_core._BoundedLRUCache, (
        "genizah_core._BoundedLRUCache is not the same object as "
        "shared.metadata_manager._BoundedLRUCache."
    )


def test_metadata_manager_standalone_import():
    """CORE-08 smoke: shared.metadata_manager imports and _BoundedLRUCache instantiates."""
    import shared.metadata_manager
    assert hasattr(shared.metadata_manager, 'MetadataManager')
    assert hasattr(shared.metadata_manager, '_BoundedLRUCache')
    # Smoke: _BoundedLRUCache instantiates with a small maxsize
    c = shared.metadata_manager._BoundedLRUCache(maxsize=10)
    assert len(c) == 0


# ---------------------------------------------------------------------------
# Phase 124: indexer (CORE-09)
# ---------------------------------------------------------------------------

def test_indexer_identity():
    """CORE-09: genizah_core.Indexer is the same class as shared.indexer.Indexer."""
    import shared.indexer
    import genizah_core

    assert shared.indexer.Indexer is genizah_core.Indexer, (
        "genizah_core.Indexer is not the same object as shared.indexer.Indexer. "
        "The re-export shim must be: from shared.indexer import Indexer  # noqa: F401"
    )


def test_indexer_standalone_import():
    """CORE-09 smoke: shared.indexer imports and Indexer instantiates with a mock meta_mgr."""
    import shared.indexer
    assert hasattr(shared.indexer, 'Indexer')

    class _FakeMM:
        pass

    idx = shared.indexer.Indexer(_FakeMM())
    assert idx.meta_mgr is not None
```

---

### `tests/test_desktop_folio_navigation.py` (modify — GUARD-03 retarget)

**Analog:** `genizah_core_source` fixture at lines 46-51 of the same file.

**Add a new fixture** (insert after line 51, before `_extract_method`):
```python
@pytest.fixture(scope="module")
def metadata_manager_source():
    """Return the full source code of shared/metadata_manager.py as a string."""
    src_path = os.path.join(os.path.dirname(__file__), '..', 'shared', 'metadata_manager.py')
    with open(src_path, 'r', encoding='utf-8') as f:
        return f.read()
```

**Retarget three tests** that call `_extract_method(genizah_core_source, 'enrich_metadata')`:
- `test_image_source_info_in_enrich_metadata` (line 169): change parameter from `genizah_core_source` to `metadata_manager_source`
- Any other test in the file that extracts `enrich_metadata` from `genizah_core_source` — check the full file for `_extract_method(genizah_core_source, 'enrich_metadata')` calls and change all occurrences.

The existing `genizah_core_source` fixture stays in place (other tests use it for `browse_render_page`, `lbl_browse_folio`, etc. which remain in `genizah_app.py` / genizah_core.py).

**This retarget must happen in the same commit as the MetadataManager extraction** — after extraction, `enrich_metadata` is absent from genizah_core.py source text (only the facade `from shared.metadata_manager import MetadataManager` line is there), so `_extract_method(genizah_core_source, 'enrich_metadata')` returns `""` and 3 assertions fail.

---

## Shared Patterns

### GUARD-01: No module-level genizah_core import
**Source:** `tests/test_no_back_edges_core.py` `_has_module_level_genizah_core_import` (lines 103-142)
**Apply to:** Both new files `shared/metadata_manager.py` and `shared/indexer.py`

The AST scanner descends into `If`/`Try`/`ClassDef` etc. but stops at `FunctionDef`. This means:
- `_tr()` function body: `from genizah_core import CURRENT_LANG` — SAFE (function body, lazy)
- `_get_crossref_service()` body: `from shared.nli_crossref_service import ...` — SAFE (function body, already `shared/`)
- Any `try: from genizah_core import X` at module level: VIOLATION — do not use

### Logger namespace
**Source:** `shared/codicological.py` line 17, `shared/joins_manager.py` line 22, `shared/lists_manager.py` line 22
**Apply to:** Both new files
```python
LOGGER = logging.getLogger("genizah." + __name__)
```
`__name__` for `shared/metadata_manager.py` is `shared.metadata_manager`, so the full logger name is `genizah.shared.metadata_manager`. This stays in the `genizah` propagation tree.

### `_tr()` helper
**Source:** `shared/lists_manager.py` lines 25-35 and `shared/responsa.py` lines 19-29 (identical)
**Apply to:** `shared/indexer.py` only (MetadataManager has no `tr()` calls)
```python
def _tr(text: str) -> str:
    from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text
```
Requires `from genizah_translations import TRANSLATIONS` at module level. GUARD-01 safe because `CURRENT_LANG` is imported inside the function body (lazy), not at module level.

### `# noqa: F401` facade shim block
**Source:** `genizah_core.py` lines 65-103 (existing Phase 122/123 shims)
**Apply to:** New shims appended after line 103

Pattern: one `from shared.X import (  # noqa: F401` block per extracted module. The `# noqa: F401` suppresses "imported but unused" for names that exist purely to maintain the public API of `genizah_core`.

### Per-file ruff check (not repo-wide)
**Source:** RESEARCH Pitfall 8 and Phase 123 lesson (memory `project_godfile_extraction_import_lesson`)
**Apply to:** Every `ruff` invocation this phase
```bash
python -m ruff check shared/metadata_manager.py shared/indexer.py genizah_core.py
```
NEVER `python -m ruff check . --fix` — it strips `# noqa: F401` shims.

---

## No Analog Found

No files in Phase 124 lack a close analog. All patterns are proven Phase 123 siblings.

---

## Commit Sequence

**Commit 1 (MetadataManager):**
1. Create `shared/metadata_manager.py` (pre-cluster lines 2100-4139 copied verbatim, with module header and reorganized imports)
2. Modify `genizah_core.py`: insert metadata_manager facade shim block; delete lines 2100-4139
3. Modify `tests/test_no_back_edges_core.py`: add `"shared/metadata_manager.py"` to `EXTRACTED_MODULES`; add `test_metadata_manager_identity` + `test_metadata_manager_standalone_import`
4. Modify `tests/test_desktop_folio_navigation.py`: add `metadata_manager_source` fixture; retarget `test_image_source_info_in_enrich_metadata` (and any other enrich_metadata tests)

**Commit 2 (Indexer):**
1. Create `shared/indexer.py` (lines 4143-4583 copied verbatim, with `_tr()` + `_strip_brackets` added, `tr(` replaced with `_tr(`)
2. Modify `genizah_core.py`: insert indexer facade shim; delete lines 4143-4583
3. Modify `tests/test_no_back_edges_core.py`: add `"shared/indexer.py"` to `EXTRACTED_MODULES`; add `test_indexer_identity` + `test_indexer_standalone_import`

**Per-commit gate:**
```bash
GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_no_back_edges_core.py tests/test_nli_cache_bounded_lru.py tests/test_browse_synthetic.py tests/test_audit_followup_2026_05_29.py tests/test_desktop_folio_navigation.py tests/test_api_nli_breaker_integration.py -x -q
```

---

## Metadata

**Analog search scope:** `shared/`, `tests/`, `genizah_core.py` (lines 65-103, 2100-4583)
**Files scanned:** 7 (codicological.py, joins_manager.py, lists_manager.py, responsa.py, genizah_core.py shim block, test_no_back_edges_core.py, test_desktop_folio_navigation.py)
**Pattern extraction date:** 2026-06-26
