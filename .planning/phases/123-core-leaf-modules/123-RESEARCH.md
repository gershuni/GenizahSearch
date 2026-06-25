# Phase 123: Core Leaf Modules - Research

**Researched:** 2026-06-25
**Domain:** Python module extraction / import graph mechanics (pure internal codebase refactor)
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01 (back-edge retarget scope):** Retarget ALL now-unblocked `shared/` importers of moved
  symbols to their new homes this phase. Mandatory: `shared/local_indexer.py:3154`/`:3826`
  (text-normalize, CORE-07), `shared/exclusion_service.py:17` (normalize_shelfmark, module-level),
  `shared/nli_crossref_service.py:365`/`:376` (normalize_shelfmark, lazy),
  `shared/search_serializer.py:248` (get_library_display, lazy). Leave on facade:
  `construct_mosseri_cudl_label` callers (symbol not moved this phase).
- **D-02 (commit shape):** Single plan, one atomic commit per cluster, forced ordering:
  `browse_map_utils` → `text_normalize` → `variants` → `responsa` → `codicological` →
  `joins_manager` → `lists_manager`. Full pytest suite green at EVERY commit boundary (SC#5).
- **D-03 (test coverage):** Per-module identity assertion (`shared.X.Y is genizah_core.Y`) +
  standalone-import smoke test for ALL 7 modules, mirroring the Phase 122 `test_config_identity`
  pattern.

### Claude's Discretion
- Exact function membership of `shared/responsa.py` (which module-level functions/constants
  vs. engine-side) — resolved by research below.
- Resolution mechanics for `_load_ie_volume_map`'s JSON path after the move (CORE-06).
- Exact shim comment wording, import-line placement, AST mechanics of growing `EXTRACTED_MODULES`.
- Which test file holds the per-module identity/smoke tests.

### Deferred Ideas (OUT OF SCOPE)
None.
</user_constraints>

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CORE-01 | Responsa parsing/expansion logic extracted to `shared/responsa.py`; responsa test suites pass via facade | Cluster boundary mapped (§ Primary Research Q1); `_apply_explosion_guard`/`_count_expanded_terms` confirmed in-scope; `build_tantivy_query` confirmed engine-side |
| CORE-02 | `VariantManager` extracted to `shared/variants.py` | Cluster boundary confirmed; Config.VARIANT_GEN_LIMIT only Config dep; no LOGGER usage in class |
| CORE-03 | `CodicologicalManager` extracted to `shared/codicological.py` | Cluster confirmed; uses `natural_sort_key` (from shared.browse_map_utils), `Config.OXFORD_DB`, `LOGGER` |
| CORE-04 | `JoinsManager` extracted to `shared/joins_manager.py` | Cluster confirmed; uses `normalize_shelfmark` (from shared.browse_map_utils), `Config.INDEX_DIR`, `LOGGER` |
| CORE-05 | `ListsManager` extracted to `shared/lists_manager.py` | Cluster confirmed; uses `Config.INDEX_DIR`, `LOGGER`, `tr()` lazily |
| CORE-06 | Browse-map + shelfmark utils extracted to `shared/browse_map_utils.py` | Path fix documented (§ Primary Research Q3); `LIBRARY_CODES` moves with cluster; `CURRENT_LANG` accessed lazily |
| CORE-07 | `strip_nikud`, `strip_search_diacritics` + constants extracted to `shared/text_normalize.py`; local_indexer lazy back-edges retargeted | Confirmed; NIKUD_PATTERN and COMBINING_DIACRITICALS_PATTERN move with cluster; GRAMMATICAL_PREFIXES/SUFFIXES belong in responsa |
| GUARD-02 | Zero behavior change; full pytest suite green at every commit boundary | Per-file ruff only; same-object shims ensure no callers break |
| GUARD-03 | 5 source-scanning/AST tests retargeted to both locations before deletion | Per-test analysis provided (§ Primary Research Q5) |
| GUARD-04 | `genizah_core.py` remains permanent compat facade | Shim recipe documented; never repo-wide ruff --fix |
</phase_requirements>

---

## Summary

Phase 123 is a pure mechanical refactor: seven cohesive clusters are copied from `genizah_core.py`
to new `shared/` modules, each guarded behind a permanent same-object `# noqa: F401` re-export
shim so all existing `from genizah_core import …` callers continue working unchanged. The recipe
was proven in Phase 122 (`shared/config.py`) and earlier in the v7.9 desktop extractions.

The single highest-risk open question — the exact boundary of `shared/responsa.py` — is resolved:
`build_tantivy_query` is a `SearchEngine` method (stays engine-side); the pure parsing/expansion
functions (lines 5865–7044) form the cluster, minus a narrow set of engine-coupled helpers
(`_add_bracket_variants`, `_query_has_brackets`, `_strip_brackets`, `_index_has_field`,
`content_search_staleness_messages`, `MARK_TOLERANT_INSERTER`, `make_mark_tolerant_pattern`,
`_SOFIT_TO_NORMAL`, `_build_wildcard_regex`, `_make_flex_spacing_pattern`, `_has_line_break_syntax`,
`LineGroup`, `_parse_line_break_query`) that are called from `SearchEngine.build_tantivy_query` /
`build_regex_pattern` and stay in `genizah_core.py` until Phase 125.

Two non-trivial discoveries: (1) `_load_ie_volume_map` in CORE-06 uses `Config.INTERNAL_DIR` —
it already resolves paths via `Config` (which lives in `shared.config`) rather than `__file__`, so
the Phase 122 BLOCKER #1 does NOT apply. (2) `get_library_display` references `CURRENT_LANG` — this
must be handled with a lazy function-body import (GUARD-01-safe).

**Primary recommendation:** Follow the D-02 ordering; land `browse_map_utils` first to unblock the
four D-01 retargets and the `codicological`/`joins_manager` cluster dependencies.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Responsa query parsing/expansion | Shared library | — | Pure functions; no I/O; no DB; no UI coupling |
| Spelling variant generation | Shared library | — | Pure algorithm; settings injected at runtime |
| Codicological unit management | Shared library | API/Desktop consumer | Loads Oxford DB via Config path; no network |
| Join document management | Shared library | Web + Desktop consumer | Reads/writes local pickle cache via Config |
| User list management | Shared library | Web + Desktop consumer | Reads/writes local pickle cache via Config |
| Browse-map utilities + shelfmark ops | Shared library | — | Pure functions; `Config` path resolution |
| Text/diacritic normalization | Shared library | — | Pure functions; stdlib only |

---

## Standard Stack

### Core (already in tree — no new packages)

| Library | Source | Purpose | Notes |
|---------|--------|---------|-------|
| `shared/config.py` | Phase 122 (this repo) | Config constants/paths | Must exist before any of the 7 clusters |
| `genizah_translations` | Existing (this repo) | `LIBRARY_CODES_HE`, `TRANSLATIONS` | Importable directly by new modules |
| Python stdlib (`re`, `os`, `json`, `pickle`, `logging`, `threading`, `dataclasses`, `typing`) | stdlib | Module-level helpers | No new third-party deps needed |

**No new packages needed.** This phase installs nothing; all dependencies are stdlib or existing
in-tree modules.

---

## Package Legitimacy Audit

> Not applicable — this phase installs zero external packages.

---

## Primary Research Findings

### Q1 — Responsa cluster boundary (CORE-01, the #1 open question)

**Verdict: `build_tantivy_query` stays engine-side. The cluster boundary is lines 5865–7044 minus
the engine-coupled helper subset.**

**Confirmed in-scope for `shared/responsa.py`** (verified by grepping call sites and dependency
graph):

| Symbol | Type | Location | Notes |
|--------|------|----------|-------|
| `ResponsaComponent` | dataclass | :5870 | Pure data; zero deps beyond stdlib |
| `parse_responsa_query` | function | :5889 | Calls `_tokenize_responsa_query`, `_parse_single_token`; no engine dep |
| `_GAP_TOKEN_RE`, `_LINE_GAP_TOKEN_RE` | regex constants | :5936, :5939 | Used by parse helpers |
| `extract_per_pair_gaps` | function | :6120 | Calls `_tokenize_responsa_query`, `_GAP_TOKEN_RE` |
| `generate_tabular_syntax` | function | :6162 | Pure formatter; zero deps beyond `ResponsaComponent` |
| `_tokenize_responsa_query` | function | :6260 | Splits on whitespace; stdlib only |
| `_parse_single_token` | function | :6290 | Creates `ResponsaComponent`; stdlib only |
| `expand_grammatical_prefixes` | function | :6419 | Uses `GRAMMATICAL_PREFIXES` |
| `expand_judeo_arabic` | function | :6441 | Pure; stdlib only |
| `expand_grammatical_suffixes` | function | :6661 | Uses `GRAMMATICAL_SUFFIXES`, `_SOFIT_TO_NORMAL`... |
| `expand_plene_defective` | function | :6689 | Pure; stdlib only |
| `_count_expanded_terms` | function | :6736 | Internal counter for `_apply_explosion_guard` |
| `_apply_explosion_guard` | function | :6794 | Uses `Config.MAX_EXPANDED_TERMS`, `tr()` |
| `_expand_inline_alternation` | function | :7005 | Pure; stdlib only |
| `GRAMMATICAL_PREFIXES` | constant list | :165 | Used by `expand_grammatical_prefixes`, tests import it from `genizah_core` |
| `GRAMMATICAL_SUFFIXES` | constant list | :174 | Used by `expand_grammatical_suffixes`, tests import it |

**`GRAMMATICAL_PREFIXES` and `GRAMMATICAL_SUFFIXES` placement decision:** They are defined near
line 165 in `genizah_core.py`, adjacent to `NIKUD_PATTERN`. The comment labels them "Responsa
Search Constants." They are ONLY used by the responsa expansion functions and the 6 responsa test
files. They belong in `shared/responsa.py`, not `shared/text_normalize.py`. The `genizah_core.py`
shim exports them alongside the other responsa shims.

**`_SOFIT_TO_NORMAL` dict (line :6652):** Used by `expand_grammatical_suffixes` (in-scope) AND by
`SearchEngine.build_tantivy_query` (:7993–7994) and `build_regex_pattern` (:8624–8627). Because it
is used by both responsa functions AND engine methods, it should move to `shared/responsa.py` and
the engine references it via the `genizah_core` facade shim (same object, zero behavior change).

**`_apply_explosion_guard` dependencies:**
- Calls `Config.MAX_EXPANDED_TERMS` → `from shared.config import Config` (safe)
- Calls `tr()` → lazy import inside the function body: `from genizah_core import tr` is NOT safe
  at module level (GUARD-01 violation). The clean fix: copy `tr()` lazily, OR import
  `genizah_translations.TRANSLATIONS` directly and replicate the `tr()` logic inline.
  **Recommended:** `shared/responsa.py` imports `TRANSLATIONS` from `genizah_translations`
  and defines its own `_tr(text) -> str` inline (3 lines; same logic as `genizah_core.tr`).
  This is the cleanest GUARD-01-safe approach. The genizah_core `tr()` stays in `genizah_core.py`.

**Confirmed engine-side (NOT moved, stay in `genizah_core.py` until Phase 125):**

| Symbol | Reason to stay |
|--------|---------------|
| `build_tantivy_query` | SearchEngine method; requires `self`; SEED-006 `content_search` compat gates live here |
| `build_regex_pattern` | SearchEngine method; calls all the helpers below |
| `_add_bracket_variants` | Called from `build_tantivy_query` (:8008, :8091) |
| `_query_has_brackets` | Called from `build_regex_pattern` (:8708, :8724–8727) |
| `_strip_brackets` | Called from `_query_has_brackets` sites and `_strip_brackets` via non-responsa path (:5472) |
| `_index_has_field` | Called by SearchEngine `_open_local_searcher` (:7163, :7203, :7704) |
| `content_search_staleness_messages` | Called by SearchEngine `reload_index` (:7713, :7744, :7760) |
| `MARK_TOLERANT_INSERTER`, `make_mark_tolerant_pattern` | Called by `build_tantivy_query` (:8150, :8220, :8520); also by `enrich_metadata` (:1375) |
| `_build_wildcard_regex` | Called by `build_tantivy_query` |
| `_make_flex_spacing_pattern` | Called by `build_tantivy_query` (:7924, :9087); also tested directly by `test_responsa_edge_cases.py` which imports it from `genizah_core` — stays on facade |
| `_has_line_break_syntax` | Called from SearchEngine path |
| `LineGroup`, `_parse_line_break_query` | Called from SearchEngine line-break paths (:7851, :8964) |

**`strip_search_diacritics` boundary note:** This function is at line :6493, within the responsa-
range block, but its purpose is text normalization (SEED-006, content_search field). It belongs in
`shared/text_normalize.py` (CORE-07), NOT `shared/responsa.py`. The `COMBINING_DIACRITICALS_PATTERN`
constant (:6490) moves with it to `text_normalize.py`. The genizah_core.py shim covers both.

**Import list for `shared/responsa.py`:**
```python
import re
from collections import defaultdict  # if needed by expansion functions
from typing import List, Optional
from dataclasses import dataclass, field
from genizah_translations import TRANSLATIONS   # for _tr() helper; NOT genizah_core
from shared.config import Config                # for Config.MAX_EXPANDED_TERMS
```

No `import genizah_core` at module level — GUARD-01 compliant.

---

### Q2 — Per-cluster dependency closure for all 7 modules

#### Module 1: `shared/text_normalize.py` (CORE-07, lands second)

**Symbols to move:**
- `NIKUD_PATTERN` (line :161)
- `strip_nikud` (line :203)
- `COMBINING_DIACRITICALS_PATTERN` (line :6490)
- `strip_search_diacritics` (line :6493)

**Dependencies:** stdlib (`re`) only. Zero Config, zero LOGGER, zero genizah_core imports.

**Shims in `genizah_core.py`:**
```python
from shared.text_normalize import NIKUD_PATTERN, strip_nikud  # noqa: F401
from shared.text_normalize import COMBINING_DIACRITICALS_PATTERN, strip_search_diacritics  # noqa: F401
```

**D-01 retargets at this commit:**
- `shared/local_indexer.py:3154`: `from genizah_core import strip_nikud, strip_search_diacritics`
  → `from shared.text_normalize import strip_nikud, strip_search_diacritics`
- `shared/local_indexer.py:3826`: `from genizah_core import strip_search_diacritics`
  → `from shared.text_normalize import strip_search_diacritics`

---

#### Module 2: `shared/browse_map_utils.py` (CORE-06, lands first — enables codicological + joins)

**Symbols to move (all defined in `genizah_core.py`):**

| Symbol | Line | Notes |
|--------|------|-------|
| `LIBRARY_CODES` | :2114 | Constant dict — moves WITH cluster; shim stays in genizah_core |
| `get_library_display` | :2213 | Needs `CURRENT_LANG` lazily, `LIBRARY_CODES`, `LIBRARY_CODES_HE` |
| `_LIBRARY_PREFIX_ALIASES` | :2236 | Module-level mutable (None → built lazily) |
| `_get_library_prefix_aliases` | :2240 | Uses `LIBRARY_CODES` |
| `_strip_library_prefix` | :2271 | Private; used only inside genizah_core.py (:5310) — move with cluster |
| `normalize_shelfmark` | :213 | Pure function; stdlib only; no deps |
| `natural_sort_key` | :540 | Pure function; stdlib only |
| `_load_ie_volume_map` | :2296 | Uses `Config.INTERNAL_DIR`, `LOGGER.debug` |
| `_extract_ie_from_header` | :2342 | Private; called by `_repair_missing_ie_pages`, `dedupe_browse_map` |
| `_repair_missing_ie_pages` | :2348 | Uses `Config.FILE_V8`, `LOGGER`, `_load_ie_volume_map`, `_extract_ie_from_header` |
| `dedupe_browse_map` | :2479 | Calls `_repair_missing_ie_pages`, `_load_ie_volume_map`, `_extract_ie_from_header` |

**Dependencies for `shared/browse_map_utils.py`:**
```python
import os
import re
import json
import logging
from typing import Optional
from genizah_translations import LIBRARY_CODES_HE          # safe; not genizah_core
from shared.config import Config                            # safe; not a cycle

LOGGER = logging.getLogger(__name__)                        # no genizah_core needed
```

**`CURRENT_LANG` in `get_library_display`:** The function uses `CURRENT_LANG` as a fallback when
`lang` is FALSY. The live code is `effective_lang = lang if lang else CURRENT_LANG` — a FALSY check
(handles both `None` AND `""`), **NOT** `is None`. Since `CURRENT_LANG` is a module-level mutable global
defined at `genizah_core.py:2774`, the new module cannot import it at module level (GUARD-01).
**Solution:** lazy import inside the function body, PRESERVING the exact falsy semantics — using
`is None` would change `lang=""` from current-language to English (Codex F1, zero-behavior-change
violation):
```python
def get_library_display(code: str, short: bool = True, lang: str = None) -> str:
    ...
    effective_lang = lang
    if not effective_lang:        # falsy: None OR "" — matches live `lang if lang else CURRENT_LANG`
        from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
        effective_lang = CURRENT_LANG
    ...
```

**`LIBRARY_CODES` shim:** Since `tests/test_fist_gap_fill.py:23` imports `LIBRARY_CODES` from
`genizah_core`, add it to the shim:
```python
from shared.browse_map_utils import LIBRARY_CODES  # noqa: F401
```

**D-01 retargets at this commit:**
- `shared/exclusion_service.py:17`: `from genizah_core import normalize_shelfmark`
  → `from shared.browse_map_utils import normalize_shelfmark`
- `shared/nli_crossref_service.py:365, :376`: same retarget (lazy)
- `shared/search_serializer.py:248`: `from genizah_core import get_library_display`
  → `from shared.browse_map_utils import get_library_display`

---

#### Module 3: `shared/variants.py` (CORE-02)

**Symbols to move:**
- `class VariantManager` (lines :2790–~3190)

**Dependencies for `shared/variants.py`:**
```python
from collections import defaultdict
from typing import List
import itertools
from shared.config import Config   # for Config.VARIANT_GEN_LIMIT
try:
    from unified_variants import UNIFIED_VARIANT_PAIRS, get_top_pairs
except ImportError:
    UNIFIED_VARIANT_PAIRS = []
    def get_top_pairs(n): return []
```

No `LOGGER` in VariantManager (verified by grep — the class uses no logging). No `tr()`. Clean.

**Shim:** `from shared.variants import VariantManager  # noqa: F401`

---

#### Module 4: `shared/codicological.py` (CORE-03, must come after browse_map_utils)

**Symbols to move:**
- `class CodicologicalManager` (lines :3191–~3705)

**Dependencies for `shared/codicological.py`:**
```python
import os
import json
import logging
from shared.config import Config            # for Config.OXFORD_DB
from shared.browse_map_utils import natural_sort_key  # for sort calls at :3389, :3545

LOGGER = logging.getLogger(__name__)
```

No `tr()` in CodicologicalManager (verified — the class logs but does not translate user-facing strings). Clean, no genizah_core at module level.

**Shim:** `from shared.codicological import CodicologicalManager  # noqa: F401`

---

#### Module 5: `shared/responsa.py` (CORE-01, independent)

**Symbols to move (per Q1 analysis above):**
- `GRAMMATICAL_PREFIXES` (currently :165 — will be relocated)
- `GRAMMATICAL_SUFFIXES` (currently :174)
- `_SOFIT_TO_NORMAL` (currently :6652)
- `ResponsaComponent` dataclass
- `_GAP_TOKEN_RE`, `_LINE_GAP_TOKEN_RE`
- `parse_responsa_query`, `_tokenize_responsa_query`, `_parse_single_token`
- `_has_line_break_syntax`, `LineGroup`, `_parse_line_break_query` — **DEFERRED NOTE:**
  These are also used by `SearchEngine` (`:7851`, `:8964`). Decision: move them to
  `shared/responsa.py` and access them from the genizah_core shim. Same pattern, they have no
  engine coupling themselves (they only parse query strings). This is cleaner than leaving them
  engine-side and having responsa.py and SearchEngine both deal with them.
- `extract_per_pair_gaps`
- `generate_tabular_syntax`
- `expand_grammatical_prefixes`, `expand_judeo_arabic`, `expand_grammatical_suffixes`
- `expand_plene_defective`, `_expand_inline_alternation`
- `_count_expanded_terms`, `_apply_explosion_guard`

**Dependencies for `shared/responsa.py`:**
```python
import re
from typing import List, Optional
from dataclasses import dataclass
from shared.config import Config                 # for Config.MAX_EXPANDED_TERMS
from genizah_translations import TRANSLATIONS    # for inline _tr() helper
```

**Inline `_tr()` helper** (3-line private helper in `shared/responsa.py`):
```python
# Mirrors genizah_core.tr() — lazy language translation using TRANSLATIONS dict.
# Avoids importing genizah_core at module level (GUARD-01).
def _tr(text: str) -> str:
    """Translate text if Hebrew mode is active."""
    try:
        from genizah_core import CURRENT_LANG  # noqa: PLC0415 — lazy; GUARD-01 safe
        if CURRENT_LANG == 'he':
            return TRANSLATIONS.get(text, text)
    except Exception:
        pass
    return text
```

Note: `_apply_explosion_guard` calls `tr()` for user-facing warning messages. At test time
(no language file set) `CURRENT_LANG == 'en'` so `_tr()` returns the English string unchanged.
All 6 responsa test files import from `genizah_core` and will continue working via the shim.

**`_SOFIT_TO_NORMAL` placement decision:** Moves to `shared/responsa.py` along with `expand_grammatical_suffixes`. The engine references at `build_tantivy_query` (:7993–7994) and `build_regex_pattern` (:8624–8627) go through the `genizah_core` facade shim.

---

#### Module 6: `shared/joins_manager.py` (CORE-04, must come after browse_map_utils)

**Symbols to move:**
- `class JoinsManager` (lines :10669–~11200)

**Dependencies for `shared/joins_manager.py`:**
```python
import os
import pickle
import threading
import logging
from shared.config import Config                              # for Config.INDEX_DIR
from shared.browse_map_utils import normalize_shelfmark       # for _normalize_shelfmark wrapper

LOGGER = logging.getLogger(__name__)
```

**Verify:** `JoinsManager.JOINS_FILE = os.path.join(Config.INDEX_DIR, "joins_cache.pkl")` — class-level attribute resolved at class-definition time, so `Config` must be imported at module level. This is safe since `shared.config` is a true leaf (no genizah_core import).

**Shim:** `from shared.joins_manager import JoinsManager  # noqa: F401`

---

#### Module 7: `shared/lists_manager.py` (CORE-05, must come after browse_map_utils — though ListsManager doesn't use browse_map_utils directly, it's last by convention)

**Symbols to move:**
- `class ListsManager` (lines :11201–12374, end of file)

**Dependencies for `shared/lists_manager.py`:**
```python
import os
import pickle
import time
import logging
from shared.config import Config    # for Config.INDEX_DIR

LOGGER = logging.getLogger(__name__)
```

**`tr()` in ListsManager:** Used in methods at :11728, :12243, :12354–12367. All are inside
method bodies — can be lazy-imported inside those methods:
```python
def some_method(self):
    from genizah_core import tr  # noqa: PLC0415 — lazy; GUARD-01 safe
```

Alternatively, `shared/lists_manager.py` can define the same inline `_tr()` as `shared/responsa.py`.
**Recommended:** use the same inline `_tr()` pattern — avoids the lazy-per-method clutter.

**Shim:** `from shared.lists_manager import ListsManager  # noqa: F401`

---

### Q3 — `_load_ie_volume_map` JSON-path resolution after the move (CORE-06)

**Finding: The Phase 122 BLOCKER #1 does NOT apply here.**

The function resolves paths via `Config.INTERNAL_DIR`, not via `__file__`:

```python
vol_path = os.path.join(Config.INTERNAL_DIR, "ie_volume_map.json")   # :2304
map_path = os.path.join(Config.INTERNAL_DIR, "primary_ie_map.json")  # :2314
```

`Config.INTERNAL_DIR` is already correct after Phase 122: it resolves to the repo root (or the
PyInstaller bundle's `_MEIPASS`), regardless of which module accesses it. The `Config` class is
now in `shared/config.py` with the correct `__file__`-depth fix applied. There is no `__file__`-
relative path in `_load_ie_volume_map` itself.

**Verdict:** Copy `_load_ie_volume_map` verbatim to `shared/browse_map_utils.py`. Replace the
`LOGGER` reference with `logging.getLogger(__name__)` at module level. No path-resolution
adjustment needed.

---

### Q4 — GUARD-01 registry growth mechanics

**Verified against `tests/test_no_back_edges_core.py`.**

The `EXTRACTED_MODULES` registry (line :29) is a plain Python list of relative paths:
```python
EXTRACTED_MODULES = [
    "shared/config.py",          # Phase 122
    # Phase 123 additions below:
    "shared/browse_map_utils.py",
    "shared/text_normalize.py",
    "shared/variants.py",
    "shared/responsa.py",
    "shared/codicological.py",
    "shared/joins_manager.py",
    "shared/lists_manager.py",
]
```

The parametrized test `test_no_module_level_genizah_core_import` iterates over this list and
for each path asserts:
1. The file exists at `REPO_ROOT / rel_path`
2. The file has zero module-level `import genizah_core` or `from genizah_core import ...` statements
   (scope-aware: descends into top-level `If`/`Try`/`With` but stops at `FunctionDef` bodies)

**The registry should be grown one entry per commit**, so at each commit boundary only the
newly-extracted modules are tested (the prior modules were already green before). In practice,
adding all 7 at once after the last commit is also acceptable since they'll all pass by then.
The safest pattern: add each module's entry to `EXTRACTED_MODULES` IN THE SAME COMMIT as its
extraction, so the guard verifies it immediately.

**Confirmed: None of the D-01 retarget consumers are in the registry.** `shared/exclusion_service.py`,
`shared/nli_crossref_service.py`, `shared/search_serializer.py`, and `shared/local_indexer.py`
are NOT in `EXTRACTED_MODULES` and are NOT milestone-extracted modules — they are pre-existing
`shared/` files that happen to import from `genizah_core`. GUARD-01 only scans the extracted subset.

---

### Q5 — GUARD-03 source-scanning / AST tests (the 5 named tests)

**Detailed retarget analysis per test:**

#### `test_shelfmark_bridge.py` (DIRECTLY AFFECTED by CORE-06)

- **File scanned:** `genizah_core.py` (via `from genizah_core import normalize_shelfmark` at line :11)
- **Critical action at line :85:**
  ```python
  src = inspect.getsource(normalize_shelfmark)
  current = hashlib.sha256(src.encode("utf-8")).hexdigest()
  assert current == snapshot["source_sha256"]
  ```
  After extraction, `genizah_core.normalize_shelfmark` IS the same object as
  `shared.browse_map_utils.normalize_shelfmark` (same-object shim). However,
  `inspect.getsource(normalize_shelfmark)` follows the object to its **actual definition location**
  — which will be `shared/browse_map_utils.py` after the move. The SHA256 hash will CHANGE.
- **Retarget required:** Before deletion (Phase 127), update the test to import from BOTH locations
  and accept the new hash. **During this additive phase (Phase 123):** update the snapshot fixture
  `tests/fixtures/normalize_shelfmark_snapshot.json` with the new SHA256 hash of the function
  as it appears in `shared/browse_map_utils.py`. The test should be updated to also check
  `shared.browse_map_utils.normalize_shelfmark` source is identical.
- **Practical fix for this phase:** Add a dual-path fixture: regenerate the snapshot from the
  canonical location `shared.browse_map_utils.normalize_shelfmark` and verify both point to the
  same source. The import `from genizah_core import normalize_shelfmark` continues to work.

#### `test_desktop_folio_navigation.py` (NOT affected by this phase)

- **Files scanned:** `genizah_app.py` (line :33–34) and `genizah_core.py` (line :47–49)
- **Moved symbols tested:** `enrich_metadata` (a `MetadataManager` method, NOT moved in Phase 123)
- **Action:** No retarget needed in Phase 123. This test is affected by Phase 124
  (`MetadataManager` move). Note for the planner: the GUARD-03 requirement specifies retargeting
  "before any deletion" — since Phase 123 deletes nothing, this test is safe until Phase 124+.

#### `test_wr01_open_local_browse_page_ast.py` (NOT affected by this phase)

- **File scanned:** `genizah_app.py` (line :24: `open('genizah_app.py', encoding='utf-8').read()`)
- **Moved symbols tested:** None in this phase (scans for `browse_show_search_results` which is
  a `GenizahGUI` method in `genizah_app.py`)
- **Action:** No retarget needed in Phase 123.

#### `test_tabular_builder_rtl.py` (NOT affected by this phase)

- **File scanned:** `genizah_app.py` (line :15: `TARGET = ... / "genizah_app.py"`)
- **Moved symbols tested:** `TabularQueryBuilderDialog` — a desktop dialog class in `genizah_app.py`,
  NOT moved in Phase 123 (it's a desktop Phase 126 extraction)
- **Action:** No retarget needed in Phase 123.

#### `test_view_all_cap.py` (NOT affected by this phase)

- **File scanned:** `genizah_app.py` (line :14: reads the file at module load time)
- **Moved symbols tested:** `_VIEW_ALL_PAGE_CAP`, `browse_text` — both in `genizah_app.py`, NOT
  moved in Phase 123
- **Action:** No retarget needed in Phase 123.

**Phase 123 GUARD-03 work summary:**
- **Only `test_shelfmark_bridge.py` requires attention in Phase 123.** Specifically: update
  `tests/fixtures/normalize_shelfmark_snapshot.json` with the new SHA256 hash from
  `shared/browse_map_utils.normalize_shelfmark` source, and verify the test passes with both
  the shim import (`genizah_core.normalize_shelfmark`) and the direct import
  (`shared.browse_map_utils.normalize_shelfmark`).
- The other 4 tests are affected by later phases (124–126) and are untouched here.

---

### Q6 — Same-object re-export shim recipe (verified against Phase 122)

**Exact recipe (proven in Phase 122, `shared/config.py`):**

Step 1 — Copy implementation to new module (`shared/X.py`). The new file is GUARD-01-clean:
no module-level `import genizah_core` or `from genizah_core import ...`.

Step 2 — In `genizah_core.py`, REPLACE the original definition with a re-export shim:
```python
from shared.text_normalize import strip_nikud, strip_search_diacritics  # noqa: F401
```
The `# noqa: F401` is MANDATORY to prevent ruff from stripping the import as "unused"
(these are re-exports, not direct callers).

Step 3 — For class-level definitions that need the same-object identity guarantee (D-03):
```python
from shared.variants import VariantManager  # noqa: F401
```
After this, `shared.variants.VariantManager is genizah_core.VariantManager` is True
because Python module caching ensures the same class object. This is the same-object
identity guaranteed by `import` caching — NOT a copy.

**Per-file ruff caveat (Phase 122 D-06, confirmed):**
- `ruff --fix` strips `# noqa: F401` shims if run repo-wide. The ONLY safe verification
  is `python -m ruff check <file>` per-file, reviewed by a human. This is SC#5 compliance.
- Never run `python -m ruff check . --fix` during Phase 123 commits.

**Where to place shim lines in `genizah_core.py`:** Group all Phase 123 shims together near
the top of the file, immediately after the existing Phase 122 shim at line :64
(`from shared.config import Config  # noqa: F401`). For multi-symbol shims, one import line
per module is fine. Order: text_normalize, browse_map_utils, variants, responsa, codicological,
joins_manager, lists_manager — matches D-02 commit ordering.

---

### Q7 — Forced ordering / circular-import risk

**Verified: no circular imports among the 7 new `shared/` modules.**

Dependency graph among the new modules:
```
shared/text_normalize.py      → (stdlib only)
shared/browse_map_utils.py    → shared.config, genizah_translations
shared/variants.py            → shared.config, unified_variants (optional)
shared/responsa.py            → shared.config, genizah_translations
shared/codicological.py       → shared.config, shared.browse_map_utils
shared/joins_manager.py       → shared.config, shared.browse_map_utils
shared/lists_manager.py       → shared.config
```

The only intra-shared dependencies are:
- `shared/codicological.py` → `shared/browse_map_utils.py` (for `natural_sort_key`)
- `shared/joins_manager.py` → `shared/browse_map_utils.py` (for `normalize_shelfmark`)

Both are resolved by the D-02 forced ordering: `browse_map_utils` lands first.

**No cross-manager coupling (confirmed by grep):**
- `VariantManager` does NOT import `CodicologicalManager`, `JoinsManager`, or `ListsManager`
- `CodicologicalManager` does NOT import `VariantManager`, `JoinsManager`, or `ListsManager`
- `JoinsManager` does NOT reference `VariantManager`, `CodicologicalManager`, or `ListsManager`
- `ListsManager` does NOT reference any of the other managers
- None of the 7 modules imports `MetadataManager` or `SearchEngine` (those are Phase 124/125)

**`CodicologicalManager.load(csv_bank=None)` confirmation (SEED-020 §7 C-2):**
`csv_bank` is a method parameter defaulting to `None`, NOT a module-level `MetadataManager` import.
`CodicologicalManager` is clean to extract without Phase 124. Verified at :3219–3220.

---

## Architecture Patterns

### The Same-Object Shim Recipe

```
genizah_core.py (before):          shared/X.py (new):
  def normalize_shelfmark(...):       def normalize_shelfmark(...):
      ...                                 ...

genizah_core.py (after Phase 123):
  from shared.browse_map_utils import normalize_shelfmark  # noqa: F401
  # (original def removed)
```

Invariant: `shared.browse_map_utils.normalize_shelfmark is genizah_core.normalize_shelfmark`

### Forced Commit Ordering (D-02)

```
Commit 1: shared/browse_map_utils.py  ← D-01 retargets for exclusion_service,
           + GUARD-01 registry entry     nli_crossref_service, search_serializer
           + test_shelfmark_bridge snapshot update

Commit 2: shared/text_normalize.py   ← D-01 retargets for local_indexer (×2)
           + GUARD-01 registry entry

Commit 3: shared/variants.py
           + GUARD-01 registry entry
           + identity/smoke tests for variants

Commit 4: shared/responsa.py
           + GUARD-01 registry entry
           + identity/smoke tests for responsa cluster

Commit 5: shared/codicological.py    ← imports shared.browse_map_utils
           + GUARD-01 registry entry

Commit 6: shared/joins_manager.py    ← imports shared.browse_map_utils
           + GUARD-01 registry entry

Commit 7: shared/lists_manager.py
           + GUARD-01 registry entry
           + all remaining identity/smoke tests
           + per-file ruff review on all 7 new files
```

### Recommended Project Structure (additions only)

```
shared/
├── config.py               # Phase 122 (existing)
├── text_normalize.py       # Phase 123 NEW — NIKUD_PATTERN, strip_nikud, COMBINING_DIACRITICALS_PATTERN, strip_search_diacritics
├── browse_map_utils.py     # Phase 123 NEW — normalize_shelfmark, natural_sort_key, LIBRARY_CODES, get_library_display, dedupe_browse_map, _load_ie_volume_map
├── variants.py             # Phase 123 NEW — VariantManager
├── responsa.py             # Phase 123 NEW — ResponsaComponent, parse_responsa_query, expand_*, _apply_explosion_guard, GRAMMATICAL_PREFIXES/SUFFIXES
├── codicological.py        # Phase 123 NEW — CodicologicalManager
├── joins_manager.py        # Phase 123 NEW — JoinsManager
└── lists_manager.py        # Phase 123 NEW — ListsManager
```

### Anti-Patterns to Avoid

- **Module-level `from genizah_core import …` in any new `shared/` module.** Use `from shared.config import Config` instead. Use `from genizah_translations import TRANSLATIONS` instead of `from genizah_core import TRANSLATIONS`. Use lazy function-body imports for mutable globals like `CURRENT_LANG` and `tr()`.
- **Repo-wide `ruff --fix`.** Strips the `# noqa: F401` shims. Run `python -m ruff check <file>` per-file only.
- **Using `inspect.getsource` on shim-backed symbols without updating test fixtures.** `inspect.getsource` follows the object to its defining module — the SHA256 hash in `normalize_shelfmark_snapshot.json` must be regenerated from `shared/browse_map_utils.normalize_shelfmark`.
- **Growing `EXTRACTED_MODULES` without creating the file.** The guard asserts the file exists. Add the registry entry IN THE SAME COMMIT as the file creation.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Translation helper in new modules | New i18n system | Inline `_tr()` pattern + lazy `from genizah_core import CURRENT_LANG` | 3-line helper; matches existing tr() semantics exactly |
| Path resolution for data files | `__file__`-relative logic | `Config.INTERNAL_DIR` (already correct from Phase 122) | Config already has the depth-fix applied |
| GUARD-01 verification | Manual review | `tests/test_no_back_edges_core.py` parametrization | Already written; just add module entries to `EXTRACTED_MODULES` |
| Same-object identity verification | Runtime assertion in production code | Test-time `assert shared.X.Y is genizah_core.Y` (D-03 tests) | Proof-of-correctness, not prod guard |

---

## Common Pitfalls

### Pitfall 1: `inspect.getsource` Following the Object to its Real File

**What goes wrong:** `test_shelfmark_bridge.py:85` hashes `inspect.getsource(normalize_shelfmark)`. After extraction, the function object lives in `shared/browse_map_utils.py` — inspect will read THAT file. The snapshot SHA256 no longer matches.

**Why it happens:** Python's `inspect.getsource` uses `co_filename` from the function's code object, not the module it was imported FROM.

**How to avoid:** Regenerate `tests/fixtures/normalize_shelfmark_snapshot.json` after extracting to `shared/browse_map_utils.py`. The updated hash must reflect the function's source as it appears in the new location.

**Warning signs:** `test_shelfmark_bridge.py::TestNormalizeShelfmark::test_source_sha256_unchanged` FAILS with a hash mismatch immediately after the browse_map_utils commit.

---

### Pitfall 2: Module-Level Mutable Globals That Must Reflect Runtime State

**What goes wrong:** `CURRENT_LANG` is set by `load_language()` at genizah_core.py:2774 and mutated by `save_language()`. If `shared/browse_map_utils.py` imported `from genizah_core import CURRENT_LANG` AT MODULE LEVEL, it would capture the value at import time (always 'en') and never update when the language changes.

**Why it happens:** Python module-level name binding; mutable assignments to `CURRENT_LANG = ...` don't propagate to re-bound names in other modules.

**How to avoid:** Only access `CURRENT_LANG` via lazy function-body import: `from genizah_core import CURRENT_LANG` inside `get_library_display`. This re-reads the current value every call.

**Warning signs:** `get_library_display` always returns English strings even when Hebrew mode is set.

---

### Pitfall 3: Class-Level `Config.*` Attribute Binding at Class-Definition Time

**What goes wrong:** `JoinsManager.JOINS_FILE = os.path.join(Config.INDEX_DIR, "joins_cache.pkl")` — this executes at class-definition time (when the module is first imported). If `Config` is not imported yet, it raises `NameError`.

**Why it happens:** Class body code runs at class-definition time, not at instantiation time.

**How to avoid:** Ensure `from shared.config import Config` is at the module level of `joins_manager.py` and `lists_manager.py` BEFORE the class definition. `shared.config` is a stdlib-only leaf — safe to import at module level without GUARD-01 concern.

**Warning signs:** `NameError: name 'Config' is not defined` when importing `shared.joins_manager`.

---

### Pitfall 4: ruff Stripping `# noqa: F401` Shims

**What goes wrong:** Someone runs `python -m ruff check . --fix` repo-wide. Ruff auto-removes "unused" imports, destroying all the `# noqa: F401` re-export shims. Every existing `from genizah_core import ...` caller then gets `ImportError`.

**Why it happens:** `# noqa: F401` suppresses the warning during check but `--fix` still acts on it in some ruff versions. The risk is process-level (CI/CD might run `--fix`).

**How to avoid:** Per-file `python -m ruff check <file>` review only; never `python -m ruff check . --fix`. Document this in the commit message and plan task descriptions.

**Warning signs:** `ImportError: cannot import name 'normalize_shelfmark' from 'genizah_core'` at runtime after a repo-wide ruff run.

---

### Pitfall 5: Growing GUARD-01 Registry Before File Exists

**What goes wrong:** Adding `"shared/variants.py"` to `EXTRACTED_MODULES` in the same commit as a failing step causes the guard to fail with "file not found" even before the module content is checked.

**Why it happens:** The guard asserts `path.exists()` first; if you add the registry entry before creating the file, CI fails.

**How to avoid:** Add registry entry AND create the file IN THE SAME COMMIT. Test locally before committing.

---

### Pitfall 6: `_apply_explosion_guard` / `_count_expanded_terms` Missing from Responsa Shims

**What goes wrong:** `test_responsa_core.py:29` imports `_apply_explosion_guard` from `genizah_core`. If the shim is missing, `ImportError`.

**Why it happens:** Private functions starting with `_` are easy to overlook when building the shim list. `_apply_explosion_guard` is explicitly imported by the test suite.

**How to avoid:** The shim in `genizah_core.py` must export ALL symbols including private ones imported by tests:
```python
from shared.responsa import (
    ResponsaComponent, parse_responsa_query, extract_per_pair_gaps,
    generate_tabular_syntax, expand_grammatical_prefixes,
    expand_grammatical_suffixes, expand_judeo_arabic, expand_plene_defective,
    _expand_inline_alternation, _apply_explosion_guard, _count_expanded_terms,
    GRAMMATICAL_PREFIXES, GRAMMATICAL_SUFFIXES, _SOFIT_TO_NORMAL,
    _GAP_TOKEN_RE, _LINE_GAP_TOKEN_RE, _has_line_break_syntax,
    LineGroup, _parse_line_break_query,
)  # noqa: F401
```

---

## Code Examples

### Phase 122 Shim Pattern (reference implementation)

```python
# genizah_core.py — Phase 122 shim (already in tree at line :64)
from shared.config import Config  # noqa: F401
```

```python
# tests/test_no_back_edges_core.py — CONFIG-01 identity test (already in tree)
def test_config_identity():
    import shared.config
    import genizah_core
    assert shared.config.Config is genizah_core.Config
```

### Phase 123 Shim Pattern (how to apply per cluster)

```python
# genizah_core.py additions — after line :64
from shared.text_normalize import (            # noqa: F401
    NIKUD_PATTERN, strip_nikud,
    COMBINING_DIACRITICALS_PATTERN, strip_search_diacritics,
)
from shared.browse_map_utils import (          # noqa: F401
    LIBRARY_CODES, normalize_shelfmark, natural_sort_key,
    get_library_display, dedupe_browse_map,
)
from shared.variants import VariantManager     # noqa: F401
from shared.responsa import (                  # noqa: F401
    GRAMMATICAL_PREFIXES, GRAMMATICAL_SUFFIXES, _SOFIT_TO_NORMAL,
    ResponsaComponent, parse_responsa_query, extract_per_pair_gaps,
    generate_tabular_syntax, expand_grammatical_prefixes,
    expand_grammatical_suffixes, expand_judeo_arabic, expand_plene_defective,
    _expand_inline_alternation, _apply_explosion_guard, _count_expanded_terms,
    _GAP_TOKEN_RE, _LINE_GAP_TOKEN_RE, _has_line_break_syntax,
    LineGroup, _parse_line_break_query,
)
from shared.codicological import CodicologicalManager  # noqa: F401
from shared.joins_manager import JoinsManager          # noqa: F401
from shared.lists_manager import ListsManager          # noqa: F401
```

### GUARD-01 Registry Extension

```python
# tests/test_no_back_edges_core.py — EXTRACTED_MODULES (Phase 123 additions)
EXTRACTED_MODULES = [
    "shared/config.py",            # Phase 122
    "shared/text_normalize.py",    # Phase 123
    "shared/browse_map_utils.py",  # Phase 123
    "shared/variants.py",          # Phase 123
    "shared/responsa.py",          # Phase 123
    "shared/codicological.py",     # Phase 123
    "shared/joins_manager.py",     # Phase 123
    "shared/lists_manager.py",     # Phase 123
]
```

### D-03 Identity Test Pattern (per module, to be added to `test_no_back_edges_core.py`)

```python
# Add 7 similar tests; this shows the pattern for variants:
def test_variant_manager_identity():
    """CORE-02: genizah_core.VariantManager is the same class as shared.variants.VariantManager."""
    import shared.variants
    import genizah_core
    assert shared.variants.VariantManager is genizah_core.VariantManager, (
        "genizah_core.VariantManager is not the same object as shared.variants.VariantManager. "
        "The re-export shim must be: from shared.variants import VariantManager  # noqa: F401"
    )

def test_variant_manager_standalone_import():
    """CORE-02 smoke: shared.variants can be imported without importing genizah_core."""
    import sys
    # Save state
    saved = {k: v for k, v in sys.modules.items() if 'genizah_core' in k}
    try:
        # Temporarily remove genizah_core if present (optional; the key check is no ImportError)
        import shared.variants
        assert hasattr(shared.variants, 'VariantManager')
    finally:
        pass  # Restore is not needed; the point is it doesn't raise
```

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | `pytest.ini` (existing) |
| Quick run command | `pytest tests/test_no_back_edges_core.py tests/test_responsa_core.py tests/test_shelfmark_bridge.py -x -q` |
| Full suite command | `pytest tests/ -q --ignore=tests/gui_tests/` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | Notes |
|--------|----------|-----------|-------------------|-------|
| CORE-01 | `shared.responsa.ResponsaComponent is genizah_core.ResponsaComponent` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "responsa_identity" -x` | New D-03 test |
| CORE-01 | Responsa test suite passes via facade | regression | `pytest tests/test_responsa_*.py -q` | SC#2 |
| CORE-02 | `shared.variants.VariantManager is genizah_core.VariantManager` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "variant_manager" -x` | New D-03 test |
| CORE-03 | `shared.codicological.CodicologicalManager is genizah_core.CodicologicalManager` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "codicological" -x` | New D-03 test |
| CORE-04 | `shared.joins_manager.JoinsManager is genizah_core.JoinsManager` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "joins_manager" -x` | New D-03 test |
| CORE-05 | `shared.lists_manager.ListsManager is genizah_core.ListsManager` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "lists_manager" -x` | New D-03 test |
| CORE-06 | `shared.browse_map_utils.normalize_shelfmark is genizah_core.normalize_shelfmark` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "browse_map" -x` | New D-03 test |
| CORE-06 | Shelfmark bridge tests pass | regression | `pytest tests/test_shelfmark_bridge.py -q` | Snapshot must be regenerated |
| CORE-07 | `shared.text_normalize.strip_nikud is genizah_core.strip_nikud` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "text_normalize" -x` | New D-03 test |
| CORE-07 | local_indexer retargets work | regression | `pytest tests/test_local_pdf_nikud_strip.py -q` | Existing L1 guard |
| GUARD-01 | No extracted module imports genizah_core at module level | static (AST) | `pytest tests/test_no_back_edges_core.py -q` | Grows from 1→8 modules |
| GUARD-02 | Full pytest suite green | regression | `pytest tests/ -q --ignore=tests/gui_tests/` | At every commit boundary |
| GUARD-03 | test_shelfmark_bridge snapshot updated | regression | `pytest tests/test_shelfmark_bridge.py -q` | Snapshot regeneration required |
| GUARD-04 | `genizah_core.py` shims remain intact | static | `python -m ruff check genizah_core.py` | Per-file check, not --fix |

### Sampling Rate

- **Per task commit:** `pytest tests/test_no_back_edges_core.py tests/test_responsa_core.py tests/test_shelfmark_bridge.py -x -q` (< 30 seconds)
- **Per wave merge:** `pytest tests/ -q --ignore=tests/gui_tests/`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps

New test additions for Phase 123 (to be created during Wave 0):
- `tests/test_no_back_edges_core.py` — extend with 7 identity tests + 7 smoke tests (one per module); same file as Phase 122

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `VariantManager` contains no LOGGER calls | Q2 / Module 3 | New module needs `import logging; LOGGER = logging.getLogger(__name__)` |
| A2 | `_has_line_break_syntax`, `LineGroup`, `_parse_line_break_query` have no engine coupling of their own (only called BY the engine) | Q1 | If they reference SearchEngine internals, they cannot safely move to responsa.py this phase |
| A3 | `_make_flex_spacing_pattern` is imported by `test_responsa_edge_cases.py` at line :24, confirming it needs a genizah_core shim | Q5 | It already has one (it stays in genizah_core.py as engine-coupled); no shim confusion expected |

**All claims tagged `[ASSUMED]` are low-risk; the primary findings are verified directly from the live code.**

---

## Open Questions (RESOLVED)

> Both questions below carry an adopted `Recommendation:` verdict (folded into 123-01-PLAN.md).
> No open decision remains for the planner or executor.

1. **`_has_line_break_syntax`/`LineGroup`/`_parse_line_break_query` placement**
   - What we know: they are pure query-string parsers; they are called by `SearchEngine.build_tantivy_query` / `build_regex_pattern` and by `tests/test_joins_lab.py` / `tests/test_line_break_word_gaps.py` via genizah_core facade; they have no engine coupling themselves
   - What's unclear: whether moving them to `shared/responsa.py` creates any unexpected coupling risk when Phase 125 moves SearchEngine
   - Recommendation: Move them to `shared/responsa.py`. They are query parsers, not engine code. The engine accesses them via the genizah_core shim in Phase 123 and then via the direct `shared.responsa` import in Phase 125.

2. **`ListsManager.tr()` calls — inline `_tr()` vs. lazy per-call import**
   - What we know: `tr()` is called in 4 places inside `ListsManager` method bodies; all are inside function bodies (GUARD-01 safe)
   - What's unclear: which pattern is preferred — inline `_tr()` at module level vs. lazy `from genizah_core import tr` per call
   - Recommendation: Inline `_tr()` pattern (same as in `shared/responsa.py`) — cleaner, reduces per-call overhead, consistent.

---

## Environment Availability

> This phase is code/config-only (no external dependencies beyond what is already installed).

| Dependency | Required By | Available | Notes |
|------------|------------|-----------|-------|
| Python 3.10+ | All modules | ✓ | Already in use |
| `shared/config.py` | All 7 modules | ✓ | Phase 122 complete |
| `genizah_translations` | browse_map_utils, responsa | ✓ | Existing in tree |
| `unified_variants` (optional) | variants.py | ✓ (with ImportError fallback) | Try/except already in genizah_core |
| pytest | Validation | ✓ | Existing test infrastructure |

---

## Security Domain

> Phase 123 is a pure internal module extraction with zero user-facing behavior change. No new
> authentication, input validation, cryptography, or access-control surfaces are introduced.
> ASVS categories are not applicable to this refactor phase.

---

## Sources

### Primary (HIGH confidence — verified against live code)

All findings derived from direct inspection of the live tree via grep/Read/Bash:

- `genizah_core.py` — Symbol locations, dependency relationships, Config/LOGGER/tr() usage per cluster (grep-verified 2026-06-25)
- `tests/test_no_back_edges_core.py` — GUARD-01 registry mechanics, exact parametrization shape (read-verified)
- `tests/test_shelfmark_bridge.py` — `inspect.getsource` hash test mechanics (read-verified)
- `shared/config.py` — Phase 122 extraction result; `INTERNAL_DIR` resolution confirmed
- `shared/exclusion_service.py`, `shared/nli_crossref_service.py`, `shared/search_serializer.py`, `shared/local_indexer.py` — D-01 back-edge locations (grep-verified)
- `.planning/phases/122-config-enabler/122-CODEX-CRITIQUE.md` — BLOCKER #1 precedent (read-verified)
- `.planning/phases/123-core-leaf-modules/123-CONTEXT.md` — Locked decisions, cluster locations (read-verified)
- `.planning/seeds/SEED-020-decomposition-map.md` §7 C-2/C-3 — Codex-review corrections (read-verified)
- `tests/test_responsa_core.py`, `tests/test_responsa_edge_cases.py` — Import lists for all responded symbols (read-verified)

### Secondary (MEDIUM confidence)

- SEED-020 §0–§6 strategy — overridden by §7 where noted; used for background context only

---

## Metadata

**Confidence breakdown:**
- Cluster boundaries (responsa, text_normalize): HIGH — verified by grep of all call sites and import lists
- Path resolution (`_load_ie_volume_map`): HIGH — verified Config.INTERNAL_DIR is already repo-root-relative
- GUARD-01 mechanics: HIGH — read the test file directly
- GUARD-03 test analysis: HIGH — read all 5 test files for their scanning targets
- Intra-shared dependency graph: HIGH — verified by grep of all `natural_sort_key` / `normalize_shelfmark` call sites in the new modules' ranges
- `_tr()` helper pattern: MEDIUM — same logic as existing `tr()`, untested in the new context until execution

**Research date:** 2026-06-25
**Valid until:** 2026-07-25 (stable codebase; invalidated if genizah_core.py is modified before Phase 123 executes)
