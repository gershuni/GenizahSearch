# Phase 124: Core Metadata & Index — Research

**Researched:** 2026-06-26
**Domain:** Python module extraction / god-file decomposition — MetadataManager + Indexer
**Confidence:** HIGH

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **CORE-08:** Extract `MetadataManager` (+ `_BoundedLRUCache`) to `shared/metadata_manager.py`
  with `_BoundedLRUCache` co-located in the same module.
- **CORE-09:** Extract `Indexer` to `shared/indexer.py`.
- **GUARD-04 (permanent facade):** genizah_core re-exports both as same-object `# noqa: F401` shims;
  `from genizah_core import MetadataManager/Indexer` must keep resolving to the same class objects.
  Never delete genizah_core shims.
- **GUARD-01 (no back-edges):** No module-level import from the new shared modules back into
  genizah_core. MetadataManager has heavy outbound deps; any genizah_core dependency must be a
  lazy/function-local import.
- **local_indexer retarget:** Retarget `shared/local_indexer.py`'s lazy back-edges into genizah_core
  helpers as part of this phase (per roadmap goal). **NOTE: already complete after Phase 123 — zero
  genizah_core back-edges remain in shared/local_indexer.py.** The CONTEXT.md note is obsolete; the
  retarget described happened in Phase 123. No new local_indexer retargets needed this phase.
- **GUARD-02:** These integration tests must pass unchanged:
  `tests/test_browse_synthetic.py`, `tests/test_audit_followup_2026_05_29.py`,
  `tests/test_api_nli_breaker_integration.py`; `build_index.py` must still resolve `Indexer.create_index`.
- **Process:** derive imports from actual copied bodies; ruff F401 + full-suite-green are the
  two-sided gate; per-file ruff only; run 3-round Codex convergence loop post-execution.

### Claude's Discretion

- Plan decomposition: 1 plan (both classes) vs 2 (metadata_manager then indexer) — planner decides.

### Deferred Ideas (OUT OF SCOPE)

None.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CORE-08 | `MetadataManager` (+ `_BoundedLRUCache`) extracted to `shared/metadata_manager.py` | Full symbol inventory, module-level deps, import list, GUARD-01 handling documented below |
| CORE-09 | `Indexer` extracted to `shared/indexer.py` | Full symbol inventory, dep graph, `tr()` and `_strip_brackets` handling documented below |
| GUARD-02 | Zero behavior change — full suite green at every commit boundary | Integration test inventory + GUARD-03 retarget identified |
| GUARD-03 | Source-scanning test retarget before deletion | `test_desktop_folio_navigation.py` fixture retarget identified |
| GUARD-04 | genizah_core compat facade preserved | Full facade symbol list including `_BoundedLRUCache` documented |
</phase_requirements>

---

## Summary

Phase 124 extracts the two remaining heavy classes from `genizah_core.py` — `MetadataManager`
(~1,714 lines, 4 co-located module-level helpers + `_BoundedLRUCache`) and `Indexer` (~440 lines)
— into `shared/metadata_manager.py` and `shared/indexer.py` respectively, behind permanent
`# noqa: F401` re-export shims. This is the same copy→shim→retarget recipe validated in Phase 123.

`MetadataManager` is the most dependency-entangled class in the codebase. Its direct outbound
dependencies include `shared/nli_circuit_breaker.py` (already in `shared/`), `CodicologicalManager`
(already in `shared/codicological.py`), `normalize_shelfmark` (already in `shared/browse_map_utils`),
`Config` (already in `shared/config.py`), and several lazy-loaded sidecar services
(`_get_crossref_service`, `_get_fjms_service`) that access `shared/nli_crossref_service` and
`shared/fjms_service` at call time. Every dependency the class needs already lives in `shared/`, the
stdlib, or can be imported lazily at method body scope — there are zero required back-edges into
`genizah_core`.

`Indexer` is smaller and cleaner. Its key dependencies are `tantivy` (third-party), `register_search_tokenizers`
(in `shared/search_tokenizer.py`), `strip_search_diacritics` (in `shared/text_normalize.py`),
`dedupe_browse_map` (in `shared/browse_map_utils.py`), and `Config` (in `shared/config.py`). It calls
`tr()` in one place (needs the inline `_tr()` pattern) and calls `_strip_brackets` in one place
(simple 2-line function, inline in the new module). Both dependencies are GUARD-01-clean.

One GUARD-03 retarget is required: `tests/test_desktop_folio_navigation.py` reads `genizah_core.py`
source directly to find `enrich_metadata` via string extraction — after the move the method is in
`shared/metadata_manager.py`, so the fixture path must be updated before or in this phase.

**Primary recommendation:** One plan, two commits (metadata_manager first, indexer second), both
touching `genizah_core.py` so serialization is necessary anyway.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| NLI metadata fetch + IIIF/MARC enrichment | API/Backend | — | HTTP calls, threading; stays in shared/ |
| CSV/library catalog loading | Backend | — | Disk I/O at startup; shared between web and desktop |
| Tantivy index construction | Backend | — | Disk + engine operation; one-off admin tool |
| Browse-map assembly | Backend | — | Downstream of index build; shared |
| On-disk cache (nli_cache.pkl, meta_cache.pkl) | Backend | — | Persistence layer |

---

## Standard Stack

### Core — already in the project (no new installs)

| Library | Purpose | Already in shared/? |
|---------|---------|---------------------|
| `shared/nli_circuit_breaker` | NLI outage protection | YES [VERIFIED: codebase grep] |
| `shared/codicological` | CodicologicalManager | YES — Phase 123 |
| `shared/browse_map_utils` | normalize_shelfmark, dedupe_browse_map | YES — Phase 123 |
| `shared/text_normalize` | strip_search_diacritics | YES — Phase 123 |
| `shared/config` | Config | YES — Phase 122 |
| `shared/search_tokenizer` | register_search_tokenizers | YES [VERIFIED: codebase grep] |
| `tantivy` | Indexer needs tantivy.SchemaBuilder, tantivy.Index, tantivy.Document | Already a project dep |
| `requests` | MetadataManager HTTP calls | Already a project dep |

**Installation:** No new packages required. [VERIFIED: codebase dependency scan]

---

## Package Legitimacy Audit

Not applicable — no new packages installed in this phase.

---

## Architecture Patterns

### Proven Recipe (Phase 122/123)

1. Copy implementation VERBATIM into new `shared/X.py` — NO module-level `import genizah_core`.
2. In `genizah_core.py` REPLACE originals with `from shared.X import ... # noqa: F401`.
3. Python import caching guarantees `shared.X.Y is genizah_core.Y` (same object identity).
4. Per-file `python -m ruff check <file>` — NEVER repo-wide `ruff check . --fix`.
5. Full suite green at every commit boundary; Codex 3-round convergence post-execution.

### Recommended Project Structure

```
shared/
├── metadata_manager.py   # _BoundedLRUCache + MetadataManager (CORE-08)
├── indexer.py            # Indexer class (CORE-09)
└── ... (Phase 122/123 modules already in place)
```

---

## Q1: MetadataManager Dependency Graph

### Line ranges (grep-verified, 2026-06-26)
[VERIFIED: codebase grep]

- `_CUDL_LABEL_RE` pattern — line 2100
- `_parse_cudl_label` function — lines 2103-2132
- `_BRIDGE_IMPORT_WARNED` + `_warn_bridge_import_failed` — lines 2171-2179
- `_nli_crossref_svc` + `_get_crossref_service` — lines 2281-2292
- `_fjms_svc` + `_get_fjms_service` — lines 2298-2309
- `_NLI_CACHE_MAX_ENTRIES` — line 2332
- `class _BoundedLRUCache` — lines 2335-2413
- `MARC_FUTURE_TIMEOUT`, `NLI_IIIF_FUTURE_TIMEOUT`, `EXTERNAL_IIIF_HTTP_TIMEOUT` — lines 2421-2423
- `class MetadataManager` — lines 2426-4139

**Total extraction block: lines 2100 to 4139** (all 8 items above move together; they form one cohesive cluster — the helpers are ONLY called within MetadataManager methods and nowhere else in genizah_core.)

### Class-definition time deps (must be module-level imports)

| Dependency | Where | Handling |
|-----------|-------|---------|
| `os` | `__init__` body + class body | stdlib — module-level import |
| `re` | `_CUDL_LABEL_RE`, `extract_unique_id`, `_fetch_single_worker`, etc. | stdlib — module-level import |
| `threading` | `__init__` body (RLock in `_BoundedLRUCache`) | stdlib — module-level import |
| `time` | `fetch_iiif_manifest`, `_fetch_single_worker`, etc. | stdlib — lazy inside methods (already local `import time` in most) OR module-level; check ruff |
| `pickle` | `_load_small_caches`, `save_caches`, `_build_file_map_background` | stdlib — module-level import |
| `requests` | `_make_session`, all HTTP methods | third-party — module-level import |
| `xml.etree.ElementTree as ET` | `fetch_marc_data`, `_fetch_single_worker` | stdlib — module-level import |
| `json` | `enrich_metadata` (via cambridge alignment path) | stdlib — module-level import |
| `collections.OrderedDict` | `_BoundedLRUCache._data`, `_iiif_manifest_cache` | stdlib — module-level import |
| `concurrent.futures.ThreadPoolExecutor` | `__init__`, `enrich_metadata` | stdlib — module-level import |
| `concurrent.futures.as_completed` | `batch_fetch_shelfmarks` | stdlib — module-level import |
| `shared.config.Config` | `__init__` + many methods | `shared/` — module-level import, GUARD-01 clean |
| `shared.nli_circuit_breaker` (`is_open`, `record_failure`, `record_success`, `NLI_CONNECT_TIMEOUT`, `NLI_IIIF_READ_TIMEOUT`, `NLI_MARC_READ_TIMEOUT`) | `fetch_iiif_manifest`, `fetch_marc_data`, `_fetch_single_worker`, `_fetch_fl_ids` | `shared/` — module-level import, GUARD-01 clean |
| `shared.codicological.CodicologicalManager` | `__init__`: `self.codico_mgr = CodicologicalManager()` | `shared/` — module-level import, GUARD-01 clean |
| `shared.browse_map_utils.normalize_shelfmark` | `enrich_metadata` (line 3408), `_normalize_shelfmark` | `shared/` — module-level import, GUARD-01 clean |
| `LOGGER` | everywhere | define `LOGGER = logging.getLogger("genizah.shared.metadata_manager")` (Phase 123 pattern — use `"genizah." + __name__` to stay in propagation tree) |

### Method-runtime lazy deps (lazy import inside function body — GUARD-01 safe)

| Dependency | Method | Pattern |
|-----------|-------|---------|
| `CURRENT_LANG` (genizah_core module-level var) | NOT NEEDED — MetadataManager has no `tr()` calls | N/A |
| `shared.synthetic_sys_id.is_synthetic_sys_id` | `fetch_iiif_manifest`, `fetch_marc_data`, `_fetch_single_worker` | already a lazy import in current code: `from shared.synthetic_sys_id import is_synthetic_sys_id` — copy verbatim |
| `shared.nli_crossref_service.NliCrossrefService` | `_get_crossref_service` | already lazy: `from shared.nli_crossref_service import NliCrossrefService` — copy verbatim |
| `shared.fjms_service.FjmsService` | `_get_fjms_service` | already lazy: `from shared.fjms_service import FjmsService` — copy verbatim |
| `shared.shelfmark_bridge.lookup_cudl`, `shared.shelfmark_bridge.build_alias_index` | `resolve_system_by_shelfmark`, `_load_csv_bank` | already lazy — copy verbatim |
| `shared.nli_crossref_service.classify_cambridge_alignment` | `enrich_metadata` | already lazy — copy verbatim |
| `csv` module | `_load_csv_bank` | already a local `import csv` inside the method — keep as-is |

### Dependencies that would create back-edges (DO NOT import at module level)

None found. There are no module-level genizah_core symbols MetadataManager uses that have not already been extracted to `shared/`. [VERIFIED: codebase grep — all module-level symbols used by MetadataManager are either stdlib, requests, or already-extracted shared/ modules.]

**Key finding:** `MetadataManager` is GUARD-01 clean — no back-edge needed. Its outbound deps are all already in `shared/` or stdlib/third-party.

---

## Q2: _BoundedLRUCache

- **Defined at:** lines 2335-2413 (78 lines) [VERIFIED: grep line 2335]
- **External callers:** `tests/test_nli_cache_bounded_lru.py` imports it via `from genizah_core import _BoundedLRUCache` [VERIFIED: grep]
- **Internal callers:** `MetadataManager.__init__` (line 2434), `MetadataManager._load_small_caches` (lines 2465, 2468)
- **Co-location decision (LOCKED):** Moves together with MetadataManager into `shared/metadata_manager.py`
- **Facade requirement (GUARD-04):** genizah_core must re-export `_BoundedLRUCache` — `from shared.metadata_manager import _BoundedLRUCache  # noqa: F401`
- **deps:** `OrderedDict` (stdlib `collections`), `threading.RLock` (stdlib) — all stdlib, no genizah_core
- **Module-level constant:** `_NLI_CACHE_MAX_ENTRIES = max(0, int(os.environ.get('NLI_CACHE_MAX_ENTRIES', '75000')))` must move with the class (used as the default `maxsize` argument in `_BoundedLRUCache.__init__`)

---

## Q3: Indexer Dependency Graph

**Class range:** lines 4143-4583 (440 lines) [VERIFIED: grep]

### Module-level imports needed for shared/indexer.py

| Dependency | Where used | Handling |
|-----------|-----------|---------|
| `os` | `create_index`, `_add_continuous_document`, etc. | stdlib — module-level |
| `re` | `create_index` (`word_pattern = re.compile(...)`) | stdlib — module-level |
| `shutil` | `create_index` (`shutil.rmtree`) | stdlib — module-level |
| `json` | `_add_continuous_document` (`json.dumps`) | stdlib — module-level |
| `collections.defaultdict` | `create_index` (`browse_map = defaultdict(list)`) | stdlib — module-level |
| `pickle` | `create_index` (at end: `pickle.dump(browse_map, f)`) | stdlib — module-level |
| `tantivy` | `create_index`, `_add_continuous_document` | third-party — module-level |
| `shared.config.Config` | `create_index` (FILE_V8, INDEX_DIR, BROWSE_MAP, WORD_TOKEN_PATTERN) | `shared/` — module-level, GUARD-01 clean |
| `shared.search_tokenizer.register_search_tokenizers` | `create_index` | `shared/` — module-level, GUARD-01 clean |
| `shared.text_normalize.strip_search_diacritics` | `create_index`, `_add_continuous_document` | `shared/` — module-level, GUARD-01 clean |
| `shared.browse_map_utils.dedupe_browse_map` | `create_index` | `shared/` — module-level, GUARD-01 clean |
| `LOGGER` | `create_index` | define `LOGGER = logging.getLogger("genizah.shared.indexer")` |

### Two specific pitfalls in Indexer

**Pitfall A — `tr()` call in `create_index`:** Line 4319:
```python
raise FileNotFoundError(tr("Input file not found: {}...").format(Config.FILE_V8))
```
`tr()` is defined in `genizah_core` and cannot be imported at module level (GUARD-01). Use the same
inline `_tr(text)` helper pattern proven in Phase 123 (`shared/responsa.py`, `shared/lists_manager.py`):
```python
def _tr(text: str) -> str:
    from genizah_core import CURRENT_LANG  # noqa: PLC0415
    from genizah_translations import TRANSLATIONS
    return TRANSLATIONS.get(text, text) if CURRENT_LANG == 'he' else text
```
Replace `tr(...)` in `create_index` with `_tr(...)`.

**Pitfall B — `_strip_brackets` call in `_validate_position_match`:** Line 4192:
```python
return _strip_brackets(s).strip() if strip_brackets else s.strip()
```
`_strip_brackets` is a genizah_core RESPONSA REGEX HELPER (line 4690, stays in genizah_core per
Phase 123 decision). It is 2 lines:
```python
def _strip_brackets(text: str) -> str:
    return text.replace('[', '').replace(']', '')
```
**Resolution:** Inline a private copy directly in `shared/indexer.py` as `_strip_brackets`:
```python
def _strip_brackets(text: str) -> str:
    """Remove all square brackets from *text*."""
    return text.replace('[', '').replace(']', '')
```
This avoids any back-edge into genizah_core and is zero-behavior-change (identical logic). It is a
small, pure utility. No facade needed for the genizah_core copy (it's still there for SearchEngine).

### build_index.py coupling

`build_index.py` line 17: `from genizah_core import Indexer, MetadataManager, VariantManager, LabEngine, Config`

This imports via the genizah_core facade — GUARD-04 guarantees `genizah_core.Indexer` keeps
resolving to `shared.indexer.Indexer` after the shim lands. **No change to build_index.py needed.**
[VERIFIED: build_index.py uses facade import only, not `from shared.indexer import Indexer`]

---

## Q4: local_indexer Back-Edges — Already Resolved

[VERIFIED: grep of shared/local_indexer.py for `genizah_core` — zero matches]

The CONTEXT.md note about "retarget shared/local_indexer.py's lazy back-edges into genizah_core
helpers" referred to the Phase 123 CORE-07 work (retargeting `strip_nikud`/`strip_search_diacritics`).
That was completed in Phase 123 commit `1c3930d0`. After Phase 123, `shared/local_indexer.py`
has **zero** `from genizah_core import` back-edges. **Nothing to do for this phase.**

---

## Q5: Facade Completeness — Symbol List for genizah_core Shims

All symbols that must remain importable via `from genizah_core import ...` after Phase 124:

### From shared/metadata_manager.py (CORE-08 facade)

```python
from shared.metadata_manager import (  # noqa: F401
    _NLI_CACHE_MAX_ENTRIES,
    _BoundedLRUCache,
    MARC_FUTURE_TIMEOUT,
    NLI_IIIF_FUTURE_TIMEOUT,
    EXTERNAL_IIIF_HTTP_TIMEOUT,
    MetadataManager,
)
```

**Justification for each:**
- `_NLI_CACHE_MAX_ENTRIES`: module-level constant; moves with `_BoundedLRUCache`; include for completeness (GUARD-04 — any name importable from genizah_core pre-v8.3.0 stays importable)
- `_BoundedLRUCache`: imported by `tests/test_nli_cache_bounded_lru.py` [VERIFIED]; MUST be in facade
- `MARC_FUTURE_TIMEOUT`, `NLI_IIIF_FUTURE_TIMEOUT`, `EXTERNAL_IIIF_HTTP_TIMEOUT`: module-level timeout constants defined at lines 2421-2423; imported from genizah_core by at least `web/api.py` — verify with grep before finalizing (see note below)
- `MetadataManager`: class itself; imported by web/main.py, web/state.py, genizah_app.py, gui_threads.py, build_index.py, many tests

**Note on helper functions (`_parse_cudl_label`, `_warn_bridge_import_failed`, `_get_crossref_service`, `_get_fjms_service`, `_CUDL_LABEL_RE`, `_nli_crossref_svc`, `_fjms_svc`, `_BRIDGE_IMPORT_WARNED`):** These are private helpers that move as part of the MetadataManager cluster. None of them are imported from genizah_core by external code (verified by grep — zero external imports). They do NOT need facade re-exports. The `_get_crossref_service` / `_get_fjms_service` singletons (`_nli_crossref_svc`, `_fjms_svc`) are process-scoped state — they must live in `shared/metadata_manager.py` where MetadataManager's methods call them.

**CRITICAL facade name check:** Run a systematic module-level name diff (base vs HEAD) immediately after Phase 124 commits — this is how Phase 123 caught the `UNIFIED_VARIANT_PAIRS`/`get_top_pairs`/`LIBRARY_CODES_HE` drops. Check: does genizah_core currently export `MARC_FUTURE_TIMEOUT`, `NLI_IIIF_FUTURE_TIMEOUT`, `EXTERNAL_IIIF_HTTP_TIMEOUT` as public names? Grep external code first:
```bash
grep -rn "from genizah_core import.*MARC_FUTURE\|from genizah_core import.*NLI_IIIF_FUTURE\|from genizah_core import.*EXTERNAL_IIIF" --include="*.py" .
```
If no external code imports them, they still need the shim (GUARD-04: any previously importable name stays importable). If they were importable before, they stay importable.

### From shared/indexer.py (CORE-09 facade)

```python
from shared.indexer import Indexer  # noqa: F401
```

Only `Indexer` is imported externally. The static `_validate_position_match` and
`_validate_line_break_match` methods are used only by SearchEngine (Phase 125 concern via inheritance
or delegation) — verify they remain accessible. [ASSUMED — verify with grep before plan finalizes]

---

## Q6: GUARD-03 Source-Scanning Tests — Phase 124 Assessment

The 5 named GUARD-03 tests: [VERIFIED: grep + source inspection]

| Test | What it scans | Phase 124 impact |
|------|--------------|-----------------|
| `test_desktop_folio_navigation.py` | Reads **`genizah_core.py` source** to find `enrich_metadata` method | **RETARGET NEEDED** — must update `genizah_core_source` fixture to read `shared/metadata_manager.py` |
| `test_wr01_open_local_browse_page_ast.py` | Reads `genizah_app.py` source | No impact |
| `test_tabular_builder_rtl.py` | Reads `genizah_app.py` AST | No impact |
| `test_view_all_cap.py` | Reads `genizah_app.py` AST | No impact |
| `test_shelfmark_bridge.py` | Source hash of `normalize_shelfmark` (in shared/browse_map_utils) + `MetadataManager` via `from genizah_core import MetadataManager` | `MetadataManager` import keeps working via facade shim — NO retarget needed for this aspect |

**`test_desktop_folio_navigation.py` detail:**
- Fixture `genizah_core_source()` (line 47) reads `genizah_core.py`
- 3 tests use it: `test_image_source_info_in_enrich_metadata` (line 169) calls `_extract_method(genizah_core_source, 'enrich_metadata')`; also line 171 extracts the method body and checks for `image_source_info`, `get_image_sources`, `folio_images`, `get_folio_images`, `sys_id`
- After extraction, `enrich_metadata` is in `shared/metadata_manager.py` — the string `'enrich_metadata'` will NOT be found in genizah_core.py source (only the import shim is there)
- **Fix:** Add a new fixture `metadata_manager_source()` that reads `shared/metadata_manager.py`, and update the 3 tests that use `genizah_core_source` for this method. The existing `genizah_core_source` fixture can remain (other tests may use it for other methods)
- **Timing:** Since genizah_core KEEPS the shim (no deletion this phase), technically the test WOULD fail because `_extract_method` looks for a `def enrich_metadata(` string in `genizah_core.py` — which won't be there after extraction. Therefore this retarget must happen IN the same commit as the MetadataManager extraction.

---

## Q7: Plan Decomposition Recommendation

**Recommendation: ONE plan, TWO commits (sequentialized)**

**Rationale:**
1. Both classes touch `genizah_core.py` — the shim additions must be sequential to avoid conflicts
2. MetadataManager is larger and has the GUARD-03 retarget — higher risk, goes first
3. Indexer is smaller, cleaner, no GUARD-03 work — goes second
4. Two commits (not one) keeps each commit's diff reviewable and ensures full-suite-green between them

**Commit order:**
- Commit 1: `shared/metadata_manager.py` + `_BoundedLRUCache` + genizah_core shims + GUARD-03 retarget of `test_desktop_folio_navigation.py` + GUARD-01 registry grown to 9 + identity/smoke tests
- Commit 2: `shared/indexer.py` + genizah_core shim + GUARD-01 registry grown to 10 + identity/smoke tests

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead |
|---------|-------------|-------------|
| GUARD-01 back-edge for `tr()` in Indexer | Custom translator mechanism | Inline `_tr()` helper (proven pattern from shared/responsa.py + shared/lists_manager.py) |
| `_strip_brackets` in Indexer | Cross-module import | Inline 2-line private copy in shared/indexer.py |
| CURRENT_LANG dependency | module-level `CURRENT_LANG` freeze | Lazy function-body import pattern (established in Phase 123) |
| Module-level mutable singletons (`_nli_crossref_svc`, `_fjms_svc`) | Re-architect | Move verbatim as module-level globals in shared/metadata_manager.py |

---

## Common Pitfalls

### Pitfall 1: Forgetting the MetadataManager pre-cluster (lines 2100-2423)

**What goes wrong:** Copying only `class MetadataManager` without the 7 module-level items that
precede it (from `_CUDL_LABEL_RE` at line 2100 to the timeout constants at line 2423). The class
uses `_NLI_CACHE_MAX_ENTRIES`, `_BoundedLRUCache`, `_CUDL_LABEL_RE`, `_parse_cudl_label`,
`_warn_bridge_import_failed`, `_BRIDGE_IMPORT_WARNED`, `_get_crossref_service`,
`_get_fjms_service` — all defined in this pre-cluster.
**How to avoid:** Copy the full block lines 2100-4139. Use grep to confirm all 8 items are present
in the new file before committing.

### Pitfall 2: Missing the _BoundedLRUCache facade shim

**What goes wrong:** `tests/test_nli_cache_bounded_lru.py` imports `_BoundedLRUCache` from
`genizah_core` — if the facade shim omits it, this test breaks on import.
**How to avoid:** Include `_BoundedLRUCache` in the genizah_core re-export shim.

### Pitfall 3: GUARD-03 failure — test_desktop_folio_navigation.py

**What goes wrong:** `_extract_method(genizah_core_source, 'enrich_metadata')` returns empty
string because `enrich_metadata` is now in `shared/metadata_manager.py`, not genizah_core.
Three tests assert against the extracted method body and will all fail with confusing "enrich_method
is empty" assertions.
**How to avoid:** Update the `genizah_core_source` fixture to `metadata_manager_source` reading
from `shared/metadata_manager.py` IN THE SAME COMMIT as the extraction.

### Pitfall 4: _strip_brackets back-edge

**What goes wrong:** `Indexer._validate_position_match` calls `_strip_brackets(s)` at line 4192.
A naive extraction adds `from genizah_core import _strip_brackets` at module level — GUARD-01 violation.
**How to avoid:** Inline a private `_strip_brackets` copy (2 lines) in `shared/indexer.py`.

### Pitfall 5: tr() back-edge in Indexer.create_index

**What goes wrong:** `create_index` line 4319 calls `tr(...)`. `tr` is defined in genizah_core.
A naive extraction adds `from genizah_core import tr` at module level — GUARD-01 violation.
**How to avoid:** Define inline `_tr()` helper with lazy `CURRENT_LANG` import inside the function
body (same pattern as shared/responsa.py and shared/lists_manager.py).

### Pitfall 6: Mutable global singletons not moving together

**What goes wrong:** `_nli_crossref_svc` and `_fjms_svc` are process-level singletons. If they
stay in genizah_core while `_get_crossref_service()`/`_get_fjms_service()` move to
shared/metadata_manager.py, the genizah_core version stays `None` but the new module's version
gets set — the global is process-scoped to the module where it lives.
**How to avoid:** Move `_nli_crossref_svc`, `_fjms_svc` AND their accessor functions together
as part of the MetadataManager cluster (they're already in the 2100-4139 block).

### Pitfall 7: Logging namespace regression

**What goes wrong:** Using `logging.getLogger(__name__)` in the new modules routes log output
under `shared.metadata_manager` / `shared.indexer`, bypassing the `genizah` logger tree's
`propagate=False` configuration (Phase 123 Round 1 LOW finding, fixed in commit `674d16b5`).
**How to avoid:** Use `logging.getLogger("genizah." + __name__)` as established in Phase 123 for
browse_map_utils, codicological, joins_manager, lists_manager.

### Pitfall 8: repo-wide ruff --fix strips shims

**What goes wrong:** `python -m ruff check . --fix` strips `# noqa: F401` shims.
**How to avoid:** Per-file ruff only: `python -m ruff check shared/metadata_manager.py shared/indexer.py genizah_core.py`.

---

## Code Examples

### Inline `_tr()` helper (proven pattern)

```python
# Source: shared/lists_manager.py, shared/responsa.py (Phase 123)
def _tr(text: str) -> str:
    from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
    from genizah_translations import TRANSLATIONS
    return TRANSLATIONS.get(text, text) if CURRENT_LANG == 'he' else text
```

### Inline `_strip_brackets` in shared/indexer.py

```python
def _strip_brackets(text: str) -> str:
    """Remove all square brackets from *text*. Mirrors genizah_core._strip_brackets."""
    return text.replace('[', '').replace(']', '')
```

### Identity test pattern (from test_no_back_edges_core.py)

```python
def test_metadata_manager_identity():
    import shared.metadata_manager
    import genizah_core
    assert shared.metadata_manager.MetadataManager is genizah_core.MetadataManager
    assert shared.metadata_manager._BoundedLRUCache is genizah_core._BoundedLRUCache

def test_indexer_identity():
    import shared.indexer
    import genizah_core
    assert shared.indexer.Indexer is genizah_core.Indexer
```

### Standalone smoke test pattern

```python
def test_metadata_manager_standalone_import():
    import shared.metadata_manager
    assert hasattr(shared.metadata_manager, 'MetadataManager')
    assert hasattr(shared.metadata_manager, '_BoundedLRUCache')
    # Smoke: _BoundedLRUCache instantiates
    c = shared.metadata_manager._BoundedLRUCache(maxsize=10)
    assert len(c) == 0

def test_indexer_standalone_import():
    import shared.indexer
    assert hasattr(shared.indexer, 'Indexer')
    # Smoke: Indexer instantiates with a mock meta_mgr
    class _FakeMM:
        pass
    idx = shared.indexer.Indexer(_FakeMM())
    assert idx.meta_mgr is not None
```

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | none — pytest.ini or pyproject.toml discovered automatically |
| Quick run command | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_no_back_edges_core.py tests/test_nli_cache_bounded_lru.py tests/test_browse_synthetic.py -x -q` |
| Full suite command | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -q` (no `-n auto`) |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| CORE-08 | `shared.metadata_manager.MetadataManager is genizah_core.MetadataManager` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "metadata_manager" -x -q` | Wave 0 |
| CORE-08 | `_BoundedLRUCache` importable from genizah_core and shared | unit (identity) | same | Wave 0 |
| CORE-08 | `shared.metadata_manager` imports without pulling genizah_core at module level | unit (GUARD-01) | `pytest tests/test_no_back_edges_core.py -x -q` | Wave 0 (EXTRACTED_MODULES) |
| CORE-09 | `shared.indexer.Indexer is genizah_core.Indexer` | unit (identity) | `pytest tests/test_no_back_edges_core.py -k "indexer" -x -q` | Wave 0 |
| GUARD-02 | NLI circuit breaker integration unchanged | integration | `pytest tests/test_api_nli_breaker_integration.py -q` | YES |
| GUARD-02 | MetadataManager IIIF/MARC fetch unchanged | integration | `pytest tests/test_browse_synthetic.py tests/test_audit_followup_2026_05_29.py -q` | YES |
| GUARD-02 | _BoundedLRUCache behavior unchanged | unit | `pytest tests/test_nli_cache_bounded_lru.py -q` | YES |
| GUARD-03 | enrich_metadata test reads from shared/metadata_manager.py | unit (AST/source) | `pytest tests/test_desktop_folio_navigation.py -q` | YES (retargeted) |
| GUARD-04 | genizah_core shims not stripped | lint | `python -m ruff check genizah_core.py` | N/A (gate check) |

### Sampling Rate

- **Per-commit gate:** `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_no_back_edges_core.py tests/test_nli_cache_bounded_lru.py tests/test_browse_synthetic.py tests/test_audit_followup_2026_05_29.py tests/test_desktop_folio_navigation.py tests/test_api_nli_breaker_integration.py -x -q`
- **Phase gate (before /gsd-verify-work):** Full suite green

### Wave 0 Gaps

- [ ] `tests/test_no_back_edges_core.py` — add `"shared/metadata_manager.py"` and `"shared/indexer.py"` to `EXTRACTED_MODULES`; add `test_metadata_manager_identity`, `test_metadata_manager_standalone_import`, `test_indexer_identity`, `test_indexer_standalone_import`
- [ ] `tests/test_desktop_folio_navigation.py` — add `metadata_manager_source()` fixture reading `shared/metadata_manager.py`; update 3 tests that use `genizah_core_source` to use `metadata_manager_source`

---

## Security Domain

Phase 124 is a pure mechanical refactor — zero behavior change (GUARD-02). No new authentication,
input-validation, cryptography, network, or access-control surface is introduced. Same code runs
in the same places; only its file location changes. All ASVS categories: N/A.

---

## Runtime State Inventory

Not applicable (greenfield module creation, no rename/refactor).

---

## Environment Availability

Not applicable (no new external tools required; tantivy and requests are pre-existing deps).

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `MARC_FUTURE_TIMEOUT`, `NLI_IIIF_FUTURE_TIMEOUT`, `EXTERNAL_IIIF_HTTP_TIMEOUT` have no external importers (not imported from genizah_core by web/api.py or other callers) | Q5 Facade | If imported externally, facade shim is mandatory; grep is needed before coding |
| A2 | `Indexer._validate_position_match` and `_validate_line_break_match` static methods are only called by SearchEngine (Phase 125) and not by any module that would need to import them from `shared.indexer` directly | Q3 | If other callers exist, add them to facade |
| A3 | The `test_desktop_folio_navigation.py` fixture needs updating because `_extract_method` does string-level scanning of source files; it will fail when `enrich_metadata` method text is absent from genizah_core.py | Q6 GUARD-03 | If test uses `inspect.getsource` (not string scan), the shim would auto-follow to shared/; check test implementation before coding |

**Notes on A1:** Grep command for executor to run:
```bash
grep -rn "from genizah_core import.*MARC_FUTURE\|from genizah_core import.*NLI_IIIF_FUTURE\|from genizah_core import.*EXTERNAL_IIIF" --include="*.py" .
```
If any match: add those names to facade. If no match: still add them (GUARD-04 is conservative — previously importable = stays importable).

**Notes on A3:** The test fixture at line 47-51 reads the file as a string and uses `_extract_method()` which does regex search for `def enrich_metadata(` in the string. This WILL fail unless retargeted. [VERIFIED: source inspection of test fixture]

---

## Open Questions

1. **Do MARC_FUTURE_TIMEOUT etc. need facade shims?**
   - What we know: they're defined at lines 2421-2423, between the service singletons and MetadataManager
   - What's unclear: whether any external code imports them directly from genizah_core
   - Recommendation: run the grep in Q5 before coding; add them to facade regardless (conservative)

2. **Does SearchEngine directly call Indexer static methods?**
   - What we know: `_validate_position_match` and `_validate_line_break_match` are static methods on Indexer, and SearchEngine calls them
   - What's unclear: does SearchEngine do `Indexer._validate_position_match(...)` (would need facade entry) or does it have its own copy?
   - Recommendation: grep SearchEngine body for `Indexer._validate_position_match` before coding

---

## Sources

### Primary (HIGH confidence)
- genizah_core.py — direct source inspection; all line numbers verified 2026-06-26
- tests/test_no_back_edges_core.py — current EXTRACTED_MODULES registry and identity test patterns
- tests/test_nli_cache_bounded_lru.py — `_BoundedLRUCache` import path
- tests/test_desktop_folio_navigation.py — GUARD-03 retarget need
- shared/local_indexer.py — zero back-edge confirmation
- 123-01-PLAN.md + 123-01-SUMMARY.md — proven recipe + Codex convergence record
- 124-CONTEXT.md — locked decisions
- .planning/REQUIREMENTS.md — CORE-08, CORE-09, GUARD-01 through GUARD-04

### Secondary (MEDIUM confidence)
- build_index.py — Indexer.create_index coupling verified

---

## Metadata

**Confidence breakdown:**
- MetadataManager dep graph: HIGH — verified via direct source inspection
- Indexer dep graph: HIGH — verified via direct source inspection
- Pitfalls: HIGH — derived from Phase 123 SUMMARY Codex convergence record + new code analysis
- GUARD-03 retarget: HIGH — source inspection of test fixture confirmed string-scan mechanism
- local_indexer back-edges: HIGH — grep confirmed zero remaining back-edges

**Research date:** 2026-06-26
**Valid until:** 2026-07-26 (stable code, 30-day validity)
