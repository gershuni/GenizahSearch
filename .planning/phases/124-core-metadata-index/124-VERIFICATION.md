---
phase: 124-core-metadata-index
verified: 2026-06-26T07:30:00Z
status: passed
score: 7/7 must-haves verified
overrides_applied: 0
---

# Phase 124: Core Metadata & Index Extraction Verification Report

**Phase Goal:** `MetadataManager` (and `_BoundedLRUCache`) are extracted to `shared/metadata_manager.py`, and `Indexer` is extracted to `shared/indexer.py`. These depend on `shared/config.py` (Phase 122) and are prerequisites for the engine moves in Phase 125.
**Verified:** 2026-06-26T07:30:00Z
**Status:** passed
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | `from shared.metadata_manager import MetadataManager` resolves; `genizah_core.MetadataManager is shared.metadata_manager.MetadataManager` (same object via `# noqa: F401` shim) (CORE-08, GUARD-04) | VERIFIED | `python -c "assert shared.metadata_manager.MetadataManager is genizah_core.MetadataManager"` exits 0; live Python identity check PASS |
| 2 | `from shared.metadata_manager import _BoundedLRUCache` resolves; `genizah_core._BoundedLRUCache` is the same object; `tests/test_nli_cache_bounded_lru.py` green via the facade (CORE-08) | VERIFIED | Identity check PASS; 10/10 `test_nli_cache_bounded_lru.py` tests pass |
| 3 | `from shared.indexer import Indexer` resolves; `genizah_core.Indexer is shared.indexer.Indexer` (same object); `build_index.py` resolves `Indexer.create_index` and `SearchEngine` resolves `Indexer._validate_position_match` (CORE-09, GUARD-02, GUARD-04) | VERIFIED | Identity check PASS; `build_index.Indexer.create_index` hasattr PASS; `_validate_position_match` identity PASS |
| 4 | Neither `shared/metadata_manager.py` nor `shared/indexer.py` imports `genizah_core` at module level (GUARD-01); GUARD-01 AST registry grows 8→10 and stays green | VERIFIED | `grep -n "^import genizah_core\|^from genizah_core import" shared/metadata_manager.py shared/indexer.py` returns empty; `tests/test_no_back_edges_core.py` 32/32 tests pass; EXTRACTED_MODULES confirmed at 10 entries |
| 5 | `shared/indexer.py` is GUARD-01-clean via inline `_tr()` (lazy `CURRENT_LANG` import) and inline private `_strip_brackets` copy — no module-level genizah_core back-edge | VERIFIED | `_tr()` at line 38 uses `from genizah_core import CURRENT_LANG` inside function body only; `_strip_brackets` at line 51 is a standalone 2-line function with no genizah_core dependency; ruff clean |
| 6 | `tests/test_desktop_folio_navigation.py` reads `enrich_metadata` from `shared/metadata_manager.py` (GUARD-03 retarget) and is green | VERIFIED | `metadata_manager_source()` fixture at line 55 reads `shared/metadata_manager.py`; `test_image_source_info_in_enrich_metadata` takes `metadata_manager_source`; 9/9 tests pass |
| 7 | All named GUARD-02 integration tests pass unchanged; full suite has zero new failures attributable to Phase 124 | VERIFIED | `tests/test_browse_synthetic.py` + `tests/test_audit_followup_2026_05_29.py` + `tests/test_api_nli_breaker_integration.py` = 56/56 pass; GUARD-02 fast gate (107 tests) = 107/107 pass; bulk suite has 8 pre-existing failures (all confirmed red at base `e6714343`; see note below) + 6 environment failures (empty Tantivy index) — zero new failures |

**Score:** 7/7 truths verified

---

### Note on Bulk Suite Failures

The bulk suite (non-GUI, non-render_smoke) shows 14 failures total. All are pre-existing at base commit `e6714343`:

**Pre-existing code failures (7 — forward-looking LabEngine/composition territory, Phase 125/SEED-011):**
- `test_audit_2026_06_23_guards.py::test_lab_composition_search_dedup_swallows_now_log`
- `test_audit_2026_06_23_guards.py::test_lab_composition_search_local_lab_scan_logs_exc_info`
- `test_local_lab_invalidation.py::TestLabCompositionSearchLocalLab::test_search_composition_logic_extends_regular_local_query`
- `test_local_lab_invalidation.py::TestLabCompositionSearchLocalLab::test_lab_composition_search_extends_local_lab_query`
- `test_local_lab_invalidation.py::TestCR02LabEngineHasLocalLabHook::test_lab_engine_has_local_lab_attrs`
- `test_local_post_dedup_merge.py::test_local_merge_inserts_after_dedup_call_site`
- `test_phase_97_invariants.py::test_local_post_dedup_merge`

**Pre-existing environment failure (1 — UTF-8 BOM in genizah_core.py causes ast.parse() SyntaxError):**
- `test_nli_breaker_cross_module_invariants.py::TestNoResidualHardcodedNliTimeouts::test_no_bare_timeout_on_nli_calls_ast`

**Environment failures — empty Tantivy index (6 — `Genizah_Index/` directory exists but has no index data):**
- `test_search_api_v2.py::test_search_mode_real_index_returns_at_least_one_result[exact/variants/responsa/title/shelfmark/fuzzy]`
  - These tests carry `@pytest.mark.skipif(not _has_index(), ...)` but `_has_index()` only checks `os.path.isdir('Genizah_Index')` — which is True even though the directory is empty. The error is `'NoneType' object has no attribute 'execute_search'` — the searcher is None because there is no actual index to open. This is a local dev environment condition, not a code regression.

None of these 14 failures are attributable to Phase 124. The SUMMARY's Post-Execution Review documented the same 8 code + BOM failures; the 6 real-index failures are an additional environmental constant.

---

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/metadata_manager.py` | `_CUDL_LABEL_RE`, `_parse_cudl_label`, `_BRIDGE_IMPORT_WARNED`, `_warn_bridge_import_failed`, `_nli_crossref_svc`, `_get_crossref_service`, `_fjms_svc`, `_get_fjms_service`, `_NLI_CACHE_MAX_ENTRIES`, `_BoundedLRUCache`, `MARC_FUTURE_TIMEOUT`, `NLI_IIIF_FUTURE_TIMEOUT`, `EXTERNAL_IIIF_HTTP_TIMEOUT`, `class MetadataManager` | VERIFIED | 1,940 lines; all 14 required symbols confirmed present; `LOGGER = logging.getLogger("genizah." + __name__)` at line 31, before `_warn_bridge_import_failed` at line 37 |
| `shared/indexer.py` | `class Indexer`, inline `def _tr()`, inline `def _strip_brackets()`, `LOGGER = logging.getLogger(...)`, guarded `import tantivy` | VERIFIED | 495 lines; `class Indexer` at line 56; `_tr()` at line 38 (lazy `CURRENT_LANG`); `_strip_brackets()` at line 51; tantivy ImportError guard at lines 17–26 |
| `genizah_core.py` | Permanent same-object re-export shims for 9 metadata_manager names + `Indexer` (`# noqa: F401`); no original `class MetadataManager` / `class Indexer` definitions | VERIFIED | Shim at lines 102–114; `from shared.metadata_manager import (... 9 names ...)  # noqa: F401`; `from shared.indexer import Indexer  # noqa: F401`; `grep -n "^class MetadataManager\|^class Indexer genizah_core.py"` returns empty |
| `tests/test_no_back_edges_core.py` | EXTRACTED_MODULES grown 8→10; `test_metadata_manager_identity`, `test_indexer_identity`, `test_metadata_manager_standalone_import`, `test_indexer_standalone_import` present | VERIFIED | EXTRACTED_MODULES at lines 31–42 has 10 entries including `shared/metadata_manager.py` (line 40) and `shared/indexer.py` (line 41); identity tests at lines 465, 510; smoke tests at lines 489, 521; 32/32 tests pass |
| `tests/test_desktop_folio_navigation.py` | `metadata_manager_source()` fixture + retargeted `test_image_source_info_in_enrich_metadata` (GUARD-03) | VERIFIED | Fixture at lines 54–59 reads `shared/metadata_manager.py`; `test_image_source_info_in_enrich_metadata` at line 177 takes `metadata_manager_source`; 9/9 tests pass |

---

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `genizah_core.py` | `shared.metadata_manager` | `from shared.metadata_manager import (... 9 names ...) # noqa: F401` at line 102 | WIRED | Identity assertions pass for all 9 names including `_parse_cudl_label` (post-exec fix `fc3ce883`) |
| `genizah_core.py` | `shared.indexer` | `from shared.indexer import Indexer # noqa: F401` at line 114 | WIRED | `shared.indexer.Indexer is genizah_core.Indexer` PASS |
| `shared/metadata_manager.py` | `shared.codicological.CodicologicalManager` | module-level import (not via genizah_core) | WIRED | Module imports cleanly; no genizah_core back-edge |
| `shared/indexer.py` | `genizah_core.CURRENT_LANG` | lazy function-body import inside `_tr()` only (GUARD-01 safe) | WIRED | Line 45: `from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe` |

---

### Data-Flow Trace (Level 4)

Not applicable. This phase is a pure mechanical refactor — no components, no dynamic data rendering, no API endpoints. All Level 4 checks are N/A.

---

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| MetadataManager same-object identity | `python -c "assert shared.metadata_manager.MetadataManager is genizah_core.MetadataManager"` | exit 0 | PASS |
| _BoundedLRUCache same-object identity | `python -c "assert shared.metadata_manager._BoundedLRUCache is genizah_core._BoundedLRUCache"` | exit 0 | PASS |
| All 6 timeout constants via facade | `python -c "assert genizah_core.MARC_FUTURE_TIMEOUT == shared.metadata_manager.MARC_FUTURE_TIMEOUT ..."` | exit 0 | PASS |
| `_parse_cudl_label` via facade | `python -c "assert genizah_core._parse_cudl_label is shared.metadata_manager._parse_cudl_label"` | exit 0 | PASS |
| Indexer same-object identity | `python -c "assert shared.indexer.Indexer is genizah_core.Indexer"` | exit 0 | PASS |
| `create_index` and `_validate_position_match` intact | `python -c "assert genizah_core.Indexer.create_index is shared.indexer.Indexer.create_index ..."` | exit 0 | PASS |
| `build_index.py` coupling preserved | `python -c "import build_index; assert hasattr(build_index.Indexer, 'create_index')"` | exit 0 | PASS |
| GUARD-02 fast gate | `pytest tests/test_no_back_edges_core.py tests/test_nli_cache_bounded_lru.py tests/test_browse_synthetic.py tests/test_audit_followup_2026_05_29.py tests/test_desktop_folio_navigation.py tests/test_api_nli_breaker_integration.py -x -q` | 107 passed | PASS |

---

### Probe Execution

Not applicable. No `scripts/*/tests/probe-*.sh` probes exist for this phase. Phase does not declare probes.

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| CORE-08 | 124-01-PLAN.md | `MetadataManager` (+ `_BoundedLRUCache`) extracted to `shared/metadata_manager.py` | SATISFIED | `shared/metadata_manager.py` exists (1,940 lines); `class MetadataManager` at line 228; `class _BoundedLRUCache` at line 137; same-object identity confirmed; `test_nli_cache_bounded_lru.py` 10/10 pass |
| CORE-09 | 124-01-PLAN.md | `Indexer` extracted to `shared/indexer.py` | SATISFIED | `shared/indexer.py` exists (495 lines); `class Indexer` at line 56; same-object identity confirmed; `build_index.py` coupling preserved |
| GUARD-02 | 124-01-PLAN.md | Zero behavior change — full pytest suite passes at every phase boundary | SATISFIED | 107-test GUARD-02 fast gate: 107/107 pass; bulk suite: 0 new failures (8 pre-existing + 6 environment failures confirmed pre-existing at base `e6714343`) |
| GUARD-03 | 124-01-PLAN.md | Source-scanning tests retargeted before implementation deleted | SATISFIED | `test_desktop_folio_navigation.py` `metadata_manager_source` fixture reads `shared/metadata_manager.py`; `test_genizah_core_nli_breaker_migration.py` path-string registry updated to include `shared/metadata_manager.py` (post-exec fix `e4abf248`); `test_browse_synthetic.py` retargeted; 9/9 `test_desktop_folio_navigation.py` pass |
| GUARD-04 | 124-01-PLAN.md | `genizah_core.py` permanent compatibility facade; shims preserved (never stripped) | SATISFIED | Facade at lines 102–114 exports 9 metadata_manager names + `Indexer` with `# noqa: F401`; `_parse_cudl_label` added post-exec (`fc3ce883`); ruff check on all 3 files: "All checks passed!" |

**Orphaned requirements check:** REQUIREMENTS.md maps GUARD-01 to this phase boundary (all phases 122–127). GUARD-01 is verified: 32/32 `test_no_back_edges_core.py` tests pass (10-entry registry, no module-level back-edges in either new module).

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `shared/metadata_manager.py` | 302, 327, 1257 | Word "placeholder" | Info | Domain term only — "titles_non_placeholder" (CSV column name) and "cache precedence (Enrichment overrides basic placeholders)" are data-model references, not implementation stubs |

No blockers, no warnings. No `TBD`, `FIXME`, or `XXX` markers in any phase-modified file.

---

### Human Verification Required

None. This is a pure mechanical refactor with zero user-visible behavior change. All verification is automatable and has been completed.

---

### Gaps Summary

No gaps. All 7 must-haves verified, all 5 requirement IDs satisfied, all 4 post-execution fixes confirmed present and working (`fc3ce883`, `e4abf248`, `741f7b24`, plus the logging section restoration in `b63411c1`).

---

_Verified: 2026-06-26T07:30:00Z_
_Verifier: Claude (gsd-verifier)_
