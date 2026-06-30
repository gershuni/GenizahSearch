---
phase: 123-core-leaf-modules
verified: 2026-06-25T22:04:56Z
status: passed
score: 10/10 must-haves verified
overrides_applied: 0
---

# Phase 123: Core Leaf Modules Verification Report

**Phase Goal:** Seven low-risk, well-tested core clusters extracted to `shared/` behind re-export shims — `shared/variants.py`, `shared/codicological.py`, `shared/responsa.py`, `shared/joins_manager.py`, `shared/lists_manager.py`, `shared/browse_map_utils.py`, `shared/text_normalize.py`. Lazy back-edges in `shared/local_indexer.py` retargeted. Zero behavior change.
**Verified:** 2026-06-25T22:04:56Z
**Status:** PASSED
**Re-verification:** No — initial verification

---

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|---------|
| 1 | `from shared.browse_map_utils import normalize_shelfmark` resolves; `genizah_core.normalize_shelfmark` is same object (CORE-06, SC#1) | VERIFIED | Python identity assertion passed: `shared.browse_map_utils.normalize_shelfmark is genizah_core.normalize_shelfmark` |
| 2 | `from shared.text_normalize import strip_nikud, strip_search_diacritics` resolves; same objects via genizah_core shim (CORE-07) | VERIFIED | Both identity assertions passed; `shared.text_normalize.strip_nikud is genizah_core.strip_nikud` confirmed |
| 3 | `from shared.variants import VariantManager` resolves; `genizah_core.VariantManager` is the same class (CORE-02, SC#1) | VERIFIED | `shared.variants.VariantManager is genizah_core.VariantManager` confirmed |
| 4 | `from shared.responsa import parse_responsa_query` resolves; full responsa suite green via genizah_core facade (CORE-01, SC#2) | VERIFIED | 191 tests pass across `test_responsa_core.py`, `test_responsa_edge_cases.py`, `test_hebrew_search_tokenizer.py`, `test_search_normalization.py`; private symbols `_apply_explosion_guard` and `_count_expanded_terms` also re-exported as same objects |
| 5 | `from shared.codicological import CodicologicalManager` resolves; same class via shim (CORE-03) | VERIFIED | Identity assertion passed; imports `natural_sort_key` directly from `shared.browse_map_utils`, not genizah_core |
| 6 | `from shared.joins_manager import JoinsManager` resolves; same class via shim (CORE-04) | VERIFIED | Identity assertion passed; `JoinsManager` test suite (`test_known_joins_group.py`) green |
| 7 | `from shared.lists_manager import ListsManager` resolves; same class via shim (CORE-05) | VERIFIED | Identity assertion passed; `test_user_lists_cache_isolation.py` and `test_recently_viewed_bugs.py` pass; inline `_tr()` helper present replacing all `tr()` calls |
| 8 | No extracted `shared/` module imports `genizah_core` at module level (GUARD-01); full pytest suite green at every cluster commit boundary (GUARD-02, SC#5) | VERIFIED | AST scan (BOM-safe) of all 7 modules: zero module-level back-edges; `tests/test_no_back_edges_core.py` 26/26 passed; EXTRACTED_MODULES registry grew 1→8; 310 targeted tests pass; executor reported 1,917+ full-suite passes |
| 9 | `shared/local_indexer.py` no longer imports text-normalize helpers via `genizah_core` at module level; D-01 retargets landed (CORE-07, SC#3) | VERIFIED | `local_indexer.py` lazy imports at lines 3154 and 3826 point to `shared.text_normalize`; `exclusion_service.py` points to `shared.browse_map_utils`; `nli_crossref_service.py` ×2 and `search_serializer.py` retargeted; `test_local_pdf_nikud_strip.py` 5/5 passed |
| 10 | `test_shelfmark_bridge.py` source-hash snapshot regenerated from new location and green (GUARD-03, SC#4) | VERIFIED | SHA256 hash in `tests/fixtures/normalize_shelfmark_snapshot.json` (`a2aba916...`) matches `inspect.getsource(shared.browse_map_utils.normalize_shelfmark)` — confirms source was read from `shared/browse_map_utils.py`, not genizah_core; 61 shelfmark bridge tests pass |

**Score:** 10/10 truths verified

---

## Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `shared/browse_map_utils.py` | normalize_shelfmark, natural_sort_key, LIBRARY_CODES, get_library_display, dedupe_browse_map, IE helpers | VERIFIED | 20,484 bytes; all 6 expected symbols present; no module-level genizah_core import |
| `shared/text_normalize.py` | NIKUD_PATTERN, strip_nikud, COMBINING_DIACRITICALS_PATTERN, strip_search_diacritics | VERIFIED | 2,561 bytes; all 4 symbols present; stdlib-only imports (just `re`) |
| `shared/variants.py` | VariantManager class | VERIFIED | 16,008 bytes; `class VariantManager` present; imports `from shared.config import Config` |
| `shared/responsa.py` | ResponsaComponent, parse_responsa_query, expand_* functions, _apply_explosion_guard, GRAMMATICAL_PREFIXES/SUFFIXES, inline `_tr()` | VERIFIED | 37,698 bytes; all expected symbols present; engine helpers (`build_tantivy_query`, `_add_bracket_variants`, etc.) correctly absent |
| `shared/codicological.py` | CodicologicalManager class | VERIFIED | 14,825 bytes; imports `natural_sort_key` directly from `shared.browse_map_utils` (not via genizah_core) |
| `shared/joins_manager.py` | JoinsManager class | VERIFIED | 21,171 bytes; `from shared.config import Config` placed before class body (Pitfall 3 guarded); imports `normalize_shelfmark` from `shared.browse_map_utils` |
| `shared/lists_manager.py` | ListsManager class, inline `_tr()` | VERIFIED | 45,843 bytes; `class ListsManager` present; inline `def _tr(` present; `from shared.config import Config` before class body |
| `genizah_core.py` | Permanent same-object re-export shims for all 7 clusters (# noqa: F401) | VERIFIED | ~8,398 lines (down from ~12,500); all 7 cluster shim blocks present; `UNIFIED_VARIANT_PAIRS`, `get_top_pairs`, `LIBRARY_CODES_HE` restored on facade (Codex round-2 GUARD-04 fix) |
| `tests/test_no_back_edges_core.py` | EXTRACTED_MODULES grown 1→8; 14 D-03 tests (7 identity + 7 smoke) | VERIFIED | 26 tests pass (8 GUARD-01 params + 8 identity + 8 smoke + 2 extra lists_manager); EXTRACTED_MODULES has 8 entries |

---

## Key Link Verification

| From | To | Via | Status | Details |
|------|----|-----|--------|---------|
| `genizah_core.py` | `shared.browse_map_utils` | `from shared.browse_map_utils import ... # noqa: F401` | VERIFIED | Shim present; same-object identity confirmed |
| `genizah_core.py` | `shared.text_normalize` | `from shared.text_normalize import ... # noqa: F401` | VERIFIED | Shim present; both symbols identity-confirmed |
| `genizah_core.py` | `shared.variants` | `from shared.variants import VariantManager # noqa: F401` | VERIFIED | Shim present; identity confirmed |
| `genizah_core.py` | `shared.responsa` | `from shared.responsa import ... # noqa: F401` | VERIFIED | Shim present; includes private symbols (`_apply_explosion_guard`, `_count_expanded_terms`, `_GAP_TOKEN_RE`, etc.) |
| `genizah_core.py` | `shared.codicological` | `from shared.codicological import CodicologicalManager # noqa: F401` | VERIFIED | Shim present |
| `genizah_core.py` | `shared.joins_manager` | `from shared.joins_manager import JoinsManager # noqa: F401` | VERIFIED | Shim present |
| `genizah_core.py` | `shared.lists_manager` | `from shared.lists_manager import ListsManager # noqa: F401` | VERIFIED | Shim present |
| `shared/codicological.py` | `shared.browse_map_utils.natural_sort_key` | `from shared.browse_map_utils import natural_sort_key` (module-level, NOT via genizah_core) | VERIFIED | Direct import confirmed; GUARD-01 pass |
| `shared/joins_manager.py` | `shared.browse_map_utils.normalize_shelfmark` | `from shared.browse_map_utils import normalize_shelfmark` (module-level, NOT via genizah_core) | VERIFIED | Direct import confirmed; GUARD-01 pass |
| `shared/local_indexer.py` | `shared.text_normalize` | Lazy `from shared.text_normalize import strip_nikud, strip_search_diacritics` (retargeted from genizah_core) | VERIFIED | Both lazy sites retargeted; no genizah_core references to moved symbols remain |

---

## Data-Flow Trace (Level 4)

Not applicable — this is a pure code-movement refactor. No dynamic data rendering involved.

---

## Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| All 7 same-object identity checks | `python -c "import shared.*; assert shared.X.Y is genizah_core.Y"` for all 7 clusters | 10/10 PASS | PASS |
| GUARD-01 back-edge test suite | `pytest tests/test_no_back_edges_core.py -q` | 26 passed in 0.29s | PASS |
| Responsa test suite (SC#2) | `pytest tests/test_responsa_core.py tests/test_responsa_edge_cases.py ...` | 191 passed in 4.24s | PASS |
| Shelfmark bridge + snapshot hash | `pytest tests/test_shelfmark_bridge.py -q` + SHA256 comparison | 61 passed; hash matches | PASS |
| GUARD-03 all 5 named tests | `pytest tests/test_shelfmark_bridge.py tests/test_desktop_folio_navigation.py tests/test_wr01_open_local_browse_page_ast.py tests/test_tabular_builder_rtl.py tests/test_view_all_cap.py` | 80 passed in 33.34s | PASS |
| Nikud strip (CORE-07 D-01 gate) | `pytest tests/test_local_pdf_nikud_strip.py -q` | 5 passed in 0.71s | PASS |
| Full targeted battery | 310-test battery (all of the above combined) | 310 passed, 1 warning | PASS |
| GUARD-04 compat symbols | `from genizah_core import UNIFIED_VARIANT_PAIRS, get_top_pairs, LIBRARY_CODES_HE` | All resolve; UNIFIED_VARIANT_PAIRS has 25,802 entries | PASS |
| Engine helpers absent from responsa.py | grep for `build_tantivy_query`, `_add_bracket_variants`, etc. | All 10 engine symbols correctly absent | PASS |
| Engine helpers present in genizah_core | grep for `build_tantivy_query`, `build_regex_pattern`, `MARK_TOLERANT_INSERTER` | All 4 checked: present | PASS |

---

## Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|---------|
| CORE-01 | 123-01-PLAN.md | Responsa parsing cluster extracted | SATISFIED | `shared/responsa.py` 1,146 lines; full 6-suite responsa tests pass |
| CORE-02 | 123-01-PLAN.md | `VariantManager` extracted | SATISFIED | `shared/variants.py`; identity confirmed |
| CORE-03 | 123-01-PLAN.md | `CodicologicalManager` extracted | SATISFIED | `shared/codicological.py`; identity confirmed |
| CORE-04 | 123-01-PLAN.md | `JoinsManager` extracted | SATISFIED | `shared/joins_manager.py`; joins tests pass |
| CORE-05 | 123-01-PLAN.md | `ListsManager` extracted | SATISFIED | `shared/lists_manager.py`; lists/recently-viewed tests pass |
| CORE-06 | 123-01-PLAN.md | `browse_map_utils` cluster extracted | SATISFIED | `shared/browse_map_utils.py`; normalize_shelfmark, natural_sort_key, LIBRARY_CODES, etc. |
| CORE-07 | 123-01-PLAN.md | `text_normalize` extracted; `local_indexer.py` back-edges retargeted | SATISFIED | `shared/text_normalize.py`; both local_indexer lazy sites retargeted; nikud test passes |
| GUARD-02 | 123-01-PLAN.md | Zero behavior change — full suite passes at every commit boundary | SATISFIED | Executor: 1,917+ passes; 310-test targeted battery PASS; 1 pre-existing asyncio flaky test unrelated to this phase |
| GUARD-03 | 123-01-PLAN.md | 5 named source-scanning tests retargeted / remain green | SATISFIED | All 5 pass (80 tests total); `test_shelfmark_bridge.py` uses regenerated snapshot that now reads from `shared/browse_map_utils.py`; other 4 scan symbols not moved this phase and need no retarget until Phase 124-126 |
| GUARD-04 | 123-01-PLAN.md | `genizah_core.py` remains permanent compat facade | SATISFIED | All 7 shim blocks present; `UNIFIED_VARIANT_PAIRS`/`get_top_pairs`/`LIBRARY_CODES_HE` restored on facade (Codex r2); genizah_core.py shrank ~12,500→8,398 lines |

---

## Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| `shared/responsa.py` | 1 | BOM (`﻿`) in file header | INFO | Causes SyntaxError if parsed with plain `ast.parse()` without BOM-stripping; Python import handles it correctly via `utf-8-sig`; tests pass; no behavior impact |

The BOM in `shared/responsa.py` is an encoding artifact, not a logic issue. Python's import machinery handles it transparently; the file imports and runs correctly. It does not block any test or use.

No `TBD`, `FIXME`, or `XXX` debt markers found in the 7 new modules.

---

## Commit Record

| Cluster | Commit SHA | Description |
|---------|-----------|-------------|
| browse_map_utils | `1d77d90a` | Extract + 4 D-01 retargets |
| text_normalize | `1c3930d0` | Extract + local_indexer retargets |
| variants | `74b46e6c` | Extract VariantManager |
| responsa | `57023501` | Extract parsing/expansion cluster |
| codicological | `5bf8b335` | Extract CodicologicalManager |
| joins_manager | `746176f4` | Extract JoinsManager |
| lists_manager | `3fca9bd1` | Extract ListsManager |
| Codex r1 fix | `674d16b5` | Remove spurious responsa search-helper copies, fix duplicate flex/wildcard, fix logger routing |
| Codex r2 fix | `0a095e89` | Restore UNIFIED_VARIANT_PAIRS, get_top_pairs, LIBRARY_CODES_HE on facade |
| Summary | `d277d932` | Record Codex convergence (3 rounds → APPROVE) |

---

## GUARD-03 Detail: The 5 Named Source-Scanning Tests

The plan distinguishes two groups:

**test_shelfmark_bridge.py** — scans `normalize_shelfmark` source via `inspect.getsource`, which follows the function object to its defining file. After Phase 123, `inspect.getsource(genizah_core.normalize_shelfmark)` reads `shared/browse_map_utils.py`. The snapshot SHA256 was regenerated to match. VERIFIED green (61 tests).

**test_desktop_folio_navigation.py, test_wr01_open_local_browse_page_ast.py, test_tabular_builder_rtl.py, test_view_all_cap.py** — these scan `genizah_app.py` or `genizah_core.py` for symbols NOT moved in Phase 123 (`enrich_metadata`, browse page patterns, tabular builder, view_all). Per plan: "Retarget during the additive phase; flip at deletion." Since no deletions occur in Phase 123 (genizah_core keeps its code replaced by shims, not deleted outright), these 4 tests require no retarget this phase. They pass unchanged (71 tests).

This matches the phase plan's SC#4 statement: "The other 4 source-scanning tests... scan symbols NOT moved this phase and need no change here; they are affected by Phases 124–126."

---

## Gaps Summary

None. All 10 must-have truths are VERIFIED. The phase goal is achieved.

**Only notable item:** a pre-existing asyncio event loop flaky test (`test_round_trip_search_type_fuzzy`) was documented in the SUMMARY as pre-existing and unrelated to Phase 123 changes. This is consistent with prior test history and is not a Phase 123 regression.

---

_Verified: 2026-06-25T22:04:56Z_
_Verifier: Claude (gsd-verifier)_
