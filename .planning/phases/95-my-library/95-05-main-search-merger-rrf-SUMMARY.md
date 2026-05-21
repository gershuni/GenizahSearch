---
phase: 95
plan: "05"
subsystem: my-library
tags: [rrf-merge, local-searcher, tantivy, d-08, d-37, high-1, wave-2]
dependency_graph:
  requires: [95-02, 95-03]
  provides: [genizah_core.py:SearchEngine.local_searcher, _rrf_merge, _query_local_index, reload_local_indexes]
  affects: [genizah_core.py:execute_search]
tech_stack:
  added: []
  patterns: [reciprocal-rank-fusion, d37-corrupt-index-fallback, content-driven-tiebreak, medium1-option-b-deferred]
key_files:
  created:
    - tests/test_local_index_open_fallback.py
    - tests/test_local_post_dedup_merge.py
    - tests/test_side_index_merge.py
    - tests/test_local_reload_after_refresh.py
  modified:
    - genizah_core.py
decisions:
  - "D-08 P0: LOCAL merge inserted AFTER execute_search:_deduplicate (line 8047) — not before (dedup body whitelists V0.8/V0.7 only)"
  - "_rrf_merge tie-break is content-driven (display.source != 'LOCAL') not argument-order-driven — order-independent per W7 requirement"
  - "self.local_index stored separately from self.local_searcher (tantivy.Searcher has no .index attr in tantivy-py API)"
  - "MEDIUM-1 Option B taken: query builder extraction deferred — execute_search query path is ~200+ LOC with Responsa/variants/morphology"
  - "LOCAL merge wrapped in try/except (T-95-19) — main search unaffected by LOCAL query failures"
metrics:
  duration: "~45 minutes"
  completed: "2026-05-21"
  tasks_completed: 3
  files_changed: 5
---

# Phase 95 Plan 05: Main Search Merger RRF Summary

**One-liner:** RRF k=60 LOCAL side-index merge wired POST-`_deduplicate()` in `execute_search` with D-37 corrupt-index fallback and HIGH-1 live-reload methods.

## What Was Built

### Task 1: LOCAL searcher initialization + D-37 fallback + RRF/query helpers

Added to `SearchEngine.__init__` and as new methods in `genizah_core.py`:

| Symbol | Role |
|--------|------|
| `self.local_index` | `tantivy.Index` kept alongside `local_searcher` (Searcher has no `.index` attr in tantivy-py API) |
| `self.local_searcher` | `tantivy.Searcher` snapshot; None on any open failure (D-37) |
| `_open_local_searcher()` | Opens both `local_index` + `local_searcher`; try/except sets both to None on any error (D-37 fallback) |
| `_query_local_index(query_str, mode, gap, limit)` | Queries LOCAL side-index via `local_index.parse_query`; returns `[]` when `local_searcher is None` |
| `_build_local_result_dict(doc, score)` | Constructs D-34 result row shape from Tantivy doc |
| `_rrf_merge(genizah_hits, local_hits, k=60, limit)` | RRF k=60 fusion with content-driven Genizah-first tie-break |
| `reload_local_indexes()` | HIGH-1: close+reopen local_index+local_searcher; calls `reload_local_lab_index()` |
| `reload_local_lab_index()` | LAB-side narrow reload; reopens `local_lab_searcher` + re-reads `.meta.json` |

**D-37 fallback contract:** On ANY exception during `tantivy.Index()` call (corrupt meta.json, missing files, file lock), `LOGGER.warning(...)` is emitted and both `self.local_index = None` and `self.local_searcher = None`. Main search proceeds normally — Genizah-only results.

### Task 2: LOCAL merge POST-`_deduplicate` (D-08 P0) + W6 AST + W7 tie-break

**Insertion point:** `genizah_core.py:8047` (inside `execute_search`, after `deduped = self._deduplicate(results)`)

```python
# Phase 95 D-08 (Codex P0): LOCAL hits merge AFTER _deduplicate.
if getattr(self, "local_searcher", None) is not None:
    try:
        local_hits = self._query_local_index(query_str, mode, gap)
    except Exception as _e:
        LOGGER.warning("LOCAL side-index query failed; main results unaffected: %r", _e)
        local_hits = []
    if local_hits:
        deduped = self._rrf_merge(deduped, local_hits, k=60)
```

**`_deduplicate()` body at `:8096` is UNCHANGED** — V0.8/V0.7 whitelist preserved.

**W7 tie-break design:** The `_rrf_merge` sort key uses `r["hit"].get("display", {}).get("source") != "LOCAL"` (True > False at equal score) — content-driven, order-independent. Passing `(local_hits, genizah_hits)` gives the same ranking as `(genizah_hits, local_hits)`.

### Task 3: HIGH-1 reload methods + MEDIUM-1 Option B deferred

**HIGH-1 resolution:** `reload_local_indexes()` closes and reopens both `local_index` and `local_searcher` so newly committed docs (after MyLibraryTab Refresh/Delete/Rebuild) are visible in the live session without app restart. Plan 07 wires the call sites.

**MEDIUM-1 Option B chosen:** The `execute_search` query construction path is ~200+ LOC touching Responsa expansion, spelling variants, grammatical prefix/suffix/JA expansion, flex spacing, and per-line constraints. Extracting a shared `_build_tantivy_query()` helper is too invasive for this revision. `_query_local_index` uses `index.parse_query(query_str, ["content", "content_head", "content_tail"])` — simplified but functional. Two `@pytest.mark.xfail` tests document the gap as a follow-up trigger.

## Final Line Numbers

| Location | Purpose |
|----------|---------|
| `genizah_core.py:6476-6480` | `__init__` — local_index/local_searcher/lab attrs + `_open_local_searcher()` call |
| `genizah_core.py:6482-6506` | `_open_local_searcher()` — D-37 fallback |
| `genizah_core.py:6508-6517` | `reload_local_indexes()` — HIGH-1 |
| `genizah_core.py:6519-6553` | `reload_local_lab_index()` — HIGH-1 LAB variant |
| `genizah_core.py:6555-6578` | `_query_local_index()` |
| `genizah_core.py:6580-6611` | `_build_local_result_dict()` |
| `genizah_core.py:6613-6643` | `_rrf_merge()` — RRF k=60, W7 tie-break |
| `genizah_core.py:8047-8063` | LOCAL merge hook AFTER `_deduplicate()` in `execute_search` |

## Test Results

| File | Tests | Result |
|------|-------|--------|
| `tests/test_local_index_open_fallback.py` | 6 | 6 PASSED |
| `tests/test_local_post_dedup_merge.py` | 3 | 3 PASSED (D-08 pin + W6 AST) |
| `tests/test_side_index_merge.py` | 6 | 6 PASSED (RRF + W7 tie-break) |
| `tests/test_local_reload_after_refresh.py` | 4 + 2 xfail | 4 PASSED, 2 XFAILED (MEDIUM-1) |
| **Total** | **21** | **19 PASSED, 2 XFAILED** |

## Commits

| Hash | Task | Description |
|------|------|-------------|
| `a0ead053` | Task 1 | LOCAL searcher init + D-37 fallback + RRF/query helpers |
| `57f426f9` | Task 2 | LOCAL merge POST-_deduplicate (D-08 P0) + W6 AST + W7 tie-break |
| `b0328c92` | Task 3 | reload_local_indexes() HIGH-1 fix + local_index attr + MEDIUM-1 Option B |
| `bdcd9e65` | Style | ruff cleanup — remove unused imports |

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] tantivy.Searcher has no `.index` attribute**
- **Found during:** Task 3 (test_reload_local_indexes_picks_up_new_docs_without_restart failed with AttributeError)
- **Issue:** Plan's `_query_local_index` template used `self.local_searcher.index` to call `parse_query`, but the tantivy-py `Searcher` object has no `.index` attribute. The `Index` object must be stored separately.
- **Fix:** Added `self.local_index` attribute in `__init__` and `_open_local_searcher()` to store the `tantivy.Index` object alongside `self.local_searcher`. `_query_local_index` uses `self.local_index.parse_query(...)`.
- **Files modified:** `genizah_core.py`
- **Commit:** `b0328c92`

**2. [Rule 1 - Bug] _rrf_merge tie-break was argument-order-dependent**
- **Found during:** Task 2 (test_rrf_tiebreak_genizah_first failed when local_hits passed as first arg)
- **Issue:** Original implementation tagged the first arg's hits as 'genizah' source via a `sources.add('genizah')` loop. When `_rrf_merge(local_hits, genizah_hits)` was called, local hits were tagged as 'genizah' — violating W7's order-independence requirement.
- **Fix:** Rewrote `_rrf_merge` to use content-driven tie-break: `r["hit"].get("display", {}).get("source") != "LOCAL"` as the secondary sort key. No `sources` set needed.
- **Files modified:** `genizah_core.py`
- **Commit:** `57f426f9`

**3. [Rule 2 - Missing] Tantivy corrupt index test needed real corruption signal**
- **Found during:** Task 1 (test_corrupt_local_index_falls_back_to_genizah_only — Tantivy opened the garbage-file dir as valid empty index)
- **Issue:** Test wrote random binary files to a directory and expected Tantivy to reject it. Tantivy creates a fresh empty index in any directory that has no `meta.json`.
- **Fix:** Changed test to write an invalid `meta.json` with `{invalid json!!!}` content — this causes `tantivy.Index()` to raise `ValueError`.
- **Files modified:** `tests/test_local_index_open_fallback.py`
- **Commit:** `a0ead053`

### MEDIUM-1 Option B — Deferred

The `execute_search` query construction path (~200+ LOC) touches:
- Responsa expansion (components, wildcard patterns, inline patterns)
- Spelling variant expansion (`get_variants`)
- Grammatical prefix/suffix expansion (`expand_grammatical_prefixes/suffixes`)
- Judeo-Arabic expansion (`expand_judeo_arabic`)
- Plene/defective expansion (`expand_plene_defective`)
- Per-line position constraints (`_line_constraints`)
- Flex spacing patterns (`_make_flex_spacing_pattern`)
- Bidirectional + flex_spacing flags

Extracting a shared `_build_tantivy_query(query_str, mode, gap, fields)` helper is too invasive for this plan. `_query_local_index` uses simplified `index.parse_query(query_str, ["content", "content_head", "content_tail"])`.

**Follow-up:** Extract `_build_tantivy_query()` in a v7.14.x patch plan. Two xfail tests (`test_query_semantics_phrase_mode_parity_with_main`, `test_query_semantics_gap_mode_parity_with_main`) serve as the follow-up trigger.

## Known Stubs

None — all methods are fully implemented. The two xfail tests document intentional deferred work, not stubs in production code.

## Threat Flags

None found. No new network endpoints, auth paths, or cloud-write paths introduced. `_query_local_index` wraps Tantivy operations in try/except (T-95-19). D-37 fallback ensures main search continues on any LOCAL index failure (T-95-18).

## Self-Check: PASSED

- `genizah_core.py` — FOUND
- `tests/test_local_index_open_fallback.py` — FOUND
- `tests/test_local_post_dedup_merge.py` — FOUND
- `tests/test_side_index_merge.py` — FOUND
- `tests/test_local_reload_after_refresh.py` — FOUND
- Commit `a0ead053` — FOUND
- Commit `57f426f9` — FOUND
- Commit `b0328c92` — FOUND
- Commit `bdcd9e65` — FOUND
- 19 tests pass, 2 xfailed: CONFIRMED
- ruff clean: CONFIRMED
- `_deduplicate()` body unchanged: CONFIRMED (V0.8/V0.7 whitelist preserved)
- LOCAL merge AFTER `_deduplicate()` in execute_search: CONFIRMED (line 8047)
- `1.0 / (k + rank)` appears ≥ 2 times: CONFIRMED (2 occurrences in _rrf_merge)
