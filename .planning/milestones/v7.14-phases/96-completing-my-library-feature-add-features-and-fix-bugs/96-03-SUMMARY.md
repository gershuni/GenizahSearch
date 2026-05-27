---
phase: 96
plan: 03
subsystem: search-engine
tags: [phase-96, my-library, highlight, navigation, d-f5, new-2, genizah_core]
dependency_graph:
  requires: [96-01]
  provides: [D-F5-highlight-normalization, NEW-2-engine-primitive]
  affects: [genizah_core.py, tests/test_local_hit_highlighting.py, tests/test_local_nav_page_chunk.py]
tech_stack:
  added: []
  patterns:
    - "D-04.1 filter-out: _build_local_result_dict returns None on regex non-match; _query_local_index skips Nones"
    - "Codex HIGH #3 instrumentation: _last_local_query_regex attribute records regex for test spy assertions"
    - "Per-sys_id page-list cache in _local_pages_cache, invalidated on reload_local_indexes()"
key_files:
  modified:
    - genizah_core.py
decisions:
  - "Option A (normalize at construction time): _build_local_result_dict now computes highlight_pattern + asterisk-marker snippet + raw_file_hl when regex is provided — single change site fixes both search-table and ResultDialog render without UI touches"
  - "D-04.1 filter-out: returns None on regex non-match, _query_local_index skips. Matches Genizah two-phase algebra. No fallback display."
  - "Back-compat: regex=None path returns old shape (snippet = content[:200]) for legacy callers"
  - "corpus_scope=local fast path: builds regex before calling _query_local_index so LOCAL-only searches also get highlighting"
  - "get_local_browse_page: does NOT apply D-04.1 filter-out — browse must return all pages regardless of search regex"
metrics:
  duration: "~20 minutes"
  completed: "2026-05-24T09:45:50Z"
  tasks_completed: 2
  files_changed: 1
---

# Phase 96 Plan 03: LOCAL Hit Highlight Normalization + Navigation Primitive Summary

Engine-side D-F5 fix and NEW-2 navigation primitive: `_build_local_result_dict` now normalizes LOCAL hit dicts to carry `highlight_pattern` + asterisk-marker `snippet` + `raw_file_hl` matching Genizah hit shape; non-matching Tantivy candidates are filtered out (D-04.1); `get_local_browse_page` provides per-sys_id paged navigation for LOCAL files.

## Tasks Completed

| Task | Name | Commit | Files |
|------|------|--------|-------|
| 1 | Thread regex through `_query_local_index`, normalize `_build_local_result_dict`, D-04.1 filter-out | ec7ec82c | genizah_core.py |
| 2 | Add `SearchEngine.get_local_browse_page` navigation primitive | ec7ec82c | genizah_core.py |

(Both tasks committed in a single atomic commit since they touch the same file with no intervening checkpoint.)

## What Was Built

### Task 1 — D-F5 LOCAL Hit Highlight Normalization (D-04.1 filter-out)

**Root cause of D-F5:** `_build_local_result_dict` returned `snippet = content[:200]` (raw, no asterisks) and no `highlight_pattern` or `raw_file_hl` fields. The downstream UI (`format_snippet` in search table, `if pattern_str:` branch in `ResultDialog`) silently fell through to plain-text display because those keys were missing.

**Fix — Option A (normalize at construction time):**

`_build_local_result_dict(doc, score, regex=None, pattern_str=None)`:
- When `regex` is provided AND matches `content`: calls `self.highlight(content, regex, for_file=False)` for `snippet` (with `*...*` markers) and `for_file=True` for `raw_file_hl`; sets `highlight_pattern = pattern_str or regex.pattern`.
- When `regex` is provided AND does NOT match `content` (D-04.1): returns `None` — caller skips the candidate. Tantivy false positives are silently filtered, matching Genizah two-phase algebra.
- When `regex` is None (back-compat): returns old shape (`snippet = content[:200]`, `highlight_pattern = ""`).

`_query_local_index(query_str, mode, gap, limit=None, regex=None)`:
- Records `self._last_local_query_regex = regex` on each call (Codex HIGH #3 instrumentation spy hook).
- Passes `regex` and `pattern_str = regex.pattern` down to `_build_local_result_dict`.
- Skips any hit where `_build_local_result_dict` returned `None` (D-04.1 loop).

**RRF merge call site (BLOCKER 4 closure):** Line updated from:
```python
local_hits = self._query_local_index(query_str, mode, gap)
```
to:
```python
local_hits = self._query_local_index(query_str, mode, gap, regex=regex)
```
Variable name `regex` was pinned by source verification 2026-05-24.

**corpus_scope="local" fast path (Rule 2 — missing critical functionality):** The early-return path for LOCAL-only searches at `execute_search` line ~8050 was also updated to build a regex via `self.build_regex_pattern` before calling `_query_local_index`, so LOCAL-only searches also produce highlighted results.

**`__init__` additions:**
- `self._last_local_query_regex = None` — test spy attribute always present
- `self._local_pages_cache: dict = {}` — page-list cache for `get_local_browse_page`

### Task 2 — NEW-2 `get_local_browse_page` Navigation Primitive

New method `SearchEngine.get_local_browse_page(sys_id, p_num=None, next_prev=0, ...)` inserted immediately before `get_browse_page_by_fl` (after `get_browse_page`).

**Return shape** matches `get_browse_page`: `{uid, p_num, full_header, text, total_pages, current_idx, internal_index, sys_id}` (omits `volume_ie` — LOCAL has no volumes).

**Cache strategy:** First call per `sys_id` runs a Tantivy `parse_query` over `full_header` field, collects all pages matching `{sys_id}_LOCAL_P*`, sorts by `p_num`, stores in `self._local_pages_cache[sys_id]`. Subsequent nav clicks are O(1) dict lookups. Cache invalidated by `reload_local_indexes()`.

**D-12 no-wrap semantics:** `target_idx < 0 or target_idx >= len(pages)` returns `None` — caller disables prev/next buttons at boundaries.

**Browse vs. search semantics:** `get_local_browse_page` does NOT apply D-04.1 filter-out — user clicked into a specific file and wants to see ALL pages, not only pages where the search regex matches.

## Deviations from Plan

### Auto-added Issues

**1. [Rule 2 - Missing Critical Functionality] Extended D-F5 to corpus_scope="local" fast path**
- **Found during:** Task 1 — checking all callers of `_query_local_index`
- **Issue:** The `corpus_scope == "local"` early-return path at `execute_search` line ~8050 called `_query_local_index(query_str, mode, gap)` without building a regex first. LOCAL-only searches would produce unhighlighted results even after the D-F5 fix.
- **Fix:** Added regex build (`build_regex_pattern`) before `_query_local_index` call in the fast path, passes `regex=_local_regex or None`.
- **Files modified:** `genizah_core.py`
- **Commit:** ec7ec82c

No other deviations. Plan executed as written.

## Test Results

| Suite | Result |
|-------|--------|
| `tests/test_local_hit_highlighting.py` (6 tests) | 6 PASSED |
| `tests/test_local_nav_page_chunk.py::test_next_page` | PASSED |
| `tests/test_local_nav_page_chunk.py::test_no_wrap_at_boundary` | PASSED |
| `tests/test_local_post_dedup_merge.py` | 5 passed, 2 skipped |
| `tests/test_local_filter_cascade.py` | 2 passed |
| `python -m ruff check genizah_core.py` | Clean |

Pre-existing failures in `tests/test_local_pdf_extraction_fallback.py` (2 failures) are D-F4 PDF extraction tests — belong to plan 96-02, not this plan, and were failing before this plan started.

## REVISION 2026-05-24 Notes

- **D-04.1 LOAD-BEARING:** Filter-out semantics (not fallback display) confirmed by `test_regex_non_match_filtered_out`.
- **BLOCKER 4 closure:** `test_d_f5_integration_regex_arrives_at_build_local_result_dict` confirms regex arrives non-None at `_build_local_result_dict` via spy pattern.
- **Codex HIGH #3:** `self._last_local_query_regex` instrumentation hook present (`grep -c` returns 3: init + assignment + comment).
- **Merge call site variable name:** `regex` (lowercase, source-verified) — `grep` returns exactly 1 match for `_query_local_index(query_str, mode, gap, regex=regex)`.

## Known Stubs

None — all functionality is fully wired.

## Threat Flags

None — this plan adds no new network endpoints, auth paths, file access patterns, or schema changes.

## Self-Check: PASSED

- genizah_core.py: FOUND
- 96-03-SUMMARY.md: FOUND
- commit ec7ec82c: FOUND
