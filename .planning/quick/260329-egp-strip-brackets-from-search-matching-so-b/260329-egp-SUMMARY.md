---
phase: quick
plan: 260329-egp
subsystem: search
tags: [search, brackets, tantivy, regex, genizah-notation]
dependency_graph:
  requires: []
  provides: [bracket-aware-search]
  affects: [execute_search, search_composition_logic, _execute_line_break_search, build_tantivy_query]
tech_stack:
  added: []
  patterns: [bracket-variant-expansion, content-stripping-for-matching]
key_files:
  created:
    - tests/test_bracket_search.py
  modified:
    - genizah_core.py
decisions:
  - Always strip brackets in composition search (chunks come from user text, never bracketed)
  - Re-search on original content for highlighting to preserve scholarly notation
  - Bracket variants added without boost (recall aids only)
metrics:
  duration: 16min
  completed: 2026-03-29
---

# Quick Task 260329-egp: Strip Brackets from Search Matching Summary

Bracket-aware search matching so bracket-free queries find documents containing scholarly brackets (e.g., searching for a plain Hebrew word finds it when wrapped in reconstruction brackets like ]word or [d]l[k])

## What Changed

### Three helper functions added to genizah_core.py

- `_add_bracket_variants(term)` -- returns bracket-adorned variants ([term, term], ]term, etc.) for Tantivy OR expansion
- `_query_has_brackets(query_str)` -- detects if query contains literal brackets
- `_strip_brackets(text)` -- removes all [ and ] from text

### Tantivy query expansion (build_tantivy_query)

Both the standard path and Responsa path now add bracket-variant terms to the OR clause for each search term. This ensures that documents where a word is stored as a bracketed token (e.g., `]word`) appear in the Tantivy candidate set even when the query is bracket-free.

### Regex matching with bracket stripping

Three search paths modified:
1. **execute_search** -- strips brackets from content before regex matching when query has no brackets
2. **_execute_line_break_search** -- same pattern
3. **search_composition_logic** -- always strips brackets (composition chunks come from user text)

For highlighting, the code re-searches on original content to preserve scholarly bracket notation in displayed snippets.

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Add bracket-aware search matching (TDD) | 503554ee | genizah_core.py, tests/test_bracket_search.py |

## Test Results

- 19 new bracket search tests: all pass
- Full suite: 1033 passed, 9 skipped, 0 failures

## Deviations from Plan

None -- plan executed exactly as written.

## Known Stubs

None.

## Self-Check: PASSED
