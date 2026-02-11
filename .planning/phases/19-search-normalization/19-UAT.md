---
status: complete
phase: 19-search-normalization
source: 19-01-SUMMARY.md, 19-02-SUMMARY.md, 19-03-SUMMARY.md
started: 2026-02-11T08:00:00Z
updated: 2026-02-11T08:25:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Diacritics in query return same results as plain query
expected: Search with combining diacritical marks in query returns identical results to plain query (marks stripped automatically)
result: pass

### 2. Geresh/gershayim in query return same results
expected: Search with Hebrew geresh (׳) or gershayim (״) in query returns identical results to query without them -- these punctuation marks are stripped automatically
result: pass (fixed in 19-03)
reported: "If I search הקב'ה I get 503 results. הקבה - 11006 results."
resolution: Added ASCII apostrophe (U+0027) and curly quotes (U+2018, U+2019) to COMBINING_DIACRITICALS_PATTERN

### 3. Highlighting works through combining marks in source text
expected: When search results contain source text with combining marks between base letters, the matched text is still highlighted correctly (marks don't break the highlight span)
result: pass

### 4. Regex mode bypasses diacritics stripping
expected: When search mode is set to Regex, the query is used as-is without stripping. If you include a combining mark in a regex pattern, it searches for that literal mark (not stripped)
result: pass

### 5. Existing search modes unaffected
expected: Standard search, Variants search, and Responsa search all produce the same results as before normalization was added -- no regressions
result: pass

## Summary

total: 5
passed: 5
issues: 0
pending: 0
skipped: 0

## Gaps

None -- all gaps closed by 19-03 (apostrophe variant normalization).
