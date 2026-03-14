---
created: 2026-02-13T12:10:00Z
title: Pre-search domain filtering optimization
area: search, performance
files:
  - genizah_core.py
  - shared/fjms_service.py
priority: low
tags: [performance, domains, future]
---

## Description

Constrain Tantivy search to domain sys_ids BEFORE scoring, rather than post-filtering results.

## Approach

1. Get sys_ids from selected domains via `get_manuscripts_by_domain()`
2. Add `filter_sys_ids: set` parameter to genizah_core search functions
3. Pass sys_id set as a Tantivy filter constraint during candidate retrieval
4. Results are pre-narrowed, potentially faster for narrow domains

## Notes

- Most beneficial for narrow domains (e.g., Piyyut ~5K manuscripts vs 217K total)
- Less benefit for exclude-mode or broad domains
- Current post-filter approach works correctly, this is a speed optimization
- Requires Tantivy API support for document-level filtering (needs investigation)
