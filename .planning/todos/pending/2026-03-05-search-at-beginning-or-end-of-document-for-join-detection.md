---
created: 2026-03-05T11:51:35.572Z
title: Search at beginning or end of document for join detection
area: search
files:
  - genizah_core.py
  - web/pages/search.py
  - genizah_app.py
---

## Problem

Currently text search matches anywhere within a document's transcription. Researchers looking for joins between fragments need to find text that appears specifically at the beginning or end of a document — text at boundaries suggests the fragment may continue on another physical piece.

This positional search is critical for:
1. Manual join detection by scholars (search for text near fragment edges)
2. Future automated join-finding algorithm that will use this as a building block

## Solution

Add an advanced search option (checkbox or dropdown) to constrain matches to document boundaries:
- "Beginning of document" — match only in the first N lines/characters
- "End of document" — match only in the last N lines/characters
- Configurable threshold (e.g., first/last 3 lines or 200 chars)

Implementation approach:
- Add position filter to the regex phase (Phase 2) of the two-phase search — Tantivy still retrieves candidates, regex checks position within the transcription text
- UI: add option to search settings in both web (search.py) and desktop (genizah_app.py)
- Core: extend `search_genizah()` or post-filter results based on match position within the document text
- Future: expose as a service method for programmatic join detection algorithm
