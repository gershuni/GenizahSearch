---
created: 2026-03-09T19:00:00Z
title: Unified metadata text search with translations
area: search, browse
files:
  - shared/fjms_service.py
  - shared/translation_service.py
  - genizah_app.py
  - web/pages/search.py
  - web/pages/catalog_browse.py
---

## Problem

The browse text filter and pre-search filter only search FJMS catalog data via `catalog_fts` FTS5 index. Several major metadata sources are NOT searchable:

- **PGP descriptions** (35K English + 35K Hebrew translations in pgp.db)
- **Libraries titles** (184K bilingual in libraries_translations.db)
- **FJMS translations** (412K: RunningTitle, FullText, FreeDesc, catalog fields in fjms_translations)

Users searching in Hebrew won't find manuscripts whose translated descriptions match, and users can't search PGP/library metadata at all from browse or pre-search filters.

## Solution

Option A (preferred): Build a unified FTS5 index that includes all metadata sources + their translations. Single query covers everything.

Option B: Run parallel FTS5 queries on separate indexes (catalog_fts + pgp_fts + translations_fts + libraries_fts) and UNION the alma_id/sys_id results.

Option C: Add a "search translations too" checkbox that does a LIKE fallback on translation tables for the current query.

User should be able to choose whether to include translations in the search (checkbox/toggle).

## Data volumes

| Source | Records | Languages |
|--------|---------|-----------|
| FJMS catalog_fts | ~685K | HE+EN (original) |
| FJMS translations | 412K | EN->HE and HE->EN |
| PGP descriptions | 35K EN + 35K HE | Bilingual |
| Libraries titles | 184K | Bilingual |
