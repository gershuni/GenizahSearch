---
created: 2026-03-18T19:45:41.247Z
title: "Fill missing genizah manuscripts from FIST.db"
area: data
files:
  - genizah_core.py
  - shared/fjms_service.py
---

## Problem

The current dataset only includes manuscripts that have transcriptions (PGP corpus: ~35K documents). Many thousands of additional Genizah manuscripts in FIST.db were never transcribed but still have valuable metadata (shelfmarks, classifications, catalog descriptions, images via NLI crossref). These are invisible in GenizahSearch — users cannot browse or view their images/metadata.

## Solution

Import non-transcribed manuscripts from FIST.db to fill the gaps:
1. Identify manuscripts in FIST.db `dbo_Signature` / `dbo_UnitCatalogRec` that are NOT already in libraries.csv or pgp.db
2. Extract their metadata (shelfmark, library, domain classification, catalog description, physical description)
3. Cross-reference with nli_crossref.db for image availability
4. Add to libraries.csv and/or a new sidecar so they appear in browse/search with images and metadata
5. Consider whether these should be indexed in Tantivy (metadata-only, no transcription text)

Key considerations:
- FIST.db has ~1.56M signatures, ~411K catalog records — significant overlap with existing 217K in libraries.csv
- Need to deduplicate carefully by shelfmark/sys_id
- These records won't have transcription text but will have FJMS enrichment (domains, joins, bibliography)
