---
created: 2026-03-08
priority: medium
scope: data-pipeline
tags: [nli, marc, translation, sidecar]
---

# NLI MARC Data Crawl, Local Storage & Translation

## Problem

NLI Ktiv metadata (notes, english_title, physical_desc, subjects, people, bibliography) is fetched live per-manuscript from the NLI MARC XML API. This means:
- No batch translation possible — on-demand Dicta only
- Slow browse page loads (network-dependent)
- No offline access to Ktiv Info
- Notes can be very long (e.g. 990001966850205171) — on-demand translation is slow

## Proposed Solution

1. **Server-side crawl script**: Gradually fetch MARC XML for all ~217K sys_ids via `NLI_IIIF_BASE/marc/bib/{system_id}`. Throttled to respect NLI rate limits. Checkpoint-based resume. Store parsed fields in a new `nli_marc.db` SQLite sidecar.

2. **Batch translate**: Run Dicta translation on all fetched English fields (notes, english_title, physical_desc, subjects). Same pattern as libraries/PGP/FJMS batch scripts.

3. **Download locally**: Ship `nli_marc.db` alongside other sidecars. MetadataManager reads from local DB first, falls back to live API for missing/new records.

4. **Delta updates**: Periodic re-crawl (monthly?) checking for changes. Compare stored vs fetched — update only changed records. Could use NLI's OAI-PMH or last-modified headers if available.

## Benefits

- Instant Ktiv Info display (no network wait)
- Pre-translated notes/fields with toggle (same as PGP descriptions)
- Offline Ktiv Info access
- Consistent with existing sidecar architecture (pgp.db, fjms_enrichment.db, nli_crossref.db)

## Estimated Effort

- Crawl script: ~1 day (similar to existing batch scripts)
- Crawl execution: ~2-3 days server time (throttled)
- Translation: ~1 day execution (similar to FJMS free desc)
- Integration: ~1 phase (read from sidecar, fallback to live API)

## Notes

- Could extend existing nli_crossref.db or create separate nli_marc.db
- Consider storing raw XML for future field extraction
- NLI data is public but respect their infrastructure
