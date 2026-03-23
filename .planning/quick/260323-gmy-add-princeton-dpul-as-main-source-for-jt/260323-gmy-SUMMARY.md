---
quick_id: 260323-gmy
status: completed
commits: 36ebe881, a05a0041
---

# Quick Task 260323-gmy: Summary

## Problem

JTS manuscripts (30K+ records) had almost no DPUL image coverage. The jts_dpul table had only 453 rows
(1% of 44K JTS shelfmarks) because the v1 import script searched per-shelfmark and had ~60% match rate
on a small subset. The web app also defaulted to NLI images even when DPUL images were available.

## What Changed

### 1. Web auto-default to JTS/Princeton images (36ebe881)

**web/pages/browse.py**: Added `source_user_override` flag to auto-default to DPUL images for JTS
manuscripts. Resets on manuscript navigation, respects explicit user source selection.

### 2. DPUL v2 full catalog import script (a05a0041)

**scripts/import_jts_dpul_v2.py**: New approach — iterates the entire DPUL Cairo Geniza catalog
(36,283 items) via paginated listing API instead of searching per-shelfmark.

- Phase 1: Iterate 363 catalog pages (100 items/page) -> shelfmark + ark_suffix
- Phase 2: Fetch each item's detail JSON -> manifest_url + thumbnail_url (parallel workers)
- Tested: 500 items, 100% manifest success rate, ~14 items/sec with 5 workers
- Full import: ~50 min. Supports --resume, --limit, --phase1-only

### Run command

```bash
python scripts/import_jts_dpul_v2.py --workers 5 --db nli_data/nli_crossref.db
```

After import, existing code automatically uses DPUL manifests for JTS image loading.
