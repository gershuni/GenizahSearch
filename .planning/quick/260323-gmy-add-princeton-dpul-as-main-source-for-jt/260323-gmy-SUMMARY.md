---
quick_id: 260323-gmy
status: completed
commits: 36ebe881, a05a0041, b03dc9bd
---

# Quick Task 260323-gmy: Summary

## Problem

JTS manuscripts (30K+ records) had almost no DPUL image coverage. The jts_dpul table had only 453 rows
(1% of 44K JTS shelfmarks) because the v1 import script searched per-shelfmark and had ~60% match rate
on a small subset. The web app also defaulted to NLI images even when DPUL images were available.
External link buttons pointed to Figgy manifest URLs instead of DPUL catalog pages.

## What Changed

### 1. Web auto-default to JTS/Princeton images (36ebe881)

**web/pages/browse.py**: Added `source_user_override` flag to auto-default to DPUL images for JTS
manuscripts. Resets on manuscript navigation, respects explicit user source selection.

### 2. DPUL v2 full catalog import script (a05a0041)

**scripts/import_jts_dpul_v2.py**: Iterates entire DPUL Cairo Geniza catalog (36,283 items) via
paginated listing API. Two phases: catalog pages -> item details. 100% manifest success rate.

### 3. External link fix — catalog page instead of manifest (b03dc9bd)

Three locations were setting the external link to the Figgy manifest URL:
- **genizah_core.py**: `external_url` now uses `get_jts_dpul_url()` (catalog page)
- **genizah_app.py ResultDialog**: Added `library_viewer_url` override for JTS
- **genizah_app.py browse tab**: Same override

The manifest URL is still used internally for image fetching (`ext_link` variable).

## Results

- 36,283 DPUL items imported (was 453)
- JTS manuscripts auto-default to Princeton DPUL images in web browse
- Desktop already defaulted to external images
- External links now go to direct DPUL catalog pages (e.g. `dpul.princeton.edu/cairo_geniza/catalog/dc44558q31d`)
