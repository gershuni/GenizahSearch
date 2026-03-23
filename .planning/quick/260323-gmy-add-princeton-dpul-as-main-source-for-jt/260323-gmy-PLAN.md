---
quick_id: 260323-gmy
description: "Add Princeton DPUL as main source for JTS images"
tasks: 2
---

# Quick Task 260323-gmy: Add Princeton DPUL as Main Source for JTS Images

## Context

The jts_dpul table had only 453/44K rows (1% coverage) because v1 script searched per-shelfmark.
DPUL catalog has 36,283 items. V2 script iterates the entire catalog to get all manifests.

Desktop already defaults to external images. Web app needed auto-default logic.

## Plan 260323-gmy-1: Auto-default to JTS source in web browse

**files**: web/pages/browse.py
**action**: Add source_user_override flag + auto-default to JTS when DPUL images available
**done**: Committed as 36ebe881

## Plan 260323-gmy-2: Full DPUL catalog import script

**files**: scripts/import_jts_dpul_v2.py
**action**: New script that iterates all 36,283 DPUL items (paginated listing + detail fetches)
**done**: Committed as a05a0041. User runs manually: `python scripts/import_jts_dpul_v2.py --workers 5`
