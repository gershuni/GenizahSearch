---
quick_id: 260322-uqk
description: "Deduplicate FJMS catalog free descriptions in get_catalog_detail()"
tasks: 1
---

# Quick Plan: Deduplicate FJMS Catalog Free Descriptions

## Context

`fjms_enrichment.db` `catalog_free_desc` table has 14,504 duplicate rows (same AlmaId + FreeDesc text, different SignatureIds) across 12,507 manuscripts. The `get_catalog_detail()` method loads free descriptions without deduplication, causing duplicate entries in the FJMS catalog dialog.

## Task 1: Add deduplication to free descriptions loading

**Files:** `shared/fjms_service.py`
**Action:** In `get_catalog_detail()`, deduplicate free_descriptions by (source_name, text) tuple after loading, keeping first occurrence. This matches the pattern already used by `get_catalog_records()`.
**Verify:** Run the app and check Ms. Add. 3207 (sys_id 990001398720205171) — should show 3 unique free descriptions instead of 4.
**Done:** No duplicate free descriptions appear in catalog dialog.

## Out of Scope

- **Missing bibliography volumes**: 98% of FIST bib entries lack volume data. This is a source data gap in FIST.db (backup folder), not a code bug. Needs separate investigation against the real FIST.db.
