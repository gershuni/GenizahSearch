---
quick_id: 260322-uqk
status: complete
commits: [77e562e2, da414fd6]
---

# Summary: FIST Data Fixes — Catalog Dedup + Bibliography Enhancement

## Part 1: Catalog Free Description Deduplication (77e562e2)

Added deduplication to `get_catalog_detail()` in `shared/fjms_service.py`. The FIST source data contains 14,504 duplicate rows in `catalog_free_desc` (same AlmaId + FreeDesc, different SignatureIds) across 12,507 manuscripts. Fixed by deduplicating on (source_name, text) tuple.

**Verified:** Ms. Add. 3207 — 4 entries → 3 unique entries.

## Part 2: Bibliography Enhancement (da414fd6)

### Volume Fix
- Export used `bib.Volume` (11K rows populated). Actual volume data is in `bib.JournalVolumeTxt` (71K rows, mutually exclusive with Volume).
- Fixed: `COALESCE(NULLIF(bib.JournalVolumeTxt, ''), bib.Volume)`

### 8 New Fields Added
| Field | Source | Populated |
|-------|--------|-----------|
| RunningTitleHeb | CODE_Title | 2,138 |
| TitleAcronymHeb | CODE_Title | ~2,000 |
| EVolume | bib.EVolume | 20,478 |
| JournalDate | bib.JournalDate | 441,112 |
| Comment | bib.Comment | 447,191 |
| NoteForDisplay | bib.NoteForDisplay | 1,253 |
| CatalogEntry | bib.CatalogEntry | 7,181 |

### Changes
- **scripts/export_fist_enrichment.py**: 15 → 22 columns, COALESCE volume, parameterized INSERT
- **shared/fjms_service.py**: `_has_bib_extended` flag for backward compat, extended `get_bibliography()` returns
- **web/components/bibliography_dialog.py**: Hebrew title fallback, HTML-escaped detail panel with new fields, Hebrew fields in filter
- **genizah_app.py**: Mirror of web changes for PyQt6
- **tests/test_fjms_service.py**: Updated expected keys

### Backward Compatibility
Service detects extended columns via `SELECT RunningTitleHeb FROM bibliography LIMIT 0` at init. Old sidecars get None for new fields.

### Re-export
Bibliography re-exported from `FIST_DB_BACKUP/FIST.db`: 828,105 rows (133,019 distinct AlmaIds). Backup at `fist_data/fjms_enrichment.db.bak.2026-03-22`.

**Verified:** MS heb. d. 32/6 now shows volume `תרצ"ד, ספר חמישי` and Hebrew title `גנזי קדם (ישן)`.

## External Reviews
- GPT Codex review #1: Found P1 (n_cols mismatch), P2 (TitleAcronymHeb unwired), P2 (HTML escaping). All fixed.
- GPT Codex review #2: Clean pass. Confirmed all fixes. One nit (tr("Volume") → tr("Vol.")) adopted.
