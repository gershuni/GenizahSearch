# External Review: FIST Bibliography Data Enhancement

## Context

GenizahSearch is a Cairo Genizah manuscript research platform (NiceGUI web + PyQt6 desktop). FJMS bibliography data is exported from a FIST SQL Server database (converted to SQLite) into an `fjms_enrichment.db` sidecar via `scripts/export_fist_enrichment.py`, then served via `shared/fjms_service.py` and displayed in bibliography dialogs on both web and desktop.

## Problem

User reported missing bibliography volume data (e.g., `תרצ"ד, ספר חמישי` for MS heb. d. 32/6). Investigation revealed:

1. **Wrong volume field exported**: The export uses `bib.Volume` (populated for 10,867/733,209 rows = 1.5%). The actual volume data lives in `bib.JournalVolumeTxt` (71,132 rows = 9.7%) — these are **mutually exclusive** (zero overlap). There's also `bib.EVolume` (20,478 rows) for English volume text.

2. **Several useful fields not exported at all**: The FIST source has significant additional data that should be surfaced to researchers.

## What Was Already Done (committed)

- **Duplicate catalog entries fix**: Added deduplication to `get_catalog_detail()` in `fjms_service.py` for free descriptions (14,504 duplicate rows across 12,507 manuscripts). Committed as `77e562e2`.

## Proposed Changes

### 1. Export Script (`scripts/export_fist_enrichment.py`)

**Already applied** — adding 8 new columns to the bibliography table:

| New Column | FIST Source | Population | Purpose |
|-----------|------------|-----------|---------|
| `RunningTitleHeb` | `CODE_Title.RunningTitleHeb` | 2,138/4,309 titles | Hebrew title name (currently English only) |
| `TitleAcronymHeb` | `CODE_Title.AcronymHeb` | ~2K | Hebrew acronym |
| `EVolume` | `bib.EVolume` | 20,478 | English volume text |
| `JournalDate` | `bib.JournalDate` | 441,112 | Publication date |
| `Comment` | `bib.Comment` | 447,191 | Scholarly comments |
| `NoteForDisplay` | `bib.NoteForDisplay` | 1,253 | Display notes |
| `CatalogEntry` | `bib.CatalogEntry` | 7,181 | Catalog entry numbers |

**Volume fix**: Changed `bib.Volume` → `COALESCE(NULLIF(bib.JournalVolumeTxt, ''), bib.Volume)` — picks JournalVolumeTxt when available, falls back to Volume.

The INSERT uses a parameterized placeholder count (`n_cols = 23`) to match the new column count.

### 2. Service Layer (`shared/fjms_service.py` — `get_bibliography()`)

**Not yet applied.** Current method (lines 2175-2224) returns 15 fields. Needs to return the 8 new fields.

Proposed: Add to the returned dict:
```python
"running_title_heb": row["RunningTitleHeb"],
"title_acronym_heb": row["TitleAcronymHeb"],
"e_volume": row["EVolume"],
"journal_date": row["JournalDate"],
"comment": row["Comment"],
"note_for_display": row["NoteForDisplay"],
"catalog_entry": row["CatalogEntry"],
```

**Backward compatibility concern**: If the enrichment DB hasn't been re-exported yet, these columns won't exist. The method should handle `KeyError` gracefully (try/except or check column existence).

### 3. Web Bibliography Dialog (`web/components/bibliography_dialog.py`)

**Not yet applied.** Current dialog (lines 19-203) shows 8 columns: Author, Article/Title, Year, Vol., Pages, Type, T, S.

Proposed changes:
- **Table**: No new columns in main table (already quite wide). Keep table as-is.
- **Detail panel** (shown on row click, lines 125-156): Add new fields when populated:
  - Hebrew title (`running_title_heb`)
  - Journal date (`journal_date`)
  - Volume detail: show `e_volume` alongside existing volume if different
  - Comment (`comment`)
  - Note for display (`note_for_display`)
  - Catalog entry (`catalog_entry`)
- **Filter**: Add Hebrew title to searchable text (line 186-191)

### 4. Desktop Bibliography Dialog (`genizah_app.py` — `FjmsBibliographyDialog`)

**Not yet applied.** Current dialog (lines 9563-9730) mirrors web with same 8 columns.

Proposed: Same approach as web — enrich the detail panel (`_on_row_selected`, lines 9702-9729) with new fields. Add Hebrew title to filter searchable text (lines 9692-9699).

### 5. Re-export

Run `export_bibliography()` from the updated script against `C:\GenizahSearch\FIST_DB_BACKUP\FIST.db` to populate the new columns in `fjms_enrichment.db`.

## Questions for Reviewer

1. **Volume display**: Should the main table's "Vol." column show `COALESCE(volume, e_volume)` (best available), or should we keep volume and e_volume separate? The Hebrew JournalVolumeTxt values can be long (e.g., `'אפשטיין, תרביץ ג'`).

2. **Comment field (447K rows)**: These are scholarly comments from FIST. Should they go in the detail panel only (click to see), or should particularly short ones (<50 chars) show as a column? Some samples:
   - `'קבץ על יד יח (תשס"ה)'`
   - `'קטע מתוך אוסף הרכבי'`
   - `'לוין מזכיר מספר קאולי שגוי 2648'`

3. **Backward compatibility**: The service layer will break if someone runs the app without re-exporting the enrichment DB. Options:
   - (A) Wrap new column access in try/except with fallback to None
   - (B) Check column existence once on service init
   - (C) Just require re-export (document in release notes)
   Which approach?

4. **Hebrew title in table**: Currently the "Article/Title" column shows English. Should it show Hebrew when the language toggle is set to Hebrew, or always show both? The Hebrew RunningTitle is available for 2,138/4,309 titles.

5. **JournalDate (441K rows)**: This is a publication date separate from TitleYear. Should it appear in the Year column when TitleYear is empty, or stay in the detail panel?

## Current Code References

- **Export script**: `scripts/export_fist_enrichment.py:789-867`
- **Service**: `shared/fjms_service.py:2175-2224` (`get_bibliography()`)
- **Web dialog**: `web/components/bibliography_dialog.py:19-203`
- **Desktop dialog**: `genizah_app.py:9563-9730` (`FjmsBibliographyDialog`)
