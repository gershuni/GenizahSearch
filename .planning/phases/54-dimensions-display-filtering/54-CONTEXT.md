# Phase 54: Dimensions Display & Filtering - Context

**Gathered:** 2026-03-26
**Status:** Ready for planning

<domain>
## Phase Boundary

Import all physical measurement data from FIST_Computed_Measurements.xlsx into fjms_enrichment.db and display measurements in browse via a dedicated dialog. This phase covers data import + display only. Filtering (pre-search and post-search dimension/material filters) is deferred to Phase 55.

**Phase split decision:** Original Phase 54 covered import+display+filtering. Scope expanded due to full xlsx import (1.5M rows across 4 sheets). Split into:
- Phase 54: Import + display (this phase)
- Phase 55: Dimension/material filtering (new phase, renumber existing 55→56, 56→57, 57→58)

</domain>

<decisions>
## Implementation Decisions

### Data Source
- **D-01:** Import all 4 data sheets from `fist_data/FIST_Computed_Measurements.xlsx`:
  - Catalog_Sizes (179K rows) — catalog-reported dimensions with pre-normalized SizeX_cm/SizeY_cm
  - Computed_Measurements (434K rows) — image-derived measurements (page size, margins, lines, text density)
  - Extra_Info (743K rows) — AlmaId linkage, Material, Size_Category, DPI, folio/bifolio counts
  - Blank_Images (165K rows) — fragment dimensions for images without text blocks
- **D-02:** Replace existing `catalog_sizes` raw data with normalized cm values from Catalog_Sizes sheet (SizeX_cm, SizeY_cm, InnerSizeX_cm, InnerSizeY_cm). Original raw values are not needed.

### Schema Design
- **D-03:** Store at two granularities:
  - Image-level detail tables (per FGP) — full measurement data for detailed dialog
  - Manuscript-level summary table (per AlmaId) — precomputed aggregates for fast filtering/display
- **D-04:** New tables in fjms_enrichment.db (not a separate sidecar):
  - `computed_measurements` — image-level: FGP, AlmaId, Page_Width_cm, Page_Height_cm, margins, Written_Width/Height, Num_Lines, line heights, Text_Density, flags
  - `extra_info` — image-level: FGP, AlmaId, Material, Size_Category, NumFolio, NumBifolio, pixel dims, DPI
  - `manuscript_measurements` — manuscript-level summary: AlmaId, avg/min/max dimensions, material, size_category, line count range, image count
  - Replace `catalog_sizes` with normalized cm values from Catalog_Sizes sheet (add SizeUnit, Flag_WH_Swap, Flag_Unit_Error, Measurement_Scope columns)
- **D-05:** Join key: FGP links Computed_Measurements ↔ Extra_Info. Extra_Info.AlmaId links to manuscript sys_id.

### Display
- **D-06:** New "Measurements" button in browse (alongside existing Catalog/Bibliography buttons), opens a dedicated dialog showing all measurement data for the manuscript.
- **D-07:** Dialog shows per-image measurements: page dimensions, margins, written area, line count, text density, material, DPI quality. One section per image/side.
- **D-08:** Display format for dimensions: compact "W × H cm" (e.g., "13.2 × 20.7 cm")
- **D-09:** When a manuscript has multiple size records from different catalogers, show all with source attribution.
- **D-10:** Browse info panel: no inline dimensions display — all measurements accessed via the dialog button.

### Data Quality
- **D-11:** Exclude flagged records from display (Flag_DPI_High, Flag_DPI_Low, Flag_Negative_Margin, Flag_BifolioLoc_Error). Bad data is worse than no data.
- **D-12:** Material and Size_Category: show when available, hide when NULL. No "Unknown" fallback — just omit the field.
- **D-13:** Data quality flags from the xlsx (Flag_Unit_Error, Flag_WH_Swap from Catalog_Sizes; DPI flags from Computed_Measurements) stored in DB but used only to exclude bad records, not shown to user.

### Both Apps
- **D-14:** Web (NiceGUI) and desktop (PyQt6) both get the Measurements button and dialog.

### Claude's Discretion
- Table schema details (indexes, column types, exact aggregation formulas for summary table)
- Dialog layout and visual design within the button+dialog pattern
- Import script structure and batch processing approach
- How to aggregate image-level data to manuscript-level summary (max, avg, median — Claude picks)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Data Source
- `fist_data/FIST_Computed_Measurements.xlsx` — Source data file with 6 data sheets + About + Column_Reference
  - Sheet "About" — detailed documentation of all computed fields, DPI pipeline, bifolio handling, flag definitions
  - Sheet "Column_Reference" — 196 column descriptions with DB source mapping
  - Sheet "Data_Quality_Flags" — 852 flagged records with severity and recommended action

### Existing Code
- `shared/fjms_service.py` — FjmsService class; `get_catalog_detail()` already fetches from catalog_sizes (line ~2360)
- `shared/fjms_service.py` — `get_filter_sys_ids()` (line ~848) — pattern for pre-search filtering (Phase 55)
- `web/components/catalog_dialog.py` — existing catalog detail dialog pattern to follow for measurements dialog
- `scripts/export_fist_enrichment.py` — existing FIST→enrichment.db export script pattern
- `web/components/filter_panel.py` — post-search filter panel (Phase 55)

### Research
- `.planning/research/ARCHITECTURE.md` — integration points for dimension features
- `.planning/research/PITFALLS.md` — data quality concerns and performance risks

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `FjmsService.get_catalog_detail()` — already fetches catalog_sizes; needs extension for new measurement tables
- `web/components/catalog_dialog.py` — dialog pattern with bilingual support, can model measurements dialog after this
- `scripts/export_fist_enrichment.py` — FIST→SQLite export pattern (openpyxl not needed at runtime; import script reads xlsx once)

### Established Patterns
- Browse buttons open dedicated dialogs (Catalog, Bibliography) — measurements follows same pattern
- Per-sys_id enrichment via batch lookup in search results
- SQLite sidecar tables with AlmaId as join key

### Integration Points
- Browse page: add Measurements button next to Catalog/Bibliography buttons (web + desktop)
- FjmsService: add `get_measurements()` method for dialog data
- FjmsService: add `get_manuscript_summary()` for quick lookup (Phase 55 filtering)
- Import script: new script to read xlsx and populate fjms_enrichment.db tables

</code_context>

<specifics>
## Specific Ideas

- User provided `FIST_Computed_Measurements.xlsx` (generated 2026-02-18) as the authoritative measurement source
- The xlsx About sheet documents that bifolio Right_Margin_cm values are corrected vs the FJMS website (which has a formula error for 98.7% of bifolios)
- Grid-calibrated images (DpiGrid > 0, ~74K) are reliable; ruler-only (~360K) are approximate
- PuzzleRatio and CentroidDescriptor in Blank_Images could be useful for puzzle/join features (future)

</specifics>

<deferred>
## Deferred Ideas

- **Dimension range filtering (pre-search + post-search)** — moved to Phase 55 due to scope split
- **Material as a search filter** — Phase 55 alongside dimension filtering
- **PuzzleRatio/CentroidDescriptor for join suggestions** — future phase, interesting for puzzle feature
- **Image-derived DPI for puzzle canvas calibration** — could improve physical-scale puzzle assembly

</deferred>

---

*Phase: 54-dimensions-display-filtering*
*Context gathered: 2026-03-26*
