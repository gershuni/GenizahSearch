# Phase 54: Dimensions Display & Filtering - Context

**Gathered:** 2026-03-26 (display), 2026-03-27 (filtering update)
**Status:** Ready for planning (plan 54-03: filtering)

<domain>
## Phase Boundary

Import all physical measurement data from FIST_Computed_Measurements.xlsx into fjms_enrichment.db, display measurements in browse via a dedicated dialog, AND provide dimension/measurement filtering in search (pre-search and post-search).

**Phase history:** Originally split into Phase 54 (import+display) and Phase 55 (filtering). Filtering is now folded back into Phase 54 as plan 54-03, since the data foundation is already in place.

Plans 54-01 and 54-02 are complete (import + display). Plan 54-03 covers filtering (DIM-02, DIM-03).

</domain>

<decisions>
## Implementation Decisions

### Data Source (plans 54-01/54-02 — COMPLETE)
- **D-01:** Import all 4 data sheets from `fist_data/FIST_Computed_Measurements.xlsx`:
  - Catalog_Sizes (179K rows) — catalog-reported dimensions with pre-normalized SizeX_cm/SizeY_cm
  - Computed_Measurements (434K rows) — image-derived measurements (page size, margins, lines, text density)
  - Extra_Info (743K rows) — AlmaId linkage, Material, Size_Category, DPI, folio/bifolio counts
  - Blank_Images (165K rows) — fragment dimensions for images without text blocks
- **D-02:** Replace existing `catalog_sizes` raw data with normalized cm values from Catalog_Sizes sheet (SizeX_cm, SizeY_cm, InnerSizeX_cm, InnerSizeY_cm). Original raw values are not needed.

### Schema Design (plans 54-01/54-02 — COMPLETE)
- **D-03:** Store at two granularities:
  - Image-level detail tables (per FGP) — full measurement data for detailed dialog
  - Manuscript-level summary table (per AlmaId) — precomputed aggregates for fast filtering/display
- **D-04:** New tables in fjms_enrichment.db (not a separate sidecar):
  - `computed_measurements` — image-level: FGP, AlmaId, Page_Width_cm, Page_Height_cm, margins, Written_Width/Height, Num_Lines, line heights, Text_Density, flags
  - `extra_info` — image-level: FGP, AlmaId, Material, Size_Category, NumFolio, NumBifolio, pixel dims, DPI
  - `manuscript_measurements` — manuscript-level summary: AlmaId, avg/min/max dimensions, material, size_category, line count range, image count
  - Replace `catalog_sizes` with normalized cm values from Catalog_Sizes sheet (add SizeUnit, Flag_WH_Swap, Flag_Unit_Error, Measurement_Scope columns)
- **D-05:** Join key: FGP links Computed_Measurements ↔ Extra_Info. Extra_Info.AlmaId links to manuscript sys_id.

### Display (plans 54-01/54-02 — COMPLETE)
- **D-06:** New "Measurements" button in browse (alongside existing Catalog/Bibliography buttons), opens a dedicated dialog showing all measurement data for the manuscript.
- **D-07:** Dialog shows per-image measurements: page dimensions, margins, written area, line count, text density, material, DPI quality. One section per image/side.
- **D-08:** Display format for dimensions: compact "W × H cm" (e.g., "13.2 × 20.7 cm")
- **D-09:** When a manuscript has multiple size records from different catalogers, show all with source attribution.
- **D-10:** Browse info panel: no inline dimensions display — all measurements accessed via the dialog button.

### Data Quality (plans 54-01/54-02 — COMPLETE)
- **D-11:** Exclude flagged records from display (Flag_DPI_High, Flag_DPI_Low, Flag_Negative_Margin, Flag_BifolioLoc_Error). Bad data is worse than no data.
- **D-12:** Material and Size_Category: show when available, hide when NULL. No "Unknown" fallback — just omit the field.
- **D-13:** Data quality flags from the xlsx (Flag_Unit_Error, Flag_WH_Swap from Catalog_Sizes; DPI flags from Computed_Measurements) stored in DB but used only to exclude bad records, not shown to user.

### Filtering — Plan 54-03 (NEW)

#### Filterable Fields
- **D-15:** Six measurement fields are filterable, grouped into two sections:
  - **Page size**: width (cm), height (cm)
  - **Layout**: line count, line height (mm), text density (per 10cm²)
  - **Material**: paper/parchment/etc.
- **D-16:** Line height (`avg_line_height_mm`) needs to be added to `manuscript_measurements` summary table — aggregate from `computed_measurements.Avg_Line_Height_Text_mm` (AVG of unflagged rows per AlmaId). Store at full precision, display/input at 0.1mm.
- **D-17:** Rationale for field selection: line height and text density are key join-matching signals — fragments of the same manuscript share scribal characteristics (line height, density) but NOT page dimensions or line count (which vary between fragments).

#### Input UX
- **D-18:** Min/max number input pairs per field. Leave blank = no constraint. Compact academic-search style.
- **D-19:** Validate ranges: normalize or reject min > max. Use `>= min` / `<= max` consistently (inclusive boundaries).

#### Placement
- **D-20:** In existing filter panel alongside domain/author/work/date/material. Collapsible "Measurements" section, collapsed by default. Active filter chips visible outside the collapsed section.

#### Pre-Search (DIM-02)
- **D-21:** Add dimension range params to `get_filter_sys_ids()` → returns restrict_sys_ids before Tantivy query. Same mechanism as existing domain/author/date filters.
- **D-22:** Uses `manuscript_measurements` summary table for fast lookup. Known limitation: width and height are aggregated independently (could false-match when a manuscript has multiple catalogers/images). Accepted as approximation — user sees exact values in measurements dialog.
- **D-23:** Material filtering for measurements should use `manuscript_measurements.material` (from extra_info), NOT `catalog_fields.FragmentMaterial` — avoids mixed-source confusion when dimension filters are active.

#### Post-Search (DIM-03)
- **D-24:** Same filter inputs in post-search result filter area. Identical UX to pre-search.
- **D-25:** **Apply button** (not live debounce) for post-search dimension filters. Avoids partial-range flicker. Enter key also triggers apply.

#### NULL Handling
- **D-26:** Manuscripts with no measurement data are **excluded** when any measurement filter is active. Matches researcher expectation (unknown ≠ match). No "include unknown" toggle initially.

### Both Apps
- **D-14:** Web (NiceGUI) and desktop (PyQt6) both get the Measurements button, dialog, AND filtering.

### Claude's Discretion
- Table schema details (indexes, column types, exact aggregation formulas for summary table)
- Dialog layout and visual design within the button+dialog pattern
- Import script structure and batch processing approach
- How to aggregate image-level data to manuscript-level summary (max, avg, median — Claude picks)
- Desktop pre-search filter dialog layout for measurement fields
- Exact filter chip display format

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Data Source
- `fist_data/FIST_Computed_Measurements.xlsx` — Source data file with 6 data sheets + About + Column_Reference
  - Sheet "About" — detailed documentation of all computed fields, DPI pipeline, bifolio handling, flag definitions
  - Sheet "Column_Reference" — 196 column descriptions with DB source mapping
  - Sheet "Data_Quality_Flags" — 852 flagged records with severity and recommended action

### Existing Code (filtering-relevant)
- `shared/fjms_service.py` — `get_filter_sys_ids()` (line ~848) — pre-search filtering entry point; already has material_include/exclude params; needs dimension range params added
- `shared/fjms_service.py` — `get_measurements()` (line ~2578) — measurements dialog data fetch
- `shared/fjms_service.py` — `has_measurements()` (line ~2658) — checks measurement data availability
- `web/components/filter_panel.py` — shared filter logic (domain/author/work builders); dimension filters integrate here
- `web/pages/search.py` — web search page (~3,204 lines); pre-search and post-search filter wiring
- `genizah_app.py` line ~9084 — `PreSearchFilterDialog` (desktop); needs measurement fields added
- `genizah_app.py` line ~9559 — desktop pre-search filter apply logic
- `scripts/import_measurements.py` — import script; `manuscript_measurements` table schema at line ~496

### Existing Patterns
- `web/components/catalog_dialog.py` — existing catalog detail dialog pattern
- `scripts/export_fist_enrichment.py` — existing FIST→enrichment.db export script pattern

### Research
- `.planning/research/ARCHITECTURE.md` — integration points for dimension features
- `.planning/research/PITFALLS.md` — data quality concerns and performance risks

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `FjmsService.get_filter_sys_ids()` — already handles domain/author/work/date/material intersection filtering; dimension params plug into same pattern
- `manuscript_measurements` table — already indexed on width/height columns; needs `avg_line_height_mm` column added
- `web/components/filter_panel.py` — shared filter option builders; extend for measurement options
- Desktop `PreSearchFilterDialog` — existing PyQt6 dialog with domain/author/work/date/material; add measurement section

### Established Patterns
- Pre-search: `get_filter_sys_ids()` returns `set[str]` → passed as `restrict_sys_ids` to search
- Post-search: web result filters narrow displayed results client-side with Apply button
- Filter panel sections: collapsible groups with active chips
- SQLite sidecar tables with AlmaId as join key

### Integration Points
- `get_filter_sys_ids()`: add width_min/max, height_min/max, line_count_min/max, line_height_min/max, text_density_min/max params
- `manuscript_measurements`: add `avg_line_height_mm` column + index
- Web filter panel: add "Measurements" collapsible section with 6 min/max input pairs + Apply button (post-search)
- Desktop `PreSearchFilterDialog`: add measurement fields section
- Both apps: wire dimension params through search pipeline

</code_context>

<specifics>
## Specific Ideas

- User provided `FIST_Computed_Measurements.xlsx` (generated 2026-02-18) as the authoritative measurement source
- The xlsx About sheet documents that bifolio Right_Margin_cm values are corrected vs the FJMS website (which has a formula error for 98.7% of bifolios)
- Grid-calibrated images (DpiGrid > 0, ~74K) are reliable; ruler-only (~360K) are approximate
- PuzzleRatio and CentroidDescriptor in Blank_Images could be useful for puzzle/join features (future)
- Key user insight: line height and text density are MORE important than page dimensions for join matching — fragments of the same manuscript share scribal characteristics but not physical dimensions

</specifics>

<deferred>
## Deferred Ideas

- **PuzzleRatio/CentroidDescriptor for join suggestions** — future phase, interesting for puzzle feature
- **Image-derived DPI for puzzle canvas calibration** — could improve physical-scale puzzle assembly
- **"Include manuscripts with unknown measurements" toggle** — may add later if users want exploratory filtering
- **"N results would match" live preview** — enhancement for iterative range exploration

</deferred>

<review_findings>
## External Review Findings (2026-03-27)

### Addressed in Decisions
1. **Unpaired aggregation (P1)**: Width/height aggregated independently could false-match. Accepted as approximation (D-22).
2. **Material source split (P2)**: Use `manuscript_measurements.material` for consistency (D-23).
3. **Apply button + Enter**: Post-search uses Apply button with Enter-to-apply (D-25).
4. **Six fields grouping**: Collapsed section with active chips (D-20).
5. **Line height precision**: Full precision stored, 0.1mm input/display (D-16).
6. **NULL exclusion**: Manuscripts without data excluded when filter active (D-26).

### Testing Requirements
- Paired width+height semantics (false-positive awareness)
- NULL exclusion behavior
- Blank-image manuscripts
- Boundary equality (inclusive)
- min > max validation
- Cross-app parity for the same filter set

</review_findings>

---

*Phase: 54-dimensions-display-filtering*
*Context gathered: 2026-03-26 (display), 2026-03-27 (filtering)*
