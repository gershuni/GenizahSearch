# Phase 54: Dimensions Display & Filtering - Research

**Researched:** 2026-03-26
**Domain:** FIST measurement data import + NiceGUI/PyQt6 dialog display
**Confidence:** HIGH

## Summary

Phase 54 imports 4 data sheets (~1.5M total rows) from `fist_data/FIST_Computed_Measurements.xlsx` into `fjms_enrichment.db` and adds a "Measurements" dialog button to both web and desktop browse pages. The xlsx contains pre-computed, normalized physical measurements from the FIST image processing pipeline (image-derived dimensions, margins, line counts, text density, material, size category) plus catalog-reported sizes with pre-normalized centimeter values.

The data architecture has two key join challenges: (1) Computed_Measurements and Blank_Images link via FGP, and Extra_Info provides the FGP-to-AlmaId mapping (~91% coverage); (2) Catalog_Sizes uses FIST inventory Shelfmark (not AlmaId), requiring a Shelfmark+Collection join through FIST.db to resolve AlmaId. A manuscript-level summary table must be pre-aggregated during import for fast lookup and future filtering (Phase 55).

**Primary recommendation:** Write a standalone import script (`scripts/import_measurements.py`) using openpyxl (already installed, v3.1.5) that reads the xlsx once, builds an in-memory FGP-to-AlmaId lookup from Extra_Info, and writes 4 new tables + replaces catalog_sizes in fjms_enrichment.db. Then add `get_measurements()` to FjmsService and create a Measurements dialog following the existing catalog_dialog.py pattern.

## Project Constraints (from CLAUDE.md)

- Python 3.10+, NiceGUI for web, PyQt6 for desktop
- Both apps must be maintained; shared service layer
- Hebrew RTL support required; bilingual (Hebrew/English)
- Data stored in SQLite sidecars (fjms_enrichment.db)
- AlmaId is the manuscript join key (FIST AlmaId is integer, CSV system_number is string -- CAST required)
- Tests via pytest; ~946 existing tests
- Update docs/OPEN_ISSUES.md if bugs found/fixed
- Run `python scripts/check_docs.py` before finishing

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Import all 4 data sheets from `fist_data/FIST_Computed_Measurements.xlsx` (Catalog_Sizes 179K, Computed_Measurements 434K, Extra_Info 743K, Blank_Images 165K)
- **D-02:** Replace existing `catalog_sizes` raw data with normalized cm values from Catalog_Sizes sheet (SizeX_cm, SizeY_cm, InnerSizeX_cm, InnerSizeY_cm). Original raw values not needed.
- **D-03:** Store at two granularities: image-level detail tables (per FGP) and manuscript-level summary table (per AlmaId)
- **D-04:** New tables in fjms_enrichment.db: `computed_measurements`, `extra_info`, `manuscript_measurements` (summary), plus replace `catalog_sizes` with normalized cm values
- **D-05:** Join key: FGP links Computed_Measurements <-> Extra_Info. Extra_Info.AlmaId links to manuscript sys_id.
- **D-06:** New "Measurements" button in browse alongside Catalog/Bibliography buttons, opens dedicated dialog
- **D-07:** Dialog shows per-image measurements: page dimensions, margins, written area, line count, text density, material, DPI quality
- **D-08:** Display format: compact "W x H cm" (e.g., "13.2 x 20.7 cm")
- **D-09:** Multiple size records from different catalogers shown with source attribution
- **D-10:** Browse info panel: no inline dimensions -- all measurements via dialog button only
- **D-11:** Exclude flagged records from display (Flag_DPI_High, Flag_DPI_Low, Flag_Negative_Margin, Flag_BifolioLoc_Error)
- **D-12:** Material and Size_Category: show when available, hide when NULL. No "Unknown" fallback.
- **D-13:** Data quality flags stored in DB but used only to exclude bad records, not shown to user
- **D-14:** Web (NiceGUI) and desktop (PyQt6) both get the Measurements button and dialog

### Claude's Discretion
- Table schema details (indexes, column types, exact aggregation formulas for summary table)
- Dialog layout and visual design within the button+dialog pattern
- Import script structure and batch processing approach
- How to aggregate image-level data to manuscript-level summary (max, avg, median)

### Deferred Ideas (OUT OF SCOPE)
- Dimension range filtering (pre-search + post-search) -- Phase 55
- Material as a search filter -- Phase 55
- PuzzleRatio/CentroidDescriptor for join suggestions -- future phase
- Image-derived DPI for puzzle canvas calibration -- future phase
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DIM-01 | User can see manuscript dimensions in browse and result views | Measurements dialog button in browse (D-06/D-07); catalog_sizes replacement with normalized cm (D-02); `get_measurements()` in FjmsService |
| DIM-02 | User can filter search by dimension range as pre-search filter | DEFERRED to Phase 55 per CONTEXT.md split decision |
| DIM-03 | User can filter within results by dimension range as post-search filter | DEFERRED to Phase 55 per CONTEXT.md split decision |
| DIM-04 | Dimensions normalized across units with display formatting | Catalog_Sizes xlsx has pre-normalized SizeX_cm/SizeY_cm; Computed_Measurements already in cm; import script stores cm values; display uses "W x H cm" format |

**Note:** DIM-02 and DIM-03 are covered by the `manuscript_measurements` summary table schema (created in this phase) but the actual filtering UI/logic is Phase 55.
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| openpyxl | 3.1.5 | Read xlsx sheets (import script only) | Already installed; read_only mode handles 275MB file efficiently |
| sqlite3 | stdlib | Write to fjms_enrichment.db | Standard Python, matches existing export pattern |
| NiceGUI | (project ver) | Web measurements dialog | Project standard for web UI |
| PyQt6 | (project ver) | Desktop measurements dialog | Project standard for desktop UI |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tqdm | (installed) | Progress bars in import script | For long-running xlsx reads (~1.5M rows) |

### Alternatives Considered
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| openpyxl | pandas | pandas adds ~200MB memory overhead for 1.5M rows; openpyxl read_only streams rows; pandas not installed |

## Architecture Patterns

### Import Script Structure
```
scripts/import_measurements.py
    read_extra_info()       -> {FGP: AlmaId} lookup (743K rows)
    read_computed_measurements() -> computed_measurements table (434K, joined with FGP->AlmaId)
    read_blank_images()     -> (skip for Phase 54 or store minimal; 165K rows)
    read_catalog_sizes()    -> catalog_sizes replacement (179K rows, needs FIST.db for AlmaId)
    build_manuscript_summary() -> manuscript_measurements table (aggregated from all above)
```

### Data Flow: Catalog_Sizes AlmaId Resolution
**Critical finding:** The xlsx Catalog_Sizes sheet has `Shelfmark` (FIST inventory shelfmark) + `Collection` as identifiers but NO AlmaId. Shelfmark is NOT unique across collections (e.g., "100" appears in 4+ collections).

**Resolution strategy:** Two options:
1. **Use FIST.db SQL joins** (like existing `export_fist_enrichment.py`): Read Catalog_Sizes from FIST.db directly via `dbo_CatalogMultiSize` with the full join chain, then merge pre-normalized cm values from xlsx by matching on (Shelfmark, Collection) or UnitCatalogRecId.
2. **Build lookup from FIST.db**: Create a (Shelfmark, Collection) -> AlmaId mapping from FIST.db's `dbo_Inventory` + `dbo_InventoryAlma` + collection code tables, then join with xlsx rows.

**Recommendation:** Option 1 is simpler -- keep the existing FIST.db join for Catalog_Sizes (which already produces AlmaId) but ADD the normalized cm columns from the xlsx. The xlsx pre-computed SizeX_cm/SizeY_cm handles unit conversion. The import script can read both FIST.db and xlsx.

### Data Flow: Computed_Measurements + Extra_Info
```
Extra_Info (743K rows) --[FGP]--> AlmaId lookup
Computed_Measurements (434K rows) --[FGP join]--> AlmaId from Extra_Info lookup
Result: computed_measurements table with AlmaId column added
```

**AlmaId float precision issue:** openpyxl reads AlmaId from Extra_Info as a float (e.g., `9.900017468002052e+17`). Must convert to integer then to string: `str(int(alma_id))`. This matches the FIST AlmaId handling pattern (CAST(AlmaId AS TEXT)).

### Recommended Table Schema

```sql
-- Replace existing catalog_sizes with normalized cm values
CREATE TABLE catalog_sizes (
    AlmaId TEXT NOT NULL,
    UnitCatalogRecId INTEGER NOT NULL,
    SizeX_cm REAL,          -- normalized from xlsx
    SizeY_cm REAL,          -- normalized from xlsx
    InnerSizeX_cm REAL,
    InnerSizeY_cm REAL,
    SizeUnit TEXT,           -- original unit (cm/mm/inch/None)
    Measurement_Scope TEXT,  -- 'leaf', 'bifolio', etc.
    Flag_WH_Swap TEXT,       -- NULL or description
    Flag_Unit_Error TEXT     -- NULL or description
);
CREATE INDEX idx_catsz_alma ON catalog_sizes(AlmaId);

-- Image-level computed measurements (434K rows, flagged rows included but marked)
CREATE TABLE computed_measurements (
    FGP TEXT NOT NULL,
    AlmaId TEXT,             -- from Extra_Info join (NULL for ~9% without AlmaId)
    Image_Side TEXT,
    Component_Num INTEGER,
    Bifolio_Side TEXT,
    Page_Width_cm REAL,
    Page_Height_cm REAL,
    Num_Lines INTEGER,
    Left_Margin_cm REAL,
    Right_Margin_cm REAL,
    Top_Margin_cm REAL,
    Bottom_Margin_cm REAL,
    Written_Width_cm REAL,
    Written_Height_cm REAL,
    Avg_Line_Height_Text_mm REAL,
    Text_Density_per10cm REAL,
    DpiGrid INTEGER,
    DisplayDPI INTEGER,
    Flag_DPI_High INTEGER DEFAULT 0,
    Flag_DPI_Low INTEGER DEFAULT 0,
    Flag_Negative_Margin INTEGER DEFAULT 0,
    Flag_BifolioLoc_Error INTEGER DEFAULT 0
);
CREATE INDEX idx_cm_alma ON computed_measurements(AlmaId);
CREATE INDEX idx_cm_fgp ON computed_measurements(FGP);

-- Image-level extra info (743K rows)
CREATE TABLE extra_info (
    FGP TEXT NOT NULL PRIMARY KEY,
    AlmaId TEXT,
    Shelfmark TEXT,
    Material TEXT,            -- Paper/Papyrus/Vellum/Wood/Mix
    Size_Category TEXT,       -- Small/Medium/Large/Very large/Oversize
    NumFolio INTEGER,
    NumBifolio INTEGER,
    PixelWidth INTEGER,
    PixelHeight INTEGER,
    Image_Type TEXT,
    Rotation_Angle_deg REAL
);
CREATE INDEX idx_ei_alma ON extra_info(AlmaId);

-- Pre-aggregated manuscript summary (one row per AlmaId)
CREATE TABLE manuscript_measurements (
    AlmaId TEXT NOT NULL PRIMARY KEY,
    -- From catalog_sizes (catalog-reported, most authoritative for physical size)
    catalog_width_cm REAL,    -- MAX of SizeX_cm across records
    catalog_height_cm REAL,   -- MAX of SizeY_cm
    catalog_inner_width_cm REAL,
    catalog_inner_height_cm REAL,
    catalog_count INTEGER,    -- number of catalog size records
    -- From computed_measurements (image-derived, unflagged only)
    computed_width_cm REAL,   -- MAX of Page_Width_cm (unflagged)
    computed_height_cm REAL,  -- MAX of Page_Height_cm (unflagged)
    avg_num_lines REAL,       -- AVG of Num_Lines (unflagged)
    min_num_lines INTEGER,
    max_num_lines INTEGER,
    avg_text_density REAL,
    -- From extra_info
    material TEXT,             -- most common material for this AlmaId
    size_category TEXT,        -- most common size_category
    image_count INTEGER        -- total images for this manuscript
);
CREATE INDEX idx_ms_width ON manuscript_measurements(catalog_width_cm);
CREATE INDEX idx_ms_height ON manuscript_measurements(catalog_height_cm);
```

**Aggregation rationale:** Use MAX for catalog dimensions (largest reported size is typically the full page; smaller values may be written area). Use MAX for computed dimensions (different images may capture different sides). Use AVG for line counts/density (represents typical page). Use MODE for material/size_category (most common across images).

### Dialog Pattern (Web)

Follow `web/components/catalog_dialog.py` pattern:
```python
# web/components/measurements_dialog.py
def show_measurements_dialog(sys_id: str, shelfmark: str, fjms_service=None):
    """Show measurements dialog with catalog + computed data."""
    if fjms_service is None:
        from shared.fjms_service import get_fjms_service
        fjms_service = get_fjms_service(thread_safe=True)

    data = fjms_service.get_measurements(sys_id)
    # ... build dialog with ui.dialog()
```

### Dialog Pattern (Desktop)

Follow `FjmsCatalogDialog` pattern (QDialog with QTextBrowser for HTML content):
```python
class FjmsMeasurementsDialog(QDialog):
    def __init__(self, data: dict, sys_id: str = '', shelfmark: str = '', parent=None):
        # ... QTextBrowser with HTML table
```

### Button Placement

Both apps: add "Measurements" button alongside existing "Catalog rec." and "Bib. FJMS" buttons.

**Web (browse.py ~line 1048-1110):** In `_populate_bib_catalog_buttons()`, add measurements button with a teal/green chip style to visually distinguish from catalog (indigo) and bibliography (purple).

**Desktop (genizah_app.py ~line 5948, 8131):** Add `btn_rd_measurements` alongside `btn_rd_catalog`, open `FjmsMeasurementsDialog`.

### Anti-Patterns to Avoid
- **Loading xlsx at runtime:** The xlsx is 275MB. Import script runs ONCE offline, stores results in SQLite. Never read xlsx in the app.
- **Displaying flagged data:** Per D-11, exclude rows where any flag column = 1. Filter in SQL query, not in display code.
- **Joining Catalog_Sizes by Shelfmark alone:** Shelfmark is NOT unique. Must use Shelfmark+Collection pair or go through FIST.db join chain.
- **Truncating AlmaId floats:** openpyxl reads 18-digit integers as floats. `str(int(value))` preserves precision. Do NOT use `str(value)` which gives scientific notation.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Unit normalization | Custom mm/inch->cm converter | xlsx pre-normalized SizeX_cm/SizeY_cm | Already done by data provider; includes flag corrections |
| W/H swap detection | Heuristic width>height check | xlsx Flag_WH_Swap column | Provider pre-analyzed with source-specific knowledge |
| Bifolio margin correction | Manual recalculation | xlsx Computed_Measurements values | Provider fixed the 98.7% bifolio formula error |
| AlmaId resolution for Catalog_Sizes | Shelfmark text matching | FIST.db SQL join chain | Existing pattern in export_fist_enrichment.py |

## Common Pitfalls

### Pitfall 1: AlmaId Float Precision Loss
**What goes wrong:** openpyxl reads 18-digit AlmaIds as Python floats, losing last 2-3 digits.
**Why it happens:** Excel stores large integers as IEEE 754 doubles (53-bit mantissa, ~15.9 decimal digits).
**How to avoid:** Convert with `str(int(value))` immediately on read. Verify a sample against known AlmaIds.
**Warning signs:** AlmaIds ending in `0000` or not matching any manuscript in libraries.csv.

### Pitfall 2: Catalog_Sizes Shelfmark Ambiguity
**What goes wrong:** "Shelfmark=100" maps to 4+ different manuscripts across collections.
**Why it happens:** FIST inventory shelfmarks are collection-scoped, not globally unique.
**How to avoid:** For Catalog_Sizes, use FIST.db SQL join (existing pattern in `export_fist_enrichment.py`) to get AlmaId, then merge pre-normalized cm values from xlsx by matching on UnitCatalogRecId or (Shelfmark, Collection).
**Warning signs:** AlmaId count differs significantly from existing catalog_sizes (should be ~104K).

### Pitfall 3: Memory Pressure from 1.5M Rows
**What goes wrong:** Loading all xlsx data into memory at once causes swap/OOM.
**Why it happens:** openpyxl in read_only mode streams rows, but building a 743K-entry dict for FGP->AlmaId lookup takes ~100MB.
**How to avoid:** Build FGP->AlmaId lookup first (Extra_Info), then stream other sheets row-by-row, writing batches to SQLite. Never hold all rows in memory.
**Warning signs:** Import script taking >5 minutes or consuming >1GB RAM.

### Pitfall 4: Duplicate Rows in Catalog_Sizes
**What goes wrong:** Same shelfmark appears 3-7 times with identical values (multiple InventorySignature paths).
**Why it happens:** FIST's many-to-many inventory/signature relationship produces cross-product duplicates.
**How to avoid:** Use DISTINCT in SQL or deduplicate in import script. The existing `export_fist_enrichment.py` uses `SELECT DISTINCT`.
**Warning signs:** Row count much higher than expected (current catalog_sizes: 178K rows for 104K AlmaIds).

### Pitfall 5: Existing catalog_sizes Consumers Break
**What goes wrong:** Code reading catalog_sizes expects old column names (SizeX, SizeY) but new schema has SizeX_cm, SizeY_cm.
**Why it happens:** Replacing table schema without updating all consumers.
**How to avoid:** Search codebase for all `catalog_sizes` references. Update `FjmsService.get_catalog_detail()` sizes section and both catalog dialog renderers (web + desktop).
**Warning signs:** "no such column: SizeX" errors in catalog dialog.

## Code Examples

### Reading xlsx with openpyxl read_only mode
```python
# Source: openpyxl docs + project pattern
import openpyxl

wb = openpyxl.load_workbook('fist_data/FIST_Computed_Measurements.xlsx', read_only=True)
ws = wb['Extra_Info']

fgp_to_alma = {}
for row in ws.iter_rows(min_row=2, values_only=True):
    fgp = row[0]   # FGP column
    alma = row[4]   # AlmaId column
    if fgp and alma is not None:
        fgp_to_alma[fgp] = str(int(alma))  # float -> int -> str

wb.close()
```

### FjmsService.get_measurements() pattern
```python
# Following get_catalog_detail() pattern in shared/fjms_service.py
def get_measurements(self, sys_id: str) -> dict:
    """Get all measurement data for a manuscript."""
    if self._conn is None:
        return {"catalog_sizes": [], "computed": [], "extra_info": [], "summary": None}

    result = {"catalog_sizes": [], "computed": [], "extra_info": [], "summary": None}

    # Summary row
    cursor = self._conn.execute(
        "SELECT * FROM manuscript_measurements WHERE AlmaId = ?", (sys_id,)
    )
    row = cursor.fetchone()
    if row:
        result["summary"] = dict(row)

    # Catalog sizes (with source attribution)
    cursor = self._conn.execute(
        "SELECT * FROM catalog_sizes WHERE AlmaId = ? "
        "AND Flag_WH_Swap IS NULL AND Flag_Unit_Error IS NULL",
        (sys_id,)
    )
    result["catalog_sizes"] = [dict(r) for r in cursor]

    # Computed measurements (exclude flagged)
    cursor = self._conn.execute(
        "SELECT * FROM computed_measurements WHERE AlmaId = ? "
        "AND Flag_DPI_High = 0 AND Flag_DPI_Low = 0 "
        "AND Flag_Negative_Margin = 0 AND Flag_BifolioLoc_Error = 0",
        (sys_id,)
    )
    result["computed"] = [dict(r) for r in cursor]

    return result
```

### Web dialog pattern (following catalog_dialog.py)
```python
# web/components/measurements_dialog.py
from nicegui import ui
from web.translations import tr, get_language

def show_measurements_dialog(sys_id: str, shelfmark: str, fjms_service=None):
    if fjms_service is None:
        from shared.fjms_service import get_fjms_service
        fjms_service = get_fjms_service(thread_safe=True)

    data = fjms_service.get_measurements(sys_id)
    lang = get_language()
    is_heb = lang == 'he'

    dialog = ui.dialog().props('maximized=false full-width')
    with dialog, ui.card().classes('w-full max-w-[900px] max-h-[90vh]').style(
        'overflow: hidden; display: flex; flex-direction: column;'
    ):
        # Header with teal gradient
        with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
            'background: linear-gradient(135deg, #00695c, #26a69a); color: white;'
        ):
            with ui.row().classes('items-center gap-2'):
                ui.icon('straighten').classes('text-xl')
                ui.label(f'{tr("Measurements")} -- {shelfmark}').classes('text-lg font-bold')
            ui.button(icon='close', on_click=dialog.close).props('flat dense round').classes('text-white')

        # Content...
    dialog.open()
```

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | pytest.ini (or pyproject.toml) |
| Quick run command | `pytest tests/test_fjms_service.py -x -q` |
| Full suite command | `pytest tests/ -x -q` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| DIM-01 | Measurements visible in browse dialog | unit | `pytest tests/test_measurements.py::test_get_measurements -x` | Wave 0 |
| DIM-04 | Dimensions normalized to cm | unit | `pytest tests/test_measurements.py::test_catalog_sizes_normalized -x` | Wave 0 |
| DIM-04 | AlmaId float conversion correct | unit | `pytest tests/test_measurements.py::test_alma_id_precision -x` | Wave 0 |
| DIM-01 | Summary aggregation correct | unit | `pytest tests/test_measurements.py::test_manuscript_summary -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_measurements.py -x -q`
- **Per wave merge:** `pytest tests/ -x -q`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_measurements.py` -- covers DIM-01, DIM-04 (import validation, service methods, summary aggregation)
- [ ] Test fixtures: small sample xlsx data or pre-built test SQLite with measurement tables

## Data Statistics (verified from source)

| Sheet | Rows | Key Column | AlmaId Source |
|-------|------|------------|---------------|
| Computed_Measurements | 434,370 | FGP | Via Extra_Info FGP->AlmaId join |
| Extra_Info | 742,854 | FGP (PK), AlmaId | Direct (~91% have AlmaId) |
| Blank_Images | 164,657 | FGP | Via Extra_Info FGP->AlmaId join |
| Catalog_Sizes | 179,428 | Shelfmark+Collection | Via FIST.db join chain |
| Data_Quality_Flags | 852 | FGP | Reference only |

**Current catalog_sizes in enrichment.db:** 178,579 rows, 104,650 distinct AlmaIds.
**Current size range (raw, unnormalized):** SizeX -3.0 to 7,230.0; SizeY 0.0 to 8,617.0 -- confirms mixed units/outliers.

## Open Questions

1. **Blank_Images scope in Phase 54**
   - What we know: 165K rows, fragment dimensions but no text block data. Deferred features (PuzzleRatio) use this.
   - What's unclear: Whether to import Blank_Images now or defer to a future phase.
   - Recommendation: Import the table now (minimal additional work) to avoid re-running the import later. Store just FGP, AlmaId, Fragment_Width_cm, Fragment_Height_cm, IsNotWhole.

2. **Raw_Data sheet (434K rows, 65 columns)**
   - What we know: Contains all underlying pixel values for Computed_Measurements.
   - What's unclear: Whether any consumer needs raw pixel data.
   - Recommendation: Skip Raw_Data entirely. Computed_Measurements has all the cm-converted values needed.

3. **Catalog_Sizes merge strategy**
   - What we know: Current export uses FIST.db SQL joins. xlsx has pre-normalized cm values.
   - What's unclear: Whether to read from xlsx + FIST.db hybrid, or just augment existing FIST.db export.
   - Recommendation: Keep existing FIST.db join path for AlmaId resolution, but ADD the pre-normalized cm columns and quality flags from xlsx. Match on (Shelfmark, Collection, SourceId, SubId) between xlsx and FIST.db results.

## Sources

### Primary (HIGH confidence)
- `fist_data/FIST_Computed_Measurements.xlsx` About sheet -- authoritative documentation of all fields, DPI pipeline, flags
- `fist_data/fjms_enrichment.db` -- direct schema inspection of current catalog_sizes table
- `FIST_DB_BACKUP/FIST.db` -- dbo_InventoryAlma, dbo_Inventory table structure verified
- `scripts/export_fist_enrichment.py` -- existing FIST->SQLite export pattern (lines 415-471)
- `shared/fjms_service.py` -- FjmsService.get_catalog_detail() pattern (line 2360)
- `web/components/catalog_dialog.py` -- web dialog pattern (show_catalog_dialog)
- `genizah_app.py:9816` -- FjmsCatalogDialog desktop pattern

### Secondary (MEDIUM confidence)
- openpyxl 3.1.5 read_only mode -- verified installed and working on this xlsx file
- AlmaId float precision issue -- verified by reading Extra_Info sample (9.900017468002052e+17)

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH - all libraries already installed and used in project
- Architecture: HIGH - follows established patterns (export script, FjmsService, dialog components)
- Pitfalls: HIGH - verified by direct data inspection (AlmaId float, shelfmark ambiguity, unit ranges)
- Data schema: MEDIUM - aggregation strategy is a recommendation; planner should validate choices

**Research date:** 2026-03-26
**Valid until:** 2026-04-26 (stable data, no external API dependencies)
