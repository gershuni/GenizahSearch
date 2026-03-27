# Phase 54: Dimension & Measurement Filtering - Research

**Researched:** 2026-03-27
**Domain:** Pre-search and post-search measurement filtering in NiceGUI web + PyQt6 desktop
**Confidence:** HIGH

## Summary

Plan 54-03 adds 6 measurement filters (width, height, line count, line height, text density, material) to the existing pre-search and post-search filter infrastructure in both web and desktop apps. The `manuscript_measurements` summary table (231K rows) already exists with most needed columns; only `avg_line_height_mm` needs to be added via ALTER TABLE + backfill from `computed_measurements`.

The pre-search path is well-defined: `get_filter_sys_ids()` in `shared/fjms_service.py` already handles domain/author/work/date/material intersection filtering and returns `set[str]` of AlmaIds. Measurement params add 10 new conditions (5 min/max pairs) against `manuscript_measurements` using subquery-based intersection. The post-search path differs between web (client-side text filtering with Apply button on `results_list` dicts) and desktop (row-hiding on QTableWidget with column-based filter rules). Post-search measurement filtering requires a batch lookup of measurement data for result sys_ids, then client-side range comparison.

**Primary recommendation:** Add `avg_line_height_mm` column to `manuscript_measurements` via the import script (ALTER TABLE + UPDATE from computed_measurements AVG), then extend `get_filter_sys_ids()` with measurement range params, build a shared `get_measurement_filter_data()` batch method for post-search, add collapsible "Measurements" section to both web and desktop filter UIs.

## Project Constraints (from CLAUDE.md)

- Python 3.10+, NiceGUI for web, PyQt6 for desktop
- Both apps must be maintained; shared service layer in `shared/`
- Hebrew RTL support; bilingual translations required for all new UI labels
- Data stored in SQLite sidecars (fjms_enrichment.db)
- AlmaId is the manuscript join key (FIST AlmaId is integer, CSV system_number is string)
- Tests via pytest; existing `tests/test_measurements.py` covers import/display
- Session persistence via `app.storage.user` (web) and QSettings (desktop)
- Run `python scripts/check_docs.py` before finishing

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-15:** Six measurement fields are filterable: width (cm), height (cm), line count, line height (mm), text density (per 10cm^2), material (paper/parchment/etc)
- **D-16:** Line height (`avg_line_height_mm`) needs to be added to `manuscript_measurements` -- aggregate from `computed_measurements.Avg_Line_Height_Text_mm` (AVG of unflagged rows per AlmaId). Store at full precision, display/input at 0.1mm.
- **D-17:** Line height and text density are key join-matching signals (fragments share scribal characteristics)
- **D-18:** Min/max number input pairs per field. Leave blank = no constraint. Compact academic-search style.
- **D-19:** Validate ranges: normalize or reject min > max. Use >= min / <= max consistently (inclusive boundaries).
- **D-20:** In existing filter panel alongside domain/author/work/date/material. Collapsible "Measurements" section, collapsed by default. Active filter chips visible outside the collapsed section.
- **D-21:** Add dimension range params to `get_filter_sys_ids()` -> returns restrict_sys_ids. Same mechanism as existing filters.
- **D-22:** Uses `manuscript_measurements` summary table. Known limitation: unpaired aggregation accepted.
- **D-23:** Material filtering for measurements should use `manuscript_measurements.material` (from extra_info), NOT `catalog_fields.FragmentMaterial`.
- **D-24:** Same filter inputs in post-search. Identical UX to pre-search.
- **D-25:** Apply button (not live debounce) for post-search dimension filters. Enter key also triggers apply.
- **D-26:** Manuscripts with no measurement data are excluded when any measurement filter is active.
- **D-14:** Web (NiceGUI) and desktop (PyQt6) both get filtering.

### Claude's Discretion
- Desktop pre-search filter dialog layout for measurement fields
- Exact filter chip display format
- How to handle material dropdown vs checkbox UX (measurement-specific material filter separate from existing printed/material filter)

### Deferred Ideas (OUT OF SCOPE)
- "Include manuscripts with unknown measurements" toggle
- "N results would match" live preview for measurement ranges
- PuzzleRatio/CentroidDescriptor for join suggestions
- Image-derived DPI for puzzle canvas calibration
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| DIM-02 | User can filter search by dimension range as pre-search filter (web + desktop) | `get_filter_sys_ids()` extension with measurement params; `manuscript_measurements` table with existing indexes + new `avg_line_height_mm` column |
| DIM-03 | User can filter within results by dimension range as post-search filter (web + desktop) | Batch `get_measurement_summary_batch()` method for post-search lookup; Apply button pattern matches existing post-search filter UX |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| NiceGUI | (project ver) | Web filter UI -- collapsible section with min/max inputs | Project standard for web UI |
| PyQt6 | (project ver) | Desktop filter UI -- QGroupBox with QDoubleSpinBox pairs | Project standard for desktop UI |
| sqlite3 | stdlib | Query manuscript_measurements for filter matching | Existing pattern in fjms_service.py |

### Supporting
No new dependencies. Everything needed is already in the project.

## Architecture Patterns

### Pre-Search Filter Flow (Existing Pattern)

```
User sets filters -> get_filter_sys_ids(**kwargs) -> set[str] of AlmaIds
                     |                                    |
                     v                                    v
              SELECT DISTINCT c.AlmaId          restrict_sys_ids passed
              FROM catalog c                    to execute_search()
              WHERE [conditions]
```

**Key insight:** `get_filter_sys_ids` uses `catalog` as base table with subquery conditions. Measurement filters will add `c.AlmaId IN (SELECT AlmaId FROM manuscript_measurements WHERE ...)` conditions. This naturally handles NULL exclusion (D-26) because manuscripts without rows in `manuscript_measurements` will not be in the subquery results.

### Measurement Filter SQL Pattern

```sql
-- Width filter (uses COALESCE to check catalog OR computed)
c.AlmaId IN (
    SELECT AlmaId FROM manuscript_measurements
    WHERE COALESCE(catalog_width_cm, max_computed_width_cm) >= ?
      AND COALESCE(catalog_width_cm, max_computed_width_cm) <= ?
)
```

**Width/Height strategy:** Use `COALESCE(catalog_width_cm, max_computed_width_cm)` to prefer catalog-reported dimensions (most authoritative) but fall back to image-derived computed dimensions. This maximizes coverage: 104K manuscripts have catalog data, ~157K have computed data, combined ~180K have at least one.

**Line count strategy:** Use `avg_num_lines` for range filtering (AVG represents typical page). For min/max: `avg_num_lines >= ? AND avg_num_lines <= ?`.

**Line height strategy:** New `avg_line_height_mm` column. Same BETWEEN pattern.

**Text density strategy:** `avg_text_density` column exists. Same BETWEEN pattern.

**Material strategy (D-23):** Use `manuscript_measurements.material` (from extra_info), NOT the existing `catalog_fields.FragmentMaterial` used by the printed filter. This is a separate filter param, not reusing the existing material_include/material_exclude.

### Post-Search Filter Flow

**Web (current pattern):** Post-search filters are text-based client-side filtering on result dicts. The `apply_filters()` function iterates `search_state.results` and checks each result against filter values.

**For measurement post-search:** Need to batch-fetch measurement summaries for all result sys_ids, then filter client-side.

```python
# New method in FjmsService
def get_measurement_summaries_batch(self, sys_ids: list[str]) -> dict[str, dict]:
    """Batch fetch measurement summaries for post-search filtering.
    Returns {AlmaId: {width, height, num_lines, line_height, text_density, material}}
    """
    # Use IN clause with batched sys_ids (same pattern as batch enrichment)
```

**Desktop:** Desktop post-search uses `_apply_results_table_filters()` which hides rows. Measurement data needs to be cached per-result at enrichment time (already happens for domain/printed lookups). Store measurement summary as part of result metadata.

### UI Placement

**Web pre-search:** The advanced filter panel (built in `web/pages/search.py`) contains domain/author/work/date/material selects. Add a collapsible "Measurements" section BELOW the existing filters.

**Web post-search:** The "Filter Results" panel (line ~1235) has shelfmark/title/text inputs + Apply/Clear buttons. Add measurement inputs as a collapsible section within this panel.

**Desktop pre-search:** `PreSearchFilterDialog` (line ~9083) has domain tree (left) + author/work/date/material (right). Add measurement fields as a new QGroupBox in the right column, below material.

**Desktop post-search:** Desktop uses column header click filters + printed toggle. Measurement post-search filtering should be added as a toolbar section in the results area.

### NiceGUI Input Pattern for Min/Max

```python
# Compact min/max pair
with ui.row().classes('gap-1 items-center'):
    ui.label('Width (cm)').classes('text-xs w-20')
    min_input = ui.number(placeholder='Min', format='%.1f').props(
        'outlined dense type=number step=0.1'
    ).classes('w-20')
    ui.label('-').classes('text-xs')
    max_input = ui.number(placeholder='Max', format='%.1f').props(
        'outlined dense type=number step=0.1'
    ).classes('w-20')
```

### PyQt6 Input Pattern for Min/Max

```python
# QDoubleSpinBox pair in a QGroupBox
width_group = QGroupBox(tr("Width (cm)"))
layout = QHBoxLayout(width_group)
width_min = QDoubleSpinBox()
width_min.setRange(0, 100)
width_min.setDecimals(1)
width_min.setSpecialValueText(tr("Any"))  # Shows "Any" when 0
width_min.setValue(0)
width_min.setPrefix(tr("Min") + ": ")
layout.addWidget(width_min)
width_max = QDoubleSpinBox()
width_max.setRange(0, 100)
width_max.setDecimals(1)
width_max.setSpecialValueText(tr("Any"))
width_max.setValue(0)
width_max.setPrefix(tr("Max") + ": ")
layout.addWidget(width_max)
```

### Filter Chip Format

Active measurement filter chips should be compact:
- Width: "W: 10-20 cm"
- Height: "H: 15-30 cm"
- Lines: "Lines: 10-25"
- Line height: "LH: 3-5 mm"
- Density: "Density: 10-30"
- Material: "Paper" / "Vellum"

### Anti-Patterns to Avoid
- **Querying `catalog_fields.FragmentMaterial` for measurement material filter:** D-23 explicitly requires `manuscript_measurements.material`. The existing printed filter uses `catalog_fields` -- measurement material is a SEPARATE filter.
- **Live debounce on measurement inputs:** D-25 says Apply button only, not live. Avoids partial-range flicker when typing "15" (would trigger at "1" then "15").
- **Building per-result measurement queries in post-search loop:** Use batch lookup once, then iterate in-memory.
- **Ignoring NULL manuscripts in pre-search:** The subquery pattern naturally excludes NULLs (D-26) because they have no rows matching in manuscript_measurements. But post-search must ALSO exclude results with no measurement data when any measurement filter is active.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Pre-search filter intersection | Custom measurement query | Extend `get_filter_sys_ids()` with subquery conditions | Existing intersection pattern handles all edge cases |
| Batch measurement lookup | Per-result queries in loop | `get_measurement_summaries_batch()` with IN clause | Same batch pattern used for domains, printed IDs |
| Filter state persistence | Custom storage | `app.storage.user` (web) / QSettings (desktop) | Existing session persistence pattern in `filter_panel.py` |
| Range validation (min>max) | Manual swap logic | Simple normalize: `if min > max: min, max = max, min` | Standard pattern, one line |

## Common Pitfalls

### Pitfall 1: Mixed Material Sources
**What goes wrong:** Measurement material filter returns different results than the existing "Exclude printed" filter because they query different tables.
**Why it happens:** Existing material filter uses `catalog_fields.FragmentMaterial` (FJMS catalog data), measurement material comes from `extra_info` (image analysis metadata). Coverage differs: only 17,804 manuscripts have `manuscript_measurements.material`, vs broader `catalog_fields` coverage.
**How to avoid:** Keep them as clearly separate filters. The measurement material filter is in the "Measurements" collapsible section. Document that measurement material comes from image analysis, not catalog records.
**Warning signs:** User selects "Paper" in measurement filter but sees manuscripts they expected to be excluded.

### Pitfall 2: COALESCE Width/Height Column Choice
**What goes wrong:** Filter uses wrong width column -- `catalog_width_cm` returns only 104K matches, `max_computed_width_cm` only 157K. Many manuscripts have one but not the other.
**Why it happens:** Width/height data comes from two independent sources (catalog reports vs image analysis).
**How to avoid:** Use `COALESCE(catalog_width_cm, max_computed_width_cm)` for filtering. This gives ~180K manuscripts with at least one width value.
**Warning signs:** Filtering by width returns far fewer results than expected.

### Pitfall 3: Post-Search Measurement Lookup Performance
**What goes wrong:** Fetching measurement data for 10K+ search results causes noticeable delay.
**Why it happens:** Large result sets from broad searches.
**How to avoid:** Use single SQL query with `WHERE AlmaId IN (...)` for the batch. SQLite handles IN clauses well up to ~50K items (use chunking above that). Consider caching measurement data per-session.
**Warning signs:** Apply button hangs for >2 seconds on large result sets.

### Pitfall 4: Text Density Outliers
**What goes wrong:** Max text density in the data is 6,096 (vs typical range 0-50). Users may set max=50 and miss legitimate manuscripts.
**Why it happens:** 177 manuscripts have avg_text_density > 100, likely measurement artifacts.
**How to avoid:** The filter should use the user's input values as-is. Don't cap or normalize. The dialog (from plan 54-02) shows actual values, so users can verify.
**Warning signs:** None -- this is a data quality issue, not a code issue.

### Pitfall 5: State Synchronization Between Pre and Post Search
**What goes wrong:** Pre-search measurement filters and post-search measurement filters could conflict or confuse users.
**Why it happens:** Pre-search restricts the candidate set before Tantivy. Post-search narrows displayed results.
**How to avoid:** Use the same input values for both. When pre-search filters are active, post-search measurement inputs should be pre-filled with the same values (or hidden). Follow existing pattern: pre-search domain filter and post-search domain filter are independent but complementary.
**Warning signs:** User sets pre-search width 10-20 cm, gets results, then sees post-search width filter is blank.

### Pitfall 6: avg_line_height_mm Column Addition
**What goes wrong:** ALTER TABLE adds column but import script on next run drops and recreates table, losing the column.
**Why it happens:** `step5_manuscript_measurements()` does `DROP TABLE IF EXISTS` then `CREATE TABLE`.
**How to avoid:** Add `avg_line_height_mm` to the CREATE TABLE statement in the import script AND run a one-time backfill for the current DB. Both paths must be covered.
**Warning signs:** Column exists after manual ALTER but disappears after next import run.

## Data Statistics (verified from DB)

| Column | Non-NULL Count | Min | Max | Typical Range |
|--------|---------------|-----|-----|---------------|
| catalog_width_cm | ~104K AlmaIds | 0.0 | 99.0 | 5-40 cm |
| catalog_height_cm | ~104K AlmaIds | 0.0 | 98.0 | 5-50 cm |
| max_computed_width_cm | ~157K AlmaIds | 0.5 | 76.0 | 5-30 cm |
| max_computed_height_cm | ~157K AlmaIds | 0.6 | 88.1 | 5-40 cm |
| avg_num_lines | ~118K AlmaIds | 1 | 113 | 5-40 lines |
| avg_text_density | ~118K AlmaIds | -1.0 | 6,096 | 5-50 per 10cm^2 |
| Avg_Line_Height_Text_mm (source) | ~157K AlmaIds | 0.0 | 19.5 | 2-6 mm |
| material | ~18K AlmaIds | -- | -- | Paper(14.6K), Vellum(3.2K), Papyrus(8), Mix(5), Wood(2) |

**Total manuscript_measurements rows:** 231,490
**With any dimension data (width or height):** ~180K
**With no useful measurement data at all:** 44,865

## Code Examples

### Extending get_filter_sys_ids() with Measurement Params

```python
# In shared/fjms_service.py, add params to get_filter_sys_ids():
def get_filter_sys_ids(
    self,
    # ... existing params ...
    # Measurement filters (D-15)
    width_min: float = None, width_max: float = None,
    height_min: float = None, height_max: float = None,
    line_count_min: int = None, line_count_max: int = None,
    line_height_min: float = None, line_height_max: float = None,
    text_density_min: float = None, text_density_max: float = None,
    measurement_material: list[str] = None,
) -> Optional[set]:
```

```python
# Measurement conditions (inside the try block, before final query):
_has_measurement_filter = any([
    width_min is not None, width_max is not None,
    height_min is not None, height_max is not None,
    line_count_min is not None, line_count_max is not None,
    line_height_min is not None, line_height_max is not None,
    text_density_min is not None, text_density_max is not None,
    measurement_material,
])

if _has_measurement_filter:
    mm_conditions = []
    mm_params = []

    if width_min is not None:
        mm_conditions.append(
            "COALESCE(m.catalog_width_cm, m.max_computed_width_cm) >= ?")
        mm_params.append(width_min)
    if width_max is not None:
        mm_conditions.append(
            "COALESCE(m.catalog_width_cm, m.max_computed_width_cm) <= ?")
        mm_params.append(width_max)
    # ... similar for height, line_count, line_height, text_density ...
    if measurement_material:
        ph = ','.join('?' * len(measurement_material))
        mm_conditions.append(f"m.material IN ({ph})")
        mm_params.extend(measurement_material)

    mm_where = " AND ".join(mm_conditions) if mm_conditions else "1=1"
    conditions.append(
        f"c.AlmaId IN (SELECT m.AlmaId FROM manuscript_measurements m WHERE {mm_where})"
    )
    params.extend(mm_params)
```

### Batch Measurement Lookup for Post-Search

```python
# New method in FjmsService
def get_measurement_summaries_batch(self, sys_ids: list[str]) -> dict[str, dict]:
    """Batch fetch measurement summaries for post-search filtering."""
    if not sys_ids or self._conn is None:
        return {}
    result = {}
    # Chunk to avoid SQLite variable limit (999)
    for i in range(0, len(sys_ids), 500):
        chunk = sys_ids[i:i+500]
        ph = ','.join('?' * len(chunk))
        cursor = self._conn.execute(f"""
            SELECT AlmaId,
                   COALESCE(catalog_width_cm, max_computed_width_cm) as width_cm,
                   COALESCE(catalog_height_cm, max_computed_height_cm) as height_cm,
                   avg_num_lines, avg_line_height_mm, avg_text_density, material
            FROM manuscript_measurements
            WHERE AlmaId IN ({ph})
        """, chunk)
        for row in cursor:
            result[row['AlmaId']] = dict(row)
    return result
```

### Adding avg_line_height_mm Column

```python
# In scripts/import_measurements.py, update step5_manuscript_measurements():
# Add to CREATE TABLE:
#     avg_line_height_mm REAL,  -- AVG of Avg_Line_Height_Text_mm (unflagged)

# Add to the comp_agg subquery:
#     AVG(Avg_Line_Height_Text_mm) as avg_line_height_mm,

# Also add a one-time migration function:
def migrate_add_line_height(conn):
    """Add avg_line_height_mm column if missing (for existing DBs)."""
    cols = {row[1] for row in conn.execute("PRAGMA table_info(manuscript_measurements)")}
    if 'avg_line_height_mm' in cols:
        return  # Already present
    conn.execute("ALTER TABLE manuscript_measurements ADD COLUMN avg_line_height_mm REAL")
    conn.execute("""
        UPDATE manuscript_measurements SET avg_line_height_mm = (
            SELECT AVG(cm.Avg_Line_Height_Text_mm)
            FROM computed_measurements cm
            WHERE cm.AlmaId = manuscript_measurements.AlmaId
              AND cm.Flag_DPI_High = 0 AND cm.Flag_DPI_Low = 0
              AND cm.Flag_Negative_Margin = 0 AND cm.Flag_BifolioLoc_Error = 0
              AND cm.Avg_Line_Height_Text_mm IS NOT NULL
              AND cm.Avg_Line_Height_Text_mm > 0
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_ms_line_height ON manuscript_measurements(avg_line_height_mm)")
    conn.commit()
```

### Web Collapsible Measurements Section

```python
# In the filter panel, using NiceGUI expansion
with ui.expansion(tr('Measurements'), icon='straighten').classes(
    'w-full'
).props('dense default-closed'):
    with ui.grid(columns=2).classes('gap-2 w-full'):
        # Width
        with ui.column().classes('gap-0'):
            ui.label(tr('Width (cm)')).classes('text-xs font-medium')
            with ui.row().classes('gap-1 items-center'):
                width_min = ui.number(placeholder='Min').props('outlined dense').classes('w-20')
                ui.label('-').classes('text-xs')
                width_max = ui.number(placeholder='Max').props('outlined dense').classes('w-20')
        # Height, Lines, Line Height, Text Density similarly...
    # Material select
    material_select = ui.select(
        options=['Paper', 'Vellum', 'Papyrus'],
        label=tr('Material'), multiple=True
    ).props('outlined dense clearable use-chips')
```

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest |
| Config file | pytest.ini |
| Quick run command | `pytest tests/test_measurements.py -x -q` |
| Full suite command | `pytest tests/ -x -q` |

### Phase Requirements -> Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| DIM-02 | Pre-search width filter returns correct sys_ids | unit | `pytest tests/test_measurements.py::test_filter_width_range -x` | Wave 0 |
| DIM-02 | Pre-search height filter returns correct sys_ids | unit | `pytest tests/test_measurements.py::test_filter_height_range -x` | Wave 0 |
| DIM-02 | Pre-search line count filter works | unit | `pytest tests/test_measurements.py::test_filter_line_count -x` | Wave 0 |
| DIM-02 | Pre-search line height filter works | unit | `pytest tests/test_measurements.py::test_filter_line_height -x` | Wave 0 |
| DIM-02 | Pre-search text density filter works | unit | `pytest tests/test_measurements.py::test_filter_text_density -x` | Wave 0 |
| DIM-02 | Pre-search measurement_material filter works | unit | `pytest tests/test_measurements.py::test_filter_measurement_material -x` | Wave 0 |
| DIM-02 | NULL exclusion when filter active (D-26) | unit | `pytest tests/test_measurements.py::test_filter_null_exclusion -x` | Wave 0 |
| DIM-02 | Combined measurement + domain filters intersect correctly | unit | `pytest tests/test_measurements.py::test_filter_combined -x` | Wave 0 |
| DIM-02 | Inclusive boundaries (>= min, <= max) | unit | `pytest tests/test_measurements.py::test_filter_boundaries_inclusive -x` | Wave 0 |
| DIM-03 | Batch measurement summary lookup | unit | `pytest tests/test_measurements.py::test_batch_measurement_summaries -x` | Wave 0 |
| DIM-02 | avg_line_height_mm column exists after migration | unit | `pytest tests/test_measurements.py::test_line_height_column -x` | Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_measurements.py -x -q`
- **Per wave merge:** `pytest tests/ -x -q`
- **Phase gate:** Full suite green before `/gsd:verify-work`

### Wave 0 Gaps
- [ ] `tests/test_measurements.py` -- extend existing file with filter tests (DIM-02 test cases above)
- [ ] Test fixture: add `manuscript_measurements` test rows with known filter values to `_create_measurement_tables()`

## Integration Points Summary

| Component | File | Line | Change |
|-----------|------|------|--------|
| Pre-search service | `shared/fjms_service.py` | ~848 | Add 11 measurement params to `get_filter_sys_ids()` |
| Pre-search service | `shared/fjms_service.py` | ~912 | Add measurement params to `has_any` check |
| Pre-search service | `shared/fjms_service.py` | ~1086 | Add measurement SQL conditions before final query |
| Post-search service | `shared/fjms_service.py` | new | Add `get_measurement_summaries_batch()` method |
| Import script | `scripts/import_measurements.py` | ~496 | Add `avg_line_height_mm` to CREATE TABLE + aggregation |
| Import script | `scripts/import_measurements.py` | new | Add `migrate_add_line_height()` for existing DBs |
| Web filter panel | `web/components/filter_panel.py` | ~187 | Add measurement fields to `has_active_filters()` |
| Web filter panel | `web/components/filter_panel.py` | ~123 | Add measurement summary to `build_filter_summary()` |
| Web filter panel | `web/components/filter_panel.py` | ~218 | Add measurement state to `load_filter_state()` |
| Web filter panel | `web/components/filter_panel.py` | ~304 | Add measurement recompute to `recompute_filter_count()` |
| Web search page | `web/pages/search.py` | ~1241 | Add measurement section to post-search filter panel |
| Web search page | `web/pages/search.py` | ~1416 | Extend `apply_filters()` with measurement range checks |
| Web search page | `web/pages/search.py` | ~2860 | Pass measurement params to `get_filter_sys_ids()` |
| Desktop filter dialog | `genizah_app.py` | ~9083 | Add measurement QGroupBox to `PreSearchFilterDialog` |
| Desktop filter dialog | `genizah_app.py` | ~9559 | Include measurement values in filter result dict |
| Desktop search | `genizah_app.py` | ~9060 | Pass measurement params from filter dict to `get_filter_sys_ids()` |
| Desktop post-search | `genizah_app.py` | ~24194 | Extend `_apply_results_table_filters()` with measurement checks |
| Translations | `web/translations.py` | various | Add Hebrew translations for all new filter labels |
| Tests | `tests/test_measurements.py` | extend | Add filter test cases |

## Open Questions

1. **Post-search measurement data availability on desktop**
   - What we know: Desktop enrichment happens async after search. Measurement data could be fetched alongside domain/printed enrichment.
   - What's unclear: Whether to add measurement data to the per-row data model or keep a separate lookup dict.
   - Recommendation: Separate lookup dict (like `_result_domain_map`). Fetch measurement summaries in the same enrichment pass.

2. **Material filter overlap with existing printed filter**
   - What we know: D-23 says use `manuscript_measurements.material`. Existing printed filter uses `catalog_fields.FragmentMaterial`. Only 18K manuscripts have measurement material vs broader catalog_fields coverage.
   - What's unclear: Whether users will be confused by two "material" filters.
   - Recommendation: Label the measurement material filter clearly as "Material (from image analysis)" and keep it inside the Measurements collapsible section to avoid confusion with the printed/manuscript toggle.

## Sources

### Primary (HIGH confidence)
- `shared/fjms_service.py` lines 848-1171 -- `get_filter_sys_ids()` full implementation verified
- `scripts/import_measurements.py` lines 486-640 -- `manuscript_measurements` CREATE TABLE and aggregation SQL verified
- `web/components/filter_panel.py` -- full filter panel logic (466 lines) verified
- `web/pages/search.py` lines 1235-1267 -- post-search filter panel UI verified
- `genizah_app.py` lines 9083-9306 -- PreSearchFilterDialog full implementation verified
- `fist_data/fjms_enrichment.db` -- direct schema and data range queries verified

### Secondary (MEDIUM confidence)
- `tests/test_measurements.py` -- existing test structure verified, filter tests need to be added

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- no new libraries, all patterns established
- Architecture: HIGH -- extending existing filter mechanism, verified all integration points
- Pitfalls: HIGH -- data ranges and edge cases verified from actual DB
- Post-search desktop: MEDIUM -- desktop post-search filter pattern is more complex (row-hiding vs result-list filtering)

**Research date:** 2026-03-27
**Valid until:** 2026-04-27 (stable data, no external dependencies)
