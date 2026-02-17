# Phase 37: FJMS Catalog Descriptions (v2) - Research

**Researched:** 2026-02-17
**Domain:** SQLite export enrichment + dual-app dialog UI (NiceGUI + PyQt6)
**Confidence:** HIGH

## Summary

Phase 37 v2 requires two major work streams: (1) extending the `export_fist_enrichment.py` script to extract 5 new data categories from FIST.db into the sidecar, and (2) building a rich catalog records dialog in both web and desktop apps with multi-team side-by-side layout, physical metadata, and running titles.

The v1 implementation was reverted because it only displayed TextualFrame data already visible in browse metadata. The v2 scope adds genuinely new data: multi-team scholarly identifications side-by-side, physical measurements (SizeX/SizeY), material type, condition/physical status codes, running titles, free-text descriptions, and GenizahTitle lookups. The export must add ~1.7M new rows across 4 new tables plus extend the existing catalog table with ~6 new columns.

The codebase has well-established patterns for all UI needs: bibliography dialogs in both apps, batch enrichment for search cards, button placement in ext_info_row (desktop) and bibliography buttons row (web). The reverted code provides a working template that needs enhancement rather than a fresh build.

**Primary recommendation:** Extend the export script first (new tables + catalog column additions), then rebuild the service layer methods, then rebuild the dialog component for both apps using the reverted code as a foundation with new physical metadata sections and multi-team column layout.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

#### Export Enrichment (NEW -- core change from v1)
- **Extend `export_fist_enrichment.py`** to add the following new tables:
  - `catalog_running_titles` -- from `dbo_CatalogMultiRunningTitle` (235K rows), linked by UnitCatalogRecId
  - `catalog_sizes` -- from `dbo_CatalogMultiSize` (161K rows), SizeX/SizeY per catalog record
  - `catalog_fields` -- from `dbo_CatalogMultiField` (1.1M rows), coded multi-values: physical status, material, script type, language
  - `catalog_free_desc` -- from `dbo_UnitFreeDescription` (190K rows), linked by SignatureId
- **Add to existing catalog table:** NumRow, NumColumn, NumFolio from `dbo_UnitCatalogRec` (not currently selected)
- **Add inventory-level data:** NumFolio, MaterialCode (-> CODE_FullCode lookup), SizeCode (-> CODE_FullCode lookup) from `dbo_Inventory`
- **Add GenizahTitle lookup:** OrgTitle, EngTitle from `CODE_GenizahTitle` via GenizahTitleId
- **Drop empty columns:** Remove DescriptionEng/DescriptionHeb (IdentificationText) -- always empty across all 500K records
- **Extend FTS5 index:** Include RunningTitle and FreeDescription in catalog_fts for full-text search
- All new tables stored as separate normalized tables in the sidecar (not flattened into catalog)

#### Physical Metadata Display
- **Key-value pairs** layout: label: value rows (Material: Vellum, Size: 165 x 210 mm, etc.)
- **Separate section** with its own header (e.g., "Physical Description") within the dialog
- **Hide empty fields** -- only show fields that have actual data, don't show placeholders
- **Follow app language** for labels -- Hebrew labels in Hebrew mode, English labels in English mode

#### Dialog Content Structure
- **Textual descriptions first, physical metadata below** -- lead with scholarly identification
- **Side-by-side columns** for multi-team entries -- teams as columns, fields as rows
- **Title and Running Title** shown as distinct fields -- both displayed when present
- **Free Description always fully visible** -- no expandable/collapsible, show complete scholarly notes inline
- **Dialog title:** "Catalog Records -- {shelfmark}"
- **All FJMS data shown** -- no deduplication against what browse already shows
- Button appears in **all four locations**: web browse, web search results, desktop browse (ext_info_row), desktop Result Dialog

#### Button Design & Placement (carried from v1)
- **Label:** "Catalog Records (N)" in English / "catalog_records_heb (N)" in Hebrew
- **Count N** = number of distinct sources/teams that contributed data
- **Icon:** `description` (Material doc icon)
- **Style:** `outline dense` -- matches existing Bibliography FJMS/Ktiv buttons
- **No data:** Button disabled with (0) count

#### Attribution Display (carried from v1)
- **Source header per group** -- group entries by SourceName
- **Source language** -- follow app language
- **Author placement** -- after title, before frame text

#### Partial Data Handling
- **Show physical-only records** -- catalog record with only sizes/material but no text still shows
- **No data at all** -- button disabled with (0), consistent across all locations

### Claude's Discretion
- Exact spacing, typography, and scroll behavior in the dialog
- Handling of >3 team columns (horizontal scroll vs tabs)
- Empty cell display strategy in side-by-side layout
- FTS5 tokenizer configuration for multilingual RunningTitle/FreeDescription content
- Whether to add UnitCatalogRecId as a foreign key in the sidecar tables or use AlmaId-only joins

### Deferred Ideas (OUT OF SCOPE)
- **Clickable [$reference$] links** -- clicking a `[$Sifra$]` or `[$Talmud Yerushalmi$]` reference to trigger a search -- future phase
- **FJMS FTS5 search of descriptions** -- already tracked as FJMS-04 in requirements (partially addressed by FTS5 extension in this phase)
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|-----------------|
| FJMS-01 | FJMS catalog descriptions (65K records) exported to `fjms_enrichment.db` | Export script extension adds 4 new tables + extends catalog table. Total ~1.7M new rows across running titles (235K), sizes (161K), fields (1.1M), free descriptions (190K). Catalog table gets UnitCatalogRecId, NumRow, NumColumn, NumFolio, GenizahTitle OrgTitle/EngTitle. DescriptionEng/DescriptionHeb dropped (confirmed 0 non-empty rows). |
| FJMS-02 | User can view FJMS scholarly descriptions from browse page via dedicated button in both apps | Reverted v1 code provides foundation for dialog component. New dialog adds physical metadata section and multi-team side-by-side layout. Button pattern established in web browse (bibliography buttons row at line ~2167) and desktop browse (ext_info_row at line ~8540). Search cards at line ~2238. Desktop ResultDialog at line ~4323. |
| FJMS-03 | Descriptions show source attribution (which catalog/scholar) | catalog table already has SourceName/SourceNameHeb columns. New data tables will carry UnitCatalogRecId linking back to catalog source. Side-by-side team layout groups all data by source. |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| sqlite3 (stdlib) | Python 3.10+ | Sidecar read/write | Already used for all sidecar operations |
| NiceGUI | Current | Web dialog component | Project web framework |
| PyQt6 | Current | Desktop dialog component | Project desktop framework |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| tqdm | Current | Export progress bars | Already used in export script |

### Alternatives Considered
None -- all technology choices are already established in the codebase.

## Architecture Patterns

### Recommended Project Structure
```
scripts/
  export_fist_enrichment.py  # Extended with 4 new export functions
shared/
  fjms_service.py            # New methods for catalog detail queries
web/
  components/
    catalog_dialog.py        # Recreated web dialog (was reverted)
  pages/
    browse.py                # Add catalog records button
    search.py                # Add catalog records button to cards
genizah_app.py               # Desktop dialog class + button wiring
genizah_translations.py      # New translation keys
tests/
  test_fjms_service.py       # Extended with new method tests
```

### Pattern 1: Export Script Extension
**What:** Add new `export_*()` functions following the identical batch-insert pattern used by existing exports.
**When to use:** For each new table (running_titles, sizes, fields, free_desc).
**Key detail:** The join chain is: `dbo_InventoryAlma -> dbo_Inventory -> dbo_InventorySignature -> dbo_Signature -> dbo_UnitCatalogRec -> [child table]`. This MUST be preserved for AlmaId resolution.
**Example (from existing code):**
```python
# Source: scripts/export_fist_enrichment.py, export_catalog() pattern
def export_catalog_running_titles(source, target):
    target.execute("DROP TABLE IF EXISTS catalog_running_titles")
    target.execute("""
        CREATE TABLE catalog_running_titles (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            RunningTitle TEXT,
            Comment TEXT
        )
    """)
    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.UnitCatalogRecId,
            rt.RunningTitle,
            rt.Comment
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        JOIN dbo_CatalogMultiRunningTitle rt ON cat.UnitCatalogRecId = rt.UnitCatalogRecId
    """)
    # ... batch insert pattern ...
```

### Pattern 2: Service Layer Query (Existing Pattern)
**What:** Add methods to FjmsService that query by AlmaId and return structured dicts.
**When to use:** For each new data type the dialog needs.
**Example (from existing get_catalog_records):**
```python
def get_catalog_detail(self, sys_id: str) -> dict:
    """Get complete catalog detail including all child tables."""
    # 1. Get catalog records (existing)
    # 2. Get running titles by AlmaId
    # 3. Get sizes by AlmaId
    # 4. Get fields by AlmaId (with category grouping)
    # 5. Get free descriptions by AlmaId
    # Return structured dict grouped by UnitCatalogRecId -> source team
```

### Pattern 3: Dialog Component (Web)
**What:** NiceGUI dialog with scrollable content, header with icon and close button.
**When to use:** For the catalog records dialog.
**Source:** Reverted `web/components/catalog_dialog.py` (in git history at commit `bbb7b6de`) provides the template. The new version adds physical metadata section and multi-team column layout.
**Key patterns from existing bibliography_dialog.py:**
```python
dialog = ui.dialog().props('maximized=false full-width')
with dialog, ui.card().classes('w-full max-w-[900px] max-h-[90vh]').style(
    'overflow: hidden; display: flex; flex-direction: column;'
):
    # Header with gradient
    with ui.row().classes('w-full items-center justify-between p-3 rounded-t').style(
        'background: linear-gradient(135deg, #6c3483, #9b59b6); color: white;'
    ):
        # ... icon, title, close button
    # Scrollable content
    with ui.scroll_area().classes('w-full').style('flex: 1;'):
        # ... content
```

### Pattern 4: Dialog Component (Desktop)
**What:** QDialog with QTextBrowser for HTML content, matching existing FjmsBibliographyDialog pattern.
**Source:** Reverted `FjmsCatalogDialog` class (in git history at commit `73320310`).
```python
class FjmsCatalogDialog(QDialog):
    def __init__(self, data, sys_id='', shelfmark='', parent=None):
        super().__init__(parent)
        self.setMinimumSize(700, 500)
        self.resize(800, 600)
        layout = QVBoxLayout(self)
        # Header, QTextBrowser with HTML, bottom buttons
        self.text_browser = QTextBrowser()
        self.text_browser.setHtml(self._build_html(data))
```

### Pattern 5: Button Placement (Four Locations)
**What:** Catalog records button appears in all four surfaces.

**Web Browse** (browse.py ~line 2167): In the bibliography buttons `ui.row()` after Bibliography FJMS/Ktiv:
```python
ui.button(f'{tr("Catalog Records")} ({count})', icon='description',
          on_click=dlg.open).props('outline dense').classes('text-sm')
```

**Web Search** (search.py ~line 2238): In the result card metadata area, similar to domain badges:
```python
ui.button(f'{tr("Catalog Records")} ({count})', icon='description',
          on_click=handler).props('outline dense size=sm no-caps').classes('text-xs')
```

**Desktop Browse** (genizah_app.py ~line 8554): In ext_info_row after bibliography buttons:
```python
self.btn_b_catalog_records = QPushButton(f"{tr('Catalog Records')} (0)")
self.btn_b_catalog_records.setEnabled(False)
self.btn_b_catalog_records.clicked.connect(self._show_fjms_catalog_dialog)
ext_info_row.addWidget(self.btn_b_catalog_records)
```

**Desktop ResultDialog** (genizah_app.py ~line 4323): In action_row after bibliography buttons:
```python
self.btn_rd_catalog = QPushButton(f"{tr('Catalog Records')} (0)")
self.btn_rd_catalog.setEnabled(False)
self.btn_rd_catalog.clicked.connect(self._show_rd_catalog)
```

### Anti-Patterns to Avoid
- **Flattening child tables into catalog** -- The user explicitly wants normalized tables. Don't try to denormalize running titles, sizes, and fields into the catalog table.
- **Deduplicating against browse metadata** -- The user explicitly said "All FJMS data shown" with no dedup. The side-by-side multi-team view IS the unique value.
- **Showing only TextualFrame data** -- This is what caused the v1 revert. The v2 dialog must show physical metadata, running titles, free descriptions, and sizes as genuinely new content.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Batch SQL inserts | Custom loop | Existing `BATCH_SIZE` + `executemany` pattern | Already proven in export script |
| Dialog layout | Custom HTML-in-string | NiceGUI component composition / QTextBrowser HTML | Existing patterns handle RTL, theming, responsiveness |
| Code lookups | Runtime CODE_FullCode joins | Pre-resolved EngDesc/HebDesc in export | One-time JOIN at export eliminates runtime lookups |

**Key insight:** The export script should resolve ALL code lookups (CODE_FullCode, CODE_GenizahTitle, CODE_FCDTable) at export time, storing human-readable text in the sidecar. This avoids needing reference tables at runtime.

## Common Pitfalls

### Pitfall 1: Missing UnitCatalogRecId in Existing Catalog Table
**What goes wrong:** The existing `catalog` table has no UnitCatalogRecId. Child tables (running_titles, sizes, fields) are keyed by UnitCatalogRecId. Without it in the catalog table, you can't group child data by team/source.
**Why it happens:** The v1 export didn't need it because it only used catalog-level fields.
**How to avoid:** Add UnitCatalogRecId to the catalog table schema AND store AlmaId in all child tables for direct lookup. Index both columns.
**Warning signs:** If you can't display per-team physical metadata alongside per-team textual descriptions.

### Pitfall 2: catalog_fields FCDTableId Category Mapping
**What goes wrong:** `dbo_CatalogMultiField` uses `ValueCode` referencing `CODE_FullCode.ComputedCode`, and the category (Material vs Physical Status vs Language etc.) is determined by `FCDTableId` in CODE_FullCode, NOT by a column in CatalogMultiField itself.
**Why it happens:** The FIST schema uses a single CatalogMultiField table for ALL coded multi-values across many categories.
**How to avoid:** In the export, JOIN CODE_FullCode to get EngDesc + HebDesc, and JOIN CODE_FCDTable to get the TableName (category). Store the category name in the sidecar so the service layer can filter by field type.
**Warning signs:** All fields appear as a flat list with no way to distinguish material from physical status.

**Confirmed FCDTableId -> Category mappings in CatalogMultiField data:**
| FCDTableId | TableName | Sample Values | Data Volume |
|------------|-----------|---------------|-------------|
| 11 | FragmentMaterial | Paper, Vellum, Printed, Leather | 194K rows |
| 18 | FragmentStatus | Torn, Rubbed, Mutilated, Stained, Holes, Missing, Faded, etc. | 317K rows |
| 10 | GenizahLanguages | Arabic, Hebrew, Aramaic | 265K rows |
| 26 | TypeOfScript | Hebrew, Arabic, Other | 154K rows |
| 27 | TypeOfScriptPlace | Oriental, Spanish, Ashkenazi, Syrian, Yemenite | 50K rows |
| 28 | TypeOfScriptStyle | Semi-Cursive, Square, Naskhi, Cursive, Rabbinical | 60K rows |
| 29 | TypeOfVocalization | Tiberian, Babylonian, Palestinian, Arabic, Diacritic | 54K rows |

### Pitfall 3: FreeDescription Joins Through SignatureId, Not UnitCatalogRecId
**What goes wrong:** `dbo_UnitFreeDescription` links via `SignatureId`, not `UnitCatalogRecId`. This is a different join path than the other child tables.
**Why it happens:** Free descriptions are per-signature (per-team entry point), while running titles/sizes/fields are per-catalog-record.
**How to avoid:** The export query for catalog_free_desc must join through SignatureId. In the sidecar, store both AlmaId and SignatureId (or map to UnitCatalogRecId through catalog).
**Warning signs:** Free descriptions don't appear or appear duplicated.

### Pitfall 4: Sidecar File Size Growth
**What goes wrong:** Adding ~1.7M rows (especially 1.1M catalog_fields with text) could significantly increase the 245MB sidecar.
**Why it happens:** The catalog_fields table alone has 1.1M rows with text descriptions.
**How to avoid:** Monitor file size after export. Consider whether all FCDTableId categories are needed (the user wants physical status, material, script type, language -- all are needed). VACUUM after export.
**Warning signs:** Sidecar file size doubles or more.

### Pitfall 5: DescriptionEng/DescriptionHeb Removal Breaking Existing Code
**What goes wrong:** The existing catalog table has DescriptionEng/DescriptionHeb columns. Removing them changes the schema, breaking existing service code that references them.
**Why it happens:** The `get_catalog()` method returns `description_eng` and `description_heb` keys from these columns.
**How to avoid:** Update `get_catalog()` and `get_catalog_records()` in fjms_service.py to no longer reference these columns. Also update the FTS5 index definition which currently indexes them. The test fixtures in test_fjms_service.py also reference the old schema.
**Warning signs:** `sqlite3.OperationalError: no such column: DescriptionEng`.

### Pitfall 6: GenizahTitle Text vs GenizahTitleId
**What goes wrong:** `dbo_UnitCatalogRec` has both `GenizahTitleId` (FK to CODE_GenizahTitle) and `GenizahTitleText` (inline text). The GenizahTitleText is already exported as `TitleHeb` in the catalog table. The GenizahTitle lookup adds `OrgTitle` and `EngTitle` which are different values.
**Why it happens:** FIST stores the original title name (OrgTitle, typically Hebrew) and an English translation (EngTitle) in CODE_GenizahTitle, while GenizahTitleText is the team's specific textual entry.
**How to avoid:** Add OrgTitle and EngTitle as new columns to the catalog table (alongside existing TitleHeb which comes from GenizahTitleText). Don't replace TitleHeb -- add alongside.
**Warning signs:** Title data appears duplicated or overwritten.

### Pitfall 7: Count N for Button Label -- "Distinct Sources/Teams"
**What goes wrong:** The button label shows "(N)" where N should be distinct sources/teams, not total records. But the v1 code counted total records.
**Why it happens:** The user specifically wants N = distinct SourceNames that contributed data.
**How to avoid:** The batch count query should count `COUNT(DISTINCT SourceName)` not `COUNT(*)`. The existing `get_source_names()` method filters out generic names ('Catalogs', 'Institution', 'Collection', 'Other') -- the count should also filter these.
**Warning signs:** Button shows large numbers like "(15)" for a manuscript with only 2 scholarly teams.

## Code Examples

### Export: catalog_fields with Category Resolution
```python
def export_catalog_fields(source, target):
    """Export coded multi-value fields (material, status, language, script)."""
    target.execute("DROP TABLE IF EXISTS catalog_fields")
    target.execute("""
        CREATE TABLE catalog_fields (
            AlmaId TEXT NOT NULL,
            UnitCatalogRecId INTEGER NOT NULL,
            FieldCategory TEXT NOT NULL,
            FieldValue TEXT,
            FieldValueHeb TEXT
        )
    """)
    cursor = source.execute("""
        SELECT DISTINCT
            TRIM(CAST(alma.AlmaId AS TEXT)) as AlmaId,
            cat.UnitCatalogRecId,
            fct.TableName as FieldCategory,
            fc.EngDesc as FieldValue,
            fc.HebDesc as FieldValueHeb
        FROM dbo_InventoryAlma alma
        JOIN dbo_Inventory inv ON alma.InventoryId = inv.InventoryId
        JOIN dbo_InventorySignature isig ON inv.InventoryId = isig.InventoryId
        JOIN dbo_Signature sig ON isig.SetSignatureId = sig.SetSignatureId
        JOIN dbo_UnitCatalogRec cat ON sig.SignatureId = cat.SignatureId
        JOIN dbo_CatalogMultiField fld ON cat.UnitCatalogRecId = fld.UnitCatalogRecId
        JOIN CODE_FullCode fc ON fld.ValueCode = fc.ComputedCode
        JOIN CODE_FCDTable fct ON fc.FCDTableId = fct.FCDTableId
    """)
    # ... batch insert ...
```

### Export: Extending Catalog Table with New Columns
```python
# Modified catalog schema (v2)
target.execute("""
    CREATE TABLE catalog (
        AlmaId TEXT NOT NULL,
        UnitCatalogRecId INTEGER NOT NULL,
        Title TEXT,
        TitleHeb TEXT,
        AuthorText TEXT,
        CopyDate TEXT,
        CopyPlace TEXT,
        TextualFrameHeb TEXT,
        TextualFrameEng TEXT,
        SourceName TEXT,
        SourceNameHeb TEXT,
        NumFolio REAL,
        NumColumn TEXT,
        NumRow TEXT,
        GenizahTitleOrgTitle TEXT,
        GenizahTitleEngTitle TEXT
    )
""")
# Note: DescriptionEng/DescriptionHeb removed (always empty)
# GenizahTitle resolved via LEFT JOIN CODE_GenizahTitle
```

### Service: Batch Catalog Detail Count for Search Cards
```python
def get_catalog_source_counts(self, sys_ids: list[str]) -> dict[str, int]:
    """Get count of distinct scholarly sources per AlmaId for button labels.

    Excludes generic source names (Catalogs, Institution, Collection, Other).
    Returns dict mapping sys_id -> count of distinct non-generic SourceNames.
    """
    if not self._conn or not sys_ids:
        return {}
    result = {}
    batch_size = 500
    for i in range(0, len(sys_ids), batch_size):
        batch = sys_ids[i:i + batch_size]
        placeholders = ','.join('?' * len(batch))
        cursor = self._conn.execute(
            f"SELECT AlmaId, COUNT(DISTINCT SourceName) as cnt FROM catalog "
            f"WHERE AlmaId IN ({placeholders}) "
            f"AND SourceName IS NOT NULL AND SourceName != '' "
            f"AND SourceName NOT IN ('Catalogs','Institution','Collection','Other') "
            f"GROUP BY AlmaId",
            batch,
        )
        for row in cursor:
            result[row["AlmaId"]] = row["cnt"]
    return result
```

### Service: Get Full Catalog Detail for Dialog
```python
def get_catalog_detail(self, sys_id: str) -> dict:
    """Get complete catalog detail for dialog display.

    Returns dict with:
      - records: list of catalog records with all fields including new columns
      - running_titles: dict mapping UnitCatalogRecId -> list of running titles
      - sizes: dict mapping UnitCatalogRecId -> list of {size_x, size_y, inner_x, inner_y}
      - fields: dict mapping UnitCatalogRecId -> dict of {FieldCategory -> list of values}
      - free_descriptions: list of {text, signature_id}
    """
```

### Dialog: Multi-Team Side-by-Side Layout (Web)
```python
# Group records by SourceName, then display as columns
# NiceGUI grid layout for side-by-side teams
with ui.grid(columns=len(team_groups)).classes('w-full gap-4'):
    for source_name, team_records in team_groups:
        with ui.column().classes('w-full'):
            # Team header
            ui.label(source_name).classes('font-bold text-sm')
            # Team's running titles, sizes, fields, descriptions
```

## Data Model (Sidecar Schema v3.0.0)

### Existing Tables (Modified)
```sql
-- catalog: ADD UnitCatalogRecId, NumFolio, NumColumn, NumRow,
--          GenizahTitleOrgTitle, GenizahTitleEngTitle
--          REMOVE DescriptionEng, DescriptionHeb
-- catalog_fts: REBUILD with RunningTitle + FreeDescription content
```

### New Tables
```sql
CREATE TABLE catalog_running_titles (
    AlmaId TEXT NOT NULL,
    UnitCatalogRecId INTEGER NOT NULL,
    RunningTitle TEXT,
    Comment TEXT
);
-- Indexes: idx_catrt_alma(AlmaId), idx_catrt_ucrid(UnitCatalogRecId)

CREATE TABLE catalog_sizes (
    AlmaId TEXT NOT NULL,
    UnitCatalogRecId INTEGER NOT NULL,
    SizeX REAL,
    SizeY REAL,
    InnerSizeX REAL,
    InnerSizeY REAL
);
-- Index: idx_catsz_alma(AlmaId)

CREATE TABLE catalog_fields (
    AlmaId TEXT NOT NULL,
    UnitCatalogRecId INTEGER NOT NULL,
    FieldCategory TEXT NOT NULL,
    FieldValue TEXT,
    FieldValueHeb TEXT
);
-- Indexes: idx_catfld_alma(AlmaId), idx_catfld_cat(FieldCategory)

CREATE TABLE catalog_free_desc (
    AlmaId TEXT NOT NULL,
    SignatureId INTEGER NOT NULL,
    FreeDesc TEXT
);
-- Index: idx_catfd_alma(AlmaId)
```

### Data Volumes
| Table | Estimated Rows | Key Fields |
|-------|---------------|------------|
| catalog_running_titles | ~235K | AlmaId, UnitCatalogRecId, RunningTitle |
| catalog_sizes | ~161K | AlmaId, UnitCatalogRecId, SizeX, SizeY |
| catalog_fields | ~1.1M | AlmaId, UnitCatalogRecId, FieldCategory, FieldValue |
| catalog_free_desc | ~190K | AlmaId, SignatureId, FreeDesc |
| catalog (extended) | ~500K | +UnitCatalogRecId, +NumFolio, +NumColumn, +NumRow, +GenizahTitleOrgTitle, +GenizahTitleEngTitle, -DescriptionEng, -DescriptionHeb |

### FTS5 Extension
```sql
-- Rebuild catalog_fts to include running titles and free descriptions
CREATE VIRTUAL TABLE catalog_fts USING fts5(
    AlmaId,
    Title,
    TitleHeb,
    TextualFrameHeb,
    TextualFrameEng,
    RunningTitle,        -- NEW: from catalog_running_titles (aggregated per AlmaId)
    FreeDescription,     -- NEW: from catalog_free_desc (aggregated per AlmaId)
    content='',          -- contentless (not synced to a single table)
    content_rowid='rowid'
);
```
Note: Since FTS5 content now spans multiple tables, use a contentless FTS5 table populated by a custom INSERT that aggregates running titles and free descriptions per AlmaId. This is a departure from the current content-synced approach.

## FIST.db Source Schema Reference

### Key Join Chain (AlmaId -> UnitCatalogRecId)
```
dbo_InventoryAlma(AlmaId, InventoryId)
  -> dbo_Inventory(InventoryId)
    -> dbo_InventorySignature(InventoryId, SetSignatureId)
      -> dbo_Signature(SetSignatureId, SignatureId, SourceId)
        -> dbo_UnitCatalogRec(SignatureId, UnitCatalogRecId, GenizahTitleId, ...)
          -> dbo_CatalogMultiRunningTitle(UnitCatalogRecId)
          -> dbo_CatalogMultiSize(UnitCatalogRecId)
          -> dbo_CatalogMultiField(UnitCatalogRecId, ValueCode)
  -> dbo_UnitFreeDescription(SignatureId)  // Note: via Signature, not CatalogRec
```

### Source Team Resolution
```
dbo_Signature(SourceId) -> dbo_CodeSource(TeamCode) -> EngDesc/HebDesc
```
This is already used in the existing catalog export. Each Signature belongs to one source team.

### CODE_FullCode Category Resolution
```
dbo_CatalogMultiField(ValueCode)
  -> CODE_FullCode(ComputedCode) -> EngDesc, HebDesc, FCDTableId
    -> CODE_FCDTable(FCDTableId) -> TableName (category)
```

### CODE_GenizahTitle Resolution
```
dbo_UnitCatalogRec(GenizahTitleId)
  -> CODE_GenizahTitle(GenizahTitleID) -> OrgTitle, EngTitle
```

## Verified Sample Data (T-S C1.15 = AlmaId 990051150460205171)

This manuscript has data from multiple teams, demonstrating the side-by-side use case:

**Teams present:** Inventory, Midrash Eikha Rabba, Aggadic Midrashim, Catalogs, Institution
**Catalog records:** 8 UnitCatalogRecIds
**Running Titles:** 6 entries, e.g.:
  - "Midrash Lamentations Rabbati: Petihah 2 - 7 (Buber ed., 320-62) N.B. The petihot do not appear in the same order..." (CatRecId=19311, Mandel team)
  - Hebrew running titles from Aggadic Midrashim team
**Sizes:** 165 x 210 mm (from CatRecId 19311 and 4968814)
**Fields:** Material=Vellum (from 4 records), Status=Missing (1), Status=Injured (1)
**FreeDesc:** 6 entries including physical description ("Parchment, Fragment: Left and right margins visible...") and scholarly notes
**GenizahTitle:** OrgTitle="Midrash Rabbah Lamentation" (ID 7219), "Midrash Rabbah Lamentation (Buber)" (ID 7220)
**Inventory:** NumFolio=1.0 (no MaterialCode/SizeCode for this record)

## Translation Keys Needed

New keys for `genizah_translations.py`:
```python
"Catalog Records": "מידע קטלוגי",
"Running Title": "כותרת רצה",
"Free Description": "תיאור חופשי",
"Physical Description": "תיאור פיזי",  # Already exists
"Material": "חומר",                      # Already exists
"Size": "גודל",
"Script Type": "סוג כתב",
"Script Style": "סגנון כתב",
"Vocalization": "ניקוד",
"Physical Status": "מצב פיזי",
"Number of Folios": "מספר דפים",
"Number of Columns": "מספר טורים",
"Number of Lines": "מספר שורות",
"Open in KTIV": "פתח בכתיב",  # May already exist
"Unknown": "לא ידוע",          # May already exist
```

## Open Questions

1. **FTS5 Contentless vs Content-Synced**
   - What we know: Current FTS5 uses `content='catalog'` (synced to catalog table). New FTS5 needs data from multiple tables (catalog + running_titles + free_desc).
   - What's unclear: Whether to use contentless FTS5 or create a denormalized FTS source table.
   - Recommendation: Use contentless FTS5 with manual INSERT. Simpler than maintaining a denormalized view. The FTS5 is only for search, not for retrieval -- actual data comes from individual tables.

2. **Inventory-Level Data Overlap with Catalog-Level Data**
   - What we know: `dbo_Inventory` has NumFolio, MaterialCode, SizeCode at the inventory (shelfmark) level. `dbo_UnitCatalogRec` and child tables have NumFolio, Material (via CatalogMultiField), Size (via CatalogMultiSize) at the catalog record (team) level.
   - What's unclear: Whether to show both levels or prefer one.
   - Recommendation: Export both. Inventory-level data goes as new columns in catalog or a separate lookup. Catalog-level data is per-team in child tables. The dialog can show inventory-level as "overall" and catalog-level as per-team, but this is Claude's discretion for display.

3. **Sidecar Size After Export**
   - What we know: Current size is 245 MB. Adding ~1.7M rows with text content.
   - What's unclear: Final size after VACUUM. Rough estimate: +50-100 MB (running_titles ~10MB, sizes ~5MB, fields ~30MB, free_desc ~25MB, catalog extension ~15MB).
   - Recommendation: Monitor after export. If >400MB, consider whether all FCDTableId categories are needed. All listed categories ARE needed per user decisions.

## Sources

### Primary (HIGH confidence)
- **FIST.db direct inspection** -- All schemas, row counts, sample data, join chains verified by querying the actual database
- **Codebase inspection** -- export_fist_enrichment.py, shared/fjms_service.py, web/pages/browse.py, web/pages/search.py, genizah_app.py
- **Git history** -- Reverted commit `73320310` provides complete v1 code diff

### Secondary (MEDIUM confidence)
- **Data volume estimates** -- Based on actual COUNT queries, but export with JOINs may produce slightly different row counts due to DISTINCT

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- all technology already in use, no new dependencies
- Architecture: HIGH -- following exact established patterns (export, service, dialog)
- Data model: HIGH -- verified against actual FIST.db schemas and sample data
- Pitfalls: HIGH -- identified from actual v1 revert experience and schema analysis

**Research date:** 2026-02-17
**Valid until:** 2026-03-17 (stable -- internal project, no external dependency changes)
