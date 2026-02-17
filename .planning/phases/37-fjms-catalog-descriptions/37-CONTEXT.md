# Phase 37: FJMS Catalog Descriptions - Context (v2)

**Gathered:** 2026-02-17 (updated after revert)
**Status:** Ready for re-planning

<domain>
## Phase Boundary

Surface the FULL FJMS scholarly catalog data — multi-team content identification, physical metadata, running titles, free descriptions, and sizes — via a dedicated button and dialog in both web and desktop apps. Requires extending the fjms_enrichment.db export script to pull data from 5+ additional FIST.db tables (RunningTitle, Size, Field, FreeDescription, Inventory physical fields).

**Why the revert:** The initial Phase 37 implementation only showed TextualFrame data already visible in browse metadata. The enriched version adds truly new data: multi-team scholarly identifications side-by-side, physical measurements, condition status, and detailed scholarly notes.

</domain>

<decisions>
## Implementation Decisions

### Export Enrichment (NEW — core change from v1)
- **Extend `export_fist_enrichment.py`** to add the following new tables:
  - `catalog_running_titles` — from `dbo_CatalogMultiRunningTitle` (235K rows), linked by UnitCatalogRecId
  - `catalog_sizes` — from `dbo_CatalogMultiSize` (161K rows), SizeX/SizeY per catalog record
  - `catalog_fields` — from `dbo_CatalogMultiField` (1.1M rows), coded multi-values: physical status, material, script type, language
  - `catalog_free_desc` — from `dbo_UnitFreeDescription` (190K rows), linked by SignatureId
- **Add to existing catalog table:** NumRow, NumColumn, NumFolio from `dbo_UnitCatalogRec` (not currently selected)
- **Add inventory-level data:** NumFolio, MaterialCode (→ CODE_FullCode lookup), SizeCode (→ CODE_FullCode lookup) from `dbo_Inventory`
- **Add GenizahTitle lookup:** OrgTitle, EngTitle from `CODE_GenizahTitle` via GenizahTitleId
- **Drop empty columns:** Remove DescriptionEng/DescriptionHeb (IdentificationText) — always empty across all 500K records
- **Extend FTS5 index:** Include RunningTitle and FreeDescription in catalog_fts for full-text search
- All new tables stored as separate normalized tables in the sidecar (not flattened into catalog)

### Physical Metadata Display
- **Key-value pairs** layout: label: value rows (Material: Vellum, Size: 165 × 210 mm, etc.)
- **Separate section** with its own header (e.g., "Physical Description") within the dialog
- **Hide empty fields** — only show fields that have actual data, don't show placeholders
- **Follow app language** for labels — Hebrew labels in Hebrew mode (חומר: קלף), English labels in English mode (Material: Vellum)

### Dialog Content Structure
- **Textual descriptions first, physical metadata below** — lead with scholarly identification (the catalog's main value), physical metadata as supplementary section
- **Side-by-side columns** for multi-team entries — teams as columns, fields as rows (matching FIST web interface layout)
- **Claude's discretion** for >3 teams: horizontal scroll or tabs as appropriate for the data
- **Title and Running Title** shown as distinct fields — both displayed when present
- **Free Description always fully visible** — no expandable/collapsible, show complete scholarly notes inline
- **Dialog title:** "Catalog Records — {shelfmark}" (unchanged from v1)
- **All FJMS data shown** — no deduplication against what browse already shows; the multi-team side-by-side view IS the unique value
- Button appears in **all three locations**: web browse, web search results, desktop Result Dialog

### Button Design & Placement (carried from v1)
- **Label:** "Catalog Records (N)" in English / "מידע קטלוגי (N)" in Hebrew
- **Count N** = number of distinct sources/teams that contributed data (not total entries)
- **Icon:** `description` (Material doc icon) — distinct from bibliography's `menu_book`
- **Style:** `outline dense` — matches existing Bibliography FJMS/Ktiv buttons
- **Web browse:** In the bibliography buttons row (near Bibliography FJMS / Bibliography Ktiv)
- **Web search results:** Button in the metadata section of search result cards
- **Desktop browse:** Same row as bibliography buttons (ext_info_row)
- **Desktop Result Dialog:** Button follows bibliography button pattern
- **No data:** Button disabled with (0) count — researchers see the capability exists even when no data

### Attribution Display (carried from v1)
- **Source header per group** — group entries by SourceName, show source name once as a section header
- **Source language** — follow app language (SourceNameHeb in Hebrew mode, SourceName in English)
- **Author placement** — after title, before frame text: Title → Author → Description

### Partial Data Handling
- **Show physical-only records** — if a catalog record has only sizes/material/status but no textual description, still show it in the dialog
- **Empty cells in side-by-side** — Claude's discretion (dash, skip empty rows, or mix)
- **No data at all** — button disabled with (0), consistent across all locations

### Claude's Discretion
- Exact spacing, typography, and scroll behavior in the dialog
- Handling of >3 team columns (horizontal scroll vs tabs)
- Empty cell display strategy in side-by-side layout
- FTS5 tokenizer configuration for multilingual RunningTitle/FreeDescription content
- Whether to add UnitCatalogRecId as a foreign key in the sidecar tables or use AlmaId-only joins

</decisions>

<specifics>
## Specific Ideas

- The user showed actual FIST data for T-S C1.15 with two teams (Milikowsky Aggadic Midrashim, Mandel Midrash Eikha Rabba) — this side-by-side multi-team view is the reference layout
- The `[$...$]` markup "probably used to smart search" — preserve it in the data, render it nicely but don't strip it
- Desktop and web should have feature parity for this feature across all surfaces (browse, search results, Result Dialog)
- Disabled-when-empty pattern everywhere for consistency
- Alma imported some FJMS cataloging data, so there IS overlap with browse, but show all FJMS data anyway — the multi-team scholarly comparison is the unique value

## Data Source Reference

| FIST Source Table | Sidecar Table | Rows | New Data |
|---|---|---|---|
| `dbo_CatalogMultiRunningTitle` | `catalog_running_titles` | 235K | Detailed per-team running titles |
| `dbo_CatalogMultiSize` | `catalog_sizes` | 161K | SizeX × SizeY per catalog record |
| `dbo_CatalogMultiField` → `CODE_FullCode` | `catalog_fields` | 1.1M | Physical status, material, script, language |
| `dbo_UnitFreeDescription` | `catalog_free_desc` | 190K | Scholarly free-text descriptions |
| `dbo_UnitCatalogRec` | (extend `catalog`) | — | NumRow, NumColumn, NumFolio |
| `dbo_Inventory` + `CODE_FullCode` | (extend `catalog` or separate) | — | Material name, Size category, NumFolio |
| `CODE_GenizahTitle` via GenizahTitleId | (extend `catalog`) | 109K | OrgTitle, EngTitle |

Physical Status codes in CODE_FullCode: Injured (1018), Damaged (15018), Good (3341/35018), Torn, Rubbed, Mutilated, Stained, Holes, Missing, Faded, Defective, Fragmented, Illegible, Remnant, Blurred

</specifics>

<deferred>
## Deferred Ideas

- **Clickable [$reference$] links** — clicking a `[$Sifra$]` or `[$Talmud Yerushalmi$]` reference to trigger a search — future phase
- **FJMS FTS5 search of descriptions** — already tracked as FJMS-04 in requirements (partially addressed by FTS5 extension in this phase)

</deferred>

---

*Phase: 37-fjms-catalog-descriptions*
*Context gathered: 2026-02-17 (v2 — post-revert enrichment)*
