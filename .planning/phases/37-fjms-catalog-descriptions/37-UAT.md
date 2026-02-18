---
status: diagnosed
phase: 37-fjms-catalog-descriptions
source: 37-01-SUMMARY.md, 37-02-SUMMARY.md, 37-03-SUMMARY.md, 37-04-SUMMARY.md
started: 2026-02-18T12:00:00Z
updated: 2026-02-18T12:35:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web Browse - Catalog Records Button Visible
expected: Navigate to a PGP document with FJMS catalog data in the web browse page. A "Catalog Records (N)" button appears in the bibliography row showing the source count.
result: pass

### 2. Web Browse - Catalog Dialog Content
expected: Click the Catalog Records button on the browse page. A dialog opens with the FIST 5-section layout (Shelfmark Description, Content Description, Script Description, Miscellaneous). Data shows team columns side-by-side with source attribution headers.
result: issue
reported: "The Miscellaneous is shown line under line - understandable given the nature of the long data, but if we keep it that way we have to put there the attribution to the source of information"
severity: minor

### 3. Web Search - Catalog Records Button on Result Cards
expected: Run a search returning results with FJMS catalog data. Result cards show a "Catalog Records (N)" button with the count of catalog sources.
result: pass

### 4. Web Search - Catalog Dialog from Search Card
expected: Click the Catalog Records button on a search result card. The catalog dialog opens with the same 5-section layout as the browse page dialog.
result: pass

### 5. Desktop Browse - Catalog Records Button Visible
expected: Navigate to a PGP document with FJMS catalog data in the desktop Browse tab. A "Catalog Records (N)" button appears in the external info row with the source count.
result: pass

### 6. Desktop Browse - Catalog Dialog Content
expected: Click the Catalog Records button in the desktop app. A dialog opens with an HTML table showing the 5-section layout (Shelfmark, Content, Script, Format, Misc) with RTL support for Hebrew text.
result: issue
reported: "RTL in Hebrew only when English interface is on. Heb interface it's LTR for both languages and the table layout is LTR also"
severity: major

### 7. Desktop ResultDialog - Catalog Button
expected: Open a search result in the desktop ResultDialog. A "Catalog Records (N)" button appears in the action row. Clicking it opens the catalog dialog with the same structured data.
result: pass

## Summary

total: 7
passed: 5
issues: 2
pending: 0
skipped: 0

## Gaps

- truth: "Miscellaneous free descriptions show source attribution for each entry"
  status: failed
  reason: "User reported: The Miscellaneous is shown line under line - understandable given the nature of the long data, but if we keep it that way we have to put there the attribution to the source of information"
  severity: minor
  test: 2
  root_cause: "catalog_free_desc table lacks SourceName/SourceNameHeb columns. Free descriptions are stored per-SignatureId without source team attribution. The join chain SignatureId->UnitCatalogRecId->SourceName is not preserved in the sidecar export."
  artifacts:
    - path: "scripts/export_fist_enrichment.py"
      issue: "export_catalog_free_desc() does not join to get source attribution"
    - path: "shared/fjms_service.py"
      issue: "get_catalog_detail() free_descriptions query returns only text+signature_id, no source"
    - path: "web/components/catalog_dialog.py"
      issue: "_render_free_descriptions() only renders text, no source label"
    - path: "genizah_app.py"
      issue: "Desktop _build_html() free descriptions section has same missing attribution"
  missing:
    - "Add SourceName/SourceNameHeb to catalog_free_desc export via JOIN through Signature->UnitCatalogRec->catalog"
    - "Update get_catalog_detail() to return source_name with each free description"
    - "Update web and desktop renderers to show source attribution per free description entry"

- truth: "Desktop catalog dialog respects RTL layout direction in Hebrew interface mode"
  status: failed
  reason: "User reported: RTL in Hebrew only when English interface is on. Heb interface it's LTR for both languages and the table layout is LTR also"
  severity: major
  test: 6
  root_cause: "Three missing RTL configurations in FjmsCatalogDialog: (1) No dir wrapper on HTML content, (2) No setLayoutDirection() on QTextBrowser widget, (3) Hardcoded text-align:left in team header row instead of conditional"
  artifacts:
    - path: "genizah_app.py"
      issue: "FjmsCatalogDialog.__init__ ~line 5205: text_browser missing setLayoutDirection()"
    - path: "genizah_app.py"
      issue: "_build_html() ~line 5218: returns raw table without <div dir='rtl'> wrapper"
    - path: "genizah_app.py"
      issue: "Team header ~line 5305: hardcoded text-align:left instead of conditional on is_heb"
  missing:
    - "Add setLayoutDirection(RightToLeft) on text_browser when is_heb"
    - "Wrap _build_html() return in <div dir='rtl'> when is_heb"
    - "Make header text-align conditional: right for Hebrew, left for English"
