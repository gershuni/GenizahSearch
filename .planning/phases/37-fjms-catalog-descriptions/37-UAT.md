---
status: resolved
phase: 37-fjms-catalog-descriptions
source: 37-01-SUMMARY.md, 37-02-SUMMARY.md, 37-03-SUMMARY.md, 37-04-SUMMARY.md
started: 2026-02-18T12:00:00Z
updated: 2026-02-22T20:00:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web Browse - Catalog Records Button Visible
expected: Navigate to a PGP document with FJMS catalog data in the web browse page. A "Catalog Records (N)" button appears in the bibliography row showing the source count.
result: pass

### 2. Web Browse - Catalog Dialog Content
expected: Click the Catalog Records button on the browse page. A dialog opens with the FIST 5-section layout (Shelfmark Description, Content Description, Script Description, Miscellaneous). Data shows team columns side-by-side with source attribution headers.
result: pass (fixed 2026-02-18 in phase 37-05)
reported: "The Miscellaneous is shown line under line - understandable given the nature of the long data, but if we keep it that way we have to put there the attribution to the source of information"
resolution: Source attribution added to free descriptions via catalog_free_desc SourceName/SourceNameHeb columns

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
result: pass (fixed 2026-02-22)
reported: "RTL in Hebrew only when English interface is on. Heb interface it's LTR for both languages and the table layout is LTR also"
resolution: Removed manual text-align overrides from FjmsCatalogDialog HTML — Qt app-level RTL handles text alignment naturally; kept manual column order reversal for Hebrew

### 7. Desktop ResultDialog - Catalog Button
expected: Open a search result in the desktop ResultDialog. A "Catalog Records (N)" button appears in the action row. Clicking it opens the catalog dialog with the same structured data.
result: pass

## Summary

total: 7
passed: 7
issues: 0 (2 resolved)
pending: 0
skipped: 0

## Gaps

All gaps resolved.

- truth: "Miscellaneous free descriptions show source attribution for each entry"
  status: resolved (2026-02-18, phase 37-05)
  fix: Added SourceName/SourceNameHeb to catalog_free_desc export, service layer, and both UI renderers

- truth: "Desktop catalog dialog respects RTL layout direction in Hebrew interface mode"
  status: resolved (2026-02-22)
  fix: Removed manual text-align CSS overrides from FjmsCatalogDialog HTML — Qt app-level RTL handles text alignment naturally; kept manual column order reversal (values first, label last in <tr>) for correct Hebrew layout
