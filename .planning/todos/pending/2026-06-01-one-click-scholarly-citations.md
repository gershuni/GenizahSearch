---
created: 2026-06-01
title: One-click scholarly citations (BibTeX / RIS / Zotero / Chicago)
area: export, web, desktop
priority: high
source: v8.0 headline-feature ideation (see docs/FEATURE_IDEAS.md)
files:
  - shared/citations.py   # new
  - shared/export_dossier.py
  - web/pages/search_results.py
  - web/pages/browse.py
  - genizah_app.py
---

## Problem

There is no per-manuscript citation. `Copy full citation` (web footer, `web/main.py`) only
copies the **dataset-level** MiDRASH citation — a single static string. A scholar who finds a
manuscript and wants to cite it in a paper has to assemble shelfmark + library + transcription
source + DOI by hand, and there is no machine-readable export (BibTeX / RIS) for reference
managers (Zotero, Mendeley).

This is the #1 friction between *finding* a manuscript here and *publishing* about it — a
strong, low-cost headline for the "Pro" rebrand release (candidate to justify v8.0).

## Solution

1. New shared `shared/citations.py` with `format_citation(meta, style)` returning:
   - `bibtex`, `ris` (import into Zotero/Mendeley), `chicago`/footnote text, `plain`.
   - Inputs are the SAME fields already assembled for the xlsx Bibliography sheet in
     `shared/export_dossier.py`: shelfmark, library, title, FJMS bibliography, catalog data,
     MiDRASH dataset DOI. No new data source, no network, no auth.
2. A **Cite** button on the search result card, Browse/manuscript view, Reading Desk, and lists
   → small dialog with the four formats + per-format copy buttons.
3. **"Export all results as `.bib`"** next to the existing export buttons (web + desktop), reusing
   the export result-set plumbing.
4. Bilingual labels via the i18n layer (`tr()`), RTL-aware dialog.

## Notes / gray areas (for /gsd-discuss-phase)

- Author/date fields for Genizah fragments are often unknown — decide the citation shape for
  manuscripts vs printed editions (FJMS `is_printed`).
- LOCAL "My Library" hits have no scholarly metadata — gate the Cite button off for LOCAL rows
  (same pattern as the community buttons; see OPEN_ISSUES D-F11).
- Decide whether the per-manuscript citation also credits the transcription dataset (MiDRASH DOI)
  inline or only in the dataset-level citation.
