# Codex Critique Brief — Phase 94 `shared/export_dossier.py` API

## Why I am asking

Phase 94 is restructuring the search-results xlsx export on BOTH the web (NiceGUI) app and the desktop (PyQt6) app of GenizahSearch into a 3-sheet workbook (`Genizah Results` + `Manuscripts` dossier sub-sheet + `Bibliography` sub-sheet). To make the workbook structure identical across both apps I'm introducing a new shared module `shared/export_dossier.py`.

The user (`hillel@dicta.org.il`) is the visionary; I'm the builder. They picked aggressive options on Area 2 (column unification, RTL conditional, rich-text snippet) that increase the surface area for the shared module. They explicitly asked me to ask Codex to critique my Area 1 recommendations on `shared/export_dossier.py`'s shape before locking CONTEXT.md.

You (Codex) are a second opinion. Please push back on anything that looks wrong, over-engineered, or under-specified.

## My proposed decisions

### D-A1 — Module scope
`shared/export_dossier.py` exposes:

1. **4 lookup helpers** — module-level functions:
   - `_pgp_subset_for_sys_id(sys_id: str) -> Optional[dict]` — wraps `shared.document_service.get_document_for_fragment(sys_id)`. Returns `{pgp_url, description, document_type, date_display, languages, tags}` or `None`. Date uses fallback chain: `inferred_date_display → doc_date_standard → doc_date_original → None`. Languages from `languages_primary` + `languages_secondary` (handle the comma-string bug — split on comma, NOT iterate chars).
   - `_nli_subset_for_sys_id(sys_id: str) -> Optional[dict]` — wraps `shared.nli_crossref_service.NLICrossrefService.get_catalog_entry(sys_id)` + `.get_library_viewer_url(sys_id)`. Returns `{description, library_viewer_url}` or `None`.
   - `_catalog_summary_for_sys_id(sys_id: str) -> Optional[dict]` — wraps `shared.fjms_service.FjmsService.get_catalog_detail(sys_id)`. Returns 3-5 planner-chosen fields with documented rationale (Author, Title, Copy date, Comment likely; 37 fields total available). Or `None`.
   - `_bibliography_for_sys_id(sys_id: str) -> List[dict]` — wraps `shared.fjms_service.FjmsService.get_bibliography(sys_id)`. Returns a list of bib entries `[{author, title, publisher, year, page_ref, source_name}, ...]`. Empty list when none.

2. **2 row-emitter functions** — return Python primitives (no openpyxl objects):
   - `build_manuscript_row(sys_id, meta_mgr, lang='en') -> List[Any]` — returns the 14 cell values for one Manuscripts sub-sheet row, in this fixed order: `[System ID, Shelfmark, Library, Title, PGP URL, PGP Description, PGP Type, PGP Date, PGP Languages, PGP Tags, NLI Description, Catalog Summary, Library Viewer URL, GenizahSearch URL]`. Calls the 4 lookup helpers internally.
   - `build_bibliography_rows(sys_id, meta_mgr) -> List[List[Any]]` — returns 0..N row tuples for the Bibliography sub-sheet for a single sys_id. Each row: `[System ID, Shelfmark, Author, Title, Publisher, Year, Page Reference, Source Name]`.

Each app (web + desktop) calls these in a loop over unique sys_ids and appends the returned values to its own openpyxl workbook with its own styling (RTL conditional, header style, column widths).

### D-A2 — Error resilience
Each of the 4 lookup helpers wraps its service call in `try/except Exception: logger.warning(...); return None` (or `[]` for bibliography). Lookup failures produce empty cells in the xlsx (matches D-06 missing-data convention). The 2 row-emitter functions are NOT wrapped — they collect Nones and emit empty strings/Nones in the right slots.

### D-A3 — Batch shape (THE QUESTION I MOST WANT YOUR OPINION ON)
The result set may have ~50-200 unique sys_ids. Existing service methods:
- `shared.fjms_service.FjmsService.get_domains_for_sys_ids(sys_ids: List[str]) -> Dict[str, List[str]]` — EXISTS as a batch method (`fjms_service.py:866`).
- `get_catalog_detail(sys_id)`, `get_bibliography(sys_id)`, `get_catalog_entry(sys_id)`, `get_library_viewer_url(sys_id)` — ALL per-sys_id only. No batch variants in the existing service modules.
- `get_document_for_fragment(sys_id)` — per-sys_id.

My proposed plan: **per-sys_id loops for the 4 lookup helpers as the default, no new batch methods added to the existing service modules in this phase.** The user explicitly confirmed (D-09 in the existing CONTEXT.md): "Acceptable cost for an explicit user-triggered download. If lookup times exceed ~3 seconds for typical downloads, consider batch fetches."

The shared module would call each per-sys_id helper N times inside `build_manuscript_row` and `build_bibliography_rows`. The `domains` field on the main sheet (which IS already batched) stays as it is — it's not part of the new dossier module's scope (Domains comes from `search_state.result_domains[sys_id]` per the existing decisions).

**Questions for you**:
1. Is per-sys_id looping inside the row-emitter functions the right factoring? Or should the row builders accept pre-fetched dicts (e.g., `build_manuscript_row(sys_id, pgp_data, nli_data, catalog_data, ...)`) so a higher-level loop can choose to batch where possible?
2. For SQLite-backed lookups (pgp.db, nli_crossref.db, fjms_enrichment.db are all SQLite sidecars per CLAUDE.md), is the per-sys_id call cost dominated by Python overhead or by SQLite query latency? If by Python overhead, batching matters less.
3. Should `_catalog_summary_for_sys_id` be opinionated about which 3-5 FJMS fields it returns, or should it return the full 37-field dict and let `build_manuscript_row` pick? (My instinct: opinionated — the row builder knows the column layout, so the helper should match it.)
4. The user wants identical row content on both apps. Is there a risk that the desktop's `meta_mgr` (shelfmark/title lookup) and web's `meta_mgr` differ enough that `build_manuscript_row(sys_id, meta_mgr)` produces different output? Should I instead pass `(shelfmark, title)` as primitives so the row builder is fully data-driven?
5. The user picked "RTL if downloaded from Hebrew UI, LTR if English UI" for the sheet view. The shared module's row builders return data only — RTL is set by each app's caller via `ws.sheet_view.rightToLeft = (lang == 'he')`. Is this the right cleavage point, or should the shared module own a `write_workbook(wb, results, signals, lang)` function that also sets the sheet view?

## Additional decisions for context

### Area 2 (locked) — affects shared module
- **D-B1**: Final unified main-sheet column order on BOTH apps: `System ID | Library | Shelfmark | Title | Image/Page | Source | Snippet | Full Text | Has PGP | Is Printed | Domains | IIIF Manifest` (12 cols). Web drops `Score` (empty in practice), gains `Image/Page` + `Source` derived from web result dict; desktop gains `Full Text` + the 4 new flag/URL columns. The main sheet is NOT built by `shared/export_dossier.py` — each app builds its own main sheet from its own result rows because the row data is per-folio-hit, not per-sys_id. The shared module only owns the Manuscripts + Bibliography sub-sheets (deduped, one row per sys_id).
- **D-B2**: Manuscripts + Bibliography sub-sheets are identical column-order on both apps via the shared row builders.
- **D-B3**: RTL conditional on UI language (Hebrew → RTL, English → LTR). Applied uniformly across all 3 sheets in the workbook. Each app reads its own UI lang and sets the sheet view itself.
- **D-B4**: Extend desktop's `*` → red bold rich-text snippet rendering to web's main sheet too. Sub-sheets (built from shared module) stay plain text — no rich-text in the dossier.

### Area 3 (locked) — plan structure
- **D-C1**: Move existing 3 plans (94-01, 94-02, 94-03) to `.SUPERSEDED-v2.md`. Re-plan from scratch.
- **D-C2**: 4-wave shape: (1) `shared/export_dossier.py` module + tests, (2) web state plumbing (`printed_ids` through `set_search_export`; JSON `has_pgp`/`is_printed`/`domains`), (3) web xlsx restructure (unified column order, rich-text snippet on web, conditional RTL), (4) desktop xlsx parity + verification + docs.
- **D-C3**: Full `/gsd-review --phase 94 --all` per wave (Gemini + Codex).

## Constraints

- v7.12 multitenant invariants carry forward: every per-user persistence call MUST route through `web/safe_storage.py`. Zero raw `app.storage.user.{get,pop,[key]=}` access under `web/`. `tests/test_no_raw_storage_access.py` allowlist `[]`. Phase 94 should not introduce new raw-storage accesses.
- D-04 (English only) intent: do NOT translate content via `shared/translation_service.py` calls in the export path. But the conditional-RTL change DOES read UI language. Is reading the UI lang for sheet-view direction (not translation) compatible with D-04's spirit? I think yes — D-04 is about translation, not view direction.
- D-02 (no transcription text in NEW dossier surfaces): the existing main-sheet `Full Text` column is grandfathered (Tantivy-indexed page text already in the search payload). New dossier helpers MUST NOT include transcription text.

## What I want from you

Push back where my proposal looks wrong, under-specified, or over-engineered. I am especially interested in:
- Whether per-sys_id looping inside row builders is the right factoring (vs pre-fetched dicts threaded in).
- Whether the 4 lookup helpers should be "leaf" or "opinionated" about their return shape.
- Whether the row builders should own `meta_mgr` calls or take primitives.
- Whether RTL should be set by the shared module or by each caller.
- Anything else you spot.

Format your response as a bullet list of findings, each tagged `MUST-FIX` / `SHOULD-FIX` / `NICE-TO-HAVE` / `OK`. Skip preamble. Keep it under 600 words.
