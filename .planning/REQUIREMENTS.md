# Milestone v7.13: Research-Grade Downloads & PGP Filter — Requirements

**Status:** ACTIVE (started 2026-05-19)
**Goal:** Surface PGP coverage at the result-set level on `/search` and upgrade downloaded xlsx artifacts into citation-grade dossiers so a downloaded file stands alone as a scholarly source.
**Scope:** Web only — desktop explicitly out of scope per both source CONTEXT files.
**Carried-forward invariant:** Every per-user persistence call MUST route through `web/safe_storage.py` (Phase 87). Zero raw `app.storage.user.{get,pop,[key]=}` access under `web/` (enforced by `tests/test_no_raw_storage_access.py`, allowlist `[]`).
**Targets:** 13 requirements across 2 phases (93 + 94).

---

## v7.13 Requirements

### PGP Filter — Phase 93 (5 requirements) — promoted from backlog 999.2-filtering-by-pgp

- [ ] **PGP-FILTER-01**: Post-search 3-state filter button (`all` → `only_pgp` → `hide_pgp` → `all`) rendered in the search results toolbar immediately after the existing `printed_filter_btn` (`web/pages/search.py:1430-1434`). Labels per state: `'All'` / `'Has PGP'` / `'No PGP'`, all wrapped in `tr()`. Same `outline dense no-caps` styling as `printed_filter`.
- [ ] **PGP-FILTER-02**: Button visibility gated by current result set — hidden until `bool(search_state.transcription_sys_ids)` is true. Same idiom as `_set_btn_visible(printed_filter_btn, False)` in the existing printed-filter flow.
- [ ] **PGP-FILTER-03**: Active-filter chip shown in the results header row (co-located with `exclusion_chips_row` at `web/pages/search.py:1448-1449`) when state is `only_pgp` or `hide_pgp`. Chip labels mirror state: `'Only PGP'` / `'Hiding PGP'`, translated. Single-click chip → revert to `all`.
- [ ] **PGP-FILTER-04**: Filter cascade applies PGP filter AFTER `printed_filter` in the existing render pipeline (`web/pages/search.py:1409-1414`). Stacks with `exclusion_sources`, `domain_exclusions`, refinement chain — no re-query, post-search only.
- [ ] **PGP-FILTER-05**: Choice persisted via `persist_value('search_pgp_filter', ...)` routed through `web/safe_storage.py` chokepoint (Phase 87 invariant). Bootstrap read via `_safe_get('search_pgp_filter', 'all')` at search-page init, mirroring `printed_filter` at `:148`.

### Export Metadata — Phase 94 (8 requirements) — promoted from backlog 999.3-adding-pgp-to-downloaded-data

- [ ] **EXPORT-META-01**: Main xlsx sheet appends `Has PGP`, `Is Printed`, `Domains` columns after the existing `Full Text` column. `Has PGP` / `Is Printed` rendered as `"Yes"` / empty (NOT `"True"`/`"False"`). `Domains` is pipe-delimited (`'Bible|Letter|Legal'`). Multi-folio hits for the same manuscript repeat these per-row flag/value cells.
- [ ] **EXPORT-META-02**: NEW `Manuscripts` sub-sheet — ONE row per unique `sys_id` observed in the result set (deduped, first-occurrence order). Columns: `System ID`, `Shelfmark`, `Library` (full name via `core_get_library_display(library_code, short=False, lang='en')`), `Title`, `PGP URL`, `PGP Description`, `PGP Type`, `PGP Date` (with `inferred_date_display → doc_date_standard → doc_date_original` fallback), `PGP Languages` (pipe-delimited), `PGP Tags` (pipe-delimited), `NLI Description`, `Catalog Summary` (3-5 FJMS fields chosen by planner with rationale), `Library Viewer URL`, `GenizahSearch URL` (`https://genizahsearch.com/browse?sys_id={sys_id}`). Empty cells for missing data; no `"N/A"` placeholders.
- [ ] **EXPORT-META-03**: NEW `Bibliography` sub-sheet — ONE row per FJMS bib entry, joinable to `Manuscripts` via `System ID`. Columns suggested: `System ID`, `Shelfmark` (denormalized), `Author`, `Title`, `Publisher`, `Year`, `Page Reference`, `Source Name`. Grouped by `System ID`, then natural FJMS order. No row cap.
- [ ] **EXPORT-META-04**: Workbook sheet order is `Genizah Results` (existing, with new columns) → `Manuscripts` → `Bibliography`. First sheet remains default-active on open so existing users see no change in initial-load behavior.
- [ ] **EXPORT-META-05**: All metadata emitted in English. When source DB has both English and Hebrew variants (some NLI / FJMS fields), pick English; emit Hebrew text only as graceful fallback if English absent. NO `get_language()`, NO translation lookups, NO `shared/translation_service.py` calls in the export path. NO PGP transcription text in any export surface.
- [ ] **EXPORT-META-06**: `printed_ids` plumbed through `web/export_state.set_search_export(...)` alongside the existing `transcription_sys_ids`, so the export pipeline can compute `Is Printed` and `Has PGP` for every row.
- [ ] **EXPORT-META-07**: JSON per-item dict (in `shared/search_serializer.py:_serialize_item`) gains 3 additive keys: `is_printed` (bool), `has_pgp` (bool), `domains` (list — verify whether already present at `:315`). Envelope `schema_version` stays `1` per Phase 83 additive-change commitment. NO `pgp` subobject; NO dossier in JSON.
- [ ] **EXPORT-META-08**: `IIIF Manifest` per-page column on the main xlsx sheet — SOFT scope. Planner may emit it (using `nli_crossref_service.get_folio_images(sys_id)` and library-viewer manifest reachability for NLI / Cambridge / Manchester / JTS) OR defer it to the Manuscripts sub-sheet with documented rationale (likely "per-page IIIF resolution requires too much new plumbing for this phase").

---

## Future Requirements (deferred from this milestone)

These were explicitly deferred during `/gsd-discuss-phase` for 999.2 / 999.3 and are noted here so they don't get lost:

- **Pre-search PGP filter** — wire `has_pgp_transcription` into the filter-panel dialog and pass through to the search engine so the result list is filtered server-side. Useful if PGP-only searches become a frequent workflow.
- **PGP filter on the parallels page** — same toggle on `/parallels` (uses same `filter_panel.py` helpers).
- **PGP filter on the desktop app** — parity entry. Desktop search toolbar in `genizah_app.py:5163`.
- **Filter by PGP source / author / version** — fine-grained (only Goitein, only V0.8, only translations, etc.).
- **Per-item `metadata` block in JSON** — surfaces full dossier in JSON. Soft-rejected to keep envelope small.
- **Word export with the same metadata** — Word's layout primitives are paragraphs, not sheets; significant redesign.
- **List export with metadata** — `export_list_excel` at `:413`. Same shape applicable; mirror after this phase ships.
- **Parallels export with metadata** — same shape, parallels-specific row structure.
- **Hebrew translations of metadata** — pgp.db has 34,954 EN→HE PGP descriptions; NLI / FJMS have Hebrew variants. Possible opt-in "translated" export mode later.
- **Excel hyperlink rendering** for URL columns — plain text default per citation safety, upgrade later if cheap.
- **CSV export** — multi-sheet doesn't map to CSV.
- **Visual similarity partners column** — `vs_availability` is on search state; future addition.
- **FJMS join data sub-sheet** — `get_joins(sys_id)` could populate a third sub-sheet for puzzle-relevant manuscripts.

## Out of Scope (explicit exclusions)

- Word search-results export (PGP-FILTER + EXPORT-META untouched).
- List exports (`export_list_excel`).
- Parallels exports.
- PGP transcription text in any export surface (size + scope).
- Translating any metadata (PGP / NLI / catalog descriptions) at export time.
- Per-item dossier in JSON envelope.
- PGP filter on desktop app and `/parallels` page.

## Traceability

| REQ-ID | Phase | Source CONTEXT |
|--------|-------|----------------|
| PGP-FILTER-01..05 | 93 (was 999.2) | `.planning/phases/999.2-filtering-by-pgp/999.2-CONTEXT.md` |
| EXPORT-META-01..08 | 94 (was 999.3) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` |

Phase ↔ requirement map will be locked when the roadmapper writes `ROADMAP.md`.

---

*Last updated: 2026-05-19 — v7.13 milestone scaffold committed; awaiting roadmapper.*
