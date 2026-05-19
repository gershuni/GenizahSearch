# Milestone v7.13: Research-Grade Downloads & PGP Filter — Requirements

**Status:** ACTIVE (started 2026-05-19)
**Goal:** Surface PGP coverage at the result-set level on `/search` and upgrade downloaded xlsx artifacts into citation-grade dossiers so a downloaded file stands alone as a scholarly source.
**Scope:** Mixed.
- Phase 93 (PGP filter) is **web only** — desktop already exposes the same signal via a sortable `COL_PGP` badge column at `genizah_app.py:5599-5634`, no parity required.
- Phase 94 (export metadata) is **web + desktop for the xlsx workbook** (shared helpers in `shared/export_dossier.py` consumed by both apps), and **web-only for the JSON additive flags and the `printed_ids` state plumbing** (desktop has no JSON export and reads `_pgp_transcription_sys_ids` / `_printed_sys_ids` directly from its state machine).

**Carried-forward invariant:** Every per-user persistence call MUST route through `web/safe_storage.py` (Phase 87). Zero raw `app.storage.user.{get,pop,[key]=}` access under `web/` (enforced by `tests/test_no_raw_storage_access.py`, allowlist `[]`).
**Targets:** 14 requirements across 2 phases (93 + 94).

---

## v7.13 Requirements

### PGP Filter — Phase 93 (5 requirements) — promoted from backlog 999.2-filtering-by-pgp

- [ ] **PGP-FILTER-01**: Post-search 3-state filter button (`all` → `only_pgp` → `hide_pgp` → `all`) rendered in the search results toolbar immediately after the existing `printed_filter_btn` (`web/pages/search.py:1430-1434`). Labels per state: `'All'` / `'Has PGP'` / `'No PGP'`, all wrapped in `tr()`. Same `outline dense no-caps` styling as `printed_filter`.
- [ ] **PGP-FILTER-02**: Button visibility gated by current result set — hidden until `bool(search_state.transcription_sys_ids)` is true. Same idiom as `_set_btn_visible(printed_filter_btn, False)` in the existing printed-filter flow.
- [ ] **PGP-FILTER-03**: Active-filter chip shown in the results header row (co-located with `exclusion_chips_row` at `web/pages/search.py:1448-1449`) when state is `only_pgp` or `hide_pgp`. Chip labels mirror state: `'Only PGP'` / `'Hiding PGP'`, translated. Single-click chip → revert to `all`.
- [ ] **PGP-FILTER-04**: Filter cascade applies PGP filter AFTER `printed_filter` in the existing render pipeline (`web/pages/search.py:1409-1414`). Stacks with `exclusion_sources`, `domain_exclusions`, refinement chain — no re-query, post-search only.
- [ ] **PGP-FILTER-05**: Choice persisted via `persist_value('search_pgp_filter', ...)` routed through `web/safe_storage.py` chokepoint (Phase 87 invariant). Bootstrap read via `_safe_get('search_pgp_filter', 'all')` at search-page init, mirroring `printed_filter` at `:148`.

### Export Metadata — Phase 94 (9 requirements) — promoted from backlog 999.3-adding-pgp-to-downloaded-data; web + desktop xlsx

EXPORT-META-01..05 apply to BOTH web AND desktop xlsx exports (consume the same `shared/export_dossier.py` helpers; structure must be identical across apps). EXPORT-META-06 + EXPORT-META-07 are web-only by construction. EXPORT-META-08 is soft scope. EXPORT-META-09 is the desktop-parity requirement.

- [ ] **EXPORT-META-01** (web + desktop): Main xlsx sheet appends `Has PGP`, `Is Printed`, `Domains` columns after the existing `Full Text` column. `Has PGP` / `Is Printed` rendered as `"Yes"` / empty (NOT `"True"`/`"False"`). `Domains` is pipe-delimited (`'Bible|Letter|Legal'`). Multi-folio hits for the same manuscript repeat these per-row flag/value cells. Same on both `web/export_service.py:export_search_results_excel` and `genizah_app.py:export_results('xlsx')`.
- [ ] **EXPORT-META-02** (web + desktop): NEW `Manuscripts` sub-sheet — ONE row per unique `sys_id` observed in the result set (deduped, first-occurrence order). Columns: `System ID`, `Shelfmark`, `Library` (full name via `core_get_library_display(library_code, short=False, lang='en')`), `Title`, `PGP URL`, `PGP Description`, `PGP Type`, `PGP Date` (with `inferred_date_display → doc_date_standard → doc_date_original` fallback), `PGP Languages` (pipe-delimited), `PGP Tags` (pipe-delimited), `NLI Description`, `Catalog Summary` (3-5 FJMS fields chosen by planner with rationale), `Library Viewer URL`, `GenizahSearch URL` (`https://genizahsearch.com/browse?sys_id={sys_id}`). Empty cells for missing data; no `"N/A"` placeholders. Same on both apps via shared dossier helper.
- [ ] **EXPORT-META-03** (web + desktop): NEW `Bibliography` sub-sheet — ONE row per FJMS bib entry, joinable to `Manuscripts` via `System ID`. Columns suggested: `System ID`, `Shelfmark` (denormalized), `Author`, `Title`, `Publisher`, `Year`, `Page Reference`, `Source Name`. Grouped by `System ID`, then natural FJMS order. No row cap. Same on both apps via shared dossier helper.
- [ ] **EXPORT-META-04** (web + desktop): Workbook sheet order is `Genizah Results` (existing, with new columns) → `Manuscripts` → `Bibliography`. First sheet remains default-active on open so existing users see no change in initial-load behavior. Same on both apps.
- [ ] **EXPORT-META-05** (web + desktop): All metadata emitted in English. When source DB has both English and Hebrew variants (some NLI / FJMS fields), pick English; emit Hebrew text only as graceful fallback if English absent. NO `get_language()`, NO translation lookups, NO `shared/translation_service.py` calls in the export path. NO PGP transcription text in any export surface. Same on both apps.
- [ ] **EXPORT-META-06** (web only): `printed_ids` plumbed through `web/export_state.set_search_export(...)` alongside the existing `transcription_sys_ids`, so the export pipeline can compute `Is Printed` and `Has PGP` for every row. Desktop reads `self._printed_sys_ids` and `self._pgp_transcription_sys_ids` directly at the export call site — no state plumbing required.
- [ ] **EXPORT-META-07** (web only): JSON per-item dict (in `shared/search_serializer.py:_serialize_item`) gains 3 additive keys: `is_printed` (bool), `has_pgp` (bool), `domains` (list — verify whether already present at `:315`). Envelope `schema_version` stays `1` per Phase 83 additive-change commitment. NO `pgp` subobject; NO dossier in JSON. Desktop has no JSON export so this requirement does not apply there.
- [ ] **EXPORT-META-08** (web + desktop, soft scope): `IIIF Manifest` per-page column on the main xlsx sheet — SOFT scope. Planner may emit it (using `nli_crossref_service.get_folio_images(sys_id)` and library-viewer manifest reachability for NLI / Cambridge / Manchester / JTS) OR defer it to the Manuscripts sub-sheet with documented rationale (likely "per-page IIIF resolution requires too much new plumbing for this phase"). If emitted, applies to both apps via shared helper.
- [ ] **EXPORT-META-09** (desktop): Desktop xlsx search-results export rewired to emit the same 3-sheet workbook structure as web (`Genizah Results` with new flag columns + `Manuscripts` sub-sheet + `Bibliography` sub-sheet), reusing the same 4 lookup helpers from `shared/export_dossier.py`. Wired into `genizah_app.py:export_results('xlsx')` at the existing call site (around lines 13254 / 17986 — planner to identify the search-results path vs the composition-results path). Desktop CSV, TXT, and DOCX exports are NOT modified (parity is xlsx-only). Verify `_result_domains` or equivalent exists on the desktop state machine; if not, plumb it through the desktop search pipeline as part of this requirement.

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

| REQ-ID | Phase | Plan slots (estimated) | Source CONTEXT | Status |
|--------|-------|------------------------|----------------|--------|
| PGP-FILTER-01 | 93 | 1 | `.planning/phases/999.2-filtering-by-pgp/999.2-CONTEXT.md` | Pending |
| PGP-FILTER-02 | 93 | 1 | `.planning/phases/999.2-filtering-by-pgp/999.2-CONTEXT.md` | Pending |
| PGP-FILTER-03 | 93 | 1 | `.planning/phases/999.2-filtering-by-pgp/999.2-CONTEXT.md` | Pending |
| PGP-FILTER-04 | 93 | 1 | `.planning/phases/999.2-filtering-by-pgp/999.2-CONTEXT.md` | Pending |
| PGP-FILTER-05 | 93 | 1 | `.planning/phases/999.2-filtering-by-pgp/999.2-CONTEXT.md` | Pending |
| EXPORT-META-01 | 94 | 4 (wave-shared) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` | Pending |
| EXPORT-META-02 | 94 | 4 (wave-shared) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` | Pending |
| EXPORT-META-03 | 94 | 4 (wave-shared) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` | Pending |
| EXPORT-META-04 | 94 | 4 (wave-shared) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` | Pending |
| EXPORT-META-05 | 94 | 4 (wave-shared) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` | Pending |
| EXPORT-META-06 | 94 | 4 (wave-shared) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` | Pending |
| EXPORT-META-07 | 94 | 4 (wave-shared) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` | Pending |
| EXPORT-META-08 | 94 | 4 (wave-shared) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` | Pending |
| EXPORT-META-09 | 94 | 4 (wave-shared; landed in Wave 4 — desktop parity) | `.planning/phases/999.3-adding-pgp-to-downloaded-data/999.3-CONTEXT.md` + user scope clarification 2026-05-19 | Pending |

**Coverage:** 14/14 requirements mapped (5 → Phase 93, 9 → Phase 94). No orphans. No duplicates.

**Note on Phase 94 plan slot count:** The "Plan slots (estimated)" column shows 4 per EXPORT-META-* requirement because Phase 94's plans are organized by *wave* (shared dossier module → web state plumbing → web xlsx restructure → desktop xlsx parity), not by per-requirement. All 9 EXPORT-META requirements span the 4-plan estimate, not 4 plans each. The prior backlog plan `999.3-01-PLAN.md` is SUPERSEDED by (a) the 2026-05-17 CONTEXT.md broadening AND (b) the 2026-05-19 desktop-parity scope addition; it will be re-planned from scratch via `/gsd-plan-phase 94`. The original `999.3-CONTEXT.md` does NOT yet record the desktop-parity scope — `/gsd-discuss-phase 94` should be re-run to refresh CONTEXT.md before planning, OR the planner should incorporate EXPORT-META-09's intent directly from this file.

---

*Last updated: 2026-05-19 — Scope clarification: Phase 93 web-only (unchanged); Phase 94 expanded to web + desktop xlsx (EXPORT-META-09 added). Goal sentence reworded "with PGP transcriptions" → "with PGP info" to match the green badge's actual semantics. v7.12 multitenant invariants carry forward.*
