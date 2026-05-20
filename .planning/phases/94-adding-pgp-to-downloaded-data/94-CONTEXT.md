# Phase 94: Research-Grade Export Metadata — Context

**Phase name:** "Research-Grade Export Metadata" (renamed from "Adding PGP to downloaded data" → "Adding metadata to downloaded data" → final v7.13 name on 2026-05-19). Phase number is `94` (promoted from backlog `999.3` by `/gsd-review-backlog` on 2026-05-19; directory `94-adding-pgp-to-downloaded-data` retained for git-history continuity).

**Gathered:** 2026-05-15
**Revised 2026-05-17:** Scope broadened — PGP became one of several metadata sources; xlsx gained two new sub-sheets (Manuscripts dossier + Bibliography). JSON stays minimal.
**Revised 2026-05-19:** Phase renumbered (`999.3` → `94`); requirement names updated (`METADATA-EXPORT-01..07` → `EXPORT-META-01..09`); **desktop-parity scope added** (`EXPORT-META-09`) — desktop xlsx must match web's 3-sheet structure via shared `shared/export_dossier.py`.
**Revised 2026-05-19 (this revision):** Column order UNIFIED across web and desktop; RTL conditional on UI language; rich-text snippet rendering extended to web's main sheet; shared module API tightened per `94-CODEX-CRITIQUE.md` (real FJMS bib field names; NLI column renamed; `get_catalog_records()` instead of `get_catalog_detail()`; public-API helper names without underscore prefix; metadata resolver instead of opaque `meta_mgr`).

**Status:** Ready for re-planning. The 3 prior plans (`94-01-PLAN.md`, `94-02-PLAN.md`, `94-03-PLAN.md`) are SUPERSEDED by this revision and will be moved to `*.SUPERSEDED-v2.md`. The original `94-01-PLAN.SUPERSEDED.md` (from the 999.3 era) remains as historical artifact.

<domain>
## Phase Boundary

Extend the search-results downloads (xlsx + JSON) on BOTH the web app and the desktop app with research-grade metadata so a downloaded artifact stands alone as a citation source. The xlsx workbook structure is identical across both apps via a new shared module `shared/export_dossier.py` consumed by both call sites.

**Xlsx gains 3 sheets, identical column order on both apps:**

1. **Genizah Results** (existing, restructured): unified 12-column order with new flag/URL columns appended.
2. **Manuscripts** (NEW, sub-sheet): one row per unique sys_id observed in the result set (deduped). Dossier-style — PGP fields, NLI catalog entry, FJMS catalog summary, library viewer URL, GenizahSearch deep link.
3. **Bibliography** (NEW, sub-sheet): one row per FJMS bibliography entry, joinable to `Manuscripts` via `System ID`.

**JSON (web only) stays minimal:** per-item gains `is_printed` (bool), `domains` (list), `has_pgp` (bool). NO PGP subobject, NO dossier metadata. The JSON envelope keeps its small, stable public shape — research scripts that want dossier data either download the xlsx or call `/api/browse` per sys_id. Desktop has no JSON export.

**In scope (web):**
- `web/export_service.py:410 export_search_results_excel` — restructured to build a 3-sheet workbook, consume `shared/export_dossier.py`, switch to unified column order, add rich-text snippet rendering, conditional RTL based on user UI lang.
- `shared/search_serializer.py:298 _serialize_item` — add `is_printed` and `has_pgp` keys (verify `domains` already at `:315`).
- `web/export_state.py:set_search_export(...)` — accept and persist `transcription_sys_ids`, `printed_ids`, `result_domains` kwargs; add `update_search_export_enrichment(...)` helper for post-enrichment patching.
- `web/pages/search.py` — all 3 `set_search_export` call sites pass the new kwargs; post-enrichment site calls `update_search_export_enrichment`.
- `web/api.py:export_excel` and `:export_json` endpoints — pass `transcription_sys_ids`/`printed_ids` from the session payload through to the export pipeline.

**In scope (desktop, NEW 2026-05-19):**
- `genizah_app.py:17895 def export_results(self, fmt='xlsx')` — restructured to emit the same 3-sheet workbook via `shared/export_dossier.py`. Reads `_pgp_transcription_sys_ids` (`:2547`), `_printed_sys_ids` (`:2550`), `_result_domain_map` (`:5461`) directly from the desktop state machine — no `export_state.py`-equivalent indirection.
- Desktop's existing rich-text snippet rendering (`*` markers → red bold via `TextBlock`/`CellRichText`/`InlineFont` at `:17988`+) is preserved and is the canonical pattern that web's main sheet adopts.

**In scope (shared):**
- NEW `shared/export_dossier.py` module: 4 lookup helpers + 2 row-emitter functions + 2 header-list constants. Consumed by both apps.

**Out of scope (see Deferred Ideas):**
- Word search-results export (`web/export_service.py:export_search_results_word`).
- List exports (`web/export_service.py:export_list_excel`).
- Parallels exports (Excel + Word).
- Desktop CSV / TXT / DOCX exports.
- Desktop composition-results export (separate state: `_comp_*` at `:5805-5807`).
- PGP transcription text in NEW dossier surfaces (Manuscripts, Bibliography, JSON). The existing main-sheet `Full Text` column is grandfathered — see D-02.
- Translating PGP / NLI / catalog content to Hebrew. (`lang` parameter on row builders is ONLY for downstream sheet-view direction, never content translation.)
- A per-item `metadata` block in JSON.

</domain>

<decisions>
## Implementation Decisions

### D-01 — Unified main-sheet column order (BOTH apps, REVISED 2026-05-19)

Final column order on the `Genizah Results` sheet, identical on web and desktop:

```
System ID | Library | Shelfmark | Title | Image/Page | Source |
Snippet | Full Text | Has PGP | Is Printed | Domains | IIIF Manifest
```

12 columns total. The first 7 match desktop's existing layout (`genizah_app.py:17910`). `Full Text` is column 8. The 4 new metadata columns are columns 9-12.

**Web changes from previous main-sheet layout:**
- DROPS `Score` (Tantivy relevance score; empty in practice — see CODEX-CRITIQUE for rationale).
- REORDERS: previous web layout was `Shelfmark | Library | Title | System ID | Score | Snippet | Full Text` → new layout puts `System ID` first, then `Library | Shelfmark | Title`.
- GAINS `Image/Page` (per-row folio/page reference; derive from `result['display']['img']` or equivalent in web's result dict — planner identifies the exact field).
- GAINS `Source` (per-row source label; derive from `result['display']['source']` or equivalent).
- GAINS the 4 new appended columns: `Has PGP`, `Is Printed`, `Domains`, `IIIF Manifest`.

**Desktop changes from previous main-sheet layout:**
- KEEPS its existing 7 columns (`System ID, Library, Shelfmark, Title, Image/Page, Source, Snippet`) unchanged in order.
- GAINS `Full Text` (column 8) — read from the desktop search result's full-text field (verify what's available in the desktop result dict; planner identifies the field).
- GAINS the 4 new appended columns: `Has PGP`, `Is Printed`, `Domains`, `IIIF Manifest`.

**Column data sources (both apps):**

| Column | Source |
|---|---|
| System ID | `result['display']['id']` |
| Library | full library name via `genizah_core.get_library_display(library_code, short=False, lang='en')` |
| Shelfmark | `result['display']['shelfmark']` |
| Title | `result['display']['title']` (or `meta_mgr.get_meta_for_id(sys_id)`) |
| Image/Page | per-row folio/page label; desktop: `result['display'].get('img', '')`; web: planner identifies equivalent field |
| Source | per-row source label; desktop: `d.get('source', '')`; web: planner identifies equivalent field |
| Snippet | `raw_file_hl` with `*` markers preserved (rich-text rendering on both apps — see D-B4) |
| Full Text | grandfathered Tantivy-indexed page text per D-02 amendment; web reads via `_resolve_result_full_text`; desktop reads from result dict |
| Has PGP | `sys_id in transcription_sys_ids` → `"Yes"` / empty cell (D-06) |
| Is Printed | `sys_id in printed_ids` → `"Yes"` / empty cell |
| Domains | pipe-joined `result_domains[sys_id]` per D-05 — e.g. `Bible\|Letter\|Legal` |
| IIIF Manifest | per-page IIIF URL when available (NLI / Cambridge / Manchester / JTS via existing image-resolution logic); empty cell otherwise. Per D-13, may be DEFERRED to the Manuscripts sub-sheet with documented rationale — Claude's Discretion. |

**Per-row repetition:** Multi-folio hits for the same manuscript repeat the 4 new flag/URL cells on every row. The Manuscripts sub-sheet dedupes — ONE row per unique sys_id — so the dossier data lives there exactly once.

### D-02 — Transcription text (REVISED 2026-05-19, amends original D-02)

**Old D-02:** "Do NOT include PGP transcription text in any export surface."

**New D-02:** The existing main-sheet `Full Text` column is GRANDFATHERED. It contains Tantivy-indexed page text already present in the search payload, accessed via `web/export_service.py:_resolve_result_full_text` (web) and result-dict full-text field (desktop). This text MAY overlap with PGP transcription text when the result has a PGP record, but it's a pre-existing column, not a fresh PGP lookup, and removing it would be a regression for citation users.

**Strict D-02 prohibition applies to NEW dossier surfaces only:**
- Manuscripts sub-sheet: NO transcription text. PGP Description (scholarly summary) is included; PGP `page_section_text` is NOT.
- Bibliography sub-sheet: NO transcription text.
- JSON envelope additions (`has_pgp`, `is_printed`, `domains`): NO transcription text.
- `shared/export_dossier.py`: NO helpers that read PGP `page_section_text` or FJMS `full_texts`. The `catalog_summary_for_sys_id` helper specifically MUST use `get_catalog_records()` (narrow query) rather than `get_catalog_detail()` (pulls `full_texts`) — see D-08 + Codex MUST-FIX 3.

Transcription text remains available via `/api/browse` per sys_id.

### D-03 — Sheet ordering & default-active sheet
Sheet order in the workbook (both apps): `Genizah Results` (default-active) → `Manuscripts` → `Bibliography`. The first sheet remains the default-active sheet on open so existing users see no change in initial-load behavior.

### D-04 — Language (English-only metadata; conditional RTL view)

**Content:** Always emit metadata content in English. PGP is canonically English; NLI catalog entries are English (plus Hebrew on some entries — surface the English one); FJMS catalog/bib data is mixed but the canonical/scholarly form is English. NO `get_language()` for translation lookups, NO `shared/translation_service.py` calls in the export path. When a field has both English and Hebrew variants in the source DB, pick English; emit Hebrew text only as graceful fallback when English absent.

**View direction (REVISED 2026-05-19):** RTL `sheet_view.rightToLeft` is CONDITIONAL on the user's UI language at export time. Hebrew UI → RTL on all 3 sheets. English UI → LTR on all 3 sheets. Reading the UI lang for view-direction is NOT a translation operation — D-04's translation prohibition does NOT apply to sheet-view direction.

**Web:** reads its UI lang preference via `web/safe_storage.py` chokepoint (planner identifies the exact key — likely `lang` or `user_language`). Passes the lang as a parameter to `export_search_results_excel`.

**Desktop:** reads its locale state directly at the export call site (planner identifies — likely a `self.current_language` field or equivalent). Sets `ws.sheet_view.rightToLeft = (lang == 'he')` for each of the 3 sheets.

**Shared module contract:** `shared/export_dossier.py` row builders accept a `lang` parameter that is DOCUMENTED as ONLY for downstream view direction. The shared module itself does NOT set sheet_view (that's the caller's job). Row content is always English.

**REVISED 2026-05-20 (smoke verification gap fix):** The English-only-content
prohibition above is REVERSED for row content. After Hillel ran the
post-Wave 4 smoke verification on real exports, two gaps were reported:

1. Hebrew UI must produce Hebrew xlsx (headers + sheet names). The desktop
   previously used Qt `tr()` for these strings and `ws.title = tr("Search Results")`;
   the Wave 4 restructure dropped header/sheet-title translation. Web was
   English-only since inception so this is also a forward-improvement on
   the web side.

2. Metadata source language: when the source DB has both Hebrew and English
   variants of a field, prefer the variant matching the UI language (with
   graceful fallback to the other variant).

**New contract:**

- `lang='he'` → Hebrew sheet titles + headers + Hebrew-preferred metadata
  content (with English fallback per field).
- `lang='en'` → English everywhere (with Hebrew fallback per field; symmetric
  for back-compat with the pre-reversal behavior).
- The reversal scope is NARROW: only the row content layer. The D-02
  prohibition on transcription / full-text in NEW dossier surfaces is
  UNCHANGED. The D-10 parallels-envelope strip is UNCHANGED. The
  conditional RTL view-direction logic is UNCHANGED.

**Implementation surfaces (Phase 94-04 follow-up commit):**

- `shared/export_dossier.py` gains 4 new bilingual helpers: `main_header_row(lang)`,
  `manuscript_header_row(lang)`, `bibliography_header_row(lang)`,
  `sheet_titles(lang) -> {main, manuscripts, bibliography}`. The English
  `MANUSCRIPT_HEADERS` / `BIBLIOGRAPHY_HEADERS` constants remain for
  back-compat — `manuscript_header_row('en')` returns them verbatim.
- `pgp_subset_for_sys_id(sys_id, lang='en')` prefers Hebrew `description` /
  `document_type` from the existing `pgp_translations` table (via
  `shared/translation_service.py:TranslationService.get_pgp_translations_by_sys_ids`)
  when `lang == 'he'`. Service is already battle-tested — no new infrastructure.
- `catalog_summary_for_sys_id(sys_id, lang='en')` flips title preference:
  `title_heb` first when `lang == 'he'`, else `title` first. The other
  fields (`author_text`, `copy_date`, `copy_place`) are not Hebrew-translated
  in the FJMS sidecar so they pass through unchanged.
- `bibliography_for_sys_id(sys_id, lang='en')` prefers `running_title_heb` /
  `article_author_heb` when `lang == 'he'`. These columns were already
  surfaced by `FjmsService.get_bibliography` (Phase 33 META-03) — no new
  service method required.
- `build_manuscript_row(sys_id, meta_resolver, lang='en')` threads `lang` to
  its 3 inner helpers AND localizes the Catalog Summary cell field labels
  (`Title:` → `כותרת:`, `Author:` → `מחבר:`, `Date:` → `תאריך:`, `Place:` → `מקום:`).
- `build_bibliography_rows(sys_id, meta_resolver, lang='en')` threads `lang` through.
- Web caller: `web/export_service.py:export_search_results_excel` consumes
  the new bilingual helpers, calls `get_library_display(code, lang=lang)`.
- Desktop caller: `genizah_app.py:_build_search_results_xlsx_bytes` ditto.
  `export_results('xlsx')` builds the meta_resolver with
  `_row_lang = CURRENT_LANG`; `headers_main = main_header_row(CURRENT_LANG)`.

**Service inventory notes (for the parent CONTEXT integrity):**

- `pgp_translations` table EXISTS in the production `pgp_data/pgp.db` sidecar.
  The worktree's copy may be empty (test mocks the call path anyway).
- `FjmsService.get_bibliography` already returns Hebrew variants
  (`running_title_heb`, `article_author_heb`, `title_acronym_heb`) — no
  service-layer additions needed.
- `NliCrossrefService.get_catalog_entry` returns Neubauer-Cowley reference
  strings (numeric-with-prefix) that are language-neutral — no Hebrew
  accessor needed; the column passes through verbatim per the original D-08
  contract.
- `genizah_core.get_library_display(code, short=False, lang='he')` was
  already lang-aware via `LIBRARY_CODES_HE` — no core changes needed.

The MUST-FIX 94-04-B (English-locked desktop headers) and 94-04-E (English-locked
sheet title literal "Genizah Results") clauses are **SUPERSEDED** by this
revision. The cross-parity test (MUST-FIX 94-04-C) still passes because it
builds both apps' workbooks at the default `lang='en'` -> identical English
output on both sides.

### D-05 — Multi-value field formatting
List-valued cells use the pipe character `|` with NO surrounding spaces: `'Bible|Letter|Legal'`. Applies to: Domains, PGP Languages, PGP Tags. NOT applied to URL fields.

For JSON, lists stay as native arrays (no pipe-joining).

### D-06 — Missing data
Empty cells for missing values. Do NOT write `"N/A"`, `"—"`, `"None"`, or any placeholder. JSON: `is_printed` and `has_pgp` are always boolean (`true`/`false`, never `null`); `domains` is `[]` when none.

### D-07 — Per-row repetition (main sheet)
Multi-folio hits for the same manuscript repeat the per-row flag/value cells (`Has PGP`, `Is Printed`, `Domains`, `IIIF Manifest`) on every row. The Manuscripts sub-sheet dedupes — ONE row per unique sys_id — so the dossier data lives there exactly once.

### D-08 — Data sources & helpers (REVISED 2026-05-19 per Codex critique)

The shared module `shared/export_dossier.py` exposes (public API — no underscore prefix per Codex SHOULD-FIX 10):

**4 lookup helpers** (each exception-resilient per D-A2 — wraps service call in try/except, logs warning, returns `None` or `[]`):

1. **`pgp_subset_for_sys_id(sys_id: str) -> Optional[dict]`** — wraps `shared.document_service.get_document_for_fragment(sys_id)`. Returns `{pgp_url, description, document_type, date_display, languages, tags}` or `None`. Date uses fallback chain: `inferred_date_display → doc_date_standard → doc_date_original → None`. Languages: handle the comma-separated TEXT projection bug (split on comma, NOT iterate chars) — `_split_pgp_languages` helper.

2. **`nli_subset_for_sys_id(sys_id: str) -> Optional[dict]`** — wraps `shared.nli_crossref_service.NLICrossrefService.get_catalog_entry(sys_id)` + `.get_library_viewer_url(sys_id)`. Returns `{catalog_entry, library_viewer_url}` or `None`. Note: column is `NLI Catalog Entry` not `NLI Description` (Codex MUST-FIX 2 — `get_catalog_entry()` returns catalog reference strings, sometimes numeric, NOT descriptions).

3. **`catalog_summary_for_sys_id(sys_id: str) -> Optional[dict]`** — wraps `shared.fjms_service.FjmsService.get_catalog_records(sys_id)` (NOT `get_catalog_detail()` — per Codex MUST-FIX 3, the detail variant reads `full_texts` and risks D-02 violation + performance hit). Returns 3-5 narrow fields chosen by planner with documented rationale. Likely candidates: `Author`, `Title`, `Copy date`, `Comment` (planner verifies what `get_catalog_records()` actually exposes).

4. **`bibliography_for_sys_id(sys_id: str) -> List[dict]`** — wraps `shared.fjms_service.FjmsService.get_bibliography(sys_id)`. Returns list of bib entries with REAL FJMS field names per Codex MUST-FIX 1: `{running_title, title_year, mention_page, article_name, article_author_eng, catalog_acronym}`. Empty list when none.

**2 row-emitter functions** (return Python primitives only — no openpyxl objects):

5. **`build_manuscript_row(sys_id, meta_resolver, lang='en') -> List[Any]`** — returns the 14 cell values for one Manuscripts sub-sheet row, in fixed order. Calls helpers 1+2+3 (PGP, NLI, Catalog) — does NOT call helper 4 (bibliography is separate per Codex MUST-FIX 4). Manuscripts row order:

```
[System ID, Shelfmark, Library, Title, PGP URL, PGP Description,
 PGP Type, PGP Date, PGP Languages, PGP Tags, NLI Catalog Entry,
 Catalog Summary, Library Viewer URL, GenizahSearch URL]
```

6. **`build_bibliography_rows(sys_id, meta_resolver) -> List[List[Any]]`** — calls helper 4 only. Returns 0..N row tuples for one sys_id. Bibliography row order:

```
[System ID, Shelfmark, Article Author, Article Name, Running Title,
 Title Year, Mention Page, Catalog Acronym]
```

**2 header-list constants** (Codex SHOULD-FIX 7 — both apps consume the same constants, tests assert row length/order):

7. **`MANUSCRIPT_HEADERS: List[str]`** — 14 column names matching `build_manuscript_row` output.
8. **`BIBLIOGRAPHY_HEADERS: List[str]`** — 8 column names matching `build_bibliography_rows` output.

**Metadata resolver** (Codex SHOULD-FIX 8 — replaces opaque `meta_mgr` to prevent web/desktop drift): `meta_resolver` is a callable `sys_id -> Optional[dict]` returning `{shelfmark, title, library_code, library_name}` or `None`. Each app constructs its resolver from its own meta source (web: `meta_mgr.get_meta_for_id` + `meta_mgr.get_library_for_id`; desktop: same field names but different module). The shared module never reaches into a meta_mgr — only calls the resolver.

**Optional higher-level wrapper** (Codex SHOULD-FIX 5 — Claude's Discretion in Wave 1 planner): `build_dossier_rows(sys_ids, meta_resolver) -> {manuscripts: List[List], bibliography: List[List]}` convenience function that loops the row builders. Wave 1 planner decides whether to ship it now or defer.

### D-09 — Performance
Per-sys_id loops inside row builders are acceptable for ~50-200 unique sys_ids (Codex SHOULD-FIX 6 — SQLite point lookups cheap relative to Python orchestration). The dangerous call (`get_catalog_detail()` with full_texts) is replaced by `get_catalog_records()` per D-08 — eliminates the performance footgun. No new batch methods added to the existing service modules in this phase.

If lookup times exceed ~3 seconds for typical downloads in human smoke testing, the planner may add batched fetches in a follow-up plan; for now, per-sys_id is fine.

### D-10 — Out-of-scope confirmations
- Word export NOT touched (web `export_search_results_word`).
- List export NOT touched (`export_list_excel`).
- Parallels exports NOT touched (Excel + Word).
- Desktop CSV / TXT / DOCX exports NOT touched.
- Desktop composition-results export NOT touched (separate state `_comp_*`).
- Per-item `metadata` block in JSON NOT added (dossier stays xlsx-only).
- Parallels JSON envelope NOT touched (`serialize_parallels_payload` via `_to_parallels_envelope_item` does NOT inherit `has_pgp`/`is_printed` — D-10 regression test required).

### D-11 — JSON envelope stability (web only)
The JSON envelope's existing keys (`schema_version`, `source`, `query`, `mode`, `gap`, `filters`, `count`, `total`, `warnings`, `generated_at`, `results`, `request`) are UNCHANGED. The 3 new per-item keys (`is_printed`, `has_pgp`, `domains` if not already present) are ADDITIVE. Schema version stays 1 (additive change per Phase 83 stability commitment).

### D-12 — Manuscripts sub-sheet dedupe semantics
"Unique sys_id" means the distinct values of `result['display']['id']` (canonical for dedupe). Iterate the result set, build an ordered set of sys_ids, then emit one row per sys_id via `build_manuscript_row`. Order: first-occurrence order in the result list (preserves "found this first" feel).

### D-13 — IIIF Manifest column (soft scope)
If the planner DOES emit the IIIF Manifest column on the main sheet (column 12 in D-01), the value is the per-page IIIF URL via existing image-resolution logic (`nli_crossref_service.get_folio_images(sys_id)` returns canvas data; manifest URL reachable for NLI / Cambridge / Manchester / JTS / Oxford). Empty cell when not available.

If the planner DEFERS this column, document the rationale (likely "per-page IIIF resolution requires too much new plumbing for this phase; provide a manifest URL on the Manuscripts sub-sheet instead via `library_viewer_url`").

### D-14 — Rich-text snippet rendering (REVISED 2026-05-19, NEW)
Desktop's existing snippet rendering (`*` markers in `raw_file_hl` → red bold inline run via `openpyxl.cell.rich_text.TextBlock`, `CellRichText`, `openpyxl.cell.text.InlineFont`) at `genizah_app.py:17988-18030` is the canonical pattern. Web's main sheet `Snippet` column adopts the same rendering — extract the existing desktop helper (`write_rich_cell` inner function at `:18000`) into a small shared helper in `shared_export_utils` or `shared/export_dossier.py` (planner's call). Sub-sheets (Manuscripts, Bibliography) stay plain text — no rich-text in dossier columns.

### D-15 — Plan re-org strategy (REVISED 2026-05-19, NEW)
The 3 existing plans (`94-01`, `94-02`, `94-03`) are SUPERSEDED by this revision and will be renamed to `*.SUPERSEDED-v2.md`. The original `94-01-PLAN.SUPERSEDED.md` (from the 999.3 era) is preserved as `.SUPERSEDED-v1.md` for historical continuity.

**4-wave structure** (replanned from scratch):

1. **Wave 1** — `shared/export_dossier.py` module: 4 lookup helpers + 2 row-emitter functions + 2 header constants + `_split_pgp_languages` bug fix. Unit tests cover each helper independently (missing sidecars, comma-split languages, empty tags, service exceptions, no-transcription-text guarantee). Optional `build_dossier_rows` wrapper (Claude's Discretion).
2. **Wave 2** — Web state plumbing: `printed_ids` through `set_search_export(...)`; add `update_search_export_enrichment(...)`; thread `transcription_sys_ids`/`printed_ids` through `_serialize_item` for JSON `has_pgp`/`is_printed`; parallels D-10 regression test.
3. **Wave 3** — Web xlsx restructure: rewire `web/export_service.py:export_search_results_excel` to emit 3-sheet workbook, consume shared dossier helpers, switch to unified column order (D-01), extract `Image/Page` + `Source` from web result dict (planner identifies fields), extend rich-text snippet rendering to web (D-14), wire conditional RTL (D-04).
4. **Wave 4** — Desktop xlsx parity + verification + docs: rewire `genizah_app.py:export_results('xlsx')` to consume shared dossier helpers, emit same 3-sheet workbook, add `Full Text` + 4 new flag/URL columns. Human smoke verification on real xlsx (web + desktop) and JSON (web only). REQUIREMENTS.md traceability close. Milestone closeout docs.

### D-16 — Cross-AI review posture (NEW 2026-05-19)
Each of the 4 waves goes through `/gsd-review --phase 94 --all` (Gemini + Codex) BEFORE execution. MUST + SHOULD revisions applied via `/gsd-plan-phase 94 --reviews` in-place. Matches v7.12 milestone pattern. Wave 1 (shared dossier API) is the most load-bearing — extra-careful review.

If Gemini is quota-exhausted at review time (per `feedback_codex_during_discuss_phase.md`), proceed with Codex-only review and document the gap in the plan summary.

### Claude's Discretion
- Column widths for all new columns (planner sets via `set_excel_column_widths`).
- Exact FJMS catalog-records fields to surface in `Catalog Summary` (3-5 fields, rationale required; verify what `get_catalog_records()` exposes — likely a subset of the 37 catalog fields).
- Whether the optional `build_dossier_rows` higher-level wrapper ships in Wave 1 or is deferred.
- Whether `IIIF Manifest` lives on the main sheet (D-13 soft scope) or only on the Manuscripts sub-sheet.
- Whether URLs become clickable Excel hyperlinks vs plain text. Default: plain text for citation safety.
- Whether to escape pipe characters in tag/language values that legitimately contain them. Default: leave as-is.
- Sheet names: `Manuscripts` and `Bibliography` are locked (don't translate or abbreviate). Main sheet name stays `Genizah Results` on web, desktop uses `tr("Search Results")` (existing convention).
- Exact field on web result dict for `Image/Page` and `Source` columns — planner inspects `_serialize_item` and search-state to identify the closest analogs to desktop's `d.get('img', '')` and `d.get('source', '')`.
- Exact field on desktop result dict for `Full Text` — planner verifies what desktop's `_collect_sorted_results()` exposes (or whether a fresh fetch via meta_mgr/Tantivy is needed).
- Whether to extract the rich-text helper (`write_rich_cell`) into `shared_export_utils` or `shared/export_dossier.py`.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### v7.13 milestone planning artifacts
- `.planning/milestones/v7.13-ROADMAP.md` — milestone scope and success criteria, including the 2026-05-19 desktop-parity expansion (`EXPORT-META-09`).
- `.planning/REQUIREMENTS.md` — `EXPORT-META-01..09` with web/desktop scope annotations.
- `.planning/phases/94-adding-pgp-to-downloaded-data/94-CODEX-CRITIQUE.md` — Codex review of the shared-module API (4 MUST-FIX, 6 SHOULD-FIX, 2 OK) — all findings folded into D-01, D-02, D-08, D-14, D-15 above.

### Existing render sites (must be modified)
- `web/export_service.py:410 export_search_results_excel` — restructured into multi-sheet builder. Switch to unified column order per D-01. Wire shared dossier helpers. Add rich-text snippet rendering. Wire conditional RTL.
- `web/export_service.py:55 _resolve_result_full_text` — kept; D-02 grandfathers `Full Text` column.
- `shared/search_serializer.py:298 _serialize_item` — add `is_printed` and `has_pgp` keys (verify `domains` already at `:315`).
- `web/export_state.py:set_search_export` — accept 3 new kwargs (`transcription_sys_ids`, `printed_ids`, `result_domains`); add `update_search_export_enrichment` helper.
- `web/pages/search.py` — 3 `set_search_export` call sites + 1 `update_search_export_enrichment` call site at the enrichment block (~line 4494-4496).
- `web/api.py:2021 export_excel`, `:2164 export_json` — pass new payload kwargs through.
- `genizah_app.py:17895 def export_results(self, fmt='xlsx')` — desktop entry point. Restructure xlsx branch (`fmt == 'xlsx'` block from `:17983`) to emit 3-sheet workbook via shared dossier.

### Existing services (must reuse — NEVER add new transcription-text exposure)
- `shared/document_service.py:950 get_document_for_fragment(sys_id, page_num=None)` — PGP lookup. The new `pgp_subset_for_sys_id` helper wraps this.
- `shared/nli_crossref_service.py:727 get_catalog_entry`, `:448 get_library_viewer_url`, `:255 get_folio_images` — NLI sources. New `nli_subset_for_sys_id` helper wraps the first two.
- `shared/fjms_service.py:get_catalog_records` — NEW dependency for catalog summary (NOT `get_catalog_detail` per Codex MUST-FIX 3). Planner verifies the exact method signature and what fields it returns.
- `shared/fjms_service.py:2531 get_bibliography(sys_id)` — bibliography lookup; new `bibliography_for_sys_id` helper wraps it. REAL field names: `running_title`, `title_year`, `mention_page`, `article_name`, `article_author_eng`, `catalog_acronym` (Codex MUST-FIX 1).
- `shared/fjms_service.py:866 get_domains_for_sys_ids` — already-batched; consumed by search-state `result_domains` (not by the new dossier module).
- `genizah_core.get_library_display(library_code, short=False, lang='en')` — library name resolution.

### State to plumb through

**Web (via `web/export_state.py`):**
- `web/pages/search_state.py:43 transcription_sys_ids: Set[str]` — Has PGP signal.
- `web/pages/search_state.py:54 printed_ids: set` — Is Printed signal. Currently NOT on the export payload — Wave 2 plumbs it.
- `web/pages/search_state.py:46 result_domains: dict` — Domains list per sys_id.

**Desktop (read directly from state machine):**
- `genizah_app.py:2547 self._pgp_transcription_sys_ids` — Has PGP signal.
- `genizah_app.py:2550 self._printed_sys_ids` — Is Printed signal.
- `genizah_app.py:5461 self._result_domain_map` — sys_id → list of domain names.

### Existing export entry points (DO NOT modify in this phase)
- `web/api.py:export_word` — out of scope; confirm zero diff.
- `web/api.py:export_json` for parallels — `serialize_parallels_payload` does NOT inherit `has_pgp`/`is_printed`; D-10 regression test required.

### Workbook structure helpers
- `web/export_service.py:283 create_excel_workbook(sheet_name, rtl_sheet=True)` — wire conditional RTL via lang.
- `web/export_service.py:299 style_excel_header`, `:313 set_excel_column_widths`, `:319 get_cell_alignment`, `:329 add_excel_credits` — reuse.
- `web/export_service.py:sanitize_text_for_excel` — handles overlong / control-char text.
- Rich-text helper (`write_rich_cell` currently inner function at `genizah_app.py:18000`) — extract per D-14.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `_build_pgp_subset` at `shared/search_serializer.py:473` — 10-key PGP projection (still used by browse; do not modify).
- `_pgp_sync` at `shared/browse_service.py:155-182` — same projection template (still used by browse JSON; do not modify).
- `transcription_sys_ids`, `printed_ids`, `result_domains` — per-search signals already populated in web search_state and in desktop's state machine. Web plumbing into the export payload is part of this phase (Wave 2); desktop reads directly at the call site.
- NLI / FJMS service methods are stateless, side-effect-free, and safe to call per-sys_id during export.
- Desktop's rich-text snippet rendering (`write_rich_cell` at `genizah_app.py:18000`) is the canonical pattern that web's main sheet adopts.

### Established Patterns
- Excel exports use `wb, ws = create_excel_workbook(...)` returning workbook + active sheet. Add additional sheets via `wb.create_sheet(title='Manuscripts')` and `wb.create_sheet(title='Bibliography')`.
- The shared `sanitize_text_for_excel` helper handles overlong / control-char-laden text safely.
- Boolean fields render as `"Yes"` / empty cell (NOT `"True"`/`"False"`) — match this convention for `Has PGP` and `Is Printed`.
- Service modules expose stateless query methods per sys_id (point lookups, indexed); per-sys_id loops are the established pattern (Codex SHOULD-FIX 6 validates this for 50-200 ids).

### Integration Points
- ONE main entry on each app: `export_search_results_excel` (web) and `export_results('xlsx')` (desktop). After restructuring, both return the same `(bytes, filename)` tuple (or write to a user-chosen file path on desktop) — no endpoint signature changes.
- TWO web-side state-plumbing changes: `printed_ids` must flow through `set_search_export(...)` to the export payload; `update_search_export_enrichment(...)` patches enrichment fields post-search.
- ZERO desktop-side state-plumbing changes: desktop reads `_pgp_transcription_sys_ids`, `_printed_sys_ids`, `_result_domain_map` directly at the export call site.

### Design Cleavage (Codex OK findings)
- RTL/openpyxl styling stays OUTSIDE `shared/export_dossier.py`. Each app sets `ws.sheet_view.rightToLeft` from its own UI lang. The main sheet is per-folio-hit (not deduped to sys_id), so it stays app-specific; only the Manuscripts + Bibliography sub-sheets are built from shared row data.
- Opinionated leaf helpers (return narrow shape, not raw service dicts) are correct. Each lookup helper has an explicit, narrow return contract covered by tests.

</code_context>

<specifics>
## Specific Ideas

- User explicitly wants TWO new sub-sheets (Manuscripts + Bibliography), not three.
- User explicitly moved the full PGP fields OFF the main sheet (which was the prior plan) — main sheet now only has the `Has PGP` Yes/No flag plus other booleans/URLs.
- User noted "IIIF is per image and may be in the main sheet instead" — interpretation: IIIF URL belongs on the main sheet rows (because each row is a specific page/folio), NOT in the Manuscripts dossier. Soft scope per D-13.
- User explicitly chose "Two sub-sheets" granularity (Manuscripts + Bibliography), confirming the join-by-sys_id pattern.
- JSON stays minimal — only 3 per-item keys gained. No dossier in JSON.
- User explicitly chose to UNIFY both apps' main-sheet column order (REVISED 2026-05-19), dropping web's `Score` column (empty in practice) and accepting that web reshuffles to match desktop's column order. Both apps end up with 12 identical columns.
- User explicitly chose to extend desktop's rich-text snippet rendering to web's main sheet too (REVISED 2026-05-19).
- User explicitly chose conditional RTL based on UI lang at export time (REVISED 2026-05-19) — Hebrew UI → RTL all 3 sheets, English UI → LTR all 3 sheets.
- User delegated the shared-module API design to Codex critique (2026-05-19) — Codex's 4 MUST-FIX + 6 SHOULD-FIX findings are all folded into D-08, D-01, D-02, D-14, D-15.

</specifics>

<deferred>
## Deferred Ideas

- **Per-item `metadata` block in JSON** — surfaces the full dossier in JSON. Useful for research scripts but adds substantial envelope size. Soft-rejected this phase.
- **Word export with the same metadata** — Word's layout primitives are paragraphs, not sheets. Significant redesign.
- **List export with metadata** — `export_list_excel` at `:413`. Same shape could apply; planner can mirror after this phase ships.
- **Parallels export with metadata** — same shape, parallels-specific row structure.
- **Desktop CSV / TXT / DOCX exports** — out of scope; only xlsx gets the dossier treatment.
- **Desktop composition-results export** — separate state (`_comp_*`); out of scope.
- **PGP transcription text in NEW dossier surfaces** — still excluded for size. Existing main-sheet `Full Text` is grandfathered (D-02).
- **Hebrew translations of metadata** — pgp.db has 34,954 EN→HE PGP descriptions; NLI has Hebrew variants; FJMS has translated catalog fields. Could be an opt-in "translated" export mode in a future phase.
- **PGP source / scholar attribution** (Goitein, V0.8, etc.) — beyond the basic record.
- **Excel hyperlink rendering** for URL columns — plain text default, upgrade later if cheap.
- **CSV export** — different concern; multi-sheet doesn't map to CSV.
- **Visual similarity partners column** — `vs_availability` is on the search state; could be a future addition.
- **FJMS join data** — `get_joins(sys_id)` could populate a third sub-sheet for puzzle-relevant manuscripts.
- **Batch fetches in shared/export_dossier.py** — Codex SHOULD-FIX 5 noted the API should not block future batching/caching. Wave 1 ships per-sys_id loops; a follow-up phase could add prefetch-map support if smoke testing reveals latency issues.

</deferred>

---

*Phase: 94-adding-pgp-to-downloaded-data (renamed from 999.3-adding-pgp-to-downloaded-data on 2026-05-19; slug preserved for git-history continuity)*
*Context gathered: 2026-05-15*
*Context broadened: 2026-05-17 — added Manuscripts + Bibliography sub-sheets*
*Context revised: 2026-05-19 — renumbered 999.3 → 94, requirement rename METADATA-EXPORT → EXPORT-META, desktop-parity scope added (EXPORT-META-09), unified column order, conditional RTL, rich-text web extension, shared module API per Codex critique*
