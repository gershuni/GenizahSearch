# Phase 999.3: Adding metadata to downloaded data - Context

**Phase name (CONTEXT):** "Adding metadata to downloaded data" (broadened 2026-05-17 from the original "Adding PGP to downloaded data"). Phase number stays `999.3`; the slug and directory name retain the original `999.3-adding-pgp-to-downloaded-data` for commit-history continuity.

**Gathered:** 2026-05-15
**Last revised:** 2026-05-17 — scope significantly broadened: PGP is now ONE of several metadata sources, and the xlsx gains two new sub-sheets (Manuscripts dossier + Bibliography). JSON stays minimal.
**Status:** Ready for re-planning — the prior `999.3-01-PLAN.md` (6-PGP-columns-on-main-sheet + JSON `pgp` subobject) is SUPERSEDED by this revision.

<domain>
## Phase Boundary

Extend the search-results downloads (xlsx + JSON) with research-grade
metadata so a downloaded artifact stands alone as a citation source.

**Xlsx gains 3 surfaces:**
1. The existing search-results sheet gains a small set of per-row flag/value columns (`Has PGP`, `Is Printed`, `Domains`, optional per-page `IIIF Manifest`).
2. A NEW `Manuscripts` sub-sheet: ONE row per unique sys_id observed in the result set. Dossier-style — PGP fields, NLI descriptions, catalog summary, library viewer URL, GenizahSearch internal URL.
3. A NEW `Bibliography` sub-sheet: ONE row per bibliography entry, joinable to `Manuscripts` via `System ID`.

**JSON stays minimal:** per-item gains `is_printed` (bool), `domains` (list), `has_pgp` (bool). NO PGP subobject, NO dossier metadata. The JSON envelope keeps its small, stable public shape — research scripts that want dossier data either download the xlsx or call `/api/browse` per sys_id.

In scope:
- `web/export_service.py:286 export_search_results_excel` — restructured to build a multi-sheet workbook.
- New helper module(s) producing the Manuscripts dossier and Bibliography rows.
- `shared/search_serializer.py:_serialize_item` — add 3 keys (`is_printed`, `domains`, `has_pgp`) to the per-item dict. (`domains` is already computed; `is_printed` needs new plumbing from `printed_ids` to the export payload; `has_pgp` is derivable from `transcription_sys_ids`.)
- A shared PGP-projection helper (the one already specified in the prior plan), now consumed only by the Manuscripts sub-sheet (NOT the main sheet).

Out of scope (see Deferred Ideas):
- Word search-results export.
- List exports.
- Parallels exports.
- PGP transcription text in any export.
- Translating PGP / NLI / catalog descriptions to Hebrew.
- A per-item `metadata` block in JSON (dossier shape stays xlsx-only).

</domain>

<decisions>
## Implementation Decisions

### D-01a — Main search-results sheet: new per-row flag/value columns

Append to the existing Excel main sheet AFTER the `Full Text` column. Columns:

| Column | Source | Notes |
|---|---|---|
| Has PGP | `result['display']['id'] in search_state.transcription_sys_ids` | "Yes" / "" (empty cell when False) — D-06 |
| Is Printed | `result['display']['id'] in search_state.printed_ids` | "Yes" / "" |
| Domains | `search_state.result_domains[sys_id]` | Pipe-delimited (D-05): e.g. `Bible\|Letter\|Legal` |
| IIIF Manifest | per-page IIIF URL when available (NLI / Cambridge / Manchester / JTS / Oxford via the same library-viewer logic) | URL or empty cell. May be SKIPPED in planning if planner finds the per-page resolution too costly; defer to Manuscripts sub-sheet instead. Claude's Discretion. |

Final main-sheet column order:
```
Shelfmark | Library | Title | System ID | Score | Snippet | Full Text |
Has PGP | Is Printed | Domains | IIIF Manifest
```

**Removed from main sheet (vs prior plan):** PGP URL, PGP Description, PGP Type, PGP Date, PGP Languages, PGP Tags. These move to the Manuscripts sub-sheet per user direction.

### D-01b — JSON per-item additions

Add 3 keys to `_serialize_item`'s return dict:
- `is_printed`: bool
- `domains`: list (already computed inside the function; expose explicitly — currently rendered into a `domains` key that is fine to keep)
- `has_pgp`: bool

NO `pgp` subobject. The dossier metadata is xlsx-only per user direction.

If `'domains'` is already present in `_serialize_item` output (verify during planning — `:315 'domains': domains,` looks like it already is), then only `is_printed` and `has_pgp` are net-new.

### D-01c — NEW `Manuscripts` sub-sheet (xlsx-only)

ONE row per unique sys_id observed in the search result set (deduped). Suggested columns:

| Column | Source |
|---|---|
| System ID | `display.id` |
| Shelfmark | `display.shelfmark` (canonical) |
| Library | full library name (not code) — use `core_get_library_display(library_code, short=False, lang='en')` |
| Title | `display.title` |
| PGP URL | `pgp_url` (or empty if no PGP record) |
| PGP Description | `description` |
| PGP Type | `document_type` |
| PGP Date | `inferred_date_display → doc_date_standard → doc_date_original` fallback chain |
| PGP Languages | `languages_primary` + `languages_secondary`, pipe-delimited |
| PGP Tags | `tags`, pipe-delimited |
| NLI Description | `nli_crossref_service.get_catalog_entry(sys_id)` or equivalent text field on the NLI side. Concatenate / pick the most useful free-text NLI catalog description if multiple exist |
| Catalog Summary | A short summary of FJMS catalog data — Claude's Discretion which fields to surface. Candidates: `Author`, `Title`, `Copy date`, `Comment` from `fjms_service.get_catalog_detail(sys_id)` (37 fields available; pick a useful subset of ~3-5 with explicit rationale in the plan) |
| Library Viewer URL | `nli_crossref_service.get_library_viewer_url(sys_id)` per library_code |
| GenizahSearch URL | `https://genizahsearch.com/browse?sys_id={sys_id}` (deep link back) |

**Empty cells** for missing data (D-06). **Pipe-delimited** for list-valued cells (D-05). **English only** (D-04).

### D-01d — NEW `Bibliography` sub-sheet (xlsx-only)

ONE row per bibliography entry — joinable to `Manuscripts` via `System ID`. Pulled from `fjms_service.get_bibliography(sys_id)` for each unique sys_id in the result set.

Suggested columns (planner picks the final list after inspecting the FJMS bib schema):

| Column | Source |
|---|---|
| System ID | foreign key, joins to Manuscripts |
| Shelfmark | for readability (denormalized) |
| Author | FJMS bib author field |
| Title | FJMS bib title field |
| Publisher | FJMS bib publisher field |
| Year | FJMS bib year field |
| Page Reference | FJMS bib reference / pages field |
| Source Name | which FJMS bib catalog this entry comes from |

**Ordering:** Group by `System ID`, then by whatever natural order FJMS already returns (creation date or alphabetical — planner verifies). One row per entry, no caps. Some manuscripts may produce dozens of bib rows; that's intentional.

### D-02 — No transcription text
Unchanged from prior CONTEXT.md: do NOT include PGP transcription text (`page_section_text`) in any export surface (main sheet, Manuscripts dossier, JSON, Bibliography). Transcription text remains available via `/api/browse`.

### D-03 — Sheet ordering & default-active sheet
Sheet order in the workbook: `Genizah Results` (existing, with new columns) → `Manuscripts` → `Bibliography`. The first sheet remains the default-active sheet on open so existing users see no change in initial-load behavior.

### D-04 — Language
**Always emit metadata in English.** PGP is canonically English; NLI descriptions are English (plus Hebrew on some entries — surface the English one); FJMS catalog/bib data is mixed but the canonical/scholarly form is English. NO `get_language()`, NO translation lookups, NO `shared/translation_service.py` calls in the export path.

When a field has both English and Hebrew variants in the source DB (some NLI fields, some FJMS fields), pick the English one. If only Hebrew exists, emit the Hebrew text rather than empty (graceful degradation).

### D-05 — Multi-value field formatting
List-valued cells use the pipe character `|` with NO surrounding spaces: `'Bible|Letter|Legal'`. Applies to: Domains, PGP Languages, PGP Tags. NOT applied to URL fields (no list URLs in this phase).

For JSON, lists stay as native arrays (no pipe-joining).

### D-06 — Missing data
Empty cells for missing values. Do NOT write `"N/A"`, `"—"`, `"None"`, or any placeholder. JSON uses `null` for missing booleans? — NO: `is_printed` and `has_pgp` are always boolean (`true`/`false`, never `null`). `domains` is `[]` when none.

### D-07 — Per-row repetition (main sheet)
Multi-folio hits for the same manuscript repeat the per-row flag/value cells (`Has PGP`, `Is Printed`, `Domains`, `IIIF Manifest`) on every row. The Manuscripts sub-sheet dedupes — ONE row per unique sys_id — so the dossier data lives there exactly once.

### D-08 — Data sources & helpers (REVISED 2026-05-17)
The phase now requires multiple lookup helpers:

1. **PGP projection** — As specified in the prior CONTEXT.md: a shared `_pgp_subset_for_sys_id(sys_id, *, available_sys_ids=None)` helper in `shared/search_serializer.py` wrapping `get_document_for_fragment`. Consumed by the Manuscripts sub-sheet only (NOT the main sheet — main sheet just gets the boolean `Has PGP` flag).
2. **NLI projection** — Use `shared/nli_crossref_service.py:get_catalog_entry(sys_id)` and `get_library_viewer_url(sys_id)`. Wrap in a small helper `_nli_subset_for_sys_id(sys_id)` that returns `{description, library_viewer_url}` or None.
3. **Catalog projection** — Use `shared/fjms_service.py:get_catalog_detail(sys_id)`. Wrap as `_catalog_summary_for_sys_id(sys_id)` returning a short dict — planner picks the 3-5 most useful fields with rationale.
4. **Bibliography projection** — Use `shared/fjms_service.py:get_bibliography(sys_id)` returning a list of bib entries; one row per entry on the Bibliography sub-sheet.

Each helper returns a small dict (or None / empty list) — no shared base class. Each is independently testable.

### D-09 — Performance
The Manuscripts and Bibliography sub-sheets potentially do 4 lookups per unique sys_id (PGP, NLI, catalog, bib). The result set's `selected_uids` may contain ~50-200 results, but unique sys_ids will usually be smaller. Acceptable cost for an explicit user-triggered download.

If lookup times exceed ~3 seconds for typical downloads, consider batch fetches (e.g., `get_catalog_for_sys_ids` if FJMS service exposes one). Otherwise per-sys_id calls are fine.

### D-10 — Out-of-scope confirmations
- Word export NOT touched.
- List export NOT touched.
- Parallels exports NOT touched.
- Per-item `metadata` block in JSON NOT added (dossier stays xlsx-only).
- IIIF manifest in main sheet is SOFT scope — planner may defer to Manuscripts sub-sheet if per-page resolution is non-trivial. Document the choice in the plan.

### D-11 — JSON envelope stability
The JSON envelope's existing keys (`schema_version`, `source`, `query`, `mode`, `gap`, `filters`, `count`, `total`, `warnings`, `generated_at`, `results`, `request`) are UNCHANGED. The 3 new per-item keys (`is_printed`, `has_pgp`, `domains` if not already present) are ADDITIVE. Schema version stays 1 (additive change per existing Phase 83 stability commitment).

### D-12 — Manuscripts sub-sheet dedupe semantics
"Unique sys_id" means the distinct values of `result['display']['id']` (or `result['display']['uid']` if uid is preferred — verify which is canonical for dedupe purposes). Iterate the result set, build an ordered set of sys_ids, then emit one row per sys_id. Order: first-occurrence order in the result list (preserves "found this first" feel).

### D-13 — IIIF Manifest column (optional)
If the planner DOES emit the IIIF Manifest column on the main sheet, the value is the per-page IIIF URL via existing image-resolution logic (`nli_crossref_service.get_folio_images(sys_id)` returns canvas data; the manifest URL is reachable from there for libraries with IIIF support — NLI, Cambridge, Manchester, JTS). Empty cell when not available.

If the planner DEFERS this column, document the rationale (likely "per-page IIIF resolution requires too much new plumbing for this phase; provide a manifest URL on the Manuscripts sub-sheet instead").

### Claude's Discretion
- Column widths for all new columns (planner sets via `set_excel_column_widths`).
- Exact FJMS catalog fields to surface in `Catalog Summary` (3-5 fields, rationale required).
- Exact FJMS bib fields to surface on the Bibliography sub-sheet (planner reads schema, picks).
- Whether `IIIF Manifest` lives on the main sheet (D-13 soft scope) or only on the Manuscripts sub-sheet.
- Whether URLs become clickable Excel hyperlinks vs plain text. Default: plain text for citation safety.
- Whether to escape pipe characters in tag/language values that legitimately contain them. Default: leave as-is.
- Naming for the new sub-sheets: `Manuscripts` and `Bibliography` are the locked names (don't translate or abbreviate).

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing render sites (must be modified)
- `web/export_service.py:286-355` — `export_search_results_excel`. Restructured into multi-sheet builder. New helper functions added at module scope or in a sibling module (planner's call).
- `shared/search_serializer.py:298-322` — `_serialize_item` return dict. Add `is_printed` and `has_pgp` keys (verify `domains` already exists at `:315`).

### Existing services (must reuse)
- `shared/document_service.py:950` — `get_document_for_fragment(sys_id, page_num=None)` — PGP lookup.
- `shared/nli_crossref_service.py:727 get_catalog_entry`, `:448 get_library_viewer_url`, `:255 get_folio_images` — NLI sources.
- `shared/fjms_service.py:2389 get_catalog`, `:2435 get_catalog_records`, `:2531 get_bibliography`, `:2707 get_catalog_detail`, `:866 get_domains_for_sys_ids` — FJMS sources.
- `genizah_core.get_library_display(library_code, short=False, lang='en')` — library name resolution.

### State to plumb through
- `web/pages/search_state.py:43 transcription_sys_ids: Set[str]` — Has PGP signal.
- `web/pages/search_state.py:54 printed_ids: set` — Is Printed signal.
- `web/pages/search_state.py:46 result_domains: dict` — Domains list per sys_id.

Both `transcription_sys_ids` and `printed_ids` must be passed to the export payload via `web/export_state.py:set_search_export(...)` so the export pipeline has access. Currently `printed_ids` may NOT be on the export payload — verify and plumb if missing.

### Existing export entry points (DO NOT modify in this phase)
- `web/api.py:2021 export_excel` — Excel endpoint; calls into the service layer.
- `web/api.py:2063 export_word` — Word export (out of scope — confirm zero diff).
- `web/api.py:2164-2213 export_json` — JSON endpoint; calls `serialize_search_payload`. The new per-item keys flow here via `_serialize_item`.

### Workbook structure helpers
- `shared_export_utils` — `create_excel_workbook`, `style_excel_header`, `set_excel_column_widths`, `sanitize_text_for_excel`, `get_cell_alignment`, `add_excel_credits`. Multi-sheet support is already present in openpyxl (`wb.create_sheet(title='Manuscripts')`).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `_build_pgp_subset` at `shared/search_serializer.py:473` — 10-key PGP projection (still used by browse; do not modify).
- `_pgp_sync` at `shared/browse_service.py:155-182` — same projection template (still used by browse JSON; do not modify).
- `transcription_sys_ids`, `printed_ids`, `result_domains` — per-search signals already populated in search_state. Plumbing into the export payload is part of this phase.
- NLI / FJMS service methods are stateless, side-effect-free, and safe to call per-sys_id during export.

### Established Patterns
- Excel exports use `wb, ws = create_excel_workbook(...)` returning workbook + active sheet. Add additional sheets via `wb.create_sheet(title='Manuscripts')` and `wb.create_sheet(title='Bibliography')`.
- The shared `sanitize_text_for_excel` helper handles overlong / control-char-laden text safely.
- Boolean fields are typically rendered as `"Yes"` / empty (NOT `"True"`/`"False"`) — match this convention for `Has PGP` and `Is Printed`.

### Integration Points
- ONE main entry: `export_search_results_excel`. After restructuring, it returns the same `(bytes, filename)` tuple — no endpoint signature changes.
- TWO new state-plumbing changes: `printed_ids` must flow through `set_search_export(...)` to the export payload; the export pipeline reads it alongside `transcription_sys_ids`.

</code_context>

<specifics>
## Specific Ideas

- User explicitly wants TWO new sub-sheets (Manuscripts + Bibliography), not three.
- User explicitly moved the full PGP fields OFF the main sheet (which was the prior plan) — main sheet now only has the `Has PGP` Yes/No flag.
- User noted "IIIF is per image and may be in the main sheet instead" — interpretation: IIIF URL belongs on the main sheet rows (because each row is a specific page/folio), NOT in the Manuscripts dossier. Soft scope per D-13.
- User explicitly chose "Two sub-sheets" granularity (Manuscripts + Bibliography), confirming the join-by-sys_id pattern.
- JSON stays minimal — only 3 per-item keys gained. No dossier in JSON.

</specifics>

<deferred>
## Deferred Ideas

- **Per-item `metadata` block in JSON** — surfaces the full dossier in JSON. Useful for research scripts but adds substantial envelope size. Soft-rejected this phase.
- **Word export with the same metadata** — Word's layout primitives are paragraphs, not sheets. Significant redesign.
- **List export with metadata** — `export_list_excel` at `:413`. Same shape could apply; planner can mirror after this phase ships.
- **Parallels export with metadata** — same shape, parallels-specific row structure.
- **PGP transcription text** — `page_section_text`. Still excluded for size.
- **Hebrew translations of metadata** — pgp.db has 34,954 EN→HE PGP descriptions; NLI has Hebrew variants; FJMS has translated catalog fields. Could be an opt-in "translated" export mode in a future phase.
- **PGP source / scholar attribution** (Goitein, V0.8, etc.) — beyond the basic record.
- **Excel hyperlink rendering** for URL columns — plain text default, upgrade later if cheap.
- **CSV export** — different concern; multi-sheet doesn't map to CSV.
- **Visual similarity partners column** — `vs_availability` is on the search state; could be a future addition.
- **FJMS join data** — `get_joins(sys_id)` could populate a third sub-sheet for puzzle-relevant manuscripts.

</deferred>

---

*Phase: 999.3-adding-pgp-to-downloaded-data (slug preserved; renamed in CONTEXT)*
*Context gathered: 2026-05-15*
*Context broadened: 2026-05-17 — added Manuscripts + Bibliography sub-sheets, scope now beyond PGP*
