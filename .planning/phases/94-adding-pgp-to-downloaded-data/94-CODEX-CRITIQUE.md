# Codex Critique — Phase 94 `shared/export_dossier.py` API

**Date:** 2026-05-19
**Trigger:** User delegated technical sub-decision ("Take your recommendations and ask Codex") on Area 1 of Phase 94 CONTEXT.md revision.
**Brief:** `94-CODEX-BRIEF.md` (Q1-Q5 on shared-module shape + Area 2/3 decision context).
**Reviewer:** codex-cli 0.130.0, sandbox read-only.
**Tokens used:** 128,025.

## Findings

### MUST-FIX

1. **Bibliography schema mismatch**: The proposed Bibliography row schema (`Author, Title, Publisher, Year, Page Reference, Source Name`) does not match `FjmsService.get_bibliography()`. That service returns `running_title`, `title_year`, `mention_page`, `article_name`, `article_author_eng`, `catalog_acronym` — no `publisher`, no `source_name`. Either change the sheet columns or define an explicit mapping from real service fields.

2. **"NLI Description" mislabeling**: `NLICrossrefService.get_catalog_entry()` returns catalog-entry/reference strings, sometimes numeric-only, not descriptions. Use a column/helper name like `NLI Catalog Entry` unless we have another actual description source.

3. **`catalog_summary_for_sys_id` MUST NOT use `get_catalog_detail()`**: That call includes `full_texts`, which is both performance-heavy and too close to the D-02 "no new transcription/full-text dossier surface" boundary. Prefer a narrow summary from `get_catalog_records()` or another deliberately narrow query.

4. **`build_manuscript_row` calls bibliography**: If literally calling all 4 lookup helpers, remove the bibliography call from the manuscript row builder. Manuscript rows do not need bibliography, and 50-200 wasted bibliography queries are avoidable.

### SHOULD-FIX

5. **API extensibility**: Don't make per-row builders the only API that performs service I/O. Keep leaf helpers, but add a higher-level `build_dossier_rows(sys_ids, meta_resolver, services=None)` or accept optional prefetch maps. Per-id SQLite is acceptable for now, but the API should not block future batching/caching.

6. **Performance baseline**: SQLite point lookups are probably cheap relative to Python/service orchestration for 50-200 ids, assuming persistent singleton connections and indexed columns. The exception is `get_catalog_detail()` (multiple queries per id + full-text reads). Avoid in export path. (Subsumed by MUST-FIX 3.)

7. **Shared header constants**: Expose `MANUSCRIPT_HEADERS` and `BIBLIOGRAPHY_HEADERS` as module-level constants. Returning bare positional lists is fine only if both apps consume the same constants and tests assert row length/order.

8. **Metadata resolver, not `meta_mgr`**: For strict cross-app parity, prefer a shared metadata resolver or precomputed primitive map: `sys_id -> {shelfmark, title, library_code, library_name}`. Otherwise desktop/web fallback behavior can drift silently.

9. **English-only contract on row builders**: Keep export content English-only. UI language should control only `sheet_view.rightToLeft`, not library/title/header localization. If `lang` remains on row builders, document that it must not trigger translation/localized content.

10. **Underscore-prefixed helpers are private by Python convention**: If CONTEXT.md treats them as module API, rename without `_` (`pgp_subset_for_sys_id` not `_pgp_subset_for_sys_id`).

### OK (validated, no change)

11. Keeping RTL/openpyxl styling outside `shared/export_dossier.py` is the right cleavage point. The main sheet remains app-specific (per-folio-hit rows are not deduped to sys_id), so a shared `write_workbook()` would over-centralize too early.

12. Opinionated leaf helpers (return narrow shape, not raw service dicts) are correct. Make the opinion explicit, narrow, and covered by tests: missing sidecars, comma-split languages, empty tags, service exceptions.

## Disposition

All 4 MUST-FIX findings folded into CONTEXT.md D-A1 (Module scope), D-01c (Manuscripts columns), D-01d (Bibliography columns), and D-08 (Data sources & helpers).

All 6 SHOULD-FIX findings folded into CONTEXT.md as locked decisions (no further deferral). The `build_dossier_rows` higher-level wrapper (SHOULD-FIX 5) is added as Claude's Discretion — Wave 1 planner decides whether to ship it now or defer.

The 2 OK findings are noted as design-cleavage rationale in `<code_context>`.

## Hand-off to planning

Wave 1 of the re-planned Phase 94 (shared dossier module) must:
- Use real FJMS bib field names (running_title, title_year, mention_page, article_name, article_author_eng, catalog_acronym) on the Bibliography sub-sheet.
- Use `get_catalog_records()` (narrow query, no full_texts) for catalog summary. Verify the schema and pick 3-5 fields with documented rationale.
- Rename the Manuscripts column "NLI Description" → "NLI Catalog Entry" to match what `get_catalog_entry()` actually returns.
- Public module API: 4 lookup helpers (no underscore prefix) + 2 row-emitter functions + 2 header-list constants (`MANUSCRIPT_HEADERS`, `BIBLIOGRAPHY_HEADERS`).
- `build_manuscript_row` calls 3 helpers (PGP, NLI, Catalog) — NOT 4. Bibliography is separate via `build_bibliography_rows`.
- Row builders accept a `meta_resolver` callable OR a precomputed primitive map (`sys_id -> {shelfmark, title, library_code, library_name}`) — NOT an opaque `meta_mgr` object. Documents that `lang` parameter is ONLY for downstream RTL view, never for content translation.
