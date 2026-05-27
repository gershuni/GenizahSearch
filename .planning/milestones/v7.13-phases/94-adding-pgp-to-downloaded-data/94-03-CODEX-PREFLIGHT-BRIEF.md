# Codex Pre-Flight Brief — Phase 94 Wave 3 (web xlsx restructure)

You are doing **integration archaeology** on Wave 3 of GenizahSearch Phase 94 *before* it executes. Scope: Wave 3 only (`web/export_service.py:export_search_results_excel` rewrite, 3-sheet workbook emission, unified 12-column main sheet, conditional RTL, rich-text snippet on main sheet).

Plan: `.planning/phases/94-adding-pgp-to-downloaded-data/94-03-PLAN.md`.
Context: `94-CONTEXT.md` (D-01, D-04, D-13, D-14).
Live code: `web/export_service.py`, `web/export_state.py`, `web/safe_storage.py`, `shared/export_dossier.py` (Wave 1 deliverable; assume the planned public API).

The earlier bilateral review (`94-REVIEWS.md`) raised these HIGH/MEDIUM concerns on Wave 3:
- (a) **HIGH:** Compacted rows lack `display.img` and `display.source`. The planned `Image/Page` and `Source` columns will be empty.
- (b) **MEDIUM:** `search_terms = extract_search_terms(search_query)` becomes unused in the rewrite → Ruff F841.
- (c) **MEDIUM:** Sheet name `"Genizah Results"` (web) vs `"Search Results"` (desktop) weakens "identical structure" — confirm whether Wave 3 locks the English name on web.

Key signal you may have missed: `_resolve_result_display` at `web/export_service.py:76-180` ALREADY rehydrates `shelfmark` / `title` / `library_code` / `library_name` from compacted rows via meta_mgr (SEED-002 fixup, 2026-05-19), and `_resolve_result_full_text` at `:55-73` rehydrates `full_text` via `web.state.searcher.get_full_text_by_id(uid)`. But neither rehydrates `img` or `source`. So concern (a) is real for those two columns specifically.

## What to verify

Read the real code first, then answer each question as **PROBLEM** / **OK** / **UNCERTAIN** / **STALE**. Cite file:line evidence.

### Q1 — `Image/Page` source field

The plan reads `Image/Page` from `result['display'].get('img', '')` on web. After compaction, `display` is dropped and `img` is NOT in `_compact_search_result_row`'s allowlist (`web/export_state.py:_SEARCH_ROW_ALLOWLIST` — find it). What is the actual best path?
- Option A: extend the allowlist to preserve `display.img` and `display.source` directly.
- Option B: rehydrate from `raw_header` parse — does `raw_header` contain the page/folio label parseably?
- Option C: derive from `uid` / `parse_full_id_components`.
- Option D: read from a separate state field that is NOT compacted.

State which option the plan should adopt and why. If the answer is Option A, what is the size cost (those are short strings — pennies) and is anything else in the allowlist a precedent?

### Q2 — `Source` source field

Same question for `Source`: the plan reads `result['display'].get('source', '')` (desktop convention). Where does this come from on web? Is it the same `Source` shown in the search results UI at `web/pages/search_results.py` (e.g. "PGP transcription", "FJMS catalog")? Find the real producing site, confirm whether it's in `display`, and state the rehydration path that survives compaction.

### Q3 — Sheet name `"Genizah Results"` today

What does the current `export_search_results_excel` actually pass as the sheet name to `create_excel_workbook`? Is it already `"Genizah Results"`, or is it currently something else (e.g. `tr('Search Results')`, hard-coded English)? Confirm Wave 3's plan really is renaming/keeping consistent vs the desktop's `"Search Results"`. If currently translated via `tr()`, what's the Hebrew form, and does Wave 3 lock it to English on web?

### Q4 — Unused `search_terms` after rewrite

Is the bilateral concern about `extract_search_terms` real? Find every site that uses `search_terms` inside `export_search_results_excel` today. Does the rewrite genuinely eliminate every read, leaving the binding orphan (F841)? Or does it survive because the rich-text snippet rendering uses it elsewhere?

### Q5 — `build_rich_snippet_cell` signature vs caller

Wave 1's planned API is `build_rich_snippet_cell(text, sanitize_fn) -> Union[str, CellRichText]`. The web caller at `export_search_results_excel` writes to a cell. Does Wave 3 correctly pass `sanitize_text_for_excel` (the existing helper at `shared_export_utils.py:19-61`) as `sanitize_fn`? Does it pass the right `text` field — `raw_file_hl` from the compacted row, or a different field? Confirm `raw_file_hl` is in the `_SEARCH_ROW_ALLOWLIST`.

### Q6 — UI language for conditional RTL

The plan reads UI lang via `web/safe_storage.py` chokepoint. What is the actual storage key — `'lang'`, `'user_language'`, `'ui_lang'`, or something else? Where is it written when the user toggles between Hebrew and English? Find the producing site and the canonical key, and state which call the plan must make in `export_search_results_excel` to retrieve it. Note: this must NOT go through any translation lookup (D-04 strict).

### Q7 — `sanitize_text_for_excel` applied to new metadata cells

Does the plan apply `sanitize_text_for_excel` to every new cell (Has PGP / Is Printed / Domains / IIIF Manifest / and dossier-sheet cells), or only to the snippet? The dossier helper returns raw primitives per D-08; the caller is responsible for sanitizing. Verify the plan does this for all string cells, not just `Snippet`. Threat T-94-01.

### Q8 — Any other Wave 3 data-flow trap?

Anything else you'd flag HIGH if you reviewed this with real-code archaeology. Especially the export_excel → export_search_results_excel kwarg activation handshake from Wave 2 (does the plan correctly receive `transcription_sys_ids` / `printed_ids` / `result_domains` and pass them down to the row-builder closures?).

## Output format

For each question (Q1..Q8): one labeled section, verdict (PROBLEM / OK / UNCERTAIN / STALE), 2-5 sentence finding with file:line evidence, (if PROBLEM) one-sentence concrete fix. No general summary.
