# Codex Pre-Flight Brief — Phase 94 Wave 4 (desktop xlsx parity + docs)

You are doing **integration archaeology** on Wave 4 of GenizahSearch Phase 94 *before* it executes. Scope: Wave 4 only (`genizah_app.py:export_results('xlsx')` restructure to consume `shared/export_dossier.py`, emit same 3-sheet workbook, add `Full Text` + 4 new flag/URL columns; human smoke gate; closeout docs).

Plan: `.planning/phases/94-adding-pgp-to-downloaded-data/94-04-PLAN.md`.
Context: `94-CONTEXT.md` (D-01, D-04, D-15 desktop-parity expansion).
Live code: `genizah_app.py` (large file — use `grep -n` to navigate to relevant line ranges; do NOT read the whole file).

The earlier bilateral review (`94-REVIEWS.md`) raised these HIGH/MEDIUM concerns on Wave 4:
- (a) **HIGH:** Desktop sheet name is `"Search Results"` (via `tr()`), web is `"Genizah Results"`. Violates "identical structure".
- (b) **HIGH:** Desktop main headers are `tr()`-translated. Hebrew UI produces non-English headers, violating EXPORT-META-05 English-only metadata + D-04 unified column order.
- (c) **MEDIUM:** Desktop `Full Text` may be empty for rows where the desktop result dict lacks `full_text`. Add fallback to `self.searcher.get_full_text_by_id(uid)` if available.
- (d) **MEDIUM:** Importing `genizah_app.py` in unit tests is heavy/flaky due to PyQt; move the pure xlsx helper outside `genizah_app.py` if feasible.

## What to verify

Read the real code first (use `grep -n` + targeted `Read` with offset/limit; do NOT load all of `genizah_app.py`). Answer each as **PROBLEM** / **OK** / **UNCERTAIN** / **STALE**. Cite file:line evidence.

### Q1 — Current desktop sheet name

What does the current `export_results('xlsx')` branch pass as the sheet name? Is it `tr("Search Results")` literally, or hard-coded English, or something else? Find the exact line in `genizah_app.py` (search for `wb.create_sheet` or `Workbook` instantiation in `export_results` and `_collect_sorted_results` neighborhoods around `:17895-18030`). State whether Wave 4 must override `tr()` for this string, or accept Hebrew-language sheet name in HE UI mode, or do something else.

### Q2 — Current desktop main-sheet headers

Find the current main-sheet header row construction in the xlsx branch of `export_results`. Are headers passed through `tr()` (so Hebrew UI gets Hebrew column names)? List the exact header strings used today and how they're constructed. State the concrete recommendation: lock English on both sheets, lock English on dossier sheets only, or amend EXPORT-META-09 to accept divergence.

### Q3 — Desktop `Full Text` field availability

What does the desktop search result dict actually carry? Find where it's populated (search for `_collect_sorted_results` or similar around `:17900-17950`). Does each row dict have `full_text` reliably, or is it sometimes empty? If empty, does `self.searcher.get_full_text_by_id(uid)` exist as a fallback (verify by grep)? State whether the plan needs to add the fallback path.

### Q4 — PyQt import cost in unit tests

Wave 4 plans `_build_search_results_xlsx_bytes` as a module-level function for offline testing. Confirm that `from genizah_app import _build_search_results_xlsx_bytes` will not trigger Qt application initialization at import time (look for top-level `QApplication(...)` instantiation in `genizah_app.py` — `grep -n "QApplication" genizah_app.py | head`). If Qt init fires at import, the unit tests will be flaky on headless CI. State whether the helper needs to move out of `genizah_app.py` into a new file (e.g. `desktop_export.py` or `shared/desktop_export.py`).

### Q5 — Desktop `_result_domain_map` shape

CONTEXT names `_result_domain_map` at `genizah_app.py:5461`. Confirm it exists at that line and verify its shape: is it `dict[sys_id_str] -> list[domain_name_str]`, or something else? Is it populated reliably before xlsx export fires, or does it have an initialization race similar to web's Stage-1/Stage-2 enrichment?

### Q6 — `_pgp_transcription_sys_ids` and `_printed_sys_ids` initialization

Confirm both exist as instance attributes initialized in `__init__` (search for both names in `genizah_app.py:2540-2560` per CONTEXT). Are they always present, or only conditional (e.g. only set after the first search)? If conditional, exporting before the first search would AttributeError on `self._printed_sys_ids` — does the plan handle this?

### Q7 — Search-results vs composition-results routing

`export_results('xlsx')` is a single method that handles both search and composition results (per CONTEXT). Find the routing branch (the if/else that picks which result set to export). Confirm the plan's xlsx restructure only touches the search-results branch, not the composition-results branch (`_comp_*` state). What line is the branch decision on, and how does Wave 4 isolate the change?

### Q8 — Any other Wave 4 data-flow trap?

Anything else you'd flag HIGH from real-code archaeology. Especially: existing rich-text snippet rendering at `:17988-18030` (CONTEXT D-14 says this is canonical — the Wave 1 helper at `shared_export_utils.build_rich_snippet_cell` extracts it). Does Wave 4 correctly swap the inline `write_rich_cell` for the shared helper, AND does the existing call site signature match the new helper's `(text, sanitize_fn) -> Union[str, CellRichText]`?

## Output format

For each question (Q1..Q8): one labeled section, verdict (PROBLEM / OK / UNCERTAIN / STALE), 2-5 sentence finding with file:line evidence, (if PROBLEM) one-sentence concrete fix. No general summary.
