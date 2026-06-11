# Phase 110 Code Review - Composition / Parallels LOCAL Corpus Support

Reviewed live source and the `ee68a018..HEAD` source-file diff requested by the prompt. Focused test run:

```text
pytest tests/test_comp_corpus_scope.py tests/test_comp_export_local.py -q
12 passed, 4 xpassed
```

## Findings

### [HIGH] LOCAL-only composition exports still emit Genizah/MiDRASH credits

Evidence:

- `genizah_app.py:20591` builds `credit_text = self._get_credit_header()` with the default `local_only=False`.
- That same `credit_text` is written into every composition format: xlsx via `_write_credit_block()` at `genizah_app.py:20855-20864` and calls at `genizah_app.py:20902` / `genizah_app.py:20932`, csv at `genizah_app.py:21142-21144`, docx at `genizah_app.py:21181-21188`, and txt at `genizah_app.py:21426-21429`.
- `_get_credit_header(local_only=True)` already suppresses the MiDRASH/Zenodo lines at `genizah_app.py:15787-15806`.
- The Phase 103 search-results export already solved this: it computes `_local_only_export` at `genizah_app.py:20121-20131`, passes it to `_get_credit_header(local_only=...)` at `genizah_app.py:20182`, and passes it into the shared credits sheet builder at `genizah_app.py:20369-20370`. The shared `credits_lines(..., local_only=True)` also omits the MiDRASH rows at `shared/export_dossier.py:553-584`.

Root cause:

`export_comp_report()` detects LOCAL rows for metadata/cache/export routing, but never computes a LOCAL-only export predicate and never threads it into the credits builder/header.

Concrete fix:

- In `export_comp_report()`, after `_is_local_item()` and after the selected/full comp item sets are known, compute a structural predicate over Main + Appendix + Filtered + Filtered Appendix + Known:
  - `_has_local_comp_export = any(_is_local_item(item) for item in all_comp_items)`
  - `_local_only_comp_export = _has_local_comp_export and all(_is_local_item(item) for item in all_comp_items)`
- Change `genizah_app.py:20591` to `self._get_credit_header(local_only=_local_only_comp_export)`.
- If the xlsx comp export is migrated to a dedicated Credits/Info sheet, call `shared.export_dossier.build_credits_info_sheet(..., local_only=_local_only_comp_export, app_name="Dicta Genizah Search Pro")`, mirroring `genizah_app.py:2900-2911`.
- Add a regression test that a LOCAL-only comp csv/txt/docx/xlsx export contains no `MiDRASH`, `Stoekl`, `Dataset`, `Zenodo`, `NLI`, `FGP`, or `PGP` attribution lines except app/creator credit.

### [HIGH] LOCAL-only composition exports still use manuscript terminology and empty Genizah export surfaces

Evidence:

- The xlsx comp export always creates Genizah-oriented `Report View`, `Raw Data`, and `Query information` sheets at `genizah_app.py:20820-20828`, even when `table_rows` is empty and all rows are LOCAL.
- The xlsx/csv/docx headers always include `tr("Manuscript Text")` at `genizah_app.py:20888-20899`, `genizah_app.py:21130-21141`, and `genizah_app.py:21202-21213`.
- Category/summary labels still say manuscripts: `tr("Main Manuscripts")` and `tr("Excluded Manuscripts")` at `genizah_app.py:20805-20811`, docx summary labels at `genizah_app.py:21191-21195`, txt summary labels at `genizah_app.py:21388-21407`, and txt section heading `tr("EXCLUDED MANUSCRIPTS")` at `genizah_app.py:21422-21423`.
- The local rows are added later to `Local Documents` at `genizah_app.py:21101-21119`, but the legacy Genizah table/header/summaries still remain in the same export.
- The Phase 103 search-results LOCAL export already solved the equivalent shape problem by computing `_local_only` at `genizah_app.py:2666-2668` and removing empty Genizah sheets so LOCAL-only xlsx is exactly `[Local Documents, Credits and Info]` at `genizah_app.py:2924-2934`.

Root cause:

The comp export adds a parallel Local Documents surface but leaves the old Genizah/manuscript report structure unconditional. For a LOCAL-only export, that produces empty Genizah report tables/sheets and incorrect "manuscript(s)" terminology.

Concrete fix:

- Reuse the `_local_only_comp_export` predicate from the credits fix.
- For LOCAL-only xlsx, mirror `genizah_app.py:2924-2934`: either do not create the Genizah `Report View` / `Raw Data` sheets, or remove them before save and leave only `Local Documents` plus a credits/query-info sheet.
- For LOCAL-only csv/docx/txt, do not emit the 10-column Genizah table headers at all; emit the `local_documents_header_row(CURRENT_LANG)` section as the primary export surface.
- If the product wants a summary section, use LOCAL terminology: "Documents Found", "Main Documents", "Filtered by Text (Documents)", "Excluded Documents", "Document Text" or "Matched Text". Add flat EN keys in `genizah_translations.py` for those labels before calling `tr(...)`.
- Keep mixed exports as two surfaces: Genizah rows keep manuscript terminology; LOCAL rows stay under `Local Documents`.

### [BLOCKER] LOCAL composition grouping can still send private `97...` ids to NLI metadata fetch

Evidence:

- `group_composition_results()` collects every item `sys_id` into `ids` at `genizah_core.py:9560-9569`.
- It calls `self.meta_mgr.batch_fetch_shelfmarks([x for x in ids if x], ...)` without filtering LOCAL ids at `genizah_core.py:9571-9575`.
- `MetadataManager.batch_fetch_shelfmarks()` builds `to_fetch = [sid for sid in system_ids if sid not in self.nli_cache]` at `genizah_core.py:5094-5095` and submits every missing id to `_fetch_single_worker` at `genizah_core.py:5103-5104`.
- LOCAL `97...` ids are not in `csv_bank`, so a grouped LOCAL composition run can reach the NLI network path before export. This violates the same D-12 privacy invariant that the export prefetch fixed at `genizah_app.py:20529-20545`.

Root cause:

Phase 110 filtered LOCAL ids out of `export_comp_report()` metadata prefetch, but the pre-display grouping path still treats LOCAL sys_ids as Genizah catalog ids.

Concrete fix:

- In `genizah_core.py:9560-9575`, import/use `shared.local_sys_id.is_local_sys_id` and filter before `batch_fetch_shelfmarks`:
  - `genizah_ids = [sid for sid in ids if sid and not is_local_sys_id(sid)]`
  - call `batch_fetch_shelfmarks(genizah_ids, ...)`.
- Also avoid LOCAL ids in Genizah-only enrichment helpers used during comp display:
  - `_collect_comp_domain_data()` currently sends all ids to FJMS at `genizah_app.py:16687-16730` and printed lookup at `genizah_app.py:16760-16761`; filter out LOCAL there.
  - `_collect_id()` in `display_comp_results()` adds cache-missing ids at `genizah_app.py:22700-22706`, and `start_metadata_loading()` creates a `ShelfmarkLoaderThread` at `genizah_app.py:18644-18685`; filter LOCAL ids before adding them.
- Add a pure test where `group_composition_results()` receives a LOCAL grouped item and `meta_mgr.batch_fetch_shelfmarks` is asserted to receive `[]`.

### [MEDIUM] Excluded LOCAL comp rows are duplicated into Genizah "Excluded Manuscripts" exports

Evidence:

- `_collect_local_comp_pages()` includes LOCAL rows from `c_known` at `genizah_app.py:20678-20680`, so excluded LOCAL pages are already exported to `Local Documents`.
- The xlsx excluded sheet is still created for any `c_known` at `genizah_app.py:21070-21095` and `_excluded_row()` does not skip `_is_local_item(item)`.
- The docx excluded table has the same issue at `genizah_app.py:21267-21297`.
- Both paths label the surface "Excluded Manuscripts", which is also wrong for LOCAL files.

Root cause:

The main/raw Genizah table writers skip LOCAL rows, but the special excluded-manuscripts sub-export was not updated.

Concrete fix:

- In the xlsx excluded-sheet writer and docx excluded-table writer, partition `c_known` into Genizah vs LOCAL first.
- Only Genizah known items should go to `Excluded Manuscripts`.
- LOCAL known items should remain only in the Local Documents output, or in a LOCAL-only "Excluded Documents" subsection if the UI requires explicit excluded labeling.
- For LOCAL-only export, do not create `Excluded Manuscripts` at all.

### [MEDIUM] Restored LOCAL composition results can fall back to per-row SQLite filepath lookups

Evidence:

- Fresh scan completion primes local filepaths before grouping/rendering at `genizah_app.py:22261-22265`.
- Session restore displays restored comp results directly at `genizah_app.py:25745-25763` without calling `_prime_comp_local_filepath_cache()`.
- `_comp_local_display_fields()` calls `_lookup_local_filepath(sys_id)` at `genizah_app.py:21729-21733`.
- `_lookup_local_filepath()` falls through to `indexer.get_filepath(sys_id)` at `genizah_app.py:19285-19298` when the cache was not primed, which is the per-row SQLite path the Phase 110 display helper was meant to avoid.

Root cause:

Batch priming is tied to the fresh search callback instead of the display entry point. Restored/session-rendered comp data bypasses the prime.

Concrete fix:

- At the start of `display_comp_results()`, flatten `main_res`, `main_appx`, `filt_res`, `filt_appx`, and `self.comp_known`, then call `_prime_comp_local_filepath_cache(...)` once.
- Keep the existing fresh-scan prime if desired, but the display-time prime is the safer invariant because every renderer path flows through it.

### [MEDIUM] `export_comp_report()` has inconsistent LOCAL detection for source-labeled LOCAL items

Evidence:

- `_has_local` is computed from `local_ids = [sid for sid in unique_ids if _is_local_sys_id(sid)]` at `genizah_app.py:20553-20563`.
- `_is_local_item()` correctly treats `src_lbl == 'LOCAL'` as a fast path at `genizah_app.py:20573-20578`.
- `local_comp_pages = _collect_local_comp_pages() if _has_local else []` at `genizah_app.py:20688`; therefore a source-labeled LOCAL item with a missing/non-97 parsed sid is detected by `_is_local_item()` but never exported to Local Documents because `_has_local` is false.
- The regular search-results exporter avoids this class of bug by making `display.source == 'LOCAL'` the primary discriminator at `genizah_app.py:2660-2664`.

Root cause:

The export gate uses only the secondary 97-prefix discriminator, while the row-level discriminator uses both source label and sys_id.

Concrete fix:

- Compute `_has_local` from the actual comp item predicate:
  - `_all_comp_items = _collect_comp_items(c_main, c_appx, c_filt, c_filt_appx, c_known)` or an equivalent flattening helper.
  - `_has_local = any(_is_local_item(item) for item in _all_comp_items)`.
- Keep `local_ids` as a separate list solely for filepath-cache priming.
- Add a test where a grouped item has `src_lbl='LOCAL'` but lacks a `display` dict and still lands on Local Documents.

### [LOW] Phase 110 export tests are marked xfail after they now pass

Evidence:

- All four tests in `tests/test_comp_export_local.py` still carry `@pytest.mark.xfail(..., strict=False)` at `tests/test_comp_export_local.py:94`, `tests/test_comp_export_local.py:124`, `tests/test_comp_export_local.py:159`, and `tests/test_comp_export_local.py:183`.
- The focused run reports `4 xpassed`, not plain passes.

Root cause:

The Wave-0 scaffolding markers were never removed after the helpers landed.

Concrete fix:

- Remove the four `xfail` decorators or set `strict=True` temporarily while converting them.
- Add explicit tests for the two reported bugs: LOCAL-only comp export credits and LOCAL-only comp export terminology/sheet shape.

## Notes On Reviewed Areas Without Findings

- Standard `search_composition_logic()` does query the regular My-Library index (`self.local_index` / `self.local_searcher`) rather than the LAB index at `genizah_core.py:9183-9221`.
- The standard LOCAL hook initializes `doc_hits`, `was_cancelled`, and `total_chunks` before the branch at `genizah_core.py:9006-9024`, so a LOCAL-only run does not appear to reference Genizah-loop-local variables.
- Both standard and Lab composition paths fail closed to `genizah` for bad `corpus_scope` values at `genizah_core.py:8978-8980` and `genizah_core.py:1427-1429`.
- The Genizah LAB None guard covers both `self.lab_index` and `self.lab_searcher` at `genizah_core.py:1501-1505` and returns a normal dict with `corpus_scope` / `local_lab_stale` at `genizah_core.py:1922-1931`.
