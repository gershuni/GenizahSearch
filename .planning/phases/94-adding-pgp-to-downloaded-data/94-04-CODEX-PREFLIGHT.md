**Q1**

Verdict: **PROBLEM**. Current desktop xlsx uses the active workbook sheet and sets `ws.title = tr("Search Results")` at `genizah_app.py:17990-17993`. Current web xlsx uses `create_excel_workbook("Genizah Results")` at `web/export_service.py:425`. Fix: Wave 4 should hard-code the desktop main sheet name to English `"Genizah Results"` if EXPORT-META-09 means identical workbook structure.

**Q2**

Verdict: **PROBLEM**. Current desktop headers are built as `headers = [tr("System ID"), tr("Library"), tr("Shelfmark"), tr("Title"), tr("Image/Page"), tr("Source"), tr("Snippet")]` at `genizah_app.py:17909-17910`, then written directly at `genizah_app.py:18038-18042`. In Hebrew UI these become Hebrew labels. Fix: lock English headers on the main sheet and dossier sheets, or explicitly amend EXPORT-META-09 to allow sheet/header divergence.

**Q3**

Verdict: **PROBLEM**. `_collect_sorted_results()` only returns row `UserRole` dicts or `self.last_results`; it does not hydrate missing text at `genizah_app.py:17503-17516`. Regular core search rows often include `full_text` (`genizah_core.py:7362-7369`), but PGP tag results appended in desktop omit `full_text` and `uid` entirely at `genizah_app.py:17065-17076`; existing app code already uses `self.searcher.get_full_text_by_id(...)` as a lazy fallback elsewhere at `genizah_app.py:17813-17815`, and the method exists at `genizah_core.py:8452-8459`. Fix: Wave 4 should fetch `get_full_text_by_id(uid)` when `full_text/full_text_excerpt` is empty and `uid` is available.

**Q4**

Verdict: **OK**. Importing `genizah_app.py` imports PyQt classes at `genizah_app.py:13-26`, but it does not create a `QApplication` during a normal import. The real `QApplication(sys.argv)` construction is under `if __name__ == "__main__"` at `genizah_app.py:23403-23423`; the earlier construction at `genizah_app.py:39-48` only runs the app path when the file is executed as main after a core import failure. Moving the helper out is still cleaner for PyQt-free tests, but not required specifically to avoid QApplication initialization.

**Q5**

Verdict: **PROBLEM**. `_result_domain_map` exists as `sys_id -> list of domain names` at `genizah_app.py:5461`, and `_on_domain_enrichment_loaded()` populates it with qualified string names at `genizah_app.py:15587-15600`. It is explicitly reset before enrichment at `genizah_app.py:16421-16426`, export buttons are enabled earlier at `genizah_app.py:16414`, and the domain worker starts asynchronously at `genizah_app.py:16727-16729`. Fix: do not rely only on `_result_domain_map` at export time; synchronously fetch/recompute domains for exported sys_ids or block/disable export until enrichment completes.

**Q6**

Verdict: **OK**. Both fields are unconditional instance attributes in `__init__`: `_pgp_transcription_sys_ids = set()` at `genizah_app.py:2547` and `_printed_sys_ids = set()` at `genizah_app.py:2550`. They are later updated by async workers at `genizah_app.py:16957-16975`. No AttributeError risk exists before first search, though empty values before worker completion remain a data-completeness risk.

**Q7**

Verdict: **STALE**. The live code does not route search and composition through one `export_results()` branch. Search export buttons call `self.export_results(...)` at `genizah_app.py:5693-5705`, while composition export buttons call `self.export_comp_report(...)` at `genizah_app.py:6080-6090`; the composition method starts at `genizah_app.py:18158`, with its own xlsx branch at `genizah_app.py:18370-18385`. Wave 4 should isolate changes to the search xlsx block at `genizah_app.py:17983-18067` and leave `export_comp_report()` untouched.

**Q8**

Verdict: **PROBLEM**. The current rich snippet helper is an inline writer with signature `write_rich_cell(row, col, text)` at `genizah_app.py:18000-18021`, called at `genizah_app.py:18050-18052`; a direct replacement with `(text, sanitize_fn)` would require changing the call site to assign the returned value into the cell. The Wave 4 plan’s detailed sample does that correctly with `value=build_rich_snippet_cell(snippet_raw, sanitize_fn)` at `.planning/phases/94-adding-pgp-to-downloaded-data/94-04-PLAN.md:417-421`, but the plan also says keeping the inline helper is allowed at `94-04-PLAN.md:24`, which conflicts with the parity/test intent. Fix: make the shared-helper swap mandatory and use the return-value call shape, while also addressing the async PGP/printed/domain readiness race before reading those flags for export.
