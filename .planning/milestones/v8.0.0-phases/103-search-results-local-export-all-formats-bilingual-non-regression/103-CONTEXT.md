# Phase 103: Search-Results LOCAL Export (All Formats + Bilingual + Non-Regression) - Context

**Gathered:** 2026-06-01
**Status:** Ready for planning

<domain>
## Phase Boundary

Adapt the **desktop** Search-results export flow (`genizah_app.py::export_results`, all four formats — XLSX / CSV / TXT / DOCX) so a result set containing **LOCAL ("My Library")** hits exports with local-meaningful values (filename, parent folder, full filepath, page, matched text) instead of the empty/irrelevant Genizah columns (shelfmark / library / IIIF / PGP / bibliography / domains) those rows emit today.

- **xlsx:** a dedicated **"Local Documents"** sheet holds LOCAL rows; the Genizah sub-sheets exclude LOCAL.
- **CSV / TXT / DOCX:** each format gets its own LOCAL-aware treatment (they are structurally different surfaces — see decisions).
- **Bilingual** (he/en) headers and sheet title, consistent with the existing 4-sheet export.
- **Non-regression** on Genizah-only exports (with one deliberate, user-approved DOCX exception — see D-10/D-12).

**Out of this phase:** the Composition-report surface (`export_comp_report`) — that is Phase 104 (LEXP-02). Web exports, JSON export, and Parallels export are out of the v7.17 milestone entirely.

</domain>

<decisions>
## Implementation Decisions

### "Local Documents" xlsx sheet
- **D-01:** The sheet carries **exactly the 5 required columns** — Filename, Parent Folder, Full Filepath, Page, Matched Text. **No** System ID column and **no** Full Text column (kept lean; the synthetic 97-prefix sys_id is meaningless to users, and full page text would bloat the sheet).
- **D-02:** The **Page** column is populated from the LOCAL result dict's `chunk_locator` (human-readable, e.g. `"p. 3"` for PDFs / `"§ Intro"` for docx/html/txt chunks), **falling back to the raw 1-based `p_num`** when no locator is present.
- **D-03:** **Matched Text** uses the same rich-cell treatment as the Genizah "Snippet" column — `build_rich_snippet_cell` bolds matched terms from the asterisk markers in `raw_file_hl`.

### xlsx workbook shape
- **D-04:** **Mixed** (Genizah + LOCAL) sheet order: `[Search Results, Manuscripts, Bibliography, Local Documents, Credits and Info]`. "Local Documents" slots in at **position 4**; "Credits and Info" stays the closing sheet. **Active sheet on open = Search Results.**
- **D-05:** **LOCAL-only** export **omits the empty Genizah sheets entirely** → workbook = `[Local Documents, Credits and Info]`, with **Local Documents active**. It must be usable and never raise a Python error (LEXP-05).
- **D-06:** The "Local Documents" sheet is created **only when the result set contains ≥1 LOCAL hit**. A **Genizah-only** export produces the unchanged 4-sheet workbook — this is what keeps the xlsx cross-parity invariant green (its fixtures are Genizah-only). (LEXP-03, LEXP-08.)
- **D-07:** LOCAL rows are **excluded from the Manuscripts and Bibliography** sub-sheets by flipping the desktop `build_manuscript_row(...)` / `build_bibliography_rows(...)` calls in `_build_search_results_xlsx_bytes` from `skip_local=False` → `skip_local=True`. The kwarg already exists (Phase 95 D-45) and returns `None`/`[]` for LOCAL sys_ids; web already passes `True`. (LEXP-04.)

### CSV (single flat table)
- **D-08:** When LOCAL rows are present, LOCAL rows **repurpose existing columns to mirror the on-screen results table** (`genizah_app.py:16726`): **Shelfmark col = filename, Library col = parent folder, Source = "LOCAL", Snippet = matched text** — and the table gains **two appended columns, Filepath and Page**, that appear **only when LOCAL rows are present**. No empty/meaningless Genizah cells on LOCAL rows (LEXP-06). A **Genizah-only** CSV is the unchanged 7-column table (LEXP-08).

### TXT (labeled blocks, not a table)
- **D-09:** A LOCAL block is:
  ```
  === {filename} | {parent folder} ===
  Path: {full filepath}  (page N)
  {matched-text snippet}
  ```
  Genizah blocks are unchanged (`=== {shelfmark} | {title} ===` + snippet). The "page N" uses the same locator/page-fallback as D-02.

### DOCX (intentional enrichment — applies to BOTH Genizah and LOCAL)
- **D-10:** DOCX is **redesigned from the current cramped 7-column python-docx table into a per-result "rich document" block layout**, applied to **both Genizah and LOCAL** results. Each result is one block:
  - **Heading:** `Shelfmark — Title` (Genizah) / `Filename — Parent folder` (LOCAL)
  - **Metadata line:** `Library · Image/Page · Source` (Genizah) / `{full filepath} · page N · LOCAL` (LOCAL)
  - **Matched text** as a flowing paragraph with **bold highlights** (not a clipped table cell)
  - **URL line** (see D-11)
  - A thin **separator** between results
  - Reads like a research handout, not a spreadsheet dump. RTL handling for Hebrew is preserved.
- **D-11:** The **URL line** shows the **GenizahSearch URL** for Genizah rows and the **full filepath** for LOCAL rows. (User direction: "add URL".)
- **D-12:** ⚠ **INTENTIONAL DEVIATION from LEXP-08 (DOCX clause only).** The user explicitly approved enriching the DOCX for Genizah too, so **Genizah-only DOCX output changes by design** — it is no longer "structurally unchanged." This was a conscious scope decision. **The xlsx cross-parity invariant (`tests/test_export_xlsx_cross_parity.py`) is xlsx-only and Genizah-only-fixture based — it is UNAFFECTED and must stay green.** `REQUIREMENTS.md` LEXP-08 and `ROADMAP.md` Phase 103 success-criterion #5 were amended during this discussion to carve out DOCX (XLSX/CSV/TXT non-regression unchanged). The verifier must NOT flag the Genizah DOCX change as a regression.

### Bilingual labels (LEXP-07)
- **D-13:** The Local Documents sheet title + the new local column headers are bilingual, consistent with the existing 4-sheet headers in `shared/export_dossier.py`:
  - **EN:** sheet `Local Documents` · columns `Filename` / `Parent Folder` / `Full Filepath` / `Page` / `Matched Text`
  - **HE:** sheet `מסמכים מקומיים` · columns `שם קובץ` / `תיקייה` / `נתיב מלא` / `עמוד` / `טקסט תואם`
  - The appended CSV Filepath/Page column headers and the TXT/DOCX field labels use these same bilingual terms.

### LOCAL row discrimination & field sourcing
- **D-14:** Partition LOCAL vs Genizah rows at export time using **`result['display'].get('source') == 'LOCAL'`** as the primary discriminator (set in `genizah_core.py::_build_local_result_dict`), with the 97-prefix synthetic sys_id / `is_synthetic_sys_id` as a secondary guard. Field sourcing for a LOCAL row:
  - **filename** ← `display['shelfmark']`
  - **full filepath** ← `_lookup_local_filepath(sys_id)` (batch-primed `_local_filepath_cache` — do NOT do per-row SQLite round-trips; prime the cache once like the v7.16 BUG-6 fix)
  - **parent folder** ← `os.path.basename(os.path.dirname(filepath))`
  - **page** ← `chunk_locator` then `p_num` (D-02)
  - **matched text** ← `raw_file_hl` (D-03)

### Claude's Discretion
- Exact openpyxl column widths for the Local Documents sheet; DOCX block typography, heading style, and separator rendering.
- Whether to surface any *optional extra* Genizah metadata (PGP description / domains / date) in DOCX blocks beyond the URL — **kept lean by default (URL only)** unless trivially available; do not expand without need.
- Helper placement — likely new functions in `shared/export_dossier.py` (Local Documents bilingual header/title + a LOCAL-row builder taking a result dict) and a **shared DOCX block writer** designed to be reusable by Phase 104's `export_comp_report`.
- Whether a LOCAL filepath that can't be resolved (missing/moved file) emits a blank Filepath cell + a logged warning (graceful, never error) — recommended.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & success criteria (this phase)
- `.planning/REQUIREMENTS.md` — milestone v7.17; LEXP-01, LEXP-03, LEXP-04, LEXP-05, LEXP-06, LEXP-07, LEXP-08 (LEXP-08 **amended** this session for the DOCX carve-out — see D-12).
- `.planning/ROADMAP.md` § "Phase 103" — goal + 6 success criteria (criterion #5 **amended** this session for the DOCX carve-out).

### Non-regression invariant (MUST stay green)
- `tests/test_export_xlsx_cross_parity.py` — pins **xlsx structural parity** between web and desktop: identical `sheetnames` (and order) + byte-identical header rows for the main (12-col), Manuscripts (14-col), and Bibliography (8-col) sheets. **Fixtures are Genizah-only**, so adding the "Local Documents" sheet *only when LOCAL rows exist* (D-06) keeps it passing with no test modification (success-criterion #5). Data cells are NOT pinned.

### Export code (desktop)
- `genizah_app.py:19595` — `export_results(self, fmt)`: format dispatch (XLSX ~19684 / CSV ~19856 / DOCX ~19874 / TXT ~19925); per-result collection + snippet cleaning ~19614-19667.
- `genizah_app.py:2531` — `_build_search_results_xlsx_bytes(...)`: the 4-sheet builder. LOCAL exclusion happens here (D-07: flip the two `skip_local` args).
- `genizah_app.py:18804` — `_lookup_local_filepath(sys_id)` + `_local_filepath_cache` batch prime (D-14 filepath source).
- `genizah_app.py:16726` — established on-screen LOCAL display pattern (filename→Shelfmark, parent folder→Library) that CSV mirrors (D-08).

### Shared export primitives
- `shared/export_dossier.py` — bilingual helpers (`main_header_row`, `manuscript_header_row`, `bibliography_header_row`, `sheet_titles`, `build_manuscript_row`/`build_bibliography_rows` with the existing `skip_local` kwarg + `is_local_sys_id`). New Local Documents header/title + LOCAL-row builder belong here (bilingual, Qt-free).
- `build_rich_snippet_cell` (shared export utils) — rich bold-highlight matched-text cell (D-03).

### LOCAL data model
- `genizah_core.py:7159` — `_build_local_result_dict(...)`: the LOCAL result-dict shape (`sys_id`, `p_num`, `img`, `raw_file_hl`, `full_text`, `snippet`, `chunk_locator`, `display['source']=='LOCAL'`, `display['shelfmark']`=filename, `full_header`).
- `shared/local_indexer.py` — `local_files` (sys_id, filepath, original_filename, folder_id) + `local_pages` (chunk_locator) schema; `get_filepath` / `get_filepaths`.
- `desktop/file_actions.py` — established LOCAL filepath utilities pattern (`reveal_local_file`, `copy_file_location`).

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **`skip_local` kwarg** already on `build_manuscript_row` / `build_bibliography_rows` (Phase 95 D-45) — LEXP-04 is a one-flag flip on desktop, not new logic.
- **`_lookup_local_filepath` + `_local_filepath_cache`** — batch-primed filepath lookup already exists (v7.16 BUG-6); reuse it, don't re-implement per-row SQLite reads.
- **Bilingual header/title infra** in `export_dossier.py` (`sheet_titles`, `*_header_row`) — extend with Local Documents EN/HE following the same pattern.
- **`build_rich_snippet_cell`** — already produces the bold-highlight matched-text cell used by the Genizah Snippet column.
- **On-screen LOCAL table mapping** (filename→Shelfmark, parent folder→Library) — the export should match it so on-screen and exported shapes agree.

### Established Patterns
- Desktop xlsx builder is a **module-level pure function** (`_build_search_results_xlsx_bytes`, Qt-free, offline-testable) — keep new helpers Qt-free for unit testing (mirrors `test_desktop_xlsx_multi_sheet.py`).
- LOCAL hits already carry everything needed on the result dict except filepath/folder (derived from filepath) — no new query path needed beyond the existing filepath lookup.

### Integration Points
- New **Local Documents sheet** writer in `_build_search_results_xlsx_bytes` (gated on "≥1 LOCAL row present"); new bilingual header/title + LOCAL-row builder in `export_dossier.py`.
- New **shared DOCX per-result block writer** (reusable by Phase 104) replacing the current table branch in `export_results`'s DOCX path.
- LOCAL/Genizah **row partition** helper keyed on `display['source']=='LOCAL'`.
- CSV branch: conditional column widening (Filepath/Page appended only when LOCAL present).

</code_context>

<specifics>
## Specific Ideas

- The user's framing for DOCX: **"it's not a table"** — DOCX should read like a research document (per-result blocks), not a cramped spreadsheet dump. Apply the richer layout to Genizah too ("for Genizah too, while we're at it") with a URL line.
- CSV should mirror the **on-screen results table** mapping so the exported and displayed shapes agree (filename in Shelfmark column, parent folder in Library column).
- Hebrew matters (native-speaker user): the chosen HE label set is `מסמכים מקומיים` / `שם קובץ` / `תיקייה` / `נתיב מלא` / `עמוד` / `טקסט תואם`.

</specifics>

<deferred>
## Deferred Ideas

- **Composition-report DOCX/LOCAL parity (Phase 104, LEXP-02):** design the shared DOCX block writer so `export_comp_report` can reuse it. Not in Phase 103 scope.
- **Full Text column on the Local Documents sheet:** explicitly declined (D-01); could be revisited if users ask for full page text in the export.
- **Extra Genizah metadata in DOCX blocks** (PGP description / domains / date beyond the URL): left lean for now; could enrich further later.
- **JSON export of LOCAL rows** and **Parallels-export LOCAL adaptation:** out of v7.17 entirely (EXP-F1 / EXP-F2).

### Reviewed Todos (not folded)
The phase-103 todo matcher surfaced 6 items, all generic keyword matches with no relation to export shape — none folded:
- "Migrate desktop corrections fetch to shared corrections_service" — unrelated (corrections, not export).
- "Reading Desk UX fixes" — unrelated (Reading Desk).
- "Server-side search with email notification" — unrelated (search infra).
- "NLI MARC crawl and translate" — unrelated (data pipeline).
- "Unified metadata text search with translations" — unrelated (search/browse).
- "Fill missing genizah manuscripts from FIST.db" — unrelated (data).

</deferred>

---

*Phase: 103-search-results-local-export-all-formats-bilingual-non-regression*
*Context gathered: 2026-06-01*
