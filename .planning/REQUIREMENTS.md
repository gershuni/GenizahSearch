# Requirements: GenizahSearch — Milestone v7.17 LOCAL Export Support

**Defined:** 2026-06-01
**Core Value:** Researchers can find what they need in the Genizah corpus
**Closes:** OPEN_ISSUES **D-F17** (xlsx/Word/TXT/CSV export not adapted to LOCAL hits)

## Milestone Goal

Adapt the **desktop** result-export flows so a result set containing LOCAL ("My Library")
hits exports usefully: LOCAL rows carry local-meaningful columns (filename, folder,
filepath, page, matched text) instead of the empty/irrelevant Genizah columns (shelfmark,
library, IIIF URL, PGP, bibliography, domains) they emit today. Mixed (Genizah + LOCAL)
xlsx workbooks get a dedicated **"Local Documents"** sheet; single-table formats fall back
to one LOCAL-aware table. Genizah-only exports stay unchanged.

**Apps:** Desktop only. Web "My Library" does not exist, and web export already excludes
LOCAL via `skip_local`, so the dual-app maintenance rule does not apply here.

## v7.17 Requirements

Each maps to a roadmap phase (continues numbering from Phase 102 → starts at **Phase 103**).

### LOCAL Export Adaptation (LEXP)

- [x] **LEXP-01**: When a user exports **Search results** containing LOCAL hits, each LOCAL row carries local-meaningful values — **filename, parent folder, full filepath, page, and matched text** — in place of the empty Genizah columns (shelfmark / library / IIIF / PGP / domains).
- [⏸] **LEXP-02**: When a user exports a **Composition-Search report** containing LOCAL hits, each LOCAL row carries the same local columns (LEXP-01 parity on the `export_comp_report` surface). **DEFERRED 2026-06-01 → Future Requirements (EXP-F3).** Composition Search has no LOCAL corpus path today (corpus selector is Search-tab-only; `CompositionThread` gets no `corpus_scope`), so a composition report can never contain LOCAL hits — there is nothing to adapt until a LOCAL composition-search UI exists.
- [x] **LEXP-03**: When a user exports a **mixed** Genizah + LOCAL result set to **xlsx**, the workbook contains a dedicated **"Local Documents"** sheet holding the LOCAL rows, and the Genizah "Search Results" sheet contains only Genizah rows.
- [x] **LEXP-04**: The Genizah-only sub-sheets (**Manuscripts**, **Bibliography**) contain **no** LOCAL rows — LOCAL synthetic sys_ids never produce empty/placeholder manuscript or bibliography entries.
- [x] **LEXP-05**: When a user exports a **LOCAL-only** result set to xlsx, the workbook is usable and centered on the Local Documents sheet; the Genizah sub-sheets are omitted or empty (never error).
- [x] **LEXP-06**: When a user exports to **CSV / TXT / DOCX**, the output is a single LOCAL-aware table: LOCAL rows populate the local columns and Genizah rows populate the Genizah columns, with no misleading empty cells for local rows.
- [x] **LEXP-07**: The new LOCAL columns and the "Local Documents" sheet title/headers are **bilingual** — Hebrew when `lang='he'`, English when `lang='en'` — consistent with the existing 4-sheet bilingual export.
- [x] **LEXP-08**: **Genizah-only XLSX / CSV / TXT exports remain structurally unchanged**; the xlsx cross-parity invariant (`tests/test_export_xlsx_cross_parity.py`) and the existing 4-sheet Genizah workbook are preserved (no regression). **DOCX is the deliberate exception** — per Phase 103 discussion (`.planning/phases/103-.../103-CONTEXT.md` D-10/D-12), the DOCX export is intentionally redesigned from a 7-column table into a per-result rich-document block layout for **both** Genizah and LOCAL rows, so Genizah-only DOCX output changes **by design**. (The xlsx invariant is DOCX-independent and stays green.)

## Future Requirements

Deferred to a future milestone. Tracked but not in this roadmap.

### Export (EXP)

- **EXP-F1**: Desktop JSON export of LOCAL / ALL result sets (only if a desktop JSON export is added later; desktop has no JSON export today).
- **EXP-F2**: Parallels-export LOCAL adaptation (excluded from v7.17 scope by user direction).
- **EXP-F3** (was LEXP-02, deferred 2026-06-01): Composition-report LOCAL export — LEXP-01 parity on `export_comp_report`. **Trigger:** wire a LOCAL/ALL corpus scope into Composition Search (so composition reports can contain LOCAL hits). Implementation is already scaffolded: reuse the Phase 103 helpers — `shared/export_dossier.py::build_local_document_row` / `local_documents_header_row`, `shared/docx_export.py::write_docx_result_block`, the `display['source']=='LOCAL'` partition, and the batch-primed `_local_filepath_cache`. The composition surface differs from search results (hierarchical Category→Manuscript→Pages, two text columns Source Context + Manuscript Text, 3+ xlsx sheets), so the xlsx placement / DOCX strategy / Source-Context-for-LOCAL decisions still need a discuss pass when revived.

### Performance (PERF)

- **PERF-F1**: D-F12 — regular Search ~constant ~8s wall-clock investigation (separate, profile-first effort; unrelated to export shape).

## Out of Scope

Explicitly excluded for v7.17. Documented to prevent scope creep.

| Feature | Reason |
|---------|--------|
| Web exports (Excel / Word / JSON) | Web has no LOCAL corpus; web export already excludes LOCAL via `skip_local`. Nothing to adapt. |
| JSON export of LOCAL rows | JSON export is web-only; desktop has no JSON export, and we are not adding one this milestone. |
| Parallels export | User scoped this milestone to Search-results + Composition-report surfaces only. |
| Changing Genizah export columns/structure | Non-goal — Genizah workbook shape and cross-parity invariant must be preserved (LEXP-08). |
| OCR / text-extraction changes for image-only or corrupt PDFs | Separate concern (SEED-003); does not affect export shape. |
| Search-latency work (D-F12) | Separate profile-first effort; not an export-shape change. |

## Traceability

Which phases cover which requirements. Populated during roadmap creation.

| Requirement | Phase | Status |
|-------------|-------|--------|
| LEXP-01 | 103 | Complete |
| LEXP-02 | 104 | ⏸ Deferred → EXP-F3 (no LOCAL comp-search UI) |
| LEXP-03 | 103 | Complete |
| LEXP-04 | 103 | Complete |
| LEXP-05 | 103 | Complete |
| LEXP-06 | 103 | Complete |
| LEXP-07 | 103 | Complete |
| LEXP-08 | 103 | Complete |

**Coverage:**
- v7.17 requirements: 8 total
- Delivered (Phase 103): 7 (LEXP-01, 03, 04, 05, 06, 07, 08)
- Deferred to Future (EXP-F3): 1 (LEXP-02 — blocked on a missing LOCAL composition-search UI)
- Unmapped: 0 ✓

---
*Requirements defined: 2026-06-01*
*Last updated: 2026-06-01 — LEXP-02 (Phase 104) DEFERRED → Future Requirements EXP-F3 during /gsd-discuss-phase 104: Composition Search has no LOCAL corpus path (corpus selector is Search-tab-only; CompositionThread gets no corpus_scope), so export_comp_report can never see LOCAL hits. v7.17 ships with 7/8 LEXP requirements via Phase 103; LEXP-02 revives when a LOCAL composition-search UI is wired.*
