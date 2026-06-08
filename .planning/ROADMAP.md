# Roadmap: GenizahSearch

## Milestones

- **v1 External Data Integration** -- Phases 1-7 (shipped 2026-02-07)
- **v5.6.0 Desktop Parity & PGP Integration** -- Phases 8-12 (shipped 2026-02-09)
- **v5.7.0 Responsa Search** -- Phases 14-17 (shipped 2026-02-10)
- **v5.7.2 Cleanup, Normalization & Sections** -- Phases 18-21 (shipped 2026-02-11)
- **v5.7.3 Pending Corrections Visibility** -- Phases 22-24 (shipped 2026-02-11)
- **v5.8.0 FJMS Integration** -- Phases 25-28 (shipped 2026-02-15)
- **v5.9.0 Multi-Source Image & Metadata Integration** -- Phases 29-34 (shipped 2026-02-16)
- **v6.0.0 Local Data Architecture** -- Phases 35-40 (shipped 2026-02-22)
- **v6.1.0 Catalog Browse & Navigation** -- Phase 41 (shipped 2026-02-27)
- **v6.5.0 Search UX & Filtered Search** -- Phases 42-46 (shipped 2026-03-14)
- **v7.0.0 Fragment Puzzle** -- Phases 47-52 (shipped 2026-03-17)
- **v7.1.0 FIST Gap Fill** -- Phase 53 (shipped 2026-03-19)
- **v7.6 Search Refinement & Scholarly Joins** -- Phases 54-57 (shipped 2026-03-31)
- **v7.7 Volume-Aware Browse** -- Phases 58-61 (shipped 2026-04-01)
- **v7.8 Structural Foundation** -- Phases 63-66 (shipped 2026-04-15)
- **v7.9 Decomposition** -- Phases 67-76 (complete 2026-04-17)
- **v7.10 Search API** -- Phases 77-83 (shipped 2026-05-05)
- **v7.11 CUDL Coverage & Synthetic Inventories** -- Phases 84-86 (shipped 2026-05-12)
- **v7.12 Multitenant Architecture (Path B)** -- Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4 (shipped 2026-05-18)
- **v7.13 Research-Grade Downloads & PGP Filter** -- Phases 93-94 (shipped 2026-05-21)
- **v7.14 My Library — Local Document Search** -- Phases 95-98 (shipped 2026-05-24; closed 2026-05-27)
- **v7.15 My Library Visual** -- Phases 99-101 (shipped 2026-05-28). See `milestones/v7.15-ROADMAP.md`
- **v7.16 Hebrew PDF Text Quality** -- Phase 102 + no-phase quality work (shipped 2026-06-01). See `milestones/v7.16-ROADMAP.md`
- **v8.0.0 Dicta Rebrand & Joins Lab** -- BRAND (no-phase) + Phases 103 + 105 (delivered, folded from v7.17; Phase 104 → EXP-F3) + **Phases 106-110 Joins Lab** (active; roadmap created 2026-06-03). Web Joins Lab UI deferred to a later phase.

## Phases

### Phase 103: Search-Results LOCAL Export (All Formats + Bilingual + Non-Regression)

**Goal**: Users can export a Search-results set containing LOCAL hits — in any of the four desktop formats — and receive useful, locally-meaningful columns for each LOCAL row, with a dedicated "Local Documents" sheet in xlsx and full non-regression on Genizah-only exports.

**Depends on**: Phase 102 (LOCAL corpus infrastructure in place)

**Requirements**: LEXP-01, LEXP-03, LEXP-04, LEXP-05, LEXP-06, LEXP-07, LEXP-08

**Success Criteria** (what must be TRUE):
  1. Exporting a mixed Genizah + LOCAL result set to xlsx produces a workbook whose "Local Documents" sheet lists each LOCAL hit's filename, parent folder, full filepath, page number, and matched-text snippet — and whose "Search Results" sheet contains only Genizah rows with no placeholder LOCAL entries.
  2. The "Manuscripts" and "Bibliography" sub-sheets in a mixed xlsx export contain no rows for LOCAL synthetic sys_ids — only real Genizah manuscripts appear there.
  3. Exporting a LOCAL-only result set to xlsx produces a usable workbook (the "Local Documents" sheet is populated and active; the Genizah sub-sheets are omitted or empty; no Python error is raised).
  4. Exporting a mixed result set to CSV, TXT, or DOCX produces a single unified table where LOCAL rows carry the local columns (filename/folder/filepath/page/matched-text) and Genizah rows carry the Genizah columns — no LOCAL row shows a row of empty/meaningless Genizah cells.
  5. Exporting a Genizah-only result set to XLSX / CSV / TXT produces output structurally identical to pre-v7.17 output, and the `tests/test_export_xlsx_cross_parity.py` invariant passes with no modifications. (DOCX is the intentional exception — see `103-CONTEXT.md` D-10/D-12: the DOCX export is redesigned into a per-result rich-document block layout for both Genizah and LOCAL rows, so Genizah-only DOCX changes by design.)
  6. The "Local Documents" sheet title and its column headers appear in Hebrew when the export language is `he` and in English when the export language is `en`, matching the bilingual behaviour of the existing four Genizah sheets.

**Plans**: 4 plans
- [x] 103-01-PLAN.md — Shared primitives: Local Documents bilingual header/title + row builder in export_dossier.py + reusable module-level DOCX block writer (shared/docx_export.py) [wave 1]
- [x] 103-02-PLAN.md — Desktop xlsx: Local Documents sheet (D-04/D-06), skip_local flip (D-07), LOCAL-only workbook (D-05), filepath-map wiring [wave 2]
- [x] 103-03-PLAN.md — Desktop CSV/TXT/DOCX: CSV column repurposing + Filepath/Page (D-08), TXT blocks (D-09), DOCX per-result block redesign (D-10/D-11/D-12) [wave 2]
- [x] 103-04-PLAN.md — Non-regression gate: cross-parity invariant unmodified + Genizah-only XLSX/CSV/TXT structure + DOCX carve-out assertion (LEXP-08) [wave 3]

---

### Phase 104: Composition-Report LOCAL Export — ⏸ DEFERRED (2026-06-01)

> **DEFERRED — blocked on a missing precondition.** Composition Search has **no LOCAL corpus path**: the `Genizah/Local/ALL` corpus selector exists only on the regular Search tab (`genizah_app.py:5924`), and `CompositionThread` (`genizah_app.py:21532`) is never passed a `corpus_scope` (unlike `SearchThread` at `:16763`). LOCAL hits therefore cannot appear in a composition report, so `export_comp_report` has no observable LOCAL surface to adapt. Building LEXP-02 now would be speculative, real-UI-untestable code against data that cannot exist.
>
> **Trigger to revive:** when a LOCAL/ALL corpus scope is wired into Composition Search (so composition reports can contain LOCAL hits). At that point re-open this phase and apply the Phase 103 helpers (`shared/export_dossier.py::build_local_document_row` / `local_documents_header_row`, `shared/docx_export.py::write_docx_result_block`, the `display['source']=='LOCAL'` partition, and the batch-primed `_local_filepath_cache`). LEXP-02 moved to REQUIREMENTS.md → Future Requirements (EXP-F3).

**Goal**: Users running a Composition Search whose result set contains LOCAL hits receive the same local-column treatment (LEXP-01 parity) on the Composition-report export surface (`export_comp_report`), using the helpers and row-shape established in Phase 103.

**Depends on**: Phase 103 (LOCAL row-shape + xlsx sheet helpers defined there) **+ a LOCAL composition-search UI (does not exist yet)**

**Requirements**: LEXP-02 (deferred — see EXP-F3)

**Success Criteria** (what must be TRUE):
  1. Exporting a Composition-Search report whose chunks include LOCAL hits to xlsx places those LOCAL rows in a "Local Documents" sheet (or LOCAL-aware section), not in the Genizah chunk rows — each LOCAL row shows filename, folder, filepath, page, and matched text.
  2. Exporting the same Composition-Search report to CSV, TXT, or DOCX produces a single table where LOCAL chunk rows carry the local columns and Genizah chunk rows carry the Genizah columns — no misleading empty cells for LOCAL rows.
  3. A Genizah-only Composition-Search report export is structurally unchanged from pre-v7.17 output across all four formats.

**Plans**: None — deferred before planning (see deferral banner above).

---

### Phase 105: Export UX Polish

**Goal**: Four desktop export-experience fixes surfaced during the v7.17 close, spanning both export surfaces (`export_results` + `export_comp_report`): a richer "export complete" dialog, and three LOCAL-aware corrections that stop Genizah-only artifacts from leaking into LOCAL exports.

**Depends on**: Phase 103 (LOCAL row-shape + `_has_local`/`source=='LOCAL'` partition + `shared/docx_export.py` block writer)

**Requirements**: EXPUX-01, EXPUX-02, EXPUX-03, EXPUX-04

**Success Criteria** (what must be TRUE):
  1. The "export complete" dialog (every `tr("Saved to {}")` site in `export_results` + `export_comp_report`) offers **Open File** and **Open Folder** actions that launch the saved file / reveal it in Explorer, alongside Close. (EXPUX-01)
  2. Exporting a **LOCAL-only** result set to xlsx does **not** raise the "Domain enrichment pending" warning — that warning fires only when ≥1 **Genizah** sys_id is present (domains are a Genizah-only concept). (EXPUX-02)
  3. A **LOCAL-only** export (any format) **omits** the MiDRASH data-source / Zenodo dataset credit lines; the "Generated by Genizah Search Pro" line and per-export search metadata remain. Mixed and Genizah-only exports keep the full MiDRASH credit unchanged. (EXPUX-03)
  4. DOCX and TXT exports (both Genizah and LOCAL rows, `export_results`) show the **full matched page/chunk text capped at ~2000 chars** with matched terms still highlighted, instead of the ±60-char one-line snippet; missing `full_text` falls back to today's snippet (no regression). (EXPUX-04)
  5. Genizah-only XLSX/CSV exports remain structurally unchanged; `tests/test_export_xlsx_cross_parity.py` stays green with no modification.

**Plans**: Implemented directly (no formal PLAN.md — 4 well-scoped edits, executed as 4 atomic commits with unit tests). See `105-SUMMARY.md`. EXPUX-04 widens the LEXP-08 TXT carve-out (Genizah TXT context expands by design, analogous to Phase 103 D-12's DOCX carve-out; header/marker structure + xlsx/csv unchanged).

---

## v8.0.0 Joins Lab — Phases 106-110 (active; roadmap created 2026-06-03)

> Sequenced per the Codex productionize critique ("C-stricter"): extract pure, unit-tested logic
> first → desktop UI on top → VS merge → search-support. **Web Joins Lab UI is deferred to a LATER
> phase** on the same shared API (desktop-first; the shared core is web-usable from Phase 106). The
> validated throwaway sketch (tag `spike-002-joins-workbench`, frozen at
> `.planning/spikes/002-assisted-join-workbench/sketch/`) is the **executable spec**, NOT the build
> base. **7 deferrals (REQUIREMENTS § Deferrals / discuss-phase questions) MUST be resolved in
> `/gsd-discuss-phase` before each phase's plan locks** — especially the join-model richness (#1),
> dimensions-as-evidence-not-filter (#2), and the JSA-02/03 keep/spike/cut call (#6).

### Phase 106: Joins Lab Shared Core (pure logic + service adapters + unit tests)

**Goal**: A new shared, **web-reusable, unit-tested** module encapsulates the validated Joins Lab
domain logic — anchor/candidate identity, line-by-line query composition into the engine's
line-break syntax, cross-side `(sys_id, page±1)` membership, candidate dedup/compaction, text/visual
merge ordering with provenance, self-match detection, and snippet/page helpers — behind a
`SearchExecutor` adapter and the existing shared services (visual_similarity / FJMS-measurement /
metadata-image), with **no PyQt and no direct `fist_data/*.db` access**. No UI.

**Depends on**: none (foundational). Executable spec: tag `spike-002-joins-workbench`.

**Requirements**: foundational logic for JWB-10, JWB-11, JWB-12 + the architecture build constraints.

**Success Criteria** (what must be TRUE):
  1. A pure function composes a multi-row builder spec (per-row line START/END anchors + "↓ N lines" gaps) into the engine's line-break query (`|` groups, `[|N]` line-gaps), unit-tested for round-trips against `genizah_core._parse_line_break_query` including RTL line-start-on-right.
  2. A pure function resolves the "other side" page set (first→+1, last→−1, middle→both) and decides cross-side AND/OR candidate membership by `(sys_id, page±1)` set logic, unit-tested with AND-narrows / OR-widens fixtures.
  3. Candidate dedup/compaction collapses one-result-per-image via a canonical candidate key (sys_id, page, side image, adjacent-side membership), unit-tested.
  4. The text/visual-similarity merge yields a stable both-first → text → VS-only ordering with provenance tags, unit-tested.
  5. Self-match detection (does the anchor satisfy the composed query) plus the centered snippet/page helpers are unit-tested.
  6. A static import test proves the module imports with no PyQt symbols and opens no `fist_data/*.db` directly — all data flows through shared services or the `SearchExecutor` adapter.

**Plans:** 3/3 plans complete
  - [x] 106-01-PLAN.md — module skeleton: frozen dataclasses + SearchExecutor Protocol + dict→Candidate normalizer + page_of + compose() (SC#1) + static import guard (SC#6)
  - [x] 106-02-PLAN.md — cross-side AND/OR membership via SearchExecutor (SC#2) + candidate dedup/compaction (SC#3) + text/VS merge ordering with provenance (SC#4)
  - [x] 106-03-PLAN.md — self-match detection (SC#5) + centered snippet/page helpers (SC#5)

---

### Phase 107: Desktop Join Workbench — Anchor, Entry Points, Actions & Join Model

**Goal**: A dedicated desktop **"Join Workbench"** opens with a fragment **pinned as anchor** (image
+ numbered transcription, zoom + folio nav, brief metadata, dark-mode/RTL safe), shows the anchor's
**already-known joins as a connected GROUP** (pairwise→group BFS), and exposes **public action
APIs** (Browse / Puzzle / Add-to-List / Add-as-Join) that persist a confirmed join via the existing
pairwise path and refresh the group. Bilingual from the first line. No candidate search yet.

**Depends on**: Phase 106 (shared identity / known-joins helpers).

**Requirements**: JWB-01, JWB-02, JWB-03, JWB-04, JWB-09 + build constraints (public APIs, i18n).

**Success Criteria** (what must be TRUE):
  1. A "Join Workbench" tab/window exists in the desktop app and opens with an anchor pinned via a **"Find joins"** action from the **ResultDialog** and from **Browse**, and **by shelfmark** for a cold start (JWB-01/02).
  2. The anchor's **image** (via the proven `enrich_metadata` → `images_nli/ext` route + `ImageLoaderThread`, not raw FL-substituted thumbnails) and its **numbered transcription** stay in view with zoom ± and folio prev/next; both render correctly in **dark mode and RTL** (JWB-03).
  3. The Workbench lists the anchor's already-known joins as a **connected group** from `JoinsManager.get_connected_fragments_by_id` over PGP + FJMS + user + community joins (JWB-04).
  4. **"Add as Join"** persists a 2-fragment record via the existing `corrections_client.create_join` / `JoinsManager.create_join_local` path (Supabase + `joins_cache.pkl`), and the known-joins group refreshes to include it (JWB-09).
  5. Browse / Puzzle / Add-to-List / Add-as-Join are invoked through **public, named action methods** — **no `_vs_*` private calls** remain on the workbench path.
  6. Every new string is wrapped in `tr()`; the Workbench renders fully in Hebrew under `lang=he` with **no hardcoded English** (acceptance criterion, not cleanup).

**Plans**: 3 plans
- [x] 107-01-PLAN.md — i18n keys + pure helpers (iiif_full/meta_brief/badge_for_source/dedup_join_rows) + 3 Wave-0 test files (unit suite + 2 AST guards: no _vs_*, tr-key coverage) [wave 1]
- [x] 107-02-PLAN.md — JoinWorkbenchWindow shell: modeless QDialog + anchor pane (image route/zoom/folio nav/transcription) + three-source known-joins panel (badges + batched thumbnails, hidden-when-empty) + anchor action-row + per-row actions [wave 2]
- [x] 107-03-PLAN.md — host wiring: open_joins_workbench single-instance + open_anchor_in_puzzle/open_anchor_as_join public wrappers + ResultDialog & Browse Find-joins entry hooks + full-suite/ruff gate [wave 3]

---

### Phase 108: Desktop Join Workbench — Query Builders, Candidates & Compare

**Goal**: The scholar drives the hunt — a **line-by-line query builder** for the anchor side and an
**identical builder for the OTHER side** of the leaf (cross-side AND/OR), running the **existing**
search engine; results return as **deduped one-per-image candidates** in grid + table views with
material + highlighted snippet + Y/?/N triage + a self-match readout, plus side-by-side
anchor↔candidate compare. The conservative `[`/`]` tear-side assist is **DEFERRED out of this phase**
(2026-06-04 discuss-phase) to the algorithmic Component B (Phase 110 disposition) — 108 is the
*manual* finder.

**Depends on**: Phase 106 (composition / membership / dedup / merge logic) + Phase 107 (Workbench shell + actions).

**Requirements**: JWB-06 (reframed), JWB-07, JWB-08, JWB-10, JWB-11 + the text/combined candidate surface of JWB-12. (JWB-05 tear-side assist **deferred** to Phase 110 disposition — 2026-06-04 discuss-phase.)

**Success Criteria** (what must be TRUE):
  1. A line-by-line builder (rows = lines; per-row line START/END anchors with the START anchor on the **right** for RTL; "↓ N lines" gap) composes and runs a line-break query through the existing engine — hunting the **missing continuation**, NOT pre-seeding the anchor's own line text (JWB-10, JWB-06 reframed).
  2. An identical builder for the **OTHER side** (adjacent image p±1; first→+1, last→−1, middle→both) runs query B; **AND narrows** a flood and **OR widens** a poor yield, via `(sys_id, page±1)` membership (JWB-11).
  3. Candidates render **deduped one-per-image** in both grid and table views, each with material, a **highlighted matched-text snippet**, and **Y/?/N triage**; a refine/filter bar filters by text / material / has-dimensions / triage (JWB-07; JWB-12 surface).
  4. A **self-match readout** shows whether the anchor itself satisfies the current query (✓/✗) and an **"include anchor itself"** toggle works (JWB-12 verification — fixes the "can't find the fragment itself" trap).
  5. Selecting a candidate shows it **side-by-side** with the anchor (image + transcription) for eyeball confirmation, with the four actions available (JWB-08).
  6. ~~The tear-side assist reads the anchor's `[`/`]` markers…~~ **DEFERRED out of Phase 108** (2026-06-04 discuss-phase) — JWB-05 moves to the algorithmic Component B (Phase 110 disposition). NOT a Phase 108 success criterion. (Bracket-aware *matching* correctness — a leading `]` must not defeat line-start / self-match — stays in scope; only the side-verdict UI is deferred.)
  7. Manuscript dimensions appear as **evidence / soft warnings, never an *automatic* hard filter** (deferral #2 — **CONFIRMED** in discuss-phase: soft evidence + mismatch hint by default, PLUS an *opt-in* explicit min/max size cull, off by default).

**Plans**: 4 plans
- [x] 108-01-PLAN.md — Wave-0 scaffolds: extend get_measurement_summaries_batch (+size_category, D-21/RR-6/RR-11) + TabularQueryBuilderDialog RTL-chrome fix (D-06) + builder/triage/RTL test stubs (parser-level OR + hoisted-modifier round-trip) [wave 1]
- [x] 108-02-PLAN.md — JoinQueryBuilder widget (line-by-line, slash-group OR `(a/b)` with per-row hoisted modifiers `#(a/b)`, page-position, modifier row, Preview) + _DesktopSearchExecutor adapter (JWB-06/10/11 input) [wave 2]
- [x] 108-03-PLAN.md — Candidate surface: JoinCandidatePane + CandidateCard + batched workers + source selector + self-match readout + grid/table + triage + public actions (JWB-07/10/11/12) [wave 3]
- [x] 108-04-PLAN.md — CompareDialog side-by-side (JWB-08) + matched-page open + in-dialog actions/triage/re-anchor + full-suite/ruff phase gate [wave 4]

---

### Phase 109: Visual-Similarity Merge & Soft-Retire

**Goal**: The candidate surface gains the **visual-similarity look-alike source** and a **combined
view** (provenance badges ★both / ⊙VS / ✎text, both-first ordering) via the shared VS service; the
standalone **Visual Similarity dialog's entry points are rerouted** into the Workbench and the old
dialog is **marked removable** after a parity verification pass (the JoinsDialog pick-mode hook is
preserved).

**Depends on**: Phase 106 (merge logic) + Phase 108 (candidate surface).

**Requirements**: JWB-12 (VS source + combined view + soft-retire).

**Success Criteria** (what must be TRUE):
  1. "Visual similarities" and "Search + visual" sources populate the candidate surface from the **shared visual_similarity service**, with provenance badges and both-first ordering (JWB-12).
  2. The standalone Visual Similarity dialog's entry points **route into the Workbench**; the old dialog is **deprecated (marked removable)** after a parity verification pass; the JoinsDialog **pick-mode hook still works**.
  3. Per-candidate enrichment (browse text / measurement / thumbnail / snippet / membership) is **batched**, not per-candidate-serial, verified on an ~80-candidate VS load (Codex perf risk).

**Plans**: 13 plans (3 executed + 4 gap-closure round-2 G-01..G-05 + 6 gap-closure round-3 G-06..G-13 after UAT REJECTED again)
- [x] 109-01-PLAN.md — Wave-0 VS adapter shim + i18n keys + tests
- [x] 109-02-PLAN.md — Visual/Combined source selector + source-aware merge + badges + parity test
- [x] 109-03-PLAN.md — reroute Browse/ResultDialog entry points + deprecation marker + UAT scaffold
- [x] 109-04-PLAN.md — G-01: fix Hebrew חיצוני→חזותי + pre-seed gap-round tr() keys (Wave 1)
- [x] 109-05-PLAN.md — G-04/G-03/G-02: single Visual-Similarity toggle (replaces 3 radios) + intersection assemble + page-lazy VS card text (Wave 2)
- [x] 109-06-PLAN.md — G-05: reroute JoinsDialog pick-mode into the Workbench + tooltip + deprecation re-eval (Wave 3)
- [x] 109-07-PLAN.md — re-UAT checkpoint (toggle states + deferred scenarios; gates the deprecation marker, Wave 4)
- [x] 109-08-PLAN.md — gap-round-3: pre-seed EN+HE tr() keys (eye tooltip / G-13 hint+empty / G-08 link tooltip) (Wave 1)
- [x] 109-09-PLAN.md — G-06/G-09/G-12: single eye 👁 badge (drops ★both/⊙VS/rank) + eye-prefixed toggle + visible-ON :checked style (Wave 2)
- [x] 109-10-PLAN.md — G-07: remove duplicate Browse + ResultDialog VS buttons; mark reroute handlers removable (Wave 3)
- [x] 109-11-PLAN.md — G-10/G-11/G-13: triage second-click undo + merged folio+triage row + VS hint line & combined empty message (Wave 4)
- [x] 109-12-PLAN.md — G-08: JoinsDialog VS button → 🔗 plain Join-Lab open + close (reverses G-05); pick machinery marked removable; _show_vs_dialog fully unreferenced (Wave 5)
- [x] 109-13-PLAN.md — gap-round-3 re-UAT checkpoint (eye badge + G-07..G-13 + deferred K/L/M); owns the deprecation-marker flip (Wave 6, autonomous:false)

---

### Phase 110: Search-Support — Parallels Seeding (JSA-01)

**Goal**: From the anchor passage the scholar **seeds a parallels (composition) search** to surface
shared-distinctive-phrase candidates across the corpus, returned into the Workbench candidate
surface. JSA-02 (corpus-completion), JSA-03 (torn-word completion), and **JWB-05 (the `[`/`]`
tear-side assist, deferred here from Phase 108 — both are `[`/`]`-driven algorithmic features) are
resolved at discuss-phase: keep / spike / cut.

**Depends on**: Phase 108 (candidate surface).

**Requirements**: JSA-01 (JSA-02 / JSA-03 / **JWB-05** — discuss-phase disposition; earlier lean: JSA-01 only, spike JSA-03 + JWB-05, cut JSA-02).

**Success Criteria** (what must be TRUE):
  1. A "Find parallels" action **seeds the existing composition/parallels search** from the anchor passage and returns shared-distinctive-phrase candidates into the Workbench candidate list (JSA-01).
  2. The JSA-02 / JSA-03 disposition is **recorded** (implemented, spiked, or cut) per the discuss-phase decision, with the rationale captured.

**Plans**: TBD.

---

> **Deferred to a later phase / milestone:** **Web Joins Lab UI** on the shared Phase-106 API
> (desktop-first per the productionize critique — building two UIs before the model stabilizes
> doubles QA). Web parity timing is deferral #5.

---

<details>
<summary>✅ v7.16 Hebrew PDF Text Quality (Phase 102 + no-phase quality work) — SHIPPED 2026-06-01</summary>

See: .planning/milestones/v7.16-ROADMAP.md

1 formal phase (102, 5 plans) + post-phase no-phase quality work. Rewrote the LOCAL ("My Library") Hebrew PDF text-layer extractor onto a `page.get_text("rawdict")` per-glyph foundation (`shared/local_indexer_rtl.py`): RTL-gated reorder (Meiri core, no LTR regression), Unicode-`Mn` nikud/maqaf classification, per-line 1-D Otsu word-gap de-space, `_ltr_damage_guard` RTL-trust fix, corrupt_encoding detection; `extraction_format_version` 2→3. Emphasis letter-spacing no longer shatters Hebrew words (אוצר הגאונים single-letter tokens 73.5%→~3-5%) and tight typesetting no longer fuses phrases (רביצקי 15.8%→0.07%). No-phase work bundled: de-space follow-ups (D-F13b/c/d), LOCAL UAT extraction fixes (HTML/xlsx/CSV + folder opt-out cascade BLOCKER, D-F19..D-F22/D-F25), file-management actions for LOCAL hits (D-F24), and three search/startup freeze fixes (D-F23: 778 MB `search_history.json`, large-folder O(n²) startup, LAB-rebuild churn). Shipped v7.16.0 desktop-only (tag `v7.16.0`, GitHub Release with installer, CI green).

</details>

<details>
<summary>✅ v7.15 My Library Visual (Phases 99-101) — SHIPPED 2026-05-28</summary>

- [x] Phase 99: PDF Page Renderer (2/2 plans) — completed 2026-05-27
- [x] Phase 100: LOCAL PDF Image in ResultDialog + Browse (3/3 plans) — completed 2026-05-27
- [x] Phase 101: LOCAL PDF Text Extraction RTL Fix + Phase 100 Remnant Cleanup (2/2 plans + UAT follow-ons) — completed 2026-05-28

</details>

<details>
<summary>✅ v7.14 My Library — Local Document Search (Phases 95-98) — SHIPPED 2026-05-24, closed 2026-05-27</summary>

See: .planning/milestones/v7.14-ROADMAP.md

6 phases (95, 96, 97, 97.2 INSERTED, 97.3 INSERTED, 98), 37 plans. Desktop-first "My Library" tab indexing user folders of `.docx`/`.pdf`/`.txt`/`.html`/`.xlsx`/`.csv` into a separate Tantivy side-index merged into Search/Composition/Parallels via RRF k=60 POST-dedup, with a `LOCAL` badge, a corpus selector, and three cloud-write gates keeping personal corpora off the cloud. Public v7.14.0 release 2026-05-24 (Phase 95 MVP + Phase 96 completion). Internal hotfix chain through 2026-05-27: Phase 97 (scale to 13K files / 43 GB + `.html`/`.xlsx`/`.csv` + atomic rebuild + crash recovery), Phase 97.2 INSERTED (recovery cascade + Reset My Library), Phase 97.3 INSERTED (mega-folder UI-thread stability). Phase 98 (web infra) added a shared NLI circuit breaker wired into all 10 NLI/IIIF fetch sites, dropping worst-case per-request blocking 45s → ~9s (closes the 2026-05-25 production hang). v7.12 multitenant invariants preserved (zero raw `app.storage.user` under `web/`; LOCAL never reaches web/API/Supabase).

</details>

<details>
<summary>v7.13 Research-Grade Downloads & PGP Filter (Phases 93-94) -- BOTH PHASES COMPLETE (Phase 93 2026-05-19; Phase 94 2026-05-21; milestone closeable)</summary>

See: .planning/milestones/v7.13-ROADMAP.md

2 phases, 5 plans (Phase 93: 1; Phase 94: 4 waves), 14/14 requirements satisfied (5 PGP-FILTER + 9 EXPORT-META). Both phases promoted from backlog (999.2 + 999.3). Phase 93 (PGP Filter on `/search`, web-only) shipped 2026-05-19 with 4/5 PGP-FILTER reqs directly satisfied; PGP-FILTER-03 (chip) superseded by user smoke direction (colored button label already conveys state). Phase 94 (Research-Grade Export Metadata, web + desktop xlsx) shipped 2026-05-21 after a 4-wave implementation (94-01 shared dossier primitives → 94-02 web state plumbing + JSON envelope → 94-03 web xlsx restructure → 94-04 desktop xlsx parity + smoke verification + docs closeout). The final workbook is 4-sheet (`Search Results` + `Manuscripts` + `Bibliography` + `Credits and Info`) bilingual (lang='he' produces Hebrew sheet titles + headers + Hebrew-preferred metadata, lang='en' produces English everywhere) with clickable Manuscripts URLs, deduped Domains, int Image/Page values, and a 4th Credits-and-Info sheet carrying search metadata + GenizahSearch.com hyperlink + Creator credit — refined across 6 rounds of smoke-verification patches (bilingual headers + source-language metadata + sheet rename + 4th sheet + label realignment + clickable URLs + Domains dedupe + Image/Page int coercion). On web only, JSON gains 3 additive per-item flags (`has_pgp`, `is_printed`, `domains`) with envelope `schema_version` unchanged; desktop has no JSON export. CONTEXT D-04 was REVERSED 2026-05-20 for the row content layer only (the D-02 transcription-text prohibition + D-10 parallels-envelope strip + conditional RTL view-direction are UNCHANGED). v7.12 multitenant invariants carry forward (zero raw `app.storage.user` under `web/`; allowlist still `[]`).

</details>

<details>
<summary>v1 External Data Integration (Phases 1-7) -- SHIPPED 2026-02-07</summary>

See: .planning/milestones/v1-ROADMAP.md

9 phases (including inserted 7.1, 7.2), 18 plans, 173 min total execution.
Imported 7,090 PGP documents with 9,364 transcription/translation sources.
Full PGP feature set in web app.

</details>

<details>
<summary>v5.6.0 Desktop Parity & PGP Integration (Phases 8-12) -- SHIPPED 2026-02-09</summary>

See: .planning/milestones/v5.6.0-ROADMAP.md

5 phases, 25 plans, ~134 min total execution.
Desktop PGP feature parity, Virtual Reading Desk, 35,839 PGP documents imported.
Phase 13 (Transcription Search) deferred -- index build too slow for desktop.

</details>

<details>
<summary>v5.7.0 Responsa Search (Phases 14-17) -- SHIPPED 2026-02-10</summary>

See: .planning/milestones/v5.7.0-ROADMAP.md

4 phases, 14 plans.
Responsa Project-style advanced search with syntax parsing, JA expansion, tabular query builder, explosion guards.
25/25 requirements satisfied. 221 automated Responsa tests.

</details>

<details>
<summary>v5.7.2 Cleanup, Normalization & Sections (Phases 18-21) -- SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.2-ROADMAP.md

4 phases, 11 plans.
Dead AI code removed, Unicode search normalization, full green test suite (447 tests),
structural HTML section parser for PGP transcriptions.
13/13 requirements satisfied.

</details>

<details>
<summary>v5.7.3 Pending Corrections Visibility (Phases 22-24) -- SHIPPED 2026-02-11</summary>

See: .planning/milestones/v5.7.3-ROADMAP.md

3 phases, 3 plans.
Pending corrections visible as selectable version in web and desktop version selectors.
Shared corrections service, amber styling (web), emoji labels (desktop).
6/6 requirements satisfied. 20 milestone-specific tests.

</details>

<details>
<summary>v5.8.0 FJMS Integration (Phases 25-28) -- SHIPPED 2026-02-15</summary>

See: .planning/milestones/v5.8.0-ROADMAP.md

4 phases, 12 plans.
FJMS scholarly metadata (domains, joins, catalog) integrated via SQLite sidecar.
Domain filtering, scientific joins with scholar attribution, catalog enrichment in both apps.
19/19 requirements satisfied. 38+ tests covering service layer and integration.

</details>

<details>
<summary>v5.9.0 Multi-Source Image & Metadata Integration (Phases 29-34) -- SHIPPED 2026-02-16</summary>

See: .planning/milestones/v5.9.0-ROADMAP.md

6 phases, 22 plans (including 3 gap closure plans), 76 commits.
NLI crossref sidecar (815K records), Cambridge IIIF (141K), Manchester LUNA (28K), JTS/Princeton Figgy (453).
Multi-source image viewing with folio navigation, bibliography (542K), catalog refs (64K), physical metadata.
11/14 requirements satisfied, 1 invalidated (FGP!=FL), 2 deferred (REL-01/REL-02).

</details>

<details>
<summary>v6.0.0 Local Data Architecture (Phases 35-40) -- SHIPPED 2026-02-22</summary>

See: .planning/milestones/v6.0.0-ROADMAP.md

6 phases, 21 plans (8 core + 8 bug-fix/cleanup + 5 performance optimization), 155 commits.
PGP data migrated to local pgp.db sidecar (147MB). FJMS catalog descriptions expanded (4 new tables, ~1.7M rows).
Desktop offline PGP browsing. All desktop crashes fixed. Paginated search (PAGE_SIZE=50).
Performance: parallel NLI fetch, browse crossref parallelization, FL ID index, variant cache unification.
14/14 requirements satisfied (audit passed).

</details>

<details>
<summary>v6.1.0 Catalog Browse & Navigation (Phase 41) -- SHIPPED 2026-02-27</summary>

1 phase, 4 plans.
Faceted browsing by domain hierarchy, author, and work title in both apps.
FIST v5.0.0 enrichment (genizah_persons, genizah_titles, code_values), FTS5+domain text filter,
cross-links between browse and catalog browse pages. 72 tests.

</details>

<details>
<summary>v6.5.0 Search UX & Filtered Search (Phases 42-46) -- SHIPPED 2026-03-14</summary>

See: .planning/milestones/v6.5.0-ROADMAP.md

5 phases, 26 plans, 244 commits.
Search UX overhaul (timer, ETA, partial results, printed filter), session persistence,
Hebrew library names, bidirectional filtered search (domain/author/work/date/material),
~580K Dicta translations for multilingual access. Origin: power user feedback letter (17 requests).

</details>

<details>
<summary>v7.0.0 Fragment Puzzle (Phases 47-52) -- SHIPPED 2026-03-17</summary>

6 phases, 15 plans.
Visual jigsaw tool for assembling physical joins from manuscript fragment images with background removal,
DPI calibration, recto/verso views, join document persistence, and community publishing --
in both web (NiceGUI + Fabric.js) and desktop (PyQt6 + QGraphicsScene).

</details>

<details>
<summary>v7.1.0 FIST Gap Fill (Phase 53) -- SHIPPED 2026-03-19</summary>

1 phase, 2 plans.
Added 38,673 Genizah manuscripts from FIST.db that were missing from libraries.csv.
Browsable with images and FJMS enrichment. Metadata search guard fix. 7 new library codes.

</details>

<details>
<summary>v7.6 Search Refinement & Scholarly Joins (Phases 54-57) -- SHIPPED 2026-03-31</summary>

See: .planning/milestones/v7.6-ROADMAP.md

5 phases (+ 55.1 inserted), 17 plans, 206 commits, 151 files changed (+28K/-3.7K lines).
Manuscript dimensions display + filtering, search within results with breadcrumb chain,
exclude known manuscripts (lists/files/paste), FIST visual similarity browse + search mode,
lightweight browse first-render. 14/14 requirements satisfied.

</details>

<details>
<summary>v7.7 Volume-Aware Browse (Phases 58-61) -- SHIPPED 2026-04-01</summary>

4 phases, 8 plans, 13 commits.
Fixed multi-IE image/text mismatch for 3,193 manuscripts (1.5%) by making search->browse->paging
IE-aware across both apps. IE volume data infrastructure, web + desktop volume selector dropdown,
per-IE paging, volume-correct images for external providers (Manchester/Oxford/Cambridge/JTS),
auto-default to external sources when NLI is down, session persistence for active volume,
community writes (corrections/comments) include IE context.

</details>

<details>
<summary>v7.8 Structural Foundation (Phases 63-66) -- SHIPPED 2026-04-15</summary>

See: .planning/milestones/v7.8-ROADMAP.md

4 phases, 9 plans, 64 commits, 173 files changed (+6,269/-828 lines).
CI safety net with GitHub Actions (Ubuntu + Windows matrix, ruff + check_docs + pytest),
two-file dependency pinning (14 direct + 115 transitive), Supabase auth migration
(gotrue -> supabase_auth, PKCE-only OAuth), 205+ silent exception handlers audited across
76 first-party files, isolated NiceGUI monkey-patches with version guards, repo root
cleanup (.gitignore 50->126 lines, untracked root 67->1), documentation refresh
(CODE_INDEX, OPEN_ISSUES, DEVELOPER_GUIDE). 12/12 requirements satisfied.
Zero user-visible behavior changes.

</details>

<details>
<summary>v7.9 Decomposition (Phases 67-76) -- COMPLETE 2026-04-17</summary>

10 phases, 23 plans.
Decomposition of largest source files into focused modules. Desktop split: ResultDialog,
filter/scholarly dialogs, image viewers (ManuscriptViewerWidget, FullscreenImageWindow),
puzzle canvas, VS cache, widgets extracted into desktop/ package. Web split:
search.py -> search_state.py + search_results.py; browse.py -> browse_state.py + browse_enrichment.py.
Page-scoped state refactor reducing app.storage.user sprawl. Back-navigation state-loss bugfix
(regression from 2026-03-27 commit 829cd7cf). Zero user-visible behavior change except the
back-nav bugfix.

</details>

<details>
<summary>v7.10 Search API (Phases 77-83) -- SHIPPED 2026-05-05</summary>

See: .planning/milestones/v7.10-ROADMAP.md

8 phases (77, 78, 79, 80, 81A, 81B, 82, 83), 37 plans.
Public HTTP/JSON research-automation API over the Genizah corpus: `/api/search` (keyword/Responsa/title/shelfmark with rate limiting, mode gating, error envelope), `/api/browse` (stateless drill-down returning text + metadata + image URLs), `/api/parallels` (composition matching). Security hardening (XFF spoofing, fail-closed filter validation, MAX_EXPANDED_TERMS=500, HMAC-hashed PostHog telemetry). OpenAPI auto-generated at `/api/openapi.json` + Swagger at `/api/docs`. Reference Claude skill `cairo-genizah-research` (file-locked token-bucket throttling, browse-honesty annotations). 36/36 in-traceability requirements + 8 PUBLIC-* satisfied (deployed to production 2026-05-05). Web-only release: no git tag, no GitHub Release.

</details>

<details>
<summary>v7.11 CUDL Coverage & Synthetic Inventories (Phases 84-86) -- SHIPPED 2026-05-12</summary>

3 phases, 14 plans (84: 5/5, 85: 5/5, 86: 4/5 executed + optional release plan).
FIST-CUDL bridge (shared/fist_cudl_bridge.py + shared/shelfmark_bridge.py) with normalizers for Mosseri label form, Cambridge Or. numeric collapse, CUL slash/comma/dot/leading-zero fixes; 6 wiring call sites. Synthetic libraries.csv infrastructure: is_synthetic_sys_id helper, Option-2 18-digit format, browse hide-NLI gates, is_synthetic on API responses, corrections-write reject. 108 image-bearing synthetic manuscripts injected (101 CUL + 7 Mosseri). T-S NS 329.96 (originating case) resolved. 5-tier CUDL coverage audit (96.23% phase84_hit, 0.08% synthetic, 1.13% residue needing human-in-loop). Deploy posture codified: scp DBs FIRST, then push code.

</details>

<details>
<summary>v7.12 Multitenant Architecture (Path B) (Phases 87-92 + 92.1 + 92.2 + promoted 999.1/999.4) -- SHIPPED 2026-05-18</summary>

See: .planning/milestones/v7.12-ROADMAP.md

10 phases (87, 88, 89, 90, 91, 92, 92.1 INSERTED, 92.2 INSERTED, 999.1 promoted, 999.4 promoted), 28 plans, 49/49 requirements satisfied (38 v7.12 core + 11 promoted backlog).
Refactored GenizahSearch's web layer off the desktop-inherited single-user mental model. 131 raw `app.storage.user` accesses migrated through `web/safe_storage.py` chokepoint with allowlist driven to 0 entries. State separation by deletion (10 AppState mirror fields gone), per-request `UserListsManager`, request-scoped auth with NO `set_session()` mid-flight (Codex constraint at `gotrue_client.py:713` respected), `_session_uuid`-keyed refresh locks, real server-side `sign_out` revocation. Phase 92.1 (INSERTED) closed P0 RLS-reachability regression by migrating 12 reader call sites from anonymous singleton to `get_user_client()`. Phase 92.2 (INSERTED) closed `/lists` 36s warm-render regression via task-scoped `WeakKeyDictionary` memo (19.3x mean speedup). 5-surface SWEEP audit clean. `docs/guides/MULTITENANT.md` shipped as architecture reference. Promoted backlog: Phase 999.1 (search-result folio chip parity) + Phase 999.4 (line-number gutter in both apps).

</details>

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|-------|-----------|----------------|--------|-----------|
| 106. Joins Lab Shared Core (logic + adapters + tests) | v8.0.0 | 3/3 | Complete    | 2026-06-03 |
| 107. Desktop Join Workbench — Anchor, Entry, Actions & Join Model | v8.0.0 | 3/3 | Complete    | 2026-06-04 |
| 108. Desktop Join Workbench — Builders, Candidates & Compare | v8.0.0 | 7/4 | Complete    | 2026-06-06 |
| 109. Visual-Similarity Merge & Soft-Retire | v8.0.0 | 13/13 | Complete   | 2026-06-08 |
| 110. Search-Support — Parallels Seeding (JSA-01) | v8.0.0 | 0/? | Not started | - |
| 103. Search-Results LOCAL Export | v7.17→v8.0.0 | 4/4 | Complete    | 2026-06-01 |
| 104. Composition-Report LOCAL Export | v7.17 | 0/0 | ⏸ Deferred (no LOCAL comp-search UI) | - |
| 105. Export UX Polish | v7.17 | 4/4 EXPUX | Implemented (tests green; UAT pending) | 2026-06-01 |
| 99. PDF Page Renderer | v7.15 | 2/2 | Complete | 2026-05-27 |
| 100. LOCAL PDF Image in ResultDialog + Browse | v7.15 | 3/3 | Complete | 2026-05-27 |
| 101. LOCAL PDF RTL fix + remnant cleanup | v7.15 | 2/2 | Complete | 2026-05-28 |
| 102. LOCAL PDF Text-Layer Extraction Rewrite | v7.16 | 5/5 | Complete | 2026-05-29 |

## Backlog

No active backlog entries for future milestones.

Next-milestone candidates (not in v7.17 scope): **D-F12** (regular Search ~8s wall-clock investigation), **D-F18** (context-menu LOCAL detection normalize through `display`) — see `docs/OPEN_ISSUES.md`.

---

*Roadmap created: 2026-02-09*
*Last updated: 2026-06-03 — **v8.0.0 Joins Lab roadmap created**: Phases 106-110 added (106 shared core + tests; 107 desktop Workbench frame + actions + pairwise→group join model; 108 desktop builders + candidates + compare; 109 Visual-Similarity merge + soft-retire; 110 parallels seeding / JSA-01). Codex extract-pure-logic-first sequence; web UI deferred to a later phase. v7.17 folded into v8.0.0 (Phases 103/105 delivered). 7 deferrals (REQUIREMENTS § Deferrals) to resolve in `/gsd-discuss-phase` before each phase's plan locks; the validated sketch is preserved as the executable spec at tag `spike-002-joins-workbench`. NEXT: `/gsd-discuss-phase 106` → `/gsd-plan-phase 106`.*
*Prior update: 2026-06-01 — Phase 105 (Export UX Polish) ADDED to v7.17: 4 export-experience fixes (EXPUX-01 Open File/Folder dialog; EXPUX-02 LOCAL xlsx domain-warning suppression; EXPUX-03 LOCAL-only MiDRASH-credit omission; EXPUX-04 full-text capped context in DOCX/TXT). Phase 104 DEFERRED during /gsd-discuss-phase 104. Discovered Composition Search has no LOCAL corpus path (corpus selector is Search-tab-only at genizah_app.py:5924; CompositionThread at :21532 gets no corpus_scope), so export_comp_report has no LOCAL surface to adapt. LEXP-02 moved to REQUIREMENTS.md Future (EXP-F3), gated on a LOCAL composition-search UI. v7.17 now = Phase 103 only and is closeable. Prior: Phase 103 COMPLETE & verified 6/6 (LEXP-01/03/04/05/06/07/08). v7.16 Hebrew PDF Text Quality CLOSED 2026-06-01 (Phase 102 + no-phase work; tag v7.16.0 @ ccb87c90).*
