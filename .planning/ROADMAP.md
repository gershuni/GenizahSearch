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
- **v8.0.0 Dicta Rebrand & Joins Lab** -- BRAND (no-phase) + Phases 103, 105 (folded from v7.17; Phase 104 → EXP-F3) + Phases 106-110 Joins Lab (shipped 2026-06-09; closed 2026-06-11). Component B (JSA-01/02/03 + JWB-05) + web Joins Lab UI deferred post-v8.0.0. See `milestones/v8.0.0-ROADMAP.md`
- **v8.1.0 Desktop Telemetry** -- Phases 111-116 (shipped 2026-06-16). See `milestones/v8.1.0-ROADMAP.md`

## Phases

<details>
<summary>✅ v8.1.0 Desktop Telemetry (Phases 111-116) — SHIPPED 2026-06-16</summary>

See: .planning/milestones/v8.1.0-ROADMAP.md

6 phases (111-116), 20 plans, 32 tasks. Git range `v8.0.0` → `v8.1.0` (228 commits); 316 files, +40,844 / −1,907; 2026-06-11 → 2026-06-16.

Opt-in, privacy-preserving desktop telemetry → the shared web PostHog project (id 134161, EU), identity-aligned with web (`platform=desktop`); default OFF, bilingual consent, never search/My-Library content (enforced by the `desktop/telemetry.py` chokepoint + scrubber + property allowlist + fixed event registry + CI AST guard). Phases: 111 foundation (`desktop/telemetry.py` + neutral `shared/posthog_server.py` additions, no `posthog` SDK), 112 consent UX, 113 crash reporting (chained `sys`/`threading` hooks + faulthandler), 114 usage analytics (web-aligned identity → Supabase `_uuid`), 115 performance summaries (bucketed, per-session flush), 116 privacy audit + CI gate + frozen-exe `SSL_OK`. Also bundled into the release: desktop "Public API & AI Tools" advertising + web public-Search-API enhancements (quick task 260616-p9x) + the `platform=web` super-property. Tagged `v8.1.0` @ `e7382977` (both apps; GitHub Release with installer; web deployed). Closed 2026-06-16.

</details>

<details>
<summary>✅ v8.0.0 Dicta Rebrand & Joins Lab (Phases 103, 105 + 106-110) — SHIPPED 2026-06-09, closed 2026-06-11</summary>

See: .planning/milestones/v8.0.0-ROADMAP.md

7 phases — 103 + 105 (folded from the v7.17 cycle) + 106-110 (Joins Lab Component A). Phase 104 deferred → EXP-F3 (delivered in 110). 31 formal plans (35 completed plan-equivalents incl. 108 redesign/polish + 109 gap rounds). Git range `v7.16.0` → `v8.0.0` (328 commits); 266 files, +55,320 / −785; 2026-06-01 → 2026-06-09.

The flagship **"Dicta Genizah Search Pro"** release: the desktop **rebrand** (display-only; binary identifiers unchanged so installs upgrade in place) + LOCAL ("My Library") **export** support (Phases 103 + 105, closes D-F17) + the new **Joins Lab** — an interactive, human-in-the-loop join-hunting workbench (desktop). Phase 106 shared core (`shared/joins_lab.py`, web-reusable, no PyQt / no direct `fist_data`, `SearchExecutor` adapter); Phases 107-108 the desktop Join Workbench (anchor pane + line-by-line query builders for both sides of the leaf + deduped candidate grid/table + side-by-side Compare + pairwise→group join model + public action APIs); Phase 109 merged Visual Similarity into the candidate surface (single 👁 eye badge + Visual Similarity toggle; standalone VS dialog soft-retired; "Find Joins" is the single entry); Phase 110 added Composition Search over the LOCAL corpus (Genizah/Local/ALL selector orthogonal to Lab mode; standard LOCAL composition uses the REGULAR My-Library index not the LAB side-index; score-interleaved merge, no RRF) + LOCAL-aware `export_comp_report` (EXP-F3). 25 requirements satisfied (BRAND 2 + LEXP 7 + EXPUX 4 + JWB 9 + COMP-LOC 2 + EXP-F3 1). **Deferred by user decision (2026-06-08):** all of Component B (JSA-01/02/03 + JWB-05) and the web Joins Lab UI → post-v8.0.0. Tagged `v8.0.0` @ `71e0912e` (both apps; GitHub Release with installer). Close ritual run retroactively 2026-06-11.

</details>

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
