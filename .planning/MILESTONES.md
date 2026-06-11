# Project Milestones: GenizahSearch

## v8.0.0 Dicta Rebrand & Joins Lab (Shipped: 2026-06-09; closed 2026-06-11)

**Phases completed:** 7 phases — 103 + 105 (folded from the v7.17 cycle) + 106, 107, 108, 109, 110 (Joins Lab Component A). Phase 104 deferred → EXP-F3 (delivered in 110).
**Plans:** 31 formal plans (35 completed plan-equivalents incl. 108 redesign/polish + 109 gap rounds)
**Git range:** `v7.16.0` → `v8.0.0` (328 commits)
**Scope:** 266 files changed, +55,320 / −785
**Timeline:** 2026-06-01 → 2026-06-09 (9 days)
**App releases:** v8.0.0 (2026-06-09, both apps), tagged `v8.0.0` @ `71e0912e` (GitHub Release with desktop installer)

**Delivered:** The flagship **"Dicta Genizah Search Pro"** release. Bundles the delivered v7.17 cycle — the desktop **rebrand** (display-only; binary identifiers unchanged so installs upgrade in place) and LOCAL ("My Library") **export** support — with the new **Joins Lab**: an interactive, human-in-the-loop join-hunting workbench (desktop) where a scholar keeps one anchor fragment in view (image + numbered transcription) and drives the app's existing search tools to find the fragments that physically join it. There is NO automated join-finder — the scholar is the ranker and confirmer. Also adds Composition Search over the Local corpus (Genizah/Local/ALL selector orthogonal to Lab mode) + LOCAL-aware composition export (EXP-F3). Web Joins Lab UI deferred to a later phase on the same shared Phase-106 API. All of Component B (search-support algorithms JSA-01/02/03 + the JWB-05 tear-side assist) was deferred to a post-v8.0.0 milestone (2026-06-08) so the milestone could ship.

**Key accomplishments:**

- **Rebrand → "Dicta Genizah Search Pro" (BRAND-01/02, pre-release polish):** desktop display name updated across window title, About (EN+HE), updater, exported-file credits, puzzle PNG footer, version metadata, installer, README/CHANGELOG, web download-page title — **DISPLAY-only**; binary identifier `GenizahSearchPro` (exe/.spec/.iss/dist/auto-update) UNCHANGED so installs upgrade in place; web brand "Dicta Genizah Search" unchanged. Closed 223 desktop+web i18n gaps / 246 keys during the polish pass.
- **Search-results LOCAL export (Phase 103, LEXP-01/03–08):** export a mixed/LOCAL-only result set across XLSX/CSV/TXT/DOCX with local-meaningful columns (filename/folder/filepath/page/matched-text) and a dedicated bilingual "Local Documents" xlsx sheet; Genizah sub-sheets exclude LOCAL; DOCX redesigned into per-result rich blocks (by design); Genizah-only XLSX/CSV/TXT cross-parity preserved. Closes **D-F17**.
- **Export UX polish (Phase 105, EXPUX-01..04):** Open File/Folder dialog on export complete; LOCAL-only exports suppress the Genizah domain-warning + omit MiDRASH/Zenodo credits; DOCX/TXT show full matched text capped ~2000 chars.
- **Joins Lab shared core (Phase 106):** new web-reusable, unit-tested `shared/joins_lab.py` (~750 lines, 66 tests) — anchor/candidate identity, line-by-line query composition, cross-side `(sys_id, page±1)` membership, dedup/compaction, text/VS merge with provenance, self-match + snippet helpers — behind a `SearchExecutor` adapter, no PyQt / no direct `fist_data` (Codex extract-pure-logic-first constraint).
- **Desktop Join Workbench (Phases 107–108):** modeless anchor-pinned window (`desktop/join_workbench.py`, ~1.5K lines) opened via "Find joins" from ResultDialog + Browse + cold-start by shelfmark; anchor pane (image + numbered transcription, zoom/folio nav) + connected known-joins group (pairwise→group BFS, no new schema); a line-by-line query builder for BOTH sides of the leaf (per-row OR word-boxes + per-line ⚙ modifiers + global toggles + inline gap), candidates as deduped one-per-image grid + 8-column table with triage Y/?/N, side-by-side Compare, self-match readout, Add-as-Join / Add-to-Puzzle (anchor included). All actions via public APIs (no `_vs_*` on the workbench path); bilingual from line one.
- **Visual-Similarity merge & soft-retire (Phase 109, JWB-12):** the candidate surface absorbed the visual-similarity look-alike source via the shared VS service; a single 👁 eye badge (grid/table/Compare) + a single "Visual Similarity" toggle replaced the standalone VS dialog and the 3-radio selector; "Find Joins" became the single entry; `_show_vs_dialog` deprecated/marked-removable (one-cycle soft-retire). Enrichment batched (one IN-query).
- **Composition Search over the Local corpus (Phase 110, COMP-LOC-01/02 + EXP-F3):** pre-search Genizah/Local/ALL corpus selector on the composition tab, **orthogonal to Lab mode** (Lab no longer hardwired to LOCAL); standard LOCAL composition uses the REGULAR My-Library index (NOT the LAB side-index — LAB is opt-in only); score-interleaved merge (no RRF); stale-LAB rebuild signal; LOCAL-aware `export_comp_report` across xlsx/csv/txt/docx via the Phase 103 helpers. Un-gated the deferred Phase 104.

**Deferred by user decision (NOT gaps):** all of Component B — JSA-01 (anchor parallels seeding), JSA-02 (corpus completion), JSA-03 (torn-word completion), and JWB-05 (tear-side assist) — pushed to a post-v8.0.0 milestone (2026-06-08, /gsd-discuss-phase 110) so the milestone could ship. Web Joins Lab UI deferred to a later phase. EXPUX-01 dialog UI UAT still pending.

**Known deferred items at close:** 103 open artifacts (41 debug sessions, 53 quick tasks, 5 todos, 2 false-positive "UAT gaps" — Phase 107/108 both `[passed]` with 0 pending scenarios, 2 dormant seeds) — the same historical accumulation acknowledged at the v7.14/v7.15/v7.16 closes; none are v8.0.0-specific blockers (see STATE.md Deferred Items). `/gsd-cleanup` recommended on the historical backlog.

**Carried forward:** D-F12 (regular Search ~8s wall-clock — profile-first), D-F18 (context-menu LOCAL detection via `display`), the 7 design-critique deferrals (N-fragment join richness, builder depth, VS-dialog physical deletion, web-parity timing, multi-leaf "other side"), and the Phase 106/107 advisory code-review findings (WR-01/WR-02).

**Tag posture:** `v8.0.0` already created by `/release` 2026-06-09 (both apps; GitHub Release with installer). The GSD milestone-close ritual (this entry + archives + ROADMAP/PROJECT/STATE evolution + REQUIREMENTS.md removal) was run **retroactively 2026-06-11** — `/release` ships + tags but does not run the close. See `.planning/milestones/v8.0.0-ROADMAP.md` and `v8.0.0-REQUIREMENTS.md`.

---

## v7.16 Hebrew PDF Text Quality (Shipped: 2026-06-01)

**Phases completed:** 1 formal phase (102, 5 plans) + post-phase no-phase quality work + release-cycle UAT/freeze fixes
**Git range:** `v7.15.0` → `ccb87c90` (61 commits)
**Scope:** 104 files changed, +34,513 / −441 (large insertion reflects the rawdict RTL helpers, ~150 new tests, and bundled real-PDF regression fixtures)
**Timeline:** 2026-05-28 → 2026-06-01 (4 days)
**App releases:** v7.16.0 (2026-06-01, desktop only), tagged `v7.16.0`

**Delivered:** Rewrote how LOCAL ("My Library") Hebrew PDFs are read into the Tantivy index, so search of typeset Hebrew scholarly books works — emphasis letter-spacing no longer shatters words into single letters, tightly-set books no longer fuse phrases into one token, the maqaf and embedded numbers read correctly, and unrecoverable text layers are flagged. Bundled new file-management actions for LOCAL hits and three significant search/startup performance fixes discovered during UAT. Desktop-only — web "My Library" does not exist, so the dual-app maintenance rule does not apply. Formally one phase (102); the de-space refinements, UAT extraction fixes, and freeze fixes landed as no-phase edit+test work per the user's chosen workflow.

**Key accomplishments:**

- **Phase 102 — LOCAL PDF Text-Layer Extraction Rewrite (5 plans):** `extract_pdf_pages` rebuilt on a `page.get_text("rawdict")` per-glyph foundation with pure RTL glyph-trace reconstruction helpers (`shared/local_indexer_rtl.py`): RTL-gated segment reorder (Meiri core, no LTR regression), Unicode-`Mn` nikud/te'amim classification (preserves maqaf `־`/sof-pasuq), per-line 1-D Otsu word-gap valley de-space, `_ltr_damage_guard` RTL-trust fix (the real production blocker — it was discarding the better RTL output), and `corrupt_encoding` detection + status surface. Nikud stripped once in `_write_page_doc` for all LOCAL formats. `extraction_format_version` 2→3 (existing libraries need one manual "Re-index All"). Measured on identical pages: אוצר הגאונים single-letter tokens 73.5% → ~3-5%, רביצקי word-merge 15.8% → 0.07%, איגרות הרמב״ם 5.2% → 0.17%. ~150 new/updated tests; validated against real-corpus Spike 001.
- **Post-102 de-space follow-ups (no-phase, D-F13b/c/d):** edge-gap + per-line Otsu boundary metric replacing the first-cut fixed-floor/median (which shattered wide letters and merged tight-set books); `startup_recovery` Pass B deferred off the UI thread (D-F13c launch freeze); locally-gated zero-width space-glyph word boundary (N1 — packed headings/tables encode word-spaces as zero-width glyphs the gap test can't see) + embedded-number bidi flip (N3 — Du-Siach years `3191` → `1913`); maqaf cured by the Mn test (N2).
- **LOCAL UAT extraction fixes (D-F19..D-F22, D-F25):** HTML semicolon-less `&nbsp` decode (`_clean_html_text`); `.xlsx` formula-only/empty-workbook fallback to `data_only=False` (was indexed `no_text_layer`); UTF-16 `.csv` BOM detection before the cp1255 fallback; **unchecking a folder now cascades to its files** (BLOCKER — `ItemIsAutoTristate` is child→parent only); `אמ'` apostrophe Tantivy-metacharacter parse-crash sanitized.
- **File-management actions for LOCAL hits (D-F24 + new):** "Open file location" (reveals in OS file manager), file-aware right-click menu (Open file / Open file location / Copy file location / Copy filename) replacing the Genizah cloud-community actions, per-folder opt-out checkboxes in the My Library list, and a fix so `.html`/`.xlsx`/`.csv` LOCAL files can be opened (centralized in `desktop/file_actions.py`).
- **Three search/startup freezes fixed (D-F23, Codex-assisted):** (1) every search froze ~20-30s because `search_history.json` had grown to **778 MB** (each entry stored `results[:5000]`) and was re-read/rewritten on the UI thread per search — history now stores no result snapshots and **re-runs** the search on click (migrated 778 MB → 0.08 MB); (2) large-folder startup froze ~45s from a per-batch `O(batches×folders×files)` opt-out-checkbox refresh — moved to fire once after tree load (14.96s → 0.10s); (3) LAB-rebuild churn from a `lab_index_normalize` AttributeError that aborted every rebuild — fixed the delegation and moved the rebuild to a background `LabRebuildWorker`. Plus `idx_local_files_folder_id` and off-thread refinement replay.
- **Pre-release cleanup:** removed the `[PROFILE]` (`GENIZAH_PROFILE_SEARCH=1`) debug instrumentation added during the freeze hunt (`ccb87c90`).

**Known deferred items at close:** 102 historical backlog items (41 debug sessions, 53 quick tasks, 5 todos, 1 UAT gap, 2 unimplemented seeds) — the same accumulation deferred at the v7.14 and v7.15 closes; none are v7.16-specific blockers (see STATE.md Deferred Items). NEW/carried forward to the next milestone: **D-F12** (regular Search ~constant 8s wall-clock investigation), **D-F17** (xlsx/Word/JSON export not yet adapted to LOCAL / ALL results), **D-F18** (context-menu LOCAL detection could normalize through `display`).

**Tag posture:** `v7.16.0` already created by `/release` 2026-06-01 (desktop only; GitHub Release with installer attached, marked `latest`). No separate `v7.16` milestone tag — consistent with v7.10–v7.15 convention.

---

## v7.15 My Library Visual (Shipped: 2026-05-28)

**Phases completed:** 3 phases (99, 100, 101), 7 plans, 8 tasks
**Scope:** Desktop-only; PDFs only (other LOCAL formats stay text-only)
**Timeline:** 2026-05-27 → 2026-05-28 (2 days)

**Key accomplishments:**

- **Phase 99: PDF Page Renderer** — Shared on-demand PyMuPDF page renderer in `desktop/pdf_image_controller.py` with bounded LRU of open `fitz.Document` handles, off-thread `ImageLoaderThread`-style worker, and graceful failure (placeholder + log on missing/corrupt/out-of-range/encrypted PDFs). No on-disk image cache; only currently-displayed pages live in memory.
- **Phase 100: LOCAL PDF Image in ResultDialog + Browse** — Wired the renderer into both desktop surfaces. `ResultDialog` shows the rendered page image next to extracted text and re-renders on prev/next result. Browse panel shows the image in the previously-hidden image pane and syncs prev/next page with the text. Non-PDF LOCAL files (`.docx`/`.html`/`.xlsx`/`.csv`/`.txt`) stay text-only — image pane gated on file extension.
- **Phase 101: Pre-release polish** — Wave 1 fixed LOCAL PDF RTL/bidi word-order reversal via S-1 directional-run reversal helpers in `shared/local_indexer.py::extract_pdf_pages` (gated on `_rtl_ratio > 0.4`; embedded Latin shelfmarks like `T-S 12.123` stay adjacent). D-04 auto-self-heal-on-launch ROLLED BACK post-UAT — froze 12K-PDF library; existing libraries need manual recovery. Wave 2 closed Phase 100 review remnants (WR-01 single-lookup collapse, WR-02 discard_scope test). UAT-driven follow-ons: LAB rebuild 5-failure bail + pre-flight callback probe; remove-folder batched commit + retry (was triggering ERROR_ACCESS_DENIED storm on Windows); i18n leak in remove-folder dialog; intra-block newline collapse in PDF extraction (joined bidi-fragmented Hebrew paragraphs into continuous prose); new "Re-index All" button in My Library tab to force re-extraction via the background worker (recovers existing libraries after the RTL + reflow fixes).

**Known deferred items at close:** 100 historical backlog items (40 debug sessions, 53 quick tasks, 5 todos, 1 UAT gap, 1 unimplemented seed) — same set as the v7.14 close. NEW deferred: D-F12 (regular Search ~constant 8s wall-clock investigation) logged in `docs/OPEN_ISSUES.md` as v7.16+ work.

---

## v7.14 My Library — Local Document Search (Shipped: 2026-05-24; closed 2026-05-27)

**Phases completed:** 6 phases (95, 96, 97, 97.2 INSERTED, 97.3 INSERTED, 98), 37 plans
**Git range:** `v7.13.0` → `8ad0e69d` (355 commits across the v7.14 cycle, incl. bundled data assets)
**Scope:** Python + tests across the desktop My Library subsystem (`shared/local_indexer.py`, `desktop/my_library_tab.py`, `genizah_core.py` LOCAL merge) plus the web-side Phase 98 NLI resilience module (`shared/nli_circuit_breaker.py`, `shared/posthog_server.py`)
**Timeline:** 2026-05-21 → 2026-05-27 (7 days wall clock; public v7.14.0 release 2026-05-24, internal hotfix chain 97/97.2/97.3 + Phase 98 NLI resilience through 2026-05-27)
**App releases:** v7.14.0 (2026-05-24, both apps), tagged `v7.14.0`

**Delivered:** Productized Yehuda Seewald's external prototype into a first-class desktop "My Library" tab (7th tab) that indexes user folders of `.docx`/`.pdf`/`.txt`/`.html`/`.xlsx`/`.csv` into a separate Tantivy side-index merged into Search / Composition / Parallels results via RRF (k=60, POST-dedup) with a `LOCAL` badge and a three-state filter. Personal corpora NEVER leak to the cloud — three cloud-write gates pinned at the TOP of the serializer / corrections / lists-sync paths, enforced by regression tests. The milestone then hardened the feature through three post-ship cascades (PDF extraction, recovery semantics, mega-folder UI stability) and shipped a parallel web resilience milestone (Phase 98) that prevents any single NLI/IIIF upstream slowdown from hanging `genizah-web`.

**Key accomplishments:**

- **Phase 95 — My Library MVP (9 plans, shipped v7.14.0):** Desktop 7th tab indexes `.docx`/`.pdf`/`.txt` into a SEPARATE Tantivy side-index, merged into Search/Composition/Parallels via RRF k=60 *after* `_deduplicate()` (Codex D-08 P0). Namespace-isolated synthetic sys_ids, never colliding with NLI/PGP/CUDL. Three cloud-write gates pinned at the TOP of `shared/search_serializer.py`, `corrections_client.py`, `lists_sync.{sync_item_to_cloud, sync_list_to_cloud}` (Codex D-30 P0). PyMuPDF dep + `collect_all('pymupdf')` + `--self-test-pymupdf` CLI. Per-thread SQLite via `threading.local()`; Tantivy commit retry on Windows `os error 5`. Pre-search corpus dropdown `Genizah`/`Local`/`ALL` + 3-state `Filter Local` button. Bilingual About/Help with Seewald attribution (יהודה זייבלד). Static AST guard `tests/test_web_library_options_no_local.py` pins the web LIBRARY_CODES invariant (`[]`).
- **Phase 96 — Completing My Library (9 plans):** Closed the P1 LOCAL highlight regression (engine-side `_build_local_result_dict` normalization), the PDF one-word-per-line extraction bug (detect-then-fallback in `extract_pdf_pages`, 0.70 single-word-ratio threshold), and added the per-file opt-in/out drill-down tree (`_OptoutTreeWidget`, Qt-native tri-state, session-JSON persistence). Removed the redundant `צפה בדפדוף` button; added next/prev + View-All navigation for LOCAL hits in ResultDialog and the Browse panel. D-F2 (OCR) + D-F3 (side-by-side PDF) explicitly deferred to v7.15+.
- **Phase 97 — More LOCAL features (6 plans):** Scaled My Library to Seewald's prototype size (13K files / 43 GB; ceiling 50K/50GB) via SQLite v1→v2 migration + zstd `cached_text` + atomic Tantivy rebuild (WAL+FULL durability bracket) + recovery UX gate. Added `.html` (lxml.html, not BeautifulSoup), `.xlsx` (openpyxl streaming), `.csv` extractors with encoding chains; 100 MB raw cap + zip-bomb defense; mtime_ns incremental audit; byte/count/time commit policy; phase-aware ETA + `scan_run_id` (mutated-rows-only) + `FolderWalkWorker` QThread + View-All 500-cap incremental render; network-drive semantics; bilingual privacy disclosure; 4 invariant CI guards.
- **Phase 97.2 INSERTED — Recovery Cascade (3 plans):** Fixed 5 interacting Phase 97 bugs that left the LOCAL index permanently broken when atomic rebuild failed at startup (redundant `tantivy.Index` reopen leaking the writer lock; stale `.tantivy-writer.lock` carried through `os.rename`; `discard_run` schema-version mismatch; missing `self._writer is None` guards) and implemented "Reset My Library" recovery UX (close handles → delete LocalIndex + LocalLabIndex + SQLite → recreate empty). 8/8 R97.2-* requirements MET; 25 commits.
- **Phase 97.3 INSERTED — UAT Stability (4 plans):** Closed six post-Phase-97.2 UAT defects: workerized tree population via `FolderWalkWorker` (closes UI-thread freeze on mega folders), Reset-button guard simplified to `worker_running` only (orphan `scan_runs.running` rows no longer block Reset), one-shot `_skip_startup_rescan_once` flag (Skip no longer re-launches the broken scan), indeterminate→determinate progress bar with `status_updated` signal, `fitz.TOOLS.mupdf_display_warnings(False)` to silence 624× stderr noise, and UI tree extension parity (`.html`/`.xlsx`/`.csv` now visible). 6/6 R97.3-* requirements MET; 7 new test files. Followed by a recovery-modal recurrence fix (`1859b8ac`+`528906e4`) and a post-UAT Codex review follow-up (`fb5cbdb8`).
- **Phase 98 — NLI Resilience (6 plans, web infra):** Shared `shared/nli_circuit_breaker.py` (module-level singleton, `time.monotonic`, `threading.Lock`) wired into all 10 NLI fetch sites, replacing the buggy class-attribute breaker. 6 new env knobs; `NLI_SEMAPHORE_TIMEOUT` default dropped 20→1. PostHog telemetry on breaker open/close via factored `shared/posthog_server.py` (Option (a) — `shared/` no longer depends on `web/`). Worst-case per-request blocking budget dropped 45s → ~9s; after 3 consecutive failures the breaker trips for 60s. Closes the 2026-05-25 production hang (Starlette threadpool saturation on synchronous `requests.get` to `iiif.nli.org.il`). Production canary PASSED 2026-05-25. Async refactor to httpx, event-loop watchdog, and multi-worker uvicorn explicitly deferred.

**Known deferred items at close:** 104 stale audit items (40 debug sessions, 53 quick tasks, 5 todos, 1 seed, plus Phase 95/96 partial UAT + Phase 95/97 `human_needed` verification flags). The v7.14-specific items are substantively closed by the shipped v7.14.0 release + the 97.x hotfix chain; only status-flag bookkeeping and the long historical backlog (predating v7.12) were deferred. See STATE.md Deferred Items. Recommend a `/gsd-cleanup` pass between milestones.

**Tag posture:** `v7.14.0` already created by `/release` 2026-05-24 (both apps). No additional tag created at close — this was a retroactive bookkeeping reconciliation.

---

## v7.13 Research-Grade Downloads & PGP Filter (Shipped: 2026-05-21; closed 2026-05-27)

**Phases completed:** 2 phases (93, 94), 5 plans
**Git range:** `v7.12.0` → `v7.13.0` (102 commits)
**Scope:** 209 files changed across the v7.13 tag range (large delta reflects planning-doc + data churn alongside web/desktop export code)
**Timeline:** 2026-05-19 → 2026-05-21 (3 days wall clock)
**Requirements:** 14/14 satisfied (5 PGP-FILTER + 9 EXPORT-META; PGP-FILTER-03 superseded by user smoke direction)
**App releases:** v7.13.0 (2026-05-21, both apps), tagged `v7.13.0`

**Delivered:** Surfaced PGP coverage at the result-set level on `/search` (web) and upgraded downloaded xlsx artifacts into citation-grade dossiers so a downloaded file stands alone as a scholarly source. Both phases were promoted from the backlog (999.2 + 999.3).

**Key accomplishments:**

- **Phase 93 — PGP Filter on `/search` (1 plan, web-only):** Post-search 3-state filter button (`Filter PGP` / `Has PGP` / `No PGP`) mirroring the `printed_filter` pattern, persisted via the Phase 87 `web/safe_storage.py` chokepoint. Strict cascade discipline across 6 render branches pinned by a static AST guard (`tests/test_pgp_filter_cascade.py`). 4/5 PGP-FILTER reqs satisfied directly; PGP-FILTER-03 (chip) superseded after user smoke feedback (colored button label already conveys state). Hebrew: `סנן PGP` / `PGP בלבד` / `ללא PGP`. Desktop already exposed the same signal via a sortable `COL_PGP` badge column, so no desktop parity was required.
- **Phase 94 — Research-Grade Export Metadata (4 waves, web + desktop xlsx):** 4-sheet bilingual xlsx workbook (`Search Results` + `Manuscripts` + `Bibliography` + `Credits and Info`) on both apps via shared `shared/export_dossier.py` helpers. Main sheet gains `Has PGP`/`Is Printed`/`Domains` columns; `Manuscripts` sub-sheet has one row per unique sys_id with PGP + NLI + catalog + library-viewer + GenizahSearch URLs (clickable hyperlinks); `Bibliography` sub-sheet has one row per FJMS bib entry. Web JSON gains 3 additive per-item flags (`has_pgp`/`is_printed`/`domains`) with envelope `schema_version` unchanged. Cross-parity invariant pinned by `tests/test_export_xlsx_cross_parity.py`. CONTEXT D-04 REVERSED 2026-05-20 for the row-content layer only (bilingual headers + source-language metadata). Refined across 6 rounds of smoke-verification patches approved by Hillel same-day. Phase 94.1 post-closeout patch (commit `e01bfd14`) lifted D-13 and populated the renamed `Image URL` column with per-folio GenizahSearch proxy URLs.

**Known deferred items at close:** Captured under the v7.14 close (shared historical backlog). v7.13-specific follow-ups (pre-search PGP filter, parallels-page filter, desktop PGP filter parity, Hebrew metadata export mode) are recorded in the archived `v7.13-REQUIREMENTS.md` "Future Requirements" section.

**Tag posture:** `v7.13.0` already created by `/release` 2026-05-21 (both apps). No additional tag created at close — retroactive bookkeeping reconciliation.

---

## v7.12 Multitenant Architecture (Path B) (Shipped: 2026-05-18)

**Phases completed:** 10 phases (87, 88, 89, 90, 91, 92, 92.1 INSERTED, 92.2 INSERTED, 999.1 promoted, 999.4 promoted), 28 plans
**Git range:** `af9f749c` → `315777d1` (277 commits)
**Scope:** Python +10,587 / -1,604 across 66 files; tests +7,034 / -386 across 35 files
**Timeline:** 2026-05-13 → 2026-05-18 (6 days wall clock)
**Requirements:** 49/49 satisfied (38 v7.12 core + 11 promoted backlog)

**Delivered:** Refactored GenizahSearch's web layer off the desktop-inherited single-user mental model so per-user state, auth, and caches cannot leak across concurrent sessions sharing one Python process. The cross-user xlsx export filename leak fixed in v7.11.1 was one instance of a class of bugs surfaced across 4 rounds of Codex review; v7.12 replaces that pattern with intentional multitenant primitives.

**Key accomplishments:**

- **Phase 87 — Foundations:** 131 raw `app.storage.user` access sites migrated across 14 files through the new `web/safe_storage.py` chokepoint with `_session_uuid` minted on first request. AST-based pytest lint scanner (`tests/test_no_raw_storage_access.py`) installed as permanent CI guard. 8 plans across 8 waves.
- **Phase 88 — State Separation by Deletion:** 10 per-user fields physically deleted from `web/state.py:AppState`; `web/export_state.py` becomes the sole path for per-user export state. `_TEST_BACKEND` shim replaced by `SimpleNamespace`-based fixture pattern. Two permanent regression guards (runtime attr-absence + static AST scanner with alias-import coverage).
- **Phase 89 — Lists Cache Per-Request:** `UserListsManager` singleton + `_cache_entry` tuple + 10s TTL plumbing all deleted. Per-request instantiation in page handlers. Cross-user cache leak structurally impossible.
- **Phase 90 — Auth Caching Rewrite (No set_session):** Request-scoped auth via local header mutation; proactive refresh gated by `_refresh_locks` keyed by `_session_uuid` (not access tokens — stable across rotation). All 5 auth-mutating helpers use throwaway clients to sidestep the supabase event-listener leak. `sign_out` uses `throwaway.auth.admin.sign_out(jwt, "global")` for real server-side revocation. 4 globals + 2 helpers atomically deleted with 3 CI guards installed in a single commit.
- **Phase 91 — Atomic Auth State Writes:** 12 raw accesses migrated. `set_auth` returns `bool` with symmetric user/profile 2-key rollback AND treats `profile=None` as "clear stale auth_profile" (Codex HIGH catch — stale profile leaked role via `GlobalAuthState.get_role()`/`is_admin()`/`is_editor()`). `do_login` and `_oauth_complete_login` use session-first multi-write ordering with defensive 3-key caller-level cleanup. Phase 87 allowlist self-eliminates: **2 → 0 entries** (lint scanner now enforces zero raw `app.storage.user` accesses anywhere under `web/`).
- **Phase 92 — Final Sweep and Acceptance:** 5-surface widened SWEEP-01 audit clean (`app.storage.user` + `app.storage.browser` + `app.storage.client` + `joins.db` + `web/analytics.py`). 4 Codex transcripts re-audited thematically; 23 raw findings deduped into 13 unique issues with git short-hash citations. SWEEP-05 smoke run 2 PASS 2026-05-18 against server commit `9fd68b7c`: R0/R1/R2/cross-user concurrent all PASS. `docs/guides/MULTITENANT.md` shipped as architecture reference (~2150 words, 8 sections).
- **Phase 92.1 INSERTED — Reader-Client Retrofit:** Closed P0 RLS-reachability regression introduced by Phase 90's singleton-anonymous-only invariant. 12 reader call sites in `web/supabase_client.py` migrated from anonymous `get_client()` to authenticated `get_user_client()`. 6 KEEP sites annotated with forward-looking RLS evidence (e.g. verified `discoveries` only SELECT policy is `TO public USING (is_hidden=false)` with no admin SELECT branch). AST scanner `tests/test_no_anonymous_reads_on_authenticated_tables.py` with `BANNED_TABLES = {user_lists, list_items, recent_items, projects}`. 5-test regression suite exercising the REAL `lists_mgr.create_list → get_user_client → safe_user_get('auth_session')` chain.
- **Phase 92.2 INSERTED — Lists Performance Investigation:** Closed `/lists` 36s warm-render regression introduced by Phase 92.1 reader migration. Pre-fix BASELINE 35.5/34.2/45.8s; post-fix POSTFIX 1.9/1.9/2.2s — **19.3x mean speedup, 26x reduction in Client builds per render**. Fix: task-scoped `WeakKeyDictionary` memo on `get_user_client()` keyed by `(get_persisted_session_uuid(), access_token)` with `asyncio.current_task()` as the WeakKey; sync-context bypass for `run.io_bound`; zero-arg `get_list_item_counts_for_user()` RPC pushed to prod Supabase replaces per-list fanout; `data`+`counts` threaded through `/lists` render path. Per-user Client cache (E-path) explicitly REJECTED — Codex flagged "reopens Phase 90's scary surface."
- **Phase 999.1 promoted — Search Results by Folio:** Small theme-aware chip after shelfmark on web `/search` result cards rendering `result['display']['img']` for desktop COL_IMG parity. Adapts to light/parchment/dark themes via existing CSS tokens; descriptive `tr('Image number')` tooltip.
- **Phase 999.4 promoted — Line Numbering:** Right-side (RTL leading-edge) line-number gutter on 5 surfaces (web Browse + Quick View + Full Manuscript View; desktop Browse tab + ResultDialog). Numbering anchored to `text.split('\n')` matching the existing Responsa `L<N>:` parser. D-04 copy-paste invariant achieved structurally on both surfaces (CSS-grid column with `user-select: none` on web; sibling `QWidget` outside `QTextDocument` on desktop). Shipped across 13 commits over 4 human-verify smoke-check rounds.

**Architecture reference:** `docs/guides/MULTITENANT.md`
**Live CI enforcement:** `tests/test_no_raw_storage_access.py` (allowlist `[]`)

**Known deferred items at close:** 96 stale audit items predating v7.12 (38 debug sessions, 50 quick tasks, 5 todos, 1 seed, 2 verification gaps substantively closed by SWEEP-05 PASS but with `human_needed` status flag not yet flipped). See STATE.md Deferred Items.

**Tag posture:** No git tag created during milestone close — deferred to `/release` skill which bundles the web changes with the next desktop installer build. Web-only milestones do not get standalone tags per `feedback_no_github_release_for_web_only.md`, but v7.12 ships alongside desktop changes (Phase 999.4 LINE-NUM-07/08/09/10 + v7.11.2 desktop patch bundling) so the release will tag both.

---

## v7.10 Search API (Shipped: 2026-05-05)

**Phases completed:** 8 phases, 37 plans, 52 tasks

**Key accomplishments:**

- Five new AppState fields populated at six execute-time sites + 22-test RED scaffold for shared.search_serializer; latent state.current_search_query bug fixed as a side effect at all three search-execute paths.
- chunk_hits is now populated inside lab_composition_search and surfaced on the returned items, with 5 tests (3 static contract + 2 behavioral, monkeypatch-driven) locking the contract behaviorally per HIGH-04.
- shared/search_serializer.py is now the single source of truth for the Claude-friendly JSON payload shape — one module, two top-level functions, one private _serialize_item, all 22 contract tests GREEN, and three review hardening fixes (HIGH-05 singleton-no-close, HIGH-06 millisecond+counter filename, HIGH-07 Oxford-null image_url) baked in.
- Two new GET handlers (/api/export/json, /api/export/parallels/json) wired to toolbar buttons on /search and /parallels; init_api_routes refactored to accept an app_override parameter so the 5 handler tests register onto a bare FastAPI app instead of mutating the NiceGUI singleton; LOW-01 Hebrew translations added.
- Phase 77 close-out plan: docs trail updated for the latent `state.current_search_query` bug fixed by Plan 01, the `shared/search_serializer.py` module shipped by Plan 03, and a `chunk_hits` field-name collision uncovered + fixed during the manual smoke check on /search and /parallels JSON downloads. Phase 77 ready for `/gsd-verify-work`.
- Two pre-existing UAT major-severity gaps closed in one bundled plan: (1) `_reset_search` now clears the global `state` singleton's envelope-echo fields so post-'New Search' exports return 400 instead of emitting prior search results; (2) all 3 search-side export handlers (Excel/Word/JSON) now filter `state.last_results` by uid when checkbox selection is non-empty, with `-selected-N` filename suffix.
- Files created (verified via `[ -f ... ] && echo FOUND`):
- Found during:
- `shared/api_errors.py`:
- Found during:
- Public surface:
- Locked in 38 D-24 tests for GET /api/browse including a real-HTTP search→browse round-trip, plus a D-25 legacy spot check for /api/nli_image_by_sysid; uncovered and fixed a wrap_endpoint signature regression that was producing 422 on every browse request.
- [Rule 3 — Blocking]
- Anthropic Skill instruction file (SKILL.md, 204 lines) + human-facing README + Level-3 api_contract.md reference, with REQUIREMENTS.md SKILL-04 R2 enum mismatch closed
- Status:
- APPROVED
- 1. [Rule 2 — missing critical functionality] Web banner copy refresh added as Task 2b

---

## v7.8 Structural Foundation (Shipped: 2026-04-15)

**Phases completed:** 4 phases (63-66), 9 plans
**Git range:** 506ec1e7 → c987c2f2 (64 commits)
**Scope:** 173 files changed (+6,269 / -828 lines)
**Timeline:** 2026-04-14 → 2026-04-15 (~14 hours)
**Requirements:** 12/12 satisfied

**Delivered:** Structural debt reduction with zero user-visible behavior changes — CI safety net, pinned dependencies, migrated auth stack, cleaned repo hygiene, refreshed documentation.

**Key accomplishments:**

- **CI safety net** (Phase 63): GitHub Actions workflow with Ubuntu + Windows matrix runs ruff, scripts/check_docs.py, and pytest on every push and PR. Ruff configured with scoped ruleset (E9/F401/F811/F821), 267 violations fixed to establish zero-violation baseline.
- **Reproducible builds** (Phase 63): Two-file dependency pinning — 14 direct deps in requirements.txt + 115 transitive in requirements-lock.txt, all exact `==` pins. CI installs from lock file.
- **Auth modernization** (Phase 64): Migrated from deprecated gotrue to supabase_auth.errors across web and desktop clients. Removed implicit OAuth flow and dead /api/auth/oauth-callback endpoint. PKCE-only callback with error parameter handling. Production-verified including OAuth cancellation and expired code replay.
- **Framework patches isolated** (Phase 65): NiceGUI monkey-patches extracted to web/framework_patches.py with per-patch `packaging.version.Version()` guards and WARNING-level failure logging. packaging==26.0 pinned.
- **Exception hygiene** (Phase 65): 205+ silent exception handlers across 76 first-party files audited — each now logs or has inline justification. Zero behavioral changes.
- **Repo cleanup** (Phase 65): .gitignore extended 50→126 lines with root-anchored patterns covering 15+ debris categories. Untracked root files 67→1 (intentional asset).
- **Documentation refresh** (Phase 66): CODE_INDEX.md gained v7.8 sections for framework_patches, auth_state, thread_local_db. OPEN_ISSUES.md tracks 7 Phase 65/64 code review findings. DEVELOPER_GUIDE.md documents CI workflow, ruff config, and dependency upgrade process. scripts/check_docs.py passes green.

**Known deferred items at close:** 0 critical. Tech debt tracked: 4 Phase 65 code review findings in OPEN_ISSUES.md (WR-01, WR-02, IN-01, IN-02 — non-blocking), CODE_INDEX line-number drift for genizah_app.py/core.py (pre-existing), Nyquist VALIDATION.md partial/missing across all 4 phases (acceptable for infrastructure milestone).

---

## v7.7 Volume-Aware Browse (Shipped: 2026-04-03)

**Phases completed:** 62 phases, 197 plans, 343 tasks

**Key accomplishments:**

- PostgreSQL tables for PGP document storage with pgpid natural key, JSONB tags, GENERATED url, and RLS public-read policies
- One-liner:
- 7,090 PGP transcriptions with metadata imported to Supabase via two-pass batch upsert script with 7,764 fragment links
- Service layer for PGP document-fragment relationships with 4 query functions, unit tests, and integration verification
- PGP transcriptions integrated into browse page version selector with auto-selection, verified icon, and attribution display
- Code verification:
- Regex-based recto/verso section parsing added to document_service.py, integrated into browse.py for page-filtered transcription display
- One-liner:
- One-liner:
- One-liner:
- Multi-source version selector with grouped transcriptions/translations sections, scholar attribution, and page-aware content filtering.
- ALTER TABLE migration adding languages_primary, languages_secondary, inferred_date_standard, inferred_date_rationale to documents table with updated import script
- PGP metadata section added to browse page with document type, tags, description, dates, and translate buttons
- Clicking a PGP tag navigates to search page with filtered results, viewer pane preview with manuscript text
- One-liner:
- Commit:
- Added ASCII apostrophe and curly quote variants to search normalization, closing the UAT gap where typing ' (keyboard apostrophe) returned 503 results instead of 11,006
- Fixed 7 test failures across export service and boundary search by updating expectations to match production behavior changes
- Fixed 10 test failures (4 responsa mark-tolerant, 6 shelfmark expectations) to achieve full green suite of 410 tests
- PGP HTML canvas parser using stdlib HTMLParser with 14 tests, plus fixed section regex handling all 712 missed marker variants
- Canvas-based section lookup wired into both web and desktop display pipelines with 5 consumer sites and 8 integration tests
- Language-based translation grouping in desktop _populate_pgp_combo matching web app Hebrew-first, English-second order
- Shared corrections service with get_pending_corrections_for_page() querying Supabase by sys_id, page, author, and status filter
- Pending corrections as selectable amber-styled entries in web version selector with schedule icon, status label, and on_version_change callback
- 9 verification tests confirming Browse tab and Reading Desk pending corrections display with permission filtering and emoji labels
- SQLite sidecar export from 13GB FIST.db producing fjms_enrichment.db with 762K rows across domains/joins/catalog tables plus FTS5 full-text search index
- FjmsService class providing domain, join, and catalog queries from SQLite sidecar with thread-safe read-only access and 27 unit tests
- FJMS scholarly join groups merged into Related Fragments panel in both web and desktop apps with scholar name, join type display, purple badge, and deduplication against user/PGP joins
- GROUP BY + GROUP_CONCAT deduplication in get_join_group() so multi-group manuscripts show each partner once with all scholars and join types aggregated
- Source merging replaces source-dropping dedup so fragments in both PGP and FJMS show dual badges (blue PGP + purple FJMS) in web and "PGP, FJMS" in desktop
- 815K NLI image records and 141K Cambridge IIIF manifests imported into nli_crossref.db sidecar with normalized shelfmarks and indexed join keys
- NliCrossrefService with 12 methods providing image lookup, Cambridge IIIF manifests, physical metadata, relationship queries, and availability indicators for both web and desktop apps
- Local-first FL ID resolution via NLI crossref sidecar, eliminating network manifest fetch for 766K+ manuscripts
- Local-first FL ID and Cambridge manifest resolution in enrich_metadata via NLI crossref SQLite sidecar, eliminating 2-3 network calls per manuscript for covered records
- Folio label parsing from NLI ImageName patterns with navigation dropdown and clickable NLI/CUDL/Oxford source indicator chips on web browse page
- Folio-labeled page combo, KTIV viewer button, and source indicator enhancements in desktop browse tab matching web app patterns from Plan 01
- Cambridge/NLI image source toggle via styled chips with cached IIIF proxy endpoint
- Physical metadata (material, folios) and library digital collection links in web browse via NLI crossref enrichment
- Physical metadata (material, folios, size) and library digital collection links in desktop browse extended info panel via enrich_metadata enrichment
- Supabase PGP tables exported to 146.6 MB local SQLite sidecar (pgp.db) with pagination, JSON serialization, built-in validation, and idempotent rebuild
- PgpService class reading from pgp.db sidecar via SQLite, replacing all 11 Supabase REST API calls with sub-millisecond local queries while preserving identical 14-function public API
- 33 SQLite-backed tests replacing all Supabase mocks, verifying JSON deserialization, json_each tag search, batch lookup, and graceful degradation for PgpService
- Surgical fix adding state.pgp_metadata assignment to FL ID initialization path in browse.py, closing gap where PGP tags/links/dates were invisible on search-to-browse navigation
- Extended fjms_enrichment.db with 4 new catalog tables (2.1M rows), v2 catalog schema with GenizahTitle/NumFolio/UnitCatalogRecId, and contentless FTS5 index spanning RunningTitle + FreeDescription
- Added get_catalog_source_counts() and get_catalog_detail() methods to FjmsService with v3.0.0 schema support, 46 passing tests, and 10 Hebrew translation keys for dialog labels
- NiceGUI catalog dialog with FIST 5-section side-by-side team layout, wired into browse page and search cards with batch-loaded source counts
- Desktop FjmsCatalogDialog with FIST 5-section HTML table layout and Catalog Records (N) button wired into Browse tab and ResultDialog
- Source team attribution added to catalog free descriptions pipeline (export → service → both UIs) with desktop RTL layout fix for Hebrew interface
- pgp.db bundled in desktop build via --add-data, deployment docs updated with scp/regeneration commands, PgpService.get_version() added for update checker
- 12 automated tests proving PGP/FJMS/NLI sidecar services operate entirely from local SQLite with zero network dependencies
- Desktop app auto-checks GitHub Releases for sidecar updates on startup, prompts user, downloads to LOCALAPPDATA, resets service singletons, and About screen shows installed data versions
- Parallelized search enrichment via asyncio.gather, batched FJMS metadata pre-fetch in browse, and async stats+feed loading on discoveries page
- Async domain enrichment via DomainEnrichmentWorker QThread + lazy catalog detail fetch on button click in browse and reading desk
- Crossref metadata queries moved from synchronous render path to parallel enrichment via asyncio.gather with module-level session cache for instant back-navigation
- O(1) dict lookup for FL ID browse navigation, replacing linear scan over 217K browse_map entries with background-built index fallback
- Unified variant cache with superset-aware lookup: Tantivy phase (limit=200) slices from pre-computed regex-phase result (limit=8000) instead of recomputing
- Elapsed timer, ETA, chunk count, summary line, and min-chunks filter across all search modes in both web and desktop apps
- Windows toast notification on search complete, OS sleep prevention via SetThreadExecutionState in all search threads, and right-click copy options on search result rows
- LIBRARY_CODES_HE dictionary with 81 Hebrew library names, lang-aware get_library_display(), all web callers updated for Hebrew mode
- Web translation integration with global toggle, clickable Translated/Original badges, translated match detection, Dicta-powered translate buttons, and browse shelfmark/sys_id URL support
- Supabase publish service with 7 CRUD functions, 9 mocked tests, and full RLS schema for community puzzle join sharing
- Publish/unpublish toggle in web puzzle toolbar, published joins in Discoveries feed with thumbnails, Community Puzzle Joins section in joins panel, and /puzzle?doc= deep link route
- Desktop publish/unpublish toggle with worker thread, DiscoveriesDialog feed integration, JoinsDialog community section, and All/My Puzzles sub-tabs
- 38,673 FIST-only manuscripts merged into libraries.csv (216,942 -> 255,615) with 7 new library codes and Yevr/Halper shelfmark normalization aliases
- Metadata search guard fix enables Title/Shelfmark search to return 38K FIST-only records using meta_mgr API, with TDD test suite and browse fallback
- RefinementStep dataclass with chain helpers for search-within-results: serialization, None/empty-set restrict merging, replay, scope signature
- Web search refinement with breadcrumb chain, refine mode toggle, session persistence with replay, and zero-result recovery
- PyQt6 desktop refinement chain with breadcrumb strip, refine mode badge, session replay, and zero-result recovery
- Date completed:
- Split browse page into fast Phase A (Tantivy + csv_bank, zero SQLite) and deferred Phase B (crossref + Oxford + Cambridge + attribution enrichment)
- Desktop PyQt6 browse parity with web: volume selector, suffix-aware IIIF, search-to-browse IE propagation for 3,193 multi-IE manuscripts
- ie_id column added to corrections/comments write and read paths so multi-IE manuscript contributions reference the specific volume
- Stratified IIIF validation script for ie_volume_map.json with volume_ie session persistence in both web and desktop apps

---

## v7.6 Search Refinement & Scholarly Joins (Shipped: 2026-03-31)

**Phases completed:** 11 phases, 33 plans, 53 tasks

**Key accomplishments:**

- Supabase publish service with 7 CRUD functions, 9 mocked tests, and full RLS schema for community puzzle join sharing
- Publish/unpublish toggle in web puzzle toolbar, published joins in Discoveries feed with thumbnails, Community Puzzle Joins section in joins panel, and /puzzle?doc= deep link route
- Desktop publish/unpublish toggle with worker thread, DiscoveriesDialog feed integration, JoinsDialog community section, and All/My Puzzles sub-tabs
- 38,673 FIST-only manuscripts merged into libraries.csv (216,942 -> 255,615) with 7 new library codes and Yevr/Halper shelfmark normalization aliases
- Metadata search guard fix enables Title/Shelfmark search to return 38K FIST-only records using meta_mgr API, with TDD test suite and browse fallback
- RefinementStep dataclass with chain helpers for search-within-results: serialization, None/empty-set restrict merging, replay, scope signature
- Web search refinement with breadcrumb chain, refine mode toggle, session persistence with replay, and zero-result recovery
- PyQt6 desktop refinement chain with breadcrumb strip, refine mode badge, session replay, and zero-result recovery
- Split browse page into fast Phase A (Tantivy + csv_bank, zero SQLite) and deferred Phase B (crossref + Oxford + Cambridge + attribution enrichment)

---

## v7.0 Fragment Puzzle (Shipped: 2026-03-17)

**Phases completed:** 5 phases, 15 plans, 0 tasks

**Key accomplishments:**

- (none recorded)

---

## v6.5.0 Search UX & Filtered Search (Shipped: 2026-03-14)

**Delivered:** Overhauled the daily search experience based on power user feedback — composition progress display with ETA, partial results on cancel, session persistence (restoring state including 5K+ exclusions), bidirectional filtered search by scholarly categories, and ~580K Dicta translations for multilingual access across all scholarly data.

**Phases completed:** 42-46 (26 plans total, including 6 UAT gap closure plans)

**Key accomplishments:**

- Search UX overhaul: elapsed timer, ETA, partial results on cancel, chunk count, min-chunks filter, 3-state printed filter, CreationType badge (both apps)
- Session persistence: full state + exclusion restore on reopen, search/composition history dropdowns (both apps)
- Quick UX wins: desktop notifications on search completion, sleep prevention during search, Hebrew library names (81 codes), copy context menu
- Bidirectional filtered search: pre-search filtering by domain/author/work/date/material across all modes including parallels, browse-to-search navigation
- Dicta translation: ~580K translations (libraries 185K, PGP 35K, FJMS catalog 4K, FJMS descriptions 255K, FJMS running titles 107K) with translation toggle
- Translation QA: 10-heuristic QC module, audit sampling, user-facing report dialog, 12,827 data fixes applied

**Stats:**

- 244 commits, 223 files changed, +44,331 / -3,414 lines
- 5 phases (42-46), 26 plans
- 15 days (Feb 28 -> Mar 14, 2026)
- Origin: Power user feedback letter (2026-02-27, 17 requests)

**Git tag:** v6.5.0

---

## v1 External Data Integration (Shipped: 2026-02-07)

**Delivered:** Integrated Princeton Geniza Project scholarly data -- transcriptions, metadata, and fragment joins -- into GenizahSearch web app, transforming it from a manuscript browser into a research platform with scholarly context.

**Phases completed:** 1-7 (18 plans total, including 2 inserted phases)

**Key accomplishments:**

- Imported 7,090 PGP documents with 9,364 transcription/translation sources into Supabase
- Built multi-source version selector -- users switch between scholars' editions and Hebrew/English translations
- Added PGP metadata display (document type, dates, description, tags) with tag-based search
- Implemented Related Fragments panel with unified PGP + user joins and View All Fragments mode
- Added PGP transcription indicator to search results with batch lookup
- Full Hebrew translation coverage for all new UI strings

**Stats:**

- 87 files created/modified
- 3,913 lines of Python/SQL (net additions)
- 9 phases, 18 plans, 173 min total execution time
- 3 days (Feb 5 -> Feb 7, 2026)

**Git range:** `feat(01-01)` -> `docs(07)`

---

## v5.6.0 Desktop Parity & PGP Integration (Shipped: 2026-02-09)

**Delivered:** Brought all PGP features to the desktop app via a shared service layer, imported remaining PGP documents, and built a Virtual Reading Desk for multi-manuscript viewing in both apps.

**Phases completed:** 8-12 (25 plans total, including gap closure plans)

**Key accomplishments:**

- Extracted shared/document_service.py for both apps to consume PGP data
- Imported all 35,839 PGP documents with footnotes and fragment metadata
- Desktop PGP feature parity: transcriptions, metadata, joins, tag search, version selector
- Virtual Reading Desk: synchronized dual-pane multi-manuscript viewer in both web and desktop
- PGP badges, filters, and tag search in both apps
- Phase 13 (Transcription Search) deferred -- index build too slow for desktop

**Stats:**

- 5 phases (8-12), 25 plans, ~134 min total execution time
- 2 days (Feb 7 -> Feb 9, 2026)

**Git tag:** v5.6.0

---

## v5.7.0 Responsa Search (Shipped: 2026-02-10)

**Delivered:** Added Responsa Project-style advanced search to both web and desktop apps -- syntax parsing with wildcards, grammatical prefix/suffix expansion, Judeo-Arabic article forms, flexible spacing, bidirectional gap search, tabular query builder, and combinatorial explosion guards.

**Phases completed:** 14-17 (14 plans total, including 5 gap closure plans)

**Key accomplishments:**

- Responsa query parser with full syntax: `#`prefix, suffix`#`, `*`wildcards, `(%/%)` plene/defective, `(a/b)` OR groups, `[N]` gap notation
- Hebrew grammatical expansion (24 prefix forms + 25 suffix forms per word) with sofit letter conversion
- Judeo-Arabic definite article expansion (8 forms per word) with simplified al- model
- Combinatorial explosion guard with 6-step cascade (MAX_EXPANDED_TERMS=500)
- Responsa as first-class dropdown mode in both web and desktop, with sub-option checkboxes and syntax legend
- Tabular query builder dialogs (NiceGUI + PyQt6) with 2-4 components, per-word modifiers, live preview, one-way sync
- 221 automated Responsa tests: parity (all 16 checkbox combos), regression (30 non-Responsa modes), edge cases, performance

**Stats:**

- 71 files modified
- +12,670 / -213 lines
- 4 phases (14-17), 14 plans, 2 days (Feb 9 -> Feb 10, 2026)
- 25/25 requirements satisfied (audit passed)

**Git tag:** v5.7.0

---

## v5.7.2 Cleanup, Normalization & Sections (Shipped: 2026-02-11)

**Delivered:** Removed dead AI code, added Unicode search normalization (diacritics + apostrophe variants), fixed all pre-existing test failures, and rebuilt PGP transcription import with structural HTML parsing for correct recto/verso section display.

**Phases completed:** 18-21 (11 plans total, including 2 gap closure plans)

**Key accomplishments:**

- Purged all AI Search artifacts from both apps (314+ lines, google-genai dependency removed)
- Unicode search normalization: combining marks, geresh, and apostrophe variants stripped at query time with mark-tolerant highlighting
- Full green test suite: fixed 17 pre-existing failures, deleted 3 obsolete backend tests, 447 tests passing
- Structural HTML section parser for PGP transcriptions (replaces fragile regex with canvas-based parsing)
- Sections JSONB schema migration with language/direction metadata for structured section display
- Cross-app display parity for canvas sections with language-based translation ordering

**Stats:**

- 75 files modified
- +8,799 / -1,546 lines
- 4 phases (18-21), 11 plans, 83 commits
- 1 day (Feb 10-11, 2026)
- 13/13 v5.7.1 requirements satisfied + Phase 21 bonus

**Git tag:** v5.7.2

---

## v5.7.3 Pending Corrections Visibility (Shipped: 2026-02-11)

**Delivered:** Added pending corrections visibility to both web and desktop apps — users can now see their own unapproved corrections as selectable versions in the version selector while browsing manuscripts, with visual distinction from approved corrections.

**Phases completed:** 22-24 (3 plans total)

**Key accomplishments:**

- Shared pending corrections data layer (client-as-parameter pattern for both apps)
- Web version selector shows pending corrections with amber/orange styling and schedule icon
- Desktop pending corrections verified in Browse tab and Reading Desk (9 verification tests)
- 20 new tests across corrections service, web UI, and desktop verification
- Fixed NiceGUI timer parent_slot RuntimeError (bonus bugfix)

**Stats:**

- 26 files modified
- +2,184 / -18 lines
- 3 phases (22-24), 3 plans, 5 tasks
- 1 day (Feb 11, 2026)
- 6/6 requirements satisfied (audit passed)

**Git tag:** v5.7.3

---

## v5.8.0 FJMS Integration (Shipped: 2026-02-15)

**Delivered:** Integrated FJMS scholarly metadata (domain classifications, scientific joins, catalog records) into GenizahSearch via a SQLite sidecar database, enabling subject-based filtering and enriched manuscript display in both web and desktop apps.

**Phases completed:** 25-28 (12 plans total, including 5 gap closure plans)

**Key accomplishments:**

- SQLite sidecar database (762K rows) exported from 13GB FIST.db with domains, joins, catalog tables, and FTS5 index
- Shared FjmsService with 8 query methods, thread-safe SQLite for web, graceful degradation when sidecar missing
- FJMS scholarly join groups with scholar attribution merged into Related Fragments panel (purple badge, three-source dedup)
- Domain classification badges on browse page with hierarchical search filtering and standalone domain browsing
- Post-search dynamic domain filter with checkbox tree dialog in both apps (exclude-by-unchecking pattern)
- FJMS catalog enrichment: titles, authors, dates, content identifications alongside PGP metadata in both apps

**Stats:**

- 22 source files modified
- +6,323 / -69 lines (Python)
- 4 phases (25-28), 12 plans, 44 commits
- 3 days (Feb 12 -> Feb 15, 2026)
- 19/19 requirements satisfied (audit passed)

**Git tag:** v5.8.0

---

## v5.9.0 Multi-Source Image & Metadata Integration (Shipped: 2026-02-16)

**Delivered:** Imported NLI crossreference data (815K image-level records) and Cambridge IIIF manifests (141K URLs) into a second SQLite sidecar, plus Manchester LUNA and JTS/Princeton Figgy integration, enabling direct image access across 75+ libraries, physical metadata, scholarly bibliography, and library-specific viewer links in both apps.

**Phases completed:** 29-34 (22 plans total, including 3 gap closure plans)

**Key accomplishments:**

- NLI crossref sidecar (815K records, 253K distinct AlmaIds) with NliCrossrefService (16 query methods)
- Cambridge IIIF (141K manifest URLs) for direct image resolution, bypassing NLI
- Folio navigation with scholarly notation (1r/1v) and multi-source image switching (NLI, Cambridge, Manchester, JTS)
- FIST bibliography (542K denormalized references) with mention type badges and scholar attribution
- Catalog cross-references (64K entries across 80 catalogs), Neubauer-Cowley numbers, physical metadata
- Manchester LUNA (28K) and JTS/Princeton Figgy (453) integration with detail page links and IIIF manifests

**Stats:**

- 76 commits, 6 phases (29-34), 22 plans
- 6 days (Feb 10 -> Feb 16, 2026)
- 11/14 requirements satisfied, 1 invalidated (FGP != FL), 2 deferred (REL-01/REL-02)

**Git tag:** v5.9.0

---

## v6.0.0 Local Data Architecture (Shipped: 2026-02-22)

**Delivered:** Migrated all PGP reference data from Supabase to a local SQLite sidecar (pgp.db, 147MB) and added FJMS catalog descriptions as a scholarly resource, making browsing fully offline-capable and eliminating cloud dependency for read-only data. Additionally stabilized the app with crash fixes, pagination, and analytics, and optimized performance with parallel NLI fetch, async domain enrichment, and variant cache unification.

**Phases completed:** 35-40 (21 plans total: 8 core + 8 bug-fix/cleanup + 5 performance optimization)

**Key accomplishments:**

- PGP data migrated to local pgp.db sidecar (147MB, 104K rows across 5 tables) -- zero Supabase dependency for read-only data
- PgpService rewritten for SQLite with sub-millisecond local queries replacing 50-200ms API calls
- FJMS catalog descriptions expanded with 4 new tables (~1.7M rows), dedicated 5-section scholarly dialog in both apps
- Desktop offline PGP browsing verified, sidecar update mechanism for future data updates without app reinstall
- All desktop Qt lifecycle crashes fixed (sip.isdeleted guards), 200-result cap replaced with PAGE_SIZE=50 pagination
- Performance optimizations: parallel NLI fetch, browse crossref parallelization, FL ID O(1) index, variant cache unification

**Stats:**

- 155 commits, 122 files changed, +25,123 / -4,595 lines
- 6 phases (35-40), 21 plans
- 6 days (Feb 16 -> Feb 22, 2026)
- 14/14 requirements satisfied (audit passed)

**Git tag:** v6.0.0

---

## v6.1.0 Catalog Browse & Navigation (Shipped: 2026-02-27)

**Delivered:** Added faceted catalog browsing by domain hierarchy, author, and work title in both web and desktop apps, with FIST v5.0.0 enrichment data (genizah_persons, genizah_titles, code_values) and cross-links between browse and catalog browse pages.

**Phases completed:** 41 (4 plans total)

**Key accomplishments:**

- Faceted browsing by FJMS domain hierarchy, author (801 from v5.0.0), and work title (663)
- FIST v5.0.0 enrichment: genizah_persons (2,286), genizah_titles (775), code_values (3,440)
- FTS5+domain text filter for catalog browse
- Cross-links between browse page and catalog browse page
- 72 tests

**Stats:**

- 1 phase (41), 4 plans
- 1 day (Feb 27, 2026)

**Git tag:** v6.1.0

---
