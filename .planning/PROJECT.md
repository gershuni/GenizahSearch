# GenizahSearch

## What This Is

A research platform for the Cairo Genizah that combines manuscript image browsing with scholarly data from Princeton Geniza Project (PGP) and Fragment of the Jewish Manuscript Studies (FJMS). Users can view human-curated transcriptions from multiple scholars, browse rich document metadata with domain classifications and catalog enrichment, navigate fragment relationships including scientific joins, search across ~217,000 manuscript records with domain-based filtering, and perform advanced Responsa-Project style searches with grammatical expansion, Judeo-Arabic support, and a visual query builder. All scholarly reference data served from local SQLite sidecars for offline-capable, sub-millisecond browsing. Available as both a NiceGUI web app and a PyQt6 desktop app.

## Core Value

**Researchers can find what they need in the Genizah corpus.** The platform brings together manuscript images, scholarly transcriptions, PGP metadata, FJMS domain classifications, scientific joins, catalog records, and powerful search tools -- from simple keyword search to Responsa-Project style syntax with grammatical prefix expansion, Judeo-Arabic forms, and flexible spacing.

## Current Milestone: v8.0.0 Dicta Rebrand & Joins Lab

**Goal:** Ship the flagship **v8.0.0** "Dicta Genizah Search Pro" release — the desktop rebrand (delivered) and LOCAL ("My Library") export support (delivered, Phases 103 + 105) bundled with the new **Joins Lab**: an interactive, **human-in-the-loop** join-hunting workbench (both apps) where a scholar keeps ONE anchor fragment in view (image + numbered transcription) and drives the app's EXISTING search tools to find the fragments that physically join it. There is NO automated join-finder — the scholar is the ranker and confirmer.

**Bundles (already delivered, folded in from the v7.17 cycle):**
- Desktop rebrand → "Dicta Genizah Search Pro" — display name only; binary identifiers unchanged so installs upgrade in place (commit `6e0c312d` + follow-ups).
- LOCAL ("My Library") export across XLSX/CSV/TXT/DOCX with a bilingual "Local Documents" sheet — Phases 103 + 105 (closes **D-F17**).

**New build — Joins Lab (two independent components, both apps):**
- **Component A — Join Workbench (primary):** dedicated tab/page; pinned anchor (image + numbered transcription); "Find joins" entry from desktop ResultDialog + Browse (web+desktop); show existing/known joins (PGP+FJMS+user+community); conservative `[`/`]` tear-side assist (only when clear); seed searches from the anchor into the existing search module (variants/fuzzy/Responsa/regex); collect candidates to a list; side-by-side compare; act on a confirmed join (joins button + export + add-to-list; optional open-in-Puzzle).
- **Component B — Search-support algorithms (secondary, independent):** parallels seeded from the anchor; corpus-driven suggest-then-search completion (first/last N words); `[`/`]`-aware torn-word completion.

**Key context:** The MVP rides existing primitives — line-start/line-end search, gap notation, parallels, the joins panel, the Fragment Puzzle (both apps), and the variant/fuzzy/Responsa/regex search module — so it is largely UI composition + seed helpers, not new search-engine work. No new index or sidecar (`line_starts`/`line_ends` already present on web + most desktop users). The slow research-only auto-ranked v7/v8 join finder is explicitly OUT of scope. Requirements (JWB-01..09, JSA-01..03) in `.planning/REQUIREMENTS.md`. **The roadmap is intentionally deferred pending a Genizah-scholar design-critique session that will pressure-test the design against the real material nature of the corpus before phases are locked.** Origin: Spike 002 (`.planning/spikes/002-assisted-join-workbench/SPIKE-FINDINGS.md`) + `docs/FEATURE_IDEAS.md` + `docs/archive/JOIN_FINDER_REPORT.md`.

## Current State (v8.0.0 Joins Lab — Phase 109 visual-similarity merge & soft-retire complete 2026-06-08)

**Phase 109 complete (verified 3/3 SC; UAT-approved by Hillel after 3 gap-closure rounds + a 6-finding round-4 polish loop):** the desktop candidate surface absorbed the **visual-similarity look-alike source** via the shared VS service, replacing the standalone Visual Similarity dialog. The 3-radio Text/Visual/Combined selector became a single **"Visual Similarity" toggle** (👁): ON + empty box = pure VS look-alikes; ON + term = the text∩VS **intersection**; OFF = text results with a 👁 badge on any candidate that is also a look-alike. A **single eye badge** (G-06/G-09, replacing ★both/⊙VS/rank) marks visual look-alikes consistently across the **grid, table, and Compare** surfaces; ⚓self / ⇄other-side precedence preserved. The Browse + ResultDialog standalone VS buttons were removed (G-07) — **"Find Joins" is the single entry**; the JoinsDialog VS button became a 🔗 plain Join-Lab open (G-08, retiring the pick-back). `_show_vs_dialog` is **deprecated/marked-removable** (D-11 one-cycle soft-retire; physical deletion deferred to a cleanup phase). Round-4 polish fixed a `_EnrichWorker` QThread-destroyed-mid-run crash (Windows 0xC0000409) and its `_PageTextWorker` twin (code-review WR-01), Compare-pane client-side zoom + page-scoped transcription text, ✓/?/✗ triage glyphs, RTL nav arrows, and **Join Lab session persistence** (single reusable window + carry-forward of `join_lab` on jw-None saves so state survives close/reopen AND app restart). JWB-12 satisfied; enrichment stays batched (one IN-query). ruff clean; automated gate 62 green; Codex/code-review pass with WR-01 fixed.

## Current State (v8.0.0 Joins Lab — Phase 108 desktop query builders + candidates + compare complete 2026-06-06)

**Phase 108 complete (verified 7/7 must-haves; UAT-approved by Hillel after a full UI redesign + 3 polish rounds):** the desktop Joins Lab gained the candidate-hunt half. A line-by-line `JoinQueryBuilder` (rows = manuscript lines; per-row OR word-boxes; per-line ⚙ for modifiers + ⊢/⊣ start/end with parser-hoisted forms `#(a/b)`/`-(a/b)`/`(a/b)*`; inline gap; a global "Search options" dialog; typed-sign pass-through with an ⓘ legend) drives the EXISTING engine for the anchor side AND an other-side builder (cross-side AND-narrow/OR-widen via `apply_cross_side`). Results render as deduped one-per-image `Candidate`s in a grid (icon-only actions, per-card checkbox + folio `<>` flipping image+text, right-click context menu) and an 8-column table (checkbox + master select-all, shared bulk-action bar with single-only Browse/Join). A side-by-side `CompareDialog` (anchor ↔ candidate, per-pane material/dims + zoom + folio nav, Y/?/N triage colored border, three entry points) closes JWB-08. Filter is a button → dialog with current-fragment info + "from anchor" shortcuts. "Add to Puzzle" includes the anchor (`open_anchors_in_puzzle`). Join Lab state persists (input-only, re-run on restore) for crash recovery + deliberate close, reachable from a main-window 🔗 icon. Hebrew material terms (נייר/קלף…); RTL-correct arrows. Codex-pre-flighted; zero `_vs_` on the workbench path; parser/i18n/no-private/construction gates green throughout.
- JWB-06 (reframed), JWB-07, JWB-08, JWB-10, JWB-11, + JWB-12 text/combined surface satisfied. JWB-05 tear-side assist deferred to Phase 110.

## Current State (v8.0.0 Joins Lab — Phase 107 desktop Joins Lab shell complete 2026-06-04)

**Phase 107 complete (verified passed 6/6 SC; UAT-approved by Hillel after 3 live rounds):** the desktop **Joins Lab** window shipped — a modeless anchor-pinned shell (`desktop/join_workbench.py`, ~1.5K lines) opened via a **"Find joins"** action from ResultDialog and Browse, plus cold-start by shelfmark / 📋 pick-from-list. Anchor pane: fit-to-view image with drag-pan + zoom + folio nav, right-aligned (RTL) numbered transcription. Known-joins shown as a connected group (PGP + FJMS + user + community) in a collapsible/resizable left-pane panel with source badges, per-row + bulk "add selected to puzzle" (anchor auto-pinned, deduped), and a chain-icon joins-context dropdown. "Add as Join" persists via the existing pairwise path and refreshes the group. All actions go through public host methods (no `_vs_*` on the workbench path); fully bilingual.
- 3 plans / 3 waves (107-01 i18n + pure helpers + test scaffolding → 107-02 `JoinWorkbenchWindow` shell → 107-03 host wiring + entry points). 78 phase tests + `puzzle_add_targets` units; ruff clean. Feature named **"Joins Lab"** in English (matches Hebrew מעבדת צירופים).
- UAT folded 15 refinements (G1–G15) across 3 rounds into the shell; a general puzzle-window fix (whole-fragment fit on single add) rode along.
- Code review (advisory): 0 Critical, 3 Warning, 5 Info — open for follow-up: **WR-01** transitive-edge member could drop on reverse storage order; **WR-02** `ThumbBatchWorker` bypasses the Phase-98 NLI circuit breaker. `/gsd-code-review-fix 107` available. Security artifact not yet generated — `/gsd-secure-phase 107` available.
- **Next:** `/gsd-discuss-phase 108` (query builders, candidates & compare — the candidate-hunt surface in the right pane).

## Current State (v8.0.0 Joins Lab — Phase 106 shared core complete 2026-06-03)

**Phase 106 complete (verified passed, 6/6 success criteria):** the first build phase of the Joins Lab milestone landed the shared, web-reusable, unit-tested pure-logic core in `shared/joins_lab.py` (~750 lines) + `tests/test_joins_lab.py` (66 tests, all green; ruff clean; imports with no PyQt and no `fist_data` sqlite).
- 3 plans across 3 sequential waves (single-file module → forced sequential mutation): 106-01 domain model (frozen `BuilderRow`/`SideQuery`/`Candidate`/`MergeResult`) + `SearchExecutor` Protocol + `normalize_candidate()`/`page_of()` + `compose()` (SC#1) + static AST import guard (SC#6); 106-02 `resolve_other_side_pages`/`cross_side_membership`/`apply_cross_side` (SC#2) + `dedup_candidates` (SC#3) + `merge_candidates` provenance ordering (SC#4); 106-03 `detect_self_match` + `_match_line` + centered `htmlify`/`snippet_html`/`snippet_plain` (SC#5).
- Delivers the **foundational pure logic** for JWB-10/JWB-11/JWB-12 (the UI that completes these requirements lands in Phases 107–109) and demonstrates the ARCH build constraints (no PyQt, no direct `fist_data/*.db`, web-reusable, `SearchExecutor` adapter seam).
- Code review (advisory, non-blocking): 0 Critical, 2 Warning, 3 Info. Open non-SC findings for the Phase 107+ adapters: **WR-02** `merge_candidates()` drops `vs_score` on overlap annotation (real one-line correctness fix), **WR-01** `htmlify()` doesn't strip pre-existing highlight sentinel bytes from corpus text. `/gsd-code-review-fix 106` available.
- **Next:** `/gsd-discuss-phase 107` (desktop Join Workbench frame + entry points + actions + pairwise→group join model). Security artifact not yet generated — `/gsd-secure-phase 106` available (each plan carries an inline STRIDE threat_model with all threats dispositioned accept/mitigate).

## Current State (v7.16 Hebrew PDF Text Quality shipped 2026-06-01)

**Shipped:** v7.16 Hebrew PDF Text Quality (desktop only, 2026-06-01; tag `v7.16.0` @ `ccb87c90`)
- 1 formal phase (102, 5 plans) + post-phase no-phase quality/UAT/freeze work. 61 commits since v7.15.0; 104 files, +34,513/−441; 2026-05-28 → 2026-06-01.
- **Phase 102 — LOCAL PDF text-layer extraction rewrite:** `extract_pdf_pages` rebuilt on a `page.get_text("rawdict")` per-glyph foundation (`shared/local_indexer_rtl.py`): RTL-gated Meiri reorder core (no LTR regression), Unicode-`Mn` nikud/te'amim classification (preserves maqaf `־`), per-line 1-D Otsu word-gap valley de-space, `_ltr_damage_guard` RTL-trust fix (the real production blocker), corrupt_encoding detection. Nikud stripped once for all LOCAL formats. `extraction_format_version` 2→3 (existing libraries need one "Re-index All"). אוצר הגאונים single-letter tokens 73.5%→~3-5%, רביצקי word-merge 15.8%→0.07%. ~150 tests; validated by Spike 001.
- **No-phase work:** de-space follow-ups (D-F13b/c/d — edge-gap+Otsu, launch-freeze deferral, zero-width space-glyph boundary, number bidi), LOCAL UAT extraction fixes (D-F19..D-F22 HTML/xlsx/CSV + folder opt-out cascade BLOCKER, D-F25 apostrophe crash), file-management actions for LOCAL hits (D-F24), and three search/startup freeze fixes (D-F23: 778 MB `search_history.json`, large-folder O(n²) startup, LAB-rebuild churn).
- Tagged `v7.16.0` via `/release` (desktop only; GitHub Release with installer, marked latest; CI green). Closed via `/gsd-complete-milestone` 2026-06-01.
- **Carried forward:** D-F12 (regular Search ~8s wall-clock — leading next-milestone candidate), D-F17 (LOCAL/ALL export), D-F18 (context-menu LOCAL detection).

## Current State (v7.15 My Library Visual shipped + closed 2026-05-28)

**Shipped + closed:** v7.15 My Library Visual (closed 2026-05-28)
- 3 phases (99 PDF Page Renderer + 100 LOCAL PDF Image in ResultDialog + Browse + 101 RTL/reflow polish), 7 plans, 6/6 PDFIMG-* requirements. Desktop-only — web "My Library" does not exist.
- **Phase 99:** Shared on-demand `desktop/pdf_image_controller.py` renderer + `PdfRenderWorker` long-lived QThread; bounded LRU of open `fitz.Document` handles; no on-disk image cache; render failures (missing/corrupt/encrypted/out-of-range) return graceful placeholder + log entry instead of UI hang.
- **Phase 100:** Wired the renderer into both desktop surfaces. `ResultDialog` shows the rendered page image alongside extracted text and re-renders on result navigation. Browse panel reveals the previously-hidden image pane and syncs prev/next page with the text. Non-PDF LOCAL files (`.docx`/`.html`/`.xlsx`/`.csv`/`.txt`) stay text-only — image pane gated on file extension. `PdfImageController` does token + latest-wins + 150ms debounce + 8s watchdog.
- **Phase 101 (pre-release polish):** S-1 directional-run RTL/bidi word-order reversal in `shared/local_indexer.py::extract_pdf_pages` (gated on `_rtl_ratio > 0.4`; Latin shelfmarks like `T-S 12.123` stay adjacent). D-04 auto-self-heal ROLLED BACK post-UAT (12K-PDF library froze launch). UAT-driven follow-ons: intra-block newline collapse (joined bidi-fragmented Hebrew paragraphs into continuous prose); LAB rebuild 5-failure bail + pre-flight callback probe (silenced 1.9M-warning log storm + 10s freeze); remove-folder batched commit + retry (closed Windows ERROR_ACCESS_DENIED storm); i18n leak in remove-folder dialog; new **"Re-index All"** button to force re-extraction via the background worker (recovers existing libraries after the RTL + reflow fixes).
- **D-F12 deferred (new):** Regular Search ~constant 8s wall-clock investigation — profile-first approach planned for v7.16+.
- Tagged `v7.15.0` via `/release` (deferred to release pipeline).

## Current State (v7.14 My Library shipped + closed 2026-05-27)

**Shipped + closed:** v7.14 My Library — Local Document Search (public release 2026-05-24; closed 2026-05-27)
- 6 phases (95, 96, 97, 97.2 INSERTED, 97.3 INSERTED, 98), 37 plans. Desktop "My Library" tab indexes user folders (`.docx`/`.pdf`/`.txt`/`.html`/`.xlsx`/`.csv`) into a separate Tantivy side-index merged into Search / Composition / Parallels via RRF k=60 POST-dedup; `LOCAL` badge + corpus selector; three cloud-write gates keep personal corpora off the cloud.
- Hardened across an internal hotfix chain: Phase 97 (scale to 13K files / 43 GB + `.html`/`.xlsx`/`.csv` + atomic rebuild + crash recovery), Phase 97.2 INSERTED (recovery cascade + Reset My Library), Phase 97.3 INSERTED (mega-folder UI-thread stability).
- Phase 98 (web infra): shared NLI circuit breaker wired into all 10 NLI/IIIF fetch sites; worst-case per-request blocking 45s → ~9s; closes the 2026-05-25 production hang. Tagged `v7.14.0` (both apps). See `.planning/milestones/v7.14-ROADMAP.md`.

**Shipped + closed:** v7.13 Research-Grade Downloads & PGP Filter (2026-05-21; closed 2026-05-27)
- 2 phases (93 web-only 3-state PGP filter; 94 web + desktop 4-sheet bilingual research-grade xlsx + web JSON flags), 5 plans, 14/14 requirements. Tagged `v7.13.0` (both apps). See `.planning/milestones/v7.13-ROADMAP.md`.

**Shipped:** v7.12 Multitenant Architecture (Path B) (2026-05-18)
- 10 phases (87-92 + 92.1/92.2 inserted + 999.1/999.4 promoted backlog), 28 plans, 49/49 requirements satisfied
- 131 raw `app.storage.user` accesses migrated through `web/safe_storage.py` chokepoint; allowlist driven to 0 entries; `tests/test_no_raw_storage_access.py` is the permanent CI guard
- 10 per-user `AppState` mirror fields deleted (state separation by deletion, not migration)
- `UserListsManager` singleton + 10s TTL plumbing deleted; per-request instantiation
- Process-wide auth client cache (4 globals + 2 helpers) deleted; request-scoped auth via local header mutation; refresh locks keyed by `_session_uuid`; NO `auth.set_session()` mid-flight (Codex constraint at `gotrue_client.py:713` respected)
- `sign_out` uses `throwaway.auth.admin.sign_out(jwt, "global")` for real server-side revocation
- Phase 92.1 (INSERTED) closed P0 RLS-reachability regression: 12 reader sites migrated from anonymous `get_client()` to authenticated `get_user_client()`
- Phase 92.2 (INSERTED) closed `/lists` 36s warm-render regression: task-scoped `WeakKeyDictionary` memo + zero-arg RPC + threading; **19.3x mean speedup**
- 5-surface SWEEP audit clean; SWEEP-05 smoke run 2 PASS 2026-05-18 (R0/R1/R2/cross-user concurrent)
- `docs/guides/MULTITENANT.md` shipped as architecture reference
- Promoted backlog: Phase 999.1 (search-result folio chip parity) + Phase 999.4 (line-number gutter web + desktop)
- `deploy.sh` UNBLOCKED. Git tag deferred to `/release` (web + desktop bundle)

**Shipped:** v7.11.1 Web Hotfix (2026-05-12)
- 4 user-reported bugs closed: cross-user xlsx export filename leak (the originating report), /help 500 (chained `set_visibility()` returning None), /browse 500 on pruned NiceGUI session AssertionError, lists "Sync Now" UX confusion (renamed + added "Refresh from Cloud")
- Deployed at commit `242664d3`; git tag `v7.11.1`; web-only (no GitHub Release per desktop-poll prompt avoidance)
- Server systemd override bumped `SEARCH_API_BROWSE_CORE_TIMEOUT` 2.0 → 5.0s to mitigate cold-Tantivy-reader race; applies on next restart
- 4 rounds of Codex code review of post-release hotfixes (`22b45f68 → cca23db3`) surfaced deeper architectural problem behind the export leak — singleton-thinking in web layer spanning `AppState`, `UserListsManager`, `get_user_client()` cache, raw `app.storage.user` reads at 30+ bootstrap sites. v7.12 Path B (shipped above) addressed each strand intentionally.

**Shipped:** v7.11.0 CUDL Coverage & Synthetic Inventories (2026-05-12)
- 3-phase milestone (Phases 84/85/86) closing the gap between CUDL's ~141K classmark catalogue and GenizahSearch's libraries.csv
- Phase 84 (NORM-01..04): FIST↔CUDL bridge modules with normalizers (Mosseri labels, Cambridge Or. numeric collapse, slash/comma/dot bug fixes, leading-zero collision audit); 6 bridge wiring call sites; 3-layer regression guard
- Phase 85 (SYNTH-01..06): Synthetic libraries.csv infrastructure with `is_synthetic_sys_id` helper, Option-2 18-digit numeric sys_id format, FJMS sidecar UNION-ALL pattern, browse hide-NLI gates, `is_synthetic` field on API responses, corrections-write reject
- Phase 86 (AUDIT-01..03): 108 image-bearing synthetic manuscripts injected (101 CUL + 7 Mosseri, all with CUDL canvas images via bridge); 3,264 catalog rows + 103 FTS5 docs surgically added to fjms_enrichment.db. T-S NS 329.96 (originating case) resolved. 5-tier coverage report: phase84_hit 96.23%, residue 1.13%.
- Deploy posture codified after 2026-05-11 incident: scp DBs FIRST, then push code. Web auto-deployed via deploy.sh atomic systemd swap.

**Shipped:** v7.10 Search API (2026-05-05)
- Public HTTP/JSON research-automation API: `POST /api/search`, `GET /api/browse`, `POST /api/parallels`
- Security hardening: per-IP rate limiter (default 30 req/min), `SEARCH_API_MODE` access gate (open/localhost-only/disabled), uniform error envelope, XFF spoofing protection, fail-closed filter validation, MAX_EXPANDED_TERMS=500 Responsa cascade cap, HMAC-hashed PostHog telemetry with persistent IP salt
- OpenAPI auto-generated at `/api/openapi.json` + Swagger UI at `/api/docs` + ReDoc at `/api/redoc` — sub-mounted, scoped to the 3 search-helper endpoints, legacy `/api/*` excluded
- Reference Anthropic Skill `cairo-genizah-research` (skills/cairo-genizah-research/) — staged phrase discovery, browse drill-down, R2 honesty annotations, file-locked token-bucket throttle (≤24 req/min/bucket; configurable via `GENIZAH_SKILL_REQ_PER_MIN`)
- `docs/SEARCH_API.md` reframed public-facing: Stability + Quick Start + Attribution + Changelog
- 8 phases, 37 plans, 36 in-traceability + 8 PUBLIC-* requirements satisfied
- Web-only release: no git tag, no GitHub Release object (desktop-poll prompt avoidance per project convention)
- Known follow-ups carried to v7.11: skill clarification turn, broader-than-domain mode, API gaps (uid/language/parallels-apostrophe/filter-vocab), rate-limiter sustained-load soak, SEARCH_API_MODE flip drill

**Shipped:** v7.8 Structural Foundation (2026-04-15)
- CI safety net: GitHub Actions (Ubuntu + Windows matrix) runs ruff + scripts/check_docs.py + pytest on every push/PR
- Reproducible builds: two-file dependency pinning (14 direct + 115 transitive, all exact `==`)
- Auth modernized: gotrue → supabase_auth, PKCE-only OAuth callback, dead implicit-flow endpoint removed
- Framework patches isolated: web/framework_patches.py with per-patch `packaging.version.Version` guards
- Exception hygiene: 205+ silent handlers across 76 first-party files audited (each logs or is justified)
- Repo cleanup: .gitignore 50→126 lines, root untracked files 67→1
- Documentation refresh: CODE_INDEX v7.8 sections, OPEN_ISSUES code review tracking, DEVELOPER_GUIDE CI/ruff/deps docs
- 4 phases, 9 plans, 64 commits, 12/12 requirements satisfied. Zero user-visible behavior changes.

**Shipped:** v7.7.0 Volume-Aware Browse (2026-04-01)
- IE volume data infrastructure: ie_volume_map.json for 3,193 multi-IE manuscripts with per-IE browse_map grouping
- Volume-aware web browse: selector dropdown, per-IE paging, volume-correct IIIF suffix loading
- Desktop volume-aware browse parity: volume selector, suffix-aware IIIF, search-to-browse IE propagation
- Community writes (corrections/comments) tagged with ie_id for per-volume attribution
- Session persistence for active volume (web URL + desktop state); shareable browse URLs include volume
- Stratified IIIF validation confirming 907→suffix mapping accuracy
- 13 commits, 39 files changed, 26/26 requirements satisfied

**Architecture:**
- Web: NiceGUI -> Tantivy (search) + SQLite sidecars (pgp.db + FJMS + NLI + libraries_translations.db + visual_similarity.db) + Supabase (community features only)
- Desktop: PyQt6 -> Tantivy (search) + SQLite sidecars (pgp.db + FJMS + NLI + optional visual_similarity.db cache) + Supabase (community features only)
- Shared: genizah_core.py (~8,300 lines -- search engine, metadata, variants, Responsa, filtered search)
- Shared: shared/document_service.py (PGP data from pgp.db SQLite)
- Shared: shared/corrections_service.py (corrections data access)
- Shared: shared/fjms_service.py (FJMS domain, join, catalog, bibliography, measurements from fjms_enrichment.db)
- Shared: shared/nli_crossref_service.py (NLI crossref, images, metadata, library URLs from nli_crossref.db)
- Shared: shared/visual_similarity_service.py (FIST SVM image similarity from visual_similarity.db)
- Shared: shared/exclusion_service.py (manuscript exclusion from lists/files/paste with shelfmark resolution)
- Shared: shared/translation_service.py (Dicta translations from pgp.db, fjms_enrichment.db, libraries_translations.db)
- Shared: shared/dicta_client.py (Dicta Translate API client with few-shot scholarly prompts)
- Shared: shared/translation_qc.py (translation QC heuristics)
- Shared: shared/puzzle_service.py (joins.db CRUD for puzzle documents)
- Shared: shared/puzzle_publish_service.py (Supabase publish/unpublish/fork/list)
- Shared: shared/puzzle_export.py (composite PNG export with metadata banner)
- Shared: shared/puzzle_image_service.py (IIIF fetch + HSV background removal + disk cache)
- Shared: shared/background_removal.py (HSV-based parchment isolation engine)

**Data:**
- pgp.db: 35,839 documents, 9,364 sources, 22,757 footnotes, 36,155 fragments, 34,954 translations (v1.0.0)
- manuscripts (libraries.csv): ~255,615 records (including 38K FIST gap fill, 3,193 multi-IE with volume data)
- libraries_translations.db: 184,514 title translations (76MB)
- fjms_enrichment.db: 390K domains, 48K joins, 685K catalog (37 cols), 427K bib (deduped), 64K catalog_refs, ~260K translations, 1.5M computed measurements (v5.0.0)
- nli_crossref.db: 815K NLI images, 141K Cambridge manifests, 28K Manchester LUNA, 36,283 JTS DPUL (v2.0.0)
- visual_similarity.db: ~15.5M pairs from FJMS SVM image analysis (server-only, ~500-700MB)

## Requirements

### Validated

<details>
<summary>v1 through v5.9.0 requirements (55 items)</summary>

- Search MiDRASH auto-transcriptions (V0.8/V0.7) per page -- existing
- User correction submissions with approval workflow -- existing
- Version selector showing V0.8 + user corrections -- existing
- Pairwise fragment joins for navigation -- existing
- Shelfmark normalization with 96.5% PGP match rate -- existing
- PGP transcriptions appear as a version source (primary when available) -- v1
- Document-level entity for multi-fragment PGP records (joined manuscripts) -- v1
- Unified viewer: all images from joined fragments in sequence -- v1
- PGP metadata display: type, tags, dates, descriptions in browse view -- v1
- Search results indicate when PGP transcription available -- v1
- Multi-source selector: switch between scholars' editions and translations -- v1
- Tag-based search from PGP metadata -- v1
- Shared service layer for Supabase access -- v5.6.0
- Desktop PGP feature parity (transcriptions, metadata, joins, tag search, version selector) -- v5.6.0
- Virtual Reading Desk (multi-manuscript viewer in both apps) -- v5.6.0
- Responsa syntax parsing: wildcards, grammatical prefixes/suffixes, OR groups, plene/defective, gap notation -- v5.7.0
- Judeo-Arabic definite article expansion (8 forms, simplified al- model) -- v5.7.0
- Flexible spacing for OCR error tolerance (basic + advanced on original terms) -- v5.7.0
- Bidirectional gap search (both word orders) -- v5.7.0
- Combinatorial explosion guard with 6-step cascade (MAX=500) -- v5.7.0
- Web UI: Responsa as dropdown mode with sub-options, syntax legend, URL state -- v5.7.0
- Desktop UI: Responsa as combo mode with sub-options, syntax legend -- v5.7.0
- Tabular query builder (web dialog + desktop QDialog) with 2-4 components, per-word modifiers -- v5.7.0
- Cross-app parity: identical Responsa results in web and desktop (221 tests) -- v5.7.0
- AI Search artifacts removed from both apps (desktop + web + help docs + core) -- v5.7.2
- Unicode search normalization: combining marks, geresh, apostrophe variants stripped at query time -- v5.7.2
- Mark-tolerant search highlighting (matches through interleaved combining marks) -- v5.7.2
- Full green test suite: 447 tests passing, 0 failures -- v5.7.2
- Structural HTML section parser for PGP transcriptions (canvas-based, replaces regex) -- v5.7.2
- Sections JSONB schema with language/direction metadata per source -- v5.7.2
- Cross-app canvas section display with language-based translation ordering -- v5.7.2
- Pending corrections visible as selectable version in web version selector (amber styling, schedule icon) -- v5.7.3
- Pending corrections visible in desktop Browse tab and Reading Desk (emoji labels) -- v5.7.3
- Shared corrections service with auth-filtered pending corrections (only submitter sees own) -- v5.7.3
- Export FJMS domain classifications, scientific joins, and catalog records into SQLite sidecar -- v5.8.0
- Domain-based filtering in search results (both apps) -- v5.8.0
- Domain display on browse page (both apps) -- v5.8.0
- FJMS join group display with scholar attribution on browse page (both apps) -- v5.8.0
- Catalog enrichment display (titles, authors, dates) on browse page (both apps) -- v5.8.0
- FTS5 schema in sidecar (UI deferred) -- v5.8.0
- NLI crossref sidecar (815K records) imported with NliCrossrefService (16 methods) -- v5.9.0
- Cambridge IIIF manifests (141K) imported into sidecar with local image resolution -- v5.9.0
- Cambridge images load via local CUDL IIIF, bypassing NLI -- v5.9.0
- Image availability source indicators and folio navigation in both apps -- v5.9.0
- Physical metadata (material, folios) and library collection links (KTIV, CUDL, LUNA, DPUL) -- v5.9.0
- FIST bibliography (542K references) with mention type badges and scholar attribution -- v5.9.0
- Catalog cross-references (64K entries across 80 catalogs), Neubauer-Cowley -- v5.9.0
- Manchester LUNA (28K IDs) and JTS/Princeton Figgy integration with detail page links -- v5.9.0

</details>

- PGP data (documents, sources, footnotes, fragments) exported to pgp.db sidecar -- v6.0.0
- document_service.py rewritten to read from SQLite instead of Supabase -- v6.0.0
- Both web and desktop apps use pgp.db for all PGP reference data -- v6.0.0
- JSON data (tags, sections) preserved correctly in SQLite with query parity -- v6.0.0
- Search result enrichment (PGP metadata batch lookup) uses pgp.db -- v6.0.0
- PGP tag-based search uses SQLite json_each() instead of Supabase -- v6.0.0
- FJMS catalog descriptions exported and accessible via dedicated dialog in both apps -- v6.0.0
- pgp.db bundled in desktop installer and web deployment -- v6.0.0
- Desktop PGP browsing works without internet (images excluded) -- v6.0.0
- Paginated search results (PAGE_SIZE=50) replacing 200-result cap -- v6.0.0
- PostHog analytics integrated alongside Google Analytics -- v6.0.0
- Desktop crash fixes (sip.isdeleted guards on all Qt lifecycle sites) -- v6.0.0
- Performance: parallel NLI fetch, browse crossref parallelization, variant cache unification -- v6.0.0
- Search UX: elapsed timer, ETA, partial results on cancel, chunk count, min-chunks filter, CreationType badge (both apps) -- v6.5.0
- Session persistence: full state + exclusion restore on reopen, search/composition history dropdowns (both apps) -- v6.5.0
- Quick UX wins: desktop notifications, sleep prevention, Hebrew library names (81 codes), copy context menu -- v6.5.0
- Bidirectional filtered search: pre-search filtering by domain/author/work/date/material across all modes (both apps) -- v6.5.0
- Dicta translation: ~580K translations for multilingual access with translation toggle (both apps) -- v6.5.0

- Fragment puzzle canvas: visual assembly tool for physical joins with drag, rotate, flip, resize (both apps) -- v7.0.0
- Background removal: HSV-based segmentation with two-pass detection for colored mats -- v7.0.0
- Join document creation: composite image + metadata saved to local joins.db, publishable for community review -- v7.0.0
- Recto/verso support: auto-generated verso from recto arrangement with correct verso images -- v7.0.0
- Community publishing: publish/fork/browse puzzle joins via Supabase with RLS -- v7.0.0
- Manuscript dimensions display in browse/results with summary, catalog, computed, blank image sizes (both apps) -- v7.3.0
- Pre-search dimension range filter (min/max width/height/line height) across all search modes (both apps) -- v7.3.0
- Post-search dimension filtering within results via expandable panel (both apps) -- v7.3.0
- Search within results: progressive refinement restricting queries to current result set, breadcrumb chain, per-chip removal (both apps) -- v7.4.0
- Lightweight browse first-render: zero SQLite calls in hot path, deferred enrichment in Phase B (web) -- v7.4.0
- Bracket-aware search: scholarly notation brackets preserved through search pipeline (both apps) -- v7.4.0

- Exclude known manuscripts from search via saved lists, imported files, or pasted shelfmarks with resolution report (both apps) -- v7.5.0
- FIST visual similarity suggestions in browse with ranked partners and action buttons (both apps) -- v7.5.0
- Search within FIST visual suggestion partner pools with union/intersection modes (both apps) -- v7.5.0

- Volume-aware browse: IE-specific IIIF manifest loading, per-IE paging, volume selector for multi-IE manuscripts (both apps) -- v7.7.0
- Search→browse IE propagation: clicking a search result opens the matching IE/volume (both apps) -- v7.7.0
- Community writes include ie_id context for per-volume corrections and comments -- v7.7.0
- Session persistence for active volume with shareable browse URLs including volume parameter -- v7.7.0

- GitHub Actions CI (Ubuntu + Windows matrix) running ruff, scripts/check_docs.py, pytest on every push and PR -- v7.8
- Two-file dependency pinning: requirements.txt (14 direct) + requirements-lock.txt (115 transitive), exact `==` pins -- v7.8
- Ruff scoped ruleset (E9/F401/F811/F821) with zero-violation baseline across 105 source files -- v7.8
- Supabase auth migrated from deprecated gotrue to supabase_auth.errors in web and desktop clients -- v7.8
- PKCE-only OAuth callback (implicit flow removed) with error parameter handling and dead /api/auth/oauth-callback endpoint removed -- v7.8
- NiceGUI monkey-patches isolated in web/framework_patches.py with packaging.version version guards -- v7.8
- 205+ silent exception handlers across 76 first-party files audited (each logs or has justification comment) -- v7.8
- .gitignore root debris cleanup (50→126 lines, untracked root 67→1) with exempted intentional assets -- v7.8
- Documentation refresh: CODE_INDEX.md v7.8 sections, OPEN_ISSUES.md code review tracking, DEVELOPER_GUIDE.md CI/ruff/deps workflow -- v7.8

- Public HTTP/JSON research-automation API: /api/search, /api/browse, /api/parallels with rate limiting + access mode gate -- v7.10
- OpenAPI auto-generated at /api/openapi.json + Swagger UI at /api/docs scoped to public endpoints -- v7.10
- Reference Anthropic Skill cairo-genizah-research with file-locked token-bucket throttle and browse-honesty annotations -- v7.10
- Security hardening: XFF spoofing protection, fail-closed filter validation, MAX_EXPANDED_TERMS=500 cascade cap, HMAC-hashed PostHog telemetry with persistent IP salt -- v7.10
- docs/SEARCH_API.md public-facing reference (Stability + Quick Start + Attribution + Changelog) -- v7.10

- FIST↔CUDL bridge with shelfmark normalizers (Mosseri label form, Cambridge Or. numeric collapse, CUL slash/comma/dot/leading-zero fixes) recovering thousands of CUDL classmarks masked by format mismatch -- v7.11
- 6 bridge wiring call sites across genizah_core.py, web/services.py, web/pages/browse_enrichment.py, image-source resolution, CUDL link builder, orphan scanner -- v7.11
- 3-layer regression guard: cudl_must_resolve fixture, cudl_baseline_resolved snapshot, unit tests for bridge normalizers -- v7.11
- Synthetic libraries.csv infrastructure: is_synthetic_sys_id helper, Option-2 18-digit numeric sys_id format (99 + InventoryId-padded-10 + 000000) preserving sys_id "starts with 99" contract -- v7.11
- Browse + search + lists + exclusions + parallels + comments tolerate synthetic sys_ids; FJMS enrichment lookups fall back to InventoryId resolution; corrections-write reject at service layer -- v7.11
- is_synthetic field on /api/search + /api/browse + /api/parallels response items + PostHog event property -- v7.11
- 108 image-bearing synthetic manuscripts injected (101 CUL + 7 Mosseri with CUDL canvas images via bridge); T-S NS 329.96 originating case resolved -- v7.11
- CUDL coverage 5-tier audit report: phase84_hit 96.23%, phase86_existing_alma_candidate 2.39%, phase86_synthetic 0.08%, phase86_residue 1.13%, multi_inventory_ambiguous 0.18% -- v7.11
- Browse pagination fixes for synthetic sys_ids (web + desktop): metadata_only mode derives total from largest of folio_images/images_ext/images_nli; bypasses Tantivy via is_synthetic_sys_id short-circuit -- v7.11
- NLI library code data fix: 461 manuscripts flipped Oxford → NLI (call_numbers contained only NLI shelfmarks) -- v7.9.4

- Cross-user xlsx export filename leak fixed (route export payload through per-session storage, not AppState singleton) -- v7.11.1
- /help 500 fixed (chained set_visibility() returning None) -- v7.11.1
- /browse 500 fixed (AssertionError on pruned NiceGUI session state during browse render) -- v7.11.1
- Lists "Sync Now" UX clarified (renamed + added explicit "Refresh from Cloud" button) -- v7.11.1

- ✓ `_session_uuid` minted on first request, stored in `app.storage.user`, stable across token refresh -- v7.12 (FOUND-01)
- ✓ `web/safe_storage.py` adopted as the single chokepoint adapter; 131 raw `app.storage.user` access sites migrated; allowlist driven to 0 entries -- v7.12 (FOUND-02..04)
- ✓ AST-based pytest lint scanner enforces zero raw `app.storage.user` accesses under `web/` (permanent CI guard) -- v7.12 (FOUND-04)
- ✓ 10 per-user `AppState` mirror fields deleted; `web/export_state.py` is the sole path for per-user export state -- v7.12 (STATE-01..06)
- ✓ `_TEST_BACKEND` shim removed; tests use `SimpleNamespace`-based fixture injection through `web.safe_storage.app` monkeypatching -- v7.12 (STATE-04..05)
- ✓ Cross-user xlsx + parallels export leak structurally impossible (SWEEP-05 smoke run 2 PASS 2026-05-18) -- v7.12 (STATE-01..06, SWEEP-05)
- ✓ `UserListsManager` singleton + `_cache_entry` tuple + 10s TTL plumbing deleted; per-request instantiation in page handlers -- v7.12 (LISTS-01..04)
- ✓ Process-wide `_client_cache` + `_session_locks` + `_locks_guard` + `_CLIENT_CACHE_TTL` deleted; request-scoped auth via local header mutation -- v7.12 (AUTHC-01)
- ✓ NO `auth.set_session()` mid-flight (Codex constraint at `gotrue_client.py:713` respected); static AST scanner enforces -- v7.12 (AUTHC-02)
- ✓ Refresh-only locking keyed by `_session_uuid` (NOT access tokens; stable across rotation); D-17 behavioral test proves `max_concurrent == 2` for distinct UUIDs -- v7.12 (AUTHC-03)
- ✓ Auth-resurrection guard removed; AST scanner with 13 seed traps catches `get_client().auth.<mutating>` resurrection ban -- v7.12 (AUTHC-04)
- ✓ Code comment in auth path documents WHY `set_session()` is avoided (Codex finding cited) -- v7.12 (AUTHC-05)
- ✓ Auth state writes migrated to safe_storage helpers; `set_auth` returns `bool` with symmetric 2-key rollback + `profile=None` clears stale -- v7.12 (AUTHW-01, AUTHW-02)
- ✓ `sign_out` calls `throwaway.auth.admin.sign_out(jwt, "global")` for real server-side revocation; local keys popped in `finally` -- v7.12 (AUTHW-03, AUTHW-04)
- ✓ OAuth callback prune-mid-flight resilience tested via `tests/test_auth_callback_resilience.py` (7 tests) -- v7.12 (AUTHW-05)
- ✓ `persist_value` safe-wrap in `filter_panel.py` retained; 6-test retention guard installed -- v7.12 (AUTHW-06)
- ✓ 5-surface SWEEP-01 audit clean (`app.storage.user` + `app.storage.browser` + `app.storage.client` + `joins.db` + `web/analytics.py`) -- v7.12 (SWEEP-01)
- ✓ 12 reader sites in `web/supabase_client.py` migrated from anonymous `get_client()` to authenticated `get_user_client()`; AST scanner CI guard -- v7.12 (READER-01..06)
- ✓ `/lists` warm-render: 36s → 2s (19.3x mean speedup) via task-scoped `WeakKeyDictionary` memo + zero-arg `get_list_item_counts_for_user()` RPC -- v7.12 (Phase 92.2)
- ✓ `docs/guides/MULTITENANT.md` shipped as architecture reference (~2150 words, 8 sections) -- v7.12 (SWEEP-06)
- ✓ Web search-result folio chip parity with desktop COL_IMG (`display['img']` chip after shelfmark) -- v7.12 (FOLIO-01)
- ✓ Right-side line-number gutter on 5 surfaces (web Browse + Quick View + Full Manuscript View; desktop Browse + ResultDialog) with copy-paste invariant -- v7.12 (LINE-NUM-01..10)

- ✓ Post-search 3-state PGP filter button on `/search` results toolbar (`Filter PGP` / `Has PGP` / `No PGP`) persisted via `web/safe_storage.py` chokepoint; cascade discipline pinned by `tests/test_pgp_filter_cascade.py` static AST guard -- v7.13 (PGP-FILTER-01, PGP-FILTER-02, PGP-FILTER-04, PGP-FILTER-05; PGP-FILTER-03 chip Superseded by user smoke direction)
- ✓ Main xlsx sheet appends per-row `Has PGP` / `Is Printed` / `Domains` columns (Yes/empty booleans + pipe-delimited domains, multi-folio rows repeat per-row) on both web and desktop; Domains deduped per sys_id -- v7.13 (EXPORT-META-01)
- ✓ NEW `Manuscripts` xlsx sub-sheet — one row per unique `sys_id` (first-occurrence dedupe) with PGP URL + Description + Type + Date + Languages + Tags + NLI Catalog Entry + Catalog Summary + Library Viewer URL + GenizahSearch URL; URL cells clickable hyperlinks with blue-underline styling on both apps via `shared/export_dossier.py:build_manuscript_row` -- v7.13 (EXPORT-META-02)
- ✓ NEW `Bibliography` xlsx sub-sheet — one row per FJMS bib entry, joinable to `Manuscripts` by System ID; 8 columns with real FJMS field names (running_title / title_year / mention_page / article_name / article_author_eng / catalog_acronym) via `shared/export_dossier.py:build_bibliography_rows` -- v7.13 (EXPORT-META-03)
- ✓ 4-sheet xlsx workbook order on both apps (`Search Results` → `Manuscripts` → `Bibliography` → `Credits and Info`; first sheet default-active); 4th `Credits and Info` sheet carries search metadata (Query / Mode / Gap / generated_at / result count) + GenizahSearch.com hyperlink + Creator credit -- v7.13 (EXPORT-META-04)
- ✓ Bilingual xlsx exports: lang='he' produces Hebrew sheet titles + headers + Hebrew-preferred metadata (with English fallback per field); lang='en' produces English everywhere (with Hebrew fallback). D-02 transcription-text prohibition UNCHANGED; D-10 parallels-envelope strip UNCHANGED; conditional RTL view-direction UNCHANGED. 4 new bilingual helpers in `shared/export_dossier.py` (`main_header_row` / `manuscript_header_row` / `bibliography_header_row` / `sheet_titles`) -- v7.13 (EXPORT-META-05)
- ✓ `printed_ids` plumbed through `web/export_state.set_search_export(...)` alongside `transcription_sys_ids` + `result_domains`; new sibling helper `update_search_export_enrichment(...)` with independent-field patch semantics; 5 call sites in `web/pages/search.py` -- v7.13 (EXPORT-META-06)
- ✓ Web JSON per-item gains additive `has_pgp` (bool) / `is_printed` (bool) / `domains` (list); envelope `schema_version` stays 1; opt-in semantics preserve `/api/search` public response shape (D-11). Parallels JSON envelope unchanged (D-10 negative invariant pinned by `tests/test_parallels_envelope_no_pgp_keys.py`) -- v7.13 (EXPORT-META-07)
- ✓ `IIIF Manifest` column DEFERRED per D-13 soft scope: header present on main sheet but cells empty on both apps; Library Viewer URL on Manuscripts sub-sheet provides sys_id-scoped reachability instead -- v7.13 (EXPORT-META-08)
- ✓ Desktop xlsx search-results export rewired to emit the same 4-sheet bilingual workbook as web via `shared/export_dossier.py`; new module-level pure-function `_build_search_results_xlsx_bytes(...)` at `genizah_app.py:2473` (Qt-free, offline-testable); cross-parity invariant pinned by `tests/test_export_xlsx_cross_parity.py`; CSV / TXT / DOCX branches at `genizah_app.py:18294+` unchanged -- v7.13 (EXPORT-META-09)

- ✓ My Library — desktop local document search: 7th tab indexes user folders of `.docx`/`.pdf`/`.txt`/`.html`/`.xlsx`/`.csv` into a separate Tantivy side-index merged into Search / Composition / Parallels via RRF k=60 POST-dedup, with a `LOCAL` badge, corpus selector (`Genizah`/`Local`/`ALL`), three-state LOCAL filter, per-file opt-out tree, and three cloud-write gates that keep personal corpora entirely off the cloud (web/API/Supabase). Scaled to 13K files / 43 GB with atomic Tantivy rebuild + zstd text cache + crash recovery + Reset My Library. -- v7.14 (Phases 95-97.3)
- ✓ NLI Resilience — shared circuit breaker (`shared/nli_circuit_breaker.py`) wired into all 10 NLI/IIIF fetch sites with bounded env-configurable timeouts; worst-case per-request blocking dropped 45s → ~9s; PostHog breaker telemetry via factored `shared/posthog_server.py`. Closes the 2026-05-25 production hang. -- v7.14 (Phase 98)

- ✓ PDF page image rendering for LOCAL results (desktop): shared on-demand PyMuPDF renderer (QImage from filepath + 1-based page) off the UI thread via a worker with a bounded LRU of open `fitz.Document` handles and no on-disk image cache; ResultDialog + Browse show the rendered page next to extracted text and stay in sync on result/page navigation; non-PDF LOCAL files stay text-only (extension-gated); render failures degrade to a placeholder + log with no UI hang -- v7.15 (PDFIMG-01..06, Phases 99-100)

- ✓ Hebrew PDF text-layer extraction rewrite for LOCAL search (desktop): `extract_pdf_pages` rebuilt on a `rawdict` per-glyph foundation with RTL-gated reorder, Unicode-`Mn` nikud/maqaf classification, per-line 1-D Otsu word-gap de-space, and `_ltr_damage_guard` RTL-trust fix — emphasis letter-spacing no longer shatters Hebrew words and tight typesetting no longer fuses phrases (אוצר הגאונים single-letter tokens 73.5%→~3-5%; רביצקי merge 15.8%→0.07%); corrupt text layers detected + flagged; `extraction_format_version` 2→3 -- v7.16 (Phase 102 + D-F13b/c/d)
- ✓ File-management actions for LOCAL hits (desktop): Open file location + file-aware right-click menu (open / reveal / copy path / copy filename) replacing Genizah cloud-community actions, per-folder opt-out checkboxes, `.html`/`.xlsx`/`.csv` open support (`desktop/file_actions.py`) -- v7.16 (D-F24)
- ✓ LOCAL search/startup performance + format fixes (desktop): search history no longer stores result snapshots (re-runs on click), eliminating the 778 MB `search_history.json` ~20-30s per-search freeze; large-folder startup O(n²) opt-out-checkbox refresh fixed (14.96s→0.10s); LAB rebuild runs on a background worker; HTML `&nbsp` / `.xlsx` formula-only / UTF-16 `.csv` extraction fixes; folder opt-out cascade -- v7.16 (D-F23, D-F19..D-F22)

- ✓ LOCAL ("My Library") export support (desktop): Search-results export across XLSX/CSV/TXT/DOCX emits local-meaningful columns (filename/folder/filepath/page/matched-text) for LOCAL hits, with a dedicated bilingual "Local Documents" xlsx sheet, LOCAL-excluded Genizah sub-sheets, single-table CSV/TXT/DOCX fallbacks, and a preserved Genizah cross-parity invariant (DOCX redesigned by design). Plus export UX polish (Open File/Folder dialog, LOCAL-only domain-warning + MiDRASH-credit suppression, capped full-text context in DOCX/TXT). Closes D-F17. -- v8.0.0 / folded v7.17 (Phases 103 + 105; LEXP-01/03–08, EXPUX-01–04)
- ✓ Desktop rebrand → "Dicta Genizah Search Pro" (display name only — window title, About EN+HE, updater, exported-file credits, puzzle PNG footer, version metadata, installer, README/CHANGELOG, web download-page title; binary identifiers UNCHANGED so installs upgrade in place; web brand "Dicta Genizah Search" unchanged) + i18n gap closure (223 desktop+web gaps / 246 keys) -- v8.0.0 / folded v7.17 (BRAND-01/02; commit `6e0c312d` + follow-ups)

### Active

**Milestone v8.0.0 — Dicta Rebrand & Joins Lab (in progress).** The rebrand (display-only) and LOCAL export (Phases 103 + 105) are delivered and fold into the v8.0.0 release. The new build is **Joins Lab** — Component A (Join Workbench hub, primary) + Component B (search-support algorithms, secondary/independent), both apps, human-in-the-loop, NO auto-finder. Full requirements (JWB-01..09, JSA-01..03) in `.planning/REQUIREMENTS.md`. **Roadmap deferred pending a Genizah-scholar design-critique session.** Version decision RESOLVED → v8.0.0 (the actual version-file bump happens at `/release` time, not now).

Carried-forward candidates (NOT in v8.0.0 scope unless promoted):
- **D-F12** — regular Search ~constant 8s wall-clock investigation (profile-first: instrument Tantivy candidate fetch → regex post-filter → enrichment → highlight build → return-to-UI; profile LOCAL-only / Genizah-unfiltered / Genizah-filtered; optimize the actual bottleneck — do NOT guess).
- **D-F18** — context-menu LOCAL detection could normalize through `display` (P3, opportunistic when next editing `_show_results_context_menu`).
- **EXP-F3** (was LEXP-02) — Composition-report LOCAL export, gated on a LOCAL composition-search UI.

### Out of Scope

- PGP people/places integration -- complexity too high, defer
- Map-based geographic browse -- requires places.csv + UI work, defer
- Automatic PGP sync from GitHub -- manual refresh sufficient
- Build transcription editor -- link to external tools instead
- Build join detection AI -- import from NLI/PGP instead
- NLI PartOf relationships UI (424K records) -- service method exists, UI deferred
- NLI See cross-references UI (19K records) -- service method exists, UI deferred
- NLI BifolioWith pairs UI (23K records) -- service method exists, UI deferred
- FJMS full texts as version selector sources -- deferred (catalog descriptions only)
- Migrating libraries.csv to SQLite -- high refactoring risk, no user-visible benefit yet
- Server-Side Image Cache (prev v7.8) -- deferred to v7.9+, blocked on NLI TOS outreach (INV-04)
- FGP direct image access -- FGPImageNumberId ≠ IIIF FL ID, different numbering systems
- Search tabs / multi-search workspace (יג) -- deferred, too architectural
- Transcription search (FJMS import + unified index + distribution) -- deferred to future milestone

## Context

### Search Engine (Two-Phase Architecture)

1. **Phase 1 (Tantivy)**: Fast full-text index -> retrieves candidate documents via OR groups
2. **Phase 2 (Regex)**: Precise pattern matching -> filters, highlights results

Responsa adds a **parsing layer** before both phases -- `parse_responsa_query()` translates syntax into structured components, which feed into `build_tantivy_query()` (OR groups with boosting) and `build_regex_pattern()` (wildcards, alternations).

### Architectural Principle

**Both apps must be maintained.** All search logic lives in `genizah_core.py` (shared). UI is app-specific.

## Constraints

- **Dual App Maintenance**: All features must work in both web and desktop
- **Shared Core**: All search logic in genizah_core.py -- UI-only code in app-specific files
- **Backward Compatibility**: All existing search modes unchanged when Responsa mode OFF
- **Combinatorial Cap**: MAX_EXPANDED_TERMS = 500 with 6-step downgrade cascade
- **PGP Tags Interaction**: Responsa sub-options hidden when PGP Tags mode active
- **Legacy Supabase**: PGP tables kept in Supabase (legacy desktop users depend on them)

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| Supabase direct (no FastAPI) | Simplicity, reduced infrastructure | Good |
| Tantivy local index | Fast search, no server dependency | Good |
| Two-phase search (Tantivy + Regex) | Best of both: speed + precision | Good |
| Shared service layer (document_service.py) | Both apps consume PGP data | Good |
| Option II (Hybrid) for Responsa core | Tantivy OR groups + Regex patterns, best balance | Good |
| SQLite sidecar pattern (3 sidecars) | Read-only reference data, both apps, offline capable | Good |
| pgp.db as separate sidecar (not extending existing) | Distinct domain boundary, different update cycle | Good |
| Tags as TEXT JSON with json_each() | Simple start, 115ms acceptable for 2695 tags | Good |
| joins.db SQLite sidecar for puzzle docs | Local-first, offline capable, consistent with pgp.db/fjms pattern | Good |
| HSV background removal (not AI/ML) | Deterministic, fast, no model dependency, handles solid-color backgrounds well | Good |
| Fabric.js for web canvas | Rich 2D manipulation, active community, MIT license | Good |
| Client-parameter injection for publish service | Same service code for web (anon client) and desktop (auth client) | Good |
| Supabase puzzle-images storage bucket | Public read, user-scoped write, thumbnail + full-res PNG | Good |
| Post-search domain filtering (not pre-search) | Users see all results first, then narrow by domain | Good |
| Separate nli_crossref.db sidecar | Different provenance and update cycles from FJMS | Good |
| FGP ≠ FL (crossref FGPImageNumberId not usable for IIIF) | Friedberg photo numbers are different numbering system | Lesson Learned |
| Phase 13 deferred | Transcription index build too slow for desktop | Revisit in v7.0.0 |
| v6.5.0 UX-first ordering | Power user feedback: search UX pain > catalog features | Good — addressed 15/17 user requests |
| Bidirectional filtered search | Pre-filter from search + "search within" from browse — same restrict_sys_ids mechanism | Good |
| Dicta Translation for all data | Multilingual access + search completeness, scholarly few-shot prompts | Good — 580K translations, 0 failures |
| Translation QA with heuristic checks | Catch hallucinations, script mismatches, length anomalies before display | Good — found and fixed 12,827 issues |
| Transcription deferred to v7.0.0 | v6.5.0 focuses on UX + filtering; transcription is separate milestone | ⚠️ Revisit — v7.0.0 now Fragment Puzzle; transcription deferred further |
| Fragment Puzzle as v7.0.0 | Visual join assembly tool is a unique research capability; transcription search deferred | — Pending |
| Manuscript-level search restriction (not page-level) | Broader scholarly relevance -- manuscripts where both terms appear anywhere | Good |
| COALESCE(catalog, computed) for dimension filtering | Maximizes coverage across data sources | Good |
| visual_similarity.db as separate sidecar (server-only default) | 500-700MB too large for desktop bundle; on-demand download option | Good |
| Browse Phase A/B split (zero SQLite hot path) | First paint renders instantly; enrichment loads async | Good |
| ExclusionSource model with per-source tracking | Users can see and clear individual exclusion sources | Good |
| IE volume data from MARC 907 field order | 907 field position maps to IIIF suffix; validated via stratified IIIF sampling | Good |
| Per-IE browse_map grouping (not cross-IE dedup) | Each IE's pages independently addressable; 98.5% single-IE manuscripts unchanged | Good |
| Two-file dependency pinning (requirements.txt + requirements-lock.txt) | Direct deps editable, full transitive closure reproducible in CI, cross-platform caveat documented | Good |
| Scoped ruff ruleset (E9/F401/F811/F821 only) | Catch real bugs without side-questing over a legacy codebase; expandable over time | Good |
| CI matrix on both Ubuntu and Windows | Windows is dev + deploy platform; ensures CI catches platform-specific regressions | Good |
| Per-patch version guards using packaging.version.Version() | Each patch can be retired independently as NiceGUI fixes them upstream; string comparison would break at 3.10 vs 3.8 | Good |
| Inline justification comments for silent handlers (not converting to logging) | Preserves intentional suppression behavior; grep-visible; zero behavioral change | Good |
| Root-anchored .gitignore patterns with explicit exemption block | Prevents accidentally hiding subdirectory files; intentional assets documented at the source of truth | Good |
| PKCE-only OAuth callback (implicit flow removed) | Removes unused dead code path; aligns with Supabase default; confirmed via production testing | Good |
| v7.12 Path B: foundations first (session UUID + safe_storage chokepoint) | Subsequent phases need stable cache key + zero-raw-storage invariant before auth/lists can be rewritten safely | ✓ Good — 131 sites migrated cleanly under the chokepoint |
| v7.12 Path B: state separation by deletion, not migration | Dual-write through singleton mirrors invites regression; `web/export_state.py` becomes the only path | ✓ Good — 10 AppState fields physically gone; cross-user leak structurally impossible |
| v7.12 Path B: lists cache goes per-request (drop the 10s TTL) | Cache was a perf optimization not load-bearing for normal use; not worth preserving during multitenant safety refactor | ✓ Good — Phase 92.2 perf fix made memoization request-scoped instead |
| v7.12 Path B: NO `auth.set_session()` per request | Codex verified gotrue_client.py:713 — `set_session()` is networked (calls `get_user` or `_refresh_access_token`); request-scoped auth must avoid it | ✓ Good — request-scoped auth via local header mutation works correctly |
| v7.12 Path B: refresh-only locking keyed by `_session_uuid` | Token-keyed locks rotate when tokens rotate; UUID-keyed locks are stable across refresh; no cached authenticated client objects | ✓ Good — D-17 behavioral test proves `max_concurrent == 2` for distinct UUIDs |
| v7.12 Path B: `sign_out` calls user's authenticated client (not anonymous singleton) | Anonymous singleton can't revoke the user's token server-side; revocation must happen with the user's credentials before popping auth_session | ✓ Good — AUTHW-03/04 pulled forward from Phase 91 to Phase 90 per Codex P1 |
| v7.12 Path B: `_TEST_BACKEND` shim removed | Tests should use real session storage with proper fixtures or adapter injection, not a parallel-universe storage backend | ✓ Good — `SimpleNamespace` + `monkeypatch.setattr('web.safe_storage.app', ...)` is the canonical pattern |
| v7.12 Path B: task-scoped `WeakKeyDictionary` memo for `get_user_client()` | Per-user Client cache (E-path) rejected — Codex flagged "reopens Phase 90's scary surface." Memo keyed by `asyncio.current_task()` cannot survive across requests by construction | ✓ Good — 19.3x mean `/lists` speedup; Phase 90 D-12 invariant preserved |
| v7.12 Path B: cross-AI plan review BEFORE execution | Gemini + Codex caught items internal plan-checker missed (e.g. Phase 92 5-surface audit widening; Phase 91 stale auth_profile security leak) | ✓ Good — pattern applied to every v7.12 phase plan |
| v7.16: rawdict per-glyph PDF extraction (not `get_text` line strings) | Only foundation that lets word boundaries be re-derived from actual glyph spacing | ✓ Good — fixed letter-spacing shatter + phrase fusion |
| v7.16: RTL-gated reorder (Meiri core; LTR untouched) | Meiri reorder helps Hebrew order/headers but HURTS Latin (Spike 001) | ✓ Good — no LTR regression |
| v7.16: per-line 1-D Otsu word-gap valley (not a global fraction) | Intra/inter-word gaps overlap across book classes but are bimodal per line | ✓ Good — replaced first-cut fixed-floor/median that shattered/merged |
| v7.16: Unicode-`Mn` combining-mark test (not `0x05B0–0x05C7` range) | Range mis-treated maqaf/sof-pasuq as vowels and missed te'amim | ✓ Good — maqaf `־` preserved |
| v7.16: search history stores no result snapshots (re-run on click) | Storing `results[:5000]`/entry grew `search_history.json` to 778 MB → ~20-30s UI-thread freeze every search | ✓ Good — migrated 778 MB → 0.08 MB |
| v7.16: diagnose perf freezes by measuring on real data + parallel Claude/Codex | Headless PyQt probes ruled out wrong hypotheses; Codex flagged the unprofiled post-checkpoint history write | ✓ Good — converged on the real root cause |
| v7.17: LOCAL rows export as local columns (not excluded); mixed/ALL xlsx gets a dedicated "Local Documents" sheet | Genizah columns (shelfmark/IIIF/PGP/bibliography/domains) are meaningless for local files; a separate sheet keeps both shapes clean and preserves the Genizah cross-parity invariant | ✓ Good (Search-results, Phase 103) — Composition-report half deferred → EXP-F3 |
| v8.0.0: fold the v7.17 cycle (rebrand + LOCAL export) INTO v8.0.0 rather than closing v7.17 separately | The rebrand is the flagship "Pro" bump; bundling the delivered LOCAL export + the new Joins Lab under one v8.0.0 milestone matches the project's own major-version convention (v5/v6/v7 marked milestones, not API breaks) and the user's milestone naming | ✓ Good — no "v7.17" in history; Phases 103/105 kept as delivered (no destructive phase-clear) |
| v8.0.0: Joins Lab is human-in-the-loop (scholar = ranker), NOT the auto-ranked v7/v8 finder | The auto-finder is research-only (no code), slow (~90s/fragment), low-recall (≤47% R@50), and 40% of cases have no parallels; the scholar-driven workbench composes existing primitives and ships as ~M | — Pending (roadmap deferred to scholar-critique) |
| v8.0.0: defer the roadmap until after a Genizah-scholar design-critique session | The user will role-play a scholar to pressure-test JWB/JSA against the real material nature before phases lock; hardening a roadmap first would be likely-throwaway churn | — Pending |

## Evolution

This document evolves at phase transitions and milestone boundaries.

**After each phase transition** (via `/gsd:transition`):
1. Requirements invalidated? → Move to Out of Scope with reason
2. Requirements validated? → Move to Validated with phase reference
3. New requirements emerged? → Add to Active
4. Decisions to log? → Add to Key Decisions
5. "What This Is" still accurate? → Update if drifted

**After each milestone** (via `/gsd:complete-milestone`):
1. Full review of all sections
2. Core Value check — still the right priority?
3. Audit Out of Scope — reasons still valid?
4. Update Context with current state

---
*Last updated: 2026-06-08 — **Phase 109 (visual-similarity merge & soft-retire) complete & verified** (3/3 SC; JWB-12 satisfied; UAT-approved after 3 gap rounds + a 6-finding round-4 polish loop incl. two QThread 0xC0000409 crash fixes + Join Lab session persistence; `_show_vs_dialog` marked removable, D-11). Next: `/gsd-discuss-phase 110` (search-support — parallels seeding, JSA-01). Prior entry: 2026-06-04 — **Phase 107 (desktop Joins Lab shell) complete & verified** (6/6 SC; UAT-approved after 3 rounds; `desktop/join_workbench.py` anchor shell + entry points + actions + pairwise→group join model; feature renamed "Joins Lab"). Next: `/gsd-discuss-phase 108`. Prior entry: 2026-06-03 — **Phase 106 (Joins Lab shared core) complete & verified** (6/6 SC; `shared/joins_lab.py` + 66 tests, web-reusable pure logic; foundational for JWB-10/11/12; UI in Phases 107-109). Next: `/gsd-discuss-phase 107`. Prior entry: Milestone **v8.0.0 Dicta Rebrand & Joins Lab** opened (folds the delivered v7.17 cycle — rebrand + LOCAL export, Phases 103/105 — into v8.0.0; version decision RESOLVED → v8.0.0). New build = **Joins Lab**: Component A (Join Workbench hub) + Component B (search-support algorithms), both apps, human-in-the-loop, NO auto-finder. Requirements written (JWB-01..09, JSA-01..03 in REQUIREMENTS.md). **Roadmap intentionally deferred** pending a Genizah-scholar design-critique session (user-led, fresh session) that will pressure-test the design against the real material nature before phases lock. Origin: Spike 002 SPIKE-FINDINGS.md + docs/FEATURE_IDEAS.md.*

*Prior: 2026-06-01 — v7.16 Hebrew PDF Text Quality milestone CLOSED (1 formal phase 102 + no-phase de-space/UAT/freeze work; shipped v7.16.0 desktop, tag `v7.16.0` @ `ccb87c90`, GitHub Release with installer marked latest, CI green). LOCAL Hebrew PDF text-layer extraction rewritten (rawdict per-glyph, RTL-gated, Otsu de-space, Mn nikud), file-management actions for LOCAL hits, and three search/startup freeze fixes (778 MB history file, large-folder O(n²) startup, LAB-rebuild churn).*

*Prior: 2026-05-28 — v7.15 My Library Visual CLOSED (3 phases 99-101, 7 plans, 6/6 PDFIMG-*). PDF page image rendering in ResultDialog + Browse + pre-release polish (RTL fix, LAB/remove-folder Windows fixes, "Re-index All" button).*

*Prior: 2026-05-27 — v7.13 + v7.14 milestones CLOSED via retroactive `/gsd-complete-milestone` reconciliation (both shipped as app releases earlier — v7.13.0 2026-05-21, v7.14.0 2026-05-24 — but the GSD close ritual had been skipped). v7.13 requirements archived to `.planning/milestones/v7.13-REQUIREMENTS.md`; v7.14 recorded in `.planning/milestones/v7.14-ROADMAP.md`; MILESTONES.md gained both entries; the live `REQUIREMENTS.md` was deleted (fresh for the next milestone). For authoritative current behavior see CHANGELOG.md / CLAUDE.md "Recently Changed".*
