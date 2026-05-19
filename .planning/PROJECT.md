# GenizahSearch

## What This Is

A research platform for the Cairo Genizah that combines manuscript image browsing with scholarly data from Princeton Geniza Project (PGP) and Fragment of the Jewish Manuscript Studies (FJMS). Users can view human-curated transcriptions from multiple scholars, browse rich document metadata with domain classifications and catalog enrichment, navigate fragment relationships including scientific joins, search across ~217,000 manuscript records with domain-based filtering, and perform advanced Responsa-Project style searches with grammatical expansion, Judeo-Arabic support, and a visual query builder. All scholarly reference data served from local SQLite sidecars for offline-capable, sub-millisecond browsing. Available as both a NiceGUI web app and a PyQt6 desktop app.

## Core Value

**Researchers can find what they need in the Genizah corpus.** The platform brings together manuscript images, scholarly transcriptions, PGP metadata, FJMS domain classifications, scientific joins, catalog records, and powerful search tools -- from simple keyword search to Responsa-Project style syntax with grammatical prefix expansion, Judeo-Arabic forms, and flexible spacing.

## Current Milestone: v7.13 Research-Grade Downloads & PGP Filter

**Goal:** Surface PGP coverage at the result-set level on `/search` and upgrade downloaded xlsx artifacts into citation-grade dossiers so a downloaded file stands alone as a scholarly source.

**Target features:**
- Post-search 3-state PGP filter on `/search` toolbar (`All` / `Has PGP` / `No PGP`) with active-filter chip, persisted via `web/safe_storage.py` chokepoint (web-only — promoted backlog 999.2).
- Multi-sheet xlsx downloads: main sheet gains `Has PGP` / `Is Printed` / `Domains` / `IIIF Manifest` (soft) columns; NEW `Manuscripts` sub-sheet (one row per unique `sys_id` with PGP + NLI + catalog + library viewer + GenizahSearch URL); NEW `Bibliography` sub-sheet (one row per FJMS bib entry, joinable by `System ID`); JSON gains 3 additive per-item flags (`has_pgp`, `is_printed`, `domains`). English-only, no transcription text. Web-only (promoted backlog 999.3).

**Scope guardrails:**
- Web only — desktop is explicitly out of scope per both phase CONTEXT files.
- All per-user persistence MUST go through `web/safe_storage.py` (Phase 87 multitenant invariant).
- `printed_ids` must plumb through `web/export_state.set_search_export(...)` so the export pipeline can read it alongside `transcription_sys_ids`.
- JSON envelope schema_version stays 1 (additive changes only per Phase 83 stability commitment).

## Current State (after v7.12 shipped)

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

### Active

**v7.13 Research-Grade Downloads & PGP Filter** (started 2026-05-19; phases 93 + 94 promoted from backlog 999.2 + 999.3):

PGP Filter (web `/search`):
- [ ] **PGP-FILTER-01**: Post-search 3-state filter button (`all` → `only_pgp` → `hide_pgp` → `all`) in results toolbar, immediately after `printed_filter_btn` — see `web/pages/search.py:1430-1434`
- [ ] **PGP-FILTER-02**: Button hidden until current result set contains at least one PGP-tagged hit (`_set_btn_visible(pgp_filter_btn, bool(search_state.transcription_sys_ids))`)
- [ ] **PGP-FILTER-03**: Active-filter chip in results header row when state is `only_pgp` or `hide_pgp` (co-located with `exclusion_chips_row` at `search.py:1448-1449`)
- [ ] **PGP-FILTER-04**: Filter cascade slots in after `printed_filter`; stacks with manuscript exclusions, domain exclusions, refinement chain
- [ ] **PGP-FILTER-05**: Choice persisted via `persist_value('search_pgp_filter', ...)` routed through `web/safe_storage.py` chokepoint; bootstrap read at search-page init

Export Metadata (xlsx + JSON):
- [ ] **EXPORT-META-01**: Main xlsx sheet appends `Has PGP`, `Is Printed`, `Domains` columns after `Full Text` (pipe-delimited lists, empty cell for missing, "Yes"/empty for booleans)
- [ ] **EXPORT-META-02**: NEW `Manuscripts` sub-sheet: one row per unique `sys_id` with PGP fields, NLI description, FJMS catalog summary (planner picks 3-5 fields with rationale), library viewer URL, GenizahSearch deep link
- [ ] **EXPORT-META-03**: NEW `Bibliography` sub-sheet: one row per FJMS bib entry, joinable to `Manuscripts` via `System ID`, no row cap
- [ ] **EXPORT-META-04**: Sheet order `Genizah Results` → `Manuscripts` → `Bibliography`; first sheet remains default-active on open
- [ ] **EXPORT-META-05**: English-only metadata; no transcription text; no Hebrew translation lookups; graceful Hebrew fallback only when English variant absent
- [ ] **EXPORT-META-06**: `printed_ids` plumbed through `web/export_state.set_search_export(...)` alongside existing `transcription_sys_ids`
- [ ] **EXPORT-META-07**: JSON per-item gains additive `has_pgp` (bool), `is_printed` (bool), `domains` (list); envelope schema_version stays 1; no `pgp` subobject, no dossier in JSON
- [ ] **EXPORT-META-08**: `IIIF Manifest` per-page column on main sheet — soft scope; planner may defer to Manuscripts sub-sheet with documented rationale

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
*Last updated: 2026-05-19 — v7.13 Research-Grade Downloads & PGP Filter milestone STARTED. 13 active requirements across 2 phases (93 PGP filter + 94 export metadata), both promoted from backlog (999.2 + 999.3) with `CONTEXT.md` already in place from prior `/gsd-discuss-phase` runs. Web-only milestone; multitenant invariants from v7.12 carry forward (zero raw `app.storage.user` access under `web/`, enforced by `tests/test_no_raw_storage_access.py`).*
