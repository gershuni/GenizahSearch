# Project Milestones: GenizahSearch

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
