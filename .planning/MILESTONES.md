# Project Milestones: GenizahSearch

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

