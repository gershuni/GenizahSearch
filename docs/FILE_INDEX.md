# GenizahSearch File Index

> Last updated: 2026-03-26

This document provides a complete index of all files in the GenizahSearch project, organized by category and purpose.

---

## Quick Reference

| Category | Location | Description |
|----------|----------|-------------|
| Core Logic | Root | Search engine and data models |
| Desktop App | Root | PyQt6 desktop application |
| Shared Services | `shared/` | Business logic shared by both apps |
| Web App | `web/` | NiceGUI web application |
| Browser Extension | `extension/` | Chrome/Firefox NLI image helper |
| Scripts | `scripts/` | Utility and maintenance scripts |
| Documentation | `docs/` | Project documentation |
| Tests | `tests/` | Automated tests |
| Data Processing | `corpus_mapper/` | Corpus parsing and processing |

---

## Root Directory

### Core Application Files

| File | Description | Importance |
|------|-------------|------------|
| `genizah_core.py` | **Core search engine** - Tantivy indexing, search logic, data models, metadata management (~9.5K lines) | Critical |
| `genizah_translations.py` | UI translations (Hebrew/English) for both web and desktop (~3.1K lines) | Critical |
| `genizah_app.py` | **Desktop application** - PyQt6 GUI with all desktop features (~30.4K lines) | Critical |
| `version.py` | Version number definition (source of truth) | Required |

### Desktop Application Support

| File | Description |
|------|-------------|
| `corrections_client.py` | Local corrections storage client |
| `corrections_ui.py` | Corrections dialog UI components |
| `supabase_corrections_client.py` | Supabase client for corrections, comments, discoveries, joins |
| `lists_sync.py` | Cloud synchronization for user lists |
| `gui_threads.py` | Background worker threads for desktop app |
| `column_filter_dialog.py` | Column filter dialog for results table |
| `filter_text_dialog.py` | Text filter dialog for search results |
| `list_filter_dialog.py` | List filter dialog |
| `sefaria_utils.py` | Sefaria API integration utilities |
| `unified_variants.py` | Character variant mappings for search |
| `pgp_tag_translations.py` | PGP tag translations module |
| `shared_export_utils.py` | Export utilities shared between apps |
| `server.py` | Web server runner |
| `web_pilot.py` | Web pilot/automation utility |

### Build & Deployment

| File | Description |
|------|-------------|
| `deploy.sh` | Production deployment script (runs on server) |
| `build_app.bat` | Windows batch script to build desktop executable |
| `build_index.py` | Tantivy search index builder |
| `GenizahSearchPro.spec` | PyInstaller spec file for desktop build |
| `CompileScriptGenizah.iss` | Inno Setup installer script |
| `version_info.txt` | Windows EXE version metadata |

### Configuration & Environment

| File | Description |
|------|-------------|
| `.env` | Environment variables (not in git) |
| `.env.production.example` | Example production environment |
| `.gitignore` | Git ignore rules |
| `requirements.txt` | Python dependencies |
| `supabase_setup.sql` | Supabase database schema setup |

### Documentation (Root)

| File | Description |
|------|-------------|
| `README.md` | Project overview and quick start |
| `CLAUDE.md` | Instructions for AI assistants |
| `CHANGELOG.md` | Version history and release notes |
| `LICENSE` | MIT License |
| `ANTIVIRUS_INFO.txt` | Antivirus false positive information |

### Assets

| File | Description |
|------|-------------|
| `icon.ico` | Application icon |
| `image.png` | OG image for social sharing |
| `Help.html` | Desktop app help content |

### Data Files

| File | Size | Description |
|------|------|-------------|
| `Transcriptions.txt` | 1.4 GB | **Main corpus** - Cairo Genizah transcriptions |
| `libraries.csv` | ~50 MB | Library metadata mappings (~255K records) |
| `oxford_full_db.json` | 8.5 MB | Oxford Bodleian metadata |
| `bodleian_master_index.csv` | 88 KB | Bodleian shelfmark index |

### SQLite Sidecar Databases

| File | Size | Description |
|------|------|-------------|
| `pgp.db` | 165 MB | PGP reference data (35K documents, transcriptions, footnotes) |
| `fjms_enrichment.db` | 941 MB | FJMS scholarly data (390K domains, 685K catalog, 427K bib) |
| `nli_crossref.db` | -- | NLI images/metadata crossref (815K NLI, 141K Cambridge, 28K Manchester, 36K JTS DPUL) |
| `libraries_translations.db` | 76 MB | Dicta translations for library titles |

---

## Shared Services (`shared/`)

Service layer providing business logic shared by both web and desktop apps. Extracted during v5.6.0-v7.0.0 milestones.

| File | Description |
|------|-------------|
| `__init__.py` | Package init |
| `corrections_service.py` | Corrections business logic (Phase 22) |
| `document_service.py` | PGP document data service (Phase 8) |
| `fjms_service.py` | FJMS catalog integration (Phase 25) |
| `nli_crossref_service.py` | NLI crossref service, 16 methods (Phase 29) |
| `translation_service.py` | Translation lookup and toggle |
| `translation_qc.py` | Translation QC heuristics (10 checks) |
| `dicta_client.py` | Dicta API client for batch translation |
| `reading_desk_model.py` | Virtual reading desk data model |
| `session_persistence.py` | Session state persistence |
| `supabase_provider.py` | Supabase configuration provider |
| `puzzle_model.py` | PuzzleDocument/PuzzleFragment dataclasses (v7.0.0) |
| `puzzle_service.py` | SQLite CRUD for joins.db sidecar (v7.0.0) |
| `puzzle_export.py` | Composite PNG export, thumbnail generation (v7.0.0) |
| `puzzle_image_service.py` | IIIF image fetch + background removal + cache versioning (v7.0.0) |
| `puzzle_publish_service.py` | Community publish/unpublish/fork (v7.0.0) |
| `background_removal.py` | HSV-based background removal engine (v7.0.0) |

---

## Web Application (`web/`)

### Main Files

| File | Description |
|------|-------------|
| `main.py` | **Web app entry point** - NiceGUI application setup |
| `api.py` | API endpoints for web features |
| `state.py` | Application state management |
| `auth_state.py` | Authentication state handling |
| `supabase_client.py` | Supabase database client |
| `services.py` | Business logic services |
| `export_service.py` | Export functionality (Excel, Word, etc.) |
| `translations.py` | Web-specific translations |
| `user_lists.py` | User lists management |
| `analytics.py` | PostHog analytics integration |
| `feature_flags.py` | Feature flag management |
| `puzzle_tokens.py` | HMAC upload token generation/verification (v7.0.0) |
| `pesach.py` | Pesach seasonal easter eggs |

### Web Shim Services

These re-export shared/ modules for web-specific usage:

| File | Re-exports |
|------|------------|
| `corrections_service.py` | `shared.corrections_service` |
| `document_service.py` | `shared.document_service` |
| `fjms_service.py` | `shared.fjms_service` |
| `nli_crossref_service.py` | `shared.nli_crossref_service` |

### Pages (`web/pages/`)

| File | Description |
|------|-------------|
| `home.py` | Landing page |
| `search.py` | **Main search page** - Text, variant, responsa, filtered search (~5.3K lines) |
| `browse.py` | Manuscript browser with PGP enrichment (~4.8K lines) |
| `catalog_browse.py` | Catalog browsing by domain/author/work (v6.1.0) |
| `puzzle.py` | **Fragment puzzle page** - Fabric.js canvas + unified image loader (~3.9K lines, v7.0.0) |
| `document.py` | Document viewer page |
| `viewer.py` | IIIF image viewer |
| `parallels.py` | Parallel text finder |
| `lists.py` | User lists management |
| `discoveries.py` | Community discoveries feed |
| `corrections.py` | Corrections submission/viewing |
| `profile.py` | User profile page |
| `settings.py` | User settings |
| `admin.py` | Admin dashboard |
| `help.py` | Help documentation page |
| `download.py` | Desktop app download page |
| `accessibility.py` | Accessibility statement |
| `about.py` | About page |

### Components (`web/components/`)

| File | Description |
|------|-------------|
| `add_to_list_dialog.py` | Dialog for adding items to lists |
| `bibliography_dialog.py` | Bibliography display dialog (v5.9.0) |
| `catalog_dialog.py` | FJMS catalog enrichment dialog (v6.1.0) |
| `comment_dialog.py` | Comment submission dialog |
| `filter_panel.py` | Shared filter panel component (v7.2.4) |
| `joins_panel.py` | Fragment joins display panel |
| `measurements_dialog.py` | Manuscript measurements dialog (v7.3.0) |
| `notes_display.py` | Notes/annotations display with translate |
| `project_tree.py` | Project/list tree widget |
| `text_editor.py` | Text editing component |
| `translate_button.py` | Translation UI button (v6.5.0) |
| `translation_report.py` | Translation issue reporting (v6.5.0) |
| `typography.py` | Typography utilities (RTL support) |
| `version_selector.py` | Version selection dropdown |

### Static Assets (`web/static/`)

Contains CSS, JavaScript, and image assets for the web application.

---

## Browser Extension (`extension/`)

GenizahSearch Image Helper - Chrome/Firefox extension for NLI image acquisition (v7.0.1).

| File | Description |
|------|-------------|
| `manifest.json` | Chrome MV3 manifest with NLI host permissions |
| `manifest.firefox.json` | Firefox MV3 manifest (gecko settings, background.scripts) |
| `background.js` | Service worker fetching NLI images as binary |
| `content_script.js` | Page-background bridge + extension detection |
| `build.py` | Builds Chrome and Firefox ZIP packages into `extension/dist/` |
| `icons/` | Extension icons (16/48/128px) |

---

## Scripts (`scripts/`)

### Data Import & Export

| File | Description |
|------|-------------|
| `export_fist_enrichment.py` | Export FJMS enrichment to SQLite sidecar |
| `export_pgp_sidecar.py` | Export PGP data to SQLite sidecar |
| `import_pgp_documents.py` | Import PGP documents from CSV |
| `import_pgp_full.py` | Full PGP data export pipeline |
| `import_pgp_sections.py` | Import PGP document sections |
| `import_document_sources.py` | Import PGP document sources |
| `import_nli_crossref.py` | Build NLI crossref sidecar (v5.9.0) |
| `import_jts_dpul.py` | Import JTS DPUL images v1 (v5.9.0) |
| `import_jts_dpul_v2.py` | Import JTS DPUL images v2 — full catalog iteration (v7.2.4) |
| `import_manchester_luna.py` | Import Manchester LUNA images (v5.9.0) |
| `import_measurements.py` | Import FJMS computed measurements (v7.3.0) |
| `import_base_versions.py` | Import base transcription versions |
| `fist_shelfmarks_export.py` | Export FJMS shelfmark data |
| `generate_fist_gap_csv.py` | Generate FIST gap-fill CSV for libraries.csv (v7.1.0) |

### Translation Scripts

| File | Description |
|------|-------------|
| `run_translate.py` | Translation runner (orchestrator) |
| `translate_fjms_catalog.py` | FJMS catalog field translation |
| `translate_fjms_catalog_text.py` | FJMS running titles + full text translation |
| `translate_fjms_free_desc.py` | FJMS free description translation (254K records) |
| `translate_catalog_freedesc_en2he.py` | FJMS free description EN->HE translation |
| `translate_library_titles_en2he.py` | Library title EN->HE translation |
| `translate_libraries_titles.py` | Library title translation (alternate runner) |
| `translate_oxford_metadata.py` | Oxford metadata translation |
| `translate_pgp_descriptions.py` | PGP description EN->HE translation |
| `translate_rt_en2he_local.py` | Local running title translation |
| `translate_gaps_server.py` | Server-side translation gap filler |
| `extract_translation_gaps.py` | Find untranslated records |
| `extract_libraries_english.py` | Extract English library data for translation |
| `merge_translation_results.py` | Merge translation batches |
| `retranslate_flagged.py` | Re-translate flagged translations |
| `export_translation_audit_sample.py` | Translation QA stratified sampling |

### Data Fixes & Attribution

| File | Description |
|------|-------------|
| `fix_handlist_source_names.py` | Fix FJMS handlist source attribution |
| `fix_missing_library_codes.py` | Fill missing library codes |
| `fix_shelfmark_sysid_mismatch.py` | Shelfmark/sys_id reconciliation |
| `attribute_sources_via_api.py` | FJMS API bridge for site user attribution |
| `map_site_user_subids.py` | Map FJMS site user SubIds to names |
| `update_doc_relation.py` | Update document relationships |
| `dedup_bibliography.py` | Deduplicate FJMS bibliography entries (v7.3.0) |
| `extract_library_codes.py` | Extract library codes from data |

### Database & Admin

| File | Description |
|------|-------------|
| `create_admin.py` | Create admin user account |
| `promote_to_admin.py` | Promote user to admin role |
| `delete_corrections.py` | Delete corrections from database |
| `cleanup_duplicate_lists.py` | Remove duplicate list entries |
| `fix_rls_policies.sql` | Supabase RLS policy fixes |
| `checkpoint_sidecars.py` | Backup sidecar databases |

### Corpus Processing

| File | Description |
|------|-------------|
| `prepare_corpus.py` | Prepare corpus for indexing |
| `analyze_char_merges.py` | Analyze character merges in corpus |
| `generate_multichar_pairs.py` | Generate multi-character variant pairs |
| `generate_unified_variants.py` | Generate unified variant mappings |
| `multichar_pairs.py` | Multi-character pair definitions |
| `pgp_transcriptions_export.py` | Export PGP transcriptions |

### Background Removal & Puzzle

| File | Description |
|------|-------------|
| `preview_background_removal.py` | Preview background removal results |
| `test_bg_removal_samples.py` | Test background removal on sample images |
| `test_blue_mat_detection.py` | Test blue mat auto-detection |
| `puzzle_local_helper.py` | Local helper server for puzzle image acquisition |
| `mockup_export_banner.py` | Mockup export banner (v1) |
| `mockup_export_banner_v2.py` | Mockup export banner (v2) |
| `mockup_export_banner_v3.py` | Mockup export banner (v3) |

### Build & Deploy

| File | Description |
|------|-------------|
| `bump_version.py` | Version bumping script (updates all version files) |
| `check_docs.py` | Documentation health checker |
| `rebrand.py` | Project rebranding utility |

### Utilities

| File | Description |
|------|-------------|
| `create_og_image.py` | Generate Open Graph images |
| `test_nli_fetch.py` | Test NLI API fetching |
| `test_shelfmark_search.py` | Test shelfmark search functionality |
| `test_extraction_fix.py` | Test extraction fixes |
| `verify_export.py` | Verify export functionality |
| `verify_newline_removal.py` | Verify newline handling |
| `compare_few_shot.py` | Compare few-shot translation approaches |
| `debug_full_gui.py` | Debug full desktop GUI |
| `scrape_transcription_guids.py` | Scrape transcription GUIDs from FJMS |
| `explore_fjms_transcriptions.py` | Explore FJMS transcription data |

### Join Finder Research

| File | Description |
|------|-------------|
| `join_finder_poc.py` | Join finder proof of concept |
| `join_finder_eval.py` | Join finder evaluation harness |
| `join_finder_sequential.py` | Sequential join finder |
| `join_finder_v2.py` - `join_finder_v8.py` | Join finder iterations (v2-v8) |
| `find_complementary_joins.py` | Find complementary fragment joins |

---

## Corpus Mapper (`corpus_mapper/`)

Tool for parsing and processing Genizah corpus data.

| File | Description |
|------|-------------|
| `main.py` | Main entry point |
| `runner.py` | Corpus processing runner |
| `config.py` | Configuration settings |
| `interactive_config.py` | Interactive configuration wizard |
| `canonical_filter.py` | Canonical text filtering |
| `symbol_discovery.py` | Symbol discovery in texts |
| `text_cleaner.py` | Text cleaning utilities |
| `parsers/` | Parser modules for different formats |

---

## Tests (`tests/`)

### Core Engine Tests

| File | Description |
|------|-------------|
| `test_shelfmark_normalization.py` | Shelfmark parsing tests |
| `test_shelfmark_normalization_unified.py` | Unified shelfmark tests |
| `test_search_normalization.py` | Search normalization tests |
| `test_boundary_search.py` | Boundary search tests |
| `test_missing_tantivy.py` | Missing index handling tests |
| `test_fist_gap_fill.py` | FIST gap fill tests (v7.1.0) |

### Responsa Tests (~221 tests across 6 files)

| File | Description |
|------|-------------|
| `test_responsa_core.py` | Responsa engine tests (~68 tests) |
| `test_responsa_edge_cases.py` | Edge case tests |
| `test_responsa_integration.py` | Integration tests |
| `test_responsa_parity.py` | Web/desktop parity tests |
| `test_responsa_performance.py` | Performance tests |
| `test_responsa_regression.py` | Regression tests |

### Service Layer Tests

| File | Description |
|------|-------------|
| `test_corrections_service.py` | Corrections service tests |
| `test_document_service.py` | Document service tests |
| `test_fjms_service.py` | FJMS service tests |
| `test_fjms_joins_integration.py` | FJMS joins integration tests |
| `test_nli_crossref_service.py` | NLI crossref service tests |
| `test_shared_service.py` | Shared service layer tests |
| `test_translation_qc.py` | Translation QC tests |
| `test_translation_service.py` | Translation service tests |
| `test_measurements.py` | Manuscript measurements tests (v7.3.0) |

### Puzzle Tests (v7.0.0)

| File | Description |
|------|-------------|
| `test_puzzle_model.py` | PuzzleDocument/PuzzleFragment model tests |
| `test_puzzle_service.py` | Puzzle SQLite CRUD tests |
| `test_puzzle_export.py` | Composite PNG export tests |
| `test_puzzle_image_service.py` | IIIF image fetch + bg removal tests |
| `test_puzzle_publish.py` | Community publish/fork tests |
| `test_puzzle_web_api.py` | Web puzzle API tests |
| `test_background_removal.py` | Background removal engine tests |
| `test_mosseri_cudl.py` | Mosseri CUDL image fallback tests |

### UI & Integration Tests

| File | Description |
|------|-------------|
| `conftest.py` | Pytest configuration and fixtures |
| `test_excel_logic.py` | Excel export logic tests |
| `test_export_service.py` | Export service tests |
| `test_desktop_folio_navigation.py` | Desktop folio navigation tests |
| `test_desktop_pending_corrections.py` | Desktop pending corrections tests |
| `test_direct_image_resolution.py` | Direct image resolution tests |
| `test_bibliography_merge.py` | Bibliography merge tests |
| `test_offline_verification.py` | Offline mode tests |
| `test_version_selector_pending.py` | Version selector tests |

---

## Documentation (`docs/`)

See [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) for full documentation structure.

---

## Data Directories

| Directory | Description |
|-----------|-------------|
| `data/` | Local databases and logs |
| `data/corrections.db` | Local corrections SQLite database |
| `data/genizah_users.db` | Local user data SQLite database |
| `data/logs/` | Application logs |
| `joins_data/` | Saved puzzle/join documents (joins.db) |
| `pgp_data/` | PGP data exports and CSVs |
| `fist_data/` | FJMS/FIST data files |
| `corpus_mapper_output/` | Output from corpus processing |
| `dist/` | Distribution builds and installers |
| `reports/` | Translation issue reports CSV |

---

## Generated/Build Directories (Not in Git)

| Directory | Description |
|-----------|-------------|
| `venv/` | Python virtual environment |
| `__pycache__/` | Python bytecode cache |
| `.pytest_cache/` | Pytest cache |
| `.nicegui/` | NiceGUI cache |
| `Genizah_Index/` | Tantivy search indexes (generated) |

---

## File Naming Conventions

- **UPPERCASE.md** - Important documentation files
- **snake_case.py** - Python modules
- Hebrew filenames are acceptable for user-facing content

---

## See Also

- [CODE_INDEX.md](CODE_INDEX.md) - Detailed class/method index
- [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) - Documentation structure
- [CLAUDE.md](../CLAUDE.md) - AI assistant context

---

*Last updated: 2026-03-26*
