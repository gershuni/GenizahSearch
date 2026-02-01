# GenizahSearch File Index

> Last updated: 2026-02-01

This document provides a complete index of all files in the GenizahSearch project, organized by category and purpose.

---

## Quick Reference

| Category | Location | Description |
|----------|----------|-------------|
| Core Logic | Root | Search engine and data models |
| Desktop App | Root | PyQt6 desktop application |
| Web App | `web/` | NiceGUI web application |
| Scripts | `scripts/` | Utility and maintenance scripts |
| Documentation | `docs/` | Project documentation |
| Tests | `tests/` | Automated tests |
| Data Processing | `corpus_mapper/` | Corpus parsing and processing |

---

## Root Directory

### Core Application Files

| File | Description | Importance |
|------|-------------|------------|
| `genizah_core.py` | **Core search engine** - Tantivy indexing, search logic, data models, metadata management | Critical |
| `genizah_translations.py` | UI translations (Hebrew/English) for both web and desktop | Critical |
| `genizah_app.py` | **Desktop application** - PyQt6 GUI with all desktop features | Critical |
| `version.py` | Version number definition | Required |

### Desktop Application Support

| File | Description |
|------|-------------|
| `corrections_client.py` | Local corrections storage client |
| `corrections_ui.py` | Corrections dialog UI components |
| `supabase_corrections_client.py` | Supabase client for corrections sync |
| `lists_sync.py` | Cloud synchronization for user lists |
| `gui_threads.py` | Background worker threads for desktop app |
| `column_filter_dialog.py` | Column filter dialog for results table |
| `filter_text_dialog.py` | Text filter dialog for search results |
| `list_filter_dialog.py` | List filter dialog |
| `sefaria_utils.py` | Sefaria API integration utilities |
| `unified_variants.py` | Character variant mappings for search |

### Build & Deployment

| File | Description |
|------|-------------|
| `deploy.sh` | Production deployment script (runs on server) |
| `build_app.bat` | Windows batch script to build desktop executable |
| `build_index.py` | Tantivy search index builder |
| `GenizahSearchPro.spec` | PyInstaller spec file for desktop build |
| `start_servers.py` | Development server launcher |
| `start_servers.bat` | Windows batch for dev servers |
| `start_servers.sh` | Unix shell for dev servers |
| `server.py` | Legacy server script |
| `web_pilot.py` | Web testing pilot script |

### Configuration & Environment

| File | Description |
|------|-------------|
| `.env` | Environment variables (not in git) |
| `.env.production.example` | Example production environment |
| `.gitignore` | Git ignore rules |
| `.cursorrules` | Cursor IDE rules |
| `requirements.txt` | Python dependencies |
| `supabase_setup.sql` | Supabase database schema setup |

### Documentation (Root)

| File | Description |
|------|-------------|
| `README.md` | Project overview and quick start |
| `CLAUDE.md` | Instructions for AI assistants |
| `CHANGELOG.md` | Version history and release notes |
| `CONTRIBUTING.md` | Contribution guidelines |
| `LICENSE` | MIT License |
| `ANTIVIRUS_INFO.txt` | Antivirus false positive information |
| `version_info.txt` | Version metadata for builds |

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
| `AllGenizah_OLD.txt` | 1.6 GB | Legacy corpus (v0.7, kept for reference) |
| `libraries.csv` | 45 MB | Library metadata mappings |
| `oxford_full_db.json` | 8.5 MB | Oxford Bodleian metadata |
| `bodleian_master_index.csv` | 88 KB | Bodleian shelfmark index |
| `char_merges_*.xlsx` | ~90 MB | Character merge analysis reports |

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

### Pages (`web/pages/`)

| File | Description |
|------|-------------|
| `home.py` | Landing page |
| `search.py` | **Main search page** - Text and variant search |
| `browse.py` | Manuscript browser |
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

### Components (`web/components/`)

| File | Description |
|------|-------------|
| `add_to_list_dialog.py` | Dialog for adding items to lists |
| `comment_dialog.py` | Comment submission dialog |
| `joins_panel.py` | Fragment joins display panel |
| `notes_display.py` | Notes/annotations display |
| `project_tree.py` | Project/list tree widget |
| `text_editor.py` | Text editing component |
| `typography.py` | Typography utilities (RTL support) |
| `version_selector.py` | Version selection dropdown |

### Static Assets (`web/static/`)

Contains CSS, JavaScript, and image assets for the web application.

---

## Scripts (`scripts/`)

### Database & Admin

| File | Description |
|------|-------------|
| `create_admin.py` | Create admin user account |
| `promote_to_admin.py` | Promote user to admin role |
| `delete_corrections.py` | Delete corrections from database |
| `cleanup_duplicate_lists.py` | Remove duplicate list entries |
| `fix_rls_policies.sql` | Supabase RLS policy fixes |
| `import_base_versions.py` | Import base transcription versions |

### Corpus Processing

| File | Description |
|------|-------------|
| `prepare_corpus.py` | Prepare corpus for indexing |
| `analyze_char_merges.py` | Analyze character merges in corpus |
| `generate_multichar_pairs.py` | Generate multi-character variant pairs |
| `generate_unified_variants.py` | Generate unified variant mappings |
| `multichar_pairs.py` | Multi-character pair definitions |

### Utilities

| File | Description |
|------|-------------|
| `check_docs.py` | Documentation health checker |
| `create_og_image.py` | Generate Open Graph images |
| `rebrand.py` | Rebranding utility |
| `test_nli_fetch.py` | Test NLI API fetching |
| `verify_export.py` | Verify export functionality |
| `verify_newline_removal.py` | Verify newline handling |
| `debug_full_gui.py` | Debug desktop GUI issues |

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

| File | Description |
|------|-------------|
| `conftest.py` | Pytest configuration and fixtures |
| `test_api_flow.py` | API workflow tests |
| `test_corrections_api.py` | Corrections API tests |
| `test_corrections_integration.py` | Corrections integration tests |
| `test_excel_logic.py` | Excel export logic tests |
| `test_export_service.py` | Export service tests |
| `test_missing_tantivy.py` | Missing index handling tests |
| `test_shelfmark_normalization.py` | Shelfmark parsing tests |

---

## Documentation (`docs/`)

See [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) for full documentation structure.

### Guides (`docs/guides/`)

| File | Description |
|------|-------------|
| `DEPLOYMENT_TECHNICAL.md` | Technical deployment guide |
| `WEBSITE_ADMIN_GUIDE.md` | Non-technical admin guide |
| `DEVELOPER_GUIDE.md` | Developer setup guide |
| `SUPABASE_GUIDE.md` | Supabase database guide |

### Plans (`docs/plans/`)

Implementation plans for features. See `plans/PLANS_INDEX.md`.

### Specs (`docs/specs/`)

Technical specifications for complex features.

### Archive (`docs/archive/`)

Historical documents kept for reference.

---

## Data Directories

| Directory | Description |
|-----------|-------------|
| `data/` | Local databases and logs |
| `data/corrections.db` | Local corrections SQLite database |
| `data/genizah_users.db` | Local user data SQLite database |
| `data/logs/` | Application logs |
| `corpus_mapper_output/` | Output from corpus processing |
| `dist/` | Distribution builds and installers |
| `verification/` | Verification scripts and screenshots |
| `migrations/` | Database migration scripts |

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
- **kebab-case/** - Some directory names
- Hebrew filenames are acceptable for user-facing content

---

## See Also

- [CODE_INDEX.md](CODE_INDEX.md) - Detailed class/method index
- [DOCUMENTATION_INDEX.md](DOCUMENTATION_INDEX.md) - Documentation structure
- [CLAUDE.md](../CLAUDE.md) - AI assistant context

---

*Last updated: 2026-02-01*
