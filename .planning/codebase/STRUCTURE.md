# Codebase Structure

**Analysis Date:** 2026-02-05

## Directory Layout

```
C:/GenizahSearch/
├── web/                           # NiceGUI web application
│   ├── main.py                    # Web app entry point (port 8081)
│   ├── state.py                   # Singleton app state container
│   ├── api.py                     # Image proxy, export API endpoints
│   ├── auth_state.py              # Authentication state management
│   ├── supabase_client.py         # Supabase client (auth, CRUD)
│   ├── user_lists.py              # Cloud-aware lists manager
│   ├── export_service.py          # Multi-format export (XLSX/DOCX/JSON)
│   ├── services.py                # Shared services (BrowsePage, DocumentPage)
│   ├── translations.py            # i18n helpers
│   ├── pages/                     # Page components (route handlers)
│   │   ├── home.py                # Dashboard with stats
│   │   ├── search.py              # Advanced search interface
│   │   ├── parallels.py           # Find parallels across documents
│   │   ├── browse.py              # Manuscript viewer (image + transcription)
│   │   ├── lists.py               # User research lists
│   │   ├── corrections.py         # Manuscript corrections interface
│   │   ├── discoveries.py         # Community discoveries
│   │   ├── admin.py               # Admin corrections review
│   │   ├── profile.py             # User profile management
│   │   ├── settings.py            # App settings
│   │   ├── help.py                # Help documentation
│   │   ├── accessibility.py       # Accessibility statement
│   │   ├── download.py            # Desktop app download
│   │   └── document.py            # Deprecated legacy route
│   ├── components/                # Reusable UI components
│   │   ├── typography.py          # Text components (h1-h6)
│   │   ├── text_editor.py         # Transcription text editor
│   │   ├── notes_display.py       # User notes component
│   │   ├── translate_button.py    # Translation toggle
│   │   ├── comment_dialog.py      # Comment entry dialog
│   │   ├── add_to_list_dialog.py  # List management dialog
│   │   ├── joins_panel.py         # Text joins display
│   │   ├── version_selector.py    # HTR version switcher
│   │   ├── project_tree.py        # Project/list tree view
│   │   └── __init__.py
│   ├── static/                    # Static assets
│   │   ├── favicon.ico
│   │   ├── og-image.png           # Social media preview
│   │   └── ... (other assets)
│   └── .nicegui/                  # Generated NiceGUI artifacts
│
├── genizah_core.py                # Core engine (search, metadata, AI) - 290K lines
├── genizah_app.py                 # PyQt6 desktop app - 720K lines
├── genizah_translations.py        # Translation strings - 135K lines
├── gui_threads.py                 # PyQt6 background worker threads
├── version.py                     # Version number (5.1)
│
├── supabase_corrections_client.py # Desktop Supabase integration
├── corrections_client.py          # Legacy corrections handling
├── corrections_ui.py              # Desktop corrections UI - 183K lines
├── lists_sync.py                  # Desktop list sync daemon
├── sefaria_utils.py               # Sefaria API integration helpers
├── server.py                      # FastAPI backend (DEPRECATED - Jan 2026)
├── start_servers.py               # Startup script (old backend)
│
├── build_index.py                 # Build Tantivy search index
├── build_app.bat                  # Windows build script
├── shared_export_utils.py         # Export helper functions
├── unified_variants.py            # Hebrew variant pairs (1M+ lines)
│
├── docs/                          # Documentation
│   ├── CODE_INDEX.md              # File and function reference
│   ├── OPEN_ISSUES.md             # Bug tracker and TODOs
│   ├── DOCUMENTATION_INDEX.md     # Doc structure
│   ├── guides/                    # User/developer guides
│   │   ├── DEPLOYMENT_TECHNICAL.md
│   │   ├── SUPABASE_GUIDE.md
│   │   └── DEVELOPER_GUIDE.md
│   ├── specs/                     # Technical specifications
│   ├── plans/                     # Implementation plans
│   └── archive/                   # Historical documents
│
├── tests/                         # Test suite
│   ├── conftest.py                # Pytest configuration
│   ├── test_api_flow.py           # Integration tests
│   ├── test_corrections_api.py    # Corrections endpoint tests
│   ├── test_boundary_search.py    # Edge case search tests
│   ├── test_export_service.py     # Export format tests
│   ├── test_shelfmark_normalization.py
│   └── ... (10+ test files)
│
├── data/                          # Runtime data
│   └── logs/                      # Application logs
│
├── Genizah_Index/                 # Tantivy search index (generated)
├── migrations/                    # Database migrations (legacy)
├── pgp_data/                      # Princeton Geniza Project transcriptions
│   ├── transcriptions_linked.csv  # 9,364 records with sys_id
│   ├── transcriptions_unmatched.csv
│   ├── fist_shelfmarks_supplement.csv
│   └── MATCHING_SUMMARY.md
│
├── CLAUDE.md                      # AI context document (THIS FILE)
├── CHANGELOG.md                   # Version history
├── README.md                      # Project overview
├── .env                           # Supabase credentials
├── .env.production.example        # Production template
└── .cursorrules                   # Cursor IDE rules
```

## Directory Purposes

**web/**
- Purpose: NiceGUI web application with responsive design
- Contains: 30+ Python modules for pages, components, services, and state management
- Key files: `main.py` (entry), `state.py` (singleton), `supabase_client.py` (backend)

**web/pages/**
- Purpose: Route handlers that create UI for each app page
- Contains: One module per page (search.py, browse.py, lists.py, etc.)
- Pattern: Functions like `create_search_page()` that return NiceGUI UI elements
- Key: All pages use `create_layout()` from main.py for consistent header/sidebar

**web/components/**
- Purpose: Reusable, single-responsibility UI building blocks
- Contains: Dialog components (comment_dialog, add_to_list_dialog), display components (notes, joins)
- Pattern: Functions returning UI element(s) for insertion into pages

**genizah_core.py**
- Purpose: Monolithic core engine - search, indexing, metadata, AI, lists
- Size: 289K lines (largest single file)
- Key classes:
  - SearchEngine: Query parsing, search execution, result ranking
  - MetadataManager: Manuscript data loading, caching, retrieval
  - VariantManager: Hebrew character variant handling
  - LabEngine: Advanced search settings management
  - Indexer: Tantivy index management
  - AIManager: LLM integration (GPT-4, Claude, Gemini)
  - ListsManager: Local list persistence

**genizah_app.py**
- Purpose: PyQt6 desktop GUI
- Size: 720K lines
- Key tabs:
  - Search Tab: Query interface with advanced options
  - Browse Tab: Manuscript viewer with dual-panel layout
  - Lab Tab: Experimental search with variant sliders
  - AI Tab: AI-assisted text analysis
  - Corrections Tab: User contribution interface
  - Community Tab: View corrections, comments, discoveries

**docs/**
- Purpose: Comprehensive project documentation
- Key files:
  - CODE_INDEX.md: Function/class reference (41K)
  - OPEN_ISSUES.md: Bug tracker with status (11K)
  - guides/: Deployment, Supabase schema, developer setup
  - specs/: Technical specifications
  - plans/: Implementation plans for features

**tests/**
- Purpose: Pytest suite for core functionality
- Key tests: Search, exports, corrections API, shelfmark normalization
- Coverage: ~80% of genizah_core, API integrations
- Run: `pytest tests/`

**data/**
- Purpose: Runtime data directory
- Contains: Application logs (data/logs/genizah.log)
- Writable: Desktop and web app write logs here

**Genizah_Index/**
- Purpose: Tantivy full-text search index (Rust)
- Contains: Inverted index of all ~500K manuscript fragments
- Generated by: `python build_index.py`
- Location: Committed to .git (large binary files)
- Note: Must exist for search functionality to work

## Key File Locations

**Entry Points:**
- `C:/GenizahSearch/web/main.py`: Web app (run: `python -m web.main`)
- `C:/GenizahSearch/genizah_app.py`: Desktop app (run: `python genizah_app.py`)

**Configuration:**
- `C:/GenizahSearch/.env`: Supabase credentials (SUPABASE_URL, SUPABASE_ANON_KEY)
- `C:/GenizahSearch/.env.production.example`: Template for env vars
- `C:/GenizahSearch/version.py`: APP_VERSION constant

**Core Logic:**
- `C:/GenizahSearch/genizah_core.py`: Search, metadata, AI engines (entry point for all logic)
- `C:/GenizahSearch/genizah_translations.py`: Bilingual strings (English/Hebrew)
- `C:/GenizahSearch/unified_variants.py`: 500+ Hebrew variant pairs

**Testing:**
- `C:/GenizahSearch/tests/conftest.py`: Pytest setup
- `C:/GenizahSearch/tests/test_*.py`: Individual test modules

**Data Files:**
- `C:/GenizahSearch/libraries.csv`: Master metadata (217K manuscripts) - **NOT in repo**
- `C:/GenizahSearch/Genizah_Index/`: Tantivy index directory
- `C:/GenizahSearch/pgp_data/`: PGP transcriptions supplement (9,364 records)

## Naming Conventions

**Files:**
- Core modules: lowercase with underscores (genizah_core.py, gui_threads.py)
- UI modules: lowercase with descriptive suffix (corrections_ui.py, text_editor.py)
- Tests: test_*.py prefix (test_api_flow.py)
- Utilities: *_utils.py suffix (sefaria_utils.py, shared_export_utils.py)

**Directories:**
- UI packages: plural nouns (pages/, components/, migrations/)
- Feature packages: domain names (web/, pgp_data/)
- Data: lowercase (data/, tests/, docs/)
- Generated: .suffix (Genizah_Index/, .nicegui/, .planning/)

**Classes:**
- PascalCase for all classes (SearchEngine, MetadataManager, ListsManager)
- Managers: *Manager suffix (MetadataManager, ListsManager, LabEngine)
- Dialogs: *Dialog suffix (LoginDialog, CorrectionSubmitDialog)

**Functions:**
- snake_case for all functions (normalize_shelfmark, create_layout)
- Create/init: create_* prefix (create_search_page, create_layout)
- Get/set: get_* / set_* prefix (get_library_display, set_language)
- Check/is: is_* / has_* prefix (is_rtl, has_permission)

**Variables:**
- snake_case for all variables (search_results, metadata_mgr)
- Constants: UPPER_CASE with underscores (SUPABASE_URL, APP_VERSION)
- Private: leading underscore (_internal_state, _cache)

## Where to Add New Code

**New Feature (E.g., Export to Format X):**
- Primary code: `C:/GenizahSearch/web/export_service.py` (add format class)
- Desktop support: Add exporter to `C:/GenizahSearch/genizah_app.py` export menu
- Tests: `C:/GenizahSearch/tests/test_export_service.py`
- Documentation: `C:/GenizahSearch/docs/CODE_INDEX.md` (update function reference)

**New Component/Module (E.g., Filter Dialog):**
- Implementation: `C:/GenizahSearch/web/components/my_component.py`
- Export: Add to `C:/GenizahSearch/web/components/__init__.py`
- Usage: Import in pages where needed (e.g., `from web.components.my_component import MyComponent`)
- Tests: Optional component tests in `C:/GenizahSearch/tests/test_components/`

**New Page (E.g., /statistics):**
- Create: `C:/GenizahSearch/web/pages/statistics.py` with `create_statistics_page()` function
- Route: Add `@ui.page('/statistics')` decorator in `C:/GenizahSearch/web/main.py`
- Navigation: Add link to sidebar nav in `create_layout()` function
- Styling: Use existing CSS classes from `COMMON_STYLES` in main.py

**New Search Mode (E.g., Phonetic):**
- Add to SearchEngine: `C:/GenizahSearch/genizah_core.py` search() method
- Update dispatch logic: Add case for new mode (e.g., 'phonetic')
- Implement: Add phonetic_search() helper method
- Tests: Add test case in `C:/GenizahSearch/tests/test_boundary_search.py`

**Utilities:**
- Shared helpers: `C:/GenizahSearch/shared_export_utils.py`
- Format-specific: Keep in module that uses them (genizah_core, corrections_ui, etc.)
- Translation strings: Add to TRANSLATIONS dict in `C:/GenizahSearch/genizah_translations.py`

## Special Directories

**Genizah_Index/**
- Purpose: Tantivy full-text search index (compiled Rust binary format)
- Generated: `python build_index.py` (time-consuming, indexes all manuscripts)
- Committed: Yes, to .git (large but essential)
- Rebuilt: When manuscripts.csv changes or HTR models updated
- Size: ~2GB (handles 500K+ documents)

**pgp_data/**
- Purpose: Princeton Geniza Project transcriptions linked to GenizahSearch sys_ids
- Generated: Scripts export transcriptions from PGP database
- Contents:
  - transcriptions_linked.csv: 9,364 PGP records matched to GenizahSearch (96.5% match rate)
  - transcriptions_unmatched.csv: 339 PGP records not found in GenizahSearch
  - fist_shelfmarks_supplement.csv: 130,947 FIST shelfmarks for matching
- Scripts: `C:/GenizahSearch/scripts/pgp_transcriptions_export.py`, `C:/GenizahSearch/scripts/fist_shelfmarks_export.py`
- Updated: Feb 5, 2026 (latest integration)

**.nicegui/**
- Purpose: NiceGUI generated artifacts (local storage, build cache)
- Generated: Automatically by NiceGUI on startup
- Committed: No (in .gitignore)
- Contents: Browser storage (JSON files), client-side data

**.planning/codebase/**
- Purpose: GSD (Genizah Search Documentation) analysis documents
- Contents: ARCHITECTURE.md, STRUCTURE.md, CONVENTIONS.md, TESTING.md, CONCERNS.md, STACK.md, INTEGRATIONS.md
- Committed: Yes (reference docs for implementation planning)

**data/logs/**
- Purpose: Runtime application logs
- Format: CSV with timestamp, level, module, message
- Rotation: SafeRotatingFileHandler (graceful Windows file locking)
- Path: `C:/GenizahSearch/data/logs/genizah.log`
- Size: Rotated after reaching max size

