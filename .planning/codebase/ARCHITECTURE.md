# Architecture

**Analysis Date:** 2026-02-05

## Pattern Overview

**Overall:** Three-tier application with monolithic core engine, dual UI frontends, and cloud database backend.

**Key Characteristics:**
- **Hybrid Architecture** - Desktop (PyQt6) and Web (NiceGUI) clients sharing common core engine
- **Modular Core** - Monolithic `genizah_core.py` with separate concerns: search, indexing, metadata, AI, lists management
- **Supabase Backend** - Centralized PostgreSQL for user data (lists, corrections, comments, discoveries)
- **Local-First Search** - Tantivy full-text index runs locally; remote IIIF/APIs for images
- **Direct Cloud Integration** - Web app connects directly to Supabase (FastAPI backend removed Jan 2026)

## Layers

**Search & Indexing Engine:**
- Purpose: Full-text search across 500K+ Cairo Genizah manuscript fragments using Tantivy (Rust-based)
- Location: `C:/GenizahSearch/genizah_core.py` (SearchEngine, Indexer classes)
- Contains: Query parsing, fuzzy matching, variant handling, regex support, shelfmark search
- Depends on: Tantivy index (`Genizah_Index/`), metadata CSV (libraries.csv)
- Used by: Both web and desktop UIs, parallels detection, lab engine

**Metadata & Manuscript Data:**
- Purpose: Load, cache, and query ~217K manuscript records from libraries.csv with 8-column structure
- Location: `C:/GenizahSearch/genizah_core.py` (MetadataManager class)
- Contains: System numbers, shelfmarks, call numbers, library codes, titles, Oxford part IDs
- Depends on: CSV file parsing, caching for performance
- Used by: Search engine, UI display, export/corrections systems

**Variant Management:**
- Purpose: Handle Hebrew character variants from HTR (handwriting recognition) accuracy issues
- Location: `C:/GenizahSearch/genizah_core.py` (VariantManager), `C:/GenizahSearch/unified_variants.py`
- Contains: 500+ character variant pairs, configurable "top N pairs" via user slider
- Depends on: Unified variant dataset, lab engine settings
- Used by: Search engine for expanded query matching

**Lab Engine & Settings:**
- Purpose: Manage experimental/advanced search settings (variant aggressiveness, rare letter weighting)
- Location: `C:/GenizahSearch/genizah_core.py` (LabEngine class)
- Contains: User preferences, algorithm parameters, rare-letter frequency tables
- Depends on: Metadata, variant manager
- Used by: Search engine for behavior tuning

**Web Application (NiceGUI):**
- Purpose: Modern, professional web UI for manuscript research
- Location: `C:/GenizahSearch/web/` directory
- Contains: Page routes, components, state management, Supabase integration
- Depends on: genizah_core, Supabase client, IIIF APIs
- Entry point: `C:/GenizahSearch/web/main.py` (port 8081)

**Desktop Application (PyQt6):**
- Purpose: Feature-rich local GUI with advanced analysis tools
- Location: `C:/GenizahSearch/genizah_app.py` (720K+ lines)
- Contains: Full search, browse, corrections, exports, AI assistance
- Depends on: genizah_core, local Supabase client, export libraries (DOCX, XLSX, PDF)
- Entry point: `C:/GenizahSearch/genizah_app.py` (PyQt6 application)

**Supabase Backend (PostgreSQL):**
- Purpose: Cloud persistence for user-generated data
- Location: Remote at https://ylcpglwxompwjcufdemz.supabase.co
- Contains: Users, profiles, lists, items, corrections, comments, discoveries, joins
- Used by: Web app (direct), desktop app (via supabase_corrections_client.py)
- Auth: Email/password and Google OAuth2

**Community Features:**
- Purpose: User contributions (corrections, comments, discoveries)
- Location: `C:/GenizahSearch/web/supabase_client.py`, `C:/GenizahSearch/supabase_corrections_client.py`
- Contains: Correction submission/approval, text comments, user discoveries, text joins
- Depends on: Supabase, auth state
- Used by: Both web and desktop UIs

## Data Flow

**Search Flow:**

1. User enters query in search UI
2. Web/Desktop UI calls SearchEngine.search() or lab_engine.search()
3. SearchEngine parses query (detect mode: exact, variants, fuzzy, regex, shelfmark, title)
4. Tantivy index queried with (optionally expanded) terms
5. Results returned with snippets, scoring, metadata enrichment
6. UI displays paginated results with highlighting
7. User selects result → browse page loads with IIIF images + transcriptions

**Manuscript Display Flow:**

1. User clicks result or navigates to `/browse?sys_id=...`
2. BrowsePage fetches manuscript pages from NLI IIIF API
3. Page metadata (fl_id, transcription, images) displayed in two-panel layout
4. Image viewer: OpenSeadragon for zoom/pan (client-side)
5. Transcription panel: Hebrew RTL text with search highlighting
6. User can create corrections → POST to Supabase via web.supabase_client or desktop client

**User Lists Flow:**

1. Anonymous user creates local list (stored in browser/local app storage)
2. User logs in → lists synced to Supabase
3. Authenticated requests use JWT token from auth state
4. CRUD operations on lists/items stored in Supabase
5. Desktop app syncs via lists_sync.py background task

**Corrections/Comments Flow:**

1. User submits correction text + notes → Supabase corrections table
2. Admin reviews in /admin page → approves/rejects
3. Approved corrections appear on browse page + parallels
4. Comments and discoveries similarly managed
5. Joins (text matches across manuscripts) tracked

**State Management:**

- Web app: `web.state.AppState` singleton holds engines, results, paralels
- Desktop app: Global variables + class state (QWidget properties)
- User auth: `web.auth_state.GlobalAuthState` for web; `supabase_corrections_client` for desktop
- UI state per page: React-like patterns (search_state, advanced_view_state, etc.)

## Key Abstractions

**SearchEngine:**
- Purpose: Unified search interface across multiple algorithms
- Examples: `C:/GenizahSearch/genizah_core.py` lines ~3000-3500
- Pattern: Single class with search() method that dispatches on query mode

**MetadataManager:**
- Purpose: Lazy-load, cache, and query manuscript CSV
- Examples: `C:/GenizahSearch/genizah_core.py` (MetadataManager.__init__, get_system, search_by_title)
- Pattern: Singleton with background loading, caching via lru_cache

**ListsManager (Local) / UserListsManager (Cloud):**
- Purpose: Manage research lists with dual-storage support
- Examples: `C:/GenizahSearch/genizah_core.py` (ListsManager), `C:/GenizahSearch/web/user_lists.py` (UserListsManager)
- Pattern: Local manager as storage layer, user manager as cloud-aware wrapper

**Page Components (Web):**
- Purpose: Modular, reusable UI building blocks
- Examples: `C:/GenizahSearch/web/components/` (text_editor.py, notes_display.py, translate_button.py)
- Pattern: Single-responsibility functions returning UI elements

**Export Service:**
- Purpose: Multi-format export (XLSX, DOCX, CSV, JSON)
- Examples: `C:/GenizahSearch/web/export_service.py`
- Pattern: Format-specific classes with common interface

## Entry Points

**Web Application:**
- Location: `C:/GenizahSearch/web/main.py`
- Triggers: `python -m web.main` (from project root)
- Responsibilities:
  - Initialize NiceGUI app with theme system, metadata, search engines
  - Register page routes (/, /search, /browse, /lists, /corrections, etc.)
  - Set up Supabase client, auth state, WebSocket heartbeat
  - Load CSS custom properties, meta tags, analytics

**Desktop Application:**
- Location: `C:/GenizahSearch/genizah_app.py`
- Triggers: `python genizah_app.py` or Windows executable
- Responsibilities:
  - Initialize PyQt6 main window with tabs (Search, Browse, Lab, AI, Corrections, Community)
  - Load search engine, variant manager, lab engine, AI manager
  - Connect UI signals to search/export/correction threads
  - Background threads for long-running operations (search, composition, updates)

**FastAPI Removed:**
- Note: `C:/GenizahSearch/server.py` exists but is not used (Jan 2026 migration)
- All routes now handled by Supabase or direct web API calls

## Error Handling

**Strategy:** Try-catch with logging to file and user-friendly messages

**Patterns:**
- Search failures: Return empty results or fallback to fuzzy search
- Supabase errors: Catch AuthApiError, show login prompt; network errors show offline message
- Image load failures: Show placeholder with fallback to external link
- Export errors: Show error dialog with retry option
- File I/O: SafeRotatingFileHandler for graceful Windows file locking

## Cross-Cutting Concerns

**Logging:**
- Desktop: File-based via `genizah_core.get_logger()` → `data/logs/`
- Web: Standard Python logging with optional file rotation
- Format: Timestamp, level, module, message

**Validation:**
- Shelfmark normalization: `normalize_shelfmark()` canonical implementation
- Email validation: Supabase handles, optional affiliation field
- Search input: Sanitize regex, prevent injection
- Export data: UTF-8 encoding, Excel cell limits

**Authentication:**
- Web: Supabase JWT tokens stored in app.storage.user
- Desktop: OAuth/email token in local storage or cached session
- RLS (Row-Level Security): Supabase tables use auth.uid() for access control

**Hebrew RTL Support:**
- Web: NiceGUI/Quasar RTL mode, CSS direction/text-align, language selector
- Desktop: PyQt6 layout mirroring via setLayoutDirection(Qt.LayoutDirection.RightToLeft)
- Text direction: Automatic detection via `is_rtl()` function

**Theming:**
- Web: CSS custom properties (--primary-*, --bg-*, --text-*), three themes (light, dark, parchment)
- Desktop: QPalette system for light/dark modes
- Stored in: app.storage.user['theme']

