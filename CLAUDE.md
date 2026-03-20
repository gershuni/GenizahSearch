# GenizahSearch - Claude Context

> This file provides context for AI assistants working on the GenizahSearch project.

## Project Overview

GenizahSearch is a collaborative research platform for the Cairo Genizah, featuring:
- **Web Application** (NiceGUI) - https://genizahsearch.com
- **Desktop Application** (PyQt6) - Windows executable
- **Supabase Backend** - Cloud database for user data

## Architecture (January 2026)

```
Web App (NiceGUI) ──────────────┐
         │                       │
         ├── Tantivy Index       ├──► Supabase (PostgreSQL)
         │   (local search)      │    - User auth
         │                       │    - Lists, corrections
         ├── pgp.db (SQLite)     │    - Comments, discoveries
         │   (PGP reference data)│
         ├── fjms_enrichment.db  │
         │   (FJMS scholarly)    │
         ├── nli_crossref.db     │
         │   (NLI images/meta)   │
         ├── joins.db (SQLite)   │
         │   (saved puzzle joins)│
Desktop App (PyQt6) ────────────┘
```

**Note:** The FastAPI backend was removed in January 2026. All read-only reference data is now served from local SQLite sidecars (pgp.db, fjms_enrichment.db, nli_crossref.db). Supabase is retained only for community features (auth, corrections, lists, comments).

## Key Files

### Core
- `genizah_core.py` - Search engine, data models, core logic
- `genizah_app.py` - Desktop application (PyQt6)

### Web
- `web/main.py` - Web app entry point
- `web/pages/` - Page components (search.py, browse.py, lists.py, etc.)
- `web/components/` - Reusable UI components
- `web/supabase_client.py` - Supabase integration

### Puzzle (Fragment Puzzle / Join Documents)
- `shared/puzzle_model.py` - PuzzleDocument/PuzzleFragment dataclasses
- `shared/puzzle_service.py` - SQLite CRUD for joins.db sidecar
- `shared/puzzle_export.py` - Composite PNG export, thumbnail generation
- `shared/puzzle_image_service.py` - IIIF image fetch + background removal + cache versioning
- `shared/background_removal.py` - HSV-based background removal engine
- `web/pages/puzzle.py` - Web puzzle page (Fabric.js canvas + unified image loader)
- `web/puzzle_tokens.py` - HMAC upload token generation/verification

### Browser Extension (GenizahSearch Image Helper)
- `extension/manifest.json` - Chrome MV3 manifest with NLI host permissions
- `extension/manifest.firefox.json` - Firefox MV3 manifest (gecko settings, background.scripts)
- `extension/background.js` - Service worker fetching NLI images as binary
- `extension/content_script.js` - Page↔background bridge + extension detection
- `extension/icons/` - Extension icons (16/48/128px)
- `extension/build.py` - Builds Chrome and Firefox ZIP packages into extension/dist/

### Desktop
- `supabase_corrections_client.py` - Desktop Supabase client
- `lists_sync.py` - Cloud sync for lists

## Common Tasks

### Running the Web App
```bash
python -m web.main
```
Opens on port 8080 or 8081.

### Running the Desktop App
```bash
python genizah_app.py
```

## Important Conventions

1. **Hebrew RTL** - Many strings are in Hebrew, text is right-to-left
2. **Shelfmarks** - Manuscript identifiers like "T-S 12.123", "MS Heb c 57"
3. **sys_id** - Internal unique identifier for manuscripts
4. **fl_id** - Fragment/leaf identifier (e.g., "T-S 12.123.1r" for recto)
5. **library_code** - Abbreviated library identifier (e.g., "CUL", "JTS", "Oxford")

## Data Files

### libraries.csv
Master metadata file for all ~217,000 manuscript records.

**Structure:**
```csv
system_number,oxford_part_id,call_numbers,library_code,,,,titles_non_placeholder
```

| Column | Index | Description |
|--------|-------|-------------|
| system_number | 0 | Unique sys_id |
| oxford_part_id | 1 | Oxford part identifier (optional) |
| call_numbers | 2 | Pipe-separated shelfmark variants |
| library_code | 3 | Library abbreviation (CUL, JTS, etc.) |
| titles_non_placeholder | 7 | Hebrew title |

**Library Codes:**
- `CUL` - Cambridge University Library (~128K records)
- `JTS` - Jewish Theological Seminary (~30K)
- `RNL` - National Library of Russia (~17K)
- `Oxford` - Bodleian Libraries (~13K)
- `Manchester` - University of Manchester (~12K)
- `BL` - British Library (~8K)
- `AIU` - Alliance Israélite Universelle
- `Mosseri`, `Gaster`, `Halper` - Private collections
- And others (see `genizah_core.LIBRARY_CODES`)

### joins.db (SQLite sidecar)
Stores saved puzzle/join documents. Created automatically in `joins_data/` on first save.
- `join_documents` - Document records (id, title, notes, fragments_json, thumbnail_b64)
- `join_document_fragments` - Fragment index for reverse lookups (doc_id, fl_id, sys_id)

## Documentation

See `docs/DOCUMENTATION_INDEX.md` for full documentation structure:
- `docs/guides/` - Admin and deployment guides
- `docs/plans/` - Implementation plans
- `docs/specs/` - Technical specifications
- `docs/archive/` - Historical documents

## Code Style

- Python 3.10+
- NiceGUI for web UI
- PyQt6 for desktop UI
- Type hints encouraged
- Hebrew comments are acceptable

## Environment Variables

```
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJ...
POSTHOG_API_KEY=phc_xxxxx (optional - enables PostHog analytics)
WEB_PUZZLE_ENABLED=true (enables web puzzle page - requires browser extension for bg removal)
PUZZLE_UPLOAD_SECRET=xxx (optional - HMAC secret for puzzle upload tokens; auto-generated if unset)
```

## Testing

```bash
pytest tests/
```

## Common Issues

1. **Search not working** - Check if Tantivy index exists in `Genizah_Index/`
2. **User data not syncing** - Check Supabase connection and credentials
3. **Images not loading** - NLI/Cambridge IIIF APIs may be down

## Documentation Maintenance (Important!)

**AI agents working on this codebase should keep documentation updated.**

### Open Issues Tracker (REQUIRED)

**`docs/OPEN_ISSUES.md`** is the central issue tracking document. **You MUST maintain it:**

1. **At session start:** Read `docs/OPEN_ISSUES.md` to understand current status
2. **After fixing any bug:** Mark the issue as `✅ Fixed (YYYY-MM-DD)` with date
3. **After finding new bugs:** Add to appropriate section with `❌ Open` status
4. **At session end:** Update the "Last Updated" timestamp and summary counts

```markdown
# Format for marking issues complete:
| Issue | ❌ Open | Notes |
↓
| Issue | ✅ Fixed (2026-02-03) | Notes |
```

### When to Update Docs

| If you change... | Update these docs |
|------------------|-------------------|
| Architecture/infrastructure | `CLAUDE.md`, `docs/guides/DEPLOYMENT_TECHNICAL.md` |
| Supabase schema (tables, RLS) | `docs/guides/SUPABASE_GUIDE.md` |
| Web app pages/components | `docs/CODE_INDEX.md` |
| Environment variables | `CLAUDE.md`, `docs/guides/DEVELOPER_GUIDE.md` |
| Major features | `CHANGELOG.md`, `README.md` |
| **App version** | Run `python scripts/bump_version.py X.Y.Z` (see below) |

### Version Bumping (REQUIRED for releases)

Run the automated script — it updates all version files at once:
```bash
python scripts/bump_version.py 6.3.0          # apply changes
python scripts/bump_version.py 6.3.0 --dry-run # preview only
```

**Files updated automatically:**
| File | What changes |
|------|-------------|
| `version.py` | `APP_VERSION` (source of truth, imported by both apps) |
| `version_info.txt` | Windows EXE metadata (`filevers`, `prodvers`, `FileVersion`, `ProductVersion`) |
| `CompileScriptGenizah.iss` | Inno Setup `#define MyAppVersion` + `OutputBaseFilename` |
| `README.md` | Header line |

**Manual steps after running the script:**
1. `CHANGELOG.md` — add `## [X.Y.Z]` section with release notes
2. `CLAUDE.md` "Recently Changed" — add entry for the new version
3. `README.md` "What's New" section — update feature description

### Before Finishing a Session

Run the documentation health check:
```bash
python scripts/check_docs.py
```

If it reports issues, fix them before committing.

### Key Docs to Keep Updated

1. **`CLAUDE.md`** - This file! Update if architecture changes
2. **`docs/guides/DEPLOYMENT_TECHNICAL.md`** - Server/deployment info
3. **`docs/guides/SUPABASE_GUIDE.md`** - Database schema and queries

### Outdated Terms to Avoid

These terms indicate outdated documentation:
- `FastAPI` / `backend server` - Removed in Jan 2026
- `genizah-backend` service - No longer exists
- `DATABASE_URL` - Replaced by `SUPABASE_URL`
- `port 8000` - Backend port no longer used

## Recently Changed

- March 2026: v7.2.2 Desktop Browse Tab Polish -- emoji icon buttons matching ResultDialog, reorganized ext_info_row (Puzzle/Parallels/List + Info/Bib/Catalog/External links + compact translations toggle), dynamic external library link buttons (Cambridge/Oxford/Manchester/Princeton), cross-shelfmark page navigation (Prev/Next wrap at boundaries), extended info and image toggle state preserved across navigation, fullscreen image viewer (FullscreenImageWindow with zoom/rotate/adjustments/arrow-key page nav, works from both browse and modal ResultDialog), enrichment race condition fix (stale Part context leak), centralized _start_browse_enrichment() with generation counter (desktop)
- March 2026: v7.2.0 Image Adjustment Controls -- brightness/contrast/gamma sliders + invert toggle on all image viewers (web browse standard/fullscreen/reading desk, web search advanced, desktop ManuscriptViewerWidget), desktop export bakes in adjustments (copy/save), icon-based compact toolbar, per-viewer SVG gamma filters for reading desk isolation, LUT-based desktop pixel processing with 80ms debounce, browse page async slot context fix, desktop image race condition fixes (both apps)
- March 2026: v7.1.0 FIST Gap Fill & Expanded Catalog -- 38,673 new manuscript records from FIST.db (libraries.csv 216K→255K), 7 new library codes (Solomon, Reinach, Vatican, CentralArch, JCMainz, Corwin, Mehlman), metadata-only search (Title/Shelfmark returns records without transcription text), metadata-only browse (shows NLI images + FJMS enrichment instead of error), Yevr→EVR and Halper→Genizah shelfmark normalization, Mosseri CUDL image fallback, server stability fixes (both apps)
- March 2026: v7.0.1 Web Puzzle Browser Extension -- GenizahSearch Image Helper Chrome/Firefox extension for NLI image acquisition, unified _loadImageWithFallbacks() fallback chain (server cache → extension → localhost helper → direct NLI), HMAC upload tokens, server derivative cache with processing version, extension install banner, privacy policy page, nginx /api/ proxy fix, Firefox AMO submission
- March 2026: v7.0.0 Fragment Puzzle & Community Publishing -- visual canvas for arranging fragments (background removal, zoom/rotate/crop, folio nav, layer ordering), save/load join documents to joins.db, composite PNG export with metadata banner, recto/verso support, community publishing (publish/unpublish/fork/browse), Discoveries Center integration, All/My Puzzles tabs, fragment selector combobox+browse, clickable shelfmark badges, admin soft-delete, auto-unpublish on delete, 90+ Hebrew translations, bilingual help sections, Windows index rebuild fix (both apps)
- March 2026: v6.5.3 Image viewer copy & save -- right-click context menu on manuscript images with Copy Image and Save Image As (desktop)
- March 2026: v6.5.2 UI polish -- desktop ResultDialog icon+text compact buttons, web language toggle moved to header
- March 2026: v6.5.1 Bug fixes + session restore -- desktop composition ResultDialog nav for filtered/appendix results, web parallels parent_slot timer crash, session persistence for browse tabs/composition summary/active tab
- March 2026: v6.5.0 Search UX & Filtered Search -- focused search by manuscript properties (domain/author/work/date/material), ~580K Dicta translations (Hebrew↔English) for catalog data, translation toggle, browse-to-search navigation, citation reminder popup (both apps)
- March 2026: v6.2.0 Power-User UX -- composition search UX (timer, ETA, cancel with partial results, printed badge/filter), session persistence, search history dropdowns, desktop notifications, sleep prevention, copy menu, Hebrew library names (both apps)
- March 2026: v6.1.0/v6.1.1 Catalog Browse & Navigation -- faceted catalog browsing by domain/author/work, free-text filter, FIST v5.0.0 enrichment, 100x faster domain queries, async desktop catalog
- February 2026: v6.0.0 Local Data Architecture -- PGP data migrated to pgp.db sidecar (147MB), FJMS catalog descriptions expanded, offline browsing, desktop crash fixes, paginated search, performance optimizations (both apps)
- February 2026: v5.9.0 Multi-Source Image & Metadata Integration -- NLI crossref sidecar (815K), Cambridge/Manchester/JTS IIIF, folio navigation, bibliography, catalog refs (both apps)
- February 2026: v5.8.0 FJMS Integration -- domain classifications, scientific joins, catalog enrichment via SQLite sidecar (both apps)
- February 2026: v5.7.2 Cleanup, Normalization & Sections -- AI code removed, Unicode search normalization, full green test suite, structural HTML section parser
- February 2026: v5.7.0 Responsa Search -- syntax parsing, grammatical expansion, Judeo-Arabic, tabular query builder (both apps)
- February 2026: v5.6.0 Desktop Parity -- PGP integration, Virtual Reading Desk, 35K documents imported
- January 2026: Migrated from FastAPI to Supabase
- January 2026: Documentation reorganized into `docs/` subdirectories
