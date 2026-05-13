# GenizahSearch - Claude Context

> This file provides context for AI assistants working on the GenizahSearch project.

## Project Overview

GenizahSearch is a collaborative research platform for the Cairo Genizah, featuring:
- **Web Application** (NiceGUI) - https://genizahsearch.com
- **Desktop Application** (PyQt6) - Windows executable
- **Supabase Backend** - Cloud database for user data

## Architecture

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

The FastAPI backend was removed in January 2026. All read-only reference data is now served from local SQLite sidecars. Supabase is retained only for community features (auth, corrections, lists, comments).

## Key Files

### Core
- `genizah_core.py` - Search engine, data models, core logic
- `genizah_app.py` - Desktop application (PyQt6)

### Web
- `web/main.py` - Web app entry point
- `web/pages/` - Page components (search.py, browse.py, lists.py, etc.)
- `web/components/` - Reusable UI components
- `web/supabase_client.py` - Supabase integration
- `web/safe_storage.py` - Chokepoint for per-user state (post-Phase 87)

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
- `extension/manifest.firefox.json` - Firefox MV3 manifest
- `extension/background.js` - Service worker fetching NLI images as binary
- `extension/content_script.js` - Page↔background bridge + extension detection
- `extension/build.py` - Builds Chrome and Firefox ZIP packages

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
Master metadata file for ~255,000 manuscript records.

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

**Library Codes:** CUL (~128K), JTS (~30K), RNL (~17K), Oxford (~13K), Manchester (~12K), BL (~8K), AIU, Mosseri, Gaster, Halper, NLI, and others (see `genizah_core.LIBRARY_CODES`).

### joins.db (SQLite sidecar)
Stores saved puzzle/join documents in `joins_data/`.
- `join_documents` - Document records (id, title, notes, fragments_json, thumbnail_b64)
- `join_document_fragments` - Fragment index for reverse lookups (doc_id, fl_id, sys_id)

## Documentation

See `docs/DOCUMENTATION_INDEX.md` for full documentation structure:
- `docs/guides/` - Admin and deployment guides
- `docs/plans/` - Implementation plans
- `docs/specs/` - Technical specifications
- `docs/archive/` - Historical documents
- `CHANGELOG.md` - Full release history

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
WEB_PUZZLE_ENABLED=true (default: true)
PUZZLE_UPLOAD_SECRET=xxx (optional - HMAC secret for puzzle upload tokens; auto-generated if unset)
POSTHOG_IP_SALT=xxx (optional - HMAC salt for hashing client IPs; auto-generated if unset, production should set explicitly so hashes survive restarts)

# Search API (Phase 77-83 public HTTP/JSON API over the corpus)
SEARCH_API_MODE=open                  # open | localhost-only | disabled (flippable per request, no restart)
SEARCH_API_RATE_LIMIT=30              # per-IP requests/minute; shared ceiling but each endpoint has its own bucket
SEARCH_API_POSTHOG_SAMPLE_N=1         # capture every Nth API request to PostHog
SEARCH_API_BROWSE_TIMEOUT=1.0         # per-source enrichment timeout (PGP/FJMS/NLI), seconds
SEARCH_API_BROWSE_CORE_TIMEOUT=2.0    # core BrowsePage fetch timeout, seconds
SEARCH_API_BROWSE_TEXT_CAP=4000       # default char cap for transcription text; ?text_cap=N override bounded [100, 10000]

# Skill-side (cairo-genizah-research skill consumer)
GENIZAH_API_BASE=https://genizahsearch.com    # overrides --base-url CLI flag (env wins)
GENIZAH_SKILL_REQ_PER_MIN=24                  # skill self-throttle, leaves 6 rpm headroom under server's 30 rpm
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

### When to Update Docs

| If you change... | Update these docs |
|------------------|-------------------|
| Architecture/infrastructure | `CLAUDE.md`, `docs/guides/DEPLOYMENT_TECHNICAL.md` |
| Supabase schema (tables, RLS) | `docs/guides/SUPABASE_GUIDE.md` |
| Web app pages/components | `docs/CODE_INDEX.md` |
| Environment variables | `CLAUDE.md`, `docs/guides/DEVELOPER_GUIDE.md` |
| Major features | `CHANGELOG.md`, `README.md` |
| **App version** | Run `python scripts/bump_version.py X.Y.Z` |

### Version Bumping (REQUIRED for releases)

Run the automated script — it updates all version files at once:
```bash
python scripts/bump_version.py 6.3.0          # apply changes
python scripts/bump_version.py 6.3.0 --dry-run # preview only
```

**Files updated automatically:** `version.py` (source of truth), `version_info.txt` (Windows EXE metadata), `CompileScriptGenizah.iss` (Inno Setup), `README.md` (header line).

**Manual steps after running the script:**
1. `CHANGELOG.md` — add `## [X.Y.Z]` section with release notes
2. `CLAUDE.md` "Recently Changed" — add one-line entry
3. `README.md` "What's New" section — update feature description

### Before Finishing a Session

```bash
python scripts/check_docs.py
```

Fix any reported issues before committing.

### Outdated Terms to Avoid

- `FastAPI` / `backend server` - Removed in Jan 2026
- `genizah-backend` service - No longer exists
- `DATABASE_URL` - Replaced by `SUPABASE_URL`
- `port 8000` - Backend port no longer used

## Recently Changed

For full release history see `CHANGELOG.md`. Most recent:

- **May 2026: v7.12 Path B Phase 88 (State Separation by Deletion)** — internal milestone, not a release. Second phase of v7.12 Multitenant Architecture (Path B) refactor. Deletes the 10 per-user export-state singleton mirror fields from `web/state.py:AppState` (`last_results`, `current_search_query`, `current_search_mode`, `current_search_gap`, `last_filters_applied`, `last_search_warnings`, `last_selected_uids`, `parallels_results`, `parallels_filtered`, `parallels_search_meta`). Plan 88-01 migrated 13 writer sites across `web/pages/search.py`, `web/pages/search_results.py`, `web/pages/parallels.py` from `state.X = value` to local-variable threading through the existing `web.export_state.set_*` / `update_*` / `clear_*` calls (Codex round-5 catch: reorder of original plan ordering required because `set_search_export(...)` calls passed `state.current_search_gap` etc. as kwargs two lines below their assignments). Plan 88-02 rewrote `web/export_state.py` to route through Phase 87 `web.safe_storage` chokepoint helpers, deleted the production-code test-backend shim + selector helper (per D-09 + Phase 87 chokepoint discipline), hardened `update_*` functions with `isinstance(payload, dict)` guard (D-11) + copy-on-update (D-12), hardened getters `get_search_export()` / `get_parallels_export()` with isinstance guard (Refinement 4 — cross-AI review), folded `parallels_source_text` into the `set_parallels_export(meta={'source_text': ...})` payload (D-13), deleted the reader-side fallback at `web/api.py:1928-1931, 1962-1964, 2063-2066` (D-14), rewrote 4 affected test files (`test_export_cross_user_isolation`, `test_export_state_selection`, `test_api_export_json`, `test_api_legacy_unchanged`) to `monkeypatch.setattr('web.safe_storage.app', ...)` pattern with `SimpleNamespace`-based instance-isolated stubs (D-01, D-02, Refinement 6) — deleting the legacy proxy wrapper, dropping all `state.X =` fixture setup, adding a strengthened source_text cross-user leak regression test exercising a POSITIVE export path (D-15, Refinement 2), and deleted the `web/export_state.py` entry from `.planning/phase87_storage_allowlist.yaml` (allowlist count: 4 → 3 entries). Plan 88-03 deleted the 10 fields from `AppState.init()`, installed two permanent CI regression guards: runtime attr-absence test `tests/test_no_appstate_export_fields.py` (11 tests = 10 parametrized + 1 survivor sanity per D-06) and static AST scanner `tests/test_no_deleted_state_references.py` (D-07 — walks `web/` + `tests/` for `state.<deleted_field>` / `setattr(state, ...)` / `getattr(state, ...)` AND aliased imports per Refinement 5 (`from web.state import state as s`, `import web.state as web_state`), 4 tests = 3 seed-traps + 1 production scan), refreshed stale docstring/comment mentions per D-16 at `web/api.py:1846-1851` and `web/search_api.py:1198-1204`. Codex round-5 review reshaped plan ordering (locals-first instead of fields-first per D-04 + D-05) to eliminate the data-loss window where deletion-first ordering would feed stale defaults into `set_search_export(...)` kwargs. Cross-AI plan review (Gemini + Codex, 2026-05-13) refined plans pre-execution with 7 targeted improvements (scoped greps to `web/`+`tests/`, strengthened D-15 positive-path test, audit of `set_parallels_export(..., meta=None)` paths, getter isinstance guards, alias-import scanner coverage, SimpleNamespace stubs, Windows-tooling fallback notes). Full pytest suite green at each plan boundary (D-05). All 6 STATE-XX requirements satisfied. Zero user-visible behavior change. Web-only milestone — desktop unaffected. Hand-off chain: Phase 89 deletes `UserListsManager._cache_entry` singleton (LISTS-01..04); Phase 90 deletes `_client_cache` / `_session_locks` / `_CLIENT_CACHE_TTL` and the auth `_app.storage.user` allowlist entry (AUTHC-01..05); Phase 91 deletes the `web/auth_state.py` + `web/main.py` OAuth allowlist entries (AUTHW-01..06); Phase 92 final sweep + acceptance (SWEEP-01..06). (web)
- **May 2026: v7.12 Phase 87** (internal milestone) — Foundations for Path B multitenant refactor. `web/safe_storage.py` is now THE chokepoint for per-user state via `_session_uuid` lazy-mint helpers. Migrated 131 raw `app.storage.user.*` access sites across 14 files. AST-based pytest lint scanner (`tests/test_no_raw_storage_access.py`) is the permanent CI guard. Allowlist YAML covers bootstrap sites Phases 88-92 will delete. Zero user-visible change. (web)
- **May 2026: v7.11.0** — CUDL Coverage & Synthetic Inventories. Phases 84-86. FIST↔CUDL shelfmark bridge recovers thousands of CUDL classmarks. 108 image-bearing synthetic manuscripts injected (incl. T-S NS 329.96). **Deploy posture codified: scp DBs FIRST, then push code** (after 2026-05-11 incident). (both apps)
- **May 2026: v7.10.0** — Search API Public Release. Phases 77-83. Public HTTP/JSON endpoints `POST /api/search`, `GET /api/browse`, `POST /api/parallels`. OpenAPI at `/api/openapi.json`, Swagger at `/api/docs`. Reference skill `cairo-genizah-research`. `docs/SEARCH_API.md` is the public contract. (web)
- **May 2026: v7.9.4** — NLI Library Code Fix. Data-only: 461 manuscripts flipped Oxford → NLI in libraries.csv. (both apps)
- **April 2026: v7.9.3** — Visual Similarity Dialog Fixes. Firefox scroll, modifier-click open-in-new-tab, copy-paste includes shelfmarks. (web)
- **April 2026: v7.9.2** — PGP Data Refresh. pgp.db re-imported from upstream. (both apps)
- **April 2026: v7.9.1** — Catalog Attribution & Reading Desk Polish. FJMS Institution migration, JTS source-switch fix, CUL CUDL/NLI alignment helper, ilike injection sanitization. (both apps)
- **April 2026: v7.9.0** — Bundles v7.8 Structural Foundation + v7.9 Decomposition. Back-nav state-loss bugfix, CUL paired-leaf folio-label fix. Mostly internal refactor. (both apps)
- **April 2026: v7.7.2** — PageSpeed (a11y 85→96, perf 90→98). (web)
- **April 2026: v7.7.1** — SEO Round 2 (bilingual meta, JSON-LD, deferred PostHog). (web)
- **April 2026: v7.7.0** — Volume-Aware Browse. 3,193 multi-IE manuscripts get volume selector. (both apps)

## v7.12 Path B Milestone (active)

6 phases (87-92) refactoring web for multitenant safety. `deploy.sh` BLOCKED until ships.
- Phase 87 ✅ done (Foundations — session UUID + safe_storage chokepoint)
- Phase 88 ✅ done (State separation by deletion — 10 AppState mirror fields gone; D-06 runtime + D-07 static AST regression guards installed; allowlist 4→3)
- Phase 90: auth caching rewrite (deletes `web/supabase_client.py:111` allowlist)
- Phase 91: atomic auth-state writes (deletes `web/auth_state.py` + OAuth allowlist)
- Phases 89, 92: TBD
