# GenizahSearch - Claude Context

> This file provides context for AI assistants working on the GenizahSearch project.

## How to work here

**Start with `powershell -File scripts/init.ps1`.** It prints repo state, milestone state,
the tracker's newest entry, recent `_tmp/` working notes, and every gate's exit code. Run it
instead of reading documentation to re-orient.

**When the owner is asking a question, describing a problem, or thinking out loud, the
deliverable is your assessment — not a change.** Lead with the outcome: the first sentence
answers "what happened". Then stop.

**Before reporting progress, check each claim against a tool result from this session.**
Report only what you can point at evidence for, and say plainly when something is unverified.
"Tests pass" requires a test run in this session, not an inference from the diff.

**Write shell commands to a `.ps1` file and run the file** rather than composing long inline
commands. This is PowerShell 5.1: no `&&`, no ternary, and here-strings passed inline to a
native command get mangled. Use `git commit -F <file>` for commit messages.

**Keep the always-loaded files small.** `CLAUDE.md` and `docs/OPEN_ISSUES.md` are read every
session, so their size is a recurring cost; `scripts/check_docs.py` fails the build if either
passes its ceiling. When a ceiling is hit, split closed content into `docs/archive/` — never
raise the number.

**Effort is routed, not pinned.** Sessions run at `high`. Raise with `/effort xhigh` for the
calls where being wrong costs the owner grading hours — sample design and weighting, threshold
selection, the precedence matrix, relation-label taxonomy, anything that decides what a public
claim asserts. Drop to `/effort medium` for plumbing: deck builders, schema batches, harness
scaffolding, mechanical edits. Say which tier you are on when it is not the default.
Subagents run on Sonnet 5 (`CLAUDE_CODE_SUBAGENT_MODEL`).

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

The **standalone** backend service (the separate `genizah-backend` process on port 8000, with its own database and routers) was removed in January 2026. All read-only reference data is now served from local SQLite sidecars. Supabase is retained only for community features (auth, corrections, lists, comments).

> **Note:** FastAPI itself is still live and load-bearing — NiceGUI's `app` *is* a FastAPI instance, and `/api/*` routes (image proxies, exports, and the public Search API) are registered as FastAPI routes (`web/api.py::init_api_routes`) plus a dedicated `FastAPI()` sub-app mounted at `/api` in `web/main.py`. "Removed" refers only to the standalone backend process, not the framework.

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

### Discovery (Computed Identifications — live beta)
- `shared/discovery_service.py` - Async chokepoint for every discovery read (bounded concurrency, timeouts, LRU)
- `shared/discovery_relation_matrix.py` - Frozen precedence matrix; the ONLY source of a rendered relation
- `shared/discovery_locus.py` - Per-work citation-address ("locus") computation + range filtering
- `shared/discovery_panel_model.py` / `discovery_main_pool.py` - Panel display model + main-pool/more-matches rule
- `shared/discovery_band_labels.py` / `discovery_display_strings.py` - Honesty-safe vocabulary (no precision percentages)
- `shared/discovery_surface_projection.py` - Public-audience projection from the private artifact
- `web/discovery_assets.py` - Fail-closed sidecar loader + `discovery_available()` (flag AND sidecar readiness)
- `web/pages/findings.py` - Corpus-wide findings page (`/computed-identifications`)
- `web/pages/start.py` - Guided "Start Here" launchpad (`/start`)
- `web/identification_reviews.py` - Community-review storage boundary (Supabase RPCs only, never a direct write)
- `web/components/discovery_panel.py` / `findings_rows.py` / `identification_review.py` - Panel, rows, review dialog
- `web/components/discovery_links.py` - The one folio-correct link builder (AST-guarded: no surface may hand-build a `/browse` URL)

### Passage-Matching Parallels Search (web beta, Phase 145 — `method='passage'` on `/api/parallels`)
- `shared/passage_index.py` / `passage_search.py` / `passage_normalize.py` / `passage_policy.py` - The engine (fail-closed mmap reader, seed-and-extend query, versioned normalizer, frozen policy presets)
- `shared/passage_parallels.py` - `PassageSearcher`, a `CompositionSearcher` wrapper; bounded re-normalization builds highlight text for only the top-rendered rows
- `web/passage_assets.py` - Fail-closed index loader + `passage_available()` (flag AND index readiness)

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
6. **Supabase Data API grants** - Every migration/provisioning script that creates a `public` table intended for `supabase-js`/PostgREST/GraphQL access must include explicit `GRANT` statements for the needed roles, in addition to RLS and policies. This is required for new projects from 2026-05-30 and existing projects from 2026-10-30.

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

### Skill routing

- **Sketch findings for GenizahSearch** (validated design decisions, CSS patterns, visual direction —
  the Phase 136 discovery panel, its `/browse` embedding, the evidence-highlighting algorithm, and the
  corpus-wide findings page) → `Skill("sketch-findings-genizahsearch")`

## Code Style

- Python 3.10+
- NiceGUI for web UI
- PyQt6 for desktop UI
- Type hints encouraged
- Hebrew comments are acceptable

## Environment Variables

**Full reference: [`docs/guides/ENV_VARS.md`](docs/guides/ENV_VARS.md)** — every var, its
default, and the reasoning behind the non-obvious ones. Read it before changing any of them.

Only the vars that change **what you do** are repeated here: the flags that decide whether a
surface exists at all. Every one is **necessary but NOT sufficient** — each is ANDed with a
fail-closed readiness check on its data, so a flag-ON/asset-missing window still hides cleanly.
Never treat a flag alone as proof a feature is live.

| Flag | Default | Gates | ANDed with |
|---|---|---|---|
| `DISCOVERY_ENABLED` | `false` — **but ON in production since 2026-08-08** | `/computed-identifications`, the browse connections panel | `web/discovery_assets.py::discovery_available()` |
| `ATLAS_PREVIEW_ENABLED` | `false` | `/atlas` + its data routes and nav link | `web/atlas_assets.py::atlas_preview_available()` |
| `PASSAGE_PARALLELS_ENABLED` | `false` | `method='passage'` on `/api/parallels` + the method selector | `web/passage_assets.py::passage_available()` |
| `PASSAGE_MULTI_WITNESS_ENABLED` | `false` | the Witnesses panel + `witnesses[]` on the API | `passage_available()` |
| `IDENTIFICATION_REVIEWS_ENABLED` | `true` | community identification reviews (beta) | — |
| `FGP_TRANSCRIPTIONS_ENABLED` | `true` | FGP transcriptions as a version-chooser source | presence of `fgp_data/` |
| `WEB_PUZZLE_ENABLED` | `true` | the web Fragment Puzzle page | — |
| `SEARCH_API_MODE` | `open` | the public API (`open`/`localhost-only`/`disabled`) | — |

Three more you will hit directly while working:

- `MASKING_SCAN_PATTERNS_FILE` — dev/CI. Unset or empty makes `scripts/check_atlas_masking.py`
  **fail (exit 1) BY DESIGN**, never silently pass. A red masking scan with no file set is the
  tool working, not a bug.
- `GENIZAH_DISCOVERY_DATA_DIR` / `GENIZAH_PASSAGE_DATA_DIR` — dev/CI only, read ONCE at import.
  They select *which* directory is read; the loader still applies its full fail-closed contract.
  Leave unset in production.

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

1. **At session start:** read the **"Last Updated" note and "Quick Summary"** at the top, then
   read only the section relevant to the work at hand. **Do not read the whole file** — it is
   ~150 KB, it is resident for the rest of the session, and re-reading it is the single largest
   recurring context cost in this project. Grep for what you need.
2. **After fixing any bug:** mark it `✅ Fixed (YYYY-MM-DD)` **and move the entry to
   `docs/archive/OPEN_ISSUES_ARCHIVE.md`** in the same edit — closed items must not accumulate
   in the tracker.
3. **After finding new bugs:** Add to appropriate section with `❌ Open` status
4. **At session end:** Update the "Last Updated" timestamp and summary counts

Closed history (fixed/resolved/superseded + the full Change Log) lives in
`docs/archive/OPEN_ISSUES_ARCHIVE.md` (~320 KB). **Grep it; never read it whole.**
Re-split with `scripts/split_open_issues.ps1` if the tracker grows past ~150 KB again.

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

- standalone `backend server` / `genizah-backend` service - Removed in Jan 2026 (NOTE: FastAPI the framework is still live — it serves `/api/*`; only the separate backend process was removed)
- `DATABASE_URL` - Replaced by `SUPABASE_URL`
- `port 8000` - Backend port no longer used

## Recently Changed

**One line per release. Full release notes live in [`CHANGELOG.md`](CHANGELOG.md)** — every
version below is a `## [X.Y.Z]` section there. Do not re-expand this list; it is loaded into
every session's context.

### Facts that outlive their release (keep these here)

- **Where code lives.** Search / metadata / variants / responsa / engines are in `shared/*.py`,
  not `genizah_core.py` (which is a 755-line facade re-exporting 27 names). Desktop dialogs,
  widgets, and update-UI are in `desktop/*.py`. **Grep `shared/` and `desktop/`**, not just the
  old god-files. (v8.3.0 decomposition)
- **LOCAL extractor is at `extraction_format_version` 3.** Libraries indexed before that need a
  manual **Re-index All**; there is no auto-flip (bulk re-extraction must never run from
  `__init__` or the UI thread).
- **`97` is the LOCAL "My Library" namespace, never a corpus prefix.** Corpus sys_ids are
  all `99` (255,723/255,723 in `libraries.csv`). Import from `shared/sys_id_patterns.py`:
  `CORPUS_SYS_ID_RE`, or `ANY_SYS_ID_RE` only where a LOCAL header can arrive. A lint
  fails CI on a hand-rolled one.
- **Web is not continuous-deploy.** Deploy DBs/assets first (`scp`), then push code.
- **Still open:** the `Set-Cookie`-on-every-response Cloudflare BYPASS for ordinary page requests —
  see `docs/OPEN_ISSUES.md`. Partially mitigated 2026-08-13 (`ab064ff9`): static/immutable assets now
  bypass the session-cookie middleware and are cacheable; page HTML, APIs and auth are unchanged.
- **The discovery beta is LIVE in production** (`DISCOVERY_ENABLED=1` since 2026-08-08) — but it was
  flipped on ahead of the REL-01 gate the roadmap defines, and the planning record was never updated
  to say so. Treat `.planning/STATE.md`'s "the flag must NOT be flipped yet" as historical.

### Releases

- **v9.1.0 — Several Witnesses of One Work (2026-08-27)** — desktop multi-witness letter-level search: a Witnesses panel in the Composition tab (paste one, or a file split on blank lines), promote manuscripts from your own results, and optional auto-expand rounds; results are rank-fused (RRF k=60), NEVER concatenated — 17 Birkat Hamazon witnesses fused find 455 of 614 census manuscripts (74.1%) against 348 (56.7%) for the best single witness and 296 (48.2%) concatenated. Stop now works BETWEEN witnesses (the one in flight finishes; "New" defers and never terminates), so the witness cap is a flat 25 at every depth. "Full Recursive Search" runs fusion in letter-level mode and still concatenates in chunk mode, which is correct for that engine. Letter-level search is now the DEFAULT method once an index exists -- but never over a stored choice; the orange "New" markers are one-shot (`config.pkl`, not the session). **Desktop-only.** (desktop)
- **v8.6.0 — Pause/Resume, and Stop that actually stops (2026-08-20)** — desktop Pause/Resume for regular, Lab and Composition searches (parks the worker at a checkpoint; Resume continues at the same index, paused time excluded from timings); Stop repaired in Lab Mode (three causes, incl. a bare `except Exception` eating `InterruptedError`, an `OSError` subclass) and stopped runs now keep partial results in Title/Shelfmark/My Library; monotonic elapsed timing; search toolbar no longer overlaps itself (row 2 needed 1423 px on a 1440 px screen). **Desktop-only — the desktop line continues from v8.5.2, since 9.0.0 was web-only and never tagged.** (desktop)
- **v9.0.0 — Computed Identifications, the Visual Atlas & Start Here (public beta, 2026-08-16)** — `DISCOVERY_ENABLED` flipped ON in production 2026-08-08; `/computed-identifications`, the browse connections panel and the `/help` methods section are publicly reachable. Then a fast post-launch run: catalogue-divergent rows un-hidden (3,570, 12.5% of the main pool), a frozen relation-precedence matrix, a "View text match" excerpt view, a `/start` guided launchpad, homepage discovery entry points, and beta community identification reviews. **Desktop 9.0.0 followed 2026-08-26** — letter-level (passage) search in the Composition tab, on a locally built index; that half does carry a GitHub Release + installer. One REL-01 gate item (the cross-surface masking re-sweep) is still open; see ROADMAP.md. (both)
- **v8.5.2 — Composition Filter Fix & Web Responsiveness (2026-07-31)** — desktop three-state Printed filter read the wrong Qt data role so it never filtered; web fixed five `await <sync fn>` sites that stalled the single event loop, plus always-on `web/perf_watch.py`. (both)
- **v8.5.1 — Browse doubled-folios fix + Visual Genizah Atlas live (2026-07-21)** — single-IE browse maps now dedupe by `(ie_id, p_num)`; the Atlas went live behind `ATLAS_PREVIEW_ENABLED=1`. (both)
- **v8.5.0 — Smarter Default Transcription (2026-07-14)** — the reading view no longer defaults to a partial FGP transcription over a fuller MiDRASH/HTR; coverage ratio in `shared/fgp_service.py::choose_default_source`. Bundles the desktop telemetry re-ask. (both)
- **v8.4.1 — Public API Dual-Mode (2026-07-01)** — `filters.library_filter_mode` (include/exclude) on `/api/search` + `/api/parallels`; `default=None` keeps callers byte-for-byte compatible. (web)
- **v8.4.0 — Dual-Mode Library Filter (2026-07-01)** — Show-only / Hide library filter at web+desktop parity, persisted across searches. (both)
- **v8.3.0 — Search & Browse: Library Filter + Space-Scroll (2026-06-29)** — multi-select library filter; Space / Shift+Space page-scrolls the results area. (both)
- **v8.3.0 — God-File Decomposition (2026-06-26, internal)** — `genizah_core.py` 12,500 → 755 lines behind a permanent 27-name re-export facade; desktop UI classes moved to `desktop/`. Zero behavior change. AST back-edge guards lock the layering. (both)
- **v8.2.2 — FGP Credits, Homepage Stats, Catalog Filters (2026-06-24)** — bilingual per-transcription FGP credits recomputed from each transcription's own `image_id`, fixing the per-manuscript aggregation bug. (both)
- **v8.2.1 — Recently Viewed fixes (2026-06-23)** — the recent list read `list_items` instead of `recent_items` and lost recency ordering; system/default list names now localized. (both)
- **v8.2.0 — Web Joins Lab, FGP Transcriptions & Hebrew Search (2026-06-22)** — `/joins-lab` ported to web; FGP transcriptions go-live; `hebword` tokenizer + diacritic-folded `content_search`. (both)
**Older releases (SEED-006 back to v7.7.x) are in [`CHANGELOG.md`](CHANGELOG.md) only.**
They were one-line duplicates of sections that file already carries in full, and nothing
in them changes how you work today. If you need one, grep the changelog for its version.
