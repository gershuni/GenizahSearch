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

```
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_ANON_KEY=eyJ...
POSTHOG_API_KEY=phc_xxxxx (optional - enables PostHog analytics)
WEB_PUZZLE_ENABLED=true (default: true)
ATLAS_PREVIEW_ENABLED=false   # web-only (Phase 133, ATLAS-01). Default OFF — gates the Visual Atlas Preview beta: the /atlas page, its /atlas-data/* routes, and the nav link. The flag is necessary but NOT sufficient: web/atlas_assets.py::atlas_preview_available() ANDs it with the baked-asset readiness (manifest + plain .bin loaded at startup from repo-root atlas_data/, OUTSIDE web/static/), so a flag-ON/asset-missing window still hides cleanly. Set to 1/true to enable in the beta env.
MASKING_SCAN_PATTERNS_FILE=/path/to/.masking_patterns   # dev/CI-only (Phase 133+). Points scripts/check_atlas_masking.py at a gitignored, newline-delimited restricted-string ("M-source") pattern file (never committed in cleartext; same secret-handling posture as .env). Unset/empty => the masking scan fails safe (exit 1, never a silent green). Forerunner of the permanent DATA-05 CI guard (Phase 134).
DISCOVERY_ENABLED=false   # web-only (Phase 134, DATA-07). Default OFF at the code level, but **set to 1/true in production since 2026-08-08** — /computed-identifications and the browse-page connections panel are publicly live there (confirmed by commit 04434714; see CHANGELOG.md [Unreleased]). Local/dev checkouts stay OFF unless set explicitly. The flag is necessary but NOT sufficient: web/discovery_assets.py::discovery_available() ANDs it with the discovery.db sidecar's startup-loaded readiness (versioned, fail-closed: exact manifest asset_basename, content-hash match, PRAGMA integrity_check, schema_version reject-incompatible, release-contract row counts, required meta/tables, frozen enum vocab) — a flag-ON/sidecar-missing-or-corrupt window still hides cleanly. Set to 1/true once Phase 135+ ships a surface to gate.
DISCOVERY_FINDINGS_COUNT_MAX=0   # web-only (Phase 136). Default 0 = OFF: the corpus-wide findings page reports its EXACT total. Above 0, the counting query stops at the cap and the envelope reports total=cap with meta.approximate_total=True — a capped total reported as exact is a correctness defect, not a tuning choice. Findings page ONLY: the connections-panel work-expansion total is exact by contract and deliberately has NO equivalent knob (its approximate-total escape was withdrawn in Phase 136 pre-flight — the honest degradation there is `timeout`, not a truncated number). Read live in shared/discovery_service.py; full rationale in docs/specs/discovery-budgets.md.
DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES=24   # web-only (Phase 136). The BROWSE-path bounded-concurrency budget, separate from the heavy one (DISCOVERY_MAX_CONCURRENT_QUERIES=4). The two numbers MUST differ (a cold panel load issues 3 reads concurrently plus a fourth, so the heavy cap of 4 would put the second simultaneous visitor into `busy`), and each budget has its OWN ThreadPoolExecutor sized to it -- two semaphores over one pool are two names for one budget. Full rationale, including why the heavy semaphore never applied to the browse path at all: docs/specs/discovery-budgets.md SS2/SS3.
DISCOVERY_MAX_CONCURRENT_EXPORT_QUERIES=2   # web-only (Phase 136.2). The THIRD concurrency budget, for the findings xlsx export, with its OWN ThreadPoolExecutor sized to it (two semaphores over one pool are two names for one budget). Separate from the heavy cap of 4 because an export holds its slot for a whole-corpus walk, not one query: measured 28,635 rows in 52.9 s on the default view. The export is UNCAPPED IN ROWS by owner decision (2026-08-20) and bounded on every other axis instead.
DISCOVERY_EXPORT_TIMEOUT=300.0   # web-only (Phase 136.2). Whole-build timeout for the export walk. Its honest failure is a 504, NEVER a short file: a truncated workbook is indistinguishable from a small result set once downloaded.
DISCOVERY_EXPORT_EXCERPT_CHUNK=500   # web-only (Phase 136.2). Ids per `IN (...)` batch in the export's excerpt read, replacing the per-identification read (1 query/row would be 28,635 serialized SQLite round trips on a single-uvicorn-worker box). Same fix shape as the citation-range P1 (4f6e31f4, 10,478 ms -> 97 ms).
GENIZAH_PUBLIC_BASE_URL=https://genizahsearch.com   # web-only (Phase 136.2). The canonical origin baked into links inside an exported workbook. Deliberately NOT `request.base_url`: that derives from the client-controlled Host header, so a crafted request would put attacker-chosen origins into every link of a file that otherwise looks like ours and carries our provenance sheet — and a downloaded file outlives the request that made it. A non-http(s) value logs a warning and falls back to RELATIVE links (less convenient, never wrong).
GENIZAH_DISCOVERY_DATA_DIR=/path/to/dir   # dev/CI-only (Phase 136). Overrides the directory web/discovery_assets.py reads the discovery sidecar + manifest.json from (default: repo-root discovery_data/). Read ONCE at import, never per request. Exists because discovery_data/ is gitignored, so the CI `findings-browser-check` job has no sidecar and the findings page would clean-hide: the job materializes the SYNTHETIC fixture sidecar into a temp dir (scripts/ci_materialize_discovery_fixture.py) and points this at it. Widens no trust boundary — it selects WHICH directory is read; the loader still applies its full fail-closed contract (exact manifest asset_basename, content hash, PRAGMA integrity_check, schema_version, `public` audience gate, required tables/columns/meta keys, release-contract row counts, frozen enum vocab). Deliberately NOT used to repoint the repo's real discovery_data/manifest.json, which tests/test_cert01_grading_validator.py resolves the real artifact through. Leave unset in production.
DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT=50   # web-only (Phase 136). Default page size for /computed-identifications, clamped below the page-size max. Read in web/pages/findings.py + shared/discovery_service.py.
IDENTIFICATION_REVIEWS_ENABLED=true   # web-only (2026-08-13, beta). Default ON — gates the community identification-reviews feature (a reader submits a relation verdict on a computed identification; admins moderate before publication). Writes go only through Supabase SECURITY DEFINER RPCs, never a direct table write. Read live in web/identification_reviews.py::reviews_enabled(). Kill-switch: 0/false/no/off. NOT yet a GSD-planned phase — see docs/OPEN_ISSUES.md and the v9.0.0 roadmap note on Phase 137.
MASKING_ATTESTATION_KEY=xxx   # dev/CI-only (discovery-v3 track). HMAC key for the masking scan's keyed pattern-set attestation (scripts/check_atlas_masking.py); the attestation is omitted from output when unset.
V3_REVIEW_M_DIR=/path/to/dir  /  V3_REVIEW_JA_DIR=/path/to/dir   # dev/owner-only (discovery-v3 review tooling). Feed scripts/build_v3_review_db.py + scripts/serve_v3_review.py, the owner's private LOCAL grading server. Never deployed to genizahsearch.com.
DISCOVERY_BROWSE_LRU_MAX_ENTRIES=...   # dev/bench-only. Read by scripts/ benchmarking, not by the web app.
FGP_TRANSCRIPTIONS_ENABLED=true   # shared (both apps): show FGP transcriptions as a distinct, selectable source in the version chooser. Default ON (2026-06-22 go-live) — surfaces wherever the gitignored fgp_data/fgp_transcriptions.db is present; graceful no-op when the DB is absent. Kill-switch: set to 0/false/no/off. Read live in shared/fgp_service.py.
WEB_FGP_ENABLED=...               # optional web-only override of the above (web/feature_flags.py::web_fgp_enabled); defaults to FGP_TRANSCRIPTIONS_ENABLED (ON). Disable on web only with WEB_FGP_ENABLED=0.
PASSAGE_MULTI_WITNESS_ENABLED=false / SEARCH_API_PASSAGE_MAX_WITNESSES=25   # web-only. Multi-witness passage search; ANDed with passage_available(). NEVER concatenate witnesses here -- the posting budget starves (48% vs 74% fused, and worse than ONE witness). Rejected for method='chunk', where union and concatenation measured identical. Contract: docs/SEARCH_API.md.
PASSAGE_PARALLELS_ENABLED=false   # web-only (Phase 145). Gates method='passage' on /api/parallels + the parallels-page method selector. Necessary but NOT sufficient: web/passage_assets.py::passage_available() ANDs it with the passage index's startup-loaded readiness (shared/passage_index.py::open_index is itself fail-closed: manifest, layout/normalizer version, bit budgets, byte order, CSR sanity, declared-vs-actual file sizes). Flag-ON + index-missing/corrupt hides cleanly.
GENIZAH_PASSAGE_DATA_DIR=/path/to/dir   # dev/CI-only. Overrides the dir web/passage_assets.py opens the passage index from (default: repo-root passage_index/current/, gitignored, multi-GB, machine-local). Read ONCE at import.
PUZZLE_UPLOAD_SECRET=xxx (optional - HMAC secret for puzzle upload tokens; auto-generated if unset)
POSTHOG_IP_SALT=xxx (optional - HMAC salt for hashing client IPs; auto-generated if unset, production should set explicitly so hashes survive restarts)

# Search API (Phase 77-83 public HTTP/JSON API over the corpus)
SEARCH_API_MODE=open                  # open | localhost-only | disabled (flippable per request, no restart)
SEARCH_API_RATE_LIMIT=120             # per-IP requests/minute; shared ceiling but each endpoint has its own bucket (raised 30->120 in 2026-06 for API research)
SEARCH_API_POSTHOG_SAMPLE_N=1         # capture every Nth API request to PostHog
SEARCH_API_BROWSE_TIMEOUT=1.0         # per-source enrichment timeout (PGP/FJMS/NLI), seconds
SEARCH_API_BROWSE_CORE_TIMEOUT=2.0    # core BrowsePage fetch timeout, seconds
SEARCH_API_CORE_TIMEOUT=30.0          # interactive baseline (exact/title/shelfmark/responsa); runs in executor off the event loop -> 504 core_timeout
SEARCH_API_VARIANTS_TIMEOUT=60        # /api/search variants-mode core timeout (s); heavy tier
SEARCH_API_FUZZY_TIMEOUT=300          # /api/search fuzzy-mode core timeout (s); heaviest mode
SEARCH_API_PARALLELS_TIMEOUT=300      # /api/parallels composition core timeout (s)
SEARCH_API_HEAVY_CONCURRENCY=2        # max concurrent heavy (variants/fuzzy/parallels method=chunk) requests; over -> 503 heavy_search_busy + Retry-After
SEARCH_API_PASSAGE_TIMEOUT=30         # /api/parallels method='passage' core timeout (s); own ceiling, unrelated to SEARCH_API_PARALLELS_TIMEOUT (Phase 145)
SEARCH_API_PASSAGE_CONCURRENCY=4      # max concurrent method='passage' requests; its OWN semaphore + ThreadPoolExecutor(max_workers=4), never the default executor -> 503 passage_search_busy + Retry-After
SEARCH_API_FUZZY_MAX_LIMIT=500        # fuzzy result-cap ceiling (recall over precision; non-fuzzy stays 100)
SEARCH_API_BROWSE_TEXT_CAP=4000       # default char cap for transcription text; ?text_cap=N override bounded [100, 10000]

# Skill-side (cairo-genizah-research skill consumer)
GENIZAH_API_BASE=https://genizahsearch.com    # overrides --base-url CLI flag (env wins)
GENIZAH_SKILL_REQ_PER_MIN=96                  # skill self-throttle, leaves 24 rpm headroom under server's 120 rpm

# Web memory remediation (2026-07-08 allocator-ratchet attribution; web-only)
GENIZAH_MALLOC_TRIM_SECONDS=300       # periodic glibc malloc_trim(0) loop interval (0 disables; Linux-only, web/malloc_trim.py)
GENIZAH_MALLOC_TRIM_MIN_GROWTH_MB=64  # adaptive: trim only when RssAnon grew this much since the last trim (0 = every tick)
GENIZAH_STORAGE_RETENTION_DAYS=90     # delete .nicegui storage-user files untouched for N days at startup (0 disables;
                                       # a browser absent longer is logged out on return — auth_session lives in the file)
NLI_CACHE_MAX_ENTRIES=20000           # metadata nli_cache LRU bound (code default 75000; prod set 20000 on 2026-07-08)
IIIF_MANIFEST_CACHE_MAX_ENTRIES=1500  # IIIF manifest cache bound (code default 5000; prod set 1500 on 2026-07-08)

# Perf watch (2026-07-30 slowness diagnosis; web-only, web/perf_watch.py). Default ON and
# deliberately quiet — nothing is logged while the app behaves. Added because a 9-second
# response previously left NO server-side trace: nginx uses the default `combined` log format
# (no $request_time/$upstream_response_time) and the only in-app timing was /lists-scoped and
# flag-gated, so origin latency was invisible and had to be inferred from outside.
GENIZAH_PERF_WATCH=1                  # 0/false disables BOTH signals below
GENIZAH_SLOW_REQUEST_MS=1500          # log any http request slower than this (all paths, incl. static)
GENIZAH_LOOP_LAG_MS=300               # log event-loop stalls above this — THE decisive signal: uvicorn
                                       # runs ONE worker, so sync Supabase/NLI I/O on the loop stalls every
                                       # concurrent request incl. static files, while burning no CPU (so it
                                       # is invisible in load average — prod read 0.03 during multi-second TTFBs)
GENIZAH_LOOP_LAG_INTERVAL=1.0         # lag monitor tick, seconds (floor 0.1)
GENIZAH_NOT_SCHEDULED_MS=60000        # (2026-08-19) above this, a tick that burned almost no CPU is
                                       # reported as "monitor NOT SCHEDULED" and kept OUT of max_lag_ms:
                                       # the process stopped running (laptop asleep, container throttled,
                                       # Windows console paused by a QuickEdit selection), it was not
                                       # blocked. A 3,069,031 ms "event loop BLOCKED" reading was a
                                       # sleeping laptop, and it poisoned the all-time maximum that every
                                       # other perf line quotes. Each real stall now also names its KIND
                                       # from the same CPU measurement — GIL-bound Python (a run.io_bound
                                       # worker counts) vs blocking I/O on the loop.
GENIZAH_PERF_SUMMARY_SECONDS=300      # periodic counter summary; 0 disables

# Phase 98 NLI Resilience env knobs (added 2026-05-25)
NLI_CIRCUIT_THRESHOLD=3               # Consecutive failures to trip the shared circuit breaker
NLI_CIRCUIT_WINDOW=60                 # Seconds the breaker stays open before auto-recovery probes
NLI_CONNECT_TIMEOUT=3                 # Connection timeout (seconds) for all NLI/IIIF/Rosetta fetches
NLI_IIIF_READ_TIMEOUT=5               # Read timeout (seconds) for IIIF manifest JSON fetches
NLI_MARC_READ_TIMEOUT=3               # Read timeout (seconds) for MARC bib XML fetches
NLI_IMAGE_READ_TIMEOUT=5              # Read timeout (seconds) for NLI image-bytes fetches

# Existing NLI knob — Phase 98 changed default from 20 -> 1:
NLI_SEMAPHORE_TIMEOUT=1               # Max seconds to wait for a slot in the 8-slot NLI semaphore
                                       # (was 20 pre-Phase-98; waiting >1s burns threadpool workers)

# (Other existing NLI knobs unchanged: NLI_CACHE_TTL, NLI_FAIL_CACHE_TTL, NLI_MAX_CONCURRENT_FETCHES)
```

**Operational note (Phase 98 — two PostHog drop counters):** Phase 98 ships with TWO PostHog
queues — `web/api_hardening.py` keeps its existing queue (for `search_api_request` events) and
the new `shared/posthog_server.py` queue handles breaker telemetry (`nli_breaker_opened` /
`nli_breaker_closed`). At deploy time, monitor BOTH `web.api_hardening.get_dropped_event_count()`
AND `shared.posthog_server.get_dropped_event_count()` — growth in EITHER signals queue saturation.
The two-queue split is intentional (REVIEWS Issue 5 Option A): refactoring `web/api_hardening`'s
queue would break 5 existing test monkeypatches that target `web.api_hardening._event_queue`
directly. A future cleanup plan can unify.

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
- **Web is not continuous-deploy.** Deploy DBs/assets first (`scp`), then push code.
- **Still open:** the `Set-Cookie`-on-every-response Cloudflare BYPASS for ordinary page requests —
  see `docs/OPEN_ISSUES.md`. Partially mitigated 2026-08-13 (`ab064ff9`): static/immutable assets now
  bypass the session-cookie middleware and are cacheable; page HTML, APIs and auth are unchanged.
- **The discovery beta is LIVE in production** (`DISCOVERY_ENABLED=1` since 2026-08-08) — but it was
  flipped on ahead of the REL-01 gate the roadmap defines, and the planning record was never updated
  to say so. Treat `.planning/STATE.md`'s "the flag must NOT be flipped yet" as historical.

### Releases

- **v8.6.0 — Pause/Resume, and Stop that actually stops (2026-08-20)** — desktop Pause/Resume for regular, Lab and Composition searches (parks the worker at a checkpoint; Resume continues at the same index, paused time excluded from timings); Stop repaired in Lab Mode (three causes, incl. a bare `except Exception` eating `InterruptedError`, an `OSError` subclass) and stopped runs now keep partial results in Title/Shelfmark/My Library; monotonic elapsed timing; search toolbar no longer overlaps itself (row 2 needed 1423 px on a 1440 px screen). **Desktop-only — the desktop line continues from v8.5.2, since 9.0.0 was web-only and never tagged.** (desktop)
- **v9.0.0 — Computed Identifications, the Visual Atlas & Start Here (public beta, 2026-08-16)** — `DISCOVERY_ENABLED` flipped ON in production 2026-08-08; `/computed-identifications`, the browse connections panel and the `/help` methods section are publicly reachable. Then a fast post-launch run: catalogue-divergent rows un-hidden (3,570, 12.5% of the main pool), a frozen relation-precedence matrix, a "View text match" excerpt view, a `/start` guided launchpad, homepage discovery entry points, and beta community identification reviews. **Web-only — no GitHub Release object, desktop unchanged.** One REL-01 gate item (the cross-surface masking re-sweep) is still open; see ROADMAP.md. (web)
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
- **SEED-006 — Hebrew search retrieval fix (2026-06-21)** — punctuation- and diacritic-attached words were unretrievable under the `whitespace` tokenizer; two-stage tokenizer + additive field fix. (both)
- **v8.1.0 — Desktop Telemetry & API Enhancements (2026-06-16)** — opt-in anonymous telemetry through the existing PostHog queue (no SDK, PII risk); bilingual "Public API & AI Tools" Help section. (both)
- **Web (2026-06-16)** — P9X per-mode timeout tiering, heavy-mode concurrency cap, fuzzy result-cap raise. (web)
- **Web (2026-06-15)** — Search API rate limit 30→120 rpm, fuzzy mode exposed, `SEARCH_API_CORE_TIMEOUT`, CLS/banner fixes. (web)
- **Web hotfix (2026-06-12)** — Lab deep-scan crash (the core's dual-protocol progress callback) + four nested-link click-propagation fixes; server-side `stop_propagation()` can never work. (web)
- **v8.0.0 — Dicta Genizah Search Pro: Joins Lab & enhanced Local Library (2026-06-09)** — first release under the new desktop display name (display-only rebrand); Joins Lab + Composition Search over My Library. (both)
- **v7.16.0 search-freeze ROOT CAUSE (2026-05-31)** — `search_history.json` had grown to 778 MB and was loaded + rewritten on the UI thread every search; history no longer stores result snapshots. (desktop)
- **v7.16.0 — Hebrew PDF Text Quality (2026-05-31)** — LOCAL PDF extractor overhaul + file-aware actions for My Library hits. (desktop)
- **v7.16.0 LOCAL UAT fixes (2026-05-31)** — HTML/XLSX/CSV extraction quality, opt-out folder cascade, per-folder checkboxes. (desktop)
- **LOCAL PDF de-space follow-ups (2026-05-31)** — zero-width space-glyph boundary signal + LTR-run bidi fix so years stop reversing. (desktop)
- **LOCAL PDF de-space rewrite (2026-05-31)** — edge-gap metric + Unicode `Mn` + per-line Otsu valley; `extraction_format_version` 2→3. (desktop)
- **v7.15.0 — PDF Page Image in My Library (2026-05-28)** — PDF page rendered alongside extracted text for LOCAL hits; "Re-index All" button. (both)
- **My Library recovery-modal recurrence fixed (2026-05-27)** — orphaned `running` scan rows re-triggered the dialog every launch; the clean-shutdown sweep was dead code in a child widget's `closeEvent`. (desktop)
- **My Library post-UAT Codex follow-up (2026-05-27)** — LOCAL LAB rebuild is now a true replace, not an append. (desktop)
- **Phase 97.3 — My Library UAT Stability (2026-05-26)** — workerized tree population, six UAT defects. (desktop)
- **Phase 97.2 — Recovery Cascade (2026-05-26)** — closeout of the 8-bug startup cascade + Reset My Library. (desktop)
- **Phase 98 — NLI Resilience (2026-05-25)** — shared circuit breaker wired into all 10 NLI fetch sites; worst-case per-request blocking 45s → ~9s. (web)
- **Phase 96 — My Library Polish (2026-05-24)** — unified file tree, opt-out persistence race, 10 stale skip markers converted to assertions. (desktop)
- **v7.14.0 — My Library: Local Document Search (2026-05-24)** — desktop 7th tab indexing user folders into a side-index, merged via RRF k=60 *after* dedup. (both)
- **v7.14 Phase 95 — My Library CLOSED (2026-05-21)** — first-class desktop indexer for .docx/.pdf/.txt. (desktop)
- **v7.13.0 — Research-Grade Export & Polish (2026-05-21)** — 4-sheet bilingual research-grade xlsx exports with clickable image links. (both)
- **v7.13 Phase 94.1 — per-folio Image URL column (2026-05-21)** — proxy URLs instead of runtime IIIF fetches. (both)
- **v7.13 Phase 94 — Research-Grade Export Metadata CLOSED (2026-05-21)** — shared `shared/export_dossier.py` primitives, web+desktop parity. (both)
- **v7.13 Phase 93 — PGP Filter on `/search` (2026-05-19)** — 3-state post-search filter via the `safe_storage` chokepoint. (web)
- **v7.12.0 (2026-05-18)** — Multitenant Safety and Line Numbering; cross-user state leak made structurally impossible via the `web/safe_storage.py` chokepoint. (both)
- **v7.11.2 (2026-05)** — Desktop-only Composition Search bug fixes: Min chunks filter dedup, expanded view auto-scroll to first highlight. (desktop)
- **v7.11.0 (2026-05)** — CUDL Coverage & Synthetic Inventories. FIST↔CUDL shelfmark bridge, 108 image-bearing synthetics. (both)
- **v7.10.0 (2026-05)** — Search API Public Release. `POST /api/search`, `GET /api/browse`, `POST /api/parallels`. Contract: `docs/SEARCH_API.md`. (web)
- **v7.9.4 (2026-05)** — NLI Library Code Fix. 461 manuscripts flipped Oxford → NLI. (both)
- **v7.9.3 (2026-04)** — Visual Similarity Dialog fixes (Firefox scroll, modifier-click, copy-paste). (web)
- **v7.9.2 (2026-04)** — PGP Data Refresh. (both)
- **v7.9.1 (2026-04)** — Catalog Attribution & Reading Desk Polish. (both)
- **v7.9.0 (2026-04)** — v7.8 Structural Foundation + v7.9 Decomposition. (both)
- **v7.7.x (2026-04)** — PageSpeed + SEO Round 2 + Volume-Aware Browse. (web)
