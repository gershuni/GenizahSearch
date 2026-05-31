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

- standalone `backend server` / `genizah-backend` service - Removed in Jan 2026 (NOTE: FastAPI the framework is still live — it serves `/api/*`; only the separate backend process was removed)
- `DATABASE_URL` - Replaced by `SUPABASE_URL`
- `port 8000` - Backend port no longer used

## Recently Changed

For full release history see `CHANGELOG.md`. Most recent:

- **LOCAL PDF de-space follow-ups — space-glyph boundary + number bidi (2026-05-31)** — internal extractor quality fix on the v7.15 chain, no version bump; `extraction_format_version` stays 3 (the same "Re-index All" as the rewrite below carries these). Closes 3 UAT defects (OPEN_ISSUES D-F13d) on top of the edge-gap/Otsu rewrite. **N1 (structured-line over-merge):** tightly-set headings/citations/abbreviation-tables/bibliographies encode the inter-word space as a **zero-WIDTH space glyph** (edge gap ≈ 0.02 em — identical to intra-word, so the Otsu gap test can't see it; the "Otsu outlier" hypothesis was probe-DISPROVED). Re-introduced the space glyph as a SECONDARY boundary signal in `despace_line_to_word_units`, gated LOCALLY so letter-spacing (a space between EVERY letter, e.g. Otzar `ה כ ו כ ב י ם`) is NOT mistaken for a word-space — a space forces a boundary only when neither immediately-adjacent inter-base position also carries a space (`_is_space_glyph`; kill-switch `_SPACE_BOUNDARY_ENABLED`). Purely additive (a line with no space glyphs splits exactly as before). A/B identical pages: `פרנץ רוזנצווייגושמואלהוגוברגמן`→`פרנץ רוזנצווייג ושמואל הוגו ברגמן`; Du-Siach merge 0.13%→0.04%, Hakdamot 0.14%→0.00%, **zero shatter added** (Otzar 5.37%→5.36%, Igrot 0.21%→0.20%). **N2 (maqaf):** `דושיח`/`ובעתובעונה` were the OLD format-v2 output — the rewrite's `Mn` fix already preserves `דו־שיח`/`ובעת־ובעונה`/`ארץ־ישראל`/`לב־לבה`/`על־ידו`. **N3 (digit reversal):** `1977`→`7791` — `_order_unit_text_rtl` ordered ALL bases descending-x (RTL), reversing inherently-LTR digit runs; the F-A re-reversal only fixed order BETWEEN digit units, never WITHIN one. Added the standard bidi "reverse embedded level run" step (`_is_ltr_base`): flip maximal LTR runs (digits/Latin/numeric separators) back to ascending. A/B Du-Siach years: `3191,5191,6191`→`1913,1915,1916`. Tests: `test_zero_width_space_glyph_forces_boundary`, `test_letter_spaced_run_spaces_suppressed`, `test_order_unit_keeps_embedded_ltr_run_ascending`, `test_year_in_rtl_line_not_reversed` + real fixtures `real_dusiach_packed_names.json` (N1) / `real_dusiach_year.json` (N3). 132 local-PDF tests pass, ruff clean. (desktop)
- **LOCAL PDF de-space rewrite — edge-gap + Mn + adaptive + guard fix (2026-05-31)** — internal extractor quality fix on the v7.15 chain, no version bump. Rewrote `despace_line_to_word_units` in `shared/local_indexer_rtl.py`: (1) word-boundary metric switched from center-x→center-x vs 1.8×per-line-median to **edge-to-edge whitespace** (`next.x0 − prev.x1`) / font_size with floor `_WORD_GAP_FONT_FRACTION = 0.45` (the corpus is cleanly bimodal in edge-gap/em space, valley ~0.45 across 90 books / 300K+ gaps; center-distance conflated letter width with spacing and shattered wide letters מ/ש/ה off justified words — `פירוש המשנה`→`פירו ש ה מ ש נה`); (2) combining-mark test switched from range `0x05B0–0x05C7` to Unicode category `Mn` (`_is_nikud`) — the range mis-treated maqaf `־`/sof-pasuq as vowels (corrupted `סב־סג`) and missed te'amim; (3) **per-line 1-D Otsu valley** (`_word_gap_fraction`, clipped + bounded) instead of a fixed fraction — word-spaces are ~0.3×em in tightly-set modern books (`רביצקי`), ~0.7×em normal, vs letter-spaced heading tracking ~0.5×em (they overlap, so no global fraction works), but per line the intra/inter clusters are always bimodal so Otsu finds each line's valley; (4) **dropped the embedded-space-glyph boundary signal** (justified Hebrew emits U+0020 between every letter). **The real production blocker was `_ltr_damage_guard` in `shared/local_indexer.py`** — its token-count check ran on RTL pages, so the better (fewer-token) de-space output was discarded for the shattered blocks fallback; gated count/Jaccard to LTR pages only. **Measured (full pipeline, identical pages):** אוצר הגאונים ברכות 73.5%→3.0% single-letter tokens, רביצקי word-merge 15.8%→0.07% (mean token len 12.3→4.5), איגרות הרמב״ם 5.2%→0.17%. `extraction_format_version` bumped 2→3 — **existing LOCAL libraries need a manual "Re-index All"** to benefit (no auto-flip — bulk re-extraction must stay on the background worker, never `__init__`/UI thread). Real-PDF regression fixtures (`real_otzar_heading` shatter + `real_ravitzky_tight` merge guards) + Otsu unit tests + `TestLtrDamageGuardRtlTrust` added; targeted local-PDF suites pass, ruff clean. **Also fixed (D-F13c):** app-launch freeze — `startup_recovery` Pass B re-extracted a bulk `pending` backlog (left by an interrupted "Re-index All") synchronously on the UI thread; now `startup_recovery(reextract_pending=False)` on the desktop path defers it to the `_auto_rescan_on_startup` background worker. (desktop)
- **v7.15.0 — PDF Page Image in My Library (2026-05-28)** — public release of the v7.15 milestone (3 phases: 99 PDF Page Renderer, 100 LOCAL PDF Image in ResultDialog + Browse, 101 RTL fix + remnant cleanup + UAT follow-ons). **New:** PDF page image rendered alongside extracted text in both desktop ResultDialog and Browse panel for LOCAL PDF hits; bounded-LRU + lazy + on-demand (no on-disk image cache); non-PDF LOCAL files stay text-only. **New:** "Re-index All" / "אנדקס מחדש הכל" button in My Library tab — flips committed rows to pending and triggers re-extraction via the background worker (recovers existing libraries after extractor improvements). **Improved:** Hebrew PDF text quality — S-1 directional-run RTL word-order reversal in `_fix_sort_true_rtl_line` + intra-block newline collapse in `_collapse_intra_block_newlines` (bidi-fragmented Hebrew paragraphs now join back into continuous prose). **Fixed:** remove-folder Windows ERROR_ACCESS_DENIED storm (`remove_folder` batches to single retry-protected commit); LAB rebuild 5-failure bail + pre-flight callback probe (silenced ~1.9M-row warning + 10s freeze); remove-folder confirm dialog now actually translates to Hebrew; graceful PDF render failures (placeholder + log, no UI hang). **Rolled back post-UAT:** Phase 101 D-04 extractor-version auto-self-heal-on-launch froze 12K-PDF library at launch (synchronous `startup_recovery()` Pass B on UI thread) — code change only, no auto-flip; manual recovery via Re-index All. 6/6 PDFIMG-* requirements satisfied. New deferred: D-F12 (regular Search ~constant 8s wall-clock) for v7.16+. (both)
- **My Library recovery-modal recurrence FIXED + UAT-verified (2026-05-27, commits `1859b8ac` + `528906e4`)** — the desktop "התאוששות מאינדוקס שהופסק" / "Recover interrupted indexing" dialog popped up every launch and couldn't be dismissed for good. Two causes: (1) `_show_recovery_modal` resolved only `running_runs[0]` though `start_recovery_probe()` returns ALL `status='running'` rows — leftover orphans re-triggered it; now every branch (Restart/Skip/Resume) loops over all running runs. (2) The LD-6 clean-shutdown sweep (`running→completed`) lived in `MyLibraryTab.closeEvent`, but a child widget never receives `closeEvent` on app exit, so it was dead code and orphans accumulated across hard kills (e.g. the 2026-05-25 NLI-hang SIGKILL); extracted to `sweep_running_scan_runs()` and wired into `GenizahGUI.closeEvent`. Live UAT confirmed: hard-kill orphan → Skip marked it `canceled`, a live run flipped `running→completed` on clean X-close, 0 orphans remain. `528906e4` also adds a LAB rebuild abort-on-empty-source guard (Codex `fb5cbdb8` review MEDIUM): if the main LOCAL source can't be read, the queued `delete_all_documents()` would otherwise publish an empty index marked "fresh" — now it rolls back and leaves the prior index + `.meta.json` intact. New tests: `tests/test_recovery_scan_runs_cleanup.py` + strengthened `tests/test_local_lab_invalidation.py`. (desktop)
- **My Library post-UAT Codex review follow-up (2026-05-27, commit `fb5cbdb8`)** — fixes from the Codex review of `e397ad30`. LOCAL LAB rebuild is now a true replace: `build_lab_side_index` calls `writer.delete_all_documents()` before re-adding rows, so a rebuild after a weight change / page deletion no longer appends duplicates or leaves deleted pages searchable (Finding 1 HIGH; new `tests/test_local_lab_invalidation.py::test_lab_rebuild_is_replace_not_append` guards it — the prior tests only checked `.meta.json` + SQLite `local_pages`, never the LAB Tantivy doc count). Desktop double-click `show_full_text()` now reads the clicked row's own result dict and delegates to `show_full_text_for_result()` instead of indexing `_collect_sorted_results()` by `currentRow()` (which drops hidden/filtered rows) — consistent with the 👁-button uid-locator fixed in `e397ad30` (Finding 3 MED). Deferred (logged in `docs/OPEN_ISSUES.md` per user — "LAB barely used"): standard Composition Search omits LOCAL LAB hits because `SearchEngine`'s weights-hash differs from the `LabEngine` hash the rebuild writes (Finding 2 HIGH; pre-existing, never ran in production, LAB-mode Composition unaffected). (desktop)
- **Phase 97.3 — My Library UAT Stability (2026-05-26)** — internal hotfix on the v7.14 chain (97.1 → 97.2 → 97.3), no version bump. Closes six post-Phase-97.2 UAT defects in the desktop My Library tab: R97.3-A workerized tree population via `FolderWalkWorker` (4-tuple signal + monotonic generation token + `_SUPPORTED_EXTENSIONS` pre-filter — closes UI-thread freeze on mega folders + non-following directory traversal via `os.walk(followlinks=False)` + 100ms responsiveness target via `time.perf_counter` + `QTimer.singleShot(0)` + `QApplication.processEvents`); R97.3-A `prior_status` in-memory cache on `MyLibraryTab` (D-12 — invalidated BEFORE `_refresh_folder_list_ui` at 5 lifecycle sites per Codex Critique #2 v7.14-blocker); R97.3-B Reset-button guard simplified to `worker_running` only (orphan `scan_runs.running` rows no longer block Reset — Phase 97.2's `reset_my_library` 7-step protocol remains the load-bearing safety); R97.3-C `fitz.TOOLS.mupdf_display_warnings(False)` at `shared/local_indexer.py` module import wrapped in `try/except Exception` (silences 624× stderr noise; future PyMuPDF API change degrades to debug log, not import crash); R97.3-D one-shot `_skip_startup_rescan_once` flag suppresses same-launch auto-rescan on Skip click (D-25 default unchanged on no-modal path); R97.3-E new `status_updated(str)` signal on `LocalIndexerWorker` + indeterminate progress bar `setRange(0, 0)` during enumeration → determinate `(0, 100)` on first indexing progress + reset to `(0, 100)` on finish/cancel/error; R97.3-N old UI-side `SUPPORTED = {'.pdf', '.docx', '.txt'}` literal deleted (R97.3-N — `.html`/`.xlsx`/`.csv` now appear in opt-out tree; case-insensitive). Two Codex critiques pre-plan (`97.3-CODEX-CRITIQUE.md` Area 1 + `97.3-CODEX-CRITIQUE-2.md` full decision-set — surfaced D-11 broad-exception, D-12 cache ordering, D-16..D-22 + inverted wave order for risk locality). Phase 97.2 invariants + Phase 96 D-F1 opt-out persistence + Phase 87 multitenant allowlist `[]` all preserved. (desktop)
- **Phase 97.2 — Recovery Cascade (2026-05-26)** — closeout of the 8-bug startup cascade + Reset My Library. See CHANGELOG.md and `.planning/phases/97.2-recovery-cascade-lockbusy/` for details. (desktop)
- **Phase 98 — NLI Resilience (2026-05-25)** — internal infrastructure milestone, no user-visible change. Shared module `shared/nli_circuit_breaker.py` with module-level singleton (`time.monotonic`, `threading.Lock`) replaces the buggy class-attribute breaker that used `time.time` and was scoped to `MetadataManager` only. Wired into all 10 NLI fetch sites: 4 in `web/api.py` (`fetch_fl_ids_from_nli`, `nli_image`, `_fetch_nli_image_bytes`, `proxy_image` — D-host-conditional), 3 in puzzle (`PuzzleImageService._fetch_iiif_image`, `_fetch_direct_url` host-conditional, `web/pages/puzzle.py::_resolve_folios`), 4 in `genizah_core.py` (`fetch_iiif_manifest`, `fetch_marc_data` migrated + new wirings at `_fetch_single_worker`, `_fetch_fl_ids`); legacy class-attribute breaker REMOVED (RESEARCH Pitfall 5). 6 new env knobs (`NLI_CIRCUIT_THRESHOLD` / `NLI_CIRCUIT_WINDOW` / `NLI_CONNECT_TIMEOUT` / `NLI_IIIF_READ_TIMEOUT` / `NLI_MARC_READ_TIMEOUT` / `NLI_IMAGE_READ_TIMEOUT` — see "Environment Variables" section for defaults); `NLI_SEMAPHORE_TIMEOUT` default dropped 20→1 (D-10). PostHog telemetry on breaker open/close via factored `shared/posthog_server.py` queue+daemon (Option (a) — `shared/` no longer depends on `web/`). Worst-case per-request blocking budget dropped from 45s to ~9s; after 3 consecutive failures the breaker trips for 60s and subsequent NLI fetches return empty in microseconds. Codex REVIEWS Issue 3 closed via fallback-boundary rechecks (MARC, Rosetta, FL-ID iteration, retry loop). Origin: 2026-05-25 production hang where Starlette threadpool saturated on synchronous `requests.get(timeout=15)` calls to `iiif.nli.org.il`; SIGTERM hung 90s and SIGKILL required. Closes `docs/INCIDENT-2026-05-25-nli-iiif-hang.md` per `docs/INCIDENT-2026-05-25-CODEX-CRITIQUE.md` Minimum Ship Patch. Deferred: async refactor to httpx, event-loop watchdog, multi-worker uvicorn (CONTEXT D-05). Verification: `curl -w "%{time_total}\n" https://genizahsearch.com/api/fl_ids/990001458630205171` 10× in sequence — first 1-3 slow, rest <0.1s. (web)
- **Phase 96 — My Library Polish (2026-05-24)** — internal closeout of Phase 96 (9 plans). D-F1/D-F4/D-F5 closed. UAT bugs fixed: unified `_UnifiedFileTreeWidget` (3-column tree replaces QSplitter+separate table), opt-out persistence race fixed (`flush_pending()` in `closeEvent`), Enter-key focus fixed (`autoDefault=False` + `setFocus` on spin_page), Browse opens at correct page (p_num str→int coercion), LOCAL nav widgets hidden on Genizah manuscript load. BLOCKER-5: 10 stale `pytest.skip` markers converted to positive assertions. D-F2 (OCR) + D-F3 (side-by-side PDF) remain open for v7.15+. (desktop)
- **v7.14.0 — My Library: Local Document Search (2026-05-24)** — public release of v7.14 milestone (Phase 95). Desktop-only 7th tab indexes user folders of `.docx` / `.pdf` / `.txt` into a separate Tantivy side-index merged into Search/Composition/Parallels via RRF k=60 *after* `_deduplicate()` (Codex D-08 P0). New pre-search corpus dropdown `Genizah` / `Local` / `ALL` (default `Genizah`); existing 3-state `Filter Local` button cycles post-search. Cloud-write gates pinned at TOP of `shared/search_serializer.py`, `corrections_client.py`, `lists_sync.{sync_item_to_cloud, sync_list_to_cloud}` (Codex D-30 P0). PyMuPDF added as desktop dep + `collect_all('pymupdf')` + `--self-test-pymupdf` CLI. Per-thread SQLite via `threading.local()`; Tantivy commit retry on Windows `os error 5`. LOCAL Library column = `parent/folder`, Shelfmark = filename. About + Help bilingual (EN + HE) with Seewald attribution (HE: יהודה זייבלד). 2532 tests passing. 5 items deferred to v7.15+ in OPEN_ISSUES.md (D-F1..D-F5). (both)
- **v7.14 Phase 95 — My Library CLOSED (2026-05-21)** — internal milestone. First-class desktop indexer for .docx/.pdf/.txt. New `MyLibraryTab` (7th tab), side-index merged via RRF k=60 (Codex D-08 P0 — POST-dedup merge), three-state LOCAL filter mirroring Phase 93 PGP pattern, three cloud-write regression tests (lists_sync gate at TOP of `sync_item_to_cloud` per Codex D-30 P0). PyMuPDF dep + `GenizahSearchPro.spec` `collect_all('pymupdf')` + `--self-test-pymupdf` headless CLI flag. `shared/export_dossier.py` `skip_local` kwarg (web excludes LOCAL, desktop includes LOCAL). Static AST guard `tests/test_web_library_options_no_local.py` pins LIBRARY_CODES web invariant. Help + About updated EN + HE with D-33 cleartext disclosure + D-32 Seewald attribution. 10/10 REQ-IDs satisfied. (desktop)
- **v7.13.0 — Research-Grade Export & Polish (2026-05-21)** — first user-facing release of the v7.13 milestone. Bundles Phase 93 (PGP filter on web search), Phase 94 + 94.1 (4-sheet bilingual research-grade xlsx exports with clickable image links + JSON `has_pgp`/`is_printed`/`domains` keys), SEO Round 3 (homepage About + FAQ, manuscript JSON-LD, favicon 401 KB → 9 KB), Help page additions (4-sheet export docs + new "Public API & AI Tools" section), and a desktop sync-merge bug fix (`lists_sync.py` now matches cloud items by `cloud_id` first, then `(sys_id, fl_id)`, so items keyed locally by `img` no longer duplicate; pre-existing duplicates merge automatically). What's New banner texts (web + desktop bar/dialog) refreshed; line-numbering catch-up included since v7.12 banner was never updated. (both)
- **v7.13 Phase 94.1 — D-13 lifted, per-folio Image URL column (2026-05-21)** — internal post-closeout patch on top of Phase 94. Renamed main-sheet column 12 from `IIIF Manifest` → `Image URL` (EN) / `מניפסט IIIF` → `כתובת תמונה` (HE) and populated cells with per-folio proxy URLs routed through GenizahSearch's existing image-proxy endpoints (`https://genizahsearch.com/api/oxford_image/{sys_id}?page={N}` for Oxford, `…/nli_image_by_sysid/{sys_id}?page={N}` otherwise; synthetic sys_ids emit empty cell). Cells are clickable openpyxl hyperlinks. Helper: `shared/export_dossier.build_image_url_for_row` + `apply_main_row_image_url_hyperlink`. Trade-off chosen over runtime IIIF manifest fetches: proxy URLs require the web app to be reachable but cost zero network at export time. (both)
- **v7.13 Phase 94 — Research-Grade Export Metadata CLOSED (2026-05-21)** — internal milestone close, not a release. 4-wave implementation (94-01 shared dossier primitives → 94-02 web state plumbing + JSON envelope → 94-03 web xlsx restructure → 94-04 desktop xlsx parity) + 6 rounds of smoke-verification UX patches Hillel approved same day. Final workbook: 4-sheet bilingual xlsx (`Search Results` + `Manuscripts` + `Bibliography` + `Credits and Info`) on both web and desktop via shared `shared/export_dossier.py` helpers. Web JSON gains 3 additive per-item flags (`has_pgp` / `is_printed` / `domains`) with envelope `schema_version` unchanged. CONTEXT D-04 REVERSED 2026-05-20 for the row-content layer only. Cross-parity invariant pinned by `tests/test_export_xlsx_cross_parity.py`. All 9 EXPORT-META-01..09 reqs satisfied. v7.13 milestone now closeable (Phase 93 already complete 2026-05-19; Phase 94 complete 2026-05-21; 14/14 requirements). 2316 passed / 20 skipped / 2 xfailed; ruff clean; Phase 87 multitenant invariant unaffected (allowlist `[]`). (both)
- **v7.13 Phase 93 — PGP Filter on `/search` (2026-05-19)** — internal milestone, not a release. Web-only post-search 3-state filter button (`Filter PGP` / `Has PGP` / `No PGP`) mirroring `printed_filter` pattern, persisted via `web/safe_storage.py` chokepoint (Phase 87 invariant). 4/5 PGP-FILTER reqs directly satisfied; PGP-FILTER-03 (chip) superseded by user smoke direction. Static AST guard `tests/test_pgp_filter_cascade.py` installed. (web)
- **v7.12.0 (2026-05-18)** — Multitenant Safety and Line Numbering. Bundles v7.12 Path B Multitenant Architecture milestone (Phases 87-92 + sub-phases 92.1/92.2 — web-only refactor making cross-user state leak structurally impossible via `web/safe_storage.py` chokepoint), folio chip on search result cards, line-number gutter in transcription views (web + desktop), P1 export memory-leak hotfix (RSS 7.5 GB → 1.78 GB via `_EXPORT_RESULTS_CAP = 5000` in `web/export_state.py`). Architecture: `docs/guides/MULTITENANT.md`. CI guard: `tests/test_no_raw_storage_access.py` (allowlist `[]`). (both)
- **v7.11.2 (2026-05)** — Desktop-only Composition Search bug fixes: Min chunks filter dedup, expanded view auto-scroll to first highlight. (desktop)
- **v7.11.0 (2026-05)** — CUDL Coverage & Synthetic Inventories. FIST↔CUDL shelfmark bridge, 108 image-bearing synthetics. Deploy posture codified: scp DBs FIRST, then push code. (both)
- **v7.10.0 (2026-05)** — Search API Public Release. `POST /api/search`, `GET /api/browse`, `POST /api/parallels`. OpenAPI at `/api/openapi.json`. Skill `cairo-genizah-research`. Contract: `docs/SEARCH_API.md`. (web)
- **v7.9.4 (2026-05)** — NLI Library Code Fix. 461 manuscripts flipped Oxford → NLI. (both)
- **v7.9.3 (2026-04)** — Visual Similarity Dialog fixes (Firefox scroll, modifier-click, copy-paste). (web)
- **v7.9.2 (2026-04)** — PGP Data Refresh. (both)
- **v7.9.1 (2026-04)** — Catalog Attribution & Reading Desk Polish. (both)
- **v7.9.0 (2026-04)** — v7.8 Structural Foundation + v7.9 Decomposition. (both)
- **v7.7.x (2026-04)** — PageSpeed + SEO Round 2 + Volume-Aware Browse. (web)
