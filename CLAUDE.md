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
WEB_PUZZLE_ENABLED=true (default: true; set to false to disable web puzzle page)
PUZZLE_UPLOAD_SECRET=xxx (optional - HMAC secret for puzzle upload tokens; auto-generated if unset)
SEARCH_API_MODE=open (one of: open | localhost-only | disabled; default: open; flippable per request without restart; applies to /api/search, /api/browse, /api/parallels)
SEARCH_API_RATE_LIMIT=30 (per-IP requests per minute; default: 30; SHARED ceiling across /api/search, /api/browse, /api/parallels but each endpoint has its own independent bucket — see Phase 80 D-05)
POSTHOG_IP_SALT=xxx (optional - HMAC salt for hashing client IPs in server-side PostHog events; auto-generated if unset, but production should set explicitly so hashes survive restarts)
SEARCH_API_POSTHOG_SAMPLE_N=1 (optional - capture every Nth API request to PostHog; default: 1 = every request; applies to /api/search, /api/browse, /api/parallels)
SEARCH_API_BROWSE_TIMEOUT=1.0 (per-source enrichment timeout for /api/browse PGP/FJMS/NLI fetches in seconds; default: 1.0)
SEARCH_API_BROWSE_CORE_TIMEOUT=2.0 (core BrowsePage fetch timeout for /api/browse in seconds; default: 2.0; previously no core timeout existed — added per Phase 79 R-01 to prevent executor pinning on a hung Tantivy reader)
SEARCH_API_BROWSE_TEXT_CAP=4000 (default char cap for transcription text in /api/browse; per-request override via ?text_cap=N bounded by [100, 10000]; default: 4000)
GENIZAH_API_BASE=https://genizahsearch.com (skill-side only; cairo-genizah-research skill consumer base URL; overrides --base-url CLI flag per skill D-09 env-wins; consumed by skills/cairo-genizah-research/scripts/_config.py)
GENIZAH_SKILL_REQ_PER_MIN=24 (skill-side only; cairo-genizah-research skill self-throttle ceiling per endpoint bucket, default 24 req/min leaving 6 rpm headroom under server's 30 rpm SEARCH_API_RATE_LIMIT; SKILL-06)
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

- May 2026: v7.11.0 release -- CUDL Coverage & Synthetic Inventories. 3-phase milestone (Phases 84, 85, 86) closing the gap between CUDL's ~141K classmark catalogue and GenizahSearch's libraries.csv. Originating user case: `T-S NS 329.96` (sys_id `990065549106000000`) — present in CUDL with 2 image canvases, missing from libraries.csv because its FJMS inventory has no NLI Alma record. **Phase 84** (CUDL Shelfmark Normalization): new `shared/fist_cudl_bridge.py` + `shared/shelfmark_bridge.py` bridge modules with FIST↔CUDL normalizers (Mosseri label `Moss. III,27O` ↔ `mosseriiii27o`, Cambridge Or. numeric collapse, leading-zero collision audit, slash/comma/dot bug fixes); 6 bridge wiring call sites across `genizah_core.py`, `web/services.py`, `web/pages/browse_enrichment.py:208`, image-source resolution, CUDL link builder, orphan-scanner unification — recovers thousands of CUDL classmarks already represented in libraries.csv under different forms. 3-layer regression guard: `cudl_must_resolve` fixture, `cudl_baseline_resolved` snapshot, unit tests. **Phase 85** (Synthetic Infrastructure): helper module `shared/synthetic_sys_id.py` (`is_synthetic_sys_id`, `encode_inventory_sys_id`, `decode_inventory_id`), FJMS sidecar UNION-ALL export pattern, browse hide-NLI gates (KTIV link, NLI source toggle, NLI bibliography chips, NLI image source all hidden when synthetic), `is_synthetic` field on /api/search + /api/browse + /api/parallels response items + PostHog event property, corrections-write reject at `CorrectionsClient.create_correction` + `SupabaseCorrectionsClient.create_correction`. Initial Phase 85 POPULATION (5,035 bibliography-only rows) reverted by `3c75a9bc` during 2026-05-09 UAT — infrastructure stays active, produces zero rows until Phase 86 re-attempts with image-bearing criteria. **Phase 86** (CUDL Coverage Audit + Synthetic Re-attempt): **108 image-bearing synthetic manuscripts** including the originating case T-S NS 329.96 — distribution 101 CUL + 7 Mosseri, all with CUDL canvas images via the bridge. Surgical DB injection (`scripts/phase86_inject_synthetic_to_main_db.py`): 3,264 catalog rows + 103 FTS5 docs across 11 base tables in `fjms_enrichment.db`, leaving 7 supplemental tables untouched (translations, measurements, blank_images, extra_info, computed_measurements, import_meta, fjms_translations); `catalog_sizes` skipped per Codex review for schema drift. Backup via `.backup()` API + full gzip CRC retained. AUDIT-02 5-tier coverage report: phase84_hit 96.23%, phase86_existing_alma_candidate 2.39%, phase86_synthetic 0.08%, phase86_residue 1.13%, multi_inventory_ambiguous 0.18%. 6 residue pattern families analyzed and REJECTED (need human-in-loop, not new auto-rules). NLI attribution regression guard via 461-row golden fixture confirms v7.9.4 Oxford→NLI flip not regressed. Browse pagination fixes for synthetic sys_ids (web + desktop): `_get_metadata_only_browse_page` accepts `p_num`/`absolute_index`/`next_prev` and produces moving target page; `browse_render_page` derives `total` from largest of folio_images/images_ext/images_nli when `metadata_only` and `total_pages==0`; bypasses Tantivy for synthetic via `is_synthetic_sys_id` short-circuit; tolerates NiceGUI slot-lifecycle race. Plus 2 unrelated search UX fixes: duplicate top-toolbar "Exclude manuscripts" button removed; Text Position dropdown resets on New Search + shows active-state chip when not default. **Deploy posture codified**: scp DBs FIRST, then push code — established by the 2026-05-11 incident (deployed code without DB sync → catalog/PGP/bib data loss → reverted to `6ce42522`). Web auto-deployed 2026-05-12 via `deploy.sh` after scp + atomic systemd swap (old DB preserved on server as `fjms_enrichment.db.pre-phase86-20260512` for rollback). Desktop installer rebuilt bundling updated `libraries.csv` (108 synthetic rows) + `fjms_enrichment.db` (3,264 synthetic rows). Pre-flight: 1836 passed / 20 skipped. **Phase 87 follow-up logged**: enable corrections/joins/comments on the 108 image-bearing synthetic rows (page_number semantics now well-defined as CUDL canvas index) — currently still disabled by Phase 85 guards. (both apps)
- May 2026: v7.10.0 release -- Search API Public Release. The v7.10 milestone (Phases 77, 78, 79, 80, 81A, 81B, 82, 83 — 8 phase entries spanning serializer foundation through public release) ships a public HTTP/JSON research-automation API over the existing Genizah corpus. Three endpoints: `POST /api/search` (keyword/Responsa/title/shelfmark search with per-IP rate limiting, mode gating, and uniform error envelope), `GET /api/browse` (stateless manuscript drill-down returning PGP transcription, FJMS/NLI enrichment, and image URLs from a search locator), `POST /api/parallels` (composition-parallels detection using sliding-window chunk matching). Security hardening includes XFF spoofing protection via trusted-proxy allowlist, fail-closed filter validation, Responsa expansion cap (MAX_EXPANDED_TERMS=500), and HMAC-hashed PostHog telemetry with persistent POSTHOG_IP_SALT. OpenAPI spec auto-generated from Pydantic models with explicit `openapi_extra` route metadata at `/api/openapi.json`; interactive Swagger UI at `/api/docs` (Phase 83 sub-mount, scoped to the 3 search-helper endpoints — legacy `/api/*` proxies excluded). Reference Claude skill `cairo-genizah-research` (Phase 81B, `skills/cairo-genizah-research/`) demonstrates search → browse → rank workflow with file-locked token-bucket throttling and browse-honesty annotations. `docs/SEARCH_API.md` is the public contract with stability commitment: additive changes any time; breaking changes only on major-version releases announced in CHANGELOG. Web-only release; NO desktop installer rebuilt/distributed; NO GitHub Release object (desktop polls releases/latest at gui_threads.py:459); NO `v7.10.0` git tag (consistent with prior web-only release pattern). Phase 83 also reframed `docs/SEARCH_API.md` from internal-only to public-facing (Stability + Quick Start + Attribution + Changelog sections), added README.md "## API" section + What's New entry, fixed OpenAPI sub-mount populated requestBody/parameters/responses (Codex HIGH fix from `83-REVIEWS.md`), and produced `83-SECURITY.md` audit verifying all Phase 78–81B mitigations remain load-bearing — 7-item Post-Deploy Verification checklist re-run against production. Pre-flight: 15/15 Phase 83 Wave 0 tests GREEN (8 docs + 4 OpenAPI scope + 3 release artifact); full pytest suite GREEN; check_docs green. (web)
- May 2026: v7.9.4 release -- NLI Library Code Fix. Tiny data-only patch correcting library attribution for 461 manuscripts in `libraries.csv`. User reported (sys_id 990025143260205171) that NLI manuscripts were rendering as Oxford in browse — external links, source toggles, and folio navigation all routed through Oxford-flavored code paths because browse keys off `library_code` column. Investigation: 461 rows had `library_code=Oxford` but call_numbers contained ONLY NLI shelfmarks (`The National Library of Israel Ms. Heb. ...` or `JER NLI Heb`) with no Oxford signal anywhere. Bad data has been present since libraries.csv was first introduced (commit 68dc0e99) — not a recent regression, just user-noticed now. Fix via new `scripts/fix_nli_oxford_mislabel.py` flipping the 461 unambiguous rows Oxford → NLI; preserves CRLF line endings (initial run wrote LF and produced full-file diff, restored from backup, fixed with line-ending detection). Apparent edge cases — 11 rows under Allony/Harkavy/HAS that also have NLI shelfmarks — left untouched as intentional cross-listings of private collections deposited at NLI. Pre-flight: 1384 passed / 10 skipped, check_docs green. Both apps deploy: desktop bundles libraries.csv in installer; web pulls it via deploy.sh.
- April 2026: v7.9.3 release -- Visual Similarity Dialog Fixes. Small web-only patch fixing three usability bugs in `web/components/visual_similarity_dialog.py`, all from the same user email. (1) Firefox could not reach the post-20-results `Show more` button because the right-pane Quasar `ui.scroll_area` did not scroll reliably in Firefox (Chrome was unaffected); swapped for a plain `<div style="overflow-y:auto; height:100%">`. (2) Ctrl/Cmd-click and middle-click on a suggestion opened in the same tab because shelfmarks and the `open_in_new` icon were `ui.button`s navigating via `ui.navigate.to()` — browser-native modifier-click never fired. Shelfmarks converted to `ui.link('/browse?sys_id=...')` with `click.stop_propagation` so row expansion still works; `open_in_new` icon wrapped in `ui.link(target=..., new_tab=True)`. (3) Manual text selection on the suggestion list excluded the shelfmark column (the most important field) because Quasar `q-btn` applies `user-select: none`; `ui.link` uses a plain anchor with `user-select: text`, so copy-paste now includes shelfmarks. Remaining action buttons (Add to Puzzle, Add to List, Add as Join) moved to `click.stop` handlers to avoid triggering the row expander. Codex implemented the fix; pre-flight: 1156 passed / 8 skipped, check_docs green, OPEN_ISSUES + CODE_INDEX updated. Web-only deploy; desktop stays at 7.9.2.
- April 2026: v7.9.2 release -- PGP Data Refresh. Data-only refresh release. Bundled Princeton Geniza Project metadata (pgp.db sidecar) was last imported 2026-02-05 and had gone 2.5 months stale. Re-imported from princetongenizalab/pgp-metadata (daily upstream exports; v1.1 schema with new person/place relationship columns gracefully ignored by row.get() pattern in importer). Deltas vs prior: documents 35,839 → 35,986 (+147), document_sources 9,364 → 9,523 (+159 = +93 Digital Editions + +50 translations), document_footnotes 22,757 → 22,968 (+211), document_fragments 36,155 → 36,500 (+345). Pipeline: pgp_transcriptions_export.py regenerates transcriptions_linked.csv (9,507 linked / 346 unmatched, 96.5% match rate unchanged); import_pgp_full.py --execute upserts to Supabase; export_pgp_sidecar.py rebuilds pgp.db (148.6 MB, down from 170 MB via compaction, row-count validation passes on all 4 tables). Web server received new pgp.db and restarted 2026-04-22 13:52 UTC. Desktop installer bundles it. Also ships two small post-7.9.1 fixes riding along: Oxford/NLI source-toggle buttons on /browse restored (commit 33e165d3), desktop Cambridge nav page_idx undefined fix (ruff F821, commit cf3473fb). Desktop WhatsNewBar + WhatsNewDialog texts finally refreshed (were still showing v7.7.0 Volume-Aware Browse content). OPEN_ISSUES reclassified WhatsNewDialog RTL ✅ Fixed → 🟡 Partially Fixed: block-level alignment works, but inline parenthesized Latin runs like (PGP) in Hebrew strings still need manual LRM (\u200E) marks; proper fix would auto-inject LRM in tr() or use <bdi>/LRI+PDI isolates. pytest 1156 passed / 8 skipped.
- April 2026: v7.9.1 release -- Catalog Attribution & Reading Desk Polish. Data-quality release. FJMS Instatution migration (267K catalog + 48K free_desc rows rewritten via local CODE_Institution join — ~30K manuscripts with empty `Catalog Information` dialogs now render GRU – Cambridge, Schocken-Zulay, Fleischer Piyut Project, Yad Harav Herzog, Uri Ehrlich, etc.). JTS browse source-switch fixed (L81) — MARC 942$z last-wins bug + `Ms.` prefix tolerance + `get_jts_urls_for_sys_id` JOIN + NLI circuit breaker (nav lag ~25s→~5s). CUL CUDL/NLI alignment via new `classify_cambridge_alignment` helper consolidating 5 duplicate decision sites; handles bifolios (T-S NS 158.112), binding canvases, same-count-different-order CUDL manifests (Or.2245); desktop past-CUDL auto-fallback. Reading Desk polish (desktop): `_browse_rd_enrich_entry` helper populates images for fragments added from list/top-shelfmark/green-bar that were never browsed this session (volume-aware via current_browse_volume_ie; threads sys_id-keyed with wait/terminate on exit + window close); Add to View respects typed input in top bar; green toolbar size policy pinned to Fixed; pre-populate; What's New RTL alignment. Security: ilike injection sanitization at 4 supabase_corrections_client sites (CR-02); Supabase config unified via shared provider (CR-01). Web log hygiene: 3 ui.timer→asyncio (L91); NiceGUI audit threshold constant + startup WARNING (IN-02); banner auto-dismiss flag only persists on successful .delete(). /_nicegui/ marked noindex. /_internal/memstat diagnostic endpoint. Phase 65 cleanup (WR-01/02, IN-01). New fjms_enrichment.db uploaded (1.6 GB, 2026-04-21). pytest 1156 passed / 8 skipped.
- April 2026: v7.9.0 release -- bundles v7.8 Structural Foundation + v7.9 Decomposition internal milestones into a shippable release. User-visible changes: back-navigation state-loss bugfix (browser Back from /browse to /search now restores the saved snapshot; regression from 2026-03-27 commit 829cd7cf, fix 8f9c5ef3), CUL paired-leaf folio-label fix (parse_folio_label handles paired-leaf bifolio ImageName patterns like T-S NS 158.112). Everything else internal refactor — page-scoped state reducing app.storage.user sprawl, search/browse/desktop decomposition, CI + deps pinning + auth migration (see CHANGELOG for full detail). Deployed as leak-investigation baseline — pre-refactor app.storage.user sprawl may have contributed to observed 8.7GB RssAnon after 5 days uptime.
- April 2026: v7.9 Decomposition (internal milestone, not a release) -- 10 phases, 23 plans. Desktop split: ResultDialog, filter/scholarly dialogs, image viewers (ManuscriptViewerWidget, FullscreenImageWindow), puzzle canvas, VS cache, widgets extracted into new desktop/ package — genizah_app.py slimmer (still ~22.5K lines per external review). Web split: search.py → search_state.py + search_results.py; browse.py → browse_state.py + browse_enrichment.py. Page-scoped state refactor reducing app.storage.user sprawl and detached asyncio.ensure_future in search + browse. CODE_INDEX.md regenerated via new scripts/gen_code_index_section.py AST generator; check_docs green. Back-navigation state loss regression fixed during Phase 75 verification (origin commit 829cd7cf 2026-03-27, fix 8f9c5ef3). Zero user-visible behavior change except the back-nav bugfix. APP_VERSION still 7.7.2; will ship as next release bump.
- April 2026: v7.8 Structural Foundation (internal milestone, not a release) -- 4 phases, 9 plans, 64 commits, 173 files changed (+6,269/-828 lines). 12/12 requirements satisfied. CI safety net (GitHub Actions, Ubuntu + Windows matrix, ruff + check_docs + pytest). Dependency pinning: requirements.txt (14 direct) + requirements-lock.txt (115 transitive). Supabase auth migration: gotrue → supabase_auth, PKCE-only OAuth. 205+ silent except handlers audited across 76 first-party files. NiceGUI monkey-patches isolated with version guards in web/framework_patches.py. Repo hygiene (.gitignore 50 → 126, untracked root 67 → 1). Docs refresh: CODE_INDEX, OPEN_ISSUES, DEVELOPER_GUIDE. Zero user-visible behavior change. APP_VERSION still 7.7.2.
- April 2026: v7.7.2 PageSpeed Quick Wins (A11y + Perf) -- Lighthouse homepage: accessibility 85 → 96, performance 90 → 98. Fixed `<html lang="undefined">` by passing full Quasar lang pack + JS guard + NiceGUI template patch at startup. Aria-labels on 10 icon-only buttons (help, dismiss, theme, citation copy/close, hero search). WCAG AA color contrast: light-theme --text-muted 2.34:1 → 4.63:1, global link color 3.06:1 → 5.44:1, dark-theme overrides for muted + Quasar primary/secondary/accent. Starlette middleware injects font-display: swap into NiceGUI's fonts.css (prevents ~1200ms invisible text). Conditional IIIF preconnect only on /search, /browse, /puzzle. Homepage "What is the Cairo Genizah?" heading promoted h3 → h2. Remaining: 13 parchment-theme color-contrast warnings deferred (web)
- April 2026: v7.7.1 SEO Round 2 -- bilingual meta tags (English brand + Hebrew search phrase "חיפוש בגניזה הקהירית" + Hebrew brand "אתר הגניזה של דיקטה"), English-leading per-page titles on indexable routes, homepage h1 contains target Hebrew phrases for crawlers, Organization + BreadcrumbList JSON-LD (SearchAction kept as legacy markup — Google deprecated Sitelinks Search Box Nov 2024), PostHog deferred via requestIdleCallback + dns-prefetch for analytics CDNs, client-side browse title unified with server format, Pesach banner hidden (pattern preserved for future seasonal themes), honest disclosure that performance was not measured — follow-up deferred for real Search Console + PSI data (web)
- April 2026: v7.7.0 Volume-Aware Browse -- 3,193 multi-IE manuscripts get volume selector for switching between microfilm scans (IEs), volume-correct text+images per IE, auto-default to Manchester/Cambridge/JTS images when available, ie_id in community data (corrections/comments tagged per volume), desktop external image filtering by volume, volume page count fix, threading.Lock for browse_map, browse_map IE repair function (both apps)
- March 2026: v7.6.0 Visual Similarity Suggestions -- FJMS SVM image analysis (~15.5M pairs) visual similarity browse dialog with ranked partners, thumbnails, domain/library metadata, Browse/Puzzle/Join actions, "Search in visual suggestions" with union/intersection modes, desktop on-demand server fetch with local cache, VS text snippet preloading, exclusion dialog "Active exclusions" section with per-source remove and "Clear all", New Search properly clears exclusions (both apps)
- March 2026: v7.5.0 Exclude Known Manuscripts -- hide already-reviewed manuscripts from search results using saved lists, imported shelfmark files (TXT/CSV), or pasted shelfmarks, multi-source tracking with per-source clear, resolution report table, collapsible excluded section, export respects exclusions (WYSIWYG), session persistence, shared exclusion_service.py with 15 tests, web tabbed picker dialog (Paste/List/File), desktop ExcludeDialog with QTabWidget + setRowHidden filtering, 38 Hebrew translations (both apps)
- March 2026: v7.4.0 Search Within Results -- progressive refinement restricting follow-up queries to manuscripts from current result set, breadcrumb chip chain with per-chip removal, cross-mode support (text/Responsa/Title/Shelfmark), "Only results with all terms" page-level filter (uid intersection), chain-aware snippet highlighting, manuscript-count labels, dark mode + RTL support, thread-safe SQLite services (per-thread connections), csv_bank race condition fix (both apps)
- March 2026: v7.3.1 SEO Foundation & Shareable Browse URLs -- per-page metadata (title, description, canonical, OG/Twitter per route), manuscript-specific browse metadata (shelfmark from csv_bank), sitemap index with ~255K manuscript URLs in 40K chunks, indexability policy (noindex,follow on search/parallels/lists/settings/corrections/admin/profile), robots.txt aligned, homepage WebSite JSON-LD, preconnect hints (NLI IIIF, Cambridge CUDL), shareable browse URLs (history.replaceState syncs URL bar on navigation with share button to copy link) (web)
- March 2026: v7.3.0 Manuscript Measurements, Bibliography Cleanup & Desktop Stability -- Measurements dialog (web + desktop browse) showing physical dimensions, margins, line counts, text density, material for 231K manuscripts from FJMS computed measurements (5 new tables, 1.5M rows imported), bibliography dedup (828K→427K, 48.4% reduction), 55K new Hebrew translations via Dicta Translation, desktop browse crash fix (navigation debounce + QThread lifecycle), persistent NLI FL-ID cache, concurrent fetches 4→8 (both apps)
- March 2026: v7.2.4 JTS Image Upgrade + Shelfmark Search Fixes -- Princeton DPUL as primary JTS image source (36K items), desktop Printed badge, blue mat auto-detection, enhanced shelfmark lookup (full library name stripping, ENA-MS normalization), FJMS bibliography enrichment (8 new fields), catalog dedup, search perf (removed duplicate enrichment), shared JS extraction (~1050 lines), browse/puzzle/PostHog bug fixes (both apps)
- March 2026: PostHog-driven UX & Auth fixes -- parallels rageclick prevention (immediate button disable, clickable shelfmarks, filter loading spinners, expansion chevrons, export button state), OAuth implicit flow fix (PKCE code_verifier issue), login PostHog tracking (login_failed enrichment, OAuth callback events, POSTHOG_SCRIPT on callback page, run.io_bound for non-blocking login), login dialog for anonymous write actions (discoveries vote/create/share, puzzle publish), _posthog_identify XSS fix (json.dumps), dev-mode auto port finding (both apps)
- March 2026: Princeton DPUL as primary JTS image source -- full DPUL catalog import (36,283 items, v1 had 453), JTS manuscripts auto-default to DPUL images in web browse, external link points to DPUL catalog page instead of manifest URL (both apps)
- March 2026: v7.2.3 Chrome Extension Live + Puzzle Enabled -- Chrome Web Store install link in puzzle banner (bilingual), WEB_PUZZLE_ENABLED defaults to True, json.dumps() escaping for banner texts
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
