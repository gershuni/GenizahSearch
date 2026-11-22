# GenizahSearch - Claude Context

> This file provides context for AI assistants working on the GenizahSearch project.

## How to work here

**Start with `powershell -File scripts/init.ps1`.** It prints repo state,
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
passes its ceiling. When a ceiling is hit, split closed content into `docs/archive/` (private
repository) — never raise the number.

**A worktree never links to the main checkout's data.** Sessions run in
`.claude/worktrees/<name>`, which has none of the gitignored sidecars (`nli_data/`, `fgp_data/`,
`pgp_data/`, `fist_data/`, `blau_data/`). Never bridge that with a junction or symlink into
`C:\GenizahSearch`: `git worktree remove --force` and `rm -rf` walk a junction as a directory, and
on 2026-11-04 that emptied the main checkout's `nli_data/` and `fgp_data/`. A session that needs a
sidecar copies it under `%LOCALAPPDATA%\GenizahSearchPro\data\<dir>\` (the path every
`shared/*_service.py` checks first) or points the service's env var at the main checkout's file
where one exists (`GENIZAH_FJMS_DB_PATH`; `docs/guides/ENV_VARS.md`). `scripts/init.ps1` warns on
any reparse point in a worktree; remove one with `cmd /c rmdir <link>`, never recursively.

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

Two applications over one shared core: the NiceGUI web app (`web/`, with FastAPI serving
`/api/*`) and the PyQt6 desktop app (`genizah_app.py` + `desktop/`), sharing `shared/`. Read-only
reference data comes from Tantivy indexes and SQLite sidecars (`pgp.db`, `fjms_enrichment.db`,
`nli_crossref.db`, `fgp_transcriptions.db`; `joins.db` holds saved joins); Supabase (PostgreSQL)
holds the community data (auth, lists, corrections, comments, discoveries) and the PGP reference
tables that `pgp.db` is exported from. The full picture --
components by subsystem, the dependency rules and the tests that enforce them, who owns which
state, and a decision for every top-level directory and root file -- is
[`docs/architecture/OVERVIEW.md`](docs/architecture/OVERVIEW.md).

The **standalone** backend service (the separate `genizah-backend` process on port 8000, with its own database and routers) was removed in January 2026. All read-only reference data is now served from local SQLite sidecars. Supabase is retained only for community features (auth, corrections, lists, comments).

> **Note:** FastAPI itself is still live and load-bearing — NiceGUI's `app` *is* a FastAPI instance, and `/api/*` routes (image proxies, exports, and the public Search API) are registered as FastAPI routes (`web/api.py::init_api_routes`) plus a dedicated `FastAPI()` sub-app mounted at `/api` in `web/main.py`. "Removed" refers only to the standalone backend process, not the framework.

## Key Files

Where code and data live is documented once, in
[`docs/architecture/OVERVIEW.md`](docs/architecture/OVERVIEW.md) (components by subsystem with the
load-bearing modules named) and [`docs/architecture/DATA_LIFECYCLE.md`](docs/architecture/DATA_LIFECYCLE.md)
(one row per data artifact: producer, consumers, where its location is decided). Binding
decisions are one page each under [`docs/decisions/`](docs/decisions/README.md); the symbol-level
index is `docs/CODE_INDEX.md`. The short version: `web/main.py` and `genizah_app.py` are the entry
points; `shared/` is what both apps share and `genizah_core.py` is a permanent facade over it; the
`shared/*_service.py` modules own the SQLite sidecars; `web/safe_storage.py` is the chokepoint for
per-user web state; `shared/discovery_relation_matrix.py` is the only source of a rendered relation.

## Common Tasks

### Running the Web App
```bash
python -m web.main
```
Opens on port 8081 by default (`GENIZAH_PORT`); in dev it takes the next free port above it.

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
- `docs/architecture/` - where code and data live (OVERVIEW.md, DATA_LIFECYCLE.md) -- authoritative
- `docs/decisions/` - the binding decisions, one page each
- `docs/guides/` - Developer guides
- `docs/specs/` - Technical specifications
- `CHANGELOG.md` - Full release history

Implementation plans (`docs/plans/`) and historical documents (`docs/archive/`) are kept in the
private repository.

## The private repository

Planning records, operations guides and research notes live in a separate private repository,
checked out beside this one at `C:\GenizahSearch-private` with the same relative paths. Tests
find it through `GENIZAH_PRIVATE_DIR` or that sibling folder, and skip when it is absent. Never
copy its contents, people's names, machine paths, server details or owner quotes into this public
repository. Write planning notes there, never here. Its `docs/guides/REPOSITORY_LAYOUT.md` maps the
repositories and folders.

**History before 2026-11-10.** This repository's history begins on 2026-11-10.
- Commit ids from before then (in this file, `CHANGELOG.md` and the private plans) belong to the
  earlier repository and do not exist here.
- PR numbers belong to the repository that holds them. Numbering began again at #1 on 2026-11-10, so a
  number from before then names a different PR today.
- Work begun there is carried over as a patch on a new branch from `origin/master-main`, never merged,
  cherry-picked or rebased across. `REPOSITORY_LAYOUT.md` gives the steps.

## Code Style

- Python 3.11
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
| `EDITIONS_SEARCH_ENABLED` | `false` | scholarly editions in the text search (`sources`, one result per page; also `editors`) | `<INDEX_DIR>/Editions` coherent (`shared/editions_search.py::check_generation`) |
| `CATALOG_TEXT_SEARCH_ENABLED` | `false` | the web Catalog mode (`%`, near spellings), its offer under an empty text search, `search_mode='catalog'` (API, MCP); opened at startup. Desktop: on unless set to 0 | `web/catalog_assets.py::catalog_available()` -- `catalog_data/catalog_text.db` checked |
| `SHELFMARK_TREE_ENABLED` | `true` | "Pick from the shelf", the place on the shelf and previous / next in shelf order on Browse, both apps; desktop also Go to (without the file Browse keeps the old order) | `web/shelfmark_tree_assets.py::shelf_tree_service()`, desktop `desktop/widgets/shelfmark_tree_picker.py::shelf_tree_service()` -- `shelfmark_tree_data/shelfmark_tree.db` opened and checked |
| `BLAU_DICTIONARY_ENABLED` | `true` | Blau's dictionary on `/browse` (click a word; several words are the expression search, once the 1.1.0 sidecar with its `tokens` table is deployed) | `shared/blau_service.py::BlauService.is_ready()` -- `blau_data/blau.db` present and checked |
| `LANE_DICTIONARY_ENABLED` | `true` | Lane's Lexicon, its own tab in the dictionary panel | `shared/arabic_service.py::ArabicService.is_ready()` -- `arabic_data/arabic.db` present and checked |
| `FRIEDMAN_DICTIONARY_ENABLED` | `false` — turned ON in production with v9.15.0 (owner, after the sidecar) | Friedman's Judeo-Arabic dictionary, its own tab, with the expression search (a machine reading; the tab and Help say it may contain errors) | `shared/friedman_service.py::FriedmanService.is_available()` -- `friedman_data/friedman.db` present and checked |
| `WEB_PUZZLE_ENABLED` | `true` | the web Fragment Puzzle page | — |
| `MCP_ENABLED` | `false` | the public MCP server at `POST /mcp` (replaces the GPT; `integrations/mcp/README.md`) | the `/api/chatgpt` facade being registered |
| `SEARCH_API_MODE` | `open` | the public API (`open`/`localhost-only`/`disabled`) | — |

The three `*_DICTIONARY_ENABLED` switches also take `editors`: served only to a signed-in editor or
admin, a preview before release (`shared/dictionary_switch.py`); `is_available()` stays the public answer.

Four more you will hit directly while working:

- `GENIZAH_STORAGE_SECRET` — **required** to run the web app anywhere (32+ characters, no
  fallback); `python -m web.main` exits at startup without it. On the server it must be set
  BEFORE deploying; changing it signs every web user out once.

- `GENIZAH_DISCOVERY_DATA_DIR` / `GENIZAH_PASSAGE_DATA_DIR` — dev/CI only, read ONCE at import.
  They select *which* directory is read; the loader still applies its full fail-closed contract.
  Leave unset in production.

## Testing

```bash
python scripts/run_local_tests.py
```

**Do not run the whole suite as one `pytest tests/` process.** Measured
2026-09-09: it was killed after 2h35m unfinished, holding 91.6 GB of private
commit on a 63 GB machine and spending its time paging (45% of ONE core out of
24). Nothing in this suite frees what it allocates, so the fix is bounded
process lifetime: `run_local_tests.py` splits the same selection into balanced
chunks of <=30 files across 4 disposable processes, using CI's own marker
expression. `-j N` changes the lane count; `--dry-run` prints the plan.

`pytest tests/some_file.py` for a single file is of course still fine.

One check is deliberately NOT in any routine run: the full verification of
`discovery-v5-REVIEW.db` (3.45 GB, 519,382 rows) used to cost 57 minutes per
run and is now behind `GENIZAH_VERIFY_REAL_ARTIFACT=1` plus its own script,
kept in the private repository, which FAILS rather than skips on missing
data. It is registered to run nightly (scheduler also kept in the private
repository), and run with `--expect-rows` before promoting any rebuilt artifact.

## Common Issues

1. **Search not working** - Check if Tantivy index exists in `Genizah_Index/`
2. **User data not syncing** - Check Supabase connection and credentials
3. **Images not loading** - NLI/Cambridge IIIF APIs may be down

## Documentation Maintenance (Important!)

**AI agents working on this codebase should keep documentation updated.**

### Open Issues Tracker (REQUIRED)

**`docs/OPEN_ISSUES.md`** lists every open defect, one line each, by priority and area, under a
stable ID (`GS-12`). Rebuilt 2026-10-25: the full text of each row is in `docs/ISSUE_DETAILS.md`,
and what is not a defect (deferred, debt, test gaps, ideas, owner tasks) is in `docs/BACKLOG.md`.
**Grep those two by ID; never read them whole.**

1. **At session start:** read the "Last Updated" line, the Summary and your area's rows.
2. **New defect:** a fragment in `docs/open_issues.d/` (see its README), never a row in the
   tracker: rows conflict across open PRs. `scripts/collect_open_issues.py --apply` gives it an ID.
3. **Fixed:** set the row's status to `✅ Fixed (YYYY-MM-DD, <sha>)`, then run
   `python scripts/archive_closed_issues.py --apply`: it moves the row and its details to
   `docs/archive/OPEN_ISSUES_ARCHIVE.md` in the private repository (~530 KB, grep only) and regenerates the counts.
4. **Never hand-edit a count:** the Summary is generated; `tests/test_open_issues_fragments.py`
   fails when it drifts, and `collect_open_issues.py --apply` repairs it. Cite issues by ID, never
   by line. Never re-run `scripts/split_open_issues.ps1 -Apply`: it rebuilds the archive from scratch.

### When to Update Docs

| If you change... | Update these docs |
|------------------|-------------------|
| Architecture/infrastructure | `CLAUDE.md`, `docs/architecture/OVERVIEW.md` (server steps: the deployment guide in the private repository) |
| Supabase schema (tables, RLS) | `docs/guides/SUPABASE_GUIDE.md` |
| Web app pages/components | `docs/CODE_INDEX.md` |
| Environment variables | `CLAUDE.md`, `docs/guides/DEVELOPER_GUIDE.md` |
| Major features | a `changelog.d/` file, never `CHANGELOG.md` itself (see its README); `README.md` |
| **App version** | Run `python scripts/bump_version.py X.Y.Z` |

### Version Bumping (REQUIRED for releases)

Run the automated script — it updates all version files at once:
```bash
python scripts/bump_version.py 6.3.0          # apply changes
python scripts/bump_version.py 6.3.0 --dry-run # preview only
```

**Files updated automatically:** `version.py` (source of truth), `version_info.txt` (Windows EXE metadata), `CompileScriptGenizah.iss` (Inno Setup), `README.md` (header line).

**Manual steps after running the script:**
1. `python scripts/collect_changelog.py --apply` (folds `changelog.d/` in), then `CHANGELOG.md` — add `## [X.Y.Z]` section; `python scripts/collect_open_issues.py --apply` (folds `docs/open_issues.d/` in)
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

- **v9.0.0 is CLOSED.** Its planning record and exit audit are in the private repository; read the
  audit there before planning discovery, passage-matching or REL-01 work.
- **Where code lives.** Search / metadata / variants / responsa / engines are in `shared/*.py`,
  not `genizah_core.py` (which is a 755-line facade re-exporting 27 names). Desktop dialogs,
  widgets, and update-UI are in `desktop/*.py`. **Grep `shared/` and `desktop/`**, not just the
  old god-files. (v8.3.0 decomposition) Layout, ownership and the guard tests:
  [`docs/architecture/OVERVIEW.md`](docs/architecture/OVERVIEW.md).
- **The root alias stubs are GONE; the old names no longer resolve.** Twelve root modules moved in
  September 2026: `gui_threads`, `corrections_ui`, `corrections_client`,
  `supabase_corrections_client`, `filter_text_dialog`, `list_filter_dialog`, `column_filter_dialog`
  to `desktop/`; `sefaria_utils`, `pgp_tag_translations`, `unified_variants`, `lists_sync` to
  `shared/`; and the dev-server CLI to `scripts/server.py`. Each left a four-line stub at the old
  path while consumers were rewritten; Round 2 deleted all eleven (2026-09-21), and with them those
  basenames' exemption in `tests/test_dependencies_declared.py`. The same day
  `genizah_translations` moved to `shared/` with no stub. **Import `desktop.<name>` /
  `shared.<name>`** -- `import gui_threads` and `import genizah_translations` are now ImportErrors;
  `TRANSLATIONS` also still comes off the `genizah_core` facade.
  `tests/test_no_root_alias_stubs.py` fails any tracked file that still names one, which matters
  because two rewritten sites sit inside `except ImportError` (`desktop/join_workbench.py` ->
  `desktop.gui_threads`; `desktop/corrections_client.py` -> `desktop.supabase_corrections_client`)
  and would degrade a feature silently rather than raise.
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
- **The discovery beta is LIVE in production** (`DISCOVERY_ENABLED=1` since 2026-08-08). A planning
  note in the private repository that says the flag must not be flipped is historical.

### Releases

PR numbers and commit ids below (9.15.0 and earlier) refer to the earlier, non-public repository.

- **v10.1.0 — Search the catalogue information (2026-11-19)** — PRs #41-#58: a Catalog mode (`%`) over the FJMS catalogue, PGP descriptions and library titles with their automatic translations, in both apps, the API and MCP (`shared/catalog_text_rows.py`, web gate `web/catalog_assets.py`, desktop `desktop/catalog_search.py`; the installer carries `catalog_data/catalog_text.db`); Fuzzy search (near spellings, `shared/near_spellings.py`) in Catalog and Title; "Found in the catalogue records"; a one-time "New" and three startup tips; the search box's per-mode lead and example, the desktop tooltip lists every mode sign; go-to boxes take the mode signs; `R` needs a space; Title finds abbreviations; web Reading Desk document edition, Puzzle flip/export/full screen, free rotation. Deploy: CODE FIRST, `scripts/deploy_catalog_text_sidecar.ps1 -Rows 2456035`, then `.env` `CATALOG_TEXT_SEARCH_ENABLED=1`. Web `WHATS_NEW_VERSION` "10.1.0-catalogue" (lit once the catalogue is served); desktop What's New 10.1.0. (both)
- **v10.0.2 — Each text shown once, in any order (2026-11-17)** — PR #39: `fold_same_text` joins, after its first pass, any two results holding one text (`_join_linked`), so a joined document's result count no longer depends on its rows' ranking. Code only. Website only: no desktop build or GitHub Release. Web What's New stays "10.0.0-scholarly". (web)
- **v10.0.1 — Each text shown once, and keyboard fixes (2026-11-17)** — PRs #36 #37: with the scholarly transcriptions a text on several fragments or editions is one result naming the other parts (`shared/editions_search.py::fold_same_text`, `also_in`; Search Within, list bulk add and replay via `result_sys_ids`/`row_sys_ids`), the desktop results table serves a cell's own tooltip; desktop Tab in visual order (`desktop/widgets/tab_order.py`), Enter searches from Gap/Exclude Words. Code only. Web What's New stays "10.0.0-scholarly", desktop dialog 10.0.0. (both)
**Older releases: the one-liners before v10.0.1 are in an archive file (private repository): this list keeps the last three releases, and each release moves the oldest out (2026-11-17). v8.5.2 and older (SEED-006 back to v7.7.x) are in [`CHANGELOG.md`](CHANGELOG.md) only.**
They were one-line duplicates of sections that file already carries in full, and nothing
in them changes how you work today. If you need one, grep the changelog for its version.
