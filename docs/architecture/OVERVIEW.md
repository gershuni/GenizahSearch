# GenizahSearch -- Architecture Overview

> Last updated: 2026-09-18
>
> This page is the **current-architecture** authority for where code and data live and who owns
> them. It supersedes the "Architecture" and "Key Files" sections that `CLAUDE.md` carried until
> 2026-09-18, the file listing `docs/FILE_INDEX.md` (now `docs/archive/FILE_INDEX_2026-03-26.md`),
> the GSD codebase map of 2026-02-05 (now `docs/archive/codebase-snapshot-2026-02-05/`) and the
> directory tree in `docs/guides/DEVELOPER_GUIDE.md`. Companion pages:
> [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md) (one row per data artifact) and
> [docs/decisions/](../decisions/README.md) (the binding decisions behind this layout).
>
> It is deliberately not an inventory. Counts and per-file listings drift; the tests linked below
> are what keep the rules true.

## One system, two applications

```
                 shared/  (search, metadata, sidecar services, discovery, passage matching)
                    ^                                   ^
                    |                                   |
   web/ (NiceGUI + FastAPI /api/*)          desktop/ + genizah_app.py (PyQt6)
   genizahsearch.com                        GenizahSearchPro.exe (PyInstaller + Inno Setup)
        |                                            |
        +------------- Supabase (community data only: auth, lists, corrections, comments, reviews)
        |                                            |
   Tantivy indexes + SQLite sidecars           Tantivy indexes + SQLite sidecars (bundled)
```

Both applications are maintained. They share the search engine, metadata, variants, the sidecar
service layer, discovery and passage matching through `shared/`. Read-only reference data is
served from local SQLite sidecars and Tantivy indexes; Supabase holds only community data
([decision 0002](../decisions/0002-sidecars-instead-of-a-backend-process.md)). There is no
standalone backend process -- but **FastAPI is live**: NiceGUI's `app` is a FastAPI instance,
`/api/*` routes are registered in [web/api.py](../../web/api.py) and a dedicated sub-app is
mounted at `/api` in [web/main.py](../../web/main.py).

## Entry points

| Surface | Start | Notes |
|---|---|---|
| Web app | `python -m web.main` ([web/main.py](../../web/main.py)) | port 8081 by default (`GENIZAH_PORT`; in dev the next free port above it); `/api/*` is the public Search API ([docs/SEARCH_API.md](../SEARCH_API.md)) |
| Local web-server manager (dev only) | `python server.py start\|stop\|status` ([server.py](../../server.py)) | spawns `web.main` detached and keeps a PID file; production uses the `genizah-web` systemd unit instead |
| Desktop app | `python genizah_app.py` ([genizah_app.py](../../genizah_app.py)) | the frozen build is `dist/GenizahSearchPro/GenizahSearchPro.exe`, produced by [build_app.bat](../../build_app.bat) from the checked-in [GenizahSearchPro.spec](../../GenizahSearchPro.spec); the installer by [CompileScriptGenizah.iss](../../CompileScriptGenizah.iss) |
| Index build | `python build_index.py [main|lab]` ([build_index.py](../../build_index.py)) | builds the Tantivy indexes from the transcription inputs |
| Tests | `python scripts/run_local_tests.py` ([scripts/run_local_tests.py](../../scripts/run_local_tests.py)) | the bounded runner ([decision 0003](../decisions/0003-bounded-test-runner.md)); `python scripts/run_gui_tests.py` for the Qt lane |
| Doc gates | `python scripts/check_docs.py` ([scripts/check_docs.py](../../scripts/check_docs.py)) | links, outdated terms, size ceilings, contract-header dates |
| Browser extension | `python extension/build.py` ([extension/build.py](../../extension/build.py)) | Chrome and Firefox ZIPs |

## Components, by subsystem

Curated pointers, not a listing. For a symbol-level index see
[docs/CODE_INDEX.md](../CODE_INDEX.md).

- **Search and metadata (`shared/`).** The Tantivy search, variant expansion
  ([shared/variants.py](../../shared/variants.py)), codicological and responsa helpers,
  metadata and shelfmark handling ([shared/shelfmark_bridge.py](../../shared/shelfmark_bridge.py),
  [shared/sys_id_patterns.py](../../shared/sys_id_patterns.py) -- the single definition of a
  sys_id pattern), and the sidecar services (`shared/*_service.py`: PGP documents, FJMS
  enrichment, NLI cross-reference, FGP transcriptions, translations, visual similarity, puzzle).
  [genizah_core.py](../../genizah_core.py) is a permanent facade over these
  ([decision 0001](../decisions/0001-permanent-genizah-core-facade.md)).
- **Web (`web/`).** [web/main.py](../../web/main.py) wires pages, middleware and the API;
  `web/pages/` holds one module per page, `web/components/` the reusable pieces;
  [web/safe_storage.py](../../web/safe_storage.py) is the chokepoint for per-user state
  ([docs/guides/MULTITENANT.md](../guides/MULTITENANT.md));
  [web/supabase_client.py](../../web/supabase_client.py) talks to Supabase.
- **Desktop (`desktop/` and `genizah_app.py`).** The PyQt6 main window is `genizah_app.py`;
  dialogs, widgets, the result viewer, the update UI and telemetry live in `desktop/`
  ([desktop/telemetry.py](../../desktop/telemetry.py) classifies crash frames by source-file
  basename). Several desktop-only modules still sit at the repository root (see the root-file
  table below).
- **Discovery (computed identifications, live beta).**
  [shared/discovery_service.py](../../shared/discovery_service.py) is the async chokepoint for
  every read; [shared/discovery_relation_matrix.py](../../shared/discovery_relation_matrix.py) is
  the only source of a rendered relation; [web/discovery_assets.py](../../web/discovery_assets.py)
  loads the sidecar fail-closed; the findings page is
  [web/pages/findings.py](../../web/pages/findings.py) and
  [web/components/discovery_links.py](../../web/components/discovery_links.py) is the one
  folio-correct link builder. Specs under `docs/specs/discovery-*.md`.
- **Passage matching (letter-level parallels).** The engine is `shared/passage_index.py`,
  `passage_search.py`, `passage_normalize.py`, `passage_policy.py`, wrapped by
  [shared/passage_parallels.py](../../shared/passage_parallels.py); the web loader is
  [web/passage_assets.py](../../web/passage_assets.py); the desktop side lives in
  `desktop/passage_*.py`. Algorithm: [docs/specs/passage-matching-algorithm.md](../specs/passage-matching-algorithm.md).
- **Fragment puzzle / joins.** `shared/puzzle_model.py`, `shared/puzzle_service.py`
  (`joins_data/joins.db`), `shared/puzzle_export.py`, `shared/puzzle_image_service.py`,
  `shared/background_removal.py`; web page `web/pages/puzzle.py`; desktop
  `desktop/puzzle.py` and `desktop/join_workbench.py`.
- **Visual Atlas** ([web/atlas_assets.py](../../web/atlas_assets.py), `web/pages/atlas.py`,
  bake-time tests under `tests/atlas_bake/`), the **browser extension** (`extension/`), the
  **public Search API** ([web/search_api.py](../../web/search_api.py),
  [docs/SEARCH_API.md](../SEARCH_API.md)) and the **research skill**
  (`skills/cairo-genizah-research/`, a published contract -- see
  [decision 0006](../decisions/0006-scripts-namespace-and-skill-collision.md)).

## Where code lives, and the rules that keep it there

| Directory | Holds | Rule |
|---|---|---|
| `shared/` | code both apps use | imports neither PyQt6 nor NiceGUI, and never `genizah_core`/`genizah_app` at module level |
| `desktop/` | PyQt6 code | never imports `genizah_app` at module level |
| `web/` | the NiceGUI app and its FastAPI routes | per-user state only through `web/safe_storage.py` |
| `scripts/` | tooling and data pipelines (flat namespace package) | never imported by `shared/` (one known exception, `scripts/discovery_ids.py`, is a Round 2 item) |
| `tests/` | flat, one file per concern, plus lane subdirectories | stays flat ([decision 0007](../decisions/0007-tests-stay-flat.md)) |
| root | entry points, build inputs, the facade, data assets `Config`/the spec resolve there | nothing else new goes to the root |

The rules are enforced by tests, not by convention. One row per guard:

| Guard | What it fails on |
|---|---|
| [tests/test_no_back_edges_core.py](../../tests/test_no_back_edges_core.py) (GUARD-01) | a registered `shared/` module importing `genizah_core` at module level (lazy, in-function imports are allowed) |
| [tests/test_no_back_edges_desktop.py](../../tests/test_no_back_edges_desktop.py) (GUARD-04) | a registered `desktop/` module importing `genizah_app` at module level |
| [tests/test_no_back_edges_discovery.py](../../tests/test_no_back_edges_discovery.py) | the discovery service layer importing `web`, `nicegui` or `fastapi` |
| [tests/test_seed016_layering_executor.py](../../tests/test_seed016_layering_executor.py) | any `shared/*.py` reaching into `web/` outside a `TYPE_CHECKING` block, and the browse/parallels services touching `web` at call time |
| [tests/test_dependencies_declared.py](../../tests/test_dependencies_declared.py) | a third-party import in `shared/`, `web/` or `desktop/` that is pinned in none of `requirements.txt`, `requirements-lock.txt` or `requirements-atlas-bake.txt` (try/except-ImportError and `TYPE_CHECKING` imports are exempt) |
| [tests/test_genizah_core_facade.py](../../tests/test_genizah_core_facade.py) | a facade name that is not the identical object from its `shared/` module |
| [tests/test_export_utils_shim.py](../../tests/test_export_utils_shim.py) | the root `shared_export_utils` shim drifting from `shared/export_utils.py` |
| [tests/test_no_raw_storage_access.py](../../tests/test_no_raw_storage_access.py) | raw `app.storage.user` access in `web/` outside the Phase 87 allowlist |
| [tests/test_sys_id_patterns.py](../../tests/test_sys_id_patterns.py) | a hand-rolled sys_id regex anywhere in tracked Python, outside `shared/sys_id_patterns.py` itself and any line carrying an annotated `sys-id-pattern-exempt:` opt-out |
| [tests/conftest.py](../../tests/conftest.py) (`_pin_root_scripts_namespace`) | not a test but the pin that keeps `scripts` bound to the root namespace package |

The two back-edge guards are **registries**: a new `shared/` or `desktop/` module must be added to
their lists, and until the registry-completeness check that follows this page lands, forgetting is
silent -- today `desktop/passage_lifecycle.py`, `passage_witnesses.py`, `passage_workers.py` and
`desktop/widgets/` are unregistered, and so is every `shared/` module created after the v8.3.0
extraction list stopped growing.

## The permanent facade

`genizah_core.py` re-exports names from `shared/` modules and asserts identity
(`genizah_core.X is shared.Y.X`) in [tests/test_genizah_core_facade.py](../../tests/test_genizah_core_facade.py).
It is never removed. New code imports from `shared.*` directly. Grep `shared/` and `desktop/`,
not the old god-files: the search, metadata, variants, responsa and engines are in `shared/*.py`,
and desktop dialogs, widgets and the update UI in `desktop/*.py` (v8.3.0 decomposition).

## Who owns which state

Details, one row per artifact, in [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md). The shape:

- **`Config` decides the desktop's paths** ([shared/config.py](../../shared/config.py)).
  `BASE_DIR` is the repository root when running from source and the executable's directory when
  frozen; `INTERNAL_DIR` is where bundled resources are read (`_internal/` next to the EXE, or
  `_MEIPASS`). The transcription inputs `Transcriptions.txt` / `AllGenizah_OLD.txt` are expected
  in `BASE_DIR` (next to the executable; at the checkout root on the server). `INDEX_DIR` is
  chosen at start-up -- a portable `Genizah_Index/` under `BASE_DIR` wins if it exists; otherwise
  the legacy `~\Genizah_Tantivy_Index` if it exists and `%LOCALAPPDATA%\GenizahSearchPro\Index`
  does not; otherwise that AppData path (and the portable path again if creating the chosen
  directory fails) -- and it is a **container**, not an index: the main
  Tantivy index is `INDEX_DIR/tantivy_db/`, the Lab index `INDEX_DIR/lab_index/`, and every piece
  of **mutable personal state** hangs off the same directory: the My Library side-indexes
  (`LocalIndex/`, `LocalLabIndex/`), the desktop's own passage index, `config.pkl`,
  `session.json`, `search_history.json`, `lang.pkl`, `lists.pkl`, `browse_map.pkl`, the lab
  settings, caches and logs. Reports go to `Documents\GenizahSearchPro\Reports`, falling back to
  `INDEX_DIR/Reports` when that is not writable. Two things live elsewhere: the desktop Supabase
  session under `~/.genizah_corrections/` and the saved password in the OS keyring.
- **Eight `shared/*_service.py` modules find their own root** by walking up until they see
  `libraries.csv` (after checking a `%LOCALAPPDATA%\GenizahSearchPro\data\...` override). None of
  the sidecar databases is a `Config` attribute. That is why the root data assets stay where they
  are ([decision 0008](../decisions/0008-root-assets-stay.md)) until the finders are consolidated.
- **The web server** runs from source, so `BASE_DIR` is its checkout; it reads the same sidecars
  and builds its Tantivy indexes in place under `Genizah_Index/`. Its letter-level passage index
  is **not** the desktop's: it lives at the repo-root `passage_index/current/`, resolved by a
  module-private default in [web/passage_assets.py](../../web/passage_assets.py) that the
  environment variable `GENIZAH_PASSAGE_DATA_DIR` overrides (read once at import); the discovery
  and atlas assets resolve the same way (`GENIZAH_DISCOVERY_DATA_DIR` for discovery). Per-user
  state is NiceGUI storage behind `web/safe_storage.py`; saved joins go to `joins_data/joins.db`.
  Data reaches the server by `scp` **before** the code that reads it is pushed; `deploy.sh` itself
  has no data step ([docs/guides/DEPLOYMENT_TECHNICAL.md](../guides/DEPLOYMENT_TECHNICAL.md)).
- **Gitignored top-level directories that code resolves at runtime** -- and that a clean clone
  therefore lacks: `Genizah_Index/`, `passage_index/`, `discovery_data/`, `atlas_data/`,
  `nli_data/`, `fgp_data/`, `joins_data/`, `discovery_builds/`, plus the gitignored files inside
  `pgp_data/` and `fist_data/`. They are not in the tracked-directory table below by design; each
  has a row in [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md).
- **The desktop installer** bundles what the spec's `datas` list names plus the import graph.
  Seven of those inputs are gitignored (the five sidecar databases, `libraries_translations.db`
  and `fist_data/vs_manifest.txt`), so the build is reproducible only on a machine that has been
  provisioned, never from a clean clone.
- **Supabase** holds community data only ([docs/guides/SUPABASE_GUIDE.md](../guides/SUPABASE_GUIDE.md)).
- **Secrets** (`.env`, `web/_secrets/`, the masking pattern file) are gitignored; the masking scan
  fails closed when its pattern file is not set ([decision 0009](../decisions/0009-masked-corpus-rule.md)).

## Feature flags are ANDed with readiness

Every gated surface calls one predicate that ANDs its flag with a fail-closed check of its data:
`discovery_available()`, `passage_available()`, `atlas_preview_available()`. A flag alone is never
proof a feature is live ([decision 0004](../decisions/0004-flag-and-readiness.md)). The flag table
with defaults is in `CLAUDE.md`; every variable is in
[docs/guides/ENV_VARS.md](../guides/ENV_VARS.md).

## Which document is authoritative for what

Four classes ([decision 0005](../decisions/0005-document-classes.md)):

| Class | Lives in | Rule |
|---|---|---|
| Current architecture | `docs/architecture/`, `docs/guides/`, `docs/specs/` pages marked current | authoritative; kept current by the doc gates |
| Active planning | `.planning/ROADMAP.md`, `STATE.md`, `PROJECT.md`, `.planning/phases/` | GSD-owned; edited only through the GSD tooling |
| Audit evidence | `.planning/milestones/*-MILESTONE-AUDIT.md`, `.planning/phase87_storage_allowlist.yaml`, phase decision files that tests read | cited; never moved |
| Archived history | `docs/archive/`, `.planning/milestones/*-phases/`, `.planning/quick/`, `.planning/debug/` | banner names the successor; excluded from `check_docs` scans |

The open-issues tracker is [docs/OPEN_ISSUES.md](../OPEN_ISSUES.md) (closed items move to
`docs/archive/OPEN_ISSUES_ARCHIVE.md`); release history is [CHANGELOG.md](../../CHANGELOG.md);
the instruction files for agents are `AGENTS.md` (commands and layout rules), `CLAUDE.md`
(project context), `CONTRIBUTING.md` and `.cursorrules`, kept consistent with each other by the
doc gates.

## Decision table: every tracked top-level directory

| Directory | What it is | Decision |
|---|---|---|
| `.claude/` | Claude Code settings and the two project skills (release runbook, sketch findings) | stays; tool-specific |
| `.github/` | CI workflow ([.github/workflows/ci.yml](../../.github/workflows/ci.yml)) | fixed by GitHub |
| `.planning/` | the GSD tooling's layout: active planning, audit evidence and phase history; several scripts write their evidence into `.planning/phases/<n>/` | classes above; never bulk-moved; `.planning/codebase/` holds seven adapter files over the archived 2026-02-05 map |
| `ACL2026_papers/` | research notes and paper drafts | stays; a move to `docs/research/` is an owner call (Round 2) |
| `data/` | the v0.8 repair manifest and few-shot prompt files | stays; **not bundled** by the spec (the frozen app never sees `data/`) |
| `desktop/` | PyQt6 modules | grows: desktop-only root modules are proposed to move here (Stage 2) |
| `docs/` | documentation, by class | `architecture/` and `decisions/` added 2026-09-18 |
| `eval/` | evaluation data only: the Megillat Antiochus deck, aliases, query and graded runs (the scripts that read them live in `scripts/`) | stays |
| `extension/` | browser extension sources and its build script | stays |
| `fist_data/` | FJMS sidecar databases (gitignored) plus the tracked synthetic manifest | stays; DBs are provisioned, not committed |
| `integrations/` | the ChatGPT integration's OpenAPI schema and privacy page | stays; served to the GPT |
| `migrations/` | Supabase SQL migrations | stays; a `supabase/` regrouping with `supabase_setup.sql` is a Round 2 item |
| `pgp_data/` | PGP sidecar (`pgp.db`, gitignored) and its tracked companions | stays |
| `reports/` | CSV/TXT evidence from data pipelines; one is read at runtime (`leading_zero_collisions.csv`) and one is written at start-up (`cudl_alias_collisions.csv`, untracked) | stays; the four phase-84 leftovers move to `docs/archive/reports/phase84/` in the Round 1 archiving PR that follows this page |
| `scripts/` | tooling, flat namespace package | stays flat; regrouping is Round 2 |
| `shared/` | code both apps use | grows: cross-app root modules are proposed to move here (Stage 2) |
| `skills/` | the published research skill | **frozen in place** (published contract, CI-exercised, namespace pin) |
| `tests/` | the suite, flat plus lane subdirectories | stays flat |
| `verification/` | 2026-04 accessibility verification scripts and the screenshots they emit; nothing reads them | still at the root; moves to `docs/archive/verification-2026-04-a11y/` in the Round 1 archiving PR that follows this page |
| `web/` | the NiceGUI app | stays |

## Decision table: every tracked root file

Root is for entry points, build inputs, the facade, the instruction files, and the data assets
that `Config` or the spec resolve there. Everything else has a reason or a plan.

**Entry points and the facade**

| File | Decision |
|---|---|
| `genizah_app.py` | desktop entry point; stays (extractions are Round 2) |
| `genizah_core.py` | permanent facade; stays |
| `build_index.py` | index builder invoked by the admin guide and `scripts/rebuild_index.sh`; stays |
| `version.py` | version source of truth, rewritten by `scripts/bump_version.py`; stays |
| `shared_export_utils.py` | compatibility shim over `shared/export_utils.py`; stays while call sites use it |
| `genizah_translations.py` | translation tables imported across both apps; stays for now (Round 2 candidate for `shared/`) |

**Modules at the root whose home is `desktop/` or `shared/`** -- proposed Stage 2 moves, each with
an alias stub at the old path; not yet approved:

| File | Proposed home | Why it is not there yet |
|---|---|---|
| `gui_threads.py`, `corrections_ui.py`, `corrections_client.py`, `supabase_corrections_client.py`, `filter_text_dialog.py`, `list_filter_dialog.py`, `column_filter_dialog.py` | `desktop/` | tests read some of them by path, loggers are named after `__name__`, and the frozen build must be re-verified |
| `sefaria_utils.py`, `pgp_tag_translations.py`, `unified_variants.py` (generated), `lists_sync.py` | `shared/` | the spec bundles `pgp_tag_translations.py` by name; `lists_sync`'s logger is named after the module and `tests/test_local_namespace_no_lists_leak.py` filters `caplog` on that name, so a move renames what the tests listen to |
| `server.py` | `scripts/` | anchors the repo root from its own location |

**Build inputs** -- stay: `GenizahSearchPro.spec`, `build_app.bat`, `CompileScriptGenizah.iss`,
`version_info.txt`, `icon.ico`, `requirements.txt`, `requirements-lock.txt`,
`requirements-atlas-bake.txt`, `pyproject.toml`, `ruff.toml`; and `ANTIVIRUS_INFO.txt`, the
bilingual note for users whose antivirus flags the installer, linked from the README's download
section (it is not in the installer itself).

**Data assets resolved at the root** -- stay ([decision 0008](../decisions/0008-root-assets-stay.md)):
`libraries.csv`, `oxford_full_db.json`, `ie_volume_map.json`, `Help.html`,
`cambridge_genizah.json` (a build-time input of `nli_crossref.db`), `char_merges_report.xlsx`
(the input of the generated `unified_variants.py`), `fist_gap_rows.csv`,
`fist_gap_manifest.txt`, `fist_gap_ambiguous_titles.txt`; and `bodleian_master_index.csv`, which
no code reads but the spec still bundles -- dropping it from the spec is an owner call recorded
in the Round 2 backlog.

**Operations** -- `deploy.sh` runs on the server and `deploy.bat` is its Windows trigger (both
stay; `deploy.bat` invokes `deploy.sh` over ssh and nothing else in the repo references either,
because they are the top of the chain);
`supabase_setup.sql` stays until the `supabase/` regrouping; `.env.production.example` documents
the server's variables.

**Instruction files and repository metadata** -- stay: `README.md`, `CHANGELOG.md`, `CLAUDE.md`,
`AGENTS.md`, `CONTRIBUTING.md`, `.cursorrules`, `LICENSE`, `.gitignore`, `.gitattributes`.

**Scheduled for archiving in the Round 1 archiving PR that follows this page** (a move under
`docs/archive/`, nothing deleted; all five are still tracked at the root as this page is written):
`start_servers.sh` (launches the removed backend process), `web_pilot.py` (unreferenced),
`image.png` (not the site's OG image, which is `web/static/og-image.png`), `char_merges_all.xlsx`
and `char_merges_filtered.xlsx` (intermediate outputs of the character-merge analysis; the report
stays).

## What is deliberately not decided here

Sub-packages inside `shared/`, a `scripts/` regrouping, extractions from `genizah_app.py`, the
consolidation of the eight root finders, and a `src/` layout are Round 2 items recorded in
[docs/OPEN_ISSUES.md](../OPEN_ISSUES.md). The Stage 2 module moves above wait for the owner's
decision after this page and DATA_LIFECYCLE have been read.
