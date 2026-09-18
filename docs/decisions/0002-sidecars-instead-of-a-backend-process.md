# 0002 -- Read-only data from local SQLite sidecars; Supabase for community data; no standalone backend process

- **Status:** binding
- **Date:** 2026-02-01 (release 5.2.0, whose `CHANGELOG.md` section records the Supabase migration
  and the removal of the `backend/` directories; CLAUDE.md dates the removal to January 2026, the
  work that preceded the release)
- **Source:** the CLAUDE.md "Project Overview" note and its "Outdated Terms to Avoid" list.

## Decision

All read-only reference data is served from local SQLite sidecars opened read-only (`?mode=ro`)
by the `shared/*_service.py` modules -- `pgp_data/pgp.db`, `fist_data/fjms_enrichment.db`,
`fist_data/visual_similarity.db`, `nli_data/nli_crossref.db`, `fgp_data/fgp_transcriptions.db`,
`libraries_translations.db` -- plus the Tantivy indexes and, for the web app, the discovery
sidecar (`discovery_data/`), the passage index (`passage_index/current/`) and the baked atlas asset
(`atlas_data/`). `joins_data/joins.db` is different in kind: a **writable** puzzle-document store
created at runtime by `shared/puzzle_service.py`. Supabase (PostgreSQL) holds only community data:
auth, lists, corrections, comments, identification reviews. There is no separate backend process.

**FastAPI itself is still live.** NiceGUI's `app` is a FastAPI instance; `/api/*` routes are
registered by [web/api.py](../../web/api.py) (`init_api_routes`) and a dedicated FastAPI sub-app is
mounted at `/api` in [web/main.py](../../web/main.py). "Removed" refers to the standalone
backend process with its own database and port, nothing else.

## Why

Offline capability for the desktop app, no server dependency for reference lookups, and one less
service to run. Community features need shared state and auth, which Supabase provides.

## Consequences

- Sidecars reach the web server by `scp` **before** the code that reads them is pushed (CLAUDE.md
  fact "Web is not continuous-deploy"). The desktop installer bundles exactly seven of them by
  literal path in `GenizahSearchPro.spec`: the five databases above, `fist_data/vs_manifest.txt`
  and `libraries_translations.db`.
- CLAUDE.md's "Outdated Terms to Avoid" names the retired service, the old database-connection
  variable and `port 8000`; `scripts/check_docs.py` enforces its own `OUTDATED_TERMS` list (the
  service name, the old requirements path, the database variable) over `docs/**/*.md` outside
  `docs/archive/`, so `port 8000` is convention rather than a gate. Writing "FastAPI was removed"
  is wrong -- say "the standalone backend process".
- A 0-byte `.db` at a sidecar path is a stub, not data (see `docs/architecture/DATA_LIFECYCLE.md`).

## Supersedes

The 2025 architecture with a separate FastAPI backend service and its own PostgreSQL database.

## Enforced by

The outdated-term scan in [scripts/check_docs.py](../../scripts/check_docs.py); the service modules
open sidecars read-only and fail closed.
