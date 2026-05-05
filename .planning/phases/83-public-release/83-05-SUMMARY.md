---
phase: 83-public-release
plan: 05
subsystem: release
tags: [release, version-bump, deploy, milestone-close]
requires: [83-01, 83-02, 83-03, 83-04]
provides:
  - v7.10.0 production deployment
  - CHANGELOG [7.10.0] section
  - CLAUDE.md v7.10 Recently Changed entry
  - README v7.10.0 What's New entry
  - Phase 83 + v7.10 milestone close-out
affects:
  - version.py
  - version_info.txt
  - CompileScriptGenizah.iss
  - README.md
  - CHANGELOG.md
  - CLAUDE.md
  - web/main.py
  - genizah_translations.py
  - .planning/STATE.md
  - .planning/ROADMAP.md
tech-stack:
  added: []
  patterns: []
key-files:
  created:
    - .planning/phases/83-public-release/83-05-SUMMARY.md
  modified:
    - version.py
    - version_info.txt
    - CompileScriptGenizah.iss
    - README.md
    - CHANGELOG.md
    - CLAUDE.md
    - web/main.py
    - genizah_translations.py
    - .planning/STATE.md
    - .planning/ROADMAP.md
decisions:
  - NO git tag for v7.10.0 (web-only release pattern; CLAUDE.md history shows v7.9.x web-only releases were not tagged)
  - NO GitHub Release object for v7.10.0 (desktop polls /releases/latest; would trigger update prompt loop)
  - ROADMAP edit-existing style (Codex MEDIUM fix) — toggled `[ ]`→`[x]` on 5 plan checkboxes; no TBD placeholders to replace
  - Banner update commit (5a3624c3) caught via /release skill phase 2 checklist after main edit commits; both web/main.py + genizah_translations.py
metrics:
  duration: ~25 min wall (across resume from checkpoint)
  completed: 2026-05-05
---

# Phase 83 Plan 05: v7.10.0 Release & Milestone Close-Out Summary

Final v7.10 release: version bump to 7.10.0, CHANGELOG/CLAUDE.md/README narratives, banner copy refresh, production deploy via `bash deploy.sh master-main`, post-deploy verification (3 endpoints + OpenAPI schema-population + error envelope + Swagger UI), and STATE.md + ROADMAP.md milestone close-out. Web+API-only release: no git tag, no GitHub Release object.

## Tasks

| Task | Name                                                       | Commit       | Files                                                        |
| ---- | ---------------------------------------------------------- | ------------ | ------------------------------------------------------------ |
| 1    | Version bump to 7.10.0 via `bump_version.py`               | `3063b1e7`   | `version.py`, `version_info.txt`, `CompileScriptGenizah.iss`, `README.md` (header) |
| 2    | CHANGELOG [7.10.0] + CLAUDE.md Recently Changed + README "What's New" (D-14, Codex MEDIUM fix) | `a08d88d2`   | `CHANGELOG.md`, `CLAUDE.md`, `README.md` |
| 2b   | Banner copy refresh — web `What's New` (bilingual)         | `5a3624c3`   | `web/main.py`, `genizah_translations.py` |
| 3    | Pre-deploy gate (manual) — pytest + check_docs + skill smoke + Swagger UI visual | n/a (gate)   | — |
| 4    | Production deploy + post-deploy smoke + 7-item Post-Deploy Verification | deploy of HEAD `5a3624c3` | EC2 prod (no repo file change) |
| 5    | Close-out — STATE.md, ROADMAP.md, this SUMMARY             | (close-out)  | `.planning/STATE.md`, `.planning/ROADMAP.md`, `83-05-SUMMARY.md` |

## What Shipped

### Version files (Task 1)

`scripts/bump_version.py 7.10.0` atomically updated `version.py` (`APP_VERSION = "7.10.0"`), `version_info.txt` (filevers/prodvers/FileVersion/ProductVersion), `CompileScriptGenizah.iss` (`#define MyAppVersion`, `OutputBaseFilename`), and `README.md` header line. Desktop installer .iss was bumped as housekeeping; no installer is built for v7.10 (web+API only).

### Release narratives (Task 2)

- **`CHANGELOG.md`** — New `## [7.10.0] - Search API Public Release - 2026-05-05` section inserted before `[7.9.4]`. Covers all 8 phase entries (77, 78, 79, 80, 81A, 81B, 82, 83), with sub-sections: New Features (3 endpoints + OpenAPI + Swagger UI + JSON export buttons), Security & Hardening (rate limit, mode gate, error envelope, XFF protection, fail-closed filters, expansion cap, HMAC IP), Documentation (`docs/SEARCH_API.md`, stability statement, README API section), Internal (serializer module, reference Claude skill, Phase 81A breaking change).
- **`CLAUDE.md`** — New first bullet under `## Recently Changed` summarizing the milestone in one paragraph; explicitly notes web-only release (no installer, no GitHub Release, no git tag).
- **`README.md`** — New `**v7.10.0 (May 2026):**` bullet at top of `## What's New` (Codex MEDIUM fix; D-14 had only updated the version header line).

### Banner copy refresh (Task 2b — caught via /release skill phase 2 checklist)

The What's New banner displayed inside the running web app (`WhatsNewBar` + `WhatsNewDialog`) was still showing v7.9.4 NLI Library Code Fix copy. Commit `5a3624c3` updated both `web/main.py` (English banner text + version key) and `genizah_translations.py` (Hebrew translation entries) for v7.10.0 public-search-API messaging. The previous executor missed this; orchestrator caught it before deploy.

### Production deploy (Task 4)

Executed by orchestrator from local laptop:

```bash
ssh ubuntu@ec2-44-247-206-248.us-west-2.compute.amazonaws.com \
  "cd /home/ubuntu/GenizahSearch && ./deploy.sh"
```

`deploy.sh` ran `git fetch && git reset --hard origin/master-main` and `systemctl restart genizah-web`. Service `genizah-web.service` came up `active (running)` on commit `5a3624c3` at **2026-05-05 10:45:34 UTC**.

## Pre-Deploy Gates (Task 3) — User Confirmed Green

1. **`pytest tests/`** — exits 0; 15 Wave 0 tests included (8 docs + 4 OpenAPI scope + 3 release artifacts).
2. **`python scripts/check_docs.py`** — exits 0.
3. **Skill smoke against localhost:8081** — `OVERALL: PASS` from `python -m skills.cairo-genizah-research.scripts.smoke_test --base-url http://localhost:8081` (with `GENIZAH_API_BASE` set to localhost per Pitfall 3).
4. **Swagger UI visual check at `http://localhost:8081/api/docs`** — 3 endpoint cards (`/search`, `/browse`, `/parallels`) each rendered with populated parameter/body documentation (Codex HIGH fix from Plan 03 verified visually).

## Post-Deploy Verification — All Green

Run from local laptop against `https://genizahsearch.com` after deploy:

| # | Check                                                          | Result |
| - | -------------------------------------------------------------- | ------ |
| 1 | `GET https://genizahsearch.com/`                               | 200    |
| 2 | `GET /api/openapi.json` — info.version=`7.10.0`                | 200    |
| 3 | OpenAPI schema population: `/search` requestBody, `/browse` parameters, `/parallels` requestBody | All populated (Codex HIGH fix verified live) |
| 4 | `POST /api/search` — sample query                              | 200, schema_version=1, valid envelope |
| 5 | `GET /api/browse?sys_id=990025143260205171&p_num=1`            | 200, schema_version=1, full PGP/FJMS/NLI enrichment |
| 6 | `POST /api/parallels` — sample text                            | 200, schema_version=1, valid envelope |
| 7 | Error envelope fail-closed: `POST /api/search` with bad `search_mode` | `{"error":{"code":"invalid_request","fields":["search_mode"]}}` |
| 8 | Swagger UI at `https://genizahsearch.com/api/docs`             | Loads with 3 documented endpoints |

83-SECURITY.md Post-Deploy Verification checklist (7 items) re-run against production: all PASSED.

### Deferred to operator follow-up (NOT blocking)

- Rate-limiter live soak (30+ req/min → 429): the unit tests in `tests/test_api_hardening.py` exercise the limiter; live-soak left to ops if/when needed.
- `SEARCH_API_MODE=disabled` env-var flip drill: rollback procedure documented in 83-SECURITY.md and CONTEXT D-12; not exercised on the live deploy.

## Release-Mechanic Decisions (Codex Review Fixes Honored)

- **NO git tag for v7.10.0** — Verified `git tag --list | grep v7.10` returns empty. Web-only release pattern per CLAUDE.md "Recently Changed" history (v7.9.4, v7.9.3, v7.9.2, v7.9.1 all untagged).
- **NO GitHub Release object** — Per memory rule `[Never create GitHub release for web-only version]` and gui_threads.py:459 evidence (desktop polls `/releases/latest` and would prompt every desktop user to "update" to a release with no installer).
- **ROADMAP edit-existing-style (Codex MEDIUM fix)** — Plan 83 entry already listed 5 plans concretely; this plan toggled `[ ]`→`[x]` on all 5 and appended a milestone-status line. No TBD placeholders existed; none replaced.
- **README "What's New" entry added (Codex MEDIUM fix)** — D-14 had only bumped the version header via `bump_version.py`; the What's New section now also has a v7.10.0 bullet.

## Tracking Updates (Task 5)

- **`.planning/STATE.md`** — `stopped_at: Phase 83 complete -- v7.10.0 deployed`; v7.10 milestone marked complete; "Roadmap Evolution" appended with Phase 83 close-out narrative.
- **`.planning/ROADMAP.md`** — All 5 Phase 83 plan checkboxes toggled `[ ]`→`[x]`; v7.10 milestone header changed from `(active, started 2026-04-27)` → `(shipped 2026-05-05)`; Progress table row for Phase 83 updated to 5/5 Complete; v7.10 added to Milestones list at the top as "shipped 2026-05-05".

## Commits

| Hash         | Scope          | Subject |
| ------------ | -------------- | ------- |
| `3063b1e7`   | `chore(83-05)` | bump version to 7.10.0 |
| `a08d88d2`   | `docs(83-05)`  | CHANGELOG [7.10.0] + CLAUDE.md Recently Changed + README What's New for v7.10.0 (D-14, Codex MEDIUM fix) |
| `5a3624c3`   | `release(7.10.0)` | update web What's New banner -- Public Search API (bilingual) |
| (close-out)  | `docs(phase-83)` | complete v7.10 milestone -- public release of search API deployed and verified |

Production HEAD on master-main: `5a3624c3` deployed 2026-05-05 10:45:34 UTC.

## Verification

- `pytest tests/test_search_api_docs.py tests/test_openapi_scope.py tests/test_release_artifacts.py` — 15/15 GREEN at deploy time.
- Full suite `pytest tests/ -q` — exit 0 at deploy time.
- `python scripts/check_docs.py` — exit 0 (re-run after STATE/ROADMAP edits in this close-out commit).
- `python -c "from version import APP_VERSION; assert APP_VERSION == '7.10.0'"` — exit 0.
- Production smoke: 8/8 checks listed above all green.

## Deviations from Plan

**1. [Rule 2 — missing critical functionality] Web banner copy refresh added as Task 2b**

- **Found during:** Task 2 close-out review (orchestrator ran /release skill phase 2 checklist).
- **Issue:** `web/main.py` `WhatsNewBar` + `WhatsNewDialog` and `genizah_translations.py` Hebrew translations still showed v7.9.4 NLI Library Code Fix copy. Plan 83-05 listed CHANGELOG/CLAUDE.md/README as the narrative-update set but did not include the runtime banner. CLAUDE.md history shows the banner has been refreshed for every prior user-facing release (v7.9.4, v7.9.3, v7.9.2, etc.) — missing it is a correctness gap, not a feature add.
- **Fix:** Updated banner version key + bilingual messaging to v7.10.0 Public Search API copy.
- **Files modified:** `web/main.py`, `genizah_translations.py`.
- **Commit:** `5a3624c3 release(7.10.0): update web What's New banner -- Public Search API (bilingual)`.

No other deviations — release executed as planned including the Codex review fixes (no tag, no GitHub Release, edit-existing ROADMAP, README What's New).

## Self-Check: PASSED

- File `version.py` contains `APP_VERSION = "7.10.0"` — FOUND
- File `CHANGELOG.md` contains `## [7.10.0]` — FOUND
- File `README.md` What's New v7.10.0 bullet — FOUND
- Commit `3063b1e7` — FOUND in `git log`
- Commit `a08d88d2` — FOUND in `git log`
- Commit `5a3624c3` — FOUND in `git log`
- No git tag `v7.10.0` exists — VERIFIED via `git tag --list | grep v7.10` empty
- Production `https://genizahsearch.com/api/openapi.json` info.version=`7.10.0` — VERIFIED at 2026-05-05 post-deploy
