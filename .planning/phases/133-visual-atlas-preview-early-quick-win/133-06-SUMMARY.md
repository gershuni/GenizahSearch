---
phase: 133-visual-atlas-preview-early-quick-win
plan: 06
subsystem: web
tags: [atlas, bake, masking, deploy, render-smoke, brotli, feature-flag]
status: in-progress   # Tasks 1-2 (LOCAL) done + committed; Tasks 3-4 (PRODUCTION) pending human deploy

# Dependency graph
requires:
  - phase: 133-01
    provides: "scripts/check_atlas_masking.py (--scan-asset recursive + --scan-repo) + MASKING_SCAN_PATTERNS_FILE convention"
  - phase: 133-02
    provides: "scripts/build_atlas_asset.py (the bake) + docs/specs/atlas-asset-schema-v1.md (frozen schema)"
  - phase: 133-03
    provides: "web/atlas_assets.py (load_atlas_state + atlas_preview_available) + /atlas route + /atlas-data/* routes + predicate-gated nav"
  - phase: 133-04
    provides: "web/static/js/atlas_decode.js renderer (readiness window.__atlasRenderer; #atlas-search / hover-tooltip / click-focus interactions)"
  - phase: 133-05
    provides: "predicate-gated homepage teaser card (atlas-teaser-card) on /"
provides:
  - "scripts/capture_atlas_html.py — ASGI + Playwright browser-DOM capture helper feeding check_atlas_masking.py --scan-asset"
  - "tests/render_smoke/test_atlas_four_surface.py — parametrized page/data/nav/teaser x OFF/asset-missing/ready behavioral integration test (12 tests)"
  - "The REAL production atlas asset baked LOCALLY into gitignored atlas_data/ (content-hashed, masking-clean) — ready for the asset-first production upload (Task 3, human)"
  - "docs/guides/DEPLOYMENT_TECHNICAL.md + CHANGELOG.md + README.md — the sidecar-style atlas_data/ asset + asset-first deploy procedure + claim-free beta entry"
affects: [133-06 Tasks 3-4 (human production deploy + smoke/rollback)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Offline REAL bake from the ~2.9 GB research DB with EXACT eligible==placed node-set equality, content-hashed + Brotli, gitignored (never committed) — mirrors the SQLite sidecar posture"
    - "Two-mode masking capture: in-process ASGITransport render (server HTML, hard pre-deploy gate) + Playwright client-DOM capture (catalogue strings only appear in the interaction-created DOM — Codex HIGH-4; guaranteed run is the live smoke)"
    - "Four-surface single-predicate behavioral gate proven by rendering (nav + teaser asserted from the live tree, not source reference — Codex MEDIUM-6)"

key-files:
  created:
    - scripts/capture_atlas_html.py
    - tests/render_smoke/test_atlas_four_surface.py
  modified:
    - docs/guides/DEPLOYMENT_TECHNICAL.md
    - CHANGELOG.md
    - README.md

key-decisions:
  - "The four-surface test builds a small self-contained in-memory _AtlasState for the READY data-route case (no 2.9 GB DB, no bake-time deps in the render_smoke job, no committed atlas_data/) — the data route serves plain/manifest bytes verbatim (binary validation is load_atlas_state's job), so a tiny fabricated payload proves 200-vs-404 gating"
  - "The NAV surface is isolated by the unique 'nav-item-badge' class (the atlas item is the only badged nav item; the page-chrome Beta badge does NOT carry it), so nav presence is unambiguous even on the /atlas page"
  - "capture_atlas_html.py loads the REAL baked asset + forces the flag ON so atlas_preview_available() is genuinely True (all four surfaces + data routes read the same predicate); body_html access is defensive (a NiceGUI in-process-sim quirk raises 'Request is not set' on the busy / page) — the element-tree dump is the authoritative, more-complete masking surface"

requirements-completed: []   # ATLAS-01 SC#5 completes only after the human production deploy (Tasks 3-4)

# Metrics
duration: 90min
completed: 2026-07-21
---

# Phase 133 Plan 06: Deploy Checkpoint — LOCAL portion (Tasks 1-2) Summary

**Baked the REAL production Connections Atlas asset from the ~2.9 GB research DB with exact node-set completeness (eligible==placed==62,645, missing=0, extra=0), content-hashed and Brotli-compressed well under budget; built the two-mode (ASGI + Playwright browser-DOM) masking capture helper; passed BOTH hard pre-deploy masking gates (recursive `--scan-asset atlas_data/` AND the ASGI-captured server-HTML scan, exit 0); added a parametrized four-surface (page/data/nav/teaser × OFF/asset-missing/ready) behavioral integration test; and documented the new sidecar-style asset + asset-first deploy procedure. The production-touching Tasks 3-4 (asset-first upload → deploy → flag → restart → live smoke → rollback) are `checkpoint:human-verify` and remain PENDING the human deploy — this plan is IN PROGRESS, not complete.**

## Scope executed (LOCAL only)

Per the execution directive, ONLY the two `type="auto"` LOCAL tasks were executed. **Tasks 3 and 4 (`checkpoint:human-verify`, production deploy + live smoke + rollback) were NOT started — zero production actions were taken** (no ssh, no scp, no prod `.env` edit, no `deploy.sh`, no web server launched). The phase completes only after the human deploy.

## The baked asset (ready for the asset-first upload)

| Field | Value |
|-------|-------|
| asset_basename | `atlas-v1-61519a85a2d0` |
| content_hash | `61519a85a2d0` |
| source_db | `same_work_spike/probe/data/fullcorpus_v2.db` (~2.9 GB; the 0-byte `data/fullcorpus_v2.db` is a stub) |
| eligible_count / placed_count | **62,645 / 62,645** (missing=0, extra=0 — exact set equality) |
| regression floor | >= 62,414 ✓ |
| node / edge / cluster / label / flow | 62,645 / 437,373 / 12,922 / 298 / 3,000 |
| plain `.bin` | 12,630,832 bytes |
| Brotli `.bin.br` | **2,259,052 bytes** (≤ 6,000,000 byte cap ✓) |
| location | gitignored `atlas_data/` (never committed; outside `web/static/`) |

## Masking gate results (both HARD gates GREEN)

| Gate | Command | Exit |
|------|---------|------|
| Baked asset (recursive) | `check_atlas_masking.py --scan-asset atlas_data/` | **0** (clean) |
| ASGI-captured server HTML | `check_atlas_masking.py --scan-asset <asgi-capture-dir>` | **0** (clean) |
| Repo (binding, pre-commit ×2) | `check_atlas_masking.py --scan-repo` | **0** (clean) — both before Task 1 and after the doc edits |

The ASGI captures were confirmed to render the REAL atlas surfaces (chrome + `#atlas-canvas` on `/atlas`, the `atlas-teaser-card` on `/`, real Hebrew under HE), so the clean scan is over genuinely-rendered atlas content, not an empty clean-hidden page. The browser-DOM (client-DOM) capture is the ONLY surface that sees the interaction-created catalogue strings (Codex HIGH-4); Playwright is not installed locally so that mode SKIPPED cleanly here — its GUARANTEED run is the live production smoke (Task 4, human).

## Four-surface test

`python -m pytest tests/render_smoke/test_atlas_four_surface.py -m render_smoke -q` → **12 passed** (4 surfaces × 3 states). Run together with the existing `test_atlas_render_smoke.py` + `test_home_teaser_render_smoke.py`: **18 passed**, no cross-test pollution. Every assertion is behavioral: page renders chrome vs clean-hides; data routes 200 vs 404 (incl. the non-whitelisted-name 404); nav badge present vs absent; teaser present-and-links-to-/atlas vs absent.

## Doc updates (claim-free + masking-safe)

- **`docs/guides/DEPLOYMENT_TECHNICAL.md`** — added `atlas_data/` to the directory structure (sidecar-style, gitignored, OUTSIDE `web/static/`) and a new "Deploy the Connections Atlas Beta (asset-first)" procedure (bake → scp asset FIRST → `deploy.sh master-main` → set `ATLAS_PREVIEW_ENABLED` → restart) alongside the existing sidecar scp-first guidance, plus the flag-off rollback.
- **`CHANGELOG.md`** — an `[Unreleased]` claim-free Connections Atlas beta entry (behind `ATLAS_PREVIEW_ENABLED`, default OFF; noindex; bilingual).
- **`README.md`** — a short claim-free beta mention in "What's New".

All doc copy uses only the `M-source` codename, carries no corpus name/sigla, and no identification counts / "discoveries found" / claim numbers (`--scan-repo` clean after the edits).

## Task Commits

Explicit-path staging only (never `git add -A`); the pre-existing dirty files `genizah_translations.py`, `web/main.py`, `web/pages/browse.py` were never staged; the gitignored `atlas_data/` was never committed.

1. **Task 1: atlas HTML capture helper (ASGI + browser-DOM)** — `83115dfb` (feat) — `scripts/capture_atlas_html.py`
2. **Task 2: four-surface availability test + asset-first deploy docs** — `4831b2d4` (test) — `tests/render_smoke/test_atlas_four_surface.py`, `docs/guides/DEPLOYMENT_TECHNICAL.md`, `CHANGELOG.md`, `README.md`

**Plan metadata:** this SUMMARY + the STATE.md in-progress update — see final docs commit.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Removed an unused `import json` from capture_atlas_html.py**
- **Found during:** Task 1 (pre-commit ruff pass)
- **Issue:** ruff F401 on `import json` (unused after the capture logic settled) — would break CI (CLAUDE.md "Pre-release MUST run ruff").
- **Fix:** dropped the import. Verified `python -m ruff check scripts/capture_atlas_html.py` clean.
- **Committed in:** `83115dfb`

**2. [Rule 1 - Bug] Defensive `body_html` capture in the ASGI mode**
- **Found during:** Task 1 (first capture run)
- **Issue:** `user._client.body_html` raises `RuntimeError: Request is not set` for the busy `/` page in the in-process NiceGUI simulation (it walks `Client.instances` for session ids) — a known harness quirk, not an atlas defect.
- **Fix:** wrapped `body_html` access in try/except (fall back to empty); the element-tree text/content/props dump is the authoritative, more-complete masking surface and captured the full home page (33-38 KB). Both hard gates still exit 0.
- **Files modified:** scripts/capture_atlas_html.py
- **Committed in:** `83115dfb`

---

**Total deviations:** 2 auto-fixed (1 blocking/ruff, 1 test-harness robustness). No scope creep; no production actions.

## Known Stubs

None. The capture helper, four-surface test, and docs are fully wired. (The Playwright browser-DOM mode is a deliberate best-effort-pre-deploy / guaranteed-in-live-smoke design, not a stub.)

## Masking / Staging Discipline (binding phase rule, honored)

- `MASKING_SCAN_PATTERNS_FILE` exported for every scan. `--scan-repo` run (and green, exit 0) before Task 1 and again after the Task 2 doc edits. `--scan-asset atlas_data/` and the ASGI-captured-HTML scan both exit 0. All committed copy uses only the `M-source` codename.
- Explicit-path staging only; `git diff --cached` verified before each commit to contain ONLY this plan's files; dirty files and `atlas_data/` never staged.

## Next Steps (PENDING human production deploy — Tasks 3-4)

- **Task 3 (`checkpoint:human-verify`):** asset-first production deploy — scp the gitignored `atlas_data/` (content-hashed `.bin` + `.bin.br` + `manifest.json` for `atlas-v1-61519a85a2d0`) to `/home/ubuntu/GenizahSearch/atlas_data/` FIRST (outside the static root), then `deploy.sh master-main`, then set `ATLAS_PREVIEW_ENABLED=1`, then restart `genizah-web`; confirm `GET /atlas-data/manifest.json` → 200 + `Cache-Control: no-cache` + ETag.
- **Task 4 (`checkpoint:human-verify`):** production smoke (render + Brotli br/plain/406 + manifest no-cache+ETag + noindex + EN/HE + teaser) + the LIVE browser-DOM masking capture (`capture_atlas_html.py --browser-dom https://genizahsearch.com` → `check_atlas_masking.py --scan-asset` exit 0) + the flag-off rollback drill.
- ATLAS-01 SC#5 (beta LIVE in production) completes only after Task 4 passes.

## Self-Check: PASSED

- FOUND: `scripts/capture_atlas_html.py`
- FOUND: `tests/render_smoke/test_atlas_four_surface.py`
- FOUND: `atlas_data/atlas-v1-61519a85a2d0.bin` + `.bin.br` + `manifest.json` (gitignored — local build artifact, intentionally NOT committed)
- FOUND: commit `83115dfb` (Task 1)
- FOUND: commit `4831b2d4` (Task 2)

---
*Phase: 133-visual-atlas-preview-early-quick-win*
*Portion completed (Tasks 1-2, LOCAL): 2026-07-21 — Tasks 3-4 pending human production deploy*
