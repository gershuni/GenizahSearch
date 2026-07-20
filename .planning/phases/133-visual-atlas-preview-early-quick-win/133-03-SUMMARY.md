---
phase: 133-visual-atlas-preview-early-quick-win
plan: 03
subsystem: web
tags: [atlas, feature-flag, nicegui, fastapi, brotli, content-encoding, etag, i18n, cls]

# Dependency graph
requires:
  - phase: 133-01
    provides: "scripts/check_atlas_masking.py (D-07 masking scan) — run --scan-repo before every commit; atlas_data/ gitignored + off /static"
  - phase: 133-02
    provides: "docs/specs/atlas-asset-schema-v1.md (frozen schema) + the content-hashed manifest.json/asset_basename layout the loader + data routes serve"
provides:
  - "ATLAS_PREVIEW_ENABLED flag (web/feature_flags.py, default OFF)"
  - "web/atlas_assets.py — the SINGLE authoritative asset-state loader (load_atlas_state, plain required / brotli optional, fail-closed) + atlas_preview_available() predicate + byte/ETag accessors (atlas_bin_name/atlas_plain_bytes/atlas_br_bytes/atlas_manifest_bytes/atlas_manifest_etag)"
  - "/atlas page route (predicate-gated clean-hide + noindex + EN/HE) + predicate-gated Connections Atlas nav item (web/main.py)"
  - "/atlas-data/manifest.json (no-cache + must-revalidate + ETag + 304) and /atlas-data/{asset_name} (content-hashed, immutable, br/identity/* q-value negotiation, reachable 406, whitelisted name, off /static) — the exact fetch contract 133-04 consumes"
  - "_negotiate_encoding() helper (br/identity/* q-values, honoring q=0)"
  - "web/pages/atlas.py — create_atlas_page() beta chrome (badge, honesty banner, intro, CLS-reserved canvas + 133-04 renderer injection point)"
  - "Complete atlas HE translation set in genizah_translations.py (chrome + D-08 interactions + 133-05 teaser + error copy)"
  - "tests/test_atlas_flag_gating.py (15 tests) — the predicate-gate / negotiation / manifest-cache / translated-value guards"
affects: [133-04, 133-05, 133-06]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Single authoritative asset-state source-of-truth (web/atlas_assets.py): load ONCE at startup, ONE atlas_preview_available() predicate gates page + nav + data routes so they can never disagree (HIGH-2/MEDIUM-6)"
    - "Fail-closed startup asset load: readiness requires manifest+plain .bin; .bin.br OPTIONAL (missing-representation 406 path stays reachable); ANY error -> ready=False, no traceback"
    - "Content-Encoding negotiation over br/identity/* with q-values (honoring q=0 refusals) via a pure _negotiate_encoding() helper -> reachable 406, never an invalid 200"
    - "Mutable manifest pointer (no-cache + must-revalidate + ETag + 304) fronting an immutable content-hashed asset — a rebake never strands a client on a stale hash"
    - "Precompressed asset served via a dedicated FastAPI route (NOT add_static_files, which cannot set Content-Encoding), from repo-root atlas_data/ OUTSIDE /static (HIGH-1)"
    - "Route registrar helper (_register_atlas_data_routes(target_app)) so the same routes register on the real NiceGUI app AND on a bare FastAPI for response-level tests without full NiceGUI startup"

key-files:
  created:
    - web/atlas_assets.py
    - web/pages/atlas.py
    - tests/test_atlas_flag_gating.py
  modified:
    - web/feature_flags.py
    - web/main.py
    - genizah_translations.py
    - CLAUDE.md
    - docs/guides/DEVELOPER_GUIDE.md
    - docs/CODE_INDEX.md

key-decisions:
  - "Data routes registered via a _register_atlas_data_routes(target_app) helper (co-located in web/main.py so the plan's source-string verify passes) called with the real app AND with a bare FastAPI in tests — response-level br/identity/* + ETag/304 behavior is exercised without booting NiceGUI"
  - "Tests call the route endpoints DIRECTLY (inspecting starlette Response .body/.headers) rather than via TestClient/httpx for the asset route — httpx eagerly Brotli-decodes a Content-Encoding: br body (and strips the header), which would fail on synthetic .bin.br bytes and hide the transport assertions"
  - "web/atlas_assets.load_atlas_state() also validates content_hash == sha256(plain)[:12] when the manifest carries one — a manifest/bytes disagreement fails closed rather than serving a mismatched asset"
  - "The /atlas route's clean-hide gate uses atlas_preview_available() (flag AND readiness), NOT os.path.exists, and early-returns before the create_atlas_page delegate — so a flag-ON/asset-missing window can never render beta chrome over a 404ing fetch"

requirements-completed: [ATLAS-01]

# Metrics
duration: 55min
completed: 2026-07-20
---

# Phase 133 Plan 03: Atlas Runtime Plumbing (flag + asset loader + /atlas route + data routes + chrome + strings) Summary

**Wired the Visual Atlas Preview server surface: an `ATLAS_PREVIEW_ENABLED` flag, a single authoritative `web/atlas_assets.py` asset-state loader whose one `atlas_preview_available()` predicate gates the `/atlas` page, the nav link, and the off-`/static` data routes; the routes serve the mutable manifest (no-cache + ETag + 304) and the immutable content-hashed asset with correct br/identity/* q-value negotiation (reachable 406); plus bilingual CLS-safe page chrome, the complete HE string set, and a 15-test flag-gating/negotiation/cache guard.**

## Performance

- **Duration:** ~55 min
- **Completed:** 2026-07-20
- **Tasks:** 3/3
- **Files modified:** 9 (3 created: web/atlas_assets.py, web/pages/atlas.py, tests/test_atlas_flag_gating.py; 6 modified)

## Accomplishments

- **Single authoritative asset-state source (`web/atlas_assets.py`).** `load_atlas_state()` reads `atlas_data/manifest.json` once at startup, loads `<asset_basename>.bin` (REQUIRED) and `<asset_basename>.bin.br` (OPTIONAL), validates the content_hash, and sets `ready=True` only on full success (fail-closed, no traceback). `atlas_preview_available()` = `ATLAS_PREVIEW_ENABLED and state.ready` is the ONE predicate the page route, nav link, and both data routes share (HIGH-2/MEDIUM-6). The asset dir is repo-root `atlas_data/` — OUTSIDE `web/static/` (HIGH-1) — and is never routed through `add_static_files`.
- **`/atlas` page route + nav (`web/main.py`).** Copies the puzzle-route shape: `page_meta('/atlas', noindex=True)` (D-16), head html, `create_layout()`, then a predicate-gated clean-hide card that early-returns before the `create_atlas_page` delegate. The nav item ("Connections Atlas" / "Beta", never "Discoveries" — Pitfall #8) is appended only under `if atlas_preview_available()`.
- **Off-static data routes.** `/atlas-data/manifest.json` serves the mutable pointer with `Cache-Control: no-cache, must-revalidate` + `ETag` + `Vary` and returns `304` on a matching `If-None-Match` (so a rebake never strands a client on a stale hash — T-133-05). `/atlas-data/{asset_name}` whitelist-compares the untrusted segment to the loaded content-hashed bin name (never a filesystem path — T-133-04), negotiates `Accept-Encoding` over br/identity/* via `_negotiate_encoding()` (honoring `br;q=0`/`identity;q=0`/`*;q=0`), serves `Content-Encoding: br` + immutable cache on the br branch, plain on identity, and a **reachable 406** when no representation is acceptable (T-133-14). Both 404 while the flag is OFF or the asset is not loaded.
- **Bilingual beta chrome (`web/pages/atlas.py`).** `create_atlas_page()` renders a Beta badge, the standing honesty banner (positions/clusters are algorithmic, not physical provenance — D-15), a one-line intro, and a **CLS-reserved fixed-height `<canvas>` container** with a clearly-documented 133-04 renderer injection point (the fetch/decode/draw contract).
- **Complete HE string set.** Every new atlas string (chrome + the D-08 interaction labels + the 133-05 teaser + error/loading copy) has a real Hebrew value in `genizah_translations.py`; reuses existing keys (Title/Shelfmark/Domain/Library/Home).
- **15 guard tests, all green.** Three-surface single-predicate AST scan; flag-OFF and flag-ON/asset-not-loaded clean-hide (create_atlas_page NOT called) + the complementary ON-ready delegation; a real-chrome render asserting the banner + CLS-reserved canvas; data-route 404 when unavailable; response-level br/identity/* negotiation incl. the reachable 406 and the brotli-absent fallback; manifest no-cache+ETag+304 + the stale-manifest transition; HE translated-value assertions; and a flag-independent `/static/atlas/*` -> 404 regression guard.
- **Docs synced.** `CLAUDE.md` + `DEVELOPER_GUIDE.md` document `ATLAS_PREVIEW_ENABLED` and `MASKING_SCAN_PATTERNS_FILE`; `docs/CODE_INDEX.md` records `web/atlas_assets.py`, `web/pages/atlas.py`, and the `/atlas` + data routes.

## Task Commits

Each task committed atomically with explicit, hunk-filtered staging (never `git add -A`):

1. **Task 1: flag + asset-state loader + /atlas route + nav + docs** — `392011a7` (feat)
2. **Task 2: data routes (manifest no-cache+ETag+304; content-hashed asset immutable/Brotli; br/identity/* negotiation)** — `9f3195e7` (feat)
3. **Task 3: page chrome + complete HE strings + flag-gating tests** — `bd1faaec` (feat)

**Plan metadata:** (this SUMMARY + STATE/ROADMAP/REQUIREMENTS update) — see final docs commit.

## Files Created/Modified

- `web/feature_flags.py` — `ATLAS_PREVIEW_ENABLED = _env_enabled("ATLAS_PREVIEW_ENABLED", False)`
- `web/atlas_assets.py` (created) — authoritative asset-state loader + predicate + accessors
- `web/main.py` — feature-flag/accessor imports, `load_atlas_state()` at startup, `/atlas` route, predicate-gated nav, `_negotiate_encoding()`, `_register_atlas_data_routes(app)`
- `web/pages/atlas.py` (created) — `create_atlas_page()` chrome + CLS-reserved canvas + 133-04 injection point
- `genizah_translations.py` — atlas HE translation block (atlas-only; discovery-deck glossary left uncommitted)
- `tests/test_atlas_flag_gating.py` (created) — 16 tests
- `CLAUDE.md`, `docs/guides/DEVELOPER_GUIDE.md`, `docs/CODE_INDEX.md` — env-var + code-index doc sync

## Decisions Made

See `key-decisions` in the frontmatter (data-route registrar for testability; direct-endpoint response inspection to sidestep httpx br auto-decode; content_hash consistency check in the loader; predicate-not-os.path.exists clean-hide gate). All are implementation-detail choices within the plan's RESOLVED decisions.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Removed the unused `ATLAS_PREVIEW_ENABLED` import from web/main.py**
- **Found during:** Task 3 (pre-commit ruff pass)
- **Issue:** The plan's interface note said to "import `ATLAS_PREVIEW_ENABLED` alongside `WEB_PUZZLE_ENABLED`" in web/main.py, but web/main.py only uses the `atlas_preview_available()` predicate (which reads the flag internally) — the raw flag import was genuinely unused, so `ruff` flagged F401, which would break CI (per CLAUDE.md "Pre-release MUST run ruff" + the F401-CI-failure memory).
- **Fix:** Dropped `, ATLAS_PREVIEW_ENABLED` from the web/main.py import line (kept `WEB_PUZZLE_ENABLED` + the `web.atlas_assets` accessors). The flag still lives in `web/feature_flags.py` and is read by `atlas_preview_available()` in `web/atlas_assets.py`.
- **Files modified:** web/main.py
- **Verification:** `python -m ruff check web/main.py` → clean; `import web.main` succeeds; the predicate + all gating still work (15 tests green).
- **Committed in:** `bd1faaec` (Task 3 commit)

---

**Total deviations:** 1 auto-fixed (1 blocking / ruff-CI).
**Impact on plan:** No scope creep — the flag is still defined and consumed; only the redundant re-import was removed to keep CI green.

## Issues Encountered

- **`git apply --cached` + Windows CRLF.** The working tree is CRLF (`core.autocrlf`), the index is LF. Staging only the atlas hunks of `web/main.py` (excluding the pre-existing R2-1 `embed` hunks) required (a) a patch-filter script that drops any hunk containing `embed`/`browse_page_route`/`R2-1` and (b) writing that filtered patch with LF bytes (Windows text-mode stdout was silently adding CR, which made the patch fail to apply against the LF index). For `genizah_translations.py` — where my atlas block and the pre-existing discovery-deck block are contiguous all-added lines with no separating context (so hunk-level filtering can't split them) — I staged via a synthetic `HEAD → HEAD+atlas` patch (`git show HEAD:… > base`, append the atlas block, `git diff --no-index`, rewrite paths, `git apply --cached`). Every commit's `git diff --cached` was verified to contain ONLY atlas content (no `_embed`/`browse_page_route`/`R2-1`/`Discovery Register`), and the staged blobs were compiled + exec'd to confirm the atlas keys present and the discovery block absent.
- **httpx auto-decodes `Content-Encoding: br`.** The TestClient eagerly Brotli-decodes the response body during `.send()` and strips the header, so it errored on synthetic `.bin.br` bytes and hid the transport assertion. Resolved by calling the route endpoints directly and inspecting the raw starlette `Response` (`.body`/`.headers`) — no httpx decode.

## Masking / Staging Discipline (binding phase rule, honored)

- Ran `python scripts/check_atlas_masking.py --scan-repo` (with `MASKING_SCAN_PATTERNS_FILE` exported) before **each** of the three task commits — all exited 0 ("no matches — clean"). The restricted corpus is referred to ONLY as `M-source` in committed files (my atlas copy uses none; one benign comment uses the `M-source` codename to note the discipline).
- The pre-existing unrelated working-tree changes (R2-1 `embed` in `web/main.py` + `web/pages/browse.py`, and the discovery-deck glossary in `genizah_translations.py`) were NEVER staged and remain dirty/unstaged after all three commits. `web/pages/browse.py` was never touched.

## Known Stubs

- **`web/pages/atlas.py` — the Canvas 2D renderer is intentionally NOT implemented here.** The page renders a CLS-reserved `<canvas id="atlas-canvas">` + a "Loading the atlas…" placeholder + a documented JS injection point. Filling in the renderer (fetch manifest → fetch content-hashed asset → decode per the frozen schema → draw + wire the D-08 interactions) is **explicitly plan 133-04's job** against the contract this plan establishes. This is a planned, documented handoff — not an accidental stub. The `/atlas` route and data routes are fully functional now; only the client-side drawing is deferred.

## User Setup Required

None for this plan. Two new env vars are documented (both optional): `ATLAS_PREVIEW_ENABLED` (default OFF — the beta stays hidden until set) and `MASKING_SCAN_PATTERNS_FILE` (dev/CI-only, for the masking scan). Production enablement (asset-first upload → flag set → restart → live smoke) is plan 133-06's deploy checkpoint.

## Next Phase Readiness

- **133-04** (JS decoder + renderer) has the exact fetch contract: `GET /atlas-data/manifest.json` (revalidated pointer, read `asset_basename`) → `GET /atlas-data/<asset_basename>.bin` (browser negotiates Brotli transparently) → decode per `docs/specs/atlas-asset-schema-v1.md` → draw into `#atlas-canvas`. The renderer injection point is marked in `web/pages/atlas.py`; all D-08 interaction labels already have HE values in `genizah_translations.py`.
- **133-05** (homepage teaser) can gate on the same `atlas_preview_available()` predicate; the teaser title + claim-free description keys are already translated.
- **133-06** (deploy checkpoint) will bake the real production asset into `atlas_data/`, set `ATLAS_PREVIEW_ENABLED=1`, restart (the startup load is authoritative — HIGH-2), and run the live render/Brotli/noindex/EN-HE/masking smoke + rollback drill.
- No blockers. The smoke-bake asset already in the gitignored `atlas_data/` makes `atlas_preview_available()` True locally when the flag is set, so 133-04 can develop against a live surface immediately.

## Self-Check: PASSED

- FOUND: `web/atlas_assets.py`
- FOUND: `web/pages/atlas.py`
- FOUND: `tests/test_atlas_flag_gating.py`
- FOUND: `.planning/phases/133-visual-atlas-preview-early-quick-win/133-03-SUMMARY.md`
- FOUND: commit `392011a7` (Task 1)
- FOUND: commit `9f3195e7` (Task 2)
- FOUND: commit `bd1faaec` (Task 3)

---
*Phase: 133-visual-atlas-preview-early-quick-win*
*Completed: 2026-07-20*
