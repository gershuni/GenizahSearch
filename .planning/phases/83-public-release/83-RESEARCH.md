# Phase 83: Public Release of Search API — Research

**Researched:** 2026-05-05
**Domain:** API public release (FastAPI/NiceGUI OpenAPI exposure, security audit, docs reframe, deploy)
**Confidence:** HIGH (most decisions locked in CONTEXT.md; one critical finding about NiceGUI's OpenAPI default)

## Summary

Phase 83 is mostly mechanical work on top of code+docs that already exist:
the three search-helper endpoints are live in production, hardened by Phase 78,
contract-locked by Phases 79/80/81A, validated end-to-end by the Phase 81B skill,
and documented internally by Phase 82. The work is (1) auditing what's there,
(2) reframing one Markdown file, (3) wiring FastAPI's auto-generated OpenAPI
spec out through NiceGUI's wrapper, (4) adding a 2-3 sentence README section,
(5) bumping version + deploying. CONTEXT.md locks 15 decisions; the planner
has very little room to invent.

The single non-obvious finding from this research:
**NiceGUI ships `app` with `docs_url=None` and `openapi_url=None` by default
— FastAPI's auto-docs are DISABLED on the NiceGUI singleton.** D-07 cannot
be implemented by "just turning on FastAPI defaults." A sub-mounted FastAPI
app at `/api` is the cleanest fix; alternatives are documented below.

**Primary recommendation:** Build Phase 83 as 5-6 plans:
(1) Security audit producing `83-SECURITY.md` BEFORE deploy,
(2) `docs/SEARCH_API.md` reframe (drop banner, add Stability/Quick Start/Attribution/Changelog),
(3) OpenAPI exposure via sub-mount at `/api` with Pydantic field descriptions,
(4) README "API" section + `skills/cairo-genizah-research/SKILL.md` link,
(5) Version bump + CHANGELOG + CLAUDE.md "Recently Changed" + deploy + close-out.
Skill smoke test gates (5). NO GitHub release for v7.10 (web+API only;
desktop polls `releases/latest` and would prompt every desktop user).

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

**Stability commitment**
- **D-01:** Soft semver, NO `/api/v1/` path prefix. Endpoints stay at current paths. Future breaking changes announced via `CHANGELOG.md` + new "Changelog" section in `docs/SEARCH_API.md`. Tied to website major-version releases (v8.0+ for next breaking window).
- **D-02:** New docs page MUST contain explicit stability statement: "We aim to keep this contract stable. Breaking changes (request shape, response envelope shape, error codes) will only ship on major website-version releases and will be announced in `CHANGELOG.md` and the `Changelog` section below. Additive changes (new optional fields, new optional request keys, new endpoints) may ship at any time." This sentence is part of the public contract.

**Security audit**
- **D-03:** Threat model + mitigation-coverage report only. Run `/gsd-secure-phase` (or spawn `gsd-security-auditor`) against v7.10 phases 78-81B + Phase 83 deploy step. Output: `83-SECURITY.md` enumerating threats and verifying mitigations.
- **D-04:** NO new mitigations preemptively. Lower default rate limit, body-size caps, abuse alerting, optional API keys all out of scope unless audit surfaces a concrete gap. If gap surfaces, planner adds remediation plan BEFORE deploy.
- **D-05:** Audit MUST cover at minimum: (a) rate-limit bypass via XFF spoofing (Phase 78 Concern #1/#4); (b) IP-hash leak via PostHog (HARDEN-05 salt rotation); (c) `SEARCH_API_MODE` env-var flip safety; (d) error-envelope info leakage (no stack traces, no raw FJMS errors); (e) `restrict_sys_ids` injection via filter values (Phase 78 R2-#3 fail-closed); (f) Responsa cap (`MAX_EXPANDED_TERMS=500`) cannot be bypassed.

**Documentation**
- **D-06:** Light reframe of `docs/SEARCH_API.md`. Drop "⚠ Internal Helper" banner. Add "Stability" section quoting D-02 verbatim. Add "Quick Start" near top with one runnable curl example per endpoint. Add "Attribution & Citation" section: requested citation format, link to credit/attribution page (or stub one in this phase if it doesn't exist), language acknowledging MiDRASH / NLI / FGP / PGP. Add "Changelog" section at bottom seeded with `## v7.10 (2026-05-XX) — Initial public release`. Existing 663 lines of reference material stay intact.
- **D-07:** Auto-generate OpenAPI spec + Swagger UI from FastAPI. Spec at `GET /api/openapi.json`. Interactive UI at `GET /api/docs`. BOTH routes EXCLUDED from rate limiter. Spec MUST cover ONLY the three search-helper endpoints — not the legacy `/api/*` image proxies, NLI proxies, puzzle uploads. Use FastAPI's `tags` + `include_in_schema` to scope. Link from `docs/SEARCH_API.md` Quick Start.
- **D-08:** OpenAPI spec correctness verified by a single test loading `/api/openapi.json` and asserting the three endpoint paths are present and legacy paths are absent. No exhaustive schema validation.

**Deploy & Surfacing**
- **D-09:** Add short "API" section to `README.md`, ENGLISH ONLY. Placement: after feature list, before "Development setup" (or wherever flows best). 2-3 sentences: what API is for (research automation), one-line per endpoint, link to `docs/SEARCH_API.md`. Hebrew translation deferred.
- **D-10:** Deploy via existing `deploy.sh master-main` to https://genizahsearch.com. No staged/canary rollout.
- **D-11:** Pre-deploy gate: pytest green + `python scripts/check_docs.py` green + cairo-genizah-research skill smoke run end-to-end against local web server (`python -m web.main` + run skill against `http://localhost:8081`). Skill IS the integration test per Phase 81B SKILL-02. NO manual curl.
- **D-12:** Rollback plan: revert deploy commit on master-main + re-deploy. Traffic-level rollback also available: `SEARCH_API_MODE=disabled` env-var flip.

**Skill Consumer Update**
- **D-13:** Update `skills/cairo-genizah-research/SKILL.md` to reference public docs URL (or relative path to `docs/SEARCH_API.md`). Pure doc change. Lands in this phase, not deferred.

**Version Bump & Release Mechanics**
- **D-14:** Run `python scripts/bump_version.py 7.10.0` (planner confirms version code against `version.py`). Update `CHANGELOG.md` with `## [7.10.0]` section summarizing v7.10 milestone (NOT just Phase 83). Update `CLAUDE.md` "Recently Changed" with one entry for v7.10. README.md "What's New" updated.
- **D-15:** GitHub release for v7.10 — **defer decision to planner**. Memory note "[Never create GitHub release for web-only version]" applies. v7.10 is web+API-only. Likely SKIP GitHub release.

### Claude's Discretion

- Exact wording of Stability statement (must capture D-02 substance, exact phrasing editorial).
- Exact placement of "API" section in `README.md`.
- Exact OpenAPI tag names + per-endpoint summary strings.
- Whether OpenAPI spec uses Pydantic's existing field descriptions or adds `description=` kwargs for missing fields.
- Threat-model report format (Markdown table vs prose) — auditor agent's call.

### Deferred Ideas (OUT OF SCOPE)

- Versioned URL path `/api/v1/...` (defer until surface expands or breaking change needed)
- Optional API key authentication (its own phase)
- README.md Hebrew translation of API section
- Multi-language code samples in `docs/SEARCH_API.md` (only curl in v7.10)
- OpenAPI-generated client SDKs
- Terms-of-service / Acceptable Use page
- PostHog public API dashboard
- Body-size cap, lower default rpm, abuse alerting (only if security audit surfaces gap)
- Long-running parallels job API (Phase 81C, deferred to v7.11)
</user_constraints>

<phase_requirements>
## Phase Requirements

Proposed requirement IDs for the planner (refine as needed):

| ID | Description | Research Support |
|----|-------------|------------------|
| PUBLIC-01 | Stability statement appears in `docs/SEARCH_API.md` per D-02 verbatim | Reframe target identified at docs/SEARCH_API.md:5-18 (existing banner block) |
| PUBLIC-02 | `83-SECURITY.md` produced and reviewed BEFORE deploy; covers D-05 items (a)-(f); no concrete gap → no new code | Phase 78 VERIFICATION.md Concerns #1-#12 already verified; audit re-checks load-bearing posture; no `*-SECURITY.md` precedent in repo (new format) |
| PUBLIC-03 | `docs/SEARCH_API.md` reframed: banner dropped, Stability + Quick Start + Attribution + Changelog sections added; existing 663 lines of contract preserved verbatim | 82-CONTRACT-AUDIT.md is the contract-preservation oracle; Phase 82 cold-reader walkthrough is the quality bar |
| PUBLIC-04 | OpenAPI spec exposed at `/api/openapi.json` + Swagger UI at `/api/docs`. Spec scoped to ONLY 3 search-helper endpoints (search, browse, parallels). Legacy `/api/*` excluded. Both routes excluded from rate limiter. Single test (D-08) asserts three paths present, legacy paths absent. | NiceGUI ships `app.docs_url=None` (verified) — naïve "turn on FastAPI defaults" does NOT work. Sub-mount or `app.setup()` re-call required. |
| PUBLIC-05 | `README.md` has English "API" section: 2-3 sentences, one-line per endpoint, link to `docs/SEARCH_API.md`. Placed after feature list. | README.md is 197 lines, English-only; clean insertion point at line ~155 (between "Additional Capabilities" and "Getting Started") |
| PUBLIC-06 | `skills/cairo-genizah-research/SKILL.md` links to public docs (relative path to `docs/SEARCH_API.md` or full URL once docs are public) | Existing references at SKILL.md lines 13-19, 41-43 currently say "the GenizahSearch APIs" — easy add |
| PUBLIC-07 | Pre-deploy gate green: `pytest tests/` + `python scripts/check_docs.py` + skill smoke (`python -m skills.cairo-genizah-research.scripts.smoke_test --base-url http://localhost:8081`) | Smoke harness exists at skills/cairo-genizah-research/scripts/smoke_test.py:1-101; runs 1 search + 1 browse + 1 parallels; exits 0 on all-pass |
| PUBLIC-08 | Version bumped to 7.10.0; `CHANGELOG.md` `## [7.10.0]` entry summarizes v7.10 milestone; `CLAUDE.md` "Recently Changed" entry; `README.md` "What's New" updated; NO GitHub release object created (web-only release rule) | bump_version.py:27-57 covers 4 files automatically; CHANGELOG/CLAUDE.md manual; gui_threads.py:459 confirms desktop polls api.github.com/.../releases/latest |
</phase_requirements>

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| API contract surface | API/Backend | — | `web/search_api.py` owns the 3 endpoints; Phase 83 doesn't add tiers |
| OpenAPI spec generation | API/Backend (FastAPI) | — | FastAPI auto-derives from Pydantic models registered on the app |
| Swagger UI rendering | Browser/Client (Swagger UI JS) | API/Backend (serves HTML+JS) | FastAPI's `/docs` serves a static HTML shell that fetches `/openapi.json` via JS |
| Stability statement | Documentation | — | Markdown in `docs/SEARCH_API.md`; not code |
| Security audit | Process artifact | — | `83-SECURITY.md` is a planning doc, not runtime |
| README API surfacing | Documentation | — | Plain Markdown |
| Skill SKILL.md update | Documentation | — | Pure doc; skill code unchanged |
| Version bump artifacts | Build/Release | — | `version.py` (source of truth) → `version_info.txt`, `.iss`, README header |
| Production deploy | Infrastructure (systemd + git pull) | — | `deploy.sh master-main` runs on EC2; restarts `genizah-web.service` |
| Pre-deploy gate | Local dev workflow | — | pytest + check_docs + skill smoke, all on the developer's machine |

## Standard Stack

### Core (already in tree, no new installs)

| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| FastAPI | (pinned in requirements-lock.txt) | HTTP framework + OpenAPI auto-gen | Underlies NiceGUI; `init_search_api(app)` already mounts on it [VERIFIED: web/main.py:163-178] |
| NiceGUI | (pinned) | Web framework wrapping FastAPI | Hosts the singleton `app` that all `/api/*` routes attach to [VERIFIED: web/main.py:26] |
| Pydantic | v2 (`ConfigDict`, `model_validator`) | Request validation + OpenAPI schema source | All 3 search-helper endpoints use Pydantic models with `extra='forbid'` [VERIFIED: web/search_api.py:100, 112, 128, 210, 228] |
| Swagger UI | bundled with FastAPI | Interactive `/api/docs` rendering | FastAPI ships it; setting `docs_url='/docs'` on a sub-app activates it [VERIFIED: locally constructed test FastAPI(docs_url='/docs') exposes `/docs`, `/openapi.json`, `/redoc`, `/docs/oauth2-redirect`] |

### Supporting (existing, untouched)

| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| `web.api_hardening` | local | RateLimiter, mode-gate, error envelope, PostHog wrapper | Already wraps all 3 endpoints; OpenAPI routes (D-07) MUST bypass it |
| `shared.api_errors` | local | `APIError` + `ERROR_CODES` registry (12 codes + 2 warnings) | Contract surface for error responses [VERIFIED: shared/api_errors.py:24-45] |
| `shared.search_serializer` | local | `serialize_search_payload`, `serialize_browse_payload`, `serialize_parallels_payload` | Phase 77/79-locked envelope; OpenAPI response models can reference these shapes [CITED: 82-CONTRACT-AUDIT.md] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| FastAPI auto-OpenAPI | Hand-written OpenAPI YAML | Hand-rolled drifts from Pydantic models — brittle. FastAPI auto-gen wins. [ASSUMED: standard FastAPI ecosystem practice] |
| Sub-mounted FastAPI app | Re-call `app.setup()` after setting `docs_url`/`openapi_url` | See "Architecture Patterns / OpenAPI exposure" below — sub-mount is cleaner. |
| Swagger UI | ReDoc | FastAPI ships both; Swagger UI is the conventional "/docs" target. ReDoc at `/redoc` is a free bonus from sub-app. |

**No new packages. No `pip install`.** [VERIFIED: all needed symbols already imported in web/search_api.py and web/api_hardening.py]

**Version verification:** Skipped — no new packages to install. The pinned FastAPI/NiceGUI/Pydantic versions in `requirements-lock.txt` already power the production server.

## Architecture Patterns

### System Architecture Diagram

```
                          Phase 83 deliverables
                          ─────────────────────

  Researcher / Developer ──► HTTPS ──► genizahsearch.com (NiceGUI + FastAPI)
                                         │
                                         ├── /api/search    ┐
                                         ├── /api/browse    ├── existing (Phase 78-81A)
                                         ├── /api/parallels ┘   wrapped by api_hardening
                                         │
                                         ├── /api/openapi.json ┐
                                         └── /api/docs         ├── NEW (D-07)
                                                                │   excluded from rate limiter
                                                                │   scoped to 3 endpoints only
                                                                ┘

  Reader / Researcher ──► HTTPS ──► docs/SEARCH_API.md (in-repo Markdown, viewed via GitHub)
                                         │
                                         ├── (NEW) Stability section ─── D-02 verbatim
                                         ├── (NEW) Quick Start ─── 3 curl examples
                                         ├── (existing 663 lines)
                                         ├── (NEW) Attribution & Citation ─── MiDRASH/NLI/FGP/PGP
                                         └── (NEW) Changelog ─── seeded with v7.10

  Skill consumer ──► local Python ──► smoke_test.py ──► localhost:8081 ─┐
                                                                         ├── pre-deploy gate (D-11)
  pytest ────────────────────────────────────────────────────────────────┤
  scripts/check_docs.py ─────────────────────────────────────────────────┘

  Production deploy ─► ssh ─► deploy.sh master-main ─► git reset --hard ─► systemctl restart
```

### Recommended Project Structure (changes only)

```
docs/
├── SEARCH_API.md                       # REFRAMED (banner dropped, sections added)
├── api-attribution.md                  # NEW (or stub) — citation guidance, MiDRASH/NLI/FGP/PGP credit
.planning/phases/83-public-release/
├── 83-RESEARCH.md                      # this file
├── 83-SECURITY.md                      # NEW per D-03 — threat model + mitigation report
├── 83-01-PLAN.md ... 83-NN-PLAN.md     # plans (planner decides count)
├── 83-VERIFICATION.md                  # close-out
README.md                               # NEW "API" section
CLAUDE.md                               # "Recently Changed" entry
CHANGELOG.md                            # ## [7.10.0] entry
version.py                              # APP_VERSION = "7.10.0"
version_info.txt, CompileScriptGenizah.iss   # touched by bump_version.py
web/search_api.py                       # OPTIONAL: add description= kwargs to Pydantic Fields
web/main.py                             # NEW lines: build sub-app, register endpoints on it,
                                        #            mount at /api, OR configure openapi_url
skills/cairo-genizah-research/SKILL.md  # add link to public docs
```

### Pattern 1: OpenAPI exposure under NiceGUI — sub-mount the search-helper API

**What:** Construct a separate `FastAPI(title=..., version=..., docs_url='/docs', openapi_url='/openapi.json')`. Move the `init_search_api(app_override=...)` call to register the 3 endpoints onto THAT sub-app instead of the NiceGUI singleton. `nicegui.app.mount('/api', sub_app)`.

**When to use:** When the host app (NiceGUI) has disabled FastAPI auto-docs and you want exactly one scoped OpenAPI surface.

**Why:** [VERIFIED: 2026-05-05] NiceGUI's `App.__init__` constructs the underlying FastAPI with `docs_url=None`, `openapi_url=None`. Direct evidence:
```python
>>> from nicegui import app
>>> app.docs_url, app.openapi_url
(None, None)
```
Naïvely calling `init_search_api(app)` (current production wiring at web/main.py:178) produces an OpenAPI-less surface. Setting these attributes post-construction does NOT regenerate FastAPI's internal docs routes; FastAPI builds them inside `__init__` and they need to exist as Route objects.

**Example (sketch):**
```python
# web/main.py — new section
from fastapi import FastAPI
from web.search_api import init_search_api

search_helper_app = FastAPI(
    title="GenizahSearch Search-Helper API",
    version="7.10.0",
    description="Public research-automation API for the Cairo Genizah corpus.",
    docs_url="/docs",
    openapi_url="/openapi.json",
)
init_search_api(app_override=search_helper_app)   # registers /api/search, /api/browse, /api/parallels onto sub-app
app.mount("/api", search_helper_app)

# Result:
#   GET  /api/openapi.json   ← spec
#   GET  /api/docs           ← Swagger UI
#   POST /api/search         ← unchanged behavior, but now mounted via sub-app
#   GET  /api/browse         ← unchanged
#   POST /api/parallels      ← unchanged
```

**CAVEAT — paths and rate limiter:**
- Sub-mount means `init_search_api` already registers routes at `/api/search` (relative to sub-app root). After mount at `/api`, the public path becomes `/api/api/search` — WRONG. The fix is one of:
  - (a) Register routes inside `init_search_api` at `/search`, `/browse`, `/parallels` when `app_override` is a sub-app — i.e., parametrize the path prefix.
  - (b) Mount the sub-app at `/` instead and register the legacy NiceGUI routes elsewhere — fragile.
  - (c) Keep the current registration paths and mount the sub-app at `''` (empty prefix) — but then `/openapi.json` is at `/openapi.json`, not `/api/openapi.json`. D-07 explicitly says `/api/openapi.json`.
- **Recommended:** Option (a). Add a `path_prefix` keyword to `init_search_api` (default `''` for backward-compat with tests; pass `''` from main.py and mount sub-app at `/api`). Plan must include a TASK to refactor the routes' string paths and update existing tests that hit `/api/search` (test fixtures register on bare apps and would still work with the same paths if the test fixture mounts at `/api`).

**Alternative pattern (if sub-mount turns out to be invasive):**

Construct a fresh `FastAPI(docs_url=..., openapi_url=...)` purely as a spec source — register copies of the routes via a thin wrapper, but DO NOT mount. Have a custom Starlette route at `/api/openapi.json` that returns `search_helper_app.openapi()` (the JSON). Have a custom route at `/api/docs` that serves `fastapi.openapi.docs.get_swagger_ui_html(openapi_url='/api/openapi.json')`. The actual `/api/search`/`/api/browse`/`/api/parallels` stay registered on the NiceGUI singleton as today (no mount). [ASSUMED: this works; FastAPI's `get_openapi()` is callable independent of mount; needs verification by the implementer]

The sub-mount path is cleaner; the dual-registration alternative is the safety net.

### Pattern 2: Excluding routes from OpenAPI

**What:** FastAPI's route decorators accept `include_in_schema=False`. Routes so marked are skipped by `app.openapi()`.

**When to use:** D-07 says the spec must NOT include legacy `/api/*` (image proxies, NLI proxies, puzzle uploads, robots.txt, sitemap.xml).

**Why this Phase 83 doesn't need it:** If we sub-mount per Pattern 1, ONLY the 3 search-helper endpoints are on the sub-app. The legacy routes stay on the NiceGUI singleton, which has no openapi.json route. No `include_in_schema=False` decoration needed on legacy routes. [VERIFIED: web/api.py legacy routes register on NiceGUI's singleton via `init_api_routes(target_app=app)` — they're NOT on the sub-app]

If the alternative (dual-spec) approach is chosen instead, then `include_in_schema=False` IS needed on every legacy route in `web/api.py` (~50+ routes including `/robots.txt`, `/sitemap.xml`, `/api/image/...`, `/api/browse_debug/{sys_id}`, etc.). Sub-mount avoids this maintenance burden.

### Pattern 3: Swagger UI excluded from rate limiter

**What:** D-07 says `/api/openapi.json` and `/api/docs` MUST NOT be rate-limited.

**Why:** Browsers loading the spec hit it once; the Swagger UI shell loads `/api/openapi.json` once. Rate-limiting them is hostile and produces confusing errors when a developer reloads the page.

**How:** Sub-mount routes are NOT decorated with `@wrap_endpoint(...)` from `web/api_hardening.py` — they're built-in FastAPI routes (created by the FastAPI constructor for `docs_url`/`openapi_url`). They naturally bypass the rate limiter without explicit work.

### Pattern 4: Pydantic field descriptions for Swagger UI quality

**What:** Pydantic `Field(default=..., description="...")` populates the OpenAPI schema's per-property `description`, which Swagger UI renders next to each input box.

**When to use:** Now. Existing models (`SearchRequest`, `BrowseRequest`, `ParallelsRequest`, `FiltersModel`, `ResponsaOptions`) currently use `Field(default=..., ge=..., le=...)` WITHOUT `description=` kwargs [VERIFIED: web/search_api.py:100-253]. Without descriptions, Swagger UI shows only the type and constraint — usable but austere.

**Effort:** ~30 fields × 1 line each. The descriptions already exist as docstrings in `docs/SEARCH_API.md` per-endpoint Request fields tables — direct copy-paste source.

**Decision (Claude's discretion per CONTEXT.md):** Recommend ADDING `description=` kwargs in the same plan that wires OpenAPI exposure. Without descriptions, `/api/docs` reads as "auto-generated junk" and undermines the public-release framing.

### Anti-Patterns to Avoid

- **Re-registering legacy `/api/*` routes on the sub-app to "centralize"** — would balloon the OpenAPI spec to 50+ paths, contradicts D-07, and risks breaking the `wrap_endpoint` rate-limit cooperation that legacy routes don't expect.
- **Using `app.openapi_url = '/api/openapi.json'` post-construction** — does NOT install the route. FastAPI builds docs routes in `__init__`. [ASSUMED based on FastAPI source familiarity; implementer should verify with a quick `grep app.routes` after the assignment]
- **Generating OpenAPI YAML by hand and committing it** — drifts from Pydantic models, defeats the auto-gen story.
- **Adding the OpenAPI spec to the sitemap.xml** — D-07 doesn't ask for this; spec consumers find it via `docs/SEARCH_API.md` Quick Start link.
- **Creating a GitHub Release object for v7.10** — desktop polls `api.github.com/repos/gershuni/GenizahSearch/releases/latest` (gui_threads.py:459) and would prompt every desktop user to "update." v7.10 is web+API-only; there's no installer. The git tag itself is fine; only the GitHub Release object triggers the prompt. [VERIFIED via grep]

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| OpenAPI spec | YAML by hand | FastAPI's `app.openapi()` from Pydantic models | Auto-derived; never drifts |
| Interactive API docs UI | Custom HTML | FastAPI's bundled Swagger UI (`docs_url='/docs'`) | Maintained upstream; supports try-it-now |
| Threat model template | Invent format | Use `gsd-secure-phase` / `gsd-security-auditor` agent | D-03 explicitly names this; no in-repo `*-SECURITY.md` precedent so auditor's chosen format is the precedent-setter |
| Smoke test for skill-side end-to-end | Custom curl script | `skills/cairo-genizah-research/scripts/smoke_test.py` (already exists) | D-11; SKILL-02 made the skill the v7.10 acceptance harness |
| Version-bump file edits | sed/find scripts | `python scripts/bump_version.py 7.10.0` | Updates 4 files; documented in CLAUDE.md |
| Doc health check | Ad-hoc grep | `python scripts/check_docs.py` | Project-standard; D-11 names it |
| Deploy mechanic | Manual ssh + git | `deploy.sh master-main` | Project-standard; runs systemd restart |

**Key insight:** Phase 83 is a low-code phase. The only meaningful new code is ~10 lines in `web/main.py` to wire the OpenAPI sub-app, plus optional `description=` kwargs on Pydantic fields. Everything else is Markdown.

## Common Pitfalls

### Pitfall 1: OpenAPI sub-app path conflicts

**What goes wrong:** Routes register at `/api/search` on the sub-app; sub-app mounts at `/api`; effective path is `/api/api/search`. Tests pass (they hit the sub-app directly); production breaks.

**Why it happens:** `init_search_api` hard-codes the `/api/...` prefix in the route string at web/search_api.py:438, 774, 898.

**How to avoid:** Parametrize the prefix in `init_search_api(app_override, path_prefix='')`. When called from production main.py, pass `path_prefix=''` and mount sub-app at `/api`. When called from existing tests (which assume routes are at `/api/search` directly on a bare FastAPI app), pass `path_prefix='/api'` for backward compat — or update tests to construct a sub-app + mount.

**Warning signs:** A `pytest tests/test_search_api.py` that still passes after the wiring change but production returns 404 on `/api/search`. Add an integration smoke task that hits `http://localhost:8081/api/search` post-wiring.

### Pitfall 2: `/api/openapi.json` includes legacy routes

**What goes wrong:** Spec contains 50+ paths including `/api/image/{...}`, `/robots.txt`, `/sitemap.xml`, `/_internal/memstat`. D-07 violated. Public docs look unprofessional.

**Why it happens:** If the implementer takes the "alternative pattern" (single FastAPI app for everything) without adding `include_in_schema=False` to every legacy route.

**How to avoid:** Take the sub-mount pattern. Verify with the D-08 test: `assert '/api/search' in spec['paths']` AND `assert '/sitemap.xml' not in spec['paths']` AND `assert '/api/image/{...}' not in spec['paths']`.

**Warning signs:** D-08 test catches this — but only if the test asserts BOTH presence AND absence of specific paths.

### Pitfall 3: Smoke test gate hits production by accident

**What goes wrong:** Skill smoke uses `GENIZAH_API_BASE` env var which D-09 says wins over CLI. Developer runs `python -m skills.cairo-genizah-research.scripts.smoke_test --base-url http://localhost:8081` but `GENIZAH_API_BASE=https://genizahsearch.com` is set — smoke runs against production, which counts against rate limits and doesn't validate local changes.

**Why it happens:** Memory note `[v7.11+ skill consumer follow-ups]` warns this is a real problem. Per CLAUDE.md the skill's `_config.py` resolves env first.

**How to avoid:** Pre-deploy gate runbook explicitly says `unset GENIZAH_API_BASE` (or `GENIZAH_API_BASE=http://localhost:8081 python -m ...`). Plan task includes the runbook.

**Warning signs:** Smoke claims pass but local server logs are empty.

### Pitfall 4: Memory `[Never launch web server from Bash]` blocks automation

**What goes wrong:** Agent attempts to run `python -m web.main &` from a Bash tool call to automate D-11 skill smoke. On Windows this creates an unkillable zombie process.

**How to avoid:** Plan documents the gate as a MANUAL step. The user starts the server in a terminal (`python -m web.main`); agent runs `pytest`, `check_docs`, and `smoke_test` in separate calls.

**Warning signs:** Plan tries to mark D-11 as automated/agent-runnable.

### Pitfall 5: deploy.sh path mismatch

**What goes wrong:** CONTEXT.md (line 91 of canonical_refs) says `scripts/deploy.sh`. Actual location is `./deploy.sh` (repo root). [VERIFIED: `git ls-files | grep deploy` returns `deploy.bat`, `deploy.sh`, `docs/guides/DEPLOYMENT_TECHNICAL.md` — no `scripts/deploy.sh`].

**How to avoid:** Plan says `./deploy.sh master-main`. Don't blindly copy CONTEXT.md path.

**Warning signs:** Deploy step in plan reads `scripts/deploy.sh` — would fail at "command not found" on the prod server.

### Pitfall 6: Stub `docs/api-attribution.md` not actually needed

**What goes wrong:** D-06 says "link to the GenizahSearch credit/attribution page (or stub one in this phase if it doesn't exist)". README.md already has a "Credits & Data" section (README.md:184-196) that lists MiDRASH (with full Zenodo DOI), PGP, FJMS. That's already the attribution page.

**How to avoid:** Attribution section in `docs/SEARCH_API.md` can link to `README.md#credits--data` (anchor-link to the existing section) rather than create a new file. Confirms the source-of-truth doesn't get duplicated.

**Warning signs:** Plan creates `docs/api-attribution.md` with content that duplicates README.md credits — drift bait.

## Runtime State Inventory

> Phase 83 is a code+docs+deploy phase, NOT a rename/refactor. State inventory is short.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | None — Phase 83 doesn't migrate any database. | None |
| Live service config | `genizah-web.service` (systemd) on EC2 hosts the app — `deploy.sh` restarts it. No config changes. | systemctl restart (already in deploy.sh) |
| OS-registered state | None — no Windows Task Scheduler / launchd / pm2. | None |
| Secrets/env vars | No new env vars introduced (D-04 forbids new mitigations preemptively). Existing `SEARCH_API_MODE`, `SEARCH_API_RATE_LIMIT`, `POSTHOG_IP_SALT`, `SEARCH_API_POSTHOG_SAMPLE_N` unchanged. | None |
| Build artifacts | `version_info.txt`, `CompileScriptGenizah.iss` rewritten by `bump_version.py 7.10.0`. Desktop installer NOT actually built or shipped (web-only release per D-15). | Run `bump_version.py`; do NOT build/ship installer |

**The canonical question:** *After every file in the repo is updated, what runtime systems still have v7.9.4 cached, stored, or registered?*
- The production EC2 instance runs whatever `master-main` HEAD is at deploy time — the `git reset --hard origin/master-main` in `deploy.sh:16` ensures no stale state.
- The desktop poll loop (`api.github.com/.../releases/latest`) caches the latest GitHub Release object — and we are deliberately NOT creating one for v7.10, so desktop sees no change.
- PostHog analytics dashboards reference event property `search_api_version` if it exists — [ASSUMED: it does not; needs grep] — if it does, we may want to bump it, otherwise it's irrelevant.

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.10+ | All | ✓ | (project standard) | — |
| FastAPI | OpenAPI generation | ✓ | pinned in requirements-lock.txt | — |
| NiceGUI | App host | ✓ | pinned | — |
| Pydantic v2 | Models + OpenAPI schema | ✓ | already in use | — |
| pytest | Pre-deploy gate | ✓ | — | — |
| `gsd-secure-phase` / `gsd-security-auditor` | Security audit (D-03) | [ASSUMED ✓] | — | Hand-write 83-SECURITY.md per planner's format |
| `gsd-review` agent | Optional doc review (per `<specifics>`) | ✓ | — | Skip review |
| EC2 ssh access | Production deploy | ✓ | — | None — release blocks if access lost |
| `gh` CLI | Optional GitHub release | ✓ | — | We are NOT creating a release for v7.10 (D-15) |

**Missing dependencies with no fallback:** None.

**Missing dependencies with fallback:** `gsd-secure-phase` agent — if unavailable, planner can produce 83-SECURITY.md as a hand-written threat model + checks-against-code table, using D-05 items (a)-(f) as the row spine.

## Code Examples

### Example 1: Sub-mount FastAPI for OpenAPI exposure (D-07)

```python
# web/main.py — proposed addition near line 178
# Source: FastAPI docs (cited from training; verified locally that
# FastAPI(docs_url='/docs', openapi_url='/openapi.json') exposes
# both routes when constructed standalone)

from fastapi import FastAPI
from web.search_api import init_search_api

# 1. Build a dedicated sub-app for the public search-helper API.
search_helper_app = FastAPI(
    title="GenizahSearch Search-Helper API",
    version="7.10.0",
    description=(
        "Public research-automation API for the Cairo Genizah corpus. "
        "Three endpoints: keyword/Responsa search, manuscript drill-down, "
        "and composition-parallels detection. See "
        "https://github.com/gershuni/GenizahSearch/blob/master-main/docs/SEARCH_API.md "
        "for the full reference."
    ),
    docs_url="/docs",          # → /api/docs after mount
    openapi_url="/openapi.json",  # → /api/openapi.json after mount
)

# 2. Register the 3 search-helper endpoints onto the SUB-app, with no /api prefix
#    (handled by the mount). Requires init_search_api to accept a path_prefix kwarg.
init_search_api(app_override=search_helper_app, path_prefix="")

# 3. Mount under /api on the NiceGUI app.
from nicegui import app
app.mount("/api", search_helper_app)
```

### Example 2: Adding `description=` to existing Pydantic Fields (Pattern 4)

```python
# web/search_api.py — proposed enhancement at line 100
# Source: Pydantic docs (Field signature) [CITED: pydantic-docs.helpmanual.io]

class FiltersModel(BaseModel):
    model_config = ConfigDict(extra='forbid')
    domains: Optional[List[str]] = Field(
        default=None,
        description="FJMS domain labels (e.g., 'Halakha', 'Piyyut'). Unknown labels → 400 unresolvable_filter_value.",
    )
    date_from: Optional[int] = Field(
        default=None,
        description="Inclusive lower bound on manuscript estimated year (CE). Combine with date_to.",
    )
    # ... and so on for ~25 more fields across the 5 models
```

### Example 3: D-08 OpenAPI scope test

```python
# tests/test_openapi_scope.py — NEW file proposed for Plan covering D-04/D-08
# Source: FastAPI's TestClient pattern [CITED: fastapi.tiangolo.com/tutorial/testing/]

from fastapi.testclient import TestClient
from web.main import app  # the NiceGUI singleton AFTER sub-mount

def test_openapi_includes_only_search_helper_endpoints():
    client = TestClient(app)
    spec = client.get("/api/openapi.json").json()
    paths = set(spec["paths"].keys())

    # Required: 3 search-helper endpoints present.
    # Note: paths are relative to the sub-app root, so '/search' not '/api/search' inside the spec.
    assert "/search" in paths
    assert "/browse" in paths
    assert "/parallels" in paths

    # Forbidden: legacy routes absent.
    legacy_forbidden = {"/robots.txt", "/sitemap.xml", "/api/image/{path:path}", "/_internal/memstat"}
    leaked = legacy_forbidden & paths
    assert not leaked, f"Legacy routes leaked into OpenAPI spec: {leaked}"

def test_swagger_ui_renders():
    client = TestClient(app)
    r = client.get("/api/docs")
    assert r.status_code == 200
    assert "swagger-ui" in r.text.lower()
```

### Example 4: Pre-deploy gate runbook (D-11)

```bash
# Manual gate — agent CANNOT automate this end-to-end
# (memory: [Never launch web server from Bash])

# Terminal A (user, foreground):
python -m web.main
# Wait for "NiceGUI ready" on http://localhost:8081

# Terminal B (agent or user, in repo root):
unset GENIZAH_API_BASE              # avoid Pitfall 3 — env wins over CLI
pytest tests/ -q                    # all green (~1400 tests)
python scripts/check_docs.py        # exit 0
python -m skills.cairo-genizah-research.scripts.smoke_test \
    --base-url http://localhost:8081
# Expect: "OVERALL: PASS"

# After all three pass:
ssh ec2 './deploy.sh master-main'
```

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Internal-undocumented `/api/*` helpers | Public-documented + OpenAPI-spec'd public surface | Phase 83 (this) | New external developers can integrate; rate limit + mode gate unchanged |
| `docs/SEARCH_API.md` with "internal helper" disclaimer | Same doc reframed; stability statement; Quick Start; Attribution; Changelog | Phase 83 | Doc serves as the public contract |
| FastAPI auto-OpenAPI disabled (NiceGUI default) | Sub-mounted FastAPI app exposes spec at `/api/openapi.json` | Phase 83 | New Swagger UI surface; no breaking changes to existing routes |

**Deprecated/outdated:**
- "⚠ Internal Helper — No Stability Promise" banner at docs/SEARCH_API.md:5-18 → replaced by Stability section.
- README.md silence on the API → English "API" section added per D-09.

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (existing project standard, ~1400 tests at HEAD) |
| Config file | `pyproject.toml` (markers `slow` and `e2e` registered per Phase 78) |
| Quick run command | `pytest tests/ -q` |
| Full suite command | `pytest tests/` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|--------------|
| PUBLIC-01 | Stability statement present in docs/SEARCH_API.md | unit (text grep) | `pytest tests/test_search_api_docs.py::test_stability_statement_present` | ❌ Wave 0 |
| PUBLIC-02 | 83-SECURITY.md exists, covers D-05 items (a)-(f) | manual (cold-reader) | (manual review by planner/reviewer) | N/A |
| PUBLIC-03 | docs/SEARCH_API.md banner removed; new sections present; existing 663 lines preserved | unit (text grep + section count) | `pytest tests/test_search_api_docs.py::test_no_internal_banner -x` and `::test_required_sections_present` | ❌ Wave 0 |
| PUBLIC-04 | OpenAPI spec scoped to 3 endpoints; Swagger UI renders; routes excluded from rate limiter | unit + integration | `pytest tests/test_openapi_scope.py -x` | ❌ Wave 0 (per D-08) |
| PUBLIC-05 | README.md has English "API" section linking to docs/SEARCH_API.md | unit (text grep) | `pytest tests/test_readme_api_section.py -x` | ❌ Wave 0 (or fold into test_search_api_docs.py) |
| PUBLIC-06 | SKILL.md links to public docs path | unit (text grep) | `pytest tests/test_skill_doc_links.py -x` | ❌ Wave 0 (or fold) |
| PUBLIC-07 | pytest + check_docs + skill smoke all pass against localhost | integration (manual gate) | sequence in Example 4 above | (uses existing harnesses) |
| PUBLIC-08 | version.py = "7.10.0", CHANGELOG.md has [7.10.0] section, CLAUDE.md "Recently Changed" entry | unit (text grep) | `pytest tests/test_release_artifacts.py -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_search_api_docs.py tests/test_openapi_scope.py -x`
- **Per wave merge:** `pytest tests/ -q` (full suite; no slow markers needed for Phase 83)
- **Phase gate:** Full suite green + `python scripts/check_docs.py` green + skill smoke green before deploy

### Wave 0 Gaps
- [ ] `tests/test_search_api_docs.py` — covers PUBLIC-01, PUBLIC-03, PUBLIC-05, PUBLIC-06 (all are content-presence/absence checks on Markdown). Single file is fine; one test per assertion.
- [ ] `tests/test_openapi_scope.py` — covers PUBLIC-04 (D-08). Two tests (Example 3 above): scope assertion + Swagger UI render check.
- [ ] `tests/test_release_artifacts.py` — covers PUBLIC-08. Reads `version.py`, `CHANGELOG.md`, `CLAUDE.md`. Three small tests.
- [ ] No new fixtures needed — existing TestClient pattern from Phase 78 tests applies.
- [ ] No framework install needed.

## Security Domain

> Per D-03/D-04, Phase 83 ITSELF doesn't add code-level mitigations — it produces a report that VERIFIES existing mitigations. The audit is a planning artifact (`83-SECURITY.md`), not a runtime change.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | no | Anonymous API by design (D-04 — API keys deferred) |
| V3 Session Management | no | Stateless API (Phase 79 D-22) |
| V4 Access Control | partial | `SEARCH_API_MODE` gate (open/localhost-only/disabled) — verified by D-05(c) |
| V5 Input Validation | yes | Pydantic `extra='forbid'` on all 3 request models (web/search_api.py:100, 112, 128, 210, 228); fail-closed filter validation in `shared/fjms_service.validate_filter_values` (Phase 78 R2-#3) |
| V6 Cryptography | partial | HMAC IP-hash for PostHog telemetry (`POSTHOG_IP_SALT`); audit verifies salt persistence per D-05(b) |
| V7 Error Handling | yes | Uniform error envelope `{"error": {"code": ..., "message": ...}}`; D-05(d) verifies no stack traces / raw FJMS strings leak |
| V11 Business Logic | yes | Rate limiter per-IP per-endpoint; `MAX_EXPANDED_TERMS=500` Responsa cap — D-05(a), D-05(f) |
| V13 API & Web Service | yes | OpenAPI spec scope (D-07); rate-limit headers; trusted-proxy XFF parsing (Phase 78 Concern #1) |

### Known Threat Patterns for {FastAPI/NiceGUI search API}

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| XFF spoofing → rate-limit bypass | Tampering | Trusted-proxy allowlist (`_TRUSTED_PROXIES`); Phase 78 Concern #1/#4 — D-05(a) re-verifies |
| Filter injection (SQL/NoSQL/Tantivy) via `filters.domains[]` | Tampering | Vocabulary-bound validation; unknown values → 400 fail-closed; Phase 78 R2-#3 — D-05(e) re-verifies |
| Information leakage via error messages | Information Disclosure | `_build_envelope_response` produces fixed-shape envelope; raw exception strings sanitized via ERROR_CODES — D-05(d) re-verifies |
| Resource exhaustion via Responsa expansion | Denial of Service | `MAX_EXPANDED_TERMS=500` cap on variant expansion — D-05(f) re-verifies |
| Telemetry leak (raw IP in PostHog) | Information Disclosure | HMAC hash with `POSTHOG_IP_SALT`; salt persistence — D-05(b) re-verifies |
| Surface activation in unintended environment | Tampering | `SEARCH_API_MODE` env var (open/localhost-only/disabled) checked per request — D-05(c) re-verifies |
| OpenAPI spec leaks internal routes | Information Disclosure | Sub-mount scopes spec to 3 endpoints; D-08 test enforces — NEW in Phase 83 |
| OpenAPI Swagger UI try-it-now floods rate limit | DoS | UI fetches spec once; spec route excluded from rate limiter — NEW in Phase 83 |

## Project Constraints (from CLAUDE.md)

- **Documentation maintenance:** Update `docs/OPEN_ISSUES.md` if any issue is fixed or discovered during Phase 83 (none expected — phase is doc/release work).
- **Version bumping:** Run `python scripts/bump_version.py 7.10.0`. Manual updates: `CHANGELOG.md`, `CLAUDE.md` "Recently Changed", `README.md` "What's New" section.
- **Pre-finish doc check:** Run `python scripts/check_docs.py` before deploy (D-11 names this).
- **Outdated terms to avoid:** Don't introduce `FastAPI backend server`, `genizah-backend`, `DATABASE_URL`, `port 8000` — these are removed-feature terms. Phase 83 docs are about the FastAPI routes embedded inside NiceGUI, NOT the legacy genizah-backend service.
- **Both-apps maintenance:** Phase 83 is web-only — doesn't trigger desktop changes. The "both apps must be maintained" rule doesn't bite here because there's no behavioral change either app needs to mirror.
- **Hebrew/RTL:** README API section is English-only per D-09 — does NOT need translation.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | FastAPI's `app.openapi()` is callable on a sub-app independent of mount, so the "alternative pattern" (custom Starlette routes serving spec) would work | Architecture Patterns / Pattern 1 caveat | LOW — sub-mount is the recommended path; alternative is fallback |
| A2 | Setting `app.openapi_url='/api/openapi.json'` post-construction does NOT install the route | Anti-patterns | LOW — the recommendation is sub-mount, not post-set; if someone tries the post-set approach, they'll discover it doesn't work in 5 minutes |
| A3 | PostHog analytics has no `search_api_version` event property that needs bumping | Runtime State Inventory | LOW — easy grep to confirm; no breakage if wrong, just confusing dashboards |
| A4 | `gsd-secure-phase` / `gsd-security-auditor` agents are available in the GSD environment | Environment Availability | LOW — fallback documented (hand-write threat model) |
| A5 | The Pydantic Fields in web/search_api.py:100-253 currently lack `description=` kwargs | Pattern 4 | VERIFIED via grep: `Field(default=...)`, `Field(default=5, ge=2, le=20)`, etc. — no `description=` anywhere. NOT actually an assumption. (Promoted out of this table.) |
| A6 | The existing 663 lines of `docs/SEARCH_API.md` are exactly the contract Phase 82 cold-reader-validated | Reframe scope | LOW — 82-CONTRACT-AUDIT.md is the authoritative oracle; planner reads it before reframing |
| A7 | Existing `init_search_api` route paths (`'/api/search'`, `'/api/browse'`, `'/api/parallels'`) are hard-coded as string literals at lines 438, 774, 898 | Pitfall 1 fix | VERIFIED via grep: `@target_app.post('/api/search')` etc. — NOT actually an assumption. (Promoted out.) |

## Open Questions

1. **Sub-app path-prefix refactor scope.** `init_search_api` currently registers at `/api/search` etc. To sub-mount at `/api`, the routes need to become `/search`, `/browse`, `/parallels` (paths relative to sub-app). This breaks any test that calls `init_search_api(app_override=bare_app)` and then hits `/api/search` on `bare_app`.
   - What we know: `tests/test_search_api.py`, `tests/test_api_hardening.py` use `app_override=bare_app` patterns (per Phase 78 VERIFICATION).
   - What's unclear: How many test files hit `/api/search` directly vs through a fixture that abstracts the path.
   - Recommendation: Plan task `83-NN: Refactor init_search_api to accept path_prefix` — first task in the OpenAPI plan. Update tests to use `path_prefix='/api'` when registering on bare apps (preserves existing test URLs). Production main.py uses `path_prefix=''` and mounts at `/api`.

2. **`docs/api-attribution.md` vs link to existing README "Credits & Data".** D-06 says "link to the GenizahSearch credit/attribution page (or stub one in this phase if it doesn't exist)". README.md:184-196 already lists MiDRASH/PGP/FJMS with full citation (Stoekl Ben Ezra et al. 2025 Zenodo DOI).
   - Recommendation: Link to `README.md#credits--data` from `docs/SEARCH_API.md` Attribution section. Don't create `docs/api-attribution.md`. Less drift surface.
   - If user wants a separate page (planner asks during plan-checker), the stub is 30 lines of Markdown duplicating README.

3. **`scripts/deploy.sh` vs `./deploy.sh`.** CONTEXT.md canonical-refs line 91 lists `scripts/deploy.sh`. Actual location is `./deploy.sh` (verified via `git ls-files`).
   - Recommendation: Plan uses `./deploy.sh master-main`. Don't propagate the canonical-refs typo.

4. **Whether `/redoc` should also be public.** FastAPI's sub-app construction with `docs_url`/`openapi_url` ALSO mounts `/redoc` and `/docs/oauth2-redirect` by default (verified locally — 4 routes appear). D-07 only names `/api/docs` and `/api/openapi.json`.
   - Recommendation: Leave `/api/redoc` enabled (free; ReDoc is sometimes preferred by API consumers). Disable `/api/docs/oauth2-redirect` is moot — there's no OAuth in v7.10.
   - Planner can override by setting `redoc_url=None` on sub-app construction.

5. **Whether to add a CHANGELOG section to `docs/SEARCH_API.md` ALSO at the top of the file or only at the bottom.** D-06 says "Add a 'Changelog' section at the bottom seeded with `## v7.10 ...`". Convention varies.
   - Recommendation: Bottom only, per D-06. Adding a top-of-doc summary risks duplication and drift.

## Sources

### Primary (HIGH confidence)
- `web/search_api.py` lines 100-253, 407-432, 438, 774, 898 — Pydantic models, init function, all 3 route registrations [VERIFIED]
- `web/api.py` lines 175-188 — legacy route registrar (`init_api_routes`) [VERIFIED]
- `web/main.py` lines 163-178 — current wiring [VERIFIED]
- `shared/api_errors.py` complete file — APIError + ERROR_CODES [VERIFIED]
- `.planning/phases/82-internal-documentation/82-CONTRACT-AUDIT.md` — locked contract from Phase 82 [VERIFIED]
- `.planning/phases/78-api-search-hardening-shell/78-VERIFICATION.md` — Concerns #1-#12 mitigation table [VERIFIED]
- `skills/cairo-genizah-research/scripts/smoke_test.py` complete file — pre-deploy gate harness [VERIFIED]
- `deploy.sh` (repo root, NOT `scripts/`) — production deploy mechanic [VERIFIED]
- `scripts/bump_version.py` — version bump tool [VERIFIED]
- Live Python introspection: `from nicegui import app; app.docs_url, app.openapi_url` returned `(None, None)` [VERIFIED 2026-05-05]
- `gui_threads.py:459` — desktop polls `https://api.github.com/repos/gershuni/GenizahSearch/releases/latest` [VERIFIED via grep]

### Secondary (MEDIUM confidence)
- FastAPI sub-app construction behavior (`docs_url='/docs'` produces `/openapi.json`, `/docs`, `/redoc`, `/docs/oauth2-redirect`) [VERIFIED via local construction]
- CHANGELOG.md format (top entry is `## [version] - title - YYYY-MM-DD`, sub-headings `### Bug Fixes`, `### Internal`) [VERIFIED]
- README.md structure (header → What's New → Core Features → Additional Capabilities → Getting Started → Documentation → Credits & Data) [VERIFIED]

### Tertiary (LOW confidence — flag for validation)
- A1 (alternative dual-spec pattern works) — fallback only; sub-mount is primary recommendation
- The audit format precedent: no in-repo `*-SECURITY.md` exists → planner+auditor agree on format

## Metadata

**Confidence breakdown:**
- Locked decisions (D-01 through D-15): HIGH — copied from CONTEXT.md verbatim
- Standard stack: HIGH — all dependencies in tree
- OpenAPI exposure mechanism: MEDIUM-HIGH — sub-mount approach verified via local Python introspection; the path-prefix refactor in `init_search_api` is the only meaningful unknown
- Security audit format: MEDIUM — no precedent; auditor agent decides
- Pitfalls: HIGH — 6 pitfalls all backed by file:line evidence or verified env

**Research date:** 2026-05-05
**Valid until:** 2026-06-05 (~30 days; FastAPI/NiceGUI versions stable; v7.10 milestone is mid-deploy)

---

## RESEARCH COMPLETE

**Phase:** 83 - Public Release of Search API
**Confidence:** HIGH

### Key Findings

1. **NiceGUI ships with FastAPI auto-docs DISABLED** (`docs_url=None`, `openapi_url=None` on the singleton `app`). D-07 cannot be implemented by setting attributes; requires either a sub-mounted FastAPI app at `/api` (recommended) or a custom dual-spec setup. Sub-mount approach requires refactoring `init_search_api` to accept a `path_prefix` parameter so its hard-coded `/api/search` literals become `/search` (relative to sub-app).
2. **All 3 search-helper endpoints are already registered via the same `init_search_api()` function** (web/search_api.py:438, 774, 898). One refactor point. Legacy `/api/*` routes (image proxies, robots.txt, sitemap.xml, ~50+) live in `web/api.py` via `init_api_routes()` — they stay on the NiceGUI singleton, naturally excluded from the sub-app's OpenAPI spec.
3. **Pydantic models lack `description=` kwargs** — adding them is recommended (Claude's discretion per CONTEXT.md) for Swagger UI quality. Source text already exists in `docs/SEARCH_API.md` field tables.
4. **`scripts/deploy.sh` is wrong** — actual path is `./deploy.sh` (repo root). Don't propagate the CONTEXT.md typo.
5. **Skip GitHub release** for v7.10. Desktop polls `api.github.com/.../releases/latest` (verified gui_threads.py:459); a release object would prompt every desktop user to "update" to a web-only no-installer release. Memory note `[Never create GitHub release for web-only version]` applies. Plan documents this decision per D-15.
6. **Pre-deploy gate (D-11) is a manual sequence** — `[Never launch web server from Bash]` memory blocks agent automation. Plan documents the runbook (Example 4) including the `unset GENIZAH_API_BASE` step to avoid accidentally smoke-testing production.

### File Created

`.planning/phases/83-public-release/83-RESEARCH.md`

### Confidence Assessment

| Area | Level | Reason |
|------|-------|--------|
| Standard Stack | HIGH | All deps in tree; verified via Python introspection |
| Architecture (OpenAPI exposure) | MEDIUM-HIGH | Sub-mount path verified locally; init_search_api refactor scope is the only unknown |
| Security Audit | MEDIUM | No precedent format in repo; auditor agent decides |
| Pitfalls | HIGH | All 6 backed by file:line evidence |
| Validation Architecture | HIGH | Existing pytest infrastructure; 3 small new test files |

### Open Questions

See "Open Questions" section above (5 items). The two highest-impact:
- Q1: Refactor `init_search_api` to accept `path_prefix` — first task of the OpenAPI plan; small but invasive across test files.
- Q2: `docs/api-attribution.md` stub vs link to existing `README.md#credits--data` — recommend link, no new file.

### Ready for Planning

Research complete. Planner can now create PLAN.md files. Suggested 5-plan structure:
1. **83-01: Security audit** — produce `83-SECURITY.md` covering D-05(a)-(f). Spawn `gsd-security-auditor` or hand-write.
2. **83-02: Docs reframe** — `docs/SEARCH_API.md` banner removal + Stability/Quick Start/Attribution/Changelog sections. Tests for content presence/absence.
3. **83-03: OpenAPI exposure** — `init_search_api` path_prefix refactor + sub-app construction in `web/main.py` + Pydantic `description=` kwargs + D-08 scope test.
4. **83-04: README + skill doc updates** — README "API" section (English) + `skills/cairo-genizah-research/SKILL.md` link to public docs.
5. **83-05: Release** — `bump_version.py 7.10.0` + CHANGELOG.md `[7.10.0]` + CLAUDE.md "Recently Changed" + `README.md` "What's New" + pre-deploy gate (manual) + `./deploy.sh master-main` + close-out commit. NO GitHub release.
