# Phase 83: Public Release of Search API - Context

**Gathered:** 2026-05-05
**Status:** Ready for planning

<domain>
## Phase Boundary

Promote the v7.10 search-helper endpoints (`POST /api/search`, `GET /api/browse`, `POST /api/parallels`) from internal-undocumented to publicly documented and supported. Four work streams:

1. Security audit of the existing public posture (rate limit, IP exposure, abuse vectors, mode gate, error envelope, PostHog observability) — produced as a threat-model + mitigation-coverage report.
2. Reframe `docs/SEARCH_API.md` from internal-only to public-facing — drop the "internal helper, no stability promise" disclaimer, add a stability statement, a Quick Start, an Attribution & Citation section.
3. Auto-generate and expose an OpenAPI spec + Swagger UI (FastAPI built-in) at `/api/docs` and `/api/openapi.json`.
4. Deploy `master-main` to production and add a short English "API" section to `README.md` linking to the public docs.

Scope does NOT include: new endpoints, new shape changes, new auth (API keys), versioned path prefix (`/api/v1/`), Hebrew translation of the README API section, OpenAPI clients/SDKs, terms-of-service page. Those are deferred.

</domain>

<decisions>
## Implementation Decisions

### Stability Commitment

- **D-01:** Soft semver, **no path version**. Endpoints stay at `/api/search`, `/api/browse`, `/api/parallels` — no `/api/v1/` prefix introduced in this phase. Future breaking changes announced via `CHANGELOG.md` and a new "Changelog" section in `docs/SEARCH_API.md`. Tied to website major-version releases (v8.0+ for the next breaking window). Rationale: small surface (3 endpoints, Phase 81A-locked shape), one internal consumer today, no observed external developer demand — `/v1/` premature. Room to add `/v1/` later if the surface expands.
- **D-02:** The new docs page MUST contain an explicit stability statement that captures D-01: "We aim to keep this contract stable. Breaking changes (request shape, response envelope shape, error codes) will only ship on major website-version releases and will be announced in `CHANGELOG.md` and the `Changelog` section below. Additive changes (new optional fields, new optional request keys, new endpoints) may ship at any time." This sentence is part of the public contract — replan if the user wants to reword it.

### Security Audit

- **D-03:** Threat model + mitigation-coverage report only. Run `/gsd-secure-phase` (or spawn `gsd-security-auditor`) against the v7.10 phases (Phases 78–81B) plus the Phase 83 deploy step. Output: `83-SECURITY.md` enumerating threats and verifying mitigations exist in code.
- **D-04:** No new mitigations are added preemptively. Lower default rate limit, body-size caps, abuse alerting, optional API keys — all out of scope unless the audit surfaces a concrete gap. If the audit surfaces a gap, the planner adds a remediation plan to this phase BEFORE deploy. Audit findings drive scope; speculative hardening does not.
- **D-05:** Audit MUST cover at minimum: (a) rate-limit bypass (XFF spoofing — Phase 78 Concern #1/#4 already addressed; verify the mitigation is still load-bearing), (b) IP-hash leak via PostHog (Phase 78 HARDEN-05 — verify salt is rotated/persisted), (c) `SEARCH_API_MODE` env-var flip safety, (d) error-envelope info leakage (no stack traces, no raw FJMS errors in `message`), (e) `restrict_sys_ids` injection via filter values (Phase 78 R2-#3 fail-closed — verify), (f) Responsa cap (`MAX_EXPANDED_TERMS=500`) cannot be bypassed by adversarial query.

### Documentation

- **D-06:** Light reframe + OpenAPI spec.
  - Drop the existing "⚠ Internal Helper — No Stability Promise" banner from `docs/SEARCH_API.md`.
  - Replace with a "Stability" section quoting D-02 verbatim.
  - Add a "Quick Start" section near the top with one runnable curl example per endpoint (search, browse, parallels) — minimal payloads, expected response shape excerpt.
  - Add an "Attribution & Citation" section: requested citation format for academic use, link to the GenizahSearch credit/attribution page (or stub one in this phase if it doesn't exist), language acknowledging MiDRASH / NLI / FGP / PGP upstream data sources.
  - Add a "Changelog" section at the bottom seeded with: `## v7.10 (2026-05-XX) — Initial public release` + bullet "Endpoints `/api/search`, `/api/browse`, `/api/parallels` promoted from internal to public per Phase 83."
  - Existing 663 lines of reference material (per-endpoint payload shapes, env vars, error codes, examples) stay intact — Phase 82 cold-reader-validated them already.
- **D-07:** Auto-generate OpenAPI spec + Swagger UI from FastAPI.
  - Spec: `GET /api/openapi.json` (FastAPI default; verify the existing NiceGUI mount doesn't already claim `/openapi.json`).
  - Interactive UI: `GET /api/docs` (FastAPI's built-in Swagger UI).
  - Both routes EXCLUDED from the rate limiter (they serve static-ish metadata; rate-limiting browsers loading the spec is hostile).
  - The spec MUST cover only the three search-helper endpoints (`/api/search`, `/api/browse`, `/api/parallels`) — not the legacy `/api/*` image proxies, NLI proxies, puzzle uploads. Use FastAPI's `tags` + `include_in_schema` to scope.
  - Link from `docs/SEARCH_API.md` Quick Start.
- **D-08:** OpenAPI spec correctness is verified by a single test that loads `/api/openapi.json` and asserts the three endpoint paths are present and the legacy paths are absent. No exhaustive schema validation — FastAPI's auto-generation is trusted.

### Deploy & Surfacing

- **D-09:** Add a short "API" section to `README.md`, English only. Placement: after the existing feature list, before "Development setup" (or wherever the structural fit is — planner verifies). 2–3 sentences: what the API is for (research automation), one-line per endpoint, link to `docs/SEARCH_API.md`. Hebrew translation deferred (see Deferred Ideas).
- **D-10:** Deploy via existing `scripts/deploy.sh master-main` to production server (https://genizahsearch.com). No staged/canary rollout — the API is already live on master-main as internal/undocumented; this deploy is a code+docs deploy, not a feature flag flip.
- **D-11:** Pre-deploy gate: pytest green + `python scripts/check_docs.py` green + cairo-genizah-research skill smoke run end-to-end against the local web server (`python -m web.main` + run skill against `http://localhost:8081`). The skill IS the integration test per Phase 81B SKILL-02 (locked as v7.10 acceptance harness). Manual curl verification is redundant.
- **D-12:** Rollback plan: if the deploy fails or the security audit uncovers a gap post-deploy, revert the deploy commit (`git revert <sha>` on master-main) and re-deploy. The rate-limit + mode-gate posture means traffic-level rollback is also available without a code change: `SEARCH_API_MODE=disabled` env-var flip on the production server kills the public surface in seconds.

### Skill Consumer Update

- **D-13:** Update `skills/cairo-genizah-research/SKILL.md` to reference the public docs URL (or the relative path to `docs/SEARCH_API.md`) once the docs land. Pure doc change. No code change to the skill — its base URL stays `https://genizahsearch.com` per Phase 81B SKILL-01. Update lands in this phase, not deferred.

### Version Bump & Release Mechanics

- **D-14:** This phase ships under the next semver bump. Run `python scripts/bump_version.py 7.10.0` (or whatever the next version code is — planner confirms against `version.py`). Update `CHANGELOG.md` with a `## [7.10.0]` section summarizing the v7.10 milestone (search-helper API public release) — not just Phase 83. Update `CLAUDE.md` "Recently Changed" with one entry for v7.10 covering the milestone. README.md "What's New" updated.
- **D-15:** GitHub release for v7.10 — **defer the decision to the planner**. Memory note "[Never create GitHub release for web-only version]" applies to web-only releases. v7.10 is web+API-only (no desktop changes). Planner decides whether the desktop-prompt-loop concern still applies (likely yes — desktop polls `/releases/latest`); if so, deploy web only and skip the GitHub release. Document the decision in the plan.

### Claude's Discretion

- Exact wording of the Stability statement in `docs/SEARCH_API.md` — must capture D-02 substance, exact phrasing is editorial.
- Exact placement of the "API" section in `README.md` — wherever flows best.
- Exact OpenAPI tag names + per-endpoint summary strings (FastAPI auto-derives from docstrings; planner can adjust).
- Whether the OpenAPI spec uses Pydantic's existing field descriptions (Phase 78–81A models) or adds `description=` kwargs for missing fields — planner decides based on what reads cleanly in `/api/docs`.
- Threat-model report format (Markdown table vs prose) — auditor agent's call.

### Folded Todos

None. The pending todos in STATE.md ("Migrate desktop corrections fetch", "CUT-01 Remove read-only PGP tables", "Date range filter", "Creation type filter") are out of scope for the API public-release work.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### The reframe target & deploy artifacts
- `docs/SEARCH_API.md` — Existing 663-line internal API reference; Phase 82 cold-reader-validated. THE primary edit target.
- `README.md` — Add short English "API" section; respect existing structure (197 lines, English-only today).
- `CHANGELOG.md` — Add v7.10 entry; this is also where future breaking-change announcements will live per D-01.
- `CLAUDE.md` — Update "Recently Changed" + env-var section if any new env vars added (none expected).
- `scripts/deploy.sh` — Deploy mechanic; verify no changes needed.
- `scripts/bump_version.py` — Version bump tool; per CLAUDE.md "Version Bumping" section.
- `scripts/check_docs.py` — Pre-deploy doc-health gate.
- `version.py` — Source of truth for `APP_VERSION`.

### The API surface being made public
- `web/search_api.py` — `POST /api/search` handler + `SearchRequest` Pydantic model + `FiltersModel` + `ResponsaOptions`. Phase 81A locked the request shape.
- `web/api_hardening.py` — RateLimiter, mode-gate, error-envelope `_build_envelope_response`, PostHog `capture_api_event`, `wrap_endpoint`. Audit MUST verify Phase 78 Concerns #1, #3, #4, #5, #9 mitigations remain load-bearing.
- `shared/api_errors.py` — `APIError` + `ERROR_CODES` registry. Audit verifies error envelope leaks no stack traces / raw FJMS strings.
- `shared/search_serializer.py` — Phase 77/79-locked envelope shape (`SCHEMA_VERSION=1`, `serialize_search_payload`, `serialize_parallels_payload`, `serialize_browse_payload`). Stability promise (D-01) covers this shape.
- `shared/browse_service.py` — `fetch_browse_bundle`; Phase 79 D-22 statelessness contract.
- `web/api.py` — Hosts `/api/browse` and `/api/parallels` handlers (legacy `/api/*` routes also live here — audit must NOT touch them).

### Phase 81A request shape (locked)
- `.planning/phases/81A-api-contract-expansion/81A-VERIFICATION.md` — 8/8 must-haves verified; defines the `search_mode` enum, `responsa_options`, `request` echo block, 100-result cap, regex 256-char cap.
- `.planning/phases/81A-api-contract-expansion/81A-CONTEXT.md` — request-shape decisions.

### Phase 78 hardening (audit baseline)
- `.planning/phases/78-api-search-hardening-shell/78-VERIFICATION.md` — Concerns #1–#12 mitigation table. Audit re-verifies these are still in code.
- `.planning/phases/78-api-search-hardening-shell/78-CONTEXT.md` — original hardening decisions.

### Phase 79 (drill-down) and 80 (parallels)
- `.planning/phases/79-api-browse-drill-down/79-VERIFICATION.md` — D-10 `text_source` enum (`pgp_transcription`/`snippet`/`none`), drill-down locator contract.
- `.planning/phases/80-api-parallels/80-VERIFICATION.md` — `/api/parallels` 4/4 SCs, request-echo block.

### Skill consumer
- `skills/cairo-genizah-research/SKILL.md` — Update to point at public docs (D-13).
- `.planning/phases/81B-claude-skill-consumer/VERIFICATION.md` — 6/6 requirements verified; SKILL-02 made the skill v7.10's acceptance harness (D-11 leans on this).
- `.planning/phases/81B-claude-skill-consumer/ACCEPTANCE-RUN.md` — Approved-with-notes; informs whether anything needs follow-up before public.

### Phase 82 docs (Phase 83 reframes these)
- `.planning/phases/82-internal-documentation/82-04-SUMMARY.md` — cold-reader walkthrough acceptance + 2 inline gaps fixed (Responsa query-string syntax table; inline-alternation row removed).
- `.planning/phases/82-internal-documentation/82-CONTRACT-AUDIT.md` — locks the docs↔code contract Phase 83 must preserve when reframing.

### Project-level
- `.planning/REQUIREMENTS.md` §v7.10 — All API-*, EXPORT-*, HARDEN-*, SKILL-*, DOC-* requirements; Phase 83 closes the milestone by promoting the surface DOC-01/DOC-02 documented internally.
- `.planning/PROJECT.md` — Vision, principles.
- `.planning/STATE.md` — Phase queue, Phase 82 closure, current position.
- `.planning/ROADMAP.md` §"Phase 83: Public Release of Search API" — phase definition + dependency on Phase 82.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- **FastAPI auto-OpenAPI**: FastAPI already builds an OpenAPI spec from the registered routes + Pydantic models. `init_search_api(app)` already wires the search endpoint with full Pydantic request/response models (Phase 78 Plan 03). `/api/browse` and `/api/parallels` use Pydantic models too. Spec generation is essentially "set the right `tags=` and `include_in_schema=False` on legacy routes; mount `/docs` and `/openapi.json`." Estimated: half a day.
- **Existing error envelope** at `shared/api_errors.py` is consistent — OpenAPI examples can reference `ERROR_CODES` keys directly.
- **`SEARCH_API_MODE=disabled` env-var flip** is a working kill switch (Phase 78 HARDEN-04). D-12 rollback plan leans on this.
- **PostHog observability** from Phase 78 HARDEN-05 already captures every search-helper request — no need to add monitoring before public.
- **scripts/deploy.sh** is the project-standard deploy. Used for every prior release.
- **scripts/check_docs.py** is the project-standard doc-health check.

### Established Patterns
- **Phase 82's cold-reader walkthrough** is the doc-quality gate that already validated `docs/SEARCH_API.md`. Phase 83's reframing should preserve that quality — reference Phase 82's review for what "good" looks like.
- **CHANGELOG.md + CLAUDE.md "Recently Changed"** entries are the project's release-note pattern. Stability promise (D-01) reuses this same channel for breaking-change announcements.
- **Bilingual translations** are added to `genizah_translations.py` for UI strings — README.md API section is doc-level, not UI, so `genizah_translations.py` does NOT apply here.

### Integration Points
- **`web/api.py` and NiceGUI app mount**: `/api/openapi.json` and `/api/docs` need to mount on the same FastAPI app instance that hosts the search-helper endpoints. Verify the existing NiceGUI integration doesn't claim those paths.
- **`master-main` branch**: deploy target. `git status` at session start shows `M .planning/ROADMAP.md` and `M .planning/STATE.md` (uncommitted state from this phase's setup) — these need to commit cleanly before any deploy.

</code_context>

<specifics>
## Specific Ideas

- The Stability section in `docs/SEARCH_API.md` should explicitly call out that **additive changes** (new optional request keys, new optional response fields, new endpoints) may ship at any time. This is the contract that lets Phase 81A-style expansions ship without violating the public promise.
- Per memory `[External review via Gemini and Codex CLIs]`: consider running `/gsd-review` on the reframed `docs/SEARCH_API.md` before deploy — fresh eyes on the public-facing copy. Planner's call.
- Per memory `[Never launch web server from Bash]`: the skill smoke gate (D-11) needs the user to start the web server — skill cannot be auto-tested by the agent. Planner builds this as a manual step in the plan.
- Per memory `[Outreach strategy and positioning]`: the Attribution section should credit MiDRASH (transcriptions), NLI (images/manifests), FGP/Friedberg (catalog), PGP (PGP descriptions). User cares about this.
- Per memory `[v7.10 public API release follow-up]`: this phase IS that follow-up. Marking the memory item closed once Phase 83 ships should land in the close-out commit.

</specifics>

<deferred>
## Deferred Ideas

- **Versioned URL path `/api/v1/...`** — defer until the surface expands or a breaking change is needed. D-01 leaves room to introduce `/v1/` later. Note for v7.11+ planning.
- **Optional API key authentication** — would be its own phase. Anonymous + rate-limited is the v7.10 posture; tighter access controls only if abuse appears.
- **README.md Hebrew translation of the API section** — defer until the README itself is translated more broadly. Single-section translation would be the first Hebrew block in the file.
- **Multi-language code samples (Python, JS) in `docs/SEARCH_API.md`** — defer until external-developer demand is observed. Curl examples (D-06) suffice for v7.10.
- **OpenAPI-generated client SDKs** — out of scope. Spec is published; community can generate.
- **Terms-of-service / Acceptable Use page** — out of scope for v7.10. Attribution + citation in `docs/SEARCH_API.md` is the lightweight version.
- **PostHog public API dashboard** — already-existing PostHog HARDEN-05 events are sufficient for monitoring; building a polished dashboard is operational work, not a release blocker.
- **Body-size cap, lower default rpm, abuse alerting** — only add if Phase 83's security audit (D-03) surfaces a concrete gap. Otherwise deferred until observed need.
- **Long-running parallels job API (Phase 81C)** — already explicitly deferred to v7.11 per ROADMAP.md.

### Reviewed Todos (not folded)

None — no pending todos in STATE.md were considered relevant to API public release.

</deferred>

---

*Phase: 83-public-release*
*Context gathered: 2026-05-05*
