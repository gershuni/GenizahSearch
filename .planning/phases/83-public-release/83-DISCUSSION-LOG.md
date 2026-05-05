# Phase 83: Public Release of Search API - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in 83-CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-05
**Phase:** 83-public-release
**Areas discussed:** Stability commitment, Security audit scope, Docs reframing depth, README placement & deploy mechanics

---

## Gray Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Stability commitment | Versioning, semver promise, breaking-change channel | ✓ |
| Security audit scope | Threat-model only vs add new mitigations vs API keys | ✓ |
| Docs reframing depth | Light reframe vs OpenAPI vs full rewrite | ✓ |
| README placement & deploy mechanics | Section placement, bilingual, canary, dashboard | ✓ |

**User's choice:** All four.

---

## Stability commitment

| Option | Description | Selected |
|--------|-------------|----------|
| Versioned path `/api/v1/` | Mount endpoints under `/api/v1/`; aliases for 90 days; future breakage as `/v2/` | |
| Soft semver, no path version | Keep current paths; breaking changes only on major releases; announce in CHANGELOG | ✓ (recommended) |
| Best-effort, no promise | Drop disclaimer; "as-is for research use; reserve right to change" | |
| You decide (recommend Soft semver) | — | ✓ |

**User's choice:** "You decide (recommend default)" → Soft semver, no path version.
**Notes:** Locked: keep `/api/search`, `/api/browse`, `/api/parallels` paths; future breaking changes on major website releases announced in CHANGELOG.md + new Changelog section in SEARCH_API.md.

---

## Security audit scope

| Option | Description | Selected |
|--------|-------------|----------|
| Threat model + mitigation report only | Run /gsd-secure-phase; verify Phase 78 mitigations; no new mitigations added preemptively | ✓ |
| Audit + tighten defaults | Lower rpm, body-size cap, abuse alerting | |
| Audit + add API key option | Anonymous + keyed tiers | |
| You decide (recommend Audit + tighten) | — | |

**User's choice:** "Threat model + mitigation report only".
**Notes:** Audit is gating but adds no new mitigations unless a concrete gap is found. If audit finds a gap, planner adds remediation BEFORE deploy.

---

## Docs reframing depth

| Option | Description | Selected |
|--------|-------------|----------|
| Light reframe | Drop disclaimer, add Quick Start, Attribution; preserve existing 663 lines | |
| Light reframe + OpenAPI spec | Above + auto-generated `/api/openapi.json` and Swagger UI at `/api/docs` | ✓ (recommended) |
| Full public-API rewrite | Restructure into Reference + Guides; multi-language code samples; ToS page | |
| You decide (recommend Light + OpenAPI) | — | ✓ |

**User's choice:** "You decide (recommend Light + OpenAPI)".
**Notes:** FastAPI auto-generates OpenAPI from registered Pydantic models; near-free signal of "real public API". Heavier rewrite premature without observed external demand.

---

## README placement & deploy mechanics

| Option | Description | Selected |
|--------|-------------|----------|
| README link + standard deploy | New "API" section + scripts/deploy.sh master-main + pytest/skill smoke gate | ✓ |
| README link + canary period | Deploy first WITHOUT README link; watch PostHog 7 days; then add link | |
| Dashboard + 24h clean-traffic gate | Build PostHog dashboard; alerts; only link after 24h clean | |
| You decide (recommend canary) | — | |

**User's choice:** "README link + standard deploy".
**Notes:** No canary; the API is already live on master-main as internal/undocumented, so this is a code+docs deploy not a feature flip.

---

## Follow-ups

### OpenAPI/Swagger UI exposure

| Option | Description | Selected |
|--------|-------------|----------|
| Spec only at `/api/openapi.json` | No interactive UI | |
| Spec + Swagger UI at `/api/docs` | FastAPI built-in | ✓ (recommended) |
| You decide | — | ✓ |

**User's choice:** "You decide" → Spec + Swagger UI at `/api/docs`.

### README bilingual

| Option | Description | Selected |
|--------|-------------|----------|
| Short "API" section, English only | Defer Hebrew | ✓ (recommended) |
| Short "API" section, EN + HE | First Hebrew block in README | |
| You decide | — | |

**User's choice:** "Short 'API' section, English only".
**Notes:** Hebrew translation deferred to "Deferred Ideas".

### Skill consumer update

| Option | Description | Selected |
|--------|-------------|----------|
| Leave as-is | No skill changes | |
| Update skill base URL/docs | Update SKILL.md to reference public docs URL | ✓ |
| You decide | — | |

**User's choice:** "Update skill base URL/docs".
**Notes:** Pure doc change in `skills/cairo-genizah-research/SKILL.md`. No code change.

### Pre-deploy verification gate

| Option | Description | Selected |
|--------|-------------|----------|
| pytest + check_docs only | Standard | |
| pytest + check_docs + skill smoke | Skill is the integration test (Phase 81B SKILL-02) | ✓ (recommended) |
| pytest + check_docs + skill smoke + manual curl | Most thorough | |
| You decide | — | ✓ |

**User's choice:** "You decide" → pytest + check_docs + skill smoke. Manual curl redundant.

---

## Claude's Discretion

- Exact wording of the Stability statement in `docs/SEARCH_API.md`.
- Exact placement of the "API" section in `README.md`.
- OpenAPI tag names + per-endpoint summary strings.
- Whether to add Pydantic field `description=` kwargs for missing fields.
- Threat-model report format (table vs prose).

## Deferred Ideas

- Versioned URL path `/api/v1/...` (until surface expands or breaking change needed)
- Optional API key authentication (its own phase)
- README.md Hebrew translation of API section
- Multi-language code samples (Python, JS) in SEARCH_API.md
- OpenAPI-generated client SDKs
- Terms-of-service / Acceptable Use page
- PostHog public API dashboard
- Body-size cap, lower default rpm, abuse alerting (only if security audit surfaces gap)
- Long-running parallels job API (Phase 81C — already deferred to v7.11)
