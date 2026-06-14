# Phase 111: Telemetry Foundation - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-14
**Phase:** 111-telemetry-foundation
**Areas discussed:** PostHog project setup, embedded-key configuration, consent-gate placement, pipeline verification (+ project-creation fallback)

---

## PostHog project & key setup

| Option | Description | Selected |
|--------|-------------|----------|
| I create it now via MCP | Use the PostHog MCP to create a new project, capture the key | ✓ (intent) |
| You create it, give me the key | User makes the project in the UI, pastes the key | |
| Defer key to execution | Build against a placeholder/env key | |

**User's choice:** "I create it now via MCP" — but on attempting it, the MCP was found to expose **no project-create tool** (only read/switch/update). Re-surfaced as a fallback question (below).

---

## Project-creation fallback (after MCP limitation)

| Option | Description | Selected |
|--------|-------------|----------|
| Defer key, build against env override | 111 builds against `GENIZAH_TELEMETRY_KEY` placeholder; real project/key before Phase 114 | ✓ |
| You create it now in the UI | User makes "Dicta Genizah Desktop" project, pastes publishable key | |
| I script it via REST API | User supplies a personal API key with org-write scope; I POST to create it | |

**User's choice:** Defer key, build against env override.
**Notes:** Low-friction — Phase 111 fires no real events, so the missing project blocks nothing. Real project created before Phase 114.

---

## Embedded-key configuration

| Option | Description | Selected |
|--------|-------------|----------|
| Constant + env override | Hardcoded publishable constant + `GENIZAH_TELEMETRY_KEY`/host override for dev/staging | ✓ |
| Hardcoded constant only | Just a module constant, no override | |
| Bundled config file | Ship key/host in a bundled data file | |

**User's choice:** Constant + env override.
**Notes:** Publishable/write-only key is safe to embed and commit; override enables the dev self-test and test-project targeting.

---

## Consent-gate placement

| Option | Description | Selected |
|--------|-------------|----------|
| Gate in desktop chokepoint only | Consent enforced in `desktop/telemetry.py`; `shared/posthog_server.py` stays ungated | ✓ |
| Gate inside posthog_server | Add the gate in the shared module | |
| Discuss the seam | Talk through shared-vs-desktop additions | |

**User's choice:** Gate in desktop chokepoint only.
**Notes:** Reconciles ROADMAP SC#5 — the shared queue must stay ungated so web/NLI-breaker telemetry is unaffected; posthog_server gets neutral additions only.

---

## Pipeline verification (111 fires no real events)

| Option | Description | Selected |
|--------|-------------|----------|
| Dev-only self-test event | A `--telemetry-selftest`/env-gated throwaway event to verify end-to-end | ✓ |
| Unit tests only | Defer real end-to-end check to Phase 114 | |
| Manual one-off | Hand-fire a test event during execution | |

**User's choice:** Dev-only self-test event.
**Notes:** Gated so it never fires in normal use; ships no user-facing event in 111.

---

## Claude's Discretion

- Exact event-name registry shape, scrubber redaction regexes, property-allowlist contents, and `config.pkl` key names — left to research/planner within the locked constraints.

## Deferred Ideas

- Real PostHog "Dicta Genizah Desktop" project + key creation (operational; before Phase 114).
- All Phase 112–116 work (consent UX, exception hooks, usage/perf events, CI privacy gate).
- All 7 pending todos reviewed — none telemetry-related; left in pending.
