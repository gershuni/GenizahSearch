# Phase 116: Privacy Audit + CI Gate - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-16
**Phase:** 116-privacy-audit-ci-gate
**Areas discussed:** PRIV-04 test depth, Frozen SSL + offline self-test, Runbook + stale req, CI gate structure

---

## PRIV-04 — Forbidden-field test depth

| Option | Description | Selected |
|--------|-------------|----------|
| Producer-path capture | Drive real search/My-Library/composition/crash paths with enqueue_event monkeypatched; assert payloads forbidden-field-free (strongest) | |
| Scrubber-unit only | Extend existing fixture tests with more Windows-path/Hebrew/traceback inputs (lighter) | ✓ |
| Both, allowlist-subset assertion | Producer-path + generic ⊆ _ALLOWED_PROPS invariant (most defensive) | |

**User's choice:** Scrubber-unit only.

| Scenario | Description | Selected |
|----------|-------------|----------|
| Crash traceback w/ My-Library path | exc raised from frame with path + Hebrew locals | |
| My-Library / Local search | Local-corpus search w/ Hebrew query + filenames | |
| Composition search w/ exclusions | exclusion list + query absence | |
| Pre-consent zero-emit | fresh config.pkl → zero events | |

**User's choice (scenarios):** "Your discretion. Nothing heavy."
**Notes:** The scrubber is structural and runs inside `track()` on every payload; the PRIV-03 AST
guard already forces all desktop emission through the chokepoint. So unit-proof of the filter +
pre-consent zero-emit is sufficient — no producer-path harness. Keep it light; reuse existing fixtures.

---

## Frozen-binary SSL + offline self-test (SC#3)

| Option | Description | Selected |
|--------|-------------|----------|
| --telemetry-selftest CLI flag | Headless flag (~30 lines, mirrors --self-test-pymupdf): real POST → SSL_OK/SSL_FAIL + exit code; offline arm confirms fast no-crash return; reuses run_selftest() | ✓ |
| Fully manual / dev machine | No new code; launch app, watch PostHog, pull network | |
| CLI flag + defer clean run to release | Add flag now, defer clean-Windows run to /release smoke | |

**User's choice:** `--telemetry-selftest` CLI flag.

| Option | Description | Selected |
|--------|-------------|----------|
| Clean Windows VM, no Python | True SC#3 condition; proves certifi/SSL bundled; HUMAN-UAT before close | ✓ |
| Hillel's dev machine | Easier but dev Python certs could mask a missing bundle | |
| Defer to /release smoke | Run on the freshly-built installer at release | |

**User's choice (clean run):** Clean Windows VM, no Python.
**Notes:** This same clean-machine run also closes Phase 114's open "live PostHog event delivery" UAT.

---

## Operational runbook (INFRA-06) + stale-requirement contradiction

| Option | Description | Selected |
|--------|-------------|----------|
| Document shared + amend INFRA-06 text | Runbook documents ONE shared web project + namespace separation; edit stale "isolated" wording in REQUIREMENTS.md w/ dated note | ✓ |
| Document shared, leave req text as-is | Write runbook correctly but leave the standing contradiction | |

**User's choice:** Document shared + amend INFRA-06 text.

| Option | Description | Selected |
|--------|-------------|----------|
| New docs/guides/TELEMETRY_RUNBOOK.md | Dedicated doc; add to DOCUMENTATION_INDEX.md | ✓ |
| Fold into DEPLOYMENT_TECHNICAL.md | Fewer files, but buries desktop telemetry ops | |
| Both — runbook + CLAUDE.md pointer | New doc + always-loaded one-line pointer | |

**User's choice (location):** New `docs/guides/TELEMETRY_RUNBOOK.md`.
**Notes:** INFRA-06's "isolated project" wording predates the 2026-06-14 shared-project reversal
(INFRA-01 / POSTHOG-PROJECT-DECISION.md) — amending it keeps the planning docs internally consistent.

---

## CI gate structure + milestone-exit check

| Option | Description | Selected |
|--------|-------------|----------|
| Keep in existing tests job | Guards already run both-OS in `tests`; add PRIV-04 there; no YAML churn | ✓ |
| Add fast privacy-gate step to lint-and-docs | Move stdlib AST guards into a named fast gate | |
| Dedicated privacy-gate job (both OSes) | Separate matrix job; most visible, duplicates dep-install | |

**User's choice:** Keep in existing tests job.

| Option | Description | Selected |
|--------|-------------|----------|
| Yes, document as exit gate | ~290-test telemetry regression must be green before v8.1.0 ships | ✓ |
| No, rely on CI | CI runs everything per-push; no ceremonial step | |

**User's choice (exit check):** Yes, document as exit gate.
**Notes:** SC#1 (AST guard green on both OSes) is already true via the existing `tests` matrix.

---

## Claude's Discretion

- PRIV-04 fixture/scenario selection (keep light, reuse existing).
- `--telemetry-selftest` flag name, output tokens, exit codes, offline-arm mechanism.
- Runbook section ordering and wording.

## Deferred Ideas

- **WEB-F1** — clean the pre-existing web `search_executed` `query: clean_query[:100]` leak (web follow-up, not blocking this milestone).
- **ERR-01** — handled/non-fatal error counting (Future).
- **CONSENT-F1** — "reset telemetry id" affordance (Future).
- **CRASH-F1** — "send logs" native-crash upload (Future).
- **FLAG-F1** — PostHog feature flags / remote config on desktop (Future).
