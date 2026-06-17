---
phase: 116-privacy-audit-ci-gate
plan: "03"
subsystem: documentation
tags: [telemetry, posthog, runbook, documentation, operations, privacy]
dependency_graph:
  requires: []
  provides:
    - docs/guides/TELEMETRY_RUNBOOK.md
    - .planning/phases/116-privacy-audit-ci-gate/116-VERIFICATION.md
  affects:
    - docs/DOCUMENTATION_INDEX.md
    - .planning/REQUIREMENTS.md
tech_stack:
  added: []
  patterns:
    - Dated inline amendment note for stale planning-doc corrections (preserving history without silent rewrite)
    - Runbook separation of queue-saturation counters from delivery proof (distinct monitoring concerns)
key_files:
  created:
    - docs/guides/TELEMETRY_RUNBOOK.md
    - .planning/phases/116-privacy-audit-ci-gate/116-VERIFICATION.md
  modified:
    - docs/DOCUMENTATION_INDEX.md
    - .planning/REQUIREMENTS.md
decisions:
  - "Drop counters (queue.Full) vs delivery proof (--telemetry-selftest SSL_OK) are explicitly separated in the runbook — a zero drop count does not prove delivery"
  - "INFRA-06 status stays Pending (no completion flip in doc-only plan) — deferred to milestone verification pass after 116-01 + 116-02 land"
  - "Dated amendment note used instead of silent rewrite — preserves the stale 'isolated' clause history with POSTHOG-PROJECT-DECISION.md citation"
metrics:
  duration: "8min"
  completed: "2026-06-16"
  tasks_completed: 2
  files_created: 2
  files_modified: 2
---

# Phase 116 Plan 03: Documentation — Telemetry Runbook + Requirements Amendment Summary

Operational telemetry runbook created, stale REQUIREMENTS.md INFRA-06 wording amended, and milestone-exit gate documented.

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Write TELEMETRY_RUNBOOK.md + DOCUMENTATION_INDEX.md entry | `0db7c134` | docs/guides/TELEMETRY_RUNBOOK.md, docs/DOCUMENTATION_INDEX.md |
| 2 | Amend REQUIREMENTS.md INFRA-06 wording + create 116-VERIFICATION.md | `1a53fa21` | .planning/REQUIREMENTS.md, .planning/phases/116-privacy-audit-ci-gate/116-VERIFICATION.md |

## What Was Built

### Task 1: TELEMETRY_RUNBOOK.md (D-08 + D-10)

Created `docs/guides/TELEMETRY_RUNBOOK.md` (205 lines, 6 `##` sections) covering all 5 D-08 requirements:

- **(a) Project and namespace separation** — shared PostHog project id 134161 (EU); web↔desktop separated by `platform=desktop` super-property + `desktop_` event-name namespace; `$process_person_profile=False` for anonymous events. Documents the 2026-06-14 reversal.
- **(b) Embedded ingest key posture + rotation** — `_TELEMETRY_KEY_DEFAULT` is a publishable, write-only `phc_` key; not a secret; already public in web client JS. Documents the 3-level resolution order (`GENIZAH_TELEMETRY_KEY` → `POSTHOG_API_KEY` → embedded default). Full rotation procedure: mint in PostHog → set env var or bake → ship → revoke old.
- **(c) Two drop counters — queue saturation ONLY** — monitors both `shared.posthog_server.get_dropped_event_count()` and `web.api_hardening.get_dropped_event_count()`; explicitly states these counters count only `queue.Full` drops and do NOT detect SSL/network/no-key/non-2xx failures; zero drop count does not prove delivery.
- **(d) `--telemetry-selftest` usage** — `SSL_OK` / `SSL_FAIL` / `NO_KEY` exit tokens with exit codes; `OFFLINE_OK` smoke-only; clean no-Python VM as SSL bundle proof; `send_selftest_event_sync()` as the actual delivery confirmation mechanism.
- **(e) Opt-out behavior** — `is_enabled()` gate stops emission immediately; `_drain_and_discard()` clears the queue; per-install ID retained on disk (CONSENT-06); re-opt-in preserves continuity.
- **Milestone-exit regression gate (D-10)** — exact pytest command; both-OS CI requirement; PRIV-03/PRIV-04/SC#3 components named.

Updated `docs/DOCUMENTATION_INDEX.md`: added `TELEMETRY_RUNBOOK.md` bullet under "For Developers"; bumped timestamp from 2026-03-26 to 2026-06-16.

### Task 2: REQUIREMENTS.md amendment + 116-VERIFICATION.md (D-07 + D-10)

**REQUIREMENTS.md** INFRA-06 checklist line amended:
- Removed: "the desktop PostHog project is isolated from the web project"
- Added: "the shared PostHog project (id 134161, EU) separates desktop events by `platform=desktop` + the `desktop_` event-name namespace (NOT an isolated project)"
- Kept: existing write-only-key and two-drop-counter clauses; `[ ]` checkbox unchecked; Traceability table rows unchanged (INFRA-06 stays Pending)
- Appended: dated amendment note: `*(AMENDED 2026-06-16: the prior "isolated project" wording was stale since the 2026-06-14 reversal — see .planning/research/POSTHOG-PROJECT-DECISION.md.)*`

**116-VERIFICATION.md** created with:
- Milestone-exit gate table (PRIV-03 / D-17 / PRIV-04 / SC#3 / all telemetry tests)
- Exact regression command: `pytest tests/test_telemetry*.py tests/test_no_direct*.py tests/test_no_dynamic*.py -m "not gui"`
- CI coverage note (existing `tests` job, both OSes)
- D-06 human-UAT gate (clean-VM SSL_OK confirmation)
- Completion-flip-deferral note: PRIV-04/INFRA-06 stay Pending until 116-01 + 116-02 land

## Decisions Made

1. **Drop counters vs delivery proof strictly separated** — the runbook prominently distinguishes `queue.Full` saturation counters from actual delivery confirmation. Zero drop count = no saturation, NOT proof of delivery. Only `--telemetry-selftest SSL_OK` (HTTP-2xx confirmed) proves delivery.

2. **Completion status not flipped** — INFRA-06 remains `Pending` in the Traceability table. This doc-only plan (`depends_on: []`) does not declare requirements Complete before 116-01/116-02 code lands (REVIEWS MEDIUM, confirmed valid).

3. **Dated amendment note instead of silent rewrite** — the stale "isolated" clause is removed but the history is preserved via the `*(AMENDED 2026-06-16: ...)*` inline note, citing `POSTHOG-PROJECT-DECISION.md` (T-116-08 Repudiation/doc-drift mitigation).

## Deviations from Plan

None — plan executed exactly as written. All acceptance criteria verified by automated bash checks.

## Known Stubs

None.

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes introduced (documentation-only plan).

## Self-Check: PASSED

- `docs/guides/TELEMETRY_RUNBOOK.md` exists: FOUND
- `.planning/phases/116-privacy-audit-ci-gate/116-VERIFICATION.md` exists: FOUND
- Commit `0db7c134` exists: FOUND
- Commit `1a53fa21` exists: FOUND
- All automated verify checks passed (RUNBOOK_OK + AMEND_OK)
