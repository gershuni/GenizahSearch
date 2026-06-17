---
phase: 111
slug: telemetry-foundation
status: planned
nyquist_compliant: true
wave_0_complete: false
created: 2026-06-14
updated: 2026-06-14
---

# Phase 111 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Updated 2026-06-14 to reflect the Codex --reviews pass (HIGH-1 transport-key
> wiring, HIGH-2 _reset_for_tests global clearing, MEDIUM context-scrub /
> $identify-reject / bounded-flush, MEDIUM interface-note correction, LOW
> resolved-path chokepoint skip). No task IDs were added or renamed — the
> existing 6-task structure is preserved; behaviors per task expanded.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini / pyproject.toml (existing) |
| **Quick run command** | `python -m pytest tests/test_telemetry_*.py -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen python -m pytest tests/ -q` |
| **Estimated runtime** | ~30 seconds (targeted telemetry suite; pure-Python, no Tantivy/Qt load) |

---

## Sampling Rate

- **After every task commit:** Run `python -m pytest tests/test_telemetry_*.py tests/test_posthog_server.py -q`
- **After every plan wave:** Run the targeted telemetry + posthog_server + nli_circuit_breaker + cross-module-invariant + AST-guard suites
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 111-01-01 | 01 | 1 | INFRA-03, CONSENT-08 | T-111-01/02/05/17 | RED: neutral additions; scrub hook before queue put; drain-and-discard makes no POST; transport key override reaches transport without env mutation; flush wall-time bounded; _reset clears new globals | unit (RED) | `python -m pytest tests/test_telemetry_posthog_server_ext.py -q` | ❌ W0 (created here) | ⬜ pending |
| 111-01-02 | 01 | 1 | INFRA-03, INFRA-04, INFRA-05, CONSENT-08 | T-111-01..05, T-111-17 | GREEN: enqueue_event signature unchanged; _event_queue patches in test_posthog_server still green; nli_circuit_breaker (patches ph.enqueue_event) + cross-module-invariants still green; no global gate; key override preferred over POSTHOG_API_KEY env (web unchanged); _flush_before_exit deadline-bounded; _reset_for_tests clears _default_distinct_id/_scrub_hook/_api_key_override/_host_override | unit (GREEN) | `python -m pytest tests/test_telemetry_posthog_server_ext.py tests/test_posthog_server.py tests/test_nli_circuit_breaker.py tests/test_nli_breaker_cross_module_invariants.py -q` | ✅ (after 111-01-01) | ⬜ pending |
| 111-02-01 | 02 | 2 | CONSENT-01, CONSENT-05, CONSENT-06, CONSENT-07, INFRA-02 | T-111-08/10/11/13/17 | Cached no-throw is_enabled; zero events pre-consent; uuid4 minted on opt-in only; id retained on opt-out; desktop GENIZAH_TELEMETRY_KEY/HOST wired into transport (ph._api_key_override set) | unit | `python -m pytest tests/test_telemetry_consent_gate.py -q` | ❌ W0 (created here) | ⬜ pending |
| 111-02-02 | 02 | 2 | PRIV-01, PRIV-02, PRIV-06, INFRA-01 | T-111-06/07/18 | Banned keys stripped by EXACT/explicit match (allowlisted `context` survives); Windows/POSIX/filename/Hebrew redacted; allowlist drops env props; unknown event names rejected; track() rejects $identify | unit | `python -m pytest tests/test_telemetry_scrubbing.py tests/test_telemetry_allowlist.py tests/test_telemetry_consent_gate.py -q` | ❌ W0 (created here) | ⬜ pending |
| 111-02-03 | 02 | 2 | IDENT-03, IDENT-04, INFRA-01 | T-111-09/14/18 | $identify carries no email/name; consent-gated; reset reverts to anon; self-test gated off by default; identify() is the SOLE emitter of $identify (track rejects it) | unit | `python -m pytest tests/test_telemetry_identity.py tests/test_telemetry_consent_gate.py tests/test_telemetry_scrubbing.py tests/test_telemetry_allowlist.py -q` | ❌ W0 (created here) | ⬜ pending |
| 111-03-01 | 03 | 3 | PRIV-06 (PRIV-03 mechanism, landed early) | T-111-15/16/19 | No desktop/*.py except telemetry.py (by RESOLVED PATH) reaches enqueue_event; scanner non-vacuous (synthetic bare + `import as ph` + `from shared import posthog_server as ph` detected); resolved-path exemption (not basename) | static AST | `python -m pytest tests/test_telemetry_no_direct_posthog.py -q` | ❌ W0 (created here) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

**Continuity check:** No 3 consecutive tasks lack an `<automated>` command — every one of the 6 tasks has an automated pytest command. All test files are created within their own tasks (RED-first where TDD applies), satisfying Nyquist Dimension 8.

---

## Wave 0 Requirements

All test files are created in-task as the RED step of each TDD task (TDD_MODE off, but the consent gate / scrubber / allowlist / identity / AST guard are all unit-testable and tests precede or accompany implementation):

- [ ] `tests/test_telemetry_posthog_server_ext.py` — Plan 01 Task 1 (RED), covers INFRA-03 + CONSENT-08 mechanism + transport-key override + bounded flush + _reset-clears-globals (REVIEWS HIGH-1/HIGH-2/MEDIUM)
- [ ] `tests/test_telemetry_consent_gate.py` — Plan 02 Task 1, covers CONSENT-01/05/06/07 + IDENT-04 gate + transport key/host wiring
- [ ] `tests/test_telemetry_scrubbing.py` — Plan 02 Task 2, covers PRIV-01 (incl. context-survives regression)
- [ ] `tests/test_telemetry_allowlist.py` — Plan 02 Task 2, covers PRIV-02/06 (incl. track-rejects-$identify)
- [ ] `tests/test_telemetry_identity.py` — Plan 02 Task 3, covers IDENT-03/04
- [ ] `tests/test_telemetry_no_direct_posthog.py` — Plan 03 Task 1, covers PRIV-03 chokepoint (AST guard, resolved-path exemption, landed early)
- [ ] `desktop/telemetry.py` — the module itself (Plan 02)

Existing pytest infrastructure covers the framework — no install needed.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Self-test event reaches the live shared PostHog project (id 134161) | INFRA-01 / D-06 | Requires a real network round-trip to eu.i.posthog.com with the real publishable key (placeholder in Phase 111; real key drops in before Phase 114). With Plan 01 transport-key wiring closed (REVIEWS HIGH-1), the self-test now genuinely targets the project key rather than silently no-opping. | Run `python -m desktop.telemetry` with `GENIZAH_TELEMETRY_KEY` set; confirm one throwaway `desktop_selftest` event appears in PostHog. NOT a release gate for Phase 111 (key is a placeholder). |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify (6/6 tasks)
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Wave 0 covers all MISSING references (test files created in-task, RED-first)
- [x] No watch-mode flags
- [x] Feedback latency < 30s (pure-Python telemetry suite)
- [x] `nyquist_compliant: true` set in frontmatter
- [x] REVIEWS pass incorporated (HIGH-1/HIGH-2/MEDIUM×3/LOW×2) without adding/renaming tasks

**Approval:** planner-approved 2026-06-14 (re-approved post-reviews 2026-06-14)
