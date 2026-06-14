---
phase: 111
slug: telemetry-foundation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-14
---

# Phase 111 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini / pyproject.toml (existing) |
| **Quick run command** | `pytest tests/test_telemetry.py -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -q` |
| **Estimated runtime** | ~30 seconds (targeted telemetry suite) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_telemetry.py -q`
- **After every plan wave:** Run the targeted telemetry + posthog_server + AST-guard suites
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| {N}-01-01 | 01 | 1 | REQ-{XX} | T-{N}-01 / — | {expected secure behavior or "N/A"} | unit | `{command}` | ✅ / ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

*Per-task rows are populated by the planner during PLAN.md creation and reconciled by the Nyquist auditor.*

---

## Wave 0 Requirements

- [ ] `tests/test_telemetry.py` — stubs for the consent gate, scrubber, allowlist, identity mechanism
- [ ] `tests/test_telemetry_no_raw_enqueue.py` — AST guard mirroring `tests/test_no_raw_storage_access.py` (PRIV-03 chokepoint)
- [ ] Existing pytest infrastructure covers the framework — no install needed

*If none: "Existing infrastructure covers all phase requirements."*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Self-test event reaches the live PostHog project | INFRA-* / D-06 | Requires a real network round-trip to eu.i.posthog.com with the real key | Run `python genizah_app.py --telemetry-selftest` with `GENIZAH_TELEMETRY_KEY` set; confirm one throwaway event appears in PostHog |

*If none: "All phase behaviors have automated verification."*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
