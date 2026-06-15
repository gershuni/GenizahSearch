---
phase: 113
slug: crash-reporting
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-15
---

# Phase 113 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini / pyproject.toml |
| **Quick run command** | `pytest tests/test_crash_hooks.py tests/test_telemetry_no_direct_posthog.py -q` |
| **Full suite command** | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/ -q` |
| **Estimated runtime** | ~TBD seconds |

---

## Sampling Rate

- **After every task commit:** Run quick run command
- **After every plan wave:** Run full suite command
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** TBD seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 113-01-01 | 01 | 1 | CRASH-XX | T-113-01 / — | (expected secure behavior or "N/A") | unit | `{command}` | ✅ / ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

*Populated by the planner / gsd-nyquist-auditor against the RESEARCH.md "## Validation Architecture" section.*

---

## Wave 0 Requirements

- [ ] `tests/test_crash_hooks.py` — stubs for CRASH-01..07
- [ ] `tests/conftest.py` — shared fixtures (reset telemetry/posthog_server module globals between tests)

*If none: "Existing infrastructure covers all phase requirements."*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Frozen-binary Qt slot exception → sys.excepthook fires | CRASH-02 | Requires PyInstaller `.exe` build; cannot reproduce frozen behavior in pytest | Build EXE, raise in a QTimer.singleShot slot, confirm crash event + crash_log.txt |
| Native C-extension crash → faulthandler dump + next-launch emit | CRASH-03 | Real segfault (Tantivy/PyMuPDF) cannot be triggered deterministically in-process | Force a native crash, relaunch, confirm `desktop_prior_crash` emitted once after consent |

*If none: "All phase behaviors have automated verification."*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < TBDs
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
