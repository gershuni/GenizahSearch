---
phase: 54
slug: dimensions-display-filtering
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-26
---

# Phase 54 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini |
| **Quick run command** | `pytest tests/ -x -q --timeout=30` |
| **Full suite command** | `pytest tests/ -q --timeout=60` |
| **Estimated runtime** | ~30 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/ -x -q --timeout=30`
- **After every plan wave:** Run `pytest tests/ -q --timeout=60`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 30 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | DIM-01 | unit+integration | `pytest tests/test_measurements.py -q` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | DIM-04 | unit | `pytest tests/test_measurements.py -q` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_measurements.py` — stubs for DIM-01, DIM-04
- [ ] Test fixtures for measurement data samples

*Existing pytest infrastructure covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Measurements dialog opens with correct data | DIM-01 | Visual UI interaction | Browse to manuscript with size data, click Measurements button, verify per-image data displays |
| Dialog shows multiple cataloger sources | DIM-01 | Visual layout | Browse to manuscript with multiple size records, verify source attribution |
| Desktop dialog matches web dialog content | DIM-01 | Cross-platform visual | Open same manuscript in both apps, compare dialog content |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
