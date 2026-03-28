---
phase: 55
slug: search-within-results
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-28
---

# Phase 55 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `pytest.ini` |
| **Quick run command** | `pytest tests/test_refinement.py -x -q` |
| **Full suite command** | `pytest tests/ -x -q` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_refinement.py -x -q`
- **After every plan wave:** Run `pytest tests/ -x -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 55-01-01 | 01 | 1 | SRCH-01 | unit | `pytest tests/test_refinement.py::test_refinement_step_dataclass -x` | ❌ W0 | ⬜ pending |
| 55-01-02 | 01 | 1 | SRCH-01 | unit | `pytest tests/test_refinement.py::test_effective_restrict_computation -x` | ❌ W0 | ⬜ pending |
| 55-02-01 | 02 | 2 | SRCH-01 | manual | Web: click "Search within", verify refine mode activates | N/A | ⬜ pending |
| 55-02-02 | 02 | 2 | SRCH-02 | manual | Web: verify breadcrumb chip chain displays correctly | N/A | ⬜ pending |
| 55-02-03 | 02 | 2 | SRCH-03 | manual | Web: click × on chip, verify chain pops correctly | N/A | ⬜ pending |
| 55-03-01 | 03 | 2 | SRCH-01 | manual | Desktop: click "Search within", verify refine mode activates | N/A | ⬜ pending |
| 55-03-02 | 03 | 2 | SRCH-02 | manual | Desktop: verify breadcrumb chip chain displays correctly | N/A | ⬜ pending |
| 55-04-01 | 04 | 3 | SRCH-01 | integration | `pytest tests/test_refinement.py::test_session_persistence -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_refinement.py` — stubs for SRCH-01, SRCH-02, SRCH-03 (RefinementStep dataclass, effective restrict computation, chain replay, session persistence)

*Existing test infrastructure (pytest, conftest.py) covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Refine mode activation (scroll, focus, badge) | SRCH-01 | Visual UI behavior | Click "Search within" button, verify search bar scrolls into view with refine badge |
| Breadcrumb chip chain display | SRCH-02 | Visual layout | Run 2+ refinements, verify chip chain with › separators appears on own strip |
| Cross-mode chip labels | SRCH-02 | Visual | Refine from Word → Responsa, verify mode labels appear on chips |
| Clear all / pop chip behavior | SRCH-03 | Visual + state | Click × on middle chip, verify later chips removed; click Clear all, verify full reset |
| Cancel refine mode | SRCH-01 | Visual | Enter refine mode, click Cancel, verify no search runs |
| Zero-result refinement recovery | SRCH-01 | Visual + UX | Refine to 0 results, verify "Back to previous step" appears |
| RTL chip overflow scrolling | SRCH-02 | Visual | Create 5+ refinement steps, verify horizontal scroll works |
| Desktop breadcrumb strip | SRCH-02 | Visual | Desktop: verify dedicated strip above results table |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
