---
phase: 49
slug: web-canvas
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-16
---

# Phase 49 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini |
| **Quick run command** | `python -m pytest tests/ -x -q --timeout=30` |
| **Full suite command** | `python -m pytest tests/ -q --timeout=60` |
| **Estimated runtime** | ~45 seconds |

---

## Sampling Rate

- **After every task commit:** Run `python -m pytest tests/ -x -q --timeout=30`
- **After every plan wave:** Run `python -m pytest tests/ -q --timeout=60`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 45 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 49-01-01 | 01 | 1 | PLAT-01 | integration | `python -m pytest tests/test_puzzle_web.py -q` | ❌ W0 | ⬜ pending |
| 49-01-02 | 01 | 1 | PLAT-01 | unit | `python -m pytest tests/test_puzzle_api.py -q` | ❌ W0 | ⬜ pending |
| 49-02-01 | 02 | 1 | CANV-07 | manual | Browser test | N/A | ⬜ pending |
| 49-02-02 | 02 | 1 | CANV-08 | manual | Browser test | N/A | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_puzzle_web.py` — stubs for PLAT-01 web canvas integration
- [ ] `tests/test_puzzle_api.py` — stubs for puzzle image API endpoint

*Existing infrastructure covers framework and fixtures.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Canvas drag/rotate/flip/resize | PLAT-01 | Browser interaction requires manual visual check | Open /puzzle, add fragment, verify all manipulations work |
| Folio navigation on canvas | CANV-07 | Browser UI interaction | Add fragment, click prev/next, verify image changes |
| Snap guides appearance | CANV-08 | Visual alignment verification | Drag fragment near another, check cyan guide lines appear |
| IIIF images load without CORS | PLAT-01 | Network/CORS requires live server | Add fragment from each library, verify image loads |
| Session state persistence | PLAT-01 | Navigation flow test | Add fragments, navigate to Browse, return to /puzzle, verify fragments preserved |

*Most phase behaviors require manual browser testing due to Fabric.js canvas interaction.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 45s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
