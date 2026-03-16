---
phase: 48
slug: desktop-canvas
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-16
---

# Phase 48 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `tests/` directory (existing) |
| **Quick run command** | `pytest tests/test_puzzle_canvas.py -x -q` |
| **Full suite command** | `pytest tests/ -x -q` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_puzzle_canvas.py -x -q`
- **After every plan wave:** Run `pytest tests/ -x -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 48-01-01 | 01 | 1 | CANV-01 | unit | `pytest tests/test_puzzle_canvas.py::test_fragment_item_creation -x` | ❌ W0 | ⬜ pending |
| 48-01-02 | 01 | 1 | CANV-03 | unit | `pytest tests/test_puzzle_canvas.py::test_fragment_drag -x` | ❌ W0 | ⬜ pending |
| 48-01-03 | 01 | 1 | CANV-04 | unit | `pytest tests/test_puzzle_canvas.py::test_fragment_rotate -x` | ❌ W0 | ⬜ pending |
| 48-01-04 | 01 | 1 | CANV-05 | unit | `pytest tests/test_puzzle_canvas.py::test_fragment_flip -x` | ❌ W0 | ⬜ pending |
| 48-01-05 | 01 | 1 | CANV-06 | manual | Visual check — bg-removed overlay | N/A | ⬜ pending |
| 48-02-01 | 02 | 2 | PLAT-02 | integration | `pytest tests/test_puzzle_canvas.py::test_add_to_puzzle_integration -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_puzzle_canvas.py` — stubs for CANV-01, CANV-03, CANV-04, CANV-05, PLAT-02
- [ ] Fixtures for mock QApplication, QGraphicsScene, PuzzleFragment test data

*Existing pytest infrastructure covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Background-removed fragments overlay as parchment shapes | CANV-06 | Visual rendering quality requires human inspection | 1. Add 3+ fragments to canvas 2. Verify transparent alpha regions show canvas background 3. Overlap two fragments — parchment shapes visible, not rectangles |
| Smooth drag/rotate/flip visual feedback | CANV-03, CANV-04, CANV-05 | Animation smoothness is subjective visual quality | 1. Drag fragment across canvas — no jitter 2. Rotate via corner handle — smooth arc 3. Flip — instant visual mirror |
| Canvas zoom/pan with multiple fragments | CANV-01 | Performance perception requires real interaction | 1. Add 5 fragments 2. Ctrl+wheel zoom in/out — smooth 3. Drag empty canvas to pan — no lag |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
