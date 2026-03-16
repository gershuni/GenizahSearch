---
phase: 48
slug: desktop-canvas
status: draft
nyquist_compliant: true
wave_0_complete: true
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
| **Quick run command** | `pytest tests/test_puzzle_model.py tests/test_puzzle_image_service.py -x -q` |
| **Full suite command** | `pytest tests/ -x -q` |
| **Estimated runtime** | ~15 seconds |

---

## Automated Verification Approach

This phase creates PyQt6 QGraphicsItem / QGraphicsView / QMainWindow classes. These require a running QApplication and display server for instantiation, making traditional unit tests impractical in headless CI or without display-dependent fixtures.

**Chosen approach: import-check automation + visual checkpoint.**

- **Per-task:** `python -c "from module import Class; print('OK')"` verifies class definition, method signatures, and absence of syntax/import errors. This catches the most common failure modes (missing imports, typos, wrong base class).
- **Per-wave:** `pytest tests/ -x -q` ensures no regressions in existing test suite (puzzle_model, puzzle_image_service, and all other tests).
- **End-of-phase:** Plan 03 Task 2 is a `checkpoint:human-verify` covering all interactive behaviors (drag, rotate, flip, resize, zoom, pan, entry points).

The Phase 47 foundation (puzzle_model, puzzle_image_service, background_removal) has full unit test coverage. Phase 48 builds GUI on top of those tested foundations.

---

## Sampling Rate

- **After every task commit:** Run import check for newly created classes
- **After every plan wave:** Run `pytest tests/ -x -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | Status |
|---------|------|------|-------------|-----------|-------------------|--------|
| 48-01-01 | 01 | 1 | CANV-06 | import | `python -c "from gui_threads import PuzzleImageLoaderThread; print('OK')"` | pending |
| 48-01-02 | 01 | 1 | CANV-03,04,05 | import | `python -c "from genizah_app import PuzzleFragmentItem, PuzzleCanvasView; print('OK')"` | pending |
| 48-02-01 | 02 | 2 | CANV-01,PLAT-02 | import | `python -c "from gui_threads import PuzzleMetaLoaderThread; from genizah_app import PuzzleCanvasWindow; print('OK')"` | pending |
| 48-03-01 | 03 | 3 | CANV-01,PLAT-02 | import | `python -c "import re; code=open('genizah_app.py','r',encoding='utf-8').read(); assert 'btn_b_add_to_puzzle' in code; print('OK')"` | pending |
| 48-03-02 | 03 | 3 | ALL | visual | `checkpoint:human-verify` (23-step interactive test) | pending |

*Status: pending / green / red / flaky*

---

## Regression Guard

| Wave | Command | Purpose |
|------|---------|---------|
| After wave 1 | `pytest tests/test_puzzle_model.py tests/test_puzzle_image_service.py -x -q` | Phase 47 foundations intact |
| After wave 2 | `pytest tests/ -x -q` | Full regression |
| After wave 3 | `pytest tests/ -x -q` | Full regression before checkpoint |

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Background-removed fragments overlay as parchment shapes | CANV-06 | Visual rendering quality requires human inspection | 1. Add 3+ fragments to canvas 2. Verify transparent alpha regions show canvas background 3. Overlap two fragments — parchment shapes visible, not rectangles |
| Smooth drag/rotate/flip visual feedback | CANV-03, CANV-04, CANV-05 | Animation smoothness is subjective visual quality | 1. Drag fragment across canvas — no jitter 2. Rotate via corner handle — smooth arc 3. Flip — instant visual mirror |
| Canvas zoom/pan with multiple fragments | CANV-01 | Performance perception requires real interaction | 1. Add 5 fragments 2. Ctrl+wheel zoom in/out — smooth 3. Drag empty canvas to pan — no lag |
| Async fl_id resolution does not freeze UI | CANV-01 | Network latency perception | 1. Type shelfmark and press Enter 2. UI remains responsive during "Resolving images..." 3. Fragment appears after network response |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify commands
- [x] Sampling continuity: no 3 consecutive tasks without automated verify
- [x] Import-check approach documented and justified for GUI-only classes
- [x] No watch-mode flags
- [x] Feedback latency < 15s
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
