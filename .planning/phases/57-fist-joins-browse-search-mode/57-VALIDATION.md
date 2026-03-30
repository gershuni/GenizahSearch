---
phase: 57
slug: fist-joins-browse-search-mode
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-29
---

# Phase 57 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | tests/ directory (existing) |
| **Quick run command** | `py -m pytest tests/test_visual_similarity.py -x -q` |
| **Full suite command** | `py -m pytest tests/ -x -q` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `py -m pytest tests/test_visual_similarity.py -x -q`
- **After every plan wave:** Run `py -m pytest tests/ -x -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| TBD | 01 | 1 | JOIN-01 | unit | `py -m pytest tests/test_visual_similarity.py -x -q` | ❌ W0 | ⬜ pending |
| TBD | 02 | 1 | JOIN-02 | unit | `py -m pytest tests/test_visual_similarity.py -x -q` | ❌ W0 | ⬜ pending |
| TBD | 03 | 2 | JOIN-03 | integration | `py -m pytest tests/test_visual_similarity.py -x -q` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_visual_similarity.py` — stubs for JOIN-01, JOIN-02, JOIN-03
- [ ] Test fixtures for mock FIST.db data (DocumentID->AlmaId chain)

*Existing test infrastructure (pytest, conftest.py) covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Visual Similarity dialog renders correctly | JOIN-01 | UI rendering requires browser/Qt | Open browse view, click Visual Similarity button, verify dialog opens with ranked list |
| "Search in visual suggestions" cross-cutting action | JOIN-02 | Multi-context UI interaction | From browse/list/search results, select manuscripts, trigger action, verify suggestion pool loads |
| Union/intersection toggle works correctly | JOIN-03 | UI state interaction | Select 2+ manuscripts, toggle union/intersection, verify result set changes |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
