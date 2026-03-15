---
phase: 47
slug: foundation-background-removal
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-15
---

# Phase 47 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `pytest.ini` (existing) |
| **Quick run command** | `pytest tests/test_puzzle_service.py tests/test_background_removal.py -x -q` |
| **Full suite command** | `pytest tests/ -x -q` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_puzzle_service.py tests/test_background_removal.py -x -q`
- **After every plan wave:** Run `pytest tests/ -x -q`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | BGRM-01 | unit+integration | `pytest tests/test_background_removal.py -k "test_remove_background"` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | BGRM-02 | unit | `pytest tests/test_background_removal.py -k "test_toggle"` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | BGRM-03 | unit | `pytest tests/test_background_removal.py -k "test_threshold"` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | data-model | unit | `pytest tests/test_puzzle_service.py -k "test_roundtrip"` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | sidecar | unit | `pytest tests/test_puzzle_service.py -k "test_joins_db"` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_background_removal.py` — stubs for BGRM-01, BGRM-02, BGRM-03
- [ ] `tests/test_puzzle_service.py` — stubs for data model roundtrip, joins.db schema
- [ ] Sample test images from major libraries (NLI, Cambridge, JTS) in `tests/fixtures/`

*Existing pytest infrastructure covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Visual bg removal quality | BGRM-01 | Edge quality needs human review | Run preview tool, compare original vs stripped for each library |
| Threshold slider responsiveness | BGRM-03 | Visual feedback assessment | Adjust slider, verify mask updates visually in preview tool |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
