---
phase: 46
slug: dicta-translation
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-04
---

# Phase 46 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | none — existing pytest setup |
| **Quick run command** | `pytest tests/test_translation_service.py -x -q` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_translation_service.py -x -q`
- **After every plan wave:** Run `pytest tests/`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 46-01-01 | 01 | 1 | TRANS-01 | unit | `pytest tests/test_translation_service.py::test_en2he -x` | ❌ W0 | ⬜ pending |
| 46-01-02 | 01 | 1 | TRANS-02 | unit | `pytest tests/test_translation_service.py::test_he2en -x` | ❌ W0 | ⬜ pending |
| 46-01-03 | 01 | 1 | TRANS-04 | unit | `pytest tests/test_translation_service.py::test_no_overwrite -x` | ❌ W0 | ⬜ pending |
| 46-01-04 | 01 | 1 | TRANS-01 | unit | `pytest tests/test_translation_service.py::test_prompt_build -x` | ❌ W0 | ⬜ pending |
| 46-01-05 | 01 | 1 | TRANS-05 | unit | `pytest tests/test_translation_service.py::test_checkpoint -x` | ❌ W0 | ⬜ pending |
| 46-02-01 | 02 | 1 | TRANS-01 | unit | `pytest tests/test_translation_service.py::test_read_service -x` | ❌ W0 | ⬜ pending |
| 46-02-02 | 02 | 1 | TRANS-01 | unit | `pytest tests/test_translation_service.py::test_doc_types -x` | ❌ W0 | ⬜ pending |
| 46-03-01 | 03 | 2 | TRANS-05 | integration | `pytest tests/test_translation_service.py::test_search_integration -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_translation_service.py` — translation service unit tests (stubs)
- [ ] `shared/translation_service.py` — read-only translation service
- [ ] `data/few_shot_en2he_scholarly.json` — EN->HE few-shot template
- [ ] `data/few_shot_he2en_scholarly.json` — HE->EN few-shot template

*Existing infrastructure covers pytest framework.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Translation quality for scholarly terms | TRANS-01, TRANS-02 | Quality is subjective, needs domain expert review | Run sample batch (50 items), review Hebrew output for accuracy |
| UX toggle between original/translated | TRANS-05 | Visual/interactive UI behavior | Open web app, toggle translation on/off, verify display in both languages |
| Desktop parity | TRANS-05 | Desktop UI behavior | Launch desktop app, verify translation toggle and display match web |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
