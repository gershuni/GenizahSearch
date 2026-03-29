---
phase: 56
slug: exclude-known-manuscripts
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-29
---

# Phase 56 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `pytest.ini` |
| **Quick run command** | `pytest tests/ -x -q --timeout=30` |
| **Full suite command** | `pytest tests/ -q --timeout=60` |
| **Estimated runtime** | ~45 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/ -x -q --timeout=30`
- **After every plan wave:** Run `pytest tests/ -q --timeout=60`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 45 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 56-01-01 | 01 | 1 | EXCL-01 | unit | `pytest tests/test_exclusion_service.py -x -q` | ❌ W0 | ⬜ pending |
| 56-01-02 | 01 | 1 | EXCL-02 | unit | `pytest tests/test_exclusion_service.py -x -q` | ❌ W0 | ⬜ pending |
| 56-02-01 | 02 | 2 | EXCL-03 | unit | `pytest tests/test_exclusion_service.py -x -q` | ❌ W0 | ⬜ pending |
| 56-03-01 | 03 | 2 | EXCL-04 | integration | `pytest tests/test_exclusion_service.py -x -q` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_exclusion_service.py` — stubs for EXCL-01 through EXCL-04
- [ ] Shared fixtures for shelfmark resolution test data

*Existing pytest infrastructure covers framework needs.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Supabase list picker dialog UI | EXCL-01 | Requires authenticated Supabase session + visual dialog | 1. Log in, 2. Open exclusion picker, 3. Select a list, 4. Verify manuscripts hidden |
| File import dialog + resolution report UI | EXCL-02 | Requires file upload interaction + visual report check | 1. Click import, 2. Upload shelfmark file, 3. Verify resolution report table |
| Session persistence across search switches | EXCL-04 | Requires multi-step user session interaction | 1. Set exclusions, 2. Switch search mode, 3. Run new search, 4. Verify exclusions still active |
| Desktop ExcludeDialog multi-source extension | EXCL-01 | Requires PyQt6 desktop app running | 1. Open desktop, 2. Exclude from list + file, 3. Verify per-source clear buttons |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 45s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
