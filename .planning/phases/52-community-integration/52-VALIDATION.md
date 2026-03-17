---
phase: 52
slug: community-integration
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-03-17
---

# Phase 52 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini |
| **Quick run command** | `pytest tests/ -x -q --timeout=30` |
| **Full suite command** | `pytest tests/ --timeout=60` |
| **Estimated runtime** | ~45 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/ -x -q --timeout=30`
- **After every plan wave:** Run `pytest tests/ --timeout=60`
- **Before `/gsd:verify-work`:** Full suite must be green
- **Max feedback latency:** 45 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| TBD | TBD | TBD | COMM-02 | integration | `pytest tests/test_puzzle_publish.py` | ❌ W0 | ⬜ pending |
| TBD | TBD | TBD | COMM-03 | integration | `pytest tests/test_puzzle_publish.py` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_puzzle_publish.py` — stubs for COMM-02, COMM-03
- [ ] Supabase test fixtures or mocks for published_joins table

*Existing test infrastructure covers CANV-02 and COMM-01 (already implemented).*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Published join visible in Discoveries feed | COMM-03 | Requires live Supabase + web UI | Publish a join, navigate to Discoveries, verify thumbnail appears in feed |
| Published join visible in joins panel | COMM-03 | Requires browse page + live data | View a manuscript that has a published join, verify Community section appears |
| Desktop publish button sends to Supabase | COMM-02 | Requires desktop app + auth | Login in desktop, create puzzle, click Publish, verify in web Discoveries |
| Full-res download from published join | COMM-02 | Requires Supabase storage bucket | Publish a join, open as other user, click download, verify PNG quality |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 45s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
