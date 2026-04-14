---
phase: 64
slug: auth-migration
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-04-14
---

# Phase 64 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `pytest.ini` |
| **Quick run command** | `pytest tests/ -x -q` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | ~15 seconds |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/ -x -q`
- **After every plan wave:** Run `pytest tests/`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** 15 seconds

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 64-01-01 | 01 | 1 | BLDG-03 | — | Import resolves to supabase_auth | unit | `pytest tests/ -x -q` | ✅ | ⬜ pending |
| 64-01-02 | 01 | 1 | BLDG-03 | — | gotrue removed from requirements | integration | `pip show gotrue` returns not found | ✅ | ⬜ pending |
| 64-02-01 | 02 | 1 | BLDG-03 | — | OAuth callback uses PKCE only | manual | See Manual-Only section | — | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

*Existing infrastructure covers all phase requirements.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Web email/password login | BLDG-03 | Requires live Supabase + browser | Login at genizahsearch.com, verify dashboard access |
| Web Google OAuth login | BLDG-03 | Requires Google OAuth redirect | Click Google login, complete flow, verify session |
| Desktop email/password login | BLDG-03 | Requires PyQt6 desktop app | Launch `python genizah_app.py`, login, verify |
| Token refresh / session persistence | BLDG-03 | Requires app restart cycle | Login, close app, reopen, verify still authenticated |
| Logout both apps | BLDG-03 | Requires interactive testing | Logout in web + desktop, verify session cleared |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 15s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
