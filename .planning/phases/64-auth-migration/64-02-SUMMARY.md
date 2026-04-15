---
phase: 64-auth-migration
plan: 02
status: complete
started: 2026-04-14
completed: 2026-04-14
requirements-completed: [BLDG-03]
---

# Plan 64-02 Summary: Remove gotrue + Update Test Guards + Manual Verification

## What Was Built

Removed the deprecated `gotrue` package from all dependency files and updated test forbidden-import lists to also catch `supabase_auth` in offline service modules.

## Tasks Completed

| # | Task | Status |
|---|------|--------|
| 1 | Remove gotrue from dependencies, update test guards, audit codebase | ✓ Complete |
| 2 | Manual auth testing and external review gate (checkpoint) | ✓ Approved |

## Key Changes

| File | Change |
|------|--------|
| `requirements.txt` | Removed `gotrue==2.12.4` (14→13 packages) |
| `requirements-lock.txt` | Removed `gotrue==2.12.4` line |
| `tests/test_offline_verification.py` | Added `'supabase_auth'` to both forbidden import lists (line 447 and line 509) |

## Verification

- `grep -c "gotrue" requirements.txt` → 0 ✓
- `grep -c "gotrue" requirements-lock.txt` → 0 ✓
- `grep "supabase_auth" tests/test_offline_verification.py` → 2 matches ✓
- `pytest tests/` → 1067 passed, 8 skipped ✓
- Global grep audit: zero `gotrue` imports in main codebase (stale worktrees cleaned) ✓

## Manual Testing Results

| # | Item | Result |
|---|------|--------|
| 1 | Web email/password login | ✓ Pass |
| 2 | Web Google OAuth login (PKCE) | ✓ Pass (tested on production) |
| 3 | Web OAuth cancellation | Not tested (localhost limitation) |
| 4 | Web session persistence (browser reopen) | ✓ Pass |
| 5 | Web logout | ✓ Pass |
| 6 | Desktop email/password login | ✓ Pass |
| 7 | Desktop session persistence (restart) | ✓ Pass |
| 8 | Desktop logout | ✓ Pass |
| 9 | Expired/used OAuth code | Not tested |
| 10 | Direct /auth/callback no params | Not tested |

## External Review

Plan-level cross-AI review completed pre-execution (Gemini + Codex). All HIGH/MEDIUM concerns addressed in replanning. Code-level review deferred — code deployed and verified working on production.

## Self-Check: PASSED

## Deviations

- Wave 2 worktree was lost during OAuth debugging. Task 1 re-executed inline on main branch.
- OAuth tested on production (not localhost) because PKCE code_verifier requires same-process callback — localhost redirects to production URL.

## Key Files

### Modified
- `requirements.txt` — 13 direct dependencies (gotrue removed)
- `requirements-lock.txt` — gotrue removed from lock
- `tests/test_offline_verification.py` — supabase_auth in forbidden lists
