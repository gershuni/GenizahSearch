---
phase: 90
reviewers: [codex]
review_round: 2
reviewed_at: 2026-05-15T08:29:33Z
plans_reviewed: [90-01-PLAN.md, 90-02-PLAN.md]
prior_round_in_git: "Round 1 REVIEWS.md committed as 55aea942 (replaced by this round-2 file; round-1 findings appear in 'Fix Verification' table below and in the plans' own 'Codex Review Round 1 — Applied Fixes' traceability sections)"
---

# Cross-AI Plan Review — Phase 90 (Round 2, Auth Caching Rewrite -- No set_session)

## Codex Review (Round 2)

**Summary**
Not quite execution-ready. The substantive architecture fixes are mostly in place, including PKCE persistence, `admin.sign_out`, refresh cleanup, and alias-aware scanner coverage. But there are still two self-failing acceptance gates and a few smaller execution issues that will likely trip the executor or CI.

**New Concerns**

| Severity | Issue |
|---|---|
| **HIGH** | H2 is still partially broken: Plan 90-01 tells the executor to add production comments/docstrings containing exact `.set_session(` substrings, then later requires `grep -rn "\.set_session(" web/` to match only `set_session_from_url`. Examples: `90-01-PLAN.md:209`, `526`, while the failing grep gates remain at `1301` and `1319`. Either remove those exact substrings from comments/docstrings or delete the plain grep gates and use only AST call-node checks. |
| **HIGH** | Plan 90-02's deleted-name grep will self-fail. Plan 90-01 adds production comments/docstrings containing `_session_locks`, `_client_cache`, and `_clear_stale_auth` (`90-01-PLAN.md:187`, `325`, `638`), but Plan 90-02 requires zero matches in `web/supabase_client.py` (`90-02-PLAN.md:1146`, `1270`, `1286`). Rewrite production comments to avoid exact deleted identifiers, or make the zero-match requirement AST/runtime-only. |
| **MEDIUM** | `_refresh_user_session()` says missing persisted UUID means caller falls back anonymous, but `get_user_client()` ignores the return value. If refresh is skipped or fails while the token remains near-expiry, lines `90-01-PLAN.md:558-566` still build a user client with the stale token. Add `refreshed = _refresh_user_session(...)` and return anonymous if the post-refresh token is still near-expiry and refresh failed. |
| **MEDIUM** | Task 1's acceptance grep is too short. `_refresh_user_session` starts at `90-01-PLAN.md:308`; `except AuthApiError` is at `372`, and `safe_user_pop('auth_session')` is at `395`, but the check at `481` uses `grep -A60`, so it likely misses both. Use `-A120` or an AST/function-range check. |
| **MEDIUM** | `tests/test_auth_revocation_and_headers.py` includes unused imports: `patch` and `pytest` at `90-01-PLAN.md:1119-1121`. CI runs `ruff check .` (`.github/workflows/ci.yml:17`), so this can fail. Drop both unused imports. |

**Fix Verification**

| Prior Finding | Status | Notes |
|---|---|---|
| H1 PKCE verifier lifetime | **RESOLVED** | `get_oauth_url` persists `oauth_code_verifier` and `exchange_code_for_session` pops/passes it (`90-01-PLAN.md:706-774`, `893-942`). |
| H2 grep self-defeat | **PARTIAL** | AST checks were added, but plain grep gates and production comment substrings still conflict (`90-01-PLAN.md:526`, `1301`, `1319`). |
| H3 refresh cleanup | **RESOLVED with minor gate issue** | Terminal `AuthApiError` cleanup exists (`90-01-PLAN.md:372-396`), but its acceptance grep window is too short. |
| M1 persisted UUID | **RESOLVED/PARTIAL** | `get_persisted_session_uuid()` added (`90-01-PLAN.md:269-300`), but `get_user_client()` does not honor a failed refresh return. |
| M2 sign_out docstring/body | **RESOLVED** | Body and tests pin `admin.sign_out(access_token, "global")` (`90-01-PLAN.md:817-852`, `1097-1143`). |
| M3 unit tests | **PARTIAL** | Good behavioral coverage added, but generated test imports will trip ruff unless cleaned. |
| M4 Class B alias scanner | **RESOLVED** | Intra-function alias tracking and aliased seed traps added (`90-02-PLAN.md:277-348`, `423-446`). |
| M5/M6 xfail detection | **RESOLVED** | AST decorator checks replace brittle grep (`90-02-PLAN.md:587`, `1149`). |
| M7 SUMMARY commit | **RESOLVED** | Separate summary commit clarified (`90-02-PLAN.md:1119-1140`). |
| L1 perf sanity | **RESOLVED structurally** | Separate append commit now specified (`90-02-PLAN.md:1200-1237`). |
| L2 unused scanner import | **RESOLVED** | No unused `os` in generated scanner. |
| L3 Test C description | **RESOLVED** | Sequential nature is now explicit (`90-02-PLAN.md:612-617`). |
| B1 `create_client` dict-options | **RESOLVED** | Bare 2-arg `create_client` used (`90-01-PLAN.md:745-752`, `937-940`). |
| B2 `throwaway.auth.sign_out()` check | **RESOLVED** | AST call-node check avoids comment false positives (`90-01-PLAN.md:968`). |
| W3 Task 3 split | **RESOLVED** | Split into Task 3a/3b (`90-01-PLAN.md:609`, `981`). |
| W4 Task 5 ordering | **RESOLVED** | Perf sanity is a separate append commit (`90-02-PLAN.md:1200-1237`). |
| W5 success wording | **PARTIAL** | AST wording improved, but contradictory plain grep gates remain. |
| N6 vacuous assertion | **RESOLVED** | `or True` removed (`90-01-PLAN.md:479`). |

**Suggestions**
1. Replace production comments like ``auth.set_session()`` and ``client.auth.set_session(at, rt)`` with text that does not contain `.set_session(`, e.g. "the GoTrue session-setting API".
2. Remove exact deleted identifiers from production comments/docstrings before Plan 90-02, especially `_session_locks`, `_client_cache`, and `_clear_stale_auth`.
3. Change `get_user_client()` to check refresh outcome before applying a near-expiry token.
4. Add one small PKCE unit test if time permits: mock `throwaway.auth._storage` and verify `get_oauth_url -> exchange_code_for_session` stores, pops, and passes the verifier.
5. Remove unused `patch` / `pytest` imports from the new Plan 90-01 test file.

**Risk Assessment**
Current risk: **MEDIUM-HIGH** because the executor will likely hit self-failing grep gates and CI may fail ruff. After the grep/comment cleanup, refresh-return handling, and unused-import cleanup, I would consider the plans execution-ready at **MEDIUM** risk, appropriate for this auth-boundary rewrite.

---

## Consensus Summary

Single-reviewer run (Codex only, second pass).

### Top Concerns (must-address before execute)

1. **HIGH — H2 grep self-defeat, second wave.** Round-2 fixed the `throwaway.auth.sign_out()` and `set_session(` *Task 3a* gates with AST checks, but the plan's `<success_criteria>` and Plan 90-02's deletion-verification still contain plain `grep -rn "\.set_session(" web/` and `grep -rn "_session_locks\|_client_cache\|_clear_stale_auth" web/supabase_client.py` that will trip on the legitimate documentation substrings in 90-01's comments/docstrings. Either rewrite those production comments to avoid the literal substrings (preferred — comments can describe the anti-pattern without typing it verbatim) OR delete the plain grep gates and rely exclusively on AST checks.

2. **MEDIUM — Refresh return value not honored.** `_refresh_user_session()` is wired to fall back anonymous on missing persisted UUID or terminal error, but `get_user_client()` never checks its return — it just applies the (possibly stale) token. Add `refreshed = _refresh_user_session(...)` and on `False` either return the anonymous client or re-check token expiry before building the authenticated client.

3. **MEDIUM — Acceptance grep windows too short.** Task 1's `grep -A60 "def _refresh_user_session"` won't span to line 395 where the cleanup logic lives. Use `-A120` or an AST/function-range scan.

4. **MEDIUM — CI ruff failures from generated test imports.** `tests/test_auth_revocation_and_headers.py` imports `patch` and `pytest` but doesn't use them. CI `ruff check .` will fail.

### Lower-priority
- Optional: add a PKCE-specific unit test mocking `throwaway.auth._storage` to assert get_oauth_url → exchange_code_for_session round-trips the verifier.

### Agreed Strengths (Round 1 + Round 2 fixes that landed)
- PKCE verifier persistence wired correctly (H1)
- `admin.sign_out(access_token, "global")` pinned in body + tests (M2)
- AST-based Class A and Class B scanners with intra-function alias tracking (M4)
- AST decorator detection for xfail (M5/M6)
- Separate SUMMARY commit + perf sanity append commit (M7/W4)
- `create_client` 2-arg form bug fixed (B1)
- AST check for `throwaway.auth.sign_out()` instead of literal grep (B2)
- Task 3 split into 3a/3b (W3)
- Vacuous `or True` assertion fixed (N6)

### Divergent Views
N/A — single-reviewer run.

## How to incorporate

```
/gsd-plan-phase 90 --reviews
```

Targeted fix list for round 3:
- Rewrite 90-01-PLAN.md lines 209 + 526 + 187 + 325 + 638 to remove literal `.set_session(`, `_session_locks`, `_client_cache`, `_clear_stale_auth` substrings from production-bound comments/docstrings (use prose descriptions like "the GoTrue session-setting helper", "the deleted cache globals", "the resurrection guard").
- OR delete the plain grep gates at 90-01-PLAN.md:1301, 1319 and 90-02-PLAN.md:1146, 1270, 1286 in favor of the existing AST checks.
- Update Task 1 acceptance to `grep -A120` or AST function-range scan to cover 90-01-PLAN.md:308-395 cleanup logic.
- Make `get_user_client()` honor `_refresh_user_session()` return value (rebuild logic at 90-01-PLAN.md:558-566).
- Drop unused `patch` and `pytest` imports from `tests/test_auth_revocation_and_headers.py` template (90-01-PLAN.md:1119-1121).
- Optional: add a mocked PKCE round-trip test asserting `throwaway.auth._storage` set/pop/pass behavior across `get_oauth_url` → `exchange_code_for_session`.
