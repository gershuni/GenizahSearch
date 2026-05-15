# Phase 90: Auth Caching Rewrite — No `set_session` — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `90-CONTEXT.md` — this log preserves the alternatives considered.

**Date:** 2026-05-15
**Phase:** 90-auth-caching-rewrite-no-set-session
**Areas discussed:** Token-application mechanism · Refresh strategy + lock keying · Plan decomposition + regression guards · External red-team round (Codex)
**Workflow:** User selected only "External red-team round (Codex)" from the 4 gray areas presented (same pattern as Phase 89). Claude drafted decisions across all 4 areas, sent to Codex for review at `_tmp/codex_phase90_discuss_review_prompt.md`, received verdicts at `_tmp/codex_phase90_discuss_review_response.txt`, synthesized into final CONTEXT.md.

---

## Gray Area Selection

| Option | Description | Selected |
|--------|-------------|----------|
| Token-application mechanism | Replace `set_session` with `postgrest.auth(token)` + sub-client headers | (delegated to Codex) |
| Refresh strategy + lock keying | Reactive vs proactive refresh; `_session_uuid`-keyed lock | (delegated to Codex) |
| Plan decomposition + regression guards | 1 vs 2 plans; AST scanner + runtime + behavioral tests | (delegated to Codex) |
| External red-team round (Codex) | Send proposal to Codex for review before locking CONTEXT.md | ✓ |

**User's choice:** External red-team round (Codex) only.
**User direction (verbatim shape, matching Phase 89):** Implicit "I'm non-technical for these decisions; ask Codex" — confirmed by selecting only the Codex option out of the 4 gray areas presented.
**Notes:** Same pattern as Phase 89 where the user said "All this is very technical for me. Ask Codex for its take." Locks in the Path B workflow style of external red-team after Claude proposes.

---

## Area 1 — Token-application mechanism (replacing `set_session`)

| Option | Description | Selected (post-Codex) |
|--------|-------------|----------|
| postgrest.auth(token) only | Local header on PostgREST sub-client; covers ~all .table() callers | |
| postgrest.auth + functions.set_auth | Both local-only; defensive for any future function callers | (Claude's original proposal) |
| postgrest.auth + functions.set_auth + storage header | All 3 sub-clients header-mutated; covers authenticated storage upload | ✓ (Codex F1) |
| Custom httpx auth flow at transport level | Overengineered; reject | |
| Hybrid (set_session on refresh, postgrest.auth otherwise) | Still has Codex CRITICAL on refresh path | |

**User's choice:** Delegated to Codex.
**Codex verdict (F1):** Claude's "PostgREST + functions covers everything" claim was FALSE. Two real-world counterexamples:
- `web/pages/profile.py:149-150` uses `client.auth.update_user({'password': ...})` for password change. GoTrue's `update_user` requires a local session via `get_session()` at `gotrue_client.py:690` — header mutation alone breaks this path.
- `shared/puzzle_publish_service.py:81 publish_join` and `:152 unpublish_join` use `client.storage.from_(STORAGE_BUCKET).upload/remove` — authenticated storage upload, not anonymous-only as Claude claimed.

**Final synthesis (CONTEXT.md D-01, D-02, D-03):** All 3 sub-clients get header mutation via `_apply_user_auth_to_client(client, access_token)`. Add a dedicated `change_password(new_password)` REST helper that issues direct `httpx.put` to `{SUPABASE_URL}/auth/v1/user` with bearer header, bypassing GoTrue entirely. Migrate `profile.py:149-150` to call it.

---

## Area 2 — Refresh strategy + lock keying

| Option | Description | Selected (post-Codex) |
|--------|-------------|----------|
| Reactive only | Refresh on JWT-expired exception, retry once | (Claude's original proposal) |
| Proactive (decode JWT exp, pre-refresh) | Refresh before token expires | ✓ primary (Codex F2) |
| Background worker | Per-session async refresh; rejected (no NiceGUI primitive) | |
| Combined: proactive primary + reactive defense-in-depth | Both | ✓ (final) |

**Lock keying:** `_session_uuid` (Phase 87 primitive) — Codex agrees with original proposal.

**User's choice:** Delegated to Codex.
**Codex verdict (F2):** Reactive-only is INSUFFICIENT as scoped. Only 4 of ~30 `get_user_client()` callers have JWT-expired retry blocks (`supabase_client.py:516, 756, 935, 1101`). The ~26 write paths return errors directly on exception. Under reactive-only refresh, every authenticated write would silently fail at token expiry (~60min cadence). Forces proactive refresh in `get_user_client()`.

**Codex refresh-race verdict (post-lock semantics):** Post-lock access-token expiry check + stale-snapshot comparison. Pure "token equals pre-lock snapshot" check is unsafe — if the snapshot was taken after another thread already refreshed, comparing-and-refreshing burns the newly-rotated token. The function must:
1. Read `auth_session` first; if no tokens, return False.
2. Lock by `get_session_uuid()`.
3. Re-read `auth_session` inside the lock.
4. If access token is now unexpired → return True (another thread refreshed).
5. If caller's `stale_refresh_token` differs from current → return True (another thread already rotated).
6. Otherwise refresh once and `safe_user_set('auth_session', ...)`.
Also: do NOT prune `_refresh_locks` on `sign_out` — defeats serialization if pruned while another thread holds the lock.

**Final synthesis (CONTEXT.md D-04, D-05, D-06, D-07, D-08):** Proactive refresh as primary strategy; reactive retry blocks kept as defense-in-depth. `_refresh_user_session(stale_refresh_token=...)` with post-lock re-read + expiry-check + stale-comparison. No lock pruning on sign_out. Throwaway clients used inside refresh.

---

## Area 1.5 — Anonymous singleton stays anonymous (Codex F3, surfaced during red-team)

This area was NOT in Claude's original gray-area presentation. Codex surfaced it during red-team review.

**Codex verdict (F3):** Even after `_client_cache` deletion, the singleton `get_client()` becomes authenticated via supabase's auth event listener (`supabase/_sync/client.py:338-346` — `_listen_to_auth_events` mutates `self.options.headers["Authorization"]` and `self.auth._headers["Authorization"]` on `SIGNED_IN`/`TOKEN_REFRESHED`/`SIGNED_OUT`). Today's `sign_in()` at line 254, `set_session_from_url()` at line 388, and `exchange_code_for_session()` at line 414 all call auth-mutating methods on the module singleton `get_client()` — making the singleton carry the most recently signed-in user's auth header. Subsequent unrelated callers receive an authenticated singleton. Real cross-user leak path that survives `_client_cache` deletion.

| Option | Description | Selected |
|--------|-------------|----------|
| Keep current behavior | Singleton mutates via auth events — UNSAFE | |
| Throwaway clients for bootstrap helpers | sign_in/set_session_from_url/exchange_code_for_session use fresh `create_client()` instances, discard post-call | ✓ |
| Disable supabase auth event listener | Would require monkey-patching client internals; brittle | |

**Final synthesis (CONTEXT.md D-09, D-10, D-11):** Bootstrap helpers refactored to use throwaway clients. Module singleton `get_client()` becomes provably anonymous-only. `sign_out` is partially refactored in Phase 90 (delete cache eviction block only); Phase 91 owns the full "use user's authenticated client, revoke first, pop second" refactor.

---

## Area 3 — Plan decomposition

| Option | Description | Selected (post-Codex) |
|--------|-------------|----------|
| 1 plan (single commit) | Everything at once | |
| 2 plans (Phase 89-style) | Behavior rewrite first, then deletion + enforcement | (Claude's original proposal) |
| 2 plans with EXPANDED 90-01 scope | Plus Codex F1/F2/F3 fixes inside 90-01 | ✓ (Codex-revised) |
| 3 plans | Sweep callers between rewrite and deletion | |

**User's choice:** Delegated to Codex.
**Codex verdict:** 2-plan split is fine BUT Plan 90-01 must include the storage/auth/proactive-refresh fixes (F1/F2/F3) — they're not deferrable to 90-02 without creating regressions.

**Final synthesis (CONTEXT.md D-13):**
- **Plan 90-01:** Behavior rewrite + ALL Codex F1/F2/F3 fixes + tests + comment + allowlist deletion. Cache globals + helpers + sign_out eviction block kept as dead code.
- **Plan 90-02:** Delete the 4 globals + 2 helper functions + 1 eviction block. Install 3 regression guards.

---

## Area 4 — Regression-guard scope

| Option | Description | Selected (post-Codex) |
|--------|-------------|----------|
| Static AST only | Lint scanner over web/ + tests/ | |
| Static + runtime attr-absence (Phase 89-style) | 2 test files | |
| Static + runtime + behavioral | 3 test files including refresh-lock concurrency test | ✓ |

**Sub-decisions:**
- **Static allowlist scope (Codex catch):** Per-helper, NOT shared. `.auth.set_session(...)` allowed only in `set_session_from_url`; `.auth.exchange_code_for_session(...)` allowed only in `exchange_code_for_session`.
- **Seed traps (Codex catch):** Original Claude proposed 4 seed traps; Codex added aliased-auth and singleton-resurrection forms — final D-15 has 6 seed traps minimum.
- **Threading test reliability (Codex catch):** Behavioral refresh-lock tests use `threading.Barrier`/`Event` for deterministic ordering and monkeypatched storage/client. NO real NiceGUI storage contexts in worker threads.

**User's choice:** Delegated to Codex.
**Final synthesis (CONTEXT.md D-15, D-16, D-17, D-18):** Three test files:
1. `tests/test_no_set_session_outside_oauth.py` — static AST scanner with per-helper allowlist + 6 seed traps (D-15).
2. `tests/test_no_client_cache_globals.py` — runtime attr-absence, parametrized over 6 deleted names (D-16).
3. `tests/test_refresh_lock_per_session.py` — deterministic Barrier/Event-based concurrency test for same-uuid serialization + distinct-uuid parallelism + stale-snapshot short-circuit (D-17).

---

## Pre-decided behind-the-scenes (no separate gray area)

### Caller signature stability

| Option | Description | Selected |
|--------|-------------|----------|
| Keep `get_user_client() -> Client` stable | No caller migration churn | ✓ |
| Change to context manager / explicit per-request binding | Forces 30+ caller refactor | |

Rationale: 30+ call sites use `client = get_user_client(); client.table(X).Y()`. Internal behavior change preserves the signature.

### Anonymous fallback

| Option | Description | Selected |
|--------|-------------|----------|
| Return anonymous singleton when no auth_session | Existing semantics; all callers tolerate it | ✓ |
| Raise when not authenticated | Forces explicit auth checks at 30+ call sites | |

Rationale: Existing callers already check `is_logged_in()` before doing user-scoped operations. Keeping the fallback preserves the read-paths where the anonymous client is the correct choice (`get_user_lists` reads work anonymously; only writes need auth).

---

## Codex's Discretion

The user explicitly delegated all technical decisions to Codex per the Phase 88/89 pattern. Codex's verdicts were treated as locked unless they conflicted with the Phase 87 chokepoint discipline (`safe_storage` only, `_session_uuid` cache key only) — none did.

## Deferred Ideas

Captured in CONTEXT.md `<deferred>` block. Briefly:
- ~~`sign_out` refactor to user-authenticated client + revocation-before-pop~~ — **pulled forward into Phase 90** per Codex round-2 P1.
- `web/auth_state.py:set_auth/do_login` migration (Phase 91 AUTHW-01). Phase 90 reorders `clear_auth` but does NOT migrate its pops to `safe_user_pop` (Phase 91 owns).
- `reset_client()` deletion (out of scope; legacy helper).
- Background proactive refresh worker (no NiceGUI primitive).
- JWT signature verification (defensive read-only check; not worth dependency).
- `_refresh_locks` pruning (unbounded growth accepted, matches Phase 89).
- Async-refresh via `asyncio.Lock` (out of scope per REQUIREMENTS.md "Async session storage").

---

## Codex Round 2 (user-supplied review of round-1 synthesis)

After the initial Codex round-1 synthesis was committed at `a9fecfaf`, the user ran a second Codex review pass and surfaced 4 additional findings. All were applied to CONTEXT.md without re-discussing with the user (same delegation pattern):

| # | Severity | Finding | Resolution |
|---|----------|---------|------------|
| P1 | BLOCKING | `sign_out` regression after D-10 — anonymous singleton breaks today's accidental token revocation | Pulled AUTHW-03 + AUTHW-04 forward from Phase 91 into Phase 90; encoded as D-11 (sign_out throwaway rewrite) + D-11b (clear_auth revoke-before-pop reorder with finally-block) |
| P1 | BLOCKING | `change_password` request headers incomplete — missing `apikey` which Supabase gateway requires | D-02 expanded to spell out all 4 headers (`apikey`, `Authorization: Bearer ...`, `Content-Type: application/json`, `Accept: application/json`) + JSON body shape, with reference to `gotrue_base_api.py:54-58` header-merge behavior |
| P2 | MEDIUM | D-15 chain-matching can't see aliases (`auth = client.auth; auth.set_session(...)`) | D-15 switched to terminal-attribute-name matching (`node.func.attr in {...}`); added Class B singleton-resurrection ban (`get_client().auth.<mutating>(...)`) with broader method set (`sign_in_*`, `refresh_session`, `update_user`, `sign_out`); 6 → 10 seed traps |
| P3 | LOW | Allowlist count inconsistency ("4 → 3" vs "3 → 2" in different paragraphs) | Verified by `grep -c "^  - file:" .planning/phase87_storage_allowlist.yaml` = 3; Phase 90 takes 3 → 2; both paragraphs fixed |

**Result:** Plan 90-01 scope expanded again. Final 90-01 contents:
- All Codex round-1 fixes (D-01 token application, D-04/D-05/D-06 proactive refresh, D-09/D-10 throwaway bootstrap)
- All Codex round-2 fixes (D-02 expanded headers, D-11/D-11b sign_out + clear_auth, D-15 widened scanner)
- Tests + comments + allowlist deletion (3 → 2)
- All-pytest-green plan boundary

**Behavior change in 90-01 (newly user-visible after round 2):** Logout now actually revokes the user's refresh token server-side via the user-authenticated throwaway. Today's behavior depends on the event-listener-leak accidentally revoking some token; tomorrow's behavior is correct revocation of the right token.

---

*Audit-trail-only document.*
