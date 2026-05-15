# Phase 91: Atomic Auth State Writes — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-15
**Phase:** 91-atomic-auth-state-writes
**Areas discussed:** External Codex red-team round (user-selected — only area)

---

## Gray Areas Presented (multiSelect)

| # | Area | Description | Selected |
|---|------|-------------|----------|
| 1 | Atomicity model | Three sequential safe_user_set calls (1:1 substitution, current shape) vs. one composite-key write (auth_user+auth_profile+auth_session collapsed into a single `_auth_block` storage key — eliminates half-state by construction). 90-CONTEXT explicitly defers the composite-key option here. Affects auth_state.py + main.py + Phase 87 lint scanner expected_count math. | |
| 2 | AUTHW-05 test shape | What failure mode the prune-resilience test asserts and how (Phase 87 `monkeypatch.setattr('web.safe_storage.app', ...)` style vs. Phase 90 D-17 thread-routed app proxy). Whether the callback's user-visible behavior on prune is silent redirect, error page, or login-retry. | |
| 3 | Plan decomposition | Single plan vs. two plans. Options: (a) one plan = all 4 reqs in one commit; (b) auth_state.py migration first, OAuth callback + test second; (c) migration plan + enforcement-guard plan (mirroring 90-01/90-02 split). | |
| 4 | External Codex red-team round | Same pattern as Phases 88/89/90 — Claude proposes the synthesis, Codex CLI red-teams it, user picks the result. Phases 88/89/90 each surfaced blocking findings that pure Claude analysis missed. | ✓ |

**User's choice:** "External Codex red-team round" (only).
**Notes:** Same pattern as Phases 88, 89, 90 — user explicitly delegates technical synthesis to Codex review. Claude drafts the full proposal across all 4 areas, Codex red-teams it, Claude synthesizes the result into CONTEXT.md.

---

## External Codex red-team round

**Proposal sent to Codex:** `_tmp/codex_phase91_discuss_review_prompt.md`
**Codex response:** `_tmp/codex_phase91_discuss_review_response.txt`

### Claude's draft proposal (pre-Codex)

Synthesized across all 4 gray areas:

1. **Atomicity model:** Collapse 3 keys (`auth_user`, `auth_profile`, `auth_session`) into a single composite `_auth_block` storage key. Single write = no half-state. Add `_read_auth_block()` migration helper for backward compatibility with pre-Phase-91 logged-in users. Update `web/supabase_client.py` 8 sites to read/write composite. Rewrite `GlobalAuthState` 5 methods. Rewrite `complete_login` to single composite write.
2. **AUTHW-05 test shape:** Three cases (T-A prune-pre-write, T-B happy-path, T-C post-prune redirect). Phase 87 monkeypatch pattern.
3. **Plan decomposition:** 2 plans (91-01 composite migration + reader updates + test; 91-02 static guards + retention).
4. **Codex red-team round:** This step.

### Codex's verdicts

| Finding | Severity | Location/Detail | Resolution |
|---------|----------|-----------------|------------|
| F1 | BLOCKING | `_auth_block` is not atomic once refresh becomes RMW. `web/supabase_client.py:191` becomes block-rewrite — concurrent refresh + logout (B pops while A is mid-refresh) resurrects logged-out state; concurrent refresh + profile update clobbers each other. Composite requires copy-on-write + per-session block lock + version-stamping. | **Architectural pivot: keep 3 keys, do not consolidate.** Encoded in `<domain>` and across D-01 through D-07. |
| F2 | BLOCKING | Migration helper in `auth_state.py` would be bypassed by `supabase_client.py:159, 279, 417, 459` readers — they'd see legacy users as anonymous. | **Mooted by F1** — no migration helper needed when keeping 3 keys. |
| F3 | BLOCKING | `tests/test_no_raw_storage_access.py:200` hard-asserts non-empty allowlist (`assert entries, ...`). Plan must update before claiming pytest green. | Encoded as D-07 — replace assertion with comment + explanation; for-loop validators still run. |
| M1 | MEDIUM | `_read_auth_block()` pop-ordering issue + concurrent-migrator race. | Mooted by F1. |
| M2 | MEDIUM | `do_login()` must check `safe_user_set` return value to avoid reporting success on storage-failure. | Encoded as D-05. |
| M3 | MEDIUM | T-C unreliable — `ui.navigate.to('/')` does not synchronously execute home-page route handler in tests. | T-C reshaped per D-08: now asserts `GlobalAuthState.get_user()` under pruned storage returns None without `AssertionError`. |
| M4 | MEDIUM | Test rewrites for composite-key risk weakening Phase 90 refresh-lock coverage. | Mooted by F1 — no test rewrites needed (kept 3-key contract). |
| M5 | MEDIUM | AUTHW-06 retention guard — AST justified over grep because `persist_value` contract is function-local (must read `session_persistence_enabled` AND write via `safe_user_set`). | Encoded as D-09 — 3 AST assertions + 2 seed traps. |

### Codex's Decision verdict

> "I would not approve composite-key migration as written. There is a strong argument to keep the three legacy keys and just migrate raw access to `safe_user_*`: token refresh is a separate high-frequency update domain from user/profile cache, and NiceGUI storage gives you no compare-and-swap."

This drove the **entire phase shape pivot** from composite-key consolidation to direct raw→safe_user_* substitution. Surface reduced ~70%:
- **Original:** 8 `supabase_client.py` reader updates + 7 test file rewrites + migration helper + new `_auth_block` key contract.
- **Revised:** 12 in-place substitutions + 2 small return-value checks + 2 new test files. Zero changes to `supabase_client.py`. Zero changes to existing tests.

---

## Claude's Discretion

Captured in CONTEXT.md `<decisions>` §"Claude's Discretion":
- `set_auth` returns `bool` vs. void+raise (recommend bool per D-04).
- `do_login` `auth_session`-first vs. `set_auth`-first ordering (recommend auth_session-first for smaller blast radius on rollback).
- Exact wording of user-facing error message ("Session storage unavailable. Please try again.").
- Whether `update_profile_cache` needs return-value check (recommend `False` — best-effort write).

---

## Deferred Ideas

Captured in CONTEXT.md `<deferred>`:
- Composite-key `_auth_block` consolidation (Codex F1 rejected, captured for future if cross-key race surfaces in production).
- `GlobalAuthState` 11 helper methods consolidation (hot-path memoization).
- `update_profile_cache` rollback semantics + UI toast on failure.
- Redundant `try/except Exception` wrappers elsewhere in codebase (covered by /gsd-cleanup pass).
- OAuth callback "Retry login" button.
- PostHog `login_failed` reason instrumentation dashboard.
