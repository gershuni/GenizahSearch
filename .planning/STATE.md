---
gsd_state_version: 1.0
milestone: v7.12
milestone_name: Multitenant Architecture (Path B)
status: defining_requirements
last_updated: "2026-05-13T00:00:00.000Z"
last_activity: 2026-05-13 -- Milestone v7.12 started
progress:
  total_phases: 0
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-05-13)

**Core value:** Researchers can find what they need in the Genizah corpus
**Current focus:** v7.12 Multitenant Architecture (Path B) — refactor web layer off desktop-inherited single-user model

## Current Position

Phase: Not started (defining requirements)
Plan: —
Status: Defining requirements
Last activity: 2026-05-13 -- Milestone v7.12 started

## Milestone Goal (v7.12)

Refactor GenizahSearch's web layer off the desktop-inherited single-user mental model so per-user state, auth, and caches cannot leak across concurrent sessions sharing one Python process.

**Hard constraint (Codex finding):** no mid-flight `auth.set_session()` calls — verified at `gotrue_client.py:713` that it is networked, not local-only.

## Phase Sketch (continued numbering from 86)

1. **Phase 87** — Foundations: session UUID + safe_storage chokepoint adoption
2. **Phase 88** — State separation by deletion (export_state.py becomes sole source of truth)
3. **Phase 89** — Lists cache redesign (per-request UserListsManager)
4. **Phase 90** — Auth caching rewrite (refresh-only locking keyed by _session_uuid)
5. **Phase 91** — Atomic auth state writes (server revocation before token pop)
6. **Phase 92** — Final sweep + acceptance (zero raw app.storage.user outside whitelist)

Roadmapper produces canonical phase list in `.planning/ROADMAP.md`.

## Investigation Summary (pre-milestone)

**Origin:** v7.11.1 shipped 2026-05-12 closing 4 user-reported bugs. Cross-user xlsx export filename leak was the headline bug, fixed by routing export payload through per-session storage (commit `242664d3`).

**4 rounds of Codex review** of the post-release commits surfaced that the cross-user export was one instance of a class of multitenant bugs spanning:
- `web/state.py:AppState` singleton with per-user fields (`last_results`, `current_search_query`, `parallels_*`)
- `web/user_lists.py:UserListsManager` instance singleton via `AppState._user_lists_mgr`
- `web/supabase_client.py` process-wide `_client_cache + _session_locks` keyed first by `id(storage)` (object-id reuse after GC), then by `access_token` (rotates on refresh)
- 30+ raw `app.storage.user.get(...)` bootstrap sites that can 500 on prune-mid-flight `AssertionError`
- `web/auth_state.py:clear_auth/do_login` + OAuth callback with non-atomic multi-step writes across auth boundary
- `web/components/filter_panel.py:persist_value` raw read+write (now safe-wrapped in commit cca23db3)

The 5 hold commits (`22b45f68 → cca23db3`) on `master-main` past production are partial fixes superseded by intentional v7.12 work.

**Carryover from hold commits:**
- KEEP: `web/safe_storage.py` module + helpers (aab16e6d); `safe_user_get` migrations in search/parallels/filter_panel (8ac93eff); `persist_value` safe-wrap + more bootstrap-read migrations (cca23db3)
- DISCARD: `UserListsManager._cache_entry` tuple (22b45f68 — superseded by per-request); access_token-keyed client cache (8ac93eff — superseded by refresh-only UUID-keyed locking); auth-resurrection guard (cca23db3 — obsolete once `get_user_client` cache is gone)

**Codex review transcripts** (audit trail input):
- `_tmp/codex_post_711_review_prompt.md` + `.._response.txt`
- `_tmp/codex_critical_high_review_prompt.md` + `.._response.txt`
- `_tmp/codex_3rdpass_review_prompt.md` + `.._response.txt`
- `_tmp/codex_4thpass_review_prompt.md` + `.._response.txt`

## Test Coverage to Preserve / Rewrite

| File | Purpose | Path B fate |
|---|---|---|
| `tests/test_export_cross_user_isolation.py` | 3 tests for user-reported xlsx leak | RE-WRITE to assert against per-session storage directly, no `_TEST_BACKEND` shim |
| `tests/test_user_lists_cache_isolation.py` | 3 tests for UserListsManager cross-user | RE-WRITE to match per-request model |
| `tests/test_safe_storage.py` | 6 tests for the helpers | KEEP as-is |
| `tests/test_browse_state.py` | 7 tests including pruned-session AssertionError | KEEP |

## Accumulated Context

### Architectural Constraints (carry-over from prior milestones)

- **Dual app maintenance:** All shared logic lives in `genizah_core.py` and `shared/*`. UI is app-specific (web/, desktop/).
- **Desktop is single-user by design:** v7.12 scope is web-only. Desktop app is unaffected by Path B.
- **NiceGUI session storage:** `app.storage.user` is per-browser-session, pruned periodically. Reads must tolerate prune-mid-flight via safe_storage helpers.
- **Server posture (2026-05-12):** detached HEAD at `v7.11.1` tag (commit `242664d3`). Do NOT run `deploy.sh` until Path B is ready — it will pull `master-main` and move prod to `cca23db3` (recall-grade per Codex).

### Key References

- `web/state.py:26` — `AppState` singleton with 9 per-user fields to evict
- `web/user_lists.py` — `UserListsManager` + `_cache_entry`
- `web/supabase_client.py:34` — `_client_cache` / `_session_locks` to delete
- `web/auth_state.py` — `set_auth/clear_auth/do_login`
- `web/main.py:1456+` — OAuth callback site
- `web/safe_storage.py` — chokepoint adapter (lives at HEAD, was introduced in aab16e6d)
- `web/export_state.py` — emerging sole source of truth for export selection
- `web/components/filter_panel.py:persist_value` — example of correct safe-wrap
- `gotrue_client.py:713` (installed Supabase auth) — proves `set_session()` is networked
- `.planning/HANDOFF_v7.11.1_path_b.md` — full handoff context

## Next Step

Roadmapper runs next to produce `.planning/ROADMAP.md` (6 phases, Phase 87 onward) and `.planning/REQUIREMENTS.md` (requirements grouped by phase).

After roadmap approval:

`/clear` then `/gsd-discuss-phase 87` (Foundations: session UUID + safe_storage chokepoint).
