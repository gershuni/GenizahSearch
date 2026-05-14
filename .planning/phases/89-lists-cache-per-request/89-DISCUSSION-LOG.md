# Phase 89: Lists Cache Per-Request — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-14
**Phase:** 89-lists-cache-per-request
**Areas discussed:** Migration mechanism, Cache fate + invalidate_cache() API, Plan decomposition + static enforcement, Anonymous-user path

---

## Initial Gray-Area Selection

User answered "All this is very technical for me. Ask Codex for its take." → triggered the Phase 88 pattern of Claude proposes + Codex red-teams + user picks the synthesis.

| Option | Description | Selected |
|--------|-------------|----------|
| Migration mechanism | Factory-property vs. module-level helper at ~70 call sites | (all areas sent to Codex) |
| Cache fate + invalidate_cache() API | Stateless vs. per-request memoization; delete vs. no-op | (all areas sent to Codex) |
| Plan decomposition + static enforcement | 1 vs. 2 vs. 3 plans; whether to install static AST guard | (all areas sent to Codex) |
| Anonymous-user path unification | Collapse dual-type return into unified UserListsManager | (all areas sent to Codex) |

**User's choice:** Run all four through Codex external review.
**Notes:** Same pattern as Phase 88. User documented preference in auto-memory: "I have no way to answer technical questions. Let's ask for your advice, then ask external AIs as well."

---

## Codex Red-Team Round

Prompt: `_tmp/codex_phase89_discuss_review_prompt.md`
Response: `_tmp/codex_phase89_discuss_review_response.txt`

### Codex Verdicts by Area

**Area 1 — Migration mechanism (factory-property):**
- Codex: "Not fully correct." Factory-property is per-ACCESS, not per-request. Cited `web/api.py:2114, 2117, 2122` (3 reads in one request = 3 different managers), `web/pages/search.py:2129-2130`, `web/pages/discoveries.py:1472, 1489, 1495` (repeated accesses inside one UI flow).
- Codex: Pre-bootstrap None contract MUST be preserved. `state.lists_mgr` is `None` until `_local_lists_mgr` exists today; many `if state.lists_mgr:` callers rely on this. A bare factory returning `UserListsManager(None, None)` breaks the guard.
- Codex's revision: factory-property is acceptable as compatibility bridge IF stateless fetch (Area 2) removes the cache-staleness class of bug. Add `if self._local_lists_mgr is None: return None` to preserve pre-bootstrap behavior.

**Area 2 — Cache fate (per-request memoization vs. stateless):**
- Codex: Rejected `_cached_data` per-instance memoization. UI dialog callbacks capture `lists_mgr` references and reuse them later (`add_to_list_dialog.py:84` captured → reused at 194, 205, 243; `project_tree.py:60-62` → reused at 236-242, 397, 467, 557, 611, 651, 735). A "per-request" manager becomes long-lived once captured into a dialog lambda. With no TTL + no user-id guard, `_cached_data` serves stale data indefinitely in that captured manager.
- Codex's revision: **pure stateless fetch.** Always hit Supabase when authenticated. `invalidate_cache()` stays as compatibility no-op. This is what most literally satisfies LISTS-03.

**Area 3 — Plan decomposition (2 plans, static AST guard):**
- Codex: 2 plans fine, but **caught a definite red test:** `tests/test_no_appstate_export_fields.py:67` (Phase 88 survivor list) includes `'_user_lists_mgr'`. Plan 89-02 MUST update this in the SAME commit as field deletion.
- Codex: Static guard scope needs to be stronger than Claude proposed. Should catch (a) `self._user_lists_mgr` inside `web/state.py`, not only `state._user_lists_mgr`; (b) `_cache_entry` AND `_cache_ttl` attribute access anywhere; (c) `def init_user_lists_mgr` function-def (not only call sites); (d) `getattr/setattr/hasattr` indirect forms for Phase 88-level coverage; (e) seed traps must be parsed as code snippets (Phase 88 pattern), not raw strings.
- Codex: Plan ordering `89-01 → 89-02` is acceptable if 89-01 rewrites the old `_cache_entry` tests in the same commit as cache removal, AND 89-02 deletes the singleton and updates Phase 88's survivor test together.

**Area 4 — Anonymous path unification:**
- Codex: Claude's framing was **inaccurate**. Today `web/main.py:1508` calls `state.init_user_lists_mgr()` unconditionally, so `web/state.py:38-39` returns `UserListsManager` for anonymous users too (post-bootstrap). The property does NOT branch on auth — it branches on `_user_lists_mgr is not None`, which is True for everyone after startup.
- Codex caught **exact delegation bug:** `web/user_lists.py:667, 682` call `self.local_mgr.create_project(name, color)` but `genizah_core.py:9631` defines `ListsManager.create_project(self, name)` — no `color` arg. Anonymous project creation raises `TypeError` today.
- Codex caught **second pre-existing bug:** `web/pages/lists.py:344, :349` call async `UserListsManager.update_item_note`/`update_item_tags` without `await`. Coroutine silently dropped in authenticated path. Pre-existing, NOT Phase 89-caused.
- Codex's revision: skip the "unification" framing (it's wrong — already unified); add a delegation signature audit pass to Plan 89-01 instead, fix `create_project` color arity, flag async-drop as deferred (out of Phase 89 scope).

### Success Criteria Per Codex
- SC#1 (no `_user_lists_mgr`): yes, after Plan 89-02 AND survivor test update.
- SC#2 (no `_cache_entry`, no `_cache_ttl`): yes, if cache logic + stale comments are removed.
- SC#3 (cross-user leak): yes with stateless fetch. With `_cached_data` per-instance memoization, leakage probably fixed but callback-captured managers introduce a new stale-data bug class.
- SC#4 (test rewrite): rewrite around behavior, not private cache internals.

---

## Final Lock-In

| Option | Description | Selected |
|--------|-------------|----------|
| Lock as proposed | Codex-revised design across all four areas | ✓ |
| Lock but skip create_project fix | Defer signature mismatch fix to separate commit | |
| Revise further | Send back to Claude with concerns | |

**User's choice:** Lock as proposed.

| Option | Description | Selected |
|--------|-------------|----------|
| New file | tests/test_no_deleted_lists_state_references.py (Phase 89-specific) | ✓ |
| Extend Phase 88 file | Add new disallowed names to existing scanner | |

**User's choice:** New file for the static guard.
**Notes:** Matches Phase 88's pattern where each phase owns its guard. Cleaner audit trail per phase.

---

## Claude's Discretion (deferred to planner)

- Body of `invalidate_cache()` after D-05: `pass` with docstring vs. trivial `self._cached_data = None` with a `_cached_data` field initialized for symmetry.
- Whether to fix `create_project` color-arity by adding `color=None` to `ListsManager.create_project` in `genizah_core.py:9631` OR by dropping `color` in the wrapper's local-mgr call. Depends on blast-radius across desktop app `ListsManager` callers.
- Runtime attr-absence test and static AST test live in 1 file or 2 (Phase 88 used 2; user's "new file" choice already implies the 2-file split).

---

## Deferred Ideas (carried to CONTEXT.md `<deferred>` section)

- True per-request lifecycle (handler-boundary explicit binding) — ~70 call-site churn; defer to Phase 92 sweep or future phase if needed.
- Pre-existing async-drop bug at `web/pages/lists.py:344, :349` — separate hotfix, NOT Phase 89.
- Other delegation signature drift surfacing during audit — planner decides scope.
- Future server-side cache keyed by `_session_uuid` if perf needs it — not on the v7.12 table.
- `AppState.__setattr__` guard for deleted names — D-10 static scanner already covers this; not adopted.
