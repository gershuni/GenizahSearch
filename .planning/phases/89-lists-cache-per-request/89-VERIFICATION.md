---
phase: 89-lists-cache-per-request
verified: 2026-05-15T12:00:00Z
status: passed
score: 4/4 must-haves verified
overrides_applied: 0
roadmap_success_criteria:
  - "SC#1: Static grep of web/state.py:AppState returns zero matches for _user_lists_mgr"
  - "SC#2: Static grep of web/user_lists.py returns zero matches for _cache_entry and 10s TTL constant"
  - "SC#3: Cross-user lists isolation — User B sees their own lists, not User A's (within TTL window)"
  - "SC#4: tests/test_user_lists_cache_isolation.py passes against per-request model"
phase_90_handoff_ready: true
---

# Phase 89: Lists Cache Per-Request — Verification Report

**Phase Goal:** Drop the `UserListsManager` singleton and 10s TTL plumbing entirely; per-request instantiation becomes the simpler safe pattern.

**Verified:** 2026-05-15
**Status:** PASSED
**Re-verification:** No — initial verification.
**v7.12 Path B Progress:** 3 of 6 phases complete (87, 88, 89). Next: Phase 90.

---

## Goal Achievement

### Observable Truths (Success Criteria)

| # | Truth (ROADMAP SC) | Status | Evidence |
|---|--------------------|--------|----------|
| 1 | SC#1: `grep -rn "_user_lists_mgr" web/state.py` returns 0 lines (singleton field gone) | VERIFIED | Grep on `web/state.py` returns no matches. Grep on entire `web/` directory returns no matches. AppState class definition (lines 14-25) shows only `_local_lists_mgr` field — no `_user_lists_mgr` initialization or method. |
| 2 | SC#2: `grep -n "_cache_entry\|_cache_ttl" web/user_lists.py` returns 0 lines (10s TTL cache gone) | VERIFIED | Grep on `web/user_lists.py` returns no matches. Grep on entire `web/` directory returns no matches. `UserListsManager.__init__` (lines 59-75) sets only `local_mgr` and `meta_mgr` — no cache fields. `_get_cached_data()` (lines 119-173) is stateless: fetches Supabase fresh on every authenticated call. |
| 3 | SC#3: Cross-user lists isolation — User B does not see User A's lists within TTL window | VERIFIED | `test_authenticated_fetch_does_not_leak_across_users` (factory-access case) PASSES. `test_captured_manager_does_not_serve_stale_data_after_user_switch` (captured-manager case, R1 review addition) PASSES — proves D-03 closure-capture bug class is structurally impossible. Both tests patch `GlobalAuthState` user-id and assert distinct Supabase calls per user. |
| 4 | SC#4: `tests/test_user_lists_cache_isolation.py` passes and is per-request model (no TTL/user-id-key/singleton references) | VERIFIED | File contains 4 behavior tests: `test_two_accesses_get_distinct_managers`, `test_authenticated_fetch_does_not_leak_across_users`, `test_captured_manager_does_not_serve_stale_data_after_user_switch`, `test_invalidate_cache_is_compatibility_no_op`. All 4 PASS. Test file only references `_user_lists_mgr` in one docstring comment explicitly explaining why the test fixture does NOT touch the deleted field (line 46). Zero atomic-tuple cache assertions remain. |

**Score:** 4/4 ROADMAP success criteria verified.

---

### Required Artifacts (Level 1-4 Verification)

| Artifact | Expected | Exists | Substantive | Wired | Data Flows | Status |
|----------|----------|--------|-------------|-------|------------|--------|
| `web/state.py` | AppState minus `_user_lists_mgr` field + `init_user_lists_mgr` method; factory `lists_mgr` @property | YES (75 lines, down from ~92) | YES (factory returns fresh UserListsManager per access, pre-bootstrap guard returns None) | YES (used by `web/api.py:2114`, `web/components/comment_dialog.py:93`, `web/pages/lists.py:218`) | YES (verified `python -c` instance: `_user_lists_mgr=False`, `init_user_lists_mgr=False`, `lists_mgr=None` pre-bootstrap) | VERIFIED |
| `web/user_lists.py` | UserListsManager stateless; `_cache_entry`/`_cache_ttl` deleted; `invalidate_cache` no-op; `import time` removed | YES (936 lines, down ~64) | YES (`_get_cached_data` always hits Supabase; R2 user_id-None guard added) | YES (imported lazily in state.py:51; ~10 internal mutation paths call `invalidate_cache()` no-op) | YES (R6 audit confirmed all 27 `local_mgr.X` call sites CLEAN; signatures verified against genizah_core.py) | VERIFIED |
| `web/main.py` | `state.init_user_lists_mgr()` call deleted from line 1508 | YES (1608 lines, down 3) | YES (bootstrap flows `state.lists_mgr = ListsManager(state.meta_mgr)` → `state.lab_engine = LabEngine(...)` directly) | YES (per-access factory takes over) | N/A | VERIFIED |
| `genizah_core.py` | `ListsManager.create_project(name, color=None)` extended signature (D-06 arity fix) | YES (line 9732) | YES (body uses `color or self._get_next_project_color()`) | YES (`inspect.signature` returns `(self, name, color=None)`) | YES (called by web/user_lists.py:666, 681 sync+async paths) | VERIFIED |
| `tests/test_user_lists_cache_isolation.py` | 4 behavior tests replacing 3 atomic-tuple tests | YES (239 lines) | YES (all 4 tests pass; no atomic-tuple internals referenced) | YES (uses `bootstrapped_state` fixture wiring `_local_lists_mgr` directly) | YES (patched GlobalAuthState + get_user_lists confirm distinct user-id calls) | VERIFIED |
| `tests/test_no_deleted_lists_state_references.py` | 480-line AST scanner; 8 tests (D-10 + R7 narrow scope + R8 Call-node + EXEMPT_FILES + strings-ignored + production-scan) | YES | YES (`_DeletedListsAccessVisitor` class with 5 distinct catches: direct attr, chained attr, narrow-scope, setattr/getattr/hasattr Call, R8 init_user_lists_mgr Call, FunctionDef restoration) | YES (production scan iterates web/ + tests/, respects EXEMPT_FILES) | YES (8 tests all pass) | VERIFIED |
| `tests/test_no_user_lists_mgr_field.py` | 96-line runtime attr-absence test; 4 tests (D-11 parametrized + survivor sanity) | YES | YES (`hasattr(AppState(), '_user_lists_mgr')` returns False; `_cache_entry`/`_cache_ttl` absent on UserListsManager instances) | YES (imports from web.state and web.user_lists) | YES (4 tests all pass; no xfail markers remaining) | VERIFIED |
| `tests/test_no_appstate_export_fields.py` | Phase 88 survivor test line 67 no longer includes `_user_lists_mgr` (D-09 critical) | YES (line 67 contains only `'_local_lists_mgr',`) | YES (Phase 88 survivor sanity test passes — 11 tests including 1 survivor sanity) | YES (Phase 88 runtime guard still active) | YES (D-09 same-commit atomic invariant verified via git show `844e5b53`) | VERIFIED |

All 8 artifacts pass all four verification levels.

---

### Key Link Verification (Wiring)

| From | To | Via | Status | Detail |
|------|-----|-----|--------|--------|
| `AppState.lists_mgr @property` | `UserListsManager` constructor | Lazy import inside getter (`from web.user_lists import UserListsManager`) | WIRED | Avoids circular import (`web.user_lists → web.auth_state → web.supabase_client → nicegui app state`). Returns fresh instance per access. |
| `AppState.lists_mgr` setter | `_local_lists_mgr` field | Direct assignment (`self._local_lists_mgr = value`) | WIRED | Setter simplified from Plan 89-01 state; no longer mirrors to dead `_user_lists_mgr`. |
| `web/main.py:1517` startup | `state.lists_mgr = ListsManager(state.meta_mgr)` | Direct setter call | WIRED | Bootstrap sets `_local_lists_mgr` (per-device anonymous store) without triggering `init_user_lists_mgr()`. Three lines removed. |
| `UserListsManager._get_cached_data` | Supabase `get_user_lists(user_id)` + `get_projects(user_id)` | Direct fetch calls (no cache short-circuit) | WIRED | R2 defensive guard `if not user_id: return self._get_default_data()` added before Supabase call. |
| `UserListsManager.invalidate_cache` | (nothing — no-op) | `pass` body | WIRED (intentional) | ~10 internal mutation paths (`create_list`, `update_list`, `add_item`, etc.) call it harmlessly; D-05 byte-unchanged compat. |
| `web/user_lists.py:666,681 create_project` calls | `genizah_core.py:9732 ListsManager.create_project(name, color=None)` | Method dispatch | WIRED | D-06 arity fix — anonymous-user path no longer raises TypeError. |
| `tests/test_no_deleted_lists_state_references.py` production scan | `web/` + `tests/` directories | `_scan_file` over `SCAN_DIRS`, skipping `EXEMPT_FILES` | WIRED | EXEMPT_FILES exemption-verification test PASSES; production-scan test PASSES with zero violations. |
| `tests/test_no_user_lists_mgr_field.py` | `AppState() + UserListsManager(None, None)` instances | Direct hasattr checks | WIRED | All 4 tests PASS without xfail markers. |

All 8 critical wiring paths verified.

---

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| AppState instance has no `_user_lists_mgr` | `python -c "from web.state import AppState; print(hasattr(AppState(), '_user_lists_mgr'))"` | `False` | PASS |
| AppState instance retains `_local_lists_mgr` | `python -c "from web.state import AppState; print(hasattr(AppState(), '_local_lists_mgr'))"` | `True` | PASS |
| AppState instance has no `init_user_lists_mgr` | `python -c "from web.state import AppState; print(hasattr(AppState(), 'init_user_lists_mgr'))"` | `False` | PASS |
| `state.lists_mgr` returns None pre-bootstrap | `python -c "from web.state import AppState; print(AppState().lists_mgr)"` | `None` | PASS |
| `ListsManager.create_project` accepts color kwarg | `python -c "from genizah_core import ListsManager; import inspect; print(inspect.signature(ListsManager.create_project))"` | `(self, name, color=None)` | PASS |
| Phase 89 regression tests pass | `pytest tests/test_user_lists_cache_isolation.py tests/test_no_deleted_lists_state_references.py tests/test_no_user_lists_mgr_field.py -v` | 16 passed | PASS |
| Phase 88 survivor test still green | `pytest tests/test_no_appstate_export_fields.py -v` | 11 passed | PASS |
| Full pytest suite green (plan boundary) | `pytest tests/ -q` | **1918 passed, 20 skipped, 1 warning** | PASS |

All 8 behavioral spot-checks pass.

---

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|-------------|-------------|--------|----------|
| LISTS-01 | 89-02 | AppState has no `_user_lists_mgr` field | SATISFIED | Runtime test `test_appstate_does_not_have_user_lists_mgr` PASS; static AST scanner production scan PASS; grep returns 0 in `web/`. |
| LISTS-02 | 89-01 | Per-request lifecycle for UserListsManager (no state crosses request boundaries) | SATISFIED | Factory @property returns fresh instance per access (D-01). Stateless wrapper (D-03/D-04) makes per-access lifecycle safe. `test_two_accesses_get_distinct_managers` PASS. |
| LISTS-03 | 89-01 + 89-02 | `_cache_entry`/`_cache_ttl` 10s TTL cache deleted | SATISFIED | Runtime tests `test_user_lists_manager_does_not_have_cache_entry` + `test_user_lists_manager_does_not_have_cache_ttl` PASS. Static AST scanner with R7 narrow-scope catches re-introductions. Grep returns 0 in `web/`. |
| LISTS-04 | 89-01 | Cross-user fetch isolation (no leak between users) | SATISFIED | Two complementary tests: factory-case (`test_authenticated_fetch_does_not_leak_across_users`) + captured-manager case (`test_captured_manager_does_not_serve_stale_data_after_user_switch`, R1 review addition). Both PASS. D-03 closure-capture bug class structurally impossible. |

All 4 LISTS requirements satisfied.

---

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| (none) | — | No TODO/FIXME/PLACEHOLDER markers introduced by Phase 89 changes | — | Phase 89 is a deletion + structural enforcement phase; no new code paths to harbor stubs. |

**Pre-existing async-drop bug at `web/pages/lists.py:344, :349`** (`update_item_note`/`update_item_tags` called without `await`) — flagged in CONTEXT.md as OUT OF SCOPE for Phase 89. Not introduced by this phase. Deferred to a separate bugfix.

---

### Re-Verification Notes

This is the **initial verification** for Phase 89. No previous VERIFICATION.md existed.

---

## Permanent CI Guards Installed

Two regression guards installed atomically in Plan 89-02 commit `844e5b53`:

1. **Static AST scanner** (`tests/test_no_deleted_lists_state_references.py`, 480 lines, 8 tests) — walks `web/` + `tests/` for re-introductions of deleted names. Covers:
   - Direct attribute access (`state._user_lists_mgr`, `self._user_lists_mgr`)
   - Chained attribute access (`web_state.state.X`)
   - Aliased state imports (`from web.state import state as s`)
   - `setattr` / `getattr` / `hasattr` Call nodes
   - `FunctionDef` named `init_user_lists_mgr` (method restoration)
   - `Call` nodes shaped `<state-alias>.init_user_lists_mgr(...)` (R8 — call-site restoration)
   - R7 narrow scope: `_cache_entry` / `_cache_ttl` get unrestricted attribute catch only inside `web/user_lists.py` + `web/state.py`
   - EXEMPT_FILES exemption-verification with `_scan_file` spy

2. **Runtime attr-absence test** (`tests/test_no_user_lists_mgr_field.py`, 96 lines, 4 tests) — direct `hasattr()` assertions on `AppState()` + `UserListsManager(None, None)` instances. Catches dynamic attribute re-introduction even when AST scanner would miss it (e.g., field name built via string concat or set through metaclass).

Both guards are included in standard `pytest tests/` runs — no special CI invocation needed.

---

## Atomic-Commit Verification (R10)

Plan 89-02's D-09 atomic-commit discipline (Phase 88 survivor list update + field deletion + xfail marker removals in ONE commit) verified via git inspection:

- Commit `844e5b53` (`feat(89-02): atomic delete of _user_lists_mgr singleton...`):
  - Touched exactly 5 files: `tests/test_no_appstate_export_fields.py`, `tests/test_no_deleted_lists_state_references.py`, `tests/test_no_user_lists_mgr_field.py`, `web/main.py`, `web/state.py`
  - Cross-checked via `git log --oneline -10` — commit confirmed in history.

Phase 88's `test_appstate_still_has_non_deleted_fields` survivor list (line 67) no longer includes `_user_lists_mgr` — verified via direct read of the test file. Phase 88's 11-test suite (10 parametrized + 1 survivor sanity) PASSES.

---

## Phase 90 Hand-off Readiness

Phase 90 (Auth Caching Rewrite — No `set_session`) inherits the patterns Phase 89 established:

1. **Per-access factory + stateless wrapper** (Plan 89-01 pattern) — applicable to Phase 90's request-scoped auth strategy.
2. **R9 strict-xfail scaffolding + R10 atomic-commit gates** (Plan 89-02 pattern) — applicable to Phase 90's `_client_cache` / `_session_locks` / `_CLIENT_CACHE_TTL` deletions from `web/supabase_client.py` plus the OAuth allowlist entry at line 111.
3. **AST scanner template with R7 narrow-scope + R8 Call-node coverage** (Plan 89-02 pattern) — applicable to Phase 90's regression guards.
4. **NO `auth.set_session()` mid-flight** (CONTEXT.md constraint, Codex verified at `gotrue_client.py:713`) — Phase 90 must avoid `set_session()` entirely.
5. **EXEMPT_FILES + spy-verification pattern** — reusable across all Phase 87-92 regression-guard scanners.

**Status:** Phase 90 is unblocked. Phase 89 fully complete; plan boundary green at 1918 passed / 20 skipped. All 4 ROADMAP success criteria empirically verified.

---

## Gaps Summary

**No gaps identified.** Phase 89 fully achieved its goal:

- ROADMAP SC#1 (no `_user_lists_mgr` in web/state.py): VERIFIED
- ROADMAP SC#2 (no `_cache_entry`/`_cache_ttl` in web/user_lists.py): VERIFIED
- ROADMAP SC#3 (cross-user isolation): VERIFIED via 2 tests (factory + captured-manager)
- ROADMAP SC#4 (test file rewritten to per-request model): VERIFIED

LISTS-01..04 requirements all satisfied. Permanent CI guards installed. Plan boundary green (1918 passed). Phase 88 atomic-commit invariant (D-09) preserved. Phase 90 patterns ready for inheritance.

The Phase 89 deletion + structural enforcement work is complete with no regressions and no deferred items.

---

*Verified: 2026-05-15*
*Verifier: Claude (gsd-verifier)*
