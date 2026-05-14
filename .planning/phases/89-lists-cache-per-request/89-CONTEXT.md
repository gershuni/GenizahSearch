# Phase 89: Lists Cache Per-Request — Context

**Gathered:** 2026-05-14
**Status:** Ready for planning

<domain>
## Phase Boundary

Drop the `UserListsManager` instance singleton on `AppState._user_lists_mgr` and the 10s TTL plumbing on `UserListsManager` (`_cache_entry` tuple + `_cache_ttl` constant + user-id-key check). `state.lists_mgr` becomes a factory-property that returns a fresh `UserListsManager` wrapping `_local_lists_mgr` + `meta_mgr` on every access. `UserListsManager._get_cached_data()` becomes pure stateless — each authenticated fetch hits Supabase. `invalidate_cache()` is preserved as a compatibility no-op so the ~10 internal post-mutation callers stay byte-unchanged. Plan 89-01 ships the per-access factory + stateless fetch + delegation signature audit + test rewrite (with `_user_lists_mgr` field intact as dead-code temporary). Plan 89-02 deletes the field, updates Phase 88's survivor test (line 67), installs a Phase 89-specific static AST guard + runtime attr-absence test.

**Out of scope (carved off for other phases):**
- Auth client caching (`_client_cache`, `set_session()`) — Phase 90.
- `web/auth_state.py` migration — Phase 91.
- Final raw-storage sweep, `MULTITENANT.md` docs, cross-session smoke test — Phase 92.
- Pre-existing async-drop bug at `web/pages/lists.py:344, :349` (`update_item_note`/`update_item_tags` called without `await`) — flagged but NOT a Phase 89 fix.

</domain>

<decisions>
## Implementation Decisions

### Migration Mechanism — Factory-Property with Bootstrap Guard (Area 1)

- **D-01:** `state.lists_mgr` property becomes a factory:
  ```python
  @property
  def lists_mgr(self):
      if self._local_lists_mgr is None:
          return None  # preserve pre-bootstrap None guard contract
      return UserListsManager(self._local_lists_mgr, self.meta_mgr)
  ```
  The `if self._local_lists_mgr is None: return None` guard is **load-bearing** — Codex catch: existing `if not state.lists_mgr:` checks (across `web/api.py:2114`, `web/components/comment_dialog.py:93`, `web/pages/lists.py:218`, etc.) rely on `state.lists_mgr` being `None` during the bootstrap window before `web/main.py:1505` runs. A bare `return UserListsManager(None, None)` would break those guards by returning a truthy object pre-bootstrap.
- **D-02:** Per-ACCESS lifecycle (not strict per-request). Each `state.lists_mgr` reference constructs a new wrapper instance. Safe in this design because **the wrapper is stateless** (D-03): each `.data` call hits Supabase fresh; no cache to leak across accesses. The "per-request" requirement (LISTS-02) is satisfied in effect — no state crosses request boundaries because no state exists.

### Cache Fate — Pure Stateless Fetch, invalidate_cache() Kept as No-Op (Area 2)

- **D-03:** Delete `_cache_entry: Optional[Tuple[Optional[str], float, Dict[str, Any]]] = None` and `_cache_ttl = 10` from `UserListsManager.__init__`. Delete the user-id-key check from `_get_cached_data()`. Reason per Codex: `_cached_data` per-instance memoization is unsafe because UI callbacks capture managers into closures (`web/components/add_to_list_dialog.py:84-243`, `web/components/project_tree.py:60-651`), so a "per-request" manager becomes long-lived once captured into a dialog lambda. With no TTL + no user-id guard, `_cached_data` could serve stale data indefinitely within that captured manager.
- **D-04:** `_get_cached_data()` becomes stateless: when authenticated, always call `get_user_lists(user_id)` + `get_projects(user_id)`, build the dict, return it. No timestamp tracking. No tuple. No user-id storage. When not authenticated, return `_get_default_data()` (existing path, unchanged).
- **D-05:** `invalidate_cache()` is preserved as a compatibility no-op (or trivial `self._cached_data = None`-equivalent, but with no `_cached_data` field). ~10 internal callers (`create_list`, `update_list`, `add_item`, etc., on success paths) keep calling `self.invalidate_cache()` byte-unchanged. Public API: no external callers. Deleting it would force ~10 internal edits for zero gain.

### UserListsManager → ListsManager Delegation Audit (Area 4)

- **D-06:** Plan 89-01 includes a method-signature audit pass. Confirmed bugs to fix:
  - **`create_project` arity mismatch:** `web/user_lists.py:661, 676` (in `create_project` and `create_project_sync`) call `self.local_mgr.create_project(name, color)` when not authenticated, but `genizah_core.py:9631` defines `ListsManager.create_project(self, name)` — no `color` parameter. Anonymous project creation through the wrapper raises `TypeError` today; not a Phase 89-caused regression, but in-scope because the per-access factory routes anonymous users through `UserListsManager` exclusively (same as today after `init_user_lists_mgr()`), so signature drift is a user-visible bug. Fix: change `genizah_core.py:9631` to accept an optional `color=None` parameter (it's stored but unused for local mgr) **OR** drop the `color` arg in the wrapper's local-mgr call. Decision deferred to planner — pick whichever has lower blast radius across the desktop app's `ListsManager` callers.
- **D-07:** No "unification" of the property is needed. Codex corrected the original framing: `init_user_lists_mgr()` runs at startup unconditionally (`web/main.py:1508`), so `_user_lists_mgr` is set for both auth AND anon users today, and `state.lists_mgr` already returns `UserListsManager` for everyone post-bootstrap. The factory-property design (D-01) preserves this exactly: `None` pre-bootstrap, `UserListsManager(...)` post-bootstrap.

### Plan Decomposition (Area 3)

- **D-08:** Phase 89 ships as **2 plans**:
  - **89-01: Per-access factory + stateless fetch + delegation audit + test rewrite.**
    - Rewrite `web/state.py` `lists_mgr` property to D-01 factory shape. Keep `_user_lists_mgr` field intact (dead-code temporary). The setter's `_user_lists_mgr` branch (line 48-51) becomes unreachable but stays — Plan 89-02 deletes both together.
    - Rewrite `web/user_lists.py:UserListsManager`:
      - Delete `_cache_entry` field initialization (line 76).
      - Delete `_cache_ttl` field initialization (line 77).
      - Delete the cache-lookup block in `_get_cached_data()` (lines 132-140) and the cache-write line (line 177).
      - Update `invalidate_cache()` to be a no-op (line 184 can become `pass` with explanatory docstring, OR a trivial `self._cached_data = None` assignment with `self._cached_data: Optional[Dict] = None` initialized in `__init__` if we want the attribute to exist for compat). Recommend: `pass` with docstring "Compatibility no-op — Phase 89 removed the cache. Mutation paths still call this; the call is harmless."
    - Delegation audit (D-06): fix `create_project` color-arity mismatch. Audit other `self.local_mgr.X(args)` calls in `web/user_lists.py` for signature drift; document findings in Plan 89-01 SUMMARY.md even if no other drift is found.
    - Rewrite `tests/test_user_lists_cache_isolation.py` from 3 atomic-cache tests to 3 behavior tests:
      - `test_two_accesses_get_distinct_managers` — `assert state.lists_mgr is not state.lists_mgr` (after bootstrap, post-`init` so `_local_lists_mgr` is set).
      - `test_authenticated_fetch_does_not_leak_across_users` — patch `GlobalAuthState.get_user_id` to return `'user-A'`, fetch via `state.lists_mgr.get_all_lists()`; patch to return `'user-B'`, fetch via a freshly-accessed `state.lists_mgr.get_all_lists()`. Assert `get_user_lists` was called for both user-A and user-B separately (no user-id-keyed cache lookup possible).
      - `test_invalidate_cache_is_compatibility_no_op` — `state.lists_mgr.invalidate_cache()` does not raise; subsequent `.data` access still works.
    - **DO NOT** delete `state.init_user_lists_mgr()` call from `main.py:1508` in this plan — it stays as no-op-equivalent. Plan 89-02 deletes both the call and the method.
    - Plan boundary: full pytest green. No user-visible behavior change.
  - **89-02: Singleton deletion + Phase 88 survivor-test fix + static enforcement.**
    - Delete `self._user_lists_mgr = None` from `AppState.init()` (`web/state.py:24`).
    - Delete the `if self._user_lists_mgr is not None: return self._user_lists_mgr` branch from the property — already unreachable after 89-01, this is the cleanup.
    - Delete the `if self._user_lists_mgr is not None: self._user_lists_mgr.local_mgr = value; self._user_lists_mgr.meta_mgr = self.meta_mgr` block from the setter (`web/state.py:48-51`).
    - Delete `def init_user_lists_mgr(self)` method (`web/state.py:53-66`).
    - Delete `state.init_user_lists_mgr()` call from `web/main.py:1508`.
    - **CRITICAL Codex catch (D-09):** Update `tests/test_no_appstate_export_fields.py:67` — remove `'_user_lists_mgr'` from the `survivors` list (or this test breaks immediately on field deletion). Same commit as the field deletion to keep plan boundary green.
    - Install `tests/test_no_deleted_lists_state_references.py` — Phase 89-specific static AST scanner per D-10.
    - Install `tests/test_no_user_lists_mgr_field.py` — runtime attr-absence test per D-11.
    - Plan boundary: full pytest green. No user-visible behavior change.
- **D-09:** Plan 89-02 MUST update `tests/test_no_appstate_export_fields.py:67` in the **same commit** as the `web/state.py` deletion. Codex catch: Phase 88's survivor sanity test asserts `hasattr(AppState(), '_user_lists_mgr')` is True; deletion without the test update creates a red plan boundary.
- **D-10:** Static AST guard (`tests/test_no_deleted_lists_state_references.py`) is **Phase 89-specific** (user chose this over extending Phase 88's file). Mirrors Phase 88's `test_no_deleted_state_references.py` shape but expanded scope per Codex:
  - Scan `web/` AND `tests/` (Phase 88 pattern).
  - Disallow `state._user_lists_mgr` Attribute access (catches restoration via `state` global).
  - Disallow `self._user_lists_mgr` Attribute access (catches restoration via internal class write — important because `AppState` has no `__setattr__` guard).
  - Disallow `_cache_entry` AND `_cache_ttl` Attribute access ANYWHERE (catches restoration of either cache field).
  - Disallow `FunctionDef` named `init_user_lists_mgr` (catches function-definition restoration, not just call-site).
  - Disallow `Call` nodes for `getattr(..., '_user_lists_mgr', ...)`, `setattr(..., '_user_lists_mgr', ...)`, `hasattr(..., '_user_lists_mgr')` — Phase 88-level coverage of indirect-access forms.
  - **Seed traps:** include 4+ known-bad strings AS PARSED CODE SNIPPETS (e.g., via `ast.parse("state._user_lists_mgr = None")`), not as raw string literals. Phase 88 D-07 pattern — proves the scanner finds positives and not just false-negatives.
- **D-11:** Runtime attr-absence test (`tests/test_no_user_lists_mgr_field.py`) — parametrized over `['_user_lists_mgr', '_cache_entry', '_cache_ttl']` (the 3 field names deleted by Phase 89). For `_user_lists_mgr`: `with pytest.raises(AttributeError): AppState()._user_lists_mgr`. For `_cache_entry`/`_cache_ttl`: instantiate `UserListsManager(None, None)` and assert `not hasattr(mgr, '_cache_entry')` AND `not hasattr(mgr, '_cache_ttl')`. Defensive against direct-instantiation regressions where someone re-introduces the field via `__init__` but the static scanner misses it.

### Claude's Discretion

- The exact body of `invalidate_cache()` after D-05: `pass` with explanatory docstring vs. trivial `self._cached_data = None` with a `_cached_data` field initialized in `__init__` for symmetry. Defer to Plan 89-01 author — both satisfy "no-op" semantics and the success criteria.
- Whether to fix the `create_project` color-arity mismatch (D-06) by adding `color=None` to `ListsManager.create_project` in `genizah_core.py:9631` (preferred — extends without breaking) or by dropping the `color` argument in the wrapper's local-mgr call. Defer to planner; flag any other `ListsManager` callers of `create_project` that would care.
- Whether the runtime attr-absence test (D-11) and static AST test (D-10) live in one file or two. Phase 88 used two files (`test_no_appstate_export_fields.py` runtime + `test_no_deleted_state_references.py` static); recommend matching for consistency. The user chose "new file" — already implies two-file split.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 89 Locked Requirements
- `.planning/REQUIREMENTS.md` §"Lists Cache — Phase 89" — LISTS-01 through LISTS-04, the locked scope.
- `.planning/ROADMAP.md` §"Phase 89: Lists Cache Per-Request" — 4 success criteria.
- `.planning/HANDOFF_v7.11.1_path_b.md` §"Path B — proposed scope" item 2 ("Lists cache redesign") — the original architectural narrative; chooses option (a) per-request over option (b) session-keyed.

### Phase 87 + Phase 88 Foundations (load-bearing for Phase 89)
- `web/safe_storage.py` — Phase 87 chokepoint. Phase 89 does NOT add new raw `app.storage.user` access; if any session-keyed storage is needed it must go through `safe_user_get/set/pop`. (Current design: no new storage at all, so this is satisfied trivially.)
- `tests/test_no_raw_storage_access.py` — Phase 87 permanent CI lint scanner. Phase 89 makes no allowlist changes.
- `.planning/phases/88-state-separation-by-deletion/88-CONTEXT.md` D-04, D-05, D-07 — the plan-boundary green discipline, ordering rationale, and static AST guard pattern Phase 89 mirrors.
- `tests/test_no_deleted_state_references.py` (Phase 88 D-07) — direct template for Phase 89's `test_no_deleted_lists_state_references.py`. Same shape, broader disallowed-name list.
- `tests/test_no_appstate_export_fields.py` (Phase 88 D-06) — line 67 survivors list MUST be updated by Plan 89-02 (D-09 critical Codex catch).

### Source files modified by Phase 89

Plan 89-01:
- `web/state.py` — rewrite `lists_mgr` property to factory-with-bootstrap-guard (lines 29-42). Setter (lines 44-51) untouched in this plan.
- `web/user_lists.py` — delete `_cache_entry` (line 76), `_cache_ttl` (line 77); rewrite `_get_cached_data()` to stateless (lines 121-180); reduce `invalidate_cache()` to no-op (lines 182-184); fix `create_project` color-arity mismatch (lines 661, 676).
- `genizah_core.py` — possibly add `color=None` to `ListsManager.create_project` (line 9631) to fix D-06; depends on planner's blast-radius assessment.
- `tests/test_user_lists_cache_isolation.py` — rewrite from 3 atomic-cache tests to 3 behavior tests.

Plan 89-02:
- `web/state.py` — delete `self._user_lists_mgr = None` (line 24), the unreachable `_user_lists_mgr` branch in property (lines 38-39 — already unreachable after 89-01), the setter's `_user_lists_mgr` block (lines 48-51), and `init_user_lists_mgr()` method (lines 53-66).
- `web/main.py:1508` — delete `state.init_user_lists_mgr()` call.
- `tests/test_no_appstate_export_fields.py:67` — remove `'_user_lists_mgr'` from `survivors` list (Codex catch D-09).

### Test files created by Phase 89 (Plan 89-02)
- `tests/test_no_deleted_lists_state_references.py` — static AST scanner per D-10.
- `tests/test_no_user_lists_mgr_field.py` — runtime attr-absence test per D-11.

### External red-team review (Codex round)
- `_tmp/codex_phase89_discuss_review_prompt.md` — Claude's proposed decisions sent to Codex.
- `_tmp/codex_phase89_discuss_review_response.txt` — Codex's verdicts. Three concrete bug catches (Phase 88 survivor test, create_project arity, async-drop in lists.py) plus the per-ACCESS-vs-per-request reframe that flipped cache decision from per-request memoization to pure stateless.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tests/test_no_deleted_state_references.py` (Phase 88 D-07) — direct AST-walker template for Phase 89's `test_no_deleted_lists_state_references.py`. The seed-trap pattern (parse known-bad strings as code snippets and assert scanner catches them) carries over verbatim.
- `tests/test_no_appstate_export_fields.py` (Phase 88 D-06) — the parametrized `pytest.raises(AttributeError)` runtime pattern. Phase 89 D-11 reuses with `['_user_lists_mgr', '_cache_entry', '_cache_ttl']`.
- `web/user_lists.py:925-937` `get_lists_manager(local_mgr, meta_mgr)` factory — already exists, currently unused outside docstring. Phase 89's factory-property design effectively makes this function the new orthodox path, even though we don't migrate call sites to use it directly.

### Established Patterns
- **Deletion-not-migration discipline** (Phase 87, 88): no dual-write window. Phase 89 follows: Plan 89-01 leaves dead code temporarily (`_user_lists_mgr` field unused but present); Plan 89-02 deletes in one commit alongside the survivor-test fix to keep the plan boundary green.
- **Plan-boundary-green discipline** (Phase 88 D-05): every plan boundary must have a green test suite. Plan 89-02's deletion + Phase 88 survivor-test update MUST be the same commit (D-09).
- **Static AST guard as durable CI lint** (Phase 87 + 88): AST walker over `web/` + `tests/` with seed-trap parsed snippets is the orthodox shape for "this field is gone forever" enforcement.
- **Codex red-team after Claude proposes** (Phase 88 specifics, repeated here): user is non-technical for these decisions; Claude proposes, Codex red-teams, user picks the synthesis. Worked again — Codex caught 3 concrete bugs (Phase 88 survivor test on line 67, `create_project` color-arity, async-drop on lists.py:344) and one structural framing error (per-ACCESS-vs-per-request).

### Integration Points
- **Phase 88 survivor test** (`tests/test_no_appstate_export_fields.py:67`) — Phase 89's most fragile coupling. Plan 89-02 must update the survivor list in the same commit as field deletion (D-09).
- **`web/main.py:1505-1508` startup sequence** — Plan 89-02 deletes `state.init_user_lists_mgr()` from line 1508. `state.lists_mgr = ListsManager(state.meta_mgr)` on line 1505 (the `_local_lists_mgr` write) stays — that's the per-device anonymous store, out of scope for Phase 89.
- **UI callback capture sites** (`web/components/add_to_list_dialog.py:84-243`, `web/components/project_tree.py:60-651`, possibly others) — these capture `lists_mgr` into dialog closures that fire much later. The pure stateless design (D-03, D-04) makes this safe: a captured manager has no cached state to go stale. If a future phase reintroduces caching, these capture patterns must be revisited.

### Hot Loops That Currently Benefit from Cache (Re-Analyzed Post-Codex)
- `web/pages/discoveries.py:1485-1496` — `for list_id in lists.items(): count = state.lists_mgr._get_list_item_count(list_id)`. Re-read: only the OUTER `lists = state.lists_mgr.data.get('lists', {})` benefits from `_get_cached_data` cache. The inner `_get_list_item_count` reads via `get_list_items(int(list_id))` which is NOT cached today (per-list Supabase round-trip). So Phase 89's stateless `.data` change costs at most 1 extra Supabase call per render (was 1 cached, now 1 uncached). Not load-bearing for perf.
- `web/pages/home.py:103, :547` — `state.lists_mgr.get_all_lists()` called potentially twice per render (count display + render). Stateless adds 1 Supabase round-trip per double-render. Acceptable.
- No hot loop calls `.data` more than ~2x per render. Pure stateless fetch is safe.

### Why Codex's Per-ACCESS Reframe Matters (High-Value Insight)
The original Claude framing called the design "per-request memoization" and argued `_cached_data` was safe because each request gets a fresh manager. Codex caught:
1. `state.lists_mgr` in the factory shape is per-ACCESS, not per-request — `web/api.py:2114, 2117, 2122` reads the property 3 times in one request, gets 3 different managers.
2. Worse, UI dialog callbacks capture `lists_mgr` references (`add_to_list_dialog.py:84` → reused at line 194, 205, 243). The captured manager outlives the request that constructed it.
3. Per-instance `_cached_data` in a captured manager = stale data forever in that dialog. Different bug class than the original cross-user leak, but still a bug.

Stateless fetch sidesteps both: there's no state to go stale, and the per-ACCESS lifecycle becomes a non-issue. The "per-request" success criterion (LISTS-02) is satisfied **in spirit** (no state crosses request boundaries) even though the lifecycle is technically per-access.

</code_context>

<specifics>
## Specific Ideas

- **User direction:** "All this is very technical for me. Ask Codex for its take." → Same pattern as Phase 88. Claude proposed → Codex red-teamed → user locked the Codex-revised synthesis. Locks in the Path B style of external red-team after Claude proposes; pattern will repeat on Phases 90, 91, 92.
- **Codex round catch (per-ACCESS vs. per-request reframe):** The single most valuable Codex catch. Claude's original framing called the factory "per-request" — Codex traced 3 concrete call sites that prove it's per-access, and identified UI callback capture sites that make per-instance caching unsafe even when per-access. This flipped Area 2's recommendation from per-request memoization to pure stateless fetch. Genuine bug Claude missed; the same pattern will appear in Phase 90's `_client_cache` deletion.
- **Codex caught 3 concrete bugs:** (1) `tests/test_no_appstate_export_fields.py:67` survivor list breaks on deletion → D-09; (2) `ListsManager.create_project(self, name)` arity mismatch in genizah_core.py:9631 vs user_lists.py:667, 682 → D-06; (3) pre-existing async-drop in `web/pages/lists.py:344, :349` → flagged in deferred ideas, NOT Phase 89 scope. None of the three were in Claude's original analysis.

</specifics>

<deferred>
## Deferred Ideas

- **True per-request lifecycle (handler-boundary explicit binding):** Codex correctly noted that factory-property is per-ACCESS, not per-request. Achieving true per-request would require explicit `lists_mgr = state.lists_mgr` binding at the top of each handler and threading through, churning ~70 call sites. Defer to Phase 92 sweep or a future phase if/when this becomes load-bearing for a real bug. Stateless fetch makes per-access safe today.
- **Pre-existing async-drop bug at `web/pages/lists.py:344, :349`:** `update_item_note` and `update_item_tags` are async methods (`web/user_lists.py:507, :524`) called without `await`. In the authenticated path the coroutine is silently dropped — the database write never executes. Pre-existing (NOT Phase 89-caused), Codex flagged it during the audit. Out of Phase 89 scope; file a separate bug fix.
- **Other delegation signature drift audit:** D-06 fixes `create_project` color arity. If Plan 89-01's audit surfaces other drift (e.g., `update_list_project`, `update_item`, etc.), the planner decides whether to bundle the fixes into 89-01 or defer to a separate hotfix. Don't expand Phase 89 scope into a general `UserListsManager`↔`ListsManager` API harmonization.
- **Reintroducing caching post-Phase 89:** If future perf measurements show that stateless `.data` is too expensive, the path forward is server-side cache keyed by `_session_uuid` (Phase 87 primitive) in `app.storage.user` via `safe_storage` helpers. This sidesteps both the cross-user leak AND the UI callback capture stale-data class. Not on the v7.12 table.
- **`AppState.__setattr__` guard for deleted names:** Could catch dynamic `state._user_lists_mgr = ...` revivals even louder than the static AST scanner. Not adopted because (a) D-10's AST scanner catches it at CI lint time, (b) production code shouldn't carry sentinel guards for one-time refactors. Phase 88's deferred-ideas list contains the same item; same reasoning here.

</deferred>

---

*Phase: 89-lists-cache-per-request*
*Context gathered: 2026-05-14*
*Workflow note: This CONTEXT.md captures recommendations refined by 1 round of Codex external review (see `_tmp/codex_phase89_discuss_review_response.txt`). Cache fate (D-03, D-04, D-05), plan-boundary discipline (D-08, D-09), static guard scope (D-10), delegation audit (D-06), and per-access framing correction (D-02) all incorporate Codex's catches that the original Claude-only analysis missed.*
