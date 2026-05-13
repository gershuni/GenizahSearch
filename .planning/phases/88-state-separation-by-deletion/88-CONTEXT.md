# Phase 88: State Separation by Deletion — Context

**Gathered:** 2026-05-13
**Status:** Ready for planning

<domain>
## Phase Boundary

Delete the 10 per-user fields from `web/state.py:AppState`, route all writers (`search.py`, `search_results.py`, `parallels.py`) and readers (`web/api.py` export handlers + any straggler) exclusively through `web/export_state.py`, remove the `_TEST_BACKEND` shim from `web/export_state.py`, fold the `parallels_source_text` legacy fallback into the `set_parallels_export(meta=...)` payload, and rewrite the 4 affected test files to monkeypatch `web.safe_storage.app` directly instead of using a production-code injection shim. Add static enforcement (AST/grep + attr-absence) so the deletion cannot regress.

**Out of scope (carved off for later phases):**
- `UserListsManager` singleton + 10s TTL — Phase 89 (LISTS-01..04)
- `_client_cache` / `_session_locks` / `set_session()` — Phase 90 (AUTHC-01..05)
- `web/auth_state.py` migration — Phase 91 (AUTHW-01..06)
- Sweep audit of remaining raw `app.storage.user` reads — Phase 92 (SWEEP-01..06)

</domain>

<decisions>
## Implementation Decisions

### Test Injection Strategy (Recommendation 1 — Codex-refined)

- **D-01:** Tests monkeypatch `web.safe_storage.app` (not `web.export_state.app`) to a stub object whose `storage.user` attribute is a plain dict. Reason: after Plan 88-02 lands, `export_state.py` calls `safe_user_get/set/pop` from `web/safe_storage.py`, which read `web.safe_storage.app.storage.user`. Patching `web.export_state.app` would no-op once `_backend()` is gone. This mirrors the Phase 87 pattern already used in `tests/test_browse_state.py` and `tests/test_search_state.py`.
- **D-02:** Delete the `_StateProxy` wrapper class from `tests/test_export_state_selection.py`. Tests call `export_state.update_search_export_selection([...])` and `export_state.set_search_export(...)` directly, no shim, no proxy.
- **D-03:** Cross-user isolation test (`tests/test_export_cross_user_isolation.py`) uses the same `FastAPI() + init_api_routes(app_override=...) + TestClient` pattern but swaps the patched `web.safe_storage.app.storage.user` dict reference between requests via `monkeypatch.setattr(...)`. This is sequential simulation, **not** true concurrent coverage — explicitly documented in the test's module docstring. True concurrent coverage requires the Phase 92 SWEEP-05 production smoke-test (two browser sessions).

### Plan Decomposition (Recommendation 2 — Codex-rejected and reordered)

- **D-04:** Phase 88 ships as 3 plans, in this order (Codex-revised — the original "delete writers first" ordering was unsafe because `set_search_export(...)` calls read `state.*` as scratch variables; see DISCUSSION-LOG.md for example).
  - **88-01 Writer migration (state.* → local variables → export_state):** In each of the 13 writer sites across `web/pages/search.py`, `web/pages/search_results.py`, and `web/pages/parallels.py`, replace `state.X = value` with `local_X = value` and pass the locals through to the existing `set_search_export(...)` / `set_parallels_export(...)` / `update_*(...)` / `clear_*(...)` calls. AppState fields still exist after this plan (dead code). All tests still pass byte-unchanged because they read state.* directly in fixtures.
  - **88-02 export_state rewrite + test rewrite + _TEST_BACKEND removal:** Rewrite `web/export_state.py` per D-09..D-12 (delete `_backend()`, route through safe_user_*). Rewrite the 4 test files (`test_export_cross_user_isolation`, `test_export_state_selection`, `test_api_export_json`, `test_api_legacy_unchanged`) per D-01..D-03 to drop `_TEST_BACKEND` and the `state.*` fixture setup. Delete the `web/export_state.py` entry from `.planning/phase87_storage_allowlist.yaml` (it's marked "self-eliminating"). Lint scanner stays green.
  - **88-03 AppState deletion + static enforcement:** Delete the 10 fields from `web/state.py:AppState.init()`. Add the runtime attr-absence test (`tests/test_no_appstate_export_fields.py`) AND the static AST/grep test (`tests/test_no_deleted_state_references.py`) per D-13..D-14. Clean stale docstring/comment mentions per D-15. Full `pytest` + ruff + check_docs must pass.
- **D-05:** Plan boundaries MUST stay green. Tests are rewritten in Plan 88-02 BEFORE field deletion in Plan 88-03 — current fixtures touch `state.last_results = […]` etc. at 4 files and would `AttributeError` mid-phase if reordered (see Codex's evidence in DISCUSSION-LOG.md).

### Deletion Enforcement (Recommendation 3 — Codex-refined)

- **D-06:** Runtime attribute-absence test (`tests/test_no_appstate_export_fields.py`) parametrized over the 10 field names: `with pytest.raises(AttributeError): getattr(AppState(), field)`. Defensive against direct instantiation regressions.
- **D-07:** **Static AST/grep test** (`tests/test_no_deleted_state_references.py`) — scan `web/` AND `tests/` for any `state.<deleted_field>` attribute access AND `setattr(state, '<deleted_field>', …)` call. Reason per Codex: `AppState` has no `__setattr__` guard (`web/state.py:5-14`), so any future `state.last_results = …` re-creates the attr dynamically and runtime tests can be order-dependent. The static check survives forever as a CI guard.
- **D-08:** Both tests live in Phase 88, **not** deferred to Phase 92 SWEEP-01. Phase 92 SWEEP-01 is for residual `app.storage.user.get/pop/[]` access; D-06+D-07 specifically guard the AppState-singleton class of regression.

### `_backend()` Helper Fate + Update Function Hardening (Recommendation 4 — Codex-refined)

- **D-09:** Delete `_backend()` entirely from `web/export_state.py`. Each function uses `web.safe_storage.safe_user_get/safe_user_set/safe_user_pop` directly. Removes the production-code shim, eliminates the allowlist entry, conforms to Phase 87 chokepoint discipline.
- **D-10:** Setter functions (`set_search_export`, `set_parallels_export`, `update_search_export_results`, `update_search_export_selection`, `update_parallels_export_filtered`, `clear_search_export`, `clear_parallels_export`) return `None` (not `bool`). Wrap the `safe_user_set` boolean return internally — preserves today's silent-failure contract so external callers (search.py, search_results.py, parallels.py) need no signature changes. Codex catch: current setters return `None`, `safe_user_set` returns `bool` — direct passthrough would be a silent ABI change.
- **D-11:** `update_*` functions add an `isinstance(payload, dict)` guard before mutating retrieved storage payload. Reason per Codex: `safe_user_*` only catches storage-access failures; a poisoned-shape (storage returned non-dict) would crash `payload['results'] = ...`. Mirrors Phase 87 87-REVIEWS.md M3 Class B preservation discipline.
- **D-12:** `update_*` functions adopt copy-on-update: `payload = dict(payload)` before mutation, then `safe_user_set(_KEY, payload)`. Defensive against shared-reference bugs where two same-session requests interleave the read-modify-write. Atomicity guarantees beyond this are explicitly out of scope for Phase 88.

### Parallels source_text bypass (Recommendation 5 — Codex-surfaced gray area)

- **D-13:** Fold `parallels_source_text` into the `set_parallels_export(meta={'source_text': ...})` payload on every writer path in `web/pages/parallels.py` (line 457 happy path, line 2049-2051 reset, line 2341-2344 persistence). Delete the legacy `app.storage.user['parallels_source_text'] = ...` writes once all readers are migrated.
- **D-14:** Delete the `safe_user_get('parallels_source_text', '')` fallback in the 3 reader sites at `web/api.py:1921-1931`, `1955-1964`, `2049-2065`. Source text now reads exclusively from `meta['source_text']` in the per-session payload.
- **D-15:** Add a test in Plan 88-02 (extend `test_export_cross_user_isolation.py`) asserting that User A's `source_text` cannot leak into User B's parallels-export response via the legacy fallback — proves the fallback is genuinely dead.
- **D-16:** Plan 88-03's stale-mention cleanup includes the 2026-05-12 cross-user-fix comment at `web/api.py:1846-1848` AND the "MUST NOT touch state.last_results" docstring at `web/search_api.py:1198-1199` AND the `web/export_state.py:18-25` doctstring referencing the singleton history. These are correct as historical context but stale as forward-looking guidance.

### Claude's Discretion

- The exact local-variable naming convention in Plan 88-01 writer migration (`results_local` vs `_results` vs `local_results`). Recommendation: match the pattern already in scope at each call site, prefer the existing argument name when shadowing safely.
- Whether the static-grep test in D-07 uses AST walking (preferred — mirrors `tests/test_no_raw_storage_access.py` Phase 87 pattern) or pure regex (faster, less precise). Recommendation: AST for consistency with the existing scanner.
- Whether to split Plan 88-01 by file (search.py, search_results.py, parallels.py = 3 commits) or land as one commit. Defer to planner's wave-structure judgement; either is acceptable.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 88 Locked Requirements
- `.planning/REQUIREMENTS.md` §"State Separation — Phase 88" — STATE-01 through STATE-06, the locked scope
- `.planning/ROADMAP.md` §"Phase 88: State Separation by Deletion" — 5 success criteria
- `.planning/HANDOFF_v7.11.1_path_b.md` §"Path B — proposed scope" item 1 (State separation) — the original architectural narrative

### Phase 87 Foundations (load-bearing for Phase 88)
- `web/safe_storage.py` — the chokepoint module; `safe_user_get/set/pop` are the only legal storage access pattern outside the allowlist
- `.planning/phase87_storage_allowlist.yaml` §"web/export_state.py" — the self-eliminating entry that Plan 88-02 deletes
- `tests/test_no_raw_storage_access.py` — the permanent CI lint scanner; Phase 88 must not add new allowlist entries
- `tests/test_browse_state.py`, `tests/test_search_state.py` — Phase 87 reference for `monkeypatch.setattr('web.safe_storage.app', ...)` test pattern

### Source files modified by Phase 88
- `web/export_state.py` (full rewrite — delete `_backend()`, route through safe_user_*)
- `web/state.py` lines 26-50 (delete 10 fields from `AppState.init()`)
- `web/pages/search.py` lines 2067-2076, 2101-2106, 3801-3820, 4112-4140, 4197-4231 (writer migration to locals)
- `web/pages/search_results.py` lines 126, 377-380 (writer migration to locals)
- `web/pages/parallels.py` lines 281-302, 457, 1981-2002, 2049-2061, 2300-2338, 2341-2344 (writer migration + source_text fold-in)
- `web/api.py` lines 1846-1848 (comment cleanup), 1921-1931, 1955-1964, 2049-2065 (delete source_text fallback)
- `web/search_api.py` lines 1198-1199 (docstring cleanup)

### Test files modified by Phase 88
- `tests/test_export_cross_user_isolation.py` — rewrite to monkeypatch `web.safe_storage.app`, drop `_TEST_BACKEND` + `state.*` fixtures; extend with source_text leak test (D-15)
- `tests/test_export_state_selection.py` — rewrite, delete `_StateProxy`, drop `_TEST_BACKEND` + `state.*` fixtures
- `tests/test_api_export_json.py` — rewrite, drop `_TEST_BACKEND` + `state.*` fixtures
- `tests/test_api_legacy_unchanged.py` — rewrite, drop `_TEST_BACKEND` + `state.*` fixtures

### Test files created by Phase 88
- `tests/test_no_appstate_export_fields.py` — D-06 runtime attr-absence test
- `tests/test_no_deleted_state_references.py` — D-07 static AST/grep enforcement

### External red-team review (Codex round 5)
- `_tmp/codex_phase88_discuss_review_prompt.md` — the prompt sent to Codex
- `_tmp/codex_phase88_discuss_review_response.txt` — Codex's verdicts (refine/reject/agree per recommendation) and the 5th gray area surfacing

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `web/safe_storage.py` `safe_user_get/set/pop` — the chokepoint. Each function already absorbs `AssertionError` (pruned-session race) and unexpected exceptions, returning the default. Phase 88's `export_state.py` rewrite leans entirely on these.
- `tests/test_browse_state.py` (Phase 87 Plan 05 B3 fix) — canonical example of monkeypatching `web.safe_storage.app` to a stub. Direct template for the 4 Phase 88 test rewrites.
- `tests/test_no_raw_storage_access.py` (Phase 87 Plan 07) — the AST-based scanner. Phase 88's `test_no_deleted_state_references.py` follows the same shape (walk AST, collect Attribute/Subscript/Call nodes, fail on disallowed patterns).
- `tests/conftest.py` — minimal setup; no pytest fixtures defined globally. Phase 88 fixtures live in each test file (mirroring Phase 87 layout).

### Established Patterns
- **Dual-write deletion (this milestone's discipline):** Phase 87 already deleted ~131 raw access sites by migrating-then-deleting; the post-migrate diff is the audit trail. Phase 88 applies the same to the AppState singleton.
- **Wave-structured plans:** Phase 87 ran 8 plans, each independently green. Phase 88 mirrors with 3 plans, plan-boundary-green discipline (D-05).
- **AST-based static enforcement:** Phase 87 Plan 07 established this as the durable guard. Phase 88 D-07 extends.
- **Codex round review:** This phase's design went through 1 round of Codex review (transcribed in `_tmp/codex_phase88_discuss_review_response.txt`); Codex's 3-of-4 refinements + 5th-gray-area surfacing reshape the plan ordering and add the static-grep test.

### Integration Points
- `web/api.py` reader sites — already routed through `get_search_export()` / `get_parallels_export()` since 2026-05-12 (v7.11.1). Phase 88 has nothing to change here EXCEPT removing the `safe_user_get('parallels_source_text', '')` fallback (D-14) and the historical comment (D-16).
- `web/search_api.py` L1198 docstring — purely informational; no live state.* reference, just a "MUST NOT touch state.last_results" rule. Phase 88 updates the wording but keeps the rule (now it says "MUST NOT touch export_state singleton — handlers are stateless").
- Phase 87 lint scanner (`tests/test_no_raw_storage_access.py`) — Phase 88 makes a small change: removes the `web/export_state.py` allowlist entry (D-04 Plan 88-02). The expected_count for that file goes from 1 → 0 (i.e., entry deleted entirely, not just decremented).
- Phase 87 allowlist H1 schema — Plan 88-02 must update the schema-validation test if it asserts allowlist entry count exactly.

### Why Codex caught the original plan ordering (high-value insight)
The original plan was "delete writers first, then fields, then tests." But `set_search_export(...)` calls in search.py:4112-4140 pass `state.current_search_gap`, `state.last_filters_applied`, `state.last_search_warnings` as keyword arguments **two lines below** their `state.X = value` assignments. Deleting the `state.X =` lines first would feed default/stale values into the `set_search_export(...)` call. Codex's reordering (assignments → locals → pass-locals first, THEN delete fields) eliminates the data-loss window.

</code_context>

<specifics>
## Specific Ideas

- **User direction:** "I have no way to answer technical questions. Let's ask for your advice, then ask external AIs as well." → Established the discuss-phase pattern of "Claude recommends + Codex red-teams + user picks the refined synthesis." Worked well here; pattern can be reused in subsequent Path B phases (89-92) when the user is asked about implementation details they can't evaluate directly.
- **Codex round catch:** The state.* scratch-variable bug in original plan ordering was a genuine bug Claude missed. Reinforces the value of an external review pass before locking in plans — particularly for refactors that look mechanical but have control-flow ordering dependencies.

</specifics>

<deferred>
## Deferred Ideas

- **True concurrent test coverage:** Codex flagged that sequential-monkeypatch-swap is not true concurrent coverage. Real concurrency requires two NiceGUI processes or a fully-instantiated `app.storage.user` per request via the actual NiceGUI test harness. Deferred to Phase 92 SWEEP-05 production smoke-test (two browser sessions, manual checklist).
- **AppState `__setattr__` guard:** Adding a class-level guard that raises on writes to the 10 deleted names would catch dynamic-attr regressions even more loudly than D-07's static check. Not adopted because (a) D-07 already catches it at CI lint time, (b) production code shouldn't carry sentinel guards for one-time refactors. If a regression actually slips through, this is the fallback.
- **Atomicity for read-modify-write in update_* functions:** Same-session concurrent requests interleaving `payload = safe_user_get(...); payload[k] = v; safe_user_set(..., payload)` is theoretically racy. D-12's copy-on-update is defensive but not atomic. Real atomicity requires lock or CAS — deferred indefinitely; not on the table for v7.12.
- **Lists cache redesign:** Phase 89 territory. The `UserListsManager._cache_entry` singleton on AppState is structurally similar to the export-state singleton but has its own requirements (LISTS-01..04) and goes per-request.

</deferred>

---

*Phase: 88-state-separation-by-deletion*
*Context gathered: 2026-05-13*
*Workflow note: This CONTEXT.md captures recommendations refined by 1 round of Codex external review (see `_tmp/codex_phase88_discuss_review_response.txt`). Plan ordering (D-04, D-05), static-grep test (D-07), update-function hardening (D-10, D-11, D-12), and 5th gray area resolution (D-13, D-14, D-15) all incorporate Codex's catches that the original Claude-only analysis missed.*
