# Phase 88: State Separation by Deletion — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in `88-CONTEXT.md` — this log preserves the alternatives considered and the Codex round-5 red-team feedback that reshaped the plan.

**Date:** 2026-05-13
**Phase:** 88-state-separation-by-deletion
**Areas discussed:** Test injection strategy, Plan decomposition, Deletion enforcement, `_backend()` helper fate, Parallels source_text bypass (Codex-surfaced)

---

## Process

The user opted out of answering technical decisions directly ("The questions are technical and I have no way to answer them. We can Ask for you advice, then ask external AIs as well"). Claude presented 4 recommendations with rationale. The user chose to send to Codex CLI for red-team review before locking in. Codex returned 3-of-4 verdict refinements + surfaced a 5th gray area. The user accepted all Codex refinements and elected to fold the 5th gray area into Phase 88 scope.

---

## Gray Area 1 — Test Injection Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Real NiceGUI TestClient with cookies | Highest fidelity, heaviest infra; would require NiceGUI startup in tests | |
| Monkeypatch `web.export_state.app.storage` to a dict | Original Claude recommendation; same shape as today minus shim | refined |
| ContextVar adapter | Module-wide context variable, override per-test | |
| Constructor injection into export_state functions | Pass backend through every call signature | |

**Claude's initial choice:** Monkeypatch `web.export_state.app.storage`.

**Codex verdict (refine):** Patching `web.export_state.app.storage` only works while `_backend()` reads `app.storage.user` (`web/export_state.py:40-48`). Once Rec 4 lands and `export_state` functions call `safe_user_get/set/pop` from `web/safe_storage.py`, tests must patch `web.safe_storage.app` (`web/safe_storage.py:37,54,66,79`), not `web.export_state.app`. Same TestClient is fine for serial unit coverage; handler imports happen per request (`web/api.py:1849-1850`, `1998-2000`). It is **not** true concurrent-session coverage because the monkeypatch is global. Delete `_StateProxy`; tests should call `export_state.set_search_export(...)` and `update_search_export_selection(...)` directly.

**Final decision (D-01, D-02, D-03):** Monkeypatch `web.safe_storage.app` (not `web.export_state.app`). Delete `_StateProxy`. Sequential simulation explicitly documented as non-concurrent — true concurrency is Phase 92 SWEEP-05 production smoke-test.

---

## Gray Area 2 — Plan Decomposition

| Option | Description | Selected |
|--------|-------------|----------|
| 1 mega-plan | Atomic, simplest verification, highest blast radius | |
| 3 plans (writers, fields+readers, tests+shim) | Mirrors Phase 87 structure | initial |
| 3 plans (writers-to-locals, export_state+tests, fields+enforcement) | Codex-revised ordering — safer plan boundaries | ✓ |
| 4-5 plans (split writers by file) | Finer atomic-commit granularity | |
| 10 micro-plans (per-field migration) | Tiny commits, slow | |

**Claude's initial choice:** 3 plans in order (writers, fields+readers, tests+shim).

**Codex verdict (reject ordering):** Plan 88-01 cannot just delete `state.X = ...` and keep `set_search_export(...)` calls — several export calls use `state.*` as scratch variables read AFTER the assignment in the same code block. Cited:
- Search restore uses state attrs in `set_search_export` args: `web/pages/search.py:3802-3820`
- Search partial/happy paths use state gap/filter attrs: `web/pages/search.py:4112-4140`, `4197-4231`
- Selection writes then passes `state.last_selected_uids`: `web/pages/search.py:2101-2106`, `web/pages/search_results.py:377-380`
- Parallels passes `state.parallels_search_meta`: `web/pages/parallels.py:1988-2002`, `2300-2338`

Concrete example: in search.py:4112-4140, `state.last_search_warnings = ['partial-results']` is set on line 4130, and `set_search_export(..., warnings=state.last_search_warnings, ...)` is called on lines 4134-4144. If Plan 88-01 deletes the assignment first, the `set_search_export(...)` call below reads `[]` instead of `['partial-results']` — silent data loss.

Codex also flagged: don't delete AppState fields before tests are rewritten if every plan boundary must stay green. Tests still touch deleted fields at `tests/test_api_export_json.py:65-94`, `tests/test_api_legacy_unchanged.py:48-81`, `tests/test_export_state_selection.py:97-145`, `tests/test_export_cross_user_isolation.py:143-160`.

**Codex-revised ordering:**
1. Writer migration: replace singleton assignments with locals and pass locals to export_state.
2. export_state/test migration: safe_storage, remove `_TEST_BACKEND`, rewrite 4 tests, remove allowlist.
3. AppState deletion/enforcement.

**Final decision (D-04, D-05):** Adopted Codex's ordering. Plan boundaries stay green; tests rewritten in Plan 88-02 BEFORE field deletion in Plan 88-03.

---

## Gray Area 3 — Deletion Enforcement

| Option | Description | Selected |
|--------|-------------|----------|
| Simple grep + delete (trust the static check) | Lowest cost, no permanent guard | |
| Attr-absence pytest (parametrized over 10 fields) | Runtime check, durable | partial |
| Static AST/grep test across web/ + tests/ | CI-enforced, mirrors Phase 87 lint pattern | ✓ |
| `__getattr__` sentinel on AppState | Production guard for one cycle then delete | |

**Claude's initial choice:** Attr-absence pytest.

**Codex verdict (agree, but insufficient):** `AppState` has no `__slots__`/`__setattr__` guard (`web/state.py:5-14`), so any later `state.last_results = ...` recreates the attr dynamically. Singleton is global (`web/state.py:99-100`), so runtime attr tests can be order-dependent. Keep the attr-absence test, but ADD a static AST/grep test rejecting `state.<deleted_field>` and `setattr(state, '<deleted_field>', ...)` across `web/` AND affected tests. Put both in Phase 88, not deferred to Phase 92.

Codex also flagged stale mentions to clean: `web/export_state.py:18-25` (module docstring), `web/api.py:1846-1848` (comment), `web/search_api.py:1198-1199` (docstring).

**Final decision (D-06, D-07, D-08, D-16):** Both tests in Phase 88. Stale mentions cleaned in Plan 88-03.

---

## Gray Area 4 — `_backend()` Helper Fate

| Option | Description | Selected |
|--------|-------------|----------|
| Delete `_backend()` entirely; functions use `safe_user_*` | Eliminates allowlist entry, conforms to Phase 87 chokepoint | ✓ refined |
| Keep `_backend()` as thin shim `return app.storage.user` | Preserves allowlist entry | |

**Claude's initial choice:** Delete `_backend()` entirely.

**Codex verdict (agree, refine):**
1. Don't return `safe_user_set`'s `bool`. Current setters return `None` and swallow failures (`web/export_state.py:55-81`, `136-149`); `safe_user_set` returns `bool` (`web/safe_storage.py:63-73`). Wrap to preserve None contract.
2. Add `isinstance(payload, dict)` guards in update functions. Current wrappers swallow bad payload-shape errors (`web/export_state.py:98-105`, `114-121`, `162-169`); `safe_user_*` only protects storage access (not poisoned-shape).
3. Prefer copy-on-update: `payload = dict(payload)` before mutation. Same-session read/modify/write races already exist; atomicity is out of scope for Phase 88 but defensive copy is cheap.

**Final decision (D-09, D-10, D-11, D-12):** Delete `_backend()`. Setter return `None`. Add `isinstance` + copy-on-update in update_* functions. Atomicity deferred.

---

## Gray Area 5 — Parallels `source_text` Bypass (Codex-surfaced)

This gray area was **not** identified in Claude's initial analysis. Codex caught it via grep of the API export handlers.

| Option | Description | Selected |
|--------|-------------|----------|
| Fold into `set_parallels_export(meta={'source_text': ...})` | Truly "export_state is the only path"; strictest STATE-02/STATE-03 compliance | ✓ |
| Document as deliberate non-export UI-persistence exception | Smaller Phase 88 scope; carve-out in export_state.py docstring | |
| Defer to Phase 92 SWEEP-01 sweep audit | Phase 88 stays focused on AppState deletion only | |

**Codex's catch:** Parallels `source_text` still bypasses `export_state`. Export handlers fall back to `safe_user_get('parallels_source_text', '')` (`web/api.py:1921-1931`, `1955-1964`, `2049-2065`). Writers maintain that legacy key (`web/pages/parallels.py:457`, reset `2049-2051`, persistence `2341-2344`). Per-session safe (uses safe_user_get) but violates the "export_state.py is the only path" goal.

**User's choice:** Fold into export_state in Phase 88.

**Final decision (D-13, D-14, D-15):** Pass `source_text` through `set_parallels_export(meta={'source_text': ...})` on every parallels writer path; delete the `safe_user_get('parallels_source_text', '')` fallback in the 3 reader sites in api.py; add a cross-user `source_text` leak regression test in Plan 88-02.

---

## Claude's Discretion

- Local-variable naming convention in Plan 88-01 writer migration — Claude picks at planning time, matching pattern at each call site.
- AST vs regex for static-grep test (D-07) — recommended AST for consistency with `tests/test_no_raw_storage_access.py`; Claude picks at planning time.
- Whether to split Plan 88-01 by file (1 plan with 3 atomic commits) or one commit per file — defer to planner's wave-structure judgement.

## Deferred Ideas

- True concurrent test coverage (Phase 92 SWEEP-05)
- AppState `__setattr__` guard fallback (only if D-07 fails to catch a regression)
- Atomicity for read-modify-write in update_* functions (out of scope for v7.12)
- Lists cache redesign (Phase 89 territory)

---

## Codex Round 5 Review Artefacts

- Prompt sent to Codex: `_tmp/codex_phase88_discuss_review_prompt.md` (1,200 lines including code excerpts and red-team questions)
- Codex response captured: `_tmp/codex_phase88_discuss_review_response.txt`
- Codex model: gpt-5.5 via codex-cli 0.128.0, read-only sandbox, xhigh reasoning effort
- Codex token cost: ~106K tokens
- Net effect of Codex review: 3 of 4 recommendations refined, plan ordering reversed, 1 new gray area surfaced and folded into scope, 4 additional decisions added to CONTEXT.md (D-10, D-11, D-12, D-15)
