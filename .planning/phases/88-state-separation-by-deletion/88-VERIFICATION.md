---
phase: 88-state-separation-by-deletion
verified: 2026-05-13T00:00:00Z
status: human_needed
score: 5/5 must-haves verified (automated portion); SC#2 requires human browser test
overrides_applied: 0
re_verification:
  previous_status: null
  previous_score: null
  gaps_closed: []
  gaps_remaining: []
  regressions: []
human_verification:
  - test: "SC#2 — Two concurrent browser sessions, xlsx export does not leak between users"
    expected: |
      Open https://genizahsearch.com (or local dev server) in two different browser
      profiles or two different machines so each has a distinct NiceGUI session
      cookie. In session A: run a search that produces a non-trivial result set
      (e.g., query='שלום' with mode='text'). In session B: do not run any search
      (leave results empty) OR run a different search whose result set is
      disjoint from A's. Then in session B, trigger an xlsx export via the
      export button. The downloaded xlsx file MUST contain session B's result
      set (or an empty/error response indicating B has no results) — it must
      NEVER contain session A's results. Repeat with parallels export from
      parallels.py page.
    why_human: |
      The automated test `test_two_sessions_get_independent_filenames` in
      tests/test_export_cross_user_isolation.py simulates two sessions
      sequentially by swapping `web.safe_storage.app` stubs between calls
      (see test docstring documenting the SEQUENTIAL-simulation caveat per
      Phase 88 CONTEXT.md D-03). True concurrent two-browser cross-user
      isolation requires a real NiceGUI runtime with two cookies in flight
      against the same Python process — pytest cannot reproduce this without
      Selenium/Playwright, which Phase 88 explicitly defers to Phase 92
      SWEEP-05. Until human-verified the security closure on the
      2026-05-12 cross-user leak incident is not fully validated end-to-end
      in production conditions.
  - test: "SC#2b — Parallels export cross-user isolation under real concurrency"
    expected: |
      In session A: run a parallels (composition) search with some unique
      source_text (e.g., 'alpha-leak-bait'). In session B: do not run any
      parallels search OR run a different one. Trigger parallels excel/word/json
      export from session B. The exported file MUST NOT contain session A's
      source_text or results. Verify in particular that 'alpha-leak-bait' does
      not appear anywhere in B's exported payload — this proves the legacy
      `app.storage.user['parallels_source_text']` reader-side fallback is
      genuinely dead in production (the strengthened D-15 automated test
      proves this in a sequential simulation; this human test confirms under
      real concurrent cookies).
    why_human: |
      Same reason as SC#2 — real two-cookie concurrent state cannot be
      reproduced in pytest. The strengthened automated test
      `test_parallels_source_text_cannot_leak_via_deleted_fallback` exercises
      a POSITIVE export path with bait + valid results sequentially, but the
      true concurrent surface is human-verify-only until Phase 92 SWEEP-05
      ships a Selenium/Playwright harness.
---

# Phase 88: State Separation by Deletion -- Verification Report

**Phase Goal:** Delete singleton mirrors on `AppState` so `web/export_state.py` is the only path for per-user export state, with the `_TEST_BACKEND` shim replaced by proper test fixtures.

**Verified:** 2026-05-13
**Status:** human_needed
**Re-verification:** No -- initial verification.

## Goal Achievement

### Observable Truths (Success Criteria from ROADMAP.md)

| #   | Success Criterion | Status | Evidence |
| --- | ------------------ | ------ | -------- |
| 1 | Static grep of `web/state.py:AppState` returns zero matches for the 10 deleted per-user fields -- they do not exist on the class in any form. | VERIFIED | `Grep self\.(last_results\|...\|parallels_search_meta) web/state.py` returns 0 matches. Runtime check via `python -c "from web.state import AppState; s=AppState(); assert not hasattr(s,'last_results')..."` passes for all 10 fields. The surviving 7 fields (meta_mgr, var_mgr, searcher, lab_engine, indexer, _local_lists_mgr, _user_lists_mgr) are intact. |
| 2 | Two concurrent browser sessions, xlsx export from session B contains session B's results -- never session A's. | NEEDS HUMAN | Automated `test_two_sessions_get_independent_filenames` passes (sequential simulation via swapped safe_storage stubs). Real concurrent two-browser test requires a NiceGUI runtime with two live session cookies -- pytest cannot reproduce this. Deferred to human verification (see `human_verification` section). The auto-test is a strong proxy: the architecture routes through per-session keys, and reading session A's payload from session B's storage is now impossible in code, but the production-end-to-end smoke must be performed by hand. |
| 3 | Static grep of `web/export_state.py` returns zero matches for `_TEST_BACKEND` -- the shim is gone. | VERIFIED | `Grep _TEST_BACKEND web/export_state.py` returns 0 matches. `Grep _TEST_BACKEND web/+tests/` returns 0 matches globally. Module no longer defines `_TEST_BACKEND` module-level variable nor `_backend()` helper (verified via `python -c "from web import export_state; assert not hasattr(export_state, '_TEST_BACKEND'); assert not hasattr(export_state, '_backend')"`). |
| 4 | `tests/test_export_cross_user_isolation.py` passes and asserts cross-user isolation against per-session storage, with no `_TEST_BACKEND` reference. | VERIFIED | 4 tests pass: `test_two_sessions_get_independent_filenames`, `test_empty_session_does_not_inherit_other_session_results`, `test_parallels_cross_user_isolation`, `test_parallels_source_text_cannot_leak_via_deleted_fallback` (the strengthened D-15 positive-path leak test per Refinement 2). `Grep _TEST_BACKEND tests/test_export_cross_user_isolation.py` returns 0 matches. Tests now monkeypatch `web.safe_storage.app` directly with SimpleNamespace stubs per Refinement 6. |
| 5 | `tests/test_export_state_selection.py`, `tests/test_api_export_json.py`, `tests/test_api_legacy_unchanged.py` all pass after dropping `state.*` setup -- use only `export_state` helpers. | VERIFIED | 9 + 5 + 4 = 18 tests pass across the 3 files. `_StateProxy` wrapper class deleted from test_export_state_selection.py. Fixtures populate per-session payload via `export_state.set_search_export(...)` / `set_parallels_export(...)` and patch storage to `{}` for empty cases. `Grep state\.(deleted fields) tests/test_export_state_selection.py` returns 0 matches (only docstring/historical references remain in test_no_appstate_export_fields.py and test_no_deleted_state_references.py, which intentionally reference the deleted names in seed-traps -- those are tests OF the regression guard, not consumers of the deleted state). |

**Score:** 5 / 5 truths verified (automated). SC#2 cleared in automated form but needs human confirmation for the concurrent-browser real-world contract.

### Required Artifacts

| Artifact | Expected | Status | Details |
| -------- | -------- | ------ | ------- |
| `web/state.py` | AppState class with 10 per-user export fields physically deleted | VERIFIED | 78 lines (vs 101 pre-Phase-88). init() declares only the 7 surviving service fields. Placeholder comment at line 26 cites Phase 88 migration. Unused `List`, `Dict`, `Any` typing imports trimmed (only `Optional` remains). |
| `web/export_state.py` | Fully rewritten; routes through safe_storage helpers; no `_TEST_BACKEND`; getters hardened with isinstance guard | VERIFIED | 145 lines. Module imports `safe_user_get/safe_user_set/safe_user_pop` from `web.safe_storage`. 7 setter/updater/clearer + 2 getter functions. `_TEST_BACKEND` and `_backend()` deleted. `get_search_export`/`get_parallels_export` return `None` if storage holds non-dict (Refinement 4 hardening). `update_*` funcs guard with `isinstance(payload, dict)` and copy-on-update before mutation (D-11 + D-12). All 9 functions wired through safe_storage; only 2 `app.storage.user` mentions in the file, both in docstrings (verified). |
| `web/pages/search.py` | 5 writer-site clusters migrated to locals; export_state calls preserved | VERIFIED | 5 wired call sites confirmed: `clear_search_export()` at line 2064, `update_search_export_selection(_selected_uids)` at line 2086, `set_search_export(...)` at lines 3794, 4114, 4204 -- each with kwargs sourced from underscore-prefixed locals. Grep for `state.<deleted_field> =` returns 0 matches. |
| `web/pages/search_results.py` | 2 writer sites migrated to locals | VERIFIED | 2 `update_search_export_*` calls present (count==2). Grep for `state.(last_results|last_selected_uids) =` returns 0 matches. |
| `web/pages/parallels.py` | 3-4 writer-site clusters migrated + D-13 source_text fold-in audited | VERIFIED | 4 `set_parallels_export(` calls present (sites 1, 1b, 2, 4) -- all bucket (b) per Refinement 3 audit (each carries `meta={'source_text': ...}` from `_snapshot_meta`, `_bootstrap_meta`, or `_parallels_search_meta`). `clear_parallels_export()` site at 2062 is bucket (a). Grep for `state.parallels_* =` returns 0 matches. Writer-side `safe_user_set('parallels_source_text', ...)` retained at lines 465 and 2055 per plan (legacy UI persistence; reader-side fallback in api.py is deleted -- these writes are now dead but plan defers their cleanup). |
| `web/api.py` | Reader-side `parallels_source_text` fallback deleted at 3 export handlers | VERIFIED | `Grep parallels_source_text web/api.py` returns 0 matches. `source_text = meta.get('source_text') or ''` is now the sole reader path. |
| `tests/test_no_appstate_export_fields.py` | New runtime attr-absence guard (D-06) -- 11 tests | VERIFIED | File exists, 73 lines. `DELETED_FIELDS` list of 10 names; parametrized `test_appstate_does_not_have_deleted_field` (10 cases) + `test_appstate_still_has_non_deleted_fields` (1 sanity). 11 tests pass. |
| `tests/test_no_deleted_state_references.py` | New static AST scanner (D-07) with R5 alias-import coverage -- 4 tests | VERIFIED | File exists, 256 lines. `_DeletedStateAccessVisitor` extends `ast.NodeVisitor`; `visit_ImportFrom` tracks aliased imports, `visit_Attribute` catches direct + chained access, `visit_Call` catches `setattr`/`getattr`. 4 tests pass: 3 seed-traps (attribute access, ignores-strings, aliased-imports) + 1 production scan. |
| `.planning/phase87_storage_allowlist.yaml` | `web/export_state.py` entry deleted | VERIFIED | 3 entries remain (auth_state.py, main.py, supabase_client.py). `web/export_state.py` block absent. Phase 87 lint scanner `test_allowlist_counts_exact` and 5 sibling tests all GREEN (count=3 matches `expected`). |

### Key Link Verification

| From | To | Via | Status | Details |
| ---- | -- | --- | ------ | ------- |
| `web/export_state.py` | `web/safe_storage.py` | `safe_user_get/safe_user_set/safe_user_pop` imports | WIRED | `from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop` at line 31. 5 safe_user_set calls + 5 safe_user_get calls + 2 safe_user_pop calls inside the 9 public functions. |
| `tests/test_export_cross_user_isolation.py` | `web/safe_storage.app` | `monkeypatch.setattr` with `_make_stub(initial_storage)` SimpleNamespace factory | WIRED | Per Refinement 6, instance-isolated SimpleNamespace stubs replace per-class `_StubApp`. Tests verified passing in real test run. |
| `web/api.py` reader sites (export_parallels_excel/word/json) | `set_parallels_export` meta dict | `meta.get('source_text') or ''` -- sole source after fallback deletion | WIRED | `Grep parallels_source_text web/api.py` returns 0. Reader code path consumes only `meta['source_text']` from the per-session payload. |
| `web/pages/search.py` writer sites | `web/export_state.set_search_export` | kwargs sourced from underscore-prefixed locals (`_results`, `_query`, `_mode`, `_gap`, `_filters_applied`, `_warnings`) | WIRED | 5 wired call sites confirmed via grep. |
| `web/pages/parallels.py` writer sites | `web/export_state.set_parallels_export` | kwargs from locals including `_parallels_search_meta`, `_snapshot_meta`, `_bootstrap_meta` (all containing `source_text` per Refinement 3 bucket-(b) audit) | WIRED | 4 set_parallels_export calls all bucket (b); 1 clear_parallels_export call is bucket (a). |

### Data-Flow Trace (Level 4)

| Artifact | Data Variable | Source | Produces Real Data | Status |
| -------- | ------------- | ------ | ------------------ | ------ |
| `web/export_state.py:set_search_export` | export_search_payload dict | constructed inline from kwargs (results, query, mode, gap, filters, warnings, selected_uids) | YES -- dict literal with all 7 fields | FLOWING |
| `web/export_state.py:get_search_export` | payload | `safe_user_get(_SEARCH_KEY, None)` | YES -- reads per-session storage; isinstance(payload, dict) guard returns None for poisoned shape | FLOWING |
| `web/export_state.py:update_search_export_results` | payload['results'] | read-copy-write through safe_user_get/safe_user_set with copy-on-update | YES -- writes the updated dict back via safe_user_set | FLOWING |
| `web/api.py` parallels export handlers | source_text | `meta.get('source_text') or ''` from get_parallels_export() payload | YES -- folded in by writer per D-13 at all 4 parallels.py writer sites | FLOWING |

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
| -------- | ------- | ------ | ------ |
| AppState instance has no deleted fields | `python -c "from web.state import AppState; s=AppState(); assert not hasattr(s,'last_results'); print('OK')"` | OK (all 10 fields tested individually) | PASS |
| AppState retains surviving fields | `python -c "from web.state import state; assert state.meta_mgr is None or state.meta_mgr is not None; print('OK')"` | OK | PASS |
| export_state module exposes 9 public functions | `python -c "from web import export_state; assert all(hasattr(export_state, f) for f in ['set_search_export','get_search_export',...]); print('OK')"` | OK (all 9 present) | PASS |
| `_TEST_BACKEND` and `_backend` helper absent from module | `python -c "from web import export_state; assert not hasattr(export_state,'_TEST_BACKEND'); assert not hasattr(export_state,'_backend')"` | OK | PASS |
| Phase 88 test files all pass | `pytest tests/test_export_cross_user_isolation.py tests/test_export_state_selection.py tests/test_api_export_json.py tests/test_api_legacy_unchanged.py tests/test_no_appstate_export_fields.py tests/test_no_deleted_state_references.py -v` | 37 tests pass | PASS |
| Phase 87 lint scanner intact | `pytest tests/test_no_raw_storage_access.py -v` | 6 tests pass (allowlist_well_formed, lint_rejects_synthetic_violation, lint_handles_aliased_imports, lint_does_not_double_report_nested_nodes, allowlist_counts_exact, no_raw_storage_access_outside_allowlist) | PASS |
| Full pytest suite | `pytest -q` | 1897 passed, 20 skipped | PASS |
| Ruff clean | `python -m ruff check .` | All checks passed! | PASS |
| Docs check | `python -X utf8 scripts/check_docs.py` | All checks passed! Documentation is healthy. | PASS |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
| ----------- | ---------- | ----------- | ------ | -------- |
| STATE-01 | 88-03 | 10 per-user fields deleted from `web/state.py:AppState` | SATISFIED | Grep returns 0 matches in web/state.py; runtime hasattr returns False for all 10; SC#1 verified |
| STATE-02 | 88-01 (+ 88-03 enforcement) | All writer sites (search.py, search_results.py, parallels.py) migrated to write exclusively through web/export_state.py | SATISFIED | Scoped grep `^\s*state\.(10 fields)\s*=` in web/ returns 0 matches; 11 set/update/clear_*_export calls confirmed wired across the 3 writer files |
| STATE-03 | 88-02 | All reader sites (api.py export handlers, others) migrated to read exclusively through web/export_state.py | SATISFIED | Grep `parallels_source_text web/api.py` returns 0 matches; api.py handlers consume only `meta.get('source_text')` and `get_search_export()/get_parallels_export()` results; SC#3-adjacent evidence |
| STATE-04 | 88-02 | `_TEST_BACKEND` shim removed from web/export_state.py; replaced with proper fixture or adapter injection | SATISFIED | SC#3 grep returns 0 matches; tests use `monkeypatch.setattr('web.safe_storage.app', _make_stub({...}))` SimpleNamespace factory pattern per Refinement 6 |
| STATE-05 | 88-02 | tests/test_export_cross_user_isolation.py rewritten to assert against per-session storage directly | SATISFIED | 4 tests pass (including strengthened D-15 positive-path leak test per Refinement 2); no `_TEST_BACKEND` reference; SC#4 verified |
| STATE-06 | 88-02 | tests/test_export_state_selection.py, test_api_export_json.py, test_api_legacy_unchanged.py updated | SATISFIED | 9 + 5 + 4 = 18 tests pass; no `state.*` setup remaining (only deliberate docstring/seed-trap references in test_no_appstate_export_fields.py and test_no_deleted_state_references.py); SC#5 verified |

**All 6 requirements satisfied.** No orphaned requirements -- the 6 STATE-XX IDs map cleanly to Plans 88-01/02/03 with no gaps.

### Anti-Patterns Found

From `.planning/phases/88-state-separation-by-deletion/88-REVIEW.md` (code review):

| File | Line | Pattern | Severity | Impact |
| ---- | ---- | ------- | -------- | ------ |
| tests/test_no_deleted_state_references.py | 90-104 | AST scanner blind to plain assignment-bound aliases (`s2 = state` not tracked by `visit_Assign`) -- a contributor doing `s = state` to shorten a long block would silently bypass the regression guard for that file (WR-01) | WARNING | Non-blocking. The guard catches import-as aliases and chained module.state.field access, which are the common cases. Plain-assignment alias is a theoretical attack surface that the chained-attribute heuristic partially covers (`.state.<field>` on anything will match). Phase 92 SWEEP cleanup or a Phase 88 follow-up can add `visit_Assign`. |
| web/export_state.py | 74-91, 132-139 | TOCTOU lost-update window in `update_*` functions: two concurrent same-session requests can race the read-copy-write trio and the second write clobbers the first's field update (WR-02) | WARNING | Non-blocking. Documented as low-impact: separate sessions key on separate keys (cross-USER case is sound). For same-session concurrency, NiceGUI's single-event-loop request handling typically serializes work. Suggest documenting the limitation in the module docstring or adding a per-session lock in a follow-up. The headline cross-user-leak fix is not affected. |
| web/pages/search.py | 4093-4094, 4112, 4178-4179, 4202 | Dead-on-arrival local variables (e.g., `_current_search_query = clean_query` never read; `set_search_export(...)` passes `clean_query` directly) (IN-01) | INFO | Stylistic cleanup. The 6 unused locals are leftover scaffolding from the mechanical `state.X = Y` → `_X = Y` rewrite. Behavior identical with or without these lines. |
| web/pages/search.py + web/pages/parallels.py | various | Stale line-number references in migration comments that drifted after Phase 88 edits (IN-03) | INFO | Comment rot inherited from pre-Phase-88 source, not introduced by this phase. |
| web/pages/parallels.py | 292-293, 315-316 | Bare `except Exception: pass` in bootstrap blocks (IN-04) | INFO | Pre-existing pattern; not introduced by Phase 88. |
| web/pages/parallels.py | 465, 2055 | Writer-side `safe_user_set('parallels_source_text', text)` retained after Plan 88-02 deleted the reader-side fallback -- these writes are now dead | INFO | Documented in Plan 88-01 SUMMARY ("safe_user_set('parallels_source_text', text) at line 457 retained as-is per plan (legacy UI persistence writer); the reader-side fallback in web/api.py is deleted in Plan 88-02"). Functional impact: zero -- the storage key is written but never read, so it's harmless waste. Cleanup deferred (not in Phase 88 scope). |

No blockers or critical anti-patterns. All findings are non-blocking warnings or info-level stylistic observations.

### Phase 87 Invariants (Regression Check)

| Invariant | Status |
| --------- | ------ |
| `tests/test_no_raw_storage_access.py` 6 tests all green | PASS (all 6 tests pass in the run) |
| Allowlist entries: 3 after web/export_state.py removal (auth_state, main, supabase_client) | PASS (verified in `.planning/phase87_storage_allowlist.yaml`; expected_count fields enforce exact count) |
| `web/safe_storage.py` chokepoint unmodified | PASS (export_state.py now imports from it; no edits to safe_storage.py in Phase 88) |
| `_session_uuid` minting unchanged | PASS (Phase 87 contract untouched) |

### Human Verification Required

See `human_verification` section in the YAML frontmatter above. Two items:

1. **SC#2** -- Real concurrent two-browser xlsx export test. The automated `test_two_sessions_get_independent_filenames` simulates via sequential stub swaps and passes, but the production-end-to-end smoke against two real cookies in flight against one Python process must be performed by hand. Until human-verified, the security closure on the 2026-05-12 cross-user leak incident is not fully validated end-to-end.

2. **SC#2b** -- Real concurrent two-browser parallels export test for the source_text fallback closure. The strengthened D-15 automated test (`test_parallels_source_text_cannot_leak_via_deleted_fallback`) covers this sequentially with bait + valid results; real concurrent confirmation is human-only until Phase 92 SWEEP-05 ships a Selenium/Playwright harness.

### Gaps Summary

**No blocking gaps.** All 5 ROADMAP.md success criteria are met by static and automated tests; all 6 STATE-XX requirements are satisfied; full pytest suite passes (1897 passed / 20 skipped); ruff clean; check_docs clean; Phase 87 lint scanner all 6 tests green; allowlist correctly reduced from 4 to 3 entries.

The phase achieves its goal: the 10 singleton mirror fields on AppState are physically deleted; `web/export_state.py` is the sole path for per-user export state; the `_TEST_BACKEND` shim is replaced by `web.safe_storage.app` monkeypatching with SimpleNamespace stubs; cross-AI review refinements (R1 scoped greps, R2 strengthened D-15 leak test, R4 getter hardening, R5 alias-import scanner coverage, R6 SimpleNamespace stubs) are all landed.

**Code review warnings (WR-01 and WR-02) are non-blocking** -- they identify follow-up hardening opportunities (assignment-bound alias scanner extension and same-session TOCTOU documentation/lock) that do not affect the headline cross-user-leak fix.

The only non-automated verification needed is SC#2 -- the production concurrent-browser smoke test of xlsx and parallels exports. This is intrinsic to the success criterion ("user opens two concurrent browser sessions...") which the ROADMAP explicitly flagged for human verification.

---

_Verified: 2026-05-13_
_Verifier: Claude (gsd-verifier)_
