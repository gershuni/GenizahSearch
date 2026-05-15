---
phase: 89-lists-cache-per-request
plan: 02
subsystem: web-state-multitenant
tags: [user-lists, multitenant, atomic-deletion, regression-guard, static-ast-scanner, runtime-attr-absence, xfail-strict]

# Dependency graph
requires:
  - phase: 88-state-separation-by-deletion
    provides: AST scanner template (test_no_deleted_state_references.py), runtime attr-absence template (test_no_appstate_export_fields.py), D-09 atomic-commit discipline, survivors-list pattern
  - phase: 89-01
    provides: factory-shaped lists_mgr @property, stateless UserListsManager (no _cache_entry/_cache_ttl), 4 rewritten cache-isolation behavior tests, dead-code-temporary _user_lists_mgr field/method waiting for atomic deletion
provides:
  - AppState minus _user_lists_mgr field — singleton mirror gone (LISTS-01 closed)
  - AppState minus init_user_lists_mgr method — bootstrap helper gone
  - web/main.py:1508 minus state.init_user_lists_mgr() call — bootstrap sequence one line shorter
  - Phase 88 survivors list updated (D-09 — same-commit invariant preserved)
  - Permanent static AST regression guard (tests/test_no_deleted_lists_state_references.py — 8 tests, with R7 narrow-scope, R8 Call-node, EXEMPT_FILES verification)
  - Permanent runtime attr-absence regression guard (tests/test_no_user_lists_mgr_field.py — 4 tests, parametrized over the 3 deleted fields)
affects: [phase 90 (auth caching rewrite — uses same per-access factory + AST-scanner pattern), phase 92 (final sweep + acceptance — runs the AST scanners as part of acceptance)]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Strict xfail markers as plan-execution scaffolding: install deletion-dependent regression tests with @pytest.mark.xfail(strict=True) so the suite stays GREEN at every intermediate task boundary; remove markers in the atomic deletion commit. XPASS catches stale markers immediately."
    - "Atomic-commit operational gates (R10): pre-commit `git diff --cached --name-only | sort` and post-commit `git show --stat --name-only HEAD` MUST match the exact expected 5-file set verbatim; any divergence halts the commit."
    - "AST scanner with narrow-scope refinement (R7): some forbidden names get unrestricted attribute catches inside their owning files (`_cache_entry`/`_cache_ttl` in web/user_lists.py + web/state.py) but state-alias-qualified-only catches outside — prevents false-positives on unrelated future utilities."
    - "AST scanner with Call-node coverage (R8): visit_Call catches `<state-alias>.init_user_lists_mgr(...)` for the converse regression where someone restores the caller without re-adding the method."
    - "EXEMPT_FILES exemption-verification test (R7): a dedicated test asserts EXEMPT_FILES contains exactly the scanner-companion files AND that _scan_file is not invoked for them — guards against drift in either direction."

key-files:
  created:
    - "tests/test_no_deleted_lists_state_references.py — 480 lines, 8 tests (D-10 + R7 narrow-scope + R8 Call-node + EXEMPT_FILES verification + strings-and-comments-ignored + strict-xfail-removed-by-Task-3 production scan)"
    - "tests/test_no_user_lists_mgr_field.py — 96 lines, 4 tests (D-11 parametrized over _user_lists_mgr / _cache_entry / _cache_ttl + _local_lists_mgr survivor sanity)"
  modified:
    - "web/state.py — _user_lists_mgr field deleted from init(); init_user_lists_mgr() method deleted entirely; setter simplified (3-line unreachable block removed); docstring refreshed (75 lines, down from 92)"
    - "web/main.py — state.init_user_lists_mgr() call deleted at line 1508 + preceding comment block (1608 lines, down from 1611)"
    - "tests/test_no_appstate_export_fields.py — Phase 88 survivors list line 67 no longer contains '_user_lists_mgr' (D-09 critical: same commit as field deletion)"

key-decisions:
  - "D-09 atomic-commit discipline preserved exactly: Phase 88 survivor test update + field deletion + xfail-marker removals all in ONE git commit (844e5b53). R10 pre/post-commit gates verified the exact 5-file scope."
  - "R7 narrow-scope refinement embedded in scanner: NARROW_SCOPE_OWNING_FILES = {'web/user_lists.py', 'web/state.py'} controls whether _cache_entry/_cache_ttl get unrestricted attribute catch. test_scanner_narrow_scope_for_cache_entry_outside_owning_files asserts the scope discipline both inside AND outside owning files."
  - "R8 Call-node coverage added: visit_Call detects `<state-alias>.init_user_lists_mgr(...)` shape (including aliased state imports `s.init_user_lists_mgr()`). Complements visit_FunctionDef which catches the method-restoration regression."
  - "R9 strict-xfail scaffolding chosen over single mega-commit: Tasks 1+2 install regression tests with @pytest.mark.xfail(strict=True), Task 3 removes both markers atomically. Trade-off: 3 small task commits readable in git log + strict mode catches stale markers as XPASS = hard failure. Mega-commit would have lost task-level commit granularity."
  - "R10 operational gates encoded as actual `git diff --cached --name-only | sort` + `git show --stat --name-only HEAD` invocations before/after `git commit`. Output captured verbatim in this SUMMARY for audit trail."
  - "D-10 'self' added to default state bindings (in addition to 'state' / 'app_state'): catches the most likely regression form `self._user_lists_mgr = ...` inside AppState.init() restoration. Phase 88's scanner didn't include 'self' because Phase 88's deleted fields weren't private (no leading underscore convention to use as a self-attribute reset)."

patterns-established:
  - "Plan-level R9 + R10 dance: install regression guards with strict-xfail, then atomic-commit deletion + marker-removal together. Phase 90 will use the same shape for the `_client_cache` deletion (same per-request-not-per-access reframe Codex made in CONTEXT.md)."
  - "Cross-plan handshake via dead-code temporary fields: Plan 89-01 left `_user_lists_mgr = None` in AppState.init() as a labeled temporary; Plan 89-02 deleted it. Same shape will apply to Phase 90 (Plan 90-01 leaves `_client_cache` in place if delete-everything-at-once is impractical; Plan 90-02 closes)."
  - "AST scanner exemption-verification test (R7): every scanner that uses EXEMPT_FILES MUST have a test that BOTH (a) asserts membership and (b) confirms the production loop respects the exemption via a spy on _scan_file. Prevents quiet drift where EXEMPT_FILES grows but the scan loop forgets to honor it."

requirements-completed: [LISTS-01, LISTS-03]
# LISTS-02 (per-request lifecycle for UserListsManager) and LISTS-04 (cross-user
# fetch isolation) were satisfied empirically by Plan 89-01's behavior tests.
# LISTS-01 + LISTS-03 are explicitly closed by this plan's deletion + structural
# enforcement via the permanent CI guards installed by Tasks 1+2.

# Metrics
duration: 12min
completed: 2026-05-15
---

# Phase 89 Plan 02: Atomic Deletion + Permanent Regression Guards Summary

**AppState._user_lists_mgr singleton field deleted + init_user_lists_mgr method deleted + web/main.py:1508 bootstrap call deleted + Phase 88 survivors list updated + xfail markers removed — all in ONE atomic 5-file commit (R10 verified). Two permanent CI regression guards installed: static AST scanner (8 tests) + runtime attr-absence (4 tests). Plan boundary green at 1911 passed / 21 skipped.**

## Performance

- **Duration:** ~12 min
- **Started:** 2026-05-15T03:50:58Z
- **Completed:** 2026-05-15T04:03:24Z (sample after full pytest)
- **Tasks:** 4 (Tasks 1-3 each committed atomically; Task 4 was verification only, no new commit)
- **Files modified:** 3 (`web/state.py`, `web/main.py`, `tests/test_no_appstate_export_fields.py`)
- **Files created:** 2 (`tests/test_no_deleted_lists_state_references.py`, `tests/test_no_user_lists_mgr_field.py`)
- **Total atomic commit (Task 3) touched:** 5 files (R10 verified)

## Accomplishments

- **LISTS-01 closed structurally:** `AppState._user_lists_mgr = None` field removed from `AppState.init()` (web/state.py:24 deleted). The runtime test `test_appstate_does_not_have_user_lists_mgr` and the static AST scanner test `test_no_deleted_lists_references_in_web_and_tests` both PASS — two independent permanent guards.
- **`init_user_lists_mgr()` method deleted** from web/state.py (lines 67-80 in Plan 89-01 state). The factory @property handles all UserListsManager construction lazily on every access.
- **`web/main.py:1508` bootstrap call deleted** along with the comment line above it. Bootstrap sequence flows directly from `state.lists_mgr = ListsManager(state.meta_mgr)` (now line 1517) to `state.lab_engine = LabEngine(...)` (now line 1520). Three lines removed.
- **D-09 atomic-commit discipline satisfied:** Phase 88's `test_appstate_still_has_non_deleted_fields` reads `'_user_lists_mgr'` from the survivors list at line 67. Deleting the AppState field without updating that list in the same commit would red-fail Phase 88's test — the plan boundary discipline (D-05) would break. The atomic commit at `844e5b53` includes BOTH the field deletion AND the survivors-list edit, so Phase 88's test stays green throughout. **Verified:** `pytest tests/test_no_appstate_export_fields.py -v` reports 11 passed (10 parametrized + 1 survivor sanity).
- **R9 strict-xfail scaffolding worked end-to-end:**
  - Task 1 commit `4562f444` reported `7 passed, 1 xfailed` (strict).
  - Task 2 commit `3d9d54e8` reported `3 passed, 1 xfailed` (strict).
  - Task 3 commit `844e5b53` removed BOTH xfail decorator blocks atomically. Post-commit, the previously-xfailed tests report **PASSED** (not XPASS — markers gone, assertions natural-truthy).
- **R10 operational gates verified verbatim** (see "R10 Atomic-Commit Evidence" section below).
- **Plan boundary green:** Full `pytest tests/ -x --tb=short` exits 0 with **1911 passed, 21 skipped, 2 warnings**. Up from Plan 89-01's 1899 passed — the +12 delta is exactly 8 from `test_no_deleted_lists_state_references.py` + 4 from `test_no_user_lists_mgr_field.py`. The 2 warnings are pre-existing (httpx deprecation + urllib3 system-time warning) and unrelated to Phase 89.

## Task Commits

Each task was committed atomically with `--no-verify` (parallel-executor convention):

1. **Task 1: Static AST scanner with strict-xfail** — `4562f444` (test) — 8 tests, 7 passed / 1 xfailed.
2. **Task 2: Runtime attr-absence test with strict-xfail** — `3d9d54e8` (test) — 4 tests, 3 passed / 1 xfailed.
3. **Task 3: Atomic 5-file deletion + Phase 88 survivor update + xfail-marker removals** — `844e5b53` (feat) — R10 pre/post-commit gates verified.
4. **Task 4: Plan-boundary verification** — no commit (verification only). 16 Phase-89 tests + 14 Phase-88-survivor tests + full pytest all green.

## R10 Atomic-Commit Evidence (Task 3 — commit 844e5b53)

### Pre-commit gate: `git diff --cached --name-only | sort`

```
tests/test_no_appstate_export_fields.py
tests/test_no_deleted_lists_state_references.py
tests/test_no_user_lists_mgr_field.py
web/main.py
web/state.py
```

**5 files, alphabetically sorted. Matches the planned R10 expectation EXACTLY. No others.**

### Post-commit gate: `git show --stat --name-only HEAD` (filtered to file list)

```
tests/test_no_appstate_export_fields.py
tests/test_no_deleted_lists_state_references.py
tests/test_no_user_lists_mgr_field.py
web/main.py
web/state.py
```

**5 files, same set as pre-commit gate. R10 atomic-commit invariant satisfied.**

### Cross-check: `git diff HEAD~1 HEAD --name-only | sort`

```
tests/test_no_appstate_export_fields.py
tests/test_no_deleted_lists_state_references.py
tests/test_no_user_lists_mgr_field.py
web/main.py
web/state.py
```

**Same 5 files. Atomic-commit boundary structurally enforced.**

### Commit stat: `5 files changed, 18 insertions(+), 56 deletions(-)`

Net 38-line deletion — consistent with the surgical-deletion intent. The 18 insertions are the simplified setter docstring (10 lines), the refreshed factory-property docstring (~3 lines), and the removed xfail decorator + survivors-list edit are net-negative (the deletions).

## Files Created/Modified

- `tests/test_no_deleted_lists_state_references.py` — **created (480 lines).** 8 tests: 4 seed-trap variants (attribute, setattr/getattr/hasattr, FunctionDef, R8 call-site), R7 narrow-scope verification, EXEMPT_FILES exemption-verification, strings-and-comments-ignored, production-scan. Includes class `_DeletedListsAccessVisitor(ast.NodeVisitor)` with five distinct catches: direct attribute, chained attribute (web_state.state.X), R7 narrow scope, setattr/getattr/hasattr Call, R8 init_user_lists_mgr Call.
- `tests/test_no_user_lists_mgr_field.py` — **created (96 lines).** 4 tests: AppState._user_lists_mgr absent (R9 xfail marker removed in Task 3), UserListsManager._cache_entry absent, UserListsManager._cache_ttl absent, AppState._local_lists_mgr survivor sanity.
- `web/state.py` — **modified (75 lines, down from 92).** Deletions: `self._user_lists_mgr = None` field initialization (was line 24), 3-line unreachable setter block (`if self._user_lists_mgr is not None: ...`), entire `def init_user_lists_mgr(self):` method (was 14 lines). Replacements: setter docstring expanded (10 lines), factory-property docstring refreshed to reference Plan 89-02 completion.
- `web/main.py` — **modified (1608 lines, down from 1611).** Single deletion: 3-line block at original lines 1518-1520 (blank line + comment + `state.init_user_lists_mgr()` call). Bootstrap sequence now flows `state.lists_mgr = ListsManager(state.meta_mgr)` → `state.lab_engine = LabEngine(state.meta_mgr, None)` with no intermediate user-lists-mgr initialization step.
- `tests/test_no_appstate_export_fields.py` — **modified.** Single edit: removed `'_user_lists_mgr'` from the `survivors` list (line 67 originally; now line 67 contains only `'_local_lists_mgr',`). Phase 88's survivors-sanity test stays green per D-09.

## Decisions Made

### D-09 atomic-commit discipline preserved exactly

**Implementation:** Single git commit `844e5b53` includes all 5 file edits. R10 pre/post-commit gates verified the exact 5-file scope. Phase 88's `test_appstate_still_has_non_deleted_fields` reads the survivors list — if `'_user_lists_mgr'` had stayed in the list after the field deletion, the test would fail (hasattr returns False, the assert raises). Bundling the edits in one commit means there is no intermediate commit where the test would fail.

### R7 narrow-scope refinement structurally encoded

**Implementation:** `NARROW_SCOPE_OWNING_FILES = frozenset({'web/user_lists.py', 'web/state.py'})` controls the `is_narrow_owning_file` parameter passed to `_DeletedListsAccessVisitor`. Inside narrow-owning files, `_cache_entry`/`_cache_ttl` get unrestricted attribute-name catch. Outside, they're only flagged via state-alias-qualified access (or via `self.<>` because 'self' is in default bindings, which is acceptable as documented). The `test_scanner_narrow_scope_for_cache_entry_outside_owning_files` test asserts BOTH directions of the scope discipline (PASS outside without state-alias / FLAG inside).

### R8 Call-node coverage added

**Implementation:** `visit_Call` checks `isinstance(node.func, ast.Attribute) and node.func.attr in DELETED_FUNCTION_NAMES`. Walks the inner value to confirm state-alias qualification (Name or Attribute with `.state` chain). Catches `state.init_user_lists_mgr()`, `s.init_user_lists_mgr()` (aliased import), and `web_state.state.init_user_lists_mgr()` (chained module access).

### R9 strict-xfail scaffolding worked

**Implementation:** Tasks 1+2 used `@pytest.mark.xfail(strict=True, reason=...)` on the deletion-dependent tests. Strict mode means an unexpectedly-passing test becomes a HARD failure (XPASS reported as FAIL by pytest). At Task 1+2 boundaries, the test ran AS XFAIL (planned). At Task 3, the field deletion lands AND the decorator removal lands AT THE SAME TIME — so XPASS never appears in pytest output. Post-Task-3, the tests report PASSED (clean) with no XFAIL/XPASS noise.

**Why strict mode mattered:** Without `strict=True`, an unexpectedly-passing test would just report XPASS (a warning-level signal). With strict mode, it becomes a hard failure — which is what we want at the moment the marker is stale. If a future PR independently deletes the field without removing the marker, strict-xfail XPASS will catch it.

### R10 operational gates executed verbatim

**Implementation:** Before `git commit`, ran `git add web/state.py web/main.py tests/test_no_appstate_export_fields.py tests/test_no_deleted_lists_state_references.py tests/test_no_user_lists_mgr_field.py` then `git diff --cached --name-only | sort`. Output captured matched the expected 5-file list verbatim. After `git commit --no-verify`, ran `git show --stat --name-only HEAD` and confirmed the same 5 files. Both output sets pasted verbatim above in the "R10 Atomic-Commit Evidence" section for audit trail.

## R7 Narrow-Scope Evidence

```
$ pytest tests/test_no_deleted_lists_state_references.py::test_scanner_narrow_scope_for_cache_entry_outside_owning_files -v
PASSED [100%]

$ pytest tests/test_no_deleted_lists_state_references.py::test_exempt_files_are_skipped_in_production_scan -v
PASSED [100%]
```

The narrow-scope test asserts:
- **Outside owning files (is_narrow_owning_file=False)**: `mgr._cache_entry = None` where `mgr` is NOT a state binding produces ZERO violations (after filtering `self.` self-bindings which are intentionally caught everywhere).
- **Inside owning files (is_narrow_owning_file=True)**: The same `mgr._cache_entry = None` PRODUCES violations (≥2 — _cache_entry + _cache_ttl).

The exemption-verification test asserts:
- `EXEMPT_FILES` contains exactly `{'tests/test_no_deleted_lists_state_references.py', 'tests/test_no_user_lists_mgr_field.py'}` (membership + cardinality).
- The production scan loop iterating `SCAN_DIRS` SKIPS the exempt paths (verified via spy on `_scan_file`).

## R8 Call-Node Evidence

```
$ pytest tests/test_no_deleted_lists_state_references.py::test_scanner_catches_init_user_lists_mgr_call_site -v
PASSED [100%]
```

The synthetic source:
```python
from web.state import state
import web.state as web_state
from web.state import state as s
def restored_call():
    state.init_user_lists_mgr()        # caught via 'state' default binding
    s.init_user_lists_mgr()            # caught via aliased state import
    web_state.state.init_user_lists_mgr()  # caught via chained module access
```

The visitor produces ≥2 call-site violations (asserted in the test). All three forms covered structurally.

## R9 xfail Cleanup Evidence

```
$ grep -n "@pytest.mark.xfail" tests/test_no_deleted_lists_state_references.py tests/test_no_user_lists_mgr_field.py
tests/test_no_user_lists_mgr_field.py:19:is marked `@pytest.mark.xfail(strict=True)` until Task 3 deletes the
```

The lone hit (line 19) is inside the module docstring — a comment about R9's mechanism. No `@pytest.mark.xfail` DECORATOR survives in either file. Confirmed by:

```
$ pytest tests/test_no_deleted_lists_state_references.py tests/test_no_user_lists_mgr_field.py -v --tb=short | grep -E "(PASSED|XFAIL|XPASS)"
# All 12 tests report PASSED, 0 XFAIL, 0 XPASS.
```

## Phase 89 ROADMAP Success Criteria Verification

| SC | Criterion | Evidence |
|----|-----------|----------|
| 1 | No `_user_lists_mgr` field on AppState | `grep -rn "_user_lists_mgr" web/` returns 0 lines. `python -c "from web.state import AppState; assert not hasattr(AppState(), '_user_lists_mgr')"` exits 0. Static AST scanner `test_no_deleted_lists_references_in_web_and_tests` PASSES. |
| 2 | No `_cache_entry` or `_cache_ttl` constant on UserListsManager | `grep -rn "_cache_entry\|_cache_ttl" web/` returns 0 lines. Static AST scanner with R7 narrow-scope catches inside owning files; runtime `test_user_lists_manager_does_not_have_cache_entry` + `test_user_lists_manager_does_not_have_cache_ttl` BOTH PASS. |
| 3 | Cross-user lists isolation (no leak across user switches) | `test_authenticated_fetch_does_not_leak_across_users` PASSES; `test_captured_manager_does_not_serve_stale_data_after_user_switch` (R1 captured-manager case from Plan 89-01) PASSES — proves the closure-capture regression class is structurally impossible. |
| 4 | `test_user_lists_cache_isolation.py` rewritten to per-request model | File contains 4 behavior tests: `test_two_accesses_get_distinct_managers`, `test_authenticated_fetch_does_not_leak_across_users`, `test_captured_manager_does_not_serve_stale_data_after_user_switch`, `test_invalidate_cache_is_compatibility_no_op` — all PASS. |

All 4 SCs satisfied with concrete evidence.

## Permanent CI Guards Installed

1. **Static AST scanner** at `tests/test_no_deleted_lists_state_references.py` — 480 lines, 8 tests. Walks `web/` + `tests/` looking for re-introductions of the deleted names via 5 distinct AST patterns (direct attribute, chained attribute, setattr/getattr/hasattr, FunctionDef restoration, R8 Call-node restoration). NARROW_SCOPE_OWNING_FILES + EXEMPT_FILES handle scope discipline.

2. **Runtime attr-absence test** at `tests/test_no_user_lists_mgr_field.py` — 96 lines, 4 tests. Direct `hasattr()` assertions on `AppState()` + `UserListsManager(None, None)` instances. Catches dynamic attribute re-introduction even when the AST scanner would miss it (e.g., field name built via string concat or set through metaclass).

Both files are included in the standard `pytest tests/` run — no special CI invocation needed.

## Total Phase 89 Line-Count Delta

| File | Pre-Plan-89 | Post-Plan-89-02 | Delta |
|------|-------------|-----------------|-------|
| `web/state.py` | 116 (Plan 89-01 start) | 75 | **-41 lines** |
| `web/main.py` | 1611 (Plan 89-01 start) | 1608 | **-3 lines** |
| `web/user_lists.py` | ~1000 (pre-89, exact figure in Plan 89-01) | 936 | **-64 lines** (Plan 89-01 cache deletion) |
| `tests/test_user_lists_cache_isolation.py` | 3 atomic-tuple tests | 4 behavior tests | rewritten (89-01) |
| `tests/test_no_deleted_lists_state_references.py` | (didn't exist) | 480 | **+480 lines (new)** |
| `tests/test_no_user_lists_mgr_field.py` | (didn't exist) | 96 | **+96 lines (new)** |
| **Net** | — | — | **-44 production lines + 576 test lines** |

## Deviations from Plan

**None — plan executed exactly as written.**

The plan was highly prescriptive (literal Edit blocks, file-path verbatim instructions, exact R10 expected output) and pre-revised against Gemini + Codex review with R7-R10 refinements. No Rule-1 bugs encountered, no Rule-2 missing functionality, no Rule-3 blockers, no Rule-4 architectural questions. All R7-R10 review items were already in the plan as explicit `<action>` steps and were applied verbatim.

## Issues Encountered

**One operational issue (mirroring Plan 89-01's experience):** Initial `Write` call for `tests/test_no_deleted_lists_state_references.py` defaulted to the main-repo path (`C:\Genizahsearch\tests\...`) instead of the worktree subtree path (`C:\Genizahsearch\.claude\worktrees\agent-ae5c69bc6aff287ee\tests\...`) — despite the env block listing the worktree as the working directory. Detected by `git status --short` in the worktree showing no new file. Removed the misplaced file from main repo with `rm /c/Genizahsearch/tests/test_no_deleted_lists_state_references.py` (specific-file untracked-file delete — safe per `<destructive_git_prohibition>` since it was a brand-new untracked file I had just created in this session, not anything pre-existing). Re-issued `Write` with explicit absolute worktree path. All subsequent file operations used absolute worktree paths to avoid recurrence.

**Lesson echoing Plan 89-01:** Always provide absolute worktree paths to file-write tools when running in parallel-executor configuration. The "Working directory" env hint is advisory; the Write tool may resolve paths against a different default.

## User Setup Required

None — internal refactor + new CI guards. Zero user-visible behavior change. Web-only milestone — desktop unaffected.

## Hand-off Note to Phase 90 (Auth Caching Rewrite — No set_session)

Phase 90 uses the same patterns Phase 89 established:

1. **Per-access factory + stateless wrapper** (Plan 89-01 pattern): Phase 90 will install a request-scoped auth strategy that returns a fresh authenticated Supabase client wrapping the session UUID, instead of caching authenticated client objects in `_client_cache`.

2. **R9 strict-xfail scaffolding + R10 atomic-commit operational gates** (Plan 89-02 pattern): Phase 90's deletion of `_client_cache`, `_session_locks`, `_locks_guard`, `_CLIENT_CACHE_TTL` from `web/supabase_client.py` (plus the auth `_app.storage.user` allowlist entry at line 111) will need analogous regression guards. The Plan 89-02 SUMMARY's R10 evidence section is a reference template for the audit trail format.

3. **AST scanner template with R7 narrow-scope + R8 Call-node coverage** (Plan 89-02 pattern): Phase 90's regression guard will scan `web/` + `tests/` for re-introductions of the deleted auth-caching names. R7 narrow-scope is relevant if any of the deleted names are private fields whose owning file is `web/supabase_client.py`; R8 Call-node coverage is relevant if any deleted-name is a method that has both a definition and a call site.

4. **NO `auth.set_session()` mid-flight** (CONTEXT.md constraint): Codex verified `gotrue_client.py:713` — `set_session()` is networked (calls `get_user(access_token)` when JWT valid, `_refresh_access_token(refresh_token)` when expired). Request-scoped auth in Phase 90 must avoid `set_session()` entirely.

5. **Phase 90 may inherit the EXEMPT_FILES + spy-verification pattern** if the scanner has companion seed-trap test files. The exemption-verification test shape is reusable across all Phase 87-92 regression-guard scanners.

## Next Phase Readiness

- LISTS-01 + LISTS-03 + LISTS-02 + LISTS-04 are all satisfied (LISTS-02 + LISTS-04 by Plan 89-01 empirically, LISTS-01 + LISTS-03 by this plan structurally).
- The 4 ROADMAP Phase 89 success criteria are empirically verified with test-pass + grep evidence (table above).
- Plan boundary is green: full `pytest tests/` exits 0 with 1911 passed.
- v7.12 Path B progress: 3 of 6 phases complete (Phase 87 ✅, Phase 88 ✅, Phase 89 ✅). Next: Phase 90 (Auth Caching Rewrite).

## Self-Check: PASSED

- File `tests/test_no_deleted_lists_state_references.py` — verified present (480 lines, 8 tests, all pass).
- File `tests/test_no_user_lists_mgr_field.py` — verified present (96 lines, 4 tests, all pass).
- Commit `4562f444` (Task 1) — verified via `git log --oneline -5`.
- Commit `3d9d54e8` (Task 2) — verified via `git log --oneline -5`.
- Commit `844e5b53` (Task 3 atomic) — verified via `git log --oneline -5`; R10 pre/post-commit gates output captured verbatim above.
- Phase 89 + Phase 88 trio: 30 tests PASS (15 + 14 + 1 sanity), 0 XFAIL, 0 XPASS, 0 FAIL.
- Full pytest: 1911 passed, 21 skipped, 2 warnings (pre-existing). Plan boundary green per D-05.
- `grep -rn "_user_lists_mgr" web/` returns 0 lines.
- `grep -rn "init_user_lists_mgr" web/` returns 0 lines.
- `grep -rn "_cache_entry\|_cache_ttl" web/` returns 0 lines.
- `grep -n "@pytest.mark.xfail" tests/test_no_deleted_lists_state_references.py tests/test_no_user_lists_mgr_field.py` returns 1 line — that line is a DOCSTRING comment, not a decorator; both decorators are gone.
- `_local_lists_mgr` survives in `web/state.py` (8 references) — surgical deletion confirmed.

---
*Phase: 89-lists-cache-per-request*
*Plan: 89-02*
*Completed: 2026-05-15*
