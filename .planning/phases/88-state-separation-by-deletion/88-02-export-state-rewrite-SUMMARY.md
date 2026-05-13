---
phase: 88-state-separation-by-deletion
plan: 02
subsystem: web
tags: [multitenant, app-storage, export-state, refactor, phase-88, path-b]

# Dependency graph
requires:
  - phase: 88-state-separation-by-deletion
    plan: 01
    provides: 13 writer sites migrated to locals; D-13 source_text fold-in landed in meta dict at every parallels.py writer; AppState fields physically present but write-orphaned
provides:
  - web/export_state.py routed through web.safe_storage chokepoint (no _TEST_BACKEND, no _backend helper)
  - update_* hardened with isinstance(payload, dict) + copy-on-update (D-11, D-12)
  - get_search_export + get_parallels_export hardened with isinstance guard (Refinement 4)
  - parallels_source_text reader-side fallback deleted at 3 web/api.py handlers (D-14)
  - 4 test files rewritten to monkeypatch web.safe_storage.app with SimpleNamespace stubs (Refinement 6); no _TEST_BACKEND, no _StateProxy, no state.X = fixture setup
  - D-15 source_text leak test strengthened: positive-export path with bait in legacy key + valid results force handler to reach the code branch that USED to read the fallback (Refinement 2)
  - web/export_state.py allowlist entry deleted from .planning/phase87_storage_allowlist.yaml
affects: [88-03-appstate-deletion-and-enforcement]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "SimpleNamespace(storage=SimpleNamespace(user=...)) instance-isolated test stub mirroring app.storage.user surface (Refinement 6)"
    - "monkeypatch.setattr('web.safe_storage.app', _make_stub(initial_storage)) — canonical Phase 88 test pattern"
    - "isinstance(payload, dict) guard on both getters and update_* funcs — explicit dict-or-None contract for storage values at export keys"
    - "Copy-on-update (payload = dict(payload)) in update_* funcs — defense against shared-reference races"

key-files:
  created: []
  modified:
    - "web/export_state.py (full rewrite — 85 insertions, 118 deletions; safe_storage chokepoint routing; ABI preserved; hardening guards on getters and update_* funcs)"
    - "web/api.py (3 reader-side parallels_source_text fallback blocks deleted; 8 insertions, 15 deletions; safe_user_get import removed from each handler)"
    - "tests/test_export_cross_user_isolation.py (rewrite: 124 insertions, 35 deletions; +1 strengthened D-15 positive-path leak test; 4 tests total)"
    - "tests/test_export_state_selection.py (rewrite: 119 insertions, 119 deletions; _StateProxy deleted; +1 getter-hardening test; 9 tests total)"
    - "tests/test_api_export_json.py (rewrite: portion of 104 insertions, 99 deletions; 5 tests total)"
    - "tests/test_api_legacy_unchanged.py (rewrite: portion of 104 insertions, 99 deletions; 4 tests total)"
    - ".planning/phase87_storage_allowlist.yaml (web/export_state.py entry deleted — 20 deletions; 3 entries remain)"

key-decisions:
  - "Comments referencing the legacy key (`parallels_source_text`) at the 3 deleted reader sites rephrased to 'legacy app.storage.user key fallback' to satisfy the strict 0-match grep gate while preserving audit trail."
  - "Docstring mention of `_TEST_BACKEND` in web/export_state.py rephrased to 'pre-Phase-88 test-backend shim' for the same reason — Refinement 1 scoped grep to web/ + tests/ must return 0 matches."
  - "Storage isinstance guard placed on BOTH getters (Refinement 4) and update_* mutators (D-11) — fully closes the poisoned-shape attack surface."
  - "Empty-storage fixtures (empty_search_state / empty_parallels_state) use monkeypatch.setattr('web.safe_storage.app', _make_stub({})) so the handler reaches the no-payload 400 branch — pre-rewrite they used state.last_results = [] which would AttributeError after Plan 88-03."
  - "Test for getter hardening (`test_getters_return_none_on_poisoned_payload`) placed in test_export_state_selection.py rather than the cross-user test file — keeps the cross-user file focused on the leak/isolation contract."

requirements-completed: [STATE-03, STATE-04, STATE-05, STATE-06]

# Metrics
duration: 11min
completed: 2026-05-13
---

# Phase 88 Plan 02: export_state Rewrite + Test Modernization Summary

**web/export_state.py rewritten to route through the Phase 87 safe_storage chokepoint (no _TEST_BACKEND, no _backend helper); getters and update_* funcs hardened with isinstance(payload, dict) guards (Refinement 4 + D-11) and copy-on-update (D-12); parallels_source_text reader-side fallback at 3 web/api.py handlers deleted (D-14); 4 test files rewritten to monkeypatch web.safe_storage.app with SimpleNamespace stubs (Refinement 6); strengthened D-15 source_text leak test exercises positive-export path with bait + valid results (Refinement 2); web/export_state.py allowlist entry deleted.**

## Performance

- **Duration:** ~11 min (executor wall-clock; first task commit 18:57:20, last 19:08:30 local; includes 3:31 full-suite pytest run)
- **Started:** 2026-05-13T15:54:00Z (worktree spawn)
- **Completed:** 2026-05-13T16:08:30Z
- **Tasks:** 7 (6 file-modification tasks + 1 plan-boundary verification gate)
- **Files modified:** 7 source files (1 prod core + 1 prod handler + 4 test files + 1 allowlist YAML); 460+ insertions / 411+ deletions net (refactor with rewrites and dead-code drops)

## Accomplishments

- **web/export_state.py rewrite (Task 1):** Full module body replaced (118 deletions, 85 insertions). `_TEST_BACKEND` module-level variable deleted. `_backend()` helper function deleted. 7 setter/updater/clearer funcs now call `safe_user_get/safe_user_set/safe_user_pop` directly. `update_search_export_results`, `update_search_export_selection`, `update_parallels_export_filtered` add `isinstance(payload, dict)` guard (D-11) and adopt copy-on-update (D-12, `payload = dict(payload)`). `get_search_export()` and `get_parallels_export()` hardened with `isinstance` guard returning `None` on non-dict storage values (Refinement 4 — Codex MEDIUM). ABI preserved: setter/updater/clearer return `None`; getters return dict-or-None invariant.
- **web/api.py reader-side fallback deletion (Task 2):** 3 blocks at lines 1921-1931 (export_parallels_excel), 1955-1964 (export_parallels_word), 2049-2065 (export_parallels_json) cleaned. `safe_user_get('parallels_source_text', ...)` reads and `storage_source_text` variable deleted; `from web.safe_storage import safe_user_get` imports removed from each handler. `source_text = meta.get('source_text') or ''` is now the sole source per D-14.
- **tests/test_export_cross_user_isolation.py rewrite (Task 3):** All 3 pre-rewrite tests ported to monkeypatch `web.safe_storage.app` with SimpleNamespace stub (Refinement 6). New strengthened D-15 leak test `test_parallels_source_text_cannot_leak_via_deleted_fallback` exercises POSITIVE export path with `'alpha-leak-bait'` in legacy key + valid parallels results — handler reaches the code branch that USED to read the fallback (Refinement 2 — Codex MEDIUM). 4 tests pass.
- **tests/test_export_state_selection.py rewrite (Task 4):** `_StateProxy` class deleted entirely (D-02). 8 original selection-filtering / reset tests ported to call `export_state.update_search_export_selection(...)` and `export_state.clear_search_export()` directly. +1 new test_getters_return_none_on_poisoned_payload verifies Refinement 4 isinstance guard. 9 tests pass.
- **tests/test_api_export_json.py + tests/test_api_legacy_unchanged.py rewrite (Task 5):** Same SimpleNamespace stub pattern. `populated_search_state` / `populated_parallels_state` fixtures call `export_state.set_search_export` / `set_parallels_export` to round-trip the payload through `safe_user_set` (proves the helper works end-to-end). `empty_search_state` / `empty_parallels_state` patch storage to `{}` so the handler reaches the 400 path. 5 + 4 = 9 tests pass.
- **Allowlist deletion (Task 6):** Phase 87 self-eliminating entry for `web/export_state.py` removed (20 deletions). Now that `_backend()` is gone, `expected_count=1` would have failed `test_allowlist_counts_exact` — deletion is the right resolution. Allowlist now has 3 entries (auth_state, main, supabase_client — all Phase 90/91 deletion-scoped). All 6 lint scanner tests pass.
- **Plan-boundary green (Task 7):** Full pytest 1881 passed / 21 skipped (vs Phase 88-01 close baseline 1880 passed / 20 skipped; net +1 passed from new D-15 test, +1 skipped from a Windows-platform skip elsewhere). Ruff clean (`python -m ruff check .` exits 0). check_docs clean (with `PYTHONIOENCODING=utf-8`). All cross-cutting greps scoped to `web tests` return 0 matches for `_TEST_BACKEND`, `_StateProxy`, `export_state._backend`.

## Task Commits

Each task was committed atomically with `--no-verify` (parallel worktree mode):

1. **Task 1: web/export_state.py rewrite (safe_storage chokepoint + hardening)** — `5a1eed9d` (refactor)
2. **Task 2: parallels_source_text reader-side fallback deletion (D-14)** — `79fc278e` (refactor)
3. **Task 3: tests/test_export_cross_user_isolation.py rewrite + strengthened D-15** — `7207adf9` (test)
4. **Task 4: tests/test_export_state_selection.py rewrite (D-02; SimpleNamespace stub)** — `c2a37147` (test)
5. **Task 5: tests/test_api_export_json.py + tests/test_api_legacy_unchanged.py rewrite** — `fa06a278` (test)
6. **Task 6: web/export_state.py allowlist entry deletion** — `2be59ce4` (chore)
7. **Task 7: plan-boundary green sweep + docstring scrub** — `70abcb1c` (docs)

**Plan metadata:** committed via final docs commit (this SUMMARY.md).

## Files Created/Modified

- `web/export_state.py` — full rewrite. Module-level `_TEST_BACKEND` and `_backend()` deleted. Imports replaced (`from nicegui import app` → `from web.safe_storage import safe_user_get, safe_user_set, safe_user_pop`). 7 setter/updater/clearer funcs route directly through safe_storage helpers. `update_*` funcs gain `isinstance(payload, dict)` guard + copy-on-update. `get_*` funcs gain `isinstance` guard on return per Refinement 4. ABI unchanged.
- `web/api.py` — 3 reader-site cleanups in the parallels export handlers (excel/word/json). Each handler's local `from web.safe_storage import safe_user_get` import deleted; the `if not source_text: source_text = safe_user_get('parallels_source_text', '') or ''` / `storage_source_text = safe_user_get(...)` legacy fallback lines deleted. Updated comments cite Phase 88 D-14 closure (rephrased to avoid the literal key name for grep gate cleanliness while preserving audit context).
- `tests/test_export_cross_user_isolation.py` — full rewrite. Module docstring updated to document the SEQUENTIAL-simulation caveat (D-03) and the Phase 88 monkeypatch pattern. `_make_stub(initial_storage)` factory returns instance-isolated SimpleNamespace tree per Refinement 6. 4 tests: 2 search-isolation (filename + empty-session-400) + 1 parallels-isolation + 1 strengthened D-15 positive-path leak test (Refinement 2).
- `tests/test_export_state_selection.py` — full rewrite. `_StateProxy` wrapper class deleted entirely. New `session_with_5_results` fixture populates per-session payload via `export_state.set_search_export(...)`. Tests drive selection by calling `export_state.update_search_export_selection(...)` directly. 9 tests: 7 selection-filtering + 1 reset-clears-payload + 1 new getter-hardening (Refinement 4).
- `tests/test_api_export_json.py` — full rewrite. Same SimpleNamespace stub pattern. Empty fixtures patch storage to `{}`; populated fixtures call the helper to round-trip the payload. 5 tests preserved.
- `tests/test_api_legacy_unchanged.py` — rewrite of the export-touching test. The 3 non-export legacy-route tests are unchanged (no fixture changes needed). 4 tests total.
- `.planning/phase87_storage_allowlist.yaml` — web/export_state.py entry deleted (20 lines including patterns + justification block). 3 entries remain (auth_state, main, supabase_client).

## Decisions Made

- **Refinement 1 strict-zero-match interpretation:** Acceptance criteria for Tasks 2, 4, 7 say `grep ... returns 0 matches` for tokens like `parallels_source_text`, `_TEST_BACKEND`, `_StateProxy`. The plan text BEFORE/AFTER examples retained these tokens in updated comments (e.g., "Legacy app.storage.user['parallels_source_text'] fallback removed"). To satisfy the strict gate without losing the audit trail, comments were rephrased to "legacy app.storage.user key fallback" or "pre-Phase-88 test-backend shim." Audit context preserved; literal token removed.
- **Empty-storage fixtures use SimpleNamespace stub instead of state.X = []:** Pre-rewrite `empty_search_state` / `empty_parallels_state` set `state.last_results = []` which would AttributeError after Plan 88-03 deletes those fields. Rewrote to `monkeypatch.setattr('web.safe_storage.app', _make_stub({}))` — the handler now reaches the 400 path because `get_search_export()` returns `None` (storage has no `export_search_payload` key). This is forward-compatible with Plan 88-03.
- **Strengthened D-15 placement:** Per Refinement 2, the D-15 leak test must exercise a POSITIVE export path (results exist, handler reaches the code branch that USED to read the fallback). Placed in `tests/test_export_cross_user_isolation.py` alongside the existing 400-path test (kept both for defense-in-depth) so the test file holds the full leak-isolation story.
- **Getter-hardening test placement:** Refinement 4 acceptance criterion allows either a bare Python one-liner OR a test function. Chose to add `test_getters_return_none_on_poisoned_payload` to `tests/test_export_state_selection.py` (not the cross-user file) — keeps the cross-user file focused on the leak/isolation contract.
- **Plan-text `meta['source_text']` envelope assertion shape:** Plan-text example referenced `body.get('search_context', {}).get('source_text')`. Reality (per `shared/search_serializer.serialize_parallels_payload`) is a top-level `source_text` key on the envelope. Adjusted the D-15 assertion to `body.get('source_text', '') == ''` accordingly. The plan instructed: "executor should read the actual response shape during execution and adjust the second assertion to match. The first assertion (alpha-leak-bait not in body_bytes) is the load-bearing one — it works regardless of envelope structure." Both assertions retained.

## Deviations from Plan

**Total deviations:** 2 minor verification-gate cleanups; NO scope creep.

### Auto-fixed Issues

**1. [Rule 3 - Verification gate] Strict-zero-match grep for `parallels_source_text` required rephrasing 2 comments**
- **Found during:** Task 2 verification
- **Issue:** Plan text instructed to write the new comment as `# Legacy app.storage.user['parallels_source_text'] fallback removed;` but the acceptance criterion says `grep -n "parallels_source_text" web/api.py returns 0 matches`. The two readings conflict — strict reading wins (otherwise the verification gate fires).
- **Fix:** Rephrased the 2 new comments to `# Legacy app.storage.user key fallback removed` — same audit context, no literal token. The original comment in the threat model commentary at api.py:~1846-1848 is OUT OF SCOPE for this plan (Plan 88-03 D-16 handles it).
- **Files modified:** web/api.py (2 comment lines)
- **Verification:** `python -c "src=open('web/api.py').read(); assert 'parallels_source_text' not in src"` exits 0.
- **Committed in:** `79fc278e` (Task 2, after intermediate edits)

**2. [Rule 3 - Verification gate] `_TEST_BACKEND` and `_StateProxy` historical mentions in docstrings required rephrasing**
- **Found during:** Task 4 verification (`_StateProxy` count > 0) and Task 7 verification (`_TEST_BACKEND` count > 0 in `web/export_state.py` docstring)
- **Issue:** Plan text writes the new docstrings using the literal historical names (`_StateProxy`, `_TEST_BACKEND`). Acceptance criteria say `grep -n "_StateProxy" tests returns 0 matches` and `rg "_TEST_BACKEND" web tests returns 0 matches`. Strict reading wins.
- **Fix:** Rephrased the 3 mentions: "pre-Phase-88 ``_StateProxy`` wrapper" → "pre-Phase-88 state-proxy wrapper"; "``web.export_state._TEST_BACKEND`` shim" → "in-process test backend shim"; "removed the ``_TEST_BACKEND`` shim and the ``_backend()`` helper" → "removed the pre-Phase-88 test-backend shim and its production-code selector helper". Audit context preserved.
- **Files modified:** tests/test_export_state_selection.py (2 mentions), web/export_state.py docstring (1 mention)
- **Verification:** Final scoped grep over `web tests` returns 0 for all 3 patterns.
- **Committed in:** `c2a37147` (Task 4) + `70abcb1c` (Task 7)

**Impact on plan:** Both deviations are docstring/comment cleanups required to pass the strict verification gates. Zero functional change, zero scope creep, zero test churn. The intent of the plan text (document the historical removal) is preserved via paraphrase.

## Issues Encountered

- **Plan-text envelope shape vs. reality:** D-15 plan-text suggested asserting `body.get('search_context', {}).get('source_text')` but the actual envelope (per `shared/search_serializer.serialize_parallels_payload`) puts `source_text` at the top level. Plan acknowledged this with "Executor: read the actual response shape during execution and adjust the second assertion to match." Adjusted to `body.get('source_text', '') == ''`. Documented above under Decisions Made.
- **Pytest run shows 1881 passed / 21 skipped** vs Phase 88-01 baseline 1880 passed / 20 skipped. Net change: +1 passed (new test_parallels_source_text_cannot_leak_via_deleted_fallback) + 1 skipped (Windows-platform skip elsewhere in the tree — not introduced by this plan, observed during the run). Plan-text expected `at least 1880 passed` — satisfied with margin. The +1 getter-hardening test in test_export_state_selection.py is also new; total new tests added by this plan = 2 (D-15 strengthened + getter hardening). Counts net out because some pre-rewrite tests had identical names but moved to the new fixture mechanism (no test deletion).
- **check_docs.py UnicodeEncodeError on Windows console:** Initial run failed with the same emoji-output issue as Plan 88-01. Ran with `PYTHONIOENCODING=utf-8` and confirmed clean. Pre-existing tooling issue, not Plan-88-02-introduced.

## User Setup Required

None — refactor + test rewrite only. No environment variables, no dashboard configuration. Zero user-visible behavior change (per success_criteria).

## Next Phase Readiness

Plan 88-03 (AppState deletion + static enforcement) is unblocked. Specifically:

- **Plan 88-03 can delete the 10 AppState fields safely** — no test in the repo writes to `state.X` for the 10 export-related fields (verified via scoped grep at Task 7).
- **Plan 88-03 will add the runtime attr-absence test** (`tests/test_no_appstate_export_fields.py`) — current empty-state fixtures use SimpleNamespace stubs, so AppState shape changes do not affect them.
- **Plan 88-03 will add the static AST/grep enforcement test** (`tests/test_no_deleted_state_references.py`) — Refinement 5 suggests broader alias coverage (`from web.state import state as s`, `import web.state as web_state`, `setattr(AppState(), ...)`). Plan 88-03 owns that.
- **Plan 88-03 D-16 comment cleanup** for the 2026-05-12 cross-user-fix comment at `web/api.py:1846-1848` is deferred (intentionally not touched in Plan 88-02 Task 2).
- **No blockers, no carry-over.** All cross-cutting greps pass; AppState class shape still 10 fields per scoped grep; lint scanner all green with 3 allowlist entries.

## Self-Check: PASSED

Verifying claims before returning:

**Created files:**
- `.planning/phases/88-state-separation-by-deletion/88-02-export-state-rewrite-SUMMARY.md`: FOUND (this file)

**Modified files (per task commits):**
- `web/export_state.py`: FOUND (verified in `5a1eed9d` + `70abcb1c`)
- `web/api.py`: FOUND (verified in `79fc278e`)
- `tests/test_export_cross_user_isolation.py`: FOUND (verified in `7207adf9`)
- `tests/test_export_state_selection.py`: FOUND (verified in `c2a37147`)
- `tests/test_api_export_json.py`: FOUND (verified in `fa06a278`)
- `tests/test_api_legacy_unchanged.py`: FOUND (verified in `fa06a278`)
- `.planning/phase87_storage_allowlist.yaml`: FOUND (verified in `2be59ce4`)

**Commit hashes verified present in git log:**
- `5a1eed9d`: FOUND (Task 1)
- `79fc278e`: FOUND (Task 2)
- `7207adf9`: FOUND (Task 3)
- `c2a37147`: FOUND (Task 4)
- `fa06a278`: FOUND (Task 5)
- `2be59ce4`: FOUND (Task 6)
- `70abcb1c`: FOUND (Task 7)

**Acceptance criteria verified at end of Task 7:**
- pytest: 1881 passed, 21 skipped (above 1880 Phase 88-01 baseline)
- `python -m ruff check .`: All checks passed!
- `PYTHONIOENCODING=utf-8 python scripts/check_docs.py`: All checks passed!
- scoped `rg "_TEST_BACKEND" web tests`: 0 matches
- scoped `rg "_StateProxy" tests`: 0 matches
- scoped `rg "export_state\._backend" web tests`: 0 matches
- All 6 lint scanner tests (`tests/test_no_raw_storage_access.py`): GREEN
- AppState class shape: 10 fields still present in `web/state.py` (Plan 88-03 deletes them)
- Allowlist count: 3 file entries (auth_state, main, supabase_client) — web/export_state.py entry deleted as planned
- 4 rewritten test files: 4 + 9 + 5 + 4 = 22 tests all GREEN
- Refinement 4 getter isinstance-guard: verified via `test_getters_return_none_on_poisoned_payload` + bare Python one-liner from Task 1 acceptance criterion
- Refinement 6 SimpleNamespace stubs: all 4 rewritten files use `_make_stub` factory; 0 `class _StubApp` matches across the 4
- Refinement 2 strengthened D-15: `test_parallels_source_text_cannot_leak_via_deleted_fallback` asserts bait string absent from positive-export response

---
*Phase: 88-state-separation-by-deletion*
*Plan: 02 (export_state rewrite + test modernization + _TEST_BACKEND removal)*
*Completed: 2026-05-13*
