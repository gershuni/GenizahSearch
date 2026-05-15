---
phase: 91-atomic-auth-state-writes
plan: 02
subsystem: tests/retention-guards
tags:
  - auth
  - safe_storage
  - retention-guard
  - ast-scanner
  - behavioral-test
  - phase-91
  - authw-06
dependency_graph:
  requires:
    - phase-91-01-auth-state-migration
    - phase-87-safe_storage-chokepoint
    - phase-88-d07-ast-scanner-pattern
    - phase-90-d15-ast-scanner-pattern
  provides:
    - persist_value-retention-guard
    - strict-args-ast-scanner-pattern
    - ast+behavioral-defense-in-depth-template
  affects:
    - tests/test_persist_value_uses_safe_storage.py
tech_stack:
  added:
    - none (pure new test file; no new runtime dependencies)
  patterns:
    - ast.parse+ast.walk-function-body-scanner (Phase 88 D-07 / Phase 90 D-15)
    - _find_function_def-helper (sync + async FunctionDef tolerant)
    - strict-args-name-ref-check (Revision SHOULD-6 — new pattern; verifies safe_user_set args[0]/args[1] are Name refs to function parameters)
    - monkeypatch.setattr-string-form-storage-stub (Phase 87 B3)
    - per-case-isolated-storage-dict (Phase 88 D-01)
    - ast+behavioral-defense-in-depth (Revision MUST-5 — shape-only AST cannot detect ignored-flag-read regressions; behavioral test exercises live function)
    - intentionally-unhandled-import-aliasing (documented contract per Gemini concern)
key_files:
  created:
    - tests/test_persist_value_uses_safe_storage.py
  modified: []
decisions:
  - D-09 installed AST retention guard for filter_panel.py:220:persist_value (originally landed in commit cca23db3, 2026-05-12 Codex 3rd-pass CRITICAL fix)
  - Revision MUST-5 added T-Beh behavioral test monkeypatching web.safe_storage.app with session_persistence_enabled False/True to close the AST shape-only gap
  - Revision SHOULD-6 strengthened T-3 to STRICT args check verifying safe_user_set(<first_param>, <second_param>) Name references
  - NEW-M3 (round-2 cross-AI review) — strict single-test-file atomic CI guard. No production code touched. No documentation touched. Closeout docs (STATE.md / ROADMAP.md / CLAUDE.md / OPEN_ISSUES.md) moved to Plan 91-03.
  - Import-aliasing intentionally unhandled (documented in module docstring per Gemini concern). A future refactor using `from web.safe_storage import safe_user_set as safe_set` MUST update this test.
metrics:
  duration: ~15min (worktree wall-time)
  completed: 2026-05-15
  tasks_completed: 1
  files_created: 1
  files_modified: 0
  tests_added: 6
---

# Phase 91 Plan 02: AUTHW-06 persist_value Retention Guard Summary

**One-liner:** Install `tests/test_persist_value_uses_safe_storage.py` — a 6-test AST + behavioral retention guard preventing future regressions of the `web/components/filter_panel.py:220:persist_value` safe-wrap (originally landed in commit `cca23db3`, 2026-05-12 Codex 3rd-pass CRITICAL fix); strict single-test-file atomic CI-guard commit per NEW-M3 scope discipline.

## Summary

Plan 91-02 is a STRICT single-test-file commit per Phase 89 D-09 / Phase 90 D-13 atomic-CI-guard discipline. No production code is touched. No documentation is touched — round-2 cross-AI review NEW-M3 caught a frontmatter/body mismatch in the prior plan version (Task 2 wanted to update STATE.md / ROADMAP.md / CLAUDE.md / OPEN_ISSUES.md while `files_modified` listed only the test file); per user-selected Option (b), those docs updates were moved to a new Plan 91-03.

The retention guard combines:
- **3 AST shape assertions** scanning `web/components/filter_panel.py:persist_value` for: (T-1) `from web.safe_storage import safe_user_get, safe_user_set` imports; (T-2) `safe_user_get('session_persistence_enabled', ...)` flag-read; (T-3) `safe_user_set(<first_param>, <second_param>)` STRICT args check verifying the call passes the function's own parameter names through (Revision SHOULD-6).
- **1 behavioral test** (Revision MUST-5) that monkeypatches `web.safe_storage.app` with a `SimpleNamespace`-stubbed storage backend and exercises the LIVE `persist_value`. With `session_persistence_enabled=False`, the write must be suppressed; with `=True`, the write must happen. This closes the AST shape-only gap: a regression that ignored the flag-read return value would pass T-1/T-2/T-3 but fail T-Beh.
- **2 seed-trap snippet sanity tests** parsed inline via `ast.parse()`: a passing snippet that exercises all 3 AST positive checks (including the STRICT args check), and a failing snippet using raw `app.storage.user[k] = v` that trips the raw-subscript negative check.

Phase 87's `tests/test_no_raw_storage_access.py` operates at file scope — if a future contributor moves a raw write OUTSIDE `persist_value`, the file-scope scanner catches it. But Phase 87 wouldn't notice if a contributor REWROTE `persist_value` to un-gate the persistence check, rename the gating constant, or KEEP the AST shape but break the behavior. Plan 91-02 closes those function-local regression paths with defense-in-depth (AST shape + live behavioral verification).

## Tasks Completed

| Task | Description | Commit |
|------|-------------|--------|
| 1 | Install `tests/test_persist_value_uses_safe_storage.py` with 3 AST assertions (T-1/T-2/T-3 STRICT) + 1 behavioral test (T-Beh) + 2 seed-trap snippets. STRICT args check verifies `safe_user_set(<first_param>, <second_param>)` Name refs (Revision SHOULD-6). T-Beh monkeypatches `web.safe_storage.app` with `SimpleNamespace`-stubbed storage and exercises live `persist_value` for both `session_persistence_enabled=False` (suppresses write) and `=True` (allows write) per Revision MUST-5. | 346683f5 |

## Revision Items Applied

### Round 1 (MUST + SHOULD items)
- **MUST-5** (behavioral T-Beh test): added `test_persist_value_respects_session_persistence_flag` monkeypatching `web.safe_storage.app` with per-case isolated storage dicts pre-populated with `session_persistence_enabled: False/True`. Per-case dict instance isolation (Phase 88 D-01).
- **SHOULD-6** (STRICT safe_user_set args check): T-3 now walks the `Call` node's `args` list and verifies `args[0]` is `ast.Name(id=first_param)` AND `args[1]` is `ast.Name(id=second_param)`. New `_get_param_names(fn)` helper returns `tuple(arg.arg for arg in fn.args.args)`. The seed-trap passing test also exercises this STRICT check. Forecloses the Codex-flagged edge case where `safe_user_set('decoy', None)` plus a raw write could trick a permissive check.

### Round 2 (NEW items from 91-REVIEWS.md round 2)
- **NEW-M3** (Plan 91-02 frontmatter vs. body mismatch — STRICT SCOPE ENFORCEMENT): user-selected Option (b). Task 2 (closeout docs updates) was DELETED from Plan 91-02 and moved to a new Plan 91-03. Plan 91-02's `files_modified` frontmatter now contains exactly ONE path: `tests/test_persist_value_uses_safe_storage.py`. The verified `git diff --name-only HEAD` after the test-install commit confirms only this single path was touched.

### Items NOT changed
- Wave / depends_on / requirements frontmatter — preserved (`wave: 2`, `depends_on: ["91-01"]`, `requirements: [AUTHW-06]`).
- Test content preserved verbatim from the plan's `<action>` block — only the scope framing changed in round 2.
- AST scanner discipline preserved (Phase 88 D-07 / Phase 90 D-15 lineage); behavioral test ADDS to it rather than replacing.

## Deviations from Plan

### Auto-fixed Issues

None. Plan 91-02 executed exactly as written.

### Auth Gates

None encountered during execution.

### Verification-Command Pedantry

The plan's verification snippet at line 532 of 91-02-PLAN.md uses a single-line `monkeypatch.setattr('web.safe_storage.app'` substring check. Python's `str.count` is whitespace-sensitive, and the implemented test calls span multiple lines (`monkeypatch.setattr(\n    'web.safe_storage.app',\n    ...\n)` per ruff/pep8 line-length style). A regex-based recount (`re.findall(r"monkeypatch\\.setattr\\s*\\(\\s*'web\\.safe_storage\\.app'", src)`) confirms the pattern appears exactly twice (T-Beh False case + T-Beh True case). Not a deviation — the invariant the verification command tried to enforce IS satisfied; only the snippet's regex was too literal about whitespace.

## Verification

### Pytest
```
$ python -m pytest tests/test_persist_value_uses_safe_storage.py -v --tb=short
============================= test session starts =============================
collected 6 items

tests/test_persist_value_uses_safe_storage.py::test_persist_value_imports_safe_storage_helpers PASSED [ 16%]
tests/test_persist_value_uses_safe_storage.py::test_persist_value_reads_persistence_flag PASSED [ 33%]
tests/test_persist_value_uses_safe_storage.py::test_persist_value_writes_via_safe_user_set PASSED [ 50%]
tests/test_persist_value_uses_safe_storage.py::test_persist_value_respects_session_persistence_flag PASSED [ 66%]
tests/test_persist_value_uses_safe_storage.py::test_seed_trap_passing_snippet_passes_all_three_ast_checks PASSED [ 83%]
tests/test_persist_value_uses_safe_storage.py::test_seed_trap_failing_snippet_fails_raw_subscript_check PASSED [100%]

============================== 6 passed in 1.35s ==============================
```

### AST presence checks
- All 6 required test function names present in module
- All 3 helpers present at module scope (`_find_function_def`, `_is_app_storage_user_subscript`, `_get_param_names`)
- 2 `monkeypatch.setattr('web.safe_storage.app', ...)` calls (T-Beh False case + True case)
- 2 occurrences of `_get_param_names(fn)` (T-3 production + seed-trap passing)
- 0 occurrences of `@pytest.fixture` (instance-isolation discipline)
- 0 imports of `re` / `subprocess` (no regex/grep-based scanning)

### NEW-M3 single-test-file scope invariant
```
$ git diff --name-only HEAD~1 HEAD
tests/test_persist_value_uses_safe_storage.py
```
Exactly ONE file changed. No production code touched. No documentation touched. Strict atomic-CI-guard discipline enforced.

### Ruff
```
$ python -m ruff check tests/test_persist_value_uses_safe_storage.py
All checks passed!
```

## Threat Model Status

| Threat ID | Disposition | Verification |
|-----------|-------------|--------------|
| T-91-11 (future contributor un-does safe-wrap) | mitigated | T-1/T-2/T-3 AST assertions catch missing imports / missing flag-read / raw subscript writes |
| T-91-12 (Phase 87 lint scanner has a false-negative gap that lets raw write slip through) | mitigated (defense-in-depth) | T-3 function-scope raw-subscript negative check complements file-scope lint scanner |
| T-91-13 (decoy `safe_user_set('unrelated', None)` + raw write) | mitigated (NEW per Revision SHOULD-6) | T-3 STRICT args check verifies `args[0]` / `args[1]` are `ast.Name` refs to persist_value's own parameter names |
| T-91-14 (AST shape intact but behavior broken — flag-read return value ignored) | mitigated (NEW per Revision MUST-5) | T-Beh behavioral test exercises live `persist_value` with monkeypatched `safe_storage` — a regression that ignored the flag would pass all 3 AST checks but fail T-Beh |
| T-91-15 (false positive blocks legitimate refactor) | accepted | Single-file atomic commit makes test revert cheap if false positive surfaces; seed-trap snippets + module docstring document the canonical shape; import-aliasing intentionally unhandled |
| T-91-16 (raw write bypasses flag-read defense-in-depth) | mitigated | Both halves of cca23db3 (safe-wrap + flag-gate) verified independently by T-3 (raw-subscript negative) + T-Beh (behavioral) |

## Hand-off

Plan 91-02 is the second of 3 plans in Phase 91 (D-10 + NEW-M3 split):

| Wave | Plan | Status | Scope |
|------|------|--------|-------|
| 1 | 91-01 | Complete (commits 656e5a17, 74712a87, af28cc8a, 0c4cda29) | Migration of 12 raw `app.storage.user` accesses + AUTHW-05 resilience tests + Phase 87 allowlist self-elimination (2 → 0) |
| **2** | **91-02** | **Complete (commit 346683f5)** | **AUTHW-06 persist_value retention guard — single-test-file atomic CI guard** |
| 3 | 91-03 | Pending (next in chain) | Closeout docs — STATE.md / ROADMAP.md / CLAUDE.md / OPEN_ISSUES.md flips to "Phase 91 Complete" |

**Next plan in the wave chain:** `91-03-PLAN.md` — closeout docs only, zero new tests, zero production code. The orchestrator owns the central STATE.md / ROADMAP.md update after all worktree agents in this wave complete.

Plan 91-02 does NOT modify `.planning/STATE.md`, `.planning/ROADMAP.md`, `CLAUDE.md`, or `docs/OPEN_ISSUES.md` (NEW-M3 strict scope). The single-test-file invariant is verified by `git diff --name-only HEAD~1 HEAD` returning exactly one path.

## Cross-AI Review Summary

**Round 1 (Gemini + Codex) — integrated as Revisions:**
- MUST-5 (Codex MEDIUM in 91-REVIEWS.md round 1): AST assertions are SHAPE-ONLY; behavioral test required to close the ignored-flag-read regression path. **Applied** — T-Beh installed.
- SHOULD-6 (Codex MEDIUM in 91-REVIEWS.md round 1): T-3 args check should verify Name refs to function parameters, not just any `safe_user_set` call. **Applied** — STRICT args check with `_get_param_names(fn)` helper installed; seed-trap passing test also exercises the STRICT check.
- Gemini concern (import-aliasing): a future refactor doing `from web.safe_storage import safe_user_set as safe_set` would fail the AST test. **Documented** in module docstring as intentional brittleness — the refactor must also update this test; the trade-off is tighter contract enforcement for the literal name `safe_user_set`.

**Round 2 (Codex only; Gemini failed with HTTP 429) — integrated as NEW item:**
- NEW-M3 (frontmatter vs. body mismatch — strict scope discipline violation): Task 2 (closeout docs) MOVED to new Plan 91-03 per user-selected Option (b). Plan 91-02 stays strict single-test-file. **Applied** — `files_modified` now contains exactly one path; `git diff --name-only HEAD~1 HEAD` verifies.

## Self-Check: PASSED

- **Files created:**
  - `tests/test_persist_value_uses_safe_storage.py` → FOUND
- **Files modified:** (none — strict single-test-file invariant per NEW-M3)
- **Commits:**
  - 346683f5 (Task 1 — sole task in Plan 91-02) → FOUND
- **Plan-boundary verification:**
  - Pytest (this file): 6 passed → PASSED
  - Ruff (this file): All checks passed → PASSED
  - AST presence: 6 required tests + 3 helpers + 2 monkeypatch idioms + 2 STRICT param refs → PASSED
  - NEW-M3 single-test-file invariant: `git diff --name-only HEAD~1 HEAD` returns exactly one path → PASSED
  - No `@pytest.fixture` / no `import re` / no `import subprocess` → PASSED
