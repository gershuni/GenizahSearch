---
phase: 88
reviewers: [gemini, codex]
reviewed_at: 2026-05-13T14:44:12Z
plans_reviewed:
  - 88-01-writer-migration-PLAN.md
  - 88-02-export-state-rewrite-PLAN.md
  - 88-03-appstate-deletion-and-enforcement-PLAN.md
skipped: [claude]
skip_reason: "Self-review excluded — orchestrator is Claude Code"
---

# Cross-AI Plan Review — Phase 88: State Separation by Deletion

## Gemini Review

This is a high-quality, professional implementation plan that rigorously adheres to the architectural standards established in Phase 87. The strategy of **"migration by deletion"** is executed with surgical precision, utilizing multi-layered validation (runtime + static AST) to permanently close the cross-user state leak attack surface.

### Summary
Phase 88 successfully decouples the export state from the `AppState` singleton. **Plan 88-01** (Writer Migration) safely transitions all 13 writer sites to local variables, eliminating mirror-writes to the singleton without disrupting downstream consumers. **Plan 88-02** (export\_state Rewrite) hardens the session-storage interface by adopting the `safe_storage` chokepoint, removing the `_TEST_BACKEND` production shim, and cleaning up legacy fallbacks. Finally, **Plan 88-03** (Deletion & Enforcement) physically removes the dead code from `AppState` and installs permanent CI guards to prevent regressions.

### Strengths
- **Data-Loss Prevention:** The decision (D-04) to migrate writers to locals *before* deleting class fields is a critical catch from the Codex round-5 review. It ensures that keyword arguments sourced from `state.*` (like `gap` or `filters`) aren't lost or defaulted during the transition.
- **Robust Enforcement:** The dual-guard approach (D-06 runtime `hasattr` + D-07 static AST scanner) is excellent. The AST scanner, in particular, prevents "stealth" regressions where dynamic attributes could be re-created via `state.last_results = [...]` despite the field being deleted from `__init__`.
- **Chokepoint Discipline:** Deleting `_backend()` and `_TEST_BACKEND` (D-09) and routing exclusively through `web.safe_storage` fulfills the Phase 87 mandate of a single, validated entry point for all per-user state.
- **Update Hardening:** The inclusion of `isinstance(payload, dict)` guards (D-11) and copy-on-update patterns (D-12) provides necessary defense against poisoned-shape storage and shared-reference races.
- **Test Modernization:** Moving from the `_TEST_BACKEND` shim to direct `web.safe_storage.app` monkeypatching (D-01) aligns the export tests with the now-standard Phase 87 test patterns.

### Concerns
- **AST Scanner False-Negatives (LOW):** The static scanner (D-07) relies on `STATE_BINDING_NAMES = {'state', 'app_state'}`. While sufficient for the current codebase, it would miss a regression if a contributor imported the singleton as an alias (e.g., `from web.state import state as s; s.last_results = []`).
- **Sequential Simulation Caveat (MEDIUM):** As noted in D-03, the isolation tests are sequential. While this proves logical separation, it does not provide true concurrent stress testing of the `safe_storage` locks or `AssertionError` handling. This is correctly deferred to the Phase 92 manual smoke-test.
- **Shallow Copy in Update (LOW):** `payload = dict(payload)` (D-12) is a shallow copy. While it protects the dictionary structure itself, the nested lists (like `results`) are still shared references. However, since the `update_*` functions replace the entire list reference (`payload['results'] = results`), this is functionally safe for the current use cases.

### Suggestions
- **AST Scanner Precision:** Consider adding a check to the AST scanner that also flags any `ImportFrom` where `module == 'web.state'` and an alias (`asname`) is used that isn't in the allowed binding list, or simply flag all `Attribute` nodes where the attribute name matches `DELETED_FIELDS` regardless of the value name, and then filter out known false positives (like `self.X` inside unrelated classes). *Status: Deferred to SWEEP-01 if needed.*
- **Docstring Hygiene:** In Plan 88-01 Task 1, ensure the comment trim for Site 1 explicitly mentions "Phase 88" to aid future `git blame` investigations.

### Risk Assessment: **LOW**
The risk is low due to the incremental, wave-based approach and the fact that `api.py` readers were already migrated to the session-payload path in v7.11.1. The plan-boundary green discipline (D-05) is highly achievable because the `AppState` fields remain physically present until the very last stage, allowing existing tests and mirror-writes to coexist until the enforcement tests are ready.

**The plan is approved for execution.**

---

## Codex Review

### Summary

**Plan 88-01** is directionally strong and correctly ordered: migrating writers to locals before deleting fields is the right move and directly addresses the data-loss bug Codex caught. The main risk is that the plan is very line-number/scripted and contains a few inconsistencies around which locals are actually threaded into export calls, plus ambiguity around `parallels_source_text`.

**Plan 88-02** is the most important and riskiest plan. The `safe_storage` rewrite, `_TEST_BACKEND` deletion, ABI preservation, and test-injection target are well designed. The biggest issues are verification/test gaps: the proposed `parallels_source_text` leak test is weak, and broad `grep -rn "_TEST_BACKEND" .` gates will likely fail because planning/docs/history files intentionally mention `_TEST_BACKEND`.

**Plan 88-03** closes the phase cleanly with deletion plus runtime/static guards. The static scanner is the right idea, but its alias coverage is too narrow for a permanent regression guard. Also, the docs task reintroduces historical `_TEST_BACKEND` mentions while later verification requires zero repo-wide matches, which is contradictory.

**Overall:** The plan sequence is sound and the phase is implementable, but I would tighten verification scopes, strengthen the `parallels_source_text` regression test, and improve the AST scanner before execution. Overall risk: **MEDIUM**.

### Strengths

- The ordering is correct: `88-01` migrates writers, `88-02` rewrites storage/tests, `88-03` deletes fields and installs guards.
- Patching `web.safe_storage.app` is the correct monkeypatch target because `export_state` imports helper functions whose globals resolve `web.safe_storage.app`.
- Setter ABI preservation is explicitly handled: setters/updaters remain `None`-returning despite `safe_user_set()` returning `bool`.
- `update_*` hardening with `isinstance(payload, dict)` and copy-on-update is a useful defensive improvement with minimal behavioral risk.
- The dual guard in `88-03` is the right shape: runtime attr absence plus static AST enforcement.
- The plan-boundary green discipline is good and matches the risk profile of a multi-step deletion refactor.
- Removing `web/export_state.py` from the Phase 87 raw-storage allowlist is the right closure point.

### Concerns

- **HIGH:** Repo-wide grep gates are not achievable as written. `grep -rn "_TEST_BACKEND" .` will likely match `.planning`, `_tmp`, summaries, the supplied context, or the new `CLAUDE.md` historical entry. Same issue for `_StateProxy`, `state.last_results`, and `_backend`.
- **MEDIUM:** The proposed D-15 `parallels_source_text` leak test mostly exercises a `400 no results` path. If the fallback were reintroduced but still behind a no-results early return, the test could pass without proving the fallback is dead.
- **MEDIUM:** `parallels.py` source-text fold-in looks incomplete around bootstrap/snapshot paths. Any `set_parallels_export(..., meta=None)` path that can later export results will produce missing `source_text` after the API fallback is removed.
- **MEDIUM:** The static scanner only catches `state.X` and `app_state.X`. It misses aliases like `from web.state import state as s; s.last_results = ...`, module-qualified access, `setattr(AppState(), ...)`, and `web_state.state.last_results`.
- **MEDIUM:** `get_search_export()` and `get_parallels_export()` can still return poisoned non-dict payloads. The update functions are hardened, but readers may still crash if storage contains a non-dict.
- **LOW:** Plan 88-01 sometimes assigns locals but still passes original expressions into `set_search_export`. That may be behaviorally fine, but it weakens the "thread locals through" invariant and can confuse reviewers.
- **LOW:** The `_StubApp` nested `storage` class creates shared class-level state. Sequential tests likely pass, but an instance-level `SimpleNamespace(storage=SimpleNamespace(user=...))` is cleaner and safer.
- **LOW:** Several verification commands assume Unix tools (`grep`, `tail`, `test -f`) despite the project environment being Windows/PowerShell. Prefer `rg` or Python snippets.

### Suggestions

- Scope broad greps to executable surfaces:
  - Use `rg "_TEST_BACKEND" web tests`
  - Exclude `.planning/**`, `_tmp/**`, summaries, generated review prompts, and historical docs.
- Strengthen D-15 with a positive export case:
  - Give User B a valid parallels payload with results and `meta={}`.
  - Put `parallels_source_text: "alpha-leak-bait"` in the active storage.
  - Assert the JSON/export response does not contain `"alpha-leak-bait"` and source text is empty.
- Audit every `set_parallels_export(` call after 88-01:
  - If results are non-empty, `meta` should include `source_text` or there should be an explicit comment why not.
- Harden getters too:
  ```python
  payload = safe_user_get(_SEARCH_KEY, None)
  return payload if isinstance(payload, dict) else None
  ```
- Improve the AST scanner to track simple aliases:
  - `from web.state import state as s`
  - `from web.api import state as api_state`
  - `import web.state as web_state` followed by `web_state.state.last_results`
  - `setattr(AppState(), "last_results", ...)`
- Align D-06 with the stated requirement:
  - Keep `assert not hasattr(...)`, but also add `with pytest.raises(AttributeError): getattr(instance, field)`.
- Replace stub app classes with instance-specific objects:
  ```python
  from types import SimpleNamespace
  return SimpleNamespace(storage=SimpleNamespace(user=initial_storage))
  ```
- Avoid brittle pass-count thresholds as hard requirements. Keep "pytest exits 0" plus explicit new-test-file checks; total counts can drift for unrelated reasons.

### Risk Assessment

**Overall risk: MEDIUM.**

The architecture and ordering are sound, and the most important dependency chain is correct: writers move first, tests stop using singleton fields before deletion, then AppState fields are removed and guarded. The remaining risks are mostly execution-quality risks: contradictory verification commands, a weak source-text regression test, and scanner false negatives. None of these invalidate the phase, but they should be corrected before implementation to avoid false red gates or a future regression slipping through.

---

## Consensus Summary

Both reviewers agree the plan ordering (writers → export_state → fields) is correct and that the Codex-round-5 catch on `set_search_export(...)` kwargs reading `state.X` two lines below their assignments has been properly mitigated. Both reviewers approve the dual-guard enforcement (D-06 runtime + D-07 static AST) and the `safe_storage` chokepoint discipline.

Disagreement is on risk level: Gemini rates LOW (architectural integrity); Codex rates MEDIUM (execution-quality risks).

### Agreed Strengths

- Plan ordering is correct (writers-first prevents data-loss window) — both reviewers
- Dual enforcement (runtime D-06 + static D-07) is the right shape — both reviewers
- Monkeypatching `web.safe_storage.app` is the correct target — both reviewers
- Setter ABI preservation (D-10 returns `None`) is the right call — both reviewers
- `update_*` hardening (D-11 isinstance + D-12 copy-on-update) is defensive and minimal-risk — both reviewers
- Removing `web/export_state.py` from Phase 87 allowlist is the right closure point — both reviewers

### Agreed Concerns

- **AST scanner alias coverage too narrow (LOW–MEDIUM):** Both reviewers flag that `STATE_BINDING_NAMES = {'state', 'app_state'}` misses `from web.state import state as s`, `import web.state as web_state`, `setattr(AppState(), …)`. Gemini calls it LOW (sufficient for current codebase); Codex calls it MEDIUM (won't survive long enough as a permanent CI guard).
- **Sequential simulation is not true concurrent coverage (MEDIUM):** Both reviewers note this is correctly documented and deferred to Phase 92 SWEEP-05 production smoke test.

### Codex-Only Concerns (Not Surfaced by Gemini — Highest Priority for Replanning)

- **HIGH — Repo-wide grep gates produce false positives:** `grep -rn "_TEST_BACKEND" .` will match `.planning/`, `_tmp/`, history docs, and the CLAUDE.md "Recently Changed" entry. Scope all greps to `web/` and `tests/` (use `rg "PATTERN" web tests`).
- **MEDIUM — D-15 leak test only exercises a 400 no-results path:** A reintroduced fallback hidden behind a no-results early return would pass the test. Strengthen with a positive-export path: User B has valid parallels results + `meta={}`, the `parallels_source_text` storage key has "alpha-leak-bait", and the export response must NOT contain that bait string.
- **MEDIUM — `set_parallels_export(..., meta=None)` paths may produce missing source_text:** Audit every `set_parallels_export(` call after Plan 88-01; ensure non-empty results paths include `source_text` in meta or have explicit no-source-text comment.
- **MEDIUM — Hardened only `update_*`, not getters:** `get_search_export()` / `get_parallels_export()` can still return poisoned non-dict payloads. Add `isinstance` guard on getters too: `return payload if isinstance(payload, dict) else None`.
- **LOW — `_StubApp` nested class shares class-level state:** Sequential tests work, but `SimpleNamespace(storage=SimpleNamespace(user=initial_storage))` is cleaner and instance-isolated.
- **LOW — Unix-tool verification commands on a Windows project:** `grep`, `tail`, `test -f` may behave differently or absent in PowerShell. Switch to `rg` + Python one-liners, or accept Bash dependency explicitly.

### Divergent Views

- **Overall risk:** Gemini LOW vs Codex MEDIUM. The divergence is whether execution-quality risks (false-positive greps, weak leak test, scanner blind spots) materially threaten the phase. Codex's view is that these are addressable pre-execution; both agree none invalidate the phase.

---

## Recommended Pre-Execution Refinements (Priority-Ordered)

1. **HIGH — Scope all greps to `web/` and `tests/`** in Plan 88-02 Task 7 + Plan 88-03 Task 1/3/4 acceptance criteria. Use `rg "PATTERN" web tests` instead of `grep -rn PATTERN .`. (Codex finding; addresses false-positive contradictions with CLAUDE.md historical entries.)
2. **MEDIUM — Strengthen D-15 source_text leak test** in Plan 88-02 Task 3: positive-export case with bait string in `app.storage.user['parallels_source_text']` for User A, valid results+empty-meta for User B, assert User B's export does NOT contain User A's bait.
3. **MEDIUM — Audit `set_parallels_export(..., meta=None)` paths** in Plan 88-01 Task 3 acceptance criteria: every non-empty-results call must thread `source_text` into meta or carry an explicit comment.
4. **MEDIUM — Harden getters with `isinstance(payload, dict)` guard** in Plan 88-02 Task 1: extend D-11 from `update_*` only to `get_*` too.
5. **MEDIUM — Extend AST scanner alias coverage** in Plan 88-03 Task 3: walk `ImportFrom` and `Import` to track `state as X` and `web.state as Y` aliases; also catch `setattr(state, '<field>', …)` Call nodes.
6. **LOW — Switch `_StubApp` to `SimpleNamespace`** in Plan 88-02 Tasks 3/4/5 for instance-isolated test stubs.
7. **LOW — Verify Bash tool availability or switch to `rg`/Python** in Plan 88-01/02/03 verification commands (project is Windows/PowerShell-native).

To incorporate this feedback into the plans, run:

```
/gsd-plan-phase 88 --reviews
```
