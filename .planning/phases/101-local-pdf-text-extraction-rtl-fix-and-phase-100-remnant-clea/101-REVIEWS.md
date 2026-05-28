---
phase: 101
reviewers: [gemini, codex]
reviewed_at: 2026-05-28T03:43:28Z
plans_reviewed: [101-01-PLAN.md, 101-02-PLAN.md]
notes: |
  Second review round following plan revision (commit 1c7e2e4b). Both Gemini
  and Codex returned successfully. Claude skipped per workflow self-skip rule
  (this review was invoked from inside Claude Code).
---

# Cross-AI Plan Review — Phase 101 (Round 2)

## Gemini Review

The implementation plans for **Phase 101** are of high technical quality, particularly the adoption of the **S-1 directional-run reversal** algorithm, which correctly handles mixed-script lines (e.g., Hebrew text containing Latin shelfmarks) that blunt reversal would corrupt. The strategy for self-healing existing libraries via an atomic extractor-version bump is robust and well-integrated into the existing recovery machinery.

However, there are two critical flaws in the verification and cleanup logic that risk either failing the build or failing to resolve the target flake.

### 1. Summary
A surgical and well-researched plan that correctly identifies the `sort=True` fallback as the root cause of the RTL bug. The "directional-run" fix is a significant improvement over simple word-order reversal. The self-correction via `_CURRENT_EXTRACTOR_VERSION` is elegant. However, the AST guard for WR-01 is incompatible with the proposed code change, and the D-09 flake fix is ineffective against the project's import style.

### 2. Strengths
*   **High-Fidelity RTL Fix:** The S-1 algorithm (Task 2a) is sophisticated; by grouping tokens into runs and only reversing the sequence of runs, it correctly preserves the internal order of Latin sigla (like `T-S 12.123`) which would be broken by a simple `[::-1]` reversal.
*   **Atomic Self-Healing:** Using `BEGIN IMMEDIATE` inside `LocalIndexer.__init__` (Task 2d) to mark PDFs for re-indexing ensures that the upgrade is crash-safe and consistent across concurrent application launches.
*   **Narrowed Scope:** Precisely targeting the `sort=True` fallback avoids re-reversing correctly ordered "blocks" mode PDFs, preventing broad regressions.
*   **AST Invariants:** The extension of the F-06 AST guard to the new helpers (Plan 01 Task 1) is a strong defensive measure against accidental reuse in structured extractors.

### 3. Concerns
*   **HIGH: Plan 02 Task 1 AST Guard Failure.** The uniqueness check for `filepath` assignments (`assert assigns == 1`) will fail. The proposed implementation uses a `try/except` block:
    ```python
    try:
        filepath = self._lookup_local_filepath(sys_id)
    except Exception:
        filepath = None
    ```
    This results in **two** assignment nodes in the AST for the name `filepath`. The current script in the plan counts all assignments and will raise an error, blocking execution.
*   **HIGH: Plan 01 Task 3 D-09 Fix Ineffectiveness.** The `importlib.reload` fixture in `conftest.py` will not solve the test flake. The test file `tests/test_local_indexer.py` uses `from shared.local_indexer import LocalIndexer`. This binds the local name `LocalIndexer` to a specific class object at import time. When `reload` is called, a **new** class object is created in the module, but the name in the test file's namespace still points to the **old** class object. The "identity divergence" between `OldClass` and `NewClass` (referenced in `101-RESEARCH.md`) will persist.
*   **MEDIUM: WR-01 AST Guard Blind Spots.** The uniqueness guard for WR-01 (Plan 02 Task 1) only checks for standard `ast.Assign` and `ast.AugAssign`. It will miss regressions that use the **walrus operator** (`filepath := ...`), **tuple unpacking** (`filepath, _ = ...`), or **type-annotated assignments** (`filepath: str = ...`).
*   **LOW: SQL Logging and Transaction.** In Plan 01 Task 2d, the `pdf_rows_pending_count` is queried outside the transaction. While safe from writers due to `BEGIN IMMEDIATE`, it counts the *total* number of pending PDFs in the system, which may include files already pending from an interrupted scan, making the log message "X committed PDF files marked for re-scan" potentially inaccurate.

### 4. Suggestions
*   **Fix WR-01 AST Guard:** Update the script to allow exactly one assignment in the `try` block and one in the `except` block, or simplify the assignment to a single line using a ternary or `getattr` fallback to satisfy the `assigns == 1` constraint.
*   **Fix D-09 Flake:** Abandon the `conftest` reload fixture. Instead, follow the "Simpler alternative" in `101-RESEARCH.md` and use a **local import** inside `test_txt_undecodable_marked_encoding_error`. This is the only guaranteed way to ensure the test uses the latest objects after a module reload.
*   **Improve SQL Accuracy:** In `LocalIndexer.__init__`, use `cur.rowcount` from the `UPDATE` execution to get the exact number of rows modified, and move the log line inside the `with self._conn:` block for consistency.
*   **Strengthen AST Guard:** Update the uniqueness script to check for `ast.NamedExpr` and `ast.AnnAssign` to ensure no alternative assignment styles can bypass the guard.

### 5. Risk Assessment
**MEDIUM.** The core RTL fix and re-indexing logic are solid (LOW risk), but the Plan 02 Task 1 AST guard will almost certainly break the build as written (HIGH risk to execution flow), and the D-09 fix is unlikely to stop the flake. Correcting these two items before execution is required.

---

## Codex Review

**Summary**

The revised plans are close, but I would not execute them as-is. There are several remaining correctness holes: the specified S-1 helper does not satisfy its own pure-RTL tests, the branch-integration tests are currently no-op scaffolds, the extractor-version "atomic" marker story is not actually atomic, and the D-09 conftest reload fixture likely does not fix the stale imported-name failure it is meant to fix.

**Strengths**

- The phase scope is well-contained and correctly keeps the RTL fix in `shared/local_indexer.py::extract_pdf_pages`.
- The `status = 'committed'` filter for version-bump reindexing is the right default and matches the live `processed_files.status` / `local_files.file_extension` schema.
- Leaving the `get_text("blocks")` path untouched is correct and important.
- Plan 02's WR-01 goal is sound: derive `is_pdf` and the image-pane request from the same resolved filepath.
- The docs-date handling correctly uses `2026-05-28`.

**Concerns**

- **HIGH — S-1 algorithm contradicts required behavior.** In `101-01-PLAN.md:607-651`, `_fix_sort_true_rtl_line` groups consecutive RTL tokens into one run, then reverses only the run sequence. For a pure Hebrew line, there is one RTL run, so output is unchanged. That fails the plan's own required behavior at `101-01-PLAN.md:582-583` and `:232-238`, where pure-RTL tokens must reverse. Fix either by making RTL tokens singleton runs while grouping non-RTL runs, or by reversing tokens inside RTL runs while preserving non-RTL run order.

- **HIGH — Branch-integration tests are no-op scaffolds.** `101-01-PLAN.md:349-387` defines tests with docstrings and comments only. Those tests will pass without proving the fallback branch calls `_fix_sort_true_rtl_page` or that the blocks path is untouched.

- **HIGH — D-09 conftest fixture likely does not fix stale aliases.** `101-01-PLAN.md:839-848` reloads `shared.local_indexer`, but `tests/test_local_indexer.py` already has module-level aliases (`EncodingError`, `extract_txt`, `LocalIndexer`). Reloading the module does not rebind those aliases. Old `extract_txt` can resolve the new `EncodingError` from the reloaded module globals, while `pytest.raises(EncodingError)` still expects the old class. The fixture should rebind `request.module.EncodingError`, `request.module.extract_txt`, `request.module.LocalIndexer`, etc., after reload.

- **HIGH — Plan 02 WR-01 assignment guard conflicts with the proposed code.** `101-02-PLAN.md:136-140` assigns `filepath` in the `try` and again in the `except`, but the AST guard at `:164-170` requires exactly one `filepath` assignment. This will fail immediately.

- **MEDIUM — extractor-version marker is not truly atomic with SQLite.** `101-01-PLAN.md:729-739` writes `.extractor_version` inside a SQLite transaction, but filesystem writes are not rolled back with SQLite. A crash after marker write but before SQLite commit can leave the marker current while committed PDFs were not marked pending. Safer pattern: perform the SQL update in `BEGIN IMMEDIATE`, commit, then write the marker; a crash before marker write only causes an idempotent repeat next launch. Best pattern: store extractor version in SQLite if true atomicity is required.

- **MEDIUM — version-bump log count is semantically wrong.** `101-01-PLAN.md:740-753` re-counts all pending PDFs after the update, but the log says "committed PDF files marked for re-scan." If one PDF was already pending, the count is inflated. Use `cur.rowcount` from the `UPDATE`.

- **MEDIUM — LTR no-op boundary tests are not boundary tests.** `101-01-PLAN.md:291-318` uses ratios around `0.1` and `0.67`, not values near `0.4`. These will not catch subtle `_rtl_ratio` or threshold drift.

- **MEDIUM — WR-02 test does not prove key removal.** `101-02-PLAN.md:209-211` and `:227` assert `ctrl._pending.get("dialog") is None`. That passes if the dict retains `"dialog": None`. The requirement says `_pending` has no entry; assert `"dialog" not in ctrl._pending`.

- **MEDIUM — WR-01 AST guard misses several reassignment forms.** `101-02-PLAN.md:164` catches simple `Assign` and `AugAssign`, but not `AnnAssign`, tuple unpacking, `NamedExpr` / walrus, `for filepath in ...`, or `except ... as filepath`.

- **LOW — F-06 positive assertion is weak and slightly overconstraining.** `101-01-PLAN.md:507-528` allows empty callers, so it does not positively prove `extract_pdf_pages` calls the helper. The "only called by extract_pdf_pages or themselves" rationale is acceptable for this release, but may be maintenance-heavy for future PDF-only helpers.

- **LOW — `xfail(strict=False)` undercuts the "signal to re-review" intent.** `101-01-PLAN.md:326-344` says XPASS should signal re-review, but `strict=False` will keep CI green. Use `strict=True` if XPASS should force attention.

- **LOW — Plan 02 summary-file dependency is brittle.** `101-02-PLAN.md:250-254` blocks docs updates on `101-01-SUMMARY.md`. Better to check the actual implementation/test evidence, or require both the summary and a code/test guard.

**Suggestions**

- Replace the S-1 helper body with an implementation that reverses RTL word order while preserving non-RTL token groups, then update the prose so "within-run preserved" applies to non-RTL runs or singleton RTL runs.
- Replace the two branch-integration scaffolds with concrete fake `fitz.open` / document / page classes and hard assertions.
- Change the D-09 fixture to reload and then rebind imported names on `request.module`; also audit other module-level `from shared.local_indexer import ...` tests.
- Fix WR-01 code or guard: either use a temp variable and exactly one `filepath = _resolved_filepath`, or relax the guard to allow the exception fallback assignment.
- Use `cur.rowcount` for the extractor-version log and write the marker after the DB commit unless the marker moves into SQLite.
- Elevate `python -m ruff check .` to explicit acceptance criteria in both plans. Since Plan 02 edits docs, keep `python scripts/check_docs.py` as a hard gate there.

**Risk Assessment**

**HIGH** until the S-1 algorithm/test mismatch, D-09 fixture flaw, no-op branch tests, and WR-01 assignment contradiction are corrected. The intended production changes are not inherently broad, but the current plans can produce green-looking execution while missing the core RTL fix and the batch-order flake.

---

## Consensus Summary

### Agreed Strengths

Both reviewers independently flagged the same positives:

- The phase scope is tight and correctly chokepoints the RTL fix in `shared/local_indexer.py::extract_pdf_pages` (D-03).
- Leaving the primary `get_text("blocks")` path untouched is the correct safety boundary.
- The `AND status = 'committed' AND LOWER(COALESCE(file_extension, '')) = '.pdf'` filter on the version-bump UPDATE is the right default — error/failed/skipped rows are not revived.
- The `BEGIN IMMEDIATE` transaction wrapping for the version-bump SQL is the right primitive for concurrent launches.
- The decision to avoid `python-bidi` / character reversal is sound and well-defended.

### Agreed Concerns (BLOCKING — both reviewers flagged at HIGH/MEDIUM)

These four issues are called out independently by **both** Gemini and Codex and should be treated as must-fix before execution:

1. **HIGH — Plan 02 WR-01 AST guard is contradictory with the proposed code.**
   The `try / except Exception` block at `101-02-PLAN.md:136-140` produces TWO `filepath = ...` assignments. The AST reachability guard at `101-02-PLAN.md:164-170` asserts exactly ONE. Execution will fail at the verify step. Two remediation options:
   - **Code change:** Use a temp `_resolved` variable inside the try/except and have exactly one `filepath = _resolved` assignment after.
   - **Guard relaxation:** Count assignments only OUTSIDE try/except handlers, OR allow up to 2 if one is in an `ExceptHandler` body.
   Either is fine; the plan must pick one and update both sides in lock-step.

2. **HIGH — D-09 conftest reload fixture does not fix stale imported-name aliases.**
   `tests/test_local_indexer.py` uses `from shared.local_indexer import LocalIndexer, EncodingError, extract_txt` at module level. `importlib.reload(shared.local_indexer)` rebinds the module's own globals but does NOT rebind the names in the importing test module's namespace. Codex's specific hazard scenario: old `extract_txt` (still holding the pre-reload `EncodingError` from `shared.local_indexer.__dict__`) can raise the NEW `EncodingError`, while `pytest.raises(EncodingError)` in the test still references the OLD class. Result: a `DID NOT RAISE` failure that this fixture cannot prevent.
   Three remediation options (ranked by reviewer preference):
   - **Codex's preferred:** Inside the autouse fixture, after `importlib.reload`, rebind the names on `request.module` (e.g. `request.module.LocalIndexer = shared.local_indexer.LocalIndexer`, same for `EncodingError`, `extract_txt`). This keeps the fix at the conftest level (USER-DEC-3) and is robust to any module that re-imports those names.
   - **Gemini's preferred (and the `101-RESEARCH.md` "Simpler alternative"):** Local import inside `test_txt_undecodable_marked_encoding_error`. This abandons USER-DEC-3 (conftest-level fix).
   - **Hybrid:** Conftest fixture that rebinds the three known aliases, plus an AST guard that asserts no NEW `from shared.local_indexer import` lands in test files without being added to the rebind list.
   USER-DEC-3 explicitly locks the conftest-level fix, so option (i) is the path of least resistance and preserves the user's decision — but the plan body must spell out the rebind step.

3. **MEDIUM — Log count semantics in the extractor-version bump.**
   `pdf_rows_pending_count = ... SELECT COUNT(*) ... WHERE status = 'pending' ...` re-queries AFTER the UPDATE and counts all pending PDF rows. If even one PDF was already in `status='pending'` before the bump (a likely state after an interrupted scan), the log line "N committed PDF files marked for re-scan" overstates by however many were already pending. The fix both reviewers suggest is identical: use `cur = self._conn.execute(UPDATE...); pdf_rows_pending_count = cur.rowcount` and inline it inside the `with` block. The plan currently mentions this as an "alternatively you may" — promote it to the required form.

4. **MEDIUM — WR-01 AST guard misses non-standard assignment forms.**
   Both reviewers flagged the same gaps: `AnnAssign` (`filepath: str = ...`), `NamedExpr` / walrus (`filepath := ...`), tuple unpacking (`filepath, _ = ...`), and Codex adds `for filepath in ...` and `except ... as filepath`. The guard should iterate all of `ast.Assign`, `ast.AugAssign`, `ast.AnnAssign`, `ast.NamedExpr`, plus look at `ast.For.target`, `ast.ExceptHandler.name`, and unpacking inside `ast.Tuple` / `ast.List` targets.

### Codex-Only Concerns Worth Acting On (Gemini did not catch these)

5. **HIGH — S-1 algorithm contradicts its own pure-RTL test.**
   This is the most important catch in this round, **and Gemini missed it**. The implementation at `101-01-PLAN.md:607-651` groups consecutive same-direction tokens into runs, then reverses only the run sequence. For pure-RTL `"האישי בארכיונו עיור בעקבות"`, all four tokens form ONE run; reversed-run-list of a single run yields the same list; within-run order is preserved → **output equals input**. But `test_sort_true_rtl_pure_hebrew_word_order_fixed` (Task 1 acceptance) asserts `fixed.split() == list(reversed(wrong.split()))`. The test will FAIL against the spec'd helper.
   Two remediation options:
   - **(A) Reverse-within-RTL-runs:** Inside each RTL run, also reverse the token list before joining. This handles pure-RTL correctly AND preserves embedded LTR sub-order in mixed lines. Pseudocode: `return ' '.join(tok for is_rtl, run in reversed(runs) for tok in (run[::-1] if is_rtl else run))`.
   - **(B) Singleton-RTL runs:** Treat each RTL token as its own run while grouping non-RTL tokens. This also reverses pure-RTL correctly but is harder to reason about for digit-interleaved cases.
   The mixed-script tests (`test_sort_true_rtl_directional_runs_preserve_shelfmarks`, `test_sort_true_rtl_digits_run_with_hebrew`) happen to pass under the current implementation only because the runs alternate. **Recommend Option A** as the minimal, semantically clear fix; verify by re-deriving the expected output of all four behavioral tests.

6. **HIGH — REV-2a branch-integration tests are scaffold-only.**
   `test_extract_pdf_pages_applies_rtl_fix_in_sort_true_fallback` and `test_extract_pdf_pages_blocks_path_untouched` (Task 1, `101-01-PLAN.md:349-387`) have docstrings + comments but no actual assertions or fake-fixture code. They will collect and "pass" trivially, providing zero coverage of the branch-routing contract the rest of the round-1 review specifically demanded. Plan must inline the concrete `FakeFitz` / `FakeDoc` / `FakePage` scaffolding (or specify a `monkeypatch.setattr` pattern against the live `fitz` import in `shared.local_indexer`) and hard `assert text == ...` lines.

7. **MEDIUM — Extractor-version marker filesystem-write is OUTSIDE the SQLite transaction.**
   Even with `BEGIN IMMEDIATE`, a crash between `_write_extractor_version` and the implicit `with self._conn:` COMMIT can split state. Codex's safer ordering: COMMIT first, write marker after. This makes a crash before the marker write an idempotent repeat next launch (UPDATE runs again, matches zero rows because everything is already `pending`, marker re-written). Plan should reorder.

8. **MEDIUM — WR-02 test uses `.get(key) is None`, which passes even if the dict retains `{"dialog": None}`.**
   Code change is one-line at `101-02-PLAN.md:209-211`: `assert "dialog" not in ctrl._pending` instead of `assert ctrl._pending.get("dialog") is None`. This actually pins key absence as the regression-prevention contract claims.

9. **MEDIUM — Boundary tests for `_rtl_ratio > 0.4` use ratios of 0.1 and 0.67, not values near 0.4.**
   `test_sort_true_rtl_boundary_below_threshold_noop` and `test_sort_true_rtl_boundary_above_threshold_reverses` don't actually test the boundary. To catch threshold drift, pick candidate strings whose `_rtl_ratio` evaluates within ±0.05 of 0.4 (e.g. 0.38 below, 0.42 above). Add an assertion documenting the computed ratio so future maintainers see the proximity.

10. **LOW — `xfail(strict=False)` on `test_sort_true_rtl_pathological_mixed_script` undercuts its stated intent.**
    The docstring says XPASS should signal re-review, but `strict=False` lets CI go green when XPASS happens. Either set `strict=True` (XPASS becomes a failure forcing review) or remove the test entirely and document the limitation in a code comment instead.

11. **LOW — Plan 02 Task 3 summary-file existence gate is brittle.**
    `101-02-PLAN.md:250-254` blocks the OPEN_ISSUES.md flip on `101-01-SUMMARY.md` existing. If a partial / failed Plan 01 run produces the summary anyway (e.g. via `<output>` block), the gate would let Plan 02 mark the bug fixed. Either check the live test pass status (run a tiny pytest invocation as part of Task 3) or strengthen the gate to require BOTH the summary AND a specific assertion (e.g. `grep "_fix_sort_true_rtl_page" shared/local_indexer.py`).

### Gemini-Only Concerns Worth Acting On

12. **LOW — F-06 positive AST assertion allows empty `callers` set** (Codex also touched on this).
    `test_sort_true_rtl_helpers_only_called_from_extract_pdf_pages` (Task 1, Plan 01) computes `callers <= allowed`. If the helper isn't called anywhere, `callers = set()`, which is `<= allowed`. The test passes whether the wiring exists or not. Tighten by requiring `extract_pdf_pages in callers` (positively prove wiring) AND `callers <= allowed` (negatively prove no rogue callers).

### Cross-Reviewer Pre-Release Gate Reminder

Both reviewers (Codex explicitly, Gemini implicitly via "Risk Assessment") agree the plans should **elevate `python -m ruff check .` to an explicit acceptance criterion**. Project memory (`feedback_pre_release_must_run_ruff.md`) notes a v7.12.0 CI failure caused by exactly this gap (18 F401 errors missed by a pre-flight that only ran pytest + check_docs). Plan 01 currently lists ruff only under `<verification>`; promote to `<acceptance_criteria>` on Task 2 (or add a Task 4) and Plan 02 Task 3.

### Divergent Views

There are no direct disagreements between Gemini and Codex on the substantive findings. Where they differ is **coverage**:

- Gemini focused on the two issues most visible from static reading of the plan: the WR-01 AST contradiction and the D-09 reload semantic.
- Codex went deeper, catching the S-1 algorithm/test contradiction (which would silently ship a no-op for pure-RTL lines — the primary use case), the no-op branch-integration tests, the FS-marker / SQLite atomicity gap, and the `.get() is None` test assertion bug.

The S-1 algorithm/test contradiction (#5) is the single most important finding of this round and would have made it through execution unnoticed had Gemini reviewed alone. Treat Codex's deep-correctness pass as load-bearing for this re-plan.

### Recommended Next Step

Run `/gsd-plan-phase 101 --reviews` and address, at minimum, items #1–#6 (the consensus HIGH/MEDIUM blockers plus the two Codex-only HIGH items). Items #7–#12 are worth folding in opportunistically but do not block execution.
