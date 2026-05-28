---
phase: 101
reviewers: [codex, claude]
reviewed_at: 2026-05-28T02:31:14Z
plans_reviewed: [101-01-PLAN.md, 101-02-PLAN.md]
notes: |
  Gemini was selected but its API returned "invalid content after all retries" on
  this prompt (full error in _tmp/gsd-review-gemini-101.err). Per the user's pre-
  authorized fallback ("If Codex unavailable, send to Claude also"), Claude was
  invoked as the Gemini substitute. Both Codex and Claude produced substantive,
  independent reviews.
---

# Cross-AI Plan Review — Phase 101

## Codex Review

**Summary**

The two plans are well scoped and mostly aligned with the Phase 101 goals. The strongest parts are the decision to avoid `python-bidi`, limit the RTL fix to the `sort=True` fallback, and use an extractor-version marker to self-heal existing local PDF indexes. Main risks are around edge cases in blunt token reversal, the extractor-version SQL updating more rows than the stated requirement, and tests that may validate the helper without proving `extract_pdf_pages` uses it in the intended branch.

**Strengths**

- The plan correctly follows the research override: no `python-bidi`, no character reversal, no dependency/spec churn.
- Scoping the reversal only to the `sort=True` fallback is the right safety boundary. It avoids breaking professional RTL PDFs where `get_text("blocks")` already works.
- The extractor-version bump is a pragmatic migration mechanism for user-owned local indexes.
- WR-01 is a real cleanup: one lookup removes the known divergence between `is_pdf` and the actual image-pane path.
- The validation matrix covers unit behavior, real fixture behavior, reindex behavior, UI cleanup regression, and the known batch-order flake.
- Extending F-06 to block the new PDF-specific RTL helpers from HTML/XLSX/CSV extractors is reasonable if the invariant is framed narrowly.

**Concerns**

- **HIGH:** Plan 01's extractor-version SQL does not match D-04. D-04 says mark committed PDF rows pending, but the proposed SQL updates all matching PDF rows in `processed_files`, including possible `error`, `failed`, `skipped`, or already-`pending` rows. That could revive known-bad PDFs repeatedly.

- **MEDIUM:** The RTL helper tests appear to focus on `_fix_sort_true_rtl_page` directly. That proves the helper, but not that `extract_pdf_pages` calls it only inside the `_detect_single_word_per_line` / `sort=True` fallback. A skipped real fixture would leave the integration path weakly covered.

- **MEDIUM:** Pure `line.split()[::-1]` is sound for mostly homogeneous RTL lines, but fragile for mixed RTL/LTR runs. Latin shelfmarks, manuscript sigla, URLs, dates, page refs, and embedded English phrases may have their internal LTR order reversed after whole-line token reversal.

- **MEDIUM:** `_rtl_ratio(line) > 0.4` may false-negative realistic Judeo-Arabic/Hebrew lines with many digits, Latin sigla, punctuation, or short Hebrew fragments. It may also false-positive mostly LTR metadata lines containing enough Hebrew names.

- **LOW/MEDIUM:** `line.split()` normalizes whitespace: it strips leading/trailing whitespace and collapses multiple spaces/tabs. That may be acceptable for extracted transcription text, but the plan should treat it as an intentional tradeoff.

- **LOW/MEDIUM:** WR-01 is probably behaviorally better, but not exactly equivalent. If the early lookup fails, the new code propagates `filepath=None` downstream, whereas the old second lookup might theoretically have succeeded later. This is likely fine, but downstream `filepath` consumers need a manual guard check.

- **LOW:** The AST single-lookup guard is useful but brittle. It finds the first function named `_open_local_browse_page` and counts attribute calls by name. It does not prove the call is on `self`, and it does not prove there is no later `filepath` reassignment.

- **LOW:** The D-09 local import fix is likely sufficient for the named test, but only if every relevant reference inside that test uses the freshly imported names and no helper invoked by the test still closes over stale module-level bindings.

- **LOW:** The F-06 guard is good only if documented as "do not reuse PDF `sort=True` RTL helpers in structured extractors." If described as "HTML/XLSX/CSV must never do RTL handling," it could become an overbroad future constraint.

**Suggestions**

- Fix the extractor-version update to honor D-04:

```sql
UPDATE processed_files
SET status = 'pending'
WHERE status = 'committed'
  AND sys_id IN (
      SELECT sys_id
      FROM local_files
      WHERE LOWER(COALESCE(file_extension, '')) = '.pdf'
  )
```

- Add at least one non-skipped integration test proving `extract_pdf_pages` applies the helper in the fallback branch. A monkeypatched/fake `fitz.open` page is enough: primary text triggers `_detect_single_word_per_line`, fallback text returns reversed RTL words, output is corrected.

- Add a companion integration test proving the primary `get_text("blocks")` path is untouched for already-correct RTL text.

- Add mixed-content test cases before expanding the algorithm. Good candidates: Hebrew plus manuscript sigla, Hebrew plus page numbers, Hebrew with parentheses/colon, and Judeo-Arabic with Latin catalog references. If those fail but are common in UAT material, consider a run-preserving reversal rather than reversing every token blindly.

- In the helper docstring, explicitly state that whitespace normalization is intentional:

```python
"""Reverse visual-order word tokens from PyMuPDF sort=True RTL fallback.

This intentionally normalizes intra-line whitespace because this path repairs
reading order for extracted plain text, not layout fidelity.
"""
```

- Strengthen the WR-01 verification script to assert uniqueness more carefully: find all functions named `_open_local_browse_page`, fail if there is not exactly one, then count `_lookup_local_filepath` calls within that function.

- For D-09, consider using a module local import instead of shadowing three names:

```python
import importlib
local_indexer = importlib.import_module("shared.local_indexer")
```

Then use `local_indexer.LocalIndexer`, `local_indexer.EncodingError`, and `local_indexer.extract_txt`. This makes the reload dependency explicit and avoids `F811`.

- For `docs/OPEN_ISSUES.md`, use the actual landing date rather than hardcoding `2026-05-27` unless that is intentionally the issue-resolution date.

**Risk Assessment**

Overall risk: **MEDIUM**.

The plans are directionally strong and should achieve the Phase 101 goals if implemented carefully. The main release risk is not architectural; it is precision. The extractor-version update needs the committed-status filter, and the RTL test set needs to prove branch integration plus mixed-content behavior. With those tightened, the residual risk is mostly acceptable text-extraction edge cases rather than broad regression risk.

---

## Claude Review

### 1. Summary

The two plans form a tight, well-scoped pre-release polish package. The TDD discipline (Wave 0 RED tests → Wave 1 GREEN implementation), the explicit override of `python-bidi` in favor of pure-Python word-token reversal, the auto-reindex via extractor-version bump, and the `depends_on: ["01"]` sequencing of the OPEN_ISSUES.md flip are all sound architectural choices. The biggest residual risk is **semantic correctness of the word-token reversal on mixed RTL/LTR lines** — `' '.join(line.split()[::-1])` is bidi-naïve and will visibly mis-order embedded LTR runs (English sigla, numbers, dates) when `_rtl_ratio > 0.4`. The D-09 flake fix is also narrower than the underlying problem warrants, and a few edge cases around the extractor-version marker and WR-01 exception-flow semantics deserve a closer look.

### 2. Strengths

- **Override discipline.** D-01/D-02 reversal post-research (with `python-bidi` decisively voided) is documented with the *why*, not just the *what*. This will save the next reviewer.
- **Surgical scoping.** Restricting the fix to the `sort=True` fallback branch (D-05) avoids breaking professional RTL PDFs that already arrive in correct order via `get_text("blocks")`. Pitfall 2 internalized.
- **Auto-reindex mechanism.** `.extractor_version` marker + `WHERE LOWER(file_extension)='.pdf'` filter is the right granularity — non-PDF rows are untouched, and the user gets the fix automatically on next launch without manual rescan.
- **TDD RED-state explicitly named.** Acceptance criterion explicitly says "tests fail RED only because the implementation does not yet exist (ImportError) — that is the expected TDD RED state." Removes a class of plan-checker confusion.
- **F-06 AST guard extension.** Pinning the new helper names out of HTML/XLSX/CSV extractors at string level is the same correct pattern Phase 95 D-02 used for the dead-code helpers.
- **AST verification for WR-01.** Hard-counting `_lookup_local_filepath` calls inside the function is much stronger than a comment-based convention.
- **Sequencing guard belt-and-suspenders.** `depends_on: ["01"]` + an explicit `101-01-SUMMARY.md` existence check before flipping the OPEN_ISSUES row. Good defensive bookkeeping.

### 3. Concerns

#### HIGH

- **C-1 (HIGH) — Mixed RTL/LTR lines silently mangled.** `' '.join(line.split()[::-1])` reverses *every* token, including embedded LTR runs that PyMuPDF emitted in their correct visual sub-order. Real cases this breaks:
  - `"T-S 12.123 ספר התורה"` (Hebrew title with Cambridge shelfmark): ratio ≈ 0.45–0.55, gate triggers, output becomes `"התורה ספר 12.123 T-S"` — the shelfmark is now split into two non-adjacent tokens and the Hebrew is correct, but the shelfmark is broken.
  - `"פרק 5 עמוד 42"`: ratio triggers, becomes `"42 עמוד 5 פרק"`. The Unicode bidi algorithm would keep `5` next to `פרק` (logical adjacency). With pure token reversal, the numbers stay adjacent to the *wrong* Hebrew word.
  - **Judeo-Arabic with Latin sigla** (Joins data, PGP references) is the canonical mixed case.

  The narrow `sort=True` fallback scoping mitigates blast radius but does not eliminate it — Ligature-OCR-style PDFs that triggered the fallback in the first place can still contain inline numbers and sigla. **Worth either (a) acknowledging this in CONTEXT/RESEARCH as a known residual and listing in OPEN_ISSUES, or (b) implementing a slightly smarter reversal that keeps consecutive LTR runs intact** (split into runs by directional category, reverse only the run sequence, keep within-run order).

- **C-2 (HIGH) — WR-01 exception-flow semantics shift.** The original code likely has this shape:
  ```python
  try:
      fp = self._lookup_local_filepath(sys_id)
      is_pdf = fp.lower().endswith('.pdf')
  except Exception:
      pass
  ```
  If the lookup raised, **`is_pdf` was never bound** — meaning a `NameError` was the original "behavior" downstream, OR `is_pdf` retained a value from an enclosing scope. The new code unconditionally sets `is_pdf = False` on exception. That is *more* correct, but the plan should explicitly verify that no downstream `if is_pdf:` branch in the function was relying on the variable being undefined (which would skip the branch via `UnboundLocalError`-caught-by-outer-handler vs. cleanly evaluate as `False`).

  Recommend: add a one-line check in Step 3 of Task 1 that confirms `is_pdf` was previously set in *every* code path of the function, OR that the exception handler at line ~19151 has no companion outer `try` swallowing `NameError`.

#### MEDIUM

- **C-3 (MEDIUM) — Extractor-version marker on fresh install.** Plan doesn't specify what `_read_extractor_version(index_dir)` returns when the file is absent (fresh install, brand new My Library tab). If it returns `None` or `""`, then `None != "2"` triggers a no-op UPDATE on an empty `processed_files` table — harmless but worth verifying. If it raises (e.g., `FileNotFoundError` from `open()`), the init fails. The mirrored `_read_schema_marker` pattern likely handles this; please confirm in Plan 01 Task 2c that the helper has the same `try/except FileNotFoundError: return None` shape and write the marker unconditionally at end of init even if no update ran.

- **C-4 (MEDIUM) — Concurrent re-launch race on the UPDATE.** Phase 97.2 had recurring "running" rows from hard-kill SIGKILLs. If two desktop processes launch concurrently after a version bump (rare but possible), both will run the same UPDATE and both will write the marker. SQLite serializes the UPDATE, but the second process might write the marker *after* the first started extracting. Probably benign (the extraction itself is idempotent), but worth adding `BEGIN IMMEDIATE`/explicit transaction around `UPDATE + write_marker` so the second process either sees the new marker or blocks on the lock. Lightweight change.

- **C-5 (MEDIUM) — D-09 flake fix is per-test, not per-file.** Plan 01 Task 3 adds a local import to *one* test (`test_txt_undecodable_marked_encoding_error`). But the root cause is that `importlib.reload(shared.local_indexer)` in `test_mupdf_warnings_suppressed.py` invalidates module-level bindings for the *entire* `tests/test_local_indexer.py` module. If pytest collection order ever places another test from `test_local_indexer.py` *after* a reload-test, it will exhibit the same symptom and the local-import fix won't help it. Two safer options:
  1. Move the module-level imports inside every test function in `test_local_indexer.py` (verbose but rigorous).
  2. Add a `conftest.py`-level autouse fixture that re-imports `shared.local_indexer` for the duration of each test in `test_local_indexer.py`.
  3. Fix the *cause* in `test_mupdf_warnings_suppressed.py` by restoring `sys.modules['shared.local_indexer']` to the original after the test (teardown).

  Option 3 is cleanest but the plan deliberately excludes touching `test_mupdf_warnings_suppressed.py`. Option 2 (autouse fixture) is the smallest robust change and worth recommending over the current per-test local-import.

- **C-6 (MEDIUM) — `line.split()` whitespace normalization.** `line.split()` (no argument) collapses runs of whitespace and strips leading/trailing. After `' '.join(...)`, single-space-delimited output replaces tabs and double-spaces. PyMuPDF `sort=True` output is unlikely to contain meaningful tab structure, but transcriptions that had double-spaced columns or aligned poetry (piyyut) could lose visual layout. Consider documenting this as an intentional trade-off in a comment, or use a regex-based tokenizer that preserves the original separator widths.

- **C-7 (MEDIUM) — `_rtl_ratio > 0.4` gate on borderline lines.** A line like `"page 3 / עמ' 5"` could land near 0.3 (denominator includes digits and slashes). A bilingual heading `"Genesis בראשית"` lands near 0.5. The threshold inherited from existing code is fine as a default, but the plan doesn't include a test case at the threshold boundary. Recommend adding two boundary cases to `test_sort_true_ltr_noop` and `test_sort_true_rtl_word_order_fixed`: `~0.39` and `~0.41` ratio lines.

#### LOW

- **C-8 (LOW) — `_fix_sort_true_rtl_page` joining with `'\n'`.** If the input used `'\r\n'` or `'\r'` line terminators, the rejoin loses them. PyMuPDF's `get_text("text")` is documented to use `\n` on all platforms, so practically a non-issue — worth a one-line comment confirming.

- **C-9 (LOW) — AST guard for WR-01 robustness.** The guard catches `self._lookup_local_filepath()` via `ast.Attribute(attr='_lookup_local_filepath')`. It does NOT catch:
  - `getattr(self, '_lookup_local_filepath')()` (dynamic dispatch)
  - Indirect calls through a helper method that itself calls `_lookup_local_filepath`
  - Walrus inside an expression (`if (fp := self._lookup_local_filepath(s)):`) — actually IS caught by `ast.walk` since walrus is an `ast.NamedExpr` wrapping the Call ✓

  None of these are likely failure modes in the current codebase, but if the plan's authors are committing to "single lookup" as an invariant, the verify block should grep for `_lookup_local_filepath` textually across `genizah_app.py::_open_local_browse_page` slice as a belt-and-suspenders check, in addition to the AST count.

- **C-10 (LOW) — Empty line behavior.** `_rtl_ratio("")` is presumably 0 (no RTL chars / total chars = 0/0 → defined as 0 per existing primitive). Worth confirming the function returns 0 (not raise ZeroDivisionError) and adding `""` to the `test_sort_true_ltr_noop` case list. Plan already includes `""` ✓ — confirm `_rtl_ratio("")` semantics in the test docstring.

- **C-11 (LOW) — `_pdf_count` log line.** Plan 01 Task 2d says to "log `_pdf_count`". Naming nit: clearer as `pdf_rows_pending_count` so future log-grepping doesn't collide with desktop's `_pdf_count` UI counter (if one exists).

### 4. Suggestions

- **S-1.** Add a `_fix_sort_true_rtl_line` variant that respects directional runs (sketch):
  ```python
  def _fix_sort_true_rtl_line(line: str) -> str:
      if _rtl_ratio(line) <= 0.4:
          return line
      # Group consecutive tokens by predominant directionality, reverse the
      # group sequence, but keep tokens within a same-direction run in order.
      tokens = line.split()
      runs, current, current_is_rtl = [], [], None
      for tok in tokens:
          tok_is_rtl = _rtl_ratio(tok) > 0.4
          if current_is_rtl is None or tok_is_rtl == current_is_rtl:
              current.append(tok); current_is_rtl = tok_is_rtl
          else:
              runs.append((current_is_rtl, current))
              current, current_is_rtl = [tok], tok_is_rtl
      if current:
          runs.append((current_is_rtl, current))
      # Reverse run order (visual→logical for outer direction); keep within-run order.
      return ' '.join(tok for _, run in reversed(runs) for tok in run)
  ```
  This preserves shelfmarks (`T-S 12.123`) and numeric runs in their natural sub-order while still flipping the dominant Hebrew word order. Adds ~15 lines of code and one extra test case but resolves C-1 cleanly. If the team prefers the simpler implementation, document the residual in OPEN_ISSUES explicitly.

- **S-2.** Replace Plan 01 Task 3 (D-09) with a `conftest.py` autouse fixture scoped to `tests/test_local_indexer.py`:
  ```python
  @pytest.fixture(autouse=True)
  def _refresh_local_indexer_module():
      import importlib, shared.local_indexer
      importlib.reload(shared.local_indexer)
      yield
  ```
  This makes the file order-independent under all pytest collection sequences, not just the one currently observed.

- **S-3.** Add explicit verification in Plan 01 Task 2d that on a *fresh install* (no `.extractor_version` file, empty `processed_files`), the init writes the marker and does not crash. New test case: `test_extractor_version_fresh_install_writes_marker`.

- **S-4.** Wrap the version-bump UPDATE in `BEGIN IMMEDIATE`:
  ```python
  with self._conn:  # implicit transaction
      self._conn.execute("BEGIN IMMEDIATE")
      self._conn.execute("UPDATE processed_files SET status='pending' ...")
      _write_extractor_version(index_dir, _CURRENT_EXTRACTOR_VERSION)
  ```
  Eliminates C-4 with one extra line.

- **S-5.** In WR-01 Step 1, explicitly trace `is_pdf` reachability after the consolidation:
  ```
  # Before edit: confirm there is no codepath in _open_local_browse_page where
  # `is_pdf` is referenced but unbound (e.g., the old except-pass-no-assign branch).
  # After edit: is_pdf is set on every branch (try-success or except).
  ```
  Add this as a `<verify>` step before the AST count, not after.

- **S-6.** Plan 02 Task 2 (`test_discard_scope_clears_pending`): the comment in the test body says "interface contract confirmed in Plan 02's `<interfaces>` block". Consider promoting that contract into a one-line docstring on `PdfImageController.request()` itself if it isn't already, since the test is now load-bearing on that synchronous behavior.

- **S-7.** Extend F-06 AST guard with a positive assertion too — not just that HTML/XLSX/CSV *don't* call the helpers, but that the new helpers are *only* called from `extract_pdf_pages`. Catches accidental future cross-wiring:
  ```python
  callers = {fn.name for fn in ast.walk(tree)
             if isinstance(fn, ast.FunctionDef)
             and any(_calls(fn, '_fix_sort_true_rtl_page'))}
  assert callers <= {'extract_pdf_pages', '_fix_sort_true_rtl_page'}
  ```

- **S-8.** Add to Plan 01 Task 1a a fixture-line for a *mixed RTL/LTR* test case (even if it's a known-limitation case for the simple implementation): `test_sort_true_rtl_mixed_shelfmark_known_limitation` with a `pytest.xfail` reason citing C-1. This makes the residual explicit and lets the team upgrade S-1 later without scrambling.

### 5. Risk Assessment

**Overall risk: MEDIUM (lower bound LOW if S-1 is adopted; upper bound MEDIUM-HIGH if mixed-direction lines are common in the real Hebrew fixture).**

Justification:
- The fix is correctly scoped to a fallback branch that already represents degraded extraction, so the blast radius of any per-line reversal regression is bounded to PDFs that *were already extracting in single-word-per-line mode*. Professional RTL PDFs using the `blocks` path are not at risk.
- The auto-reindex mechanism is well-designed, mirrors an established pattern (`_read_schema_marker`), and degrades safely on missing marker or empty DB.
- The WR-01 / WR-02 / D-09 cleanups are small, well-isolated, and reviewed.
- The single open risk concentration is **C-1 (mixed RTL/LTR mishandling)** + **C-5 (flake fix scope)**: both are correctness rather than safety issues, and both have clean mitigations (S-1, S-2). If the real Hebrew fixture in D-06 is pure-Hebrew prose, C-1's blast radius is zero; if it includes shelfmarks/folio numbers/inline sigla (likely, given the corpus), C-1 will produce visibly wrong text that users will report within the v7.15 release window.

**Recommendation:** Adopt S-1 (directional-run reversal) OR explicitly document C-1 as a known-limitation in OPEN_ISSUES.md with a follow-up phase planted, AND adopt S-2 (autouse fixture for the flake). With those two changes, the plan becomes a clean LOW-risk pre-release polish phase.

---

## Gemini Review

*Skipped — Gemini API returned "invalid content after all retries" (model-routing failure). Claude was invoked as the user-pre-authorized fallback. Full Gemini error stack in `_tmp/gsd-review-gemini-101.err`.*

---

## Consensus Summary

Both reviewers rated the plan **MEDIUM overall risk** and agreed the architectural direction is sound. The disagreement is on **how much pre-execution work is warranted vs. landing-then-following-up**.

### Agreed Strengths

- Override of `python-bidi` for pure-Python word-token reversal is the right call.
- Scoping the fix to the `sort=True` fallback only (leaving `get_text("blocks")` untouched) is a correct safety boundary.
- The `.extractor_version` marker + PDF-only WHERE filter is a pragmatic, low-risk auto-reindex mechanism.
- The WR-01 single-lookup AST guard is stronger than a comment-based convention.
- TDD discipline (Wave 0 RED → Wave 1 GREEN) and the F-06 invariant extension reflect mature plan hygiene.
- The `depends_on: ["01"]` + `101-01-SUMMARY.md` existence check for the OPEN_ISSUES flip is well-considered sequencing.

### Agreed Concerns (raised by both reviewers — highest priority)

| Severity | Concern | Codex framing | Claude framing |
|----------|---------|---------------|----------------|
| **HIGH (consensus)** | **Mixed RTL/LTR lines silently mishandled by blunt token reversal.** Embedded shelfmarks (`T-S 12.123`), folio numbers, Latin sigla, page references, and Judeo-Arabic with Latin catalog references will have their internal LTR sub-order destroyed when the line's overall `_rtl_ratio > 0.4`. | MEDIUM — "fragile for mixed RTL/LTR runs" | HIGH C-1 — concrete failing examples |
| **HIGH (consensus)** | **WR-01 changes exception-flow semantics.** The new code unconditionally sets `filepath=None`/`is_pdf=False` on lookup failure; the old code's `try/except: pass` left variables possibly unbound. Plan should explicitly verify downstream `is_pdf` / `filepath` consumers don't rely on the old behavior. | LOW/MEDIUM — "not exactly equivalent" | HIGH C-2 — recommends one-line reachability check |
| **MEDIUM (consensus)** | **`_rtl_ratio > 0.4` gate has unverified borderline behavior.** No tests at `~0.39`/`~0.41`; bilingual headings, lines with many digits/sigla can flip the gate either way. | MEDIUM — "may false-negative … may false-positive" | MEDIUM C-7 — recommends boundary-case tests |
| **MEDIUM (consensus)** | **D-09 flake fix is narrower than root cause.** Local-import-in-one-test patches only the named symptom; the underlying module-reload pollution affects every test in `test_local_indexer.py` under unfavorable collection orders. | LOW — "only if every reference uses freshly imported names" | MEDIUM C-5 — recommends conftest autouse fixture instead |
| **MEDIUM (consensus)** | **`line.split()` whitespace normalization is a silent behavior change.** Tabs, double-spaces, leading/trailing whitespace are lost; piyyut/aligned-column layouts could degrade. Worth documenting as intentional or replacing with a separator-preserving tokenizer. | LOW/MEDIUM — "should treat as intentional tradeoff" | MEDIUM C-6 — recommends docstring + consider regex tokenizer |
| **MEDIUM (consensus)** | **Tests don't prove `extract_pdf_pages` integration.** Helper unit tests + a skip-if-absent real fixture leave the branch wiring weakly covered. Recommend a monkeypatched `fitz.open` integration test. | MEDIUM — explicit | (implicit in Claude's broader validation framing) |
| **LOW (consensus)** | **WR-01 AST guard is brittle.** Only catches `ast.Attribute(attr=...)` calls; misses `getattr` dynamic dispatch, doesn't verify the call is on `self`, doesn't enforce "no other `filepath =` reassignment". A textual grep belt-and-suspenders would help. | LOW — "useful but brittle" | LOW C-9 — same root, slightly different mitigation |
| **LOW (consensus)** | **F-06 invariant framing.** The negative-only "HTML/XLSX/CSV must not call these helpers" should be paired with a positive-only "helpers called only from `extract_pdf_pages`" assertion, and the invariant rationale should be documented narrowly. | LOW — "could become overbroad future constraint" | LOW S-7 — concrete positive-assertion sketch |

### Divergent Views (worth investigating)

- **Extractor-version SQL `status='committed'` filter (Codex HIGH, Claude silent).** Codex flags that the proposed UPDATE will flip `error`/`failed`/`skipped`/already-`pending` rows back to `pending` and revive known-bad PDFs each launch. The PLAN explicitly says "marks committed PDFs (and only PDFs) as 'pending'" in success criteria — so the SQL needs `AND status = 'committed'` to match the spec. **This is the most actionable single-line fix to the plan.** Claude did not raise it.

- **Fresh-install + concurrent-launch on extractor-version (Claude MEDIUM C-3 + C-4, Codex silent).** Claude flags that `_read_extractor_version` semantics on missing file and concurrent-launch races on the UPDATE + marker write are unspecified. Lightweight mitigations (try/except FileNotFoundError → None; `BEGIN IMMEDIATE` around UPDATE + marker write). Codex did not raise these.

- **Mixed RTL/LTR severity (Claude HIGH C-1, Codex MEDIUM).** Both agree on the failure mode and direction; they disagree on whether it's a release blocker. Claude provides concrete failing examples from the GenizahSearch corpus (shelfmarks, folio numbers) and argues the residual is likely to produce user-visible regressions in v7.15. Codex frames it as an acceptable edge case to address with mixed-content test cases first.

- **D-09 fix mechanism.** Codex prefers `importlib.import_module("shared.local_indexer")` + dotted access (explicit, no `F811`). Claude prefers a `conftest.py` autouse fixture scoped to the file (more rigorous against future collection orders). Both are improvements over the current per-test local-import.

### Actionable Top 3 (consensus rank)

1. **Tighten the extractor-version SQL.** Add `AND status = 'committed'` to the UPDATE (Codex HIGH). One-line fix. Matches the plan's own success criterion.
2. **Decide on mixed RTL/LTR handling before execution.** Either adopt the directional-run reversal in Claude S-1, or explicitly document the residual in `docs/OPEN_ISSUES.md` and add an xfail/limitation test (Claude S-8) so it's tracked. Both reviewers flag this as the highest correctness risk.
3. **Broaden the D-09 fix.** Replace the per-test local import with either Codex's `importlib.import_module` pattern or Claude's `conftest.py` autouse fixture — both reviewers consider the per-test patch insufficient against future test-order changes.

Secondary (worth doing): integration test that exercises the `extract_pdf_pages` `sort=True` branch via monkeypatched `fitz.open` (Codex); boundary tests at `_rtl_ratio` ≈ 0.39/0.41 (Claude C-7); WR-01 reachability check before the AST count (Claude S-5); positive-assertion in F-06 guard (Claude S-7).

To incorporate this feedback into planning:

```bash
/gsd-plan-phase 101 --reviews
```
