---
phase: 101-local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea
plan: 02
status: complete
completed: 2026-05-28
requirements:
  - D-07
  - D-08
---

## Plan 101-02 — Phase 100 code-review remnants (WR-01, WR-02, OPEN_ISSUES.md)

### What was built

**WR-01 — collapse double `_lookup_local_filepath` in `_open_local_browse_page` (`genizah_app.py`).** Two separate `self._lookup_local_filepath(sys_id)` calls (lines 19153 and 19237 of the prior tree) could diverge during an indexer rescan, leaving `is_pdf=True` + `filepath` empty so the Browse image pane reveals but `controller.request()` returns None — pane shown with no render and no placeholder (Phase 100 `100-REVIEW.md` WR-01). Collapsed to ONE lookup AND ONE `filepath` binding. The temp `_resolved` pattern keeps exception handling local while the new AST reachability guard counts exactly one `filepath = ...` Assign across the whole method body. `is_pdf` is derived from the same filepath that the pane reveal + `controller.request` consume, so the two decisions cannot diverge. The pane-reveal branch was already `if is_pdf and controller is not None and bool(filepath):` so `is_pdf` now implies a usable filepath.

**WR-02 — `test_discard_scope_clears_pending` regression (`tests/test_pdf_image_controller.py`).** Pins the documented `PdfImageController` contract that `_pending[scope]` has NO entry immediately after `discard_scope` — `request()` populates `_pending` synchronously (before the debounce timer), so no Qt event processing is needed. Per REVIEWS round 2 Codex MEDIUM #8, the assertion uses `"dialog" not in ctrl._pending` rather than `.get(scope) is None` so the test fails if the dict ever retains `{scope: None}`. Also asserts `discard_scope` idempotence (a second call must not raise). Production controller (`desktop/pdf_image_controller.py`) is UNCHANGED — this is a regression guard only.

**WR-01 AST guards (`tests/test_wr01_open_local_browse_page_ast.py`, NEW file).** Three machine-verified pins per REVIEWS round 2 BLOCKER #4 (Gemini + Codex):

- `test_open_local_browse_page_single_definition` — exactly 1 method definition (REV-2d uniqueness).
- `test_open_local_browse_page_single_lookup_call` — exactly 1 `self._lookup_local_filepath` Call inside the method body (single-lookup invariant).
- `test_open_local_browse_page_single_filepath_assignment` — exactly 1 binding of the name `filepath` across ALL rebind forms (`Assign` / `AugAssign` / `AnnAssign` / `NamedExpr` walrus / tuple-or-list unpacking / `For.target` / `ExceptHandler.name`). Strengthened over the original `python -c` one-liner so future regressions in any rebind form trip the guard.

**OPEN_ISSUES.md bookkeeping (`docs/OPEN_ISSUES.md`).** REV-2g: date is **2026-05-28**, not 2026-05-27.

- Line 220 (RTL row, formerly `❌ Open 2026-05-27`) flipped to `✅ Fixed (2026-05-28)` with full closure note covering S-1 directional-run reversal, the `_fix_sort_true_rtl_*` helpers, gate `_rtl_ratio > 0.4`, embedded Latin shelfmark adjacency preservation, the NOT-python-bidi rationale (`get_display` corrupts already-correct Hebrew letters), and the `_CURRENT_EXTRACTOR_VERSION` self-heal mechanics.
- NEW row added for `Phase 100 review WR-01/WR-02` (Fixed 2026-05-28) covering the single-lookup collapse + AST guards + `test_discard_scope_clears_pending`.
- Top-of-file `Last Updated` timestamp bumped to 2026-05-28 with full Phase 101 closure prose.

Belt-and-suspenders gate honored (REVIEWS round 2 Codex LOW #11) — flipped the RTL row to Fixed ONLY after verifying BOTH:

1. `.planning/phases/101-…/101-01-SUMMARY.md` exists.
2. `grep -q '_fix_sort_true_rtl_page' shared/local_indexer.py` succeeds.

### Verification

- `python -m pytest tests/test_wr01_open_local_browse_page_ast.py -v` → 3 passed (single_definition + single_lookup_call + single_filepath_assignment).
- `python -m pytest tests/test_pdf_image_controller.py` → 35 passed (the new `test_discard_scope_clears_pending` + 34 pre-existing).
- `python -m pytest tests/ -k "browse or local_nav or open_local or pdf_image"` → 202 passed, 5 skipped (no desktop nav regression).
- `git ls-files | grep .py | xargs python -m ruff check` → All checks passed!
- `python scripts/check_docs.py` → exit 1, BUT all 21 broken links are pre-existing in `docs/INCIDENT-2026-05-25-nli-iiif-hang.md` (Phase 98 incident report); pre-edit exit was ALSO 1; Phase 101 introduces no new issues.

### Commits

1. `5e8425bb` — fix(101-02): WR-01 collapse double _lookup_local_filepath in _open_local_browse_page
2. `fc08ee37` — test(101-02): WR-02 regression test_discard_scope_clears_pending
3. `0a17bc8a` — docs(101-02): OPEN_ISSUES.md — RTL bug + WR-01/WR-02 marked Fixed (2026-05-28)

### Decisions honored

- REV-2b (reachability) + REV-2d (uniqueness) — both encoded as AST tests in the new `tests/test_wr01_open_local_browse_page_ast.py` rather than a one-liner.
- REVIEWS round 2 BLOCKER #1 — single `filepath =` Assign target via temp `_resolved`.
- REVIEWS round 2 BLOCKER #4 — strengthened AST guard covers ALL rebind forms.
- REVIEWS round 2 Codex MEDIUM #8 — WR-02 uses `not in` key-absence assertion, NOT `.get() is None`.
- REVIEWS round 2 Codex LOW #11 — belt-and-suspenders gate: SUMMARY.md existence AND helper present.
- REV-2g — date 2026-05-28, not 2026-05-27.

### What this enables

Phase 101 v7.15 pre-release polish is now complete. The v7.15 release sequence can proceed without the RTL caveat and without the Phase 100 code-review remnants outstanding.
