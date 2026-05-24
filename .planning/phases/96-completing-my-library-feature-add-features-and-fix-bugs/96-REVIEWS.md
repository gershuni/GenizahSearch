---
phase: 96
reviewers: [gemini, codex]
reviewed_at: 2026-05-24T00:00:00Z
skipped: [claude — current session is Claude Code, would not be independent]
plans_reviewed: [96-01, 96-02, 96-03, 96-04, 96-05, 96-06, 96-07, 96-08, 96-09]
---

# Cross-AI Plan Review — Phase 96

## Gemini Review

This is a comprehensive and technically rigorous set of plans that systematically closes the remaining gaps in the "My Library" feature. The phase is well-structured, moving from foundational fixtures and skeletons (Wave 0) to engine-level normalization and primitives (Wave 1), through persistence and cascade logic (Waves 1-2), and culminating in complex UI wiring and cleanup (Waves 3-4). The "detect-then-fallback" strategy for PDF extraction and the "normalize-at-source" approach for highlighting show a mature understanding of the existing architecture.

### Strengths
- **Architectural Consistency:** The choice of **Option A** for highlighting (Plan 96-03) is excellent. It ensures that LOCAL hits are indistinguishable from Genizah hits for downstream UI components, maintaining the "two-phase" search model and preserving the `filter_text` property for existing UX.
- **Robust Fixture Validation:** Plan 96-01 Task 1 is particularly well-designed. By validating the synthetic PDF against the actual production heuristic rather than brittle PyMuPDF internals, you ensure the test artifact is resilient to future PyMuPDF version updates.
- **Gutter Preservation:** The explicit requirement to use `apply_line_numbered_text` (Plan 96-08) is a critical catch. It prevents a regression in the v7.12.0 line-number gutter feature which would have occurred if `setHtml` or `setPlainText` were used directly.
- **Cascade Discipline:** The OR-ing of `_local_filter_active` with `_optout_active` in the cascade joinpoints (Plan 96-05) is a subtle but vital fix. It ensures that the visibility set is correctly computed even when the primary three-state filter is set to "All."
- **Self-Correcting Tests:** The use of `xfail(strict=True)` in Plan 96-01 and the subsequent audit/conversion in Plan 96-09 is a professional TDD pattern that prevents "skip-rot" and ensures the final test suite is fully active and meaningful.

### Concerns
- **UI Thread Blocking during Folder Walk (Plan 96-06)** — Severity: **LOW**
  - `_OptoutTreeWidget.populate_for_folder` performs a recursive `os.listdir`/`os.path.isdir` walk and creates `QTreeWidgetItem`s on the UI thread. For very large or deep folders (e.g., reaching the 5,000-file ceiling), this might cause a noticeable UI stutter when a folder is selected.
- **Redundant Layout Logic in `_OptoutTreeWidget` (Plan 96-06)** — Severity: **LOW**
  - The `_populate_node` helper iterates `os.listdir` twice (once for subdirs, once for files) to ensure subfolders appear at the top. While this provides good UX, it doubles the I/O calls on the UI thread.
- **Ambiguity in "View All" Separator Hebrew Labels (Plan 96-08)** — Severity: **LOW**
  - In Hebrew, the word for "page" is `דף`, but for a printed book, it might sometimes be `עמוד`. `דף` is the correct choice for MS folios, but if the PDF is a printed book, `עמוד` might be more common. However, `דף` is a safe and scholarly default.

### Suggestions
- **Plan 96-03 Task 1 Step 2:** When implementing the no-match fallback in `_build_local_result_dict`, ensure the `content[:200]` snippet also replaces newlines with the "‖" separator if it doesn't already, to match the formatting of successful `highlight()` calls.
- **Plan 96-04 Task 2:** In `_prune_optouts_to_disk`, consider adding a log message if many entries are pruned, as this might help debug cases where a user's drive mapping changes and they "lose" their opt-out state.
- **Plan 96-06 Task 1 Step 2:** To mitigate the UI blocking risk, consider adding a `QApplication.processEvents()` call inside the subdir loop if `dir_count % 5 == 0`, or simply documenting that folders exceeding the ceiling may cause temporary stutters.
- **Plan 96-08 Task 2b Step 8:** Ensure the `_show_local_browse_controls(False)` call is also made when the Browse panel is cleared or when switching to a non-manuscript view (e.g., the welcome screen).

### Risk Assessment
**LOW.** The phase is exceptionally well-researched with pinned identifiers and established analogs for almost every task. The dependency on manual visual verification for the splitter and tri-state checkboxes is correctly identified, and the automated AST guards provide a strong safety net against cascade-drift regressions. The preservation of Phase 95 and v7.12 invariants is explicit and thoroughly checked.

---

## Codex Review

Phase 96 is unusually well-researched and mostly well decomposed: the plans identify the right architectural seams, preserve Phase 95 invariants explicitly, and use staged validation plus human UI checkpoints where automation is weak. The revised D-08 session-JSON decision is handled correctly in principle, and D-F5 Option A is the right direction. As written, though, there are several plan-level issues that could still ship regressions behind green tests, especially around global opt-out state, regex filtering semantics, Browse-panel LOCAL navigation, and the skip-based scaffold.

### Strengths
- Strong wave ordering: Wave 0 scaffolding, Wave 1 isolated engine/indexer/persistence work, later UI wiring.
- D-08 revision from QSettings to session JSON is correct and Plan 96-04's top-level cross-surface `local_file_optouts` key is defensible.
- D-F5 Option A is the right architectural choice: normalize LOCAL result shape in `genizah_core.py` instead of adding UI branches.
- D-F4 fixture validation via the production heuristic is sound; avoiding PyMuPDF block-count assumptions is a good correction.
- Phase 95 invariants are repeatedly called out: RRF post-dedup, web `LOCAL` guard, multitenant storage guard, cloud-write gates.
- Human checkpoints are placed where they matter: PyQt splitter/tree rendering and Browse/ResultDialog navigation.
- `96-08-WIRING-NOTES.md` is a useful artifact for preventing vague runtime discovery in the most fragile UI plan.

### Concerns

- **HIGH — Plan 96-06 Task 1 can drop opt-outs from other folders.** `_commit_changes()` rebuilds `app._local_file_optouts` from only the currently displayed tree, then clears the global set. Selecting folder B and toggling one file would erase opt-outs previously set in folder A.

- **HIGH — D-F5 may not satisfy "regex-aware LOCAL" fully.** Plan 96-03 keeps Tantivy hits whose regex does not match, falling back to `content[:200]` with empty `highlight_pattern`. CONTEXT D-04 says LOCAL should use the same two-phase Tantivy candidates → regex filter+highlight model. If the regex does not match, the hit should probably be filtered out, not shown unhighlighted.

- **HIGH — The BLOCKER-4 test does not actually test the merge call site.** `test_d_f5_integration_regex_arrives_at_build_local_result_dict` calls `_query_local_index(..., regex=regex)` directly, so it cannot catch the real regression where the main search merge call forgets `regex=regex`.

- **HIGH — Plan 96-08 Browse rendering risks incorrect/unsafe HTML.** `_open_local_browse_page` builds HTML with `text.replace("\n", "<br>")` without escaping local file content. Use `html.escape` or the existing `_htmlify` path before `apply_line_numbered_text`.

- **MEDIUM — Browse opens the wrong LOCAL page/chunk.** Plan 96-08 defaults `_open_local_browse` per-page mode to `p_num=1`, ignoring the clicked search hit's `p_num`. A user clicking a hit on page 7 should land on page 7.

- **MEDIUM — Browse toggle state may be incomplete.** View-All mode does not clearly set `_local_browse_current_sys_id`, but `_toggle_local_browse_view_mode()` depends on it. Toggling from View-All back to per-page may no-op or jump incorrectly.

- **MEDIUM — `p_num + offset` assumes contiguous pages.** Plan 96-08 Browse nav computes `new_p = cur + offset`; this breaks if blank pages are skipped. The engine primitive already supports `next_prev`; Browse should use it.

- **MEDIUM — D-F4 "good PDF stays blocks" is not directly tested.** The planned test checks detector output after extraction, but it does not prove fallback was not called. A bad implementation could fallback and still pass.

- **MEDIUM — Path canonicalization is underspecified.** Opt-outs are keyed by canonical filepath, but Plan 96-04 restore/save and Plan 96-05 lookup comparisons do not explicitly canonicalize both sides. Windows case/separator drift could make opt-outs fail silently.

- **MEDIUM — 96-06 still has vague wiring despite 96-08 notes.** MyLibraryTab folder item data, selected-path extraction, and scan-complete callback names are not pinned. This is a similar risk to the one `96-08-WIRING-NOTES.md` solved.

- **MEDIUM — Skip-then-convert scaffolding can mask failures until the final wave.** Plan 96-09's audit helps, but stale skips are discovered very late. Several tests also copy stub logic instead of importing production behavior, so they can pass while production diverges.

- **LOW — Validation commands are not consistently Windows-safe.** Many plans use `grep`, `tail`, `test -f`, `/tmp/...`, and bash-style pipelines while the project environment here is PowerShell on Windows.

- **LOW — Hebrew/RTL strings need one more pass.** Removing `צפה בדפדוף` is correct and `עיין` stays, but new `Prev`, `Next`, `View All`, `Per page`, `page/chunk` strings should be checked through the project translation mechanism, not only inline `CURRENT_LANG` checks.

### Suggestions

- In **96-06 Task 1**, change `_commit_changes()` to preserve opt-outs outside the currently displayed folder: remove only displayed paths from the existing global set, then add unchecked displayed leaves. Add a test for "folder A opt-out survives toggling folder B".

- In **96-03 Task 1**, decide explicitly whether regex non-matches are dropped. To honor D-04, filter them out in `_query_local_index` when `regex` is provided. If fallback display is intentional, revise D-04 because it is no longer the same two-phase model.

- In **96-03 tests**, add an AST guard or real search-level monkeypatch asserting the main merge call contains `_query_local_index(query_str, mode, gap, regex=regex)`.

- In **96-08 Task 2b**, render local text with the same escaping path as ResultDialog, open `hit_data.get("p_num")` rather than page 1, set `_local_browse_current_sys_id` in both View-All and per-page modes, and use `get_local_browse_page(..., next_prev=offset)` for nav.

- In **96-02**, add a monkeypatch/instrumentation test proving `get_text("text", sort=True)` is not called for `hebrew_sample.pdf`, plus at least one more clean PDF fixture or documented user sample per D-06.

- In **96-04/96-05**, canonicalize opt-out paths on restore, save, prune, and comparison. Add a Windows test with mixed case and backslash/forward-slash variants.

- In **96-06**, prefer adding `LocalIndexer.list_all_filepaths()` instead of direct `_conn` fallback, and log exceptions instead of broad silent `pass` in UI persistence/filter refresh paths.

- In **96-01/96-09**, prefer `xfail(strict=True)` for future-expected failures over `pytest.skip`, and convert tests as each implementing plan lands rather than waiting for 96-09.

- Add final invariant checks that fail if `shared/search_serializer.py`, `corrections_client.py`, or `lists_sync.py` were modified unexpectedly, or if cloud-write gate placement changed.

- Convert shell validation snippets to Python or PowerShell-compatible commands so the plans can run cleanly in the repo's Windows environment.

### Risk Assessment
**HIGH as written** — the architecture is strong, but the global opt-out overwrite, regex-filter ambiguity, and LOCAL Browse navigation/rendering gaps could ship user-visible regressions even if much of the planned test suite is green.

---

## Consensus Summary

The two reviewers diverge sharply on overall risk — **Gemini rates LOW** (architecture solid, automation strong), **Codex rates HIGH** (substantive correctness bugs that would ship behind green tests). Codex's deeper read found 4 HIGH-severity defects that Gemini missed. Both reviewers fully agreed on the architectural choices (D-F5 Option A, D-08 session JSON, D-F4 production-heuristic fixture validation, RRF invariant preservation, gutter preservation via `apply_line_numbered_text`).

### Agreed Strengths (both reviewers)
- D-F5 Option A is the right architectural choice — normalize LOCAL hit dict at engine layer, not UI per-source branches.
- D-F4 fixture validation against the production heuristic (not PyMuPDF block counts) is sound.
- v7.12.0 line-number gutter preservation via `apply_line_numbered_text` is explicitly required.
- Wave ordering (Wave 0 scaffolding → Wave 1 isolated engine/indexer work → later UI wiring) is correct.
- Phase 95 invariants (RRF POST-dedup, cloud-write gates, web LIBRARY_CODES allowlist, multitenant guard) are repeatedly called out.
- `96-08-WIRING-NOTES.md` artifact is a valuable precedent — pins fragile UI identifiers ahead of time.
- D-08 revision (QSettings → session JSON) is correct and well-implemented in Plan 96-04.

### Agreed Concerns (raised by both reviewers — highest priority for replanning)
- **Hebrew/RTL string discipline** (Gemini LOW · Codex LOW) — new `Prev`/`Next`/`View All`/`Per page`/`page/chunk` strings need translation review, not only inline `CURRENT_LANG` toggles. Gemini specifically notes `דף` vs `עמוד` for printed books.
- **UI thread / I/O cost on folder walks** (Gemini LOW · indirectly Codex MEDIUM via "vague wiring in 96-06") — the tree-population path is single-threaded and may stutter on large folders.

### Divergent Views (worth investigating)
- **Overall risk:** Gemini LOW vs Codex HIGH. The divergence is driven by Codex's HIGH-severity findings that Gemini did not surface.
- **Regex non-match handling (D-F5):** Codex argues D-04 ("regex-aware LOCAL, same two-phase model as Genizah") requires non-matches to be FILTERED OUT, not shown with empty highlight. Gemini accepts the current plan (fallback to `content[:200]`). This is a real architectural question — neither D-F5 nor the plan's Step 2 explicitly says which side D-04 lands on. Worth a quick user decision before re-plan.
- **xfail-vs-skip scaffolding:** Codex suggests `xfail(strict=True)` over `pytest.skip` so failures surface immediately when implementing plans ship. Gemini cites the current xfail+audit pattern as a "professional TDD pattern that prevents skip-rot." Both are defensible — the question is whether Phase 96 wants late-but-batched audit (current plan) or eager-flip (Codex's preference).

### Codex-only HIGH-severity findings (not seen by Gemini — DO NOT lose)
1. **Plan 96-06 `_commit_changes()` global-set rebuild** — would erase opt-outs in folders other than the currently displayed one. This is a real bug. **Must fix before execution.**
2. **D-F5 regex-aware semantics** — current plan ships fallback-display for non-matches; D-04 may require filter-out. Decide explicitly and either fix the plan or revise D-04.
3. **BLOCKER-4 integration test scope** — the test calls `_query_local_index(...regex=regex...)` directly so it can't catch "main merge call forgets `regex=regex`". Needs an AST guard or call-site monkeypatch.
4. **96-08 Browse HTML escaping** — `text.replace("\n", "<br>")` without `html.escape` on local file content. **Must fix before execution.**

### Codex-only MEDIUM findings worth absorbing
- Plan 96-08 should open the clicked hit's `p_num` in Browse per-page mode, not always page 1.
- View-All toggle should set `_local_browse_current_sys_id` so toggling back works.
- Browse nav should use `get_local_browse_page(..., next_prev=offset)` not `p_num + offset` (the latter fails on blank-page skips).
- D-F4 "good PDF stays blocks" test should monkeypatch `get_text` to PROVE fallback wasn't called.
- Opt-out path canonicalization needs explicit Windows-aware tests (mixed case + slash/backslash).
- Plan 96-06 wiring is still vague despite the 96-08 precedent — needs its own pinned-identifier pass.
- Shell validation snippets (`grep`, `tail`, `test -f`, `/tmp/...`) are not Windows-PowerShell-safe.

### Recommended Next Action
Run `/gsd-plan-phase 96 --reviews` to fold the agreed concerns and Codex-only HIGH findings back into the plans. The 4 HIGH-severity Codex findings + the D-F5 regex-semantics question are the load-bearing items — they would otherwise ship as silent regressions behind a green test suite.
