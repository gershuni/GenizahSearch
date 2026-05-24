# Codex Cross-AI Code Review — Phase 96 Final

## Context

Phase 96 closes a polish + deferred-feature wave on top of v7.14.0 (My Library local document search). After 6 iterations of fixes to the LOCAL navigation, opt-out tree, and View-All rendering, the user is requesting an independent code review before phase close.

## Components changed in Phase 96

**Wave 1 (foundations):**
- `tests/fixtures/local_indexer/single_word_per_line.pdf` — D-F4 pathological PDF regression fixture
- `scripts/generate_single_word_fixture.py` — fixture generator (scrambled content-stream Tj order)
- `shared/local_indexer.py` — `_detect_single_word_per_line` heuristic + detect-then-fallback in PDF extractor; `_prune_optouts_to_disk` rescan helper
- `genizah_core.py` — `_build_local_result_dict` normalized to Genizah hit shape (D-F5 LOCAL highlight); D-04.1 filter-out for regex non-matches; new `get_local_browse_page` LOCAL nav primitive
- `genizah_app.py` — `self._local_file_optouts: set[str]` persistence via session JSON; `_apply_local_optout_filter`

**Wave 2 (cascade):**
- `genizah_app.py` — opt-out filter wired into both Phase 95 three-state LOCAL filter cascade joinpoints

**Wave 3 (UI):**
- `desktop/my_library_tab.py` — `_OptoutTreeWidget` + later replaced by `_UnifiedFileTreeWidget` (single tree with Filename/Pages/Status columns; tri-state checkboxes)
- `desktop/result_dialog.py` — removed redundant `btn_rd_open_browse` LOCAL button

**Wave 4 (nav):**
- `desktop/result_dialog.py` — `load_local_page` dispatch + spinbox/nav button wiring
- `genizah_app.py` — Browse-panel LOCAL nav widgets; per-page vs View All; session-persisted view mode

**Wave 5 (polish, 6 iterations):**
1. UX redesign + persistence + spinner autoDefault + img field + LOCAL widget leak
2. Timing race fix + persistence flush_pending + missed autoDefault buttons + img to display sub-dict
3. Blanket findChildren autoDefault loop + View All btn_b_all disable + img field re-route
4. Codex-prescribed rework: setKeyboardTracking + returnPressed + setFocus removal + p_num vs current_idx contract fix + max_p_num return + get_local_browse_page returns None on missing
5. ResultDialog LOCAL file-path label + Browse nav button consolidation + per-page line restart (initial attempt)
6. Per-page line restart actual fix (one-`<p>`-per-page instead of one-big-`<div>`) + 200-page View All cap warning dialog

## Specific review questions for Codex

A. **Is the LOCAL navigation contract clean?** `p_num` (sparse physical page) vs `current_idx` (dense ordinal) vs `max_p_num` — is this confusing? Should it be refactored into a single page-list-iterator object?

B. **Is `_UnifiedFileTreeWidget` (the merged opt-out + status tree in `desktop/my_library_tab.py`) sound?** Concerns: (1) does `populate_for_folder` block on filesystem walks for large folders? (2) does the SET-DIFFERENCE/UNION opt-out algebra correctly scope to displayed paths? (3) is the timing-race fix (300ms QTimer.singleShot wrapping auto-select) robust, or is it papering over a deeper init-order problem?

C. **Are the Browse-panel button-dispatch (LOCAL vs Genizah mode) and the View All flag (`btn_b_all` checked/disabled state) coherent?** After iteration 5 consolidated the LOCAL nav into existing Genizah buttons, is the handler dispatch via `_is_browsing_local()` (or whatever flag is used) reliable? Or are there races where a LOCAL→Genizah switch could leave the buttons in the wrong handler?

D. **Is the line-numbering implementation robust?** The fix in iter 6 changed the View All HTML from `<div>...<br>...<br>...</div>` to `<div><p>page1</p><p>—page2—</p><p>page2</p></div>` so each page is its own QTextBlock. Is this correct Qt usage? Is the page-text-matching reliable, or could a page whose first line happens to match a separator pattern cause confusion?

E. **Tech debt review:** are there leftover dead code paths from the multi-iteration churn (e.g., old `_OptoutTreeWidget` references, deleted `btn_local_browse_*` widgets that still have ghost code, fix-N comment salt)?

F. **Test coverage:** the phase has `test_local_optout_persistence.py`, `test_local_optout_filter.py`, `test_local_nav_codex_fix4.py`, `test_local_hit_highlighting.py`, `test_local_pdf_extraction_fallback.py`, `test_local_filter_cascade.py`, `test_local_nav_page_chunk.py`, `test_local_browse_panel.py`, `test_result_dialog_local_button_removed.py`, `test_my_library_tab.py`. Are critical paths covered, or are there blind spots (e.g., the 200-page cap path, the LOCAL-vs-Genizah Browse dispatch, the file-path label visibility)?

G. **Optimal?** Could the View All rendering be simpler? Could the opt-out scope-tracking be replaced by a simpler full-set update if we also persist the full set rather than the scope-diff?

## Output format

Please provide:
1. **Strengths** — 3-5 bullets on what's well-built
2. **Problems** — concrete bugs or concerning patterns, with file:line references where possible
3. **Optimality** — is the code as clean as it could be? Top 3 refactor suggestions
4. **Phase close readiness** — fit-to-ship verdict (ship-as-is / one more polish needed / blocker-fix-then-ship)

— end brief —
