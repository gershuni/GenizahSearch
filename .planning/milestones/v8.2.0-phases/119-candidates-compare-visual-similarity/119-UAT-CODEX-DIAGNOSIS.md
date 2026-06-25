**Issue Verdicts**
1. **Confirmed.** Cards never render `snippet/full_text`; Compare never passes highlight HTML into `render_line_numbered_html`.
Root lines: [candidate_grid.py:619](C:/Genizahsearch/web/components/candidate_grid.py:619), [anchor_viewer.py:603](C:/Genizahsearch/web/components/anchor_viewer.py:603), [typography.py:66](C:/Genizahsearch/web/components/typography.py:66).
Minimal fix: render a small `ui.html(..., sanitize=False)` snippet inside `_create_candidate_card` after title/badge, using existing escaped helpers (`snippet_html/htmlify`) and only for the current page slice. Do not fetch text per card. For Compare, add optional `highlight_pattern` to `AnchorViewer`, build escaped `highlight_html`, and pass it to `render_line_numbered_html`.

2. **Partly confirmed, but leading cause incomplete.** Web expects VS data: service auto-loads `fist_data/visual_similarity.db`, and web API exposes VS routes. The stronger source bug is state recompute: `_re_render_candidates_surface()` filters `_all_candidates` without reapplying VS, and Step 9 stores already-merged display candidates as `_all_candidates`.
Root lines: [joins_lab.py:634](C:/Genizahsearch/web/pages/joins_lab.py:634), [joins_lab.py:1847](C:/Genizahsearch/web/pages/joins_lab.py:1847), [joins_lab.py:389](C:/Genizahsearch/web/pages/joins_lab.py:389).
Minimal fix: keep raw text candidates as the baseline, centralize “current display candidates” as `_apply_vs_merge(raw_text, _vs_candidates, _vs_on, builder_has_query)`, and use that helper in search, toggle, filter, pagination, and enrichment re-render. Also stop swallowing VS-unavailable as “no data”; surface “VS unavailable” and disable or annotate the toggle if `is_available()` is false.

3. **Grid confirmed; Compare partly.** Grid updates only the card border; button active fill is computed once. Compare buttons are command buttons and never reflect current verdict state at all.
Root lines: [candidate_grid.py:667](C:/Genizahsearch/web/components/candidate_grid.py:667), [candidate_grid.py:520](C:/Genizahsearch/web/components/candidate_grid.py:520), [compare_modal.py:417](C:/Genizahsearch/web/components/compare_modal.py:417).
Minimal fix: keep render-local refs for the three card triage buttons and update their styles in `_make_triage_handler` alongside `_rf(sid, t)`. `.style()` should push in NiceGUI, but an explicit `.update()` is harmless. For Compare, either accept command-only behavior or add verdict-button refs and refresh state after `_fill_candidate`.

4. **Confirmed.** Image has only error handling; Compare is only on the button.
Root lines: [candidate_grid.py:598](C:/Genizahsearch/web/components/candidate_grid.py:598), [candidate_grid.py:715](C:/Genizahsearch/web/components/candidate_grid.py:715).
Minimal fix: wrap image/placeholder in a clickable container or add `img_el.on("click", _make_compare_handler())` plus `cursor:pointer`. Prefer wrapper if the error placeholder should also remain clickable. No bubbling issue in current markup.

5. **Confirmed.** Compare creates `AnchorViewer` skeletons and never awaits `update_content`; the page path does await it.
Root lines: [compare_modal.py:305](C:/Genizahsearch/web/components/compare_modal.py:305), [compare_modal.py:369](C:/Genizahsearch/web/components/compare_modal.py:369), [joins_lab.py:1172](C:/Genizahsearch/web/pages/joins_lab.py:1172).
Minimal fix: keep `create_compare_modal` sync, store viewer refs, attach an async `dialog.on("show", ...)` loader that awaits anchor + candidate `update_content()`. Make Prev/Next/verdict handlers async and await candidate-pane reloads. Avoid naked `ensure_future`; this repo already has NiceGUI client-context regressions from that pattern. `AnchorViewer.update_content()` itself is off-loop via `run.io_bound`.

**Additional Defects**
- Compare anchor is always page 1 and lacks shelfmark: [joins_lab.py:589](C:/Genizahsearch/web/pages/joins_lab.py:589). Store anchor page/shelfmark in `_anchor_state` during `load_anchor()` and pass them into `anchor_cand`.
- Table mode is dead code: `_view_mode` exists, `create_candidate_table()` exists, but render always calls grid: [joins_lab.py:487](C:/Genizahsearch/web/pages/joins_lab.py:487), [joins_lab.py:670](C:/Genizahsearch/web/pages/joins_lab.py:670), [candidate_grid.py:812](C:/Genizahsearch/web/components/candidate_grid.py:812).
- Size-mismatch logic often lacks anchor dimensions because enrichment batches only candidate IDs; Compare has the same dependency: [candidate_grid.py:250](C:/Genizahsearch/web/components/candidate_grid.py:250), [joins_lab.py:619](C:/Genizahsearch/web/pages/joins_lab.py:619), [compare_modal.py:295](C:/Genizahsearch/web/components/compare_modal.py:295). Include anchor sys_id in enrichment.
- VS-only candidates are metadata-poor (`?` shelfmark/page-1 behavior) because local VS mapping only sets sys_id/rank/score, unlike the API enrichment path: [joins_lab.py:311](C:/Genizahsearch/web/pages/joins_lab.py:311), [web/api.py:2266](C:/Genizahsearch/web/api.py:2266).

**Recommendation**
Add a real NiceGUI render-smoke/UAT test, not just signatures: load Joins Lab with mocked anchor/search, render cards, click image Compare, wait for both panes to leave skeleton state, click triage, toggle VS, and assert candidate count/content changes. The existing cold construct test does not cover these async render defects.
