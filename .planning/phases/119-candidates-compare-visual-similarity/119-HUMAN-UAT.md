---
status: issues_found
phase: 119-candidates-compare-visual-similarity
source: [119-VERIFICATION.md]
started: 2026-06-19T00:00:00Z
updated: 2026-06-19T00:00:00Z
---

## Current Test

[UAT complete — 5 defects found, see Gaps]

## Tests

### 1. Candidate grid visual layout
expected: Open `/joins-lab`, load an anchor (sys_id), run a search with 1+ result lines. Grid renders up to 24 cards per page; each card shows a ≈160×160 image-first thumbnail, library chip, shelfmark, title, 👁 badge (if via_vs), Yes/Maybe/No triage buttons, a "View in Browse" link, and a "Compare fragment" button; Prev/Next pagination appears when more than 24 candidates.
result: ISSUE — grid renders, but cards show NO transcription text / highlighted search terms (G1); clicking the image does not open Compare (G4); Y/?/N triage feedback lags (G3).

### 2. Grid↔table triage consistency + bulk-triage bar
expected: Toggle Grid↔Table. Table shows 8 columns (Shelfmark, Score, Snippet, Material, Dimensions, Page, Triage, select); is sortable; multi-select works; selecting rows reveals a "Mark N selected as: Yes/Maybe/No" bulk bar; verdicts set in grid show the same in table (and vice-versa).
result: [pending]

### 3. Filter dialog enrichment gate + apply behavior
expected: Open Filters. Material multi-select starts disabled with a "Loading…" note; after enrichment completes it populates with available materials; applying a material filter re-renders with fewer candidates and resets to page 1; size-mismatch exclusion removes mismatched candidates.
result: [pending]

### 4. Compare modal per-pane independence + card restyle after verdict
expected: Click "Compare fragment". Full-screen modal opens with anchor image left, candidate image right (two independent AnchorViewers); the candidate pane navigates folios without moving the anchor pane; recording "Yes" gives the grid card a green border and auto-advances to the next candidate.
result: ISSUE — images do NOT load in either Compare pane (G5); no highlighted transcription text shown (G1); triage feedback lags (G3).

### 5. VS toggle: intersection mode + empty-builder union mode
expected: Toggle the 👁 Visual Similarity switch ON with a query active — displayed candidates narrow to the text∩VS intersection (fewer, each 👁-badged) and the count notice updates. Then clear the builder with VS ON — the pure VS union renders look-alikes with NO "Enter at least one search line" toast (F1 empty-builder branch).
result: ISSUE — toggling VS on/off shows the SAME candidate set; no observable change (G2).

### 6. Re-anchor invalidation + VS refetch
expected: Load a fresh anchor after triaging some candidates — all triage verdicts clear (no verdict borders on any card); if VS was ON, look-alikes refetch for the new anchor and the loading notice appears briefly.
result: [pending]

## Summary

total: 6
passed: 0
issues: 3
pending: 3
skipped: 0
blocked: 0

## Gaps

### G1 — No transcription text / highlighted search terms on grid cards or Compare panes
status: failed
detail: `_create_candidate_card` (web/components/candidate_grid.py:585-719) never renders `cand.snippet`/`cand.full_text` and applies no highlight. Compare panes show no text because `AnchorViewer.update_content` is never awaited (see G5); even once fixed, the candidate's `highlight_pattern` is not applied over the AnchorViewer transcription. User wants matched terms highlighted on the card and on both Compare sides.

### G2 — Visual Similarity toggle shows the same candidates on and off
status: failed
detail: REVISED root cause (Codex). NOT a data-availability issue — `fist_data/visual_similarity.db` (1.3GB) is present and web auto-loads it + exposes VS API routes. The real bug is **baseline-candidate pollution**: Step 9 stores the ALREADY-MERGED `display_candidates` into `_all_candidates` (joins_lab.py:1848-1849), and `_re_render_candidates_surface` (joins_lab.py:634) re-filters that merged set WITHOUT reapplying `_apply_vs_merge` from a raw text baseline — so toggling can't cleanly switch text-only ↔ intersection/union. Fix: keep RAW text candidates as the baseline and centralize display = `_apply_vs_merge(raw_text, _vs_candidates, _vs_on, builder_has_query)` applied consistently in search/toggle/filter/pagination/enrichment re-render. Also surface "VS unavailable" + disable the toggle when `vs_service.is_available()` is false (ops: confirm the 1.3GB db ships to the web server).

### G3 — Y/?/N triage feedback lags (only updates after another action), grid + Compare
status: failed
detail: `_make_triage_handler` (candidate_grid.py:673-685) calls `restyle_fn` which (`_restyle_all`, :506-528) repaints only the card BORDER. The triage button active-FILL is set once at render (:668-671) and never updated on click, so the obvious feedback appears only when a later action rebuilds the grid. Fix: update the button fill on click (or re-render the triage row). Verify the Compare verdict path (compare_modal.py:318) for the same issue.

### G4 — Clicking the candidate image does not open Compare
status: failed
detail: `img_el` (candidate_grid.py:598) has only `.on("error", ...)`; no click handler. Add `img_el.on("click", <compare>)` + `cursor:pointer` calling `on_compare(cand)`.

### G5 — Images do not load in Compare (both panes blank)
status: failed
detail: `create_compare_modal` builds `AnchorViewer(sys_id, p_num, volume_ie)` (compare_modal.py:305, 369) but never awaits `AnchorViewer.update_content()` (anchor_viewer.py:496) — `__init__`→`_build_ui()` builds only the skeleton. The working page awaits update_content (joins_lab.py:~1103). Fix: schedule/await `update_content` for the anchor pane (modal build) and candidate pane (`_fill_candidate`) via an async path / background task, honoring AnchorViewer's `_nav_gen` latest-wins guard.

### G3-compare — Compare verdict buttons never reflect current verdict state
status: failed
detail: (Codex extension of G3) The Compare modal's verdict buttons (compare_modal.py:417) are command-only — they never show which verdict is active for the current candidate. Add verdict-button refs + refresh after `_fill_candidate`.

### A1 — Compare anchor pane is always page 1 and lacks shelfmark
status: failed
detail: (Codex, new) `anchor_page_num = 1` is hardcoded when building `anchor_cand` (joins_lab.py:589) and the anchor shelfmark isn't carried. Store the anchor's resolved page + shelfmark in `_anchor_state` during `load_anchor()` and pass them into `anchor_cand`.

### A2 — Table view is dead code (grid↔table toggle can't reach the table)
status: failed
detail: (Codex, new — CONFIRMED) `_view_mode` (joins_lab.py:487) and `create_candidate_table()` exist, but the render path only ever calls `create_candidate_grid` (joins_lab.py:670). The table is never rendered, so UAT item 2 (grid↔table) cannot switch views. Wire the render to honor `_view_mode` and call `create_candidate_table()` when 'table'.

### A3 — Size-mismatch flag lacks anchor dimensions
status: failed
detail: (Codex, new) Enrichment batches only CANDIDATE sys_ids, not the anchor's, so `is_size_mismatch(cand_w, anchor_w)` (candidate_grid.py:250, compare_modal.py:295) usually has no `anchor_w`. Include the anchor sys_id in the enrichment batch.

### A4 — VS-only candidates are metadata-poor (`?` shelfmark / page 1)
status: failed
detail: (Codex, new) `_map_vs_suggestions_to_candidates` (joins_lab.py:311) sets only sys_id/rank/score, unlike the API enrichment path (web/api.py:2266). Pure-VS rows render with `?` shelfmark and default page. Enrich VS-only candidates with shelfmark/title/page like the API path.

### TEST-INFRA — render-smoke harness required
detail: The headless suite (169 passing) never exercises the NiceGUI async render path — the same blind spot that hid the 5 earlier criticals AND all 9 defects here. Codex recommendation: add a real render-smoke/UAT test that loads Joins Lab with a mocked anchor/search, renders cards, clicks image→Compare, waits for both panes to leave skeleton state, clicks triage, toggles VS, and asserts the candidate set actually changes. Signature/contract tests are insufficient.

> Full Codex diagnosis: `_tmp/codex-119-uat-output.md`. 9 defects (G1-G5 + G3-compare + A1-A4) + render-smoke test → gap-closure phase.
