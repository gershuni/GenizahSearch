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
detail: `_on_vs_toggle_change` (web/pages/joins_lab.py:1300) is wired (:1894) and re-renders. Leading hypothesis: `_vs_candidates` returns empty in the WEB runtime (Visual Similarity / `visual_similarity.db` ~1.3GB was historically a desktop asset; may be unprovisioned/unwired on web) → merge is a no-op or the fetch silently fails. Needs empirical check of VS data availability + the `_do_vs_fetch_and_update` path. (Awaiting Codex confirmation.)

### G3 — Y/?/N triage feedback lags (only updates after another action), grid + Compare
status: failed
detail: `_make_triage_handler` (candidate_grid.py:673-685) calls `restyle_fn` which (`_restyle_all`, :506-528) repaints only the card BORDER. The triage button active-FILL is set once at render (:668-671) and never updated on click, so the obvious feedback appears only when a later action rebuilds the grid. Fix: update the button fill on click (or re-render the triage row). Verify the Compare verdict path (compare_modal.py:318) for the same issue.

### G4 — Clicking the candidate image does not open Compare
status: failed
detail: `img_el` (candidate_grid.py:598) has only `.on("error", ...)`; no click handler. Add `img_el.on("click", <compare>)` + `cursor:pointer` calling `on_compare(cand)`.

### G5 — Images do not load in Compare (both panes blank)
status: failed
detail: `create_compare_modal` builds `AnchorViewer(sys_id, p_num, volume_ie)` (compare_modal.py:305, 369) but never awaits `AnchorViewer.update_content()` (anchor_viewer.py:496) — `__init__`→`_build_ui()` builds only the skeleton. The working page awaits update_content (joins_lab.py:~1103). Fix: schedule/await `update_content` for the anchor pane (modal build) and candidate pane (`_fill_candidate`) via an async path / background task, honoring AnchorViewer's `_nav_gen` latest-wins guard.

> Root theme: the headless suite (169 passing) never exercises the NiceGUI async render path — same blind spot that hid the 5 earlier criticals. A render-smoke harness is the durable fix. (Codex diagnosis pending: `_tmp/codex-119-uat-output.md`; may surface additional defects.)
