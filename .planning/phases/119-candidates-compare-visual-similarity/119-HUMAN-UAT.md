---
status: issues_found
phase: 119-candidates-compare-visual-similarity
source: [119-VERIFICATION.md, live-uat-round2-2026-06-19]
started: 2026-06-19T00:00:00Z
updated: 2026-06-19T12:00:00Z
---

## Current Test

[Round 1 (G1-G5/A1-A4 + TEST-INFRA) RESOLVED by plans 119-05/06/07/08. Round 2 (live Compare UAT 2026-06-19) found 10 new gaps R2-1..R2-10 — see Gaps. Round-2 → gap-closure.]

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

total: 16
passed: 0
round1_resolved: 10
round2_open: 10
issues: 10
pending: 3
skipped: 0
blocked: 0

## Gaps

### Round 1 — RESOLVED by plans 119-05/06/07/08 (verified green: 281 component + 7 render-smoke tests)

- **G1** (no transcription/highlight on cards + Compare) — status: resolved (119-05 cards, 119-06 Compare line-safe highlight via `highlight_pattern`)
- **G2** (VS toggle shows same set — baseline pollution) — status: resolved (119-07 raw-baseline `_compute_display_candidates`)
- **G3** (triage feedback lags, grid) — status: resolved (119-05 per-card button-fill on click)
- **G3-compare** (Compare verdict buttons don't reflect current) — status: resolved (119-06 `_refresh_verdict_buttons` on post-advance candidate)
- **G4** (image click → Compare) — status: resolved (119-05 `img_el.on("click")` + cursor:pointer)
- **G5** (Compare panes blank — `update_content` never awaited) — status: resolved (119-06 `dialog.on("show")` awaits both panes + latest-wins gen counter)
- **A1** (Compare anchor always page 1 / no shelfmark) — status: resolved (119-07 `_anchor_state` page+shelfmark)
- **A2** (table view dead code) — status: resolved (119-07 `_view_mode` render branch + Grid/Table toggle)
- **A3** (size-mismatch lacks anchor dims) — status: resolved (119-07 anchor sys_id in enrichment batch)
- **A4** (VS-only metadata-poor) — status: resolved (119-07 `run_vs_meta_core` off-loop enrichment)
- **TEST-INFRA** (render-smoke harness) — status: resolved (119-08 NiceGUI `User` sim, manual `asyncio.run` path, 7 tests, no new dependency)

> Round-1 full Codex diagnosis preserved in git history + 119-05/06/07/08 SUMMARY.md. `_tmp/codex-119-uat-output.md`.

### Round 2 — live Compare UAT 2026-06-19 (NEW; → gap closure). All in `web/components/compare_modal.py` / `candidate_grid.py` / `anchor_viewer.py` + TRANSLATIONS.

### R2-1 — Hebrew not fully translated in Compare (CANDIDATE/ANCHOR/MAYBE/NEXT/PREV)
status: failed
detail: `tr()` keys are present (compare_modal.py:465/482/502/524/533-535/550) but the Hebrew TRANSLATIONS entries are MISSING for "Candidate", "Anchor", "Maybe", "Next ›", "‹ Prev" (Yes/No/Compare DO translate → כן/לא/השווה). Fix = add the missing HE keys (scanner `_tmp/find_missing_tr2.py`; ref `reference_i18n_audit_method`). NOTE: the English description line "Biblical Exegesis ; On offerings…" is English source DATA (domains/desc) — confirm whether translatable before touching; likely out of scope.

### R2-2 — Next/Prev arrows confusing in RTL + top-center counter shows "118 / 5"
status: failed
detail: RTL bidi. Nav buttons (compare_modal.py:524 `‹ Prev`, :550 `Next ›`) read wrong in HE (correct in EN). The center counter (`_counter_label_ref`, :257) shows "118 / 5" = bidi-flipped "5 / 118" (candidate 5 of 118). Fix = bidi-isolate / force-LTR the counter and correct arrow direction/placement under RTL.

### R2-3 — Images dominate Compare window; transcription text not visible
status: failed
detail: The AnchorViewer image panes consume nearly all vertical space, pushing transcription off-screen. Cap the Compare image height (e.g. max-height / flex balance / scroll) so both image AND text are visible per pane.

### R2-4 — Replace Yes/Maybe/No text with V / ? / X icons (desktop parity)
status: failed
detail: compare_modal.py:533-535 + candidate_grid triage buttons render text. Use ✓ / ? / ✗ icon buttons matching the desktop app (also eliminates the R2-1 MAYBE leak). Keep the green/yellow/red color coding.

### R2-5 — Show verdict state as light green/yellow/red border in Compare
status: failed
detail: When the current candidate has a verdict, show it as a light green (yes) / yellow (maybe) / red (no) border on the candidate pane in the Compare window (mirrors the grid card border).

### R2-6 — Shelfmark appears twice in Compare window
status: failed
detail: The modal renders the shelfmark as the green column subtitle AND the inner AnchorViewer header also renders it (bold) — duplicate on both panes. Suppress one (pass a flag to AnchorViewer to skip its shelfmark header in Compare, keep the green column header).

### R2-7 — Esc should close the Compare window
status: failed
detail: Add a keyboard handler so Escape closes the Compare modal (NiceGUI dialog keydown / `ui.keyboard`).

### R2-8 — Show transcription beginning for VS-only / no-text-search candidates
status: failed
detail: When VS is on with no text query (or VS-only candidates), cards/Compare show no transcription. Show the beginning of the transcription text instead of blank.

### R2-9 — Browse + Compare as icon-only buttons + tooltip, in the X/?/V row
status: failed
detail: Replace the text "View in Browse" / "Compare fragment" controls with browse + compare ICON buttons (tooltips), placed in the same row as the ✓/?/✗ triage buttons (candidate_grid card actions).

### R2-10 — Table view has white background in dark mode
status: failed
detail: The candidate table view (candidate_grid.py table render) does not respect dark mode — renders white bg. Apply dark-mode-aware styling.

## Deferred to next phase (NEW features — user-flagged 2026-06-19; seeded, NOT gap-closure)

- Stop search with partial results
- "Make an anchor" (promote candidate/fragment to anchor)
- Show the anchor's joins
- "Add as join" button
- Browse-in-Compare
- Info buttons (catalog + bibliography) in Compare
