---
status: partial
phase: 107-desktop-join-workbench-anchor-entry-points-actions-join-model
source: [107-VERIFICATION.md]
started: 2026-06-04T10:00:00Z
updated: 2026-06-04T10:00:00Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. ResultDialog → Find joins opens the Workbench
expected: Open a result from the desktop ResultDialog, click "Find joins". JoinWorkbenchWindow opens as a modeless window anchored on the folio you were viewing (showing the anchor's shelfmark, image, and numbered transcription), and the ResultDialog closes.
result: [pending]

### 2. Browse → Find joins opens the Workbench, Browse stays open
expected: Open the Browse tab, load a manuscript, click "Find joins". JoinWorkbenchWindow opens with anchor from current_browse_sid/current_browse_p; the Browse tab is still visible.
result: [pending]

### 3. Single-instance re-anchoring
expected: Click "Find joins" twice on two different fragments. Only ONE Workbench window exists; the second call re-anchors/raises it rather than opening a second window (D-01/D-02).
result: [pending]

### 4. Four-source Known Joins panel
expected: With a fragment that has known joins (user + PGP or FJMS), the Known Joins panel appears with correct per-member rows and source badges (PGP=blue, FJMS=purple, user=green, community=green); the panel is hidden when the fragment has no known joins (setVisible(count>0)).
result: [pending]

### 5. Add as Join → persist → panel refreshes
expected: Click "Add as Join" in the Workbench action row. JoinsDialog opens with Fragment A pre-filled with the anchor and Fragment B empty. Create a join; after closing, _reload_known_joins fires and the new join appears in the Known Joins panel (SC#4).
result: [pending]

### 6. Dark mode + Hebrew bilingual rendering
expected: With a dark-mode desktop theme active, the anchor image area has a dark loading background (#374151), the ANCHOR tag is teal (#14b8a6), and Hebrew strings display correctly under lang=he. No hardcoded English text visible.
result: [pending]

### 7. Zoom + folio nav without side effects
expected: Zoom in/out on the anchor image rescales without re-fetching from network; folio prev/next pages the SAME fragment without reloading the Known Joins panel (D-07).
result: [pending]

## Summary

total: 7
passed: 0
issues: 6
pending: 7
skipped: 0
blocked: 0

## Gaps

UAT round 1 (2026-06-04, Hillel) surfaced 6 refinements to the workbench shell.
All addressed in code; pending live re-test.

### G1. Anchor image should default to showing the ENTIRE fragment
status: resolved-in-code
fix: `_fit_to_view()` computes a fit-to-viewport zoom on the first image of each
anchor (`_fit_pending` set in `set_anchor`, consumed in `_on_img`). Folio nav /
manual zoom keep the user's current zoom.

### G2. Known-joins panel belongs on the LEFT, under the anchor text (scrollable)
status: resolved-in-code
fix: panel construction moved from `_build_right_pane` to `_build_joins_panel()`,
added to the left anchor pane under the transcription browser. Right pane reserved
for the Phase-108 candidate hunt. Internal QScrollArea (maxHeight 320) preserved.

### G3. Joins-context button (like ResultDialog) that shows the joins
status: resolved-in-code
fix: `btn_joins_context` in the joins-panel header opens a QMenu of connected
members (`_show_joins_context_menu`); selecting one re-anchors the workbench.
NOTE: implemented as a re-anchor dropdown (closest match to ResultDialog's joins
button) — confirm this is the intended behavior vs. opening the full JoinsDialog.

### G4. Anchor text + line numbering must be right-aligned (RTL)
status: resolved-in-code
fix: `anchor_text_browser.setLayoutDirection(RightToLeft)` — the line-number
gutter's `_reposition_gutter` already moves the gutter to the right (leading) edge
under RTL, so text and numbers both right-align.

### G5. "Add selected to puzzle" checkbox + add-only-one option
status: resolved-in-code
fix: per-row select checkbox + Select-All + "Add selected to puzzle" button in the
joins panel; the existing per-row 🧩 button remains the add-only-one path.

### G6. Adding to puzzle auto-adds the anchor (dedup if already present)
status: resolved-in-code
fix: `puzzle_add_targets()` (pure, unit-tested) always pins the anchor exactly once,
deduped; both single-add and add-selected route through it via the public
`open_anchor_in_puzzle` (SC#5). Puzzle canvas also dedups by (sys_id, folio_label).

---

UAT round 2 (2026-06-04, Hillel) — refinements on the round-1 build.

### G7. Anchor image should pan with the cursor when zoomed
status: resolved-in-code
fix: `_PannableScrollArea` (drag-to-pan, hand cursor); image scroll switched to
`widgetResizable=False` + center alignment so a zoomed pixmap overflows and scrolls;
`_apply_zoom` resizes the label to the scaled pixmap.

### G8. Transcription text must be right-aligned
status: resolved-in-code
fix: `_right_align_anchor_text()` merges AlignRight block format across the whole
document after each text load (belt-and-suspenders over htmlify's RTL div).

### G9. Joins-context button = chain icon + dropdown triangle
status: resolved-in-code
fix: button text now "🔗 ▾" (icon + triangle); label moved to tooltip/accessibleName.

### G10. Joins section collapsed by default, click "Known Joins" to open, resizable, default half height
status: resolved-in-code
fix: left pane is a vertical QSplitter (image / text / joins). Joins body hidden by
default; the clickable "▸ Known Joins (N)" header toggles it (`_toggle_joins_body`);
expanded → splitter gives joins ≈ 50% of the height; all three panes drag-resizable.

### G11. Puzzle window should show the whole fragment by default (general issue)
status: resolved-in-code
fix: single-fragment add in `desktop/puzzle.py::_on_image_loaded` now calls
`_fit_all_fragments()` instead of `ensureVisible` so the whole fragment is framed.
NOTE: this is a general puzzle-window change, not strictly Phase 107.
