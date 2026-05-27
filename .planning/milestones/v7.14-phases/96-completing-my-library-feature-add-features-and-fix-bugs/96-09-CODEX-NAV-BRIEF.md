# Codex Cross-AI Critique Brief — Phase 96 Navigation Bugs (Iteration 4)

## Context

Phase 96 is closing as a polish + deferred-feature wave on top of v7.14.0 (My Library local document search). Three iterations of fixes on the navigation code have left the user reporting **persistent erratic behavior**. We are asking for an independent root-cause analysis from Codex before iteration 4.

## What's known

### Components touched

- `desktop/result_dialog.py` — Result Dialog where LOCAL hits open. Has `spin_page` (QSpinBox for page jump), `btn_res_prev`/`btn_res_next` (prev/next RESULT), `btn_pg_prev`/`btn_pg_next` (prev/next PAGE), and `btn_compact_pg_prev`/`btn_compact_pg_next` (compact-mode duplicates). View-Transcription "Browse" button is the first visible QPushButton in the action row.
- `genizah_app.py` — Browse panel where Browse-from-hit lands. Has `btn_local_browse_prev`/`btn_local_browse_next`/`lbl_local_browse_page`/`btn_local_browse_view_toggle` widgets added in Plan 96-08, plus pre-existing `btn_b_all` (View All).
- `genizah_core.py` — `_build_local_result_dict` builds LOCAL hit dicts. Has top-level `p_num` and `img` and a `display` sub-dict.

### Iterations 1–3 history

**Iteration 1 (commit `e0ee9156`):**
- Replaced QSplitter[opt-out tree | status table] with single `_UnifiedFileTreeWidget`.
- Added `flush_pending()` for closeEvent.
- Set `autoDefault=False` on `btn_res_prev` + `btn_res_next` only.
- User found: spinner Enter still wrong (39→40, 353→36, 402→41), Browse opens at page 1, "open file" button persists.

**Iteration 2 (commit `25f43763`...`f06476bb`):**
- Identified missed buttons: also set `autoDefault=False` on `btn_compact_pg_next/prev` and `btn_pg_next/prev`.
- Added `setFocus()` on `spin_page` after loading LOCAL hit.
- Added `'img': p_num` at top-level of LOCAL hit dict.
- Added `browse_open_file_btn.setVisible(False)` directly in `browse_load()`.
- User found: Enter NOW opens Browse tab (wrong direction). View All now shows "דף לא נמצא" (regression we introduced). Nav back/forward image is "skipping (a lot or some) or going to the other direction".

**Iteration 3 (commit `afe4911b`, `3f54f986`):**
- Used `self.findChildren(QPushButton)` to set `autoDefault=False` on ALL buttons (covers `btn_view_transcription` Browse button and 24+ others).
- Made `_open_local_browse` and `_open_local_browse_page` disable+uncheck `btn_b_all`.
- Moved `img` field from top-level to inside `display` sub-dict (where the render loop reads it).

**User verification after iteration 3:**
> Pages nav still erratic. One result shows Img 1552 but opens in 1529. Clicking anywhere in ResultDialog changes the page number. Nav is jumping between numbers and skipping. In Browse Tab next>/<prev are not translated.

## Remaining symptoms (Codex: please diagnose these)

1. **Off-by-23 (or off-by-N variable)**: hit shows `Img = 1552` in results table → opens in page `1529`. Difference is 23. Possibly the LOCAL Tantivy doc's `p_num` field stores absolute-page-within-document but the open-in-Browse path uses chunk-index-within-document (or vice versa). Or `p_num` for LOCAL hits is "1-indexed display" while the Browse panel uses "0-indexed internal".

2. **"Clicking anywhere in ResultDialog changes the page number"**: this is the BIG ONE. Mouse-clicks on areas that should be passive (text widget, label, decorative area) cause `spin_page.value()` to change → triggers `editingFinished` → calls `load_page(target=new_value)` → page changes. Possible cause: a `QWidget` parent has `setFocusPolicy(Qt.ClickFocus)` that propagates focus to whatever is under the cursor, and the SpinBox's clear-on-focus / step-on-focus mis-fires.

3. **Nav jumping between numbers and skipping**: `btn_pg_prev`/`btn_pg_next` calls `load_page(offset=-1/+1)`. If `editingFinished` ALSO fires (because focus shifts to/from spin_page on click), we get TWO calls: one offset, one target. With racing signals the user sees skips. Or `load_page(target=N)` followed by `load_page(offset=+1)` = N+1 (which matches iteration 1 observation: 39→40).

4. **Browse Tab next/prev not translated**: the new `btn_local_browse_next`/`btn_local_browse_prev` from Plan 96-08 likely use literal English `Next ▶` / `◀ Prev` without `tr()` wrapping. The pre-existing Genizah-side `btn_b_next`/`btn_b_prev` ARE translated.

## Specific questions for Codex

A. What is the cleanest, ONE-COMMIT fix for the spinner+nav cluster that:
   - Decouples mouse-clicks from spin_page value changes
   - Decouples Enter-in-spinner from Prev/Next button activation
   - Ensures `load_page(target=N)` jumps to N exactly (not N+1 or N±23)
   - Ensures Prev/Next button = step by 1 exactly

B. For the off-by-23 case: what does `p_num` represent in the LOCAL hit dict vs. what `get_local_browse_page` expects? Is the bug at the producer (`_build_local_result_dict`) or consumer (`get_local_browse_page`/`load_local_page`)?

C. Browse-tab i18n: where should `tr()` be applied for `btn_local_browse_*`? Are they created in `_build_browse_panel` (or similar) in `genizah_app.py`?

D. Did iterations 1–3 introduce technical debt that we should clean up rather than patch over?

## Relevant code excerpts (see /tmp/codex-brief.md)

— end brief —
