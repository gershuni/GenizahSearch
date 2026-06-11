# Phase 108 Join Lab — polish round 2 (spec for implementation)

Approved feature batch (post-redesign UAT). Implement all 7; commit atomically. Read the current code
(`desktop/join_workbench.py`, `genizah_app.py`, `genizah_translations.py`) and the redesign summary
`.planning/phases/108-…/108-REDESIGN-SUMMARY.md` first.

## Locked decisions (from the user)
- Persist the Join Lab **INPUT state only** (anchor + both query builders + triage + filter/view) and
  **RE-RUN the search on restore**. NEVER persist candidate results (mirrors the search_history.json
  778 MB freeze fix).
- Recovery piggybacks on the app's EXISTING session-restore flow (it already asks by default). On the
  user's "restore" confirmation, if the Join Lab was open, REOPEN it restored. Deliberate close also
  retains state so the next open restores it.
- The `< >` folio buttons FLIP the displayed page IN PLACE (do not leave to Browse).

## HARD CONSTRAINTS (do not violate)
- **Never block the UI thread at startup.** The restore must defer the candidate search to the existing
  BACKGROUND worker (`do_search` already launches `SearchThread`). No synchronous search / no heavy work
  in `__init__` or in `_restore_session` on the UI thread. (Prior production freezes came from exactly
  this — see the startup-recovery incidents.)
- Respect the existing `self._restoring_session` guard in `_save_session` (genizah_app.py ~24965) — do
  not write session during the restore window.
- Keep the locked Phase-108 invariants: `build_side_query`/`_responsa_opts`/`compose` output unchanged
  (parser tests green); RR-12 None-page guard; RR-14 ja/flex/bidir merge; D-20 zero `_vs_` calls from
  the workbench; D-06 no dialog-level RTL; every new tr() key registered (no dynamic keys); the window
  must OPEN (construction order).

## Features

### 1. Fix the placeholder tooltip
`desktop/join_workbench.py:793` sets `info_btn.setToolTip(tr("Typed sign legend tooltip"))` and
`genizah_translations.py:3742-3743` maps that placeholder. Replace with a REAL multi-line legend
(register EN→HE). Suggested EN:
`Type signs directly in a word box:\n  #word — grammatical prefixes\n  word# — grammatical suffixes\n  %word — plene/defective spelling\n  *word / word* — wildcard\n  −word — exclude\n(the same options are available via the line's ⚙)`
Provide a faithful Hebrew translation. Remove the old placeholder key/value.

### 2. Prev/Next visibility
Show the pagination row (`btn_prev` / `page_lbl` / `btn_next`, ~1930-1940) ONLY when the filtered
result count spans more than one page, i.e. `len(self.filtered) > _PER_PAGE` (=20). Hidden before any
search and whenever everything fits on one page. Add a `_update_pagination_visibility()` called from the
render path (`render_results` / `_render_grid_page`) and on filter changes. (Interpreting the user's
"more than 10 results" as "more than one page" — the buttons must never be shown when they'd be no-ops.)

### 3. Per-card `< >` folio browse (grid)
On each `CandidateCard`, near the Y/?/N triage, add `◀  p.N  ▶` controls that flip the card's own
thumbnail to the adjacent folio of THAT manuscript IN PLACE. Maintain a per-card current-page index
(init = candidate.page or 1), clamp to ≥1 (and ≤ image count once known), and re-resolve the image via
the window's existing per-page resolver (`JoinWorkbenchWindow._enqueue_image_for_pane(label, sys_id,
page, …)` / `_image_url_for_idx`). Show "p.{n}" (or "{n}/{N}" if the image count is known). None-page
guard (RR-12). Do not refetch on every hover — only on click.

### 4. Per-pane `< >` folio browse (Compare)
In `CompareDialog`, give BOTH panes (anchor side AND candidate side) their own `◀ ▶` folio controls that
flip that pane's image to the adjacent folio of that manuscript IN PLACE (independent of the
candidate-list Prev/Next, which steps to the next CANDIDATE). Reuse `_enqueue_image_for_pane`. Keep the
existing candidate-list Prev/Next and the three compare entry points.

### 5. "Clear" button
Add a "Clear" button in the Join Lab (results toolbar area). It resets the lab: clears the anchor, both
query builders (back to one blank row + default global opts), the candidate list, triage, selection, and
filter; and CLEARS the persisted join_lab session state (so a subsequent restore is empty). Wrap the
label in tr().

### 6. Main-window Joins icon + tighter row
In `genizah_app.py`, the corner icon row builds `corner_puzzle_btn` (🧩) in `corner_layout`
(~3496-3503). Add a `corner_joins_btn` using the link emoji "\U0001F517" (🔗) right after the puzzle
button (with its own `_corner_sep()` separator to match the pattern), tooltip tr("Joins Lab"), same flat
style, that opens the Join Lab via a new no-arg `open_join_workbench()` (see #7). ALSO tighten the
spacing of this icon row: reduce `corner_layout.setSpacing(...)` (find where corner_layout is created)
to a small value (e.g. 2-4 px) so the icons sit closer together.

### 7. Session persistence + recovery
Implement input-only persistence integrated with the existing session flow:
- **JoinQueryBuilder.to_state()/from_state(state):** serialize every row (each box's text, the row's
  `mods` dict, start/end checkbox states, gap value) + the dict-backed global opts (`_global_opts`) +
  page_position. `from_state` rebuilds rows accordingly. Round-trips losslessly.
- **JoinWorkbenchWindow.to_state()/restore_state(state):** capture anchor (sys_id/shelfmark/img/uid),
  this-side + other-side builder states, other-side enabled + AND/OR mode, the triage dict, filter
  controls + view_mode, and an `open` flag. `restore_state` sets the anchor (reuse `set_anchor`),
  rebuilds builders/filters/triage, then triggers `do_search()` (which runs on the BACKGROUND
  SearchThread). NO candidate results saved.
- **genizah_app `_save_session` (~24965-25071):** when `self._join_workbench` exists, add
  `state_dict["join_lab"] = self._join_workbench.to_state()` with `open = self._join_workbench.isVisible()`.
  Because `_save_session` already runs periodically/on events + on app close, this gives crash recovery
  for free (the last save before a crash captured the live lab state). Retain the content even when the
  lab is closed (open=False) so a manual reopen can restore it.
- **genizah_app `_restore_session` (~25083):** on the branch where the user CONFIRMS restoring the last
  session, if `state.get("join_lab", {}).get("open")` is true, reopen the Join Lab and call
  `restore_state(...)` — deferring the search to the background worker (do NOT search synchronously here).
  If `open` is false, do nothing at startup (content stays for a manual open).
- **open_join_workbench()** (new no-arg method): create/show the JoinWorkbenchWindow singleton (mirror
  the existing launcher at ~15430-15436 but WITHOUT requiring a `res`); if persisted `join_lab` content
  exists, call `restore_state(...)` (background search); else open empty (one blank builder row). The
  corner Joins icon (#6) calls this. The existing anchored launcher (`set_anchor(res)` path) is unchanged.
- **Clear** (#5) wipes the persisted join_lab state (write `state_dict["join_lab"] = {"open": False}` or
  remove the key on next save).

## Verification gates (run with PYTHONUTF8=1; construction test under QT_QPA_PLATFORM=offscreen)
- `pytest tests/test_join_workbench_builder.py tests/test_join_workbench_triage.py tests/test_join_workbench_i18n.py tests/test_join_workbench_no_private.py tests/test_join_workbench.py tests/test_joins_lab.py tests/test_fjms_service.py tests/test_tabular_builder_rtl.py -q`
- `QT_QPA_PLATFORM=offscreen pytest tests/test_join_workbench_construct.py -q` — EXTEND it with a
  to_state→from_state round-trip test for JoinQueryBuilder (set rows/mods/gaps/global-opts, serialize,
  build a fresh builder, from_state, assert identical state + identical build_side_query output) and a
  JoinWorkbenchWindow.to_state() smoke (open=False on a freshly-built window).
- `ruff check desktop/join_workbench.py genizah_app.py genizah_translations.py tests/test_join_workbench_construct.py`
- Sanity: construct JoinWorkbenchWindow(parent=None, app=MagicMock()) offscreen → opens; calling
  to_state() returns a dict; from_state round-trips.
