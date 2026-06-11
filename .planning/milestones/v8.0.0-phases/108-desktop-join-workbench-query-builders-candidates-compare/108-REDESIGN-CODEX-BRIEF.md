# Codex pre-flight brief — Phase 108 Join Lab UI redesign

**You are doing a PRE-FLIGHT REVIEW of an implementation plan. READ-ONLY: do NOT modify any files.
Output your review as text only.** Read the actual code and the approved sketch, then critique the plan
below: risks, ordering pitfalls, simpler approaches, and anything that will break the locked tests or
invariants.

## Context
PyQt6 desktop app. The Join Lab (Phase 108) is implemented in `desktop/join_workbench.py` (all classes
live inside one `try:` PyQt-import guard, 4-space class indent / 8-space method indent). It already
opens and the headless suite is green; the user reviewed it live and asked for a UI redesign. The
approved redesign mockup is at:
`.planning/phases/108-desktop-join-workbench-query-builders-candidates-compare/sketch/join-lab-redesign.html`
(interactive HTML — open it; it encodes the target layout and behaviors).

## Current code map (desktop/join_workbench.py)
- `JoinQueryBuilder(QWidget)` 653 — `_init_ui` 697, `add_row` 841, `_make_box` 933, `add_or_box` 947,
  `_remove_box` 963, `_remove_row` 978, `_sync` 992, `eventFilter` 1001, `_on_row_focus` 1011,
  `_on_modifier_changed` 1043, `_page_position` 1088, `_responsa_opts` 1094, `build_side_query` 1112,
  `_update_preview` 1189. Today: an always-visible **shared modifier row** + a global Search-Options
  row; per-row mods stored in each row's `mods` dict; the focused row is tracked via
  `eventFilter`→`_on_row_focus` and edited via `_on_modifier_changed`. Each row already has an inline
  gap `QSpinBox` (`add_row` ~879) and line start/end checkboxes.
- `JoinCandidatePane(QWidget)` 1647 — `_build_ui` 1681, `_merge_globals` 1888, `do_search` 1906,
  `apply_filters` 2095, `render_results` 2163, `_render_grid_page` 2170, `_render_table` 2232,
  `_table_double_clicked` 2255, `toggle_view` 2283, `_toggle_size_filter` 2317, `open_compare` 2326.
  Today: `_tag("THIS SIDE…")` labels, an always-on refine/filter bar, a self-match readout, an
  "include anchor itself" checkbox, an "Also constrain the OTHER side" checkbox + AND/OR combo, a
  source selector (Visual/Combined disabled), grid (20/page) + table toggle.
- `CandidateCard(QFrame)` 1473 — per-card thumbnail/snippet/triage + action buttons.
- `CompareDialog(QDialog)` 2339 — two-pane compare; `open_compare` opens it; prev/next over the
  filtered list.
- `JoinWorkbenchWindow(QDialog)` 2598 — `_init_ui` 2673 (QSplitter `setSizes([420,540])`),
  `_build_anchor_pane`, anchor actions **already exist**: `_anchor_browse` 3686, `_anchor_puzzle`
  3691, `_anchor_add_to_list` 3696, `_on_add_as_join` 3707; candidate delegators `open_result_in_browse`
  3249, `open_result_in_puzzle` 3253 (`→ self._app.open_anchor_in_puzzle(c.sys_id)`),
  `open_result_in_list` 3257, `open_result_as_join` 3265 (extended `open_anchor_as_join`),
  `set_anchor` 2945, `mark` 3227 (triage, sys_id-keyed), `_enqueue_image_for_pane` 3341 (None-page
  guarded, RR-12). Triage state `self.triage`, post-filter list `self.filtered`.
- Host (genizah_app.py): `open_anchor_in_puzzle(sys_id)` 15438 → `_vs_add_to_puzzle` 5263 →
  `add_to_puzzle(sys_id, shelf)` (ACCUMULATES onto the canvas); `open_anchor_as_join(anchor_sid,
  anchor_shelf, partner_sys_id=None, partner_shelfmark=None)` 15442; `show_add_to_list_menu(items,…)`
  (takes a LIST); `open_result_in_browse_from_table(res)`.

## Approved redesign decisions
1. Anchor pane default ~30% width, resizable (QSplitter). Anchor 4 actions = ICON-ONLY (📖 browse / 🧩
   puzzle / ☰ list / 🔗 join), no text label, bottom of anchor pane — these act on the ANCHOR.
2. Minimalism: hide modifiers/options/filter behind buttons.
3. Per-line **⚙** opens THAT line's options dialog: negation/plene/prefix/suffix/wildcards + ⊢ starts /
   ⊣ ends line (wildcard-prefix disabled when the row has >1 OR box). A separate top **"Search options"**
   button holds the GLOBAL toggles (variants / Judeo-Arabic / flex spacing / bidirectional).
4. Word boxes accept typed signs (`#`,`word#`,`%`,`*word`/`word*`,`−`) directly + an ⓘ tooltip legend.
5. Gap "↓N" is a COMPACT INLINE control on each line (no separate row) — it already is an inline spinner;
   keep compact.
6. Remove: the "THIS SIDE/OTHER" tags, the self-match readout text, the "include anchor itself" toggle.
7. "Also constrain the OTHER side" → relabel "search also on the other side of the leaf (p ±1)"; KEEP the
   AND-narrow / OR-widen combo.
8. Filter: replace the always-on refine bar with a **"Filter ▾" button** → dialog with text/material/
   has-dimensions/triage/size, PLUS a "Current fragment" info panel (anchor library/material/size/lines,
   only the fields that are known) + "from anchor" shortcuts (match material, width ±2cm).
9. Results toolbar: [Grid][Table] + prominent **"Browse results ▶"** (opens CompareDialog stepping the
   filtered list) + "Filter ▾" + count.
10. Grid cards: ICON-only action buttons (📖🧩☰🔗 + ⇄ compare); a per-card **checkbox**; a **right-click
    context menu** (same actions + triage).
11. Table: leading **checkbox column + master checkbox**; a **shared bulk-action bar** (works for grid
    AND table selections) with 📖🧩☰🔗; **Browse is enabled only when exactly ONE item is selected**
    (disabled for 0 or >1). Other bulk actions act on all checked.
12. "Add to Puzzle" (cards, context menu, bulk bar, Compare) adds the ANCHOR **and** the candidate(s).

## Planned implementation (critique this)
- **Builder:** drop the shared modifier row + the always-on global row from `_init_ui`. Add a per-row ⚙
  button (in `add_row`) opening a small modal `QDialog` bound to that row's `mods` dict (+ start/end +
  wildcard-prefix-disable-if->1-box). Add a top "Search options" button → dialog editing the global
  `chk_opt_*`. Keep `build_side_query`/`_responsa_opts`/`compose` output **byte-identical** (parser
  tests). Decide: can the `eventFilter`/`_on_row_focus`/`_on_modifier_changed` active-row machinery be
  REMOVED now that each ⚙ edits its own row directly? Add ⓘ tooltip legend on the box. Keep the inline
  gap spinner.
- **Anchor pane:** restyle the existing 4 action buttons to icon-only; ensure bottom placement; set
  splitter `setSizes` to ~30/70.
- **Candidate pane:** remove tags/self-match/include-anchor; relabel other-side; move filter controls into
  a "Filter ▾" dialog with the current-fragment info panel; add "Browse results ▶"; make card actions
  icon-only; add card checkbox + context menu; add table checkbox column + master + shared bulk bar with
  single-only Browse. Introduce a **selection set** (candidate keys) on the pane so checkbox state
  survives grid pagination + grid/table toggle (re-render reads the set).
- **Compare:** wire "Browse results ▶" to `open_compare(first_or_selected)`; puzzle inside compare adds
  anchor+candidate.
- **Host:** extend puzzle to add multiple fragments — e.g. `open_anchor_in_puzzle(sys_id, *extra_sys_ids)`
  looping `_vs_add_to_puzzle`/`add_to_puzzle` (single-arg Phase-107 callers unchanged). Workbench
  `open_result_in_puzzle(c)` → `open_anchor_in_puzzle(self._anchor_sid, c.sys_id)`; bulk → anchor + all
  checked. NO `_vs_` calls from the workbench (host owns it).
- **i18n:** register every new/changed `tr()` key in `genizah_translations.TRANSLATIONS` (EN→HE).
- **Tests:** update `tests/test_join_workbench_construct.py` (widget construction smoke, CI-skipped). Keep
  green: `test_join_workbench_builder.py`/`_triage.py` (parser contracts — build_side_query unchanged),
  `test_join_workbench_no_private.py` (zero `_vs_` in workbench), `test_join_workbench_i18n.py` (all tr
  keys registered).

## Hard invariants (must NOT break)
- RR-1/RR-13: `build_side_query`→`compose` output unchanged; parser round-trip tests green.
- RR-14: ja/flex/bidir merged into `ro`/`b_ro` before search (`_merge_globals`).
- RR-12: None-page guard in `_enqueue_image_for_pane`.
- D-20: zero `_vs_*` calls from `desktop/join_workbench.py` (host methods only).
- D-06: no dialog-level `setLayoutDirection(RightToLeft)`; content RTL, chrome LTR.
- Window must OPEN — construction order: build every widget BEFORE anything references it (we just fixed
  a `_preview_edit`-before-create crash; the redesign reshuffles `_init_ui`, so re-verify ordering).
- i18n: all `tr()` keys registered (language-gated; raw Hebrew literals would leak to EN users).

## Questions
1. Per-line ⚙ pattern: modal `QDialog` vs checkable `QMenu` vs popup `QFrame`? Is removing the active-row
   `eventFilter` machinery safe/cleaner, or does anything still depend on focus tracking?
2. Selection set across paginated grid + table + view-toggle: cleanest PyQt approach so checkbox state is
   not lost on `_render_grid_page`/`toggle_view`? Should triage and selection be distinct (they are:
   triage = Y/?/N persisted per sys_id; selection = transient for bulk)?
3. Puzzle-includes-anchor: varargs extension of `open_anchor_in_puzzle` vs a new public method? Any risk
   `add_to_puzzle` dedups/replaces rather than accumulates, or ordering matters (anchor first)?
4. "Browse results ▶" vs the existing card ⇄ / `_table_double_clicked` compare entry points — redundancy
   or conflict? Should double-click and ⇄ both still open compare?
5. Anything here that breaks the locked parser/i18n/no-private tests, or PyQt threading/teardown pitfalls
   (the desktop-Qt tests already segfault under full-suite teardown — see tests/conftest.py).
6. Sequencing: safest order to make these edits in one pass, and any change you'd split out or cut.
