---
phase: 108-desktop-join-workbench-query-builders-candidates-compare
plan: "03"
subsystem: desktop-joins-lab
tags: [joins-lab, candidate-pane, cross-side, enrichment, triage, i18n, rr-14]
dependency_graph:
  requires: [108-01, 108-02, 106-joins-lab-pure-logic, 107-join-workbench-shell]
  provides:
    - JoinCandidatePane (right-pane candidate hunt surface with builder wiring)
    - CandidateCard (grid card, Candidate-typed, triage border, action buttons)
    - ThumbResolver (grid thumbnail QThread worker)
    - _CrossSideWorker (apply_cross_side with MERGED b_ro, RR-14)
    - _EnrichWorker (get_measurement_summaries_batch keyed by (sys_id, page), RR-6)
    - candidate_to_result_dict() adapter (host-method boundary only)
    - triage state (sys_id-keyed, reset on re-anchor, D-10)
    - mark() + open_result_in_{browse,puzzle,list,as_join} public delegators (D-20)
    - _enqueue_image_for_pane (per-page _image_url_for_idx with None-page guard, RR-7/RR-12)
    - EXTENDED open_anchor_as_join(…, partner_sys_id=None, partner_shelfmark=None) in genizah_app.py (RR-3/D-17)
    - QGridLayout/QTableWidget/QTableWidgetItem/SearchThread imports (RR-10)
    - 60+ EN->HE translation entries for all new candidate-surface strings (RR-4)
  affects:
    - desktop/join_workbench.py
    - genizah_app.py
    - genizah_translations.py
tech_stack:
  added: []
  patterns:
    - Candidate dataclass attribute access (RR-2 — no r_*/page_of/.get on Candidate)
    - ja/flex/bidir MERGE into composed ro and b_ro after compose() (RR-14)
    - _CrossSideWorker receives ALREADY-MERGED b_ro as apply_cross_side b_responsa_options
    - _EnrichWorker single batch IN-query keyed by (sys_id, page) c.key
    - triage keyed by sys_id (R-05) deliberately split from (sys_id, page) enrichment key
    - None-page guard before page-1 arithmetic in _enqueue_image_for_pane (RR-12)
    - _image_url_for_idx per-page resolution (not get_thumbnail) for matched pages (RR-7)
    - bounded 5-slot ImageLoaderThread pool for grid thumbnail loading
    - merge_candidates returns a plain LIST — used directly, no .candidates (RR-2)
    - open_result_as_join calls EXTENDED public open_anchor_as_join (D-20, RR-3)
    - allow_page_position=False on other-side builder (RR-5)
key_files:
  created: []
  modified:
    - desktop/join_workbench.py (3612 lines — workers + pane + card + adapter + triage + imports)
    - genizah_app.py (open_anchor_as_join extended with partner kwargs)
    - genizah_translations.py (Phase 108-03 TRANSLATIONS.update block, 60+ EN->HE entries)
decisions:
  - "RR-14: compose() hardcodes ja/flex/bidir=False; _merge_globals() merges them back from builder._responsa_opts() into the composed ro (for SearchThread) and b_ro (for apply_cross_side) — without this the global Search-Options toggles are silent no-ops"
  - "RR-2: Candidate dataclass fields read directly (c.sys_id/c.page/etc.); candidate_to_result_dict() used ONLY at host-method boundaries (browse/list/image pump/set_anchor re-anchor)"
  - "RR-12: page is None guard added BEFORE page-1 arithmetic in _enqueue_image_for_pane; None treated as page 1 (VS-only candidates degrade gracefully)"
  - "R-05: triage keyed by sys_id (physical fragment), enrichment keyed by c.key=(sys_id,page) — deliberate split; triage is per-fragment, snippets are per-image"
  - "D-10: triage dict reset in BOTH __init__ and set_anchor so re-anchoring never bleeds old marks"
  - "merge_candidates returns a plain list; _on_cross_done uses MergeResult.candidates (from apply_cross_side), _maybe_assemble uses the LIST from merge_candidates directly"
  - "open_compare in JoinCandidatePane is a stub (sets focus + logs) — Plan 04 replaces with CompareDialog"
metrics:
  duration: "~15 minutes"
  completed: "2026-06-05T11:50:48Z"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 3
  tests_added: 0
---

# Phase 108 Plan 03: Candidate Surface — Workers, Pane, Card, Triage Summary

**One-liner:** Candidate-hunt right pane with Candidate-typed cross-side/enrich/thumb QThread workers, grid+table UI, ja/flex/bidir global-option merge into composed ro/b_ro, sys_id-keyed triage, per-page image pump with None-page guard, and extended public open_anchor_as_join.

## What Was Built

### Task 0: Plan-03 imports + open_anchor_as_join extension (RR-10, RR-3/D-17)

`desktop/join_workbench.py` — three new names added to the `from PyQt6.QtWidgets import (...)` tuple in the guarded `try:` block:
- `QGridLayout` — consumed by `JoinCandidatePane` grid host
- `QTableWidget` — consumed by `JoinCandidatePane` table view
- `QTableWidgetItem` — consumed by `_render_table()` cell construction

`from gui_threads import SearchThread` added inside the same `try:` — consumed by `JoinCandidatePane.do_search()`.

These four were deliberately kept OUT of Plan 02 (RR-10) to keep it ruff-clean; they are all consumed in this plan, so no F401 fires.

`genizah_app.py::open_anchor_as_join` extended:
- Signature: `open_anchor_as_join(self, anchor_sys_id, anchor_shelfmark, partner_sys_id=None, partner_shelfmark=None)`
- Adds `if partner_shelfmark: dialog.frag_b_input.setText(partner_shelfmark)` before `dialog.exec()`
- Default-None path unchanged (Phase-107 callers pass only two positional args)
- `_vs_open_joins_with_partner` untouched (D-20)

### Task 1: Workers + candidate_to_result_dict + triage/actions/image-pump

**`candidate_to_result_dict(c) -> dict`** — module-level pure adapter. Builds the raw result-dict shape from a Candidate. Used ONLY at host-method boundaries.

**`ThumbResolver(QThread)`** — emits `resolved(card_idx, url)` for grid card thumbnails. Manuscript-level `get_thumbnail()` is fine for small grid thumbs (RR-7 permits this); per-page resolution is in `_enqueue_image_for_pane`.

**`_CrossSideWorker(QThread)`** — receives an ALREADY-MERGED `b_ro` (caller has merged the other_builder's `ja/flex/bidir` into it). Delegates to `shared.joins_lab.apply_cross_side()`. On ValueError/empty b_query: emits `MergeResult(candidates=tuple(base))` unchanged.

**`_EnrichWorker(QThread)`** — single `get_measurement_summaries_batch(sys_ids)` call (RR-6 EXISTING method, extended in Plan 01 with `size_category`). Reads keys `width_cm`, `height_cm`, `material`, `avg_num_lines`, `size_category`. Emits dict keyed by `c.key = (sys_id, page)` (RR-2 per-page key). Snippet generation (`snippet_html` / `snippet_plain`) runs off-thread safely. Size-mismatch flag: `ratio = max(w, a_w) / max(min(w, a_w), 0.01) > 1.4` (D-13).

**`JoinWorkbenchWindow` additions:**
- `self.triage = {}` in `__init__` AND in `set_anchor()` (D-10 reset on re-anchor)
- `self.filtered = []`, `self._img_queue`, `self._img_active`, `self._img_threads`, `self._thumb_resolver`
- `mark(self, sys_id, val)`: stores in `self.triage[sys_id]`, propagates to pane `_restyle_card` + `_update_status_counts`
- `open_result_in_browse/puzzle/list/as_join`: route through Phase-107 public host methods (D-20); `open_result_as_join` calls `self._app.open_anchor_as_join(..., partner_sys_id=c.sys_id, partner_shelfmark=c.shelfmark)` — the EXTENDED public method (RR-3)
- `_enqueue_image_for_pane(label, sys_id, page, width=1400)`: guards `page is None` → treats as 1 (RR-12), fetches images via `meta_mgr.enrich_metadata(sys_id).get("images")`, resolves via `_image_url_for_idx(images, page-1, width)` (NOT `get_thumbnail` — RR-7 per-page)
- `_enqueue_image`, `_pump_images` (5-slot bounded pool), `_cancel_images`

### Task 2: JoinCandidatePane + CandidateCard + _tag helper + attach seam

**`_tag(text, color) -> QLabel`** — module-level section-label helper.

**`CandidateCard(QFrame)`** — `setFixedWidth(232)`. Layout: thumbnail QLabel (220x130, placeholder "loading…"), shelfmark + provenance badge (⚓ self / ⇄ other side), dimension/material line from `enrich[c.key]` (RR-2 per-page lookup with RR-6 key names), QTextBrowser snippet (72px), triage buttons Y/?/N (28x28, setAccessibleName), Compare + Re-anchor buttons, action row Browse/Puzzle/List/Join. `_restyle()` reads `wb.triage[sid]` for border colour. `set_pixmap()` guarded by try/except RuntimeError.

**`JoinCandidatePane(QWidget)`:**
- Section tag, this-side `JoinQueryBuilder`, other-side collapsible (`allow_page_position=False`, RR-5) with AND/OR combo
- Source selector: "Find Candidates" (wired), "Visual similarities" `setEnabled(False)`, "Search + visual" `setEnabled(False)` (D-14 stubs)
- Include-anchor checkbox `setChecked(False)` (D-15)
- Refine bar: text filter, material QComboBox (populated from enrich), has-dimensions checkbox, triage QComboBox, opt-in size filter (collapsed by default, D-13)
- Status label + view-toggle + pagination
- `QScrollArea` + `QGridLayout` grid (4 cols, 20/page) + `QTableWidget` (8 cols, hidden by default)

**`do_search()`:** `compose(side)` → `_merge_globals(builder, ro)` (RR-14 — merges ja/flex/bidir back from builder after compose dropped them) → `SearchThread(..., text_position=page_pos, corpus_scope="genizah")` (R-01)

**`_on_results(raw)`:** `detect_self_match` + `dedup_candidates`; if other-side enabled → compose b_side + `_merge_globals(other_builder, b_ro)` (RR-14) → `_CrossSideWorker(executor, deduped, b_side, b_ro, combine, a_pattern)`; else `_maybe_assemble()`

**`_on_cross_done(merge_result)`:** `list(merge_result.candidates)` (MergeResult path — `.candidates` is correct here) → `_maybe_assemble()`

**`_maybe_assemble()`:** `merge_candidates(text_cands, [])` returns a plain LIST (RR-2 — no `.candidates` on this result) → `self.results = list(...)` → `_start_enrich()`

**Attach seam:** `_build_right_pane()` — the `layout.addStretch()` placeholder replaced with `self._candidate_pane = JoinCandidatePane(self, self._executor); layout.addWidget(self._candidate_pane, 1)`

**`genizah_translations.py`:** Phase 108-03 block with 60+ EN→HE entries covering all new candidate-surface tr() keys.

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

**`open_compare(global_idx)` in JoinCandidatePane:** stub that sets card focus and logs; Plan 04 replaces with CompareDialog. Table double-click and card Compare button already call it — they will get the real dialog in Plan 04. Marked with `TODO: open_compare stub (Plan 04 will implement CompareDialog)` in the code.

This stub does NOT prevent the plan's goal (JWB-07 candidate hunt surface) from being achieved — compare is a JWB-08 requirement handled in Plan 04.

## Threat Flags

No new security-relevant surface beyond what the plan's threat_model documents:
- T-108-05 (snippet HTML injection): `snippet_html`/`htmlify` use `html.escape()` + sentinel markers — unchanged, no new escaping path.
- T-108-06 (SQL injection): `get_measurement_summaries_batch` uses parameterized `?` placeholders (existing method, Plan 01 column-guarded extension) — pane passes plain `list[str]` sys_ids.
- T-108-07 (UI thread starvation): all enrichment batched off-thread; 5-slot bounded image pool; `ThumbResolver` QThread.

## Self-Check: PASSED

Files exist:
- `desktop/join_workbench.py` (JoinCandidatePane + CandidateCard + workers + triage + image pump) — FOUND
- `genizah_app.py` (open_anchor_as_join with partner kwargs) — FOUND
- `genizah_translations.py` (Phase 108-03 TRANSLATIONS.update block) — FOUND

Commits exist:
- `d028f2a4` (Task 0: imports + open_anchor_as_join extension) — FOUND
- `56a08523` (Tasks 1+2: workers + pane + card + translations) — FOUND

Tests pass: 275/275 green (test_join_workbench_no_private + test_join_workbench_i18n + test_join_workbench + test_join_workbench_triage + test_joins_lab + test_fjms_service).
ruff: All checks passed on desktop/join_workbench.py, genizah_app.py, genizah_translations.py.
F401-clean: QGridLayout/QTableWidget/QTableWidgetItem/SearchThread all consumed (RR-10 satisfied).
