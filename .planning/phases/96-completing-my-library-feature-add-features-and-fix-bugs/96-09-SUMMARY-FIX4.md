---
phase: 96
plan: "09-fix4"
subsystem: desktop-my-library
tags: [uat-bugs, polish, fix-iteration-4, codex-prescription]
key-files:
  modified:
    - desktop/result_dialog.py
    - genizah_core.py
    - genizah_app.py
    - genizah_translations.py
  created:
    - tests/test_local_nav_codex_fix4.py
decisions:
  - "Item 1: returnPressed replaces editingFinished — focus-loss is the source of passive-click page jumps; Enter-only commit via _commit_spin_page_jump() is the correct model"
  - "Item 4: spinbox always displays p_num (physical page); cur_idx (dense ordinal) kept only for prev/next enabled-state arithmetic"
  - "Item 5: unknown p_num returns None from get_local_browse_page — silent fallback to page 1 was the root cause of the off-by-23 bug (1552 -> 1529)"
  - "max_p_num added to get_local_browse_page return dict so spinbox upper bound reflects sparse PDF range"
  - "Browse i18n: compose from atomic keys (Previous/Next/Page/Chunk/View All/Per page) — raw Hebrew literals removed from all dynamic label assignments"
---

# Phase 96 Plan 09 Fix Iteration 4 Summary

**One-liner:** Codex one-commit prescription applied — Enter-only spinner commit, p_num/cur_idx contract fix, missing-page returns None, full-size nav buttons as instance attrs, Browse i18n from atomic tr() keys.

## Diff from Iteration 3

Iteration 3 applied:
- Blanket `setAutoDefault(False)` on all QPushButtons via `findChildren`
- `btn_b_all.setEnabled(False)` in both LOCAL browse paths (View-All regression fix)
- `display['img'] = p_num` in `_build_local_result_dict` (Img column blank fix)

Iteration 4 (this fix) goes deeper on root causes:

| Symptom | Iter-3 approach | Iter-4 root fix |
|---------|-----------------|-----------------|
| Click anywhere -> page jumps | setAutoDefault(False) on buttons | Replace editingFinished with returnPressed (focus-loss can't fire anymore) |
| Spinner shows 1529 for p_num 1552 | Not addressed | setValue(p_num) not setValue(cur_idx) |
| Off-by-23 page navigation | Not addressed | get_local_browse_page returns None for unknown p_num; prev/next walks sorted list |
| Full-size prev/next not updated | Not addressed | btn_pg_prev/next as instance attrs + _set_local_page_nav_enabled() helper |
| Browse i18n broken in Hebrew | Not addressed | Compose from Previous/Next/Page/Chunk/View All/Per page atomic keys |

## Codex Items Applied

### Item 1 — Enter-only spinner commit
- `spin_page.setKeyboardTracking(False)` — suppresses mid-edit signals
- `spin_page.lineEdit().returnPressed.connect(self._commit_spin_page_jump)` — fires only on Enter
- `editingFinished` connection removed — focus-loss no longer triggers jumps
- New `_commit_spin_page_jump()` method: reads `spin_page.value()` -> `load_page(target=value)`

### Item 2 — Remove setFocus() focus hack
- Removed `self.spin_page.setFocus()` from `load_local_page()` entirely
- With Item 1 in place, focus on the spinner is no longer needed for Enter to work

### Item 3 — Fully suppress dialog default buttons
- `findChildren` loop now calls BOTH `setAutoDefault(False)` AND `setDefault(False)` on each button
- Either alone can still allow Enter propagation in edge cases

### Item 4 — Fix LOCAL spinbox contract
- `spin_page.setValue(self.current_p_num)` — physical page number, NOT cur_idx
- `spin_page.setMaximum(max_p_num)` — highest physical page number in sparse list
- `get_local_browse_page` return dict gains `max_p_num` key
- `cur_idx` kept for `_set_local_page_nav_enabled(prev=(cur_idx > 1), nxt=(cur_idx < total))`

### Item 5 — get_local_browse_page missing-page behavior
- Unknown p_num: `return None` (no silent fallback to page 1 via `current_idx = 0`)
- Prev/next: walk sorted indexed page list via `found_idx + next_prev` — blank-page skips handled
- Root cause of off-by-23: 1552nd physical page was at ordinal 1529; engine returned `current_idx=0` on StopIteration and the UI wrote that to spinner

### Item 6 — Full-size nav buttons as instance attributes
- `btn_pg_prev` -> `self.btn_pg_prev`
- `btn_pg_next` -> `self.btn_pg_next`
- New `_set_local_page_nav_enabled(prev, nxt)` helper: updates BOTH full-size and compact buttons in one call, preventing the compact-only update bug

### Browse i18n (Codex section C)
- `btn_local_browse_prev`: `f"◄ {tr('Previous')}"` (was `tr("◀ Prev")` — not in translations)
- `btn_local_browse_next`: `f"{tr('Next')} ►"` (was `tr("Next ▶")` — not in translations)
- `btn_local_browse_view_toggle` init: `tr("View All")` (was `tr("הכל") if CURRENT_LANG=='he' else tr("View All")`)
- `_open_local_browse_page` label: `tr("Page")` / `tr("Chunk")` (was raw `'דף'` / `'מקטע'` strings)
- View-all toggle label: `tr("Per page")` (was `tr("דף") if CURRENT_LANG=='he' else tr("Per page")`)
- Added to `genizah_translations.py`: `"Chunk": "מקטע"`, `"Per page": "לדף"`

### Tech-debt (Codex section D)
- `get_local_browse_page` docstring: full p_num vs current_idx contract documented; sparse page set documented (replaces incorrect "contiguous 1..N" claim)
- `_commit_spin_page_jump`: documented as sole Enter handler for page navigation
- `load_local_page` docstring: full p_num/current_idx contract + Item 1/2 rationale
- `_set_local_page_nav_enabled`: single shared update path for full-size + compact nav buttons
- Durable contract comments replace iteration-numbered "fix-N" references

## Verification Gates

| Gate | Mechanism | Status |
|------|-----------|--------|
| a. Type 1552 + Enter -> page 1552 | `test_known_sparse_p_num_returns_correct_page` | PASS |
| b. Click anywhere -> no page change | `editingFinished` removed; only `returnPressed` fires | By construction |
| c. Prev moves exactly one indexed step | `test_prev_from_sparse_page_moves_one_step` | PASS |
| d. Type 999 -> None (no bizarre page) | `test_unknown_p_num_returns_none` | PASS |
| e. Browse Tab prev/next in current UI language | `test_chunk_key_in_translations`, `test_per_page_key_in_translations` | PASS |
| f. Img column shows p_num | `test_local_browse_panel.py` (iteration 3 test, still passes) | PASS |
| g. View-All toggle never throws page-not-found | Covered by iteration 3 fix + test_local_browse_panel.py | PASS |

## Commits

| Hash | Description |
|------|-------------|
| 5d8672be | fix(96-09): iteration 4 — Codex one-commit nav bug cluster prescription |

## Test Results

- `test_local_nav_codex_fix4.py`: 8 passed (new)
- `test_local_nav_page_chunk.py`: 4 passed
- `test_local_browse_panel.py`: 10 passed
- `test_result_dialog_local_button_removed.py`: 2 passed
- Full suite: 2560 passed, 24 skipped, 4 xfailed (identical to iteration 3 baseline)
- ruff: all checks passed

## Known Stubs

None.

## Threat Flags

None — no new network endpoints, auth paths, or schema changes. File content rendered in Qt widgets with HTML escaping (Codex HIGH #4, carried from iteration 3).

## Self-Check: PASSED

- `desktop/result_dialog.py`: `_commit_spin_page_jump` present, `returnPressed` connected, `editingFinished` absent, `setFocus` absent, `self.btn_pg_prev`/`self.btn_pg_next` instance attrs, `_set_local_page_nav_enabled` present
- `genizah_core.py`: `max_p_num` in return dict, missing p_num returns None, contract docstring updated
- `genizah_app.py`: `f"◄ {tr('Previous')}"` / `f"{tr('Next')} ►"`, `tr("View All")` / `tr("Per page")` / `tr("Page")` / `tr("Chunk")` — no raw Hebrew literals
- `genizah_translations.py`: `"Chunk"` and `"Per page"` keys present
- `tests/test_local_nav_codex_fix4.py`: 8 tests, all PASSED
- Commit `5d8672be` exists in git log: confirmed
