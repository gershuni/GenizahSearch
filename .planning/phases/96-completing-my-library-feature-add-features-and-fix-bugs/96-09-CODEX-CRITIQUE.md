# Codex Cross-AI Critique — Phase 96 Navigation Bug Cluster (after iteration 3)

## Root Cause Diagnosis

The remaining bug is **not** a button-default problem anymore; it is a **mixed page-identity problem amplified by editingFinished**.

`QSpinBox.editingFinished` is connected at `desktop/result_dialog.py:247`. That signal fires on Enter AND on focus loss. Then `load_local_page()` forcibly refocuses the spinner at `desktop/result_dialog.py:2408`, so any later click elsewhere can trigger a page jump.

For LOCAL pages, the bigger contract mismatch:
- `load_local_page()` stores `current_p_num = page_data["p_num"]`
- BUT writes the spinbox with `current_idx` at `desktop/result_dialog.py:2388`
- Later the same spinbox value is interpreted as `p_num`

This explains `Img 1552 → 1529`: 1552 is the real PDF page number; 1529 is the dense index after 23 skipped blank/empty pages.

## One-Commit Fix (Codex prescription)

Do this as ONE cohesive fix, not more button patching:

1. **Replace `editingFinished` with Enter-only commit:**
   - `self.spin_page.setKeyboardTracking(False)`
   - Connect `self.spin_page.lineEdit().returnPressed` to a `_commit_spin_page_jump()` helper
   - Do NOT connect `editingFinished` — focus-loss is the source of passive-click jumps

2. **Remove `self.spin_page.setFocus()` from `load_local_page()`.**

3. **Disable dialog defaults fully:**
   - Keep `findChildren(QPushButton)` loop, but call BOTH `setAutoDefault(False)` AND `setDefault(False)`

4. **Fix LOCAL spinbox contract:**
   - Display/input `p_num`, NOT `current_idx`
   - Keep `current_idx` only for prev/next enabled state
   - Add `max_p_num` from `get_local_browse_page()` if needed for spinner maximum

5. **Fix `get_local_browse_page()` missing-page behavior:**
   - If target `p_num=N` is not found, return `None` or preserve current page
   - Do NOT fall back to page 1 at `genizah_core.py:9410`
   - Prev/next should use current page's index in sorted page list and move by exactly one indexed page

6. **Make full-size `btn_pg_prev` / `btn_pg_next` instance attributes** — currently only compact buttons are updated in LOCAL state.

## Off-By-23 (B)

**Producer is mostly correct.** LOCAL `p_num` is the physical PDF page number or DOCX chunk number from `_make_full_header()` in `shared/local_indexer.py:337`. PDFs skip text-empty pages at `shared/local_indexer.py:391`, so `p_num` can be sparse.

**The bug is mainly consumer-side:**
- `get_local_browse_page()` returns both `p_num` and dense `current_idx`
- `load_local_page()` displays `current_idx` in a control whose jump path treats the value as `p_num`

DO NOT change `_build_local_result_dict()` to emit dense indexes. Keep `display["img"] = p_num`; fix the UI to stop substituting `current_idx`.

## Browse i18n (C)

Buttons created in `create_browse_tab()` around `genizah_app.py:6831`. They ARE wrapped in `tr()`, but the keys `◀ Prev`, `Next ▶`, and LOCAL tooltip strings are not in `genizah_translations.py`. Prefer composing from existing keys:

```python
QPushButton(f"◀ {tr('Previous')}")
QPushButton(f"{tr('Next')} ▶")
```

Also fix dynamic labels in `_open_local_browse()` / `_open_local_browse_page()`: avoid `tr("הכל")` and literal Hebrew branches. Use English keys like `tr("View All")`, `tr("Per page")`, `tr("Page")`, `tr("Chunk")`, and add missing translations.

## Technical Debt (D)

Clean now:
- Remove the focus hack
- Document `p_num` vs `current_idx` contract clearly
- `get_local_browse_page()` docstring says LOCAL pages are contiguous; PDFs prove that false
- Top-level `img` + `display["img"]` is confusing; define one canonical read order
- Full and compact page nav buttons should share one update path
- "fix-N" comments are misleading; collapse into durable contract comments
