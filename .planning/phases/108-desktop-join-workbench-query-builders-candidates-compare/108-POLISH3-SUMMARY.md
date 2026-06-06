# Phase 108 Join Lab — Polish Round 3 Summary

**Date:** 2026-06-06
**Branch:** master-main
**Objective:** Post-UAT Join Lab polish — all 12 features from 108-POLISH3-SPEC.md implemented and committed atomically.

---

## One-liner

Join Lab polish round 3: folio-flip fetches page text, grid double-click opens Compare, per-pane dims/material/zoom on both Compare sides, triage colors Compare border, 2px muted triage borders, Latin-free Narrow/Widen combo, RTL-correct ▶◀ arrows everywhere, Hebrew material terms, Source column removed (8-col table), "תצוגת טבלה" typo fixed, and table Snippet highlights matched terms.

---

## Commit Table

| Commit | Message | Features |
|--------|---------|----------|
| `f96d2361` | feat(108): Polish round 3 — all 12 features | 1–12 |

---

## Features Implemented

### Feature 1 — Folio `< >` also flips TEXT (grid cards + compare panes)

**File:** `desktop/join_workbench.py`

- `CandidateCard`: added `self.snip` (was local var), `self._card_text_worker`, `self._card_text_gen`.
- `_card_folio_prev` / `_card_folio_next` now call `_refresh_card_text()` in addition to `_refresh_card_image()`.
- New `_refresh_card_text()`: cancels stale worker, increments `_card_text_gen`, starts `_PageTextWorker` on background thread. Closure checks `_card_text_gen` for staleness. On done: calls `snippet_html()` and updates `self.snip.setHtml()`.
- `CompareDialog._pane_folio_step`: extended to also fetch page text via `_PageTextWorker`. Closure stored in `self._pane_text_workers` list (GC guard). Worker gen checked against `wb._gen`. Page text rendered via `apply_line_numbered_text` + `htmlify`.
- RR-12 None-page guard: both paths use `max(1, page or 1)` before reaching workers.

### Feature 2 — Grid card double-click → Compare

**File:** `desktop/join_workbench.py`

- `CandidateCard.mouseDoubleClickEvent`: calls `self.pane.open_compare(self.global_idx)`.
- Guard: `childAt()` check skips `QCheckBox` and `QPushButton` children — checkbox and action buttons do NOT trigger compare.

### Feature 3 — Compare panes: dims/material + zoom on BOTH sides

**File:** `desktop/join_workbench.py`

- `CompareDialog._pane()` factory extended:
  - Added `dims_lbl` (QLabel, initially hidden) below `meta`.
  - Renamed `folio_row` → `ctrl_row`, added zoom `−` and `+` buttons at right end.
  - Pane dict gains: `dims_lbl`, `zoom`, `full_pix` keys.
  - Zoom buttons wired to new `_pane_zoom(pane, factor)`.
- `_fill_anchor` and `_fill_candidate` now populate `dims_lbl` from enrich data (material/dims), using `material_display()` (Feature 9).
- `_pane_zoom(pane, factor)`: clamps zoom `[0.25, 4.0]`, re-requests image at `max(400, int(1400 * zoom))` pixels via `_enqueue_image_for_pane`.

### Feature 4 — Triage Y/?/N colors Compare border

**File:** `desktop/join_workbench.py`

- New `CompareDialog._restyle_compare(triage_val)`: sets `QDialog{border:2px solid {color}}` using `_TRI_COLOR` (Feature 5 muted tones).
- `_mark(val)` calls `_restyle_compare(val)` after marking + painting.
- `paint()` calls `_restyle_compare(tri)` each time candidate changes — border reflects current candidate's existing triage state.

### Feature 5 — Gentler triage borders (2px + muted tones)

**File:** `desktop/join_workbench.py`

- `_TRI_COLOR` constants updated: `yes: #4d9e6a` (softer green), `maybe: #c4853a` (softer amber), `no: #c05050` (softer red).
- `CandidateCard._restyle()`: `3px solid` → `2px solid` everywhere (triage, anchor-self, selection). Anchor-self/selection teal softened to `#4db8a6`.

### Feature 6 — Remove Latin AND/OR from combine strings

**File:** `desktop/join_workbench.py`, `genizah_translations.py`

- `combine_combo` items: `tr("AND (narrow)")` → `tr("Narrow")`, `tr("OR (widen)")` → `tr("Widen")`.
- Tooltip: new key `"Narrow: keep only candidates whose adjacent page also matches. Widen: include adjacent pages as extra candidates."` with faithful HE translation.
- Old garbled HE keys `"AND (narrow)"` / `"OR (widen)"` replaced by clean `"Narrow"` / `"Widen"` with correct HE `"צמצום"` / `"הרחבה"`.
- `_on_results` logic unchanged: index 0 = Narrow (AND), index 1 = Widen (OR).

### Feature 7 — Flip arrow glyphs for RTL

**File:** `desktop/join_workbench.py`, `genizah_translations.py`

PREV points right, NEXT points left:

| Location | Old | New |
|----------|-----|-----|
| Card folio prev/next | ◀ / ▶ | ▶ / ◀ |
| Pagination Prev/Next | ← Prev / Next → | Prev → / ← Next |
| Compare candidate prev/next | < prev / next > | prev > / < next |
| Compare pane folio prev/next | ◀ / ▶ | ▶ / ◀ |
| Anchor pane folio prev/next | ◄ / ► | ► / ◄ |

All translation keys updated accordingly.

### Feature 8 — tr() sweep

**File:** `desktop/join_workbench.py`

- Anchor pane zoom buttons (`Zoom out`, `Zoom in`, `setAccessibleName`) now use `tr()`.
- Anchor pane folio `setAccessibleName` now uses `tr("Previous folio")` / `tr("Next folio")`.
- Compare zoom buttons use `tr("Zoom out")` / `tr("Zoom in")`.
- All other new user-facing strings in features 1–7 wrapped with `tr()` and registered.

### Feature 9 — Hebrew material terms in HE UI

**File:** `desktop/join_workbench.py`

- New pure helper `material_display(material)` gated on `CURRENT_LANG == 'he'`.
- Mapping: paper→נייר, parchment→קלף, papyrus→פפירוס, vellum→קלף, leather→עור, cloth→בד, mixed→מעורב; unknown→as-is.
- Applied in: grid card dim line, table Material column (`_render_table`), compare pane `dims_lbl` (both `_fill_anchor` and `_fill_candidate`).
- Filter dialog material combo: items use `addItem(material_display(mat), mat)` (display HE, userData=EN). `apply_filters` reads `currentData()` (English) for comparison. `_on_apply` syncs by userData. `_set_mat` shortcut finds by userData.
- Filter VALUE stays English — only the displayed label is Hebrew.

### Feature 10 — Remove Source table column

**File:** `desktop/join_workbench.py`, `tests/test_join_workbench_construct.py`

- `_headers` list: `tr("Source")` entry removed. Table now has 8 columns.
- New column layout: 0 checkbox, 1 Shelfmark, 2 Score, 3 Snippet, 4 Material, 5 Dimensions, 6 Page, 7 Triage.
- `_render_table`: removed `setItem(row, 6, ... c.scope ...)` and renumbered Page (6) and Triage (7).
- `_table_double_clicked`: col==0 guard unchanged (checkbox still col 0); comment updated.
- `test_join_workbench_construct.py`: `columnCount() == 9` → `== 8`.

### Feature 11 — Fix "תצוגת טבלאי" → "תצוגת טבלה"

**File:** `genizah_translations.py`

- `"Table view"` HE value: `"תצוגת טבלאי"` → `"תצוגת טבלה"`.

### Feature 12 — Highlight matched terms in TABLE Snippet column

**File:** `desktop/join_workbench.py`

- `_render_table`: when `snippet_html` is non-empty, uses `setCellWidget(row, 3, QLabel)` instead of `QTableWidgetItem`.
- QLabel: `TextFormat.RichText`, `LayoutDirection.RightToLeft`, `AlignRight | AlignVCenter`, `TextSelectableByMouse`, wraps snippet HTML in `<span dir="rtl">`.
- Falls back to plain `QTableWidgetItem` when no snippet_html available.

---

## Tests Updated

**`tests/test_join_workbench_construct.py`** — 1 assertion updated:

- `test_join_candidate_pane_constructs`: `columnCount() == 9` → `== 8` with updated column layout comment.

---

## Deviations from Spec

### Deviation D1: `_fill_anchor` dims/material moved to separate `dims_lbl` (not kept in `meta` line)

- **Spec:** "shows a material/dimensions line" — interpreted as a separate label.
- **Original code:** dims/material was appended to the `meta` label inline.
- **Change:** meta label now shows only `meta_brief()` (library, image count, title); dims/material gets its own `dims_lbl` line below.
- **Rationale:** Cleaner visual separation; dims_lbl is hidden when no data available.
- **Impact:** Cosmetic only; correct data still displayed.

### Deviation D2: Compare pane zoom re-requests image at scaled width (not client-side rescale)

- **Spec:** "zoom image controls (−/+)".
- **Implementation:** Calls `_enqueue_image_for_pane` with `max(400, int(1400 * zoom))` pixel width. Requests the IIIF server's next-resolution tile rather than stretching a cached pixmap.
- **Rationale:** No full pixmap cached at compare pane level (image loads go through pool with callback). Re-requesting is simpler and provides better quality. Trade-off: one network request per zoom step.

### Deviation D3: _restyle_compare applied on `paint()`, not on `_mark()` only

- **Spec:** "Marking Y/?/N inside Compare applies the matching colored border."
- **Extension:** Border also updates when `paint()` is called (stepping to a new candidate with existing triage), so the border always reflects the current candidate's triage state.
- **Rationale:** Better UX — border is meaningful at all times, not just when user marks in this session.

---

## Known Stubs

None.

---

## Threat Flags

None — no new network endpoints, auth paths, file access patterns, or schema changes.

---

## Self-Check

**Files exist:**
- `C:\Genizahsearch\desktop\join_workbench.py` — FOUND
- `C:\Genizahsearch\genizah_translations.py` — FOUND
- `C:\Genizahsearch\tests\test_join_workbench_construct.py` — FOUND

**Commits exist:**
- `f96d2361` — FOUND

**Test results:**
- 316 non-Qt tests: PASS
- 6 construct tests (columnCount 9→8): PASS
- ruff: PASS (all 4 target files clean)
- D-20 grep: PASS (no `_vs_` in `join_workbench.py`)

## Self-Check: PASSED
