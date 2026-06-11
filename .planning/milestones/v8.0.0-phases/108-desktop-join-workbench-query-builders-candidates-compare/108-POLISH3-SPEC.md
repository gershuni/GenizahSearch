# Phase 108 Join Lab — polish round 3 (spec)

Approved feedback batch. Implement all; commit atomically. Read current `desktop/join_workbench.py`,
`genizah_translations.py`, `genizah_app.py` first. Keep ALL locked invariants (build_side_query/compose
output unchanged; RR-12 None-page guard; RR-14 merge; D-20 zero `_vs_` in workbench; D-06 no dialog-level
RTL; every new tr() key registered, no dynamic keys; window must open; no UI-thread blocking / persist
input-only — unchanged from rounds 1-2).

## Features

### 1. Folio `< >` also flips the TEXT (grid cards AND compare panes)
The per-card folio buttons (CandidateCard `_folio_prev_btn`/`_folio_next_btn` ~1702/1713) and the
per-pane folio buttons in CompareDialog currently flip only the image. They must ALSO update the
displayed TEXT/transcription to that folio's page. Reuse `_PageTextWorker` (line 417) (or the same
page-text path the anchor pane uses) to fetch the page text in the BACKGROUND and update the card's
snippet / the compare pane's transcription. None-page guard (RR-12). Fetch on click only.

### 2. Grid double-click → Compare
CandidateCard should open Compare on double-click (mouseDoubleClickEvent), same as the table's
`_table_double_clicked`. Don't trigger when double-clicking the card's checkbox or action buttons.

### 3. Compare panes: dimensions/material + image controls on BOTH sides
In CompareDialog, each pane (anchor AND candidate) shows a material/dimensions line (material in Hebrew
per Feature 9 in HE UI) AND zoom image controls (− / +), mirroring the main anchor pane's zoom. The
folio `< >` (Feature 1) stays. Get dims/material from the same enrichment the cards use
(get_measurement_summaries_batch / enrich).

### 4. Triage Y/?/N colors the Compare window border too
Marking Y/?/N inside Compare applies the matching colored border to the compare window (or the candidate
pane), like grid cards. Use the gentle border from Feature 5.

### 5. Gentler triage borders (both modules)
CandidateCard triage borders are currently `3px solid {color}` (~line 1828 selected teal, 1839 triage).
Soften EVERYWHERE (cards + compare): thinner (2px) and muted/desaturated tones (e.g. green ~#16a34a→use
a softer #4d9e6a-ish, amber softer, red softer — pick gentle, lower-saturation shades). Keep them
clearly distinguishable but not loud.

### 6. Remove Latin AND/OR from the combine strings (RTL fix)
`combine_combo` items `tr("AND (narrow)")` / `tr("OR (widen)")` (~1954-1955) and their HE values
(genizah_translations.py:3822-3823 "וגם (ANDצמצם)" / "או (ORהרחב)") are garbled in RTL. Change the combo
items to `tr("Narrow")` / `tr("Widen")` → HE "צמצום" / "הרחבה". Also rephrase the tooltip (1947 / 3820)
to drop the Latin "AND"/"OR" (e.g. EN: "Narrow: keep only candidates whose adjacent page also matches.
Widen: include adjacent pages as extra candidates." + faithful HE). Remove the old garbled keys.

### 7. Flip the `< >` / prev-next arrow GLYPHS everywhere (RTL-correct)
Everywhere there is folio/page nav, mirror the arrow glyph so it reads RTL: PREVIOUS points right
(▶ / ► / →) and NEXT points left (◀ / ◄ / ←). Apply to: CandidateCard folio buttons, the candidate-list
Prev/Next (`← Prev` / `Next →`), CompareDialog's candidate-list nav AND the new per-pane folio nav, and
the main anchor pane folio nav (`◄`/`►`). The FUNCTION stays the same — only the glyph mirrors. Keep
labels tr()-wrapped.

### 8. tr() sweep
Ensure every user-facing string added in rounds 1-3 (tooltips, menu items, dialog titles, button labels,
the new compare material/dims labels) is wrapped in tr() AND registered (EN→HE) in
genizah_translations.TRANSLATIONS. No dynamic `tr(f"...")`. (test_join_workbench_i18n.py enforces it.)

### 9. Hebrew material terms in HE UI
Material values come from data in English (paper/parchment/papyrus/vellum/leather/…). In HEBREW UI,
display the Hebrew term: paper→נייר, parchment→קלף, papyrus→פפירוס, vellum→קלף, leather→עור, cloth→בד,
mixed→מעורב (extend sensibly; unknown → show as-is). Add a small `material_display(material)` helper
(gated on CURRENT_LANG=='he') and apply it EVERYWHERE material is shown: grid cards, the table Material
column, the compare panes (Feature 3), and the filter dialog's material dropdown. Keep the underlying
filter VALUE in English (only the displayed label is Hebrew).

### 10. Remove the "Source" table column
The Source column (header tr("Source") ~2134; column 6 of the 9-col table per the comment ~2505;
populated with badge_for_source) is always "text" in Phase 108 (`self._sources = {"text"}`) — uninformative.
REMOVE it: drop the header, the cell population, and shift the remaining columns (after removing Source:
0 checkbox, 1 Shelfmark, 2 Score, 3 Snippet, 4 Material, 5 Dimensions, 6 Page, 7 Triage = 8 cols). Update
`_table_double_clicked` column handling and the column-index comment. (Visual-similarity source returns
in Phase 109.)

### 11. Fix תצוגת טבלאי → תצוגת טבלה
The Table-view toggle's Hebrew label renders "תצוגת טבלאי" — change the HE translation value to
"תצוגת טבלה". (Find the relevant key, likely "Table view" or similar, in genizah_translations.py.)

### 12. Highlight the searched term in the TABLE snippet too
The grid snippet highlights matched terms; the table's Snippet column does not (QTableWidgetItem can't
render rich text). Use `setCellWidget(row, snippet_col, QLabel)` with the SAME highlight markup the grid
uses (reuse the existing snippet-highlight helper). Keep it RTL + elided sensibly.

## Interpretation notes (flagged, reversible)
- Feature 7 flips arrows UNCONDITIONALLY (RTL-correct) since this is a Hebrew-primary tool; not
  language-gated. (Easy to gate on CURRENT_LANG later if EN should differ.)
- Feature 10 removes Source rather than renaming it.

## Verification gates (PYTHONUTF8=1; construct test under QT_QPA_PLATFORM=offscreen)
- pytest tests/test_join_workbench_builder.py tests/test_join_workbench_triage.py tests/test_join_workbench_i18n.py tests/test_join_workbench_no_private.py tests/test_join_workbench.py tests/test_joins_lab.py tests/test_fjms_service.py tests/test_tabular_builder_rtl.py -q
- QT_QPA_PLATFORM=offscreen pytest tests/test_join_workbench_construct.py -q  (update the table columnCount assertion 9→8 for the removed Source column; keep the round-trip tests)
- ruff check desktop/join_workbench.py genizah_app.py genizah_translations.py tests/test_join_workbench_construct.py
- Sanity: offscreen-construct JoinWorkbenchWindow(parent=None, app=MagicMock()) → opens; table has 8 cols.
- grep -nE "self\._app\._vs_|\._vs_[a-z]" desktop/join_workbench.py → no matches.
