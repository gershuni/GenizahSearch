## [Unreleased] -- Manuscript features in the Joins Lab (web + desktop, branch `claude/joins-lab-features`)

The v9.13.0 Manuscript features panel is now in the Joins Lab's filter dialog
on both apps ("More filters…" on the web, "Filter ▾" on the desktop), as a
"Manuscript features" button that opens the same window as Search and Browse by
Identification. Owner rulings 2026-11-02: inside the existing dialog, and one
place for each filter.

- **Filters the candidates already found**, with the candidates as the universe
  (D20): "Not recorded" and "All except" are read among them; the window's total
  says "N of the M manuscripts in the results match". Checked **per manuscript**:
  every candidate page of a matching manuscript stays. The Library and Domain
  counts follow the features.
- **One place for each filter:** the dialogs' own Material (web, desktop), Width
  (desktop) and "Has dimensions" / "Has dimensions data" (it went with the size
  filters) are gone; material and size are features. The desktop's anchor
  shortcuts fill in the features: "match material" as the measured material (the
  value the cards showed), "width ±2 cm" as the Width range. A link or saved web
  session with the old Material keeps it as the Material feature (measured); an
  old "Has dimensions" is dropped (it hides nothing); the desktop's old saved
  material row never restored, so nothing carries over.
- **Never an empty list for an error** (rule 9): while the check runs, and when it
  fails, every candidate shows with a note under the toolbar (web) or in the
  status line (desktop). A stale answer for older candidates or criteria is
  dropped. A saved feature a 5.0.0 catalogue file cannot answer is dropped with
  the usual notice.
- **Fixed (web):** when the catalogue data could not be read, a saved Domain
  filter hid every candidate; it and size mismatch now stand down and the page
  says so.
- **Web table view** now has the grid's toolbar (count, Library, Domain, Sort,
  More filters…), also when the filters leave nothing. The More filters… dialog
  lives in a host the re-renders never clear, so a late answer cannot close it.
- Desktop: features are saved in the Joins Lab session; the Joins Lab's Clear
  removes them.

Code: `web/components/criteria_panel.py` (`CriteriaControl(within=...)`),
`web/components/candidate_grid.py` (`compute_filtered(criteria_matched=,
catalogue_ok=)`, `candidates_toolbar`), `web/pages/joins_lab.py`,
`web/joins_lab_link.py`, `desktop/join_workbench.py`,
`desktop/gui_threads.py::CriteriaMatchWorker`. Tests:
`tests/test_joins_lab_criteria.py`, `tests/test_join_workbench_criteria_qt.py`,
render smoke in `tests/render_smoke/test_joins_lab_render_smoke.py`.
