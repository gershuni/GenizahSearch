## [Unreleased] -- Focus Search is its own on the Search tab and on the Composition tab (desktop, branch `claude/gs-13-scope-per-tab`)

GS-13. The desktop Search tab and Composition tab shared one Focus Search:
setting it on one tab restricted the other, a lookup running for one held the
other's run, and New on either tab cleared both.

- Each tab now has its own scope (`desktop/search_scope.py`): its filters, the
  manuscripts they allow, and the lookup that computes them, with its own
  error, its own waiting run (K-14) and its own generation.
- Composition searches, their result cache key and the Conditions fingerprint
  read the Composition scope; the Search tab, refinement and the visual
  suggestions read the Search scope. The catalogue's "Search in results" and
  "Parallels in results" each set the scope of the tab they open.
- New clears only its own tab's Focus Search.
- History: a composition entry saves and restores the Composition scope.
- Session file: a new `comp_pre_search_filters` key beside
  `pre_search_filters` (the Search tab's). A session saved before this has only
  the shared key, which restores to the Search tab; Composition starts with none.
- `GenizahGUI.pre_search_filters` / `pre_search_restrict_sys_ids` keep their
  names (the Search tab's); `comp_pre_search_filters` /
  `comp_pre_search_restrict_sys_ids` are the Composition tab's.
- Tests: `tests/test_filter_unavailable_desktop_window.py` rewritten for one
  scope per tab, with GS-13 cases; session save and restore in
  `tests/test_exclusion_surfaces.py`.
