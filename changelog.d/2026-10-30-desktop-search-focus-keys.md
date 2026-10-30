## [Unreleased] -- The cursor starts in the search box; Ctrl+O, Ctrl+E and a list of keys (desktop, branch `desktop-search-focus-keys`)

- **At launch** the cursor is in the open section's search box from the moment the window
  shows (`_focus_on_launch`), so typing works while the index loads. After the session restore
  the restored words are selected (`_refocus_after_restore`), unless the user has typed or
  pasted there (`textEdited`), opened another section, or moved the cursor meanwhile.
- **Ctrl+1 .. Ctrl+7** open a section with the cursor in its search box (`_open_section`),
  the same field Ctrl+F picks. A click on a tab leaves the cursor where it was, so Space
  still pages the results.
- **Ctrl+E** puts the cursor in the search box as Ctrl+F does (File Explorer's and Outlook's
  key for it). On **Browse by Shelfmark** with no manuscript open, both go to the shelfmark
  box rather than Find; in **PGP Tags** mode, to the tag box.
- **When a search finds results** (a text search or a PGP tag search) the cursor moves to the
  results table, on the first shown row and not on the check box column, so Space pages and the
  arrow keys move at once (`_focus_results_after_search`). Only on the Search tab and only while
  the cursor is still where the search was asked for (`_focus_at_search_start`; a search that
  waits for the filter lookup keeps its own in `_deferred_search_focus`, so a running search and
  a queued one never share it): a field or section the user went to meanwhile keeps it. No shown row (no results, the all-terms filter, exclusions) leaves it in the box, and the
  rest of the first page gets a second look; a session restore does not move it.
- **Ctrl+O** opens Go to, as Ctrl+G does.
- **Keyboard shortcuts** (`desktop/shortcuts_dialog.py`): every key on one card, from Ctrl+/,
  an entry in the ☰ menu and the side menu, and a ⌨ button beside A− in the tab layout's
  top row. Its list is checked both ways against the keys the window binds, and names the
  history (Down), paging (Space) and Composition stop (Esc) keys. Reviewed by Codex (round 1:
  six MEDIUM and one LOW; round 2: five P2 edge cases; rounds 3-5: two, one and one P2
  regressions of those fixes, the last closed by keeping a queued search's place apart; all
  fixed).
- Ctrl+O, Ctrl+E and Ctrl+/ also answer to the physical key on a Hebrew layout
  (`LayoutProofShortcuts`). Help (both languages) names the new keys. Pinned in
  `tests/test_quick_open.py`.
