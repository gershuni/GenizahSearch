## [Unreleased] -- Web: Quick View on pages without an automatic transcription (web, branch `claude/awesome-lamport-1ph5sq`)

### Improvements
- **Quick View now offers the version chooser on a page without an automatic transcription**, as Browse has since
  2026-10-27. Such a page is reached in Quick View by paging from a hit on another page of the manuscript, or from
  a shelfmark or title search for a manuscript with no automatic text at all. Quick View already showed an edition
  of such a page as its text, but with no label saying whose it was, no way to switch versions, and no Edit,
  Comment or Joins: it built them only when the page had an automatic text. It now takes Browse's decision
  (`web/pages/browse_state.py::page_has_reading_view`, through
  `web/pages/search_results.py::_quick_view_has_reading_view`): the chooser names the edition shown (FGP or PGP)
  and offers the others, and Edit starts from the empty automatic text, as in Browse (GS-277). A page with nothing
  to show says "No text available": paging there from a hit used to show the hit page's snippet as if it were that
  page's text. A catalogue-only record still gets none of these. Quick View has no citation of its own (the "How to
  cite" chip sits beneath dialogs), so nothing there is cited.
