## [Unreleased] -- Web: editions on pages without an automatic transcription (web, branch `master-main`)

### Improvements
- **A page without an automatic transcription now opens its FGP or PGP edition.** The reading view built its
  version menu -- and its Edit, Comment, Notes and Joins buttons -- only when the page had an automatic text, so on
  such a page an edition could not be reached at all (T-S NS 243.146, page 1). The page's automatic text is now
  taken as empty: the menu is always there, the usual default rule chooses the edition when there is one, and the
  page's citation credits that edition. Edit there starts from the empty text and is saved as the reader's own
  version, as elsewhere (the question of editing an edition itself is GS-277). A catalogue-only record without a
  page still gets none of these (`web/pages/browse_state.py::page_has_reading_view`). Plan and the owner's
  decisions: `docs/plans/EDITIONS_WITHOUT_AUTOMATIC_TEXT_PLAN.md`.
