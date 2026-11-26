## [Unreleased] -- New Search hides the Title / Catalog switches (web, branch `claude/gracious-wright-vbzm8y`)

After a Title or Catalog search, New Search put the mode back to Exact but left the
"Fuzzy search" and "Automatic translations" switches showing under the Exact search: the
mode was set in code, which fires no change event, so the mode's own controls did not
follow it. New Search now runs the mode-change handler (`web/pages/search.py::_reset_search`),
as does cancelling a refinement step that restores a Title or Catalog query. Regression test
in `tests/render_smoke/test_new_marks_render_smoke.py`.
