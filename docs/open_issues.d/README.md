# docs/open_issues.d -- new defects, one file per change

Write a newly found defect here, as a file of its own, **not** in
`docs/OPEN_ISSUES.md`. A row added to the tracker also moves its Summary counts
and the "Last Updated" line, so any two open PRs that both found something
conflicted on the same lines, and resolving that restarted their CI. Two files
never conflict. This is the tracker's version of `changelog.d/`.

- **Name:** `YYYY-MM-DD-<short-lowercase-slug>.md`, e.g.
  `2026-10-21-web-highlight-after-escaping.md`.
- **Content:** one or more issues. Each is a `##` heading with the priority, the
  area and a one-line headline, a `Where:` line, optional `Opened:` (defaults to
  the file's date) and `Status:` (defaults to `❌ Open`) lines, a blank line, and
  the details -- what happens, where, how it was measured, a fix sketch:

  ```
  ## P3 search: A crossing from an earlier page can hide the next page's own
  Where: `shared/search_engine.py::_cross_page_spans`
  Opened: 2026-10-22

  At each page break of a whole-manuscript document, ...
  ```

  Priorities: `P1`, `P2`, `P3`. Areas: `search`, `browse`, `desktop`, `web`,
  `api`, `discovery`, `community`, `data`, `build-ci`, `docs`, `ops` (what each
  covers: `scripts/open_issues_model.py`). A status must start with an open mark
  (❌, ⏸ or ⏳): the tracker holds only open defects.
- **Not here:** closing an issue (set its status to `✅ Fixed (YYYY-MM-DD, <sha>)`
  and run `python scripts/archive_closed_issues.py --apply`), rewriting one (edit
  its row and its section in `docs/ISSUE_DETAILS.md`), and backlog items -- debt,
  test gaps, ideas, owner tasks -- which go straight into `docs/BACKLOG.md`.
- **Old form:** a fragment written before 2026-10-25 (`### P2 - Medium` and a
  four-cell row) is still accepted; its rows land under "Unsorted" until someone
  gives them an area.
- **Check:** `tests/test_open_issues_fragments.py` validates every file here in
  CI, and checks that the tracker, the backlog and the details agree.
- **Fold:** `python scripts/collect_open_issues.py --apply` gives each issue the
  next free ID, puts its row in the tracker and its details in
  `docs/ISSUE_DETAILS.md`, regenerates the Summary and the "Last Updated" line,
  and deletes the fragments. Run it at a release, alongside
  `scripts/collect_changelog.py`, or whenever the pile grows. Until then
  `scripts/init.ps1` lists the pending fragments under the tracker. Running it
  again is safe: an issue the tracker already holds is not added twice.
