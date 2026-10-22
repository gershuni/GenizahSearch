# docs/open_issues.d -- new tracker rows, one file per change

Write a newly found bug here, as a file of its own, **not** in
`docs/OPEN_ISSUES.md`. A row added to the tracker also moves its Quick Summary
counts, the count notes and the "Last Updated" line, so any two open PRs that
both found something conflicted on the same lines, and resolving that restarted
their CI. Two files never conflict. This is the tracker's version of
`changelog.d/`.

- **Name:** `YYYY-MM-DD-<short-lowercase-slug>.md`, e.g.
  `2026-10-21-web-highlight-after-escaping.md`.
- **Content:** one or more blocks. Each block is a bug-section heading, written
  exactly as the tracker writes it (`### P1 - Critical`, `### P2 - Medium` or
  `### P3 - Low Priority`), then that section's new rows in the tracker's own
  four-cell form:

  ```
  ### P3 - Low Priority
  | **Title** | `path/file.py` | ❌ Open (2026-10-21) | What, where, how measured, fix sketch. |
  ```

  Every row's status must start with an open mark (❌, ⏸ or ⏳): the tracker
  holds only open items.
- **Not here:** closing a row (mark it fixed and move it to
  `docs/archive/OPEN_ISSUES_ARCHIVE.md`, as before), rewriting an existing row,
  and rows for the tracker's other sections (they have no per-section count).
- **Check:** `tests/test_open_issues_fragments.py` validates every file here in
  CI, including that its sections exist in the tracker.
- **Fold:** `python scripts/collect_open_issues.py --apply` puts the rows at the
  top of their sections, adds them to the Quick Summary counts and the total,
  writes one count note naming the fragments, rewrites the "Last Updated" line
  with the new total, and deletes them. A session that only wrote fragments
  leaves the tracker's counts and header alone. Run it at a
  release, alongside `scripts/collect_changelog.py`, or whenever the pile grows.
  Until then `scripts/init.ps1` lists the pending fragments under the tracker, so
  a session reading the tracker sees them too. Running it again is safe: a row
  the tracker already holds is not added or counted twice, so a run that could
  not delete a fragment is finished by the next one.
