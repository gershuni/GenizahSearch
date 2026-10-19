# changelog.d -- one file per change, folded into CHANGELOG.md at release

Write your PR's changelog entry here as a file of its own, **not** in
`CHANGELOG.md`. Every PR adding its entry at the top of `CHANGELOG.md` made each
merge conflict with every other open PR, and resolving that restarted their CI.
Two files never conflict.

- **Name:** `YYYY-MM-DD-<short-lowercase-slug>.md`, e.g.
  `2026-10-19-joins-lab-library-domain.md`.
- **Content:** exactly the entry `CHANGELOG.md` would have held. The first line
  is `## [Unreleased] -- <title> (<web|desktop|both>, branch `<branch>`)`, then
  the body. `###` subsections are fine; a second `## ` heading is not.
- **Check:** `tests/test_changelog_fragments.py` validates every file here in CI.
- **Release:** `python scripts/collect_changelog.py --apply` folds the fragments
  into `CHANGELOG.md` as `[Unreleased]` entries, newest first, and deletes them.
  The release then writes its `## [X.Y.Z]` section from them, as before.
