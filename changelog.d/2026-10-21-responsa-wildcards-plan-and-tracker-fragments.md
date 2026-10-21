## [Unreleased] -- Responsa wildcard plan; new tracker rows as fragment files (ci, branch `claude/responsa-wildcards-plan`)

- `docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md` (Codex SHIP after four rounds): Responsa `*` as any
  sequence of letters inside a word through the index's own word list, `#` kept as the grammatical
  list, whole words at the query's edges, `#word#` keeping its suffixes, pages first, and seam
  documents instead of whole manuscripts for page-break matches. Measured on the local index; nine
  owner decisions open. No product code.
- `docs/open_issues.d/`: a newly found P1/P2/P3 bug is written as a file of its own, as
  `changelog.d/` does for the changelog, so open PRs stop conflicting on the tracker's counts and
  notes. `scripts/collect_open_issues.py --apply` folds the files in (rows, Quick Summary counts,
  one count note); `tests/test_open_issues_fragments.py` validates every file; `scripts/init.ps1`
  lists the pending ones. First fragment: the website's expanded result text highlights after
  HTML-escaping (P3).
