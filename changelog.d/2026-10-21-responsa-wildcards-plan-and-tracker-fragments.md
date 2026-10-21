## [Unreleased] -- Responsa wildcard plan; new tracker rows as fragment files (ci, branch `claude/responsa-wildcards-plan`)

- `docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md` (Codex SHIP after four rounds): Responsa `*` as any
  sequence of letters inside a word through the index's pattern lookup, `#` kept as the grammatical
  list, whole words at the query's edges, `#word#` keeping its suffixes, pages first. Measured on
  the local index. Decided by the owner: counts may change, and negated words keep today's
  behaviour; still provisional, pending Phase 0's numbers: how a word that starts with a star is
  looked up (accept the cost, a reversed field, or a word list beside the index) and where
  page-break matches come from under a star (seam documents, or none with a notice). No product
  code.
- `docs/open_issues.d/`: a newly found P1/P2/P3 bug is written as a file of its own, as
  `changelog.d/` does for the changelog, so open PRs stop conflicting on the tracker's counts and
  notes. `scripts/collect_open_issues.py --apply` folds the files in (rows, Quick Summary counts,
  one count note); `tests/test_open_issues_fragments.py` validates every file; `scripts/init.ps1`
  lists the pending ones. First fragment: the website's expanded result text highlights after
  HTML-escaping (P3).
- `scripts/run_local_tests.py` no longer crashes printing a failing chunk's Hebrew output on a
  cp1252 console (the Windows CI runner), which had hidden the name of the failing test.
