## [Unreleased] -- The open-issues tracker rebuilt: one line per defect, a backlog, details by ID (ci, branch `chore/open-issues-reconstruct`)

- `docs/OPEN_ISSUES.md` had grown to 179 KB: 256 paragraph-long rows in insertion order, a
  10 KB log of count changes, hand-kept counts that had drifted (P2 48 claimed, 45 marked
  open), and sections for milestones long closed. Every open row, and the 18 discovery items
  parked in `docs/archive/OPEN_ISSUES_V9_DISCOVERY_OPEN_ITEMS.md` on 2026-10-23, was re-checked
  against the code at `ca809c56` (eleven agents, one batch each; every "fixed" verdict had to
  cite code, and each closure was confirmed by a second reading before it moved).
- Result: 9 rows closed into `docs/archive/OPEN_ISSUES_ARCHIVE.md` (fixed or overtaken by
  v9.8-v9.10 work, each with its evidence), 158 open defects in the tracker (P1 0, P2 38,
  P3 120) at 40 KB, and 109 items in the new `docs/BACKLOG.md` -- deferred defects, code debt,
  test gaps, ideas, server and owner tasks. One new defect found by the triage and reproduced:
  list sync fails scenario invariant 14 on seeds 11362 and 11917 (an edit made during an upload
  can lose a website entry).
- Every row has a stable ID (`GS-<n>`), sits under its priority and area, and links to its
  full text in the new `docs/ISSUE_DETAILS.md` (each old row quoted verbatim, then its triage
  note). The Summary is generated: `scripts/open_issues_model.py` defines the form,
  `scripts/collect_open_issues.py` folds fragments in (new fragment form `## P3 search:
  headline`, `Where:`, details; the old form is still accepted) and with `--check` reports
  drift, `scripts/archive_closed_issues.py` moves a `✅` row and its details to the archive and
  never reuses an ID. `tests/test_open_issues_fragments.py` fails CI when the three files
  disagree or a count, an order or a heading drifts.
- `scripts/check_docs.py`: the tracker's ceiling is lowered from 180,000 to 80,000 bytes, and a
  line-number citation of `BACKLOG.md` or `ISSUE_DETAILS.md` fails like one of the tracker.
