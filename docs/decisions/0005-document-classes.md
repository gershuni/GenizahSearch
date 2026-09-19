# 0005 -- Four document classes decide what is authoritative

- **Status:** binding
- **Date:** 2026-09-18 (repo-structure Round 1, after an external review of the plan)
- **Source:** the Round 1 plan and its review; the rule the review replaced was "`.planning/` is
  history, `docs/` is truth", which was wrong in both directions.

## Decision

| Class | Lives in | Rule |
|---|---|---|
| **Current architecture** | `docs/architecture/`, `docs/guides/`, `docs/specs/` pages marked current | authoritative; kept current by the doc gates |
| **Active planning** | `.planning/ROADMAP.md`, `STATE.md`, `PROJECT.md`, `.planning/phases/` | owned by the GSD tooling; edited only through it |
| **Audit evidence** | `.planning/milestones/*-MILESTONE-AUDIT.md`, `.planning/phase87_storage_allowlist.yaml`, phase decision files that tests read by path | cited as evidence; never moved |
| **Archived history** | `docs/archive/`, `.planning/milestones/*-phases/`, `.planning/quick/`, `.planning/debug/` | linked with "superseded by"; excluded from `check_docs` scans |

Every superseding document names what it supersedes. Every archived document carries a one-line
banner naming its successor.

## Why

`.planning/` holds three different things: the GSD tooling's live state, audit ledgers that are the
authority on what a milestone actually shipped (the v9.0.0 audit supersedes the archived v9.0.0
roadmap wherever they disagree -- CLAUDE.md says so), and phase history. `docs/` also mixes current
guides with 2026-02 snapshots. One rule per class is what a reader can apply.

## Consequences

- A tests-read planning file is evidence and does not move (see the memory rule recorded after the
  2026-09 move of phase-136/135 planning files broke hard-coded test paths).
- `scripts/check_docs.py` skips any path containing `archive`; archived pages therefore need their
  banner, not link maintenance.
- The GSD codebase map is archived history: its 2026-02-05 analysis lives in
  `docs/archive/codebase-snapshot-2026-02-05/`, and seven superseded pointer files are kept in
  `.planning/codebase/` under their original names because the local GSD tooling checks for them
  by name.

## Supersedes

"`.planning/` is history, `docs/` is truth."

## Enforced by

Scan scope of [scripts/check_docs.py](../../scripts/check_docs.py); the rest is convention stated in
[docs/architecture/OVERVIEW.md](../architecture/OVERVIEW.md).
