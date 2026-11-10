# 0005 -- Document classes decide what is authoritative

- **Status:** binding
- **Date:** 2026-09-18 (repo-structure Round 1, after an external review of the plan); amended
  2026-11-05, when the planning records and archived history moved to the private repository and
  GSD was retired
- **Source:** the Round 1 plan and its review; the rule the review replaced was "`.planning/` is
  history, `docs/` is truth", which was wrong in both directions.

## Decision

| Class | Lives in | Rule |
|---|---|---|
| **Current architecture** | `docs/architecture/`, `docs/guides/`, `docs/specs/` pages marked current | authoritative; kept current by the doc gates |
| **Planning records and archived history** | the private repository, outside this tree | not linked from tracked docs |

Every superseding document names what it supersedes.

## Why

Planning records live in the private repository, and GSD, the tool that once owned them, is
retired. What this tree carries is documentation of the code as it is, so one rule applies to it:
a current page under `docs/architecture/`, `docs/guides/` or `docs/specs/` is the authority.

## Consequences

- A tracked document never links into the private repository; `scripts/check_docs.py` reports a
  link to a file that is not in this tree as broken.
- A test that compares code with a planning or operations record reads it through
  `tests/private_records.py` and skips where the private checkout is absent; nothing from the
  private repository is copied into this tree, and anything a test must always read belongs here.

## Supersedes

"`.planning/` is history, `docs/` is truth."

## Enforced by

Scan scope of [scripts/check_docs.py](../../scripts/check_docs.py); the rest is convention stated in
[docs/architecture/OVERVIEW.md](../architecture/OVERVIEW.md).
