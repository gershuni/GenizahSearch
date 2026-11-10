# 0007 -- `tests/` stays flat

- **Status:** binding for Round 1 and Round 2
- **Date:** 2026-09-18
- **Source:** the Round 1 inventory: a large share of test files compute the repository root as
  `Path(__file__).resolve().parent.parent` (or the `os.path` equivalent), and many read source files
  by that path.

## Decision

Test files stay directly under `tests/`. The existing lane subdirectories (`tests/atlas_bake/`,
`tests/render_smoke/`, `tests/e2e/`, `tests/scripts/`, `tests/fixtures/`) stay as they are. No
regrouping of tests into feature subdirectories.

## Why

Moving a test one level deeper silently changes what it thinks the repository root is; the tests
that read source by path would then scan nothing and pass. The bounded runner also balances lanes
by file, so the flat layout is what its duration table (`tests/lane_durations.json`) describes.

## Consequences

- A new test is `tests/test_<concern>.py`; a lane-specific one goes in its lane directory and
  carries that lane's marker.
- Never archive or move a file named `test_*.py` as part of tidying.

## Supersedes

Nothing.

## Enforced by

Convention. The path-reading tests themselves are the ones that would break.
