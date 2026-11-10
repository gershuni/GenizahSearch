# 0003 -- Broad test selections run through the bounded runner

- **Status:** binding
- **Date:** 2026-09-09
- **Source:** the measurement recorded in [tests/README.md](../../tests/README.md) ("Why not
  `pytest tests/`") and the CLAUDE.md "Testing" section.

## Decision

Any selection broader than one file or one subdirectory of `tests/` runs through
[scripts/run_local_tests.py](../../scripts/run_local_tests.py), which splits the selection into
chunks of at most 30 files (`--max-files`), runs each chunk in its own disposable pytest process,
four chunks at a time (`-j/--lanes`, default 4), under CI's own marker expression; `--dry-run`
prints the plan and `--record` refreshes the duration table it balances by
(`tests/lane_durations.json`). Direct `pytest` is for a named file (`pytest tests/test_x.py`) or a directory narrower
than `tests/` (`pytest tests/atlas_bake -m atlas_bake`). One `pytest tests/` process is never run
locally, with or without a marker.

## Why

The full non-GUI selection as one process was killed after 2h35m unfinished, holding 91.6 GB of
private commit on a 63 GB machine and paging instead of computing. The suite retains memory per
module and never releases it; process lifetime is the variable that matters. The same selection in
bounded chunks finished in about 90 minutes.

## Consequences

- CI's `tests` job still runs the selection as one process on a fresh runner; that is CI's choice
  and not a template for a developer machine.
- The runner's `-m` **replaces** its default expression, so a custom lane must repeat the exclusions
  it wants. The default -- and CI's -- is `"not gui and not render_smoke and not atlas_bake"`;
  note it does **not** exclude `slow` (that omission is deliberate, see `pyproject.toml`).
- The runner refuses to report success without having run the tests
  ([tests/test_run_local_tests_gate.py](../../tests/test_run_local_tests_gate.py) proves it).
- Instruction files state the policy once, in `AGENTS.md`, and every other file follows it.

## Supersedes

`pytest tests/` as the documented full-suite command (AGENTS.md, tests/README.md and the release
skill before 2026-09-18).

## Enforced by

[tests/test_run_local_tests_gate.py](../../tests/test_run_local_tests_gate.py) for the runner
itself; [tests/test_instruction_files_agree.py](../../tests/test_instruction_files_agree.py) for
the commands the instruction files document (every pytest line in their fenced code blocks must
name a path narrower than `tests/` or be the runner).
