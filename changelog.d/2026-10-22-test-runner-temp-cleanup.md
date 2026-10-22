## [Unreleased] -- The local test runner cleans up after itself (ci, branch `claude/test-lanes-cleanup`)

- `scripts/run_local_tests.py` now writes each run's `tmp_path` files into a folder of the run's own
  under `%TEMP%\genizah-test-lanes\`, and removes it when the run passes. A full run used to leave
  about 18 GB there until the next run started. A failed run keeps its folder for inspection; any
  later run removes it once it is a day old.
- Two runs at once no longer delete each other's files: a run used to start by wiping the one
  shared folder (133 errors in a second run, 2026-09-20).
