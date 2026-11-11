## [Unreleased] -- A failed local test run keeps only its failing chunks' files (ci, branch `claude/relaxed-lamport-d842ad`)

- `scripts/run_local_tests.py` kept every chunk's `tmp_path` folder after a failed run, about
  17.5 GB, for a day. With tests failing on master every run failed, and four runs in one morning
  filled drive C: (2026-11-11); the fifth broke with hundreds of `tmp_path` errors.
- Now a small pytest plugin (`scripts/lane_plugin/`) removes each test's `tmp_path` once the test
  has passed setup, call and teardown (pytest's own `tmp_path_retention_policy=failed` decides on
  the call alone and lost the folders of setup and teardown errors), a chunk that passed has its
  folder removed when it ends,
  and a failed run keeps only its failing chunks' folders. When it ends it writes `FINISHED.txt`
  and removes every earlier run that has one, so only the latest failure stays. A run without
  `FINISHED.txt` is still going (or was killed) and is left alone until it is a day old, as before.
- Measured on the same 11 failures: the kept folder went from 17.54 GB to 0.23 GB, and a run's
  folder peaks at about 1.1 GB while it runs (was 17.5 GB). Chunk times are unchanged.
- The runner's deletions now also remove paths over 260 characters and read-only files, which
  plain `rmtree` left behind on Windows, so a removed run folder goes whole.
- Pinned by `tests/test_run_local_tests_gate.py`, including a real pytest run of the chunk command.
