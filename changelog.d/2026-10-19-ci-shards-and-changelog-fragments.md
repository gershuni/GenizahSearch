## [Unreleased] -- Faster CI, and changelog entries that do not conflict (ci, branch `ci/changelog-fragments-split-tests`)

- **The `tests` job runs as three shards per OS.** As one process it took 41-52 min on
  windows-latest and ~25 on Ubuntu, which was the wait on every PR and was paid again on every
  push. Each shard runs its share of `scripts/run_local_tests.py`'s balanced chunks
  (`--shard K/N`, new), each chunk in a fresh process, the same chunks local runs use. A
  `tests` check (`tests-all`) reports one result for all six shard jobs.
- **Changelog entries go in `changelog.d/`, one file per PR**, instead of the top of
  `CHANGELOG.md`, where every PR's entry collided with every other open PR's after each merge
  (2026-10-19: three merges, each restarting every open PR's CI).
  `scripts/collect_changelog.py --apply` folds them into `CHANGELOG.md` at release;
  `tests/test_changelog_fragments.py` checks every fragment.
