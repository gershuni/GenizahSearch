# Tests

Standard invocation:

```bash
python scripts/run_local_tests.py
```

## Why not `pytest tests/`

Measured 2026-09-09 on the owner's machine (24 cores, 63 GB): the full non-GUI
selection as ONE pytest process was killed after **2h35m without finishing**,
holding 38.4 GB resident and **91.6 GB of private commit** -- system commit
129.2 of 130.7 GB, 106 million page faults -- and using **45% of one core out
of 24**. It was not computing, it was paging.

The same selection in chunks of <=30 files, each chunk its own process,
finished in 90.9 minutes with no chunk over 5.6 GB. Process LIFETIME is the
variable: this suite retains roughly 18 MB per module and never gives it back,
so 500 modules in one process crosses the machine's memory and throughput
collapses non-linearly. `scripts/run_local_tests.py` does that split, balances
the chunks by measured duration (`tests/lane_durations.json`), and runs four
lanes with a private `--basetemp` each.

xdist is installed but is not the answer here: its workers collect the whole
selection and persist across many files, and `--dist loadfile` keeps a file's
tests together without recycling the worker -- the shape that failed.

## Can the runner report success without running the tests?

No, and that is enforced rather than intended. Every path to a green run goes
through one pure function, `_verdict`, so the question is answerable by reading
about sixty lines; `tests/test_run_local_tests_gate.py` drives that function
directly and asserts each red path, and the three fixes below were each proven
to redden it before being trusted.

A run fails on: a non-zero chunk exit, a chunk with no parseable summary (an
OS-killed chunk, a crashed interpreter, a usage error), a chunk that recorded
no result at all, a chunk reporting failures while exiting 0, and zero tests
passed overall.

pytest exit 5 (`NO_TESTS_COLLECTED`) is the subtle one and is split by what the
chunk accounted for. `43 deselected` or `3 skipped` means the tests were found
and deliberately not run -- legitimate, and printed rather than hidden.
`no tests ran` accounts for nothing, which is what a renamed module or a
mistyped path looks like, and fails. Codex found this on PR #337: exempting
every exit 5 let one passing test in any other chunk print "All chunks passed."
over a chunk that ran nothing.

Two related traps, same review. A directory may only be excluded from the plan
if the marker expression already deselects its tests -- otherwise the exclusion
silently removes tests the equivalent CI command runs, which is what had
happened to `tests/e2e/` (no e2e job exists in `ci.yml`; the main `tests` job
runs it). And the summary parser now searches each category independently,
because pytest does not emit them in a fixed order: it puts `errors` last, and
`failed` before `passed`, so the previous ordered pattern read a real line of
this suite's own output as `426 passed / 200 errors` while losing its 2 skipped
and 53 deselected entirely.

### `-m` is honoured, not overridden

The same exclusion rule has to answer to the *active* expression, not the
default one. `-m "not gui"` selects 632 tests under `tests/render_smoke/` and
`tests/atlas_bake/`, and those directories were being dropped unconditionally
-- 632 selected tests silently omitted, under a header claiming they were
deselected, exiting green. So the decision is now asked of pytest
(`--collect-only`) rather than parsed out of the expression text:

| the expression | what happens to the directory |
|---|---|
| deselects every test in it | skipped, and reported, since skipping changes nothing |
| selects any test in it | **planned in chunks of its own**, so it runs without sharing a process |
| cannot be answered (e.g. a malformed `-m`) | the run **aborts** -- "I could not tell" must never mean "safe to omit" |

The routine run pays nothing for this: under the default expression the answer
is a constant, and `test_a_directory_is_only_skipped_when_the_markers_really_deselect_it`
is what verifies that constant against the collector once per suite run.
Collecting `tests/render_smoke/` costs 34 seconds, so probing it on every run
would be 10% of the wall clock spent re-learning a known fact.

## The one check that is not in any routine run

`tests/test_verify_v3_review_offsets.py` proves the offset verifier can FAIL.
It used to do that against the real `discovery-v5-REVIEW.db` (3.45 GB, 519,382
rows), copying it once per mutation: **3,396 seconds for six tests, 66.5% of
the entire suite's module time** -- and paid only here, because the file is
`skipif(DB is None)` and CI has no review DB.

The mutation matrix now runs against `tests/review_offsets_fixture.py` (a
handful of rows over a handful of characters, hand-specified offsets) in about
a second. The full-artifact check moved, it did not vanish:

```bash
python scripts/verify_review_artifact.py          # fails on missing data, never skips
python scripts/verify_review_artifact.py --expect-rows 519382   # before promoting a rebuild
powershell -File scripts/schedule_nightly_artifact_verify.ps1   # register it nightly
```

A green routine suite is NOT evidence that the artifact verified. On a routine
run that check does not execute at all.

## Slow tests

Some tests are decorated with `@pytest.mark.slow` because they perform sustained
HTTP-burst soaks or other long-running validations. Examples:

- `tests/test_search_api_soak.py` (Phase 78 rate-limit soak — D-22 form 1)
- `tests/e2e/test_performance.py` (existing performance suite)
- `tests/test_verify_v3_review_offsets.py::test_the_real_review_artifact_verifies_clean`
  (the full 3.45 GB artifact scan — and note that `slow` alone does NOT
  deselect it here, which is why it carries `GENIZAH_VERIFY_REAL_ARTIFACT=1` as
  well; see the section above)

Phase 78 (Concern #7 from 78-REVIEWS.md) **deliberately does NOT** add a
repo-wide `addopts = -m "not slow"` default-exclude to `pyproject.toml`. That
would silently exclude slow tests for every developer + CI invocation,
changing behavior for tests unrelated to Phase 78.

CI organisation (R2-#5 from round 2 review): `.github/workflows/ci.yml` has
TWO test jobs:

- `tests` (unchanged from pre-Phase-78): `pytest tests/` on ubuntu + windows
  matrix. Slow tests are NOT excluded here — Concern #7 preservation.
- `slow-tests` (NEW in Phase 78): `pytest -m slow tests/` on ubuntu only.
  Dedicated gate for the new soak suite. R2-#5.

### Running slow tests explicitly

```bash
# Run ONLY slow tests:
python -m pytest -m slow

# Run only Phase 78's rate-limit soak:
python -m pytest -m slow tests/test_search_api_soak.py

# Run ONLY non-slow tests (opt-OUT for fast iteration):
python -m pytest -m "not slow"
```

The `slow` marker is registered in `pyproject.toml` so `--strict-markers`
workflows accept `@pytest.mark.slow` without UnknownMark warnings.
