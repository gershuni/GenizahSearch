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
