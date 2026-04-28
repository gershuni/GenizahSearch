# Tests

Standard invocation:

```bash
pytest tests/
```

## Slow tests

Some tests are decorated with `@pytest.mark.slow` because they perform sustained
HTTP-burst soaks or other long-running validations. Examples:

- `tests/test_search_api_soak.py` (Phase 78 rate-limit soak — D-22 form 1)
- `tests/e2e/test_performance.py` (existing performance suite)

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
