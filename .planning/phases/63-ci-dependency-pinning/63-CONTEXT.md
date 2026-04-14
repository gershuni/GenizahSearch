# Phase 63: CI & Dependency Pinning - Context

**Gathered:** 2026-04-14
**Status:** Ready for planning

<domain>
## Phase Boundary

This phase delivers reproducible builds and an automated regression safety net. After this phase, every push and PR runs pytest + ruff + check_docs, and dependency versions are locked. No user-visible behavior changes.

</domain>

<decisions>
## Implementation Decisions

### CI Workflow Design
- **D-01:** Single `ci.yml` workflow replaces the existing `docs-check.yml`. Workflow name: `CI`.
- **D-02:** Two separate jobs inside the workflow:
  - `lint-and-docs` on Ubuntu: runs `ruff` and `scripts/check_docs.py`
  - `tests` as a matrix job: Ubuntu (Python 3.10) + Windows (Python 3.11), runs `pytest tests/`
- **D-03:** `tests` job depends on `lint-and-docs` (fast-fail: don't waste CI time running tests if lint fails)
- **D-04:** Triggers: `pull_request` for all PRs, `push` to `master-main` only. No CI on every branch push.
- **D-05:** Python versions: Ubuntu 3.10 (matches project minimum per CLAUDE.md), Windows 3.11. If 3.10 proves problematic, raise documented minimum.

### Ruff Configuration
- **D-06:** Initial ruleset: `select = ["E", "F"]` only (syntax errors + Pyflakes). No isort, no warnings, no style rules.
- **D-07:** Line length: set `line-length = 120` in config as future-friendly default, but do NOT enable line-length rules yet.
- **D-08:** Config goes in `pyproject.toml` (create if doesn't exist) or `ruff.toml` — Claude's discretion on which.
- **D-09:** Any existing E/F violations must be fixed before CI is merged — zero violations baseline.

### Dependency Pinning
- **D-10:** Two-file strategy:
  - `requirements.txt`: direct dependencies only, exact pins (`==`), human-maintained
  - `requirements-lock.txt`: full `pip freeze` output, generated, committed
- **D-11:** CI installs from `requirements-lock.txt` (validates full reproducibility, not just declared surface).
- **D-12:** Pinning done LAST in this phase, after CI is green with unpinned deps, so we don't re-pin after fixing ruff violations.
- **D-13:** DEVELOPER_GUIDE.md documents the upgrade workflow: edit requirements.txt, regenerate lock file, verify CI green.

### Claude's Discretion
- Exact ruff config file location (pyproject.toml vs ruff.toml)
- GitHub Actions action versions (checkout@v4, setup-python@v5, etc.)
- Whether to add pip caching in CI for speed
- How to handle the `gotrue` entry in requirements.txt (it's a Phase 64 concern but may need a note)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Existing CI
- `.github/workflows/docs-check.yml` -- Current docs-only workflow to be replaced
- `scripts/check_docs.py` -- Documentation health check script (must remain callable)

### Dependencies
- `requirements.txt` -- Current bare dependency list (14 packages, no versions)
- `CLAUDE.md` -- Project conventions, mentions Python 3.10+

### Documentation targets
- `docs/guides/DEVELOPER_GUIDE.md` -- Must be updated with dep upgrade workflow

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `scripts/check_docs.py` -- Already works standalone, just needs to be called in CI
- `.github/workflows/docs-check.yml` -- Template for workflow structure (actions/checkout@v4, setup-python@v5)

### Established Patterns
- Project uses `pip install -r requirements.txt` for dependency installation
- No pyproject.toml exists currently -- pure requirements.txt project
- Default branch is `master-main` (not `main`)

### Integration Points
- CI must install dependencies sufficient for pytest to collect 1,072 tests
- Some tests may require SQLite databases in specific locations -- check test fixtures
- PyQt6 tests may need display server on Linux (xvfb or headless)

</code_context>

<specifics>
## Specific Ideas

- Codex review specifically called out: "if CI installs from requirements.txt only, transitive drift can still surprise you" -- hence D-11 (install from lock file)
- Both code reviews agreed this phase should come first as the safety net for all subsequent phases
- User emphasized "approached very carefully so we won't break anything" -- conservative choices throughout

</specifics>

<deferred>
## Deferred Ideas

- Auth migration (gotrue replacement) -- Phase 64
- Expanding ruff ruleset (isort, warnings) -- future milestone
- Line length enforcement -- future milestone
- Type checking (mypy/pyright) -- out of scope for v7.8

</deferred>

---

*Phase: 63-ci-dependency-pinning*
*Context gathered: 2026-04-14*
