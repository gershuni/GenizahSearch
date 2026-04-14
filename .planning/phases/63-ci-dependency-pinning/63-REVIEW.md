---
phase: 63-ci-dependency-pinning
reviewed: 2026-04-14T00:00:00Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - .github/workflows/ci.yml
  - ruff.toml
  - requirements.txt
  - requirements-lock.txt
  - docs/guides/DEVELOPER_GUIDE.md
findings:
  critical: 0
  warning: 3
  info: 2
  total: 5
status: issues_found
---

# Phase 63: Code Review Report

**Reviewed:** 2026-04-14
**Depth:** standard
**Files Reviewed:** 5
**Status:** issues_found

## Summary

Phase 63 introduces CI infrastructure (GitHub Actions workflow), a ruff linting configuration, a two-file dependency strategy (`requirements.txt` + `requirements-lock.txt`), and an updated developer guide. The overall approach is sound. Three warnings require attention before this can be considered stable: an unpinned `pytest` install in CI, a stale `fastapi` entry in the lock file (the FastAPI backend was removed in January 2026), and an inconsistency in the developer guide's environment variable documentation.

---

## Warnings

### WR-01: Unpinned `pytest` install in CI tests job

**File:** `.github/workflows/ci.yml:37`
**Issue:** The tests job installs pytest without a version pin (`pip install pytest`), while `requirements-lock.txt` lists `pytest==9.0.2`. This means the two install paths are not equivalent: a future pytest release could silently change test behaviour or introduce breaking API changes in CI. The lint-and-docs job correctly pins ruff (`ruff==0.15.10`), but the tests job does not apply the same discipline to pytest.
**Fix:**
```yaml
- run: pip install pytest==9.0.2  # Keep in sync with requirements-lock.txt
```
Alternatively, add `pytest` to `requirements-lock.txt`'s install step instead of a separate `pip install` line, so the lock file alone controls the entire environment.

---

### WR-02: Removed dependency (`fastapi`) present in lock file

**File:** `requirements-lock.txt:25`
**Issue:** `fastapi==0.135.1` (and its subdependency `starlette==0.52.1` on line 97) appear in the lock file. Per CLAUDE.md, FastAPI was removed from the project in January 2026. This entry is stale. Its presence means CI installs a package that is not used, expanding the attack surface and obscuring the true dependency footprint. Stale lock entries also mislead future developers about what the project requires.
**Fix:** Regenerate the lock file after confirming FastAPI is not imported anywhere in the codebase:
```bash
pip freeze > requirements-lock.txt
```
Then audit the new output for other unexpected entries (e.g., `anthropic==0.84.0` on line 8, which also does not appear in `requirements.txt` and is not a transitive dependency of any listed direct dependency).

---

### WR-03: Environment variable table in DEVELOPER_GUIDE.md is incomplete

**File:** `docs/guides/DEVELOPER_GUIDE.md:288-295`
**Issue:** The Quick Start section (lines 56-57) documents `WEB_PUZZLE_ENABLED` and `PUZZLE_UPLOAD_SECRET` in the sample `.env` block, but the "Environment Variables Reference" table (lines 288-295) omits both variables. A developer scanning the reference table will not find these variables, and the guide will contradict `CLAUDE.md` (which documents both). The table currently lists 7 variables; 2 from the `.env` sample are missing.
**Fix:** Add the missing rows to the reference table:

```markdown
| `WEB_PUZZLE_ENABLED` | No | Enables web puzzle page (default: true; set false to hide) |
| `PUZZLE_UPLOAD_SECRET` | No | HMAC secret for puzzle upload tokens (auto-generated if unset) |
```

---

## Info

### IN-01: `ruff` version string looks unusual — verify it is correct

**File:** `.github/workflows/ci.yml:16` and `requirements-lock.txt:91`
**Issue:** Both files pin `ruff==0.15.10`. As of April 2026, ruff's published release history uses 3-segment version numbers, but `0.15.x` is a major version jump from the project's recent past. Verify this is intentional and not a typo for `0.5.10` or `0.1.15`. The two files are internally consistent with each other, but if the version string is wrong, CI's lint job will fail on install.
**Fix:** Confirm `pip index versions ruff` shows `0.15.10` as a valid release. If it is a typo, correct both files atomically.

---

### IN-02: `check_docs.py` staleness check uses timezone-naive datetime

**File:** `scripts/check_docs.py:116`
**Issue:** `datetime.now()` returns a naive datetime (no timezone). This works correctly in most CI environments but Python 3.12+ emits deprecation warnings for naive datetime arithmetic in some contexts. The staleness threshold logic is correct and will not misfire, but it is worth noting for future-proofing.
**Fix:**
```python
from datetime import datetime, timedelta, timezone

cutoff = datetime.now(tz=timezone.utc) - timedelta(days=STALE_THRESHOLD_DAYS)
```
Also update the `strptime` call to attach UTC before comparing:
```python
date = datetime.strptime(match.group(1), '%Y-%m-%d').replace(tzinfo=timezone.utc)
```

---

_Reviewed: 2026-04-14_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
