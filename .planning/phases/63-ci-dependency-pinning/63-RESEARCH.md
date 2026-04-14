# Phase 63: CI & Dependency Pinning - Research

**Researched:** 2026-04-14
**Domain:** GitHub Actions CI, ruff linting, pip dependency pinning
**Confidence:** HIGH

## Summary

This phase establishes a GitHub Actions CI pipeline and dependency pinning for the GenizahSearch project. The existing infrastructure is minimal: one `docs-check.yml` workflow that only runs `check_docs.py` on doc-related file changes, and a bare `requirements.txt` with 14 unpinned packages. The project has 1,072 tests (1,067 pass, 8 skip) that run in ~15 seconds, and `check_docs.py` is a pure-Python script with zero external dependencies.

The critical research finding is about ruff scope: the CONTEXT.md decision D-06 specifies `select = ["E", "F"]`, but running E+F on the codebase produces ~10,600 violations (9,183 are E501 line-too-long). Since D-07 explicitly says do NOT enable line-length rules, the actual ruleset must be `select = ["E", "F"]` with `ignore = ["E501"]` (and likely a few other E-series codes). A more practical interpretation aligned with "syntax errors + import hygiene" is to start with a narrow subset and use `per-file-ignores` or `ignore` to reach zero violations on the current codebase (D-09). The research below provides exact violation counts for each rule code to guide the planner.

**Primary recommendation:** Build the CI workflow first with unpinned deps, then fix ruff violations (mostly auto-fixable unused imports), then pin dependencies last.

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Single `ci.yml` workflow replaces the existing `docs-check.yml`. Workflow name: `CI`.
- **D-02:** Two separate jobs inside the workflow:
  - `lint-and-docs` on Ubuntu: runs `ruff` and `scripts/check_docs.py`
  - `tests` as a matrix job: Ubuntu (Python 3.10) + Windows (Python 3.11), runs `pytest tests/`
- **D-03:** `tests` job depends on `lint-and-docs` (fast-fail: don't waste CI time running tests if lint fails)
- **D-04:** Triggers: `pull_request` for all PRs, `push` to `master-main` only. No CI on every branch push.
- **D-05:** Python versions: Ubuntu 3.10 (matches project minimum per CLAUDE.md), Windows 3.11. If 3.10 proves problematic, raise documented minimum.
- **D-06:** Initial ruleset: `select = ["E", "F"]` only (syntax errors + Pyflakes). No isort, no warnings, no style rules.
- **D-07:** Line length: set `line-length = 120` in config as future-friendly default, but do NOT enable line-length rules yet.
- **D-08:** Config goes in `pyproject.toml` (create if doesn't exist) or `ruff.toml` -- Claude's discretion on which.
- **D-09:** Any existing E/F violations must be fixed before CI is merged -- zero violations baseline.
- **D-10:** Two-file strategy: `requirements.txt` (direct deps, exact pins), `requirements-lock.txt` (full pip freeze, committed)
- **D-11:** CI installs from `requirements-lock.txt`
- **D-12:** Pinning done LAST in this phase, after CI is green with unpinned deps
- **D-13:** DEVELOPER_GUIDE.md documents the upgrade workflow

### Claude's Discretion
- Exact ruff config file location (pyproject.toml vs ruff.toml)
- GitHub Actions action versions (checkout@v4, setup-python@v5, etc.)
- Whether to add pip caching in CI for speed
- How to handle the `gotrue` entry in requirements.txt (it's a Phase 64 concern but may need a note)

### Deferred Ideas (OUT OF SCOPE)
- Auth migration (gotrue replacement) -- Phase 64
- Expanding ruff ruleset (isort, warnings) -- future milestone
- Line length enforcement -- future milestone
- Type checking (mypy/pyright) -- out of scope for v7.8
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| BLDG-01 | All Python dependencies pinned to exact versions in requirements.txt, with dependency upgrade workflow documented | Two-file pinning strategy researched; pip freeze produces 115 transitive deps; DEVELOPER_GUIDE.md update pattern documented |
| BLDG-02 | Single GitHub Actions workflow runs pytest, ruff, and check_docs.py on push and PR, including at least one Windows runner | Workflow structure researched; existing docs-check.yml provides template; all deps have wheels for ubuntu-24.04 and windows-latest |
| BLDG-04 | Ruff runs in CI with initial scoped ruleset (syntax errors, import hygiene only), expandable over time | Violation audit completed; 286 import-hygiene violations found, 272 auto-fixable; exact rule codes and counts documented |
</phase_requirements>

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| ruff | 0.15.10 | Python linter | Fast, Rust-based, replaces flake8/isort/pyflakes; industry standard for new CI setups [VERIFIED: pip index] |
| pytest | 9.0.2 | Test runner | Already in use; 1,072 tests collected [VERIFIED: local pytest run] |
| GitHub Actions | N/A | CI platform | Project already uses it (docs-check.yml) [VERIFIED: .github/workflows/docs-check.yml] |

### CI Actions
| Action | Version | Purpose | Why This Version |
|--------|---------|---------|------------------|
| actions/checkout | v4 | Clone repo | Already used in existing workflow [VERIFIED: docs-check.yml] |
| actions/setup-python | v5 | Install Python | Already used in existing workflow [VERIFIED: docs-check.yml] |
| actions/github-script | v7 | PR comments (optional) | Already used in existing workflow [VERIFIED: docs-check.yml] |

### Supporting
| Library | Version | Purpose | When to Use |
|---------|---------|---------|-------------|
| pip (built-in) | N/A | Package install + freeze | Generating requirements-lock.txt |

**Installation (dev tools to add to requirements.txt):**
```bash
pip install ruff pytest
```

**Version verification:**
- ruff: 0.15.10 (latest as of 2026-04-14) [VERIFIED: pip index versions ruff]
- pytest: 9.0.2 installed, 9.0.3 available [VERIFIED: pip index versions pytest]

## Architecture Patterns

### Recommended CI Structure
```
.github/
  workflows/
    ci.yml              # Single CI workflow (replaces docs-check.yml)
ruff.toml               # Ruff configuration (standalone, not pyproject.toml)
requirements.txt        # Direct dependencies, exact pins (==)
requirements-lock.txt   # Full pip freeze output, generated
```

### Pattern 1: Two-Job CI Workflow
**What:** Single workflow file with `lint-and-docs` (fast, Ubuntu) and `tests` (matrix, Ubuntu+Windows) jobs. Tests depend on lint passing first.
**When to use:** Always -- this is the locked decision.
**Example:**
```yaml
# Source: GitHub Actions docs + existing docs-check.yml pattern
name: CI

on:
  push:
    branches: [master-main]
  pull_request:

jobs:
  lint-and-docs:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.10'
      - run: pip install ruff
      - run: ruff check .
      - run: python scripts/check_docs.py

  tests:
    needs: lint-and-docs
    strategy:
      matrix:
        include:
          - os: ubuntu-latest
            python-version: '3.10'
          - os: windows-latest
            python-version: '3.11'
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      - run: pip install -r requirements-lock.txt
      - run: pip install pytest
      - run: pytest tests/
```
[ASSUMED: exact workflow syntax -- standard GitHub Actions patterns, but should be validated against current docs]

### Pattern 2: ruff.toml Configuration (Recommended over pyproject.toml)
**What:** Standalone ruff configuration file.
**Why ruff.toml over pyproject.toml:** The project has no pyproject.toml currently. Creating one just for ruff config is misleading (implies a modern packaging setup). ruff.toml is purpose-specific and can be expanded later. [ASSUMED: preference -- both work identically]
**Example:**
```toml
# Source: ruff documentation
line-length = 120

[lint]
select = ["E", "F"]
ignore = [
    "E501",   # line-too-long (9,183 violations -- defer to future milestone)
    "E701",   # multiple-statements-on-one-line-colon (312 violations)
    "E702",   # multiple-statements-on-one-line-semicolon (182 violations)
    "E741",   # ambiguous-variable-name (73 -- Hebrew var names)
    "E731",   # lambda-assignment (3)
    "E712",   # true-false-comparison (16)
    "E402",   # module-import-not-at-top (91 -- conditional imports)
    "E722",   # bare-except (5)
    "E401",   # multiple-imports-on-one-line (3)
    "E703",   # useless-semicolon (2)
    "F601",   # multi-value-repeated-key-literal (164)
    "F541",   # f-string-missing-placeholders (156)
    "F841",   # unused-variable (125)
]

exclude = [
    ".claude",
    ".git",
    "venv",
    "__pycache__",
    "extension",
    "dist",
    "build",
]
```

### Pattern 3: Two-File Dependency Pinning
**What:** `requirements.txt` has human-curated direct deps with `==` pins. `requirements-lock.txt` is machine-generated `pip freeze` output.
**When to use:** Always -- locked decision.
**Upgrade workflow:**
1. Edit version in `requirements.txt`
2. `pip install -r requirements.txt` (install updated dep)
3. `pip freeze > requirements-lock.txt` (regenerate lock)
4. Run tests locally
5. Commit both files, push, verify CI green

### Anti-Patterns to Avoid
- **Using `pip freeze` as `requirements.txt`:** Mixes direct and transitive deps, making upgrades impossible to reason about. Use two-file strategy instead.
- **Installing from `requirements.txt` in CI:** Transitive dep drift can still bite you. CI must install from lock file (D-11).
- **Running ruff on `.claude/worktrees/`:** These contain full source copies. Ruff will report 10x more violations than the actual codebase. Must be excluded. [VERIFIED: 107,407 total vs 10,601 with exclusions]
- **Enabling all E+F rules from day one:** The codebase has 10,601 E+F violations (with proper exclusions). D-09 requires zero violations, so most E-series rules must be ignored initially.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Linting | Custom flake8 config | ruff with ruff.toml | 10-100x faster, single tool, industry standard |
| Dep locking | Manual version tracking | `pip freeze > requirements-lock.txt` | Captures all transitive deps automatically |
| CI workflow | Shell scripts | GitHub Actions YAML | Already in use, native integration |

## Common Pitfalls

### Pitfall 1: Ruff Scanning .claude/worktrees
**What goes wrong:** Ruff finds 107,407 violations instead of ~10,601 because it scans agent worktree copies.
**Why it happens:** `.claude/` is not in `.gitignore` (only `.claude/settings.local.json` is). Ruff's default exclude includes `.git` but not `.claude`.
**How to avoid:** Explicitly exclude `.claude` in ruff.toml `exclude` list.
**Warning signs:** Duplicate violations reported from `.claude/worktrees/agent-*/` paths.
[VERIFIED: ruff check without exclude reports 107K violations; with exclude reports 10.6K]

### Pitfall 2: PyQt6 manylinux_2_34 Requirement
**What goes wrong:** `pip install PyQt6` fails on Ubuntu 22.04 runners because PyQt6 6.10.2 requires manylinux_2_34 (glibc 2.34+).
**Why it happens:** Ubuntu 22.04 has glibc 2.35 so this is actually fine, but older runners would fail.
**How to avoid:** Use `ubuntu-latest` (maps to Ubuntu 24.04, glibc 2.39) or explicitly `ubuntu-24.04`.
**Warning signs:** `ERROR: No matching distribution found for PyQt6` on Linux CI.
[VERIFIED: PyQt6 6.10.2 requires manylinux_2_34; ubuntu-latest = Ubuntu 24.04 with glibc 2.39]

### Pitfall 3: E+F Ruleset Too Broad
**What goes wrong:** D-06 says `select = ["E", "F"]` but that produces 10,601 violations (mostly E501 line-too-long at 9,183). Fixing all of these is out of scope.
**Why it happens:** "E" includes ALL pycodestyle error rules, not just syntax errors. "Syntax errors" is really E9xx.
**How to avoid:** Select E+F but ignore rules that have hundreds of violations and aren't "syntax errors or import hygiene."
**Warning signs:** Ruff check failing with thousands of violations on existing code.
[VERIFIED: exact violation counts from ruff check on the codebase]

### Pitfall 4: check_docs.py Staleness Warnings
**What goes wrong:** `check_docs.py` has a 90-day staleness check. Docs not updated in 90+ days trigger warnings, which cause non-zero exit (CI failure).
**Why it happens:** The script treats staleness warnings as issues and returns exit code 1.
**How to avoid:** Either update stale docs before enabling CI, or accept that some docs may trigger warnings. Check current state: `python scripts/check_docs.py`.
**Warning signs:** CI fails on lint-and-docs job due to stale doc warnings, not actual problems.
[VERIFIED: check_docs.py returns 1 if total_issues > 0, and staleness counts as an issue]

### Pitfall 5: Ruff Scanning .git Directory
**What goes wrong:** Git branches with `.py` in their name create log files under `.git/logs/refs/heads/` that ruff tries to parse as Python, producing syntax errors.
**Why it happens:** The project has branches like `codex/add-image-prefetching-in-browse.py` whose git log files end in `.py`.
**How to avoid:** Ensure `.git` is in the ruff exclude list (it is by default, but explicit is better).
**Warning signs:** `invalid-syntax` errors in `.git/logs/refs/heads/` paths.
[VERIFIED: ruff check without --exclude .git produced syntax errors from git log files]

### Pitfall 6: gotrue Deprecation Warning in Tests
**What goes wrong:** Tests produce a DeprecationWarning about `gotrue` package being deprecated.
**Why it happens:** `supabase_corrections_client.py` imports `from gotrue.errors import AuthApiError`.
**How to avoid:** This is Phase 64's concern. For now, the warning is harmless (tests still pass). Do NOT remove gotrue from requirements.txt -- it would break the import.
**Warning signs:** Pytest warning output mentioning gotrue deprecation.
[VERIFIED: pytest output shows gotrue deprecation warning; 1,067 tests still pass]

## Ruff Violation Audit

Exact violation counts on the codebase (excluding `.claude`, `.git`, `venv`, `__pycache__`, `extension`):

| Rule | Count | Category | Auto-fixable | Recommendation |
|------|-------|----------|-------------|----------------|
| E501 | 9,183 | line-too-long | No | IGNORE -- D-07 says no line-length rules |
| E701 | 312 | multi-statement-colon | No | IGNORE -- style, not syntax |
| F401 | 278 | unused-import | 268 yes | FIX -- import hygiene, mostly auto-fix |
| E702 | 182 | multi-statement-semicolon | No | IGNORE -- style |
| F601 | 164 | multi-value-repeated-key | No | IGNORE -- low priority |
| F541 | 156 | f-string-no-placeholder | 156 yes | IGNORE -- cosmetic |
| F841 | 125 | unused-variable | 125 yes | IGNORE -- risky auto-fix in large codebase |
| E402 | 91 | import-not-at-top | No | IGNORE -- conditional imports intentional |
| E741 | 73 | ambiguous-variable-name | No | IGNORE -- Hebrew variables |
| E712 | 16 | true-false-comparison | No | IGNORE -- style |
| F811 | 7 | redefined-while-unused | 7 yes | FIX -- import hygiene |
| E722 | 5 | bare-except | No | IGNORE -- Phase 65 audit scope |
| E731 | 3 | lambda-assignment | No | IGNORE -- style |
| E401 | 3 | multiple-imports | 3 yes | IGNORE -- style |
| E703 | 2 | useless-semicolon | 2 yes | IGNORE -- trivial |
| F821 | 1 | undefined-name | No | FIX -- real bug |

**Summary:** To achieve zero violations (D-09), the config must:
1. Select E+F
2. Ignore all rules except F401, F811, F821 (and E9xx syntax errors -- but there are zero E9xx violations)
3. Fix the 286 remaining violations (278 unused imports + 7 redefined + 1 undefined name)

**Alternative approach (cleaner):** Select ONLY `["F401", "F811", "F821", "E9"]` -- this is exactly "syntax errors + import hygiene" without needing a long ignore list. Can expand later.

[VERIFIED: all counts from `ruff check --select E,F --exclude .claude,.git,venv,__pycache__,extension --statistics`]

## Code Examples

### ruff.toml Configuration (Recommended)
```toml
# GenizahSearch ruff configuration
# Phase 63: syntax errors + import hygiene only
# Expand ruleset in future milestones

line-length = 120  # Future-friendly default (not enforced yet)

[lint]
select = [
    "E9",    # Syntax errors (runtime-breaking)
    "F401",  # Unused imports
    "F811",  # Redefined while unused
    "F821",  # Undefined name
]

exclude = [
    ".claude",
    ".git",
    "venv",
    "__pycache__",
    "extension",
    "dist",
    "build",
]
```
[ASSUMED: exact config syntax -- based on ruff documentation patterns]

### Auto-fixing Unused Imports
```bash
# Fix 268 of 278 F401 violations automatically
ruff check --select F401 --fix --exclude ".claude,.git,venv,__pycache__,extension" .

# Then manually fix remaining 10 + 7 F811 + 1 F821
```
[VERIFIED: ruff reports 272 of 286 fixable with --fix]

### Generating requirements-lock.txt
```bash
# After pip install -r requirements.txt succeeds:
pip freeze > requirements-lock.txt
```
Current `pip freeze` produces 115 packages (14 direct + ~101 transitive). [VERIFIED: pip freeze | wc -l = 115]

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| flake8 + isort + pyflakes | ruff (single tool) | 2023+ | 10-100x faster, single config |
| requirements.txt unpinned | requirements.txt pinned + lock file | Industry standard | Reproducible builds |
| ubuntu-22.04 | ubuntu-latest (24.04) | Oct 2024 | Better glibc support, newer tools |

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | ruff.toml is preferable to pyproject.toml for this project | Architecture Patterns | LOW -- both work identically; pyproject.toml just needs a `[tool.ruff]` prefix |
| A2 | Exact CI workflow YAML syntax | Architecture Patterns | LOW -- standard GHA patterns, validated by CI run itself |
| A3 | ruff config syntax for select/ignore | Code Examples | LOW -- well-documented, errors caught immediately on `ruff check` |

## Open Questions (RESOLVED)

1. **check_docs.py staleness warnings**
   - What we know: Script returns exit 1 if any stale docs found (>90 days old)
   - What's unclear: Whether current docs will trigger staleness warnings in CI
   - Recommendation: Run `python scripts/check_docs.py` as part of the first CI test; if stale docs cause failures, fix them or adjust threshold before merging

2. **F401 auto-fix safety**
   - What we know: 268 of 278 unused imports are auto-fixable; 10 need manual review
   - What's unclear: Whether any "unused" imports are actually used via side effects or re-exports
   - Recommendation: Run `ruff check --select F401 --fix`, then run full test suite. If tests break, revert specific fixes.

3. **ruff dev dependency placement**
   - What we know: ruff is a dev-only tool, not needed at runtime
   - What's unclear: Whether to add it to requirements.txt or install separately in CI
   - Recommendation: Install ruff separately in CI (`pip install ruff`) since it's a dev tool. Do NOT add to requirements.txt (which is for runtime deps).

## Validation Architecture

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest 9.0.2 |
| Config file | None (default pytest discovery) |
| Quick run command | `pytest tests/ -x` |
| Full suite command | `pytest tests/` |

### Phase Requirements to Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| BLDG-01 | requirements.txt has pinned versions | smoke | `grep -c "==" requirements.txt` (all lines have ==) | N/A -- manual check |
| BLDG-01 | requirements-lock.txt is valid | smoke | `pip install -r requirements-lock.txt --dry-run` | N/A -- CI validates |
| BLDG-02 | CI workflow runs on push/PR | integration | Push to branch, verify GitHub Actions runs | N/A -- manual |
| BLDG-02 | CI includes Windows runner | integration | Check workflow matrix in ci.yml | N/A -- manual |
| BLDG-04 | Ruff passes with zero violations | smoke | `ruff check .` | N/A -- CI validates |

### Sampling Rate
- **Per task commit:** `ruff check . && pytest tests/ -x`
- **Per wave merge:** `ruff check . && pytest tests/ && python scripts/check_docs.py`
- **Phase gate:** Full suite green locally + CI green on GitHub

### Wave 0 Gaps
- None -- existing test infrastructure (pytest, 1,072 tests) covers all phase requirements. No new test files needed for this phase.

## Security Domain

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | N/A -- no auth changes |
| V3 Session Management | No | N/A |
| V4 Access Control | No | N/A |
| V5 Input Validation | No | N/A -- CI config only |
| V6 Cryptography | No | N/A |

This phase is infrastructure-only (CI configuration, linting, dependency pinning). No security-sensitive changes.

## Sources

### Primary (HIGH confidence)
- Local codebase inspection: `.github/workflows/docs-check.yml`, `requirements.txt`, `scripts/check_docs.py`, `tests/conftest.py`
- ruff check output: exact violation counts from running ruff 0.15.10 locally
- pip index: verified latest versions of ruff (0.15.10) and pytest (9.0.3)
- pip freeze: 115 installed packages enumerated
- PyPI simple index: tantivy 0.25.1 and PyQt6 6.10.2 wheel availability for Linux/Windows/macOS

### Secondary (MEDIUM confidence)
- [GitHub Actions runner-images](https://github.com/actions/runner-images): ubuntu-latest = Ubuntu 24.04
- [PyPI tantivy wheels](https://pypi.org/simple/tantivy/): pre-built wheels for cp310-cp314 on all platforms
- [PyPI PyQt6 wheels](https://pypi.org/simple/pyqt6/): manylinux_2_34 requirement for Linux

### Tertiary (LOW confidence)
- None

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH -- ruff and pytest versions verified against pip index; GHA action versions verified from existing workflow
- Architecture: HIGH -- CI workflow pattern is straightforward GHA; ruff config validated by running ruff locally
- Pitfalls: HIGH -- all pitfalls discovered by actually running ruff on the codebase and observing the violations

**Research date:** 2026-04-14
**Valid until:** 2026-05-14 (stable tools, 30-day window)
