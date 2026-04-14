# Phase 63: CI & Dependency Pinning - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md -- this log preserves the alternatives considered.

**Date:** 2026-04-14
**Phase:** 63-ci-dependency-pinning
**Areas discussed:** CI workflow design, Ruff ruleset scope, Dependency pinning strategy
**Review method:** User forwarded options to Codex (OpenAI) for external review

---

## CI Workflow Design

| Option | Description | Selected |
|--------|-------------|----------|
| A: Single unified workflow | One .yml, matrix ubuntu+windows, pytest+ruff+check_docs | Modified |
| B: Merge into existing docs-check.yml | Extend current workflow | |
| C: Two workflows | Keep docs-check.yml, add new ci.yml | |

**User's choice:** Option A, refined by Codex: single `ci.yml` with **separate jobs** (lint-and-docs on Ubuntu, tests as matrix Ubuntu+Windows). Tests depend on lint-and-docs for fast-fail.

**Codex notes:** Trigger on PRs + push to master-main only. Python 3.10 on Ubuntu, 3.11 on Windows.

---

## Ruff Ruleset Scope

| Option | Description | Selected |
|--------|-------------|----------|
| A: E + F only | Syntax errors + Pyflakes | Yes |
| B: E + F + I | Above + isort | |
| C: E + F + I + W | Above + warnings | |

**User's choice:** A (E + F only), per Codex recommendation.

**Codex notes:** "Avoids turning the milestone into formatting theater." Line length: set 120 as config value, don't enforce yet.

---

## Dependency Pinning Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| A: pip freeze full output | Full freeze as requirements.txt | |
| B: Curated direct pins | Pin 14 direct deps only | |
| C: Direct pins + lock file | requirements.txt (direct) + requirements-lock.txt (full freeze) | Yes |

**User's choice:** C, per Codex recommendation.

**Codex notes:** CI installs from requirements-lock.txt for full reproducibility. requirements.txt remains human-readable direct deps.

---

## Claude's Discretion

- Ruff config file location
- GitHub Actions action versions
- Pip caching in CI
- gotrue handling (Phase 64 concern)

## Deferred Ideas

- Expanding ruff ruleset (isort, warnings) -- future milestone
- Type checking -- out of scope v7.8
