---
phase: 83
slug: public-release
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-05
---

# Phase 83 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (existing project standard, ~1400 tests at HEAD) |
| **Config file** | `pyproject.toml` (markers `slow` and `e2e` registered per Phase 78) |
| **Quick run command** | `pytest tests/test_search_api_docs.py tests/test_openapi_scope.py tests/test_release_artifacts.py -x` |
| **Full suite command** | `pytest tests/ -q` |
| **Estimated runtime** | ~90 seconds (full suite); <5 seconds (quick) |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_search_api_docs.py tests/test_openapi_scope.py tests/test_release_artifacts.py -x` (Phase 83 Wave 0 files only — fast)
- **After every plan wave:** Run `pytest tests/ -q` (full suite)
- **Before `/gsd-verify-work`:** Full suite green + `python scripts/check_docs.py` green + skill smoke green
- **Max feedback latency:** ~5 seconds (quick) / ~90 seconds (full)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 83-01-01 | 01 | 1 | PUBLIC-02 | T-78-#1,#3,#4,#5,#9 + T-83-OAS-LEAK | Audit verifies XFF parse, error-envelope sanitization, salt persistence, mode-gate, filter fail-closed, Responsa cap remain load-bearing | manual (cold-reader review) | (manual review of `83-SECURITY.md`) | N/A | ⬜ pending |
| 83-02-01 | 02 | 1 | PUBLIC-01, PUBLIC-03 | — | Stability statement + Quick Start + Attribution + Changelog sections present; "internal helper" banner absent; existing 663 lines preserved | unit (text grep) | `pytest tests/test_search_api_docs.py -x` | ❌ W0 | ⬜ pending |
| 83-03-01 | 03 | 2 | PUBLIC-04 | T-83-OAS-LEAK, T-83-OAS-DOS | OpenAPI spec scoped to /api/search, /api/browse, /api/parallels (legacy /api/* excluded); /api/docs + /api/openapi.json EXCLUDED from rate limiter | unit + integration | `pytest tests/test_openapi_scope.py -x` | ❌ W0 | ⬜ pending |
| 83-04-01 | 04 | 2 | PUBLIC-05, PUBLIC-06 | — | README.md "API" section links to docs/SEARCH_API.md (English only); SKILL.md references public docs path | unit (text grep) | `pytest tests/test_search_api_docs.py::test_readme_has_api_section -x` and `::test_skill_md_references_public_docs` | ❌ W0 (folded into test_search_api_docs.py) | ⬜ pending |
| 83-05-01 | 05 | 3 | PUBLIC-07, PUBLIC-08 | — | version.py = "7.10.0", CHANGELOG.md has [7.10.0] section, CLAUDE.md "Recently Changed" entry, README.md "What's New" updated; pre-deploy gate documented (pytest + check_docs + skill smoke); deploy executed; NO GitHub release created | unit (text grep) + manual (deploy gate) | `pytest tests/test_release_artifacts.py -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_search_api_docs.py` — covers PUBLIC-01, PUBLIC-03, PUBLIC-05, PUBLIC-06. One test per assertion: stability statement present, "internal helper" banner absent, Quick Start section present, Attribution section present, Changelog section present, README.md `## API` section present and linking to `docs/SEARCH_API.md`, SKILL.md links to public docs path.
- [ ] `tests/test_openapi_scope.py` — covers PUBLIC-04 (D-08). Two tests: (1) `GET /api/openapi.json` returns spec listing exactly `/api/search`, `/api/browse`, `/api/parallels` paths and NO legacy paths (e.g., `/api/cambridge_image`, `/api/nli_image_by_sysid`, `/api/puzzle_upload`); (2) `GET /api/docs` returns 200 with Swagger UI HTML; (3) `/api/openapi.json` and `/api/docs` are NOT subject to the rate limiter (10 quick requests don't 429).
- [ ] `tests/test_release_artifacts.py` — covers PUBLIC-08. Three small tests: (1) `version.py` `APP_VERSION` equals `"7.10.0"`, (2) `CHANGELOG.md` contains a `## [7.10.0]` heading, (3) `CLAUDE.md` "Recently Changed" section first bullet starts with `May 2026` or matches v7.10.
- [ ] No new fixtures needed — existing TestClient pattern from `tests/test_search_api.py` (Phase 78) and `tests/test_api_browse.py` (Phase 79) applies.
- [ ] No framework install needed.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| 83-SECURITY.md cold-reader walkthrough | PUBLIC-02 | Threat-model audit is prose; verifying coverage of D-05(a)–(f) requires reading | Reviewer reads `83-SECURITY.md`. Confirms each of D-05(a)–(f) is present with file:line citations. Confirms each Phase 78 Concern #1, #3, #4, #5, #9 is mapped to current code locations and verified still load-bearing. Returns APPROVED or REQUEST CHANGES. |
| Swagger UI visual render at /api/docs | PUBLIC-04 | "Renders cleanly" is subjective | Start `python -m web.main` locally. Open `http://localhost:8081/api/docs`. Confirm: 3 endpoints listed (search/browse/parallels), no legacy routes visible, "Try it out" button works for /api/search, error envelope shape rendered in response examples. |
| Skill end-to-end smoke against localhost | PUBLIC-07 | Memory rule `[Never launch web server from Bash]` blocks automation; user must start server | Pre-deploy runbook: `unset GENIZAH_API_BASE` (avoid hitting production); start `python -m web.main`; in second shell run `cd skills/cairo-genizah-research && python scripts/run_smoke.py --base-url http://localhost:8081` (or equivalent skill harness); verify exit 0 and ranked output for the canonical fixture query. |
| Post-deploy production curl spot-check | PUBLIC-07 | Live production verification | After `bash deploy.sh master-main`, curl all 3 endpoints against `https://genizahsearch.com` with sample payloads from `docs/SEARCH_API.md` Quick Start. Verify response envelope shape and 200 status. |
| Production /api/docs visible | PUBLIC-04 | Live production verification | Browser open `https://genizahsearch.com/api/docs`. Confirm Swagger UI loads with 3 endpoints. |
| README.md "API" section flows naturally | PUBLIC-05 | Editorial judgment | Reviewer reads README.md top-to-bottom. Confirms the new "API" section reads as part of the document, not as a bolted-on insert. |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify (manual-only tasks 83-01 + post-deploy curl are bracketed by automated tasks 83-02..83-05)
- [ ] Wave 0 covers all MISSING references (3 new test files: test_search_api_docs.py, test_openapi_scope.py, test_release_artifacts.py)
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s (quick) / 90s (full)
- [ ] `nyquist_compliant: true` set in frontmatter (after planner spawns Wave 0 tests in plan files)

**Approval:** pending
