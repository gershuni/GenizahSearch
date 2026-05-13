---
phase: 87
slug: foundations-session-uuid-and-safe-storage-chokepoint
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-13
---

# Phase 87 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: `87-RESEARCH.md` section "Validation Architecture".

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 9.0.2 (pinned in `requirements-lock.txt`) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` — registers `slow` and `e2e` markers; restricts collection to `test_*.py` |
| **Quick run command** | `pytest tests/test_safe_storage.py tests/test_session_uuid.py tests/test_no_raw_storage_access.py -x` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | Quick: ~3 seconds. Full: ~3 minutes (1862 tests at v7.11.1 baseline). |
| **Phase gate** | Full suite green + `ruff check .` green + `python scripts/check_docs.py` green |

---

## Sampling Rate

- **After every task commit:** Run quick command (~3 seconds)
- **After every plan wave:** Run full suite command (~3 min)
- **Before `/gsd-verify-work`:** Full suite + ruff + check_docs must all be green
- **Max feedback latency:** 3 seconds per task

---

## Per-Task Verification Map

> Task IDs are placeholders; actual IDs populated by gsd-planner. The mapping by requirement is locked.

| Req ID | Behavior | Test Type | Automated Command | File Exists | Status |
|--------|----------|-----------|-------------------|-------------|--------|
| FOUND-01 | `_session_uuid` minted on first request, stable across token refresh | unit | `pytest tests/test_session_uuid.py -x` | ❌ Wave 0 | ⬜ pending |
| FOUND-01 SC1 | 100 concurrent sessions never share UUID | unit (mock-based per A3) | `pytest tests/test_session_uuid.py::test_session_uuid_unique_across_100_sessions -x` | ❌ Wave 0 | ⬜ pending |
| FOUND-02 | `safe_storage.py` is the chokepoint adapter (audit completeness) | integration (via FOUND-04 lint scan) | `pytest tests/test_no_raw_storage_access.py -x` | ❌ Wave 0 | ⬜ pending |
| FOUND-03 | Allowlist file exists with per-entry justification | unit (schema check) | `pytest tests/test_no_raw_storage_access.py::test_allowlist_well_formed -x` | ❌ Wave 0 | ⬜ pending |
| FOUND-04 | Lint rejects raw access; accepts allowlisted | unit | `pytest tests/test_no_raw_storage_access.py::test_lint_rejects_synthetic_violation -x` | ❌ Wave 0 | ⬜ pending |
| FOUND-04 SC4 | Lint passes on production code post-migration | regression | `pytest tests/test_no_raw_storage_access.py::test_no_raw_storage_access_outside_allowlist -x` | ❌ Wave 0 | ⬜ pending |
| FOUND-05 | All 6 existing safe_storage tests pass UNCHANGED | regression | `pytest tests/test_safe_storage.py -x` | ✅ exists; **MUST NOT be edited** | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_session_uuid.py` — covers FOUND-01 (minimum 5 tests: uniqueness across 100 sims, stability across token refresh, prune-race fallback returns ephemeral UUID without caching, idempotent ensure, pattern-validation on read)
- [ ] `tests/test_no_raw_storage_access.py` — covers FOUND-02 + FOUND-03 + FOUND-04 (minimum 3 tests: AST scan of `web/` excluding allowlist, synthetic-violation rejection, allowlist schema well-formed)
- [ ] `.planning/phase87_storage_allowlist.yaml` — initial allowlist seeded with `web/auth_state.py` bootstrap reads (Phase 91 will migrate) + any other genuinely-pre-session sites identified during R-02 audit
- [ ] **Verify PyYAML availability** in Wave 0: `python -c "import yaml; print(yaml.__version__)"`. If missing, add `pyyaml` to `requirements.txt` AND `requirements-lock.txt` (assumption A2 in research)
- [ ] **Verify ruff plugin API status** in Wave 0 (assumption A1): if ruff 0.15.10 still has no stable plugin API, default to pytest AST scan. No action needed if pytest path is chosen.
- [ ] Framework install: NONE — pytest, ruff, ast (stdlib), uuid (stdlib) all already available

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Visual smoke check of UUID stability across browser-tab refresh | FOUND-01 | Browser cookie/storage interaction is hard to simulate faithfully | Open `genizahsearch.com` (or local `python -m web.main`), open DevTools → Storage, capture `_session_uuid` from `app.storage.user`. Refresh page; verify same UUID. Open private/incognito window; verify different UUID. |
| Lint integrates into CI on Ubuntu + Windows matrix | FOUND-04 | CI behavior must be observed in actual GitHub Actions run | Push branch to GitHub; verify `tests` job passes on both `ubuntu-latest` and `windows-latest` runners (per v7.8 CI matrix). |

---

## Validation Sign-Off

- [ ] All tasks have automated verify command OR explicit Wave 0 dependency
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING test file references
- [ ] No watch-mode flags used (CI must run to completion)
- [ ] Feedback latency < 3 seconds for quick command
- [ ] `nyquist_compliant: true` set in frontmatter after Wave 0 lands

**Approval:** pending
