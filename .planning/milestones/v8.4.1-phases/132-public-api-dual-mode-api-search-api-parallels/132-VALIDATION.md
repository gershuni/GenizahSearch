---
phase: 132
slug: public-api-dual-mode-api-search-api-parallels
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-07-01
---

# Phase 132 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Source: 132-RESEARCH.md § Validation Architecture (HIGH confidence).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (already installed) |
| **Config file** | `pytest.ini` / `conftest.py` (existing) |
| **Quick run command** | `pytest tests/test_search_api_library_mode.py -x` |
| **Full suite command** | `pytest tests/ -k "not gui" -x` |
| **Estimated runtime** | ~15–30 s (quick), full suite gated on CI |

---

## Sampling Rate

- **After every task commit:** `pytest tests/test_search_api_library_mode.py -x`
- **After every plan wave:** `pytest tests/ -k "not gui" -x`
- **Before `/gsd:verify-work`:** Full suite green (GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen for GUI-adjacent collection)
- **Max feedback latency:** ~30 s

---

## Per-Task Verification Map

Task IDs assigned by the planner; every requirement below maps to an automated pytest in the new
`tests/test_search_api_library_mode.py` (Wave 0). Both endpoints share `FiltersModel`, so most tests
parametrize `/api/search` and `/api/parallels`.

| Requirement | Behavior | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|-------------|----------|------------|-----------------|-----------|-------------------|-------------|--------|
| DMF-11-1 | `include` (default) = today's behavior unchanged | — | N/A | unit | `pytest tests/test_search_api_library_mode.py::test_include_mode_is_default_same_as_omitted -x` | ❌ W0 | ⬜ pending |
| DMF-11-1 | Omitting `library_filter_mode` == `mode='include'` | — | Backward-compatible default | unit | `pytest tests/test_search_api_library_mode.py::test_omit_mode_equals_include -x` | ❌ W0 | ⬜ pending |
| DMF-11-2 | `exclude` + codes → only OTHER libraries (complement) | — | N/A | unit | `pytest tests/test_search_api_library_mode.py::test_exclude_restricts_to_complement -x` | ❌ W0 | ⬜ pending |
| DMF-11-2 | include vs exclude on same set → disjoint result libraries | — | N/A | unit | `pytest tests/test_search_api_library_mode.py::test_include_vs_exclude_disjoint -x` | ❌ W0 | ⬜ pending |
| DMF-11-2 | `/api/parallels` honors the mode (parity with `/api/search`) | — | N/A | unit | `pytest tests/test_search_api_library_mode.py::test_parallels_exclude_mode -x` | ❌ W0 | ⬜ pending |
| DMF-11-3 | Invalid mode value → 400 `invalid_request` | T-132 Tampering | `Literal['include','exclude']` + `extra='forbid'` → 400 fail-closed | unit | `pytest tests/test_search_api_library_mode.py::test_invalid_mode_returns_400 -x` | ❌ W0 | ⬜ pending |
| DMF-11-3 | Mode with no library list → no filter applied (no-op) | — | N/A | unit | `pytest tests/test_search_api_library_mode.py::test_mode_without_library_is_noop -x` | ❌ W0 | ⬜ pending |
| DMF-11 | `resolve_library_complement_sys_ids` helper correctness | — | N/A | unit | `pytest tests/test_search_api_library_mode.py::test_resolve_library_complement_sys_ids -x` | ❌ W0 | ⬜ pending |

**Out of scope (per user, 2026-07-01):** no LOCAL-specific handling on the API. There are no LOCAL
rows in the server corpus, so `library=['LOCAL']` self-resolves to an empty set (`include` → no
matches; `exclude` → full-corpus complement = no-op) — harmless, and DMF-10 (a UI-option invariant)
is not in this phase's scope (phase reqs = DMF-11 only). No 400-reject / strip task.

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_search_api_library_mode.py` — new file covering all rows above (~8 tests)
- [ ] `resolve_library_complement_sys_ids` in `shared/fjms_service.py` does not yet exist (asserted-against in Wave 0, implemented in a later wave)

*No framework install needed — pytest already present.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Live prod smoke of `mode=exclude` against genizahsearch.com | DMF-11-2 | Requires deployed web + network | After v8.4.1 deploy: `POST /api/search` with `{"filters":{"library":["CUL"],"library_filter_mode":"exclude"}}` → results contain no CUL rows |

*All other phase behaviors have automated verification.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
