---
phase: 74
slug: page-scoped-state-refactor
status: draft
nyquist_compliant: true
wave_0_complete: true
created: 2026-04-17
---

# Phase 74 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Sourced from `74-RESEARCH.md` §12 "Validation Architecture" and CONTEXT.md D-18/D-19/D-20/D-21/D-22/D-23/D-24.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (stdlib unit tests + selenium E2E) |
| **Config file** | None (no `pytest.ini`; `conftest.py` at repo root and `tests/e2e/`) |
| **Quick run command** | `pytest tests/test_search_state.py tests/test_browse_bootstrap.py tests/test_search_bootstrap.py -x` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | Quick: ~5s · Full: ~60–90s (baseline 1067 passed / 8 skipped) |

---

## Sampling Rate

- **After every task commit:** Run the quick command above
- **After every plan wave:** Run `pytest tests/` (full suite)
- **Before `/gsd-verify-work`:** Full suite green + web smoke check (D-22) + cross-tab manual test (D-24)
- **Max feedback latency:** ~10 seconds (quick run)

---

## Per-Task Verification Map

Test names below are the EXACT pytest function names defined in 74-01-PLAN and 74-02-PLAN. Keep this table in sync with the plans if test names change during revision.

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 74-XX-01 | persistence | 0 | WEBM-03 | — | N/A (refactor) | unit stubs | `pytest tests/test_search_state.py --collect-only` | ❌ W0 | ⬜ pending |
| 74-XX-02 | persistence | 0 | WEBM-03 | — | N/A | unit stubs | `pytest tests/test_browse_bootstrap.py --collect-only` | ❌ W0 | ⬜ pending |
| 74-XX-03 | persistence | 1 | WEBM-03 (D-06) | — | Snapshot round-trip preserves restorable fields, excludes runtime_only | unit | `pytest tests/test_search_state.py::test_persist_and_restore_round_trip -x` | ❌ W0 | ⬜ pending |
| 74-XX-04 | persistence | 1 | WEBM-03 (D-04) | — | Stale version discards snapshot, no cross-tab stomp | unit | `pytest tests/test_search_state.py::test_stale_version_discards_snapshot -x` | ❌ W0 | ⬜ pending |
| 74-XX-05 | persistence | 1 | WEBM-03 (D-06) | — | `clear_search_snapshot` wipes all keys | unit | `pytest tests/test_search_state.py::test_clear_snapshot_wipes_all_keys -x` | ❌ W0 | ⬜ pending |
| 74-XX-06 | browse-bootstrap | 1 | WEBM-03 (D-19) | — | Explicit `sys_id` URL beats saved `browse_position` | unit | `pytest tests/test_browse_bootstrap.py::test_explicit_sys_id_beats_saved_position -x` | ❌ W0 | ⬜ pending |
| 74-XX-07 | browse-bootstrap | 1 | WEBM-03 (D-19) | — | Blank `/browse` restores saved `browse_position` | unit | `pytest tests/test_browse_bootstrap.py::test_blank_browse_restores_saved_position -x` | ❌ W0 | ⬜ pending |
| 74-XX-08 | browse-bootstrap | 1 | WEBM-03 (D-19) | — | Reading-desk restore wins over position (blank URL) | unit | `pytest tests/test_browse_bootstrap.py::test_reading_desk_restore_wins_over_position -x` | ❌ W0 | ⬜ pending |
| 74-XX-08b | browse-bootstrap | 1 | WEBM-03 (D-19) | — | Explicit sys_id matching desk entry restores desk (language-switch) | unit | `pytest tests/test_browse_bootstrap.py::test_explicit_sys_id_matching_desk_restores_desk -x` | ❌ W0 | ⬜ pending |
| 74-XX-08c | browse-bootstrap | 1 | WEBM-03 (D-19) | — | Blank URL + no saved state -> action='none' | unit | `pytest tests/test_browse_bootstrap.py::test_no_context_no_action -x` | ❌ W0 | ⬜ pending |
| 74-XX-08d | browse-bootstrap | 1 | WEBM-03 (D-19) | — | fl_id URL trumps desk + position | unit | `pytest tests/test_browse_bootstrap.py::test_fl_id_trumps_everything -x` | ❌ W0 | ⬜ pending |
| 74-XX-08e | persistence (74-01 Task 3) | 1 | WEBM-03 (D-03, D-05) | — | Migrated snapshot write sites in search.py no longer touch app.storage.user directly for restorable keys | grep | `python -c "import pathlib, re; src = pathlib.Path('web/pages/search.py').read_text(encoding='utf-8'); write_pat = r\"app\.storage\.user\[['\\\"](?:search_results|domain_exclusions|search_refinement_chain|search_exclusion_sources|search_printed_filter)['\\\"]\]\s*=\"; writes = re.findall(write_pat, src); assert not writes, f'Direct writes remain for migrated keys: {writes}'"` | ✅ | ⬜ pending |
| 74-XX-09 | async | 2 | WEBM-03 (D-10, D-20) | — | Shelfmark navigation updates URL bar (Cat-1 fix proof) | E2E | `pytest tests/e2e/test_browse_flow.py::TestBrowseNavigation::test_shelfmark_navigation_updates_url -x` | ❌ W0 (addition to existing file) | ⬜ pending |
| 74-XX-10 | regression | 2 | WEBM-03 (D-21) | — | Full baseline unchanged | full suite | `pytest tests/` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

*Task IDs will be finalized by the planner. This table defines the verification command for each requirement slice — the planner will attach them to concrete tasks.*

---

## Wave 0 Requirements

- [ ] `tests/test_search_state.py` — snapshot round-trip / clear / version-stamp stubs (covers WEBM-03 persistence boundary D-06, D-18)
- [ ] `tests/test_browse_bootstrap.py` — three precedence case stubs (covers WEBM-03 browse bootstrap extraction D-17, D-19)
- [ ] Addition to `tests/e2e/test_browse_flow.py` — `test_shelfmark_navigation_updates_url` stub (covers WEBM-03 Cat-1 async fix proof D-20)

No new conftest.py or framework installs needed — existing pytest infrastructure covers everything.

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Web smoke check (5 steps) | WEBM-03 / D-22 | NiceGUI full startup + search Tantivy index + IIIF image fetch exceed unit/E2E fixture scope | 1. `python -m web.main`  2. Open `/` — search page loads  3. Run basic text search — returns results  4. Open `/browse?sys_id=<known-id>` — manuscript loads  5. Click shelfmark Prev/Next — navigation between manuscripts works |
| Cross-tab collision test | WEBM-03 / D-04 / D-24 | Requires two real browser tabs; no automated fixture simulates `localStorage`/`app.storage.user` stomping across tab boundaries | 1. Open same `/browse?sys_id=X` URL in two tabs  2. Navigate independently in each tab (different manuscripts)  3. Confirm neither tab's saved state corrupts the other's restore path  4. Reload each tab — each restores to its own last state OR discards stale snapshot cleanly |
| CI matrix (Ubuntu + Windows) | WEBM-03 / D-23 | Runs on push via `.github/workflows/ci.yml`; cannot run locally | Push branch; verify both Ubuntu and Windows jobs green |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (`tests/test_search_state.py`, `tests/test_browse_bootstrap.py`, `tests/e2e/test_browse_flow.py` addition)
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s (quick run)
- [x] Test names in Per-Task Verification Map match the exact pytest functions defined in 74-01-PLAN / 74-02-PLAN
- [x] `nyquist_compliant: true` set in frontmatter
- [x] `wave_0_complete: true` set in frontmatter

**Approval:** pending
