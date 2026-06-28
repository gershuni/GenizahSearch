---
phase: 129
slug: library-filter-search-browse-by-identification-seed-026
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-28
---

# Phase 129 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `129-RESEARCH.md` § Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | none — run directly (project convention) |
| **Quick run command** | `pytest tests/test_libfilter_catalog.py tests/test_libfilter_web_search.py -x` |
| **Full suite command** | `pytest tests/test_seed023_catalog_filters.py tests/test_catalog_availability_filter.py tests/test_fjms_service.py tests/test_pgp_filter_cascade.py tests/test_no_raw_storage_access.py -x` (GUARD-02 regression) + bulk suite |
| **Estimated runtime** | ~30–60 seconds (targeted); GUI tests run separately (offscreen) |

> **Windows note:** full `pytest tests/` aborts on a PyQt6 headless segfault; desktop (`test_libfilter_desktop.py`) is gui-marked and runs via the `gui-tests` split with `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` (see `feedback_full_suite_testing_windows`). Add new GUI test files to `_GUI_TEST_FILES` in `conftest`.

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_seed023_catalog_filters.py tests/test_fjms_service.py -x` (GUARD-02 regression + shared service)
- **After every plan wave:** Run the GUARD-02 regression set + the new `test_libfilter_*.py` files
- **Before `/gsd:verify-work`:** Full bulk suite green; GUI split green
- **Max feedback latency:** ~60 seconds (targeted), bulk on wave merge

---

## Per-Task Verification Map

> Populated at plan time from PLAN.md tasks. Initial requirement→behavior map below (from RESEARCH.md):

| Req | Behavior | Test Type | Automated Command | File Exists |
|-----|----------|-----------|-------------------|-------------|
| LIBFILTER-01 | Web search library filter narrows the FULL result set (not just the visible 200) | unit | `pytest tests/test_libfilter_web_search.py -x` | ❌ W0 |
| LIBFILTER-01 | Empty `library_filter` = no-op (all results pass) | unit | same | ❌ W0 |
| LIBFILTER-01 | Facet counts computed from the pre-filter full set; 0-count libraries hidden | unit | same | ❌ W0 |
| LIBFILTER-01 | Filter state persists via `safe_storage` (AST / chokepoint check) | unit | `pytest tests/test_libfilter_web_search.py -x` | ❌ W0 |
| LIBFILTER-02 | `library_codes` arg changes `total` correctly (full set, not page subset) | unit | `pytest tests/test_libfilter_catalog.py -x` | ❌ W0 |
| LIBFILTER-02 | None/empty `library_codes` = no-op (mirror `test_filter_skipped_when_set_missing`) | unit | same | ❌ W0 |
| LIBFILTER-02 | Composes with PGP + Editions filters (3-way AND) | unit | same | ❌ W0 |
| LIBFILTER-02 | `_FILTER_TEMP_TABLES` contains `"_browse_filter_library"` | unit | same | ❌ W0 |
| LIBFILTER-03 | Desktop `_CatalogRefreshWorker` threads `library_filter` into `get_browse_results` | unit (gui) | `pytest tests/test_libfilter_desktop.py -x` | ❌ W0 |
| GUARD-02 | Existing SEED-023 + browse + PGP-cascade + safe_storage tests all pass | regression | GUARD-02 regression set above | ✅ |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_libfilter_catalog.py` — covers LIBFILTER-02 (`fjms_service.get_browse_results` push-down: total/pagination, additive no-op, composition, `_FILTER_TEMP_TABLES` extension)
- [ ] `tests/test_libfilter_web_search.py` — covers LIBFILTER-01 (web search full-set filter, facet counts/hide-empty, `safe_storage` persistence)
- [ ] `tests/test_libfilter_desktop.py` — covers LIBFILTER-03 (desktop `_CatalogRefreshWorker` wiring); **gui-marked**, add to `_GUI_TEST_FILES`

*Existing infrastructure (pytest + the SEED-023 test shapes) covers the regression side; the three files above are the new coverage.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| RTL rendering of the library dropdown + chips under Hebrew UI | LIBFILTER-01/02/03, D-01 | Visual; headless pytest can't assert RTL layout | Switch UI to Hebrew, open the library filter on `/search` and Browse-by-Identification (web + desktop), confirm Hebrew labels (no English leak) and correct RTL chip placement |
| NiceGUI multi-select dropdown renders as a compact menu (D-03) | LIBFILTER-01/02 | Visual layout; render-smoke needs a live client | Live-client render-smoke of the filter row; confirm dropdown-with-checklist, not an always-visible inline list (see `feedback_nicegui_render_smoke_gap`) |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (3 new test files)
- [ ] No watch-mode flags
- [ ] Feedback latency < 60s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
