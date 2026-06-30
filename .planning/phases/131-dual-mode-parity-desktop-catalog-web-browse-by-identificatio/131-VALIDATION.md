---
phase: 131
slug: dual-mode-parity-desktop-catalog-web-browse-by-identification-web-parallels
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-30
---

# Phase 131 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `131-RESEARCH.md` § Validation Architecture. Parity phase — mirrors Phase 130's `(mode + set)` model onto desktop catalog, web Browse-by-Identification, and web `/parallels`.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (no GUI for web; `gui-tests` marker / `_GUI_TEST_FILES` for desktop Qt) |
| **Config file** | `tests/conftest.py` (existing) |
| **Quick run command** | `pytest tests/test_web_library_options_no_local.py tests/test_phase_97_invariants.py tests/test_no_raw_storage_access.py tests/test_catalog_dual_mode_library_filter.py tests/test_parallels_library_filter.py -x -q` |
| **Full suite command** | `pytest tests/ -q --ignore=tests/test_libfilter_desktop.py` then (gui split) `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_libfilter_desktop.py tests/test_catalog_availability_filter.py -q` |
| **Estimated runtime** | ~90–150 seconds (web suite) + ~30s (gui split) |

> **GUI caveat (project convention):** `pytest tests/` aborts on a PyQt6 headless segfault and `-n auto` OOMs on Tantivy. Desktop dialog tests must be added to `_GUI_TEST_FILES` in `conftest.py` and run in the marker-gated split with `QT_QPA_PLATFORM=offscreen`.

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_web_library_options_no_local.py tests/test_phase_97_invariants.py tests/test_no_raw_storage_access.py -x -q` (the cross-cutting guards — DMF-10 must never go red mid-phase)
- **After every plan wave:** Run the full **Quick run command** above
- **Before `/gsd:verify-work`:** Full suite must be green (web split + gui split)
- **Max feedback latency:** ~150 seconds

---

## Per-Task Verification Map

> Task IDs are assigned by the planner; rows below are keyed by requirement + surface so the planner can attach the right `<automated>` verify to each task it creates. No 3 consecutive tasks may lack an automated verify.

| Req | Surface | Wave | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|-----|---------|------|------------|-----------------|-----------|-------------------|-------------|--------|
| DMF-07 | Desktop `LibraryFilterDialog` Show-only/Hide toggle; mode+set returned; LOCAL absent both modes; D-04 mode-flip reset | gui-unit | — | inputs pass `sanitize_library_codes()` / `c != 'LOCAL'` | unit (gui-marked) | `… pytest tests/test_libfilter_desktop.py -x -q` | ❌ W0 (extend) | ⬜ pending |
| DMF-07 | Desktop `_catalog_library_mode` defaults `'hide'` (in-memory, per-launch); mode threaded through `_CatalogRefreshWorker` → `get_browse_results` | gui-unit | — | N/A | unit (gui-marked) | `… pytest tests/test_libfilter_desktop.py -x -q` | ❌ W0 (extend) | ⬜ pending |
| DMF-08 | Web catalog restore migrates plain list → Show-only; `{mode,codes}` dict round-trips via safe_storage | unit | — | safe_storage chokepoint only (V3) | unit (pure mirror) | `pytest tests/test_catalog_dual_mode_library_filter.py -x -q` | ❌ W0 (new) | ⬜ pending |
| DMF-08 | `apply_catalog_library_filter` persists dict shape; `c != 'LOCAL'` guard present in validation comprehension | AST scan | — | V5 input validation | AST source | `pytest tests/test_catalog_dual_mode_library_filter.py -x -q` | ❌ W0 (new) | ⬜ pending |
| DMF-08 | SEED-023 PGP/Editions catalog filters unaffected by library-mode change | regression | — | N/A | unit | `… pytest tests/test_catalog_availability_filter.py -x -q` | ✅ existing | ⬜ pending |
| DMF-09 | `ParallelsState` gains `library_mode`/`library_filter` with correct defaults | unit | — | N/A | unit | `pytest tests/test_parallels_library_filter.py -x -q` | ❌ W0 (new) | ⬜ pending |
| DMF-09 | Parallels restores from `parallels_library_filter` key; migrates legacy list | unit | — | safe_storage chokepoint only (V3) | unit | `pytest tests/test_parallels_library_filter.py -x -q` | ❌ W0 (new) | ⬜ pending |
| DMF-09 | Parallels library filter applies (Show-only ∈ set, Hide ∉ set) over results; scopes via `restrict_sys_ids` path | unit | — | V5 input validation | unit (pure mirror) | `pytest tests/test_parallels_library_filter.py -x -q` | ❌ W0 (new) | ⬜ pending |
| DMF-10 | `'LOCAL'` absent from all new web functions referencing `LIBRARY_CODES`; allowlist stays `[]` | guard (existing) | — | structural | AST guard | `pytest tests/test_web_library_options_no_local.py tests/test_phase_97_invariants.py tests/test_no_raw_storage_access.py -x -q` | ✅ existing | ⬜ pending |
| DMF-12 | Web catalog dialog has count-shortlist + expand-all-A–Z + client-side text-search + sort-by-count/A–Z | AST scan | — | N/A | AST source | `pytest tests/test_catalog_dual_mode_library_filter.py -x -q` | ❌ W0 (new) | ⬜ pending |
| DMF-13 | `library_codes_with_manuscripts()` used (not bare `LIBRARY_CODES`) in desktop dialog + web catalog dialog + web parallels dialog | AST scan | — | structural | AST source | `pytest tests/test_catalog_dual_mode_library_filter.py tests/test_parallels_library_filter.py tests/test_libfilter_desktop.py -x -q` | ❌ W0 (new/extend) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_catalog_dual_mode_library_filter.py` — **new file**; covers DMF-08, DMF-12, DMF-10 (catalog surface): AST source contracts + pure-mirror behavior tests for the restore migration, dict-shape persistence, and dialog affordances.
- [ ] `tests/test_parallels_library_filter.py` — **new file**; covers DMF-09, DMF-10 (parallels surface): `ParallelsState` defaults, restore/migration, post-fetch Show-only/Hide filter behavior.
- [ ] `tests/test_libfilter_desktop.py` — **extend existing**; add: `LibraryFilterDialog` mode parameter + `get_mode()`, D-04 mode-flip reset, OK-guard mode-awareness, `_catalog_library_mode` default + threading. **Register the file in `_GUI_TEST_FILES`** if not already.

*Existing infrastructure (`test_web_library_options_no_local.py`, `test_phase_97_invariants.py`, `test_no_raw_storage_access.py`, `test_catalog_availability_filter.py`) covers the guard/regression requirements — no framework installs needed.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Desktop catalog dialog renders the segmented toggle + shortlist/expand/search and re-applies mode+set on reopen within a session | DMF-07 | PyQt6 live-render not exercised by offscreen unit tests | Launch `python genizah_app.py` → Catalog → Browse by Identification → open library filter → toggle Hide/Show-only, pick libs, Apply, reopen → confirm state |
| Web catalog dialog + `/parallels` control render correctly (RTL/EN), counts/sort behave, persistence survives reload | DMF-08, DMF-09, DMF-12 | NiceGUI async render path not covered by headless pytest (project memory: render-smoke gap) | Live-client render smoke on `/search`'s sibling surfaces: open `/parallels` + catalog, exercise toggle/search/sort, reload page, confirm persisted state |

*Render-smoke is the established sign-off for NiceGUI/PyQt UI per project convention; the automated tests above prove the pure model + guards.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (2 new test files + 1 extension)
- [ ] No watch-mode flags
- [ ] Feedback latency < 150s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
