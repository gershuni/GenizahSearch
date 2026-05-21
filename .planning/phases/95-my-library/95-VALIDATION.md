---
phase: 95
slug: my-library
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-05-21
updated: 2026-05-21
---

# Phase 95 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Populated from `95-RESEARCH.md` `## Validation Architecture` section.
> **W3 RESOLVED 2026-05-21:** Per-Task Verification Map now has concrete Plan + Wave assignments (no TBDs remaining). `nyquist_compliant: true` set; `wave_0_complete: false` remains until Wave-0 stubs land in Plan 01.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | `pyproject.toml` (existing) |
| **Quick run command** | `python -m pytest tests/test_local_*.py -x -q` |
| **Full suite command** | `python -m pytest tests/ -q` |
| **Estimated runtime** | ~12s (LOCAL-only), ~120s (full suite ≈2300 tests) |

---

## Sampling Rate

- **After every task commit:** Run `python -m pytest tests/test_local_*.py -x -q` (LOCAL-scoped, ~12s)
- **After every plan wave:** Run `python -m pytest tests/ -q` (full suite, ~120s)
- **Before `/gsd-verify-work`:** Full suite must be green AND `ruff check .` clean
- **Max feedback latency:** 15 seconds (quick), 130 seconds (full)

---

## Per-Task Verification Map

> Mapping derived from 95-RESEARCH.md "Phase Requirements → Test Map" (27 rows).
> **W3 RESOLVED 2026-05-21:** Plan + Wave assignments populated. The Wave-0 stub files
> still need to land via Plan 01 (Task 6) before any green task runs — that's why
> `wave_0_complete: false` remains until Plan 01 is executed.

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 95-01-W0 | 01 | 0 | REQ-1..10 | wave-0 stubs | `python -m pytest tests/test_local_*.py --collect-only` | ❌ W0 | ⬜ pending |
| 95-T-01 | 03 | 1 | REQ-1 (PyMuPDF Hebrew extraction) | unit | `pytest tests/test_local_indexer.py::test_pymupdf_hebrew_extraction_quality` | ❌ W0 | ⬜ pending |
| 95-T-02 | 03 | 1 | REQ-4 (RTL helpers ported as dead-code) | unit | `pytest tests/test_local_indexer.py::test_rtl_helpers_ported` | ❌ W0 | ⬜ pending |
| 95-T-03 | 02 | 1 | REQ-2 (sys_id namespace) | unit | `pytest tests/test_local_sys_id_namespace.py` | ❌ W0 | ⬜ pending |
| 95-T-04 | 02 | 1 | D-13 (parser compat — `parse_header_smart` + `parse_full_id_components`) | unit | `pytest tests/test_local_sys_id_parser_compat.py` | ❌ W0 | ⬜ pending |
| 95-T-05 | 05 | 2 | D-08 (post-dedup merge) | unit + AST | `pytest tests/test_local_post_dedup_merge.py` | ❌ W0 | ⬜ pending |
| 95-T-06 | 05 | 2 | REQ-3 (side-index merge / RRF + W7 tie-break) | unit | `pytest tests/test_side_index_merge.py` | ❌ W0 | ⬜ pending |
| 95-T-07 | 06 | 3 | REQ-3 (LAB merge + D-38 invalidation) | unit | `pytest tests/test_local_lab_invalidation.py` | ❌ W0 | ⬜ pending |
| 95-T-08 | 03 | 1 | REQ-5 (mtime cache + incremental) | unit | `pytest tests/test_local_indexer_incremental.py` | ❌ W0 | ⬜ pending |
| 95-T-09 | 03 | 1 | D-21 (two-phase commit) | integration | `pytest tests/test_local_two_phase_commit.py` | ❌ W0 | ⬜ pending |
| 95-T-10 | 03 | 1 | D-20 (delete-by-uid) | unit | `pytest tests/test_local_delete_by_uid.py` | ❌ W0 | ⬜ pending |
| 95-T-11 | 07 | 3 | D-25 (indexer mutex) | unit | `pytest tests/test_local_indexer_mutex.py` | ❌ W0 | ⬜ pending |
| 95-T-12 | 05 | 2 | D-37 (open-fallback) | unit | `pytest tests/test_local_index_open_fallback.py` | ❌ W0 | ⬜ pending |
| 95-T-13 | 07 | 3 | D-40 (unavailable folder) | unit | `pytest tests/test_local_unavailable_folder.py` | ❌ W0 | ⬜ pending |
| 95-T-14 | 03 | 1 | D-17 (folder overlap detection) | unit | `pytest tests/test_folder_overlap_detection.py` | ❌ W0 | ⬜ pending |
| 95-T-15 | 02 | 1 | D-42 (canonical filepath) | unit | `pytest tests/test_canonical_filepath.py` | ❌ W0 | ⬜ pending |
| 95-T-16 | 08 | 4 | REQ-6 (filter cascade — `_apply_results_table_filters` + `_apply_comp_tree_filters`) | static AST | `pytest tests/test_local_filter_cascade.py` | ❌ W0 | ⬜ pending |
| 95-T-17 | 08 | 4 | D-39 (filter persistence — 3 QSettings keys) | unit | `pytest tests/test_local_filter_persistence.py` | ❌ W0 | ⬜ pending |
| 95-T-18 | 04 | 1 | REQ-9 (no /api leak) | unit | `pytest tests/test_local_namespace_no_api_leak.py` | ❌ W0 | ⬜ pending |
| 95-T-19 | 04 | 1 | REQ-9 + D-30 Codex P0 (no Lists leak; `_get_client.call_count == 0`) | unit | `pytest tests/test_local_namespace_no_lists_leak.py` | ❌ W0 | ⬜ pending |
| 95-T-20 | 04 | 1 | REQ-9 (no corrections leak) | unit | `pytest tests/test_local_namespace_no_corrections_leak.py` | ❌ W0 | ⬜ pending |
| 95-T-21 | 09 | 4 | D-46 (web library options static guard) | static AST | `pytest tests/test_web_library_options_no_local.py` | ❌ W0 | ⬜ pending |
| 95-T-22 | 09 | 4 | D-45 (export-dossier LOCAL handling — `skip_local`) | unit | `pytest tests/test_export_dossier_local_handling.py` | ❌ W0 | ⬜ pending |
| 95-T-23 | 03 | 1 | D-35 (schema evolution — local_files / local_pages / folders / processed_files) | unit | `pytest tests/test_local_schema_evolution.py` | ❌ W0 | ⬜ pending |
| 95-T-24 | 09 | 4 | D-43 (PyInstaller smoke) — also `.spec` packaging from Plan 01 | packaging | `pytest -m packaging tests/test_local_pyinstaller_smoke.py` | ❌ W0 | ⬜ pending |
| 95-T-25 | 07 | 3 | REQ-10 (ceiling enforcement — per-folder + W8 aggregate) | unit | `pytest tests/test_local_ceiling_enforcement.py` | ❌ W0 | ⬜ pending |
| 95-T-26 | 07 + 08 | 3 + 4 | REQ-7 (LOCAL badge) + REQ-8 (My Library tab registered) | integration | `pytest tests/test_my_library_tab.py` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

**Cross-reference notes (W3):**
- Plan 01 (Wave 0) lands the 26 red-stub files + Hebrew PDF fixture + conftest fixtures. **No green tasks may land until Plan 01 ships.**
- Plan 02 (Wave 1) turns sys_id / parser / canonical-filepath stubs green.
- Plan 03 (Wave 1) turns indexer / mtime / two-phase-commit / delete-by-uid / schema / RTL stubs green.
- Plan 04 (Wave 1) turns the three cloud-write-gate stubs green (REQ-9).
- Plan 05 (Wave 2) turns main-search merger / dedup / fallback stubs green (D-08, REQ-3, D-37).
- Plan 06 (Wave 3) turns LAB invalidation stub green (D-09, D-38).
- Plan 07 (Wave 3) turns MyLibraryTab / mutex / ceiling / unavailable-folder stubs green.
- Plan 08 (Wave 4) turns filter cascade / filter persistence / parts of MyLibraryTab badge tests green.
- Plan 09 (Wave 4) turns export / web-library-options / PyInstaller-smoke stubs green.

---

## Wave 0 Requirements

- [ ] `tests/test_local_indexer.py` — stubs for REQ-1 + REQ-4 (PyMuPDF Hebrew + RTL helpers) — turns green in Plan 03
- [ ] `tests/test_local_sys_id_namespace.py` — stubs for REQ-2 (sys_id format guarantees) — turns green in Plan 02
- [ ] `tests/test_local_sys_id_parser_compat.py` — stubs for D-13 (parser generalization) — turns green in Plan 02
- [ ] `tests/test_local_post_dedup_merge.py` — stubs for D-08 (post-dedup ordering) — turns green in Plan 05
- [ ] `tests/test_side_index_merge.py` — stubs for REQ-3 (RRF merger + W7 tie-break) — turns green in Plan 05
- [ ] `tests/test_local_lab_invalidation.py` — stubs for D-09 + D-38 — turns green in Plan 06
- [ ] `tests/test_local_indexer_incremental.py` — stubs for REQ-5 (mtime cache) — turns green in Plan 03
- [ ] `tests/test_local_two_phase_commit.py` — stubs for D-21 (fault-injection harness) — turns green in Plan 03
- [ ] `tests/test_local_delete_by_uid.py` — stubs for D-20 + D-36 — turns green in Plan 03
- [ ] `tests/test_local_indexer_mutex.py` — stubs for D-25 — turns green in Plan 07
- [ ] `tests/test_local_index_open_fallback.py` — stubs for D-37 — turns green in Plan 05
- [ ] `tests/test_local_unavailable_folder.py` — stubs for D-40 — turns green in Plan 07
- [ ] `tests/test_folder_overlap_detection.py` — stubs for D-17 (Windows path normalization) — turns green in Plan 03
- [ ] `tests/test_canonical_filepath.py` — stubs for D-42 — turns green in Plan 02
- [ ] `tests/test_local_filter_cascade.py` — stubs for REQ-6 cascade discipline (static AST) — turns green in Plan 08
- [ ] `tests/test_local_filter_persistence.py` — stubs for D-39 per-surface keys — turns green in Plan 08
- [ ] `tests/test_local_namespace_no_api_leak.py` — stubs for REQ-9 /api/search — turns green in Plan 04
- [ ] `tests/test_local_namespace_no_lists_leak.py` — stubs for REQ-9 + D-30 (Codex P0) — turns green in Plan 04
- [ ] `tests/test_local_namespace_no_corrections_leak.py` — stubs for REQ-9 corrections — turns green in Plan 04
- [ ] `tests/test_web_library_options_no_local.py` — stubs for D-46 (static AST) — turns green in Plan 09
- [ ] `tests/test_export_dossier_local_handling.py` — stubs for D-45 — turns green in Plan 09
- [ ] `tests/test_local_schema_evolution.py` — stubs for D-35 — turns green in Plan 03
- [ ] `tests/test_local_ceiling_enforcement.py` — stubs for REQ-10 (per-folder + W8 aggregate) — turns green in Plan 07
- [ ] `tests/test_my_library_tab.py` — stubs for REQ-7 + REQ-8 (badge + tab registration) — turns green in Plan 07 (tab) + Plan 08 (badge)
- [ ] `tests/test_local_pyinstaller_smoke.py` — stubs for D-43 (gated `@pytest.mark.packaging`) — turns green in Plan 09
- [ ] `tests/fixtures/local_indexer/hebrew_sample.pdf` + `.expected.txt` — fixture for D-44 (planner picks)
- [ ] `tests/conftest.py` — fixtures: temp INDEX_DIR, mock Tantivy, mock Supabase client (for cloud-write gate tests)

*Pytest framework already installed; no install needed.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| MyLibraryTab visual UX | REQ-8 | UI rendering, color, layout | Launch desktop app; verify tab is the 7th tab; verify Add Folder / Refresh / Cancel buttons; verify per-file status panel updates during scan |
| Hebrew PDF extraction quality on real corpora | D-01 / D-44 | Quality-of-extraction is subjective for edge fonts/layouts | Run indexer on 3–5 user-provided Hebrew PDFs; spot-check that search hits the expected phrases |
| `LOCAL` badge color appearance | D-11 | Visual color verification (`#3498db` blue) | Open desktop, run any search that matches a LOCAL file; verify Src cell is blue (results_table AND comp_tree per D-12 audit) |
| About dialog credit line | D-32 | String literal in dialog — visual check | Open About dialog (both apps); verify Seewald attribution line present in EN + HE |
| Help page LOCAL section visible | D-31 + D-33 | Visual presence + Hebrew translation accuracy | Open Help; verify "My Library" section + cleartext-on-disk disclosure line in EN + HE |
| Open File button launches OS default app | D-28 | OS shell integration | Click Open File on a LOCAL hit; verify Word/Acrobat/Notepad launches with the file |
| Hostname-rename re-extract behavior | D-19 | Requires changing OS hostname between runs | Index 2 files; rename hostname; restart app; verify auto-rescan re-extracts both files (new sys_ids) |
| PyInstaller-built EXE Hebrew extraction | D-43 | Smoke test gated `@pytest.mark.packaging` — runs in release CI, validated manually on first release | After build, run packaged EXE; index a Hebrew PDF; verify text extracted correctly |
| W8 aggregate ceiling on Refresh | REQ-10 + D-16 | Requires multiple registered folders to observe aggregate behavior | Register 3 folders each under threshold; click Refresh; aggregate ceiling dialog should appear if sum crosses threshold |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or Wave 0 dependencies
- [x] Wave 0 covers all MISSING references (26 stubs catalogued)
- [x] No watch-mode flags in any plan
- [x] Per-task Plan/Wave assignments populated (W3 RESOLVED)
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify (verified during execute)
- [ ] Feedback latency < 130s (full suite) (verified during execute)
- [x] `nyquist_compliant: true` set in frontmatter
- [ ] `wave_0_complete: true` — pending Plan 01 execution

**Approval:** plan-set approved; ready for `/gsd-execute-phase 95 --wave 0` (W3 — 2026-05-21).
