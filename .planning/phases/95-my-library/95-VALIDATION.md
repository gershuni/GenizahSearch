---
phase: 95
slug: my-library
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-21
---

# Phase 95 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Populated from `95-RESEARCH.md` `## Validation Architecture` section.

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
> Filled in by planner with concrete task IDs once PLAN.md slots are written.
> Wave-0 tests must exist as red stubs before any green task lands.

| Task ID | Plan | Wave | Requirement | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------|-------------------|-------------|--------|
| 95-01-W0 | 01 | 0 | REQ-1..10 | wave-0 stubs | `python -m pytest tests/test_local_*.py --collect-only` | ❌ W0 | ⬜ pending |
| TBD-01 | TBD | TBD | REQ-1 (local_indexer module) | unit | `pytest tests/test_local_indexer.py::test_pymupdf_hebrew_extraction_quality` | ❌ W0 | ⬜ pending |
| TBD-02 | TBD | TBD | REQ-1 (RTL helpers ported) | unit | `pytest tests/test_local_indexer_rtl.py` | ❌ W0 | ⬜ pending |
| TBD-03 | TBD | TBD | REQ-2 (sys_id namespace) | unit | `pytest tests/test_local_sys_id_namespace.py` | ❌ W0 | ⬜ pending |
| TBD-04 | TBD | TBD | D-13 (parser compat) | unit | `pytest tests/test_local_sys_id_parser_compat.py` | ❌ W0 | ⬜ pending |
| TBD-05 | TBD | TBD | D-08 (post-dedup merge) | unit | `pytest tests/test_local_post_dedup_merge.py` | ❌ W0 | ⬜ pending |
| TBD-06 | TBD | TBD | REQ-3 (side-index merge / RRF) | unit | `pytest tests/test_side_index_merge.py` | ❌ W0 | ⬜ pending |
| TBD-07 | TBD | TBD | REQ-3 (LAB merge + invalidation) | unit | `pytest tests/test_local_lab_invalidation.py` | ❌ W0 | ⬜ pending |
| TBD-08 | TBD | TBD | REQ-5 (mtime cache) | unit | `pytest tests/test_local_indexer_incremental.py` | ❌ W0 | ⬜ pending |
| TBD-09 | TBD | TBD | D-21 (two-phase commit) | integration | `pytest tests/test_local_two_phase_commit.py` | ❌ W0 | ⬜ pending |
| TBD-10 | TBD | TBD | D-20 (delete-by-uid) | unit | `pytest tests/test_local_delete_by_uid.py` | ❌ W0 | ⬜ pending |
| TBD-11 | TBD | TBD | D-25 (indexer mutex) | unit | `pytest tests/test_local_indexer_mutex.py` | ❌ W0 | ⬜ pending |
| TBD-12 | TBD | TBD | D-37 (open-fallback) | unit | `pytest tests/test_local_index_open_fallback.py` | ❌ W0 | ⬜ pending |
| TBD-13 | TBD | TBD | D-40 (unavailable folder) | unit | `pytest tests/test_local_unavailable_folder.py` | ❌ W0 | ⬜ pending |
| TBD-14 | TBD | TBD | D-17 (folder overlap) | unit | `pytest tests/test_folder_overlap_detection.py` | ❌ W0 | ⬜ pending |
| TBD-15 | TBD | TBD | D-42 (canonical filepath) | unit | `pytest tests/test_canonical_filepath.py` | ❌ W0 | ⬜ pending |
| TBD-16 | TBD | TBD | REQ-6 (filter cascade) | static AST | `pytest tests/test_local_filter_cascade.py` | ❌ W0 | ⬜ pending |
| TBD-17 | TBD | TBD | D-39 (filter persistence) | unit | `pytest tests/test_local_filter_persistence.py` | ❌ W0 | ⬜ pending |
| TBD-18 | TBD | TBD | REQ-9 (no /api leak) | unit | `pytest tests/test_local_namespace_no_api_leak.py` | ❌ W0 | ⬜ pending |
| TBD-19 | TBD | TBD | REQ-9 (no Lists leak) | unit | `pytest tests/test_local_namespace_no_lists_leak.py` | ❌ W0 | ⬜ pending |
| TBD-20 | TBD | TBD | REQ-9 (no corrections leak) | unit | `pytest tests/test_local_namespace_no_corrections_leak.py` | ❌ W0 | ⬜ pending |
| TBD-21 | TBD | TBD | D-46 (web library options) | static AST | `pytest tests/test_web_library_options_no_local.py` | ❌ W0 | ⬜ pending |
| TBD-22 | TBD | TBD | D-45 (export handling) | unit | `pytest tests/test_export_dossier_local_handling.py` | ❌ W0 | ⬜ pending |
| TBD-23 | TBD | TBD | D-35 (schema evolution) | unit | `pytest tests/test_local_schema_evolution.py` | ❌ W0 | ⬜ pending |
| TBD-24 | TBD | TBD | D-43 (PyInstaller smoke) | packaging | `pytest -m packaging tests/test_local_pyinstaller_smoke.py` | ❌ W0 | ⬜ pending |
| TBD-25 | TBD | TBD | REQ-10 (ceiling enforcement) | unit | `pytest tests/test_local_ceiling_enforcement.py` | ❌ W0 | ⬜ pending |
| TBD-26 | TBD | TBD | REQ-7/REQ-8 (badge + tab) | integration | `pytest tests/test_my_library_tab.py` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_local_indexer.py` — stubs for REQ-1 + REQ-4 (PyMuPDF Hebrew + RTL helpers)
- [ ] `tests/test_local_sys_id_namespace.py` — stubs for REQ-2 (sys_id format guarantees)
- [ ] `tests/test_local_sys_id_parser_compat.py` — stubs for D-13 (parser generalization)
- [ ] `tests/test_local_post_dedup_merge.py` — stubs for D-08 (post-dedup ordering)
- [ ] `tests/test_side_index_merge.py` — stubs for REQ-3 (RRF merger)
- [ ] `tests/test_local_lab_invalidation.py` — stubs for D-09 + D-38
- [ ] `tests/test_local_indexer_incremental.py` — stubs for REQ-5 (mtime cache)
- [ ] `tests/test_local_two_phase_commit.py` — stubs for D-21 (fault-injection harness)
- [ ] `tests/test_local_delete_by_uid.py` — stubs for D-20 + D-36
- [ ] `tests/test_local_indexer_mutex.py` — stubs for D-25
- [ ] `tests/test_local_index_open_fallback.py` — stubs for D-37
- [ ] `tests/test_local_unavailable_folder.py` — stubs for D-40
- [ ] `tests/test_folder_overlap_detection.py` — stubs for D-17 (Windows path normalization)
- [ ] `tests/test_canonical_filepath.py` — stubs for D-42
- [ ] `tests/test_local_filter_cascade.py` — stubs for REQ-6 cascade discipline (static AST)
- [ ] `tests/test_local_filter_persistence.py` — stubs for D-39 per-surface keys
- [ ] `tests/test_local_namespace_no_api_leak.py` — stubs for REQ-9 /api/search
- [ ] `tests/test_local_namespace_no_lists_leak.py` — stubs for REQ-9 + D-30 (Codex P0)
- [ ] `tests/test_local_namespace_no_corrections_leak.py` — stubs for REQ-9 corrections
- [ ] `tests/test_web_library_options_no_local.py` — stubs for D-46 (static AST)
- [ ] `tests/test_export_dossier_local_handling.py` — stubs for D-45
- [ ] `tests/test_local_schema_evolution.py` — stubs for D-35
- [ ] `tests/test_local_ceiling_enforcement.py` — stubs for REQ-10
- [ ] `tests/test_my_library_tab.py` — stubs for REQ-7 + REQ-8 (badge + tab registration)
- [ ] `tests/test_local_pyinstaller_smoke.py` — stubs for D-43 (gated `@pytest.mark.packaging`)
- [ ] `tests/fixtures/local_indexer/hebrew_sample.pdf` + `.expected.txt` — fixture for D-44 (planner picks)
- [ ] `tests/conftest.py` — fixtures: temp INDEX_DIR, mock Tantivy, mock Supabase client (for cloud-write gate tests)

*Pytest framework already installed; no install needed.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| MyLibraryTab visual UX | REQ-8 | UI rendering, color, layout | Launch desktop app; verify tab is the 7th tab; verify Add Folder / Refresh / Cancel buttons; verify per-file status panel updates during scan |
| Hebrew PDF extraction quality on real corpora | D-01 / D-44 | Quality-of-extraction is subjective for edge fonts/layouts | Run indexer on 3–5 user-provided Hebrew PDFs; spot-check that search hits the expected phrases |
| `LOCAL` badge color appearance | D-11 | Visual color verification (`#3498db` blue) | Open desktop, run any search that matches a LOCAL file; verify Src cell is blue |
| About dialog credit line | D-32 | String literal in dialog — visual check | Open About dialog (both apps); verify Seewald attribution line present in EN + HE |
| Help page LOCAL section visible | D-31 + D-33 | Visual presence + Hebrew translation accuracy | Open Help; verify "My Library" section + cleartext-on-disk disclosure line in EN + HE |
| Open File button launches OS default app | D-28 | OS shell integration | Click Open File on a LOCAL hit; verify Word/Acrobat/Notepad launches with the file |
| Hostname-rename re-extract behavior | D-19 | Requires changing OS hostname between runs | Index 2 files; rename hostname; restart app; verify auto-rescan re-extracts both files (new sys_ids) |
| PyInstaller-built EXE Hebrew extraction | D-43 | Smoke test gated `@pytest.mark.packaging` — runs in release CI, validated manually on first release | After build, run packaged EXE; index a Hebrew PDF; verify text extracted correctly |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 130s (full suite)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending (filled at planner step)
