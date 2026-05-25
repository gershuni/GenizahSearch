---
phase: 97
slug: more-local-features
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-25
---

# Phase 97 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from `97-RESEARCH.md` § Validation Architecture (22 test files + 5 fixtures gap).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x (existing — 2,532 tests passing at v7.14.0 close) |
| **Config file** | `pytest.ini` (existing) |
| **Quick run command** | `pytest tests/test_phase_97_*.py -x` |
| **Full suite command** | `pytest tests/ -x` |
| **Estimated runtime** | ~120 seconds (full); ~10 seconds (phase-97 subset) |

---

## Sampling Rate

- **After every task commit:** `pytest tests/test_phase_97_*.py -x` (or the targeted file for the surface touched)
- **After every plan wave:** `pytest tests/ -x` (full ~2,532+ suite)
- **Before `/gsd-verify-work`:** Full suite must be green; scale-smoke test pulled manually (`pytest tests/test_50k_scale_smoke.py --run-scale -x`, ~10-20 min on dev box)
- **Max feedback latency:** 120 seconds full / 10 seconds targeted

---

## Per-Decision Verification Map

| Decision | Wave | Behavior | Threat Ref | Test Type | Automated Command | File Exists |
|----------|------|----------|------------|-----------|-------------------|-------------|
| D-NEW-1 | A | Migration v1→v2 idempotent, three-fixture | T-A-1 schema rollback | unit | `pytest tests/test_local_indexer_migrations.py -x` | ❌ W0 |
| D-NEW-1 | A | `integrity_check` failure surfaces, no auto-delete | T-A-2 | unit | `pytest tests/test_local_indexer_migrations.py::test_integrity_check_fail -x` | ❌ W0 |
| D-NEW-1 | F | 1→2 prunes rows where extension NOT IN supported AND status IS NULL | T-A-3 | unit | `pytest tests/test_local_indexer_migrations.py::test_prune_unsupported -x` | ❌ W0 |
| R-02 | A | Atomic swap closes `local_searcher` + `local_lab_searcher` BEFORE `os.rename` | T-A-4 reader-handle leak | integration | `pytest tests/test_atomic_rebuild.py::test_close_before_rename -x` | ❌ W0 |
| R-02 | A | Corrupt LOCAL index → rebuild fires → search restored from `cached_text` | T-A-5 | integration | `pytest tests/test_atomic_rebuild.py::test_corrupt_recovery -x` | ❌ W0 |
| R-02 | A | `.old-<ts>` dir removed on next clean shutdown | T-A-6 | integration | `pytest tests/test_atomic_rebuild.py::test_old_dir_cleanup -x` | ❌ W0 |
| R-03 | A | zstd round-trip on Hebrew + English chunks | T-A-7 | unit | `pytest tests/test_cached_text.py::test_roundtrip_hebrew -x` | ❌ W0 |
| R-04 | A | Two-phase commit + `synchronous=FULL` on `pending→committed` UPDATE survives subprocess kill | T-A-8 power-loss | integration | `pytest tests/test_two_phase_durability.py::test_power_loss_simulation -x` | ❌ W0 |
| R-01 | A | LOCAL search gated (`is_searchable=False`) until Resume/Restart/Skip resolved; banner shown | T-A-9 | integration | `pytest tests/test_recovery_gate.py::test_search_returns_empty_during_recovery -x` | ❌ W0 |
| C-02 | B | Commit fires when bytes > 200 MB OR files > 100 OR seconds > 60 (no heap path in tantivy-py 0.25.1) | T-B-1 | integration | `pytest tests/test_commit_triggers.py -x` | ❌ W0 |
| C-05 | B | XLSX zip-bomb rejected via uncompressed-size sum from `ZipInfo` BEFORE openpyxl iter | T-B-2 zip-bomb | unit | `pytest tests/test_xlsx_extraction.py::test_zip_bomb_defense -x` | ❌ W0 |
| C-06 | D | Disk-headroom warning fires when `(free - estimated_growth - 2×index_size) < 1 GB` | T-D-1 | unit | `pytest tests/test_disk_headroom.py -x` | ❌ W0 |
| F-01 | C | HTML chunking at h1/h2; 20-paragraph fallback when sparse (< 3 h1/h2 OR avg inter-heading < 5) | T-C-1 | unit | `pytest tests/test_html_extraction.py -x` | ❌ W0 |
| F-01 | C | RTL Hebrew HTML round-trips unchanged (NO `_fix_rtl_*` applied — F-06 invariant) | T-C-2 RTL corruption | unit | `pytest tests/test_html_extraction.py::test_rtl_logical_order_preserved -x` | ❌ W0 |
| F-01 | C | Encoding chain: meta charset → chardet → cp1255; uses `lxml.html` (NOT BeautifulSoup) | T-C-3 XXE/billion-laughs | unit | `pytest tests/test_html_extraction.py::test_encoding_chain -x` | ❌ W0 |
| F-02 | C | XLSX per-(sheet, 500-row) chunking via `load_workbook(read_only=True, data_only=True)` | T-C-4 | unit | `pytest tests/test_xlsx_extraction.py -x` | ❌ W0 |
| F-02 | C | `sheetView.rightToLeft=True` → `is_rtl=True` metadata flag (no string mutation) | T-C-5 | unit | `pytest tests/test_xlsx_extraction.py::test_rtl_metadata -x` | ❌ W0 |
| F-03 | C | CSV per-200-row chunking with uniform `cell1 \| cell2 \| ...` joined text | T-C-6 | unit | `pytest tests/test_csv_extraction.py -x` | ❌ W0 |
| F-05 | C | CSV encoding chain (utf-8-sig → cp1255 → utf-16-le); `status='encoding_error'` on total failure | T-C-7 | unit | `pytest tests/test_csv_extraction.py::test_encoding_chain -x` | ❌ W0 |
| F-05 | C | `csv.Sniffer().sniff` detects `,` / `;` / `\t` over 4 KB sample | T-C-8 | unit | `pytest tests/test_csv_extraction.py::test_delimiter_detection -x` | ❌ W0 |
| F-06 | C | Format-RTL invariant: HTML/XLSX/CSV never call Phase 95 `_fix_rtl_line` / `_fix_rtl_page` | T-C-9 cross-format invariant | unit | `pytest tests/test_format_rtl_invariant.py -x` | ❌ W0 |
| U-01 | E | Phase-aware ETA reports 4 sub-phases (walk/extract/commit/rebuild-LAB) with separate smoothing | T-E-1 | unit | `pytest tests/test_phase_aware_eta.py -x` | ❌ W0 |
| U-02 | E | `Discard` deletes only docs/rows with current `scan_run_id` via `Term("scan_run_id", run_id)` | T-E-2 cross-run leak | integration | `pytest tests/test_scan_run_id.py::test_discard_only_this_run -x` | ❌ W0 |
| U-02 | E | `scan_run_id` NOT written on no-op skipped (mtime-unchanged) rows | T-E-3 | unit | `pytest tests/test_scan_run_id.py::test_no_run_id_on_skipped -x` | ❌ W0 |
| U-03 | E | FolderWalkWorker emits batched `pyqtSignal(list)` throttled to ≤ 100 files OR 0.5 s | T-E-4 | integration | `pytest tests/test_folder_walk_worker.py -x` | ❌ W0 |
| U-03 | E | NO QWidget mutation from worker thread (assertion via inspect of slots) | T-E-5 race | integration | `pytest tests/test_folder_walk_worker.py::test_no_widget_mutation -x` | ❌ W0 |
| U-04 | E | View All renders first 50 then appends via `QTimer.singleShot(0, …)` — event loop stays responsive | T-E-6 | integration | `pytest tests/test_view_all_incremental.py -x` | ❌ W0 |
| U-04 | E | View All cap raised 200 → 500 (`_VIEW_ALL_PAGE_CAP`) | T-E-7 | unit | `pytest tests/test_view_all_cap.py -x` | ❌ W0 |
| D-NEW-2 | F | Unreachable folder (ENOENT/EACCES) → `folders.status='unreachable'`; auto-rescan skips | T-F-1 | unit | `pytest tests/test_network_drive_semantics.py::test_enoent -x` | ❌ W0 |
| D-NEW-2 | F | Transient ETIMEDOUT retries 3× with 2 s backoff, then `status='timeout'` | T-F-2 | unit | `pytest tests/test_network_drive_semantics.py::test_etimedout_retry -x` | ❌ W0 |
| D-NEW-3 | F | File changed mid-extraction → re-queue; max 3 retries per scan_run | T-F-3 TOCTOU | integration | `pytest tests/test_changed_during_index.py -x` | ❌ W0 |
| D-NEW-4 | F | Only supported extensions OR rows with non-NULL error status get SQLite rows | T-F-4 | unit | `pytest tests/test_local_indexer_migrations.py::test_supported_only_policy -x` | ❌ W0 |
| D-NEW-5 | F | `chunk_locator` strings: `p. N` / `¶ N-M` / `§ <heading>` / `<sheet>!R<n>:R<m>` / `rows N-M` | T-F-5 | unit | `pytest tests/test_chunk_locator.py -x` | ❌ W0 |
| D-NEW-6 | F | Help + About strings contain bilingual EN+HE cleartext disclosure (mirrors Phase 95 D-33) | T-F-6 disclosure regression | static | `pytest tests/test_privacy_disclosure_strings.py -x` | ❌ W0 |
| D-NEW-7 (a) | F | AST scanner: cloud-write gates at TOP of 3 modules | T-F-7 | static | `pytest tests/test_phase_97_invariants.py::test_cloud_write_gates_at_top -x` | ❌ W0 |
| D-NEW-7 (b) | F | AST scanner: web LIBRARY_CODES allowlist `[]` (multitenant invariant) | T-F-8 | static | `pytest tests/test_phase_97_invariants.py::test_web_library_codes_empty_allowlist -x` | ❌ W0 |
| D-NEW-7 (c) | F | `is_local_sys_id()` recognizes 18-digit 97-prefixed sys_ids | T-F-9 | unit | `pytest tests/test_phase_97_invariants.py::test_is_local_sys_id -x` | ❌ W0 |
| D-NEW-7 (d) | F | LOCAL RRF merge happens POST-`_deduplicate()` (Phase 95 D-08 invariant) | T-F-10 | integration | `pytest tests/test_phase_97_invariants.py::test_local_post_dedup_merge -x` | ❌ W0 |
| D-NEW-8 | B | `mtime_ns INTEGER` stored alongside size; cheap first+last 64 KB hash on same-size/same-mtime conflict | T-B-3 | unit | `pytest tests/test_mtime_ns.py -x` | ❌ W0 |
| Scale | E | 50K-file synthetic corpus completes without crash (skip in CI via `@pytest.mark.scale`) | T-E-8 | manual | `pytest tests/test_50k_scale_smoke.py --run-scale -x` | ❌ W0 |

*Status keys: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky · ❌ W0 = file does not yet exist (Wave 0 dependency)*

---

## Wave 0 Requirements

22 new test files + 5 new fixtures. Wave 0 installs are interleaved into the matching plan (each format/decision plan adds its own test stubs). No new framework install — pytest already in `requirements-desktop.txt`.

- [ ] `tests/test_local_indexer_migrations.py` — D-NEW-1 + D-NEW-4 (5 tests)
- [ ] `tests/test_atomic_rebuild.py` — R-02 (3 tests)
- [ ] `tests/test_cached_text.py` — R-03 (1 test)
- [ ] `tests/test_two_phase_durability.py` — R-04 (1 test)
- [ ] `tests/test_recovery_gate.py` — R-01 (1 test)
- [ ] `tests/test_commit_triggers.py` — C-02 (3 tests)
- [ ] `tests/test_xlsx_extraction.py` — F-02 + C-05 (5+ tests)
- [ ] `tests/test_disk_headroom.py` — C-06 (1 test)
- [ ] `tests/test_html_extraction.py` — F-01 (4+ tests)
- [ ] `tests/test_csv_extraction.py` — F-03 + F-05 (4+ tests)
- [ ] `tests/test_format_rtl_invariant.py` — F-06 (1 test)
- [ ] `tests/test_phase_aware_eta.py` — U-01 (1 test)
- [ ] `tests/test_scan_run_id.py` — U-02 (3 tests)
- [ ] `tests/test_folder_walk_worker.py` — U-03 (2 tests)
- [ ] `tests/test_view_all_incremental.py` + `tests/test_view_all_cap.py` — U-04 (2 tests)
- [ ] `tests/test_network_drive_semantics.py` — D-NEW-2 (2 tests)
- [ ] `tests/test_changed_during_index.py` — D-NEW-3 (1 test)
- [ ] `tests/test_chunk_locator.py` — D-NEW-5 (1 test)
- [ ] `tests/test_privacy_disclosure_strings.py` — D-NEW-6 (1 test, EN + HE assertions)
- [ ] `tests/test_phase_97_invariants.py` — D-NEW-7 (4 tests)
- [ ] `tests/test_mtime_ns.py` — D-NEW-8 (1 test)
- [ ] `tests/test_50k_scale_smoke.py` — Scale smoke (1 test, `@pytest.mark.scale`, skip default)
- [ ] Fixtures: `tests/fixtures/local_indexer/hebrew_sample.html`, `hebrew_sample.xlsx`, `hebrew_sample.csv`, `zip_bomb_sample.xlsx`, `multi_sheet_large.xlsx`

---

## Manual-Only Verifications

| Behavior | Decision | Why Manual | Test Instructions |
|----------|----------|------------|-------------------|
| 13K-file / 43 GB end-to-end ingest with crash mid-run, resume on next launch | R-01 / R-02 / R-04 / U-02 | Requires a real corpus matching Seewald's prototype; no CI machine has 43 GB of Hebrew scholarly files. | (a) Point MyLibraryTab at the 13K/43GB corpus. (b) Begin indexing. (c) `taskkill /f` the desktop process at random mid-batch. (d) Reopen app — recovery modal must appear; LOCAL search must be gated; Resume must continue without re-extracting committed files. |
| Network share unavailable mid-scan | D-NEW-2 | Network outages are non-deterministic; CI can't reliably simulate ETIMEDOUT. | (a) Add a UNC path to indexed folders. (b) Disconnect network mid-scan. (c) Indexer must mark folder `timeout` and continue other folders. |
| Bilingual EN+HE Help / About visual review | D-NEW-6 | Strings exist + AST assertion verifies inclusion, but layout/wording polish needs human review. | Open Help page + About dialog; verify EN + HE side-by-side disclosure is readable and matches Phase 95 D-33 voice. |

---

## Validation Sign-Off

- [ ] All decisions have `<automated>` verify or Wave 0 dependency
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all 22 missing test files + 5 fixtures
- [ ] No watch-mode flags
- [ ] Feedback latency < 120s
- [ ] `nyquist_compliant: true` set in frontmatter (set after planner produces plans with `<automated>` verify blocks)

**Approval:** pending
