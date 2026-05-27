---
phase: 99
slug: pdf-page-renderer
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-27
---

# Phase 99 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (project standard, ~2500+ tests) |
| **Config file** | none special — `pytest tests/` |
| **Quick run command** | `pytest tests/test_pdf_page_renderer.py -x` |
| **Full suite command** | `pytest tests/` |
| **Estimated runtime** | ~5s quick / full suite minutes |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_pdf_page_renderer.py -x`
- **After every plan wave:** Run `pytest tests/test_pdf_page_renderer.py tests/test_local_indexer.py tests/test_local_pdf_extraction_fallback.py`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** ~5 seconds (quick), minutes (full)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|-----------------|-----------|-------------------|-------------|--------|
| 99-render-single | 99-01 | 1 | PDFIMG-01 | render page N → non-null QImage of expected dims; fitz idx = page_num-1 | unit | `pytest tests/test_pdf_page_renderer.py::test_render_single_page -x` | ❌ W0 | ⬜ pending |
| 99-render-once | 99-01 | 1 | PDFIMG-01 | only the requested page is rendered (get_pixmap called exactly once — "no bulk render") | unit | `pytest tests/test_pdf_page_renderer.py::test_only_requested_page_rendered -x` | ❌ W0 | ⬜ pending |
| 99-qimage-copy | 99-01 | 1 | PDFIMG-01 | `.copy()` produces independent QImage (no use-after-free) | unit | `pytest tests/test_pdf_page_renderer.py::test_qimage_independent_of_pixmap -x` | ❌ W0 | ⬜ pending |
| 99-lru-evict | 99-01 | 1 | PDFIMG-02 | LRU evicts + closes oldest doc past maxsize; closes all on shutdown | unit | `pytest tests/test_pdf_page_renderer.py::test_doc_lru_evict_and_close -x` | ❌ W0 | ⬜ pending |
| 99-lru-close-err | 99-01 | 1 | PDFIMG-02 | a doc whose close() raises does not break eviction/close_all (best-effort) | unit | `pytest tests/test_pdf_page_renderer.py::test_lru_eviction_survives_close_error -x` | ❌ W0 | ⬜ pending |
| 99-no-disk-cache | 99-01 | 1 | PDFIMG-02 | no on-disk image cache written during render | unit | `pytest tests/test_pdf_page_renderer.py::test_no_disk_cache -x` | ❌ W0 | ⬜ pending |
| 99-fail-missing | 99-01 | 1 | PDFIMG-06 | missing file → MISSING_FILE reason + log, no raise to caller | unit | `pytest tests/test_pdf_page_renderer.py::test_missing_file_reason -x` | ❌ W0 | ⬜ pending |
| 99-fail-notpdf | 99-01 | 1 | PDFIMG-06 | non-pdf extension → NOT_PDF | unit | `pytest tests/test_pdf_page_renderer.py::test_not_pdf_reason -x` | ❌ W0 | ⬜ pending |
| 99-pdf-uppercase | 99-01 | 1 | PDFIMG-06 | `.PDF`/`.Pdf` (case-insensitive) NOT misclassified as NOT_PDF; renders | unit | `pytest tests/test_pdf_page_renderer.py::test_uppercase_pdf_not_misclassified -x` | ❌ W0 | ⬜ pending |
| 99-fail-encrypted | 99-01 | 1 | PDFIMG-06 | encrypted PDF → ENCRYPTED | unit | `pytest tests/test_pdf_page_renderer.py::test_encrypted_reason -x` | ❌ W0 | ⬜ pending |
| 99-fail-corrupt | 99-01 | 1 | PDFIMG-06 | corrupt PDF → CORRUPT | unit | `pytest tests/test_pdf_page_renderer.py::test_corrupt_reason -x` | ❌ W0 | ⬜ pending |
| 99-pdf-bad-bytes | 99-01 | 1 | PDFIMG-06 | `.pdf` suffix but non-PDF bytes → CORRUPT (suffix-pass ≠ openable) | unit | `pytest tests/test_pdf_page_renderer.py::test_pdf_suffix_corrupt_bytes -x` | ❌ W0 | ⬜ pending |
| 99-fail-oob | 99-01 | 1 | PDFIMG-06 | page index out of range → PAGE_OUT_OF_RANGE (validated pre-render, D-04a) | unit | `pytest tests/test_pdf_page_renderer.py::test_page_out_of_range -x` | ❌ W0 | ⬜ pending |
| 99-page-zero | 99-01 | 1 | PDFIMG-06 | page_num=0 → idx=-1 → PAGE_OUT_OF_RANGE; get_pixmap not called | unit | `pytest tests/test_pdf_page_renderer.py::test_page_num_zero -x` | ❌ W0 | ⬜ pending |
| 99-fail-logged | 99-01 | 1 | PDFIMG-06 | every failure logs reason + detail EXACTLY ONCE (single _log_and_raise helper) | unit | `pytest tests/test_pdf_page_renderer.py::test_failures_logged -x` | ❌ W0 | ⬜ pending |
| 99-token-echo | 99-01→99-02 | 1→2 | PDFIMG-02 (D-03) | signals echo token+sys_id+page_num so stale results are discardable | unit | `pytest tests/test_pdf_page_renderer.py::test_token_echoed_in_signals -x` | ❌ W0 | ⬜ pending |
| 99-worker-fail-route | 99-02 | 2 | PDFIMG-06 | worker failure routes to render_failed with classified reason; render_succeeded not fired | unit | `pytest tests/test_pdf_page_renderer.py::test_worker_failure_routes_to_render_failed -x` | ❌ W0 | ⬜ pending |
| 99-worker-survives | 99-02 | 2 | PDFIMG-06 (D-09) | real-thread: bad render then valid render on same worker — thread survives, serves next | unit | `pytest tests/test_pdf_page_renderer.py::test_worker_survives_bad_render_and_serves_next -x` | ❌ W0 | ⬜ pending |
| 99-enqueue-after-stop | 99-02 | 2 | PDFIMG-02 | enqueue() after stop() returns False + drops (no orphan work) | unit | `pytest tests/test_pdf_page_renderer.py::test_enqueue_after_stop_dropped -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Plan 01 Task 1 creates the test file with the 16 Plan-01 rows (RED); Plan 02 Task 2 upgrades `99-token-echo` and adds the 3 worker rows → 19 tests total.*

---

## Wave 0 Requirements

- [ ] `tests/test_pdf_page_renderer.py` — covers PDFIMG-01/02/06 + D-03 token echo (all 19 rows above)
- [ ] Encrypted + corrupt + multi-page PDF fixtures via `scripts/generate_pdf_render_fixtures.py` (`PDF_ENCRYPT_AES_256` for encrypted; garbage bytes for corrupt) — committed under `tests/fixtures/local_indexer/`; generator is a regeneration tool; test pytest.fail's if a fixture is missing
- [ ] No framework install needed — pytest + fitz already present; pytest-qt is NOT installed and is NOT required

*Test strategy (mirrors `tests/test_folder_walk_worker.py`, the in-repo QThread-without-pytest-qt precedent): unit-test render functions + LRU directly (no QThread). For the worker, drive `_handle_request(item)` SYNCHRONOUSLY (the thread assertion lives in `run()`, not `_handle_request`) for deterministic per-request tests; use ONE real-thread test (`worker.start()` + `Qt.ConnectionType.DirectConnection` + `threading.Event`) only for the loop-continuity proof. No `qtbot`/`pytestqt`.*

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| ~8s soft watchdog shows timeout placeholder without UI freeze | PDFIMG-06 (D-05) | Timeout placeholder + watchdog wiring lands in Phase 100; the Phase-99 worker only emits success/failure (timeout is a UI-controller concern per D-07) | Deferred to Phase 100 UAT |
| Rendered page feeds `ManuscriptViewerWidget.display_image()` with working zoom/pan | PDFIMG-01 (D-02) | Display wiring is Phase 100 scope | Deferred to Phase 100 UAT |

*Phase 99 is a headless service; visual verification is Phase 100.*

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s (quick)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
