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

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 99-render-single | TBD | — | PDFIMG-01 | — | render page N → non-null QImage of expected dims; fitz idx = page_num-1 | unit | `pytest tests/test_pdf_page_renderer.py::test_render_single_page -x` | ❌ W0 | ⬜ pending |
| 99-qimage-copy | TBD | — | PDFIMG-01 | — | `.copy()` produces independent QImage (no use-after-free) | unit | `pytest tests/test_pdf_page_renderer.py::test_qimage_independent_of_pixmap -x` | ❌ W0 | ⬜ pending |
| 99-lru-evict | TBD | — | PDFIMG-02 | — | LRU evicts + closes oldest doc past maxsize; closes all on shutdown | unit | `pytest tests/test_pdf_page_renderer.py::test_doc_lru_evict_and_close -x` | ❌ W0 | ⬜ pending |
| 99-no-disk-cache | TBD | — | PDFIMG-02 | — | no on-disk image cache written during render | unit | `pytest tests/test_pdf_page_renderer.py::test_no_disk_cache -x` | ❌ W0 | ⬜ pending |
| 99-fail-missing | TBD | — | PDFIMG-06 | — | missing file → MISSING_FILE reason + log, no raise to caller | unit | `pytest tests/test_pdf_page_renderer.py::test_missing_file_reason -x` | ❌ W0 | ⬜ pending |
| 99-fail-notpdf | TBD | — | PDFIMG-06 | — | non-pdf extension → NOT_PDF | unit | `pytest tests/test_pdf_page_renderer.py::test_not_pdf_reason -x` | ❌ W0 | ⬜ pending |
| 99-fail-encrypted | TBD | — | PDFIMG-06 | — | encrypted PDF → ENCRYPTED | unit | `pytest tests/test_pdf_page_renderer.py::test_encrypted_reason -x` | ❌ W0 | ⬜ pending |
| 99-fail-corrupt | TBD | — | PDFIMG-06 | — | corrupt PDF → CORRUPT | unit | `pytest tests/test_pdf_page_renderer.py::test_corrupt_reason -x` | ❌ W0 | ⬜ pending |
| 99-fail-oob | TBD | — | PDFIMG-06 | — | page index out of range → PAGE_OUT_OF_RANGE (validated pre-render, D-04a) | unit | `pytest tests/test_pdf_page_renderer.py::test_page_out_of_range -x` | ❌ W0 | ⬜ pending |
| 99-fail-logged | TBD | — | PDFIMG-06 | — | every failure path logs reason + detail | unit | `pytest tests/test_pdf_page_renderer.py::test_failures_logged -x` | ❌ W0 | ⬜ pending |
| 99-token-echo | TBD | — | PDFIMG-02 (D-03) | — | signals echo token so stale results are discardable by controller | unit | `pytest tests/test_pdf_page_renderer.py::test_token_echoed_in_signals -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*
*Task IDs are placeholders — planner replaces with real `{plan}-{task}` IDs.*

---

## Wave 0 Requirements

- [ ] `tests/test_pdf_page_renderer.py` — covers PDFIMG-01/02/06 + D-03 token echo (all rows above)
- [ ] Encrypted + corrupt + multi-page PDF fixtures — extend `scripts/generate_single_word_fixture.py` or generate inline with `fitz` (`doc.save(path, encryption=fitz.PDF_ENCRYPT_AES_256, owner_pw=..., user_pw=...)` for encrypted; truncate bytes for corrupt)
- [ ] No framework install needed — pytest + fitz already present

*Note: unit-test the render functions + LRU directly (no QThread needed). The QThread wrapper can use a signal-spy if pytest-qt is present, else test the worker's underlying render method synchronously — mirror how existing tests call `extract_pdf_pages()` directly.*

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
