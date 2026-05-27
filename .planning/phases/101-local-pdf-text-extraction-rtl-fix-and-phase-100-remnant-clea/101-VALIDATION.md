---
phase: 101
slug: local-pdf-text-extraction-rtl-fix-and-phase-100-remnant-clea
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-27
---

# Phase 101 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (configured in `pyproject.toml` `[tool.pytest.ini_options]`) |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `pytest tests/test_local_indexer.py tests/test_local_pdf_extraction_fallback.py tests/test_pdf_image_controller.py -x` |
| **Full suite command** | `pytest tests/ -x` |
| **Estimated runtime** | ~5–10 seconds (quick) / a few minutes (full ~2500 tests) |

---

## Sampling Rate

- **After every task commit:** Run quick command (relevant test file(s))
- **After every plan wave:** Run full suite command
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** ~10 seconds (quick), full suite a few minutes

---

## Per-Task Verification Map

> Scope items map to decisions D-01..D-09 (D-01/D-02/D-05 per the post-research override box in 101-CONTEXT.md). No formal REQ-IDs are mapped to this phase ("Requirements: TBD" in ROADMAP); the four scope items are authoritative.

| Scope Item | Decision | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|------------|----------|------------|-----------------|-----------|-------------------|-------------|--------|
| RTL word-order fix (sort=True path) | D-01/D-03 | — | malformed PDF → `extract_pdf_pages` does not raise (existing try/except) | unit | `pytest tests/test_local_pdf_extraction_fallback.py -x -k rtl_word_order` | ❌ W0 | ⬜ pending |
| LTR no-op (gate `_rtl_ratio > 0.4`) | D-05 | — | N/A | unit | `pytest tests/test_local_pdf_extraction_fallback.py -x -k ltr_noop` | ❌ W0 | ⬜ pending |
| Real Hebrew fixture (inbound asset) | D-06 | — | N/A | unit | `pytest tests/test_local_pdf_extraction_fallback.py -x -k real_hebrew` | ❌ W0 (skip-if-absent until Hillel provides) | ⬜ pending |
| Extractor version bump → PDFs marked pending | D-04 | — | non-PDF files unaffected (extension WHERE filter) | unit | `pytest tests/test_local_indexer.py -x -k extractor_version` | ❌ W0 | ⬜ pending |
| WR-01 single `_lookup_local_filepath` | D-07 | — | `is_pdf` + `filepath` can never diverge | AST/unit | `pytest tests/ -x -k wr01_single_lookup` | ❌ W0 | ⬜ pending |
| WR-02 `_pending` cleared after discard_scope | D-08 | — | no callback retention after scope discard | unit | `pytest tests/test_pdf_image_controller.py -x -k discard_scope_clears_pending` | ❌ W0 | ⬜ pending |
| Flake fix (batch-order isolation) | D-09 | — | N/A | isolation | `pytest tests/test_mupdf_warnings_suppressed.py tests/test_local_indexer.py -v` | ✅ (exists, flaky) | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_local_pdf_extraction_fallback.py` — add `test_sort_true_rtl_word_order_fixed`, `test_sort_true_ltr_noop`, `test_sort_true_rtl_real_hebrew_fixture` (last skips until D-06 fixture committed)
- [ ] `tests/test_local_indexer.py` — add `test_extractor_version_bumps_pdf_to_pending` (D-04)
- [ ] `tests/test_pdf_image_controller.py` — add `test_discard_scope_clears_pending` (WR-02)
- [ ] WR-01 single-lookup assertion (AST guard or behavioral test) — new or in an existing `genizah_app` test file
- [ ] Flake fix: local import inside `test_txt_undecodable_marked_encoding_error` OR conftest autouse fixture (D-09)

*Framework already installed (pytest). No framework install needed.*

---

## Manual-Only Verifications

| Behavior | Decision | Why Manual | Test Instructions |
|----------|----------|------------|-------------------|
| Built EXE runs the RTL reorder | D-03 | python-bidi dropped (D-02 voided) → no Rust `.pyd` packaging risk; word-reversal is pure Python, so packaging risk is minimal, but confirm a built run extracts Hebrew correctly | After build: index a Hebrew PDF in My Library, confirm transcription reads right-to-left in correct word order |
| Auto-reindex self-corrects existing libraries on next launch | D-04 | Requires a pre-existing LOCAL index built with the old extractor | Launch app with an existing LOCAL PDF library → confirm PDFs re-index on startup and reversed text self-corrects |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references
- [ ] No watch-mode flags
- [ ] Feedback latency < 10s (quick)
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
