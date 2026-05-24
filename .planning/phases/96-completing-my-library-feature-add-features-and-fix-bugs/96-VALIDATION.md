---
phase: 96
slug: completing-my-library-feature-add-features-and-fix-bugs
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-24
---

# Phase 96 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Sourced from `96-RESEARCH.md §8 Validation Architecture`.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest (per `pyproject.toml` `[tool.pytest.ini_options]`) |
| **Config file** | `pyproject.toml` |
| **Quick run command** | `pytest tests/test_local_<feature>.py -x` |
| **Full suite command** | `pytest tests/ -q` (~2532 tests as of v7.14.0) |
| **Estimated runtime** | Quick: ~1s · Full: ~80–120s |

---

## Sampling Rate

- **After every task commit:** Run the feature-specific quick command (sub-second).
- **After every plan wave:** Run `pytest tests/test_local_*.py tests/test_web_library_options_no_local.py tests/test_no_raw_storage_access.py -q` (Phase 95 regression bundle + new Phase 96 tests).
- **Before `/gsd-verify-work`:** Full suite must be green AND `python -m ruff check .` clean AND `python scripts/check_docs.py` clean.
- **Max feedback latency:** ≤ 5 seconds per task commit.

---

## Per-Feature Verification Map

| Feature | Behavior | Test Type | Automated Command | File Exists | Status |
|---------|----------|-----------|-------------------|-------------|--------|
| D-F5 | LOCAL hit dict has `highlight_pattern` + `snippet` carries `*…*` markers when regex matches | unit | `pytest tests/test_local_hit_highlighting.py -x` | ❌ Wave 0 | ⬜ pending |
| D-F5 | LOCAL hit dict falls back to first-200-chars when regex doesn't match content | unit | `pytest tests/test_local_hit_highlighting.py::test_no_match_fallback -x` | ❌ Wave 0 | ⬜ pending |
| D-F5 | Search-table render shows highlighted span for LOCAL hits | integration (widget) | `pytest tests/test_local_hit_highlighting.py::test_render_pipeline -x` | ❌ Wave 0 | ⬜ pending |
| D-F5 | ResultDialog render shows highlighted span for LOCAL hits | integration (widget) | `pytest tests/test_local_hit_highlighting.py::test_result_dialog_render -x` | ❌ Wave 0 | ⬜ pending |
| D-F4 | `extract_pdf_pages` on `single_word_per_line.pdf` triggers fallback + returns paragraph-shaped text | unit | `pytest tests/test_local_pdf_extraction_fallback.py::test_pathological_pdf_uses_fallback -x` | ❌ Wave 0 | ⬜ pending |
| D-F4 | `extract_pdf_pages` on Phase 95 existing-good fixture still uses blocks mode (no regression) | unit | `pytest tests/test_local_pdf_extraction_fallback.py::test_good_pdf_stays_blocks -x` | ❌ Wave 0 | ⬜ pending |
| D-F4 | Detection heuristic returns False when <5 lines (small sample skip) | unit | `pytest tests/test_local_pdf_extraction_fallback.py::test_small_sample_skipped -x` | ❌ Wave 0 | ⬜ pending |
| D-F1 | session-JSON round-trip preserves opt-out set | unit | `pytest tests/test_local_optout_persistence.py -x` | ❌ Wave 0 | ⬜ pending |
| D-F1 | Rescan preserves opt-out state for surviving files, drops removed files | unit | `pytest tests/test_local_optout_persistence.py::test_rescan_preserves -x` | ❌ Wave 0 | ⬜ pending |
| D-F1 | Opt-out filter composes with Phase 95 three-state local filter (cascade discipline) | unit | `pytest tests/test_local_optout_filter.py -x` | ❌ Wave 0 | ⬜ pending |
| D-F1 | Opt-out filter applied at BOTH cascade joinpoints (Search + Composition/Parallels) | static AST | extend `tests/test_local_filter_cascade.py` | ✅ exists (extend) | ⬜ pending |
| NEW-1 | `desktop/result_dialog.py` no longer creates `btn_rd_open_browse` widget on LOCAL hits | static AST | `pytest tests/test_result_dialog_local_button_removed.py -x` | ❌ Wave 0 | ⬜ pending |
| NEW-1 | Existing `tests/test_local_browse_panel.py::test_result_dialog_has_view_in_browse_button` deleted or updated | regression | `pytest tests/test_local_browse_panel.py -x` | ✅ exists (must update) | ⬜ pending |
| NEW-2 | `get_local_browse_page` returns correct page on offset=+1 | unit | `pytest tests/test_local_nav_page_chunk.py::test_next_page -x` | ❌ Wave 0 | ⬜ pending |
| NEW-2 | `get_local_browse_page` returns None at boundary (no wrap) | unit | `pytest tests/test_local_nav_page_chunk.py::test_no_wrap -x` | ❌ Wave 0 | ⬜ pending |
| NEW-2 | View-All aggregates all pages with `— page N —` / `— chunk N —` separators | unit | `pytest tests/test_local_nav_page_chunk.py::test_view_all_separators -x` | ❌ Wave 0 | ⬜ pending |
| NEW-2 | PDF file uses "page" label; DOCX/TXT uses "chunk" label | unit | `pytest tests/test_local_nav_page_chunk.py::test_format_aware_label -x` | ❌ Wave 0 | ⬜ pending |
| Phase 95 | All existing LOCAL guard tests stay green | regression | `pytest tests/test_local_*.py tests/test_web_library_options_no_local.py tests/test_no_raw_storage_access.py -q` | ✅ exists | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/fixtures/local_indexer/single_word_per_line.pdf` — D-F4 regression fixture (CONTEXT.md error: claims it exists but does not)
- [ ] `tests/test_local_pdf_extraction_fallback.py` — D-F4 detection + fallback unit tests
- [ ] `tests/test_local_hit_highlighting.py` — D-F5 hit dict shape + render pipeline tests
- [ ] `tests/test_local_optout_persistence.py` — D-F1 session-JSON round-trip + rescan-preserve tests
- [ ] `tests/test_local_optout_filter.py` — D-F1 filter composition tests
- [ ] `tests/test_result_dialog_local_button_removed.py` — NEW-1 AST guard
- [ ] `tests/test_local_nav_page_chunk.py` — NEW-2 navigation primitive + View-All separator tests
- [ ] Extend `tests/test_local_filter_cascade.py` — assert opt-out filter at both joinpoints
- [ ] Update or delete `tests/test_local_browse_panel.py::test_result_dialog_has_view_in_browse_button` — superseded by NEW-1 removal

*Framework install:* none — pytest already configured per `pyproject.toml`.

---

## Manual-Only Verifications

| Behavior | Feature | Why Manual | Test Instructions |
|----------|---------|------------|-------------------|
| Tri-state checkbox visual rendering in tree widget (all / some / none) | D-F1 | Qt tri-state rendering is widget-only and OS-themed; pixel-level assertion brittle | Smoke: open desktop app → My Library tab → expand a folder → toggle a leaf file → verify parent shows partial-check state |
| Browse panel "View All" button toggles between per-page and all-pages views without losing scroll position | NEW-2 | Scroll-position state interacts with QTextBrowser repaint; hard to mock | Smoke: open a multi-page LOCAL hit → click "הכל" → scroll to middle → click "Per-page" → verify view returns to same chunk |
| Page/chunk separator label correctly labels PDF vs DOCX/TXT in Hebrew context | NEW-2 | RTL rendering nuance | Smoke: search for term hitting both a PDF and a DOCX → open each → verify labels |
| Removed `צפה בדפדוף` button gone from LOCAL ResultDialog but `עיין` Browse button still present | NEW-1 | Widget-tree assertion is approximate; verify the visual change | Smoke: open LOCAL hit dialog → confirm only `עיין` is present (or equivalent) |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (9 items above)
- [ ] No watch-mode flags
- [ ] Feedback latency < 5s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
