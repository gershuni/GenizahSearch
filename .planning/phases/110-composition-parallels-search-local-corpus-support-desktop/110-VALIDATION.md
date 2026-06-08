---
phase: 110
slug: composition-parallels-search-local-corpus-support-desktop
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-08
---

# Phase 110 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from 110-RESEARCH.md § Validation Architecture. Task IDs are finalized by the planner.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x |
| **Config file** | pytest.ini (inferred) |
| **Quick run command** | `pytest tests/test_comp_corpus_scope.py tests/test_comp_export_local.py -x --tb=short` |
| **Full suite command** | `pytest tests/ -x --tb=short` |
| **Estimated runtime** | ~quick: a few seconds; full suite: minutes |

---

## Sampling Rate

- **After every task commit:** Run `pytest tests/test_comp_corpus_scope.py tests/test_comp_export_local.py -x --tb=short`
- **After every plan wave:** Run `pytest tests/ -x --tb=short`
- **Before `/gsd-verify-work`:** Full suite must be green
- **Max feedback latency:** ~30 seconds (quick run)

---

## Per-Task Verification Map

> Requirement → behavior → automated command. Task IDs (`110-NN-NN`) are assigned by the planner; each row's test becomes the `<acceptance_criteria>` automated check for the task that implements it.

| Requirement | Behavior | Test Type | Automated Command | File Exists | Status |
|-------------|----------|-----------|-------------------|-------------|--------|
| COMP-LOC-01 | `corpus_scope='genizah'` → Lab-mode composition skips the LOCAL LAB loop | unit | `pytest tests/test_comp_corpus_scope.py::test_lab_comp_genizah_skips_local_lab -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-01 | `corpus_scope='local'` → Lab-mode composition skips the Genizah loop | unit | `pytest tests/test_comp_corpus_scope.py::test_lab_comp_local_skips_genizah_lab -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-01 | `corpus_scope='genizah'` → standard composition skips the LOCAL LAB hook | unit | `pytest tests/test_comp_corpus_scope.py::test_std_comp_genizah_skips_local_lab -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-01 | Lab Mode is decoupled from corpus — both standard & Lab honor the selector | unit | `pytest tests/test_comp_corpus_scope.py::test_lab_mode_not_hardwired_to_local -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-02 | `corpus_scope='all'` on standard composition includes LOCAL LAB hits | unit | `pytest tests/test_comp_corpus_scope.py::test_std_comp_all_includes_local_hits -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-02 | Stale LAB index sets `local_lab_searcher_stale=True` (no silent drop) | unit | `pytest tests/test_comp_corpus_scope.py::test_stale_lab_sets_flag -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-02 | Genizah default path is byte-for-byte unchanged (D-13 non-regression) | regression | `pytest tests/test_comp_corpus_scope.py::test_genizah_default_nonregression -x` | ❌ W0 | ⬜ pending |
| EXP-F3 | LOCAL hit in composition export carries LOCAL columns (filename/folder/path/page/matched-text) | unit | `pytest tests/test_comp_export_local.py::test_xlsx_local_row_shape -x` | ❌ W0 | ⬜ pending |
| EXP-F3 | All four formats (xlsx/csv/txt/docx) emit a LOCAL-aware path | unit | `pytest tests/test_comp_export_local.py::test_all_formats_local_aware -x` | ❌ W0 | ⬜ pending |
| EXP-F3 | Genizah-only composition export unchanged (cross-parity) | unit | `pytest tests/test_comp_export_local.py::test_genizah_only_export_unchanged -x` | ❌ W0 | ⬜ pending |
| D-12 | No Supabase/cloud-write calls after a LOCAL composition run | unit | `pytest tests/test_comp_corpus_scope.py::test_no_cloud_write_on_local_comp -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_comp_corpus_scope.py` — stubs for COMP-LOC-01/02, D-12, Genizah non-regression
- [ ] `tests/test_comp_export_local.py` — stubs for EXP-F3 (LOCAL-hit export shape, all-formats, cross-parity)

**Existing tests to extend (not re-create):**
- `tests/test_corpus_scope_routing.py` — add composition-scope variants
- `tests/test_lab_composition_chunk_hits.py` — add `corpus_scope` param non-regression

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Corpus selector renders next to the mode/Lab-Mode controls on the composition tab, bilingual (EN/HE) | COMP-LOC-01 | PyQt6 widget placement/label is visual; headless tests can't assert Qt layout reliably | Launch desktop app → Composition tab → confirm Genizah/Local/ALL dropdown present, switches HE/EN with the language toggle |
| Stale-LAB rebuild/staleness signal is visible to the user (banner/inline note/toast) | COMP-LOC-02 | Signal styling/visibility is a UI affordance | Run a Local/ALL composition with a stale LAB index → confirm a rebuild/staleness signal appears (not empty results) |
| Parallels-from-browse launch inherits the selected corpus scope | EXP-F3 / RF-6 | End-to-end UI flow across tabs | Browse → "search parallels" → confirm the composition run uses the current selector value |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (`test_comp_corpus_scope.py`, `test_comp_export_local.py`)
- [ ] No watch-mode flags
- [ ] Feedback latency < 30s
- [ ] `nyquist_compliant: true` set in frontmatter

**Approval:** pending
