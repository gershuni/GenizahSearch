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
> Updated 2026-06-08 (--reviews replan): added `test_invalid_scope_fails_closed` (C4); reworded the
> staleness row (per-run payload + stale-vs-no-index, A2/M2), the D-13 row (default-equality), and the
> cross-parity row (structural, not byte-for-byte for xlsx/docx, C5). All test_comp_corpus_scope.py
> tests are PURE-ENGINE so Wave 2 is deterministically green (C2). Export tests target MODULE-LEVEL
> helpers in shared/export_dossier.py (C1).

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
| COMP-LOC-01 | Lab Mode is decoupled from corpus — both standard & Lab honor the selector (both directions, pure-engine) | unit | `pytest tests/test_comp_corpus_scope.py::test_lab_mode_not_hardwired_to_local -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-02 | `corpus_scope='all'` on standard composition includes LOCAL LAB hits | unit | `pytest tests/test_comp_corpus_scope.py::test_std_comp_all_includes_local_hits -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-02 | Present-but-stale LAB sets the PER-RUN verdict (`result['local_lab_stale']`) + engine flag; no-index reports nothing; no silent drop | unit | `pytest tests/test_comp_corpus_scope.py::test_stale_lab_sets_flag -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-02 / D-12 | Invalid/typo `corpus_scope` fails CLOSED to `genizah` (LOCAL LAB loop never runs) | unit | `pytest tests/test_comp_corpus_scope.py::test_invalid_scope_fails_closed -x` | ❌ W0 | ⬜ pending |
| COMP-LOC-02 | Genizah default path equals the omitted-arg path (D-13 default-equality; full non-regression also guarded by LOCAL-hook-not-called + pre-existing comp suites) | regression | `pytest tests/test_comp_corpus_scope.py::test_genizah_default_nonregression -x` | ❌ W0 | ⬜ pending |
| EXP-F3 | LOCAL hit in composition export carries LOCAL columns (filename/folder/path/page/matched-text) via module-level helper | unit | `pytest tests/test_comp_export_local.py::test_xlsx_local_row_shape -x` | ❌ W0 | ⬜ pending |
| EXP-F3 | All four formats (xlsx/csv/txt/docx) emit a LOCAL-aware path; partition helper splits Genizah vs LOCAL | unit | `pytest tests/test_comp_export_local.py::test_all_formats_local_aware -x` | ❌ W0 | ⬜ pending |
| EXP-F3 | Genizah-only composition export STRUCTURALLY unchanged (partition leaves Genizah rows identical + empty LOCAL set; not byte-for-byte for xlsx/docx) | unit | `pytest tests/test_comp_export_local.py::test_genizah_only_export_unchanged -x` | ❌ W0 | ⬜ pending |
| D-12 | No Supabase/cloud-write calls after a LOCAL composition run (pure-engine) | unit | `pytest tests/test_comp_corpus_scope.py::test_no_cloud_write_on_local_comp -x` | ❌ W0 | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `tests/test_comp_corpus_scope.py` — stubs for COMP-LOC-01/02, D-12, fail-closed scope, Genizah non-regression (ALL pure-engine — no UI import — so Wave 2's whole-file verify is green, C2)
- [ ] `tests/test_comp_export_local.py` — stubs for EXP-F3 targeting the MODULE-LEVEL helpers `_partition_comp_export_rows` / `_build_local_comp_row` in shared/export_dossier.py (C1); structural parity (C5)

**Existing tests to extend (not re-create):**
- `tests/test_corpus_scope_routing.py` — add composition-scope variants
- `tests/test_lab_composition_chunk_hits.py` — add `corpus_scope` param non-regression (also part of the D-13 historical-parity guard: must stay green after Plan 02)

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Corpus selector renders next to the mode/Lab-Mode controls on the composition tab, bilingual (EN/HE) | COMP-LOC-01 | PyQt6 widget placement/label is visual; headless tests can't assert Qt layout reliably | Launch desktop app → Composition tab → confirm Genizah/Local/ALL dropdown present (no "Corpus:" label, mirrors the Search tab). Labels switch HE/EN only after a language change + RESTART (combo built once from CURRENT_LANG; M3) |
| Stale-LAB rebuild/staleness signal is visible to the user (banner/inline note) for a present-but-stale index, and CLEARS after a My-Library rebuild | COMP-LOC-02 | Signal styling/visibility + post-rebuild refresh is a UI affordance | Run a Local/ALL composition with a present-but-stale LAB index → confirm a rebuild/staleness signal appears (not empty results); rebuild in My Library → run again → confirm it clears. A user with NO LOCAL index never sees it |
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
