# Phase 127 - Plan Check Report

**Checker:** gsd-plan-checker
**Date:** 2026-06-26
**Plans verified:** 127-01, 127-02, 127-03 (3 plans, 3 serialized waves)
**Requirement IDs:** DESK-08, GUARD-02, GUARD-03, GUARD-04

---

## VERDICT: PASS WITH WARNINGS

**Blockers:** 0  |  **Warnings:** 3  |  **Info:** 1

Plans may be executed as-is. The 3 warnings are executor guidance items that do not block
correct execution if the executor follows the read_first and adapt-to-real-method-bodies
instructions already present in the plans.

---

## Dimension 1: Requirement Coverage

| Requirement | Plans Covering | Status |
|-------------|---------------|--------|
| DESK-08 | 127-01 (Task 2), 127-02 (Task 1) | COVERED |
| GUARD-02 | 127-02 (Task 2), 127-03 (Task 2) | COVERED |
| GUARD-03 | 127-03 (Task 1) | COVERED |
| GUARD-04 | 127-01 (Task 1), 127-02, 127-03 | COVERED |

All 4 requirement IDs appear in at least one plan frontmatter requirements field.

SC#1-5 traceability:

| SC# | What must be true | Covered by |
|-----|-------------------|------------|
| SC#1 | desktop/update_ui.py + 4 classes + behavioral tests | 127-01 Task 2 + 127-02 Task 1 |
| SC#2 | D1 shims retired (noqa suffix removed, callers retargeted) | 127-03 Task 1 |
| SC#3 | genizah_core permanent facade confirmed | 127-01 Task 1 |
| SC#4 | Both back-edge guards green | 127-01 Task 1 + 127-03 Task 2 |
| SC#5 | Full suite green (bulk + gui) | 127-03 Task 2 |

SC#1-5: all covered. PASS.

---

## Dimensions 2-12 Summary

**Dimension 2 (Task Completeness):** All 6 tasks have files, action, verify with automated commands, and acceptance_criteria/done fields. All auto type. No missing required fields. PASS.

**Dimension 3 (Dependency Correctness):** Linear chain 127-01->127-02->127-03. No cycles, no missing refs, no forward refs. Serialization correct -- all plans touch genizah_app.py shim block. PASS.

**Dimension 4 (Key Links Planned):** Critical link genizah_app.UpdateNotificationBar is desktop.update_ui.UpdateNotificationBar is in 127-02 must_haves.truths and verified by inline Python identity assertion in Task 1 automated command. PASS.

**Dimension 5 (Scope Sanity):** 127-01: 2 tasks / 4 files. 127-02: 2 tasks / 2 files. 127-03: 2 tasks / 3 files. All within 2-3 task target. PASS.

**Dimension 6 (Verification Derivation):** All must_haves.truths are user-observable at module/test level. Non-trivial, testable, correctly scoped. PASS.

**Dimension 7 (Context Compliance):** All locked decisions covered. Deferred ideas excluded. No scope creep. PASS.

**Dimension 7b (Scope Reduction Detection):** No scope reduction language found. MOVE-and-shim is a full MOVE. PASS.

**Dimension 7c (Architectural Tier Compliance):** All tasks assign capabilities to correct tiers per RESEARCH.md Responsibility Map. No tier mismatches. PASS.

**Dimension 8 (Nyquist Compliance):** VALIDATION.md exists. All checks pass (8e, 8a, 8b, 8c, 8d). PASS.

**Dimension 9 (Cross-Plan Data Contracts):** Non-conflicting edits on genizah_app.py; correct retarget sequencing. PASS.

**Dimension 10 (CLAUDE.md Compliance):** No violations. Per-file ruff, pytest, zero behavior change. PASS.

**Dimension 11 (Research Resolution):** 2 Open Questions, both resolved in plans. PASS.

**Dimension 12 (Pattern Compliance):** PATTERNS.md exists. All 6 files have analogs, all referenced in read_first. PASS.

---

## Specific Concern Adjudication

### Concern 1: ORDERING -- test_no_back_edges_desktop.py green at HEAD before desktop/update_ui.py exists

VERDICT: NO GAP. Option (a): skip-until-exists. Plan 127-01 Task 1 explicitly states to pre-register desktop/update_ui.py with the skip-until-exists branch verbatim, so that module SKIPS until Plan 02 creates it. The acceptance_criteria confirms: desktop/update_ui.py is SKIPPED (not yet created). Same proven pattern as Phase 125. Plan 01 green-at-HEAD claim is VALID.

### Concern 2: test_genizah_core_facade.py green at HEAD

VERDICT: VALID. Phases 122-125 complete (ROADMAP shows Complete). All shimmed names live at HEAD. The test is additive documentation of an already-true state. Green at HEAD confirmed.

### Concern 3: test_update_ui_coordination.py green at HEAD

VERDICT: VALID with WARNING-2. The three coordination methods exist at lines 24416-24452 (verified live). NOT being moved. Tests will be green provided QMessageBox.information and SidecarDownloadThread are correctly patched for the queue-advance and empty-queue test cases. Plan correctly says adapt assertions to real method bodies.

### Concern 4: D1 shim retirement = noqa-SUFFIX removal, NOT line deletion

VERDICT: CORRECTLY PLANNED. Plan 127-03 is unambiguous: remove ONLY the noqa suffix ... The imports STAY. PATTERNS.md shows exact before/after text. Pitfall 1 (line deletion) is explicitly named and guarded in all three artifacts. ruff check genizah_app.py is the correct gate. NO BLOCKER.

### Concern 5: New update_ui shim noqa determination

VERDICT: CORRECTLY PLANNED. Plan 127-02 must_haves.truths is explicit: shim line carries NO noqa: F401 because all 4 classes are used by genizah_app.py at lines 2263, 2269, 24464, 24476. Pitfall 2 named and guarded. No issue.

### Concern 6: Scope fence compliance

VERDICT: FULLY RESPECTED. No plan touches D2-D5 panels, composition tab, genizah_core facade removal, web code, or test_tabular_builder_rtl.py. The last is explicitly listed DO NOT TOUCH in Plan 127-03 interfaces.

---

## Issues

**WARNING-1** [127-01, task 1, task_completeness]:
The interfaces block states 13 shared modules / 20 names but the full name-list across
13 modules totals 25+ individual symbols (shared.metadata_manager has 4; shared.responsa
has 5; shared.browse_map_utils has 5; shared.text_normalize has 4). RESEARCH.md Directive
#5 table is the authoritative source. If the executor stops at 20 assertions instead of
covering all names in the table, the facade contract will be under-tested.
Fix: Executor should count from RESEARCH.md Directive #5 table (all names in every row),
not the 20 names shorthand in the interfaces block. No plan revision strictly required.

**WARNING-2** [127-01, task 2, task_completeness]:
_download_next_sidecar calls QMessageBox.information(self, ...) in the empty-queue branch
(verified at genizah_app.py line 24434). A GenizahGUI.__new__() bare object is not a real
QWidget; this raises a PyQt6 TypeError unless QMessageBox.information is patched or the
test avoids that branch. The plan says adapt assertions to real method bodies (correct) but
does not explicitly name QMessageBox.information as a required patch target alongside
reset_catalog_filter_sets and the three sidecar service resets.
Fix: For the empty-queue branch test, patch genizah_app.QMessageBox.information (or mock
genizah_app.QMessageBox). Alternatively, focus _download_next_sidecar test on the non-empty-
queue branch (queue pops + SidecarDownloadThread fires) and handle the empty-queue case via
the _on_sidecar_download_finished delegation test. The plan is correct in spirit.

**WARNING-3** [127-02, task 1, task_completeness]:
RESEARCH.md Assumption A1 confirms UpdateDownloaderThread is LAZY inside
UpdateProgressDialog.start_download (not module-level). However, the RESEARCH.md Code
Examples section shows a module-level import of both SidecarDownloadThread and
UpdateDownloaderThread -- contradicting A1 for UpdateDownloaderThread. Additionally,
SidecarDownloadThread is used in _download_next_sidecar (line 24442) -- but that method
STAYS on GenizahGUI and is NOT in any of the 4 moved class bodies. If SidecarDownloadThread
does not appear in the 4 class bodies, a module-level import in desktop/update_ui.py
triggers ruff F401. The misleading code example may cause the executor to add unnecessary
imports.
Fix: Before writing desktop/update_ui.py, re-read the bodies of all 4 classes (lines
184-~597) to verify whether SidecarDownloadThread or UpdateDownloaderThread actually
appears in them. The plan already says DERIVE the exact import set from the ACTUAL moved
class bodies -- follow this, not the code example. ruff F401 is the definitive gate.

**INFO-1** [phase-level, scope_sanity]:
VALIDATION.md uses Wave 0 naming for the test-scaffold plan while plan frontmatter uses
wave: 1. This naming inconsistency has zero execution impact -- both agree 127-01 runs first.
Fix: Documentation alignment only. No action required.

---

## Final Coverage Summary

| Requirement | Plans | Status |
|-------------|-------|--------|
| DESK-08 | 127-01, 127-02 | Covered |
| GUARD-02 | 127-02, 127-03 | Covered |
| GUARD-03 | 127-03 | Covered |
| GUARD-04 | 127-01, 127-02, 127-03 | Covered |

| Plan | Tasks | Files | Wave | Status |
|------|-------|-------|------|--------|
| 127-01 | 2 | 4 | 1 | Valid |
| 127-02 | 2 | 2 | 2 | Valid |
| 127-03 | 2 | 3 | 3 | Valid |

All SC#1-5 covered. **0 blockers. 3 executor-guidance warnings. 1 info.**

**Proceed with /gsd-execute-phase 127.**
