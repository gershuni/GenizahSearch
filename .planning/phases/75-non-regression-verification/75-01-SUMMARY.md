---
phase: 75-non-regression-verification
plan: 01
subsystem: testing
tags: [uat, verification, non-regression, decomposition]

# Dependency graph
requires:
  - phase: 71-desktop-smoke-checklist
    provides: docs/desktop-smoke-checklist.md §2 and §4 reused as desktop functional baseline
  - phase: 74-page-scoped-state-refactor
    provides: 74-HUMAN-UAT.md format precedent (YAML frontmatter mirrored in 75-UAT.md)
provides:
  - Pre-populated .planning/phases/75-non-regression-verification/75-UAT.md with 5 fixed test sections
  - Locked sys_ids for 4 fixed test manuscripts (Cambridge, NLI-only Oxford, multi-IE, JTS DPUL)
  - Deterministic UAT checklist ready for Plan 75-02 walkthrough (no authoring left to walkthrough time)
affects: [75-02-walkthrough, 75-VERIFICATION]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - YAML frontmatter UAT format (mirrors Phase 74 74-HUMAN-UAT.md with 75-prefixed naming)
    - Fixed Test Manuscripts table locks sys_ids for reproducible reruns across sessions
    - Desktop sections reference existing Phase 71 checklist by section number (no rewriting)

key-files:
  created:
    - .planning/phases/75-non-regression-verification/75-UAT.md
  modified: []

key-decisions:
  - "Oxford arch. O.d.8/1 chosen for NLI-only test (Oxford in nli_images is not in cambridge_manifests or jts_dpul)"
  - "Multi-IE sys_id 990000412990205171 LOCKED per D-08 and multi_ie_fl_validation.csv row 1"
  - "Desktop tests 3 and 4 reference docs/desktop-smoke-checklist.md §2 and §4 by section number (not rewritten, per D-06)"

patterns-established:
  - "UAT template: YAML frontmatter (status/phase/source/started/updated) + fixed-manuscripts table + 5 tests + Summary + Gaps — reusable for future verification phases"
  - "Two-phase UAT authoring: lock test inputs first (sys_ids), then pre-populate checklist — separation makes reruns identical"

requirements-completed: [NREG-01]

# Metrics
duration: ~10min
completed: 2026-04-17
---

# Phase 75 Plan 01: Pre-populate Non-Regression UAT Summary

**Pre-populated 75-UAT.md with YAML frontmatter, four locked test-manuscript sys_ids, and five deterministic test sections covering web/desktop search + browse responsiveness and the pytest baseline — ready for the walkthrough plan to execute without further authoring.**

## Performance

- **Duration:** ~10 min
- **Started:** 2026-04-17
- **Completed:** 2026-04-17
- **Tasks:** 2
- **Files modified:** 1

## Accomplishments

- Created `.planning/phases/75-non-regression-verification/75-UAT.md` with D-11 YAML frontmatter (status=in-progress, phase=75-non-regression-verification, source=[75-VERIFICATION.md], started/updated=2026-04-17).
- Resolved and locked four fixed test-manuscript sys_ids (D-08):
  - Cambridge T-S 12.123 → `990051334060205171` (CUL, CUDL image load surface)
  - Oxford arch. O.d.8/1 → `990053385670205171` (NLI-only, no Cambridge/JTS crossref)
  - Multi-IE Ms. Heb. 6972=8 → `990000412990205171` (Allony/NLI, 2 IEs, 7 trans FLs — LOCKED from multi_ie_fl_validation.csv)
  - JTS ENA 1052.1 → `990053572370205171` (Princeton DPUL image path, v7.2.3 regression surface)
- Pre-populated 5 test sections with D-07 web checklist verbatim, D-06 desktop responsiveness overlay referencing `docs/desktop-smoke-checklist.md` §2 and §4, and the pytest baseline (1067 passed, 8 skipped per D-18).
- Included Phase 74 D-20 URL-bar regression check in Test 2(e) so the detached-task fix is re-confirmed under real use.
- Emitted Summary block (total=5, pending=5) and empty Gaps placeholder ready for walkthrough triage per D-15.

## Task Commits

Each task was committed atomically (`--no-verify` per parallel worktree policy):

1. **Task 1: Lock fixed test manuscript sys_ids** — `d85d8e74` (docs)
2. **Task 2: Pre-populate UAT test sections (5 tests + Summary + Gaps)** — `c76511af` (docs)

## Files Created/Modified

- `.planning/phases/75-non-regression-verification/75-UAT.md` — NEW (86 lines). Pre-populated UAT checklist per D-11; walkthrough plan (75-02) runs against this file without further authoring.

## Decisions Made

- **NLI-only library pick = Oxford (arch. O.d.8/1):** The plan's default candidate was RNL, but `nli_crossref.db.nli_images` uses the string `'St. Peterburg'` (not `'RNL'`) as LibraryAbbrev, and libraries.csv has no exact matching code for that legacy name. Oxford is the plan's explicit fallback (`LibraryAbbrev='Oxford'` with `row[3]=='Oxford'`), produces a clean one-to-one match, and has no crossref overlap with Cambridge or JTS by library-code construction. Recorded in the Fixed Test Manuscripts table.
- **JTS sys_id resolved cleanly on first attempt:** `ENA 1052.1` → `990053572370205171` appeared unambiguously in libraries.csv (JTS + "ENA 1052.1"). No fallback to jts_dpul iteration needed.
- **nli_crossref.db located in main repo, not worktree:** This is a worktree-mode side effect — data sidecars live at `../../../nli_data/nli_crossref.db` relative to the worktree. Queried via that relative path. Does not affect the committed artifact.

## Deviations from Plan

None — plan executed exactly as written. Task 1's fallback paths (UNRESOLVED annotation) were not triggered because all four sys_ids resolved cleanly on first lookup.

## Issues Encountered

- **Early Write tool call hit main-repo path instead of worktree:** First invocation of `Write` on `C:\Genizahsearch\.planning\phases\75-non-regression-verification\75-UAT.md` landed in the main checkout because the tool resolved the relative-ish path to the project root, not the worktree. Detected via `ls` mismatch, removed the errant main-repo file, and retried with the explicit worktree absolute path `C:\Genizahsearch\.claude\worktrees\agent-ab7745b4\...`. No data lost. Subsequent Edit calls worked correctly against the worktree path.
- No code paths touched; this was pure artifact authoring. Threat model (declared "none — no trust boundaries crossed") held.

## User Setup Required

None — no external service configuration required. This plan is pure artifact authoring (no app changes, no env vars, no secrets).

## Next Phase Readiness

- `75-UAT.md` is fully deterministic — Plan 75-02 walkthrough can execute surface-by-surface against locked sys_ids without any re-resolution.
- No blockers. Plan 75-02 should proceed with the walkthrough script per D-05, leveraging the pre-refactor worktree at `56facc3d` (D-02) only as an A/B fallback when user recall is uncertain on a specific surface.
- Pytest runs LAST in Plan 75-02 per D-18 — do NOT run `pytest tests/` in this plan (confirmed: no pytest invoked here).

## Self-Check: PASSED

- [x] `.planning/phases/75-non-regression-verification/75-UAT.md` exists (86 lines, written to worktree).
- [x] Multi-IE sys_id `990000412990205171` appears literally in 75-UAT.md (count=2: once in fixed-manuscripts table, once in test 2d).
- [x] YAML frontmatter present with required fields (phase, status, source, started, updated).
- [x] 5 test section headings (`### 1` through `### 5`) present.
- [x] `result: pending` appears exactly 5 times.
- [x] `docs/desktop-smoke-checklist.md` referenced exactly 2 times (tests 3 and 4).
- [x] `1067 passed, 8 skipped` present in test 5.
- [x] "URL bar" reference present (Phase 74 D-20 regression check in test 2e).
- [x] Zero unresolved `{SYS_ID_` placeholders.
- [x] Task 1 commit `d85d8e74` exists in git log.
- [x] Task 2 commit `c76511af` exists in git log.

---
*Phase: 75-non-regression-verification*
*Completed: 2026-04-17*
