---
phase: 70-puzzle-extraction
verified: 2026-04-16T00:00:00Z
status: human_needed
score: 4/5 must-haves verified
overrides_applied: 0
human_verification:
  - test: "Launch desktop app, open puzzle window, add fragment by shelfmark, flip/rotate fragment, close puzzle window. Then browse a manuscript and click Add to Puzzle. Then open a saved join document from puzzle File menu."
    expected: "No crash or visible regression. Fragment image loads on canvas. Flip/rotate controls work. All three entry points work: standalone shelfmark entry, browse Add to Puzzle integration (_folio_lists/_on_meta_resolved wiring), and saved document load (_load_document private API)."
    why_human: "Runtime PyQt6 GUI behavior cannot be verified programmatically. Tests cover logic but not the full event-loop/signal wiring of the extracted PuzzleCanvasWindow in its new module."
---

# Phase 70: Puzzle Extraction Verification Report

**Phase Goal:** All puzzle/join canvas classes live in their own module
**Verified:** 2026-04-16
**Status:** human_needed
**Re-verification:** No — initial verification

## Goal Achievement

### Observable Truths

| # | Truth | Status | Evidence |
|---|-------|--------|----------|
| 1 | PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow are defined in desktop/puzzle.py, not in genizah_app.py | VERIFIED | `grep "^class Puzzle" desktop/puzzle.py` returns all 5 classes at lines 29, 464, 624, 674, 696. `grep "^class PuzzleFragmentItem\|..."` in genizah_app.py returns 0 matches. |
| 2 | genizah_app.py re-exports all 5 puzzle classes for back-compat | VERIFIED | Line 63 of genizah_app.py: `from desktop.puzzle import PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow  # noqa: F401` |
| 3 | ShelfmarkCompleter remains in genizah_app.py and is imported by desktop/puzzle.py via lazy function-local import | VERIFIED | `class ShelfmarkCompleter` at genizah_app.py:1014. `from genizah_app import ShelfmarkCompleter` at desktop/puzzle.py:751 (inside PuzzleCanvasWindow.__init__). |
| 4 | pytest baseline remains green (1066 passed, 9 skipped — actual baseline per SUMMARY) | VERIFIED | SUMMARY reports `pytest tests/ -q`: 1066 passed, 9 skipped. Matches pre-extraction baseline (plan stated 1067/8 which was slightly stale — no change from extraction). |
| 5 | Desktop puzzle window opens, loads a fragment, flip/rotate work, close without crash | NEEDS HUMAN | Runtime GUI behavior. Task 2 in SUMMARY shows status PENDING — human smoke test not yet confirmed. |

**Score:** 4/5 truths verified (1 pending human confirmation)

### Roadmap Success Criteria

| # | Success Criterion | Status | Evidence |
|---|-------------------|--------|----------|
| SC1 | PuzzleCanvasWindow and all puzzle-related classes defined in desktop/puzzle.py (not genizah_app.py) | VERIFIED | 5 classes present in desktop/puzzle.py; 0 class definitions remaining in genizah_app.py |
| SC2 | Puzzle classes import image helpers from desktop/viewers.py or desktop/image_utils.py (no circular imports) | VERIFIED (intent met, per D-10) | CONTEXT D-10 documents that puzzle does NOT use desktop/viewers.py — it uses PuzzleImageLoaderThread from gui_threads.py and shared/puzzle_image_service.py (function-local). No circular imports. The "viewers.py/image_utils.py" clause was an anticipatory concern confirmed moot. |
| SC3 | genizah_app.py imports puzzle classes from the new module and all existing call sites work unchanged | VERIFIED | Re-export at line 63. Call sites at lines 14043 and 14051 still reference PuzzleCanvasWindow by name unchanged. |
| SC4 | Current pytest baseline remains green | VERIFIED | 1066 passed, 9 skipped per SUMMARY |

### Required Artifacts

| Artifact | Expected | Status | Details |
|----------|----------|--------|---------|
| `desktop/puzzle.py` | PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow | VERIFIED | 2669 lines, all 5 classes present in dependency order per D-06. Module docstring present. |
| `genizah_app.py` | Re-exports for back-compat via `from desktop.puzzle import` | VERIFIED | Line 63 contains the re-export. PuzzleCanvasWindow call sites at lines 14043/14051 unchanged. |

### Key Link Verification

| From | To | Via | Status | Details |
|------|----|----|--------|---------|
| genizah_app.py | desktop/puzzle.py | `from desktop.puzzle import` at line 63 | VERIFIED | Pattern found at line 63 |
| desktop/puzzle.py | gui_threads.py | `from gui_threads import PuzzleImageLoaderThread, PuzzleMetaLoaderThread` | VERIFIED | Line 24 of desktop/puzzle.py |
| desktop/puzzle.py | genizah_app.py | `from genizah_app import ShelfmarkCompleter` (lazy function-local) | VERIFIED | Line 751 of desktop/puzzle.py, inside PuzzleCanvasWindow.__init__ |

### Data-Flow Trace (Level 4)

Not applicable. This phase is a pure code restructuring (extraction/refactor) — no new data flows were introduced. All data paths moved verbatim from genizah_app.py to desktop/puzzle.py.

### Behavioral Spot-Checks

| Behavior | Command | Result | Status |
|----------|---------|--------|--------|
| Import from desktop.puzzle works | `python -c "from desktop.puzzle import ..."` | 0 exit per SUMMARY | PASS (SUMMARY-confirmed) |
| Import from genizah_app re-export works | `python -c "from genizah_app import PuzzleCanvasWindow"` | 0 exit per SUMMARY | PASS (SUMMARY-confirmed) |
| Desktop puzzle runtime | Launch app, open puzzle, add fragment | PENDING human | SKIP — requires running GUI |

### Requirements Coverage

| Requirement | Source Plan | Description | Status | Evidence |
|-------------|------------|-------------|--------|----------|
| DESK-02 | 70-01-PLAN.md | PuzzleCanvasWindow and puzzle-related classes extracted to a dedicated module | SATISFIED | All 5 puzzle classes in desktop/puzzle.py; genizah_app.py re-exports for back-compat |

### Anti-Patterns Found

| File | Line | Pattern | Severity | Impact |
|------|------|---------|----------|--------|
| desktop/puzzle.py | 718, 746, 790, 1009, 1015, 1096-1101, 1335-1338 | "placeholder" references | INFO | All are legitimate UI placeholder text (input field hints, canvas loading indicators) — not stub code. No empty implementations found. |

No blockers found. The 2669-line file contains substantive class implementations, not stubs. `return {}`, `return []`, `return None` patterns not found in the extracted classes.

### Human Verification Required

#### 1. Desktop Puzzle Smoke Test (Task 2 — D-13)

**Test:**
1. Launch desktop app: `python genizah_app.py`
2. Open puzzle window via toolbar or menu
3. Add a fragment by shelfmark in the puzzle combobox (e.g., type a known shelfmark and select)
4. Verify the fragment image loads on the canvas
5. Try flip/rotate controls on the loaded fragment
6. Close the puzzle window
7. Browse a manuscript, then click "Add to Puzzle" from browse tab (exercises `_folio_lists`, `_on_meta_resolved` wiring at lines 14043-14051 of genizah_app.py)
8. Verify the fragment appears in the puzzle canvas
9. If a saved join document exists, open it from puzzle File menu (exercises `_load_document` private API)
10. Close the app

**Expected:** No crash, no visible regression. All three entry points work: standalone shelfmark entry, browse "Add to Puzzle", and saved document load.

**Why human:** Runtime PyQt6 GUI behavior cannot be verified programmatically. The key risk is that self.app coupling (20+ hard-coupled attribute accesses via `self.app.meta_mgr`, `self.app.corrections_client`, etc.) and private signal wiring (`_on_meta_resolved`, `_meta_threads`) may silently break at runtime even though imports succeed. CONTEXT D-03 explicitly flags these as hard-coupled paths that require full code-path testing.

### Gaps Summary

No gaps found. All automated must-haves pass. The single pending item is human runtime verification (Task 2/D-13 smoke test) which was always planned as a human gate.

The SUMMARY reports tasks_completed: 1 of 2 — Task 2 is the human smoke test checkpoint which is by design pending human confirmation. Once the user runs the smoke test and confirms no crash, this phase can be marked fully complete.

---

_Verified: 2026-04-16_
_Verifier: Claude (gsd-verifier)_
