---
phase: quick
plan: 005
subsystem: codebase-maintenance
tags: [dead-code, vulture, static-analysis, code-quality]
dependency-graph:
  requires: []
  provides: [unused-functions-report]
  affects: [future-cleanup-tasks]
tech-stack:
  added: [vulture]
  patterns: [static-analysis, grep-cross-reference]
key-files:
  created:
    - .planning/quick/005-find-unused-functions/vulture-raw-output.txt
    - .planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md
  modified: []
decisions:
  - id: Q005-01
    decision: "Categorize findings into three confidence levels based on grep cross-referencing"
    rationale: "Prevents accidental removal of framework-registered callbacks (NiceGUI routes, Qt overrides)"
  - id: Q005-02
    decision: "Flag NiceGUI @ui.page() and @app.get() decorated functions as false positives"
    rationale: "These are registered by the web framework and called via HTTP, not direct Python calls"
  - id: Q005-03
    decision: "Keep document_service.py functions used only in tests as 'Possibly Unused'"
    rationale: "Phase 6+ may need these; documented in STATE.md service layer API"
metrics:
  duration: 8 min
  completed: 2026-02-06
---

# Quick Task 005: Find Unused Functions Summary

**One-liner:** vulture + grep cross-reference identifies ~1,800 lines of dead code across 14 files, including 2 entire unused files

## What Was Done

1. **Installed vulture** (Python dead code finder) and ran it against all 16+ source files and the web/ directory with --min-confidence 60
2. **Got 253 raw findings** covering unused functions, methods, classes, imports, variables, and attributes
3. **Cross-referenced each finding** using grep to check for references across the entire codebase (including tests, scripts, dynamic imports, Qt signal connections, NiceGUI callbacks)
4. **Categorized findings** into three confidence levels:
   - **Definitely Unused (34 items):** Zero references outside definition, not framework callbacks
   - **Possibly Unused (18 items):** Only tests, or interface methods for planned features
   - **False Positives (~201 items):** NiceGUI routes, Qt overrides, UI attributes, loop variables

## Key Findings

- **2 entire files are dead code:** `web/pages/viewer.py` (222 lines) and `web/pages/document.py` (276 lines) -- never imported anywhere
- **TextEditorDialog** (218 lines) and **CommunityHubWidget** (131 lines) in corrections_ui.py are never instantiated
- **10+ composition tree methods** in genizah_app.py are unused
- **8+ joins/image helper methods** in genizah_core.py are unused
- **FastAPI leftovers** in auth_state.py (`get_api_base`, `api_call`) from Jan 2026 removal
- **12 interface methods** in corrections_client.py/supabase_corrections_client.py that define an API contract but are never called

## Task Commits

| Task | Name | Commit | Key Files |
|------|------|--------|-----------|
| 1 | Install vulture and run dead code analysis | dd959b1 | vulture-raw-output.txt |
| 2 | Cross-reference and categorize findings | daef528 | 005-UNUSED-FUNCTIONS-REPORT.md |
| 3 | Checkpoint (report ready for review) | -- | Noted: report ready, no user prompt needed |

## Deviations from Plan

None - plan executed exactly as written.

## Decisions Made

1. **Q005-01:** Three-tier confidence categorization based on grep evidence (not just vulture confidence scores)
2. **Q005-02:** NiceGUI routes and FastAPI endpoints treated as framework-registered (false positives)
3. **Q005-03:** document_service.py functions kept as "Possibly Unused" since Phase 6 roadmap references them

## Checkpoint Note

Task 3 was a human-verify checkpoint for reviewing the report. Per execution constraints, the report is ready for review at:
`.planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md`

No code was modified or deleted. The report is information-only.

## Next Steps

1. Review the report and decide which categories to approve for removal
2. Create a follow-up quick task for the actual cleanup (Priority 1: dead files, Priority 2: unused functions)
3. Run tests after removal to verify nothing breaks

## Self-Check: PASSED
