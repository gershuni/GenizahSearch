---
phase: 66-documentation-update
reviewed: 2026-04-15T18:30:00Z
depth: standard
files_reviewed: 3
files_reviewed_list:
  - docs/CODE_INDEX.md
  - docs/OPEN_ISSUES.md
  - docs/guides/DEVELOPER_GUIDE.md
findings:
  critical: 0
  warning: 3
  info: 2
  total: 5
status: issues_found
---

# Phase 66: Code Review Report

**Reviewed:** 2026-04-15T18:30:00Z
**Depth:** standard
**Files Reviewed:** 3
**Status:** issues_found

## Summary

Three documentation files were reviewed for factual accuracy, internal consistency, and correctness of referenced file locations, line numbers, and configuration claims. The CI workflow description and ruff configuration details in DEVELOPER_GUIDE.md are accurate and match the actual `.github/workflows/ci.yml` and `ruff.toml` files. However, the CODE_INDEX.md has significantly stale line numbers for `genizah_app.py`, the OPEN_ISSUES.md summary table does not match the actual count of open items, and the DEVELOPER_GUIDE.md has a stale record count for `libraries.csv`.

## Warnings

### WR-01: CODE_INDEX.md line numbers for genizah_app.py are stale

**File:** `docs/CODE_INDEX.md:9-600+`
**Issue:** The line numbers for `genizah_app.py` entries are systematically off and the drift grows throughout the file. Early entries are off by ~5 lines (e.g., `_setup_crash_handler` listed at line 79, actual line 84; `UpdateNotificationBar` listed at 230, actual 235). By mid-file the drift is hundreds of lines (`ResultDialog` listed at 5795, actual 6045, off by +250). Deep in the file, the drift exceeds 1,000 lines (`GenizahGUI` listed at 11992, actual 12754, off by +762; `create_search_tab` listed at 13942, actual 15439, off by +1,497). This makes the index unreliable for navigating the codebase.
**Fix:** Regenerate the CODE_INDEX.md by re-running whatever script or process was used to create it. If it was manually assembled, consider adding an automated `scripts/generate_code_index.py` that parses `def`/`class` lines from source files.

### WR-02: OPEN_ISSUES.md summary table counts do not match actual open items

**File:** `docs/OPEN_ISSUES.md:47-58`
**Issue:** The Quick Summary table claims P2 Medium Bugs = 12 Open, but counting the actual `Open` markers in the P2 section yields 10 open items. The table also claims Untested Areas = 4 Open, but the actual section has 2 "Not Tested" + 1 "Needs Testing" = 3 open items. This puts the actual total at 21, not 23 as stated. The discrepancy suggests items were resolved without updating the summary, or the recount in the April 15 changelog entry was imprecise.
**Fix:** Recount each section and update the summary table to match. Specifically:
- P2 open: recount and update (appears to be 10, not 12)
- Untested open: recount and update (appears to be 3, not 4)
- Recalculate total accordingly

### WR-03: DEVELOPER_GUIDE.md libraries.csv record count is stale

**File:** `docs/guides/DEVELOPER_GUIDE.md:166`
**Issue:** The project structure description says `libraries.csv` has `~217K records`, but the actual file has ~255K lines. This was expanded in v7.1.0 (March 2026) when 38,673 new manuscript records were added from FIST.db. CLAUDE.md correctly reflects the updated count.
**Fix:** Change line 166 from:
```
├── libraries.csv            # Master manuscript metadata (~217K records)
```
to:
```
├── libraries.csv            # Master manuscript metadata (~255K records)
```

## Info

### IN-01: DEVELOPER_GUIDE.md project structure omits puzzle.py from web pages listing

**File:** `docs/guides/DEVELOPER_GUIDE.md:127`
**Issue:** The `web/pages/` listing in the project structure ends with `download.py` and does not include `puzzle.py`, which is a significant page (the Fragment Puzzle feature). The listing does use `└── ...` ellipsis convention for components, but the pages section lists every file explicitly and then stops, implying completeness.
**Fix:** Add `├── puzzle.py          # Fragment puzzle canvas` before the `download.py` entry.

### IN-02: OPEN_ISSUES.md uses inconsistent status markers

**File:** `docs/OPEN_ISSUES.md:70-72,143,149-155`
**Issue:** Some older entries use Hebrew-prefixed status markers (e.g., the checkmark character in lines 70-72, 143, 149-155) while newer entries use standard emoji markers. This is cosmetic and does not affect comprehension, but the mixed encoding could cause issues with automated parsing of the document.
**Fix:** Standardize all status markers to the emoji format used in newer entries for consistency.

---

_Reviewed: 2026-04-15T18:30:00Z_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
