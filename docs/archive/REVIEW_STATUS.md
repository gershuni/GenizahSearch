# Pre-Launch Review Status

**Date:** 2026-01-29
**Branch:** `claude/pre-launch-checklist-4224e`
**Last Commit:** `42d0d5b` - Fix P2 bugs: exports formatting, shelfmark matching, add-to-list dialog

---

## Completed Tasks (Committed)

### Security Fixes (Commit 111d99b)
- [x] Path traversal protection
- [x] JavaScript injection prevention
- [x] Rate limiting added

### P1 Bug Fixes (Commit d09ec5c)
- [x] #10 List export bug - Fixed
- [x] #15 Comments not showing - Fixed
- [x] #16 Parallels export - Fixed

### P2 Bug Fixes (Commit 42d0d5b)
- [x] #8 Word export RTL - David font, bidi, right alignment
- [x] #9 Excel export - RTL layout, column widths, header styling
- [x] #11 Shelfmark matching - 3-tier matching system
- [x] #13 Add to list dialog - Renamed, inline creation, empty note default

---

## Issues Found During Testing (Need Fixes)

### 1. Word Export Improvements Needed
**File:** `web/api.py` (export_word function)
- [ ] Hebrew manuscript text should be right-aligned (currently not)
- [ ] Filename should be the search string (not generic name)
- [ ] Search term should be highlighted in results
- **Reference:** Check desktop app `genizah_app.py` for logic

### 2. Excel Export Improvements Needed
**File:** `web/api.py` (export_excel function)
- [ ] Replace line breaks with spaces (not wrap text)
- [ ] Highlight search term in cells
- **Reference:** Check desktop app `genizah_app.py` for logic

### 3. Shelfmark Matching Logic Wrong
**File:** `genizah_core.py` (shelfmark matching functions)
- Current behavior: Finds random match when no exact match
- Expected behavior: Show list of suggestions ordered by similarity
- **Reference:** Check desktop app `genizah_app.py` for proper logic

### 4. Add to List Dialog UI Bugs
**File:** `web/components/add_to_list_dialog.py`
- [ ] All list colors appear the same (color not displayed in dropdown)
- [ ] Duplicate buttons showing (Cancel/Add + Back/Create and Add visible together)
- Need to:
  1. Show list colors as colored dots/icons in dropdown
  2. Hide main action row when new_list_container is visible

### 5. List Export Not Working
**File:** `web/pages/lists.py` or `web/api.py`
- Clicking export button gets no response
- Was supposedly fixed in P1 commit but still broken

### 6. Comments Display Issues
**File:** `web/pages/browse.py` (comments section)
- Only author name and date visible, comment text not showing
- Clicking comment icon gets no response
- Was supposedly fixed in P1 commit but still broken

---

## Remaining P3 Tasks (Not Started)

- #12 Loading spinners
- #14 Lists per-user
- #17 Redesign Advanced View

---

## Task List Status

| ID | Task | Status |
|----|------|--------|
| 1-7 | Security & improvements | Done |
| 8 | Word export RTL | Done but needs improvements |
| 9 | Excel export | Done but needs improvements |
| 10 | List export bug | Needs re-testing |
| 11 | Shelfmark matching | Done but logic wrong |
| 12 | Loading spinners | P3 - Not started |
| 13 | Add to list dialog | Done but UI bugs |
| 14 | Lists per-user | P3 - Not started |
| 15 | Comments not showing | Needs re-testing |
| 16 | Parallels export | Done |
| 17 | Redesign Advanced View | P3 - Not started |
| 18 | Word export improvements | Created - Not started |
| 19 | Excel export improvements | Created - Not started |
| 20 | Add to list dialog UI fixes | Created - Not started |

---

## Key Files to Review

1. **Desktop app reference:** `genizah_app.py` - Contains export logic to learn from
2. **Web exports:** `web/api.py` - Word/Excel export functions
3. **Shelfmark matching:** `genizah_core.py` - Normalization and matching
4. **Add to list:** `web/components/add_to_list_dialog.py` - Dialog component
5. **Lists page:** `web/pages/lists.py` - Export and display
6. **Browse page:** `web/pages/browse.py` - Comments functionality

---

## Next Steps for Continuing Agent

1. Read `genizah_app.py` to understand desktop export logic
2. Fix Word export: alignment, filename, highlighting
3. Fix Excel export: line breaks, highlighting
4. Fix shelfmark matching: suggestion list instead of random match
5. Fix add-to-list dialog: colors in dropdown, hide duplicate buttons
6. Re-test list export and comments display
7. Commit all fixes
