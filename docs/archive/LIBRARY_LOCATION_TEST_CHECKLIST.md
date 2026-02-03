# Library Location Feature - Testing Checklist

**Date:** 2026-02-03
**Feature Branch:** `claude/plan-library-location-kpaAv`
**Related Plan:** [LIBRARY_LOCATION_PLAN.md](./LIBRARY_LOCATION_PLAN.md)

---

## Pre-Merge Testing Checklist

### 1. Data Verification

#### 1.1 CSV Extraction
- [ ] Run `python scripts/extract_library_codes.py --dry-run` to verify statistics
- [ ] Confirm ~99.65% coverage (only ~751 unmatched out of ~217K)
- [ ] Verify distribution matches expected:
  - CUL: ~127,500 (58.8%)
  - JTS: ~30,200 (13.9%)
  - RNL: ~16,700 (7.7%)
  - Oxford: ~12,800 (5.9%)
  - Manchester: ~11,800 (5.4%)
  - BL: ~7,900 (3.6%)

#### 1.2 Sample Record Verification
Test these specific records:

| sys_id | Expected Library | Call Number Pattern |
|--------|------------------|---------------------|
| `990053727750205171` | Manchester | "The University of Manchester Library" |
| `990053835020205171` | Mosseri | "Moss. III,27O" |
| `990051344900205171` | CUL | "Cambridge University Library" |
| `990053229230205171` | JTS | "Jewish Theological Seminary" |
| `990053464220205171` | Oxford | "Bodleian Libraries" |

```bash
# Verify sample records
grep "990053727750205171" libraries.csv  # Should show: Manchester
grep "990053835020205171" libraries.csv  # Should show: Mosseri
grep "990051344900205171" libraries.csv  # Should show: CUL
```

---

### 2. Core Library (genizah_core.py)

#### 2.1 Constants & Functions
- [ ] `LIBRARY_CODES` dict contains all 29 library codes
- [ ] `get_library_display('CUL', short=True)` returns `'CUL'`
- [ ] `get_library_display('CUL', short=False)` returns `'Cambridge University Library'`
- [ ] `get_library_display('UNKNOWN_CODE', short=False)` returns `'UNKNOWN_CODE'`
- [ ] `get_library_display('', short=False)` returns `''`

#### 2.2 CSV Loading
- [ ] `_load_csv_bank()` correctly reads library_code from column 3
- [ ] Title correctly read from column 7 (shifted from 6)
- [ ] Empty library_code values handled gracefully

#### 2.3 Metadata Retrieval
- [ ] `get_library_for_id('990053727750205171')` returns `'Manchester'`
- [ ] `get_library_for_id('invalid_id')` returns `''`
- [ ] `get_library_for_id(None)` returns `''`

#### 2.4 Display Data
- [ ] `get_display_data()` includes `library_code` key in returned dict

---

### 3. Web Application

#### 3.1 Search Results (web/pages/search.py)
- [ ] Library badge appears before shelfmark in result cards
- [ ] Badge shows abbreviated code (e.g., "CUL")
- [ ] Tooltip shows full library name on hover
- [ ] Badge uses appropriate styling (primary-100 bg, primary-700 text)
- [ ] Empty library_code: no badge displayed

#### 3.2 Advanced View Dialog
- [ ] Library field appears in metadata cards
- [ ] Shows full library name (not abbreviated)
- [ ] Icon: `account_balance`
- [ ] Color: `var(--accent-amber)`

#### 3.3 Viewer Panel (Info Tab)
- [ ] Library field appears at top of info_items list
- [ ] Shows full library name
- [ ] Missing library shows "Not available"

#### 3.4 Browse Page (web/pages/browse.py)
- [ ] Library field appears in metadata panel
- [ ] Uses `col-span-2` for full width
- [ ] Shows full library name
- [ ] Only displays if `page.library_name` is non-empty

#### 3.5 Mobile Responsiveness
- [ ] Library badge visible on mobile (<768px)
- [ ] Badge doesn't overflow or break layout
- [ ] Metadata panel library field readable on mobile

---

### 4. Desktop Application (genizah_app.py)

#### 4.1 Column Constants
- [ ] `COL_LIBRARY = 4` (new)
- [ ] `COL_IMG = 5` (was 4)
- [ ] `COL_TITLE = 6` (was 5)
- [ ] `COL_SNIPPET = 7` (was 6)
- [ ] `COL_SRC = 8` (was 7)
- [ ] Column count: 9

#### 4.2 Results Table
- [ ] Header shows "Library" column
- [ ] Library column width: 90px
- [ ] Library data populates correctly for search results
- [ ] Column alignment correct (LTR for library code)

#### 4.3 Table Filtering
- [ ] Library column included in filter_columns list
- [ ] Filter by library code works correctly

#### 4.4 Sorting
- [ ] Sort by Library column works
- [ ] Image column (now index 5) still non-sortable

---

### 5. Export Functions

#### 5.1 Web Search Results Excel
- [ ] Headers: "Shelfmark", "Library", "Title", "System ID", "Score", "Snippet", "Full Text"
- [ ] Library column width: 15
- [ ] Library values populated correctly
- [ ] Alignment: LTR for library column

#### 5.2 Web List Export Excel
- [ ] Headers include "Library" after "Shelfmark"
- [ ] Library code retrieved via `get_library_code(sys_id)`
- [ ] Column widths correct (C: 15)

#### 5.3 Web Parallels Export Excel
- [ ] Headers include "Library" after "Shelfmark"
- [ ] Library code populated from sys_id extraction
- [ ] Column alignments correct

#### 5.4 Export File Verification
```bash
# After running exports, verify:
# 1. Open Excel file
# 2. Check Library column exists
# 3. Verify sample values (CUL, JTS, Oxford, etc.)
# 4. Check column width and alignment
```

---

### 6. Backward Compatibility

#### 6.1 Old CSV Without library_code Column
- [ ] Application starts without error
- [ ] Search works (library_code empty)
- [ ] Browse works (library_code empty)
- [ ] No crashes when library_code missing

#### 6.2 Old Tantivy Index
- [ ] Application works with existing index
- [ ] Library info from CSV, not index
- [ ] Search results display correctly

---

### 7. Regression Testing

#### 7.1 Existing Functionality
- [ ] Search (exact, variants, fuzzy) works correctly
- [ ] Browse navigation works
- [ ] Lists functionality works
- [ ] Export all formats work
- [ ] Shelfmark display unchanged
- [ ] Title display unchanged

#### 7.2 Performance
- [ ] CSV loading time acceptable (~217K records)
- [ ] Search performance not degraded
- [ ] Memory usage acceptable

---

### 8. Edge Cases

#### 8.1 Empty/Missing Data
- [ ] Record with empty library_code displays correctly
- [ ] Record with empty shelfmark displays correctly
- [ ] Record with empty title displays correctly

#### 8.2 Special Characters
- [ ] Library names with accents display correctly (e.g., "Alliance Israélite")
- [ ] Hebrew characters in titles still work

#### 8.3 Long Library Names
- [ ] Full names don't overflow in metadata panels
- [ ] Tooltips show complete names

---

## Test Execution Log

| Test Section | Tester | Date | Status | Notes |
|--------------|--------|------|--------|-------|
| 1. Data Verification | | | | |
| 2. Core Library | | | | |
| 3. Web Application | | | | |
| 4. Desktop Application | | | | |
| 5. Export Functions | | | | |
| 6. Backward Compatibility | | | | |
| 7. Regression Testing | | | | |
| 8. Edge Cases | | | | |

---

## Sign-Off

- [ ] All tests passed
- [ ] No regressions found
- [ ] Documentation updated
- [ ] Ready for merge

**Tested by:** _________________
**Date:** _________________
**Approved by:** _________________
