# Library Location Implementation Plan

**Date:** 2026-02-03
**Status:** Planning
**Priority:** High

---

## Executive Summary

Add library/holding institution information to manuscript records throughout GenizahSearch. This critical metadata is currently missing from the UI despite being partially available in the source data. The implementation will extract library codes from the existing `call_numbers` field in `libraries.csv`, add a new `library_code` column, and display this information across both web and desktop applications.

---

## Objectives

### Primary Goals
1. **Data Enrichment**: Add `library_code` column to `libraries.csv` mapping each manuscript to its holding institution
2. **User Visibility**: Display library information in search results, browse pages, and manuscript viewers
3. **Export Support**: Include library data in all export formats (Excel, CSV, Word, Text)
4. **Search Enhancement**: Enable library-based filtering via Tantivy index (with backward compatibility)

### Success Criteria
- [ ] All ~217,000 records have a `library_code` value
- [ ] Library displays correctly in web search results (abbreviated) and browse pages (full name)
- [ ] Library column appears in desktop search results and joins tables
- [ ] All export formats include library information
- [ ] Existing users without rebuilt index can still use the application

---

## Current State Analysis

### Data Source: `libraries.csv`

**Location:** `/home/user/GenizahSearch/libraries.csv`

**Current Structure:**
```csv
system_number,oxford_part_id,call_numbers,,,,titles_non_placeholder
```

**Column 3 (`call_numbers`)** contains pipe-separated shelfmark variants. Library names appear in various positions within these variants:

| Pattern | Example | Library |
|---------|---------|---------|
| Library at end after `\|` | `Ms. B 4889 \| The University of Manchester Library Ms. B 4889` | Manchester |
| Library at start | `Cambridge University Library Ms. T-S AS 33.24 \| Ms. T-S AS 33.24` | Cambridge |
| Library in middle | `ENA 3939.4 \| Ms. ENA 3939.4 \| The Jewish Theological Seminary of America Ms. ENA 3939.4` | JTS |
| No explicit library | `Moss. III,27O \| Mosseri, Jacques Ms. III 27O` | Mosseri (collection) |

### Record Distribution by Library

| Library | Records | Has Explicit Name |
|---------|---------|-------------------|
| Cambridge University Library | ~128,112 | Yes |
| The Jewish Theological Seminary of America | ~30,327 | Yes |
| The National Library of Russia | ~18,031 | Yes |
| The Bodleian Libraries, University of Oxford | ~12,433 | Yes |
| The University of Manchester Library | ~11,902 | Yes |
| The British Library | ~7,989 | Yes |
| Mosseri Collection | ~3,163 | No (collection prefix only) |
| Alliance Israélite Universelle | ~3,333 | Yes |
| Westminster College | ~1,666 | Yes |
| Gaster Collection | ~732 | No (collection prefix only) |
| Katz Center | ~374 | Partial |
| Freer Gallery | ~101 | Yes |
| Other small collections | ~500+ | Various |

### Existing Infrastructure

**MARC Integration (`genizah_core.py:3078-3080`):**
```python
elif tag == '710': # Current Owner (Library Name)
    val = get_sub('a')
    if val: result['current_owner'] = val
```

**ResultDialog already displays library from MARC (`genizah_app.py:3632-3633`):**
```python
library = marc.get('current_owner')
if library: shelf = f"{library} | {shelf}"
```

This implementation extends this to work offline using CSV data.

---

## Library Code Mapping

### Standard Libraries

| Code | Full Name | Detection Pattern |
|------|-----------|-------------------|
| `CUL` | Cambridge University Library | `Cambridge University Library` in call_numbers |
| `JTS` | The Jewish Theological Seminary of America | `Jewish Theological Seminary` in call_numbers |
| `RNL` | The National Library of Russia | `National Library of Russia` in call_numbers |
| `Oxford` | The Bodleian Libraries, University of Oxford | `Bodleian Libraries` in call_numbers |
| `Manchester` | The University of Manchester Library | `University of Manchester` in call_numbers |
| `BL` | The British Library | `British Library` in call_numbers |
| `AIU` | Alliance Israélite Universelle | `Alliance Israélite` in call_numbers |
| `Westminster` | Westminster College | `Westminster College` in call_numbers |
| `Freer` | Freer Gallery of Art | `Freer Gallery` in call_numbers |

### Collection-Based (No Explicit Library Name)

| Code | Collection Name | Detection Pattern |
|------|-----------------|-------------------|
| `Mosseri` | Mosseri Collection | Starts with `Moss.` or `Mosseri` |
| `Gaster` | Gaster Collection | Starts with `Gaster` |
| `Katz` | Katz Center | `Katz Center` or `Herbert D. Katz` |
| `Halper` | Halper Catalogue | `Halper` or `Catalogue Halper` |

### Small Collections (Use Name As-Is)

Collections with <50 records will use their collection name as the code:
- `Schoeyen`, `Harkavy`, `Combs`, `Lehnardt`, `Allony`, `Boesky`, `Bisno`
- `Institute of France`, `Hungarian Academy`, `Academy of Sciences`

---

## Implementation Phases

### Phase 1: Data Extraction Script

**Goal:** Create interactive script to extract and validate library mappings

**New File:** `scripts/extract_library_codes.py`

**Functionality:**
1. Parse `libraries.csv` and analyze `call_numbers` patterns
2. Apply detection rules to identify library for each record
3. Display statistics and unmatched records for manual review
4. Generate updated CSV with `library_code` column
5. Interactive mode: prompt user to confirm/correct ambiguous mappings

**Output:** Updated `libraries.csv` with new column structure:
```csv
system_number,oxford_part_id,call_numbers,library_code,,,titles_non_placeholder
```

---

### Phase 2: Core Data Layer Updates

**File:** `genizah_core.py`

#### 2.1 Library Code Constants

**Location:** After line ~150 (with other constants)

```python
# Library code mappings
LIBRARY_CODES = {
    'CUL': 'Cambridge University Library',
    'JTS': 'The Jewish Theological Seminary of America',
    'RNL': 'The National Library of Russia',
    'Oxford': 'The Bodleian Libraries, University of Oxford',
    'Manchester': 'The University of Manchester Library',
    'BL': 'The British Library',
    'AIU': 'Alliance Israélite Universelle',
    'Westminster': 'Westminster College',
    'Freer': 'Freer Gallery of Art',
    'Mosseri': 'Mosseri Collection',
    'Gaster': 'Gaster Collection',
    'Katz': 'Katz Center',
    'Halper': 'Halper Catalogue',
}

def get_library_display(code: str, short: bool = True) -> str:
    """Return library name for display.

    Args:
        code: Library code (e.g., 'CUL', 'JTS')
        short: If True, return code; if False, return full name

    Returns:
        Display string for the library
    """
    if short:
        return code
    return LIBRARY_CODES.get(code, code)
```

#### 2.2 CSV Bank Loading

**Location:** `MetadataManager._load_csv_bank()` (lines 2689-2736)

**Current:**
```python
self.csv_bank[sys_id] = {
    'shelfmark': shelf,
    'title': title,
    'oxford_part_id': oxford_part_id,
}
```

**Updated:**
```python
self.csv_bank[sys_id] = {
    'shelfmark': shelf,
    'title': title,
    'oxford_part_id': oxford_part_id,
    'library_code': library_code,  # NEW: from column 3
}
```

#### 2.3 Display Data

**Location:** `get_display_data()` (lines 3690-3702)

**Add to returned dictionary:**
```python
'library_code': csv_entry.get('library_code', '')
```

#### 2.4 Metadata Retrieval

**Location:** `get_meta_for_id()` (lines 2738-2773)

**Update return value** to include library_code, or add new method:
```python
def get_library_for_id(self, sys_id: str) -> str:
    """Get library code for a system ID."""
    entry = self.csv_bank.get(sys_id, {})
    return entry.get('library_code', '')
```

---

### Phase 3: Web Application Updates

#### 3.1 Data Models

**File:** `web/services.py`

**SearchResult (lines 32-44):**
```python
@dataclass
class SearchResult:
    uid: str
    sys_id: str
    display: Dict[str, str]  # Now includes 'library_code'
    snippet: str
    raw_header: str
    source: str
    full_text: str
    highlight_pattern: Optional[str]
    library_code: str = ''  # NEW: for convenience
```

**BrowsePage (lines 47-68):**
```python
@dataclass
class BrowsePage:
    # ... existing fields ...
    library_code: str = ''      # NEW
    library_name: str = ''      # NEW: full name for display
```

**ManuscriptInfo (lines 81-89):**
```python
@dataclass
class ManuscriptInfo:
    sys_id: str
    shelfmark: str
    title: str
    oxford_part_id: Optional[str] = None
    library_code: str = ''      # NEW
```

#### 3.2 Search Results Display

**File:** `web/pages/search.py`

**Location:** Result card rendering (lines 959-1000)

**Current display:**
```
T-S 12.123
Some manuscript title...
```

**New display:**
```
[CUL] T-S 12.123     ← Abbreviation with tooltip showing full name
Some manuscript title...
```

**Implementation:**
- Add styled badge/label before shelfmark
- Tooltip shows full library name
- Use `get_library_display(code, short=True)` for badge
- Use `get_library_display(code, short=False)` for tooltip

#### 3.3 Browse Page Display

**File:** `web/pages/browse.py`

**Location:** Header section (lines 1446, 1551)

**Current:**
```
T-S 12.123
```

**New:**
```
Cambridge University Library T-S 12.123
```

**Metadata panel addition:**
```
Library: Cambridge University Library
```

---

### Phase 4: Desktop Application Updates

**File:** `genizah_app.py`

#### 4.1 Search Results Table

**Location:** `_create_results_table()` or similar (search for `QTableWidget` setup)

**Add column:** "Library" between existing columns

**Column order suggestion:**
```
# | Shelfmark | Library | Title | Image | Source | Snippet
```

#### 4.2 Joins/Parallels Results Table

**Location:** Search for joins results display

**Add column:** "Library" to show which library each parallel is from

#### 4.3 ResultDialog Updates

**Location:** `ResultDialog.on_metadata_loaded()` (line 3632)

**Current logic:**
```python
library = marc.get('current_owner')
if library: shelf = f"{library} | {shelf}"
```

**New logic (CSV first, MARC fallback):**
```python
# Try CSV first (faster, always available)
library = self.meta_mgr.get_library_for_id(self.current_sys_id)
if not library:
    # Fallback to MARC if available
    library = marc.get('current_owner')

if library:
    library_display = get_library_display(library, short=False)
    shelf = f"{library_display} | {shelf}"
```

---

### Phase 5: Export Updates

#### 5.1 Web Export Service

**File:** `web/export_service.py`

**Excel Export (lines 322-385):**
```python
# Current headers
headers = ["Shelfmark", "Title", "System ID", "Score", "Snippet", "Full Text"]

# New headers
headers = ["Shelfmark", "Library", "Title", "System ID", "Score", "Snippet", "Full Text"]
```

**Word Export:**
Add "Library: {library_name}" field to each result block

**List Export (lines 433-490):**
```python
# Add Library column
headers = ["#", "Shelfmark", "Library", "Title", "System ID", "FL ID", "Notes", "Added"]
```

#### 5.2 Desktop Export Functions

**File:** `genizah_app.py`

**Search Results Excel (line 11639+):**
```python
# Current
headers = [tr("System ID"), tr("Shelfmark"), tr("Title"), tr("Image/Page"), tr("Source"), tr("Snippet")]

# New
headers = [tr("System ID"), tr("Shelfmark"), tr("Library"), tr("Title"), tr("Image/Page"), tr("Source"), tr("Snippet")]
```

**Lists Excel (lines 8810-8848):**
Add Library column

**Word/Text exports:**
Add Library line to each result

---

### Phase 6: Tantivy Index Enhancement (Optional)

**File:** `genizah_core.py`

#### 6.1 Schema Update

**Location:** Index schema definition

**Add field:**
```python
schema_builder.add_text_field("library", stored=True)
```

#### 6.2 Backward Compatibility

**At index load time:**
```python
def _check_index_has_library_field(self) -> bool:
    """Check if index has library field (for backward compatibility)."""
    try:
        schema = self.index.schema()
        return schema.has_field("library")
    except:
        return False
```

**At search time:**
```python
if self._has_library_field:
    # Use indexed library
    library = doc.get("library")
else:
    # Fall back to CSV lookup
    library = self.meta_mgr.get_library_for_id(sys_id)
```

#### 6.3 Index Building

**Location:** Document indexing code

**Add library to document:**
```python
doc.add_text("library", library_code)
```

---

## File Change Summary

| File | Changes | Risk |
|------|---------|------|
| `libraries.csv` | Add `library_code` column | Low - additive |
| `scripts/extract_library_codes.py` | New file | None |
| `genizah_core.py` | Constants, csv_bank, get_display_data, new methods | Medium |
| `web/services.py` | Add fields to dataclasses | Low |
| `web/pages/search.py` | Display library badge | Low |
| `web/pages/browse.py` | Display full library name | Low |
| `web/export_service.py` | Add library column | Low |
| `genizah_app.py` | Table columns, ResultDialog, exports | Medium |

---

## Testing Plan

### Unit Tests

1. **Library Code Detection**
   - Test each library pattern detection
   - Test edge cases (empty call_numbers, unusual formats)
   - Test small collections fallback

2. **CSV Loading**
   - Verify library_code loads correctly
   - Test missing column (backward compatibility)
   - Test empty values

3. **Display Functions**
   - `get_library_display(code, short=True)` returns code
   - `get_library_display(code, short=False)` returns full name
   - Unknown codes return code as-is

### Integration Tests

1. **Web Application**
   - Search results show library badge
   - Browse page shows full library name
   - Metadata panel includes library field
   - Export includes library column

2. **Desktop Application**
   - Search results table has Library column
   - Joins results have Library column
   - ResultDialog shows library
   - All export formats include library

### Manual Testing Checklist

- [ ] Run extraction script and verify output
- [ ] Verify all ~217,000 records have library_code
- [ ] Check display in web search results
- [ ] Check display in web browse page
- [ ] Check display in desktop search results
- [ ] Check display in desktop ResultDialog
- [ ] Export search results to Excel - verify Library column
- [ ] Export list to Excel - verify Library column
- [ ] Export to Word - verify Library field
- [ ] Test with old index (no library field) - should work

### Sample Records for Testing

| sys_id | Expected Library | Notes |
|--------|------------------|-------|
| `990053727750205171` | Manchester | Has explicit name |
| `990053835020205171` | Mosseri | Collection prefix only |
| `990000085850205171` | CUL | T-S shelfmark |
| (JTS record) | JTS | ENA shelfmark |
| (Bodleian record) | Oxford | MS heb shelfmark |

---

## Rollback Plan

If issues arise:

1. **CSV**: Keep backup of original `libraries.csv` before modification
2. **Code**: All changes are in separate commits, can revert individually
3. **Index**: Backward compatibility means old index still works

---

## Documentation Updates Required

After implementation, update:

| Document | Update |
|----------|--------|
| `CLAUDE.md` | Add library_code to CSV structure description |
| `docs/CODE_INDEX.md` | Add new script, update file descriptions |
| `CHANGELOG.md` | Add feature entry |
| `docs/plans/PLANS_INDEX.md` | Add link to this plan |

---

## Timeline Estimate

| Phase | Description | Complexity |
|-------|-------------|------------|
| 1 | Extraction script | Medium |
| 2 | Core data layer | Medium |
| 3 | Web application | Low-Medium |
| 4 | Desktop application | Medium |
| 5 | Export updates | Low |
| 6 | Tantivy index | Low |
| - | Testing | Medium |

---

## Open Questions

1. ~~Library abbreviations~~ → Resolved (see mapping table)
2. ~~Priority: CSV vs MARC~~ → CSV first
3. ~~Small collections handling~~ → Use name as-is
4. Should library be searchable as free text or only filterable?
5. Should we add library-based grouping to search results?

---

## Appendix: Code Locations Quick Reference

### genizah_core.py
- Line 2689-2736: `_load_csv_bank()` - CSV loading
- Line 2738-2773: `get_meta_for_id()` - Metadata retrieval
- Line 3078-3080: MARC tag 710 parsing
- Line 3690-3702: `get_display_data()` - Display dict creation

### web/services.py
- Line 32-44: `SearchResult` dataclass
- Line 47-68: `BrowsePage` dataclass
- Line 81-89: `ManuscriptInfo` dataclass

### web/pages/search.py
- Line 959-1000: Result card rendering
- Line 1137: Shelfmark display

### web/pages/browse.py
- Line 1446: Page shelfmark display
- Line 1551: Header rendering

### web/export_service.py
- Line 322-385: `export_search_results_excel()`
- Line 433-490: `export_list_excel()`

### genizah_app.py
- Line 2067+: `ResultDialog` class
- Line 3632-3633: Library display in ResultDialog
- Line 8810-8848: List export
- Line 11639+: Search results export
