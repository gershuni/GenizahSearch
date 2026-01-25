# Codebase Audit Report: Dicta Genizah Search

## Executive Summary
This report details the findings from a code analysis of the Dicta Genizah Search application, focusing on the differences and redundancies between the Desktop (PyQt6) and Web (NiceGUI) versions. The analysis revealed significant code duplication, feature drift, and architectural opportunities to centralize business logic.

**Status Update (Current Session):**
All identified high and medium priority issues (Translations, Search Logic, Feature Drift) have been addressed and fixed.

## 1. Redundancies & Code Duplication

### A. Translation Files [FIXED]
**Severity:** Medium
**Description:**
The application maintained two separate translation dictionaries with nearly identical content.
- **Status:** **Fixed.** `genizah_translations.py` is now the single source of truth. `web/translations.py` imports from it. All unique keys from the web version were merged into the core file.

### B. Search Query Parsing [FIXED]
**Severity:** Medium
**Description:**
The logic for parsing special search syntax (prefixes like `#` for shelfmark, `$` for title, `???` for max variants) was duplicated.
- **Status:** **Fixed.** Logic centralized in `SearchEngine.parse_query_syntax` within `genizah_core.py`. Both Desktop and Web apps now use this method.

### C. Snippet Formatting [FIXED]
**Severity:** Low
**Description:**
Both versions implemented Regex logic to convert search result highlights (asterisks `*word*`) into HTML markup.
- **Status:** **Fixed.** Logic centralized in `SearchEngine.format_snippet` (static method) in `genizah_core.py`. Both apps use this method, supporting both CSS classes (Web) and inline styles (Desktop).

---

## 2. Feature Drift (Inconsistencies)

### A. "Exclude Words" (NOT Filter) [FIXED]
**Severity:** High
**Description:**
The Web version implemented a "NOT Filter" feature that was missing from the Desktop version.
- **Status:** **Fixed.**
    1.  Core Logic: `SearchEngine.execute_search` now accepts an `exclude_words` parameter and handles the filtering internally.
    2.  Desktop UI: Added an "Exclude Words" input field to the Search Tab in `genizah_app.py` and connected it to the search logic.
    3.  Web UI: Updated to use the Core logic instead of local implementation.

---

## 3. Architectural Recommendations

### A. Centralize Business Logic in `genizah_core.py`
**Problem:** `SearchEngine` was too "dumb".
**Solution:**
- **Move Query Parsing:** Done.
- **Move Filtering:** Done.

### B. Unify Internationalization (i18n)
**Problem:** Split translation files.
**Solution:** Done.

### C. Standardize Result Models
**Problem:** UI layers manually manipulate result dictionaries for display.
**Solution:**
- **Future Work:** Ensure `SearchEngine` returns fully enriched result objects. Currently, `SearchEngine.format_snippet` helps, but full result object standardization (e.g., Pydantic models or dataclasses) remains as a potential future improvement.

## 4. Summary of Changes

1.  **Refactor Search Engine:** Updated `genizah_core.py` to include `exclude_words` logic and `parse_query_syntax`. Added `format_snippet`.
2.  **Update Web App:** Removed local search logic and snippet formatting from `web/pages/search.py`.
3.  **Update Desktop App:** Updated `genizah_app.py` to use Core methods. Added "Exclude" UI field. Updated `gui_threads.py` to support exclusion.
4.  **Merge Translations:** Unified translation dictionaries into `genizah_translations.py`.
