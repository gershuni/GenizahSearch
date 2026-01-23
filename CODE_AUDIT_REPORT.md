# Codebase Audit Report: Genizah Search Pro

## Executive Summary
This report details the findings from a code analysis of the Genizah Search Pro application, focusing on the differences and redundancies between the Desktop (PyQt6) and Web (NiceGUI) versions. The analysis revealed significant code duplication, feature drift, and architectural opportunities to centralize business logic.

## 1. Redundancies & Code Duplication

### A. Translation Files
**Severity:** Medium
**Description:**
The application maintains two separate translation dictionaries with nearly identical content:
- **Desktop:** `genizah_translations.py` (Core dictionary `TRANSLATIONS`)
- **Web:** `web/translations.py` (Dictionary `TRANSLATIONS`)

**Impact:**
- High maintenance overhead: Every UI text change requires updates in two files.
- Inconsistency risk: Users on different platforms may see different translations for the same features.

### B. Search Query Parsing
**Severity:** Medium
**Description:**
The logic for parsing special search syntax (prefixes like `#` for shelfmark, `$` for title, `???` for max variants) is implemented independently in both UI layers:
- **Desktop:** `genizah_app.py` inside `_detect_query_prefix`.
- **Web:** `web/pages/search.py` inside `execute_search`.

**Impact:**
- Violation of DRY (Don't Repeat Yourself) principle.
- If a new search syntax is added, it must be implemented twice.

### C. Snippet Formatting
**Severity:** Low
**Description:**
Both versions implement Regex logic to convert search result highlights (asterisks `*word*`) into HTML markup:
- **Desktop:** `render_asterisks_to_html` (Injects inline styles).
- **Web:** `format_snippet` (Injects CSS classes).

**Impact:**
- Redundant logic that could be centralized in the Core, returning a neutral format or standardized HTML.

---

## 2. Feature Drift (Inconsistencies)

### A. "Exclude Words" (NOT Filter)
**Severity:** High
**Description:**
The Web version implements a "NOT Filter" (Exclude Words) feature that post-processes search results to remove items containing specific words.
- **Web:** Implemented in `web/pages/search.py` within `run_core_search`.
- **Desktop:** **Missing.** The `genizah_app.py` search logic does not implement this filtering step.

**Impact:**
- Desktop users lack search refinement capabilities available to Web users.

---

## 3. Architectural Recommendations

### A. Centralize Business Logic in `genizah_core.py`
**Problem:** Currently, the `SearchEngine` class (Core) is too "dumb"—it only executes raw Tantivy queries. The UI layers are responsible for "smart" features like query parsing and result filtering.
**Solution:**
1.  **Move Query Parsing:** Create a method `SearchEngine.parse_query_syntax(raw_query)` that returns the `clean_query`, `mode`, and `gap` settings.
2.  **Move Filtering:** Update `SearchEngine.execute_search` to accept an `exclude_words` list and handle the filtering internally.

### B. Unify Internationalization (i18n)
**Problem:** Split translation files.
**Solution:**
1.  Create a shared module (e.g., `genizah_core/i18n.py` or keep `genizah_translations.py` as the master).
2.  Refactor both Desktop and Web apps to import the translation dictionary from this single source.

### C. Standardize Result Models
**Problem:** UI layers manually manipulate result dictionaries for display.
**Solution:**
Ensure `SearchEngine` returns fully enriched result objects that require minimal processing by the UI (e.g., pre-formatted snippets, standardized metadata fields).

## 4. Immediate Action Plan

1.  **Refactor Search Engine:** Update `genizah_core.py` to include the `exclude_words` logic and query prefix parsing.
2.  **Update Web App:** Remove local logic from `web/pages/search.py` and call the enhanced Core methods.
3.  **Update Desktop App:** Update `genizah_app.py` to use the enhanced Core methods, thereby automatically gaining the "Exclude Words" feature (once the UI input is added).
4.  **Merge Translations:** Delete `web/translations.py` content and make it import from `genizah_translations.py`.
