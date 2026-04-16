---
phase: 68-desktop-dialog-extractions
reviewed: 2026-04-16T00:00:00Z
depth: standard
files_reviewed: 5
files_reviewed_list:
  - desktop/dialogs_scholarly.py
  - desktop/dialogs_filter.py
  - desktop/result_dialog.py
  - genizah_app.py
  - gui_threads.py
findings:
  critical: 0
  warning: 1
  info: 2
  total: 3
status: warnings
---

# Phase 68: Code Review Report

**Reviewed:** 2026-04-16
**Depth:** standard
**Files Reviewed:** 5
**Status:** warnings (1 warning, 2 info)

## Summary

Phase 68 extracted 7 dialog classes from `genizah_app.py` into two new modules
(`desktop/dialogs_scholarly.py`, `desktop/dialogs_filter.py`) and relocated
`FilterCountWorker` from `genizah_app.py` to `gui_threads.py`. The refactoring
is structurally sound.

Import graph is correct: all 7 classes are re-exported from `genizah_app.py`
via `# noqa: F401` import lines (lines 61-62), and `FilterCountWorker` is
re-exported from `genizah_app.py` line 49 via the `gui_threads` import. No
stale references to old class locations exist in any `.py` file. AST parses
cleanly for both new modules. `result_dialog.py` lazy imports all 4 scholarly
dialogs from the correct new path (`desktop.dialogs_scholarly`).

One warning was identified: a potential `AttributeError` at runtime if
`_section_row` or `_field_row` are ever called before `_build_html` initialises
`self._colors`. Two info items cover a stale orphaned comment and an E302
blank-line deficit in `dialogs_scholarly.py`.

---

## Warnings

### WR-01: `FjmsCatalogDialog._section_row` / `_field_row` access `self._colors` before guaranteed initialisation

**File:** `desktop/dialogs_scholarly.py:836-860`

**Issue:** `_section_row` and `_field_row` read `self._colors` (set inside
`_build_html` at line 325). Both are private methods currently only called from
within `_build_html`, so in practice `self._colors` is always set first. However
`self._colors` is not initialised in `__init__`, which means any future caller
that invokes `_section_row` or `_field_row` directly — or any subclass that
calls them without first calling `_build_html` — will raise `AttributeError`.
This is a latent fragility introduced by moving the class verbatim; the original
code had the same issue, but extraction makes it more likely to be reused.

**Fix:** Initialise `self._colors` to an empty dict (or a safe default) in
`__init__`, so the class invariant holds from construction:

```python
class FjmsCatalogDialog(QDialog):
    def __init__(self, detail: dict, sys_id: str = '', shelfmark: str = '', parent=None):
        super().__init__(parent)
        self._colors = {}  # populated by _build_html; guards _section_row/_field_row
        ...
```

---

## Info

### IN-01: Orphaned section comment in `dialogs_filter.py`

**File:** `desktop/dialogs_filter.py:528`

**Issue:** Line 528 contains a stale box-drawing comment
`# -- Puzzle Canvas Building Blocks (Phase 48) ---` that was a section
separator in `genizah_app.py` marking where Puzzle classes began. No Puzzle
classes follow it in `dialogs_filter.py`; the next class is `DomainFilterDialog`
(line 533). The comment is misleading and refers to a different feature.

**Fix:** Delete the orphaned comment block (lines 528-531, including the
surrounding extra blank lines that leave four consecutive blank lines):

```python
# Remove lines 528-531:
# ── Puzzle Canvas Building Blocks (Phase 48) ────────────────────────────
# (three blank lines follow)
```

Replace with two blank lines to satisfy E303.

---

### IN-02: Missing second blank line before `FjmsBibliographyDialog` (E302)

**File:** `desktop/dialogs_scholarly.py:13`

**Issue:** PEP 8 / ruff E302 requires two blank lines before a top-level class
definition. Only one blank line separates the last `import` statement (line 11)
from `class FjmsBibliographyDialog` (line 13). All other top-level classes in
the file correctly have two blank lines.

**Fix:** Add one blank line between lines 12 and 13:

```python
from genizah_core import CURRENT_LANG, load_app_config, tr


class FjmsBibliographyDialog(QDialog):
```

---

_Reviewed: 2026-04-16_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
