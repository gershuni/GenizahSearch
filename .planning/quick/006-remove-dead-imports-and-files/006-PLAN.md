---
phase: quick
plan: 006
type: execute
wave: 1
depends_on: []
files_modified:
  - web/pages/viewer.py        # DELETE
  - web/pages/document.py      # DELETE
  - genizah_app.py
  - corrections_ui.py
  - genizah_core.py
  - web/state.py
  - web/supabase_client.py
  - web/user_lists.py
  - web/pages/corrections.py
autonomous: true

must_haves:
  truths:
    - "Two dead files (viewer.py, document.py) no longer exist"
    - "All listed unused imports are removed"
    - "Application still imports and starts without errors"
  artifacts:
    - path: "web/pages/viewer.py"
      provides: "DELETED - must not exist"
    - path: "web/pages/document.py"
      provides: "DELETED - must not exist"
  key_links:
    - from: "genizah_app.py"
      to: "gui_threads, corrections_ui"
      via: "import statements"
      pattern: "remaining imports still resolve"
---

<objective>
Remove dead files and unused imports identified by the quick-005 unused functions report.
This is a conservative first pass: only dead files and unused imports, NO functions/methods/classes.

Purpose: Reduce code noise, improve clarity, remove ~500 lines of dead files and ~15 lines of dead imports.
Output: Cleaner codebase with no dead file or import warnings for the targeted items.
</objective>

<execution_context>
@C:\Users\gersh\.claude/get-shit-done/workflows/execute-plan.md
@C:\Users\gersh\.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/STATE.md
@.planning/quick/005-find-unused-functions/005-UNUSED-FUNCTIONS-REPORT.md
</context>

<tasks>

<task type="auto">
  <name>Task 1: Delete dead files</name>
  <files>web/pages/viewer.py, web/pages/document.py</files>
  <action>
Before deleting, run grep across the entire codebase to CONFIRM these files are not imported anywhere:

```bash
grep -r "viewer" --include="*.py" . | grep -i "import"
grep -r "document" --include="*.py" web/ | grep -i "from web.pages.document"
```

Expected: No imports of `web.pages.viewer` or `web.pages.document` anywhere.

If confirmed safe:
- Delete `web/pages/viewer.py` (222 lines, old standalone viewer superseded by browse.py)
- Delete `web/pages/document.py` (276 lines, old document page superseded by browse.py, no @ui.page decorator)

Stage and commit:
```
git add -u web/pages/viewer.py web/pages/document.py
git commit -m "chore(quick-006): remove dead files viewer.py and document.py"
```
  </action>
  <verify>
Confirm files are gone:
```bash
test ! -f web/pages/viewer.py && test ! -f web/pages/document.py && echo "PASS"
```
  </verify>
  <done>Both dead files deleted, no import references broken.</done>
</task>

<task type="auto">
  <name>Task 2: Remove unused imports</name>
  <files>genizah_app.py, corrections_ui.py, genizah_core.py, web/state.py, web/supabase_client.py, web/user_lists.py, web/pages/corrections.py</files>
  <action>
Make these specific, surgical edits. Each is removing an unused import or unused variable. Do NOT touch any function/method/class bodies except the one vote_val case noted.

**genizah_app.py:**

1. Line 33 - Remove `QSize` from the QtCore import:
   Change: `from PyQt6.QtCore import (Qt, QTimer, QUrl, QSize, pyqtSignal, QThread, QEventLoop, QEvent, QRect, QRectF)`
   To:     `from PyQt6.QtCore import (Qt, QTimer, QUrl, pyqtSignal, QThread, QEventLoop, QEvent, QRect, QRectF)`

2. Line 34 - Remove `QTextDocument` and `QTransform` from the QtGui import:
   Change: `from PyQt6.QtGui import (QFont, QIcon, QDesktopServices, QPixmap, QImage, QFontMetrics, QTextDocument, QTransform, QPainter, QColor,`
   To:     `from PyQt6.QtGui import (QFont, QIcon, QDesktopServices, QPixmap, QImage, QFontMetrics, QPainter, QColor,`

3. Line 57 - Remove `ExternalResourceThread` from gui_threads import:
   Change: `from gui_threads import SearchThread, LabSearchThread, IndexerThread, ShelfmarkLoaderThread, CompositionThread, LabCompositionThread, GroupingThread, AIWorkerThread, StartupThread, EnrichMetadataThread, ExternalResourceThread, UpdateCheckerThread`
   To:     `from gui_threads import SearchThread, LabSearchThread, IndexerThread, ShelfmarkLoaderThread, CompositionThread, LabCompositionThread, GroupingThread, AIWorkerThread, StartupThread, EnrichMetadataThread, UpdateCheckerThread`

4. Line 71 - Remove `TextEditorDialog` from corrections_ui import:
   Change: `    TextEditorDialog, JoinsDialog`
   To:     `    JoinsDialog`
   (Ensure the preceding line's trailing comma is correct after removing TextEditorDialog)

**corrections_ui.py:**

5. Line 15 - Remove `QStatusBar` from QtWidgets import:
   Change: `    QSplitter, QMenu, QStatusBar, QListWidget, QListWidgetItem,`
   To:     `    QSplitter, QMenu, QListWidget, QListWidgetItem,`

6. Line 19 - Remove `QAction` from QtGui import:
   Change: `from PyQt6.QtGui import QFont, QColor, QAction, QPalette, QStandardItem, QStandardItemModel`
   To:     `from PyQt6.QtGui import QFont, QColor, QPalette, QStandardItem, QStandardItemModel`

**genizah_core.py:**

7. Lines 48-53 - Remove `get_top_pairs` import and its fallback function:
   Change:
   ```python
   try:
       from unified_variants import UNIFIED_VARIANT_PAIRS, get_top_pairs
   except ImportError:
       # Fallback if file not found
       UNIFIED_VARIANT_PAIRS = []
       def get_top_pairs(n): return []
   ```
   To:
   ```python
   try:
       from unified_variants import UNIFIED_VARIANT_PAIRS
   except ImportError:
       # Fallback if file not found
       UNIFIED_VARIANT_PAIRS = []
   ```

**web/state.py:**

8. Line 1 - Remove `Union` from typing import:
   Change: `from typing import Optional, List, Dict, Any, Union`
   To:     `from typing import Optional, List, Dict, Any`

**web/supabase_client.py:**

9. Line 16 - Remove the entire `urlencode` import line:
   Delete: `from urllib.parse import urlencode`

**web/user_lists.py:**

10. Line 30 - Remove `get_recent_items` from the supabase_client import:
    Change: `    get_recent_items, add_recent_item, get_projects, create_project as sb_create_project,`
    To:     `    add_recent_item, get_projects, create_project as sb_create_project,`

**web/pages/corrections.py:**

11. Line 242 - The function `do_vote(vote_val: int, cid=corr_id)` receives `vote_val` as a parameter but never reads it. Remove the parameter:
    Change: `def do_vote(vote_val: int, cid=corr_id):`
    To:     `def do_vote(cid=corr_id):`
    Also update the two callers immediately below:
    - `upvote`: Change `do_vote(1, cid)` to `do_vote(cid)`
    - `downvote`: Change `do_vote(-1, cid)` to `do_vote(cid)`

Stage and commit all import changes together:
```
git add genizah_app.py corrections_ui.py genizah_core.py web/state.py web/supabase_client.py web/user_lists.py web/pages/corrections.py
git commit -m "chore(quick-006): remove unused imports and dead code"
```
  </action>
  <verify>
Run a syntax check on each modified file:
```bash
python -c "import py_compile; py_compile.compile('genizah_app.py', doraise=True)"
python -c "import py_compile; py_compile.compile('corrections_ui.py', doraise=True)"
python -c "import py_compile; py_compile.compile('genizah_core.py', doraise=True)"
python -c "import py_compile; py_compile.compile('web/state.py', doraise=True)"
python -c "import py_compile; py_compile.compile('web/supabase_client.py', doraise=True)"
python -c "import py_compile; py_compile.compile('web/user_lists.py', doraise=True)"
python -c "import py_compile; py_compile.compile('web/pages/corrections.py', doraise=True)"
```
All must compile without errors.
  </verify>
  <done>All 11 unused imports/variables removed. All files compile cleanly.</done>
</task>

<task type="auto">
  <name>Task 3: Verify application still starts</name>
  <files></files>
  <action>
Run import checks to verify nothing is broken:

```bash
python -c "import genizah_core; print('genizah_core OK')"
python -c "import genizah_app; print('genizah_app OK')"
python -c "from web.main import *; print('web.main OK')"
```

If any import fails, diagnose and fix the specific issue (likely a missed dependency in the import edits).

Also verify corrections_ui imports cleanly:
```bash
python -c "import corrections_ui; print('corrections_ui OK')"
```

Note: genizah_app.py may need a display/GUI environment to fully import (PyQt6). If it fails due to missing display, that is expected in a headless environment - the important thing is it does NOT fail due to ImportError from the removed imports. Check the error message carefully.
  </action>
  <verify>
All four import commands succeed (or fail only due to display/environment issues, NOT due to ImportError from the changes made).
  </verify>
  <done>Application modules import successfully. No regressions from dead code removal.</done>
</task>

</tasks>

<verification>
- `web/pages/viewer.py` and `web/pages/document.py` do not exist
- All 7 modified files compile without syntax errors
- Core modules (genizah_core, genizah_app, web.main, corrections_ui) import without ImportError
- Two clean git commits: one for file deletions, one for import cleanup
</verification>

<success_criteria>
- 2 dead files deleted (~498 lines removed)
- 11 unused imports/variables removed across 7 files
- Zero import errors introduced
- 2 atomic commits created
</success_criteria>

<output>
After completion, create `.planning/quick/006-remove-dead-imports-and-files/006-SUMMARY.md`
</output>
