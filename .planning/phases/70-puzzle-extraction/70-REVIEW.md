---
phase: 70-puzzle-extraction
reviewed: 2026-04-16T00:00:00Z
depth: standard
files_reviewed: 2
files_reviewed_list:
  - desktop/puzzle.py
  - genizah_app.py
findings:
  critical: 0
  warning: 0
  info: 0
  total: 0
status: clean
---

# Phase 70: Code Review Report

**Reviewed:** 2026-04-16
**Depth:** standard
**Files Reviewed:** 2
**Status:** clean

## Summary

Phase 70 is a pure structural extraction: 5 puzzle/join canvas classes (~2,642 lines) moved from `genizah_app.py` into a new `desktop/puzzle.py` module, with back-compat re-exports added to `genizah_app.py`.

All reviewed files meet quality standards. No issues found.

### Checks performed

**Import correctness (`desktop/puzzle.py`)**

All 30+ symbols imported at the module top are actively used in the class bodies:
- Every `QtWidgets` import (`QApplication`, `QComboBox`, `QCompleter`, `QDialog`, `QDockWidget`, `QFileDialog`, `QGraphicsItem`, `QGraphicsPixmapItem`, `QGraphicsTextItem`, `QGraphicsScene`, `QGraphicsView`, `QGroupBox`, `QHBoxLayout`, `QInputDialog`, `QLabel`, `QLineEdit`, `QListWidget`, `QListWidgetItem`, `QMainWindow`, `QMenu`, `QMessageBox`, `QPushButton`, `QProgressDialog`, `QSlider`, `QTextEdit`, `QVBoxLayout`, `QWidget`) is referenced in at least one class body.
- Every `QtCore` import (`Qt`, `QRectF`, `QSize`, `QPointF`, `QTimer`, `pyqtSignal`, `QThread`) is used.
- Every `QtGui` import (`QAction`, `QBrush`, `QColor`, `QCursor`, `QIcon`, `QImage`, `QPainter`, `QPainterPath`, `QPen`, `QPixmap`, `QTransform`) is used.
- `sip` used for deleted-widget guards; `math`, `os`, `functools.partial` all have call sites.
- `normalize_shelfmark` from `genizah_core` is called at lines 1126 and 1252.
- `PuzzleImageLoaderThread` and `PuzzleMetaLoaderThread` from `gui_threads` are both referenced in `PuzzleCanvasWindow`.

**ShelfmarkCompleter lazy import (D-04)**

The lazy function-local import at line 751 (`from genizah_app import ShelfmarkCompleter`) is correctly placed inside a `hasattr` guard (`if hasattr(self.app, 'shelf_model') and self.app.shelf_model:`). This avoids circular import at module load time and matches the decision recorded in D-04/D-05. No issue.

**Re-export correctness (`genizah_app.py` line 63)**

The re-export line is present and covers all 5 extracted classes in the correct order:
```
from desktop.puzzle import PuzzleFragmentItem, PuzzleCanvasView, PuzzleExportThread, PuzzlePublishThread, PuzzleCanvasWindow  # noqa: F401
```
The `# noqa: F401` suppressor is required because ruff cannot see that these names are used downstream as `PuzzleCanvasWindow(self)` at lines 14043 and 14051. Correct pattern — matches Phase 68/69.

**Zero stale class definitions**

`grep -n "^class Puzzle"` finds no matches in `genizah_app.py`. The 5 classes appear only in `desktop/puzzle.py`.

**Call sites in GenizahGUI**

All documented call sites work through the re-export:
- `PuzzleCanvasWindow(self)` at lines 14043, 14051 — resolved via re-export.
- `self._puzzle_window._load_document(new_doc_id)` at line 13936 — private API call unchanged.
- `self._puzzle_window._folio_lists[sys_id]` at line 14056 — private state access unchanged.
- `self._puzzle_window._on_meta_resolved`, `._on_meta_failed`, `._meta_threads` at lines 14067–14069 — signal wiring unchanged.

**`math` import in `genizah_app.py`**

Top-level `import math` was removed as part of the 15 cleaned-up unused imports. The remaining use at line 1224 is a function-local `import math` inside a star-drawing method. This is valid — the local import is self-contained and the removal of the top-level import is correct.

**`desktop/__init__.py`**

Minimal one-liner docstring — no barrel re-exports added, consistent with D-09.

**Ruff / test baseline**

Per 70-01-SUMMARY.md: `ruff check genizah_app.py desktop/puzzle.py` clean; pytest 1066 passed, 9 skipped (matches pre-extraction baseline).

---

_Reviewed: 2026-04-16_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
