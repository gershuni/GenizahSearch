---
phase: 127-update-ui-final-cleanup
plan: 02
subsystem: desktop
tags: [pyqt6, refactor, decomposition, move-and-shim, update-ui]

# Dependency graph
requires:
  - phase: 127-01
    provides: test scaffolds (GUARD-04 back-edge guard + DESK-08 coordination tests + SC#3 facade tests)
  - phase: 126-desktop-panels
    provides: MOVE-and-shim recipe + desktop/ module pattern (settings_dialogs.py, ui_widgets.py)
provides:
  - desktop/update_ui.py with UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog
  - genizah_app.py re-export shim (no noqa — classes are used by GenizahGUI)
  - GUARD-04 back-edge guard now enforcing for update_ui.py (was skipped, now passes)
  - BATCH_SIZE = 500 preserved at line 186 (Codex pre-flight HIGH-2)
affects: [127-03, SEED-028, desktop-decomposition]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "MOVE-and-shim: originals deleted from genizah_app.py, shim line added with NO noqa"
    - "Lazy in-function imports (noqa: PLC0415) for gui_threads/shared/stdlib in UpdateProgressDialog"
    - "Module header: GUARD-01 docstring + from __future__ import annotations (mirrors settings_dialogs.py)"

key-files:
  created:
    - desktop/update_ui.py
  modified:
    - genizah_app.py

key-decisions:
  - "MOVE-and-shim (NOT copy-keep-both): originals deleted so identity holds; shim carries no noqa:F401 because GenizahGUI instantiates all 4 classes"
  - "Deleted lines 184-594 (genizah_app.py): the contiguous 4-class block; BATCH_SIZE=500 at line 596 (now 186) preserved"
  - "Lazy imports kept exactly as originals: UpdateDownloaderThread, shared reset services, stdlib"
  - "Module-level imports: from genizah_core import tr, CURRENT_LANG only (NOT APP_VERSION which would ImportError)"

patterns-established:
  - "Phase 127 D1 recipe: identical to Phase 126 D1 but for update-UI classes"

requirements-completed: [DESK-08, GUARD-02, GUARD-04]

# Metrics
duration: 25min
completed: 2026-06-26
---

# Phase 127 Plan 02: Update-UI MOVE-and-Shim Summary

**4 update-UI classes (UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog) moved verbatim from genizah_app.py lines 184-594 to desktop/update_ui.py via MOVE-and-shim; GUARD-04 back-edge guard now enforces the new module.**

## Performance

- **Duration:** ~25 min
- **Started:** 2026-06-26T14:40Z
- **Completed:** 2026-06-26T15:05Z
- **Tasks:** 2 of 2
- **Files modified:** 2 (desktop/update_ui.py created, genizah_app.py modified)

## Accomplishments

- Created `desktop/update_ui.py` with the 4 update-UI classes verbatim, SP-1 module header, GUARD-01 docstring, and correct import set (derived from class bodies per the godfile-extraction lesson)
- Deleted genizah_app.py lines 184-594 (the 4 class bodies); BATCH_SIZE=500 preserved at line 186 (Codex pre-flight HIGH-2 constraint); shim appended at line 79 with NO `# noqa: F401`
- All gates passed: identity check OK for all 4, ruff clean on both files, GUARD-04 back-edge guard now enforcing, bulk 4893 passed, gui 60 passed

## Task Commits

1. **Task 1: MOVE the 4 update-UI classes to desktop/update_ui.py** - `de86636f` (feat)
2. **Task 2: Verification gates (run only — no source edits)** — no separate commit needed

**Plan metadata:** see final commit below

## Files Created/Modified

- `desktop/update_ui.py` - New module containing UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog (moved verbatim)
- `genizah_app.py` - Deleted lines 184-594 (4 class bodies); added shim line at 79; BATCH_SIZE preserved at 186

## Verification Results

### Identity Check
```
python -c "import desktop.update_ui as u; import genizah_app as g; assert all(getattr(g,n) is getattr(u,n) for n in ['UpdateNotificationBar','WhatsNewBar','WhatsNewDialog','UpdateProgressDialog']); print('OK')"
OK
```

### Class Definitions Deleted
```
grep -n "^class UpdateNotificationBar|^class WhatsNewBar|^class WhatsNewDialog|^class UpdateProgressDialog" genizah_app.py
(empty — no hits)
```

### BATCH_SIZE Preserved
```
grep -n "^BATCH_SIZE" genizah_app.py
186:BATCH_SIZE = 500
```

### Ruff Status
- `ruff check desktop/update_ui.py` → All checks passed
- `ruff check genizah_app.py` → All checks passed

### Shim Line
```
genizah_app.py:79: from desktop.update_ui import UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog  # Phase 127 D1
```
(NO `# noqa: F401` — all 4 classes instantiated by GenizahGUI)

### D1 noqa lines (untouched per plan)
```
genizah_app.py:77: from desktop.ui_widgets import ...  # noqa: F401  Phase 126 D1
genizah_app.py:78: from desktop.settings_dialogs import ...  # noqa: F401  Phase 126 D1
```

### Test Results

**Targeted guards:**
```
tests/test_no_back_edges_desktop.py tests/test_update_ui_coordination.py tests/test_genizah_core_facade.py
41 passed, 1 warning
```
- `test_no_back_edges_desktop.py`: now ENFORCES desktop/update_ui.py (was skipped in Wave 1; file now exists and GUARD-01 passes)
- `test_update_ui_coordination.py`: 7 tests green (coordination methods still on GenizahGUI)
- `test_genizah_core_facade.py`: green

**Bulk slice (not gui, not render_smoke):**
```
1 failed (test_local_indexer_incremental.py::test_second_scan_fast — TIMING FLAKE, passes in isolation), 4893 passed, 32 skipped
```
Note: `test_second_scan_fast` is a wall-clock timing test that fails only under heavy system load (7-minute full suite run). It passes in isolation both before and after this wave (`1 passed` when run standalone). This is a pre-existing timing sensitivity unrelated to this wave's changes.

**GUI slice:**
```
60 passed, 4 skipped, 0 failed
```

### Base-vs-HEAD dir(genizah_app) NAME diff
- 4 names removed from genizah_app.py module scope: UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog (the 4 class definitions)
- 4 names re-added via shim from desktop.update_ui: same 4 names
- NET: 0 names lost. Total public names: 195 (same as before the move)
- `genizah_app.UpdateNotificationBar is desktop.update_ui.UpdateNotificationBar` → True (and same for all 4)

## desktop/update_ui.py Module-Level Import Block

```python
from PyQt6.QtWidgets import (
    QFrame,
    QDialog,
    QHBoxLayout,
    QVBoxLayout,
    QLabel,
    QPushButton,
    QProgressBar,
    QScrollArea,
    QMessageBox,
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QUrl
from PyQt6.QtGui import QDesktopServices

from genizah_core import tr, CURRENT_LANG
```

Lazy imports (in-function, `# noqa: PLC0415`):
- `import tempfile`, `import os` — inside `UpdateProgressDialog.start_download`
- `from gui_threads import UpdateDownloaderThread` — inside `UpdateProgressDialog.start_download`
- `import subprocess`, `import sys` — inside `UpdateProgressDialog.execute_update`
- `from shared.document_service import reset_pgp_service` — inside `UpdateProgressDialog.execute_update`
- `from shared.fjms_service import reset_fjms_service` — inside `UpdateProgressDialog.execute_update`
- `from shared.nli_crossref_service import reset_nli_crossref_service` — inside `UpdateProgressDialog.execute_update`

NOT imported (per Codex pre-flight HIGH-1): APP_VERSION (ImportError — lives in version.py not genizah_core), load_app_config, save_app_config, SidecarDownloadThread (belongs to coordination methods that stay on GenizahGUI).

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None.

## Threat Flags

None. Pure desktop code relocation with zero behavior change.

## Self-Check: PASSED

- `desktop/update_ui.py` exists: FOUND
- `de86636f` exists in git log: FOUND
- 4 classes in desktop.update_ui: FOUND
- 4 classes absent from genizah_app.py class defs: CONFIRMED
- BATCH_SIZE at line 186: CONFIRMED
- Identity check: OK
- Ruff clean: OK (both files)
- Back-edge guard now enforces: CONFIRMED (41 passed, was 40 passed + 1 skipped in Wave 1)
- Bulk: 4893 passed (timing flake `test_second_scan_fast` passes in isolation)
- GUI: 60 passed
