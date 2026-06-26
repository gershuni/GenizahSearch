# Phase 127: Update UI & Final Cleanup - Pattern Map

**Mapped:** 2026-06-26
**Files analyzed:** 6 (1 new module, 2 new test guards, 2 new behavioral/facade tests, 1 modify + 2 modify tests)
**Analogs found:** 6 / 6

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `desktop/update_ui.py` | module (MOVE-and-shim) | event-driven (PyQt6 signals) | `desktop/settings_dialogs.py` + `desktop/ui_widgets.py` | exact (same D1 recipe) |
| `genizah_app.py` (modify) | god-file (shim lines only) | n/a | `genizah_app.py` lines 77-78 (D1 shim lines) | exact |
| `tests/test_update_ui_coordination.py` | test (behavioral, GUI-duck) | request-response | `tests/test_telemetry_consent_ux.py` | exact (`__new__` + stub pattern) |
| `tests/test_no_back_edges_desktop.py` | test (AST guard) | transform | `tests/test_no_back_edges_core.py` | exact (template) |
| `tests/test_genizah_core_facade.py` | test (identity assertions) | transform | identity-assertion blocks in `tests/test_no_back_edges_core.py` lines 178-499+ | exact |
| `tests/test_privacy_disclosure_strings.py` (modify) | test (OR-location flip) | transform | `tests/test_tabular_builder_rtl.py` (OR-location pattern) | exact |

---

## Pattern Assignments

### `desktop/update_ui.py` (new module, MOVE-and-shim)

**Analog:** `desktop/settings_dialogs.py` lines 1-23 AND `desktop/ui_widgets.py` lines 1-37

**Module header + docstring pattern** (`desktop/settings_dialogs.py` lines 1-23):
```python
# -*- coding: utf-8 -*-
"""Top-level modal dialogs (extracted from genizah_app.py, Phase 126 D1).

Provides five modal QDialog subclasses moved verbatim out of the
28K-line ``genizah_app.py`` god file:

  - LabScoringDialog(QDialog)         — advanced Lab-mode scoring weights
  ...

ZERO behavior change vs. the originals. ``genizah_app.py`` re-exports these
via a ``# noqa: F401`` shim (MOVE-and-shim, mirroring genizah_core 122-125).

GUARD-01: NO module-level ``import genizah_app`` — shared symbols come from the
``genizah_core`` facade only.
...
"""
from __future__ import annotations
```

**Import pattern** (`desktop/ui_widgets.py` lines 17-37):
```python
from __future__ import annotations

import re

from PyQt6.QtWidgets import (
    QTableWidgetItem,
    QHeaderView,
    QScrollArea,
    QTreeWidget,
    QFrame,
    QLabel,
    QStyle,
    QStyleOptionButton,
    QToolTip,
    QAbstractItemView,
)
from PyQt6.QtCore import Qt, QRect, QEvent, QTimer, pyqtSignal
from PyQt6.QtGui import QColor, QPalette, QPen, QBrush, QPainterPath

from genizah_core import natural_sort_key, tr
```

**Divergence for `desktop/update_ui.py`:** The import block uses `genizah_core` facade for `tr`, `CURRENT_LANG`, `load_app_config`, `save_app_config`, `APP_VERSION`, plus `gui_threads` for `SidecarDownloadThread`/`UpdateDownloaderThread`, plus PyQt6 `QFrame`/`QDialog`/`QProgressBar`/`QScrollArea`/`QMessageBox`/`QUrl`/`QDesktopServices`. The `# -*- coding: utf-8 -*-` header, `from __future__ import annotations`, and `GUARD-01` docstring note must all be present exactly as in both analogs. No `# noqa: F401` on the new shim import in `genizah_app.py` because the 4 classes are used by `genizah_app.py` directly (lines 2263, 2269, 24464, 24476).

---

### `genizah_app.py` (modify — shim + D1 noqa retirement)

**Analog:** `genizah_app.py` lines 77-78 (the existing D1 shim lines, current state)

**Current state of D1 shim lines** (`genizah_app.py` lines 77-78):
```python
from desktop.ui_widgets import ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget  # noqa: F401  Phase 126 D1
from desktop.settings_dialogs import SettingsDialog, SearchSettingsDialog, HelpDialog, TabularQueryBuilderDialog, LabScoringDialog  # noqa: F401  Phase 126 D1
```

**Target state after Phase 127 (remove `# noqa: F401  Phase 126 D1` suffix; imports STAY because genizah_app.py uses all 9 classes):**
```python
from desktop.ui_widgets import ShelfmarkTableWidgetItem, CheckBoxHeader, HiddenScrollArea, ListsTreeWidget
from desktop.settings_dialogs import SettingsDialog, SearchSettingsDialog, HelpDialog, TabularQueryBuilderDialog, LabScoringDialog
```

**New shim line to add for DESK-08 (no `# noqa: F401` because classes ARE used in genizah_app.py body):**
```python
from desktop.update_ui import UpdateNotificationBar, WhatsNewBar, WhatsNewDialog, UpdateProgressDialog
```

**Critical divergence:** D1 shims needed `# noqa: F401` because the only "use" was re-export to external callers. The DESK-08 shim does NOT need `# noqa: F401` because `genizah_app.py` itself instantiates all four classes (at lines ~2263, ~2269, ~24464, ~24476). Deleting the import lines would be wrong — only the `# noqa: F401  Phase 126 D1` suffix is removed from lines 77-78.

---

### `tests/test_update_ui_coordination.py` (new behavioral tests)

**Analog:** `tests/test_telemetry_consent_ux.py` lines 475-546 (the `__new__` construction + stub pattern)

**`__new__` construction + stub pattern** (`tests/test_telemetry_consent_ux.py` lines 485-546):
```python
from PyQt6.QtWidgets import QDialog
import genizah_app

class _FakeMainWin:
    """Minimal stand-in for self.main_win attributes SettingsDialog touches."""
    def _on_language_combo_changed(self, idx):
        pass

    def check_updates_manual(self):
        pass

    def run_indexing(self):
        pass

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)

    def __getattr__(self, name):
        return lambda *a, **kw: None

# Patch out heavy parts so SettingsDialog can be constructed without a real GenizahGUI.
fake_cfg = {}
monkeypatch.setattr(genizah_app, 'load_app_config', lambda: dict(fake_cfg))
import genizah_core
monkeypatch.setattr(genizah_core, 'save_app_config', lambda d: fake_cfg.update(d))

sd = genizah_app.SettingsDialog.__new__(genizah_app.SettingsDialog)
# Minimal init attributes needed by SettingsDialog.__init__
# We can't call super().__init__ without a display, so call directly.
QDialog.__init__(sd)
sd.main_win = _FakeMainWin()
```

**How it asserts** (`tests/test_telemetry_consent_ux.py` lines 541-547):
```python
assert hasattr(sd, 'chk_telemetry'), "SettingsDialog must have chk_telemetry attribute"
# State: disabled → unchecked
assert sd.chk_telemetry.isChecked() is False, (
    "chk_telemetry must be unchecked when is_enabled() returns False"
)
del _page  # explicit cleanup
```

**Adaptation for `test_update_ui_coordination.py`:** Use `genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)` (not `SettingsDialog`) since the three coordination methods (`_reset_sidecar_connections`, `_download_next_sidecar`, `_on_sidecar_download_finished`) live on `GenizahGUI`. Stub exactly the `self.*` attributes those methods access: `_sidecar_download_queue`, `_sidecar_data_dir`, `_current_sidecar_download`. Use `unittest.mock.MagicMock` + `patch` for `reset_catalog_filter_sets`, `shared.document_service.reset_pgp_service`, etc. The research-provided template (RESEARCH.md lines 410-424) is the concrete target shape. No `_FakeMainWin` needed — these methods do not touch `self.main_win`. If `QApplication` errors appear during collection, add to `_GUI_TEST_FILES` in `tests/conftest.py`.

---

### `tests/test_no_back_edges_desktop.py` (new AST guard)

**Analog:** `tests/test_no_back_edges_core.py` lines 1-175 — the EXACT template; substitute `genizah_app` for `genizah_core` as the forbidden module name.

**Module registry pattern** (`tests/test_no_back_edges_core.py` lines 25-47):
```python
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent

# Registry: add one entry per phase as modules are extracted (v8.3.0 decomposition).
EXTRACTED_MODULES = [
    "shared/config.py",
    "shared/browse_map_utils.py",
    ...
    "shared/search_engine.py",       # Phase 125d
]
```

**Compound-vs-lazy scope constants** (`tests/test_no_back_edges_core.py` lines 49-70):
```python
# Compound statement types whose bodies run at import time
_IMPORT_TIME_COMPOUND = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.ClassDef,
)
# Python 3.11+ TryStar (ExceptGroup / try*)
if hasattr(ast, "TryStar"):
    _IMPORT_TIME_COMPOUND = _IMPORT_TIME_COMPOUND + (ast.TryStar,)
# Python 3.10+ Match
if hasattr(ast, "Match"):
    _IMPORT_TIME_COMPOUND = _IMPORT_TIME_COMPOUND + (ast.Match,)

# Function bodies are evaluated lazily — stop here
_LAZY_SCOPE = (ast.FunctionDef, ast.AsyncFunctionDef)
```

**`_collect_stmt_lists` traversal function** (`tests/test_no_back_edges_core.py` lines 73-109): copy verbatim — it has no references to the forbidden module name.

**Back-edge detection function** (`tests/test_no_back_edges_core.py` lines 111-150): copy and rename to `_has_module_level_genizah_app_import`; change the module match condition from `"genizah_core"` to `"genizah_app"`.

**Parametrized test** (`tests/test_no_back_edges_core.py` lines 153-175):
```python
@pytest.mark.parametrize("rel_path", EXTRACTED_MODULES)
def test_no_module_level_genizah_core_import(rel_path):
    """GUARD-01 strict: extracted shared/ module must not import genizah_core at module level.

    Modules that are pre-registered in EXTRACTED_MODULES but not yet created
    (e.g., Phase 125 entries registered in 125-01 before 125b/c/d land) are
    skipped with a descriptive message.  Once the file exists, the test
    automatically becomes enforcing with no code change needed.
    """
    path = REPO_ROOT / rel_path
    if not path.exists():
        pytest.skip(
            f"{rel_path} not yet created (pre-registered in Phase 125 Wave 0); "
            "this test will become enforcing automatically once the file exists."
        )
    source = path.read_text(encoding="utf-8")
    violations = _has_module_level_genizah_core_import(source)
    assert not violations, (
        f"{rel_path} imports genizah_core at module level on lines {violations}. "
        "GUARD-01 violation: ..."
    )
```

**Guard self-test cases** (`tests/test_no_back_edges_core.py` lines 214-252): copy `test_guard_catches_top_level_guarded_import` and `test_guard_ignores_lazy_function_body_import` verbatim, adapting the source snippet to use `genizah_app` instead of `genizah_core`.

**Adaptation for `test_no_back_edges_desktop.py`:** Rename the module list to `DESKTOP_MODULES`, populate with the 19 entries from RESEARCH.md lines 198-219. Rename the test function to `test_no_module_level_genizah_app_import`. Rename the detection function to `_has_module_level_genizah_app_import`. Add a specific test that verifies `desktop/join_workbench.py`'s lazy function-body import (line 4135) is NOT flagged (mirrors `test_guard_ignores_lazy_function_body_import`). Pre-register `desktop/update_ui.py` in the list with the same skip-until-exists guard — it will be created in Wave 1 before this guard runs in Wave 3.

---

### `tests/test_genizah_core_facade.py` (new identity assertions)

**Analog:** Identity-assertion blocks in `tests/test_no_back_edges_core.py` lines 178-499+

**Per-module identity assertion pattern** (`tests/test_no_back_edges_core.py` lines 178-187):
```python
def test_config_identity():
    """CONFIG-01: genizah_core.Config is the same class object as shared.config.Config."""
    import shared.config
    import genizah_core

    assert shared.config.Config is genizah_core.Config, (
        "genizah_core.Config is not the same object as shared.config.Config. "
        "The re-export shim in genizah_core.py must be: "
        "from shared.config import Config  # noqa: F401"
    )
```

**Multi-name module pattern** (`tests/test_no_back_edges_core.py` lines 259-280):
```python
def test_browse_map_utils_identity():
    """CORE-06: genizah_core.normalize_shelfmark is the same object as shared.browse_map_utils.normalize_shelfmark."""
    import shared.browse_map_utils
    import genizah_core

    assert shared.browse_map_utils.normalize_shelfmark is genizah_core.normalize_shelfmark, (...)
    assert shared.browse_map_utils.natural_sort_key is genizah_core.natural_sort_key, (...)
    assert shared.browse_map_utils.get_library_display is genizah_core.get_library_display, (...)
    assert shared.browse_map_utils.LIBRARY_CODES is genizah_core.LIBRARY_CODES, (...)
    assert shared.browse_map_utils.dedupe_browse_map is genizah_core.dedupe_browse_map, (...)
```

**Full list of 13 shared modules and their facade names to assert** (extracted from `tests/test_no_back_edges_core.py` lines 178-499+):

| shared module | Names to assert `is` |
|---|---|
| `shared.config` | `Config` |
| `shared.browse_map_utils` | `normalize_shelfmark`, `natural_sort_key`, `get_library_display`, `LIBRARY_CODES`, `dedupe_browse_map` |
| `shared.text_normalize` | `strip_nikud`, `strip_search_diacritics`, `NIKUD_PATTERN`, `COMBINING_DIACRITICALS_PATTERN` |
| `shared.variants` | `VariantManager` |
| `shared.responsa` | `ResponsaComponent`, `parse_responsa_query`, `_apply_explosion_guard`, `_count_expanded_terms`, `GRAMMATICAL_PREFIXES` |
| `shared.codicological` | `CodicologicalManager` |
| `shared.joins_manager` | `JoinsManager` |
| `shared.lists_manager` | `ListsManager` |
| `shared.metadata_manager` | `MetadataManager`, `_BoundedLRUCache`, `MARC_FUTURE_TIMEOUT`, `_NLI_CACHE_MAX_ENTRIES` |
| `shared.indexer` | `Indexer` |
| `shared.lab_settings` | `LabSettings` |
| `shared.lab_engine` | `LabEngine` |
| `shared.search_engine` | `SearchEngine` |

**Adaptation:** The new `test_genizah_core_facade.py` lifts these 13 identity-assertion functions verbatim from `test_no_back_edges_core.py`. The smoke-instantiation tests and guard self-tests stay in the original file. `test_genizah_core_facade.py` is documentation of the permanent contract; the existing `test_no_back_edges_core.py` tests are NOT removed (they serve a different guard purpose). There will be no conflict — both will pass.

---

### `tests/test_privacy_disclosure_strings.py` (modify — OR-location flip)

**Analog:** `tests/test_tabular_builder_rtl.py` lines 19-91 (the OR-location pattern Phase 127 must flip FROM)

**Current OR-location pattern in `test_privacy_disclosure_strings.py`** (lines 66-76):
```python
def test_about_dialog_contains_local_cache_disclosure_en():
    """D-NEW-6: EN About dialog must mention zstd and 'never uploaded'.

    Phase 126 D1 (GUARD-03, additive): SettingsDialog (which owns the About-tab
    HTML) was MOVED from genizah_app.py to desktop/settings_dialogs.py and is
    re-exported via a # noqa: F401 shim. Scan BOTH candidate sources (OR-location)
    so the disclosure is accepted wherever the dialog body lives — NOT flipped to
    new-only (that hard flip is Phase 127's job)."""
    app_src = (REPO_ROOT / "genizah_app.py").read_text(encoding="utf-8")
    dialogs_src = (REPO_ROOT / "desktop" / "settings_dialogs.py").read_text(encoding="utf-8")
    combined = app_src + dialogs_src
    assert "zstd" in combined, (...)
```

**Target pattern after Phase 127 flip (scan `desktop/settings_dialogs.py` only):**
```python
def test_about_dialog_contains_local_cache_disclosure_en():
    """D-NEW-6: EN About dialog must mention zstd and 'never uploaded'.

    Phase 127 final flip: SettingsDialog source is now definitively in
    desktop/settings_dialogs.py (D1 shim retired). Scan new-only location."""
    dialogs_src = (REPO_ROOT / "desktop" / "settings_dialogs.py").read_text(encoding="utf-8")
    assert "zstd" in dialogs_src, (...)
```

**OR-location pattern to KEEP unchanged** (`tests/test_tabular_builder_rtl.py` lines 26-28):
```python
CANDIDATE_TARGETS = [
    _REPO_ROOT / "genizah_app.py",
    _REPO_ROOT / "desktop" / "settings_dialogs.py",
]
```
`test_tabular_builder_rtl.py` is intentionally NOT flipped — it is kept as OR-location per RESEARCH.md directive. Only `test_privacy_disclosure_strings.py` gets the hard flip. The flip covers all three `test_about_dialog_*` functions in the file (EN and HE variants must both be retargeted).

---

## Shared Patterns

### MOVE-and-shim recipe (applies to `desktop/update_ui.py` + genizah_app.py shim line)
**Source:** `desktop/settings_dialogs.py` lines 1-23 and `desktop/ui_widgets.py` lines 1-37
**Apply to:** `desktop/update_ui.py` header, docstring, GUARD-01 note, `from __future__ import annotations`
```python
# -*- coding: utf-8 -*-
"""<description of extracted classes>

Extracted from genizah_app.py (Phase 127 DESK-08) via MOVE-and-shim.
genizah_app.py re-exports these classes for backward compatibility: ...

GUARD-01: NO module-level ``import genizah_app`` — symbols come from
genizah_core (tr, CURRENT_LANG, ...), gui_threads, and PyQt6.
"""
from __future__ import annotations
```

### `__new__` + stub construction (applies to `test_update_ui_coordination.py`)
**Source:** `tests/test_telemetry_consent_ux.py` lines 485-546
**Apply to:** All `_make_gui_coordinator()` helper functions in the new test
```python
import genizah_app
gui = genizah_app.GenizahGUI.__new__(genizah_app.GenizahGUI)
# Stub only the self.* attributes the tested method accesses
gui._sidecar_download_queue = []
gui._sidecar_data_dir = "/fake/data"
gui._current_sidecar_download = None
```

### AST guard scope-aware traversal (applies to `test_no_back_edges_desktop.py`)
**Source:** `tests/test_no_back_edges_core.py` lines 49-175
**Apply to:** Copy verbatim; rename detection function; swap `"genizah_core"` → `"genizah_app"`; swap `EXTRACTED_MODULES` → `DESKTOP_MODULES`

---

## No Analog Found

None. All six files have a close or exact analog in the codebase.

---

## Metadata

**Analog search scope:** `desktop/`, `tests/`, `genizah_app.py`
**Files scanned:** 6 analog files read in full
**Pattern extraction date:** 2026-06-26

**Pitfall notes for planner:**
1. Do NOT delete the D1 import lines 77-78 from `genizah_app.py` — only remove the `# noqa: F401  Phase 126 D1` suffix. All 9 D1 classes are used by `genizah_app.py` itself.
2. Do NOT add `# noqa: F401` to the DESK-08 shim line — the 4 update_ui classes are used by `genizah_app.py` directly.
3. The `test_tabular_builder_rtl.py` OR-location is intentionally NOT flipped in Phase 127 — only `test_privacy_disclosure_strings.py` gets the hard flip.
4. `test_no_back_edges_desktop.py` must verify that `desktop/join_workbench.py` line 4135 (lazy function-body import) is NOT flagged — add a self-test case for this.
5. `test_genizah_core_facade.py` duplicates existing identity assertions from `test_no_back_edges_core.py`; both files should pass; the original is NOT modified.
