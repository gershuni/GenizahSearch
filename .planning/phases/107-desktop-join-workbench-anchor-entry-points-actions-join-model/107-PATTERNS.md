# Phase 107: Desktop Join Workbench — Pattern Map

**Mapped:** 2026-06-04
**Files analyzed:** 7 (4 create, 3 modify)
**Analogs found:** 7 / 7

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `desktop/join_workbench.py` | component / dialog + workers | request-response + event-driven | `desktop/result_dialog.py` + frozen sketch `join_workbench.py.txt` | role-match (no identical modeless shell; sketch = behavior spec) |
| `tests/test_join_workbench.py` | test (unit, Tier-1) | — | `tests/test_joins_lab.py` | exact (same domain, pure-function + AST-guard pattern) |
| `tests/test_join_workbench_no_private.py` | test (AST guard) | — | `tests/test_pgp_filter_cascade.py` / `tests/test_web_library_options_no_local.py` | exact |
| `tests/test_join_workbench_i18n.py` | test (AST guard) | — | `tests/test_pgp_filter_cascade.py` | exact |
| `desktop/result_dialog.py` | component (modify — add entry button) | request-response | self (`desktop/result_dialog.py` existing action pattern) | exact |
| `genizah_app.py` | host app (modify — Browse hook + public wrappers + `_join_workbench` ref) | request-response | self (`genizah_app.py` `_vs_open_joins_with_partner` / `ext_info_row` button additions) | exact |
| `genizah_translations.py` | config / i18n (modify — add 8-9 keys) | — | self (`genizah_translations.py` `TRANSLATIONS.update({...})` block at line 3308) | exact |

---

## Pattern Assignments

---

### `desktop/join_workbench.py` (component, request-response + event-driven)

**Primary analog:** `desktop/result_dialog.py` — image loading, line-numbered text, folio nav, dark-mode, QPalette
**Secondary analog:** `.planning/spikes/002-assisted-join-workbench/sketch/join_workbench.py.txt` — QThread workers, `iiif_full`, `htmlify`, `meta_brief`, `r_sid`/`r_shelf`/`r_text` accessors (behavior spec; discard PyQt scaffolding)
**Tertiary analog:** `corrections_ui.py:3547–3923` `JoinsDialog._get_fjms_joins()` / `_get_pgp_joins()` / `load_joins()` — three-source join load + dedup pattern

#### Imports pattern

From `desktop/result_dialog.py` lines 1-41 (the exact import block to mirror for the new module):

```python
from PyQt6.QtWidgets import (
    QDialog, QHBoxLayout, QLabel, QPushButton, QScrollArea,
    QSplitter, QTextBrowser, QVBoxLayout, QWidget,
    QInputDialog, QMessageBox,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QPalette, QPixmap

from genizah_core import CURRENT_LANG, get_logger, tr
from desktop.image_loader import ImageLoaderThread
from desktop.widgets.line_number_text_edit import apply_line_numbered_text
```

Key difference from `result_dialog.py`: import `QThread, pyqtSignal` (for workers); do NOT import `EnrichMetadataThread` (the workbench uses its own `_AnchorLoadWorker`).

#### Dark-mode detection pattern

**Source:** `desktop/result_dialog.py:562` and `desktop/dialogs_scholarly.py:239`

```python
# In __init__ or init_ui, after self is constructed:
palette = self.palette()
is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
# Then use is_dark to pick QSS hex values:
img_bg = "#374151" if is_dark else "#e2e8f0"
img_placeholder_color = "#9ca3af" if is_dark else "#64748b"
joins_header_color = "#14b8a6" if is_dark else "#0f766e"
```

From `desktop/dialogs_scholarly.py:239-324` — the richer pattern with a color dict:

```python
is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
colors = {
    'muted': '#777' if is_dark else '#999',
    'border': '#444' if is_dark else '#eee',
    'section_bg': '#2d1f3d' if is_dark else '#f3e5f5',
}
```

For the workbench, use the same detection; build an `is_dark: bool` local and pass it into helper methods.

#### QThread worker pattern

**Source:** sketch `join_workbench.py.txt:286-329` (the `_AnchorLoadWorker` and `_PageTextWorker` — these are KEPT AS-IS, behavior is validated):

```python
class _AnchorLoadWorker(QThread):
    """Load anchor image list (enrich_metadata route) + folio text."""
    done = pyqtSignal(dict)

    def __init__(self, wb, sys_id, page, initial=False):
        super().__init__()
        self.wb = wb
        self.sys_id = sys_id
        self.page = page
        self.initial = initial

    def run(self):
        out = {"page": self.page, "initial": self.initial,
               "images": [], "text": "", "total": None}
        try:
            meta = self.wb.meta_mgr.enrich_metadata(self.sys_id) or {}
            out["images"] = meta.get("images_nli") or meta.get("images_ext") or []
        except Exception:
            out["images"] = []
        try:
            bp = self.wb.searcher.get_browse_page(self.sys_id, self.page) or {}
            out["text"] = bp.get("text", "") or ""
            out["total"] = bp.get("total_pages")
        except Exception:
            pass
        self.done.emit(out)


class _PageTextWorker(QThread):
    done = pyqtSignal(str)

    def __init__(self, wb, sid, p):
        super().__init__()
        self.wb = wb; self.sid = sid; self.p = p

    def run(self):
        txt = ""
        try:
            txt = (self.wb.searcher.get_browse_page(self.sid, self.p) or {}).get("text", "") or ""
        except Exception:
            txt = ""
        self.done.emit(txt)
```

**ThumbBatchWorker** — no direct analog in the sketch (sketch has `ThumbResolver` which fires one thread per card). The production version is a single QThread that iterates the sys_id list and emits per-item signals (D-10/D-18 batch constraint). Model it on the `_AnchorLoadWorker` pattern with `progress = pyqtSignal(int, str, object)` (index, sys_id, QPixmap-or-None):

```python
class ThumbBatchWorker(QThread):
    resolved = pyqtSignal(int, object)   # (row_index, QPixmap or None)
    _cancel = False

    def __init__(self, wb, sys_ids: list):
        super().__init__()
        self.wb = wb
        self.sys_ids = sys_ids

    def cancel(self): self._cancel = True

    def run(self):
        for i, sid in enumerate(self.sys_ids):
            if self._cancel:
                return
            url = None
            try:
                url = self.wb.meta_mgr.get_thumbnail(sid, size=320)
            except Exception:
                pass
            # load image if url is not None
            pixmap = None
            if url:
                # Use ImageLoaderThread inline: load synchronously in this worker
                # (we are already off-UI thread; simpler than nesting threads)
                try:
                    import requests, genizah_core
                    from PyQt6.QtGui import QImage, QPixmap as _QPixmap
                    resp = requests.get(url, headers=genizah_core.Config.HTTP_HEADERS,
                                        timeout=5, verify=False)
                    if resp.status_code == 200:
                        img = QImage.fromData(resp.content)
                        if not img.isNull():
                            pixmap = _QPixmap.fromImage(img)
                except Exception:
                    pass
            self.resolved.emit(i, pixmap)
```

Note: The simpler alternative — fire one `ImageLoaderThread` per join row sequentially and connect signals — is also acceptable; the key constraint (D-10) is "no per-row serial dispatch from the UI thread". Either pattern satisfies it as long as the batch starts off the UI thread.

#### Pure helper functions — transplant from sketch verbatim

**Source:** sketch `join_workbench.py.txt:61-157`

```python
# result dict accessors (keep exactly as-is — verified)
def r_sid(res):
    return (res.get("display") or {}).get("id") or res.get("sys_id") or ""

def r_shelf(res):
    return (res.get("display") or {}).get("shelfmark") or res.get("shelfmark") or res.get("uid") or "?"

def r_title(res):
    return (res.get("display") or {}).get("title") or ""

def r_text(res):
    return res.get("full_text") or res.get("text") or ""

def r_lib(res):
    d = res.get("display") or {}
    return d.get("library_code") or d.get("library") or ""

def iiif_full(base_url, width=2000):
    if not base_url:
        return ""
    if base_url.endswith(".jpg"):
        return base_url
    return f"{base_url}/full/{width},/0/default.jpg"

def htmlify(text, pattern=None):
    """Escape + newlines to <br> + optional regex highlight. RTL."""
    text = text or ""
    MARK_A, MARK_B = "\x01", "\x02"
    if pattern:
        import re, html as _html
        try:
            rx = re.compile(pattern, re.IGNORECASE | re.MULTILINE)
            text = rx.sub(lambda m: MARK_A + m.group(0) + MARK_B, text)
        except re.error:
            pass
    import html as _html
    t = _html.escape(text)
    t = t.replace("\n", "<br>")
    t = t.replace(MARK_A, "<b style='color:#dc2626'>").replace(MARK_B, "</b>")
    return f"<div dir='rtl' style='text-align:right'>{t}</div>"
```

**WHAT TO DROP from the sketch:** `meas_for`, `vs_score`, `material_str`, `ThumbResolver` (replaced by `ThumbBatchWorker`), `CandidateCard` (Phase 108), `QueryBuilder` (Phase 108), `CompareDialog` (Phase 108), `_CrossSideWorker` (Phase 108), `_VsLoadWorker` (Phase 109). The result-accessors and `iiif_full`/`htmlify` transplant unchanged. Also note: `shared/joins_lab.py` (Phase 106) already exports `htmlify`, `page_of`, `snippet_html`, `snippet_plain` — import from there instead of redefining locally (D-18).

#### `meta_brief` helper

**Source:** sketch — not in the first 200 lines; defined later. Reconstruct from behavior: `library · img_count · title (truncated)`. The workbench uses `enrich_metadata` output:

```python
def meta_brief(meta: dict, shelfmark: str = "") -> str:
    """One-line summary: library · N images · title."""
    lib = meta.get("library_code", "") or ""
    n_img = len(meta.get("images_nli") or meta.get("images_ext") or [])
    title = (meta.get("title") or "")[:60]
    parts = [p for p in [lib, f"{n_img} {tr('img')}" if n_img else "", title] if p]
    return " · ".join(parts)
```

#### Source badge mapping (D-09)

Pure dict lookup — no analog in existing codebase; implement directly:

```python
_BADGE_CONFIG = {
    "PGP":       (tr("PGP"),       "#0ea5e9", "#38bdf8"),   # (label, light_bg, dark_bg)
    "FJMS":      (tr("FJMS"),      "#8b5cf6", "#a78bfa"),
    "user":      (tr("User"),      "#10b981", "#34d399"),
    "community": (tr("Community"), "#10b981", "#34d399"),
}
_BADGE_FALLBACK = (tr("Known join"), "#6b7280", "#9ca3af")

def badge_for_source(source: str, is_dark: bool) -> tuple[str, str]:
    """Return (label, hex_color) for a join source string."""
    cfg = _BADGE_CONFIG.get(source or "", _BADGE_FALLBACK)
    return cfg[0], cfg[2] if is_dark else cfg[1]
```

#### Known-joins dedup pattern

**Source:** `corrections_ui.py:3607` `_merge_fjms_joins_into_display` — dedup by (fragment_a_upper, fragment_b_upper) pair:

```python
def dedup_join_rows(join_lists: list[list]) -> list:
    """Merge join dicts from multiple sources, dedup by (a_upper, b_upper) pair.
    Later sources win on badge/source if the pair already exists."""
    seen: dict[tuple, dict] = {}
    for joins in join_lists:
        for j in joins:
            a = (j.get("fragment_a") or "").upper().strip()
            b = (j.get("fragment_b") or "").upper().strip()
            key = (min(a, b), max(a, b))
            if key not in seen:
                seen[key] = j
            # else: first source wins (PGP > FJMS > user order assumed by call order)
    return list(seen.values())
```

#### apply_line_numbered_text call for anchor pane

**Source:** `desktop/result_dialog.py:1368-1370` + `RESEARCH.md` verified signature:

```python
from desktop.widgets.line_number_text_edit import apply_line_numbered_text

# In _on_anchor_text_loaded(text):
apply_line_numbered_text(
    self.anchor_text_browser,         # QTextBrowser instance
    htmlify(text, pattern=None),      # RTL div wrapper — no highlight for anchor view
    source_text=text,                 # raw text for line counting
    is_html=True,
)
```

Do NOT pass `pages=` for the anchor (single-fragment, no multi-volume pagination).

#### QLabel deleted-widget guard

**Source:** sketch (comment in DESKTOP-INTEGRATION-NOTES) — guard all QLabel writes in slots:

```python
# In image_loaded / load_failed signal slots:
try:
    self.anchor_img_label.setPixmap(scaled_pixmap)
except RuntimeError:
    pass  # Label was deleted (dialog closed while thread was running)
```

#### Window lifecycle and single-instance pattern

**Source:** sketch + CONTEXT D-01/D-02. No direct analog for modeless single-instance in the codebase (all existing dialogs are `exec()`). Mirror the puzzle window pattern from `genizah_app.py:15362`:

```python
# In GenizahGUI.open_joins_workbench(res):
if self._join_workbench is None or not self._join_workbench.isVisible():
    self._join_workbench = JoinWorkbenchWindow(self, self)
self._join_workbench.set_anchor(res)
self._join_workbench.show()
self._join_workbench.raise_()
self._join_workbench.activateWindow()
```

On close, cancel in-flight workers:

```python
def closeEvent(self, event):
    for w in (self._anchor_worker, self._page_text_worker, self._thumb_worker):
        if w and w.isRunning():
            try: w.cancel()
            except Exception: pass
    super().closeEvent(event)
```

#### Modeless QDialog + `set_anchor` swap

The workbench is `QDialog` with `setModal(False)`, opened via `show()`. `set_anchor(res)` is the re-anchor entry point (D-02):

```python
class JoinWorkbenchWindow(QDialog):
    def __init__(self, parent, app):
        super().__init__(parent)
        self.setModal(False)
        self._app = app
        self.meta_mgr = app.meta_mgr
        self.searcher = app.searcher
        self.joins_mgr = app.joins_mgr
        self._join_workbench = None  # not needed on self; lives on app
        self._anchor_sid = None
        self._anchor_images = []
        self._anchor_idx = 0
        self._zoom = 1.0
        self._anchor_full_pix = None
        self._anchor_worker = None
        self._page_text_worker = None
        self._thumb_worker = None
        palette = self.palette()
        self.is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
        self._init_ui()
        self.setWindowTitle(tr("Join Workbench"))
        self.setMinimumSize(900, 680)
        self.resize(1000, 720)

    def set_anchor(self, res: dict):
        """Set or replace the anchor fragment. Cancels any in-flight workers."""
        # cancel existing workers
        ...
        self._anchor_sid = r_sid(res)
        # update shelfmark / meta labels
        shelf = r_shelf(res)
        self.anchor_shelf.setText(shelf)
        page = (res.get("display") or {}).get("img") or 1
        self._anchor_idx = max(0, int(page) - 1)
        # fire _AnchorLoadWorker
        self._start_anchor_load(page=page, initial=True)
```

---

### `tests/test_join_workbench.py` (unit test, Tier-1)

**Analog:** `tests/test_joins_lab.py` lines 1-137 — pure function tests without QApplication, class-per-domain structure, `_make_result` helper

**Import pattern** (from `test_joins_lab.py` lines 1-29):

```python
# -*- coding: utf-8 -*-
import ast
import pathlib
import pytest

# Import only pure functions — no QApplication needed
from desktop.join_workbench import (
    iiif_full,
    htmlify,
    meta_brief,
    badge_for_source,
    dedup_join_rows,
    r_sid, r_shelf, r_title, r_text,
)
```

**Test structure** (mirrors `test_joins_lab.py:85-137`):

```python
class TestIiifFull:
    def test_nli_base_url_appends_path(self):
        url = "https://www.nli.org.il/en/image/NNL_ALEPH001234567/FL12345678"
        assert iiif_full(url) == url + "/full/2000,/0/default.jpg"

    def test_direct_jpg_returned_unchanged(self):
        assert iiif_full("https://cudl.lib.cam.ac.uk/img/1.jpg") == "https://cudl.lib.cam.ac.uk/img/1.jpg"

    def test_empty_returns_empty(self):
        assert iiif_full("") == ""
        assert iiif_full(None) == ""

    def test_custom_width(self):
        url = "https://nli.org.il/FL9999"
        assert iiif_full(url, width=400) == url + "/full/400,/0/default.jpg"


class TestBadgeForSource:
    def test_pgp_light(self):
        label, color = badge_for_source("PGP", is_dark=False)
        assert label == "PGP"
        assert color == "#0ea5e9"

    def test_fjms_dark(self):
        label, color = badge_for_source("FJMS", is_dark=True)
        assert label == "FJMS"
        assert color == "#a78bfa"

    def test_unknown_source_gets_fallback(self):
        label, color = badge_for_source("mystery", is_dark=False)
        assert "join" in label.lower() or "צירוף" in label  # generic fallback


class TestDedupJoinRows:
    def test_identical_pair_deduped(self):
        j1 = {"fragment_a": "T-S 12.1", "fragment_b": "T-S 12.2", "source": "PGP"}
        j2 = {"fragment_a": "T-S 12.2", "fragment_b": "T-S 12.1", "source": "FJMS"}
        result = dedup_join_rows([[j1], [j2]])
        assert len(result) == 1

    def test_distinct_pairs_kept(self):
        j1 = {"fragment_a": "T-S 12.1", "fragment_b": "T-S 12.2", "source": "PGP"}
        j2 = {"fragment_a": "T-S 12.1", "fragment_b": "T-S 12.3", "source": "FJMS"}
        result = dedup_join_rows([[j1], [j2]])
        assert len(result) == 2
```

---

### `tests/test_join_workbench_no_private.py` (AST guard — no `_vs_*` calls)

**Analog:** `tests/test_pgp_filter_cascade.py` lines 1-66 (AST walk + `_function_contains_call` pattern) AND `tests/test_web_library_options_no_local.py` lines 1-119 (file-level AST scan + offender list)

**Full pattern to copy** (blend of the two):

```python
"""Static AST guard: desktop/join_workbench.py must not call any _vs_* private methods.

SC#5 invariant: all actions in the Join Workbench go through public wrappers
(open_anchor_in_puzzle, open_anchor_as_join, etc.). A _vs_* call would couple the
workbench to GenizahGUI private internals.

Pattern source: tests/test_pgp_filter_cascade.py (AST scanner) and
tests/test_web_library_options_no_local.py (file-level offender pattern).
"""
import ast
import pathlib

TARGET = pathlib.Path(__file__).parent.parent / "desktop" / "join_workbench.py"


def _iter_calls(tree):
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            callee = node.func
            if isinstance(callee, ast.Attribute):
                yield callee.attr, node.lineno
            elif isinstance(callee, ast.Name):
                yield callee.id, node.lineno


def test_no_vs_private_calls_in_join_workbench():
    """SC#5: join_workbench.py must not call _vs_* private methods directly."""
    source = TARGET.read_text(encoding="utf-8")
    tree = ast.parse(source)
    offenders = [
        (name, lineno)
        for name, lineno in _iter_calls(tree)
        if name.startswith("_vs_")
    ]
    assert not offenders, (
        "SC#5 violation — desktop/join_workbench.py calls _vs_* private methods:\n"
        + "\n".join(f"  {name}() at line {lineno}" for name, lineno in offenders)
        + "\n\nFix: call the public wrapper (open_anchor_in_puzzle / open_anchor_as_join) instead."
    )


def test_target_file_exists():
    assert TARGET.exists(), f"desktop/join_workbench.py not found at {TARGET}"
```

---

### `tests/test_join_workbench_i18n.py` (AST guard — all `tr()` keys present in TRANSLATIONS)

**Analog:** `tests/test_pgp_filter_cascade.py` (AST walk pattern). Combine with a direct import of TRANSLATIONS:

```python
"""Static AST guard: every tr() call in desktop/join_workbench.py has a corresponding
key in genizah_translations.TRANSLATIONS.

SC#6 invariant: all strings in the workbench are bilingual from the first line.
If a new string is added via tr() but not added to TRANSLATIONS, this test fails CI.

Pattern source: tests/test_pgp_filter_cascade.py AST scanner.
"""
import ast
import pathlib

TARGET = pathlib.Path(__file__).parent.parent / "desktop" / "join_workbench.py"


def _extract_tr_keys(source: str) -> list[tuple[str, int]]:
    """Return (key_string, lineno) for every tr("...") literal call in source."""
    tree = ast.parse(source)
    keys = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "tr"
            and node.args
            and isinstance(node.args[0], ast.Constant)
            and isinstance(node.args[0].value, str)
        ):
            keys.append((node.args[0].value, node.lineno))
    return keys


def test_all_tr_keys_in_translations():
    """SC#6: every tr() key in join_workbench.py must be in TRANSLATIONS."""
    from genizah_translations import TRANSLATIONS

    source = TARGET.read_text(encoding="utf-8")
    keys = _extract_tr_keys(source)
    missing = [
        (key, lineno)
        for key, lineno in keys
        if key not in TRANSLATIONS
    ]
    assert not missing, (
        "SC#6 violation — tr() keys in desktop/join_workbench.py not in TRANSLATIONS:\n"
        + "\n".join(f"  {key!r} (line {lineno})" for key, lineno in missing)
        + "\n\nFix: add the key to genizah_translations.TRANSLATIONS (use TRANSLATIONS.update({...}) at the end)."
    )


def test_target_file_exists():
    assert TARGET.exists(), f"desktop/join_workbench.py not found at {TARGET}"
```

---

### `desktop/result_dialog.py` (modify — add "Find joins" entry button)

**Analog (exact):** existing ResultDialog compact action row at `desktop/result_dialog.py:189-196`:

```python
# Existing compact joins button (line 189-196) — mirror this pattern
self.btn_compact_joins = QToolButton()
self.btn_compact_joins.setText("🔗")
self.btn_compact_joins.setToolTip(tr("View joined fragments"))
self.btn_compact_joins.setFixedSize(40, 32)
self.btn_compact_joins.setStyleSheet("background-color: #95a5a6; color: white; border-radius: 4px;")
self.btn_compact_joins.setPopupMode(QToolButton.ToolButtonPopupMode.MenuButtonPopup)
self.btn_compact_joins.clicked.connect(self._rd_view_joins)
```

**What to add** — a new "Find joins" button on the action row. The action row in ResultDialog uses `QPushButton` (not `QToolButton`) for its buttons. Add:

```python
# In ResultDialog.init_ui() in the action-row section (after existing action buttons):
self.btn_rd_find_joins = QPushButton(f"🔗 {tr('Find joins')}")
self.btn_rd_find_joins.setToolTip(tr("Find joins"))
self.btn_rd_find_joins.clicked.connect(self._open_join_workbench)
# add to the action row layout where other action buttons are placed
```

And the callback:

```python
def _open_join_workbench(self):
    """Entry point: open/re-anchor the Join Workbench with the live page state."""
    res = self.all_results[self.current_result_idx]
    # Supplement with live page state if different from result page
    if self.current_sys_id:
        res = dict(res)
        res.setdefault("display", {})
        res["display"] = dict(res["display"])
        res["display"]["id"] = self.current_sys_id
        res["display"]["img"] = self.current_p_num or res["display"].get("img")

    app = self._app  # GenizahGUI instance (set as self._app = parent in __init__)
    if hasattr(app, "open_joins_workbench"):
        app.open_joins_workbench(res)
    self.close()    # ResultDialog closes after launching workbench (iteration B behavior)
```

---

### `genizah_app.py` (modify — Browse hook + public wrappers + instance ref)

#### Browse `ext_info_row` entry hook

**Analog (exact):** `genizah_app.py:6937-6940` `btn_b_add_to_puzzle` pattern in `create_browse_tab()`:

```python
# Existing analog (lines 6937-6940):
self.btn_b_add_to_puzzle = QPushButton(f"\U0001f9e9 {tr('Puzzle')}")
self.btn_b_add_to_puzzle.setToolTip(tr("Add current manuscript to Fragment Puzzle"))
self.btn_b_add_to_puzzle.setEnabled(False)
self.btn_b_add_to_puzzle.clicked.connect(self._browse_add_to_puzzle)

# New button to add (same pattern):
self.btn_b_find_joins = QPushButton(f"🔗 {tr('Find joins')}")
self.btn_b_find_joins.setToolTip(tr("Find joins"))
self.btn_b_find_joins.setEnabled(False)   # enabled in _browse_loaded() when sys_id is set
self.btn_b_find_joins.clicked.connect(self._browse_open_join_workbench)
```

Then in `ext_info_row` assembly (analog: `genizah_app.py:7023`):

```python
ext_info_row.addWidget(self.btn_b_find_joins)   # add after btn_b_add_to_puzzle
```

And the callback:

```python
def _browse_open_join_workbench(self):
    """Browse tab entry point for the Join Workbench."""
    sid = getattr(self, "current_browse_sid", None)
    if not sid:
        return
    p = getattr(self, "p", 1) or 1
    text = getattr(self, "browse_original_text", "") or ""
    shelf = ""
    try:
        shelf, _ = self.meta_mgr.get_meta_for_id(sid)
    except Exception:
        pass
    res = {
        "display": {"id": sid, "shelfmark": shelf, "img": p, "library_code": "", "title": ""},
        "full_text": text,
        "uid": f"{sid}_P{p:03d}",
    }
    self.open_joins_workbench(res)
    # Browse tab does NOT close (it is a persistent tab)
```

Enable/disable the button when browse loads a manuscript:

```python
# In _browse_loaded() or wherever btn_b_add_to_puzzle.setEnabled(True) is called:
if hasattr(self, "btn_b_find_joins"):
    self.btn_b_find_joins.setEnabled(True)
```

#### `open_joins_workbench` host method + `_join_workbench` instance ref

**Analog:** `genizah_app.py:15362` `add_to_puzzle` (opens/raises the puzzle window and keeps a ref):

```python
def open_joins_workbench(self, res: dict):
    """Open (or re-anchor) the Join Workbench for the given result.

    D-01: modeless, opens with show().
    D-02: single reusable instance — second call re-anchors.
    """
    from desktop.join_workbench import JoinWorkbenchWindow  # lazy import (desktop-only)
    if self._join_workbench is None or not self._join_workbench.isVisible():
        self._join_workbench = JoinWorkbenchWindow(self, self)
    self._join_workbench.set_anchor(res)
    self._join_workbench.show()
    self._join_workbench.raise_()
    self._join_workbench.activateWindow()
```

Add `self._join_workbench = None` to `GenizahGUI.__init__` (or `on_startup_finished`).

#### `open_anchor_in_puzzle` public wrapper

**Analog (exact):** `genizah_app.py:5261-5272` `_vs_add_to_puzzle` — the wrapper is a simple rename:

```python
def open_anchor_in_puzzle(self, sys_id: str):
    """Public: add a fragment to the Fragment Puzzle canvas (Join Workbench path).

    SC#5: workbench calls this instead of _vs_add_to_puzzle directly.
    """
    self._vs_add_to_puzzle(sys_id)
```

#### `open_anchor_as_join` public wrapper

**Analog (exact):** `genizah_app.py:5239-5259` `_vs_open_joins_with_partner` — anchor-only variant (no `frag_b_input.setText()`):

```python
def open_anchor_as_join(self, anchor_sys_id: str, anchor_shelfmark: str):
    """Public: open JoinsDialog with anchor as Fragment A; scholar enters B freely.

    SC#5: workbench calls this instead of _vs_open_joins_with_partner.
    D-14: JoinsDialog reused pre-filled, partner B left empty (free entry).
    """
    def browse_shelfmark(target_shelfmark):
        self.browse_shelf_input.setText(target_shelfmark)
        self._set_last_browse_field("shelf")
        self.browse_load()

    from corrections_ui import JoinsDialog
    dialog = JoinsDialog(
        self, self.corrections_client,
        document_id=anchor_sys_id,
        shelfmark=anchor_shelfmark,
        on_browse=browse_shelfmark,
        shelf_model=getattr(self, 'shelf_model', None),
        joins_mgr=getattr(self, 'joins_mgr', None),
        shelf_completer=getattr(self, 'shelf_completer', None),
        lists_mgr=getattr(self, 'lists_mgr', None),
        meta_mgr=self.meta_mgr,
    )
    # frag_b_input left EMPTY — scholar enters B freely (R-02 confirmed)
    dialog.exec()
    # SC#4: after dialog closes, workbench must call _reload_known_joins()
    # The workbench handles this by connecting to the JoinsDialog.finished signal
    # or by the caller's button-click handler calling _reload_known_joins() after
    # open_anchor_as_join returns. open_anchor_as_join is synchronous (exec() blocks).
```

---

### `genizah_translations.py` (modify — add new i18n keys)

**Analog (exact):** `genizah_translations.py:3308` — `TRANSLATIONS.update({...})` at end of file:

```python
# Existing block at line 3308 (the pattern to append to):
TRANSLATIONS.update({
    # --- Lists / Projects (web project_tree, ...) ---
    "(No project - standalone)": "(ללא פרויקט - עצמאי)",
    ...
})
```

**New block to add** (append after the existing `TRANSLATIONS.update` at line 3308+):

```python
# === Phase 107 — Join Workbench i18n ===
TRANSLATIONS.update({
    "Find joins":       "מצא צירופים",
    "Join Workbench":   "מעבדת צירופים",
    "ANCHOR":           "עוגן",
    "Known Joins":      "צירופים ידועים",
    "Make anchor":      "הגדר כעוגן",
    "Add as Join":      "הוסף כצירוף",
    "Known join":       "צירוף ידוע",       # generic badge fallback (D-09)
    "Open fragment":    "פתח קטע",           # REC-1: more specific than bare "Open"
    # "Enter shelfmark…": "הזן סימת מדף…",  # verify if an existing key matches; add if not
})
```

**Existing keys confirmed present (do NOT re-add):** `"Browse manuscript"` (line 103), `"Add to Puzzle"` (line 850), `"Add to List"` (line 849), `"User"` (line 2039), `"Community"` (lines 1011, 1469), `"No image"` (line 882).

**Key to verify before planning Wave 0:** `"Enter shelfmark…"` — multiple near-identical variants exist (lines 81, 295, 868, 1868, 2873); during plan, pick closest or add the ellipsis-period form. `"img"` (for `meta_brief` "N img") — likely not present as a bare key; add if needed.

---

## Shared Patterns

### Dark-mode detection (apply to `JoinWorkbenchWindow.__init__`)

**Source:** `desktop/result_dialog.py:562` + `desktop/dialogs_scholarly.py:239`

```python
palette = self.palette()
is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
```

Set once in `__init__` and store as `self.is_dark`. Pass to all `setStyleSheet` calls that pick light/dark hex variants. Do NOT call `self.palette()` on every repaint.

### QThread worker guard (apply to all `_AnchorLoadWorker`, `_PageTextWorker`, `ThumbBatchWorker` slot-side code)

**Source:** sketch comment in DESKTOP-INTEGRATION-NOTES + corrections_ui.py pattern

```python
try:
    widget.setText(value)   # or setPixmap / setVisible
except RuntimeError:
    pass  # widget deleted while thread was running
```

### `tr()` usage

**Source:** `genizah_app.py:17063` — import at module top via `from genizah_core import ... tr`:

```python
from genizah_core import CURRENT_LANG, get_logger, tr
```

Every string literal visible to the user MUST be wrapped: `tr("Find joins")`, `tr("ANCHOR")`, etc. Template strings use `.format()`: `f"{tr('Known Joins')} ({count})"`.

### Three-source join load (apply to `_reload_known_joins` worker)

**Source:** `corrections_ui.py:3830-3923` `JoinsDialog.load_joins()` — the established call pattern:

```python
# Call order determines badge priority (first = wins on dedup):
# 1. User joins via JoinsManager
cached = joins_mgr.get_connected_fragments_by_id(anchor_sid)
user_joins = (cached or {}).get("joins", [])

# 2. PGP joins via shared.document_service
from shared.document_service import get_document_for_fragment, get_fragments_for_document
# ... (see _get_pgp_joins pattern above)

# 3. FJMS joins via shared.fjms_service
from shared.fjms_service import get_fjms_service
fjms_svc = get_fjms_service()
fjms_members = fjms_svc.get_join_group(anchor_sid) if fjms_svc.is_available() else []
# ... (see _get_fjms_joins pattern above)

# Dedup merged list
all_joins = dedup_join_rows([user_joins, pgp_joins, fjms_joins])
```

This MUST run off the UI thread (in a QThread worker). All three shared services manage their own thread-local connections — safe to call from QThread.

---

## No Analog Found

No files in this phase lack an analog. All patterns have close code matches.

---

## Metadata

**Analog search scope:** `desktop/`, `corrections_ui.py`, `genizah_app.py`, `genizah_translations.py`, `tests/`, `.planning/spikes/002-assisted-join-workbench/sketch/`
**Files scanned (analog read):** 12 source files + 4 test files + frozen sketch
**Pattern extraction date:** 2026-06-04

### Pitfalls to call out in every plan action

1. **FL-substituted thumbnail trap** (D-05): anchor image MUST use `enrich_metadata` → `images_nli`/`images_ext` → `iiif_full()`, never `get_thumbnail()`. Confirmed at `genizah_core.py:4476,4518`.
2. **UI methods on UI thread only**: `open_anchor_in_puzzle`, `open_anchor_as_join`, `open_result_in_browse_from_table`, `show_add_to_list_menu` are button-click handlers — already on UI thread. Never call them from a QThread worker.
3. **Known-joins refresh after JoinsDialog.exec()**: `exec()` is synchronous and blocks until dialog closes. The workbench's `_reload_known_joins()` must be called AFTER `exec()` returns (or via `dialog.finished` signal).
4. **Three-source load required**: calling only `JoinsManager.get_connected_fragments_by_id` misses PGP and FJMS joins — the two most authoritative sources.
5. **`get_browse_page` returns None** for untranscribed fragments: guard with `bp = searcher.get_browse_page(sid, page) or {}`.
