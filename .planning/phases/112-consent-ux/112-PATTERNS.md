# Phase 112: Consent UX — Pattern Map

**Mapped:** 2026-06-14
**Files analyzed:** 4 (2 new, 2 modified)
**Analogs found:** 4 / 4

---

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `desktop/consent_dialog.py` | component (dialog) | request-response (modal exec) | `genizah_app.py:284` `WhatsNewDialog` + `genizah_app.py:1447` `HelpDialog` | role-match (bilingual + QTextBrowser) |
| `desktop/telemetry.py` (stub fill) | service (engine API) | request-response | `desktop/telemetry.py:399` `set_consent()` pattern already in file | exact (same file) |
| `genizah_app.py` (SettingsDialog + startup) | controller (UI wiring) | request-response | `genizah_app.py:2238` `chk_notifications` row + `genizah_app.py:15621` `_show_citation_reminder` | exact (same file) |
| `tests/test_telemetry_consent_ux.py` | test | headless + Qt-offscreen | `tests/test_telemetry_consent_gate.py` (autouse fixture) + `tests/test_telemetry_no_direct_posthog.py` (AST guard) | exact |

---

## Pattern Assignments

### `desktop/consent_dialog.py` (NEW — component, request-response modal)

**Analogs:**
- `genizah_app.py:284` — `WhatsNewDialog` (QDialog structure, windowFlags, setFixedSize, button layout)
- `genizah_app.py:1447` — `HelpDialog` (QTextBrowser, setOpenExternalLinks, setHtml, bilingual content loading)

**CRITICAL constraint:** `consent_dialog.py` MUST NOT import `shared.posthog_server`. All consent writes go through `desktop.telemetry.set_consent()`. The PRIV-03 AST guard in `tests/test_telemetry_no_direct_posthog.py` scans all `desktop/*.py` and will fail CI if violated.

---

#### Imports pattern (copy from WhatsNewDialog's enclosing file scope)

From `genizah_app.py` imports block (top of file):
```python
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel,
    QTextBrowser, QApplication,
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QPalette
from genizah_core import save_app_config
from desktop import telemetry
from desktop.telemetry import FIRST_RUN_SHOWN_KEY
```

Note: `desktop/consent_dialog.py` is a new file — use these imports. Do NOT import `shared.posthog_server`.

---

#### QDialog structure pattern (lines 284-343 — WhatsNewDialog)

```python
# genizah_app.py:284-343
class WhatsNewDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle(tr("New Features!"))
        self.setModal(True)
        self.setFixedSize(500, 440)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint)
        if CURRENT_LANG == 'he':
            self.setLayoutDirection(Qt.LayoutDirection.RightToLeft)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(16)
        ...
        btn_ok = QPushButton(tr("Got it!"))
        btn_ok.setStyleSheet("background-color: #10b981; color: white; ...")
        btn_ok.clicked.connect(self.accept)
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        btn_layout.addWidget(btn_ok)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)
```

**Divergence for ConsentDialog:** Do NOT use `setLayoutDirection` on the consent dialog (both languages shown regardless of `CURRENT_LANG` — D-01). Do NOT set a default button. Override `keyPressEvent` to treat Enter/Return as decline.

---

#### QTextBrowser pattern for bilingual HTML (lines 1447-1508 — HelpDialog)

```python
# genizah_app.py:1447-1508
class HelpDialog(QDialog):
    def __init__(self, parent, title, source_path=None, anchor=None, fallback_html="", lang="en"):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(900, 700)
        layout = QVBoxLayout()
        self.text = QTextBrowser()
        self.text.setOpenExternalLinks(True)
        layout.addWidget(self.text)
        self._load_content(source_path, anchor, fallback_html, lang)
        btn = QPushButton(tr("Close"))
        btn.clicked.connect(self.close)
        layout.addWidget(btn)
        self.setLayout(layout)
```

HelpDialog strips one language block using `<!-- START_LANG_HE -->` / `<!-- END_LANG_HE -->` markers. **PrivacyDialog does NOT strip** — it renders both EN and HE blocks always (D-02). Use `<div dir='rtl'>` for the Hebrew block inside the HTML so QTextBrowser renders Hebrew RTL without applying RTL to the whole dialog frame.

---

#### No-default button pattern (from RESEARCH.md — concrete implementation)

This pattern does NOT exist yet — it is new for Phase 112. Copy exactly:

```python
# ConsentDialog: equal-weight buttons — NO default on either
btn_enable = QPushButton("Enable / הפעל")
btn_enable.setDefault(False)
btn_enable.setAutoDefault(False)
btn_enable.clicked.connect(self._on_enable)

btn_decline = QPushButton("Not now / לא עכשיו")
btn_decline.setDefault(False)
btn_decline.setAutoDefault(False)
btn_decline.clicked.connect(self._on_decline)

def keyPressEvent(self, event):
    # Enter/Return without clicking = implicit decline (D-05, SC#1)
    if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
        self._on_decline()
        return
    super().keyPressEvent(event)
```

---

#### Shown-flag write pattern (all exit paths — D-05)

Write `FIRST_RUN_SHOWN_KEY=True` unconditionally on accept, decline, AND close-via-X:

```python
def _write_shown_flag(self):
    from genizah_core import save_app_config
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY
    save_app_config({FIRST_RUN_SHOWN_KEY: True})

def _on_enable(self):
    from desktop import telemetry
    telemetry.set_consent(True)
    self._write_shown_flag()
    self.accept()

def _on_decline(self):
    from desktop import telemetry
    telemetry.set_consent(False)
    self._write_shown_flag()
    self.reject()

def closeEvent(self, event):
    # X-button = implicit decline
    from desktop import telemetry
    telemetry.set_consent(False)
    self._write_shown_flag()
    super().closeEvent(event)
```

---

#### Palette-aware colors pattern (lines 2167-2176 — SettingsDialog.__init__)

```python
# genizah_app.py:2167-2176
pal = QApplication.palette()
self._is_dark = pal.color(QPalette.ColorRole.Window).lightness() < 128
self._text = pal.color(QPalette.ColorRole.Text).name()
self._base = pal.color(QPalette.ColorRole.Base).name()
self._muted = '#888' if self._is_dark else '#666'
self._border = '#555' if self._is_dark else '#d0d0d0'
```

Apply the same palette detection in `PrivacyDialog.__init__` before building the HTML string, so the disclosure text respects the OS dark/light theme.

---

### `desktop/telemetry.py` — `show_first_run_prompt()` stub fill (line 712)

**Analog:** The stub itself at line 712 (same file). The surrounding module pattern governs.

**Stub location** (lines 712-717):

```python
# desktop/telemetry.py:712-717 — current no-op stub
def show_first_run_prompt() -> None:
    """Display the first-run consent prompt. Implemented in Phase 112.

    No-op in Phase 111. Never raises.
    """
    # Phase 112 implementation
```

**Implementation pattern to copy:** The function must remain no-raise (consistent with every other public callable in the module — the docstring says "Never raises"). Wrap the entire body in `try/except Exception`. Import `ConsentDialog` lazily inside the function body (avoids circular import risk and keeps PyQt6 out of the module-level import for headless test runs):

```python
def show_first_run_prompt() -> None:
    """Display the first-run consent prompt. Never raises."""
    try:
        cfg = load_app_config()
        if cfg.get(FIRST_RUN_SHOWN_KEY, False):
            return  # already shown — D-05 gate
        from desktop.consent_dialog import ConsentDialog
        dlg = ConsentDialog()   # parent=None is safe; parent passed from call site
        dlg.exec()
    except Exception:
        logger.debug('telemetry: show_first_run_prompt failed', exc_info=True)
```

Note: the call site in `genizah_app.py` passes `parent=self` — planner should decide whether `show_first_run_prompt(parent=None)` accepts an optional `parent` kwarg or whether `ConsentDialog` is constructed with the main window reference available via another mechanism.

---

### `genizah_app.py` — SettingsDialog modifications

#### A. `_config_snapshot` snapshot exemption (line 2179 — D-07b)

**Current code** (line 2179):
```python
# genizah_app.py:2179
self._config_snapshot = dict(load_app_config())
```

**Pattern to apply:** Strip all telemetry keys from the snapshot at construction time. `save_app_config` is additive-merge (reads full config, does `cfg.update(new_data)`, writes merged result — `genizah_core.py:2882-2890`), so keys absent from the snapshot dict are PRESERVED in config.pkl after restore. This is the cleanest fix — no change to `_on_cancel` required.

```python
# genizah_app.py:2179 — MODIFIED (D-07b)
from desktop.telemetry import (
    TELEMETRY_ENABLED_KEY, FIRST_RUN_SHOWN_KEY, TELEMETRY_INSTALL_ID_KEY,
    CONSENT_TIMESTAMP_KEY, CONSENT_APP_VERSION_KEY, CONSENT_UI_VERSION_KEY,
    IDENTIFIED_USER_KEY,
)
_TELEMETRY_SNAPSHOT_EXCLUDE = {
    TELEMETRY_ENABLED_KEY, FIRST_RUN_SHOWN_KEY, TELEMETRY_INSTALL_ID_KEY,
    CONSENT_TIMESTAMP_KEY, CONSENT_APP_VERSION_KEY, CONSENT_UI_VERSION_KEY,
    IDENTIFIED_USER_KEY,
}
self._config_snapshot = {
    k: v for k, v in load_app_config().items()
    if k not in _TELEMETRY_SNAPSHOT_EXCLUDE
}
```

**Verification:** `save_app_config` additive-merge confirmed at `genizah_core.py:2882-2890` — the function calls `load_app_config()` first, then `cfg.update(new_data)`, so omitted keys in `new_data` are not deleted.

---

#### B. Telemetry checkbox row in `_build_general_tab()` (lines 2238-2295 — checkbox analog)

**Analog:** `chk_notifications` row (lines 2238-2250) and `chk_show_translations` row (lines 2278-2295).

```python
# genizah_app.py:2238-2250 — chk_notifications (direct template)
self.main_win.chk_notifications = QCheckBox(tr("Desktop Notifications"))
self.main_win.chk_notifications.setChecked(load_app_config().get('notifications_enabled', True))
self.main_win.chk_notifications.setToolTip(tr("Flash taskbar..."))
self.main_win.chk_notifications.stateChanged.connect(
    lambda state: save_app_config({'notifications_enabled': state == 2})
)
notif_row = QHBoxLayout()
notif_row.addWidget(self.main_win.chk_notifications)
notif_row.addStretch()
layout.addLayout(notif_row)
layout.addSpacing(4)
```

**Telemetry checkbox divergences from the template:**
1. Initial state uses `is_enabled()` not `load_app_config().get(...)` directly.
2. Does NOT call `save_app_config` in the handler — calls `set_consent()` (D-08).
3. Shows `QMessageBox.question` confirm before applying (D-07a).
4. Has a "Privacy details" flat `QPushButton` beside the checkbox in the same row (D-06).
5. Block signals during `setChecked` to prevent spurious `stateChanged` on setup (Pitfall 5).

```python
# Telemetry row — new, inserted after chk_show_translations block
from desktop.telemetry import is_enabled, set_consent as _set_consent
from desktop.consent_dialog import PrivacyDialog

chk_telemetry = QCheckBox("Help improve the app / עזרו לשפר את האפליקציה")
chk_telemetry.blockSignals(True)
chk_telemetry.setChecked(is_enabled())
chk_telemetry.blockSignals(False)

def _on_telemetry_changed(state):
    new_val = (state == 2)
    prior = is_enabled()
    if new_val == prior:
        return
    action = "start" if new_val else "stop"
    reply = QMessageBox.question(
        self, "Telemetry / טלמטריה",
        f"Telemetry will {action} now.\nאיסוף נתונים יתחיל/יפסיק כעת.",
        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
    )
    if reply == QMessageBox.StandardButton.Yes:
        _set_consent(new_val)
    else:
        chk_telemetry.blockSignals(True)
        chk_telemetry.setChecked(prior)  # revert visual — D-07a
        chk_telemetry.blockSignals(False)

chk_telemetry.stateChanged.connect(_on_telemetry_changed)

btn_privacy = QPushButton("Privacy details / פרטי פרטיות")
btn_privacy.setFlat(True)
btn_privacy.setCursor(Qt.CursorShape.PointingHandCursor)
btn_privacy.clicked.connect(lambda: PrivacyDialog(self).exec())

telemetry_row = QHBoxLayout()
telemetry_row.addWidget(chk_telemetry)
telemetry_row.addSpacing(8)
telemetry_row.addWidget(btn_privacy)
telemetry_row.addStretch()
layout.addLayout(telemetry_row)
layout.addSpacing(4)
```

---

#### C. Startup hook in `on_startup_finished()` (line 3254 — D-03/D-04)

**Analog:** `_show_citation_reminder` singleShot chain at lines 3341-3346.

```python
# genizah_app.py:3337-3346 — existing singleShot chain (startup hook analog)
cfg = load_app_config()
if cfg.get('whats_new_seen') != APP_VERSION:
    self.whats_new_bar.show_whats_new(APP_VERSION)

# One-time citation reminder (shown once per installation)
if not cfg.get('citation_reminder_seen', False):
    QTimer.singleShot(500, self._show_citation_reminder)

# Restore session state (deferred slightly so all widgets are settled)
QTimer.singleShot(200, self._restore_session)
```

**Pattern to copy for consent hook:** Add `_maybe_show_first_run_prompt` as a new method and chain it after the citation reminder. Since `_show_citation_reminder` calls `msg.exec()` (a blocking nested loop), the consent prompt must not share a `singleShot` slot at the same or earlier delay.

**Best approach (from RESEARCH.md §4):** Make `_show_citation_reminder` call `_maybe_show_first_run_prompt` at its own end, so ordering is guaranteed without timing guesswork:

```python
# genizah_app.py:15651 — end of _show_citation_reminder (ADD call here)
    msg.exec()
    save_app_config({'citation_reminder_seen': True})
    self._maybe_show_first_run_prompt()   # NEW — chain consent after citation
```

And in `on_startup_finished()`, add the else-branch for installs that already saw the citation:

```python
# In on_startup_finished() — existing block modified:
if not cfg.get('citation_reminder_seen', False):
    QTimer.singleShot(500, self._show_citation_reminder)
    # consent is chained at the end of _show_citation_reminder
else:
    QTimer.singleShot(500, self._maybe_show_first_run_prompt)   # NEW

# New method to add to GenizahGUI:
def _maybe_show_first_run_prompt(self):
    """Gate for first-run consent dialog — called once after other startup modals."""
    from genizah_core import load_app_config
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY, show_first_run_prompt
    try:
        if not load_app_config().get(FIRST_RUN_SHOWN_KEY, False):
            show_first_run_prompt()  # no-raise; internally calls ConsentDialog.exec()
    except Exception:
        pass  # never block startup
```

**D-04 recovery-modal guarantee:** The recovery modal fires in `MyLibraryTab.__init__()` (line ~1071-1075) during `GenizahGUI.init_ui()`, which is synchronous and completes before `on_startup_finished()` is ever called. By the time any `singleShot` in `on_startup_finished()` fires, the recovery modal is guaranteed closed. No additional sequencing machinery is needed.

---

#### D. About-tab telemetry disclosure pointer (lines 2472-2476 — PRIV-05)

**Analog:** "Local Index Cache Privacy" block at lines 2472-2476 (tone and placement reference):

```python
# genizah_app.py:2472-2476 — existing privacy block (tone reference)
<h3>Local Index Cache Privacy</h3>
<p>Your indexed document text is stored in <code>local_index.sqlite3</code>...
The text is compressed with <b>zstd</b> (compression, not encryption).
This cached data is <b>never uploaded</b> to GenizahSearch servers.
For at-rest encryption, use OS-level disk encryption (BitLocker / FileVault).</p>
```

**New block to insert immediately after this existing block:**

```html
<h3>Usage Telemetry</h3>
<p>This app optionally collects anonymous usage data to help improve it.
Telemetry is <b>opt-in only</b> — nothing is sent unless you enable it in Settings.
What is collected: anonymous feature counts, app version, OS version, performance summaries, and crash signals.
What is <b>never</b> collected: search queries, My Library file paths or filenames, or your name/email.
Data is processed by <a href='https://posthog.com/privacy'>PostHog</a> (EU region) and Dicta.
You can opt out at any time in Settings → General → Preferences.
<a href='#'>See full Privacy Details</a></p>
```

The "See full Privacy Details" link should open `PrivacyDialog` — planner should wire this as a `QPushButton` or handled link in `QTextBrowser`. The About tab already uses `browser.setOpenExternalLinks(True)` (line 2503), so internal links need a different mechanism (e.g. `anchorClicked` signal).

---

### `tests/test_telemetry_consent_ux.py` (NEW — test file)

**Analogs:**
- `tests/test_telemetry_consent_gate.py:1-59` — autouse fixture pattern (in-memory fake config, `_reset_for_tests`, monkeypatch)
- `tests/test_telemetry_no_direct_posthog.py:148-181` — AST-guard pattern (already ships for PRIV-03; new test file must NOT re-implement, just reference)

---

#### Autouse fixture pattern (lines 24-59 — test_telemetry_consent_gate.py)

```python
# tests/test_telemetry_consent_gate.py:24-59 — copy this fixture verbatim
@pytest.fixture(autouse=True)
def _reset_telemetry_state(monkeypatch):
    """Reset desktop.telemetry + posthog_server state before/after each test."""
    fake_config: dict = {}

    def fake_load_app_config():
        return dict(fake_config)

    def fake_save_app_config(new_data: dict):
        fake_config.update(new_data)

    import genizah_core
    monkeypatch.setattr(genizah_core, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(genizah_core, 'save_app_config', fake_save_app_config)

    import desktop.telemetry as tel
    monkeypatch.setattr(tel, 'load_app_config', fake_load_app_config)
    monkeypatch.setattr(tel, 'save_app_config', fake_save_app_config)

    import shared.posthog_server as ph
    ph._reset_for_tests()
    import queue
    fresh_q: queue.Queue = queue.Queue(maxsize=10000)
    monkeypatch.setattr(ph, '_event_queue', fresh_q)

    tel._reset_for_tests()
    tel._load_consent_state()

    yield fake_config

    tel._reset_for_tests()
    ph._reset_for_tests()
```

**Key note:** Headless tests (gate logic, shown-flag, snapshot exemption) reuse this fixture as-is and do NOT need `QApplication`. Qt-offscreen tests (dialog widget tests) require `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` and a `QApplication` instance — mark them with `pytest.mark.qt` or a custom marker per the project's Windows test guidance (`feedback_full_suite_testing_windows.md`).

---

#### Headless test structure pattern (lines 65-129 — test_telemetry_consent_gate.py)

```python
# tests/test_telemetry_consent_gate.py:65-68 — headless test pattern
def test_is_enabled_false_on_absent_key():
    import desktop.telemetry as tel
    assert tel.is_enabled() is False

# Monkeypatch set_consent pattern for shown-flag tests:
def test_shown_flag_written_on_accept(monkeypatch):
    import desktop.telemetry as tel
    from desktop.telemetry import FIRST_RUN_SHOWN_KEY
    # monkeypatch set_consent to a no-op (avoid PostHog side effects)
    monkeypatch.setattr(tel, 'set_consent', lambda v: None)
    # Simulate ConsentDialog._on_enable path:
    tel.save_app_config({FIRST_RUN_SHOWN_KEY: True})
    assert tel.load_app_config().get(FIRST_RUN_SHOWN_KEY) is True
```

---

## Shared Patterns

### Consent single source of truth (D-08)
**Source:** `desktop/telemetry.py:399` `set_consent()`
**Apply to:** `ConsentDialog._on_enable`, `ConsentDialog._on_decline`, `ConsentDialog.closeEvent`, `SettingsDialog._on_telemetry_changed`

All consent writes must call `desktop.telemetry.set_consent(bool)`. Never call `save_app_config({'telemetry_enabled': ...})` directly from UI code.

```python
# desktop/telemetry.py:399 — sole write path
def set_consent(enabled: bool) -> None:
    """On opt-in: mints UUID, writes audit fields, wires transport.
       On opt-out: clears gate, drains queue, retains install_id."""
    # ... never raises
```

### `save_app_config` additive-merge guarantee
**Source:** `genizah_core.py:2882-2890`
**Apply to:** `SettingsDialog._config_snapshot` strip approach (D-07b)

```python
# genizah_core.py:2882-2890 — additive-merge (keys NOT in new_data are preserved)
def save_app_config(new_data):
    try:
        cfg = load_app_config()   # loads full current config
        cfg.update(new_data)      # merges — keys absent from new_data are UNTOUCHED
        ...
        pickle.dump(cfg, f)
```

This is the foundational guarantee that makes the "strip telemetry keys from snapshot" approach safe.

### QMessageBox confirm pattern
**Source:** `genizah_app.py:3314-3318` (index-missing confirm) — same 2-button pattern for the telemetry toggle confirm

```python
# genizah_app.py:3314-3318 — QMessageBox.question pattern
reply = QMessageBox.question(self, tr("Index Missing"), msg,
                             QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
if reply == QMessageBox.StandardButton.Yes:
    ...
```

Apply same 2-button `Yes | No` form for the telemetry toggle confirm-on-change (D-07a).

### blockSignals during setChecked
**Source:** Not yet in codebase for checkboxes — established PyQt6 convention needed to prevent spurious `stateChanged` on initial `setChecked` call. See RESEARCH.md Pitfall 5.

```python
chk.blockSignals(True)
chk.setChecked(initial_value)
chk.blockSignals(False)
chk.stateChanged.connect(handler)  # connect AFTER setting initial state
```

---

## No Analog Found

No files are without analogs. All four files have concrete matches in the codebase.

---

## Metadata

**Analog search scope:** `genizah_app.py` (SettingsDialog + dialogs), `desktop/` (all .py files), `tests/test_telemetry_*.py`
**Files scanned:** `genizah_app.py`, `desktop/telemetry.py`, `tests/test_telemetry_consent_gate.py`, `tests/test_telemetry_no_direct_posthog.py`
**Pattern extraction date:** 2026-06-14
