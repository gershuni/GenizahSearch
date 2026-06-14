# Phase 112: Consent UX - Research

**Researched:** 2026-06-14
**Domain:** PyQt6 dialog composition + startup sequencing + SettingsDialog wiring
**Confidence:** HIGH (pure codebase archaeology — all findings verified by reading live source)

---

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions

- **D-01:** First-run consent dialog shows BOTH EN and HE text stacked in one dialog (English block + Hebrew block, or two columns). Do NOT build with `tr()` alone — both languages always visible regardless of `CURRENT_LANG`.
- **D-02:** Same bilingual-stacked treatment for the privacy disclosure (PrivacyDialog).
- **D-03:** Modal appears after the main window paints (queue post-`show()`, e.g. `QTimer.singleShot` after startup work completes).
- **D-04:** Must NOT stack on top of recovery modal or sync prompt. Consent shows only after those resolve.
- **D-05:** Shown exactly once. After any choice (Accept, Decline, close/Escape) → write `FIRST_RUN_SHOWN_KEY=True` unconditionally. Close without choosing = opted out.
- **D-06:** Toggle lives in SettingsDialog → General tab → Preferences group, alongside chk_notifications / chk_show_translations.
- **D-07a:** Confirm-on-change → immediate-apply. Flip checkbox → show confirm → on confirm call `set_consent()` immediately. On cancel-of-confirm, revert checkbox visually with NO `set_consent()` call.
- **D-07b:** Telemetry consent keys EXEMPT from `SettingsDialog._config_snapshot` / `_on_cancel` restore.
- **D-08:** Toggle and dialog route consent changes ONLY through `set_consent()` — never raw `save_app_config({'telemetry_enabled': ...})`.
- **D-09:** Standalone bilingual `PrivacyDialog` (QDialog, EN+HE stacked) opened from BOTH first-run "Learn more" button AND Settings "Privacy details" link.
- **D-10:** Disclosure content covers: what is collected, what is NOT, who processes (PostHog EU + Dicta), how to opt out (Settings toggle), pseudonymous install id.

### Claude's Discretion

- Exact EN/HE button labels (equal-weight, no default — e.g. "Enable" / "Not now").
- Stacked-vertical vs two-column bilingual layout.
- Exact PrivacyDialog copy (within D-10's required points).
- PRIV-05 breadth: brief telemetry pointer + link in About tab and optionally Help.html.
- Whether to stamp a consent-audit record on opt-out (optional nicety).

### Deferred Ideas (OUT OF SCOPE)

- CONSENT-F1: "Reset telemetry id" affordance in Settings.
- Web consent gate harmonization.
- Producers (crash/usage/perf events) — Phases 113-115.
- No events fire in this phase.
</user_constraints>

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| CONSENT-02 | First-run bilingual EN/HE consent dialog, equal-weight yes/no, nothing pre-selected, states what is/isn't collected. | Dialog builder pattern from WhatsNewDialog (:284) + QDialog.exec() + no setDefault on either button. |
| CONSENT-03 | Dialog shown at most once; choice + "prompt shown" flag persist; stored record captures timestamp, app version, consent-UI version. | `FIRST_RUN_SHOWN_KEY` already a constant in `desktop/telemetry.py`; audit fields already written by `set_consent(True)`; this phase writes the shown-flag on all exit paths. |
| CONSENT-04 | User can toggle telemetry on/off from Settings at any time, same consent source of truth as first-run dialog. | SettingsDialog General tab `_pref_row()` pattern at :2221; `set_consent()` is the sole write path (D-08). |
| CONSENT-08 | Opting out drains/discards queued events. | **Already Complete (Phase 111)** — implemented inside `set_consent(False)` via `_drain_and_discard()`. No work here. |
| PRIV-05 | Help/About disclosure updated bilingually (EN/HE): what is collected, opt-in, how to opt out, pseudonymous install id. | Existing "Local Index Cache Privacy" block at :2472 is the tone/placement precedent. PrivacyDialog is the canonical text; About tab gets a brief pointer + link. |
</phase_requirements>

---

## Summary

Phase 112 is pure PyQt6 UI composition and wiring on top of the Phase 111 telemetry engine (`desktop/telemetry.py`). The engine's public API is already complete and tested: `set_consent()`, `is_enabled()`, `get_install_id()`, all `*_KEY` constants, the `FIRST_RUN_SHOWN_KEY` constant, and the `show_first_run_prompt()` no-op stub at line 712. This phase fills in that stub and adds the Settings toggle.

The key implementation challenges are sequencing (D-04: consent dialog must not stack on the recovery modal, which fires synchronously in `MyLibraryTab.__init__` before the main window even shows) and snapshot exemption (D-07b: `SettingsDialog._on_cancel()` calls `save_app_config(self._config_snapshot)` which overwrites the whole config, so the telemetry keys must be preserved across a Cancel). Both have concrete solutions documented below.

**Primary recommendation:** Implement `show_first_run_prompt()` as a self-contained builder inside `desktop/telemetry.py` (or a thin helper in `desktop/consent_dialog.py` imported only by `desktop/telemetry.py`). Wire the startup call at the end of `on_startup_finished()` after `QTimer.singleShot(200, self._restore_session)` using a longer delay (e.g. 1500 ms) so the citation-reminder (500 ms) and session-restore (200 ms) have resolved. This avoids modal stacking with zero new machinery.

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Consent persistence (read/write config.pkl) | `desktop/telemetry.py` | — | Engine owns the store; UI reads through `is_enabled()`, writes through `set_consent()` |
| First-run dialog UI | `desktop/telemetry.py::show_first_run_prompt()` | Optional `desktop/consent_dialog.py` | PRIV-03 AST guard requires all posthog_server access stay in `desktop/telemetry.py`; dialog builder can live there or in a helper it imports |
| Settings toggle | `genizah_app.py::SettingsDialog._build_general_tab()` | — | Follows existing checkbox row pattern; calls `set_consent()` not raw save |
| Privacy disclosure | `desktop/consent_dialog.py::PrivacyDialog` | About tab HTML block | Standalone QDialog, opened from both first-run and Settings |
| Startup sequencing | `genizah_app.py::GenizahGUI.on_startup_finished()` | — | Only safe post-paint site; recovery modal fires in `MyLibraryTab.__init__` (earlier) |

---

## Standard Stack

### Core (all already in-project — zero new dependencies)

| Library | Source | Purpose | Status |
|---------|--------|---------|--------|
| PyQt6.QtWidgets (QDialog, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QTextBrowser, QCheckBox, QMessageBox) | PyQt6 (already bundled) | Dialog/widget construction | [VERIFIED: live codebase] |
| PyQt6.QtCore (Qt, QTimer) | PyQt6 (already bundled) | Layout direction, deferred calls | [VERIFIED: live codebase] |
| PyQt6.QtGui (QPalette, QApplication.palette()) | PyQt6 (already bundled) | Palette-aware colors for PrivacyDialog | [VERIFIED: live codebase] |
| `desktop/telemetry.py` public API | in-project | `set_consent()`, `is_enabled()`, `FIRST_RUN_SHOWN_KEY`, `*_KEY` constants | [VERIFIED: desktop/telemetry.py] |
| `genizah_core.load_app_config` / `save_app_config` | in-project | Reading FIRST_RUN_SHOWN_KEY gate | [VERIFIED: genizah_core.py:2871] |

### No New Packages Required

INFRA-04 is locked: zero new pip dependencies. This phase installs nothing. The Package Legitimacy Audit section is omitted (no external packages).

---

## Architecture Patterns

### System Architecture Diagram

```
GenizahGUI.__init__()
  │
  ├─► init_ui()                        (synchronous — UI widgets created)
  │     └─► MyLibraryTab.__init__()    (RECOVERY MODAL fires HERE if needed)
  │           └─► _show_recovery_modal() → QMessageBox.exec() → blocks until closed
  │
  └─► QTimer.singleShot(100, start_background_init)
        └─► StartupThread.finished → on_startup_finished()
              │
              ├─► [existing] QTimer.singleShot(200, _restore_session)
              ├─► [existing] QTimer.singleShot(500, _show_citation_reminder)   ← blocks 500ms
              │
              └─► [NEW] QTimer.singleShot(1500, _maybe_show_first_run_prompt)
                    │
                    ├─ FIRST_RUN_SHOWN_KEY=True already → skip
                    └─ not shown yet → call desktop.telemetry.show_first_run_prompt(parent)
                          │
                          ├─► ConsentDialog.exec()           (blocks, user reads + chooses)
                          │     ├─ Accept → set_consent(True)
                          │     ├─ Decline / close / Escape → set_consent(False) [default]
                          │     └─ "Learn more" → PrivacyDialog(parent).exec() → returns to ConsentDialog
                          └─► save_app_config({FIRST_RUN_SHOWN_KEY: True})   [UNCONDITIONAL]

SettingsDialog._build_general_tab()
  └─► chk_telemetry (QCheckBox, initial state = is_enabled())
        └─► stateChanged → _on_telemetry_changed(state)
              ├─ show QMessageBox confirm ("start"/"stop")
              ├─ confirmed → set_consent(bool)    [D-08: sole write path]
              ├─ cancelled → chk_telemetry.setChecked(prior)  [revert visual]
              └─ "Privacy details" link beside checkbox → PrivacyDialog(parent).exec()

SettingsDialog._on_cancel()
  └─► CURRENT: save_app_config(self._config_snapshot)  [overwrites whole config]
      FIX NEEDED (D-07b): re-apply current telemetry keys after restore
```

### Recommended Project Structure

```
desktop/
├── telemetry.py              # Engine (Phase 111) + show_first_run_prompt() stub → implement here
├── consent_dialog.py         # NEW: ConsentDialog + PrivacyDialog (no posthog_server import — safe)
genizah_app.py                # Modify: SettingsDialog._build_general_tab(), _on_cancel(),
                              #          on_startup_finished() startup hook
tests/
├── test_telemetry_consent_ux.py   # NEW: headless tests for gate logic, snapshot exemption
```

**Module placement decision:** The dialog classes (`ConsentDialog`, `PrivacyDialog`) can live in `desktop/consent_dialog.py`. `desktop/telemetry.py::show_first_run_prompt()` imports and instantiates `ConsentDialog` from there. This keeps `posthog_server` imports confined to `desktop/telemetry.py` (PRIV-03 compliant) while keeping dialog code out of the already-large telemetry module. The PRIV-03 AST guard scans `desktop/` for `posthog_server` imports — `consent_dialog.py` does not import it, so the guard passes.

---

## Engine API Surface (Phase 111 — verified)

Verified by reading `desktop/telemetry.py` directly. All of the following exist and are production-ready:

| Symbol | Location | Purpose |
|--------|----------|---------|
| `show_first_run_prompt()` | line 712 | No-op stub — THIS phase fills it in |
| `set_consent(enabled: bool)` | line 399 | Sole consent write path; handles UUID mint, audit fields, queue drain, transport wire/revoke |
| `is_enabled()` | line 377 | Cached no-throw consent read; safe to call from any thread |
| `get_install_id()` | line 390 | Returns persisted UUID hex or None |
| `TELEMETRY_ENABLED_KEY` | line 68 | `'telemetry_enabled'` |
| `FIRST_RUN_SHOWN_KEY` | line 70 | `'telemetry_first_run_shown'` — Phase 112 writes this |
| `TELEMETRY_INSTALL_ID_KEY` | line 69 | `'telemetry_install_id'` |
| `CONSENT_TIMESTAMP_KEY` | line 71 | `'telemetry_consent_ts'` |
| `CONSENT_APP_VERSION_KEY` | line 72 | `'telemetry_consent_version'` |
| `CONSENT_UI_VERSION_KEY` | line 73 | `'telemetry_consent_ui_ver'` |
| `IDENTIFIED_USER_KEY` | line 74 | `'telemetry_identified_user'` |

`set_consent(True)` already writes `CONSENT_TIMESTAMP_KEY`, `CONSENT_APP_VERSION_KEY`, `CONSENT_UI_VERSION_KEY='1'`. This phase does NOT need to write those — they are already handled by the engine on opt-in (CONSENT-03 satisfied by calling `set_consent(True)`).

---

## Critical Code Analysis

### 1. `show_first_run_prompt()` Stub (line 712, confirmed)

```python
# desktop/telemetry.py:712 — current no-op stub
def show_first_run_prompt() -> None:
    """Display the first-run consent prompt. Implemented in Phase 112.

    No-op in Phase 111. Never raises.
    """
    # Phase 112 implementation
```

The stub is in place. Phase 112 fills the body. The function must stay no-raise (consistent with the rest of the module's contract — CRASH-05).

### 2. `SettingsDialog` — `_config_snapshot` / `_on_cancel` (line 2178-2207, verified)

```python
# genizah_app.py:2178-2179 — snapshot taken at dialog open
self._config_snapshot = dict(load_app_config())

# genizah_app.py:2204-2207 — Cancel restores entire config
def _on_cancel(self):
    """Restore config snapshot and close."""
    save_app_config(self._config_snapshot)
    self.reject()
```

`_config_snapshot` is a full dict copy of ALL config.pkl keys at the moment `SettingsDialog.__init__` runs. `_on_cancel` calls `save_app_config(self._config_snapshot)` which calls `load_app_config()` internally and then **updates** with the snapshot dict — but since the snapshot IS the full config, this effectively OVERWRITES all keys back to the snapshot state.

**The D-07b problem:** If the user opens Settings, flips the telemetry toggle (which immediately calls `set_consent()` → writes new values to config.pkl + updates in-memory `_enabled` cache), and then presses Cancel — `_on_cancel()` restores `self._config_snapshot` which has the PRE-flip values. config.pkl now says `telemetry_enabled=False` (or the old value) but `desktop/telemetry._enabled` is still `True` (or the new value). The in-memory state and the disk state are now desynced.

**D-07b fix approach:** After `save_app_config(self._config_snapshot)`, re-apply the current telemetry state from the in-memory engine:

```python
def _on_cancel(self):
    """Restore config snapshot and close."""
    save_app_config(self._config_snapshot)
    # D-07b: telemetry keys were applied immediately by set_consent(); restore
    # them from the in-memory engine state so config.pkl stays consistent with
    # the live _enabled cache — Cancel cannot desync config from the engine.
    from desktop.telemetry import (
        is_enabled, get_install_id,
        TELEMETRY_ENABLED_KEY, TELEMETRY_INSTALL_ID_KEY
    )
    reapply = {TELEMETRY_ENABLED_KEY: is_enabled()}
    iid = get_install_id()
    if iid is not None:
        reapply[TELEMETRY_INSTALL_ID_KEY] = iid
    save_app_config(reapply)
    self.reject()
```

This is the minimal-impact fix: after the snapshot restore, immediately re-stamp the telemetry keys that `set_consent()` owns. The consent audit fields (`CONSENT_TIMESTAMP_KEY`, etc.) are also protected because `is_enabled()` being True implies they were written by `set_consent(True)` — and the snapshot would have had them absent (first-time opt-in) or already present (re-opt-in). The safest approach is to also re-apply `CONSENT_TIMESTAMP_KEY` / `CONSENT_APP_VERSION_KEY` / `CONSENT_UI_VERSION_KEY` when `is_enabled()` is True, by reading them from config.pkl AFTER the restore (they survived since the snapshot contained them if they existed at open-time). Actually the simpler invariant: only `TELEMETRY_ENABLED_KEY` and `TELEMETRY_INSTALL_ID_KEY` need re-stamping, since the audit fields are write-once-on-opt-in and the snapshot already has them if they existed.

**Alternative approach (no _on_cancel change):** Exclude the telemetry keys from the snapshot at construction time:

```python
# In __init__: take snapshot but strip telemetry keys
_TELEMETRY_KEYS = {
    TELEMETRY_ENABLED_KEY, FIRST_RUN_SHOWN_KEY,
    TELEMETRY_INSTALL_ID_KEY, CONSENT_TIMESTAMP_KEY,
    CONSENT_APP_VERSION_KEY, CONSENT_UI_VERSION_KEY, IDENTIFIED_USER_KEY
}
self._config_snapshot = {k: v for k, v in load_app_config().items()
                         if k not in _TELEMETRY_KEYS}
```

This is cleaner: the snapshot never contains telemetry keys, so `save_app_config(self._config_snapshot)` cannot overwrite them (because `save_app_config` MERGES — it does `cfg.update(new_data)`, so missing keys are not deleted). **This is the recommended approach** — it works because `save_app_config` is additive-merge, not full-replace: it calls `load_app_config()` first and then `cfg.update(new_data)`, so keys not in `new_data` are left untouched.

Verification of this claim from `genizah_core.py:2882-2890`:
```python
def save_app_config(new_data):
    try:
        cfg = load_app_config()       # loads full current config
        cfg.update(new_data)          # merges new_data into it
        ...
        pickle.dump(cfg, f)           # writes merged result
```

Therefore: if `_config_snapshot` does NOT contain the telemetry keys, `save_app_config(self._config_snapshot)` will merge the snapshot back while leaving any telemetry keys that are currently in config.pkl UNTOUCHED. This is the correct, minimal fix.

### 3. `_build_general_tab()` — Existing Checkbox Pattern (lines 2238-2295, verified)

The `chk_notifications` and `chk_show_translations` patterns are the direct template for the telemetry checkbox:

```python
# Pattern A: checkbox with row layout (chk_notifications style — lines 2238-2250)
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
```

The telemetry checkbox diverges in two ways:
1. Does NOT call `save_app_config` directly — calls `set_consent()` (D-08)
2. Shows a confirm dialog before applying (D-07a)
3. Has a "Privacy details" link button beside the checkbox (D-06)

The telemetry checkbox row pattern:
```python
telemetry_row = QHBoxLayout()
chk_telemetry = QCheckBox("Help improve the app / עזרו לשפר את האפליקציה")
chk_telemetry.setChecked(is_enabled())
chk_telemetry.stateChanged.connect(self._on_telemetry_changed)
telemetry_row.addWidget(chk_telemetry)
privacy_link = QPushButton("Privacy details / פרטי פרטיות")
privacy_link.setFlat(True)
privacy_link.clicked.connect(lambda: PrivacyDialog(self).exec())
telemetry_row.addWidget(privacy_link)
telemetry_row.addStretch()
layout.addLayout(telemetry_row)
```

Note: the checkbox label itself can be bilingual-inline (both languages always visible) to satisfy D-01's spirit even in the Settings context.

### 4. Startup Sequencing — D-03/D-04 (verified)

**Recovery modal timing (critical for D-04):**

`MyLibraryTab.__init__()` is called from `GenizahGUI.init_ui()` at line 3434 (synchronous, during `__init__`). Inside `MyLibraryTab.__init__`, the recovery probe runs at line 1071-1075 and calls `_show_recovery_modal()` at line 1075, which calls `mb.exec()` — a synchronous modal that blocks until the user clicks. This all happens BEFORE `GenizahGUI.show()` is called and BEFORE the window is visible.

**Key insight:** The recovery modal fires DURING `GenizahGUI.__init__`, not during `on_startup_finished()`. By the time `on_startup_finished()` runs (triggered by the background `StartupThread`), the recovery modal has already been closed.

Therefore the D-04 concern about stacking is actually simpler than it appears: by the time `on_startup_finished()` calls `QTimer.singleShot(N, show_first_run_prompt)`, the recovery modal is GUARANTEED to be gone. The risk scenario is: a naïve `singleShot(0, ...)` inside `__init__` itself. Since the plan puts the consent call in `on_startup_finished()`, the recovery modal stacking problem is automatically avoided.

**Citation reminder timing (the actual conflict to avoid):**
The citation reminder fires `QTimer.singleShot(500, self._show_citation_reminder)` at line 3343 and uses `msg.exec()` — a blocking modal. If consent is `singleShot(0, ...)` or even `singleShot(200, ...)`, it could fire before the 500ms citation timer expires, AND both are in the same event loop. More critically: `msg.exec()` in the citation reminder creates a nested event loop. A consent `singleShot` with delay < 500ms could fire INSIDE that nested loop if the citation reminder runs first.

**Safe sequencing:** Use `QTimer.singleShot(1500, self._maybe_show_first_run_prompt)`. This fires after:
- Session restore: 200ms (non-blocking — just populates UI widgets)
- Citation reminder: 500ms (`msg.exec()` blocks for user click, then returns)
- 1500ms ≥ 500ms + typical user-clicks-OK time

As an even safer alternative: call `show_first_run_prompt` as the LAST action of `_show_citation_reminder()` itself (in a `finally` block or chained via another `singleShot(0)`). This guarantees strict ordering without needing to guess at durations.

**Best approach:** Chain the consent call to the end of the citation reminder:

```python
# In on_startup_finished():
if not cfg.get('citation_reminder_seen', False):
    QTimer.singleShot(500, self._show_citation_reminder_then_consent)
else:
    QTimer.singleShot(500, self._maybe_show_first_run_prompt)
```

Or more elegantly, make `_show_citation_reminder()` call `_maybe_show_first_run_prompt()` at its own end. This avoids any timing guesswork and is the pattern used for `_restore_session` (which is already chained in the existing code).

**Sync prompt:** Grepping for sync_prompt found no blocking sync dialog in the startup path. The lists sync (`joins_mgr.start_background_sync()`) is non-blocking. There is no blocking sync modal to sequence around.

### 5. `WhatsNewDialog` + `HelpDialog` — Precedents for `PrivacyDialog` (verified)

**WhatsNewDialog (lines 284-343):**
- `QDialog` with `setModal(True)`, `setFixedSize(500, 440)`
- Language via `CURRENT_LANG` / `tr()` — SINGLE LANGUAGE (diverges from D-02)
- `setLayoutDirection(RightToLeft)` when Hebrew
- Plain `QLabel` for content, `QPushButton` for action
- No `QTextBrowser`

**HelpDialog (lines 1447-1508):**
- `QDialog` with `resize(900, 700)` (not fixed)
- `QTextBrowser` with `setOpenExternalLinks(True)` for HTML content
- Language stripping via `<!-- START_LANG_HE -->` / `<!-- END_LANG_HE -->` markers — SINGLE LANGUAGE shown
- `setHtml()` for rich content

**`PrivacyDialog` should use `QTextBrowser` approach** (like HelpDialog) because:
- Content is longer and benefits from scrolling
- HTML formatting (bold, lists) helps readability
- `setOpenExternalLinks(True)` allows PostHog privacy policy link
- But: render BOTH languages in the HTML (no stripping — D-02)

**Palette-aware colors pattern (from SettingsDialog):**
```python
pal = QApplication.palette()
self._is_dark = pal.color(QPalette.ColorRole.Window).lightness() < 128
```

**PrivacyDialog structure to mirror:**
```python
class PrivacyDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Privacy / פרטיות")
        self.setModal(True)
        self.resize(600, 500)
        self.setWindowFlags(self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint)
        # No setLayoutDirection — both languages shown; let Qt default (LTR for EN block)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)

        browser = QTextBrowser()
        browser.setOpenExternalLinks(True)
        browser.setHtml(self._build_html())
        layout.addWidget(browser)

        btn_close = QPushButton("Close / סגור")
        btn_close.clicked.connect(self.accept)
        layout.addWidget(btn_close)
```

The HTML content for the browser renders EN and HE sections vertically with `<div dir='ltr'>` and `<div dir='rtl'>` to ensure correct RTL rendering of Hebrew without applying RTL to the whole dialog.

### 6. `load_language()` / `tr()` Confirmation of D-01 Premise (verified)

```python
# genizah_core.py:2852-2860
def load_language():
    try:
        if os.path.exists(Config.LANGUAGE_FILE):
            with open(Config.LANGUAGE_FILE, 'rb') as f:
                return pickle.load(f)
    except Exception:
        ...
    return 'en'  # Default: English

# genizah_core.py:2896-2900
CURRENT_LANG = load_language()  # Set at module import time

def tr(text):
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text
```

`CURRENT_LANG` defaults to `'en'` if no language file exists. On a fresh install before the user has ever touched the language toggle, `tr()` returns English strings only. D-01's premise is confirmed: `tr()` alone cannot show Hebrew to a Hebrew-first user on first launch. Both languages must be hardcoded into the dialog HTML/text, not gated on `CURRENT_LANG`.

### 7. "Local Index Cache Privacy" Precedent (lines 2472-2476, verified)

```python
# genizah_app.py:2472-2476 — About tab privacy block (tone reference)
<h3>Local Index Cache Privacy</h3>
<p>Your indexed document text is stored in <code>local_index.sqlite3</code>...
The text is compressed with <b>zstd</b> (compression, not encryption).
This cached data is <b>never uploaded</b> to GenizahSearch servers.
For at-rest encryption, use OS-level disk encryption (BitLocker / FileVault).</p>
```

The new telemetry disclosure block in the About tab should follow this pattern: a `<h3>` heading, plain declarative prose, bold emphasis on "never" for the key privacy guarantees. Placed immediately AFTER the existing "Local Index Cache Privacy" block (before "Data Source & Acknowledgments") for logical grouping.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| "No Enter-key default" button behavior | Custom event filter | `btn.setDefault(False); btn.setAutoDefault(False)` on BOTH buttons | QDialog's default button mechanism; Enter activates `isDefault()` button |
| Consent persistence | New config file or QSettings | `set_consent()` + `load_app_config()` / `save_app_config()` | Already implemented, tested, and in config.pkl (config.pkl survives crashes; session.json does not) |
| Bilingual RTL rendering | CSS direction hacks | `<div dir='rtl'>..HE text..</div>` in QTextBrowser HTML | QTextBrowser honors `dir` attribute per-block |
| Confirm-on-change dialog | Custom confirm QDialog | `QMessageBox.question(self, title, text, Yes\|No)` | Standard Qt pattern; two lines of code |

---

## Common Pitfalls

### Pitfall 1: Enter-Key Opt-In (SC#1 CRITICAL)
**What goes wrong:** `QDialog` sets the last-added button with `setDefault(True)` by convention. If the "Enable" button happens to be added last (or is the only `setDefault(True)` button), pressing Enter activates it — the user opted in without reading. SC#1 says "Enter without reading ⇒ opted out."
**Why it happens:** QDialog's auto-default mechanism; the most-recently-focused `QPushButton` becomes the default unless explicitly disabled.
**How to avoid:** Call `btn_enable.setDefault(False); btn_enable.setAutoDefault(False)` AND `btn_decline.setDefault(False); btn_decline.setAutoDefault(False)` on BOTH buttons. Override the dialog's `keyPressEvent` to treat Enter/Return as "Decline" (or do nothing, leaving consent unchanged at False).
**Warning signs:** If pressing Enter in the dialog triggers opt-in, the default is set.

### Pitfall 2: Escape/Close Not Writing FIRST_RUN_SHOWN_KEY (D-05 CRITICAL)
**What goes wrong:** User closes the dialog with the X button or Escape key. `QDialog.reject()` is called. If `FIRST_RUN_SHOWN_KEY` is only written in the button slots, close/Escape bypasses it. The dialog shows again next launch.
**How to avoid:** Override `reject()` (or `closeEvent()`) to also write the shown-flag and call `set_consent(False)` before `super().reject()`. Better: write the flag in `done(result)` override — it's called for ALL exit paths (accept, reject, close).
**Warning signs:** Dialog reappears after closing with X or Escape.

### Pitfall 3: singleShot Inside Nested Event Loop (D-04)
**What goes wrong:** If `_maybe_show_first_run_prompt` is queued with `singleShot(0, ...)` from within `__init__` itself, it fires when the event loop next processes events — which could be INSIDE the recovery modal's `exec()` nested loop if the recovery modal fires before the singleShot callback runs. Two modals stack.
**Why it happens:** `QMessageBox.exec()` and `QDialog.exec()` create nested event loops. A pending `singleShot(0)` fires in the next pending event, which can be inside the nested loop.
**How to avoid:** As analyzed above — put the consent call in `on_startup_finished()`, not in `__init__`. The recovery modal is guaranteed closed by that point.

### Pitfall 4: Cancel Desyncs telemetry_enabled (D-07b)
**What goes wrong:** User opens Settings, flips telemetry toggle (calls `set_consent()` immediately), then presses Cancel. `_on_cancel()` calls `save_app_config(self._config_snapshot)`. The snapshot was taken BEFORE the toggle flip. config.pkl is now the pre-flip value, but `desktop/telemetry._enabled` in memory is the post-flip value. Consent state is split between disk and RAM.
**How to avoid:** Strip telemetry keys from `_config_snapshot` at construction time (recommended). `save_app_config` is additive-merge (reads existing config first, then updates), so keys not in the snapshot dict are PRESERVED in config.pkl after restore.
**Warning signs:** `is_enabled()` returns `True` but `load_app_config()['telemetry_enabled']` returns `False` after a Cancel.

### Pitfall 5: Dialog Checkbox Initial State Off-by-One
**What goes wrong:** `chk_telemetry.setChecked(is_enabled())` fires `stateChanged` signal, which triggers the confirm dialog immediately on open.
**How to avoid:** Block signals during setup: `chk_telemetry.blockSignals(True); chk_telemetry.setChecked(is_enabled()); chk_telemetry.blockSignals(False)`. Then connect `stateChanged`.

### Pitfall 6: PRIV-03 Violation in consent_dialog.py
**What goes wrong:** `desktop/consent_dialog.py` imports `shared.posthog_server` or calls `enqueue_event` directly. The PRIV-03 AST guard (tests/test_telemetry_no_direct_posthog.py) catches this and fails CI.
**How to avoid:** `consent_dialog.py` never imports `shared.posthog_server`. All consent writes go through `desktop.telemetry.set_consent()`.

### Pitfall 7: SettingsDialog is Instantiated Once and Reused
**What goes wrong:** `self.settings_dialog = SettingsDialog(self)` at line 3438 is called ONCE during `init_ui()`. The `_config_snapshot` is taken at construction time, not at each open. If `_config_snapshot` is built once at init time (before any telemetry changes), it is effectively correct — it captures the state at dialog creation. But if the dialog is shown multiple times (user opens Settings, closes, opens again), the snapshot is NOT refreshed. In practice, other config changes between opens are also not snapshotted, so this is existing behavior — but it means the telemetry strip-from-snapshot fix must happen at construction time, not at `exec()` time.
**How to avoid:** The strip-from-snapshot approach (strip telemetry keys from `_config_snapshot` once at `__init__`) is correct for this single-instantiation pattern.

---

## Implementation Guidance: No-Default Button Pattern

```python
# ConsentDialog — equal-weight buttons, no Enter-key default
class ConsentDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Telemetry Consent / הסכמה לטלמטריה")
        self.setModal(True)
        self.setFixedSize(520, 400)
        # Prevent Enter from triggering any button
        self.setWindowFlags(
            self.windowFlags() & ~Qt.WindowType.WindowContextHelpButtonHint
        )
        layout = QVBoxLayout(self)
        ...

        # Equal-weight buttons — NO default on either
        btn_enable = QPushButton("Enable / הפעל")
        btn_enable.setDefault(False)
        btn_enable.setAutoDefault(False)
        btn_enable.clicked.connect(self._on_enable)

        btn_decline = QPushButton("Not now / לא עכשיו")
        btn_decline.setDefault(False)
        btn_decline.setAutoDefault(False)
        btn_decline.clicked.connect(self._on_decline)

        ...

    def keyPressEvent(self, event):
        # Enter/Return without clicking a button = implicit decline (D-05)
        from PyQt6.QtCore import Qt
        if event.key() in (Qt.Key.Key_Return, Qt.Key.Key_Enter):
            self._on_decline()
            return
        super().keyPressEvent(event)

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

    def _write_shown_flag(self):
        from genizah_core import save_app_config
        from desktop.telemetry import FIRST_RUN_SHOWN_KEY
        save_app_config({FIRST_RUN_SHOWN_KEY: True})

    def closeEvent(self, event):
        # X button = implicit decline (D-05)
        from desktop import telemetry
        from desktop.telemetry import FIRST_RUN_SHOWN_KEY
        from genizah_core import save_app_config
        telemetry.set_consent(False)
        save_app_config({FIRST_RUN_SHOWN_KEY: True})
        super().closeEvent(event)
```

---

## What Exists vs What This Phase Builds

| Requirement | Already Exists (Phase 111) | This Phase Builds |
|-------------|---------------------------|-------------------|
| CONSENT-02 | `show_first_run_prompt()` stub exists | Dialog UI (bilingual, 2 equal-weight buttons, no default, Learn more link) |
| CONSENT-03 | `FIRST_RUN_SHOWN_KEY` constant; audit fields written by `set_consent(True)` | Write `FIRST_RUN_SHOWN_KEY=True` unconditionally on ALL exit paths of the dialog |
| CONSENT-04 | `set_consent()` / `is_enabled()` public API | Checkbox in SettingsDialog General tab, confirm-on-change, snapshot exemption |
| CONSENT-08 | `set_consent(False)` calls `_drain_and_discard()` — COMPLETE | Nothing (already done) |
| PRIV-05 | "Local Index Cache Privacy" block in About tab (precedent only) | `PrivacyDialog` class (bilingual, D-10 content); pointer in About tab HTML |

---

## Validation Architecture

### Test Framework

| Property | Value |
|----------|-------|
| Framework | pytest (existing) |
| Config file | `pytest.ini` (existing) |
| Quick run command | `pytest tests/test_telemetry_consent_ux.py -x -q` |
| Full suite command | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry*.py -x` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | Notes |
|--------|----------|-----------|-------------------|-------|
| CONSENT-02 | Dialog shows only when `FIRST_RUN_SHOWN_KEY` absent | unit (headless) | `pytest tests/test_telemetry_consent_ux.py::test_first_run_gate_skips_if_shown -x` | No Qt needed — tests config.pkl gate logic |
| CONSENT-02 | Dialog shows with equal-weight buttons, no default | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_consent_dialog_no_default_button -x` | Needs PyQt6 offscreen |
| CONSENT-03 | FIRST_RUN_SHOWN_KEY written on all exit paths | unit (headless) | `pytest tests/test_telemetry_consent_ux.py::test_shown_flag_written_on_accept -x` and `::test_shown_flag_written_on_decline` and `::test_shown_flag_written_on_close` | Monkeypatch set_consent |
| CONSENT-04 | Settings toggle initial state matches is_enabled() | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_settings_toggle_initial_state -x` | Needs Qt |
| CONSENT-04 | Toggle → confirm → set_consent() called | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_settings_toggle_applies_on_confirm -x` | Monkeypatch set_consent |
| CONSENT-04 | Toggle → cancel confirm → set_consent() NOT called, checkbox reverts | GUI (offscreen) | `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry_consent_ux.py::test_settings_toggle_reverts_on_cancel_confirm -x` | Monkeypatch |
| D-07b | Cancel does not overwrite telemetry keys | unit (headless) | `pytest tests/test_telemetry_consent_ux.py::test_settings_cancel_does_not_desync_telemetry -x` | Simulate snapshot strip |
| PRIV-05 | PRIV-03 guard still passes (no new posthog imports) | static AST | `pytest tests/test_telemetry_no_direct_posthog.py -x` | Already existing; must still pass |

### Sampling Rate

- **Per task commit:** `pytest tests/test_telemetry_consent_ux.py -x -q`
- **Per wave merge:** `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen pytest tests/test_telemetry*.py -x`
- **Phase gate:** Full telemetry test suite green before `/gsd:verify-work`

### Wave 0 Gaps

- [ ] `tests/test_telemetry_consent_ux.py` — new file; covers all CONSENT-02/03/04 + D-07b headless + GUI-offscreen cases
- [ ] Wave 0 must establish the test file with at least the headless gate-logic tests before implementation begins

### Test Strategy Notes (Windows-specific)

Per project feedback (`feedback_full_suite_testing_windows.md`): full `pytest tests/` aborts on a non-deterministic PyQt6 headless segfault. Run Qt tests with `GITHUB_ACTIONS=true QT_QPA_PLATFORM=offscreen` and isolate them from the full suite run. The headless tests (config.pkl gate logic, snapshot exemption, shown-flag write) do NOT need Qt and should run without the env vars — keep them separated in the same file using `pytest.mark`.

---

## Environment Availability

Step 2.6: SKIPPED — Phase 112 is pure code changes (PyQt6 dialog composition) with no external tool dependencies. PyQt6 is already installed and verified working in the existing desktop app.

---

## Security Domain

`security_enforcement` is absent from `.planning/config.json` → treated as enabled.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---------------|---------|-----------------|
| V2 Authentication | No | Consent storage in config.pkl is local-only, no auth surface |
| V3 Session Management | No | No sessions involved |
| V4 Access Control | No | Local desktop app |
| V5 Input Validation | Partial | Button labels/dialog text are hardcoded, not user input; no XSS risk in QTextBrowser unless external URLs are opened (mitigated by `setOpenExternalLinks(True)` only for known PostHog/Dicta links) |
| V6 Cryptography | No | No crypto in this phase |

### Known Threat Patterns for this Phase

| Pattern | STRIDE | Standard Mitigation |
|---------|--------|---------------------|
| Cancel desync: config.pkl says opted-out but engine says opted-in | Tampering (privacy overstatement) | D-07b snapshot exemption — strip telemetry keys from snapshot |
| Enter-key implicit opt-in | Elevation of privilege (consent bypass) | `setDefault(False); setAutoDefault(False)` + `keyPressEvent` override |
| PRIV-03 bypass via consent_dialog.py importing posthog_server | Tampering (consent gate bypass) | Existing AST guard in `tests/test_telemetry_no_direct_posthog.py` catches it |
| FIRST_RUN_SHOWN_KEY not written on close/Escape | Spoofing (repeated prompting) | `closeEvent()` override writes the flag unconditionally |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | The sync prompt / joins sync does not show a blocking `exec()` dialog at startup | Startup Sequencing analysis | If it does, D-04 analysis needs to include it; consent might stack on it |
| A2 | SettingsDialog is not re-instantiated when it is re-shown (it calls `.show()` or `.exec()` on the existing instance) | snapshot analysis | If it IS re-instantiated each time, snapshot is always fresh and the strip-at-construction approach is even safer |

**A1 verification:** Searched codebase for sync-prompt blocking dialog in startup path; `joins_mgr.start_background_sync()` is non-blocking. No blocking sync modal found. Confidence: HIGH.

**A2 verification:** Line 3437-3438 shows `self.settings_dialog = SettingsDialog(self)` called ONCE in `init_ui()`. The gear button at line 3520 calls `self._open_settings_dialog`. Searching for `_open_settings_dialog`:

The instance is reused (single construction). This confirms the snapshot is taken once at `__init__` and the strip-at-construction approach is correct.

---

## Open Questions

1. **SettingsDialog `_open_settings_dialog` method**
   - What we know: `self.settings_dialog = SettingsDialog(self)` at line 3438 (constructed once)
   - What's unclear: Whether `_open_settings_dialog` calls `.show()` or `.exec()` — this affects whether subsequent opens trigger any re-init
   - Recommendation: Planner should read `_open_settings_dialog` at implementation time; if it uses `.exec()`, the dialog is modal and destroyed or hidden after close; if `.show()`, it persists. Either way the snapshot-strip approach is correct.

2. **PrivacyDialog Hebrew text**
   - What we know: D-10 lists required content points in English
   - What's unclear: The exact Hebrew translation of the disclosure text
   - Recommendation: Planner writes English text; Hebrew translation can be done by the implementer or via the project's existing translation workflow. Flag as a manual task in Wave 0.

---

## Sources

### Primary (HIGH confidence — verified by reading live source)

- `desktop/telemetry.py` — full file read; confirmed stub at line 712, all KEY constants at lines 68-74, `set_consent()` at line 399, public API surface
- `genizah_app.py:2145-2513` — SettingsDialog: confirmed `_config_snapshot` at line 2179, `_on_cancel` at line 2204-2207, `_build_general_tab` pattern at lines 2210-2350
- `genizah_app.py:284-343` — WhatsNewDialog: confirmed `setLayoutDirection`, single-language via `CURRENT_LANG`
- `genizah_app.py:1447-1508` — HelpDialog: confirmed `QTextBrowser`, language stripping pattern
- `genizah_app.py:2472-2476` — "Local Index Cache Privacy" block: confirmed tone/placement precedent
- `genizah_app.py:3200-3350` — startup path: confirmed `init_ui()` → `MyLibraryTab.__init__()` → `_show_recovery_modal()` (synchronous, during construction); `on_startup_finished()` with `singleShot(500, _show_citation_reminder)` at line 3343
- `genizah_core.py:2848-2901` — `load_language()`, `save_language()`, `load_app_config()`, `save_app_config()`, `CURRENT_LANG`, `tr()` — confirmed D-01 premise
- `desktop/my_library_tab.py:1520-1595` — `_show_recovery_modal()`: confirmed synchronous `mb.exec()`, fires in `MyLibraryTab.__init__` (before window shows)
- `tests/test_telemetry_no_direct_posthog.py` — confirmed PRIV-03 AST guard: scans `desktop/` for `posthog_server` imports, exempts only `desktop/telemetry.py` by resolved path
- `tests/test_telemetry_consent_gate.py` — confirmed test fixture pattern for headless telemetry tests

### Secondary (MEDIUM confidence)

- `.planning/phases/111-telemetry-foundation/111-PATTERNS.md` — confirmed analog assignments and code patterns for all Phase 111 files

---

## Metadata

**Confidence breakdown:**
- Engine API surface: HIGH — read directly from source
- Dialog patterns (WhatsNewDialog/HelpDialog): HIGH — read directly from source
- Startup sequencing / D-04 analysis: HIGH — traced execution path through source
- D-07b snapshot exemption: HIGH — verified `save_app_config` is additive-merge, not full-replace
- PRIV-03 guard scope: HIGH — read the test file and confirmed it scans `desktop/` rglob

**Research date:** 2026-06-14
**Valid until:** 2026-07-14 (stable codebase; PyQt6 API changes are rare)
