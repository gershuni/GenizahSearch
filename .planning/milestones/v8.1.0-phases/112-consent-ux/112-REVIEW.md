---
phase: 112-consent-ux
reviewed: 2026-06-15T00:00:00Z
depth: deep
files_reviewed: 4
files_reviewed_list:
  - desktop/consent_dialog.py
  - desktop/telemetry.py
  - genizah_app.py
  - tests/test_telemetry_consent_ux.py
findings:
  critical: 1
  warning: 5
  info: 3
  total: 9
status: issues_found
resolution: blocker_and_high_value_warnings_fixed
resolved: [CR-01, WR-01, WR-04]
resolved_commit: 2d5db9be
deferred: [WR-02, WR-03, WR-05, IN-01, IN-02, IN-03]
---

> **Resolution (2026-06-15, commit `2d5db9be`):** The BLOCKER **CR-01** (unbounded
> `_maybe_show_first_run_prompt` reschedule poll) is fixed — the reschedule is now
> capped at 200 retries (~60s); past the cap the prompt defers to next launch
> (`FIRST_RUN_SHOWN_KEY` stays unset). **WR-01** (`state == 2` enum fragility on the
> privacy-critical toggle) fixed — the handler now reads `self.chk_telemetry.isChecked()`.
> **WR-04** (silent `except: pass`) fixed — now `logger.debug(exc_info=True)`. 34 telemetry
> tests green, ruff clean after the fix. Deferred as minor/non-blocking: WR-02 (double-`done()`
> guard — benign given idempotent set_consent), WR-03 (dialog-level keyPressEvent test angle —
> the no-opt-in behavior is already proven), WR-05 (documentation note), IN-01/02/03 (style).

# Phase 112: Code Review Report

**Reviewed:** 2026-06-15
**Depth:** deep
**Files Reviewed:** 4
**Status:** issues_found

## Summary

Phase 112 implements the consent UX surface on top of the Phase 111 telemetry engine: a bilingual first-run `ConsentDialog`, a `PrivacyDialog`, a Settings toggle, and startup wiring. The consent-correctness invariants (single `done()` finalizer, no-default buttons, `FIRST_RUN_SHOWN_KEY` written on all exit paths, D-08 sole-`set_consent()` path, D-07b `_config_snapshot` exemption) are all correctly implemented. PRIV-03 is respected — `consent_dialog.py` imports `desktop.telemetry` only, never `shared.posthog_server`. The `_open_settings_dialog` stale-checkbox refresh (REVIEWS HIGH-3) is present and signal-guarded.

One **BLOCKER** was found: an unbounded rescheduling loop in `_maybe_show_first_run_prompt`. If a persistent modal is present for an extended time (e.g., user opens Settings immediately, or the "Index Missing" dialog keeps looping), the 300 ms timer fires indefinitely with no back-off or iteration cap, creating a busy-reschedule accumulation. Five **WARNINGs** cover: (1) a PyQt6 version portability issue with `state == 2`; (2) `done()` called twice if `reject()` is triggered on an already-accepted dialog; (3) a gap in the test for the "Enter key on focused button" scenario; (4) bare `pass` in `_maybe_show_first_run_prompt` swallowing real exceptions; and (5) `install_exception_hooks()` having a missing `return` statement (silently returns `None` as a stub body with a comment, though this is intentional the comment is misleading as a docstring promise). Three **Info** items cover minor code-quality observations.

---

## Critical Issues

### CR-01: Unbounded rescheduling loop in `_maybe_show_first_run_prompt`

**File:** `genizah_app.py:15849–15852`
**Issue:** When `QApplication.activeModalWidget() is not None`, the function schedules itself again after 300 ms with no guard on how many times it reschedules. If a modal widget remains open for a sustained period (e.g., the "Index Missing" `QMessageBox.question` at line 3473, which blocks synchronously but could keep the event loop spinning if re-shown, or a user who opens Settings immediately after startup and leaves it open for minutes), `QTimer.singleShot(300, self._maybe_show_first_run_prompt)` fires again and again, each scheduling another call. Because `QTimer.singleShot` does not dedup, after N reschedulings there are N pending timer callbacks. Under normal usage this drains fast once the modal closes, but if a modal stays open for 30 seconds that is 100 pending invocations of `_maybe_show_first_run_prompt` queued up — they then all fire in sequence when the modal closes, each calling `show_first_run_prompt` which calls `load_app_config()` and (on the first successful call) writes `FIRST_RUN_SHOWN_KEY=True`. Subsequent calls hit the early-return gate, so consent is written once. **The privacy/correctness risk is low** but there is a tangible **startup-correctness risk**: each queued invocation loads config from disk; 100 rapid `load_app_config()` calls on the UI thread can produce a perceptible lag spike when a long-running modal finally closes.

Additionally, the outer `except Exception: pass` (line 15854) means if this reschedule loop accumulates and something in `show_first_run_prompt` raises, the bug is silently swallowed with no log.

**Fix:** Add a reschedule counter or use a flag so the dialog is attempted at most once after the modal clears. The simplest safe fix is a class-level or closure-level counter:

```python
def _maybe_show_first_run_prompt(self, _retry: int = 0) -> None:
    try:
        from genizah_core import load_app_config as _load_cfg
        from desktop.telemetry import FIRST_RUN_SHOWN_KEY, show_first_run_prompt
        if _load_cfg().get(FIRST_RUN_SHOWN_KEY, False):
            return
        if QApplication.activeModalWidget() is not None:
            if _retry < 60:  # give up after ~18 s (60 x 300 ms)
                QTimer.singleShot(300, lambda: self._maybe_show_first_run_prompt(_retry + 1))
            return
        show_first_run_prompt(self)
    except Exception:
        logger.debug('_maybe_show_first_run_prompt failed', exc_info=True)
```

Alternatively use an instance-level `_consent_prompt_scheduled: bool` flag set before the singleShot and cleared on entry, so at most one timer is live at any time.

---

## Warnings

### WR-01: `state == 2` is PyQt6 version-fragile for `stateChanged`

**File:** `genizah_app.py:2339`
**Issue:** `_on_telemetry_changed` checks `new_val = (state == 2)`. In PyQt6, `QCheckBox.stateChanged` passes an `int` that corresponds to `Qt.CheckState` enum values: `Unchecked=0`, `PartiallyChecked=1`, `Checked=2`. The magic number `2` works in current PyQt6 because the enum's integer value is still `2`, and the codebase uses this pattern in two other pre-existing places (lines 2263, 2301). However, PyQt6 6.x has been moving toward stricter enum handling in some APIs; `stateChanged` in newer bindings may emit a `Qt.CheckState` enum object rather than a raw int, in which case `state == 2` is `False` even when the box is checked (it would need `state == Qt.CheckState.Checked`).

The same pattern appears at lines 2263 and 2301 (pre-Phase-112 code), so this is an inherited project pattern rather than a new defect introduced by Phase 112. However, the telemetry checkbox is privacy-critical: if this comparison silently returns `False` after a PyQt6 upgrade, the toggle appears to work visually but never calls `set_consent(True)`, silently preventing opt-in. The other two callsites control notifications and translation preferences — less critical.

**Fix:** Use the enum-safe comparison:
```python
from PyQt6.QtCore import Qt
new_val = (state == Qt.CheckState.Checked.value) or (state == Qt.CheckState.Checked)
# or simply:
new_val = bool(state == 2 or state == Qt.CheckState.Checked)
```
The most defensive fix is:
```python
from PyQt6.QtCore import Qt
new_val = state in (2, Qt.CheckState.Checked)
```

### WR-02: `done()` called twice on `accept()` after `reject()` path

**File:** `desktop/consent_dialog.py:201–209`
**Issue:** Qt's `QDialog.done()` override is the correct single-finalizer pattern, but there is a subtle double-call risk: if code outside the dialog calls `dlg.reject()` (which calls `done(Rejected)`) and then the dialog is still alive and `dlg.accept()` is called (or vice versa), `done()` fires twice. In that scenario `set_consent()` is called twice with possibly different arguments.

More concretely: the test `test_consent_dialog_close_opts_out` (line 416) calls `dlg.reject()`, then relies on `reject()→done()`. But `test_consent_dialog_no_default_button` (line 333) calls `dlg.reject()` as cleanup at the end. If a test drives `dlg._on_enable()` (which calls `accept()` → `done(Accepted)`) and then cleanup code calls `dlg.reject()` again, `done()` fires a second time with `_accepted_telemetry=True` (set before the first call), and `set_consent(True)` is called again. In production this is harmless because `set_consent(True)` is idempotent (re-mints nothing if install_id already exists), but the double `save_app_config` write on every close is wasteful. More importantly, after the first `done()` call, `super().done(result)` hides the dialog; a second `done()` call on a hidden dialog may trigger a Qt warning.

**Fix:** Guard against double-call with an internal flag:
```python
def __init__(self, parent=None):
    ...
    self._done_called: bool = False

def done(self, result: int) -> None:
    if self._done_called:
        return
    self._done_called = True
    genizah_core.save_app_config({FIRST_RUN_SHOWN_KEY: True})
    telemetry.set_consent(self._accepted_telemetry)
    super().done(result)
```

### WR-03: Enter-key test sends event to button, not to dialog — gap in coverage

**File:** `tests/test_telemetry_consent_ux.py:356–357`
**Issue:** `test_consent_dialog_enter_is_decline` sends `Key_Return` to `dlg.btn_enable` (the focused widget) via `QApplication.sendEvent(dlg.btn_enable, key_event)`. This tests whether the button itself consumes the Return event. However, the dialog's `keyPressEvent` override (the belt-and-braces guard in `consent_dialog.py:186`) is on the `QDialog` level — it fires when the event reaches the dialog after not being consumed by a child. The test does NOT also send the event to `dlg` itself. So the test covers: "does btn_enable activate on Key_Return sent directly to it?" but does NOT cover: "if the button does NOT consume it and it bubbles to the dialog, does keyPressEvent route to decline?"

In practice, because `setAutoDefault(False)` is set, a focused `QPushButton` should NOT activate on Return unless it is the default button. The implementation is likely correct. But the test comment claims it covers the "worst case: a focused QPushButton could consume Return before the dialog's keyPressEvent" — it only partially proves this because it checks `consent_calls` after posting to the button without verifying the dialog's keyPressEvent ran.

**Fix:** Add a second assertion that posts the event to the dialog directly to verify the keyPressEvent route:
```python
# Also verify the dialog's keyPressEvent correctly routes to decline
key_event2 = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Return, Qt.KeyboardModifier.NoModifier)
QApplication.sendEvent(dlg, key_event2)
QApplication.processEvents()
assert True not in consent_calls, "Return sent to dialog must not opt in"
```

### WR-04: Bare `pass` in `_maybe_show_first_run_prompt` exception handler swallows real errors

**File:** `genizah_app.py:15854`
**Issue:** The outer `except Exception: pass` is silent — no `logger.debug` or `logger.warning`. This is different from the project's established no-raise contract in `desktop/telemetry.py` (which logs at DEBUG with `exc_info=True`). If `show_first_run_prompt` raises (e.g., because `ConsentDialog` construction fails due to a missing Qt resource, or a PyQt6 import path breaks), the failure is completely invisible. The user never sees the consent dialog and no diagnostic information is emitted.

**Fix:** Log at debug level so at least a dev run can catch it:
```python
except Exception:
    logger.debug('_maybe_show_first_run_prompt failed', exc_info=True)
```

### WR-05: `show_first_run_prompt` double-gate redundancy creates false confidence but misses a real case

**File:** `genizah_app.py:15847–15848` and `desktop/telemetry.py:724–726`
**Issue:** `_maybe_show_first_run_prompt` checks `FIRST_RUN_SHOWN_KEY` from `load_app_config()` before calling `show_first_run_prompt`, and then `show_first_run_prompt` itself also checks the same key. This is belt-and-suspenders and is fine. However, there is an ordering hazard: both read `FIRST_RUN_SHOWN_KEY` from disk **before** `done()` writes it. Because `done()` writes `FIRST_RUN_SHOWN_KEY=True` synchronously (via `save_app_config`), and the dialog is `exec()`'d (blocking modal loop), the sequence is safe in normal operation. The edge case is: if `_maybe_show_first_run_prompt` is rescheduled (WR-01 scenario), the rescheduled call reads `FIRST_RUN_SHOWN_KEY` from disk. If the first call successfully showed the dialog and `done()` wrote the flag, `load_app_config()` should see `True` and return early. This is correct — `save_app_config` is synchronous. So the double-gate does work as intended, but only because `save_app_config` is truly synchronous. If a future change makes `save_app_config` async or deferred, this gate silently fails. A code comment clarifying this assumption would make the intent explicit.

This is a documentation/robustness warning, not a current bug.

**Fix:** Add a comment at the `_maybe_show_first_run_prompt` gate clarifying:
```python
# Note: the disk read is safe because save_app_config is synchronous;
# if it becomes deferred this gate needs an in-memory fallback.
if _load_cfg().get(FIRST_RUN_SHOWN_KEY, False):
    return
```

---

## Info

### IN-01: `install_exception_hooks()` stub body is a comment, not a `pass` — misleading as a no-op

**File:** `desktop/telemetry.py:704–709`
**Issue:** The stub function body is just a comment `# Phase 113 implementation` with no `pass` statement. In Python this is valid (a comment-only body is implicitly `pass`). However, the docstring promises "Consent-gated no-op" — the function is a genuine stub, not a no-op with a consent gate. A reader implementing Phase 113 might not notice the function body needs to be replaced (not extended). The pattern is consistent with `pass`-as-comment elsewhere but could mislead.

**Fix:** Either add an explicit `pass` or document clearly that the body needs full replacement:
```python
def install_exception_hooks() -> None:
    """Install crash-capture exception hooks. Implemented in Phase 113. Never raises."""
    pass  # Phase 113 replaces this entire body
```

### IN-02: `telemetry.set_consent(True if self._accepted_telemetry else False)` is redundant

**File:** `desktop/consent_dialog.py:208`
**Issue:** `True if self._accepted_telemetry else False` is functionally identical to `bool(self._accepted_telemetry)` and since `_accepted_telemetry` is already typed as `bool`, is identical to `self._accepted_telemetry`. The ternary adds visual noise that obscures the intent.

**Fix:**
```python
telemetry.set_consent(self._accepted_telemetry)
```

### IN-03: Test `test_settings_checkbox_refreshes_on_open` manually simulates the refresh instead of exercising `_open_settings_dialog`

**File:** `tests/test_telemetry_consent_ux.py:742–755`
**Issue:** The test inlines the refresh logic (blockSignals → setChecked → blockSignals) rather than calling the actual `_open_settings_dialog` method on a mock `GenizahGUI` instance. This means if someone changes the refresh logic in `_open_settings_dialog` (e.g., adds an exception guard but forgets to call `setChecked`), the test still passes because it tests the inlined simulation, not the real code path. The test's comment on line 743 explicitly acknowledges it is "simulating `_open_settings_dialog`'s refresh logic" — this is a meaningful coverage gap for the REVIEWS HIGH-3 finding it was added to verify.

**Fix:** Either: (a) construct a `GenizahGUI`-like stub that has a `settings_dialog` attribute pointing at the partially-initialized `sd` object, and call `_open_settings_dialog` directly; or (b) at minimum assert the behavior against `genizah_app.GenizahGUI._open_settings_dialog`'s source code via AST inspection (as the project does for other guard tests).

---

_Reviewed: 2026-06-15_
_Reviewer: Claude Sonnet 4.6 (gsd-code-reviewer)_
_Depth: deep_
