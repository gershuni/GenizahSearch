---
status: resolved
trigger: "two bugs in desktop app: 1. minimizing ResultDialog minimizes the app itself."
created: 2026-09-17T00:00:00Z
updated: 2026-09-17T01:15:00Z
---

## Current Focus

hypothesis: CONFIRMED — see Resolution.root_cause. ResultDialog has no independent minimize
affordance (default Qt::Dialog flags = title+system-menu+close only, no WS_MINIMIZEBOX) and is
an owned window of the QMainWindow on Windows (Qt sets GWL_HWNDPARENT via
QWindowsWindow::updateTransientParent()); the only minimize path reachable from the dialog's
context is the shared taskbar entry / global shell command acting on the OWNER, and Win32
owned-window semantics then hide every owned window when the owner minimizes.
test: reasoning_checkpoint written below; fix = add Qt.WindowType.WindowMinimizeButtonHint to
ResultDialog's existing flags in __init__ (single site, covers all six construction call sites).
expecting: dialog gains its own WS_MINIMIZEBOX; minimizing it no longer routes through the
owner's taskbar entry; main window unaffected. Native minimize behavior itself is NOT verifiable
offscreen — see Resolution.verification for what a test CAN and CANNOT prove, and the
human-verify checkpoint for what the owner must confirm by hand.
next_action: AWAITING HUMAN VERIFICATION on real Windows -- fix applied and self-verified (see
Resolution.verification); native minimize behavior cannot be exercised offscreen. Owner must
open a search result's ResultDialog, click its title-bar minimize button, and confirm the main
window stays visible/enabled; then confirm the dialog is restorable (click the shared taskbar
button/group). If confirmed: archive_session (move file to resolved/, append knowledge base).
If NOT confirmed: reopen investigation_loop Phase 2 with this finding recorded as new Evidence.

### reasoning_checkpoint

```yaml
reasoning_checkpoint:
  hypothesis: >
    ResultDialog minimizes the whole app because (1) it carries no WindowMinimizeButtonHint
    (default Qt::Dialog flags), so it has no minimize affordance of its own, and (2) on Windows
    it is an owned window of the QMainWindow (Qt sets GWL_HWNDPARENT to the parent's HWND), so it
    shares the main window's single taskbar entry; the only minimize action reachable from the
    dialog's context therefore targets the OWNER, and Win32 owned-window semantics cascade that
    owner-minimize down to hide every owned window, including the dialog.
  confirming_evidence:
    - "desktop/result_dialog.py:44-99 -- ResultDialog(QDialog) constructed with parent=main
      window (all 6 sites in genizah_app.py), no setWindowFlags/setWindowModality anywhere;
      default Qt::Dialog hints are Title+SystemMenu+Close only (Qt docs) -- no WS_MINIMIZEBOX."
    - "qtbase qwindowswindow.cpp (dev branch, tracks 6.10) lines 1936-1962:
      QWindowsWindow::updateTransientParent() calls
      SetWindowLongPtr(m_data.hwnd, GWL_HWNDPARENT, ...) for any QWindow with a transientParent
      -- this is the exact code path that makes ResultDialog an owned window of the main window
      on Windows."
    - "MS Learn Win32 'Window Features' + MS Learn troubleshoot doc on owner/taskbar-button
      creation (fetched via WebSearch): an owned window is hidden when its owner is minimized
      (Windows sends WM_SHOWWINDOW to hide it), and owned windows normally share the owner's
      taskbar button rather than getting an independent one."
    - "grep of qwindowswindow.cpp's minimize/state-change paths (handleWindowStateChange,
      setWindowState_sys, ~lines 2654-2925) shows no transient-parent/owner special-casing --
      rules out a Qt-internal minimize-propagation bug; the cascade is native Win32 shell
      behavior, consistent with the hypothesis."
    - "Qt Forum thread 55741 (Chris Kawa, Qt Lifetime Champion, fetched full post text via
      curl): confirms this exact symptom class exists in Qt/Windows dialogs, and that the
      documented independent-minimize fix is to ADD Qt::WindowMinimizeButtonHint to a
      Qt::Dialog-typed window -- NOT to switch it to Qt::Window while modal (that combination is
      the OP's actual bug: 'the Qt::Window flag makes the widget a normal window, so when shown
      modally it is treated as main app window and thus minimizing it minimizes whole app')."
    - "desktop/join_workbench.py:4291-4292 already uses the identical pattern
      (self.setWindowFlags(self.windowFlags() | Qt.WindowType.WindowMaximizeButtonHint)) on a
      QDialog subclass in this codebase, without changing window type/parent/modality --
      existing, working precedent for the chosen fix mechanism."
  falsification_test: >
    If the dialog, once given WindowMinimizeButtonHint, still drags the main window down when
    the user clicks the DIALOG'S OWN new minimize button (not the shared taskbar entry), the
    owned-window-cascade hypothesis is wrong in the reverse direction and the mechanism is
    something else (e.g. a Qt/EnableWindow interaction specific to ApplicationModal). This
    requires native Windows testing -- flagged for the human-verify checkpoint, cannot be run
    offscreen.
  fix_rationale: >
    Adding Qt.WindowType.WindowMinimizeButtonHint gives the dialog's own HWND a working
    WS_MINIMIZEBOX, so SC_MINIMIZE can target the dialog directly instead of forcing the user
    through the shared taskbar entry that targets the owner. This addresses the root cause (no
    independent minimize affordance) without touching window type, parent, or modality --
    unlike constraint option (b) taken literally (Qt.WindowType.Window), which the Qt Forum
    evidence shows is the DOCUMENTED CAUSE of a structurally identical bug, not a fix for it.
  blind_spots: >
    Cannot exercise native minimize on Windows offscreen (QT_QPA_PLATFORM=offscreen has no real
    window manager). Untested by this session: (1) whether the minimized dialog remains
    reachable to restore, given the main window is disabled (ApplicationModal from .exec()) and
    owned windows typically lack their own taskbar button -- MS Learn's WPF/taskbar-button
    doc says the shell activates 'the correct window in the ownership group' on taskbar click,
    which should restore the dialog, but this is not independently confirmed for Qt's exact HWND
    setup; (2) whether Alt+Space system-menu Minimize, Win+Down, and the taskbar route now all
    correctly target the dialog once WS_MINIMIZEBOX is present, vs. only the title-bar button.
    Both require the owner's hands-on Windows verification.
  candidate_causes:
    - "code: ResultDialog never sets Qt.WindowType.WindowMinimizeButtonHint -- relies on
      Qt::Dialog's default hints (title+system-menu+close only)"
    - "environment: Windows Win32 owned-window semantics -- Qt sets GWL_HWNDPARENT on the
      dialog's HWND because it is constructed with the QMainWindow as parent, so the dialog
      shares the owner's taskbar entry and is subject to the MSDN-documented
      owner-minimize-hides-owned-windows cascade"
  and_gate: >
    yes -- the bug needs BOTH conditions at once. The missing minimize hint (code) is harmless
    by itself if the dialog were not an owned window (e.g. if shown without a parent); the
    owned-window relationship (environment: Windows-specific) is harmless by itself if the
    dialog had its own minimize affordance, since minimizing an owned window directly does not
    cascade back up to the owner (no such rule is documented, and qwindowswindow.cpp's
    state-change code confirms Qt does not implement one). Only the combination -- owned window
    AND no independent minimize control -- forces the user through the shared taskbar/owner path
    that triggers the documented cascade.
```

## Symptoms

expected: Minimizing the ResultDialog (manuscript reading/result viewer) should minimize only that dialog, leaving the main GenizahSearch window where it is
actual: Minimizing the ResultDialog minimizes the whole application (main window disappears too)
errors: None reported
reproduction: Desktop app (PyQt6 6.10.2 / Qt 6.10.0, Windows 11): run a search, open a result in the ResultDialog, minimize the dialog; the main window minimizes as well
started: Unknown; reported 2026-09-17

## Pre-gathered evidence (orchestrator, 2026-09-17)

- desktop/result_dialog.py:44 `class ResultDialog(QDialog)`; :50-51 `__init__(self, parent, ...)` -> `super().__init__(parent)`; :99 `self.resize(1300, 850)`. No setWindowFlags / setWindowFlag / setWindowModality / changeEvent anywhere in the file.
- Every construction site passes `self` (the QMainWindow) as parent and calls `.exec()`: genizah_app.py:11575, 13211, 13283, 22357, 27947, 30882-30883.
- The only dialog in the codebase that touches min/max hints is desktop/join_workbench.py:4292 (`windowFlags() | Qt.WindowType.WindowMaximizeButtonHint`).
- No `changeEvent` / `WindowStateChange` / `showMinimized` handler exists in genizah_app.py or desktop/*.py, so nothing in app code minimizes the main window on purpose; the behaviour must come from Qt/Windows owned-window semantics.

## Eliminated

- hypothesis: "Fix by switching ResultDialog's window type to Qt.WindowType.Window (constraint
  option (b), taken literally) while keeping .exec() and the existing parent."
  evidence: Qt Forum thread 55741 (Chris Kawa, Qt Lifetime Champion; full post fetched via
  curl) documents this exact combination as the CAUSE of a structurally identical bug in
  another reporter's app -- "the Qt::Window flag makes the widget a normal window, so when it
  is shown modally it is treated as main app window and thus minimizing it minimizes whole
  app." Adopting it here would trade one occurrence of the reported symptom for another;
  eliminated as the implementation route while keeping its INTENT (independent minimize),
  delivered instead via WindowMinimizeButtonHint on the existing Qt::Dialog type.
  timestamp: 2026-09-17T01:00:00Z

## Evidence

- timestamp: 2026-09-17T00:40:00Z
  checked: Qt docs (via WebSearch) for QDialog/Qt::Dialog default window hints.
  found: Qt::Dialog's default hints are WindowTitleHint | WindowSystemMenuHint |
  WindowCloseButtonHint only -- no WindowMinimizeButtonHint/WindowMaximizeButtonHint. Matches
  pre-gathered evidence that no code sets flags on ResultDialog.
  implication: ResultDialog's title bar has no minimize button at all; whatever the user calls
  "minimizing the dialog" cannot be a click on the dialog's own chrome.

- timestamp: 2026-09-17T00:45:00Z
  checked: qtbase/src/plugins/platforms/windows/qwindowswindow.cpp (dev branch, fetched via
  curl from raw.githubusercontent.com), QWindowsWindow::updateTransientParent(), lines
  1936-1962.
  found: "Update the transient parent for a toplevel window... by setting the parent using
  GWL_HWNDPARENT" -- SetWindowLongPtr(m_data.hwnd, GWL_HWNDPARENT, LONG_PTR(newTransientParent))
  runs whenever window()->transientParent() is set, i.e. whenever a QWidget/QDialog is
  constructed with a widget parent.
  implication: Confirms directly from Qt 6 source that ResultDialog(parent=main_window) makes
  the dialog a genuine Win32 "owned window" of the QMainWindow's HWND, not just a Qt-level
  convention.

- timestamp: 2026-09-17T00:50:00Z
  checked: qwindowswindow.cpp minimize/window-state-change code paths (handleWindowStateChange
  ~2654, setWindowState_sys ~2798-2925) for any transient-parent/owner special-casing.
  found: No references to GWL_HWNDPARENT, GW_OWNER, or transientParent() anywhere in the
  minimize/state-change handling.
  implication: Rules out a Qt-internal "propagate minimize to owner" bug; the cascade is native
  Win32 shell/taskbar behavior outside Qt's control, consistent with the owned-window
  hypothesis and inconsistent with a Qt-code-level competing hypothesis.

- timestamp: 2026-09-17T00:55:00Z
  checked: MS Learn Win32 "Window Features" doc + MS Learn troubleshoot doc "The owner window
  gets activated when taskbar buttons are created" (via WebSearch).
  found: "An owned window is hidden when its owner is minimized... Windows automatically hides
  the associated owned windows" (WM_SHOWWINDOW cascade, official Win32 doc). Separately: owned
  windows normally share the owner's taskbar button ("ownership group") rather than getting an
  independent one; clicking that shared button makes the system "activate the correct window in
  the ownership group."
  implication: Confirms both halves of the mechanism -- (1) the shared taskbar button is the
  only minimize affordance reachable while a minimize-button-less owned dialog has focus, and
  (2) minimizing the owner via that button cascades to hide the owned dialog too, producing
  "minimizing the dialog minimizes the app."

- timestamp: 2026-09-17T01:00:00Z
  checked: desktop/join_workbench.py:4272-4294 (CompareDialog(QDialog)) for existing
  window-flag precedent in this codebase.
  found: self.setWindowFlags(self.windowFlags() | Qt.WindowType.WindowMaximizeButtonHint) run
  in __init__ right after super().__init__(wb) and setWindowTitle/resize, on a QDialog
  subclass, without changing window type, parent, or (for that dialog) modality.
  implication: This exact OR-in-a-button-hint pattern is already used and presumably working in
  this codebase; strong precedent that the chosen fix mechanism (add
  WindowMinimizeButtonHint the same way) is safe and idiomatic here.

## Resolution

root_cause: >
  Two conditions combine (AND-gate -- see reasoning_checkpoint): (1) code -- ResultDialog
  (desktop/result_dialog.py) never sets Qt.WindowType.WindowMinimizeButtonHint, so it inherits
  Qt::Dialog's default hints (title+system-menu+close only) and has no independent minimize
  affordance; (2) environment -- on Windows, because ResultDialog is constructed with the
  QMainWindow as parent, Qt marks it as an owned window of the main window
  (QWindowsWindow::updateTransientParent() sets GWL_HWNDPARENT), so it shares the main window's
  single taskbar entry instead of getting its own. The only minimize action reachable from the
  dialog's context is therefore the shared taskbar entry / global shell command, which targets
  the OWNER (main window); per documented Win32 owned-window semantics, minimizing the owner
  automatically hides every owned window (including the dialog), so the whole app appears to
  minimize when the user tries to minimize "just the dialog."
fix: >
  desktop/result_dialog.py ResultDialog.__init__ (single site, covers all six construction
  call sites in genizah_app.py: 11575, 13211, 13283, 22357, 27947, 30882) now runs
  `self.setWindowFlags(self.windowFlags() | Qt.WindowType.WindowMinimizeButtonHint |
  Qt.WindowType.WindowSystemMenuHint)` immediately after super().__init__(parent), before the
  best-effort telemetry call and before init_ui()/load_result_by_index(). This ORs the minimize
  hint onto the existing Qt::Dialog defaults -- it does NOT change window type (still
  Qt::Dialog, never Qt::Window), parent (still the QMainWindow -- the owned-window relationship
  and its "always above the owner" stacking are unchanged), or modality (still forced
  ApplicationModal by .exec() at every call site). ELIMINATED implementation: switching to
  Qt.WindowType.Window, which a Qt Forum thread (55741, Chris Kawa) documents as the actual
  CAUSE of the same symptom class elsewhere ("the Qt::Window flag makes the widget a normal
  window ... minimizing it minimizes whole app").
verification: |
  Guardrail signals run this session (native minimize itself cannot be exercised offscreen --
  see blind_spots in the reasoning_checkpoint):
  - Regression test added: tests/test_result_dialog_minimize.py (5 tests). Confirmed RED before
    the fix (manually reverted the setWindowFlags block, reran -- all 5 failed with the
    expected "found 0" / node-not-found errors) and GREEN after restoring it
    (`QT_QPA_PLATFORM=offscreen python -m pytest tests/test_result_dialog_minimize.py -q` ->
    5 passed). This is a genuine bug-catching test, not a vacuous one.
  - What the test pins: (1) AST/source -- ResultDialog.__init__ contains exactly one
    self.setWindowFlags(...) call, ORing (not replacing) WindowMinimizeButtonHint onto
    self.windowFlags(), running before the telemetry call; (2) it does NOT reference
    Qt.WindowType.Window (guards the eliminated hypothesis); (3) real PyQt6 offscreen: applying
    the exact flag names the source references to a bare QDialog(owner) adds the
    WindowMinimizeButtonHint bit while leaving windowType()==Qt.WindowType.Dialog and
    parent() is owner unchanged, both before and after.
  - What the test CANNOT pin (explicitly documented in the test file's module docstring and
    here): actual native minimize behavior on Windows -- whether clicking the dialog's new
    minimize button leaves the main window alone, and whether the minimized dialog stays
    reachable to restore given the main window is disabled while the dialog is
    ApplicationModal. QT_QPA_PLATFORM=offscreen creates no real HWND/window manager, so there
    is no taskbar, no WS_MINIMIZEBOX rendering, and no owned-window cascade to observe.
  - Adjacent regression check: ran all four sibling ResultDialog test files plus the new one
    together (`QT_QPA_PLATFORM=offscreen python -m pytest tests/test_result_dialog_cite.py
    tests/test_result_dialog_highlight_persistence.py
    tests/test_result_dialog_local_button_removed.py tests/test_result_dialog_manuscript_nav.py
    tests/test_result_dialog_minimize.py -q`) -> 48 passed, no regressions.
  - Static checks: `python -m py_compile desktop/result_dialog.py` -> OK;
    `python -m ruff check desktop/result_dialog.py tests/test_result_dialog_minimize.py` ->
    all checks passed.
  - guardrail_verdict: accepted, WITH an unresolved human-verify item (native minimize
    behavior) -- see the CHECKPOINT REACHED returned to the user. Do not treat this session as
    resolved until the owner confirms on real Windows.
files_changed:
  - desktop/result_dialog.py
  - tests/test_result_dialog_minimize.py (new)

## Evidence (orchestrator, 2026-09-17, after owner falsified the first fix)

- timestamp: 2026-09-17T02:00:00Z
  checked: Owner ran the app with the WindowMinimizeButtonHint fix applied
  found: Clicking the dialog's minimize button still "minimizes the entire window"
  implication: The shared-taskbar-entry hypothesis is FALSIFIED; the hint changed nothing.
- timestamp: 2026-09-17T02:20:00Z
  checked: scratchpad/minimize_probe.py -- live-desktop PyQt6 6.10 probe, QMainWindow + QDialog in 9 configurations (default flags / +MinHint / parent=None / setTransientParent(None) / Qt.Window / NonModal / main maximized), dialog sent WM_SYSCOMMAND SC_MINIMIZE, then IsIconic/IsWindowVisible/IsWindowEnabled/GetForegroundWindow read back
  found: (1) a DEFAULT QDialog on this Qt already has WS_MINIMIZEBOX, so the applied fix was a no-op; (2) in EVERY configuration the dialog goes iconic and the MAIN WINDOW DOES NOT (IsIconic False, visible True); (3) with ApplicationModal the main window is disabled (IsWindowEnabled False) and after the minimize the foreground goes to a third-party window
  implication: Nothing minimizes the main window. The owned dialog minimizes to nowhere (no taskbar button of its own), the modal main window cannot take activation, so another app's window comes to the front and covers the maximized main window -- to the user the whole app appears to have minimized. The real defect is the modal + owned design, not a flag.
- timestamp: 2026-09-17T02:25:00Z
  checked: grep genizah_app.py / desktop/*.py for changeEvent, hideEvent, WindowStateChange, showMinimized, eventFilter on the app
  found: no app code reacts to the dialog's window state
  implication: confirms the mechanism is native window ownership + modality, not a handler.

## Resolution (superseded -- the hint fix was reverted 2026-09-17; desktop/result_dialog.py is back to HEAD; tests/test_result_dialog_minimize.py still pins the reverted flags and should be removed)

root_cause: ResultDialog is an ApplicationModal (.exec()) owned window of the main window. Minimizing it hides it with no taskbar button to bring it back, and the disabled owner cannot take focus, so the next application comes forward over the main window.
fix: NOT APPLIED -- owner decision pending between (1) remove the minimize affordance (WindowMinimizeButtonHint cleared + CustomizeWindowHint), (2) make ResultDialog a non-modal independent top-level window (Qt parent None, show() not exec(), reference held by the app, single instance) so it has its own taskbar button and the main search panel stays usable alongside it.

## Resolution (option 1, owner-chosen 2026-09-17: independent non-modal window)

root_cause: ResultDialog was an application-modal (.exec()) Win32 owned window of the main window. Minimizing it hid it with no taskbar button of its own; the disabled owner could not take focus, so Windows activated the next application over the maximized main window. Measured on the live desktop: IsIconic(main) stayed False in all nine configurations, so nothing ever minimized the main window -- it was buried.
fix: desktop/result_dialog.py -- ResultDialog.__init__ passes None to QDialog (host kept as self._app), setModal(False), WindowMinMaxButtonsHint ORed onto the default flags; the two daemon-thread signal emits tolerate a deleted dialog. genizah_app.py -- new GenizahGUI._show_result_dialog(results, index) closes the open viewer, constructs the dialog, holds it in self._result_dialog, centres it over the main window, show()/raise_()/activateWindow(); _on_result_dialog_finished releases the reference and deleteLater()s. All six former exec() sites call the helper. self._result_dialog = None initialised beside _puzzle_window.
verification: tests/test_result_dialog_minimize.py rewritten to the new contract (8 tests; the AST route test and init test both fail against HEAD sources -- checked). 391 tests green across the ResultDialog siblings, layering guards, telemetry, PDF controller and passage-gate files; ruff clean. Native behaviour still needs the owner: open a result, minimize the viewer -> only the viewer minimizes and it has its own taskbar button; main window stays usable; opening another result replaces the viewer; Esc/X closes cleanly.
files_changed:
  - desktop/result_dialog.py
  - genizah_app.py
  - tests/test_result_dialog_minimize.py
