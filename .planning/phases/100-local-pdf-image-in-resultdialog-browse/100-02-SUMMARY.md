---
phase: "100"
plan: "02"
subsystem: desktop
tags: [pdf, rendering, result-dialog, qt, pyqt6, desktop-only, controller-wiring]
dependency_graph:
  requires:
    - "100-01 (PdfImageController — per-scope request state coordinator)"
  provides:
    - "ResultDialog wired to PdfImageController: per-dialog scope, single render trigger, cancel on non-PDF nav, guaranteed scope teardown"
  affects:
    - "desktop/result_dialog.py (ResultDialog.__init__ + load_result_by_index + load_local_page + closeEvent + new helpers)"
tech_stack:
  added: []
  patterns:
    - "Per-dialog scope key id(self) passed to PdfImageController on every request/cancel/discard_scope call"
    - "finished signal teardown (REVIEWS-R2-2): guaranteed scope discard on accept/reject/done/Esc"
    - "Single render trigger at load_local_page success path (REVIEWS HIGH-3 de-dup)"
    - "cancel() for in-session non-PDF navigation; discard_scope() for dialog close (REVIEWS-R2-3)"
key_files:
  created: []
  modified:
    - desktop/result_dialog.py
decisions:
  - "finished.connect(_on_pdf_dialog_finished) is the PRIMARY teardown path (REVIEWS-R2-2); closeEvent discard_scope is belt-and-suspenders (idempotent)"
  - "Single render trigger in load_local_page (not load_result_by_index) avoids duplicate-on-open because load_result_by_index always dispatches LOCAL through load_page -> load_local_page (REVIEWS HIGH-3)"
  - "_cancel_local_pdf_image uses cancel() not discard_scope() because the dialog scope is still live during in-session navigation and may be re-requested"
metrics:
  duration: "8 minutes"
  completed_date: "2026-05-27"
  tasks_completed: 2
  files_created: 0
  files_modified: 1
---

# Phase 100 Plan 02: ResultDialog PDF image wiring Summary

Wire the shared PdfImageController (Plan 01) into ResultDialog so LOCAL PDF hits show their rendered page image in the existing ms_viewer, with per-dialog scope isolation, single render trigger, cancel-on-non-PDF navigation, and guaranteed scope teardown on every dialog-close path.

## What Was Built

### Task 1: Per-dialog scope, finished-signal teardown, and render/cancel helpers (desktop/result_dialog.py)

**`__init__` additions:**
- `self._pdf_scope = id(self)` — per-dialog scope key (REVIEWS HIGH-1). Each ResultDialog gets a unique integer scope so its render state is isolated from Browse's on the shared controller.
- `self.finished.connect(self._on_pdf_dialog_finished)` — guaranteed teardown signal wiring (REVIEWS-R2-2). `finished` fires on accept/reject/done/Esc — all dialog termination paths that `closeEvent` misses.

**New methods:**
- `_pdf_controller()` — safe accessor returning `self._app._pdf_image_controller` or None.
- `_is_current_hit_local() -> bool` — True iff `current_sys_id` is a LOCAL result; uses the established `from shared.local_sys_id import is_local_sys_id` idiom.
- `_render_local_pdf_image()` — the ONLY call site for `controller.request()`. None-safe `controller.is_pdf(fp)` gate (REVIEWS MEDIUM-6 / PDFIMG-05). Reveals `external_pane` + `btn_toggle_image`, then requests a render via `controller.request(self._pdf_scope, ...)` feeding `ms_viewer.display_image` and `ms_viewer.scroll_area.set_status_message` callbacks.
- `_cancel_local_pdf_image()` — calls `controller.cancel(self._pdf_scope, silent=True)` and hides the pane. Uses `cancel` (not `discard_scope`) since the dialog scope is still live during in-session navigation.
- `_on_pdf_dialog_finished(_result)` — idempotent `controller.discard_scope(self._pdf_scope)` on every dialog termination (REVIEWS-R2-2 + R2-3). Releases `_pending` callbacks AND removes scope's debounce/watchdog QTimer dict entries via `deleteLater()`.

### Task 2: Three call sites wired (desktop/result_dialog.py)

**1. Single render trigger — `load_local_page` success path (L2542):**
Added `self._render_local_pdf_image()` at the end of the success block, after `current_p_num` is set (L2478), after spinbox/nav updates — and critically AFTER the early return at boundary/unknown pages (L2475). This is the SINGLE render trigger (REVIEWS HIGH-3): both initial open AND prev/next RESULT navigate here via `load_result_by_index → load_page → load_local_page`, and within-document prev/next PAGE also reaches this path.

**2. Cancel on non-LOCAL-PDF result — `load_result_by_index` (~L2036):**
After the LOCAL-detection try/except, computes `_is_local_pdf` (True only when the new result is a LOCAL file AND `controller.is_pdf(fp)` confirms it's a `.pdf`). If NOT a local PDF, calls `self._cancel_local_pdf_image()` — invalidates any in-flight render and hides the pane. This ensures navigating from a LOCAL PDF to any non-PDF result (Genizah, LOCAL .docx, etc.) immediately hides stale content (REVIEWS HIGH-2).

**3. Discard scope in `closeEvent` (L3229) — belt-and-suspenders:**
Inside the existing `try:` block alongside `ms_viewer.stop_threads()`, added a protected `ctrl.discard_scope(self._pdf_scope)` call. Idempotent with `_on_pdf_dialog_finished` — whichever runs first clears the scope; the second call is a no-op (`cancel/pop` with `pop(scope, None)` semantics). Ensures that an X-close path also removes timer dict entries (REVIEWS-R2-3).

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None. The render/cancel/teardown wiring is complete. The controller (Plan 01) handles all token/debounce/watchdog/placeholder logic; this plan only adds the trigger/cancel call sites and scope lifecycle management.

## Threat Surface Scan

No new network endpoints, auth paths, file access patterns, or schema changes introduced. This plan only wires calls to the Plan 01 controller (which itself only processes results from the Phase 99 worker). All threats in the plan's `<threat_model>` are mitigated as specified:

- T-100-07 (stale image on fast prev/next): mitigated — controller latest-wins token discard; dialog re-requests on every LOCAL page load.
- T-100-08 (non-PDF LOCAL render attempt): mitigated — `controller.is_pdf(fp)` None-safe gate in `_render_local_pdf_image` + controller extension gate.
- T-100-09 (render failure hangs dialog): mitigated — controller watchdog + placeholder; dialog feeds `set_status_message` and never blocks.
- T-100-10 (pane shown for Genizah hit): mitigated — `_cancel_local_pdf_image` called for all non-LOCAL-PDF results in `load_result_by_index`; Genizah `on_enriched_data_loaded` path untouched.
- T-100-19 (late render writes into closed dialog): mitigated — `discard_scope` on EVERY close path (finished signal + closeEvent).
- T-100-20 (retained callbacks + timer accumulation): mitigated — `discard_scope` removes timer dict entries via `deleteLater()` and releases `_pending` callbacks.

## Self-Check: PASSED

- desktop/result_dialog.py: FOUND
- Commit d7de1bed (Task 1 - helpers): FOUND
- Commit e9cb1ade (Task 2 - wiring): FOUND
- `self._pdf_scope = id(self)` in __init__: CONFIRMED (L55)
- `self.finished.connect(self._on_pdf_dialog_finished)` in __init__: CONFIRMED (L58)
- `self._render_local_pdf_image()` call count: EXACTLY 1 (L2542)
- `self._cancel_local_pdf_image()` in load_result_by_index: CONFIRMED (L2044)
- `ctrl.discard_scope(self._pdf_scope)` in closeEvent: CONFIRMED (L3239)
- `controller.discard_scope(self._pdf_scope)` in _on_pdf_dialog_finished: CONFIRMED (L3320)
- AST parse: OK
- ruff check: clean
