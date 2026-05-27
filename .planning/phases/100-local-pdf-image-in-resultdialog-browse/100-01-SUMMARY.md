---
phase: "100"
plan: "01"
subsystem: desktop
tags: [pdf, rendering, controller, qt, pyqt6, desktop-only]
dependency_graph:
  requires:
    - "99-02 (PdfRenderWorker + PdfRenderFailure API)"
  provides:
    - "PdfImageController: per-scope request state, global token counter, latest-wins discard, debounce, watchdog, cancel, discard_scope, extension gate, localized placeholder map"
    - "GenizahGUI._pdf_render_worker (started + stopped cooperatively)"
    - "GenizahGUI._pdf_image_controller (wraps the shared worker)"
  affects:
    - "genizah_app.py (GenizahGUI.__init__ + closeEvent)"
    - "Plans 02 and 03 (wiring ResultDialog + Browse on top of this controller)"
tech_stack:
  added:
    - "desktop/pdf_image_controller.py (new PdfImageController QObject class)"
    - "tests/test_pdf_image_controller.py (34 new unit tests)"
  patterns:
    - "Per-scope QTimer dicts (lazily created, persistent) — mirrors ManuscriptViewerWidget._nav_debounce_timer"
    - "Token-routed worker signal routing via _scope_for_token linear scan"
    - "Watchdog-token guard (_watchdog_token dict) preventing old watchdog from timing out newer request (REVIEWS-R2-1)"
    - "discard_scope() calling deleteLater() on timer QObjects (REVIEWS-R2-3)"
    - "Lazy genizah_core.CURRENT_LANG import inside _lang() to avoid import cycle (REVIEWS LOW-8)"
key_files:
  created:
    - desktop/pdf_image_controller.py
    - tests/test_pdf_image_controller.py
  modified:
    - genizah_app.py
decisions:
  - "Per-scope state on ONE controller (Option A per REVIEWS HIGH-1): Browse and ResultDialog each have their own _awaiting_token/_pending/timer dict entries; one global token counter makes tokens globally unique for cross-scope routing"
  - "_watchdog_token guard (REVIEWS-R2-1): request() stops prior watchdog before overwriting _awaiting_token; _fire_pending records armed token; _on_watchdog no-ops on mismatch"
  - "discard_scope() for transient dialog scopes: cancel + pop + deleteLater() prevents QTimer accumulation per dialog lifecycle (REVIEWS-R2-3)"
  - "_clear_scope() on all terminal states (success/failure/watchdog/cancel) releases _pending callbacks immediately (REVIEWS MEDIUM-4)"
  - "CURRENT_LANG imported lazily inside _lang() from genizah_core (not genizah_app) to keep controller-only tests independent (REVIEWS LOW-8)"
metrics:
  duration: "7 minutes"
  completed_date: "2026-05-27"
  tasks_completed: 3
  files_created: 2
  files_modified: 1
---

# Phase 100 Plan 01: PdfImageController — per-scope request state coordinator

Per-scope latest-wins PDF render controller wrapping one shared PdfRenderWorker, with QTimer debounce + watchdog + token-guarded watchdog race fix + discard_scope timer cleanup.

## What Was Built

### Task 1: PdfImageController (desktop/pdf_image_controller.py)

New `PdfImageController(QObject)` implementing REVIEWS HIGH-1 Option A (one controller, per-scope partitioned state):

- **ONE global monotonic token counter** (`self._token`) shared across all scopes (D-07b). Globally unique tokens allow `_scope_for_token` to route worker results to the correct scope.
- **Per-scope dicts** (`_awaiting_token`, `_pending`, `_debounce_timers`, `_watchdog_timers`, `_watchdog_token`) keyed by the scope argument (string `"browse"` or integer `id(dialog)`).
- **`request(scope, ...)`**: gates on `.pdf` extension (None-safe), stops the scope's prior watchdog (REVIEWS-R2-1), mints a new global token, stores pending state, shows "Loading…" immediately (D-01), restarts the per-scope debounce timer.
- **`cancel(scope, silent=True)`**: clears `_pending[scope]` + `_awaiting_token[scope]` + `_watchdog_token[scope]`, stops scope's debounce + watchdog. A late worker result then finds no matching scope and is silently discarded (REVIEWS HIGH-2).
- **`discard_scope(scope)`**: cancels then pops + `deleteLater()`s the scope's timer dict entries; idempotent (REVIEWS-R2-3). For transient ResultDialog scopes only; Browse uses plain `cancel()`.
- **`_fire_pending(scope)`**: called by debounce timer; re-checks token for sub-debounce coalescing (REVIEWS MEDIUM-7); calls `self._worker.enqueue()`; records `_watchdog_token[scope] = token` and starts the watchdog (REVIEWS-R2-1 guard).
- **`_on_render_succeeded` / `_on_render_failed`**: route via `_scope_for_token`; discard silently if no match; run `_clear_scope` (terminal cleanup) BEFORE invoking the callback for re-entrancy safety (REVIEWS MEDIUM-4).
- **`_on_watchdog(scope)`**: REVIEWS-R2-1 guard: no-op unless `_watchdog_token[scope] == _awaiting_token[scope]`. On match: logs TIMEOUT warning, runs `_clear_scope`, calls `on_placeholder` with the TIMEOUT string.
- **`_clear_scope(scope)`**: helper that pops `_pending`, `_awaiting_token`, `_watchdog_token`, stops the scope's watchdog.
- **Localized placeholder map** (`_PLACEHOLDER_TEXT`): 7 `PdfRenderFailure` reasons → `(he, en)` pairs. `CANCELLED` intentionally absent (silent discard). `_placeholder_for()` selects by `_lang()` → `CURRENT_LANG` from `genizah_core`.

### Task 2: GenizahGUI worker ownership (genizah_app.py)

- Added `from desktop.pdf_page_renderer import PdfRenderWorker` and `from desktop.pdf_image_controller import PdfImageController` imports (after `MyLibraryTab` import).
- In `GenizahGUI.__init__`, anchored to `self.browse_viewer = ManuscriptViewerWidget()`: instantiates `PdfRenderWorker(maxsize=4)`, calls `.start()`, wraps in `PdfImageController`.
- In `GenizahGUI.closeEvent`, anchored to `browse_viewer.stop_threads` semantic string: adds `_pdf_render_worker.stop()` in a `try/except` block (cooperative only, no `.terminate()` per D-07a/D-05).

### Task 3: Unit tests (tests/test_pdf_image_controller.py — 34 tests)

All 34 tests pass. Tests cover:

| Test | Proves |
|------|--------|
| `test_request_returns_global_monotonic_tokens` | ONE shared counter across scopes |
| `test_cross_surface_independence` ★ | REVIEWS HIGH-1 fixed: Browse not stranded by dialog |
| `test_cancel_before_debounce_no_enqueue_no_callback` ★ | REVIEWS HIGH-2: cancel prevents enqueue + callback |
| `test_genizah_nav_before_success_no_stale_display` ★ | REVIEWS HIGH-2: late result discarded after cancel |
| `test_terminal_states_release_callbacks` ★ | REVIEWS MEDIUM-4: parametrize success/failure/watchdog |
| `test_same_sysid_different_page_discards_stale` ★ | Per-scope latest-wins token discard |
| `test_old_watchdog_does_not_timeout_newer_request` ☆ | REVIEWS-R2-1 watchdog-token guard |
| `test_discard_scope_removes_timer_entries` ☆ | REVIEWS-R2-3 timer dict cleanup + idempotent |
| `test_latest_success_displays` | on_image called exactly once |
| `test_non_pdf_extensions_gated_out` (5 extensions) | Extension gate |
| `test_uppercase_pdf_accepted` | .PDF uppercase accepted |
| `test_none_filepath_returns_none` | None-safe filepath |
| `test_placeholder_missing_file` (en + he) | Localized MISSING_FILE placeholder |
| `test_placeholder_per_reason_localized` (10 combos) | All reasons × languages |
| `test_placeholder_cancelled_returns_none` | CANCELLED → None |
| `test_debounce_coalesces` | 3 rapid requests → 1 enqueue with last token |
| `test_watchdog_fires_timeout_placeholder` | TIMEOUT placeholder + late result discarded |
| `test_failure_maps_to_localized_placeholder_via_signal` | render_failed signal → placeholder |

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None. This plan creates the controller infrastructure; no UI stubs exist here. Plans 02 and 03 wire the controller into the actual ResultDialog and Browse surfaces.

## Threat Surface Scan

No new network endpoints, auth paths, or file access patterns introduced. This plan creates a Qt-thread-safe render coordinator that only processes results already validated by the Phase 99 worker (filesystem path already resolved at index time). All threats in the plan's `<threat_model>` are mitigated as specified.

## Self-Check: PASSED

- desktop/pdf_image_controller.py: FOUND
- tests/test_pdf_image_controller.py: FOUND
- Commit 91d8e833 (Task 1 - PdfImageController): FOUND
- Commit 019250d9 (Task 2 - GenizahGUI wiring): FOUND
- Commit caff2ee1 (Task 3 - unit tests): FOUND
