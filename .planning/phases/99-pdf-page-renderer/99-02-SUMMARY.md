---
phase: 99-pdf-page-renderer
plan: "02"
subsystem: desktop/pdf-renderer
tags: [pdf, rendering, qthread, worker, signals, tokenized, queue, desktop-only]
dependency_graph:
  requires: [desktop/pdf_page_renderer.py (Plan 01 render core)]
  provides: [PdfRenderWorker, render_succeeded signal, render_failed signal]
  affects: [desktop/pdf_page_renderer.py, tests/test_pdf_page_renderer.py]
tech_stack:
  added: []
  patterns:
    - queue.Queue blocking get() + _STOP sentinel for long-lived QThread loop
    - _handle_request extraction for synchronous testability (REVIEW item 1 Codex HIGH)
    - DirectConnection + threading.Event for pytest-qt-FREE real-thread tests (mirrors test_folder_walk_worker.py)
    - D-09a single-owner fitz discipline — _assert_worker_thread() guard in run()
    - Cooperative shutdown only — no terminate() (D-05); LRU closed in run() finally
key_files:
  created: []
  modified:
    - desktop/pdf_page_renderer.py
    - tests/test_pdf_page_renderer.py
decisions:
  - "PdfRenderWorker added to desktop/pdf_page_renderer.py (D-08 same-module rule: no shared/ split, web has no My Library)"
  - "render_failed signal uses object slot for PdfRenderFailure (not the enum class) — PyMuPDF enums are not Qt metatypes; object accepts any Python object across queued connections"
  - "_handle_request extracted as its own method (not inlined in run()) so unit tests can call it synchronously on the main thread without spinning QThread or tripping the thread assertion"
  - "Thread assertion lives only in run(), not in _handle_request or enqueue/stop, enabling the synchronous test pattern"
  - "Unbounded queue intentional for Phase 99 — latest-wins token echo (D-03) protects what is displayed; Phase 100 controller concern per plan spec"
metrics:
  duration: "~15 minutes"
  completed: "2026-05-27"
  tasks_completed: 2
  tasks_total: 2
  files_created: 0
  files_modified: 2
---

# Phase 99 Plan 02: PdfRenderWorker — Off-Thread Queue-Driven Render Worker Summary

**One-liner:** Queue-driven `PdfRenderWorker(QThread)` wrapping the Plan 01 render core with D-07 tokenized signals, PDFIMG-06 no-crash envelope, cooperative shutdown, and 19 green pytest-qt-free tests.

## What Was Built

### Task 1: PdfRenderWorker class

Added `class PdfRenderWorker(QThread)` to `desktop/pdf_page_renderer.py` (305 lines added, ruff clean):

**Signals (D-07 tokenized):**
- `render_succeeded = pyqtSignal(int, str, int, QImage)` — token, sys_id, page_num, image
- `render_failed = pyqtSignal(int, str, int, object, str)` — token, sys_id, page_num, PdfRenderFailure, detail

The enum slot uses `object` (not `PdfRenderFailure`) because PyMuPDF enums are not Qt metatypes.

**`__init__(maxsize=4)`:** creates `DocLRU`, `queue.Queue`, `_stopping=False`. Module-level `_STOP = object()` sentinel.

**`enqueue(token, sys_id, page_num, filepath) -> bool`:** Drops and logs if `_stopping` (returns `False`) — REVIEW item 4 Codex MEDIUM. Otherwise puts the 4-tuple and returns `True`. Token is passed IN (counter lives in Phase 100 controller per D-03/Open Question 1).

**`_handle_request(item) -> None`:** Per-request no-crash envelope (PDFIMG-06). Calls `render_via_lru` in a try/except with both `PdfRenderError` (emits `render_failed` with classified reason) and bare `except Exception` (maps to `RENDER_ERROR`). NEVER raises. Thread assertion intentionally absent here (lives in `run()`) so tests can call synchronously.

**`run() -> None`:** Calls `_assert_worker_thread()` once at top (D-09a single-owner guard), then blocks on `self._queue.get()` in a loop, breaks on `_STOP` sentinel. `try/finally` ensures `self._lru.close_all()` runs on the render thread (D-06).

**`stop(timeout_ms=5000) -> None`:** Sets `_stopping`, puts `_STOP`, calls `wait(timeout_ms)`. Logs warning if not exited (wedged C call cannot be force-killed — D-05). No `terminate()`. No `close_all()` from caller thread (single-owner violation).

**`_assert_worker_thread() -> None`:** `assert QThread.currentThread() is self` — fires loudly if fitz is touched off the render thread.

### Task 2: Worker signal tests (19 total, all green)

Upgraded `tests/test_pdf_page_renderer.py` to 19 tests:

**`test_token_echoed_in_signals` (UPGRADED):** Synchronous `_handle_request((7, "S1", 2, MULTIPAGE_PDF))` via `DirectConnection` → verifies `render_succeeded` args are exactly `(7, "S1", 2, <non-null QImage>)`. Pins D-03 token-echo contract.

**`test_worker_failure_routes_to_render_failed` (NEW):** Synchronous `_handle_request` with missing filepath → `render_failed` fires with `MISSING_FILE`, token `42` echoed verbatim; `render_succeeded` not fired.

**`test_worker_survives_bad_render_and_serves_next` (NEW — REAL THREAD):** `worker.start()`; enqueue `corrupt_sample.pdf` then `multipage_sample.pdf`; `threading.Event` waits for 2 emissions; asserts first is `render_failed` (CORRUPT) and second is `render_succeeded` with non-null QImage. Proves the run loop survives a bad render (PDFIMG-06 no-crash + D-03 continuity).

**`test_enqueue_after_stop_dropped` (NEW):** `worker.stop()` (never started); `worker.enqueue(...)` returns `False`. Pins REVIEW item 4.

**`_require_pyqt()` helper (NEW):** Mirrors `test_folder_walk_worker.py:149-155` exactly.

All tests use `Qt.ConnectionType.DirectConnection` — pytest-qt-FREE, no `qtbot`, no `pytest-qt`.

## Verification

```
pytest tests/test_pdf_page_renderer.py -x -q     → 19 passed in 0.80s
pytest tests/test_pdf_page_renderer.py tests/test_local_indexer.py tests/test_local_pdf_extraction_fallback.py → 32 passed
python -m ruff check desktop/pdf_page_renderer.py tests/test_pdf_page_renderer.py → All checks passed
```

## Commits

| Hash | Type | Description |
|------|------|-------------|
| `8283bae2` | feat(99-02) | PdfRenderWorker — queue-driven render thread with tokenized signals |
| `a6ff27c9` | test(99-02) | Upgrade token-echo test + 3 new worker signal tests (19 total) |

## Deviations from Plan

None — plan executed exactly as written.

## Threat Model Coverage

| Threat | Mitigation | Status |
|--------|-----------|--------|
| T-99-04: concurrent fitz access | `_assert_worker_thread()` in `run()` | IMPLEMENTED |
| T-99-05: worker thread dying after bad render | `_handle_request` try/except no-crash + `test_worker_survives_bad_render_and_serves_next` | IMPLEMENTED |
| T-99-06: zombie thread / unclosed handles | cooperative `stop()` + `close_all()` in `run()` finally | IMPLEMENTED |

## Requirements Covered

- **PDFIMG-02 (off-thread half):** `PdfRenderWorker(QThread)` is long-lived and queue-driven, mirroring `ImageLoaderThread` conventions; UI never blocks (work is off-thread). Verified by worker signal tests.
- **D-03 (latest-wins):** Both signals echo `(token, sys_id, page_num)` verbatim so Phase 100 controller can discard stale results; verified by `test_token_echoed_in_signals`.
- **PDFIMG-06 (no-crash, off-thread half):** `_handle_request` envelope ensures `render_failed` fires and the thread survives; verified by `test_worker_failure_routes_to_render_failed` + `test_worker_survives_bad_render_and_serves_next`.
- **D-06 shutdown:** `stop()` + `close_all()` in `run()` finally close all docs cooperatively.

## Self-Check: PASSED

| Item | Result |
|------|--------|
| `desktop/pdf_page_renderer.py` contains `class PdfRenderWorker(QThread)` | FOUND |
| `render_succeeded = pyqtSignal(int, str, int, QImage)` | FOUND |
| `render_failed = pyqtSignal(int, str, int, object, str)` | FOUND |
| No `.terminate(` in module | CONFIRMED ABSENT |
| `close_all()` only in `run()`'s finally (not in `stop()`) | CONFIRMED |
| `tests/test_pdf_page_renderer.py` has 19 tests | 19 passed |
| Commit `8283bae2` exists | FOUND |
| Commit `a6ff27c9` exists | FOUND |
| `python -m ruff check` clean | All checks passed |
