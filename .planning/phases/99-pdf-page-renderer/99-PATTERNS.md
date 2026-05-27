# Phase 99: PDF Page Renderer - Pattern Map

**Mapped:** 2026-05-27
**Files analyzed:** 2 (1 new module + 1 new test file)
**Analogs found:** 2 / 2 (both files have strong analogs; one structural gap flagged below)

## File Classification

| New/Modified File | Role | Data Flow | Closest Analog | Match Quality |
|-------------------|------|-----------|----------------|---------------|
| `desktop/pdf_page_renderer.py` | service (Qt worker + doc LRU + failure enum) | request-response (off-UI render) + file-I/O (fitz) | `desktop/image_loader.py::ImageLoaderThread` (QThread/signal/cancel) + `shared/local_indexer.py::extract_pdf_pages` (fitz access) + `desktop/my_library_tab.py::FolderWalkWorker` (token-through-signals) | role-match (composite — no single file is all three) |
| `tests/test_pdf_page_renderer.py` | test | request-response (direct call, no QThread) | `tests/test_local_pdf_extraction_fallback.py` | exact (same fixture dir, same direct-call-not-threaded style, same `unittest.mock.patch` spy idiom) |

**Structural note for planner (do not skip):** Every existing desktop QThread worker
(`ImageLoaderThread`, `LocalIndexerWorker`, `PrescanWorker`, `FolderWalkWorker`,
`desktop/puzzle.py`, `desktop/vs_cache.py`) is a **one-shot** worker — `run()` does one job
and exits. D-09(a) requires a **long-lived** render thread that processes a *queue* of
requests across its lifetime. **No existing analog has a queue-driven `run()` loop.** The
planner must compose: borrow the signal/cancel/token/lifecycle *conventions* from the analogs
below, but the long-lived `run()` loop (e.g. `queue.Queue.get()` blocking loop with a sentinel
to stop) is net-new and should be designed deliberately. RESEARCH §"Open Questions 2" and
Pitfall 3 both reinforce single-owner discipline for this loop.

## Pattern Assignments

### `desktop/pdf_page_renderer.py` (service: Qt worker + doc LRU + failure enum)

This module is an assembly of FOUR established patterns from THREE analog files. Each is excerpted below with the exact convention to replicate.

---

**Analog 1 — `desktop/image_loader.py::ImageLoaderThread` (QThread + pyqtSignal + cooperative cancel)**

Imports pattern (lines 1-12) — replicate this header shape:
```python
import os
from PyQt6.QtCore import QThread, pyqtSignal
from PyQt6.QtGui import QImage
from genizah_core import get_logger   # logger source used across desktop

logger = get_logger(__name__)
```

Class + signal declaration + cooperative cancel (lines 15-43) — the shape to mirror:
```python
class ImageLoaderThread(QThread):
    image_loaded = pyqtSignal(QImage)
    load_failed = pyqtSignal()

    def __init__(self, url):
        super().__init__()
        self.url = url
        self._cancelled = False

    def cancel(self):
        self._cancelled = True
```
**Replicate as (D-07 signal contract):**
```python
render_succeeded = pyqtSignal(int, str, int, QImage)        # token, sys_id, page_num, image
render_failed    = pyqtSignal(int, str, int, object, str)   # token, sys_id, page_num, PdfRenderFailure, detail
```
Note the existing `image_loaded = pyqtSignal(QImage)` proves `QImage` marshals cleanly across a
queued cross-thread connection — D-07's `render_succeeded(...QImage)` is valid. For the enum slot
use `object` (or `int`) in the `pyqtSignal` declaration since a `PdfRenderFailure` enum is not a
registered Qt metatype.

Cooperative-cancel checked between attempts (lines 87, 95, 129) — `if self._cancelled: return None`.
The render worker generalizes this: the cancel flag stops the queue loop; per-request supersede is
the token (D-03), NOT a cancel flag.

---

**Analog 2 — `shared/local_indexer.py` (fitz access + 1-based page_num + stderr suppression)**

Import-time stderr suppression (lines 66, 81-94) — the renderer inherits this **for free** if it
imports `shared.local_indexer` (or imports after it loads); RESEARCH A3 says belt-and-suspenders
is to repeat the 3-line pattern in the renderer:
```python
import fitz  # PyMuPDF - D-01
try:
    fitz.TOOLS.mupdf_display_warnings(False)
except Exception as _e:   # noqa: BLE001 — non-critical; must not crash import
    logging.getLogger(__name__).debug("mupdf_display_warnings unavailable: %s", _e)
try:
    fitz.TOOLS.mupdf_display_errors(False)
except Exception as _e:   # noqa: BLE001
    logging.getLogger(__name__).debug("mupdf_display_errors unavailable: %s", _e)
```

The 1-based `page_num` ↔ fitz index convention (lines 696-721) — confirms **render index = page_num - 1**:
```python
doc = fitz.open(filepath)
try:
    for page_num, page in enumerate(doc, start=1):   # 1-based page_num
        ...
finally:
    doc.close()
```
The renderer does NOT iterate; it loads one page: `page = doc.load_page(page_num - 1)` after
validating `0 <= page_num - 1 < doc.page_count` (D-04a). Note `extract_pdf_pages` uses the
open/try/finally/close discipline — the renderer's LRU replaces the per-call `doc.close()` with
eviction-time / shutdown-time `.close()` (D-06), but the close discipline itself comes from here.

`get_filepath(sys_id)` (lines 1343-1352) — the path resolver the UI passes results from; the
renderer receives an already-resolved `filepath` (or `sys_id` + a resolver), per RESEARCH the
controller in Phase 100 calls this:
```python
def get_filepath(self, sys_id: str) -> Optional[str]:
    row = self._conn.execute(
        "SELECT filepath FROM local_files WHERE sys_id = ?", (sys_id,)
    ).fetchone()
    return row["filepath"] if row else None
```

---

**Analog 3 — `desktop/my_library_tab.py` (token-through-all-signals + worker lifecycle conventions)**

`FolderWalkWorker` token pattern (lines 760-781) — the latest-wins token discipline to mirror in
D-03/D-07 (every signal carries the token; UI drops payloads whose `token != current`):
```python
batch_emitted   = pyqtSignal(list, int)        # payload; token
finished_signal = pyqtSignal(int, int, int)    # ...; token
error_signal    = pyqtSignal(str, int)         # message; token

def __init__(self, folder_paths: list, token: int = 0) -> None:
    super().__init__()
    self._token = int(token)

@property
def token(self) -> int:
    return self._token
```
For Phase 99 the token is **per-request** (carried in each enqueued render job and echoed in
`render_succeeded`/`render_failed`), NOT captured once at construction — because the render thread
is long-lived and serves many requests. RESEARCH Open Question 1: the counter is owned by the
Phase 100 controller; Phase 99 accepts `token: int` per request and echoes it. Tests pass explicit tokens.

`LocalIndexerWorker.run()` exception envelope (lines 667-691) — replicate the broad try/except that
turns any worker-thread exception into a failure signal rather than a crash (this is the PDFIMG-06
"no hang/crash" contract):
```python
def run(self) -> None:
    try:
        ...
    except Exception as exc:  # noqa: BLE001
        logger.exception("LocalIndexerWorker: unhandled error")
        self.error_signal.emit(str(exc))
```
For the renderer, each per-request body is wrapped so one bad render emits `render_failed(...,
RENDER_ERROR, detail)` and the loop continues serving the next request — the thread never dies.

---

**Analog 4 — `desktop/viewers.py` (downstream consumer + existing token-discard wiring + thread shutdown)**

The QImage destination signature the render output MUST satisfy (lines 1209-1213) — `display_image`
takes a plain `QImage` and wraps it to a `QPixmap`:
```python
def display_image(self, image):
    if self._closing:
        return
    pix = QPixmap.fromImage(image)        # <-- the QImage must survive to here (D-01b .copy())
    self.scroll_area.set_image(pix)       # ZoomableScrollArea inner entry
    ...
```
This is why D-01b is load-bearing: the `.copy()`'d QImage travels through a queued signal, sits in
the event queue, and is only consumed at `QPixmap.fromImage(image)` — long after the source pixmap
is freed. A buffer-backed (non-copied) QImage would be use-after-free here.

Existing latest-wins discard wiring (lines 1198-1203) — the exact lambda idiom Phase 100 will reuse,
shown here so the planner designs the Phase-99 signal to fit it:
```python
self.loader_thread.image_loaded.connect(
    lambda img, g=gen: self.display_image(img)
        if g == self._load_generation and not self._closing else None
)
```

Thread shutdown discipline (lines 1078-1102) — `cancel()` then `wait(timeout)` then `terminate()`
last-resort; on the long-lived render thread the analog is: set stop flag → push sentinel to the
queue → `wait(timeout)`; close all LRU docs (D-06 `close_all()`) before/after the thread exits:
```python
def _wait_or_terminate(self, thread, timeout_ms=2000):
    thread.cancel()
    if not thread.wait(timeout_ms):
        logger.warning("Image thread did not finish in %dms, terminating", timeout_ms)
        thread.terminate()
        thread.wait()
```

---

### `tests/test_pdf_page_renderer.py` (test)

**Analog:** `tests/test_local_pdf_extraction_fallback.py`

Fixture-dir + guard pattern (lines 24-30, 51-55) — reuse the SAME fixtures directory:
```python
import os
import pytest

FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures", "local_indexer")
SINGLE_WORD_PDF = os.path.join(FIXTURES_DIR, "single_word_per_line.pdf")
CLEAN_PDF       = os.path.join(FIXTURES_DIR, "clean_sample.pdf")
HEBREW_PDF      = os.path.join(FIXTURES_DIR, "hebrew_sample.pdf")
# guard:
if not os.path.exists(SINGLE_WORD_PDF):
    pytest.fail("fixture missing: regenerate via scripts/generate_single_word_fixture.py")
```

Direct-call (no QThread spin-up) test style (lines 50-68) — `extract_pdf_pages()` is called
directly, never wrapped in a QThread. **Mirror this:** unit-test the render *function* and the
*DocLRU* class directly; do NOT spin a QThread in unit tests (RESEARCH Wave-0 note). The QThread
wrapper, if tested, uses a signal-spy.

`unittest.mock.patch` spy idiom (lines 132-171) — the established way to assert which fitz calls
fired; reuse for asserting `.copy()` was called / that no disk write happened:
```python
import fitz
from unittest.mock import patch

invoked_calls = []
original = fitz.Page.get_text
def _spy(self, *args, **kwargs):
    invoked_calls.append((args, kwargs))
    return original(self, *args, **kwargs)

with patch.object(fitz.Page, "get_text", _spy):
    ...
assert <condition on invoked_calls>
```

New fixtures needed (RESEARCH Wave-0 gaps) — generate inline with `fitz` or extend
`scripts/generate_single_word_fixture.py`:
- encrypted PDF: `doc.save(path, encryption=fitz.PDF_ENCRYPT_AES_256, owner_pw=..., user_pw=...)`
- corrupt PDF: write/truncate arbitrary bytes
- multi-page PDF: for page-out-of-range + page_num-1 index tests

Test map (RESEARCH §Validation, all Wave 0): `test_render_single_page`,
`test_qimage_independent_of_pixmap`, `test_doc_lru_evict_and_close`, `test_no_disk_cache`,
`test_missing_file_reason`, `test_not_pdf_reason`, `test_encrypted_reason`, `test_corrupt_reason`,
`test_page_out_of_range`, `test_failures_logged`, `test_token_echoed_in_signals`.

## Shared Patterns

### fitz stderr suppression
**Source:** `shared/local_indexer.py:66,81-94`
**Apply to:** `desktop/pdf_page_renderer.py` module top (inherited for free via local_indexer import; repeat the 3-line try/except as belt-and-suspenders per RESEARCH A3).

### Worker exception → signal envelope (PDFIMG-06 no-crash contract)
**Source:** `desktop/my_library_tab.py:667-691` (`LocalIndexerWorker.run`)
**Apply to:** the render worker's per-request body — `except Exception` → `logger.exception(...)` +
`render_failed.emit(token, sys_id, page_num, PdfRenderFailure.RENDER_ERROR, str(exc))`. Every
failure path logs reason + detail (D-04).

### Token-carrying signals for latest-wins discard
**Source:** `desktop/my_library_tab.py:760-781` (`FolderWalkWorker`) + `desktop/viewers.py:1198-1203` (discard lambda)
**Apply to:** both `render_succeeded` and `render_failed` carry `token` as the first arg (D-07); the
Phase 100 controller owns the counter and discards stale results.

### QThread shutdown / explicit resource close
**Source:** `desktop/viewers.py:1078-1102` (`_wait_or_terminate` / `stop_threads`) + `shared/local_indexer.py:720-721` (`doc.close()` in `finally`)
**Apply to:** renderer shutdown — stop flag + queue sentinel + `wait(timeout)`; `DocLRU.close_all()`
closes every open `fitz.Document` (D-06: all docs closed on app shutdown).

### QImage memory-safety copy (D-01b)
**Source:** RESEARCH Pattern 1 + the consumer `desktop/viewers.py:1212` (`QPixmap.fromImage` happens
later on the UI thread, after the source pixmap is gone)
**Apply to:** the single render function — `return QImage(pix.samples, pix.width, pix.height,
pix.stride, QImage.Format.Format_RGB888).copy()`. Always pass `pix.stride`, never `width*3`.

## No Analog Found (compose from RESEARCH instead)

| Sub-component | Role | Data Flow | Reason |
|---------------|------|-----------|--------|
| Long-lived queue-driven `run()` loop | service | request-response | All existing desktop QThreads are one-shot; no queue-loop worker exists. Use RESEARCH §System Architecture Diagram + a `queue.Queue` blocking-get loop with a stop sentinel. Single-owner discipline (RESEARCH Pitfall 3) — consider `assert QThread.currentThread() is self` at LRU-touch points. |
| `PdfRenderFailure` enum | model | — | No failure-reason enum exists in the codebase. Build from RESEARCH Pattern 3 (D-04 values: missing-file, not-pdf, encrypted, corrupt, page-out-of-range, render-error, timeout, cancelled). Representation is Claude's discretion (CONTEXT D-04 / "Claude's Discretion"). |
| Bounded `DocLRU` with explicit `.close()` on evict | service | file-I/O | No `OrderedDict`-LRU-with-cleanup exists; `functools.lru_cache` can't run `.close()` (RESEARCH Don't-Hand-Roll). Build from RESEARCH Pattern 2 (OrderedDict, maxsize=4, `popitem(last=False)` → `.close()`, `close_all()`). |
| ~8s `QTimer` watchdog | — | — | UI-side (Phase 100 controller), NOT in Phase 99 worker (D-05/D-07). Phase 99 does not emit `timeout`; it only defines the enum value. Out of scope for this module's code; note it so the planner does not place it in the worker. |

## Metadata

**Analog search scope:** `desktop/` (image_loader.py, viewers.py, my_library_tab.py, puzzle.py, vs_cache.py), `shared/local_indexer.py`, `tests/`
**Files scanned:** 6 analog source files + 1 analog test file
**Pattern extraction date:** 2026-05-27
