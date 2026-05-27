# Phase 99: PDF Page Renderer - Research

**Researched:** 2026-05-27
**Domain:** PyQt6 desktop threading + PyMuPDF (fitz) single-page rendering + QImage memory safety
**Confidence:** HIGH

## Summary

Phase 99 builds `desktop/pdf_page_renderer.py`: a desktop-only worker that renders ONE PDF page to a `QImage` off the UI thread, backed by a bounded LRU of open `fitz.Document` handles, with graceful, logged failure and no on-disk image cache. Nearly every implementation decision is already LOCKED in `99-CONTEXT.md` (D-01..D-08). The single genuinely open decision is **D-09 the threading model**, which the user escalated to the planner with a demand for concrete codebase investigation.

I investigated D-09 directly against the live codebase. The result is unambiguous: **option (a) — single dedicated long-lived QThread that exclusively owns all `fitz` access — is the correct choice**, and option (b) (a separate render process) carries real, unmitigated frozen-EXE risk that the codebase has never paid for. The evidence: (1) the frozen `GenizahSearch.exe` builds from `genizah_app.py` only (`GenizahSearchPro.spec:30`) and contains ZERO `multiprocessing` use and ZERO `freeze_support()` calls anywhere in app/UI/shared code; (2) the ONLY `multiprocessing` in the repo is `corpus_mapper/runner.py`, a standalone offline dev batch tool that is never frozen and not in the spec's `datas`/`hiddenimports`; (3) the app already runs `fitz` off the UI thread today via `LocalIndexerWorker(QThread)` → `LocalIndexer.scan_all()` → `extract_pdf_pages()` → `fitz.open()`, hardened through Phase 95/96/97.x UAT with no crashes; (4) PyMuPDF's "no multithreading" caveat is about *concurrent* access from multiple threads — the existing indexer already relies safely on single-owner discipline, which is exactly the mitigation D-09(a) prescribes.

**Primary recommendation:** Implement D-09 as option (a): one long-lived `QThread` that owns the `fitz.Document` LRU and performs every `open`/`render`/`close` call; no other thread ever touches `fitz` concurrently. Mirror `ImageLoaderThread`'s signal/cancel shape and the `FolderWalkWorker` token-based stale-result guard already in the codebase. Use the existing `display_image(QImage)` → `ZoomableScrollArea.set_image()` path verbatim (D-02). Verified PyMuPDF version: **1.27.2.3**.

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| PDF page → pixmap render | Render worker thread (off-UI) | — | `fitz` C calls block; must not run on the Qt event-loop thread |
| `fitz.Document` LRU (open/evict/close) | Render worker thread | — | Single-owner discipline is the thread-safety mitigation (D-09a) |
| Pixmap→QImage `.copy()` | Render worker thread | — | Must copy off the pixmap sample buffer before pixmap is freed (D-01b) |
| Generation-token supersede / stale drop | UI controller (Phase 100) + worker | — | Latest-wins; mirrors existing `viewers.py:1199` `_load_generation` pattern |
| ~8s timeout watchdog | UI controller (`QTimer`) | — | Soft UX contract; cannot kill the C call in thread model (D-05) |
| Display (zoom/pan/rotate/adjust) | `ManuscriptViewerWidget` (existing) | `ZoomableScrollArea` | Reuse for free; no new pane (D-02) — Phase 100 wires it |
| Path resolution (sys_id → filepath) | `LocalIndexer.get_filepath()` (shared) | — | Already exists at `shared/local_indexer.py:1343` |

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---------|---------|---------|--------------|
| PyMuPDF (`fitz`) | 1.27.2.3 `[VERIFIED: python -c import]` | PDF page → pixmap render | Already the desktop PDF dep (indexer); `collect_all('pymupdf')` in spec |
| PyQt6 | (project standard) | `QThread`, `pyqtSignal`, `QImage`, `QTimer` | Desktop UI framework (CLAUDE.md) |

No new dependencies. All required libraries already ship in the frozen EXE.

### Supporting (all already present — reuse, do not add)
| Asset | Location | Purpose |
|-------|----------|---------|
| `ImageLoaderThread(QThread)` | `desktop/image_loader.py:15` | Signal/cancel pattern to mirror |
| `ManuscriptViewerWidget.display_image(QImage)` | `desktop/viewers.py:1209` | Display entry point (D-02) |
| `ZoomableScrollArea.set_image(pixmap)` | `desktop/viewers.py:71` | Inner zoom/pan/rotate/adjust widget |
| `LocalIndexer.get_filepath(sys_id)` | `shared/local_indexer.py:1343` | sys_id → filepath resolution |
| `extract_pdf_pages()` | `shared/local_indexer.py:672` | Confirms 1-based `page_num` ↔ `fitz` index = `page_num-1` |
| `fitz.TOOLS.mupdf_display_warnings/errors(False)` | `shared/local_indexer.py:82,89` | stderr suppression — inherited for free if renderer imports after indexer module loads |

### Alternatives Considered (D-09)
| Instead of | Could Use | Tradeoff |
|------------|-----------|----------|
| (a) Single dedicated QThread | (b) Separate render process + IPC | Documented-safe + killable wedged render, BUT: adds `freeze_support()` to a frozen EXE that has never used multiprocessing (PyInstaller relaunch/`sys.argv` hazard on Windows), ~15 MB/page bitmap IPC, and a new shutdown/zombie-process surface. **Rejected** — risk is non-trivial and unproven here; D-09's own fallback clause says default to (a). |

**Installation:** None. `import fitz` already works; spec already collects PyMuPDF C-extensions (`GenizahSearchPro.spec:17`).

**Version verification:** `python -c "import pymupdf; print(pymupdf.__version__)"` → `1.27.2.3` `[VERIFIED]`. MuPDF library 1.27.2.

## Architecture Patterns

### System Architecture Diagram

```
[Phase 100 UI controller]                         [Render worker — D-09(a) single QThread]
   sys_id + page_num                                 owns fitz.Document LRU (size 4)
        │                                                     │
        │  enqueue(token, sys_id, page_num) ─────────────────►│
        │  (token = monotonic generation counter, D-03)       │
        │                                            ┌─────────▼──────────┐
   start ~8s QTimer (D-05) ◄───────────┐             │ resolve filepath   │ get_filepath(sys_id)
        │                              │             │ (passed in by UI)  │
        │                              │             ├────────────────────┤
        │                              │             │ LRU.get(filepath)  │── miss ─► fitz.open()
        │                              │             │  → fitz.Document    │           (validate: exists?
        │                              │             ├────────────────────┤            pdf? encrypted?)
        │                              │             │ validate page bounds│ D-04a
        │                              │             │  0≤idx<page_count   │
        │                              │             ├────────────────────┤
        │                              │             │ page.get_pixmap(    │ D-01
        │                              │             │   dpi=200,          │
        │                              │             │   colorspace=csRGB, │
        │                              │             │   alpha=False)      │
        │                              │             ├────────────────────┤
        │                              │             │ QImage(samples,...).│ D-01b
        │                              │             │   .copy()  ◄── before pixmap freed
        │                              │             └─────────┬──────────┘
        │  render_succeeded(token, sys_id, page_num, QImage) ◄─┤  (D-07)
        │  render_failed(token, sys_id, page_num, reason,detail)│  (D-04 enum)
        ▼                                                       │
  if token == current_gen and not timed_out:                   │
     display_image(QImage)  ──► ZoomableScrollArea.set_image()  │
  else: discard (stale / superseded)  ◄── latest-wins (D-03)    │
  on QTimer expiry: show 'timeout' placeholder, discard late result
```

Data flow: a navigation event produces `(token, sys_id, page_num)`; the worker resolves the doc (LRU hit or `fitz.open`), validates, renders one page to a copied `QImage`, and emits a tokenized result; the UI drops any result whose token is not the current generation or that arrived after the watchdog fired.

### Recommended Module Structure (D-08)
```
desktop/
└── pdf_page_renderer.py     # NEW: PdfRenderFailure enum + render worker + doc LRU
                             # (Qt-facing; NO shared/ Qt-free split — D-08)
```
No `shared/` split: web has no "My Library" and there is no second consumer (Codex-confirmed, D-08).

### Pattern 1: Render one page to a memory-safe QImage
**What:** Render a single page at fixed DPI and copy the bitmap off the pixmap buffer before the pixmap is garbage-collected.
**When to use:** Every render call.
**Example:**
```python
# Source: PyMuPDF page.get_pixmap docs + D-01/D-01b; pattern matches local_indexer fitz usage
# [CITED: pymupdf.readthedocs.io/en/latest/page.html] for get_pixmap(dpi=, colorspace=, alpha=)
import fitz
from PyQt6.QtGui import QImage

RENDER_DPI = 200  # D-01 module constant — NOT adaptive

def render_page(doc: fitz.Document, page_num: int) -> QImage:
    # D-04a: validate bounds BEFORE render (page_num is 1-based; fitz idx = page_num-1)
    idx = page_num - 1
    if not (0 <= idx < doc.page_count):
        raise IndexError("page-out-of-range")
    page = doc.load_page(idx)
    pix = page.get_pixmap(dpi=RENDER_DPI, colorspace=fitz.csRGB, alpha=False)  # D-01
    # alpha=False → 3 bytes/px (Format_RGB888); stride = pix.stride
    img = QImage(pix.samples, pix.width, pix.height, pix.stride,
                 QImage.Format.Format_RGB888)
    return img.copy()  # D-01b: copy BEFORE pix is freed — else use-after-free
```
**Critical:** `pix.samples` is a Python `bytes` view backed by the C pixmap. The `QImage(samples,...)` constructor does NOT copy; `.copy()` is mandatory. Pass `pix.stride` (NOT a computed `width*3`) to the constructor — MuPDF may pad rows.

### Pattern 2: Bounded fitz.Document LRU (D-06)
**What:** Cache up to 4 open `fitz.Document` handles keyed by canonical filepath; explicitly `.close()` on eviction and on shutdown.
**Example:**
```python
# Source: OrderedDict LRU; D-06 (handles only — NO page/pixmap caching)
from collections import OrderedDict
import fitz

class DocLRU:
    def __init__(self, maxsize: int = 4):   # D-06: 4 (configurable 2–8)
        self._cache: "OrderedDict[str, fitz.Document]" = OrderedDict()
        self._maxsize = maxsize

    def get(self, filepath: str) -> fitz.Document:
        doc = self._cache.get(filepath)
        if doc is not None:
            self._cache.move_to_end(filepath)
            return doc
        doc = fitz.open(filepath)        # may raise — caller maps to reason enum
        if doc.needs_pass:               # encrypted → caller raises 'password-needed'
            doc.close()
            raise ValueError("password-needed")
        self._cache[filepath] = doc
        if len(self._cache) > self._maxsize:
            _, evicted = self._cache.popitem(last=False)
            evicted.close()              # D-06: explicit close on evict
        return doc

    def close_all(self):                 # D-06: all docs closed on app shutdown
        for doc in self._cache.values():
            try: doc.close()
            except Exception: pass
        self._cache.clear()
```
**Note:** The LRU lives inside and is touched ONLY by the single render thread (D-09a). No lock needed because there is exactly one owner.

### Pattern 3: Failure-reason enum + classification (D-04)
**What:** Classify every failure into the locked enum so Phase 100 can map each to placeholder text.
```python
# Source: D-04 expanded enum (Codex). Claude's discretion: enum.Enum representation.
import enum

class PdfRenderFailure(enum.Enum):
    MISSING_FILE = "missing-file"
    NOT_PDF = "not-pdf"
    ENCRYPTED = "encrypted"            # a.k.a. password-needed
    CORRUPT = "corrupt"               # open-error
    PAGE_OUT_OF_RANGE = "page-out-of-range"
    RENDER_ERROR = "render-error"
    TIMEOUT = "timeout"               # surfaced by UI watchdog, NOT the worker (D-07)
    CANCELLED = "cancelled"           # stale / superseded
```
Classification order in the worker: missing file (`os.path.exists`) → wrong extension (`.pdf`) → `fitz.open` raises → `doc.needs_pass` → page bounds (D-04a) → `get_pixmap` raises. Every failure is logged with `reason` + a `detail` string (D-04).

### Pattern 4: Latest-wins generation token (D-03) — mirror existing code
The codebase already does this at `desktop/viewers.py:1199`:
```python
# Source: desktop/viewers.py:1198-1203 (existing ImageLoaderThread wiring)
self.loader_thread.image_loaded.connect(
    lambda img, g=gen: self.display_image(img)
        if g == self._load_generation and not self._closing else None
)
```
And `FolderWalkWorker` carries a token through all three signals (`desktop/my_library_tab.py:761-763`) with the UI dropping payloads whose `token != current`. Phase 99's `render_succeeded`/`render_failed` carry `token` (D-07); the controller compares against the current generation and discards stale results. **No debounce** (D-03).

### Anti-Patterns to Avoid
- **Constructing the QImage without `.copy()`** — use-after-free crash once the pixmap is freed (D-01b, the single most likely landmine).
- **Touching `fitz` from more than one thread** — violates PyMuPDF's safety line; the whole point of D-09(a) is single-owner.
- **Caching `Page` or `Pixmap` objects in the LRU** — D-06 says handles only (memory blowup otherwise; `Page` objects hold doc references).
- **Computing stride as `width*3`** — MuPDF may pad rows; always pass `pix.stride`.
- **Trying to force-kill the render in the thread model** — impossible; the soft `QTimer` watchdog (D-05) shows the placeholder and the latest-wins logic drops the late result.
- **Adding `multiprocessing`/`freeze_support()`** — see D-09 evidence; introduces frozen-EXE risk the app has never carried.

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Off-thread work + cancel | Custom thread pool | `QThread` + cooperative cancel flag (mirror `ImageLoaderThread`) | Battle-tested through Phase 97.x; Qt signal marshalling is correct cross-thread |
| Zoom / pan / rotate / brightness pane | New image viewer | `ManuscriptViewerWidget.display_image()` (D-02) | Already gives Ctrl+wheel 0.1–5.0× zoom, pan, rotate, brightness/contrast/gamma/invert for free |
| Stale-result suppression | New mutex/state machine | Generation token (mirror `viewers.py:1199` + `FolderWalkWorker` token) | Established pattern; latest-wins is already proven in the codebase |
| sys_id → filepath | New SQLite query | `LocalIndexer.get_filepath()` (`local_indexer.py:1343`) | Public method exists; canonical path handling already correct |
| PDF stderr noise suppression | New stderr redirect | Inherited `fitz.TOOLS.mupdf_display_errors(False)` from `local_indexer` import | Set at module import (Phase 97.3 R97.3-C); free if renderer imports after |
| LRU | `functools.lru_cache` | `OrderedDict` | `lru_cache` can't run `.close()` on eviction; you need explicit cleanup (D-06) |

**Key insight:** Phase 99 is almost entirely *assembly of existing, hardened patterns*. The only net-new code is the failure enum, the doc LRU with explicit close, and the per-page render+copy. Everything threading- and display-related already exists and works in the shipped product.

## Runtime State Inventory

> Phase 99 is greenfield code (a new module) with no rename/refactor/migration. The following confirms there is no hidden runtime state to migrate.

| Category | Items Found | Action Required |
|----------|-------------|------------------|
| Stored data | None — D-06 forbids any on-disk image cache; renderer holds only in-RAM `fitz.Document` handles | None |
| Live service config | None — desktop-only, no external service | None |
| OS-registered state | None | None |
| Secrets/env vars | None | None |
| Build artifacts | PyMuPDF C-extensions already collected (`GenizahSearchPro.spec:17`, `collect_all('pymupdf')`); no spec change needed since `import fitz` is already a desktop dep | None — verified spec already ships fitz |

## Common Pitfalls

### Pitfall 1: QImage use-after-free (the #1 risk)
**What goes wrong:** `QImage(pix.samples, ...)` shares the C pixmap's memory; once `pix` is GC'd, the QImage points at freed memory → garbage pixels or segfault (silent in frozen EXE).
**Why it happens:** Qt's `QImage` raw-buffer constructor is zero-copy by design.
**How to avoid:** Always `.copy()` before the pixmap goes out of scope (D-01b). Return only the copied image; never the buffer-backed one.
**Warning signs:** Intermittent corrupted pages, crashes under fast navigation, or pages that look fine in tests (where the pixmap survives in scope) but corrupt in production.

### Pitfall 2: Wrong stride → sheared image
**What goes wrong:** Passing `width*3` instead of `pix.stride` to `QImage` shears/skews the image when MuPDF pads rows.
**How to avoid:** Pass `pix.stride` from the pixmap.
**Warning signs:** Diagonal-skew rendering on some PDFs but not others.

### Pitfall 3: Concurrent fitz access creeping in
**What goes wrong:** A future contributor calls the renderer's LRU or `fitz.open` from the UI thread (e.g. a "quick preview") → violates single-owner, may crash Python (PyMuPDF caveat).
**How to avoid:** Keep ALL `fitz` calls inside the worker's `run()`/worker-owned methods. Consider an assertion that the LRU is only touched on the worker thread (`assert QThread.currentThread() is self`).
**Warning signs:** Rare, unreproducible crashes that correlate with simultaneous indexing + rendering.

### Pitfall 4: Watchdog can't stop the C render (by design)
**What goes wrong:** A pathological PDF page takes >8s; the `QTimer` fires and shows a timeout placeholder, but the worker thread is still busy in the MuPDF C call and can't accept the next request until it returns.
**Why it happens:** A single render thread is busy; the C call is not interruptible (D-05).
**How to avoid:** Accept it — this is the documented trade-off of choosing (a) over (b). The UI never freezes (watchdog + latest-wins). If a wedged render blocks subsequent navigation noticeably, the mitigation is a follow-up (e.g. drain/replace the thread), NOT switching to multiprocessing now.
**Warning signs:** Navigation feels "stuck" for a few seconds after a timeout placeholder on a specific bad PDF.

### Pitfall 5: `doc.needs_pass` vs open-time exception for encryption
**What goes wrong:** Some encrypted PDFs open fine but `needs_pass` is True; others raise on `fitz.open`. Classifying both as the same reason needs care.
**How to avoid:** Check `doc.needs_pass` after a successful open → `ENCRYPTED`; catch `fitz.open` exceptions → `CORRUPT`/open-error. Validate `.pdf` extension first for `NOT_PDF`.

## Code Examples

### Detecting failure reasons at open time
```python
# Source: PyMuPDF Document API; D-04 enum mapping
import os, fitz

def open_doc_classified(filepath: str) -> fitz.Document:
    if not os.path.exists(filepath):
        raise FileNotFoundError("missing-file")
    if not filepath.lower().endswith(".pdf"):
        raise ValueError("not-pdf")
    try:
        doc = fitz.open(filepath)
    except Exception as e:
        raise RuntimeError(f"corrupt: {e}")
    if doc.needs_pass:
        doc.close()
        raise PermissionError("encrypted")
    return doc
```

### Test fixtures already available
The codebase ships ready-to-use PDF fixtures and a generator:
- `tests/fixtures/local_indexer/hebrew_sample.pdf` (real RTL Hebrew PDF)
- `tests/fixtures/local_indexer/clean_sample.pdf` (normal paragraph text)
- `tests/fixtures/local_indexer/single_word_per_line.pdf` (pathological)
- `scripts/generate_single_word_fixture.py` (regenerator — can be extended to make a multi-page / encrypted / corrupt fixture for PDFIMG-06 tests) `[VERIFIED: tests/test_local_pdf_extraction_fallback.py:6-22]`

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| `fitz.Matrix(zoom, zoom)` for resolution | `get_pixmap(dpi=200, ...)` | PyMuPDF ≥1.19 | D-01 uses `dpi=` directly — cleaner, exact |
| `alpha=True` default pixmaps (4 bytes/px) | `alpha=False` (3 bytes/px, Format_RGB888) | always available | D-01 saves ~25% per-page RAM (Codex) |
| Multiple threads touching fitz | Single-owner thread OR multiprocessing | longstanding caveat | D-09(a) single-owner is the in-codebase precedent |

**Deprecated/outdated:** Nothing in scope. PyMuPDF 1.27.x is current; `import fitz` and `import pymupdf` both resolve.

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|-------|---------|---------------|
| A1 | `pix.samples` + `Format_RGB888` + `pix.stride` is the correct zero-copy QImage construction for an `alpha=False` csRGB pixmap | Pattern 1 | LOW — well-documented PyMuPDF↔Qt bridge; verified API shape against PyMuPDF 1.27 docs. Plan should include a render smoke test asserting non-null QImage of expected dimensions. |
| A2 | The frozen EXE truly never enters `corpus_mapper` code (so its `multiprocessing` is irrelevant to D-09) | D-09 / Summary | LOW — `GenizahSearchPro.spec:30` entry is `genizah_app.py`; `corpus_mapper` is absent from `datas`/`hiddenimports`. Grep found `multiprocessing` only in `corpus_mapper/` + agent worktrees, never in app/desktop/shared/web. |
| A3 | Inheriting `mupdf_display_errors(False)` requires the renderer to import after `shared.local_indexer` is loaded | Don't Hand-Roll | LOW — if unconfirmed, the renderer can set it itself in a try/except (same 3-line pattern as `local_indexer.py:82`). Cheap belt-and-suspenders. |

## Open Questions

1. **Where does the generation-token counter live (worker vs UI controller)?**
   - What we know: D-03 says monotonic token; CONTEXT marks token location as Claude's discretion.
   - What's unclear: Phase 99 (worker-only) vs Phase 100 (UI controller). Phase 99 must define the signal *shape* carrying the token (D-07); the *issuing* of tokens is naturally the UI controller's job (Phase 100).
   - Recommendation: Phase 99 accepts a `token: int` per request and echoes it in both result signals; the counter itself is owned by the Phase 100 controller. Phase 99 tests can pass explicit tokens.

2. **Does the LRU need a thread-affinity assertion?**
   - What we know: Single-owner is the safety guarantee.
   - Recommendation: Add `assert QThread.currentThread() is self` (or a debug-only check) at LRU-touch points to fail loudly if a future contributor calls fitz off-thread. Cheap insurance against Pitfall 3. (Planner's discretion.)

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| PyMuPDF (`fitz`) | PDFIMG-01 render | ✓ | 1.27.2.3 | — |
| PyQt6 | worker/QImage/QTimer | ✓ | project std | — |
| Test PDF fixtures | PDFIMG-06 failure tests | ✓ (3 fixtures + generator) | — | extend `generate_single_word_fixture.py` for encrypted/corrupt/multi-page |

**Missing dependencies with no fallback:** None.
**Missing dependencies with fallback:** None — encrypted/corrupt test PDFs may need generating, but the generator script and `fitz` make that trivial.

## Validation Architecture

> nyquist_validation = true (`.planning/config.json`). PDFIMG-06 is failure-mode-heavy, so this matters.

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (project standard, ~2500+ tests) |
| Config file | `pytest` invoked as `pytest tests/` (no special ini noted) |
| Quick run command | `pytest tests/test_pdf_page_renderer.py -x` |
| Full suite command | `pytest tests/` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| PDFIMG-01 | render page N → non-null QImage of expected dims; fitz idx = page_num-1 | unit | `pytest tests/test_pdf_page_renderer.py::test_render_single_page -x` | ❌ Wave 0 |
| PDFIMG-01 | `.copy()` produces an independent QImage (no use-after-free) | unit | `pytest tests/test_pdf_page_renderer.py::test_qimage_independent_of_pixmap -x` | ❌ Wave 0 |
| PDFIMG-02 | LRU evicts + closes oldest doc past maxsize; closes all on shutdown | unit | `pytest tests/test_pdf_page_renderer.py::test_doc_lru_evict_and_close -x` | ❌ Wave 0 |
| PDFIMG-02 | no on-disk image cache written during render | unit | `pytest tests/test_pdf_page_renderer.py::test_no_disk_cache -x` | ❌ Wave 0 |
| PDFIMG-06 | missing file → MISSING_FILE reason + log, no raise to caller | unit | `pytest tests/test_pdf_page_renderer.py::test_missing_file_reason -x` | ❌ Wave 0 |
| PDFIMG-06 | non-pdf extension → NOT_PDF | unit | `pytest tests/test_pdf_page_renderer.py::test_not_pdf_reason -x` | ❌ Wave 0 |
| PDFIMG-06 | encrypted PDF → ENCRYPTED | unit | `pytest tests/test_pdf_page_renderer.py::test_encrypted_reason -x` | ❌ Wave 0 |
| PDFIMG-06 | corrupt PDF → CORRUPT | unit | `pytest tests/test_pdf_page_renderer.py::test_corrupt_reason -x` | ❌ Wave 0 |
| PDFIMG-06 | page index out of range → PAGE_OUT_OF_RANGE (validated pre-render, D-04a) | unit | `pytest tests/test_pdf_page_renderer.py::test_page_out_of_range -x` | ❌ Wave 0 |
| PDFIMG-06 | every failure path logs reason + detail | unit | `pytest tests/test_pdf_page_renderer.py::test_failures_logged -x` | ❌ Wave 0 |
| D-03 | stale token result discarded by controller (signal carries token) | unit | `pytest tests/test_pdf_page_renderer.py::test_token_echoed_in_signals -x` | ❌ Wave 0 |

### Sampling Rate
- **Per task commit:** `pytest tests/test_pdf_page_renderer.py -x`
- **Per wave merge:** `pytest tests/test_pdf_page_renderer.py tests/test_local_indexer.py tests/test_local_pdf_extraction_fallback.py`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/test_pdf_page_renderer.py` — covers PDFIMG-01/02/06 + D-03 token echo (all rows above)
- [ ] Encrypted + corrupt + multi-page PDF fixtures — extend `scripts/generate_single_word_fixture.py` (or generate inline in a fixture with `fitz`: `doc.save(path, encryption=fitz.PDF_ENCRYPT_AES_256, owner_pw=..., user_pw=...)` for encrypted; truncate bytes for corrupt)
- [ ] QThread render test note: unit-test the *render functions* and *LRU* directly (no QThread needed); the QThread wrapper can be tested with a `qtbot`/signal-spy if the project uses pytest-qt, else test the worker's `run()` synchronously by calling the underlying method. Existing tests call `extract_pdf_pages()` directly without spinning a QThread — mirror that.

*(Framework install: none — pytest + fitz already present.)*

## Project Constraints (from CLAUDE.md)

- **Dual app, but this phase is desktop-only** — `desktop/pdf_page_renderer.py` is PyQt6; web must remain unaffected (web has no My Library). D-08 forbids a `shared/` split.
- **Python 3.10+, type hints encouraged.**
- **Both apps must be maintained** — but PDFIMG Out-of-Scope explicitly excludes web PDF rendering (`REQUIREMENTS.md:24`), so no web parity work.
- **Keep docs updated** — `docs/OPEN_ISSUES.md` at session start/end; `docs/CODE_INDEX.md` for new desktop module; this is a closeout-time chore, not a Phase 99 code task.
- **Hebrew RTL** — fixtures include RTL Hebrew PDFs; rendering is image-based so RTL is irrelevant to the renderer itself.

## Sources

### Primary (HIGH confidence)
- Codebase (VERIFIED via Read/Grep/Bash this session):
  - `GenizahSearchPro.spec:30` (entry = genizah_app.py), `:17` (collect_all pymupdf) — no multiprocessing/freeze_support
  - `desktop/image_loader.py:15-135` (ImageLoaderThread signal/cancel pattern)
  - `desktop/viewers.py:71` (ZoomableScrollArea), `:1209` (display_image), `:1198-1203` (existing generation-token wiring)
  - `desktop/my_library_tab.py:637` (LocalIndexerWorker QThread), `:739-796` (FolderWalkWorker token pattern), `:698` (PrescanWorker)
  - `shared/local_indexer.py:66-94` (fitz import + stderr suppression), `:672-721` (extract_pdf_pages, 1-based page_num + fitz.open), `:1343` (get_filepath)
  - `corpus_mapper/runner.py:24,570` (ProcessPoolExecutor — standalone dev tool, NOT frozen)
  - `tests/test_local_pdf_extraction_fallback.py:6-22` (existing PDF fixtures + generator)
- `python -c "import pymupdf; print(pymupdf.__version__)"` → 1.27.2.3 `[VERIFIED]`
- `.planning/config.json` → nyquist_validation=true `[VERIFIED]`

### Secondary (MEDIUM confidence)
- PyMuPDF multiprocessing recipe — `[CITED: pymupdf.readthedocs.io/en/latest/recipes-multiprocessing.html]` "PyMuPDF does not support running on multiple threads - doing so may cause incorrect behaviour or even crash Python itself." Recommends multiprocessing for *parallelism*. Note: the existing single-owner QThread indexer does NOT violate this — it is single-threaded fitz access, not concurrent.
- PyMuPDF `Page.get_pixmap` args (`dpi=`, `colorspace=`, `alpha=`) — `[CITED: pymupdf.readthedocs.io/en/latest/page.html]`

### Tertiary (LOW confidence)
- None requiring validation.

## Metadata

**Confidence breakdown:**
- D-09 threading recommendation: HIGH — direct codebase evidence (no multiprocessing in frozen EXE; fitz already runs single-owner off-thread via LocalIndexerWorker through Phase 97.x UAT).
- Standard stack: HIGH — no new deps; PyMuPDF version verified; spec already collects fitz.
- Render/QImage memory safety: HIGH — D-01b is explicit; PyMuPDF↔Qt zero-copy + `.copy()` is well-established.
- Pitfalls: HIGH — derived from PyMuPDF docs + the locked Codex critique + codebase patterns.

**Research date:** 2026-05-27
**Valid until:** 2026-06-26 (stable; PyMuPDF/PyQt6 are slow-moving and pinned in the frozen build)

---

## D-09 Decision — Recommendation: OPTION (a), Single Dedicated Render QThread

**Evidence gathered (the user demanded concrete codebase investigation):**

1. **Does the app/spec call `multiprocessing.freeze_support()`?** NO. Grep for `freeze_support` across the repo returns only `.planning` docs (the decision text itself) — zero occurrences in any `.py` in app/desktop/shared/web or in `GenizahSearchPro.spec`.

2. **Does ANY current frozen-EXE code use `multiprocessing`?** NO. The only `multiprocessing`/`ProcessPoolExecutor` usage is `corpus_mapper/runner.py` (`:24` import, `:570` `ProcessPoolExecutor(... initializer=_init_worker)`), a standalone offline batch dev tool. It is NOT the EXE entry point (`GenizahSearchPro.spec:30` builds `genizah_app.py`) and is NOT in the spec's `datas`/`hiddenimports`, so it is never frozen. Adding multiprocessing to the frozen app would be net-new and would require introducing `freeze_support()` plus PyInstaller-relaunch hardening that has never been exercised on this Windows EXE.

3. **How does fitz run off the UI thread today?** Via **`LocalIndexerWorker(QThread)`** (`desktop/my_library_tab.py:637`) whose `run()` calls `LocalIndexer.scan_all()` → `extract_pdf_pages()` (`shared/local_indexer.py:696`) → `fitz.open()`. Also `PrescanWorker(QThread)` and `FolderWalkWorker(QThread)`. All QThread, all cooperative-cancel, hardened through Phases 95/96/97.x UAT with no fitz crashes observed.

4. **ImageLoaderThread pattern (to mirror):** `desktop/image_loader.py:15` — `QThread` subclass, `image_loaded`/`load_failed` `pyqtSignal`s, cooperative `cancel()` flag checked between network attempts. The render worker generalizes this with a token + the D-04 reason enum.

5. **PyMuPDF thread-safety stance:** The docs say "does not support running on multiple threads … may crash Python." This is about *concurrent* access. Option (a)'s single-owner discipline (ONE thread owns ALL fitz calls + the LRU) satisfies it — and is exactly what the existing indexer already does safely. Option (b) is "more documented-safe" only in the sense of process isolation, but it solves a concurrency problem the single-owner design does not have.

**Conclusion:** The frozen-EXE multiprocessing risk for option (b) is **non-trivial and unproven in this codebase**, while option (a) is **a direct extension of a shipping, UAT-hardened pattern with zero new risk surface**. Per D-09's own fallback clause ("If risk is non-trivial, default to (a)"), the planner should choose **(a)**. The only conceded trade-off — a wedged >8s render briefly blocks the single render thread until the C call returns (Pitfall 4) — is mitigated by the soft `QTimer` watchdog + latest-wins (the UI never freezes) and, if it ever bites in practice, is a contained follow-up (drain/replace the thread), not a reason to take on multiprocessing now.
