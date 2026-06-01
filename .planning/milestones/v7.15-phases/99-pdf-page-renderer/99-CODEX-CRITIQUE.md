# Codex Critique — Phase 99 PDF Page Renderer

**Date:** 2026-05-27 · **Model:** gpt-5.5 (xhigh) · Brief: `99-CODEX-BRIEF.md`

## Verdict per decision

1. **Threading (PUSHBACK):** PyMuPDF docs say multithreaded use is unsupported / may crash Python; they recommend **multiprocessing** (one long-lived render process, serialized queue, LRU + latest-wins token inside it). If keeping QThread: use **exactly one dedicated render thread** that exclusively owns all `fitz` access — never touch `fitz` from any other thread concurrently — but acknowledge that is outside the documented safety line. Source: pymupdf.readthedocs.io/en/latest/recipes-multiprocessing.html
2. **LRU size:** Small. Start at **4 open documents** (configurable 2–8). Close evicted docs explicitly. Do NOT cache pages/pixmaps in the LRU — handles only.
3. **DPI:** Fixed 200 DPI reasonable. Prefer `page.get_pixmap(dpi=200, colorspace=fitz.csRGB, alpha=False)` over a manual matrix — `alpha=False` saves real memory. Keep DPI a module constant, not adaptive yet.
4. **Watchdog:** UI-side `QTimer` watchdog is the right UX contract; it cannot stop MuPDF mid-render. With a render *process*, a wedged render can be killed past a larger threshold; with a QThread it cannot.
5. **Signal contract:** Tokenized + surface-agnostic, carry `sys_id` + `page_num`:
   - `render_succeeded(token:int, sys_id:str, page_num:int, image:QImage)`
   - `render_failed(token:int, sys_id:str, page_num:int, reason:PdfRenderFailure, detail:str)`
   - timeout surfaced by the UI controller, not the worker.
6. **Placement:** `desktop/pdf_page_renderer.py` (Qt-facing). A `shared/` Qt-free renderer is only worth it if web/CLI will consume it — not now (over-engineering).
7. **Expanded failure enum:** add `password-needed/encrypted`, `not-pdf`, `missing-file`, `page-out-of-range`, `open-error`, `render-error`, `timeout`, `cancelled/stale`. Validate page bounds before render. Close docs on shutdown. **Copy the QImage away from pixmap bytes** (`QImage(...).copy()`) before the pixmap buffer is freed — else use-after-free.

## Adopted vs deferred
- **Adopted:** LRU=4 (2–8 configurable), `get_pixmap(dpi=200, csRGB, alpha=False)`, richer tokenized signal contract w/ sys_id+page_num+detail, expanded failure enum, `desktop/pdf_page_renderer.py` placement, QImage `.copy()` memory-safety, explicit doc close on evict/shutdown.
- **Escalated to user:** threading model — single dedicated render thread (matches existing `ImageLoaderThread` + existing off-UI-thread `fitz` indexing in `local_indexer`/`LocalIndexerWorker`, frozen-EXE-friendly) vs Codex-preferred multiprocessing render process (documented-safe but heavy IPC of ~15MB bitmaps + PyInstaller `freeze_support` complexity on the frozen Windows EXE).
