# Codex Brief — Phase 99: PDF Page Renderer (GenizahSearch desktop, PyQt6)

## Context
Desktop-only (PyQt6) feature. We index local PDFs into a Tantivy side-index ("My Library"). We now want to show the **source PDF page image** next to extracted text for LOCAL search hits. Phase 99 builds ONLY the shared renderer + off-thread worker + graceful failure. Phase 100 wires it into two UI surfaces (ResultDialog + Browse).

### Existing assets (confirmed)
- `desktop/image_loader.py` — `ImageLoaderThread(QThread)` with `image_loaded(QImage)` / `load_failed()` signals + cooperative `cancel()` flag. The new renderer worker should mirror this pattern.
- `shared/local_indexer.py` — already imports `fitz` (PyMuPDF), has `get_filepath(sys_id)` and `extract_pdf_pages()` (`fitz.open(filepath)`, 1-based page enumeration, so `fitz` page index = `page_num - 1`). `sys_id` + `page_num` already flow to the UI.
- `desktop/viewers.py` — `ZoomableScrollArea(QGraphicsView)` (Ctrl+wheel zoom 0.1–5.0×, fit-to-view, rotate, brightness/contrast/gamma/invert) wrapped by `ManuscriptViewerWidget` with `display_image(QImage)`. We plan to feed the rendered page QImage into `display_image()` to get zoom/pan/rotate for free (no new controls).

### Hard constraints (from REQUIREMENTS)
- Render ONE page on demand; never bulk-render the ~10K-file × up-to-500-page corpus.
- NO on-disk cache of rendered images; hold only currently-displayed page(s) in memory.
- Bounded LRU of open `fitz.Document` handles.
- Failures (missing/corrupt/encrypted/out-of-range/exception/timeout) must degrade to a placeholder + log — no UI hang, no crash.
- Non-PDF files stay text-only (Phase 100 gates on extension).

## Proposed decisions (critique these)
- **D-01 Sharpness:** Fixed ~200 DPI one-shot render (`fitz.Matrix(~2.78,~2.78)`), ≈1700×2200px / ~10–15MB QImage per page. Displayed via existing `ManuscriptViewerWidget` (free ≤5× zoom). No re-render-on-zoom.
- **D-02 Concurrency/nav:** Latest-wins supersede via a generation token; discard stale results. Mirror `ImageLoaderThread.cancel()`. No debounce.
- **D-03 Failure contract:** Worker failure signal carries a reason enum {missing-file, encrypted, corrupt, page-out-of-range, render-error}; always log; Phase 100 maps to placeholder text.
- **D-04 Timeout:** Soft UI-side watchdog ~8s; on expiry show timeout placeholder + discard late result. Off-thread `get_pixmap()` not force-killed.

## Questions for you
1. **fitz thread-safety / LRU:** PyMuPDF `Document` objects are NOT thread-safe across concurrent access. With a latest-wins worker model, is a single dedicated render thread serializing access to a shared LRU of `fitz.Document` handles the right call, vs per-request QThreads each opening its own doc (no shared LRU)? What LRU size is sane for a desktop app (open handles hold file + some memory)? Any reopen-on-evict pitfalls?
2. **200 DPI fixed vs fit-to-pane:** Given the viewer zooms ≤5×, is a fixed ~200 DPI bitmap the right memory/quality tradeoff, or is it wasteful for large multi-hundred-page browsing? Better matrix?
3. **Watchdog:** Is a UI-side QTimer watchdog the cleanest way to bound render time when the C call can't be interrupted, or is there a better PyMuPDF-native guard? Risk of orphaned threads accumulating under rapid navigation + slow renders?
4. **Worker signal contract:** Best shape for the worker's signals to serve BOTH a single-result surface (ResultDialog) and a paged surface (Browse) in Phase 100? (e.g. emit (request_token, page_num, QImage) on success, (request_token, reason) on failure.)
5. **Module placement:** Renderer produces a QImage (PyQt6 dependency) but is conceptually "shared". Put it in `desktop/` (PyQt-coupled, fine since web has no My Library) or keep a Qt-free core renderer in `shared/` returning raw pixmap bytes + a thin `desktop/` QImage adapter? Over-engineering?
6. Anything we're missing for graceful failure or memory safety?

Be concise and decisive. Flag anything that would cause a bug, hang, or memory leak.
