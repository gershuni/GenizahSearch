# Phase 99: PDF Page Renderer - Context

**Gathered:** 2026-05-27
**Status:** Ready for planning

<domain>
## Phase Boundary

A shared, desktop-only service that renders **one** PDF page to a `QImage` on demand, off the UI thread, with a bounded LRU of open `fitz.Document` handles, **no on-disk image cache**, and graceful failure on bad input. Satisfies PDFIMG-01 (single-page render, `fitz` index = `page_num − 1`), PDFIMG-02 (off-thread worker mirroring `ImageLoaderThread` + bounded doc LRU + no disk cache), PDFIMG-06 (graceful failure → placeholder + log, no hang/crash).

**NOT in this phase:** wiring into `ResultDialog` / Browse (Phase 100), non-PDF handling/extension gating (Phase 100), any change to the LOCAL indexing/extraction pipeline, OCR (D-F2 deferred), disk caching or pre-rendering.
</domain>

<decisions>
## Implementation Decisions

### Render output (PDFIMG-01)
- **D-01:** Fixed **200 DPI** one-shot render per page via `page.get_pixmap(dpi=200, colorspace=fitz.csRGB, alpha=False)` (Codex refinement — `alpha=False` saves real memory; prefer `dpi=` over a manual matrix). DPI is a **module constant**, not adaptive. `fitz` page index = `page_num − 1` (1-based `page_num` already stored per LOCAL page).
- **D-01a:** The rendered bitmap is sized generously *on purpose* so the existing viewer's ≤5× zoom reveals real detail rather than upscaled blur. **No re-render-on-zoom** (rejected as Phase-100 scope creep).
- **D-01b (memory safety):** The `QImage` MUST be `.copy()`'d off the pixmap sample buffer before the pixmap is freed — otherwise use-after-free. (Codex.)

### Display reuse (carries into Phase 100)
- **D-02:** Render output is displayed by feeding the `QImage` into the **existing `ManuscriptViewerWidget.display_image()`** (`desktop/viewers.py`), which wraps `ZoomableScrollArea` — giving Ctrl+wheel zoom (0.1–5.0×), pan, rotate, and brightness/contrast/gamma/invert **for free**. **No new image pane or zoom controls are built.** This is the REQUIREMENTS "reuse existing viewer affordances *if free*" clause, surfaced by the user during discussion. (Phase 100 wires it; locked here because it shapes D-01's resolution choice.)

### Concurrency / navigation (PDFIMG-02)
- **D-03:** **Latest-wins supersede.** Each request carries a monotonic **generation token**; results from superseded requests are discarded so a stale page never lands in the pane after the user has navigated on. Mirrors `ImageLoaderThread.cancel()`. **No debounce.**

### Failure contract (PDFIMG-06)
- **D-04:** Worker failure signal carries a **reason enum** — `missing-file`, `not-pdf`, `encrypted`/`password-needed`, `corrupt`/`open-error`, `page-out-of-range`, `render-error`, `timeout`, `cancelled`/`stale` (expanded per Codex). Phase 100 maps each to specific placeholder text. **Every failure is logged** with the reason + a `detail` string.
- **D-04a:** Validate page bounds (`0 ≤ page_num−1 < doc.page_count`) before attempting render → `page-out-of-range`.

### Timeout (PDFIMG-06)
- **D-05:** **Soft UI-side `QTimer` watchdog, ~8s budget.** On expiry: show the timeout placeholder (`timeout` reason) and discard any late result (latest-wins already discards). The off-thread `get_pixmap()` C call is **not force-killed** in the thread model; it finishes and is dropped. UI never freezes. (If the process model is chosen — see open decision — a wedged render past a larger threshold may be killed.)

### Document LRU (PDFIMG-02)
- **D-06:** Bounded LRU of **open `fitz.Document` handles, size 4** (configurable 2–8; Codex). Evicted docs are `.close()`'d **explicitly**. **No page or pixmap objects cached** — handles only. **No on-disk image cache** (hard REQUIREMENTS constraint). All docs closed on app shutdown.

### Worker signal shape (serves both Phase 100 surfaces)
- **D-07:** Tokenized, surface-agnostic, carrying `sys_id` + `page_num` (Browse needs them; aids stale-result logging):
  - `render_succeeded(token: int, sys_id: str, page_num: int, image: QImage)`
  - `render_failed(token: int, sys_id: str, page_num: int, reason: PdfRenderFailure, detail: str)`
  - Timeout is surfaced by the UI controller (the watchdog), **not** emitted by the worker.

### Module placement
- **D-08:** New module `desktop/pdf_page_renderer.py` (Qt-facing). **No `shared/` Qt-free split** — over-engineering, since web has no "My Library" and there's no other consumer. (Codex.)

### Open Decision — escalated to planner
- **D-09 (THREADING MODEL — planner to decide with investigation):** Two viable models, user deferred to planning:
  - **(a) Single dedicated render thread** *(recommended default)* — ONE long-lived QThread exclusively owns all `fitz` access (the LRU + every open/render call); no other thread touches `fitz` concurrently. Matches `ImageLoaderThread` and the existing off-UI-thread `fitz` indexing in `local_indexer.py` / `LocalIndexerWorker` (hardened through Phase 97.x UAT). Frozen-EXE-friendly, no IPC. The single-owner discipline is the mitigation for PyMuPDF's "not thread-safe across concurrent access" caveat.
  - **(b) Separate render process** *(Codex-preferred, documented-safe)* — a long-lived child process owns `fitz`, returns raw RGB bytes/width/height/stride over IPC, parent builds the `QImage`. Documented-safe and a wedged render can be killed — but adds PyInstaller `multiprocessing.freeze_support()` risk on the frozen Windows EXE and ~15MB/page IPC cost.
  - **Planner action:** investigate the frozen-EXE multiprocessing risk concretely (does the app already call `freeze_support()`? does any current code use `multiprocessing`?) before choosing. If risk is non-trivial, default to (a).

### Claude's Discretion
- Exact LRU implementation (`OrderedDict` vs `functools`), token type/counter location, watchdog wiring details, `PdfRenderFailure` enum representation (`enum.Enum` vs string constants).
</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Requirements & milestone
- `.planning/REQUIREMENTS.md` — v7.15 PDFIMG-01..06 (Phase 99 owns PDFIMG-01/02/06); Out-of-Scope list (no zoom-build, no disk cache, no OCR, no non-PDF rendering).
- `.planning/ROADMAP.md` §"Phase 99: PDF Page Renderer" — goal + 4 success criteria.

### Cross-AI review (this phase)
- `.planning/phases/99-pdf-page-renderer/99-CODEX-CRITIQUE.md` — Codex critique of all decisions; threading pushback, LRU=4, `alpha=False`, expanded failure enum, QImage `.copy()` memory-safety, signal contract.
- `.planning/phases/99-pdf-page-renderer/99-CODEX-BRIEF.md` — the brief Codex was given.

### External docs (PyMuPDF)
- PyMuPDF multiprocessing recipe — https://pymupdf.readthedocs.io/en/latest/recipes-multiprocessing.html (relevant to D-09 threading choice)
- PyMuPDF `Page.get_pixmap` — https://pymupdf.readthedocs.io/en/latest/page.html (`dpi=`, `colorspace`, `alpha` args)
</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `desktop/image_loader.py::ImageLoaderThread(QThread)` — the pattern to mirror: `image_loaded(QImage)` / `load_failed()` signals + cooperative `cancel()` flag. New worker generalizes this with a token + reason enum.
- `desktop/viewers.py::ManuscriptViewerWidget.display_image(QImage)` (line ~1209) wrapping `ZoomableScrollArea(QGraphicsView)` (line ~71) — provides zoom/pan/rotate/adjustments for free (D-02). `set_image(pixmap)` is the inner entry; `display_image` is the QImage-facing method.
- `shared/local_indexer.py::get_filepath(sys_id)` (line ~643) — resolves the PDF path the renderer needs. `extract_pdf_pages()` (line ~672) confirms 1-based `page_num` ↔ `fitz` enumerate(start=1), so render index = `page_num − 1`.

### Established Patterns
- `fitz` is already a desktop dependency and already runs **off the UI thread** during indexing (`LocalIndexerWorker` / `FolderWalkWorker`, Phase 97.3) — no separate process, no crashes observed. Informs D-09 option (a).
- `fitz.TOOLS.mupdf_display_warnings(False)` / `mupdf_display_errors(False)` set at `local_indexer.py` import (Phase 97.3 R97.3-C) — keeps MuPDF stderr noise down; the renderer module gets the same quiet behavior for free via that import.

### Integration Points
- New file `desktop/pdf_page_renderer.py` (D-08). Phase 100 calls it from `desktop/result_dialog.py` and the Browse panel, feeding results into `ManuscriptViewerWidget`.
</code_context>

<specifics>
## Specific Ideas

- The user explicitly asked "Why not use the image controls including Zoom?" — this drove D-02 (reuse `ManuscriptViewerWidget`, no new controls) and D-01a (render generously so the free zoom is meaningful). This is the load-bearing design choice of the milestone's visual layer.
- The user delegated the threading sub-decision to the planner after the Codex critique (D-09) — planner must investigate frozen-EXE multiprocessing risk, not just pick.
</specifics>

<deferred>
## Deferred Ideas

- **Re-render-on-zoom** (higher DPI when zoomed past a threshold) — sharper at deep zoom with less idle memory, but spills zoom→re-render plumbing into Phase 100. Out of scope; fixed 200 DPI chosen instead.
- **Adaptive / fit-to-pane DPI** — render to the pane's pixel width. Rejected for Phase 99 in favor of a fixed module constant; revisit only if memory becomes a problem during multi-hundred-page browsing.
- **PDF OCR for image-only scanned PDFs (D-F2)** — already a milestone-level deferral; unrelated to display.
</deferred>

---

*Phase: 99-pdf-page-renderer*
*Context gathered: 2026-05-27*
