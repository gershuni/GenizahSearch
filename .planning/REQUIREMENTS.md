# Requirements: v7.15 My Library Visual

> Milestone goal: Show the source PDF *page image* alongside extracted text for LOCAL ("My Library") results in the desktop app — closing deferred item D-F3.
>
> **Scope:** Desktop-only (web "My Library" does not exist; the dual-app rule does not apply). PDFs only — other LOCAL file types stay text-only.

## v7.15 Requirements

### PDF Page Image Rendering (PDFIMG)

- [ ] **PDFIMG-01**: A shared on-demand renderer produces a single PDF page image (QImage) from a filepath + 1-based page number, without loading the rest of the document. Page numbering matches the `page_num` already stored per LOCAL page (PDF physical page; `fitz` page index = `page_num - 1`).
- [ ] **PDFIMG-02**: Rendering runs off the UI thread via a worker (mirroring the existing `ImageLoaderThread` pattern), backed by a bounded LRU of open `fitz.Document` handles, with no on-disk image cache — render output is held only for the currently displayed page(s).
- [ ] **PDFIMG-03**: In the desktop `ResultDialog`, a LOCAL PDF hit shows its rendered page image alongside the extracted text; navigating between results (prev/next result) re-renders the image for the newly shown hit.
- [ ] **PDFIMG-04**: In the desktop Browse panel, opening a LOCAL PDF result shows the rendered page image in the (previously hidden) image pane; prev/next *page* navigation updates the image to the matching page in sync with the text.
- [ ] **PDFIMG-05**: Non-PDF LOCAL files (`.docx`/`.html`/`.xlsx`/`.csv`/`.txt`) remain text-only — the image pane stays hidden, gated on file extension (no attempt to render).
- [ ] **PDFIMG-06**: Render failures (missing file, corrupt/encrypted PDF, page out of range, render exception/timeout) degrade gracefully to a user-visible placeholder and a log entry — no UI hang and no crash.

## Future Requirements (deferred)

- **PDF OCR (D-F2)**: Scanned/image-only PDFs (no text layer) get an OCR pass (Tesseract or similar) so their text becomes indexable/searchable. Related but separable from page-image *display*; candidate follow-up phase within this milestone or a later one.

## Out of Scope

- **Web "My Library" PDF rendering** — the feature does not exist on web; no web parity required.
- **Zoom / pan / rotate / annotation of the PDF page** — display the page; reuse existing viewer affordances only if free. Rich image manipulation is not in scope.
- **Disk-caching or pre-rendering rendered page images** — explicitly excluded to keep the 10K×500-page corpus from ever being bulk-rendered; rendering stays lazy and ephemeral.
- **Rendering non-PDF formats to images** (e.g. rendering a `.docx`/`.xlsx` page to an image) — text-only for those types.
- **Changing the LOCAL indexing/extraction pipeline** — this milestone is display-only; `local_indexer.py` text extraction is unchanged.

## Traceability

| Requirement | Phase |
|-------------|-------|
| PDFIMG-01 | _(filled by roadmap)_ |
| PDFIMG-02 | _(filled by roadmap)_ |
| PDFIMG-03 | _(filled by roadmap)_ |
| PDFIMG-04 | _(filled by roadmap)_ |
| PDFIMG-05 | _(filled by roadmap)_ |
| PDFIMG-06 | _(filled by roadmap)_ |
