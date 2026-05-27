---
status: partial
phase: 100-local-pdf-image-in-resultdialog-browse
source: [100-VERIFICATION.md]
started: 2026-05-27T16:06:04Z
updated: 2026-05-27T16:06:04Z
---

## Current Test

[awaiting human testing]

## Tests

### 1. ResultDialog LOCAL PDF initial open
expected: Opening a LOCAL PDF hit in the desktop ResultDialog reveals the (previously hidden-for-LOCAL) external image pane and shows the rendered page image alongside the extracted text. (PDFIMG-03)
result: [pending]

### 2. ResultDialog prev/next result navigation
expected: Navigating between results (prev/next result) re-renders the image for the newly shown LOCAL PDF hit, in sync with the text. (PDFIMG-03)
result: [pending]

### 3. ResultDialog LOCAL non-PDF hit
expected: A LOCAL non-PDF hit (.docx/.html/.xlsx/.csv/.txt) keeps the image pane hidden with no render attempt. (PDFIMG-05)
result: [pending]

### 4. Browse LOCAL PDF pane reveal + page sync
expected: Opening a LOCAL PDF result in the desktop Browse panel reveals the image pane and shows the rendered page; prev/next PAGE navigation updates the image to the matching page in sync with the text. (PDFIMG-04)
result: [pending]

### 5. Browse LOCAL non-PDF result
expected: A LOCAL non-PDF result stays text-only — the Browse image pane remains hidden, no render attempt. (PDFIMG-05)
result: [pending]

### 6. Browse LOCAL PDF → Genizah manuscript switch
expected: Switching from a LOCAL PDF Browse result to a Genizah manuscript shows no stale PDF image and does not crash (the "browse" scope is cancelled). (REVIEWS-R2-4)
result: [pending]

### 7. Render failure graceful degradation
expected: If a page fails to render (corrupt/unreadable PDF), a localized error placeholder is shown; the app does not freeze or crash. (watchdog/placeholder path)
result: [pending]

## Summary

total: 7
passed: 0
issues: 0
pending: 7
skipped: 0
blocked: 0

## Gaps
