---
status: complete
phase: 30-direct-image-access
source: 30-01-SUMMARY.md, 30-02-SUMMARY.md
started: 2026-02-15T14:10:00Z
updated: 2026-02-15T14:25:00Z
---

## Current Test

[testing complete]

## Tests

### 1. Web NLI Image Loading (Sidecar)
expected: Open a manuscript with NLI coverage in the web app browse page (e.g., a CUL T-S shelfmark). The manuscript image should load and display normally.
result: pass

### 2. Web Fallback Image Loading (No Sidecar Coverage)
expected: Open a manuscript that is NOT covered by the NLI crossref sidecar in the web app. The image should still load via the existing network IIIF manifest fetch fallback.
result: pass

### 3. Desktop NLI Image Loading (Sidecar)
expected: Open a manuscript with NLI coverage in the desktop app. The manuscript image should load and display in the image viewer. Resolution happens from the local sidecar.
result: pass

### 4. Desktop Cambridge Image Loading (Sidecar Supplement)
expected: Open a Cambridge (CUL) manuscript in the desktop app. If the MARC data lacks a CUDL link, the sidecar provides the Cambridge IIIF manifest URL. The image should still load.
result: pass

### 5. Desktop Fallback (No Sidecar)
expected: Open a manuscript NOT covered by the sidecar in the desktop app. Images should still load via the existing network fetch path (IIIF manifest or MARC API).
result: pass

## Summary

total: 5
passed: 5
issues: 0
pending: 0
skipped: 0

## Gaps

[none yet]
