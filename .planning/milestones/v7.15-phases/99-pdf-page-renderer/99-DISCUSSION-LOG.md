# Phase 99: PDF Page Renderer - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-27
**Phase:** 99-pdf-page-renderer
**Areas discussed:** Render sharpness, Navigation feel, Failure granularity, Slow-PDF timeout, Threading model (+ Codex review)

---

## Render sharpness

| Option | Description | Selected |
|--------|-------------|----------|
| Fit-to-pane width | Render to pane pixel width, capped | |
| Fixed ~150 DPI | Constant 2× zoom | |
| Fixed ~220 DPI | Constant 3× zoom | |
| Fit-to-pane (re-asked) | — | |
| **Fixed ~200 DPI** (re-asked, grounded in viewer reuse) | One-shot 200 DPI bitmap; existing viewer provides ≤5× zoom | ✓ |
| Re-render on zoom | Low first, higher on zoom | |

**User's choice:** Fixed ~200 DPI.
**Notes:** User interjected "Why not use the image controls including Zoom?" — investigation found `ManuscriptViewerWidget` already gives zoom/pan/rotate for free. Question re-posed; user picked fixed 200 DPI so the free zoom reveals real detail. Drove D-01 + D-02.

---

## Navigation feel

| Option | Description | Selected |
|--------|-------------|----------|
| **Latest-wins (supersede)** | Generation token, discard stale results | ✓ |
| Latest-wins + debounce | + ~120ms settle delay | |
| Queue in order | Render every requested page to completion | |

**User's choice:** Latest-wins (supersede). No debounce.

---

## Failure granularity

| Option | Description | Selected |
|--------|-------------|----------|
| **Reason code enum** | missing/encrypted/corrupt/out-of-range/render-error | ✓ |
| Two buckets | file-problem vs render-problem | |
| Single generic failure | one signal for everything | |

**User's choice:** Reason code enum. (Codex later expanded the enum — see D-04.)

---

## Slow-PDF timeout

| Option | Description | Selected |
|--------|-------------|----------|
| **Soft watchdog ~8s** | Timer budget, placeholder on expiry, discard late result | ✓ |
| No explicit timeout | exceptions only | |
| Tight watchdog ~3s | faster placeholder, risk of giving up on heavy pages | |

**User's choice:** Soft watchdog ~8s.

---

## Threading model (raised by Codex review)

| Option | Description | Selected |
|--------|-------------|----------|
| Single render thread | One dedicated QThread owns all fitz access | |
| Separate render process | Codex-preferred; documented-safe, IPC + freeze_support cost | |
| **Let planner decide** | Capture both + tradeoff; planner investigates frozen-EXE risk | ✓ |

**User's choice:** Let planner decide (D-09 open decision). Single-thread is the recommended default; multiprocessing risk on the frozen Windows EXE must be investigated first.

## Claude's Discretion
- LRU implementation, token counter location, watchdog wiring, `PdfRenderFailure` enum representation.

## Deferred Ideas
- Re-render-on-zoom; adaptive/fit-to-pane DPI; PDF OCR (D-F2).
