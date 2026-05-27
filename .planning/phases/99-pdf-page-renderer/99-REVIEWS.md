---
phase: 99
reviewers: [codex]
reviewed_at: 2026-05-27
plans_reviewed: [99-01-PLAN.md, 99-02-PLAN.md]
---

# Cross-AI Plan Review — Phase 99

> Reviewer: Codex (OpenAI, default model). Claude CLI skipped (running inside Claude Code — self-review excluded for independence). Gemini not requested.

## Codex Review

## PLAN 99-01 Review

### Summary
The render-core plan is mostly aligned with the locked decisions and has the right boundaries for Phase 99: single-page render, no disk cache, no UI, bounded open-document LRU. The main weaknesses are around failure logging ownership, fixture/test hermeticity, and edge-case classification. The `.copy()` plus `pix.stride` approach is correct for QImage memory safety, assuming the copy happens immediately before the pixmap can be freed.

### Strengths
- Uses the locked 200 DPI, `fitz.csRGB`, `alpha=False`, and `page_num - 1` mapping.
- Correctly calls `.copy()` on the `QImage` and uses `pix.stride`, addressing the key use-after-free / row-stride hazard.
- Keeps only open `fitz.Document` handles in the LRU; no page, pixmap, image, or disk cache scope creep.
- Classifies encrypted PDFs after `fitz.open()` and closes the handle before raising.
- Page bounds are validated before render, satisfying D-04a.

### Concerns
- **MEDIUM:** "Every failure logged" is not clearly satisfied. Plan 99-01 raises `PdfRenderError`, while Plan 99-02 logs in the worker. Direct uses of `render_via_lru()` could fail without a log unless the core logs before raising.
- **MEDIUM:** PDFIMG-06 says placeholder degradation, but Phase 99 has no UI. Fine architecturally, but the plan should explicitly narrow Phase 99's responsibility to "failure signal / exception with reason + detail"; placeholder image/display belongs to Phase 100.
- **MEDIUM:** `.pdf` suffix classification needs to be case-insensitive. Local files can be `.PDF` / `.Pdf`.
- **MEDIUM:** The fixture script is underspecified — whether fixtures are committed or generated in `tmp_path` during tests. Otherwise CI can silently depend on a missing manual step.
- **LOW:** `close()` on eviction/shutdown can raise. Unlikely, but it could turn cleanup into a render/shutdown failure.
- **LOW:** `test_render_single_page` does not necessarily prove "no bulk render." It proves one output, not that only the requested page was rendered.

### Suggestions
- Add a single helper that logs `reason + detail` exactly once before raising `PdfRenderError`, or explicitly state that only the worker API satisfies the logging part of PDFIMG-06.
- Normalize paths with `Path`/`os.fspath`, use `suffix.lower() == ".pdf"`, catch `OSError` around path checks.
- Make fixture generation hermetic: generate PDFs inside pytest fixtures using `tmp_path`, or commit generated fixtures and make the script only a regeneration tool.
- Add tests for `page_num=0`, uppercase `.PDF`, and a valid-looking `.pdf` containing non-PDF bytes.
- Wrap document close on eviction and `close_all()` in best-effort logging cleanup.
- To prove no bulk rendering, monkeypatch/wrap `page.get_pixmap` and assert it is called only once for the requested page.

### Risk Assessment
**LOW-MEDIUM.** Core design sound. Remaining risks are test precision and failure-contract ambiguity, not fundamental architecture problems.

---

## PLAN 99-02 Review

### Summary
The worker plan has the right high-level shape, but its current testing and shutdown strategy are the weak points. A blocking queue-loop `QThread.run()` is not synchronously testable by simply calling `run()`: it blocks on `queue.get()`, `_assert_worker_thread()` will fail outside a real QThread, and default cross-thread Qt signals need an event loop. The plan's optional `_handle(self, item)` refactor should be mandatory. The proposed `terminate()` fallback is also a serious violation of the "do not force-kill the C call" decision and risks closing `fitz.Document` handles from the wrong thread.

### Strengths
- Signal shape matches D-07, including `object` for `PdfRenderFailure`.
- Token ownership stays outside the worker; the worker echoes tokens only.
- A single long-lived worker thread owning all render-time `fitz` access matches D-09.
- Per-request exception handling lets the worker survive a corrupt render and serve the next request.
- Keeping the LRU inside the worker model is the right way to avoid cross-thread `fitz` access.

### Concerns
- **HIGH:** The synchronous test strategy will deadlock or fail unless refactored. Calling `run()` directly blocks forever unless the queue is preloaded with work and a stop sentinel; even then `_assert_worker_thread()` fails because the current thread is not `self`.
- **HIGH:** Plain Python slot capture is insufficient with a real QThread unless connections use `Qt.DirectConnection` or the test spins a Qt event loop. The repo already uses `Qt.DirectConnection` in QThread tests for this reason.
- **HIGH:** `stop()` using `terminate()` contradicts D-05's "worker does NOT force-kill the C call." It can also leave PyMuPDF in an unsafe state.
- **HIGH:** `stop()` calling `self._lru.close_all()` from the caller thread breaks the single-owner rule. If a render is wedged or active, this can close a document while the render thread owns it.
- **MEDIUM:** `if item is _STOP or self._stopping: break` means calling `stop()` can cause the worker to drop queued real render items before reaching the sentinel. May be acceptable on shutdown, but the behavior must be explicit and tests must not rely on prequeue-then-stop.
- **MEDIUM:** The queue is unbounded. Token echo prevents stale display, but not stale rendering work piling up if Phase 100 enqueues rapidly.
- **LOW:** `enqueue()` after stop/dead-thread has no defined behavior. It can silently enqueue work that never emits a result.
- **LOW:** Splitting both plans across `desktop/pdf_page_renderer.py` is fine only if Wave 2 truly depends on Wave 1 and they are not implemented concurrently.

### Suggestions
- Make `_handle_request(item)` mandatory. Put the thread assertion in `run()`, then call `_handle_request()` from there. Unit tests can call `_handle_request()` synchronously without testing the blocking queue loop.
- Add one real-thread integration test using `worker.start()`, `Qt.DirectConnection`, and a `threading.Event`/`queue.Queue` to capture emitted results without pytest-qt.
- Remove `terminate()` from `stop()`. Use cooperative shutdown: set stopping flag, enqueue `_STOP`, wait, log if the thread does not exit. Let `run()` close the LRU in a `finally` block on the render thread.
- Do not call `close_all()` from outside the render thread except in tests that never started the thread.
- Define `enqueue()` behavior after stop: raise, return `False`, or log/drop.
- Consider a bounded or coalescing queue API for Phase 100. Otherwise "latest wins" only protects UI state, not CPU/render backlog.

### Risk Assessment
**MEDIUM-HIGH as written.** The worker concept is correct, but testability and shutdown details are correctness risks, not implementation details. Fixing `_handle_request()` and removing cross-thread/forced shutdown would bring this down to **LOW-MEDIUM**.

---

## Consensus Summary

Single external reviewer (Codex). Highlights and orchestrator cross-checks against the full plan text:

### Agreed Strengths
- D-01b memory safety (`.copy()` + `pix.stride`) is correct.
- LRU scope discipline (handles only, no disk/pixmap cache) honored.
- D-07 signal shape (incl. `object` enum slot) and token-echo ownership correct.
- Single-owner fitz / D-09 option (a) is the right call.

### Agreed Concerns (action priority)

**Already covered in the full plan text (Codex worked from a summary):**
- *Case-insensitive `.pdf`* — 99-01 Task 2 action already uses `filepath.lower().endswith(".pdf")`. ✅ Covered; could add an uppercase-`.PDF` test for explicitness.
- *Per-failure logging* — 99-01 already states the classifier "logs before raising: `logger.warning(...)`" at every raise site, so direct `render_via_lru()` callers DO get a log. Mostly ✅; worth tightening to a single log helper to guarantee "exactly once."
- *Phase 99 = no UI placeholder* — already explicit in CONTEXT/plan boundaries (placeholder is Phase 100). ✅

**Genuinely actionable (NOT yet in the plans) — strongest signal:**
1. **HIGH — 99-02 synchronous testability.** Calling `run()` directly deadlocks on `queue.get()`, and `_assert_worker_thread()` fails off-thread. The `_handle(self, item)` extraction (currently *optional* in 99-02 Task 2) should be **mandatory**, with the thread assertion in `run()` (not `_handle`). Unit tests call `_handle_request()` synchronously.
2. **HIGH — 99-02 `stop()` `terminate()` + cross-thread `close_all()`.** `terminate()` force-kills a thread possibly mid-`fitz` C call (against D-05's spirit and PyMuPDF safety) and the caller-thread `close_all()` violates single-owner. Fix: cooperative shutdown only — set flag, enqueue `_STOP`, `wait()`, log if it doesn't exit; close the LRU in a `finally` inside `run()` (render thread). Keep `close_all()` off the caller thread except in never-started-thread tests.
3. **MEDIUM — real-thread signal test.** Without pytest-qt, a real-thread test needs `Qt.DirectConnection` (the repo already does this elsewhere) + a `threading.Event`/`queue.Queue` to capture emissions. Add one such integration test alongside the synchronous `_handle` tests.
4. **MEDIUM — `enqueue()` after `stop()` undefined; unbounded queue.** Define post-stop enqueue behavior (drop+log). Unbounded queue is acceptable for Phase 99 (latest-wins is a Phase 100 controller concern) but note it explicitly as a Phase 100 follow-up.
5. **LOW — fixture hermeticity, `close()` raising, no-bulk-render assertion.** Make fixtures committed-or-`tmp_path` explicit; wrap evict/close in best-effort try/except; strengthen `test_render_single_page` (or add a test) to monkeypatch `get_pixmap` and assert it's called exactly once.

### Divergent Views
None — single reviewer.

### Verdict
99-01 is **LOW-MEDIUM** risk and largely ship-ready (its open items are test-precision polish). 99-02's worker **shutdown + testability** are the real findings worth a revision pass before execution: items 1–3 above are correctness-level, not cosmetic.
