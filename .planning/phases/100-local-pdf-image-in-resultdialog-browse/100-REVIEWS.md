---
phase: 100
reviewers: [codex]
reviewed_at: 2026-05-27
plans_reviewed: [100-01-PLAN.md, 100-02-PLAN.md, 100-03-PLAN.md]
---

# Cross-AI Plan Review — Phase 100

## Codex Review

**Summary**

The plans cover the intended feature path and reuse Phase 99 well, but Plan 100-01's single shared `PdfImageController` is not safe as written. It implements "latest wins" globally across Browse and ResultDialog, while the product needs "latest wins per visible surface." There are also stale-render invalidation gaps when leaving a PDF view, especially in ResultDialog.

**Strengths**

- Reuses the Phase 99 worker cleanly: one app-level `PdfRenderWorker`, no extra fitz access in UI code.
- Good separation of rendering policy into a thin controller: debounce, tokening, watchdog, placeholders.
- Extension gate is intentionally simple and mostly correct: `.PDF` works, fake `.pdf` falls through to worker `NOT_PDF`.
- Qt thread model is broadly sound: controller lives on the GUI thread, worker emits `QImage`, and `display_image()` converts to `QPixmap` on the GUI thread.
- Browse hooks the right path: `_open_local_browse_page` is the per-page local Browse point of truth.

**Concerns**

- **HIGH — Plan 100-01: one shared controller state causes cross-surface interference.**
  A single `_awaiting_token`, `_pending`, debounce timer, and watchdog means a Browse render in flight is invalidated when ResultDialog requests a render, and vice versa. That can leave the older surface stuck on "Loading…" or silently never updated. D-05 requires one code path, not one global request slot.

- **HIGH — Plan 100-01 / 100-02: stale PDF requests are not cancelled when navigating to non-PDF or non-local content.**
  `request()` returns `None` for non-PDF but does not invalidate an older PDF token. Plan 100-02 hides the pane for non-PDF but does not cancel the previous render, and its callbacks do not recheck `current_sys_id/current_p_num`. A late PDF success can write into `ms_viewer` after the dialog has moved to a `.docx`, `.txt`, or Genizah result.

- **HIGH — Plan 100-02: likely duplicate render requests on opening a LOCAL result.**
  `load_result_by_index()` calls `load_page(target=p)`, which dispatches LOCAL hits to `load_local_page()`. If `_maybe_render_local_pdf_image()` is added at the end of both methods, initial LOCAL PDF open will request twice. Debounce may mask it, but token churn and duplicate "Loading…" callbacks are avoidable.

- **MEDIUM — Plan 100-01: `_pending` retains UI callbacks after terminal states.**
  Success/failure stop the watchdog but do not clear `_pending` or `_awaiting_token`. Because callbacks close over dialog/viewer objects, the app-level controller can retain a closed ResultDialog until another request overwrites `_pending`.

- **MEDIUM — Plan 100-01: one watchdog timer is only correct for one logical consumer.**
  After timeout, the next request works because it sets a fresh token. Dropping the late result is intentional. But with Browse and ResultDialog both active, a single watchdog cannot track both outstanding UI promises.

- **MEDIUM — Plan 100-03: Browse callback only checks `current_browse_sid`, not page.**
  Tokening should prevent stale same-document page renders, but the defense-in-depth guard should capture and compare both `sys_id` and `page_num` against `current_browse_sid/current_browse_p`.

- **MEDIUM — Plan 100-01 / 100-03: rapid navigation can build a stale FIFO render backlog.**
  The Phase 99 worker queue is unbounded and FIFO. A 150ms debounce helps only for faster-than-150ms changes; page changes every 200ms can enqueue many stale renders, delaying the current page image.

- **MEDIUM — Plan 100-02: `fp.lower().endswith('.pdf')` can crash if `fp` is `None`.**
  The plan should consistently call `controller.is_pdf(fp)` or guard with `bool(fp)`.

- **LOW — Plan 100-01: `_lang()` importing `genizah_app` is heavier than needed.**
  `CURRENT_LANG` originates in `genizah_core`; importing from there avoids a heavy lazy import in controller-only tests.

- **LOW — Wave ordering: 100-01 and 100-03 both edit `genizah_app.py`.**
  The edits are logically separate, but line-anchor drift is likely in a 25K-line file. Use semantic anchors and integrate serially after 100-01 lands.

**Suggestions**

- Change Plan 100-01 to either:
  - one controller object with per-surface scoped state, e.g. `request(scope, ...)`, `cancel(scope)`, per-scope debounce/watchdog timers, and a global token counter; or
  - separate controller instances per surface sharing one worker plus a shared/global token allocator.
- Add explicit invalidation APIs:
  - `cancel(scope, silent=True)` for non-PDF, non-local, dialog close, and Browse leaving LOCAL PDF.
  - Clear `_pending`, callbacks, and `_awaiting_token` on success, failure, timeout, and cancel.
- Route worker results by token to stored request state, and validate `sys_id` plus `page_num` before invoking callbacks.
- In ResultDialog, avoid duplicate calls: let `load_local_page()` trigger PDF rendering for LOCAL page state changes, and have `load_result_by_index()` only clear/cancel when the new result is not LOCAL PDF.
- Add tests for the actual risky cases:
  - Browse request in flight, then ResultDialog request: both surfaces should behave independently.
  - PDF request, then non-PDF navigation before debounce fires: no enqueue and no late callback.
  - PDF request, then Genizah navigation before success: no stale PDF display.
  - Success/failure/timeout clears retained callbacks.
  - Same `sys_id`, different page: stale page image is discarded.

**Risk Assessment: HIGH**

As written, the feature likely works in simple single-surface happy paths, but the shared controller state and missing cancellation are central correctness issues. They can produce stale images, stuck loading placeholders, and retained dialog objects. The architecture is salvageable: keep the single shared worker, but make controller request state scoped per consumer and add explicit cancellation/invalidation.

---

## Consensus Summary

Single reviewer (Codex) this pass. Synthesized priorities:

### Agreed Strengths
- Clean reuse of the shipped Phase 99 worker (one app-level worker, no fitz in UI code).
- Thin controller correctly isolates rendering policy (debounce / token / watchdog / placeholder).
- Qt thread model is sound; extension gate is acceptably simple.

### Top Concerns (priority order)
1. **HIGH — Shared single-token controller is the architectural flaw.** One `_awaiting_token` / `_pending` / debounce / watchdog is shared by BOTH surfaces. When both are live (a ResultDialog opened over Browse, which is a real scenario), one surface's request supersedes the other's `_awaiting_token`, stranding it on "Loading…" forever. The plan-checker's own "Minor Observations" assumed "only one surface is navigated at a time" — Codex disputes that assumption. **Fix:** per-surface scoped request state (`request(scope, ...)` / `cancel(scope)` with per-scope timers) sharing one global token counter + one worker.
2. **HIGH — No cancellation when leaving a PDF view.** Navigating from a PDF to a non-PDF / Genizah / closed dialog does not invalidate the in-flight token, so a late success writes a stale image into the viewer. **Fix:** explicit `cancel(scope, silent=True)` on non-PDF nav, dialog close, and Browse leaving LOCAL PDF; clear `_pending`/`_awaiting_token` on every terminal state.
3. **HIGH — Duplicate render on initial ResultDialog open.** `load_result_by_index()` → `load_page()` → `load_local_page()`, so adding the trigger at the end of BOTH methods double-requests. **Fix:** trigger only from `load_local_page()` for page-state changes; have `load_result_by_index()` only cancel when the new result is not a LOCAL PDF.
4. **MEDIUM — Memory retention:** terminal states don't clear `_pending`, so the app-level controller retains a closed ResultDialog (and its callbacks/viewer) until the next request.
5. **MEDIUM — Browse guard should compare page too** (`sys_id` + `page_num`), not just `current_browse_sid`.
6. **MEDIUM — `fp.lower()` NoneType crash risk** in Plan 100-02's `is_pdf` inline check — use `controller.is_pdf(fp)` / `bool(fp)` guard.
7. **MEDIUM — unbounded FIFO worker backlog** under sustained sub-debounce navigation (page every ~200ms).
8. **LOW — `_lang()`** could import `CURRENT_LANG` from `genizah_core` rather than `genizah_app`.
9. **LOW — wave ordering** 100-01 + 100-03 both edit `genizah_app.py` (25K lines) — line-anchor drift; integrate serially with semantic anchors.

### Divergent Views
None (single reviewer). Note: Codex's #1/#2 directly contradict the internal plan-checker's "Minor Observations (no action required)" which accepted the single shared controller. This disagreement is the key decision for the user.

### Recommended next step
`/gsd-plan-phase 100 --reviews` to replan 100-01's controller as per-surface-scoped (keeping one shared worker), add `cancel()` + terminal-state cleanup, de-duplicate the ResultDialog trigger, and add the cross-surface/cancellation tests Codex named.
