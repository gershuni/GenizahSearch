---
phase: 100-local-pdf-image-in-resultdialog-browse
reviewed: 2026-05-27T00:00:00Z
depth: standard
files_reviewed: 4
files_reviewed_list:
  - desktop/pdf_image_controller.py
  - desktop/result_dialog.py
  - genizah_app.py
  - tests/test_pdf_image_controller.py
findings:
  critical: 0
  warning: 2
  info: 4
  total: 6
status: issues_found
---

# Phase 100: Code Review Report

**Reviewed:** 2026-05-27
**Depth:** standard
**Files Reviewed:** 4 (genizah_app.py scoped to the phase-100 diff; others in full)
**Status:** issues_found

## Summary

Phase 100 wires a single shared `PdfImageController` (one `PdfRenderWorker`, one
global token counter, per-scope request state) into the desktop Result Dialog and
the Browse panel to render LOCAL PDF page images. The design is well-considered:
Qt threading is respected (all fitz access stays on the render thread; signals are
queued to the UI thread), the latest-wins / per-scope token routing is correct,
the watchdog-token guard (REVIEWS-R2-1) is sound, and scope teardown is funneled
through both `finished` and `closeEvent` for idempotent `discard_scope`. The unit
tests are thorough and cover the tricky concurrency invariants directly.

No critical issues found. Two warnings concern correctness edge cases (a callback
leak window when the controller is created but `_set_browse_image_pane_visible`
fails to reset, and an unbounded-token theoretical edge). Four info items concern
minor robustness/consistency. Overall this is high-quality, defensively-written
code consistent with the project's existing patterns.

## Warnings

### WR-01: Browse non-PDF / controller-unavailable branch can leave the pane visible if controller exists but request is gated

**File:** `genizah_app.py:19249-19272` (and the symmetric block referenced in the diff)
**Issue:** The Browse render block reveals the image pane and calls
`controller.request(...)` only when `is_pdf and controller is not None and bool(filepath)`.
`controller.request()` itself re-gates on `is_pdf(filepath)` and returns `None`
when the path is not a `.pdf`. Because the outer `is_pdf` flag is computed
independently (line 19151-19154, via `_lookup_local_filepath(sys_id)`) from the
`filepath` re-looked-up at line 19237, a divergence between the two lookups (e.g.
the indexer returns a different/empty path on the second call, or a case where
`is_pdf` is `True` but `filepath` resolves empty) would call `_set_browse_image_pane_visible(True)`
and then `controller.request(...)` would return `None` (gated out) — leaving the
pane revealed with no render and no placeholder. The two `_lookup_local_filepath`
calls should share one result.
**Fix:** Compute `filepath` once and derive `is_pdf` from it so the pane-reveal
decision and the request decision can never diverge:
```python
filepath = self._lookup_local_filepath(sys_id)
is_pdf = bool(filepath) and filepath.lower().endswith('.pdf')
# ... later ...
controller = getattr(self, '_pdf_image_controller', None)
if is_pdf and controller is not None:
    self._set_browse_image_pane_visible(True)
    ...
```
Today the divergence is unlikely (both call the same helper with the same `sys_id`),
so this is a latent rather than active bug — hence Warning, not Critical.

### WR-02: Retained `on_image` / `on_placeholder` closures hold a strong reference to the dialog/app until a terminal result

**File:** `desktop/pdf_image_controller.py:227-234` and `desktop/result_dialog.py:3286-3293`
**Issue:** `request()` stores `(token, sys_id, page_num, filepath, on_image, on_placeholder)`
in `self._pending[scope]`. The ResultDialog `on_image` lambda closes over `self`
(`self.ms_viewer.display_image`). If a dialog is opened on a LOCAL PDF, a render
is in flight, and the dialog is closed, `_on_pdf_dialog_finished` → `discard_scope`
→ `cancel` pops `_pending[scope]`, releasing the closure — good. However, if the
worker has already dequeued the request and is mid-render of a large/slow PDF
page, the closure is held until the watchdog (8s) or the real result arrives and
`_clear_scope` runs. During that window the closed dialog (and its `ms_viewer`
widget tree) cannot be garbage-collected. The `_scope_for_token` lookup after
`discard_scope` will correctly find no scope and discard the late result, so there
is no stale-display or crash — only a transient retention. This is acceptable but
worth an explicit note; the watchdog bounds it to ~8s.
**Fix:** No code change strictly required (the retention is bounded and harmless).
If tighter release is desired, have `discard_scope`/`cancel` not only pop
`_pending[scope]` (already done) — which it does — so the closure is in fact
released promptly on close. Verify in a manual test that closing a dialog mid-render
does not log a `_handle_request` exception when `ms_viewer` is torn down; the
sid/scope guard prevents the callback from ever firing, so this should be clean.
Recommend adding a regression test that opens + closes a dialog while a render is
queued and asserts `ctrl._pending` is empty immediately after `discard_scope`.

## Info

### IN-01: Global token counter grows unbounded for the app session

**File:** `desktop/pdf_image_controller.py:125,144-147`
**Issue:** `self._token` is a monotonically incrementing Python `int` that never
resets. In practice Python ints are arbitrary-precision so there is no overflow,
and at human navigation rates the value stays tiny — this is purely a note, not a
defect.
**Fix:** None needed. Documented here only so a future reader does not mistake the
unbounded counter for a leak.

### IN-02: `_scope_for_token` is an O(n) linear scan over awaiting scopes

**File:** `desktop/pdf_image_controller.py:178-183`
**Issue:** Each `render_succeeded`/`render_failed` scans `_awaiting_token.items()`
to find the owning scope. With at most one "browse" scope plus a handful of open
dialogs, `n` is tiny, so this is fine. A reverse index (token→scope) would be
overkill.
**Fix:** None needed.

### IN-03: `silent` parameter of `cancel()` is effectively unused

**File:** `desktop/pdf_image_controller.py:243-267`
**Issue:** `cancel(scope, silent=True)` documents that `silent=False` would show a
blank/cleared placeholder, but the body never branches on `silent` — it always
behaves as `silent=True`. Every caller passes `silent=True`. This is dead-parameter
surface that could mislead a future caller into expecting `silent=False` to clear
the viewer.
**Fix:** Either drop the parameter until a caller needs it, or implement the
`silent=False` branch (e.g. invoke a stored `on_placeholder` with a cleared/blank
string) so the documented contract is real. Low priority.

### IN-04: Two redundant teardown paths (`finished` signal + `closeEvent`) both call `discard_scope`

**File:** `desktop/result_dialog.py:58,3236-3241,3311-3322`
**Issue:** `discard_scope` runs from both `_on_pdf_dialog_finished` (connected to
`finished`) and `closeEvent`. This is intentional and documented as idempotent,
and `discard_scope` is indeed idempotent (pop with default + guarded
`deleteLater`). No bug — noting for clarity that the double-call is by design to
cover accept/reject/done/Esc paths that `closeEvent` alone misses.
**Fix:** None needed; the redundancy is correct defensive coverage.

---

_Reviewed: 2026-05-27_
_Reviewer: Claude (gsd-code-reviewer)_
_Depth: standard_
