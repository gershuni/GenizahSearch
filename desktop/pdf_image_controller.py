"""Phase 100 Plan 01: PdfImageController — per-scope request state coordinator.

This module is the thin coordinator both ResultDialog (Plan 02) and Browse
(Plan 03) consume. It owns:

  - ONE global monotonic token counter (D-07b), shared across scopes so tokens
    are globally unique.
  - Per-scope request state (dicts keyed by scope string/object) so a Browse
    render in flight is NEVER superseded by a ResultDialog request (REVIEWS
    HIGH-1 option A — one controller, per-scope state).
  - Latest-wins discard of stale render_succeeded / render_failed results via
    _scope_for_token (D-03).
  - Per-scope ~150ms debounce QTimer before enqueue (D-04).
  - Per-scope ~8s watchdog QTimer that synthesizes a TIMEOUT placeholder (D-05).
  - Per-scope watchdog-token guard (_watchdog_token dict): request() stops the
    scope's existing watchdog BEFORE overwriting _awaiting_token so an old
    watchdog cannot fire against a newer token (REVIEWS-R2-1 HIGH).
  - cancel(scope, silent=True): invalidates an in-flight render for a surface
    without showing a placeholder (REVIEWS HIGH-2).
  - discard_scope(scope): cancels AND removes+deleteLater()s the scope's timer
    dict entries so per-dialog QObject entries do not accumulate (REVIEWS-R2-3).
  - None-safe .pdf extension gate (D-08 / PDFIMG-05).
  - CURRENT_LANG-from-genizah_core localized per-PdfRenderFailure placeholder
    text map (D-02 / D-03).

Scope keys:
  "browse"  — Browse panel (permanent scope; use cancel(), not discard_scope()).
  id(dialog) — per-ResultDialog integer key (transient; caller calls
                discard_scope() when the dialog closes).

Import contract: this module NEVER imports genizah_app (avoids circular import).
CURRENT_LANG is imported lazily from genizah_core inside _lang() so
controller-only tests can patch genizah_core.CURRENT_LANG without pulling in
the heavy app module.
"""

import logging
from typing import Optional

from PyQt6.QtCore import QObject, QTimer

from desktop.pdf_page_renderer import PdfRenderFailure

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level localized placeholder text map (D-02)
# Each entry: PdfRenderFailure -> (he, en)
# CANCELLED is intentionally absent — stale/superseded results are discarded
# silently without a user-visible placeholder.
# ---------------------------------------------------------------------------

_PLACEHOLDER_TEXT: dict[PdfRenderFailure, tuple[str, str]] = {
    PdfRenderFailure.MISSING_FILE: (
        "הקובץ לא נמצא",
        "File not found",
    ),
    PdfRenderFailure.NOT_PDF: (
        "לא ניתן להציג קובץ זה",
        "Cannot display this file",
    ),
    PdfRenderFailure.ENCRYPTED: (
        "ה-PDF מוגן בסיסמה",
        "PDF is password-protected",
    ),
    PdfRenderFailure.CORRUPT: (
        "לא ניתן לפתוח PDF זה",
        "Could not open this PDF",
    ),
    PdfRenderFailure.PAGE_OUT_OF_RANGE: (
        "העמוד לא נמצא בקובץ",
        "Page not found in file",
    ),
    PdfRenderFailure.RENDER_ERROR: (
        "לא ניתן להציג עמוד זה",
        "Could not display this page",
    ),
    PdfRenderFailure.TIMEOUT: (
        "זמן העיבוד הסתיים",
        "Rendering timed out",
    ),
    # PdfRenderFailure.CANCELLED intentionally absent — discarded silently.
}


class PdfImageController(QObject):
    """Per-scope request state coordinator over one shared PdfRenderWorker.

    Architecture (REVIEWS HIGH-1 Option A — one controller, per-scope state):
    ONE controller object, ONE shared worker, ONE global token counter.
    Request state is partitioned by the 'scope' argument on every call so that
    a Browse render in flight is never stranded by a ResultDialog render.

    Token routing: every token is globally unique (single shared counter), so
    the render_succeeded / render_failed slots can route each result to its
    owning scope by scanning _awaiting_token for a match. An unmatched (stale /
    cancelled / superseded) token is discarded silently.

    Per-scope watchdog-token guard (REVIEWS-R2-1): request() stops the scope's
    existing watchdog before overwriting _awaiting_token; _fire_pending records
    the armed token in _watchdog_token[scope]; _on_watchdog is a no-op unless
    that guard token still equals the scope's awaited token.

    Per-scope timer cleanup (REVIEWS-R2-3): discard_scope() cancels then
    pop+deleteLater()s the scope's QTimer dict entries. Transient dialog scopes
    (keyed by id(dialog)) must call discard_scope(); the permanent "browse"
    scope uses plain cancel().
    """

    def __init__(
        self,
        worker: object,
        *,
        debounce_ms: int = 150,
        watchdog_ms: int = 8000,
        parent: Optional[QObject] = None,
    ) -> None:
        super().__init__(parent)
        self._worker = worker
        self._debounce_ms = debounce_ms
        self._watchdog_ms = watchdog_ms

        # ONE global monotonic counter shared across all scopes (D-07b).
        # Incrementing returns the next token; tokens are globally unique.
        self._token: int = 0

        # Per-scope request state — all keyed by the same scope object/string.
        self._awaiting_token: dict[object, int] = {}
        self._pending: dict[object, tuple] = {}  # scope -> (token, sys_id, page_num, filepath, on_image, on_placeholder)  # noqa: E501
        self._debounce_timers: dict[object, QTimer] = {}
        self._watchdog_timers: dict[object, QTimer] = {}
        # REVIEWS-R2-1: per-scope guard storing which token the armed watchdog
        # belongs to so an old watchdog cannot time out a newer request.
        self._watchdog_token: dict[object, int] = {}

        # Connect ONCE to the shared worker signals.
        self._worker.render_succeeded.connect(self._on_render_succeeded)
        self._worker.render_failed.connect(self._on_render_failed)

    # ------------------------------------------------------------------
    # Token helper
    # ------------------------------------------------------------------

    def _next_token(self) -> int:
        """Increment and return the global monotonic token counter."""
        self._token += 1
        return self._token

    # ------------------------------------------------------------------
    # Per-scope timer accessors (lazily created, persistent — mirror
    # ManuscriptViewerWidget._nav_debounce_timer "persistent, not recreated").
    # ------------------------------------------------------------------

    def _debounce_timer(self, scope: object) -> QTimer:
        """Return (creating if needed) the persistent debounce QTimer for scope."""
        t = self._debounce_timers.get(scope)
        if t is None:
            t = QTimer(self)
            t.setSingleShot(True)
            t.timeout.connect(lambda s=scope: self._fire_pending(s))
            self._debounce_timers[scope] = t
        return t

    def _watchdog_timer(self, scope: object) -> QTimer:
        """Return (creating if needed) the persistent watchdog QTimer for scope."""
        t = self._watchdog_timers.get(scope)
        if t is None:
            t = QTimer(self)
            t.setSingleShot(True)
            t.timeout.connect(lambda s=scope: self._on_watchdog(s))
            self._watchdog_timers[scope] = t
        return t

    # ------------------------------------------------------------------
    # Token→scope router (REVIEWS HIGH-1 — route by echoed token)
    # ------------------------------------------------------------------

    def _scope_for_token(self, token: int) -> Optional[object]:
        """Return the scope currently awaiting this token, or None if stale/unmatched."""
        for scope, awaited in self._awaiting_token.items():
            if awaited == token:
                return scope
        return None

    # ------------------------------------------------------------------
    # Extension gate (D-08 / PDFIMG-05 — None-safe)
    # ------------------------------------------------------------------

    def is_pdf(self, filepath: object) -> bool:
        """Return True iff filepath is a non-empty path string ending in '.pdf' (case-insensitive).

        None-safe: bool(filepath) guard ensures None / empty string returns False
        without raising (REVIEWS MEDIUM-6 origin).
        """
        return bool(filepath) and str(filepath).lower().endswith(".pdf")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def request(self, scope: object, sys_id: str, page_num: int, filepath: object, on_image: object, on_placeholder: object) -> Optional[int]:  # noqa: E501
        """Debounced render request for a given surface SCOPE.

        Returns the token for this request, or None if gated out (non-PDF /
        no filepath). A later render_succeeded / render_failed carrying a
        different scope's token is discarded silently; per-scope state means a
        request in another scope never supersedes this one's awaited token
        (REVIEWS HIGH-1).

        on_image(QImage) is called on success with the scope's latest token;
        on_placeholder(str) shows a localized placeholder immediately (D-01
        loading state) and on failure / timeout.
        """
        if not self.is_pdf(filepath):
            return None

        # REVIEWS-R2-1: stop the scope's existing watchdog BEFORE overwriting
        # _awaiting_token so a stale watchdog armed for the PRIOR token cannot
        # fire against this new token.
        w = self._watchdog_timers.get(scope)
        if w is not None:
            w.stop()
        self._watchdog_token.pop(scope, None)

        token = self._next_token()
        self._awaiting_token[scope] = token
        self._pending[scope] = (
            token,
            sys_id,
            int(page_num),
            str(filepath),
            on_image,
            on_placeholder,
        )

        # D-01: show Loading immediately, before debounce/enqueue.
        on_placeholder(self._loading_text())

        # Restart (or start) the persistent per-scope debounce timer (D-04).
        self._debounce_timer(scope).start(self._debounce_ms)
        return token

    def cancel(self, scope: object, silent: bool = True) -> None:
        """Invalidate any in-flight render for SCOPE (REVIEWS HIGH-2).

        Clears the scope's pending state + awaited token + watchdog-token guard,
        stops the scope's debounce + watchdog timers. silent=True shows no
        placeholder — use this when navigating off a PDF or closing a dialog
        so a late render_succeeded cannot write a stale image into the viewer.

        A worker result that arrives after cancel() will find no matching scope
        via _scope_for_token and will be discarded silently (D-03 latest-wins).
        """
        self._pending.pop(scope, None)
        self._awaiting_token.pop(scope, None)
        self._watchdog_token.pop(scope, None)

        t = self._debounce_timers.get(scope)
        if t is not None:
            t.stop()

        w = self._watchdog_timers.get(scope)
        if w is not None:
            w.stop()

        # silent=True: deliberately show nothing (param reserved for future
        # callers that want an explicit blank/cleared state).

    def discard_scope(self, scope: object) -> None:
        """Fully tear down a TRANSIENT scope (e.g. a closed ResultDialog keyed by id()).

        Cancels any in-flight render, then removes AND deleteLater()s the scope's
        debounce and watchdog QTimer dict entries so they do not accumulate for
        the app session (REVIEWS-R2-3).

        Idempotent — safe to call when the scope is already absent.

        Browse's permanent "browse" scope uses cancel(), NOT discard_scope().
        """
        self.cancel(scope, silent=True)
        for timers in (self._debounce_timers, self._watchdog_timers):
            t = timers.pop(scope, None)
            if t is not None:
                t.deleteLater()
        # _watchdog_token already popped by cancel(); pop again is a no-op
        self._watchdog_token.pop(scope, None)

    # ------------------------------------------------------------------
    # Internal: debounce fires → enqueue + arm watchdog
    # ------------------------------------------------------------------

    def _fire_pending(self, scope: object) -> None:
        """Called when the scope's debounce timer fires. Enqueues the pending render.

        If the pending request was superseded between .start() and .timeout
        (REVIEWS MEDIUM-7 sub-debounce coalescing), the token check here
        discards the stale entry without enqueueing.
        """
        pending = self._pending.get(scope)
        if pending is None:
            return
        token, sys_id, page_num, filepath, _on_image, _on_placeholder = pending
        if token != self._awaiting_token.get(scope):
            # Superseded during debounce window — discard silently.
            return

        self._worker.enqueue(token, sys_id, page_num, filepath)

        # REVIEWS-R2-1: record the guard token for the watchdog we are arming.
        # _on_watchdog will no-op if this guard no longer equals _awaiting_token.
        self._watchdog_token[scope] = token
        self._watchdog_timer(scope).start(self._watchdog_ms)

    # ------------------------------------------------------------------
    # Worker signal handlers (called on main/UI thread via queued signal)
    # ------------------------------------------------------------------

    def _on_render_succeeded(
        self, token: int, sys_id: str, page_num: int, image: object
    ) -> None:
        """Route a successful render result to the owning scope (D-03 latest-wins).

        If the token matches no scope's awaited token (stale/cancelled/
        superseded), the result is discarded silently without any UI change.
        Terminal cleanup (REVIEWS MEDIUM-4) runs BEFORE invoking the callback
        so a re-entrant request inside the callback starts with clean state.
        """
        scope = self._scope_for_token(token)
        if scope is None:
            return  # stale / cancelled / superseded — discard silently

        pending = self._pending.get(scope)
        on_image = pending[4] if pending is not None else None

        # Terminal cleanup BEFORE callback (re-entrancy safety — MEDIUM-4).
        self._clear_scope(scope)

        if on_image is not None:
            on_image(image)

    def _on_render_failed(
        self,
        token: int,
        sys_id: str,
        page_num: int,
        reason: object,
        detail: str,
    ) -> None:
        """Route a failure to the owning scope (D-03 latest-wins).

        Maps the PdfRenderFailure reason to a localized placeholder string via
        _placeholder_for. If the reason maps to None (CANCELLED — discarded
        silently) no placeholder is shown. Terminal cleanup (MEDIUM-4) always
        runs even when the reason is CANCELLED so the scope state is released.
        """
        scope = self._scope_for_token(token)
        if scope is None:
            return  # stale / cancelled / superseded

        pending = self._pending.get(scope)
        on_placeholder = pending[5] if pending is not None else None

        logger.warning(
            "PDF render failed scope=%s token=%s sys_id=%s page=%s reason=%s detail=%s",
            scope,
            token,
            sys_id,
            page_num,
            getattr(reason, "value", reason),
            detail,
        )

        # Terminal cleanup BEFORE callback (re-entrancy safety — MEDIUM-4).
        self._clear_scope(scope)

        text = self._placeholder_for(reason)
        if text is not None and on_placeholder is not None:
            on_placeholder(text)

    # ------------------------------------------------------------------
    # Watchdog timer handler
    # ------------------------------------------------------------------

    def _on_watchdog(self, scope: object) -> None:
        """Called when the scope's watchdog fires.

        REVIEWS-R2-1 guard: only acts if _watchdog_token[scope] still equals
        the scope's current _awaiting_token. If a newer request replaced the
        awaited token after this watchdog was armed, this is a no-op — the
        new request is NOT stranded on a false TIMEOUT.
        """
        token = self._awaiting_token.get(scope)
        if token is None:
            return  # scope already cleared (cancel / success / failure)

        # REVIEWS-R2-1: the watchdog only fires for the token it was armed for.
        if self._watchdog_token.get(scope) != token:
            return  # stale watchdog — a newer request replaced _awaiting_token

        logger.warning(
            "PDF render watchdog TIMEOUT scope=%s token=%s", scope, token
        )

        pending = self._pending.get(scope)
        on_placeholder = pending[5] if pending is not None else None

        # Terminal cleanup: clear the scope's state so a late real result
        # finds no matching scope in _scope_for_token and is discarded (MEDIUM-4).
        self._clear_scope(scope)

        text = self._placeholder_for(PdfRenderFailure.TIMEOUT)
        if text is not None and on_placeholder is not None:
            on_placeholder(text)

    # ------------------------------------------------------------------
    # Terminal-state helper (REVIEWS MEDIUM-4)
    # ------------------------------------------------------------------

    def _clear_scope(self, scope: object) -> None:
        """Clear all per-scope state for a terminal result (success/failure/timeout/cancel).

        Pops _pending, _awaiting_token, _watchdog_token (REVIEWS-R2-1 so the
        guard does not leak across terminal states), and stops the scope's
        watchdog timer. Does NOT remove timer dict entries (that is discard_scope).
        """
        self._pending.pop(scope, None)
        self._awaiting_token.pop(scope, None)
        self._watchdog_token.pop(scope, None)

        w = self._watchdog_timers.get(scope)
        if w is not None:
            w.stop()

    # ------------------------------------------------------------------
    # Localized text helpers (D-02 / D-03)
    # ------------------------------------------------------------------

    def _placeholder_for(self, reason: object) -> Optional[str]:
        """Map a PdfRenderFailure to a localized placeholder string, or None.

        Returns None for PdfRenderFailure.CANCELLED (silently discarded) and
        for any unrecognized reason.
        """
        pair = _PLACEHOLDER_TEXT.get(reason)  # type: ignore[call-overload]
        if pair is None:
            return None
        return pair[0] if self._lang() == "he" else pair[1]

    def _loading_text(self) -> str:
        """Return the localized "Loading…" status text (D-01)."""
        return "טוען…" if self._lang() == "he" else "Loading…"

    def _lang(self) -> str:
        """Return the current UI language ('he' or 'en').

        Imported lazily from genizah_core so controller-only tests can patch
        genizah_core.CURRENT_LANG without importing the heavy app module
        (REVIEWS LOW-8). Falls back to 'en' on any import error.
        """
        try:
            from genizah_core import CURRENT_LANG  # noqa: PLC0415

            return CURRENT_LANG
        except Exception:  # noqa: BLE001 — graceful fallback
            return "en"
