# -*- coding: utf-8 -*-
"""WebSearchExecutor — adapter satisfying the shared/joins_lab.py SearchExecutor Protocol.

Wraps ``state.searcher.execute_search`` / ``state.searcher.get_browse_page`` /
``state.meta_mgr.get_meta_for_id`` / ``state.meta_mgr.get_library_for_id`` directly — NOT
through any HTTP endpoint. Using the HTTP search endpoint would drop ``text_position`` /
``corpus_scope`` and cap modes (Codex BLOCKER 1); the engine is wired directly instead.

Mirrors the desktop ``_DesktopSearchExecutor`` (``desktop/join_workbench.py:1473-1538``) as a
thin passthrough. The web variant reads ``state`` at call time (no ``__init__`` wiring) because
the NiceGUI web app manages a single ``AppState`` singleton (``web/state.py``) that is ready by
the time request handlers run.

IMPORTANT: All ``execute_search`` calls MUST be made inside ``run.io_bound(...)`` by the async
caller (e.g. ``web/pages/joins_lab.py``) and NEVER directly on the NiceGUI event loop. The
``tests/test_joins_lab_off_loop.py`` CI test statically enforces this (SC#3 / MEDIUM-4 from
117-REVIEWS.md).
"""

from shared.joins_lab import SearchExecutor  # noqa: F401 — referenced in docstring/isinstance
from web.state import state


class WebSearchExecutor:
    """Concrete adapter satisfying the Phase-106 SearchExecutor Protocol for the web app.

    Thin passthrough — no per-app normalizer (Phase-106 D-01).

    The class has no ``__init__``; it reads ``state.searcher`` and ``state.meta_mgr``
    at each call site so that callers do not need to construct or inject the searcher —
    they just instantiate ``WebSearchExecutor()`` once the app is ready (``state.is_ready()``
    is True).

    All four ``execute_search`` calls MUST be dispatched via ``await run.io_bound(fn)``
    by the async NiceGUI page handler; never call this class's methods directly inside an
    ``async def`` handler.
    """

    def execute_search(
        self,
        query_str: str,
        mode: str,
        gap: int,
        progress_callback=None,
        exclude_words=None,
        responsa_options: "dict | None" = None,
        restrict_sys_ids: "set | None" = None,
        text_position: "str | None" = None,
        corpus_scope: str = "all",
    ) -> list:
        """Forward to state.searcher.execute_search; return [] on any failure.

        NOTE on cooperative cancellation (Plan 04): the adapter does NOT handle
        InterruptedError specially.  ``SearchEngine.execute_search`` CATCHES it
        internally (``genizah_core.py:9000``), aborts the scan loop early (freeing
        the ``run.io_bound`` worker), and RETURNS the partial deduped results gathered
        so far (``genizah_core.py:9005/:9071``).  Plan 04 relies on its stale-generation
        guard (``_should_apply_results``) to DISCARD those partial results — not on this
        adapter re-raising.  Keep the plain ``except Exception: return []``.
        """
        try:
            return state.searcher.execute_search(
                query_str,
                mode,
                gap,
                progress_callback=progress_callback,
                exclude_words=exclude_words,
                responsa_options=responsa_options,
                restrict_sys_ids=restrict_sys_ids,
                text_position=text_position,
                corpus_scope=corpus_scope,
            ) or []
        except Exception:
            return []

    def get_browse_page(
        self,
        sys_id: str,
        p_num: "int | None" = None,
        next_prev: int = 0,
        absolute_index: "int | None" = None,
        allow_cross: bool = False,
        volume_ie: "str | None" = None,
    ) -> "dict | None":
        """Forward to state.searcher.get_browse_page; return None on any failure.

        Returns the NARROW dict produced by ``SearchEngine.get_browse_page`` (keys:
        ``uid``, ``p_num``, ``full_header``, ``text``, ``total_pages``,
        ``current_idx``, ``internal_index``, ``sys_id``, ``volume_ie``).

        HIGH-1: Do NOT enrich this result with provider-aware image data here.
        AnchorViewer (Plan 06) uses a SEPARATE rich resolver
        (``web.services.service.get_browse_page()``) for images.  This method
        stays narrow.
        """
        try:
            return state.searcher.get_browse_page(
                sys_id,
                p_num=p_num,
                next_prev=next_prev,
                absolute_index=absolute_index,
                allow_cross=allow_cross,
                volume_ie=volume_ie,
            )
        except Exception:
            return None

    def get_meta_for_id(self, sys_id: str) -> "tuple[str, str]":
        """Forward to state.meta_mgr.get_meta_for_id; return ('', '') on any failure.

        Returns (shelfmark, title).
        """
        try:
            return state.meta_mgr.get_meta_for_id(sys_id)
        except Exception:
            return ("", "")

    def get_library_for_id(self, sys_id: str) -> str:
        """Forward to state.meta_mgr.get_library_for_id; return '' on any failure.

        Returns library_code string ('CUL', 'JTS', ...) or ''.
        """
        try:
            return state.meta_mgr.get_library_for_id(sys_id) or ""
        except Exception:
            return ""
