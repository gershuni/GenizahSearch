# -*- coding: utf-8 -*-
"""Whether the website offers "Pick from the shelf" and the shelf order (shelf tree picker plan,
§3.6), and its startup hook.

Fail-closed like the other sidecars: the ``SHELFMARK_TREE_ENABLED`` switch (on unless set off)
AND a shelf tree file that ``shared/shelfmark_tree_service.py`` opened and checked. The file is
opened once, at startup, off the event loop; until then, and whenever it is missing or refused,
``shelf_tree_available()`` is False: no picker button, no place on the shelf, and Browse's
previous / next keep today's order.
"""
from __future__ import annotations

from typing import Optional


def shelf_tree_service():
    """The opened, checked service when the website serves the shelf tree, else None. Never opens
    the file."""
    from shared.shelfmark_tree_service import peek_shelfmark_tree_service, shelfmark_tree_enabled
    if not shelfmark_tree_enabled():
        return None
    service = peek_shelfmark_tree_service()
    return service if service is not None and service.is_ready() else None


def shelf_tree_available() -> bool:
    return shelf_tree_service() is not None


async def open_shelf_tree_on_startup() -> Optional[object]:
    """Open and check the shelf tree once, off the loop, when the switch is on. Nothing here can
    stop the app."""
    from shared.shelfmark_tree_service import get_shelfmark_tree_service, shelfmark_tree_enabled
    if not shelfmark_tree_enabled():
        return None
    from nicegui import run
    try:
        service = await run.io_bound(get_shelfmark_tree_service)
        if not service.is_ready():
            print(f"[init] Shelf tree not served: {service.unavailable_reason}", flush=True)
        else:
            print(f"[init] Shelf tree served from {service.db_path} "
                  f"({service.meta('manuscripts_total')} manuscripts).", flush=True)
        return service
    except Exception as e:  # noqa: BLE001 -- startup never fails on this file
        print(f"[init] Shelf tree check failed (non-fatal): {e}", flush=True)
        return None
