# -*- coding: utf-8 -*-
"""Whether the website serves the catalogue text search (docs/plans/CATALOGUE_TEXT_SEARCH_PLAN.md
§4.5), and its startup hook.

Fail-closed, per decision 0004: the ``CATALOG_TEXT_SEARCH_ENABLED`` flag AND a sidecar that
``shared/catalog_text_service.py`` opened and checked. The file is opened once, at startup, off
the event loop (``open_catalog_text_on_startup``); until then, and whenever the flag is off or
the file was refused, ``catalog_available()`` is False and no page offers the search: no
Catalogue mode, no ``%`` shortcut, no offer under an empty text search, and the public API
refuses ``search_mode='catalog'``.
"""
from __future__ import annotations

# The switches' tooltips are shared with the desktop (shared/catalog_text_rows.py).
from shared.catalog_text_rows import CATALOG_NEAR_TOOLTIP, CATALOG_TRANSLATIONS_TOOLTIP  # noqa: E402,F401

#: How many of the near spellings a search added the line under the count names.
CATALOG_NEAR_FORMS_SHOWN = 8
#: The search page's modes that show the Fuzzy search (near spellings) switch (plan §4.7). Title
#: mode has it whether or not the catalogue text search is served: it reads the titles only.
NEAR_SPELLINGS_MODES = frozenset({'Catalog', 'Title'})


def catalog_available() -> bool:
    """The flag is on and the sidecar is open and checked. Never opens the file."""
    from shared.catalog_text_service import catalog_text_enabled, peek_catalog_text_service
    if not catalog_text_enabled():
        return False
    service = peek_catalog_text_service()
    return service is not None and service.is_available()


async def open_catalog_text_on_startup() -> None:
    """Open and check the catalogue text sidecar once, off the loop, when the flag is on; log why
    it is not served, or how its inputs drifted from the installed FJMS and PGP sidecars (plan
    §4.6). Nothing here can stop the app."""
    from shared.catalog_text_service import catalog_text_enabled, open_and_check
    if not catalog_text_enabled():
        return
    from nicegui import run
    try:
        service = await run.io_bound(open_and_check)
        if service is not None and not service.is_ready():
            print(f"[init] Catalogue text search not served: {service.unavailable_reason}", flush=True)
        elif service is not None:
            print(f"[init] Catalogue text search served from {service.db_path} "
                  f"({service.meta('rows_total')} records).", flush=True)
    except Exception as e:  # noqa: BLE001 -- startup never fails on this file
        print(f"[init] Catalogue text sidecar check failed (non-fatal): {e}", flush=True)
