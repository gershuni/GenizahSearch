# -*- coding: utf-8 -*-
"""
Browse Enrichment Functions

Extracted from web/pages/browse.py (Phase 73).
Plan 01 creates this stub with BrowsePageRefs dataclass.
Plan 02 adds the three enrichment function bodies
(load_enrichment, update_enrichment_sections, populate_bib_catalog_buttons).

Each function that was a closure in create_browse_page() now takes explicit
state and refs parameters instead of capturing them via closure.

IMPORTANT: This module must NOT import from web.pages.browse to avoid
circular imports. All shared types come from web.pages.browse_state.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class BrowsePageRefs:
    """UI element references and callbacks needed by extracted browse_enrichment functions.

    Populated in create_browse_page() after all UI elements and callbacks are defined.
    These are ephemeral per-page references, NOT persistent state (per D-06).

    Population timing:
    - enrichment_refs: alias assigned at construction, populated incrementally
      during update_content() calls (lines ~4189, ~4313-4341, ~4617-4618)
    - load_generation: default {'value': 0} at construction, used as-is
    - page_client: set immediately after construction (ui.context.client)
    - content_container: set when main content area is created (~line 4210+)
    - slider_refs: alias assigned at construction, populated in update_content()
    - enter_joined_view: set after enter_joined_view() defined (~line 1450)
    - update_content: set after update_content() defined (~line 2083)

    All callbacks are set BEFORE any asyncio.ensure_future(load_page(...)) call,
    which happens at the end of create_browse_page() (~lines 4982-5041).
    """
    enrichment_refs: Dict[str, Any] = field(default_factory=dict)
    # SAFETY NOTE: load_generation MUST remain a mutable dict container (not an int).
    # Local aliases in browse.py bind to this dict object -- if changed to a primitive,
    # the alias would be a copy and stale-generation checks would silently break.
    load_generation: Dict[str, int] = field(default_factory=lambda: {'value': 0})
    page_client: Any = None
    content_container: Any = None
    slider_refs: Dict[str, Any] = field(default_factory=dict)

    # Callback functions (set after definition in create_browse_page)
    enter_joined_view: Any = None
    update_content: Any = None
