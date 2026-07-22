# -*- coding: utf-8 -*-
"""Neutral, web-free exception types for the Discovery Data Spine (Phase 134,
DATA-06 async chokepoint).

Modeled 1:1 on ``shared/api_errors.py``: these types live in ``shared/`` and
deliberately import NOTHING from the web layer or any web framework (nicegui,
fastapi, starlette) -- so ``web/pages/*`` and ``web/discovery.py`` can catch
``DiscoveryUnavailable`` without ``shared/discovery_service.py`` ever
depending on ``web/`` (the shared/->web back-edge convention). This is proven
by the NEW essential AST guard ``tests/test_no_back_edges_discovery.py``.
"""

from __future__ import annotations


class DiscoveryUnavailable(Exception):
    """Raised when a ``DiscoveryService`` read cannot complete right now:
    the sidecar is unavailable, a per-query timeout elapsed, or a heavy-query
    concurrency slot could not be acquired. Callers should render a
    "temporarily unavailable" message -- never let a traceback escape to the
    user (T-134-failopen)."""


class DiscoveryOverload(DiscoveryUnavailable):
    """Raised specifically when the bounded heavy-query concurrency
    semaphore is full (a non-blocking acquire attempt failed). A subclass of
    ``DiscoveryUnavailable`` so a caller that only catches the base type
    still handles this case; a caller that wants to distinguish "overload"
    from a plain query timeout may catch this subclass specifically."""
