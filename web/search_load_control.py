"""Small process-wide budgets for the public search UI."""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager


UI_SEARCH_CONCURRENCY = 3
ENRICHMENT_BATCH_CONCURRENCY = 2

_ui_search_slots = asyncio.Semaphore(UI_SEARCH_CONCURRENCY)
_enrichment_batch_slots = asyncio.Semaphore(ENRICHMENT_BATCH_CONCURRENCY)


async def try_acquire_ui_search_slot() -> Callable[[], None] | None:
    """Return a release callback, or ``None`` when all core-search slots are busy."""
    if _ui_search_slots.locked():
        return None
    await _ui_search_slots.acquire()
    return _ui_search_slots.release


@asynccontextmanager
async def enrichment_batch_slot():
    """Limit concurrent non-visible-result enrichment batches process-wide."""
    await _enrichment_batch_slots.acquire()
    try:
        yield
    finally:
        _enrichment_batch_slots.release()
