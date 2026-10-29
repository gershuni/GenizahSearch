# -*- coding: utf-8 -*-
"""The page count of a manuscript the index holds no pages of: one without any automatic transcription.

The index gives such a page ``total_pages`` 0 (``SearchEngine._get_metadata_only_browse_page``), so Browse and
Quick View showed "Page 1/0" and could not page on. Its pages are its NLI images. Counting them may need the NLI
manifest, so neither view waits for that: :func:`count_pages` answers at once from what is known locally or
cached, and otherwise lets the page render without a total and reports the count when it comes (Codex on #454).
"""
from __future__ import annotations

import asyncio
import logging
from typing import Callable, Dict, List, Optional

from nicegui import background_tasks, run

from shared.synthetic_sys_id import is_synthetic_sys_id

logger = logging.getLogger(__name__)

#: How long a view waits for the count before rendering without it. A count from the crossref or the FL-id cache
#: comes well inside it; only a manifest fetch is slower.
PAGE_COUNT_WAIT_S = 0.5


def manuscript_image_count(sys_id: str, folio_images: Optional[List[Dict]] = None) -> int:
    """How many pages (images) a manuscript the index holds none of has.

    The NLI crossref folio list (local; ``folio_images`` when the caller has it), else the FL-id list the image
    route pages through (``web.api.nli_fl_ids``, which may fetch the manifest). 0 when neither is known, or for a
    synthetic record (no NLI images); callers then show no total. BLOCKING: call it off the event loop.
    """
    if not sys_id or is_synthetic_sys_id(sys_id):
        return 0
    if folio_images is None:
        try:
            from shared.nli_crossref_service import get_nli_crossref_service
            crossref = get_nli_crossref_service(thread_safe=True)
            folio_images = crossref.get_folio_images(sys_id) if crossref.is_available() else []
        except Exception as e:
            logger.debug("NLI crossref folios unavailable for %s: %s", sys_id, e)
            folio_images = []
    if folio_images:
        return len(folio_images)
    try:
        from web.api import nli_fl_ids
        return len(nli_fl_ids(sys_id))
    except Exception as e:
        logger.debug("NLI image list unavailable for %s: %s", sys_id, e)
        return 0


def apply_page_count(page, count: int) -> bool:
    """Give ``page`` (a ``BrowsePage`` that came with no count) its count, its page number kept within it.

    False, and nothing changed, when the count is unknown (0) or the page already has one."""
    if not count or page is None or page.total_pages:
        return False
    page.total_pages = count
    page.p_num = page.current_idx = min(max(page.p_num or 1, 1), count)
    return True


async def count_pages(sys_id: str, folio_images: Optional[List[Dict]] = None, *,
                      on_late: Callable[[int], None]) -> Optional[int]:
    """The count, if it comes within :data:`PAGE_COUNT_WAIT_S` (0 when it is unknown).

    Otherwise None at once, so the caller renders the page without a total, and ``on_late(count)`` is called when
    the count comes; the callback checks that the page it is for is still the one shown.
    """
    task = asyncio.ensure_future(run.io_bound(manuscript_image_count, sys_id, folio_images))
    try:
        return await asyncio.wait_for(asyncio.shield(task), PAGE_COUNT_WAIT_S) or 0
    except asyncio.TimeoutError:
        async def _late():
            count = await task  # None when the app is stopping
            if count:
                on_late(count)

        background_tasks.create(_late(), name=f'page_count:{sys_id}')
        return None
