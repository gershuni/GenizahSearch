# -*- coding: utf-8 -*-
"""The genizahsearch.com address of a manuscript page, for the desktop app.

The desktop's "Open on GenizahSearch.com" and "Copy link" buttons (Manuscript
Viewer and Browse tab, 2026-09-24) must point at the SAME page the reader has on
screen. The website's own locator is ``/browse?sys_id=...&page=...&volume_ie=...``
-- ``web/pages/browse.py::_citation_page_url`` and ``_update_browser_url`` build
exactly these parameters, from the same shared browse map the desktop reads, so
this module repeats that rule rather than inventing a second one.

* A LOCAL ("My Library", ``97``) document is the reader's own file: it is not on
  the website, so there is no link (``None``).
* A synthetic (metadata-only) record has a page on the website but no
  transcription pages, so its link carries only the ``sys_id``.
* ``page`` is added when it is a positive page number, ``volume_ie`` when the
  manuscript is viewed by volume -- the two parameters the website itself puts
  in its address bar.
"""
from __future__ import annotations

from typing import Any, Optional
from urllib.parse import urlencode

from shared.export_utils import GENIZAHSEARCH_URL
from shared.local_sys_id import is_local_sys_id
from shared.synthetic_sys_id import is_synthetic_sys_id


def web_browse_url(sys_id: Any, page: Any = None, volume_ie: Any = None) -> Optional[str]:
    """``https://genizahsearch.com/browse?...`` for this page, or None if it has none."""
    sid = str(sys_id or "").strip()
    if not sid or is_local_sys_id(sid):
        return None
    params = {"sys_id": sid}
    if not is_synthetic_sys_id(sid):
        try:
            p = int(page)
        except (TypeError, ValueError):
            p = 0
        if p > 0:
            params["page"] = p
        if volume_ie:
            params["volume_ie"] = str(volume_ie)
    return f"{GENIZAHSEARCH_URL}/browse?{urlencode(params)}"
