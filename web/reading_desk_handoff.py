# -*- coding: utf-8 -*-
"""Several manuscripts handed to Browse's Reading Desk from another page: the shelf picker on the
home page and in the header, with Browse's switch "Open more than one", whose confirm "Open them in
the Reading Desk" opens them as Browse's own picker does.

The list is kept once in this browser's user storage (``reading_desk_handoff``) and the page goes to
Browse at the first manuscript (``/browse?sys_id=<first>&place=<its place>&desk=shelf``). Browse takes
the list there (``take_reading_desk_handoff``): the first opens and the others begin a desk with it
(``web/pages/browse.py::_browse_open_chosen``). Without the list (a reload, another tab took it, too
old) the address still opens the first manuscript. The picker's tick limit is the one Browse's picker
has (``choice_limit``: 30 manuscripts, 1,000 pages), so a new desk holds everything ticked.
"""
from __future__ import annotations

import time
from typing import Optional

from nicegui import ui

from shared.shelfmark_tree_limits import desk_pages_function, limit_for
from web.components.shelf_pick import browse_url_for_pick, corpus_selection
from web.safe_storage import safe_user_pop, safe_user_set
from web.translations import tr

HANDOFF_KEY = 'reading_desk_handoff'
#: How long a list waits for Browse to take it (seconds): the page is opened right after it is kept.
HANDOFF_MAX_AGE = 600
#: At most this many are kept (the desk takes what fits its own limit).
HANDOFF_MAX = 100
#: The address's mark that Browse is to take a list.
DESK_PARAM = 'desk=shelf'


def choice_limit():
    """What may be ticked at all, as in Browse's picker: 30 manuscripts and 1,000 pages as the desk
    draws them (``web/services.py::page_count``); nothing a desk holds is counted (a new one is begun)."""
    from web.services import get_service
    return limit_for('reading_desk', pages_of=desk_pages_function(get_service().page_count))


def stage_for_reading_desk(entries: list) -> Optional[str]:
    """Keep ``entries`` (the picker's ``{sys_id, shelfmark, placement_id}``) for Browse; the address
    to go to, or None when there is nothing to keep or the storage refused it."""
    chosen = []
    for entry in entries or ():
        picked = corpus_selection(entry)
        if picked is not None and all(c['sys_id'] != picked['sys_id'] for c in chosen):
            chosen.append(picked)
    chosen = chosen[:HANDOFF_MAX]
    if not chosen:
        return None
    if not safe_user_set(HANDOFF_KEY, {'schema_version': 1, 'entries': chosen, 'at': time.time()}):
        return None
    return f"{browse_url_for_pick(chosen[0])}&{DESK_PARAM}"


def take_reading_desk_handoff(first_sys_id: Optional[str]) -> list:
    """The list kept for Browse, taken once, when it begins with ``first_sys_id`` (the manuscript the
    address names) and is fresh; else nothing (and a kept list is dropped all the same)."""
    kept = safe_user_pop(HANDOFF_KEY, None)
    if not first_sys_id or not isinstance(kept, dict):
        return []
    at = kept.get('at')
    if not isinstance(at, (int, float)) or not 0 <= time.time() - at <= HANDOFF_MAX_AGE:
        return []
    chosen = [c for c in (corpus_selection(e) for e in kept.get('entries') or ()) if c is not None]
    if not chosen or chosen[0]['sys_id'] != first_sys_id:
        return []
    return chosen[:HANDOFF_MAX]


def open_in_reading_desk(entries: list, _label: str = '') -> list:
    """The home page's and the header's "Open" of several (to the Reading Desk): the list kept, then Browse.
    Answers the ids handed over (none, and nothing opened, when the storage refused)."""
    url = stage_for_reading_desk(entries)
    if url is None:
        ui.notify(tr('The change could not be saved. Check your connection and try again.'), type='negative')
        return []
    ui.navigate.to(url)
    return [c['sys_id'] for c in (corpus_selection(e) for e in entries or ()) if c is not None][:HANDOFF_MAX]
