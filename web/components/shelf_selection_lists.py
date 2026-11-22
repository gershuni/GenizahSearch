# -*- coding: utf-8 -*-
"""Save a choice made on the shelf as one of the reader's lists (D12: "savable as a list").

One adapter for every host of the picker's multiple-choice mode (Exclude now, the Lists page and
the Reading Desk later): a new list, then its items in one insert, each write through
``web/components/lists_write.py::run_lists_write`` (signed in, awaited, its result checked).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from nicegui import ui

from web.components.lists_write import run_lists_write
from web.translations import tr


@dataclass(frozen=True)
class SavedList:
    list_id: str
    added: int
    wanted: int

    @property
    def complete(self) -> bool:
        return self.added == self.wanted


async def save_selection_as_list(lists_mgr, name: str, sys_ids: Iterable[str],
                                 list_id: Optional[str] = None) -> Optional[SavedList]:
    """Create the list (or fill ``list_id``, one made by an earlier try whose items were not added)
    and add the manuscripts in one insert. None when the list could not be created; a
    ``SavedList`` whose ``complete`` is False when it was created but its items were not all added
    (the caller keeps the choice and offers to try again into the same list, not a new empty one).
    ``run_lists_write`` has told the reader about any failed write.

    A save into an existing ``list_id`` (a retry) reads the list first and inserts only what it does
    not hold yet: the earlier insert may have been made although its answer was lost, and
    ``list_items`` has no unique key, so inserting everything again would put each manuscript in
    twice. A list that cannot be read whole takes nothing, and the reader is told."""
    ids = [s for s in dict.fromkeys(sys_ids) if s]
    name = (name or "").strip()
    if lists_mgr is None or not ids or not (name or list_id):
        return None
    wanted, held_already = ids, 0
    if not list_id:
        made = await run_lists_write(lambda: lists_mgr.create_list(name))
        if not made:
            return None
        list_id = str(made)
    else:
        held = await _held_in(lists_mgr, str(list_id))
        if held is None:
            ui.notify(tr('The change could not be saved. Check your connection and try again.'), type='negative')
            return SavedList(str(list_id), 0, len(ids))
        wanted = [s for s in ids if s not in held]
        held_already = len(ids) - len(wanted)
    added = await run_lists_write(lambda: lists_mgr.add_items(wanted, str(list_id))) if wanted else 0
    return SavedList(str(list_id), held_already + int(added or 0), len(ids))


async def _held_in(lists_mgr, list_id: str) -> Optional[set]:
    """The sys_ids a list holds, read whole (``UserListsManager.sys_ids_in_list``), or None when
    they could not be read."""
    read = getattr(lists_mgr, 'sys_ids_in_list', None)
    if read is None:
        return None
    try:
        return await read(list_id)
    except Exception:
        return None
