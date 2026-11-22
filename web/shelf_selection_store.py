# -*- coding: utf-8 -*-
"""Where the website keeps a reader's pending choice on the shelf (D12, handoff DX4).

One selection per purpose (``search_exclude``, ``reading_desk`` ...), kept in ``app.storage.user``
through ``web/safe_storage.py`` until it is applied or cleared, so closing the picker or reloading
the page does not lose it. ``app.storage.user`` belongs to the browser (shared by its tabs), not to
an account across devices, and the site forgets an idle browser's storage after a while.

The key carries the account the choice was made under (``anon`` for a visitor): an account switch
in the same browser never shows another account's set. A picker binds itself to the account it
was opened under (``current_owner()`` at open) and passes it to every read and write, so a switch
in another tab while it is open cannot make it write one account's set under another's key.

It is apart from what a host has applied (``search_exclusion_sources``, ``reading_desk_state``).
What is kept is ``Selection.to_dict()`` (versioned JSON, chips only, never a node's manuscripts);
a stale or broken value reads as empty. Two pickers open for one purpose: the last write wins.
Call these on the UI thread (they read the request's storage).
"""
from __future__ import annotations

import re

from shared.shelfmark_tree_selection import Selection
from web.auth_state import GlobalAuthState
from web.safe_storage import safe_user_get, safe_user_remove, safe_user_set

_PURPOSE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")


def current_owner() -> str:
    """The account a choice is kept under: the signed-in user's id, else ``anon``."""
    return str(GlobalAuthState.get_user_id() or "anon")


def _key(purpose: str, owner: str) -> str:
    if not _PURPOSE.fullmatch(purpose or ""):
        raise ValueError(f"not a selection purpose: {purpose!r}")
    return f"shelf_selection:{purpose}:{owner}"


def load_selection(purpose: str, owner: str) -> Selection:
    return Selection.from_dict(safe_user_get(_key(purpose, owner)))


def save_selection(purpose: str, owner: str, selection: Selection) -> bool:
    """Keep it (an empty selection is cleared); False when the browser's storage refused it --
    the write, or the clearing (a cleared choice that stayed would come back on reopen)."""
    if not len(selection):
        return clear_selection(purpose, owner)
    return safe_user_set(_key(purpose, owner), selection.to_dict())


def clear_selection(purpose: str, owner: str) -> bool:
    """True when no choice is kept for the purpose afterwards; False when the storage refused."""
    return safe_user_remove(_key(purpose, owner))
