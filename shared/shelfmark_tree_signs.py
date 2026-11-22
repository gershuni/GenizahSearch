# -*- coding: utf-8 -*-
"""The signs on both apps' shelf picker rows: which manuscripts the Princeton Geniza Project knows,
which have a scholarly transcription, which are joined to another, so a reader sees them on the
shelf before clicking.

Each sign is one corpus-wide set of sys_ids, read once a process from the local sidecars by the same
rules the details pane uses (``shared/shelfmark_tree_details.py``), so a row's sign and its pane
never disagree:

- ``pgp``: a PGP document names the fragment (the green PGP badge of the search results);
- ``edition``: a scholarly transcription, PGP's editions or FGP's digital editions (the catalogue's
  editions filter; FGP only while its switch is on);
- ``join``: a PGP document of several fragments or an FJMS join group with another manuscript.
  The joins readers made live in the community database and are not signed: the pane names them.

``shelf_signs()`` is the process's one ``ShelfSigns``. ``load()`` blocks (a few SQLite reads, well
under a second): call it off the UI thread / event loop, once, when a picker opens; rows drawn before
it is ready carry no sign. A source that fails or is missing leaves its sign off every row.

Pure: no Qt, no NiceGUI.
"""
from __future__ import annotations

import logging
import threading
from typing import Callable, Iterable, Mapping, Optional

logger = logging.getLogger(__name__)

#: The signs, in the order a row shows them.
SIGNS = ('pgp', 'edition', 'join')
#: What each sign is called (English; each app translates).
SIGN_LABELS = {'pgp': 'PGP', 'edition': 'Scholarly transcription', 'join': 'Joins'}


def _pgp() -> Iterable[str]:
    from shared.document_service import get_all_pgp_link_sys_ids
    return get_all_pgp_link_sys_ids()


def _editions() -> Iterable[str]:
    from shared.document_service import get_sys_ids_with_editions
    from shared.fgp_service import get_sys_ids_with_fgp_editions   # honours the FGP switch
    return set(get_sys_ids_with_editions(None)) | set(get_sys_ids_with_fgp_editions(None))


def _joins() -> Iterable[str]:
    from shared.document_service import get_all_joined_sys_ids
    out = set(get_all_joined_sys_ids())
    from shared.fjms_service import get_fjms_service
    fjms = get_fjms_service(thread_safe=True)
    if fjms.is_available():
        out |= fjms.get_all_joined_sys_ids()
    return out


#: Where each sign's set comes from.
LOADERS: Mapping[str, Callable[[], Iterable[str]]] = {'pgp': _pgp, 'edition': _editions, 'join': _joins}


class ShelfSigns:
    """The signs of every manuscript, once loaded. ``loaders``: a sign -> a function giving its
    sys_ids (``LOADERS`` by default)."""

    def __init__(self, loaders: Optional[Mapping[str, Callable[[], Iterable[str]]]] = None):
        self._loaders = dict(LOADERS if loaders is None else loaders)
        self._sets: dict = {}
        self._lock = threading.Lock()
        self._ready = False

    @property
    def ready(self) -> bool:
        return self._ready

    def load(self) -> 'ShelfSigns':
        """Read every sign's set (blocking; once: later calls return at once). Never raises."""
        if self._ready:
            return self
        with self._lock:
            if self._ready:
                return self
            sets = {}
            for key in SIGNS:
                loader = self._loaders.get(key)
                try:
                    sets[key] = frozenset(str(s) for s in (loader() if loader else ()) or () if s)
                except Exception as exc:   # noqa: BLE001 -- no sign, never a crash
                    logger.warning("shelf signs: the %s sign could not be read: %s", key, exc)
                    sets[key] = frozenset()
            self._sets = sets
            self._ready = True
        return self

    def of(self, sys_id: str) -> tuple:
        """A manuscript's signs, in ``SIGNS`` order (none before ``load``)."""
        if not self._ready or not sys_id:
            return ()
        return tuple(key for key in SIGNS if sys_id in self._sets.get(key, ()))


_shared: Optional[ShelfSigns] = None
_shared_lock = threading.Lock()


def shelf_signs() -> ShelfSigns:
    """The process's one ``ShelfSigns`` (not loaded until someone calls ``load``)."""
    global _shared
    with _shared_lock:
        if _shared is None:
            _shared = ShelfSigns()
        return _shared


def signs_text(signs: Iterable[str], tr: Callable[[str], str]) -> str:
    """The signs named for a row's tooltip: "PGP · Scholarly transcription · Joins"."""
    return ' · '.join(tr(SIGN_LABELS[key]) for key in signs if key in SIGN_LABELS)
