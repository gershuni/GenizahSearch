# -*- coding: utf-8 -*-
"""A search hit in a scholarly transcription, as the desktop's viewers open it.

Editions plan section 3.4; the website's twin is web/pages/browse_editions.py, and the rules
both follow are in shared/edition_target.py. A result whose best match is in an FGP or PGP
edition (its ``edition`` record) opens -- in the result viewer (desktop/result_dialog.py) and
in Browse (genizah_app.py, ``open_result_in_browse``) -- on that edition:

* on the folio the hit arrived for (the first one shown claims it: :meth:`EditionTarget.owns`),
  the edition is kept among the page's sources even where the page filter would leave it out
  (PGPSourceWorker ``keep_version``) and chosen in the version chooser
  (:meth:`EditionTarget.choose`), instead of the default choice;
* the match is marked on the edition's own characters, mapped from the span the search read
  (:meth:`EditionTarget.marked`), never guessed again from a phrase;
* a stale hit (the edition gone, or its text changed since the index was built) opens the
  manuscript as usual, and the reader is told once (:meth:`EditionTarget.notice`); so is a hit
  whose page could not be identified (it shows with the page the manuscript opens on).

The row's identity stays the edition's: the viewer never replaces it with the automatic page's.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Tuple

from shared.edition_target import occurrence_at, resolve_mark, source_version, target_of

LOGGER = logging.getLogger("genizah." + __name__)

STALE_NOTICE = "The scholarly transcription has changed since the search; the manuscript is shown"
UNPLACED_NOTICE = ("The page of this match could not be identified; the edition is shown with the "
                   "page the manuscript opens on")

_resolver = None


def editions_resolver():
    """The process's EditionResolver (the sidecars' paths, found once)."""
    global _resolver
    if _resolver is None:
        from shared.editions_search import EditionResolver
        _resolver = EditionResolver()
    return _resolver


class EditionTarget:
    """The edition one opened result names, and what its folio shows of it."""

    def __init__(self, target: Dict[str, Any]):
        self.version: str = target['version']
        self.span = target.get('span')
        self.fp = target.get('fp')
        self.sys_id: str = target.get('sys_id') or ''
        self.located: bool = bool(target.get('located'))
        self.scope: Optional[Tuple] = None      # the folio that claimed it
        self.resolved = False
        self.stale = False
        self.mark: Optional[str] = None
        self.occurrence = 0
        self._notified = False

    @classmethod
    def from_row(cls, row: Optional[Dict[str, Any]]) -> Optional["EditionTarget"]:
        target = target_of(row)
        return cls(target) if target else None

    def owns(self, key: Tuple) -> bool:
        """Is *key* (a folio: sys_id, volume, page) the one this hit arrived for? The first
        folio asked claims it; every other folio is browsed as usual."""
        if self.scope is None:
            self.scope = key
        return self.scope == key

    def keep_version(self, key: Tuple, engine=None) -> str:
        """The version the page's sources must keep (PGPSourceWorker ``keep_version``), on the
        folio this hit owns, else ''. With *engine*, the hit is resolved first: a stale edition
        is never forced among the page's sources, where the default choice could then show it
        (Codex review of #28)."""
        if not self.owns(key):
            return ''
        if engine is not None:
            self.resolve(engine)
        return '' if self.stale else self.version

    def resolve(self, engine) -> None:
        """Find the match in the edition's own text (once): a sidecar read and an index lookup."""
        if self.resolved:
            return
        try:
            found = resolve_mark(engine, editions_resolver(), self.sys_id, self.version, self.span, self.fp)
        except Exception:
            LOGGER.warning("Could not read the scholarly transcription %s", self.version, exc_info=True)
            found = {'stale': True, 'mark': None, 'occurrence': 0}
        self.stale = bool(found.get('stale'))
        self.mark = found.get('mark')
        self.occurrence = int(found.get('occurrence') or 0)
        self.resolved = True

    def choose(self, combo, key: Tuple, engine) -> Optional[Dict[str, Any]]:
        """On the folio this hit owns, choose its edition in the version chooser *combo*
        (without its change signal: the caller shows the text) and return the item's data;
        None when another folio shows, the hit is stale, or the chooser lacks the edition --
        the caller's default choice then applies."""
        if not self.owns(key):
            return None
        self.resolve(engine)
        if self.stale:
            return None
        for i in range(combo.count()):
            data = combo.itemData(i)
            if isinstance(data, dict) and source_version(data) == self.version:
                combo.blockSignals(True)
                combo.setCurrentIndex(i)
                combo.blockSignals(False)
                return data
        return None

    def marked(self, text: str, shown: Optional[Dict[str, Any]], key: Tuple) -> Optional[str]:
        """*text* with the match marked (``*...*``, one stretch per line: the viewers lay the
        text out line by line) while it is this hit's edition on its folio (*shown*: the
        chooser's current item data); None otherwise, and the usual search marks apply."""
        if not (self.resolved and not self.stale and self.mark) or self.scope != key:
            return None
        if not isinstance(shown, dict) or source_version(shown) != self.version:
            return None
        where = occurrence_at(text, self.mark, self.occurrence)
        if where is None:
            return None
        start, end = where
        inner = '*\n*'.join(text[start:end].split('\n'))
        return f"{text[:start]}*{inner}*{text[end:]}"

    def notice(self) -> Optional[str]:
        """What the reader should be told about this hit (in English, translated where shown),
        once: that the edition has changed since the search, or that its page could not be
        identified. None otherwise, or when already told."""
        if self._notified or not self.resolved:
            return None
        message = STALE_NOTICE if self.stale else (None if self.located else UNPLACED_NOTICE)
        if message:
            self._notified = True
        return message
