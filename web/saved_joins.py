# -*- coding: utf-8 -*-
"""Saved Fragment Puzzle joins, kept per visitor.

The web app's saved joins ("drafts") live in the shared ``joins.db`` sidecar
(``shared/puzzle_service.py``). Each visitor sees only their own:

* signed in  -> owner ``'u:<Supabase user id>'`` (follows the account);
* signed out -> owner ``'b:<session uuid>'`` (``web/safe_storage.py``; kept for
  this browser only).

Published joins are a separate thing (Supabase ``published_joins``) and stay
public; publishing still needs sign-in.

This is the ONLY web module that may call ``get_puzzle_service(``
(``tests/test_web_saved_joins_isolation.py`` fails the build otherwise), so
every web read and write of a saved join carries an owner.

Usage: resolve the owner in the page's own context, BEFORE any
``run.io_bound`` (a worker thread has no NiceGUI request context, so it
cannot tell who the visitor is)::

    joins = for_current_visitor()          # raises NoVisitorKey
    doc = await run.io_bound(joins.load_document, doc_id)
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional

from shared.puzzle_model import PuzzleDocument

logger = logging.getLogger(__name__)

USER_PREFIX = 'u:'
BROWSER_PREFIX = 'b:'


class NoVisitorKey(RuntimeError):
    """The current visitor could not be identified, so no saved join is touched."""


def owner_key() -> Optional[str]:
    """Return the current visitor's owner key, or None when it cannot be told.

    Signed in: ``'u:' + user id``. Signed out: ``'b:' + session uuid``. Never
    returns an ephemeral value: a key that changes between calls would save a
    join where its owner can never find it again.
    """
    try:
        from web.auth_state import GlobalAuthState
        user_id = GlobalAuthState.get_user_id()
    except Exception as e:  # storage unavailable
        logger.debug("saved_joins.owner_key: auth state unavailable: %s", e)
        user_id = None
    if isinstance(user_id, str) and user_id.strip():
        return USER_PREFIX + user_id.strip()

    from web.safe_storage import ensure_session_uuid, get_persisted_session_uuid
    ensure_session_uuid()
    session_uuid = get_persisted_session_uuid()
    if session_uuid:
        return BROWSER_PREFIX + session_uuid
    return None


class SavedJoins:
    """A view of ``joins.db`` restricted to one owner.

    Method names match ``PuzzleService`` so this object can be handed to
    shared code that expects a puzzle service (e.g.
    ``shared.puzzle_publish_service.fork_published_join``).
    """

    def __init__(self, owner: str):
        if not isinstance(owner, str) or not owner.strip() or owner.strip() in (USER_PREFIX, BROWSER_PREFIX):
            raise NoVisitorKey('saved joins need an owner')
        self._owner = owner

    @property
    def owner(self) -> str:
        return self._owner

    @staticmethod
    def _service():
        from shared.puzzle_service import get_puzzle_service
        return get_puzzle_service(thread_safe=True)

    def list_documents(self) -> List[Dict]:
        return self._service().list_documents(owner_key=self._owner)

    def load_document(self, doc_id: str) -> Optional[PuzzleDocument]:
        if not doc_id:
            return None
        return self._service().load_document(doc_id, owner_key=self._owner)

    def owns(self, doc_id: str) -> bool:
        return self.load_document(doc_id) is not None

    def save_document(self, doc: PuzzleDocument, thumbnail_b64: str = None) -> Optional[str]:
        return self._service().save_document(doc, thumbnail_b64=thumbnail_b64, owner_key=self._owner)

    def delete_document(self, doc_id: str) -> bool:
        if not doc_id:
            return False
        return self._service().delete_document(doc_id, owner_key=self._owner)

    def list_documents_for_fragment(self, fl_id: str = None, sys_id: str = None) -> List[str]:
        return self._service().list_documents_for_fragment(fl_id=fl_id, sys_id=sys_id, owner_key=self._owner)


def for_current_visitor() -> SavedJoins:
    """Return the current visitor's saved joins. Raises NoVisitorKey if the
    visitor cannot be identified. Call from the page context, not a worker."""
    key = owner_key()
    if not key:
        raise NoVisitorKey('the current visitor could not be identified')
    return SavedJoins(key)
