# -*- coding: utf-8 -*-
"""Who is asking for a Fragment Puzzle image, as the image cache needs to know.

The image cache (``shared/puzzle_image_service.py``) keeps a browser's
uploads in one of two places:

* signed in  -> the shared cache, with the account id recorded per file;
* signed out -> that browser's own directory, given back only to it.

A browser is identified by its NiceGUI session id (``request.session['id']``,
which is also ``app.storage.browser['id']`` on a page), prefixed ``'b:'``.
A request that arrived without a session cookie has no browser key: the id
the middleware mints for it would never be sent again.
The account is ``GlobalAuthState.get_user_id()``.

Resolve these in the request or page context, BEFORE any ``run.io_bound``:
a worker thread has no request context and cannot tell who is asking.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)

BROWSER_PREFIX = 'b:'

# Starlette's SessionMiddleware cookie name (NiceGUI keeps the default).
SESSION_COOKIE_NAME = 'session'


def _browser_key_from_session_id(session_id: Any) -> Optional[str]:
    from web.session_hardening import is_canonical_session_id
    if is_canonical_session_id(session_id):
        return BROWSER_PREFIX + session_id
    return None


def browser_key_for_request(request) -> Optional[str]:
    """The requesting browser's key, or None when it has no session.

    Only a request that carried a session cookie has one: a request without
    it is given a fresh id by the middleware, which no later request repeats.
    """
    try:
        if not request.cookies.get(SESSION_COOKIE_NAME):
            return None
    except Exception:
        return None
    try:
        session = request.session
    except (AssertionError, AttributeError):  # no session middleware
        return None
    try:
        return _browser_key_from_session_id(session.get('id'))
    except Exception as e:
        logger.debug("puzzle_image_access: session unreadable: %s", e)
        return None


def browser_key_for_page() -> Optional[str]:
    """The current page visitor's browser key (NiceGUI page context only)."""
    try:
        from nicegui import app
        return _browser_key_from_session_id(app.storage.browser.get('id'))
    except Exception as e:
        logger.debug("puzzle_image_access: browser storage unavailable: %s", e)
        return None


def signed_in_user_id() -> Optional[str]:
    """The signed-in account id for the current request, or None."""
    try:
        from web.auth_state import GlobalAuthState
        user_id = GlobalAuthState.get_user_id()
    except Exception as e:
        logger.debug("puzzle_image_access: auth state unavailable: %s", e)
        return None
    if isinstance(user_id, str) and user_id.strip():
        return user_id.strip()
    return None
