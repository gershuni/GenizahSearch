# -*- coding: utf-8 -*-
"""Safe wrappers around ``nicegui.app.storage.user``.

Bug 2026-05-12: NiceGUI's session storage raises
``AssertionError: user storage for {uuid} should be created before
accessing it`` (storage.py:121) when the cookie's session_id was just
evicted by ``prune_user_storage`` (10s scheduler) but the request
already carried that cookie. Any code that touches ``app.storage.user``
without a try/except inherits a 500 from this race.

The hotfix for ``web/pages/browse_state.py`` wrapped the reads inline,
but Codex review of v7.11.0 post-release commits flagged the SAME
unprotected pattern at five more sites:

    web/pages/search_state.py:343    restore_search_snapshot
    web/pages/search_state.py:389    persist_search_snapshot
    web/pages/search.py:98           search bootstrap (5 raw reads)
    web/pages/parallels.py:258, :266 parallels bootstrap (2 raw reads)
    web/api.py:2106                  /api/export/browse/word handler

This module centralizes the guard so future code only has to call
``safe_user_get(...)`` / ``safe_user_set(...)`` / ``safe_user_pop(...)``
instead of re-implementing the try/except each time.

Production: routes through ``app.storage.user`` per request session.
Tests: when NiceGUI's storage context is unavailable the helpers swallow
the AssertionError and return defaults — exactly the contract callers
already had inline.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from nicegui import app

logger = logging.getLogger(__name__)


def safe_user_get(key: str, default: Any = None) -> Any:
    """Read ``app.storage.user[key]``, returning ``default`` on any failure.

    Catches the pruned-session AssertionError specifically and logs it at
    debug (expected under high concurrency / storage churn). Catches
    other exceptions at warning so unexpected breakage still surfaces.
    """
    try:
        return app.storage.user.get(key, default)
    except AssertionError as e:
        logger.debug("safe_user_get(%r): session storage unavailable: %s", key, e)
        return default
    except Exception as e:
        logger.warning("safe_user_get(%r) unexpected failure: %s", key, e, exc_info=False)
        return default


def safe_user_set(key: str, value: Any) -> bool:
    """Write ``app.storage.user[key] = value``. Return True on success."""
    try:
        app.storage.user[key] = value
        return True
    except AssertionError as e:
        logger.debug("safe_user_set(%r): session storage unavailable: %s", key, e)
        return False
    except Exception as e:
        logger.warning("safe_user_set(%r) unexpected failure: %s", key, e, exc_info=False)
        return False


def safe_user_pop(key: str, default: Any = None) -> Any:
    """``app.storage.user.pop(key, default)`` with the same protection."""
    try:
        return app.storage.user.pop(key, default)
    except AssertionError as e:
        logger.debug("safe_user_pop(%r): session storage unavailable: %s", key, e)
        return default
    except Exception as e:
        logger.warning("safe_user_pop(%r) unexpected failure: %s", key, e, exc_info=False)
        return default
