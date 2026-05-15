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
import re
import uuid as _uuid
from typing import Any, Optional

from nicegui import app

logger = logging.getLogger(__name__)


_SESSION_UUID_KEY = '_session_uuid'
_SESSION_UUID_RE = re.compile(r"^[0-9a-f]{32}$")


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


def _is_valid_uuid(value: Any) -> bool:
    """Return True iff `value` is a 32-char lowercase-hex string matching uuid4().hex shape.

    Per M5 in 87-REVIEWS.md: this is the canonical validation that defends
    against T-87-02 storage-poisoning (uppercase hex, non-string types,
    malformed length, non-hex characters all rejected).
    """
    return isinstance(value, str) and bool(_SESSION_UUID_RE.fullmatch(value))


def get_session_uuid() -> str:
    """Return this session's stable UUID, minting one on first call.

    The UUID is generated lazily via :func:`uuid.uuid4` (CSPRNG-backed in
    CPython per Python docs `uuid.uuid4`) and stored in
    ``app.storage.user['_session_uuid']``. It survives token refresh
    because it lives in storage, not in any auth dict -- Phase 91's
    auth-token rotation will not affect it.

    Returns a fresh ephemeral UUID4 hex string if storage is unavailable
    (prune race) -- callers receive a valid 32-char lowercase hex string
    but the same call site within a different prune window may get a
    DIFFERENT UUID. Downstream cache lookups against a fallback UUID will
    simply miss, which is the correct behavior (no false-positive cache hit).

    Validates retrieved value against ``^[0-9a-f]{32}$`` via
    :func:`_is_valid_uuid`. On storage-poisoning (non-string, uppercase
    hex, wrong length, or non-hex characters) mints fresh and overwrites.
    This defends against the T-87-02 storage-poisoning threat.

    Security note (T-87-03): NEVER log this UUID at INFO+ level, expose
    it in URLs/query strings, or include it in PostHog events without
    HMAC. It is an opaque server-side cache key -- treat as session-secret.

    :returns: A 32-character lowercase hex UUID4 string. Never None. Never raises.
    """
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if _is_valid_uuid(uid):
            return uid
        # Stored value missing, poisoned, or malformed -- mint fresh
        if uid is not None:
            logger.warning(
                "get_session_uuid: stored _session_uuid is not a valid 32-char "
                "lowercase hex string (type=%s); regenerating",
                type(uid).__name__,
            )
        new_uid = _uuid.uuid4().hex
        try:
            app.storage.user[_SESSION_UUID_KEY] = new_uid
        except AssertionError as e:
            logger.debug("get_session_uuid: prune-race during mint write: %s", e)
            # Return the new UUID anyway; it just won't be cached this request
        return new_uid
    except AssertionError as e:
        logger.debug("get_session_uuid: session storage unavailable: %s", e)
        return _uuid.uuid4().hex  # Ephemeral; do NOT cache anywhere persistent
    except Exception as e:
        logger.warning("get_session_uuid unexpected failure: %s", e, exc_info=False)
        return _uuid.uuid4().hex


def get_persisted_session_uuid() -> Optional[str]:
    """Return the persisted `_session_uuid` from storage, or None.

    Strict variant of :func:`get_session_uuid` that refuses to mint or
    return an ephemeral UUID under prune race. Use this when a None
    return is meaningful -- e.g., refresh-lock keying in
    `_refresh_user_session` MUST NOT key by an ephemeral UUID because
    a second concurrent attempt would mint a DIFFERENT ephemeral UUID
    and bypass the per-uuid lock, re-introducing the concurrent
    refresh-token burn that Phase 90 D-06 set out to prevent.

    On prune / storage AssertionError: returns None.
    On poisoned/malformed stored value: returns None (caller must
    re-mint via :func:`get_session_uuid` if minting is appropriate).
    On valid stored value: returns the 32-char lowercase hex string.

    Note: ``get_session_uuid()`` remains the right choice for "mint if
    absent" call sites (e.g., bootstrap, JS bridge). Use this helper
    only when the caller has a meaningful "skip operation" branch for
    the absent case.
    """
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if _is_valid_uuid(uid):
            return uid
        return None
    except AssertionError as e:
        logger.debug("get_persisted_session_uuid: session storage unavailable: %s", e)
        return None
    except Exception as e:
        logger.warning("get_persisted_session_uuid unexpected failure: %s", e, exc_info=False)
        return None


def ensure_session_uuid() -> bool:
    """Eagerly mint session UUID if not present. Returns True on success.

    Use this from a top-of-page-handler when downstream code depends on
    the UUID being present in storage before any conditional path runs
    (e.g., for sharing the UUID with browser JavaScript via
    ``add_head_html``, or for Phase 88+ code that reads _session_uuid
    without going through get_session_uuid).

    Wired into web/main.py:create_layout() per B1 in 87-REVIEWS.md -- every
    page render invokes this before any other layout logic, so the UUID
    is present in storage by the time downstream handlers execute.

    :returns: True if UUID is present in storage after the call (minted
              or already existed and well-formed). False only if storage
              raises AssertionError on either read or write (prune race)
              -- caller may retry on next request.
    """
    try:
        uid = app.storage.user.get(_SESSION_UUID_KEY)
        if _is_valid_uuid(uid):
            return True  # Already present and well-formed
        # Missing, poisoned, or malformed -- mint and write
        try:
            app.storage.user[_SESSION_UUID_KEY] = _uuid.uuid4().hex
            return True
        except AssertionError as e:
            logger.debug("ensure_session_uuid: prune-race during write: %s", e)
            return False
    except AssertionError as e:
        logger.debug("ensure_session_uuid: session storage unavailable: %s", e)
        return False
    except Exception as e:
        logger.warning("ensure_session_uuid unexpected failure: %s", e, exc_info=False)
        return False
