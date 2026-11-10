"""Web session hardening: storage secret from the environment, session id validation.

* ``resolve_storage_secret()`` reads ``GENIZAH_STORAGE_SECRET``. It is required
  everywhere (production and local development alike); there is no fallback.
  ``web/main.py`` calls it only inside its ``if __name__ in {...}`` startup
  block, never at import time, because many tests and scripts import
  ``web.main`` without the variable set.
* ``is_canonical_session_id()`` is the rule the session middleware
  (``web.framework_patches._CacheSafeRequestTrackingMiddleware``) applies to
  NiceGUI's session id before user storage is looked up or created.
* ``require_session_id_validation()`` refuses to start the web app unless that
  validating middleware is the class NiceGUI is about to install. On a NiceGUI
  newer than the audited version the swap in ``web.framework_patches`` is
  skipped, so startup refuses until the patches are re-audited.

Import-light on purpose: only the standard library at module level, so a
launcher such as ``scripts/server.py`` can check the secret without importing
NiceGUI or the web app. Never print or log the secret's value.
"""
from __future__ import annotations

import os
import re
from collections.abc import Mapping

STORAGE_SECRET_ENV = 'GENIZAH_STORAGE_SECRET'
MIN_STORAGE_SECRET_LENGTH = 32
_GENERATE_COMMAND = 'python -c "import secrets; print(secrets.token_urlsafe(32))"'

# Canonical lowercase uuid4, exactly what NiceGUI mints (str(uuid.uuid4())).
_CANONICAL_UUID4_RE = re.compile(
    r'[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}'
)


def resolve_storage_secret(environ: Mapping[str, str] | None = None) -> str:
    """Return the stripped ``GENIZAH_STORAGE_SECRET``, or refuse to start.

    Raises ``SystemExit`` (with a message that names the variable and how to
    generate one, never the value) when it is unset, blank, or shorter than
    ``MIN_STORAGE_SECRET_LENGTH`` characters after stripping.
    """
    env = os.environ if environ is None else environ
    value = (env.get(STORAGE_SECRET_ENV) or '').strip()
    if len(value) < MIN_STORAGE_SECRET_LENGTH:
        state = 'is not set' if not value else f'is shorter than {MIN_STORAGE_SECRET_LENGTH} characters'
        raise SystemExit(
            f'{STORAGE_SECRET_ENV} {state}; the web app will not start without it.\n'
            f'Generate one with:  {_GENERATE_COMMAND}\n'
            f'and add {STORAGE_SECRET_ENV}=<that value> to .env (or the service environment). '
            'Changing it signs every web user out once.'
        )
    return value


def is_canonical_session_id(value: object) -> bool:
    """Return whether ``value`` is a canonical lowercase uuid4 string."""
    return isinstance(value, str) and _CANONICAL_UUID4_RE.fullmatch(value) is not None


def require_session_id_validation() -> None:
    """Refuse to start unless NiceGUI will install the session id validating middleware.

    ``nicegui.storage.set_storage_secret`` (run inside ``ui.run``'s server
    start) constructs whatever class ``nicegui.storage.RequestTrackingMiddleware``
    names at that moment, so checking the reference just before ``ui.run`` is
    checking what gets installed.
    """
    import nicegui
    import nicegui.storage as nicegui_storage

    from web.framework_patches import _CacheSafeRequestTrackingMiddleware

    installed = nicegui_storage.RequestTrackingMiddleware
    if isinstance(installed, type) and issubclass(installed, _CacheSafeRequestTrackingMiddleware):
        return
    raise SystemExit(
        'Session id validation is not installed (NiceGUI '
        f'{nicegui.__version__}); the web app will not start without it. '
        'Re-audit web/framework_patches.py for this NiceGUI version and bump '
        '_PATCH_AUDIT_THRESHOLD, or pin the audited NiceGUI version.'
    )
