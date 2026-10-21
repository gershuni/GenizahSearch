"""Reconnect backoff: the browser side that keeps a restart from becoming a reload storm.

When the website restarts, every open tab's socket drops; socket.io retries on a
rigid 5 s cadence, and when the server is back each tab's implicit handshake
carries a client id the server no longer has, is refused, and nicegui.js reloads
the page at once -- every tab inside one 5 s window, each a full page build on
the one event loop, and the saturated server's late answers ("timeout") reload
them again. nicegui.js is served at an immutable cached URL and
``window.location.reload`` cannot be overridden, so the fix runs in ``<head>``
before the deferred socket.io.min.js: web/static/js/reconnect_backoff.js wraps
``window.io`` to give socket.io a longer, better-spread reconnection schedule and
to delay NiceGUI's reload for a refused handshake or a timeout by an exponential,
jittered backoff (per tab, in sessionStorage; hidden tabs wait to be viewed).

``head_html`` INLINES the script: a ``src`` with ``defer`` would run after
socket.io.min.js, and a parser-blocking fetch is worse than ~3 KB inline. It is
added once for every page with ``ui.add_head_html(..., shared=True)`` in
web/main.py, so ``/privacy-extension`` and ``/reset-hints`` are covered too.
The server side counts the handshakes it refuses:
``web/framework_patches.py::_patch_handshake_counter`` (``/_internal/memstat``,
``handshakes``).

Env (docs/guides/ENV_VARS.md): RECONNECT_BACKOFF_ENABLED (default on; '0' turns
it off), RECONNECT_BACKOFF_BASE_MS, RECONNECT_BACKOFF_CAP_MS (the reload backoff)
and RECONNECT_DELAY_MAX_MS (socket.io's reconnectionDelayMax). Independent of
POSTHOG_API_KEY: the backoff is not analytics.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

_JS_PATH = Path(__file__).resolve().parent / 'static' / 'js' / 'reconnect_backoff.js'

DEFAULT_BASE_MS = 1500        # first reload wait, and socket.io's reconnectionDelay
DEFAULT_CAP_MS = 60000        # the exponential's cap (the jitter spreads around it)
DEFAULT_DELAY_MAX_MS = 30000  # socket.io's reconnectionDelayMax (its default is 5000)


def enabled() -> bool:
    return os.environ.get('RECONNECT_BACKOFF_ENABLED', '1').strip().lower() not in ('0', 'false', 'no', 'off')


def _positive_int_env(name: str, default: int) -> int:
    try:
        value = int(os.environ.get(name, '').strip() or default)
    except ValueError:
        return default
    return value if value > 0 else default


def config() -> dict:
    """What the browser reads from ``window.GS_RECONNECT``."""
    return {
        'base_ms': _positive_int_env('RECONNECT_BACKOFF_BASE_MS', DEFAULT_BASE_MS),
        'cap_ms': _positive_int_env('RECONNECT_BACKOFF_CAP_MS', DEFAULT_CAP_MS),
        'delay_max_ms': _positive_int_env('RECONNECT_DELAY_MAX_MS', DEFAULT_DELAY_MAX_MS),
    }


def head_html() -> str:
    """The <head> snippet: the config, then the script inlined. '' when switched off."""
    if not enabled():
        return ''
    try:
        script = _JS_PATH.read_text(encoding='utf-8')
    except OSError:
        logger.warning('reconnect_backoff: %s is missing; reloads are not spread', _JS_PATH)
        return ''
    return (f'<script>window.GS_RECONNECT={json.dumps(config())};</script>\n'
            f'<script>\n{script}</script>\n')
