"""Feature flags for the web application."""

from __future__ import annotations

import os


def _env_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


WEB_PUZZLE_ENABLED = _env_enabled("WEB_PUZZLE_ENABLED", True)


def web_fgp_enabled() -> bool:
    """Whether FGP transcriptions surface in the WEB version chooser.

    Layers an optional web-only override on top of the shared
    ``FGP_TRANSCRIPTIONS_ENABLED`` gate (the live one, re-read per call in
    ``shared/fgp_service.py``). When ``WEB_FGP_ENABLED`` is unset it defaults to
    the shared flag; when set it wins for the web app (e.g. disable on web while
    the shared/desktop default is on). Read per call so an env flip + restart
    takes effect without code changes. ``shared/`` must not import this.

    Default: ON (2026-06-22, go-live) — mirrors the shared
    ``FGP_TRANSCRIPTIONS_ENABLED`` default. Graceful no-op when the sidecar DB is
    absent. Disable on web only with ``WEB_FGP_ENABLED=0``.
    """
    shared_default = _env_enabled("FGP_TRANSCRIPTIONS_ENABLED", True)
    return _env_enabled("WEB_FGP_ENABLED", shared_default)
