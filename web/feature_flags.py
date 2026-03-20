"""Feature flags for the web application."""

from __future__ import annotations

import os


def _env_enabled(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


WEB_PUZZLE_ENABLED = _env_enabled("WEB_PUZZLE_ENABLED", True)
