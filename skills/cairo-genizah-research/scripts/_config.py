"""Configuration helpers for the cairo-genizah-research skill.

D-09 precedence: GENIZAH_API_BASE env var > --base-url CLI flag > default.
This INVERTS the typical CLI convention (CLI usually wins) — see SKILL.md
for rationale: the env var is the operator-level override for pointing the
skill at a local development server without modifying every script invocation.
"""
from __future__ import annotations
import os
from pathlib import Path

DEFAULT_BASE_URL = "https://genizahsearch.com"


def resolve_base_url(cli_arg: str | None) -> str:
    """Return the effective base URL per D-09 precedence.

    Precedence order (highest first):
    1. GENIZAH_API_BASE environment variable
    2. cli_arg (from --base-url flag)
    3. DEFAULT_BASE_URL constant
    """
    env = os.environ.get("GENIZAH_API_BASE")
    if env:
        return env.rstrip("/")
    if cli_arg:
        return cli_arg.rstrip("/")
    return DEFAULT_BASE_URL


def _state_dir() -> Path:
    override = os.environ.get("CAIRO_GENIZAH_STATE_DIR")
    if override:
        return Path(override)
    # Skill-relative: state/ sibling of scripts/
    return Path(__file__).resolve().parent.parent / "state"


# Resolved at import time; tests that monkeypatch the env var must reload the module.
STATE_DIR = _state_dir()


def state_path(filename: str) -> Path:
    """Return the full path for a state file, creating the directory if needed.

    Re-resolves CAIRO_GENIZAH_STATE_DIR on every call so tests can monkeypatch
    the env var without reloading the module.
    """
    d = _state_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d / filename


def get_rpm() -> int:
    """Effective requests-per-minute per bucket.

    Default 24 — leaves 6 rpm headroom under the server's 30 rpm ceiling
    (Phase 78 HARDEN-01). Override via GENIZAH_SKILL_REQ_PER_MIN env var.
    """
    return int(os.environ.get("GENIZAH_SKILL_REQ_PER_MIN", "24"))


def get_burst() -> int:
    """Maximum burst tokens for the token-bucket throttle.

    Default 5. Override via GENIZAH_SKILL_BURST env var.
    """
    return int(os.environ.get("GENIZAH_SKILL_BURST", "5"))
