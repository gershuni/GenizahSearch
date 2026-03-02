"""
Session Persistence Service
===========================
Serializes/deserializes desktop search state to JSON for crash-safe
session restore. Uses atomic writes (write-to-tmp then os.replace)
to prevent corruption.

Exports:
    save_session_state(state_dict, path) -> bool
    load_session_state(path) -> Optional[dict]
    clear_session_state(path) -> bool
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Optional

from genizah_core import Config

logger = logging.getLogger(__name__)

# Schema version -- bump when SessionState shape changes
SESSION_VERSION = 1


def save_session_state(state_dict: dict, path: str | None = None) -> bool:
    """
    Persist session state to disk as JSON.

    Writes atomically: tmp file -> os.replace() to avoid corruption on crash.
    Sets/overwrites ``version`` and ``saved_at`` fields automatically.

    Args:
        state_dict: Dict matching the SessionState structure.
        path: Target file path (defaults to Config.SESSION_FILE).

    Returns:
        True on success, False on failure.
    """
    if path is None:
        path = Config.SESSION_FILE

    try:
        state_dict["version"] = SESSION_VERSION
        state_dict["saved_at"] = datetime.now().isoformat()

        # Ensure the parent directory exists
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        # Atomic write: write to temp file in the same directory, then rename
        fd, tmp_path = tempfile.mkstemp(
            suffix=".tmp", prefix="session_", dir=parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(state_dict, f, ensure_ascii=False, indent=2, default=_json_default)
            os.replace(tmp_path, path)
        except Exception:
            # Clean up temp file on failure
            try:
                os.remove(tmp_path)
            except OSError:
                pass
            raise

        return True

    except Exception as e:
        logger.error("Failed to save session state: %s", e)
        return False


def load_session_state(path: str | None = None) -> Optional[dict]:
    """
    Load session state from disk.

    Returns:
        The parsed dict on success, or None if the file doesn't exist or on error.
    """
    if path is None:
        path = Config.SESSION_FILE

    if not os.path.exists(path):
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            state = json.load(f)

        # Validate version field
        version = state.get("version")
        if version is None:
            logger.warning("Session file missing version field, ignoring")
            return None
        if version > SESSION_VERSION:
            logger.warning(
                "Session file version %d is newer than supported %d, ignoring",
                version,
                SESSION_VERSION,
            )
            return None

        return state

    except Exception as e:
        logger.error("Failed to load session state: %s", e)
        return None


def clear_session_state(path: str | None = None) -> bool:
    """
    Delete the session file if it exists.

    Returns:
        True if deleted or already absent, False on error.
    """
    if path is None:
        path = Config.SESSION_FILE

    try:
        if os.path.exists(path):
            os.remove(path)
        return True
    except Exception as e:
        logger.error("Failed to clear session state: %s", e)
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _json_default(obj):
    """
    Custom JSON serializer for types that aren't natively serializable.
    Converts sets to sorted lists.
    """
    if isinstance(obj, set):
        return sorted(obj)
    if isinstance(obj, frozenset):
        return sorted(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")
