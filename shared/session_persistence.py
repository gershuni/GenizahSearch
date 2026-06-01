"""
Session Persistence Service
===========================
Serializes/deserializes desktop search state to JSON for crash-safe
session restore. Uses atomic writes (write-to-tmp then os.replace)
to prevent corruption.

Also provides search history management (separate file) with dedup,
limit enforcement, and interrupted-search detection.

Exports:
    save_session_state(state_dict, path) -> bool
    load_session_state(path) -> Optional[dict]
    clear_session_state(path) -> bool
    add_history_entry(search_type, entry, limit) -> bool
    get_history(search_type) -> dict | list
    delete_history_entry(search_type, index) -> bool
    clear_history(search_type) -> bool
    get_interrupted_search() -> Optional[dict]
    clear_interrupted_flag() -> bool
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

# Search history file (separate from session.json)
HISTORY_FILE = os.path.join(Config.INDEX_DIR, "search_history.json")


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
        regular = state_dict.get("regular_search", {}) or {}
        composition = state_dict.get("composition_search", {}) or {}
        logger.info(
            "Session persistence save: path=%s scope=%s optouts=%d "
            "regular_results=%d composition_results=%d",
            path,
            regular.get("search_corpus_scope", "genizah"),
            len(state_dict.get("local_file_optouts", []) or []),
            len(regular.get("results", []) or []),
            len(composition.get("results", []) or []),
        )

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
        logger.info("Session persistence load: no file at path=%s", path)
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

        regular = state.get("regular_search", {}) or {}
        logger.info(
            "Session persistence load: path=%s version=%s scope=%s optouts=%d",
            path,
            version,
            regular.get("search_corpus_scope", "genizah"),
            len(state.get("local_file_optouts", []) or []),
        )
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
# Search History Management
# ---------------------------------------------------------------------------


def _strip_history_result_snapshots(data: dict) -> bool:
    """Drop heavy per-entry result snapshots from a history dict, in place.

    Older builds stored up to 5000 full result dicts per entry under
    ``state['results']`` (and ``state['filtered_results']`` for composition).
    With a 20-entry limit that grew ``search_history.json`` to hundreds of MB,
    and every search loaded + rewrote the whole file on the UI thread — a
    ~20-30s freeze on every search, independent of result count. History no
    longer keeps result snapshots (clicking an entry re-runs the search), so
    we remove them here. Returns True if anything was stripped.
    """
    stripped = False
    for key in ("regular", "composition"):
        for entry in data.get(key, []) or []:
            state = entry.get("state") if isinstance(entry, dict) else None
            if isinstance(state, dict):
                if state.pop("results", None) is not None:
                    stripped = True
                if state.pop("filtered_results", None) is not None:
                    stripped = True
    return stripped


def _load_history_file() -> dict:
    """Load the raw history dict from disk. Returns empty structure on error."""
    if not os.path.exists(HISTORY_FILE):
        return {"regular": [], "composition": []}
    try:
        with open(HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"regular": [], "composition": []}
        # Ensure both keys exist
        data.setdefault("regular", [])
        data.setdefault("composition", [])
        # Self-heal legacy bloat (see _strip_history_result_snapshots): a file
        # that still carries result snapshots will be slimmed on the next save.
        _strip_history_result_snapshots(data)
        return data
    except Exception as e:
        logger.error("Failed to load history file: %s", e)
        return {"regular": [], "composition": []}


def _save_history_file(data: dict) -> bool:
    """Atomically write history dict to disk."""
    try:
        parent = os.path.dirname(HISTORY_FILE)
        if parent:
            os.makedirs(parent, exist_ok=True)

        fd, tmp_path = tempfile.mkstemp(
            suffix=".tmp", prefix="history_", dir=parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, default=_json_default)
            os.replace(tmp_path, HISTORY_FILE)
        except Exception:
            try:  # Primary method failed; try fallback
                os.remove(tmp_path)
            except OSError:
                pass
            raise
        return True
    except Exception as e:
        logger.error("Failed to save history file: %s", e)
        return False


def add_history_entry(search_type: str, entry: dict, limit: int = 20) -> bool:
    """
    Add or update a search history entry.

    Args:
        search_type: ``'regular'`` or ``'composition'``.
        entry: Dict with keys: query, result_count, timestamp, search_params, state.
        limit: Maximum entries to keep per search_type.

    Dedup: if an entry with matching ``query`` AND ``search_params`` already
    exists for this search_type, update it in place (new state, count, timestamp).

    Returns:
        True on success, False on failure.
    """
    try:
        # Invariant: history never persists result snapshots. Drop any that a
        # caller passes (defensive — see _strip_history_result_snapshots).
        _strip_history_result_snapshots({search_type: [entry]})

        data = _load_history_file()
        entries = data.get(search_type, [])

        # Dedup: find existing entry with same query + search_params
        query = entry.get("query", "")
        params = entry.get("search_params", {})
        found_idx = None
        for i, existing in enumerate(entries):
            if (existing.get("query") == query
                    and existing.get("search_params") == params):
                found_idx = i
                break

        if found_idx is not None:
            # Update existing entry
            entries[found_idx]["result_count"] = entry.get("result_count", 0)
            entries[found_idx]["timestamp"] = entry.get("timestamp", datetime.now().isoformat())
            entries[found_idx]["state"] = entry.get("state", {})
            if "pre_search_filters" in entry:
                entries[found_idx]["pre_search_filters"] = entry["pre_search_filters"]
        else:
            # Add new entry at the beginning (newest first)
            new_entry = {
                "query": query,
                "result_count": entry.get("result_count", 0),
                "timestamp": entry.get("timestamp", datetime.now().isoformat()),
                "search_params": params,
                "state": entry.get("state", {}),
            }
            if "pre_search_filters" in entry:
                new_entry["pre_search_filters"] = entry["pre_search_filters"]
            entries.insert(0, new_entry)

        # Sort by timestamp descending (newest first) after update
        entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)

        # Enforce limit
        if len(entries) > limit:
            entries = entries[:limit]

        data[search_type] = entries
        return _save_history_file(data)

    except Exception as e:
        logger.error("Failed to add history entry: %s", e)
        return False


def get_history(search_type: str | None = None) -> dict | list:
    """
    Retrieve search history.

    Args:
        search_type: ``'regular'``, ``'composition'``, or None for all.

    Returns:
        If search_type is None: ``{'regular': [...], 'composition': [...]}``.
        If search_type given: list of entries (sorted newest first).
    """
    try:
        data = _load_history_file()
        if search_type is None:
            return data
        entries = data.get(search_type, [])
        # Ensure sorted newest first
        entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
        return entries
    except Exception as e:
        logger.error("Failed to get history: %s", e)
        if search_type is None:
            return {"regular": [], "composition": []}
        return []


def delete_history_entry(search_type: str, index: int) -> bool:
    """
    Delete a single history entry by index.

    Args:
        search_type: ``'regular'`` or ``'composition'``.
        index: Zero-based index in the (sorted newest-first) list.

    Returns:
        True on success, False on failure or invalid index.
    """
    try:
        data = _load_history_file()
        entries = data.get(search_type, [])
        entries.sort(key=lambda e: e.get("timestamp", ""), reverse=True)
        if 0 <= index < len(entries):
            entries.pop(index)
            data[search_type] = entries
            return _save_history_file(data)
        return False
    except Exception as e:
        logger.error("Failed to delete history entry: %s", e)
        return False


def clear_history(search_type: str | None = None) -> bool:
    """
    Clear search history.

    Args:
        search_type: ``'regular'``, ``'composition'``, or None to clear all.

    Returns:
        True on success, False on failure.
    """
    try:
        if search_type is None:
            return _save_history_file({"regular": [], "composition": []})
        data = _load_history_file()
        data[search_type] = []
        return _save_history_file(data)
    except Exception as e:
        logger.error("Failed to clear history: %s", e)
        return False


def get_interrupted_search() -> Optional[dict]:
    """
    Check if the last session had an interrupted composition search.

    Returns:
        The composition_search state dict if ``was_interrupted`` is True
        in session.json, else None.
    """
    try:
        state = load_session_state()
        if state and state.get("was_interrupted"):
            return state.get("composition_search")
        return None
    except Exception as e:
        logger.error("Failed to check interrupted search: %s", e)
        return None


def clear_interrupted_flag() -> bool:
    """
    Remove the ``was_interrupted`` flag from session.json.

    Returns:
        True on success, False on failure.
    """
    try:
        state = load_session_state()
        if state and "was_interrupted" in state:
            state.pop("was_interrupted", None)
            return save_session_state(state)
        return True
    except Exception as e:
        logger.error("Failed to clear interrupted flag: %s", e)
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
