"""Centralized LOCAL file actions for the desktop app (v7.16).

Open / reveal / copy-path operations for LOCAL "My Library" hits, shared by the
search-results context menu, the ResultDialog, and the Browse panel. Keeping the
"openable" extension gate in ONE place fixes the v7.15 bug where the gate was
duplicated in two handlers with a stale ``{'.docx', '.pdf', '.txt'}`` set that
refused the ``.html`` / ``.xlsx`` / ``.csv`` files My Library now indexes.

The single source of truth for openable extensions is
``shared.local_indexer._SUPPORTED_EXTENSIONS`` (imported lazily so this module
stays light and importable in tests without pulling in PyMuPDF at module load).
"""
from __future__ import annotations

import logging
import os
import subprocess

logger = logging.getLogger(__name__)


def is_openable_local_file(filepath: str | None) -> bool:
    """Return True if *filepath* exists and its extension is LOCAL-supported.

    Defense-in-depth gate for :func:`open_local_file`. Filepaths only ever come
    from ``indexer.get_filepath()`` (paths walked under user-chosen folders with
    ``followlinks=False``), but we still refuse extensions outside the supported
    set before handing the path to the OS.
    """
    if not filepath or not os.path.isfile(filepath):
        return False
    from shared.local_indexer import _SUPPORTED_EXTENSIONS

    return os.path.splitext(filepath)[1].lower() in _SUPPORTED_EXTENSIONS


def open_local_file(filepath: str | None) -> bool:
    """Launch *filepath* in the OS default application.

    Returns True if the file was launched, False if it was missing or refused by
    the extension gate.
    """
    if not is_openable_local_file(filepath):
        logger.warning("open_local_file: refusing to open %r", filepath)
        return False
    os.startfile(filepath)  # noqa: S606 — Windows-native, extension-gated above
    return True


def reveal_local_file(filepath: str | None) -> bool:
    """Open the containing folder in the OS file manager with the file selected.

    Unlike :func:`open_local_file` this never launches the file itself — it only
    reveals a folder — so it is intentionally NOT gated on the supported-extension
    set. Returns True if a reveal command was issued.
    """
    if not filepath or not os.path.exists(filepath):
        return False
    normalized = os.path.normpath(filepath)
    # IMPORTANT: pass `/select,` and the path as SEPARATE argv entries. The
    # "documented" combined single-argument form (`/select,<path>`) BREAKS for
    # any path containing spaces — subprocess quotes the whole token as
    # "/select,C:\path with spaces\file", which Explorer cannot parse, so it
    # silently opens "My Documents" instead of selecting the file (UAT-confirmed
    # regression, v7.16). The separate-argument form is the one that actually
    # works on Windows. explorer.exe returns exit code 1 even on success, so do
    # not check it. (Bare "explorer" resolves from System32 via PATH.)
    subprocess.Popen(["explorer", "/select,", normalized])  # noqa: S603,S607
    return True


def copy_file_location(filepath: str | None, clipboard) -> bool:
    """Copy the normalized *filepath* to the given Qt clipboard.

    Returns True if a path was copied, False if there was nothing to copy.
    """
    if not filepath:
        return False
    clipboard.setText(os.path.normpath(filepath))
    return True
