# -*- coding: utf-8 -*-
"""Phase 97.3 R97.3-D (D-08 + D-09) — Skip suppresses same-launch auto-rescan.

Pins the new one-shot `_skip_startup_rescan_once` flag semantics on MyLibraryTab:

  - Test 1 (D-09 invariant): the attribute is initialised to False in __init__
    BEFORE the recovery-modal call path, so it always exists.
  - Test 2 (D-09 suppression): when the flag is True at entry to
    `_auto_rescan_on_startup`, the function returns early without calling
    `_start_worker` AND clears the flag (consumed on first read).
  - Test 3 (D-25 default unchanged): when the flag is False and folders are
    registered, `_auto_rescan_on_startup` fires normally and spawns a worker.

These tests MUST be RED against the current (pre-D-08/D-09) code where the
attribute does NOT exist (Test 1) and where `_auto_rescan_on_startup` does NOT
consult the flag (Test 2). Test 3 is GREEN against current code (D-25 default)
and stays GREEN after the fix — it is the regression guard.

RED-before-fix protocol: this file is created in Task 1 expecting Tests 1 + 2
to fail; Task 2 implements D-08 + D-09 in desktop/my_library_tab.py to flip
them GREEN.
"""
from __future__ import annotations

import os
import sys

# Path bootstrap so `desktop.my_library_tab` is importable.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

# PyQt6-aware fixture pattern mirroring tests/test_folder_walk_worker.py and
# tests/test_my_library_tab_reset_guard.py (Phase 97.3-01 Task 1).
try:
    from PyQt6.QtWidgets import QApplication
    _app = QApplication.instance() or QApplication(sys.argv[:1])
except ImportError:  # pragma: no cover - PyQt6 missing
    pytest.skip("PyQt6 not available", allow_module_level=True)


def _make_tab(tmp_path, monkeypatch):
    """Construct a real MyLibraryTab against an isolated tmp_path index.

    Mirrors the helper in tests/test_my_library_tab_reset_guard.py (97.3-01).
    Patches genizah_core.Config.LOCAL_INDEX_DIR and LOCAL_LAB_INDEX_DIR BEFORE
    construction so MyLibraryTab._init_indexer wires the LocalIndexer at the
    test-owned paths.
    """
    from genizah_core import Config
    idx_dir = str(tmp_path / "local_index")
    lab_dir = str(tmp_path / "local_lab")
    monkeypatch.setattr(Config, "LOCAL_INDEX_DIR", idx_dir, raising=False)
    monkeypatch.setattr(Config, "LOCAL_LAB_INDEX_DIR", lab_dir, raising=False)

    from desktop.my_library_tab import MyLibraryTab
    tab = MyLibraryTab(parent=None)
    return tab


def _close_tab(tab):
    """Best-effort cleanup of Qt resources + indexer SQLite handle."""
    try:
        if getattr(tab, "_indexer", None) is not None:
            try:
                tab._indexer._conn.close()
            except Exception:
                pass
        tab.deleteLater()
    except Exception:
        pass


def test_skip_flag_initialised_false_in_init(tmp_path, monkeypatch):
    """D-09 invariant: `_skip_startup_rescan_once` attribute exists, False.

    The flag must be initialised in __init__ BEFORE the recovery-modal call
    path so the attribute always exists even on init failure paths. A fresh
    MyLibraryTab construction (no recovery modal triggered, since the tmp_path
    DB has no orphan scan_runs rows) must leave the flag at its initial False.
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        assert hasattr(tab, "_skip_startup_rescan_once"), (
            "D-09: MyLibraryTab.__init__ must initialise _skip_startup_rescan_once "
            "BEFORE the recovery-modal call path so the attribute always exists."
        )
        assert tab._skip_startup_rescan_once is False, (
            "D-09: Initial value of _skip_startup_rescan_once must be False; "
            f"got {tab._skip_startup_rescan_once!r}."
        )
    finally:
        _close_tab(tab)


def test_auto_rescan_returns_early_when_flag_set(tmp_path, monkeypatch):
    """D-09 suppression: flag=True -> `_auto_rescan_on_startup` returns early.

    Simulates the Skip-click outcome by directly setting the flag, then calls
    `_auto_rescan_on_startup` and verifies (a) `_start_worker` was NOT invoked
    and (b) the flag is cleared after the call (consumed on first read so a
    subsequent manual Refresh works normally).
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        # Simulate Skip-click flag (would normally be set by _show_recovery_modal)
        tab._skip_startup_rescan_once = True

        # Record _start_worker calls — they MUST NOT happen.
        calls = []
        monkeypatch.setattr(
            tab,
            "_start_worker",
            lambda toast_on_complete=False: calls.append(toast_on_complete),
        )

        tab._auto_rescan_on_startup()

        assert calls == [], (
            "D-09: _auto_rescan_on_startup must NOT call _start_worker when "
            f"_skip_startup_rescan_once is True; got calls={calls!r}."
        )
        assert tab._skip_startup_rescan_once is False, (
            "D-09: _skip_startup_rescan_once must be CLEARED after first read "
            "(one-shot semantics) so a subsequent manual Refresh fires normally."
        )
    finally:
        _close_tab(tab)


def test_auto_rescan_fires_normally_when_flag_unset(tmp_path, monkeypatch):
    """D-25 default unchanged: flag=False + folders registered -> worker spawns.

    Verifies the no-modal path (no orphan scan_runs rows) still triggers the
    D-25 silent auto-rescan exactly as before Phase 97.3.
    """
    tab = _make_tab(tmp_path, monkeypatch)
    try:
        assert tab._indexer is not None, "fixture must produce a real LocalIndexer"

        # Register a real folder so list_folders() is non-empty
        real_folder = tmp_path / "test_corpus"
        real_folder.mkdir()
        tab._indexer.add_folder(str(real_folder))

        # Flag at default False (no Skip click)
        tab._skip_startup_rescan_once = False

        # Record _start_worker calls — exactly one expected (D-25 default).
        calls = []
        monkeypatch.setattr(
            tab,
            "_start_worker",
            lambda toast_on_complete=False: calls.append(toast_on_complete),
        )

        tab._auto_rescan_on_startup()

        assert len(calls) == 1, (
            "D-25: _auto_rescan_on_startup must call _start_worker exactly once "
            f"when flag is False and folders are registered; got calls={calls!r}."
        )
        # toast_on_complete=True is the existing D-25 contract
        assert calls[0] is True, (
            "D-25: silent auto-rescan must pass toast_on_complete=True; "
            f"got {calls[0]!r}."
        )
    finally:
        _close_tab(tab)
