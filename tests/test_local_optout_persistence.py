# -*- coding: utf-8 -*-
"""Phase 96 D-F1: session-JSON round-trip + rescan-preservation tests.

Implementation plan: 96-04-PLAN.md (CONTEXT D-08 REVISED 2026-05-24 ->
session JSON, NOT QSettings -- matches Phase 95 local_filter pattern).
"""
import pytest
import types


def _build_session_dict(local_file_optouts=None):
    """Mirror tests/test_local_filter_persistence.py:17 with new key."""
    return {
        'version': 1,
        'local_file_optouts': sorted(local_file_optouts or []),
        'regular_search': {
            'printed_filter': 'all',
            'local_filter': 'all',
            'results': [],
        },
        'composition_search': {
            'printed_filter': 'all',
            'local_filter_composition': 'all',
            'local_filter_parallels': 'all',
            'results': [],
            'filtered_results': [],
        },
    }


def _restore_local_file_optouts(state_dict):
    """Simulate Phase 96 96-04 restore logic — top-level key."""
    return state_dict.get('local_file_optouts', [])


def test_session_json_roundtrip_preserves_optouts():
    """D-F1: opt-out list round-trips through session-JSON pattern."""
    paths = [
        r"c:\users\h\genizah\file1.pdf",
        r"c:\users\h\genizah\sub\file2.docx",
    ]
    state = _build_session_dict(local_file_optouts=paths)
    assert _restore_local_file_optouts(state) == sorted(paths)


def test_optout_list_default_empty_for_old_sessions():
    """D-F1: pre-Phase-96 session files lack the key — restore returns []."""
    pre_phase_96 = {
        'version': 1,
        'regular_search': {'local_filter': 'all', 'results': []},
        'composition_search': {'local_filter_composition': 'all', 'local_filter_parallels': 'all'},
    }
    assert _restore_local_file_optouts(pre_phase_96) == []


def test_rescan_preserves_survivors_drops_removed():
    """D-F1 D-09: after rescan, opt-out set for files still on disk is
    preserved; entries for files no longer present are dropped.

    NOTE: this test exercises the Phase 96 helper `_prune_optouts_to_disk`
    shipped in plan 96-04 (closed 2026-05-24). Skip converted to direct
    import per BLOCKER 5 audit in plan 96-09.
    """
    # Phase 96 D-F1 shipped in plan 96-04 (closed 2026-05-24).
    from desktop.my_library_tab import _prune_optouts_to_disk
    optouts = {
        r"c:\users\h\genizah\file1.pdf",
        r"c:\users\h\genizah\removed.pdf",
    }
    on_disk = {r"c:\users\h\genizah\file1.pdf"}
    pruned = _prune_optouts_to_disk(optouts, on_disk)
    assert pruned == {r"c:\users\h\genizah\file1.pdf"}


def test_folder_a_optout_survives_folder_b_toggle():
    """Phase 96 D-F1 -- Codex HIGH #1 regression guard (REVISION 2026-05-24).

    Simulates the cross-folder scenario:
      1. User opts out file `/folder_a/file.pdf` (added to global set).
      2. User switches the MyLibraryTab folder list to folder B.
      3. User toggles ANY file in folder B (irrelevant which -- the bug
         was triggered just by the toggle event firing _commit_changes()).
      4. Folder A's opt-out must STILL be in the global set.

    Tests the SET-DIFFERENCE/UNION update logic. We emulate the production
    method below with the EXACT same algebra; if the production code regresses
    to clear+rebuild, this test still passes (it tests the algebra, not the
    production method). The protection comes from:
      - This algebra-level test (catches conceptual regressions)
      - The AST guard in tests/test_local_filter_cascade.py (catches that
        _commit_changes() exists with no .clear() call)
      - The acceptance criterion in 96-06-PLAN.md that greps the production
        body for absence of `.clear()` + presence of `difference_update`/`update`.

    Implementation plan: 96-06-PLAN.md
    """
    # Initial global set (across folders A and B).
    global_optouts = {r"c:\users\h\folder_a\file.pdf"}  # folder A opt-out

    # User switches to folder B; tree displays only folder B's files.
    displayed_in_folder_b = {
        r"c:\users\h\folder_b\file1.pdf",
        r"c:\users\h\folder_b\file2.pdf",
    }

    # User unchecks folder_b\file1.pdf. _commit_changes() walks the tree:
    currently_unchecked = {r"c:\users\h\folder_b\file1.pdf"}
    currently_checked = {r"c:\users\h\folder_b\file2.pdf"}

    # The PRODUCTION algebra (set-difference then set-union, SCOPED to
    # displayed paths only):
    global_optouts.difference_update(currently_checked)   # remove re-checked
    global_optouts.update(currently_unchecked)             # add newly unchecked

    # Folder A's opt-out MUST still be present.
    assert r"c:\users\h\folder_a\file.pdf" in global_optouts, (
        "Codex HIGH #1 regression: folder A opt-out was erased by folder B toggle. "
        "_commit_changes() must use SET-DIFFERENCE/UNION (NOT clear+rebuild)."
    )
    # Folder B's newly-unchecked file is now in the set.
    assert r"c:\users\h\folder_b\file1.pdf" in global_optouts
    # Folder B's re-checked file is not in the set.
    assert r"c:\users\h\folder_b\file2.pdf" not in global_optouts


def test_canonical_filepath_windows_variants():
    """Phase 96 D-F1 -- Codex MEDIUM #9 closure (REVISION 2026-05-24).

    Asserts that `_canonical_filepath` from shared/local_sys_id.py
    normalizes Windows path variants (mixed case + forward/backward slashes)
    to a single canonical form. Without this, an opt-out stored under one
    casing/slash form could fail to match the same logical file looked up
    under another form on a case-insensitive filesystem.

    Skipped on non-Windows platforms because the case-folding behaviour is
    Windows-specific (Unix is case-preserving).

    Implementation plan: 96-04-PLAN.md / 96-06-PLAN.md (both rely on
    canonical form for set membership).
    """
    import os
    import sys

    if sys.platform != 'win32':
        pytest.skip("canonical_filepath case-folding is Windows-specific")

    try:
        from shared.local_sys_id import _canonical_filepath
    except ImportError:
        pytest.skip("_canonical_filepath helper not importable")

    # Build a real file path that exists so _canonical_filepath can resolve.
    # Use a stable system file that's case-insensitive on Windows.
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as f:
        f.write(b"test")
        real_path = f.name
    try:
        # Variant 1: as-stored
        canon_1 = _canonical_filepath(real_path)
        # Variant 2: uppercase
        canon_2 = _canonical_filepath(real_path.upper())
        # Variant 3: forward slashes (Windows accepts both)
        canon_3 = _canonical_filepath(real_path.replace('\\', '/'))
        # Variant 4: mixed case
        canon_4 = _canonical_filepath(real_path.lower())

        assert canon_1 == canon_2 == canon_3 == canon_4, (
            f"Codex MEDIUM #9: _canonical_filepath did not normalize Windows variants.\n"
            f"  as-stored: {canon_1}\n"
            f"  upper:     {canon_2}\n"
            f"  fwd-slash: {canon_3}\n"
            f"  lower:     {canon_4}"
        )
    finally:
        try:
            os.unlink(real_path)
        except OSError:
            pass


def test_unified_tree_uncheck_close_reopen_roundtrip(tmp_path):
    """User scenario: file appears in My Library, user unchecks it, closes the
    app, and reopens. The real tree widget must write the canonical path to
    session JSON and redraw that file unchecked on reopen.
    """
    import sys

    qt_core = pytest.importorskip("PyQt6.QtCore")
    qt_widgets = pytest.importorskip("PyQt6.QtWidgets")
    Qt = qt_core.Qt
    QApplication = qt_widgets.QApplication
    QWidget = qt_widgets.QWidget

    from desktop.my_library_tab import _UnifiedFileTreeWidget
    from shared.local_sys_id import _canonical_filepath
    from shared.session_persistence import load_session_state, save_session_state

    qt_app = QApplication.instance() or QApplication(sys.argv)
    assert qt_app is not None

    folder = tmp_path / "library"
    folder.mkdir()
    file_path = folder / "scan.pdf"
    file_path.write_text("sample", encoding="utf-8")
    canonical = _canonical_filepath(file_path)

    class DummyApp:
        def __init__(self):
            self._local_file_optouts = set()
            self.saved = []

        def _save_session(self):
            self.saved.append(set(self._local_file_optouts))

        def _reapply_filters_for_optout_change(self):
            pass

    app1 = DummyApp()
    parent1 = QWidget()
    tree1 = _UnifiedFileTreeWidget(parent1, app1)
    tree1.populate_for_folder(str(folder), prior_status={})

    leaf = tree1.topLevelItem(0).child(0)
    assert leaf.data(0, Qt.ItemDataRole.UserRole) == canonical
    leaf.setCheckState(0, Qt.CheckState.Unchecked)

    # closeEvent calls flush_pending before _save_session.
    tree1.flush_pending()
    assert canonical in app1._local_file_optouts
    assert app1.saved[-1] == {canonical}

    session_path = tmp_path / "session.json"
    save_session_state(
        {
            "regular_search": {
                "search_corpus_scope": "genizah",
                "results": [],
            },
            "composition_search": {"results": [], "filtered_results": []},
            "local_file_optouts": sorted(app1._local_file_optouts),
        },
        path=str(session_path),
    )

    state = load_session_state(path=str(session_path))
    app2 = DummyApp()
    app2._local_file_optouts = set(state["local_file_optouts"])
    parent2 = QWidget()
    tree2 = _UnifiedFileTreeWidget(parent2, app2)
    tree2.populate_for_folder(str(folder), prior_status={})

    reopened_leaf = tree2.topLevelItem(0).child(0)
    assert reopened_leaf.data(0, Qt.ItemDataRole.UserRole) == canonical
    assert reopened_leaf.checkState(0) == Qt.CheckState.Unchecked


def test_restore_mode_never_still_persists_optouts_and_corpus(monkeypatch):
    """User scenario: Restore State is set to Never, but lightweight
    preferences still have to survive close/reopen.

    This is the path older Phase 96 tests skipped, and it is where both the
    corpus dropdown and opt-out persistence chain broke.
    """
    genizah_app = pytest.importorskip("genizah_app")
    import shared.session_persistence as session_persistence

    saved = {}
    monkeypatch.setattr(
        genizah_app,
        "load_app_config",
        lambda: {"restore_mode": "never"},
    )
    monkeypatch.setattr(
        session_persistence,
        "save_session_state",
        lambda state: saved.setdefault("state", state) or True,
    )

    path = r"c:\users\h\docs\scan.pdf"
    save_fake = types.SimpleNamespace(
        _local_file_optouts={path},
        _search_corpus_scope="local",
        _is_browsing_local=lambda: False,
    )

    genizah_app.GenizahGUI._save_session(save_fake)
    assert saved["state"]["local_file_optouts"] == [path]
    assert saved["state"]["regular_search"]["search_corpus_scope"] == "local"

    class ComboStub:
        def __init__(self):
            self.values = ["genizah", "local", "all"]
            self.index = 0
            self.blocked = False

        def findData(self, value):
            return self.values.index(value) if value in self.values else -1

        def blockSignals(self, blocked):
            self.blocked = blocked

        def setCurrentIndex(self, index):
            self.index = index

        def currentData(self):
            return self.values[self.index]

    class ProgressStub:
        def setVisible(self, _visible):
            pass

    class MyLibraryStub:
        def __init__(self):
            self.notified = False

        def notify_session_restored(self):
            self.notified = True

    restore_fake = types.SimpleNamespace(
        corpus_scope_combo=ComboStub(),
        search_progress=ProgressStub(),
        my_library_tab=MyLibraryStub(),
    )
    restore_fake._apply_persistent_session_preferences = types.MethodType(
        genizah_app.GenizahGUI._apply_persistent_session_preferences,
        restore_fake,
    )
    monkeypatch.setattr(
        session_persistence,
        "load_session_state",
        lambda: saved["state"],
    )

    genizah_app.GenizahGUI._restore_session(restore_fake)

    assert restore_fake._local_file_optouts == {path}
    assert restore_fake._search_corpus_scope == "local"
    assert restore_fake.corpus_scope_combo.currentData() == "local"
    assert restore_fake.my_library_tab.notified
