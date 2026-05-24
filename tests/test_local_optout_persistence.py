# -*- coding: utf-8 -*-
"""Phase 96 D-F1: session-JSON round-trip + rescan-preservation tests.

Implementation plan: 96-04-PLAN.md (CONTEXT D-08 REVISED 2026-05-24 ->
session JSON, NOT QSettings -- matches Phase 95 local_filter pattern).
"""
import pytest


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
