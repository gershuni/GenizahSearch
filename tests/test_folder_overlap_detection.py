# -*- coding: utf-8 -*-
"""Phase 95 D-17: folder overlap detection via os.path.commonpath.

Tests check_folder_overlap() with ancestor, descendant, exact-match,
and sibling cases. Windows-specific junction/UNC tests are skipped on
non-Windows platforms.
"""
import os
import sys

import pytest

from shared.local_indexer import check_folder_overlap


def test_overlap_via_commonpath(tmp_path):
    """D-17 Codex P1: reject a new folder whose resolved canonical path equals,
    is an ancestor of, or is a descendant of an existing registered folder.
    Uses Path.resolve() + os.path.normcase() + os.path.commonpath().
    """
    # Create real dirs so resolve() has something to work with
    base = tmp_path / "a" / "b"
    base.mkdir(parents=True)
    sibling = tmp_path / "a" / "b2"
    sibling.mkdir(parents=True)
    descendant = base / "c"
    descendant.mkdir()
    ancestor = tmp_path / "a"

    registered = str(base)

    # 1. Exact match — REJECT (already registered)
    conflict = check_folder_overlap(str(base), [registered])
    assert conflict is not None, "Exact match should be rejected"

    # 2. Descendant — REJECT (registered folder is ancestor of candidate)
    conflict = check_folder_overlap(str(descendant), [registered])
    assert conflict is not None, f"Descendant should be rejected, got: {conflict}"

    # 3. Ancestor — REJECT (registered folder is descendant of candidate)
    conflict = check_folder_overlap(str(ancestor), [registered])
    assert conflict is not None, f"Ancestor should be rejected, got: {conflict}"

    # 4. Sibling — ACCEPT (no overlap)
    conflict = check_folder_overlap(str(sibling), [registered])
    assert conflict is None, f"Sibling should be accepted, got conflict: {conflict}"


def test_overlap_empty_existing(tmp_path):
    """No registered folders — any candidate is accepted."""
    candidate = str(tmp_path / "new_folder")
    result = check_folder_overlap(candidate, [])
    assert result is None


def test_overlap_multiple_registered(tmp_path):
    """Overlap detection works when multiple folders are registered."""
    folder_a = tmp_path / "a"
    folder_b = tmp_path / "b"
    folder_a.mkdir()
    folder_b.mkdir()

    existing = [str(folder_a), str(folder_b)]

    # Sub-folder of folder_a — REJECT
    sub_a = folder_a / "sub"
    sub_a.mkdir()
    conflict = check_folder_overlap(str(sub_a), existing)
    assert conflict is not None, "Sub-folder of registered folder should be rejected"
    assert os.path.normcase(conflict) == os.path.normcase(str(folder_a))

    # Sibling of both — ACCEPT
    folder_c = tmp_path / "c"
    folder_c.mkdir()
    conflict = check_folder_overlap(str(folder_c), existing)
    assert conflict is None, "Unrelated sibling should be accepted"


@pytest.mark.skipif(sys.platform != "win32", reason="Case-insensitive path test (Windows only)")
def test_overlap_case_insensitive_windows(tmp_path):
    """D-17 + D-42: Windows path normalization — mixed-case paths overlap correctly."""
    base = tmp_path / "FolderA"
    base.mkdir()
    registered = str(base)

    # Same path with different case — should REJECT
    lower_path = str(base).lower()
    upper_path = str(base).upper()

    conflict = check_folder_overlap(lower_path, [registered])
    assert conflict is not None, f"Lowercase version should conflict with '{registered}'"

    conflict = check_folder_overlap(upper_path, [registered])
    assert conflict is not None, f"Uppercase version should conflict with '{registered}'"
