# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-17: folder overlap detection via os.path.commonpath.

Real implementation: desktop/my_library_tab.py Add Folder dialog (Wave 1, Plan 95-03).
"""


def test_overlap_via_commonpath():
    """D-17 Codex P1: reject a new folder whose resolved canonical path equals,
    is an ancestor of, or is a descendant of an existing registered folder.
    Uses Path.resolve() + os.path.normcase() + os.path.commonpath().
    Fixtures: junction-link, UNC mount, drive-letter-equivalent path, mixed-case.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-17 folder overlap detection — implemented in Wave 1 plan 95-03"
    )
