# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-40: unavailable folder behavior at app startup.

Real implementation: desktop/my_library_tab.py (Wave 3, Plan 95-07).
"""


def test_unavailable_folder_marked_status_unavailable():
    """D-40: when os.path.isdir(folder.path) is False at auto-rescan, the folder
    row is updated to status='unavailable'. Existing Tantivy docs are NOT deleted.
    Previously-indexed files remain searchable.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-40 unavailable folder — implemented in Wave 3 plan 95-07"
    )
