# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-10 + D-26: pre-scan ceiling enforcement.

Real implementation: desktop/my_library_tab.py (Wave 3, Plan 95-07).
"""


def test_prescan_warning_above_5000_files():
    """REQ-10 + D-26: when pre-scan counts > 5000 files, a modal warning dialog
    appears: 'Indexing N files — performance may degrade. Continue?'
    """
    raise NotImplementedError(
        "Wave 0 stub for REQ-10/D-26 prescan file-count ceiling — implemented in Wave 3 plan 95-07"
    )


def test_prescan_warning_above_2gb():
    """REQ-10 + D-41: when pre-scan total_bytes > 2 GB, a modal warning dialog
    appears showing both file count and size: 'Indexing N files (X.X GB) — ...'
    """
    raise NotImplementedError(
        "Wave 0 stub for REQ-10/D-41 prescan 2 GB ceiling — implemented in Wave 3 plan 95-07"
    )
