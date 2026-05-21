# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-42: _canonical_filepath() helper for sys_id generation.

Real implementation: shared/local_indexer.py (Wave 1, Plan 95-02).
"""


def test_unc_path():
    """D-42: UNC path (\\\\server\\share\\file.pdf) normalizes consistently."""
    raise NotImplementedError(
        "Wave 0 stub for D-42 canonical filepath (UNC) — implemented in Wave 1 plan 95-02"
    )


def test_junction():
    """D-42: junction-linked folder resolves to the real path (or normcase equivalent)."""
    raise NotImplementedError(
        "Wave 0 stub for D-42 canonical filepath (junction) — implemented in Wave 1 plan 95-02"
    )


def test_drive_letter_casing():
    """D-42: C:\\foo and c:\\foo produce the same canonical path on Windows (normcase)."""
    raise NotImplementedError(
        "Wave 0 stub for D-42 canonical filepath (drive-letter casing) — implemented in Wave 1 plan 95-02"
    )
