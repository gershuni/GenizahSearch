# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-25: single indexer mutex prevents concurrent writes.

Real implementation: desktop/my_library_tab.py QMutex (Wave 3, Plan 95-07).
"""


def test_concurrent_refresh_no_interleave():
    """D-25: spawn N concurrent Refresh/Remove requests; assert no interleaving
    in SQLite log (QMutex gates all side-index mutations).
    """
    raise NotImplementedError(
        "Wave 0 stub for D-25 indexer mutex — implemented in Wave 3 plan 95-07"
    )
