# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-39: three per-surface QSettings keys persist LOCAL filter state.

Real implementation: genizah_app.py / desktop/my_library_tab.py (Wave 4, Plan 95-08).
"""


def test_3_qsettings_keys_persist():
    """D-39: three independent QSettings keys survive app restart:
    - myLibrary/search_local_filter
    - myLibrary/composition_local_filter
    - myLibrary/parallels_local_filter
    Default value 'all'; cycle: all -> only_local -> no_local -> all.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-39 per-surface filter persistence — implemented in Wave 4 plan 95-08"
    )
