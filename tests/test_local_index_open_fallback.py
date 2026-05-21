# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-37: fallback when LOCAL side-index is missing/corrupt/locked.

Real implementation: genizah_core.py search-init + MyLibraryTab (Wave 2, Plan 95-05).
"""


def test_corrupt_local_index_falls_back_to_genizah_only():
    """D-37: when tantivy.Index.open(LOCAL_INDEX_DIR) raises (missing files, file lock,
    schema corruption), main search returns Genizah-only results without traceback.
    LOCAL filter button stays hidden; status bar shows 'My Library index unavailable — Rebuild?'
    """
    raise NotImplementedError(
        "Wave 0 stub for D-37 open-fallback — implemented in Wave 2 plan 95-05"
    )
