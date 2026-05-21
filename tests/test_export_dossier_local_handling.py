# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-45: export_dossier skip_local parameter.

Real implementation: shared/export_dossier.py (Wave 4, Plan 95-09).
"""


def test_skip_local_true_excludes_local_rows():
    """D-45: web xlsx export path sets skip_local=True; LOCAL rows are excluded
    from shared/export_dossier.py row builders.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-45 export skip_local=True — implemented in Wave 4 plan 95-09"
    )


def test_skip_local_false_includes_local_rows():
    """D-45: desktop xlsx export path sets skip_local=False; LOCAL rows ARE included.
    Source column shows 'LOCAL'; Library column shows 'My Library'.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-45 export skip_local=False — implemented in Wave 4 plan 95-09"
    )
