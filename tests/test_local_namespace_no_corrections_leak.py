# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-9: LOCAL sys_ids must not reach corrections_client.

Real implementation: corrections_client.py gate extension (Wave 1, Plan 95-04).
"""


def test_corrections_submit_returns_local_corrections_disabled():
    """REQ-9: corrections_client.submit_correction() with a LOCAL sys_id returns
    immediately (corrections disabled for LOCAL items). No Supabase call made.
    """
    raise NotImplementedError(
        "Wave 0 stub for REQ-9 corrections LOCAL gate — implemented in Wave 1 plan 95-04"
    )
