# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 D-08: LOCAL hits must merge AFTER _deduplicate().

Real implementation: genizah_core.py (Wave 2, Plan 95-05).
"""


def test_local_hit_before_dedup_dropped():
    """D-08 Codex P0: a LOCAL hit injected BEFORE _deduplicate() is dropped
    because _deduplicate() only passes V0.8/V0.7 sources through.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-08 post-dedup merge (before → dropped) — implemented in Wave 2 plan 95-05"
    )


def test_local_hit_after_dedup_survives():
    """D-08 Codex P0: a LOCAL hit injected AFTER _deduplicate() survives
    in the final result list.
    """
    raise NotImplementedError(
        "Wave 0 stub for D-08 post-dedup merge (after → survives) — implemented in Wave 2 plan 95-05"
    )
