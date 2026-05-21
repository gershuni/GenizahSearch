# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-9: LOCAL sys_ids must not appear in /api/search payload.

Real implementation: shared/search_serializer.py _serialize_item (Wave 1, Plan 95-04).
"""


def test_serialize_search_payload_drops_local():
    """REQ-9: _serialize_item filters out items with is_local_sys_id(sys_id) == True.
    Asserts the serialized JSON payload for a LOCAL hit is absent from the output.
    """
    raise NotImplementedError(
        "Wave 0 stub for REQ-9 /api/search LOCAL filter — implemented in Wave 1 plan 95-04"
    )
