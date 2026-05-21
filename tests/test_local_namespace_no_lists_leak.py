# -*- coding: utf-8 -*-
"""Wave 0 stub — Phase 95 REQ-9 + D-30 Codex P0: LOCAL sys_ids must not trigger
cloud client calls in lists_sync.sync_item_to_cloud.

The gate MUST be at the TOP of sync_item_to_cloud(), BEFORE _get_client() is called.
Real implementation: lists_sync.py (Wave 1, Plan 95-04).
"""


def test_sync_item_to_cloud_zero_get_client_calls_for_local():
    """REQ-9 + D-30 Codex P0: mock _get_client and Supabase calls; pass a LOCAL
    sys_id to sync_item_to_cloud(); assert _get_client.call_count == 0.
    The gate fires before any network operation.
    """
    raise NotImplementedError(
        "Wave 0 stub for REQ-9 + D-30 lists_sync LOCAL gate — implemented in Wave 1 plan 95-04"
    )
