# -*- coding: utf-8 -*-
"""DiscoveryService tests (Phase 134, plan 134-06, Task 1 sync core + Task 2
async wrappers): lazy/versioned connection (F15/R8), graceful-absent reads,
pagination bounds, off-loop timeout behavior (asyncio.wait, never
wait_for), bounded heavy-query concurrency with add_done_callback slot
release (DC6), version-keyed browse-enrichment LRU (F15), and the DATA-10
unit x work projection (both as a pure helper unit test and as an
integration test against the committed 134-03 golden fixture).

Masking discipline: every value/scenario in this file either comes from the
already-committed, masking-safe synthetic fixture
(tests/fixtures/discovery/discovery-v1-fixture.db) or is freshly fabricated
in-test via scripts/build_discovery_sidecar.create_schema -- never real
research data.
"""
from __future__ import annotations

import asyncio
import os
import sqlite3
import threading
import time

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from shared.discovery_errors import DiscoveryOverload, DiscoveryUnavailable
from shared.discovery_service import DiscoveryService, _band_rank, _project_work_witnesses

FIXTURE_DB = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "tests", "fixtures", "discovery", "discovery-v1-fixture.db",
)
FIXTURE_VERSION = "discovery-v1-synthetic-fixture"


def _make_service(db_path=FIXTURE_DB, version=FIXTURE_VERSION, available=True):
    return DiscoveryService(
        path_provider=lambda: db_path,
        availability_callable=lambda: available,
        sidecar_version_provider=lambda: version,
    )


# ---------------------------------------------------------------------------
# Lazy connection (F15) -- never built in __init__, built on first use.
# ---------------------------------------------------------------------------

def test_conn_not_built_in_init():
    service = _make_service()
    assert service._conn is None, "DiscoveryService.__init__ must not build a connection (F15)"


def test_conn_built_lazily_on_first_available_call():
    service = _make_service()
    assert service._conn is None
    assert service.get_version() == FIXTURE_VERSION
    assert service._conn is not None


def test_unavailable_service_never_builds_conn_and_reads_are_empty():
    service = _make_service(available=False)
    assert service.get_version() is None
    assert service.get_claims_for_page("p001") == []
    assert service.get_pages_related_to_page("p004") == []
    assert service.get_evidence("whatever") == []
    assert service.get_work_witnesses("w000001") == []
    assert service._conn is None


def test_missing_path_provider_result_is_graceful():
    service = _make_service(db_path=None)
    assert service.get_version() is None
    assert service.get_claims_for_page("p001") == []


# ---------------------------------------------------------------------------
# Conn swap on path/version change + prior pool CLOSED (R8)
# ---------------------------------------------------------------------------

def _build_version_b_db(tmp_path):
    """A second, independently-built synthetic sidecar with a DIFFERENT
    sidecar_version -- proves a path/version swap actually serves different
    data, not just a different version string."""
    db_path = tmp_path / "discovery-v1-version-b.db"
    conn = sqlite3.connect(str(db_path))
    try:
        sidecar_build.create_schema(conn)
        sidecar_build.populate_synthetic(conn, source_db_hash="test-version-b")
        conn.execute(
            "UPDATE meta SET value = ? WHERE key = 'sidecar_version'",
            ("discovery-v1-version-b",),
        )
        conn.commit()
    finally:
        conn.close()
    return str(db_path)


def test_conn_swapped_on_version_change_and_old_pool_closed(tmp_path):
    db_b_path = _build_version_b_db(tmp_path)
    state = {"path": FIXTURE_DB, "version": FIXTURE_VERSION}
    service = DiscoveryService(
        path_provider=lambda: state["path"],
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: state["version"],
    )

    assert service.get_version() == FIXTURE_VERSION
    old_conn = service._conn
    assert old_conn is not None
    assert old_conn._conns, "expected at least one per-thread connection registered"

    # Swap BOTH path and version -- simulates a rebuild + restart pointed at
    # a new sidecar release.
    state["path"] = db_b_path
    state["version"] = "discovery-v1-version-b"

    assert service.get_version() == "discovery-v1-version-b"
    new_conn = service._conn
    assert new_conn is not old_conn, "a version/path change must rebuild the connection pool"
    assert old_conn._conns == {}, (
        "the PRIOR ThreadLocalConnection pool must be .close()d on swap -- no leaked handles (R8)"
    )


def test_conn_not_rebuilt_when_path_and_version_are_unchanged():
    service = _make_service()
    assert service.get_version() == FIXTURE_VERSION
    first_conn = service._conn
    assert service.get_claims_for_page("p001") is not None
    assert service._conn is first_conn, "an unchanged path/version must NOT rebuild the connection"


# ---------------------------------------------------------------------------
# Sync read shape against the fixture (display-evidence band selection,
# bidirectional shared_text lookup, JSON seed_spans parsing)
# ---------------------------------------------------------------------------

def test_get_claims_for_page_uses_display_evidence_band():
    service = _make_service()
    claims = service.get_claims_for_page("p005")
    assert len(claims) == 1
    # p005/w000004: human_confirmed screening_canon dominance wins over corroborated.
    assert claims[0]["confidence_band"] == "screening_canon"
    assert claims[0]["adjudication_status"] == "human_confirmed"


def test_get_claims_for_page_multi_work_per_ms_preserved():
    service = _make_service()
    claims = service.get_claims_for_page("p012")
    assert len(claims) == 2
    work_ids = {c["work_id"] for c in claims}
    assert work_ids == {"w000003", "w000004"}


def test_get_pages_related_to_page_is_bidirectional():
    service = _make_service()
    from_a = service.get_pages_related_to_page("p004")
    from_b = service.get_pages_related_to_page("p104")
    assert len(from_a) == 1
    assert from_a[0]["related_page_id"] == "p104"
    assert len(from_b) == 1
    assert from_b[0]["related_page_id"] == "p004"


def test_get_claims_for_page_hides_review_only_by_default_and_opt_in_reveals_it():
    # p010/w000008 is the family-router (review_only) claim (C10 in the
    # synthetic fixture) -- its sole evidence row IS the display_evidence_id,
    # so the whole claim must be invisible by default (L1) and reachable
    # only via the explicit include_review opt-in.
    service = _make_service()
    default_claims = service.get_claims_for_page("p010")
    assert default_claims == []

    review_claims = service.get_claims_for_page("p010", include_review=True)
    assert len(review_claims) == 1
    assert review_claims[0]["routing_status"] == "review_only"
    assert review_claims[0]["work_id"] == "w000008"


def test_get_pages_related_to_page_hides_review_only_by_default_and_opt_in_reveals_it():
    service = _make_service()
    default_related = service.get_pages_related_to_page("p010")
    assert default_related == []

    review_related = service.get_pages_related_to_page("p010", include_review=True)
    assert len(review_related) == 1
    assert review_related[0]["routing_status"] == "review_only"


def test_get_evidence_parses_seed_spans_json():
    service = _make_service()
    claim_id = "e843bafbcb1ec1b85cf641899775fa6fcca405bfc497c210c547effbcd7840e0"  # p002/w000002
    rows = service.get_evidence(claim_id)
    corroborated = [r for r in rows if r["confidence_band"] == "corroborated"][0]
    assert isinstance(corroborated["seed_spans"], list)
    assert len(corroborated["seed_spans"]) == 2  # R4 multi-occurrence row
    assert corroborated["seed_spans"][0]["occ_class"] in ("core", "flank")


# ---------------------------------------------------------------------------
# Pagination bounds (server-side LIMIT/OFFSET everywhere)
# ---------------------------------------------------------------------------

def test_get_claims_for_page_pagination_bounds():
    service = _make_service()
    page1 = service.get_claims_for_page("p012", page=1, page_size=1)
    page2 = service.get_claims_for_page("p012", page=2, page_size=1)
    page3 = service.get_claims_for_page("p012", page=3, page_size=1)
    assert len(page1) == 1
    assert len(page2) == 1
    assert page1[0]["work_id"] != page2[0]["work_id"]
    assert page3 == []


def test_get_evidence_pagination_bounds():
    service = _make_service()
    claim_id = "e843bafbcb1ec1b85cf641899775fa6fcca405bfc497c210c547effbcd7840e0"
    ev_all = service.get_evidence(claim_id, page=1, page_size=200)
    assert len(ev_all) == 2
    ev_page1 = service.get_evidence(claim_id, page=1, page_size=1)
    ev_page2 = service.get_evidence(claim_id, page=2, page_size=1)
    assert len(ev_page1) == 1
    assert len(ev_page2) == 1
    assert ev_page1[0]["evidence_id"] != ev_page2[0]["evidence_id"]


def test_get_work_witnesses_pagination_bounds():
    service = _make_service()
    page1 = service.get_work_witnesses("w000005", page=1, page_size=1)
    page2 = service.get_work_witnesses("w000005", page=2, page_size=1)
    all_items = service.get_work_witnesses("w000005", page=1, page_size=200)
    assert len(page1) == 1
    assert len(page2) == 1
    assert len(all_items) == 2
    assert page1 + page2 == all_items


def test_page_size_max_is_enforced(monkeypatch):
    monkeypatch.setenv("DISCOVERY_PAGE_SIZE_MAX", "1")
    service = _make_service()
    items = service.get_work_witnesses("w000005", page=1, page_size=999)
    assert len(items) == 1  # clamped to the (monkeypatched) hard max


# ---------------------------------------------------------------------------
# DATA-10 unit x work projection -- pure helper (no DB) proving the rules
# directly, incl. the "highest band wins across DIFFERING member bands"
# case the shared fixture itself cannot exercise (both its merged units
# happen to share one band per member).
# ---------------------------------------------------------------------------

def test_data10_highest_band_wins_within_unit_when_members_differ():
    claim_rows = [
        {"page_id": "pA", "work_id": "wX", "claim_id": "cA", "claim_type": "direct_witness",
         "sys_id": "sysA", "evidence_source": "track1_direct", "confidence_band": "screening_rb"},
        {"page_id": "pB", "work_id": "wX", "claim_id": "cB", "claim_type": "direct_witness",
         "sys_id": "sysB", "evidence_source": "track1_direct", "confidence_band": "expert_verified"},
    ]
    unit_by_sys = {"sysA": "unitX", "sysB": "unitX"}
    items = _project_work_witnesses(claim_rows, unit_by_sys)
    assert len(items) == 1  # merged into ONE row
    assert items[0]["confidence_band"] == "expert_verified"  # the HIGHEST member band wins
    assert items[0]["unit_id"] == "unitX"
    assert items[0]["representative_sys_id"] == "sysB"
    assert set(items[0]["member_sys_ids"]) == {"sysA", "sysB"}


def test_data10_anchor_unit_excluded():
    claim_rows = [
        {"page_id": "pA", "work_id": "wX", "claim_id": "cA", "claim_type": "direct_witness",
         "sys_id": "sysA", "evidence_source": "track1_direct", "confidence_band": "tier_a"},
        {"page_id": "pB", "work_id": "wX", "claim_id": "cB", "claim_type": "direct_witness",
         "sys_id": "sysB", "evidence_source": "propagated", "confidence_band": "weak"},
    ]
    items = _project_work_witnesses(claim_rows, {}, anchor_sys_id="sysA")
    assert len(items) == 1
    assert items[0]["representative_sys_id"] == "sysB"


def test_data10_anchor_unit_excludes_whole_merged_unit_not_just_the_anchor_sys_id():
    claim_rows = [
        {"page_id": "pA", "work_id": "wX", "claim_id": "cA", "claim_type": "direct_witness",
         "sys_id": "sysA", "evidence_source": "track1_direct", "confidence_band": "tier_a"},
        {"page_id": "pB", "work_id": "wX", "claim_id": "cB", "claim_type": "direct_witness",
         "sys_id": "sysB", "evidence_source": "track1_direct", "confidence_band": "tier_a"},
        {"page_id": "pC", "work_id": "wX", "claim_id": "cC", "claim_type": "direct_witness",
         "sys_id": "sysC", "evidence_source": "propagated", "confidence_band": "corroborated"},
    ]
    unit_by_sys = {"sysA": "unitX", "sysB": "unitX"}
    # Anchor is sysB (a MEMBER of unitX, not the representative) -- the WHOLE
    # unit (sysA+sysB) must be excluded, not just sysB.
    items = _project_work_witnesses(claim_rows, unit_by_sys, anchor_sys_id="sysB")
    assert len(items) == 1
    assert items[0]["representative_sys_id"] == "sysC"


def test_data10_enabled_bands_filters_before_pagination():
    claim_rows = [
        {"page_id": "pA", "work_id": "wX", "claim_id": "cA", "claim_type": "direct_witness",
         "sys_id": "sysA", "evidence_source": "track1_direct", "confidence_band": "tier_a"},
        {"page_id": "pB", "work_id": "wX", "claim_id": "cB", "claim_type": "direct_witness",
         "sys_id": "sysB", "evidence_source": "propagated", "confidence_band": "weak"},
    ]
    items = _project_work_witnesses(claim_rows, {}, enabled_bands={"weak"}, page=1, page_size=50)
    assert len(items) == 1
    assert items[0]["confidence_band"] == "weak"

    # Filter + tiny page_size together: filter happens BEFORE pagination, so
    # a page_size big enough to hold everything still returns only the
    # filtered set (never accidentally re-admits the filtered-out tier_a row).
    items_paginated = _project_work_witnesses(claim_rows, {}, enabled_bands={"weak"}, page=1, page_size=1)
    assert len(items_paginated) == 1
    assert items_paginated[0]["confidence_band"] == "weak"


def test_data10_same_unit_members_suppressed_to_one_row():
    claim_rows = [
        {"page_id": "pA", "work_id": "wX", "claim_id": "cA", "claim_type": "direct_witness",
         "sys_id": "sysA", "evidence_source": "track1_direct", "confidence_band": "tier_a"},
        {"page_id": "pB", "work_id": "wX", "claim_id": "cB", "claim_type": "direct_witness",
         "sys_id": "sysB", "evidence_source": "track1_direct", "confidence_band": "tier_a"},
        {"page_id": "pC", "work_id": "wX", "claim_id": "cC", "claim_type": "direct_witness",
         "sys_id": "sysC", "evidence_source": "propagated", "confidence_band": "corroborated"},
    ]
    unit_by_sys = {"sysA": "unitX", "sysB": "unitX"}
    items = _project_work_witnesses(claim_rows, unit_by_sys)
    assert len(items) == 2  # unitX (ONE row, not two) + sysC


def test_data10_empty_claim_rows_returns_empty():
    assert _project_work_witnesses([], {}) == []


# --- DATA-10 integration against the shared golden fixture ---

def test_data10_integration_oxford_part_unit_projection():
    service = _make_service()
    items = service.get_work_witnesses("w000005")
    assert len(items) == 2  # the oxford_part unit (p013+p014) + standalone p007
    unit_items = [it for it in items if it["unit_id"] is not None]
    singleton_items = [it for it in items if it["unit_id"] is None]
    assert len(unit_items) == 1
    assert unit_items[0]["confidence_band"] == "tier_a"
    assert set(unit_items[0]["member_sys_ids"]) == {"990000000000000013", "990000000000000014"}
    assert len(singleton_items) == 1
    assert singleton_items[0]["confidence_band"] == "screening_canon"


def test_data10_integration_physical_join_unit_projection():
    service = _make_service()
    items = service.get_work_witnesses("w000006")
    assert len(items) == 2  # the physical_join unit (p015+p016) + standalone p008
    unit_items = [it for it in items if it["unit_id"] is not None]
    assert len(unit_items) == 1
    assert unit_items[0]["confidence_band"] == "screening_rb"
    assert set(unit_items[0]["member_sys_ids"]) == {"990000000000000015", "990000000000000016"}


def test_data10_integration_anchor_excludes_own_unit():
    service = _make_service()
    items = service.get_work_witnesses("w000005", anchor_sys_id="990000000000000013")
    assert len(items) == 1
    assert items[0]["unit_id"] is None
    assert items[0]["representative_sys_id"] == "990000000000000007"


def test_data10_integration_same_scribe_pair_never_merged():
    service = _make_service()
    # p017 (w000007) and p018 (w000008) are a deliberate "same scribe" pair
    # that DATA-10 forbids merging -- each must surface as its OWN unmerged
    # singleton, never grouped together (they aren't even the same work, but
    # this also proves no stray witness_units row exists for them).
    items_w7 = service.get_work_witnesses("w000007")
    items_w8 = service.get_work_witnesses("w000008")
    sys017_items = [it for it in items_w7 if it["representative_sys_id"] == "990000000000000017"]
    assert len(sys017_items) == 1
    assert sys017_items[0]["unit_id"] is None
    sys018_items = [it for it in items_w8 if it["representative_sys_id"] == "990000000000000018"]
    assert len(sys018_items) == 1
    assert sys018_items[0]["unit_id"] is None


def test_data10_integration_excludes_shared_text_only_family_router_claim():
    service = _make_service()
    # p010/w000008 is claim_type=shared_text (family-router, review_only) --
    # it must NOT appear as a "witness" of w000008; only p018 (direct_witness) should.
    items = service.get_work_witnesses("w000008")
    assert len(items) == 1
    assert items[0]["representative_sys_id"] == "990000000000000018"
    assert items[0]["confidence_band"] == "weak"


# ---------------------------------------------------------------------------
# Async off-loop dispatch: asyncio.wait (never wait_for), timeout -> unavailable
# ---------------------------------------------------------------------------

def test_timeout_returns_unavailable_and_loop_stays_responsive(monkeypatch):
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_BROWSE", "0.05")
    service = _make_service()
    block_event = threading.Event()

    def _slow_get_claims_for_page(page_id, page, page_size):
        block_event.wait(timeout=5)
        return []

    service.get_claims_for_page = _slow_get_claims_for_page

    async def _run():
        other_ran = {"flag": False}

        async def _other_task():
            await asyncio.sleep(0.01)
            other_ran["flag"] = True

        task = asyncio.create_task(_other_task())
        start = time.monotonic()
        with pytest.raises(DiscoveryUnavailable):
            await service.get_claims_for_page_async("p001")
        elapsed = time.monotonic() - start
        await task
        assert other_ran["flag"] is True, "the event loop must stay responsive during the timeout wait"
        assert elapsed < 1.0, "a timed-out query must fail fast, not hang for the full stuck duration"

    try:
        asyncio.run(_run())
    finally:
        block_event.set()  # release the stuck background thread so it doesn't leak
        time.sleep(0.1)


async def _async_get_version_smoke():
    service = _make_service()
    return await service.get_version_async()


def test_get_version_async_smoke():
    assert asyncio.run(_async_get_version_smoke()) == FIXTURE_VERSION


# ---------------------------------------------------------------------------
# Heavy-query bounded concurrency (non-blocking semaphore -> DiscoveryOverload)
# ---------------------------------------------------------------------------

def test_overload_returns_unavailable_immediately(monkeypatch):
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_QUERIES", "1")
    service = _make_service()

    async def _run():
        await service._heavy_sem.acquire()  # simulate the single slot already held
        try:
            start = time.monotonic()
            with pytest.raises(DiscoveryOverload):
                await service.get_work_witnesses_async("w000001")
            elapsed = time.monotonic() - start
            assert elapsed < 1.0, "overload must fail fast (non-blocking acquire), never wait"
        finally:
            service._heavy_sem.release()

    asyncio.run(_run())


def test_timed_out_heavy_slot_not_recycled_until_thread_finishes(monkeypatch):
    """DC6: a timed-out heavy query's slot must NOT be released until the
    underlying (uncancellable) thread actually finishes -- proven by a
    THIRD call only succeeding after the stuck thread completes."""
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_QUERIES", "1")
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_WORK", "0.05")
    service = _make_service()
    block_event = threading.Event()
    call_count = {"n": 0}

    def _slow_get_work_witnesses(work_id, enabled_bands, page, page_size, anchor_sys_id):
        call_count["n"] += 1
        if call_count["n"] == 1:
            block_event.wait(timeout=5)
        return []

    service.get_work_witnesses = _slow_get_work_witnesses

    async def _run():
        # Call 1: acquires the sole slot, dispatches to the executor, times
        # out at the loop level while the underlying thread keeps blocking.
        with pytest.raises(DiscoveryUnavailable):
            await service.get_work_witnesses_async("w000001")
        assert call_count["n"] == 1

        # Call 2: attempted BEFORE the stuck thread finishes -- the slot is
        # still held, so this must overload immediately, WITHOUT spawning a
        # second executor call.
        with pytest.raises(DiscoveryUnavailable):
            await service.get_work_witnesses_async("w000001")
        assert call_count["n"] == 1, "a still-held slot must not admit a second concurrent call"

        # Let the stuck thread finish -- the slot is released from its
        # future's add_done_callback.
        block_event.set()
        for _ in range(50):
            if not service._heavy_sem.locked():
                break
            await asyncio.sleep(0.05)
        assert not service._heavy_sem.locked(), (
            "the heavy slot was never released after the stuck thread finished (DC6)"
        )

        # Call 3: the slot is free now -- succeeds.
        result = await service.get_work_witnesses_async("w000001")
        assert result == []
        assert call_count["n"] == 2

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# Version-keyed browse-enrichment LRU (F15)
# ---------------------------------------------------------------------------

def test_browse_lru_hit_avoids_second_sync_call():
    service = _make_service()
    calls = {"n": 0}
    real_fn = service.get_claims_for_page

    def _counting(page_id, page, page_size):
        calls["n"] += 1
        return real_fn(page_id, page, page_size)

    service.get_claims_for_page = _counting

    async def _run():
        r1 = await service.get_claims_for_page_async("p001")
        r2 = await service.get_claims_for_page_async("p001")
        assert calls["n"] == 1, "the second identical call must be served from the LRU cache"
        assert r1 == r2

    asyncio.run(_run())


def test_browse_lru_version_bump_invalidates_cache():
    state = {"version": FIXTURE_VERSION}
    service = DiscoveryService(
        path_provider=lambda: FIXTURE_DB,
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: state["version"],
    )
    calls = {"n": 0}
    real_fn = service.get_claims_for_page

    def _counting(page_id, page, page_size):
        calls["n"] += 1
        return real_fn(page_id, page, page_size)

    service.get_claims_for_page = _counting

    async def _run():
        await service.get_claims_for_page_async("p001")
        await service.get_claims_for_page_async("p001")
        assert calls["n"] == 1

        state["version"] = "discovery-v1-different-version"
        await service.get_claims_for_page_async("p001")
        assert calls["n"] == 2, "a sidecar version bump must invalidate the previously-cached key"

    asyncio.run(_run())


def test_browse_lru_bounded_by_max_entries(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", "2")
    service = _make_service()

    async def _run():
        await service.get_claims_for_page_async("p001")
        await service.get_claims_for_page_async("p002")
        await service.get_claims_for_page_async("p003")
        assert len(service._browse_lru) <= 2, "the browse LRU must stay bounded to its max-entries cap"

    asyncio.run(_run())


# ---------------------------------------------------------------------------
# M3: env-var clamps -- a misconfigured value can never widen a frozen
# ceiling, crash construction, or disable a bound (it must always fall
# back to a sane default, or -- for the LRU size -- disable-and-clear).
# ---------------------------------------------------------------------------

def test_page_size_max_env_cannot_raise_absolute_ceiling(monkeypatch):
    monkeypatch.setenv("DISCOVERY_PAGE_SIZE_MAX", "999999")
    assert DiscoveryService._clamp_page_size(999999) <= 200
    service = _make_service()
    items = service.get_work_witnesses("w000005", page=1, page_size=999999)
    assert len(items) <= 200


def test_page_size_max_env_non_positive_falls_back_to_ceiling(monkeypatch):
    monkeypatch.setenv("DISCOVERY_PAGE_SIZE_MAX", "0")
    assert DiscoveryService._clamp_page_size(50) == 50
    monkeypatch.setenv("DISCOVERY_PAGE_SIZE_MAX", "-10")
    assert DiscoveryService._clamp_page_size(50) == 50


def test_concurrency_env_negative_falls_back_to_default_without_crashing(monkeypatch):
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_QUERIES", "-3")
    # asyncio.Semaphore(negative) raises ValueError -- construction must not
    # crash on a misconfigured env var (M3).
    service = _make_service()
    assert service._heavy_capacity >= 1


def test_concurrency_env_zero_falls_back_to_default_without_crashing(monkeypatch):
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_QUERIES", "0")
    service = _make_service()
    assert service._heavy_capacity >= 1


def test_timeout_env_non_positive_falls_back_to_default():
    import os

    os.environ["DISCOVERY_QUERY_TIMEOUT_BROWSE"] = "0"
    try:
        service = _make_service()
        assert service._browse_timeout() > 0
    finally:
        del os.environ["DISCOVERY_QUERY_TIMEOUT_BROWSE"]

    os.environ["DISCOVERY_QUERY_TIMEOUT_WORK"] = "-5"
    try:
        service = _make_service()
        assert service._work_timeout() > 0
    finally:
        del os.environ["DISCOVERY_QUERY_TIMEOUT_WORK"]


def test_browse_lru_non_positive_max_entries_disables_and_clears_cache(monkeypatch):
    service = _make_service()

    async def _run():
        await service.get_claims_for_page_async("p001")
        assert len(service._browse_lru) == 1

        monkeypatch.setenv("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", "0")
        await service.get_claims_for_page_async("p002")
        # M3: a non-positive size disables AND clears -- never unbounded.
        assert service._browse_lru == {}

        # Confirms caching stays OFF (never re-populated) while disabled.
        await service.get_claims_for_page_async("p003")
        assert service._browse_lru == {}

    asyncio.run(_run())


def test_band_rank_orders_strongest_first():
    assert _band_rank("track1_direct", "expert_verified") < _band_rank("track1_direct", "tier_a")
    assert _band_rank("track1_direct", "tier_a") < _band_rank("propagated", "corroborated")
    assert _band_rank("propagated", "corroborated") < _band_rank("track1_direct", "screening_rb")
    assert _band_rank("track1_direct", "screening_rb") < _band_rank("track1_direct", "screening_canon")
    assert _band_rank("track1_direct", "screening_canon") < _band_rank("propagated", "weak")
    assert _band_rank("propagated", "weak") < _band_rank("propagated", "not_evaluated")
