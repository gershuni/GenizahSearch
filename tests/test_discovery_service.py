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


# ---------------------------------------------------------------------------
# H1: get_work_witnesses must never silently drop units on a work with more
# raw claims than the old (removed) 5000-row pre-projection cap -- the full
# unit set must be reachable across pages via server-side SQL pagination
# over the GROUPED units, not the raw claim rows.
# ---------------------------------------------------------------------------

def _build_large_single_work_db(tmp_path, *, n_claims):
    """A synthetic sidecar with ONE work carrying `n_claims` witness claims,
    each on its own page/sys_id and deliberately left UNMERGED (no
    witness_units row at all) -- so unit count == claim count exactly,
    letting a dropped-unit bug surface as a simple missing-count assertion.
    `n_claims` is chosen by the caller to exceed the old
    _MAX_RAW_CLAIMS_PER_WORK=5000 cap this test guards against (H1)."""
    work_id = "w999999"
    evidence_specs = []
    for i in range(n_claims):
        page_id = f"lp{i:06d}"
        sys_id = f"9900000000000{i:05d}"
        evidence_specs.append(sidecar_build._mk_evidence(
            page_id=page_id, work_id=work_id, sys_id=sys_id,
            evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
            confidence_band=sidecar_build._TIER_A,
            adjudication_status=sidecar_build._UNREVIEWED, audit_status=sidecar_build._NA,
            routing_status=sidecar_build._SHIPPED, routing_reason=sidecar_build._NONE_REASON,
            span_start=0, span_end=10,
        ))
    result = sidecar_build.assemble_claims_and_evidence(
        evidence_specs, {work_id: "sefaria"}, sidecar_version="test-large-work",
    )

    db_path = tmp_path / "discovery-large-work.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, source_corpus) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (work_id, work_id, "Synthetic Large Work", None, None, "sefaria"),
        )
        sidecar_build._insert_claims_and_evidence_real(cur, result["claim_rows"], result["evidence_rows"])
        meta_rows = [
            ("schema_version", "discovery-v1"), ("sidecar_version", "test-large-work"),
            ("source_db_sha256", "test"), ("build_date", "2026-01-01T00:00:00Z"),
            ("data_as_of", "2026-01-01"), ("htr_snapshot_hash", "test"),
            ("expected_rows_claims", str(n_claims)), ("expected_rows_evidence", str(n_claims)),
            ("expected_rows_works", "1"), ("expected_rows_units", "0"),
            ("frame_content_hash", "test"),
        ]
        cur.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta_rows)
        conn.commit()
    finally:
        conn.close()
    return str(db_path), work_id


def test_get_work_witnesses_no_truncation_beyond_old_5000_claim_cap(tmp_path):
    n_claims = 5200  # > the old (removed) _MAX_RAW_CLAIMS_PER_WORK = 5000
    db_path, work_id = _build_large_single_work_db(tmp_path, n_claims=n_claims)
    service = DiscoveryService(
        path_provider=lambda: db_path,
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: "test-large-work",
    )

    seen_representative_sys_ids = set()
    page = 1
    page_size = 200  # the frozen absolute page-size ceiling
    while True:
        items = service.get_work_witnesses(work_id, page=page, page_size=page_size)
        if not items:
            break
        for it in items:
            assert it["unit_id"] is None  # every sys_id here is an unmerged singleton
            seen_representative_sys_ids.add(it["representative_sys_id"])
        page += 1
        assert page < 100, "pagination did not terminate -- possible infinite loop"

    assert len(seen_representative_sys_ids) == n_claims, (
        f"expected all {n_claims} unmerged singleton units reachable across pages, got "
        f"{len(seen_representative_sys_ids)} -- units silently dropped (H1 regression)"
    )


def test_band_rank_orders_strongest_first():
    assert _band_rank("track1_direct", "expert_verified") < _band_rank("track1_direct", "tier_a")
    assert _band_rank("track1_direct", "tier_a") < _band_rank("propagated", "corroborated")
    assert _band_rank("propagated", "corroborated") < _band_rank("track1_direct", "screening_rb")
    assert _band_rank("track1_direct", "screening_rb") < _band_rank("track1_direct", "screening_canon")
    assert _band_rank("track1_direct", "screening_canon") < _band_rank("propagated", "weak")
    assert _band_rank("propagated", "weak") < _band_rank("propagated", "not_evaluated")


# ---------------------------------------------------------------------------
# MED (Codex R2): get_work_witnesses' ROW_NUMBER() OVER (PARTITION BY
# unit_key ORDER BY band_rank ASC, sys_id ASC) is not a TOTAL order when a
# unit/sys_id carries >=2 same-band page claims (2,829 tied units observed
# in the cited real-corpus large work) -- the representative must be
# deterministic (page_id, claim_id secondary tie-breakers), never dependent
# on scan/insertion order. _project_work_witnesses must agree with the SQL.
# ---------------------------------------------------------------------------

def _build_two_page_tied_unit_db(tmp_path, *, name, page_ids):
    """A synthetic sidecar with ONE work carrying TWO witness claims that
    share the SAME sys_id (an unmerged singleton unit) and the SAME
    (evidence_source, confidence_band) -- a genuine band_rank tie within
    ONE unit_key partition, differing only by page_id/claim_id. `page_ids`
    controls the evidence_specs (and therefore INSERT) order, so a test
    can prove the chosen representative is independent of that order."""
    work_id = "w778899"
    sys_id = "9900000000000777001"
    evidence_specs = [
        sidecar_build._mk_evidence(
            page_id=page_id, work_id=work_id, sys_id=sys_id,
            evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
            confidence_band=sidecar_build._TIER_A,
            adjudication_status=sidecar_build._UNREVIEWED, audit_status=sidecar_build._NA,
            routing_status=sidecar_build._SHIPPED, routing_reason=sidecar_build._NONE_REASON,
            span_start=0, span_end=10,
        )
        for page_id in page_ids
    ]
    result = sidecar_build.assemble_claims_and_evidence(
        evidence_specs, {work_id: "sefaria"}, sidecar_version="test-tied-unit",
    )

    db_path = tmp_path / name
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, genre, source_corpus) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (work_id, work_id, "Synthetic Tied Unit Work", None, None, "sefaria"),
        )
        sidecar_build._insert_claims_and_evidence_real(cur, result["claim_rows"], result["evidence_rows"])
        meta_rows = [
            ("schema_version", "discovery-v1"), ("sidecar_version", "test-tied-unit"),
            ("source_db_sha256", "test"), ("build_date", "2026-01-01T00:00:00Z"),
            ("data_as_of", "2026-01-01"), ("htr_snapshot_hash", "test"),
            ("expected_rows_claims", "2"), ("expected_rows_evidence", "2"),
            ("expected_rows_works", "1"), ("expected_rows_units", "0"),
            ("frame_content_hash", "test"),
        ]
        cur.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta_rows)
        conn.commit()
    finally:
        conn.close()
    return str(db_path), work_id


def test_get_work_witnesses_tied_same_band_representative_is_stable_across_insertion_order(tmp_path):
    db_asc, work_id = _build_two_page_tied_unit_db(
        tmp_path, name="tied-asc.db", page_ids=["lp000A", "lp000B"],
    )
    db_desc, _ = _build_two_page_tied_unit_db(
        tmp_path, name="tied-desc.db", page_ids=["lp000B", "lp000A"],
    )

    service_asc = DiscoveryService(
        path_provider=lambda: db_asc, availability_callable=lambda: True,
        sidecar_version_provider=lambda: "test-tied-unit",
    )
    service_desc = DiscoveryService(
        path_provider=lambda: db_desc, availability_callable=lambda: True,
        sidecar_version_provider=lambda: "test-tied-unit",
    )

    items_asc = service_asc.get_work_witnesses(work_id)
    items_desc = service_desc.get_work_witnesses(work_id)

    # Both claims share one sys_id and are unmerged -- they collapse into
    # exactly ONE unit regardless of insertion order.
    assert len(items_asc) == 1
    assert len(items_desc) == 1
    # The deterministic tie-break (page_id ASC) must pick the SAME
    # representative page_id in BOTH databases, even though db_desc
    # inserted the rows in the opposite order -- proving the choice does
    # not depend on scan/insertion order (the pre-fix regression).
    assert items_asc[0]["representative_page_id"] == "lp000A"
    assert items_desc[0]["representative_page_id"] == "lp000A"
    # Stable across repeated calls too.
    assert service_asc.get_work_witnesses(work_id)[0]["representative_page_id"] == "lp000A"
    assert service_desc.get_work_witnesses(work_id)[0]["representative_page_id"] == "lp000A"


def test_project_work_witnesses_tied_same_band_representative_is_stable_across_input_order():
    """Mirrors the SQL test above at the pure-Python reference-implementation
    level: page_id (NOT claim_id) is the primary tie-breaker after
    band_rank/sys_id, deliberately using claim_id values that would pick
    the OPPOSITE winner if claim_id were compared first -- proving the two
    implementations use the SAME tie-break key order."""
    rows_a = [
        {"page_id": "lp000A", "work_id": "w1", "claim_id": "zzz_high_claim_id",
         "claim_type": "direct_witness", "sys_id": "s1",
         "evidence_source": "track1_direct", "confidence_band": "tier_a"},
        {"page_id": "lp000B", "work_id": "w1", "claim_id": "aaa_low_claim_id",
         "claim_type": "direct_witness", "sys_id": "s1",
         "evidence_source": "track1_direct", "confidence_band": "tier_a"},
    ]
    rows_b = list(reversed(rows_a))

    items_a = _project_work_witnesses(rows_a, unit_by_sys={})
    items_b = _project_work_witnesses(rows_b, unit_by_sys={})

    assert len(items_a) == 1
    assert len(items_b) == 1
    assert items_a[0]["representative_page_id"] == "lp000A"
    assert items_b[0]["representative_page_id"] == "lp000A"


# ===========================================================================
# Phase 136, plan 136-14, Task 1: the {status, items, total} envelope (D-13),
# the human-confirmed routing fix (D-13g) and the panel's display fields.
#
# Masking discipline unchanged: every fixture below is fabricated in-test via
# scripts/build_discovery_sidecar (synthetic ids, synthetic titles) -- never
# real research data, never a corpus name.
# ===========================================================================

import json as _json  # noqa: E402 -- appended section, grouped with its own tests

from shared.discovery_surface_projection import (  # noqa: E402
    OUTAGE_STATUSES,
    STATUS_BUSY,
    STATUS_OK,
    STATUS_TIMEOUT,
    STATUS_UNAVAILABLE,
    SURFACE_CLAIM_FIELDS,
    SURFACE_STATUSES,
    is_outage,
    make_envelope,
    surface_safe_claim,
)


def _new_sidecar(tmp_path, name, *, works, evidence_specs, version, unit_specs=None):
    """Build a fresh synthetic sidecar carrying the FULL Phase-136 grain:
    claims + evidence + the materialized `discovery_identification` table, the
    latter produced by the REAL builder (`populate_discovery_identification`),
    never a hand-written stand-in -- so these tests exercise the shipped
    eligibility rule (`routing_status='shipped'` OR
    `adjudication_status='human_confirmed'`) rather than a test-local copy.

    `works`: list of (work_id, canonical_work_id, neutral_title, author, genre,
    source_corpus).
    """
    result = sidecar_build.assemble_claims_and_evidence(
        evidence_specs,
        {w[0]: w[5] for w in works},
        sidecar_version=version,
    )
    db_path = tmp_path / name
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, "
            "genre, source_corpus, identity_visibility) VALUES (?, ?, ?, ?, ?, ?, 'public')",
            works,
        )
        sidecar_build._insert_claims_and_evidence_real(
            cur, result["claim_rows"], result["evidence_rows"])
        if unit_specs:
            sidecar_build._insert_witness_units_real(cur, unit_specs)
        cur.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [
                ("schema_version", "discovery-v1"), ("sidecar_version", version),
                ("source_db_sha256", "test"), ("build_date", "2026-01-01T00:00:00Z"),
                ("data_as_of", "2026-01-01"), ("htr_snapshot_hash", "test"),
                ("expected_rows_claims", str(len(result["claim_rows"]))),
                ("expected_rows_evidence", str(len(result["evidence_rows"]))),
                ("expected_rows_works", str(len(works))),
                ("expected_rows_units", "0"), ("frame_content_hash", "test"),
                ("audience", "private"),
            ],
        )
        conn.commit()
        sidecar_build.populate_discovery_identification(conn)
        conn.commit()
    finally:
        conn.close()
    return str(db_path)


def _service_for(db_path, version):
    return DiscoveryService(
        path_provider=lambda: db_path,
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: version,
    )


# --- D-13g regression fixture ----------------------------------------------
#
# The EXACT observed symptom (136-CONTEXT.md D-13g, quantified in the sketch
# reference discovery-panel-layout.md): on ONE manuscript, two rows a human
# confirmed were treated differently -- one page's human_confirmed row was
# hidden because routing had demoted it for low coverage, while another page's
# human_confirmed row showed. The routing predicate ran BEFORE
# is_default_eligible(), which returns True for human_confirmed
# unconditionally, so the predicate meant to protect those rows never ran.

_D13G_VERSION = "test-d13g-regression"
_D13G_SYS = "990000000000000901"
_D13G_HIDDEN_PAGE = "d13g_p22"      # human_confirmed, routing demoted (low_coverage)
_D13G_SHOWN_PAGE = "d13g_p23"       # human_confirmed, shipped
_D13G_UNREVIEWED_PAGE = "d13g_p24"  # unreviewed + review_only -> still hidden


def _build_d13g_regression_db(tmp_path):
    works = [
        ("w000901", "w000901", "Synthetic Commentary On Alpha", "Synthetic Author Z",
         "Synthetic Parent A / Synthetic Leaf A", "sefaria"),
        ("w000902", "w000902", "Synthetic Commentary On Beta", "Synthetic Author Z",
         "Synthetic Parent A / Synthetic Leaf A", "sefaria"),
        ("w000903", "w000903", "Synthetic Unreviewed Work", None, None, "sefaria"),
    ]
    specs = [
        # The row the routing filter dropped: a human CONFIRMED identification
        # demoted to review_only for low coverage (a commentary occupying only
        # part of a densely-written page).
        sidecar_build._mk_evidence(
            page_id=_D13G_HIDDEN_PAGE, work_id="w000901", sys_id=_D13G_SYS,
            evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
            confidence_band=sidecar_build._TIER_A,
            adjudication_status=sidecar_build._HUMAN_CONFIRMED,
            audit_status=sidecar_build._NA,
            routing_status=sidecar_build._REVIEW_ONLY,
            routing_reason=sidecar_build._LOW_COVERAGE,
            span_start=0, span_end=1400, matched_letters=1329, n_spans=1,
            coverage=0.21, page_norm_letters=6300,
        ),
        # The row that showed: same manuscript, same human review, shipped.
        sidecar_build._mk_evidence(
            page_id=_D13G_SHOWN_PAGE, work_id="w000902", sys_id=_D13G_SYS,
            evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
            confidence_band=sidecar_build._TIER_A,
            adjudication_status=sidecar_build._HUMAN_CONFIRMED,
            audit_status=sidecar_build._NA,
            routing_status=sidecar_build._SHIPPED,
            routing_reason=sidecar_build._NONE_REASON,
            span_start=0, span_end=900, matched_letters=880, n_spans=1,
            coverage=0.9, page_norm_letters=978,
        ),
        # The control: review_only WITHOUT human review stays hidden by default.
        sidecar_build._mk_evidence(
            page_id=_D13G_UNREVIEWED_PAGE, work_id="w000903", sys_id=_D13G_SYS,
            evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
            confidence_band=sidecar_build._SCREENING_RB,
            adjudication_status=sidecar_build._UNREVIEWED,
            audit_status=sidecar_build._NA,
            routing_status=sidecar_build._REVIEW_ONLY,
            routing_reason=sidecar_build._LOW_COVERAGE,
            span_start=0, span_end=120, matched_letters=90, n_spans=1,
        ),
    ]
    return _new_sidecar(tmp_path, "d13g.db", works=works,
                        evidence_specs=specs, version=_D13G_VERSION)


@pytest.fixture()
def d13g_service(tmp_path):
    return _service_for(_build_d13g_regression_db(tmp_path), _D13G_VERSION)


# ---------------------------------------------------------------------------
# The four envelope states, pairwise distinct (D-13 / T-136-14-01)
# ---------------------------------------------------------------------------

def test_envelope_unavailable_when_sidecar_is_not_serving():
    service = _make_service(available=False)
    env = service.get_claims_for_page_enveloped("p001")
    assert env["status"] == STATUS_UNAVAILABLE
    assert env["items"] == []
    assert env["total"] == 0
    assert is_outage(env) is True


def test_envelope_timeout_is_distinct_from_unavailable(monkeypatch):
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_BROWSE", "0.05")
    monkeypatch.setenv("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", "0")
    service = _make_service()
    block = threading.Event()

    def _slow(page_id, page=1, page_size=None, include_review=False, lang="en"):
        block.wait(timeout=5)
        return make_envelope(STATUS_OK, [], 0)

    service.get_claims_for_page_enveloped = _slow
    try:
        env = asyncio.run(service.get_claims_for_page_enveloped_async("p001"))
    finally:
        block.set()
        time.sleep(0.1)
    assert env["status"] == STATUS_TIMEOUT
    assert env["status"] != STATUS_UNAVAILABLE
    assert env["items"] == []
    assert env["total"] == 0


def test_envelope_busy_is_distinct_from_timeout_and_unavailable():
    service = _make_service()

    async def _overloaded(*a, **k):
        raise DiscoveryOverload("temporarily unavailable")

    service._run_off_loop = _overloaded
    env = asyncio.run(service.get_claims_for_page_enveloped_async("p001"))
    assert env["status"] == STATUS_BUSY
    assert env["status"] not in (STATUS_TIMEOUT, STATUS_UNAVAILABLE)
    assert is_outage(env) is True


def test_envelope_ok_on_a_genuine_empty_result():
    service = _make_service()
    env = service.get_claims_for_page_enveloped("page-that-does-not-exist")
    assert env["status"] == STATUS_OK
    assert env["items"] == []
    assert env["total"] == 0
    assert is_outage(env) is False, (
        "a genuine zero must be distinguishable from an outage -- this is the "
        "whole point of D-13 (the panel hides on a SUCCESSFUL zero only)"
    )


def test_envelope_status_vocabulary_is_closed_and_pairwise_distinct():
    assert SURFACE_STATUSES == {STATUS_OK, STATUS_UNAVAILABLE, STATUS_TIMEOUT, STATUS_BUSY}
    assert len(SURFACE_STATUSES) == 4, "four states, pairwise distinct"
    assert OUTAGE_STATUSES == SURFACE_STATUSES - {STATUS_OK}
    with pytest.raises(ValueError):
        make_envelope("degraded")  # a surface cannot invent a fifth state


def test_preexisting_list_methods_keep_signature_and_empty_behaviour():
    """The legacy list-returning API is UNCHANGED: same call shape, still a
    list, still `[]` (never an exception) on every failure path."""
    import inspect

    sig = inspect.signature(DiscoveryService.get_claims_for_page)
    assert list(sig.parameters) == ["self", "page_id", "page", "page_size", "include_review"]

    unavailable = _make_service(available=False)
    assert unavailable.get_claims_for_page("p001") == []
    assert unavailable.get_pages_related_to_page("p004") == []
    assert unavailable.get_evidence("whatever") == []
    assert unavailable.get_work_witnesses("w000001") == []

    service = _make_service()
    rows = service.get_claims_for_page("p001")
    assert isinstance(rows, list) and rows and isinstance(rows[0], dict)


# ---------------------------------------------------------------------------
# D-13g: the human-confirmed routing fix (T-136-14-07)
# ---------------------------------------------------------------------------

def test_two_human_confirmed_rows_on_one_manuscript_are_treated_alike(d13g_service):
    """THE regression. Population, recorded so the two figures are never
    conflated: 19 of 121 human-confirmed rows are dropped across ALL
    human-confirmed evidence; on the DISPLAY evidence this page query actually
    reads it is 14 of 116."""
    hidden = d13g_service.get_claims_for_page(_D13G_HIDDEN_PAGE)
    shown = d13g_service.get_claims_for_page(_D13G_SHOWN_PAGE)

    assert len(shown) == 1, "the shipped human-confirmed row always showed"
    assert len(hidden) == 1, (
        "a human-confirmed row demoted by routing must NOT be dropped by the "
        "query before is_default_eligible() -- which returns True for "
        "human_confirmed unconditionally -- ever runs (D-13g)"
    )
    assert hidden[0]["adjudication_status"] == "human_confirmed"
    assert hidden[0]["routing_status"] == "review_only"
    assert shown[0]["adjudication_status"] == "human_confirmed"


def test_unreviewed_review_only_row_still_hidden_by_default_and_shown_under_flag(d13g_service):
    assert d13g_service.get_claims_for_page(_D13G_UNREVIEWED_PAGE) == []
    opted_in = d13g_service.get_claims_for_page(_D13G_UNREVIEWED_PAGE, include_review=True)
    assert len(opted_in) == 1
    assert opted_in[0]["adjudication_status"] == "unreviewed"
    assert opted_in[0]["routing_status"] == "review_only"


def test_restored_human_confirmed_row_carries_a_low_coverage_marker_not_a_band_change(d13g_service):
    env = d13g_service.get_claims_for_page_enveloped(_D13G_HIDDEN_PAGE)
    row = env["items"][0]
    assert row["low_coverage_marker"] is True
    assert row["restored_by_human_confirmation"] is True
    assert row["eligibility_basis"] == "human_confirmed"
    # The band is UNCHANGED -- the marker is a note, never a re-banding.
    assert row["confidence_band"] == "tier_a"
    assert row["band_rank"] == _band_rank("track1_direct", "tier_a")

    shipped = d13g_service.get_claims_for_page_enveloped(_D13G_SHOWN_PAGE)["items"][0]
    assert shipped["low_coverage_marker"] is False
    assert shipped["restored_by_human_confirmation"] is False
    assert shipped["eligibility_basis"] == "shipped"


def test_review_only_human_confirmed_row_resolves_to_an_identification(d13g_service):
    """The restore must not be undone one layer down: the materialized
    identification grain admits `shipped` OR `human_confirmed`, so the join
    finds a row carrying a bucket AND a reason."""
    row = d13g_service.get_claims_for_page_enveloped(_D13G_HIDDEN_PAGE)["items"][0]
    assert row["identification_id"], "no identification row for a restored human-confirmed claim"
    assert row["main_pool"] in (True, False)
    assert row["main_pool_reason"]
    assert row["main_pool"] is True and row["main_pool_reason"] == "main_human_confirmed", (
        "main_pool_decision puts every human_confirmed identification in the main pool"
    )


# ---------------------------------------------------------------------------
# The panel's display fields, in ONE query (T-136-14-03)
# ---------------------------------------------------------------------------

def test_enveloped_claim_row_key_set_is_exactly_the_surface_allowlist(d13g_service):
    env = d13g_service.get_claims_for_page_enveloped(_D13G_SHOWN_PAGE)
    assert env["status"] == STATUS_OK
    row = env["items"][0]
    assert set(row) == set(SURFACE_CLAIM_FIELDS)
    # Every display field the panel renders, present and populated in ONE query.
    assert row["display_work_id"] == "w000902"
    assert row["neutral_title"] == "Synthetic Commentary On Beta"
    assert row["relation_kind"] == "direct_witness"
    assert row["confidence_band"] == "tier_a"
    assert row["band_label"]
    assert row["band_rank"] == _band_rank("track1_direct", "tier_a")
    assert row["coverage_ppm"] == 900000
    assert row["coverage_status"] == "measured"
    assert row["main_pool"] is True
    assert row["main_pool_reason"]
    assert row["novelty_status"] == "not_checked"
    assert row["matched_letters"] == 880
    assert row["span_start"] == 0 and row["span_end"] == 900
    assert row["n_spans"] == 1
    assert row["evidence_id"]


class _ExecuteSpy:
    """Counts `execute` calls on a real connection object, delegating
    everything else -- so a per-row follow-up query is visible as a count that
    GROWS with the row count."""

    def __init__(self, inner):
        self._inner = inner
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append(sql)
        return self._inner.execute(sql, params)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def test_one_query_per_page_no_per_row_followup(tmp_path):
    """T-136-14-03: the execute count must NOT grow with the number of rows on
    the page. A fixture with 1 row and one with 6 rows on a single page are
    compared -- a per-row follow-up shows up as a difference."""
    def _page_db(name, n_works):
        works = [
            (f"w0010{i:02d}", f"w0010{i:02d}", f"Synthetic Work {i}", None, None, "sefaria")
            for i in range(n_works)
        ]
        specs = [
            sidecar_build._mk_evidence(
                page_id="spy_p1", work_id=f"w0010{i:02d}", sys_id="990000000000000902",
                evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
                confidence_band=sidecar_build._TIER_A,
                adjudication_status=sidecar_build._UNREVIEWED, audit_status=sidecar_build._NA,
                routing_status=sidecar_build._SHIPPED, routing_reason=sidecar_build._NONE_REASON,
                span_start=0, span_end=100 + i, matched_letters=200 + i, n_spans=1,
            )
            for i in range(n_works)
        ]
        return _new_sidecar(tmp_path, name, works=works, evidence_specs=specs,
                            version="test-spy")

    def _count_executes(db_path, expected_rows):
        service = _service_for(db_path, "test-spy")
        service.is_available()  # build the connection OUTSIDE the measurement
        spy = _ExecuteSpy(service._get_conn())
        service._get_conn = lambda: spy
        env = service.get_claims_for_page_enveloped("spy_p1")
        assert len(env["items"]) == expected_rows, "fixture did not reach its own assertion"
        return len(spy.calls)

    one_row = _count_executes(_page_db("spy-1.db", 1), 1)
    six_rows = _count_executes(_page_db("spy-6.db", 6), 6)

    assert one_row == six_rows, (
        f"execute count grew with row count ({one_row} -> {six_rows}) -- a "
        "per-row follow-up query multiplies browse-enrichment latency by the "
        "row count (T-136-14-03)"
    )
    assert six_rows <= 2, (
        f"expected the page query (plus at most the cached band-measurement "
        f"lookup), got {six_rows} executes"
    )


# ---------------------------------------------------------------------------
# T-136-14-10: the `works` join is 1:1 and keys on `display_work_id`
# ---------------------------------------------------------------------------

_DUP_VERSION = "test-duplicate-canonical"
_DUP_SYS = "990000000000000903"
_DUP_PAGE = "dup_p1"


def _build_duplicated_canonical_group_db(tmp_path):
    """A duplicated `canonical_work_id` group with DIFFERENT titles -- the
    measured hazard: 15 such groups exist on the live asset, three with
    different titles and mixed source corpora, and a `canonical_work_id` join
    fans the 64,509-row identification grain out to 65,587."""
    works = [
        # The canonical anchor (work_id == canonical_work_id) -- schema (B1)
        # rule 1 selects it as the group's display representative.
        ("w000910", "w000910", "Synthetic Canonical Title", "Synthetic Author Y", None, "sefaria"),
        # A second row in the SAME canonical group, carrying a DIFFERENT title.
        ("w000911", "w000910", "Synthetic Duplicate Title", "Synthetic Author Y", None, "sefaria"),
    ]
    specs = [
        sidecar_build._mk_evidence(
            page_id=_DUP_PAGE, work_id="w000911", sys_id=_DUP_SYS,
            evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
            confidence_band=sidecar_build._TIER_A,
            adjudication_status=sidecar_build._UNREVIEWED, audit_status=sidecar_build._NA,
            routing_status=sidecar_build._SHIPPED, routing_reason=sidecar_build._NONE_REASON,
            span_start=0, span_end=500, matched_letters=400, n_spans=1,
        ),
    ]
    return _new_sidecar(tmp_path, "dup-canonical.db", works=works,
                        evidence_specs=specs, version=_DUP_VERSION)


def test_works_join_is_1to1_and_keys_on_display_work_id(tmp_path):
    db_path = _build_duplicated_canonical_group_db(tmp_path)

    # The fixture MUST be able to reach its own assertion: prove the fan-out a
    # canonical_work_id join would produce actually exists here.
    probe = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        (fanned,) = probe.execute(
            "SELECT COUNT(*) FROM discovery_identification di "
            "JOIN works w ON w.canonical_work_id = di.canonical_work_id"
        ).fetchone()
        (grain,) = probe.execute("SELECT COUNT(*) FROM discovery_identification").fetchone()
    finally:
        probe.close()
    assert fanned > grain, (
        "the fixture does not contain a duplicated canonical group -- it cannot "
        "prove the join hazard it exists to prove"
    )

    service = _service_for(db_path, _DUP_VERSION)
    env = service.get_claims_for_page_enveloped(_DUP_PAGE)
    assert env["total"] == 1, "the works join fanned out (T-136-14-10)"
    assert len(env["items"]) == 1
    row = env["items"][0]
    assert row["work_id"] == "w000911", "the claim's own work is unchanged"
    assert row["display_work_id"] == "w000910", "the join key must be display_work_id"
    assert row["neutral_title"] == "Synthetic Canonical Title", (
        "D-13a: the canonical work's OWN title wins -- a canonical_work_id join "
        "would make the displayed title depend on which row it happened to return"
    )


# ---------------------------------------------------------------------------
# T-136-14-09: the allowlist, and four assertions that the badge / precision /
# interval never leave the service.
# ---------------------------------------------------------------------------

def test_surface_safe_claim_is_an_allowlist_not_a_denylist():
    from shared.discovery_band_labels import serialize_banded_claim

    serialized = serialize_banded_claim({
        "evidence_source": "track1_direct",
        "confidence_band": "tier_a",
        "adjudication_status": "human_confirmed",
        "routing_status": "shipped",
    })
    # A field the serializer might grow LATER must be excluded by DEFAULT.
    serialized["a_future_serializer_field"] = "should not reach a surface"
    serialized["review_overlay_v2"] = "Expert-reviewed"
    projected = surface_safe_claim(serialized)

    assert "a_future_serializer_field" not in projected
    assert "review_overlay_v2" not in projected
    assert "review_overlay" not in projected
    assert set(projected) == set(SURFACE_CLAIM_FIELDS)


def test_forbidden_fields_absent_from_a_returned_row(d13g_service):
    row = d13g_service.get_claims_for_page_enveloped(_D13G_HIDDEN_PAGE)["items"][0]
    for forbidden in ("review_overlay", "precision", "ci_low", "ci_high"):
        assert forbidden not in row


def test_forbidden_fields_absent_from_the_envelope(d13g_service):
    env = d13g_service.get_claims_for_page_enveloped(_D13G_HIDDEN_PAGE)
    for forbidden in ("review_overlay", "precision", "ci_low", "ci_high"):
        assert forbidden not in env
        assert forbidden not in env["meta"]


def test_forbidden_values_absent_from_a_serialized_json_payload(d13g_service):
    env = d13g_service.get_claims_for_page_enveloped(_D13G_HIDDEN_PAGE)
    payload = _json.dumps(env, ensure_ascii=False)
    for forbidden_key in ("review_overlay", "ci_low", "ci_high", "precision"):
        assert forbidden_key not in payload
    for forbidden_value in ("Expert-reviewed", "נבדק בידי מומחה"):
        assert forbidden_value not in payload


def test_forbidden_fields_absent_from_an_error_path_payload():
    """The error paths -- unavailable, timeout, busy -- carry no forbidden key
    and no forbidden value either. An error path is exactly where a leak would
    otherwise be invisible to a renderer-level assertion."""
    unavailable = _make_service(available=False).get_claims_for_page_enveloped("p001")

    service = _make_service()

    async def _overloaded(*a, **k):
        raise DiscoveryOverload("busy")

    service._run_off_loop = _overloaded
    busy = asyncio.run(service.get_claims_for_page_enveloped_async("p001"))

    async def _timed_out(*a, **k):
        raise DiscoveryUnavailable("timeout")

    service._run_off_loop = _timed_out
    timed_out = asyncio.run(service.get_claims_for_page_enveloped_async("p001"))

    for env in (unavailable, busy, timed_out):
        payload = _json.dumps(env, ensure_ascii=False)
        for forbidden in ("review_overlay", "ci_low", "ci_high", "precision", "Expert-reviewed"):
            assert forbidden not in payload


def test_expert_reviewed_badge_never_appears_in_service_output_for_a_human_confirmed_row(d13g_service):
    """D-13f: no row on any surface claims human review. The badge string must
    not appear ANYWHERE in the service's output for the very row whose
    adjudication_status would produce it."""
    from shared.discovery_band_labels import review_overlay

    badge_en = review_overlay("human_confirmed", "en")
    badge_he = review_overlay("human_confirmed", "he")
    assert "Expert-reviewed" in badge_en  # the fixture reaches its own assertion

    outputs = [
        d13g_service.get_claims_for_page_enveloped(_D13G_HIDDEN_PAGE),
        d13g_service.get_claims_for_page_enveloped(_D13G_SHOWN_PAGE),
        d13g_service.get_claims_for_page_enveloped(_D13G_HIDDEN_PAGE, lang="he"),
    ]
    for env in outputs:
        payload = _json.dumps(env, ensure_ascii=False)
        assert badge_en not in payload
        assert badge_he not in payload


# ---------------------------------------------------------------------------
# The band presentation goes through serialize_banded_claim, never a hardcoded
# string (T-136-14-08); and the two enveloped shapes share ONE implementation.
# ---------------------------------------------------------------------------

def test_no_hardcoded_band_display_string_in_the_service_module():
    import pathlib

    from shared.discovery_band_labels import BAND_LABELS

    repo_root = pathlib.Path(__file__).resolve().parent.parent
    source = (repo_root / "shared" / "discovery_service.py").read_text(encoding="utf-8")
    for entry in BAND_LABELS.values():
        for label in entry.values():
            assert label not in source, (
                f"band display string {label!r} is hardcoded in the service -- "
                "presentation must go through serialize_banded_claim (SC#1)"
            )
    assert "serialize_banded_claim" in source


def test_bandless_row_raises_rather_than_rendering_without_its_band():
    from shared.discovery_band_labels import serialize_banded_claim

    with pytest.raises(ValueError):
        serialize_banded_claim({"evidence_source": "track1_direct", "confidence_band": None,
                                "adjudication_status": "unreviewed"})


def test_enveloped_sync_and_async_are_one_implementation_in_two_shapes(monkeypatch):
    monkeypatch.setenv("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", "0")
    service = _make_service()
    sentinel = make_envelope(STATUS_OK, [{"page_id": "sentinel"}], 1)
    calls = []

    def _sync(page_id, page=1, page_size=None, include_review=False, lang="en"):
        calls.append((page_id, page, page_size, include_review, lang))
        return sentinel

    service.get_claims_for_page_enveloped = _sync
    got = asyncio.run(service.get_claims_for_page_enveloped_async("p001"))
    assert got == sentinel, "the async shape must delegate to the SYNC one, not re-query"
    assert calls, "the async wrapper did not call the sync implementation"


# ---------------------------------------------------------------------------
# web/discovery.py: the enveloped wrappers still fail OPEN, but the failure is
# now NAMED rather than collapsed into an empty list.
# ---------------------------------------------------------------------------

def test_web_wrapper_returns_unavailable_envelope_when_discovery_is_off(monkeypatch):
    import web.discovery as web_discovery

    monkeypatch.setattr(web_discovery, "discovery_available", lambda: False)
    env = asyncio.run(web_discovery.get_claims_for_page_enveloped("p001"))
    assert env["status"] == STATUS_UNAVAILABLE
    assert env["items"] == [] and env["total"] == 0
    # The pre-existing list wrapper is untouched and still fails open to [].
    assert asyncio.run(web_discovery.get_claims_for_page("p001")) == []


def test_web_enveloped_wrapper_honours_the_browse_timeout(monkeypatch):
    """The wrapper must dispatch through the service's browse-budget path --
    the enveloped call is on the browse hot path, where PERF-01 caps added
    latency at p95 <= 150 ms and the per-query timeout at 2 s."""
    import web.discovery as web_discovery

    monkeypatch.setattr(web_discovery, "discovery_available", lambda: True)
    seen = {}

    async def _fake(page_id, page=1, page_size=None, include_review=False, lang="en"):
        seen["timeout"] = web_discovery._service._browse_timeout()
        return make_envelope(STATUS_OK, [], 0)

    monkeypatch.setattr(
        web_discovery._service, "get_claims_for_page_enveloped_async", _fake)
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_BROWSE", "1.25")
    env = asyncio.run(web_discovery.get_claims_for_page_enveloped("p001"))
    assert env["status"] == STATUS_OK
    assert seen["timeout"] == 1.25
