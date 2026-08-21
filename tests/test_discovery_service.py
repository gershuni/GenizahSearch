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
from concurrent.futures import ThreadPoolExecutor

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
# THE BROWSE BUDGET (code review round 12, finding 4).
#
# `_acquire_heavy_slot` was entered ONLY when `heavy=True`, and no browse
# caller passed it -- so "bounded concurrency" was a property
# docs/specs/discovery-budgets.md documents and the connections-panel path did
# not have. Every executor crossing now takes one of two budgets.
# ---------------------------------------------------------------------------

def test_a_browse_read_takes_a_slot_at_all(monkeypatch):
    """The finding, as a test: with the browse budget exhausted, a browse read
    must fail fast rather than dispatch. Before the fix it dispatched happily,
    because it asked for no slot."""
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES", "1")
    service = _make_service()

    async def _run():
        await service._browse_sem.acquire()
        try:
            start = time.monotonic()
            with pytest.raises(DiscoveryOverload):
                await service.get_claims_for_page_async("p001")
            assert time.monotonic() - start < 1.0, "the bound must fail fast, never wait"
        finally:
            service._browse_sem.release()

    asyncio.run(_run())


def test_a_browse_read_takes_the_BROWSE_budget_and_not_the_heavy_one(monkeypatch):
    """Which budget, not merely that there is one. Two budgets that behave as
    one are a rename, and this is the assertion that tells them apart."""
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_QUERIES", "1")
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES", "1")
    service = _make_service()

    async def _run():
        # The HEAVY budget is fully held; a BROWSE read must still go through.
        await service._heavy_sem.acquire()
        try:
            assert await service.get_claims_for_page_async("p001") is not None
        finally:
            service._heavy_sem.release()

        # ...and the reverse: the browse budget held, a HEAVY read still runs
        # its query (it returns rows rather than raising the overload).
        await service._browse_sem.acquire()
        try:
            assert isinstance(await service.get_work_witnesses_async("w000001"), list)
        finally:
            service._browse_sem.release()

    asyncio.run(_run())


def test_an_enveloped_browse_read_reports_busy_THROUGH_THE_LIVE_GATE(monkeypatch):
    """`busy` stops being a status only an injection could produce.

    Every `busy` assertion in this phase's panel suites injects
    `DiscoveryOverload` at the point the gate would raise, because the gate
    could not raise on this path at all. This drives the REAL gate.
    """
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES", "1")
    service = _make_service()

    async def _run():
        await service._browse_sem.acquire()
        try:
            for envelope in (
                await service.get_claims_for_page_enveloped_async("p001"),
                await service.get_manuscript_works_enveloped_async(("p001",)),
                await service.get_related_page_count_enveloped_async("p001"),
                await service.get_related_pages_enveloped_async("p001"),
            ):
                assert envelope["status"] == "busy", envelope
                assert envelope["meta"]["reason"] == "bounded_concurrency"
        finally:
            service._browse_sem.release()

    asyncio.run(_run())


def test_a_cache_HIT_takes_no_slot_because_it_runs_no_query():
    """The bound is on DISPATCH, not on calls. A warm folio turn -- the case
    the measured 0.1 ms p95 describes -- must not be able to overload."""
    service = _make_service()

    async def _run():
        first = await service.get_claims_for_page_enveloped_async("p001")
        assert first["status"] == "ok"
        # Now hold EVERY browse slot and repeat the identical call.
        held = []
        try:
            while not service._browse_sem.locked():
                await service._browse_sem.acquire()
                held.append(True)
            again = await service.get_claims_for_page_enveloped_async("p001")
            assert again["status"] == "ok", "a cache hit was refused by the bound"
        finally:
            for _ in held:
                service._browse_sem.release()

    asyncio.run(_run())


def test_a_timed_out_browse_slot_is_not_recycled_until_the_thread_finishes(monkeypatch):
    """DC6 for the browse budget. This is the mechanism the finding named: a
    `run_in_executor` thread is not cancellable, so a timed-out read keeps its
    worker; the slot must stay held until the thread ACTUALLY finishes or the
    bound re-admits work past its own budget."""
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES", "1")
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_BROWSE", "0.05")
    service = _make_service()
    block_event = threading.Event()
    calls = {"n": 0}

    def _slow(page_id, page, page_size):
        calls["n"] += 1
        if calls["n"] == 1:
            block_event.wait(timeout=5)
        return []

    service.get_claims_for_page = _slow

    async def _run():
        with pytest.raises(DiscoveryUnavailable):
            await service.get_claims_for_page_async("p001")
        assert calls["n"] == 1
        with pytest.raises(DiscoveryOverload):
            await service.get_claims_for_page_async("p002")
        assert calls["n"] == 1, "a still-held slot admitted a second concurrent call"
        block_event.set()
        for _ in range(50):
            if not service._browse_sem.locked():
                break
            await asyncio.sleep(0.05)
        assert not service._browse_sem.locked(), (
            "the browse slot was never released after the stuck thread finished")
        assert await service.get_claims_for_page_async("p003") == []
        assert calls["n"] == 2

    asyncio.run(_run())


def test_the_browse_budget_is_larger_than_the_heavy_one_by_construction():
    """Not a taste call. A cold connections-panel load issues THREE reads
    concurrently and then a fourth, so the heavy cap of 4 would put the SECOND
    simultaneous browse visitor into `busy` -- shipping an outage on an
    already-live page in the name of bounding it. The browse budget has to
    admit several whole page loads at once."""
    from shared.discovery_service import (
        _DEFAULT_MAX_CONCURRENT_BROWSE_QUERIES, _DEFAULT_MAX_CONCURRENT_QUERIES,
    )
    concurrent_reads_per_cold_panel_load = 3
    assert _DEFAULT_MAX_CONCURRENT_BROWSE_QUERIES >= (
        _DEFAULT_MAX_CONCURRENT_QUERIES + concurrent_reads_per_cold_panel_load), (
        'the browse budget cannot admit even one more page load than the heavy one')
    assert _DEFAULT_MAX_CONCURRENT_BROWSE_QUERIES % concurrent_reads_per_cold_panel_load == 0



def test_the_off_loop_signature_is_pinned_because_test_doubles_mirror_it():
    """`_run_off_loop` is stood in for by hand-written doubles in other files.

    Three of them re-declare its signature rather than taking `**kwargs`, which
    is deliberate -- a double that swallows any call cannot detect a wrong one.
    The cost is that ADDING a parameter here breaks them, and it breaks them
    somewhere unhelpful: `_dispatch_enveloped` forwards every keyword on every
    crossing, so the `TypeError` lands inside whatever surface the double is
    serving and is reported as "temporarily unavailable". That is exactly how
    the `slot` parameter reached CI green locally and red there, having broken
    the browse panel's expansion tests, which the change never touched.

    So the keyword set is PINNED. If this fails, the signature grew -- update
    these three doubles in the same commit:

      * `tests/test_discovery_panel_render.py::_Spy.__call__`
      * `tests/test_discovery_panel_browse_wiring.py::_Spy.__call__`
      * `tests/test_discovery_launch_stats.py`, the local `spy` inside
        `test_the_read_runs_under_the_findings_timeout_not_the_browse_timeout`
    """
    import inspect
    from shared.discovery_service import DiscoveryService as _DS
    params = inspect.signature(_DS._run_off_loop).parameters
    keyword_only = {name for name, p in params.items()
                    if p.kind is inspect.Parameter.KEYWORD_ONLY}
    assert keyword_only == {"timeout", "heavy", "slot"}, (
        f"`_run_off_loop` keyword-only parameters are now {sorted(keyword_only)}"
        " -- update the three test doubles named in this test's docstring")


def test_no_executor_crossing_can_opt_out_of_a_slot():
    """A source guard, because the defect was an OPT-IN bound whose next caller
    forgot it. `_run_off_loop` is the ONE place a crossing happens; it must take
    a slot unconditionally, with the only choice being WHICH budget."""
    import inspect
    from shared.discovery_service import DiscoveryService as _DS
    source = inspect.getsource(_DS._run_off_loop)
    assert "await self._acquire_slot(" in source
    dispatch = source.index("run_in_executor")
    acquire = source.index("await self._acquire_slot(")
    assert acquire < dispatch, "a crossing happens before the slot is taken"
    for escape in ("if heavy:", "if bounded", "if not heavy:"):
        assert escape not in source, (
            f"{escape!r} makes the bound conditional again")


def test_browse_concurrency_env_non_positive_falls_back_to_the_default(monkeypatch):
    for value in ("-3", "0", "not-a-number"):
        monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES", value)
        service = _make_service()
        assert service._browse_capacity >= 1


# ---------------------------------------------------------------------------
# THE ISOLATION IS REAL, NOT NOMINAL (code review round 13, finding 2).
#
# Two semaphores over ONE threadpool are two names for one budget: 24 browse
# jobs can occupy or queue ahead of every worker in the default
# `run_in_executor` pool -- which this repository never configures and whose
# width is not guaranteed -- and a heavy read then times out while its OWN
# semaphore still has capacity. Each class now has its own executor, sized to
# its own capacity, so a slot guarantees a worker.
# ---------------------------------------------------------------------------

def test_a_HEAVY_read_gets_through_while_blocked_BROWSE_reads_hold_the_default_pool(
        monkeypatch):
    """The reproduction, as a test.

    The event loop's DEFAULT executor is shrunk to ONE worker and two browse
    reads are blocked in it. Before the per-class executors, the heavy read
    queued behind them in that same pool and timed out with its own semaphore
    at full capacity -- a `busy`/`unavailable` panel produced by an unrelated
    path's back-pressure. It must now run.
    """
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_WORK", "1.0")
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_BROWSE", "10.0")
    monkeypatch.setenv("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", "0")
    service = _make_service()

    release_browse = threading.Event()
    browse_started = threading.Event()
    heavy_ran = {"n": 0}

    def _blocking_browse(page_id, page, page_size):
        browse_started.set()
        release_browse.wait(timeout=10)
        return []

    def _heavy(work_id, enabled_bands, page, page_size, anchor_sys_id):
        heavy_ran["n"] += 1
        return []

    service.get_claims_for_page = _blocking_browse
    service.get_work_witnesses = _heavy

    async def _run():
        # A ONE-worker default pool: the narrowest honest statement of "the
        # width of the shared pool is not something this code may assume".
        tiny = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tiny-default")
        asyncio.get_running_loop().set_default_executor(tiny)
        try:
            blocked = [
                asyncio.ensure_future(service.get_claims_for_page_async("p001")),
                asyncio.ensure_future(service.get_claims_for_page_async("p002")),
            ]
            for _ in range(200):
                if browse_started.is_set():
                    break
                await asyncio.sleep(0.01)
            assert browse_started.is_set(), "no browse read ever reached a worker"

            # The heavy read, with both browse reads still occupying/queued.
            result = await service.get_work_witnesses_async("w000001")
            assert result == [], result
            assert heavy_ran["n"] == 1, (
                "the heavy read never reached a worker -- it is queued behind "
                "browse work, so the two budgets share one execution resource "
                "and the split is nominal")
        finally:
            release_browse.set()
            await asyncio.gather(*blocked, return_exceptions=True)
            tiny.shutdown(wait=False)

    asyncio.run(_run())


def test_each_budget_dispatches_into_its_OWN_executor_sized_to_its_own_capacity(
        monkeypatch):
    """The mechanism the test above depends on, asserted directly: two distinct
    executors, each `max_workers` equal to its class's semaphore capacity. A
    slot that does not guarantee a worker is not a budget."""
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_QUERIES", "2")
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES", "7")
    monkeypatch.setenv("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", "0")
    service = _make_service()

    # Lazy: nothing is built until a read dispatches, so a flag-OFF process
    # pays no threads at all.
    assert service._executors == {}

    async def _run():
        await service.get_claims_for_page_async("p001")
        await service.get_work_witnesses_async("w000001")

    asyncio.run(_run())

    assert set(service._executors) == {"browse", "heavy"}
    assert service._executors["browse"] is not service._executors["heavy"]
    assert service._executors["browse"]._max_workers == service._browse_capacity == 7
    assert service._executors["heavy"]._max_workers == service._heavy_capacity == 2


def test_a_capacity_change_rebuilds_the_executor_WITH_the_semaphore(monkeypatch):
    """If the semaphore is resized and the executor is not, the slot stops
    guaranteeing a worker and the isolation quietly reverts to the defect."""
    monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES", "3")
    monkeypatch.setenv("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", "0")
    service = _make_service()

    async def _run():
        await service.get_claims_for_page_async("p001")
        first = service._executors["browse"]
        assert first._max_workers == 3
        monkeypatch.setenv("DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES", "9")
        await service.get_claims_for_page_async("p002")
        second = service._executors["browse"]
        assert second is not first, "the executor survived a capacity change"
        assert second._max_workers == service._browse_capacity == 9

    asyncio.run(_run())


def test_the_executors_are_retired_when_the_service_is_collected():
    """A per-instance threadpool that outlives its service is a thread leak,
    and the tests build a great many services."""
    import gc

    service = _make_service()
    asyncio.run(service.get_claims_for_page_async("p001"))
    executors = service._executors
    assert executors, "nothing was built, so nothing is being proved"
    pool = executors["browse"]

    del service
    gc.collect()
    assert executors == {}, "the finalizer did not run"
    assert getattr(pool, "_shutdown", False), "the threadpool was never shut down"


def test_no_executor_crossing_uses_the_SHARED_default_pool():
    """A source guard beside the behavioural ones. `run_in_executor(None, ...)`
    means "the default pool", which is the shared resource the two budgets were
    silently competing over."""
    import inspect
    from shared.discovery_service import DiscoveryService as _DS
    source = inspect.getsource(_DS._run_off_loop)
    assert "run_in_executor(None" not in source, (
        "a crossing dispatches into the SHARED default executor, so the two "
        "budgets are two names for one budget again")
    assert "self._executor_for(" in source


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


def test_a_failing_query_is_an_outage_not_a_genuine_zero(tmp_path):
    """A query that FAILS must never present as `ok` with a total of 0.

    Found against a real PRE-REBUILD asset, which has no
    `discovery_identification` table: the page query raised, the shared query
    helper swallowed the exception into an empty result, and the envelope
    reported `ok` / total 0 -- a false zero on a surface whose rule is to hide
    itself on a zero. Every synthetic fixture in this file carries the new
    tables, so only an asset that genuinely lacks them can reach this
    assertion; the fixture below drops the table to reproduce that shape
    exactly."""
    db_path = _build_d13g_regression_db(tmp_path)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("DROP TABLE discovery_identification")
        conn.commit()
    finally:
        conn.close()

    service = _service_for(db_path, _D13G_VERSION)
    env = service.get_claims_for_page_enveloped(_D13G_SHOWN_PAGE)
    assert env["status"] == STATUS_UNAVAILABLE, (
        "a pre-rebuild asset must read as an OUTAGE, never as 'this manuscript "
        "has no identifications'"
    )
    assert env["meta"]["reason"] == "query_failed"
    assert is_outage(env) is True
    # The legacy list method keeps its own contract: [] on any failure.
    assert service.get_claims_for_page(_D13G_SHOWN_PAGE) == []


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
    assert row["rendered_relation"] == "direct_witness"
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


# ---------------------------------------------------------------------------
# C-track step 3b: the claim row carries the MATRIX output, capped at member
# grain (matrix spec §3.2), read from the identification the row already joins.
# ---------------------------------------------------------------------------

def _service_over_mutated_identifications(tmp_path, sql, params=()):
    """A d13g service reading an asset whose `discovery_identification` table
    was rewritten by `sql`.

    The service opens its sidecar READ-ONLY, so the mutation is applied to the
    FILE before the service exists -- which is the honest way round anyway: what
    these tests prove is that the read path reports what the ASSET says, so the
    only mutation worth making is the asset's own.
    """
    db_path = _build_d13g_regression_db(tmp_path)
    writer = sqlite3.connect(str(db_path))
    try:
        writer.execute(sql, params)
        writer.commit()
    finally:
        writer.close()
    return _service_for(db_path, _D13G_VERSION)


@pytest.mark.parametrize("rendered,expected", (
    ("direct_witness", "direct_witness"),
    ("shared_text", "shared_text"),
    ("quotes_this_work", "quotes_this_work"),
    ("uncertain", "uncertain"),
))
def test_claim_row_relation_is_capped_by_its_identifications_matrix_output(
        tmp_path, rendered, expected):
    """The stored claim type stays put; what the row may SAY follows the
    identification. Driven from the asset four ways, so a presenter that ignored
    the joined column would report `direct_witness` all four times."""
    service = _service_over_mutated_identifications(
        tmp_path, "UPDATE discovery_identification SET rendered_relation = ?",
        (rendered,))
    row = service.get_claims_for_page_enveloped(_D13G_SHOWN_PAGE)["items"][0]
    assert "relation_kind" not in row, (
        "step 3d removed the stored claim type from this surface -- its last "
        "consumer was the expansion anchor, which now travels already capped")
    assert row["rendered_relation"] == expected


def test_a_claim_row_with_no_published_identification_renders_uncertain(tmp_path):
    """Matrix spec §5a.1's resolution at claim grain: no identification, no
    verdict to cap against, so the row asserts nothing.

    Reached only behind the review toggle in the served corpus -- 0 of 150,604
    default rows and 52,510 of 231,322 overall (measured 2026-08-12) -- which is
    exactly why it needs a test rather than a reader to find it.
    """
    service = _service_over_mutated_identifications(
        tmp_path, "DELETE FROM discovery_identification")
    row = service.get_claims_for_page_enveloped(_D13G_SHOWN_PAGE)["items"][0]
    assert row["identification_id"] is None
    assert row["rendered_relation"] == "uncertain"


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


# ===========================================================================
# Phase 136, plan 136-14, Task 2: manuscript scope that NAMES the works
# (D-13h), the related-page count (D-11a), and the page-ID accessor the
# manuscript scope is served by (D-09).
# ===========================================================================

# --- the manuscript-scope fixture ------------------------------------------

_MS_VERSION = "test-manuscript-scope"
_MS_SYS = "990000000000000920"
_MS_PAGES = [f"m_p{i}" for i in range(1, 9)]


def _build_manuscript_scope_db(tmp_path):
    """One manuscript, EIGHT pages, SEVEN distinct canonical works:

    - `w000920` on two pages   -> main pool via multi-folio, page_count 2
    - `w000921` review_only    -> reachable only behind the screening toggle
    - `w000922` EMPTY title    -> the missing-title marker has something to mark
    - `w000923`..`w000926`     -> four more, so pagination has a real total
    """
    works = [
        ("w000920", "w000920", "Synthetic Manuscript Work Alpha", "Synthetic Author P",
         "Synthetic Parent A / Synthetic Leaf A", "sefaria"),
        ("w000921", "w000921", "Synthetic Manuscript Work Beta", None, None, "sefaria"),
        # An EMPTY neutral_title -- `works.neutral_title` is NOT NULL, so this is
        # the only shape a title-less work can actually take in the asset. Four
        # of thirteen sampled manuscripts had no work title available for their
        # "elsewhere" claims, which the panel reference calls a service-layer
        # gap rather than a display choice.
        ("w000922", "w000922", "", None, None, "sefaria"),
        ("w000923", "w000923", "Synthetic Manuscript Work Delta", None, None, "sefaria"),
        ("w000924", "w000924", "Synthetic Manuscript Work Epsilon", None, None, "sefaria"),
        ("w000925", "w000925", "Synthetic Manuscript Work Zeta", None, None, "sefaria"),
        ("w000926", "w000926", "Synthetic Manuscript Work Eta", None, None, "sefaria"),
    ]

    def _ev(page_id, work_id, *, band, routing, adjudication, reason, span=(0, 600),
            matched=500, coverage=0.95):
        return sidecar_build._mk_evidence(
            page_id=page_id, work_id=work_id, sys_id=_MS_SYS,
            evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
            confidence_band=band, adjudication_status=adjudication,
            audit_status=sidecar_build._NA, routing_status=routing, routing_reason=reason,
            span_start=span[0], span_end=span[1], matched_letters=matched, n_spans=1,
            coverage=coverage, page_norm_letters=int(matched / coverage),
        )

    specs = [
        # Alpha across two folios -> main_multifolio.
        _ev("m_p1", "w000920", band=sidecar_build._EXPERT_VERIFIED,
            routing=sidecar_build._SHIPPED, adjudication=sidecar_build._UNREVIEWED,
            reason=sidecar_build._NONE_REASON),
        _ev("m_p2", "w000920", band=sidecar_build._EXPERT_VERIFIED,
            routing=sidecar_build._SHIPPED, adjudication=sidecar_build._UNREVIEWED,
            reason=sidecar_build._NONE_REASON),
        # Beta: review_only + unreviewed -> no identification row at all.
        _ev("m_p3", "w000921", band=sidecar_build._SCREENING_RB,
            routing=sidecar_build._REVIEW_ONLY, adjudication=sidecar_build._UNREVIEWED,
            reason=sidecar_build._LOW_COVERAGE, matched=90, coverage=0.1),
        # The title-less work.
        _ev("m_p4", "w000922", band=sidecar_build._EXPERT_VERIFIED,
            routing=sidecar_build._SHIPPED, adjudication=sidecar_build._UNREVIEWED,
            reason=sidecar_build._NONE_REASON),
    ]
    for i, work_id in enumerate(("w000923", "w000924", "w000925", "w000926")):
        specs.append(_ev(f"m_p{5 + i}", work_id, band=sidecar_build._EXPERT_VERIFIED,
                         routing=sidecar_build._SHIPPED,
                         adjudication=sidecar_build._UNREVIEWED,
                         reason=sidecar_build._NONE_REASON))
    return _new_sidecar(tmp_path, "manuscript-scope.db", works=works,
                        evidence_specs=specs, version=_MS_VERSION)


@pytest.fixture()
def ms_scope_service(tmp_path):
    return _service_for(_build_manuscript_scope_db(tmp_path), _MS_VERSION)


def test_manuscript_scope_names_the_works_with_pages_band_and_gating(ms_scope_service):
    """D-13h: "Elsewhere in this manuscript" reads "Rashi on Song of Songs
    (5 pages), ..." -- NAMES, not a bare count. One row per distinct canonical
    work, each carrying its page count, its strongest band rank and its gating."""
    env = ms_scope_service.get_manuscript_works_enveloped(_MS_PAGES)
    assert env["status"] == STATUS_OK
    assert env["total"] == 7, "one row per DISTINCT canonical work"
    by_work = {row["canonical_work_id"]: row for row in env["items"]}
    assert len(by_work) == 7

    alpha = by_work["w000920"]
    assert alpha["neutral_title"] == "Synthetic Manuscript Work Alpha"
    assert alpha["page_count"] == 2, "the work is identified on two folios"
    assert alpha["best_band_rank"] == _band_rank("track1_direct", "expert_verified")
    assert alpha["gated"] is False
    assert alpha["main_pool"] is True
    # C-track step 3c: the MATRIX output for this work in this manuscript, not
    # the strongest raw claim type over its claims.
    assert alpha["rendered_relation"] == "direct_witness"
    assert "relation_kind" not in alpha, (
        "this surface carries ONE relation field -- it has no anchor and issues "
        "no follow-up query, so the stored value has no second job here")

    # Deterministic ordering: strongest band first, then a stable key.
    ranks = [row["best_band_rank"] for row in env["items"]]
    assert ranks == sorted(ranks)


def test_manuscript_pane_relation_is_the_strongest_MATRIX_output_over_the_group(tmp_path):
    """§3.1's last bullet: strongest-member, but over matrix outputs.

    Driven from the asset three ways. The stored `claim_type` never moves, so a
    query still ranking claim types reports `direct_witness` every time.
    """
    for rendered, expected in (
        ("direct_witness", "direct_witness"),
        ("quotes_this_work", "quotes_this_work"),
        ("shared_text", "shared_text"),
    ):
        variant_dir = tmp_path / rendered
        variant_dir.mkdir()
        db_path = _build_manuscript_scope_db(variant_dir)
        writer = sqlite3.connect(str(db_path))
        try:
            writer.execute(
                "UPDATE discovery_identification SET rendered_relation = ?", (rendered,))
            writer.commit()
        finally:
            writer.close()
        env = _service_for(db_path, _MS_VERSION).get_manuscript_works_enveloped(_MS_PAGES)
        alpha = next(r for r in env["items"] if r["canonical_work_id"] == "w000920")
        assert alpha["rendered_relation"] == expected


def test_a_manuscript_pane_work_with_no_identification_reads_uncertain(tmp_path):
    """The LEFT JOIN misses, so there is no verdict to report -- §3.2a, and the
    same answer the claim rows give. It must not fall back to the strongest
    thing available, which is what a raw-`claim_type` rank would have done."""
    no_ident_dir = tmp_path / "no-ident"
    no_ident_dir.mkdir()
    db_path = _build_manuscript_scope_db(no_ident_dir)
    writer = sqlite3.connect(str(db_path))
    try:
        writer.execute("DELETE FROM discovery_identification")
        writer.commit()
    finally:
        writer.close()
    env = _service_for(db_path, _MS_VERSION).get_manuscript_works_enveloped(_MS_PAGES)
    assert env["items"], "the works must still be NAMED -- the row never vanishes"
    assert {r["rendered_relation"] for r in env["items"]} == {"uncertain"}


def test_gated_work_is_returned_with_the_flag_set_not_omitted(ms_scope_service):
    """The manuscript pane renders a gated work as a dashed chip -- so the
    service must RETURN it, flagged, rather than filter it out. On the mockup's
    teaching case the five folios that make the anchor judgeable were all
    review_only/low_coverage; omitting them removed the context."""
    env = ms_scope_service.get_manuscript_works_enveloped(_MS_PAGES)
    beta = next(r for r in env["items"] if r["canonical_work_id"] == "w000921")
    assert beta["gated"] is True
    assert beta["main_pool"] is not True
    assert beta["neutral_title"] == "Synthetic Manuscript Work Beta"


def test_manuscript_scope_paginates_with_a_real_total(ms_scope_service):
    page1 = ms_scope_service.get_manuscript_works_enveloped(_MS_PAGES, page=1, page_size=3)
    page2 = ms_scope_service.get_manuscript_works_enveloped(_MS_PAGES, page=2, page_size=3)
    page3 = ms_scope_service.get_manuscript_works_enveloped(_MS_PAGES, page=3, page_size=3)
    assert [len(p["items"]) for p in (page1, page2, page3)] == [3, 3, 1]
    for env in (page1, page2, page3):
        assert env["total"] == 7, "the total is the REAL group count, not the page length"
    seen = [r["canonical_work_id"] for p in (page1, page2, page3) for r in p["items"]]
    assert len(set(seen)) == 7, "pagination must be a partition, not a resample"


def test_title_less_work_is_returned_with_an_explicit_missing_title_marker(ms_scope_service):
    env = ms_scope_service.get_manuscript_works_enveloped(_MS_PAGES)
    row = next(r for r in env["items"] if r["canonical_work_id"] == "w000922")
    assert row["title_missing"] is True
    assert row["neutral_title"] is None, (
        "an unavailable title must be an explicit marker the surface can voice, "
        "never a blank chip"
    )
    # Every other row is the control.
    others = [r for r in env["items"] if r["canonical_work_id"] != "w000922"]
    assert all(r["title_missing"] is False for r in others)


def test_manuscript_scope_query_uses_the_claim_page_index(ms_scope_service, tmp_path):
    """D-09: served by `page_id IN (...)` over the browse page's own page list,
    because `discovery_evidence` has NO `sys_id` index. The plan is asserted,
    not assumed."""
    from shared.discovery_service import _build_manuscript_works_sql

    sql = _build_manuscript_works_sql(len(_MS_PAGES))
    conn = ms_scope_service._get_conn()
    plan = " ".join(
        str(row[3]) for row in conn.execute(
            "EXPLAIN QUERY PLAN " + sql, [*_MS_PAGES, 50, 0]).fetchall()
    )
    assert "ix_discovery_claim_page_id" in plan, plan


def test_empty_page_set_is_distinguishable_from_no_identifications(ms_scope_service):
    """T-136-14-11: an unresolvable page set must never render as a genuine
    zero. Both calls return `ok` with total 0 -- the DIFFERENCE is in the meta,
    and that difference is the whole point."""
    unresolved = ms_scope_service.get_manuscript_works_enveloped([])
    assert unresolved["status"] == STATUS_OK
    assert unresolved["total"] == 0
    assert unresolved["meta"]["page_scope_resolved"] is False

    resolved_but_empty = ms_scope_service.get_manuscript_works_enveloped(
        ["a-page-with-no-claims"])
    assert resolved_but_empty["status"] == STATUS_OK
    assert resolved_but_empty["total"] == 0
    assert resolved_but_empty["meta"]["page_scope_resolved"] is True


# --- the related-page count (D-11a) ----------------------------------------

_REL_VERSION = "test-related-pages"
_REL_ANCHOR = "r_p1"


def _build_related_pages_db(tmp_path):
    """FOUR shared-text evidence rows touching the anchor page, over THREE
    distinct opposite pages -- two of them on the same opposite page, and one
    from the b-side. The earlier published figure conflated exactly these:
    40,968 shipped shared-text evidence rows vs 37,397 directed pairs vs
    30,539 unordered pairs. The header counts DISTINCT OPPOSITE PAGES."""
    works = [("w000930", "w000930", "Synthetic Shared Text Work", None, None, "sefaria")]

    def _st(a_page, other_page, sys_id, span):
        return sidecar_build._mk_evidence(
            page_id=a_page, work_id="w000930", sys_id=sys_id,
            evidence_kind=sidecar_build._SHARED_TEXT,
            evidence_source=sidecar_build._PROPAGATED,
            confidence_band=sidecar_build._NOT_EVALUATED,
            adjudication_status=sidecar_build._UNREVIEWED,
            audit_status=sidecar_build._NA, routing_status=sidecar_build._SHIPPED,
            routing_reason=sidecar_build._NONE_REASON,
            span_start=span[0], span_end=span[1],
            other_page_id=other_page, b_start=None, b_end=None,
        )

    specs = [
        _st(_REL_ANCHOR, "r_p2", "990000000000000930", (0, 100)),
        _st(_REL_ANCHOR, "r_p2", "990000000000000930", (200, 300)),  # SAME opposite page
        _st(_REL_ANCHOR, "r_p3", "990000000000000930", (400, 500)),
        _st("r_p4", _REL_ANCHOR, "990000000000000931", (0, 50)),      # the b-side
    ]
    return _new_sidecar(tmp_path, "related-pages.db", works=works,
                        evidence_specs=specs, version=_REL_VERSION)


@pytest.fixture()
def related_service(tmp_path):
    return _service_for(_build_related_pages_db(tmp_path), _REL_VERSION)


def test_related_page_count_is_distinct_opposite_pages_not_evidence_rows(related_service):
    raw_rows = related_service.get_pages_related_to_page(_REL_ANCHOR)
    assert len(raw_rows) == 4, "the fixture must actually contain a duplicate opposite page"

    env = related_service.get_related_page_count_enveloped(_REL_ANCHOR)
    assert env["status"] == STATUS_OK
    assert env["total"] == 3, (
        "the count is DISTINCT opposite pages, deduplicated -- not evidence "
        "rows (4 here) and not directed pairs (D-11a)"
    )
    assert env["total"] != len(raw_rows)
    assert env["items"] == [], (
        "the header shows a count by default; the rows come only behind the "
        "toggle, so the count call returns no rows at all (D-11)"
    )


def test_related_pages_rows_are_returned_separately_from_the_count(related_service):
    env = related_service.get_related_pages_enveloped(_REL_ANCHOR)
    assert env["status"] == STATUS_OK
    assert env["total"] == 3
    ids_seen = [row["related_page_id"] for row in env["items"]]
    assert sorted(ids_seen) == ["r_p2", "r_p3", "r_p4"]
    doubled = next(r for r in env["items"] if r["related_page_id"] == "r_p2")
    assert doubled["evidence_row_count"] == 2, (
        "one row per DISTINCT opposite page, carrying how many evidence rows "
        "collapsed into it"
    )
    single = next(r for r in env["items"] if r["related_page_id"] == "r_p4")
    assert single["evidence_row_count"] == 1

    paged = related_service.get_related_pages_enveloped(_REL_ANCHOR, page=1, page_size=2)
    assert len(paged["items"]) == 2 and paged["total"] == 3


# --- the page-ID accessor (NEW plumbing) -----------------------------------

def _stub_browse_pages(sys_id, n, ie_id="IE500001", start=1):
    return [
        {
            "uid": f"{sys_id}-{i}",
            "p_num": i,
            "ie_id": ie_id,
            "full_header": f"{sys_id}_{ie_id}_P{i:06d}_FL{600000 + i}",
        }
        for i in range(start, start + n)
    ]


class _StubSearcher:
    def __init__(self, browse_map, raises=False):
        self._browse_map = browse_map
        self._raises = raises

    def _load_browse_map(self):
        if self._raises:
            raise RuntimeError("browse map unavailable")
        return self._browse_map


class _StubState:
    def __init__(self, searcher):
        self.searcher = searcher


def test_page_id_accessor_derives_discovery_page_ids_from_the_browse_map(monkeypatch):
    import web.services as web_services

    sys_id = "990000000000000940"
    monkeypatch.setattr(web_services, "state", _StubState(
        _StubSearcher({sys_id: _stub_browse_pages(sys_id, 3)})))

    result = web_services.get_service().get_manuscript_page_ids(sys_id)
    assert result.resolved is True
    assert result.total == 3
    assert result.truncated is False
    assert result.page_ids == [
        f"{sys_id}_IE500001_P000001_FL600001",
        f"{sys_id}_IE500001_P000002_FL600002",
        f"{sys_id}_IE500001_P000003_FL600003",
    ]


def test_page_id_accessor_is_volume_aware_and_bounded(monkeypatch):
    """A multi-volume manuscript can carry many pages (the largest is 427), so
    the accessor is BOUNDED and says when it truncated. It is also
    volume-aware: browse itself navigates one IE at a time."""
    import web.services as web_services

    sys_id = "990000000000000941"
    pages = (_stub_browse_pages(sys_id, 600, ie_id="IE500002")
             + _stub_browse_pages(sys_id, 40, ie_id="IE500003", start=1000))
    monkeypatch.setattr(web_services, "state", _StubState(_StubSearcher({sys_id: pages})))
    service = web_services.get_service()

    bounded = service.get_manuscript_page_ids(sys_id)
    assert bounded.total == 640
    assert len(bounded.page_ids) == web_services.DISCOVERY_PAGE_ID_LIMIT
    assert bounded.truncated is True

    one_volume = service.get_manuscript_page_ids(sys_id, volume_ie="IE500003")
    assert one_volume.total == 40
    assert one_volume.truncated is False
    assert all("IE500003" in pid for pid in one_volume.page_ids)


@pytest.mark.parametrize("searcher,label", [
    (None, "no searcher at all"),
    (_StubSearcher({}), "an empty browse map"),
    (_StubSearcher({}, raises=True), "a browse map that raises"),
])
def test_page_id_accessor_returns_an_explicit_empty_result_rather_than_raising(
        monkeypatch, searcher, label):
    """T-136-14-11: the panel must be able to say "we could not resolve this
    manuscript's pages" instead of querying an empty page set and rendering a
    false zero."""
    import web.services as web_services

    monkeypatch.setattr(web_services, "state", _StubState(searcher))
    result = web_services.get_service().get_manuscript_page_ids("990000000000000942")
    assert result.page_ids == [], label
    assert result.total == 0
    assert result.resolved is False, label


def test_page_id_accessor_runs_off_the_event_loop(monkeypatch):
    """Every read here runs OFF the loop. The web app runs a SINGLE uvicorn
    worker, so a synchronous browse-map load on the loop stalls every
    concurrent request while burning no CPU."""
    import web.discovery as web_discovery
    import web.services as web_services

    sys_id = "990000000000000943"
    monkeypatch.setattr(web_services, "state", _StubState(
        _StubSearcher({sys_id: _stub_browse_pages(sys_id, 2)})))
    monkeypatch.setattr(web_discovery, "discovery_available", lambda: True)

    threads = {}

    async def _run():
        threads["loop"] = threading.get_ident()
        return await web_discovery.get_manuscript_page_ids(sys_id)

    real_accessor = web_services.GenizahService.get_manuscript_page_ids

    def _recording(self, *a, **k):
        threads["worker"] = threading.get_ident()
        return real_accessor(self, *a, **k)

    monkeypatch.setattr(web_services.GenizahService, "get_manuscript_page_ids", _recording)
    env = asyncio.run(_run())

    assert env["status"] == STATUS_OK
    assert env["total"] == 2
    assert env["meta"]["resolved"] is True
    assert threads["worker"] != threads["loop"], (
        "the accessor ran ON the event loop -- it must be dispatched through "
        "the off-loop executor like every other read here"
    )


def test_page_id_helper_rejects_a_header_it_cannot_resolve():
    from web.services import discovery_page_id_from_header

    assert discovery_page_id_from_header("") is None
    assert discovery_page_id_from_header("not a header") is None
    # A LOCAL ("My Library") header carries no IE component and must never be
    # mistaken for a Genizah page.
    assert discovery_page_id_from_header("970000000000000001_F0001_P000001") is None
    # Zero-padding is normalized to the asset's own six-digit form.
    assert discovery_page_id_from_header("990000000000000944_IE1_P2_FL3") == (
        "990000000000000944_IE1_P000002_FL3")


def test_manuscript_scope_and_related_page_wrappers_exist_and_honour_the_browse_timeout(monkeypatch):
    import web.discovery as web_discovery

    monkeypatch.setattr(web_discovery, "discovery_available", lambda: True)
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_BROWSE", "1.75")
    seen = []

    for name, args in (
        ("get_manuscript_works_enveloped", (["m_p1"],)),
        ("get_related_page_count_enveloped", ("r_p1",)),
        ("get_related_pages_enveloped", ("r_p1",)),
    ):
        wrapper = getattr(web_discovery, name)
        assert asyncio.iscoroutinefunction(wrapper), name

        async def _fake(*a, _n=name, **k):
            seen.append((_n, web_discovery._service._browse_timeout()))
            return make_envelope(STATUS_OK, [], 0)

        monkeypatch.setattr(web_discovery._service, name + "_async", _fake)
        env = asyncio.run(wrapper(*args))
        assert env["status"] == STATUS_OK

    assert [t for _n, t in seen] == [1.75, 1.75, 1.75]


# ---------------------------------------------------------------------------
# Envelope safety, BELOW the top level
# (Codex code review 2026-08-03, finding 7 -- MEDIUM)
# ---------------------------------------------------------------------------
#
# `_assert_surface_safe` checked only TOP-LEVEL keys while its docstring claimed
# a hand-built envelope "cannot carry the badge, a precision value or an
# interval". Two shapes walked straight through: a forbidden key one level down
# inside a nested mapping or a list of sub-rows, and the review badge as a VALUE
# under an allowed key.
#
# The tests above assert on envelopes the service produced. These build the
# envelope by hand, which is the case the redundant check exists for -- a future
# caller that skipped surface_safe_*.

def test_nested_forbidden_key_is_rejected():
    from shared.discovery_surface_projection import make_envelope

    with pytest.raises(ValueError) as exc:
        make_envelope("ok", items=[{"work_id": "w1", "nested": {"review_overlay": "x"}}], total=1)
    assert "forbidden surface field" in str(exc.value)
    assert "nested" in str(exc.value), "the error must name where it found it"


def test_forbidden_key_inside_a_list_of_subrows_is_rejected():
    from shared.discovery_surface_projection import make_envelope

    with pytest.raises(ValueError):
        make_envelope(
            "ok",
            items=[{"work_id": "w1", "carriers": [{"ok": 1}, {"ci_low": 0.5}]}],
            total=1,
        )


@pytest.mark.parametrize("badge", ["Expert-reviewed ✓", "נבדק בידי מומחה ✓"])
def test_review_badge_as_a_value_under_an_allowed_key_is_rejected(badge):
    """The shape no key-based check could ever have caught, in both languages."""
    from shared.discovery_surface_projection import make_envelope

    with pytest.raises(ValueError) as exc:
        make_envelope("ok", items=[{"work_id": "w1", "band_label": badge}], total=1)
    assert "badge as a VALUE" in str(exc.value)


def test_badge_in_meta_is_rejected():
    from shared.discovery_surface_projection import make_envelope

    with pytest.raises(ValueError):
        make_envelope("ok", items=[], total=0, meta={"note": "Expert-reviewed ✓"})


def test_machine_vocabulary_in_values_is_NOT_rejected():
    """The counter-control, and the reason this check scans a closed two-item
    badge list rather than a general prohibited vocabulary.

    The projection intentionally carries machine values like `direct_witness`.
    A naive value scan would reject every correct envelope -- a gate that fails
    on valid output costs as much as one that passes on invalid output. If this
    test ever starts failing, the check has been over-broadened and will begin
    rejecting real service output."""
    from shared.discovery_surface_projection import make_envelope

    env = make_envelope(
        "ok",
        items=[{"work_id": "w1", "relation": "direct_witness",
                "novelty_status": "refines_granularity",
                "bucket": "more", "band_label": "Strong match"}],
        total=1,
        meta={"basis": "main_pool", "audience": "public"},
    )
    assert env["status"] == "ok"


# ---------------------------------------------------------------------------
# The offload boundary, proven by THREAD IDENTITY
# (Codex code review 2026-08-03, finding 10 -- LOW)
# ---------------------------------------------------------------------------
#
# `tests/test_no_await_sync_function.py` detects exactly one shape: `await
# name()` where `name` is a synchronous function defined in the SAME module. It
# cannot see a direct blocking call, an imported function, or a synchronous
# method invoked inside an async handler -- and one of its own tests explicitly
# blesses the direct-call shape. So the repo-wide AST guard is narrower than the
# property everyone relies on: that no discovery read runs on the event loop.
#
# This app runs ONE uvicorn worker, so a blocking read on the loop stalls every
# concurrent request while burning no CPU -- invisible in load average, which is
# how it went undiagnosed for weeks in 2026-07. Thread identity is the property
# that actually matters, and unlike an AST pattern it cannot be satisfied by
# accident.

def test_enveloped_reads_execute_off_the_event_loop_thread():
    """Behavioural counterpart to the AST guard: the query body must run on a
    DIFFERENT thread from the event loop."""
    import threading

    service = _make_service()
    seen = {}
    original = service.get_claims_for_page_enveloped

    def _recording(*args, **kwargs):
        seen["thread_id"] = threading.get_ident()
        return original(*args, **kwargs)

    service.get_claims_for_page_enveloped = _recording

    async def _run():
        seen["loop_thread_id"] = threading.get_ident()
        return await service.get_claims_for_page_enveloped_async("p001")

    env = asyncio.run(_run())
    loop_thread_id = seen["loop_thread_id"]

    assert env["status"] in ("ok", "unavailable", "timeout", "busy")
    assert "thread_id" in seen, (
        "the enveloped wrapper never called the sync query at all -- this test "
        "would pass vacuously; re-point it at a wrapper that does"
    )
    assert seen["thread_id"] != loop_thread_id, (
        "a discovery read executed on the event-loop thread. One uvicorn worker "
        "means this stalls every concurrent request while burning no CPU, so it "
        "is invisible in load average -- exactly the 2026-07 failure mode."
    )


# ---------------------------------------------------------------------------
# Code review 2A, finding 7: a rate under an INNOCUOUS key.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,shape", [
    ("matches 91% of the time", "a percentage"),
    ("[0.88, 0.94]", "a confidence interval"),
    ("accuracy 0.91", "an accuracy rate"),
])
@pytest.mark.parametrize("holder", ["label", "note", "band_label"])
def test_a_rate_under_an_allowed_key_never_reaches_a_surface(value, shape, holder):
    """Recursive KEY checking and the two badge strings both passed these.

    The forbidden-field set catches `ci_low`; the badge markers catch
    "Expert-reviewed". Neither sees a percentage written into `label`, which is
    precisely what the no-precision rule exists to stop reaching a reader."""
    with pytest.raises(ValueError) as exc:
        make_envelope("ok", items=[{holder: value}], total=1)
    assert shape in str(exc.value), (
        f"refused for the wrong reason -- expected {shape!r}: {exc.value}"
    )
    # and the same value in meta, which is a separate walk
    with pytest.raises(ValueError):
        make_envelope("ok", items=[], total=0, meta={holder: value})


@pytest.mark.parametrize("value", [
    "direct_witness", "quotes_this_work", "low_coverage", "fills_gap",
    "Talmud / Bavli", "T-S 12.123", "V0.8", "version 2.1", "1.25 seconds",
    "1,329 matched letters", "5,684 units",
    # REAL shelfmarks that this gate actually rejected, live, on 2026-08-07.
    # `T-S 12.123` above could never have caught it: the rule needs a rate WORD
    # near a decimal, and the Cambridge Fragment-of-a-Fragment class is the one
    # shelfmark family that contains one -- `F1`, four characters from `.2`,
    # which `\d*\.\d+` read as a decimal because the integer part was optional.
    # 39 shelfmarks / 51 identifications / 37 manuscripts, and because the check
    # guards the WHOLE envelope, every reader whose filters touched one lost the
    # entire findings page rather than a row.
    "T-S F1(1).2", "T-S F1(2).121", "T-S F1(1).114", "T-S F1.2",
    # The same fraction-shaped tail, glued to a segment, in other families --
    # the sibling detector in `tests/render_smoke/discovery_honesty_gate.py`
    # cites `MS Heb c.57` for exactly this reason.
    "MS Heb c.57", "Or.1080 J266", "CUL Or.1081 2.75",
    # Percent-encoded UTF-8 in a provenance URL is an address, not a precision
    # claim.  This exact shape occurs in 15,107 baked excerpt attributions; the
    # old detector read the ``7%`` in ``%D7%AA`` as a published rate and made
    # every affected text-match button fail closed.
    "Original source: http://he.wikisource.org/wiki/%D7%AA%D7%9C%D7%9E%D7%95%D7%93",
])
def test_the_rate_check_does_not_reject_valid_envelope_values(value):
    """The false-positive half, and the reason this is a SHAPE rule rather than
    a vocabulary scan.

    The projection legitimately carries machine enums, shelfmarks, genre paths
    and version markers. A general prohibited-word sweep over every string would
    reject correct envelopes -- a gate that fails on valid output costs as much
    as one that passes on invalid output, which is the defect class this phase
    produced most often."""
    env = make_envelope("ok", items=[{"label": value}], total=1,
                        meta={"note": value})
    assert env["status"] == "ok"


def test_no_real_display_value_in_the_artifact_trips_the_rate_check():
    """THE CHECK THAT WOULD HAVE CAUGHT THIS, run over the real corpus.

    Every hand-written false-positive list is a list of shapes someone thought
    of. `T-S 12.123` was in ours for months and could not have caught `T-S
    F1(1).2`, because nobody writing it was thinking about a shelfmark that
    contains a rate word. So this sweeps every value the artifact can actually
    put on a surface -- shelfmarks, library codes, work titles, authors, genres
    -- through the real predicate.

    Deliberately over the CORPUS rather than over examples: the defect was a
    collision between a real naming convention and a regex, and only real names
    can find the next one.
    """
    from shared.discovery_surface_projection import _rate_or_interval_violation
    from tests.test_discovery_launch_stats import resolve_guard_artifact

    path, reason = resolve_guard_artifact()
    if path is None:
        pytest.skip((reason or "no resolvable discovery artifact") +
                    " -- set DISCOVERY_LAUNCH_GUARD_DB to sweep the real corpus")

    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        values = []
        for sql in (
            "SELECT shelfmark_display FROM manuscript_display",
            "SELECT library_code FROM manuscript_display",
            "SELECT neutral_title FROM works",
            "SELECT author FROM works",
            "SELECT genre FROM works",
            "SELECT attribution FROM discovery_excerpt",
        ):
            values.extend(str(v) for (v,) in conn.execute(sql) if v is not None)
    finally:
        conn.close()

    assert values, "swept nothing -- the artifact resolved but carries no display values"
    tripped = sorted({v for v in values if _rate_or_interval_violation(v)})
    assert not tripped, (
        f"{len(tripped)} real display value(s) are read as a precision claim and "
        f"would raise, taking the WHOLE envelope down: {tripped[:10]}"
    )


@pytest.mark.parametrize("interval", [
    "[0.88, 0.94]", "(0.88-0.94)", "[0.88 , 0.94]", "(0.9–0.95)",
    # The BARE-decimal spelling, which a paper writes as readily as the full one.
    # It is why the interval rule deliberately does NOT get the rate rule's
    # not-glued-to-a-word-character escape: an opening bracket is neither a word
    # character nor `)`, so `[.88, .94]` and a hypothetical `(.2, .3)` shelfmark
    # are the SAME shape to any lookbehind, and only one of them is real.
    "[.88, .94]", "(.88-.94)",
])
def test_a_confidence_interval_is_refused_in_both_spellings(interval):
    """A Codex review (2026-08-07) proposed relaxing this rule the way the rate
    rule was relaxed, on the strength of `MS Heb c.57 (.2, .3)`. This pins why
    that would be wrong: the relaxation cannot tell that string from a real
    interval, and the string is not a real shelfmark (see the sweep below)."""
    with pytest.raises(ValueError) as exc:
        make_envelope("ok", items=[{"note": interval}], total=1)
    assert "a confidence interval" in str(exc.value)


def test_no_real_shelfmark_anywhere_carries_a_bracketed_decimal_pair():
    """The other half of that judgement, measured rather than assumed.

    Swept over EVERY `call_numbers` variant in libraries.csv -- ~720k of them,
    not just the one shortest form the artifact stores for display, because the
    catalogue is where a naming convention would show up first. If this ever
    fails, the interval rule has acquired a real false positive and the trade-off
    recorded on `_INTERVAL_RE` has to be re-decided with the new evidence.
    """
    import csv as _csv
    import re as _re
    from pathlib import Path

    catalogue = Path(__file__).resolve().parents[1] / "libraries.csv"
    if not catalogue.is_file():
        pytest.skip(f"{catalogue} is absent -- the catalogue sweep needs it")

    pair = _re.compile(r"[\[(]\s*\d*\.\d+\s*[,–-]\s*\d*\.\d+\s*[\])]")
    scanned = 0
    offenders = []
    with open(catalogue, encoding="utf-8-sig", newline="") as handle:
        for row in _csv.reader(handle):
            if not row or len(row) < 3:
                continue
            for variant in row[2].split("|"):
                variant = variant.strip()
                if not variant:
                    continue
                scanned += 1
                if pair.search(variant):
                    offenders.append(variant)

    assert scanned > 500_000, (
        f"only {scanned} call-number variants scanned -- the sweep is reading "
        "the wrong column or the wrong file")
    assert not offenders, (
        f"{len(offenders)} real shelfmark(s) carry a bracketed decimal pair and "
        f"would be refused as a confidence interval: {offenders[:10]}")


def test_a_bare_decimal_beside_a_rate_word_is_still_refused():
    """The fix's OWN boundary, and why it is not simply "require an integer part".

    Requiring `\\d+\\.\\d+` alone would clear every shelfmark -- and would also
    let `accuracy .88` through, which is a precision claim written the way
    people actually write one. So the decimal rule admits a BARE `.NN` too, and
    excludes it only when glued to a word character or a closing paren, which is
    what a shelfmark's tail always is (`F1(1).2`, `c.57`) and what a written rate
    never is.

    This is the half of the rule a "just relax it" fix would have dropped
    silently, so it is pinned separately from the shelfmark list above.
    """
    with pytest.raises(ValueError) as exc:
        make_envelope("ok", items=[{"label": "accuracy .88"}], total=1)
    assert "an accuracy rate" in str(exc.value)


def test_thread_identity_control_would_catch_an_on_loop_read():
    """Positive control: the assertion above is only meaningful if calling the
    sync path directly on the loop would FAIL it. Proven by doing exactly that,
    rather than asserting the detector's shape."""
    import threading

    service = _make_service()
    observed = {}

    async def _run():
        observed["loop_thread_id"] = threading.get_ident()
        # deliberately NOT offloaded -- the shape the guard exists to reject
        service.get_claims_for_page_enveloped("p001")
        observed["call_thread_id"] = threading.get_ident()

    asyncio.run(_run())

    assert observed["call_thread_id"] == observed["loop_thread_id"], (
        "a direct synchronous call somehow left the loop thread -- this control "
        "no longer demonstrates the failure it exists to demonstrate"
    )


# ===========================================================================
# WHERE THE MATCH IS: the first matched folio, resolved at read time
# (owner report, 2026-08-08).
#
# The findings row's preview opened `/browse` at the manuscript's FIRST page,
# and the row copy named that as a structural limit -- "a findings row carries
# `page_count` and NO folio identifier" -- with the folio deferred to a future
# bake. The first clause was true of the GRAIN and false of the ASSET: every
# contributing page id is already on `discovery_evidence.a_page_id`, aggregated
# away only when `populate_discovery_identification` collapses a group into one
# row. These tests pin the read-time resolution that recovers it.
# ===========================================================================

from shared.discovery_service import (  # noqa: E402 -- appended section
    BUCKET_ALL,
    DIVERGENCE_SHOWN,
    _browse_address_from_page_id,
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNIT_MANUSCRIPT,
    FINDINGS_UNIT_WORK,
)

_FOLIO_VERSION = "test-first-matched-folio"
_FOLIO_SYS = "990000000000000905"
#: TWO volumes, with the LOWER folio number in the LEXICOGRAPHICALLY LATER one.
#: That inversion is the whole point of the fixture: a rule that took "the
#: lowest folio number in the manuscript" would answer folio 2 and cross a
#: volume boundary to do it, and folio 2 of volume 2 is not earlier than folio 3
#: of volume 1.
_FOLIO_VOL_A = "IE100000001"
_FOLIO_VOL_B = "IE100000002"
_FOLIO_WORK = "w000905"


def _folio_page(volume: str, folio: int) -> str:
    """A page id in the CORPUS's own shape, `{sys_id}_{ie_id}_P{n:06d}_{fl_id}`.

    Built here rather than hand-written per row so a fixture cannot drift from
    the shape the resolver's range predicate and the two parsers depend on.
    """
    return f"{_FOLIO_SYS}_{volume}_P{folio:06d}_FL{int(volume[2:]) + folio}"


def _build_first_folio_db(tmp_path):
    works = [(_FOLIO_WORK, _FOLIO_WORK, "Synthetic Multi Folio Work",
              "Synthetic Author F", "Synthetic Parent A / Synthetic Leaf A",
              "sefaria")]

    def _row(page_id, *, shipped=True, reviewed=False):
        return sidecar_build._mk_evidence(
            page_id=page_id, work_id=_FOLIO_WORK, sys_id=_FOLIO_SYS,
            evidence_kind=sidecar_build._WITNESS,
            evidence_source=sidecar_build._TRACK1,
            confidence_band=sidecar_build._TIER_A,
            adjudication_status=(sidecar_build._HUMAN_CONFIRMED if reviewed
                                 else sidecar_build._UNREVIEWED),
            audit_status=sidecar_build._NA,
            routing_status=(sidecar_build._SHIPPED if shipped
                            else sidecar_build._REVIEW_ONLY),
            routing_reason=(sidecar_build._NONE_REASON if shipped
                            else sidecar_build._LOW_COVERAGE),
            span_start=0, span_end=900, matched_letters=880, n_spans=1,
            coverage=0.9, page_norm_letters=978,
        )

    specs = [
        # INELIGIBLE, and it sorts FIRST of all four. Neither shipped nor human
        # confirmed, so `populate_discovery_identification` excludes it from the
        # group and `page_count` does not count it -- a resolver that dropped
        # the eligibility clause would point every reader of this row at a folio
        # the row itself does not claim.
        _row(_folio_page(_FOLIO_VOL_A, 1), shipped=False, reviewed=False),
        _row(_folio_page(_FOLIO_VOL_A, 3)),
        _row(_folio_page(_FOLIO_VOL_A, 9)),
        _row(_folio_page(_FOLIO_VOL_B, 2)),
    ]
    return _new_sidecar(tmp_path, "first_folio.db", works=works,
                        evidence_specs=specs, version=_FOLIO_VERSION)


@pytest.fixture()
def folio_service(tmp_path):
    return _service_for(_build_first_folio_db(tmp_path), _FOLIO_VERSION)


def _folio_leaf_row(service):
    env = service.get_findings_enveloped(
        unit=FINDINGS_UNIT_IDENTIFICATION, bucket=BUCKET_ALL,
        divergence=DIVERGENCE_SHOWN)
    assert env["status"] == STATUS_OK and env["items"], env
    return env["items"][0]


def test_the_leaf_row_carries_the_FIRST_matched_folio_and_its_volume(folio_service):
    """The two components of one `/browse` address, parsed in the SERVICE."""
    item = _folio_leaf_row(folio_service)
    assert item["first_match_page"] == 3, item
    assert item["first_match_volume_ie"] == _FOLIO_VOL_A, item


def test_the_first_folio_never_crosses_a_VOLUME_boundary_for_a_lower_number(
        folio_service):
    """Folio 2 of volume 2 is not earlier than folio 3 of volume 1.

    988 of the served artifact's 53,581 identifications span more than one
    volume, so a rule that ordered by bare folio number would send that
    population into the wrong volume -- and a reader cannot see that they are in
    the wrong volume, which makes it worse than a link that plainly failed.
    """
    item = _folio_leaf_row(folio_service)
    assert item["first_match_page"] != 2, (
        "the resolver crossed into a later volume to answer with a lower folio "
        "number")
    assert item["first_match_volume_ie"] != _FOLIO_VOL_B


def test_an_INELIGIBLE_page_is_never_the_folio_the_preview_opens(folio_service):
    """THE eligibility gate, and the one that can really fail.

    The ineligible row sorts FIRST of the fixture's four, so a resolver that
    dropped `routing_status='shipped' OR adjudication_status='human_confirmed'`
    answers folio 1 and this test fails. `page_count` counts three pages, and
    the folio a reader is sent to has to be one of the three the row is
    counting -- otherwise the row's own number denies the page it opened.
    """
    item = _folio_leaf_row(folio_service)
    assert item["first_match_page"] != 1, (
        "the preview targets a page the identification's own page_count "
        "excludes -- the eligibility predicate is not being applied")
    assert item["page_count"] == 3, item


def test_the_first_matched_folio_is_resolved_on_the_LEAF_unit_only(folio_service):
    """A work row spans manuscripts and a manuscript row spans works, so "the
    first matched folio" on either would have to CHOOSE which of the row's
    candidates to answer for -- and choosing between a row's candidates is
    adjudication, the one thing no surface in this phase does.

    THE RESOLVER MUST NOT BE CALLED ON A GROUPED UNIT, and asserting the CALL is
    the only way to measure that. The output alone proves nothing: a grouped row
    carries a NULL on one half of the lookup key, so the lookup misses and the
    field comes back None whether the guard is there or not -- an earlier version
    of this test asserted exactly that and passed with the guard deleted. What
    the guard actually buys is not running the query at all, once per grouped
    page, on a single-uvicorn-worker server.
    """
    calls = []
    original = folio_service._first_match_pages

    def _spy(conn, sys_ids):
        calls.append(list(sys_ids))
        return original(conn, sys_ids)

    folio_service._first_match_pages = _spy
    try:
        for unit in (FINDINGS_UNIT_MANUSCRIPT, FINDINGS_UNIT_WORK):
            env = folio_service.get_findings_enveloped(
                unit=unit, bucket=BUCKET_ALL, divergence=DIVERGENCE_SHOWN)
            assert env["status"] == STATUS_OK and env["items"], (unit, env)
            for item in env["items"]:
                assert item["first_match_page"] is None, (unit, item)
                assert item["first_match_volume_ie"] is None, (unit, item)
        assert calls == [], (
            "the folio resolver ran for a grouped unit, which has no single "
            "folio to resolve: {}".format(calls))
        # ...and the SAME spy sees the leaf ask for it, so "no calls" above is a
        # measured absence rather than a spy that was never wired up.
        _folio_leaf_row(folio_service)
        assert len(calls) == 1 and calls[0] == [_FOLIO_SYS], calls
    finally:
        folio_service._first_match_pages = original


def test_a_page_id_without_the_corpus_shape_resolves_to_NOTHING():
    """Both components are None TOGETHER, so the surface has one state to
    branch on and `preview_url` cannot build a half-resolved address."""
    from shared.discovery_service import _browse_address_from_page_id

    for bad in (None, "", 12, "d13g_p22", "990000_IE1_FL9"):
        assert _browse_address_from_page_id(bad) == (None, None), bad
    good = _folio_page(_FOLIO_VOL_A, 3)
    assert _browse_address_from_page_id(good) == (3, _FOLIO_VOL_A)


def test_a_FOLIO_WITHOUT_ITS_VOLUME_is_withheld_rather_than_half_answered():
    """A page id carrying a folio and NO volume must resolve to NEITHER.

    Found by review, and it is the one failure this whole change exists to
    prevent, reached from the other side. The two parsers accept different
    things -- `_page_number_from_page_id` matches a bare `_P<n>_` anywhere while
    the volume parser needs the full `_IE<n>_P<n>_` run -- so `9900_P000007_FL1`
    parses to a folio and no volume. A consumer that asked only "is there a
    folio?" would then print "opens at a folio the match was found on" over a
    link that, lacking the volume, opens the manuscript. The note would be
    claiming more than the link delivers, on the exact rows where a reader is
    least able to tell.
    """
    from shared.discovery_service import (
        _browse_address_from_page_id, _page_number_from_page_id)

    partial = "9900_P000007_FL1"
    # The half that makes the hazard real: the folio parser DOES answer here.
    assert _page_number_from_page_id(partial) == 7
    # ...and the atomic accessor refuses to pass it on as half an address.
    assert _browse_address_from_page_id(partial) == (None, None)
    # A zero or negative folio is not an address either -- `/browse` clamps it
    # to page 1 silently, which is the unresolved case wearing a folio's clothes.
    assert _browse_address_from_page_id(f"{_FOLIO_SYS}_{_FOLIO_VOL_A}_P000000_FL1") == (
        None, None)


def test_a_related_page_row_names_the_VOLUME_its_folio_number_belongs_to(
        tmp_path):
    """The candidate-alignment row already carried `page_number` and NOT the
    volume, so the panel linked `&page=<n>` alone -- a real folio in an unknown
    volume (owner request, 2026-08-08).

    ASSERTED AT THE SERVICE, not only at the renderer, and the difference is the
    reason this test exists: the panel's own tests build their rows by hand, so
    the service could stop emitting the volume entirely and every render test
    would still pass while every production link lost its folio. A mutation
    proved exactly that before this was written.
    """
    anchor = _folio_page(_FOLIO_VOL_A, 3)
    other = f"990000000000000906_{_FOLIO_VOL_B}_P000012_FL{_FOLIO_VOL_B[2:]}12"
    works = [(_FOLIO_WORK, _FOLIO_WORK, "Synthetic Shared Text Work", None,
              None, "sefaria")]
    specs = [sidecar_build._mk_evidence(
        page_id=anchor, work_id=_FOLIO_WORK, sys_id=_FOLIO_SYS,
        evidence_kind=sidecar_build._SHARED_TEXT,
        evidence_source=sidecar_build._PROPAGATED,
        confidence_band=sidecar_build._NOT_EVALUATED,
        adjudication_status=sidecar_build._UNREVIEWED,
        audit_status=sidecar_build._NA,
        routing_status=sidecar_build._SHIPPED,
        routing_reason=sidecar_build._NONE_REASON,
        span_start=0, span_end=400, other_page_id=other, b_start=0, b_end=400,
    )]
    service = _service_for(
        _new_sidecar(tmp_path, "related_volume.db", works=works,
                     evidence_specs=specs, version=_FOLIO_VERSION),
        _FOLIO_VERSION)
    env = service.get_related_pages_enveloped(anchor)
    assert env["status"] == STATUS_OK and env["items"], env
    row = env["items"][0]
    assert row["page_number"] == 12, row
    assert row["volume_ie"] == _FOLIO_VOL_B, row


def test_an_expansion_row_names_its_representative_claims_folio_and_volume(
        folio_service):
    """The panel's "other manuscripts carrying this work" row linked to the
    manuscript's first page while already holding the ranked representative
    claim's own page id. Asserted at the SERVICE for the same reason as the
    related-page pair above."""
    rows = folio_service.get_work_witnesses(_FOLIO_WORK)
    assert rows, "the fixture produced no expansion rows"
    row = rows[0]
    page, volume = _browse_address_from_page_id(row["representative_page_id"])
    assert row["representative_page"] == page
    assert row["representative_volume_ie"] == volume
    # ...and it is a REAL address, not two Nones agreeing with each other.
    assert isinstance(row["representative_page"], int)
    assert row["representative_volume_ie"] == _FOLIO_VOL_A


def test_a_FAILED_folio_resolution_costs_the_link_and_never_the_result_set(
        folio_service):
    """A folio is a link detail. Losing one must degrade the preview to the
    manuscript it always opened -- never take down a result page that has
    already been fetched."""
    class _Broken:
        def execute(self, *_a, **_k):
            raise sqlite3.OperationalError("no such table: discovery_evidence")

    assert folio_service._first_match_pages(_Broken(), [_FOLIO_SYS]) == {}
    # ...and with the resolver returning nothing, the rows still arrive.
    original = folio_service._first_match_pages
    folio_service._first_match_pages = lambda *_a, **_k: {}
    try:
        item = _folio_leaf_row(folio_service)
    finally:
        folio_service._first_match_pages = original
    assert item["first_match_page"] is None
    assert item["sys_id"] == _FOLIO_SYS


# ===========================================================================
# PLAN-textvtext-excerpts.md, Track D: the text-vs-text excerpt read path --
# `excerpts_available()` (path/version-aware, per (c)), `get_excerpt_enveloped`
# / `get_excerpt_enveloped_async` (the ENVELOPED-end-to-end shape a Codex
# pre-flight review required, mirroring `get_work_expansion_enveloped` rather
# than `get_evidence`'s legacy list-swallows-failures shape), and the
# `SURFACE_EXCERPT_FIELDS` allowlist.
#
# Masking discipline unchanged: every fixture below is fabricated in-test via
# scripts/build_discovery_sidecar (synthetic ids, synthetic titles) plus a
# hand-built `discovery_excerpt` table -- `scripts/bake_discovery_excerpts.py`,
# the real producer named in the plan, does not exist yet, so this table is
# built directly here, exactly the way other ad hoc schema pieces in this file
# are. Never real research data.
# ===========================================================================

from shared.discovery_surface_projection import (  # noqa: E402 -- appended section
    SURFACE_EXCERPT_FIELDS,
    surface_safe_excerpt,
)

_EXCERPT_VERSION = "test-excerpt-v1"
_EXCERPT_WORK = "w000950"
_EXCERPT_SYS = "990000000000000950"
_EXCERPT_PAGE = "excerpt_p01"

_EXCERPT_TABLE_SQL = """
CREATE TABLE discovery_excerpt(
  identification_id TEXT PRIMARY KEY,
  evidence_id TEXT NOT NULL, a_page_id TEXT NOT NULL,
  frag_before TEXT NOT NULL, frag_span TEXT NOT NULL, frag_after TEXT NOT NULL,
  frag_clipped INTEGER NOT NULL,
  work_before TEXT, work_span TEXT, work_after TEXT, work_clipped INTEGER,
  work_source TEXT, align_score REAL, attribution TEXT, n_spans INTEGER,
  text_layer TEXT,
  frag_hl TEXT, work_hl TEXT, work_markup TEXT
)
"""


def _build_excerpt_base_db(tmp_path, name, version):
    """A minimal sidecar with ONE real, materialized identification -- the
    same real builder pipeline (`_new_sidecar` / `populate_discovery_identification`)
    every other integration fixture in this file uses, so the
    `identification_id` an excerpt row keys on is a genuine one, never a
    made-up string. Carries NO `discovery_excerpt` table: callers add one via
    `_add_excerpt_table` when a test wants the feature PRESENT; the bare db
    returned here is itself the "older asset" fixture.
    """
    works = [
        (_EXCERPT_WORK, _EXCERPT_WORK, "Synthetic Excerpt Work", "Synthetic Author E",
         "Synthetic Parent A / Synthetic Leaf A", "sefaria"),
    ]
    specs = [
        sidecar_build._mk_evidence(
            page_id=_EXCERPT_PAGE, work_id=_EXCERPT_WORK, sys_id=_EXCERPT_SYS,
            evidence_kind=sidecar_build._WITNESS, evidence_source=sidecar_build._TRACK1,
            confidence_band=sidecar_build._TIER_A,
            adjudication_status=sidecar_build._HUMAN_CONFIRMED,
            audit_status=sidecar_build._NA,
            routing_status=sidecar_build._SHIPPED,
            routing_reason=sidecar_build._NONE_REASON,
            span_start=0, span_end=900, matched_letters=880, n_spans=1,
            coverage=0.9, page_norm_letters=978,
        ),
    ]
    return _new_sidecar(tmp_path, name, works=works, evidence_specs=specs, version=version)


def _identification_id_in(db_path):
    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT identification_id FROM discovery_identification LIMIT 1"
        ).fetchone()
        assert row is not None, "fixture produced no discovery_identification row"
        return row[0]
    finally:
        conn.close()


def _add_excerpt_table(db_path, identification_id, *, with_marker=True, row=None):
    """Adds `discovery_excerpt` (+ its bake-time marker, unless suppressed) to
    an already-built sidecar -- the shape `scripts/bake_discovery_excerpts.py`
    is meant to produce, per the plan. `with_marker=False` lets the AND-gate
    test below build the table WITHOUT the marker, and the older-asset tests
    simply never call this at all -- so both halves of the conjunction are
    independently reachable.
    """
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(_EXCERPT_TABLE_SQL)
        fields = {
            "identification_id": identification_id,
            "evidence_id": "test-evidence-950",
            "a_page_id": _EXCERPT_PAGE,
            "frag_before": "before text ",
            "frag_span": "MATCHED FRAGMENT SPAN",
            "frag_after": " after text",
            "frag_clipped": 0,
            "work_before": "work before ",
            "work_span": "MATCHED WORK SPAN",
            "work_after": " work after",
            "work_clipped": 0,
            "work_source": "direct",
            "align_score": None,
            "attribution": "Synthetic Edition Attribution",
            "n_spans": 1,
            "text_layer": "htr",
        }
        if row:
            fields.update(row)
        conn.execute(
            "INSERT INTO discovery_excerpt (%s) VALUES (%s)" % (
                ", ".join(fields), ", ".join("?" for _ in fields)),
            tuple(fields.values()),
        )
        if with_marker:
            conn.execute(
                "INSERT INTO meta (key, value) VALUES ('excerpt_schema_version', 'excerpt-v1')"
            )
        conn.commit()
    finally:
        conn.close()


def test_excerpt_round_trips_through_the_envelope_with_only_allowlisted_fields(tmp_path):
    db_path = _build_excerpt_base_db(tmp_path, "excerpt-roundtrip.db", _EXCERPT_VERSION)
    ident_id = _identification_id_in(db_path)
    _add_excerpt_table(db_path, ident_id)
    service = _service_for(db_path, _EXCERPT_VERSION)

    env = asyncio.run(service.get_excerpt_enveloped_async(ident_id))

    assert env["status"] == STATUS_OK
    assert env["total"] == 1
    assert len(env["items"]) == 1
    item = env["items"][0]
    assert set(item) == set(SURFACE_EXCERPT_FIELDS)
    assert item["identification_id"] == ident_id
    assert item["frag_span"] == "MATCHED FRAGMENT SPAN"
    assert item["work_span"] == "MATCHED WORK SPAN"
    assert item["attribution"] == "Synthetic Edition Attribution"
    assert item["work_source"] == "direct"


def test_an_identification_with_no_excerpt_row_is_an_honest_empty_not_an_outage(tmp_path):
    db_path = _build_excerpt_base_db(tmp_path, "excerpt-empty.db", _EXCERPT_VERSION)
    ident_id = _identification_id_in(db_path)
    _add_excerpt_table(db_path, ident_id)  # the row exists for THIS id only
    service = _service_for(db_path, _EXCERPT_VERSION)

    env = asyncio.run(
        service.get_excerpt_enveloped_async(ident_id + "-does-not-exist"))

    assert env["status"] == STATUS_OK
    assert env["items"] == []
    assert env["total"] == 0
    assert is_outage(env) is False


def test_excerpts_unavailable_on_an_older_asset_and_the_read_stays_safe(tmp_path):
    """Older asset: no `discovery_excerpt` table, no marker. The UI's
    "old-asset/new-code: toggle hidden" rule means a real caller never reaches
    `get_excerpt_enveloped_async` here at all -- `excerpts_available()` gates
    that -- but the call must still be safe rather than crash, and it must
    report a named outage (`unavailable`) rather than a silent ok-with-zero
    that a caller which skipped the gate could mistake for "no excerpt"."""
    db_path = _build_excerpt_base_db(tmp_path, "excerpt-old-asset.db", _EXCERPT_VERSION)
    ident_id = _identification_id_in(db_path)
    # deliberately NOT calling _add_excerpt_table.
    service = _service_for(db_path, _EXCERPT_VERSION)

    assert service.excerpts_available() is False
    assert asyncio.run(service.excerpts_available_async()) is False

    env = asyncio.run(service.get_excerpt_enveloped_async(ident_id))
    assert env["status"] == STATUS_UNAVAILABLE
    assert env["items"] == []
    assert env["total"] == 0
    assert is_outage(env) is True


def test_excerpts_available_requires_both_the_marker_and_the_table(tmp_path):
    """Proves the AND is real, not just documented: EITHER half missing must
    still read False, so a partially-baked asset (a marker written before its
    table, or a table copied without its meta row) never turns the toggle on."""
    version = "excerpt-and-gate"

    marker_only_db = _build_excerpt_base_db(tmp_path, "excerpt-marker-only.db", version)
    conn = sqlite3.connect(marker_only_db)
    conn.execute(
        "INSERT INTO meta (key, value) VALUES ('excerpt_schema_version', 'excerpt-v1')")
    conn.commit()
    conn.close()
    assert _service_for(marker_only_db, version).excerpts_available() is False

    table_only_db = _build_excerpt_base_db(tmp_path, "excerpt-table-only.db", version)
    _add_excerpt_table(
        table_only_db, _identification_id_in(table_only_db), with_marker=False)
    assert _service_for(table_only_db, version).excerpts_available() is False

    both_db = _build_excerpt_base_db(tmp_path, "excerpt-both.db", version)
    _add_excerpt_table(both_db, _identification_id_in(both_db), with_marker=True)
    assert _service_for(both_db, version).excerpts_available() is True


def test_excerpts_available_is_path_aware_not_just_version_aware(tmp_path):
    """Mirrors the hazard `get_launch_stats_enveloped_async` documents at
    length: `sidecar_version` can stay IDENTICAL across a resolved-path swap
    (the pre-rebuild asset, the private rebuild and the public projection all
    three report the SAME version locally), so `excerpts_available()` caches
    per `(path, version)` -- like `_band_measurements`/`_launch_stats_cache`
    -- specifically so a path swap under a CONSTANT version flips the answer
    instead of replaying the previous asset's."""
    constant_version = "excerpt-avail-constant-version"
    old_db = _build_excerpt_base_db(tmp_path, "excerpt-avail-old.db", constant_version)
    new_db = _build_excerpt_base_db(tmp_path, "excerpt-avail-new.db", constant_version)
    _add_excerpt_table(new_db, _identification_id_in(new_db))
    # old_db deliberately carries no discovery_excerpt table.

    state = {"path": old_db}
    service = DiscoveryService(
        path_provider=lambda: state["path"],
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: constant_version,
    )

    assert service.excerpts_available() is False

    state["path"] = new_db
    assert service.excerpts_available() is True, (
        "a path swap at a CONSTANT version must not serve the PREVIOUS "
        "asset's availability answer"
    )


def test_excerpt_read_does_not_serve_the_previous_assets_row_after_a_path_swap_at_a_constant_version(
        tmp_path):
    """The exact hazard `get_excerpt_enveloped_async`'s own docstring names:
    `_browse_cached_call`'s generic LRU key has no path component, and
    `sidecar_version` can stay IDENTICAL across a resolved-path swap -- so
    without a path-aware key, swapping the resolved path while the version
    STAYS CONSTANT would keep answering out of the PREVIOUS asset's cache
    entry for the SAME `identification_id` (identical here on purpose: both
    builds use the same `(sys_id, canonical_work_id)` pair, so the swap is
    indistinguishable from the cache's point of view except for the path).
    """
    constant_version = "excerpt-constant-version"
    db_a = _build_excerpt_base_db(tmp_path, "excerpt-swap-a.db", constant_version)
    db_b = _build_excerpt_base_db(tmp_path, "excerpt-swap-b.db", constant_version)
    ident_a = _identification_id_in(db_a)
    ident_b = _identification_id_in(db_b)
    assert ident_a == ident_b, (
        "both builds share (sys_id, canonical_work_id) on purpose -- see docstring"
    )
    _add_excerpt_table(db_a, ident_a, row={"frag_span": "SPAN FROM ASSET A"})
    _add_excerpt_table(db_b, ident_b, row={"frag_span": "SPAN FROM ASSET B"})

    state = {"path": db_a}
    service = DiscoveryService(
        path_provider=lambda: state["path"],
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: constant_version,
    )

    async def _run():
        env_a = await service.get_excerpt_enveloped_async(ident_a)
        assert env_a["items"][0]["frag_span"] == "SPAN FROM ASSET A", env_a

        state["path"] = db_b
        env_b = await service.get_excerpt_enveloped_async(ident_a)
        assert env_b["items"][0]["frag_span"] == "SPAN FROM ASSET B", (
            "a path swap at a CONSTANT version must not serve asset A's "
            "cached excerpt row for asset B's request"
        )

    asyncio.run(_run())


def test_excerpt_lru_serves_the_second_call(tmp_path):
    db_path = _build_excerpt_base_db(tmp_path, "excerpt-lru.db", _EXCERPT_VERSION)
    ident_id = _identification_id_in(db_path)
    _add_excerpt_table(db_path, ident_id)
    service = _service_for(db_path, _EXCERPT_VERSION)
    calls = {"n": 0}
    real_fn = service.get_excerpt_enveloped

    def _counting(identification_id):
        calls["n"] += 1
        return real_fn(identification_id)

    service.get_excerpt_enveloped = _counting

    async def _run():
        e1 = await service.get_excerpt_enveloped_async(ident_id)
        e2 = await service.get_excerpt_enveloped_async(ident_id)
        assert calls["n"] == 1, "the second identical call must be served from the LRU cache"
        assert e1 == e2

    asyncio.run(_run())


def test_surface_safe_excerpt_is_an_allowlist_not_a_denylist():
    fake_row = {field: f"value-{field}" for field in SURFACE_EXCERPT_FIELDS}
    # A field the future bake might grow, and the forbidden badge itself --
    # neither may survive projection.
    fake_row["a_future_bake_field"] = "should not reach a surface"
    fake_row["review_overlay"] = "Expert-reviewed"

    projected = surface_safe_excerpt(fake_row)

    assert "a_future_bake_field" not in projected
    assert "review_overlay" not in projected
    assert set(projected) == set(SURFACE_EXCERPT_FIELDS)
