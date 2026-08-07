"""Tests for the gen-2 routing ingest (Codex blocker 2 / gate 10).

The claim under test is that the emitted routing REPRODUCES gen-2's router rather
than re-deriving it. So the central test flips a route label in the input and
requires the output to follow -- if it does not, "we ingested the router" is
unproven, and the handoff's measured quality does not transfer.

Every guard is mutation-checked in the same spirit as the other v3 suites, after
this session's vacuous-test lesson.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from v3_routing_ingest import (  # noqa: E402
    SURFACE_TO_ROUTING,
    RoutingIngestError,
    load_router,
    parity_report,
    resolve_routing,
)


def _make_evidence(path: Path, routes, claims, *, meta=("g", 0.2984)):
    """routes: [(page_id, canonical_work_id, surface, page_coverage, shipped)]
    claims:  [(claim_id, page_id, work_id, canonical_work_id)]"""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE coverage_route (page_id TEXT, canonical_work_id TEXT, run_id TEXT, "
        "page_coverage REAL, matched_letters INT, page_chars INT, shipped INT, surface TEXT)"
    )
    conn.executemany(
        "INSERT INTO coverage_route (page_id, canonical_work_id, surface, page_coverage, "
        "shipped, run_id, matched_letters, page_chars) VALUES (?,?,?,?,?,'g',10,100)",
        routes,
    )
    conn.execute("CREATE TABLE coverage_route_meta (run_id TEXT, t REAL)")
    if meta is not None:
        conn.execute("INSERT INTO coverage_route_meta VALUES (?,?)", meta)
    conn.execute(
        "CREATE TABLE discovery_claim (claim_id TEXT, page_id TEXT, work_id TEXT, "
        "canonical_work_id TEXT)"
    )
    conn.executemany("INSERT INTO discovery_claim VALUES (?,?,?,?)", claims)
    conn.commit()
    conn.close()


P1, P2 = "990000000000000001_IE1_P1_FL1", "990000000000000002_IE1_P1_FL1"


def test_the_ingested_routing_follows_the_router_and_the_gate_can_fail(tmp_path):
    """Gate 10: flip a route label in the input; the output MUST follow.

    This is the whole point. If a demoted route still resolved to `shipped`, the
    builder would be applying its own rule while claiming to carry gen-2's.
    """
    db = tmp_path / "e.db"
    _make_evidence(
        db,
        [(P1, "c_w1", "same_work", 0.31, 1)],
        [("cl1", P1, "M:w1", "c_w1")],
    )
    router = load_router(str(db))
    assert resolve_routing(P1, "M:w1", router)[0] == "shipped"

    # Flip it in the SOURCE and reload -- the mutation is of the input, not the code.
    flipped = tmp_path / "e2.db"
    _make_evidence(
        flipped,
        [(P1, "c_w1", "not_shipped", 0.31, 0)],
        [("cl1", P1, "M:w1", "c_w1")],
    )
    router2 = load_router(str(flipped))
    status, reason, _ = resolve_routing(P1, "M:w1", router2)
    assert status == "review_only", "the ingest did not follow the router's decision"
    assert reason == "gen2_router_not_shipped"


def test_the_parallel_surface_is_shipped_but_distinguishable(tmp_path):
    """The second surface must ship AND be tellable apart without re-deriving."""
    db = tmp_path / "e.db"
    _make_evidence(
        db,
        [(P1, "c_w1", "same_work", 0.9, 1), (P2, "c_w2", "parallel", 0.30, 1)],
        [("cl1", P1, "M:w1", "c_w1"), ("cl2", P2, "M:w2", "c_w2")],
    )
    router = load_router(str(db))
    assert resolve_routing(P1, "M:w1", router)[:2] == ("shipped", None)
    assert resolve_routing(P2, "M:w2", router)[:2] == ("shipped", "gen2_parallel_surface")


def test_a_pair_the_router_never_decided_returns_no_default(tmp_path):
    """No silent default: an undecided pair must be reported as undecided.

    BOTH miss paths are exercised deliberately. A first version tested only an
    unknown work id, which returns early on the raw->canonical lookup and never
    reaches the route lookup -- so a mutation that defaulted the ROUTE miss to
    `shipped` passed. Caught by mutation testing, 2026-08-07.
    """
    db = tmp_path / "e.db"
    _make_evidence(db, [(P1, "c_w1", "same_work", 0.9, 1)],
                   [("cl1", P1, "M:w1", "c_w1"),
                    # A work the router KNOWS, on a page it never routed.
                    ("cl2", P2, "M:w2", "c_w2")])
    router = load_router(str(db))

    # (a) unknown work id -> no canonical mapping at all
    assert resolve_routing(P2, "M:unknown", router) == (None, None, None)
    # (b) KNOWN work id, but the router made no decision for this pair. This is
    #     the path a silent default would corrupt.
    assert resolve_routing(P2, "M:w2", router) == (None, None, None), \
        "an undecided (page, work) pair was given a default routing"


def test_an_unknown_surface_value_halts(tmp_path):
    db = tmp_path / "e.db"
    _make_evidence(db, [(P1, "c_w1", "brand_new_surface", 0.5, 1)],
                   [("cl1", P1, "M:w1", "c_w1")])
    with pytest.raises(RoutingIngestError, match="unknown surface"):
        load_router(str(db))


def test_an_ambiguous_raw_to_canonical_map_halts(tmp_path):
    """A raw work id resolving to two canonical ids has no defined route."""
    db = tmp_path / "e.db"
    _make_evidence(
        db,
        [(P1, "c_a", "same_work", 0.9, 1), (P1, "c_b", "parallel", 0.3, 1)],
        [("cl1", P1, "M:w1", "c_a"), ("cl2", P1, "M:w1", "c_b")],   # same raw id!
    )
    with pytest.raises(RoutingIngestError, match="more than one canonical"):
        load_router(str(db))


def test_a_missing_router_meta_halts(tmp_path):
    """Refuse a router decision whose threshold/provenance is unrecorded."""
    db = tmp_path / "e.db"
    _make_evidence(db, [(P1, "c_w1", "same_work", 0.9, 1)],
                   [("cl1", P1, "M:w1", "c_w1")], meta=None)
    with pytest.raises(RoutingIngestError, match="unrecorded"):
        load_router(str(db))


def test_the_parity_report_quantifies_what_recomputing_would_lose(tmp_path):
    """The blocker-2 evidence: a number, not an assertion."""
    db = tmp_path / "e.db"
    _make_evidence(
        db,
        [
            (P1, "c_w1", "same_work", 0.31, 1),   # between the thresholds
            (P2, "c_w2", "same_work", 0.90, 1),   # above both
            ("p3", "c_w3", "parallel", 0.20, 1),  # below both
        ],
        [("cl1", P1, "M:w1", "c_w1"), ("cl2", P2, "M:w2", "c_w2"),
         ("cl3", "p3", "M:w3", "c_w3")],
    )
    report = parity_report(load_router(str(db)))
    assert report["same_work_total"] == 2
    assert report["same_work_would_be_demoted"] == 1, \
        "the report failed to notice a row the builder cliff would demote"
    assert report["parallel_would_be_promoted"] == 0
    assert report["one_way"] is True


def test_every_router_surface_has_an_explicit_mapping():
    """A new surface must not be silently absorbed by a fallback."""
    from v3_routing_ingest import ROUTER_SURFACES
    assert set(ROUTER_SURFACES) == set(SURFACE_TO_ROUTING), \
        "a router surface has no declared routing mapping"
    for status, _reason in SURFACE_TO_ROUTING.values():
        assert status in ("shipped", "review_only")
