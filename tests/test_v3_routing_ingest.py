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

# Column positions in the emitted `evidence_rows` tuples.
_ROUTING_STATUS_IDX = 7
_ROUTING_REASON_IDX = 8


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


# ---------------------------------------------------------------------------
# The wiring tests. Codex round 2's central finding was that the module above
# was never CALLED by the builder, so all of the tests before this line could
# pass while the router had no effect whatsoever. These drive
# `build_claims_and_evidence` itself.
# ---------------------------------------------------------------------------

def _tiny_research_db(path: Path, rows):
    """rows: [(page_id, sys_id, work_id, matched_letters, spans_json)]"""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE track1_matches (page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, "
        "genre TEXT, author TEXT, title TEXT, matched_letters INT, best_density REAL, "
        "n_spans INT, spans_json TEXT, shadowed_by TEXT)"
    )
    conn.executemany(
        "INSERT INTO track1_matches VALUES (?,?,?,'JA','G','A','T',?,0.2,1,?,NULL)",
        rows,
    )
    conn.execute(
        "CREATE TABLE pages (page_id TEXT PRIMARY KEY, sys_id TEXT, buckets TEXT, "
        "n_chars INT, text TEXT, provenance TEXT, fgp_id INT, fgp_score REAL, htr_n_chars INT)"
    )
    conn.executemany(
        "INSERT INTO pages VALUES (?,?,'b',100,?,'htr',NULL,NULL,100)",
        [(r[0], r[1], "\u05d0" * 100) for r in rows],
    )
    conn.commit()
    conn.close()


def _build(research_db, works, router=None, *, apply_lever1=False):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import build_discovery_sidecar as bds

    conn = bds._connect_research_ro(str(research_db))
    try:
        page_index = bds.PageTextIndex(conn)
        return bds, bds.build_claims_and_evidence(
            conn=conn, works=works, page_index=page_index,
            apply_lever1=apply_lever1, gen2_router=router,
        )
    finally:
        conn.close()


def _works():
    return [{"raw_work_id": "M:w1", "work_id": "w000001", "source_corpus": "ja",
             "neutral_title": "T", "author": None, "genre": None, "cat": "JA"}]


def test_the_router_actually_changes_the_built_result(tmp_path):
    """THE test round 2's finding demands: the router must reach the OUTPUT.

    Every earlier test in this file exercised the reader in isolation, which is
    why an unwired module passed all of them. This one builds twice over the same
    research DB -- once with the router demoting the row, once shipping it -- and
    requires the built evidence to differ.
    """
    rdb = tmp_path / "r.db"
    _tiny_research_db(rdb, [(P1, "990000000000000001", "M:w1", 40, "[[0,40,0.2]]")])

    ship = tmp_path / "ship.db"
    _make_evidence(ship, [(P1, "c_w1", "same_work", 0.40, 1)],
                   [("cl1", P1, "M:w1", "c_w1")])
    demote = tmp_path / "demote.db"
    _make_evidence(demote, [(P1, "c_w1", "not_shipped", 0.40, 0)],
                   [("cl1", P1, "M:w1", "c_w1")])

    _, shipped = _build(rdb, _works(), load_router(str(ship)))
    _, demoted = _build(rdb, _works(), load_router(str(demote)))

    # `evidence_rows` are TUPLES, not dicts: routing_status is index 7 and
    # routing_reason index 8 (see `assemble_claims_and_evidence`). Asserting on
    # the emitted tuple rather than on an intermediate dict is deliberate --
    # this is the built OUTPUT, which is what round 2 said was never checked.
    def statuses(result):
        return [row[_ROUTING_STATUS_IDX] for row in result["evidence_rows"]]

    assert statuses(shipped) == ["shipped"], statuses(shipped)
    assert statuses(demoted) == ["review_only"], (
        "the router's not_shipped decision did not reach the built evidence -- "
        "the ingest is not wired"
    )


def test_the_router_beats_the_lever1_cliff_on_a_row_between_the_thresholds(tmp_path):
    """The 19.3% case, end to end.

    coverage 0.40 is BELOW the builder's 0.45 cliff and ABOVE the router's
    threshold. With the router, the row must ship; with Lever-1, it must be
    demoted. That difference is the whole reason for this work.
    """
    rdb = tmp_path / "r.db"
    _tiny_research_db(rdb, [(P1, "990000000000000001", "M:w1", 40, "[[0,40,0.2]]")])
    router_db = tmp_path / "route.db"
    _make_evidence(router_db, [(P1, "c_w1", "same_work", 0.40, 1)],
                   [("cl1", P1, "M:w1", "c_w1")])

    _, with_router = _build(rdb, _works(), load_router(str(router_db)))
    _, with_lever1 = _build(rdb, _works(), None, apply_lever1=True)

    assert [r[_ROUTING_STATUS_IDX] for r in with_router["evidence_rows"]] == ["shipped"]
    assert [r[_ROUTING_STATUS_IDX] for r in with_lever1["evidence_rows"]] == ["review_only"], (
        "Lever-1 no longer demotes a sub-cliff row -- the control for this test is gone"
    )


def test_requesting_both_the_router_and_lever1_is_refused(tmp_path):
    """A caller that asks for both has not decided; refuse rather than pick."""
    rdb = tmp_path / "r.db"
    _tiny_research_db(rdb, [(P1, "990000000000000001", "M:w1", 40, "[[0,40,0.2]]")])
    router_db = tmp_path / "route.db"
    _make_evidence(router_db, [(P1, "c_w1", "same_work", 0.40, 1)],
                   [("cl1", P1, "M:w1", "c_w1")])
    import build_discovery_sidecar as bds
    with pytest.raises(bds.RoutingConflictError):
        _build(rdb, _works(), load_router(str(router_db)), apply_lever1=True)


def test_a_row_the_router_never_decided_aborts_the_build(tmp_path):
    """Gate 10 part 1: an undecided tier-A row must not keep the ingest default.

    Silently shipping it is exactly the bypass this replaces, so the build must
    fail rather than emit a row the router never approved.
    """
    rdb = tmp_path / "r.db"
    _tiny_research_db(rdb, [(P1, "990000000000000001", "M:w1", 40, "[[0,40,0.2]]")])
    empty = tmp_path / "empty.db"
    _make_evidence(empty, [(P2, "c_other", "same_work", 0.9, 1)],
                   [("cl1", P2, "M:other", "c_other")])
    with pytest.raises(RoutingIngestError, match="no router decision"):
        _build(rdb, _works(), load_router(str(empty)))
