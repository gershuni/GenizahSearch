"""Tests for the gen-2 routing ingest (Codex blocker 2 / gate 10).

The claim under test is that the emitted routing REPRODUCES gen-2's router rather
than re-deriving it. So the central test flips a route label in the input and
requires the output to follow -- if it does not, "we ingested the router" is
unproven, and the handoff's measured quality does not transfer.

Every guard is mutation-checked in the same spirit as the other v3 suites, after
this session's vacuous-test lesson.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from v3_routing_ingest import (  # noqa: E402
    SURFACE_PARALLEL,
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


def test_the_two_shipped_surfaces_are_told_apart(tmp_path):
    """`same_work` ships; `parallel` does NOT, and carries its own reason.

    CORRECTED 2026-08-07: this test previously asserted `parallel` -> `shipped`,
    which Codex round 2 identified as a semantic corruption (see
    `test_the_parallel_surface_never_ships_as_a_witness` for the mechanism and
    the demonstration). The router still calls both surfaces "shipped" on its own
    side -- what changed is the builder-side routing they map onto.
    """
    db = tmp_path / "e.db"
    _make_evidence(
        db,
        [(P1, "c_w1", "same_work", 0.9, 1), (P2, "c_w2", "parallel", 0.30, 1)],
        [("cl1", P1, "M:w1", "c_w1"), ("cl2", P2, "M:w2", "c_w2")],
    )
    router = load_router(str(db))
    assert resolve_routing(P1, "M:w1", router)[:2] == ("shipped", None)
    assert resolve_routing(P2, "M:w2", router)[:2] == ("review_only", "gen2_parallel_surface")


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


def test_every_mapped_reason_survives_a_REAL_insert_into_the_real_schema(tmp_path):
    """Codex round 2's most damaging find, and the one no in-memory test could reach.

    The first version of this mapping invented `gen2_parallel_surface` and
    `gen2_router_not_shipped` without adding them to `ROUTING_REASONS` or to
    `discovery_evidence`'s `routing_reason` CHECK constraint. Every test above
    asserted on emitted TUPLES, which never touch a database -- so a mapping
    guaranteed to die at INSERT passed the whole suite.

    This drives the ACTUAL DDL and the ACTUAL insert statement. Removing either
    reason from the CHECK constraint turns it red.
    """
    import sqlite3 as sq

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import build_discovery_sidecar as bds
    import discovery_ids as dids

    for _status, reason in SURFACE_TO_ROUTING.values():
        if reason is None:
            continue
        assert reason in dids.ROUTING_REASONS, (
            f"{reason!r} is mapped by the router ingest but is not in the frozen "
            f"ROUTING_REASONS vocabulary"
        )

    conn = sq.connect(":memory:")
    try:
        bds.create_schema(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, source_corpus) "
            "VALUES ('w000001','c1','T','ja')"
        )
        for n, (status, reason) in enumerate(SURFACE_TO_ROUTING.values()):
            claim_id = f"cl{n:04d}"
            # A DISTINCT page per reason: `discovery_claim` carries a UNIQUE
            # (page_id, work_id) constraint. Reusing one page raised
            # IntegrityError on the second row -- another constraint no
            # tuple-level test could have surfaced.
            page_id = f"99000000000000{n:04d}_IE1_P1_FL1"
            cur.execute(
                "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
                "display_evidence_id, source_corpus, sidecar_version) "
                "VALUES (?,?,?,?,?,?,?)",
                (page_id, "w000001", claim_id, "direct_witness", f"ev{n:04d}", "ja", "x"),
            )
            # The real insert, with the real column list. A reason outside the
            # CHECK constraint raises sqlite3.IntegrityError here.
            cur.execute(
                "INSERT INTO discovery_evidence (evidence_id, claim_id, evidence_kind, "
                "evidence_source, confidence_band, adjudication_status, audit_status, "
                "routing_status, routing_reason, is_new, a_page_id, sys_id, "
                "span_start, span_end) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (f"ev{n:04d}", claim_id, "witness", "track1_direct", "tier_a",
                 "unreviewed", "n/a", status, reason or "none", 0, page_id,
                 "990000000000000001", 0, 40),
            )
        conn.commit()
    finally:
        conn.close()


def test_the_parallel_surface_never_ships_as_a_witness(tmp_path):
    """Codex round 2: a shipped `parallel` row is a semantic corruption.

    `assemble_claims_and_evidence` derives `claim_type` from witness span
    DOMINANCE, and the panel renders its relation chip from `claim_type`, not
    from `routing_reason`. So a shipped quotation holding the page's largest span
    resolves to `direct_witness` and enters the main pool as same-work evidence.

    This asserts BOTH halves: the mapping demotes it, AND the built claim would
    have mislabelled it had the mapping not. The second half is what makes the
    first half matter -- without it, `review_only` looks like an arbitrary
    preference rather than the fix to a real corruption.
    """
    status, reason = SURFACE_TO_ROUTING[SURFACE_PARALLEL]
    assert status == "review_only", (
        "a `parallel` (quotation) row is mapped to a SHIPPED witness -- it will "
        "render as direct_witness via span dominance"
    )
    assert reason == "gen2_parallel_surface", "the quotation origin is not recorded"

    # The corruption this prevents, demonstrated rather than asserted: build a
    # page whose ONLY witness is the quotation, and confirm the claim_type the
    # builder would have given it.
    rdb = tmp_path / "r.db"
    _tiny_research_db(rdb, [(P1, "990000000000000001", "M:w1", 40, "[[0,40,0.2]]")])
    router_db = tmp_path / "route.db"
    _make_evidence(router_db, [(P1, "c_w1", "parallel", 0.30, 1)],
                   [("cl1", P1, "M:w1", "c_w1")])
    _, built = _build(rdb, _works(), load_router(str(router_db)))
    claim_types = {row[3] for row in built["claim_rows"]}
    assert claim_types == {"direct_witness"}, (
        f"expected the witness dominance rule to label this quotation "
        f"direct_witness (that is the corruption), got {claim_types}"
    )
    # ...and it is kept out of every shipped-gated read anyway.
    assert [r[_ROUTING_STATUS_IDX] for r in built["evidence_rows"]] == ["review_only"]
    assert [r[_ROUTING_REASON_IDX] for r in built["evidence_rows"]] == ["gen2_parallel_surface"]


def test_a_duplicate_router_key_halts_even_when_the_surfaces_agree(tmp_path):
    """Round 2: agreeing duplicates inflate `counts` while replacing the entry,
    so the parity report is at neither the router's grain nor the emitted one."""
    db = tmp_path / "e.db"
    _make_evidence(
        db,
        [(P1, "c_w1", "same_work", 0.9, 1), (P1, "c_w1", "same_work", 0.9, 1)],
        [("cl1", P1, "M:w1", "c_w1")],
    )
    with pytest.raises(RoutingIngestError, match="more than one row"):
        load_router(str(db))


def test_a_row_whose_shipped_flag_contradicts_its_surface_halts(tmp_path):
    """Round 2: `load_router` read `shipped` and threw it away."""
    db = tmp_path / "e.db"
    _make_evidence(db, [(P1, "c_w1", "same_work", 0.9, 0)],   # same_work but shipped=0
                   [("cl1", P1, "M:w1", "c_w1")])
    with pytest.raises(RoutingIngestError, match="disagrees with itself"):
        load_router(str(db))


def test_the_router_runs_before_d17_not_after(tmp_path):
    """Codex round 2: the plan said the v2 order was "no longer inherited" and
    must be "re-derived", which is not an order. This pins the one that ships.

    The mechanism, made observable: `apply_d17_demotion` arbitrates among the
    currently-SHIPPED witnesses on a page, earliest-first. Two works co-claim one
    page here, and the router DEMOTES the earlier one. If the router ran after
    D-17, D-17 would see both, demote the later work as "chronologically later
    than" a competitor that does not ship at all, and stamp it
    `later_shared_text` -- a reason naming a cause that never existed.

    With the router first, the later work is the only shipped witness on the
    page, so it has nothing to lose to and keeps its own reason.
    """
    rdb = tmp_path / "r.db"
    # Both works claim P1 with overlapping, long spans (over D17_MIN_ML=200), so
    # D-17 would consider them competitors.
    conn = sqlite3.connect(str(rdb))
    conn.execute(
        "CREATE TABLE track1_matches (page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, "
        "genre TEXT, author TEXT, title TEXT, matched_letters INT, best_density REAL, "
        "n_spans INT, spans_json TEXT, shadowed_by TEXT)"
    )
    conn.executemany(
        "INSERT INTO track1_matches VALUES (?,?,?,'JA','G','A','T',?,0.2,1,?,NULL)",
        [(P1, "990000000000000001", "M:early", 500, "[[0,500,0.2]]"),
         (P1, "990000000000000001", "M:late", 400, "[[10,410,0.2]]")],
    )
    conn.execute(
        "CREATE TABLE pages (page_id TEXT PRIMARY KEY, sys_id TEXT, buckets TEXT, "
        "n_chars INT, text TEXT, provenance TEXT, fgp_id INT, fgp_score REAL, htr_n_chars INT)"
    )
    conn.execute("INSERT INTO pages VALUES (?,?,'b',600,?,'htr',NULL,NULL,600)",
                 (P1, "990000000000000001", "א" * 600))
    conn.commit()
    conn.close()

    # The router ships ONLY the later work; the earlier one it declines.
    router_db = tmp_path / "route.db"
    _make_evidence(
        router_db,
        [(P1, "c_early", "not_shipped", 0.10, 0), (P1, "c_late", "same_work", 0.70, 1)],
        [("cl1", P1, "M:early", "c_early"), ("cl2", P1, "M:late", "c_late")],
    )

    works = [
        {"raw_work_id": "M:early", "work_id": "w000001", "source_corpus": "ja",
         "neutral_title": "E", "author": None, "genre": None, "cat": "JA"},
        {"raw_work_id": "M:late", "work_id": "w000002", "source_corpus": "ja",
         "neutral_title": "L", "author": None, "genre": None, "cat": "JA"},
    ]

    sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
    import build_discovery_sidecar as bds

    conn = bds._connect_research_ro(str(rdb))
    try:
        result = bds.build_claims_and_evidence(
            conn=conn, works=works, page_index=bds.PageTextIndex(conn),
            gen2_router=load_router(str(router_db)),
            # D-17 active: the earlier work resolves 500 years before the later.
            cross_corpus_map=None,
            year_by_canonical={"w000001": 500, "w000002": 1000},
        )
    finally:
        conn.close()

    by_work = {}
    for row in result["evidence_rows"]:
        # evidence_rows carry a_page_id/sys_id but not work_id; recover it via the
        # claim row the evidence points at.
        by_work[row[0]] = (row[_ROUTING_STATUS_IDX], row[_ROUTING_REASON_IDX])
    claim_work = {c[2]: c[1] for c in result["claim_rows"]}
    status_by_work = {}
    for row in result["evidence_rows"]:
        status_by_work[claim_work[row[1]]] = (row[_ROUTING_STATUS_IDX], row[_ROUTING_REASON_IDX])

    assert status_by_work["w000001"] == ("review_only", "gen2_router_not_shipped"), (
        f"the router's decline did not survive D-17: {status_by_work['w000001']}"
    )
    # THE assertion: the later work must NOT have been demoted against a
    # competitor the router already removed.
    assert status_by_work["w000002"][0] == "shipped", (
        f"the later work was demoted although it is the only shipped witness on "
        f"the page -- D-17 arbitrated against a phantom competitor, which means "
        f"the router did not run first: {status_by_work['w000002']}"
    )
    assert status_by_work["w000002"][1] != "later_shared_text"


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


# ---------------------------------------------------------------------------
# Codex ROUND 3 BLOCKER: `finalize_build` had NO router input at all. It never
# imported this module and passed `apply_lever1=run_d17`, so the real build ran
# the legacy 0.45 cliff precisely when D-17 ran -- the entire ingest (mapping,
# parity gate, order justification, 17 tests) affected no artifact.
#
# The third instance of "correct function nobody calls" in this work. Every test
# above this line drives `build_claims_and_evidence`, which is a HELPER; these
# drive `finalize_build`, which is what the bake actually runs, and read the
# BUILT SQLite file.
# ---------------------------------------------------------------------------

def _v3_finalize_fixture(tmp_path):
    """Two works co-claiming one page at coverage 0.75 -- ABOVE the legacy 0.45
    cliff, so Lever-1 ships both and the router's decision is the only thing that
    can demote one. That makes the two routings distinguishable in the output."""
    import csv as _csv

    import discovery_ids as dids

    tmp_path = Path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)   # callers pass a SUBdirectory
    research_db = tmp_path / "research.db"
    conn = sqlite3.connect(str(research_db))
    conn.executescript(
        """
        CREATE TABLE track1_matches (
          page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, genre TEXT, author TEXT,
          title TEXT, matched_letters INT, best_density REAL, n_spans INT,
          spans_json TEXT, shadowed_by TEXT, ref_spans_json TEXT
        );
        CREATE TABLE pages (
          page_id TEXT PRIMARY KEY, n_chars INTEGER, text TEXT, provenance TEXT
        );
        """
    )
    conn.executemany(
        "INSERT INTO track1_matches VALUES (?,?,?,?,NULL,NULL,NULL,?,?,1,?,NULL,?)",
        [
            ("pg1", "s1", "raw:w1", "Sefaria", 300, 0.9, "[[0, 300, 0.9]]",
             '[{"p0":0,"p1":300,"rg0":10,"rg1":310}]'),
            ("pg1", "s1", "raw:w2", "Sefaria", 300, 0.9, "[[0, 300, 0.9]]",
             '[{"p0":0,"p1":300,"rg0":50,"rg1":350}]'),
        ],
    )
    conn.execute(
        "INSERT INTO pages (page_id, provenance, text) VALUES ('pg1','htr',?)",
        (chr(0x05D0) * 400,),          # coverage = 300/400 = 0.75
    )
    conn.commit()
    conn.close()

    crosswalk = tmp_path / "crosswalk.json"
    crosswalk.write_text(json.dumps({"raw:w1": "w000001", "raw:w2": "w000002"}),
                         encoding="utf-8")

    import build_discovery_sidecar as bds

    approved = tmp_path / "approved.csv"
    with open(approved, "w", encoding="utf-8-sig", newline="") as fh:
        writer = _csv.DictWriter(fh, fieldnames=bds.APPROVED_HEADER)
        writer.writeheader()
        for wid, title in (("w000001", "Synthetic One"), ("w000002", "Synthetic Two")):
            row = {h: "" for h in bds.APPROVED_HEADER}
            row["work_id"] = wid
            row["owner_verdict"] = "approve"
            row["candidate_title"] = title
            row["source_label"] = dids.SOURCE_CORPUS_SEFARIA
            writer.writerow(row)

    return {"research_db": research_db, "crosswalk": crosswalk, "approved": approved,
            "out": tmp_path / "out" / "discovery-v3.db"}


def _finalize(fx, tmp_path, **kw):
    import build_discovery_sidecar as bds

    tmp_path = Path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)
    dates = tmp_path / "comp.json"
    # The FLAT pre-normalized form (raw id -> integer CE year), which is what
    # `parse_composition_dates` accepts alongside the designator form.
    dates.write_text(json.dumps({"raw:w1": 900, "raw:w2": 1400}), encoding="utf-8")
    return bds.finalize_build(
        source_db_path=str(fx["research_db"]),
        from_approved_path=str(fx["approved"]),
        crosswalk_path=str(fx["crosswalk"]),
        out_db_path=str(fx["out"]),
        composition_dates_path=str(dates),
        masking_patterns=["TOTALLY-UNMATCHED-MARKER-XYZ-123"],
        **kw,
    )


def _routing_in_built_asset(db_path):
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT c.work_id, e.routing_status, e.routing_reason "
            "FROM discovery_evidence e JOIN discovery_claim c ON c.claim_id = e.claim_id "
            "WHERE e.evidence_source = 'track1_direct'"
        ).fetchall()
        meta = dict(conn.execute("SELECT key, value FROM meta").fetchall())
    finally:
        conn.close()
    return {w: (s, r) for w, s, r in rows}, meta


def test_finalize_build_REFUSES_to_default_to_the_legacy_cliff(tmp_path):
    """THE round-3 fix. Silently defaulting is what the real build used to do.

    Measured consequence of that default: 30,899 of 160,095 `same_work` rows
    (19.3%) demoted, one-way -- so the asset would not contain the population the
    grading measured, while every router test still passed.
    """
    import build_discovery_sidecar as bds

    fx = _v3_finalize_fixture(tmp_path)
    with pytest.raises(bds.RoutingConflictError, match="coverage routing is unspecified"):
        _finalize(fx, tmp_path)


def test_the_router_reaches_the_BUILT_ASSET_and_the_legacy_cliff_does_not(tmp_path):
    """Build twice through `finalize_build` and read the emitted SQLite.

    Every earlier router test drove the helper. This drives the real entrypoint
    and asserts on rows read back out of the built file -- the only evidence that
    distinguishes "the ingest works" from "the ingest runs".
    """
    router_db = tmp_path / "route.db"
    _make_evidence(
        router_db,
        # w1 ships; w2 the router DECLINES -- at coverage 0.75 the legacy cliff
        # would ship both, so this difference can only come from the router.
        [("pg1", "c_w1", "same_work", 0.75, 1), ("pg1", "c_w2", "not_shipped", 0.75, 0)],
        [("cl1", "pg1", "raw:w1", "c_w1"), ("cl2", "pg1", "raw:w2", "c_w2")],
    )

    fx_router = _v3_finalize_fixture(tmp_path / "a")
    _finalize(fx_router, tmp_path / "a", gen2_router_evidence_db=str(router_db))
    routed, meta_routed = _routing_in_built_asset(fx_router["out"])

    fx_legacy = _v3_finalize_fixture(tmp_path / "b")
    _finalize(fx_legacy, tmp_path / "b", allow_lever1_coverage=True)
    legacy, meta_legacy = _routing_in_built_asset(fx_legacy["out"])

    assert routed["w000002"] == ("review_only", "gen2_router_not_shipped"), (
        f"the router's decline did not reach the built asset: {routed}"
    )
    assert routed["w000001"][0] == "shipped", routed
    # The control: under the legacy cliff w2 is NOT demoted for coverage, so the
    # difference above is attributable to the router and nothing else.
    assert legacy["w000002"][1] != "gen2_router_not_shipped", (
        f"the legacy path produced a router reason -- the two are not distinct: {legacy}"
    )
    # And the asset says which routing produced it.
    assert meta_routed["coverage_routing"] == "gen2_router", meta_routed.get("coverage_routing")
    assert meta_legacy["coverage_routing"] == "lever1_cliff", meta_legacy.get("coverage_routing")


def test_the_cli_offers_and_threads_the_router(tmp_path):
    """A CLI build must face the same forced choice; a declared-but-unthreaded
    flag is the same bypass in a new place."""
    import build_discovery_sidecar as bds

    src = Path(bds.__file__).read_text(encoding="utf-8")
    assert "--gen2-router-evidence-db" in src
    assert "--allow-lever1-coverage" in src
    assert "gen2_router_evidence_db=args.gen2_router_evidence_db" in src
    assert "allow_lever1_coverage=args.allow_lever1_coverage" in src
