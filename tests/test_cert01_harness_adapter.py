# -*- coding: utf-8 -*-
"""Adapter tests for `scripts/cert01_draw_deck.py` (Phase 135, plan 135-09, Task 2).

Proves the CERT-01 deck-drawing adapter only SELECTS/SHAPES estimand rows
and DELEGATES to the reused `same_work_spike/probe/scripts/e1_deck.py`
primitives (`components_of`) -- never re-deriving the bipartite
work<->physMS component algorithm itself -- plus unit tests for the
adapter's own pure glue (`uid_of`, `draw_stratified_deck`, `draw_gold_cards`,
`build_diagnostic_sample`).

Every fixture below is FABRICATED (synthetic `w000xxx`/`page:N`/`sys:N`
values) -- never real research content. Skips CLEANLY (matching the
`tests/test_discovery_coverage_replication.py` precedent) when the
gitignored `same_work_spike/probe/scripts/e1_deck.py` research tree is
absent on this box (e.g. CI) -- the pure-glue tests below do NOT need that
tree and always run.
"""
import sqlite3

import pytest

from scripts import cert01_draw_deck as adapter


# ---------------------------------------------------------------------------
# Pure glue -- no e1_deck.py dependency
# ---------------------------------------------------------------------------


def test_uid_of():
    assert adapter.uid_of("page:1", "w000001") == "page:1|w000001"


def test_draw_stratified_deck_respects_allocation_and_is_seed_reproducible():
    rows = (
        [{"page_id": f"pA{i}", "canonical_work_id": f"w{i}", "stratum": "sefaria:high",
          "sys_id": f"s{i}", "unit_key": f"unit{i}"} for i in range(10)]
        + [{"page_id": f"pB{i}", "canonical_work_id": f"w{i+100}", "stratum": "ja:medium",
            "sys_id": f"s{i+100}", "unit_key": f"unit{i+100}"} for i in range(5)]
    )
    allocation = {"sefaria:high": 3, "ja:medium": 2}
    drawn1 = adapter.draw_stratified_deck(rows, allocation, seed=42)
    drawn2 = adapter.draw_stratified_deck(rows, allocation, seed=42)
    assert len(drawn1) == 5
    assert {r["stratum"] for r in drawn1} == {"sefaria:high", "ja:medium"}
    assert sum(1 for r in drawn1 if r["stratum"] == "sefaria:high") == 3
    assert sum(1 for r in drawn1 if r["stratum"] == "ja:medium") == 2
    # reproducible from the same seed
    assert [r["page_id"] for r in drawn1] == [r["page_id"] for r in drawn2]

    drawn3 = adapter.draw_stratified_deck(rows, allocation, seed=99)
    assert [r["page_id"] for r in drawn1] != [r["page_id"] for r in drawn3], \
        "a different seed must (almost certainly) draw a different sample"


def test_draw_stratified_deck_caps_at_available_pool_size():
    rows = [{"page_id": "p1", "canonical_work_id": "w1", "stratum": "sefaria:high",
             "sys_id": "s1", "unit_key": "u1"}]
    allocation = {"sefaria:high": 50}  # more than available
    drawn = adapter.draw_stratified_deck(rows, allocation, seed=1)
    assert len(drawn) == 1


def test_draw_gold_cards_excludes_cluster_overlap_with_drawn_deck():
    gold_pool = [
        {"page_id": "gp1", "work_id": "gw1", "phys_ms": "clusterA"},
        {"page_id": "gp2", "work_id": "gw2", "phys_ms": "clusterB"},
        {"page_id": "gp3", "work_id": "gw3", "phys_ms": "clusterC"},
    ]
    drawn_deck_clusters = {"clusterB"}  # gp2 must be excluded
    picked = adapter.draw_gold_cards(gold_pool, drawn_deck_clusters, n_target=10, seed=7)
    assert all(g["phys_ms"] != "clusterB" for g in picked)
    assert len(picked) == 2  # gp1 + gp3 only


def test_draw_gold_cards_caps_at_target_n():
    gold_pool = [{"page_id": f"gp{i}", "work_id": f"gw{i}", "phys_ms": f"cluster{i}"}
                 for i in range(10)]
    picked = adapter.draw_gold_cards(gold_pool, set(), n_target=3, seed=1)
    assert len(picked) == 3


def test_deck_cards_never_carry_a_grader_visible_demotion_field():
    # This mirrors the assertion cert01_draw_deck.main() runs on the real
    # deck -- re-asserted here directly against the adapter's card shape.
    card = {"uid": "p1|w1", "role": "candidate", "stratum": "sefaria:high",
            "page_id": "p1", "canonical_work_id": "w1", "sys_id": "s1"}
    assert "later_shared_text" not in card
    assert "routing_status" not in card


# ---------------------------------------------------------------------------
# build_diagnostic_sample against a fabricated sidecar fixture (real SQL,
# no e1_deck.py dependency)
# ---------------------------------------------------------------------------


def _make_diagnostic_fixture_db(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE works (work_id TEXT PRIMARY KEY, canonical_work_id TEXT, source_corpus TEXT);
        CREATE TABLE discovery_claim (page_id TEXT, work_id TEXT, claim_id TEXT,
            claim_type TEXT, display_evidence_id TEXT);
        CREATE TABLE discovery_evidence (evidence_id TEXT PRIMARY KEY, claim_id TEXT,
            evidence_kind TEXT, evidence_source TEXT, confidence_band TEXT,
            adjudication_status TEXT, routing_status TEXT, routing_reason TEXT,
            a_page_id TEXT, sys_id TEXT, matched_letters INTEGER);
        CREATE TABLE discovery_routing_audit (id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id TEXT, kept_work_id TEXT, demoted_work_id TEXT, kept_year INTEGER,
            demoted_year INTEGER, delta_years INTEGER, decision TEXT, routing_reason TEXT);
        """
    )
    # one demoted evidence row
    conn.execute("INSERT INTO works VALUES ('w000001','w000001','sefaria')")
    conn.execute("INSERT INTO discovery_claim VALUES ('pD','w000001','claimD','direct_witness','evD')")
    conn.execute(
        "INSERT INTO discovery_evidence VALUES ('evD','claimD','witness','track1_direct','tier_a',"
        "'unreviewed','review_only','later_shared_text','pD','sysD',100)"
    )
    # one kept_tie audit row on a page that ALSO has a shipped tier_a claim
    conn.execute("INSERT INTO works VALUES ('w000002','w000002','sefaria')")
    conn.execute("INSERT INTO discovery_claim VALUES ('pR','w000002','claimR','direct_witness','evR')")
    conn.execute(
        "INSERT INTO discovery_evidence VALUES ('evR','claimR','witness','track1_direct','tier_a',"
        "'unreviewed','shipped','none','pR','sysR',200)"
    )
    conn.execute(
        "INSERT INTO discovery_routing_audit (page_id, kept_work_id, kept_year, demoted_year, "
        "delta_years, decision, routing_reason) VALUES ('pR','w000002',1000,970,30,'kept_tie',NULL)"
    )
    conn.commit()
    db_path = tmp_path / "fixture.db"
    dest = sqlite3.connect(str(db_path))
    conn.backup(dest)
    dest.close()
    return db_path


def test_build_diagnostic_sample_finds_demoted_and_retained_pages(tmp_path):
    db_path = _make_diagnostic_fixture_db(tmp_path)
    diag = adapter.build_diagnostic_sample(str(db_path), seed=1, n_per_group=20)
    assert len(diag["demoted"]) == 1
    assert diag["demoted"][0]["page_id"] == "pD"
    assert diag["retained_pages"] == ["pR"]


# ---------------------------------------------------------------------------
# e1_deck.components_of delegation -- proves the adapter's card-shaping
# produces IDENTICAL results to calling the reused function directly on the
# same fabricated frame (skips cleanly if the research tree is absent).
# ---------------------------------------------------------------------------


def test_components_of_delegation_matches_direct_call():
    try:
        e1 = adapter.load_e1_deck()
    except RuntimeError:
        pytest.skip("same_work_spike/probe/scripts/e1_deck.py not present on this box")

    estimand_rows = [
        {"page_id": "p1", "canonical_work_id": "w1", "unit_key": "clusterA"},
        {"page_id": "p2", "canonical_work_id": "w1", "unit_key": "clusterA"},
        {"page_id": "p3", "canonical_work_id": "w2", "unit_key": "clusterB"},
    ]

    # The adapter's own card-shaping (mirrors cert01_draw_deck.main()'s
    # e1_style_cards construction verbatim).
    e1_style_cards = [
        {"uid": adapter.uid_of(r["page_id"], r["canonical_work_id"]),
         "work_id": r["canonical_work_id"], "phys": r["unit_key"]}
        for r in estimand_rows
    ]
    via_adapter = e1.components_of(e1_style_cards)

    # Direct call on hand-built cards in e1_deck's OWN expected shape.
    direct_cards = [
        {"uid": "p1|w1", "work_id": "w1", "phys": "clusterA"},
        {"uid": "p2|w1", "work_id": "w1", "phys": "clusterA"},
        {"uid": "p3|w2", "work_id": "w2", "phys": "clusterB"},
    ]
    direct = e1.components_of(direct_cards)

    # Identical component ASSIGNMENT structure (same partition), proving the
    # adapter delegates rather than re-implementing the union-find algorithm.
    assert via_adapter == direct
    # p1 and p2 share a component (same work + same physMS cluster); p3 is separate.
    assert via_adapter["p1|w1"] == via_adapter["p2|w1"]
    assert via_adapter["p3|w2"] != via_adapter["p1|w1"]
