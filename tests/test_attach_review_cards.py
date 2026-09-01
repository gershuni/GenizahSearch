# -*- coding: utf-8 -*-
"""Gate tests for scripts/attach_review_cards.py.

The green fixture pins the projection's shape and its honesty rules ('mixed'
where rows disagree, NULL locus where they carry different labels); every gate
is proven able to fail.
"""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
from attach_review_cards import (  # noqa: E402
    GateError, build, card_id_of, check_card_gates, summarize, summarize_pool)

RR_COLS = ("evidence_id", "sys_id", "shelfmark", "library_code", "page_id",
           "work_id", "locus_label", "locus_status", "main_pool",
           "matched_letters", "coverage_ppm", "source_corpus",
           "routing_status", "novelty_status", "confidence_band",
           "router_verdict", "adjudication_status", "claim_type", "domain")


def row(ev, page, work, **kw):
    d = dict(evidence_id=ev, sys_id="99123", shelfmark="T-S 1.1",
             library_code="CUL", page_id=page, work_id=work,
             locus_label=None, locus_status="resolved", main_pool=1,
             matched_letters=100, coverage_ppm=5000, source_corpus="sefaria",
             routing_status="shipped", novelty_status="confirms",
             confidence_band="tier_a", router_verdict="same_work",
             adjudication_status="unreviewed", claim_type="direct_witness",
             domain="halakha")
    d.update(kw)
    return tuple(d[c] for c in RR_COLS)


def make_db(path, rows, members, works=None, assertions=(),
            root_verified="yes"):
    con = sqlite3.connect(path)
    # evidence_id is the PK in the real artifact -- card_member's FK needs it
    con.execute("CREATE TABLE review_row(%s)" % ", ".join(
        ("evidence_id TEXT PRIMARY KEY" if c == "evidence_id" else
         (f"{c} INT" if c in ("main_pool", "matched_letters", "coverage_ppm")
          else f"{c} TEXT")) for c in RR_COLS))
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    con.execute("""CREATE TABLE known_work(
  kw_id TEXT PRIMARY KEY, anchor TEXT, title TEXT, provisional INT NOT NULL)""")
    con.execute("""CREATE TABLE known_work_member(
  kw_id TEXT, work_id TEXT, scope TEXT, scope_prefix TEXT, basis TEXT,
  route_basis TEXT, evidence_rows INT)""")
    con.execute("""CREATE TABLE known_work_assertion(
  kw_id TEXT, work_id TEXT PRIMARY KEY, note TEXT)""")
    con.executemany("INSERT INTO review_row VALUES (%s)"
                    % ",".join("?" * len(RR_COLS)), rows)
    seen_kw = {}
    for kw, wid, scope, prefix, basis, rbasis, n in members:
        seen_kw.setdefault(kw, 0)
        con.execute("INSERT INTO known_work_member VALUES (?,?,?,?,?,?,?)",
                    (kw, wid, scope, prefix, basis, rbasis, n))
    for kw in (works or {k: 0 for k in seen_kw}):
        con.execute("INSERT INTO known_work VALUES (?,?,?,?)",
                    (kw, "work:" + kw, "כותרת", (works or {}).get(kw, 0)))
    for kw, wid in assertions:
        con.execute("INSERT INTO known_work_assertion VALUES (?,?,?)",
                    (kw, wid, "census member with no evidence rows"))
    con.execute("INSERT INTO meta VALUES ('work_registry.pins_root_verified',?)",
                (root_verified,))
    con.execute("INSERT INTO meta VALUES "
                "('work_registry.pins_sha256','ab' )")
    con.commit()
    con.close()


BASE_ROWS = [
    # one page, one known work, TWO witnesses -> one card, two members
    row("e1", "p1", "wA", locus_label="פרק א"),
    row("e2", "p1", "wB", locus_label="ב ע\"א"),
    # a second page of the same work -> its own card
    row("e3", "p2", "wA", locus_label="פרק ב"),
    # an anthology container routed by prefix
    row("e4", "p1", "rsC", locus_label="אסופה, חלק ראשון, פרק א"),
    row("e5", "p1", "rsC", locus_label="אסופה, חלק שני, פרק א"),
]
BASE_MEMBERS = [
    ("kw1", "wA", "whole", None, "owner_merge", None, 2),
    ("kw1", "wB", "whole", None, "owner_merge", None, 1),
    ("kw2", "rsC", "אסופה, חלק ראשון", "אסופה, חלק ראשון", "scope_map",
     "division_title", 1),
    ("kw3", "rsC", "אסופה, חלק שני", "אסופה, חלק שני", "scope_map",
     "pending_owner", 1),
]


@pytest.fixture
def db(tmp_path):
    p = str(tmp_path / "cards.db")
    make_db(p, BASE_ROWS, BASE_MEMBERS)
    return p


def test_green_projection(db):
    cards, members, ms = build(db, say=lambda *a: None)
    assert (cards, members, ms) == (4, 5, 1)
    con = sqlite3.connect(db)
    got = {(c, k): (n, w, kwn, loc, lv) for c, k, n, w, kwn, loc, lv in
           con.execute("SELECT page_id, kw_id, evidence_rows, witnesses, "
                       "kw_witnesses, locus_label, locus_variants FROM card")}
    # p1 x kw1 holds BOTH witnesses; its two rows carry different loci, so the
    # card states none and says there are two
    assert got[("p1", "kw1")] == (2, 2, 2, None, 2)
    assert got[("p2", "kw1")] == (1, 1, 2, "פרק ב", 1)
    # the container's two divisions are two different known works
    assert got[("p1", "kw2")][0] == 1 and got[("p1", "kw3")][0] == 1
    mem = dict(con.execute("SELECT evidence_id, work_id || '/' || scope "
                           "FROM card_member"))
    assert mem["e4"] == "rsC/אסופה, חלק ראשון"
    assert mem["e5"] == "rsC/אסופה, חלק שני"
    # provenance survives per member
    assert con.execute("SELECT route_basis FROM card_member WHERE "
                       "evidence_id='e5'").fetchone()[0] == "pending_owner"
    # content-derived ids
    assert con.execute("SELECT card_id FROM card WHERE page_id='p2'"
                       ).fetchone()[0] == card_id_of("p2", "kw1")
    n = dict(con.execute("SELECT key, value FROM meta WHERE key LIKE 'card%'"))
    con.close()
    assert (n["card_grain.cards"], n["card_grain.members"],
            n["card_grain.manuscripts"]) == ("4", "5", "1")


def test_disagreeing_rows_read_mixed(tmp_path):
    p = str(tmp_path / "m.db")
    make_db(p, [row("e1", "p1", "wA", novelty_status="confirms", main_pool=1,
                    source_corpus="sefaria"),
                row("e2", "p1", "wB", novelty_status="diverges_work",
                    main_pool=0, source_corpus="msource")],
            [("kw1", "wA", "whole", None, "census", None, 1),
             ("kw1", "wB", "whole", None, "census", None, 1)])
    build(p, say=lambda *a: None)
    con = sqlite3.connect(p)
    nov, pool, corp, band = con.execute(
        "SELECT novelty_status, main_pool, source_corpora, confidence_band "
        "FROM card").fetchone()
    con.close()
    assert (nov, pool) == ("mixed", "mixed")
    assert corp == "msource · sefaria"        # both named, neither dropped
    assert band == "tier_a"                   # agreed values stay concrete


def test_null_is_named_not_dropped(tmp_path):
    """A row that was never scored must not read as agreement with a scored
    one."""
    p = str(tmp_path / "n.db")
    make_db(p, [row("e1", "p1", "wA", confidence_band="tier_a"),
                row("e2", "p1", "wB", confidence_band=None)],
            [("kw1", "wA", "whole", None, "census", None, 1),
             ("kw1", "wB", "whole", None, "census", None, 1)])
    build(p, say=lambda *a: None)
    con = sqlite3.connect(p)
    assert con.execute("SELECT confidence_band FROM card").fetchone()[0] == "mixed"
    con.close()
    assert summarize(["x", None]) == "mixed"
    assert summarize([None, None]) == "unset"
    assert summarize_pool([None, None]) == "unset"


def test_unrouted_container_row_refuses(tmp_path):
    p = str(tmp_path / "u.db")
    make_db(p, [row("e1", "p1", "rsC", locus_label="אסופה, חלק שלישי, פרק א"),
                row("e2", "p1", "rsC", locus_label="אסופה, חלק ראשון, פרק א")],
            [("kw2", "rsC", "אסופה, חלק ראשון", "אסופה, חלק ראשון",
              "scope_map", "division_title", 1),
             ("kw3", "rsC", "אסופה, חלק שני", "אסופה, חלק שני", "scope_map",
              "division_title", 1)])
    with pytest.raises(GateError, match="route to no witness scope"):
        build(p, say=lambda *a: None)


def test_work_without_membership_refuses(tmp_path):
    p = str(tmp_path / "w.db")
    make_db(p, [row("e1", "p1", "wA"), row("e2", "p1", "wOrphan")],
            [("kw1", "wA", "whole", None, "singleton", None, 1)])
    with pytest.raises(GateError, match="no known-work membership"):
        build(p, say=lambda *a: None)


def test_row_count_drift_from_the_registry_refuses(tmp_path):
    """The two grains must agree on how many rows each witness holds."""
    p = str(tmp_path / "d.db")
    make_db(p, [row("e1", "p1", "wA"), row("e2", "p2", "wA")],
            [("kw1", "wA", "whole", None, "singleton", None, 1)])  # pinned 1, live 2
    with pytest.raises(GateError, match="differs from known_work_member"):
        build(p, say=lambda *a: None)


def test_assertion_with_evidence_refuses(tmp_path):
    """known_work_assertion is an identity claim with NO evidence."""
    p = str(tmp_path / "a.db")
    make_db(p, [row("e1", "p1", "wA")],
            [("kw1", "wA", "whole", None, "singleton", None, 1)],
            assertions=[("kw1", "wA")])
    with pytest.raises(GateError, match="an assertion is never evidence"):
        build(p, say=lambda *a: None)


def test_unverified_registry_root_refuses(tmp_path):
    p = str(tmp_path / "r.db")
    make_db(p, [row("e1", "p1", "wA")],
            [("kw1", "wA", "whole", None, "singleton", None, 1)],
            root_verified="no")
    with pytest.raises(GateError, match="verified trusted root"):
        build(p, say=lambda *a: None)


def test_missing_registry_refuses(tmp_path):
    p = str(tmp_path / "e.db")
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE review_row(evidence_id TEXT)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    con.commit()
    con.close()
    with pytest.raises(GateError, match="run build_work_registry.py first"):
        build(p, say=lambda *a: None)


# ---- the reconciliation gates, exercised directly -------------------------
def _card(cid="c1", page="p1", kw="kw1", n=1, w=1, kwn=1, loc=None, lv=0):
    return (cid, page, kw, "99123", "T-S 1.1", "CUL", n, w, kwn, 100, 5000,
            loc, lv, "resolved", "yes", "shipped", "confirms", "tier_a",
            "same_work", "unreviewed", "direct_witness", "halakha",
            "sefaria", 0)


MEMBERS = {("wA", "whole"): dict(prefix=None, basis="singleton",
                                 route_basis=None, rows=1)}


def test_evidence_row_in_two_cards_refuses():
    cards = [_card("c1", n=1), _card("c2", page="p2", n=1)]
    mem = [("e1", "c1", "wA", "whole", None, "singleton", None),
           ("e1", "c2", "wA", "whole", None, "singleton", None)]
    with pytest.raises(GateError, match="more than one card"):
        check_card_gates(cards, mem, {}, MEMBERS, {"kw1": 1})


def test_member_count_mismatch_refuses():
    cards = [_card("c1", n=2)]
    mem = [("e1", "c1", "wA", "whole", None, "singleton", None)]
    with pytest.raises(GateError, match="evidence_rows=2 but 1 members"):
        check_card_gates(cards, mem, {}, MEMBERS, {"kw1": 1})


def test_more_witnesses_than_the_known_work_has_refuses():
    cards = [_card("c1", n=1, w=3, kwn=2)]
    mem = [("e1", "c1", "wA", "whole", None, "singleton", None)]
    with pytest.raises(GateError, match="witnesses aligned but its known work"):
        check_card_gates(cards, mem, {}, MEMBERS, {"kw1": 2})


def test_locus_label_with_several_labels_refuses():
    cards = [_card("c1", n=1, loc="פרק א", lv=2)]
    mem = [("e1", "c1", "wA", "whole", None, "singleton", None)]
    with pytest.raises(GateError, match="locus_label set with 2 distinct"):
        check_card_gates(cards, mem, {}, MEMBERS, {"kw1": 1})


def test_member_outside_the_registry_refuses():
    cards = [_card("c1", n=1)]
    mem = [("e1", "c1", "wGhost", "whole", None, "singleton", None)]
    with pytest.raises(GateError, match="is not a known_work_member"):
        check_card_gates(cards, mem, {}, MEMBERS, {"kw1": 1})


def test_two_cards_for_one_page_and_work_refuses():
    cards = [_card("c1"), _card("c2")]        # same (page, kw), different ids
    mem = [("e1", "c1", "wA", "whole", None, "singleton", None),
           ("e2", "c2", "wA", "whole", None, "singleton", None)]
    with pytest.raises(GateError, match="two cards for one"):
        check_card_gates(cards, mem, {}, MEMBERS, {"kw1": 1})
