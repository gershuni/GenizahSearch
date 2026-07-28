# -*- coding: utf-8 -*-
"""Tests for `scripts/cert01_render_deck.py` (Phase 135, plan 135-09, Task 2
rendering correction).

Every fixture below is FABRICATED (synthetic `w000xxx`/opaque page/sys ids,
a two-line placeholder reference text) -- never real research content. Skips
cleanly (matching the `tests/test_discovery_coverage_replication.py`
precedent) when the gitignored `same_work_spike/probe/scripts/e1_deck.py`
research tree is absent on this box.
"""
import json
import sqlite3

import pytest

from scripts import cert01_render_deck as render_mod


def _e1_or_skip():
    try:
        return render_mod.load_e1_render_deps()
    except RuntimeError:
        pytest.skip("same_work_spike/probe/scripts/e1_deck.py not present on this box")


# ---------------------------------------------------------------------------
# build_reverse_crosswalk
# ---------------------------------------------------------------------------


def test_build_reverse_crosswalk_inverts_mapping(tmp_path):
    crosswalk = {"J:01-fake-work": "w000001", "M:Yfake000123": "w000002"}
    p = tmp_path / "crosswalk.json"
    p.write_text(json.dumps(crosswalk), encoding="utf-8")
    reverse = render_mod.build_reverse_crosswalk(p)
    assert reverse == {"w000001": "J:01-fake-work", "w000002": "M:Yfake000123"}


# ---------------------------------------------------------------------------
# load_gold_pool / load_demoted_spans / load_neutral_titles
# ---------------------------------------------------------------------------


def test_load_gold_pool_keys_by_page_and_work(tmp_path, monkeypatch):
    pool_path = tmp_path / "gold.jsonl"
    pool_path.write_text(
        json.dumps({"page_id": "pG1", "work_id": "M:Yfake1", "o0": 10, "o1": 50,
                    "work_title": "fake title"}) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(render_mod, "GOLD_POOL_PATH", pool_path)
    pool = render_mod.load_gold_pool()
    assert pool[("pG1", "M:Yfake1")]["o0"] == 10


def test_load_demoted_spans_scoped_to_later_shared_text(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE works (work_id TEXT PRIMARY KEY, canonical_work_id TEXT, source_corpus TEXT);
        CREATE TABLE discovery_claim (page_id TEXT, work_id TEXT, claim_id TEXT,
            claim_type TEXT, display_evidence_id TEXT);
        CREATE TABLE discovery_evidence (evidence_id TEXT PRIMARY KEY, claim_id TEXT,
            evidence_kind TEXT, evidence_source TEXT, confidence_band TEXT,
            adjudication_status TEXT, routing_status TEXT, routing_reason TEXT,
            a_page_id TEXT, sys_id TEXT, matched_letters INTEGER,
            span_start INTEGER, span_end INTEGER);
        """
    )
    conn.execute("INSERT INTO works VALUES ('w000001','w000001','sefaria')")
    conn.execute("INSERT INTO discovery_claim VALUES ('pD','w000001','claimD','direct_witness','evD')")
    conn.execute(
        "INSERT INTO discovery_evidence VALUES ('evD','claimD','witness','track1_direct','tier_a',"
        "'unreviewed','review_only','later_shared_text','pD','sysD',100,5,55)"
    )
    conn.execute("INSERT INTO works VALUES ('w000002','w000002','sefaria')")
    conn.execute("INSERT INTO discovery_claim VALUES ('pS','w000002','claimS','direct_witness','evS')")
    conn.execute(
        "INSERT INTO discovery_evidence VALUES ('evS','claimS','witness','track1_direct','tier_a',"
        "'unreviewed','shipped','none','pS','sysS',100,1,10)"
    )
    conn.commit()
    db_path = tmp_path / "fixture.db"
    dest = sqlite3.connect(str(db_path))
    conn.backup(dest)
    dest.close()

    spans = render_mod.load_demoted_spans(str(db_path))
    assert ("pD", "w000001") in spans
    assert spans[("pD", "w000001")]["span_start"] == 5
    assert ("pS", "w000002") not in spans  # not later_shared_text -- excluded


def test_load_neutral_titles(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE works (work_id TEXT PRIMARY KEY, canonical_work_id TEXT, "
                "neutral_title TEXT, author TEXT, genre TEXT, source_corpus TEXT)")
    conn.execute("INSERT INTO works VALUES ('w000001','w000001','Fake Neutral Title','Fake Author',NULL,'sefaria')")
    conn.commit()
    db_path = tmp_path / "fixture.db"
    dest = sqlite3.connect(str(db_path))
    conn.backup(dest)
    dest.close()

    titles = render_mod.load_neutral_titles(str(db_path), {"w000001"})
    assert titles == {"w000001": "Fake Neutral Title"}


# ---------------------------------------------------------------------------
# patch_export_for_ledger_shape -- against a REAL render_deck() output
# (fabricated 2-card frame), proving the string-anchor patch survives an
# actual reused-function call, not just a hand-crafted stand-in string.
# ---------------------------------------------------------------------------


def test_patch_export_for_ledger_shape_on_real_render_output(tmp_path):
    e1, RefText = _e1_or_skip()

    research_conn = sqlite3.connect(":memory:")
    research_conn.execute("CREATE TABLE pages (page_id TEXT PRIMARY KEY, text TEXT)")
    research_conn.execute("INSERT INTO pages VALUES ('pF1', 'א' * 200)")
    research_conn.commit()

    items = [{
        "no": 1, "uid": "pF1|FAKE:1", "role": "candidate", "band": None,
        "row": {"page_id": "pF1", "sys_id": "990000000000000000", "work_id": "FAKE:NOTAREALWORK",
                "o0": 5, "o1": 40, "work_title": "Fabricated Placeholder Title", "cat": ""},
    }]
    reftext = RefText()  # a real instance is fine -- .passage() just returns '' for an unknown id
    html = e1.render_deck("cert01test", items, research_conn, {}, reftext, "TEST DECK")
    research_conn.close()

    patched = render_mod.patch_export_for_ledger_shape(html, "cert01test")

    assert "id=graderName" in patched
    assert "const out=[];" in patched
    assert "cert01_deck_verdicts.json" in patched
    # the OLD dict-of-dicts export shape must be gone
    assert "out={};for(const it of ITEMS){if(store[it.uid])out[it.uid]=store[it.uid];}" not in patched


def test_patch_export_for_ledger_shape_raises_on_anchor_mismatch():
    with pytest.raises(ValueError, match="toolbar anchor"):
        render_mod.patch_export_for_ledger_shape("<html>not a real deck</html>", "cert01")


# ---------------------------------------------------------------------------
# build_render_items -- resolves candidate + gold cards without redrawing
# ---------------------------------------------------------------------------


def test_build_render_items_preserves_frozen_order_and_resolves_spans(tmp_path):
    # Minimal fabricated sidecar with one shipped tier_a estimand row.
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE works (work_id TEXT PRIMARY KEY, canonical_work_id TEXT,
            neutral_title TEXT, author TEXT, genre TEXT, source_corpus TEXT);
        CREATE TABLE discovery_claim (page_id TEXT, work_id TEXT, claim_id TEXT,
            claim_type TEXT, display_evidence_id TEXT);
        CREATE TABLE discovery_evidence (evidence_id TEXT PRIMARY KEY, claim_id TEXT,
            evidence_kind TEXT, evidence_source TEXT, confidence_band TEXT,
            adjudication_status TEXT, routing_status TEXT, routing_reason TEXT,
            a_page_id TEXT, sys_id TEXT, matched_letters INTEGER,
            span_start INTEGER, span_end INTEGER);
        CREATE TABLE witness_unit_members (unit_id TEXT, sys_id TEXT, merge_basis TEXT);
        """
    )
    conn.execute("INSERT INTO works VALUES ('w000001','w000001','Fake Title','Fake Author',NULL,'sefaria')")
    conn.execute("INSERT INTO discovery_claim VALUES ('p1','w000001','claim1','direct_witness','ev1')")
    conn.execute(
        "INSERT INTO discovery_evidence VALUES ('ev1','claim1','witness','track1_direct','tier_a',"
        "'unreviewed','shipped','none','p1','sys1',80,3,30)"
    )
    conn.commit()
    db_path = tmp_path / "sidecar.db"
    dest = sqlite3.connect(str(db_path))
    conn.backup(dest)
    dest.close()

    research_conn = sqlite3.connect(":memory:")
    research_conn.execute("CREATE TABLE pages (page_id TEXT PRIMARY KEY, text TEXT)")
    research_conn.execute("INSERT INTO pages VALUES ('p1', ?)", ("א" * 100,))
    research_conn.commit()
    research_db_path = tmp_path / "research.db"
    dest2 = sqlite3.connect(str(research_db_path))
    research_conn.backup(dest2)
    dest2.close()

    crosswalk_path = tmp_path / "crosswalk.json"
    crosswalk_path.write_text(json.dumps({"J:fake-raw-id": "w000001"}), encoding="utf-8")

    deck_cards = [
        {"uid": "p1|w000001", "role": "candidate", "stratum": "sefaria:high",
         "page_id": "p1", "canonical_work_id": "w000001", "sys_id": "sys1"},
    ]

    items = render_mod.build_render_items(
        deck_cards, sidecar_db_path=str(db_path), research_db_path=str(research_db_path),
        crosswalk_path=str(crosswalk_path),
    )
    assert len(items) == 1
    row = items[0]["row"]
    assert row["work_id"] == "J:fake-raw-id"  # resolved via the reverse crosswalk
    assert row["o0"] == 3 and row["o1"] == 30  # from discovery_evidence.span_start/span_end
    assert row["work_title"] == "Fake Title"
    assert row["cat"] == ""  # never source_corpus
    assert items[0]["uid"] == "p1|w000001"


def test_build_render_items_raises_on_missing_crosswalk_entry(tmp_path):
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        """
        CREATE TABLE works (work_id TEXT PRIMARY KEY, canonical_work_id TEXT,
            neutral_title TEXT, author TEXT, genre TEXT, source_corpus TEXT);
        CREATE TABLE discovery_claim (page_id TEXT, work_id TEXT, claim_id TEXT,
            claim_type TEXT, display_evidence_id TEXT);
        CREATE TABLE discovery_evidence (evidence_id TEXT PRIMARY KEY, claim_id TEXT,
            evidence_kind TEXT, evidence_source TEXT, confidence_band TEXT,
            adjudication_status TEXT, routing_status TEXT, routing_reason TEXT,
            a_page_id TEXT, sys_id TEXT, matched_letters INTEGER,
            span_start INTEGER, span_end INTEGER);
        CREATE TABLE witness_unit_members (unit_id TEXT, sys_id TEXT, merge_basis TEXT);
        """
    )
    conn.execute("INSERT INTO works VALUES ('w000099','w000099','Fake','Fake',NULL,'sefaria')")
    conn.execute("INSERT INTO discovery_claim VALUES ('p9','w000099','claim9','direct_witness','ev9')")
    conn.execute(
        "INSERT INTO discovery_evidence VALUES ('ev9','claim9','witness','track1_direct','tier_a',"
        "'unreviewed','shipped','none','p9','sys9',80,3,30)"
    )
    conn.commit()
    db_path = tmp_path / "sidecar.db"
    dest = sqlite3.connect(str(db_path))
    conn.backup(dest)
    dest.close()

    research_conn = sqlite3.connect(":memory:")
    research_conn.execute("CREATE TABLE pages (page_id TEXT PRIMARY KEY, text TEXT)")
    research_conn.execute("INSERT INTO pages VALUES ('p9', ?)", ("א" * 100,))
    research_conn.commit()
    research_db_path = tmp_path / "research.db"
    dest2 = sqlite3.connect(str(research_db_path))
    research_conn.backup(dest2)
    dest2.close()

    crosswalk_path = tmp_path / "crosswalk.json"
    crosswalk_path.write_text(json.dumps({}), encoding="utf-8")  # empty -- no entry for w000099

    deck_cards = [
        {"uid": "p9|w000099", "role": "candidate", "stratum": "sefaria:high",
         "page_id": "p9", "canonical_work_id": "w000099", "sys_id": "sys9"},
    ]
    with pytest.raises(ValueError, match="no reverse-crosswalk"):
        render_mod.build_render_items(
            deck_cards, sidecar_db_path=str(db_path), research_db_path=str(research_db_path),
            crosswalk_path=str(crosswalk_path),
        )
