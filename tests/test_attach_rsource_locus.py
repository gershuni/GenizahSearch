# -*- coding: utf-8 -*-
"""R-source locus attach: header indexing, placement, and end-to-end write."""
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import attach_rsource_locus as arl  # noqa: E402


RAW = ("prefix text before any header\n"
       "### תוספתא -- אהלות -- פרק א\n"
       "first section body here\n"
       "### תוספתא -- אהלות -- פרק ב\n"
       "second section body\n")


def test_index_headers_positions_and_labels():
    hs = arl.index_headers(RAW)
    assert len(hs) == 2
    p1, l1 = hs[0]
    assert RAW[p1:p1 + 3] == "###"
    assert l1 == "אהלות, פרק א"      # category dropped, comma-joined


def test_locus_for_innermost_preceding():
    hs = arl.index_headers(RAW)
    pos = [p for p, _ in hs]
    lab = [l for _, l in hs]
    h1, h2 = pos
    assert arl.locus_for(pos, lab, 0) is None                 # before all
    assert arl.locus_for(pos, lab, h1) == lab[0]              # at the header
    assert arl.locus_for(pos, lab, h2 - 1) == lab[0]
    assert arl.locus_for(pos, lab, h2 + 5) == lab[1]
    assert arl.locus_for(pos, lab, len(RAW)) == lab[1]


def test_clean_label_collapses_and_caps():
    assert arl.clean_label("  a\t\tb   c ") == "a b c"
    long = "א" * 300
    out = arl.clean_label(long)
    assert len(out) == arl.LABEL_MAX + 1 and out.endswith("…")


def test_clean_label_strips_the_closing_marker():
    # headers are written `### title ###` -- the closing marker is not locus
    assert arl.clean_label(" סדר עולם רבה -- פרק כה ###") == \
        "סדר עולם רבה, פרק כה"
    assert arl.clean_label(" title ## ") == "title"


@pytest.fixture()
def mini(tmp_path):
    raw_path = tmp_path / "work.txt"
    raw_path.write_text(RAW, encoding="utf-8")
    keys = tmp_path / "keys.json"
    keys.write_text(json.dumps({"RS:1.1": str(raw_path)}), encoding="utf-8")

    db = tmp_path / "mini.db"
    con = sqlite3.connect(db)
    con.executescript("""
        CREATE TABLE review_row(
            evidence_id TEXT PRIMARY KEY, source_corpus TEXT,
            ref_char_start INTEGER, ref_provenance_status TEXT,
            work_title TEXT, locus_label TEXT, locus_status TEXT,
            witness_id TEXT);
        CREATE TABLE reference_witness(
            witness_id TEXT PRIMARY KEY, source_file_id TEXT);
        CREATE TABLE source_file(id TEXT PRIMARY KEY, ref_id TEXT);
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO source_file VALUES ('sf1','RS:1.1');
        INSERT INTO reference_witness VALUES ('w1','sf1');
    """)
    h2 = RAW.index("### תוספתא -- אהלות -- פרק ב")
    con.executemany("INSERT INTO review_row VALUES (?,?,?,?,?,?,?,?)", [
        ("ev_sec2", "rsource", h2 + 40, "ok", "כותרת", None, "not_computed", "w1"),
        ("ev_pre", "rsource", 3, "ok", "כותרת העבודה", None, "not_computed", "w1"),
        ("ev_stream", "rsource", None, "stream_fallback", "כותרת", None,
         "not_computed", None),
        ("ev_other", "sefaria", 5, "ok", "אחר", "קיים", "resolved", None),
    ])
    con.commit()
    con.close()
    return db, keys


def test_attach_end_to_end(mini):
    db, keys = mini
    counts = arl.attach(str(db), str(keys), masking=False,
                        say=lambda *a: None)
    assert counts == {"resolved": 1, "whole_work": 1, "not_computed": 1}
    con = sqlite3.connect(db)
    got = dict((r[0], (r[1], r[2])) for r in con.execute(
        "SELECT evidence_id, locus_label, locus_status FROM review_row"))
    assert got["ev_sec2"] == ("אהלות, פרק ב", "resolved")
    # before the first header -> the base corpora's whole_work shape
    assert got["ev_pre"] == ("כותרת העבודה", "whole_work")
    # no offsets -> untouched honest absence
    assert got["ev_stream"] == (None, "not_computed")
    # other corpora untouched
    assert got["ev_other"] == ("קיים", "resolved")
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert json.loads(meta["rsource_locus.counts"])["resolved"] == 1
    con.close()


def test_attach_moves_with_the_offset(mini):
    """Mutation: shift a row's offset past the next header -> its locus moves.
    Pins that the label really is a function of the stored position."""
    db, keys = mini
    con = sqlite3.connect(db)
    con.execute("UPDATE review_row SET ref_char_start=? WHERE evidence_id='ev_pre'",
                (RAW.index("first section body"),))
    con.commit()
    con.close()
    arl.attach(str(db), str(keys), masking=False, say=lambda *a: None)
    con = sqlite3.connect(db)
    lab = con.execute("SELECT locus_label FROM review_row "
                      "WHERE evidence_id='ev_pre'").fetchone()[0]
    con.close()
    assert lab == "אהלות, פרק א"


def test_attach_refuses_unknown_ref_id(mini):
    db, keys = mini
    con = sqlite3.connect(db)
    con.execute("UPDATE source_file SET ref_id='RS:9.9'")
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="no path for RS:9.9"):
        arl.attach(str(db), str(keys), masking=False, say=lambda *a: None)
