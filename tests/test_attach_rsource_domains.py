# -*- coding: utf-8 -*-
"""R-source domain mapping: category extraction, closed map, end-to-end."""
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import attach_rsource_domains as ard  # noqa: E402


def test_category_of_first_segment():
    raw = "junk\n### מדרשי אגדה -- סדר עולם רבה -- פרק א ###\nbody\n"
    assert ard.category_of(raw) == "מדרשי אגדה"
    assert ard.category_of("no headers at all") is None


def test_map_values_use_parent_slash_leaf_shape():
    for cat, dom in ard.MAP.items():
        assert " / " in dom, (cat, dom)


@pytest.fixture()
def mini(tmp_path):
    f1 = tmp_path / "a.txt"
    f1.write_text("### מדרשי אגדה -- עבודה -- פרק א ###\nגוף\n", encoding="utf-8")
    f2 = tmp_path / "b.txt"
    f2.write_text("### טור -- חלק א ###\nגוף\n", encoding="utf-8")
    keys = tmp_path / "keys.json"
    keys.write_text(json.dumps({"RS:1.1": str(f1), "RS:2.2": str(f2)}),
                    encoding="utf-8")
    db = tmp_path / "mini.db"
    con = sqlite3.connect(db)
    con.executescript("""
        CREATE TABLE review_row(evidence_id TEXT PRIMARY KEY,
            source_corpus TEXT, domain TEXT, witness_id TEXT);
        CREATE TABLE reference_witness(witness_id TEXT PRIMARY KEY,
            source_file_id TEXT);
        CREATE TABLE source_file(id TEXT PRIMARY KEY, ref_id TEXT);
        CREATE TABLE facet_row(evidence_id TEXT);
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
        INSERT INTO source_file VALUES ('s1','RS:1.1'), ('s2','RS:2.2');
        INSERT INTO reference_witness VALUES ('w1','s1'), ('w2','s2');
        INSERT INTO review_row VALUES
            ('e1','rsource','R-source','w1'),
            ('e2','rsource','R-source','w2'),
            ('e3','sefaria','Midrash / Aggadic Midrashim',NULL),
            ('e4','sefaria','Halakhic / Halakhic- Rishonim and Aharonim',NULL);
    """)
    con.commit()
    con.close()
    return db, keys


def test_attach_end_to_end(mini):
    db, keys = mini
    counts = ard.attach(str(db), str(keys), say=lambda *a: None)
    assert counts == {"Midrash / Aggadic Midrashim": 1,
                      "Halakhic / Halakhic- Rishonim and Aharonim": 1}
    con = sqlite3.connect(db)
    got = dict(con.execute("SELECT evidence_id, domain FROM review_row"))
    assert got["e1"] == "Midrash / Aggadic Midrashim"
    assert got["e2"] == "Halakhic / Halakhic- Rishonim and Aharonim"
    assert got["e3"] == "Midrash / Aggadic Midrashim"     # untouched
    # placeholder gone, facet projection dropped
    assert "R-source" not in got.values()
    assert con.execute("SELECT COUNT(*) FROM sqlite_master "
                       "WHERE name='facet_row'").fetchone()[0] == 0
    con.close()


def test_attach_refuses_an_unmapped_category(mini, tmp_path):
    db, keys = mini
    f3 = tmp_path / "c.txt"
    f3.write_text("### קטגוריה חדשה לגמרי -- עבודה ###\nגוף\n", encoding="utf-8")
    k = json.loads(open(keys, encoding="utf-8").read())
    k["RS:3.3"] = str(f3)
    keys.write_text(json.dumps(k), encoding="utf-8")
    con = sqlite3.connect(db)
    con.executescript("""
        INSERT INTO source_file VALUES ('s3','RS:3.3');
        INSERT INTO reference_witness VALUES ('w3','s3');
        INSERT INTO review_row VALUES ('e5','rsource','R-source','w3');
    """)
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="UNMAPPED"):
        ard.attach(str(db), str(keys), say=lambda *a: None)
    # nothing was written
    con = sqlite3.connect(db)
    assert con.execute("SELECT domain FROM review_row WHERE evidence_id='e1'"
                       ).fetchone()[0] == "R-source"
    con.close()


def test_attach_refuses_a_target_outside_the_artifacts_vocabulary(mini):
    db, keys = mini
    con = sqlite3.connect(db)
    # remove the base row that carries the Halakhic- Rishonim domain, so the
    # mapping target no longer exists in the artifact's own vocabulary
    con.execute("DELETE FROM review_row WHERE evidence_id='e4'")
    con.commit()
    con.close()
    with pytest.raises(SystemExit, match="not present in this artifact"):
        ard.attach(str(db), str(keys), say=lambda *a: None)
