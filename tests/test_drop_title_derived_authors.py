# -*- coding: utf-8 -*-
"""Gates for scripts/drop_title_derived_authors.py."""
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
from drop_title_derived_authors import GateError, apply, bare  # noqa: E402


def make_db(path, rows):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE review_row(evidence_id TEXT PRIMARY KEY, "
                "work_id TEXT, work_title TEXT, work_author TEXT, "
                "author_provenance TEXT)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    con.execute("CREATE TABLE facet_row(evidence_id TEXT)")
    con.executemany("INSERT INTO review_row VALUES (?,?,?,?,?)", rows)
    con.commit()
    con.close()


def state(path):
    con = sqlite3.connect(path)
    got = {w: (a, p) for w, a, p in con.execute(
        "SELECT work_id, work_author, author_provenance FROM review_row")}
    facets = bool(con.execute("SELECT 1 FROM sqlite_master WHERE "
                              "name='facet_row'").fetchone())
    con.close()
    return got, facets


def test_redundant_author_is_cleared(tmp_path):
    p = str(tmp_path / "a.db")
    make_db(p, [("e1", "rs1", "פסקי חלה לרשב״א", "רשב״א", "from_title"),
                ("e2", "rs1", "פסקי חלה לרשב״א", "רשב״א", "from_title"),
                # untouched: a real authority author
                ("e3", "w1", "ספר כלשהו", "פלוני", "author_authority")])
    assert apply(p, say=lambda *a: None) == (1, 0)
    got, facets = state(p)
    assert got["rs1"] == (None, None)
    assert got["w1"] == ("פלוני", "author_authority")
    assert not facets            # dropped so the projection is rebuilt


def test_owner_ruled_author_is_kept_and_relabelled(tmp_path):
    """Three works' author IS an owner decision, recorded with the mechanical
    label; the ruling must survive and be named for what it is."""
    p = str(tmp_path / "b.db")
    make_db(p, [("e1", "rs86b18b09c3c0", "שו״ת רבי אברהם בן הרמב״ם",
                 "רבי אברהם בן הרמב״ם", "from_title")])
    assert apply(p, say=lambda *a: None) == (0, 1)
    got, _ = state(p)
    assert got["rs86b18b09c3c0"] == ("רבי אברהם בן הרמב״ם", "owner_ruling")


def test_additive_author_refuses(tmp_path):
    """An author the title does NOT spell is a real attribution: the script
    stops rather than deleting it."""
    p = str(tmp_path / "c.db")
    make_db(p, [("e1", "rs9", "ספר האשכול", "אברהם בן יצחק", "from_title")])
    with pytest.raises(GateError, match="say something the title does not"):
        apply(p, say=lambda *a: None)
    got, facets = state(p)
    assert got["rs9"] == ("אברהם בן יצחק", "from_title")   # nothing touched
    assert facets


def test_no_label_is_left_behind(tmp_path):
    p = str(tmp_path / "d.db")
    make_db(p, [("e1", "rs1", "שו״ת מהרי״ל", "מהרי״ל", "from_title")])
    apply(p, say=lambda *a: None)
    con = sqlite3.connect(p)
    left = con.execute("SELECT COUNT(*) FROM review_row WHERE "
                       "author_provenance='from_title'").fetchone()[0]
    note = con.execute("SELECT value FROM meta WHERE key="
                       "'rsource_authors.title_derived_cleared'").fetchone()[0]
    con.close()
    assert left == 0 and "owner 2026-09-01" in note


def test_rerun_is_a_no_op(tmp_path):
    p = str(tmp_path / "e.db")
    make_db(p, [("e1", "rs1", "שו״ת מהרי״ל", "מהרי״ל", "from_title")])
    apply(p, say=lambda *a: None)
    assert apply(p, say=lambda *a: None) == (0, 0)


def test_containment_ignores_gershayim_and_spacing():
    """A name inside a title differs in quote marks, brackets and spacing."""
    assert bare("רשב״א") in bare("פסקי חלה לרשב\"א")
    assert bare("ר׳ יוסף בן עקנין") in bare("מבוא התלמוד לר' יוסף בן עקנין")
    assert bare("אברהם בן יצחק") not in bare("ספר האשכול")
