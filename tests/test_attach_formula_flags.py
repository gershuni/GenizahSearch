# -*- coding: utf-8 -*-
"""The liturgy/formulary detector: classification rules + end-to-end attach.

The pinned strings are the owner-graded cards that defined each rule -- if a
regex edit stops catching one of them, the corresponding test goes red.
"""
import json
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import attach_formula_flags as aff  # noqa: E402


def test_embedded_sections_from_the_graded_formula_cards():
    # the five owner-graded formula cards' loci (v5 decks, 2026-08-30)
    for locus in ("סדר תפלות כל השנה, נוסח הברכות האמצעיות",
                  "סדר רב עמרם גאון, ברכות השחר",
                  "השטרות (לרה\"ג), סימן ד - שטר אביזאריה",
                  "סדר רב עמרם גאון, סדר ראש השנה",
                  "מחזור ויטרי, הלכות פסח (עמ' 310-254), סימן קא"):
        assert aff.classify("Halakhic / Halakhic- Gaonim", locus, None,
                            False) == "embedded_section", locus


def test_halakha_about_prayer_does_not_fire():
    # halakhic prose ABOUT prayer -- graded correct by the owner
    for locus in ("הלכות קריאת שמע, פרק ב, הלכה יב",
                  "הלכות תפילה ונשיאת כפים, פרק יג, הלכה ח"):
        assert aff.classify("Halakhic / Mishneh Torah and its Commentaries",
                            locus, None, False) is None, locus
    # ...but a nusach section inside a hilkhot header still fires
    assert aff.classify("Halakhic / X", "הלכות גירושין, נוסח הגט", None,
                        False) == "embedded_section"
    # the Haggadah embedded in Mishneh Torah -- the gap the owner's question
    # exposed (2026-08-30): 'נוסח ההגדה' was not in the keyword list
    assert aff.classify("Halakhic / Mishneh Torah and its Commentaries",
                        "הלכות חמץ ומצה, נוסח ההגדה", None,
                        False) == "embedded_section"


def test_standalone_liturgy_units():
    assert aff.classify("Liturgy and Brakhot / Common Prayers",
                        "מודים", None, False) == "standalone_unit"
    # the domain decides BEFORE the section keywords, and the WHOLE liturgy
    # parent counts -- the Passover Haggadah leaf was the gap that sent the
    # hazakah-responsum discovery to the wrong bucket
    assert aff.classify("Liturgy and Brakhot / Passover Haggadah",
                        "הגדה של פסח", None, False) == "standalone_unit"


def test_documentary_page_guard():
    # a pure deed page matching a literary work -> flagged
    assert aff.classify("Halakhic / Halakhic- Gaonim", "סימן ג",
                        "שטר (קטע).", False) == "documentary_page"
    # a REUSE page carrying Bible text too -> spared (its literary match is real)
    assert aff.classify("Biblical Exegesis / Biblical Exegesis- Rabbanite",
                        "פרק א", "מקרא [טקסט];תעודות אישיות ושטרות.",
                        False) is None
    # canonical works are exempt even on a pure deed page (page reuse: the
    # "deed" side may simply carry Bible text)
    assert aff.classify("Bible: Texts and Translations / Bible: Texts",
                        "פרק ח", "שטר (קטע).", True) is None


def test_attach_end_to_end(tmp_path):
    db = tmp_path / "mini.db"
    con = sqlite3.connect(db)
    con.executescript("""
        CREATE TABLE review_row(evidence_id TEXT PRIMARY KEY, domain TEXT,
            locus_label TEXT, catalogue_title TEXT);
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE facet_row(evidence_id TEXT);
        INSERT INTO review_row VALUES
          ('e1','Halakhic / Halakhic- Gaonim','סדר רב עמרם גאון, ברכות השחר',NULL),
          ('e2','Liturgy and Brakhot / Common Prayers','מודים',NULL),
          ('e3','Halakhic / Halakhic- Gaonim','סימן ג','שטר (קטע).'),
          ('e4','Midrash / Aggadic Midrashim','פרק א','מדרש;דרשה'),
          ('e5','Bible: Texts and Translations / Bible: Texts','פרק ח','שטר (קטע).');
    """)
    con.commit()
    con.close()
    counts = aff.attach(str(db), say=lambda *a: None)
    assert counts == {"embedded_section": 1, "standalone_unit": 1,
                      "documentary_page": 1}
    con = sqlite3.connect(db)
    got = dict(con.execute("SELECT evidence_id, kind FROM formula_fact"))
    assert got == {"e1": "embedded_section", "e2": "standalone_unit",
                   "e3": "documentary_page"}
    # e4 (plain midrash) and e5 (canonical on a deed page) have NO row --
    # the rows that prove the detector can say no
    assert con.execute("SELECT COUNT(*) FROM sqlite_master "
                       "WHERE name='facet_row'").fetchone()[0] == 0
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert "doc.formula_kind" in meta
    assert json.loads(meta["formula_fact.counts"])["standalone_unit"] == 1
    con.close()
