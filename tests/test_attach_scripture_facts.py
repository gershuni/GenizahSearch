# -*- coding: utf-8 -*-
"""The shared-scripture review flag: detectors + end-to-end attach.

The end-to-end test builds a three-row artifact where exactly one row should
fire each detector and one row should stay clean -- the clean row is what
proves the flag CAN say no.
"""
import json
import os
import pickle
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))

import attach_scripture_facts as asf  # noqa: E402


# ---------------------------------------------------------------------------
# fold: display text -> letter-stream alphabet
# ---------------------------------------------------------------------------

def test_fold_drops_niqqud_and_folds_finals():
    # niqqud/te'amim are combining marks outside [א-ת]; finals fold.
    assert asf.fold("שָׁלוֹם") == "שלומ"
    assert asf.fold("אֶרֶץ") == "ארצ"
    # punctuation, digits, latin, spaces: all dropped
    assert asf.fold("א(ב)ג 12 xyz") == "אבג"
    assert asf.fold(None) == ""


# ---------------------------------------------------------------------------
# flank_kind: citation formulas beside the match
# ---------------------------------------------------------------------------

def test_flank_fires_on_the_seder_olam_example():
    # The REAL shape of the row that started this (evidence a732f002e9d8...):
    # the inline citation's letters are part of the letter stream, so the
    # matched span SWALLOWS its opening -- "(דברי" ends the match and the rest
    # sits in the after-flank. Only a scan of the three pieces joined sees the
    # citation whole.
    before = " יהודה ובנימין, ועליהן הכתוב אומר, כה אמר ה' [אלהי ישראל] כתאנים"
    match = "בן שמונה שנים יהויכין במלכו ושלשה חדשים ועשרת ימים מלך בירושלם (דברי"
    after = " הימים ב לו ט), ובמקום אחר הוא אומר בן שמנה עשרה שנה"
    assert asf.flank_kind(before, match, after) == "paren"
    # the split citation is invisible to a flanks-only scan -- the regression
    # this test pins
    assert asf.flank_kind(before, "", after) is None


def test_flank_kinds_and_clean_text():
    assert asf.flank_kind("", "טקסט", "שנאמר לא תרצח") == "formula"
    assert asf.flank_kind("(ישעיה ו ג) לפני", "טקסט", "") == "paren"
    assert asf.flank_kind("שנאמר", "טקסט", "(תהלים א א)") == "both"
    # plain prose, no citation machinery
    assert asf.flank_kind("הלכות שבת פרק ראשון", "טקסט",
                          "ומותר לטלטל בחצר") is None


def test_flank_scans_only_the_near_context():
    # A formula further than FLANK chars from the match must not fire.
    far = "שנאמר" + " " + "א" * (asf.FLANK + 10)
    assert asf.flank_kind(far, "טקסט", "") is None


def test_formula_deep_inside_a_long_match_does_not_fire():
    # A genuine witness of a midrashic work matches long spans that are
    # THEMSELVES full of שנאמר -- the work's own voice. Scanning whole matches
    # flagged 54% of same_work rows; only the match's EDGE windows are signal.
    deep = "א" * (asf.EDGE + 5) + " שנאמר לא תרצח " + "ב" * (asf.EDGE + 5)
    assert asf.flank_kind("דברי הפוסק", deep, "וכן עיקר") is None
    # ...but the same formula AT the edge does fire.
    at_edge = "שנאמר " + "א" * 60
    assert asf.flank_kind("", at_edge, "") == "formula"


# ---------------------------------------------------------------------------
# MaskIndex.distance
# ---------------------------------------------------------------------------

def test_mask_distance():
    m = asf.MaskIndex({"RS:1.1": [[100, 200], [500, 600]]})
    assert m.distance("RS:1.1", 150, 180) == 0      # inside
    assert m.distance("RS:1.1", 190, 250) == 0      # straddles the edge
    assert m.distance("RS:1.1", 230, 260) == 30     # gap after [100,200]
    assert m.distance("RS:1.1", 460, 480) == 20     # gap before [500,600]
    assert m.distance("RS:9.9", 0, 10) is None      # work has no mask


def test_mask_overlap_frac():
    m = asf.MaskIndex({"RS:1.1": [[100, 200], [500, 600]]})
    assert m.overlap_frac("RS:1.1", 150, 180) == 1.0        # fully inside
    assert m.overlap_frac("RS:1.1", 190, 250) == pytest.approx(10 / 60)
    assert m.overlap_frac("RS:1.1", 230, 260) == 0.0        # no contact
    assert m.overlap_frac("RS:1.1", 150, 550) == pytest.approx(100 / 400)
    assert m.overlap_frac("RS:9.9", 0, 10) is None          # work has no mask


# ---------------------------------------------------------------------------
# share: gram membership
# ---------------------------------------------------------------------------

def test_share_verbatim_and_short():
    bible_stream = "בראשיתבראאלהימאתהשמימואתהארצוהארצהיתהתהוובהו"
    grams = {hash(bible_stream[i:i + asf.GRAM])
             for i in range(len(bible_stream) - asf.GRAM + 1)}
    assert asf.share(bible_stream, grams) == 1.0
    assert asf.share(bible_stream[:asf.GRAM - 1], grams) == 0.0  # too short
    assert asf.share("ש" * 40, grams) == 0.0


# ---------------------------------------------------------------------------
# end to end: three rows, three fates
# ---------------------------------------------------------------------------

BIBLE = ("ויהיבשמונימשנהוארבעמאותשנהלצאתבניישראלמארצמצרימ"
         "בשנההרביעיתבחדשזובנהאתהביתלהויקמבנימלכ")


@pytest.fixture()
def mini(tmp_path):
    db = tmp_path / "mini.db"
    con = sqlite3.connect(db)
    con.executescript("""
        CREATE TABLE review_row(
            evidence_id TEXT PRIMARY KEY, source_corpus TEXT, domain TEXT,
            ref_before TEXT, ref_match TEXT, ref_after TEXT,
            w_start INTEGER, w_end INTEGER, matched_letters INTEGER,
            witness_id TEXT);
        CREATE TABLE reference_witness(
            witness_id TEXT PRIMARY KEY, work_id TEXT, raw_id TEXT,
            source_file_id TEXT, w_shift INTEGER, w_is_stream INTEGER);
        CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE facet_row(evidence_id TEXT);
        INSERT INTO reference_witness VALUES
            ('w1','rsX','RS:1.1','sf1',0,0);
    """)
    rows = [
        # (a) the matched text IS scripture, verbatim
        ("ev_verbatim", "rsource", "Midrash / Aggadic Midrashim", "לפני", BIBLE[:40], "אחרי",
         5000, 5040, 40, "w1"),
        # (b) variant quote -- but a citation formula flanks it, and it is short
        ("ev_flank", "rsource", "Midrash / Aggadic Midrashim", "ועליו הכתוב אומר שנאמר",
         "טקסטשאיננומקראכלל" * 3, "ובמקום אחר", 7000, 7030, 51, "w1"),
        # (c) clean: no scripture, no formula, far from every mask interval
        ("ev_clean", "rsource", "Halakhic / Halakhic- Gaonim", "דברי הפוסק עצמו",
         "חידושגמורשלבעלהחיבור" * 2, "וכן עיקר", 9000, 9020, 40, "w1"),
        # (d) another corpus, NON-canonical work: scored since scope v3
        ("ev_other", "sefaria", "Biblical Exegesis / Biblical Exegesis- Rabbanite",
         "", BIBLE[:40], "", 1, 40, 40, None),
        # (d2) a work that IS scripture: exempt, must get NO fact row at all
        ("ev_canon", "sefaria", "Bible: Texts and Translations / Bible: Texts",
         "", BIBLE[:40], "", 1, 40, 40, None),
        # (e) formula at the boundary of a LONG match: the work's own voice --
        # must NOT flag (flank fires only under FLANK_MAX_LETTERS)
        ("ev_long_flank", "rsource", "Halakhic / Halakhic- Gaonim", "ועליו הכתוב אומר שנאמר",
         "טקסטשאיננומקראכלל" * 3, "ובמקום אחר", 11000, 11800, 800, "w1"),
        # (f) most of the span inside a masked quotation interval -> flags
        ("ev_masked", "rsource", "Halakhic / Halakhic- Gaonim", "לפני", "חידושגמורשלבעלהחיבור" * 2,
         "אחרי", 5110, 5170, 60, "w1"),
    ]
    con.executemany("INSERT INTO review_row VALUES (?,?,?,?,?,?,?,?,?,?)", rows)
    con.commit()
    con.close()

    corpus = tmp_path / "corpus.pkl"
    with open(corpus, "wb") as f:
        pickle.dump([{"id": "b1", "cat": "Bible", "stream": BIBLE}], f)
    masks = tmp_path / "masks.json"
    # one interval 60 letters from row (a)'s span; >MASK_NEAR from (b) and (c)
    masks.write_text(json.dumps({"RS:1.1": [[5100, 5200]]}), encoding="utf-8")
    return db, corpus, masks


def test_attach_end_to_end(mini):
    db, corpus, masks = mini
    n, flagged = asf.attach(str(db), str(corpus), str(masks), say=lambda *a: None)
    assert n == 6            # scope v3: ev_other scored, ev_canon exempt
    assert flagged == 4
    con = sqlite3.connect(db)
    got = {r[0]: r for r in con.execute(
        "SELECT evidence_id, bible_share, flank_cite, mask_distance, "
        "mask_overlap, flagged FROM scripture_fact")}
    assert set(got) == {"ev_verbatim", "ev_flank", "ev_clean",
                        "ev_long_flank", "ev_masked", "ev_other"}
    assert got["ev_other"][5] == 1        # verbatim Bible span on an exegesis work
    # the exempt canonical work is the row that proves the scope rule
    assert got["ev_verbatim"][1] == 1.0 and got["ev_verbatim"][5] == 1
    assert got["ev_flank"][1] < 0.5 and got["ev_flank"][2] == 1 \
        and got["ev_flank"][5] == 1
    # the rows that prove the flag can say NO:
    assert got["ev_clean"][5] == 0 and got["ev_clean"][2] == 0
    # ... including a citation at the edge of a LONG match (the work's voice)
    assert got["ev_long_flank"][2] == 1 and got["ev_long_flank"][5] == 0
    # a span 60/60 inside [5100,5200) flags on overlap fraction alone
    assert got["ev_masked"][4] == 1.0 and got["ev_masked"][5] == 1
    # mask distance recorded (60 letters for ev_verbatim's [5000,5040) vs [5100,5200))
    assert got["ev_verbatim"][3] == 60
    assert got["ev_verbatim"][4] == 0.0    # contact-free: overlap 0, not None
    # facet projection dropped, meta written
    assert con.execute("SELECT COUNT(*) FROM sqlite_master "
                       "WHERE name='facet_row'").fetchone()[0] == 0
    meta = dict(con.execute("SELECT key, value FROM meta"))
    assert meta["scripture_fact.rows"] == "6"
    assert meta["scripture_fact.flagged"] == "4"
    assert "doc.scripture_flag" in meta
    con.close()


def test_attach_flag_flips_when_the_flank_gains_a_formula(mini):
    """Mutation: give the clean row a citation formula -> it must flag."""
    db, corpus, masks = mini
    con = sqlite3.connect(db)
    con.execute("UPDATE review_row SET ref_after='וגו'' (מלכים ב כד טז)' "
                "WHERE evidence_id='ev_clean'")
    con.commit()
    con.close()
    _n, flagged = asf.attach(str(db), str(corpus), str(masks),
                             say=lambda *a: None)
    assert flagged == 5


def test_attach_refuses_partial_scoring(mini, monkeypatch):
    """If scoring yields fewer rows than the artifact holds, nothing publishes."""
    db, corpus, masks = mini

    real = asf.compute_rows

    def drop_one(*a, **k):
        rows = list(real(*a, **k))
        return iter(rows[:-1])

    monkeypatch.setattr(asf, "compute_rows", drop_one)
    with pytest.raises(RuntimeError, match="refusing to publish"):
        asf.attach(str(db), str(corpus), str(masks), say=lambda *a: None)
    con = sqlite3.connect(db)
    assert con.execute("SELECT COUNT(*) FROM sqlite_master "
                       "WHERE name='scripture_fact'").fetchone()[0] == 0
    con.close()
