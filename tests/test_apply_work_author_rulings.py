# -*- coding: utf-8 -*-
"""Gates for scripts/apply_work_author_rulings.py."""
import csv
import os
import sqlite3
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
from apply_work_author_rulings import (  # noqa: E402
    GateError, apply, normalize_hebrew_quotes)
from export_work_authors import COLUMNS, export, flag_of  # noqa: E402

ROWS = [
    ("e1", "w1", "פרקי רבי אליעזר", "מיוחס לר' אליעזר (המאה ה-9)", None,
     "msource", None),
    ("e2", "w1", "פרקי רבי אליעזר", "מיוחס לר' אליעזר (המאה ה-9)", None,
     "msource", None),
    ("e3", "w2", "משנת רבי אליעזר", 'אנונימי (מדרש של"ב מידות)', None,
     "sefaria", None),
    ("e4", "w3", "ספר כלשהו", "פלוני", "author_authority", "sefaria", None),
]


def make_db(path, rows=ROWS):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE review_row(evidence_id TEXT PRIMARY KEY, "
                "work_id TEXT, work_title TEXT, work_author TEXT, "
                "author_provenance TEXT, source_corpus TEXT, "
                "title_provenance TEXT)")
    con.execute("CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT)")
    con.execute("CREATE TABLE facet_row(evidence_id TEXT)")
    con.executemany("INSERT INTO review_row VALUES (?,?,?,?,?,?,?)", rows)
    con.commit()
    con.close()
    return path


def state(path):
    con = sqlite3.connect(path)
    got = {w: (t, a, ap, tp) for w, t, a, ap, tp in con.execute(
        "SELECT work_id, MIN(work_title), MIN(work_author), "
        "MIN(author_provenance), MIN(title_provenance) FROM review_row "
        "GROUP BY work_id")}
    ruled = {}
    try:
        ruled = {(w, f): (o, n) for w, f, o, n in con.execute(
            "SELECT work_id, field, old_value, new_value FROM "
            "work_author_ruling")}
    except sqlite3.OperationalError:
        pass
    con.close()
    return got, ruled


def test_drop_author_from_code_rulings(tmp_path):
    db = make_db(str(tmp_path / "a.db"))
    n_auth, n_title = apply(db, rulings={"w1": dict(drop_author=True,
                                                    note="not an author")},
                            say=lambda *a: None)
    got, ruled = state(db)
    assert (n_auth, n_title) == (1, 0)
    assert got["w1"][1:3] == (None, None)
    assert got["w3"][1] == "פלוני"                      # untouched
    assert ruled[("w1", "author")][0].startswith("מיוחס")   # what it WAS
    assert ruled[("w1", "author")][1] is None


def test_rerun_changes_nothing(tmp_path):
    db = make_db(str(tmp_path / "b.db"))
    r = {"w1": dict(drop_author=True)}
    apply(db, rulings=r, say=lambda *a: None)
    assert apply(db, rulings=r, say=lambda *a: None) == (0, 0)


def test_new_author_and_title(tmp_path):
    db = make_db(str(tmp_path / "c.db"))
    apply(db, rulings={"w2": dict(author="אנונימי", title="משנת רבי אליעזר "
                                  '(מדרש ל"ב מידות)')},
          say=lambda *a: None)
    got, ruled = state(db)
    assert got["w2"][0].endswith('ל"ב מידות)')
    assert got["w2"][2] == "owner_ruling" and got["w2"][3] == "owner_ruling"
    assert ruled[("w2", "title")][0] == "משנת רבי אליעזר"


def test_unknown_work_refuses(tmp_path):
    db = make_db(str(tmp_path / "d.db"))
    with pytest.raises(GateError, match="not in this db"):
        apply(db, rulings={"wGhost": dict(drop_author=True)},
              say=lambda *a: None)


LEGACY_COLUMNS = ("work_id", "source_corpus", "evidence_rows", "work_title",
                  "work_author", "author_provenance", "title_provenance",
                  "kw_id", "kw_title", "kw_author", "kw_author_basis", "FLAG",
                  "DROP_AUTHOR", "NEW_AUTHOR", "NEW_TITLE", "NOTE")


def _csv(path, rows, columns=COLUMNS):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in columns})
    return path


def _exported(db, out):
    export(db, out, say=lambda *a: None)
    return list(csv.DictReader(open(out, encoding="utf-8-sig")))


def test_in_place_edit_changes_and_clears(tmp_path):
    """The owner edits work_author itself: new text = change, emptied = drop."""
    db = make_db(str(tmp_path / "e.db"))
    out = str(tmp_path / "wa.csv")
    rows = _exported(db, out)
    for r in rows:
        if r["work_id"] == "w1":
            r["work_author"] = ""                     # clear
        if r["work_id"] == "w2":
            r["work_author"] = "אנונימי"              # change
    _csv(out, rows)
    assert apply(db, csv_path=out, say=lambda *a: None) == (2, 0)
    got, ruled = state(db)
    assert got["w1"][1] is None and got["w2"][1] == "אנונימי"
    assert ruled[("w1", "author")][1] is None
    assert ruled[("w2", "author")][1] == "אנונימי"
    assert got["w3"][1] == "פלוני"                    # untouched row untouched


def test_in_place_title_edit(tmp_path):
    db = make_db(str(tmp_path / "f.db"))
    out = str(tmp_path / "wa.csv")
    rows = _exported(db, out)
    for r in rows:
        if r["work_id"] == "w2":
            r["work_title"] = 'משנת רבי אליעזר (מדרש ל"ב מידות)'
    _csv(out, rows)
    assert apply(db, csv_path=out, say=lambda *a: None) == (0, 1)
    got, ruled = state(db)
    # gershayim, not the ASCII quote typed into the cell
    assert got["w2"][0].endswith("ל״ב מידות)") and got["w2"][3] == "owner_ruling"
    assert ruled[("w2", "title")][0] == "משנת רבי אליעזר"


def test_emptied_title_refuses(tmp_path):
    """A cleared author means "no author"; a cleared title means nothing."""
    db = make_db(str(tmp_path / "g.db"))
    out = str(tmp_path / "wa.csv")
    rows = _exported(db, out)
    for r in rows:
        if r["work_id"] == "w2":
            r["work_title"] = ""
    _csv(out, rows)
    with pytest.raises(GateError, match="must keep a title"):
        apply(db, csv_path=out, say=lambda *a: None)


def test_untouched_file_is_a_no_op(tmp_path):
    db = make_db(str(tmp_path / "h.db"))
    out = str(tmp_path / "wa.csv")
    _csv(out, _exported(db, out))
    assert apply(db, csv_path=out, say=lambda *a: None) == (0, 0)


def test_deleted_rows_are_not_deletions(tmp_path):
    """Removing a row from the sheet must not remove anything from the db."""
    db = make_db(str(tmp_path / "i.db"))
    out = str(tmp_path / "wa.csv")
    rows = [r for r in _exported(db, out) if r["work_id"] != "w1"]
    _csv(out, rows)
    assert apply(db, csv_path=out, say=lambda *a: None) == (0, 0)
    got, _ = state(db)
    assert got["w1"][1].startswith("מיוחס")


def test_stale_baseline_refuses(tmp_path):
    """The correction was made against an author the db no longer carries."""
    db = make_db(str(tmp_path / "j.db"))
    out = str(tmp_path / "wa.csv")
    rows = _exported(db, out)
    for r in rows:
        if r["work_id"] == "w1":
            r["work_author"] = ""
    _csv(out, rows)
    apply(db, rulings={"w1": dict(author="מישהו אחר")}, say=lambda *a: None)
    with pytest.raises(GateError, match="no longer carries"):
        apply(db, csv_path=out, say=lambda *a: None)


def test_editing_kw_author_refuses(tmp_path):
    """The identity's author is derived; editing it there would do nothing, so
    the applier says so instead of ignoring the edit."""
    db = make_db(str(tmp_path / "k.db"))
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE known_work(kw_id TEXT PRIMARY KEY, title TEXT, "
                "author TEXT, author_basis TEXT)")
    con.execute("CREATE TABLE known_work_member(kw_id TEXT, work_id TEXT)")
    con.execute("INSERT INTO known_work VALUES "
                "('kw1','ספר כלשהו','פלוני','authority')")
    con.execute("INSERT INTO known_work_member VALUES ('kw1','w3')")
    con.commit()
    con.close()
    out = str(tmp_path / "wa.csv")
    rows = _exported(db, out)
    for r in rows:
        if r["work_id"] == "w3":
            r["kw_author"] = "אלמוני"
            r["work_author"] = "אלמוני"      # a real edit, so the row is read
    _csv(out, rows)
    with pytest.raises(GateError, match="edited kw_author"):
        apply(db, csv_path=out, say=lambda *a: None)


def test_legacy_columns_still_apply(tmp_path):
    """A file exported before in-place editing must still work."""
    db = make_db(str(tmp_path / "l.db"))
    out = str(tmp_path / "wa.csv")
    rows = _exported(db, out)
    for r in rows:
        if r["work_id"] == "w1":
            r["DROP_AUTHOR"] = "x"
    _csv(out, rows, columns=LEGACY_COLUMNS)
    assert apply(db, csv_path=out, say=lambda *a: None) == (1, 0)
    got, _ = state(db)
    assert got["w1"][1] is None


def test_mixing_both_mechanisms_refuses(tmp_path):
    db = make_db(str(tmp_path / "m.db"))
    out = str(tmp_path / "wa.csv")
    rows = _exported(db, out)
    for r in rows:
        if r["work_id"] == "w1":
            r["work_author"] = "פלוני חדש"
            r["DROP_AUTHOR"] = "x"
    _csv(out, rows, columns=LEGACY_COLUMNS + ("ORIG_TITLE", "ORIG_AUTHOR"))
    with pytest.raises(GateError, match="pick one way"):
        apply(db, csv_path=out, say=lambda *a: None)


def test_export_flags_are_not_noisy():
    """The ruled canonical form 'full name (acronym)' must NOT be flagged --
    249 works use it, and flagging them would bury the real cases."""
    assert flag_of("משנה תורה לרמב״ם", "משה בן מימון (רמב״ם)") == ""
    assert flag_of("רש״י", "שלמה בן יצחק (רש״י)") == ""
    assert flag_of("משנת רבי אליעזר", 'אנונימי (מדרש של"ב מידות)') == "anonymous"
    assert flag_of("פרקי רבי אליעזר",
                   "מיוחס לר' אליעזר (המאה ה-9)") == "attribution_gloss"
    assert flag_of("נסים גאון, חמשה ספרים", "נסים גאון") == "author_in_title"
    assert flag_of("האמונות והדעות",
                   "סעדיה גאון (רס״ג); תרגום יהודה אבן תיבון") == "compound_credit"
    assert flag_of("תשובות על דונש",
                   "תלמידי מנחם בן סרוק") == "relation_to_person"
    assert flag_of("ספר כלשהו", "") == ""


def test_ascii_quotes_normalize_to_gershayim():
    """The owner types ראב"ש on a keyboard; the corpus spells it ראב״ש, and the
    author authority's whole point is one string per person."""
    n = normalize_hebrew_quotes
    assert n('אברהם בן שלמה (ראב"ש)') == "אברהם בן שלמה (ראב״ש)"
    assert n('רמב"ם') == "רמב״ם"
    assert n("ר' יוסף") == "ר׳ יוסף"
    assert n('פירוש לעשרת הדיברות(?)') == "פירוש לעשרת הדיברות(?)"
    # a quote NOT between Hebrew letters is left alone
    assert n('X"Y') == 'X"Y'
    assert n('"פתיחה') == '"פתיחה'
    assert n("") == "" and n(None) is None


def test_edited_cell_is_normalized_before_storing(tmp_path):
    db = make_db(str(tmp_path / "n.db"))
    out = str(tmp_path / "wa.csv")
    rows = _exported(db, out)
    for r in rows:
        if r["work_id"] == "w2":
            r["work_author"] = 'אברהם בן שלמה (ראב"ש)'
    _csv(out, rows)
    apply(db, csv_path=out, say=lambda *a: None)
    got, _ = state(db)
    assert got["w2"][1] == "אברהם בן שלמה (ראב״ש)"


def test_stored_ascii_quotes_do_not_read_as_edits(tmp_path):
    """A db value that itself uses ASCII quotes must not look edited on every
    cycle -- otherwise the loop invents changes nobody made."""
    db = make_db(str(tmp_path / "q.db"),
                 rows=[("e1", "wq", 'ספר של"ב', 'פלוני (רמב"ם)', None,
                        "sefaria", None)])
    out = str(tmp_path / "wa.csv")
    _csv(out, _exported(db, out))
    assert apply(db, csv_path=out, say=lambda *a: None) == (0, 0)
    got, _ = state(db)
    assert got["wq"][1] == 'פלוני (רמב"ם)'      # left exactly as it was


ISSUE_COLUMNS = ("kind", "detail", "work_id", "work_title", "work_author",
                 "source_corpus", "evidence_rows", "kw_id", "kw_title")


def _issues(path, rows):
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=ISSUE_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in ISSUE_COLUMNS})
    return path


def test_issues_report_can_be_filled_in(tmp_path):
    """The report was meant to be read-only; the owner filled authors into it,
    which is reasonable -- it IS the list of what is missing. The db is the
    baseline there."""
    db = make_db(str(tmp_path / "i1.db"),
                 rows=[("e1", "wm", "ספר בלי מחבר", None, None, "sefaria",
                        None),
                       ("e2", "wk", "ספר אחר", "פלוני", None, "sefaria", None)])
    out = _issues(str(tmp_path / "iss.csv"), [
        dict(kind="missing_author", work_id="wm", work_title="ספר בלי מחבר",
             work_author='אברהם בן שלמה (ראב"ש)'),
        dict(kind="duplicate_person", work_id="wk", work_title="ספר אחר",
             work_author="פלוני"),                    # untouched
    ])
    assert apply(db, csv_path=out, say=lambda *a: None) == (1, 0)
    got, ruled = state(db)
    assert got["wm"][1] == "אברהם בן שלמה (ראב״ש)"     # normalized on store
    assert got["wk"][1] == "פלוני"                     # untouched stays put
    assert ruled[("wm", "author")] == (None, "אברהם בן שלמה (ראב״ש)")


def test_issues_report_untouched_is_a_no_op(tmp_path):
    """Including the ascii_quotes rows: normalization must not turn a row the
    owner never touched into an edit."""
    db = make_db(str(tmp_path / "i2.db"),
                 rows=[("e1", "wq", "ספר", 'פלוני (רמ"ה)', None, "sefaria",
                        None)])
    out = _issues(str(tmp_path / "iss.csv"), [
        dict(kind="ascii_quotes", work_id="wq", work_title="ספר",
             work_author='פלוני (רמ"ה)')])
    assert apply(db, csv_path=out, say=lambda *a: None) == (0, 0)
    got, _ = state(db)
    assert got["wq"][1] == 'פלוני (רמ"ה)'


def test_issues_retyped_with_gershayim_is_applied(tmp_path):
    """...but retyping the SAME name with gershayim is a real correction and
    must not be swallowed."""
    db = make_db(str(tmp_path / "i3.db"),
                 rows=[("e1", "wq", "ספר", 'פלוני (רמ"ה)', None, "sefaria",
                        None)])
    out = _issues(str(tmp_path / "iss.csv"), [
        dict(kind="ascii_quotes", work_id="wq", work_title="ספר",
             work_author="פלוני (רמ״ה)")])
    assert apply(db, csv_path=out, say=lambda *a: None) == (1, 0)
    got, _ = state(db)
    assert got["wq"][1] == "פלוני (רמ״ה)"


def test_issues_contradiction_refuses(tmp_path):
    """A work appears once per kind, so two rows can disagree."""
    db = make_db(str(tmp_path / "i4.db"),
                 rows=[("e1", "wk", "ספר", "פלוני", None, "sefaria", None)])
    out = _issues(str(tmp_path / "iss.csv"), [
        dict(kind="duplicate_person", work_id="wk", work_title="ספר",
             work_author="אלמוני"),
        dict(kind="ascii_quotes", work_id="wk", work_title="ספר",
             work_author="פלמוני")])
    with pytest.raises(GateError, match="appears twice in the report"):
        apply(db, csv_path=out, say=lambda *a: None)


def test_issues_drifted_row_refuses(tmp_path):
    """The report has no baseline of its own, so the row must still describe
    the work it names."""
    db = make_db(str(tmp_path / "i5.db"),
                 rows=[("e1", "wk", "הכותרת החדשה", None, None, "sefaria",
                        None)])
    out = _issues(str(tmp_path / "iss.csv"), [
        dict(kind="missing_author", work_id="wk",
             work_title="הכותרת הישנה", work_author="פלוני")])
    with pytest.raises(GateError, match="no longer describe the work"):
        apply(db, csv_path=out, say=lambda *a: None)
