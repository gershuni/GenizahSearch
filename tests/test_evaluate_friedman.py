# -*- coding: utf-8 -*-
"""scripts/evaluate_friedman.py on a synthetic sidecar: every automatic measure can FAIL (a head
key its head does not hold, an index root no entry carries, a shifted dot), the scholar's sheet
carries every stratum with the marked text, and grading refuses an ungraded sheet.

No dictionary text in the repository.
"""
from __future__ import annotations

import csv
import sqlite3

import pytest

from scripts import evaluate_friedman as ev
from scripts.build_friedman_sidecar import write_sidecar
from shared import friedman_service as fs
from shared.friedman_service import FriedmanService

VIEWER = "https://ja.genizah.org/JA_Dictionary_pdfjs/web/viewer.html?file=PDF/"


def unit(uid, kind, head, keys, dotted=(), parent=None, binyan=None, examples=(), letter="א", page="1", pages=1,
         same_page_split=False):
    return {"id": uid, "kind": kind, "letter": letter, "parent": parent, "binyan": binyan,
            "head": None if head is None else {"text": head, "plain": head, "root": head, "root_source": "single_word",
                                                "root_candidate": None, "root_dotted": list(dotted), "lemma": head,
                                                "alternatives": [head], "frames": [], "notes": [], "keys": list(keys)},
            "gloss": {"definition": f"def {uid}", "references": "", "foreign": "", "xrefs": [], "other": "",
                      "locator": ""},
            "examples": [{"quote": q, "translation": t, "locator": loc, "foreign": "", "xrefs": [], "other": ""}
                         for q, t, loc in examples],
            "segments": [], "text": f"text {uid}", "flags": ["no_quotations"] if not examples else [],
            "locations": [{"source_id": "1", "pdf_page": 1 if same_page_split else p, "printed_page": page, "paragraph": p - 1,
                           "bbox": [0, 0, 1, 1], "chars": 5} for p in range(1, (2 if same_page_split else pages) + 1)]}


def units_result():
    return {"recovery_version": 4, "units": [
        unit("f000001", "entry", "אבד", ["אבד"], ["אבד"],
             examples=[("לא אבד מן", "'no escape'", "(ח1, שורה 2)")]),
        unit("f000002", "subentry", "לא אבד", ["לא אבד"], parent="f000001", pages=2,
             examples=[("לא לך אבד מן", "'you cannot escape'", "(ח1, שורה 5)"),
                       ("לא ואבד מנה", "'by a clitic'", "(ח1, שורה 9)"),
                       ("אבדת מנה", "'inflected: missed'", "(ח1, שורה 12)")]),
        unit("f000003", "entry", "ג̇לב", ["גלב"], ["ג̇לב"], letter="ג̇"),
        unit("f000004", "entry", "גלב", ["גלב"], ["גלב"], letter="ג"),
        unit("f000005", "entry", "קלב", ["קלב"], ["קלב"], letter="ק",
             examples=[("קאל אבד לא שך", "'reversed'", "(ח3, שורה 1)")]),
        unit("f000006", "entry", "וَ̇גְה", ["וגה"], ["ו̇גה"], letter="ו"),     # the dot shifted onto ו
        unit("f000007", "entry", "בצט<בסט.", ["בצט בסט"], ["בצט"], letter="ב"),  # a key no head holds
        unit("f000008", "entry", "קטע", ["קטע"], ["קטע"], letter="ק", same_page_split=True),  # split on ONE page
    ]}


def index_result():
    def row(form, *roots):
        return {"form": form, "form_key": fs.normalize_ja(form), "line": form, "pdf_page": 1, "printed_page": "969",
                "url": f"{VIEWER}Value_Keys.pdf#page=1",
                "roots": [{"text": r, "dotted": r, "key": fs.normalize_ja(r), "alternatives": 1} for r in roots]}
    return {"index_version": 1, "rows": [row("אבאד", "אבד"), row("תקלב", "קלב", "קלוב"), row("תוגה", "וג̇ה"),
                                         row("אבריסם")]}


@pytest.fixture
def evaluated(tmp_path, monkeypatch):
    path = tmp_path / "friedman.db"
    write_sidecar(units_result(), index_result(), {"units_sha256": "u", "index_sha256": "i", "manifest_sha256": "m"}, path)
    monkeypatch.setenv(fs.ENV_ENABLED, "1")
    service = FriedmanService(str(path))
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    expr = ev.evaluate_expressions(service, conn)
    rev = ev.evaluate_reversed(service, conn)
    index = ev.evaluate_index(service, conn)
    homographs = ev.evaluate_homographs(service, conn)
    report = ev.summarize(expr, rev, index, homographs, ev.evaluate_conservation(conn))
    yield conn, expr, rev, index, homographs, report
    conn.close()
    service.close()


def test_every_measure_counts_what_the_book_gives_and_can_fail(evaluated):
    conn, expr, rev, index, homographs, report = evaluated
    e = report["expressions"]
    assert e["head_keys"] == 2 and e["head_found"] == 1 and e["head_missed"] == ["בצט בסט @f000007"]
    by_n = {(q["unit"], q["n"]): q for q in expr["quotations"]}
    assert by_n[("f000002", 1)]["first_gap"] == 1 and by_n[("f000002", 1)]["positions"] == [0, 2]
    assert by_n[("f000002", 2)]["first_gap"] == 0 and by_n[("f000002", 2)]["stem"], "ואבד reached from אבד by a clitic"
    assert by_n[("f000002", 3)]["first_gap"] is None and by_n[("f000002", 3)]["positions"] is None
    assert e["own_quotations"] == 3 and e["own_quotations_found_by_gap_cumulative"] == {"0": 1, "1": 2, "2": 2, "3": 2, "4": 2, "5": 2}
    assert e["own_quotations_missed_at_max_gap"] == 1 and e["own_quotations_found_through_a_clitic"] == 1
    assert [(o["other"], o["scope"], o["n"]) for o in expr["others"]] == [("f000001", "quotation", 1)]
    assert e["reversed_two_word_places"] == 1 and rev[0]["other"] == "f000005" and not rev[0]["own"]
    assert e["continued_units"] == {"head_keys": 1, "head_found": 1, "own_quotations": 3, "own_quotations_found": 2}
    assert ev._unit_pages(conn) == {**{f"f00000{i}": 1 for i in range(1, 9)}, "f000002": 2}, "a split on one page is no page crossing"
    lk = report["lookup"]
    assert lk["index_forms_with_a_root"] == 4 and lk["index_found"] == 3
    assert lk["index_missed"] == ["תקלב -> קלוב"]
    hows = {(i["form"], i["root"]): i["how"] for i in index}
    assert hows[("אבאד", "אבד")] == "dotted" and hows[("תוגה", "וג̇ה")] == "plain", "the shifted dot leaves only the letters"
    assert lk["index_ambiguous_forms"] == 1 and lk["index_ambiguous_roots_found"] == 1
    assert lk["homograph_keys"] == 1 and homographs[0]["entries"] == ["f000003", "f000004"] and homographs[0]["complete"]
    c = report["conservation"]
    assert c["heads"]["dot_shifted"] == 1 and c["unit_roots"]["dot_shifted"] == 1 and c["index_roots"]["dot_shifted"] == 0
    assert c["index_dotted_roots_with_a_unit_root"] == 2   # אבד, קלב; וג̇ה is not ו̇גה
    text = ev.render_report(report)
    assert "found in their own head: 1" in text and "בצט בסט @f000007" in text and "shifted in 1" in text


def test_shifted_dots_names_only_a_dot_on_the_wrong_letter():
    assert ev.shifted_dots("ג̇לב") == 0 and ev.shifted_dots("וَ̇גְה") == 1 and ev.shifted_dots("לִאַ̇גְלִ דַ̇לִךְ") == 1
    assert ev.shifted_dots("א̇בד") == 0, "a dot on א before ב is odd but not a shift onto a dotted letter"
    assert ev.shifted_dots("נ̇ע̇") == 0 and ev.shifted_dots("י̇ג̇ דינאר") == 0, "an abbreviation or a numeral dots every letter"
    assert ev.shifted_dots("מעארץ̇ה") == 0 and ev.shifted_dots("שיך̇") == 0, "final ץ and ך take the dot"
    assert ev.shifted_dots("נ̇ג") == 1, "but a dot on נ before an undotted ג has moved"
    assert ev.shifted_dots("א̇ע") == 0, "ע takes no dot in this book (plain ג is غ)"


def test_the_sheet_carries_every_stratum_with_the_marked_text(evaluated, tmp_path):
    conn, expr, rev, index, homographs, report = evaluated
    rows = ev.build_sheet(conn, expr, rev, index, homographs, per_stratum=12, seed=1)
    strata = {r["stratum"] for r in rows}
    assert strata == set(ev.STRATA), "every stratum has a case"
    assert [r["case"] for r in rows] == [f"c{i:03d}" for i in range(1, len(rows) + 1)]
    disc = next(r for r in rows if r["stratum"] == "discontinuous")
    assert disc["text"].startswith("[[לא]] לך [[אבד]] מן") and disc["components"] == "לא <- לא (word); אבד <- אבד (word)"
    assert disc["where"] == "quotation 1" and disc["parent_head"] == "אבד" and disc["link"] == f"{VIEWER}1.pdf#page=1 ; {VIEWER}1.pdf#page=2"
    clitic = next(r for r in rows if r["stratum"] == "clitic")
    assert clitic["components"] == "לא <- לא (word); אבד <- ואבד (stem)" and "[[ואבד]]" in clitic["text"]
    missed_q = next(r for r in rows if r["stratum"] == "quotation_missed")
    assert missed_q["text"].startswith("אבדת מנה") and missed_q["components"] == "" and missed_q["gap"] == ev.MAX_GAP
    other = next(r for r in rows if r["stratum"] == "other_unit")
    assert other["unit"] == "f000001" and other["note"] == "searched from the head of f000002"
    reversed_row = next(r for r in rows if r["stratum"] == "reversed")
    assert reversed_row["search"] == "אבד לא" and "[[אבד]] [[לא]]" in reversed_row["text"]
    missed = next(r for r in rows if r["stratum"] == "index_missed")
    assert missed["search"] == "תקלב" and missed["head"] == "קלוב"
    homograph = next(r for r in rows if r["stratum"] == "homograph")
    assert "f000003 ג̇לב" in homograph["text"] and "MISSING" not in homograph["text"]
    assert set(rows[0]) == set(ev.SHEET_COLUMNS) and all(r["verdict"] == "" for r in rows)
    path = tmp_path / "sheet.csv"
    ev.write_sheet(rows, path)
    assert path.read_bytes().startswith(b"\xef\xbb\xbf"), "Excel reads the Hebrew only with the BOM"
    assert ev.read_sheet(path) == [{k: str(v) for k, v in r.items()} for r in rows]
    guide = ev.render_guide(rows, report)
    assert "| other_unit | 1 |" in guide and "inflection" in guide


def test_grading_refuses_an_ungraded_sheet_and_rates_a_graded_one(evaluated, tmp_path):
    conn, expr, rev, index, homographs, report = evaluated
    rows = ev.build_sheet(conn, expr, rev, index, homographs, per_stratum=12, seed=1)
    ungraded = ev.grade_sheet(rows, report)
    assert len(ungraded["problems"]) == len(rows) and all("no verdict" in p for p in ungraded["problems"])
    for r in rows:
        r["verdict"] = {"other_unit": "false", "index_missed": "not_there", "head_missed": "extraction",
                        "quotation_missed": "inflection"}.get(r["stratum"], "correct")
    rows[next(i for i, r in enumerate(rows) if r["stratum"] == "discontinuous")]["verdict"] = "Correct "   # case does not matter
    graded = ev.grade_sheet(rows, report)
    assert graded["problems"] == []
    assert graded["expressions"]["false_among_other_unit_places"]["rate"] == 1.0
    assert graded["expressions"]["projected_false_other_unit_places"] == 1.0
    assert graded["expressions"]["missed_among_quotations_not_found"]["rate"] == 1.0
    assert graded["expressions"]["projected_missed_quotations"] == 1.0
    assert graded["expressions"]["projected_missed_rate_of_own_quotations"] == round(1 / 3, 4)
    assert graded["lookup"]["missed_among_index_not_reached"]["rate"] == 0.0
    assert graded["lookup"]["projected_missed_rate_of_index_forms"] == 0.0
    assert graded["expressions"]["head_missed_real"] == {"graded": 1, "unsure": 0, "bad": 1, "rate": 1.0, "low": 0.207, "high": 1.0}
    rows[1]["verdict"] = "maybe"
    assert ev.grade_sheet(rows, report)["problems"] == [f"{rows[1]['case']}: verdict 'maybe' is not one of {ev.STRATA[rows[1]['stratum']][1]}"]
    path = tmp_path / "sheet.csv"
    ev.write_sheet(rows, path)
    assert ev.main(["grade", "--sheet", str(path)]) == 2
    with path.open("r", encoding="utf-8-sig", newline="") as fh:
        assert len(list(csv.DictReader(fh))) == len(rows)
