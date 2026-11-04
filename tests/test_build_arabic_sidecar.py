# -*- coding: utf-8 -*-
"""scripts/build_arabic_sidecar.py and scripts/verify_arabic_sidecar.py on a synthetic source.

The build writes every table from pinned sources and refuses what it cannot vouch for (a file
that differs from its pin, a credit line that is not Perseus's, a count that disagrees with the
source's manifest, an OpenITI manifest that is not D7's whole selection, a closed-class row Lane
cannot answer, a smaller sidecar over a larger one);
the gate passes the file it wrote and fails each mutation with a sentence that says what changed.
No dictionary or corpus text is in the repository: the entries below are made up.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

import scripts.build_arabic_sidecar as build
import scripts.verify_arabic_sidecar as gate
from shared.arabic_normalize import root_key

SHARDS = 4
#: A made-up OpenITI metadata table: two texts D7 selects, one too late and one dictionary.
METADATA_TSV = "\t".join(("version_uri", "date", "language", "status", "uncorrected_OCR", "tags", "local_path",
                           "tok_length")) + "\n" + "".join("\t".join(row) + "\n" for row in (
    ("0300Test.A.Text-ara1", "300", "ara", "pri", "False", "_HADITH", "data/x/a", "10"),
    ("0400Test.B.Text-ara1", "400", "ara", "pri", "False", "_TARIKH", "data/x/b", "12"),
    ("0700Test.C.Text-ara1", "700", "ara", "pri", "False", "_TARIKH", "data/x/c", "5"),
    ("0350Test.D.Text-ara1", "350", "ara", "pri", "False", "_MACAJIM", "data/x/d", "5"),
))
METADATA_SHA256 = hashlib.sha256(METADATA_TSV.encode("utf-8")).hexdigest()
TEXTS = {"a": "0300Test.A.Text-ara1", "b": "0400Test.B.Text-ara1", "c": "0700Test.C.Text-ara1"}
ENTRIES = [
    # id, headword, root, page
    ("lane:t:1", "كَتَبَ", "كتب", "1"),
    ("lane:t:2", "كِتَابٌ", "كتب", "2"),
    ("lane:t:3", "مِنْ", "من", "3"),
    ("lane:t:4", "لِ", "ل", "4"),
    ("lane:t:5", "ذَا", "ذا", "5"),
    ("lane:t:6", "أَخَذَ", "أخذ", "6"),
    ("lane:t:7", "عَلَى", "علو", "7"),
    ("lane:t:8", "مَا كَانَ", "كون", "8"),
    # Headings that join spellings: one root written two ways, and two roots (filed under the first).
    ("lane:t:9", "سَعَى", "سعو & سعى", "9"),
    ("lane:t:10", "ثَنْدَأ", "ثند & ثندأ", "10"),
]


def _write(path: Path, data) -> bytes:
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def make_source(root: Path, credit: str = build.PERSEUS_CREDIT, count: int = len(ENTRIES),
                counted: tuple[str, ...] = ("a", "b"), unavailable: tuple[str, ...] = ()) -> Path:
    emeiri = root / "emeiri"
    pins = {}
    manifest = {"built_at": "2026-11-01T00:00:00Z", "counts": {"lane": count},
                "sources": {"lane": {"credit": credit, "statement": build.PERSEUS_AVAILABILITY,
                                     "edition": "Lane (test)"}}}
    raw = _write(emeiri / "manifest.json", manifest)
    pins["manifest.json"] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    shards: list[dict] = [{} for _ in range(SHARDS)]
    for n, (eid, headword, rootname, page) in enumerate(ENTRIES):
        shards[n % SHARDS][eid] = {"id": eid, "source": "lane", "headword": headword, "root": rootname,
                                   "gloss": f"{headword} a made-up article {n}", "pages": [page], "_doc": 100 + n}
    shards[0]["lisan:x"] = {"id": "lisan:x", "source": "lisan", "headword": "x", "root": "x", "gloss": "x"}
    for i, shard in enumerate(shards):
        raw = _write(emeiri / "entries" / f"{i}.json", shard)
        pins[f"entries/{i}.json"] = {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
    _write(emeiri / "pins.json", pins)
    openiti = root / "openiti"
    texts = []
    bodies = {"a": "#META# x\nقال ذلك الكتاب والكتاب ذلك أخذ أخذ أخذ\n",
              "b": "ذلك ذلك لنا لنا من من على على كتب كتب دلك\n",
              "c": "كتب كتب كتب\n"}
    openiti.mkdir(parents=True, exist_ok=True)
    (openiti / "metadata.tsv").write_bytes(METADATA_TSV.encode("utf-8"))
    for name in counted:
        raw = bodies[name].encode("utf-8")
        (openiti / f"{name}.txt").write_bytes(raw)
        texts.append({"version_uri": TEXTS[name], "path": f"data/x/{name}",
                      "sha256": hashlib.sha256(raw).hexdigest()})
    _write(openiti / "manifest.json", {"texts": texts, "unavailable": [TEXTS[n] for n in unavailable]})
    return root


def make_closed_class(path: Path, forms=None) -> Path:
    forms = forms if forms is not None else [
        {"form": "لنا", "root": "ل", "headword": "لِ", "note": "li- + -nā"},
        {"form": "منه", "root": "من", "note": "min + -hu"},
        {"form": "ذلك", "root": "ذا", "headword": "ذَا", "note": "dhālika"},
        {"form": "على", "root": "علو", "note": "ʿalā"},
    ]
    _write(path, {"forms": forms})
    return path


@pytest.fixture
def small(monkeypatch):
    monkeypatch.setattr(build, "EMEIRI_SHARDS", SHARDS)
    monkeypatch.setattr(build, "OPENITI_METADATA_SHA256", METADATA_SHA256)
    monkeypatch.setattr(gate, "MIN_ROOTS", 1)
    monkeypatch.setattr(gate, "MIN_FORMS", 1)
    monkeypatch.setattr(gate, "MIN_FREQ_TOKENS", 1)
    monkeypatch.setattr(gate, "MIN_CLOSED_CLASS", 1)


def run_build(tmp_path: Path, *extra: str, source: Path | None = None, closed: Path | None = None) -> int:
    source = source or make_source(tmp_path / "src")
    closed = closed or make_closed_class(tmp_path / "closed.json")
    return build.main(["build", "--source-dir", str(source), "--out", str(tmp_path / "arabic.db"),
                       "--closed-class", str(closed), "--workers", "1", *extra])


def test_the_build_writes_every_table_and_the_gate_passes_it(tmp_path, small):
    assert run_build(tmp_path) == build.EXIT_OK
    db = tmp_path / "arabic.db"
    assert gate.verify(str(db), len(ENTRIES)) == []
    con = sqlite3.connect(db)
    try:
        meta = dict(con.execute("SELECT key, value FROM meta"))
        assert meta["lane_credit"] == build.PERSEUS_CREDIT and meta["rows_lane_entries"] == str(len(ENTRIES))
        assert con.execute("SELECT COUNT(*) FROM lane_entries WHERE id LIKE 'lisan:%'").fetchone()[0] == 0
        assert con.execute("SELECT root_key FROM lane_forms WHERE skeleton = 'كتاب'").fetchone()[0] == "كتب"
        assert con.execute("SELECT COUNT(*) FROM lane_forms WHERE entry_id = 'lane:t:8'").fetchone()[0] == 0, \
            "a several-word headword is not a form"
        assert con.execute("SELECT count, surface FROM freq WHERE skeleton = 'اخذ'").fetchone() == (3, "أخذ")
        assert con.execute("SELECT count FROM freq WHERE skeleton = 'ذلك'").fetchone()[0] == 4
        assert con.execute("SELECT COUNT(*) FROM freq WHERE skeleton = 'دلك'").fetchone()[0] == 0, "count 1 dropped"
        assert con.execute("SELECT entry_id FROM closed_class WHERE skeleton = 'لنا'").fetchone()[0] == "lane:t:4"
        # A joined heading files under its root, never under the letters run together.
        assert con.execute("SELECT root_key FROM lane_entries WHERE id = 'lane:t:9'").fetchone()[0] == root_key("سعو")
        assert json.loads(con.execute("SELECT names_json FROM lane_roots WHERE root_key = ?",
                                      (root_key("سعو"),)).fetchone()[0]) == ["سعو", "سعى"]
        assert con.execute("SELECT root, root_key FROM lane_entries WHERE id = 'lane:t:10'").fetchone() == \
            ("ثند & ثندأ", "ثند"), "the heading is kept whole"
        assert meta["lane_entries_heading_names_two_roots"] == "1"
    finally:
        con.close()


def test_root_spellings_split_a_joined_heading_and_keep_a_plain_one():
    assert build.root_spellings("سعو & سعى") == ["سعو", "سعى"]
    assert build.root_spellings("غو | غوى") == ["غو", "غوى"]
    assert build.root_spellings("(دمو or دمى)") == ["دمو", "دمى"]
    assert build.root_spellings("جبو & جبى [The words of the former are few.]") == ["جبو", "جبى"]
    assert build.root_spellings("سفر جل") == ["سفرجل"], "one word written with a space"
    assert build.root_spellings("كَتَبَ") == ["كتب"]
    assert build.heading_root_key("ثند & ثندأ") == ("ثند", ["ثند"])
    assert build.heading_root_key("[a note]") == ("", [])


def test_a_file_that_differs_from_its_pin_is_refused(tmp_path, small):
    source = make_source(tmp_path / "src")
    (source / "emeiri" / "entries" / "1.json").write_text("{}", encoding="utf-8")
    assert run_build(tmp_path, source=source) == build.EXIT_SOURCE
    assert not (tmp_path / "arabic.db").exists()


def test_a_credit_line_that_is_not_perseus_is_refused(tmp_path, small):
    source = make_source(tmp_path / "src", credit="Text provided by someone else.")
    assert run_build(tmp_path, source=source) == build.EXIT_SOURCE


def test_a_count_that_disagrees_with_the_manifest_is_refused(tmp_path, small):
    source = make_source(tmp_path / "src", count=len(ENTRIES) + 1)
    assert run_build(tmp_path, source=source) == build.EXIT_SOURCE


def test_a_closed_class_row_lane_cannot_answer_is_refused(tmp_path, small):
    bad_root = make_closed_class(tmp_path / "c1.json", [{"form": "لنا", "root": "قرط", "note": "x"}])
    assert run_build(tmp_path, closed=bad_root) == build.EXIT_SOURCE
    bad_headword = make_closed_class(tmp_path / "c2.json",
                                     [{"form": "لنا", "root": "ل", "headword": "لَنْ", "note": "x"}])
    assert run_build(tmp_path, closed=bad_headword) == build.EXIT_SOURCE


def test_a_smaller_build_never_replaces_a_larger_sidecar_without_force(tmp_path, small):
    assert run_build(tmp_path) == build.EXIT_OK
    con = sqlite3.connect(tmp_path / "arabic.db")
    con.execute("INSERT INTO freq VALUES ('زززز', 9, 'زززز')")
    con.commit()
    con.close()
    assert run_build(tmp_path) == build.EXIT_REFUSED
    assert run_build(tmp_path, "--force") == build.EXIT_OK


def _mutate(db: Path, sql: str) -> None:
    con = sqlite3.connect(db)
    con.execute(sql)
    con.commit()
    con.close()


@pytest.mark.parametrize("sql, expect", [
    ("DELETE FROM lane_forms WHERE entry_id = 'lane:t:1'; ", "rows_lane_forms"),
    ("UPDATE lane_forms SET skeleton = 'كتابة' WHERE skeleton = 'كتاب'", "lane_forms keys differ"),
    ("UPDATE lane_entries SET root_key = 'كتب' WHERE id = 'lane:t:6'", "root_key differs"),
    ("UPDATE freq SET skeleton = 'اخد' WHERE skeleton = 'اخذ'", "freq rows whose key differs"),
    ("UPDATE meta SET value = 'Text provided by someone.' WHERE key = 'lane_credit'", "Perseus credit line"),
    ("UPDATE meta SET value = '99' WHERE key = 'normalizer_version'", "normalizer_version"),
    ("UPDATE closed_class SET skeleton = 'لكم' WHERE form = 'لنا'", "closed_class keys differ"),
    ("UPDATE freq SET count = count + 1 WHERE skeleton = 'ذلك'", "does not add up to meta.freq_tokens"),
    ("UPDATE meta SET value = '1' WHERE key = 'freq_texts'", "selected"),
    ("UPDATE lane_roots SET names_json = '[\"كتب\", \"(كتب)\"]' WHERE name = 'كتب'", "not plain letters"),
    ("UPDATE lane_entries SET root_key = 'سعWسعW' WHERE id = 'lane:t:9'", "root_key differs"),
])
def test_the_gate_fails_each_mutation(tmp_path, small, sql, expect):
    assert run_build(tmp_path) == build.EXIT_OK
    db = tmp_path / "arabic.db"
    _mutate(db, sql)
    problems = gate.verify(str(db), len(ENTRIES))
    assert any(expect in p for p in problems), problems


def _refused(tmp_path: Path, capsys, source: Path, words: str) -> None:
    assert run_build(tmp_path, source=source) == build.EXIT_SOURCE
    out = capsys.readouterr().out
    assert words in out, out
    assert not (tmp_path / "arabic.db").exists()


def test_a_manifest_that_lost_a_selected_text_is_refused(tmp_path, small, capsys):
    """However many tokens the rest hold: the count must be D7's whole selection."""
    _refused(tmp_path, capsys, make_source(tmp_path / "src", counted=("a",)), "neither counted nor listed")


def test_a_text_outside_the_selection_is_refused(tmp_path, small, capsys):
    _refused(tmp_path, capsys, make_source(tmp_path / "src", counted=("a", "b", "c")), "outside the D7 selection")


def test_a_text_named_twice_is_refused(tmp_path, small, capsys):
    _refused(tmp_path, capsys, make_source(tmp_path / "src", counted=("a", "b"), unavailable=("b",)), "twice")


def test_a_metadata_table_that_differs_from_its_pin_is_refused(tmp_path, small, capsys):
    source = make_source(tmp_path / "src")
    (source / "openiti" / "metadata.tsv").write_text(METADATA_TSV.replace("_TARIKH", "_ADAB", 1), encoding="utf-8")
    _refused(tmp_path, capsys, source, "differs from the pinned release")


def test_unavailable_texts_are_recorded_and_capped(tmp_path, small, capsys, monkeypatch):
    source = make_source(tmp_path / "src", counted=("a",), unavailable=("b",))
    _refused(tmp_path, capsys, source, "unavailable, more than")
    monkeypatch.setattr(build, "MAX_UNAVAILABLE_SHARE", 0.5)
    monkeypatch.setattr(gate, "MAX_UNAVAILABLE_SHARE", 0.5)
    assert run_build(tmp_path, source=source) == build.EXIT_OK
    db = tmp_path / "arabic.db"
    assert gate.verify(str(db), len(ENTRIES)) == []
    con = sqlite3.connect(db)
    try:
        meta = dict(con.execute("SELECT key, value FROM meta"))
    finally:
        con.close()
    assert (meta["freq_texts"], meta["freq_texts_unavailable"], meta["freq_texts_selected"]) == ("1", "1", "2")
    monkeypatch.setattr(gate, "MAX_UNAVAILABLE_SHARE", 0.005)
    assert any("freq_texts_unavailable" in p for p in gate.verify(str(db), len(ENTRIES)))


def test_the_gate_fails_a_form_table_rebuilt_without_its_foreign_keys(tmp_path, small):
    """Without REFERENCES, foreign_key_check has nothing to check and an inner join drops the row."""
    assert run_build(tmp_path) == build.EXIT_OK
    db = tmp_path / "arabic.db"
    con = sqlite3.connect(db)
    con.executescript("""
        ALTER TABLE lane_forms RENAME TO old_forms;
        CREATE TABLE lane_forms (skeleton TEXT NOT NULL, root_key TEXT NOT NULL, entry_id TEXT NOT NULL,
                                 PRIMARY KEY (skeleton, entry_id));
        INSERT INTO lane_forms SELECT * FROM old_forms;
        DROP TABLE old_forms;
        CREATE INDEX idx_lane_forms_skeleton ON lane_forms(skeleton);
        UPDATE lane_forms SET entry_id = 'lane:none' WHERE entry_id = 'lane:t:2';
    """)
    con.commit()
    con.close()
    problems = gate.verify(str(db), len(ENTRIES))
    assert any("foreign keys" in p for p in problems), problems


def test_the_gate_fails_a_form_that_points_at_no_entry(tmp_path, small):
    assert run_build(tmp_path) == build.EXIT_OK
    db = tmp_path / "arabic.db"
    _mutate(db, "UPDATE lane_forms SET entry_id = 'lane:none' WHERE entry_id = 'lane:t:2'")
    assert any("point at no entry" in p for p in gate.verify(str(db), len(ENTRIES)))


def test_two_identifiers_cannot_share_one_file(tmp_path, small, capsys):
    """Text b's row given text a's file: a would be counted twice and b never."""
    source = make_source(tmp_path / "src")
    manifest = json.loads((source / "openiti" / "manifest.json").read_text(encoding="utf-8"))
    manifest["texts"][1].update(path=manifest["texts"][0]["path"], sha256=manifest["texts"][0]["sha256"])
    _write(source / "openiti" / "manifest.json", manifest)
    _refused(tmp_path, capsys, source, "names the file")


def test_the_repository_closed_class_table_is_well_formed():
    """The real table: every row a form, a root and a note; no form and root twice; enough rows
    for the gate's floor. Whether Lane has each root is checked by the build on the real data."""
    from shared.arabic_normalize import root_key, skeleton, strip_harakat

    data = json.loads(build.CLOSED_CLASS_FILE.read_text(encoding="utf-8"))
    forms = data["forms"]
    assert len(forms) >= 100
    keys = [(skeleton(strip_harakat(f["form"])), root_key(strip_harakat(f["root"]))) for f in forms]
    assert len(keys) == len(set(keys)), "a form and root twice"
    assert all(f["form"].strip() and f["root"].strip() and f["note"].strip() for f in forms)
    assert {"لنا", "ذلك", "الي", "علي"} <= {k for k, _ in keys}, "the owner's examples and the commonest words"


def test_the_gate_fails_a_missing_file_and_a_wrong_entry_count(tmp_path, small):
    assert gate.verify(str(tmp_path / "nope.db"), 1) == [f"{tmp_path / 'nope.db'} does not exist"]
    assert run_build(tmp_path) == build.EXIT_OK
    assert any("lane_entries" in p for p in gate.verify(str(tmp_path / "arabic.db"), len(ENTRIES) + 1))
    assert gate.main(["--db", str(tmp_path / "arabic.db"), "--expect-entries", str(len(ENTRIES))]) == 0


def test_the_frequent_forms_the_judges_found_are_in_the_closed_class_table():
    """Plan §5.4 (two AI judges, 2026-11-04): frequent words the method filed under the wrong root
    (مولاي under مول 'wealth', ستين under تين 'fig', بن under بن 'coffee') are in the table, each
    under the root the judges gave."""
    from shared.arabic_normalize import root_key, skeleton, strip_harakat

    data = json.loads(build.CLOSED_CLASS_FILE.read_text(encoding="utf-8"))
    table = {(skeleton(strip_harakat(f["form"])), root_key(strip_harakat(f["root"]))) for f in data["forms"]}
    for form, root in (("مولاي", "ولى"), ("مولانا", "ولى"), ("سيدي", "سود"), ("ياسيدي", "سود"),
                       ("ابن", "بنى"), ("بن", "بنى"), ("ابو", "ابو"), ("ابي", "ابو"), ("قلت", "قول"), ("كنت", "كون"), ("ادام", "دوم"),
                       ("عز", "عزز"), ("الاثنين", "ثنى"), ("ستين", "ست")):
        assert (skeleton(form), root_key(root)) in table, form
