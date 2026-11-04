# -*- coding: utf-8 -*-
"""scripts/build_arabic_sidecar.py and scripts/verify_arabic_sidecar.py on a synthetic source.

The build writes every table from pinned sources and refuses what it cannot vouch for (a file
that differs from its pin, a credit line that is not Perseus's, a count that disagrees with the
source's manifest, a closed-class row Lane cannot answer, a smaller sidecar over a larger one);
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

SHARDS = 4
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
]


def _write(path: Path, data) -> bytes:
    raw = json.dumps(data, ensure_ascii=False).encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(raw)
    return raw


def make_source(root: Path, credit: str = build.PERSEUS_CREDIT, count: int = len(ENTRIES)) -> Path:
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
    for name, body in (("a.txt", "#META# x\nقال ذلك الكتاب والكتاب ذلك أخذ أخذ أخذ\n"),
                       ("b.txt", "ذلك ذلك لنا لنا من من على على كتب كتب دلك\n")):
        raw = body.encode("utf-8")
        (openiti / name).parent.mkdir(parents=True, exist_ok=True)
        (openiti / name).write_bytes(raw)
        texts.append({"path": f"data/x/{name[:-4]}", "sha256": hashlib.sha256(raw).hexdigest()})
    _write(openiti / "manifest.json", {"texts": texts})
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
    finally:
        con.close()


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
])
def test_the_gate_fails_each_mutation(tmp_path, small, sql, expect):
    assert run_build(tmp_path) == build.EXIT_OK
    db = tmp_path / "arabic.db"
    _mutate(db, sql)
    problems = gate.verify(str(db), len(ENTRIES))
    assert any(expect in p for p in problems), problems


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
