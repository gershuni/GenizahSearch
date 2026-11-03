# -*- coding: utf-8 -*-
"""scripts/build_blau_sidecar.py and scripts/verify_blau_sidecar.py on a synthetic v6 dictionary.

The fixture mirrors the shapes of the real release-1.1 JSON (an ordinary root with a stem, two
forms, a dictionary citation and a parsed reference; a sub-entry with a headword; a cross-reference
root whose form points at the first; a root with an empty name) with invented text, so nothing
of the dictionary itself is in the repository. The real build is run by hand on the download
(docs/plans/BLAU_DICTIONARY_PLAN.md, §7) and gated by the same verifier with --expect-roots 3047
--expect-forms 12753.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import sqlite3
from pathlib import Path

import pytest

from scripts.build_blau_sidecar import (
    BIBLIOGRAPHY_FILE,
    DICTIONARY_FILE,
    EXIT_OK,
    EXIT_OUTPUT,
    EXIT_REFUSED,
    EXIT_SOURCE,
    SCHEMA_VERSION,
)
from scripts.build_blau_sidecar import main as build_main
from scripts.verify_blau_sidecar import main as verify_main
from scripts.verify_blau_sidecar import verify
from shared.blau_normalize import NORMALIZER_VERSION


def _seg(text, text_type, font="Hebrew-Frankrehul", language="Hebrew", **extra):
    return {"font": font, "language": language, "text": text, "text_type": text_type, **extra}


def _ja(text, text_type="freeform_note", **extra):
    return _seg(text, text_type, font="Hebrew-Miriam", **extra)


def _ar(text, text_type, **extra):
    return _seg(text, text_type, font="Arabic", language="Arabic", **extra)


def _form(fid, segments, **flags):
    form = {"segments": segments, "is_classical": False, "is_reconstructed": False, "is_erroneous": False,
            "dictionary_citations": None, "references": None, "additional_notes": None, "id": fid}
    form.update(flags)
    return form


def _root(key, rid, name_ja, arabic, stems, sub_entries=(), page=7, xref=None):
    return {
        "schema_version": "v6", "name_ja": list(name_ja), "name_ja_clean": list(name_ja),
        "arabic_spelling": list(arabic), "start_page_printed": page, "is_cross_reference": xref is not None,
        "stems": stems, "sub_entries": list(sub_entries), "num_stems": len(stems),
        "num_sub_entries": len(sub_entries), "num_forms": sum(len(s["forms"]) for s in stems), "id": rid,
        **({"target_id": xref} if xref else {}),
    }


def dictionary(with_third_root=True, unnamed=False, dangling_source=False):
    first = _root("אכ̇ד̇ (أخذ)", "blau-e-000001", ["אכ̇ד̇"], ["أَخَذ"], stems=[{
        "id": "blau-st-000001",
        "stem_head": {"binyan": "I", "complement_ja": "ה ולד", "segments": [_ja("ה ולד", "complement_ja")]},
        "forms": [
            _form("blau-f-000001", [
                _seg("'לקח'", "definition_hebrew"),
                _seg("'to take'", "definition_english", font="English-TNR", language="English"),
                _ja("אכ̇ד̇ מנה"),
                _seg(".", "punctuation"),
            ], dictionary_citations=[{
                "id": "blau-dc-000001", "source_id": "blau-bib-000001",
                "segments": [_seg("דוזי, א, 15", "dictionary_cite")],
            }], references=[{
                "id": "blau-r-000001", "parse_type": "standard", "editorial_notes": [],
                "segments": [_seg("גויטיין, חברה, ב, 10", "reference", ref_role="citation")],
                "parsed": [{
                    "id": "blau-c-000001", "citation_type": "full", "source_work": "גויטיין, חברה",
                    "source_id": "blau-bib-000099" if dangling_source else "blau-bib-000002",
                    "citation": [_seg("גויטיין, חברה, ב, 10", "reference", ref_role="citation")],
                    "ja_text": [_ja("ואכ̇ד̇ת אלכתאב להם", "reference", ref_role="ja_text")],
                    "translation": [_seg("'ולקחתי להם את הספר'", "reference", ref_role="translation")],
                    "commentary": [],
                }],
            }]),
            _form("blau-f-000002", [
                _ja("מאכ̇וד̇", "variant_ja"),
                _ar("مأخوذ", "variant_arabic"),
                _seg("'taken'", "definition_english", font="English-TNR", language="English"),
            ], is_classical=True, variant_head={"binyan": "IV"}),
        ],
    }], sub_entries=[{
        "id": "blau-se-000001", "name_ja": [] if unnamed else ["מאכ̇ד̇"], "arabic_spelling": ["مَأْخَذ"],
        "stems": [{"id": "blau-st-000002", "stem_head": {}, "forms": [
            _form("blau-f-000003", [_seg("'מקור'", "definition_hebrew"), _ja("מאכ̇ד̇ים", "headword_ja")]),
        ]}],
    }])
    xref = _root("חוה (حوى) [→]", "blau-e-000002", ["חוה"], ["حوى"], stems=[{
        "id": "blau-st-000003", "stem_head": {}, "forms": [
            _form("blau-f-000004", [
                _seg("←", "cross_reference", font="Special-1", language="Special"),
                _ar("أخذ", "cross_reference", target_id="blau-e-000001"),
                _seg(".", "punctuation"),
            ]),
        ],
    }], xref="blau-e-000001")
    data = {first["name_ja"][0] + " (أخذ)": first, "חוה (حوى) [→]": xref}
    if with_third_root:
        third = _root("בלג (بلغ)", "blau-e-000003", [] if unnamed else ["בלג"], ["بلغ"], stems=[{
            "id": "blau-st-000004", "stem_head": {"binyan": "II"}, "forms": [
                _form("blau-f-000005", [_seg("'הגיע'", "definition_hebrew")]),
            ],
        }])
        data["(بلغ)" if unnamed else "בלג (بلغ)"] = third
    return data


BIBLIOGRAPHY = [
    {"id": "blau-bib-000001", "abbreviation": "דוזי", "full_reference": "R. Dozy, Supplément, Leiden 1881",
     "page": 18, "parent": ""},
    {"id": "blau-bib-000002", "abbreviation": "גויטיין, חברה",
     "full_reference": "S. D. Goitein, A Mediterranean Society, Berkeley 1967-1993", "page": 19, "parent": ""},
]


def write_source(dir_path: Path, data=None, bibliography=BIBLIOGRAPHY, gz_bytes=None) -> Path:
    dir_path.mkdir(parents=True, exist_ok=True)
    if gz_bytes is None:
        gz_bytes = gzip.compress(json.dumps(data if data is not None else dictionary(),
                                            ensure_ascii=False).encode("utf-8"))
    (dir_path / DICTIONARY_FILE).write_bytes(gz_bytes)
    if bibliography is not None:
        (dir_path / BIBLIOGRAPHY_FILE).write_text(json.dumps(bibliography, ensure_ascii=False), encoding="utf-8")
    return dir_path


def _sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)


@pytest.fixture
def built(tmp_path):
    src = write_source(tmp_path / "download")
    out = tmp_path / "blau_data" / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    return out


# ---------------------------------------------------------------------------
# The build
# ---------------------------------------------------------------------------

def test_build_writes_every_table_and_the_meta(built):
    con = _ro(built)
    try:
        meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ("roots", "sub_entries", "stems", "forms", "citations", "forms_norm", "sources")}
    finally:
        con.close()
    assert counts == {"roots": 3, "sub_entries": 1, "stems": 4, "forms": 5, "citations": 2,
                      "forms_norm": 9, "sources": 2}
    assert meta["version"] == SCHEMA_VERSION
    assert meta["source_release"] == "1.1"
    assert meta["source_schema_version"] == "v6"
    assert meta["normalizer_version"] == str(NORMALIZER_VERSION)
    assert len(meta["source_sha256"]) == 64 and len(meta["source_json_sha256"]) == 64
    assert meta["built_at"].startswith("20")
    assert "בלאו" in meta["credit_he"] and "פורסם ברשות" in meta["credit_he"]
    assert "Published with permission" in meta["credit_en"]
    for t, n in counts.items():
        assert meta[f"rows_{t}"] == str(n)
    assert meta["cross_references"] == "1" and meta["cross_references_unresolved"] == "0"
    assert meta["roots_unnamed"] == "0" and meta["sub_entries_unnamed"] == "0"
    assert meta["citations_dangling_source"] == "0"


def test_forms_carry_text_glosses_flags_and_page(built):
    con = _ro(built)
    try:
        f1 = con.execute("SELECT ja_text, arabic_spelling, gloss_he, gloss_en, notes, binyan, is_classical, page, "
                         "cross_reference, target_ids FROM forms WHERE id = 'blau-f-000001'").fetchone()
        f2 = con.execute("SELECT ja_text, arabic_spelling, gloss_en, binyan, is_classical FROM forms "
                         "WHERE id = 'blau-f-000002'").fetchone()
        f4 = con.execute("SELECT gloss_he, cross_reference, target_ids, root_id FROM forms "
                         "WHERE id = 'blau-f-000004'").fetchone()
        sub = con.execute("SELECT root_id, name_ja, arabic_spelling, arabic_norm FROM sub_entries").fetchone()
        stem = con.execute("SELECT binyan, complement_ja FROM stems WHERE id = 'blau-st-000001'").fetchone()
        root = con.execute("SELECT key, name_ja, arabic_norm, start_page_printed, is_cross_reference, target_id, "
                           "num_stems, num_sub_entries, num_forms FROM roots WHERE id = 'blau-e-000001'").fetchone()
        xref = con.execute("SELECT is_cross_reference, target_id FROM roots WHERE id = 'blau-e-000002'").fetchone()
    finally:
        con.close()
    assert f1 == (None, None, "'לקח'", "'to take'", "אכ̇ד̇ מנה", "I", 0, 7, None, None)
    assert f2 == ("מאכ̇וד̇", "مأخوذ", "'taken'", "IV", 1)       # the variant head's binyan wins
    assert f4 == (None, "أخذ", '["blau-e-000001"]', "blau-e-000002")
    assert sub == ("blau-e-000001", "מאכ̇ד̇", "مَأْخَذ", "ماخذ")
    assert stem == ("I", "ה ולד")
    assert root == ("אכ̇ד̇ (أخذ)", "אכ̇ד̇", "اخذ", 7, 0, None, 2, 1, 3)
    assert xref == (1, "blau-e-000001")


def test_lookup_keys_are_normalized_and_typed(built):
    con = _ro(built)
    try:
        rows = con.execute("SELECT norm, raw, kind, root_id, sub_entry_id, form_id, word_count FROM forms_norm "
                           "ORDER BY kind, norm").fetchall()
    finally:
        con.close()
    assert rows == [
        ("מאכדימ", "מאכ̇ד̇ים", "headword", "blau-e-000001", "blau-se-000001", "blau-f-000003", 1),
        ("אכד מנה", "אכ̇ד̇ מנה", "phrase", "blau-e-000001", None, "blau-f-000001", 2),
        ("ה ולד", "ה ולד", "phrase", "blau-e-000001", None, None, 2),           # the stem head's complement
        ("ואכדת אלכתאב להמ", "ואכ̇ד̇ת אלכתאב להם", "phrase", "blau-e-000001", None, "blau-f-000001", 3),
        ("אכד", "אכ̇ד̇", "root", "blau-e-000001", None, None, 1),
        ("בלג", "בלג", "root", "blau-e-000003", None, None, 1),
        ("חוה", "חוה", "root", "blau-e-000002", None, None, 1),
        ("מאכד", "מאכ̇ד̇", "sub_entry", "blau-e-000001", "blau-se-000001", None, 1),
        ("מאכוד", "מאכ̇וד̇", "variant", "blau-e-000001", None, "blau-f-000002", 1),
    ]


def test_citations_carry_kind_source_and_texts(built):
    con = _ro(built)
    try:
        rows = con.execute("SELECT id, form_id, kind, reference_id, citation_type, source_work, source_id, "
                           "citation, ja_text, translation FROM citations ORDER BY id").fetchall()
        sources = con.execute("SELECT id, abbreviation, page FROM sources ORDER BY id").fetchall()
    finally:
        con.close()
    assert rows == [
        ("blau-c-000001", "blau-f-000001", "reference", "blau-r-000001", "full", "גויטיין, חברה", "blau-bib-000002",
         "גויטיין, חברה, ב, 10", "ואכ̇ד̇ת אלכתאב להם", "'ולקחתי להם את הספר'"),
        ("blau-dc-000001", "blau-f-000001", "dictionary", None, "dictionary", None, "blau-bib-000001",
         "דוזי, א, 15", None, None),
    ]
    assert sources == [("blau-bib-000001", "דוזי", 18), ("blau-bib-000002", "גויטיין, חברה", 19)]


def test_unnamed_entries_are_kept_and_counted(tmp_path):
    src = write_source(tmp_path / "download", dictionary(unnamed=True))
    out = tmp_path / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    con = _ro(out)
    try:
        meta = dict(con.execute("SELECT key, value FROM meta").fetchall())
        assert con.execute("SELECT name_ja, names_json FROM roots WHERE id = 'blau-e-000003'").fetchone() == ("", "[]")
        assert con.execute("SELECT name_ja FROM sub_entries").fetchone() == ("",)
        assert con.execute("SELECT COUNT(*) FROM forms_norm WHERE kind IN ('sub_entry')").fetchone()[0] == 0
    finally:
        con.close()
    assert meta["roots_unnamed"] == "1" and meta["sub_entries_unnamed"] == "1"
    assert verify(str(out), 3, 5) == []


def test_a_dangling_source_id_is_recorded_and_pinned(tmp_path):
    src = write_source(tmp_path / "download", dictionary(dangling_source=True))
    out = tmp_path / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    con = _ro(out)
    try:
        assert con.execute("SELECT value FROM meta WHERE key = 'citations_dangling_source'").fetchone() == ("1",)
    finally:
        con.close()
    assert verify(str(out), 3, 5) == []
    con = sqlite3.connect(str(out))
    con.execute("UPDATE meta SET value = '0' WHERE key = 'citations_dangling_source'")
    con.commit()
    con.close()
    assert any("citations_dangling_source" in p for p in verify(str(out), 3, 5))


# ---------------------------------------------------------------------------
# Failing, not skipping
# ---------------------------------------------------------------------------

def test_build_fails_on_a_missing_dictionary(tmp_path, capsys):
    src = tmp_path / "download"
    src.mkdir()
    (src / BIBLIOGRAPHY_FILE).write_text("[]", encoding="utf-8")
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    assert "does not exist" in capsys.readouterr().out
    assert not (tmp_path / "blau.db").exists()


def test_build_fails_on_an_empty_download(tmp_path, capsys):
    src = write_source(tmp_path / "download", gz_bytes=b"")
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    assert "is empty" in capsys.readouterr().out


def test_build_fails_on_a_truncated_gzip(tmp_path, capsys):
    whole = gzip.compress(json.dumps(dictionary(), ensure_ascii=False).encode("utf-8"))
    src = write_source(tmp_path / "download", gz_bytes=whole[: len(whole) // 2])
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    assert "not a complete gzip" in capsys.readouterr().out
    assert not (tmp_path / "blau.db").exists()


def test_build_fails_on_a_truncated_json_inside_a_valid_gzip(tmp_path, capsys):
    text = json.dumps(dictionary(), ensure_ascii=False)
    src = write_source(tmp_path / "download", gz_bytes=gzip.compress(text[: len(text) // 2].encode("utf-8")))
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    assert "not valid UTF-8 JSON" in capsys.readouterr().out


@pytest.mark.parametrize("data, message", [
    ({}, "non-empty object"),
    ([], "non-empty object"),
    ({"x": "y"}, "is not an object"),
    ({"x": {"schema_version": "v5", "id": "blau-e-1"}}, "schema_version 'v5'"),
    ({"x": {"schema_version": "v6"}}, "has no id"),
])
def test_build_fails_on_a_dictionary_that_is_not_v6(tmp_path, capsys, data, message):
    src = write_source(tmp_path / "download", data)
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    assert message in capsys.readouterr().out


@pytest.mark.parametrize("field, value, message", [
    ("name_ja", 123, "name_ja is int, not a list of strings"),
    ("arabic_spelling", {"x": 1}, "arabic_spelling is dict"),
    ("stems", 5, "not shaped like the dictionary (TypeError"),
    ("sub_entries", "x", "sub-entry 0 has no id"),        # a string iterates as characters: caught by the walk
])
def test_build_fails_without_a_traceback_on_a_field_of_the_wrong_type(tmp_path, capsys, field, value, message):
    """Codex round 1: a scalar name_ja raised an uncaught TypeError; every wrong shape is EXIT_SOURCE."""
    data = dictionary()
    data["בלג (بلغ)"][field] = value
    src = write_source(tmp_path / "download", data)
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    out = capsys.readouterr().out
    assert message in out and "Traceback" not in out
    assert not (tmp_path / "blau.db").exists()


def test_build_fails_on_a_missing_or_malformed_bibliography(tmp_path, capsys):
    src = write_source(tmp_path / "download", bibliography=None)
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    assert BIBLIOGRAPHY_FILE in capsys.readouterr().out
    write_source(src, bibliography=[{"id": "blau-bib-000001"}])
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    assert "abbreviation" in capsys.readouterr().out


def test_build_fails_on_a_duplicate_id(tmp_path, capsys):
    data = dictionary()
    data["בלג (بلغ)"]["stems"][0]["forms"][0]["id"] = "blau-f-000001"
    src = write_source(tmp_path / "download", data)
    assert build_main(["--source-dir", str(src), "--out", str(tmp_path / "blau.db")]) == EXIT_SOURCE
    assert "duplicate form id" in capsys.readouterr().out


def test_build_refuses_to_shrink_an_existing_sidecar_unless_forced(tmp_path, capsys):
    src = write_source(tmp_path / "download")
    out = tmp_path / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    before = _sha(out)
    smaller = write_source(tmp_path / "download2", dictionary(with_third_root=False))
    assert build_main(["--source-dir", str(smaller), "--out", str(out)]) == EXIT_REFUSED
    printed = capsys.readouterr().out
    assert "REFUSED" in printed and "roots 3 -> 2" in printed and "forms 5 -> 4" in printed
    assert _sha(out) == before                        # the served file is untouched
    assert not list(tmp_path.glob("*.building-*"))    # and no temp file is left behind
    assert build_main(["--source-dir", str(smaller), "--out", str(out), "--force"]) == EXIT_OK
    con = _ro(out)
    try:
        assert con.execute("SELECT COUNT(*) FROM roots").fetchone()[0] == 2
    finally:
        con.close()


def test_build_replaces_an_equal_sidecar_without_force(tmp_path):
    src = write_source(tmp_path / "download")
    out = tmp_path / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    assert verify(str(out), 3, 5) == []


def test_build_replaces_an_unreadable_file_at_the_target(tmp_path):
    src = write_source(tmp_path / "download")
    out = tmp_path / "blau.db"
    out.write_bytes(b"not a database")
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    assert verify(str(out), 3, 5) == []


def test_a_failed_write_keeps_the_old_sidecar_and_leaves_no_temp_file(tmp_path, monkeypatch, capsys):
    import scripts.build_blau_sidecar as mod
    src = write_source(tmp_path / "download")
    out = tmp_path / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OK
    before = _sha(out)

    def boom(path, *a, **k):
        Path(path).write_bytes(b"half")
        raise sqlite3.OperationalError("disk full")

    monkeypatch.setattr(mod, "write_sidecar", boom)
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OUTPUT
    assert "disk full" in capsys.readouterr().out
    assert _sha(out) == before
    assert not list(tmp_path.glob("*.building-*"))


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------

def test_verify_passes_on_the_built_file(built, capsys):
    assert verify_main(["--db", str(built), "--expect-roots", "3", "--expect-forms", "5",
                        "--expect-version", SCHEMA_VERSION]) == 0
    assert "RESULT: PASS" in capsys.readouterr().out


def test_verify_fails_rather_than_skips_on_a_missing_file(tmp_path, capsys):
    assert verify_main(["--db", str(tmp_path / "none.db"), "--expect-roots", "3", "--expect-forms", "5"]) == 1
    out = capsys.readouterr().out
    assert "does not exist" in out and "RESULT: FAIL" in out


def test_verify_fails_on_a_file_that_is_not_a_database(tmp_path):
    bad = tmp_path / "bad.db"
    bad.write_bytes(b"x" * 100)
    problems = verify(str(bad), 3, 5)
    assert problems and "not a SQLite database" in problems[0]


def test_verify_fails_when_the_expected_counts_differ(built):
    problems = verify(str(built), 3047, 12753)
    assert any("roots 3 != expected 3,047" in p for p in problems)
    assert any("forms 5 != expected 12,753" in p for p in problems)


def test_verify_fails_on_a_wrong_source_digest_or_version(built):
    assert any("source_sha256" in p for p in verify(str(built), 3, 5, expect_source_sha256="ab" * 32))
    assert any("meta.version" in p for p in verify(str(built), 3, 5, expect_version="9.9.9"))


def _mutate(path: Path, sql: str) -> None:
    con = sqlite3.connect(str(path))
    try:
        con.execute(sql)
        con.commit()
    finally:
        con.close()


def test_verify_fails_on_a_dropped_form(built):
    _mutate(built, "DELETE FROM forms WHERE id = 'blau-f-000005'")
    problems = verify(str(built), 3, 5)
    assert any("forms 4 != expected 5" in p for p in problems)
    assert any("meta.rows_forms" in p for p in problems)
    assert any("num_forms that differs" in p for p in problems)


def test_verify_fails_on_an_edited_lookup_key(built):
    _mutate(built, "UPDATE forms_norm SET norm = 'אכדד' WHERE raw = 'אכ̇ד̇' AND kind = 'root'")
    problems = verify(str(built), 3, 5)
    assert any("differ from normalize_ja(raw)" in p and "אכדד" in p for p in problems)


def test_verify_fails_on_a_lookup_key_built_by_another_normalizer(built):
    _mutate(built, "UPDATE meta SET value = '0' WHERE key = 'normalizer_version'")
    assert any("normalizer_version" in p and "rebuild" in p for p in verify(str(built), 3, 5))


def test_verify_fails_on_a_doctored_meta_count(built):
    _mutate(built, "UPDATE meta SET value = '999' WHERE key = 'rows_roots'")
    assert any("meta.rows_roots = '999'" in p for p in verify(str(built), 3, 5))


def test_verify_fails_on_a_missing_credit_line(built):
    _mutate(built, "UPDATE meta SET value = '' WHERE key = 'credit_he'")
    assert any("meta.credit_he" in p for p in verify(str(built), 3, 5))


def test_verify_fails_on_a_dangling_cross_reference(built):
    _mutate(built, "UPDATE roots SET target_id = 'blau-e-999999' WHERE id = 'blau-e-000002'")
    assert any("cross-reference roots" in p and "not in the table" in p for p in verify(str(built), 3, 5))


def test_verify_fails_on_a_cross_reference_without_a_target(built):
    """Codex round 1: a NULL target slipped past the dangling-target check."""
    _mutate(built, "UPDATE roots SET target_id = NULL WHERE id = 'blau-e-000002'")
    assert any("cross-reference roots" in p and "no target" in p for p in verify(str(built), 3, 5))


def test_verify_fails_on_a_dropped_index(built):
    _mutate(built, "DROP INDEX idx_forms_norm_norm")
    assert any("idx_forms_norm_norm" in p for p in verify(str(built), 3, 5))


def test_verify_fails_on_a_dropped_table(built):
    _mutate(built, "DROP TABLE citations")
    problems = verify(str(built), 3, 5)
    assert problems == ["tables missing: ['citations']"]


def test_verify_checks_columns_types_constraints_and_indexes(built):
    """Codex round 2: table names alone let a file with a dropped column pass."""
    assert verify(str(built), 3, 5) == []
    _mutate(built, "ALTER TABLE roots DROP COLUMN name_ja_clean")
    problems = verify(str(built), 3, 5)
    assert any("table roots does not match the schema: missing ['name_ja_clean']" in p for p in problems)
    _mutate(built, "ALTER TABLE forms ADD COLUMN extra TEXT")
    assert any("table forms does not match the schema" in p and "extra ['extra']" in p
               for p in verify(str(built), 3, 5))
    _mutate(built, "CREATE TABLE stray (x)")
    assert any("table stray is not part of the schema" in p for p in verify(str(built), 3, 5))
    _mutate(built, "CREATE INDEX idx_stray ON stray(x)")
    assert any(p.startswith("indexes ") for p in verify(str(built), 3, 5))


def test_verify_requires_the_schema_version_this_code_reads(built):
    _mutate(built, "UPDATE meta SET value = '0.9.0' WHERE key = 'version'")
    assert any("meta.version '0.9.0' != the schema this code reads" in p for p in verify(str(built), 3, 5))


def test_verify_fails_on_an_erased_sub_entry_name(built):
    """Codex round 2: only unnamed roots were compared with the pinned count."""
    _mutate(built, "UPDATE sub_entries SET name_ja = ''")
    assert any("sub-entries have no name_ja but meta.sub_entries_unnamed" in p for p in verify(str(built), 3, 5))


def test_build_reports_an_unwritable_output_directory_as_exit_output(tmp_path, capsys):
    """Codex round 2: a parent that is a file raised before the output-error handler."""
    src = write_source(tmp_path / "download")
    (tmp_path / "blocker").write_text("a file, not a directory", encoding="utf-8")
    out = tmp_path / "blocker" / "blau.db"
    assert build_main(["--source-dir", str(src), "--out", str(out)]) == EXIT_OUTPUT
    printed = capsys.readouterr().out
    assert "FAIL: could not write" in printed and "Traceback" not in printed
