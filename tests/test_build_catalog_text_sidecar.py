# -*- coding: utf-8 -*-
"""scripts/build_catalog_text_sidecar.py on synthetic inputs (tests/catalog_text_fixture.py)."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from catalog_text_fixture import A, C, D, E, F, G, H, M, build_args, write_inputs  # noqa: E402
from scripts.build_catalog_text_sidecar import (  # noqa: E402
    COLUMN_OF,
    EXIT_REFUSED,
    EXIT_SOURCE,
    SCHEMA_VERSION,
    drop_placeholders,
    main,
)
from shared.catalog_text_normalize import NORMALIZER_VERSION  # noqa: E402


def _ro(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path.resolve().as_posix()}?mode=ro", uri=True)


@pytest.fixture
def built(tmp_path):
    inputs = write_inputs(tmp_path / "inputs")
    out = tmp_path / "out" / "catalog_text.db"
    assert main(build_args(inputs, out)) == 0
    return out, inputs


def _records(path: Path) -> list[tuple]:
    con = _ro(path)
    try:
        return con.execute("SELECT sys_id, source, field, lang, automatic, text FROM records ORDER BY id").fetchall()
    finally:
        con.close()


def _meta(path: Path) -> dict:
    con = _ro(path)
    try:
        return dict(con.execute("SELECT key, value FROM meta").fetchall())
    finally:
        con.close()


def test_meta_records_versions_counts_and_inputs(built):
    out, inputs = built
    meta, records = _meta(out), _records(out)
    assert meta["version"] == SCHEMA_VERSION
    assert meta["normalizer_version"] == str(NORMALIZER_VERSION)
    assert meta["rows_total"] == str(len(records))
    assert meta["manuscripts_total"] == str(len({r[0] for r in records}))
    assert meta["input_fjms_version"] == "5.1.0"
    assert meta["input_pgp_upstream_commit"] == "abc123"
    assert meta["input_pgp_upstream_committed"] == "2026-09-21T19:31:40Z"
    assert len(meta["input_libraries_csv_sha256"]) == 64
    assert meta["input_libraries_csv_records"] == "3"
    for source in ("fjms", "pgp", "libraries"):
        for kind, flag in (("original", 0), ("automatic", 1)):
            assert meta[f"rows_{source}_{kind}"] == str(sum(1 for r in records if r[1] == source and r[4] == flag))


def test_every_record_has_a_corpus_sys_id_and_a_known_field(built):
    out, _ = built
    records = _records(out)
    assert all(r[0].startswith("99") for r in records)
    assert all((r[1], r[2]) in COLUMN_OF for r in records)
    assert "12345" not in {r[0] for r in records} and "system_number" not in {r[0] for r in records}
    assert _meta(out)["skipped_fjms_bad_sys_id"] == "1"


def test_a_text_is_stored_once_per_manuscript_whatever_field_said_it(built):
    out, _ = built
    records = _records(out)
    assert [r for r in records if r[0] == A and r[5] == "תפילין"] == [(A, "fjms", "TextualFrame", "he", 0, "תפילין")]
    # A's work title appears in two catalogue records and again as an automatic translation.
    assert [r[2] for r in records if r[0] == A and r[5] == "כתובה"] == ["GenizahTitle"]
    assert int(_meta(out)["skipped_fjms_duplicate"]) >= 3


def test_person_descriptions_are_not_indexed(built):
    out, _ = built
    assert not [r for r in _records(out) if "person" in r[5].lower()]


def test_translations_are_marked_automatic(built):
    out, _ = built
    records = _records(out)
    assert (D, "fjms", "FreeDesc", "en", 1, "Marriage contract (ketubba) of Sitt al-Dar") in records
    assert (D, "fjms", "FreeDesc", "he", 0, "כתובת סת אלדאר") in records


def test_title_and_author_translations_reach_the_manuscripts_that_carry_them(built):
    out, _ = built
    records = _records(out)
    for sys_id in (H, M):
        assert (sys_id, "fjms", "GenizahTitleEngTitle", "en", 1, "Questions and Answers") in records
    assert (C, "fjms", "AuthorText", "en", 1, "Nathan ben Nahorai") in records
    assert not [r for r in records if r[5] == "Orphan title"]
    assert _meta(out)["skipped_fjms_GenizahTitleEngTitle_unmapped"] == "2"   # the -1 and the orphan


def test_unknown_titles_and_authors_are_not_indexed(built):
    out, _ = built
    texts = {r[5] for r in _records(out)}
    assert not texts & {"לא ידוע", "לא מזוהה", "Unknown"}
    assert int(_meta(out)["skipped_fjms_placeholder"]) >= 3   # G's title, K's author and its translation


def test_a_pgp_description_goes_to_every_manuscript_of_its_document(built):
    out, _ = built
    records = _records(out)
    for sys_id in (E, F):
        assert (sys_id, "pgp", "description", "en", 0, "Marriage contract (ketubba). Fusṭāṭ, 1100.") in records
    assert not [r for r in records if r[1] == "pgp" and r[0] == A]  # its document has no description


def test_placeholder_title_segments_are_dropped(built):
    out, _ = built
    titles = {(r[2], r[5]) for r in _records(out) if r[0] == G}
    assert titles == {("title", "פיוט ליום כפור"), ("title_en", "Piyyut for Yom Kippur")}
    assert drop_placeholders("Cannot be determined from the catalog.") == ""


def test_words_go_to_the_column_of_their_field(built):
    out, _ = built
    con = _ro(out)
    try:
        def ids(column, word):
            return {r[0] for r in con.execute(
                f"SELECT r.sys_id FROM catalog_fts f JOIN records r ON r.id = f.rowid "
                f"WHERE catalog_fts MATCH ?", (f'{column} : "{word}"',))}
        assert ids("title", "כתובה") == {A}
        assert ids("fulltext", "כתובה") == {C}
        assert ids("description", "ketubba") == {D, E, F}
        assert H in ids("title", "רמבמ")
    finally:
        con.close()


def test_a_missing_input_fails_and_leaves_the_served_file(built, tmp_path):
    out, inputs = built
    before = out.read_bytes()
    broken = dict(inputs, pgp=tmp_path / "nope.db")
    assert main(build_args(broken, out)) == EXIT_SOURCE
    assert out.read_bytes() == before
    assert not list(out.parent.glob("*.building-*"))


def test_bibliography_translations_are_skipped_not_fatal(tmp_path):
    inputs = write_inputs(tmp_path / "inputs", extra_translations=[("123", "BibRunningTitle", "A bibliography title")])
    out = tmp_path / "catalog_text.db"
    assert main(build_args(inputs, out)) == 0
    assert not [r for r in _records(out) if r[5] == "A bibliography title"]


def test_an_untyped_translation_field_fails_the_build(tmp_path):
    inputs = write_inputs(tmp_path / "inputs", extra_translations=[(A, "Bogus", "text")])
    out = tmp_path / "catalog_text.db"
    assert main(build_args(inputs, out)) == EXIT_SOURCE
    assert not out.exists()


def test_a_smaller_build_is_refused_without_force(built, tmp_path):
    out, inputs = built
    before = out.read_bytes()
    con = sqlite3.connect(inputs["fjms"])
    con.execute("DELETE FROM catalog_free_desc")
    con.commit()
    con.close()
    assert main(build_args(inputs, out)) == EXIT_REFUSED
    assert out.read_bytes() == before
    assert main(build_args(inputs, out) + ["--force"]) == 0
    assert out.read_bytes() != before
