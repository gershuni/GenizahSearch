# -*- coding: utf-8 -*-
"""Drive the REAL export orchestration against a fake Supabase.

Why this file exists, separately from tests/test_export_pgp_sidecar_integrity.py:

An independent review (gpt-6-astra, 2026-09-22) took a copy of the exporter, removed all
four ``assert_no_dropped_columns`` calls, removed the carry-forward and its validation
from ``main()``, and replaced every exported ``doc_relation`` value with ``None`` --
**and all twelve tests still passed.** They tested the helper functions; nothing tested
that the export CALLS them. The mutation harness missed it for the same reason: it broke
helper bodies, which the helper tests do cover, so it never tried deleting a call site.

So these tests run ``build_sidecar()`` itself and assert on the database that comes out.
A deleted call site fails here.
"""
from __future__ import annotations

import importlib.util
import pathlib
import sqlite3

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPT_PATH = REPO_ROOT / "scripts" / "export_pgp_sidecar.py"


@pytest.fixture(scope="module")
def exporter():
    spec = importlib.util.spec_from_file_location("_export_e2e", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# ── a Supabase stand-in ───────────────────────────────────────────────────────

DOCUMENTS = [
    {
        "pgpid": 1, "shelfmark_combined": "T-S 12.1", "document_type": "Letter",
        "tags": ["communal"], "doc_date_original": None, "doc_date_standard": None,
        "doc_date_calendar": None, "inferred_date_display": None,
        "inferred_date_standard": None, "inferred_date_rationale": None,
        "inferred_date_notes": None, "description": "A letter.",
        "transcription": "some text", "transcription_source": "Goitein",
        "languages_primary": "Judaeo-Arabic", "languages_secondary": None,
        "language_note": None, "scholarship_records": None, "shelfmarks_historic": None,
        "has_transcription": 1, "has_translation": 0, "input_by": None,
        "pgp_url": "https://geniza.princeton.edu/documents/1/",
        "created_at": "2026-01-01T00:00:00Z",
        "doc_relation": "Digital Translation",
    },
    {
        "pgpid": 2, "shelfmark_combined": "T-S 12.2", "document_type": None,
        "tags": None, "doc_date_original": None, "doc_date_standard": None,
        "doc_date_calendar": None, "inferred_date_display": None,
        "inferred_date_standard": None, "inferred_date_rationale": None,
        "inferred_date_notes": None, "description": None, "transcription": None,
        "transcription_source": None, "languages_primary": None,
        "languages_secondary": None, "language_note": None, "scholarship_records": None,
        "shelfmarks_historic": None, "has_transcription": 0, "has_translation": 0,
        "input_by": None, "pgp_url": None, "created_at": "2026-01-01T00:00:00Z",
        # NULL is the common case: 28,896 of 35,986 real documents.
        "doc_relation": None,
    },
]

OTHER_TABLES = {
    "document_sources": [{
        "id": 1, "pgpid": 1, "source_scholar": "Goitein", "doc_relation": "Digital Edition",
        "language": "Judaeo-Arabic", "content": "text", "content_length": 4,
        "source_url": None, "notes": None, "sequence_order": 1, "sections": None,
        "source_language": None, "source_direction": None,
        "created_at": "2026-01-01T00:00:00Z",
    }],
    "document_footnotes": [{
        "id": 1, "pgpid": 1, "source": "Goitein, Med. Soc.", "source_slug": "goitein",
        "doc_relation": "Discussion", "location": "p. 1", "url": None, "notes": None,
        "content": None, "content_length": None, "created_at": "2026-01-01T00:00:00Z",
    }],
    "document_fragments": [{
        "id": 1, "document_id": 1, "sys_id": "990001", "shelfmark": "T-S 12.1",
        "sequence_order": 1, "page_info": "recto", "collection": "Taylor-Schechter",
        "library": "Cambridge University Library", "library_abbrev": "CUL",
        "fragment_url": None, "iiif_url": "https://example.org/iiif",
        "created_at": "2026-01-01T00:00:00Z",
    }],
}


class _Query:
    def __init__(self, rows, extra_column=None):
        self._rows = rows
        self._extra = extra_column
        self.count = len(rows)
        self.data = rows

    def select(self, *_a, **_k):
        return self

    def order(self, *_a, **_k):
        return self

    def limit(self, n):
        self.data = self._rows[:n]
        return self

    def range(self, start, end):
        self.data = self._rows[start:end + 1]
        return self

    def execute(self):
        return self


class FakeSupabase:
    """Just enough PostgREST to drive the exporter."""

    def __init__(self, documents=None, extra_column=None):
        self.tables = dict(OTHER_TABLES)
        self.tables["documents"] = documents if documents is not None else DOCUMENTS
        if extra_column:
            self.tables["documents"] = [
                dict(row, **{extra_column: "surprise"}) for row in self.tables["documents"]
            ]

    def table(self, name):
        return _Query(self.tables[name])


def _seed_existing_sidecar(path, translation_rows):
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(
            "CREATE TABLE pgp_translations ("
            " pgpid INTEGER PRIMARY KEY, description_he TEXT, document_type_he TEXT,"
            " translated_at TEXT, model_version TEXT DEFAULT 'dictalm2.0')"
        )
        conn.executemany("INSERT INTO pgp_translations VALUES (?,?,?,?,?)", translation_rows)
        conn.commit()
    finally:
        conn.close()


# ── the tests ─────────────────────────────────────────────────────────────────


def test_the_export_really_writes_doc_relation(exporter, tmp_path):
    """Not 'the constant lists it' -- the built database holds the values."""
    assert exporter.build_sidecar(FakeSupabase(), tmp_path, "https://fake") == 0

    conn = sqlite3.connect(str(tmp_path / "pgp.db"))
    try:
        rows = dict(conn.execute("SELECT pgpid, doc_relation FROM documents"))
    finally:
        conn.close()

    assert rows == {1: "Digital Translation", 2: None}, (
        "the exported values must survive, including the NULL that 80% of real "
        "documents carry"
    )


def test_the_export_really_carries_translations_forward(exporter, tmp_path):
    """The 2026-04-22 data loss, end to end through the real orchestration."""
    rows = [(1, "תרגום", None, "2026-09-21T00:00:00Z", "dictalm2.0")]
    _seed_existing_sidecar(tmp_path / "pgp.db", rows)

    assert exporter.build_sidecar(FakeSupabase(), tmp_path, "https://fake") == 0

    conn = sqlite3.connect(str(tmp_path / "pgp.db"))
    try:
        assert conn.execute(
            "SELECT pgpid, description_he FROM pgp_translations"
        ).fetchall() == [(1, "תרגום")]
        # ...and the rebuild really did happen around it
        assert conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 2
    finally:
        conn.close()


def test_a_new_supabase_column_actually_stops_the_export(exporter, tmp_path):
    """The guard must be WIRED IN, not merely defined."""
    with pytest.raises(RuntimeError) as excinfo:
        exporter.build_sidecar(
            FakeSupabase(extra_column="newly_added_by_princeton"), tmp_path, "https://fake"
        )
    assert "newly_added_by_princeton" in str(excinfo.value)
    assert not (tmp_path / "pgp.db").exists(), "a refused build must leave nothing behind"


@pytest.mark.parametrize(
    "table", ["document_sources", "document_footnotes", "document_fragments"]
)
def test_every_table_is_guarded_not_just_documents(exporter, tmp_path, table):
    """Round 2 of the same review: removing only the source/footnote/fragment guard calls
    still passed all 24 tests, because only `documents` was exercised. Four call sites
    need four tests."""
    fake = FakeSupabase()
    fake.tables[table] = [
        dict(row, newly_added_by_princeton="surprise") for row in fake.tables[table]
    ]

    with pytest.raises(RuntimeError) as excinfo:
        exporter.build_sidecar(fake, tmp_path, "https://fake")
    assert "newly_added_by_princeton" in str(excinfo.value)
    assert table in str(excinfo.value)


def test_fragment_page_info_survives_the_export(exporter, tmp_path):
    """Also round 2: forcing exported page_info to None passed every test. It decides
    WHICH document a two-sided fragment resolves to (shared/document_service.py) and
    which page's text renders (shared/browse_service.py), so losing it is not cosmetic."""
    assert exporter.build_sidecar(FakeSupabase(), tmp_path, "https://fake") == 0

    conn = sqlite3.connect(str(tmp_path / "pgp.db"))
    try:
        assert conn.execute(
            "SELECT page_info FROM document_fragments WHERE document_id = 1"
        ).fetchone()[0] == "recto"
    finally:
        conn.close()


def test_a_failed_export_leaves_the_previous_sidecar_intact(exporter, tmp_path):
    """The reason the build happens beside the live file rather than on top of it."""
    rows = [(1, "תרגום", None, "2026-09-21T00:00:00Z", "dictalm2.0")]
    _seed_existing_sidecar(tmp_path / "pgp.db", rows)
    before = (tmp_path / "pgp.db").read_bytes()

    with pytest.raises(RuntimeError):
        exporter.build_sidecar(
            FakeSupabase(extra_column="newly_added_by_princeton"), tmp_path, "https://fake"
        )

    assert (tmp_path / "pgp.db").read_bytes() == before
    assert not (tmp_path / "pgp.db.new").exists(), "the partial build must be cleaned up"


def test_provenance_is_only_stamped_when_it_is_corroborated(exporter, tmp_path):
    """A commit id the sidecar cannot vouch for is worse than none."""
    # No import record at all -> no stamp.
    assert exporter.build_sidecar(FakeSupabase(), tmp_path, "https://fake") == 0
    conn = sqlite3.connect(str(tmp_path / "pgp.db"))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta"))
    finally:
        conn.close()
    assert "upstream_commit" not in meta

    # A record whose counts match what we export -> stamped.
    (tmp_path / exporter.IMPORT_PROVENANCE_FILENAME).write_text(
        '{"upstream_commit": "a94528cc", "upstream_repo": "princetongenizalab/pgp-metadata",'
        ' "inputs_verified": true, "supabase_url": "https://fake",'
        ' "supabase_counts_after": {"documents": 2, "document_sources": 1,'
        ' "document_footnotes": 1, "document_fragments": 1}}',
        encoding="utf-8",
    )
    assert exporter.build_sidecar(FakeSupabase(), tmp_path, "https://fake") == 0
    conn = sqlite3.connect(str(tmp_path / "pgp.db"))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta"))
    finally:
        conn.close()
    assert meta["upstream_commit"] == "a94528cc"

    # Same record, exported from a different project -> no stamp.
    assert exporter.build_sidecar(FakeSupabase(), tmp_path, "https://another") == 0
    conn = sqlite3.connect(str(tmp_path / "pgp.db"))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta"))
    finally:
        conn.close()
    assert "upstream_commit" not in meta


def test_a_stale_import_record_is_not_stamped(exporter, tmp_path):
    """Somebody else refreshed Supabase after that import."""
    (tmp_path / exporter.IMPORT_PROVENANCE_FILENAME).write_text(
        '{"upstream_commit": "a94528cc",'
        ' "supabase_counts_after": {"documents": 99999, "document_sources": 1,'
        ' "document_footnotes": 1, "document_fragments": 1}}',
        encoding="utf-8",
    )
    assert exporter.build_sidecar(FakeSupabase(), tmp_path, "https://fake") == 0

    conn = sqlite3.connect(str(tmp_path / "pgp.db"))
    try:
        meta = dict(conn.execute("SELECT key, value FROM meta"))
    finally:
        conn.close()
    assert "upstream_commit" not in meta


@pytest.mark.parametrize(
    "table", ["documents", "document_sources", "document_footnotes", "document_fragments"]
)
def test_an_empty_core_table_aborts_the_export(exporter, tmp_path, table):
    """A core PGP table is never legitimately empty. If one comes back with no rows --
    a rotated key, an RLS change, the wrong project -- the exporter would build an empty
    table, and validate_export() counts through the SAME restricted client, so both sides
    read zero and the 'validated' build replaces the live sidecar. Silent whole-table loss."""
    fake = FakeSupabase()
    fake.tables[table] = []

    with pytest.raises(RuntimeError) as excinfo:
        exporter.build_sidecar(fake, tmp_path, "https://fake")
    assert table in str(excinfo.value)
    assert not (tmp_path / "pgp.db").exists()


@pytest.mark.parametrize(
    "table", ["documents", "document_sources", "document_footnotes", "document_fragments"]
)
def test_a_core_table_that_came_back_short_aborts_the_export(exporter, tmp_path, table):
    """Same threat as the empty-table guard, one row up: a restricted client returning
    1 of N rows passed everything and replaced the live sidecar with the 1-row one, because
    validate_export() counts through the same restricted client. Upserts never delete, so
    a core table that shrank between two exports is never a smaller corpus."""
    # First build: the fake corpus, with one extra row in the table under test, becomes
    # the live sidecar.
    bigger = FakeSupabase()
    extra = dict(bigger.tables[table][0])
    for key in ("id", "pgpid", "document_id"):
        if key in extra:
            extra[key] = 999
    bigger.tables[table] = bigger.tables[table] + [extra]
    assert exporter.build_sidecar(bigger, tmp_path, "https://fake") == 0
    before = (tmp_path / "pgp.db").read_bytes()

    # Second build: the same table now comes back one row short -- still non-empty, so
    # only the shrink guard can catch it.
    with pytest.raises(RuntimeError) as excinfo:
        exporter.build_sidecar(FakeSupabase(), tmp_path, "https://fake")
    assert table in str(excinfo.value)
    assert "->" in str(excinfo.value), "must be the shrink guard, not the empty-table guard"
    assert (tmp_path / "pgp.db").read_bytes() == before, "the live sidecar must be untouched"
    assert not (tmp_path / "pgp.db.new").exists()


def test_the_shrink_override_is_wired_in(exporter, tmp_path):
    assert exporter.build_sidecar(FakeSupabase(), tmp_path, "https://fake") == 0
    fake = FakeSupabase(documents=DOCUMENTS[:1])
    assert exporter.build_sidecar(fake, tmp_path, "https://fake", allow_shrink=True) == 0
    conn = sqlite3.connect(str(tmp_path / "pgp.db"))
    try:
        assert conn.execute("SELECT COUNT(*) FROM documents").fetchone()[0] == 1
    finally:
        conn.close()
