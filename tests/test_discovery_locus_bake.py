import hashlib
import json
import sqlite3

import pytest

from scripts import build_discovery_sidecar as builder


REF_SHA = "a" * 64


def _source_fixture(tmp_path, *, bad_ordinal=False, invariant_problems=None,
                    labels=("פרק א", "פרק ב")):
    path = tmp_path / "work_divisions.db"
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE locus_work (
          locus_ref_id TEXT PRIMARY KEY, family TEXT NOT NULL, grain TEXT NOT NULL,
          stream_len INTEGER NOT NULL, unit_count INTEGER NOT NULL);
        CREATE TABLE locus_unit (
          locus_ref_id TEXT NOT NULL, unit_ord INTEGER NOT NULL,
          start_offset INTEGER NOT NULL, part_key TEXT NOT NULL,
          label_he TEXT NOT NULL, citation_pos INTEGER,
          PRIMARY KEY (locus_ref_id, unit_ord));
        CREATE TABLE locus_edition (
          locus_ref_id TEXT PRIMARY KEY, title_he TEXT NOT NULL,
          title_original TEXT NOT NULL, author_short TEXT NOT NULL,
          author_full TEXT NOT NULL, publisher TEXT NOT NULL,
          publisher_city TEXT NOT NULL, publisher_year TEXT NOT NULL,
          editor TEXT NOT NULL, edition TEXT NOT NULL);
        """
    )
    conn.executemany(
        "INSERT INTO locus_work VALUES (?, 'sefaria', 'chapter', 200, 2)",
        [("raw:kept",), ("raw:unmapped",)],
    )
    second_ord = 2 if bad_ordinal else 1
    conn.executemany(
        "INSERT INTO locus_unit VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("raw:kept", 0, 0, "ch:1", labels[0], 1),
            ("raw:kept", second_ord, 100, "ch:2", labels[1], 2),
            ("raw:unmapped", 0, 0, "ch:1", "פרק א", 1),
            ("raw:unmapped", 1, 100, "ch:2", "פרק ב", 2),
        ],
    )
    conn.commit()
    conn.close()
    coverage = {
        "reference_corpus_sha256": REF_SHA,
        "works_with_units": 2,
        "units_total": 4,
        "by_family": {"sefaria": 2, "ja": 0, "msource_header": 0, "msource_daf": 0},
        "by_grain": {"chapter": 2},
        "invariant_problems": [] if invariant_problems is None else invariant_problems,
    }
    (tmp_path / "coverage.json").write_text(json.dumps(coverage), encoding="utf-8")
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    return path, digest


def _asset(title="Synthetic Work"):
    conn = sqlite3.connect(":memory:")
    builder.create_schema(conn)
    conn.execute(
        "INSERT INTO works VALUES "
        "(?,'w000001',?,NULL,NULL,'sefaria','public')",
        ("w000001", title),
    )
    return conn


def test_locus_import_rekeys_and_never_stores_raw_reference_ids(tmp_path):
    path, digest = _source_fixture(tmp_path)
    conn = _asset()
    meta = dict(builder.ingest_locus_divisions(
        conn, str(path), {"raw:kept": "w000001"}, REF_SHA, expected_sha256=digest
    ))
    assert conn.execute("SELECT work_id FROM locus_work").fetchall() == [("w000001",)]
    assert conn.execute("SELECT COUNT(*) FROM locus_unit").fetchone()[0] == 2
    assert "raw:" not in " ".join(
        str(value) for row in conn.iterdump() for value in (row,)
    )
    assert meta["locus_unmapped_references"] == "1"


def test_locus_import_rejects_a_bad_pin_before_reading_rows(tmp_path):
    path, _digest = _source_fixture(tmp_path)
    with pytest.raises(ValueError, match="SHA-256"):
        builder.ingest_locus_divisions(
            _asset(), str(path), {}, REF_SHA, expected_sha256="0" * 64
        )


def test_locus_import_selects_pin_by_reference_corpus(tmp_path, monkeypatch):
    path, digest = _source_fixture(tmp_path)
    monkeypatch.setitem(builder.LOCUS_DIVISIONS_BY_REFERENCE_SHA256, REF_SHA, digest)
    meta = dict(builder.ingest_locus_divisions(
        _asset(), str(path), {"raw:kept": "w000001"}, REF_SHA
    ))
    assert meta["locus_divisions_sha256"] == digest

    with pytest.raises(ValueError, match="no approved"):
        builder.ingest_locus_divisions(
            _asset(), str(path), {}, "b" * 64
        )


def test_locus_import_rejects_coverage_invariant_problems(tmp_path):
    path, digest = _source_fixture(tmp_path, invariant_problems=["synthetic"])
    with pytest.raises(ValueError, match="invariant problems"):
        builder.ingest_locus_divisions(
            _asset(), str(path), {}, REF_SHA, expected_sha256=digest
        )


def test_locus_import_rejects_noncontiguous_ordinals(tmp_path):
    path, digest = _source_fixture(tmp_path, bad_ordinal=True)
    with pytest.raises(ValueError, match="ordinals"):
        builder.ingest_locus_divisions(
            _asset(), str(path), {"raw:kept": "w000001"}, REF_SHA,
            expected_sha256=digest,
        )


def test_locus_import_strips_the_work_title_from_the_stored_address(tmp_path):
    """The ASSET carries the reader's address, not the input's qualified one.

    The divisions database stores `ארבעה טורים, יורה דעה א` because
    `discovery_v4_build_reference.py::_locus_label` prepends the work title to every
    chapter address. Every surface that renders a locus renders the work title
    beside it, so shipping that verbatim gives a reader the title twice -- and once
    per rendered run (owner report, 2026-08-19).

    Asserted on `locus_unit.label_he` in the ASSET rather than on the pure helper's
    return value, because the helper passing while the ingestion ignored it is
    exactly the failure this is here to catch: the column is ALSO published directly
    by `get_locus_units_enveloped` for the findings page's address filter.
    """
    path, digest = _source_fixture(
        tmp_path, labels=("ארבעה טורים, יורה דעה א", "ארבעה טורים, יורה דעה ב"))
    conn = _asset(title="ארבעה טורים, יורה דעה")
    builder.ingest_locus_divisions(
        conn, str(path), {"raw:kept": "w000001"}, REF_SHA, expected_sha256=digest
    )
    assert conn.execute(
        "SELECT label_he FROM locus_unit ORDER BY unit_ord"
    ).fetchall() == [("א",), ("ב",)]


def test_locus_import_stores_an_unqualified_address_verbatim(tmp_path):
    """The strip is a no-op on the 571 works whose addresses were already clean.

    The partner of the test above, and the reason it is not enough on its own: an
    ingestion that blanket-shortened every label would satisfy the first assertion
    while destroying `פרק א` for the M-source and REF2 works, which are most of the
    corpus.
    """
    path, digest = _source_fixture(tmp_path)
    conn = _asset(title="רד\"ק על ישעיה")
    builder.ingest_locus_divisions(
        conn, str(path), {"raw:kept": "w000001"}, REF_SHA, expected_sha256=digest
    )
    assert conn.execute(
        "SELECT label_he FROM locus_unit ORDER BY unit_ord"
    ).fetchall() == [("פרק א",), ("פרק ב",)]


def test_materializer_resolves_claim_and_identification_and_fails_closed_out_of_range():
    conn = sqlite3.connect(":memory:")
    builder.create_schema(conn)
    builder.populate_synthetic(conn, "f" * 64)
    claim_id, work_id, identification_id = conn.execute(
        "SELECT dc.claim_id, dc.work_id, di.identification_id "
        "FROM discovery_claim dc "
        "JOIN discovery_evidence de ON de.claim_id=dc.claim_id "
        "JOIN works w ON w.work_id=dc.work_id "
        "JOIN discovery_identification di ON di.sys_id=de.sys_id "
        " AND di.canonical_work_id=w.canonical_work_id "
        "WHERE de.routing_status='shipped' LIMIT 1"
    ).fetchone()
    conn.execute(
        "INSERT INTO locus_work VALUES (?, 'sefaria', 'chapter', 200, 2)", (work_id,)
    )
    conn.executemany(
        "INSERT INTO locus_unit VALUES (?, ?, ?, ?, ?, ?)",
        [(work_id, 0, 0, "ch:1", "פרק א", 1),
         (work_id, 1, 100, "ch:2", "פרק ב", 2)],
    )
    conn.execute(
        "UPDATE discovery_evidence SET w_start=10, w_end=150, matched_letters=140 "
        "WHERE claim_id=?", (claim_id,)
    )
    builder.materialize_locus_labels(conn)
    assert conn.execute(
        "SELECT locus_status, locus_work_id, locus_label FROM discovery_claim "
        "WHERE claim_id=?", (claim_id,)
    ).fetchone() == ("resolved", work_id, "פרק א–ב")
    assert conn.execute(
        "SELECT locus_status FROM discovery_identification WHERE identification_id=?",
        (identification_id,),
    ).fetchone() == ("resolved",)
    assert conn.execute(
        "SELECT piece_ord, start_unit_ord, end_unit_ord "
        "FROM discovery_locus_piece WHERE identification_id=? ORDER BY piece_ord",
        (identification_id,),
    ).fetchall() == [(0, 0, 1)]

    conn.execute(
        "UPDATE discovery_evidence SET w_end=201 WHERE claim_id=?", (claim_id,)
    )
    builder.materialize_locus_labels(conn)
    assert conn.execute(
        "SELECT locus_status, locus_label FROM discovery_claim WHERE claim_id=?",
        (claim_id,),
    ).fetchone() == ("unavailable", None)
    assert conn.execute(
        "SELECT COUNT(*) FROM discovery_locus_piece WHERE identification_id=?",
        (identification_id,),
    ).fetchone() == (0,)
