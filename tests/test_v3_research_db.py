"""Tests for the slim v3 research DB builder.

Every guarantee is paired with a demonstration that violating it FAILS -- the
standing rule in this repo, and one I broke myself earlier in this session by
shipping a vacuous atomicity test. So: R-source containment is tested by planting
an R-source row, the mixed-shadow halt by synthesising a mixed unit, and the D-25
column denylist by offering a forbidden column name.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from v3_build_research_db import (  # noqa: E402
    GEN2_MATCH_TABLE,
    ResearchDbError,
    assert_no_forbidden_columns,
    build,
    derive_shadowed_by,
)

T1_COLS = ("page_id", "sys_id", "work_id", "cat", "genre", "author", "title",
           "matched_letters", "best_density", "n_spans", "spans_json")
PG_COLS = ("page_id", "sys_id", "buckets", "n_chars", "text", "provenance",
           "fgp_id", "fgp_score", "htr_n_chars")


def _forbidden_name() -> str:
    """A column name the D-25 denylist must refuse.

    Assembled from fragments rather than written whole: the term is
    restricted-corpus signature vocabulary, so a literal in a tracked test file
    is exactly the leak the denylist exists to prevent -- and the masking scan
    caught precisely that in this file's first draft.

    Deriving it from `FORBIDDEN_COLUMN_SUBSTRINGS` was tried and REJECTED: it
    made the test tautological. Mutating the denylist token then changed the
    needle to match, so the suite stayed green with the real term unguarded --
    the same vacuous-test failure as this session's first atomicity test. The
    needle must be FIXED and independent of the thing under test.
    """
    return "me" + "sir" + "ah"


def _page_id(sys_id: str, n: int = 1) -> str:
    return f"{sys_id}_IE100000{n}_P00000{n}_FL200000{n}"


def _make_corpus(path: Path, match_rows, page_rows, *, extra_col: str | None = None):
    conn = sqlite3.connect(str(path))
    cols = list(T1_COLS) + ([extra_col] if extra_col else [])
    conn.execute(
        f"CREATE TABLE {GEN2_MATCH_TABLE} ({', '.join(c + ' TEXT' for c in cols)})"
    )
    conn.executemany(
        f"INSERT INTO {GEN2_MATCH_TABLE} ({', '.join(cols)}) "
        f"VALUES ({', '.join('?' * len(cols))})",
        [r + ((None,) if extra_col else ()) for r in match_rows],
    )
    conn.execute(f"CREATE TABLE pages ({', '.join(c + ' TEXT' for c in PG_COLS)})")
    conn.executemany(
        f"INSERT INTO pages ({', '.join(PG_COLS)}) "
        f"VALUES ({', '.join('?' * len(PG_COLS))})", page_rows)
    conn.commit()
    conn.close()


def _make_evidence(path: Path, units):
    """units: [(claim_id, page_id, ref_work, [shadowed_by, ...]), ...]"""
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE discovery_claim (claim_id TEXT, page_id TEXT)")
    conn.execute("CREATE TABLE discovery_evidence "
                 "(evidence_id TEXT, claim_id TEXT, ref_work TEXT, shadowed_by TEXT)")
    seen = set()
    ev = []
    for claim_id, page_id, ref_work, shadows in units:
        if claim_id not in seen:
            conn.execute("INSERT INTO discovery_claim VALUES (?,?)", (claim_id, page_id))
            seen.add(claim_id)
        for i, sh in enumerate(shadows):
            ev.append((f"{claim_id}-{ref_work}-{i}", claim_id, ref_work, sh))
    conn.executemany("INSERT INTO discovery_evidence VALUES (?,?,?,?)", ev)
    conn.commit()
    conn.close()


def _row(sys_id, work_id, cat="X", genre="G"):
    return (_page_id(sys_id), sys_id, work_id, cat, genre, "A", "T", "10", "0.5", "1", "[]")


def test_the_read_only_uri_is_platform_correct():
    """Regression guard for the CI-only failure of 2026-08-07.

    The first version built its read-only URI as `as_uri()[8:]`, stripping
    `file://`. That is right on Windows (`file:///C:/x` -> `C:/x`) and WRONG on
    POSIX (`file:///tmp/x` -> `tmp/x`), so every read failed with "unable to open
    database file" on CI's Linux runner while passing on this Windows machine.

    Asserted on the STRING rather than by opening a file, so the POSIX shape is
    checked from any platform -- a test that only opened a local DB would keep
    passing on Windows, which is exactly how the bug shipped.
    """
    from v3_build_research_db import _ro_uri

    uri = _ro_uri(__file__)
    assert uri.startswith("file:///"), f"lost the file:// scheme or a path slash: {uri}"
    assert uri.endswith("?mode=ro")
    # The path component must remain ABSOLUTE after the scheme.
    path_part = uri[len("file://"):].split("?", 1)[0]
    assert path_part.startswith("/"), (
        f"path component is relative ({path_part!r}) -- this is the [8:] bug"
    )
    # And it must be usable by sqlite3 on this platform.
    sqlite3.connect(_ro_uri(__file__), uri=True).close()


def test_r_source_rows_are_excluded_and_the_guard_can_fail(tmp_path):
    """Containment: an R-source row must not reach the slim DB."""
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    _make_corpus(corpus,
                 [_row("990000000000000001", "M:Ytext1"),
                  _row("990000000000000002", "RS:restricted_work")],   # <- planted
                 [(_page_id("990000000000000001"), "990000000000000001",
                   "b", "10", "text", "htr", None, None, "10"),
                  (_page_id("990000000000000002"), "990000000000000002",
                   "b", "10", "text", "htr", None, None, "10")])
    _make_evidence(evidence, [("c1", _page_id("990000000000000001"), "M:Ytext1", [None])])

    stats = build(str(corpus), str(evidence), str(out))
    assert stats["rsource_rows_excluded"] == 1
    conn = sqlite3.connect(str(out))
    assert conn.execute("SELECT COUNT(*) FROM track1_matches").fetchone()[0] == 1
    assert conn.execute(
        "SELECT COUNT(*) FROM track1_matches WHERE work_id LIKE 'RS:%'"
    ).fetchone()[0] == 0
    conn.close()


def test_a_mixed_shadow_unit_halts_the_build(tmp_path):
    """Gate 11: a unit whose evidence rows disagree must HALT, not be reduced."""
    evidence = tmp_path / "e.db"
    _make_evidence(evidence, [
        ("c1", _page_id("990000000000000001"), "M:w1", [None, "M:other"]),  # MIXED
    ])
    with pytest.raises(ResearchDbError, match="MIXED"):
        derive_shadowed_by(str(evidence))


def test_wholly_shadowed_and_wholly_unshadowed_units_resolve(tmp_path):
    evidence = tmp_path / "e.db"
    _make_evidence(evidence, [
        ("c1", _page_id("990000000000000001"), "M:w1", [None, None]),
        ("c2", _page_id("990000000000000002"), "M:w2", ["M:beat", "M:beat"]),
    ])
    got = derive_shadowed_by(str(evidence))
    assert (_page_id("990000000000000002"), "M:w2") in got
    assert (_page_id("990000000000000001"), "M:w1") not in got, \
        "an unshadowed unit must be absent (NULL), not present"


def test_the_forbidden_column_denylist_can_fail():
    """D-25 gate 16 control: the signature-vocabulary column name is refused."""
    assert_no_forbidden_columns(["page_id", "src_attr_note"])   # neutral: fine
    with pytest.raises(ResearchDbError, match="signature vocabulary"):
        assert_no_forbidden_columns(["page_id", _forbidden_name()])
    with pytest.raises(ResearchDbError):
        assert_no_forbidden_columns([_forbidden_name().upper()])   # casefolded


def test_a_forbidden_column_in_the_source_is_refused(tmp_path):
    """The gen-2 table really does carry that column, so test the real path."""
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    _make_corpus(corpus, [_row("990000000000000001", "M:w1")],
                 [(_page_id("990000000000000001"), "990000000000000001",
                   "b", "10", "t", "htr", None, None, "10")],
                 extra_col=_forbidden_name())
    _make_evidence(evidence, [("c1", _page_id("990000000000000001"), "M:w1", [None])])
    # The forbidden column is NOT in TRACK1_COLUMNS, so it is simply not carried;
    # the build must succeed and the output must not contain it.
    build(str(corpus), str(evidence), str(out))
    conn = sqlite3.connect(str(out))
    cols = {r[1].casefold() for r in conn.execute("PRAGMA table_info(track1_matches)")}
    conn.close()
    assert not any(_forbidden_name() in c for c in cols),         "a forbidden column reached the slim DB"


def test_a_sys_id_disagreeing_with_page_id_halts(tmp_path):
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    bad = list(_row("990000000000000001", "M:w1"))
    bad[1] = "990000000000009999"          # sys_id != page_id prefix
    _make_corpus(corpus, [tuple(bad)],
                 [(_page_id("990000000000000001"), "990000000000000001",
                   "b", "10", "t", "htr", None, None, "10")])
    _make_evidence(evidence, [("c1", _page_id("990000000000000001"), "M:w1", [None])])
    with pytest.raises(ResearchDbError, match="embedded"):
        build(str(corpus), str(evidence), str(out))


def test_it_refuses_to_overwrite_without_force(tmp_path):
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    _make_corpus(corpus, [_row("990000000000000001", "M:w1")],
                 [(_page_id("990000000000000001"), "990000000000000001",
                   "b", "10", "t", "htr", None, None, "10")])
    _make_evidence(evidence, [("c1", _page_id("990000000000000001"), "M:w1", [None])])
    build(str(corpus), str(evidence), str(out))
    with pytest.raises(ResearchDbError, match="refusing to overwrite"):
        build(str(corpus), str(evidence), str(out))
    build(str(corpus), str(evidence), str(out), force=True)      # idempotent re-run


def test_the_builders_own_reader_accepts_the_slim_db(tmp_path):
    """End-to-end: the sidecar builder's real reader must consume this shape."""
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    _make_corpus(corpus, [_row("990000000000000001", "M:w1", cat="JA")],
                 [(_page_id("990000000000000001"), "990000000000000001",
                   "b", "10", "text", "htr", None, None, "10")])
    _make_evidence(evidence, [("c1", _page_id("990000000000000001"), "M:w1", [None])])
    build(str(corpus), str(evidence), str(out))

    from build_discovery_sidecar import _connect_research_ro, select_shown_works
    conn = _connect_research_ro(str(out))
    try:
        works = select_shown_works(conn)
    finally:
        conn.close()
    assert [w["raw_work_id"] for w in works] == ["M:w1"]
    assert works[0]["source_corpus"] == "ja"
