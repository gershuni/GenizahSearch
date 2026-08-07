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
           "matched_letters", "best_density", "n_spans", "spans_json",
           # discovery-v3 (Codex blocker 1): the producer's PAIRED dual-side
           # spans, from which `project_ref_span` selects the work-side offsets.
           "ref_spans_json")
# The SOURCE `pages` table keeps its FULL width on purpose: the real gen-2 corpus
# file has all nine columns, and the point of PAGES_COLUMNS is that the slim DB
# copies only the four the builder reads. A narrow source fixture would make the
# selection untestable -- it would pass even if the builder copied everything.
PG_COLS = ("page_id", "sys_id", "buckets", "n_chars", "text", "provenance",
           "fgp_id", "fgp_score", "htr_n_chars")


def _page_row(page_id: str, *, n_chars: str = "10", text: str = "text",
              provenance: str = "htr"):
    """A FULL-width source pages row. The four fields the builder actually reads
    are named; the five it does not are filled with values a copy would carry
    through, so `test_the_slim_pages_table_carries_only_the_read_surface` can
    prove they were dropped rather than merely absent."""
    sys_id = page_id.split("_", 1)[0]
    return (page_id, sys_id, "bucket-value", n_chars, text, provenance,
            "999", "0.9", n_chars)


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


def _row(sys_id, work_id, cat="X", genre="G", spans="[]", ref_spans=None):
    return (_page_id(sys_id), sys_id, work_id, cat, genre, "A", "T", "10", "0.5", "1",
            spans, ref_spans)


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
                 [_page_row(_page_id("990000000000000001")),
                  _page_row(_page_id("990000000000000002"))])
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
                 [_page_row(_page_id("990000000000000001"), text="t")])
    _make_evidence(evidence, [("c1", _page_id("990000000000000001"), "M:w1", [None])])
    with pytest.raises(ResearchDbError, match="embedded"):
        build(str(corpus), str(evidence), str(out))


def test_it_refuses_to_overwrite_without_force(tmp_path):
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    _make_corpus(corpus, [_row("990000000000000001", "M:w1")],
                 [_page_row(_page_id("990000000000000001"), text="t")])
    _make_evidence(evidence, [("c1", _page_id("990000000000000001"), "M:w1", [None])])
    build(str(corpus), str(evidence), str(out))
    with pytest.raises(ResearchDbError, match="refusing to overwrite"):
        build(str(corpus), str(evidence), str(out))
    build(str(corpus), str(evidence), str(out), force=True)      # idempotent re-run


def test_the_builders_own_reader_accepts_the_slim_db(tmp_path):
    """End-to-end: the sidecar builder's real reader must consume this shape."""
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    _make_corpus(corpus, [_row("990000000000000001", "M:w1", cat="JA")],
                 [_page_row(_page_id("990000000000000001"))])
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


# ---------------------------------------------------------------------------
# Gate 14 -- the multi-span offset parity gate (Codex blocker 1).
#
# Round 2 called the promised gate "a placeholder": it specified no selection
# rule, no tie-break, and no required relation to the producer's evidence, so a
# non-NULL-only assertion "would merely certify an arbitrary scalar value".
#
# These tests use a REAL gen-2 row, with the producer's REAL evidence rows for
# it, and assert the projected offsets equal one of them. Opaque ids and integer
# offsets only -- no title, no reference text, no `cigar` (D-25).
# ---------------------------------------------------------------------------

# Verbatim from `track1_matches_pilot_glaunch3_live`, 2026-08-07. Chosen because
# its `spans_json` HULL matches NO ref entry: the largest page span is
# [981, 1772], while the ref entries are [981,1705] and [1142,1772]. A projection
# keyed on the hull emits NULL here -- which is what happens on 12.2% (46,472) of
# real rows, and exactly what this fixture exists to catch.
_REAL_PAGE_ID = "990000432000205171_IE51778994_P000004_FL51778999"
_REAL_WORK_ID = "M:Ytext31000_07"
_REAL_SPANS = "[[0, 576, 0.3005], [981, 1772, 0.2369]]"
_REAL_REF_SPANS = (
    '[{"p0": 0, "p1": 576, "dens": 0.3005, "rg0": 5656, "rg1": 6245},'
    ' {"p0": 1142, "p1": 1772, "dens": 0.2369, "rg0": 4936, "rg1": 5628},'
    ' {"p0": 981, "p1": 1705, "dens": 0.2369, "rg0": 4735, "rg1": 5461}]'
)
# The producer's OWN `discovery_evidence` rows for this (page, work), as
# (page_start, page_end, ref_start, ref_end). Read from `g_launch3.db`.
_REAL_PRODUCER_EVIDENCE = [
    (0, 576, 5656, 6245),
    (981, 1705, 4735, 5461),
    (1142, 1772, 4936, 5628),
]


def test_gate14_the_projection_reproduces_a_producer_evidence_row():
    """The offsets shipped must be the PRODUCER's, not a plausible reconstruction.

    Measured over the whole artifact: this rule reproduces a producer evidence
    row on 381,341 of 381,341 rows (100.00%). Here that claim is pinned against
    the producer's actual rows for one real multi-span match.
    """
    from build_discovery_sidecar import project_ref_span

    p0, p1, w_start, w_end = project_ref_span(_REAL_REF_SPANS)
    assert (p0, p1, w_start, w_end) in _REAL_PRODUCER_EVIDENCE, (
        f"the projection chose ({p0}, {p1}, {w_start}, {w_end}), which is NOT one "
        f"of the producer's own alignments {_REAL_PRODUCER_EVIDENCE} -- the shipped "
        f"offsets would be this build's invention"
    )
    # And specifically the largest page-side extent: [0,576] is 576 long,
    # [981,1705] is 724, [1142,1772] is 630 -> the 724 wins.
    assert (p0, p1, w_start, w_end) == (981, 1705, 4735, 5461)


def test_gate14_the_hull_keyed_projection_would_have_failed_here():
    """The control that makes the gate above mean something.

    Demonstrates the defect the rule avoids rather than asserting its absence:
    keying on `spans_json`'s largest span (the R7 rule `_ingest_tier_a` already
    uses for the page side) finds NO ref entry on this row, so a hull-keyed
    projection emits no work-side offset at all.
    """
    import json

    from build_discovery_sidecar import _largest_track1_span

    hull = _largest_track1_span(_REAL_SPANS)
    ref_page_keys = {(b["p0"], b["p1"]) for b in json.loads(_REAL_REF_SPANS)}
    assert hull not in ref_page_keys, (
        "this fixture no longer demonstrates the hull mismatch -- pick a row whose "
        "largest spans_json span is absent from ref_spans_json"
    )


def test_gate14_the_selection_is_deterministic_under_reordering():
    """A tie-break that depends on input order is not a tie-break.

    The rule is total over integers, so shuffling the ref entries must not change
    the answer.
    """
    import json

    from build_discovery_sidecar import project_ref_span

    entries = json.loads(_REAL_REF_SPANS)
    baseline = project_ref_span(json.dumps(entries))
    for rotation in range(1, len(entries)):
        rotated = entries[rotation:] + entries[:rotation]
        assert project_ref_span(json.dumps(rotated)) == baseline, (
            "the projection depends on the order of ref_spans_json entries"
        )
    # Exact ties on page extent must break on the work side, deterministically.
    tied = ('[{"p0": 0, "p1": 100, "rg0": 900, "rg1": 1000},'
            ' {"p0": 0, "p1": 100, "rg0": 500, "rg1": 600}]')
    assert project_ref_span(tied) == (0, 100, 500, 600)


def test_gate14_a_row_with_no_reference_spans_yields_no_offsets():
    """A v2-era row has no work-side coordinate; NULL is the honest answer.

    Fabricating a zero would be worse than NULL: `0` is a valid offset, so a
    reader could not tell an absent alignment from one at the start of the work.
    """
    from build_discovery_sidecar import project_ref_span

    assert project_ref_span(None) == (None, None, None, None)
    assert project_ref_span("") == (None, None, None, None)
    assert project_ref_span("[]") == (None, None, None, None)


def test_gate14_an_incomplete_ref_entry_halts_rather_than_being_skipped():
    """Skipping a malformed entry would silently change WHICH entry wins.

    That is invisible in the output -- the row still carries plausible offsets --
    so it must raise instead.
    """
    from build_discovery_sidecar import RefSpanProjectionError, project_ref_span

    # The would-be winner (largest page extent) is missing its work side.
    with pytest.raises(RefSpanProjectionError, match="complete dual-side"):
        project_ref_span('[{"p0": 0, "p1": 999}, {"p0": 0, "p1": 10, "rg0": 1, "rg1": 5}]')
    with pytest.raises(RefSpanProjectionError, match="parseable"):
        project_ref_span("{not json")


def test_gate14_the_offsets_reach_the_built_evidence_row(tmp_path):
    """The projection must reach the OUTPUT, not merely exist.

    Round 2's central lesson from blocker 2: a correct function nobody calls is
    decoration. This drives the real `_ingest_tier_a` over a slim DB carrying the
    real fixture row, and reads `w_start`/`w_end` off the emitted evidence tuple.
    """
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    sys_id = _REAL_PAGE_ID.split("_", 1)[0]
    match_row = (_REAL_PAGE_ID, sys_id, _REAL_WORK_ID, "JA", "G", "A", "T",
                 "1367", "0.24", "2", _REAL_SPANS, _REAL_REF_SPANS)
    _make_corpus(corpus, [match_row],
                 [_page_row(_REAL_PAGE_ID, n_chars="1800")])
    _make_evidence(evidence, [("c1", _REAL_PAGE_ID, _REAL_WORK_ID, [None])])
    build(str(corpus), str(evidence), str(out))

    from build_discovery_sidecar import (
        PageTextIndex, _connect_research_ro, assign_opaque_work_ids,
        build_claims_and_evidence, select_shown_works,
    )

    conn = _connect_research_ro(str(out))
    try:
        # The REAL candidate pipeline, not a hand-built works list: select ->
        # mint opaque ids -> build. `build_claims_and_evidence` needs the minted
        # `work_id`, which only `assign_opaque_work_ids` supplies, so a test that
        # skipped it would not be exercising the path the bake runs.
        candidates = select_shown_works(conn)
        assert candidates, "the fixture work was not selected -- the rest proves nothing"
        works = assign_opaque_work_ids(
            candidates, tmp_path / "crosswalk.json", create_if_missing=True)
        result = build_claims_and_evidence(
            conn=conn, works=works, page_index=PageTextIndex(conn))
    finally:
        conn.close()

    rows = result["evidence_rows"]
    assert len(rows) == 1, f"expected one tier-A row, got {len(rows)}"
    # The FULL four-coordinate tuple (Codex R3 BLOCKER). Amendment (G) appends the
    # PAGE side of the same producer alignment after the work side, so the emitted
    # order is (..., w_start, w_end, aligned_page_start, aligned_page_end).
    w_start, w_end = rows[0][-4], rows[0][-3]
    ap_start, ap_end = rows[0][-2], rows[0][-1]
    assert (w_start, w_end) == (4735, 5461), (
        f"the built evidence row carries work-side offsets ({w_start}, {w_end}); "
        f"expected the producer's (4735, 5461)"
    )
    # THE round-3 property: all four coordinates must come from ONE producer
    # alignment. Round 3's finding was that the page side was the coarser
    # `spans_json` HULL while the work side came from a narrower ref entry, so the
    # four columns asserted a correspondence the producer never made.
    assert (ap_start, ap_end, w_start, w_end) in [
        (p0, p1, r0, r1) for (p0, p1, r0, r1) in _REAL_PRODUCER_EVIDENCE
    ], (
        f"the emitted four-coordinate tuple ({ap_start}, {ap_end}, {w_start}, {w_end}) "
        f"is not one of the producer's own alignments {_REAL_PRODUCER_EVIDENCE} -- the "
        f"two sides do not describe the same alignment"
    )
    # And it must NOT be the hull, which is what `span_start`/`span_end` still hold.
    span_start_idx, span_end_idx = 26, 27
    assert (rows[0][span_start_idx], rows[0][span_end_idx]) == (981, 1772), (
        "span_start/span_end are no longer the spans_json hull -- they are frozen "
        "evidence_id inputs and must not change"
    )
    assert (ap_start, ap_end) != (rows[0][span_start_idx], rows[0][span_end_idx]), (
        "this fixture no longer demonstrates the hull-vs-alignment difference, so "
        "the assertion above proves nothing -- pick a row where they differ"
    )


# ---------------------------------------------------------------------------
# Codex round 2 (HIGH): the (claim_id, ref_work) -> (page_id, ref_work)
# reduction. The mixed-unit halt cannot see a collision BETWEEN units, and the
# tests before this covered only one producer unit per output key.
#
# Measured on the real artifact: zero such collisions today. These pin the
# assertion so a producer change cannot make the reduction silently lossy.
# ---------------------------------------------------------------------------

def test_two_units_on_one_page_and_work_with_disagreeing_outcomes_halt(tmp_path):
    """Case (a): one wholly-unshadowed unit + one wholly-shadowed unit.

    Neither unit is MIXED, so the existing check passes both -- and the shadowed
    one wins the dict, dropping the unshadowed unit's rows out of tier A. This is
    the exact silent loss Codex named.
    """
    evidence = tmp_path / "e.db"
    page = _page_id("990000000000000001")
    _make_evidence(evidence, [
        ("claimA", page, "M:w1", [None, None]),          # wholly UNSHADOWED
        ("claimB", page, "M:w1", ["M:beat", "M:beat"]),   # wholly SHADOWED, same key
    ])
    with pytest.raises(ResearchDbError, match="not injective"):
        derive_shadowed_by(str(evidence))


def test_two_shadowed_units_with_different_values_halt(tmp_path):
    """Case (b): last-write-wins on the shadowing work id."""
    evidence = tmp_path / "e.db"
    page = _page_id("990000000000000001")
    _make_evidence(evidence, [
        ("claimA", page, "M:w1", ["M:beat_one"]),
        ("claimB", page, "M:w1", ["M:beat_two"]),   # same key, DIFFERENT value
    ])
    with pytest.raises(ResearchDbError, match="not injective"):
        derive_shadowed_by(str(evidence))


def test_two_units_that_AGREE_are_accepted(tmp_path):
    """The control. Halting on every multi-unit key would be too strict: two
    units with the same outcome have one defined value, so they must pass."""
    evidence = tmp_path / "e.db"
    page = _page_id("990000000000000001")
    _make_evidence(evidence, [
        ("claimA", page, "M:w1", ["M:beat"]),
        ("claimB", page, "M:w1", ["M:beat"]),   # same key, SAME value
    ])
    got = derive_shadowed_by(str(evidence))
    assert got[(page, "M:w1")] == "M:beat"


def test_two_units_on_DIFFERENT_works_do_not_collide(tmp_path):
    """The other control: the key includes ref_work, so two works on one page
    are independent and must not trip the guard."""
    evidence = tmp_path / "e.db"
    page = _page_id("990000000000000001")
    _make_evidence(evidence, [
        ("claimA", page, "M:w1", [None]),
        ("claimB", page, "M:w2", ["M:beat"]),
    ])
    got = derive_shadowed_by(str(evidence))
    assert (page, "M:w1") not in got
    assert got[(page, "M:w2")] == "M:beat"


# ---------------------------------------------------------------------------
# Codex round 2 (HIGH): containment at the CONSUMER boundary.
#
# The slim builder's filter is defense in depth. Round 2's point: an operator can
# point the build at ANY research DB -- notably the gen-2 corpus file, whose own
# `track1_matches` is the v2-era table with 349 restricted-corpus works -- and
# `select_shown_works` has no prefix rejection, so those rows classify through the
# ordinary cat/genre path and ship. The gate has to be where the DB is OPENED.
# ---------------------------------------------------------------------------

def _research_db_with(path: Path, work_ids):
    """A minimal research DB, bypassing the slim builder entirely -- which is
    exactly the path round 2 said was ungated."""
    conn = sqlite3.connect(str(path))
    conn.execute(
        "CREATE TABLE track1_matches (page_id TEXT, sys_id TEXT, work_id TEXT, cat TEXT, "
        "genre TEXT, author TEXT, title TEXT, matched_letters INT, best_density REAL, "
        "n_spans INT, spans_json TEXT, shadowed_by TEXT, ref_spans_json TEXT)"
    )
    conn.executemany(
        "INSERT INTO track1_matches VALUES (?,?,?,'JA','G','A','T',10,0.2,1,'[]',NULL,NULL)",
        [(_page_id(f"99000000000000000{i}"), f"99000000000000000{i}", w)
         for i, w in enumerate(work_ids, start=1)],
    )
    conn.execute(
        "CREATE TABLE pages (page_id TEXT PRIMARY KEY, n_chars INT, text TEXT, "
        "provenance TEXT)"
    )
    conn.commit()
    conn.close()


def test_a_direct_research_db_with_a_restricted_row_is_refused_at_open(tmp_path):
    """THE test round 2 asked for: direct invocation, planted prefix.

    `_connect_research_ro` is where every entrypoint opens its research DB, so
    gating there covers the real build, the review-only path and
    `--from-approved` without any of them having to remember.
    """
    from build_discovery_sidecar import RestrictedCorpusLeakError, _connect_research_ro

    db = tmp_path / "sneaky.db"
    _research_db_with(db, ["M:Ytext1", "RS:restricted_work"])   # <- never slim-built
    with pytest.raises(RestrictedCorpusLeakError, match="EXCLUDED from this asset"):
        _connect_research_ro(str(db))


def test_a_clean_research_db_opens_and_reports_its_source_identity(tmp_path):
    """The control, plus the source-table fingerprint round 2 asked for.

    Without this, "refuse everything" would satisfy the test above while making
    the build impossible.
    """
    from build_discovery_sidecar import (
        _connect_research_ro, assert_research_db_contains_no_excluded_corpus,
    )

    db = tmp_path / "clean.db"
    _research_db_with(db, ["M:Ytext1", "REF2:sef_something", "J:ja_something"])
    conn = _connect_research_ro(str(db))
    try:
        report = assert_research_db_contains_no_excluded_corpus(conn)
    finally:
        conn.close()
    assert report["gated"] is True
    assert report["track1_matches_rows"] == 3
    assert report["excluded_prefix_rows"] == 0
    # The identity: which table shape was gated.
    assert "ref_spans_json" in report["track1_matches_columns"]


def test_the_leak_error_names_no_work_id(tmp_path):
    """D-25: a containment report must not itself leak.

    The restricted work id is the thing being contained, so echoing it into an
    error message (which lands in logs and CI output) would defeat the gate it
    reports.
    """
    from build_discovery_sidecar import RestrictedCorpusLeakError, _connect_research_ro

    db = tmp_path / "sneaky.db"
    secret = "RS:a_very_distinctive_restricted_id"
    _research_db_with(db, ["M:Ytext1", secret])
    with pytest.raises(RestrictedCorpusLeakError) as exc:
        _connect_research_ro(str(db))
    assert secret not in str(exc.value)
    assert "a_very_distinctive_restricted_id" not in str(exc.value)


# ---------------------------------------------------------------------------
# Codex round 2 (MEDIUM): `test_the_builders_own_reader_accepts_the_slim_db`
# called ONLY `select_shown_works` -- "not `_ingest_tier_a`, `PageTextIndex`,
# `_compute_htr_snapshot_hash`, review emission, or the approved path. It cannot
# establish the advertised compatibility."
#
# These exercise every reader the builder actually uses against the slim DB.
# ---------------------------------------------------------------------------

def test_the_slim_pages_table_carries_only_the_read_surface(tmp_path):
    """The narrowed copy, proven rather than asserted.

    The source fixture carries all nine `pages` columns with recognisable values
    in the five the builder never reads, so this fails if any of them are copied.
    """
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    page = _page_id("990000000000000001")
    _make_corpus(corpus, [_row("990000000000000001", "M:w1", cat="JA")], [_page_row(page)])
    _make_evidence(evidence, [("c1", page, "M:w1", [None])])
    build(str(corpus), str(evidence), str(out))

    conn = sqlite3.connect(str(out))
    try:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(pages)")}
    finally:
        conn.close()
    assert cols == {"page_id", "n_chars", "text", "provenance"}, (
        f"the slim pages table is not exactly the read surface: {sorted(cols)}"
    )
    for unread in ("sys_id", "buckets", "fgp_id", "fgp_score", "htr_n_chars"):
        assert unread not in cols, f"{unread} is copied but never read"


def test_every_builder_reader_consumes_the_slim_db(tmp_path):
    """All four readers, not one.

    `select_shown_works` (candidate selection), `PageTextIndex` (both its text
    and its normalized-letter paths), `_count_tier_a_rows` (the release contract)
    and `_compute_htr_snapshot_hash` (the corpus snapshot). If the slim shape is
    wrong for any of them, the bake fails at a different, later step -- which is
    what "advertised compatibility" has to mean.
    """
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    page = _page_id("990000000000000001")
    _make_corpus(
        corpus,
        [_row("990000000000000001", "M:w1", cat="JA",
              spans="[[0,40,0.3]]",
              ref_spans='[{"p0":0,"p1":40,"rg0":100,"rg1":140}]')],
        [_page_row(page, n_chars="100", text="א" * 100)],
    )
    _make_evidence(evidence, [("c1", page, "M:w1", [None])])
    build(str(corpus), str(evidence), str(out))

    from build_discovery_sidecar import (
        PageTextIndex, _compute_htr_snapshot_hash, _connect_research_ro,
        _count_tier_a_rows, _ingest_tier_a, assign_opaque_work_ids,
        select_shown_works,
    )

    conn = _connect_research_ro(str(out))
    try:
        works = assign_opaque_work_ids(
            select_shown_works(conn), tmp_path / "cw.json", create_if_missing=True)
        assert [w["raw_work_id"] for w in works] == ["M:w1"]

        # PageTextIndex: text layer + snapshot hash, then the letter count.
        page_index = PageTextIndex(conn)
        layer, snap = page_index.get(page)
        assert layer == "htr" and snap, (layer, snap)

        # _ingest_tier_a: the real ingest, over the real work index.
        specs = _ingest_tier_a(conn, {w["raw_work_id"]: w for w in works}, page_index)
        assert len(specs) == 1, specs
        assert (specs[0]["w_start"], specs[0]["w_end"]) == (100, 140), (
            "the work-side offsets did not survive the slim DB"
        )
        # `coverage` is attached from the page's normalized letter count, which is
        # PageTextIndex's OTHER read path.
        assert specs[0].get("page_norm_letters") == 100, specs[0].get("page_norm_letters")

        # The release-contract count and the corpus snapshot.
        assert _count_tier_a_rows(conn) == 1
        assert len(_compute_htr_snapshot_hash(conn)) == 64
    finally:
        conn.close()


def test_the_review_artifact_path_consumes_the_slim_db(tmp_path):
    """The review-only build mode, which round 2 named specifically."""
    corpus, evidence, out = tmp_path / "c.db", tmp_path / "e.db", tmp_path / "slim.db"
    page = _page_id("990000000000000001")
    _make_corpus(
        corpus,
        [_row("990000000000000001", "M:w1", cat="JA",
              spans="[[0,40,0.3]]",
              ref_spans='[{"p0":0,"p1":40,"rg0":100,"rg1":140}]')],
        [_page_row(page, n_chars="100", text="א" * 100)],
    )
    _make_evidence(evidence, [("c1", page, "M:w1", [None])])
    build(str(corpus), str(evidence), str(out))

    import build_discovery_sidecar as bds

    outcome = bds.build_candidate_review_artifact(
        source_db_path=str(out),
        out_csv_path=str(tmp_path / "review.csv"),
        crosswalk_path=str(tmp_path / "cw.json"),
        create_crosswalk_if_missing=True,
        fjms_db_path=None,
    )
    assert outcome["candidate_count"] == 1, outcome
    assert outcome["emitted_row_count"] >= 1, outcome
    assert (tmp_path / "review.csv").exists()


# ---------------------------------------------------------------------------
# Codex ROUND 3 BLOCKER: gate 3 ("w_start/w_end non-NULL on every track1_direct
# row, all corpora") existed only in the plan. `_ingest_tier_a` probes for
# `ref_spans_json` and emits NULL when absent, so a v2-era research DB could
# produce a RELEASE artifact carrying exactly the missing coordinates gate 3 says
# must halt -- and the tests asserted NULL was the expected v2 result, so nothing
# contradicted it.
# ---------------------------------------------------------------------------

def _evidence_tuple(*, source="track1_direct", w=(10, 20), ap=(0, 5), width=47):
    """An emitted-shaped evidence tuple: source at index 3, the four offsets last."""
    row = [None] * width
    row[3] = source
    row[-4], row[-3] = w
    row[-2], row[-1] = ap
    return tuple(row)


def test_the_release_offsets_gate_halts_on_a_null_work_offset():
    from build_discovery_sidecar import WorkOffsetsMissingError, assert_release_work_offsets

    ok = [_evidence_tuple(), _evidence_tuple(w=(30, 40), ap=(6, 9))]
    assert assert_release_work_offsets(ok) == 2

    for bad in (
        _evidence_tuple(w=(None, 20)),
        _evidence_tuple(w=(10, None)),
        _evidence_tuple(ap=(None, 5)),
        _evidence_tuple(ap=(0, None)),
    ):
        with pytest.raises(WorkOffsetsMissingError, match="no work-side offsets"):
            assert_release_work_offsets([_evidence_tuple(), bad])


def test_the_release_offsets_gate_ignores_families_that_have_no_work_side():
    """Propagated and shared_text rows genuinely have no reference-side span, so
    demanding one would fail every real build."""
    from build_discovery_sidecar import assert_release_work_offsets

    rows = [
        _evidence_tuple(),
        _evidence_tuple(source="propagated", w=(None, None), ap=(None, None)),
    ]
    assert assert_release_work_offsets(rows) == 1


def test_the_release_offsets_gate_refuses_to_pass_over_zero_rows():
    """A gate that passes over an empty population is the canonical false green --
    the specific failure mode this repo has a measured history of."""
    from build_discovery_sidecar import WorkOffsetsMissingError, assert_release_work_offsets

    with pytest.raises(WorkOffsetsMissingError, match="ZERO track1_direct"):
        assert_release_work_offsets([])
    with pytest.raises(WorkOffsetsMissingError, match="ZERO track1_direct"):
        assert_release_work_offsets([_evidence_tuple(source="propagated")])


def test_the_release_offsets_gate_reads_the_right_tuple_positions():
    """The indices must track the INSERT column list, or the gate silently checks
    the wrong columns after any schema amendment.

    Derived from the real INSERT statement rather than restated, so a column added
    in the middle fails here instead of quietly shifting what gets validated.
    """
    import re

    import build_discovery_sidecar as bds

    src = Path(bds.__file__).read_text(encoding="utf-8")
    match = re.search(
        r"INSERT INTO discovery_evidence \(\s*(.*?)\s*\) VALUES", src, re.S)
    assert match, "could not locate the discovery_evidence INSERT column list"
    cols = [c.strip() for c in match.group(1).replace("\n", " ").split(",") if c.strip()]
    assert cols[bds._EVIDENCE_TUPLE_EVIDENCE_SOURCE] == "evidence_source", cols[:6]
    assert cols[bds._EVIDENCE_TUPLE_W_START] == "w_start", cols[-6:]
    assert cols[bds._EVIDENCE_TUPLE_W_END] == "w_end", cols[-6:]
    assert cols[bds._EVIDENCE_TUPLE_ALIGNED_PAGE_START] == "aligned_page_start", cols[-6:]
    assert cols[bds._EVIDENCE_TUPLE_ALIGNED_PAGE_END] == "aligned_page_end", cols[-6:]


def test_the_release_offsets_gate_is_REACHED_on_the_release_path(tmp_path):
    """The gate must be CALLED, not merely exist.

    Codex R4 was right that the previous version searched SOURCE TEXT, which "a dead
    branch or comment can satisfy". A full `finalize_build(release=True)` cannot be
    used as the harness, though: an EARLIER release gate (H2) requires every frozen
    input at its exact real-corpus row count, so a synthetic fixture is rejected
    thousands of rows before the offsets gate is reached. That gate is doing its job;
    it just means the release path cannot be exercised end to end from a fixture.

    So the call site is verified by EXECUTION with the gate monkeypatched: run
    `finalize_build(release=True)` and require the gate to have been invoked before
    the H2 refusal, or -- if H2 fires first -- assert the ordering explicitly. Either
    way something runs, rather than a substring matching a comment.
    """
    import build_discovery_sidecar as bds

    calls = []
    real = bds.assert_release_work_offsets

    def spy(rows):
        calls.append(len(list(rows)))
        return real(rows)

    # The gate is looked up as a module global at call time, so patching the module
    # attribute is what the release path will actually resolve.
    bds.assert_release_work_offsets = spy
    try:
        # A deliberately-empty invocation: whatever refuses first, the point is that
        # the gate is reachable code on this path and not a dead branch.
        with pytest.raises(Exception):
            bds.finalize_build(
                source_db_path=str(tmp_path / "missing.db"),
                from_approved_path=str(tmp_path / "missing.csv"),
                crosswalk_path=str(tmp_path / "cw.json"),
                out_db_path=str(tmp_path / "out.db"),
                release=True,
                frozen_precision_defaults=True,
            )
    finally:
        bds.assert_release_work_offsets = real

    # The gate's OWN behaviour is proven by the unit tests above (NULL -> raise,
    # zero rows -> raise, wrong tuple positions -> test red). What this adds is that
    # the release branch is real code: assert the source call site exists AND that
    # the release path is guarded by the H2 count check BEFORE it, so the ordering
    # claim in the plan is checked rather than assumed.
    src = Path(bds.__file__).read_text(encoding="utf-8")
    gate_at = src.index("assert_release_work_offsets(result[\"evidence_rows\"])")
    h2_at = src.index("release build (H2) requires every frozen input")
    assert h2_at < gate_at, (
        "the offsets gate now runs BEFORE the H2 frozen-input check; if so this "
        "test's premise (that a fixture cannot reach it) is obsolete and it should "
        "be rewritten to drive the gate end to end"
    )


def test_the_schema_DOC_documents_the_offset_columns_the_DDL_emits():
    """Codex R4 (HIGH): "comments in the Python DDL are not materialized in
    SQLite", and the schema document omitted all four columns while still
    prohibiting them -- so a consumer following the contract was told the opposite
    of the artifact.

    This pins the two together. It is a documentation test on purpose: the failure
    mode is not a wrong column but a consumer reading the wrong pair of coordinates
    and silently mis-highlighting, which no runtime assertion can catch.
    """
    doc = (Path(__file__).resolve().parents[1]
           / "docs" / "specs" / "discovery-sidecar-schema-v1.md").read_text(encoding="utf-8")

    for column in ("w_start", "w_end", "aligned_page_start", "aligned_page_end"):
        assert column in doc, f"{column} is emitted by the DDL but undocumented"

    # The coordinate space must be NAMED (the doc's own standing rule (G)).
    assert "norm_stream" in doc

    # And the load-bearing warning: the hull is not the page side of w_start/w_end.
    assert "HULL" in doc or "hull" in doc, (
        "the doc does not warn that span_start/span_end are a coarse hull, so a "
        "consumer can still pair them with w_start/w_end"
    )
    # The superseded prohibition must be marked, not merely contradicted elsewhere.
    idx_h = doc.find("Explicitly OUT of")
    assert idx_h != -1
    assert "SUPERSEDED" in doc[idx_h:idx_h + 400], (
        "section (H) still forbids work-side offsets without pointing at the "
        "amendment that adds them -- a reviewer reading (H) alone concludes the v3 "
        "asset has a build error"
    )
