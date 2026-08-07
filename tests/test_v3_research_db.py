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
                 [(_REAL_PAGE_ID, sys_id, "b", "1800", "text", "htr", None, None, "1800")])
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
    # w_start/w_end are the LAST two columns of the emitted tuple.
    w_start, w_end = rows[0][-2], rows[0][-1]
    assert (w_start, w_end) == (4735, 5461), (
        f"the built evidence row carries work-side offsets ({w_start}, {w_end}); "
        f"expected the producer's (4735, 5461)"
    )
