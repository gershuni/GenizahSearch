# -*- coding: utf-8 -*-
"""Amendment 2026-08-13 (V): matrix step 3's offset-keyed region source.

Step 3 has been in the frozen matrix since A0a-2 and has never been able to fire:
`iter_relation_inputs` hardcoded its input to `None` because the only addressing
scheme on offer was the locus unit, which needs the D-track import. This file
owns the band source that replaces that dependency — the footprint classifier,
its fail-closed tri-state, the ingest's four refusals, and the two verifier gates.

**The coverage trap this file must not fall into.** The synthetic asset has
`w_start`/`w_end` NULL on every one of its 24 evidence rows, so a band test that
merely builds it and switches step 3 on would find every footprint "unplaceable",
demote nothing, and pass while exercising no branch at all. Every test below that
needs a placed footprint drives the offsets explicitly, and
`test_the_synthetic_asset_has_no_work_offsets_at_all` pins the limitation so a
future fixture change has to come here and update the claim.
"""
from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
for _p in (str(REPO_ROOT), str(REPO_ROOT / "scripts")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import build_discovery_sidecar as sidecar_build  # noqa: E402
import discovery_ids as ids  # noqa: E402
import verify_discovery_sidecar as verify_mod  # noqa: E402
from shared import discovery_relation_matrix as matrix  # noqa: E402

REGION_ON = matrix.MatrixParameterization(region_active=True, quoter_threshold=None)
SHA = "a" * 64


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _build_synthetic(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        sidecar_build.populate_synthetic(conn, source_db_hash="region-band-test")
        conn.commit()
    finally:
        conn.close()


def _meta(conn: sqlite3.Connection) -> dict:
    return {k: v for k, v in conn.execute("SELECT key, value FROM meta")}


def _set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute("DELETE FROM meta WHERE key = ?", (key,))
    conn.execute("INSERT INTO meta (key, value) VALUES (?, ?)", (key, value))


def _sync_band_count(conn: sqlite3.Connection) -> None:
    """Keep the release-contract count key honest after a direct band edit, so a
    test that means to exercise one gate is not answered by another."""
    (n,) = conn.execute("SELECT COUNT(*) FROM discovery_region_band").fetchone()
    _set_meta(conn, "expected_rows_discovery_region_band", str(n))


def _add_band(conn, work_id, w_start, w_end, discriminative=0,
              version="band-test", source="ruling", basis=None) -> None:
    conn.execute(
        "INSERT INTO discovery_region_band (band_version, work_id, w_start, w_end, "
        "discriminative, source, basis) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (version, work_id, w_start, w_end, discriminative, source, basis),
    )
    _set_meta(conn, "region_band_version", version)
    _set_meta(conn, "reference_corpus_sha256", SHA)
    _sync_band_count(conn)


def _first_identification_with_witness(conn) -> tuple:
    """(identification_id, work_id) for a row that actually carries witness
    evidence — the only kind of row a band can reach."""
    row = conn.execute(
        """
        SELECT di.identification_id, dc.work_id
        FROM discovery_identification di
        JOIN discovery_evidence de ON de.sys_id = di.sys_id
        JOIN discovery_claim dc ON dc.claim_id = de.claim_id
        JOIN works w ON w.work_id = dc.work_id
                     AND w.canonical_work_id = di.canonical_work_id
        WHERE de.evidence_kind = 'witness'
        ORDER BY di.identification_id LIMIT 1
        """
    ).fetchone()
    assert row is not None, "synthetic asset has no witness evidence"
    return row


def _identification_with_two_witness_rows(conn) -> tuple:
    """An identification whose footprint has more than one piece — the only kind
    that can exercise a MIXED footprint. Picking `_first_identification_with_witness`
    instead silently skipped the dominance test on the synthetic asset, where the
    id-ordered first row happens to carry exactly one witness."""
    row = conn.execute(
        """
        SELECT di.identification_id, dc.work_id, COUNT(DISTINCT de.evidence_id) AS n
        FROM discovery_identification di
        JOIN discovery_evidence de ON de.sys_id = di.sys_id
        JOIN discovery_claim dc ON dc.claim_id = de.claim_id
        JOIN works w ON w.work_id = dc.work_id
                     AND w.canonical_work_id = di.canonical_work_id
        WHERE de.evidence_kind = 'witness'
        GROUP BY di.identification_id, dc.work_id
        HAVING n >= 2
        ORDER BY di.identification_id LIMIT 1
        """
    ).fetchone()
    assert row is not None, (
        "no identification carries two witness rows -- the mixed-footprint tests "
        "cannot exercise what they claim")
    return row[0], row[1]


def _place_footprint(conn, identification_id, w_start, w_end) -> None:
    """Give every witness row of one identification the same work-side span."""
    conn.execute(
        """
        UPDATE discovery_evidence SET w_start = ?, w_end = ?
        WHERE evidence_kind = 'witness' AND sys_id = (
            SELECT sys_id FROM discovery_identification WHERE identification_id = ?)
        """,
        (w_start, w_end, identification_id),
    )


@pytest.fixture()
def asset(tmp_path):
    """A synthetic asset with coverage present, so rows reach step 6 rather than
    all short-circuiting at step 5 before step 3 is even consulted."""
    db = tmp_path / "asset.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    conn.execute("UPDATE discovery_identification SET max_coverage_ppm = 500000")
    matrix.recompute_and_store(conn, matrix.DEPLOY_1_PARAMETERIZATION)
    conn.commit()
    try:
        yield conn
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 1. covering_verdict — containment, half-open, and what absence means
# ---------------------------------------------------------------------------

def test_a_row_inside_the_band_takes_the_bands_verdict():
    assert matrix.covering_verdict([(100, 200, 0)], 120, 180) == 0
    assert matrix.covering_verdict([(100, 200, 1)], 120, 180) == 1


def test_containment_is_total_not_overlap():
    """A row that starts inside a band and ends outside it witnesses text the
    owner never ruled on. Treating that as covered is the whole failure mode
    step 3's ENTIRE-footprint wording exists to prevent."""
    assert matrix.covering_verdict([(100, 200, 0)], 150, 250) is None
    assert matrix.covering_verdict([(100, 200, 0)], 50, 150) is None


def test_the_band_is_half_open_at_its_end():
    """`[100, 200)` covers a row ending exactly at 200 and not one ending at 201
    — matching `shared.discovery_locus.merge_witnessed_spans`' work-side
    convention, so the two never disagree about the same pair of numbers."""
    assert matrix.covering_verdict([(100, 200, 0)], 100, 200) == 0
    assert matrix.covering_verdict([(100, 200, 0)], 100, 201) is None


def test_an_unplaceable_row_is_covered_by_nothing():
    assert matrix.covering_verdict([(0, 10 ** 9, 0)], None, None) is None
    assert matrix.covering_verdict([(0, 10 ** 9, 0)], 5, None) is None


def test_a_malformed_witness_span_is_unplaceable_not_covered():
    """`discovery_evidence.w_start`/`w_end` carry NO CHECK constraint (the band
    table does). A degenerate or inverted witness span would otherwise satisfy
    `band_start <= w_start and w_end <= band_end` and produce a CONFIDENT verdict
    about a footprint that means nothing — an unwarranted demotion from a
    malformed operand. Nothing emits such a row today; this is what stops that
    fact from being load-bearing. (Adversarial review 2026-08-13.)"""
    assert matrix.covering_verdict([(0, 100, 1)], 100, 100) is None   # degenerate
    assert matrix.covering_verdict([(0, 100, 1)], 150, 50) is None    # inverted
    assert matrix.covering_verdict([(0, 100, 0)], 60, 40) is None
    # Control: the same band still answers for a well-formed span inside it.
    assert matrix.covering_verdict([(0, 100, 1)], 40, 60) == 1


def test_a_row_covered_by_no_band_returns_none():
    assert matrix.covering_verdict([(100, 200, 0)], 900, 950) is None
    assert matrix.covering_verdict([], 900, 950) is None


def test_the_scan_finds_a_band_that_is_not_the_first():
    """The sorted scan breaks once a band starts after the row. A band list whose
    covering member sorts late must still be found, or the classifier silently
    under-fires on every work with more than one band."""
    bands = [(0, 50, 1), (100, 200, 0), (300, 400, 1)]
    assert matrix.covering_verdict(bands, 120, 180) == 0
    assert matrix.covering_verdict(bands, 310, 390) == 1


def test_the_scan_continues_past_an_earlier_band_that_does_not_contain_the_row():
    """The covering band is the SECOND one here: the first starts before the row
    (so the early break does not fire) but ends inside it. An implementation that
    stopped at the first band starting early enough would answer None.

    (This test replaced one named "a nested band is reachable behind a wider one"
    whose data — `[(0,100), (400,500)]` — was disjoint, not nested, and so pinned
    nothing of the sort. Codex review 2026-08-13, finding 2.)"""
    bands = [(0, 450, 0), (400, 500, 0)]
    assert matrix.covering_verdict(bands, 420, 480) == 0


def test_a_row_inside_a_truly_nested_pair_is_covered():
    """Genuine nesting: `[400,500)` sits wholly inside `[0,1000)`. Either band
    answers, and both agree — which is the only arrangement the ingest permits."""
    bands = [(0, 1000, 0), (400, 500, 0)]
    assert matrix.covering_verdict(bands, 420, 480) == 0


# ---------------------------------------------------------------------------
# 2. footprint_verdicts — the tri-state over a real asset
# ---------------------------------------------------------------------------

def test_the_synthetic_asset_has_no_work_offsets_at_all(asset):
    """Pins this module's docstring. If synthetic offsets ever appear, the tests
    below stop proving what they claim and must be revisited."""
    (n,) = asset.execute(
        "SELECT COUNT(*) FROM discovery_evidence WHERE w_start IS NOT NULL").fetchone()
    assert n == 0, "synthetic work offsets appeared -- re-read this module's docstring"


def test_a_wholly_covered_footprint_is_true(asset):
    iid, work_id = _first_identification_with_witness(asset)
    _place_footprint(asset, iid, 120, 180)
    verdicts = matrix.footprint_verdicts(
        asset, {work_id: [(100, 200, 0)]})
    assert verdicts[iid] is True


def test_a_discriminative_band_makes_it_false(asset):
    iid, work_id = _first_identification_with_witness(asset)
    _place_footprint(asset, iid, 120, 180)
    verdicts = matrix.footprint_verdicts(asset, {work_id: [(100, 200, 1)]})
    assert verdicts[iid] is False


def test_an_open_card_blocks_rather_than_demotes(asset):
    """`discriminative IS NULL` is an 'open' card: asked, not answered. The
    frozen step-3 semantics make that block the demotion, exactly as an
    unruled locus unit does."""
    iid, work_id = _first_identification_with_witness(asset)
    _place_footprint(asset, iid, 120, 180)
    verdicts = matrix.footprint_verdicts(asset, {work_id: [(100, 200, None)]})
    assert verdicts[iid] is None


def test_an_uncovered_row_makes_the_whole_footprint_unknowable(asset):
    iid, work_id = _first_identification_with_witness(asset)
    _place_footprint(asset, iid, 900, 950)
    verdicts = matrix.footprint_verdicts(asset, {work_id: [(100, 200, 0)]})
    assert verdicts[iid] is None


def test_one_unplaced_witness_row_blocks_a_footprint_that_is_otherwise_covered(asset):
    """The 0.7% case. A single witness row with no offset means part of the
    footprint cannot be placed, and a demotion is an assertion."""
    iid, work_id = _first_identification_with_witness(asset)
    _place_footprint(asset, iid, 120, 180)
    asset.execute(
        """
        UPDATE discovery_evidence SET w_start = NULL, w_end = NULL
        WHERE evidence_kind = 'witness' AND sys_id = (
            SELECT sys_id FROM discovery_identification WHERE identification_id = ?)
          AND rowid = (SELECT MIN(rowid) FROM discovery_evidence
                       WHERE evidence_kind = 'witness' AND sys_id = (
                           SELECT sys_id FROM discovery_identification
                           WHERE identification_id = ?))
        """,
        (iid, iid),
    )
    verdicts = matrix.footprint_verdicts(asset, {work_id: [(100, 200, 0)]})
    assert verdicts[iid] is None


def test_not_knowable_dominates_discriminative(asset):
    """Both block, so the surface cannot tell them apart — but whoever reads
    these verdicts to explain WHY a row did not demote can, and "I could not
    place part of this footprint" must not be reported as "I placed it in
    distinctive text"."""
    iid, work_id = _identification_with_two_witness_rows(asset)
    rows = asset.execute(
        """
        SELECT DISTINCT de.evidence_id FROM discovery_evidence de
        WHERE de.evidence_kind = 'witness' AND de.sys_id = (
            SELECT sys_id FROM discovery_identification WHERE identification_id = ?)
        ORDER BY de.evidence_id
        """, (iid,)).fetchall()
    assert len(rows) >= 2
    asset.execute("UPDATE discovery_evidence SET w_start = 120, w_end = 180 "
                  "WHERE evidence_id = ?", (rows[0][0],))
    asset.execute("UPDATE discovery_evidence SET w_start = 900, w_end = 950 "
                  "WHERE evidence_id = ?", (rows[1][0],))
    # 900-950 is uncovered; 120-180 sits in a DISCRIMINATIVE band. False would be
    # the answer if 'discriminative' won; None is the answer that tells the truth.
    verdicts = matrix.footprint_verdicts(asset, {work_id: [(100, 200, 1)]})
    assert verdicts[iid] is None


def test_shared_text_evidence_is_not_part_of_the_footprint(asset):
    """`shared_text` rows have no work-side alignment by construction. Counting
    them would make every identification carrying one permanently unknowable."""
    row = asset.execute(
        """
        SELECT di.identification_id, dc.work_id
        FROM discovery_identification di
        JOIN discovery_evidence de ON de.sys_id = di.sys_id
        JOIN discovery_claim dc ON dc.claim_id = de.claim_id
        JOIN works w ON w.work_id = dc.work_id
                     AND w.canonical_work_id = di.canonical_work_id
        WHERE de.evidence_kind = 'shared_text' LIMIT 1
        """).fetchone()
    if row is None:
        pytest.skip("synthetic asset carries no shared_text evidence")
    iid, work_id = row
    _place_footprint(asset, iid, 120, 180)   # witness rows only
    verdicts = matrix.footprint_verdicts(asset, {work_id: [(100, 200, 0)]})
    assert verdicts[iid] is True, (
        "a shared_text row with a NULL offset blocked the footprint -- the "
        "classifier is reading the wrong evidence channel")


def test_bands_are_scoped_to_their_own_work(asset):
    """A band on work A must not reach a row on work B."""
    iid, work_id = _first_identification_with_witness(asset)
    _place_footprint(asset, iid, 120, 180)
    other = asset.execute(
        "SELECT work_id FROM works WHERE work_id != ? LIMIT 1", (work_id,)).fetchone()[0]
    verdicts = matrix.footprint_verdicts(asset, {other: [(100, 200, 0)]})
    assert verdicts[iid] is None


def test_a_band_does_not_cross_to_the_OTHER_EDITION_of_the_same_canonical_work(asset):
    """THE case the `work_id` key exists for, and the one an arbitrary
    different-work test does not reach (Codex review 2026-08-13, finding 1).

    Two `works` rows sharing a `canonical_work_id` are two editions of one work
    with two independent reference streams — 15 such groups on the private asset,
    every one a sefaria/msource pair. Their offsets are NOT comparable, so a band
    measured on one must not classify a row matched against the other. Keying the
    table canonically would silently do exactly that."""
    iid, work_id = _first_identification_with_witness(asset)

    # Put the work into a canonical GROUP whose id is neither edition's work_id,
    # which is what a real sefaria/msource pair looks like. The band is then keyed
    # on the group id — a value a correct per-work lookup can never match.
    twin, group = "w_twin001", "w_canon01"
    asset.execute(
        "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, "
        "genre, source_corpus, identity_visibility) "
        "SELECT ?, ?, neutral_title, author, genre, source_corpus, "
        "identity_visibility FROM works WHERE work_id = ?", (twin, group, work_id))
    asset.execute("UPDATE works SET canonical_work_id = ? WHERE work_id = ?",
                  (group, work_id))
    asset.execute(
        "UPDATE discovery_identification SET canonical_work_id = ? "
        "WHERE identification_id = ?", (group, iid))
    _place_footprint(asset, iid, 120, 180)

    # A band keyed on the canonical GROUP. The row was matched against `work_id`,
    # so nothing may fire — and if the lookup were canonical it would.
    verdicts = matrix.footprint_verdicts(asset, {group: [(100, 200, 0)]})
    assert verdicts.get(iid) is None, (
        "a band keyed on the canonical group classified a row matched against one "
        "edition -- the footprint lookup is keyed canonically somewhere")

    # Control: the SAME band on the row's OWN work does fire, so the assertion
    # above is about the key and not about the band being unreachable.
    assert matrix.footprint_verdicts(asset, {work_id: [(100, 200, 0)]})[iid] is True


# ---------------------------------------------------------------------------
# 3. The guard, and the matrix end to end
# ---------------------------------------------------------------------------

def test_region_active_with_no_band_source_still_stops_the_build(asset):
    """`RegionInputUnavailable` narrowed; it must not have become dead code."""
    with pytest.raises(matrix.RegionInputUnavailable):
        matrix.recompute_and_store(asset, REGION_ON)


def test_region_active_with_bands_demotes_a_direct_row(asset):
    iid, work_id = _first_identification_with_witness(asset)
    asset.execute(
        "UPDATE discovery_identification SET relation_kind = ? WHERE identification_id = ?",
        (ids.CLAIM_TYPE_DIRECT_WITNESS, iid))
    _place_footprint(asset, iid, 120, 180)
    _add_band(asset, work_id, 100, 200, discriminative=0)
    matrix.recompute_and_store(asset, REGION_ON)
    (stored,) = asset.execute(
        "SELECT rendered_relation FROM discovery_identification "
        "WHERE identification_id = ?", (iid,)).fetchone()
    assert stored == ids.RENDERED_RELATION_SHARED_TEXT


def test_bands_present_but_region_off_change_nothing(asset):
    """The capability ships DARK. Deploy 1's ruling is `region_active=False`, and
    an asset carrying bands under that ruling must render exactly as it did
    without them — otherwise merely landing this table would move a surface."""
    before = dict(asset.execute(
        "SELECT identification_id, rendered_relation FROM discovery_identification"))
    iid, work_id = _first_identification_with_witness(asset)
    _place_footprint(asset, iid, 120, 180)
    _add_band(asset, work_id, 100, 200, discriminative=0)
    matrix.recompute_and_store(asset, matrix.DEPLOY_1_PARAMETERIZATION)
    after = dict(asset.execute(
        "SELECT identification_id, rendered_relation FROM discovery_identification"))
    assert after == before


def test_step_2_still_beats_step_3(asset):
    """Precedence is frozen; a new source for step 3's input must not reorder it."""
    iid, work_id = _first_identification_with_witness(asset)
    asset.execute(
        "UPDATE discovery_identification SET relation_kind = ?, routing_reason = ? "
        "WHERE identification_id = ?",
        (ids.CLAIM_TYPE_DIRECT_WITNESS, ids.ROUTING_REASON_CO_CITATION, iid))
    _place_footprint(asset, iid, 120, 180)
    _add_band(asset, work_id, 100, 200, discriminative=0)
    matrix.recompute_and_store(asset, REGION_ON)
    (stored,) = asset.execute(
        "SELECT rendered_relation FROM discovery_identification "
        "WHERE identification_id = ?", (iid,)).fetchone()
    # Both render shared_text, so assert the STEP rather than the string: with
    # the band removed the row must still be shared_text (step 2 alone).
    assert stored == ids.RENDERED_RELATION_SHARED_TEXT
    asset.execute("DELETE FROM discovery_region_band")
    _sync_band_count(asset)
    matrix.recompute_and_store(asset, matrix.DEPLOY_1_PARAMETERIZATION)
    (without_band,) = asset.execute(
        "SELECT rendered_relation FROM discovery_identification "
        "WHERE identification_id = ?", (iid,)).fetchone()
    assert without_band == ids.RENDERED_RELATION_SHARED_TEXT


# ---------------------------------------------------------------------------
# 4. THE MUTATION PROOF — the recompute gate must fail on a mutated ARTIFACT
# ---------------------------------------------------------------------------

def test_the_recompute_gate_catches_a_band_the_stored_values_predate(asset):
    """The gate proved able to fail, by mutating the ARTIFACT rather than a
    fixture: an asset is materialized with step 3 active over a footprint no band
    covers (so nothing demotes and the gate is green), then a covering band is
    inserted WITHOUT rematerializing. The stored `direct_witness` is now a value
    the matrix does not produce from the asset's own inputs, and the gate must
    say so. Without this, `check_relation_matrix_recompute` would be satisfied by
    any pair of self-consistent numbers.
    """
    iid, work_id = _first_identification_with_witness(asset)
    asset.execute(
        "UPDATE discovery_identification SET relation_kind = ? WHERE identification_id = ?",
        (ids.CLAIM_TYPE_DIRECT_WITNESS, iid))
    _place_footprint(asset, iid, 120, 180)
    # A band that covers NOTHING: region is active and has a source, so the
    # recompute runs, and this row keeps its direct_witness.
    _add_band(asset, work_id, 5000, 6000, discriminative=0)
    _set_meta(asset, *matrix.parameterization_meta_rows(REGION_ON)[0])
    for key, value in matrix.parameterization_meta_rows(REGION_ON):
        _set_meta(asset, key, value)
    matrix.recompute_and_store(asset, REGION_ON)
    asset.commit()

    (stored,) = asset.execute(
        "SELECT rendered_relation FROM discovery_identification "
        "WHERE identification_id = ?", (iid,)).fetchone()
    assert stored == ids.RENDERED_RELATION_DIRECT_WITNESS
    assert verify_mod.check_relation_matrix_recompute(asset, _meta(asset)) == []

    # THE MUTATION: the owner's ruling now covers this footprint, but the stored
    # column was written before it.
    _add_band(asset, work_id, 100, 200, discriminative=0)
    asset.commit()

    violations = verify_mod.check_relation_matrix_recompute(asset, _meta(asset))
    assert violations, (
        "the recompute gate accepted a stored direct_witness under a band that "
        "demotes it -- the gate cannot fail and proves nothing")
    assert "Contract 1" in violations[0]
    assert ids.RENDERED_RELATION_SHARED_TEXT in violations[0]


# ---------------------------------------------------------------------------
# 5. The ingest's four refusals
# ---------------------------------------------------------------------------

def _band_file(tmp_path, rows, *, version="band-v1", sha=SHA, name="band.json") -> str:
    payload = {"band_version": version, "ruled_date": "2026-08-13", "rows": rows}
    if sha is not None:
        payload["reference_corpus_sha256"] = sha
    path = tmp_path / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _row(work_id="w000001", w_start=100, w_end=200, stream_len=1000, **kw) -> dict:
    row = {"work_id": work_id, "w_start": w_start, "w_end": w_end,
           "stream_len": stream_len, "discriminative": False, "source": "ruling"}
    row.update(kw)
    return row


def test_ingest_inserts_a_well_formed_band(asset, tmp_path):
    meta_rows = sidecar_build.ingest_region_band(
        asset, _band_file(tmp_path, [_row()]), SHA)
    assert meta_rows == [("region_band_version", "band-v1")]
    stored = asset.execute(
        "SELECT band_version, work_id, w_start, w_end, discriminative "
        "FROM discovery_region_band").fetchall()
    assert stored == [("band-v1", "w000001", 100, 200, 0)]


def test_ingest_refuses_a_file_that_names_no_stream(asset, tmp_path):
    with pytest.raises(ValueError, match="reference_corpus_sha256"):
        sidecar_build.ingest_region_band(
            asset, _band_file(tmp_path, [_row()], sha=None), SHA)


def test_ingest_refuses_an_unpinned_bake(asset, tmp_path):
    """Contract 0, forced at the point it becomes checkable: bands are offsets,
    and a bake that never pinned its reference corpus cannot ship them."""
    with pytest.raises(ValueError, match="Contract 0|--reference-corpus-sha256"):
        sidecar_build.ingest_region_band(
            asset, _band_file(tmp_path, [_row()]), None)


def test_ingest_refuses_a_file_measured_against_another_stream(asset, tmp_path):
    with pytest.raises(ValueError, match="DIFFERENT stream"):
        sidecar_build.ingest_region_band(
            asset, _band_file(tmp_path, [_row()], sha="b" * 64), SHA)


def test_ingest_refuses_an_unknown_work(asset, tmp_path):
    with pytest.raises(ValueError, match="does not carry"):
        sidecar_build.ingest_region_band(
            asset, _band_file(tmp_path, [_row(work_id="w_nope")]), SHA)


def test_ingest_refuses_a_band_past_the_end_of_its_stream(asset, tmp_path):
    with pytest.raises(ValueError, match="different edition"):
        sidecar_build.ingest_region_band(
            asset, _band_file(tmp_path, [_row(w_start=900, w_end=1200, stream_len=1000)]),
            SHA)


def test_ingest_refuses_a_malformed_span(asset, tmp_path):
    with pytest.raises(ValueError, match="malformed half-open span"):
        sidecar_build.ingest_region_band(
            asset, _band_file(tmp_path, [_row(w_start=200, w_end=200)]), SHA)


def test_ingest_refuses_contradictory_overlap(asset, tmp_path):
    with pytest.raises(ValueError, match="overlap with DIFFERENT verdicts"):
        sidecar_build.ingest_region_band(asset, _band_file(tmp_path, [
            _row(w_start=100, w_end=300, discriminative=False),
            _row(w_start=200, w_end=400, discriminative=True),
        ]), SHA)


def test_ingest_allows_agreeing_overlap(asset, tmp_path):
    """An owner may rule the same stretch twice from two bases. Only DISAGREEING
    overlap makes the classifier order-dependent."""
    sidecar_build.ingest_region_band(asset, _band_file(tmp_path, [
        _row(w_start=100, w_end=300, basis="one"),
        _row(w_start=200, w_end=400, basis="two"),
    ]), SHA)
    (n,) = asset.execute("SELECT COUNT(*) FROM discovery_region_band").fetchone()
    assert n == 2


def test_overlap_detection_compares_against_every_open_predecessor(asset, tmp_path):
    """A neighbours-only sweep misses a long band that contains two later ones.
    The conflicting pair here is the FIRST and the THIRD."""
    with pytest.raises(ValueError, match="overlap with DIFFERENT verdicts"):
        sidecar_build.ingest_region_band(asset, _band_file(tmp_path, [
            _row(w_start=0, w_end=900, discriminative=False),
            _row(w_start=100, w_end=200, discriminative=False),
            _row(w_start=300, w_end=400, discriminative=True),
        ]), SHA)


# ---------------------------------------------------------------------------
# 6. The verifier gates
# ---------------------------------------------------------------------------

def test_band_contract_requires_the_stream_to_be_named(asset):
    _add_band(asset, "w000001", 100, 200)
    asset.execute("DELETE FROM meta WHERE key = 'reference_corpus_sha256'")
    violations = verify_mod.check_region_band_contract(asset, _meta(asset))
    assert any("Contract 0" in v for v in violations)


def test_band_contract_is_silent_on_an_asset_with_no_bands(asset):
    assert verify_mod.check_region_band_contract(asset, _meta(asset)) == []


def test_band_contract_catches_a_band_on_an_unknown_work(asset):
    """The `REFERENCES works` FK is not enforced on the release path (no
    `PRAGMA foreign_keys`), so an inert band would otherwise read as a pass."""
    asset.execute("PRAGMA foreign_keys = OFF")
    _add_band(asset, "w_ghost", 100, 200)
    violations = verify_mod.check_region_band_contract(asset, _meta(asset))
    assert any("does not carry" in v for v in violations)


def test_band_contract_catches_contradictory_overlap_in_the_artifact(asset):
    """Re-derived rather than trusted: a hand-edited or badly-projected asset
    never went through the ingest that rejects this."""
    _add_band(asset, "w000001", 100, 300, discriminative=0)
    _add_band(asset, "w000001", 200, 400, discriminative=1)
    violations = verify_mod.check_region_band_contract(asset, _meta(asset))
    assert any("overlap" in v for v in violations)


def test_band_contract_accepts_agreeing_overlap(asset):
    _add_band(asset, "w000001", 100, 300, discriminative=0)
    _add_band(asset, "w000001", 200, 400, discriminative=0)
    assert verify_mod.check_region_band_contract(asset, _meta(asset)) == []


def test_a_second_band_version_is_refused(asset):
    """The matrix reads the table WHOLE. Two versions of an owner ruling would
    both fire, so the asset is refused rather than half-read."""
    _add_band(asset, "w000001", 100, 200, version="band-v1")
    _add_band(asset, "w000001", 300, 400, version="band-v2")
    violations = verify_mod.check_single_input_version(asset, _meta(asset))
    assert any("discovery_region_band" in v and "distinct" in v for v in violations)


def test_meta_must_name_the_version_the_table_holds(asset):
    _add_band(asset, "w000001", 100, 200, version="band-v1")
    _set_meta(asset, "region_band_version", "band-v9")
    violations = verify_mod.check_single_input_version(asset, _meta(asset))
    assert any("names a ruling the table does not hold" in v for v in violations)


def test_a_populated_table_with_no_meta_version_is_refused(asset):
    _add_band(asset, "w000001", 100, 200)
    asset.execute("DELETE FROM meta WHERE key = 'region_band_version'")
    violations = verify_mod.check_single_input_version(asset, _meta(asset))
    assert any("does not say which ruling" in v for v in violations)


def test_the_same_gate_covers_the_curated_quoter_list(asset):
    """The DDL has always claimed the curated list is read "at the single
    curated_quoter_version" and nothing enforced it. Two versions there is the
    identical defect and is now caught by the identical gate."""
    asset.executemany(
        "INSERT INTO discovery_curated_quoter (list_version, canonical_work_id, "
        "ruled_date, note) VALUES (?, ?, ?, NULL)",
        [("quoter-v1", "w000001", "2026-08-12"),
         ("quoter-v2", "w000002", "2026-08-13")])
    violations = verify_mod.check_single_input_version(asset, _meta(asset))
    assert any("discovery_curated_quoter" in v and "distinct" in v for v in violations)


def test_the_version_gate_is_silent_on_a_clean_asset(asset):
    assert verify_mod.check_single_input_version(asset, _meta(asset)) == []


def test_two_versions_sharing_a_span_do_not_crash_the_sort(asset):
    """A tri-state in a sort key is a TypeError waiting for the right data.

    `discovery_region_band`'s primary key includes `band_version`, so two rows
    CAN share `(work_id, w_start, w_end)` and disagree about `discriminative` —
    and `discriminative` is `None` for an open card. Sorting the raw tuples
    compares `None` with `0` and raises. That crash is reachable on exactly the
    asset the version gate exists to reject, and during the BUILD, before any
    verifier runs — so every sort over these spans keys on the offsets alone.
    """
    _add_band(asset, "w000001", 100, 200, discriminative=0, version="band-v1")
    _add_band(asset, "w000001", 100, 200, discriminative=None, version="band-v2")

    bands = matrix.region_bands_by_work(asset)          # must not raise
    assert len(bands["w000001"]) == 2

    violations = verify_mod.check_region_band_contract(asset, _meta(asset))
    assert any("overlap" in v for v in violations), (
        "two verdicts on the identical span is the contradictory-overlap case")
    assert verify_mod.check_single_input_version(asset, _meta(asset)), (
        "two band versions must also be reported as such")


def test_ingest_refuses_two_stream_lengths_for_one_work(asset, tmp_path):
    with pytest.raises(ValueError, match="two different stream lengths"):
        sidecar_build.ingest_region_band(asset, _band_file(tmp_path, [
            _row(w_start=100, w_end=200, stream_len=1000),
            _row(w_start=300, w_end=400, stream_len=2000),
        ]), SHA)


def test_ingest_refuses_a_stream_length_the_asset_already_contradicts(asset, tmp_path):
    """`stream_len` is checked against the asset, not only against itself: the
    largest work-side offset the evidence already carries is a lower bound on the
    true stream, so a smaller declaration is provably the wrong stream."""
    row = asset.execute(
        "SELECT dc.work_id FROM discovery_evidence de "
        "JOIN discovery_claim dc ON dc.claim_id = de.claim_id LIMIT 1").fetchone()
    work_id = row[0]
    asset.execute(
        "UPDATE discovery_evidence SET w_start = 5000, w_end = 6000 "
        "WHERE claim_id IN (SELECT claim_id FROM discovery_claim WHERE work_id = ?)",
        (work_id,))
    with pytest.raises(ValueError, match="not the stream the evidence indexes"):
        sidecar_build.ingest_region_band(asset, _band_file(tmp_path, [
            _row(work_id=work_id, w_start=100, w_end=200, stream_len=1000),
        ]), SHA)


# ---------------------------------------------------------------------------
# 7. Projection
# ---------------------------------------------------------------------------

def test_a_band_is_pruned_with_its_work(tmp_path):
    """Bands follow their `works` row rather than copying verbatim like the
    curated list. A band carried past its work is a coordinate into a stream the
    asset no longer describes — and it would still be counted by the
    release-contract count key (Codex pre-flight, finding 6)."""
    import project_discovery_public as projector  # noqa: E402

    db = tmp_path / "private.db"
    _build_synthetic(db)
    conn = sqlite3.connect(str(db))
    conn.row_factory = sqlite3.Row  # the projector's own row reader needs it
    try:
        # A work with no surviving public claim is the one that gets pruned; find
        # one the projector will not keep rather than assuming which.
        ctx = projector.ProjectionContext(conn)
        public = set(ctx.public_work_ids)
        all_works = [r[0] for r in conn.execute("SELECT work_id FROM works ORDER BY work_id")]
        dropped = next((w for w in all_works if w not in public), None)
        kept = next((w for w in all_works if w in public), None)
        # Asserted, not skipped around: a fixture where nothing is pruned would
        # make the important half of this test disappear while it still passed.
        assert kept is not None, "no public work at all -- fixture cannot exercise this"
        assert dropped is not None, (
            "every synthetic work survives projection -- this test can no longer "
            "prove a band is pruned with its work")

        for work_id in (kept, dropped):
            conn.execute(
                "INSERT INTO discovery_region_band VALUES ('b1', ?, 0, 100, 0, 'ruling', NULL)",
                (work_id,))
        conn.commit()

        ctx = projector.ProjectionContext(conn)
        projected = {r["work_id"] for r in projector._project_discovery_region_band(ctx)}
        assert kept in projected, "a band on a surviving work was dropped"
        assert dropped not in projected, "a band survived the pruning of its own work"
    finally:
        conn.close()
