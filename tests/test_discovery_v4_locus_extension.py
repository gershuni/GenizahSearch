"""Chain-append proof for `_extend_locus` (C6/C12).

The V4.2 plan chains the reference build append-only: base -> +REF5 -> +REF6,
each stage hash-pinned, and every stage calls `_extend_locus` once against the
PREVIOUS stage's output database and coverage document (see
`docs/specs/discovery-v4.2-combined-bake-and-public-first-plan.md`, C2/C6).

`_extend_locus` also threads a `supplemental_structures` summary through every
stage for the small set of fixed-id supplemental works it re-passes on every
run (e.g. the Guide for the Perplexed, `REF2:ja2_rambam_moreh`). Before the C6
fix, that summary was unconditionally overwritten each call, so a later stage
-- which typically finds the supplemental work already present and adds
nothing new -- silently erased an earlier stage's real counts. These tests
build tiny synthetic locus databases and coverage documents shaped like the
real chain and prove: (1) two independent namespace extensions compose
end-to-end without disturbing each other's summary, (2) the
`supplemental_structures` block MERGES instead of replacing, (3) an already-
present supplemental entry is never double-counted, and (4) the existing
same-namespace re-extension guard still holds after a real chain.

Every test here calls `_extend_locus` directly against synthetic fixtures --
never the real V2/V4/V4.1 artifacts, and never `run()`.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from scripts.build_work_divisions import Unit, WorkUnits
from scripts.discovery_v4_build_reference import _extend_locus


_SCHEMA = """
CREATE TABLE locus_work (
  locus_ref_id  TEXT PRIMARY KEY,
  family        TEXT NOT NULL,
  grain         TEXT NOT NULL,
  stream_len    INTEGER NOT NULL,
  unit_count    INTEGER NOT NULL);

CREATE TABLE locus_unit (
  locus_ref_id  TEXT NOT NULL REFERENCES locus_work(locus_ref_id),
  unit_ord      INTEGER NOT NULL,
  start_offset  INTEGER NOT NULL,
  part_key      TEXT NOT NULL,
  label_he      TEXT NOT NULL,
  citation_pos  INTEGER,
  PRIMARY KEY (locus_ref_id, unit_ord));
"""


def _make_db(path: Path, works: list[dict]) -> None:
    """Build a minimal locus database with the given pre-existing works.

    Each ``work`` dict is ``{"ref_id", "family", "grain", "stream_len",
    "units"}`` where ``units`` is a list of ``(start, part_key, label_he,
    citation_pos)`` rows, ordered ascending (``unit_ord`` is the list index).
    """
    conn = sqlite3.connect(path)
    try:
        conn.executescript(_SCHEMA)
        for work in works:
            conn.execute(
                "INSERT INTO locus_work VALUES (?,?,?,?,?)",
                (
                    work["ref_id"],
                    work["family"],
                    work["grain"],
                    work["stream_len"],
                    len(work["units"]),
                ),
            )
            conn.executemany(
                "INSERT INTO locus_unit VALUES (?,?,?,?,?,?)",
                [
                    (work["ref_id"], ordinal, start, part_key, label, pos)
                    for ordinal, (start, part_key, label, pos) in enumerate(
                        work["units"]
                    )
                ],
            )
        conn.commit()
    finally:
        conn.close()


def _write_coverage(path: Path, **fields) -> Path:
    path.write_text(json.dumps(fields), encoding="utf-8")
    return path


def _ref_entry(raw_id: str, grain: str, stream_len: int, rows: list[tuple]) -> dict:
    """One synthetic ``reference_entries`` row, shaped like the real call site
    in ``run()``: only the fields ``_extend_locus`` itself reads."""
    return {
        "raw_reference_id": raw_id,
        "locus_grain": grain,
        "stream_len": stream_len,
        "unit_offsets": [
            {
                "source_ordinal": ordinal,
                "start_offset": start,
                "label_he": label,
                "citation_pos": pos,
            }
            for ordinal, start, label, pos in rows
        ],
    }


def _work_units(ref_id: str, family: str, grain: str, units: list[tuple]) -> WorkUnits:
    built = [
        Unit(index, start, part_key, label, pos)
        for index, (start, part_key, label, pos) in enumerate(units)
    ]
    stream_len = max((unit.start for unit in built), default=0) + 10
    return WorkUnits(ref_id, family, grain, built, stream_len)


# ---------------------------------------------------------------------------
# End-to-end chain: base -> +REF5 -> +REF6
# ---------------------------------------------------------------------------

# Pre-existing rows a real base could already carry: one ordinary reference
# work and one supplemental work (standing in for the Guide for the
# Perplexed) that an earlier stage already recorded.
_BASE_REF_UNITS = [(0, "pk:0", "one", 1), (15, "pk:1", "two", 2)]
_ALPHA_UNITS = [(0, "pk:0", "א", 1), (10, "pk:1", "ב", 2), (20, "pk:2", "ג", 3)]
_BETA_UNITS = [(0, "pk:0", "one", 1), (12, "pk:1", "two", 2)]


def _build_base(tmp_path: Path) -> tuple[Path, Path]:
    base_db = tmp_path / "base.db"
    _make_db(
        base_db,
        [
            {
                "ref_id": "BASE:one",
                "family": "sefaria",
                "grain": "chapter",
                "stream_len": 30,
                "units": _BASE_REF_UNITS,
            },
            {
                "ref_id": "SUPP:alpha",
                "family": "ja",
                "grain": "chapter",
                "stream_len": 30,
                "units": _ALPHA_UNITS,
            },
        ],
    )
    base_coverage = _write_coverage(
        tmp_path / "base_coverage.json",
        reference_corpus_sha256="0" * 64,
        works_with_units=2,
        units_total=5,
        by_family={"sefaria": 1, "ja": 1},
        by_grain={"chapter": 2},
        invariant_problems=[],
        supplemental_structures={
            "added_works_with_units": 1,
            "added_units": 3,
            "reference_ids": ["SUPP:alpha"],
        },
    )
    return base_db, base_coverage


def test_chain_append_ref5_then_ref6_preserves_history_and_recounts_correctly(
    tmp_path: Path,
):
    base_db, base_coverage = _build_base(tmp_path)
    alpha_work = _work_units("SUPP:alpha", "ja", "chapter", _ALPHA_UNITS)

    ref5_entry = _ref_entry("REF5:one", "chapter", 40, [(1, 0, "א", 1), (2, 20, "ב", 2)])
    ref5_db = tmp_path / "ref5.db"
    ref5_coverage = tmp_path / "ref5_coverage.json"
    coverage_after_ref5 = _extend_locus(
        base_db=base_db,
        base_coverage=base_coverage,
        output_db=ref5_db,
        output_coverage=ref5_coverage,
        new_reference_hash="5" * 64,
        reference_entries=[ref5_entry],
        namespace="REF5",
        supplemental_works=[alpha_work],
    )

    # REF5 added one new reference work (2 units); the supplemental work was
    # already present, so it contributes nothing new this stage.
    assert coverage_after_ref5["ref5_extension"] == {
        "added_works_with_units": 1,
        "added_units": 2,
        "whole_work_fallback_refs": 0,
        "added_by_grain": {"chapter": 1},
    }
    assert coverage_after_ref5["supplemental_structures"] == {
        "added_works_with_units": 1,
        "added_units": 3,
        "reference_ids": ["SUPP:alpha"],
    }
    assert coverage_after_ref5["works_with_units"] == 3  # base(2) + REF5(1)
    assert coverage_after_ref5["units_total"] == 7  # base(5) + REF5(2)

    # REF6 chains onto REF5's own output, and introduces one genuinely NEW
    # supplemental work (beta) alongside the already-recorded alpha.
    beta_work = _work_units("SUPP:beta", "ja", "chapter", _BETA_UNITS)
    ref6_entry = _ref_entry(
        "REF6:two", "section", 30, [(1, 0, "1", 1), (2, 15, "2", 2)]
    )
    ref6_db = tmp_path / "ref6.db"
    ref6_coverage = tmp_path / "ref6_coverage.json"
    coverage_after_ref6 = _extend_locus(
        base_db=ref5_db,
        base_coverage=ref5_coverage,
        output_db=ref6_db,
        output_coverage=ref6_coverage,
        new_reference_hash="6" * 64,
        reference_entries=[ref6_entry],
        namespace="REF6",
        supplemental_works=[alpha_work, beta_work],
    )

    # Both extension keys are present ...
    assert "ref5_extension" in coverage_after_ref6
    assert "ref6_extension" in coverage_after_ref6
    # ... and the earlier stage's own key is untouched by the later run.
    assert coverage_after_ref6["ref5_extension"] == coverage_after_ref5["ref5_extension"]
    assert coverage_after_ref6["ref6_extension"] == {
        "added_works_with_units": 1,
        "added_units": 2,
        "whole_work_fallback_refs": 0,
        "added_by_grain": {"section": 1},
    }
    # The supplemental record is preserved AND merged: alpha's historical
    # count survives, beta's fresh insertion is added on top, and the id
    # list is the order-stable union (alpha first, beta appended).
    assert coverage_after_ref6["supplemental_structures"] == {
        "added_works_with_units": 2,
        "added_units": 5,
        "reference_ids": ["SUPP:alpha", "SUPP:beta"],
    }
    # Invariant recomputation across the whole chain: base(2) + REF5(1) +
    # REF6(1 reference + 1 newly-inserted supplemental) = 5 works;
    # base(5) + REF5(2) + REF6(2 reference + 2 supplemental units) = 11.
    assert coverage_after_ref6["works_with_units"] == 5
    assert coverage_after_ref6["units_total"] == 11
    assert coverage_after_ref6["invariant_problems"] == []

    # The existing same-namespace guard still refuses a second REF6 stage
    # chained onto a REF6-extended coverage.
    with pytest.raises(ValueError, match="already records a REF6 extension"):
        _extend_locus(
            base_db=ref6_db,
            base_coverage=ref6_coverage,
            output_db=tmp_path / "ref6_again.db",
            output_coverage=tmp_path / "ref6_again_coverage.json",
            new_reference_hash="7" * 64,
            reference_entries=[],
            namespace="REF6",
        )


# ---------------------------------------------------------------------------
# Focused gates (isolated from the full chain, for a sharper mutation signal)
# ---------------------------------------------------------------------------


def test_supplemental_structures_merge_survives_a_run_with_no_supplemental_works(
    tmp_path: Path,
):
    """A stage that passes no `supplemental_works` at all must still carry the
    prior recorded supplemental summary forward unchanged -- a plain
    dict-replace would zero it out even though nothing this run touched it."""
    base_db = tmp_path / "base.db"
    _make_db(
        base_db,
        [
            {
                "ref_id": "BASE:one",
                "family": "sefaria",
                "grain": "chapter",
                "stream_len": 30,
                "units": _BASE_REF_UNITS,
            }
        ],
    )
    prior_supplemental = {
        "added_works_with_units": 1,
        "added_units": 5,
        "reference_ids": ["SUPP:already-recorded"],
    }
    base_coverage = _write_coverage(
        tmp_path / "base_coverage.json",
        reference_corpus_sha256="0" * 64,
        works_with_units=1,
        units_total=2,
        by_family={"sefaria": 1},
        by_grain={"chapter": 1},
        invariant_problems=[],
        supplemental_structures=prior_supplemental,
    )
    ref5_entry = _ref_entry("REF5:one", "chapter", 40, [(1, 0, "א", 1), (2, 20, "ב", 2)])

    result = _extend_locus(
        base_db=base_db,
        base_coverage=base_coverage,
        output_db=tmp_path / "out.db",
        output_coverage=tmp_path / "out_coverage.json",
        new_reference_hash="5" * 64,
        reference_entries=[ref5_entry],
        namespace="REF5",
        supplemental_works=None,
    )

    assert result["supplemental_structures"] == prior_supplemental


def test_supplemental_merge_does_not_double_count_an_existing_entry(tmp_path: Path):
    """Re-passing a supplemental work that already exists in the DB must add
    ZERO to the merged totals -- the exists-guard already keeps it out of
    `supplemental_added_works`/`supplemental_added_units`, so a merge that
    instead added `len(supplemental_works)` would double the prior count."""
    base_db = tmp_path / "base.db"
    _make_db(
        base_db,
        [
            {
                "ref_id": "SUPP:alpha",
                "family": "ja",
                "grain": "chapter",
                "stream_len": 30,
                "units": _ALPHA_UNITS,
            }
        ],
    )
    base_coverage = _write_coverage(
        tmp_path / "base_coverage.json",
        reference_corpus_sha256="0" * 64,
        works_with_units=1,
        units_total=3,
        by_family={"ja": 1},
        by_grain={"chapter": 1},
        invariant_problems=[],
        supplemental_structures={
            "added_works_with_units": 1,
            "added_units": 3,
            "reference_ids": ["SUPP:alpha"],
        },
    )
    alpha_work = _work_units("SUPP:alpha", "ja", "chapter", _ALPHA_UNITS)

    result = _extend_locus(
        base_db=base_db,
        base_coverage=base_coverage,
        output_db=tmp_path / "out.db",
        output_coverage=tmp_path / "out_coverage.json",
        new_reference_hash="5" * 64,
        reference_entries=[],
        namespace="REF5",
        supplemental_works=[alpha_work],
    )

    assert result["supplemental_structures"] == {
        "added_works_with_units": 1,
        "added_units": 3,
        "reference_ids": ["SUPP:alpha"],
    }
