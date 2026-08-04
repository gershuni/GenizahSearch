# -*- coding: utf-8 -*-
"""PANEL-02 "Other manuscripts matching <work>" expansion (Phase 136, plan 136-21).

WHAT THE RETURN SHAPE WAS BEFORE THIS PLAN, and why it made the earlier
"wire the existing field through" instruction impossible:

  `get_work_witnesses()` returned a list of dicts carrying EXACTLY NINE keys --
  `work_id`, `unit_id`, `representative_sys_id`, `representative_page_id`,
  `representative_claim_id`, `claim_type`, `evidence_source`,
  `confidence_band`, `member_sys_ids`.

  The `claim_type` and the band on that row were the OTHER CARRIER's, and only
  the other carrier's. There was:
    * NO anchor relation kind and NO anchor band, so PANEL-02's "shows each
      side's own relation type when they differ" had nothing to compare and
      DATA-01's "displays the WEAKER of the two claims' bands" had no second
      band to rank;
    * NO `library_code` and NO `shelfmark_display` -- `manuscript_display` was
      not joined in `_WORK_WITNESSES_RANKED_CTE_SQL` at all -- so a returned row
      could not NAME the manuscript it pointed at;
    * NO `band_label`, so a renderer would have had to format a band itself;
    * NO total of any kind, so a page length was the only number available to a
      surface that promises a real count.

  And every query failure -- the list query, the member-sys_ids query, the
  anchor-unit lookup -- was swallowed by `except Exception: return []`. An
  envelope built by wrapping that method would have reported `ok` with zero
  items after a failed query: the exact false-zero class plan 136-14 found on a
  real pre-rebuild asset and fixed for the page query.

  Field-wiring was therefore never possible: there was no anchor field to wire,
  no name to render, and no total to report.

Masking discipline: every fixture here is fabricated in-test through
`scripts/build_discovery_sidecar.create_schema` -- never real research data. The
one test that reads a REAL artifact (the highest-cardinality expansion probe)
reports aggregate counts and an opaque minted `work_id` only.
"""
from __future__ import annotations

import ast
import inspect
import sqlite3

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from shared.discovery_band_labels import serialize_banded_claim
from shared.discovery_service import (
    DiscoveryService,
    _project_work_witnesses,
)

_TRACK1 = "track1_direct"
_PROPAGATED = "propagated"


# ---------------------------------------------------------------------------
# Fixture builder: a synthetic sidecar carrying ONE work and N carriers, each
# with its own band / relation kind and (optionally) a manuscript_display row.
# ---------------------------------------------------------------------------

def _build_expansion_db(
    db_path,
    carriers,
    *,
    work_id="wEXP001",
    units=(),
    display=None,
    sidecar_version="test-expansion",
):
    """`carriers`: list of dicts with `sys_id`, `page_id`, `claim_type`,
    `evidence_source`, `confidence_band` (+ optional `adjudication_status`).

    `display`: sys_id -> (library_code, shelfmark_display). `None` means "seed a
    display row for EVERY carrier"; pass an explicit dict to omit some.

    `units`: iterable of (unit_id, [sys_id, ...]) merged witness units.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, "
            "genre, source_corpus) VALUES (?, ?, ?, ?, ?, ?)",
            (work_id, work_id, "Synthetic Expansion Work", None, None, "sefaria"),
        )
        claim_rows = []
        evidence_rows = []
        for i, c in enumerate(carriers):
            claim_id = f"c{i:06d}"
            evidence_id = f"e{i:06d}"
            claim_rows.append((
                c["page_id"], work_id, claim_id, c["claim_type"], evidence_id,
                "sefaria", sidecar_version,
            ))
            evidence_rows.append((
                evidence_id, claim_id, "witness", c["evidence_source"],
                c["confidence_band"], c.get("adjudication_status", "unreviewed"),
                "n/a", "shipped", "none", c["page_id"], c["sys_id"], 0, 10,
            ))
        cur.executemany(
            "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
            "display_evidence_id, source_corpus, sidecar_version) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            claim_rows,
        )
        cur.executemany(
            "INSERT INTO discovery_evidence (evidence_id, claim_id, evidence_kind, "
            "evidence_source, confidence_band, adjudication_status, audit_status, "
            "routing_status, routing_reason, a_page_id, sys_id, span_start, span_end) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            evidence_rows,
        )
        for unit_id, members in units:
            cur.execute("INSERT INTO witness_units (unit_id) VALUES (?)", (unit_id,))
            cur.executemany(
                "INSERT INTO witness_unit_members (unit_id, sys_id, merge_basis) "
                "VALUES (?, ?, ?)",
                [(unit_id, sys_id, "physical_join") for sys_id in members],
            )
        if display is None:
            display = {
                c["sys_id"]: ("CUL", f"T-S {c['sys_id'][-4:]}") for c in carriers
            }
        cur.executemany(
            "INSERT OR IGNORE INTO manuscript_display (sys_id, library_code, "
            "library_sort_key, shelfmark_display, shelfmark_sort_key) "
            "VALUES (?, ?, ?, ?, ?)",
            [(sys_id, lib, lib, shelf, shelf) for sys_id, (lib, shelf) in display.items()],
        )
        cur.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [("schema_version", "discovery-v1"), ("sidecar_version", sidecar_version),
             ("audience", "public")],
        )
        conn.commit()
    finally:
        conn.close()
    return str(db_path)


def _service_for(db_path, version="test-expansion"):
    return DiscoveryService(
        path_provider=lambda: str(db_path),
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: version,
    )


def _carrier(sys_id, page_id, *, claim_type="direct_witness",
             evidence_source=_TRACK1, confidence_band="tier_a"):
    return {
        "sys_id": sys_id, "page_id": page_id, "claim_type": claim_type,
        "evidence_source": evidence_source, "confidence_band": confidence_band,
    }


#: The anchor triple used wherever the anchor's own identity is not the
#: variable under test. `direct_witness` + the STRONGEST band, so the other
#: carrier decides the displayed band unless a test says otherwise.
_ANCHOR_STRONG = dict(
    anchor_claim_type="direct_witness",
    anchor_evidence_source=_TRACK1,
    anchor_confidence_band="expert_verified",
)


# ===========================================================================
# Task 1: both sides' relation + band, the weaker-band rule, renderable rows
# ===========================================================================

def test_row_carries_both_sides_relation_and_band(tmp_path):
    db = _build_expansion_db(tmp_path / "both-sides.db", [
        _carrier("990000000000000001", "p001", claim_type="quotes_this_work",
                 confidence_band="tier_a"),
    ])
    items = _service_for(db).get_work_witnesses("wEXP001", **_ANCHOR_STRONG)
    assert len(items) == 1
    row = items[0]
    # the OTHER carrier's side
    assert row["claim_type"] == "quotes_this_work"
    assert row["confidence_band"] == "tier_a"
    assert row["evidence_source"] == _TRACK1
    # the ANCHOR's side, alongside it
    assert row["anchor_claim_type"] == "direct_witness"
    assert row["anchor_confidence_band"] == "expert_verified"
    assert row["anchor_evidence_source"] == _TRACK1


def test_relations_differ_is_true_and_both_kinds_are_present_and_distinct(tmp_path):
    db = _build_expansion_db(tmp_path / "differ.db", [
        _carrier("990000000000000001", "p001", claim_type="quotes_this_work"),
    ])
    row = _service_for(db).get_work_witnesses("wEXP001", **_ANCHOR_STRONG)[0]
    assert row["relations_differ"] is True
    assert row["claim_type"] == "quotes_this_work"
    assert row["anchor_claim_type"] == "direct_witness"
    assert row["claim_type"] != row["anchor_claim_type"]


def test_relations_differ_is_false_when_both_sides_share_a_relation(tmp_path):
    db = _build_expansion_db(tmp_path / "same.db", [
        _carrier("990000000000000001", "p001", claim_type="direct_witness"),
    ])
    row = _service_for(db).get_work_witnesses("wEXP001", **_ANCHOR_STRONG)[0]
    assert row["relations_differ"] is False
    assert row["claim_type"] == row["anchor_claim_type"] == "direct_witness"


def test_stronger_anchor_displays_the_other_carriers_band(tmp_path):
    db = _build_expansion_db(tmp_path / "anchor-strong.db", [
        _carrier("990000000000000001", "p001", confidence_band="screening_rb"),
    ])
    row = _service_for(db).get_work_witnesses("wEXP001", **_ANCHOR_STRONG)[0]
    # anchor = expert_verified (rank 1), carrier = screening_rb (rank 4):
    # the WEAKER of the pair is the carrier's.
    assert row["displayed_confidence_band"] == "screening_rb"
    assert row["displayed_confidence_band"] != row["anchor_confidence_band"]
    assert row["displayed_evidence_source"] == _TRACK1


def test_weaker_anchor_displays_the_anchors_own_band(tmp_path):
    db = _build_expansion_db(tmp_path / "anchor-weak.db", [
        _carrier("990000000000000001", "p001", confidence_band="tier_a"),
    ])
    row = _service_for(db).get_work_witnesses(
        "wEXP001",
        anchor_claim_type="direct_witness",
        anchor_evidence_source=_PROPAGATED,
        anchor_confidence_band="weak",
    )[0]
    # anchor = propagated/weak (rank 6), carrier = track1/tier_a (rank 2):
    # the WEAKER of the pair is the ANCHOR's, and it is what is displayed.
    assert row["displayed_confidence_band"] == "weak"
    assert row["displayed_evidence_source"] == _PROPAGATED
    assert row["displayed_confidence_band"] != row["confidence_band"]


def test_anchor_evidence_source_reaches_the_band_comparison(tmp_path):
    """Two calls identical except the anchor's EVIDENCE SOURCE -- the resolved
    band must differ, which is only possible if the source reaches `_band_rank`.

    `propagated/corroborated` is rank 3 (STRONGER than the carrier's
    `screening_rb`, rank 4) while `track1_direct/screening_canon` is rank 5
    (WEAKER). Same band string family, opposite verdicts."""
    db = _build_expansion_db(tmp_path / "anchor-source.db", [
        _carrier("990000000000000001", "p001", confidence_band="screening_rb"),
    ])
    service = _service_for(db)
    stronger = service.get_work_witnesses(
        "wEXP001", anchor_claim_type="direct_witness",
        anchor_evidence_source=_PROPAGATED, anchor_confidence_band="corroborated")[0]
    weaker = service.get_work_witnesses(
        "wEXP001", anchor_claim_type="direct_witness",
        anchor_evidence_source=_TRACK1, anchor_confidence_band="screening_canon")[0]
    assert stronger["displayed_confidence_band"] == "screening_rb"
    assert weaker["displayed_confidence_band"] == "screening_canon"
    assert stronger["displayed_confidence_band"] != weaker["displayed_confidence_band"]


def test_every_row_is_renderable_on_a_seeded_display_fixture(tmp_path):
    db = _build_expansion_db(tmp_path / "renderable.db", [
        _carrier("990000000000000001", "p001", confidence_band="tier_a"),
        _carrier("990000000000000002", "p002", confidence_band="screening_rb"),
        _carrier("990000000000000003", "p003", confidence_band="weak",
                 evidence_source=_PROPAGATED),
    ])
    items = _service_for(db).get_work_witnesses("wEXP001", **_ANCHOR_STRONG)
    assert len(items) == 3
    for row in items:
        assert row["library_code"], f"library_code missing on {row['representative_sys_id']}"
        assert row["shelfmark_display"], "shelfmark_display missing"
        assert row["band_label"], "band_label missing"
        assert row["display_missing"] is False
        expected = serialize_banded_claim({
            "evidence_source": row["displayed_evidence_source"],
            "confidence_band": row["displayed_confidence_band"],
            "adjudication_status": "unreviewed",
        }, "en")["band_label"]
        assert row["band_label"] == expected
        # ... and NOT the label of the other carrier's RAW pair, when they differ
        if (row["displayed_evidence_source"], row["displayed_confidence_band"]) != (
                row["evidence_source"], row["confidence_band"]):
            raw = serialize_banded_claim({
                "evidence_source": row["evidence_source"],
                "confidence_band": row["confidence_band"],
                "adjudication_status": "unreviewed",
            }, "en")["band_label"]
            assert row["band_label"] != raw


def test_band_label_tracks_the_resolved_pair_not_the_carriers_raw_pair(tmp_path):
    db = _build_expansion_db(tmp_path / "label-resolved.db", [
        _carrier("990000000000000001", "p001", confidence_band="tier_a"),
    ])
    row = _service_for(db).get_work_witnesses(
        "wEXP001", anchor_claim_type="direct_witness",
        anchor_evidence_source=_PROPAGATED, anchor_confidence_band="weak")[0]
    resolved = serialize_banded_claim(
        {"evidence_source": _PROPAGATED, "confidence_band": "weak",
         "adjudication_status": "unreviewed"}, "en")["band_label"]
    raw = serialize_banded_claim(
        {"evidence_source": _TRACK1, "confidence_band": "tier_a",
         "adjudication_status": "unreviewed"}, "en")["band_label"]
    assert row["band_label"] == resolved
    assert row["band_label"] != raw


def test_carrier_without_a_manuscript_display_row_is_returned_and_flagged(tmp_path):
    db = _build_expansion_db(
        tmp_path / "display-missing.db",
        [
            _carrier("990000000000000001", "p001"),
            _carrier("990000000000000002", "p002"),
        ],
        display={"990000000000000001": ("CUL", "T-S 12.1")},
    )
    items = _service_for(db).get_work_witnesses("wEXP001", **_ANCHOR_STRONG)
    assert len(items) == 2, "a carrier absent from manuscript_display must not vanish"
    by_sys = {r["representative_sys_id"]: r for r in items}
    named = by_sys["990000000000000001"]
    unnamed = by_sys["990000000000000002"]
    assert named["library_code"] == "CUL"
    assert named["display_missing"] is False
    assert unnamed["library_code"] is None
    assert unnamed["shelfmark_display"] is None
    assert unnamed["display_missing"] is True


def test_expansion_list_query_plan_is_index_driven_on_work_id(tmp_path):
    """The added `manuscript_display` LEFT JOIN must not turn the expansion
    into a table scan of `discovery_claim`."""
    from shared.discovery_service import build_work_expansion_rows_sql

    db = _build_expansion_db(tmp_path / "plan.db", [
        _carrier("990000000000000001", "p001"),
    ])
    sql, params = build_work_expansion_rows_sql(
        work_id="wEXP001", anchor_unit_key=None, anchor_evidence_source=None,
        anchor_confidence_band=None, enabled_bands=None, page_size=50, offset=0)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        plan = "\n".join(
            str(r) for r in conn.execute("EXPLAIN QUERY PLAN " + sql, params).fetchall())
    finally:
        conn.close()
    assert "ix_discovery_claim_work_id" in plan, (
        f"the expansion list query is no longer index-driven on work_id:\n{plan}")
    assert "SCAN discovery_claim" not in plan, (
        f"the expansion list query now SCANS discovery_claim:\n{plan}")


def test_enabled_band_filter_with_a_stronger_anchor_follows_the_other_carrier(tmp_path):
    db = _build_expansion_db(tmp_path / "filter-strong-anchor.db", [
        _carrier("990000000000000001", "p001", confidence_band="tier_a"),
        _carrier("990000000000000002", "p002", confidence_band="screening_rb"),
    ])
    service = _service_for(db)
    items = service.get_work_witnesses(
        "wEXP001", enabled_bands=["tier_a"], **_ANCHOR_STRONG)
    assert [r["representative_sys_id"] for r in items] == ["990000000000000001"]
    assert items[0]["displayed_confidence_band"] == "tier_a"


def test_enabled_band_filter_with_a_weaker_anchor_excludes_an_enabled_carrier(tmp_path):
    """The row the reader WOULD have seen at `tier_a` is excluded, because the
    band they actually see is the anchor's weaker one."""
    db = _build_expansion_db(tmp_path / "filter-weak-anchor.db", [
        _carrier("990000000000000001", "p001", confidence_band="tier_a"),
    ])
    service = _service_for(db)
    weak_anchor = dict(
        anchor_claim_type="direct_witness",
        anchor_evidence_source=_PROPAGATED,
        anchor_confidence_band="weak",
    )
    # The other carrier IS in an enabled band -- and the row is still excluded.
    assert service.get_work_witnesses(
        "wEXP001", enabled_bands=["tier_a"], **weak_anchor) == []
    # Enabling the RESOLVED (weaker) band admits it.
    admitted = service.get_work_witnesses(
        "wEXP001", enabled_bands=["weak"], **weak_anchor)
    assert len(admitted) == 1
    assert admitted[0]["displayed_confidence_band"] == "weak"


def test_no_anchor_call_filters_on_the_other_carriers_band_exactly_as_before(tmp_path):
    db = _build_expansion_db(tmp_path / "no-anchor-filter.db", [
        _carrier("990000000000000001", "p001", confidence_band="tier_a"),
        _carrier("990000000000000002", "p002", confidence_band="weak",
                 evidence_source=_PROPAGATED),
    ])
    service = _service_for(db)
    assert [r["representative_sys_id"]
            for r in service.get_work_witnesses("wEXP001", enabled_bands=["tier_a"])] == [
        "990000000000000001"]
    assert [r["representative_sys_id"]
            for r in service.get_work_witnesses("wEXP001", enabled_bands=["weak"])] == [
        "990000000000000002"]
    assert len(service.get_work_witnesses("wEXP001")) == 2


def test_no_anchor_call_keeps_the_nine_legacy_keys_with_null_anchor_fields(tmp_path):
    db = _build_expansion_db(tmp_path / "no-anchor-keys.db", [
        _carrier("990000000000000001", "p001"),
    ])
    row = _service_for(db).get_work_witnesses("wEXP001")[0]
    for key in ("work_id", "unit_id", "representative_sys_id", "representative_page_id",
                "representative_claim_id", "claim_type", "evidence_source",
                "confidence_band", "member_sys_ids"):
        assert key in row, f"pre-existing key {key!r} disappeared"
    assert row["anchor_claim_type"] is None
    assert row["anchor_evidence_source"] is None
    assert row["anchor_confidence_band"] is None
    assert row["relations_differ"] is False
    # the carrier fields are still present and populated on the no-anchor path
    assert row["displayed_confidence_band"] == row["confidence_band"]
    assert row["library_code"] == "CUL"


_ANCHOR_KEYS = ("anchor_claim_type", "anchor_evidence_source", "anchor_confidence_band")
_ANCHOR_VALUES = {
    "anchor_claim_type": "direct_witness",
    "anchor_evidence_source": _TRACK1,
    "anchor_confidence_band": "tier_a",
}


@pytest.mark.parametrize("present", [
    (),
    ("anchor_claim_type",),
    ("anchor_evidence_source",),
    ("anchor_confidence_band",),
    ("anchor_claim_type", "anchor_evidence_source"),
    ("anchor_claim_type", "anchor_confidence_band"),
    ("anchor_evidence_source", "anchor_confidence_band"),
    _ANCHOR_KEYS,
])
def test_anchor_identity_is_all_three_or_none_across_the_full_matrix(tmp_path, present):
    """All EIGHT combinations, not a spot check: the ambiguity lives exactly in
    the combinations nobody thinks to try."""
    db = _build_expansion_db(tmp_path / f"matrix-{len(present)}-{'_'.join(present)}.db", [
        _carrier("990000000000000001", "p001"),
    ])
    service = _service_for(db)
    kwargs = {k: _ANCHOR_VALUES[k] for k in present}
    if len(present) in (0, 3):
        items = service.get_work_witnesses("wEXP001", **kwargs)
        assert len(items) == 1
    else:
        with pytest.raises(ValueError):
            service.get_work_witnesses("wEXP001", **kwargs)


def test_partial_anchor_error_names_the_present_and_the_missing_fields(tmp_path):
    db = _build_expansion_db(tmp_path / "matrix-msg.db", [
        _carrier("990000000000000001", "p001"),
    ])
    with pytest.raises(ValueError) as exc:
        _service_for(db).get_work_witnesses(
            "wEXP001", anchor_claim_type="direct_witness")
    message = str(exc.value)
    assert "anchor_claim_type" in message, "the PRESENT field is not named"
    assert "anchor_evidence_source" in message, "a MISSING field is not named"
    assert "anchor_confidence_band" in message, "a MISSING field is not named"


def test_sql_path_and_pure_helper_agree_on_every_field_both_compute(tmp_path):
    carriers = [
        _carrier("990000000000000001", "p001", confidence_band="tier_a"),
        _carrier("990000000000000002", "p002", confidence_band="screening_rb",
                 claim_type="quotes_this_work"),
        _carrier("990000000000000003", "p003", confidence_band="weak",
                 evidence_source=_PROPAGATED),
    ]
    db = _build_expansion_db(tmp_path / "symmetry.db", carriers,
                             units=[("unitX", ["990000000000000001",
                                               "990000000000000002"])])
    anchor = dict(anchor_claim_type="direct_witness",
                  anchor_evidence_source=_TRACK1,
                  anchor_confidence_band="screening_canon")
    sql_items = _service_for(db).get_work_witnesses("wEXP001", **anchor)

    claim_rows = [
        {"page_id": c["page_id"], "work_id": "wEXP001", "claim_id": f"c{i:06d}",
         "claim_type": c["claim_type"], "sys_id": c["sys_id"],
         "evidence_source": c["evidence_source"],
         "confidence_band": c["confidence_band"],
         "library_code": "CUL", "shelfmark_display": f"T-S {c['sys_id'][-4:]}"}
        for i, c in enumerate(carriers)
    ]
    pure_items = _project_work_witnesses(
        claim_rows,
        {"990000000000000001": "unitX", "990000000000000002": "unitX"},
        **anchor,
    )
    # `band_label` is the one field the pure helper has no source for (it needs
    # the sidecar's band-measurement read and a UI language); everything else
    # must agree exactly.
    comparable = [k for k in sql_items[0] if k != "band_label"]
    assert sorted(comparable) == sorted(k for k in pure_items[0])
    for sql_row, pure_row in zip(sql_items, pure_items):
        for key in comparable:
            assert sql_row[key] == pure_row[key], f"SQL/pure disagreement on {key!r}"


def test_anchor_unit_still_excluded_and_same_unit_members_still_suppressed(tmp_path):
    db = _build_expansion_db(
        tmp_path / "unchanged-rules.db",
        [
            _carrier("990000000000000001", "p001"),
            _carrier("990000000000000002", "p002"),
            _carrier("990000000000000003", "p003"),
        ],
        units=[("unitX", ["990000000000000001", "990000000000000002"]),
               ("unitY", ["990000000000000003"])],
    )
    service = _service_for(db)
    # unitX collapses to ONE row (same-unit member suppression)
    assert len(service.get_work_witnesses("wEXP001")) == 2
    # anchoring on a MEMBER of unitX removes the whole unit
    anchored = service.get_work_witnesses(
        "wEXP001", anchor_sys_id="990000000000000002", **_ANCHOR_STRONG)
    assert [r["representative_sys_id"] for r in anchored] == ["990000000000000003"]


# ---------------------------------------------------------------------------
# Static guards: ONE band lattice, and no band_precision on the expansion path
# ---------------------------------------------------------------------------

def _service_module_source():
    import shared.discovery_service as svc
    return svc, inspect.getsource(svc)


def test_no_second_band_ordering_comparison_exists_in_the_service_module():
    """The weaker band is chosen through `_band_rank`; a second, hand-written
    ordering over `confidence_band` values would drift from the lattice."""
    _svc, source = _service_module_source()
    tree = ast.parse(source)
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        if not any(isinstance(op, (ast.Lt, ast.LtE, ast.Gt, ast.GtE))
                   for op in node.ops):
            continue
        operands = [node.left, *node.comparators]
        rendered = " ".join(ast.dump(o) for o in operands)
        if "confidence_band" in rendered:
            offenders.append(ast.unparse(node))
    assert offenders == [], (
        "confidence_band values are being ORDERED directly; the frozen lattice "
        f"in _band_rank is the only comparator: {offenders}")


def test_no_band_precision_join_was_added_to_any_expansion_query():
    from shared.discovery_service import (
        build_work_expansion_count_sql,
        build_work_expansion_rows_sql,
    )
    rows_sql, _ = build_work_expansion_rows_sql(
        work_id="w", anchor_unit_key="u", anchor_evidence_source=_TRACK1,
        anchor_confidence_band="tier_a", enabled_bands=["tier_a"],
        page_size=10, offset=0)
    count_sql, _ = build_work_expansion_count_sql(
        work_id="w", anchor_unit_key="u", anchor_evidence_source=_TRACK1,
        anchor_confidence_band="tier_a", enabled_bands=["tier_a"])
    for name, sql in (("rows", rows_sql), ("count", count_sql)):
        assert "band_precision" not in sql, (
            f"the {name} expansion query joins band_precision -- its "
            "precision/ci_low/ci_high columns would then sit on every returned row")
