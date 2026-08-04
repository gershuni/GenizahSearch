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
import asyncio
import inspect
import os
import sqlite3
import time

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from shared.discovery_band_labels import serialize_banded_claim
from shared.discovery_service import (
    DiscoveryService,
    _build_work_expansion_pipeline,
    _project_work_witnesses,
)
from shared.discovery_surface_projection import (
    _ALL_ALLOWLISTS,
    SURFACE_EXPANSION_FIELDS,
    is_forbidden_surface_field,
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


# ===========================================================================
# Task 2: the raising helper, the count query, the allowlist, the envelope
# ===========================================================================

# --- forced-failure injection -------------------------------------------
#
# The THREE queries on this path -- the count, the paginated row query and the
# member-sys_ids follow-up -- read the SAME four tables, and they run in that
# order. Dropping a table therefore only ever exercises whichever runs FIRST,
# which is exactly the "one combined 'a query failed' test" the acceptance
# criteria reject: an implementation can map one and swallow the others.
#
# So each is failed INDIVIDUALLY by injecting a REAL `sqlite3.OperationalError`
# at the statement that query issues -- a genuine driver failure inside the
# query path, against a real-shaped fixture, not a faked return value. A
# companion test below ALSO drops a real table, so the ordinary
# missing-table shape is covered too.

_COUNT_MARKER = "SELECT COUNT(*) AS n FROM filtered"
_ROWS_MARKER = "LIMIT ? OFFSET ?"
_MEMBERS_MARKER = "SELECT unit_key, sys_id FROM ranked"


class _StatementFailingConnection:
    def __init__(self, inner, marker):
        self._inner = inner
        self._marker = marker

    def execute(self, sql, parameters=()):
        if self._marker in sql:
            raise sqlite3.OperationalError("injected statement failure")
        return self._inner.execute(sql, parameters)

    def __getattr__(self, name):
        return getattr(self._inner, name)


def _service_failing_at(db, marker):
    service = _service_for(db)
    real = service._get_conn()
    assert real is not None
    service._get_conn = lambda: _StatementFailingConnection(real, marker)
    return service


@pytest.fixture()
def failing_db(tmp_path):
    return _build_expansion_db(tmp_path / "forced-failure.db", [
        _carrier("990000000000000001", "p001"),
        _carrier("990000000000000002", "p002"),
    ])


@pytest.mark.parametrize("marker,label", [
    (_COUNT_MARKER, "count"),
    (_ROWS_MARKER, "page"),
    (_MEMBERS_MARKER, "member"),
])
def test_each_failed_query_is_an_outage_never_ok_with_zero(failing_db, marker, label):
    envelope = _service_failing_at(failing_db, marker).get_work_expansion_enveloped(
        "wEXP001")
    assert envelope["status"] != "ok", (
        f"a failed {label} query reported `ok` -- the false-zero class 136-14 fixed")
    assert envelope["status"] in ("unavailable", "timeout")
    assert envelope["items"] == []
    assert envelope["total"] == 0
    assert envelope["meta"]["reason"] == "query_failed"


@pytest.mark.parametrize("marker,label", [
    (_COUNT_MARKER, "count"),
    (_ROWS_MARKER, "page"),
    (_MEMBERS_MARKER, "member"),
])
def test_legacy_list_api_still_returns_empty_on_each_of_the_same_failures(
        failing_db, marker, label):
    """`get_work_witnesses`' contract is unchanged: a list, `[]` on failure,
    never an exception. Existing callers must not start seeing raises."""
    assert _service_failing_at(failing_db, marker).get_work_witnesses("wEXP001") == []


def test_a_genuinely_dropped_table_is_an_outage_not_a_zero(tmp_path):
    db = _build_expansion_db(tmp_path / "dropped-table.db", [
        _carrier("990000000000000001", "p001"),
    ])
    conn = sqlite3.connect(db)
    try:
        conn.execute("DROP TABLE manuscript_display")
        conn.commit()
    finally:
        conn.close()
    service = _service_for(db)
    envelope = service.get_work_expansion_enveloped("wEXP001")
    assert envelope["status"] == "unavailable"
    assert envelope["meta"]["reason"] == "query_failed"
    assert envelope["total"] == 0
    assert service.get_work_witnesses("wEXP001") == []


# --- the total is the count, not the page length -------------------------

def test_total_is_the_full_count_not_the_length_of_the_returned_page(tmp_path):
    db = _build_expansion_db(tmp_path / "multipage.db", [
        _carrier(f"99000000000000{i:04d}", f"p{i:04d}") for i in range(7)
    ])
    service = _service_for(db)
    seen = 0
    for page in range(1, 5):
        envelope = service.get_work_expansion_enveloped("wEXP001", page=page, page_size=2)
        assert envelope["total"] == 7, "the page length was substituted for the total"
        seen += len(envelope["items"])
        assert len(envelope["items"]) <= 2
    assert seen == 7, "the pages do not sum to the reported total"
    first = service.get_work_expansion_enveloped("wEXP001", page=1, page_size=2)
    assert first["total"] > len(first["items"])


def test_the_count_honours_anchor_exclusion_and_the_enabled_band_filter(tmp_path):
    db = _build_expansion_db(
        tmp_path / "count-filters.db",
        [
            _carrier("990000000000000001", "p001", confidence_band="tier_a"),
            _carrier("990000000000000002", "p002", confidence_band="tier_a"),
            _carrier("990000000000000003", "p003", confidence_band="weak",
                     evidence_source=_PROPAGATED),
        ],
    )
    service = _service_for(db)
    assert service.get_work_expansion_enveloped("wEXP001")["total"] == 3
    assert service.get_work_expansion_enveloped(
        "wEXP001", anchor_sys_id="990000000000000001")["total"] == 2
    assert service.get_work_expansion_enveloped(
        "wEXP001", enabled_bands=["tier_a"])["total"] == 2
    assert service.get_work_expansion_enveloped(
        "wEXP001", enabled_bands=["tier_a"],
        anchor_sys_id="990000000000000001")["total"] == 1


def test_count_and_list_agree_under_a_weaker_anchor_filter(tmp_path):
    """The FILTERING stage -- not just the grouping stage -- decides membership
    here. A count sharing only the raw CTE would have missed it."""
    db = _build_expansion_db(tmp_path / "weak-anchor-count.db", [
        _carrier("990000000000000001", "p001", confidence_band="tier_a"),
        _carrier("990000000000000002", "p002", confidence_band="screening_rb"),
    ])
    weak_anchor = dict(anchor_claim_type="direct_witness",
                       anchor_evidence_source=_PROPAGATED,
                       anchor_confidence_band="weak")
    service = _service_for(db)
    # Every carrier resolves to the anchor's `weak` band, so `tier_a` -- which
    # one carrier genuinely IS in -- must count ZERO.
    tier_a = service.get_work_expansion_enveloped(
        "wEXP001", enabled_bands=["tier_a"], **weak_anchor)
    assert tier_a["total"] == 0
    assert tier_a["items"] == []
    resolved = service.get_work_expansion_enveloped(
        "wEXP001", enabled_bands=["weak"], **weak_anchor)
    assert resolved["total"] == 2 == len(resolved["items"])


def test_the_count_is_built_from_the_same_factored_pipeline_as_the_list():
    from shared.discovery_service import (
        build_work_expansion_count_sql,
        build_work_expansion_rows_sql,
    )
    kwargs = dict(work_id="w", anchor_unit_key="unitZ",
                  anchor_evidence_source=_PROPAGATED,
                  anchor_confidence_band="weak", enabled_bands=["weak", "tier_a"])
    pipeline, pipeline_params = _build_work_expansion_pipeline(**kwargs)
    rows_sql, rows_params = build_work_expansion_rows_sql(page_size=10, offset=0, **kwargs)
    count_sql, count_params = build_work_expansion_count_sql(**kwargs)
    assert rows_sql.startswith(pipeline), "the row query does not use the shared pipeline"
    assert count_sql.startswith(pipeline), "the count query does not use the shared pipeline"
    assert count_params == pipeline_params
    assert rows_params == [*pipeline_params, 10, 0]
    # No SECOND grouping and no SECOND filtering written into the count.
    count_tail = count_sql[len(pipeline):]
    for forbidden in ("ROW_NUMBER", "PARTITION BY", "GROUP BY", "WHERE", "DISTINCT",
                      "LIMIT"):
        assert forbidden not in count_tail.upper(), (
            f"the count query writes its own {forbidden} instead of reusing the "
            f"shared filtered pipeline: {count_tail!r}")


def test_a_count_timeout_yields_timeout_and_never_substitutes_a_page_length(monkeypatch):
    """A count that cannot be produced in budget degrades to `timeout` -- an
    honest temporary failure -- rather than reporting a number nobody can
    reproduce."""
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_BROWSE", "0.05")
    service = DiscoveryService(
        path_provider=lambda: "unused", availability_callable=lambda: True,
        sidecar_version_provider=lambda: "v")

    def _slow(*_args, **_kwargs):
        time.sleep(0.5)
        return {"status": "ok", "items": [{"x": 1}] * 3, "total": 3, "meta": {}}

    service.get_work_expansion_enveloped = _slow
    envelope = asyncio.run(service.get_work_expansion_enveloped_async("wEXP001"))
    assert envelope["status"] == "timeout"
    assert envelope["items"] == []
    assert envelope["total"] == 0, "a page length was substituted for a real total"
    assert envelope["meta"]["reason"] == "query_timeout"


# --- (i)/(ii): no approximate or capped total is REACHABLE ----------------

_APPROXIMATION_TOKENS = ("approximate", "estimated", "sampled", "capped")


def test_expansion_meta_names_no_approximation_on_ok_or_on_an_outage(tmp_path):
    db = _build_expansion_db(tmp_path / "meta-absence.db", [
        _carrier("990000000000000001", "p001"),
    ])
    ok = _service_for(db).get_work_expansion_enveloped("wEXP001")
    assert ok["status"] == "ok"
    outage = _service_failing_at(db, _COUNT_MARKER).get_work_expansion_enveloped("wEXP001")
    assert outage["status"] == "unavailable"
    for label, envelope in (("ok", ok), ("outage", outage)):
        for key in envelope["meta"]:
            lowered = str(key).lower()
            for token in _APPROXIMATION_TOKENS:
                assert token not in lowered, (
                    f"the {label} envelope's meta carries an approximation key "
                    f"{key!r}")
    for field in SURFACE_EXPANSION_FIELDS:
        for token in _APPROXIMATION_TOKENS:
            assert token not in field.lower(), (
                f"SURFACE_EXPANSION_FIELDS names an approximation field {field!r}")


#: Every function this plan added or rewrote -- the "new code" the source scan
#: below covers. Scoped deliberately: the findings page legitimately carries its
#: OWN `approximate_total` in its own meta (plan 136-14), and a module-wide scan
#: would either fail on that unrelated surface or have to be weakened until it
#: caught nothing.
_EXPANSION_FUNCTIONS = (
    ("shared.discovery_service", "_build_work_witnesses_ranked_cte_sql"),
    ("shared.discovery_service", "_validate_anchor_identity"),
    ("shared.discovery_service", "_resolve_displayed_band"),
    ("shared.discovery_service", "_build_work_expansion_pipeline"),
    ("shared.discovery_service", "build_work_expansion_rows_sql"),
    ("shared.discovery_service", "build_work_expansion_count_sql"),
    ("shared.discovery_service", "_project_work_witnesses"),
    ("shared.discovery_service", "_present_expansion_row"),
    ("shared.discovery_service", "get_work_witnesses"),
    ("shared.discovery_service", "_query_work_expansion"),
    ("shared.discovery_service", "get_work_expansion_enveloped"),
    ("shared.discovery_service", "get_work_expansion_enveloped_async"),
    ("shared.discovery_surface_projection", "surface_safe_expansion"),
    ("web.discovery", "get_work_expansion_enveloped"),
)


def _expansion_code_lines():
    """Every SOURCE line of the new expansion code, with DOCSTRING and COMMENT
    lines removed. The exclusion is load-bearing: the docstrings this plan
    requires explain at length WHY there is no approximate total, and would
    otherwise invalidate their own gate."""
    import importlib

    out = []
    seen_modules = {}
    for module_name, func_name in _EXPANSION_FUNCTIONS:
        if module_name not in seen_modules:
            module = importlib.import_module(module_name)
            src = inspect.getsource(module)
            seen_modules[module_name] = (src, ast.parse(src), src.splitlines())
        src, tree, lines = seen_modules[module_name]
        func = _function_node(tree, func_name, module_name)
        doc_lines = set()
        for node in ast.walk(func):
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) \
                    and isinstance(node.value.value, str):
                doc_lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
        for i in range(func.lineno, (func.end_lineno or func.lineno) + 1):
            if i in doc_lines:
                continue
            out.append((module_name, func_name, i, lines[i - 1].split("#", 1)[0]))
    return out


def test_source_scan_finds_no_approximate_or_limit_bounded_count():
    offenders = [
        (module, func, lineno, code.strip())
        for module, func, lineno, code in _expansion_code_lines()
        if any(f"{token}_total" in code.lower() or f"{token} total" in code.lower()
               for token in _APPROXIMATION_TOKENS)
    ]
    assert offenders == [], (
        f"an approximate/estimated/sampled/capped total exists in the new "
        f"expansion code: {offenders}")

    # The allowlist itself may not name one either.
    for field in SURFACE_EXPANSION_FIELDS:
        assert not any(t in field.lower() for t in _APPROXIMATION_TOKENS), field

    # A LIMIT-bounded count is the other shape: `SELECT COUNT(*) FROM (... LIMIT n)`.
    from shared.discovery_service import build_work_expansion_count_sql
    count_sql, _ = build_work_expansion_count_sql(
        work_id="w", anchor_unit_key=None, anchor_evidence_source=None,
        anchor_confidence_band=None, enabled_bands=None)
    assert "LIMIT" not in count_sql.upper(), (
        f"the expansion count is LIMIT-bounded: {count_sql!r}")


# --- (iii)/(vii): exhaustive pagination on a >10,000-unit fixture ---------

#: STRICTLY MORE THAN 10,000 units, and the number is the point: "more units
#: than several pages" is satisfied by a four-page fixture, and
#: `min(exact_total, 1000)` -- which carries no forbidden word, no SQL LIMIT and
#: nothing an absence assertion or a source scan can see -- passes on four
#: pages. 10,000 sits above every round-number cap a developer plausibly
#: inserts (100 / 500 / 1,000 / 5,000 / 10,000).
_SYNTHETIC_ENABLED_UNITS = 10_301
_SYNTHETIC_FILTERED_OUT_UNITS = 100
_SYNTHETIC_TOTAL_UNITS = _SYNTHETIC_ENABLED_UNITS + _SYNTHETIC_FILTERED_OUT_UNITS

#: The largest page size docs/specs/discovery-budgets.md §1.2 permits, so an
#: exhaustive walk of a 10k-unit set is tens of queries rather than hundreds.
_BUDGET_MAX_PAGE_SIZE = 200

#: Deliberately OUTSIDE the generated `f"990000000000{i:08d}"` sys_id space --
#: an anchor that collides with carrier 0 adds no unit of its own and silently
#: turns every cardinality assertion below off by one.
_SYNTHETIC_ANCHOR_SYS = "99ANCHOR000000000000"


@pytest.fixture(scope="module")
def large_expansion_db(tmp_path_factory):
    """>10,000 witness units, built with ONE executemany per table: it exists to
    exceed a cap, not to be realistic, and an expensive fixture is how a
    cardinality floor gets quietly lowered to make a suite fast."""
    db_path = tmp_path_factory.mktemp("expansion-large") / "large.db"
    work_id = "wLARGE1"
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, "
            "genre, source_corpus) VALUES (?, ?, ?, ?, ?, ?)",
            (work_id, work_id, "Synthetic Large Expansion", None, None, "sefaria"),
        )

        def _rows():
            for i in range(_SYNTHETIC_TOTAL_UNITS):
                enabled = i < _SYNTHETIC_ENABLED_UNITS
                yield (
                    f"lp{i:07d}", f"c{i:07d}", f"e{i:07d}", f"990000000000{i:08d}",
                    _TRACK1 if enabled else _PROPAGATED,
                    "tier_a" if enabled else "weak",
                )

        specs = list(_rows())
        cur.executemany(
            "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
            "display_evidence_id, source_corpus, sidecar_version) "
            "VALUES (?, ?, ?, 'direct_witness', ?, 'sefaria', 'test-large')",
            [(page, work_id, claim, ev) for page, claim, ev, _s, _es, _b in specs],
        )
        cur.executemany(
            "INSERT INTO discovery_evidence (evidence_id, claim_id, evidence_kind, "
            "evidence_source, confidence_band, adjudication_status, audit_status, "
            "routing_status, routing_reason, a_page_id, sys_id, span_start, span_end) "
            "VALUES (?, ?, 'witness', ?, ?, 'unreviewed', 'n/a', 'shipped', 'none', "
            "?, ?, 0, 10)",
            [(ev, claim, es, band, page, sys_id)
             for page, claim, ev, sys_id, es, band in specs],
        )
        cur.executemany(
            "INSERT INTO manuscript_display (sys_id, library_code, library_sort_key, "
            "shelfmark_display, shelfmark_sort_key) VALUES (?, 'CUL', 'CUL', ?, ?)",
            [(sys_id, f"T-S {sys_id[-6:]}", f"T-S {sys_id[-6:]}")
             for _p, _c, _e, sys_id, _es, _b in specs],
        )
        # The anchor: its own claim on the same work, so excluding its unit is a
        # real exclusion rather than a no-op.
        cur.execute(
            "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
            "display_evidence_id, source_corpus, sidecar_version) "
            "VALUES ('lpANCHOR', ?, 'cANCHOR', 'direct_witness', 'eANCHOR', "
            "'sefaria', 'test-large')", (work_id,))
        cur.execute(
            "INSERT INTO discovery_evidence (evidence_id, claim_id, evidence_kind, "
            "evidence_source, confidence_band, adjudication_status, audit_status, "
            "routing_status, routing_reason, a_page_id, sys_id, span_start, span_end) "
            "VALUES ('eANCHOR', 'cANCHOR', 'witness', ?, 'tier_a', 'unreviewed', "
            "'n/a', 'shipped', 'none', 'lpANCHOR', ?, 0, 10)",
            (_TRACK1, _SYNTHETIC_ANCHOR_SYS))
        cur.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [("schema_version", "discovery-v1"), ("sidecar_version", "test-large"),
             ("audience", "public")],
        )
        conn.commit()
    finally:
        conn.close()
    return str(db_path), work_id


def _paginate_to_exhaustion(service, work_id, **call_kwargs):
    """Walk the LIST query to exhaustion and return (row_count, reported_total).

    The row count is an INDEPENDENT figure: it comes from the list query, so it
    cannot agree with the count query by sharing a defect."""
    seen = set()
    reported = None
    page = 1
    while True:
        envelope = service.get_work_expansion_enveloped(
            work_id, page=page, page_size=_BUDGET_MAX_PAGE_SIZE, **call_kwargs)
        assert envelope["status"] == "ok", envelope
        if reported is None:
            reported = envelope["total"]
        else:
            assert envelope["total"] == reported, "the total moved between pages"
        if not envelope["items"]:
            break
        for item in envelope["items"]:
            seen.add((item["unit_id"], item["representative_sys_id"]))
        page += 1
        assert page < 500, "pagination did not terminate"
    return len(seen), reported


def derived_mutation_caps():
    """The two mutation-control cap values, COMPUTED from the measured real
    maximum -- never written as constants. A plan that hardcodes "5,000 is above
    every real work" is making the round-8 grain mistake in a different place;
    the measured maximum moves with every bake.

    Returns `(real_max, cap_below, cap_above)`:
      * `cap_below` sits STRICTLY BELOW the real maximum -- it must break BOTH
        the real-artifact probe and the synthetic run;
      * `cap_above` sits STRICTLY ABOVE the real maximum and STRICTLY BELOW the
        synthetic fixture -- it must break ONLY the synthetic run, which is what
        proves the synthetic floor is load-bearing rather than decorative.
    """
    real_max = _measured_real_expansion_maximum()
    if real_max is None:
        pytest.fail(_PROBE_UNRESOLVED_MESSAGE)
    return real_max, real_max - 1, real_max + 1


def test_synthetic_floor_strictly_exceeds_the_measured_real_maximum():
    """If a future bake grows a work past the floor, this fails BY NAME and
    tells you to raise it -- rather than leaving mutation control (v)(b)
    silently inert."""
    real_max, cap_below, cap_above = derived_mutation_caps()
    synthetic = _SYNTHETIC_TOTAL_UNITS + 1  # + the anchor's own unit
    assert synthetic > real_max, (
        f"the synthetic cardinality floor ({synthetic}) no longer exceeds the "
        f"real artifact's largest expansion ({real_max}) -- RAISE THE FLOOR, or "
        "the cap-just-above-the-real-maximum mutation control is inert")
    # Each derived cap is proved to be on the side of the maximum it is
    # supposed to be on -- otherwise the control it drives is inert and this
    # says so BY NAME rather than passing silently.
    assert cap_below < real_max, (
        f"cap_below ({cap_below}) is not strictly below the real maximum "
        f"({real_max}) -- mutation control (v)(a) would not break the real probe")
    assert real_max < cap_above < synthetic, (
        f"cap_above ({cap_above}) does not sit strictly between the real maximum "
        f"({real_max}) and the synthetic floor ({synthetic}) -- mutation control "
        "(v)(b) would not distinguish them")


def test_total_survives_exhaustive_pagination_on_more_than_10000_units(
        large_expansion_db):
    db_path, work_id = large_expansion_db
    walked, reported = _paginate_to_exhaustion(_service_for(db_path, "test-large"), work_id)
    assert _SYNTHETIC_TOTAL_UNITS + 1 > 10_000
    assert reported == _SYNTHETIC_TOTAL_UNITS + 1, (
        "the reported total does not match the fixture's cardinality -- a cap or "
        "an approximation is in the path")
    assert walked == reported, (
        f"exhaustive pagination reached {walked} units but the envelope reported "
        f"{reported} -- the count is capped or transformed")


def test_total_survives_exhaustion_through_an_anchored_band_filtered_call(
        large_expansion_db):
    """(vii) -- the runtime pair of the AST control. A cap applied ONLY on the
    anchored or band-filtered branch takes no part in the unanchored runs above;
    this call takes it, and still carries more surviving units than any
    plausible cap."""
    db_path, work_id = large_expansion_db
    service = _service_for(db_path, "test-large")
    anchored_kwargs = dict(
        anchor_sys_id=_SYNTHETIC_ANCHOR_SYS,
        anchor_claim_type="direct_witness",
        anchor_evidence_source=_TRACK1,
        anchor_confidence_band="tier_a",
        enabled_bands=["tier_a"],
    )
    walked, reported = _paginate_to_exhaustion(service, work_id, **anchored_kwargs)
    unfiltered = service.get_work_expansion_enveloped(work_id, page_size=1)["total"]
    # BOTH filters must actually bite, or the shape is not being exercised.
    assert reported != unfiltered, "neither the anchor nor the band filter bit"
    assert service.get_work_expansion_enveloped(
        work_id, page_size=1, anchor_sys_id=_SYNTHETIC_ANCHOR_SYS)["total"] == \
        unfiltered - 1, "the anchor exclusion did not bite"
    assert service.get_work_expansion_enveloped(
        work_id, page_size=1, enabled_bands=["tier_a"])["total"] == \
        _SYNTHETIC_ENABLED_UNITS + 1, "the band filter did not bite"
    assert reported > 10_000, (
        f"only {reported} units survive the anchored, band-filtered call -- the "
        "fixture no longer carries more than any plausible cap")
    assert walked == reported, (
        f"exhaustive pagination through the ANCHORED, BAND-FILTERED shape reached "
        f"{walked} units but the envelope reported {reported} -- a cap on that "
        "branch would be invisible to the unanchored runs")


# --- (iv): the REAL artifact's highest-cardinality expansion --------------

_PROBE_ENV_VAR = "DISCOVERY_EXPANSION_PROBE_DB"
_PROBE_UNRESOLVED_MESSAGE = (
    "the real-artifact expansion probe could not resolve an artifact: set "
    f"{_PROBE_ENV_VAR} to a discovery sidecar, or point discovery_data/manifest.json "
    "at an artifact that passes the public loader's audience + required-table "
    "checks. Fail-closed BY DESIGN -- an unresolvable artifact is recorded NOT MET, "
    "never skipped and never quietly passed (plan 136-21 criterion (iv))."
)
_PROBE_REQUIRED_TABLES = frozenset({
    "works", "discovery_claim", "discovery_evidence", "witness_units",
    "witness_unit_members", "meta", "band_precision", "discovery_identification",
    "manuscript_display",
})


def _resolve_probe_artifact():
    """`(path, audience)` for the artifact to probe, or None.

    `DISCOVERY_EXPANSION_PROBE_DB` if set; otherwise the repository manifest's
    OWN selection -- and only if it passes the SAME `meta.audience == 'public'`
    and required-table checks the public loader applies, so a stale pre-rebuild
    asset is refused rather than silently probed."""
    override = os.environ.get(_PROBE_ENV_VAR)
    candidates = []
    if override:
        candidates.append(override)
    else:
        try:
            import web.discovery_assets as da

            path, _manifest = da._resolve_versioned_db()
            candidates.append(path)
        except Exception:
            return None
    for path in candidates:
        if not path or not os.path.exists(path):
            continue
        try:
            conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        except Exception:
            continue
        try:
            row = conn.execute("SELECT value FROM meta WHERE key = 'audience'").fetchone()
            audience = row[0] if row else None
            tables = {r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'")}
        except Exception:
            continue
        finally:
            conn.close()
        if audience != "public":
            continue
        if not _PROBE_REQUIRED_TABLES.issubset(tables):
            continue
        return path, audience
    return None


_PROBE_CACHE = {}


def _probe_largest_expansion():
    """`(path, audience, work_id, ranked_units)` for the work with the MOST
    distinct witness units, or None when no artifact resolves.

    The ranking is produced by the SAME factored `ranked -> unit-best ->
    filtered` fragment the list and the count are built from -- IMPORTED, never
    retyped -- run under the SAME parameters as the probe call itself
    (unanchored, unrestricted) and grouped by `work_id`. That keeps the grain
    correct BY CONSTRUCTION: distinct `COALESCE(wum.unit_id, 'sys:' || de.sys_id)`
    under the CTE's own claim-type filter, never identification rows per
    canonical work (a different table at a different grain).

    Sampling the RAW ranking and then running a filtered call on the samples is
    unsound: the band filter and the anchor rules run BELOW the CTE, so a work
    outside the sample can retain more rows after filtering than every sampled
    work. Making the ranking and the CALL the same shape is what closes it --
    with no anchor and no band filter, the rows the call returns ARE the CTE's
    distinct-unit_key set, so the top-ranked work is provably the maximum.
    """
    if "result" in _PROBE_CACHE:
        return _PROBE_CACHE["result"]
    resolved = _resolve_probe_artifact()
    if resolved is None:
        _PROBE_CACHE["result"] = None
        return None
    path, audience = resolved
    pipeline, params = _build_work_expansion_pipeline(
        work_id=None, anchor_unit_key=None, anchor_evidence_source=None,
        anchor_confidence_band=None, enabled_bands=None)
    sql = (pipeline
           + "\nSELECT work_id, COUNT(*) AS n FROM filtered "
             "GROUP BY work_id ORDER BY n DESC, work_id ASC LIMIT 1")
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        row = conn.execute(sql, params).fetchone()
    finally:
        conn.close()
    result = None if row is None else (path, audience, row[0], row[1])
    _PROBE_CACHE["result"] = result
    return result


def _measured_real_expansion_maximum():
    probed = _probe_largest_expansion()
    return None if probed is None else probed[3]


def test_total_survives_exhaustive_pagination_on_the_real_largest_expansion():
    probed = _probe_largest_expansion()
    if probed is None:
        pytest.fail(_PROBE_UNRESOLVED_MESSAGE)
    path, audience, work_id, ranked_units = probed
    assert audience == "public"
    service = DiscoveryService(
        path_provider=lambda: path, availability_callable=lambda: True,
        sidecar_version_provider=lambda: "probe")
    # Validity check on the PROBE itself: if the ranking does not describe the
    # call shape, we are silently measuring a smaller work.
    reported_first = service.get_work_expansion_enveloped(
        work_id, page_size=1)["total"]
    assert reported_first == ranked_units, (
        f"PROBE INVALID: the enveloped call reports {reported_first} units for the "
        f"top-ranked work but the ranking says {ranked_units} -- the ranking does "
        "not describe the call being made")
    walked, reported = _paginate_to_exhaustion(service, work_id)
    assert reported == ranked_units
    assert walked == reported, (
        f"exhaustive pagination reached {walked} units on the real artifact's "
        f"largest expansion but the envelope reported {reported}")


# --- the envelope shape, and the allowlist's registration ----------------

def test_the_envelope_carries_exactly_four_keys_and_names_its_filter_basis(tmp_path):
    db = _build_expansion_db(tmp_path / "envelope-shape.db", [
        _carrier("990000000000000001", "p001"),
    ])
    service = _service_for(db)
    unanchored = service.get_work_expansion_enveloped("wEXP001")
    anchored = service.get_work_expansion_enveloped("wEXP001", **_ANCHOR_STRONG)
    outage = _service_failing_at(db, _COUNT_MARKER).get_work_expansion_enveloped("wEXP001")
    for label, envelope in (("unanchored", unanchored), ("anchored", anchored),
                            ("outage", outage)):
        assert set(envelope) == {"status", "items", "total", "meta"}, (
            f"the {label} envelope is not the four-key shape: {sorted(envelope)}")
    assert unanchored["meta"]["anchor_mode"] == "unanchored"
    assert unanchored["meta"]["filter_basis"] == "other_carrier_band"
    assert anchored["meta"]["anchor_mode"] == "anchored"
    assert anchored["meta"]["filter_basis"] == "displayed_band"


def test_surface_expansion_fields_is_registered_in_all_allowlists():
    registered = dict(_ALL_ALLOWLISTS)
    assert "SURFACE_EXPANSION_FIELDS" in registered, (
        "an unregistered allowlist is not checked by the import-time "
        "forbidden-field guard -- the mechanism would be defeated silently")
    assert registered["SURFACE_EXPANSION_FIELDS"] == SURFACE_EXPANSION_FIELDS


def test_the_import_time_guard_would_reject_a_forbidden_name_in_this_allowlist():
    """Proves the registration is LOAD-BEARING rather than decorative: seed a
    forbidden name into a COPY of the allowlist and re-run the guard's own
    predicate over it."""
    poisoned = SURFACE_EXPANSION_FIELDS + ("band_precision",)
    leaks = sorted(f for f in poisoned if is_forbidden_surface_field(f))
    assert leaks == ["band_precision"]
    clean = sorted(f for f in SURFACE_EXPANSION_FIELDS if is_forbidden_surface_field(f))
    assert clean == [], f"SURFACE_EXPANSION_FIELDS itself names forbidden fields: {clean}"


def test_every_expansion_row_leaves_through_the_surface_safe_projection(tmp_path):
    db = _build_expansion_db(tmp_path / "projected.db", [
        _carrier("990000000000000001", "p001"),
    ])
    envelope = _service_for(db).get_work_expansion_enveloped("wEXP001", **_ANCHOR_STRONG)
    assert envelope["items"]
    for item in envelope["items"]:
        assert set(item) == set(SURFACE_EXPANSION_FIELDS)


# --- (vi): THE STRUCTURAL CONTROL ---------------------------------------

_TRANSFORMING_CALLS = {"min", "max", "abs", "len", "sorted", "round", "int", "float"}


def _function_node(tree, name, where="shared/discovery_service.py"):
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    raise AssertionError(f"{name} not found in {where}")


def _assert_untransformed(func, chain, source_lines, where):
    """FAIL if any name on `chain` is re-bound, transformed, or consumed by a
    transforming call anywhere in `func`."""
    def _line(node):
        return source_lines[node.lineno - 1].strip()

    bindings = {}
    for node in ast.walk(func):
        targets = []
        if isinstance(node, ast.Assign):
            targets = node.targets
        elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
            targets = [node.target]
        for target in targets:
            names = ([e.id for e in target.elts if isinstance(e, ast.Name)]
                     if isinstance(target, ast.Tuple) else
                     ([target.id] if isinstance(target, ast.Name) else []))
            for name in names:
                if name in chain:
                    bindings.setdefault(name, []).append(node)
        if isinstance(node, (ast.BinOp, ast.UnaryOp, ast.Compare, ast.IfExp, ast.BoolOp)):
            for sub in ast.walk(node):
                if isinstance(sub, ast.Name) and sub.id in chain:
                    raise AssertionError(
                        f"the exact count is transformed before the envelope "
                        f"[{where}] at shared/discovery_service.py:{node.lineno} -- "
                        f"{type(node).__name__} over {sub.id!r}: {_line(node)}")
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) \
                and node.func.id in _TRANSFORMING_CALLS:
            for sub in ast.walk(node):
                if isinstance(sub, ast.Name) and sub.id in chain:
                    raise AssertionError(
                        f"the exact count is transformed before the envelope "
                        f"[{where}] at shared/discovery_service.py:{node.lineno} -- "
                        f"{node.func.id}() over {sub.id!r}: {_line(node)}")
    for name, nodes in bindings.items():
        if len(nodes) > 1:
            raise AssertionError(
                f"the exact count is transformed before the envelope [{where}] -- "
                f"{name!r} is re-bound at lines "
                f"{[n.lineno for n in nodes]}: {[_line(n) for n in nodes]}")


def _shape_ok(node, chain):
    """A binding's right-hand side (or a returned expression) may ONLY be a bare
    Name on the chain, or a Subscript of one."""
    if isinstance(node, ast.Name):
        return node.id in chain
    if isinstance(node, ast.Subscript):
        return isinstance(node.value, ast.Name) and node.value.id in chain
    return False


def test_the_exact_count_reaches_the_envelope_untransformed():
    """The PRIMARY, shape-prohibiting control (T-136-21-12).

    Two LOCAL walks, anchored on the `(rows, total)` contract so the walk is
    deterministic rather than a search:
      (1) inside `_query_work_expansion`, from the fetch that consumes the count
          SQL to the SECOND position of the returned tuple;
      (2) inside `get_work_expansion_enveloped`, from the name the unpacking
          binds to that position, to the expression handed to `total=`.

    ITS OWN LIMIT, stated so it is never mistaken for complete: it follows LOCAL
    bindings inside those two functions. It cannot follow a value through a
    THIRD function, into a dict or an object attribute, or across modules. That
    blind spot is covered by its runtime pair --
    `test_total_survives_exhaustion_through_an_anchored_band_filtered_call` --
    which catches the EFFECT wherever the shape was hidden. Neither substitutes
    for the other and BOTH are required; do not delete either as redundant.

    A literal `0` is the ONE permitted second-position expression on a return
    that is NOT on the count chain (the unavailable / empty-band guards, which
    run before any count exists). A bare `0` cannot carry a cap.
    """
    import shared.discovery_service as svc

    source = inspect.getsource(svc)
    source_lines = source.splitlines()
    tree = ast.parse(source)

    # ---- walk (1): _query_work_expansion --------------------------------
    query_fn = _function_node(tree, "_query_work_expansion")
    fetch_assign = None
    for node in ast.walk(query_fn):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        call = node.value
        if not (isinstance(call.func, ast.Attribute)
                and call.func.attr in ("fetchone", "fetchall")):
            continue
        if "count_sql" not in ast.unparse(call):
            continue
        assert fetch_assign is None, "more than one count fetch site"
        fetch_assign = node
    assert fetch_assign is not None, (
        "no `conn.execute(count_sql, ...).fetchone()` site found in "
        "_query_work_expansion -- the count no longer has a single, locatable "
        "fetch, so the untransformed-count contract cannot be checked")
    chain = {t.id for t in fetch_assign.targets if isinstance(t, ast.Name)}
    assert chain, "the count fetch does not bind a simple name"
    # grow the chain over bare-Name / Subscript re-derivations
    for node in ast.walk(query_fn):
        if isinstance(node, ast.Assign) and _shape_ok(node.value, chain):
            chain.update(t.id for t in node.targets if isinstance(t, ast.Name))

    returns = [n for n in ast.walk(query_fn) if isinstance(n, ast.Return)]
    on_chain = []
    for ret in returns:
        assert isinstance(ret.value, ast.Tuple) and len(ret.value.elts) == 2, (
            "_query_work_expansion does not return a literal 2-tuple at "
            f"shared/discovery_service.py:{ret.lineno} -- the stated `(rows, total)` "
            "contract is what anchors this walk")
        second = ret.value.elts[1]
        if _shape_ok(second, chain):
            on_chain.append(ret)
        else:
            assert isinstance(second, ast.Constant) and second.value == 0, (
                "the exact count is transformed before the envelope [walk 1: "
                f"_query_work_expansion] at shared/discovery_service.py:{ret.lineno} "
                f"-- returned second position is {ast.unparse(second)!r}")
    assert len(on_chain) == 1, (
        f"expected exactly ONE return carrying the counted total, found {len(on_chain)}")
    _assert_untransformed(query_fn, chain, source_lines, "walk 1: _query_work_expansion")

    # ---- walk (2): get_work_expansion_enveloped --------------------------
    env_fn = _function_node(tree, "get_work_expansion_enveloped")
    unpack = None
    for node in ast.walk(env_fn):
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Call):
            continue
        if "_query_work_expansion" not in ast.unparse(node.value.func):
            continue
        assert unpack is None, "more than one _query_work_expansion call site"
        unpack = node
    assert unpack is not None, "get_work_expansion_enveloped never calls _query_work_expansion"
    target = unpack.targets[0]
    assert isinstance(target, ast.Tuple) and len(target.elts) == 2, (
        "the (rows, total) contract is not unpacked as a literal 2-tuple at "
        f"shared/discovery_service.py:{unpack.lineno}")
    env_chain = {target.elts[1].id}
    for node in ast.walk(env_fn):
        if isinstance(node, ast.Assign) and _shape_ok(node.value, env_chain):
            env_chain.update(t.id for t in node.targets if isinstance(t, ast.Name))

    total_kwargs = [kw for node in ast.walk(env_fn) if isinstance(node, ast.Call)
                    for kw in node.keywords if kw.arg == "total"]
    assert len(total_kwargs) == 1, (
        "the envelope's `total=` is not constructed exactly once -- the walk's "
        "anchor is gone")
    handed = total_kwargs[0].value
    assert _shape_ok(handed, env_chain), (
        "the exact count is transformed before the envelope [walk 2: "
        f"get_work_expansion_enveloped] -- `total=` receives "
        f"{ast.unparse(handed)!r}, not a bare name on the count chain")
    _assert_untransformed(env_fn, env_chain, source_lines,
                          "walk 2: get_work_expansion_enveloped")


# ===========================================================================
# Task 3: lock the contract against the failure that produced this plan.
#
# Three key sets are pinned SEPARATELY, because they are genuinely different
# and conflating them is how a field leaks: what the query builds, what a
# surface receives, and what the envelope wraps them in.
# ===========================================================================

#: The INTERNAL expansion row, before projection. Twenty keys.
_INTERNAL_EXPANSION_KEYS = frozenset({
    # the pre-plan nine
    "work_id", "unit_id", "representative_sys_id", "representative_page_id",
    "representative_claim_id", "claim_type", "evidence_source", "confidence_band",
    "member_sys_ids",
    # the anchor side (136-21)
    "anchor_claim_type", "anchor_evidence_source", "anchor_confidence_band",
    "relations_differ",
    # the resolved band presentation
    "displayed_evidence_source", "displayed_confidence_band", "band_rank",
    "band_label",
    # what NAMES the carrier
    "library_code", "shelfmark_display", "display_missing",
})

#: Keys that exist INTERNALLY and are deliberately stripped by the projection.
#: The two raw band pairs are here on purpose: DATA-01 says the surface shows
#: the WEAKER band, so giving a renderer both raw pairs invites it to re-derive
#: the comparison -- and a second comparator is how the displayed band drifts
#: from the filtered one.
_INTERNAL_ONLY_KEYS = frozenset({
    "evidence_source", "confidence_band",
    "anchor_evidence_source", "anchor_confidence_band",
})

#: Keys that must NEVER reach a surface, named individually because a generic
#: "no forbidden fields" assertion does not say WHICH.
_NEVER_ON_A_SURFACE = (
    "review_overlay", "measurement_status", "ci_low",
    "unit_key", "rn", "_total_rows",
)

#: The fields a renderer cannot draw the section without. Named EXPLICITLY
#: rather than implied by the allowlist -- a control that deleted one from the
#: allowlist would otherwise still satisfy "the key set equals the allowlist".
_PUBLIC_MUST_CONTAIN = (
    "relations_differ", "anchor_claim_type", "claim_type",
    "library_code", "shelfmark_display", "display_missing",
    "displayed_evidence_source", "displayed_confidence_band",
    "band_label", "band_rank",
)


def _one_of_each(tmp_path, name):
    db = _build_expansion_db(tmp_path / name, [
        _carrier("990000000000000001", "p001", claim_type="quotes_this_work",
                 confidence_band="screening_rb"),
    ])
    return _service_for(db)


def _rejects_missing_and_extra(actual, expected):
    """A set comparison that fails BOTH ways -- proved, not asserted.

    Returns True when `actual == expected` AND the same comparison rejects a
    copy with one key removed AND a copy with one key added."""
    if set(actual) != set(expected):
        return False
    missing = set(actual) - {sorted(actual)[0]}
    extra = set(actual) | {"an_unexpected_key"}
    return missing != set(expected) and extra != set(expected)


def test_internal_row_key_set_is_pinned(tmp_path):
    service = _one_of_each(tmp_path, "contract-internal.db")
    rows, total = service._query_work_expansion("wEXP001", **_ANCHOR_STRONG)
    assert total == 1
    assert len(rows) == 1
    assert set(rows[0]) == _INTERNAL_EXPANSION_KEYS, (
        "the INTERNAL expansion row shape changed; missing "
        f"{sorted(_INTERNAL_EXPANSION_KEYS - set(rows[0]))}, unexpected "
        f"{sorted(set(rows[0]) - _INTERNAL_EXPANSION_KEYS)}")
    assert _rejects_missing_and_extra(rows[0], _INTERNAL_EXPANSION_KEYS), (
        "the internal key-set assertion does not fail on BOTH a missing and an "
        "unexpected key")
    # No internal query discriminator survives into the row at all.
    for key in ("unit_key", "rn", "_total_rows", "adjudication_status",
                "displayed_band_rank"):
        assert key not in rows[0], f"internal query column {key!r} reached the row"


def test_public_row_key_set_equals_the_expansion_allowlist(tmp_path):
    service = _one_of_each(tmp_path, "contract-public.db")
    envelope = service.get_work_expansion_enveloped("wEXP001", **_ANCHOR_STRONG)
    item = envelope["items"][0]
    assert set(item) == set(SURFACE_EXPANSION_FIELDS), (
        "the PUBLIC expansion row is not exactly SURFACE_EXPANSION_FIELDS; missing "
        f"{sorted(set(SURFACE_EXPANSION_FIELDS) - set(item))}, unexpected "
        f"{sorted(set(item) - set(SURFACE_EXPANSION_FIELDS))}")
    assert _rejects_missing_and_extra(item, SURFACE_EXPANSION_FIELDS), (
        "the public key-set assertion does not fail on BOTH a missing and an "
        "unexpected key")
    for key in _PUBLIC_MUST_CONTAIN:
        assert key in item, (
            f"the public expansion row no longer carries {key!r} -- the panel "
            "cannot render the section without it")
    for key in _NEVER_ON_A_SURFACE:
        assert key not in item, f"{key!r} reached a surface"
    for key in _INTERNAL_ONLY_KEYS:
        assert key not in item, (
            f"{key!r} is INTERNAL-ONLY by design and reached a surface -- the "
            "surface displays the RESOLVED weaker band, never a raw pair it "
            "could re-compare")


def test_envelope_key_set_is_pinned_on_ok_and_on_an_outage(tmp_path):
    db = _build_expansion_db(tmp_path / "contract-envelope.db", [
        _carrier("990000000000000001", "p001"),
    ])
    ok = _service_for(db).get_work_expansion_enveloped("wEXP001")
    outage = _service_failing_at(db, _COUNT_MARKER).get_work_expansion_enveloped("wEXP001")
    assert ok["status"] == "ok" and outage["status"] == "unavailable"
    expected = {"status", "items", "total", "meta"}
    for label, envelope in (("ok", ok), ("outage", outage)):
        assert set(envelope) == expected, (
            f"the {label} envelope is not the four-key shape; missing "
            f"{sorted(expected - set(envelope))}, unexpected "
            f"{sorted(set(envelope) - expected)}")
        assert _rejects_missing_and_extra(envelope, expected), (
            f"the {label} envelope key-set assertion does not fail on BOTH a "
            "missing and an unexpected key")


def test_the_surface_safe_projection_strips_the_internal_only_keys():
    """Named individually, so a future edit that lets one through fails HERE
    rather than surfacing as a panel quietly showing the wrong band."""
    from shared.discovery_surface_projection import surface_safe_expansion

    poisoned = {key: "leaked" for key in _INTERNAL_EXPANSION_KEYS}
    poisoned.update({key: "leaked" for key in _NEVER_ON_A_SURFACE})
    poisoned["precision"] = 0.93
    poisoned["ci_high"] = 0.99
    projected = surface_safe_expansion(poisoned)
    assert set(projected) == set(SURFACE_EXPANSION_FIELDS)
    for key in (*_NEVER_ON_A_SURFACE, *_INTERNAL_ONLY_KEYS, "precision", "ci_high"):
        assert key not in projected, f"{key!r} survived the projection"


@pytest.mark.parametrize("lang", ["en", "he"])
def test_no_expansion_row_carries_a_review_badge_precision_or_interval(tmp_path, lang):
    # A `human_confirmed` carrier is the case that matters: it is exactly the
    # row for which `serialize_banded_claim` emits the "Expert-reviewed" badge
    # D-13f has decided never to show.
    confirmed = dict(_carrier("990000000000000001", "p001"),
                     adjudication_status="human_confirmed")
    db = _build_expansion_db(tmp_path / f"honesty-{lang}.db", [
        confirmed,
        _carrier("990000000000000002", "p002", confidence_band="weak",
                 evidence_source=_PROPAGATED),
    ])
    envelope = _service_for(db).get_work_expansion_enveloped(
        "wEXP001", lang=lang, **_ANCHOR_STRONG)
    assert envelope["status"] == "ok" and envelope["items"]
    badges = ("Expert-reviewed", "נבדק בידי מומחה")
    for item in envelope["items"]:
        for key, value in item.items():
            assert not is_forbidden_surface_field(key), (
                f"forbidden field {key!r} on an expansion row ({lang})")
            if isinstance(value, str):
                for badge in badges:
                    assert badge not in value, (
                        f"the human-review badge reached a surface as a VALUE "
                        f"under {key!r} ({lang})")
    for key in envelope["meta"]:
        assert not is_forbidden_surface_field(key), f"forbidden meta key {key!r}"
