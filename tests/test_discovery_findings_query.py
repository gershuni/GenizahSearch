# -*- coding: utf-8 -*-
"""The corpus-wide findings query and its facet cascade (Phase 136, plan
136-14, Task 3).

The surface is "Computed Identifications" -- three user-selectable row units
over the MATERIALIZED identification grain, with a domain / author / work
cascade sourced from the IDENTIFIED WORK.

Two things this file is built to catch:

1. **The wrong axis.** Filtering on the MANUSCRIPT's catalogue domain is
   tempting -- the FJMS `domains` route joins on `sys_id == AlmaId` at 83%
   coverage with zero new work -- and it is actively harmful: it hides exactly
   the findings that DISAGREE with the catalogue, which are the most valuable
   ones. (Moss. V,374 is catalogued Court Records while carrying a verifiably
   correct Rashi finding; 338 tier-A findings sit on documentary-catalogued
   manuscripts.) The guard is a source assertion naming the forbidden
   accessors.

2. **A claim-row scan.** That shape already missed its budget: 3.41-3.55 s
   against a 1.5 s cap, and 16 s for the deduped count alone. The fixture below
   therefore leaves `discovery_claim` EMPTY on purpose -- a query that read
   claims as its base table would return nothing and every assertion here would
   fail loudly, rather than passing slowly.

Masking discipline: every value below is fabricated (synthetic ids, synthetic
titles, synthetic domain tokens) -- never real research data, never a corpus
name.
"""
from __future__ import annotations

import asyncio
import hashlib
import pathlib
import sqlite3

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from shared.discovery_service import (
    BUCKET_ALL,
    BUCKET_MAIN,
    BUCKET_MORE,
    DOMAIN_UNASSIGNED,
    FACET_LEVELS,
    FINDINGS_SORT_BAND_RANK,
    FINDINGS_SORT_MATCHED_TEXT,
    FINDINGS_SORT_PAGE_COUNT,
    FINDINGS_SORTS,
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNIT_MANUSCRIPT,
    FINDINGS_UNIT_WORK,
    FINDINGS_UNITS,
    DiscoveryService,
    _build_findings_query,
)
from shared.discovery_surface_projection import STATUS_OK, SURFACE_FINDING_FIELDS

_VERSION = "test-findings"

_LEAF_A1 = "Synthetic Parent A / Synthetic Leaf A1"
_LEAF_A2 = "Synthetic Parent A / Synthetic Leaf A2"
_LEAF_B1 = "Synthetic Parent B / Synthetic Leaf B1"
_PARENT_A = "Synthetic Parent A"
_PARENT_B = "Synthetic Parent B"

_AUTHOR_ONE = "Synthetic Author One"
_AUTHOR_TWO = "Synthetic Author Two"
_AUTHOR_THREE = "Synthetic Author Three"

# (work_id, canonical_work_id, neutral_title, author, genre)
_WORKS = [
    ("wA", "wA", "Synthetic Alpha Commentary", _AUTHOR_ONE, _LEAF_A1),
    ("wB", "wB", "Synthetic Beta Commentary", _AUTHOR_ONE, _LEAF_A2),
    ("wC", "wC", "Synthetic Gamma Treatise", _AUTHOR_TWO, _LEAF_A1),
    ("wD", "wD", "Synthetic Delta Liturgy", _AUTHOR_THREE, _LEAF_B1),
    ("wE", "wE", "Synthetic Epsilon Fragment", None, None),  # Unassigned on BOTH axes
]

# (sys_id, canonical/display work, main_pool, reason, band_rank, pages,
#  coverage_ppm, relation_kind, novelty_status)
_IDENTIFICATIONS = [
    ("s1", "wA", 1, "main_multifolio", 1, 3, 900000, "direct_witness", "fills_gap"),
    ("s1", "wB", 1, "main_full_coverage", 2, 1, 850000, "direct_witness", "confirms"),
    ("s2", "wA", 1, "main_multifolio", 2, 2, 800000, "direct_witness", "confirms"),
    ("s3", "wC", 1, "main_full_coverage", 1, 1, 950000, "quotes_this_work", "fills_gap"),
    ("s4", "wD", 0, "low_coverage", 5, 1, 100000, "direct_witness", "not_checked"),
    ("s5", "wE", 1, "main_full_coverage", 3, 1, 700000, "shared_text", "not_checked"),
    ("s6", "wA", 0, "insufficient_length", 2, 1, 200000, "direct_witness", "not_checked"),
]


# Ruling F's catalogue-divergent rows, plus a row whose novelty was never
# recorded at all. Kept OUT of `_IDENTIFICATIONS` on purpose: every assertion
# above counts against that list, and folding these in would have quietly
# rewritten what a dozen unrelated tests are measuring. They are loaded by
# their own fixture instead, so the divergence behaviour is asserted on a
# population built to show it.
#
# `s7`/`s8` are MAIN-pool divergent rows -- the case that matters, because a
# divergent row in the second bucket is already one opt-in away from the
# reader, while a main-pool one renders in the default view unless something
# stops it. `s9` carries the fail-closed `not_checked` shade: the absence of a
# verdict is NOT a disagreement and must survive the filter.
_DIVERGENT_IDENTIFICATIONS = [
    ("s7", "wA", 1, "main_full_coverage", 1, 1, 880000, "direct_witness", "diverges_work"),
    ("s8", "wD", 1, "main_full_coverage", 2, 1, 870000, "direct_witness", "diverges_part"),
    ("s9", "wC", 1, "main_full_coverage", 3, 1, 860000, "direct_witness", "not_checked"),
]

#: The two rows above that ruling F hides. `s9` is deliberately not one.
_HIDDEN_SYS_IDS = frozenset({"s7", "s8"})


def _identification_id(sys_id, canonical_work_id):
    key = f"discovery_identification_v1|{sys_id}|{canonical_work_id}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _build_findings_db(tmp_path, identifications=None, name="findings.db"):
    identifications = _IDENTIFICATIONS if identifications is None else identifications
    db_path = tmp_path / name
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        cur = conn.cursor()
        cur.executemany(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, "
            "genre, source_corpus, identity_visibility) VALUES (?, ?, ?, ?, ?, 'sefaria', 'public')",
            _WORKS,
        )
        cur.executemany(
            "INSERT INTO manuscript_display (sys_id, library_code, library_sort_key, "
            "shelfmark_display, shelfmark_sort_key) VALUES (?, ?, ?, ?, ?)",
            [(f"s{i}", "SYNLIB", "synlib", f"Synthetic Shelfmark {i}", f"synthetic {i:04d}")
             for i in range(1, 10)],
        )
        cur.executemany(
            """
            INSERT INTO discovery_identification (
                identification_id, sys_id, canonical_work_id, display_work_id,
                main_pool, main_pool_reason, best_band_rank, page_count,
                max_coverage_ppm, relation_kind, novelty_status,
                assertion_visibility, identity_visibility
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'public', 'public')
            """,
            [
                (_identification_id(sys_id, work), sys_id, work, work, main, reason,
                 rank, pages, cov, relation, novelty)
                for (sys_id, work, main, reason, rank, pages, cov, relation, novelty)
                in identifications
            ],
        )
        cur.executemany(
            "INSERT INTO meta (key, value) VALUES (?, ?)",
            [("schema_version", "discovery-v1"), ("sidecar_version", _VERSION),
             ("source_db_sha256", "test"), ("build_date", "2026-01-01T00:00:00Z"),
             ("data_as_of", "2026-01-01"), ("htr_snapshot_hash", "test"),
             ("expected_rows_claims", "0"), ("expected_rows_evidence", "0"),
             ("expected_rows_works", str(len(_WORKS))), ("expected_rows_units", "0"),
             ("frame_content_hash", "test"), ("audience", "private")],
        )
        conn.commit()

        # The positive control: `discovery_claim` is EMPTY, so a findings query
        # that scanned claim rows would return nothing at all.
        (claims,) = conn.execute("SELECT COUNT(*) FROM discovery_claim").fetchone()
        assert claims == 0
        (grain,) = conn.execute("SELECT COUNT(*) FROM discovery_identification").fetchone()
        assert grain == len(identifications)
    finally:
        conn.close()
    return str(db_path)


def _service_for(db_path):
    return DiscoveryService(
        path_provider=lambda: db_path,
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: _VERSION,
    )


@pytest.fixture()
def service(tmp_path):
    return _service_for(_build_findings_db(tmp_path))


@pytest.fixture()
def divergence_service(tmp_path):
    """The same grain PLUS ruling F's divergent rows and one NULL-shade row."""
    return _service_for(_build_findings_db(
        tmp_path,
        identifications=_IDENTIFICATIONS + _DIVERGENT_IDENTIFICATIONS,
        name="findings-divergence.db",
    ))


def _values(env, key):
    return [row[key] for row in env["items"]]


# ---------------------------------------------------------------------------
# Behaviour 1-4: the row units
# ---------------------------------------------------------------------------

def test_per_identification_unit_is_one_row_per_manuscript_and_work_and_is_the_default(service):
    default = service.get_findings_enveloped(bucket=BUCKET_ALL)
    explicit = service.get_findings_enveloped(
        unit=FINDINGS_UNIT_IDENTIFICATION, bucket=BUCKET_ALL)
    assert default["meta"]["unit"] == FINDINGS_UNIT_IDENTIFICATION
    assert default["total"] == explicit["total"] == len(_IDENTIFICATIONS)

    pairs = {(row["sys_id"], row["display_work_id"]) for row in default["items"]}
    assert len(pairs) == len(_IDENTIFICATIONS), "one row per (manuscript, work) pair"
    assert all(row["unit"] == FINDINGS_UNIT_IDENTIFICATION for row in default["items"])
    assert all(row["novelty_offered"] is True for row in default["items"])


def test_per_manuscript_unit_annotates_a_manuscript_carrying_more_than_one_work(service):
    env = service.get_findings_enveloped(unit=FINDINGS_UNIT_MANUSCRIPT, bucket=BUCKET_ALL)
    assert env["total"] == 6, "one row per manuscript (s1..s6)"
    by_sys = {row["sys_id"]: row for row in env["items"]}

    multi = by_sys["s1"]
    assert multi["work_count"] == 2
    assert multi["multi_work_annotation"] is True
    assert multi["novelty_status"] is None, (
        "s1 carries fills_gap AND confirms -- a single novelty verdict on that "
        "row would be ambiguous, so the row carries an annotation instead"
    )

    single = by_sys["s2"]
    assert single["work_count"] == 1
    assert single["multi_work_annotation"] is False
    assert single["novelty_status"] == "confirms", (
        "a one-work manuscript has exactly one verdict and may state it"
    )


def test_per_work_unit_is_one_row_per_work_and_never_offers_novelty(service):
    env = service.get_findings_enveloped(unit=FINDINGS_UNIT_WORK, bucket=BUCKET_ALL)
    assert env["total"] == len(_WORKS)
    by_work = {row["display_work_id"]: row for row in env["items"]}
    assert by_work["wA"]["manuscript_count"] == 3
    assert by_work["wA"]["neutral_title"] == "Synthetic Alpha Commentary"

    for row in env["items"]:
        assert row["novelty_offered"] is False
        assert row["novelty_status"] is None, (
            "a work spanning many manuscripts has no single novelty verdict"
        )
    assert env["meta"]["novelty_offered"] is False

    with pytest.raises(ValueError):
        service.get_findings_enveloped(unit=FINDINGS_UNIT_WORK, novelty=["fills_gap"])


def test_the_per_claim_unit_is_not_reachable_through_the_api(service):
    assert "claim" not in FINDINGS_UNITS, (
        "the per-claim unit is not offered: the same identification repeats "
        "once per folio, inflating same-work matches ~2.3x"
    )
    assert FINDINGS_UNITS == {
        FINDINGS_UNIT_IDENTIFICATION, FINDINGS_UNIT_MANUSCRIPT, FINDINGS_UNIT_WORK}
    for rejected in ("claim", "page", "evidence", ""):
        with pytest.raises(ValueError):
            service.get_findings_enveloped(unit=rejected)


# ---------------------------------------------------------------------------
# Behaviour 5: filters compose as AND; the default bucket is the main pool
# ---------------------------------------------------------------------------

def test_default_result_set_is_the_main_pool_and_the_metadata_says_so(service):
    env = service.get_findings_enveloped()
    assert env["meta"]["bucket"] == BUCKET_MAIN
    assert env["total"] == 5, "the five main-pool identifications"
    assert all(row["main_pool"] is True for row in env["items"])

    more = service.get_findings_enveloped(bucket=BUCKET_MORE)
    assert more["meta"]["bucket"] == BUCKET_MORE
    assert more["total"] == 2
    assert all(row["main_pool"] is False for row in more["items"])

    every = service.get_findings_enveloped(bucket=BUCKET_ALL)
    assert every["total"] == len(_IDENTIFICATIONS)
    assert every["total"] == env["total"] + more["total"], (
        "the two buckets partition the grain -- the default narrows the corpus "
        "view, and the surface must be able to say so rather than narrow it "
        "silently"
    )


def test_filters_compose_as_and_and_an_empty_filter_set_returns_the_whole_bucket(service):
    everything = service.get_findings_enveloped(bucket=BUCKET_ALL)
    assert everything["total"] == len(_IDENTIFICATIONS)

    by_domain = service.get_findings_enveloped(bucket=BUCKET_ALL, domain=_PARENT_A)
    by_author = service.get_findings_enveloped(bucket=BUCKET_ALL, author=_AUTHOR_ONE)
    both = service.get_findings_enveloped(
        bucket=BUCKET_ALL, domain=_PARENT_A, author=_AUTHOR_ONE)
    assert both["total"] < by_domain["total"], "AND, not OR"
    assert both["total"] <= by_author["total"]
    assert both["total"] == 4  # wA x3 + wB x1

    triple = service.get_findings_enveloped(
        bucket=BUCKET_ALL, domain=_PARENT_A, author=_AUTHOR_ONE, novelty=["fills_gap"])
    assert triple["total"] == 1
    assert triple["items"][0]["sys_id"] == "s1"

    # A bucket filter composes with the rest rather than replacing it.
    scoped = service.get_findings_enveloped(
        bucket=BUCKET_MORE, domain=_PARENT_A, author=_AUTHOR_ONE)
    assert scoped["total"] == 1 and scoped["items"][0]["sys_id"] == "s6"


# ---------------------------------------------------------------------------
# Behaviour 6: the sorts -- and novelty is NOT one of them
# ---------------------------------------------------------------------------

def test_the_three_sorts_order_differently_and_novelty_is_not_offered(service):
    assert FINDINGS_SORTS == {
        FINDINGS_SORT_BAND_RANK, FINDINGS_SORT_PAGE_COUNT, FINDINGS_SORT_MATCHED_TEXT}
    assert "novelty" not in FINDINGS_SORTS, (
        "absence from a finding aid is not evidence a match is correct, and "
        "ordering by it would imply otherwise (D-15a / D-24)"
    )

    # Every sort keeps the main pool FIRST -- the bucket is positional on this
    # page, not a filter chip, so it leads every ordering. The named sort is
    # what orders WITHIN a bucket.
    def _bucket_then(env, key, reverse=True):
        keys = [(not row["main_pool"], row[key]) for row in env["items"]]
        assert [k[0] for k in keys] == sorted(k[0] for k in keys), "main pool must lead"
        for bucket_flag in (False, True):
            within = [k[1] for k in keys if k[0] is bucket_flag]
            assert within == sorted(within, reverse=reverse), (key, bucket_flag, within)

    by_band = service.get_findings_enveloped(bucket=BUCKET_ALL, sort=FINDINGS_SORT_BAND_RANK)
    _bucket_then(by_band, "best_band_rank", reverse=False)  # lower rank is stronger

    by_pages = service.get_findings_enveloped(bucket=BUCKET_ALL, sort=FINDINGS_SORT_PAGE_COUNT)
    _bucket_then(by_pages, "page_count")
    assert by_pages["meta"]["sort"] == FINDINGS_SORT_PAGE_COUNT

    by_text = service.get_findings_enveloped(bucket=BUCKET_ALL, sort=FINDINGS_SORT_MATCHED_TEXT)
    _bucket_then(by_text, "max_coverage_ppm")
    assert by_text["meta"]["sort_basis"] == "max_coverage_ppm", (
        "the identification grain materializes COVERAGE, not matched letters -- "
        "the envelope names the basis so the surface never implies a letter "
        "count it does not have"
    )

    assert _values(by_band, "identification_id") != _values(by_pages, "identification_id")

    for rejected in ("novelty", "random", ""):
        with pytest.raises(ValueError):
            service.get_findings_enveloped(sort=rejected)


# ---------------------------------------------------------------------------
# Behaviour 7: counts come from the materialized grain; approximation is flagged
# ---------------------------------------------------------------------------

def test_counts_are_served_from_the_materialized_grain_never_a_claim_row_scan(service):
    for unit in sorted(FINDINGS_UNITS):
        for sort in sorted(FINDINGS_SORTS):
            sql, _params = _build_findings_query(
                unit=unit, sort=sort, bucket=BUCKET_ALL,
                domain=_PARENT_A, author=_AUTHOR_ONE, novelty=None,
                work_id="wA", page=1, page_size=50, count_only=False,
            )
            assert "FROM discovery_identification" in sql
            assert "discovery_claim" not in sql, (
                f"the {unit}/{sort} findings query reads discovery_claim -- that "
                "is the shape that measured 3.41-3.55 s against a 1.5 s cap"
            )
            assert "discovery_evidence" not in sql

    # And empirically: the fixture's claim table is empty, yet every unit
    # returns rows.
    for unit in sorted(FINDINGS_UNITS):
        env = service.get_findings_enveloped(unit=unit, bucket=BUCKET_ALL)
        assert env["total"] > 0


def test_all_three_units_are_served_by_one_query_builder_not_three():
    """One builder, parameterised by unit. Three builders would let filter,
    sort and count semantics drift between the unit a reader selected and the
    unit its counts were computed over -- silently, and only for readers who
    change the unit."""
    source = _service_source()
    assert source.count("def _build_findings_query(") == 1
    assert source.count("def _build_findings_filter(") == 1, (
        "the predicate has exactly one builder too -- building it twice is how "
        "a facet count and the result set beside it drift apart"
    )
    # No per-unit query function has grown alongside it.
    for unit in sorted(FINDINGS_UNITS):
        assert f"def _build_{unit}_findings" not in source
        assert f"def _query_findings_{unit}" not in source
    # The per-unit difference is DATA (a select list + a group-by), not code.
    assert source.count("_FINDINGS_UNIT_SELECT[unit]") == 1
    assert source.count("_FINDINGS_UNIT_GROUP_BY[unit]") == 1


def test_an_exact_count_is_flagged_exact_and_a_capped_one_is_flagged_approximate(
        service, monkeypatch):
    exact = service.get_findings_enveloped(bucket=BUCKET_ALL)
    assert exact["meta"]["approximate_total"] is False
    assert exact["total"] == len(_IDENTIFICATIONS)

    monkeypatch.setenv("DISCOVERY_FINDINGS_COUNT_MAX", "2")
    capped = service.get_findings_enveloped(bucket=BUCKET_ALL)
    assert capped["meta"]["approximate_total"] is True, (
        "a silently approximate number presented as exact is not acceptable -- "
        "the surface must be able to render the hedge in words"
    )
    assert capped["total"] == 2
    assert len(capped["items"]) == len(_IDENTIFICATIONS), (
        "capping the COUNT must not cap the rows"
    )


def test_pagination_returns_a_bounded_page_and_the_real_total(service):
    page1 = service.get_findings_enveloped(bucket=BUCKET_ALL, page=1, page_size=3)
    page2 = service.get_findings_enveloped(bucket=BUCKET_ALL, page=2, page_size=3)
    page3 = service.get_findings_enveloped(bucket=BUCKET_ALL, page=3, page_size=3)
    assert [len(p["items"]) for p in (page1, page2, page3)] == [3, 3, 1]
    assert all(p["total"] == len(_IDENTIFICATIONS) for p in (page1, page2, page3))
    seen = [r["identification_id"] for p in (page1, page2, page3) for r in p["items"]]
    assert len(set(seen)) == len(_IDENTIFICATIONS)


def test_findings_rows_carry_exactly_the_surface_allowlist(service):
    env = service.get_findings_enveloped(bucket=BUCKET_ALL)
    for row in env["items"]:
        assert set(row) == set(SURFACE_FINDING_FIELDS)
        for forbidden in ("review_overlay", "precision", "ci_low", "ci_high"):
            assert forbidden not in row


# ---------------------------------------------------------------------------
# Behaviour 8: the facet cascade, on the IDENTIFIED WORK
# ---------------------------------------------------------------------------

def test_domain_narrows_and_a_leaf_narrows_strictly_further_than_its_parent(service):
    unfiltered = service.get_findings_enveloped(bucket=BUCKET_ALL)
    parent = service.get_findings_enveloped(bucket=BUCKET_ALL, domain=_PARENT_A)
    leaf = service.get_findings_enveloped(bucket=BUCKET_ALL, domain=_LEAF_A1)

    assert parent["total"] < unfiltered["total"], "selecting a domain must narrow"
    parent_ids = {r["identification_id"] for r in parent["items"]}
    leaf_ids = {r["identification_id"] for r in leaf["items"]}
    assert leaf_ids < parent_ids, (
        "a leaf must be a STRICT subset of its parent, not merely no larger"
    )
    assert leaf["total"] == 4 and parent["total"] == 5


def test_the_author_list_is_cross_filtered_by_domain(service):
    everything = service.get_findings_facets_enveloped("author", bucket=BUCKET_ALL)
    all_authors = {row["value"] for row in everything["items"]}
    assert all_authors == {_AUTHOR_ONE, _AUTHOR_TWO, _AUTHOR_THREE, DOMAIN_UNASSIGNED}

    in_parent_a = service.get_findings_facets_enveloped(
        "author", bucket=BUCKET_ALL, domain=_PARENT_A)
    narrowed = {row["value"] for row in in_parent_a["items"]}
    assert narrowed == {_AUTHOR_ONE, _AUTHOR_TWO}
    assert narrowed < all_authors

    counts = {row["value"]: row["count"] for row in in_parent_a["items"]}
    assert counts[_AUTHOR_ONE] == 4 and counts[_AUTHOR_TWO] == 1


def test_the_work_list_is_cross_filtered_by_domain_and_author(service):
    in_parent_a = service.get_findings_facets_enveloped(
        "work", bucket=BUCKET_ALL, domain=_PARENT_A)
    assert {row["value"] for row in in_parent_a["items"]} == {"wA", "wB", "wC"}

    narrower = service.get_findings_facets_enveloped(
        "work", bucket=BUCKET_ALL, domain=_PARENT_A, author=_AUTHOR_ONE)
    assert {row["value"] for row in narrower["items"]} == {"wA", "wB"}
    counts = {row["value"]: row["count"] for row in narrower["items"]}
    assert counts["wA"] == 3 and counts["wB"] == 1
    labels = {row["value"]: row["label"] for row in narrower["items"]}
    assert labels["wA"] == "Synthetic Alpha Commentary"


def test_unassigned_is_a_selectable_domain_bucket_with_a_real_count(service):
    facets = service.get_findings_facets_enveloped("domain", bucket=BUCKET_ALL)
    by_value = {row["value"]: row for row in facets["items"]}
    assert DOMAIN_UNASSIGNED in by_value, (
        "a work the vocabulary cannot place must not silently disappear from "
        "the corpus view"
    )
    assert by_value[DOMAIN_UNASSIGNED]["count"] == 1

    selected = service.get_findings_enveloped(bucket=BUCKET_ALL, domain=DOMAIN_UNASSIGNED)
    assert selected["total"] == 1
    assert selected["items"][0]["display_work_id"] == "wE"


def test_every_offered_domain_is_inside_the_closed_vocabulary(service):
    """The closed vocabulary is enforced at BUILD time (a leaf outside the FJMS
    tree is a build error). What the read path must guarantee is that it never
    INVENTS a value: every offered domain is either the explicit Unassigned
    bucket or a parent/leaf of a genre actually stored on `works`."""
    conn = service._get_conn()
    stored = {row[0] for row in conn.execute(
        "SELECT DISTINCT genre FROM works WHERE genre IS NOT NULL AND genre != ''")}
    allowed = {DOMAIN_UNASSIGNED}
    for genre in stored:
        allowed.add(genre)
        if " / " in genre:
            allowed.add(genre.split(" / ", 1)[0])

    facets = service.get_findings_facets_enveloped("domain", bucket=BUCKET_ALL)
    offered = {row["value"] for row in facets["items"]}
    assert offered, "the domain facet returned nothing at all"
    assert offered <= allowed, sorted(offered - allowed)

    # The tree shape is real: parents and leaves, with the parent counting its
    # leaves.
    by_value = {row["value"]: row for row in facets["items"]}
    assert by_value[_PARENT_A]["is_leaf"] is False
    assert by_value[_LEAF_A1]["is_leaf"] is True
    assert by_value[_LEAF_A1]["parent"] == _PARENT_A
    assert by_value[_PARENT_A]["count"] == (
        by_value[_LEAF_A1]["count"] + by_value[_LEAF_A2]["count"])
    assert by_value[_PARENT_B]["count"] == by_value[_LEAF_B1]["count"]


def test_facet_level_vocabulary_is_closed(service):
    assert FACET_LEVELS == {"domain", "author", "work"}
    for rejected in ("manuscript_domain", "library", ""):
        with pytest.raises(ValueError):
            service.get_findings_facets_enveloped(rejected)


# ---------------------------------------------------------------------------
# Ruling F: the divergence axis (136-GATE1-DECISIONS.md section F)
#
# `diverges_work` / `diverges_part` rows are ABSENT from the default render and
# surface only behind an explicit, separately-labelled warned opt-in. Every
# test below asserts a PROPERTY OF THE QUERY, not the value of a constant: the
# policy constant and its predicate already had tests, and both of them stayed
# green for the whole period during which nothing in `web/` or `shared/` called
# either one.
# ---------------------------------------------------------------------------

def test_divergent_rows_are_absent_from_the_default_result_set_and_from_its_total(
        divergence_service):
    """The load-bearing one. Delete the predicate in `_build_findings_filter`
    and this fails on the very first assertion."""
    env = divergence_service.get_findings_enveloped(bucket=BUCKET_ALL)

    returned = {row["sys_id"] for row in env["items"]}
    assert not (returned & _HIDDEN_SYS_IDS), (
        "a catalogue-divergent identification rendered in the DEFAULT view -- "
        "ruling F requires it be absent, not merely unbadged"
    )
    # The TOTAL, not only the page: a filter applied after the fetch would
    # leave this number describing rows the reader is not being shown.
    assert env["total"] == len(_IDENTIFICATIONS) + 1, "everything but s7/s8"
    assert env["meta"]["include_divergent"] is False

    # And on the MAIN pool specifically, which is where the divergent fixture
    # rows live -- the bucket a reader lands on.
    main = divergence_service.get_findings_enveloped(bucket=BUCKET_MAIN)
    assert not ({row["sys_id"] for row in main["items"]} & _HIDDEN_SYS_IDS)


def test_the_divergence_opt_in_returns_them_and_the_envelope_says_which_it_did(
        divergence_service):
    opted_in = divergence_service.get_findings_enveloped(
        bucket=BUCKET_ALL, include_divergent=True)
    assert opted_in["meta"]["include_divergent"] is True
    assert opted_in["total"] == len(_IDENTIFICATIONS) + len(_DIVERGENT_IDENTIFICATIONS)
    assert _HIDDEN_SYS_IDS <= {row["sys_id"] for row in opted_in["items"]}


def test_an_unchecked_row_is_not_treated_as_a_divergence(divergence_service):
    """`not_checked` is the fail-closed default, not a disagreement. Hiding it
    would under-report the corpus by exactly the rows nobody checked -- and
    `s9` is the row that proves the filter selects on the two divergence shades
    rather than on "anything that is not a positive verdict"."""
    env = divergence_service.get_findings_enveloped(bucket=BUCKET_ALL)
    assert "s9" in {row["sys_id"] for row in env["items"]}


def test_the_divergence_filter_relies_on_a_pinned_not_null_column(divergence_service):
    """The bare `NOT IN` in `_build_findings_filter` is only sound because
    `novelty_status` cannot be NULL: `NULL NOT IN (...)` is NULL, so on a
    nullable column that shape would silently drop every unrecorded row. The
    invariant is asserted here rather than defended by an unreachable
    `IS NULL OR` branch, which would read as coverage nobody has."""
    conn = divergence_service._get_conn()
    columns = {
        row["name"]: row
        for row in conn.execute("PRAGMA table_info(discovery_identification)")
    }
    assert columns["novelty_status"]["notnull"] == 1, (
        "discovery_identification.novelty_status became nullable -- the "
        "divergence filter's bare NOT IN now drops every NULL-shade row from "
        "the default view; add the IS NULL branch before relaxing this"
    )


def test_the_facet_counts_follow_the_divergence_opt_in(divergence_service):
    """A number beside an option has to describe the set that option produces.
    The divergent fixture rows sit under `_PARENT_A` (wA) and `_PARENT_B` (wD),
    so both parents move when the axis is opened."""
    def _counts(**kwargs):
        env = divergence_service.get_findings_facets_enveloped(
            "domain", bucket=BUCKET_ALL, **kwargs)
        assert env["status"] == STATUS_OK
        return {row["value"]: row["count"] for row in env["items"]}

    default = _counts()
    opted_in = _counts(include_divergent=True)
    assert opted_in[_LEAF_A1] == default[_LEAF_A1] + 1, "wA gained diverges_work"
    assert opted_in[_LEAF_B1] == default[_LEAF_B1] + 1, "wD gained diverges_part"
    assert default != opted_in

    # And the count a reader sees really is the size of what selecting that
    # option returns -- the promise `_node_text` makes on the page.
    rows = divergence_service.get_findings_enveloped(
        bucket=BUCKET_ALL, domain=_LEAF_A1)
    assert rows["total"] == default[_LEAF_A1]


def test_the_divergence_filter_is_applied_in_sql_never_by_post_filtering():
    """In the WHERE clause of the row query AND of the bounded-count query --
    the two statements a pager and a total are read from."""
    from shared.discovery_service import DIVERGENCE_SHADE_ORDER

    shades = list(DIVERGENCE_SHADE_ORDER)

    rows_sql, rows_params = _build_findings_query(bucket=BUCKET_ALL)
    assert "novelty_status NOT IN" in rows_sql
    # TWICE on the row query, and the ORDER is the contract: the SELECT list's
    # per-row divergence flag binds first, the WHERE predicate second. A
    # positional parameter list is the one place this can go wrong silently --
    # swap them and the query filters on a domain and flags on a shade name.
    assert rows_params[:len(shades)] == shades
    assert [p for p in rows_params if p in DIVERGENCE_SHADE_ORDER] == shades * 2

    count_sql, count_params = _build_findings_query(
        bucket=BUCKET_ALL, count_only=True, count_cap=100)
    assert "novelty_status NOT IN" in count_sql
    assert [p for p in count_params if p in DIVERGENCE_SHADE_ORDER] == shades, (
        "the bounded-count form selects `1`, so only the WHERE binding is there")

    opted_in_sql, opted_in_params = _build_findings_query(
        bucket=BUCKET_ALL, include_divergent=True)
    assert "novelty_status NOT IN" not in opted_in_sql
    assert [p for p in opted_in_params if p in DIVERGENCE_SHADE_ORDER] == shades, (
        "the opt-in drops the FILTER and keeps the per-row FLAG -- the rows are "
        "shown, and each one still says what it is")


def test_every_row_carries_its_own_divergence_flag_on_every_unit(divergence_service):
    """The marker's INPUT. A renderer deriving it from `novelty_status` would
    be right on the identification unit and wrong on the other two: that column
    is NULL on a mixed group and NULL on every per-work row, so a manuscript
    carrying one divergent identification and one confirming one -- and every
    work row without exception -- would render as an ordinary finding."""
    by_unit = {
        unit: divergence_service.get_findings_enveloped(
            unit=unit, bucket=BUCKET_ALL, include_divergent=True)
        for unit in sorted(FINDINGS_UNITS)
    }
    for unit, env in by_unit.items():
        assert all("divergent" in row for row in env["items"]), unit
        assert all(isinstance(row["divergent"], bool) for row in env["items"]), unit

    ident = {row["sys_id"]: row for row in by_unit[FINDINGS_UNIT_IDENTIFICATION]["items"]
             if row["sys_id"] in ("s7", "s8", "s9")}
    assert ident["s7"]["divergent"] is True
    assert ident["s8"]["divergent"] is True
    assert ident["s9"]["divergent"] is False

    # s1 carries fills_gap AND confirms -- `novelty_status` is NULL there, and
    # the flag still answers correctly rather than inheriting that NULL.
    manuscripts = {row["sys_id"]: row
                   for row in by_unit[FINDINGS_UNIT_MANUSCRIPT]["items"]}
    assert manuscripts["s1"]["novelty_status"] is None
    assert manuscripts["s1"]["divergent"] is False
    assert manuscripts["s7"]["divergent"] is True

    # wA is claimed by s1/s2/s6 (undivergent) AND s7 (diverges_work): the work
    # row mixes, `novelty_status` is NULL on that unit by construction, and the
    # flag is the only thing that can still say so.
    works = {row["display_work_id"]: row for row in by_unit[FINDINGS_UNIT_WORK]["items"]}
    assert works["wA"]["novelty_status"] is None
    assert works["wA"]["divergent"] is True
    assert works["wB"]["divergent"] is False


def test_the_divergence_flag_survives_the_other_filters(divergence_service):
    """The flag's placeholders sit in the SELECT list and the filters' in the
    WHERE clause, so this is what a mis-ordered positional parameter list would
    break: same rows, wrong flags."""
    env = divergence_service.get_findings_enveloped(
        bucket=BUCKET_ALL, domain=_PARENT_A, include_divergent=True)
    flags = {(row["sys_id"], row["display_work_id"]): row["divergent"]
             for row in env["items"]}
    assert set(flags) == {
        ("s1", "wA"), ("s1", "wB"), ("s2", "wA"), ("s3", "wC"),
        ("s6", "wA"), ("s7", "wA"), ("s9", "wC"),
    }, "wA/wB/wC are the works under Parent A"
    assert flags[("s7", "wA")] is True
    assert not any(value for key, value in flags.items() if key != ("s7", "wA")), (
        "exactly the diverges_work row is flagged -- the domain filter's own "
        "bound parameters have not displaced the flag's")


def test_the_divergence_predicate_is_derived_from_the_shared_policy(monkeypatch):
    """DERIVED, never restated. Two halves:

    1. neither shade appears as a literal anywhere in the service module, so
       there is no second list to drift;
    2. moving the POLICY moves the query. `DIVERGENCE_SHADE_ORDER` is computed
       at import from `is_hidden_by_default`, so this re-derives it the same
       way and asserts the query's own bound parameters follow.
    """
    from shared.discovery_novelty import NOVELTY_STATUS_ORDER, is_hidden_by_default
    from shared.discovery_service import DIVERGENCE_SHADE_ORDER

    source = _service_source()
    for shade in DIVERGENCE_SHADE_ORDER:
        assert f'"{shade}"' not in source and f"'{shade}'" not in source, (
            f"{shade!r} is written as a literal in shared/discovery_service.py -- "
            "the hidden-by-default set has exactly one definition and this is "
            "not it"
        )

    assert DIVERGENCE_SHADE_ORDER == tuple(
        s for s in NOVELTY_STATUS_ORDER if is_hidden_by_default(s))
    assert DIVERGENCE_SHADE_ORDER, (
        "ruling F is in force and the derived set is empty -- the default view "
        "would silently stop excluding anything"
    )
    _sql, params = _build_findings_query(bucket=BUCKET_ALL)
    assert set(DIVERGENCE_SHADE_ORDER) <= set(params)


def test_the_web_wrappers_thread_the_divergence_opt_in_through(monkeypatch):
    """The page never touches the service module; it calls `web.discovery`. A
    wrapper that accepted the argument and dropped it would leave the control
    live and the query unchanged."""
    import web.discovery as wd

    seen = {}

    class _Spy:
        async def get_findings_enveloped_async(self, unit, **kwargs):
            seen["findings"] = kwargs
            return {"status": STATUS_OK, "items": [], "total": 0, "meta": {}}

        async def get_findings_facets_enveloped_async(self, level, **kwargs):
            seen["facets"] = kwargs
            return {"status": STATUS_OK, "items": [], "total": 0, "meta": {}}

    monkeypatch.setattr(wd, "discovery_available", lambda: True)
    monkeypatch.setattr(wd, "_service", _Spy())

    asyncio.run(wd.get_findings_enveloped(include_divergent=True))
    assert seen["findings"]["include_divergent"] is True
    asyncio.run(wd.get_findings_facets_enveloped("domain", include_divergent=True))
    assert seen["facets"]["include_divergent"] is True

    asyncio.run(wd.get_findings_enveloped())
    assert seen["findings"]["include_divergent"] is False, (
        "the default must be ruling F's posture, not its opposite")
    asyncio.run(wd.get_findings_facets_enveloped("domain"))
    assert seen["facets"]["include_divergent"] is False


# ---------------------------------------------------------------------------
# T-136-14-05: the wrong-axis guard
# ---------------------------------------------------------------------------

#: The FJMS accessors that answer "what domain is this MANUSCRIPT catalogued
#: under". Naming them explicitly is the point: the guard has to be readable as
#: a statement about which axis is forbidden, not as a generic import ban.
_MANUSCRIPT_DOMAIN_ACCESSORS = frozenset({
    "get_domains",
    "get_domains_for_sys_ids",
    "get_manuscripts_by_domain",
    "_batch_domains",
    "get_domain_hierarchy",
    "get_all_domains",
})

_MANUSCRIPT_DOMAIN_MODULES = ("shared.fjms_service", "fjms_service")


def _wrong_axis_violations(source: str):
    """Run the wrong-axis guard over `source`, returning what it caught.

    AST rather than a substring scan, and the difference is not cosmetic: the
    service module's own docstring cites `shared/fjms_service.py` as the
    sidecar-service SHAPE it was modelled on, so a substring scan fires on
    prose and would have to be weakened until it caught nothing. The AST sees
    imports and CALLS only.
    """
    import ast

    tree = ast.parse(source)
    imports, calls = [], []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports += [a.name for a in node.names]
        elif isinstance(node, ast.ImportFrom):
            imports.append(node.module or "")
        elif isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            if name:
                calls.append(name)
    bad_imports = [
        m for m in imports
        if any(m == mod or m.startswith(mod + ".") for mod in _MANUSCRIPT_DOMAIN_MODULES)
    ]
    bad_calls = sorted({c for c in calls if c in _MANUSCRIPT_DOMAIN_ACCESSORS})
    return bad_imports, bad_calls


def _service_source() -> str:
    repo_root = pathlib.Path(__file__).resolve().parent.parent
    return (repo_root / "shared" / "discovery_service.py").read_text(encoding="utf-8")


def test_no_findings_query_path_reads_the_manuscripts_catalogue_domain():
    """The manuscript route is tempting precisely because it is cheap: FJMS
    `domains` joins on `sys_id == AlmaId` at 83% coverage (37,027 of 44,375
    findings-bearing manuscripts) with no new data. It is still the wrong axis,
    and filtering on it would hide the findings that DISAGREE with the
    catalogue -- the most valuable ones."""
    source = _service_source()
    bad_imports, bad_calls = _wrong_axis_violations(source)
    assert bad_imports == [], (
        f"the discovery service imports {bad_imports} -- the findings cascade "
        "must not reach into the FJMS catalogue at all"
    )
    assert bad_calls == [], (
        f"the discovery service calls {bad_calls} -- that is the MANUSCRIPT's "
        "catalogue domain, not the identified work's"
    )

    # And the positive half: the domain axis IS the identified work's.
    assert "w.work_id = di.display_work_id" in source
    assert "w.genre" in source


def test_the_wrong_axis_guard_actually_fires_on_a_seeded_violation():
    """An assertion that cannot fail is worse than none. This runs the guard's
    OWN logic over a seeded copy of the module carrying the forbidden import
    and the forbidden call, and asserts both are caught."""
    source = _service_source()
    seeded = source.replace(
        "def _build_findings_query(",
        "def _seeded_wrong_axis(sys_ids):\n"
        "    from shared.fjms_service import get_fjms_service\n"
        "    return get_fjms_service().get_domains_for_sys_ids(sys_ids)\n\n\n"
        "def _build_findings_query(",
        1,
    )
    assert seeded != source, "the seed did not apply -- the guard was not exercised"

    bad_imports, bad_calls = _wrong_axis_violations(seeded)
    assert bad_imports == ["shared.fjms_service"]
    assert bad_calls == ["get_domains_for_sys_ids"]
    # The clean source, by the same logic, is clean -- so the guard discriminates.
    assert _wrong_axis_violations(source) == ([], [])


# ---------------------------------------------------------------------------
# The web wrappers
# ---------------------------------------------------------------------------

def test_web_findings_wrappers_exist_and_honour_the_findings_timeout(monkeypatch):
    import web.discovery as web_discovery
    from shared.discovery_surface_projection import make_envelope

    monkeypatch.setattr(web_discovery, "discovery_available", lambda: True)
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_FINDINGS", "4.5")
    seen = []

    for name, args in (
        ("get_findings_enveloped", ()),
        ("get_findings_facets_enveloped", ("domain",)),
    ):
        wrapper = getattr(web_discovery, name)
        assert asyncio.iscoroutinefunction(wrapper), name

        async def _fake(*a, _n=name, **k):
            seen.append((_n, web_discovery._service._findings_timeout()))
            return make_envelope(STATUS_OK, [], 0)

        monkeypatch.setattr(web_discovery._service, name + "_async", _fake)
        env = asyncio.run(wrapper(*args))
        assert env["status"] == STATUS_OK

    assert [t for _n, t in seen] == [4.5, 4.5]


def test_web_findings_wrapper_fails_open_when_discovery_is_off(monkeypatch):
    import web.discovery as web_discovery

    monkeypatch.setattr(web_discovery, "discovery_available", lambda: False)
    env = asyncio.run(web_discovery.get_findings_enveloped())
    assert env["status"] == "unavailable"
    assert env["items"] == [] and env["total"] == 0
