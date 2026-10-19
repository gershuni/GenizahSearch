# -*- coding: utf-8 -*-
"""Joins Lab: Library and Domain -- filter options with counts, and the list order
(shared.joins_lab helpers, the website's compute_filtered and its enrichment).

As in the website's Visual Similarity window: a fragment's domain is the first of its
domain rows, shown in Hebrew in the Hebrew UI; the English domain is the filter key.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from shared.joins_lab import (
    Candidate, counted_options, domain_label, normalize_order, order_candidates,
    primary_domain,
)


def _cand(sid, lib, score=None):
    return Candidate(sys_id=sid, page=1, library_code=lib, score=score)


CANDS = [_cand("99a", "JTS", 0.9), _cand("99b", "", 0.8), _cand("99c", "CUL", 0.7),
         _cand("99d", "JTS", 0.6)]
ENRICH = {
    "99a": {"domain": "Liturgy", "domain_heb": "ליטורגיה", "material": "paper"},
    "99c": {"domain": "Bible", "domain_heb": "מקרא"},
    "99d": {"domain": "Bible", "domain_heb": "מקרא"},
}


# --- shared helpers --------------------------------------------------------------

def test_order_is_rank_unless_library_or_domain():
    assert normalize_order("library") == "library"
    assert normalize_order("domain") == "domain"
    for bad in (None, "", "score", ["library"], 3):
        assert normalize_order(bad) == "rank"


def test_primary_domain_is_the_first_named_row():
    assert primary_domain(None) == {}
    assert primary_domain([{"domain": None}, {"domain": "Bible", "domain_heb": None}]) == {
        "domain": "Bible", "domain_heb": ""}


def test_domain_label_is_hebrew_only_in_hebrew_and_when_named():
    assert domain_label("Bible", "מקרא", hebrew=True) == "מקרא"
    assert domain_label("Bible", "מקרא", hebrew=False) == "Bible"
    assert domain_label("Bible", "", hebrew=True) == "Bible"
    assert domain_label(None, None, hebrew=True) == ""


def test_counted_options_count_each_value_in_label_order():
    pairs = [("JTS", "JTS"), ("", ""), ("CUL", "CUL"), ("JTS", "JTS"), (None, "x")]
    assert counted_options(pairs) == [("CUL", "CUL", 1), ("JTS", "JTS", 2)]


def test_order_groups_stably_and_puts_the_unknown_last():
    by_lib = order_candidates(CANDS, "library", None)
    assert [c.sys_id for c in by_lib] == ["99c", "99a", "99d", "99b"]   # JTS keeps a, d
    labels = {"99a": "Liturgy", "99c": "Bible", "99d": "Bible"}
    by_dom = order_candidates(CANDS, "domain", lambda c: labels.get(c.sys_id, ""))
    assert [c.sys_id for c in by_dom] == ["99c", "99d", "99a", "99b"]
    assert order_candidates(CANDS, "rank", None) == CANDS


# --- website: compute_filtered, options, table -----------------------------------

def test_library_and_domain_filters():
    from web.components.candidate_grid import compute_filtered
    got = compute_filtered(CANDS, {"libraries": ["JTS"]}, ENRICH, {}, "")
    assert [c.sys_id for c in got] == ["99a", "99d"]
    got = compute_filtered(CANDS, {"domains": ["Bible"]}, ENRICH, {}, "")
    assert [c.sys_id for c in got] == ["99c", "99d"]
    got = compute_filtered(CANDS, {"libraries": ["JTS"], "domains": ["Bible"]}, ENRICH, {}, "")
    assert [c.sys_id for c in got] == ["99d"]


def test_order_by_domain_follows_the_label_the_visitor_reads():
    from web.components.candidate_grid import compute_filtered
    en = compute_filtered(CANDS, {"order": "domain"}, ENRICH, {}, "", hebrew=False)
    assert [c.sys_id for c in en] == ["99c", "99d", "99a", "99b"]        # Bible < Liturgy
    he = compute_filtered(CANDS, {"order": "domain"}, ENRICH, {}, "", hebrew=True)
    assert [c.sys_id for c in he] == ["99a", "99c", "99d", "99b"]        # ליטורגיה < מקרא
    rank = compute_filtered(CANDS, {"order": "rank"}, ENRICH, {}, "")
    assert rank == CANDS


def test_filter_options_carry_counts():
    from web.components.candidate_grid import domain_options, library_options
    assert library_options(CANDS) == {"CUL": "CUL (1)", "JTS": "JTS (2)"}
    assert domain_options(CANDS, ENRICH, hebrew=False) == {
        "Bible": "Bible (2)", "Liturgy": "Liturgy (1)"}
    assert domain_options(CANDS, ENRICH, hebrew=True) == {
        "Liturgy": "ליטורגיה (1)", "Bible": "מקרא (2)"}


def test_table_keeps_a_grouped_order():
    from web.components.candidate_grid import _make_table_rows
    grouped = order_candidates(CANDS, "library", None)
    rows = _make_table_rows(grouped, {}, ENRICH, sort_mode="given")
    assert [r["sys_id"] for r in rows] == [c.sys_id for c in grouped]
    rows = _make_table_rows(grouped, {}, ENRICH, sort_mode="score")
    assert [r["sys_id"] for r in rows] == ["99a", "99b", "99c", "99d"]


def test_enrichment_adds_each_fragments_first_domain():
    """Domains ride in the same batch as the measurements, also for a fragment
    that has no measurements."""
    from web.pages import joins_lab
    svc = MagicMock()
    svc.is_available.return_value = True
    svc.get_measurement_summaries_batch.return_value = {"99a": {"material": "paper"}}
    svc.get_domains_for_sys_ids.return_value = {
        "99a": [{"domain": "Liturgy", "domain_heb": "ליטורגיה"}],
        "99c": [{"domain": "Bible", "domain_heb": "מקרא"}, {"domain": "Law"}],
    }
    with patch.object(joins_lab, "get_fjms_service", return_value=svc):
        got = asyncio.run(joins_lab._enrich_candidates(["99a", "99c"]))
    assert got == {
        "99a": {"material": "paper", "domain": "Liturgy", "domain_heb": "ליטורגיה"},
        "99c": {"domain": "Bible", "domain_heb": "מקרא"},
    }
