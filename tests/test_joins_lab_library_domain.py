# -*- coding: utf-8 -*-
"""Joins Lab: Library and Domain -- filter options with counts, and the list order
(shared.joins_lab helpers, the website's compute_filtered and its enrichment).

As in the website's Visual Similarity window: a fragment's domain is the first of its
domain rows, shown in Hebrew in the Hebrew UI; the English domain is the filter key.
Each filter takes several values, to show only them or to hide them. Domain is a
tree (the canonical FJMS hierarchy: a parent takes in its branch), and each filter
counts the candidates the other filters keep.
"""
from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch

from shared.joins_lab import (
    Candidate, counted_options, domain_hebrew_names, domain_label, domain_path,
    domain_path_of, domain_tree_options, filter_passes, normalize_filter_mode,
    normalize_order, order_candidates, path_passes, primary_domain,
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
        "domain": "Bible", "domain_heb": "", "domain_path": [["Bible", "Bible", ""]]}


# Hierarchy rows as FjmsService.get_domain_hierarchy() gives them (names only)
HIERARCHY = {
    "Rabbinic Literature": {"parent_domain_heb": "ספרות חז\"ל", "children": [
        {"domain": "Midrash", "domain_heb": "מדרש", "children": [
            {"domain": "Aggadic Midrashim", "domain_heb": "מדרשי אגדה"}]}]},
    "Piyut and its Interpretation": {"parent_domain_heb": "פיוט ופירושו", "children": [
        {"domain": "Piyyut", "domain_heb": "פיוט"}]},
}
NAMES = domain_hebrew_names(HIERARCHY)


def test_hebrew_names_cover_every_level():
    assert NAMES == {"Rabbinic Literature": "ספרות חז\"ל", "Midrash": "מדרש",
                     "Aggadic Midrashim": "מדרשי אגדה",
                     "Piyut and its Interpretation": "פיוט ופירושו", "Piyyut": "פיוט"}
    assert domain_hebrew_names(None) == {}


def test_domain_path_climbs_the_canonical_tree():
    # A sub-domain: two levels up, though the row names no parent
    assert domain_path("Aggadic Midrashim", None, "מדרשי אגדה", "", NAMES) == [
        ["Rabbinic Literature", "Rabbinic Literature", "ספרות חז\"ל"],
        ["Midrash", "Midrash", "מדרש"],
        ["Aggadic Midrashim", "Aggadic Midrashim", "מדרשי אגדה"]]
    # A domain recorded at the top level that the hierarchy puts under a parent
    assert [step[0] for step in domain_path("Piyyut", None, "", "", NAMES)] == [
        "Piyut and its Interpretation", "Piyyut"]
    # An ambiguous name is keyed by its parent; an unknown one by the row's parent
    assert domain_path("Other", "Liturgy and Brakhot", "אחר", "תפילה וברכות") == [
        ["Liturgy and Brakhot", "Liturgy and Brakhot", "תפילה וברכות"],
        ["Other (Liturgy and Brakhot)", "Other", "אחר"]]
    assert domain_path("Bible", "Bible") == [["Bible", "Bible", ""]]    # its own parent
    assert domain_path(None) == [] and domain_path("") == []


def test_domain_path_of_falls_back_to_the_domain_alone():
    path = [["Midrash", "Midrash", ""]]
    assert domain_path_of({"domain_path": path, "domain": "x"}) == path
    assert domain_path_of({"domain": "Bible", "domain_heb": "מקרא"}) == [
        ["Bible", "Bible", "מקרא"]]
    assert domain_path_of({"domain": "--"}) == [] and domain_path_of(None) == []


def test_a_chosen_parent_takes_in_its_branch():
    agg = domain_path("Aggadic Midrashim", hebrew_names=NAMES)
    assert path_passes(agg, {"Rabbinic Literature"}, "show_only")
    assert path_passes(agg, {"Midrash"}, "show_only")
    assert not path_passes(agg, {"Midrash"}, "hide")
    assert path_passes(agg, {"Piyyut"}, "hide") and not path_passes(agg, {"Piyyut"}, "show_only")
    assert path_passes([], {"Midrash"}, "hide") and not path_passes([], {"Midrash"}, "show_only")
    assert path_passes(agg, set(), "show_only") and path_passes([], [], "hide")


def test_domain_tree_options_nest_in_the_canonical_order_and_count_the_branch():
    agg = domain_path("Aggadic Midrashim", hebrew_names=NAMES)
    mid = domain_path("Midrash", "Rabbinic Literature", hebrew_names=NAMES)
    piy = domain_path("Piyyut", hebrew_names=NAMES)
    assert domain_tree_options([piy, agg, mid]) == [
        ("Rabbinic Literature", "Rabbinic Literature", 2, 0),       # canonical: before Piyut
        ("Midrash", "Midrash", 2, 1),
        ("Aggadic Midrashim", "Aggadic Midrashim", 1, 2),
        ("Piyut and its Interpretation", "Piyut and its Interpretation", 1, 0),
        ("Piyyut", "Piyyut", 1, 1)]
    # Counted among some candidates only: every node keeps its row
    counted = domain_tree_options([piy, agg, mid], [piy], hebrew=True)
    assert [(key, n) for key, _label, n, _d in counted] == [
        ("Rabbinic Literature", 0), ("Midrash", 0), ("Aggadic Midrashim", 0),
        ("Piyut and its Interpretation", 1), ("Piyyut", 1)]
    assert counted[0][1] == "ספרות חז\"ל"                          # Hebrew labels


def test_counted_options_can_count_some_candidates_only():
    pairs = [("JTS", "JTS"), ("CUL", "CUL"), ("JTS", "JTS")]
    assert counted_options(pairs, counted=["CUL", "CUL", "X"]) == [
        ("CUL", "CUL", 2), ("JTS", "JTS", 0)]


def test_domain_label_is_hebrew_only_in_hebrew_and_when_named():
    assert domain_label("Bible", "מקרא", hebrew=True) == "מקרא"
    assert domain_label("Bible", "מקרא", hebrew=False) == "Bible"
    assert domain_label("Bible", "", hebrew=True) == "Bible"
    assert domain_label(None, None, hebrew=True) == ""


def test_filter_mode_is_show_only_unless_hide():
    assert normalize_filter_mode("hide") == "hide"
    for other in (None, "", "show_only", "exclude", ["hide"], 1):
        assert normalize_filter_mode(other) == "show_only"


def test_filter_passes_shows_only_or_hides_the_chosen():
    chosen = {"Bible", "Liturgy"}
    assert filter_passes("Bible", chosen, "show_only")
    assert not filter_passes("Law", chosen, "show_only")
    assert not filter_passes(None, chosen, "show_only")      # no value: not chosen
    assert not filter_passes("Bible", chosen, "hide")
    assert filter_passes("Law", chosen, "hide")
    assert filter_passes(None, chosen, "hide")               # no value: not hidden
    assert filter_passes("", {"CUL"}, "hide") and not filter_passes("", {"CUL"}, "show_only")
    for mode in ("show_only", "hide", None):                 # nothing chosen: all pass
        assert filter_passes("Bible", set(), mode) and filter_passes(None, [], mode)


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
    got = compute_filtered(CANDS, {"domains": ["Bible", "Liturgy"]}, ENRICH, {}, "")
    assert [c.sys_id for c in got] == ["99a", "99c", "99d"]


def test_hidden_libraries_are_left_out():
    """'hide' leaves the chosen libraries out; a candidate with no library stays."""
    from web.components.candidate_grid import compute_filtered
    f = {"libraries": ["JTS"], "libraries_mode": "hide"}
    assert [c.sys_id for c in compute_filtered(CANDS, f, ENRICH, {}, "")] == ["99b", "99c"]
    f = {"libraries": ["JTS", "CUL"], "libraries_mode": "hide"}
    assert [c.sys_id for c in compute_filtered(CANDS, f, ENRICH, {}, "")] == ["99b"]
    f = {"libraries": [], "libraries_mode": "hide"}           # nothing hidden: all
    assert compute_filtered(CANDS, f, ENRICH, {}, "") == CANDS
    f = {"libraries": ["CUL"], "libraries_mode": "hide",
         "domains": ["Liturgy"], "domains_mode": "hide"}
    assert [c.sys_id for c in compute_filtered(CANDS, f, ENRICH, {}, "")] == ["99b", "99d"]


def test_hidden_domains_are_left_out():
    """'hide' leaves the chosen domains out; a candidate with no domain stays."""
    from web.components.candidate_grid import compute_filtered
    f = {"domains": ["Bible"], "domains_mode": "hide"}
    assert [c.sys_id for c in compute_filtered(CANDS, f, ENRICH, {}, "")] == ["99a", "99b"]
    f = {"domains": ["Bible", "Liturgy"], "domains_mode": "hide"}
    assert [c.sys_id for c in compute_filtered(CANDS, f, ENRICH, {}, "")] == ["99b"]
    f = {"domains": [], "domains_mode": "hide"}               # nothing hidden: all
    assert compute_filtered(CANDS, f, ENRICH, {}, "") == CANDS
    f = {"libraries": ["JTS"], "domains": ["Liturgy"], "domains_mode": "hide"}
    assert [c.sys_id for c in compute_filtered(CANDS, f, ENRICH, {}, "")] == ["99d"]


def test_order_by_domain_follows_the_label_the_visitor_reads():
    from web.components.candidate_grid import compute_filtered
    en = compute_filtered(CANDS, {"order": "domain"}, ENRICH, {}, "", hebrew=False)
    assert [c.sys_id for c in en] == ["99c", "99d", "99a", "99b"]        # Bible < Liturgy
    he = compute_filtered(CANDS, {"order": "domain"}, ENRICH, {}, "", hebrew=True)
    assert [c.sys_id for c in he] == ["99a", "99c", "99d", "99b"]        # ליטורגיה < מקרא
    rank = compute_filtered(CANDS, {"order": "rank"}, ENRICH, {}, "")
    assert rank == CANDS


def test_filter_options_carry_counts():
    from web.components.candidate_grid import facet_options
    libs, doms = facet_options(CANDS, {}, ENRICH, {}, "", hebrew=False)
    assert libs == [("CUL", "CUL", 1, 0), ("JTS", "JTS", 2, 0)]
    assert doms == [("Bible", "Bible", 2, 0), ("Liturgy", "Liturgy", 1, 0)]
    _libs, doms = facet_options(CANDS, {}, ENRICH, {}, "", hebrew=True)
    assert [(v, label) for v, label, _n, _d in doms] == [("Bible", "מקרא"), ("Liturgy", "ליטורגיה")]


def test_each_filter_counts_what_the_other_filters_keep():
    """Choosing (or hiding) domains updates the library counts, and the other way
    round; a filter's own choice never changes its own counts, and every value
    keeps its row."""
    from web.components.candidate_grid import facet_options

    def counts(f):
        libs, doms = facet_options(CANDS, f, ENRICH, {"99d": "no"}, "", hebrew=False)
        return ({v: n for v, _l, n, _d in libs}, {v: n for v, _l, n, _d in doms})

    assert counts({"domains": ["Bible"]}) == ({"CUL": 1, "JTS": 1}, {"Bible": 2, "Liturgy": 1})
    assert counts({"domains": ["Bible"], "domains_mode": "hide"}) == (
        {"CUL": 0, "JTS": 1}, {"Bible": 2, "Liturgy": 1})
    assert counts({"libraries": ["JTS"]}) == ({"CUL": 1, "JTS": 2}, {"Bible": 1, "Liturgy": 1})
    assert counts({"libraries": ["JTS"], "libraries_mode": "hide"}) == (
        {"CUL": 1, "JTS": 2}, {"Bible": 1, "Liturgy": 0})
    # The other filters count too: Triage "No" keeps 99d only
    assert counts({"triage_states": ["No"]}) == ({"CUL": 0, "JTS": 1}, {"Bible": 1, "Liturgy": 0})


def test_a_chosen_parent_domain_filters_its_branch():
    from web.components.candidate_grid import compute_filtered, facet_options
    enrich = {
        "99a": primary_domain([{"domain": "Aggadic Midrashim"}], NAMES),
        "99c": primary_domain([{"domain": "Midrash", "parent_domain": "Rabbinic Literature"}], NAMES),
        "99d": primary_domain([{"domain": "Piyyut"}], NAMES),
    }
    f = {"domains": ["Rabbinic Literature"]}
    assert [c.sys_id for c in compute_filtered(CANDS, f, enrich, {}, "")] == ["99a", "99c"]
    f = {"domains": ["Midrash"], "domains_mode": "hide"}
    assert [c.sys_id for c in compute_filtered(CANDS, f, enrich, {}, "")] == ["99b", "99d"]
    _libs, doms = facet_options(CANDS, {}, enrich, {}, "", hebrew=True)
    assert [(v, label, n, d) for v, label, n, d in doms] == [
        ("Rabbinic Literature", "ספרות חז\"ל", 2, 0), ("Midrash", "מדרש", 2, 1),
        ("Aggadic Midrashim", "מדרשי אגדה", 1, 2),
        ("Piyut and its Interpretation", "פיוט ופירושו", 1, 0), ("Piyyut", "פיוט", 1, 1)]


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
    svc.get_domain_hierarchy.return_value = HIERARCHY
    with patch.object(joins_lab, "get_fjms_service", return_value=svc):
        got = asyncio.run(joins_lab._enrich_candidates(["99a", "99c"]))
    assert got == {
        "99a": {"material": "paper", "domain": "Liturgy", "domain_heb": "ליטורגיה",
                "domain_path": [["Liturgy", "Liturgy", "ליטורגיה"]]},
        "99c": {"domain": "Bible", "domain_heb": "מקרא",
                "domain_path": [["Bible", "Bible", "מקרא"]]},
    }
    # With its place in the tree, named in Hebrew from the hierarchy
    svc.get_domains_for_sys_ids.return_value = {"99a": [{"domain": "Aggadic Midrashim"}]}
    with patch.object(joins_lab, "get_fjms_service", return_value=svc):
        got = asyncio.run(joins_lab._enrich_candidates(["99a"]))
    assert [step[2] for step in got["99a"]["domain_path"]] == [
        "ספרות חז\"ל", "מדרש", "מדרשי אגדה"]
