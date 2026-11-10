# -*- coding: utf-8 -*-
"""Manuscript features in the web Joins Lab (2026-11-02): the "More filters…"
dialog's features filter the candidates already found, per manuscript, with the
candidates as the universe (D20 of docs/plans/CATALOGUE_CRITERIA_PLAN.md), and
the filters that read the catalogue data stand down when it cannot be read.

The pure filter (compute_filtered / facet_options / more_filters_count) is
driven directly; the matching runs on the real small sidecar
(tests/criteria_sidecar.py), over candidates' sys_ids, as the page asks it.
The page wiring is in tests/render_smoke/test_joins_lab_render_smoke.py.
"""
from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from criteria_sidecar import A, B, C, D, E, build_criteria_sidecar  # noqa: E402

from shared.joins_lab import Candidate  # noqa: E402
from web.components.candidate_grid import (  # noqa: E402
    compute_filtered, facet_options, more_filters_count,
)

LIB = {A: "CUL", B: "JTS", C: "CUL", D: "JTS", E: "BL"}


def _cands(*pairs):
    return [Candidate(sys_id=sid, page=page, via_text=True, library_code=LIB.get(sid, ""))
            for sid, page in pairs]


def _state(**over):
    state = {"criteria": {}, "exclude_mismatch": False,
             "triage_states": [], "text_q": "", "libraries": [], "domains": []}
    state.update(over)
    return state


def _keys(cands):
    return [(c.sys_id, c.page) for c in cands]


# --- The filter ------------------------------------------------------------------

def test_the_features_keep_every_candidate_page_of_a_matching_manuscript():
    cands = _cands((A, 1), (B, 3), (A, 7), (C, 2))
    got = compute_filtered(cands, _state(criteria={"material": {"values": ["Vellum"]}}),
                           {}, {}, "", hebrew=False, criteria_matched={A})
    assert _keys(got) == [(A, 1), (A, 7)]


def test_no_answer_yet_or_a_failed_one_filters_nothing():
    """None is "not known": still checking, or the check failed. The candidates
    show unfiltered (the page says why), never an empty list (rule 9)."""
    cands = _cands((A, 1), (B, 3))
    state = _state(criteria={"material": {"values": ["Vellum"]}})
    assert _keys(compute_filtered(cands, state, {}, {}, "", hebrew=False,
                                  criteria_matched=None)) == [(A, 1), (B, 3)]
    # An answer that keeps none is an answer: nothing matches
    assert compute_filtered(cands, state, {}, {}, "", hebrew=False,
                            criteria_matched=set()) == []


def test_unreadable_catalogue_data_leaves_the_filters_that_read_it_off():
    """Size mismatch and Domain read the enrichment. When it could not be read, an
    empty enrichment would hide every candidate under Domain: they stand down."""
    cands = _cands((A, 1), (B, 3))
    state = _state(exclude_mismatch=True, domains=["Bible"])
    # Readable but empty (no candidate has a domain): the Domain filter hides all
    assert compute_filtered(cands, state, {}, {}, "", hebrew=False) == []
    # Not readable: every candidate stays
    assert _keys(compute_filtered(cands, state, {}, {}, "", hebrew=False,
                                  catalogue_ok=False)) == [(A, 1), (B, 3)]


def test_the_library_counts_follow_the_features():
    cands = _cands((A, 1), (A, 2), (B, 3), (C, 4))
    state = _state(criteria={"region": {"values": ["Spanish"], "mode": "except"}})
    libs, _doms = facet_options(cands, state, {}, {}, "", False, criteria_matched={A, C})
    assert [(value, count) for value, _label, count, _depth in libs] == [
        ("CUL", 3), ("JTS", 0)]
    libs, _doms = facet_options(cands, state, {}, {}, "", False)     # not known yet
    assert [(value, count) for value, _label, count, _depth in libs] == [
        ("CUL", 3), ("JTS", 1)]


def test_more_filters_counts_each_feature():
    assert more_filters_count(_state()) == 0
    assert more_filters_count(_state(
        criteria={"material": {"values": ["Paper"]}, "width": {"min": 10}},
        exclude_mismatch=True)) == 3
    assert more_filters_count({"has_dims": True}) == 0    # not a filter any more


# --- The matching, over the candidates (D20) ---------------------------------------

@pytest.fixture
def svc(tmp_path):
    from shared.fjms_service import FjmsService
    service = FjmsService(db_path=build_criteria_sidecar(tmp_path / "c.db"))
    yield service
    service.close()


def test_the_candidates_are_the_universe(svc):
    """"All except" and "Not recorded" are read among the candidates: E (no
    catalogue record) is not recorded for region, and so passes "all except
    Oriental"; a manuscript outside the candidates never comes back."""
    universe = frozenset({A, B, E})
    assert svc.match_result_criteria(
        {"region": {"values": ["Oriental"], "mode": "except"}}, universe) == {B, E}
    assert svc.match_result_criteria(
        {"region": {"values": ["_none"]}}, universe) == {E}


def test_the_material_shortcut_matches_as_the_cards_did(svc):
    """The old Material filter read the measured material on the cards; a link or
    session from then becomes the Material feature with source "measured", which
    keeps the same manuscripts: D (measured Paper), not A (catalogue Paper)."""
    from web.joins_lab_link import clean_share_state
    f = clean_share_state({"f": {"materials": ["Paper"]}})["f"]
    assert f["criteria"] == {"material": {"source": "measured", "values": ["Paper"],
                                          "mode": "only"}}
    assert svc.match_result_criteria(f["criteria"], frozenset({A, B, D})) == {D}


def test_the_dialog_total_counts_within_the_candidates(svc, monkeypatch):
    """The features window, opened from More filters…, counts among the
    candidates ("N of the M manuscripts in the results match") and hides the
    corpus note -- the value lists stay on the corpus."""
    import asyncio

    from web.components import criteria_panel

    async def io_bound(fn, *a, **k):
        return fn(*a, **k)
    monkeypatch.setattr(criteria_panel.run, "io_bound", io_bound)
    monkeypatch.setattr(criteria_panel, "tr", lambda text: text)   # English, whatever the default

    class _Label:
        text = ""

        def set_text(self, text):
            self.text = text

    control = criteria_panel.CriteriaControl(
        get_criteria=lambda: {}, on_apply=lambda c: None, fjms=lambda: svc,
        universe=lambda: frozenset({A, B, C, D, E}), within=lambda: {A, B, E})
    control._draft = criteria_panel._draft_of(
        {"region": {"values": ["Oriental"], "mode": "except"}})
    control._total_label = _Label()
    control._note_label = note = _Label()
    control._total_gen = 1
    real_sleep = asyncio.sleep
    monkeypatch.setattr(asyncio, "sleep", lambda *_a, **_k: real_sleep(0))  # the debounce
    asyncio.run(control._update_total(1))
    assert control._total_label.text.startswith("2 of the 3 manuscripts in the results match")
    asyncio.run(control._load_note())
    assert note.text == ""
