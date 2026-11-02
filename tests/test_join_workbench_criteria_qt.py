# -*- coding: utf-8 -*-
"""Desktop Joins Lab: Manuscript features in the Filter dialog (2026-11-02).

The features filter the candidates already found, per manuscript (every page of
a manuscript they keep stays), with the candidates as the universe (D20); their
answer comes from a CriteriaMatchWorker, and until it lands -- or when it fails
-- every candidate shows and the status line says so (rule 9). Material, Width
and Has dimensions went into the features: the dialog has no filters of its own
for them, and the anchor shortcuts fill in the features.

Real JoinCandidatePane code under a QApplication over the small real sidecar
(tests/criteria_sidecar.py); the workers are run by hand, on this thread.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

import pytest

try:
    from PyQt6.QtWidgets import QApplication

    _app = QApplication.instance() or QApplication(sys.argv)
    QT_AVAILABLE = True
except ImportError:
    QT_AVAILABLE = False

pytestmark = [
    pytest.mark.skipif(not QT_AVAILABLE, reason="PyQt6 not available"),
    pytest.mark.gui,   # imports PyQt6: gui bucket only
]

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from criteria_sidecar import A, B, C, D, build_criteria_sidecar  # noqa: E402

VELLUM = {"material": {"values": ["Vellum"]}}     # A and B (catalogue)


@pytest.fixture
def svc(tmp_path, monkeypatch):
    import desktop.dialogs_filter as dialogs_filter
    import desktop.join_workbench as jw
    import genizah_core
    from shared import fjms_service
    from shared.fjms_service import FjmsService
    service = FjmsService(db_path=build_criteria_sidecar(tmp_path / "c.db"))
    monkeypatch.setattr(fjms_service, "_default_service", service)
    # English, whatever an earlier test left (tr() reads genizah_core's language)
    for module in (genizah_core, jw, dialogs_filter):
        monkeypatch.setattr(module, "CURRENT_LANG", "en")
    yield service
    service.close()


@pytest.fixture
def workers(monkeypatch):
    """CriteriaMatchWorkers are kept, not started: run_all() runs them here."""
    from desktop import gui_threads
    started = []
    monkeypatch.setattr(gui_threads.CriteriaMatchWorker, "start",
                        lambda self: started.append(self))

    def run_all():
        while started:
            started.pop(0).run()
    run_all.started = started
    return run_all


def _pane(rows=((A, 1), (A, 2), (B, 1), (C, 1), (D, 1))):
    from desktop.join_workbench import JoinCandidatePane
    from shared.joins_lab import normalize_candidate

    wb = MagicMock()
    wb.meta_mgr = None          # no ThumbResolver thread for the cards
    wb._pick_callback = None
    wb._anchor_sid = None
    wb._anchor_res = None
    wb.triage = {}
    pane = JoinCandidatePane(wb=wb, executor=None)
    pane.results = [normalize_candidate({
        "display": {"id": sid, "shelfmark": f"T-S {i}", "title": "", "library_code": "CUL",
                    "img": page},
        "uid": f"{sid}_P{page:04d}", "full_text": "x",
    }) for i, (sid, page) in enumerate(rows)]
    pane._on_enriched({})
    return pane


def _shown(pane):
    return [(c.sys_id, c.page) for c in pane.wb.filtered]


def _chip_texts(pane):
    from PyQt6.QtWidgets import QPushButton
    layout = pane._chips_layout
    return [layout.itemAt(i).widget().text() for i in range(layout.count())
            if isinstance(layout.itemAt(i).widget(), QPushButton)]


def test_the_features_keep_every_page_of_a_matching_manuscript(svc, workers):
    pane = _pane()
    pane._criteria = dict(VELLUM)
    pane.apply_filters()
    # Not known yet: every candidate, and the status says so
    assert len(_shown(pane)) == 5
    assert "Checking manuscript features…" in pane.status.text()
    assert len(workers.started) == 1
    workers()
    assert _shown(pane) == [(A, 1), (A, 2), (B, 1)]
    assert "Checking" not in pane.status.text()
    # Each feature is a chip, counted on the Filter button, and removable
    assert _chip_texts(pane) == ["Material: Vellum  ×"]
    assert pane.btn_filter.text() == "Filter (1) ▾"
    pane._chips_layout.itemAt(0).widget().click()
    assert pane._criteria == {} and len(_shown(pane)) == 5


def test_the_candidates_are_the_universe(svc, workers):
    """"Not recorded" is read among the candidates: D has no catalogue record."""
    pane = _pane()
    pane._criteria = {"region": {"values": ["_none"]}}
    pane.apply_filters()
    workers()
    assert _shown(pane) == [(D, 1)]
    assert workers.started == []     # the answer is kept: a re-render asks for nothing
    pane.apply_filters()
    assert workers.started == []


def test_a_failed_check_hides_nothing_and_says_so(svc, workers, monkeypatch):
    from shared.fjms_service import FilterUnavailable

    def boom(*_a, **_k):
        raise FilterUnavailable("query_failed", "broken")
    monkeypatch.setattr(svc, "match_result_criteria", boom)
    pane = _pane()
    pane._criteria = dict(VELLUM)
    pane.apply_filters()
    workers()
    assert len(_shown(pane)) == 5
    assert "The manuscript features could not be applied to these results." in pane.status.text()
    pane.apply_filters()             # not asked again on every render
    assert workers.started == []


def test_a_stale_answer_does_not_filter_new_results(svc, workers):
    pane = _pane()
    pane._criteria = dict(VELLUM)
    pane.apply_filters()
    pane.results = pane.results[2:]          # new results before the answer lands
    pane.apply_filters()
    workers()                                # both answers, each for its own results
    assert _shown(pane) == [(B, 1)]


def test_the_dialog_has_no_material_width_or_dimensions_and_its_shortcuts_fill_the_features(
        svc, workers, monkeypatch):
    """Material, Width and Has dimensions live in the features window only (no
    checkbox, no material list, no width boxes in the dialog); "match material" and
    "width ±2 cm of anchor" fill in the features (the measured material, as the
    cards showed it). Apply makes them the pane's; the draft's answer recounts
    the dialog's Library list before Apply."""
    import desktop.join_workbench as jw
    from PyQt6.QtWidgets import QCheckBox, QComboBox, QPushButton, QSpinBox

    pane = _pane()
    pane.wb._anchor_sid = A
    pane._enrich = {(A, 1): {"width_cm": 11.43, "material": "Vellum"}}
    seen = {}

    def drive(dlg):
        buttons = {b.text(): b for b in dlg.findChildren(QPushButton)}
        assert "Manuscript features…" in buttons
        assert dlg.findChildren(QCheckBox) == [] and dlg.findChildren(QSpinBox) == []
        assert all(c.itemText(0) != "any material" for c in dlg.findChildren(QComboBox))
        buttons["match material (Vellum)"].click()
        buttons["width ±2 cm of anchor"].click()
        seen["button"] = next(b.text() for b in dlg.findChildren(QPushButton)
                              if b.text().startswith("Manuscript features"))
        workers()
        next(b for b in dlg.findChildren(QPushButton) if b.text() == "Apply").click()

    monkeypatch.setattr(jw.QDialog, "exec", lambda dlg: (drive(dlg), 1)[1])
    pane._open_filter_dialog()
    assert seen["button"] == "Manuscript features (2)…"
    assert pane._criteria == {
        "material": {"values": ["Vellum"], "mode": "only", "source": "measured"},
        "width": {"min": 9.4, "max": 13.4}}
    workers()
    assert pane._dialog_recount is None      # let go when the dialog closed
    # Measured Vellum, width 9.4-13.4: none in the sidecar is measured Vellum
    assert _shown(pane) == []


def test_reset_and_clear_drop_the_features(svc, workers, monkeypatch):
    import desktop.join_workbench as jw
    from PyQt6.QtWidgets import QPushButton

    pane = _pane()
    pane._criteria = dict(VELLUM)
    pane.apply_filters()

    def drive(dlg):
        buttons = {b.text(): b for b in dlg.findChildren(QPushButton)}
        assert "Manuscript features (1)…" in buttons
        buttons["Reset"].click()
        assert "Manuscript features…" in {b.text() for b in dlg.findChildren(QPushButton)}
        buttons["Apply"].click()

    monkeypatch.setattr(jw.QDialog, "exec", lambda dlg: (drive(dlg), 1)[1])
    pane._open_filter_dialog()
    assert pane._criteria == {}

    pane._criteria = dict(VELLUM)
    pane._clear_lab()
    assert pane._criteria == {}


def test_a_saved_session_brings_the_features_back_cleaned(svc):
    """restore_state reads the saved features through _restored_criteria: a
    malformed one is dropped."""
    from desktop.join_workbench import _restored_criteria
    assert _restored_criteria({"material": {"values": ["Vellum"]},
                               "lines": {"min": "many"}, "evil": {}}) == {
        "material": {"source": "either", "values": ["Vellum"], "mode": "only"}}
    assert _restored_criteria(None) == {}


def test_a_5_0_0_file_drops_the_features_it_cannot_answer(tmp_path, monkeypatch):
    from desktop.join_workbench import _restored_criteria
    from shared import fjms_service
    from shared.fjms_service import FjmsService
    service = FjmsService(db_path=build_criteria_sidecar(tmp_path / "old.db", upgrade=False))
    monkeypatch.setattr(fjms_service, "_default_service", service)
    try:
        assert _restored_criteria({"period": {"min": 1000}, **VELLUM}) == {
            "material": {"source": "either", "values": ["Vellum"], "mode": "only"}}
    finally:
        service.close()
