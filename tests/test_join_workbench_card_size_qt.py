# -*- coding: utf-8 -*-
"""Desktop Joins Lab grid cards come in three sizes (S/M/L), as on the website.

Real JoinCandidatePane / CandidateCard widgets under a QApplication; the window is a
MagicMock with no metadata manager, so no thumbnail thread starts. config.pkl is an
in-memory dict.
"""
from __future__ import annotations

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


@pytest.fixture
def config(monkeypatch):
    import desktop.join_workbench as jw

    cfg = {}
    monkeypatch.setattr(jw, "load_app_config", lambda: dict(cfg))
    monkeypatch.setattr(jw, "save_app_config", lambda new: cfg.update(new))
    return cfg


def _candidates(n):
    from shared.joins_lab import normalize_candidate
    return [normalize_candidate({
        "display": {"id": f"99{i:016d}", "shelfmark": f"T-S {i}", "title": "",
                    "library_code": "CUL", "img": 1},
        "uid": f"99{i:016d}_P0001", "full_text": "x",
    }) for i in range(n)]


def _pane(n=10):
    from desktop.join_workbench import JoinCandidatePane

    wb = MagicMock()
    wb.meta_mgr = None          # no ThumbResolver thread
    wb._pick_callback = None
    wb.triage = {}
    wb.filtered = _candidates(n)
    pane = JoinCandidatePane(wb=wb, executor=None)
    pane.render_results()
    return pane


def _columns(pane):
    layout = pane.grid_layout
    return {layout.getItemPosition(i)[1] for i in range(layout.count())}


def test_medium_is_the_grid_as_it_was(config):
    pane = _pane()
    assert pane.card_size == "m"
    assert pane.card_size_combo.currentData() == "m"
    card = pane.cards[0]
    assert card.width() == 232
    assert (card.img.width(), card.img.height()) == (220, 130)
    assert _columns(pane) == {0, 1, 2, 3}


def test_a_stored_size_is_used(config):
    config["joins_lab_card_size"] = "l"
    pane = _pane()
    assert pane.card_size_combo.currentData() == "l"
    card = pane.cards[0]
    assert card.width() == 312
    assert (card.img.width(), card.img.height()) == (300, 240)
    assert _columns(pane) == {0, 1, 2}


def test_a_mangled_stored_size_is_medium(config):
    config["joins_lab_card_size"] = ["l"]
    assert _pane().card_size == "m"


def test_choosing_a_size_redraws_the_page_and_is_remembered(config):
    pane = _pane()
    pane.card_size_combo.setCurrentIndex(pane.card_size_combo.findData("s"))
    assert pane.card_size == "s"
    assert config["joins_lab_card_size"] == "s"
    assert pane.cards[0].width() == 196
    assert _columns(pane) == {0, 1, 2, 3, 4}


@pytest.mark.parametrize("size", ["s", "m", "l"])
def test_every_size_holds_the_card_rows(config, size):
    """The action row's six fixed buttons must fit inside the card at every size."""
    config["joins_lab_card_size"] = size
    card = _pane(1).cards[0]
    assert card.layout().minimumSize().width() <= card.contentsRect().width()


def test_the_size_control_is_for_the_grid_only(config):
    pane = _pane()
    pane.show()
    try:
        assert pane.card_size_combo.isVisible()
        pane.toggle_view()                       # table
        assert not pane.card_size_combo.isVisible()
        pane.toggle_view()                       # grid again
        assert pane.card_size_combo.isVisible()
    finally:
        pane.close()


def test_a_size_chosen_in_table_view_applies_on_return_to_the_grid(config):
    pane = _pane()
    pane.toggle_view()                           # table
    pane.card_size_combo.setCurrentIndex(pane.card_size_combo.findData("l"))
    assert config["joins_lab_card_size"] == "l"
    pane.toggle_view()                           # grid
    assert pane.cards[0].width() == 312
