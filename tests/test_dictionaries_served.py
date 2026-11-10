# -*- coding: utf-8 -*-
"""web/keyboard_shortcuts.py::dictionaries_served asks the same three services /browse asks before
it puts the dictionary panel on the page, so the shortcuts card and the Help page show the
dictionary rows whenever a dictionary is served, Friedman's alone included (Codex on #487)."""
from __future__ import annotations

import pytest


class _Service:
    def __init__(self, available: bool, expressions: bool = True):
        self._available = available
        self._expressions = expressions

    def is_available(self) -> bool:
        return self._available

    def expressions_available(self) -> bool:
        return self._expressions


@pytest.mark.parametrize("blau,lane,friedman,expected", [
    (False, False, False, False), (True, False, False, True), (False, True, False, True),
    (False, False, True, True),
])
def test_any_served_dictionary_counts(monkeypatch, blau, lane, friedman, expected):
    import shared.arabic_service as arabic
    import shared.blau_service as blau_module
    import shared.friedman_service as friedman_module
    from web.keyboard_shortcuts import dictionaries_served

    monkeypatch.setattr(blau_module, "get_blau_service", lambda: _Service(blau))
    monkeypatch.setattr(arabic, "get_arabic_service", lambda: _Service(lane))
    monkeypatch.setattr(friedman_module, "get_friedman_service", lambda: _Service(friedman))
    assert dictionaries_served() is expected


def test_a_broken_service_never_costs_the_page(monkeypatch):
    import shared.blau_service as blau_module
    from web.keyboard_shortcuts import dictionaries_served, served_dictionaries

    def broken():
        raise RuntimeError("disk gone")

    monkeypatch.setattr(blau_module, "get_blau_service", broken)
    assert dictionaries_served() is False
    assert served_dictionaries() == {"blau": False, "friedman": False, "lane": False}


def test_served_dictionaries_are_named_in_the_panels_tab_order(monkeypatch):
    import shared.arabic_service as arabic
    import shared.blau_service as blau_module
    import shared.friedman_service as friedman_module
    from web.components.blau_panel import BLAU_TAB, FRIEDMAN_TAB, LANE_TAB
    from web.keyboard_shortcuts import served_dictionaries

    monkeypatch.setattr(blau_module, "get_blau_service", lambda: _Service(False))
    monkeypatch.setattr(arabic, "get_arabic_service", lambda: _Service(True))
    monkeypatch.setattr(friedman_module, "get_friedman_service", lambda: _Service(True))
    served = served_dictionaries()
    assert list(served) == [BLAU_TAB, FRIEDMAN_TAB, LANE_TAB], "the Help page names them in the tab order"
    assert served == {BLAU_TAB: False, FRIEDMAN_TAB: True, LANE_TAB: True}


def test_the_expression_search_is_named_only_where_its_tokens_are(monkeypatch):
    """Blau's expression search needs the 1.1.0 sidecar; a 1.0.0 file is served without it while
    the code is on the server ahead of the rebuilt file, and the Help page must not promise it
    then (Codex on #488). Friedman's has it whenever served."""
    import shared.blau_service as blau_module
    import shared.friedman_service as friedman_module
    from web.keyboard_shortcuts import expression_dictionaries

    monkeypatch.setattr(blau_module, "get_blau_service", lambda: _Service(True, expressions=False))
    monkeypatch.setattr(friedman_module, "get_friedman_service", lambda: _Service(True))
    assert expression_dictionaries() == {"blau": False, "friedman": True}
    monkeypatch.setattr(blau_module, "get_blau_service", lambda: _Service(True, expressions=True))
    monkeypatch.setattr(friedman_module, "get_friedman_service", lambda: _Service(False))
    assert expression_dictionaries() == {"blau": True, "friedman": False}
    monkeypatch.setattr(blau_module, "get_blau_service", lambda: _Service(False, expressions=True))
    assert expression_dictionaries()["blau"] is False, "a switched-off Blau has no search to name"

    def broken():
        raise RuntimeError("disk gone")

    monkeypatch.setattr(blau_module, "get_blau_service", broken)
    assert expression_dictionaries() == {"blau": False, "friedman": False}
