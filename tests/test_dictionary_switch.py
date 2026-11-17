# -*- coding: utf-8 -*-
"""shared/dictionary_switch.py: a dictionary's switch is on, off, or ``editors`` (a preview for a
signed-in editor or admin before release), and the three services read it the
same way."""
from __future__ import annotations

import pytest

from shared import arabic_service, blau_service, friedman_service
from shared.dictionary_switch import EDITORS, OFF, ON, read_switch, serves

NAME = "JA_TEST_DICTIONARY_ENABLED"


@pytest.mark.parametrize("value, state", [
    ("1", ON), ("true", ON), (" Yes ", ON), ("ON", ON),
    ("editors", EDITORS), (" Editors ", EDITORS),
    ("0", OFF), ("", OFF), ("no", OFF), ("editor", OFF), ("admins", OFF),
])
def test_a_switch_reads_on_off_or_editors(monkeypatch, value, state):
    monkeypatch.setenv(NAME, value)
    assert read_switch(NAME, ON) == state


def test_an_unset_switch_is_its_default(monkeypatch):
    monkeypatch.delenv(NAME, raising=False)
    assert read_switch(NAME, ON) == ON and read_switch(NAME, OFF) == OFF


def test_who_is_served():
    assert serves(ON, False) and serves(ON, True)
    assert serves(EDITORS, True) and not serves(EDITORS, False)
    assert not serves(OFF, True) and not serves(OFF, False)


def test_the_shortcut_card_and_help_see_an_editor_s_preview_only_for_an_editor(monkeypatch):
    """web/keyboard_shortcuts.py asks as /browse does: served to everyone, or with ``editor`` a
    dictionary switched to editors only. The What's New lines keep asking the public question."""
    from web import keyboard_shortcuts as ks

    class Service:
        def __init__(self, public, preview):
            self.public, self.preview = public, preview

        def is_available(self):
            return self.public

        def served_to(self, editor):
            return self.public or (self.preview and editor)

        def expressions_available(self):
            return True

    monkeypatch.setattr(blau_service, "get_blau_service", lambda: Service(False, True))
    monkeypatch.setattr(friedman_service, "get_friedman_service", lambda: Service(False, True))
    monkeypatch.setattr(arabic_service, "get_arabic_service", lambda: Service(True, True))
    assert ks.served_dictionaries() == {ks.BLAU: False, ks.FRIEDMAN: False, ks.LANE: True}
    assert ks.served_dictionaries(editor=True) == {ks.BLAU: True, ks.FRIEDMAN: True, ks.LANE: True}
    assert ks.expression_dictionaries() == {ks.BLAU: False, ks.FRIEDMAN: False}
    assert ks.expression_dictionaries(editor=True) == {ks.BLAU: True, ks.FRIEDMAN: True}
    monkeypatch.setattr(arabic_service, "get_arabic_service", lambda: Service(False, False))
    assert not ks.dictionaries_served() and ks.dictionaries_served(editor=True)


def test_the_card_and_help_ask_for_this_reader():
    """The call sites: the ``?`` card and the Help bullets pass whether the reader is an editor, as
    /browse does; nothing else does (What's New announces only what everyone sees)."""
    from pathlib import Path
    repo = Path(__file__).resolve().parents[1]
    main = (repo / "web" / "main.py").read_text(encoding="utf-8")
    help_src = (repo / "web" / "pages" / "help.py").read_text(encoding="utf-8")
    browse = (repo / "web" / "pages" / "browse.py").read_text(encoding="utf-8")
    assert "dictionaries=_shortcut_dictionaries_served(GlobalAuthState.is_editor())" in main
    assert "served_dictionaries(editor), expression_dictionaries(editor)" in help_src
    assert "dict_editor = GlobalAuthState.is_editor()" in browse
    # What's New: the 10.0.0 dictionaries line, which replaced 9.14.0's and 9.15.0's (Friedman's,
    # gated on served_dictionaries()), names all three, so it asks whether every reader has
    # all three (tests/render_smoke/test_whats_new_banner.py shows it hidden with one).
    assert "if all(_public_dictionaries().values()):" in main and "_served_dictionaries()" not in main


@pytest.mark.parametrize("switch, env, default", [
    (blau_service.blau_switch, blau_service.ENV_ENABLED, ON),
    (arabic_service.lane_switch, arabic_service.ENV_ENABLED, ON),
    (friedman_service.friedman_switch, friedman_service.ENV_ENABLED, OFF),
])
def test_each_service_reads_its_own_switch_with_its_default(monkeypatch, switch, env, default):
    monkeypatch.delenv(env, raising=False)
    assert switch() == default
    monkeypatch.setenv(env, "editors")
    assert switch() == EDITORS
    assert env in ("BLAU_DICTIONARY_ENABLED", "LANE_DICTIONARY_ENABLED", "FRIEDMAN_DICTIONARY_ENABLED")
