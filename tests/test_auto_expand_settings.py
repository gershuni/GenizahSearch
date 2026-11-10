"""Auto-expand must not depend on the Witnesses dialog having been opened.

Owner UAT 2026-09-03, "Full Recursive Search" in letter-level mode:

    File "genizah_app.py", line 25315, in run_recursive_composition
      self._run_auto_expand()
    File "genizah_app.py", line 16890, in _run_auto_expand
      self._auto_expand_left = int(self.spin_comp_auto_rounds.value())
  AttributeError: 'GenizahGUI' object has no attribute 'spin_comp_auto_rounds'

`spin_comp_auto_rounds` / `spin_comp_auto_topk` are built inside
`_open_witness_dialog` and destroyed with it. `run_recursive_composition`
reaches `_run_auto_expand` without going through that dialog at all, so the
attributes need never have existed -- and once the dialog HAS been opened and
closed, touching the deleted C++ object raises RuntimeError instead. The
durable values are `_comp_auto_rounds_pref` / `_comp_auto_topk_pref`, which the
dialog writes back on close.
"""

import inspect
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest

from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui


class _Bare:
    """A window that has never opened the Witnesses dialog."""

    _auto_expand_settings = GenizahGUI._auto_expand_settings


class _DeletedWidget:
    """A spin box whose underlying C++ object is gone."""

    def value(self):
        raise RuntimeError(
            "wrapped C/C++ object of type QSpinBox has been deleted")


class _Live:
    _auto_expand_settings = GenizahGUI._auto_expand_settings

    def __init__(self, rounds, top_k):
        self._witness_dialog = object()
        self.spin_comp_auto_rounds = type("_S", (), {"value": lambda _s: rounds})()
        self.spin_comp_auto_topk = type("_S", (), {"value": lambda _s: top_k})()


def test_defaults_when_the_dialog_was_never_opened():
    """The reported crash: no attributes at all."""
    assert _Bare()._auto_expand_settings() == (3, 5)


def test_saved_preferences_are_used_when_the_dialog_is_closed():
    w = _Bare()
    w._comp_auto_rounds_pref = 5
    w._comp_auto_topk_pref = 2
    assert w._auto_expand_settings() == (5, 2)


def test_live_spin_boxes_win_while_the_dialog_is_open():
    assert _Live(4, 7)._auto_expand_settings() == (4, 7)


def test_deleted_spin_boxes_fall_back_to_the_preferences():
    w = _Live(4, 7)
    w.spin_comp_auto_rounds = _DeletedWidget()
    w.spin_comp_auto_topk = _DeletedWidget()
    w._comp_auto_rounds_pref = 2
    w._comp_auto_topk_pref = 9
    assert w._auto_expand_settings() == (2, 9)


def test_a_dialog_reference_without_the_widgets_still_returns_defaults():
    w = _Bare()
    w._witness_dialog = object()  # mid-construction, widgets not attached yet
    assert w._auto_expand_settings() == (3, 5)


def test_values_are_ints():
    rounds, top_k = _Live(4.0, 7.0)._auto_expand_settings()
    assert isinstance(rounds, int) and isinstance(top_k, int)


def test_run_auto_expand_reads_the_accessor_not_the_widgets():
    src = inspect.getsource(GenizahGUI._run_auto_expand)
    assert "_auto_expand_settings()" in src
    assert "self.spin_comp_auto_rounds" not in src, (
        "reading the dialog-owned spin box directly is the reported crash"
    )
    assert "self.spin_comp_auto_topk" not in src


def test_the_defaults_match_the_dialog_spin_boxes():
    """The accessor's fallbacks must equal what the dialog would show."""
    dlg_src = inspect.getsource(GenizahGUI._open_witness_dialog)
    assert "getattr(self, '_comp_auto_rounds_pref', 3)" in dlg_src
    assert "getattr(self, '_comp_auto_topk_pref', 5)" in dlg_src
    acc_src = inspect.getsource(GenizahGUI._auto_expand_settings)
    assert "'_comp_auto_rounds_pref', 3" in acc_src
    assert "'_comp_auto_topk_pref', 5" in acc_src
