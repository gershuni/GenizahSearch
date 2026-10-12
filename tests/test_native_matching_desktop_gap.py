# -*- coding: utf-8 -*-
"""The word gap limit on the desktop (docs/plans/DESKTOP_NATIVE_MATCHING_PLAN.md §3.6,
gate 10's desktop rows). Owner ruling 2026-10-12: at most 10 in Exact, Variants and
Fuzzy; Responsa and Regex take any gap. Never clamped: another gap is another search.

- The Search press (GenizahGUI.start_search) refuses a capped-mode gap over the limit
  with a translated notice, before anything of a run starts: no worker, no Stop
  button. Regex, Responsa, Title and Lab Mode (which never reads the gap) start.
- A saved refinement step over the limit (a search saved before it) is refused by
  the engine at execute_search entry. Every path that runs a saved step again --
  the session-restore replay, the chain completion before search-within, and the
  replay after a chip is removed -- says which step and why, and leaves the step's
  gap as it was stored. A Responsa or Regex step with gap 50 runs as before.

The window is a MagicMock with the real methods under test bound to it (the
pattern of tests/test_desktop_variant_prefix_level.py::_composition_window), so
each test drives the call site itself; the threads are the real ones, run on
this thread. The stand-in engine refuses with the engine's own entry check, and
test_the_stand_in_refuses_as_execute_search_does pins that to SearchEngine.

Mutations that redden these tests:
- remove the check in start_search (`if not _is_lab and _word_gap_limit_error(`
  -> `if False and ...`): the refused press rows;
- replace the notice in GenizahGUI._explain_word_gap_refusal with a clamp
  (`step.gap = MAX_WORD_GAP`), or clamp the chain's gaps before the restore
  replay starts: every saved-step row.
"""
from __future__ import annotations

import ast
import io
import os
import string
from types import MethodType, SimpleNamespace
from unittest.mock import MagicMock

import pytest

import genizah_app
import genizah_core
import shared.search_engine as se
from shared.genizah_translations import TRANSLATIONS
from shared.refinement import RefinementStep

APP = genizah_app.GenizahGUI
APP_PY = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "genizah_app.py")
A, B = "990000000000100001", "990000000000200002"
EXACT, VARIANTS, RESPONSA, FUZZY, REGEX, TITLE = 0, 1, 2, 3, 4, 5      # the mode combo


@pytest.fixture(params=["en", "he"])
def lang(request, monkeypatch):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", request.param)
    return request.param


@pytest.fixture
def notices(monkeypatch):
    """What _show_ok_notice put on screen: (kind, title, text)."""
    shown = []
    monkeypatch.setattr(genizah_app, "_show_ok_notice",
                        lambda parent, kind, title, text: shown.append((kind, title, text)))
    return shown


def _press_text(gap):
    return genizah_core.tr(
        "In Exact, Variants and Fuzzy searches the word gap is at most {limit}; "
        "this search asks for {gap}.\n\nLower the gap, or search in Responsa or "
        "Regex mode, which have no limit.").format(limit=10, gap=gap)


def _step_text(query, gap):
    return genizah_core.tr(
        "The saved search \"{query}\" uses a word gap of {gap}. In Exact, Variants "
        "and Fuzzy searches the gap is at most {limit}, so this step was not run "
        "again. Its gap was not changed.").format(query=query, gap=gap, limit=10)


# --- The Search press ------------------------------------------------------------------

def _pressing(monkeypatch, mode_idx, gap, lab=False):
    """The window as start_search reads it, a search typed and the gap box at *gap*."""
    w = MagicMock()
    w.MODE_RESPONSA, w.MODE_PGP_TAGS = RESPONSA, 7
    w.query_input.text.return_value = "שלום עולם"
    w._pre_search_filters_blocked.return_value = False
    w.searcher = SimpleNamespace(parse_query_syntax=lambda q, responsa_mode=False: (None, q))
    w.mode_combo.currentIndex.return_value = mode_idx
    w.btn_lab_mode_toggle.isChecked.return_value = lab
    w.gap_input.text.return_value = str(gap)
    w.exclude_input.text.return_value = ""
    w.corpus_scope_combo.currentData.return_value = "genizah"
    w._refine_mode, w.refinement_chain, w.var_mgr, w.meta_loader = False, [], None, None
    w._lab_settings.return_value = None
    w.export_buttons, w._run_seq, w.is_searching = [], 0, False
    w.pre_search_restrict_sys_ids = w.refinement_restrict_sys_ids = None
    w._current_search_run = {"mode": "the previous run"}
    w._drain_previous_worker.return_value = True
    search, lab_search = MagicMock(name="SearchThread"), MagicMock(name="LabSearchThread")
    monkeypatch.setattr(genizah_app, "SearchThread", search)
    monkeypatch.setattr(genizah_app, "LabSearchThread", lab_search)
    return w, search, lab_search


@pytest.mark.parametrize("mode_idx", [EXACT, VARIANTS, FUZZY], ids=["exact", "variants", "fuzzy"])
def test_a_gap_over_the_limit_is_refused_at_the_press(monkeypatch, notices, lang, mode_idx):
    w, search, lab_search = _pressing(monkeypatch, mode_idx, 11)
    APP.start_search(w)
    assert notices == [("warning", genizah_core.tr("Word gap"), _press_text(11))]
    assert search.call_count == 0 and lab_search.call_count == 0, "a worker was built"
    assert w.is_searching is False
    w.btn_search.setText.assert_not_called()          # the button never turned into Stop
    w._drain_previous_worker.assert_not_called()      # a running search is left alone
    assert w._current_search_run == {"mode": "the previous run"}
    w.gap_input.setText.assert_not_called()           # nothing clamped


def test_the_notice_is_in_the_interface_language(monkeypatch, notices):
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "he")
    w, _search, _lab = _pressing(monkeypatch, VARIANTS, 12)
    APP.start_search(w)
    ((_kind, title, text),) = notices
    assert title == "מרווח מילים" and text.startswith("בחיפוש מדויק") and "12" in text and "10" in text


@pytest.mark.parametrize("mode_idx,gap,mode", [
    (VARIANTS, 10, "variants"),          # at the limit
    (EXACT, 0, "literal"),
    (REGEX, 50, "Regex"),                # Regex ignores the gap box: no limit
    (RESPONSA, 50, "exact"),             # Responsa keeps any gap ([N] tokens too)
    (TITLE, 50, "Title"),
], ids=["variants-10", "exact-0", "regex-50", "responsa-50", "title-50"])
def test_a_gap_the_engine_takes_starts_the_search(monkeypatch, notices, mode_idx, gap, mode):
    w, search, _lab = _pressing(monkeypatch, mode_idx, gap)
    APP.start_search(w)
    assert notices == []
    assert search.call_count == 1, "no search started"
    assert search.call_args.args[2:4] == (mode, gap)
    responsa = search.call_args.kwargs["responsa_options"]
    assert bool(responsa and responsa["responsa_mode"]) is (mode_idx == RESPONSA)
    search.return_value.start.assert_called_once_with()
    assert w.is_searching is True


def test_lab_mode_starts_whatever_the_gap_it_does_not_read(monkeypatch, notices):
    w, search, lab_search = _pressing(monkeypatch, VARIANTS, 50, lab=True)
    APP.start_search(w)
    assert notices == [] and search.call_count == 0
    assert lab_search.call_count == 1
    lab_search.return_value.start.assert_called_once_with()


# --- A saved step over the limit -----------------------------------------------------------

class _Engine:
    """execute_search's entry as the engine has it -- the word-gap check first
    (pinned to SearchEngine below) -- then the rows held for the query. Records
    every call, so a clamp would show as a call with another gap."""

    def __init__(self, rows=None, fail=None):
        self.rows, self.fail, self.calls = rows or {}, fail, []

    def execute_search(self, query, mode, gap, **kw):
        self.calls.append((query, mode, gap))
        se._validate_word_gap(mode, gap, kw.get("responsa_options"))
        if self.fail:
            raise RuntimeError(self.fail)
        return [{"uid": f"{sid}_1", "display": {"id": sid}} for sid in self.rows.get(query, [])]


def test_the_stand_in_refuses_as_execute_search_does():
    """SearchEngine.execute_search refuses at entry, before reading anything of
    the engine (so a bare one shows it), with the message the stand-in raises --
    the one the desktop matches a failed step by."""
    engine = se.SearchEngine.__new__(se.SearchEngine)
    with pytest.raises(ValueError) as refused:
        engine.execute_search("שלום עולם", "variants", 50)
    assert str(refused.value) == genizah_app._word_gap_limit_error("variants", 50)
    with pytest.raises(ValueError) as stand_in:
        _Engine().execute_search("שלום עולם", "variants", 50)
    assert str(stand_in.value) == str(refused.value)


class _NowReplay(genizah_app.RefinementReplayThread):
    def start(self):
        self.run()                      # same thread: delivered directly


class _NowCompletion(genizah_app.ChainCompletionThread):
    def start(self):
        self.run()


@pytest.fixture
def chain_window(monkeypatch):
    """A window holding *chain*, searching with *engine*, with the real replay,
    completion and explanation methods bound to it."""
    monkeypatch.setattr(genizah_app, "RefinementReplayThread", _NowReplay)
    monkeypatch.setattr(genizah_app, "ChainCompletionThread", _NowCompletion)
    monkeypatch.setattr(genizah_app, "QApplication", SimpleNamespace(processEvents=lambda: None))
    boxes = MagicMock(name="QMessageBox")             # the generic error path's box
    monkeypatch.setattr(genizah_app, "QMessageBox", boxes)

    def build(chain, engine):
        w = MagicMock()
        w.refinement_chain, w.searcher = chain, engine
        w.pre_search_restrict_sys_ids, w.refinement_restrict_sys_ids = None, {"before"}
        w._refinement_step_searcher = lambda: None   # every step on *engine*
        w._after_restore_replay = w._replay_restore_thread = None
        w.is_searching, w._run_seq, w._search_new_generation = False, 0, 0
        w._search_was_cancelled = False
        for name in ("_replay_for_restore", "_on_replay_for_restore_finished",
                     "_on_replay_for_restore_error", "_run_after_restore_replay",
                     "_explain_word_gap_refusal", "_replay_refinement_chain",
                     "_remove_refinement_step", "_complete_chain_then",
                     "_discardable", "_deliver_unless_discarded"):
            setattr(w, name, MethodType(getattr(APP, name), w))
        w.boxes = boxes
        return w
    return build


def _ok(query):
    return RefinementStep(query, "literal", gap=0, corpus_scope="genizah")


def _over(query="שני", gap=50):
    return RefinementStep(query, "variants", gap=gap, corpus_scope="genizah",
                          responsa_options={"responsa_mode": False})


def test_a_restored_step_over_the_limit_is_explained_and_kept_as_saved(chain_window, notices, lang):
    first, refused = _ok("ראשון"), _over()
    engine = _Engine({"ראשון": [A, B]})
    w = chain_window([first, refused], engine)
    w._replay_for_restore()
    assert engine.calls == [("ראשון", "literal", 0), ("שני", "variants", 50)], (
        "the step was run with another gap")
    assert notices == [("warning", genizah_core.tr("Word gap"), _step_text("שני", 50))]
    w.boxes.warning.assert_not_called()
    assert w.refinement_chain == [first, refused] and refused.gap == 50, "the saved chain changed"
    assert first._result_sys_ids == {A, B} and refused._result_sys_ids is None
    assert w.refinement_restrict_sys_ids is None


@pytest.mark.parametrize("step", [
    RefinementStep("שלום [50] עולם", "responsa", gap=50, corpus_scope="genizah",
                   responsa_options={"responsa_mode": True, "variants": False}),
    RefinementStep("שלום.*עולם", "Regex", gap=50, corpus_scope="genizah"),
], ids=["responsa", "regex"])
def test_a_restored_responsa_or_regex_step_with_gap_50_replays(chain_window, notices, step):
    engine = _Engine({step.query: [A]})
    w = chain_window([step], engine)
    w._replay_for_restore()
    assert engine.calls == [(step.query, step.mode, 50)]
    assert notices == [] and step.gap == 50
    assert w.refinement_restrict_sys_ids == {A} and w.refinement_chain == [step]


def test_another_restore_failure_still_clears_the_chain(chain_window, notices):
    w = chain_window([_ok("ראשון")], _Engine(fail="disk gone"))
    w._replay_for_restore()
    assert notices == [] and w.refinement_chain == [] and w.refinement_restrict_sys_ids is None


def test_completing_a_chain_stops_at_a_step_over_the_limit_with_the_explanation(chain_window, notices):
    refused = _over("שלישי", 30)
    refused.result_count_capped = True             # cut off: search-within completes it first
    engine = _Engine()
    w = chain_window([refused], engine)
    done = []
    w._complete_chain_then(None, done.append)
    assert engine.calls == [("שלישי", "variants", 30)]
    assert notices == [("warning", genizah_core.tr("Word gap"), _step_text("שלישי", 30))]
    w.boxes.warning.assert_not_called()
    assert done == [{"restrict": None, "interrupted": True}]
    assert refused.gap == 30 and w.refinement_chain == [refused]


def test_another_completion_failure_keeps_its_error_box(chain_window, notices):
    step = _ok("ראשון")
    step.result_count_capped = True
    w = chain_window([step], _Engine(fail="disk gone"))
    done = []
    w._complete_chain_then(None, done.append)
    assert notices == []
    w.boxes.warning.assert_called_once_with(w, genizah_core.tr("Error"), "disk gone")
    assert done == [{"restrict": None, "interrupted": True}]


def test_removing_a_chip_replays_up_to_a_step_over_the_limit_and_explains(chain_window, notices):
    first, refused, last = _ok("ראשון"), _over("שני", 11), _ok("שלישי")
    engine = _Engine({"ראשון": [A]})
    w = chain_window([first, refused, last], engine)
    w._remove_refinement_step(2)
    assert engine.calls == [("ראשון", "literal", 0), ("שני", "variants", 11)]
    assert notices == [("warning", genizah_core.tr("Word gap"), _step_text("שני", 11))]
    assert w.refinement_chain == [first, refused] and refused.gap == 11


# --- Both languages ---------------------------------------------------------------------

def _gap_limit_keys():
    """The tr() literals of the word-gap notices in genizah_app.py."""
    tree = ast.parse(io.open(APP_PY, encoding="utf-8").read())
    return sorted({n.args[0].value for n in ast.walk(tree)
                   if isinstance(n, ast.Call) and getattr(n.func, "id", None) == "tr"
                   and n.args and isinstance(n.args[0], ast.Constant)
                   and isinstance(n.args[0].value, str)
                   and "Exact, Variants" in n.args[0].value and "{limit}" in n.args[0].value})


def _fields(text):
    return sorted(f for _lit, f, _spec, _conv in string.Formatter().parse(text) if f)


def test_each_notice_has_a_hebrew_text_with_the_same_fields():
    keys = _gap_limit_keys()
    assert len(keys) == 2, keys          # the press, and a saved step not run again
    for key in keys:
        he = TRANSLATIONS.get(key)
        assert he and he != key, f"no Hebrew for {key!r}"
        assert _fields(he) == _fields(key), key
        assert any("֐" <= ch <= "׿" for ch in he)
