# -*- coding: utf-8 -*-
"""Guards & exception-hygiene regressions for the 2026-06-23 audit (SEED-013).

Covers:
  #12  GenizahGUI._local_filter_state_index — unknown restored state -> 0
       (no ValueError from list.index on the next filter cycle).
  #13  GenizahGUI._text_position_from_index — combo currentIndex() == -1 or
       out-of-range -> None (no silent last-element 'line_end' wrap).
  #7   Silent swallows now log + fall back:
         - LabEngine corrupt dynamic-weights file -> logs WARNING, keeps defaults.
         - chunk-hit dedup / LOCAL-LAB dedup / LOCAL-LAB scan swallows now log
           (verified at the source level — these branches are deep inside
           lab_composition_search and only fire mid-search against a live index).
         - desktop/result_dialog regex-highlight re.error sites now log.

The #12/#13 helpers are @staticmethod / @classmethod, so they are exercised off
the class WITHOUT building a QApplication (importing genizah_app under the
conftest-set offscreen Qt platform is enough). The deep dedup branches are
asserted structurally via AST so this file is CI-safe (no widget construction).
"""
from __future__ import annotations

import ast
import logging
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# #12 — _local_filter_state_index normalizes unknown -> default (0)
# ---------------------------------------------------------------------------
def _gui_cls():
    import genizah_app
    return genizah_app.GenizahGUI


STATES = ['all', 'only_local', 'no_local']


@pytest.mark.parametrize("value,expected", [
    ('all', 0),
    ('only_local', 1),
    ('no_local', 2),
])
def test_local_filter_state_index_known(value, expected):
    assert _gui_cls()._local_filter_state_index(STATES, value) == expected


@pytest.mark.parametrize("value", ['', 'bogus', None, 'ALL', 42])
def test_local_filter_state_index_unknown_returns_default_not_raises(value):
    # A corrupt restored session value must normalize to 0, never raise.
    assert _gui_cls()._local_filter_state_index(STATES, value) == 0


def test_local_filter_state_index_cycle_is_safe_from_corrupt_value():
    # Simulate the cycle arithmetic with a corrupt stored value.
    cur = _gui_cls()._local_filter_state_index(STATES, 'corrupt')
    nxt = STATES[(cur + 1) % 3]
    assert nxt == 'only_local'  # 0 -> 1


# ---------------------------------------------------------------------------
# #13 — _text_position_from_index guards -1 / out-of-range -> None
# ---------------------------------------------------------------------------
def test_text_position_from_index_minus_one_is_none():
    # QComboBox.currentIndex() == -1 (no selection) must NOT wrap to 'line_end'.
    assert _gui_cls()._text_position_from_index(-1) is None


@pytest.mark.parametrize("i,expected", [
    (0, None),          # the explicit None option
    (1, 'start'),
    (2, 'end'),
    (3, 'line_start'),
    (4, 'line_end'),
])
def test_text_position_from_index_valid(i, expected):
    assert _gui_cls()._text_position_from_index(i) == expected


@pytest.mark.parametrize("i", [5, 99, -2, -100])
def test_text_position_from_index_out_of_range_is_none(i):
    assert _gui_cls()._text_position_from_index(i) is None


# ---------------------------------------------------------------------------
# #7 — LabEngine corrupt dynamic-weights file: log WARNING, keep defaults
# ---------------------------------------------------------------------------
def test_corrupt_dynamic_weights_logs_and_falls_back(caplog):
    import genizah_core as gc
    with patch.object(gc.LabEngine, '_reload_lab_index'), \
         patch.object(gc.LabEngine, 'reload_local_lab_index'), \
         patch('os.path.exists', return_value=True), \
         patch('builtins.open', side_effect=ValueError("corrupt weights")):
        with caplog.at_level(logging.WARNING, logger='genizah_core'):
            eng = gc.LabEngine(MagicMock(), MagicMock())
    # Defaults preserved (load failed silently before this fix).
    assert eng.dynamic_rank_map is None
    assert any('dynamic weights' in r.getMessage().lower() for r in caplog.records), \
        "corrupt dynamic-weights load must emit a WARNING"


# ---------------------------------------------------------------------------
# #7 — source-level: deep swallow sites now log instead of bare `pass`.
# These except-branches fire only mid-search against a live Tantivy index, so
# they are asserted structurally (the codebase uses AST guards for the same
# reason elsewhere). We require: the handler body contains a logging call and
# is NOT a bare `pass`.
# ---------------------------------------------------------------------------
def _handlers_in_func(tree: ast.AST, func_name: str):
    out = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
            for h in ast.walk(node):
                if isinstance(h, ast.ExceptHandler):
                    out.append(h)
    return out


def _handler_logs(handler: ast.ExceptHandler) -> bool:
    for n in ast.walk(handler):
        if isinstance(n, ast.Call):
            f = n.func
            # logging.getLogger(...).warning/debug(...) OR logger.debug(...) etc.
            if isinstance(f, ast.Attribute) and f.attr in {
                'debug', 'info', 'warning', 'error', 'exception', 'critical'
            }:
                return True
    return False


def _handler_is_bare_pass(handler: ast.ExceptHandler) -> bool:
    return len(handler.body) == 1 and isinstance(handler.body[0], ast.Pass)


def _handler_excepts_types(handler: ast.ExceptHandler, names: set[str]) -> bool:
    t = handler.type
    if t is None:
        return False
    elts = t.elts if isinstance(t, ast.Tuple) else [t]
    got = {e.id for e in elts if isinstance(e, ast.Name)}
    return got == names


def test_lab_composition_search_dedup_swallows_now_log():
    """#7: the two (KeyError, IndexError, TypeError) chunk-hit dedup handlers in
    lab_composition_search must log (formerly bare `pass`). There are two — the
    main composition path and the LOCAL-LAB branch sibling."""
    src = (ROOT / "genizah_core.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    handlers = _handlers_in_func(tree, "lab_composition_search")
    assert handlers, "lab_composition_search not found"
    dedup = [h for h in handlers
             if _handler_excepts_types(h, {"KeyError", "IndexError", "TypeError"})]
    assert len(dedup) == 2, f"expected 2 dedup handlers, found {len(dedup)}"
    for h in dedup:
        assert not _handler_is_bare_pass(h), "dedup handler still a bare `pass`"
        assert _handler_logs(h), "dedup handler must log before continuing"


def test_lab_composition_search_local_lab_scan_logs_exc_info():
    """#7: the broad LOCAL-LAB scan handler logs WITH exc_info (logging-only;
    return shape unchanged — no degraded flag added by decision)."""
    src = (ROOT / "genizah_core.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    handlers = _handlers_in_func(tree, "lab_composition_search")
    # The LOCAL-LAB scan handler binds `_local_lab_exc`.
    scan = [h for h in handlers if h.name == "_local_lab_exc"]
    assert len(scan) == 1, "LOCAL-LAB scan handler not found"
    h = scan[0]
    assert _handler_logs(h), "LOCAL-LAB scan handler must log"
    # exc_info=True present on the logging call.
    has_exc_info = any(
        isinstance(n, ast.Call) and any(
            kw.arg == "exc_info" for kw in n.keywords
        ) for n in ast.walk(h)
    )
    assert has_exc_info, "LOCAL-LAB scan log should pass exc_info=True"


def test_result_dialog_regex_error_sites_log():
    """#7: the three re.error highlight handlers in result_dialog now log."""
    src = (ROOT / "desktop" / "result_dialog.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    re_error_handlers = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and node.type is not None:
            # match `except re.error ...`
            t = node.type
            if isinstance(t, ast.Attribute) and t.attr == 'error' and \
               isinstance(t.value, ast.Name) and t.value.id == 're':
                re_error_handlers.append(node)
    # Highlight sites the audit targeted (3 of the re.error handlers; the 4th is
    # an early `return` guard, not a swallow). Require at least 3 that log.
    logging_re_handlers = [h for h in re_error_handlers if _handler_logs(h)]
    assert len(logging_re_handlers) >= 3, (
        f"expected >=3 logging re.error handlers, found {len(logging_re_handlers)} "
        f"of {len(re_error_handlers)} total"
    )


# ---------------------------------------------------------------------------
# #39 — gui_threads sleep-prevention handlers log (debug) instead of pass.
# ---------------------------------------------------------------------------
def test_sleep_prevention_handlers_log():
    src = (ROOT / "gui_threads.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    for fn in ("_prevent_sleep", "_allow_sleep"):
        handlers = _handlers_in_func(tree, fn)
        assert handlers, f"{fn} not found"
        for h in handlers:
            assert not _handler_is_bare_pass(h), f"{fn}: bare-pass handler remains"
            assert _handler_logs(h), f"{fn}: handler must log"


# ---------------------------------------------------------------------------
# #32 — RRF_K constant replaces magic 60 (default arg + module constant).
# ---------------------------------------------------------------------------
def test_rrf_k_constant_value_and_default_arg():
    import inspect
    import genizah_core as gc
    assert gc.RRF_K == 60
    sig = inspect.signature(gc.SearchEngine._rrf_merge)
    assert sig.parameters['k'].default == gc.RRF_K
