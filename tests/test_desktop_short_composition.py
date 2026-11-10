# -*- coding: utf-8 -*-
"""Desktop Composition tab: a text that does not fit the chunk settings.

Qt-free (non-gui lane). The REAL worker `run` method is borrowed onto a stub
with recording signals and drives the REAL engine with only the index
mocked, so what reaches `scan_finished_signal` is exactly what the window
receives. The window side binds the real GenizahGUI methods to a stub
carrying a label-shaped object, and AST checks pin where they are called.
"""
from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from desktop import gui_threads  # noqa: E402

_WORDS = ['אבגד', 'הוזח', 'טיכל', 'מנסע', 'פצקר', 'שתאב', 'גדהו', 'זחטי']

_SAMPLE_NOTICES = {
    'text_shorter_than_chunk_size': {'code': 'text_shorter_than_chunk_size', 'words': 3,
                                     'chunk_size': 5, 'effective_chunk_size': 3},
    'text_too_short': {'code': 'text_too_short', 'words': 1, 'minimum': 2},
    'chunk_size_raised': {'code': 'chunk_size_raised', 'chunk_size': 2,
                          'effective_chunk_size': 4},
    'min_chunk_matches_lowered': {'code': 'min_chunk_matches_lowered',
                                  'min_chunk_matches': 3, 'windows': 1},
    'text_too_common': {'code': 'text_too_common', 'words': 4},
}


class _Sig:
    def __init__(self):
        self.calls = []

    def emit(self, *args):
        self.calls.append(args if len(args) != 1 else args[0])


def _real_standard_engine():
    from genizah_core import SearchEngine
    e = SearchEngine.__new__(SearchEngine)
    e.index = MagicMock()
    e.index.parse_query.return_value = MagicMock()
    hits = MagicMock()
    hits.hits = []
    e.searcher = MagicMock()
    e.searcher.search.return_value = hits
    e.local_index = None
    e.local_searcher = None
    e._my_library_tab_ref = None
    e._has_content_search = False
    e.build_tantivy_query = MagicMock(return_value='content:x')
    e.build_regex_pattern = MagicMock(return_value=re.compile('x'))
    e._load_browse_map = MagicMock(return_value={})
    return e


def _stub_worker(cls, **attrs):
    mixin = gui_threads.PausableSearchMixin

    class _W:
        run = cls.run
        _checkpoint = mixin._checkpoint
        _should_abort = mixin._should_abort
        _emit_pause_ack = mixin._emit_pause_ack
        _init_pause_support = mixin._init_pause_support

    w = _W()
    for k, v in attrs.items():
        setattr(w, k, v)
    for s in ('progress_signal', 'status_signal', 'scan_finished_signal',
              'error_signal', 'perf_signal', 'pause_ack_signal'):
        setattr(w, s, _Sig())
    w._init_pause_support(0)
    return w


def test_composition_worker_hands_the_window_a_short_text_notice(monkeypatch):
    monkeypatch.setattr(gui_threads, '_prevent_sleep', lambda: None)
    monkeypatch.setattr(gui_threads, '_allow_sleep', lambda: None)
    engine = _real_standard_engine()
    w = _stub_worker(
        gui_threads.CompositionThread,
        searcher=engine, text=' '.join(_WORDS[:3]), chunk=5, freq=10,
        mode='exact', filter_text=None, threshold=5, boundary_mode='full',
        boundary_delimiter='\n', boundary_boost=1.5, min_boundary_matches=0,
        min_delimiter_distance=3, restrict_sys_ids=None, corpus_scope='genizah')
    w.run()
    assert w.error_signal.calls == []
    (result,) = w.scan_finished_signal.calls
    assert engine.build_tantivy_query.call_count == 1, 'the short text was never searched'
    assert [n['code'] for n in result.get('composition_notices') or []] == [
        'text_shorter_than_chunk_size']


class _Label:
    def __init__(self):
        self.text = 'stale'
        self.visible = True

    def setText(self, s):
        self.text = s

    def setVisible(self, b):
        self.visible = bool(b)


def _gui():
    import genizah_app
    return genizah_app.GenizahGUI


def _stub_window():
    stub = type('S', (), {})()
    stub.lbl_comp_chunk_notice = _Label()
    return stub


def test_window_shows_the_short_text_notice():
    stub = _stub_window()
    _gui()._show_comp_chunk_notice(
        stub, {'main': [], 'composition_notices': [_SAMPLE_NOTICES['text_shorter_than_chunk_size']]})
    lbl = stub.lbl_comp_chunk_notice
    assert lbl.visible and '3' in lbl.text and '5' in lbl.text


def test_window_clears_the_line_for_an_ordinary_run():
    # A leftover message from the previous run must not sit above new rows.
    stub = _stub_window()
    _gui()._show_comp_chunk_notice(stub, {'main': [], 'composition_notices': []})
    assert not stub.lbl_comp_chunk_notice.visible
    assert stub.lbl_comp_chunk_notice.text == ''


# --- call sites --------------------------------------------------------------

def _function(name):
    tree = ast.parse((REPO_ROOT / 'genizah_app.py').read_text(encoding='utf-8'))
    return next(n for n in ast.walk(tree)
                if isinstance(n, ast.FunctionDef) and n.name == name)


def _is_self_call(stmt, attr):
    return (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
            and isinstance(stmt.value.func, ast.Attribute)
            and stmt.value.func.attr == attr)


def test_scan_finished_shows_the_notice_for_every_run():
    """The call must be a top-level statement of on_comp_scan_finished --
    not inside the letter-level branch or any other condition -- and must
    come after the one early return (a pending close)."""
    fn = _function('on_comp_scan_finished')
    idx = [i for i, s in enumerate(fn.body) if _is_self_call(s, '_show_comp_chunk_notice')]
    assert idx, ('on_comp_scan_finished does not call _show_comp_chunk_notice '
                 'as a top-level statement')
    first_return_holder = next(
        i for i, s in enumerate(fn.body)
        if any(isinstance(n, ast.Return) for n in ast.walk(s)))
    assert idx[0] > first_return_holder


def test_new_and_restore_clear_the_notice():
    """The notice is not saved with a session or history entry, so New and a
    restore must clear it rather than leave it above rows it does not
    describe."""
    for name in ('_reset_composition', '_display_restored_comp_snapshot'):
        fn = _function(name)
        calls = [n for n in ast.walk(fn) if isinstance(n, ast.Call)
                 and isinstance(n.func, ast.Attribute)
                 and n.func.attr == '_clear_comp_chunk_notice']
        assert calls, f'{name} does not clear the chunk notice'


def test_chunk_spin_box_matches_the_api_range():
    src = (REPO_ROOT / 'genizah_app.py').read_text(encoding='utf-8')
    assert 'self.spin_chunk.setRange(2, 20)' in src


# --- strings -----------------------------------------------------------------

def test_every_notice_renders_in_both_languages():
    """Every code formats without error in English and Hebrew, and the Hebrew
    keeps every number the English shows."""
    from shared.composition_windows import NOTICE_STRINGS, chunk_notice_message
    from shared.genizah_translations import TRANSLATIONS

    missing = [s for s in NOTICE_STRINGS if s not in TRANSLATIONS]
    assert not missing, missing
    for code, notice in _SAMPLE_NOTICES.items():
        en = chunk_notice_message(notice, lambda s: s)
        he = chunk_notice_message(notice, lambda s: TRANSLATIONS[s])
        assert en and he, code
        assert sorted(re.findall(r'\d+', en)) == sorted(re.findall(r'\d+', he)), (code, en, he)
