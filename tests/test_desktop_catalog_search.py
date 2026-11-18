# -*- coding: utf-8 -*-
"""The catalogue text search on the desktop (catalogue plan §4.4, PR 3): the combo's rows in one
place (desktop/catalog_search.py), the mode offered only where its file is installed, the % prefix,
the switches reaching the search thread, a search-within step keeping them, and the Source column.
No window is built: the GenizahGUI methods run on small hosts, as tests/test_desktop_variant_prefix_level.py does."""
from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

pytest.importorskip("PyQt6")

import desktop.catalog_search as cs  # noqa: E402


# --- the combo's rows -------------------------------------------------------------------

def test_the_rows_keep_every_index_they_had():
    # Before this module: start_search, the shortcut and both step builders each spelled these out.
    old = ['literal', 'variants', None, 'fuzzy', 'Regex', 'Title', 'Shelfmark']
    for index, mode in enumerate(old):
        assert cs.engine_mode(index) == (mode or 'exact')
    assert cs.engine_mode(cs.MODE_INDEX_PGP_TAGS) == 'literal'      # no engine search
    assert cs.engine_mode(99) == 'literal'
    assert cs.step_mode(cs.MODE_INDEX_RESPONSA) == 'responsa'
    assert cs.engine_mode(cs.MODE_INDEX_CATALOG) == cs.step_mode(cs.MODE_INDEX_CATALOG) == 'Catalog'


def test_the_telemetry_keys_are_the_old_enum_plus_the_catalogue():
    old = {0: 'keyword', 1: 'variants', 2: 'responsa', 3: 'fuzzy', 4: 'regex', 5: 'title',
           6: 'shelfmark', 7: 'pgp_tags'}
    for index, key in old.items():
        assert cs.telemetry_mode(index) == key
    assert cs.telemetry_mode(8) == 'catalog' and cs.telemetry_mode(42) == 'keyword'


@pytest.mark.parametrize("mode, offered, index", [
    ('literal', False, 0), ('exact', False, 0), ('variants', False, 1), ('responsa', False, 2),
    ('fuzzy', False, 3), ('Regex', False, 4), ('Title', False, 5), ('Shelfmark', False, 6),
    ('Catalog', True, 8), ('Catalog', False, None), ('variants_extended', True, None),
])
def test_a_prefix_selects_its_row(mode, offered, index):
    assert cs.combo_index(mode, offered) == index


def test_the_switches_as_the_engine_takes_them():
    assert cs.catalog_options(1, 0) == {'near_spellings': True, 'include_translations': False}


# --- offered only where the file is installed --------------------------------------------

def test_offered_when_the_file_is_where_the_service_looks(tmp_path, monkeypatch):
    import shared.catalog_text_service as svc
    db = tmp_path / "catalog_text.db"
    monkeypatch.setattr(svc, "default_sidecar_path", lambda: str(db))
    monkeypatch.delenv("CATALOG_TEXT_SEARCH_ENABLED", raising=False)
    assert not cs.catalog_offered()                 # no file
    db.write_bytes(b"x")
    assert cs.catalog_offered()                     # unset flag: on in the desktop app
    monkeypatch.setenv("CATALOG_TEXT_SEARCH_ENABLED", "0")
    assert not cs.catalog_offered()                 # turned off on purpose


def test_the_websites_default_stays_off(monkeypatch):
    from shared.catalog_text_service import catalog_text_enabled
    monkeypatch.delenv("CATALOG_TEXT_SEARCH_ENABLED", raising=False)
    assert catalog_text_enabled() is False and catalog_text_enabled(default=True) is True


# --- genizah_app on small hosts -------------------------------------------------------------

def _app():
    import genizah_app
    return genizah_app


class _Text:
    def __init__(self, text=''):
        self._text = text

    def text(self):
        return self._text

    def setText(self, text):
        self._text = text

    def blockSignals(self, _b):
        return False


class _Combo:
    def __init__(self, index=0):
        self.index = index

    def currentIndex(self):
        return self.index

    def setCurrentIndex(self, index):
        self.index = index


class _ShortcutHost:
    def __init__(self, text, offered):
        app = _app().GenizahGUI
        self._on_query_text_changed = app._on_query_text_changed.__get__(self)
        self._SHORTCUT_PREFIXES = app._SHORTCUT_PREFIXES
        self._catalog_offered = offered
        self.query_input = _Text(text)
        self.mode_combo = _Combo(0)

    def _apply_prefix_variant_preset(self, mode):
        return mode

    def _update_variant_count_preview(self):
        pass


@pytest.mark.parametrize("offered, text, index", [(True, "", 8), (False, "% ", 0)])
def test_typing_percent_and_a_space_switches_to_the_catalogue_only_where_offered(offered, text, index, monkeypatch):
    monkeypatch.setattr(_app().startup_tips, "mark_used", lambda *_a: None)
    host = _ShortcutHost("% ", offered)
    host._on_query_text_changed()
    assert host.mode_combo.index == index and host.query_input.text() == text


def test_a_search_within_step_keeps_the_switches():
    from shared.refinement import RefinementStep
    host = SimpleNamespace(_last_search_params=dict(
        query='cheese', gap=0, exclude_words=[], text_position=None, responsa_options=None,
        corpus_scope='genizah', variant_settings=None, sources=None,
        catalog_options={'near_spellings': True, 'include_translations': True}))
    step = RefinementStep(query='cheese', mode='Catalog')
    _app().GenizahGUI._apply_run_params(host, step)
    assert step.catalog_options == {'near_spellings': True, 'include_translations': True}


def test_the_search_thread_hands_the_switches_to_the_engine():
    from desktop.gui_threads import SearchThread
    calls = []

    class _Engine:
        def execute_search(self, *args, **kwargs):
            calls.append(kwargs)
            return []
    thread = SearchThread(_Engine(), 'cheese', 'Catalog', 0, catalog_options={'near_spellings': True})
    thread.run()
    assert calls and calls[0]['catalog_options'] == {'near_spellings': True}
    calls.clear()
    SearchThread(_Engine(), 'שלום', 'literal', 0).run()
    assert 'catalog_options' not in calls[0]


def test_the_source_column_names_the_catalogue_record():
    row = {'catalogue': {'source': 'pgp', 'field': 'description', 'automatic': False, 'near': True}}
    text = cs.source_text(row)
    assert text.count(' · ') == 1 and text            # "PGP description · Fuzzy" in the app's language
    assert cs.source_text({'snippet': 'x'}) == ''


def test_my_library_alone_has_no_catalogue_rows():
    from shared.search_engine import SearchEngine
    engine = SearchEngine.__new__(SearchEngine)
    engine.meta_mgr = None
    assert engine.execute_search('cheese', 'Catalog', 0, corpus_scope='local') == []


def test_no_catalogue_combo_row_means_no_catalogue_search(monkeypatch):
    # A Catalog row exists only where offered: index 8 is never selected otherwise (the % prefix
    # and the offer both check), and start_search maps an unknown index to Exact.
    assert cs.engine_mode(len(cs.COMBO_ROWS)) == 'literal'
    assert os.path.basename(cs.__file__) == 'catalog_search.py'


# --- review of #48 -------------------------------------------------------------------------------

def _variant_prefix_harness():
    import importlib
    import sys
    from pathlib import Path
    here = str(Path(__file__).resolve().parent)
    if here not in sys.path:
        sys.path.insert(0, here)
    return importlib.import_module("test_desktop_variant_prefix_level")


@pytest.mark.parametrize("lab_on, query, expected", [
    (True, "% cheese", "catalog"),          # the Lab toggle on: still not a Lab search
    (False, "% cheese", "catalog"),
    (True, "cheese", "lab_keyword"),        # a text search with Lab on is one
])
def test_a_catalogue_search_is_never_counted_as_a_lab_search(lab_on, query, expected):
    harness = _variant_prefix_harness()
    host = harness._Host(query, 30)
    host._catalog_offered = True
    host.btn_lab_mode_toggle = harness._Check(lab_on)
    host.start_search()
    assert host._current_search_run["mode"] == expected


def test_the_spec_itself_refuses_a_catalogue_file_the_reader_would_refuse():
    # A direct PyInstaller run never goes through build_app.bat (Codex on #48).
    from pathlib import Path
    root = Path(__file__).resolve().parents[1]
    spec = (root / "GenizahSearchPro.spec").read_text(encoding="utf-8")
    gate = spec.index("verify_catalog_text_sidecar.py")
    assert "--rows-from-meta" in spec[gate:gate + 200]
    assert gate < spec.index("Analysis(")
    assert "raise SystemExit" in spec[gate:spec.index("Analysis(")]
    bat = (root / "build_app.bat").read_text(encoding="utf-8")
    assert bat.index("catalog_text.db is missing") < bat.index("python -m PyInstaller")


class _Switch:
    def __init__(self, on):
        self.on = on

    def isChecked(self):
        return self.on

    def setChecked(self, on):
        self.on = bool(on)


class _HistoryHost:
    def __init__(self, mode_idx, near, translations):
        app = _app().GenizahGUI
        for name in ("_add_regular_search_to_history", "_history_catalog_options", "_history_mode_index",
                     "_restore_catalog_switches"):
            setattr(self, name, getattr(app, name).__get__(self))
        self.query_input = _Text("cheese")
        self.last_results = []
        self.mode_combo = _Combo(mode_idx)
        self.gap_input = _Text("0")
        self.text_position_combo = _Combo(0)
        self.chk_catalog_near = _Switch(near)
        self.chk_catalog_translations = _Switch(translations)

    def _shown_results_capped(self):
        return False

    def _refresh_search_history(self):
        pass


def test_a_history_entry_keeps_the_catalogue_switches_and_gives_them_back(monkeypatch):
    import shared.session_persistence as sp
    saved = []
    monkeypatch.setattr(sp, "add_history_entry", lambda kind, entry, limit=None: saved.append(entry))
    monkeypatch.setattr(_app(), "load_app_config", lambda: {})
    host = _HistoryHost(cs.MODE_INDEX_CATALOG, near=True, translations=False)
    host._add_regular_search_to_history()
    params = saved[-1]["search_params"]
    assert params["catalog_options"] == {"near_spellings": True, "include_translations": False}
    # Picked later, with the switches moved: the entry's switches come back.
    host.chk_catalog_near.setChecked(False)
    host.chk_catalog_translations.setChecked(True)
    host._restore_catalog_switches(params["catalog_options"])
    assert host.chk_catalog_near.isChecked() and not host.chk_catalog_translations.isChecked()
    # Any other mode keeps none, and an entry without them leaves the switches as they are.
    other = _HistoryHost(0, near=True, translations=True)
    other._add_regular_search_to_history()
    assert saved[-1]["search_params"]["catalog_options"] is None
    other._restore_catalog_switches(None)
    assert other.chk_catalog_near.isChecked()


def test_restoring_an_entry_puts_its_switches_and_mode_back():
    import inspect
    src = inspect.getsource(_app().GenizahGUI._restore_regular_search_from_state)
    assert "self._restore_catalog_switches(params.get('catalog_options'))" in src
    assert "params['mode_index'] != MODE_INDEX_CATALOG" in src     # not where it is not installed


def test_history_keeps_the_switches_the_run_searched_with(monkeypatch):
    # The switches and the mode stay editable while the search runs (Codex on #48).
    import shared.session_persistence as sp
    saved = []
    monkeypatch.setattr(sp, "add_history_entry", lambda kind, entry, limit=None: saved.append(entry))
    monkeypatch.setattr(_app(), "load_app_config", lambda: {})
    host = _HistoryHost(0, near=False, translations=True)       # moved to Exact, Fuzzy off since
    host._last_search_params = {"catalog_options": {"near_spellings": True, "include_translations": False},
                                "mode_index": cs.MODE_INDEX_CATALOG}
    host._add_regular_search_to_history()
    assert saved[-1]["search_params"]["catalog_options"] == {"near_spellings": True,
                                                             "include_translations": False}
    assert saved[-1]["search_params"]["mode_index"] == cs.MODE_INDEX_CATALOG   # the run's mode, not Exact
    host._last_search_params = {"catalog_options": None, "mode_index": 0}  # the run was a text search
    host.mode_combo.setCurrentIndex(cs.MODE_INDEX_CATALOG)
    host._add_regular_search_to_history()
    assert saved[-1]["search_params"]["catalog_options"] is None


def test_start_search_records_the_mode_it_dispatched():
    import inspect
    src = inspect.getsource(_app().GenizahGUI.start_search)
    assert "mode_index=mode_idx)" in src


# --- where the search found a manuscript (the viewer's pane, the Browse box) ----------------

_FOUND = {"records": [{"label": "FJMS Catalog · Note", "rtl": True,
                       "segments": [("רשומה <מוטעית> (", False), ("טעות", True), (" בסיגנטורה)", False)]}],
          "count": 3}
_ROW = {"display": {"id": "990000000000000003"},
        "catalogue": {"source": "fjms", "field": "Comment",
                      "searched": {"query": "טעות", "include_translations": True, "near_spellings": False}}}


def test_found_html_marks_the_words_escapes_the_text_and_says_what_is_left_out():
    html = cs.found_html(_FOUND, lambda s: s)
    assert "<span style='color:gray;'>FJMS Catalog · Note</span>" in html
    assert "<b style='color:red;'>טעות</b>" in html and "&lt;מוטעית&gt;" in html
    assert "<span dir='rtl'>" in html and "1 of 3 matching records" in html
    assert cs.found_html({"records": [], "count": 0}) == "" and cs.found_html(None) == ""
    two_lines = {"records": [{"label": "x", "segments": [("a\n<b>", False), ("c\nd", True)]}], "count": 1}
    assert "a<br>&lt;b&gt;<b style='color:red;'>c<br>d</b>" in cs.found_html(two_lines)


_LOOKUP = ("990000000000000003", "טעות", False, True)


class _Match:
    source, field, lang, automatic, near = "fjms", "Comment", "he", False, False
    text, spans = "רשומה זו מוטעית (טעות בסיגנטורה)", ((17, 21),)


_ANSWER = ([_Match()], 1)


def test_a_rows_lookup_is_the_search_it_carries():
    assert cs.found_lookup(_ROW) == _LOOKUP
    assert cs.found_lookup({"display": {"id": "1"}, "snippet": "x"}) is None
    assert cs.found_lookup({"display": {"id": "1"}, "catalogue": {"source": "fjms"}}) is None


@pytest.fixture
def empty_found_cache(monkeypatch):
    from collections import OrderedDict
    monkeypatch.setattr(cs, "_FOUND_CACHE", OrderedDict())


def test_kept_answers_are_bounded(empty_found_cache, monkeypatch):
    monkeypatch.setattr(cs, "FOUND_CACHE_SIZE", 2)
    for word in ("a", "b", "c"):
        cs.keep_found(("1", word, False, True), ([], 0))
    assert cs.cached_found(("1", "a", False, True)) is None
    assert cs.cached_found(("1", "c", False, True)) == ([], 0)


def test_the_worker_answers_with_the_lookups_matches(monkeypatch):
    import shared.catalog_text_rows as rows
    asked = []
    monkeypatch.setattr(rows, "catalogue_matches", lambda *a: asked.append(a) or _ANSWER)
    worker = cs.CatalogFoundWorker(_LOOKUP)
    got = []
    worker.answered.connect(lambda lookup, answer: got.append((lookup, answer)))
    worker.run()                                   # in this thread: the signal is delivered at once
    assert got == [(_LOOKUP, _ANSWER)]
    assert asked == [("990000000000000003", "טעות", {"near_spellings": False, "include_translations": True})]


class _Signal:
    def __init__(self):
        self.slots = []

    def connect(self, slot):
        self.slots.append(slot)

    def emit(self, *args):
        for slot in self.slots:
            slot(*args)


class _FakeWorker:
    """CatalogFoundWorker without a thread: the test answers it."""
    made = []

    def __init__(self, lookup, parent=None):
        self.lookup, self.started, self.interrupted = lookup, False, False
        self.answered, self.finished = _Signal(), _Signal()
        _FakeWorker.made.append(self)

    def start(self):
        self.started = True

    def isRunning(self):
        return self.started

    def requestInterruption(self):
        self.interrupted = True

    def wait(self, _ms=None):
        return True

    def deleteLater(self):
        pass

    def finish(self, answer=None):
        """The thread's end: its answer (if any), then finished, as Qt delivers them."""
        if answer is not None:
            self.answered.emit(self.lookup, answer)
        self.started = False
        self.finished.emit()


@pytest.fixture
def fake_worker(monkeypatch, empty_found_cache):
    _FakeWorker.made = []
    monkeypatch.setattr(cs, "CatalogFoundWorker", _FakeWorker)
    return _FakeWorker


class _View:
    def __init__(self):
        self.html, self.visible = None, None

    def setHtml(self, html):
        self.html = html

    def setVisible(self, visible):
        self.visible = visible

    def clear(self):
        self.html = ""


def _browse_host():
    gui = _app().GenizahGUI
    host = SimpleNamespace(browse_found_view=_View(), _browse_catalogue_row=_ROW)
    for name in ("_update_browse_found", "_on_browse_found_answered", "_fill_browse_found"):
        setattr(host, name, getattr(gui, name).__get__(host))
    return host


def test_browse_looks_up_off_the_gui_thread_and_draws_the_answer(fake_worker):
    """Codex review of #51: the lookup ran on the GUI thread. Browse now starts a worker, shows
    nothing until it answers, keeps the answer and draws it; a reopened manuscript draws at once."""
    host = _browse_host()
    host._update_browse_found("990000000000000003")
    assert len(fake_worker.made) == 1 and fake_worker.made[0].started
    assert host.browse_found_view.visible is False
    fake_worker.made[0].finish(_ANSWER)
    assert host.browse_found_view.visible and "טעות" in host.browse_found_view.html
    assert cs.cached_found(_LOOKUP) == _ANSWER
    host._update_browse_found("990000000000000099")          # another manuscript: no box
    assert host.browse_found_view.visible is False
    host._update_browse_found("990000000000000003")          # back: the kept answer, no worker
    assert len(fake_worker.made) == 1 and host.browse_found_view.visible


def test_browse_drops_an_answer_for_a_manuscript_it_left(fake_worker):
    host = _browse_host()
    host._update_browse_found("990000000000000003")
    host._update_browse_found("990000000000000099")
    fake_worker.made[0].finish(_ANSWER)
    assert host.browse_found_view.visible is False


class _Splitter:
    def __init__(self):
        self.set = None

    def sizes(self):
        return [600, 40]

    def setSizes(self, sizes):
        self.set = sizes


def _viewer_host():
    from desktop.result_dialog import ResultDialog
    host = SimpleNamespace(src_widget=_View(), text_src=_View(), lbl_src_heading=_View(),
                           ms_text_splitter=_Splitter())
    host.lbl_src_heading.setText = host.lbl_src_heading.setHtml
    for name in ("_show_catalogue_found", "_on_found_answered", "_fill_found_pane", "_give_found_pane_room"):
        setattr(host, name, getattr(ResultDialog, name).__get__(host))
    return host


def test_the_viewer_looks_up_off_the_gui_thread_and_fills_its_pane(fake_worker):
    host = _viewer_host()
    assert host._show_catalogue_found(_ROW) is True
    assert host.src_widget.visible is False and fake_worker.made[0].started
    fake_worker.made[0].finish(_ANSWER)
    assert host.src_widget.visible and "טעות" in host.text_src.html
    assert host.ms_text_splitter.set == [427, 213]           # a third of the column for the pane


def test_the_viewer_keeps_the_pane_for_other_rows_and_drops_stale_answers(fake_worker):
    host = _viewer_host()
    host._show_catalogue_found(_ROW)
    assert host._show_catalogue_found({"display": {"id": "1"}, "snippet": "x"}) is False
    fake_worker.made[0].finish(_ANSWER)                      # the row it was for is gone
    assert host.src_widget.visible is False
    assert cs.cached_found(_LOOKUP) == _ANSWER               # kept all the same
    cs._FOUND_CACHE.clear()
    host._workers_torn_down = True                           # a closed viewer draws nothing
    host._show_catalogue_found(_ROW)
    fake_worker.made[1].finish(_ANSWER)
    assert host.src_widget.visible is False


def _row_for(sys_id):
    return {"display": {"id": sys_id}, "catalogue": dict(_ROW["catalogue"])}


def test_paging_through_results_runs_one_lookup_at_a_time(fake_worker):
    """Codex review of #51: each result paged to started its own thread, all repeating the same
    near spellings. One runs at a time; of those asked for meanwhile only the latest runs next."""
    host = _viewer_host()
    rows = [_row_for(f"99000000000000000{n}") for n in (1, 2, 3)]
    for row in rows:
        host._show_catalogue_found(row)
    assert len(fake_worker.made) == 1
    fake_worker.made[0].finish(_ANSWER)
    assert len(fake_worker.made) == 2 and fake_worker.made[1].lookup == cs.found_lookup(rows[2])
    assert host.src_widget.visible is False                  # the first row's answer is not the third's
    fake_worker.made[1].finish(_ANSWER)
    assert host.src_widget.visible and len(fake_worker.made) == 2


def test_a_closed_viewer_stops_its_lookup_and_starts_no_other(fake_worker):
    host = _viewer_host()
    host._show_catalogue_found(_row_for("990000000000000001"))
    host._show_catalogue_found(_row_for("990000000000000002"))
    host._rd_found_queue.stop()
    assert fake_worker.made[0].interrupted
    fake_worker.made[0].finish(_ANSWER)
    assert len(fake_worker.made) == 1 and host.src_widget.visible is False


def test_the_viewer_and_browse_call_the_catalogue_pane():
    """The call sites: the viewer fills its lower pane from the row and stops the lookups when
    it closes; Browse keeps the row it was opened from and refreshes the box on every page."""
    import ast
    import inspect
    import textwrap
    from desktop.result_dialog import ResultDialog

    def calls(func):
        tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
        return {n.func.attr for n in ast.walk(tree)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}

    gui = _app().GenizahGUI
    assert "_show_catalogue_found" in calls(ResultDialog.load_result_by_index)
    assert "_rd_found_queue" in inspect.getsource(ResultDialog._teardown_workers)
    assert "_update_browse_found" in calls(gui.open_result_in_browse)
    assert "_update_browse_found" in calls(gui.browse_render_page)
    assert "self._browse_catalogue_row = " in inspect.getsource(gui.open_result_in_browse)
