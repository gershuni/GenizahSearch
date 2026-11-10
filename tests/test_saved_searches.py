"""Saved searches, the window's size, and the Composition Search tooltips
(owner, 2026-10-25: "saved searches (star icon on the list) and usable tooltips
in composition search. Remember windows size and position").

Pinned here: a starred history entry is kept whatever its age -- the history
limit counts only the rest and Clear keeps it -- and a re-run keeps it starred;
the menu lists saved searches first, each row acting on its own stored index;
the window's geometry survives a close and a start; and the Composition
tooltips are translated.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest
from PyQt6.QtWidgets import QApplication, QMenu, QPushButton, QWidget, QWidgetAction

import genizah_app
from genizah_app import GenizahGUI

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])


@pytest.fixture
def sp(tmp_path, monkeypatch):
    import shared.session_persistence as session_persistence
    monkeypatch.setattr(session_persistence, "HISTORY_FILE",
                        str(tmp_path / "search_history.json"))
    return session_persistence


def _add(sp, query, ts, limit=20, kind="regular"):
    sp.add_history_entry(kind, {"query": query, "result_count": 1, "timestamp": ts,
                                "search_params": {"mode_index": 0}, "state": {"q": query}},
                         limit=limit)


def _queries(sp, kind="regular"):
    return [e["query"] for e in sp.get_history(kind)]


def test_a_saved_search_outlives_the_limit(sp):
    _add(sp, "old", "2026-01-01T00:00:00")
    assert sp.set_history_pinned("regular", 0, True)
    for n in range(5):
        _add(sp, f"q{n}", f"2026-02-0{n + 1}T00:00:00", limit=3)
    # The three newest plain searches, and the saved one besides.
    assert _queries(sp) == ["q4", "q3", "q2", "old"]
    assert sp.get_history("regular")[-1]["pinned"] is True


def test_rerunning_a_saved_search_keeps_it_saved(sp):
    _add(sp, "a", "2026-01-01T00:00:00")
    sp.set_history_pinned("regular", 0, True)
    _add(sp, "a", "2026-03-01T00:00:00")          # same query and params: updated in place
    [entry] = sp.get_history("regular")
    assert entry["pinned"] is True and entry["timestamp"].startswith("2026-03")


def test_unsave_and_bad_index(sp):
    _add(sp, "a", "2026-01-01T00:00:00")
    sp.set_history_pinned("regular", 0, True)
    assert sp.set_history_pinned("regular", 0, False)
    assert "pinned" not in sp.get_history("regular")[0]
    assert not sp.set_history_pinned("regular", 5, True)


def test_clear_keeps_saved_searches_only_when_asked(sp):
    _add(sp, "keep", "2026-01-01T00:00:00")
    _add(sp, "drop", "2026-01-02T00:00:00")
    sp.set_history_pinned("regular", 1, True)      # "keep" (newest first: drop, keep)
    _add(sp, "comp", "2026-01-03T00:00:00", kind="composition")
    assert sp.clear_history("regular", keep_pinned=True)
    assert _queries(sp) == ["keep"]
    assert _queries(sp, "composition") == ["comp"]  # the other list is untouched
    assert sp.clear_history(None)
    assert _queries(sp) == [] and _queries(sp, "composition") == []


class _Host:
    """The parts of GenizahGUI the history menu uses, with the real methods."""

    _MODE_CHARS = GenizahGUI._MODE_CHARS
    _HISTORY_HIGHLIGHT = GenizahGUI._HISTORY_HIGHLIGHT
    _HISTORY_LBL_HIGHLIGHT = GenizahGUI._HISTORY_LBL_HIGHLIGHT
    _HISTORY_LBL_NORMAL = GenizahGUI._HISTORY_LBL_NORMAL
    _HISTORY_DEL_HIGHLIGHT = GenizahGUI._HISTORY_DEL_HIGHLIGHT
    _HISTORY_DEL_NORMAL = GenizahGUI._HISTORY_DEL_NORMAL
    _HISTORY_STAR_HIGHLIGHT = GenizahGUI._HISTORY_STAR_HIGHLIGHT
    _HISTORY_STAR_SAVED = GenizahGUI._HISTORY_STAR_SAVED
    _HISTORY_STAR_NORMAL = GenizahGUI._HISTORY_STAR_NORMAL
    _BAKED_WITNESS_SUFFIX = GenizahGUI._BAKED_WITNESS_SUFFIX
    _DANGLING_BRACKET = GenizahGUI._DANGLING_BRACKET
    _fill_history_menu = GenizahGUI._fill_history_menu
    _add_history_heading = staticmethod(GenizahGUI._add_history_heading)
    _add_history_menu_item = GenizahGUI._add_history_menu_item
    _history_star_style = GenizahGUI._history_star_style
    _on_history_menu_hovered = GenizahGUI._on_history_menu_hovered
    _history_query_with_witnesses = GenizahGUI._history_query_with_witnesses
    _build_filter_summary = staticmethod(GenizahGUI._build_filter_summary)
    _count_text = GenizahGUI._count_text
    _set_history_saved = GenizahGUI._set_history_saved

    def __init__(self):
        self.menu = QMenu()
        self.reshown = []

    def _on_history_item_clicked(self, st, entry):
        pass

    def _delete_history_item(self, st, idx):
        pass

    def _reshow_history_menu(self, search_type, reopen):
        self.reshown.append((search_type, reopen))


def _rows(menu):
    """(heading or query label, star text) per menu row, in order."""
    out = []
    for a in menu.actions():
        if isinstance(a, QWidgetAction):
            w = a.defaultWidget()
            out.append((w.findChild(QPushButton, "history_label").text(),
                        w.findChild(QPushButton, "history_star").text(), a._history_index))
        elif not a.isSeparator():
            out.append((a.text(), None, None))
    return out


def test_the_menu_lists_saved_searches_first_with_their_own_indexes(sp):
    _add(sp, "older", "2026-01-01T00:00:00")
    _add(sp, "newer", "2026-01-02T00:00:00")
    sp.set_history_pinned("regular", 1, True)      # "older"
    host = _Host()
    host._fill_history_menu(host.menu, "regular", "empty")
    rows = _rows(host.menu)
    assert rows[0][0] == genizah_app.tr("Saved searches")
    assert "older" in rows[1][0] and rows[1][1] == "★" and rows[1][2] == 1
    assert rows[2][0] == genizah_app.tr("Recent searches")
    assert "newer" in rows[3][0] and rows[3][1] == "☆" and rows[3][2] == 0
    assert rows[-1][0] == genizah_app.tr("Clear history (saved searches stay)")


def test_the_star_saves_and_the_hover_keeps_its_colour(sp):
    _add(sp, "q", "2026-01-01T00:00:00")
    host = _Host()
    host._fill_history_menu(host.menu, "regular", "empty")
    action = next(a for a in host.menu.actions() if isinstance(a, QWidgetAction))
    star = action.defaultWidget().findChild(QPushButton, "history_star")
    assert _rows(host.menu)[-1][0] == genizah_app.tr("Clear all history")   # nothing saved yet
    star.click()
    assert sp.get_history("regular")[0]["pinned"] is True
    assert host.reshown == [("regular", True)]
    host._fill_history_menu(host.menu, "regular", "empty")
    action = next(a for a in host.menu.actions() if isinstance(a, QWidgetAction))
    star = action.defaultWidget().findChild(QPushButton, "history_star")
    host._on_history_menu_hovered(action, menu=host.menu)
    assert star.styleSheet() == GenizahGUI._HISTORY_STAR_HIGHLIGHT
    host._on_history_menu_hovered(None, menu=host.menu)
    assert star.styleSheet() == GenizahGUI._HISTORY_STAR_SAVED


def test_an_empty_history(sp):
    host = _Host()
    host._fill_history_menu(host.menu, "composition", "nothing")
    [only] = host.menu.actions()
    assert only.text() == "nothing" and not only.isEnabled()


class _Window(QWidget):
    WINDOW_GEOMETRY_KEY = GenizahGUI.WINDOW_GEOMETRY_KEY
    save_window_geometry = GenizahGUI.save_window_geometry
    restore_window_geometry = GenizahGUI.restore_window_geometry


def test_the_window_comes_back_where_it_was(monkeypatch):
    store = {}
    monkeypatch.setattr(genizah_app, "save_app_config", store.update)
    monkeypatch.setattr(genizah_app, "load_app_config", lambda: dict(store))
    first = _Window()
    assert first.restore_window_geometry() is False       # nothing kept: opens maximized
    first.setGeometry(120, 90, 700, 500)
    first.show()
    QApplication.processEvents()
    first.save_window_geometry()
    assert isinstance(store[GenizahGUI.WINDOW_GEOMETRY_KEY], str)
    second = _Window()
    assert second.restore_window_geometry() is True
    assert (second.width(), second.height()) == (700, 500)
    store[GenizahGUI.WINDOW_GEOMETRY_KEY] = "not geometry"
    assert _Window().restore_window_geometry() is False
    first.close()


def test_startup_and_close_wiring():
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")
    assert "if window.restore_window_geometry():\n        window.show()\n    else:\n        window.showMaximized()" in src
    close = src[src.index("    def closeEvent(self, event):"):]
    close = close[:close.index("self._app_shutting_down = True")]
    assert "self.save_window_geometry()" in close


def test_composition_tooltips_are_translated():
    from shared.genizah_translations import TRANSLATIONS
    for key in (
            "Your text is searched in runs of this many words. Fewer words find more, "
            "including chance matches; more words find fewer, surer ones. "
            "5 to 7 suits most texts.",
            "A page is listed only when at least this many different chunks of your "
            "text are found on it. Raise it to drop pages that share only one phrase "
            "with your text. With cross-paragraph search, the ⚙ setting is used "
            "instead.",
            "Search again with the best results as more witnesses, merged into one list."):
        assert TRANSLATIONS.get(key), key
    assert GenizahGUI._recursive_tip("checked") == genizah_app.tr(
        "Search again with the full text of the ticked results added to yours.")
