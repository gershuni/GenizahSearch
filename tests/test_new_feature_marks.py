"""The release's one-time "New" (owner, 2026-11-18), as letter-level search was announced: the
control highlighted and its label saying it is new, no badge. The search-mode list and its Catalog
item ("New! Catalog (%)", the list's tooltip saying what it searches); the Fuzzy search switch in
Title mode ("New! Fuzzy search"). Each is shown ONE time: spent on the launch (website: the visit)
the reader first sees it, and gone at once when the reader chooses a mode, works the switch, or
uses the feature -- with the feature's startup tip. The Catalog item keeps its tooltip. Also: PGP
Tags mode keeps the mode list, the way back.

Desktop: GenizahGUI._update_new_feature_marks and its call sites. Website: web/components/new_mark.py
(the /search page itself: tests/render_smoke/test_new_marks_render_smoke.py).
"""

import ast
import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from pathlib import Path

import pytest
from PyQt6.QtCore import Qt
from PyQt6.QtWidgets import QApplication, QCheckBox, QComboBox, QHBoxLayout, QLabel, QLineEdit, QTabWidget, QWidget

import genizah_app
from desktop import startup_tips
from desktop.catalog_search import MODE_INDEX_CATALOG
from genizah_app import GenizahGUI, tr

_APP = QApplication.instance() or QApplication([])
_ROOT = Path(__file__).resolve().parent.parent
_SRC = (_ROOT / "genizah_app.py").read_text(encoding="utf-8")


@pytest.fixture
def cfg(monkeypatch):
    """config.pkl as a dict, for startup_tips and the window alike."""
    store = {}
    for module in (startup_tips, genizah_app):
        monkeypatch.setattr(module, "load_app_config", lambda: dict(store))
        monkeypatch.setattr(module, "save_app_config", store.update)
    monkeypatch.setattr(startup_tips, "_used_cache", None)
    return store


def _history(monkeypatch, regular=()):
    monkeypatch.setattr("shared.session_persistence.get_history",
                        lambda *a: {"regular": list(regular), "composition": []})


class _Host:
    """The parts of GenizahGUI the marks use, with the real methods. Each _Host is a launch."""

    for _name in ("_update_new_feature_marks", "_feature_used", "_new_mark_features_used", "_mode_chosen",
                  "_title_switch_clicked", "_search_tab_is_current", "_announcement_allowed",
                  "_mark_announcement_seen", "_retire_announcement", "_on_search_mode_changed",
                  "_show_row1_for_tags", "_MODE_INDEX_TITLE", "_NEW_COMBO_STYLE", "_NEW_SWITCH_STYLE",
                  "_NEW_COLOR", "_ANNOUNCE_CATALOG_MODE", "_ANNOUNCE_TITLE_FUZZY", "_FEATURE_ANNOUNCEMENT",
                  "CATALOG_MODE_TIP"):
        locals()[_name] = GenizahGUI.__dict__[_name]
    del _name
    _catalog_features_in = staticmethod(GenizahGUI._catalog_features_in)
    MODE_RESPONSA = 2
    MODE_PGP_TAGS = 7

    def __init__(self, offered=True, search_in_front=True):
        self._catalog_offered = offered
        self.tabs = QTabWidget()
        self.search_tab = QWidget()
        other = QWidget()
        self.tabs.addTab(other, "other")
        self.tabs.addTab(self.search_tab, "search")
        self.tabs.setCurrentWidget(self.search_tab if search_in_front else other)
        self.mode_combo = QComboBox(self.search_tab)
        self.mode_combo.addItems([str(i) for i in range(MODE_INDEX_CATALOG)])
        self.mode_combo.addItem(tr("Catalog") + " (%)")
        self.catalog_sub_row = QWidget(self.search_tab)
        self.chk_catalog_near = QCheckBox(tr("Fuzzy search"), self.catalog_sub_row)
        self.chk_catalog_translations = QCheckBox(self.catalog_sub_row)

    def _update_sources_visibility(self):
        pass

    def choose(self, index):
        """What a reader's pick in the list does: the change, then activated."""
        self.mode_combo.setCurrentIndex(index)
        self._on_search_mode_changed(index)
        self._mode_chosen()

    def marks(self):
        """(the mode list's "New", the Fuzzy search switch's "New"), each checked whole."""
        combo, near = self.mode_combo, self.chk_catalog_near
        item = (combo.itemText(MODE_INDEX_CATALOG),
                combo.itemData(MODE_INDEX_CATALOG, Qt.ItemDataRole.ForegroundRole),
                combo.itemData(MODE_INDEX_CATALOG, Qt.ItemDataRole.FontRole))
        if combo.styleSheet() == GenizahGUI._NEW_COMBO_STYLE:
            assert item[0] == tr("New! Catalog") + " (%)" and item[1] is not None and item[2].bold(), item
            assert combo.toolTip() == tr(GenizahGUI.CATALOG_MODE_TIP)
            catalog = True
        else:
            assert combo.styleSheet() == "" and item == (tr("Catalog") + " (%)", None, None), item
            assert combo.toolTip() == ""
            catalog = False
        if near.styleSheet() == GenizahGUI._NEW_SWITCH_STYLE:
            assert near.text() == tr("New! Fuzzy search"), near.text()
            fuzzy = True
        else:
            assert near.styleSheet() == "" and near.text() == tr("Fuzzy search"), near.text()
            fuzzy = False
        return catalog, fuzzy


@pytest.mark.gui
def test_each_new_goes_when_the_reader_chooses_another_mode(cfg, monkeypatch):
    """The owner's case (2026-11-18): the orange stayed after switching to another mode."""
    _history(monkeypatch)
    host = _Host()
    host._update_new_feature_marks()
    assert host.marks() == (True, False)                 # Exact: no Fuzzy search switch to mark
    host.choose(5)                                       # Title: the list's goes, the switch's shows
    assert host.marks() == (False, True)
    host.choose(0)                                       # and goes once the reader leaves Title
    assert host.marks() == (False, False)
    host.choose(5)
    assert host.marks() == (False, False)                # one time: not back on returning


@pytest.mark.gui
def test_shown_one_time_even_untouched(cfg, monkeypatch):
    """Seen on a launch, never on a later one, though the reader did nothing with it."""
    _history(monkeypatch)
    first = _Host()
    first.mode_combo.setCurrentIndex(5)
    first._on_search_mode_changed(5)
    assert first.marks() == (True, True)
    first._update_new_feature_marks()
    assert first.marks() == (True, True)                 # the launch that shows it keeps it
    later = _Host()
    later.mode_combo.setCurrentIndex(5)
    later._update_new_feature_marks()
    assert later.marks() == (False, False)


@pytest.mark.gui
def test_not_spent_unseen_behind_another_section(cfg, monkeypatch):
    _history(monkeypatch)
    hidden = _Host(search_in_front=False)
    hidden._update_new_feature_marks()
    assert hidden.marks() == (True, False)
    assert GenizahGUI._ANNOUNCE_CATALOG_MODE not in cfg
    _Host()._update_new_feature_marks()                  # the next launch, in front: shown, now spent
    assert cfg[GenizahGUI._ANNOUNCE_CATALOG_MODE] is True


@pytest.mark.gui
def test_working_the_switch_or_using_the_feature_retires_it(cfg, monkeypatch):
    _history(monkeypatch)
    host = _Host()
    host.mode_combo.setCurrentIndex(5)
    host._on_search_mode_changed(5)
    host._title_switch_clicked(False)                    # turned off: seen, not used
    assert host.marks() == (True, False)
    assert "title_fuzzy" not in startup_tips.features_used()
    host._feature_used("catalog_search")                 # a Catalog search
    assert host.marks() == (False, False)
    other = _Host.__new__(_Host)
    cfg.clear()
    startup_tips._used_cache = None
    other.__init__()
    other.mode_combo.setCurrentIndex(5)
    other._on_search_mode_changed(5)
    other._title_switch_clicked(True)                    # turned on: used, its tip retired too
    assert other.marks() == (True, False)
    assert "title_fuzzy" in startup_tips.features_used()


@pytest.mark.gui
def test_no_catalogue_new_where_it_is_not_offered_or_already_used(cfg, monkeypatch):
    _history(monkeypatch)
    host = _Host(offered=False)
    host._update_new_feature_marks()
    assert host.marks() == (False, False)
    _history(monkeypatch, [
        {"query": "x", "search_params": {"mode_index": MODE_INDEX_CATALOG}},
        {"query": "y", "search_params": {"mode_index": 5, "catalog_options": {"near_spellings": True}}}])
    used = _Host()
    used.mode_combo.setCurrentIndex(5)
    used._update_new_feature_marks()
    assert used.marks() == (False, False)


@pytest.mark.gui
def test_pgp_tags_keeps_the_mode_list(cfg, monkeypatch):
    """The owner's case (2026-11-18): PGP Tags hid row 1 whole, the mode list with it, so it
    could not be left. Leaving shows again what it hid, and only that."""
    host = _Host()
    host.search_row1_container = QWidget()
    row = QHBoxLayout(host.search_row1_container)
    query, badge = QLineEdit(), QLabel("refine")
    for widget in (query, host.mode_combo, badge):
        row.addWidget(widget)
    badge.setVisible(False)                              # its own rule: shown in refine mode only
    host._show_row1_for_tags(True)
    host._show_row1_for_tags(True)                       # twice: still knows what it hid
    assert (query.isHidden(), host.mode_combo.isHidden(), badge.isHidden()) == (True, False, True)
    host._show_row1_for_tags(False)
    assert (query.isHidden(), host.mode_combo.isHidden(), badge.isHidden()) == (False, False, True)


# -- the desktop's call sites -------------------------------------------------------------------

def _function(name):
    tree = ast.parse(_SRC)
    return ast.unparse(next(f for f in ast.walk(tree) if isinstance(f, ast.FunctionDef) and f.name == name))


def test_no_badge_and_shown_from_the_start():
    """The controls themselves say it (owner, 2026-11-18: no "New" badge beside them)."""
    assert "lbl_catalog_mode_new" not in _SRC and "lbl_title_near_new" not in _SRC
    init = _SRC[_SRC.index("        self._wire_tip_signals()\n"):][:200]
    assert "self._update_new_feature_marks()" in init
    assert "self._update_new_feature_marks()" in _function("_on_tab_changed")   # first sight


def test_only_the_readers_own_acts_spend_it():
    wire = _function("_wire_tip_signals")
    assert "self.mode_combo.activated.connect(lambda _row: self._mode_chosen())" in wire
    assert "near.clicked.connect(self._title_switch_clicked)" in wire
    assert "near.toggled.connect" not in wire


def test_a_search_retires_its_feature():
    start = _function("start_search")
    title = start.index("if mode == 'Title' and getattr(self, '_last_search_params', None) is not None")
    catalog = start.index("elif mode == 'Catalog':")
    assert title < start.index("self._feature_used('title_fuzzy')") < catalog
    assert catalog < start.index("self._feature_used('catalog_search')") < start.index("self.search_thread.start()")


def test_a_mode_change_updates_the_marks_and_keeps_the_list_in_pgp_tags():
    changed = _function("_on_search_mode_changed")
    assert "update_marks()" in changed
    assert "self._show_row1_for_tags(is_pgp_tags)" in changed
    assert "search_row1_container.setVisible" not in changed


def test_the_catalog_item_says_what_it_searches():
    assert ("self.mode_combo.setItemData(MODE_INDEX_CATALOG, tr(self.CATALOG_MODE_TIP),\n"
            "                                        Qt.ItemDataRole.ToolTipRole)") in _SRC


def test_the_found_records_retire_their_tip():
    assert "startup_tips.mark_used('catalog_found')" in _function("_fill_browse_found")
    dialog = (_ROOT / "desktop" / "result_dialog.py").read_text(encoding="utf-8")
    fill = ast.unparse(next(f for f in ast.walk(ast.parse(dialog))
                            if isinstance(f, ast.FunctionDef) and f.name == "_fill_found_pane"))
    assert fill.index("self.text_src.setHtml(found)") < fill.index("startup_tips.mark_used('catalog_found')")


def test_the_tips_show_me_opens_the_mode():
    run = _function("_run_tip_action")
    assert "elif kind == 'catalog_mode':" in run and "self.mode_combo.setCurrentIndex(MODE_INDEX_CATALOG)" in run
    assert "elif kind == 'title_fuzzy':" in run and "self._flash_widget(self.chk_catalog_near)" in run


def test_the_labels_say_new_in_the_interface_language():
    from shared.genizah_translations import TRANSLATIONS
    from web.components import new_mark
    assert TRANSLATIONS["New! Catalog"] == "חדש! קטלוג"
    assert TRANSLATIONS["New! Fuzzy search"] == "חדש! חיפוש מקורב"
    assert TRANSLATIONS["Search the catalogue information"] == "חיפוש במידע קטלוגי"
    assert GenizahGUI.CATALOG_MODE_TIP == new_mark.CATALOG_MODE_TIP
    # letter-level search's orange (_METHOD_COMBO_ACCENT)
    assert GenizahGUI._NEW_COMBO_STYLE == GenizahGUI._METHOD_COMBO_ACCENT


# -- the website's marks (web/components/new_mark.py) ---------------------------------------------

def test_the_website_marks_are_kept_per_reader(monkeypatch):
    from web import safe_storage
    from web.components import new_mark
    store = {}
    monkeypatch.setattr(safe_storage, "safe_user_get", lambda key, default=None: store.get(key, default))
    writes = []
    monkeypatch.setattr(safe_storage, "safe_user_set", lambda key, value: (writes.append(key), store.__setitem__(key, value)))
    assert new_mark.is_new(new_mark.CATALOG_SEARCH_USED_KEY)
    new_mark.mark_used(new_mark.CATALOG_SEARCH_USED_KEY)
    new_mark.mark_used(new_mark.CATALOG_SEARCH_USED_KEY)          # written once
    assert writes == [new_mark.CATALOG_SEARCH_USED_KEY]
    assert not new_mark.is_new(new_mark.CATALOG_SEARCH_USED_KEY)
    assert new_mark.is_new(new_mark.TITLE_FUZZY_USED_KEY)


def test_the_website_retires_the_marks_on_a_search():
    page = (_ROOT / "web" / "pages" / "search.py").read_text(encoding="utf-8")
    tree = ast.parse(page)
    run = ast.unparse(next(f for f in ast.walk(tree)
                           if isinstance(f, ast.AsyncFunctionDef) and f.name == "execute_search"))
    catalog = run.index("if mode == 'Catalog':\n        _retire_catalog_new()")
    assert catalog < run.index("elif mode == 'Title' and catalog_near_cb.value:\n        _retire_title_fuzzy_new()")


@pytest.mark.parametrize("run, spent", [
    ({"mode": "Catalog", "page_mode": "Catalog"}, {"ui.catalog_search_used"}),
    ({"mode": "catalog", "page_mode": "Catalog"}, {"ui.catalog_search_used"}),
    ({"page_mode": "Title", "catalog_options": {"near_spellings": True}}, {"ui.title_fuzzy_used"}),
    ({"page_mode": "Title", "catalog_options": {"near_spellings": False}}, set()),
    ({"page_mode": "exact"}, set()),
    (None, set()),
])
def test_a_run_shown_again_spends_its_features_new(monkeypatch, run, spent):
    """A reader whose restored results are a Catalog search ran one (Codex review of PR #55)."""
    from web import safe_storage
    from web.components import new_mark
    store = {}
    monkeypatch.setattr(safe_storage, "safe_user_get", lambda key, default=None: store.get(key, default))
    monkeypatch.setattr(safe_storage, "safe_user_set", store.__setitem__)
    new_mark.mark_used_by_run(run)
    assert set(store) == spent


def test_the_page_spends_it_before_it_draws_the_list():
    page = (_ROOT / "web" / "pages" / "search.py").read_text(encoding="utf-8")
    restored = page.index("        _restored_run = None\n")
    spent = page.index("new_mark.mark_used_by_run(_restored_run)")
    assert restored < spent < page.index("catalog_is_new = {'value': bool(_catalog_served)")


def test_the_option_slot_highlights_the_new_label_and_tips_the_catalog_item():
    from web.components import new_mark
    slot = new_mark.option_slot("חדש! קטלוג (%)", "קטלוג (%)", "חיפוש במידע קטלוגי")
    assert 'v-bind="props.itemProps"' in slot                      # Quasar's own item and behaviour
    assert ":class=\"{'gs-new-option': props.opt.label === 'חדש! קטלוג (%)'}\"" in slot
    assert ("<q-tooltip v-if=\"props.opt.label === 'חדש! קטלוג (%)' || props.opt.label === 'קטלוג (%)'\">"
            "חיפוש במידע קטלוגי</q-tooltip>") in slot
    quoted = new_mark.option_slot("a'b\"c", "x", "<tip>")
    assert quoted.count("a\\'b&quot;c") == 2 and "&lt;tip&gt;" in quoted   # nothing closes the attribute


def test_reset_hints_forgets_the_website_marks():
    main = (_ROOT / "web" / "main.py").read_text(encoding="utf-8")
    route = main[main.index("def reset_hints_route"):][:900]
    assert "from web.components.new_mark import KEYS as new_mark_keys" in route
    assert "*new_mark_keys" in route
