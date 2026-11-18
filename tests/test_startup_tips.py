"""Startup tips (desktop/startup_tips.py; owner, 2026-10-25).

Pinned here: the 61 tips the owner approved, each with its Hebrew, in the chosen
order (the strongest five first, then the newest features); a tip is shown once a round, and not when its
feature is already in use, and a new round starts when all have been; no tip on the first start, beside another notice bar, or once
turned off; five closes in a row turn the tips off; a tip that needs the
letter-level index or an account invites to it; every Show me names a control
or action the window has; and the strip's direction marks.
"""

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

from pathlib import Path

import pytest
from PyQt6.QtWidgets import QApplication, QWidget

import genizah_app
from desktop import startup_tips
from desktop.startup_tips import TIPS, TIPS_BY_KEY, TipStrip, next_tip, tip_html
from genizah_app import GenizahGUI
from shared.genizah_translations import TRANSLATIONS

pytestmark = pytest.mark.gui

_APP = QApplication.instance() or QApplication([])
_SRC = (Path(__file__).resolve().parent.parent / "genizah_app.py").read_text(encoding="utf-8")


@pytest.fixture
def cfg(monkeypatch):
    """config.pkl as a dict, for startup_tips and the window alike."""
    store = {}
    for module in (startup_tips, genizah_app):
        monkeypatch.setattr(module, "load_app_config", lambda: dict(store))
        monkeypatch.setattr(module, "save_app_config", store.update)
    monkeypatch.setattr(startup_tips, "_used_cache", None)
    return store


def test_the_owners_tips_in_the_owners_order():
    assert len(TIPS) == 61
    assert len(TIPS_BY_KEY) == 61                     # keys are unique
    assert [t.key for t in TIPS[:5]] == [
        "go_to", "saved_searches", "letter_level", "witnesses", "responsa_star"]
    # the catalogue search and Title's Fuzzy search (2026-11-18) come next, then the
    # Manuscript features (2026-11-02), so tip readers meet the newest soon
    assert [t.key for t in TIPS[5:8]] == ["catalog_search", "catalog_found", "title_fuzzy"]
    assert [t.requires for t in TIPS[5:8]] == ["catalog", "catalog", None]
    assert [t.key for t in TIPS[8:11]] == ["features_search", "features_browse", "features_joins"]
    assert {t.used for t in TIPS[8:11]} == {"features"}
    for tip in TIPS:
        assert TRANSLATIONS.get(tip.text), tip.key     # every tip has its Hebrew
        assert tip.text.count("**") % 2 == 0, tip.key
        assert TRANSLATIONS[tip.text].count("**") % 2 == 0, tip.key
        assert tip.needs in (None, "letter_index", "account"), tip.key
        assert tip.requires in (None, "catalog"), tip.key


def test_bold_escaping_and_direction():
    tip = startup_tips.Tip("t", "Use **A & B** <now>")
    assert tip_html(tip, lang="en") == "‎Use <b>A &amp; B</b> &lt;now&gt;"
    he = tip_html(TIPS_BY_KEY["go_to"], lang="he")
    assert he.startswith("‏Ctrl+G")          # a key first, still read right to left


def test_next_tip_skips_what_was_shown_or_is_in_use():
    assert next_tip([], []).key == "go_to"
    assert next_tip(["go_to"], ["saved_search"]).key == "letter_level"
    assert next_tip([t.key for t in TIPS], []) is None
    # a tip with no feature to watch is skipped only once shown
    assert next_tip([t.key for t in TIPS if t.key != "result_keys"], ["anything"]).key == "result_keys"


def test_state_in_config(cfg):
    assert startup_tips.record_start() == 1 and startup_tips.record_start() == 2
    startup_tips.mark_seen(TIPS[0])
    startup_tips.mark_seen(TIPS[0])
    assert cfg[startup_tips.TIPS_SEEN_KEY] == ["go_to"]
    startup_tips.mark_used("cite")
    writes = dict(cfg)
    startup_tips.mark_used("cite")                     # once per feature
    assert cfg == writes and startup_tips.features_used() == {"cite"}
    for _ in range(startup_tips.CLOSES_BEFORE_OFF - 1):
        assert not startup_tips.note_closed()
    startup_tips.note_engaged()                        # Show me / Next tip: the count starts again
    assert cfg[startup_tips.TIPS_CLOSED_KEY] == 0
    for _ in range(startup_tips.CLOSES_BEFORE_OFF - 1):
        startup_tips.note_closed()
    assert startup_tips.note_closed() and startup_tips.tips_off()


def test_the_strip():
    strip = TipStrip()
    got = []
    strip.show_me.connect(got.append)
    strip.next_requested.connect(lambda: got.append("next"))
    strip.closed.connect(lambda: got.append("close"))
    strip.turned_off.connect(lambda: got.append("off"))
    strip.set_tip(TIPS[0], "Show me", has_next=True)
    assert not strip.isHidden() and strip.btn_show.text() == "Show me"
    for b in (strip.btn_show, strip.btn_next, strip.btn_close, strip.btn_off):
        b.click()
    assert got == [TIPS[0], "next", "close", "off"]
    strip.set_tip(TIPS_BY_KEY["result_keys"], None, has_next=False)
    assert strip.btn_show.isHidden() and strip.btn_next.isHidden()


class _Host:
    """The parts of GenizahGUI the tips use, with the real methods."""

    _maybe_show_startup_tip = GenizahGUI._maybe_show_startup_tip
    _show_next_tip = GenizahGUI._show_next_tip
    _tip_invitation = GenizahGUI._tip_invitation
    _tip_action_label = GenizahGUI._tip_action_label
    _tip_show_me = GenizahGUI._tip_show_me
    _tip_next = GenizahGUI._tip_next
    _tip_features_in_use = GenizahGUI._tip_features_in_use
    _tip_derived_features = GenizahGUI._tip_derived_features
    _tips_offered = GenizahGUI._tips_offered
    _catalog_features_in = staticmethod(GenizahGUI._catalog_features_in)
    _catalog_offered = True
    _MODE_INDEX_RESPONSA = GenizahGUI._MODE_INDEX_RESPONSA
    _MODE_INDEX_TITLE = GenizahGUI._MODE_INDEX_TITLE
    _MODE_INDEX_PGP_TAGS = GenizahGUI._MODE_INDEX_PGP_TAGS
    RESULTS_TEXT_KEY = GenizahGUI.RESULTS_TEXT_KEY

    def __init__(self):
        self.tip_strip = TipStrip()
        self.whats_new_bar = QWidget()
        self.actions = []

    def _run_tip_action(self, action):
        self.actions.append(action)

    def _maybe_offer_passage_build(self, explicit=False):
        self.actions.append(("build", explicit))

    def _corner_login_clicked(self):
        self.actions.append("login")


def test_no_tip_on_the_first_start_beside_whats_new_or_when_off(cfg, monkeypatch):
    monkeypatch.setattr("shared.session_persistence.get_history", lambda *a: {"regular": [], "composition": []})
    host = _Host()
    assert host._maybe_show_startup_tip() is None                  # the first start
    host.whats_new_bar.show()
    assert host._maybe_show_startup_tip() is None                  # What's New is up
    host.whats_new_bar.hide()
    assert host._maybe_show_startup_tip().key == "go_to"
    assert not host.tip_strip.isHidden()
    cfg[startup_tips.TIPS_OFF_KEY] = True
    assert host._maybe_show_startup_tip() is None


def test_a_new_round_when_every_tip_was_shown_or_is_in_use(cfg, monkeypatch):
    """The owner's case (2026-11-02): 42 starts, every tip shown or its feature
    in use, so no tip at start, and Next tip closed the strip. A new round now
    starts; Next on a round's last tip goes on to another tip, never the same."""
    monkeypatch.setattr("shared.session_persistence.get_history", lambda *a: {"regular": [], "composition": []})
    # every feature in use but go_to's: go_to and the tips nothing retires are left
    cfg[startup_tips.FEATURES_USED_KEY] = sorted({t.used for t in TIPS if t.used and t.key != "go_to"})
    cfg[startup_tips.TIPS_SEEN_KEY] = [t.key for t in TIPS]
    cfg[startup_tips.TIPS_STARTS_KEY] = 41
    eligible = [t.key for t in TIPS if not t.used or t.key == "go_to"]
    assert eligible[:2] == ["go_to", "flexible_shelfmark"]
    host = _Host()
    assert host._maybe_show_startup_tip().key == "go_to"            # a new round, from the top
    assert cfg[startup_tips.TIPS_SEEN_KEY] == ["go_to"]
    assert not host.tip_strip.btn_next.isHidden()
    for key in eligible[1:]:
        host._tip_next()
        assert host.tip_strip.tip.key == key and not host.tip_strip.isHidden()
    host._tip_next()                                                 # the round's end: on, not closed
    assert host.tip_strip.tip.key == "go_to" and not host.tip_strip.isHidden()
    assert cfg[startup_tips.TIPS_SEEN_KEY] == ["go_to"]
    # the strip's tip is shown last in the new round, not again at once
    assert startup_tips.pick_tip([t.key for t in TIPS], [], "go_to") == (TIPS_BY_KEY["saved_searches"], True)
    # nothing left at all: no tip, and Next is not offered for a tip alone
    only = TIPS_BY_KEY["go_to"]
    assert startup_tips.pick_tip([], [], None, tips=(only,)) == (only, False)
    assert startup_tips.pick_tip(["go_to"], ["go_to"], "go_to", tips=(only,)) == (None, False)
    assert not startup_tips.has_other_tip(only, [], tips=(only,))


def test_show_all_tips_again_includes_the_features_in_use_for_one_round(cfg, monkeypatch):
    """Settings > Show all tips again (owner, 2026-11-02): every tip from the
    first, those for features in use too, the tips on, the first on the strip at
    once; the round after it skips the features in use again."""
    from PyQt6.QtWidgets import QCheckBox

    from desktop import settings_dialogs
    from desktop.settings_dialogs import SettingsDialog

    monkeypatch.setattr("shared.session_persistence.get_history", lambda *a: {"regular": [], "composition": []})
    monkeypatch.setattr(genizah_app.passage_lifecycle, "passage_available", lambda: False)
    for name in ("load_app_config", "save_app_config"):
        monkeypatch.setattr(settings_dialogs, name, getattr(genizah_app, name))
    cfg[startup_tips.FEATURES_USED_KEY] = sorted({t.used for t in TIPS if t.used})
    cfg[startup_tips.TIPS_SEEN_KEY] = [t.key for t in TIPS]
    cfg[startup_tips.TIPS_OFF_KEY] = True
    host = _Host()
    dialog = type("D", (), {"chk_startup_tips": QCheckBox(), "main_win": host,
                            "_CANCEL_DEFAULTS": SettingsDialog._CANCEL_DEFAULTS, "reject": lambda self: None})()
    # Cancel in Settings undoes the button, though the round flag was never saved before
    SettingsDialog.refresh_config_snapshot(dialog)
    SettingsDialog._show_all_tips_again(dialog)
    assert startup_tips.all_round()
    SettingsDialog._on_cancel(dialog)
    assert not startup_tips.all_round() and startup_tips.tips_off()
    host.tip_strip.tip = None
    SettingsDialog._show_all_tips_again(dialog)
    assert dialog.chk_startup_tips.isChecked() and not startup_tips.tips_off()
    assert host.tip_strip.tip.key == "go_to" and not host.tip_strip.isHidden()   # in use, shown anyway
    for tip in TIPS[1:]:
        host._tip_next()
        assert host.tip_strip.tip.key == tip.key
    host._tip_next()                                   # the round is over: features in use skipped again
    assert host.tip_strip.tip.key == "flexible_shelfmark" and not host.tip_strip.isHidden()
    assert cfg[startup_tips.TIPS_ALL_KEY] is False and cfg[startup_tips.TIPS_SEEN_KEY] == ["flexible_shelfmark"]


def test_invitations_and_show_me(cfg, monkeypatch):
    monkeypatch.setattr("shared.session_persistence.get_history", lambda *a: {"regular": [], "composition": []})
    monkeypatch.setattr(genizah_app.passage_lifecycle, "passage_available", lambda: False)
    host = _Host()
    host.corrections_client = type("C", (), {"current_user": None})()
    letter = TIPS_BY_KEY["letter_level"]
    assert host._tip_action_label(letter) == genizah_app.tr("Build the index")
    host._tip_show_me(letter)
    assert host.actions == [("build", True)]
    host._tip_show_me(TIPS_BY_KEY["community"])
    assert host.actions[-1] == "login"
    host.corrections_client.current_user = object()
    host._tip_show_me(TIPS_BY_KEY["community"])
    assert host.actions[-1] == "tab:community_tab"
    assert host._tip_action_label(TIPS_BY_KEY["section_keys"]) is None   # nothing to point at


def test_features_in_use_are_read_from_what_is_kept(cfg, monkeypatch):
    history = {"regular": [
        {"query": "#שלום# [2] (א/ב)", "search_params": {"mode_index": 2}, "pinned": True},
        {"query": "x", "search_params": {"mode_index": 0, "text_position": 2},
         "pre_search_filters": {"include_mode": True, "domains": ["Bible"],
                                "criteria": {"region": {"values": ["x"]}}}}],
        "composition": [{"query": "t", "search_params": {"comp_method": "passage"}}]}
    monkeypatch.setattr("shared.session_persistence.get_history", lambda *a: history)
    cfg["nav_layout"] = "side"
    host = _Host()
    host.lists_mgr = type("L", (), {"data": {"items": {"a": {"lists": ["default"]}}},
                                     "get_projects": lambda self: [], "get_all_tags": lambda self: ["x"]})()
    found = host._tip_derived_features()
    assert {"responsa_search", "responsa_affixes", "responsa_or_gap", "saved_search", "position",
            "filters", "features", "letter_level", "side_menu", "list_items", "projects"} <= found
    # and they retire their tips
    assert next_tip([], host._tip_features_in_use()).key == "go_to"
    assert next_tip(["go_to"], found).key == "witnesses"     # saved searches and letter-level are in use


def test_every_show_me_and_wire_names_what_the_window_has():
    kinds = {"query", "responsa", "help", "history", "go_to", "pgp_tags", "witness_dialog", "joins_lab",
             "settings", "search_settings", "menu", "recent_list", "tab", "search", "composition", "browse",
             "catalog", "catalog_mode", "title_fuzzy"}
    for tip in TIPS:
        if not tip.show:
            continue
        kind, _, target = tip.show.partition(":")
        assert kind in kinds, tip.key
        if target and kind in ("tab", "search", "responsa", "composition", "browse", "catalog"):
            assert f"self.{target} = " in _SRC, (tip.key, target)
        assert f"elif kind == '{kind}'" in _SRC or kind in GenizahGUI._TIP_TABS, kind
    for name, _signal, _feature, _cond in GenizahGUI._TIP_WIRES:
        assert f"self.{name} = " in _SRC, name


def test_main_window_wiring():
    assert "main_layout.addWidget(self.tip_strip)" in _SRC
    assert "QTimer.singleShot(1500, self._maybe_show_startup_tip)" in _SRC
    assert "self._wire_tip_signals()" in _SRC
    settings = (Path(__file__).resolve().parent.parent / "desktop" / "settings_dialogs.py").read_text(encoding="utf-8")
    assert 'tr("Show a tip when the program starts")' in settings


def test_the_catalogue_tips_only_where_the_catalogue_is_offered(cfg, monkeypatch):
    """The catalogue search's two tips need its file (desktop/catalog_search.py::catalog_offered);
    Title's Fuzzy search has none to need."""
    monkeypatch.setattr("shared.session_persistence.get_history", lambda *a: {"regular": [], "composition": []})
    cfg[startup_tips.TIPS_STARTS_KEY] = 1
    cfg[startup_tips.TIPS_SEEN_KEY] = [t.key for t in TIPS[:5]]
    host = _Host()
    assert host._maybe_show_startup_tip().key == "catalog_search"
    host._catalog_offered = False
    assert [t.key for t in host._tips_offered()] == [
        t.key for t in TIPS if t.key not in ("catalog_search", "catalog_found")]
    cfg[startup_tips.TIPS_SEEN_KEY] = [t.key for t in TIPS[:5]]
    assert host._maybe_show_startup_tip().key == "title_fuzzy"


def test_a_catalogue_or_title_fuzzy_search_in_the_history_retires_its_tip(cfg, monkeypatch):
    from desktop.catalog_search import MODE_INDEX_CATALOG
    history = {"regular": [
        {"query": "x", "search_params": {"mode_index": MODE_INDEX_CATALOG}},
        {"query": "y", "search_params": {"mode_index": 5, "catalog_options": {"near_spellings": True}}},
        {"query": "z", "search_params": {"mode_index": 5, "catalog_options": None}}],
        "composition": []}
    monkeypatch.setattr("shared.session_persistence.get_history", lambda *a: history)
    assert {"catalog_search", "title_fuzzy"} <= _Host()._tip_derived_features()
    assert GenizahGUI._catalog_features_in({"regular": history["regular"][2:]}) == set()
