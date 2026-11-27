# -*- coding: utf-8 -*-
"""The desktop's visible part of the scholarly transcriptions in the search (editions plan
section 3.5, PR 4b): the Transcriptions choice, results grouped by page in the table, the
viewer and Browse opening on the edition with its match marked, lists, exports, Help.

Small stand-ins borrow the real GenizahGUI / ResultDialog methods (as
tests/test_search_hit_rerender.py does); the edition's text goes through the real versioned
cleaner (shared/edition_search_text.py), so the span the search read maps back to the
edition's own characters exactly as on a real build. The rendering of grouped rows in the
results table is in tests/test_exclusion_surfaces.py (its window fixture).
"""
import ast
import os
from types import MethodType, SimpleNamespace

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import pytest  # noqa: E402
from PyQt6.QtWidgets import QApplication, QComboBox, QLabel, QPushButton, QTextBrowser, QWidget  # noqa: E402

import genizah_app  # noqa: E402
import genizah_core  # noqa: E402
from desktop.edition_view import STALE_NOTICE, UNPLACED_NOTICE, EditionTarget  # noqa: E402
from desktop.result_dialog import ResultDialog  # noqa: E402
from genizah_app import GenizahGUI  # noqa: E402
from shared.edition_search_text import SOURCE_FGP, search_text  # noqa: E402
from shared.edition_target import keep_target, occurrence_at, resolve_mark, source_version, target_of  # noqa: E402
from shared.editions_search import span_fingerprint  # noqa: E402
from shared.refinement import RefinementStep  # noqa: E402

pytestmark = pytest.mark.gui  # Qt widgets: the gui bucket

_APP = QApplication.instance() or QApplication([])

SID = "990000000140205171"
PAGE = "IE14_P000001_FL141"
# Three numbered lines: the cleaner strips the editor's line numbers (shared/edition_search_text.py).
ORIGINAL = "01 ברוך אתה\n02 שלום עליכם רבותי\n03 שלום לכם וברכה"
SEARCH = search_text(ORIGINAL, SOURCE_FGP)
SPAN = (SEARCH.text.index("שלום לכם"), SEARCH.text.index("שלום לכם") + len("שלום"))
FP = span_fingerprint(SEARCH.text, SPAN)


@pytest.fixture(autouse=True)
def _english(monkeypatch):
    """The asserted sentences are English: tr() must not read this machine's language."""
    monkeypatch.setattr(genizah_core, "CURRENT_LANG", "en")
    monkeypatch.setattr(genizah_app, "CURRENT_LANG", "en", raising=False)


class _Engine:
    """edition_entry / get_full_text_by_id as the engine holding the editions index answers."""

    def __init__(self, indexed=SEARCH.text):
        self.indexed = indexed

    def edition_entry(self, sys_id, version):
        return {"uid": "ED:fgp:5"} if (sys_id, version) == (SID, "fgp:5") else None

    def get_full_text_by_id(self, uid):
        return self.indexed if uid == "ED:fgp:5" else None


class _Resolver:
    def __init__(self, original=ORIGINAL):
        self.original = original

    def resolve(self, version):
        if version != "fgp:5":
            return None
        return {"original": self.original, "search": search_text(self.original, SOURCE_FGP)}


@pytest.fixture
def resolver(monkeypatch):
    r = _Resolver()
    monkeypatch.setattr("desktop.edition_view.editions_resolver", lambda: r)
    return r


def _edition(located=True, span=SPAN, fp=FP):
    return {"kind": "fgp", "id": "5", "uid": "ED:fgp:5", "version": "fgp:5", "sys_id": SID,
            "located": located, "page_uid": PAGE if located else None, "span": list(span),
            "cross_page": False, "credit": {"en": "Team Five", "he": "צוות"}, "also_ids": [],
            "span_fp": fp}


def _edition_row(located=True, **kw):
    """A result grouped by page whose best match is the FGP edition."""
    header = f"{SID}_ED_fgp_5_{PAGE}" if located else f"{SID}_ED_fgp_5"
    member = {"uid": "ED:fgp:5", "raw_header": header, "display": {"id": SID, "source": "FGP", "img": "1" if located else ""},
              "edition": _edition(located, **kw), "full_text": SEARCH.text, "snippet": "*שלום* לכם",
              "highlight_pattern": "שלום"}
    automatic = {"uid": f"{SID}_{PAGE}", "raw_header": f"{SID}_{PAGE}", "display": {"id": SID, "source": "V0.8", "img": "1"},
                 "edition": None, "full_text": "שלום לכם", "snippet": "*שלום*"}
    row = dict(member)
    row.update(uid=PAGE if located else "ED:fgp:5", members=[member, automatic] if located else [member],
               sources=["fgp_edition", "automatic"] if located else ["fgp_edition"], edition=member["edition"],
               edition_match=True, page_identified=located)
    return row


# --- shared/edition_target.py ------------------------------------------------------------

def test_source_version_reads_sources_and_chooser_items():
    assert source_version({"id": 7, "uid": "fgp:7", "is_fgp": True}) == "fgp:7"
    assert source_version({"id": 9, "source_scholar": "Goitein"}) == "pgp:9"
    assert source_version({"source": "fgp_edition", "source_id": 7, "uid": "fgp:7"}) == "fgp:7"
    assert source_version({"source": "pgp_edition", "source_id": 9}) == "pgp:9"
    assert source_version({"source": "original"}) is None


def test_keep_target_keeps_the_linked_edition_the_page_filter_left_out():
    every = [{"id": 1, "source_scholar": "A"}, {"id": 2, "source_scholar": "B"}]
    assert keep_target(every, [every[0]], "pgp:2") == every
    assert keep_target(every, [every[0]], None) == [every[0]]
    assert keep_target(every, every, "pgp:2") == every


def test_resolve_mark_maps_the_span_to_the_editions_own_characters():
    found = resolve_mark(_Engine(), _Resolver(), SID, "fgp:5", SPAN, FP)
    assert found == {"stale": False, "mark": "שלום", "occurrence": 1}     # the second one
    where = occurrence_at(ORIGINAL, found["mark"], found["occurrence"])
    assert ORIGINAL[where[0]:].startswith("שלום לכם")


@pytest.mark.parametrize("engine, resolver_, fp", [
    (_Engine(indexed="another text"), _Resolver(), FP),              # rebuilt since the search
    (_Engine(), _Resolver(), "0123456789ab"),                          # the span holds other words
    (_Engine(), _Resolver(original=None), FP),                         # gone from the sidecar
])
def test_resolve_mark_is_stale_when_the_edition_changed(engine, resolver_, fp):
    if resolver_.original is None:
        resolver_.resolve = lambda version: None
    assert resolve_mark(engine, resolver_, SID, "fgp:5", SPAN, fp)["stale"] is True
    assert resolve_mark(_Engine(), _Resolver(), "990000000999205171", "fgp:5", SPAN, FP)["stale"] is True


def test_target_of_names_the_edition_of_a_result():
    t = target_of(_edition_row())
    assert t == {"version": "fgp:5", "span": SPAN, "fp": FP, "sys_id": SID, "located": True}
    assert target_of({"edition": None}) is None and target_of(None) is None


# --- desktop/edition_view.py ---------------------------------------------------------------

def _chooser(*items):
    combo = QComboBox()
    combo.addItem("V0.8", {"source": "original"})
    for label, data in items:
        combo.addItem(label, data)
    return combo


FGP_ITEM = ("FGP edition", {"source": "fgp_edition", "source_id": 5, "uid": "fgp:5", "content": ORIGINAL})
PGP_ITEM = ("PGP edition", {"source": "pgp_edition", "source_id": 3, "content": "פירוש"})
KEY = (SID, None, 1)


def test_a_stale_edition_is_never_forced_among_the_page_sources(resolver):
    """keep_version resolves the hit first: a stale edition is not kept through the page
    filter, where the default choice could then show it (Codex review of #28)."""
    stale = EditionTarget.from_row(_edition_row(fp="0123456789ab"))
    assert stale.keep_version(KEY, _Engine()) == "" and stale.stale
    fresh = EditionTarget.from_row(_edition_row())
    assert fresh.keep_version(KEY, _Engine()) == "fgp:5" and fresh.resolved and not fresh.stale
    assert fresh.keep_version((SID, None, 2), _Engine()) == ""        # another folio than the claimed one


def test_the_first_folio_claims_the_hit(resolver):
    t = EditionTarget.from_row(_edition_row())
    assert t.keep_version(KEY) == "fgp:5"
    assert t.keep_version((SID, None, 2)) == ""          # another folio: browsed as usual
    assert EditionTarget.from_row({"display": {}}) is None


def test_choose_selects_the_edition_without_the_change_signal(resolver):
    t = EditionTarget.from_row(_edition_row())
    combo = _chooser(PGP_ITEM, FGP_ITEM)
    changed = []
    combo.currentIndexChanged.connect(changed.append)
    data = t.choose(combo, KEY, _Engine())
    assert data["uid"] == "fgp:5" and combo.currentIndex() == 2 and changed == []
    assert t.choose(combo, (SID, None, 2), _Engine()) is None


def test_a_stale_hit_chooses_nothing_and_says_so_once(resolver):
    t = EditionTarget.from_row(_edition_row(fp="0123456789ab"))
    assert t.choose(_chooser(FGP_ITEM), KEY, _Engine()) is None
    assert t.notice() == STALE_NOTICE and t.notice() is None
    assert t.keep_version(KEY) == ""


def test_an_unplaced_hit_says_its_page_is_not_known(resolver):
    t = EditionTarget.from_row(_edition_row(located=False))
    assert t.choose(_chooser(FGP_ITEM), KEY, _Engine()) is not None
    assert t.notice() == UNPLACED_NOTICE and t.notice() is None


def test_marked_marks_that_occurrence_on_each_line(resolver):
    t = EditionTarget.from_row(_edition_row())
    t.choose(_chooser(FGP_ITEM), KEY, _Engine())
    shown = FGP_ITEM[1]
    assert t.marked(ORIGINAL, shown, KEY) == "01 ברוך אתה\n02 שלום עליכם רבותי\n03 *שלום* לכם וברכה"
    assert t.marked(ORIGINAL, PGP_ITEM[1], KEY) is None          # another version shows
    assert t.marked(ORIGINAL, shown, (SID, None, 2)) is None      # another folio
    t.mark, t.occurrence = "רבותי\n03 שלום", 0                    # a match across a line break
    assert t.marked(ORIGINAL, shown, KEY) == "01 ברוך אתה\n02 שלום עליכם *רבותי*\n*03 שלום* לכם וברכה"


# --- the result viewer -------------------------------------------------------------------

class _Dialog:
    """The ResultDialog methods that choose and show a version, on stand-in widgets."""

    def __init__(self, row, parent):
        self.text_ms = QTextBrowser()
        self.rd_version_combo = QComboBox()
        self.data = row
        self._app = parent
        self.searcher = _Engine()
        self.current_sys_id, self.current_volume_ie, self.current_p_num = KEY
        self.find_ms_input = SimpleNamespace(text=lambda: "")
        self._rd_edition_target = EditionTarget.from_row(row)
        self._rd_update_calls = []
        for name in ("_on_rd_pgp_loaded", "_rd_display_pgp_text", "_mark_search_hits", "_htmlify",
                     "_refresh_find_highlights", "_rd_page_key", "_rd_show_edition_notice"):
            setattr(self, name, MethodType(getattr(ResultDialog, name), self))

    def _update_rd_pgp_button(self, doc):
        pass


class _Parent:
    def __init__(self, *items):
        self.items = items
        self.auto_selected = []

    def _populate_pgp_combo(self, combo, sources, pgp_doc, **kw):
        combo.clear()
        combo.addItem("V0.8", {"source": "original"})
        for label, data in self.items:
            combo.addItem(label, data)
        return True

    def _auto_select_pgp_edition(self, combo, **kw):
        self.auto_selected.append(kw)
        combo.setCurrentIndex(1)
        return combo.itemData(1)


def _red_words(widget):
    """The text of the red (search-hit) runs in *widget*."""
    import re
    html = widget.toHtml()
    return re.findall(r'color:#ff0000[^>]*>([^<]+)<', html)


def test_the_viewer_opens_on_the_edition_with_its_match_marked(resolver, monkeypatch):
    tips = []
    monkeypatch.setattr("desktop.result_dialog.QToolTip.showText", lambda *a: tips.append(a))
    parent = _Parent(PGP_ITEM, FGP_ITEM)
    d = _Dialog(_edition_row(), parent)
    d._on_rd_pgp_loaded(SID, [{"id": 1}], {})
    assert parent.auto_selected == []                     # not the default choice
    assert d.rd_version_combo.currentData()["uid"] == "fgp:5"
    assert d.text_ms.toPlainText().replace(" ", "\n").endswith("שלום לכם וברכה")
    assert _red_words(d.text_ms) == ["שלום"] and "*" not in d.text_ms.toPlainText()
    assert tips == []                                     # placed and current: nothing to say


def test_the_viewers_exact_mark_needs_no_search_pattern(resolver):
    """A row with no highlight pattern of its own: the exact mark is still a search hit,
    drawn red, never literal asterisks."""
    row = _edition_row()
    row.pop("highlight_pattern")
    d = _Dialog(row, _Parent(FGP_ITEM))
    d._on_rd_pgp_loaded(SID, [{"id": 1}], {})
    assert _red_words(d.text_ms) == ["שלום"] and "*" not in d.text_ms.toPlainText()


def test_the_viewer_opens_an_unplaced_hit_on_its_edition_and_says_its_page_is_not_known(resolver, monkeypatch):
    """A hit whose page could not be identified (a short excerpt of a long manuscript, say):
    the viewer shows the edition that matched, its match marked, with the page it opens on,
    never that page's automatic text, and tells the reader why the page may not be its own."""
    tips = []
    monkeypatch.setattr("desktop.result_dialog.QToolTip.showText", lambda *a: tips.append(a[1]))
    parent = _Parent(PGP_ITEM, FGP_ITEM)
    d = _Dialog(_edition_row(located=False), parent)
    d._on_rd_pgp_loaded(SID, [{"id": 1}], {})
    assert parent.auto_selected == []                     # not the default choice
    assert d.rd_version_combo.currentData()["uid"] == "fgp:5"
    assert _red_words(d.text_ms) == ["שלום"]
    assert tips == [UNPLACED_NOTICE]


def test_a_stale_hit_in_the_viewer_falls_back_to_the_default_choice_and_says_why(resolver, monkeypatch):
    tips = []
    monkeypatch.setattr("desktop.result_dialog.QToolTip.showText", lambda *a: tips.append(a[1]))
    parent = _Parent(PGP_ITEM, FGP_ITEM)
    d = _Dialog(_edition_row(fp="0123456789ab"), parent)
    d._on_rd_pgp_loaded(SID, [{"id": 1}], {})
    assert len(parent.auto_selected) == 1
    assert tips == [STALE_NOTICE]


def _aligned(row, folios):
    """The dialog's row after showing each of *folios* ((sys_id, page uid, p_num) in turn)
    through ResultDialog._rd_align_data_with_page, as load_page calls it."""
    target = EditionTarget.from_row(row)
    d = SimpleNamespace(data=row, current_volume_ie=None, _rd_edition_target=target,
                        _rd_edition_row=row if target is not None else None,
                        _EDITION_ROW_KEYS=ResultDialog._EDITION_ROW_KEYS)
    for name in ("_rd_align_data_with_page", "_rd_page_key"):
        setattr(d, name, MethodType(getattr(ResultDialog, name), d))
    for sys_id, page_uid, p_num in folios:
        d.current_sys_id, d.current_p_num = sys_id, p_num
        d._rd_align_data_with_page({"full_header": f"{sys_id}_{page_uid}", "uid": page_uid, "text": f"text of {page_uid}"})
    return d.data


def test_the_viewer_keeps_an_edition_rows_identity_on_its_folio():
    """load_page aligns the dialog's row with the folio shown; a hit in a scholarly
    transcription keeps the edition's identity while its own folio shows."""
    row = _edition_row()
    kept = _aligned(row, [(SID, PAGE, 1)])
    assert kept is row and row["uid"] == PAGE and row["raw_header"].startswith(f"{SID}_ED_fgp_5")
    assert row["full_text"] == SEARCH.text


def test_after_leaving_the_hits_folio_the_viewer_follows_the_folio_shown_on_a_copy():
    """Navigation past the hit's folio -- to the next page, or across to another manuscript --
    makes the dialog's row the folio shown (Browse, lists and Composition read it), on a copy:
    the results table's row keeps the edition's identity (Codex review of #28)."""
    row = _edition_row()
    other = "990000000150205171"
    data = _aligned(row, [(SID, PAGE, 1), (SID, "IE14_P000002_FL142", 2), (other, "IE15_P000001_FL151", 1)])
    assert data is not row
    assert data["uid"] == "IE15_P000001_FL151" and data["display"]["id"] == other
    assert data["raw_header"] == f"{other}_IE15_P000001_FL151" and data["full_text"] == "text of IE15_P000001_FL151"
    assert "edition" not in data and "members" not in data
    assert row["edition"]["version"] == "fgp:5" and row["uid"] == PAGE and row["display"]["id"] == SID
    plain = {"uid": "a", "raw_header": "h", "full_text": "t", "display": {"id": SID}}
    assert _aligned(plain, [(SID, PAGE, 3)]) is plain and plain["uid"] == PAGE   # any other row: as before


def test_coming_back_to_the_hits_folio_puts_the_edition_row_back():
    """The chooser shows the edition again on the hit's folio: the actions must act on it
    again too (Codex review of #28)."""
    row = _edition_row()
    back = _aligned(row, [(SID, PAGE, 1), (SID, "IE14_P000002_FL142", 2), (SID, PAGE, 1)])
    assert back is row and back["edition"]["version"] == "fgp:5"


def test_load_page_aligns_through_the_helper_and_keeps_the_edition_in_the_sources():
    import desktop.result_dialog as rd
    src = ast.unparse(next(f for f in ast.walk(ast.parse(open(rd.__file__, encoding="utf-8").read()))
                           if isinstance(f, ast.FunctionDef) and f.name == "load_page"))
    assert "self._rd_align_data_with_page(page_data)" in src
    assert "self.data['uid'] = " not in src
    assert "keep_version=_rd_target.keep_version(self._rd_page_key(), self.searcher) if _rd_target else ''" in src


# --- Browse ------------------------------------------------------------------------------

class _Browse:
    def __init__(self, row, *items):
        self.browse_text = QTextBrowser()
        self.browse_version_combo = QComboBox()
        # browse_load() clears the search context open_result_in_browse set (Codex review of
        # #28): the edition's exact mark must stand on its own.
        self.browse_highlight_pattern = None
        self.browse_highlight_data = []
        self.browse_find_input = SimpleNamespace(text=lambda: "")
        self.current_browse_sid, self.current_browse_volume_ie, self.current_browse_p = KEY
        self.searcher = _Engine()
        self.status_label = QLabel()
        self.txt_b_extended_info = QTextBrowser()
        self.btn_b_ext_info = QPushButton()
        self._browse_enriched_html = ""
        self.browse_original_page_text = ""
        self._browse_edition_target = EditionTarget.from_row(row)
        self.parent = _Parent(*items)
        for name in ("_on_browse_pgp_loaded", "_browse_display_pgp_text", "_browse_mark_search_hits",
                     "_browse_markers_are_ours", "_browse_page_key", "_browse_keep_version"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))

    def _update_browse_pgp_button(self, doc):
        pass

    def _build_pgp_extended_info_html(self, doc, **kw):
        return ""

    def _update_joins_dropdown(self):
        pass

    def _populate_pgp_combo(self, *a, **kw):
        return self.parent._populate_pgp_combo(*a, **kw)

    def _auto_select_pgp_edition(self, *a, **kw):
        return self.parent._auto_select_pgp_edition(*a, **kw)


def test_browse_opens_on_the_edition_with_its_match_marked(resolver):
    b = _Browse(_edition_row(), PGP_ITEM, FGP_ITEM)
    assert b._browse_keep_version() == "fgp:5"
    b._on_browse_pgp_loaded(SID, [{"id": 1}], {})
    assert b.parent.auto_selected == []
    assert b.browse_version_combo.currentData()["uid"] == "fgp:5"
    assert _red_words(b.browse_text) == ["שלום"] and "*" not in b.browse_text.toPlainText()


def test_browse_says_when_an_unplaced_hit_opens_on_the_first_page(resolver):
    b = _Browse(_edition_row(located=False), FGP_ITEM)
    b._on_browse_pgp_loaded(SID, [{"id": 1}], {})
    assert b.status_label.text() == UNPLACED_NOTICE


# --- Find Parallels sends the text on screen ------------------------------------------------

def test_parallels_text_is_the_version_on_screen_but_never_a_translation():
    from shared.edition_target import parallels_text
    assert parallels_text("automatic", "edition", {"source": "fgp_edition"}) == "edition"
    assert parallels_text("automatic", "edition", {"source": "pgp"}) == "edition"          # the website's
    assert parallels_text("automatic", "fixed", {"source": "correction"}) == "fixed"
    assert parallels_text("automatic", None, None) == "automatic"                          # nothing chosen
    assert parallels_text("automatic", "  ", {"source": "fgp_edition"}) == "automatic"
    for translation in ({"source": "pgp_translation"}, {"source": "fgp_translation"},
                        {"source": "translation"}, {"source": "fgp", "is_translation": True}):
        assert parallels_text("automatic", "English", translation) == "automatic", translation
    assert parallels_text(None, None) == ""


def test_the_parallels_link_cuts_the_text_before_encoding_it():
    """A whole edition or manuscript made a link the server refused (Codex review of #33): the
    website's link carries the first PARALLELS_URL_TEXT_LIMIT characters, cut before encoding."""
    from urllib.parse import parse_qs, urlparse
    from shared.edition_target import PARALLELS_URL_TEXT_LIMIT, parallels_url
    long_text = "שלום עליכם רבותי " * 400                    # 6,800 characters
    url = parallels_url(long_text)
    assert url.startswith("/parallels?text=")
    sent = parse_qs(urlparse(url).query)["text"][0]
    assert sent == long_text[:PARALLELS_URL_TEXT_LIMIT] and PARALLELS_URL_TEXT_LIMIT == 2000
    assert len(url) < 16_000                                  # under the usual request-line limits
    assert parse_qs(urlparse(parallels_url("קצר")).query)["text"] == ["קצר"]
    assert parallels_url(None) == "/parallels?text="


def _sent_to_composition(holder):
    sent = []
    holder.send_result_to_composition = lambda res, source_text=None, title=None: sent.append(source_text)
    return sent


def test_the_viewers_find_parallels_sends_the_edition_on_screen(resolver):
    parent = _Parent(PGP_ITEM, FGP_ITEM)
    d = _Dialog(_edition_row(), parent)
    d._on_rd_pgp_loaded(SID, [{"id": 1}], {})
    assert d.rd_version_combo.currentData()["uid"] == "fgp:5"
    d.current_page_text = "the automatic transcription"
    d.lbl_title = QLabel("A title")
    d.close = lambda: None
    sent = _sent_to_composition(parent)
    for name in ("search_for_parallels", "_rd_text_for_parallels"):
        setattr(d, name, MethodType(getattr(ResultDialog, name), d))
    d.search_for_parallels()
    d._rd_edit_mode = True                                # editing the edition shown
    d.text_ms.setPlainText("my edit")
    d.search_for_parallels()
    d._rd_edit_mode = False
    d.rd_version_combo.setCurrentIndex(0)                 # back to V0.8
    d.search_for_parallels()
    assert sent == [ORIGINAL, "my edit", "the automatic transcription"]


def test_browses_find_parallels_sends_the_edition_on_screen(resolver):
    b = _Browse(_edition_row(), PGP_ITEM, FGP_ITEM)
    b._on_browse_pgp_loaded(SID, [{"id": 1}], {})
    assert b.browse_version_combo.currentData()["uid"] == "fgp:5"
    b.meta_mgr = SimpleNamespace(nli_cache={})
    b.btn_b_all = SimpleNamespace(isChecked=lambda: False)
    b.current_browse_internal_idx = 0
    b.searcher.get_browse_page = lambda sid, absolute_index=None, volume_ie=None: {"text": "the automatic transcription"}
    sent = _sent_to_composition(b)
    for name in ("browse_search_parallels", "_browse_text_for_parallels"):
        setattr(b, name, MethodType(getattr(GenizahGUI, name), b))
    b.browse_search_parallels()
    b.browse_edit_mode = True                             # editing the edition shown
    b.browse_text.setPlainText("my edit")
    b.browse_search_parallels()
    b.browse_edit_mode = False
    b.browse_version_combo.setCurrentIndex(0)             # back to V0.8
    b.browse_search_parallels()
    assert sent == [ORIGINAL, "my edit", "the automatic transcription"]


def test_open_result_in_browse_takes_the_rows_edition():
    src = ast.unparse(next(f for f in ast.walk(ast.parse(open(genizah_app.__file__, encoding="utf-8").read()))
                           if isinstance(f, ast.FunctionDef) and f.name == "open_result_in_browse"))
    assert "self._browse_edition_target = EditionTarget.from_row(res) if isinstance(res, dict) else None" in src
    assert src.index("_browse_edition_target =") < src.rindex("self.browse_load()")
    # Both of Browse's source workers keep the linked edition through the page filter.
    with open(genizah_app.__file__, encoding="utf-8") as f:
        assert f.read().count("keep_version=self._browse_keep_version())") == 2
    assert "target.keep_version(self._browse_page_key(), self.searcher)" in _function_source("_browse_keep_version")


def test_the_page_sources_keep_the_linked_edition(monkeypatch):
    """PGPSourceWorker keep_version: the page filter leaves the edition out; it is kept."""
    import shared.document_service as ds
    import shared.fgp_service as fs
    from desktop.gui_threads import PGPSourceWorker
    fgp = {"id": 5, "uid": "fgp:5", "is_fgp": True, "content": ORIGINAL}
    monkeypatch.setattr(ds, "get_all_sources_for_fragment", lambda sid: [])
    monkeypatch.setattr(ds, "get_document_for_fragment", lambda sid, p: None)
    monkeypatch.setattr(fs, "get_fgp_sources_for_fragment", lambda sid: [fgp])
    monkeypatch.setattr(fs, "filter_sources_for_page", lambda sources, *a, **kw: [])
    for keep, expected in (("fgp:5", [fgp]), ("", [])):
        w = PGPSourceWorker(SID, 1, keep_version=keep)
        got = []
        w.finished_signal.connect(lambda sid, sources, doc: got.append(sources))
        w.run()
        assert got == [expected]


# --- the Transcriptions choice ------------------------------------------------------------

class _Search:
    """The window's choice methods on stand-in widgets."""

    def __init__(self):
        self._search_corpus_scope = "genizah"
        self._search_sources_choice = "all"
        self.mode_combo = QComboBox()
        self.mode_combo.addItems([str(i) for i in range(8)])
        self.btn_lab_mode_toggle = QPushButton()
        self.btn_lab_mode_toggle.setCheckable(True)
        self._host = QWidget()                      # never shown: its children only change state
        self.sources_label = QLabel(self._host)
        self.sources_combo = QComboBox(self._host)
        for key, label in GenizahGUI._SOURCE_CHOICES:
            self.sources_combo.addItem(label, key)
        self.saves = 0
        self._SOURCE_CHOICES = GenizahGUI._SOURCE_CHOICES
        self._NO_SOURCES_MODE_INDEXES = GenizahGUI._NO_SOURCES_MODE_INDEXES
        for name in ("_sources_apply", "_search_sources", "_update_sources_visibility", "_on_sources_changed",
                     "_set_sources_choice", "_apply_run_params"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))
        self.sources_combo.currentIndexChanged.connect(self._on_sources_changed)

    def _save_session(self):
        self.saves += 1


def test_the_choice_defaults_to_all_and_is_remembered():
    s = _Search()
    assert s._search_sources() == "all"
    s.sources_combo.setCurrentIndex(s.sources_combo.findData("editions"))
    assert s._search_sources() == "editions" and s._search_sources_choice == "editions" and s.saves == 1
    s._set_sources_choice("automatic")                      # a saved choice: not saved again
    assert s.sources_combo.currentData() == "automatic" and s.saves == 1
    s._set_sources_choice("bogus")                          # an older or damaged file
    assert s._search_sources() == "automatic"


@pytest.mark.parametrize("change", ["local", "lab", "title", "shelfmark", "pgp tags"])
def test_the_choice_applies_only_where_the_transcriptions_are_read(change):
    s = _Search()
    s._update_sources_visibility()
    assert not s.sources_combo.isHidden()
    if change == "local":
        s._search_corpus_scope = "local"
    elif change == "lab":
        s.btn_lab_mode_toggle.setChecked(True)
    else:
        s.mode_combo.setCurrentIndex({"title": 5, "shelfmark": 6, "pgp tags": 7}[change])
    s._update_sources_visibility()
    assert s._search_sources() is None
    assert s.sources_combo.isHidden() and s.sources_label.isHidden()


def test_a_step_records_the_transcriptions_it_read():
    s = _Search()
    s._last_search_params = dict(query="q", gap=0, exclude_words=[], text_position=None, responsa_options=None,
                                 corpus_scope="genizah", variant_settings=None, sources="editions")
    step = RefinementStep(query="q", mode="exact")
    s._apply_run_params(step)
    assert step.sources == "editions"
    restored = RefinementStep.from_dict(step.to_dict())
    assert restored.sources == "editions"


def _function_source(name):
    tree = ast.parse(open(genizah_app.__file__, encoding="utf-8").read())
    return ast.unparse(next(f for f in ast.walk(tree) if isinstance(f, ast.FunctionDef) and f.name == name))


def test_the_choice_is_laid_out_saved_and_restored():
    tab = _function_source("create_search_tab")
    assert tab.index("row1.addWidget(self.corpus_scope_combo)") < tab.index("row1.addWidget(self.sources_combo)")
    assert "self.sources_label = QLabel(tr('Transcriptions') + ':')" in tab
    start = _function_source("start_search")
    assert "_run_sources = self._search_sources()" in start
    # The thread, and the step's params (each also carries the catalogue switches since PR 3).
    assert start.count("sources=_run_sources, catalog_options=_run_catalog") == 2
    assert "'search_sources': getattr(self, '_search_sources_choice', 'all')" in _function_source("_save_session")
    assert "_restore_sources_choice(self, reg.get('search_sources', 'all'))" in \
        _function_source("_apply_persistent_session_preferences")
    history = _function_source("_add_regular_search_to_history")
    assert "'sources': getattr(self, '_search_sources_choice', 'all')" in history
    assert "_restore_sources_choice(self, params.get('sources'))" in _function_source("_restore_regular_search_from_state")
    for name in ("_on_corpus_scope_changed", "_on_search_mode_changed", "update_lab_ui_state"):
        assert "self._update_sources_visibility()" in _function_source(name), name


def test_the_choice_strings_are_translated():
    from shared.genizah_translations import TRANSLATIONS
    for key in ("Transcriptions", "All", "Scholarly", "Automatic", "Matched in", "Automatic transcription",
                "FGP edition", "PGP edition", "Page not identified", "in scholarly transcriptions",
                "Search the automatic transcriptions, the scholarly transcriptions (FGP and PGP), or both",
                STALE_NOTICE, UNPLACED_NOTICE):
        assert TRANSLATIONS.get(key), key
        assert "תעתוק" not in TRANSLATIONS[key]


# --- results, lists, exports, the session ---------------------------------------------------

def test_the_src_cell_names_every_text_that_matched():
    text, tip = genizah_app._result_sources_cell(_edition_row())
    assert text == "FGP, V0.8"
    assert tip == "Matched in: FGP edition (Team Five), Automatic transcription"
    assert genizah_app._result_sources_cell({"display": {"source": "V0.8"}}) is None


def test_a_folded_result_names_the_other_parts_of_its_document():
    row = _edition_row()
    row["also_in"] = [{"sys_id": "990B", "shelfmark": "T-S 16.223"}, {"sys_id": "990C", "shelfmark": "Unknown"}]
    meta = SimpleNamespace(get_meta_for_id=lambda sid: ("T-S 12.634", "") if sid == "990C" else ("Unknown", ""))
    assert genizah_app._also_in_text(row, meta) == "Other parts of this document: T-S 16.223, T-S 12.634"
    assert genizah_app._also_in_text(row) == "Other parts of this document: T-S 16.223, 990C"
    _text, tip = genizah_app._result_sources_cell(row)
    assert tip.endswith("\nOther parts of this document: T-S 16.223, 990C")
    assert genizah_app._export_source_label(row).endswith("; Other parts of this document: T-S 16.223, 990C")
    assert genizah_app._also_in_text(_edition_row()) == ""


def test_the_results_table_shows_a_cells_own_tooltip(monkeypatch):
    """The results table's event filter answers every tooltip request itself: a cell's own
    tooltip (Shelfmark, Src, Img) has to be served there, or it is never shown."""
    from PyQt6.QtCore import QEvent
    from PyQt6.QtGui import QHelpEvent
    from PyQt6.QtWidgets import QTableWidget, QTableWidgetItem
    table = QTableWidget(1, 4)
    table.resize(900, 200)
    table.setColumnWidth(0, 400)       # the shelfmark is not cut: only its own tooltip shows
    # Column 0 the shelfmark, 2 the image number; the checkbox, actions and PGP columns elsewhere.
    gui = SimpleNamespace(results_table=table, COL_CHECKBOX=10, COL_ACTIONS=11, COL_IMG=2, COL_PGP=12,
                          COL_SHELF=0)
    shelf = QTableWidgetItem("T-S 24.18")
    shelf.setToolTip("Other parts of this document: T-S 16.223")
    table.setItem(0, 0, shelf)
    img = QTableWidgetItem("")
    img.setToolTip("Page not identified")
    table.setItem(0, 2, img)
    tips = []
    monkeypatch.setattr(genizah_app.QToolTip, "showText", lambda pos, text, *a: tips.append(text))
    monkeypatch.setattr(genizah_app.QToolTip, "hideText", lambda *a: tips.append(None))
    for col in (0, 2):
        center = table.visualRect(table.model().index(0, col)).center()
        event = QHelpEvent(QEvent.Type.ToolTip, center, table.viewport().mapToGlobal(center))
        assert GenizahGUI.eventFilter(gui, table.viewport(), event) is True
    assert tips == ["Other parts of this document: T-S 16.223", "Page not identified"]
    # A cut-off cell with a tooltip of its own: the tooltip alone (a compact title's full title
    # is not said twice); a cut-off shelfmark keeps its text above the other fragments.
    table.setColumnWidth(0, 30)
    table.setColumnWidth(1, 30)
    title = QTableWidgetItem("A compact title cut off")
    title.setToolTip("A compact title cut off, in full")
    table.setItem(0, 1, title)
    tips.clear()
    for col in (1, 0):
        center = table.visualRect(table.model().index(0, col)).center()
        GenizahGUI.eventFilter(gui, table.viewport(), QHelpEvent(QEvent.Type.ToolTip, center,
                                                                 table.viewport().mapToGlobal(center)))
    assert tips == ["A compact title cut off, in full", "T-S 24.18\nOther parts of this document: T-S 16.223"]


def test_the_count_line_says_how_many_matched_in_a_scholarly_transcription():
    plain = {"uid": "x", "members": [{"edition": None}], "edition_match": False}
    assert genizah_app._editions_match_note([_edition_row(), plain]) == " · 1 in scholarly transcriptions"
    assert genizah_app._editions_match_note([plain]) == ""
    assert genizah_app._editions_match_note([{"uid": "x"}]) == ""       # today's search


class _Lists:
    def __init__(self):
        from shared.metadata_manager import MetadataManager
        self.meta_mgr = SimpleNamespace(
            parse_full_id_components=lambda h: MetadataManager.parse_full_id_components(MetadataManager, h))
        self.last_search_query = "שלום"
        self.lists_mgr = object()
        self.results_table = None
        self.added = []
        for name in ("search_add_row_to_list", "_extract_fl_id", "_normalize_fl_id"):
            setattr(self, name, MethodType(getattr(GenizahGUI, name), self))

    def show_add_to_list_menu(self, items, source=None, anchor_widget=None):
        self.added.append(items)


def test_lists_add_a_located_hits_page_and_an_unplaced_hits_manuscript():
    lists = _Lists()
    lists.search_add_row_to_list(_edition_row())
    lists.search_add_row_to_list(_edition_row(located=False))
    (page,), (manuscript,) = lists.added
    assert page["sys_id"] == SID and page["img"] == "1" and page["fl_id"]
    assert manuscript["sys_id"] == SID and not manuscript["img"] and not manuscript["fl_id"]


def test_exports_name_every_text_that_matched():
    row = _edition_row()
    assert genizah_app._export_source_label(row) == "FGP edition (Team Five), Automatic transcription"
    assert genizah_app._export_source_label({"display": {"source": "V0.7"}}) == "V0.7"
    data, _local = genizah_app._build_export_data_row(row)
    assert data[5] == "FGP edition (Team Five), Automatic transcription"
    docx_row = genizah_app._export_row_with_sources(row)
    assert docx_row["display"]["source"] == data[5] and row["display"]["source"] == "FGP"
    plain = {"display": {"source": "V0.8"}}
    assert genizah_app._export_row_with_sources(plain) is plain


def test_every_export_writer_gets_the_matched_texts():
    """The call sites: CSV / TXT / DOCX rows and the DOCX block name every text that matched,
    and the search information says which transcriptions were read."""
    src = _function_source("_build_search_export")       # the builder export_results runs (GS-9)
    assert "str(d.get('img', '')), _export_source_label(r), snippet" in src
    assert "write_docx_result_block(doc, _export_row_with_sources(r)" in src
    assert "search_info_lines.append(tr('Transcriptions') + f': {job.transcriptions_label}')" in src
    assert "transcriptions_label=tr(names[sources]) if sources in names else ''" in _function_source("_search_export_job")


def test_the_xlsx_source_column_names_every_text_that_matched(monkeypatch):
    from openpyxl import load_workbook
    import io
    from shared import export_dossier
    for name in ("pgp_subset_for_sys_id", "nli_subset_for_sys_id", "catalog_summary_for_sys_id"):
        monkeypatch.setattr(export_dossier, name, lambda s, **kw: None)
    monkeypatch.setattr(export_dossier, "bibliography_for_sys_id", lambda s, **kw: [])
    headers = ["System ID", "Library", "Shelfmark", "Title", "Image/Page", "Source", "Snippet", "Full Text",
               "Has PGP", "Is Printed", "Domains", "Image URL"]
    content = genizah_app._build_search_results_xlsx_bytes(
        results=[_edition_row()], headers_main=headers,
        meta_resolver=lambda s: {"shelfmark": "T-S 1", "title": "", "library_name": "CUL"},
        sanitize_fn=lambda t: t)
    sheet = load_workbook(io.BytesIO(content)).worksheets[0]
    rows = [[c.value for c in r] for r in sheet.iter_rows()]
    header_row = next(i for i, r in enumerate(rows) if "Source" in r)
    assert rows[header_row + 1][rows[header_row].index("Source")] == "FGP edition (Team Five), Automatic transcription"


def test_the_session_keeps_members_light():
    row = _edition_row()
    (light,) = genizah_app._session_rows([row])
    assert light["full_text"] == row["full_text"] and light["edition"] == row["edition"]
    assert [set(m) for m in light["members"]] == [{"uid", "raw_header", "display", "edition"}] * 2
    assert genizah_app._result_sources_cell(light) == genizah_app._result_sources_cell(row)
    plain = {"uid": "x", "full_text": "t"}
    assert genizah_app._session_rows([plain]) == [plain]


def test_session_json_gets_the_light_rows():
    assert "'results': _session_rows(getattr(self, 'last_results', [])[:5000])" in _function_source("_save_session")


# --- Help ----------------------------------------------------------------------------------

def test_help_describes_the_scholarly_transcriptions_in_both_languages():
    with open(os.path.join(os.path.dirname(genizah_app.__file__), "Help.html"), encoding="utf-8") as f:
        html = f.read()
    en = html[html.index("<!-- START_LANG_EN -->"):html.index("<!-- END_LANG_EN -->")]
    he = html[html.index("<!-- START_LANG_HE -->"):html.index("<!-- END_LANG_HE -->")]
    assert '<h3 id="scholarly-transcriptions">Scholarly transcriptions in the search</h3>' in en
    assert '<h3 id="scholarly-transcriptions">תעתיקים מדעיים בחיפוש</h3>' in he
    for block, words in ((en, ("<b>Transcriptions</b>", "<b>All</b>", "<b>Scholarly</b>", "<b>Automatic</b>")),
                         (he, ("<b>תעתיקים</b>", "<b>הכל</b>", "<b>מדעיים</b>", "<b>אוטומטיים</b>"))):
        for word in words:
            assert word in block, word
    assert "תעתוק" not in html
