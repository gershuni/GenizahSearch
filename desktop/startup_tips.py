# -*- coding: utf-8 -*-
"""Startup tips (owner, 2026-10-25): one tip each time the program starts, in a
thin strip under the top bars, for the features people miss.

The owner wrote and approved the 55 tips (English and Hebrew; the Hebrew is
each English text's TRANSLATIONS entry) and chose: the strongest first, a tip
at every start, the strip rather than a window, and tips that need the
letter-level index or an account shown to everyone as invitations (their
button then builds the index or opens the login).

**bold** in a tip is a name on screen (a button, tab or menu item): Hebrew has
no capitals to mark one.

What is kept, in config.pkl: the tips already shown (each is shown once), the
features already used (a tip whose feature is in use is skipped: see
mark_used and GenizahGUI._tip_features_in_use), the number of starts
(the first start shows no tip), the closes in a row (after
CLOSES_BEFORE_OFF the tips turn themselves off) and the off switch
(Settings, or "Don't show tips").
"""
from __future__ import annotations

import html
import re
from dataclasses import dataclass

from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtWidgets import QFrame, QHBoxLayout, QLabel, QPushButton, QSizePolicy

from genizah_core import load_app_config, save_app_config, tr

TIPS_OFF_KEY = 'tips_off'
TIPS_SEEN_KEY = 'tips_seen'
TIPS_STARTS_KEY = 'tips_starts'
TIPS_CLOSED_KEY = 'tips_closed_in_row'
FEATURES_USED_KEY = 'features_used'
CLOSES_BEFORE_OFF = 5


@dataclass(frozen=True)
class Tip:
    key: str
    text: str                 # English; its TRANSLATIONS entry is the Hebrew
    show: str | None = None   # what Show me does (GenizahGUI._tip_show_me)
    used: str | None = None   # the feature whose use makes the tip unnecessary
    needs: str | None = None  # 'letter_index' / 'account': unmet, the button invites


# The strongest five first (owner), then the rest in the review sheet's order.
TIPS = (
    Tip('go_to',
        "Ctrl+G opens any manuscript from anywhere: type a shelfmark or system number, or pick one of the last ten you opened.",
        show='go_to', used='go_to'),
    Tip('saved_searches',
        "The arrow at the end of the query box reruns a recent search; ☆ keeps it at the top of the list for good.",
        show='history', used='saved_search'),
    Tip('letter_level',
        "**Letter-level search** finds parallels much faster than chunk search, with fewer unrelated results. Its index is built once, from **Settings**.",
        show='composition:comp_method_combo', used='letter_level', needs='letter_index'),
    Tip('witnesses',
        "Have several textual witnesses of the work you are looking for? Add each one in the **Witnesses** window: each copy is searched separately and the results are merged, which finds more than any single copy.",
        show='composition:btn_comp_witnesses', used='witness', needs='letter_index'),
    Tip('responsa_star',
        "In **Responsa** style, * stands for any letters inside a word: כהנ* גדו* finds both כהן גדול and כהנים גדולים.",
        show='responsa', used='responsa_search'),
    Tip('mode_signs',
        "Type a sign followed by a space to pick the search mode: ? שלום for **Variants**, ~ for **Fuzzy**, $ for catalog titles, # for shelfmarks, and R and a space for **Responsa** style.",
        show='query', used='mode_sign'),
    Tip('responsa_affixes',
        "#שלום# finds the word with every prefix and suffix (600 forms), and %שלום finds it with and without ו and י.",
        show='help:responsa', used='responsa_affixes'),
    Tip('tabular_search',
        "Not sure of the **Responsa** syntax? **Tabular Search** builds the query from boxes, with a live preview.",
        show='responsa:btn_query_builder', used='tabular'),
    Tip('position_list',
        "The position list beside the query box (**Anywhere**, **Start of text**, **End of text**, **Line starts**, **Line ends**) limits the search to the start or end of a text or of a line, which helps when hunting joins.",
        show='search:text_position_combo', used='position'),
    Tip('focus_search',
        "**Focus Search** narrows a search before it runs: by subject, author, work, date, or printed versus handwritten.",
        show='search:btn_pre_search_filters', used='filters'),
    Tip('search_within',
        "After a search, **Search within N manuscripts** searches again only in the manuscripts it found.",
        show='search:search_within_btn', used='search_within'),
    Tip('column_filter',
        "Click the filter mark in a column heading to filter the results by text; tick **Exclude matches** to hide the matching results instead.",
        show='search:results_table', used='column_filter'),
    Tip('result_menu',
        "Right-click a result to hide that manuscript, copy its shelfmark, or send a correction or a comment.",
        show='search:results_table', used='result_menu'),
    Tip('promote_results',
        "Tick results and press **Search with these too**: the pages that matched become more witnesses for the next round.",
        show='witness_dialog', used='promote', needs='letter_index'),
    Tip('quoted_texts',
        "If your text contains verses or other quoted sources, you can filter them out with **Filter texts** > **Quoted texts**.",
        show='composition:btn_comp_filter_texts', used='quoted'),
    Tip('quoting_texts',
        "**Filter texts** > **Quoting texts** > **Load from Sefaria**: choose the passage of a work that quotes yours (the Rif on Berakhot, say), and that work's pages move out of the results.",
        show='composition:btn_comp_filter_texts', used='quoting', needs='letter_index'),
    Tip('add_condition',
        "**Filter texts** > **Add condition** shows only pages where another known text also appears: on the same image, on an adjacent one, or anywhere in the manuscript.",
        show='composition:btn_comp_filter_texts', used='condition', needs='letter_index'),
    Tip('exclude_known',
        "Already know some witnesses? List them in **Exclude Manuscripts**: they move to the **Excluded** group and the new finds stand out.",
        show='composition:btn_comp_exclude', used='exclude'),
    Tip('flexible_shelfmark',
        "Shelfmarks needn't be exact: TS NS 13 15 finds T-S NS 13.15.",
        show='browse:browse_shelf_input', used=None),
    Tip('browse_find',
        "**Find** on the Browse tab marks a word as you type it; **View All** puts every page of the manuscript in one scroll.",
        show='browse:browse_find_input', used='browse_find'),
    Tip('parallels',
        "🔍 **Parallels**, on the Browse tab and in the viewer, sends the page you are reading to Composition Search to find other copies of it.",
        show='browse:btn_find_parallels', used='parallels'),
    Tip('reading_desk',
        "The **Reading Desk** shows several manuscripts side by side with their images: 👁️ **Add to View** adds the one on screen, and the 🔗 joins menu opens all joined fragments at once.",
        show='browse:btn_b_add_to_view', used='reading_desk'),
    Tip('cite_page',
        "**Cite this page** copies a ready citation for the manuscript on screen.",
        show='browse:btn_b_cite', used='cite'),
    Tip('open_website',
        "**Open on the website** and **Copy link** lead to the same page on genizahsearch.com, handy for sharing with others.",
        show='browse:btn_b_open_web', used='website'),
    Tip('pgp_tags',
        "Click a PGP tag on the Browse tab (a letter, a contract) to list every manuscript with that topic, or choose **PGP Tags** as the search mode.",
        show='pgp_tags', used='pgp_tags'),
    Tip('browse_identification',
        "**Browse by Identification** lists manuscripts by subject, author, work and date; **Search in these results** then searches only them.",
        show='tab:catalog_browse_tab', used='catalog_tab'),
    Tip('list_star',
        "The ☆ on each search result adds it to a personal list; signed in, your lists sync with genizahsearch.com.",
        show='search:results_table', used='list_items', needs='account'),
    Tip('list_projects',
        "Group your lists into projects, give items tags and notes, and save a list to Excel or Word with **Export List**.",
        show='tab:lists_tab', used='projects'),
    Tip('copy_info',
        "📋 **Copy Info** on a list item copies its details (short, detailed or with a link), ready to paste into an article.",
        show='tab:lists_tab', used='copy_info'),
    Tip('joins_lab',
        "The **Joins Lab** helps you find joins for a given manuscript. Anchor it, and see beside it the results of a search for text you expect on the joining fragment, fragments that look like it, or both together.",
        show='joins_lab', used='joins_lab'),
    Tip('visual_similarity',
        "In the **Joins Lab**, **Visual Similarity** finds fragments that look alike. Note: it is a computer algorithm that covers only about half of the Genizah fragments, so its suggestions add matches but never rule one out.",
        show='joins_lab', used=None),
    Tip('puzzle',
        "Want to join fragments to each other physically? 🧩 **Puzzle** on the Browse tab puts the fragment on a canvas: remove the background, rotate and fit the pieces together, then export the result as one image.",
        show='browse:btn_b_add_to_puzzle', used='puzzle'),
    Tip('community',
        "Ask the community: in the **Community** tab, + shares a discovery or a question about a manuscript, and other researchers can reply.",
        show='tab:community_tab', used=None, needs='account'),
    Tip('my_library',
        "You can search your own files! **My Library** indexes your Word, PDF, text and Excel files, so **Search** and **Composition Search** search them too when you choose **Local** or **ALL**.",
        show='tab:my_library_tab', used='my_library'),
    Tip('help_f1',
        "F1 opens Help at the section you are in; the orange **?** buttons do the same.",
        show='help', used='help'),
    Tip('section_keys',
        "Ctrl+1 to Ctrl+7 jump between the sections, and Ctrl+F puts the cursor in the section's own search box.",
        show=None, used='section_key'),
    Tip('text_size',
        "Ctrl+= and Ctrl+- make the text in the result lists larger or smaller, and Ctrl+0 returns to normal; **A−** and **A+** size the transcription.",
        show='settings', used='text_size'),
    Tip('side_menu',
        "Prefer a menu down the side of the window? ☰ > **Move the menu to the side**.",
        show='menu', used='side_menu'),
    # 'result_keys', not the old 'space_scroll' (owner, 2026-10-30: Space ticks
    # now): a user who read the paging tip is shown this one.
    Tip('result_keys',
        "In the search results, Space ticks a result, Enter opens it, and Ctrl+Enter or Ctrl+B opens it in **Browse by Shelfmark**.",
        show=None, used=None),
    Tip('xlsx_export',
        "The **XLSX** button exports a research workbook: the results, the manuscripts, a bibliography and credits, with a link to each image. Tick rows first to export only those.",
        show='search:btn_exp_xlsx', used='export'),
    Tip('pause_search',
        "Is a search taking a long time? ⏸ pauses it and ▶ resumes it from the same point; **Stop** keeps everything found so far.",
        show='search:btn_search', used='pause'),
    Tip('custom_pairs',
        "You can add your own variant pairs, such as מ=נו, in the ⚙ **Search Settings** window.",
        show='search_settings', used='custom_pairs'),
    Tip('flex_spacing',
        "The automatic transcription sometimes splits or joins words. To get past this, choose **Responsa** style and tick **Flex Spacing**.",
        show='responsa:chk_responsa_flex', used='flex'),
    Tip('judeo_arabic',
        "Searching Judeo-Arabic? In **Responsa** style, tick **Judeo-Arabic** to add the article אל- to every word.",
        show='responsa:chk_responsa_ja', used='judeo_arabic'),
    Tip('responsa_or_gap',
        "In **Responsa** style, (שלום/שלומות) finds either word, and שלום [3] עולם allows up to three words between the two.",
        show='help:responsa', used='responsa_or_gap'),
    Tip('bidirectional',
        "Not sure which word comes first? In **Responsa** style, tick **Bidirectional** to find the words in either order.",
        show='responsa:chk_bidirectional', used='bidirectional'),
    Tip('cross_paragraph',
        "If you are looking for a work in several paragraphs, such as the Amidah or a piyyut, try **Cross-paragraph only**: the program looks only for phrases that link the paragraphs, which filters out quotations of a single paragraph.",
        show='composition:boundary_mode_combo', used='cross_paragraph'),
    Tip('witness_files',
        "Many textual witnesses in one file? In the **Witnesses** window, tick **Split each file on blank lines** and press **Load from files** to add them all at once.",
        show='witness_dialog', used='witness_files', needs='letter_index'),
    Tip('next_manuscript',
        "In the viewer opened from Composition Search, **Next Manuscript ▶▶** skips the remaining pages of the fragment you are reading.",
        show=None, used='next_ms'),
    Tip('ktiv',
        "🌐 **View on Ktiv** opens the manuscript in the National Library of Israel's catalogue.",
        show='browse:btn_b_catalog', used='ktiv'),
    Tip('recently_viewed',
        "Can't find a manuscript you saw yesterday? **Personal Lists** keeps a **Recently Viewed** list with the last 50 you opened.",
        show='recent_list', used='recent_list'),
    Tip('edit_comment',
        "You can edit the transcription or post comments with the matching buttons on the Browse tab or in the results viewer.",
        show='browse:btn_b_edit', used='edit_comment'),
    Tip('browse_results',
        "In the **Joins Lab** window, press **Browse results ▶** to show the results one after another beside the anchor fragment.",
        show='joins_lab', used=None),
    Tip('other_side',
        "If you have a fragment and a guess at what the fragment joining it says on both sides, try a tailored search: in the **Joins Lab**, tick **search also on the other side of the leaf (p ±1)**.",
        show='joins_lab', used=None),
    Tip('browse_from_result',
        "📖 on a search result opens the whole manuscript on the Browse tab; a double-click opens the viewer at that page.",
        show='search:results_table', used='browse_from_result'),
)


TIPS_BY_KEY = {t.key: t for t in TIPS}


def tip_html(tip: Tip, lang: str | None = None) -> str:
    """The tip in the interface language, **bold** as <b>, the rest escaped.

    It starts with the language's direction mark: a Hebrew tip that opens with
    a key ("Ctrl+G ...") or an English one that opens with a Hebrew example
    ("#שלום# finds ...") would otherwise take its direction from that first
    word and lay the sentence out backwards."""
    if lang is None:
        import genizah_core
        lang = genizah_core.CURRENT_LANG
    text = html.escape(tr(tip.text), quote=False)
    mark = "\u200f" if lang == 'he' else "\u200e"
    return mark + re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", text)


def next_tip(seen, used, tips=TIPS):
    """The first tip not shown yet whose feature is not in use, or None."""
    seen, used = set(seen or ()), set(used or ())
    for tip in tips:
        if tip.key in seen or (tip.used and tip.used in used):
            continue
        return tip
    return None


_used_cache: set | None = None


def features_used() -> set:
    global _used_cache
    if _used_cache is None:
        _used_cache = set(load_app_config().get(FEATURES_USED_KEY) or ())
    return set(_used_cache)


def mark_used(feature: str) -> None:
    """A feature was used: its tip is no longer needed. Written once per feature."""
    global _used_cache
    if not feature:
        return
    current = features_used()
    if feature in current:
        return
    current.add(feature)
    _used_cache = current
    save_app_config({FEATURES_USED_KEY: sorted(current)})


def record_start() -> int:
    """Count this start of the program; returns the count (1 = the first start)."""
    starts = int(load_app_config().get(TIPS_STARTS_KEY) or 0) + 1
    save_app_config({TIPS_STARTS_KEY: starts})
    return starts


def mark_seen(tip: Tip) -> None:
    seen = list(load_app_config().get(TIPS_SEEN_KEY) or [])
    if tip.key not in seen:
        seen.append(tip.key)
        save_app_config({TIPS_SEEN_KEY: seen})


def tips_off() -> bool:
    return bool(load_app_config().get(TIPS_OFF_KEY, False))


def set_tips_off(off: bool) -> None:
    save_app_config({TIPS_OFF_KEY: bool(off), TIPS_CLOSED_KEY: 0})


def note_closed() -> bool:
    """x on the strip. Returns True when this close turned the tips off
    (CLOSES_BEFORE_OFF in a row, with no Show me or Next tip between)."""
    closed = int(load_app_config().get(TIPS_CLOSED_KEY) or 0) + 1
    if closed >= CLOSES_BEFORE_OFF:
        set_tips_off(True)
        return True
    save_app_config({TIPS_CLOSED_KEY: closed})
    return False


def note_engaged() -> None:
    """Show me or Next tip: the closes in a row start again."""
    if load_app_config().get(TIPS_CLOSED_KEY):
        save_app_config({TIPS_CLOSED_KEY: 0})


class TipStrip(QFrame):
    """The strip: the tip, Show me (or the invitation), Next tip, Don't show
    tips and x. It holds the tip; the window decides what the buttons do."""

    show_me = pyqtSignal(object)
    next_requested = pyqtSignal()
    closed = pyqtSignal()
    turned_off = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.tip = None
        self.setObjectName("tip_strip")
        self.setStyleSheet(
            "#tip_strip { background-color: #fff7e0; border-bottom: 1px solid #f3d27a; }"
            "#tip_strip QLabel { color: #5c4300; background: transparent; }")
        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 4, 10, 4)
        icon = QLabel("\U0001F4A1")
        icon.setStyleSheet("font-size: 15px;")
        self.label = QLabel()
        self.label.setTextFormat(Qt.TextFormat.RichText)
        self.label.setWordWrap(True)
        self.label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.btn_show = QPushButton(tr("Show me"))
        self.btn_show.setStyleSheet(
            "QPushButton { background-color: #f39c12; color: white; font-weight: bold;"
            " border-radius: 4px; padding: 4px 10px; }")
        self.btn_next = QPushButton(tr("Next tip"))
        self.btn_off = QPushButton(tr("Don't show tips"))
        self.btn_off.setFlat(True)
        self.btn_off.setStyleSheet("QPushButton { color: #7a5b00; text-decoration: underline; border: none; }")
        self.btn_close = QPushButton("\u2715")
        self.btn_close.setToolTip(tr("Close"))
        self.btn_close.setFlat(True)
        self.btn_close.setStyleSheet(
            "QPushButton { color: #7a5b00; font-weight: bold; border: none; font-size: 15px; }"
            "QPushButton:hover { color: #dc3545; }")
        for b in (self.btn_show, self.btn_next, self.btn_off, self.btn_close):
            b.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_show.clicked.connect(lambda: self.show_me.emit(self.tip))
        self.btn_next.clicked.connect(self.next_requested.emit)
        self.btn_off.clicked.connect(self.turned_off.emit)
        self.btn_close.clicked.connect(self.closed.emit)
        layout.addWidget(icon, 0, Qt.AlignmentFlag.AlignTop)
        layout.addWidget(self.label, 1)
        for b in (self.btn_show, self.btn_next, self.btn_off, self.btn_close):
            layout.addWidget(b, 0, Qt.AlignmentFlag.AlignVCenter)
        self.hide()

    def set_tip(self, tip: Tip, action_label: str | None = None, has_next: bool = True):
        """Show *tip*. *action_label* names the main button (an invitation's
        "Build the index" / "Login"); None hides it, for a tip with nothing
        to point at."""
        self.tip = tip
        self.label.setText(tip_html(tip))
        self.btn_show.setVisible(action_label is not None)
        if action_label is not None:
            self.btn_show.setText(action_label)
        self.btn_next.setVisible(has_next)
        self.show()
