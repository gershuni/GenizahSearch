# -*- coding: utf-8 -*-
"""The ResultDialog "Cite" button, and the parent method it now shares.

WHY THE DIALOG NEEDS ITS OWN BUTTON. ResultDialog is shown with `.exec()` and
never overrides modality, so it is application-modal: while it is open the main
window -- and with it the citation bar at its foot -- is inert. Before this
there was no way to get a citation for the folio you were actually looking at.

WHAT THESE TESTS COVER, AND WHAT THEY CANNOT. Constructing a ResultDialog needs
a QApplication, a MetadataManager and a SearchEngine, so the widget itself is
out of reach here; `tests/test_eliding_label.py` shows the shape a `gui`-marked
widget test takes. What IS covered without Qt, and is where the real risk sits:

  - `_credit_version_info`'s new `pgp_url` parameter, exercised as a function
    (lifted by AST, as in `tests/test_desktop_citation_bar.py`). The bug it
    exists to prevent -- the dialog's citation carrying the BROWSE TAB's PGP
    url -- is a silent wrong-value bug, not a crash.
  - the wiring and placement of the two buttons, read from source, including
    the compact-mode twin that a `community_row`-only button would lack.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
APP = REPO / 'genizah_app.py'
RD = REPO / 'desktop' / 'result_dialog.py'


def _load(name: str, src: Path = APP, extra: dict = None):
    tree = ast.parse(src.read_text(encoding='utf-8'))
    found = [n for n in ast.walk(tree)
             if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(found) == 1, 'expected one def %s, found %d' % (name, len(found))
    fn = found[0]
    fn.decorator_list = []
    module = ast.Module(body=[fn], type_ignores=[])
    ast.fix_missing_locations(module)
    ns: dict = dict(extra or {})
    exec(compile(module, str(src), 'exec'), ns)                  # noqa: S102
    return ns[name]


def _code_only(path: Path) -> str:
    """Source with comments and docstrings blanked.

    Both source-reading tests below first matched their own explanatory prose --
    the comment in `_rd_page_citation` saying the citation must NOT come from
    `lbl_shelf` contains the word `lbl_shelf`. A source assertion a docstring
    can satisfy is not an assertion about behaviour.
    """
    import io as _io
    import tokenize as _tok

    src = path.read_text(encoding='utf-8')
    drop = set()
    prev = None
    for tok in _tok.generate_tokens(_io.StringIO(src).readline):
        if tok.type == _tok.COMMENT:
            drop.update(range(tok.start[0], tok.end[0] + 1))
        elif tok.type == _tok.STRING and (
                prev is None or prev.type in (_tok.INDENT, _tok.NEWLINE,
                                              _tok.NL, _tok.DEDENT)):
            drop.update(range(tok.start[0], tok.end[0] + 1))
        if tok.type not in (_tok.NL, _tok.COMMENT):
            prev = tok
    return '\n'.join('' if i + 1 in drop else ln
                        for i, ln in enumerate(src.splitlines()))


def _method(code: str, name: str) -> str:
    """One method's body, from its def to the next def at method indentation."""
    parts = code.split('def %s' % name, 1)
    assert len(parts) == 2, 'no def %s' % name
    return parts[1].split('\n' + '    def ', 1)[0]


class _Stub:
    """Stands in for whichever surface owns the translation."""

    def __init__(self, browse_url=None):
        self._browse_pgp_url = browse_url


# ---------------------------------------------------------------------------
# 1. The shared translation, and the per-surface PGP url
# ---------------------------------------------------------------------------

def test_the_dialogs_own_pgp_url_wins_over_the_browse_tabs():
    """THE BUG THIS PARAMETER EXISTS TO PREVENT.

    `_credit_version_info` read `self._browse_pgp_url` directly. Calling it as
    `parent._credit_version_info(data)` from the dialog binds `self` to the MAIN
    WINDOW, so the citation for the dialog's manuscript would carry the url of
    whatever happened to be open on the Browse tab -- a wrong value, silently,
    with nothing to raise.
    """
    fn = _load('_credit_version_info')
    parent = _Stub(browse_url='https://geniza.princeton.edu/BROWSE-TAB')

    info = fn(parent, {'source': 'pgp_edition', 'scholar': 'Goitein, S. D.'},
              pgp_url='https://geniza.princeton.edu/THE-DIALOG')
    assert info['pgp_url'] == 'https://geniza.princeton.edu/THE-DIALOG', (
        "the dialog's citation carries the Browse tab's PGP url")


def test_omitting_it_keeps_the_browse_tabs_url():
    """The CONTROL, and the compatibility guarantee for existing callers.

    Without this, a parameter that always won would have broken the main
    window's own citation while looking like it fixed the dialog's.
    """
    fn = _load('_credit_version_info')
    parent = _Stub(browse_url='https://geniza.princeton.edu/BROWSE-TAB')
    info = fn(parent, {'source': 'pgp_edition', 'scholar': 'Goitein, S. D.'})
    assert info['pgp_url'] == 'https://geniza.princeton.edu/BROWSE-TAB'


def test_the_translation_url_reaches_translations_too():
    """`pgp_translation` carries the url as well, and had its own hardcoded read.

    Fixing only the `pgp_edition` branch would leave a translation citing the
    Browse tab's document -- the same bug, one branch over.
    """
    fn = _load('_credit_version_info')
    parent = _Stub(browse_url='https://geniza.princeton.edu/BROWSE-TAB')
    info = fn(parent, {'source': 'pgp_translation', 'scholar': 'Goitein, S. D.',
                       'language': 'English'},
              pgp_url='https://geniza.princeton.edu/THE-DIALOG')
    assert info['pgp_url'] == 'https://geniza.princeton.edu/THE-DIALOG'


def test_there_is_still_only_one_vocabulary_translation():
    """The dialog must SHARE `_credit_version_info`, not fork it.

    A copy is the obvious way to write this, and the method's own header warns
    against it: desktop's combo vocabulary and the shared module's are different
    and a forked copy stops matching the moment a provider is added on one side.
    """
    code = _code_only(RD)
    assert '_credit_version_info(' in code, (
        'the dialog no longer calls the shared translation')
    # NOT "the combo literals must not appear here": the dialog legitimately
    # branches on `pgp_edition`/`fgp_translation` to choose RTL and which text
    # to render (result_dialog.py ~1484). The fork signature is PRODUCING the
    # shared module's vocabulary, which only `_credit_version_info` may do.
    body = _method(code, '_rd_page_citation')
    for produced in ("'source': 'pgp'", "'source': 'fgp'",
                     "'source': 'translation'", "'source': 'user'",
                     "'source': 'pending'"):
        assert produced not in body, (
            '%s is built inside result_dialog.py -- the vocabulary translation '
            'has been forked out of _credit_version_info, which its own header '
            'warns against' % produced)


# ---------------------------------------------------------------------------
# 2. The buttons
# ---------------------------------------------------------------------------

def test_the_cite_button_is_on_the_toolbar_with_both_citations():
    rd = RD.read_text(encoding='utf-8')
    assert 'self.btn_cite = QToolButton()' in rd
    assert 'community_row.addWidget(self.btn_cite)' in rd, (
        'the cite button is not on the dialog toolbar')
    assert re.search(r'_rd_act_cite_page\.triggered\.connect\('
                     r'self\._rd_copy_page_citation\)', rd)
    assert re.search(r'_rd_act_cite_site\.triggered\.connect\('
                     r'self\._rd_copy_site_citation\)', rd)
    assert 'self.rd_cite_menu.aboutToShow.connect' in rd, (
        'the page entry is not re-evaluated when the menu opens')


def test_it_survives_compact_mode():
    """`_toggle_compact_mode` hides `header_widget` WHOLESALE.

    So a button placed only in `community_row` vanishes the moment a reader
    collapses the header. The codebase splits on this -- Joins, PGP and Catalog
    have compact twins; Edit, Comment and Corrections do not -- and a citation
    belongs with the first group: this dialog is modal, so it is the only route
    to a citation while it is open.
    """
    rd = RD.read_text(encoding='utf-8')
    assert 'self.header_widget.setVisible(not compact)' in rd, (
        'compact mode no longer hides the header; re-check whether the twin is '
        'still needed rather than deleting this test')
    assert 'self.btn_compact_cite = QToolButton()' in rd, (
        'no compact twin: the cite button disappears in compact mode')
    assert 'compact_layout.addWidget(self.btn_compact_cite)' in rd
    assert 'self.btn_compact_cite.setMenu(self.rd_cite_menu)' in rd, (
        'the compact twin has its own menu instead of sharing one, so the two '
        'can drift')


def test_the_message_box_is_parented_to_the_dialog():
    """The dialog is application-modal.

    A QMessageBox parented to the main window would be blocked behind it -- the
    copy would appear to hang.
    """
    body = _method(_code_only(RD), '_rd_copy_citation')
    assert 'QMessageBox.information(self,' in body, (
        'the confirmation is not parented to the dialog: %r' % body)
    assert 'self._app' not in body


def test_a_local_scan_gets_no_page_citation():
    """A "97" sys_id is the reader's OWN scan in My Library.

    Nobody in this citation transcribed it and it has no corpus library or
    shelfmark, so there is no honest page citation to give. Asserted on the
    source because the guard is a branch, and a citation that silently named
    Dicta and MiDRASH for a user's own PDF is precisely the class of false claim
    this whole change removes.
    """
    body = _method(_code_only(RD), '_rd_page_citation')
    assert '_is_local(sid)' in body, (
        'LOCAL results are not excluded, so a reader\'s own scan would be cited '
        'as a corpus manuscript')
    assert 'is_synthetic_sys_id(sid)' in body


def test_the_shelfmark_actually_reaches_the_citation():
    """READING `nli_cache` is not the same as USING what it returns.

    The source test below proves the right SOURCE is read. It does not prove
    the value survives: keeping the read and then setting `shelfmark = None`
    left it green (Codex review, confirmed by mutation). So this runs the real
    method and reads the citation it produces.
    """
    fn = _load('_rd_page_citation', src=RD, extra={
        'get_library_display': lambda code, short=False: 'Cambridge University Library',
        'is_synthetic_sys_id': lambda sid: False,
    })

    class _Combo:
        @staticmethod
        def currentData():
            return {'source': 'original'}

    class _Spin:
        @staticmethod
        def maximum():
            return 2

    class _Meta:
        nli_cache = {'99001': {'shelfmark': 'T-S Ar.50.74'}}

        @staticmethod
        def get_library_for_id(_sid):
            return 'CUL'

    class _App:
        @staticmethod
        def _credit_version_info(data, pgp_url=None):
            return None

        @staticmethod
        def _citation_lang():
            return 'en'

    dialog = _Stub()
    dialog.current_sys_id = '99001'
    dialog.meta_mgr = _Meta()
    dialog.rd_version_combo = _Combo()
    dialog.spin_page = _Spin()
    dialog._rd_pgp_url = None
    dialog._app = _App()
    dialog._rd_displayed_image_keys = lambda _total: ('1r', '')

    citation = fn(dialog, retrieved_on='2026-09-05')
    assert citation is not None
    assert 'T-S Ar.50.74' in citation.text, (
        'the shelfmark read from nli_cache never reached the citation: %r'
        % citation.text)
    assert 'Cambridge University Library' in citation.text
    assert 'folio 1r' in citation.text


def test_the_shelfmark_comes_from_the_same_source_as_the_browse_tab():
    """NOT from `lbl_shelf`, which is a formatted "Library | Shelfmark" string.

    Reusing that label is the obvious shortcut and would print the library twice
    once `library=` is also passed, and could make one manuscript cite
    differently in the dialog than on the Browse tab.
    """
    body = _method(_code_only(RD), '_rd_page_citation')
    assert "nli_cache.get(sid, {})" in body
    assert 'lbl_shelf' not in body, (
        'the citation is built from the formatted shelfmark label'
    )
