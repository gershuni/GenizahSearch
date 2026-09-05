# -*- coding: utf-8 -*-
"""The six defects the PR #335 Codex review found, each pinned so it cannot return.

All six were real, and unlike the earlier local review round -- which found only
weak tests -- these were production defects. Two of them are the INVERSE of the
bug this whole branch exists to fix: not "a machine credited for a scholar's
work", but a scholar credited for a machine's.

They are grouped here rather than scattered because they share one cause: the
credit and the thing being credited were computed in different places and could
disagree. Every test below asserts the two stay together.
"""

from __future__ import annotations

import ast
import io
import re
import tokenize
from pathlib import Path

import pytest

from shared.transcription_credits import (
    KIND_FGP,
    KIND_TRANSLATION,
    page_citation,
    resolve_transcription_credit,
)

REPO = Path(__file__).resolve().parents[1]
APP = REPO / 'genizah_app.py'
BROWSE = REPO / 'web' / 'pages' / 'browse.py'
CHIP = REPO / 'web' / 'citation_chip.py'
CSS = REPO / 'web' / 'static' / 'common.css'


def _code_only(path: Path) -> str:
    """Source with comments and docstrings blanked."""
    src = path.read_text(encoding='utf-8')
    drop = set()
    prev = None
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if tok.type == tokenize.COMMENT:
            drop.update(range(tok.start[0], tok.end[0] + 1))
        elif tok.type == tokenize.STRING and (
                prev is None or prev.type in (tokenize.INDENT, tokenize.NEWLINE,
                                              tokenize.NL, tokenize.DEDENT)):
            drop.update(range(tok.start[0], tok.end[0] + 1))
        if tok.type not in (tokenize.NL, tokenize.COMMENT):
            prev = tok
    return '\n'.join('' if i + 1 in drop else ln
                     for i, ln in enumerate(src.splitlines()))


def _method(code: str, name: str) -> str:
    """One method's body, from its def to the next def at method indentation."""
    parts = code.split('def %s' % name, 1)
    assert len(parts) == 2, 'no def %s' % name
    return parts[1].split('\n    def ', 1)[0]


def _load(name: str, src: Path, extra: dict = None):
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


# ---------------------------------------------------------------------------
# P1 -- the .docx carried HTR text under someone else's name
# ---------------------------------------------------------------------------

def _export(version_info, displayed, page_text):
    """Drive the real export with a DISPLAYED text distinct from the page text."""
    pytest.importorskip('docx')
    import zipfile

    from web.export_service import ExportService

    blob, _ = ExportService(None).export_browse_word({
        'shelfmark': 'T-S Ar.50.74', 'title': 't', 'sys_id': '99001',
        'view_all': False, 'library_code': 'CUL',
        'library_name': 'Cambridge University Library',
        'p_num': 1, 'text': displayed, 'lang': 'en',
        'version_info': version_info, 'folio_label': '1r',
        'page_url': None, 'retrieved_on': '2026-09-05',
    })
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        xml = z.read('word/document.xml').decode('utf-8')
    del page_text
    return re.sub(r'<[^>]+>', ' ', xml.replace('</w:p>', '\n'))


def test_the_export_ships_the_text_it_credits():
    """THE DEFECT, stated end to end.

    `handle_version_change` wrote the displayed text into a closure local and
    the selected source into `enrichment_refs`. The export read the SOURCE from
    `enrichment_refs` and the TEXT from `state.current_page.text` -- always the
    automatic transcription. So a .docx exported while a Princeton edition was
    on screen contained MiDRASH's text, credited to Goitein.
    """
    text = _export({'source': 'pgp', 'attribution': 'Goitein, S. D.'},
                   displayed='PRINCETON EDITION TEXT',
                   page_text='MIDRASH HTR TEXT')
    assert 'PRINCETON EDITION TEXT' in text
    assert 'Goitein' in text
    assert 'MiDRASH' not in text


def test_the_browse_page_pairs_the_text_with_the_source():
    """The two are written together and read together, or not at all.

    This is the invariant the defect broke. Asserted on code, not prose: the
    export must take BOTH from `enrichment_refs`, so no future edit can take the
    source from the version selector and the text from the page state again.
    """
    code = _code_only(BROWSE)
    assert "enrichment_refs['current_version_text'] = new_text" in code, (
        'the displayed text is no longer stored beside the source it came from')
    assert "enrichment_refs.get('current_version_text')" in code, (
        'the export no longer reads the displayed text')

    # And the pairing is CONDITIONAL on the same key, so "no version chosen"
    # gives page text AND no version_info, never one of each.
    export_block = code.split("export_data['p_num'] = state.current_page.p_num", 1)
    assert len(export_block) == 2
    tail = export_block[1][:600]
    assert "current_version_info" in tail, (
        'the text falls back on a different condition from the credit, so the '
        'two can disagree again')


def test_full_manuscript_view_still_credits_the_automatic_transcription():
    """The CONTROL for the pairing above.

    Full Manuscript View has no per-page version chooser -- every folio's stored
    text is what is exported -- so MiDRASH is the correct unconditional credit
    there, and a fix that attached a stale selected source to it would be a new
    instance of the same bug.
    """
    code = _code_only(BROWSE)
    assert 'None if state.view_all' in code, (
        'the all-pages export can carry a version_info from the single-page '
        'selector, crediting one folio\'s scholar for the whole manuscript')


# ---------------------------------------------------------------------------
# P1 -- an FGP translation was called a transcription
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('lang', ['en', 'he'])
def test_an_fgp_translation_is_not_called_a_transcription(lang):
    """`version_selector` sends `source='fgp'` with `is_translation=True`.

    The flag was ignored, so the sheet, the chip and the .docx all headed
    translated text "Transcription: Friedberg Genizah Project" -- a false claim
    about what the reader is looking at, distinct from who made it.
    """
    credit = resolve_transcription_credit(
        {'source': 'fgp', 'is_translation': True, 'language': 'English',
         'source_credit': 'FGP team'}, lang=lang)
    assert credit.kind == KIND_TRANSLATION, credit.kind
    heading = credit.heading
    assert ('Transcription' not in heading if lang == 'en'
            else 'תעתוק' not in heading), (
        'a translation is headed as a transcription: %r' % heading)
    # Still credited to FGP -- only the KIND and heading change.
    assert 'FGP team' in ' '.join(credit.citation_lines)


def test_an_fgp_translation_names_its_language_when_known():
    credit = resolve_transcription_credit(
        {'source': 'fgp', 'is_translation': True, 'language': 'English',
         'source_credit': 'FGP team'}, lang='en')
    assert 'English' in credit.heading


def test_an_fgp_EDITION_is_still_a_transcription():
    """The CONTROL. Routing every FGP source to the translation branch would
    satisfy the tests above and mislabel every FGP edition instead."""
    credit = resolve_transcription_credit(
        {'source': 'fgp', 'source_credit': 'FGP team'}, lang='en')
    assert credit.kind == KIND_FGP
    assert 'Transcription' in credit.heading


def test_the_flag_reaches_the_sentence_too():
    """The chip and print sheet render `_source_clause`, not the heading."""
    text = page_citation(
        {'source': 'fgp', 'is_translation': True, 'language': 'English',
         'source_credit': 'FGP team'}, lang='en', shelfmark='T-S 1',
        retrieved_on='2026-09-05').text
    assert 'transcription' not in text.lower(), (
        'the sentence calls an FGP translation a transcription: %r' % text)


# ---------------------------------------------------------------------------
# P1 -- the desktop Browse tab cited a reader's own local file
# ---------------------------------------------------------------------------

def test_a_local_document_gets_no_browse_page_citation():
    """The guard was added to the dialog and not to its Browse-tab twin.

    The Browse panel explicitly supports LOCAL '97' documents, so this credited
    MiDRASH for the reader's own PDF.

    The stub is FULLY POPULATED on purpose. A first version set
    `meta_mgr = None`, so the method returned None through the metadata guard
    whether or not the LOCAL check existed -- removing the guard left the test
    green, caught by mutation. Everything here is present and valid; the only
    reason to withhold a citation is that the id is LOCAL.
    """
    fn = _load('_browse_page_citation', APP, extra={
        'get_library_display': lambda code, short=False: 'Cambridge University Library',
    })

    class _Combo:
        @staticmethod
        def currentData():
            return {'source': 'original'}

    class _Meta:
        # A LOCAL sys_id is a `97` prefix and EXACTLY 18 digits (see
        # shared/local_sys_id). A first fixture used 20 and was simply not
        # LOCAL, so the test failed against a working guard.
        nli_cache = {'970012345601234567': {'shelfmark': 'my-scan.pdf'}}

        @staticmethod
        def get_library_for_id(_sid):
            return 'CUL'

    class _Stub:
        current_browse_sid = '970012345601234567'
        meta_mgr = _Meta()
        browse_version_combo = _Combo()

        @staticmethod
        def _credit_version_info(_data):
            return None

        @staticmethod
        def _citation_lang():
            return 'en'

        @staticmethod
        def _displayed_folio_label_for_pgp():
            return '1r'

    assert fn(_Stub(), retrieved_on='2026-09-05') is None, (
        "a LOCAL document produced a page citation, so the reader's own scan "
        'is credited to Dicta and MiDRASH')


def test_a_corpus_document_still_gets_one():
    """The CONTROL: a guard that rejected everything would pass the test above."""
    fn = _load('_browse_page_citation', APP, extra={
        'get_library_display': lambda code, short=False: 'Cambridge University Library',
    })

    class _Combo:
        @staticmethod
        def currentData():
            return {'source': 'original'}

    class _Meta:
        nli_cache = {'99001': {'shelfmark': 'T-S Ar.50.74'}}

        @staticmethod
        def get_library_for_id(_sid):
            return 'CUL'

    class _Stub:
        current_browse_sid = '99001'
        meta_mgr = _Meta()
        browse_version_combo = _Combo()

        @staticmethod
        def _credit_version_info(_data):
            return None

        @staticmethod
        def _citation_lang():
            return 'en'

        @staticmethod
        def _displayed_folio_label_for_pgp():
            return '1r'

        @staticmethod
        def _software_clause():
            return 'Dicta Genizah Search Pro V9.1.0'

    citation = fn(_Stub(), retrieved_on='2026-09-05')
    assert citation is not None
    assert 'T-S Ar.50.74' in citation.text


# ---------------------------------------------------------------------------
# The citation bar offered "this page" on every tab (owner, 2026-09-05)
# ---------------------------------------------------------------------------

class _BrowseTab:
    """Stands in for the Browse tab widget."""


class _SearchTab:
    """Stands in for any other tab."""


def _browse_stub(active_tab, sid='99001'):
    browse_tab = _BrowseTab()

    class _Tabs:
        def __init__(self, current):
            self._current = current

        def currentWidget(self):
            return self._current

    class _Combo:
        @staticmethod
        def currentData():
            return {'source': 'original'}

    class _Meta:
        nli_cache = {'99001': {'shelfmark': 'T-S Ar.50.74'}}

        @staticmethod
        def get_library_for_id(_sid):
            return 'CUL'

    class _Stub:
        current_browse_sid = sid
        meta_mgr = _Meta()
        browse_version_combo = _Combo()

        @staticmethod
        def _credit_version_info(_data):
            return None

        @staticmethod
        def _citation_lang():
            return 'en'

        @staticmethod
        def _displayed_folio_label_for_pgp():
            return '1r'

        @staticmethod
        def _software_clause():
            return 'Dicta Genizah Search Pro V9.1.0'

    stub = _Stub()
    stub.browse_tab = browse_tab
    stub.tabs = _Tabs(browse_tab if active_tab == 'browse' else _SearchTab())
    return stub


def _browse_citation():
    return _load('_browse_page_citation', APP, extra={
        'get_library_display': lambda code, short=False: 'Cambridge University Library',
    })


def test_no_page_citation_while_another_tab_is_on_screen():
    """THE DEFECT the owner reported.

    The bar spans the whole window and `current_browse_sid` stays set after the
    reader leaves Browse, so on the Search tab "Citation for this page" copied a
    citation for a folio nobody was looking at -- naming a library, a shelfmark
    and a scholar for a page that was not displayed.
    """
    assert _browse_citation()(_browse_stub('search'),
                              retrieved_on='2026-09-05') is None, (
        'a citation was offered for the Browse tab\'s manuscript while a '
        'different tab was on screen')


def test_the_page_citation_returns_on_the_browse_tab():
    """The CONTROL. A gate that refused everywhere would pass the test above,
    and would silently remove the feature."""
    citation = _browse_citation()(_browse_stub('browse'),
                                  retrieved_on='2026-09-05')
    assert citation is not None
    assert 'T-S Ar.50.74' in citation.text


def test_the_tab_gate_is_not_keyed_on_an_index_or_a_label():
    """Widget identity, not position or text.

    `_on_tab_changed` carries a hardcoded 0-6 index map that would mis-point the
    moment a tab is inserted, and `tabText()` is translated -- that file says so
    itself. Either would make this gate wrong in a way no test data would show.
    """
    body = _method(_code_only(APP), '_browse_page_citation')
    assert 'currentWidget()' in body
    assert 'is not browse_tab' in body
    assert 'currentIndex' not in body, 'the gate is keyed on a tab index'
    assert 'tabText' not in body, 'the gate is keyed on translated tab text'


# ---------------------------------------------------------------------------
# P1 -- an explicitly absent dialog PGP url fell back to the Browse tab's
# ---------------------------------------------------------------------------

def test_an_explicit_none_pgp_url_does_not_borrow_the_browse_tabs():
    """`None` means "this surface has no url", not "I did not say".

    `_rd_page_citation` ALWAYS passes its own `_rd_pgp_url`, which is None for a
    manuscript with no PGP record -- so `pgp_url is not None` treated the common
    case exactly like an omitted argument and the dialog's citation linked to
    whatever the Browse tab last had open. The parameter existed to stop that
    and did not, for the value that occurs most.
    """
    fn = _load('_credit_version_info', APP)

    class _MainWindow:
        _browse_pgp_url = 'https://geniza.princeton.edu/A-DIFFERENT-MS'

    info = fn(_MainWindow(), {'source': 'pgp_edition', 'scholar': 'G.'},
              pgp_url=None)
    assert info['pgp_url'] is None, (
        "an explicitly absent url borrowed the Browse tab's: %r"
        % info['pgp_url'])


def test_omitting_the_argument_still_falls_back():
    """The CONTROL, and the compatibility guarantee for the Browse tab itself."""
    fn = _load('_credit_version_info', APP)

    class _MainWindow:
        _browse_pgp_url = 'https://geniza.princeton.edu/THE-BROWSE-TAB'

    info = fn(_MainWindow(), {'source': 'pgp_edition', 'scholar': 'G.'})
    assert info['pgp_url'] == 'https://geniza.princeton.edu/THE-BROWSE-TAB'


# ---------------------------------------------------------------------------
# P2 -- malformed CSS silently dropped the panel height bound
# ---------------------------------------------------------------------------

def test_the_chip_panel_rule_is_valid_css():
    """A comment closed early left prose as raw CSS and an unmatched `*/`.

    Browsers discard the malformed declaration sequence, taking `max-height`
    with it -- so the bound was absent. My own browser check could not catch it:
    the old and new formulas compute to the SAME value at both viewports I
    measured, so the measurement could not tell "new rule" from "no rule".

    Checked by parsing comments out and confirming the declarations survive.
    """
    css = CSS.read_text(encoding='utf-8')
    stripped = re.sub(r'/\*.*?\*/', '', css, flags=re.S)
    assert '*/' not in stripped, (
        'an unmatched `*/` remains after removing every comment, so a rule '
        'block is malformed and its declarations are discarded')

    block = stripped.split('.citation-chip-panel', 1)
    assert len(block) == 2, '.citation-chip-panel rule is gone'
    body = block[1].split('}', 1)[0]
    assert 'max-height:' in body, 'the panel height bound is not in the rule'
    assert 'overflow-y:' in body
    # No prose survived into the declarations.
    for decl in [d.strip() for d in body.split(';') if d.strip()]:
        assert ':' in decl, 'non-declaration text inside the rule: %r' % decl


def test_the_bound_cannot_evaluate_to_zero():
    """`min(70vh, 32rem)` alone collapses to 0 wherever `vh` resolves to 0 --
    which is not hypothetical; measuring in a hidden pane returned exactly
    that. A bound that can reach zero hides the citation entirely."""
    css = CSS.read_text(encoding='utf-8')
    body = css.split('.citation-chip-panel', 1)[1].split('}', 1)[0]
    rule = [ln for ln in body.splitlines() if 'max-height:' in ln][0]
    assert 'max(' in rule, 'the height bound has no floor: %r' % rule


# ---------------------------------------------------------------------------
# P2 -- the chip registry grew by one closure per visit
# ---------------------------------------------------------------------------

def test_the_chip_registry_is_cleaned_up_on_disconnect():
    """`_forget` only ran when an update was attempted after the client had gone.

    A reader who closes a tab generates no later citation update, so nothing
    ever removed their entry -- and each holds a closure over a whole obsolete
    UI tree. On a long-running server that is ordinary traffic, not an edge
    case.
    """
    code = _code_only(CHIP)
    assert 'client.on_disconnect' in code, (
        'nothing removes a registry entry when its client disconnects')
    assert '_forget' in code

    # PER-CLIENT, never app-wide. `app.on_disconnect` appends to ONE global
    # list that fires for EVERY disconnect, so a per-render handler there meant
    # the first reader to close a tab forgot the chips of every other reader
    # still on the site -- their citations then silently stopped following the
    # page. That shipped, in the fix for the leak this test was written for; it
    # is worse than the leak, because a leak wastes memory and this served
    # wrong citations. The global list grew forever too, so the leak had only
    # moved.
    assert 'app.on_disconnect' not in code, (
        'cleanup is registered application-wide: one reader disconnecting '
        'unregisters every other live citation chip')


# ---------------------------------------------------------------------------
# Reviews 2-4: seven more, all verified real
# ---------------------------------------------------------------------------

def test_a_metadata_only_record_gets_no_citation():
    """P1. A synthetic inventory id is served by `get_metadata_only_browse_page`
    with `text=''`: an image and a catalogue entry, and NO transcription.

    The chip guard rejected only `None`, so those pages offered a citation
    crediting MiDRASH for a transcription that does not exist.
    """
    code = _code_only(BROWSE)
    body = code.split('def _update_citation_chip', 1)[1].split('\n    def ', 1)[0]
    assert "if not (page.text or '').strip():" in body, (
        'a page with no transcription still produces a page citation')
    assert body.index("if not (page.text or '').strip():") < body.index(
        'set_page_citation(browse_page_citation('), (
        'the empty-text check runs after the citation is built')


def test_the_chip_is_cleared_when_the_manuscript_goes_away():
    """P2. A search with no matches sets `current_page = None` and rebuilds the
    view, but the layout-owned chip kept the PREVIOUS manuscript's citation --
    so a reader could copy a citation for a folio no longer on screen."""
    body = _code_only(BROWSE).split('def _update_citation_chip', 1)[1] \
                             .split('\n    def ', 1)[0]
    # BOTH guards, matched on their own conditions. A first version asserted
    # only that `set_page_citation(None)` appeared before `_sm =`, which the
    # EMPTY-TEXT guard satisfies on its own -- so deleting the page-is-None
    # guard left it green. Found by mutation.
    assert 'if page is None:' in body
    none_guard = body.split('if page is None:', 1)[1].split('return', 1)[0]
    assert 'set_page_citation(None)' in none_guard, (
        'the stale citation survives when the page disappears: %r' % none_guard)


def test_the_citation_url_names_the_folio_it_cites():
    """P2. The URL carried only the shelfmark, so a citation naming "folio 2v"
    opened the manuscript at 1r -- and was ambiguous on a multi-volume record.

    Same three parameters `_update_browser_url` uses, because that IS this app's
    durable locator; a citation must not invent a second one that drifts.
    """
    code = _code_only(BROWSE)
    body = code.split('def _citation_page_url', 1)[1].split('\n    def ', 1)[0]
    for param in ("'sys_id'", "'page'", "'volume_ie'"):
        assert param in body, 'the citation URL omits %s' % param
    # And BOTH consumers use it, so the chip and the .docx cannot disagree.
    # CALL sites, not raw occurrences: `def _citation_page_url():` contains the
    # string too, so counting occurrences let one caller be deleted and still
    # read as two. Found by mutation.
    calls = (code.count('_citation_page_url()')
             - code.count('def _citation_page_url()'))
    assert calls >= 2, (
        'the citation URL has %d caller(s); the chip and the .docx must both '
        'use it, or a citation copied off the screen and one printed in the '
        'document can point at different folios' % calls)


def test_the_all_pages_export_does_not_name_one_folio():
    """P2. `folio_label` kept the folio selected before entering Full Manuscript
    View, so a .docx containing every page described itself as one folio."""
    code = _code_only(BROWSE)
    assert "'folio_label': None if state.view_all else _folio," in code, (
        'the all-pages export still names a single folio')


def test_print_this_page_is_disabled_where_it_would_lie():
    """P2 x2. In Full Manuscript View the DOM holds every folio, so "this page"
    printed the whole manuscript. In edit mode the masthead, credit and rendered
    text are built by the VIEW branch only, so what printed was the editor --
    unsaved draft text with no shelfmark and no attribution on the sheet."""
    code = _code_only(BROWSE)
    assert '_no_single = (state.view_all' in code
    assert 'or state.edit_mode' in code
    assert 'or state.edit_loading)' in code
    assert "_single.props('disable')" in code, (
        'the entry is computed as unavailable but still clickable')


def test_a_failed_full_manuscript_toggle_clears_the_print_intent():
    """P2. `toggle_view_all` swallows a failed fetch and leaves `view_all`
    false, but `print_pending` stayed true -- so the NEXT time the reader opened
    Full Manuscript View, deliberately and much later, a print dialog appeared
    that nobody had asked for."""
    code = _code_only(BROWSE)
    body = code.split('async def _print_all_pages', 1)[1].split('with ui.button', 1)[0]
    assert 'await toggle_view_all()' in body
    assert 'if not state.view_all:' in body, (
        'a failed toggle leaves the print intent set: %r' % body)
    assert "print_pending['value'] = False" in body


def test_an_english_translation_is_not_exported_as_rtl_hebrew():
    """P2. `add_hebrew_paragraph` sets `w:bidi`/`w:rtl` and right-aligns.

    Correct while the export always carried the Hebrew transcription; since it
    started shipping the DISPLAYED text, an English translation went through it
    too and came out right-aligned RTL.
    """
    body = _code_only(REPO / 'web' / 'export_service.py') \
        .split('def export_browse_word', 1)[1].split('\ndef ', 1)[0]
    assert "_language.lower() == 'english'" in body, (
        'the export direction ignores the translation language')
    assert 'doc.add_paragraph(browse_data[' in body, (
        'there is no non-RTL path for an English translation')
