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

    citation = fn(_Stub(), retrieved_on='2026-09-05')
    assert citation is not None
    assert 'T-S Ar.50.74' in citation.text


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
    assert 'app.on_disconnect' in code, (
        'nothing removes a registry entry when its client disconnects')
    assert '_forget' in code
