# -*- coding: utf-8 -*-
"""The "How to cite" chip, and the citation sentence behind it.

The chip replaced the sticky citation footer on 2026-09-04. A codebase map run
before that change found the footer had **no automated coverage of any kind** --
no test, no render-smoke, no Playwright selector referenced it or its classes.
That is why it could be deleted without a single red test, and it is exactly why
its replacement gets these.

What matters here, in order:

1. The SENTENCE. The first cut returned a six-row block; the owner's verdict was
   "too much duplicacy" and they wrote the shape they wanted. These pin that
   shape -- three clauses, no repeated domain, no repeated "retrieved".
2. WHO is credited. Same rule as the print sheet and the Word export, because
   all three now read one decision: MiDRASH for the automatic transcription and
   by default, the actual creator otherwise.
3. The SITE form is the default. Most readers used the site across many
   manuscripts; a citation pinned to the last folio they happened to open is
   the wrong default and they would have to edit it out.
4. The footer is really gone -- element, CSS, head script and storage keys.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from shared.export_utils import (
    GENIZAHSEARCH_URL,
    MIDRASH_CITATION_LINE,
    MIDRASH_INLINE_CITATION,
)
from shared.transcription_credits import page_citation, site_citation

REPO = Path(__file__).resolve().parents[1]
MAIN = REPO / 'web' / 'main.py'
CSS = REPO / 'web' / 'static' / 'common.css'
CHIP = REPO / 'web' / 'citation_chip.py'

#: What a surface PASSES (ISO is the transport form -- it has to survive
#: NiceGUI's JSON session storage to reach the Word export's route).
DATE_ISO = '2026-09-04'
#: What a READER sees. Owner's format, 2026-09-04: AP style in English,
#: day-month-comma-year in Hebrew.
DATE_SHOWN = {'en': 'Sept. 4, 2026', 'he': '4 בספטמבר, 2026'}
LANGS = ('en', 'he')

MS = dict(library='Cambridge University Library', shelfmark='T-S Ar.50.74',
          folio='1r', page_url=GENIZAHSEARCH_URL + '/browse?shelfmark=T-S+Ar.50.74')

SOURCES = {
    'htr': None,
    'fgp': {'source': 'fgp', 'source_credit': 'Prof. Example (FGP)'},
    'pgp': {'source': 'pgp', 'attribution': 'S. D. Goitein, unpublished editions.'},
    'translation': {'source': 'translation', 'language': 'Hebrew', 'attribution': 'M. Gil'},
    'user': {'source': 'user', 'author': 'A Reader'},
}


def _page(source='htr', lang='en', **over):
    kwargs = dict(MS)
    kwargs.update(over)
    return page_citation(SOURCES[source], lang=lang, retrieved_on=DATE_ISO, **kwargs)


# ---------------------------------------------------------------------------
# 1. The sentence
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('lang', LANGS)
def test_it_is_one_sentence_not_a_block(lang):
    """The owner's actual complaint. A block of rows is not a citation."""
    text = _page(lang=lang).text
    assert '\n' not in text, 'the citation is still multi-line: %r' % text


@pytest.mark.parametrize('lang', LANGS)
def test_the_domain_appears_once(lang):
    """The first cut printed the site credit, then the page URL, then the date --
    the domain twice and "retrieved" twice, in a string whose whole job is to be
    pasted into a bibliography."""
    text = _page(lang=lang).text
    assert text.count('genizahsearch.com') == 1, (
        'the domain appears %d times: %r' % (text.count('genizahsearch.com'), text))


@pytest.mark.parametrize('lang', LANGS)
def test_the_retrieval_date_appears_once_in_the_readers_own_format(lang):
    """The date is written for a reader, not stored for a machine.

    ISO went IN (`DATE_ISO`); what comes out is the owner's format. The ISO
    string must not leak to the reader -- that was the previous behaviour and
    the thing being reverted.
    """
    text = _page(lang=lang).text
    shown = DATE_SHOWN[lang]
    assert text.count(shown) == 1, (
        'expected %r exactly once, got %r' % (shown, text))
    assert DATE_ISO not in text, 'the raw ISO date leaked to the reader'
    assert re.search(r'\([^()]*%s[^()]*\)' % re.escape(shown), text), (
        'the date is not in the parenthetical access clause: %r' % text)


def test_every_month_is_written_out_in_both_languages():
    """A month table is exactly the thing that quietly goes wrong for one month
    of the year, so all twelve are checked rather than the one in the example.

    English follows AP style, which is what the owner's own "Sept. 4, 2026" is:
    five letters or fewer spelled out, the rest abbreviated with a period.
    Abbreviating all twelve would give "Jun." and "Jul.", which no American
    style guide writes.
    """
    import datetime
    from shared.transcription_credits import _format_retrieved
    expected_en = ['Jan.', 'Feb.', 'March', 'April', 'May', 'June',
                   'July', 'Aug.', 'Sept.', 'Oct.', 'Nov.', 'Dec.']
    for month in range(1, 13):
        d = datetime.date(2026, month, 4)
        en = _format_retrieved(d, 'en')
        he = _format_retrieved(d, 'he')
        assert en == '%s 4, 2026' % expected_en[month - 1], en
        assert he.startswith('4 ב'), 'Hebrew month lacks its ב prefix: %r' % he
        assert he.endswith(', 2026'), he
        assert re.search(r'[֐-׿]', he), he


def test_an_iso_string_is_transport_only_and_a_display_string_passes_through():
    """The Word export sends this date through JSON session storage, where a
    `date` object cannot survive -- so ISO in, formatted out. Anything that is
    NOT ISO is handed back untouched, for a caller with its own display date.
    """
    from shared.transcription_credits import _format_retrieved
    assert _format_retrieved('2026-09-04', 'en') == 'Sept. 4, 2026'
    assert _format_retrieved('some other form', 'en') == 'some other form'
    assert _format_retrieved(None, 'en') == ''


@pytest.mark.parametrize('lang', LANGS)
def test_it_names_the_manuscript_first(lang):
    """"Which manuscript" is the leading clause -- library, shelfmark, folio."""
    text = _page(lang=lang)
    assert text.text.startswith('Cambridge University Library, T-S Ar.50.74, ')
    assert text.text.index('T-S Ar.50.74') < text.text.index('genizahsearch.com')


@pytest.mark.parametrize('lang', LANGS)
def test_it_ends_as_a_sentence(lang):
    assert _page(lang=lang).text.rstrip().endswith('.')


def test_the_folio_is_labelled_in_each_language():
    assert 'folio 1r' in _page(lang='en').text
    assert 'דף 1r' in _page(lang='he').text


@pytest.mark.parametrize('lang', LANGS)
def test_a_missing_manuscript_degrades_to_the_site_clause(lang):
    """Pages with no manuscript (the homepage, /help) still cite correctly."""
    text = page_citation(None, lang=lang, retrieved_on=DATE_ISO,
                         page_url=GENIZAHSEARCH_URL).text
    assert 'genizahsearch.com' in text
    assert DATE_SHOWN[lang] in text


# ---------------------------------------------------------------------------
# 2. Who is credited
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('lang', LANGS)
def test_the_automatic_transcription_cites_midrash_inline(lang):
    text = _page('htr', lang).text
    assert MIDRASH_INLINE_CITATION in text
    assert 'doi.org/10.5281/zenodo.17734473' in text


def test_the_sentence_carries_the_full_author_list():
    """REVERSES an earlier decision in this same session, on the owner's ruling.

    The sentence used to abbreviate to "Stoekl Ben Ezra, D. et al.", on the
    reasoning that the print sheet and the .docx carry the full rows underneath.
    The owner's call (2026-09-05, supplying the exact string) is that the thing
    a reader COPIES has to be the thing they can publish -- and someone copying
    from the chip is not reading the rows.

    Asserted on the first and last names plus the DOI rather than on
    `MIDRASH_INLINE_CITATION` wholesale, so that a truncation in the middle --
    the failure mode a fixed-width surface actually produces -- cannot pass.
    """
    text = _page('htr').text
    assert 'et al.' not in text, (
        'the sentence still abbreviates the author list')
    assert 'Stoekl Ben Ezra, D., Bambaci, L.' in text
    assert 'Olszowy-Schlanger, J., & Gila, Y. (2025)' in text, (
        'the author list is cut short before its final names')
    assert 'MiDRASH Automatic Transcriptions. Zenodo.' in text
    assert 'https://doi.org/10.5281/zenodo.17734473' in text, (
        'the DOI is missing -- it is the part of a citation a reader most needs')


def test_the_full_citation_is_NOT_the_sheet_row_with_its_label_removed():
    """Two constants, deliberately, and they are not interchangeable.

    `MIDRASH_CITATION_LINE` is a LABELLED sheet row ("Citation: ..."), and
    reusing it in prose is how an earlier round of this work produced
    "\u05e2\u05dc \u05d1\u05e1\u05d9\u05e1 \u05ea\u05e2\u05ea\u05d5\u05e7: \u05e4\u05e8\u05d5\u05d9\u05e7\u05d8..." -- a label read as a sentence. The inline
    form carries the same names with no label, and this pins that they stay
    separate rather than one being defined from the other.
    """
    text = _page('htr').text
    assert MIDRASH_CITATION_LINE not in text, (
        'the labelled sheet row is being used as a sentence clause')
    assert 'Citation:' not in text


@pytest.mark.parametrize('lang', LANGS)
@pytest.mark.parametrize('source', ['fgp', 'pgp', 'translation'])
def test_someone_elses_scholarship_is_credited_instead_of_midrash(source, lang):
    text = _page(source, lang).text
    assert 'MiDRASH' not in text
    assert 'zenodo' not in text.lower()


@pytest.mark.parametrize('lang', LANGS)
def test_a_correction_credits_the_corrector_and_the_work_corrected(lang):
    text = _page('user', lang).text
    assert 'A Reader' in text
    assert 'MiDRASH' in text, (
        'a correction is an edit OF the automatic transcription; dropping '
        'MiDRASH would credit a proof-reader for a machine reading')


def test_no_provider_label_leaks_a_colon_into_the_sentence():
    """The print sheet's headings are standalone LABELS and carry a colon
    ("Transcription: Princeton Geniza Project"). Reused in prose they produced
    "using Transcription: Princeton Geniza Project, S. D. Goitein..." -- so the
    sentence has its own provider names."""
    for lang in LANGS:
        for source in SOURCES:
            text = _page(source, lang).text
            # The only colons allowed are inside URLs (https:) and the DOI.
            stripped = re.sub(r'https?://\S+', '', text)
            assert ':' not in stripped, (
                'a label colon leaked into the %s/%s sentence: %r'
                % (source, lang, text))


@pytest.mark.parametrize('source', list(SOURCES))
def test_the_hebrew_connector_is_al_basis(source):
    """Owner's wording (2026-09-04), replacing "על פי"."""
    assert 'על בסיס' in _page(source, 'he').text


def test_the_hebrew_correction_clause_does_not_double_the_root():
    """"על בסיס ... המבוסס על" put the same root twice in one clause."""
    text = _page('user', 'he').text
    assert 'המבוסס על' not in text
    assert 'לתעתוק' in text


# ---------------------------------------------------------------------------
# 3. The site form is the default
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('lang', LANGS)
def test_the_site_citation_names_the_site_and_its_address_and_zenodo(lang):
    """Owner: the regular chip mentions Dicta Genizah Search, the site address
    and the Zenodo citation -- and nothing manuscript-specific."""
    c = site_citation(lang=lang, retrieved_on=DATE_ISO)
    assert GENIZAHSEARCH_URL in c.text
    assert 'zenodo' in c.text.lower()
    assert DATE_SHOWN[lang] in c.text
    assert 'T-S' not in c.text
    assert 'folio' not in c.text and 'דף' not in c.text


@pytest.mark.parametrize('lang', LANGS)
def test_the_site_form_is_headed_as_the_site_not_the_page(lang):
    site = site_citation(lang=lang, retrieved_on=DATE_ISO).heading
    page = _page(lang=lang).heading
    assert site != page, 'the site and page citations share one heading'


def test_the_two_forms_share_one_assembler():
    """Not two hand-built strings: one assembler means the site form and the
    folio form cannot drift in wording, punctuation or language."""
    src = CHIP.read_text(encoding='utf-8')
    tc = (REPO / 'shared' / 'transcription_credits.py').read_text(encoding='utf-8')
    assert 'def site_citation' in tc
    assert 'page_citation(' in tc.split('def site_citation', 1)[1], (
        'site_citation no longer delegates to page_citation')
    assert 'site_citation' in src and 'page_citation' in src


def _code_only(path):
    """Source with every string literal and comment blanked out.

    Needed because the first version of the test below matched the module's own
    DOCSTRING, which names `app.storage.user` precisely to explain why it is not
    used. A guard that a correct explanation can trip is a guard that gets
    deleted rather than fixed.
    """
    import ast
    import io as _io
    import tokenize
    src = path.read_text(encoding='utf-8')
    tree = ast.parse(src)
    doc_lines = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) \
                and isinstance(node.value.value, str):
            doc_lines.update(range(node.lineno, (node.end_lineno or node.lineno) + 1))
    comment_lines = set()
    try:
        for tok in tokenize.generate_tokens(_io.StringIO(src).readline):
            if tok.type == tokenize.COMMENT:
                comment_lines.add(tok.start[0])
    except tokenize.TokenError:
        pass
    return '\n'.join(
        '' if (i + 1) in doc_lines or (i + 1) in comment_lines else ln
        for i, ln in enumerate(src.splitlines())
    )


# ---------------------------------------------------------------------------
# 4. The footer is gone, and the chip took its place
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('token', [
    'citation-footer', 'citation_footer_dismissed', 'citation_footer_off',
    'cite-compact', 'cite-off', 'citation-full', 'citation-compact',
])
def test_no_trace_of_the_footer_remains_in_the_layout(token):
    """Element, CSS, the blocking head script and both storage keys."""
    main = MAIN.read_text(encoding='utf-8')
    # Comments explaining the removal are allowed; live code is not.
    code = '\n'.join(ln for ln in main.splitlines()
                     if not ln.lstrip().startswith('#'))
    assert token not in code, (
        '%r is still live in web/main.py' % token)


@pytest.mark.parametrize('token', ['.citation-footer', 'cite-compact', 'cite-off'])
def test_no_footer_css_remains(token):
    css = '\n'.join(
        ln for ln in CSS.read_text(encoding='utf-8').splitlines()
        if '*' not in ln and not ln.lstrip().startswith('/*'))
    assert token not in css, '%r is still in common.css' % token


def test_the_layout_renders_the_chip():
    main = MAIN.read_text(encoding='utf-8')
    assert 'render_citation_chip(' in main
    assert 'from web.citation_chip import render_citation_chip' in main


def test_the_reasoning_is_reachable_from_the_chip():
    """The removed citation MODAL's argument has to be reachable, not just filed.

    The modal said WHY citing matters -- more citations let the MiDRASH team win
    grants and widen the work -- and that argument moved to
    `/about#citing-midrash` when the modal came out. Two comments then justified
    the removal by claiming "the footer's label now links there"; the footer was
    removed in the same session, so for a while nothing linked the anchor at all
    and the argument was reachable only by scrolling /about.

    Both halves of the claim existed independently -- about.py had the anchor,
    the comments named it -- which is exactly why nothing failed. Asserted
    against CODE, not file text, for that reason: a comment mentioning the
    anchor is what made the gap invisible in the first place.
    """
    code = _code_only(CHIP)
    assert '/about#citing-midrash' in code, (
        "the chip does not link `/about#citing-midrash`, so the citation "
        "modal's grant/funding argument is unreachable except by scrolling "
        "/about -- which is the state two comments in this module's history "
        "already wrongly claimed was fixed")

    about = (REPO / 'web' / 'pages' / 'about.py').read_text(encoding='utf-8')
    assert about.count('id="citing-midrash"') == 2, (
        'the anchor must exist in BOTH language branches of /about (found %d); '
        'a link into a missing anchor scrolls nowhere'
        % about.count('id="citing-midrash"'))


def test_the_chip_is_not_printed():
    """The printed sheet carries its own masthead and citation; a picture of the
    chip on paper is furniture.

    Checked against CODE, not the file text. The first version of this asserted
    `'print-hide' in CHIP.read_text(...)` and a mutation battery caught it
    passing while the class had been REMOVED from the element -- the string
    still occurred in this module's own comments. Same trap as
    `test_the_chip_registry_is_per_client_not_per_user` below.
    """
    code = _code_only(CHIP)
    chip_decl = [ln for ln in code.splitlines() if "citation-chip" in ln]
    assert chip_decl, 'no citation-chip element declared'
    assert any('print-hide' in ln for ln in chip_decl), (
        'the chip element does not carry print-hide, so it prints itself: %r'
        % chip_decl)


def test_the_chip_sits_below_dialogs():
    """z-index 2000, matching the footer it replaces: above page content, below
    Quasar's dialogs at 6000. A citation chip must never cover a dialog."""
    src = CHIP.read_text(encoding='utf-8')
    m = re.search(r'z-index:\s*(\d+)', src)
    assert m, 'the chip declares no z-index'
    assert int(m.group(1)) < 6000, (
        'z-index %s would put the chip over Quasar dialogs' % m.group(1))


def test_the_chip_opens_on_click_not_only_on_hover():
    """The owner's "hovering chip" meant floating-and-always-visible. The panel
    still has to be reachable by tap and keyboard: the site's own accessibility
    statement claims keyboard navigability, and both prior floating-control
    precedents here are click-triggered."""
    src = CHIP.read_text(encoding='utf-8')
    assert 'ui.menu(' in src, 'the panel is not a menu, so it may be hover-only'
    assert ':hover' not in src, 'the chip opens on hover'


def test_the_chip_registry_is_per_client_not_per_user():
    """`app.storage.user` is shared across all of a reader's tabs, so a
    manuscript open in one tab would rewrite the chip in another."""
    code = _code_only(CHIP)
    assert 'ui.context.client.id' in code
    assert 'app.storage.user' not in code, (
        'the chip keys its registry on per-USER storage, which is shared across '
        'a reader\'s tabs')


def test_chip_updates_are_guarded():
    """A dead layout must not take a page down, and must be forgotten.

    This codebase has a history of `parent_slot has been deleted`; a citation is
    never worth raising over. Previously this asserted that the strings
    `client_gone` and `RuntimeError` appeared somewhere in the module -- which
    the IMPORT line alone satisfies, so deleting the guard and keeping the
    import left it green (Codex review). It now runs the real function.
    """
    from unittest import mock

    from web import citation_chip as chip

    class _Client:
        id = 'test-client-guarded'

    calls = []

    def _raises(_citation):
        calls.append('called')
        raise RuntimeError('parent_slot has been deleted')

    chip._UPDATERS[_Client.id] = _raises
    try:
        with mock.patch.object(chip.ui, 'context',
                               mock.Mock(client=_Client())):
            # Must not raise: the surface survives a dead chip.
            chip.set_page_citation(None)
        assert calls == ['called'], 'the updater was never invoked'
        assert _Client.id not in chip._UPDATERS, (
            'a client whose layout is gone was left in the registry, so every '
            'later update retries it forever')
    finally:
        chip._UPDATERS.pop(_Client.id, None)


def test_a_live_chip_still_receives_its_citation():
    """THE CONTROL for the guard above.

    A `set_page_citation` that swallowed everything -- or never called the
    updater at all -- would satisfy "does not raise" perfectly.
    """
    from unittest import mock

    from web import citation_chip as chip

    class _Client:
        id = 'test-client-live'

    received = []
    chip._UPDATERS[_Client.id] = received.append
    try:
        with mock.patch.object(chip.ui, 'context',
                               mock.Mock(client=_Client())):
            chip.set_page_citation(None)
        assert received == [None]
        assert _Client.id in chip._UPDATERS, (
            'a healthy client was dropped from the registry')
    finally:
        chip._UPDATERS.pop(_Client.id, None)
