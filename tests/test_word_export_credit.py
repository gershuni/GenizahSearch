# -*- coding: utf-8 -*-
"""The browse Word export must credit the transcription it actually exported.

Owner, 2026-09-04: "If Word export is referring to ms text so yes fix it too."
It is — `export_browse_word` writes the transcription itself via
`add_hebrew_paragraph`, on both the single-page and the all-pages path — so the
condition is met and the credit has to follow the source.

THE TRAP THIS FILE EXISTS TO GUARD
----------------------------------
`add_word_credits` has THREE callers: this browse export, the search-results
export and the parallels export. The other two list many manuscripts and have no
"what is on screen" concept at all, so an unconditional MiDRASH credit is right
for them; `CREDITS_TEXT` is also asserted directly by
`tests/test_export_service.py::TestCreditsText`. So the fix is a SIBLING
function, and the tests below pin both halves: the browse export uses the new
one, and the other two exports are untouched.
"""

from __future__ import annotations

import io
import re
from pathlib import Path

import pytest

from shared.export_utils import MIDRASH_CREDIT_LINES

REPO = Path(__file__).resolve().parents[1]
EXPORT_SERVICE = REPO / 'web' / 'export_service.py'
BROWSE = REPO / 'web' / 'pages' / 'browse.py'

#: ISO in (it crosses JSON session storage); the owner's format out.
DATE_ISO = '2026-09-04'
DATE_SHOWN_EN = 'Sept. 4, 2026'
DATE_SHOWN_HE = '4 בספטמבר, 2026'


def _docx_text(blob: bytes) -> str:
    import zipfile
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        xml = z.read('word/document.xml').decode('utf-8')
    return re.sub(r'<[^>]+>', ' ', xml.replace('</w:p>', '\n'))


def _payload(**over):
    data = {
        'shelfmark': 'T-S Ar.50.74',
        'title': 'רשימת הפטרות',
        'sys_id': '990051324530205171',
        'view_all': False,
        'library_code': 'CUL',
        'library_name': 'Cambridge University Library',
        'p_num': 1,
        'text': 'א.ה ואב\nנן ובני כהנים גדלים',
        'lang': 'en',
        'version_info': None,
        'folio_label': '1r',
        'page_url': 'https://genizahsearch.com/browse?shelfmark=T-S+Ar.50.74',
        'retrieved_on': DATE_ISO,
    }
    data.update(over)
    return data


def _export(**over):
    pytest.importorskip('docx')
    from web.export_service import ExportService
    blob, _name = ExportService(None).export_browse_word(_payload(**over))
    return _docx_text(blob)


# ---------------------------------------------------------------------------
# The credit follows the source
# ---------------------------------------------------------------------------

def test_the_automatic_transcription_credits_midrash():
    text = _export(version_info=None)
    assert 'MiDRASH' in text
    assert 'zenodo' in text.lower()


def test_a_pgp_edition_credits_the_scholar_and_not_midrash():
    """The defect: a reader exporting Goitein's edition got a machine credited
    for his work."""
    text = _export(version_info={
        'source': 'pgp', 'attribution': 'S. D. Goitein, unpublished editions.'})
    assert 'Goitein' in text
    assert 'MiDRASH' not in text
    assert 'zenodo' not in text.lower()


def test_an_fgp_edition_credits_friedberg():
    text = _export(version_info={
        'source': 'fgp', 'source_credit': 'Prof. Example (FGP)'})
    assert 'Friedberg' in text
    assert 'MiDRASH' not in text


def test_a_correction_credits_the_corrector_and_midrash():
    text = _export(version_info={'source': 'user', 'author': 'A Reader'})
    assert 'A Reader' in text
    assert 'MiDRASH' in text


def test_an_all_pages_export_credits_the_automatic_transcription():
    """Full Manuscript View has no per-page version chooser, so every folio's
    stored text is what was exported. The browse page sends `version_info=None`
    for that path deliberately; this pins that the export honours it even if a
    stale source were somehow attached."""
    text = _export(view_all=True,
                   pages=[{'p_num': 1, 'text': 'א', 'full_header': 'h'}],
                   version_info={'source': 'pgp', 'attribution': 'Someone'})
    assert 'MiDRASH' in text
    assert 'Someone' not in text


# ---------------------------------------------------------------------------
# The rest of the document
# ---------------------------------------------------------------------------

def test_the_transcription_text_is_still_exported():
    """The credit work must not have disturbed what the export is FOR."""
    text = _export()
    assert 'ובני כהנים' in text


def test_the_citation_carries_the_retrieval_date_and_the_page_url():
    text = _export()
    assert DATE_SHOWN_EN in text, 'the docx has no reader-facing retrieval date'
    assert DATE_ISO not in text, 'the raw ISO transport date leaked into the docx'
    assert 'genizahsearch.com/browse' in text


def test_the_date_is_localized_in_the_docx_too():
    assert DATE_SHOWN_HE in _export(lang='he')


def test_the_credit_is_localized():
    he = _export(lang='he')
    assert re.search(r'[֐-׿]', he), 'no Hebrew in a Hebrew-language export'
    en = _export(lang='en')
    assert 'retrieved' in en.lower()


def test_the_document_carries_the_full_author_list_EXACTLY_ONCE():
    """The sentence now carries the seventeen names, and so does the row below.

    This function printed the one-sentence citation and then every row of
    `credit.citation_lines`, on the explicit reasoning (its own comment) that
    the sentence abbreviated to "et al." and a document has room for the full
    list. Since the owner's 2026-09-05 ruling the sentence IS the full list, so
    printing the rows unfiltered put the same seventeen names on one page twice,
    back to back -- the "too much duplicacy" the citation was collapsed into one
    sentence to fix.

    Counted on a mid-list name, not on the whole string: the two renderings
    differ in punctuation and prefix, so comparing the full citation would miss
    a near-duplicate that a reader would still see as the same list twice.
    """
    text = _export()
    assert text.count('Olszowy-Schlanger') == 1, (
        'the MiDRASH author list appears %d times in one .docx; the sentence '
        'and the rows beneath it are both printing it'
        % text.count('Olszowy-Schlanger'))
    # And it is genuinely THERE -- a filter that dropped both would also give 0
    # and must not read as success.
    assert 'Stoekl Ben Ezra' in text


def test_the_row_the_sentence_does_NOT_carry_is_still_printed():
    """THE CONTROL for the de-duplication above, aimed at the one row it can lose.

    A first version of this control exported a PGP edition and asserted the
    scholar and the source URL were present. It passed against `rows = []` --
    because for PGP the SENTENCE already names both, so the assertion was
    satisfied by text the filter never touches. A control that cannot fail is
    not a control; mutation testing is what showed it.

    The discriminating row is MiDRASH's "Data Source:" line. Of the three rows
    for the automatic transcription, two ARE now inside the sentence (the DOI
    and the full author list) and are correctly dropped; this one is not, so it
    is the only thing standing between "de-duplicate" and "delete the block".
    """
    text = _export(version_info=None)
    assert 'Data Source:' in text, (
        'the credits block lost the one row the sentence does not carry -- the '
        'filter is deleting rows rather than de-duplicating them')

    # And the redundant ones really are gone: the sentence has the DOI, so a
    # bare "Dataset: <doi>" row beneath it would be the duplication again.
    assert 'Dataset: https://doi.org/' not in text
    assert 'Citation: Stoekl Ben Ezra' not in text


def test_a_pgp_export_carries_the_scholar_and_the_url():
    """For a non-MiDRASH source every row IS inside the sentence, so the block
    disappears entirely -- and nothing is lost, which is what this pins.

    Worth stating because "the Credits block vanished" looks like a bug in a
    diff. It is the filter working: the sentence says everything the rows said.
    """
    text = _export(version_info={'source': 'pgp',
                                 'attribution': 'Friedman, M. A.',
                                 'pgp_url': 'https://geniza.princeton.edu/x'})
    assert 'Friedman, M. A.' in text
    assert 'geniza.princeton.edu/x' in text
    assert 'MiDRASH' not in text


# ---------------------------------------------------------------------------
# The other two exports are untouched
# ---------------------------------------------------------------------------

def test_the_browse_export_uses_the_sibling_not_the_shared_credit():
    src = EXPORT_SERVICE.read_text(encoding='utf-8')
    browse_fn = src.split('def export_browse_word', 1)
    assert len(browse_fn) == 2, 'export_browse_word is gone'
    body = browse_fn[1].split('\n    def ', 1)[0]
    assert 'add_word_source_credits(' in body
    assert 'add_word_credits(doc)' not in body, (
        'the browse export still prints the unconditional MiDRASH credit')


def test_the_other_two_exports_still_use_the_unconditional_credit():
    """Search results and parallels list MANY manuscripts; there is no single
    "source on screen" for them, so their credit must not change."""
    src = EXPORT_SERVICE.read_text(encoding='utf-8')
    for fn in ('export_search_results_word', 'export_parallels_word'):
        assert 'def %s' % fn in src
        body = src.split('def %s' % fn, 1)[1].split('\n    def ', 1)[0]
        assert 'add_word_credits(doc)' in body, (
            '%s no longer prints the shared credit block' % fn)


def test_the_shared_credit_function_kept_its_signature():
    """Three callers and an existing test assert `CREDITS_TEXT`'s shape."""
    src = EXPORT_SERVICE.read_text(encoding='utf-8')
    assert 'def add_word_credits(doc: Document) -> None:' in src, (
        'add_word_credits changed shape; its other two callers and '
        'tests/test_export_service.py::TestCreditsText depend on it')
    assert list(MIDRASH_CREDIT_LINES), 'the canonical citation rows are gone'


# ---------------------------------------------------------------------------
# The payload plumbing
# ---------------------------------------------------------------------------

@pytest.mark.parametrize('key', [
    'lang', 'version_info', 'folio_label', 'page_url', 'retrieved_on'])
def test_the_browse_page_sends_the_key_the_export_reads(key):
    """The payload crosses from the page to a separate FastAPI route through
    server-side session storage, so a misspelling on either side fails silently
    -- the export would just print a default. Both ends are pinned."""
    browse = BROWSE.read_text(encoding='utf-8')
    service = EXPORT_SERVICE.read_text(encoding='utf-8')
    assert "'%s':" % key in browse, 'browse.py does not send %r' % key
    assert "'%s'" % key in service, 'the export does not read %r' % key


def test_the_source_travels_through_the_refs_dict():
    """`version_info` is a closure local ~3400 lines from `export_browse_data`.
    `enrichment_refs` is the dict both reach, and it is cleared on navigation so
    a stale manuscript's source cannot be attached to a new one's export."""
    browse = BROWSE.read_text(encoding='utf-8')
    assert "enrichment_refs['current_version_info'] = version_info" in browse
    assert "enrichment_refs.get('current_version_info')" in browse
