"""Phase 94 EXPORT-META-09: desktop xlsx 3-sheet structure parity.

Offline tests for the desktop xlsx-write logic. The xlsx-write logic is
factored into a module-level helper :func:`genizah_app._build_search_results_xlsx_bytes`
that returns bytes (no Qt UI dependencies); these tests exercise it directly
with fake meta_resolver + sanitize_fn + stubbed dossier helpers.

Wave 4 Task 1 — see :file:`.planning/phases/94-adding-pgp-to-downloaded-data/94-04-PLAN.md`.
"""
from io import BytesIO

import openpyxl
import pytest


def _meta_resolver_fake(sid):
    if not sid:
        return None
    return {
        'shelfmark': f'T-S {sid[-4:]}',
        'title': f'Title {sid}',
        'library_code': 'CUL',
        'library_name': 'Cambridge University Library',
    }


def _identity_sanitize(text):
    # Tests don't need real sanitization; pass through.
    return '' if text is None else str(text)


def _make_result(sys_id, snippet='foo *bar* baz', img='1r', source='ms',
                 full_text='page text'):
    return {
        'display': {
            'id': sys_id, 'shelfmark': f'T-S {sys_id[-4:]}',
            'title': f'Title {sys_id}', 'library_code': 'CUL',
            'img': img, 'source': source,
        },
        'snippet': snippet,
        'raw_file_hl': snippet,
        'full_text': full_text,
    }


@pytest.fixture
def stub_dossier(monkeypatch):
    from shared import export_dossier
    monkeypatch.setattr(export_dossier, 'pgp_subset_for_sys_id', lambda s: {
        'pgp_url': f'https://pgp.example/{s}',
        'description': f'PGP desc {s}', 'document_type': 'Letter',
        'date_display': '1100', 'languages': ['Hebrew', 'Aramaic'],
        'tags': ['letter'],
    } if s else None)
    monkeypatch.setattr(export_dossier, 'nli_subset_for_sys_id', lambda s: {
        'catalog_entry': f'Neubauer {s[-4:]}',
        'library_viewer_url': f'https://cudl.example/{s}',
    } if s else None)
    monkeypatch.setattr(export_dossier, 'catalog_summary_for_sys_id', lambda s: {
        'title': f'CatTitle {s}', 'author_text': 'Author',
        'copy_date': '1180', 'copy_place': 'Fustat',
    } if s else None)
    monkeypatch.setattr(export_dossier, 'bibliography_for_sys_id', lambda s: [{
        'running_title': 'Med. Soc.', 'title_year': 1967, 'mention_page': '123',
        'article_name': 'Letter', 'article_author_eng': 'Goitein',
        'catalog_acronym': 'MS',
    }] if s == '99001234567890' else [])


def _build(results, **kwargs):
    from genizah_app import _build_search_results_xlsx_bytes
    return _build_search_results_xlsx_bytes(
        results=results,
        headers_main=['System ID', 'Library', 'Shelfmark', 'Title',
                      'Image/Page', 'Source', 'Snippet', 'Full Text',
                      'Has PGP', 'Is Printed', 'Domains', 'IIIF Manifest'],
        meta_resolver=_meta_resolver_fake,
        sanitize_fn=_identity_sanitize,
        **kwargs,
    )


def _load(content):
    return openpyxl.load_workbook(BytesIO(content), rich_text=True)


def _find_header_row(ws):
    for r in range(1, 25):
        if ws.cell(r, 1).value == 'System ID':
            return r
    return None


def test_workbook_has_3_sheets_in_order(stub_dossier):
    # MUST-FIX 94-04-A: sheet name is now ENGLISH-LOCKED 'Genizah Results'
    # (matches web for EXPORT-META-09 parity).
    content = _build([_make_result('99001234567890')])
    wb = _load(content)
    assert wb.sheetnames == ['Genizah Results', 'Manuscripts', 'Bibliography']
    assert wb.active.title == 'Genizah Results'


def test_main_sheet_12_columns_unified_order(stub_dossier):
    content = _build([_make_result('A')])
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    assert header_row is not None
    headers = [ws.cell(header_row, c).value for c in range(1, 13)]
    assert headers == [
        'System ID', 'Library', 'Shelfmark', 'Title',
        'Image/Page', 'Source', 'Snippet', 'Full Text',
        'Has PGP', 'Is Printed', 'Domains', 'IIIF Manifest',
    ]


def test_has_pgp_and_is_printed_yes_or_empty(stub_dossier):
    content = _build(
        [_make_result('A'), _make_result('B')],
        transcription_sys_ids={'A'},
        printed_ids={'B'},
    )
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    # Row A is header_row + 1, Row B is header_row + 2
    assert ws.cell(header_row + 1, 9).value == 'Yes'
    assert ws.cell(header_row + 1, 10).value in ('', None)
    assert ws.cell(header_row + 2, 9).value in ('', None)
    assert ws.cell(header_row + 2, 10).value == 'Yes'


def test_domains_pipe_joined(stub_dossier):
    content = _build(
        [_make_result('A')],
        result_domains={'A': ['Bible', 'Letter', 'Legal']},
    )
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    assert ws.cell(header_row + 1, 11).value == 'Bible|Letter|Legal'


def test_iiif_manifest_empty(stub_dossier):
    content = _build([_make_result('A')])
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    assert ws.cell(header_row + 1, 12).value in ('', None)


def test_full_text_column(stub_dossier):
    content = _build([_make_result('A', full_text='specific page text')])
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    assert ws.cell(header_row + 1, 8).value == 'specific page text'


def test_full_text_excerpt_fallback(stub_dossier):
    # When 'full_text' missing but 'full_text_excerpt' present, the excerpt is used.
    res = _make_result('A', full_text='')
    res.pop('full_text', None)
    res['full_text_excerpt'] = 'excerpt only'
    content = _build([res])
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    assert ws.cell(header_row + 1, 8).value == 'excerpt only'


def test_manuscripts_dedupe(stub_dossier):
    content = _build([_make_result('A'), _make_result('A'), _make_result('B')])
    wb = _load(content)
    ws = wb['Manuscripts']
    assert ws.cell(2, 1).value == 'A'
    assert ws.cell(3, 1).value == 'B'
    assert ws.cell(4, 1).value is None


def test_bibliography_zero_rows_when_empty(stub_dossier):
    # stub returns [] for sys_id != '99001234567890'
    content = _build([_make_result('A')])
    wb = _load(content)
    ws = wb['Bibliography']
    assert ws.cell(2, 1).value is None


def test_bibliography_has_row_when_entries(stub_dossier):
    content = _build([_make_result('99001234567890')])
    wb = _load(content)
    ws = wb['Bibliography']
    assert ws.cell(2, 1).value == '99001234567890'
    assert ws.cell(2, 3).value == 'Goitein'


def test_conditional_rtl_he(stub_dossier):
    content = _build([_make_result('A')], lang='he')
    wb = _load(content)
    for name in ['Genizah Results', 'Manuscripts', 'Bibliography']:
        assert wb[name].sheet_view.rightToLeft is True


def test_conditional_rtl_en(stub_dossier):
    content = _build([_make_result('A')], lang='en')
    wb = _load(content)
    for name in ['Genizah Results', 'Manuscripts', 'Bibliography']:
        assert wb[name].sheet_view.rightToLeft in (False, None)


def test_rich_snippet_renders(stub_dossier):
    from openpyxl.cell.rich_text import CellRichText
    content = _build([_make_result('A', snippet='foo *bar* baz')])
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    snippet_cell = ws.cell(header_row + 1, 7).value
    assert isinstance(snippet_cell, CellRichText)


def test_main_headers_english_locked_regardless_of_locale(stub_dossier):
    # MUST-FIX 94-04-B: main-sheet headers are English-locked literals.
    # Even when desktop locale is 'he', the headers row contains the
    # English strings (NOT Hebrew translations). EXPORT-META-09 parity
    # requires byte-identical header rows between web and desktop.
    # The fixture's _build() passes a literal English headers_main list;
    # verify the resulting workbook reflects exactly those literals.
    content = _build([_make_result('A')], lang='he')
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    assert header_row is not None
    headers = [ws.cell(header_row, c).value for c in range(1, 13)]
    assert headers == [
        'System ID', 'Library', 'Shelfmark', 'Title',
        'Image/Page', 'Source', 'Snippet', 'Full Text',
        'Has PGP', 'Is Printed', 'Domains', 'IIIF Manifest',
    ], f"headers were {headers}"


def test_full_text_fetcher_hydrates_when_missing(stub_dossier):
    # MUST-FIX 94-04-D: when the result row lacks full_text /
    # full_text_excerpt, the helper invokes full_text_fetcher(uid).
    from genizah_app import _build_search_results_xlsx_bytes
    results = [{
        'uid': 'IE_x_P_y_FL_z',
        'display': {'id': 'A', 'shelfmark': 'T-S 1', 'title': 'T'},
        'snippet': '',
        'raw_file_hl': '',
        # No 'full_text' or 'full_text_excerpt' keys (PGP tag row case).
    }]
    content = _build_search_results_xlsx_bytes(
        results=results,
        headers_main=['System ID', 'Library', 'Shelfmark', 'Title',
                      'Image/Page', 'Source', 'Snippet', 'Full Text',
                      'Has PGP', 'Is Printed', 'Domains', 'IIIF Manifest'],
        meta_resolver=_meta_resolver_fake,
        sanitize_fn=_identity_sanitize,
        full_text_fetcher=lambda uid: 'hydrated text for ' + uid,
    )
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    # Full Text column is col 8.
    cell = ws.cell(header_row + 1, 8).value
    assert 'hydrated text' in (cell or ''), f"expected hydrated text, got {cell!r}"


def test_full_text_fetcher_none_means_empty_cell(stub_dossier):
    # MUST-FIX 94-04-D: when fetcher is None and stored text is empty,
    # Full Text cell renders as empty (no exception).
    from genizah_app import _build_search_results_xlsx_bytes
    results = [{
        'uid': 'IE_x_P_y_FL_z',
        'display': {'id': 'A', 'shelfmark': 'T-S 1', 'title': 'T'},
        'snippet': '',
    }]
    content = _build_search_results_xlsx_bytes(
        results=results,
        headers_main=['System ID', 'Library', 'Shelfmark', 'Title',
                      'Image/Page', 'Source', 'Snippet', 'Full Text',
                      'Has PGP', 'Is Printed', 'Domains', 'IIIF Manifest'],
        meta_resolver=_meta_resolver_fake,
        sanitize_fn=_identity_sanitize,
        full_text_fetcher=None,
    )
    wb = _load(content)
    ws = wb['Genizah Results']
    header_row = _find_header_row(ws)
    assert ws.cell(header_row + 1, 8).value in ('', None)


def test_manuscripts_headers_match_shared_constants(stub_dossier):
    from shared.export_dossier import MANUSCRIPT_HEADERS, BIBLIOGRAPHY_HEADERS
    content = _build([_make_result('A')])
    wb = _load(content)
    manu_headers = [wb['Manuscripts'].cell(1, c).value for c in range(1, 15)]
    bib_headers = [wb['Bibliography'].cell(1, c).value for c in range(1, 9)]
    assert manu_headers == list(MANUSCRIPT_HEADERS)
    assert bib_headers == list(BIBLIOGRAPHY_HEADERS)


def test_credit_text_preserved_above_headers(stub_dossier):
    content = _build(
        [_make_result('A')],
        credit_text='Credit line 1\nCredit line 2',
        search_info_text='Query: foo\nMode: text',
    )
    wb = _load(content)
    ws = wb['Genizah Results']
    # Header row is found by scanning for 'System ID' — should be > 1
    header_row = _find_header_row(ws)
    assert header_row > 1, f"header row should be after credit+info rows; got {header_row}"
    # Row 1 should contain 'Credit line 1' (or some credit text)
    assert 'Credit' in (ws.cell(1, 1).value or '')
