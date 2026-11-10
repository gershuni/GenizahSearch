# -*- coding: utf-8 -*-
"""Phase 103 (LEXP-01/LEXP-07) — export_dossier Local Documents bilingual helpers.

Tests pin the exact behavior of:
- local_documents_header_row(lang) — 5 EN/HE column headers
- sheet_titles(lang)['local_documents'] — bilingual sheet title
- build_local_document_row(...) — 5-primitive row builder, raw matched text preserved

D-01: exactly 5 columns (no System ID / no Full Text)
D-13: EN Filename/Parent Folder/Full Filepath/Page/Matched Text;
      HE שם קובץ/תיקייה/נתיב מלא/עמוד/טקסט תואם
D-14: matched_text retains *-markers; filepath resolution is the caller's job
"""


_EN_HEADERS = ['Filename', 'Parent Folder', 'Full Filepath', 'Page', 'Matched Text']
_HE_HEADERS = ['שם קובץ', 'תיקייה', 'נתיב מלא', 'עמוד', 'טקסט תואם']


# ---------------------------------------------------------------------------
# local_documents_header_row tests
# ---------------------------------------------------------------------------


def test_local_headers_en():
    from shared.export_dossier import local_documents_header_row
    assert local_documents_header_row('en') == _EN_HEADERS


def test_local_headers_he():
    from shared.export_dossier import local_documents_header_row
    assert local_documents_header_row('he') == _HE_HEADERS


def test_local_headers_default_en():
    """No-arg call should return the EN list."""
    from shared.export_dossier import local_documents_header_row
    assert local_documents_header_row() == _EN_HEADERS


def test_local_headers_fresh_copy():
    """Mutating the returned list must not affect subsequent calls."""
    from shared.export_dossier import local_documents_header_row
    first = local_documents_header_row('en')
    first[0] = 'MODIFIED'
    second = local_documents_header_row('en')
    assert second[0] == 'Filename', "Mutating returned list should not change module constant"


# ---------------------------------------------------------------------------
# sheet_titles tests
# ---------------------------------------------------------------------------


def test_sheet_titles_local_key():
    from shared.export_dossier import sheet_titles
    assert sheet_titles('en')['local_documents'] == 'Local Documents'
    assert sheet_titles('he')['local_documents'] == 'מסמכים מקומיים'


def test_sheet_titles_existing_keys_preserved():
    """Original 4 keys must remain present in both languages."""
    from shared.export_dossier import sheet_titles
    expected = {'main', 'manuscripts', 'bibliography', 'credits_info'}
    assert expected.issubset(sheet_titles('en').keys())
    assert expected.issubset(sheet_titles('he').keys())


# ---------------------------------------------------------------------------
# build_local_document_row tests
# ---------------------------------------------------------------------------


def test_build_local_document_row_shape():
    """Returns exactly 5 items in header order; item[4] keeps the raw *-markers."""
    from shared.export_dossier import build_local_document_row
    row = build_local_document_row('f.pdf', 'MyDocs', '/a/b/f.pdf', 'p. 3', '*hit* found')
    assert len(row) == 5
    assert row[0] == 'f.pdf'
    assert row[1] == 'MyDocs'
    assert row[2] == '/a/b/f.pdf'
    assert row[3] == 'p. 3'
    assert row[4] == '*hit* found', "Matched text *-markers must be preserved (D-14)"


def test_build_local_document_row_none_safe():
    """All-None input must yield exactly ['', '', '', '', ''] — no None leaks, no 'N/A'."""
    from shared.export_dossier import build_local_document_row
    row = build_local_document_row(None, None, None, None, None)
    assert row == ['', '', '', '', '']


def test_build_local_document_row_sanitize_skips_matched_text():
    """A custom sanitize_fn is applied to items 0-3 but NOT to item[4]."""
    from shared.export_dossier import build_local_document_row

    def upper(x):
        return str(x).upper() if x is not None else ''

    row = build_local_document_row('file.pdf', 'folder', '/x/y/file.pdf', 'p. 1', '*match* text',
                                   sanitize_fn=upper)
    assert row[0] == 'FILE.PDF'
    assert row[1] == 'FOLDER'
    assert row[2] == '/X/Y/FILE.PDF'
    assert row[3] == 'P. 1'
    # item[4] must NOT be uppercased — kept raw for rich-cell rendering (D-03)
    assert row[4] == '*match* text', "sanitize_fn must not touch matched_text (item[4])"
