# -*- coding: utf-8 -*-
"""Tests for bibliography helpers in shared/fjms_service.py."""

from shared.fjms_service import (
    format_page_ref, _parse_marc_bib_string,
    _parse_marc_annotations, strip_marc_annotation_suffix, _ts_symbol,
)


# ── format_page_ref ──────────────────────────────────────────────────

class TestFormatPageRef:
    def test_mention_page(self):
        assert format_page_ref({'mention_page': '42'}) == 'p. 42'

    def test_page_range(self):
        assert format_page_ref({'from_page': '10', 'to_page': '20'}) == 'pp. 10-20'

    def test_single_page_from(self):
        assert format_page_ref({'from_page': '10', 'to_page': '10'}) == 'p. 10'

    def test_volume_with_page(self):
        assert format_page_ref({'volume': '3', 'mention_page': '15'}) == 'vol. 3, p. 15'

    def test_volume_only(self):
        assert format_page_ref({'volume': '2'}) == 'vol. 2'

    def test_empty_entry(self):
        assert format_page_ref({}) == ''

    def test_whitespace_values_ignored(self):
        assert format_page_ref({'mention_page': '  ', 'volume': ' '}) == ''

    def test_mention_page_takes_precedence(self):
        """mention_page wins over from_page/to_page."""
        result = format_page_ref({
            'mention_page': '5',
            'from_page': '10',
            'to_page': '20',
        })
        assert result == 'p. 5'

    def test_from_page_only(self):
        assert format_page_ref({'from_page': '7'}) == 'p. 7'


# ── _parse_marc_bib_string ──────────────────────────────────────────

class TestParseMarcBibString:
    def test_basic_english(self):
        result = _parse_marc_bib_string("Goitein, A Mediterranean Society, 1967, p. 234")
        assert result['author'] == 'Goitein'
        assert result['year'] == '1967'
        assert result['pages'] == '234'

    def test_page_range(self):
        result = _parse_marc_bib_string("Smith, Some Title, 2001, pp. 15-20")
        assert result['pages'] == '15-20'

    def test_hebrew_pages(self):
        result = _parse_marc_bib_string("גויטיין, חברה ים-תיכונית, 1967, עמ' 234")
        assert result['pages'] == '234'

    def test_hebrew_page_range(self):
        result = _parse_marc_bib_string("שלום, כתבי יד, עמ' 15-20")
        assert result['pages'] == '15-20'

    def test_year_extraction(self):
        result = _parse_marc_bib_string("Author, Title from 1523 edition")
        assert result['year'] == '1523'

    def test_no_author_long_prefix(self):
        """If comma is too far in (>60 chars), don't extract author."""
        long_prefix = "A" * 65
        result = _parse_marc_bib_string(f"{long_prefix}, some text")
        assert result['author'] == ''

    def test_empty_string(self):
        result = _parse_marc_bib_string('')
        assert result['author'] == ''
        assert result['year'] == ''
        assert result['pages'] == ''

    def test_none_input(self):
        result = _parse_marc_bib_string(None)
        assert result['author'] == ''
        assert result['year'] == ''
        assert result['pages'] == ''


# ── _parse_marc_annotations ─────────────────────────────────────────

class TestParseMarcAnnotations:
    def test_discussion_with_all(self):
        s = "Author, Title. (1900)- \u05e2\u05de\u05d5\u05d3 100 (\u05d3\u05d9\u05d5\u05df, \u05d9\u05e9 \u05ea\u05de\u05d5\u05e0\u05d4, \u05d9\u05e9 \u05d4\u05e2\u05ea\u05e7\u05d4 (\u05de\u05dc\u05d0), \u05d9\u05e9 \u05ea\u05e8\u05d2\u05d5\u05dd (\u05de\u05dc\u05d0))."
        result = _parse_marc_annotations(s)
        assert result['mention_type'] == 'Discussion'
        assert result['has_image'] is True
        assert result['transcription'] == 'Full'
        assert result['translation'] == 'Full'

    def test_mentioned(self):
        s = "Author, Title (\u05d0\u05d9\u05d6\u05db\u05d5\u05e8)."
        result = _parse_marc_annotations(s)
        assert result['mention_type'] == 'Mentioned'
        assert result['has_image'] is False

    def test_partial_transcription(self):
        s = "Author, Title (\u05d3\u05d9\u05d5\u05df, \u05d9\u05e9 \u05d4\u05e2\u05ea\u05e7\u05d4 (\u05d7\u05dc\u05e7\u05d9))."
        result = _parse_marc_annotations(s)
        assert result['transcription'] == 'Partial'
        assert result['translation'] == ''

    def test_no_annotations(self):
        s = "Just a plain string without parens"
        result = _parse_marc_annotations(s)
        assert result['mention_type'] == ''
        assert result['has_image'] is False


# ── strip_marc_annotation_suffix ─────────────────────────────────────

class TestStripMarcAnnotationSuffix:
    def test_strips_hebrew_annotation(self):
        s = "Adler, Some chapters. JQR, 12 (1900)- \u05e2\u05de\u05d5\u05d3 466-480 (\u05d3\u05d9\u05d5\u05df, \u05d9\u05e9 \u05ea\u05de\u05d5\u05e0\u05d4, \u05d9\u05e9 \u05d4\u05e2\u05ea\u05e7\u05d4 (\u05de\u05dc\u05d0), \u05d9\u05e9 \u05ea\u05e8\u05d2\u05d5\u05dd (\u05de\u05dc\u05d0))."
        result = strip_marc_annotation_suffix(s)
        assert '\u05d3\u05d9\u05d5\u05df' not in result
        assert 'Adler' in result
        assert 'JQR' in result

    def test_no_annotation(self):
        s = "Goitein, A Mediterranean Society, 1967, p. 234"
        result = strip_marc_annotation_suffix(s)
        assert result == s

    def test_empty_string(self):
        assert strip_marc_annotation_suffix('') == ''

    def test_none_input(self):
        assert strip_marc_annotation_suffix(None) == ''

    def test_english_parens_preserved(self):
        """Parenthetical block without Hebrew chars should be preserved."""
        s = "Author, Title (2nd edition)"
        result = strip_marc_annotation_suffix(s)
        assert '2nd edition' in result

    def test_simple_mentioned(self):
        s = "Author, Title (\u05d0\u05d9\u05d6\u05db\u05d5\u05e8)."
        result = strip_marc_annotation_suffix(s)
        assert '\u05d0\u05d9\u05d6\u05db\u05d5\u05e8' not in result
        assert 'Author' in result


# ── _ts_symbol ──────────────────────────────────────────────────────

class TestTsSymbol:
    def test_full(self):
        assert _ts_symbol('Full') == '\u2713+'

    def test_partial(self):
        assert _ts_symbol('Partial') == '\u2713\u2212'

    def test_exists(self):
        assert _ts_symbol('Exists') == '\u2713'

    def test_truthy_value(self):
        assert _ts_symbol('SomeValue') == '\u2713'

    def test_none(self):
        assert _ts_symbol(None) == ''

    def test_empty(self):
        assert _ts_symbol('') == ''

    def test_none_string(self):
        assert _ts_symbol('None') == ''

    def test_unknown(self):
        assert _ts_symbol('Unknown') == ''
