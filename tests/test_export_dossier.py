"""Tests for shared.export_dossier (Phase 94 Wave 1).

Covers:
- 4 lookup helpers (pgp_subset_for_sys_id, nli_subset_for_sys_id,
  catalog_summary_for_sys_id, bibliography_for_sys_id).
- _split_pgp_languages internal (comma-string-to-list bug fix).
- 2 row-emitters (build_manuscript_row, build_bibliography_rows).
- Module-level header constants (MANUSCRIPT_HEADERS, BIBLIOGRAPHY_HEADERS).
- D-02 boundary: no transcription / full-text fields in helper output.
- Codex MUST-FIX disposition: real FJMS bib field names; NLI Catalog Entry
  naming; get_catalog_records (NOT get_catalog_detail); manuscript row does
  NOT call bibliography helper.
- MUST-FIX 94-01-A: module-scope factory imports (so monkeypatch targets
  exist at 'shared.export_dossier.<factory>').
- MUST-FIX 94-01-B: unknown library code graceful fallback.

Per Phase 88 D-02 Refinement 6 + Phase 94 Pattern 1.9: instance-isolated
SimpleNamespace stubs; monkeypatch the factory hook at module scope.
"""
import logging

from shared.export_dossier import (
    BIBLIOGRAPHY_HEADERS,
    MANUSCRIPT_HEADERS,
    _split_pgp_languages,
    bibliography_for_sys_id,
    bibliography_header_row,
    build_bibliography_rows,
    build_manuscript_row,
    catalog_summary_for_sys_id,
    main_header_row,
    manuscript_header_row,
    nli_subset_for_sys_id,
    pgp_subset_for_sys_id,
    sheet_titles,
)


# ---------------------------------------------------------------------------
# Fake service classes
# ---------------------------------------------------------------------------


class _FakeNli:
    def __init__(
        self,
        catalog_entry=None,
        viewer=None,
        available=True,
        raises=False,
    ):
        self._catalog_entry = catalog_entry
        self._viewer = viewer
        self._available = available
        self._raises = raises

    def is_available(self):
        return self._available

    def get_catalog_entry(self, sys_id):
        if self._raises:
            raise RuntimeError("boom")
        return self._catalog_entry

    def get_library_viewer_url(self, sys_id):
        if self._raises:
            raise RuntimeError("boom")
        return self._viewer


class _FakeFjms:
    def __init__(self, records=None, bib=None, available=True, raises=False):
        self._records = records if records is not None else []
        self._bib = bib if bib is not None else []
        self._available = available
        self._raises = raises

    def is_available(self):
        return self._available

    def get_catalog_records(self, sys_id):
        if self._raises:
            raise RuntimeError("boom")
        return self._records

    def get_catalog_detail(self, sys_id):
        # D-02 boundary protection. If a future regression causes the dossier
        # module to call this method (which reads `full_texts`), the test
        # will fail loudly here.
        raise AssertionError(
            "D-02 boundary: dossier MUST NOT call get_catalog_detail "
            "(Codex MUST-FIX 3)"
        )

    def get_bibliography(self, sys_id):
        if self._raises:
            raise RuntimeError("boom")
        return self._bib


# ---------------------------------------------------------------------------
# TestSplitPgpLanguages — _split_pgp_languages internal
# ---------------------------------------------------------------------------


class TestSplitPgpLanguages:
    def test_comma_string_becomes_list(self):
        # T-94-04: the SUPERSEDED-v2 bug — list(string) iterates chars.
        # The helper must split on comma first.
        result = _split_pgp_languages('Hebrew, Aramaic, Judeo-Arabic')
        assert result == ['Hebrew', 'Aramaic', 'Judeo-Arabic']

    def test_list_input_pass_through(self):
        result = _split_pgp_languages(['Hebrew', 'Aramaic'])
        assert result == ['Hebrew', 'Aramaic']

    def test_empty_string(self):
        assert _split_pgp_languages('') == []

    def test_none_input(self):
        assert _split_pgp_languages(None) == []

    def test_single_value_no_comma(self):
        assert _split_pgp_languages('Hebrew') == ['Hebrew']

    def test_strips_whitespace(self):
        assert _split_pgp_languages('  Hebrew  ,  Aramaic  ') == ['Hebrew', 'Aramaic']

    def test_filters_empty_substrings(self):
        # 'a,,b' -> ['a', 'b'] not ['a', '', 'b']
        assert _split_pgp_languages('Hebrew,,Aramaic') == ['Hebrew', 'Aramaic']

    def test_list_with_falsy_entries_filtered(self):
        assert _split_pgp_languages(['Hebrew', '', None, 'Aramaic']) == ['Hebrew', 'Aramaic']

    def test_non_str_non_list_returns_empty(self):
        assert _split_pgp_languages(123) == []
        assert _split_pgp_languages({'a': 1}) == []


# ---------------------------------------------------------------------------
# TestPgpSubset — pgp_subset_for_sys_id
# ---------------------------------------------------------------------------


class TestPgpSubset:
    def test_missing_sidecar_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: None,
        )
        assert pgp_subset_for_sys_id('99001234567890') is None

    def test_happy_path_returns_six_keys(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'https://pgp.example/123',
                'description': 'A letter',
                'document_type': 'Letter',
                'languages_primary': ['Hebrew'],
                'languages_secondary': ['Aramaic'],
                'tags': ['letter', 'legal'],
                'inferred_date_display': '1100',
                'doc_date_standard': '1100',
                'doc_date_original': 'Early 12th c.',
            },
        )
        result = pgp_subset_for_sys_id('99001234567890')
        assert isinstance(result, dict)
        assert set(result.keys()) == {
            'pgp_url', 'description', 'document_type',
            'date_display', 'languages', 'tags',
        }
        assert result['pgp_url'] == 'https://pgp.example/123'
        assert result['description'] == 'A letter'
        assert result['document_type'] == 'Letter'
        assert result['date_display'] == '1100'
        assert result['languages'] == ['Hebrew', 'Aramaic']
        assert result['tags'] == ['letter', 'legal']

    def test_date_fallback_chain(self, monkeypatch):
        # inferred_date_display absent -> use doc_date_standard
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'd',
                'document_type': 't',
                'languages_primary': [],
                'languages_secondary': [],
                'tags': [],
                'inferred_date_display': None,
                'doc_date_standard': '1100',
                'doc_date_original': 'Early 12th c.',
            },
        )
        result = pgp_subset_for_sys_id('99001234567890')
        assert result['date_display'] == '1100'

        # All three None -> date_display is None
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'd',
                'document_type': 't',
                'languages_primary': [],
                'languages_secondary': [],
                'tags': [],
                'inferred_date_display': None,
                'doc_date_standard': None,
                'doc_date_original': None,
            },
        )
        result = pgp_subset_for_sys_id('99001234567890')
        assert result['date_display'] is None

        # Only doc_date_original set -> falls through to it
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'd',
                'document_type': 't',
                'languages_primary': [],
                'languages_secondary': [],
                'tags': [],
                'inferred_date_display': None,
                'doc_date_standard': None,
                'doc_date_original': 'Early 12th c.',
            },
        )
        result = pgp_subset_for_sys_id('99001234567890')
        assert result['date_display'] == 'Early 12th c.'

    def test_split_pgp_languages_comma_string_bug_fix(self, monkeypatch):
        # T-94-04: language values come from pgp.db as comma-separated TEXT
        # in production (per get_document_for_fragment + json_columns=('tags',)
        # only). list() on a string iterates chars. Helper must split on ','.
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'd',
                'document_type': 't',
                'languages_primary': 'Hebrew, Aramaic, Judeo-Arabic',
                'languages_secondary': '',
                'tags': [],
                'inferred_date_display': '1100',
                'doc_date_standard': '1100',
                'doc_date_original': '1100',
            },
        )
        result = pgp_subset_for_sys_id('99001234567890')
        assert result['languages'] == ['Hebrew', 'Aramaic', 'Judeo-Arabic']

    def test_dedupes_secondary_languages(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'd',
                'document_type': 't',
                'languages_primary': ['Hebrew', 'Aramaic'],
                'languages_secondary': ['Aramaic', 'Judeo-Arabic'],
                'tags': [],
                'inferred_date_display': '1100',
                'doc_date_standard': '1100',
                'doc_date_original': '1100',
            },
        )
        result = pgp_subset_for_sys_id('99001234567890')
        assert result['languages'] == ['Hebrew', 'Aramaic', 'Judeo-Arabic']

    def test_empty_sys_id_returns_none(self, monkeypatch):
        called = []

        def trap(sys_id, page_num=None):
            called.append(sys_id)
            return {}

        monkeypatch.setattr('shared.export_dossier.get_document_for_fragment', trap)
        assert pgp_subset_for_sys_id('') is None
        assert pgp_subset_for_sys_id(None) is None
        # Helper short-circuits before calling service
        assert called == []

    def test_exception_resilience(self, monkeypatch, caplog):
        def raises(sys_id, page_num=None):
            raise RuntimeError("simulated sidecar failure")

        monkeypatch.setattr('shared.export_dossier.get_document_for_fragment', raises)
        with caplog.at_level(logging.WARNING, logger='shared.export_dossier'):
            result = pgp_subset_for_sys_id('99001234567890')
        assert result is None
        # Some warning logged via the module logger
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any('pgp_subset_for_sys_id' in r.message for r in warnings)

    def test_no_transcription_text_leak(self, monkeypatch):
        # T-94-02 D-02 boundary: dossier helpers MUST drop transcription / full_text
        # keys even when upstream dict contains them. WHITELIST projection.
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'd',
                'document_type': 't',
                'languages_primary': ['Hebrew'],
                'languages_secondary': [],
                'tags': [],
                'inferred_date_display': '1100',
                'doc_date_standard': '1100',
                'doc_date_original': '1100',
                # Upstream LEAK candidates — helper must not expose:
                'page_section_text': 'LEAKED CONTENT',
                'transcription': 'LEAKED TRANSCRIPTION',
                'full_text': 'LEAKED FULL_TEXT',
            },
        )
        result = pgp_subset_for_sys_id('99001234567890')
        assert 'page_section_text' not in result
        assert 'transcription' not in result
        assert 'full_text' not in result
        # Regex-style assertion on the keys set
        keys_str = str(set(result.keys()))
        assert 'page_section_text' not in keys_str
        assert 'transcription' not in keys_str
        assert 'full_text' not in keys_str


# ---------------------------------------------------------------------------
# TestNliSubset — nli_subset_for_sys_id
# ---------------------------------------------------------------------------


class TestNliSubset:
    def test_factory_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_nli_crossref_service',
            lambda thread_safe=True: None,
        )
        assert nli_subset_for_sys_id('99001234567890') is None

    def test_service_not_available(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_nli_crossref_service',
            lambda thread_safe=True: _FakeNli(available=False),
        )
        assert nli_subset_for_sys_id('99001234567890') is None

    def test_happy_path(self, monkeypatch):
        viewer = {
            'url': 'https://cudl.example/manuscript/T-S-12-123',
            'label': 'Cambridge Digital Library',
            'library_abbrev': 'CUL',
            'library_name_eng': 'Cambridge University Library',
        }
        monkeypatch.setattr(
            'shared.export_dossier.get_nli_crossref_service',
            lambda thread_safe=True: _FakeNli(
                catalog_entry='Neubauer - Cowley 2603.1',
                viewer=viewer,
            ),
        )
        result = nli_subset_for_sys_id('99001234567890')
        assert isinstance(result, dict)
        # Codex MUST-FIX 2: column is 'NLI Catalog Entry'.
        assert set(result.keys()) == {'catalog_entry', 'library_viewer_url'}
        assert result['catalog_entry'] == 'Neubauer - Cowley 2603.1'
        # Only the URL string, not the whole dict
        assert result['library_viewer_url'] == 'https://cudl.example/manuscript/T-S-12-123'

    def test_only_catalog_entry_present(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_nli_crossref_service',
            lambda thread_safe=True: _FakeNli(
                catalog_entry='Neubauer - Cowley 2603.1',
                viewer=None,
            ),
        )
        result = nli_subset_for_sys_id('99001234567890')
        assert result == {
            'catalog_entry': 'Neubauer - Cowley 2603.1',
            'library_viewer_url': None,
        }

    def test_only_viewer_present(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_nli_crossref_service',
            lambda thread_safe=True: _FakeNli(
                catalog_entry=None,
                viewer={'url': 'https://cudl.example'},
            ),
        )
        result = nli_subset_for_sys_id('99001234567890')
        assert result == {
            'catalog_entry': None,
            'library_viewer_url': 'https://cudl.example',
        }

    def test_both_missing_returns_none(self, monkeypatch):
        # "No data at all" -> None (NOT a dict with two None values).
        monkeypatch.setattr(
            'shared.export_dossier.get_nli_crossref_service',
            lambda thread_safe=True: _FakeNli(catalog_entry=None, viewer=None),
        )
        assert nli_subset_for_sys_id('99001234567890') is None

    def test_empty_sys_id_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_nli_crossref_service',
            lambda thread_safe=True: _FakeNli(
                catalog_entry='x',
                viewer={'url': 'y'},
            ),
        )
        assert nli_subset_for_sys_id('') is None
        assert nli_subset_for_sys_id(None) is None

    def test_exception_resilience(self, monkeypatch, caplog):
        monkeypatch.setattr(
            'shared.export_dossier.get_nli_crossref_service',
            lambda thread_safe=True: _FakeNli(raises=True),
        )
        with caplog.at_level(logging.WARNING, logger='shared.export_dossier'):
            result = nli_subset_for_sys_id('99001234567890')
        assert result is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any('nli_subset_for_sys_id' in r.message for r in warnings)


# ---------------------------------------------------------------------------
# TestCatalogSummary — catalog_summary_for_sys_id
# ---------------------------------------------------------------------------


class TestCatalogSummary:
    def test_uses_get_catalog_records_not_get_catalog_detail(self, monkeypatch):
        # Codex MUST-FIX 3: D-02 boundary. The _FakeFjms.get_catalog_detail
        # raises AssertionError if called.
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(
                records=[{
                    'title': 'Mishneh Torah',
                    'title_heb': '',
                    'author_text': 'Maimonides',
                    'copy_date': '1180',
                    'copy_place': 'Fustat',
                }],
            ),
        )
        # If the helper accidentally calls get_catalog_detail, AssertionError
        # would propagate through the try/except in helper as a warning + None.
        # So we just verify the happy result here.
        result = catalog_summary_for_sys_id('99001234567890')
        assert result is not None
        assert result['title'] == 'Mishneh Torah'

    def test_factory_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: None,
        )
        assert catalog_summary_for_sys_id('99001234567890') is None

    def test_service_not_available(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(available=False),
        )
        assert catalog_summary_for_sys_id('99001234567890') is None

    def test_empty_records_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[]),
        )
        assert catalog_summary_for_sys_id('99001234567890') is None

    def test_aggregation_first_non_empty_per_field(self, monkeypatch):
        # Aggregation strategy: first non-empty per field across all records.
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[
                {
                    'title': 'Mishneh Torah',
                    'title_heb': '',
                    'author_text': 'Maimonides',
                    'copy_date': None,
                    'copy_place': '',
                },
                {
                    'title': '',
                    'title_heb': '',
                    'author_text': None,
                    'copy_date': '1180',
                    'copy_place': 'Fustat',
                },
            ]),
        )
        result = catalog_summary_for_sys_id('99001234567890')
        assert result == {
            'title': 'Mishneh Torah',
            'author_text': 'Maimonides',
            'copy_date': '1180',
            'copy_place': 'Fustat',
        }

    def test_hebrew_fallback_for_title(self, monkeypatch):
        # D-04: English first, Hebrew fallback only when English absent.
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[
                {
                    'title': '',
                    'title_heb': 'משנה תורה',
                    'author_text': None,
                    'copy_date': None,
                    'copy_place': None,
                },
            ]),
        )
        result = catalog_summary_for_sys_id('99001234567890')
        assert result is not None
        assert result['title'] == 'משנה תורה'

    def test_all_fields_none_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[
                {
                    'title': None,
                    'title_heb': None,
                    'author_text': None,
                    'copy_date': None,
                    'copy_place': None,
                },
            ]),
        )
        assert catalog_summary_for_sys_id('99001234567890') is None

    def test_empty_sys_id_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[{'title': 'x'}]),
        )
        assert catalog_summary_for_sys_id('') is None
        assert catalog_summary_for_sys_id(None) is None

    def test_exception_resilience(self, monkeypatch, caplog):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(raises=True),
        )
        with caplog.at_level(logging.WARNING, logger='shared.export_dossier'):
            result = catalog_summary_for_sys_id('99001234567890')
        assert result is None
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any('catalog_summary_for_sys_id' in r.message for r in warnings)


# ---------------------------------------------------------------------------
# TestBibliography — bibliography_for_sys_id
# ---------------------------------------------------------------------------


class TestBibliography:
    def test_real_fjms_field_names_only(self, monkeypatch):
        # Codex MUST-FIX 1: REAL FJMS bib field names.
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[{
                'running_title': 'Med. Soc.',
                'title_year': 1967,
                'mention_page': '123',
                'article_name': 'Letter to Goitein',
                'article_author_eng': 'Goitein',
                'catalog_acronym': 'MedSoc',
                # Extra source fields that the helper MUST drop:
                'title_acronym': 'MedSocAcr',
                'volume': '1',
                'comment': 'unused',
                'note_for_display': 'unused',
                'catalog_entry': 'unused',
                'article_author_heb': 'גויטיין',
            }]),
        )
        result = bibliography_for_sys_id('99001234567890')
        assert isinstance(result, list)
        assert len(result) == 1
        entry = result[0]
        assert set(entry.keys()) == {
            'running_title', 'title_year', 'mention_page',
            'article_name', 'article_author_eng', 'catalog_acronym',
        }
        assert entry['running_title'] == 'Med. Soc.'
        assert entry['title_year'] == 1967
        assert entry['mention_page'] == '123'
        assert entry['article_name'] == 'Letter to Goitein'
        assert entry['article_author_eng'] == 'Goitein'
        assert entry['catalog_acronym'] == 'MedSoc'

    def test_empty_returns_empty_list(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[]),
        )
        assert bibliography_for_sys_id('99001234567890') == []

    def test_factory_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: None,
        )
        assert bibliography_for_sys_id('99001234567890') == []

    def test_service_not_available(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(available=False),
        )
        assert bibliography_for_sys_id('99001234567890') == []

    def test_multiple_entries(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[
                {'running_title': 'A', 'title_year': 1900, 'mention_page': '1',
                 'article_name': '', 'article_author_eng': '', 'catalog_acronym': ''},
                {'running_title': 'B', 'title_year': 1901, 'mention_page': '2',
                 'article_name': '', 'article_author_eng': '', 'catalog_acronym': ''},
                {'running_title': 'C', 'title_year': 1902, 'mention_page': '3',
                 'article_name': '', 'article_author_eng': '', 'catalog_acronym': ''},
            ]),
        )
        result = bibliography_for_sys_id('99001234567890')
        assert len(result) == 3
        assert [e['running_title'] for e in result] == ['A', 'B', 'C']

    def test_empty_sys_id_returns_empty_list(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[{'running_title': 'x'}]),
        )
        assert bibliography_for_sys_id('') == []
        assert bibliography_for_sys_id(None) == []

    def test_exception_resilience(self, monkeypatch, caplog):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(raises=True),
        )
        with caplog.at_level(logging.WARNING, logger='shared.export_dossier'):
            result = bibliography_for_sys_id('99001234567890')
        assert result == []
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any('bibliography_for_sys_id' in r.message for r in warnings)

    def test_no_transcription_leak(self, monkeypatch):
        # D-02 boundary: even if upstream returns extended fields, the helper
        # whitelists to exactly 6 keys.
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[{
                'running_title': 'A',
                'title_year': 1900,
                'mention_page': '1',
                'article_name': '',
                'article_author_eng': '',
                'catalog_acronym': '',
                # Schema-extended (D-02 prohibited from dossier surfaces):
                'comment': 'LEAKED',
                'note_for_display': 'LEAKED',
                'catalog_entry': 'LEAKED',
            }]),
        )
        result = bibliography_for_sys_id('99001234567890')
        assert len(result) == 1
        # Whitelist semantics: only the 6 D-08 keys.
        assert 'comment' not in result[0]
        assert 'note_for_display' not in result[0]
        assert 'catalog_entry' not in result[0]


# ---------------------------------------------------------------------------
# TestModuleHeaders — module-level constants
# ---------------------------------------------------------------------------


class TestModuleHeaders:
    def test_manuscript_headers_is_list_of_14_strings(self):
        assert isinstance(MANUSCRIPT_HEADERS, list)
        assert len(MANUSCRIPT_HEADERS) == 14
        assert all(isinstance(h, str) for h in MANUSCRIPT_HEADERS)

    def test_bibliography_headers_is_list_of_8_strings(self):
        assert isinstance(BIBLIOGRAPHY_HEADERS, list)
        assert len(BIBLIOGRAPHY_HEADERS) == 8
        assert all(isinstance(h, str) for h in BIBLIOGRAPHY_HEADERS)

    def test_manuscript_headers_includes_nli_catalog_entry(self):
        # Codex MUST-FIX 2: column is 'NLI Catalog Entry' (NOT 'NLI Description').
        assert 'NLI Catalog Entry' in MANUSCRIPT_HEADERS
        assert 'NLI Description' not in MANUSCRIPT_HEADERS

    def test_bibliography_headers_does_not_include_publisher(self):
        # SUPERSEDED-v2 invented {Author, Publisher, Source Name} — Codex MUST-FIX 1.
        assert 'Publisher' not in BIBLIOGRAPHY_HEADERS
        assert 'Source Name' not in BIBLIOGRAPHY_HEADERS


# ---------------------------------------------------------------------------
# TestBuildManuscriptRow — Task 2 helper
# ---------------------------------------------------------------------------


class TestBuildManuscriptRow:
    def _meta_resolver(self, sys_id):
        return {
            'shelfmark': 'T-S 12.123',
            'title': 'Test Manuscript',
            'library_code': 'CUL',
            'library_name': 'Cambridge University Library',
        }

    def test_row_length_matches_headers(self, monkeypatch):
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)
        row = build_manuscript_row('99001234567890', self._meta_resolver)
        assert len(row) == len(MANUSCRIPT_HEADERS), (
            f"row has {len(row)} cells, headers has {len(MANUSCRIPT_HEADERS)}"
        )

    def test_row_order_and_content(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.pgp_subset_for_sys_id',
            lambda s, **kw: {
                'pgp_url': 'https://pgp.example/123',
                'description': 'A letter',
                'document_type': 'Letter',
                'date_display': '1100',
                'languages': ['Hebrew', 'Aramaic'],
                'tags': ['letter', 'legal'],
            },
        )
        monkeypatch.setattr(
            'shared.export_dossier.nli_subset_for_sys_id',
            lambda s, **kw: {
                'catalog_entry': 'Neubauer 2603.1',
                'library_viewer_url': 'https://cudl.example',
            },
        )
        monkeypatch.setattr(
            'shared.export_dossier.catalog_summary_for_sys_id',
            lambda s, **kw: {
                'title': 'Mishneh Torah',
                'author_text': 'Maimonides',
                'copy_date': '1180',
                'copy_place': 'Fustat',
            },
        )
        row = build_manuscript_row('99001234567890', self._meta_resolver)
        assert row[0] == '99001234567890'
        assert row[1] == 'T-S 12.123'
        assert row[2] == 'Cambridge University Library'
        assert row[3] == 'Test Manuscript'
        assert row[4] == 'https://pgp.example/123'
        assert row[5] == 'A letter'
        assert row[6] == 'Letter'
        assert row[7] == '1100'
        assert row[8] == 'Hebrew|Aramaic'
        assert row[9] == 'letter|legal'
        assert row[10] == 'Neubauer 2603.1'
        assert 'Title: Mishneh Torah' in row[11]
        assert 'Author: Maimonides' in row[11]
        assert 'Date: 1180' in row[11]
        assert 'Place: Fustat' in row[11]
        assert row[12] == 'https://cudl.example'
        assert row[13] == 'https://genizahsearch.com/browse?sys_id=99001234567890'

    def test_does_not_call_bibliography_helper(self, monkeypatch):
        # Codex MUST-FIX 4: manuscript row does NOT call bibliography helper.
        def trap(_, **kw):
            raise AssertionError("MUST NOT be called (Codex MUST-FIX 4)")

        monkeypatch.setattr('shared.export_dossier.bibliography_for_sys_id', trap)
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)
        row = build_manuscript_row('99001234567890', self._meta_resolver)
        assert len(row) == 14  # If bibliography was called, AssertionError would fire

    def test_pipe_joined_languages_no_spaces(self, monkeypatch):
        # D-05: pipe character, NO surrounding spaces.
        monkeypatch.setattr(
            'shared.export_dossier.pgp_subset_for_sys_id',
            lambda s, **kw: {
                'pgp_url': '', 'description': '', 'document_type': '',
                'date_display': '',
                'languages': ['Hebrew', 'Aramaic', 'Judeo-Arabic'],
                'tags': [],
            },
        )
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)
        row = build_manuscript_row('99001234567890', self._meta_resolver)
        assert row[8] == 'Hebrew|Aramaic|Judeo-Arabic'

    def test_pipe_joined_tags_no_spaces(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.pgp_subset_for_sys_id',
            lambda s, **kw: {
                'pgp_url': '', 'description': '', 'document_type': '',
                'date_display': '',
                'languages': [],
                'tags': ['letter', 'legal'],
            },
        )
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)
        row = build_manuscript_row('99001234567890', self._meta_resolver)
        assert row[9] == 'letter|legal'

    def test_missing_data_renders_empty_cells(self, monkeypatch):
        # D-06: empty cells for missing data — never 'N/A' / '—' / 'None'.
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)
        # meta_resolver returns None for the sys_id
        row = build_manuscript_row('99001234567890', lambda s: None)
        assert row[0] == '99001234567890'
        assert row[1] == ''  # shelfmark
        assert row[2] == ''  # library
        assert row[3] == ''  # title
        for i in range(4, 13):
            assert row[i] == '', f"cell {i} should be empty when no data, got {row[i]!r}"
        # GenizahSearch URL still populated
        assert row[13].startswith('https://genizahsearch.com')

    def test_meta_resolver_none_returns_empty_meta_cells(self, monkeypatch):
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)
        row = build_manuscript_row('99001234567890', None)
        assert row[0] == '99001234567890'
        assert row[1] == ''
        assert row[2] == ''
        assert row[3] == ''
        # GenizahSearch URL still populated
        assert row[13] == 'https://genizahsearch.com/browse?sys_id=99001234567890'

    def test_genizah_search_url_verbatim(self, monkeypatch):
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)
        row = build_manuscript_row('ABC123', self._meta_resolver)
        assert row[13] == 'https://genizahsearch.com/browse?sys_id=ABC123'

    def test_unknown_library_code_graceful_fallback(self, monkeypatch):
        # MUST-FIX 94-01-B: unknown library codes pass through get_library_display
        # unchanged (LIBRARY_CODES.get(code, code) fallback at
        # genizah_core.py:1820-1838).
        from genizah_core import get_library_display as core_get_library_display
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)

        def _meta(sys_id):
            lib_code = 'UNKNOWN_XYZ'
            return {
                'shelfmark': 'T-S 99.99',
                'title': 'Test',
                'library_code': lib_code,
                'library_name': core_get_library_display(lib_code, short=False, lang='en'),
            }

        row = build_manuscript_row('99001234567890', _meta)
        # Graceful fallback: library_name cell equals input code.
        assert row[2] == 'UNKNOWN_XYZ', (
            f"expected graceful fallback to input code, got {row[2]!r}"
        )


# ---------------------------------------------------------------------------
# TestBuildBibliographyRows — Task 2 helper
# ---------------------------------------------------------------------------


class TestBuildBibliographyRows:
    def _meta_resolver(self, sys_id):
        return {
            'shelfmark': 'T-S 12.123',
            'title': '',
            'library_code': '',
            'library_name': '',
        }

    def test_empty_when_no_entries(self, monkeypatch):
        monkeypatch.setattr('shared.export_dossier.bibliography_for_sys_id', lambda s, **kw: [])
        rows = build_bibliography_rows('99001234567890', self._meta_resolver)
        assert rows == []

    def test_row_length_and_order(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.bibliography_for_sys_id',
            lambda s, **kw: [{
                'running_title': 'Med. Soc.',
                'title_year': 1967,
                'mention_page': '123',
                'article_name': 'Letter to Goitein',
                'article_author_eng': 'Goitein',
                'catalog_acronym': 'MedSoc',
            }],
        )
        rows = build_bibliography_rows('99001234567890', self._meta_resolver)
        assert len(rows) == 1
        row = rows[0]
        assert len(row) == len(BIBLIOGRAPHY_HEADERS) == 8
        assert row[0] == '99001234567890'
        assert row[1] == 'T-S 12.123'
        assert row[2] == 'Goitein'
        assert row[3] == 'Letter to Goitein'
        assert row[4] == 'Med. Soc.'
        assert row[5] == 1967
        assert row[6] == '123'
        assert row[7] == 'MedSoc'

    def test_multiple_entries(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.bibliography_for_sys_id',
            lambda s, **kw: [
                {'running_title': 'A', 'title_year': 1900, 'mention_page': '1',
                 'article_name': 'a1', 'article_author_eng': 'auth_a',
                 'catalog_acronym': 'CA'},
                {'running_title': 'B', 'title_year': 1901, 'mention_page': '2',
                 'article_name': 'b1', 'article_author_eng': 'auth_b',
                 'catalog_acronym': 'CB'},
            ],
        )
        rows = build_bibliography_rows('99001234567890', self._meta_resolver)
        assert len(rows) == 2
        assert all(len(r) == 8 for r in rows)
        assert rows[0][4] == 'A'
        assert rows[1][4] == 'B'

    def test_does_not_call_other_helpers(self, monkeypatch):
        # Codex MUST-FIX 4: bibliography row only calls helper 4.
        def trap(_, **kw):
            raise AssertionError("MUST NOT be called (Codex MUST-FIX 4)")

        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', trap)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', trap)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', trap)
        monkeypatch.setattr('shared.export_dossier.bibliography_for_sys_id', lambda s, **kw: [])
        rows = build_bibliography_rows('99001234567890', self._meta_resolver)
        assert rows == []

    def test_meta_resolver_none_blank_shelfmark(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.bibliography_for_sys_id',
            lambda s, **kw: [{
                'running_title': 'A', 'title_year': 1900, 'mention_page': '1',
                'article_name': '', 'article_author_eng': '', 'catalog_acronym': '',
            }],
        )
        rows = build_bibliography_rows('99001234567890', lambda s: None)
        assert len(rows) == 1
        assert rows[0][0] == '99001234567890'
        assert rows[0][1] == ''  # shelfmark empty when meta_resolver returns None

    def test_empty_sys_id_returns_empty(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.bibliography_for_sys_id',
            lambda s, **kw: [{'running_title': 'x', 'title_year': 1, 'mention_page': '1',
                        'article_name': '', 'article_author_eng': '',
                        'catalog_acronym': ''}],
        )
        rows = build_bibliography_rows('', self._meta_resolver)
        # bibliography_for_sys_id('') returns []; rows is []
        assert rows == []


# ---------------------------------------------------------------------------
# Bilingual API (D-04 REVISED 2026-05-20)
# ---------------------------------------------------------------------------


class TestBilingualHeaderRows:
    """Coverage for the new lang-aware header / sheet-title helpers.

    Pinned via independent test class so any future Hebrew translation drift
    is caught loudly. Hebrew strings match the canonical entries already
    present in ``genizah_translations.TRANSLATIONS`` (used by ``tr()``).
    """

    # English contract — back-compat with prior callers reading constants directly.
    def test_main_header_row_en_returns_12_columns(self):
        row = main_header_row('en')
        assert len(row) == 12
        assert row[0] == "System ID"
        assert row[1] == "Library"
        assert row[2] == "Shelfmark"
        assert row[3] == "Title"
        assert row[4] == "Image/Page"
        assert row[5] == "Source"
        assert row[6] == "Snippet"
        assert row[7] == "Full Text"
        assert row[8] == "Has PGP"
        assert row[9] == "Is Printed"
        assert row[10] == "Domains"
        assert row[11] == "Image URL"

    def test_main_header_row_he_returns_12_hebrew_columns(self):
        row = main_header_row('he')
        assert len(row) == 12
        assert row[0] == "מספר מערכת"
        assert row[1] == "ספרייה"
        assert row[2] == "מספר מדף"
        assert row[3] == "כותרת"
        assert row[4] == "תמונה/עמוד"
        assert row[5] == "מקור"
        assert row[6] == "קטע"
        assert row[7] == "טקסט מלא"
        assert row[8] == "יש PGP"
        assert row[9] == "מודפס"
        assert row[10] == "תחומים"
        assert row[11] == "כתובת תמונה"

    def test_main_header_row_default_lang_is_english(self):
        assert main_header_row() == main_header_row('en')

    def test_main_header_row_returns_fresh_copy(self):
        # Callers can mutate the returned list without affecting future returns.
        row = main_header_row('en')
        row[0] = 'mutated'
        assert main_header_row('en')[0] == "System ID"

    def test_manuscript_header_row_en_matches_constant(self):
        # Back-compat: the English variant matches MANUSCRIPT_HEADERS verbatim.
        assert manuscript_header_row('en') == list(MANUSCRIPT_HEADERS)

    def test_manuscript_header_row_he_returns_14_hebrew_columns(self):
        row = manuscript_header_row('he')
        assert len(row) == 14
        assert row[0] == "מספר מערכת"
        assert row[1] == "מספר מדף"
        assert row[2] == "ספרייה"
        assert row[3] == "כותרת"
        assert row[4] == "כתובת PGP"
        assert row[5] == "תיאור PGP"
        assert row[6] == "סוג PGP"
        assert row[7] == "תאריך PGP"
        assert row[8] == "שפות PGP"
        assert row[9] == "תגיות PGP"
        assert row[10] == "רשומה בקטלוג הספרייה הלאומית"
        assert row[11] == "תקציר קטלוגי"
        assert row[12] == "קישור לצפייה בספרייה"
        assert row[13] == "קישור ל-GenizahSearch"

    def test_bibliography_header_row_en_matches_constant(self):
        assert bibliography_header_row('en') == list(BIBLIOGRAPHY_HEADERS)

    def test_bibliography_header_row_he_returns_8_hebrew_columns(self):
        row = bibliography_header_row('he')
        assert len(row) == 8
        assert row[0] == "מספר מערכת"
        assert row[1] == "מספר מדף"
        assert row[2] == "מחבר המאמר"
        assert row[3] == "שם המאמר"
        assert row[4] == "כותרת רצה"
        assert row[5] == "שנת הפרסום"
        assert row[6] == "עמוד אזכור"
        assert row[7] == "קיצור הקטלוג"

    def test_sheet_titles_en(self):
        titles = sheet_titles('en')
        # Smoke verification round 2 (2026-05-21): main sheet renamed
        # 'Genizah Results' -> 'Search Results'; new 'credits_info' key for
        # the 4th sheet that holds credits + per-export search metadata.
        # Phase 103 (Plan 01): 'local_documents' key added for the Local
        # Documents sheet in mixed/LOCAL-only exports (D-04/D-05).
        assert titles == {
            'main': "Search Results",
            'manuscripts': "Manuscripts",
            'bibliography': "Bibliography",
            'credits_info': "Credits and Info",
            'local_documents': "Local Documents",
        }

    def test_sheet_titles_he(self):
        titles = sheet_titles('he')
        # Smoke verification round 2 (2026-05-21): main sheet renamed
        # 'תוצאות גניזה' -> 'תוצאות חיפוש'; new 'credits_info' key.
        # Phase 103 (Plan 01): 'local_documents' key added.
        assert titles == {
            'main': "תוצאות חיפוש",
            'manuscripts': "כתבי יד",
            'bibliography': "ביבליוגרפיה",
            'credits_info': "קרדיט ומידע",
            'local_documents': "מסמכים מקומיים",
        }

    def test_sheet_titles_default_lang_is_english(self):
        assert sheet_titles() == sheet_titles('en')

    def test_unknown_lang_falls_back_to_english(self):
        # Defensive default — unknown lang codes don't error, they degrade to EN.
        assert main_header_row('fr') == main_header_row('en')
        assert sheet_titles('fr') == sheet_titles('en')


# ---------------------------------------------------------------------------
# D-04 REVISED — source-language metadata
# ---------------------------------------------------------------------------


class TestPgpSubsetLangPreference:
    """``lang`` parameter on pgp_subset_for_sys_id picks Hebrew translation
    via the existing pgp_translations table when ``lang == 'he'``.
    """

    def test_lang_en_returns_english_description(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'A letter (English)',
                'document_type': 'Letter',
                'languages_primary': [], 'languages_secondary': [],
                'tags': [],
                'inferred_date_display': '1100',
                'doc_date_standard': '1100', 'doc_date_original': '1100',
            },
        )
        # Even if a Hebrew translation exists, lang='en' uses English.
        monkeypatch.setattr(
            'shared.export_dossier._pgp_translation_he_for_sys_id',
            lambda sys_id: {
                'description_he': 'מכתב', 'document_type_he': 'מכתב',
            },
        )
        result = pgp_subset_for_sys_id('99001234567890', lang='en')
        assert result['description'] == 'A letter (English)'
        assert result['document_type'] == 'Letter'

    def test_lang_he_prefers_hebrew_description(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'A letter (English)',
                'document_type': 'Letter',
                'languages_primary': [], 'languages_secondary': [],
                'tags': [],
                'inferred_date_display': '1100',
                'doc_date_standard': '1100', 'doc_date_original': '1100',
            },
        )
        monkeypatch.setattr(
            'shared.export_dossier._pgp_translation_he_for_sys_id',
            lambda sys_id: {
                'description_he': 'מכתב באנגלית', 'document_type_he': 'מכתב',
            },
        )
        result = pgp_subset_for_sys_id('99001234567890', lang='he')
        assert result['description'] == 'מכתב באנגלית'
        assert result['document_type'] == 'מכתב'

    def test_lang_he_falls_back_to_english_when_no_translation(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'A letter (English)',
                'document_type': 'Letter',
                'languages_primary': [], 'languages_secondary': [],
                'tags': [],
                'inferred_date_display': '1100',
                'doc_date_standard': '1100', 'doc_date_original': '1100',
            },
        )
        monkeypatch.setattr(
            'shared.export_dossier._pgp_translation_he_for_sys_id',
            lambda sys_id: None,
        )
        result = pgp_subset_for_sys_id('99001234567890', lang='he')
        # Graceful fallback to English when Hebrew translation is absent.
        assert result['description'] == 'A letter (English)'
        assert result['document_type'] == 'Letter'

    def test_lang_he_partial_translation_falls_back_per_field(self, monkeypatch):
        # description_he present, document_type_he missing -> Hebrew desc,
        # English type.
        monkeypatch.setattr(
            'shared.export_dossier.get_document_for_fragment',
            lambda sys_id, page_num=None: {
                'pgp_url': 'u',
                'description': 'A letter (English)',
                'document_type': 'Letter',
                'languages_primary': [], 'languages_secondary': [],
                'tags': [],
                'inferred_date_display': '1100',
                'doc_date_standard': '1100', 'doc_date_original': '1100',
            },
        )
        monkeypatch.setattr(
            'shared.export_dossier._pgp_translation_he_for_sys_id',
            lambda sys_id: {'description_he': 'מכתב', 'document_type_he': ''},
        )
        result = pgp_subset_for_sys_id('99001234567890', lang='he')
        assert result['description'] == 'מכתב'
        assert result['document_type'] == 'Letter'


class TestCatalogSummaryLangPreference:
    """``lang`` parameter on catalog_summary_for_sys_id picks title_heb
    when ``lang == 'he'``.
    """

    def test_lang_en_prefers_english_title(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[
                {
                    'title': 'Mishneh Torah',
                    'title_heb': 'משנה תורה',
                    'author_text': 'Maimonides',
                    'copy_date': '1180',
                    'copy_place': 'Fustat',
                },
            ]),
        )
        result = catalog_summary_for_sys_id('99001234567890', lang='en')
        assert result['title'] == 'Mishneh Torah'

    def test_lang_he_prefers_hebrew_title(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[
                {
                    'title': 'Mishneh Torah',
                    'title_heb': 'משנה תורה',
                    'author_text': 'Maimonides',
                    'copy_date': '1180',
                    'copy_place': 'Fustat',
                },
            ]),
        )
        result = catalog_summary_for_sys_id('99001234567890', lang='he')
        assert result['title'] == 'משנה תורה'

    def test_lang_he_falls_back_to_english_when_no_hebrew_title(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[
                {
                    'title': 'Mishneh Torah',
                    'title_heb': '',
                    'author_text': 'Maimonides',
                    'copy_date': '1180',
                    'copy_place': 'Fustat',
                },
            ]),
        )
        result = catalog_summary_for_sys_id('99001234567890', lang='he')
        assert result['title'] == 'Mishneh Torah'

    def test_lang_en_falls_back_to_hebrew_when_no_english_title(self, monkeypatch):
        # Symmetric back-compat: previously the helper used English-first /
        # Hebrew-fallback; the revised behavior preserves the fallback path.
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(records=[
                {
                    'title': '',
                    'title_heb': 'משנה תורה',
                    'author_text': None,
                    'copy_date': None,
                    'copy_place': None,
                },
            ]),
        )
        result = catalog_summary_for_sys_id('99001234567890', lang='en')
        assert result['title'] == 'משנה תורה'


class TestBibliographyLangPreference:
    """``lang`` parameter on bibliography_for_sys_id prefers running_title_heb
    and article_author_heb when ``lang == 'he'``.
    """

    def test_lang_en_prefers_english_fields(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[{
                'running_title': 'Med. Soc.',
                'running_title_heb': 'מד. סוס.',
                'title_year': 1967,
                'mention_page': '123',
                'article_name': 'Letter to Goitein',
                'article_author_eng': 'Goitein',
                'article_author_heb': 'גויטיין',
                'catalog_acronym': 'MedSoc',
            }]),
        )
        result = bibliography_for_sys_id('99001234567890', lang='en')
        assert len(result) == 1
        assert result[0]['running_title'] == 'Med. Soc.'
        assert result[0]['article_author_eng'] == 'Goitein'

    def test_lang_he_prefers_hebrew_fields(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[{
                'running_title': 'Med. Soc.',
                'running_title_heb': 'מד. סוס.',
                'title_year': 1967,
                'mention_page': '123',
                'article_name': 'Letter to Goitein',
                'article_author_eng': 'Goitein',
                'article_author_heb': 'גויטיין',
                'catalog_acronym': 'MedSoc',
            }]),
        )
        result = bibliography_for_sys_id('99001234567890', lang='he')
        assert len(result) == 1
        assert result[0]['running_title'] == 'מד. סוס.'
        assert result[0]['article_author_eng'] == 'גויטיין'

    def test_lang_he_falls_back_to_english_when_no_hebrew(self, monkeypatch):
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[{
                'running_title': 'Med. Soc.',
                'running_title_heb': '',
                'title_year': 1967,
                'mention_page': '123',
                'article_name': 'Letter to Goitein',
                'article_author_eng': 'Goitein',
                'article_author_heb': '',
                'catalog_acronym': 'MedSoc',
            }]),
        )
        result = bibliography_for_sys_id('99001234567890', lang='he')
        assert result[0]['running_title'] == 'Med. Soc.'
        assert result[0]['article_author_eng'] == 'Goitein'


class TestBuildManuscriptRowBilingual:
    """``build_manuscript_row`` threads lang through to its 3 helpers AND
    localizes the Catalog Summary cell labels.
    """

    def _meta_resolver_en(self, sys_id):
        return {
            'shelfmark': 'T-S 12.123',
            'title': 'Test Manuscript',
            'library_code': 'CUL',
            'library_name': 'Cambridge University Library',
        }

    def _meta_resolver_he(self, sys_id):
        return {
            'shelfmark': 'T-S 12.123',
            'title': 'Test Manuscript',
            'library_code': 'CUL',
            # Hebrew library name (mimics what get_library_display(lang='he') yields).
            'library_name': 'ספריית האוניברסיטה של קיימברידג\'',
        }

    def test_lang_he_passes_lang_to_pgp_helper(self, monkeypatch):
        seen_lang = []

        def _pgp(sys_id, lang='en'):
            seen_lang.append(('pgp', lang))
            return {
                'pgp_url': '', 'description': '', 'document_type': '',
                'date_display': '', 'languages': [], 'tags': [],
            }

        def _catalog(sys_id, lang='en'):
            seen_lang.append(('catalog', lang))
            return None

        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', _pgp)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', _catalog)
        build_manuscript_row('99001234567890', self._meta_resolver_he, lang='he')
        assert ('pgp', 'he') in seen_lang
        assert ('catalog', 'he') in seen_lang

    def test_lang_he_library_name_from_resolver(self, monkeypatch):
        # The caller is responsible for supplying a lang-aware meta_resolver.
        # Here we verify the row reflects whatever the resolver returned.
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.catalog_summary_for_sys_id', lambda s, **kw: None)
        row = build_manuscript_row('99001234567890', self._meta_resolver_he, lang='he')
        assert row[2] == 'ספריית האוניברסיטה של קיימברידג\''  # column 2 = Library

    def test_lang_he_catalog_summary_uses_hebrew_labels(self, monkeypatch):
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr(
            'shared.export_dossier.catalog_summary_for_sys_id',
            lambda s, **kw: {
                'title': 'משנה תורה',
                'author_text': 'הרמב\"ם',
                'copy_date': '1180',
                'copy_place': 'פסטאט',
            },
        )
        row = build_manuscript_row('99001234567890', self._meta_resolver_he, lang='he')
        cell = row[11]
        # Hebrew labels per D-04 REVISED.
        assert 'כותרת:' in cell
        assert 'מחבר:' in cell
        assert 'תאריך:' in cell
        assert 'מקום:' in cell
        assert 'משנה תורה' in cell
        assert 'פסטאט' in cell
        # NO English labels.
        assert 'Title:' not in cell
        assert 'Author:' not in cell

    def test_lang_en_catalog_summary_uses_english_labels(self, monkeypatch):
        monkeypatch.setattr('shared.export_dossier.pgp_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr('shared.export_dossier.nli_subset_for_sys_id', lambda s, **kw: None)
        monkeypatch.setattr(
            'shared.export_dossier.catalog_summary_for_sys_id',
            lambda s, **kw: {
                'title': 'Mishneh Torah',
                'author_text': 'Maimonides',
                'copy_date': '1180',
                'copy_place': 'Fustat',
            },
        )
        row = build_manuscript_row('99001234567890', self._meta_resolver_en, lang='en')
        cell = row[11]
        assert 'Title: Mishneh Torah' in cell
        assert 'Author: Maimonides' in cell
        # NO Hebrew labels.
        assert 'כותרת:' not in cell


class TestBuildBibliographyRowsBilingual:
    """``build_bibliography_rows`` threads lang through to ``bibliography_for_sys_id``."""

    def _meta_resolver(self, sys_id):
        return {
            'shelfmark': 'T-S 12.123',
            'title': '',
            'library_code': '',
            'library_name': '',
        }

    def test_lang_threads_through(self, monkeypatch):
        seen_lang = []

        def _bib(sys_id, lang='en'):
            seen_lang.append(lang)
            return [{
                'running_title': 'r', 'title_year': 1900,
                'mention_page': '1', 'article_name': 'a',
                'article_author_eng': 'auth', 'catalog_acronym': 'CA',
            }]

        monkeypatch.setattr('shared.export_dossier.bibliography_for_sys_id', _bib)
        build_bibliography_rows('99001234567890', self._meta_resolver, lang='he')
        assert 'he' in seen_lang

    def test_lang_he_hebrew_author_in_row(self, monkeypatch):
        # End-to-end: real bibliography_for_sys_id behavior with mocked service.
        monkeypatch.setattr(
            'shared.export_dossier.get_fjms_service',
            lambda thread_safe=True: _FakeFjms(bib=[{
                'running_title': 'Med. Soc.',
                'running_title_heb': 'מד. סוס.',
                'title_year': 1967,
                'mention_page': '123',
                'article_name': 'Letter',
                'article_author_eng': 'Goitein',
                'article_author_heb': 'גויטיין',
                'catalog_acronym': 'MS',
            }]),
        )
        rows = build_bibliography_rows('99001234567890', self._meta_resolver, lang='he')
        assert len(rows) == 1
        # Column 2 = Article Author, column 4 = Running Title (Hebrew).
        assert rows[0][2] == 'גויטיין'
        assert rows[0][4] == 'מד. סוס.'
