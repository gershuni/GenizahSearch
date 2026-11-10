# -*- coding: utf-8 -*-
"""
Smoke tests for shared service layer.

Verifies:
- shared package is importable
- shared.supabase_provider exports get_client and reset_client
- shared.document_service exports all public names (14 functions + PgpService + get_pgp_service)
- web.document_service shim re-exports all public names
- web.supabase_client.get_client still works
- Desktop importability (shared works without web/ on sys.path)
"""


class TestSharedProviderImport:
    """Verify shared.supabase_provider is importable."""

    def test_shared_package_import(self):
        import shared
        assert shared is not None

    def test_shared_provider_exports(self):
        from shared.supabase_provider import get_client, reset_client
        assert callable(get_client)
        assert callable(reset_client)


class TestSharedDocumentServiceImport:
    """Verify all public names importable from shared.document_service."""

    def test_all_functions_importable(self):
        from shared.document_service import (
            get_document_for_fragment,
            get_fragments_for_document,
            get_transcription_for_document,
            get_document_metadata,
            parse_transcription_sections,
            get_section_for_page,
            get_sources_for_document,
            get_all_sources_for_fragment,
            get_editions_for_document,
            get_translations_for_document,
            get_sys_ids_with_transcriptions,
            get_fragments_by_tag,
            get_all_distinct_tags,
            parse_html_sections,
            get_pgp_service,
            PgpService,
        )
        for fn in [get_document_for_fragment, get_fragments_for_document,
                    get_transcription_for_document, get_document_metadata,
                    parse_transcription_sections, get_section_for_page,
                    get_sources_for_document, get_all_sources_for_fragment,
                    get_editions_for_document, get_translations_for_document,
                    get_sys_ids_with_transcriptions, get_fragments_by_tag,
                    get_all_distinct_tags, parse_html_sections,
                    get_pgp_service]:
            assert callable(fn)
        # PgpService is a class, verify it's a type
        assert isinstance(PgpService, type)


class TestWebShimReexports:
    """Verify web.document_service shim re-exports all public names."""

    def test_shim_reexports_all(self):
        from web.document_service import (
            get_document_for_fragment,
            get_fragments_for_document,
            get_transcription_for_document,
            get_document_metadata,
            parse_transcription_sections,
            get_section_for_page,
            get_sources_for_document,
            get_all_sources_for_fragment,
            get_editions_for_document,
            get_translations_for_document,
            get_sys_ids_with_transcriptions,
            get_fragments_by_tag,
            get_all_distinct_tags,
            parse_html_sections,
            get_pgp_service,
        )
        for fn in [get_document_for_fragment, get_fragments_for_document,
                    get_transcription_for_document, get_document_metadata,
                    parse_transcription_sections, get_section_for_page,
                    get_sources_for_document, get_all_sources_for_fragment,
                    get_editions_for_document, get_translations_for_document,
                    get_sys_ids_with_transcriptions, get_fragments_by_tag,
                    get_all_distinct_tags, parse_html_sections,
                    get_pgp_service]:
            assert callable(fn)

    def test_shim_and_shared_are_same_objects(self):
        """Web shim functions must be the exact same objects as shared ones."""
        from web.document_service import get_document_for_fragment as web_fn
        from shared.document_service import get_document_for_fragment as shared_fn
        assert web_fn is shared_fn


class TestWebSupabaseClientStillWorks:
    """Verify web/supabase_client.py get_client still works independently."""

    def test_web_get_client_importable(self):
        from web.supabase_client import get_client
        assert callable(get_client)


class TestDesktopImportability:
    """Verify desktop app can import shared service without web/ dependency."""

    def test_desktop_import_shared_directly(self):
        """Desktop imports shared.document_service, not web.document_service."""
        from shared.document_service import get_document_for_fragment
        assert callable(get_document_for_fragment)


class TestParseHtmlSections:
    """Tests for parse_html_sections -- PGP HTML parser for structured canvas sections."""

    BASIC_TWO_CANVAS_HTML = """<html><body>
<section dir="rtl" lang="jrb">
  <div data-canvas="https://cudl.lib.cam.ac.uk/iiif/MS-TS-00008-J-00027-00016/canvas/1">
    <h3>recto</h3>
    <ol><li>recto line 1</li><li>recto line 2</li></ol>
  </div>
  <div data-canvas="https://cudl.lib.cam.ac.uk/iiif/MS-TS-00008-J-00027-00016/canvas/2">
    <h3>Verso.</h3>
    <ol><li>verso line 1</li><li>verso line 2</li></ol>
  </div>
</section>
</body></html>"""

    def test_basic_two_canvas_document(self):
        """HTML with two data-canvas divs returns 2 sections with correct keys."""
        from shared.document_service import parse_html_sections
        result = parse_html_sections(self.BASIC_TWO_CANVAS_HTML)
        assert len(result['sections']) == 2
        for section in result['sections']:
            assert 'canvas_url' in section
            assert 'canvas_num' in section
            assert 'label' in section
            assert 'text' in section

    def test_canvas_url_extraction(self):
        """Canvas URLs extracted exactly from data-canvas attributes."""
        from shared.document_service import parse_html_sections
        result = parse_html_sections(self.BASIC_TWO_CANVAS_HTML)
        assert result['sections'][0]['canvas_url'] == \
            'https://cudl.lib.cam.ac.uk/iiif/MS-TS-00008-J-00027-00016/canvas/1'
        assert result['sections'][1]['canvas_url'] == \
            'https://cudl.lib.cam.ac.uk/iiif/MS-TS-00008-J-00027-00016/canvas/2'

    def test_canvas_num_from_url(self):
        """Canvas_num parsed from CUDL URL: /canvas/1 -> 1, /canvas/2 -> 2."""
        from shared.document_service import parse_html_sections
        result = parse_html_sections(self.BASIC_TWO_CANVAS_HTML)
        assert result['sections'][0]['canvas_num'] == 1
        assert result['sections'][1]['canvas_num'] == 2

    def test_canvas_num_positional_for_figgy(self):
        """Figgy UUID URLs get positional canvas_num (1, 2, ...)."""
        from shared.document_service import parse_html_sections
        html = """<html><body>
<section dir="rtl" lang="he">
  <div data-canvas="https://figgy.princeton.edu/concern/scanned_resources/abc/manifest/canvas/def-uuid-1">
    <h3>recto</h3>
    <ol><li>line 1</li></ol>
  </div>
  <div data-canvas="https://figgy.princeton.edu/concern/scanned_resources/abc/manifest/canvas/ghi-uuid-2">
    <h3>verso</h3>
    <ol><li>line 2</li></ol>
  </div>
</section>
</body></html>"""
        result = parse_html_sections(html)
        assert result['sections'][0]['canvas_num'] == 1
        assert result['sections'][1]['canvas_num'] == 2

    def test_section_label_from_h3_inside_div(self):
        """h3 INSIDE data-canvas div provides the section label."""
        from shared.document_service import parse_html_sections
        result = parse_html_sections(self.BASIC_TWO_CANVAS_HTML)
        assert result['sections'][0]['label'] == 'recto'
        assert result['sections'][1]['label'] == 'Verso.'

    def test_first_canvas_no_h3(self):
        """First canvas div with no h3 gets label=None (matches PGPID 3750/444)."""
        from shared.document_service import parse_html_sections
        html = """<html><body>
<section dir="rtl" lang="jrb">
  <div data-canvas="https://cudl.lib.cam.ac.uk/iiif/MS-TEST/canvas/1">
    <ol><li>line without header</li></ol>
  </div>
</section>
</body></html>"""
        result = parse_html_sections(html)
        assert result['sections'][0]['label'] is None

    def test_text_from_ordered_list(self):
        """Text extracted from ol/li elements joined with newlines."""
        from shared.document_service import parse_html_sections
        result = parse_html_sections(self.BASIC_TWO_CANVAS_HTML)
        assert result['sections'][0]['text'] == 'recto line 1\nrecto line 2'
        assert result['sections'][1]['text'] == 'verso line 1\nverso line 2'

    def test_text_from_p_elements(self):
        """Standalone p elements inside canvas div captured as text."""
        from shared.document_service import parse_html_sections
        html = """<html><body>
<section dir="rtl" lang="jrb">
  <div data-canvas="https://cudl.lib.cam.ac.uk/iiif/MS-TEST/canvas/1">
    <h3>verso - address</h3>
    <p>address text line 1</p>
    <p>address text line 2</p>
  </div>
</section>
</body></html>"""
        result = parse_html_sections(html)
        assert 'address text line 1' in result['sections'][0]['text']
        assert 'address text line 2' in result['sections'][0]['text']

    def test_li_with_nested_p(self):
        """li containing p (PGPID 7003 pattern) extracts text without duplication."""
        from shared.document_service import parse_html_sections
        html = """<html><body>
<section dir="rtl" lang="jrb">
  <div data-canvas="https://cudl.lib.cam.ac.uk/iiif/MS-TEST/canvas/1">
    <h3>recto</h3>
    <ol><li><p>nested text 1</p></li><li><p>nested text 2</p></li></ol>
  </div>
</section>
</body></html>"""
        result = parse_html_sections(html)
        text = result['sections'][0]['text']
        assert 'nested text 1' in text
        assert 'nested text 2' in text
        # Should NOT have duplicated lines
        assert text.count('nested text 1') == 1

    def test_multiple_subsections_per_canvas(self):
        """Multiple h3+ol pairs in one canvas div merged into single entry."""
        from shared.document_service import parse_html_sections
        html = """<html><body>
<section dir="rtl" lang="jrb">
  <div data-canvas="https://cudl.lib.cam.ac.uk/iiif/MS-TEST/canvas/1">
    <h3>recto</h3>
    <ol><li>main line 1</li><li>main line 2</li></ol>
    <h3>recto - right margin</h3>
    <ol><li>margin line 1</li></ol>
  </div>
</section>
</body></html>"""
        result = parse_html_sections(html)
        # Should be ONE section for canvas 1
        assert len(result['sections']) == 1
        section = result['sections'][0]
        assert section['canvas_num'] == 1
        # Merged text includes sub-section label and all lines
        assert 'main line 1' in section['text']
        assert 'margin line 1' in section['text']
        assert '[recto - right margin]' in section['text']
        # Subsections list should have 2 entries
        assert section['subsections'] is not None
        assert len(section['subsections']) == 2

    def test_language_and_direction(self):
        """Language and direction extracted from section element."""
        from shared.document_service import parse_html_sections
        result = parse_html_sections(self.BASIC_TWO_CANVAS_HTML)
        assert result['language'] == 'jrb'
        assert result['direction'] == 'rtl'

    def test_translation_language(self):
        """English translation section has language=en, direction=ltr."""
        from shared.document_service import parse_html_sections
        html = """<html><body>
<section lang="en" dir="ltr">
  <div data-canvas="https://example.com/canvas/1">
    <h3>recto</h3>
    <ol><li>English text</li></ol>
  </div>
</section>
</body></html>"""
        result = parse_html_sections(html)
        assert result['language'] == 'en'
        assert result['direction'] == 'ltr'

    def test_empty_html(self):
        """Empty string returns empty sections with None metadata."""
        from shared.document_service import parse_html_sections
        result = parse_html_sections('')
        assert result == {'sections': [], 'language': None, 'direction': None}

    def test_html_entities_decoded(self):
        """HTML entities decoded to plain text."""
        from shared.document_service import parse_html_sections
        html = """<html><body>
<section dir="rtl" lang="jrb">
  <div data-canvas="https://example.com/canvas/1">
    <h3>recto</h3>
    <ol><li>text &amp; more &gt; less &hellip;</li></ol>
  </div>
</section>
</body></html>"""
        result = parse_html_sections(html)
        text = result['sections'][0]['text']
        assert 'text & more > less' in text
        # &hellip; decodes to Unicode horizontal ellipsis character
        assert '\u2026' in text


class TestParseTranscriptionSections:
    """Tests for parse_transcription_sections regex fallback -- all marker variants."""

    def test_bare_markers(self):
        """Basic 'Recto\\n' / 'Verso\\n' markers (regression, should pass already)."""
        from shared.document_service import parse_transcription_sections
        text = "Recto\nrecto text\nVerso\nverso text"
        result = parse_transcription_sections(text)
        assert len(result['recto']) > 0
        assert 'recto text' in result['recto'][0]
        assert len(result['verso']) > 0
        assert 'verso text' in result['verso'][0]

    def test_period_marker(self):
        """'Verso.' with trailing period (268 corpus occurrences)."""
        from shared.document_service import parse_transcription_sections
        text = "Recto\nrecto text\nVerso.\nverso text"
        result = parse_transcription_sections(text)
        assert len(result['verso']) > 0
        assert 'verso text' in result['verso'][0]

    def test_period_address_marker(self):
        """'Verso. Address.' with period and qualifier (68 occurrences)."""
        from shared.document_service import parse_transcription_sections
        text = "Recto\nrecto text\nVerso. Address.\naddress text"
        result = parse_transcription_sections(text)
        assert len(result['verso']) > 0
        assert 'address text' in result['verso'][0]

    def test_parenthetical_marker(self):
        """'Verso (address)' with parenthetical qualifier (12 occurrences)."""
        from shared.document_service import parse_transcription_sections
        text = "Recto\nrecto text\nVerso (address)\naddress text"
        result = parse_transcription_sections(text)
        assert len(result['verso']) > 0
        assert 'address text' in result['verso'][0]

    def test_space_modifier_marker(self):
        """'Recto Margin' with space-separated modifier (12 occurrences)."""
        from shared.document_service import parse_transcription_sections
        text = "Recto\nrecto text\nRecto Margin\nmargin text\nVerso\nverso text"
        result = parse_transcription_sections(text)
        # "Recto Margin" should be a recto sub-section
        recto_combined = '\n'.join(result['recto'])
        assert 'margin text' in recto_combined
        assert len(result['verso']) > 0
        assert 'verso text' in result['verso'][0]

    def test_upside_down_marker(self):
        """'Verso (upside down)' with parenthetical (7 occurrences)."""
        from shared.document_service import parse_transcription_sections
        text = "Recto\nrecto text\nVerso (upside down)\nupside down text"
        result = parse_transcription_sections(text)
        assert len(result['verso']) > 0
        assert 'upside down text' in result['verso'][0]

    def test_recto_period_marker(self):
        """'Recto.' with trailing period (7 occurrences)."""
        from shared.document_service import parse_transcription_sections
        text = "Recto.\nrecto text\nVerso\nverso text"
        result = parse_transcription_sections(text)
        assert len(result['recto']) > 0
        assert 'recto text' in result['recto'][0]

    def test_case_insensitive(self):
        """Lowercase 'verso.' matches."""
        from shared.document_service import parse_transcription_sections
        text = "recto\nrecto text\nverso.\nverso text"
        result = parse_transcription_sections(text)
        assert len(result['verso']) > 0
        assert 'verso text' in result['verso'][0]

    def test_preamble_assigned_to_recto(self):
        """Text before first marker goes to recto."""
        from shared.document_service import parse_transcription_sections
        text = "preamble text\nRecto\nrecto text\nVerso\nverso text"
        result = parse_transcription_sections(text)
        recto_combined = '\n'.join(result['recto'])
        assert 'preamble text' in recto_combined

    def test_empty_input(self):
        """Empty string returns empty recto/verso lists."""
        from shared.document_service import parse_transcription_sections
        result = parse_transcription_sections('')
        assert result == {'recto': [], 'verso': []}

    def test_no_markers(self):
        """Text without markers returns all as recto."""
        from shared.document_service import parse_transcription_sections
        result = parse_transcription_sections('some plain text without markers')
        assert len(result['recto']) > 0
        assert 'some plain text without markers' in result['recto'][0]
        assert result['verso'] == []

    def test_get_section_for_page_with_period_marker(self):
        """get_section_for_page correctly splits with period marker."""
        from shared.document_service import get_section_for_page
        text = "Recto\nrecto content here\nVerso.\nverso content here"
        page1 = get_section_for_page(text, 1)
        page2 = get_section_for_page(text, 2)
        assert 'recto content here' in page1
        assert 'verso content here' in page2

    def test_no_false_positive_on_content_line(self):
        """Long content line starting with 'Verso' should NOT be a marker."""
        from shared.document_service import parse_transcription_sections
        # A content line >60 chars after "Verso" should not match
        long_line = "Verso is a concept in poetry that refers to the left-hand page of a manuscript or printed book and is distinct from recto"
        text = f"Recto\nrecto text\n{long_line}\nmore text"
        result = parse_transcription_sections(text)
        # The long line should NOT create a verso section
        assert result['verso'] == []

    def test_word_boundary(self):
        """'Rectory' should NOT match as recto marker."""
        from shared.document_service import parse_transcription_sections
        text = "Rectory\nsome text about a rectory"
        result = parse_transcription_sections(text)
        # No markers matched, so all goes to recto as plain text
        recto_combined = '\n'.join(result['recto'])
        assert 'Rectory' in recto_combined
        # There should only be one recto entry (the full text, no split)
        assert len(result['recto']) == 1


class TestStructuredSectionsIntegration:
    """Integration tests for structured sections display path (canvas-based lookup)."""

    SECTIONS = [
        {"canvas_url": "https://cudl.lib.cam.ac.uk/iiif/MS-TS/canvas/1",
         "canvas_num": 1, "label": None, "text": "structured recto line 1\nstructured recto line 2"},
        {"canvas_url": "https://cudl.lib.cam.ac.uk/iiif/MS-TS/canvas/2",
         "canvas_num": 2, "label": "Verso.", "text": "structured verso line 1\nstructured verso line 2"},
    ]

    # Transcription content that CONTAINS the section text (valid match)
    TRANSCRIPTION = "Recto\nstructured recto line 1\nstructured recto line 2\nVerso\nstructured verso line 1\nstructured verso line 2"

    # Transcription from a DIFFERENT source (section text not found in it)
    OTHER_SOURCE_TRANSCRIPTION = "Recto\nother scholar recto text here\nVerso\nother scholar verso text"

    def test_structured_sections_canvas_lookup(self):
        """Structured sections use canvas_num matching when text matches source."""
        from shared.document_service import get_section_for_page
        result1 = get_section_for_page(self.TRANSCRIPTION, 1, self.SECTIONS)
        assert result1 == "structured recto line 1\nstructured recto line 2"
        result2 = get_section_for_page(self.TRANSCRIPTION, 2, self.SECTIONS)
        assert result2 == "structured verso line 1\nstructured verso line 2"

    def test_structured_sections_override_regex(self):
        """Structured sections take priority over regex when text matches source."""
        from shared.document_service import get_section_for_page
        # The transcription has recto/verso markers that regex would parse,
        # but structured sections should be used instead (text matches)
        result = get_section_for_page(self.TRANSCRIPTION, 1, self.SECTIONS)
        assert 'structured recto' in result

    def test_structured_sections_mismatch_falls_back_to_regex(self):
        """Sections with text from a different source fall back to regex parsing."""
        from shared.document_service import get_section_for_page
        # Sections contain text from one source, but transcription is from another.
        # Validation guard detects mismatch and falls back to regex.
        result = get_section_for_page(self.OTHER_SOURCE_TRANSCRIPTION, 1, self.SECTIONS)
        assert 'other scholar recto' in result
        assert 'structured recto' not in result

    def test_structured_sections_none_falls_back(self):
        """sections=None falls back to regex parsing."""
        from shared.document_service import get_section_for_page
        text = "Recto\nrecto content here\nVerso.\nverso content here"
        page1 = get_section_for_page(text, 1, None)
        page2 = get_section_for_page(text, 2, None)
        assert 'recto content here' in page1
        assert 'verso content here' in page2

    def test_structured_sections_empty_list_falls_back(self):
        """sections=[] (empty list) falls back to regex parsing."""
        from shared.document_service import get_section_for_page
        text = "Recto\nrecto content here\nVerso.\nverso content here"
        page1 = get_section_for_page(text, 1, [])
        page2 = get_section_for_page(text, 2, [])
        assert 'recto content here' in page1
        assert 'verso content here' in page2

    def test_structured_sections_page_beyond_range(self):
        """Page beyond available canvases returns full transcription."""
        from shared.document_service import get_section_for_page
        result = get_section_for_page(self.TRANSCRIPTION, 3, self.SECTIONS)
        assert result == self.TRANSCRIPTION

    def test_structured_sections_multi_canvas(self):
        """Three-canvas document returns correct margin text for page 3."""
        from shared.document_service import get_section_for_page
        margin_text = "margin notes here"
        transcription_3 = self.TRANSCRIPTION + f"\nMargin\n{margin_text}"
        sections_3 = self.SECTIONS + [
            {"canvas_url": "https://cudl.lib.cam.ac.uk/iiif/MS-TS/canvas/3",
             "canvas_num": 3, "label": "Margin", "text": margin_text},
        ]
        result = get_section_for_page(transcription_3, 3, sections_3)
        assert result == margin_text

    def test_source_dict_flow(self):
        """Simulate full consumer flow: source dict with and without sections key."""
        from shared.document_service import get_section_for_page

        # Source WITH sections (structured path, text matches content)
        source_with = {
            'content': self.TRANSCRIPTION,
            'sections': self.SECTIONS,
        }
        result = get_section_for_page(source_with['content'], 1, source_with.get('sections'))
        assert 'structured recto' in result

        # Source WITHOUT sections key (regex fallback)
        source_without = {
            'content': "Recto\nfallback recto\nVerso\nfallback verso",
        }
        result = get_section_for_page(source_without['content'], 1, source_without.get('sections'))
        assert 'fallback recto' in result

    def test_backward_compatibility(self):
        """Calling without sections argument preserves existing behavior."""
        from shared.document_service import get_section_for_page
        text = "Recto\nrecto text here\nVerso\nverso text here"
        # Call without the third argument at all
        page1 = get_section_for_page(text, 1)
        page2 = get_section_for_page(text, 2)
        assert 'recto text here' in page1
        assert 'verso text here' in page2
