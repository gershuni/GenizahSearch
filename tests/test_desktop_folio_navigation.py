# -*- coding: utf-8 -*-
"""
Verification tests for desktop folio navigation and source indicators.

Phase 31 / Plan 02: Confirms the desktop app implementation in genizah_app.py
correctly integrates folio-labeled page navigation and source indicator buttons
(KTIV, Cambridge, Oxford) consistent with the web app patterns from Plan 01.

These tests read source code to verify patterns (avoids Qt dependency), and
exercise parse_folio_label / get_folio_images directly where possible.

Success criteria verified:
  1. Browse page combo uses folio labels from crossref (test 1)
  2. KTIV button exists with correct NLI URL pattern (test 2)
  3. Source combo includes page count context (test 3)
  4. Folio label display in browse nav area (test 4)
  5. image_source_info populated in enrich_metadata (test 5)
  6. Real crossref folio labels (functional, if DB available) (test 6)
"""

import os
import re
import pytest


# ---------------------------------------------------------------------------
# Helpers: read source file and extract method sections
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def genizah_app_source():
    """Return the full source code of genizah_app.py as a string."""
    src_path = os.path.join(os.path.dirname(__file__), '..', 'genizah_app.py')
    with open(src_path, 'r', encoding='utf-8') as f:
        return f.read()


@pytest.fixture(scope="module")
def genizah_core_source():
    """Return the full source code of genizah_core.py as a string."""
    src_path = os.path.join(os.path.dirname(__file__), '..', 'genizah_core.py')
    with open(src_path, 'r', encoding='utf-8') as f:
        return f.read()


def _extract_method(source, method_name):
    """Extract a method's source from the full file by finding its def and
    reading until the next def at the same or lower indentation level."""
    pattern = re.compile(r'^( +)def ' + re.escape(method_name) + r'\(', re.MULTILINE)
    match = pattern.search(source)
    if not match:
        return ""
    indent = len(match.group(1))
    start = match.start()
    # Find end: next def at same or lower indent, or end of file
    rest = source[match.end():]
    end_pattern = re.compile(r'^\s{0,' + str(indent) + r'}(?:def |class )', re.MULTILINE)
    end_match = end_pattern.search(rest)
    if end_match:
        return source[start:match.end() + end_match.start()]
    return source[start:]


# ---------------------------------------------------------------------------
# Test 1: Browse combo uses folio labels from crossref
# ---------------------------------------------------------------------------

def test_browse_combo_folio_label_population(genizah_app_source):
    """Verify that combo_browse_page population references folio labels."""
    render_method = _extract_method(genizah_app_source, 'browse_render_page')
    assert render_method, "browse_render_page method not found"

    # Should reference folio_images for label population
    assert '_browse_folio_images' in render_method, \
        "browse_render_page should use _browse_folio_images for combo labels"

    # Should use folio_label from the image data
    assert 'folio_label' in render_method, \
        "browse_render_page should reference folio_label"

    # Should have fallback to generic page numbers
    assert 'range(1, total + 1)' in render_method, \
        "browse_render_page should have fallback to generic page numbers"


# ---------------------------------------------------------------------------
# Test 2: KTIV button exists with correct NLI URL pattern
# ---------------------------------------------------------------------------

def test_msviewer_ktiv_button_exists(genizah_app_source):
    """Verify a KTIV/NLI viewer button is created with the correct URL."""
    # Check KTIV button creation
    assert 'self.btn_ktiv' in genizah_app_source, \
        "ManuscriptViewerWidget should have a btn_ktiv button"

    # Check that it opens the correct NLI URL
    ktiv_method = _extract_method(genizah_app_source, '_open_ktiv_viewer')
    assert ktiv_method, "_open_ktiv_viewer method not found"
    assert 'nli.org.il' in ktiv_method, \
        "_open_ktiv_viewer should open NLI website"
    assert 'ItemID' in ktiv_method, \
        "_open_ktiv_viewer URL should include ItemID parameter"

    # Check KTIV button has styled chip appearance
    assert 'border: 1.5px solid #4caf50' in genizah_app_source, \
        "KTIV button should have green border chip style"


# ---------------------------------------------------------------------------
# Test 3: Source combo includes page count context
# ---------------------------------------------------------------------------

def test_msviewer_source_combo_enhanced(genizah_app_source):
    """Verify combo_source population includes page count context."""
    load_method = _extract_method(genizah_app_source, 'load_images')
    assert load_method, "load_images method not found"

    # Should include "pages" in the combo item text
    assert 'pages)' in load_method, \
        "load_images should include 'pages' suffix in combo_source items"

    # Should have entries for both NLI and external sources with page counts
    assert re.search(r'NLI \(.*pages\)', load_method), \
        "NLI combo items should show page counts"


# ---------------------------------------------------------------------------
# Test 4: Folio label display in browse nav area
# ---------------------------------------------------------------------------

def test_folio_label_in_browse_nav(genizah_app_source):
    """Verify folio label display code exists in browse navigation area."""
    # Check that a folio label widget exists
    assert 'lbl_browse_folio' in genizah_app_source, \
        "Browse nav should have lbl_browse_folio label"

    # Check it's added to nav_bar layout
    assert 'nav_bar.addWidget(self.lbl_browse_folio)' in genizah_app_source, \
        "lbl_browse_folio should be added to nav_bar"

    # Check that page count label exists
    assert 'lbl_browse_page_count' in genizah_app_source, \
        "Browse nav should have lbl_browse_page_count label"

    # Check the folio label is updated in browse_render_page
    render_method = _extract_method(genizah_app_source, 'browse_render_page')
    assert 'lbl_browse_folio' in render_method, \
        "browse_render_page should update lbl_browse_folio"

    # Check "Folio" text is used
    assert "Folio" in render_method, \
        "browse_render_page should display 'Folio' prefix"


# ---------------------------------------------------------------------------
# Test 5: image_source_info populated in enrich_metadata
# ---------------------------------------------------------------------------

def test_image_source_info_in_enrich_metadata(genizah_core_source):
    """Verify that enrich_metadata populates image_source_info."""
    enrich_method = _extract_method(genizah_core_source, 'enrich_metadata')
    assert enrich_method, "enrich_metadata method not found"

    # Should populate image_source_info
    assert 'image_source_info' in enrich_method, \
        "enrich_metadata should set image_source_info"

    # Should call get_image_sources
    assert 'get_image_sources' in enrich_method, \
        "enrich_metadata should call get_image_sources"

    # Should populate folio_images
    assert 'folio_images' in enrich_method, \
        "enrich_metadata should set folio_images"

    # Should call get_folio_images
    assert 'get_folio_images' in enrich_method, \
        "enrich_metadata should call get_folio_images"

    # Should store sys_id in meta
    assert "sys_id" in enrich_method, \
        "enrich_metadata should store sys_id in meta dict"


# ---------------------------------------------------------------------------
# Test 6: Functional test with real crossref data (if DB available)
# ---------------------------------------------------------------------------

def test_real_crossref_folio_labels():
    """Exercise parse_folio_label and get_folio_images with real crossref data."""
    from shared.nli_crossref_service import parse_folio_label, NliCrossrefService

    # Test parse_folio_label with known patterns
    assert parse_folio_label('T_S_12_1__L1F0B0S1') == '1r'
    assert parse_folio_label('T_S_12_1__L1F0B0S2') == '1v'
    assert parse_folio_label('I_C_71__L3F0B0S1') == '3r'
    assert parse_folio_label('Yevr_III_B_1093__L7F0B0S1') == '7r'
    assert parse_folio_label('') == ''
    assert parse_folio_label('no_folio_pattern') == ''

    # Test with real DB if available
    db_path = os.path.join(os.path.dirname(__file__), '..', 'nli_data', 'nli_crossref.db')
    if not os.path.exists(db_path):
        pytest.skip("nli_crossref.db not available for functional test")

    svc = NliCrossrefService(db_path=db_path)
    assert svc.is_available(), "Service should be available when DB exists"

    # Find a multi-page manuscript to test folio labels
    images = svc.get_folio_images('990000918030205171')  # Known multi-page fragment
    if images:
        # Should have folio_label on each image
        for img in images:
            assert 'folio_label' in img, "Each image should have folio_label key"
            assert img['folio_label'], f"folio_label should not be empty for {img.get('image_name', 'unknown')}"

        # First image should be recto (1r) or a sequential number
        first_label = images[0].get('folio_label', '')
        assert first_label, "First image should have a non-empty folio label"

    svc.close()


# ---------------------------------------------------------------------------
# Test 7: KTIV button visibility controlled by image_source_info
# ---------------------------------------------------------------------------

def test_ktiv_button_visibility_logic(genizah_app_source):
    """Verify KTIV button visibility is controlled by image_source_info.nli_fgp."""
    load_method = _extract_method(genizah_app_source, 'load_images')
    assert load_method, "load_images method not found"

    # Should check image_source_info for nli_fgp
    assert 'image_source_info' in load_method, \
        "load_images should check image_source_info"
    assert 'nli_fgp' in load_method, \
        "load_images should check nli_fgp flag"

    # Should set KTIV visibility based on source info
    assert 'btn_ktiv' in load_method, \
        "load_images should control btn_ktiv visibility"


# ---------------------------------------------------------------------------
# Test 8: Folio images stored in on_browse_enriched_loaded
# ---------------------------------------------------------------------------

def test_folio_images_stored_on_enrichment(genizah_app_source):
    """Verify on_browse_enriched_loaded stores folio_images for page combo."""
    enriched_method = _extract_method(genizah_app_source, 'on_browse_enriched_loaded')
    assert enriched_method, "on_browse_enriched_loaded method not found"

    # Should store folio_images from meta
    assert '_browse_folio_images' in enriched_method, \
        "on_browse_enriched_loaded should set _browse_folio_images"
    assert 'folio_images' in enriched_method, \
        "on_browse_enriched_loaded should read folio_images from meta"


# ---------------------------------------------------------------------------
# Test 9: NLI crossref service import with graceful degradation
# ---------------------------------------------------------------------------

def test_nli_crossref_import_graceful(genizah_app_source):
    """Verify nli_crossref_service is imported with try/except."""
    # Should have try/except import
    assert 'from shared.nli_crossref_service import' in genizah_app_source, \
        "genizah_app should import from shared.nli_crossref_service"
    assert '_HAS_NLI_CROSSREF' in genizah_app_source, \
        "genizah_app should have _HAS_NLI_CROSSREF flag"

    # The import should be wrapped in try/except
    import_section = genizah_app_source[
        genizah_app_source.index('from shared.nli_crossref_service') - 50:
        genizah_app_source.index('from shared.nli_crossref_service') + 200
    ]
    assert 'try:' in import_section, \
        "nli_crossref_service import should be wrapped in try/except"
    assert 'except ImportError' in import_section, \
        "nli_crossref_service import should catch ImportError"
