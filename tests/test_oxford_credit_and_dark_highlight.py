"""Oxford image credit (licence form, no CC label) + readable highlights in dark mode.

2026-09-02 (debug/oxford-fgp-image-mismatch, owner UAT on the web): the image
footer read "Bodleian Libraries, University of Oxford · CC BY-NC 4.0" while the
Genizah Fragments licence is NOT Creative Commons and asks for
"Image provided by [owner]" with a link to the site; and the search-hit
highlight rendered white text on a yellow box in dark mode.
"""
from __future__ import annotations

import re
from unittest.mock import patch

from shared.metadata_manager import OXFORD_IMAGE_CREDIT_EN


def _read(path):
    return open(path, encoding="utf-8").read()


class TestOxfordCreditWording:
    def test_single_source_of_truth_is_the_licence_form(self):
        assert OXFORD_IMAGE_CREDIT_EN == "Image provided by the Bodleian Libraries, University of Oxford"

    def test_no_cc_label_on_oxford_anywhere_in_web(self):
        for path in ("web/services.py", "web/pages/browse_enrichment.py", "web/pages/browse.py",
                     "web/pages/search_results.py"):
            src = _read(path)
            assert not re.search(r"Oxford[^\n]{0,80}CC BY-NC", src), path

    def test_web_table_uses_the_shared_constant_in_both_languages(self):
        import web.services as services
        with patch.object(services, "get_language", return_value="en"):
            assert services._get_library_attribution("Oxford") == OXFORD_IMAGE_CREDIT_EN
        with patch.object(services, "get_language", return_value="he"):
            he = services._get_library_attribution("Oxford")
        assert he and he != OXFORD_IMAGE_CREDIT_EN
        assert "בודליאנה" in he

    def test_old_wording_is_gone_from_every_surface(self):
        for path in ("genizah_app.py", "shared/metadata_manager.py", "web/pages/search_results.py"):
            assert "From the collections of the Bodleian Libraries, Oxford" not in _read(path), path

    def test_desktop_translates_the_credit(self):
        assert '"Image provided by the Bodleian Libraries, University of Oxford"' in _read("genizah_translations.py")
        assert "tr(OXFORD_IMAGE_CREDIT_EN)" in _read("genizah_app.py")


class TestCreditFollowsDisplayedSource:
    def test_footer_carries_both_credits_and_links_to_genizah_fragments(self):
        src = _read("web/pages/browse.py")
        assert 'data-credit-nli=' in src and 'data-credit-oxford=' in src
        assert "credit_link = _ox_direct_link or page.external_url or 'https://hebrew.bodleian.ox.ac.uk/'" in src
        assert "https://digital.bodleian.ox.ac.uk/" not in src.split("# === Image Credit/Attribution Footer ===")[1].split("# === RIGHT PANEL")[0]

    def test_enrichment_keeps_the_nli_manifest_credit_separately(self):
        src = _read("web/pages/browse_enrichment.py")
        assert "result['attribution_nli'] = nli_attribution" in src
        assert "pg.attribution_nli = browse_enrich['attribution_nli']" in src
        assert "attribution_nli: str = ''" in _read("web/services.py")

    def test_js_fallback_switches_the_credit_to_nli(self):
        js = _read("web/static/manuscript_viewer.js")
        assert "function switchImageCredit(source)" in js
        assert js.count("if (isOxford) switchImageCredit('nli');") == 2  # manifest + proxy fallbacks


class TestDarkModeHighlight:
    def test_browse_highlight_has_explicit_text_colour_and_dark_rule(self):
        src = _read("web/pages/browse.py")
        light = re.search(r"\.highlight-term\s*\{[^}]*\}", src).group(0)
        assert "color:" in light
        assert '[data-theme="dark"] .highlight-term' in src

    def test_search_inline_reader_marks_use_the_themed_class(self):
        src = _read("web/pages/search_results.py")
        assert 'class="highlight-match"' in src
        assert 'background-color: #fef08a' not in src

    def test_common_css_dark_rule_exists_for_highlight_match(self):
        css = _read("web/static/common.css")
        assert '[data-theme="dark"] .highlight-match' in css
