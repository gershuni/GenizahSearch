"""Regression for the Codex finding in round 5 on PR #333 (2026-09-02).

P2 `web/pages/search_results.py` — the Advanced-view image runs the same
`handleImageError` chain as the browse page (Oxford proxy → NLI manifest → NLI
proxy), but its credit footer was a plain label with none of the
`data-role="image-credit"` metadata `switchImageCredit()` updates. An Oxford
image that fell back to NLI therefore kept the Bodleian credit.
"""
from __future__ import annotations

import ast
import re


def _read(path):
    return open(path, encoding="utf-8").read()


SRC = "web/pages/search_results.py"


def _footer_block(src):
    i = src.index("# Attribution footer")
    return src[i:src.index("adv_state.image_credit_el = _adv_credit_lbl", i)]


class TestAdvancedViewCreditIsSwitchable:
    def test_footer_renders_the_switchable_metadata(self):
        block = _footer_block(_read(SRC))
        # set directly since 2026-11-23 (escaped into a props string, `&quot;` stayed as text)
        assert "_adv_credit_lbl.props['data-role'] = 'image-credit'" in block
        assert "_adv_credit_lbl.props['data-credit-oxford']" in block
        assert "_adv_credit_lbl.props['data-credit-nli']" in block

    def test_both_credit_variants_are_set_as_values_not_markup(self):
        # props set directly are attribute values (Vue escapes them); escaping them as well put `&quot;`
        # in the text the fallback shows (2026-11-23)
        block = _footer_block(_read(SRC))
        assert block.count("html.escape(") == 0

    def test_the_nli_variant_is_not_the_oxford_credit(self):
        block = _footer_block(_read(SRC))
        assert "_adv_credit_nli" in block
        m = re.search(r"props\['data-credit-nli'\] = (\w+)", block)
        assert m and m.group(1) == "_adv_credit_nli"
        m2 = re.search(r"props\['data-credit-oxford'\] = (\w+)", block)
        assert m2 and m2.group(1) == "attribution"

    def test_the_image_still_runs_the_shared_error_chain(self):
        from web.pages.search_results import quick_view_img_html
        img = quick_view_img_html('/api/oxford_image/990000000000205171?page=2', '990000000000205171', 2,
                                  is_oxford=True)
        assert "handleImageError(this, '990000000000205171', 2, true, 'advViewer')" in img

    def test_module_imports_html(self):
        src = _read(SRC)
        tree = ast.parse(src)
        imported = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert "html" in imported

    def test_metadata_is_only_emitted_for_pages_whose_image_can_fall_back(self):
        # An NLI image has nothing to switch away from; keep the DOM clean. Oxford's image and
        # a library's own (Cambridge, ...) fall back to NLI (2026-11-11).
        src = _read(SRC)
        i = src.index("_adv_credit_lbl = ui.label(attribution)")
        window = src[i:i + 700]
        gate = window.index("if is_oxford or library_image_credit:")
        props = window.index("_adv_credit_lbl.props[")
        assert gate < props


class TestSwitchHelperToleratesAMissingLink:
    def test_js_guards_the_link_element(self):
        js = _read("web/static/manuscript_viewer.js")
        i = js.index("function switchImageCredit(")
        body = js[i:i + 900]
        # The Advanced footer renders a label but no <a>; the helper must not throw.
        assert "if (!lbl) return;" in body
        assert re.search(r"if \(lnk && href\)", body)
