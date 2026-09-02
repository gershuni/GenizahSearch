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


class TestAdvancedViewCreditIsSwitchable:
    def test_footer_renders_the_switchable_metadata(self):
        src = _read(SRC)
        i = src.index("# Attribution footer")
        block = src[i:i + 2200]
        assert 'data-role="image-credit"' in block
        assert "data-credit-oxford=" in block
        assert "data-credit-nli=" in block

    def test_both_credit_variants_are_html_escaped(self):
        src = _read(SRC)
        i = src.index("# Attribution footer")
        block = src[i:i + 2200]
        assert block.count("html.escape(") == 2, "both attribute values must be escaped"

    def test_the_nli_variant_is_not_the_oxford_credit(self):
        src = _read(SRC)
        i = src.index("# Attribution footer")
        block = src[i:i + 2200]
        assert "_adv_credit_nli" in block
        m = re.search(r"data-credit-nli=\"\{html\.escape\((\w+),", block)
        assert m and m.group(1) == "_adv_credit_nli"
        m2 = re.search(r"data-credit-oxford=\"\{html\.escape\((\w+),", block)
        assert m2 and m2.group(1) == "attribution"

    def test_the_image_still_runs_the_shared_error_chain(self):
        src = _read(SRC)
        assert "handleImageError(this, '{safe_sys_id}', {page_idx}, {is_oxford_js}, 'advViewer')" in src

    def test_module_imports_html(self):
        src = _read(SRC)
        tree = ast.parse(src)
        imported = {a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names}
        assert "html" in imported

    def test_metadata_is_only_emitted_for_oxford_pages(self):
        # A non-Oxford manuscript has nothing to switch away from; keep the DOM clean.
        src = _read(SRC)
        i = src.index("_adv_credit_lbl = ui.label(attribution)")
        window = src[i:i + 700]
        gate = window.index("if is_oxford:")
        props = window.index("_adv_credit_lbl.props(")
        assert gate < props


class TestSwitchHelperToleratesAMissingLink:
    def test_js_guards_the_link_element(self):
        js = _read("web/static/manuscript_viewer.js")
        i = js.index("function switchImageCredit(")
        body = js[i:i + 900]
        # The Advanced footer renders a label but no <a>; the helper must not throw.
        assert "if (!lbl) return;" in body
        assert re.search(r"if \(lnk && href\)", body)
