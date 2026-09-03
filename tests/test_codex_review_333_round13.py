"""Regressions for the two Codex findings in round 13 on PR #333 (2026-09-02).

Both are cases where the desktop credit could name the wrong library.

P2 — `genizah_app.py::_switch_browse_viewer_to_nli_for_page` and
`_restore_browse_viewer_to_ext` assign `current_source` and call `set_page`
directly, never touching the attribution helper, so an NLI image kept the
Cambridge credit and the restored CUDL image could keep the NLI one.

P2 — when CUDL has no canvas for a page, a synthetic entry marked
`is_nli_fallback` is appended to `images_ext`, so the EXTERNAL list stays active
while an NLI URL is displayed; deciding the credit from the list alone credited
Cambridge for an NLI image.
"""
from __future__ import annotations

import ast


def _read(path):
    return open(path, encoding="utf-8").read()


class _Viewer:
    """The two credit helpers, unbound from the widget (no QApplication)."""

    def __init__(self, active_list, idx, source, attr_ext="", attr_nli=""):
        import desktop.viewers as viewers
        self.active_list = active_list
        self.current_idx = idx
        self.current_source = source
        self._attr_ext = attr_ext
        self._attr_nli = attr_nli
        self.text = None
        self.visible = None
        self._showing_nli_image = (
            viewers.ManuscriptViewerWidget._showing_nli_image.__get__(self))
        self._apply = (
            viewers.ManuscriptViewerWidget._apply_attribution_for_source.__get__(self))

    @property
    def lbl_attribution(self):
        outer = self

        class _L:
            def setText(self, t):
                outer.text = t

            def setVisible(self, v):
                outer.visible = v
        return _L()


CUDL = "Cambridge University Library · CC BY-NC 3.0"
NLI = 'From the collections of the "Ktiv" Project, The National Library of Israel'


class TestCreditFollowsTheActiveEntry:
    def test_a_synthetic_nli_entry_inside_images_ext_credits_nli(self):
        images_ext = [
            {"label": "1r", "url": "https://cudl/1"},
            {"label": "NLI", "url": "https://iiif.nli.org.il/FL1", "is_nli_fallback": True},
        ]
        v = _Viewer(images_ext, 1, "ext", attr_ext=CUDL, attr_nli=NLI)
        assert v._showing_nli_image() is True
        v._apply()
        assert v.text == NLI, "an NLI image inside images_ext must not be credited to CUDL"

    def test_an_ordinary_external_entry_keeps_the_external_credit(self):
        images_ext = [{"label": "1r", "url": "https://cudl/1"}]
        v = _Viewer(images_ext, 0, "ext", attr_ext=CUDL, attr_nli=NLI)
        assert v._showing_nli_image() is False
        v._apply()
        assert v.text == CUDL

    def test_the_nli_list_still_credits_nli(self):
        v = _Viewer([{"label": "FL1"}], 0, "nli", attr_ext=CUDL, attr_nli=NLI)
        v._apply()
        assert v.text == NLI

    def test_an_nli_image_never_falls_back_to_the_external_credit(self):
        # Unknown NLI credit: say NLI generically rather than name the wrong library.
        v = _Viewer([{"label": "FL1"}], 0, "nli", attr_ext=CUDL, attr_nli="")
        v._apply()
        assert v.text != CUDL
        assert "National Library of Israel" in v.text or "הספרייה הלאומית" in v.text

    def test_an_out_of_range_index_is_safe(self):
        v = _Viewer([], 3, "ext", attr_ext=CUDL, attr_nli=NLI)
        assert v._showing_nli_image() is False
        v._apply()
        assert v.text == CUDL

    def test_empty_credits_hide_the_label(self):
        v = _Viewer([{"label": "1r"}], 0, "ext", attr_ext="", attr_nli="")
        v._apply()
        assert v.visible is False


class TestProgrammaticSwitchesRefreshTheCredit:
    def test_set_page_reapplies_the_attribution(self):
        # genizah_app's CUDL-coverage switches assign current_source then call
        # set_page directly, so set_page is the one place that must refresh.
        src = _read("desktop/viewers.py")
        fn = next(n for n in ast.walk(ast.parse(src))
                  if isinstance(n, ast.FunctionDef) and n.name == "set_page")
        body = ast.get_source_segment(src, fn)
        assert "_apply_attribution_for_source()" in body

    def test_the_programmatic_switch_sites_do_call_set_page(self):
        src = _read("genizah_app.py")
        for name in ("_switch_browse_viewer_to_nli_for_page",
                     "_restore_browse_viewer_to_ext"):
            fn = next(n for n in ast.walk(ast.parse(src))
                      if isinstance(n, ast.FunctionDef) and n.name == name)
            body = ast.get_source_segment(src, fn)
            assert "viewer.set_page(" in body, name
            assert "viewer.current_source = " in body, name

    def test_translation_key_exists(self):
        assert '"From the collections of the National Library of Israel"' in _read(
            "genizah_translations.py")
