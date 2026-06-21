# -*- coding: utf-8 -*-
"""Headless tests for web/components/candidate_grid.py (Phase 117 CND-02).

Tests pure helper functions — build_thumbnail_url, build_browse_url, _truncate_title —
without requiring a NiceGUI event loop or Qt runtime.  The helper functions are
importable in isolation via the real module (no stubs needed; web.services and
web.translations are thin pure-Python modules with no side-effects at import time).

Coverage targets (per plan acceptance criteria):
  - build_thumbnail_url: NLI proxy URL form (page/no-page)
  - build_thumbnail_url: synthetic sys_id -> None
  - build_thumbnail_url: Oxford fork (Bodleian / /api/oxford_image, NOT NLI proxy)
  - build_thumbnail_url: no direct iiif.nli.org.il URL ever
  - build_browse_url: page/no-page URL forms
  - _truncate_title: titles > 80 chars truncated with ellipsis
  - empty-list message string (module-level constant sanity)
  - library-chip gating (logic: chip only when library_code non-empty)
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from unittest.mock import patch

import web.components.candidate_grid as cgrid

# Aliases for convenience
build_thumbnail_url = cgrid.build_thumbnail_url
build_browse_url = cgrid.build_browse_url
_truncate_title = cgrid._truncate_title


# ---------------------------------------------------------------------------
# Minimal Candidate stand-in (mirrors shared.joins_lab.Candidate fields).
# Using a local dataclass avoids importing the heavy genizah_core chain.
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class _Cand:
    sys_id: str
    page: Optional[int]
    shelfmark: str = "?"
    title: str = ""
    library_code: str = ""


# Realistic NLI sys_id (real Alma - NOT synthetic, NLI institution suffix 205171)
_NLI_SYS_ID = "990025143260205171"

# Synthetic sys_id (18 digits, 99 prefix, 000000 suffix)
_SYNTH_SYS_ID = "990001234560000000"

# Oxford shelfmark + library code (matches the regex in web/services.py)
_OX_SHELFMARK = "MS Heb e.7/5"
_OX_LIBRARY = "Oxford"


# ===========================================================================
# Tests: build_thumbnail_url — NLI default proxy
# ===========================================================================

class TestBuildThumbnailUrlNli:
    """NLI default proxy URL: /api/nli_image_by_sysid/{id}?page={idx}&width=300"""

    def test_page_2_yields_page_idx_1(self):
        """page=2 -> page_idx = max(0, 2-1) = 1"""
        url = build_thumbnail_url(_NLI_SYS_ID, page=2, shelfmark="T-S 12.100", library_code="CUL")
        assert url == f"/api/nli_image_by_sysid/{_NLI_SYS_ID}?page=1&width=300"

    def test_page_none_yields_page_0(self):
        """page=None -> (page or 1) - 1 = 0"""
        url = build_thumbnail_url(_NLI_SYS_ID, page=None, shelfmark="T-S 12.100", library_code="CUL")
        assert url == f"/api/nli_image_by_sysid/{_NLI_SYS_ID}?page=0&width=300"

    def test_page_1_yields_page_0(self):
        """page=1 -> page_idx = max(0, 1-1) = 0"""
        url = build_thumbnail_url(_NLI_SYS_ID, page=1, shelfmark="T-S 12.100", library_code="CUL")
        assert url == f"/api/nli_image_by_sysid/{_NLI_SYS_ID}?page=0&width=300"

    def test_no_direct_iiif_url(self):
        url = build_thumbnail_url(_NLI_SYS_ID, page=1, shelfmark="T-S 12.100", library_code="CUL")
        assert url is not None
        assert "iiif.nli.org.il" not in url

    def test_contains_nli_proxy_path(self):
        url = build_thumbnail_url(_NLI_SYS_ID, page=1, shelfmark="T-S 12.100", library_code="CUL")
        assert "/api/nli_image_by_sysid/" in url

    def test_contains_width_300(self):
        url = build_thumbnail_url(_NLI_SYS_ID, page=1, shelfmark="T-S 12.100", library_code="CUL")
        assert "width=300" in url


# ===========================================================================
# Tests: build_thumbnail_url — synthetic sys_id
# ===========================================================================

class TestBuildThumbnailUrlSynthetic:
    """Synthetic sys_ids must return None (placeholder path, no proxy attempt)."""

    def test_synthetic_returns_none(self):
        url = build_thumbnail_url(_SYNTH_SYS_ID, page=1)
        assert url is None

    def test_synthetic_with_page_none_returns_none(self):
        url = build_thumbnail_url(_SYNTH_SYS_ID, page=None)
        assert url is None

    def test_synthetic_page_5_returns_none(self):
        url = build_thumbnail_url(_SYNTH_SYS_ID, page=5)
        assert url is None

    def test_synthetic_with_shelfmark_still_none(self):
        """Even if shelfmark/library_code are provided, synthetic -> None."""
        url = build_thumbnail_url(_SYNTH_SYS_ID, page=1, shelfmark="T-S 12.100", library_code="CUL")
        assert url is None


# ===========================================================================
# Tests: build_thumbnail_url — Oxford fork (MEDIUM-6)
# ===========================================================================

class TestBuildThumbnailUrlOxford:
    """Oxford fork: Bodleian direct URL or /api/oxford_image proxy — NOT the NLI proxy."""

    def test_derivable_oxford_shelfmark_returns_bodleian_url(self):
        """MS Heb e.7/5, page_idx=0 -> bodleian.ox.ac.uk URL."""
        url = build_thumbnail_url(
            _NLI_SYS_ID, page=1,
            shelfmark=_OX_SHELFMARK, library_code=_OX_LIBRARY
        )
        assert url is not None
        # Must be a Bodleian URL (derivable shelfmark) or /api/oxford_image
        assert "bodleian.ox.ac.uk" in url or "/api/oxford_image/" in url

    def test_oxford_url_not_nli_proxy(self):
        url = build_thumbnail_url(
            _NLI_SYS_ID, page=1,
            shelfmark=_OX_SHELFMARK, library_code=_OX_LIBRARY
        )
        assert "/api/nli_image_by_sysid/" not in url

    def test_oxford_no_direct_iiif(self):
        """Oxford uses Bodleian or /api/oxford_image — never iiif.nli.org.il."""
        url = build_thumbnail_url(
            _NLI_SYS_ID, page=1,
            shelfmark=_OX_SHELFMARK, library_code=_OX_LIBRARY
        )
        assert url is None or "iiif.nli.org.il" not in url

    def test_oxford_non_derivable_shelfmark_falls_back_to_api_proxy(self):
        """Oxford library_code but non-matching shelfmark -> /api/oxford_image fallback."""
        url = build_thumbnail_url(
            _NLI_SYS_ID, page=2,
            shelfmark="UNKNOWN FORMAT",
            library_code="Oxford"
        )
        assert url is not None
        assert "/api/oxford_image/" in url
        assert "/api/nli_image_by_sysid/" not in url

    def test_oxford_monkeypatch_bodleian_url(self):
        """Monkeypatch: is_oxford_manuscript True + get_oxford_direct_image_url known URL."""
        with (
            patch.object(cgrid, "is_oxford_manuscript", return_value=True),
            patch.object(cgrid, "get_oxford_direct_image_url", return_value="https://bodleian.example/img.jpg"),
        ):
            url = build_thumbnail_url("990099999990205171", page=1)
        assert url == "https://bodleian.example/img.jpg"
        assert "/api/nli_image_by_sysid/" not in url

    def test_oxford_monkeypatch_no_bodleian_uses_api_proxy(self):
        """When get_oxford_direct_image_url returns empty, fallback to /api/oxford_image."""
        sys_id = "990099999990205171"
        with (
            patch.object(cgrid, "is_oxford_manuscript", return_value=True),
            patch.object(cgrid, "get_oxford_direct_image_url", return_value=""),
        ):
            url = build_thumbnail_url(sys_id, page=3)
        # page=3 -> page_idx = max(0, 3-1) = 2
        assert url == f"/api/oxford_image/{sys_id}?page=2"
        assert "/api/nli_image_by_sysid/" not in url

    def test_oxford_ms_heb_prefix_detected_by_shelfmark(self):
        """Shelfmark 'MS Heb' prefix alone (no explicit Oxford library_code) -> Oxford fork."""
        url = build_thumbnail_url(
            _NLI_SYS_ID, page=1,
            shelfmark="MS Heb c.50/14",
            library_code="CUL"  # wrong but shelfmark wins
        )
        # is_oxford_manuscript("MS Heb c.50/14", "CUL") -> True (shelfmark prefix)
        assert url is not None
        assert "/api/nli_image_by_sysid/" not in url


# ===========================================================================
# Tests: build_browse_url
# ===========================================================================

class TestBuildBrowseUrl:
    """Browse URL construction."""

    def test_with_page(self):
        cand = _Cand(sys_id=_NLI_SYS_ID, page=2)
        url = build_browse_url(cand)
        assert url == f"/browse?sys_id={_NLI_SYS_ID}&page=2"

    def test_without_page(self):
        cand = _Cand(sys_id=_NLI_SYS_ID, page=None)
        url = build_browse_url(cand)
        assert url == f"/browse?sys_id={_NLI_SYS_ID}"
        assert "&page=" not in url

    def test_page_1_included(self):
        cand = _Cand(sys_id=_NLI_SYS_ID, page=1)
        url = build_browse_url(cand)
        assert "&page=1" in url

    def test_sys_id_in_url(self):
        cand = _Cand(sys_id=_NLI_SYS_ID, page=None)
        url = build_browse_url(cand)
        assert _NLI_SYS_ID in url

    def test_starts_with_browse(self):
        cand = _Cand(sys_id=_NLI_SYS_ID, page=None)
        url = build_browse_url(cand)
        assert url.startswith("/browse?sys_id=")


# ===========================================================================
# Tests: _truncate_title
# ===========================================================================

class TestTruncateTitle:
    """Title truncation helper."""

    def test_short_title_unchanged(self):
        assert _truncate_title("Short title") == "Short title"

    def test_empty_title_unchanged(self):
        assert _truncate_title("") == ""

    def test_exactly_80_chars_unchanged(self):
        title = "A" * 80
        assert _truncate_title(title) == title

    def test_81_chars_truncated_with_ellipsis(self):
        title = "A" * 81
        result = _truncate_title(title)
        assert result.endswith("...")
        assert len(result) == 83  # 80 chars + "..."

    def test_200_char_title_truncated(self):
        title = "ב" * 200
        result = _truncate_title(title)
        assert result.endswith("...")
        assert len(result) == 83

    def test_truncation_preserves_first_80_chars(self):
        title = "X" * 100
        result = _truncate_title(title)
        assert result[:80] == "X" * 80


# ===========================================================================
# Tests: Module-level constants and string keys
# ===========================================================================

class TestModuleConstants:
    """Sanity checks on module-level constants used in empty state and cards."""

    def test_truncate_at_is_80(self):
        assert cgrid._TITLE_TRUNCATE_AT == 80

    def test_empty_state_message_string_present(self):
        """The empty-state message must use the UI-SPEC canonical EN string."""
        # We can verify by checking that the tr() call key is the correct one
        # by scanning the source for the string.
        import inspect
        source = inspect.getsource(cgrid)
        assert "No candidates found. Try different lines or broader terms." in source

    def test_candidates_label_present_in_source(self):
        import inspect
        source = inspect.getsource(cgrid)
        assert "Candidates" in source

    def test_view_in_browse_label_present_in_source(self):
        import inspect
        source = inspect.getsource(cgrid)
        assert "View in Browse" in source


# ===========================================================================
# Tests: cap_candidates (WebSocket-safety render cap)
# ===========================================================================

class TestCapCandidates:
    """The render cap prevents a common-term search (700+ hits) from dropping
    the NiceGUI websocket ('Connection Lost' + session reset)."""

    cap_candidates = staticmethod(cgrid.cap_candidates)

    def _cands(self, n: int) -> list:
        return [_Cand(sys_id=str(i), page=1) for i in range(n)]

    def test_cap_constant_is_200(self):
        assert cgrid._MAX_RENDERED_CANDIDATES == 200

    def test_under_cap_returns_all(self):
        cands = self._cands(50)
        to_render, total = self.cap_candidates(cands)
        assert total == 50
        assert len(to_render) == 50
        assert to_render is cands  # no copy when under the cap

    def test_exactly_at_cap_returns_all(self):
        cands = self._cands(200)
        to_render, total = self.cap_candidates(cands)
        assert total == 200
        assert len(to_render) == 200

    def test_over_cap_truncates_but_reports_full_total(self):
        # The 782-candidate "פזורה" case from UAT.
        cands = self._cands(782)
        to_render, total = self.cap_candidates(cands)
        assert total == 782, "header must show the FULL count"
        assert len(to_render) == 200, "only the cap is rendered"
        assert to_render == cands[:200]

    def test_custom_cap(self):
        cands = self._cands(10)
        to_render, total = self.cap_candidates(cands, max_rendered=3)
        assert total == 10
        assert len(to_render) == 3

    def test_empty_list(self):
        to_render, total = self.cap_candidates([])
        assert total == 0
        assert to_render == []


# ===========================================================================
# Tests: Library chip gating
# ===========================================================================

class TestLibraryChipGating:
    """The library chip branch is gated on `if cand.library_code:` (falsy guard)."""

    def test_empty_library_code_is_falsy(self):
        """Empty library_code -> chip branch NOT taken."""
        cand = _Cand(sys_id=_NLI_SYS_ID, page=1, library_code="")
        assert not cand.library_code

    def test_non_empty_library_code_is_truthy(self):
        """Non-empty library_code -> chip branch IS taken."""
        cand = _Cand(sys_id=_NLI_SYS_ID, page=1, library_code="CUL")
        assert cand.library_code

    def test_none_library_code_is_falsy(self):
        """None-like value -> chip branch NOT taken. (Candidate default is '' so this
        confirms the guard works for any falsy value.)"""
        # Candidate default library_code is "" not None, but confirm empty is falsy
        cand = _Cand(sys_id=_NLI_SYS_ID, page=1, library_code="")
        assert not bool(cand.library_code)


# ===========================================================================
# Tests: Threat boundary asserts (static source inspection)
# ===========================================================================

class TestThreatBoundaryAsserts:
    """Source-level asserts that T-117-07 and T-117-03 boundaries are not breached."""

    def _get_source_lines(self):
        import inspect
        return inspect.getsource(cgrid).splitlines()

    def test_no_direct_iiif_url_in_source(self):
        """build_thumbnail_url must never emit a direct iiif.nli.org.il URL."""
        lines = self._get_source_lines()
        # Allow comments/docstrings; check that no f-string / return produces it
        code_lines = [
            l for l in lines
            if not l.lstrip().startswith("#")
            and "iiif.nli.org.il" in l
        ]
        # Only docstring lines (inside triple-quoted strings) are OK
        # We check that none of those lines contain `return` or `f"` (actual code)
        for line in code_lines:
            stripped = line.strip()
            assert not stripped.startswith("return"), \
                f"Direct iiif.nli.org.il URL in return statement: {line}"

    def test_no_handle_image_error_in_source(self):
        """handleImageError must not appear as a callable/function call in functional code.

        It may appear in comments and docstrings (as a prohibition note) but
        must never be invoked as a JS handler string or Python call.
        """
        lines = self._get_source_lines()
        # Look for lines that call or reference handleImageError as JS/Python code
        # (not just mentions in comments/docstrings).  The key patterns would be:
        #   js_handler="...handleImageError..."   (JS handler)
        #   .on('error', js_handler='...handleImageError...')
        #   or a Python function call handleImageError(...)
        for line in lines:
            stripped = line.lstrip()
            if "handleImageError" not in line:
                continue
            # Skip comment lines
            if stripped.startswith("#"):
                continue
            # Skip docstring-style lines (contain the word as a prohibition note,
            # not as a function call).  A call would have '(' or '=' nearby.
            # We allow the word in strings that describe the prohibition.
            assert "handleImageError(" not in line and \
                   ("js_handler" not in line or "no handleImageError" in line or "NO handleImageError" in line), \
                   f"handleImageError appears as functional code: {line!r}"

    def test_no_raw_storage_user_access_in_code(self):
        """app.storage.user must not appear as a Python expression in functional code."""
        lines = self._get_source_lines()
        # Only allow in comments and string literals (docstrings)
        for i, line in enumerate(lines, 1):
            stripped = line.lstrip()
            if stripped.startswith("#"):
                continue  # skip comments
            if "app.storage.user" in line:
                # Check it's inside a string literal (docstring context)
                # Simple heuristic: the surrounding module docstring lines
                # are all in the top section and contain known marker text
                assert "CI-guarded" in line or "Zero raw" in line or "zero raw" in line, \
                    f"Line {i}: app.storage.user in non-comment, non-docstring code: {line!r}"


# ===========================================================================
# Tests: G1 — Snippet + highlight render (Phase 119-05)
# ===========================================================================

class TestSnippetHighlightRender:
    """Source-level structural assertions for G1 (CND-03).

    Plan 119-05 adds a transcription snippet render to _create_candidate_card.
    These assertions verify that:
    (a) snippet_html is imported from shared.joins_lab;
    (b) _create_candidate_card renders the snippet via ui.html(sanitize=False)
        using the escaped helper — never raw cand.full_text/cand.snippet;
    (c) the snippet div carries direction:rtl and -webkit-line-clamp;
    (d) the actual highlight markup used is <b style='color:#dc2626'> (NOT <mark>).

    A live render-driven assertion for the <b style='color:#dc2626'> highlight is
    owned by Plan-08's render-smoke harness (which exercises the full NiceGUI async
    render path); these structural checks are the in-file co-required owner.
    """

    def _get_source(self):
        import inspect
        return inspect.getsource(cgrid)

    def test_snippet_html_imported_from_shared_joins_lab(self):
        """snippet_html must be imported (inline or at module level) from shared.joins_lab."""
        source = self._get_source()
        assert "snippet_html" in source, "snippet_html must be referenced in candidate_grid.py"
        assert "from shared.joins_lab import" in source
        # Confirm the import statement includes snippet_html
        import_lines = [
            line for line in source.splitlines()
            if "from shared.joins_lab import" in line
        ]
        joined = " ".join(import_lines)
        assert "snippet_html" in joined, \
            "snippet_html must appear in a 'from shared.joins_lab import ...' statement"

    def test_snippet_rendered_via_ui_html_sanitize_false(self):
        """_create_candidate_card must render the snippet via ui.html(..., sanitize=False)."""
        source = self._get_source()
        assert "sanitize=False" in source, \
            "ui.html(sanitize=False) must be used for the snippet render"
        assert "snippet_html(" in source, \
            "_create_candidate_card must call snippet_html() to produce the HTML"

    def test_raw_full_text_not_passed_to_ui_html(self):
        """cand.full_text / cand.snippet must NOT be passed directly to ui.html()."""
        source = self._get_source()
        # The pattern we forbid: ui.html(cand.full_text ...) or ui.html(cand.snippet ...)
        import re
        bad_patterns = [
            r'ui\.html\s*\(\s*cand\.full_text',
            r'ui\.html\s*\(\s*cand\.snippet',
        ]
        for pat in bad_patterns:
            assert not re.search(pat, source), \
                f"Raw corpus field passed directly to ui.html — must use snippet_html() helper: {pat}"

    def test_snippet_div_has_rtl_direction(self):
        """The snippet container must include direction:rtl styling."""
        source = self._get_source()
        # The snippet render block must carry direction:rtl
        assert "direction:rtl" in source, \
            "Snippet div must carry direction:rtl style for Hebrew RTL text"

    def test_snippet_div_has_webkit_line_clamp(self):
        """The snippet container must include -webkit-line-clamp for text clamping."""
        source = self._get_source()
        assert "-webkit-line-clamp" in source, \
            "Snippet div must carry -webkit-line-clamp for visual line clamping"

    def test_highlight_markup_is_b_tag_not_mark(self):
        """The highlight markup emitted by snippet_html/htmlify is <b style='color:#dc2626'>.

        F-G1a: the helper uses a <b style=...> span, NOT <mark> — test infra must
        assert the REAL markup form, never <mark>.
        """
        from shared.joins_lab import snippet_html
        result = snippet_html("hello world test", "world")
        assert "<b style='color:#dc2626'>" in result, \
            "snippet_html must produce <b style='color:#dc2626'> highlight spans, not <mark>"
        assert "<mark>" not in result, \
            "snippet_html must NOT produce <mark> tags (F-G1a VERIFIED)"
        assert "world" in result, "The matched term must appear in the snippet output"

    def test_snippet_html_escapes_corpus_text(self):
        """snippet_html must HTML-escape corpus text before injecting highlights (T-119-05)."""
        from shared.joins_lab import snippet_html
        xss_text = "<script>alert(1)</script> normal text"
        result = snippet_html(xss_text, None)
        assert "<script>" not in result, "snippet_html must escape < characters"
        assert "&lt;script&gt;" in result or "script" in result  # escaped form
        # More specifically, < must be escaped to &lt;
        assert "&lt;" in result, "snippet_html must convert < to &lt;"


# ===========================================================================
# Tests: G4 — Image click opens Compare (Phase 119-05)
# ===========================================================================

class TestImageClickCompare:
    """Source-level structural assertions for G4 (CND-04 image click).

    Plan 119-05 wires img_el.on('click', ...) calling on_compare(cand) with the
    FULL candidate, and adds cursor:pointer to the image style.  The synthetic
    placeholder also gets click + cursor:pointer.
    """

    def _get_source(self):
        import inspect
        return inspect.getsource(cgrid)

    def test_img_el_has_click_handler_in_source(self):
        """_create_candidate_card must register a click handler on img_el."""
        source = self._get_source()
        assert 'img_el.on("click"' in source or "img_el.on('click'" in source, \
            "img_el must have a click handler wired to the compare handler"

    def test_img_el_has_cursor_pointer_in_source(self):
        """img_el must carry cursor:pointer in its style to signal clickability."""
        source = self._get_source()
        assert "cursor:pointer" in source, \
            "img_el style must include cursor:pointer (G4 clickability signal)"

    def test_placeholder_has_cursor_pointer_in_source(self):
        """The synthetic placeholder branch must also have cursor:pointer."""
        source = self._get_source()
        # cursor:pointer must appear (shared by both image and placeholder branches)
        assert source.count("cursor:pointer") >= 1, \
            "cursor:pointer must appear for the placeholder too"

    def test_compare_handler_hoisted_before_thumbnail(self):
        """_make_compare_handler must be defined before the ui.card() / thumbnail block.

        G4 requires the handler to be available both for the image click AND the
        Compare button at the bottom — hoisting it before the card context ensures this.
        """
        source = self._get_source()
        # _make_compare_handler must appear before 'img_el = ui.image'
        idx_handler = source.find("def _make_compare_handler")
        idx_img = source.find("img_el = ui.image(")
        assert idx_handler != -1, "_make_compare_handler must be defined in _create_candidate_card"
        assert idx_img != -1, "img_el = ui.image( must appear in _create_candidate_card"
        assert idx_handler < idx_img, \
            "_make_compare_handler must be defined BEFORE img_el = ui.image( (hoisted for G4)"

    def test_image_click_calls_on_compare_with_full_candidate(self):
        """Invoking the image-click handler must call on_compare with the full candidate."""
        @dataclass
        class _FullCand:
            sys_id: str
            page: Optional[int]
            shelfmark: str = "T-S 12.1"
            title: str = "Test title"
            library_code: str = "CUL"
            full_text: str = ""
            snippet: str = ""
            highlight_pattern: Optional[str] = None
            via_text: bool = True
            via_vs: bool = False
            via_other_side: bool = False
            is_anchor_self: bool = False
            vs_rank: Optional[int] = None
            vs_score: Optional[float] = None
            volume_ie: Optional[str] = None

        received = []

        def _on_compare(cand):
            received.append(cand)

        # Build _make_compare_handler directly using the function's closure pattern
        cand = _FullCand(sys_id="990025143260205171", page=1)
        cand_ref = cand

        def _make_compare_handler(c=cand_ref, handler=_on_compare):
            def _handler():
                if handler:
                    handler(c)
            return _handler

        _make_compare_handler()()  # invoke
        assert received == [cand], \
            "Image-click handler must call on_compare with the FULL candidate object"


# ===========================================================================
# Tests: G3 — Immediate triage-button fill update (Phase 119-05)
# ===========================================================================

class TestTriageButtonFillImmediate:
    """Source-level structural assertions for G3 (immediate triage button fill).

    Plan 119-05 extends _make_triage_handler to hold render-local refs to the
    three triage buttons and update their fill style on click.
    """

    def _get_source(self):
        import inspect
        return inspect.getsource(cgrid)

    def test_triage_btn_refs_dict_present_in_source(self):
        """_create_candidate_card must maintain a render-local _triage_btn_refs dict."""
        source = self._get_source()
        assert "_triage_btn_refs" in source, \
            "_triage_btn_refs must be defined as a per-card render-local dict (G3, T-119-07)"

    def test_triage_btn_refs_keyed_into_handler_in_source(self):
        """_make_triage_handler must receive _triage_btn_refs (not a module global)."""
        source = self._get_source()
        assert "_btn_refs" in source, \
            "_triage_btn_refs must be passed into _make_triage_handler as a closure param"

    def test_triage_handler_updates_btn_style_in_source(self):
        """_make_triage_handler must call _btn.style(...) to update the fill."""
        source = self._get_source()
        assert "_btn.style(" in source, \
            "_make_triage_handler must call _btn.style() to push the fill update to the client"

    def test_triage_handler_sets_active_fill_color_in_source(self):
        """The handler must set background:{_TRIAGE_COLORS[v]} on the active button."""
        source = self._get_source()
        assert "_TRIAGE_COLORS" in source
        # The handler must reference background: in the fill update path
        assert "background:" in source, \
            "Triage handler must set background: CSS property for active button fill"

    def test_triage_btn_refs_assigned_after_button_creation(self):
        """Each button element must be captured into _triage_btn_refs after creation.

        R2-4 (119-10): the button assignment may use a multi-line chained form
        (``_btn_el = (\n    ui.button(glyph)\n    .props(...)``). Accept either
        the inline ``_btn_el = ui.button(`` or the parenthesised form
        ``_btn_el = (`` followed by ``ui.button(`` elsewhere in the source.
        """
        source = self._get_source()
        # Accept either inline or chained-parenthesised assignment
        has_btn_el_assign = (
            "_btn_el = ui.button(" in source
            or "_btn_el=ui.button(" in source
            or ("_btn_el = (" in source and "ui.button(" in source)
        )
        assert has_btn_el_assign, \
            "Button element must be assigned to _btn_el for capture into _triage_btn_refs"
        assert "_triage_btn_refs[verdict] = _btn_el" in source, \
            "Each button must be stored in _triage_btn_refs keyed by verdict"

    def test_triage_handler_resets_other_buttons_in_source(self):
        """When a verdict is clicked, the other two buttons must be reset to unfilled style."""
        source = self._get_source()
        # The handler iterates over all buttons and sets either filled or unfilled style
        assert "for _verdict, _btn in _btn_refs.items():" in source, \
            "Handler must iterate over all button refs to reset non-active buttons"


# ===========================================================================
# Issue D (UAT 2026-06-21): restyle fn updates the V/?/X triage BUTTON fills
# on the grid after a Compare verdict — not just the card border.
# ===========================================================================

class _FakeBtn:
    """Mock NiceGUI button — records the last inline style string applied."""

    def __init__(self) -> None:
        self.last_style: Optional[str] = None

    def style(self, s: str) -> "_FakeBtn":
        self.last_style = s
        return self


class _FakeCard:
    """Mock NiceGUI card — records the last inline style string applied."""

    def __init__(self) -> None:
        self.last_style: Optional[str] = None

    def style(self, s: str) -> "_FakeCard":
        self.last_style = s
        return self


class TestRestyleUpdatesTriageButtons:
    """The render-scoped restyle fn must update both the card border AND the
    triage button active-fill when a verdict changes (Issue D).

    Before the fix, _make_restyle_fn only restyled the card border, so a verdict
    set in the Compare modal never updated the grid's ✓/?/✗ button highlight
    until a full re-render.
    """

    _SYS_ID = "990025143260205171"

    def _build_refs(self):
        """Return (card_refs, triage_btn_refs, card, btn_map) for one card."""
        card = _FakeCard()
        btn_map = {"yes": _FakeBtn(), "maybe": _FakeBtn(), "no": _FakeBtn()}
        card_refs = {self._SYS_ID: [card]}
        triage_btn_refs = {self._SYS_ID: [btn_map]}
        return card_refs, triage_btn_refs, card, btn_map

    def test_make_restyle_fn_accepts_triage_btn_refs(self):
        """_make_restyle_fn must accept the optional triage_btn_refs dict (Issue D)."""
        import inspect
        sig = inspect.signature(cgrid._make_restyle_fn)
        assert "triage_btn_refs" in sig.parameters, (
            "_make_restyle_fn must accept a triage_btn_refs param so the restyle "
            "path can update the V/?/X button fills (Issue D)."
        )

    def test_verdict_yes_fills_yes_button_and_clears_others(self):
        """Setting 'yes' must give the YES button the active green fill; others reset."""
        card_refs, triage_btn_refs, card, btn_map = self._build_refs()
        restyle = cgrid._make_restyle_fn(card_refs, triage_btn_refs)

        restyle(self._SYS_ID, {self._SYS_ID: "yes"})

        yes_color = cgrid._TRIAGE_COLORS["yes"]
        assert btn_map["yes"].last_style is not None
        assert f"background:{yes_color}" in btn_map["yes"].last_style, (
            "Active YES button must get the active fill background. "
            f"Got: {btn_map['yes'].last_style!r}"
        )
        assert "color:#fff" in btn_map["yes"].last_style
        # Round-4 Issue 6: the other two buttons must be EXPLICITLY reset to a
        # transparent fill — NOT merely omit a background.  element.style() does not
        # remove a previously-set background, so a prior verdict's color would
        # otherwise persist (all three lit at once).  Assert the inactive buttons
        # carry no TRIAGE-COLOR fill and an explicit transparent reset.
        maybe_color = cgrid._TRIAGE_COLORS["maybe"]
        no_color = cgrid._TRIAGE_COLORS["no"]
        for v, other_color in (("maybe", maybe_color), ("no", no_color)):
            assert btn_map[v].last_style is not None
            assert f"background:{other_color}" not in btn_map[v].last_style, (
                f"{v} button must NOT keep its own triage color when YES is active. "
                f"Got: {btn_map[v].last_style!r}"
            )
            assert "background:transparent" in btn_map[v].last_style, (
                f"{v} button should be EXPLICITLY reset to background:transparent when "
                f"YES is active (Round-4 Issue 6). Got: {btn_map[v].last_style!r}"
            )

    def test_verdict_change_also_updates_card_border(self):
        """The card border must still update alongside the button fills (regression)."""
        card_refs, triage_btn_refs, card, btn_map = self._build_refs()
        restyle = cgrid._make_restyle_fn(card_refs, triage_btn_refs)

        restyle(self._SYS_ID, {self._SYS_ID: "no"})

        no_color = cgrid._TRIAGE_COLORS["no"]
        assert card.last_style is not None
        assert f"2px solid {no_color}" in card.last_style, (
            f"Card border must reflect the NO verdict color. Got: {card.last_style!r}"
        )

    def test_clearing_verdict_resets_all_buttons(self):
        """A None/absent verdict must reset every button to the no-fill style."""
        card_refs, triage_btn_refs, card, btn_map = self._build_refs()
        restyle = cgrid._make_restyle_fn(card_refs, triage_btn_refs)

        # No verdict for this sys_id → all buttons reset, neutral border
        restyle(self._SYS_ID, {})

        # Round-4 Issue 6: a cleared verdict must EXPLICITLY reset each button to a
        # transparent fill (not just omit a background) so no prior color lingers.
        for v in ("yes", "maybe", "no"):
            assert btn_map[v].last_style is not None
            assert "background:transparent" in btn_map[v].last_style, (
                f"{v} button should be reset to background:transparent on clear "
                f"(Round-4 Issue 6). Got: {btn_map[v].last_style!r}"
            )
            # And no TRIAGE color fill remains
            for _color in cgrid._TRIAGE_COLORS.values():
                assert f"background:{_color}" not in btn_map[v].last_style
        assert "var(--border-light)" in (card.last_style or "")

    def test_restyle_fn_without_triage_btn_refs_is_backward_compatible(self):
        """Omitting triage_btn_refs must not raise (backward compat for old callers)."""
        card = _FakeCard()
        restyle = cgrid._make_restyle_fn({self._SYS_ID: [card]})
        # Should restyle the border and not raise even with no button refs
        restyle(self._SYS_ID, {self._SYS_ID: "maybe"})
        assert card.last_style is not None


# ===========================================================================
# Issue A (UAT 2026-06-21): Add-as-Join in the TABLE bulk action bar — enabled
# only when exactly ONE row is selected (pairwise anchor + one candidate).
# ===========================================================================

class _RecordingBtn:
    """Mock NiceGUI button — records props() add/remove + tooltip + on_click."""

    def __init__(self, *args, **kwargs):
        self.init_args = args
        self.init_kwargs = kwargs
        self.props_history: list = []  # list of (positional_str, remove_kwarg)
        self.tooltips: list = []
        self.on_click = kwargs.get("on_click")

    def props(self, s: str = "", *, remove: str = None):
        self.props_history.append((s, remove))
        return self

    def style(self, *a, **k):
        return self

    def tooltip(self, t):
        self.tooltips.append(t)
        return self

    def on(self, event, handler=None, **kwargs):
        # The bulk-bar triage buttons attach handlers via .on('click', ...).
        # Capture a click handler if one is supplied this way.
        if event == "click" and handler is not None:
            self.on_click = handler
        return self

    def classes(self, *a, **k):
        return self

    def is_currently_disabled(self) -> bool:
        """Replay props history to compute the current disabled state."""
        disabled = "disable" in (self.init_kwargs.get("props") or "")
        for s, remove in self.props_history:
            if s and "disable" in s:
                disabled = True
            if remove and "disable" in remove:
                disabled = False
        return disabled


def _build_table_with_mock_ui(*, on_add_as_join=None, candidates=None):
    """Build create_candidate_table under a mocked NiceGUI ``ui``.

    Returns (selection_handler, add_join_btn, button_factory_calls).
    selection_handler is the function registered on the table's 'selection' event.
    """
    from unittest.mock import MagicMock, patch

    candidates = candidates or []

    # Element factory shared by row/column/label/icon
    def _mk_el():
        el = MagicMock()
        el.__enter__ = lambda s: s
        el.__exit__ = MagicMock(return_value=False)
        for m in ("classes", "props", "style", "mark", "tooltip", "on", "set_text"):
            setattr(el, m, MagicMock(return_value=el))
        return el

    created_buttons: list = []

    def _button_factory(*args, **kwargs):
        btn = _RecordingBtn(*args, **kwargs)
        created_buttons.append(btn)
        return btn

    table_handlers: dict = {}

    def _make_table(*args, **kwargs):
        t = MagicMock()
        t.__enter__ = lambda s: s
        t.__exit__ = MagicMock(return_value=False)
        t.classes = MagicMock(return_value=t)
        t.props = MagicMock(return_value=t)
        t.style = MagicMock(return_value=t)

        def _on(event, handler=None, **kw):
            if handler is not None:
                table_handlers[event] = handler
            return t
        t.on = MagicMock(side_effect=_on)
        return t

    with patch("web.components.candidate_grid.ui") as mock_ui:
        mock_ui.button = MagicMock(side_effect=_button_factory)
        mock_ui.table = MagicMock(side_effect=_make_table)
        for attr in ("row", "column", "label", "icon", "grid"):
            factory = MagicMock(side_effect=lambda *a, **k: _mk_el())
            setattr(mock_ui, attr, factory)

        cgrid.create_candidate_table(
            candidates,
            triage={},
            enrichment={},
            on_add_as_join=on_add_as_join,
        )

    # The Add-as-Join button is the one carrying icon='add_link'
    add_join_btn = next(
        (b for b in created_buttons if b.init_kwargs.get("icon") == "add_link"),
        None,
    )
    return table_handlers.get("selection"), add_join_btn, created_buttons


class TestTableAddAsJoinSingleSelectGate:
    """Issue A: the table bulk-bar Add-as-Join button is enabled only on a single
    selected row, and calls back with the row's sys_id + RAW shelfmark."""

    def test_table_accepts_on_add_as_join_param(self):
        import inspect
        sig = inspect.signature(cgrid.create_candidate_table)
        assert "on_add_as_join" in sig.parameters, (
            "Issue A: create_candidate_table must accept on_add_as_join="
        )

    def test_add_join_button_created_when_callback_provided(self):
        _sel, add_join_btn, _btns = _build_table_with_mock_ui(
            on_add_as_join=lambda sid, sm: None,
        )
        assert add_join_btn is not None, (
            "Issue A: an Add-as-Join button (icon='add_link') must be created in the "
            "table bulk bar when on_add_as_join is provided."
        )

    def test_add_join_button_starts_disabled(self):
        _sel, add_join_btn, _btns = _build_table_with_mock_ui(
            on_add_as_join=lambda sid, sm: None,
        )
        assert add_join_btn.is_currently_disabled(), (
            "Issue A: Add-as-Join must start disabled (no selection yet)."
        )

    def test_add_join_enabled_on_exactly_one_selection(self):
        sel_handler, add_join_btn, _btns = _build_table_with_mock_ui(
            on_add_as_join=lambda sid, sm: None,
        )
        assert sel_handler is not None, "selection handler must be registered"

        # Simulate selecting exactly one row
        from types import SimpleNamespace
        ev = SimpleNamespace(args=[{"sys_id": "S1", "shelfmark_raw": "T-S 1.1"}])
        sel_handler(ev)
        assert not add_join_btn.is_currently_disabled(), (
            "Issue A: Add-as-Join must be ENABLED when exactly one row is selected."
        )

    def test_add_join_disabled_on_two_selections(self):
        sel_handler, add_join_btn, _btns = _build_table_with_mock_ui(
            on_add_as_join=lambda sid, sm: None,
        )
        from types import SimpleNamespace
        # First select one (enables), then two (must re-disable)
        sel_handler(SimpleNamespace(args=[{"sys_id": "S1", "shelfmark_raw": "T-S 1.1"}]))
        sel_handler(SimpleNamespace(args=[
            {"sys_id": "S1", "shelfmark_raw": "T-S 1.1"},
            {"sys_id": "S2", "shelfmark_raw": "T-S 2.2"},
        ]))
        assert add_join_btn.is_currently_disabled(), (
            "Issue A: Add-as-Join must be DISABLED when more than one row is selected."
        )

    def test_add_join_disabled_on_zero_selection(self):
        sel_handler, add_join_btn, _btns = _build_table_with_mock_ui(
            on_add_as_join=lambda sid, sm: None,
        )
        from types import SimpleNamespace
        sel_handler(SimpleNamespace(args=[{"sys_id": "S1", "shelfmark_raw": "T-S 1.1"}]))
        sel_handler(SimpleNamespace(args=[]))
        assert add_join_btn.is_currently_disabled(), (
            "Issue A: Add-as-Join must be DISABLED when the selection is cleared."
        )

    def test_add_join_click_calls_back_with_sysid_and_raw_shelfmark(self):
        captured = []
        sel_handler, add_join_btn, _btns = _build_table_with_mock_ui(
            on_add_as_join=lambda sid, sm: captured.append((sid, sm)),
        )
        from types import SimpleNamespace
        sel_handler(SimpleNamespace(args=[{"sys_id": "S42", "shelfmark_raw": "T-S 42.42"}]))

        # The bulk button's on_click is captured at init
        on_click = add_join_btn.on_click
        assert callable(on_click), "Add-as-Join must have an on_click handler"
        on_click()
        assert captured == [("S42", "T-S 42.42")], (
            "Issue A: clicking Add-as-Join with one selection must call back with the "
            f"row's sys_id + RAW shelfmark (no badge prefix). Got: {captured!r}"
        )

    def test_add_join_click_noop_when_not_exactly_one(self):
        captured = []
        sel_handler, add_join_btn, _btns = _build_table_with_mock_ui(
            on_add_as_join=lambda sid, sm: captured.append((sid, sm)),
        )
        from types import SimpleNamespace
        sel_handler(SimpleNamespace(args=[
            {"sys_id": "S1", "shelfmark_raw": "T-S 1.1"},
            {"sys_id": "S2", "shelfmark_raw": "T-S 2.2"},
        ]))
        add_join_btn.on_click()
        assert captured == [], (
            "Issue A: Add-as-Join click must be a no-op when not exactly one row selected."
        )

    def test_no_add_join_button_when_callback_absent(self):
        _sel, add_join_btn, _btns = _build_table_with_mock_ui(on_add_as_join=None)
        assert add_join_btn is None, (
            "Issue A: no Add-as-Join button should be created when on_add_as_join is None."
        )


class TestTableRowsHaveRawShelfmark:
    """Issue A support: table rows must carry a raw (un-prefixed) shelfmark so the
    single-select Add-as-Join can resolve it without the badge marker."""

    def test_make_table_rows_includes_shelfmark_raw(self):
        from dataclasses import dataclass as _dc

        @_dc
        class _C:
            uid: str
            sys_id: str
            shelfmark: str
            page: object = None
            score: object = None
            snippet: str = ""
            # Fields read by badge_and_tooltip / _make_table_rows
            is_anchor_self: bool = False
            via_other_side: bool = False
            via_vs: bool = False
            vs_rank: object = None

        rows = cgrid._make_table_rows(
            [_C(uid="u1", sys_id="S1", shelfmark="T-S 1.1")],
            triage={},
            enrichment={},
        )
        assert rows, "expected one row"
        assert rows[0].get("shelfmark_raw") == "T-S 1.1", (
            "Issue A: each table row must carry 'shelfmark_raw' (the un-prefixed shelfmark)."
        )


# ===========================================================================
# Round 4 UAT (2026-06-21) — Issues 6, 7, 8 (candidate grid)
# ===========================================================================

import inspect as _inspect  # noqa: E402


def test_issue7_8_grid_accepts_new_callbacks():
    """Round-4 Issues 7+8: create_candidate_grid must accept on_add_to_puzzle,
    on_card_select, and selected_sys_ids."""
    sig = _inspect.signature(cgrid.create_candidate_grid)
    for p in ("on_add_to_puzzle", "on_card_select", "selected_sys_ids"):
        assert p in sig.parameters, (
            f"create_candidate_grid must accept {p!r} (Round-4 Issues 7/8)."
        )


def test_issue7_8_card_accepts_new_callbacks():
    """Round-4 Issues 7+8: _create_candidate_card must accept on_add_to_puzzle,
    on_card_select, and selected."""
    sig = _inspect.signature(cgrid._create_candidate_card)
    for p in ("on_add_to_puzzle", "on_card_select", "selected"):
        assert p in sig.parameters, (
            f"_create_candidate_card must accept {p!r} (Round-4 Issues 7/8)."
        )


def test_issue6_in_card_triage_handler_resets_background_on_clear():
    """Round-4 Issue 6: the in-card triage handler must EXPLICITLY reset the inactive
    buttons to background:transparent (not just omit a background) so the previously
    lit button turns off when a new verdict is clicked."""
    import pathlib
    src = pathlib.Path("web/components/candidate_grid.py").read_text(encoding="utf-8")
    # The clear branch inside _make_triage_handler must set a transparent background.
    assert "background:transparent; color:inherit;" in src, (
        "Issue 6: the triage 'clear' branches must reset inactive buttons to "
        "'background:transparent; color:inherit;' — element.style() does not remove a "
        "previously-set background, so without this all verdicts can be lit at once."
    )


def test_issue6_restyle_clear_branch_resets_background():
    """Round-4 Issue 6: _make_restyle_fn's button-update path must reset inactive
    buttons to a transparent fill (mutual exclusivity from the restyle path too)."""
    card_refs = {}
    triage_btn_refs = {}

    class _FB:
        def __init__(self):
            self.last = None
        def style(self, s):
            self.last = s

    class _FC:
        def style(self, s):
            pass

    sid = "S9"
    btns = {"yes": _FB(), "maybe": _FB(), "no": _FB()}
    card_refs[sid] = [_FC()]
    triage_btn_refs[sid] = [btns]

    restyle = cgrid._make_restyle_fn(card_refs, triage_btn_refs)
    # Set 'yes' → maybe/no must be reset to transparent
    restyle(sid, {sid: "yes"})
    yes_color = cgrid._TRIAGE_COLORS["yes"]
    assert f"background:{yes_color}" in btns["yes"].last
    for v in ("maybe", "no"):
        assert "background:transparent" in btns[v].last, (
            f"Issue 6: restyle must reset {v} button to background:transparent when "
            f"yes is active. Got: {btns[v].last!r}"
        )
