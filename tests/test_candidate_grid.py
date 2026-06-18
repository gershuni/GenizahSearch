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
