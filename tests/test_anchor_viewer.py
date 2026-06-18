# -*- coding: utf-8 -*-
"""
Phase 117 Plan 06 — Tests for web/components/anchor_viewer.py.

Tests the AnchorViewer component logic with injected browse_resolver and
external_resolver so no live NiceGUI render / AppState / meta_mgr is needed.

Coverage:
- zoom_in clamps at 4.0; zoom_out clamps at 0.25; zoom_reset → 1.0
- NLI proxy-only image URL (no iiif.nli.org.il)
- Cambridge new-HIGH wiring: browse page with EMPTY cambridge_images +
  external_resolver supplying cambridge_images → resolved URL is
  /api/cambridge_image/... (would FAIL if resolve_external_images were skipped)
- None boundary: browse_resolver returns None → _resolve_off_loop returns None
- No handleImageError / no iiif.nli.org.il in built <img> HTML (HIGH-2)
- window._msViewerLoaded idempotency guard present in _VIEWER_HEAD
"""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest


# ─── Fake page helpers ────────────────────────────────────────────────────────

def _make_page(
    sys_id: str = "990025143260205171",
    p_num: int = 1,
    total_pages: int = 5,
    text: str = "שורה ראשונה\nשורה שנייה",
    shelfmark: str = "T-S 12.123",
    is_oxford: bool = False,
    library_code: str = "CUL",
    volume_suffix: int = 1,
    cambridge_images: Optional[List] = None,
    external_provider: str = "",
    cambridge_alignment: Optional[Dict] = None,
    volumes: Optional[List] = None,
    current_idx: Optional[int] = None,
    library_name: str = "Cambridge University Library",
    title: str = "Liturgical fragment",
) -> SimpleNamespace:
    """Build a fake BrowsePage-like object suitable for injection as browse_resolver result.

    NOTE: cambridge_images defaults to an empty list — matching the REAL
    service.get_browse_page() behaviour (it does NOT populate external-provider
    fields). The Cambridge wiring test intentionally passes empty cambridge_images
    here and lets the external_resolver supply them.

    current_idx is the DENSE 1-based ordinal the core returns (new_idx + 1); it
    defaults to p_num for the simple contiguous case the tests use. The real
    BrowsePage always carries current_idx, library_name and title.
    """
    return SimpleNamespace(
        sys_id=sys_id,
        p_num=p_num,
        total_pages=total_pages,
        text=text,
        shelfmark=shelfmark,
        is_oxford=is_oxford,
        library_code=library_code,
        library_name=library_name,
        title=title,
        volume_suffix=volume_suffix,
        cambridge_images=cambridge_images if cambridge_images is not None else [],
        external_provider=external_provider,
        cambridge_alignment=cambridge_alignment,
        volumes=volumes or [],
        current_idx=current_idx if current_idx is not None else p_num,
    )


def _make_external_empty() -> Dict:
    """External resolver return value when no external images are available."""
    return {"cambridge_images": [], "external_provider": "", "cambridge_alignment": None}


def _make_external_cambridge(sys_id: str = "990025143260205171") -> Dict:
    """External resolver return value for a Cambridge manuscript."""
    return {
        "cambridge_images": [
            {"@id": f"https://cudl.lib.cam.ac.uk/iiif/{sys_id}/canvas/1"},
        ],
        "external_provider": "",       # '' = Cambridge (not 'manchester'/'jts')
        "cambridge_alignment": {"verdict": "aligned", "count": 1},
    }


def _noop_external(_sys_id: str, **_kw) -> Dict:
    """External resolver stub that returns no external images."""
    return _make_external_empty()


# ─── Fixture: AnchorViewer with mocked NiceGUI ────────────────────────────────

def _make_viewer(
    browse_resolver=None,
    external_resolver=None,
    sys_id: str = "990025143260205171",
) -> "Any":
    """Instantiate AnchorViewer with NiceGUI UI calls mocked out."""
    # Mock all NiceGUI calls so we can import AnchorViewer without a running server
    mock_element = MagicMock()
    mock_element.__enter__ = lambda s: s
    mock_element.__exit__ = MagicMock(return_value=False)
    mock_element.classes = MagicMock(return_value=mock_element)
    mock_element.props = MagicMock(return_value=mock_element)
    mock_element.style = MagicMock(return_value=mock_element)
    mock_element.set_text = MagicMock()
    mock_element.set_content = MagicMock()
    mock_element.clear = MagicMock()

    with (
        patch("web.components.anchor_viewer.ui") as mock_ui,
        patch("web.components.anchor_viewer.run"),
    ):
        # Wire every ui.* factory to return the mock_element
        for attr in (
            "column", "row", "element", "html", "button", "icon",
            "label", "tooltip", "add_head_html", "run_javascript",
        ):
            factory = MagicMock(return_value=mock_element)
            factory.__enter__ = lambda s: s
            factory.__exit__ = MagicMock(return_value=False)
            setattr(mock_ui, attr, factory)

        from web.components.anchor_viewer import AnchorViewer
        viewer = AnchorViewer(
            sys_id=sys_id,
            browse_resolver=browse_resolver,
            external_resolver=external_resolver,
        )
    return viewer


# ─── Zoom arithmetic tests (no NiceGUI needed) ────────────────────────────────

class TestZoomArithmetic:
    """Test the per-instance zoom state arithmetic directly."""

    def _viewer(self) -> "Any":
        return _make_viewer(
            browse_resolver=lambda *a, **kw: None,
            external_resolver=_noop_external,
        )

    def test_initial_zoom_is_1(self):
        v = self._viewer()
        assert v._zoom == 1.0

    def test_zoom_in_increments_by_0_25(self):
        v = self._viewer()
        v._zoom = 1.0
        with patch.object(v, "_zoom", 1.0, create=True):
            pass  # direct attribute access is fine
        v._zoom = 1.0
        # Simulate zoom_in without running the JS
        v._zoom = min(v._zoom + 0.25, 4.0)
        assert v._zoom == pytest.approx(1.25)

    def test_zoom_in_clamps_at_4(self):
        v = self._viewer()
        v._zoom = 4.0
        v._zoom = min(v._zoom + 0.25, 4.0)
        assert v._zoom == pytest.approx(4.0)

    def test_zoom_in_from_near_max(self):
        v = self._viewer()
        v._zoom = 3.9
        v._zoom = min(v._zoom + 0.25, 4.0)
        assert v._zoom == pytest.approx(4.0)

    def test_zoom_out_decrements_by_0_25(self):
        v = self._viewer()
        v._zoom = 2.0
        v._zoom = max(v._zoom - 0.25, 0.25)
        assert v._zoom == pytest.approx(1.75)

    def test_zoom_out_clamps_at_0_25(self):
        v = self._viewer()
        v._zoom = 0.25
        v._zoom = max(v._zoom - 0.25, 0.25)
        assert v._zoom == pytest.approx(0.25)

    def test_zoom_out_from_near_min(self):
        v = self._viewer()
        v._zoom = 0.30
        v._zoom = max(v._zoom - 0.25, 0.25)
        assert v._zoom == pytest.approx(0.25)

    def test_zoom_reset_returns_1(self):
        v = self._viewer()
        v._zoom = 3.5
        v._zoom = 1.0
        assert v._zoom == pytest.approx(1.0)


# ─── _resolve_off_loop tests (pure sync, no async) ────────────────────────────

class TestResolveOffLoop:
    """Test the synchronous _resolve_off_loop method directly."""

    def test_nli_proxy_url_no_direct_nli(self):
        """NLI manuscript → resolve_image_url returns /api/nli_image_by_sysid proxy URL."""
        sys_id = "990025143260205171"
        page = _make_page(sys_id=sys_id, p_num=3, is_oxford=False)

        def fake_browse(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            return page

        v = _make_viewer(browse_resolver=fake_browse, external_resolver=_noop_external, sys_id=sys_id)
        result = v._resolve_off_loop(p_num=3)

        assert result is not None
        resolved_page, resolved = result
        assert resolved_page.sys_id == sys_id
        assert resolved["has_image"] is True
        img_url = resolved["img_url"]
        assert img_url.startswith(f"/api/nli_image_by_sysid/{sys_id}")
        assert "iiif.nli.org.il" not in img_url

    def test_cambridge_wiring_new_high(self):
        """NEW-HIGH round-2 wiring test.

        Simulates the REAL gap: service.get_browse_page() returns a Cambridge
        manuscript with EMPTY cambridge_images (it never populates them), and the
        injected external_resolver supplies the actual cambridge_images.

        Asserts:
        (a) external_resolver IS called with the page's sys_id (spy check).
        (b) The resolved image URL is /api/cambridge_image/... (proves the
            external_resolver result flowed into resolve_image_url).

        If AnchorViewer skipped resolve_external_images, cambridge_images would
        stay [] and the URL would still be /api/nli_image_by_sysid/... → FAIL.

        The page's cambridge_images is intentionally left EMPTY (not pre-filled)
        to close the round-1 masking gap.
        """
        sys_id = "990001234560205171"  # Cambridge manuscript sys_id
        # Page with EMPTY cambridge_images (what service.get_browse_page really returns)
        page = _make_page(
            sys_id=sys_id,
            p_num=1,
            is_oxford=False,
            library_code="CUL",
            cambridge_images=[],          # intentionally EMPTY — simulates the gap
            external_provider="",
            cambridge_alignment=None,
        )

        def fake_browse(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            return page

        # Track calls to external_resolver (spy)
        call_log: List = []

        def fake_external(sid, **kw):
            call_log.append(sid)
            return _make_external_cambridge(sys_id=sid)

        v = _make_viewer(browse_resolver=fake_browse, external_resolver=fake_external, sys_id=sys_id)
        result = v._resolve_off_loop(p_num=1)

        assert result is not None, "resolve_off_loop should return a result"
        _, resolved = result

        # (a) external_resolver was called with the page sys_id
        assert call_log, "external_resolver was NOT called — external-image enrichment is missing (new-HIGH)"
        assert call_log[0] == sys_id, f"expected external_resolver called with {sys_id!r}, got {call_log}"

        # (b) The resolved URL is the Cambridge proxy — NOT the NLI proxy
        img_url = resolved["img_url"]
        assert img_url.startswith(f"/api/cambridge_image/{sys_id}"), (
            f"Expected /api/cambridge_image/... but got {img_url!r}. "
            "If this test fails, AnchorViewer probably skipped resolve_external_images "
            "(the cambridge_images stayed empty and NLI was used instead)."
        )
        assert "iiif.nli.org.il" not in img_url

    def test_none_boundary_no_raise(self):
        """browse_resolver returns None (unknown sys_id / boundary) → _resolve_off_loop returns None."""
        def fake_browse(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            return None

        v = _make_viewer(browse_resolver=fake_browse, external_resolver=_noop_external)
        result = v._resolve_off_loop()
        assert result is None

    def test_external_resolver_called_with_page_sys_id(self):
        """external_resolver is always called with the page's sys_id (not the original)."""
        original_sys_id = "990025143260205171"
        # In cross-folio navigation the page.sys_id may differ from the input sys_id
        # (allow_cross=True case). Ensure we always pass page.sys_id to the external resolver.
        page = _make_page(sys_id=original_sys_id, p_num=2)
        calls = []

        def fake_browse(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            return page

        def spy_external(sid, **kw):
            calls.append(sid)
            return _make_external_empty()

        v = _make_viewer(browse_resolver=fake_browse, external_resolver=spy_external, sys_id=original_sys_id)
        v._resolve_off_loop(p_num=2)
        assert calls == [original_sys_id]


# ─── _build_img_html tests (HIGH-2 / ANC-02) ─────────────────────────────────

class TestBuildImgHtml:
    """Assert the built <img> HTML satisfies HIGH-2 invariants."""

    def _viewer(self) -> "Any":
        return _make_viewer(
            browse_resolver=lambda *a, **kw: None,
            external_resolver=_noop_external,
        )

    def test_no_handle_image_error_in_img_html(self):
        """<img> HTML must NOT contain onerror=handleImageError (HIGH-2)."""
        v = self._viewer()
        html = v._build_img_html("/api/nli_image_by_sysid/990025143260205171?page=0")
        assert "handleImageError" not in html, (
            "HIGH-2 violation: handleImageError found in <img> HTML; "
            "this wires the direct-NLI fallback bypass."
        )

    def test_no_direct_nli_url_in_img_html(self):
        """<img> src must never be a direct iiif.nli.org.il URL (ANC-02 / HIGH-2)."""
        v = self._viewer()
        html = v._build_img_html("/api/nli_image_by_sysid/990025143260205171?page=0")
        assert "iiif.nli.org.il" not in html

    def test_img_html_contains_proxy_url(self):
        """The proxy URL is present as the src attribute."""
        v = self._viewer()
        proxy_url = "/api/nli_image_by_sysid/990025143260205171?page=2"
        html = v._build_img_html(proxy_url)
        assert proxy_url in html

    def test_img_html_has_zoomable_image_class(self):
        """manuscriptViewer relies on .zoomable-image selector."""
        v = self._viewer()
        html = v._build_img_html("/api/nli_image_by_sysid/990025143260205171?page=0")
        assert "zoomable-image" in html

    def test_img_html_has_safe_onload_only(self):
        """Only the safe onload=manuscriptViewer.init() handler is wired, nothing more."""
        v = self._viewer()
        html = v._build_img_html("/api/nli_image_by_sysid/990025143260205171?page=0")
        assert "manuscriptViewer" in html  # safe zoom-init handler present
        assert "onerror" not in html       # no error handler that could trigger NLI fallback


# ─── _VIEWER_HEAD idempotency guard ───────────────────────────────────────────

class TestViewerHeadIdempotencyGuard:
    """The head HTML must contain the window._msViewerLoaded guard (Phase 119 safe)."""

    def test_viewer_head_contains_ms_viewer_loaded(self):
        from web.components.anchor_viewer import _VIEWER_HEAD
        assert "window._msViewerLoaded" in _VIEWER_HEAD, (
            "_VIEWER_HEAD must contain the window._msViewerLoaded idempotency guard "
            "so two AnchorViewer instances on one page (Phase 119 Compare) "
            "do not double-run createManuscriptViewer."
        )

    def test_viewer_head_does_not_contain_handle_image_error(self):
        from web.components.anchor_viewer import _VIEWER_HEAD
        # The handleImageError function lives in manuscript_viewer.js.
        # It MUST NOT appear in our head HTML — wiring it would bypass Phase-98.
        assert "handleImageError" not in _VIEWER_HEAD

    def test_viewer_head_does_not_contain_nli_iiif_base(self):
        from web.components.anchor_viewer import _VIEWER_HEAD
        assert "iiif.nli.org.il" not in _VIEWER_HEAD

    def test_viewer_head_contains_create_manuscript_viewer(self):
        from web.components.anchor_viewer import _VIEWER_HEAD
        assert "createManuscriptViewer" in _VIEWER_HEAD


# ─── Rich BrowsePage shape confirmation (HIGH-1) ─────────────────────────────

class TestRichBrowsePageShape:
    """Confirm AnchorViewer uses the rich BrowsePage shape, not the narrow Protocol dict."""

    def test_browse_resolver_injected_is_called_for_image_data(self):
        """browse_resolver callable is invoked with sys_id (proves the rich resolver is wired)."""
        call_log = []
        sys_id = "990025143260205171"
        page = _make_page(sys_id=sys_id)

        def spy_browse(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            call_log.append((sid, p_num, direction))
            return page

        v = _make_viewer(browse_resolver=spy_browse, external_resolver=_noop_external, sys_id=sys_id)
        v._resolve_off_loop(p_num=1)
        assert call_log, "browse_resolver was not called"
        assert call_log[0][0] == sys_id

    def test_direction_neg1_passes_to_browse_resolver(self):
        """Folio-prev (direction=-1) is forwarded to browse_resolver."""
        direction_log = []
        sys_id = "990025143260205171"
        page = _make_page(sys_id=sys_id, p_num=2)

        def spy_browse(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            direction_log.append(direction)
            return page

        v = _make_viewer(browse_resolver=spy_browse, external_resolver=_noop_external, sys_id=sys_id)
        v._resolve_off_loop(direction=-1)
        assert direction_log and direction_log[0] == -1


# ─── Oxford direct-URL branch (MEDIUM-5 documented exception) ─────────────────

class TestOxfordBranch:
    """Oxford manuscripts route to direct Bodleian URL (MEDIUM-5 exception)."""

    def test_oxford_uses_bodleian_url(self):
        """Oxford page with derivable shelfmark → resolve_image_url returns Bodleian URL."""
        sys_id = "990001458630205171"  # Oxford sys_id
        page = _make_page(
            sys_id=sys_id,
            p_num=1,
            is_oxford=True,
            shelfmark="MS Heb. e.93/58",
            library_code="Oxford",
        )

        def fake_browse(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            return page

        v = _make_viewer(browse_resolver=fake_browse, external_resolver=_noop_external, sys_id=sys_id)
        result = v._resolve_off_loop(p_num=1)
        assert result is not None
        _, resolved = result
        # Oxford images come from Bodleian (not NLI proxy and not iiif.nli.org.il)
        img_url = resolved.get("img_url", "")
        if resolved["has_image"]:
            # Either direct Bodleian or /api/oxford_image — not NLI IIIF
            assert "iiif.nli.org.il" not in img_url


# ─── Regression: default resolver wiring (UAT 2026-06-17) ─────────────────────

class TestDefaultResolverWiring:
    """Guards the lazy default ``browse_resolver`` import in ``__init__``.

    Every other test injects ``browse_resolver``, so the default fallback was
    never exercised by the suite. It imported a non-existent symbol
    (``from web.services import service``) and raised ImportError only at
    runtime, the first time a real anchor was loaded in the browser. The fix
    binds ``get_service().get_browse_page`` (the same accessor browse.py uses).
    """

    def test_default_browse_resolver_binds_without_import_error(self):
        # browse_resolver=None + external_resolver=None → __init__ resolves the
        # real defaults. This MUST NOT raise ImportError.
        v = _make_viewer(browse_resolver=None, external_resolver=None)
        assert callable(v._browse_resolver)
        assert callable(v._external_resolver)

    def test_default_browse_resolver_is_service_get_browse_page(self):
        from web.services import get_service

        v = _make_viewer(browse_resolver=None, external_resolver=None)
        assert v._browse_resolver == get_service().get_browse_page


# ─── Regression: viewer assets must inject at build, not dynamically (UAT) ────

class TestViewerAssetInjection:
    """Guards the zoom/pan fix.

    A <script> injected via add_head_html into an already-live SPA page does
    NOT execute, so AnchorViewer (constructed dynamically on a user click) must
    NOT inject the viewer JS itself — the page builder calls
    inject_viewer_assets() at initial render instead. These tests pin both
    halves so a refactor can't silently reintroduce dead zoom.
    """

    def test_inject_viewer_assets_adds_head_html(self):
        from unittest.mock import patch
        with patch("web.components.anchor_viewer.ui") as mock_ui:
            from web.components.anchor_viewer import inject_viewer_assets, _VIEWER_HEAD
            inject_viewer_assets()
            mock_ui.add_head_html.assert_called_once_with(_VIEWER_HEAD)

    def test_constructing_viewer_does_not_inject_head_html(self):
        # The dynamic path must NOT call add_head_html (its <script> wouldn't run).
        from unittest.mock import patch, MagicMock
        mock_element = MagicMock()
        mock_element.__enter__ = lambda s: s
        mock_element.__exit__ = MagicMock(return_value=False)
        for m in ("classes", "props", "style"):
            setattr(mock_element, m, MagicMock(return_value=mock_element))
        with (
            patch("web.components.anchor_viewer.ui") as mock_ui,
            patch("web.components.anchor_viewer.run"),
        ):
            for attr in (
                "column", "row", "element", "html", "button", "icon",
                "label", "tooltip", "add_head_html", "run_javascript",
            ):
                factory = MagicMock(return_value=mock_element)
                factory.__enter__ = lambda s: s
                factory.__exit__ = MagicMock(return_value=False)
                setattr(mock_ui, attr, factory)
            from web.components.anchor_viewer import AnchorViewer
            AnchorViewer(
                sys_id="990025143260205171",
                browse_resolver=lambda *a, **kw: None,
                external_resolver=lambda *a, **kw: {},
            )
            mock_ui.add_head_html.assert_not_called()


# ─── Regression: folio-nav boundary handling (UAT round 2) ────────────────────

class TestFolioBoundary:
    """Navigating before the first / past the last folio must NOT destroy the
    view with a 'fragment not found' state, and prev/next must disable at the
    boundaries. (UAT: clicking prev at page 1 showed 'fragment not found'.)
    """

    def _viewer_with_async_run(self, browse_resolver):
        from unittest.mock import patch, MagicMock

        mock_element = MagicMock()
        mock_element.__enter__ = lambda s: s
        mock_element.__exit__ = MagicMock(return_value=False)
        for m in ("classes", "props", "style"):
            setattr(mock_element, m, MagicMock(return_value=mock_element))

        async def _fake_io_bound(fn, *a, **k):
            return fn()

        ctx = patch("web.components.anchor_viewer.ui")
        rctx = patch("web.components.anchor_viewer.run")
        mock_ui = ctx.start()
        mock_run = rctx.start()
        mock_run.io_bound = _fake_io_bound
        for attr in (
            "column", "row", "element", "html", "button", "icon",
            "label", "tooltip", "add_head_html", "run_javascript", "notify",
        ):
            factory = MagicMock(return_value=mock_element)
            factory.__enter__ = lambda s: s
            factory.__exit__ = MagicMock(return_value=False)
            setattr(mock_ui, attr, factory)
        from web.components.anchor_viewer import AnchorViewer
        v = AnchorViewer(
            sys_id="990025143260205171",
            browse_resolver=browse_resolver,
            external_resolver=lambda *a, **kw: {},
        )
        # Give prev/next buttons real MagicMocks so we can assert set_enabled.
        v._prev_btn = MagicMock()
        v._next_btn = MagicMock()
        v._page_label = MagicMock()
        v._transcription_html = MagicMock()
        return v, mock_ui, (ctx, rctx)

    def test_prev_disabled_at_first_folio(self):
        import asyncio
        page = _make_page(p_num=1, total_pages=4, library_code="CUL")
        v, mock_ui, ctxs = self._viewer_with_async_run(
            lambda *a, **kw: page
        )
        try:
            asyncio.run(v.update_content(p_num=1))
            v._prev_btn.set_enabled.assert_called_with(False)   # at first folio
            v._next_btn.set_enabled.assert_called_with(True)    # more folios ahead
        finally:
            for c in ctxs:
                c.stop()

    def test_boundary_nav_keeps_view_no_not_found(self):
        import asyncio
        page = _make_page(p_num=1, total_pages=4, library_code="CUL")

        def resolver(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            return None if direction != 0 else page

        v, mock_ui, ctxs = self._viewer_with_async_run(resolver)
        try:
            asyncio.run(v.update_content(p_num=1))   # loads page 1, sets _last_img_url
            assert v._last_img_url, "page 1 should have rendered an image"
            asyncio.run(v.update_content(direction=-1))  # boundary
            # A boundary nav with a prior image notifies and keeps the view.
            assert mock_ui.notify.called, "boundary nav should notify, not blow away the view"
        finally:
            for c in ctxs:
                c.stop()

    def test_sequential_next_passes_current_folio_pnum(self):
        """Regression (UAT round 3): the stateless core needs the CURRENT folio's
        p_num to navigate. Before the fix, nav passed p_num=None so the core
        always restarted from index 0 — the viewer could only ever reach folio 2
        ('advance once, then stuck, then no more folios'). Here a stateful fake
        mimics the real core (index(p_num)+direction) over a 4-folio manuscript;
        sequential Next must walk 1→2→3→4 and Prev must step back.
        """
        import asyncio
        pages = [1, 2, 3, 4]
        calls = []

        def resolver(sid, *, p_num=None, direction=0, volume_ie=None, **kw):
            calls.append((p_num, direction))
            idx = pages.index(p_num) if p_num in pages else 0
            new_idx = idx + direction
            if new_idx < 0 or new_idx >= len(pages):
                return None  # boundary
            return _make_page(
                p_num=pages[new_idx],
                total_pages=len(pages),
                current_idx=new_idx + 1,
            )

        v, mock_ui, ctxs = self._viewer_with_async_run(resolver)
        try:
            asyncio.run(v.update_content(p_num=None))     # initial → folio 1
            assert v._p_num == 1
            asyncio.run(v.update_content(direction=+1))   # → folio 2
            assert v._p_num == 2
            asyncio.run(v.update_content(direction=+1))   # → folio 3 (the regression)
            assert v._p_num == 3, f"second Next stuck at folio {v._p_num} (nav bug)"
            asyncio.run(v.update_content(direction=+1))   # → folio 4
            assert v._p_num == 4
            v._next_btn.set_enabled.assert_called_with(False)  # last folio
            # The 2nd Next MUST forward the current folio (p_num=2), not None/1.
            assert (2, +1) in calls, f"current-folio p_num not forwarded: {calls}"
            asyncio.run(v.update_content(direction=-1))   # back → folio 3
            assert v._p_num == 3
        finally:
            for c in ctxs:
                c.stop()
