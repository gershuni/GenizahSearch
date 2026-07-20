# -*- coding: utf-8 -*-
"""Capture the rendered Connections Atlas surfaces for the masking gate (Phase 133, ATLAS-01).

This is a DEV/OPS capture helper (never imported by the running web app). It
produces HTML/text captures of the ``/atlas`` and ``/`` surfaces that
``scripts/check_atlas_masking.py --scan-asset <dir>`` then scans for any
restricted-corpus leak. It has TWO modes:

(a) ASGI mode (default) -- the HARD pre-deploy gate.
    Using the in-process ``httpx.ASGITransport(core.app)`` harness (NOT a
    launched web server -- never launch one from Bash on Windows, see project
    memory ``feedback_no_background_webserver.md``), it loads the freshly-baked
    atlas asset (``web.atlas_assets.load_atlas_state()``), forces
    ``atlas_preview_available()`` True, and renders ``/atlas`` + ``/`` in BOTH
    EN and HE, writing the server-rendered HTML (``body_html`` + a full
    element-tree text/content/props dump) to the capture dir. This captures
    everything the SERVER renders -- it deliberately does NOT see the
    client-only interaction DOM (tooltips/focus rows/search results), which is
    what mode (b) is for.

(b) ``--browser-dom <base-url>`` mode (Codex HIGH-4) -- best-effort pre-deploy,
    GUARANTEED in the live production smoke (plan 133-06 Task 4).
    Using Playwright (a dev/ops tool installed ad hoc -- ``pip install
    playwright && python -m playwright install chromium``; DELIBERATELY NOT
    added to any requirements file), it drives a real headless browser to
    ``<base-url>/atlas`` in EN then HE, waits for renderer readiness
    (``window.__atlasRenderer`` -- set by web/static/js/atlas_decode.js after
    its first successful draw), exercises the interactions that MATERIALIZE the
    catalogue-derived DOM (types into ``#atlas-search``, hovers a star to fire
    the tooltip, clicks a region to build the focus-constellation panel), then
    dumps ``document.documentElement.outerHTML`` per language. This is the ONLY
    capture that sees the catalogue strings, because they render only in the
    client DOM an ASGI GET never receives. If Playwright/Chromium is
    unavailable, this mode SKIPS with a clear message (exit 0 for the caller to
    interpret) -- the guaranteed browser-DOM masking run is the live smoke.

Both capture dirs feed ``scripts/check_atlas_masking.py --scan-asset <dir>``
(recursive; scans .html/.txt). The hard pre-deploy leak gate is the recursive
``--scan-asset atlas_data/`` (the built asset) + the ASGI-captured-HTML scan;
the guaranteed client-DOM scan is the live browser-DOM capture in the smoke.

Usage:
    python scripts/capture_atlas_html.py [--out-dir DIR]
    python scripts/capture_atlas_html.py --browser-dom https://genizahsearch.com [--out-dir DIR]

The restricted reference corpus is referred to ONLY by its codename "M-source"
in this file; this helper never reads or emits any corpus name.
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

# The two page surfaces we capture (the data routes serve the binary asset,
# already covered by --scan-asset atlas_data/; the nav + teaser render inside
# these two page bodies).
_SURFACES = ("/atlas", "/")
_LANGS = ("en", "he")


# ---------------------------------------------------------------------------
# (a) ASGI in-process capture -- the hard pre-deploy gate
# ---------------------------------------------------------------------------

def _element_tree_dump(user) -> str:
    """Serialize every rendered element's visible strings (text, ui.html
    content, string-valued props, markers) so the masking scan sees ALL of the
    server-rendered output -- not just body_html."""
    from nicegui import ui  # noqa: F401 (import side effects / parity with tests)

    lines: list[str] = []
    with user._client:
        for el in list(user._client.elements.values()):
            text = getattr(el, "text", None)
            if isinstance(text, str) and text:
                lines.append(f"text: {text}")
            content = getattr(el, "content", None)
            if isinstance(content, str) and content:
                lines.append(f"content: {content}")
            props = getattr(el, "_props", None)
            if isinstance(props, dict):
                for k, v in props.items():
                    if isinstance(v, str) and v:
                        lines.append(f"prop[{k}]: {v}")
            markers = getattr(el, "_markers", None)
            if markers:
                lines.append(f"markers: {list(markers)}")
    return "\n".join(lines) + "\n"


async def _capture_asgi(out_dir: Path) -> list[Path]:
    """Render /atlas + / (EN + HE) via ASGITransport and write the server HTML."""
    # Import here so a --browser-dom-only run never needs the web app / NiceGUI
    # testing internals.
    import httpx  # noqa: PLC0415
    import web.main as _web_main  # noqa: F401,PLC0415 (registers routes on core.app)
    import web.atlas_assets as aa  # noqa: PLC0415
    from nicegui import core  # noqa: PLC0415
    from nicegui.context import context as _ctx  # noqa: PLC0415
    from nicegui.testing.general import prepare_simulation  # noqa: PLC0415
    from nicegui.testing.user import User  # noqa: PLC0415
    from nicegui.ui_run import set_storage_secret  # noqa: PLC0415
    from unittest.mock import patch  # noqa: PLC0415

    # Load the freshly-baked asset from repo-root atlas_data/ and force the flag
    # ON so atlas_preview_available() is genuinely True (flag AND ready) -- the
    # page route, nav, teaser and data routes all read this same predicate.
    aa.ATLAS_PREVIEW_ENABLED = True
    ready = aa.load_atlas_state()
    if not ready:
        raise RuntimeError(
            "atlas asset failed to load from atlas_data/ -- bake it first "
            "(python scripts/build_atlas_asset.py <research-db>)."
        )
    if not aa.atlas_preview_available():
        raise RuntimeError("atlas_preview_available() is False after loading the asset")

    written: list[Path] = []
    saved_slot_stack = list(_ctx.slot_stack)
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()  # no real SearchEngine build
    try:
        prepare_simulation()
        set_storage_secret("atlas-capture-secret", {})
        for lang in _LANGS:
            with patch("web.main._resolve_ui_language", return_value=lang):
                os.environ["NICEGUI_USER_SIMULATION"] = "true"
                try:
                    async with core.app.router.lifespan_context(core.app):
                        async with httpx.AsyncClient(
                            transport=httpx.ASGITransport(core.app),
                            base_url="http://test",
                        ) as client:
                            user = User(client)
                            for surface in _SURFACES:
                                await user.open(surface)
                                tag = ("home" if surface == "/" else surface.strip("/")) + f"_{lang}"
                                body_path = out_dir / f"{tag}.html"
                                # body_html can raise "Request is not set" in the
                                # in-process simulation for a busy page (it walks
                                # Client.instances for session ids). The
                                # element-tree dump below is the authoritative,
                                # more-complete masking surface, so fall back to
                                # empty rather than crash on that NiceGUI quirk.
                                try:
                                    body = user._client.body_html
                                except Exception:
                                    body = ""
                                body_path.write_text(body, encoding="utf-8")
                                elements_path = out_dir / f"{tag}_elements.txt"
                                elements_path.write_text(
                                    _element_tree_dump(user), encoding="utf-8"
                                )
                                written.extend([body_path, elements_path])
                finally:
                    os.environ.pop("NICEGUI_USER_SIMULATION", None)
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        _ctx.slot_stack.clear()
        _ctx.slot_stack.extend(saved_slot_stack)
    return written


def run_asgi_capture(out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    written = asyncio.run(_capture_asgi(out_dir))
    print(f"ASGI capture: rendered /atlas + / (EN + HE) -> {out_dir}")
    for p in written:
        print(f"  wrote {p}")
    print(
        "\nNow scan the captured server HTML (hard pre-deploy gate):\n"
        f"  python scripts/check_atlas_masking.py --scan-asset {out_dir}"
    )
    return 0


# ---------------------------------------------------------------------------
# (b) Playwright browser-DOM capture -- best-effort pre-deploy, guaranteed live
# ---------------------------------------------------------------------------

# The renderer sets window.__atlasRenderer after its first successful draw
# (web/static/js/atlas_decode.js). That is the documented readiness signal.
_READY_JS = "() => !!window.__atlasRenderer"
_SEARCH_SELECTOR = "#atlas-search"
_CANVAS_SELECTOR = "#atlas-canvas"
# A broad, non-restricted probe query that materializes catalogue-derived search
# result rows without ever typing a restricted term.
_SEARCH_PROBE = "a"


def _exercise_and_dump(page, lang_tag: str, out_dir: Path) -> Path:
    """On an already-loaded /atlas page: exercise the interactions that create
    catalogue DOM (search / hover-tooltip / click-focus), then dump the full
    client DOM. Returns the written path."""
    # Type into the search box -> materializes the search filter + any result DOM.
    try:
        page.fill(_SEARCH_SELECTOR, _SEARCH_PROBE, timeout=5000)
        page.wait_for_timeout(400)
    except Exception as exc:  # search box is best-effort
        print(f"  [warn] search interaction skipped ({lang_tag}): {exc}")
    # Hover the canvas centre -> fires the star tooltip (atlas-tip).
    try:
        box = page.locator(_CANVAS_SELECTOR).bounding_box()
        if box:
            cx = box["x"] + box["width"] / 2
            cy = box["y"] + box["height"] / 2
            page.mouse.move(cx, cy)
            page.wait_for_timeout(250)
            # Click the centre -> builds the focus-constellation panel rows.
            page.mouse.click(cx, cy)
            page.wait_for_timeout(400)
    except Exception as exc:  # hover/click is best-effort
        print(f"  [warn] hover/click interaction skipped ({lang_tag}): {exc}")

    html = page.content()  # document.documentElement.outerHTML equivalent
    dump_path = out_dir / f"browser_atlas_{lang_tag}.html"
    dump_path.write_text(html, encoding="utf-8")
    print(f"  wrote {dump_path}")
    return dump_path


def run_browser_dom_capture(base_url: str, out_dir: Path) -> int:
    try:
        from playwright.sync_api import sync_playwright  # noqa: PLC0415
    except Exception:
        print(
            "browser-dom capture SKIPPED: Playwright is not installed. This mode "
            "is best-effort pre-deploy; the GUARANTEED client-DOM masking scan is "
            "the live production smoke (plan 133-06 Task 4). To enable it here:\n"
            "  pip install playwright && python -m playwright install chromium\n"
            "(do NOT add Playwright to any requirements file).",
            file=sys.stderr,
        )
        return 0

    out_dir.mkdir(parents=True, exist_ok=True)
    base_url = base_url.rstrip("/")
    written: list[Path] = []
    try:
        with sync_playwright() as pw:
            try:
                browser = pw.chromium.launch(headless=True)
            except Exception as exc:
                print(
                    "browser-dom capture SKIPPED: Chromium is not installed for "
                    f"Playwright ({exc}). Run: python -m playwright install chromium",
                    file=sys.stderr,
                )
                return 0
            try:
                for lang in _LANGS:
                    context = browser.new_context(locale=("he-IL" if lang == "he" else "en-US"))
                    page = context.new_page()
                    page.goto(f"{base_url}/atlas", wait_until="domcontentloaded", timeout=30000)
                    # If a HE capture is wanted, click the header language toggle
                    # so the atlas re-renders in Hebrew (best-effort -- the live
                    # smoke does the authoritative EN/HE toggle).
                    if lang == "he":
                        try:
                            page.click(".lang-btn-header", timeout=4000)
                            page.wait_for_load_state("domcontentloaded", timeout=15000)
                            page.goto(
                                f"{base_url}/atlas",
                                wait_until="domcontentloaded",
                                timeout=30000,
                            )
                        except Exception as exc:
                            print(f"  [warn] HE language toggle skipped: {exc}")
                    try:
                        page.wait_for_function(_READY_JS, timeout=20000)
                    except Exception as exc:
                        print(
                            f"  [warn] renderer readiness ({lang}) not observed "
                            f"within timeout ({exc}); dumping current DOM anyway."
                        )
                    written.append(_exercise_and_dump(page, lang, out_dir))
                    context.close()
            finally:
                browser.close()
    except Exception as exc:
        print(f"ERROR: browser-dom capture failed: {exc}", file=sys.stderr)
        return 1

    print(f"\nbrowser-dom capture: dumped client DOM (EN + HE) -> {out_dir}")
    print(
        "Now scan the captured client DOM:\n"
        f"  python scripts/check_atlas_masking.py --scan-asset {out_dir}"
    )
    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _default_out_dir(kind: str) -> Path:
    return Path(tempfile.gettempdir()) / f"atlas_capture_{kind}"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--browser-dom",
        metavar="BASE_URL",
        default=None,
        help="Capture the real client-rendered DOM from a running instance at "
        "BASE_URL via Playwright (best-effort; skips if Playwright is absent)",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Directory to write captures into (default: a temp dir)",
    )
    args = parser.parse_args(argv)

    if args.browser_dom is not None:
        out_dir = Path(args.out_dir) if args.out_dir else _default_out_dir("browser")
        return run_browser_dom_capture(args.browser_dom, out_dir)

    out_dir = Path(args.out_dir) if args.out_dir else _default_out_dir("asgi")
    return run_asgi_capture(out_dir)


if __name__ == "__main__":
    sys.exit(main())
