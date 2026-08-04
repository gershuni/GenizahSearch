# -*- coding: utf-8 -*-
"""Phase 136, plan 136-16 — the corpus-wide "Computed Identifications" findings page.

Task 1 (this file's first section) proves the gating contract:

  * the route is ``/computed-identifications`` and is registered on the app;
  * with ``discovery_available()`` False the route renders the availability card
    and NEVER imports ``web.pages.findings`` — the heavy page module is not
    merely un-called, it is un-imported;
  * with it True the route delegates to ``create_findings_page``;
  * the nav entry is ABSENT (not rendered-and-disabled) when unavailable and
    present when available — asserted behaviourally by driving the real
    ``create_layout()``, not by reading the source;
  * the page meta sets ``noindex=True``;
  * the pre-existing ``/discoveries`` Community route and its nav entry are
    untouched (same English word, adjacent in the same list — the naming hazard
    D-19 called out);
  * ``page_client`` is bound at render time, before the first ``await`` in
    ``create_findings_page`` (an AST assertion — a late binding is a latent
    failure, not an error, because a background context has no UI context to
    read it from).

Later sections cover Task 2 (header/caveat/headline slot/mode strip/filter bar
and the ruling-T "more matches" control) and Task 3 (result bar, pager, service
states).
"""

from __future__ import annotations

import ast
import asyncio
import pathlib
import sys

import pytest

import web.discovery_assets as da
import web.main as wm
from web.translations import set_language

FINDINGS_PY = pathlib.Path(__file__).resolve().parents[1] / "web" / "pages" / "findings.py"
FINDINGS_SRC = FINDINGS_PY.read_text(encoding="utf-8")

MAIN_PY = pathlib.Path(wm.__file__)
MAIN_SRC = MAIN_PY.read_text(encoding="utf-8")

FINDINGS_ROUTE = "/computed-identifications"

#: The pre-existing Supabase Community page. Unrelated to this work, adjacent in
#: the same nav list, and sharing the English word — the collision D-19 settled.
COMMUNITY_ROUTE = "/discoveries"


# ---------------------------------------------------------------------------
# Harness — a minimal NiceGUI client context, mirroring
# tests/test_atlas_flag_gating.py::_run_atlas_route (the already-hardened model
# this route was copied from). No app lifespan, no SearchEngine, no sockets.
# ---------------------------------------------------------------------------

_SIM_READY = False


def _ensure_sim():
    global _SIM_READY
    if not _SIM_READY:
        from nicegui.testing.general import prepare_simulation

        prepare_simulation()
        _SIM_READY = True


def _collect_texts(client) -> list:
    out = []
    for el in client.elements.values():
        for attr in ("text", "_text", "content"):
            val = getattr(el, attr, None)
            if isinstance(val, str) and val.strip():
                out.append(val)
    return out


def _set_availability(monkeypatch, available: bool) -> None:
    """Flip the ONE predicate both surfaces gate on, at both of its inputs."""
    monkeypatch.setattr(da, "DISCOVERY_ENABLED", available)
    monkeypatch.setattr(da, "_state", da._DiscoveryState(ready=available))
    assert da.discovery_available() is available


def _run_findings_route(monkeypatch, *, available: bool, lang: str = "en"):
    """Drive the REAL (decorated) wm.findings_page_route() in a client context.

    Returns (rendered_texts, page_meta_kwargs). The shared shell + storage are
    isolated so this targets the gating logic, not create_layout()."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    meta_kwargs = {}

    def _page_meta(*args, **kwargs):
        meta_kwargs.update(kwargs)
        meta_kwargs["_args"] = args
        return ""

    monkeypatch.setattr(wm, "create_layout", lambda: ui.column())
    monkeypatch.setattr(wm, "safe_user_set", lambda *a, **k: None)
    monkeypatch.setattr(wm, "page_meta", _page_meta)
    monkeypatch.setattr(wm, "apply_theme_immediately", lambda: "")
    _set_availability(monkeypatch, available)
    set_language(lang)

    texts: list = []

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_findings_probe")) as client:
            with client:
                await wm.findings_page_route()
        texts.extend(_collect_texts(client))

    asyncio.run(_run())
    return texts, meta_kwargs


def _run_create_layout(monkeypatch, *, available: bool, lang: str = "en") -> list:
    """Drive the REAL create_layout() and return every rendered label's text."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    _set_availability(monkeypatch, available)
    set_language(lang)
    labels: list = []

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_findings_nav_probe")) as client:
            with client:
                wm.create_layout()
        labels.extend(_collect_texts(client))

    asyncio.run(_run())
    return labels


# ===========================================================================
# TASK 1 — route, availability gate, nav entry
# ===========================================================================

def test_route_is_registered_at_the_computed_identifications_path():
    """The route path is the one the budget document's findings entry names, and
    it is NOT the pre-existing Community route."""
    from web.pages.findings import FINDINGS_ROUTE as MODULE_ROUTE

    assert MODULE_ROUTE == FINDINGS_ROUTE
    assert FINDINGS_ROUTE != COMMUNITY_ROUTE

    paths = {getattr(r, "path", None) for r in wm.app.routes}
    assert FINDINGS_ROUTE in paths, (
        f"{FINDINGS_ROUTE} is not registered on the app. Registered: "
        f"{sorted(p for p in paths if isinstance(p, str))[:40]}"
    )

    # The budget document (docs/specs/discovery-budgets.md §5) is the artifact
    # that names this page; the route must belong to the page it budgets.
    budgets = (
        pathlib.Path(__file__).resolve().parents[1]
        / "docs" / "specs" / "discovery-budgets.md"
    ).read_text(encoding="utf-8")
    assert "Computed Identifications" in budgets, (
        "the budget document no longer names the findings page — the route and "
        "the budget entry must stay in lockstep"
    )


def test_unavailable_route_renders_card_and_never_imports_the_page_builder(monkeypatch):
    """The clean-hide branch must not merely skip the call — it must not IMPORT
    the heavy page module at all (the deferred-import contract)."""
    saved = sys.modules.pop("web.pages.findings", None)
    try:
        texts, _meta = _run_findings_route(monkeypatch, available=False)
        assert "web.pages.findings" not in sys.modules, (
            "the unavailable route imported the page builder — the import must "
            "be deferred into the available branch"
        )
        blob = "\n".join(texts)
        assert "Computed Identifications is not available right now" in blob, (
            f"the availability card did not render. Texts: {texts!r}"
        )
        assert "REAL-FINDINGS-SENTINEL" not in blob
    finally:
        if saved is not None:
            sys.modules["web.pages.findings"] = saved


def test_available_route_delegates_to_create_findings_page(monkeypatch):
    """The complementary case — proves the gate is not a constant always-hide."""
    import web.pages.findings as findings_mod
    from nicegui import ui

    calls = {"n": 0}

    async def _recorder():
        calls["n"] += 1
        ui.label("REAL-FINDINGS-SENTINEL")

    monkeypatch.setattr(findings_mod, "create_findings_page", _recorder)
    texts, _meta = _run_findings_route(monkeypatch, available=True)
    assert calls["n"] == 1, "create_findings_page must be called when available"
    blob = "\n".join(texts)
    assert "REAL-FINDINGS-SENTINEL" in blob
    assert "is not available right now" not in blob


def test_page_meta_sets_noindex(monkeypatch):
    """T-136-16-02: the pre-release surface must not be crawled."""
    import web.pages.findings as findings_mod
    from nicegui import ui

    async def _noop():
        ui.label("x")

    monkeypatch.setattr(findings_mod, "create_findings_page", _noop)
    _texts, meta = _run_findings_route(monkeypatch, available=True)
    assert meta.get("noindex") is True, f"page_meta kwargs were {meta!r}"
    assert meta.get("_args") and meta["_args"][0] == FINDINGS_ROUTE


@pytest.mark.parametrize("lang", ["en", "he"])
def test_nav_entry_absent_when_unavailable_and_present_when_available(monkeypatch, lang):
    """The nav entry DISAPPEARS ENTIRELY in the unavailable state — never
    rendered-and-disabled. Asserted by driving the real create_layout()."""
    from web.translations import tr

    set_language(lang)
    expected = tr("Computed Identifications")

    try:
        on = _run_create_layout(monkeypatch, available=True, lang=lang)
        off = _run_create_layout(monkeypatch, available=False, lang=lang)
    finally:
        set_language("he")

    assert any(text == expected for text in on), (
        f"nav entry {expected!r} missing while discovery is available"
    )
    assert not any(expected in text for text in off), (
        f"nav entry {expected!r} rendered while discovery is UNAVAILABLE — it "
        "must be absent, not disabled"
    )


def test_route_and_nav_gate_on_the_single_availability_predicate():
    """Both surfaces must call discovery_available() — the flag ANDed with the
    sidecar's startup-loaded readiness, never the flag alone."""
    tree = ast.parse(MAIN_SRC)
    by_name = {}
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            by_name.setdefault(node.name, node)

    def _calls_predicate(func_node) -> bool:
        for node in ast.walk(func_node):
            if isinstance(node, ast.Call):
                fn = node.func
                if isinstance(fn, ast.Name) and fn.id == "discovery_available":
                    return True
                if isinstance(fn, ast.Attribute) and fn.attr == "discovery_available":
                    return True
        return False

    missing = []
    for name in ("findings_page_route", "create_layout"):
        node = by_name.get(name)
        if node is None:
            missing.append(f"{name} (function not found)")
        elif not _calls_predicate(node):
            missing.append(f"{name} (no discovery_available() call)")
    assert not missing, (
        "these findings surfaces must gate on discovery_available(): " + ", ".join(missing)
    )
    # The flag alone is never sufficient: the route must never read
    # DISCOVERY_ENABLED itself, only the ANDed predicate.
    route = by_name["findings_page_route"]
    flag_reads = [
        n.id for n in ast.walk(route)
        if isinstance(n, ast.Name) and n.id == "DISCOVERY_ENABLED"
    ]
    assert not flag_reads, (
        "findings_page_route reads DISCOVERY_ENABLED directly — gate on "
        "discovery_available() (flag AND sidecar readiness) instead"
    )


def test_community_discoveries_route_and_nav_entry_are_untouched():
    """The pre-existing Community page shares the English word and sits next to
    the new entry in the same list; this plan must not have edited either."""
    assert "('/discoveries', 'lightbulb', tr('Community'), None)," in MAIN_SRC, (
        "the pre-existing /discoveries nav entry was modified"
    )
    assert "@ui.page('/discoveries'" in MAIN_SRC, "the /discoveries route was modified"

    # The Community route must not have acquired the discovery gate.
    tree = ast.parse(MAIN_SRC)
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and "discoveries" in node.name:
            if node.name.startswith("findings"):
                continue
            src = ast.get_source_segment(MAIN_SRC, node) or ""
            assert "discovery_available" not in src, (
                f"{node.name} (the Community page) must not gate on the discovery predicate"
            )


def test_page_client_is_bound_before_the_first_await():
    """`page_client` must be captured at RENDER time, inside the UI context,
    before any awaited work — a late binding silently reads no context."""
    tree = ast.parse(FINDINGS_SRC)
    func = next(
        (n for n in ast.walk(tree)
         if isinstance(n, ast.AsyncFunctionDef) and n.name == "create_findings_page"),
        None,
    )
    assert func is not None, "create_findings_page not found"

    bind_line = None
    for node in ast.walk(func):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "page_client":
                    bind_line = node.lineno if bind_line is None else min(bind_line, node.lineno)
    assert bind_line is not None, "create_findings_page never binds page_client"

    await_lines = [n.lineno for n in ast.walk(func) if isinstance(n, ast.Await)]
    if await_lines:
        assert bind_line < min(await_lines), (
            f"page_client is bound at line {bind_line} but the first await is at "
            f"line {min(await_lines)} — bind at render time, before any await"
        )


def test_per_user_state_goes_through_the_storage_chokepoint():
    """T-136-16-07: no raw app.storage.user access in the page module.

    The repo-wide guard (tests/test_no_raw_storage_access.py) enforces this for
    every file; this is the module-scoped restatement so a regression here names
    this plan rather than the whole allowlist."""
    # AST, not a substring scan: this module's own comments legitimately NAME
    # the thing they forbid, and a grep would fail on the documentation rather
    # than on a real access.
    tree = ast.parse(FINDINGS_SRC)
    raw = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Attribute)
        and node.attr == "storage"
        and isinstance(node.value, ast.Name)
        and node.value.id == "app"
    ]
    assert not raw, (
        "web/pages/findings.py touches app.storage directly at line(s) "
        f"{[n.lineno for n in raw]} — use web/safe_storage.py's "
        "safe_user_get / safe_user_set"
    )
    assert "safe_user_get" in FINDINGS_SRC and "safe_user_set" in FINDINGS_SRC
