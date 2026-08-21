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


# ===========================================================================
# TASK 2 — header + reserved headline slot + permanent caveat, mode strip,
# filter bar, and the ruling-T "more matches" control
# ===========================================================================

import os as _os  # noqa: E402
import re as _re  # noqa: E402
from contextlib import ExitStack  # noqa: E402
from unittest.mock import patch  # noqa: E402

import web.pages.findings as fp  # noqa: E402
from tests.render_smoke.discovery_honesty_gate import assert_discovery_honesty  # noqa: E402

#: A digit in any script the two rendered languages can produce.
_DIGIT_RE = _re.compile(r"[0-9٠-٩۰-۹]")

#: The curated work whose bare title misleads (ruling R). Its raw recorded
#: title is deliberately DIFFERENT from its curated display title.
_CURATED_WORK_ID = "w000176"
_CURATED_RAW_TITLE = "משנה תורה, ספר אהבה"
_UNCURATED_WORK_ID = "w000999"
_UNCURATED_RAW_TITLE = "Some Uncurated Work"


# ---------------------------------------------------------------------------
# Fixtures + render harness for the page builder itself.
# ---------------------------------------------------------------------------

def _finding_row(work_id: str, title: str, shelfmark: str) -> dict:
    return {
        "unit": "identification",
        "identification_id": f"id-{work_id}-{shelfmark}",
        "sys_id": "990000000000000000",
        "canonical_work_id": work_id,
        "display_work_id": work_id,
        "neutral_title": title,
        "author": None,
        "genre": "Liturgy",
        "domain": "Liturgy",
        "library_code": "CUL",
        "shelfmark_display": shelfmark,
        "main_pool": True,
        "relation_kind": "direct_witness",
        "novelty_status": "fills_gap",
        "novelty_offered": True,
        "page_count": 2,
    }


MAIN_ROW_TITLE = "MAIN-POOL-ROW-SENTINEL"
MORE_ROW_TITLE = "MORE-MATCHES-ROW-SENTINEL"

_ROWS_BY_BUCKET = {
    "main": [_finding_row("w000001", MAIN_ROW_TITLE, "T-S 12.111")],
    "more": [_finding_row("w000002", MORE_ROW_TITLE, "T-S 12.222")],
}

_FACET_ITEMS = {
    "domain": [
        {"level": "domain", "value": "Liturgy", "label": "Liturgy",
         "parent": None, "is_leaf": True, "count": 7},
    ],
    "author": [
        {"level": "author", "value": "Maimonides", "label": "Maimonides",
         "parent": None, "is_leaf": True, "count": 3},
    ],
    "work": [
        {"level": "work", "value": _CURATED_WORK_ID, "label": _CURATED_RAW_TITLE,
         "parent": None, "is_leaf": True, "count": 5},
        {"level": "work", "value": _UNCURATED_WORK_ID, "label": _UNCURATED_RAW_TITLE,
         "parent": None, "is_leaf": True, "count": 1},
    ],
}


def _fake_findings(rows_by_bucket=None, *, total=None, status="ok", meta_extra=None):
    rows_by_bucket = _ROWS_BY_BUCKET if rows_by_bucket is None else rows_by_bucket

    async def _call(unit="identification", *, bucket="main", sort="band_rank", **_kw):
        items = list(rows_by_bucket.get(bucket, []))
        return {
            "status": status,
            "items": items if status == "ok" else [],
            "total": (total if total is not None else len(items)) if status == "ok" else 0,
            "meta": {
                "unit": unit, "bucket": bucket, "sort": sort,
                "sort_basis": "best_band_rank", "novelty_offered": True,
                # The shipped reader always reports which population it
                # counted; a stub that omitted it would let the result bar's
                # reconciliation line go silent in every test that uses this.
                # `divergent_included` is the key the bar reads as of 2026-08-06
                # (`divergence` is pinned to `shown` by the page and no longer
                # varies with the reader's choice, so it cannot answer this).
                "divergence": "shown",
                "divergent_included": True,
                "approximate_total": False, **(meta_extra or {}),
            },
        }

    return _call


def _fake_facets(items_by_level=None, *, status="ok"):
    items_by_level = _FACET_ITEMS if items_by_level is None else items_by_level

    async def _call(level, **_kw):
        items = list(items_by_level.get(level, []))
        return {
            "status": status,
            "items": items if status == "ok" else [],
            "total": len(items) if status == "ok" else 0,
            "meta": {"level": level},
        }

    return _call


#: The second pool's size as a SENTINEL -- a figure that appears in no artifact
#: and in no committed file, so a rendered occurrence of it can only have come
#: through `meta.more_pool_total`. A real figure here would prove nothing: it
#: could equally have been hardcoded, which is the defect ruling U forbids.
SENTINEL_MORE_POOL_TOTAL = 818_181


def _fake_launch(*, status="ok", meta_extra=None):
    """The launch envelope the page reads ONCE and two surfaces consume.

    NOT stubbed by default -- most tests here let the real (unavailable) read
    run, which is exactly the degraded path the pool invitation has to survive.
    """
    async def _call(*_args, **_kwargs):
        return {
            "status": status,
            "items": [],
            "total": 0,
            "meta": {"basis": "main_pool", **(meta_extra or {})} if status == "ok"
            else {"reason": "sidecar_not_serving"},
        }

    return _call


def _render_page(monkeypatch, *, lang="en", findings=None, facets=None, state=None,
                 launch=None, driver=None):
    """Render the REAL create_findings_page() in a bare client context.

    Returns the NiceGUI Client; tests walk `client.elements` directly.

    `driver(client)` is awaited INSIDE the same event loop and after the page
    has painted, which is the only way to drive a control whose handler is a
    coroutine that awaits the page's own refresh path. A test that clicks after
    `asyncio.run` has returned has no loop to await on, and one that calls the
    handler synchronously never runs the refresh at all -- which is exactly the
    class of defect this harness exists to catch."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    monkeypatch.setattr(fp, "get_findings_enveloped", findings or _fake_findings())
    monkeypatch.setattr(fp, "get_findings_facets_enveloped", facets or _fake_facets())

    async def _no_approved_reviews(_items):
        return {}

    monkeypatch.setattr(fp, "_fetch_approved_review_map", _no_approved_reviews)
    if launch is not None:
        monkeypatch.setattr(fp, "get_launch_stats_enveloped", launch)
    if state is not None:
        monkeypatch.setattr(fp, "read_state", lambda: dict(state))
    set_language(lang)

    holder = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_findings_page_probe")) as client:
            with client:
                await fp.create_findings_page()
                if driver is not None:
                    await driver(client)
        holder["client"] = client

    try:
        asyncio.run(_run())
    finally:
        set_language("he")
    return holder["client"]


def _elements_with_class(client, marker: str) -> list:
    return [
        el for el in client.elements.values()
        if marker in (getattr(el, "_classes", None) or [])
    ]


def _subtree_strings(element) -> list:
    out = []
    for node in element.descendants(include_self=True):
        for attr in ("text", "_text", "content"):
            value = getattr(node, attr, None)
            if isinstance(value, str) and value.strip():
                out.append(value)
        for value in (getattr(node, "_props", None) or {}).values():
            if isinstance(value, str) and value.strip():
                out.append(value)
    return out


def _scoped_text(client, marker: str) -> str:
    parts = []
    for element in _elements_with_class(client, marker):
        parts.extend(_subtree_strings(element))
    return "\n".join(parts)


def _scoped_fragment(client, marker: str) -> str:
    """A class-scoped HTML fragment for the SHARED honesty gate, which extracts
    by class token over real markup (its scope argument is mandatory)."""
    import html as _html

    return f'<div class="{marker}">{_html.escape(_scoped_text(client, marker))}</div>'


def _ancestors(element) -> list:
    out = []
    slot = getattr(element, "parent_slot", None)
    while slot is not None:
        parent = getattr(slot, "parent", None)
        if parent is None:
            break
        out.append(parent)
        slot = getattr(parent, "parent_slot", None)
    return out


def _find_bucket_control(client, lang: str):
    """The "more matches" control, located by its ACCESSIBLE NAME — exactly the
    way a reader (or a screen reader) would find it."""
    from shared.discovery_display_strings import bucket_name

    wanted = bucket_name(False, lang)
    for element in client.elements.values():
        props = getattr(element, "_props", None) or {}
        if props.get("label") == wanted:
            return element
        if getattr(element, "text", None) == wanted:
            return element
    return None


# ---------------------------------------------------------------------------
# The permanent caveat slot
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_caveat_renders_between_header_and_body_and_passes_the_honesty_gate(monkeypatch, lang):
    client = _render_page(monkeypatch, lang=lang)

    caveats = _elements_with_class(client, fp.CAVEAT_CLASS)
    assert len(caveats) == 1, f"expected exactly one caveat slot, got {len(caveats)}"

    caveat = caveats[0]
    assert "caveat" in (caveat._classes or []), (
        "the caveat must carry the sketch's `caveat` class or the gold "
        "inline-start rule in the shared CSS block never applies"
    )

    # It sits INSIDE the page head (between header and body), not in a footer.
    ancestor_classes = {c for a in _ancestors(caveat) for c in (a._classes or [])}
    assert fp.HEAD_CLASS in ancestor_classes, (
        "the caveat must sit in the page head, between the header and the body"
    )

    text = _scoped_text(client, fp.CAVEAT_CLASS)
    assert text.strip(), "the caveat rendered empty"
    # The SHARED gate — including the negation-proof prohibited-wording rule
    # that caught the sketch's own first draft.
    assert_discovery_honesty(
        _scoped_fragment(client, fp.CAVEAT_CLASS),
        scope_selector=fp.CAVEAT_CLASS,
        lang=lang,
    )


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_caveat_carries_an_icon_and_stays_undismissible(monkeypatch, lang):
    """"The disclaimer text is easily missed" (owner report, 2026-08-06).

    The caveat already had a tinted plate and a gold inline-start rule and was
    STILL being read past, so what it lacked was the one signal that marks a block
    as an advisory rather than as more prose. `info` and not `warning`: nothing
    here is going wrong, and an alarm glyph on a PERMANENT element trains a reader
    to dismiss it.

    The dismissibility half is asserted in the same test because that is the
    property an icon most invites someone to break: a glyph makes the block look
    like a Quasar banner, and the obvious next edit is a close button. This
    caveat is permanent by design, so there must be no control inside it.
    """
    client = _render_page(monkeypatch, lang=lang)
    caveat = _elements_with_class(client, fp.CAVEAT_CLASS)[0]

    icons = [element for element in caveat.descendants()
             if (getattr(element, "tag", "") or "").lower() in ("q-icon", "i")
             or "q-icon" in (element._classes or [])]
    assert icons, "the caveat renders no icon -- it reads as more prose"

    # NOT an alarm glyph on a permanent element.
    names = {(element._props or {}).get("name") for element in icons}
    assert "warning" not in names and "error" not in names, (
        f"the caveat uses an alarm glyph {names!r} on a permanent element")

    # NOT DISMISSIBLE: no button, no link, nothing clickable inside it.
    for element in caveat.descendants():
        tag = (getattr(element, "tag", "") or "").lower()
        assert tag not in ("q-btn", "a"), (
            f"the caveat gained a {tag!r} -- it is permanent, never dismissible")

    # ...and the text is still there in full, beside the glyph rather than
    # replaced by it.
    assert fp.copy_text("caveat", lang) in _scoped_text(client, fp.CAVEAT_CLASS)


def test_caveat_wording_differs_between_the_two_languages():
    """A half-filled bilingual entry would silently fall back to English."""
    en = fp.copy_text("caveat", "en")
    he = fp.copy_text("caveat", "he")
    assert en and he and en != he
    assert not he.isascii(), "the Hebrew caveat is not Hebrew"


@pytest.mark.parametrize("lang", ["en", "he"])
def test_page_local_copy_passes_the_shared_honesty_gate(lang):
    """Every one of the page-local strings, in both languages, through the SAME
    gate every other discovery string goes through."""
    import html as _html

    assert fp.copy_keys(), "the page-local copy table is empty"
    for key in fp.copy_keys():
        value = fp.copy_text(key, lang)
        assert value.strip(), f"page-local copy {key!r} is empty in {lang}"
        fragment = f'<div class="probe">{_html.escape(value)}</div>'
        assert_discovery_honesty(fragment, scope_selector="probe", lang=lang)


def test_page_local_copy_table_is_bilingually_complete():
    for key in fp.copy_keys():
        en = fp.copy_text(key, "en")
        he = fp.copy_text(key, "he")
        assert en.strip() and he.strip(), f"{key!r} is half-filled"
        assert en != he, f"{key!r} has the same value in both languages"


# ---------------------------------------------------------------------------
# The RESERVED launch-headline slot (ruling U)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_headline_slot_is_present_and_contains_no_digit(monkeypatch, lang):
    """The property asserted is the ABSENCE OF ANY DIGIT in that subtree, not
    the absence of a named list of figures: a list of digits fixed into a test
    rots the moment the artifact moves, which is the defect ruling U was issued
    to prevent, one layer up. The figure-specific, artifact-derived guard
    belongs to plan 136-22 and is run as a gate by plan 136-18."""
    client = _render_page(monkeypatch, lang=lang)

    slots = _elements_with_class(client, fp.HEADLINE_SLOT_CLASS)
    assert len(slots) == 1, (
        f"the reserved headline region must render exactly once, got {len(slots)}"
    )
    text = _scoped_text(client, fp.HEADLINE_SLOT_CLASS)
    found = _DIGIT_RE.findall(text)
    assert not found, (
        f"the reserved headline region contains digit(s) {found!r} — plan 136-16 "
        "reserves the slot and writes NO launch number into it; 136-18 fills it "
        f"from 136-22's artifact-backed reader. Subtree text: {text!r}"
    )
    # The scaffolding is bilingual: the region carries an accessible label.
    assert fp.copy_text("headline_slot_label", lang) in text


def _docstring_nodes(tree) -> set:
    """Every docstring Constant node id in `tree`. A docstring may legitimately
    cite plan numbers and may legitimately NAME the strings it forbids; only
    non-docstring literals can reach markup."""
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            body = getattr(node, "body", None) or []
            if body and isinstance(body[0], ast.Expr) and isinstance(body[0].value, ast.Constant):
                if isinstance(body[0].value.value, str):
                    out.add(id(body[0].value))
    return out


def _css_class_literal_ids(tree) -> set:
    """Constant nodes that are arguments to `.classes(...)` — CSS utility names
    (`w-full`, `gap-1`, `text-2xl`) legitimately contain digits and never reach
    the reader as words."""
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr != "classes":
                continue
            for arg in node.args:
                for inner in ast.walk(arg):
                    if isinstance(inner, ast.Constant):
                        out.add(id(inner))
    return out


def test_module_writes_no_digit_bearing_user_facing_string_into_the_headline():
    """In code AND in the bilingual entries this plan adds.

    Ruling U constraint 2: a hardcoded launch number is the same class of bug as
    the figure it replaced, and would not survive the next bake."""
    for lang in ("en", "he"):
        label = fp.copy_text("headline_slot_label", lang)
        assert not _DIGIT_RE.search(label), (
            f"the headline slot's {lang} label carries a digit: {label!r}"
        )

    tree = ast.parse(FINDINGS_SRC)
    skip = _docstring_nodes(tree) | _css_class_literal_ids(tree)
    func = next(
        (n for n in ast.walk(tree)
         if isinstance(n, ast.FunctionDef) and n.name == "_render_headline_slot"),
        None,
    )
    assert func is not None, "_render_headline_slot not found"
    for node in ast.walk(func):
        if not isinstance(node, ast.Constant) or id(node) in skip:
            continue
        if isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
            pytest.fail(
                f"_render_headline_slot contains a numeric literal {node.value!r}"
            )
        if isinstance(node.value, str) and _DIGIT_RE.search(node.value):
            pytest.fail(
                "_render_headline_slot contains a digit-bearing string literal "
                f"{node.value!r}"
            )


# ---------------------------------------------------------------------------
# Ruling T — the "more matches" control, gated on OPERABILITY
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_more_matches_control_is_present_in_the_filter_bar(monkeypatch, lang):
    """(a) Scoped to the FILTER BAR element, not the page, and located by the
    accessible name `bucket_label(False, lang)` produces."""
    from shared.discovery_display_strings import bucket_name

    client = _render_page(monkeypatch, lang=lang)
    bars = _elements_with_class(client, fp.FILTER_BAR_CLASS)
    assert len(bars) == 1, f"expected one filter bar, got {len(bars)}"

    wanted = bucket_name(False, lang)
    bar_strings = _subtree_strings(bars[0])
    assert wanted in bar_strings, (
        f"the {wanted!r} control is not in the filter bar. Filter-bar strings: "
        f"{bar_strings!r}"
    )
    # And the FIRST bucket is named too, so neither state is anonymous.
    assert bucket_name(True, lang) in bar_strings


@pytest.mark.parametrize("lang", ["en", "he"])
def test_more_matches_control_sits_in_no_overflow_or_disclosure_ancestor(monkeypatch, lang):
    """(b) A SUPPLEMENT, explicitly NOT sufficient alone: a child can be
    display:block under an ancestor collapsed by visibility, height, clipping or
    a framework expansion container, none of which an ancestry check can see.
    That is exactly what the real-browser check (e) exists for."""
    client = _render_page(monkeypatch, lang=lang)
    control = _find_bucket_control(client, lang)
    assert control is not None, "the 'more matches' control did not render"

    forbidden_tags = {"details", "q-menu", "q-expansion-item", "q-drawer", "q-dialog"}
    forbidden_class_fragments = ("overflow", "advanced", "collapse", "accordion", "hidden")

    ancestors = _ancestors(control)
    for ancestor in ancestors:
        tag = (getattr(ancestor, "tag", "") or "").lower()
        assert tag not in forbidden_tags, (
            f"the 'more matches' control sits inside a <{tag}> disclosure container"
        )
        classes = " ".join(ancestor._classes or []).lower()
        for fragment in forbidden_class_fragments:
            assert fragment not in classes, (
                f"the 'more matches' control sits inside a {fragment!r} container "
                f"(classes: {classes!r})"
            )

    ancestor_classes = {c for a in ancestors for c in (a._classes or [])}
    assert fp.FILTER_BAR_CLASS in ancestor_classes, (
        "the 'more matches' control must be a first-class filter-bar control"
    )
    # And it is never below the results.
    assert fp.RESULTS_CLASS not in ancestor_classes


@pytest.mark.parametrize("lang", ["en", "he"])
def test_more_matches_control_subtree_carries_no_digit_and_no_count(monkeypatch, lang):
    """(d) Ruling T: no number may be attached to this control, anywhere."""
    client = _render_page(monkeypatch, lang=lang)
    groups = _elements_with_class(client, fp.BUCKET_CONTROL_CLASS)
    assert groups, "the bucket control row did not render"
    text = "\n".join(_subtree_strings(groups[0]))
    found = _DIGIT_RE.findall(text)
    assert not found, (
        f"the bucket control's subtree carries digit(s) {found!r}: {text!r}"
    )


def test_more_matches_control_uses_the_shared_bucket_vocabulary_not_a_local_string():
    """Match framing, from the ONE definition of the two bucket names.

    Scoped to non-docstring STRING LITERALS: the module's prose legitimately
    explains what the second bucket means, and a raw substring scan would fail
    on the documentation rather than on a retyped string."""
    assert "bucket_name" in FINDINGS_SRC
    tree = ast.parse(FINDINGS_SRC)
    skip = _docstring_nodes(tree)
    literals = [
        n.value for n in ast.walk(tree)
        if isinstance(n, ast.Constant) and isinstance(n.value, str) and id(n) not in skip
    ]
    for forbidden in ("more matches", "התאמות נוספות", "main pool", "מאגר עיקרי"):
        offenders = [lit for lit in literals if forbidden in lit]
        assert not offenders, (
            f"the bucket name {forbidden!r} is retyped as a literal in "
            f"web/pages/findings.py ({offenders!r}) — it has exactly one "
            "definition, in shared/discovery_main_pool.py"
        )


# ---------------------------------------------------------------------------
# Mode strip
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_mode_strip_renders_three_modes_with_two_inert_and_badged(monkeypatch, lang):
    from web.translations import tr as _tr

    client = _render_page(monkeypatch, lang=lang)
    assert len(_elements_with_class(client, fp.MODES_CLASS)) == 1

    items = _elements_with_class(client, f"{fp.MODES_CLASS}-item")
    assert len(items) == 3, f"expected three modes, got {len(items)}"

    set_language(lang)
    try:
        labels = [(el._props or {}).get("label") for el in items]
        enabled = [el for el in items if getattr(el, "enabled", True)]
        disabled = [el for el in items if not getattr(el, "enabled", True)]
        assert _tr("All findings") in labels
        assert _tr("Screening leads") in labels
        assert _tr("My saved") in labels
        assert len(enabled) == 1, "exactly one mode ships live"
        assert (enabled[0]._props or {}).get("label") == _tr("All findings")
        assert len(disabled) == 2, "the two future modes must be inert (not clickable)"
        for el in disabled:
            assert "future" in (el._classes or []), (
                "a future mode must carry the `mode future` treatment"
            )
        tags = [t for el in _elements_with_class(client, f"{fp.MODES_CLASS}-phase")
                for t in _subtree_strings(el)]
        assert tags and all(t == _tr("Coming soon") for t in tags), (
            f"an inert mode must be badged for a READER; got {tags!r}"
        )
        # The badge must never carry internal planning vocabulary again. A plan
        # number tells a reader nothing about what the tab is or when it lands.
        blob = " ".join(tags)
        assert "Phase" not in blob and "שלב" not in blob, (
            f"internal phase vocabulary is showing on a reader-facing badge: {tags!r}"
        )
    finally:
        set_language("he")


# ---------------------------------------------------------------------------
# Novelty switch position, the absent grade filter, the disabled filter
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_novelty_switch_renders_first_in_the_filter_bar(monkeypatch, lang):
    """First by CSS `order`, regardless of DOM order — which is what makes it
    first in BOTH directions (LTR and RTL alike; `order` is direction-agnostic
    and the rule is the one the sketch validated)."""
    client = _render_page(monkeypatch, lang=lang)
    groups = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-novelty")
    assert len(groups) == 1, "the novelty switch group did not render"
    classes = groups[0]._classes or []
    assert "fg" in classes and "novgrp" in classes, (
        f"the novelty group must carry `fg novgrp`; got {classes!r}"
    )
    ancestor_classes = {c for a in _ancestors(groups[0]) for c in (a._classes or [])}
    assert fp.FILTER_BAR_CLASS in ancestor_classes

    css = (
        pathlib.Path(__file__).resolve().parents[1] / "web" / "static" / "common.css"
    ).read_text(encoding="utf-8")
    assert ".gs-discovery .fg.novgrp { order: -1; }" in css, (
        "the `order: -1` rule that puts the novelty switch first is missing from "
        "the shared discovery CSS block"
    )


def test_no_grade_filter_control_exists():
    """Quality is the bucket and kind is the panel's relation filter (D-16), so a
    grade-labelled control here would be a second vocabulary for an axis the
    rows no longer speak."""
    assert not _re.search(r"(?i)tier", FINDINGS_SRC), (
        "web/pages/findings.py names a grade/tier control"
    )
    # The three deleted confidence-scale levels, as WHOLE WORDS. `\bStrong\b`
    # deliberately does not match the "Strongest first" SORT label, which is
    # legitimate page chrome from plan 136-10 and is an ordering, not a grade.
    for token in ("confidence", "Strong", "Medium", "Weak"):
        hit = _re.search(r"\b" + token + r"\b", FINDINGS_SRC)
        assert hit is None, (
            f"web/pages/findings.py carries the deleted confidence-scale token "
            f"{token!r} at offset {hit.start() if hit else -1}"
        )


@pytest.mark.parametrize("lang", ["en", "he"])
def test_facet_group_with_an_outage_renders_disabled_rather_than_absent(monkeypatch, lang):
    """Never silently absent: a filter that vanishes is indistinguishable from a
    filter that never existed.

    This carries the AMBER-TAG assertion that used to live on the coverage
    filter. That filter was deleted on 2026-08-06 -- it was permanently
    disabled and the axis is not coming, because the main-pool rule already
    gates on `COVERAGE_FLOOR`, so the pool control IS the coverage filter -- but
    the principle it demonstrated is still load-bearing HERE, where a real
    outage really can withdraw a control's backing data. Deleting the test with
    the control would have taken the only guard on the amber treatment with it.
    """
    client = _render_page(monkeypatch, lang=lang, facets=_fake_facets(status="unavailable"))
    for level in ("domain", "author", "work"):
        blocked = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}-blocked")
        assert blocked, f"the {level} facet vanished on an outage instead of disabling"
        assert "blocked" in (blocked[0]._classes or [])

    tag_text = "\n".join(
        t for el in _elements_with_class(client, "needs") for t in _subtree_strings(el)
    )
    assert fp.copy_text("needs_tag", lang) in tag_text, (
        "a control whose backing data is missing must carry the amber tag -- "
        "dimming alone reads as 'nothing selected', not as 'unavailable'")


# ---------------------------------------------------------------------------
# Ruling R — work-facet labels
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_work_facet_label_routes_through_display_work_title(monkeypatch, lang):
    from shared.discovery_display_strings import display_work_title

    client = _render_page(monkeypatch, lang=lang)
    # Read the SELECT's own option labels: the work facet is a searchable
    # select now, so what a reader can pick lives in props rather than in
    # element text. An assertion over the subtree would pass over an empty list.
    # The count is stripped, so these stay EXACT comparisons: the curated
    # Hebrew title begins with the raw one, and a prefix test would report the
    # raw title as present on the label that proves the curation ran.
    labels = _facet_option_names(client, "work")
    assert labels, "the work facet list did not render"

    expected = display_work_title(_CURATED_WORK_ID, _CURATED_RAW_TITLE, lang)
    assert expected != _CURATED_RAW_TITLE, (
        "fixture error: the curated title must differ from the raw one, or this "
        "test can pass for the wrong reason"
    )
    assert expected in labels, (
        f"the curated display title {expected!r} is missing from the work facet "
        f"list. Rendered: {labels!r}"
    )
    assert _CURATED_RAW_TITLE not in labels, (
        f"the RAW recorded title {_CURATED_RAW_TITLE!r} reached the work facet "
        "list — that opts out of ruling R in the very control a reader uses to "
        "find that work"
    )
    # An uncurated work passes through unchanged.
    assert _UNCURATED_RAW_TITLE in labels


# ---------------------------------------------------------------------------
# String sourcing
# ---------------------------------------------------------------------------

_TEXT_RENDERING_CALLS = {"label", "button", "tooltip"}


def test_no_inline_user_facing_literal_in_the_module():
    """Every rendered string comes from `tr()`, from the shared claim vocabulary,
    or from the audited page-local table — never a bare literal at a render
    call. Literal punctuation inside an f-string is allowed; literal WORDS are
    not."""
    tree = ast.parse(FINDINGS_SRC)
    offenders = []
    word_re = _re.compile(r"[A-Za-z֐-׿]")

    def _check(node, where):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            if word_re.search(node.value):
                offenders.append(f"{where} line {node.lineno}: {node.value!r}")
        elif isinstance(node, ast.JoinedStr):
            for part in node.values:
                if isinstance(part, ast.Constant) and isinstance(part.value, str):
                    if word_re.search(part.value):
                        offenders.append(
                            f"{where} line {node.lineno}: f-string {part.value!r}"
                        )

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr in _TEXT_RENDERING_CALLS:
            if node.args:
                _check(node.args[0], f"{func.attr}()")
        if isinstance(func, ast.Attribute) and func.attr == "select":
            for kw in node.keywords:
                if kw.arg == "label":
                    _check(kw.value, "select(label=)")

    assert not offenders, (
        "inline user-facing literal(s) at a render call: " + "; ".join(offenders)
    )


def test_bucket_more_matches_the_service_constant():
    """The one vocabulary value this module names is pinned to the service's own
    definition — a rename there fails HERE rather than at request time."""
    from shared.discovery_service import BUCKET_MORE as SERVICE_BUCKET_MORE

    assert fp.BUCKET_MORE == SERVICE_BUCKET_MORE


# ---------------------------------------------------------------------------
# Off-loop discipline — asserted where the standing guard cannot see it
# ---------------------------------------------------------------------------

#: The ONLY modules this page may await an offloading wrapper from, each with
#: the reason it is here. Naming them in one place is the point: a THIRD module
#: appearing in `web/pages/findings.py` fails the equality assertion below by
#: name, so "the page grew a new I/O path nobody offloaded" cannot pass quietly.
#:
#: AMENDED 2026-08-04. The rule was "every read is an await on a `web.discovery`
#: wrapper", which was the whole truth while every read this page issued was a
#: discovery-sidecar read. Closing the Hebrew-genre-label gap added a second
#: read — the FJMS bilingual domain vocabulary — and it is NOT a discovery read
#: and does not belong behind a discovery wrapper. The PROPERTY is unchanged and
#: is what actually matters: no synchronous I/O on the event loop, no nested
#: offload, exactly one executor dispatch per read. Both modules below satisfy
#: it, and `test_priming_the_domain_labels_dispatches_once_and_then_never_again`
#: proves the new one does rather than assuming it.
_OFFLOAD_WRAPPER_MODULES = {
    "web.discovery": "every discovery-sidecar read (run_in_executor + asyncio.wait)",
    "web.discovery_genre_labels": (
        "the FJMS bilingual domain vocabulary, primed once per process through "
        "web.bounded_io.bounded_io_bound"
    ),
    "web.identification_reviews": (
        "the identity-free approved-review batch, dispatched once through "
        "web.bounded_io.bounded_io_bound"
    ),
}


def test_module_adds_no_nested_offload_and_no_direct_service_call():
    """(i) no run.io_bound, (ii) no access to the composition module's private
    singleton, (iii) no call to a service-module symbol, (iv) every awaited read
    is a name imported from one of the declared offloading wrapper modules.

    tests/test_no_await_sync_function.py passes too, but it detects ONLY an
    `await` on a LOCALLY defined synchronous `def` and could not have caught any
    of these four failure modes."""
    tree = ast.parse(FINDINGS_SRC)

    # (i) NESTED OFFLOAD. Asserted over CODE, not over the whole file: the
    # module's own docstring names `bounded_io_bound` in order to say which
    # wrapper does the offloading and where, and a raw substring scan fails on
    # that explanation rather than on a defect. What is forbidden is a
    # dispatching CALL or IMPORT here — the page must never wrap an
    # already-offloading wrapper.
    offload_tokens = {"io_bound", "run_in_executor", "to_thread"}
    for node in ast.walk(tree):
        if isinstance(node, (ast.Name, ast.Attribute)):
            token = getattr(node, "id", None) or getattr(node, "attr", None)
            assert token not in offload_tokens, (
                f"(i) nested offload: {token!r} used at line {node.lineno} — this "
                "page awaits wrappers that already dispatch exactly once"
            )
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = {(alias.asname or alias.name).split(".")[-1] for alias in node.names}
            assert not (names & offload_tokens), (
                f"(i) nested offload: {sorted(names & offload_tokens)} imported at "
                f"line {node.lineno}"
            )
    assert "_service" not in FINDINGS_SRC, (
        "(ii)/(iii) the module names the service module or its private singleton"
    )

    # (iv) every awaited read resolves to an import from a DECLARED wrapper
    # module, and the set of such modules is exactly the declared one.
    wrapper_names = set()
    imported_modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module in _OFFLOAD_WRAPPER_MODULES:
            imported_modules.add(node.module)
            wrapper_names.update(alias.asname or alias.name for alias in node.names)
    assert {"get_findings_enveloped", "get_findings_facets_enveloped"} <= wrapper_names
    assert imported_modules <= set(_OFFLOAD_WRAPPER_MODULES), (
        f"undeclared offload wrapper module(s): "
        f"{sorted(imported_modules - set(_OFFLOAD_WRAPPER_MODULES))}"
    )

    local_async = {n.name for n in ast.walk(tree) if isinstance(n, ast.AsyncFunctionDef)}
    for node in ast.walk(tree):
        if not isinstance(node, ast.Await):
            continue
        value = node.value
        if not isinstance(value, ast.Call):
            continue
        func = value.func
        # `await ui.<something>()` is a CLIENT round trip, not a read: it can
        # only reach the browser, so it cannot be the nested offload or the
        # direct service call this check exists to forbid. Admitted
        # STRUCTURALLY -- the receiver must be the name `ui` -- rather than by
        # listing method names, so the exception cannot widen into "anything
        # with a familiar-looking attribute". (Added 2026-08-21 with the
        # export's download-completion handshake, which awaits
        # `ui.run_javascript` to learn when the file actually arrived.)
        if (isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name)
                and func.value.id == "ui"):
            continue
        name = func.id if isinstance(func, ast.Name) else getattr(func, "attr", None)
        if name is None:
            continue
        assert name in wrapper_names or name in local_async or name == "refresh", (
            f"awaited call {name!r} at line {node.lineno} is in none of: the "
            f"declared offloading wrappers {sorted(_OFFLOAD_WRAPPER_MODULES)}, this "
            "module's own async helpers"
        )


def _spy_executor(loop):
    calls = []
    original = loop.run_in_executor

    def spy(executor, fn, *args):
        calls.append(getattr(fn, "__name__", repr(fn)))
        return original(executor, fn, *args)

    loop.run_in_executor = spy
    return calls, original


_PROBE_STATE = {
    "unit": "identification", "bucket": "main", "sort": "band_rank",
    "novelty_view": "all", "domain": None, "author": None, "work_id": None,
    "page": 1,
}


def test_exactly_one_executor_dispatch_per_enveloped_read_when_available():
    """One dispatch per READ on an AVAILABLE, cache-cold, successful call.

    Not zero (a synchronous call on the loop, which on a single-uvicorn-worker
    server stalls every concurrent request while burning no CPU, so it is
    invisible in load average) and not two (a nested offload, which burns two
    threadpool slots per request).

    The UNIT is one call to a `web.discovery` enveloped wrapper, deliberately
    NOT one page load: this page already issues a findings read and a facet
    cascade, and plan 136-18 adds the launch-headline read, so a per-page
    literal would go red on a correct implementation."""
    import web.discovery as wd

    ok_envelope = {"status": "ok", "items": [], "total": 0, "meta": {}}

    async def _run():
        loop = asyncio.get_running_loop()
        calls, original = _spy_executor(loop)
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(wd, "discovery_available", return_value=True))
                stack.enter_context(patch.object(
                    wd._service, "get_findings_enveloped",
                    lambda *a, **k: dict(ok_envelope)))
                stack.enter_context(patch.object(
                    wd._service, "get_findings_facets_enveloped",
                    lambda *a, **k: dict(ok_envelope)))

                before = len(calls)
                await fp.fetch_findings(_PROBE_STATE)
                findings_dispatches = len(calls) - before
                before = len(calls)
                await fp.fetch_facets("domain", _PROBE_STATE)
                facet_dispatches = len(calls) - before
        finally:
            loop.run_in_executor = original
        return findings_dispatches, facet_dispatches

    findings_dispatches, facet_dispatches = asyncio.run(_run())
    assert findings_dispatches == 1, (
        f"one enveloped findings read produced {findings_dispatches} executor "
        "dispatches — 0 means a synchronous call on the event loop, 2 means a "
        "nested offload"
    )
    assert facet_dispatches == 1, (
        f"one enveloped facet read produced {facet_dispatches} executor dispatches"
    )


def test_zero_executor_dispatches_when_discovery_is_unavailable():
    """ZERO on the unavailable path is CORRECT and must never be read as
    evidence of a synchronous call: the wrapper short-circuits before reaching
    the service, and the route returns early — while the page still renders its
    unavailable state."""
    import web.discovery as wd

    async def _run():
        loop = asyncio.get_running_loop()
        calls, original = _spy_executor(loop)
        try:
            with patch.object(wd, "discovery_available", return_value=False):
                findings = await fp.fetch_findings(_PROBE_STATE)
                facets = await fp.fetch_facets("domain", _PROBE_STATE)
        finally:
            loop.run_in_executor = original
        return len(calls), findings, facets

    dispatches, findings, facets = asyncio.run(_run())
    assert dispatches == 0, (
        f"the unavailable path dispatched {dispatches} times — the wrapper must "
        "short-circuit before reaching the service"
    )
    assert findings["status"] == "unavailable" and facets["status"] == "unavailable"


def test_page_load_dispatch_total_is_the_sum_over_the_reads_the_page_issues():
    """Computed from the per-read rule, never fixed as a literal — so plan
    136-18 adding a read does not turn this criterion red.

    ⚠ AMENDED by plan 136-18. The rule was right and the IMPLEMENTATION did not
    deliver it: `reads` counted only the two read kinds this function patched,
    so ANY third read the page gained produced one dispatch nobody counted and
    the assertion went red on a correct implementation — the exact outcome the
    docstring promised it would not. 136-18 added the ruling-U launch-statistics
    read, and the fix is to count it here rather than to contort the page into
    not issuing it. The criterion is unchanged and strictly stronger: one
    executor dispatch per enveloped read, now across all THREE read kinds the
    page issues."""
    import web.discovery as wd

    ok_findings = {"status": "ok", "items": [], "total": 0,
                   "meta": {"unit": "identification", "bucket": "main",
                            "sort": "band_rank", "approximate_total": False}}
    ok_facets = {"status": "ok", "items": [], "total": 0, "meta": {"level": "domain"}}
    ok_launch = {"status": "ok", "items": [], "total": 0,
                 "meta": {"basis": "main_pool"}}
    reads = []

    async def _counting_findings(*args, **kwargs):
        reads.append("findings")
        return await wd.get_findings_enveloped(*args, **kwargs)

    async def _counting_facets(*args, **kwargs):
        reads.append("facets")
        return await wd.get_findings_facets_enveloped(*args, **kwargs)

    async def _counting_launch(*args, **kwargs):
        reads.append("launch")
        return await wd.get_launch_stats_enveloped(*args, **kwargs)

    # THE FOURTH READ (2026-08-06): the admin hide list. Counted here for exactly
    # the reason the docstring above gives for counting the launch read -- the
    # criterion is "one dispatch per read", so a read the probe does not know
    # about turns a correct implementation red. It is a SUPABASE read rather than
    # a sidecar one, and it still goes through a single `bounded_io_bound`
    # dispatch, which is what this criterion is about.
    async def _counting_suppressed(*args, **kwargs):
        reads.append("suppressed")
        return await wd.suppressed_identification_ids(*args, **kwargs)

    # THE FIFTH READ (excerpt-v1, 2026-08-13): the per-refresh excerpts
    # availability probe. One dispatch on this first (uncached) call; the
    # service memoizes it per (path, version), so the criterion stays "one
    # dispatch per read the page ISSUES".
    async def _counting_excerpts_available(*args, **kwargs):
        reads.append("excerpts_available")
        return await wd.excerpts_available(*args, **kwargs)

    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    async def _run():
        loop = asyncio.get_running_loop()
        core.loop = loop
        calls, original = _spy_executor(loop)
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(wd, "discovery_available", return_value=True))
                stack.enter_context(patch.object(
                    wd._service, "get_findings_enveloped",
                    lambda *a, **k: dict(ok_findings)))
                stack.enter_context(patch.object(
                    wd._service, "get_findings_facets_enveloped",
                    lambda *a, **k: dict(ok_facets)))
                stack.enter_context(patch.object(
                    wd._service, "get_launch_stats_enveloped",
                    lambda *a, **k: dict(ok_launch)))
                stack.enter_context(patch.object(fp, "get_findings_enveloped", _counting_findings))
                stack.enter_context(patch.object(fp, "get_findings_facets_enveloped", _counting_facets))
                stack.enter_context(patch.object(fp, "get_launch_stats_enveloped", _counting_launch))
                stack.enter_context(patch.object(
                    fp, "suppressed_identification_ids", _counting_suppressed))
                stack.enter_context(patch.object(
                    fp, "excerpts_available", _counting_excerpts_available))
                with Client(ui.page("/_findings_dispatch_probe")) as client:
                    with client:
                        await fp.create_findings_page()
        finally:
            loop.run_in_executor = original
        return len(calls)

    set_language("en")
    try:
        dispatches = asyncio.run(_run())
    finally:
        set_language("he")

    assert reads, "the page issued no enveloped read at all"
    assert dispatches == len(reads), (
        f"the page issued {len(reads)} enveloped read(s) but produced {dispatches} "
        "executor dispatch(es) — the total must be exactly one per read"
    )


def test_the_mobile_rule_is_scoped_to_findings_row_anatomy():
    """The discovery CSS block was landed once, in plan 136-10, and this test
    used to assert the file was untouched thereafter.

    THAT ASSERTION WAS RETIRED FOR A MEASURED DEFECT, not for convenience. The
    landed block contained `.gs-discovery .row { flex-direction: column }` under
    a 700px media query -- a DESCENDANT selector matching every NiceGUI
    `ui.row()` inside the page (24 of them: the pool segment, the result
    toolbar, the active-filter chips, the launch figures, the pager), turning
    each into a vertical stack on any phone. It could not even have been doing
    its stated job: the one element carrying `row` is `ROW_CLASS`, a
    `ui.column()` that is already vertical, and nothing on either surface
    carries `.side`. So the rule stacked everything except its own target.

    A guard whose only effect is to freeze a bug in place is not protecting
    anything. What the guard was FOR -- CSS not drifting quietly, and never
    growing physical-direction properties that break RTL -- is now asserted
    directly, over the file's content, which is a stronger statement than "no
    diff" and cannot be satisfied by simply not touching a broken rule.
    """
    raw = (pathlib.Path(__file__).resolve().parents[1]
           / "web" / "static" / "common.css").read_text(encoding="utf-8")

    # COMMENTS ARE STRIPPED FIRST, and that is not a detail. A previous revision
    # of this test scanned the raw file for the forbidden selector, and it went
    # red on the CSS comment that EXPLAINS why the selector was removed -- the
    # same trap `tests/render_smoke/test_findings_render_smoke.py` already
    # documents for its own source scans. Prose about a defect is not the defect.
    css = _re.sub(r"/\*.*?\*/", "", raw, flags=_re.S)

    # The generic descendant form, matched as a WHOLE CLASS NAME rather than as
    # a prefix. A plain substring test would also fire on the correctly-scoped
    # `.gs-discovery .gs-findings-row-meta`, which merely happens to start
    # differently today -- and every rule in this block must be page-scoped
    # (`tests/test_discovery_display_strings.py`), so the scoped forms and the
    # forbidden form share a prefix by DESIGN, not by accident. The trailing
    # boundary is what distinguishes them.
    #
    # The pattern is composed so this assertion does not contain the literal it
    # forbids.
    generic = _re.compile(
        _re.escape(".gs-discovery " + ".row") + r"(?![\w-])")
    assert not generic.search(css), (
        "the unscoped mobile row rule is back: it matches every NiceGUI row "
        "element on the page rather than the result rows, and stacks the filter "
        "controls on every phone")

    # The replacement targets the row's own meta line by CLASS.
    assert ".gs-findings-row-meta" in css, (
        "the findings mobile rule no longer targets the row meta line")

    # ...and every property in the discovery block stays direction-neutral, so
    # the page still mirrors in Hebrew. This is the half of the original guard's
    # purpose that actually protects a reader.
    start = css.find(".gs-findings-row-meta")
    block = css[start:] if start >= 0 else ""
    for physical in ("margin-left", "margin-right", "padding-left",
                     "padding-right", "border-left", "border-right",
                     "text-align: left", "text-align: right"):
        assert physical not in block, (
            f"the findings mobile block gained the physical property "
            f"{physical!r} -- logical properties only, or the page stops "
            "mirroring in RTL")


@pytest.mark.parametrize("module_path", [
    "web/pages/findings.py",
    "web/components/findings_rows.py",
])
def test_no_quasar_prop_pins_an_element_to_a_physical_side(module_path):
    """The RTL hole the CSS-property guard above could not see.

    Its sibling scans the STYLESHEET for `margin-left` and friends. A physical
    direction can also arrive as a QUASAR PROP -- `align=left`, `side=right` --
    which never appears in any `.css` file and so passed every existing check.

    That is not hypothetical: the domain facet buttons shipped with
    `.props("flat dense no-caps align=left")`, so a Hebrew reader saw every
    domain label pinned to the LEFT edge of its own button while the rest of the
    tree read right-to-left (external review, 2026-08-06). The shared block
    already had the logical equivalent (`.dnode { text-align: start }`); the prop
    was overriding it.

    `justify-start` / `items-start` are deliberately NOT forbidden: those are
    Quasar's flex utilities and resolve to `flex-start`, which FOLLOWS the
    writing mode. The rule is about named physical SIDES.

    SCANNED OVER CODE ONLY, comments and docstrings stripped -- the fix for the
    defect above left behind a comment naming `align=left` in order to say why
    it must not come back, and a raw text scan failed on the explanation instead
    of on a defect. (The sibling render-smoke suite strips prose for the same
    reason; `_code_lines` there has the same job.)
    """
    import ast

    source = pathlib.Path(module_path).read_text(encoding="utf-8")
    tree = ast.parse(source)
    prose: set = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) and \
                    isinstance(body[0].value, ast.Constant) and \
                    isinstance(body[0].value.value, str):
                doc = body[0].value
                prose.update(range(doc.lineno, (doc.end_lineno or doc.lineno) + 1))
    code = "\n".join(
        "" if (n in prose or line.lstrip().startswith("#")) else line
        for n, line in enumerate(source.splitlines(), start=1))
    offenders = _re.findall(
        r"(?:align|side|anchor|self)\s*=\s*[\"']?(left|right)\b", code)
    assert not offenders, (
        f"{module_path} pins an element to a physical side via a Quasar prop "
        f"({sorted(set(offenders))}) -- these surfaces render in both "
        "directions, so use a logical/flex value (`justify-start`, "
        "`text-align: start`) instead of a named side")


# ---------------------------------------------------------------------------
# (c) The INTERACTION test — through the NiceGUI User simulation, never by
# calling the handler directly. Marked render_smoke: it enters and tears down
# the app lifespan, which is not safe to interleave with the bulk suite.
# ---------------------------------------------------------------------------

# NiceGUI's simulated ``should_see`` waits only 300 ms by default. A refresh
# that has already completed by the time its failure dump is built can still
# lose that race on a busy Windows runner, so post-interaction assertions get a
# bounded three-second window. The real-browser gate below remains responsible
# for browser actionability and uses its own larger network/UI timeout.
_ASYNC_UI_RETRIES = 30

@pytest.mark.render_smoke
@pytest.mark.parametrize("lang", ["en", "he"])
def test_more_matches_click_replaces_the_rendered_result_set(lang):
    """Ruling T, criterion (c).

    Open the page, locate the control by its ACCESSIBLE NAME with NO preceding
    expand/open/menu interaction of any kind, click it THROUGH the simulated
    user, and assert the RENDERED RESULT REGION is REPLACED: main-pool rows
    gone, second-bucket rows present.

    Asserting only that the outgoing service call carried the second bucket does
    NOT satisfy this: an inert DOM binding with a directly-invoked handler
    passes that assertion while showing the reader nothing. This test never
    calls the handler function directly — `UserInteraction.click()` dispatches
    through the element's own registered event listeners, and skips any element
    that is disabled."""
    import httpx
    from nicegui import core, ui
    from nicegui.context import context as _nicegui_context
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.user import User
    from nicegui.ui_run import set_storage_secret

    from shared.discovery_display_strings import bucket_name

    saved_slot_stack = list(_nicegui_context.slot_stack)
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()

    async def _run():
        prepare_simulation()
        set_storage_secret("findings-page-interaction-secret", {})
        with ExitStack() as stack:
            stack.enter_context(patch("web.main.discovery_available", return_value=True))
            stack.enter_context(patch.object(fp, "get_findings_enveloped", _fake_findings()))
            stack.enter_context(patch.object(fp, "get_findings_facets_enveloped", _fake_facets()))
            stack.enter_context(patch("web.main._resolve_ui_language", return_value=lang))
            _os.environ["NICEGUI_USER_SIMULATION"] = "true"
            set_language(lang)
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(core.app), base_url="http://test"
                    ) as http_client:
                        user = User(http_client)
                        await user.open(FINDINGS_ROUTE)

                        # The default view is the main pool.
                        await user.should_see(MAIN_ROW_TITLE)
                        await user.should_not_see(MORE_ROW_TITLE)

                        # NO preceding disclosure action of ANY kind: find the
                        # control by its accessible name and click it.
                        #
                        # NARROWED TO A BUTTON (2026-08-05). `user.find(str)`
                        # matches text as a SUBSTRING, and the second-pool
                        # invitation strip beside the rows now names the bucket
                        # in its prose -- so the bare locator resolves to the
                        # control AND to a paragraph, and this test would be
                        # clicking a set rather than a control.
                        #
                        # `user.find(target, kind=...)` would NOT fix that:
                        # nicegui's `_gather_elements` ignores `kind` entirely
                        # whenever `target` is a string. The keyword form below
                        # is the one that actually filters, and the uniqueness
                        # assertion is what keeps this honest if a second
                        # button ever takes the same name.
                        control = user.find(kind=ui.button,
                                            content=bucket_name(False, lang))
                        assert len(control.elements) == 1, (
                            "the 'more matches' control is no longer uniquely "
                            f"locatable: {len(control.elements)} button(s) carry "
                            "that name")
                        control.click()

                        # The RENDERED result region is replaced.
                        await user.should_see(
                            MORE_ROW_TITLE, retries=_ASYNC_UI_RETRIES
                        )
                        await user.should_not_see(MAIN_ROW_TITLE)
            finally:
                _os.environ.pop("NICEGUI_USER_SIMULATION", None)

    try:
        asyncio.run(_run())
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)
        set_language("he")


# ---------------------------------------------------------------------------
# The candidacy switch x row unit interaction (code review round 15, finding 1).
#
# These two axes are NOT independent, and the page used to pretend they were:
# the novelty switch stayed live and the "Show as" control changed the unit
# underneath it, so a reader who turned candidates on and then chose "one row
# per work" made `fetch_findings` hand the shipped builder a combination it
# REFUSES. The `ValueError` escaped into a background task, the refresh died
# half-way, and the page silently stopped updating.
#
# The test below drives that exact reader sequence through the simulated user.
# A test that called the builder directly would NOT have caught this: the bug
# was never in the builder, it was in the page's state handling.
# ---------------------------------------------------------------------------

#: Row titles keyed by (unit, whether the candidacy filter was applied). Every
#: one is a sentinel, so "which query answered" is legible in the RENDER.
#:
#: `("work", True)` IS a member as of 2026-08-06: the candidacy/unit coupling
#: this whole block exists for is gone (the builder answers novelty on every
#: unit now), so a per-work read WITH the candidacy filter on is a legal,
#: answerable combination that the interaction test below must actually see
#: land, not a combination the page still refuses.
_INTERACTION_ROW_TITLES = {
    ("identification", False): "SEQ-IDENTIFICATION-ALL",
    ("identification", True): "SEQ-IDENTIFICATION-CANDIDATES",
    ("manuscript", False): "SEQ-MANUSCRIPT-ALL",
    ("manuscript", True): "SEQ-MANUSCRIPT-CANDIDATES",
    ("work", False): "SEQ-WORK-ALL",
    ("work", True): "SEQ-WORK-CANDIDATES",
}


def _contract_bound_findings(recorder=None):
    """A findings stub that REFUSES exactly what the shipped service refuses.

    The request is validated by `_build_findings_query` itself — the same
    builder `DiscoveryService.get_findings_enveloped` calls — so this stub
    cannot drift from the rule it stands in for. A stub that merely re-stated
    "work plus novelty is illegal" would keep passing after the service changed
    its mind, which is the whole failure mode this test exists to catch.
    """
    from shared.discovery_service import _build_findings_query

    async def _call(unit="identification", *, bucket="main", novelty=None,
                    divergence="hidden", domain=None, author=None,
                    work_id=None, sort="band_rank", page=1, page_size=50,
                    suppressed=None, **extra_axes):
        # `suppressed` (2026-08-06): the admin hide list. Accepted AND FORWARDED to
        # the real builder rather than merely swallowed -- the whole point of this
        # stub is that it refuses what the shipped service refuses, so a filter
        # axis it silently dropped would be an axis whose validation this test
        # stopped exercising. Forwarding it means the over-cap raise is live here
        # too.
        #
        # `**extra_axes` FOR THE SAME REASON, and it is what keeps this stub from
        # needing an edit per axis. Adding `sys_id` to the builder (2026-08-07) made
        # this raise `TypeError` from inside a background task -- a stub whose
        # SIGNATURE had drifted from the service it stands in for, which is the
        # drift its docstring says it exists to prevent. Every keyword is forwarded
        # to the real builder, so an axis the builder does not accept still fails
        # here (loudly, as a `TypeError` from the builder itself) rather than being
        # quietly absorbed by this fake.
        _build_findings_query(
            unit=unit, sort=sort, bucket=bucket, novelty=novelty,
            divergence=divergence, domain=domain,
            author=author, work_id=work_id, suppressed=suppressed,
            page=page, page_size=page_size, **extra_axes)
        if recorder is not None:
            recorder.append({"unit": unit, "novelty": novelty, "bucket": bucket,
                             "divergence": divergence})
        title = _INTERACTION_ROW_TITLES[(unit, bool(novelty))]
        row = dict(_finding_row("w000001", title, "T-S 12.111"), unit=unit)
        return {
            "status": "ok", "items": [row], "total": 1,
            "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                     "sort_basis": "best_band_rank",
                     "novelty_offered": unit != "work",
                     "divergence": divergence,
                     "approximate_total": False},
        }

    return _call


@pytest.mark.render_smoke
@pytest.mark.parametrize("lang", ["en", "he"])
def test_turning_candidates_on_then_switching_to_one_row_per_work_does_not_break_the_page(lang):
    """THE READER SEQUENCE, end to end, through the simulated user.

    Pick "candidates" on the selector; then change "Show as" to one row per
    work. Both are first-class controls a reader reaches for.

    Round-15 finding 1 was that this SAME sequence, under the old two-control
    design, drove `fetch_findings` into a combination the shipped builder
    refused (`ValueError`) -- the exception escaped into a background task and
    the page silently stopped updating. `normalise_state` used to paper over
    it by clearing the candidacy selection the moment the unit changed.

    That coupling is GONE (owner ruling, 2026-08-06): `_build_findings_filter`
    now answers the candidacy predicate on every unit, so this test's job is
    the mirror image of what it used to check -- not "the page recovers from a
    combination it cannot serve", but "the combination is now genuinely
    answerable and the page asks for exactly that; the selection is NOT
    silently dropped when the unit changes." A test that kept asserting the
    old recovery behaviour would be pinning a regression: `normalise_state`
    dropping a reader's live selection for no reason a currently-supported
    combination would explain.

    Two things must hold:

      1. NOTHING RAISED, for the reason round 15 gave: `handle_event` routes an
         exception from an async handler into `core.app.handle_exception`,
         where it becomes a log line and nothing else -- so a raising page
         looks, from the outside, exactly like a page that did not respond.
         The recorder makes that difference visible.
      2. THE PAGE RENDERED THE NEW UNIT WITH THE SELECTION STILL APPLIED. Not
         "did not crash": the work-unit-plus-candidates row must actually be on
         screen (a distinct sentinel from the plain work-unit row), the
         previous result set gone, and the selector still shows "candidates" --
         proving the refresh ran to completion and the selection survived the
         unit change rather than being cleared underneath the reader.
    """
    import httpx
    from nicegui import core
    from nicegui.context import context as _nicegui_context
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.user import User
    from nicegui.ui_run import set_storage_secret

    from shared.discovery_display_strings import novelty_strings
    from web.translations import tr

    saved_slot_stack = list(_nicegui_context.slot_stack)
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()
    raised: list = []
    issued: list = []

    #: Scoped to THIS page's own code. Entering the app lifespan starts
    #: NiceGUI's storage-pruning timer, which raises `Request is not set`
    #: against a simulated client on every run; recording that would make the
    #: assertion below unconditionally red and it would then be deleted. The
    #: filter is on the TRACEBACK, so anything the page or the service it calls
    #: raises is still recorded — including the ValueError this test exists for.
    _OWN_CODE = ("web\\pages\\findings.py", "web/pages/findings.py",
                 "web\\components\\findings_rows.py", "web/components/findings_rows.py",
                 "shared\\discovery_service.py", "shared/discovery_service.py")

    def _record(exception: Exception) -> None:
        import traceback as _traceback

        frames = "".join(_traceback.format_exception(
            type(exception), exception, exception.__traceback__))
        if any(marker in frames for marker in _OWN_CODE):
            raised.append(exception)

    core.app._exception_handlers.append(_record)

    async def _run():
        prepare_simulation()
        set_storage_secret("findings-page-unit-novelty-secret", {})
        with ExitStack() as stack:
            stack.enter_context(patch("web.main.discovery_available", return_value=True))
            stack.enter_context(patch.object(
                fp, "get_findings_enveloped", _contract_bound_findings(issued)))
            stack.enter_context(patch.object(fp, "get_findings_facets_enveloped", _fake_facets()))
            stack.enter_context(patch("web.main._resolve_ui_language", return_value=lang))
            _os.environ["NICEGUI_USER_SIMULATION"] = "true"
            set_language(lang)
            words = novelty_strings(lang)
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(core.app), base_url="http://test"
                    ) as http_client:
                        user = User(http_client)
                        await user.open(FINDINGS_ROUTE)
                        await user.should_see(_INTERACTION_ROW_TITLES[("identification", False)])

                        # (1) The reader picks "candidates" on the selector,
                        # through its own popup -- the same two-click pattern
                        # already used below for "Show as" (open via the
                        # select's own `label` prop, which is present whether
                        # or not the popup is showing; then pick the option).
                        user.find(fp.copy_text("novelty_view_label", lang)).click()
                        user.find(words["toggle"]).click()
                        await user.should_see(
                            _INTERACTION_ROW_TITLES[("identification", True)],
                            retries=_ASYNC_UI_RETRIES,
                        )
                        await user.should_not_see(_INTERACTION_ROW_TITLES[("identification", False)])

                        # (2) ...and then changes the row unit to one row per
                        # work, through the select's own popup.
                        unit_label = tr("One row per work")
                        user.find(tr("Show as")).click()
                        user.find(unit_label).click()

                        # (2a) The new unit is RENDERED, the selection is STILL
                        # APPLIED (a distinct sentinel from the plain work
                        # row), and the old set is gone.
                        await user.should_see(
                            _INTERACTION_ROW_TITLES[("work", True)],
                            retries=_ASYNC_UI_RETRIES,
                        )
                        await user.should_not_see(_INTERACTION_ROW_TITLES[("work", False)])
                        await user.should_not_see(
                            _INTERACTION_ROW_TITLES[("identification", True)])

                        # (2b) The selector itself still reads "candidates" --
                        # the unit change did not reset it underneath the
                        # reader.
                        selects = [
                            element for element in user.client.elements.values()
                            if f"{fp.FILTER_BAR_CLASS}-novelty-view"
                            in (element._classes or [])
                        ]
                        assert len(selects) == 1, (
                            f"expected exactly one novelty-view select, found {len(selects)}")
                        assert selects[0].value == fp.NOVELTY_VIEW_CANDIDATES, (
                            "the selector's own displayed value did not survive the "
                            f"unit change: {selects[0].value!r}")
            finally:
                _os.environ.pop("NICEGUI_USER_SIMULATION", None)

    try:
        asyncio.run(_run())
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        if _record in core.app._exception_handlers:
            core.app._exception_handlers.remove(_record)
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)
        set_language("he")

    assert not raised, (
        "the page raised while a reader drove it: "
        + "; ".join(f"{type(e).__name__}: {e}" for e in raised))
    # The work-unit read really was issued, and it STILL carried the
    # candidacy selection -- the inverse of what this test asserted before
    # 2026-08-06, when the builder refused the combination and the coupling
    # this test exists to catch was `normalise_state` dropping it instead.
    from shared.discovery_novelty import CANDIDATE_STATUS

    work_reads = [call for call in issued if call["unit"] == "work"]
    assert work_reads, "the page never issued a per-work read"
    assert all(call["novelty"] == (CANDIDATE_STATUS,) for call in work_reads), (
        f"a per-work read dropped the candidacy selection: {work_reads!r}")


def test_the_axis_rule_is_the_services_own_predicate_and_is_not_restated():
    """One authority for "does this unit offer novelty" -- still real, even
    though the page no longer consults it for FILTERING.

    `shared.discovery_service.findings_novelty_offered` used to gate three
    things: what the BUILDER raised on, what the ENVELOPE reports as
    `meta['novelty_offered']`, and what the PAGE disabled its switch on.
    Owner ruling 2026-08-06 removed the BUILDER's raise (the predicate is
    answerable on every unit now) and the PAGE's own use (nothing disables
    any more, see `test_the_two_controls_share_one_card` and its neighbours).
    What survives is the ENVELOPE's `meta['novelty_offered']`, which still
    gates the DISPLAYED verdict/badge on a per-work row (a per-work
    identification has no single `novelty_status` of its own to show) --
    `findings_novelty_offered`'s own docstring says exactly this. The page
    must not import the predicate at all any more: it has no remaining call
    site, so pinning `fp.findings_novelty_offered is svc.findings_novelty_offered`
    would assert an import the page has no reason to carry."""
    import shared.discovery_service as svc

    assert not hasattr(fp, "findings_novelty_offered"), (
        "the page imports a per-unit novelty-offered predicate it has no "
        "remaining call site for -- the builder answers novelty on every "
        "unit now and nothing on the page disables on this rule any more")
    assert svc.findings_novelty_offered("identification") is True
    assert svc.findings_novelty_offered("manuscript") is True
    assert svc.findings_novelty_offered("work") is False

    # The page must not restate the rule as a literal comparison of its own.
    # Scoped to comparisons whose LEFT side is about the unit, so the facet
    # cascade's own legitimate `level == "work"` is untouched.
    tree = ast.parse(FINDINGS_SRC)
    restated = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Compare):
            continue
        if not any(isinstance(c, ast.Constant) and c.value == "work"
                   for c in node.comparators):
            continue
        left = ast.get_source_segment(FINDINGS_SRC, node.left) or ""
        if "unit" in left:
            restated.append(ast.get_source_segment(FINDINGS_SRC, node) or left)
    assert not restated, (
        "web/pages/findings.py compares the row unit against the literal 'work': "
        f"{restated!r}. Use findings_novelty_offered() so the rule cannot drift "
        "from the service that enforces it")

    # `normalise_state` is a documented no-op now (owner ruling 2026-08-06 --
    # there are no retired coupling pairs left for it to settle): a state
    # that pairs the per-work unit with the candidacy view passes through
    # completely unchanged, byte for byte, rather than having either half
    # cleared the way the old candidacy/unit coupling used to.
    state = _state(unit="work", novelty_view=fp.NOVELTY_VIEW_CANDIDATES)
    assert fp.normalise_state(dict(state)) == state


def test_normalise_state_drops_a_persisted_pair_the_service_would_refuse(monkeypatch):
    """The stored pair SURVIVES now, not just the live one -- the inverse of
    what this test asserted before 2026-08-06.

    A cookie written before this ruling (or one written since, by the shipped
    control) carries `unit=work` AND the candidacy view. Owner ruling
    2026-08-06 made this a genuinely answerable combination -- the builder
    no longer raises on it -- so `read_state` settling it down to the plain
    work unit, the way it used to, would now be `read_state` silently
    discarding a reader's stored choice for no reason the current service
    would object to. This test used to name a `ValueError` it was protecting
    against; that `ValueError` is gone, and pinning the old drop behaviour
    would pin a regression instead of a guard."""
    from shared.discovery_novelty import CANDIDATE_STATUS

    store = {
        "discovery_findings_unit": "work",
        fp._KEY_NOVELTY_VIEW: fp.NOVELTY_VIEW_CANDIDATES,
    }
    monkeypatch.setattr(fp, "safe_user_get", lambda k, d=None: store.get(k, d))
    monkeypatch.setattr(fp, "safe_user_set", lambda k, v: None)

    restored = fp.read_state()
    assert restored["unit"] == "work"
    assert restored["novelty_view"] == fp.NOVELTY_VIEW_CANDIDATES, (
        "a persisted work+candidates pair was dropped by read_state, even "
        "though the shipped builder now answers it")
    assert fp._novelty_selection(restored) == (CANDIDATE_STATUS,)


def test_the_facet_cascade_never_carries_a_selection_the_result_set_dropped(monkeypatch):
    """Facet counts and the result set must agree.

    The cascade always queries at the identification grain, so novelty is legal
    there whatever unit the reader picked — which is exactly the trap: applying
    it while the RESULT SET cannot would put counts beside rows they do not
    describe."""
    captured = {}

    async def _capture(level, **kwargs):
        captured[level] = kwargs
        return {"status": "ok", "items": [], "total": 0, "meta": {"level": level}}

    monkeypatch.setattr(fp, "get_findings_facets_enveloped", _capture)
    # THE SELECTOR'S OWN KEY, and the assertion is now the INVERSE of what it
    # was. Until 2026-08-06 this passed `novelty_only=True` and asserted the
    # cascade sent `novelty=None`, because the per-work result set could not
    # apply candidacy and mismatched facet counts would have described rows the
    # reader was not being shown.
    #
    # It kept passing after the selector landed, FOR THE WRONG REASON, and that
    # is the interesting part: `_state(**overrides)` accepts any keyword, so
    # `novelty_only=True` silently added a key nothing reads. The request really
    # did carry `novelty=None` -- not because the rule under test fired, but
    # because the state said "all findings". A green test asserting an
    # unfiltered request while claiming to test candidacy. (Caught by external
    # review, 2026-08-06.)
    #
    # The service answers candidacy on every unit now, so what must hold is the
    # opposite: the cascade CARRIES the selection, and its counts therefore
    # describe the same population as the rows.
    from shared.discovery_novelty import CANDIDATE_STATUS

    state = _state(unit="work", novelty_view=fp.NOVELTY_VIEW_CANDIDATES)
    asyncio.run(fp.fetch_facets("domain", state))
    assert captured["domain"]["novelty"] == (CANDIDATE_STATUS,), (
        "the facet cascade dropped the candidacy selection on the per-work unit "
        "— its counts would describe a different population than the rows "
        f"beside them; sent {captured['domain']['novelty']!r}")
    assert captured["domain"]["divergence"] == fp.DIVERGENCE_SHOWN, (
        "the cascade must pin the legacy divergence axis to SHOWN like every "
        "other read, or it subtracts ruling F's rows back out")


# ---------------------------------------------------------------------------
# (e) + (f) The REAL-BROWSER actionability check and its positive control.
#
# STATUS AS SHIPPED BY PLAN 136-16: **NOT MET.** Playwright was not installed in
# that executor's environment (`scripts/capture_atlas_html.py` documents it as an
# ad-hoc dev/ops tool, deliberately absent from every requirements file), and a
# package install is outside what an executor may do unattended.
#
# STATUS NOW: **MET — first green run 2026-08-04 (CI run 30931268195).** The
# `findings-browser-check` job in .github/workflows/ci.yml installs Playwright +
# Chromium, materializes the SYNTHETIC fixture sidecar into a temp directory
# (CI has no real `discovery_data/` — it is gitignored), serves the app against
# it with DISCOVERY_ENABLED=1, waits for the origin to actually answer, and runs
# this check with both env vars set. The criterion was held at NOT MET until the
# job had actually passed — a wired gate is not yet a met criterion.
#
# It took four runs to go green, and it earned them: it caught THREE real
# defects, two of which were PRODUCT defects rather than test defects.
#   1. A `persistent` citation dialog covered the page for every first-time
#      visitor, so its backdrop swallowed the click. (Test-side: the check now
#      arrives as a returning visitor.)
#   2. The mobile nav drawer NEVER CLOSED. It mounted open and a deferred
#      handler was meant to retract it, but that ran under
#      `asyncio.ensure_future`, which empties NiceGUI's slot stack, so
#      `ui.run_javascript` raised and a bare `except` swallowed it. The backdrop
#      was still intercepting taps FIVE SECONDS after load — ten times the sleep
#      it was supposed to wait out. Fixed in `web/main.py` with Quasar's
#      `show-if-above`: mount closed, let the layout open it above the
#      breakpoint. A long-standing owner-reported annoyance.
#   3. The Quasar `reveal` header slides out of view on scroll-down, so after
#      the probe scrolled, the language toggle existed but was permanently
#      invisible. (Test-side: scroll to top before reaching for it.)
#
# Keep that history in mind before relaxing anything here on the assumption that
# a timeout means the check is being fussy. Three times running, it was not.
#
# The check exercises BOTH viewport widths in BOTH languages, then runs its own
# positive control. With GENIZAH_FINDINGS_BROWSER_CHECK SET and the tooling
# ABSENT (or the base URL empty) it FAILS — it never degrades to a silent green,
# which is the same fail-closed posture this phase applies to the masking scan.
# That property is load-bearing: a gate that can silently not run is worse than
# no gate, because it is trusted anyway.
#
# LANGUAGE (fixed while wiring the job): the check used to navigate to
# `?lang=en`. Nothing in the app reads that parameter — `web/main.py::
# _resolve_ui_language` resolves the UI language from PER-USER STORAGE and
# defaults to Hebrew — so a fresh browser context rendered Hebrew and the "EN"
# pass looked for an English control that was never on the page. The check now
# reads the language off the rendered control and reaches English the only way a
# reader can: by clicking the header's own language toggle.
# ---------------------------------------------------------------------------

_BROWSER_CHECK_ENV = "GENIZAH_FINDINGS_BROWSER_CHECK"
_BROWSER_BASE_URL_ENV = "GENIZAH_FINDINGS_BROWSER_BASE_URL"

#: Criterion (e) names both widths explicitly: a phone and a desktop.
_BROWSER_VIEWPORTS = ((375, 812), (1440, 900))

#: The header's own language toggle (`web/main.py::create_layout`). The app has
#: no `?lang=` parameter — the UI language comes from per-user storage and
#: defaults to Hebrew — so this control is the ONLY route a real reader has to
#: English, and therefore the only honest way for this check to reach it.
_LANG_TOGGLE_SELECTOR = ".lang-btn-header"

#: NiceGUI paints its body over the websocket AFTER `load` fires, so every
#: navigation here waits for the CONTROL, never for a network-idle heuristic.
_CONTROL_TIMEOUT_MS = 30000

#: The click's own budget. Deliberately short: this is the window in which the
#: browser's actionability conditions must already hold, and it is what makes
#: the positive control fail fast instead of hanging.
_CLICK_TIMEOUT_MS = 5000


def _playwright_available() -> bool:
    try:
        import playwright.sync_api  # noqa: F401
    except Exception:
        return False
    return True


def _wait_for_findings_page(page) -> None:
    """Block until the bucket control is actually painted, with a message that
    names the likeliest cause when it never is."""
    try:
        page.wait_for_selector(
            f".{fp.BUCKET_CONTROL_CLASS}", state="visible", timeout=_CONTROL_TIMEOUT_MS
        )
    except Exception as exc:  # noqa: BLE001 -- re-raised as a diagnosable assertion
        raise AssertionError(
            f"the findings page never painted its .{fp.BUCKET_CONTROL_CLASS} control at "
            f"{FINDINGS_ROUTE}. The likeliest cause is NOT the control: the page "
            "clean-hides behind discovery_available(), so the server needs "
            "DISCOVERY_ENABLED=1 AND a sidecar that passes web/discovery_assets.py's "
            "readiness contract. Check the server log before suspecting the control."
        ) from exc


def _rendered_language(page) -> str:
    """The language the page ACTUALLY rendered in, read off the control itself.

    Asking the page rather than asserting from the URL is the point: the app
    resolves its language from per-user storage, so any assumption made outside
    the browser is a guess.
    """
    from shared.discovery_display_strings import bucket_name

    for lang in ("he", "en"):
        if page.get_by_role("button", name=bucket_name(False, lang), exact=True).count():
            return lang
    raise AssertionError(
        "the rendered page carries neither the Hebrew nor the English name of the "
        "'more matches' control — the shared vocabulary and the page have diverged"
    )


def _switch_ui_language(page, from_lang: str) -> str:
    """Click the header's language toggle and wait for the OTHER language's
    control to be the one on the page."""
    from shared.discovery_display_strings import bucket_name

    target = "en" if from_lang == "he" else "he"

    # Bring the header back before reaching for its toggle.
    #
    # The app header is a Quasar `reveal` header: it slides out of view on
    # scroll-down and returns on scroll-up. The probe and its positive control
    # both scroll the control into view, so by the time we get here the header
    # — and the language button inside it — can be off-screen. Playwright then
    # resolves `.lang-btn-header` and waits forever for an element that exists
    # but will never be "visible, enabled and stable".
    #
    # Scrolling to the top is not the forbidden preceding action: it is how a
    # reader reaches the site's own language control between the two passes,
    # and it reveals nothing about the "more matches" control under test.
    page.evaluate("window.scrollTo(0, 0)")
    page.locator(_LANG_TOGGLE_SELECTOR).first.wait_for(
        state="visible", timeout=_CONTROL_TIMEOUT_MS
    )
    page.locator(_LANG_TOGGLE_SELECTOR).first.click(timeout=_CONTROL_TIMEOUT_MS)
    # The toggle persists the choice and calls ui.navigate.reload(), so waiting
    # on the TARGET language's control (rather than on any control) is what
    # distinguishes "the new page painted" from "the old page is still up".
    page.get_by_role("button", name=bucket_name(False, target), exact=True).first.wait_for(
        state="visible", timeout=_CONTROL_TIMEOUT_MS
    )
    actual = _rendered_language(page)
    assert actual == target, (
        f"the language toggle did not switch {from_lang!r} -> {target!r} (still {actual!r}); "
        "this check cannot claim to cover both languages"
    )
    return actual


def _reset_to_main_bucket(page, lang: str) -> None:
    """Put the page back in the MAIN bucket before probing.

    Criterion (e) is about switching INTO the second bucket, and the choice
    persists in per-user storage across a reload — so without this the second
    pass would 'switch' from `more` to `more` and prove nothing.
    """
    from shared.discovery_display_strings import bucket_name

    chip = page.get_by_role("button", name=bucket_name(True, lang), exact=True).first
    chip.wait_for(state="visible", timeout=_CONTROL_TIMEOUT_MS)
    if chip.get_attribute("aria-pressed") != "true":
        from playwright.sync_api import expect

        chip.click(timeout=_CLICK_TIMEOUT_MS)
        bucket_line = page.locator(f".{fp.RESULT_BAR_CLASS}-bucket").first
        expect(bucket_line).to_contain_text(
            bucket_name(True, lang), timeout=_CONTROL_TIMEOUT_MS
        )


def _browser_actionability_probe(page, control_name: str, lang=None) -> None:
    """Assert the browser's OWN actionability conditions hold at the control's
    locator (visible, stable, enabled, receiving pointer events at its hit
    point), then perform a real click, then assert the results region changed.

    This is the only check that can see a collapsed ancestor, a zero-height box,
    a clip or an overlay — none of which a DOM-ancestry assertion can.

    When `lang` is given the probe additionally asserts WHAT it changed to: the
    result bar must now name the second bucket. A bare "the HTML differs" check
    is satisfied by any re-render at all (NiceGUI re-mints element ids on every
    paint), so on its own it would pass for a click that changed nothing.
    """
    from shared.discovery_display_strings import bucket_name

    locator = page.get_by_role("button", name=control_name, exact=True).first
    region = page.locator(f".{fp.RESULTS_CLASS}").first
    before = region.inner_html()
    # No preceding disclosure action: click straight away. Playwright's own
    # actionability checks run inside click() and raise on failure.
    locator.click(timeout=_CLICK_TIMEOUT_MS)
    if lang is not None:
        from playwright.sync_api import expect

        # The target name also appears in the invitation and on the control,
        # both before the click. Wait on the result bar's bucket line: it is
        # the element whose text changes only after the server-side refresh.
        bucket_line = page.locator(f".{fp.RESULT_BAR_CLASS}-bucket").first
        expect(bucket_line).to_contain_text(
            bucket_name(False, lang), timeout=_CONTROL_TIMEOUT_MS
        )
    after = region.inner_html()
    assert after != before, (
        "the results region did not change after a real browser click on "
        f"{control_name!r}"
    )
    if lang is not None:
        text = region.inner_text()
        assert bucket_name(False, lang) in text, (
            "the results region changed but does not name the second bucket — the "
            f"click on {control_name!r} re-rendered without switching bucket"
        )


def _positive_control(page, lang: str, width: int) -> None:
    """(f) Same page, same locator, one ancestor collapsed: the SAME probe must
    now fail. Without this, (e) is a check nobody has watched fail."""
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeout

    from shared.discovery_display_strings import bucket_name

    page.eval_on_selector(
        f".{fp.BUCKET_CONTROL_CLASS}",
        "el => el.parentElement.style.display = 'none'",
    )
    failed = False
    try:
        _browser_actionability_probe(page, bucket_name(False, lang))
    except (PlaywrightTimeout, PlaywrightError, AssertionError):
        failed = True
    finally:
        page.eval_on_selector(
            f".{fp.BUCKET_CONTROL_CLASS}",
            "el => el.parentElement.style.display = ''",
        )
    assert failed, (
        "POSITIVE CONTROL DID NOT FIRE: the browser actionability check passed at "
        f"{width}px in {lang} with an ancestor of the control collapsed. The check "
        "is not watching what it claims to watch."
    )


def run_browser_actionability_check(base_url: str) -> None:
    """(e) at 375px and desktop, in both languages, plus (f) its positive
    control at each of the four combinations."""
    from playwright.sync_api import TimeoutError as PlaywrightTimeout
    from playwright.sync_api import sync_playwright

    from shared.discovery_display_strings import bucket_name

    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            for width, height in _BROWSER_VIEWPORTS:
                context = browser.new_context(viewport={"width": width, "height": height})
                # Arrive as a RETURNING visitor, not a first-time one.
                #
                # `web/main.py::_show_citation_reminder` opens a `persistent`
                # Quasar dialog on every page unless localStorage says it has
                # been seen. A fresh Playwright context has empty localStorage,
                # so the dialog opened and its full-screen backdrop intercepted
                # every click at the "more matches" control -- Playwright
                # reported the control itself "visible, enabled and stable" and
                # then timed out on `q-dialog__backdrop fixed-full ... intercepts
                # pointer events`. That was this job's first real finding.
                #
                # THIS IS NOT THE FORBIDDEN PRECEDING ACTION. Criterion (e)
                # forbids a preceding DISCLOSURE action -- opening an accordion,
                # expanding a section, anything that reveals the control under
                # test. Seeding an unrelated one-time site-wide modal as already
                # dismissed reveals nothing: the control's own visibility,
                # enabledness and hit-testability are still what the probe
                # measures, and a real reader dismisses this dialog once and
                # never sees it again. If a future edit makes this line click
                # something on the findings page itself, that IS a disclosure
                # action and the criterion is broken.
                context.add_init_script(
                    "try { localStorage.setItem('citation_reminder_seen', 'true'); }"
                    " catch (e) {}"
                )
                try:
                    page = context.new_page()
                    page.goto(f"{base_url}{FINDINGS_ROUTE}", wait_until="domcontentloaded")
                    _wait_for_findings_page(page)

                    # Let the nav drawer settle before probing.
                    #
                    # `web/main.py` renders the drawer OPEN (`value=True`) and
                    # only closes it ~500ms later, from `_deferred_close_drawer`,
                    # which has to round-trip `window.innerWidth` to the client
                    # to learn it is on mobile. Until that lands, the drawer's
                    # full-screen backdrop intercepts every click -- so at 375px
                    # the probe below timed out on a control Playwright itself
                    # reported "visible, enabled and stable". That was this job's
                    # SECOND real finding; the mobile flash is recorded in
                    # docs/OPEN_ISSUES.md as a product issue in its own right.
                    #
                    # THIS IS NOT THE FORBIDDEN PRECEDING ACTION. Criterion (e)
                    # forbids a preceding DISCLOSURE action -- one that reveals
                    # the control under test. Waiting for page load to finish
                    # reveals nothing and clicks nothing. A drawer that never
                    # settles IS a real defect for a reader on this viewport, so
                    # this fails loudly rather than proceeding into a misleading
                    # timeout.
                    try:
                        page.wait_for_selector(
                            ".q-drawer__backdrop", state="hidden", timeout=5000
                        )
                    except PlaywrightTimeout:
                        raise AssertionError(
                            "the nav drawer's backdrop still covers the findings page "
                            f"at {width}x{height} five seconds after load — a reader on "
                            "this viewport cannot tap anything underneath it"
                        ) from None

                    # Fail loudly if a modal is over the page anyway: a silent
                    # retry-until-timeout reads as "the control is broken" when
                    # the real cause is something covering it.
                    blockers = page.locator(".q-dialog__backdrop").count()
                    assert blockers == 0, (
                        f"{blockers} modal backdrop(s) cover the findings page at "
                        f"{width}x{height}. The probe below would time out on the "
                        "control while reporting it visible and enabled, which is "
                        "a misleading failure. Identify the dialog and decide "
                        "whether a reader meets it too."
                    )

                    covered = []
                    # Both languages in ONE context, because reaching the second
                    # one means clicking the app's own toggle, which persists in
                    # that context's storage.
                    for _pass in (1, 2):
                        lang = _rendered_language(page)
                        _reset_to_main_bucket(page, lang)
                        _browser_actionability_probe(page, bucket_name(False, lang), lang)
                        _positive_control(page, lang, width)
                        covered.append(lang)
                        if _pass == 1:
                            _switch_ui_language(page, from_lang=lang)
                            _wait_for_findings_page(page)

                    assert set(covered) == {"en", "he"}, (
                        f"at {width}px the check covered {covered!r}, not both languages — "
                        "the RTL pass is the one that can see a mirrored-layout clip, so a "
                        "run that silently covered Hebrew twice proves less than it claims"
                    )
                finally:
                    context.close()
        finally:
            browser.close()


@pytest.mark.render_smoke
def test_real_browser_actionability_of_the_more_matches_control():
    """Criterion (e) + (f). See the module note above: shipped NOT MET."""
    requested = _os.environ.get(_BROWSER_CHECK_ENV, "").strip().lower() in (
        "1", "true", "yes", "on",
    )
    if not requested:
        pytest.skip(
            "CRITERION (e)/(f) NOT MET — the real-browser actionability check for "
            "the 'more matches' control has NOT been run. This is recorded as NOT "
            f"MET in 136-16-SUMMARY.md, not as a pass. To run it: set {_BROWSER_CHECK_ENV}=1 "
            f"and {_BROWSER_BASE_URL_ENV}=<origin> with Playwright + Chromium "
            "installed (see scripts/capture_atlas_html.py for the install)."
        )
    if not _playwright_available():
        pytest.fail(
            f"NOT MET: {_BROWSER_CHECK_ENV} is set but Playwright is not installed. "
            "This check FAILS rather than skipping when it has been asked to run "
            "and cannot — a control that can silently not run is worse than no "
            "control, because it is trusted anyway."
        )
    base_url = _os.environ.get(_BROWSER_BASE_URL_ENV, "").rstrip("/")
    if not base_url:
        pytest.fail(
            f"NOT MET: {_BROWSER_CHECK_ENV} is set but {_BROWSER_BASE_URL_ENV} is "
            "empty — the check needs a reachable origin serving the findings page."
        )
    run_browser_actionability_check(base_url)


# ===========================================================================
# TASK 3 — result bar, pager, and the four service states
# ===========================================================================

def _state(**overrides) -> dict:
    base = {
        "unit": "identification", "bucket": "main", "sort": "band_rank",
        "novelty_view": "all", "domain": None, "author": None, "work_id": None,
        "page": 1,
    }
    base.update(overrides)
    return base


def _select_elements(client, marker: str) -> list:
    return _elements_with_class(client, marker)


@pytest.mark.parametrize("lang", ["en", "he"])
def test_no_control_label_on_this_page_contains_another_control_label(lang):
    """A label that CONTAINS another label makes two controls indistinguishable
    to anything matching by text — a screen reader listing controls, a test
    driver, and a reader scanning for "the one that changes what is listed".

    THIS IS A MEASURED DEFECT, not a hypothetical. The novelty selector shipped
    labelled "Show" while the row-unit control a short scroll away is labelled
    "Show as". In English one is a strict prefix of the other, so `user.find`
    matched BOTH and clicked the wrong one; the reader-sequence test failed in
    English and passed in Hebrew, whose labels happen not to collide. Naming
    only the English string would have left the pair differently shaped in the
    two languages, so both were renamed to name their axis.

    Checked in BOTH languages, because the collision existed in exactly one of
    them and a single-language guard would have missed it entirely.
    """
    from web.translations import set_language, tr

    import web.components.findings_rows as fr
    from shared.discovery_display_strings import (
        TOGGLE_MORE_MATCHES, disclosure_toggle, retry_label,
    )

    set_language(lang)
    try:
        # EVERY ACTIONABLE LABEL, not just the three selects. Restricting this
        # to the selects was itself the gap: external review found a SECOND
        # instance of exactly this collision among the BUTTONS -- the
        # expansion's child pagination read "Show more", a strict prefix of the
        # pool invitation's ratified "Show more possible matches", and the two
        # do very different things (load the next 25 children of one row, vs
        # switch the whole page to the second pool).
        labels = {
            # -- the three selects
            "novelty selector": fp.copy_text("novelty_view_label", lang),
            "row unit": tr("Show as"),
            "sort": tr("Sort by"),
            # -- the buttons a reader can aim at by name
            "expansion pagination": fr.copy_text("expand_more", lang),
            "pool invitation": disclosure_toggle(TOGGLE_MORE_MATCHES, lang),
            "row preview": fr.copy_text("preview_open", lang),
            "row report": fr.copy_text("report_link", lang),
            "retry": retry_label(lang),
        }
        for name, label in labels.items():
            for other_name, other in labels.items():
                if name == other_name:
                    continue
                assert label not in other, (
                    f"{lang}: the {name!r} label {label!r} is contained in the "
                    f"{other_name!r} label {other!r} — anything selecting a "
                    "control by text will match both, and a reader has only "
                    "position to tell them apart")
    finally:
        set_language("he")


# ---------------------------------------------------------------------------
# "Show as" and sort — the option sets ARE the exported vocabularies
# ---------------------------------------------------------------------------

def test_show_as_offers_exactly_the_three_shipped_row_units_and_defaults_to_identification(monkeypatch):
    from web.discovery import FINDINGS_UNITS, FINDINGS_UNIT_IDENTIFICATION

    client = _render_page(monkeypatch, lang="en")
    selects = _select_elements(client, f"{fp.RESULT_BAR_CLASS}-unit")
    assert len(selects) == 1, "the 'Show as' control did not render"
    control = selects[0]

    assert set(control.options) == set(FINDINGS_UNITS), (
        f"the row-unit option set {set(control.options)!r} is not the exported "
        f"FINDINGS_UNITS {set(FINDINGS_UNITS)!r} — a unit the service gains must "
        "not be silently withheld, and one it loses must not be silently offered"
    )
    assert control.value == FINDINGS_UNIT_IDENTIFICATION, (
        "the default row unit must be one row per identification — the only unit "
        "where the axes attach to exactly the thing on the line"
    )
    # The per-claim unit is deliberately NOT offered.
    assert "claim" not in set(control.options)


def test_sort_offers_exactly_the_exported_orderings_and_novelty_is_not_one(monkeypatch):
    from web.discovery import FINDINGS_SORTS

    client = _render_page(monkeypatch, lang="en")
    selects = _select_elements(client, f"{fp.RESULT_BAR_CLASS}-sort")
    assert len(selects) == 1, "the sort control did not render"
    control = selects[0]

    assert set(control.options) == set(FINDINGS_SORTS), (
        f"the sort option set {set(control.options)!r} is not the exported "
        f"FINDINGS_SORTS {set(FINDINGS_SORTS)!r}"
    )
    for forbidden in ("novelty", "novelty_status", "fills_gap"):
        assert forbidden not in set(control.options), (
            "novelty must never be a sort key: absence from a finding aid is not "
            "evidence a match is correct, and offering it as an ordering implies "
            "otherwise (D-15a / D-24)"
        )
    labels = {str(v) for v in (control.options.values() if isinstance(control.options, dict) else [])}
    for forbidden_label in ("Candidates for new finds", "מועמדים לממצאים חדשים"):
        assert forbidden_label not in labels


# ---------------------------------------------------------------------------
# The result bar names its bucket, in BOTH bucket states, in BOTH languages
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
@pytest.mark.parametrize("bucket,in_main", [("main", True), ("more", False)])
def test_result_bar_states_which_bucket_the_count_covers(monkeypatch, lang, bucket, in_main):
    """Ruling U constraint 1: one basis, STATED. A bar that names only one of
    the two bucket states fails."""
    from shared.discovery_display_strings import bucket_name

    client = _render_page(monkeypatch, lang=lang, state=_state(bucket=bucket))
    bars = _elements_with_class(client, f"{fp.RESULT_BAR_CLASS}-bucket")
    assert len(bars) == 1, f"the bucket line did not render for bucket={bucket!r}"
    text = "\n".join(_subtree_strings(bars[0]))
    assert bucket_name(in_main, lang) in text, (
        f"the result bar does not name the {bucket!r} bucket in {lang}: {text!r}"
    )
    # And it never names the OTHER bucket in the same line — that would be two
    # bases in one statement.
    assert bucket_name(not in_main, lang) not in text


@pytest.mark.parametrize("lang", ["en", "he"])
def test_approximate_total_is_labelled_and_an_exact_one_is_not(monkeypatch, lang):
    exact = _render_page(monkeypatch, lang=lang)
    assert not _elements_with_class(exact, f"{fp.RESULT_BAR_CLASS}-approx"), (
        "an EXACT total must not be labelled approximate"
    )

    approximate = _render_page(
        monkeypatch, lang=lang,
        findings=_fake_findings(meta_extra={"approximate_total": True}),
    )
    marked = _elements_with_class(approximate, f"{fp.RESULT_BAR_CLASS}-approx")
    assert marked, (
        "an approximate total must SAY SO — a silently approximate number "
        "presented as exact is worse than no number"
    )
    assert fp.copy_text("approximate_note", lang) in "\n".join(_subtree_strings(marked[0]))


def test_rendered_count_is_the_envelope_total_not_the_page_length(monkeypatch):
    """On a fixture whose `total` EXCEEDS `len(items)` — a page that rendered the
    page length would pass any assertion that only checks 'a number is shown'."""
    client = _render_page(
        monkeypatch, lang="en",
        findings=_fake_findings(total=4321),   # one item in the fixture
    )
    counts = _elements_with_class(client, f"{fp.RESULT_BAR_CLASS}-count")
    assert len(counts) == 1
    text = "\n".join(_subtree_strings(counts[0]))
    assert "4321" in text or "4,321" in text, (
        f"the rendered count is not the envelope's real pre-LIMIT total: {text!r}"
    )
    assert text.strip() != "1", "the page rendered len(items) instead of total"


# ---------------------------------------------------------------------------
# The four service states
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["unavailable", "timeout", "busy"])
@pytest.mark.parametrize("lang", ["en", "he"])
def test_each_outage_state_renders_distinctly_with_a_retry(monkeypatch, status, lang):
    """T-136-16-04: an outage must never read as 'no findings'. An empty section
    is indistinguishable from an authoritative zero, which would silently
    under-report the corpus."""
    from shared.discovery_display_strings import retry_label, service_state_message

    client = _render_page(monkeypatch, lang=lang, findings=_fake_findings(status=status))

    marked = _elements_with_class(client, f"{fp.STATE_CLASS}-{status}")
    assert marked, f"the {status!r} state has no distinct rendered marker"

    text = "\n".join(_subtree_strings(marked[0]))
    assert service_state_message(status, lang) in text, (
        f"the {status!r} state does not render its own copy: {text!r}"
    )
    retries = _elements_with_class(client, f"{fp.STATE_CLASS}-retry")
    assert retries, f"the {status!r} state offers no retry affordance"
    assert retry_label(lang) in "\n".join(_subtree_strings(retries[0]))

    # It must NOT read as an empty result set.
    set_language(lang)
    try:
        empty_marker = tr_for_test("No results found")
    finally:
        set_language("he")
    page_text = "\n".join(_collect_texts(client))
    assert empty_marker not in page_text, (
        f"the {status!r} outage rendered the empty-result copy — an outage must "
        "never read as 'this corpus has no findings'"
    )
    # And no result bar / pager, which would imply a real (zero) result set.
    assert not _elements_with_class(client, f"{fp.RESULT_BAR_CLASS}-count")


def tr_for_test(key: str) -> str:
    from web.translations import tr as _tr

    return _tr(key)


@pytest.mark.parametrize("lang", ["en", "he"])
def test_ok_with_zero_rows_renders_an_honest_empty_state(monkeypatch, lang):
    """The FOURTH state, distinct from the three outages above."""
    client = _render_page(
        monkeypatch, lang=lang,
        findings=_fake_findings({"main": [], "more": []}),
    )
    assert _elements_with_class(client, f"{fp.RESULTS_CLASS}-empty"), (
        "an ok-with-zero-rows envelope must render an honest empty state"
    )
    for status in ("unavailable", "timeout", "busy"):
        assert not _elements_with_class(client, f"{fp.STATE_CLASS}-{status}"), (
            f"an ok envelope rendered the {status!r} outage state"
        )
    # An honest empty state still shows the count bar, so the reader can see
    # WHICH bucket produced the zero.
    assert _elements_with_class(client, f"{fp.RESULT_BAR_CLASS}-bucket")


def test_the_three_outage_states_are_mutually_distinct(monkeypatch):
    """Each renders its OWN marker — not one generic 'error' box relabelled."""
    seen = {}
    for status in ("unavailable", "timeout", "busy"):
        client = _render_page(
            monkeypatch, lang="en", findings=_fake_findings(status=status)
        )
        marked = _elements_with_class(client, f"{fp.STATE_CLASS}-{status}")
        assert marked
        seen[status] = "\n".join(_subtree_strings(marked[0]))
    assert len(set(seen.values())) == 3, (
        f"the three outage states are not distinguishable to a reader: {seen!r}"
    )


# ---------------------------------------------------------------------------
# Pager
# ---------------------------------------------------------------------------

def test_pager_paginates_over_the_full_filtered_set(monkeypatch):
    """The count and pagination apply to the FULL filtered set, never to the
    current page: the envelope carries a real pre-LIMIT total."""
    size = fp._default_page_size()
    client = _render_page(
        monkeypatch, lang="en", findings=_fake_findings(total=size * 3 + 1)
    )
    position = _elements_with_class(client, f"{fp.PAGER_CLASS}-position")
    assert position, "the pager did not render"
    text = "\n".join(_subtree_strings(position[0]))
    assert "/ 4" in text, (
        f"expected 4 pages for a total of {size * 3 + 1} at page size {size}; got {text!r}"
    )
    previous = _elements_with_class(client, f"{fp.PAGER_CLASS}-prev")
    following = _elements_with_class(client, f"{fp.PAGER_CLASS}-next")
    assert previous and following
    assert previous[0].enabled is False, "Previous must be disabled on page 1"
    assert following[0].enabled is True, "Next must be enabled when more pages exist"


def test_pager_disables_next_on_the_last_page(monkeypatch):
    client = _render_page(monkeypatch, lang="en", findings=_fake_findings(total=1))
    following = _elements_with_class(client, f"{fp.PAGER_CLASS}-next")
    assert following and following[0].enabled is False


def test_page_size_cap_is_enforced_server_side_not_only_in_the_control(monkeypatch):
    """The page names only the BUDGETED DEFAULT; the ceiling lives in the
    service, so no control (and no environment variable) can widen a page beyond
    the budget."""
    from shared.discovery_service import DiscoveryService

    # 1. The service clamps whatever it is handed.
    assert DiscoveryService._clamp_findings_page_size(10 ** 6) == 200, (
        "the shared DISCOVERY_PAGE_SIZE_MAX ceiling is not enforced server-side"
    )
    assert DiscoveryService._clamp_findings_page_size(-5) >= 1

    # 2. Even an absurd environment default reaches the service unclamped by the
    #    page — the page must not silently pre-clamp and thereby hide the fact
    #    that the ceiling is the service's job.
    monkeypatch.setenv("DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT", "999999")
    assert fp._default_page_size() == 999999

    captured = {}

    async def _capture(unit="identification", **kwargs):
        captured.update(kwargs)
        return {"status": "ok", "items": [], "total": 0,
                "meta": {"unit": unit, "bucket": kwargs.get("bucket"),
                         "sort": kwargs.get("sort"), "approximate_total": False}}

    monkeypatch.setattr(fp, "get_findings_enveloped", _capture)
    asyncio.run(fp.fetch_findings(_state()))
    assert captured["page_size"] == 999999, (
        "the page pre-clamped the page size locally — the cap must be enforced "
        "server-side so there is exactly one ceiling"
    )

    # 3. And the module restates no ceiling of its OWN. Scoped to non-docstring
    #    literals: `_default_page_size`'s docstring legitimately explains that
    #    the ceiling is the service's job, and a raw substring scan would fail
    #    on that explanation rather than on a restated cap.
    tree = ast.parse(FINDINGS_SRC)
    skip = _docstring_nodes(tree)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or id(node) in skip:
            continue
        if isinstance(node.value, str) and "DISCOVERY_PAGE_SIZE_MAX" in node.value:
            pytest.fail(
                "web/pages/findings.py reads the shared page-size ceiling itself "
                "— it belongs to the service"
            )
        if isinstance(node.value, int) and not isinstance(node.value, bool):
            assert node.value != 200, (
                "web/pages/findings.py restates the page-size ceiling as a literal"
            )


# ---------------------------------------------------------------------------
# Persistence through the chokepoint
# ---------------------------------------------------------------------------

def test_selections_persist_through_the_storage_chokepoint(monkeypatch):
    """Filter, unit, bucket and sort all go through safe_storage — never the raw
    per-user store (T-136-16-07)."""
    store = {}
    monkeypatch.setattr(fp, "safe_user_get", lambda k, d=None: store.get(k, d))
    monkeypatch.setattr(fp, "safe_user_set", lambda k, v: store.__setitem__(k, v))

    fp.write_state(_state(bucket="more", sort="page_count", unit="manuscript",
                          novelty_view=fp.NOVELTY_VIEW_CANDIDATES,
                          domain="Liturgy", page=3))
    assert store["discovery_findings_bucket"] == "more"
    assert store["discovery_findings_sort"] == "page_count"
    assert store["discovery_findings_unit"] == "manuscript"
    assert store[fp._KEY_NOVELTY_VIEW] == fp.NOVELTY_VIEW_CANDIDATES
    assert store["discovery_findings_domain"] == "Liturgy"
    assert store["discovery_findings_page"] == 3

    restored = fp.read_state()
    assert restored["bucket"] == "more"
    assert restored["sort"] == "page_count"
    assert restored["unit"] == "manuscript"
    assert restored["novelty_view"] == fp.NOVELTY_VIEW_CANDIDATES
    assert restored["domain"] == "Liturgy"
    assert restored["page"] == 3


def test_out_of_vocabulary_persisted_values_fall_back_instead_of_reaching_the_service(monkeypatch):
    """An out-of-vocabulary unit/sort/bucket RAISES in the service rather than
    becoming an envelope, so a hand-edited value must never get that far."""
    store = {
        "discovery_findings_unit": "claim",          # deliberately not offered
        "discovery_findings_sort": "novelty",        # never a sort key
        "discovery_findings_bucket": "all",          # in the vocabulary, not offered
        "discovery_findings_page": "not-a-number",
    }
    monkeypatch.setattr(fp, "safe_user_get", lambda k, d=None: store.get(k, d))
    monkeypatch.setattr(fp, "safe_user_set", lambda k, v: None)

    restored = fp.read_state()
    assert restored["unit"] == "identification"
    assert restored["sort"] == "band_rank"
    assert restored["bucket"] == "main"
    assert restored["page"] == 1


def test_empty_filter_selection_means_all(monkeypatch):
    """Filters compose as AND and an empty set is not a filter — a reader who
    has selected nothing is never shown nothing."""
    captured = {}

    async def _capture(unit="identification", **kwargs):
        captured.update(kwargs)
        return {"status": "ok", "items": [], "total": 0, "meta": {}}

    monkeypatch.setattr(fp, "get_findings_enveloped", _capture)
    asyncio.run(fp.fetch_findings(_state()))
    assert captured["novelty"] is None
    assert captured["domain"] is None
    assert captured["author"] is None
    assert captured["work_id"] is None


def test_novelty_switch_selects_only_the_candidacy_shade(monkeypatch):
    from shared.discovery_novelty import CANDIDATE_STATUS

    captured = {}

    async def _capture(unit="identification", **kwargs):
        captured.update(kwargs)
        return {"status": "ok", "items": [], "total": 0, "meta": {}}

    monkeypatch.setattr(fp, "get_findings_enveloped", _capture)
    asyncio.run(fp.fetch_findings(_state(novelty_view=fp.NOVELTY_VIEW_CANDIDATES)))
    assert captured["novelty"] == (CANDIDATE_STATUS,)


# ===========================================================================
# TASK 4 (2026-08-04, owner verdict) — the page is SHAPED: `/catalog-browse`'s
# card sidebar, the prose demoted into a collapsed panel, the domain facet a
# counted collapsible tree, and Hebrew genre labels.
#
# The owner's words: "/computed-identifications is not shaped at all, looks
# more like a draft… A bunch of headlines, then long domains list, then
# dropdown of works. Compare to /catalog-browse."
# ===========================================================================

import web.components.findings_rows as rows  # noqa: E402
import web.discovery_genre_labels as gl  # noqa: E402
from shared.discovery_display_strings import (  # noqa: E402
    novelty_strings,
    recall_disclaimer,
    rule_sentence,
)

CATALOG_BROWSE_SRC = (
    pathlib.Path(__file__).resolve().parents[1] / "web" / "pages" / "catalog_browse.py"
).read_text(encoding="utf-8")

#: The card-header treatment `/catalog-browse` uses on DOMAIN / FILTER BY
#: AVAILABILITY / FILTER BY LIBRARY. Read from ITS source at test time, never
#: retyped here, so the two pages cannot drift into two visual languages
#: without this failing.
_CATALOG_CARD_HEADER_CLASSES = "text-sm font-bold uppercase tracking-wide"

#: A two-level domain facet, in exactly the shape
#: `DiscoveryService._project_facets` emits: each parent as its own selectable
#: node carrying the SUM of its leaves, each leaf carrying its parent's key.
_DOMAIN_PARENT = "Liturgy and Brakhot"
_DOMAIN_LEAF_A = "Liturgy and Brakhot / Common Prayers"
_DOMAIN_LEAF_B = "Liturgy and Brakhot / Passover Haggadah"
_DOMAIN_ORPHAN = "Unassigned"

_DOMAIN_TREE = [
    {"level": "domain", "value": _DOMAIN_PARENT, "label": _DOMAIN_PARENT,
     "parent": None, "is_leaf": False, "count": 11},
    {"level": "domain", "value": _DOMAIN_LEAF_A, "label": _DOMAIN_LEAF_A,
     "parent": _DOMAIN_PARENT, "is_leaf": True, "count": 7},
    {"level": "domain", "value": _DOMAIN_LEAF_B, "label": _DOMAIN_LEAF_B,
     "parent": _DOMAIN_PARENT, "is_leaf": True, "count": 4},
    {"level": "domain", "value": _DOMAIN_ORPHAN, "label": _DOMAIN_ORPHAN,
     "parent": None, "is_leaf": True, "count": 3},
]

#: The real FJMS Hebrew names for that parent and one of its leaves (read off
#: the live sidecar 2026-08-04; stubbed here so the suite needs no 1.5 GB
#: database and no environment luck).
_DOMAIN_PARENT_HE = "תפילה וברכות"
_DOMAIN_LEAF_A_HE = "תפילות שכיחות"


@pytest.fixture(autouse=True)
def _no_live_fjms_read_during_page_tests():
    """Every test here starts with the domain-label cache BUILT AND EMPTY.

    Without this a Hebrew render would prime the cache for real: 1.5 GB of FJMS
    opened inside the test process, and a suite whose rendered labels depend on
    whether the machine happens to carry a sidecar. Built-and-empty is a state
    the production code genuinely reaches (FJMS absent), and it is the state the
    English-label assertions below are about. The tests that need a map inject
    one explicitly, and `test_a_hebrew_page_primes_the_domain_labels...` proves
    the priming call still happens.
    """
    gl.reset_for_tests()
    gl._STATE["map"] = {}
    yield
    gl.reset_for_tests()


def _facets_with_domain_tree():
    items = dict(_FACET_ITEMS)
    items["domain"] = _DOMAIN_TREE
    return _fake_facets(items)


def _facet_select(client, level: str):
    """The AUTHOR or WORK facet's searchable select (2026-08-05).

    Those two facets are `ui.select(with_input=True, clearable)` now, not flat
    button stacks -- 478 unsearchable buttons in a 340px box was the worst
    control on the page. Their option labels live in `_props['options']` rather
    than in element text, so an assertion about what a reader can pick has to
    read them from there or it is asserting about an empty subtree.
    """
    selects = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}-select")
    assert len(selects) <= 1, f"more than one {level} select rendered"
    return selects[0] if selects else None


def _facet_option_labels(client, level: str) -> list:
    control = _facet_select(client, level)
    if control is None:
        return []
    return [option["label"] for option in (control._props or {}).get("options") or []]


_OPTION_COUNT_SUFFIX = _re.compile(r"\s+\([\d,]+\)$")


def _facet_option_names(client, level: str) -> list:
    """Option labels with the facet count stripped, so an assertion can compare
    a NAME exactly.

    `startswith` is not a substitute and it is a trap here: the curated Hebrew
    title for `w000176` begins with the raw recorded one, so a prefix test
    would report the raw title as present on the very label that proves ruling
    R is being applied.
    """
    return [_OPTION_COUNT_SUFFIX.sub("", label)
            for label in _facet_option_labels(client, level)]


def _facet_option_values(client, level: str) -> list:
    """The VALUES a reader's pick would set, in NiceGUI's own option order --
    read through the element's `options` mapping, which is the stored key the
    service filters on and never the label."""
    control = _facet_select(client, level)
    if control is None:
        return []
    options = control.options
    return list(options) if isinstance(options, dict) else list(options)


def _node_texts(client, marker: str) -> list:
    """Every rendered string under `marker`, as a list (not one blob), so an
    assertion can be about ONE node rather than about the page."""
    return [
        text
        for element in _elements_with_class(client, marker)
        for text in _subtree_strings(element)
    ]


# ---------------------------------------------------------------------------
# The card sidebar
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("marker", [
    # NO "-coverage". That card was deleted on 2026-08-06: it rendered a
    # permanently-disabled control, and the axis is not coming, because the
    # main-pool rule already gates on `COVERAGE_FLOOR` -- the pool control IS
    # the coverage filter. See `_render_filter_bar`.
    "-novelty", "-domain", "-author", "-work",
])
def test_each_filter_group_is_a_card_in_the_sidebar_column(monkeypatch, marker):
    """`/catalog-browse` renders each filter as a white card in a left column.
    The findings page's filters were a flat, unstyled vertical stack; they are
    cards now, and each one is a REAL card element rather than a div with a
    border, so the two pages share Quasar's card treatment."""
    client = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    groups = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}{marker}")
    assert len(groups) == 1, f"expected one {marker!r} group, got {len(groups)}"
    card = groups[0]
    assert card.tag == "q-card", (
        f"the {marker!r} filter is a <{card.tag}>, not a card — `/catalog-browse` "
        "puts every filter in a ui.card and this page must read as the same product"
    )
    classes = card._classes or []
    assert "fg" in classes, (
        "the card must carry `fg`: the shared CSS block styles `.fg.novgrp` "
        "(first position) and `.fg.blocked` (dimming) on the GROUP element"
    )
    assert fp.CARD_CLASS in classes
    ancestor_classes = {c for a in _ancestors(card) for c in (a._classes or [])}
    assert fp.FILTER_BAR_CLASS in ancestor_classes


def test_the_bucket_control_also_sits_in_a_card_and_still_passes_ruling_t(monkeypatch):
    """The "more matches" control moved INTO a card with the others, which is a
    layout change it is allowed to have — and it must still be a first-class,
    non-disclosed control (ruling T). A `q-card` is not a disclosure container;
    an expansion would be, and the existing ancestor test would fail."""
    client = _render_page(monkeypatch, lang="en")
    groups = _elements_with_class(client, f"{fp.BUCKET_CONTROL_CLASS}-group")
    assert len(groups) == 1
    assert groups[0].tag == "q-card"
    control = _find_bucket_control(client, "en")
    assert control is not None
    tags = {(getattr(a, "tag", "") or "").lower() for a in _ancestors(control)}
    assert "nicegui-expansion" not in tags and "q-expansion-item" not in tags, (
        "the bucket control ended up inside an expansion — ruling T forbids it"
    )


def test_the_card_headers_use_catalog_browses_own_header_treatment(monkeypatch):
    """Pinned to `/catalog-browse`'s SOURCE, read at test time.

    A retyped copy of the class string would let the two pages drift apart
    silently, which is exactly the "looks like a different product" complaint
    this work answers."""
    assert _CATALOG_CARD_HEADER_CLASSES in CATALOG_BROWSE_SRC, (
        "`/catalog-browse` no longer uses this card-header treatment — the "
        "findings page was matched to it, so re-check both pages together"
    )
    client = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    headers = _elements_with_class(client, fp.CARD_HEADER_CLASS)
    assert headers, "no card headers rendered"
    wanted = set(_CATALOG_CARD_HEADER_CLASSES.split())
    for header in headers:
        assert wanted <= set(header._classes or []), (
            f"a card header carries {header._classes!r}, not `/catalog-browse`'s "
            f"{_CATALOG_CARD_HEADER_CLASSES!r}"
        )


def test_the_domain_card_header_still_names_the_identified_work(monkeypatch):
    """The header moved into a card and changed size; it must NOT have changed
    what it says. Filtering on the MANUSCRIPT's catalogue domain would hide the
    findings that disagree with the catalogue."""
    for lang in ("en", "he"):
        client = _render_page(monkeypatch, lang=lang, facets=_facets_with_domain_tree())
        headers = _elements_with_class(client, f"{fp.FACET_HEADER_CLASS}-domain")
        assert len(headers) == 1
        # A set, because a NiceGUI label reports its text through more than one
        # attribute; the property is that the header says this and ONLY this.
        said = set(_subtree_strings(headers[0]))
        assert said == {rows.copy_text("facet_domain_header", lang)}, (
            f"the domain header reads {said!r}, not the ratified axis wording"
        )


def test_the_filter_cards_and_the_results_are_two_columns_of_one_body_row(monkeypatch):
    """The shape the owner asked for: cards on one side, results beside them.
    Before this, everything was one long column."""
    client = _render_page(monkeypatch, lang="en")
    bodies = _elements_with_class(client, fp.BODY_CLASS)
    assert len(bodies) == 1, "the two-column body row did not render"
    body = bodies[0]

    bars = _elements_with_class(client, fp.FILTER_BAR_CLASS)
    mains = _elements_with_class(client, fp.MAIN_CLASS)
    assert len(bars) == 1 and len(mains) == 1
    assert body in _ancestors(bars[0]), "the filter bar is not inside the body row"
    assert body in _ancestors(mains[0]), "the results column is not inside the body row"
    assert fp.MAIN_CLASS not in set(
        c for a in _ancestors(bars[0]) for c in (a._classes or [])), (
        "the filter bar is nested INSIDE the results column, not beside it")

    results = _elements_with_class(client, fp.RESULTS_CLASS)
    assert results and mains[0] in _ancestors(results[0]), (
        "the results region is not in the results column")


# ---------------------------------------------------------------------------
# The demoted prose — moved, complete, and collapsed
# ---------------------------------------------------------------------------

def _demoted_prose(lang: str) -> dict:
    """The four pieces of copy the owner asked to demote, from the modules that
    OWN them — never retyped here, so a reworded line fails rather than a
    stale copy quietly agreeing with itself."""
    return {
        "recall disclaimer": recall_disclaimer(lang),
        "candidacy sub-line": novelty_strings(lang)["subline"],
        "novelty help (sources + candidacy sentence)": novelty_strings(lang)["help"],
        "two-bucket rule": rule_sentence(lang),
    }


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_demoted_prose_is_present_verbatim_inside_the_collapsible(monkeypatch, lang):
    """MOVED, not deleted and not reworded. Every line is honesty-critical text
    under D-06a and the match-framing rule; the owner asked for it out of the
    way, not out of the page."""
    client = _render_page(monkeypatch, lang=lang)
    panels = _elements_with_class(client, fp.HOWTO_CLASS)
    assert len(panels) == 1, "the 'how to read this page' panel did not render"
    inside = "\n".join(_subtree_strings(panels[0]))
    for what, text in _demoted_prose(lang).items():
        assert text in inside, (
            f"the {what} is missing from the collapsible ({lang}): {text!r}"
        )


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_collapsible_is_collapsed_by_default(monkeypatch, lang):
    client = _render_page(monkeypatch, lang=lang)
    panel = _elements_with_class(client, fp.HOWTO_CLASS)[0]
    assert getattr(panel, "value", None) is False, (
        "the explanatory panel opens by default — the wall of prose is back"
    )
    assert (panel._props or {}).get("model-value") is False


@pytest.mark.parametrize("lang", ["en", "he"])
def test_only_the_caveat_and_the_headline_remain_visible_in_the_head(monkeypatch, lang):
    """At most ONE short caveat line stays out in the open. The head carries the
    caveat and ruling U's headline; every other line of prose is one click
    away."""
    client = _render_page(monkeypatch, lang=lang)
    heads = _elements_with_class(client, fp.HEAD_CLASS)
    assert len(heads) == 1
    panel = _elements_with_class(client, fp.HOWTO_CLASS)[0]
    panel_texts = set(_subtree_strings(panel))

    visible = [
        text for text in _subtree_strings(heads[0])
        if text not in panel_texts
    ]
    for what, text in _demoted_prose(lang).items():
        assert text not in visible, (
            f"the {what} is still rendered OUTSIDE the collapsible in the head"
        )
    # ...and the caveat is NOT in the collapsible: it is the one line that stays.
    caveat = fp.copy_text("caveat", lang)
    assert caveat in visible, "the permanent caveat left the visible head"
    assert caveat not in panel_texts, "the caveat was demoted — it must stay visible"


def test_the_caveat_is_not_inside_the_collapsible_element(monkeypatch):
    client = _render_page(monkeypatch, lang="en")
    caveat = _elements_with_class(client, fp.CAVEAT_CLASS)[0]
    ancestor_classes = {c for a in _ancestors(caveat) for c in (a._classes or [])}
    assert fp.HOWTO_CLASS not in ancestor_classes


# ---------------------------------------------------------------------------
# The launch headline — same figures and wording, one block instead of seven
# stacked lines
# ---------------------------------------------------------------------------

def test_the_headline_notes_share_one_wrapping_row_rather_than_stacking(monkeypatch):
    """Ruling U's figures stay visible and unchanged in substance; what changed
    is that the basis line, the fragment line and the context line sit on ONE
    wrapping row, and the three contribution shades on another. Asserted
    structurally: on the old implementation each was a direct child of the
    headline column, and this fails."""
    from web.components import findings_rows as fr

    envelope = {
        "status": "ok",
        "items": [
            {"shade": shade, "identification_count": 10 + i, "manuscript_count": 5 + i}
            for i, shade in enumerate(("fills_gap", "refines_granularity",
                                       "container_predicts"))
        ],
        "total": 33,
        "meta": {"basis": "main_pool", "main_pool_manuscript_count": 7,
                 "corpus_manuscript_count": 88, "corpus_page_count": 99},
    }
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    holder = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_headline_layout_probe")) as client:
            with client:
                fr.render_launch_headline(envelope, "en")
        holder["client"] = client

    asyncio.run(_run())
    client = holder["client"]

    notes = _elements_with_class(client, fr.LAUNCH_BASIS_CLASS)
    context = _elements_with_class(client, fr.LAUNCH_CONTEXT_CLASS)
    assert len(notes) == 2 and len(context) == 1
    parents = {n.parent_slot.parent for n in notes + context}
    assert len(parents) == 1, "the headline's small notes are still stacked apart"
    row = parents.pop()
    assert "flex-wrap" in (row._classes or []), (
        "the notes row does not wrap — on a phone it would overflow instead")

    shades = _elements_with_class(client, fr.LAUNCH_SHADE_CLASS)
    assert len(shades) == 3

    # THE SHADES SHARE ONE WRAPPING GROUP -- the property this has always
    # asserted. Since the stat-card band (owner, 2026-08-06) each shade is a CARD,
    # so its immediate parent is its own card and the shared group is one level
    # up. Walking to the nearest common ancestor is what keeps the assertion about
    # "are these three laid out together" rather than about how many wrappers deep
    # they happen to sit.
    def _ancestor_chain(element):
        chain = []
        slot = element.parent_slot
        while slot is not None and getattr(slot, "parent", None) is not None:
            chain.append(slot.parent)
            slot = getattr(slot.parent, "parent_slot", None)
        return chain

    chains = [_ancestor_chain(shade) for shade in shades]
    shared = [a for a in chains[0] if all(a in chain for chain in chains[1:])]
    assert shared, "the three shades share no common container -- they are stacked"
    group = shared[0]
    assert "flex-wrap" in (group._classes or []), (
        f"the shades' group does not wrap: {group._classes!r}")
    assert group is not row, (
        "the shades and the notes share one row — they are two distinct groups")


# ---------------------------------------------------------------------------
# TASK 10 (2026-08-05) — the headline LEADS WITH THE LARGER NUMBER.
#
# `total` is the SHADE-FILTERED contribution figure, so the block led with the
# subset and the release under-sold itself threefold. `meta.main_pool_total` and
# `meta.main_pool_total_manuscript_count` are an UNCONDITIONAL main-pool
# population — a different population from `total` and from
# `main_pool_manuscript_count`, both of which are shade filtered.
#
# The figures below are SENTINELS that appear in no artifact and in no committed
# figure file, so a hardcode fails these assertions in whatever form it took —
# a string, a numeric constant, a formatted expression, folded arithmetic, a
# value assembled across two module-level names, an import, or a file read. Only
# the first four of those are visible to a static scan.
# ---------------------------------------------------------------------------

_LEDE_SENTINEL_TOTAL = 41777
_LEDE_SENTINEL_MANUSCRIPTS = 38222


def _launch_envelope(*, with_lede: bool) -> dict:
    meta = {"basis": "main_pool", "main_pool_manuscript_count": 7,
            "corpus_manuscript_count": 88, "corpus_page_count": 99}
    if with_lede:
        meta["main_pool_total"] = _LEDE_SENTINEL_TOTAL
        meta["main_pool_total_manuscript_count"] = _LEDE_SENTINEL_MANUSCRIPTS
    return {
        "status": "ok",
        "items": [
            {"shade": shade, "identification_count": 10 + i, "manuscript_count": 5 + i}
            for i, shade in enumerate(("fills_gap", "refines_granularity",
                                       "container_predicts"))
        ],
        "total": 33,
        "meta": meta,
    }


def _render_headline(envelope, lang="en"):
    from web.components import findings_rows as fr

    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    holder = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_headline_rank_probe")) as client:
            with client:
                fr.render_launch_headline(envelope, lang)
        holder["client"] = client

    asyncio.run(_run())
    return holder["client"]


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_headline_ledes_with_the_pool_total_from_the_envelope(lang):
    from shared.discovery_display_strings import bucket_name
    from web.components import findings_rows as fr

    client = _render_headline(_launch_envelope(with_lede=True), lang)

    ledes = _elements_with_class(client, fr.LAUNCH_POOL_TOTAL_CLASS)
    assert len(ledes) == 1, "the lede figure did not render"
    assert getattr(ledes[0], "text", None) == "{:,}".format(_LEDE_SENTINEL_TOTAL), (
        "the lede figure is not the envelope's `meta.main_pool_total`")

    labels = _elements_with_class(client, fr.LAUNCH_POOL_LABEL_CLASS)
    assert len(labels) == 1
    assert getattr(labels[0], "text", None) == fr.copy_text(
        "launch_pool_total", lang).format(bucket=bucket_name(True, lang))

    # THE FIGURE AND ITS LABEL ARE TWO ELEMENTS IN ONE BASELINE ROW, never one
    # string: a leading Latin-digit run followed by a Hebrew phrase can reorder
    # unpredictably at the boundary.
    lede_rows = _elements_with_class(client, fr.LAUNCH_LEDE_CLASS)
    assert len(lede_rows) == 1
    assert ledes[0].parent_slot.parent is lede_rows[0]
    assert labels[0].parent_slot.parent is lede_rows[0]
    assert "items-baseline" in (lede_rows[0]._classes or [])

    text = "\n".join(_subtree_strings(_elements_with_class(client, fr.LAUNCH_CLASS)[0]))
    assert "{:,}".format(_LEDE_SENTINEL_MANUSCRIPTS) in text, (
        "the lede's fragment span is not the envelope's "
        "`meta.main_pool_total_manuscript_count`")
    # ...and the CONTRIBUTION figure is still there, at its own weight, with its
    # own basis line. The lede did not replace it.
    totals = _elements_with_class(client, fr.LAUNCH_TOTAL_CLASS)
    assert len(totals) == 1
    assert "33" in (getattr(totals[0], "text", None) or "")


def test_the_lede_and_the_contribution_are_separated():
    """A thin rule is what stops seven numbers reading as one list: it says
    everything below is a part of, or context for, what is above.

    LOGICAL, because this surface renders in both directions — the module's own
    guard fails on any physical directional property."""
    from web.components import findings_rows as fr

    client = _render_headline(_launch_envelope(with_lede=True), "en")
    block = _elements_with_class(client, fr.LAUNCH_CLASS)[0]
    separators = [
        element for element in block.descendants()
        if "border-block-start" in " ".join(element._style or {})
    ]
    assert len(separators) == 1, (
        "the lede and the contribution run together as one list of figures")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_headline_degrades_to_the_previous_block_without_the_new_keys(lang):
    """An older sidecar, or any envelope built before those keys existed, must
    render EXACTLY what shipped — never a fabricated zero, which is the same
    class of falsehood as a hardcoded figure."""
    from web.components import findings_rows as fr

    client = _render_headline(_launch_envelope(with_lede=False), lang)
    assert not _elements_with_class(client, fr.LAUNCH_POOL_TOTAL_CLASS), (
        "a missing `meta.main_pool_total` still rendered a lede figure")
    assert not _elements_with_class(client, fr.LAUNCH_LEDE_CLASS)

    text = "\n".join(_subtree_strings(_elements_with_class(client, fr.LAUNCH_CLASS)[0]))
    assert "0" not in text.replace("10", "").replace("100", ""), (
        f"the degraded headline invented a zero: {text!r}")
    # The 2026-08-04 block, element for element.
    assert len(_elements_with_class(client, fr.LAUNCH_TOTAL_CLASS)) == 1
    assert len(_elements_with_class(client, fr.LAUNCH_BASIS_CLASS)) == 2
    assert len(_elements_with_class(client, fr.LAUNCH_CONTEXT_CLASS)) == 1
    assert len(_elements_with_class(client, fr.LAUNCH_SHADE_CLASS)) == 3


# ---------------------------------------------------------------------------
# TASK E (2026-08-05) — the headline leads with the RELEASE, not with a pool.
#
# The lede was the main pool, a subset, and the second pool appeared nowhere in
# the headline at all. The first fix led with the all-in-all identification
# count; the owner chose against it from a rendered comparison, because 81% of
# fragments carry exactly one identification, so "53,581 identifications on
# 38,431 fragments" reads as an error rather than as a distribution. The
# approved block ledes with TWO DIFFERENT KINDS of thing -- fragments and works
# -- so no ratio is invited, states the all-in-all count quietly under them, and
# shows the pool split.
#
# All three lede figures share ONE basis (every bucket, every shade). The
# contribution keeps its own basis line, because it does not.
# ---------------------------------------------------------------------------

_ALL_SENTINEL_TOTAL = 71333
_ALL_SENTINEL_WORKS = 6444
_ALL_SENTINEL_FRAGMENTS = 59222
_ALL_SENTINEL_MORE_POOL = 34555


def _launch_envelope_v3() -> dict:
    envelope = _launch_envelope(with_lede=True)
    envelope["meta"].update({
        "identification_total": _ALL_SENTINEL_TOTAL,
        "work_total": _ALL_SENTINEL_WORKS,
        "corpus_manuscript_count": _ALL_SENTINEL_FRAGMENTS,
        "more_pool_total": _ALL_SENTINEL_MORE_POOL,
    })
    return envelope


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_headline_ledes_with_fragments_and_works_from_the_envelope(lang):
    from web.components import findings_rows as fr

    client = _render_headline(_launch_envelope_v3(), lang)

    fragments = _elements_with_class(client, fr.LAUNCH_FRAGMENTS_CLASS)
    assert len(fragments) == 1, "the lede figure did not render"
    assert getattr(fragments[0], "text", None) == "{:,}".format(_ALL_SENTINEL_FRAGMENTS)
    assert "text-4xl" in (fragments[0]._classes or []), (
        "the lede is not the largest figure in the block")

    labels = _elements_with_class(client, fr.LAUNCH_FRAGMENTS_LABEL_CLASS)
    assert len(labels) == 1
    assert getattr(labels[0], "text", None) == fr.copy_text(
        "launch_fragments_lede", lang)
    # TWO ELEMENTS IN ONE BASELINE ROW, never one string.
    lede_rows = _elements_with_class(client, fr.LAUNCH_LEDE_CLASS)
    assert len(lede_rows) == 1
    assert fragments[0].parent_slot.parent is lede_rows[0]
    assert labels[0].parent_slot.parent is lede_rows[0]

    # The work count is its OWN element, carrying a BIG weight, inside the
    # "matched to ... known works" card.
    #
    # `text-3xl`, not the `font-semibold` this asserted before the stat-card band
    # (owner ruling, 2026-08-06: "make all numbers big"). The figure moved out of
    # the middle of its sentence and into the card's own figure slot, which is
    # what let it be sized at all -- a 3xl number inside a running sentence
    # wrecks its line breaking.
    works = _elements_with_class(client, fr.LAUNCH_WORK_TOTAL_CLASS)
    assert len(works) == 1
    assert getattr(works[0], "text", None) == "{:,}".format(_ALL_SENTINEL_WORKS)
    assert "text-3xl" in (works[0]._classes or []), (
        f"the work count is not a big figure: {works[0]._classes!r}")
    matched = _elements_with_class(client, fr.LAUNCH_MATCHED_CLASS)
    assert len(matched) == 1 and works[0].parent_slot.parent is matched[0]
    # ...and the FIGURE AND ITS WORDS ARE STILL SEPARATE ELEMENTS under that
    # container, which is the RTL property this test has always been about: a
    # Latin-digit run and a Hebrew phrase in one string can reorder at the
    # boundary. What changed is the ORDER (figure first, words beneath) rather
    # than the separation, so this no longer reassembles the template -- it
    # asserts both parts are present, each in its own element.
    parts = [child.text or "" for child in matched[0]]
    assert len(parts) == 2, f"the card is not figure-plus-label: {parts!r}"
    assert "{:,}".format(_ALL_SENTINEL_WORKS) in parts
    # The ratified WORDS survive, minus the placeholder the figure now fills.
    words = fr._sentence_label(fr.copy_text("launch_matched_works", lang), lang)
    assert words in parts, f"{words!r} not in {parts!r}"

    # The all-in-all count, from its own key, as a card.
    all_totals = _elements_with_class(client, fr.LAUNCH_ALL_TOTAL_CLASS)
    assert len(all_totals) == 1
    said = [child.text or "" for child in all_totals[0]]
    assert "{:,}".format(_ALL_SENTINEL_TOTAL) in said, (
        f"the all-in-all figure is not in its card: {said!r}")
    assert fr._sentence_label(
        fr.copy_text("launch_matches_in_all", lang), lang) in said

    # ...and the CONTRIBUTION is still there, at its own weight, with its own
    # basis line: it is shade filtered and the three figures above it are not.
    totals = _elements_with_class(client, fr.LAUNCH_TOTAL_CLASS)
    assert len(totals) == 1
    assert "33" in (getattr(totals[0], "text", None) or "")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_headline_shows_the_pool_split_and_names_both_buckets(lang):
    """The level that makes the second pool visible as a comparable body of
    work. Ruling T forbids a count on the bucket CONTROL -- this is the
    headline, and a separate test asserts the figures did not reach the chips.
    """
    from shared.discovery_display_strings import bucket_name
    from web.components import findings_rows as fr

    client = _render_headline(_launch_envelope_v3(), lang)
    halves = _elements_with_class(client, fr.LAUNCH_SPLIT_ITEM_CLASS)
    assert len(halves) == 2, "the pool split did not render both halves"

    # ONE CARD PER POOL since the stat-card band (owner ruling, 2026-08-06), so
    # each half is a figure element plus a label element rather than one string.
    # The figure is BIG and the bucket name is the noun beneath it -- and the
    # bucket still comes from the single definition, which is the property this
    # test exists for.
    for count, in_main in ((_LEDE_SENTINEL_TOTAL, True),
                           (_ALL_SENTINEL_MORE_POOL, False)):
        figure = "{:,}".format(count)
        words = fr._sentence_label(
            fr.copy_text("launch_pool_share", lang), lang,
            bucket=bucket_name(in_main, lang))
        match = [
            half for half in halves
            if figure in [child.text or "" for child in half]
            and words in [child.text or "" for child in half]
        ]
        assert len(match) == 1, (
            f"no single card carries both {figure!r} and {words!r}: "
            + repr([[c.text for c in h] for h in halves]))
        # ...and the figure really is a big one, not a run of body text.
        big = [child for child in match[0]
               if (child.text or "") == figure
               and "text-3xl" in (child._classes or [])]
        assert big, f"the {figure!r} pool figure is not a big number"

    # BOTH halves carry the split marker, and they are SIBLINGS in the band --
    # they used to share a nested wrapper row, which made the pair one flex item
    # and stopped them wrapping with the other cards on a phone.
    assert all(fr.LAUNCH_SPLIT_CLASS in (half._classes or []) for half in halves)
    band = _elements_with_class(client, fr.LAUNCH_STATS_BAND_CLASS)
    assert len(band) == 1
    for half in halves:
        ancestors = set()
        slot = half.parent_slot
        while slot is not None and getattr(slot, "parent", None) is not None:
            ancestors.add(slot.parent)
            slot = getattr(slot.parent, "parent_slot", None)
        assert band[0] in ancestors, "a pool card is outside the stat band"


def test_a_half_of_the_split_with_no_figure_is_omitted_not_zeroed():
    from web.components import findings_rows as fr

    envelope = _launch_envelope_v3()
    del envelope["meta"]["more_pool_total"]
    client = _render_headline(envelope, "en")
    halves = _elements_with_class(client, fr.LAUNCH_SPLIT_ITEM_CLASS)
    assert len(halves) == 1, "a missing pool figure was rendered as a zero"
    # A card, so the figure is a CHILD element rather than the container's text.
    said = [child.text or "" for child in halves[0]]
    assert "{:,}".format(_LEDE_SENTINEL_TOTAL) in said, said
    # ...and no zero was invented for the absent half anywhere in the band.
    band_text = "\n".join(_subtree_strings(
        _elements_with_class(client, fr.LAUNCH_STATS_BAND_CLASS)[0]))
    assert "\n0\n" not in "\n" + band_text + "\n", (
        f"the missing pool figure was rendered as a zero: {band_text!r}")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_headline_claims_no_corpus_denominator(lang):
    """A COVERAGE OVERCLAIM, fixed. `corpus_manuscript_count` counts fragments
    that already carry a computed identification and `corpus_page_count` counts
    pages carrying at least one claim, while the project's corpus is ~255,615
    manuscript records -- so "out of N fragments ... in the whole corpus" made
    the release read roughly 6.6x better covered than it is, on the most
    prominent line of a scholarly surface.

    Asserted over EVERY block the module can render, not just the new one: the
    string was shared, and a fallback path that kept the overclaim would still
    ship it to anyone whose envelope is a version behind.
    """
    from web.components import findings_rows as fr

    for key in fr.copy_keys():
        value = fr.copy_text(key, lang)
        for claim in ("whole corpus", "entire corpus", "the corpus",
                      "כלל האוסף", "כל האוסף"):
            assert claim not in value, (
                f"{key!r} claims a corpus denominator it does not count: {value!r}")

    for envelope in (_launch_envelope_v3(),
                     _launch_envelope(with_lede=True),
                     _launch_envelope(with_lede=False)):
        client = _render_headline(envelope, lang)
        text = "\n".join(_subtree_strings(
            _elements_with_class(client, fr.LAUNCH_CLASS)[0]))
        for claim in ("whole corpus", "entire corpus", "כלל האוסף", "כל האוסף"):
            assert claim not in text, f"the headline still says {claim!r}: {text!r}"


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_headline_degrades_to_the_previous_lede_block_without_the_all_keys(lang):
    """Three blocks, each the previous one's fallback. An envelope carrying the
    lede pair but no `identification_total` renders the earlier 2026-08-05
    block, and a missing key never becomes a rendered zero."""
    from web.components import findings_rows as fr

    client = _render_headline(_launch_envelope(with_lede=True), lang)
    assert not _elements_with_class(client, fr.LAUNCH_FRAGMENTS_CLASS)
    assert not _elements_with_class(client, fr.LAUNCH_ALL_TOTAL_CLASS)
    assert not _elements_with_class(client, fr.LAUNCH_SPLIT_ITEM_CLASS)
    # ...and the block it DOES render is the pool-lede one, intact.
    assert len(_elements_with_class(client, fr.LAUNCH_POOL_TOTAL_CLASS)) == 1


def test_no_launch_figure_is_written_as_a_literal_in_the_row_component():
    """The lede's two figures join the forbidden set the moment the committed
    figure file names them, and 136-22's guard is RUN here rather than assumed
    to still hold."""
    import tests.test_discovery_launch_stats as guard

    committed = guard.load_committed_figures()
    for key in ("meta.main_pool_total", "meta.main_pool_total_manuscript_count"):
        assert key in committed, (
            f"{key} is not in the committed figure file, so no literal of it is "
            "forbidden anywhere — regenerate the file")

    for key in ("meta.identification_total", "meta.work_total"):
        assert key in committed, (
            f"{key} is not in the committed figure file, so no literal of it is "
            "forbidden anywhere — regenerate the file")

    figures = guard.forbidden_figures()
    key_names = guard.envelope_key_names(_launch_envelope_v3())
    root = pathlib.Path(__file__).resolve().parents[1]
    violations = guard.scan_launch_literals(root, figures, key_names)
    assert not violations, "launch figures found as literals: " + "; ".join(
        v.message() for v in violations)
    # The sentinels above must never BE real figures, or these tests would be
    # agreement rather than provenance.
    for sentinel in (_LEDE_SENTINEL_TOTAL, _LEDE_SENTINEL_MANUSCRIPTS,
                     _ALL_SENTINEL_TOTAL, _ALL_SENTINEL_WORKS,
                     _ALL_SENTINEL_FRAGMENTS, _ALL_SENTINEL_MORE_POOL):
        assert sentinel not in figures


# ---------------------------------------------------------------------------
# The domain facet — counts and parent→child collapse
# ---------------------------------------------------------------------------

def test_domain_nodes_carry_the_facet_counts_from_the_envelope(monkeypatch):
    """`/catalog-browse` shows `Bible: Texts and Translations (56,028)`. The
    findings page's domain list showed no counts at all, so a reader could not
    tell a domain with three findings from one with three thousand."""
    client = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    texts = _node_texts(client, f"{fp.FILTER_BAR_CLASS}-domain-items")
    for item in _DOMAIN_TREE:
        expected = "({:,})".format(item["count"])
        assert any(expected in text for text in texts), (
            f"the {item['value']!r} node does not carry its facet count "
            f"{expected}: {texts!r}"
        )


def test_domain_leaves_are_grouped_under_their_parent_and_start_collapsed(monkeypatch):
    """The flat stack is gone: a parent node, a chevron beside it, and its
    leaves in a container that starts closed."""
    client = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    branches = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-branch")
    assert len(branches) == 1, "the parent/child grouping did not render"
    boxes = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-children")
    assert len(boxes) == 1
    assert boxes[0].visible is False, "the leaves are not collapsed by default"

    inside = "\n".join(_subtree_strings(boxes[0]))
    assert "Common Prayers" in inside and "Passover Haggadah" in inside
    toggles = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-toggle")
    assert len(toggles) == 1, "the parent has no expand control"
    assert (toggles[0]._props or {}).get("icon") == "expand_more"

    # The orphan (no parent) is NOT swallowed by the tree — a silent
    # "Unassigned" bucket is exactly what the design forbids.
    branch_texts = set(_subtree_strings(branches[0]))
    assert not any(_DOMAIN_ORPHAN in text for text in branch_texts)
    assert any(_DOMAIN_ORPHAN in text
               for text in _node_texts(client, f"{fp.FILTER_BAR_CLASS}-domain-items"))


def test_the_chevron_actually_opens_and_closes_the_branch(monkeypatch):
    """Driven through the element's OWN registered listener, not by calling a
    handler this test reached into the module for: a chevron that is wired to
    nothing renders identically to one that works."""
    from nicegui.events import handle_event

    client = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    toggle = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-toggle")[0]
    box = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-children")[0]
    assert box.visible is False

    listeners = [
        listener for listener in toggle._event_listeners.values()
        if listener.type == "click"
    ]
    assert listeners, "the chevron has no click listener at all"

    handle_event(listeners[0].handler, None)
    assert box.visible is True, "the chevron did not open the branch"
    assert (toggle._props or {}).get("icon") == "expand_less"

    handle_event(listeners[0].handler, None)
    assert box.visible is False, "the chevron did not close the branch again"
    assert (toggle._props or {}).get("icon") == "expand_more"


def test_a_leaf_whose_parent_node_is_absent_is_still_rendered(monkeypatch):
    """The service emits a parent node for every leaf that names one, so this
    should not occur — and if it ever does, the leaf must still be selectable.
    A facet list that silently drops a domain is the one failure it cannot
    have."""
    orphan = dict(_DOMAIN_TREE[1])  # a leaf naming a parent that is NOT offered
    client = _render_page(
        monkeypatch, lang="en",
        facets=_fake_facets({**_FACET_ITEMS, "domain": [orphan]}))
    texts = _node_texts(client, f"{fp.FILTER_BAR_CLASS}-domain-items")
    assert any(_DOMAIN_LEAF_A in text for text in texts), (
        f"an orphaned leaf vanished from the facet list: {texts!r}")


def test_the_branch_holding_the_current_selection_starts_open(monkeypatch):
    """A collapse that hides the reader's own active filter is worse than no
    collapse."""
    client = _render_page(
        monkeypatch, lang="en", facets=_facets_with_domain_tree(),
        state=_state(domain=_DOMAIN_LEAF_A))
    boxes = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-children")
    assert boxes and boxes[0].visible is True, (
        "the selected leaf is hidden inside a collapsed branch")
    toggles = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-toggle")
    assert (toggles[0]._props or {}).get("icon") == "expand_less"


def test_a_leaf_shows_its_own_name_and_keeps_the_full_path_on_its_tooltip(monkeypatch):
    """`Liturgy and Brakhot / Common Prayers` under a heading that already says
    `Liturgy and Brakhot` is a row that truncates for no reason. The full path
    is one hover away."""
    client = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    box = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-children")[0]
    labels = [
        (el._props or {}).get("label")
        for el in box.descendants()
        if "dnode" in (getattr(el, "_classes", None) or [])
    ]
    assert "Common Prayers (7)" in labels, labels
    assert not any(label and _DOMAIN_LEAF_A == label for label in labels), (
        "the leaf repeats its parent's name in its own label")
    tooltips = _subtree_strings(box)
    assert any(_DOMAIN_LEAF_A in text for text in tooltips), (
        "the full domain path is not reachable from the leaf at all")


# ---------------------------------------------------------------------------
# Hebrew genre labels — the i18n gap
# ---------------------------------------------------------------------------

def test_a_hebrew_reader_gets_hebrew_domain_labels(monkeypatch):
    """`works.genre` is 100% English and it is this page's main facet, so a
    Hebrew reader met an English filter list. FJMS holds the authority; this is
    a display-time lookup over it."""
    monkeypatch.setitem(gl._STATE, "map", {
        _DOMAIN_PARENT: _DOMAIN_PARENT_HE,
        "Common Prayers": _DOMAIN_LEAF_A_HE,
    })
    client = _render_page(monkeypatch, lang="he", facets=_facets_with_domain_tree())
    texts = _node_texts(client, f"{fp.FILTER_BAR_CLASS}-domain-items")
    blob = "\n".join(texts)
    assert _DOMAIN_PARENT_HE in blob, f"the Hebrew parent name is absent: {texts!r}"
    assert _DOMAIN_LEAF_A_HE in blob, f"the Hebrew leaf name is absent: {texts!r}"
    assert not any(text.startswith(_DOMAIN_PARENT + " (") for text in texts), (
        "the English domain name is still on a node label for a Hebrew reader")


def test_an_english_reader_gets_the_stored_english_genre_string(monkeypatch):
    """The complementary case — proves the mapping is language-gated and not a
    constant rewrite."""
    monkeypatch.setitem(gl._STATE, "map", {
        _DOMAIN_PARENT: _DOMAIN_PARENT_HE,
        "Common Prayers": _DOMAIN_LEAF_A_HE,
    })
    client = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    blob = "\n".join(_node_texts(client, f"{fp.FILTER_BAR_CLASS}-domain-items"))
    assert _DOMAIN_PARENT in blob
    assert _DOMAIN_PARENT_HE not in blob


def test_a_domain_with_no_hebrew_name_falls_back_to_english_never_blank(monkeypatch):
    monkeypatch.setitem(gl._STATE, "map", {_DOMAIN_PARENT: _DOMAIN_PARENT_HE})
    client = _render_page(monkeypatch, lang="he", facets=_facets_with_domain_tree())
    texts = _node_texts(client, f"{fp.FILTER_BAR_CLASS}-domain-items")
    assert any("Passover Haggadah" in text for text in texts), (
        "an unmapped leaf rendered blank instead of falling back to English")
    assert all(text.strip() for text in texts if text is not None)


def test_the_selection_stays_keyed_on_the_english_value_in_hebrew(monkeypatch):
    """The label is translated; the VALUE is not. A translated value would stop
    matching the service's filter and would not persist across a language
    switch."""
    monkeypatch.setitem(gl._STATE, "map", {
        _DOMAIN_PARENT: _DOMAIN_PARENT_HE,
        "Common Prayers": _DOMAIN_LEAF_A_HE,
    })
    # The persisted state holds the ENGLISH value; the Hebrew page must render
    # that node as SELECTED.
    client = _render_page(
        monkeypatch, lang="he", facets=_facets_with_domain_tree(),
        state=_state(domain=_DOMAIN_LEAF_A))
    pressed = [
        el for el in _elements_with_class(client, "dnode")
        if (el._props or {}).get("aria-pressed") == "true"
    ]
    assert len(pressed) == 1, (
        "the Hebrew page did not mark the English-keyed selection as active")
    assert _DOMAIN_LEAF_A_HE in ((pressed[0]._props or {}).get("label") or ""), (
        "the selected node is not the Hebrew-labelled one")

    # And the pure mapping never rewrites the row it is handed.
    item = dict(_DOMAIN_TREE[1])
    label = fp.facet_display_label("domain", item, "he")
    assert label != item["value"]
    assert item["value"] == _DOMAIN_LEAF_A


def test_the_unassigned_bucket_is_visible_and_named_in_both_languages(monkeypatch):
    """"A silent 'unassigned' domain bucket" is on the design's forbidden list:
    works the vocabulary cannot place must stay visible. It must also not be the
    one English item on an otherwise Hebrew page."""
    monkeypatch.setitem(gl._STATE, "map", {_DOMAIN_PARENT: _DOMAIN_PARENT_HE})
    english = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    assert any(_DOMAIN_ORPHAN in text
               for text in _node_texts(english, f"{fp.FILTER_BAR_CLASS}-domain-items"))

    hebrew = _render_page(monkeypatch, lang="he", facets=_facets_with_domain_tree())
    texts = _node_texts(hebrew, f"{fp.FILTER_BAR_CLASS}-domain-items")
    assert not any(text.startswith(_DOMAIN_ORPHAN + " (") for text in texts), (
        f"the unassigned bucket is still English on a Hebrew page: {texts!r}")
    assert any("(3)" in text and not text.isascii() for text in texts), (
        f"the unassigned bucket lost its count or its name: {texts!r}")


def test_a_hebrew_page_primes_the_domain_labels_and_an_english_page_does_not(monkeypatch):
    """The prime is the ONLY thing that reads FJMS, it runs off the loop, and it
    is not paid for by readers who cannot benefit from it."""
    calls = {"n": 0}

    async def _prime():
        calls["n"] += 1

    monkeypatch.setattr(fp, "prime_domain_translations", _prime)
    _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    assert calls["n"] == 0, "an English page paid for the Hebrew label lookup"
    _render_page(monkeypatch, lang="he", facets=_facets_with_domain_tree())
    assert calls["n"] == 1, "a Hebrew page never primed the domain labels"


def test_the_work_facet_is_not_translated_through_the_domain_vocabulary(monkeypatch):
    """Work titles route through ruling R's curation and NOTHING else. A domain
    map that also fired on titles would be a second title vocabulary."""
    monkeypatch.setitem(gl._STATE, "map", {_UNCURATED_RAW_TITLE: "לא נכון"})
    client = _render_page(monkeypatch, lang="he")
    blob = "\n".join(_facet_option_labels(client, "work"))
    assert _UNCURATED_RAW_TITLE in blob
    assert "לא נכון" not in blob


# ===========================================================================
# TASK 5 (2026-08-05) — THE FACET LISTS ARE PART OF THE ONE REFRESH PATH.
#
# They were filled once, after the first paint, and never again. Three live
# consequences, all of them correctness rather than polish:
#
#   1. a count beside a domain described whichever BUCKET was active at first
#      paint, while `_node_text`'s docstring promises the count "always agrees
#      with the result set that domain produces";
#   2. the domain -> author -> work CASCADE never ran, because `_facet_request`
#      was only ever evaluated against the state as it stood at page load;
#   3. a facet node's `.here` treatment is decided when the node is BUILT, so
#      the reader's own selection was never marked.
#
# Re-reading all three levels on every interaction would triple this page's
# draw on the HEAVY bounded-concurrency budget, so the read (not the render) is
# skipped when a level's own request arguments are unchanged. Both directions
# are asserted below: a skip that never happens is a budget regression, and a
# skip that happens too often is defect 1 and 2 back again.
# ===========================================================================

_FACET_MAIN_SENTINEL = "FACET-MAIN-POOL-SENTINEL"
_FACET_MORE_SENTINEL = "FACET-MORE-MATCHES-SENTINEL"


def _bucket_keyed_facets():
    """A cascade whose DOMAIN list differs by bucket, so "which bucket answered"
    is legible in the render rather than only in the outgoing call."""
    per_bucket = {
        "main": _FACET_MAIN_SENTINEL,
        "more": _FACET_MORE_SENTINEL,
    }

    async def _call(level, *, bucket="main", **_kw):
        if level == "domain":
            items = [{"level": "domain", "value": per_bucket[bucket],
                      "label": per_bucket[bucket], "parent": None,
                      "is_leaf": True, "count": 7}]
        else:
            items = list(_FACET_ITEMS.get(level, []))
        return {"status": "ok", "items": items, "total": len(items),
                "meta": {"level": level, "bucket": bucket}}

    return _call


def _drive_facet_rounds(monkeypatch, rounds, *, lang="en", state=None,
                        status_for=None):
    """Build a REAL filter bar, then run `_populate_facets` once per round.

    Returns `[(label, [levels read]), ...]` plus the client, so an assertion can
    be about which levels issued a READ and about what the containers now hold.

    `rounds` is a list of `(label, mutation)`; each mutation is applied to the
    shared state dict before that round runs, so the rounds compose exactly the
    way a reader's successive interactions do.

    `status_for(round_label, level)` -> an envelope status, so a round can be
    made to fail. Defaults to `ok` everywhere.
    """
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    reads: list = []
    current = {"label": None}

    async def _recording(level, **_kwargs):
        reads.append(level)
        status = ("ok" if status_for is None
                  else status_for(current["label"], level))
        if status != "ok":
            return {"status": status, "items": [], "total": 0,
                    "meta": {"reason": "query_timeout"}}
        return {"status": "ok", "items": list(_FACET_ITEMS.get(level, [])),
                "total": 0, "meta": {"level": level}}

    monkeypatch.setattr(fp, "get_findings_facets_enveloped", _recording)
    set_language(lang)

    live_state = _state() if state is None else dict(state)
    cache: dict = {}
    observed: list = []
    holder: dict = {}

    async def _noop_refresh() -> None:
        return None

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_facet_cascade_probe")) as client:
            with client:
                bar = ui.column().classes(f"fbar {fp.FILTER_BAR_CLASS}")
                with bar:
                    fp._render_facet_groups(lang)
                for label, mutation in rounds:
                    current["label"] = label
                    live_state.update(mutation)
                    del reads[:]
                    await fp._populate_facets(
                        bar, live_state, lang, _noop_refresh, cache=cache)
                    observed.append((label, list(reads)))
        holder["client"] = client

    try:
        asyncio.run(_run())
    finally:
        set_language("he")
    return observed, holder["client"]


def test_a_facet_level_is_re_read_only_when_its_own_request_changes(monkeypatch):
    """The skip rule, in both directions, over one reader's sequence.

    A level is re-read when ITS OWN request arguments move and not otherwise:
    the domain list is offered unfiltered (bucket + candidacy), author narrows
    by domain, work narrows by domain and author. A page turn and a sort change
    move none of them.
    """
    observed, _client = _drive_facet_rounds(monkeypatch, [
        ("first paint", {}),
        ("nothing changed", {}),
        ("page turn", {"page": 3}),
        ("sort change", {"sort": "page_count"}),
        ("domain pick", {"domain": "Liturgy"}),
        ("author pick", {"author": "Maimonides"}),
        ("bucket switch", {"bucket": "more"}),
        ("candidacy on", {"novelty_view": fp.NOVELTY_VIEW_CANDIDATES}),
    ])
    by_label = dict(observed)

    assert by_label["first paint"] == ["domain", "author", "work"], (
        "a cold page must read every level once")
    assert by_label["nothing changed"] == [], (
        "an identical request re-read the cascade — three heavy-budget slots "
        "spent to recompute an answer that provably cannot have changed")
    assert by_label["page turn"] == [], "a page turn re-read the cascade"
    assert by_label["sort change"] == [], "a sort change re-read the cascade"

    assert by_label["domain pick"] == ["author", "work"], (
        "picking a domain must narrow author and work — and must NOT re-read "
        "the domain list, whose own request does not carry the domain")
    assert by_label["author pick"] == ["work"], (
        "picking an author must narrow work, and nothing above it")
    assert by_label["bucket switch"] == ["domain", "author", "work"], (
        "a bucket switch left a facet count describing the OTHER bucket")
    assert by_label["candidacy on"] == ["domain", "author", "work"], (
        "the candidacy filter applies to every level of the cascade")


def test_the_reader_selection_is_marked_even_when_the_read_is_skipped(monkeypatch):
    """The third defect: a node's `.here` treatment is decided when the node is
    BUILT, and picking a domain does not change the domain level's request — so
    a skip that also skipped the RENDER would leave the reader's own selection
    unmarked forever."""
    observed, client = _drive_facet_rounds(monkeypatch, [
        ("first paint", {}),
        ("domain pick", {"domain": "Liturgy"}),
    ])
    assert dict(observed)["domain pick"] == ["author", "work"], (
        "fixture error: the domain level must be SKIPPED here, or this test "
        "cannot be about the render")

    pressed = [
        element for element in _elements_with_class(client, "dnode")
        if (element._props or {}).get("aria-pressed") == "true"
    ]
    assert len(pressed) == 1, (
        "the reader's own domain selection is not marked on any facet node "
        f"({len(pressed)} marked) — the level was re-rendered from the cached "
        "envelope, or it was not re-rendered at all")
    assert "Liturgy" in ((pressed[0]._props or {}).get("label") or "")
    assert "here" in (pressed[0]._classes or [])


def test_the_facet_cards_are_built_once_and_only_their_items_refill(monkeypatch):
    """Re-rendering the CARDS would rebuild the filter bar around the ruling-T
    bucket control, and would drop every card header with it."""
    observed, client = _drive_facet_rounds(monkeypatch, [
        ("first paint", {}),
        ("bucket switch", {"bucket": "more"}),
        ("domain pick", {"domain": "Liturgy"}),
    ])
    assert dict(observed)["bucket switch"], "fixture error: no round re-read"
    for level in ("domain", "author", "work"):
        cards = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}")
        assert len(cards) == 1, (
            f"the {level!r} facet CARD was rebuilt ({len(cards)} present) — only "
            "the items container may refill")
        boxes = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}-items")
        assert len(boxes) == 1


# ---------------------------------------------------------------------------
# TASK 6 (2026-08-05) — the pool card is NAMED and EXPLAINED.
#
# Of the sidebar's five cards, the two carrying the page's AXES (candidacy,
# pool) were the only two with no header, while the three that merely NARROW
# all carried the loud uppercase one. The pool card was therefore two
# unlabelled grey pills in a header-less box — 25,872 identifications reachable
# only by noticing that one of them was not selected.
#
# Ruling T is unaffected and is what bounds the fix: a header, a name and
# explanatory prose are explicitly permitted; a COUNT on the control, or moving
# it into any disclosure, is not. The existing ruling-T tests above still run.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_pool_card_names_its_axis_and_carries_the_rule_that_decides_it(
        monkeypatch, lang):
    client = _render_page(monkeypatch, lang=lang)
    cards = _elements_with_class(client, f"{fp.BUCKET_CONTROL_CLASS}-group")
    assert len(cards) == 1
    card = cards[0]
    assert card.tag == "q-card", "ruling T: the control's card stays a plain card"

    said = "\n".join(_subtree_strings(card))
    assert fp.copy_text("pool_card_header", lang) in said, (
        "the pool card is still the only narrowing-free axis card with no header "
        "— the page shouts its narrowing tools and whispers its axes")
    assert rule_sentence(lang) in said, (
        "the pool card offers two buckets and never says what decides between "
        "them; the sentence that does is buried in a collapsed panel")

    headers = [element for element in card.descendants()
               if fp.CARD_HEADER_CLASS in (element._classes or [])]
    assert len(headers) == 1, (
        f"expected exactly one card header in the pool card, got {len(headers)}")


def test_the_rule_sentence_is_COPIED_into_the_pool_card_and_not_moved(monkeypatch):
    """It must be in BOTH places. `test_the_demoted_prose_is_present_verbatim_
    inside_the_collapsible` pins the panel copy, and this pins that the card did
    not take it away from there."""
    client = _render_page(monkeypatch, lang="en")
    panel = _elements_with_class(client, fp.HOWTO_CLASS)[0]
    card = _elements_with_class(client, f"{fp.BUCKET_CONTROL_CLASS}-group")[0]
    assert rule_sentence("en") in "\n".join(_subtree_strings(panel))
    assert rule_sentence("en") in "\n".join(_subtree_strings(card))
    assert fp.HOWTO_CLASS not in {c for a in _ancestors(card) for c in (a._classes or [])}


def test_the_two_bucket_chips_read_as_one_segment_with_a_visible_selection(monkeypatch):
    """Two loose pills at `gap-2` read as two unrelated buttons; one bordered
    box holding both reads as one control with two states.

    The SELECTED half must also be visible as such. `.fchip` has no base rule in
    the shared CSS block and `.fchip.here` has none at all — only `.chip.here`
    does, which is a different class on a different element — so before this the
    selection was carried by `aria-pressed` and by nothing a sighted reader could
    see. No stylesheet rule is added: the treatment is inline, and every
    directional property in it is logical or side-neutral."""
    client = _render_page(monkeypatch, lang="en")
    segments = _elements_with_class(client, fp.BUCKET_CONTROL_CLASS)
    assert len(segments) == 1
    segment = segments[0]

    chips = [element for element in segment.descendants()
             if "fchip" in (element._classes or [])]
    assert len(chips) == 2, f"both buckets must be named on the control, got {len(chips)}"
    assert {chip.parent_slot.parent for chip in chips} == {segment}, (
        "the two chips do not share one parent — they cannot read as one segment")
    assert (segment._style or {}).get("border"), (
        "the segment carries no enclosing rule, so the two chips still read as "
        "two unrelated buttons")

    selected = [chip for chip in chips
                if (chip._props or {}).get("aria-pressed") == "true"]
    assert len(selected) == 1, "exactly one bucket is active at a time"
    other = [chip for chip in chips if chip not in selected][0]
    assert dict(selected[0]._style or {}) != dict(other._style or {}), (
        "the selected bucket looks exactly like the unselected one — the "
        "selection is announced to a screen reader and to nobody else")

    # Ruling T still holds over the whole card, header and prose included.
    text = "\n".join(_subtree_strings(segment))
    assert not _DIGIT_RE.findall(text), f"a number reached the bucket control: {text!r}"


@pytest.mark.parametrize("start", ["main", "more"])
def test_clicking_the_bucket_control_moves_its_own_selected_state(monkeypatch, start):
    """THE CONTROL WENT STALE, and only a CLICK can see it.

    `_render_bucket_control` returned None and the filter bar collected a
    re-sync callable only from the novelty switch, while the bar itself is built
    ONCE -- so `selected` was evaluated at build time and never again. Clicking
    "more matches" switched the whole result set while the chip kept `here` on
    the main pool and `aria-pressed="true"` stayed on the wrong half: ruling T's
    own control contradicting the result set it had just produced, for sighted
    and screen-reader readers alike.

    A test that renders from initial state PASSES against that defect, which is
    why this one drives the control's OWN registered click listener inside the
    page's event loop and asserts all three announcements moved together.
    """
    other = "more" if start == "main" else "main"
    seen = {}

    async def _drive(client):
        segment = _elements_with_class(client, fp.BUCKET_CONTROL_CLASS)[0]
        chips = [element for element in segment.descendants()
                 if "fchip" in (element._classes or [])]
        assert len(chips) == 2
        pressed = [c for c in chips if (c._props or {}).get("aria-pressed") == "true"]
        assert len(pressed) == 1, "no single bucket is announced as active"
        target = [c for c in chips if c is not pressed[0]][0]

        listeners = [listener for listener in target._event_listeners.values()
                     if listener.type == "click"]
        assert listeners, "the bucket chip is wired to nothing"
        result = listeners[0].handler(None)
        if asyncio.iscoroutine(result):
            await result

        seen["chips"] = chips
        seen["clicked"] = target
        seen["stale"] = pressed[0]

    _render_page(monkeypatch, lang="en", state=_state(bucket=start), driver=_drive)

    clicked, stale = seen["clicked"], seen["stale"]
    assert (clicked._props or {}).get("aria-pressed") == "true", (
        f"clicking {other!r} left aria-pressed on the other half -- a screen "
        "reader is told the page is showing what it is not")
    assert (stale._props or {}).get("aria-pressed") == "false"
    assert "here" in (clicked._classes or []), (
        "the clicked half did not take the selected treatment")
    assert "here" not in (stale._classes or []), (
        "the previously selected half kept its `here` class")
    # The inline segment styling is the only difference a sighted reader sees,
    # and the two styles are not symmetric -- ADDING one over the other leaves a
    # deselected chip still painted as selected.
    assert dict(clicked._style or {}) != dict(stale._style or {})
    assert "background" in dict(clicked._style or {})
    assert "background" not in dict(stale._style or {}), (
        "the deselected half kept the selected background")


# ---------------------------------------------------------------------------
# TASK 7 (2026-08-05) — ACTIVE FILTERS ARE VISIBLE, ADJACENT AND REVERSIBLE.
#
# There was no "you are here" on this page at all. Click a domain and the only
# feedback was a changed row count and one `.here` class 800px away, possibly
# scrolled out of its own 340px box. `/catalog-browse` solves this with
# `render_chips()` + a red `Clear All`; this page adopted that page's CARD
# pattern and not its STATE pattern, and state is the half that produces
# confidence.
#
# THE POOL IS NOT ONE OF THESE CHIPS, and that is a ruling, not an oversight: a
# removable chip implies a neutral "no pool" state, and the service offers
# exactly two buckets with no union between them (`_OFFERED_BUCKETS`).
# ---------------------------------------------------------------------------

_ALL_FILTERS = dict(novelty_view=fp.NOVELTY_VIEW_CANDIDATES, domain="Liturgy",
                    author="Maimonides", work_id=_CURATED_WORK_ID,
                    work_label=_CURATED_RAW_TITLE)


def _chip_texts(client) -> list:
    return [
        "\n".join(_subtree_strings(chip))
        for chip in _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip")
    ]


@pytest.mark.parametrize("lang", ["en", "he"])
def test_every_active_selection_gets_a_removable_chip(monkeypatch, lang):
    from shared.discovery_display_strings import display_work_title

    client = _render_page(monkeypatch, lang=lang, state=_state(**_ALL_FILTERS))
    chips = _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip")
    assert len(chips) == 4, (
        f"four axes are selected and {len(chips)} chip(s) rendered")

    blob = "\n".join(_chip_texts(client))
    assert novelty_strings(lang)["toggle"] in blob, "the candidacy filter has no chip"
    assert "Liturgy" in blob, "the domain filter has no chip"
    assert "Maimonides" in blob, "the author filter has no chip"
    # Ruling R applies here exactly as it does in the facet list: the CURATED
    # display title, never the raw recorded one. Asserted on the WHOLE chip
    # label rather than by substring — in Hebrew the curated title EXTENDS the
    # raw one ("...ספר אהבה" -> "...ספר אהבה / סידור"), so a substring check
    # would pass for the wrong reason in one language and fail in the other.
    set_language(lang)
    try:
        axis = tr_for_test("Work")
    finally:
        set_language("he")
    curated = display_work_title(_CURATED_WORK_ID, _CURATED_RAW_TITLE, lang)
    assert curated != _CURATED_RAW_TITLE, "fixture error: the titles must differ"
    exact = [
        text
        for chip in _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip")
        for text in _subtree_strings(chip)
    ]
    assert f"{axis}: {curated}" in exact, (
        f"the work chip does not use the curated title: {exact!r}")
    assert f"{axis}: {_CURATED_RAW_TITLE}" not in exact, (
        "the RAW recorded work title reached the active-filter chip")

    # Each chip is REMOVABLE, and the removal is wired rather than decorative.
    for chip in chips:
        removes = [element for element in chip.descendants()
                   if f"{fp.ACTIVE_FILTERS_CLASS}-remove" in (element._classes or [])]
        assert len(removes) == 1, "a chip with no remove control is not removable"
        listeners = [listener for listener in removes[0]._event_listeners.values()
                     if listener.type == "click"]
        assert listeners, "the chip's remove control is wired to nothing"

    clears = _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-clear")
    assert len(clears) == 1, "there is no way back to the unfiltered set"


def test_the_chip_bar_is_absent_when_nothing_is_selected(monkeypatch):
    """An empty chip bar is chrome that says nothing; the page starts unfiltered
    and must not open with a bar reserved for a state nobody is in."""
    client = _render_page(monkeypatch, lang="en")
    assert not _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip")
    assert not _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-clear")


@pytest.mark.parametrize("bucket", ["main", "more"])
def test_the_pool_is_never_a_removable_chip(monkeypatch, bucket):
    """A removable chip implies a third, neutral "no pool" state. The service
    offers two buckets and no union between them (`_OFFERED_BUCKETS`), so a chip
    that could be dismissed would promise a view that cannot be produced."""
    from shared.discovery_display_strings import bucket_name

    client = _render_page(monkeypatch, lang="en",
                          state=_state(bucket=bucket, **_ALL_FILTERS))
    blob = "\n".join(_chip_texts(client))
    for in_main in (True, False):
        assert bucket_name(in_main, "en") not in blob, (
            f"the {bucket!r} pool is offered as a removable chip")


def test_the_chip_bar_sits_between_the_result_bar_and_the_rows(monkeypatch):
    """Adjacent to the rows it explains. A chip bar in the sidebar, or below the
    results, is the same information at the distance that made the `.here` class
    useless."""
    client = _render_page(monkeypatch, lang="en", state=_state(**_ALL_FILTERS))
    bars = _elements_with_class(client, fp.ACTIVE_FILTERS_CLASS)
    assert len(bars) == 1
    ancestors = {c for a in _ancestors(bars[0]) for c in (a._classes or [])}
    assert fp.RESULTS_CLASS in ancestors, "the chip bar is not in the results region"
    assert fp.FILTER_BAR_CLASS not in ancestors, "the chip bar drifted into the sidebar"

    region = _elements_with_class(client, fp.RESULTS_CLASS)[0]
    order = [element for element in region.descendants(include_self=True)]
    result_bar = _elements_with_class(client, fp.RESULT_BAR_CLASS)[0]
    rows_column = _elements_with_class(client, f"{fp.RESULTS_CLASS}-rows")[0]
    assert order.index(result_bar) < order.index(bars[0]) < order.index(rows_column), (
        "the chip bar is not between the result bar and the rows")


def test_the_chips_carry_no_physical_directional_property(monkeypatch):
    """`/catalog-browse`'s `_make_chip` uses `ml-1` and `text-left`, which put
    the close button and the label on the wrong side in Hebrew. This page copies
    its SHAPE and not its classes."""
    client = _render_page(monkeypatch, lang="he", state=_state(**_ALL_FILTERS))
    for marker in (f"{fp.ACTIVE_FILTERS_CLASS}-chip",
                   f"{fp.ACTIVE_FILTERS_CLASS}-remove"):
        for element in _elements_with_class(client, marker):
            classes = " ".join(element._classes or [])
            for forbidden in ("ml-", "mr-", "pl-", "pr-", "text-left", "text-right"):
                assert forbidden not in classes, (
                    f"{marker} carries the physical class {forbidden!r}: {classes!r}")
            style = " ".join(f"{k}:{v}" for k, v in (element._style or {}).items())
            for forbidden in ("margin-left", "margin-right", "padding-left",
                              "padding-right", "border-left", "border-right"):
                assert forbidden not in style, (
                    f"{marker} carries the physical property {forbidden!r}: {style!r}")


def test_clearing_one_axis_leaves_the_others_and_returns_to_page_one(monkeypatch):
    """The pure half of the removal, so the behaviour is asserted without a
    browser: a chip clears ITS OWN axis and nothing else, and any filter change
    returns to page 1 (page 4 of the old set is not page 4 of the new one)."""
    state = _state(page=6, **_ALL_FILTERS)
    fp._clear_filter_axis(state, "domain")
    assert state["domain"] is None
    assert state["author"] == "Maimonides"
    assert state["work_id"] == _CURATED_WORK_ID
    assert state["novelty_view"] == fp.NOVELTY_VIEW_CANDIDATES
    assert state["page"] == 1

    fp._clear_filter_axis(state, "work_id")
    assert state["work_id"] is None
    assert state.get("work_label") is None, (
        "the work chip's label outlived the selection it labelled")
    assert state["author"] == "Maimonides"

    fp._clear_filter_axis(state, None)          # clear all
    assert state["author"] is None
    assert state["novelty_view"] == fp.NOVELTY_VIEW_ALL
    assert state["page"] == 1
    # ...and the POOL survives a clear-all: it has no neutral value.
    assert state["bucket"] == "main"
    assert state["unit"] == "identification"
    assert state["sort"] == "band_rank"


# ---------------------------------------------------------------------------
# TASK 8 (2026-08-05) — THE SECOND POOL IS INTRODUCED WHERE THE READER LOOKS.
#
# A body of identifications comparable in size to the one on display was
# reachable only by noticing that one of two unlabelled pills in a header-less
# box was not selected. The control was never the problem — it works, it is one
# interaction, and it switches the whole result set. What it never did was say
# that a second pool exists, what is in it, or why anyone would look.
#
# The strip is a SECOND entry point, not a move: ruling T's control stays in the
# filter bar and every assertion about it above still runs. Creation order is
# now load-bearing (`_find_bucket_control` resolves the FIRST match), which is
# why `test_the_ruling_T_control_is_still_the_first_match_by_that_name` exists.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
@pytest.mark.parametrize("bucket", ["main", "more"])
def test_the_pool_invitation_speaks_in_both_bucket_states(monkeypatch, lang, bucket):
    from shared.discovery_display_strings import (
        TOGGLE_MORE_MATCHES,
        bucket_name,
        disclosure_toggle,
    )

    client = _render_page(monkeypatch, lang=lang, state=_state(bucket=bucket))
    strips = _elements_with_class(client, fp.POOL_INVITE_CLASS)
    assert len(strips) == 1, (
        f"expected exactly one pool-invitation strip, got {len(strips)}")
    said = _subtree_strings(strips[0])
    blob = "\n".join(said)

    if bucket == "main":
        assert fp.copy_text("pool_invite_heading", lang) in said
        assert fp.copy_text("pool_invite_body", lang).format(
            bucket=bucket_name(False, lang)) in said
        assert disclosure_toggle(TOGGLE_MORE_MATCHES, lang) in blob, (
            "the invitation offers no way into the second pool")
    else:
        assert fp.copy_text("pool_here_heading", lang).format(
            bucket=bucket_name(False, lang)) in said
        assert fp.copy_text("pool_here_body", lang).format(
            main_bucket=bucket_name(True, lang)) in said
        assert bucket_name(True, lang) in blob, "there is no way back"

    # ⚠ AMENDED 2026-08-05, and the harness is what keeps it meaningful: this
    # test does NOT stub the launch read, so `meta.more_pool_total` is absent
    # and the DEGRADED (digit-free) sentence is what renders. That is the state
    # asserted here -- an older sidecar, or a launch read that failed -- and it
    # must never print `0`, `None` or a gap where the figure would be. The
    # SIZED state is asserted separately, from a sentinel envelope.
    assert not _DIGIT_RE.findall(blob), f"a number reached the invitation: {blob!r}"
    assert "None" not in blob, f"a missing figure printed as None: {blob!r}"


# ---------------------------------------------------------------------------
# TASK B (2026-08-05) — THE SECOND POOL'S SIZE BECOMES VISIBLE (owner ruling).
#
# The pool carried no number anywhere; the design deliberately did not need one
# and said a figure should be an owner ruling rather than a designer's choice.
# The owner has now ruled: show it. What the ruling does NOT overturn:
#
#   * ruling T — the bucket CONTROL still carries no count
#     (`test_the_two_pool_chips_read_as_one_segment...` still asserts no digit
#     anywhere in that card);
#   * the prohibition on the owner's QUALITY assessment of that pool ever
#     becoming a percentage, a rate, an interval or a score. A SIZE is a
#     different kind of fact;
#   * match framing — never "probably wrong", never "leftovers", never
#     "findings you are missing".
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_invitation_carries_the_second_pools_size_from_the_envelope(
        monkeypatch, lang):
    """A SENTINEL, not a real figure: 818,181 appears in no artifact and in no
    committed file, so its presence in the rendered strip can only mean it
    travelled `meta.more_pool_total` -> render. A real figure asserted here
    would pass equally against a hardcode, which is the defect ruling U
    forbids."""
    from shared.discovery_display_strings import bucket_name

    client = _render_page(
        monkeypatch, lang=lang,
        launch=_fake_launch(meta_extra={"more_pool_total": SENTINEL_MORE_POOL_TOTAL}))
    strip = _elements_with_class(client, fp.POOL_INVITE_CLASS)[0]
    blob = "\n".join(_subtree_strings(strip))

    assert f"{SENTINEL_MORE_POOL_TOTAL:,}" in blob, (
        f"the second pool's size did not reach the invitation: {blob!r}")
    assert fp.copy_text("pool_invite_body_counted", lang).format(
        count=f"{SENTINEL_MORE_POOL_TOTAL:,}", bucket=bucket_name(False, lang)) in blob

    # A SIZE, and nothing that could read as a quality figure.
    for shape in ("%", "percent", "accuracy", "precision", "confidence",
                  "אחוז", "דיוק"):
        assert shape not in blob.lower(), (
            f"the sized invitation carries {shape!r} -- a size is the only "
            "figure this pool may ever show")


def test_the_size_never_reaches_the_bucket_control(monkeypatch):
    """Ruling T, re-asserted WITH the figure available. The digit ban on the
    control is what this whole change had to stay clear of, and the only state
    that could break it is the one where a figure exists to leak."""
    client = _render_page(
        monkeypatch, lang="en",
        launch=_fake_launch(meta_extra={"more_pool_total": SENTINEL_MORE_POOL_TOTAL}))
    segment = _elements_with_class(client, fp.BUCKET_CONTROL_CLASS)[0]
    text = "\n".join(_subtree_strings(segment))
    assert not _DIGIT_RE.findall(text), (
        f"the second pool's size reached the bucket control: {text!r}")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_second_bucket_state_shows_no_figure_even_when_one_is_available(
        monkeypatch, lang):
    """A reader already inside that pool reads its total off the result bar, on
    the same basis as the rows beside it. A second number a few pixels away, on
    a different basis, is how two figures on one screen start disagreeing."""
    client = _render_page(
        monkeypatch, lang=lang, state=_state(bucket="more"),
        launch=_fake_launch(meta_extra={"more_pool_total": SENTINEL_MORE_POOL_TOTAL}))
    blob = "\n".join(_subtree_strings(
        _elements_with_class(client, fp.POOL_INVITE_CLASS)[0]))
    assert f"{SENTINEL_MORE_POOL_TOTAL:,}" not in blob
    assert not _DIGIT_RE.findall(blob), f"a number reached the invitation: {blob!r}"


@pytest.mark.parametrize("launch_kwargs", [
    {"status": "unavailable"},                       # the launch read failed
    {},                                              # an OLDER sidecar: no key
    {"meta_extra": {"more_pool_total": None}},       # present, unmeasured
    {"meta_extra": {"more_pool_total": 0}},          # present, and a zero
    {"meta_extra": {"more_pool_total": "many"}},     # present, and not a number
])
@pytest.mark.parametrize("lang", ["en", "he"])
def test_a_missing_size_degrades_to_the_digit_free_sentence(
        monkeypatch, lang, launch_kwargs):
    """Five ways for the figure to be absent, one rendering. `int(None)` raises,
    `str(None)` prints "None" and `int(x) or 0` prints a zero -- three ways for
    a missing number to become a visible wrong one."""
    from shared.discovery_display_strings import bucket_name

    client = _render_page(monkeypatch, lang=lang, launch=_fake_launch(**launch_kwargs))
    blob = "\n".join(_subtree_strings(
        _elements_with_class(client, fp.POOL_INVITE_CLASS)[0]))

    assert fp.copy_text("pool_invite_body", lang).format(
        bucket=bucket_name(False, lang)) in blob, (
        f"the degraded invitation lost its sentence: {blob!r}")
    assert not _DIGIT_RE.findall(blob), f"a figure was invented: {blob!r}"
    assert "None" not in blob and "many" not in blob


def test_the_rendered_size_equals_the_more_bucket_total_the_findings_query_returns(
        tmp_path, monkeypatch):
    """The cross-check the figure has to survive to be an invitation at all: a
    reader who follows it must land on the number it advertised.

    The two sides are TWO SEPARATELY-WRITTEN QUERIES against one artifact: the
    launch reader's `COUNT(*) ... WHERE main_pool = 0`, and the total the
    corpus-wide FINDINGS query reports for `bucket='more'` -- the very query
    that produces the rows the reader lands on, with its own unit, routing and
    visibility clauses. Comparing the advertised figure against a re-run of its
    own SQL would compare a number with itself.

    THE LANDING SIDE GOES THROUGH THE PAGE'S OWN `fetch_findings`, and that is
    the whole correctness of this guard (2026-08-07). It used to call
    `service.get_findings_enveloped(bucket="more")` directly, which picks up the
    SERVICE's default `divergence=DIVERGENCE_HIDDEN` -- a default NO page path
    uses, because `fetch_findings` pins `DIVERGENCE_SHOWN` unconditionally (the
    four-state novelty selector expresses ruling F's rows through `novelty` now,
    so the old axis must add no predicate). The test therefore measured a view
    the product does not render: it reported a 25,872-vs-16,437 mismatch on the
    real artifact -- exactly the 9,435 divergent rows the unused default
    subtracts -- while the actual page landed on 25,872 and agreed with the card.

    That failure mode is the dangerous direction for a guard: it was RED while
    the product was correct, and it would have stayed GREEN if the page itself
    ever started hiding rows, because it never asked the page anything. Reading
    the reader's number from the reader's own code path is what makes it a check
    on the journey rather than on a query someone rewrote in a test.

    Runs on a SYNTHETIC artifact first, so it holds in every environment, and
    then on the real one when it resolves -- an assertion that only ever skips
    is not a check.
    """
    import asyncio

    from shared.discovery_service import DiscoveryService
    from tests.test_discovery_launch_stats import (
        _EXPECTED_PAGES,
        _POPULATED_ROWS,
        _build_launch_db,
        resolve_guard_artifact,
    )

    def _agree(path, version, monkeypatch):
        service = DiscoveryService(
            path_provider=lambda: path,
            availability_callable=lambda: True,
            sidecar_version_provider=lambda: version,
        )
        envelope = service.get_launch_stats_enveloped()
        assert envelope["status"] == "ok"
        advertised = envelope["meta"]["more_pool_total"]

        # THE READER'S OWN READ. `fp.fetch_findings` is the one path the page
        # uses; pointing its async wrapper at this artifact's service is the
        # smallest substitution that leaves every argument the page passes --
        # unit, bucket, novelty, divergence, sort -- coming from the page.
        async def _enveloped(unit, **kwargs):
            return service.get_findings_enveloped(unit, **kwargs)

        monkeypatch.setattr(fp, "get_findings_enveloped", _enveloped)
        landed = asyncio.run(fp.fetch_findings(_state(bucket="more")))
        assert landed["status"] == "ok"
        assert advertised == landed["total"], (
            f"{path}: the invitation would advertise {advertised} and the "
            f"reader would land on {landed['total']}")
        return advertised

    synthetic = _build_launch_db(
        tmp_path / "invite-cross-check.db", _POPULATED_ROWS, pages=_EXPECTED_PAGES)
    assert _agree(synthetic, "synthetic", monkeypatch) > 0

    path, reason = resolve_guard_artifact()
    if path is None:
        pytest.skip(
            (reason or "no resolvable discovery artifact") +
            " -- the synthetic half of this check ran; set "
            "DISCOVERY_LAUNCH_GUARD_DB to run it against the served artifact")
    assert _agree(path, "real", monkeypatch) > 0


def test_the_page_never_leaves_the_divergence_axis_at_the_service_default():
    """The trap the guard above fell into, pinned at its source.

    `_build_findings_filter` still defaults `divergence=DIVERGENCE_HIDDEN`, which
    was right before the four-state novelty selector replaced that axis and is
    wrong now: the selector expresses ruling F's rows through `novelty` (they are
    shades of one column), so leaving the old axis at its default subtracts them
    a second time and the "do not correspond" view returns nothing at all.

    `fetch_findings` therefore pins `DIVERGENCE_SHOWN` unconditionally. A caller
    that FORGETS the argument silently gets the retired behaviour and no error --
    which is exactly what happened to the cross-check above, on the real artifact,
    for two CI runs.

    Checked over the AST rather than the source text, because the page explains
    this rule in prose directly above the line and a text scan would match its own
    explanation.
    """
    import ast
    import inspect

    tree = ast.parse(inspect.getsource(fp))
    fetch = next(
        (n for n in ast.walk(tree)
         if isinstance(n, ast.AsyncFunctionDef) and n.name == "fetch_findings"),
        None)
    assert fetch is not None, "fetch_findings is gone -- re-point this guard"

    passed = [
        kw for call in ast.walk(fetch) if isinstance(call, ast.Call)
        for kw in call.keywords if kw.arg == "divergence"
    ]
    assert passed, (
        "fetch_findings no longer passes `divergence`, so it inherits the "
        "service default DIVERGENCE_HIDDEN -- which subtracts ruling F's rows a "
        "second time and empties the 'do not correspond' view")
    for kw in passed:
        assert isinstance(kw.value, ast.Name) and kw.value.id == "DIVERGENCE_SHOWN", (
            "fetch_findings passes a divergence other than DIVERGENCE_SHOWN; the "
            "axis must add NO predicate now that novelty expresses those shades")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_invitation_never_calls_the_second_pool_wrong_or_low_quality(
        monkeypatch, lang):
    """Match framing: the second bucket means there was not enough evidence for
    the main-pool rule. It never means the identification is probably wrong, and
    the honesty gate cannot see a quality claim that avoids its lexicon."""
    client = _render_page(monkeypatch, lang=lang, state=_state(bucket="more"))
    blob = "\n".join(_subtree_strings(
        _elements_with_class(client, fp.POOL_INVITE_CLASS)[0])).lower()
    for forbidden in ("probably wrong", "low quality", "poor", "rejected",
                      "leftover", "worth a look", "hidden gem",
                      "כנראה שגוי", "איכות נמוכה", "נדחה"):
        assert forbidden.lower() not in blob, (
            f"the invitation frames the second pool as {forbidden!r}")


def test_the_invitation_sits_between_the_result_bar_and_the_rows(monkeypatch):
    """The one placement ruling T names is "never below the results", and it
    binds this strip as well as the control it complements."""
    client = _render_page(monkeypatch, lang="en")
    region = _elements_with_class(client, fp.RESULTS_CLASS)[0]
    order = list(region.descendants(include_self=True))
    strip = _elements_with_class(client, fp.POOL_INVITE_CLASS)[0]
    result_bar = _elements_with_class(client, fp.RESULT_BAR_CLASS)[0]
    rows_column = _elements_with_class(client, f"{fp.RESULTS_CLASS}-rows")[0]
    assert order.index(result_bar) < order.index(strip) < order.index(rows_column), (
        "the invitation is not between the result bar and the rows")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_ruling_T_control_is_still_the_first_match_by_that_name(monkeypatch, lang):
    """CREATION ORDER IS LOAD-BEARING NOW. `_find_bucket_control` resolves the
    FIRST element carrying the second bucket's accessible name, and every
    ruling-T ancestry assertion is made about whatever it returns. The sidebar
    card is built before the first refresh paints the strip, so it wins — and if
    that ever reverses, those assertions would quietly start describing an
    element in the results region instead."""
    client = _render_page(monkeypatch, lang=lang)
    control = _find_bucket_control(client, lang)
    assert control is not None
    ancestors = {c for a in _ancestors(control) for c in (a._classes or [])}
    assert fp.FILTER_BAR_CLASS in ancestors
    assert fp.POOL_INVITE_CLASS not in ancestors
    assert fp.RESULTS_CLASS not in ancestors


def test_the_invitation_introduces_no_new_button_string(monkeypatch):
    """Both actions come from vocabulary that already exists and was already
    ratified — the D-11 disclosure toggle and the single bucket definition."""
    from shared.discovery_display_strings import (
        TOGGLE_MORE_MATCHES,
        bucket_name,
        disclosure_toggle,
    )

    for bucket, expected in (
        ("main", disclosure_toggle(TOGGLE_MORE_MATCHES, "en")),
        ("more", bucket_name(True, "en")),
    ):
        client = _render_page(monkeypatch, lang="en", state=_state(bucket=bucket))
        actions = _elements_with_class(client, f"{fp.POOL_INVITE_CLASS}-action")
        assert len(actions) == 1
        assert (actions[0]._props or {}).get("label") == expected, (
            f"the {bucket!r} invitation invented a button string: "
            f"{(actions[0]._props or {}).get('label')!r}")


# ---------------------------------------------------------------------------
# TASK 9 (2026-08-05) — the FOURTH state stops being four grey words.
#
# An `ok` envelope with zero rows after three filters is the highest-intent
# moment on the page for meeting the second pool, and it was answered with one
# unstyled label. It must still be VISUALLY AND STRUCTURALLY DISTINCT from the
# three outage states — an outage that reads as "this corpus has no findings"
# silently under-reports the corpus, which is what T-136-16-04 is about.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_an_empty_result_set_offers_the_other_pool(monkeypatch, lang):
    client = _render_page(monkeypatch, lang=lang,
                          findings=_fake_findings({"main": [], "more": []}))
    empties = _elements_with_class(client, f"{fp.RESULTS_CLASS}-empty")
    assert len(empties) == 1, "the honest empty state did not render"
    empty = empties[0]

    set_language(lang)
    try:
        expected = tr_for_test("No results found")
    finally:
        set_language("he")
    said = "\n".join(_subtree_strings(empty))
    assert expected in said, f"the empty state lost its own message: {said!r}"
    assert _elements_with_class(client, f"{fp.RESULTS_CLASS}-empty-icon"), (
        "the empty state is still a bare line of text")

    strips = _elements_with_class(client, fp.POOL_INVITE_CLASS)
    assert len(strips) == 1, (
        f"exactly one pool invitation belongs on the page, got {len(strips)}")
    assert empty in _ancestors(strips[0]), (
        "the empty result set — the moment a reader is most likely to want the "
        "other pool — does not offer it")


@pytest.mark.parametrize("status", ["unavailable", "timeout", "busy"])
def test_the_empty_state_and_the_outage_states_stay_distinguishable(monkeypatch, status):
    """Both directions. An outage dressed as an empty result under-reports the
    corpus; an empty result dressed as an outage tells a reader to retry a query
    that answered correctly."""
    from shared.discovery_display_strings import retry_label, service_state_message

    empty = _render_page(monkeypatch, lang="en",
                         findings=_fake_findings({"main": [], "more": []}))
    outage = _render_page(monkeypatch, lang="en", findings=_fake_findings(status=status))

    # Scoped to the RESULTS region: the launch headline runs its own outage
    # path in this harness (the launch read is not stubbed), so a page-wide
    # scan for a retry label would be red for an unrelated reason.
    empty_region = _scoped_text(empty, fp.RESULTS_CLASS)
    assert service_state_message(status, "en") not in empty_region
    assert retry_label("en") not in empty_region, (
        "the empty state offers a retry — the query answered correctly")
    assert not _elements_with_class(empty, f"{fp.STATE_CLASS}-{status}")

    assert not _elements_with_class(outage, f"{fp.RESULTS_CLASS}-empty"), (
        f"the {status!r} outage rendered the honest empty state")
    assert not _elements_with_class(outage, fp.POOL_INVITE_CLASS), (
        f"the {status!r} outage offered the other pool — the other pool is not "
        "answering either, and the offer would read as a fact about this one")


_EMPTY_FACETS = {"domain": [], "author": [], "work": []}


@pytest.mark.parametrize("lang", ["en", "he"])
def test_an_ok_facet_envelope_with_no_items_says_so_instead_of_rendering_nothing(
        monkeypatch, lang):
    """`_render_facet_items` looped over `items` and, on an empty list, emitted
    ABSOLUTELY NOTHING -- a blank card under a loud uppercase header, which a
    reader cannot tell from a bug. (Reported symptom: an empty WORK card.)

    An empty list is a FACT about the current filters and has to look like one.
    """
    client = _render_page(monkeypatch, lang=lang, facets=_fake_facets(_EMPTY_FACETS))
    for level in ("domain", "author", "work"):
        marks = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}-empty")
        assert marks, (
            f"the {level!r} facet card rendered an empty items container with no "
            "explanation at all")
        assert fp.copy_text("facet_empty", lang) in "\n".join(_subtree_strings(marks[0]))
        box = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}-items")[0]
        assert "".join(_subtree_strings(box)).strip(), (
            f"the {level!r} items container is still visually blank")


def test_the_empty_facet_line_is_not_the_missing_backing_data_treatment(monkeypatch):
    """The two states mean different things and must not borrow each other's
    clothes: `needs_tag` says the DATA to filter on is absent, and the empty
    line says the data is there and the current filters select none of it.

    The amber tag additionally sits on `fg blocked`, which dims the whole card
    and marks the control unusable -- exactly wrong for a card whose control is
    working and simply has nothing to offer right now.
    """
    empty = _render_page(monkeypatch, lang="en", facets=_fake_facets(_EMPTY_FACETS))
    outage = _render_page(monkeypatch, lang="en", facets=_fake_facets(status="unavailable"))

    for level in ("domain", "author", "work"):
        card = _elements_with_class(empty, f"{fp.FILTER_BAR_CLASS}-{level}")[0]
        said = "\n".join(_subtree_strings(card))
        assert fp.copy_text("facet_empty", "en") in said
        assert fp.copy_text("needs_tag", "en") not in said, (
            f"the empty {level!r} list is wearing the 'no backing data' tag")
        classes = {c for el in card.descendants(include_self=True)
                   for c in (el._classes or [])}
        assert "blocked" not in classes, (
            f"the empty {level!r} card is dimmed as unusable — its control works")
        assert not _elements_with_class(
            empty, f"{fp.FILTER_BAR_CLASS}-{level}-blocked")

        # ...and the converse, so neither treatment has swallowed the other.
        assert not _elements_with_class(
            outage, f"{fp.FILTER_BAR_CLASS}-{level}-empty"), (
            f"an OUTAGE on the {level!r} facet read as 'no matches under the "
            "current filters' — it is not a fact about the filters at all")
        assert _elements_with_class(outage, f"{fp.FILTER_BAR_CLASS}-{level}-blocked")


# ---------------------------------------------------------------------------
# TASK D (2026-08-05) — AUTHOR AND WORK BECOME SEARCHABLE.
#
# 47 author buttons and 478 work buttons in a 340px scroll box, with no search
# field: finding a specific work meant scrolling 478 buttons. `/catalog-browse`
# — the page this one was deliberately matched to — already uses
# `ui.select(with_input=True, clearable)` for exactly these two facets. This
# page copied that page's CARD and not its CONTROL.
#
# The DOMAIN facet keeps its tree: it is two levels with counts and a collapse,
# and it reads as navigation rather than as a lookup.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("level", ["author", "work"])
def test_the_author_and_work_facets_are_searchable_selects(monkeypatch, level):
    client = _render_page(monkeypatch, lang="en")
    control = _facet_select(client, level)
    assert control is not None, f"the {level} facet is not a select"
    assert type(control).__name__ == "Select"

    props = control._props or {}
    # `/catalog-browse`'s own props, verbatim -- the search field is
    # `use-input`, and `clearable` is what makes the axis reversible in place.
    assert props.get("use-input") is True, "the select has no search input"
    assert props.get("clearable") is True, "the axis cannot be cleared in place"
    assert props.get("dense") is True and props.get("outlined") is True
    assert str(props.get("input-debounce")) == "300"
    # ...but NOT that page's physical classes, which put the label and the
    # caret on the wrong side in Hebrew.
    classes = " ".join(control._classes or [])
    for physical in ("ml-1", "mr-1", "pl-4", "pr-4", "text-left", "text-right"):
        assert physical not in classes, (
            f"the {level} select copied a PHYSICAL class ({physical}) -- it "
            "breaks in Hebrew")


def test_the_domain_facet_keeps_its_tree(monkeypatch):
    """Only two of the three facets changed control."""
    client = _render_page(monkeypatch, lang="en", facets=_facets_with_domain_tree())
    assert _facet_select(client, "domain") is None, (
        "the domain facet became a select -- its tree, counts and collapse are "
        "what make it navigable")
    assert _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-domain-branch")


@pytest.mark.parametrize("level,state_key,expected_value", [
    ("author", "author", "Maimonides"),
    ("work", "work_id", _CURATED_WORK_ID),
])
def test_an_option_carries_the_STORED_KEY_as_its_value_and_a_count_on_its_label(
        monkeypatch, level, state_key, expected_value):
    """The label is for the reader and the VALUE is for the service. On the work
    level they are not even the same kind of thing -- the value is a
    `w`-prefixed key -- so a control that filtered on its label would filter on
    nothing at all."""
    client = _render_page(monkeypatch, lang="en")
    assert expected_value in _facet_option_values(client, level), (
        f"the {level} option does not carry the stored key the service filters "
        f"on: {_facet_option_values(client, level)!r}")

    # The count comes from the facet envelope, so the number beside an option
    # always agrees with the result set that option produces.
    counts = {item["value"]: item["count"] for item in _FACET_ITEMS[level]}
    labels = _facet_option_labels(client, level)
    assert any(f"({counts[expected_value]:,})" in label for label in labels), (
        f"no facet count on the {level} options: {labels!r}")
    del state_key


@pytest.mark.parametrize("level,state_key,picked", [
    ("author", "author", "Maimonides"),
    ("work", "work_id", _CURATED_WORK_ID),
])
def test_picking_an_option_sets_the_state_resets_the_page_and_refreshes(
        monkeypatch, level, state_key, picked):
    """The SAME three effects the old button node had, through the one refresh
    path -- driven through the select's own registered handler, because a
    control wired to nothing renders identically to one that works."""
    seen = {}

    async def _drive(client):
        control = _facet_select(client, level)
        listeners = [listener for listener in control._event_listeners.values()
                     if listener.type in ("update:model-value", "change")]
        assert listeners or control._props is not None
        # NiceGUI routes `on_change` through the value binding; set the value
        # the way the framework does and let the handler run.
        control.value = picked
        await asyncio.sleep(0)
        seen["client"] = client

    state = _state(page=4)
    captured = {}

    async def _findings(unit="identification", *, bucket="main", sort="band_rank", **kw):
        captured["kwargs"] = kw
        return {"status": "ok", "items": list(_ROWS_BY_BUCKET.get(bucket, [])),
                "total": 1,
                "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                         "sort_basis": "best_band_rank", "novelty_offered": True,
                         "approximate_total": False}}

    monkeypatch.setattr(fp, "read_state", lambda: dict(state))
    written = {}
    monkeypatch.setattr(fp, "write_state", lambda s: written.update(s))
    _render_page(monkeypatch, lang="en", findings=_findings, state=state,
                 driver=_drive)

    assert written.get(state_key) == picked, (
        f"picking a {level} did not set {state_key!r}: {written!r}")
    assert written.get("page") == 1, "a filter change did not return to page 1"
    if level == "work":
        # The RAW recorded label travels with the selection, so the chip bar can
        # name the work without a second lookup (`_KEY_WORK_LABEL`).
        assert written.get("work_label") == _CURATED_RAW_TITLE


@pytest.mark.parametrize("level,state_key,start", [
    ("author", "author", "Maimonides"),
    ("work", "work_id", _CURATED_WORK_ID),
])
def test_clearing_the_select_clears_that_axis(monkeypatch, level, state_key, start):
    """The `clearable` X emits None, and that must be the SAME state change the
    chip bar's own remove control makes -- otherwise the two controls disagree
    about what is applied."""
    async def _drive(client):
        control = _facet_select(client, level)
        assert control.value == start, (
            "the select did not show the persisted selection at all")
        control.value = None
        await asyncio.sleep(0)

    written = {}
    monkeypatch.setattr(fp, "write_state", lambda s: written.update(s))
    _render_page(monkeypatch, lang="en", state=_state(**{state_key: start}),
                 driver=_drive)
    assert written.get(state_key) is None, f"{state_key!r} survived the clear"
    if level == "work":
        assert written.get("work_label") is None


@pytest.mark.parametrize("level,state_key,start", [
    ("author", "author", "Maimonides"),
    ("work", "work_id", _CURATED_WORK_ID),
])
def test_the_select_and_the_chip_show_the_same_axis(monkeypatch, level, state_key, start):
    """Round-trip: what the chip bar says is applied is what the select shows as
    selected. Two displays of one state that can disagree are worse than one.

    The work case carries `work_label` as well, because that is what every
    write site persists alongside `work_id` -- the two are written together by
    `_facet_node._pick` and by the select's own handler. The incoherent state
    (an id with no label) is covered by its own test below, where BOTH surfaces
    say "title unavailable" rather than one of them printing the stored key.
    """
    extra = {"work_label": _CURATED_RAW_TITLE} if level == "work" else {}
    client = _render_page(monkeypatch, lang="en",
                          state=_state(**{state_key: start}, **extra))
    assert _facet_select(client, level).value == start

    chips = _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip")
    said = "\n".join(text for chip in chips for text in _subtree_strings(chip))
    expected = fp.facet_display_label(
        level, {"value": start,
                "label": _CURATED_RAW_TITLE if level == "work" else start}, "en")
    assert expected in said, (
        f"the {level} chip and the {level} select disagree: {said!r}")


@pytest.mark.parametrize("level,state_key,start", [
    ("author", "author", "A Vanished Author"),
    ("work", "work_id", "w000404"),
])
def test_a_selection_the_cascade_no_longer_offers_is_still_shown(
        monkeypatch, level, state_key, start):
    """The facets re-fetch on every refresh, so narrowing another axis can drop
    the selected option out of the returned set. A select whose value is absent
    from its own options renders BLANK while the query is still filtering on it
    -- the filter would then be invisible AND unclearable."""
    client = _render_page(monkeypatch, lang="en", state=_state(**{state_key: start}))
    control = _facet_select(client, level)
    assert start in _facet_option_values(client, level), (
        f"the applied {level} filter vanished from its own control")
    assert control.value == start, "the control shows nothing while filtering"
    # ...and never LABELLED with the stored key. A work's value is a
    # `w`-prefixed id; printing it as a title puts an internal identifier in
    # the control a reader uses to find a work.
    labels = "\n".join(_facet_option_labels(client, level))
    assert "w000404" not in labels, labels
    if level == "work":
        from shared.discovery_display_strings import missing_title
        assert missing_title("en") in labels, (
            "an unlabelled selection rendered as something other than the "
            "shared 'title unavailable' wording")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_chip_and_the_select_agree_about_an_unlabelled_work(monkeypatch, lang):
    """Both route through `facet_display_label`, so neither can print the key
    while the other prints prose."""
    from shared.discovery_display_strings import missing_title

    client = _render_page(monkeypatch, lang=lang, state=_state(work_id="w000404"))
    chips = _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip")
    said = "\n".join(text for chip in chips for text in _subtree_strings(chip))
    assert "w000404" not in said, f"the chip printed the stored key: {said!r}"
    assert missing_title(lang) in said
    assert missing_title(lang) in "\n".join(_facet_option_labels(client, "work"))


@pytest.mark.parametrize("level", ["author", "work"])
@pytest.mark.parametrize("lang", ["en", "he"])
def test_an_empty_facet_still_speaks_rather_than_offering_an_empty_dropdown(
        monkeypatch, level, lang):
    """An empty dropdown is indistinguishable from a broken one, which is the
    same failure the flat empty list had."""
    client = _render_page(monkeypatch, lang=lang, facets=_fake_facets(_EMPTY_FACETS))
    assert _facet_select(client, level) is None, (
        "an empty facet rendered a select with nothing in it")
    marks = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}-empty")
    assert marks and fp.copy_text("facet_empty", lang) in "\n".join(
        _subtree_strings(marks[0]))


@pytest.mark.render_smoke
def test_switching_bucket_replaces_the_facet_lists_as_well_as_the_rows():
    """End to end, through the simulated user: the cascade is part of the ONE
    refresh path, not a one-shot fill after the first paint.

    The domain fixture differs by bucket, so a facet list left over from the
    previous bucket is visible as a stale sentinel rather than only as a missing
    outgoing argument."""
    import httpx
    from nicegui import core, ui
    from nicegui.context import context as _nicegui_context
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.user import User
    from nicegui.ui_run import set_storage_secret

    from shared.discovery_display_strings import bucket_name

    lang = "en"
    saved_slot_stack = list(_nicegui_context.slot_stack)
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()

    async def _run():
        prepare_simulation()
        set_storage_secret("findings-facet-refetch-secret", {})
        with ExitStack() as stack:
            stack.enter_context(patch("web.main.discovery_available", return_value=True))
            stack.enter_context(patch.object(fp, "get_findings_enveloped", _fake_findings()))
            stack.enter_context(patch.object(
                fp, "get_findings_facets_enveloped", _bucket_keyed_facets()))
            stack.enter_context(patch("web.main._resolve_ui_language", return_value=lang))
            _os.environ["NICEGUI_USER_SIMULATION"] = "true"
            set_language(lang)
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(core.app), base_url="http://test"
                    ) as http_client:
                        user = User(http_client)
                        await user.open(FINDINGS_ROUTE)
                        await user.should_see(_FACET_MAIN_SENTINEL)
                        await user.should_not_see(_FACET_MORE_SENTINEL)

                        user.find(kind=ui.button,
                                  content=bucket_name(False, lang)).click()

                        await user.should_see(
                            _FACET_MORE_SENTINEL, retries=_ASYNC_UI_RETRIES
                        )
                        await user.should_not_see(_FACET_MAIN_SENTINEL)
            finally:
                _os.environ.pop("NICEGUI_USER_SIMULATION", None)

    try:
        asyncio.run(_run())
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)
        set_language("he")


_ROW_FILTERED = "ROW-FILTERED-SENTINEL"
_ROW_UNFILTERED = "ROW-UNFILTERED-SENTINEL"


def _filter_keyed_findings():
    """A result set that says, in the RENDER, whether a filter reached the
    query -- so "the chip disappeared" cannot pass for "the filter was
    cleared"."""

    async def _call(unit="identification", *, bucket="main", domain=None,
                    author=None, work_id=None, sort="band_rank", **_kw):
        title = _ROW_FILTERED if (domain or author or work_id) else _ROW_UNFILTERED
        row = dict(_finding_row("w000001", title, "T-S 12.111"), unit=unit)
        return {"status": "ok", "items": [row], "total": 1,
                "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                         "sort_basis": "best_band_rank", "novelty_offered": True,
                         "approximate_total": False}}

    return _call


@pytest.mark.render_smoke
def test_a_chip_removes_its_own_filter_and_clear_all_removes_every_filter():
    """The reversibility half, end to end, through the elements' own listeners.

    Asserting that the chip vanished would be satisfied by a chip wired to
    nothing at all -- so what is asserted is the RESULT SET: the filtered
    sentinel goes away and the unfiltered one comes back, which can only happen
    if the cleared state reached the query."""
    import httpx
    from nicegui import core, ui
    from nicegui.context import context as _nicegui_context
    from nicegui.events import handle_event
    from nicegui.testing.general import prepare_simulation
    from nicegui.testing.user import User
    from nicegui.ui_run import set_storage_secret

    from web.translations import tr

    lang = "en"
    saved_slot_stack = list(_nicegui_context.slot_stack)
    saved_handlers = list(core.app._startup_handlers)
    core.app._startup_handlers.clear()

    async def _run():
        prepare_simulation()
        set_storage_secret("findings-active-filter-secret", {})
        with ExitStack() as stack:
            stack.enter_context(patch("web.main.discovery_available", return_value=True))
            stack.enter_context(patch.object(
                fp, "get_findings_enveloped", _filter_keyed_findings()))
            stack.enter_context(patch.object(
                fp, "get_findings_facets_enveloped", _fake_facets()))
            stack.enter_context(patch("web.main._resolve_ui_language", return_value=lang))
            _os.environ["NICEGUI_USER_SIMULATION"] = "true"
            set_language(lang)
            try:
                async with core.app.router.lifespan_context(core.app):
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(core.app), base_url="http://test"
                    ) as http_client:
                        user = User(http_client)
                        await user.open(FINDINGS_ROUTE)
                        await user.should_see(_ROW_UNFILTERED)

                        # (1) pick a domain from the facet list.
                        user.find(kind=ui.button, content="Liturgy").click()
                        await user.should_see(
                            _ROW_FILTERED, retries=_ASYNC_UI_RETRIES
                        )
                        await user.should_not_see(_ROW_UNFILTERED)

                        # (2) remove it through the CHIP's own listener.
                        with user.client:
                            removes = [
                                element for element in user.client.elements.values()
                                if f"{fp.ACTIVE_FILTERS_CLASS}-remove"
                                in (element._classes or [])
                            ]
                            assert len(removes) == 1, (
                                f"expected one active-filter chip, got {len(removes)}")
                            listener = next(
                                listener for listener
                                in removes[0]._event_listeners.values()
                                if listener.type == "click")
                            handle_event(listener.handler, None)
                        await user.should_see(
                            _ROW_UNFILTERED, retries=_ASYNC_UI_RETRIES
                        )
                        await user.should_not_see(_ROW_FILTERED)

                        # (3) pick it again, then use Clear all.
                        user.find(kind=ui.button, content="Liturgy").click()
                        await user.should_see(
                            _ROW_FILTERED, retries=_ASYNC_UI_RETRIES
                        )
                        user.find(kind=ui.button, content=tr("Clear All")).click()
                        await user.should_see(
                            _ROW_UNFILTERED, retries=_ASYNC_UI_RETRIES
                        )
                        await user.should_not_see(_ROW_FILTERED)
            finally:
                _os.environ.pop("NICEGUI_USER_SIMULATION", None)

    try:
        asyncio.run(_run())
    finally:
        core.app._startup_handlers.clear()
        core.app._startup_handlers.extend(saved_handlers)
        _nicegui_context.slot_stack.clear()
        _nicegui_context.slot_stack.extend(saved_slot_stack)
        set_language("he")


# ---------------------------------------------------------------------------
# RULING F — THE DIVERGENCE AXIS (136-GATE1-DECISIONS.md section F).
#
# 12,664 of 53,581 identifications (23.6%) contradict a catalogue
# identification, 3,229 of them in the main pool. The policy that hides them by
# default has existed since plan 136-04 as `HIDDEN_BY_DEFAULT_SHADES` and
# `is_hidden_by_default`, and until this axis shipped NOTHING outside `tests/`
# called either -- so the constant had tests and the behaviour had none.
#
# EVERY TEST BELOW ASSERTS A PROPERTY OF THE PAGE, never the value of a
# constant. That distinction is this phase's signature defect: a test reading
# `HIDDEN_BY_DEFAULT_SHADES == {...}` passed for the whole period during which
# no surface honoured it.
# ---------------------------------------------------------------------------

def _recording_findings(recorder, *, meta_extra=None):
    """A findings stub that records the full request each call carried.

    `divergent_included` defaults to matching the DEFAULT `divergence` this
    stub echoes (`hidden` -> excluded), so a caller that does not care about
    the reconciliation line gets a self-consistent envelope for free; a caller
    that DOES care overrides it via `meta_extra`.
    """
    async def _call(unit="identification", **kwargs):
        recorder.append({"unit": unit, **kwargs})
        bucket = kwargs.get("bucket", "main")
        divergence = kwargs.get("divergence", "hidden")
        meta = {"unit": unit, "bucket": bucket,
                "sort": kwargs.get("sort", "band_rank"),
                "sort_basis": "best_band_rank", "novelty_offered": True,
                "divergence": divergence,
                "divergent_included": divergence != fp.DIVERGENCE_HIDDEN,
                "approximate_total": False}
        meta.update(meta_extra or {})
        return {
            "status": "ok", "items": list(_ROWS_BY_BUCKET.get(bucket, [])),
            "total": 1,
            "meta": meta,
        }
    return _call


def _recording_facets(recorder):
    async def _call(level, **kwargs):
        recorder.append({"level": level, **kwargs})
        return {"status": "ok", "items": list(_FACET_ITEMS.get(level, [])),
                "total": 0, "meta": {"level": level}}
    return _call


def _divergence_card(client):
    """The card the four-state selector lives in (owner ruling, 2026-08-06):
    ONE control now, where this card used to hold a candidacy switch AND a
    separate divergence chip. Resolved by the card's own marker class, which
    the selector shares with nothing else."""
    cards = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-novelty")
    assert len(cards) == 1, f"expected one novelty card, got {len(cards)}"
    return cards[0]


def _divergence_switch(client):
    """The four-state SELECT itself, by its own marker class.

    Named `_divergence_switch` (not renamed) because every remaining caller
    below is asking a question that used to be about the retired divergence
    chip and is now about the one control that replaced it; giving the same
    accessor a new name at every call site would be cosmetic churn with no
    behavioural difference."""
    selects = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-novelty-view")
    assert len(selects) == 1, f"expected one novelty-view select, got {len(selects)}"
    return selects[0]


def _divergence_label(client):
    """The selector's CURRENTLY CHOSEN option, as reader-facing text -- the
    equivalent of what the retired chip's own changing `label` prop used to
    say. The select's `label` PROP is now a fixed caption ("Show"/"הצגה") that
    never varies with the choice (see `_render_novelty_switch`'s own note on
    the wire encoding), so the state has to be read from `.value` mapped
    through `.options` instead."""
    select = _divergence_switch(client)
    options = select.options
    if isinstance(options, dict):
        return options.get(select.value)
    return select.value


async def _click_and_settle(element, *, until, budget=60):
    """Click through the element's OWN registered listener, then wait for the
    effect.

    `Button.on_click` wraps the handler in a lambda that returns None and
    schedules the coroutine ITSELF, so invoking the listener does not run the
    refresh -- a test that asserted immediately afterwards would be asserting
    about the state before the click and would pass for the wrong reason. This
    yields until `until()` reports the effect landed, and fails loudly rather
    than silently proceeding if it never does."""
    listeners = [listener for listener in element._event_listeners.values()
                 if listener.type == "click"]
    assert listeners, "the control is wired to nothing"
    result = listeners[0].handler(None)
    if asyncio.iscoroutine(result):
        await result
    for _ in range(budget):
        if until():
            return
        await asyncio.sleep(0)
    raise AssertionError("the click never produced its effect")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_divergence_axis_is_off_by_default_and_the_query_is_told_so(monkeypatch, lang):
    """The LOAD-BEARING one for ruling F on this surface -- INVERTED, on
    purpose, by the owner's own redesign (2026-08-06).

    Under the retired two-control design this test's job was to prove the
    divergent rows were HIDDEN on a cold load. That is no longer what a cold
    load does: the default view is `all`, which applies no novelty predicate
    at all, and the page hard-codes `divergence=SHOWN` on every call site now
    that the axis is expressed as a novelty shade rather than a separate
    parameter -- so a cold load asks the service for EVERYTHING, divergent
    rows included. Pinning the old "hidden by default" behaviour here would
    pin a property the owner deliberately removed, not one that survived.

    What still must hold: the query and the render AGREE about this, in both
    directions -- `fetch_findings` really does send `divergence=SHOWN` and
    `novelty=None`, and the result bar's own reconciliation line says the
    count includes the divergent rows rather than staying silent or claiming
    the old exclusion.
    """
    findings, facets = [], []
    client = _render_page(
        monkeypatch, lang=lang,
        findings=_recording_findings(findings),
        facets=_recording_facets(facets))
    assert findings, "the page issued no findings read"
    assert all(call.get("divergence") == fp.DIVERGENCE_SHOWN
               for call in findings), (
        "the default load no longer asks for the divergent rows unconditionally")
    assert all(call.get("novelty") is None for call in findings), (
        "the default view applied a novelty predicate nobody selected")
    assert facets, "the page issued no facet read"
    assert all(call.get("divergence") == fp.DIVERGENCE_SHOWN
               for call in facets), (
        "a facet count described a population the result set does not")

    select = _divergence_switch(client)
    assert select.value == fp.NOVELTY_VIEW_ALL, (
        "the selector does not open on the ALL view")
    assert not _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip"), (
        "the default view is not a filtered one and must open with no chip bar")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_divergence_card_is_a_first_class_card_in_the_sidebar(monkeypatch, lang):
    """The same shape as the pool card, and in the same column: a real
    `q-card`, carrying `fg`, inside the filter bar -- never an overflow menu, a
    disclosure or a footnote.

    UNCHANGED by the 2026-08-06 redesign: the card is still built by
    `_filter_card("novgrp", f"{FILTER_BAR_CLASS}-novelty")`, so `_divergence_card`
    (fixed to resolve that same marker class rather than the retired chip's)
    still finds a real `q-card` in the same place. The property this test pins
    -- card shape and position, not which control lives inside it -- survived
    unchanged."""
    client = _render_page(monkeypatch, lang=lang)
    card = _divergence_card(client)
    assert card.tag == "q-card"
    assert "fg" in (card._classes or [])
    assert fp.CARD_CLASS in (card._classes or [])
    ancestors = {c for a in _ancestors(card) for c in (a._classes or [])}
    assert fp.FILTER_BAR_CLASS in ancestors
    assert fp.RESULTS_CLASS not in ancestors

    tags = {(getattr(a, "tag", "") or "").lower() for a in _ancestors(card)}
    assert "nicegui-expansion" not in tags and "q-expansion-item" not in tags
    assert "details" not in tags


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_divergence_card_carries_the_ratified_warning_as_prose(
        monkeypatch, lang):
    """Ruling F's control is an EXPLICITLY WARNED one, and the warning is card
    PROSE rather than a tooltip: a warning a reader must hover to find is not
    one they were given before they chose.

    UNCHANGED by the 2026-08-06 redesign: `_render_novelty_switch` still renders
    `divergence_warning(lang)` as a `ui.label` with the same
    `f"{FILTER_BAR_CLASS}-divergence-warning"` class, inside the same card, so
    this property survived the switch from two controls to one untouched."""
    from shared.discovery_display_strings import divergence_warning

    client = _render_page(monkeypatch, lang=lang)
    card_text = "\n".join(_subtree_strings(_divergence_card(client)))
    assert divergence_warning(lang) in card_text

    warnings = _elements_with_class(
        client, f"{fp.FILTER_BAR_CLASS}-divergence-warning")
    assert len(warnings) == 1, "the warning is not a rendered element of the card"


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_findings_control_labels_its_STATE_not_the_panels_disclosure(
        monkeypatch, lang):
    """The correction the owner asked for, as an assertion.

    `TOGGLE_DIVERGENCE` -- "Show findings that disagree with the catalogue" --
    was ratified for the PANEL, where it used to label D-13e's fourth
    disclosure: a section that opened onto divergent rows ONLY, which the
    string described exactly. On THIS page the same string sat on a purely
    additive filter that widened a mixed set and kept every non-divergent row,
    so it described a mechanic the page does not have. The owner read the
    control as broken and then named the reason: "so why 'show that disagree'
    shows also those who do not disagree?".

    So the findings page labels its STATES, and the panel kept its own string
    -- until the owner's 2026-08-13 amendment retired the panel's
    disclosure-based divergence wording ENTIRELY (`TOGGLE_DIVERGENCE` and
    `LEVEL_DIVERGENCE` are both deleted from `shared/discovery_panel_model.py`
    / `shared/discovery_display_strings.py`; divergence is a per-ROW chip
    there now, `ds.divergence_chip`, never a disclosure toggle's label). There
    is therefore nothing left of the OLD panel string to check for leakage
    against -- the two controls that string used to distinguish no longer
    both exist. What survives, and what this test still pins, is the
    findings-page selector's own closed vocabulary: its current choice is one
    of its own four labels, and (the surviving half of the original spirit)
    none of those four collide with the panel's own live divergence string,
    the row chip.

    The two controls the owner was originally reacting to (a candidacy switch
    and a cycling divergence chip) were replaced by ONE four-state selector
    (owner ruling, 2026-08-06); that redesign is what this test was already
    translated against before the 2026-08-13 amendment retired the panel side
    of the comparison.
    """
    client = _render_page(monkeypatch, lang=lang)

    option_labels = set(fp._novelty_view_options(lang).values())
    assert _divergence_label(client) in option_labels, (
        f"the selector's current choice {_divergence_label(client)!r} is not "
        f"one of its own four state labels {option_labels!r}")

    # The panel's own SURVIVING divergence string (the per-row chip) is a
    # different mechanic again -- a marker on an identification, not a filter
    # state -- so the findings page's own four labels must not collide with it
    # either.
    from shared.discovery_display_strings import divergence_chip

    assert divergence_chip(lang) not in option_labels, (
        "the panel's row-chip wording leaked into the findings selector's own "
        "option set")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_divergence_copy_never_says_which_side_is_right(monkeypatch, lang):
    """Ruling F: the system never treats the catalogue's disagreement as a
    verdict, and ruling L leaves `divergence_correctness` NULL on every shipped
    row -- so no adjudication exists anywhere for this card to imply."""
    client = _render_page(monkeypatch, lang=lang)
    # SCOPED TO THIS AXIS'S OWN COPY. The card is shared with the candidacy
    # control now, and sweeping the whole card would put the candidacy help text
    # -- which is not this axis's to answer for -- inside an assertion about it.
    text = "\n".join(
        _node_texts(client, f"{fp.FILTER_BAR_CLASS}-divergence-warning")
        + _node_texts(client, f"{fp.FILTER_BAR_CLASS}-divergence-note")
        # The three `divergence_state_*` keys are GONE (2026-08-06): they
        # labelled the retired cycling control, their table was validated but
        # never indexed, and they carried a third vocabulary ("Catalogue
        # disagreements") for a fact the selector and the row chip already
        # state. `copy_text` raises on an unknown key by design, so this list
        # cannot silently outlive the strings.
        + [fp.copy_text(key, lang) for key in (
            "divergence_candidacy_note",
            "divergence_excluded", "divergence_included",
            "divergence_alone")]).lower()
    forbidden = {
        "en": ("wrong", "incorrect", "mistaken", "error", "probably", "likely",
               "false positive", "unreliable"),
        "he": ("שגוי", "טעות", "מוטעה", "כנראה", "לא אמין"),
    }[lang]
    for word in forbidden:
        assert word not in text, f"the divergence card adjudicates: {word!r}"
    assert not _DIGIT_RE.search(text), (
        "the divergence card carries a figure -- this axis names a category, "
        "and a count on it is a claim about that pool nobody ruled it may make")


def test_adding_the_divergence_card_left_ruling_t_and_the_candidacy_order_intact(
        monkeypatch):
    """Ruling T's control must stay a first-class, always-rendered card that
    switches the set in ONE interaction, and `.fg.novgrp {order:-1}` must still
    put the candidacy card first.

    The one-card-holds-both-controls assertion below now describes the ONLY
    card ever rendered for this axis (owner ruling, 2026-08-06 retired the
    second, separate divergence chip entirely) rather than a merge of two
    previously-independent cards -- but the property under test (exactly one
    `novgrp` card, and it is the same element `_divergence_card` resolves) is
    unchanged, so this stays a TRANSLATE against the fixed helper."""
    client = _render_page(monkeypatch, lang="en")

    control = _find_bucket_control(client, "en")
    assert control is not None
    ancestor_classes = {c for a in _ancestors(control) for c in (a._classes or [])}
    assert fp.FILTER_BAR_CLASS in ancestor_classes
    assert fp.RESULTS_CLASS not in ancestor_classes
    tags = {(getattr(a, "tag", "") or "").lower() for a in _ancestors(control)}
    assert "nicegui-expansion" not in tags and "q-expansion-item" not in tags

    novelty = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-novelty")
    assert len(novelty) == 1
    assert "novgrp" in (novelty[0]._classes or []), (
        "the candidacy card lost the class the `order:-1` rule is keyed on")
    # ONE CARD holds both controls now (owner, 2026-08-05): they select on the
    # SAME column, so two cards read as independent filters and are not. The
    # merged card must still be the one `.fg.novgrp {order:-1}` puts first.
    assert _divergence_card(client) is novelty[0], (
        "the two controls are in separate cards again")
    novgrp = [element for element in client.elements.values()
              if "novgrp" in (element._classes or [])]
    assert len(novgrp) == 1, (
        f"{len(novgrp)} cards claim the first position `order:-1` gives")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_clicking_cycles_hidden_then_shown_then_only_and_the_query_follows(
        monkeypatch, lang):
    """ALL FOUR STATES, driven through the select's OWN value-change path --
    the same mechanism `test_picking_an_option_sets_the_state_resets_the_page_
    and_refreshes` already uses for the facet selects: setting `.value` the way
    the framework does on a real client pick, then yielding for the coroutine
    handler `handle_event` schedules as a background task.

    RETIRED MECHANISM, TRANSLATED (owner ruling, 2026-08-06). The owner's
    complaint was about a chip that CYCLED through three states by repeated
    clicking, one of which was purely additive (a list sorted by relevance
    looked identical whether the divergent rows were merely ADDED to a set
    that already had room for them). Clicking-a-chip became picking-a-select,
    and the three states became four -- but the property this test exists to
    pin is unchanged: EVERY reachable state must produce a genuinely
    different, query-visible request, the facet cascade must describe the
    SAME population the rows do, and a state change must return the reader
    to page 1.

    `divergence` itself is asserted PINNED to SHOWN throughout: ruling F's
    rows are now selected through `novelty`, not through this axis, so a
    request that let `divergence` vary again would silently re-subtract them
    under the "divergent" and "either" views -- the same class of silent
    under-count the old chip's ON state was accused of.
    """
    findings, facets = [], []
    observed = []

    async def _drive(client):
        select = _divergence_switch(client)
        for view in (fp.NOVELTY_VIEW_CANDIDATES, fp.NOVELTY_VIEW_DIVERGENT,
                     fp.NOVELTY_VIEW_EITHER, fp.NOVELTY_VIEW_ALL):
            del findings[:]
            del facets[:]
            select.value = view
            for _ in range(60):
                if findings and facets:
                    break
                await asyncio.sleep(0)
            else:
                raise AssertionError(f"picking {view!r} never produced a request")
            observed.append({
                "view": view,
                "novelty": findings[-1].get("novelty"),
                "divergence": findings[-1].get("divergence"),
                "facets_novelty": [call.get("novelty") for call in facets],
                "facets_divergence": [call.get("divergence") for call in facets],
                "page": findings[-1].get("page"),
                "select_value": select.value,
            })

    _render_page(monkeypatch, lang=lang,
                 findings=_recording_findings(findings),
                 facets=_recording_facets(facets), driver=_drive)

    assert [round_["view"] for round_ in observed] == [
        fp.NOVELTY_VIEW_CANDIDATES, fp.NOVELTY_VIEW_DIVERGENT,
        fp.NOVELTY_VIEW_EITHER, fp.NOVELTY_VIEW_ALL], (
        "the selector does not offer all four states in the expected order")

    for round_ in observed:
        expected_shades = fp.novelty_view_shades(round_["view"])
        assert round_["novelty"] == expected_shades, (
            f"{round_['view']!r} asked the service for novelty={round_['novelty']!r}, "
            f"not its own shades {expected_shades!r}")
        assert round_["divergence"] == fp.DIVERGENCE_SHOWN, (
            f"{round_['view']!r} sent divergence={round_['divergence']!r} -- the "
            "axis must stay pinned SHOWN so the novelty predicate is the only "
            "thing narrowing the set")
        assert round_["facets_novelty"], "a state change did not re-read the cascade"
        assert all(shades == expected_shades for shades in round_["facets_novelty"]), (
            "the counts beside the filters describe a different population "
            "than the rows do")
        assert all(mode == fp.DIVERGENCE_SHOWN
                   for mode in round_["facets_divergence"]), (
            "a facet count silently narrowed on the retired axis")
        assert round_["page"] == 1, (
            "a state change kept the reader's page -- page 4 of the old set is "
            "not page 4 of the new one")
        assert round_["select_value"] == round_["view"], (
            "the selector's own displayed value did not follow the pick")


def test_the_divergence_axis_composes_as_an_and_with_every_other_axis(monkeypatch):
    """Like every other axis on this page, and the composition is the point: a
    reader who has narrowed to a domain and an author must be able to ask
    'and show me the ones that disagree' rather than being returned to the
    whole corpus.

    TRANSLATED against the selector (owner ruling, 2026-08-06): the "show only
    the divergent" state is now `NOVELTY_VIEW_DIVERGENT`, expressed to the
    service through `novelty` rather than through `divergence` (which the page
    now pins to SHOWN unconditionally). What survives unchanged is the
    property under test -- domain, author and work_id keep narrowing the set
    exactly as they do under every other view."""
    findings, facets = [], []
    _render_page(monkeypatch, lang="en",
                 findings=_recording_findings(findings),
                 facets=_recording_facets(facets),
                 state=_state(novelty_view=fp.NOVELTY_VIEW_DIVERGENT, bucket="more",
                              domain="Liturgy", author="Maimonides",
                              work_id=_CURATED_WORK_ID,
                              work_label=_CURATED_RAW_TITLE))
    assert findings
    call = findings[-1]
    assert call["novelty"] == fp.novelty_view_shades(fp.NOVELTY_VIEW_DIVERGENT)
    # PINNED SHOWN, not ONLY: the page no longer expresses this axis through
    # `divergence` at all -- a fixture asserting the old value here would pin
    # a parameter the current code never sends.
    assert call["divergence"] == fp.DIVERGENCE_SHOWN
    assert call["bucket"] == "more"
    assert call["domain"] == "Liturgy"
    assert call["author"] == "Maimonides"
    assert call["work_id"] == _CURATED_WORK_ID
    # The cascade carries it too, or the counts describe another population.
    assert facets and all(
        c["novelty"] == fp.novelty_view_shades(fp.NOVELTY_VIEW_DIVERGENT)
        for c in facets)
    assert all(c["divergence"] == fp.DIVERGENCE_SHOWN for c in facets)


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_open_divergence_axis_appears_in_the_active_filter_chip_bar(
        monkeypatch, lang):
    """It changes ~23.6% of the corpus; leaving it out of "why am I looking at
    these rows" would make it the one selection with no answer there.

    TRANSLATED against the selector (owner ruling, 2026-08-06): the axis is
    now three non-default views rather than two non-default `divergence`
    modes (`only` folded into `divergent`, and `either` is new), each still
    producing exactly one chip carrying the control's OWN option label --
    `_active_filter_chips` builds the label from `_novelty_view_options`, the
    same dict the selector itself renders from, so the two still say the same
    thing about the same axis."""
    for view in (fp.NOVELTY_VIEW_CANDIDATES, fp.NOVELTY_VIEW_DIVERGENT,
                 fp.NOVELTY_VIEW_EITHER):
        client = _render_page(monkeypatch, lang=lang, state=_state(novelty_view=view))
        chips = _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip")
        assert len(chips) == 1, f"{view}: expected one chip, got {len(chips)}"
        texts = [text for chip in chips for text in _subtree_strings(chip)]
        assert fp._novelty_view_options(lang)[view] in texts, (
            f"{view}: the chip does not name the state: {texts!r}")

    client = _render_page(monkeypatch, lang=lang,
                          state=_state(novelty_view=fp.NOVELTY_VIEW_DIVERGENT))
    chips = _elements_with_class(client, f"{fp.ACTIVE_FILTERS_CLASS}-chip")

    removes = [element for element in chips[0].descendants()
               if f"{fp.ACTIVE_FILTERS_CLASS}-remove" in (element._classes or [])]
    assert len(removes) == 1
    assert [listener for listener in removes[0]._event_listeners.values()
            if listener.type == "click"], "the chip's remove control is inert"


def test_removing_the_chip_and_clearing_all_both_return_the_axis_to_hidden():
    """PURE, over the shared axis-clearing rule.

    TRANSLATED (owner ruling, 2026-08-06): the neutral value this axis returns
    to is no longer `DIVERGENCE_HIDDEN` on a `divergence` key -- that key is
    pinned SHOWN unconditionally now and carries no reader intent -- it is
    `NOVELTY_VIEW_ALL` on `novelty_view`, the selector's own default and
    unfiltered state. The property is the same: this axis has a neutral value
    to return to (unlike the pool), so it may be a removable chip, and clearing
    it must not disturb any other axis."""
    state = _state(novelty_view=fp.NOVELTY_VIEW_DIVERGENT, domain="Liturgy")
    fp._clear_filter_axis(state, "novelty_view")
    assert state["novelty_view"] == fp.NOVELTY_VIEW_ALL, (
        "the axis cleared to a value outside its closed vocabulary -- the "
        "service refuses anything that is not a view")
    assert state["domain"] == "Liturgy", "clearing one axis cleared another"
    assert state["page"] == 1

    state = _state(novelty_view=fp.NOVELTY_VIEW_CANDIDATES, domain="Liturgy")
    fp._clear_filter_axis(state, None)
    assert state["novelty_view"] == fp.NOVELTY_VIEW_ALL
    assert state["domain"] is None
    assert "novelty_view" in fp._CHIP_AXES


def test_the_divergence_selection_persists_through_the_storage_chokepoint(monkeypatch):
    """Through `web/safe_storage.py` like every other selection (Phase 87), and
    DEFAULT to the WIDEST view at every read.

    TRANSLATED (owner ruling, 2026-08-06): there is one storage key now
    (`_KEY_NOVELTY_VIEW`), validated against `NOVELTY_VIEWS` rather than
    `DIVERGENCE_MODES`, and the fail-open default is `all` (show everything)
    rather than `hidden` (ruling F's old narrowest-by-default). No boolean
    migration remains to test: this axis never shipped as a boolean under the
    selector design, only under the retired chip -- `test_normalise_state_
    drops_a_persisted_pair_the_service_would_refuse` covers the one migration
    this design actually has (a stale `_RETIRED_KEY_DIVERGENCE`/`_RETIRED_KEY_
    NOVELTY` pair from a reader who loaded the page before this release)."""
    store = {}
    monkeypatch.setattr(fp, "safe_user_set",
                        lambda key, value: store.__setitem__(key, value))
    monkeypatch.setattr(fp, "safe_user_get",
                        lambda key, default=None: store.get(key, default))

    for view in fp.NOVELTY_VIEWS:
        fp.write_state(_state(novelty_view=view))
        assert store[fp._KEY_NOVELTY_VIEW] == view
        assert fp.read_state()["novelty_view"] == view

    store.clear()
    assert fp.read_state()["novelty_view"] == fp.NOVELTY_VIEW_ALL, (
        "an absent key must read as the selector's own widest default")
    for junk in (0, None, "include", "", True, "shown", "divergence", 3):
        store[fp._KEY_NOVELTY_VIEW] = junk
        assert fp.read_state()["novelty_view"] == fp.NOVELTY_VIEW_ALL, junk


# ---------------------------------------------------------------------------
# THE HEADLINE / RESULT-BAR RECONCILIATION (ruling F x ruling U).
#
# The headline reports what the RELEASE contains -- every identification in the
# artifact, on the single basis ruling U fixed. The default view shows ~23.6%
# fewer rows, because ruling F hides the catalogue-divergent ones. A reader who
# counts finds the gap.
#
# The headline was deliberately NOT made to track the reader's filters: a
# corpus figure that moves when a toggle moves stops being a corpus figure.
# The figure that MOVES is the one that explains itself, beside the rows it
# describes -- and it is read from the ENVELOPE, so it follows the query rather
# than the page's intent.
# ---------------------------------------------------------------------------

def _basis_texts(client) -> set:
    """The DISTINCT strings the reconciliation element renders.

    A set, because `_subtree_strings` reads both `text` and `_text` off the
    same node and would otherwise report one label twice."""
    return set(_node_texts(client, f"{fp.RESULT_BAR_CLASS}-divergence"))


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_result_bar_states_whether_the_count_leaves_the_divergent_rows_out(
        monkeypatch, lang):
    """All three directions the line can now say something in. A line that
    appeared only while something was excluded would leave the wider view
    unexplained and the two figures disagreeing again.

    TRANSLATED (owner ruling, 2026-08-06): `_render_divergence_basis` reads
    `meta['divergent_included']` (a plain boolean the service derives from
    whatever predicate it actually ran), not `meta['divergence']` -- that key
    is pinned `shown` on every request now and would report "included" on
    every render, which is the exact defect this line exists to prevent.
    There is also a THIRD wording now: `divergence_alone`, which fires only
    when the reader's own selected view is `divergent` -- "included" would be
    technically true but would undersell a set that is ENTIRELY the
    divergent rows, so the control's own selection decides which of the two
    truthy wordings applies."""
    excluded = _render_page(
        monkeypatch, lang=lang,
        findings=_recording_findings([], meta_extra={"divergent_included": False}))
    assert _basis_texts(excluded) == {fp.copy_text("divergence_excluded", lang)}, (
        "an envelope that excluded the divergent rows says nothing about it "
        "beside the count")

    included = _render_page(
        monkeypatch, lang=lang, state=_state(novelty_view=fp.NOVELTY_VIEW_ALL),
        findings=_recording_findings([], meta_extra={"divergent_included": True}))
    assert _basis_texts(included) == {fp.copy_text("divergence_included", lang)}

    alone = _render_page(
        monkeypatch, lang=lang, state=_state(novelty_view=fp.NOVELTY_VIEW_DIVERGENT),
        findings=_recording_findings([], meta_extra={"divergent_included": True}))
    assert _basis_texts(alone) == {fp.copy_text("divergence_alone", lang)}, (
        "a set that is ENTIRELY the divergent rows used the plain 'included' "
        "wording instead of naming that it is the whole set")


def test_the_reconciliation_line_follows_the_QUERY_not_the_pages_intent(monkeypatch):
    """`meta['divergent_included']` is the SERVICE's report of what it ran; the
    state's selected view is only what the page meant to ask for. If a control
    failed to persist or a wrapper dropped an argument, the line must follow
    the ROWS -- the half the reader is counting.

    TRANSLATED (owner ruling, 2026-08-06): the disagreement scenario is now
    between the reader's selected view (`divergent`, which the page believes
    means "only the divergent rows, so say so") and the envelope's own
    `divergent_included` (which says the query it actually ran EXCLUDED
    them) -- the inverse combination of what the old `divergence`-based
    version pinned, but the same property: the line sides with the envelope,
    not the state."""
    client = _render_page(
        monkeypatch, lang="en", state=_state(novelty_view=fp.NOVELTY_VIEW_DIVERGENT),
        findings=_recording_findings([], meta_extra={"divergent_included": False}))
    assert _basis_texts(client) == {fp.copy_text("divergence_excluded", "en")}, (
        "the page believed the axis was open and the service said otherwise; "
        "the line sided with the page")


def test_an_envelope_that_does_not_say_what_it_did_states_nothing(monkeypatch):
    """Fail closed to SILENCE, not to a default. An envelope carrying no
    `divergent_included` is not evidence of either answer, and asserting the
    service's default here would be this module claiming to know something it
    was not told.

    TRANSLATED (owner ruling, 2026-08-06) but ZERO BODY CHANGE: the stub
    below already omitted both the retired `divergence` key AND the current
    `divergent_included` key, so `_render_divergence_basis`'s
    `meta.get('divergent_included')` still reads `None` and still renders
    nothing -- the property (an envelope that says nothing gets no line) was
    never about the retired mechanism in the first place."""
    async def _silent(unit="identification", **kwargs):
        return {"status": "ok", "items": [], "total": 0,
                "meta": {"unit": unit, "bucket": kwargs.get("bucket", "main"),
                         "sort": "band_rank", "sort_basis": "best_band_rank",
                         "novelty_offered": True, "approximate_total": False}}

    client = _render_page(monkeypatch, lang="en", findings=_silent)
    assert _basis_texts(client) == set()


def test_the_shipped_envelope_always_says_which_population_it_counted():
    """The other half of the fail-closed rule above: silence is honest only
    because the shipped reader never produces it. Asserted against the SERVICE,
    not a stub.

    REPLACED (owner ruling, 2026-08-06): the old literal `'"divergence":
    divergence,'` is still present in the source (that key is still built,
    now pinned `shown`), but it is no longer the key the reconciliation line
    depends on -- `_render_divergence_basis` reads `divergent_included`.
    Asserting the old literal would keep this test green even if a future
    edit deleted `divergent_included` from the meta dict entirely, which is
    exactly the silent-envelope regression this test exists to catch."""
    import inspect

    from shared.discovery_service import DiscoveryService

    source = inspect.getsource(DiscoveryService.get_findings_enveloped)
    assert '"divergent_included":' in source, (
        "the findings envelope no longer reports whether it included the "
        "divergent rows -- the result bar's reconciliation line goes silent "
        "and the headline's figure and the row count disagree with nothing "
        "to say so")


@pytest.mark.parametrize("bucket", ["main", "more"])
def test_the_reconciliation_covers_the_pool_invitation_figure_too(monkeypatch, bucket):
    """The invitation names the SECOND pool's full size from the artifact, and
    the default view of that pool is smaller for exactly this reason. One
    statement, rendered in both bucket states, covers both figures.

    TRANSLATED (owner ruling, 2026-08-06): the default findings stub now
    reports `divergent_included: True` (the new default view, `all`, applies
    no novelty predicate and pins `divergence` to `shown`, so nothing is
    narrowed) -- so exercising the NARROWED case that this test's docstring
    describes needs an explicit override, where the old default stub's
    `divergence: 'hidden'` supplied it for free."""
    client = _render_page(
        monkeypatch, lang="en", state=_state(bucket=bucket),
        findings=_fake_findings(meta_extra={"divergent_included": False}),
        launch=_fake_launch(meta_extra={
            "more_pool_total": SENTINEL_MORE_POOL_TOTAL}))
    assert _basis_texts(client) == {fp.copy_text("divergence_excluded", "en")}, (
        f"the {bucket!r} bucket's count is narrowed and unexplained")


@pytest.mark.parametrize("lang", ["en", "he"])
def test_the_reconciliation_line_carries_no_figure_of_its_own(monkeypatch, lang):
    """A count of what is excluded would be a third number on a fourth basis --
    the mixed-basis defect ruling U was issued over. The exclusion is a
    CATEGORY, and words state a category exactly."""
    for key in ("divergence_excluded", "divergence_included"):
        assert not _DIGIT_RE.search(fp.copy_text(key, lang)), key

    client = _render_page(monkeypatch, lang=lang)
    assert not _DIGIT_RE.search("\n".join(_basis_texts(client)))


def test_the_headline_does_not_track_the_readers_filters(monkeypatch):
    """The decision this reconciliation implements, asserted as a property.

    The launch read is issued ONCE, before the body, and carries no filter
    argument at all -- so no reader selection can move it. A headline that
    silently followed the filters would stop being a corpus figure, and the
    page would have two numbers on two undeclared bases instead of one declared
    one plus a sentence.
    """
    import inspect

    calls = []

    async def _launch(*args, **kwargs):
        calls.append((args, kwargs))
        return {"status": "ok", "items": [], "total": 0,
                "meta": {"basis": "main_pool"}}

    _render_page(monkeypatch, lang="en", launch=_launch,
                 state=_state(divergence=True, domain="Liturgy",
                              bucket="more", novelty_only=True))
    assert len(calls) == 1, (
        f"the launch statistics were read {len(calls)} times -- once per page "
        "is the contract, and a per-filter read would be a headline tracking "
        "the reader")
    assert calls[0] == ((), {}), (
        "the launch read carried an argument -- a filter reaching it is a "
        "headline that moves when a toggle moves")

    source = inspect.getsource(fp.fetch_launch_stats)
    assert "state" not in source.split('"""')[-1], (
        "the launch reader took the reader's state")


# ---------------------------------------------------------------------------
# §3.3 -- A PERSISTED PAGE PAST THE END REPORTED A FALSE EMPTY CORPUS.
#
# `page` is persisted. An OFFSET past the end returns no rows, so
# `COUNT(*) OVER ()` counts nothing and the envelope's total is 0 -- for a
# filtered set that may hold thousands. The page then rendered "No results
# found" and "Page 1 / 1" with both pager buttons disabled, and the state
# survived a reload: a reader whose filters narrowed under them was told the
# corpus was empty, permanently.
#
# Two halves, and both are needed: the SERVICE must stop reporting a zero it
# never measured (asserted in tests/test_discovery_findings_query.py), and the
# PAGE must clamp its PERSISTED state -- never a display-only local -- and
# refetch.
# ---------------------------------------------------------------------------

def _paging_findings(recorder, *, total):
    """A findings stub that pages a real row list, exactly as SQL would --
    including reporting the REAL total on a page past the end."""
    async def _call(unit="identification", **kwargs):
        page = int(kwargs.get("page") or 1)
        size = int(kwargs.get("page_size") or fp._default_page_size())
        recorder.append(page)
        rows = [
            _finding_row(f"w{index:06d}", f"ROW-{index}", f"T-S {index}")
            for index in range((page - 1) * size, max((page - 1) * size,
                                                      min(page * size, total)))
        ]
        return {
            "status": "ok", "items": rows, "total": total,
            "meta": {"unit": unit, "bucket": kwargs.get("bucket", "main"),
                     "sort": "band_rank", "sort_basis": "best_band_rank",
                     "novelty_offered": True, "divergence": "hidden",
                     "page": page, "approximate_total": False},
        }
    return _call


def test_a_persisted_page_past_the_end_is_clamped_and_REFETCHED(monkeypatch):
    """The load-bearing one. A display-only clamp fixes what the pager PRINTS
    and leaves the reader on the empty page it printed about."""
    pages_read = []
    size = fp._default_page_size()
    client = _render_page(
        monkeypatch, lang="en",
        findings=_paging_findings(pages_read, total=size + 1),
        state=_state(page=999))

    assert pages_read[0] == 999, "fixture error: the out-of-range page was not sent"
    assert pages_read[-1] == 2, (
        f"the page was never refetched inside the real set: {pages_read}")
    assert len(pages_read) == 2, (
        f"the clamp cost {len(pages_read)} reads; one refetch is the budget")

    assert _elements_with_class(client, fp.ROW_CLASS), (
        "the reader is still looking at an empty result")
    assert not _elements_with_class(client, f"{fp.RESULTS_CLASS}-empty"), (
        "the honest-empty state is rendered over a corpus that has rows")


def test_the_clamp_moves_the_PERSISTED_state_not_a_display_local(monkeypatch):
    """`page` is persisted, so a display-only clamp leaves the bad page in the
    store and the reader lands back on it after a reload."""
    store = {}
    monkeypatch.setattr(fp, "safe_user_set",
                        lambda key, value: store.__setitem__(key, value))
    monkeypatch.setattr(fp, "safe_user_get",
                        lambda key, default=None: store.get(key, default))
    size = fp._default_page_size()
    _render_page(monkeypatch, lang="en",
                 findings=_paging_findings([], total=size + 1),
                 state=_state(page=999))
    assert store[fp._KEY_PAGE] == 2, (
        f"the out-of-range page survived in the store as {store.get(fp._KEY_PAGE)!r}")


def test_an_in_range_page_is_left_alone(monkeypatch):
    """The other direction, so the clamp cannot pass by resetting every page."""
    pages_read = []
    size = fp._default_page_size()
    _render_page(monkeypatch, lang="en",
                 findings=_paging_findings(pages_read, total=size * 4),
                 state=_state(page=3))
    assert pages_read == [3], f"an in-range page was disturbed: {pages_read}"


def test_the_clamp_is_pure_and_only_ever_moves_downwards():
    """Assertable without a browser, which is the point of keeping it a pure
    function of `(state, envelope)`."""
    size = fp._default_page_size()

    state = _state(page=999)
    assert fp.clamp_page_to_total(state, {"status": "ok", "total": size * 2}) is True
    assert state["page"] == 2

    state = _state(page=1)
    assert fp.clamp_page_to_total(state, {"status": "ok", "total": size * 9}) is False
    assert state["page"] == 1, "the clamp moved a page UPWARDS"

    # A genuinely empty set has ONE page, not zero.
    state = _state(page=7)
    assert fp.clamp_page_to_total(state, {"status": "ok", "total": 0}) is True
    assert state["page"] == 1


@pytest.mark.parametrize("status", ["unavailable", "timeout", "busy"])
def test_an_outage_never_clamps_the_page(status):
    """An outage carries no trustworthy total. Clamping against one would
    convert a temporary failure into a PERSISTED page-1 reset -- the reader
    loses their place because the service blinked."""
    state = _state(page=42)
    assert fp.clamp_page_to_total(state, {"status": status, "total": 0}) is False
    assert state["page"] == 42


def test_the_pager_and_the_clamp_share_one_page_arithmetic():
    """A second copy is how the two come to disagree about which page is the
    last one -- and disagreeing about that is the defect the clamp exists to
    fix."""
    import inspect

    source = inspect.getsource(fp)
    assert source.count("math.ceil(") == 1, (
        "the page count is computed in more than one place")
    assert "_page_count(" in inspect.getsource(fp._render_pager)
    assert "_page_count(" in inspect.getsource(fp.clamp_page_to_total)


# ---------------------------------------------------------------------------
# §3.2 -- A TRANSIENT FACET OUTAGE WAS CACHED FOREVER.
#
# `_populate_facets` wrote `cache[level] = (key, envelope)` unconditionally, and
# the key is derived from the REQUEST -- so a `timeout` or a `busy` was served
# for every later refresh whose request was unchanged. The filter read "not
# available yet" beside a working result set until some OTHER control happened
# to move one of that level's own inputs, and no retry a reader could reach
# cleared it.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("status", ["timeout", "busy", "unavailable"])
def test_a_failed_facet_read_is_not_cached_and_the_next_refresh_recovers(
        monkeypatch, status):
    """The load-bearing one. Round 2 changes NOTHING -- the cache-skip round --
    so a cached failure would make it read nothing and keep the amber tag."""
    def _status_for(label, _level):
        return status if label == "the read fails" else "ok"

    observed, client = _drive_facet_rounds(monkeypatch, [
        ("the read fails", {}),
        ("nothing changed", {}),
    ], status_for=_status_for)
    by_label = dict(observed)

    assert by_label["the read fails"] == ["domain", "author", "work"]
    assert by_label["nothing changed"] == ["domain", "author", "work"], (
        f"a {status!r} envelope was cached under its request key -- the filter "
        "stays 'not available yet' until some other control moves one of its "
        "inputs, and no retry a reader can reach clears it")

    # ...and the recovery really reached the reader, not just the cache.
    for level in ("domain", "author", "work"):
        assert not _elements_with_class(
            client, f"{fp.FILTER_BAR_CLASS}-{level}-blocked"), (
            f"the {level!r} facet still renders as unavailable after recovery")


def test_a_successful_facet_read_is_still_cached(monkeypatch):
    """The other direction. Not caching successes would triple this page's draw
    on the smaller of the two bounded-concurrency budgets on every interaction,
    which is what the cache exists to prevent."""
    observed, _client = _drive_facet_rounds(monkeypatch, [
        ("first paint", {}),
        ("nothing changed", {}),
        ("page turn", {"page": 4}),
    ])
    by_label = dict(observed)
    assert by_label["first paint"] == ["domain", "author", "work"]
    assert by_label["nothing changed"] == []
    assert by_label["page turn"] == []


def test_one_levels_failure_does_not_evict_another_levels_good_answer(monkeypatch):
    """The failure is per level, so the cache must be too: a reader whose author
    list timed out should not lose the domain counts they already had."""
    def _status_for(label, level):
        return "timeout" if (label == "author fails" and level == "author") else "ok"

    observed, _client = _drive_facet_rounds(monkeypatch, [
        ("first paint", {}),
        ("author fails", {"bucket": "more"}),
        ("nothing changed", {}),
    ], status_for=_status_for)
    by_label = dict(observed)
    assert by_label["author fails"] == ["domain", "author", "work"]
    assert by_label["nothing changed"] == ["author"], (
        "the unchanged round re-read a level whose answer was good, or skipped "
        f"the one whose answer failed: {by_label['nothing changed']}")


# ---------------------------------------------------------------------------
# §3.4 -- TWO OVERLAPPING REFRESHES COULD MIX POOLS.
#
# Every control's handler is a separate task on the one event loop, and every
# one of them mutates the SHARED `state` dict and then awaits. `refresh()` had
# no revision token, so the read that RETURNED last painted last regardless of
# which was ISSUED last -- and the state it labelled its rows with belonged to
# the newer handler. One pool's rows under the other pool's name and count.
#
# THE TESTS BELOW ARE INTERLEAVINGS, not source scans, and they drive the REAL
# `refresh` closure: the page's own controls are wired through a NiceGUI
# `on_click` lambda that returns None and schedules the coroutine itself, so a
# test cannot await one and cannot control which of two finishes first. The
# closure is captured off `_render_results` -- the same object every control
# calls -- and the two passes then mutate the same state dict and await the same
# gated read, which is the defect's own mechanism.
# ---------------------------------------------------------------------------

def _gated_findings():
    """`(stub, gates)`. Each call parks on its own event; the FIRST call (the
    page's initial paint) is released immediately so the page can build."""
    gates: list = []

    async def _call(unit="identification", **kwargs):
        bucket = kwargs.get("bucket", "main")
        gate = asyncio.Event()
        gates.append((bucket, gate))
        if len(gates) == 1:
            gate.set()
        await gate.wait()
        items = list(_ROWS_BY_BUCKET.get(bucket, []))
        return {
            "status": "ok", "items": items, "total": len(items),
            "meta": {"unit": unit, "bucket": bucket, "sort": "band_rank",
                     "sort_basis": "best_band_rank", "novelty_offered": True,
                     "divergence": "hidden", "page": 1,
                     "approximate_total": False},
        }

    return _call, gates


def _capture_refresh(monkeypatch):
    """A handle on the page's own `refresh` closure.

    Taken off `_render_results`, which every control's handler ultimately calls
    with it -- so this is the SAME callable the bucket chips, the facet nodes
    and the pager all invoke, not a reconstruction of it.
    """
    captured: dict = {}
    original = fp._render_results

    def _spy(envelope, state, lang, refresh, **kwargs):
        captured["refresh"] = refresh
        captured["state"] = state
        return original(envelope, state, lang, refresh, **kwargs)

    monkeypatch.setattr(fp, "_render_results", _spy)
    return captured


def test_the_older_of_two_overlapping_refreshes_never_paints(monkeypatch):
    """The load-bearing one. Issue the 'more' pass, then the 'main' pass, then
    release them in REVERSE -- so the stale read returns last, which is the only
    order in which the defect is visible."""
    findings, gates = _gated_findings()
    captured = _capture_refresh(monkeypatch)

    async def _drive(client):
        refresh, state = captured["refresh"], captured["state"]
        assert len(gates) == 1, "fixture error: the initial paint read more than once"

        state["bucket"] = fp.BUCKET_MORE
        older = asyncio.ensure_future(refresh())
        await asyncio.sleep(0)
        state["bucket"] = "main"
        newer = asyncio.ensure_future(refresh())
        await asyncio.sleep(0)

        assert [bucket for bucket, _gate in gates] == ["main", "more", "main"], (
            f"fixture error: reads issued in an unexpected order {gates!r}")

        # REVERSE ORDER: the newer read answers first, the stale one second.
        gates[2][1].set()
        await newer
        gates[1][1].set()
        await older

    client = _render_page(monkeypatch, lang="en", findings=findings,
                          driver=_drive)

    rendered = _scoped_text(client, fp.RESULTS_CLASS)
    assert MAIN_ROW_TITLE in rendered, (
        "the newer read's rows are not on screen at all")
    assert MORE_ROW_TITLE not in rendered, (
        "the STALE read painted last: the reader is shown one pool's rows while "
        "the page's own state, its result bar and its pool control all say the "
        "other")


def test_a_superseded_pass_abandons_its_paint_the_moment_it_returns(monkeypatch):
    """The same interleaving, released in the order it was issued, with the
    assertion made BETWEEN the two releases -- so it is about the stale pass's
    own behaviour rather than about who happened to finish last."""
    findings, gates = _gated_findings()
    captured = _capture_refresh(monkeypatch)
    seen: dict = {}

    async def _drive(client):
        refresh, state = captured["refresh"], captured["state"]
        state["bucket"] = fp.BUCKET_MORE
        older = asyncio.ensure_future(refresh())
        await asyncio.sleep(0)
        state["bucket"] = "main"
        newer = asyncio.ensure_future(refresh())
        await asyncio.sleep(0)

        # The STALE pass answers first and must paint nothing at all.
        gates[1][1].set()
        await older
        seen["after_stale"] = _scoped_text(client, fp.RESULTS_CLASS)

        gates[2][1].set()
        await newer

    client = _render_page(monkeypatch, lang="en", findings=findings,
                          driver=_drive)

    assert MORE_ROW_TITLE not in seen["after_stale"], (
        "the superseded pass repainted the results region before the newer one "
        "had a chance to answer -- a reader sees the wrong pool flash in, and "
        "would keep it if the newer read then failed")
    rendered = _scoped_text(client, fp.RESULTS_CLASS)
    assert MAIN_ROW_TITLE in rendered and MORE_ROW_TITLE not in rendered


def test_a_lone_refresh_is_never_treated_as_stale(monkeypatch):
    """The other direction: the guard must not suppress the ordinary path. A
    token comparison written the wrong way round would make every refresh stale,
    and the page would never paint at all."""
    client = _render_page(monkeypatch, lang="en")
    assert MAIN_ROW_TITLE in _scoped_text(client, fp.RESULTS_CLASS)
    for level in ("domain", "author", "work"):
        assert _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}-items")


def test_selected_work_lazily_renders_a_citation_order_range(monkeypatch):
    calls = []

    async def _units(work_id):
        calls.append(work_id)
        return {
            "status": "ok",
            "items": [
                {"citation_pos": 10, "part_key": "ch:10", "label_he": "Chapter Ten"},
                {"citation_pos": 11, "part_key": "ch:11", "label_he": "Chapter Eleven"},
            ],
            "total": 2,
            "meta": {"work_id": work_id, "locus_filter": True},
        }

    monkeypatch.setattr(fp, "get_locus_units_enveloped", _units)
    client = _render_page(
        monkeypatch,
        lang="en",
        state=_state(work_id=_UNCURATED_WORK_ID, work_label=_UNCURATED_RAW_TITLE),
    )
    assert calls == [_UNCURATED_WORK_ID]
    from_controls = _elements_with_class(
        client, f"{fp.FILTER_BAR_CLASS}-locus-from"
    )
    to_controls = _elements_with_class(
        client, f"{fp.FILTER_BAR_CLASS}-locus-to"
    )
    assert len(from_controls) == len(to_controls) == 1
    assert from_controls[0].options == to_controls[0].options == {
        10: "Chapter Ten", 11: "Chapter Eleven"
    }


def test_ja_page_range_options_hide_pages_without_changing_unit_labels(monkeypatch):
    items = [
        {"citation_pos": 10, "part_key": "page:10", "label_he": "פרק יז, עמ' 219"},
        {"citation_pos": 11, "part_key": "page:11", "label_he": "פרק יח, עמ׳ 220"},
    ]

    async def _units(work_id):
        return {
            "status": "ok",
            "items": items,
            "total": 2,
            "meta": {
                "work_id": work_id,
                "locus_filter": True,
                "family": "ja",
                "grain": "page",
            },
        }

    monkeypatch.setattr(fp, "get_locus_units_enveloped", _units)
    client = _render_page(
        monkeypatch,
        lang="en",
        state=_state(work_id=_UNCURATED_WORK_ID, work_label=_UNCURATED_RAW_TITLE),
    )
    from_control = _elements_with_class(
        client, f"{fp.FILTER_BAR_CLASS}-locus-from"
    )[0]
    assert from_control.options == {10: "פרק יז", 11: "פרק יח"}
    assert items[0]["label_he"] == "פרק יז, עמ' 219"


def test_the_facet_cascade_stops_reading_when_the_token_moves(monkeypatch):
    """The cascade issues THREE reads and each one is a chance for a newer
    refresh to have taken over. A superseded pass that kept filling containers
    would paint one request's counts beside another request's rows -- the same
    defect as the results region, in the control a reader uses to trust the
    number."""
    calls: list = []

    async def _facets(level, **_kwargs):
        calls.append(level)
        return {"status": "ok", "items": list(_FACET_ITEMS.get(level, [])),
                "total": 0, "meta": {"level": level}}

    async def _noop() -> None:
        return None

    # False once (the check after the domain-label prime), then True (the check
    # after the FIRST facet read), so exactly one level is read.
    checks = {"n": 0}

    def _is_stale() -> bool:
        checks["n"] += 1
        return checks["n"] > 1

    async def _run(client):
        bar = _elements_with_class(client, fp.FILTER_BAR_CLASS)[0]
        del calls[:]
        checks["n"] = 0
        await fp._populate_facets(bar, _state(), "en", _noop, cache=None,
                                  is_stale=_is_stale)

    _render_page(monkeypatch, lang="en", facets=_facets, driver=_run)

    assert calls == ["domain"], (
        f"a superseded cascade read {calls} -- the token is not checked between "
        "the levels, so two of the three lists are filled from a request the "
        "reader has already moved off")


def test_the_cascade_bails_before_any_read_when_it_is_already_superseded(monkeypatch):
    """The earliest check: a pass superseded before it reaches its first read
    must spend no slot on the heavy bounded-concurrency budget at all."""
    calls: list = []

    async def _facets(level, **_kwargs):
        calls.append(level)
        return {"status": "ok", "items": [], "total": 0, "meta": {"level": level}}

    async def _noop() -> None:
        return None

    async def _run(client):
        bar = _elements_with_class(client, fp.FILTER_BAR_CLASS)[0]
        del calls[:]
        await fp._populate_facets(bar, _state(), "en", _noop, cache=None,
                                  is_stale=lambda: True)

    _render_page(monkeypatch, lang="en", facets=_facets, driver=_run)
    assert calls == [], f"a superseded cascade still issued reads: {calls}"


def _gated_facets():
    """`(stub, gates)`. Each read parks on its own event and returns items
    LABELLED with the bucket it was asked for, so a rendered facet list says
    which request produced it. The initial paint's three reads are released
    immediately so the page can build."""
    gates: list = []

    async def _call(level, **kwargs):
        bucket = kwargs.get("bucket", "main")
        gate = asyncio.Event()
        gates.append((level, bucket, gate))
        if len(gates) <= 3:
            gate.set()
        await gate.wait()
        return {
            "status": "ok",
            # The LABEL is deliberately distinct from the VALUE: the work
            # level's `facet_display_label` treats a label equal to its value as
            # a missing title (a `w`-prefixed key must never print as one), so a
            # fixture that made them equal would render "Title unavailable" and
            # the assertion below would be about the wrong string entirely.
            "items": [{"level": level, "value": f"{bucket}-{level}",
                       "label": f"{bucket}-{level}-name", "parent": None,
                       "is_leaf": True, "count": 1}],
            "total": 1, "meta": {"level": level},
        }

    return _call, gates


async def _drain(gates, task, bucket, *, budget=60):
    """Release every parked read belonging to `bucket`, letting the task walk
    its sequential cascade one level at a time, until it finishes."""
    for _ in range(budget):
        if task.done():
            break
        for _level, asked, gate in list(gates):
            if asked == bucket and not gate.is_set():
                gate.set()
        await asyncio.sleep(0)
    assert task.done(), "the refresh pass never finished within its budget"
    await task


def test_the_page_hands_the_revision_token_to_the_facet_cascade(monkeypatch):
    """The WIRING, not the cascade's own honouring of the token.

    `_populate_facets` takes `is_stale`, and its own tests pass one directly --
    so dropping the argument at the ONE call site in `refresh` left every one of
    them green while the live page went back to painting a stale request's facet
    counts beside a newer request's rows.

    Driven as an interleaving: the older pass's cascade is released AFTER the
    newer pass has already filled the lists.
    """
    findings = _fake_findings()
    facets, gates = _gated_facets()
    captured = _capture_refresh(monkeypatch)

    async def _drive(client):
        refresh, state = captured["refresh"], captured["state"]
        assert len(gates) == 3, f"fixture error: initial cascade read {gates!r}"

        state["bucket"] = fp.BUCKET_MORE
        older = asyncio.ensure_future(refresh())
        await asyncio.sleep(0)
        state["bucket"] = "main"
        newer = asyncio.ensure_future(refresh())
        await asyncio.sleep(0)

        # The NEWER pass fills the lists first...
        await _drain(gates, newer, "main")
        # ...and the stale one is released afterwards, which is the order that
        # makes the defect visible.
        await _drain(gates, older, "more")

    client = _render_page(monkeypatch, lang="en", findings=findings,
                          facets=facets, driver=_drive)

    def _offered(level: str) -> str:
        """What a reader can pick at `level`, however that level renders it.

        The domain facet is a tree of buttons whose text is in the subtree; the
        author and work facets are searchable selects whose option labels live
        in `_props['options']` and appear in NO element text -- an assertion
        that scoped the subtree for those two would be asserting about an empty
        string and would pass for the wrong reason."""
        if level == "domain":
            return _scoped_text(client, f"{fp.FILTER_BAR_CLASS}-{level}-items")
        return " | ".join(_facet_option_names(client, level))

    for level in ("domain", "author", "work"):
        rendered = _offered(level)
        assert f"main-{level}" in rendered, (
            f"the {level!r} list does not hold the newer request's answer: "
            f"{rendered!r}")
        assert f"more-{level}" not in rendered, (
            f"the {level!r} list was refilled by the SUPERSEDED pass -- the "
            "counts beside the filters describe a request the reader has "
            "already moved off, which is what the token exists to prevent")


# ---------------------------------------------------------------------------
# §3.5 -- TWO CONFIG KNOBS COULD MAKE ROWS UNREACHABLE, SILENTLY.
#
# (a) `DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT` above `DISCOVERY_PAGE_SIZE_MAX`:
#     the service clamps the size it serves, the pager divided the real total by
#     the size it had REQUESTED, and the page count came out too small -- the
#     tail of the set unreachable, with nothing on the page saying so.
#
# (b) `DISCOVERY_FINDINGS_COUNT_MAX` above 0: the counting query stops at the
#     cap, so the last page the arithmetic can NAME is not the last page that
#     exists. The pager disabled `Next` there, which reads as "that is all
#     there is" -- the one claim a capped count cannot support.
# ---------------------------------------------------------------------------

def _sized_findings(*, total, served_size, approximate=False):
    """A findings stub that reports the page size the SERVICE used, which is
    what a clamped size looks like from the page's side."""
    async def _call(unit="identification", **kwargs):
        page = int(kwargs.get("page") or 1)
        start = (page - 1) * served_size
        rows = [
            _finding_row(f"w{index:06d}", f"ROW-{index}", f"T-S {index}")
            for index in range(start, max(start, min(start + served_size, total)))
        ]
        return {
            "status": "ok", "items": rows, "total": total,
            "meta": {"unit": unit, "bucket": kwargs.get("bucket", "main"),
                     "sort": "band_rank", "sort_basis": "best_band_rank",
                     "novelty_offered": True, "divergence": "hidden",
                     "page": page, "page_size": served_size,
                     "approximate_total": approximate},
        }
    return _call


def _pager_position(client) -> str:
    return "\n".join(_node_texts(client, f"{fp.PAGER_CLASS}-position"))


def _pager_button(client, which: str):
    found = _elements_with_class(client, f"{fp.PAGER_CLASS}-{which}")
    assert len(found) == 1, f"expected one {which!r} button, got {len(found)}"
    return found[0]


def test_the_pager_counts_pages_at_the_size_the_service_SERVED(monkeypatch):
    """(a). The requested size is 50; the service serves 200 (its own ceiling
    is the authority). 1,000 rows is FIVE pages at 200, not twenty at 50 -- and
    the wrong arithmetic in the other direction is what buried the tail."""
    client = _render_page(
        monkeypatch, lang="en",
        findings=_sized_findings(total=1000, served_size=200))
    assert "/ 5" in _pager_position(client), (
        f"the pager divided by its own requested size: {_pager_position(client)!r}")


def test_the_effective_page_size_comes_from_the_envelope_not_the_request():
    """PURE, so the rule is assertable without a browser."""
    assert fp.effective_page_size(
        {"meta": {"page_size": 200}}) == 200
    # Absent, non-positive or not an int -> the budgeted default, which is the
    # value the service would itself have clamped from.
    for bad in ({}, {"meta": {}}, {"meta": {"page_size": 0}},
                {"meta": {"page_size": -5}}, {"meta": {"page_size": "200"}},
                {"meta": {"page_size": True}}):
        assert fp.effective_page_size(bad) == fp._default_page_size(), bad


def test_the_clamp_uses_the_served_size_too(monkeypatch):
    """The clamp and the pager must agree about which page is the last one, or
    the clamp pulls a reader off a page the pager says exists."""
    state = _state(page=5)
    envelope = {"status": "ok", "items": [],
                "total": 1000, "meta": {"page_size": 200}}
    assert fp.clamp_page_to_total(state, envelope) is False, (
        "page 5 of 5 was clamped -- the clamp divided by a size the service "
        "did not use")

    state = _state(page=6)
    assert fp.clamp_page_to_total(state, envelope) is True
    assert state["page"] == 5


def test_a_capped_total_leaves_Next_live_and_SAYS_the_count_stopped(monkeypatch):
    """(b). The reader is on the last page the cap can name. There ARE more
    rows; the pager must neither pretend otherwise nor invent how many."""
    size = fp._default_page_size()
    client = _render_page(
        monkeypatch, lang="en",
        findings=_sized_findings(total=size * 2, served_size=size,
                                 approximate=True),
        state=_state(page=2))

    assert _pager_button(client, "next").enabled is True, (
        "the pager disabled Next on a CAPPED total -- a silently terminal pager "
        "reads as 'that is all there is', which is the one claim a capped count "
        "cannot support")
    notes = _node_texts(client, f"{fp.PAGER_CLASS}-capped")
    assert notes, "the cap truncates browsing and the page does not say so"
    assert fp.copy_text("pager_capped_note", "en") in notes
    assert not _DIGIT_RE.search(fp.copy_text("pager_capped_note", "en")), (
        "the note invents a figure the stopped count cannot support")


def test_an_exact_total_still_ends_the_pager(monkeypatch):
    """The other direction, so the fix cannot pass by never disabling Next."""
    size = fp._default_page_size()
    client = _render_page(
        monkeypatch, lang="en",
        findings=_sized_findings(total=size * 2, served_size=size),
        state=_state(page=2))
    assert _pager_button(client, "next").enabled is False
    assert not _node_texts(client, f"{fp.PAGER_CLASS}-capped"), (
        "an exact total rendered the capped-count note")


def test_a_capped_total_does_not_clamp_a_page_that_HAS_rows(monkeypatch):
    """The interaction between this fix and §3.3's clamp, and the reason the
    clamp requires an EMPTY page. A capped total is a lower bound, so a page
    above `total / size` can still be full -- and clamping a reader off a page
    they can see would be §3.3's fix creating §3.5's defect."""
    size = fp._default_page_size()
    state = _state(page=9)
    full_page = {"status": "ok",
                 "items": [_finding_row("w1", "ROW", "T-S 1")],
                 "total": size, "meta": {"page_size": size,
                                         "approximate_total": True}}
    assert fp.clamp_page_to_total(state, full_page) is False
    assert state["page"] == 9, "a page with rows on it was clamped away"

    # ...and an EMPTY page above the cap still clamps, to the last page the
    # count can vouch for -- a page that certainly has rows.
    state = _state(page=9)
    empty_page = dict(full_page, items=[])
    assert fp.clamp_page_to_total(state, empty_page) is True
    assert state["page"] == 1


def test_paging_past_the_capped_end_is_reachable(monkeypatch):
    """End to end: the note says more pages may follow, and Next really goes
    there rather than being clamped straight back."""
    size = fp._default_page_size()
    pages_read = []

    async def _call(unit="identification", **kwargs):
        page = int(kwargs.get("page") or 1)
        pages_read.append(page)
        # The cap names two pages; the set really has three.
        rows = [] if page > 3 else [
            _finding_row(f"w{page}", f"ROW-PAGE-{page}", f"T-S {page}")]
        return {
            "status": "ok", "items": rows, "total": size * 2,
            "meta": {"unit": unit, "bucket": "main", "sort": "band_rank",
                     "sort_basis": "best_band_rank", "novelty_offered": True,
                     "divergence": "hidden", "page": page,
                     "page_size": size, "approximate_total": True},
        }

    async def _drive(client):
        button = _pager_button(client, "next")
        listeners = [listener for listener in button._event_listeners.values()
                     if listener.type == "click"]
        assert listeners, "the Next button is wired to nothing"
        result = listeners[0].handler(None)
        if asyncio.iscoroutine(result):
            await result

    client = _render_page(monkeypatch, lang="en", findings=_call,
                         state=_state(page=2), driver=_drive)
    assert 3 in pages_read, (
        f"Next did not reach page 3, beyond what the cap could name: {pages_read}")
    assert "ROW-PAGE-3" in _scoped_text(client, fp.RESULTS_CLASS)


# ---------------------------------------------------------------------------
# §3.1 -- THE FACET CACHE KEY CARRIED NO ARTIFACT IDENTITY.
#
# The rows are re-read on every refresh, so they always come from the artifact
# being served now. The facet COUNTS were cached under a key derived from the
# request alone -- so a rebuild swapped in under a page that stays open left the
# old artifact's counts sitting beside the new artifact's rows. A number beside
# an option then described a population that option no longer produces, which is
# the one promise `_node_text` makes.
# ---------------------------------------------------------------------------

def test_the_facet_cache_key_carries_the_artifact_identity(monkeypatch):
    """PATH AND VERSION, both. The version alone is not an identity: every local
    artifact in this project reports the same `sidecar_version` string while
    holding different data, which the service's own launch-stats cache documents
    and keys on the pair to avoid."""
    state = _state()

    monkeypatch.setattr(fp, "discovery_db_path", lambda: "/artifacts/one.db")
    monkeypatch.setattr(fp, "discovery_sidecar_version", lambda: "v1")
    first = fp._facet_cache_key("domain", state)

    # A NEW VERSION at the same path.
    monkeypatch.setattr(fp, "discovery_sidecar_version", lambda: "v2")
    assert fp._facet_cache_key("domain", state) != first, (
        "a sidecar version change did not move the key -- the cascade keeps the "
        "previous artifact's counts beside the new artifact's rows")

    # A NEW PATH at the SAME version -- the trap a version-only key falls into.
    monkeypatch.setattr(fp, "discovery_sidecar_version", lambda: "v1")
    monkeypatch.setattr(fp, "discovery_db_path", lambda: "/artifacts/two.db")
    assert fp._facet_cache_key("domain", state) != first, (
        "a different artifact reporting the SAME version shares a cache key -- "
        "every local artifact here reports the same version string")

    # ...and an unchanged artifact with an unchanged request still matches, or
    # the cache would be a no-op and every interaction would re-read all three
    # levels on the smaller of the two bounded-concurrency budgets.
    monkeypatch.setattr(fp, "discovery_db_path", lambda: "/artifacts/one.db")
    assert fp._facet_cache_key("domain", state) == first


def test_an_artifact_swap_under_an_open_page_re_reads_the_cascade(monkeypatch):
    """The behaviour, not the key: the cache must MISS after a swap, on a round
    where nothing about the request moved."""
    versions = {"n": "v1"}
    monkeypatch.setattr(fp, "discovery_db_path", lambda: "/artifacts/one.db")
    monkeypatch.setattr(fp, "discovery_sidecar_version", lambda: versions["n"])

    def _bump(_label, _level):
        return "ok"

    observed, _client = _drive_facet_rounds(monkeypatch, [
        ("first paint", {}),
        ("nothing changed", {}),
    ], status_for=_bump)
    assert dict(observed)["nothing changed"] == [], (
        "fixture error: the unchanged round must be the cache-skip round")

    # Now swap the artifact and run an otherwise identical round.
    versions["n"] = "v2"
    observed, _client = _drive_facet_rounds(monkeypatch, [
        ("first paint", {}),
        ("artifact swapped", {}),
    ], status_for=_swap_version_after_first_round(versions))
    assert dict(observed)["artifact swapped"] == ["domain", "author", "work"], (
        "the cascade was served from the previous artifact's cache after a swap")


def _swap_version_after_first_round(versions):
    """Flip the reported sidecar version once the first round has read."""
    def _status_for(label, _level):
        if label == "first paint":
            versions["n"] = "v1"
        else:
            versions["n"] = "v2"
        return "ok"
    return _status_for


def test_the_artifact_identity_read_is_two_pure_in_memory_lookups():
    """It is called from a SYNCHRONOUS key builder on the one event loop, so it
    must not be I/O. Both accessors read state loaded at startup; asserted by
    naming them, because a blocking call added behind either would stall every
    concurrent request on this single-worker server."""
    import inspect

    from web import discovery_assets

    for name in ("discovery_db_path", "discovery_sidecar_version"):
        body = inspect.getsource(getattr(discovery_assets, name))
        assert "return _state." in body, (
            f"{name} no longer reads startup-loaded state directly: {body!r}")
        for blocking in ("open(", "connect(", "execute(", "requests.", "sleep("):
            assert blocking not in body, (
                f"{name} became a blocking call and is used in a synchronous "
                f"cache-key builder on the event loop: {blocking!r}")


# ---------------------------------------------------------------------------
# §3.6, the PAGE side: the row unit is part of the cascade's request.
# ---------------------------------------------------------------------------

def test_the_cascade_request_carries_the_row_unit(monkeypatch):
    """The counts are counts of ROWS and the unit decides what a row is, so a
    cascade that did not carry it put a number beside an option describing a
    population the result bar beside it did not report."""
    facets = []
    _render_page(monkeypatch, lang="en", facets=_recording_facets(facets),
                 state=_state(unit="manuscript"))
    assert facets, "the page issued no facet read"
    assert all(call.get("unit") == "manuscript" for call in facets), (
        f"the cascade was read at the wrong grain: {facets!r}")


def test_changing_the_row_unit_re_reads_every_facet_level(monkeypatch):
    """The unit is part of the request, so it must be part of the re-fetch key.
    A cached cascade served across a unit change is the defect with an extra
    step: the counts would be right for a grain the reader has left."""
    observed, _client = _drive_facet_rounds(monkeypatch, [
        ("first paint", {}),
        ("nothing changed", {}),
        ("unit change", {"unit": "manuscript"}),
        ("back again", {"unit": "identification"}),
    ])
    by_label = dict(observed)
    assert by_label["nothing changed"] == []
    assert by_label["unit change"] == ["domain", "author", "work"], (
        "a unit change left every facet count at the previous grain")
    assert by_label["back again"] == ["domain", "author", "work"]


def test_the_count_promise_is_made_on_all_three_controls_or_on_none():
    """`_node_text` is the one place a count is attached to an option, and the
    domain tree, the author select and the work select all route through it. The
    promise in its docstring is therefore made three times, which is why §3.6
    had to be fixed rather than documented away."""
    import inspect

    source = inspect.getsource(fp)
    assert source.count("def _node_text(") == 1, (
        "a second count formatter would make the promise somewhere this test "
        "cannot see")
    for function in (fp._render_domain_tree, fp._render_facet_select):
        assert "_node_text(" in inspect.getsource(function), (
            f"{function.__name__} stopped routing its count through _node_text")


# ---------------------------------------------------------------------------
# §3.7 -- THE HEADLINE READ NO LONGER RUNS BEFORE THE BODY IN SERIES.
#
# `_paint_headline`'s docstring claimed a slow headline read "never delays" the
# rest of the page, and the caller awaited that read before rendering the body
# at all -- so the rows waited for a corpus-scale count and the docstring
# asserted the opposite of the code.
#
# The read is now DISPATCHED before the body and AWAITED after it, so the two
# heavy reads overlap. That is an improvement rather than isolation: the pool
# invitation shows the second pool's size from the SAME envelope, so the body
# has a real data dependency on it -- and the corrected docstring says exactly
# that instead of promising something it cannot deliver.
# ---------------------------------------------------------------------------


def test_a_refresh_picks_up_a_row_another_admin_hid_WITHOUT_a_supabase_read(monkeypatch):
    """THE cross-page coherence property, driven through the REAL page.

    Scenario: this page loads, another admin hides a row, then this reader touches
    any control. The refresh must pass the newly-hidden id into the findings read.

    AND IT MUST DO SO WITHOUT AWAITING A SUPABASE READ. That half is not a
    nice-to-have: awaiting the real reader on the refresh path broke the
    one-dispatch-per-read probe AND
    `test_clicking_cycles_hidden_then_shown_then_only_and_the_query_follows`, whose
    rows stopped arriving inside its yield budget. So the async reader is stubbed to
    RAISE -- if the page reaches for it during a refresh, this test fails loudly
    rather than passing slowly.

    Fails on the pre-fix implementation, which resolved the hide list once per page
    and never consulted it again.
    """
    seen_suppressed = []

    async def _findings(unit="identification", *, bucket="main", sort="band_rank",
                        suppressed=(), **_kw):
        seen_suppressed.append(tuple(suppressed))
        return {
            "status": "ok", "items": list(_ROWS_BY_BUCKET.get(bucket, [])),
            "total": 1,
            "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                     "sort_basis": "best_band_rank", "novelty_offered": True,
                     "divergence": "shown", "divergent_included": True,
                     "page": 1, "page_size": fp._default_page_size(),
                     "approximate_total": False},
        }

    # The page loads with an EMPTY hide list...
    async def _cold_list():
        return ()

    # ...and must never await this again during a refresh.
    async def _must_not_be_awaited():
        raise AssertionError(
            "the refresh path awaited the Supabase hide-list reader -- that puts a "
            "round trip on every filter change and page turn")

    # Another admin's hide, visible only through the synchronous cache peek.
    peeked = {"value": None}

    def _peek():
        return peeked["value"]

    monkeypatch.setattr(fp, "suppressed_identification_ids", _cold_list)
    monkeypatch.setattr(fp, "cached_suppressed_identification_ids", _peek)

    async def _drive(client):
        # The page has painted once, with nothing hidden.
        assert seen_suppressed and seen_suppressed[0] == (), seen_suppressed
        # NOW another admin hides a row, and the page must not re-read Supabase.
        monkeypatch.setattr(fp, "suppressed_identification_ids", _must_not_be_awaited)
        peeked["value"] = ("hidden-by-someone-else",)
        select = _divergence_switch(client)
        before = len(seen_suppressed)
        # CANDIDATES, not ALL: `ALL` is the default the page loads with, so
        # assigning it changes nothing and fires no event.
        select.value = fp.NOVELTY_VIEW_CANDIDATES
        for _ in range(60):
            if len(seen_suppressed) > before:
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError("the control change never produced a findings read")

    _render_page(monkeypatch, lang="en", findings=_findings, driver=_drive)

    assert seen_suppressed[-1] == ("hidden-by-someone-else",), (
        "the refresh did not pass the newly-hidden id to the findings read -- a "
        f"row another admin withdrew is still being shown (saw {seen_suppressed})")


def test_the_peek_returning_unknown_leaves_an_existing_hide_list_intact(monkeypatch):
    """`None` from the peek must mean KEEP WHAT WE HAD, never `()`.

    Drives the real page with a POPULATED initial list and a peek that reports
    "nothing fresh cached". Every subsequent read must still carry the original id;
    a page that treated `None` as empty would un-hide it on the first refresh.
    """
    seen_suppressed = []

    async def _findings(unit="identification", *, bucket="main", sort="band_rank",
                        suppressed=(), **_kw):
        seen_suppressed.append(tuple(suppressed))
        return {
            "status": "ok", "items": list(_ROWS_BY_BUCKET.get(bucket, [])),
            "total": 1,
            "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                     "sort_basis": "best_band_rank", "novelty_offered": True,
                     "divergence": "shown", "divergent_included": True,
                     "page": 1, "page_size": fp._default_page_size(),
                     "approximate_total": False},
        }

    async def _warm_list():
        return ("hidden-at-load",)

    monkeypatch.setattr(fp, "suppressed_identification_ids", _warm_list)
    monkeypatch.setattr(fp, "cached_suppressed_identification_ids", lambda: None)

    async def _drive(client):
        select = _divergence_switch(client)
        before = len(seen_suppressed)
        # CANDIDATES, not ALL: `ALL` is the default the page loads with, so
        # assigning it changes nothing and fires no event.
        select.value = fp.NOVELTY_VIEW_CANDIDATES
        for _ in range(60):
            if len(seen_suppressed) > before:
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError("the control change never produced a findings read")

    _render_page(monkeypatch, lang="en", findings=_findings, driver=_drive)

    assert seen_suppressed, "the page issued no findings read"
    for observed in seen_suppressed:
        assert "hidden-at-load" in observed, (
            "an unknown cache peek un-hid the row the page loaded with "
            f"(saw {seen_suppressed})")



def test_an_expired_cache_CONVERGES_on_its_own_with_no_further_interaction(monkeypatch):
    """THE convergence property, end to end, through the REAL re-warm.

    An earlier revision of this test replaced `_rewarm_hide_list` with a counter,
    which Codex's third pass correctly called insufficient: it proved the page calls
    a helper, not that the helper reads, applies and re-renders. It also let a
    genuine defect through -- the re-warm only warmed the CACHE and left the result
    for "the next refresh", so a page nobody touched again never converged at all.

    So nothing here is mocked except the two I/O boundaries. The peek starts as
    "expired" and the async reader returns a row hidden elsewhere; the page must
    reach a state where its findings query carries that id, WITHOUT any further
    control change.
    """
    reads = []

    async def _findings(unit="identification", *, bucket="main", sort="band_rank",
                        suppressed=(), **_kw):
        reads.append(tuple(suppressed))
        return {
            "status": "ok", "items": list(_ROWS_BY_BUCKET.get(bucket, [])),
            "total": 1,
            "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                     "sort_basis": "best_band_rank", "novelty_offered": True,
                     "divergence": "shown", "divergent_included": True,
                     "page": 1, "page_size": fp._default_page_size(),
                     "approximate_total": False},
        }

    # THE LIST CHANGES BETWEEN THE TWO READS, and that ordering is the whole test.
    # The PAGE LOAD reads it first and must see nothing hidden -- otherwise
    # `hidden["ids"]` starts populated, every later query carries the id for free,
    # and the assertion passes without the re-warm applying anything at all (an
    # earlier revision of this test had exactly that hole and passed against a
    # deliberately broken cache-only re-warm). Only the SECOND read -- the re-warm's
    # -- returns the row another admin hid.
    remote = {"calls": 0}

    async def _remote_list():
        remote["calls"] += 1
        if remote["calls"] == 1:
            return ()
        return ("hidden-by-someone-else",)

    monkeypatch.setattr(fp, "suppressed_identification_ids", _remote_list)
    # The page LOADS with nothing hidden (its own first read is stubbed below), and
    # the peek reports no fresh cached answer -- the expired-cache state.
    monkeypatch.setattr(fp, "cached_suppressed_identification_ids", lambda: None)
    # `cache_needs_refresh` is the real predicate's job; force the "wants a re-warm"
    # answer so this test is about the page's behaviour, not the cache's clock.
    import web.discovery_suppression as sup
    monkeypatch.setattr(sup, "cache_needs_refresh", lambda: True)

    async def _drive(client):
        # Let the background re-warm run to completion. No control is touched.
        for _ in range(200):
            if any("hidden-by-someone-else" in r for r in reads):
                break
            await asyncio.sleep(0)

    _render_page(monkeypatch, lang="en", findings=_findings, driver=_drive)

    assert any("hidden-by-someone-else" in r for r in reads), (
        "the page never converged on the row another admin hid, with no further "
        f"interaction -- staleness is unbounded, not one click (saw {reads})")


def test_the_rewarm_is_SINGLE_FLIGHT_and_does_not_dispatch_a_task_per_refresh(monkeypatch):
    """One in-flight re-warm per page, not one per refresh (Codex, MEDIUM).

    Without a marker every refresh passes the expired-cache test and creates its own
    task. The shared semaphore then serialises the I/O so Supabase is not stormed,
    but the task queue is unbounded and every task after the first is redundant.

    Counts REAL reads through the async reader while driving several refreshes.
    """
    reads = []
    list_reads = {"n": 0}

    async def _findings(unit="identification", *, bucket="main", sort="band_rank",
                        suppressed=(), **_kw):
        reads.append(tuple(suppressed))
        return {
            "status": "ok", "items": list(_ROWS_BY_BUCKET.get(bucket, [])),
            "total": 1,
            "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                     "sort_basis": "best_band_rank", "novelty_offered": True,
                     "divergence": "shown", "divergent_included": True,
                     "page": 1, "page_size": fp._default_page_size(),
                     "approximate_total": False},
        }

    async def _slow_list():
        list_reads["n"] += 1
        # Park, so several refreshes can overlap this one in-flight read.
        for _ in range(5):
            await asyncio.sleep(0)
        return ()

    monkeypatch.setattr(fp, "suppressed_identification_ids", _slow_list)
    monkeypatch.setattr(fp, "cached_suppressed_identification_ids", lambda: None)
    import web.discovery_suppression as sup
    monkeypatch.setattr(sup, "cache_needs_refresh", lambda: True)

    async def _drive(client):
        select = _divergence_switch(client)
        for view in (fp.NOVELTY_VIEW_CANDIDATES, fp.NOVELTY_VIEW_DIVERGENT,
                     fp.NOVELTY_VIEW_EITHER):
            before = len(reads)
            select.value = view
            for _ in range(60):
                if len(reads) > before:
                    break
                await asyncio.sleep(0)

    _render_page(monkeypatch, lang="en", findings=_findings, driver=_drive)

    # The page load reads the list once directly; the re-warms must not add one per
    # refresh on top of that. Four refreshes (load + three control changes) with an
    # always-expired cache would dispatch four tasks without the single-flight flag.
    assert list_reads["n"] <= 3, (
        f"the hide list was read {list_reads['n']} times across four refreshes -- "
        "the re-warm is dispatching per refresh instead of single-flighting")


def test_the_rewarm_is_NOT_dispatched_when_the_peek_has_a_fresh_answer(monkeypatch):
    """The other direction: a fresh cache must not trigger a fetch. Without this
    bound, the re-warm becomes a Supabase round trip on every interaction -- the
    exact cost the peek was introduced to avoid."""
    rewarms = {"n": 0}
    reads = []

    async def _findings(unit="identification", *, bucket="main", sort="band_rank",
                        suppressed=(), **_kw):
        reads.append(tuple(suppressed))
        return {
            "status": "ok", "items": list(_ROWS_BY_BUCKET.get(bucket, [])),
            "total": 1,
            "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                     "sort_basis": "best_band_rank", "novelty_offered": True,
                     "divergence": "shown", "divergent_included": True,
                     "page": 1, "page_size": fp._default_page_size(),
                     "approximate_total": False},
        }

    async def _load_list():
        return ()

    monkeypatch.setattr(fp, "suppressed_identification_ids", _load_list)
    monkeypatch.setattr(fp, "cached_suppressed_identification_ids",
                        lambda: ("hidden-1",))
    monkeypatch.setattr(fp, "_rewarm_hide_list",
                        lambda: rewarms.__setitem__("n", rewarms["n"] + 1))

    async def _drive(client):
        select = _divergence_switch(client)
        before = len(reads)
        select.value = fp.NOVELTY_VIEW_CANDIDATES
        for _ in range(60):
            if len(reads) > before:
                break
            await asyncio.sleep(0)
        else:
            raise AssertionError("the control change never produced a findings read")

    _render_page(monkeypatch, lang="en", findings=_findings, driver=_drive)

    assert rewarms["n"] == 0, (
        "a FRESH cache still dispatched a re-warm -- that is a Supabase round trip "
        "on every interaction")
    assert reads[-1] == ("hidden-1",), (
        f"the fresh peek was not applied to the query (saw {reads})")


def test_the_launch_read_and_the_row_read_OVERLAP(monkeypatch):
    """The load-bearing one. The findings read must be ISSUED while the launch
    read is still parked -- which is impossible if the launch read is awaited
    first."""
    order: list = []

    async def _launch(*_args, **_kwargs):
        order.append("launch:start")
        # ONE yield, so the read cannot complete without the loop handing
        # control back. Deliberately NOT an Event released by the row read: that
        # would DEADLOCK against the serialized order instead of failing, and a
        # test that hangs on the defect is not a test that reports it.
        await asyncio.sleep(0)
        order.append("launch:done")
        return {"status": "ok", "items": [], "total": 0,
                "meta": {"basis": "main_pool",
                         "more_pool_total": SENTINEL_MORE_POOL_TOTAL}}

    async def _findings(unit="identification", **kwargs):
        order.append("findings:start")
        return {
            "status": "ok", "items": list(_ROWS_BY_BUCKET["main"]), "total": 1,
            "meta": {"unit": unit, "bucket": kwargs.get("bucket", "main"),
                     "sort": "band_rank", "sort_basis": "best_band_rank",
                     "novelty_offered": True, "divergence": "hidden",
                     "page": 1, "page_size": fp._default_page_size(),
                     "approximate_total": False},
        }

    client = _render_page(monkeypatch, lang="en", findings=_findings,
                          launch=_launch)

    assert {"launch:start", "launch:done", "findings:start"} <= set(order), (
        f"fixture error: not every read ran: {order}")
    assert order.index("findings:start") < order.index("launch:done"), (
        f"the row read was issued only AFTER the launch read completed ({order}) "
        "-- the rows wait for a corpus-scale count in series, which is exactly "
        "what the docstring claimed they do not")

    # ...and both consumers still got their envelope.
    assert MAIN_ROW_TITLE in _scoped_text(client, fp.RESULTS_CLASS)
    assert f"{SENTINEL_MORE_POOL_TOTAL:,}" in _scoped_text(
        client, fp.POOL_INVITE_CLASS), (
        "the pool invitation lost the size the launch envelope carries")
    assert _scoped_text(client, rows.LAUNCH_CLASS), (
        "the headline was never painted")


def test_the_launch_read_is_issued_exactly_once_for_its_two_consumers(monkeypatch):
    """A task, not a bare coroutine: two places await it, and a second READ
    would be a second crossing on the heavy budget for a figure the page has."""
    calls: list = []

    async def _launch(*_args, **_kwargs):
        calls.append(1)
        return {"status": "ok", "items": [], "total": 0,
                "meta": {"basis": "main_pool",
                         "more_pool_total": SENTINEL_MORE_POOL_TOTAL}}

    client = _render_page(monkeypatch, lang="en", launch=_launch)
    assert len(calls) == 1, f"the launch statistics were read {len(calls)} times"
    assert f"{SENTINEL_MORE_POOL_TOTAL:,}" in _scoped_text(
        client, fp.POOL_INVITE_CLASS)
    assert _scoped_text(client, rows.LAUNCH_CLASS)


def test_three_refreshes_still_issue_exactly_one_launch_read(monkeypatch):
    """The body awaits the launch TASK on every refresh, and that must stay free.

    Asserted as a count of READS, not as the presence of a memo: awaiting a
    completed task is already free, so a memo would be unfalsifiable. What this
    can and does catch is the body re-CALLING the launch reader per refresh --
    a heavy-budget crossing behind every filter change for a figure that cannot
    move while the page is open."""
    calls: list = []

    async def _launch(*_args, **_kwargs):
        calls.append(1)
        return {"status": "ok", "items": [], "total": 0,
                "meta": {"basis": "main_pool",
                         "more_pool_total": SENTINEL_MORE_POOL_TOTAL}}

    captured = _capture_refresh(monkeypatch)

    async def _drive(_client):
        for _ in range(3):
            await captured["refresh"]()

    _render_page(monkeypatch, lang="en", launch=_launch, driver=_drive)
    assert len(calls) == 1, (
        f"three refreshes issued {len(calls)} launch reads")


def test_the_headline_docstring_does_not_promise_isolation_it_cannot_give():
    """A docstring asserting the opposite of the code is worse than none. The
    body genuinely depends on this read -- the pool invitation shows the second
    pool's size from the same envelope -- so the claim that had to go is the one
    about DELAY, and what replaced it names the dependency."""
    import inspect

    doc = inspect.getdoc(fp._paint_headline) or ""
    assert "never delays" not in doc, (
        "the docstring still promises the rows are not delayed by this read")
    assert "never breaks" in doc, (
        "the surviving claim -- a failing headline read does not break the rest "
        "of the page -- was dropped along with the false one")
    assert "dependency" in doc, (
        "the docstring does not say WHY the body waits for part of this read")


# ---------------------------------------------------------------------------
# G4 -- RETIRED 2026-08-06, with ONE assertion carried forward.
#
# This block held four tests about the candidacy switch and the divergence chip
# being two controls over ONE column: that they shared a card, that the chip was
# a chip and not a link, that the pair went inert together, and that
# `normalise_state` settled it via `findings_divergence_offered`.
#
# All four premises are gone. The four-state selector replaced BOTH controls
# with a single-select that cannot express an incoherent combination at all, so
# there is no pair to settle, no inert state to announce, and no shared card --
# `normalise_state` is a documented no-op and `findings_divergence_offered` is
# not even imported by the page. Tests asserting those mechanics would now be
# asserting the absence of a design rather than the presence of one, which is
# what the selector's own tests above do directly and better.
#
# WHAT SURVIVES is the one assertion in that block that was never about the two
# controls: the page must not carry a stored novelty shade as a literal. That
# rule outlived its original host and is kept below, on its own, because a shade
# literal in the page is a second definition of vocabulary the service owns --
# and the selector made the page MORE likely to grow one, not less, since it now
# reasons about shades on every unit.
#
# The pool control keeping its own card (ruling T) is likewise still asserted,
# by `test_the_pool_card_names_its_axis_and_carries_the_rule_that_decides_it`.
# ---------------------------------------------------------------------------

def test_no_stored_novelty_shade_is_a_literal_in_the_page():
    """The service owns the shade vocabulary; the page names views, not shades.

    Carried forward from the retired G4 block (see above). A literal here would
    be a second definition of `novelty_status`'s values living in a surface --
    the same class of duplication `bucket_name` and the relation vocabulary are
    each guarded against elsewhere in this file.
    """
    import inspect

    source = inspect.getsource(fp)
    for shade in ("diverges_work", "diverges_part", "fills_gap"):
        assert f'"{shade}"' not in source and f"'{shade}'" not in source, (
            f"{shade!r} is a literal in the page -- the shade vocabulary "
            "belongs to shared/discovery_service.py, and the page addresses it "
            "through NOVELTY_VIEWS / novelty_view_shades()")


# ---------------------------------------------------------------------------
# The expansion must carry the CITATION RANGE.
#
# `_child_state` copies the reader's whole filter state, and its docstring
# promises "every axis is carried over unchanged ... its count and the rows
# underneath it come from ONE predicate and cannot contradict each other".
# `_fetch_children` nonetheless never read `locus_from` / `locus_to` back out of
# that dict, so a range-filtered parent opened onto children drawn from an
# UNFILTERED predicate. It was unreachable until 2026-08-20 only because the
# parent query timed out first (the correlated locus predicate in
# shared/discovery_service.py), so repairing that query is what exposed it.
# ---------------------------------------------------------------------------

def _capture_children_request(monkeypatch, state, item, axis_value="wA"):
    """The kwargs `_fetch_children` hands the shipped read, captured."""
    seen = {}

    async def _fake_get_findings_enveloped(unit, **kwargs):
        seen["unit"] = unit
        seen.update(kwargs)
        return {"status": "ok", "items": [], "total": 0, "meta": {}}

    monkeypatch.setattr(fp, "get_findings_enveloped", _fake_get_findings_enveloped)
    asyncio.run(fp._fetch_children(state, item))
    return seen


def _range_state(**overrides):
    state = {
        "unit": "work",
        "bucket": "main",
        "sort": "band_rank",
        "page": 1,
        "domain": None,
        "author": None,
        "work_id": "wA",
        "locus_from": 3,
        "locus_to": 7,
        "sys_id": None,
    }
    state.update(overrides)
    return state


def test_the_expansion_carries_the_citation_RANGE_not_only_the_work(monkeypatch):
    """A range-filtered parent must not open onto unfiltered children."""
    state = _range_state()
    item = {"unit": "work", "display_work_id": "wA"}
    seen = _capture_children_request(monkeypatch, state, item)

    assert seen.get("work_id") == "wA", (
        "the expansion lost the work pin, so this test is not exercising the "
        "path it claims to")
    assert seen.get("locus_from") == 3 and seen.get("locus_to") == 7, (
        "the expansion dropped the citation range: the parent row was counted "
        "under a range filter and its children were not, so the count above "
        "the list and the list itself describe different populations")


def test_the_expansion_passes_no_range_when_the_reader_set_none(monkeypatch):
    """The forwarding must be the reader's state, not an invented default.

    Guards the opposite error: hardcoding a range, or defaulting it to
    something, would narrow an expansion the reader never narrowed.
    """
    state = _range_state(locus_from=None, locus_to=None)
    item = {"unit": "work", "display_work_id": "wA"}
    seen = _capture_children_request(monkeypatch, state, item)
    assert seen.get("locus_from") is None and seen.get("locus_to") is None


def test_a_one_sided_citation_bound_survives_the_expansion(monkeypatch):
    """`locus_from` alone and `locus_to` alone are both real reader states."""
    item = {"unit": "work", "display_work_id": "wA"}
    only_from = _capture_children_request(
        monkeypatch, _range_state(locus_to=None), item)
    assert only_from.get("locus_from") == 3 and only_from.get("locus_to") is None
    only_to = _capture_children_request(
        monkeypatch, _range_state(locus_from=None), item)
    assert only_to.get("locus_from") is None and only_to.get("locus_to") == 7

# ===========================================================================
# Owner reports 2026-08-21 (2) the progress card that never went away, and
# (3) no warning before a very large download.
# ===========================================================================

#: A complete view, as `export_query_params` expects one. Written out rather
#: than trimmed to the keys that happen to be read today: a state missing a key
#: raises inside `start_export`, and the first version of these tests hid that
#: behind the handshake's own `except`.
_VIEW = {"unit": "identification", "bucket": "main", "sort": "band_rank"}


class _FakeNotification:
    def __init__(self, *a, **kw):
        self.kwargs = kw
        self.dismissed = 0

    def dismiss(self):
        self.dismissed += 1


def _patched_download(monkeypatch, *, js):
    """Patch the three `ui` calls `start_export` makes and record them."""
    seen = {"downloads": [], "notes": [], "js": []}

    def _notification(*a, **kw):
        note = _FakeNotification(*a, **kw)
        seen["notes"].append(note)
        return note

    async def _run_javascript(code, **kw):
        seen["js"].append(code)
        return js()

    monkeypatch.setattr(fp.ui, "notification", _notification)
    monkeypatch.setattr(fp.ui, "download", lambda url: seen["downloads"].append(url))
    monkeypatch.setattr(fp.ui, "run_javascript", _run_javascript)
    return seen


@pytest.mark.parametrize("js,label", (
    (lambda: "done", "the handshake resolved"),
    (lambda: "timeout", "the handshake timed out"),
    (lambda: (_ for _ in ()).throw(RuntimeError("socket gone")), "the client vanished"),
))
def test_the_progress_card_is_dismissed_on_every_path(monkeypatch, js, label):
    """A `ui.download` is a browser navigation: nothing reports back, so the old
    `type="ongoing"` notify (Quasar `timeout: 0`) had no event that could close
    it and stayed on screen after the file had landed.

    THE PROPERTY IS "DISMISSED ON EVERY PATH", so all three are driven --
    including the one where the client goes away mid-wait, which is where a
    `try/except` around the wrong statement would leave the card up.
    """
    seen = _patched_download(monkeypatch, js=js)
    asyncio.run(fp.start_export(dict(_VIEW), "en"))

    assert len(seen["notes"]) == 1, "no progress card was raised"
    note = seen["notes"][0]
    assert note.kwargs.get("timeout") is None, (
        "a card with its own timeout is a card that lies about how long the "
        "build takes")
    assert note.dismissed == 1, f"the card survived {label}"


def test_a_download_that_cannot_start_clears_the_card_and_still_reports(
        monkeypatch):
    """The card must come down, and the failure must NOT come down with it.

    The first version wrapped `ui.download` in the same `except` that makes the
    handshake best-effort, so a download that never started tidied its own
    spinner away and told nobody -- a reader watching a card disappear with no
    file has no way to know anything went wrong. Only the WAIT is best-effort.
    """
    seen = _patched_download(monkeypatch, js=lambda: "done")

    def _boom(url):
        raise RuntimeError("no client")

    monkeypatch.setattr(fp.ui, "download", _boom)
    with pytest.raises(RuntimeError):
        asyncio.run(fp.start_export(dict(_VIEW), "en"))
    assert seen["notes"][0].dismissed == 1, "the card outlived the failure"


def test_declining_the_large_download_warning_starts_nothing(monkeypatch):
    """The other half of the warning. Asked directly because the rendered
    capture never paints it: a reader who says no must get NO download and NO
    progress card, and a confirmation that proceeds anyway is worse than no
    confirmation at all."""
    seen = _patched_download(monkeypatch, js=lambda: "done")
    started = []

    async def _decline(state, lang, rows):
        return False

    async def _start(state, lang):
        started.append((state, lang))

    monkeypatch.setattr(fp, "_confirm_large_export", _decline)
    monkeypatch.setattr(fp, "start_export", _start)

    async def _drive():
        # THE REAL DECISION FUNCTION, not a copy of its condition: a test that
        # rebuilt the branch beside the code would agree with itself for
        # exactly as long as the two happened to match.
        if await fp.should_start_export(dict(_VIEW), "en", 50_000):
            await fp.start_export(dict(_VIEW), "en")

    asyncio.run(_drive())
    assert started == [], "a declined confirmation started the download anyway"
    assert seen["downloads"] == [] and seen["notes"] == []

    # ... and a small view goes straight through, or the warning is a wall.
    async def _small():
        assert await fp.should_start_export(dict(_VIEW), "en", 10) is True

    asyncio.run(_small())


def test_the_download_carries_a_handshake_token_the_watcher_looks_for(monkeypatch):
    """The token is the only thing tying the response back to this page, so the
    URL and the poll must agree on it. A test that checked either alone would
    pass with the two out of step, which is the silent version of the bug."""
    seen = _patched_download(monkeypatch, js=lambda: "done")
    asyncio.run(fp.start_export(dict(_VIEW), "en"))

    url = seen["downloads"][0]
    token = _re.search(r"[?&]dl=([0-9a-f]+)", url)
    assert token, f"no handshake token in {url}"
    code = seen["js"][0]
    assert f"gs_dl_{token.group(1)}=" in code, (
        "the watcher is polling for a different cookie than the URL minted")
    assert "__TOKEN__" not in code and "__MS__" not in code, (
        "a placeholder survived into the script")
    # It clears the cookie after seeing it: a sticky cookie would dismiss the
    # NEXT download's card instantly.
    assert "Max-Age=0" in code


def test_the_route_signals_completion_and_ignores_a_forged_token():
    """The cookie name is built from a query parameter, so the token has to be
    anchored hex or it is header injection. Ignored rather than rejected: the
    handshake is a convenience, and a malformed token should cost a spinner
    that times out, not the reader's download."""
    from starlette.responses import Response

    import web.api as api

    ok = api.stamp_download_done(Response("x"), "deadbeef01")
    assert "gs_dl_deadbeef01=1" in (ok.headers.get("set-cookie") or "")
    for forged in ("nothex", "dead; injected=1", "DEADBEEF", "", None, "a" * 200):
        bad = api.stamp_download_done(Response("x"), forged)
        assert bad.headers.get("set-cookie") is None, (
            f"a forged token {forged!r} reached Set-Cookie")


@pytest.mark.parametrize("total,expected", (
    (None, False), ("", False), (0, False), (1999, False), (2000, True),
    (28635, True),
))
def test_only_a_large_view_asks_before_downloading(total, expected):
    """Pure, so the threshold is assertable without a browser. Chosen from the
    measurement rather than from taste: a 28,635-row build takes ~66 s plus its
    walk and the cost is close to linear."""
    assert fp._export_is_large(total) is expected


@pytest.mark.parametrize("lang", ("en", "he"))
def test_the_grouped_warning_never_calls_the_group_count_a_row_count(lang):
    """"{rows} rows" is FALSE on a grouped view: the file is one row per
    identification, so a view of 3,000 works is tens of thousands of rows.
    The two sentences are deliberately different, and this is the difference."""
    plain = fp.copy_text("export_confirm_body", lang)
    grouped = fp.copy_text("export_confirm_body_grouped", lang)
    assert plain and grouped and plain != grouped
    assert "{rows}" in plain and "{rows}" in grouped
    assert "{unit}" in grouped and "{unit}" not in plain
    # The grouped sentence must say the file is BIGGER than the number it
    # quotes -- otherwise quoting the number at all is misleading.
    bigger = {"en": ("more rows",), "he": ("הרבה יותר",)}[lang]
    assert any(phrase in grouped for phrase in bigger), (
        f"the grouped warning does not say the file exceeds {{rows}}: {grouped!r}")
    for key in ("export_confirm_title", "export_confirm_go",
                "export_confirm_cancel"):
        assert fp.copy_text(key, lang), f"{key} is empty in {lang}"
