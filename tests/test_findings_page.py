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
import subprocess as _subprocess  # noqa: E402
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
                 launch=None):
    """Render the REAL create_findings_page() in a bare client context.

    Returns the NiceGUI Client; tests walk `client.elements` directly."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    monkeypatch.setattr(fp, "get_findings_enveloped", findings or _fake_findings())
    monkeypatch.setattr(fp, "get_findings_facets_enveloped", facets or _fake_facets())
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
def test_filter_with_missing_backing_data_renders_disabled_with_the_amber_tag(monkeypatch, lang):
    """Never silently absent: a filter that vanishes is indistinguishable from a
    filter that never existed."""
    client = _render_page(monkeypatch, lang=lang)

    blocked = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-coverage")
    assert len(blocked) == 1, "the coverage filter must be RENDERED, not absent"
    classes = blocked[0]._classes or []
    assert "fg" in classes and "blocked" in classes, (
        f"a filter with no backing data must carry `fg blocked`; got {classes!r}"
    )
    tag_text = "\n".join(
        t for el in _elements_with_class(client, "needs") for t in _subtree_strings(el)
    )
    assert fp.copy_text("needs_tag", lang) in tag_text, (
        "the disabled filter must carry the amber tag"
    )
    chips = [el for el in blocked[0].descendants() if getattr(el, "enabled", True) is False]
    assert chips, "the disabled filter's control must actually be disabled"


@pytest.mark.parametrize("lang", ["en", "he"])
def test_facet_group_with_an_outage_renders_disabled_rather_than_absent(monkeypatch, lang):
    client = _render_page(monkeypatch, lang=lang, facets=_fake_facets(status="unavailable"))
    for level in ("domain", "author", "work"):
        blocked = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-{level}-blocked")
        assert blocked, f"the {level} facet vanished on an outage instead of disabling"
        assert "blocked" in (blocked[0]._classes or [])


# ---------------------------------------------------------------------------
# Ruling R — work-facet labels
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("lang", ["en", "he"])
def test_work_facet_label_routes_through_display_work_title(monkeypatch, lang):
    from shared.discovery_display_strings import display_work_title

    client = _render_page(monkeypatch, lang=lang)
    items = _elements_with_class(client, f"{fp.FILTER_BAR_CLASS}-work-items")
    assert items, "the work facet list did not render"
    labels = _subtree_strings(items[0])

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
}


def test_module_adds_no_nested_offload_and_no_direct_service_call():
    """(i) no run.io_bound, (ii) no access to the composition module's private
    singleton, (iii) no call to a service-module symbol, (iv) every awaited read
    is a name imported from one of the two declared offloading wrapper modules.

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
    "novelty_only": False, "domain": None, "author": None, "work_id": None,
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


def test_this_plan_adds_no_css():
    """The discovery CSS block was landed once, in plan 136-10."""
    result = _subprocess.run(
        ["git", "diff", "--stat", "HEAD", "--", "web/static/common.css"],
        capture_output=True, text=True,
        cwd=str(pathlib.Path(__file__).resolve().parents[1]),
    )
    assert result.stdout.strip() == "", (
        f"web/static/common.css was modified by this plan: {result.stdout!r}"
    )


# ---------------------------------------------------------------------------
# (c) The INTERACTION test — through the NiceGUI User simulation, never by
# calling the handler directly. Marked render_smoke: it enters and tears down
# the app lifespan, which is not safe to interleave with the bulk suite.
# ---------------------------------------------------------------------------

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
                        await user.should_see(MORE_ROW_TITLE)
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
_INTERACTION_ROW_TITLES = {
    ("identification", False): "SEQ-IDENTIFICATION-ALL",
    ("identification", True): "SEQ-IDENTIFICATION-CANDIDATES",
    ("manuscript", False): "SEQ-MANUSCRIPT-ALL",
    ("manuscript", True): "SEQ-MANUSCRIPT-CANDIDATES",
    ("work", False): "SEQ-WORK-ALL",
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
                    domain=None, author=None, work_id=None, sort="band_rank",
                    page=1, page_size=50):
        # Raises for an unreachable-by-contract combination, exactly as shipped.
        _build_findings_query(
            unit=unit, sort=sort, bucket=bucket, novelty=novelty, domain=domain,
            author=author, work_id=work_id, page=page, page_size=page_size)
        if recorder is not None:
            recorder.append({"unit": unit, "novelty": novelty, "bucket": bucket})
        title = _INTERACTION_ROW_TITLES[(unit, bool(novelty))]
        row = dict(_finding_row("w000001", title, "T-S 12.111"), unit=unit)
        return {
            "status": "ok", "items": [row], "total": 1,
            "meta": {"unit": unit, "bucket": bucket, "sort": sort,
                     "sort_basis": "best_band_rank",
                     "novelty_offered": unit != "work",
                     "approximate_total": False},
        }

    return _call


@pytest.mark.render_smoke
@pytest.mark.parametrize("lang", ["en", "he"])
def test_turning_candidates_on_then_switching_to_one_row_per_work_does_not_break_the_page(lang):
    """THE READER SEQUENCE, end to end, through the simulated user.

    Turn the candidacy switch on; then change "Show as" to one row per work.
    Both are first-class controls a reader reaches for, and neither warns about
    the other.

    Three things must hold afterwards, and the first is the one the round-15
    finding was about:

      1. NOTHING RAISED. `handle_event` routes an exception from an async
         handler into `core.app.handle_exception`, where it becomes a log line
         and nothing else — so a raising page looks, from the outside, exactly
         like a page that did not respond. The recorder makes that difference
         visible.
      2. THE PAGE RENDERED THE NEW UNIT. Not "did not crash": the work-unit row
         must actually be on screen and the previous result set gone, which
         proves the refresh ran to completion rather than dying half-way.
      3. THE CONTROL AGREES WITH THE QUERY. The switch is off and disabled, it
         carries `aria-pressed=false`, its card is dimmed and the reason is
         rendered in words. A filter that silently stopped filtering while still
         showing itself as on would be a worse bug than the crash.
    """
    import httpx
    from nicegui import core, ui
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
                        # The reason is not shown while the axis applies.
                        await user.should_not_see(fp.copy_text("novelty_unit_note", lang))

                        # (1) The reader turns candidates on.
                        user.find(words["toggle"]).click()
                        await user.should_see(_INTERACTION_ROW_TITLES[("identification", True)])
                        await user.should_not_see(_INTERACTION_ROW_TITLES[("identification", False)])

                        # (2) ...and then changes the row unit to one row per
                        # work, through the select's own popup.
                        unit_label = tr("One row per work")
                        user.find(tr("Show as")).click()
                        user.find(unit_label).click()

                        # (2a) The new unit is RENDERED and the old set is gone.
                        await user.should_see(_INTERACTION_ROW_TITLES[("work", False)])
                        await user.should_not_see(
                            _INTERACTION_ROW_TITLES[("identification", True)])
                        # (2b) ...and the reason the switch went quiet is on the page.
                        await user.should_see(fp.copy_text("novelty_unit_note", lang))

                        switches = [
                            element for element in user.client.elements.values()
                            if isinstance(element, ui.button)
                            and getattr(element, "text", None) == words["toggle"]
                        ]
                        assert len(switches) == 1, (
                            f"expected exactly one candidacy switch, found {len(switches)}")
                        switch = switches[0]
                        assert switch.enabled is False, (
                            "the candidacy switch is still clickable on the per-work "
                            "unit — one more click puts the page back in the state "
                            "the service refuses")
                        assert switch.props.get("aria-pressed") == "false", (
                            "the switch still announces itself as pressed while the "
                            "query no longer applies it")
                        cards = [
                            element for element in user.client.elements.values()
                            if f"{fp.FILTER_BAR_CLASS}-novelty" in (element._classes or [])
                        ]
                        assert cards and "blocked" in (cards[0]._classes or []), (
                            "the inactionable filter card is not dimmed — it looks "
                            "exactly like the ones that still work")

                        # (3) A further click on the disabled switch is inert:
                        # the simulated user skips disabled elements, and the
                        # handler refuses the state anyway.
                        user.find(words["toggle"]).click()
                        await user.should_see(_INTERACTION_ROW_TITLES[("work", False)])
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
    # The work-unit read really was issued, and it carried NO novelty selection.
    work_reads = [call for call in issued if call["unit"] == "work"]
    assert work_reads, "the page never issued a per-work read"
    assert all(call["novelty"] is None for call in work_reads), (
        f"a per-work read carried a novelty selection: {work_reads!r}")


def test_the_axis_rule_is_the_services_own_predicate_and_is_not_restated(monkeypatch):
    """One authority for "does this unit offer novelty", used by three callers.

    `shared.discovery_service.findings_novelty_offered` is what the BUILDER
    raises on, what the ENVELOPE reports as `meta['novelty_offered']`, and what
    the PAGE disables its switch on. Flipping the predicate must move all three
    together — a page that hard-codes `unit != "work"` is a page that keeps its
    switch live the day the service changes its mind."""
    import shared.discovery_service as svc

    assert fp.findings_novelty_offered is svc.findings_novelty_offered, (
        "the page imported something other than the service's own predicate")
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

    # ...and with the predicate flipped, the page's own normalisation follows.
    monkeypatch.setattr(fp, "findings_novelty_offered", lambda unit: unit == "work")
    assert fp.normalise_state(_state(unit="work", novelty_only=True))["novelty_only"] is True
    assert (fp.normalise_state(_state(unit="identification", novelty_only=True))
            ["novelty_only"] is False)


def test_normalise_state_drops_a_persisted_pair_the_service_would_refuse(monkeypatch):
    """The stored pair, not just the live one. A cookie written before this fix
    (or edited by hand) carries `unit=work` AND `novelty_only=True`, and
    `read_state` must settle it before it reaches a query."""
    store = {
        "discovery_findings_unit": "work",
        "discovery_findings_novelty_only": True,
    }
    monkeypatch.setattr(fp, "safe_user_get", lambda k, d=None: store.get(k, d))
    monkeypatch.setattr(fp, "safe_user_set", lambda k, v: None)

    restored = fp.read_state()
    assert restored["unit"] == "work"
    assert restored["novelty_only"] is False, (
        "a persisted work+candidates pair survived read_state — it reaches "
        "fetch_findings and the shipped builder raises on it")
    assert fp._novelty_selection(restored) is None


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
    state = _state(unit="work", novelty_only=True)
    asyncio.run(fp.fetch_facets("domain", state))
    assert captured["domain"]["novelty"] is None, (
        "the facet cascade applied a candidacy filter the per-work result set "
        "could not — the counts would not describe the rows beside them")


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
        chip.click(timeout=_CLICK_TIMEOUT_MS)
        page.wait_for_timeout(750)


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
    page.wait_for_timeout(750)
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
        "novelty_only": False, "domain": None, "author": None, "work_id": None,
        "page": 1,
    }
    base.update(overrides)
    return base


def _select_elements(client, marker: str) -> list:
    return _elements_with_class(client, marker)


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
                          novelty_only=True, domain="Liturgy", page=3))
    assert store["discovery_findings_bucket"] == "more"
    assert store["discovery_findings_sort"] == "page_count"
    assert store["discovery_findings_unit"] == "manuscript"
    assert store["discovery_findings_novelty_only"] is True
    assert store["discovery_findings_domain"] == "Liturgy"
    assert store["discovery_findings_page"] == 3

    restored = fp.read_state()
    assert restored["bucket"] == "more"
    assert restored["sort"] == "page_count"
    assert restored["unit"] == "manuscript"
    assert restored["novelty_only"] is True
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
    asyncio.run(fp.fetch_findings(_state(novelty_only=True)))
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
    "-novelty", "-coverage", "-domain", "-author", "-work",
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
    shade_parents = {s.parent_slot.parent for s in shades}
    assert len(shade_parents) == 1, "the three shades are still stacked"
    assert shade_parents.pop() is not row, (
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

    figures = guard.forbidden_figures()
    key_names = guard.envelope_key_names(_launch_envelope(with_lede=True))
    root = pathlib.Path(__file__).resolve().parents[1]
    violations = guard.scan_launch_literals(root, figures, key_names)
    assert not violations, "launch figures found as literals: " + "; ".join(
        v.message() for v in violations)
    # The sentinels above must never BE real figures, or these tests would be
    # agreement rather than provenance.
    assert _LEDE_SENTINEL_TOTAL not in figures
    assert _LEDE_SENTINEL_MANUSCRIPTS not in figures


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
    blob = "\n".join(_node_texts(client, f"{fp.FILTER_BAR_CLASS}-work-items"))
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


def _drive_facet_rounds(monkeypatch, rounds, *, lang="en", state=None):
    """Build a REAL filter bar, then run `_populate_facets` once per round.

    Returns `[(label, [levels read]), ...]` plus the client, so an assertion can
    be about which levels issued a READ and about what the containers now hold.

    `rounds` is a list of `(label, mutation)`; each mutation is applied to the
    shared state dict before that round runs, so the rounds compose exactly the
    way a reader's successive interactions do.
    """
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    reads: list = []

    async def _recording(level, **_kwargs):
        reads.append(level)
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
        ("candidacy on", {"novelty_only": True}),
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

_ALL_FILTERS = dict(novelty_only=True, domain="Liturgy", author="Maimonides",
                    work_id=_CURATED_WORK_ID, work_label=_CURATED_RAW_TITLE)


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
    assert state["novelty_only"] is True
    assert state["page"] == 1

    fp._clear_filter_axis(state, "work_id")
    assert state["work_id"] is None
    assert state.get("work_label") is None, (
        "the work chip's label outlived the selection it labelled")
    assert state["author"] == "Maimonides"

    fp._clear_filter_axis(state, None)          # clear all
    assert state["author"] is None
    assert state["novelty_only"] is False
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
        tmp_path):
    """The cross-check the figure has to survive to be an invitation at all: a
    reader who follows it must land on the number it advertised.

    The two sides are TWO SEPARATELY-WRITTEN QUERIES against one artifact: the
    launch reader's `COUNT(*) ... WHERE main_pool = 0`, and the total the
    corpus-wide FINDINGS query reports for `bucket='more'` -- the very query
    that produces the rows the reader lands on, with its own unit, routing and
    visibility clauses. Comparing the advertised figure against a re-run of its
    own SQL would compare a number with itself.

    Runs on a SYNTHETIC artifact first, so it holds in every environment, and
    then on the real one when it resolves -- an assertion that only ever skips
    is not a check.
    """
    from shared.discovery_service import DiscoveryService
    from tests.test_discovery_launch_stats import (
        _EXPECTED_PAGES,
        _POPULATED_ROWS,
        _build_launch_db,
        resolve_guard_artifact,
    )

    def _agree(path, version):
        service = DiscoveryService(
            path_provider=lambda: path,
            availability_callable=lambda: True,
            sidecar_version_provider=lambda: version,
        )
        envelope = service.get_launch_stats_enveloped()
        assert envelope["status"] == "ok"
        advertised = envelope["meta"]["more_pool_total"]
        landed = service.get_findings_enveloped(bucket="more")
        assert landed["status"] == "ok"
        assert advertised == landed["total"], (
            f"{path}: the invitation would advertise {advertised} and the "
            f"reader would land on {landed['total']}")
        return advertised

    synthetic = _build_launch_db(
        tmp_path / "invite-cross-check.db", _POPULATED_ROWS, pages=_EXPECTED_PAGES)
    assert _agree(synthetic, "synthetic") > 0

    path, reason = resolve_guard_artifact()
    if path is None:
        pytest.skip(
            (reason or "no resolvable discovery artifact") +
            " -- the synthetic half of this check ran; set "
            "DISCOVERY_LAUNCH_GUARD_DB to run it against the served artifact")
    assert _agree(path, "real") > 0


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

                        await user.should_see(_FACET_MORE_SENTINEL)
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
                        await user.should_see(_ROW_FILTERED)
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
                        await user.should_see(_ROW_UNFILTERED)
                        await user.should_not_see(_ROW_FILTERED)

                        # (3) pick it again, then use Clear all.
                        user.find(kind=ui.button, content="Liturgy").click()
                        await user.should_see(_ROW_FILTERED)
                        user.find(kind=ui.button, content=tr("Clear All")).click()
                        await user.should_see(_ROW_UNFILTERED)
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
