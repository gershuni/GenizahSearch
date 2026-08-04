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


def _render_page(monkeypatch, *, lang="en", findings=None, facets=None, state=None):
    """Render the REAL create_findings_page() in a bare client context.

    Returns the NiceGUI Client; tests walk `client.elements` directly."""
    _ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    monkeypatch.setattr(fp, "get_findings_enveloped", findings or _fake_findings())
    monkeypatch.setattr(fp, "get_findings_facets_enveloped", facets or _fake_facets())
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

def test_module_adds_no_nested_offload_and_no_direct_service_call():
    """(i) no run.io_bound, (ii) no access to the composition module's private
    singleton, (iii) no call to a service-module symbol, (iv) every discovery
    read is an await on a name imported from web.discovery.

    tests/test_no_await_sync_function.py passes too, but it detects ONLY an
    `await` on a LOCALLY defined synchronous `def` and could not have caught any
    of these four failure modes."""
    tree = ast.parse(FINDINGS_SRC)

    # (i) + (ii)/(iii): the plan's own gate, verbatim.
    assert "io_bound" not in FINDINGS_SRC, "(i) nested offload: run.io_bound"
    assert "_service" not in FINDINGS_SRC, (
        "(ii)/(iii) the module names the service module or its private singleton"
    )

    # (iv) every awaited discovery read resolves to a web.discovery import.
    web_discovery_names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "web.discovery":
            web_discovery_names.update(alias.asname or alias.name for alias in node.names)
    assert {"get_findings_enveloped", "get_findings_facets_enveloped"} <= web_discovery_names

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
        assert name in web_discovery_names or name in local_async or name == "refresh", (
            f"awaited call {name!r} at line {node.lineno} is neither a web.discovery "
            "wrapper nor a local async helper"
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
    from nicegui import core
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
                        user.find(bucket_name(False, lang)).click()

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
