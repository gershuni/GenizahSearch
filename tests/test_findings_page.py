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
def test_mode_strip_renders_three_modes_with_two_inert_and_phase_tagged(monkeypatch, lang):
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
        assert _tr("Phase 137") in tags and _tr("Phase 138") in tags, (
            f"the future modes must be phase-tagged; got {tags!r}"
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
    136-18 adding a read does not turn this criterion red."""
    import web.discovery as wd

    ok_findings = {"status": "ok", "items": [], "total": 0,
                   "meta": {"unit": "identification", "bucket": "main",
                            "sort": "band_rank", "approximate_total": False}}
    ok_facets = {"status": "ok", "items": [], "total": 0, "meta": {"level": "domain"}}
    reads = []

    async def _counting_findings(*args, **kwargs):
        reads.append("findings")
        return await wd.get_findings_enveloped(*args, **kwargs)

    async def _counting_facets(*args, **kwargs):
        reads.append("facets")
        return await wd.get_findings_facets_enveloped(*args, **kwargs)

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
                stack.enter_context(patch.object(fp, "get_findings_enveloped", _counting_findings))
                stack.enter_context(patch.object(fp, "get_findings_facets_enveloped", _counting_facets))
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
# STATUS AS SHIPPED BY PLAN 136-16: **NOT MET.** Playwright is not installed in
# the execution environment (`scripts/capture_atlas_html.py` documents it as an
# ad-hoc dev/ops tool, deliberately absent from every requirements file), and a
# package install is outside what an executor may do unattended. The criterion
# is therefore recorded NOT MET in 136-16-SUMMARY.md — never as a pass, and the
# skip below is a CI-hygiene mechanism, not a claim about the criterion.
#
# The check itself is written and runnable: set GENIZAH_FINDINGS_BROWSER_CHECK=1
# with Playwright + Chromium installed and a server reachable at
# GENIZAH_FINDINGS_BROWSER_BASE_URL, and it exercises BOTH viewport widths in
# BOTH languages, then runs its own positive control. With the env var SET and
# the tooling ABSENT it FAILS — it never degrades to a silent green, which is
# the same fail-closed posture this phase applies to the masking scan.
# ---------------------------------------------------------------------------

_BROWSER_CHECK_ENV = "GENIZAH_FINDINGS_BROWSER_CHECK"
_BROWSER_BASE_URL_ENV = "GENIZAH_FINDINGS_BROWSER_BASE_URL"

#: Criterion (e) names both widths explicitly: a phone and a desktop.
_BROWSER_VIEWPORTS = ((375, 812), (1440, 900))


def _playwright_available() -> bool:
    try:
        import playwright.sync_api  # noqa: F401
    except Exception:
        return False
    return True


def _browser_actionability_probe(page, control_name: str) -> None:
    """Assert the browser's OWN actionability conditions hold at the control's
    locator (visible, stable, enabled, receiving pointer events at its hit
    point), then perform a real click, then assert the results region changed.

    This is the only check that can see a collapsed ancestor, a zero-height box,
    a clip or an overlay — none of which a DOM-ancestry assertion can."""
    locator = page.get_by_role("button", name=control_name, exact=True).first
    before = page.locator(f".{fp.RESULTS_CLASS}").inner_html()
    # No preceding disclosure action: click straight away. Playwright's own
    # actionability checks run inside click() and raise on failure.
    locator.click(timeout=5000)
    page.wait_for_timeout(750)
    after = page.locator(f".{fp.RESULTS_CLASS}").inner_html()
    assert after != before, (
        "the results region did not change after a real browser click on "
        f"{control_name!r}"
    )


def run_browser_actionability_check(base_url: str) -> None:
    """(e) at 375px and desktop, in both languages, plus (f) its positive
    control — a deliberately collapsed ancestor that must make the SAME check
    fail with an actionability error. Without (f), (e) is a check nobody has
    watched fail."""
    from playwright.sync_api import Error as PlaywrightError
    from playwright.sync_api import TimeoutError as PlaywrightTimeout
    from playwright.sync_api import sync_playwright

    from shared.discovery_display_strings import bucket_name

    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            for lang in ("en", "he"):
                for width, height in _BROWSER_VIEWPORTS:
                    context = browser.new_context(viewport={"width": width, "height": height})
                    page = context.new_page()
                    page.goto(f"{base_url}{FINDINGS_ROUTE}?lang={lang}", wait_until="networkidle")
                    _browser_actionability_probe(page, bucket_name(False, lang))

                    # (f) POSITIVE CONTROL, same page, same locator: collapse an
                    # ancestor and confirm the check FAILS with an actionability
                    # error rather than passing anyway.
                    page.eval_on_selector(
                        f".{fp.BUCKET_CONTROL_CLASS}",
                        "el => el.parentElement.style.display = 'none'",
                    )
                    failed = False
                    try:
                        _browser_actionability_probe(page, bucket_name(False, lang))
                    except (PlaywrightTimeout, PlaywrightError, AssertionError):
                        failed = True
                    page.eval_on_selector(
                        f".{fp.BUCKET_CONTROL_CLASS}",
                        "el => el.parentElement.style.display = ''",
                    )
                    assert failed, (
                        "POSITIVE CONTROL DID NOT FIRE: the browser actionability "
                        f"check passed at {width}px in {lang} with an ancestor of "
                        "the control collapsed. The check is not watching what it "
                        "claims to watch."
                    )
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
