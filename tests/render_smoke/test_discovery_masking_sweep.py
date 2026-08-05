# -*- coding: utf-8 -*-
"""The FINAL cross-surface masking sweep (Phase 136, plan 136-19, NOVEL-02).

D-25 says no restricted-corpus name may be reachable from any surface. A leak
does not care which door it leaves by, so this sweep drives the existing DATA-05
gate (`scripts/check_atlas_masking.py`) over FOUR egress classes rather than one:

  1. RENDERED output -- both surfaces, both languages, every service state,
     every row unit, both buckets, and the surfaces painted only after a click.
  2. JSON PAYLOADS -- every enveloped read either surface consumes, DERIVED from
     the code rather than remembered, so a read added tomorrow and not scanned
     fails here by name.
  3. COPY / EXPORT paths -- the link targets a reader can copy, plus a derived
     inventory of the clipboard/download APIs the surfaces do NOT use. An
     absence that is asserted is a finding; an absence that is assumed is not.
  4. ERROR paths -- forced failures from both surfaces: exception messages, real
     log lines, and rendered error states. An exception message that
     interpolates a value IS a surface.

WHAT THIS ADDS BEYOND THE PER-SURFACE SCANS
-------------------------------------------
Plans 136-17 and 136-18 each scan their OWN surface. This is the cross-surface
pass and it is deliberately not a restatement:

* 136-17's `test_the_rendered_panel_and_repo_pass_the_real_masking_scan` covers
  the PANEL only, and its capture is reused here rather than rebuilt.
* 136-18 wrote `capture_rendered_output` for the findings page and then ran the
  scan over it BY HAND. Nothing called it. This sweep is the first automated
  caller -- until now the findings surface had no masking gate that could run
  itself.
* Neither per-surface suite scanned a COPY/EXPORT path or a LINK TARGET at all.
* Neither drove the findings page's CLICK handlers, so the facet tree, the
  selects, the pager and the bucket control -- every one of which paints
  artifact-derived text -- had never been in any masking capture.

WHY EVERY CHECK HERE IS BUILT TO FAIL LOUDLY
--------------------------------------------
This phase produced six instances of one defect: a gate that reported success
without performing its check (a skipped masking test, a browser capture that ran
no interactions, a dispatch assertion over zero dispatches, a "non-empty" check
counting the rows of a `COUNT(*)`, a skip list calling a reachable state
unreachable, and a masking capture whose "derived" coverage was a naming
convention). All six failed toward false confidence. So:

* an unset or unreadable `MASKING_SCAN_PATTERNS_FILE` FAILS this sweep. It is
  never a skip and never a pass, and it BLOCKS flag-on readiness
  (`masking_readiness`, below, is the one function that decides that);
* this module contains no `pytest.skip`, no `skipif` and no `xfail` -- pinned by
  a source assertion, because the first of the six defects was a skip;
* every path class has a POSITIVE CONTROL that seeds a marker generated AT TEST
  TIME from the pattern file and requires the scan to report THAT class's file
  and no other. "The suite went red" is not evidence that a particular class is
  covered;
* rendered coverage is derived from what the code CALLS (`ui.*`, transitively)
  and checked at LINE granularity, never from a list of function names someone
  remembered to keep up to date;
* `--scan-asset` alone is proved INSUFFICIENT for a SQLite sidecar by
  construction, not by quoting the scanner's docstring.

NOTHING RESTRICTED IS WRITTEN HERE. The needles come from the gitignored file
`MASKING_SCAN_PATTERNS_FILE` points at, at test time. A restricted name typed
into this file would be the exact leak the file exists to prevent.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import io
import json
import logging
import os
import pathlib
import subprocess
import sys
import types
from typing import Any, Dict, FrozenSet, List, Mapping, Optional, Set, Tuple

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# The scanner is imported FLAT (`check_atlas_masking`, not
# `scripts.check_atlas_masking`) so this module holds the SAME object the build
# script and the sibling masking suites hold -- two copies would have two
# `_ACTIVE_MATCHER` redactors and only one of them would be the live one.
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))
import check_atlas_masking as cam  # noqa: E402

import shared.discovery_display_strings as ds  # noqa: E402
import web.components.discovery_panel as dp  # noqa: E402
import web.components.findings_rows as fr  # noqa: E402
import web.pages.findings as fp  # noqa: E402
from shared.discovery_errors import DiscoveryOverload  # noqa: E402
from shared.discovery_surface_projection import (  # noqa: E402
    STATUS_OK,
    make_envelope,
)
from tests.render_smoke import test_findings_render_smoke as tf  # noqa: E402
from tests.render_smoke import test_panel_render_smoke as tp  # noqa: E402
from web.translations import tr  # noqa: E402

MASKING_SCRIPT = "scripts/check_atlas_masking.py"
SWEEP_PATH = "tests/render_smoke/test_discovery_masking_sweep.py"

LANGS = ("en", "he")

#: The four egress classes, each written to its OWN file so a positive control
#: can assert WHICH class the scanner reported rather than merely that it
#: reported something.
CLASS_RENDERED = "rendered"
CLASS_PAYLOADS = "json-payloads"
CLASS_COPY_EXPORT = "copy-export"
CLASS_ERROR_PATHS = "error-paths"
PATH_CLASSES = (CLASS_RENDERED, CLASS_PAYLOADS, CLASS_COPY_EXPORT, CLASS_ERROR_PATHS)

CLASS_FILES = {
    CLASS_RENDERED: "class-1-rendered.txt",
    CLASS_PAYLOADS: "class-2-json-payloads.txt",
    CLASS_COPY_EXPORT: "class-3-copy-export.txt",
    CLASS_ERROR_PATHS: "class-4-error-paths.txt",
}

#: The surface modules whose UI-emitting lines the rendered capture must reach.
#: The PANEL is covered at line granularity by 136-17's own suite; the two
#: findings modules had no line-level coverage anywhere before this sweep.
RENDERED_COVERAGE_MODULES = {
    "web/components/findings_rows.py": fr,
    "web/pages/findings.py": fp,
}

#: Clipboard / download / export egress APIs. A surface that gains one gains an
#: egress class this sweep does not capture, so the inventory below is asserted
#: to stay at zero rather than assumed to be.
_COPY_EXPORT_APIS = (
    "clipboard",
    "copy_to_clipboard",
    "ui.download",
    "download.content",
    "download.file",
    "StreamingResponse",
    "FileResponse",
    "to_csv",
    "to_excel",
    "writerow",
)


# ===========================================================================
# THE EXECUTOR-RUN DATABASE EVIDENCE.
#
# The three sidecars are gitignored and ~390-470 MB each, so no CI job can hold
# them and this record cannot be a CI gate. It is stated here, next to the
# checks that ARE gates, with its date and its measured numbers, so that the
# attestation quotes a measurement rather than a recollection -- and so that the
# limit of the automated sweep is visible in the same file as its strength.
# ===========================================================================

PRODUCTION_DATABASE_SCANS: Dict[str, Dict[str, Any]] = {
    "discovery-v1-e9365edcab27af7d0739ab1a07b1a187683993bcbff41ff88128c8fe4fbb7181.db": {
        "what": "the DEPLOYED public artifact (audience=public, manifest.deploy.json)",
        "modes": ("--strict", "--scan-repo", "--scan-asset", "--scan-sqlite"),
        "result": "clean",
        "seconds": 48,
        "date": "2026-08-05",
    },
    "discovery-public-136rebuild.db": {
        "what": "the public projection of the 136 private rebuild",
        "modes": ("--strict", "--scan-repo", "--scan-asset", "--scan-sqlite"),
        "result": "clean",
        "seconds": 49,
        "date": "2026-08-05",
    },
    "discovery-v1-136rebuild.db": {
        "what": "the PRIVATE 136 rebuild (never shipped; scanned because it is the "
                "source the public projection is derived from)",
        "modes": ("--strict", "--scan-repo", "--scan-asset", "--scan-sqlite"),
        "result": "clean",
        "seconds": 55,
        "date": "2026-08-05",
    },
    "discovery-v1-33499c5b89f9e635565cd1cc8831c012f5373811c2870ddbda7d303e60d4c5ff.db": {
        "what": "the artifact the repo's own manifest.json still resolves, i.e. what "
                "a LOCAL run of the app serves. Found by the manifest-derived check "
                "below, not by recollection: the first three were scanned and this "
                "one was not",
        "modes": ("--strict", "--scan-repo", "--scan-asset", "--scan-sqlite"),
        "result": "clean",
        "seconds": 50,
        "date": "2026-08-05",
    },
    "discovery-v1-89dfa4449543c91bf0b3c319603954c7dbdd5275df8b3fefa8849e40b9a90969.db": {
        "what": "a superseded bake still resolvable through manifest.v1-89dfa."
                "backup.json. Found by widening the manifest derivation from three "
                "remembered filenames to every manifest*.json on disk -- the second "
                "time in one session that a remembered list was narrower than the "
                "code it claimed to cover",
        "modes": ("--strict", "--scan-repo", "--scan-asset", "--scan-sqlite"),
        "result": "clean",
        "seconds": 47,
        "date": "2026-08-05",
    },
    "discovery-v1-8e43451300429ed4ace5e29e5513359a29674ac49731d5c969eb1d607e0ca065.db": {
        "what": "a superseded bake still resolvable through manifest.v1.json; found "
                "by the same widened derivation",
        "modes": ("--strict", "--scan-repo", "--scan-asset", "--scan-sqlite"),
        "result": "clean",
        "seconds": 46,
        "date": "2026-08-05",
    },
}

#: The measured proof that the 48-second clean runs above were not vacuous: a
#: value read OUT of the deployed artifact, fed back in as the whole pattern
#: set, and reported. A cell walk that never happened would report nothing here
#: exactly as it reports nothing on a clean run.
PRODUCTION_SQLITE_NON_VACUITY = {
    "needle from manuscript_display.sys_id": {"exit": 1, "hits": 11, "seconds": 32},
    "needle from discovery_evidence.novelty_status": {"exit": 1, "hits": 26480, "seconds": 32},
    "needle from a TABLE NAME (schema pass only)": {"exit": 1, "hits": 1, "seconds": 32},
}


def masking_readiness(*, pattern_set_loaded: bool,
                      modes_run: FrozenSet[str]) -> Tuple[str, str]:
    """THE ONE FUNCTION THAT DECIDES whether the D-25 masking item may be
    recorded as met in `136-FLAG-ON-READINESS.md`.

    `MET` requires BOTH: a real pattern set was loaded, and both database scan
    modes were run. An earlier draft of plan 136-19 allowed the sweep to
    complete with a noted skip while still claiming readiness -- which is
    precisely how a masking gate becomes decorative, and is the same shape as
    the D-25 test that SKIPPED when its pattern file was absent.

    Returns `(state, reason)`; `state` is `"MET"` or `"NOT MET"`.
    """
    if not pattern_set_loaded:
        return ("NOT MET",
                "no pattern set was loaded, so the scan searched for nothing and "
                "reports a clean surface it never inspected")
    missing = {"--scan-asset", "--scan-sqlite"} - set(modes_run)
    if missing:
        return ("NOT MET",
                "database coverage is incomplete: " + ", ".join(sorted(missing))
                + " did not run, and a raw byte scan cannot see a value that "
                  "straddles a SQLite overflow-page boundary")
    return ("MET", "a real pattern set was loaded and both scan modes ran")


# ===========================================================================
# THE NEEDLE. Generated at test time from the pattern file -- never typed here.
# ===========================================================================

class PatternSetUnavailable(RuntimeError):
    """Raised when `MASKING_SCAN_PATTERNS_FILE` yields no patterns.

    A hard error rather than a skip: see `masking_readiness`.
    """


def _pattern_set() -> List[str]:
    patterns = cam.load_patterns()
    if not patterns:
        configured = os.environ.get("MASKING_SCAN_PATTERNS_FILE")
        raise PatternSetUnavailable(
            "the D-25 cross-surface masking sweep cannot run: "
            f"MASKING_SCAN_PATTERNS_FILE is "
            f"{'set to a path that yields no patterns' if configured else 'unset'}.\n"
            "This is a FAILURE and not a skip on purpose -- a masking sweep that "
            "searches for no patterns reports four clean surfaces it never "
            "inspected, and `masking_readiness` therefore records the item as "
            "NOT MET and flag-on readiness is not claimed.\n"
            "To fix: write the restricted name/alias list to a gitignored file "
            "(one pattern per non-comment line) and point "
            "MASKING_SCAN_PATTERNS_FILE at it. In CI it is injected from the "
            "MASKING_SCAN_PATTERNS repository secret and deleted in an "
            "`if: always()` step; it is never committed."
        )
    return patterns


def _needle() -> str:
    """The longest real pattern -- the most distinctive one, so a control that
    fires cannot be firing on an incidental substring."""
    return max(_pattern_set(), key=len)


def _needle_pattern_file(tmp_path: pathlib.Path) -> pathlib.Path:
    """A one-pattern file holding ONLY the seeded needle, so a control's report
    is unambiguous about what was found."""
    path = tmp_path / "sweep_control_patterns"
    path.write_text(_needle() + "\n", encoding="utf-8")
    return path


# ===========================================================================
# DERIVATIONS. Every "what must be covered" set below is a property of the
# CODE, so it cannot drift out of step with what a reader can reach.
# ===========================================================================

def _read(rel: str) -> str:
    return io.open(REPO_ROOT / rel, encoding="utf-8").read()


def _ui_emitting_functions(rel_path: str) -> Dict[str, Any]:
    """`{name: ast node}` for every top-level function in a module that can put
    something on a screen -- DERIVED from what it CALLS, never from its name.

    A function emits UI if its body calls `ui.<anything>`, or if it calls
    another function in the module that does. Rename every function in the
    module and this set is identical; add a data-bearing renderer with any name
    at all and it is included. The naming convention this replaces missed a
    relation-chip drawer and an outage-state drawer in the panel, and would miss
    `_facet_node` and `_card_header` here.
    """
    tree = ast.parse(_read(rel_path))
    by_name: Dict[str, Any] = {
        node.name: node for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    def _calls_ui(node) -> bool:
        for child in ast.walk(node):
            if isinstance(child, ast.Call) and isinstance(child.func, ast.Attribute):
                base = child.func.value
                if isinstance(base, ast.Name) and base.id == "ui":
                    return True
        return False

    emitting = {name for name, node in by_name.items() if _calls_ui(node)}
    changed = True
    while changed:
        changed = False
        for name, node in by_name.items():
            if name in emitting:
                continue
            for child in ast.walk(node):
                if (isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
                        and child.func.id in emitting):
                    emitting.add(name)
                    changed = True
                    break
    return {name: by_name[name] for name in sorted(emitting)}


def _module_executable_lines(rel_path: str) -> FrozenSet[int]:
    """Every line the COMPILER emitted bytecode for, nested bodies included.

    Asking Python's own compiler rather than walking the AST ourselves: every
    "is this a statement?" decision we made instead would be a place where "the
    line was not required" and "the line was never run" become
    indistinguishable.
    """
    code = compile(_read(rel_path), rel_path, "exec")
    lines: Set[int] = set()
    stack = [code]
    while stack:
        current = stack.pop()
        for _start, _end, lineno in current.co_lines():
            if lineno:
                lines.add(lineno)
        stack.extend(const for const in current.co_consts
                     if isinstance(const, types.CodeType))
    return frozenset(lines)


def _required_lines(rel_path: str) -> Dict[str, FrozenSet[int]]:
    """`{function: the lines of its body the capture must execute}`.

    The `def` line is excluded -- it runs at import, so requiring it would
    assert nothing about the capture. Nested handler bodies are INCLUDED: a
    click handler that paints artifact text is a surface a reader reaches, and a
    capture that never takes it is a capture that never looked.
    """
    executable = _module_executable_lines(rel_path)
    required: Dict[str, FrozenSet[int]] = {}
    for name, node in _ui_emitting_functions(rel_path).items():
        body = getattr(node, "body", None)
        if not body:                                          # pragma: no cover
            continue
        first = body[0].lineno
        last = node.end_lineno or first
        required[name] = frozenset(
            line for line in executable if first <= line <= last)
    return required


#: The four constructors that make a `web.discovery` return value a D-13
#: ENVELOPE. A read whose body names one of these emits an envelope; a read that
#: names none of them returns a bare value. The partition below is derived from
#: this, so a read that GAINS an envelope tomorrow moves into the must-be-scanned
#: set without this file being edited.
_ENVELOPE_CONSTRUCTORS = frozenset({
    "make_envelope", "unavailable_envelope", "timeout_envelope", "busy_envelope",
})


def _enveloped_reads() -> FrozenSet[str]:
    """The `web.discovery` reads that emit a D-13 envelope, derived from what
    each one's body CONSTRUCTS -- never from the `_enveloped` name suffix, which
    `get_manuscript_page_ids` does not carry and which would therefore have been
    wrong on the panel's page-scope read."""
    tree = ast.parse(_read("web/discovery.py"))
    out: Set[str] = set()
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for child in ast.walk(node):
            if (isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
                    and child.func.id in _ENVELOPE_CONSTRUCTORS):
                out.add(node.name)
                break
    return frozenset(out)


def _discovery_reads_consumed() -> Dict[str, List[str]]:
    """`{web.discovery read: [modules that call it]}`, DERIVED.

    Every `.py` under `web/` is parsed; a module is in scope if it imports
    `web.discovery` at all, and a read is in scope if that module names it --
    either through `from web.discovery import X` or through an alias attribute
    `_discovery.X`. The result is filtered to the coroutine functions
    `web.discovery` actually defines, so constants and vocabulary re-exports do
    not masquerade as reads.

    This is the anti-drift half of the JSON class: add a read to a surface and
    it appears here without this file being edited, and the payload-coverage
    test then fails BY NAME until the payload is scanned. It found one on its
    first run -- the /help methods page's two band-precision reads, which are
    NOT enveloped and are recorded as out of scope rather than quietly dropped.
    """
    import web.discovery as wd

    reads: Dict[str, List[str]] = {}
    for path in sorted((REPO_ROOT / "web").rglob("*.py")):
        rel = path.relative_to(REPO_ROOT).as_posix()
        source = io.open(path, encoding="utf-8").read()
        if "web.discovery" not in source and "from web import discovery" not in source:
            continue
        tree = ast.parse(source)
        named: Set[str] = set()
        aliases: Set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == "web.discovery":
                named.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module == "web":
                for alias in node.names:
                    if alias.name == "discovery":
                        aliases.add(alias.asname or alias.name)
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "web.discovery":
                        aliases.add(alias.asname or "web")
        for node in ast.walk(tree):
            if (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
                    and node.value.id in aliases):
                named.add(node.attr)
        for name in sorted(named):
            member = getattr(wd, name, None)
            if inspect.iscoroutinefunction(member):
                reads.setdefault(name, []).append(rel)
    return reads


def _copy_export_api_hits() -> Dict[str, List[str]]:
    """`{api: [modules using it]}` over the surface modules -- the derived proof
    behind the copy/export class's asserted ABSENCE."""
    hits: Dict[str, List[str]] = {}
    surfaces = ["web/components/discovery_panel.py",
                "web/components/findings_rows.py",
                "web/pages/findings.py",
                "web/pages/browse_enrichment.py"]
    for rel in surfaces:
        source = _read(rel)
        for api in _COPY_EXPORT_APIS:
            if api in source:
                hits.setdefault(api, []).append(rel)
    return hits


# ===========================================================================
# THE RENDER HARNESS. Mirrors the findings suite's own, plus the two things it
# does not do: it drives the CLICK handlers, and it hands back the client so
# link targets can be read off it.
# ===========================================================================

class _Patch:
    """A monkeypatch-shaped object usable outside a pytest fixture."""

    def __init__(self) -> None:
        self._undo: List[Tuple[Any, str, Any]] = []

    def setattr(self, obj: Any, name: str, value: Any) -> None:
        self._undo.append((obj, name, getattr(obj, name)))
        setattr(obj, name, value)

    def undo(self) -> None:
        for obj, name, value in reversed(self._undo):
            setattr(obj, name, value)


def _render_findings_page(*, lang: str, findings: Any, facets: Any = None,
                          launch: Any = None, state: Any = None,
                          as_of: Optional[str] = "2026-08-03",
                          drive: bool = True) -> Any:
    """The REAL `create_findings_page()` with its three reads stubbed, rendered
    in a real client, with every control it produced CLICKED.

    The clicking is the part 136-18's capture did not do, and it is where the
    facet tree, the unit/sort selects, the novelty switch, the bucket control
    and the pager all paint -- every one of them from artifact-derived text.
    """
    tf._ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    launch_envelope = tf.sentinel_launch_envelope() if launch is None else launch
    facets_by_level = facets or {}

    async def _findings(*_a, **_k):
        return dict(findings)

    async def _facets(level, **_k):
        return dict(facets_by_level.get(level) or tf.facets_envelope(level))

    async def _launch(*_a, **_k):
        return dict(launch_envelope)

    patch = _Patch()
    patch.setattr(fp, "get_findings_enveloped", _findings)
    patch.setattr(fp, "get_findings_facets_enveloped", _facets)
    patch.setattr(fp, "get_launch_stats_enveloped", _launch)
    patch.setattr(fp, "discovery_meta",
                  lambda key: as_of if key == "data_as_of" else None)
    if state is not None:
        patch.setattr(fp, "read_state", lambda: dict(state))
    tf.set_language(lang)

    holder: Dict[str, Any] = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_discovery_masking_sweep")) as client:
            with client:
                result = fp.create_findings_page()
                if asyncio.iscoroutine(result):
                    await result
                if drive:
                    await tp._drive_click_handlers(client)
                    await _drive_value_change_handlers(client)
        holder["client"] = client

    try:
        asyncio.run(_run())
    finally:
        patch.undo()
        tf.set_language("he")
    return holder["client"]


def _render_component(paint) -> Any:
    """One findings COMPONENT painted in a real client, controls driven."""
    tf._ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client
    holder: Dict[str, Any] = {}

    async def _run():
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_discovery_masking_sweep_component")) as client:
            with client:
                result = paint()
                if asyncio.iscoroutine(result):
                    await result
                await tp._drive_click_handlers(client)
                await _drive_value_change_handlers(client)
        holder["client"] = client

    asyncio.run(_run())
    return holder["client"]


class _ValueEvent:
    """The shape a NiceGUI `on_change` handler reads (`event.value`)."""

    def __init__(self, value: Any) -> None:
        self.value = value


def _control_option_values(element) -> List[Any]:
    """The values a control OFFERS, read off the control itself.

    Never a list written here: a unit or a sort added to the exported vocabulary
    is driven without this function being edited, which is the whole reason the
    findings page builds these options from `FINDINGS_UNITS` / `FINDINGS_SORTS`
    rather than from literals.
    """
    options = getattr(element, "options", None)
    if isinstance(options, dict):
        return list(options)
    if isinstance(options, (list, tuple)) and options:
        return [option.get("value") if isinstance(option, dict) else option
                for option in options]
    return [None]


async def _drive_value_change_handlers(client) -> None:
    """Run every VALUE-CHANGE handler the render produced, once per option the
    control offers.

    `_drive_click_handlers` reaches buttons and toggles. The findings page's unit
    and sort controls are `ui.select`s: their handler is registered as a change
    handler, not a click listener, so a click sweep alone never runs them --
    and each one re-queries and repaints the whole result region, which is
    exactly where artifact-derived rows are drawn. Those five lines apiece had
    never been in any masking capture.

    The shipped handler is invoked directly with the `.value`-shaped event it
    reads, rather than through `set_value`: NiceGUI schedules a coroutine change
    handler as a background task, and a capture that returns before that task
    runs would record the paint as having happened when it had not.
    """
    import inspect as _inspect

    for element in list(client.elements.values()):
        slot = getattr(element, "parent_slot", None)
        handlers = list(getattr(element, "_change_handlers", None) or ())
        # ...plus any NON-click listener the surface registered itself. NiceGUI's
        # own internal bridges are skipped: they read a different event shape and
        # their failures would be noise, not findings.
        for listener in list(getattr(element, "_event_listeners", {}).values()):
            if getattr(listener, "type", None) == "click":
                continue
            handler = getattr(listener, "handler", None)
            module = getattr(handler, "__module__", "") or ""
            if handler is not None and not module.startswith("nicegui"):
                handlers.append(handler)
        if not handlers:
            continue
        for value in _control_option_values(element):
            for handler in handlers:
                try:
                    with slot if slot is not None else client:
                        result = (handler()
                                  if not _inspect.signature(handler).parameters
                                  else handler(_ValueEvent(value)))
                        if _inspect.isawaitable(result):
                            await result
                except Exception:                             # noqa: BLE001
                    logging.getLogger(__name__).debug(
                        "sweep: change handler raised", exc_info=True)


def _client_texts(client) -> List[str]:
    out: List[str] = []
    for element in client.elements.values():
        out.extend(tp._own_texts(element))
    return out


def _client_hrefs(client) -> List[str]:
    """Every link target the render produced -- what a reader copies with
    "copy link address", and the one copy/export egress either surface has."""
    out: List[str] = []
    for element in client.elements.values():
        props = getattr(element, "_props", None) or {}
        for key in ("href", "to", "src", "download"):
            value = props.get(key)
            if isinstance(value, str) and value.strip():
                out.append(f"{key}={value}")
    return out


# ===========================================================================
# THE FOUR CAPTURES.
# ===========================================================================

def _seeded(seed: Optional[str], default: Any) -> Any:
    return seed if seed else default


def _findings_deep_renders(seed: Optional[str] = None) -> Tuple[List[str], List[str]]:
    """The findings surfaces the 48-state matrix does not reach, plus the link
    targets. Returns `(texts, hrefs)`.

    Every entry here exists because a line-coverage run showed the matrix never
    executed it -- the facet tree and its leaves, the blocked facet state, the
    honest empty result, the approximate-total note, the launch outage in both
    of its shapes, the out-of-vocabulary relation kind, the multi-work
    annotation, and the two shelfmark shapes.
    """
    title = _seeded(seed, tf.UNCURATED_RAW_TITLE)
    texts: List[str] = []
    hrefs: List[str] = []

    def _take(client) -> None:
        texts.extend(_client_texts(client))
        hrefs.extend(_client_hrefs(client))

    # A facet cascade with a PARENT and its LEAF, so the domain tree renders its
    # branch, its chevron and its collapsed child container rather than a flat
    # node -- and the chevron is then clicked.
    parent = tf.facet_row(level="domain", value="Synthetic Parent A",
                          label="Synthetic Parent A", parent=None, is_leaf=False)
    leaf = tf.facet_row(level="domain", value="Synthetic Parent A / Synthetic Leaf A",
                        label="Synthetic Parent A / Synthetic Leaf A",
                        parent="Synthetic Parent A", is_leaf=True)
    orphan = tf.facet_row(level="domain", value="Synthetic Orphan Leaf",
                          label="Synthetic Orphan Leaf",
                          parent="Not A Present Parent", is_leaf=True)
    rich_facets = {
        "domain": tf.facets_envelope("domain", items=[parent, leaf, orphan]),
        "author": tf.facets_envelope("author", items=[
            tf.facet_row(level="author", value="Synthetic Author A",
                         label="Synthetic Author A", parent=None)]),
        "work": tf.facets_envelope("work", items=[
            tf.facet_row(level="work", value=tf.CURATED_WORK_ID,
                         label=tf.CURATED_RAW_TITLE, parent=None)]),
    }
    blocked_facets = {level: tf.facets_envelope(level, status=tf.STATUS_UNAVAILABLE)
                      for level in ("domain", "author", "work")}

    rows_full = [tf.finding_row(neutral_title=title)]
    for lang in LANGS:
        # 1. the populated facet cascade, with the selected domain INSIDE a
        #    branch so the branch renders expanded.
        _take(_render_findings_page(
            lang=lang, findings=tf.findings_envelope(rows_full, total=97),
            facets=rich_facets,
            state={"unit": tf.FINDINGS_UNIT_IDENTIFICATION,
                   "bucket": tf.BUCKET_MAIN, "sort": "band_rank",
                   "novelty_only": False, "divergence": False,
                   "domain": "Synthetic Parent A / Synthetic Leaf A",
                   "author": None, "work_id": None, "page": 2}))
        # 2. the same cascade with NOTHING selected, so the branch renders
        #    collapsed and the chevron takes its other icon.
        _take(_render_findings_page(
            lang=lang, findings=tf.findings_envelope(rows_full),
            facets=rich_facets))
        # 3. facets whose backing data is absent -- a visibly blocked control.
        _take(_render_findings_page(
            lang=lang, findings=tf.findings_envelope(rows_full),
            facets=blocked_facets))
        # 4. the HONEST empty state: ok with zero rows, which must not look
        #    like an outage.
        _take(_render_findings_page(
            lang=lang, findings=tf.findings_envelope([])))
        # 5. an approximate total, whose note is the one place the findings page
        #    admits its count is capped.
        approx = tf.findings_envelope(rows_full, total=500000)
        approx["meta"]["approximate_total"] = True
        _take(_render_findings_page(lang=lang, findings=approx))
        # 6. no `data_as_of` recorded -- the as-of line is omitted, not guessed.
        _take(_render_findings_page(
            lang=lang, findings=tf.findings_envelope(rows_full), as_of=None))
        # 7. the launch headline's outage, in each named service state and with
        #    and without a retry.
        for status in (tf.STATUS_UNAVAILABLE, tf.STATUS_TIMEOUT, tf.STATUS_BUSY):
            envelope = tf.findings_envelope([], status=status)
            for retry in (True, False):
                _take(_render_component(
                    lambda e=envelope, ln=lang, r=retry: fr.render_launch_headline(
                        e, ln, on_retry=(lambda _ev=None: None) if r else None)))
        # ...and the defensive shape: an envelope carrying NO status, which must
        # be reported as unavailable rather than rendering silence. Painted
        # through the renderer directly because `render_launch_headline` reaches
        # it only via `is_outage`, which already requires one of the three
        # statuses -- so the branch is unreachable from the caller and would
        # otherwise be a line the masking scan never looked at.
        for envelope in ({"items": [], "total": 0, "meta": {}}, {}):
            _take(_render_component(
                lambda e=envelope, ln=lang: fr._render_launch_outage(e, ln, None)))
        # 7b. the page's own outage states, DRIVEN -- the retry affordance runs
        #     the whole refresh path, which repaints the result region.
        for status in (tf.STATUS_UNAVAILABLE, tf.STATUS_TIMEOUT, tf.STATUS_BUSY):
            _take(_render_findings_page(
                lang=lang, findings=tf.findings_envelope([], status=status)))
        # 7c. the per-work unit, DRIVEN. The candidacy switch refuses its own
        #     toggle on a unit the service does not offer novelty for -- a
        #     reachable state, and one that no undriven render can reach.
        _take(_render_findings_page(
            lang=lang, findings=tf.findings_envelope(
                [tf.finding_row(unit=tf.FINDINGS_UNIT_WORK, novelty_offered=False,
                                novelty_status=None, manuscript_count=9,
                                neutral_title=title)],
                unit=tf.FINDINGS_UNIT_WORK),
            state={"unit": tf.FINDINGS_UNIT_WORK, "bucket": tf.BUCKET_MAIN,
                   "sort": "band_rank", "novelty_only": True, "divergence": False, "domain": None,
                   "author": None, "work_id": None, "page": 1}))
        # 8. the novelty help, with and without an as-of date.
        for as_of in ("2026-08-03", None):
            _take(_render_component(
                lambda ln=lang, a=as_of: fr.render_novelty_help(ln, as_of=a)))
        # 9. row shapes the matrix never produced.
        odd_rows = [
            tf.finding_row(neutral_title=title, relation_kind="not_a_relation"),
            tf.finding_row(neutral_title=title,
                           unit=tf.FINDINGS_UNIT_MANUSCRIPT, work_count=3,
                           multi_work_annotation=True),
            tf.finding_row(neutral_title=title, shelfmark_display=None),
            tf.finding_row(neutral_title=title, sys_id=None),
            tf.finding_row(neutral_title=title, library_code=None,
                           page_count=None, max_coverage_ppm=None,
                           novelty_status=tf.DEFAULT_STATUS),
            # Ruling F's divergence marker. Reachable only when the reader has
            # opened the axis, so no undriven default render paints it -- and
            # both of its strings (the chip and its tooltip) are reader-facing
            # text this scan has to have looked at.
            tf.finding_row(neutral_title=title, divergent=True),
        ]
        _take(_render_component(
            lambda rows=odd_rows, ln=lang: [fr.render_finding_row(r, ln)
                                            for r in rows]))
    return texts, hrefs


def capture_rendered(seed: Optional[str] = None,
                     *, matrix: bool = True,
                     tmp_dir: Optional[pathlib.Path] = None
                     ) -> Tuple[str, List[str], Dict[str, Any]]:
    """Path class 1. Returns `(text, hrefs, coverage)`.

    `coverage` carries the executed line numbers per findings module, so
    `test_the_rendered_capture_executes_every_ui_emitting_LINE` can name what
    was never painted instead of trusting this docstring.
    """
    parts: List[str] = []
    hrefs: List[str] = []
    traced_paths = {str(module.__file__): rel
                    for rel, module in RENDERED_COVERAGE_MODULES.items()}
    executed: Dict[str, Set[int]] = {rel: set() for rel in RENDERED_COVERAGE_MODULES}
    previous = sys.gettrace()

    def _trace(frame, event, _arg):
        rel = traced_paths.get(frame.f_code.co_filename)
        if rel is None:
            return None
        if event == "line":
            executed[rel].add(frame.f_lineno)
        return _trace

    sys.settrace(_trace)
    try:
        if matrix:
            # 136-18's own capture -- 3 units x 4 service states x 2 buckets x 2
            # languages of the REAL page. Nothing called it until now.
            destination = (tmp_dir or pathlib.Path(
                os.environ.get("TMPDIR") or ".")) / "findings_matrix_capture.txt"
            tf.capture_rendered_output(str(destination))
            parts.append(io.open(destination, encoding="utf-8").read())
        deep_texts, deep_hrefs = _findings_deep_renders(seed)
        parts.extend(deep_texts)
        hrefs.extend(deep_hrefs)
    finally:
        sys.settrace(previous)

    # The PANEL half, from 136-17's capture: it already drives its own clicks and
    # is already line-checked by its own suite, so it is reused rather than
    # rebuilt (one capture, one place it is maintained).
    panel = tp._capture_panel_surface(seed=seed)
    parts.append(panel["rendered"])

    coverage = {"executed": {rel: frozenset(lines) for rel, lines in executed.items()},
                "panel_exercised": panel["exercised"]}
    return "\n".join(parts), hrefs, coverage


def _seeded_finding_rows(seed: Optional[str]) -> List[Dict[str, Any]]:
    title = _seeded(seed, tf.UNCURATED_RAW_TITLE)
    return [tf.finding_row(neutral_title=title)]


def payload_map(seed: Optional[str] = None) -> Dict[str, List[Tuple[str, Any]]]:
    """Path class 2, keyed by the `web.discovery` read that PRODUCES each
    payload -- so the coverage test can compare against the derived read set
    rather than against a list of nice names."""
    panel = dict(tp.panel_envelopes(seed=seed))
    title = _seeded(seed, tf.UNCURATED_RAW_TITLE)
    findings_envelopes = [
        (name, envelope) for name, envelope in tf.surface_envelopes()
    ]
    seeded_findings = tf.findings_envelope(_seeded_finding_rows(seed))
    seeded_facet = tf.facets_envelope("work", items=[
        tf.facet_row(level="work", value=tf.CURATED_WORK_ID, label=title, parent=None)])
    return {
        "get_claims_for_page_enveloped": [("claims", panel["claims"])],
        "get_manuscript_page_ids": [("page_ids", panel["page_ids"])],
        "get_manuscript_works_enveloped": [("manuscript_works",
                                            panel["manuscript_works"])],
        "get_related_page_count_enveloped": [("related_count", panel["related_count"])],
        "get_related_pages_enveloped": [("related_rows", panel["related_rows"])],
        "get_work_expansion_enveloped": [("expansion", panel["expansion"])],
        "get_findings_enveloped": (
            [(n, e) for n, e in findings_envelopes if n.startswith("findings/")]
            + [("findings/seeded", seeded_findings)]),
        "get_findings_facets_enveloped": (
            [(n, e) for n, e in findings_envelopes if n.startswith("facets/")]
            + [("facets/seeded", seeded_facet)]),
        "get_launch_stats_enveloped": (
            [(n, e) for n, e in findings_envelopes if n.startswith("launch/")]
            + [("launch_shades", panel["launch_shades"])]),
    }


def capture_payloads(seed: Optional[str] = None) -> str:
    """Every enveloped payload, serialised BOTH the way a JSON response body
    carries it (raw UTF-8) and the way an `ensure_ascii` encoder carries it
    (`\\uXXXX` escapes). The scanner de-escapes the second form; dumping both
    means the class is scanned as it would actually travel, not as we assume it
    travels."""
    lines: List[str] = ["# CLASS 2 -- JSON PAYLOADS (every enveloped read either "
                        "surface consumes)"]
    consumed = _discovery_reads_consumed()
    enveloped = _enveloped_reads()
    lines.append("# NOT COVERED BY THIS CLASS, and why -- stated here so the "
                 "limit is visible in the artifact, not only in a summary:")
    for name in sorted(set(consumed) - enveloped):
        lines.append(f"#   {name}: returns a bare value, not a D-13 envelope; "
                     f"consumed by {', '.join(consumed[name])} (the /help methods "
                     "page, Phase 135), which is not a surface this sweep renders")
    for read, payloads in sorted(payload_map(seed).items()):
        for where, envelope in payloads:
            lines.append(f"{read} :: {where} :: "
                         + json.dumps(envelope, ensure_ascii=False, default=str))
            lines.append(f"{read} :: {where} :: escaped :: "
                         + json.dumps(envelope, ensure_ascii=True, default=str))
    return "\n".join(lines)


def capture_copy_export(hrefs: List[str], seed: Optional[str] = None) -> str:
    """Path class 3.

    Neither surface has a clipboard button, a download route or an export
    handler -- and that absence is DERIVED here rather than asserted from
    memory. What a reader CAN copy is a link target, so every `href` the render
    produced is listed and scanned.
    """
    lines = ["# CLASS 3 -- COPY / EXPORT EGRESS",
             "# derived inventory of clipboard/download/export APIs in the "
             "surface modules:"]
    hits = _copy_export_api_hits()
    for api in _COPY_EXPORT_APIS:
        lines.append(f"#   {api}: {', '.join(hits.get(api, [])) or 'ABSENT'}")
    lines.append("# link targets a reader can copy (the ONE copy path either "
                 "surface has):")
    seen: List[str] = []
    for href in hrefs:
        if href not in seen:
            seen.append(href)
    lines.extend(seen)
    if seed:
        # The seeded row's link target, rendered by the shipped renderer.
        client = _render_component(
            lambda: fr.render_finding_row(tf.finding_row(sys_id=seed), "en"))
        lines.extend(_client_hrefs(client))
    return "\n".join(lines)


def _forced_log_lines(seed: Optional[str] = None) -> List[Tuple[str, str]]:
    """REAL log records, forced.

    `web/discovery.py` interpolates a `page_id` (and elsewhere a `sys_id` and a
    `work_id`) into its degraded-path log lines. Those identifiers are
    artifact-derived, so a log line is an egress with artifact text on it --
    which is why the error-path positive control seeds one and why a log scan
    that only ever saw fixed strings would prove nothing.
    """
    import web.discovery as wd

    records: List[Tuple[str, str]] = []
    handler_records: List[logging.LogRecord] = []

    class _Collect(logging.Handler):
        def emit(self, record):
            handler_records.append(record)

    handler = _Collect()
    logger = logging.getLogger("web.discovery")
    logger.addHandler(handler)
    previous_level = logger.level
    logger.setLevel(logging.DEBUG)
    patch = _Patch()
    try:
        async def _overload(*_a, **_k):
            raise DiscoveryOverload("bounded concurrency")

        patch.setattr(wd, "discovery_available", lambda: True)
        patch.setattr(wd._service, "get_claims_for_page_enveloped_async", _overload)
        page_id = seed or "990000000000000944_IE1_P000002_FL3"
        envelope = asyncio.run(wd.get_claims_for_page_enveloped(page_id))
        records.append(("log/claims-overload-envelope", json.dumps(
            envelope, ensure_ascii=False, default=str)))
    finally:
        patch.undo()
        logger.removeHandler(handler)
        logger.setLevel(previous_level)
    for record in handler_records:
        records.append((f"log/{record.name}", record.getMessage()))

    # A REAL service-side log line: a findings query that raises inside the
    # query rather than at the availability gate.
    import tempfile
    service_records: List[logging.LogRecord] = []

    class _CollectService(logging.Handler):
        def emit(self, record):
            service_records.append(record)

    service_handler = _CollectService()
    service_logger = logging.getLogger("shared.discovery_service")
    service_logger.addHandler(service_handler)
    service_previous = service_logger.level
    service_logger.setLevel(logging.DEBUG)
    try:
        directory = pathlib.Path(tempfile.mkdtemp())
        broken = tf._broken_service(directory)
        broken_envelope = broken.get_findings_enveloped()
        records.append(("log/findings-query-failed-envelope", json.dumps(
            broken_envelope, ensure_ascii=False, default=str)))
    finally:
        service_logger.removeHandler(service_handler)
        service_logger.setLevel(service_previous)
    for record in service_records:
        records.append((f"log/{record.name}", record.getMessage()))
    return records


def _rendered_error_states(seed: Optional[str] = None) -> List[Tuple[str, str]]:
    """The RENDERED half of the error class: what a reader sees when a read
    fails, and what a malformed row looks like once the surface has swallowed
    its exception. The findings row keeps rendering with the artifact's own
    title on it, so this is where a value CAN arrive on an error path -- the
    message half structurally cannot (asserted below)."""
    out: List[Tuple[str, str]] = []
    title = _seeded(seed, tf.UNCURATED_RAW_TITLE)
    for lang in LANGS:
        for status in (tf.STATUS_UNAVAILABLE, tf.STATUS_TIMEOUT, tf.STATUS_BUSY):
            client = _render_findings_page(
                lang=lang, findings=tf.findings_envelope([], status=status),
                drive=True)
            out.append((f"rendered-outage/{status}/{lang}",
                        "\n".join(_client_texts(client))))
        client = _render_component(lambda ln=lang, t=title: fr.render_finding_row(
            tf.finding_row(neutral_title=t, relation_kind="not_a_relation"), ln))
        out.append((f"rendered-malformed-row/{lang}",
                    "\n".join(_client_texts(client))))
    return out


def capture_error_paths(seed: Optional[str] = None) -> str:
    """Path class 4: exception messages, real log lines and rendered error
    states, from BOTH surfaces."""
    lines = ["# CLASS 4 -- ERROR PATHS"]
    for mode, message in tp.forced_error_paths():
        lines.append(f"panel/{mode} :: {message}")
    for mode, message in tf._error_modes(None):
        lines.append(f"findings/{mode} :: {message}")
    for mode, message in _forced_log_lines(seed):
        lines.append(f"{mode} :: {message}")
    for mode, message in _rendered_error_states(seed):
        lines.append(f"{mode} :: {message}")
    return "\n".join(lines)


# ===========================================================================
# THE CAPTURE FIXTURES. Written OUTSIDE the working tree.
# ===========================================================================

def _assert_outside_the_repo(directory: pathlib.Path) -> None:
    """A capture written INSIDE the tree is itself an untracked-not-ignored file
    that `--scan-repo` enumerates, so a clean surface would be reported as a
    leak in its own capture. Fail closed rather than produce that confusion."""
    resolved = directory.resolve()
    assert REPO_ROOT.resolve() not in resolved.parents and resolved != REPO_ROOT, (
        f"the sweep capture directory {resolved} is inside the repository. "
        "Run pytest without a --basetemp under the working tree: --scan-repo "
        "enumerates untracked-but-not-ignored files and would report this "
        "capture as a leak.")


def _write_class_files(directory: pathlib.Path,
                       texts: Mapping[str, str]) -> Dict[str, pathlib.Path]:
    paths: Dict[str, pathlib.Path] = {}
    for path_class, text in texts.items():
        path = directory / CLASS_FILES[path_class]
        path.write_text(text, encoding="utf-8")
        paths[path_class] = path
    return paths


@pytest.fixture(scope="module")
def clean_capture(tmp_path_factory):
    """The full, unseeded capture of all four classes."""
    directory = tmp_path_factory.mktemp("discovery-masking-sweep-clean")
    _assert_outside_the_repo(directory)
    rendered, hrefs, coverage = capture_rendered(tmp_dir=directory)
    texts = {
        CLASS_RENDERED: rendered,
        CLASS_PAYLOADS: capture_payloads(),
        CLASS_COPY_EXPORT: capture_copy_export(hrefs),
        CLASS_ERROR_PATHS: capture_error_paths(),
    }
    directory_files = directory / "capture"
    directory_files.mkdir()
    paths = _write_class_files(directory_files, texts)
    return {"dir": directory_files, "texts": texts, "paths": paths,
            "hrefs": hrefs, "coverage": coverage}


@pytest.fixture(scope="module")
def seeded_capture(tmp_path_factory):
    """The same four classes with a REAL restricted pattern planted in each.

    Deliberately smaller than the clean capture: a control needs the needle to
    travel one real path per class, not the whole cross-product. The needle is
    read from the pattern file at test time (`_needle`), so this fixture cannot
    exist without the pattern set -- which is what makes an absent pattern file
    a FAILURE here rather than a quiet reduction in coverage.
    """
    needle = _needle()
    directory = tmp_path_factory.mktemp("discovery-masking-sweep-seeded")
    _assert_outside_the_repo(directory)
    rendered, hrefs, _coverage = capture_rendered(seed=needle, matrix=False,
                                                  tmp_dir=directory)
    texts = {
        CLASS_RENDERED: rendered,
        CLASS_PAYLOADS: capture_payloads(needle),
        CLASS_COPY_EXPORT: capture_copy_export(hrefs, seed=needle),
        CLASS_ERROR_PATHS: capture_error_paths(needle),
    }
    return {"needle": needle, "texts": texts, "dir": directory}


# ===========================================================================
# RUNNING THE GATE.
# ===========================================================================

def _run_scan(args: List[str], patterns_file: Any) -> subprocess.CompletedProcess:
    env = dict(os.environ,
               MASKING_SCAN_PATTERNS_FILE=str(pathlib.Path(patterns_file).resolve()))
    return subprocess.run([sys.executable, MASKING_SCRIPT] + args,
                          capture_output=True, text=True, env=env,
                          cwd=str(REPO_ROOT))


def _configured_pattern_file() -> pathlib.Path:
    """The pattern file the real scan runs with.

    An env var that is SET but names no readable file is a misconfiguration --
    the shape a broken CI secret takes -- and does NOT fall back to the local
    file: falling back would turn a broken secret into a green run on the one
    machine that happens to have a copy.
    """
    configured = os.environ.get("MASKING_SCAN_PATTERNS_FILE")
    if configured:
        path = pathlib.Path(configured)
        if not path.is_file():
            raise PatternSetUnavailable(
                "MASKING_SCAN_PATTERNS_FILE is SET but names no readable file "
                f"({configured!r}). That is a misconfiguration, not an absence, "
                "and it deliberately does NOT fall back to a local pattern file: "
                "see `_pattern_set` for why this is a failure and not a skip.")
        return path
    path = REPO_ROOT / ".masking_patterns"
    if not path.is_file():
        raise PatternSetUnavailable(
            "MASKING_SCAN_PATTERNS_FILE is unset and .masking_patterns is absent "
            "-- see `_pattern_set` for why this is a failure and not a skip.")
    return path


# ===========================================================================
# A. THE CAPTURE IS REAL AND COVERS WHAT IT CLAIMS.
# ===========================================================================

def test_all_four_path_classes_are_captured_and_none_is_a_stub(clean_capture):
    """The purest false green available here is a clean scan over an empty
    capture -- every other test in this file would report exactly that. Sizes
    are asserted per class, by name, so a class that silently stopped producing
    output is caught as itself rather than as a byte total."""
    minimums = {CLASS_RENDERED: 200_000, CLASS_PAYLOADS: 20_000,
                CLASS_COPY_EXPORT: 200, CLASS_ERROR_PATHS: 5_000}
    for path_class in PATH_CLASSES:
        text = clean_capture["texts"][path_class]
        assert len(text) >= minimums[path_class], (
            f"the {path_class} capture is {len(text)} chars, under its "
            f"{minimums[path_class]} floor -- it captured little or nothing, and "
            "a clean scan over it would prove nothing")


def test_the_rendered_class_covers_both_surfaces_every_state_unit_bucket_and_language(
        clean_capture):
    """Named markers, not a byte count. Each assertion reads the SHIPPED
    wording, so a rewording moves the check with it."""
    text = clean_capture["texts"][CLASS_RENDERED]
    # The PANEL half, by its own shipped wording.
    assert dp._MANUSCRIPT_PANE_SCOPE_NOTE["en"] in text, "no PANEL text captured"
    # The FINDINGS half, as the FULL cross-product. 136-18's capture labels each
    # state it renders, so this asserts all 48 combinations are present rather
    # than asserting that one word appeared somewhere.
    missing_states = [
        f"{lang}/{status}/{unit}/{bucket}"
        for lang in tf.LANGS for status in tf.SERVICE_STATES
        for unit in tf.UNITS for bucket in tf.BUCKETS
        if f"--- {lang}/{status}/{unit}/{bucket} ---" not in text]
    assert not missing_states, (
        "these findings states are absent from the rendered capture: "
        + repr(missing_states))
    assert tr("No results found") in text, (
        "the HONEST empty state is not in the capture -- an `ok` result with zero "
        "rows is the one state that must not read as an outage")
    for lang in LANGS:
        # Every service state, through the shared outage vocabulary.
        for status in ("unavailable", "timeout", "busy"):
            assert ds.service_state_message(status, lang) in text, (
                f"{lang}/{status}: the outage state is not in the capture")
        assert ds.retry_label(lang) in text, f"{lang}: no retry affordance captured"
        # Both buckets, by the ONE shared rule's own names.
        for in_main in (True, False):
            assert ds.bucket_name(in_main, lang) in text, (
                f"{lang}/main_pool={in_main}: the bucket name is not in the capture")


def test_the_rendered_capture_executes_every_ui_emitting_LINE_of_both_findings_modules(
        clean_capture):
    """The rule the naming convention could not enforce.

    Function-entry instrumentation records that a renderer was CALLED, which
    says nothing about the branch inside it that paints an artifact-derived
    label. This requires every line the compiler emitted for every UI-emitting
    function -- nested click handlers included, because a surface painted only
    after a click is still a surface a reader reads.

    The panel is line-checked by its own suite (136-17) and is not re-checked
    here; its FUNCTION coverage is asserted separately below.
    """
    problems: List[str] = []
    for rel in sorted(RENDERED_COVERAGE_MODULES):
        source_lines = _read(rel).splitlines()
        executed = clean_capture["coverage"]["executed"][rel]
        for name, required in sorted(_required_lines(rel).items()):
            missing = sorted(required - executed)
            missing = [line for line in missing
                       if (rel, name, source_lines[line - 1].strip())
                       not in NON_PAINTING_EXEMPT]
            if missing:
                problems.append(
                    f"{rel}::{name} never executed "
                    + "; ".join(f"L{line} {source_lines[line - 1].strip()!r}"
                                for line in missing))
    assert not problems, (
        "the masking capture never painted these lines, so the scan has never "
        "looked at what they put on a screen:\n" + "\n".join(problems))


def test_the_rendered_capture_reaches_every_ui_emitting_FUNCTION_of_the_panel(
        clean_capture):
    """The panel half, at function granularity, against the same derivation."""
    expected = frozenset(_ui_emitting_functions("web/components/discovery_panel.py"))
    missing = sorted(expected - clean_capture["coverage"]["panel_exercised"])
    assert not missing, (
        "these panel renderers were never reached by the capture: " + repr(missing))


#: Lines of a UI-emitting function that this capture cannot execute, each with
#: the reason. Keyed by `(module, function, exact source text)` so an edit above
#: moves the anchor with the code instead of silently exempting whatever slid
#: into that line number.
#:
#: EVERY ENTRY PAINTS NOTHING. That is the only admissible reason: a `return`
#: taken when the reader's page is already gone, or a defensive `except` that
#: assigns a local. A branch that draws something must never appear here.
NON_PAINTING_EXEMPT: Dict[Tuple[str, str, str], str] = {
    ("web/pages/findings.py", "_populate_facets", "continue"):
        "structural, and it paints nothing: the loop skips a facet level whose "
        "container the page did not build. `_facet_containers` builds all three, "
        "so the branch is defensive against a future level with no container. "
        "Reaching it would require breaking the page's own layout.",
    ("web/pages/findings.py", "_populate_facets", "return"):
        "the page-gone guard between two facet reads. It RETURNS -- it paints "
        "nothing -- and it is taken only when the reader's client disconnected "
        "while a cascade read was in flight, which a synchronous capture cannot "
        "produce without faking the client's own liveness.",
    ("web/pages/findings.py", "_render_body", "return"):
        "the page-gone guards inside `refresh`. They RETURN -- they put nothing "
        "on a screen -- and they are taken only when the reader's client has "
        "disconnected mid-await, which a synchronous capture has no way to "
        "produce without faking the client's own liveness.",
    ("web/pages/findings.py", "create_findings_page", "return"):
        "the same page-gone guards at the top level of the page builder: two "
        "bare returns taken when the client disconnected during the headline or "
        "the body await. They paint nothing, so there is nothing here for the "
        "masking scan to have missed.",
    ("web/pages/findings.py", "create_findings_page",
     "except Exception:  # pragma: no cover -- no client in a bare probe context"):
        "the defensive capture of `ui.context.client`, which assigns a local and "
        "paints nothing. In this harness the client always exists, which is why "
        "the branch is unreachable here rather than merely unexercised.",
    ("web/pages/findings.py", "create_findings_page", "page_client = None"):
        "the body of the same defensive except: a local assignment, no paint. "
        "Exempted as a pair with the handler above so neither reads as covered "
        "while the other is not.",
}


def test_the_exemption_list_paints_nothing_and_has_not_grown_quietly():
    """The escape hatch, pinned. An exemption whose anchor no longer matches any
    line is a dead excuse that reads as though it covers something."""
    assert len(NON_PAINTING_EXEMPT) <= 12, (
        "the non-painting exemption list has grown to "
        f"{len(NON_PAINTING_EXEMPT)} entries. Each one is a line the masking "
        "scan has never looked at; growing this list is how a capture stops "
        "covering the surface it claims to.")
    for (rel, function, text), reason in NON_PAINTING_EXEMPT.items():
        assert len(reason) > 60, f"{rel}::{function} is exempted with no stated reason"
        source = _read(rel)
        assert f"def {function}" in source, (
            f"{rel}::{function} no longer exists -- delete the exemption")
        assert text in source, (
            f"the exemption anchor {text!r} no longer matches any line in {rel}")
        assert "ui." not in text, (
            f"{rel}::{function} exempts a line that PAINTS ({text!r}); the only "
            "admissible exemption is a line that puts nothing on a screen")


def test_the_json_class_scans_every_enveloped_read_either_surface_consumes():
    """Derived, so a read added tomorrow and left unscanned fails BY NAME.

    The read set comes from parsing every module under `web/` that imports
    `web.discovery` and keeping the coroutine functions it names. A remembered
    list is what this replaces -- twice in this phase a remembered list was
    called a derivation and was wrong.
    """
    consumed = _discovery_reads_consumed()
    enveloped = _enveloped_reads()
    scanned = set(payload_map())
    missing = sorted((set(consumed) & enveloped) - scanned)
    assert not missing, (
        "these enveloped reads are consumed by a surface but no payload of "
        "theirs is scanned:\n"
        + "\n".join(f"  {name}  (called from {', '.join(consumed[name])})"
                    for name in missing))
    stale = sorted(scanned - set(consumed))
    assert not stale, (
        "these payloads are scanned but no surface consumes them any more -- "
        "delete them rather than reporting coverage of a dead read: " + repr(stale))
    assert len(scanned) >= 9, (
        f"only {len(scanned)} enveloped reads are scanned; this phase ships nine "
        "(five panel reads, the findings rows, the facet cascade, the work "
        "expansion and the launch statistics)")
    # The out-of-scope half, stated rather than dropped. These are consumed by a
    # DIFFERENT surface (the /help methods page, Phase 135) and return a bare
    # value, not an envelope -- so this phase's payload class does not cover
    # them, and the fact that it does not is recorded here and in the capture.
    out_of_scope = sorted(set(consumed) - enveloped)
    for name in out_of_scope:
        assert name not in scanned
        assert name not in enveloped, (
            f"{name} now constructs an envelope, so it is no longer out of "
            "scope: give it a scanned payload")


def test_the_json_class_holds_a_real_payload_for_every_read_it_claims(clean_capture):
    """The enumeration above proves the KEYS line up. This proves the captured
    file actually carries each read's payload text -- a map with an empty list
    under a key would satisfy the enumeration and scan clean."""
    text = clean_capture["texts"][CLASS_PAYLOADS]
    for read, payloads in sorted(payload_map().items()):
        assert payloads, f"{read} maps to no payload at all"
        for where, envelope in payloads:
            assert f"{read} :: {where} ::" in text, (
                f"{read}/{where} is missing from the captured payload file")
            assert '"status"' in json.dumps(envelope, default=str), (
                f"{read}/{where} is not an envelope")


def test_the_copy_export_class_records_an_absence_it_actually_verified(clean_capture):
    """Criterion: *if* a copy or export path does not exist, that is asserted and
    recorded rather than assumed.

    It does not exist -- no clipboard call, no download route, no export handler
    on either surface -- and the ONE thing a reader can copy is a link target,
    so those are captured and scanned. Both halves are checked: the absence is
    derived from the source, and the presence of the link targets is asserted on
    the rendered output.
    """
    hits = _copy_export_api_hits()
    assert not hits, (
        "a surface gained a copy/export egress this sweep does not capture: "
        + repr(hits))
    hrefs = clean_capture["hrefs"]
    assert hrefs, (
        "no link target was captured at all -- either the shelfmark link was "
        "removed (in which case this surface now has NO copy path and the class "
        "should say so) or the capture stopped reading element props")
    assert any(href.startswith("href=/browse?sys_id=") for href in hrefs), (
        "the findings row's manuscript link is missing from the copy/export "
        f"capture; captured targets were {sorted(set(hrefs))[:10]}")
    text = clean_capture["texts"][CLASS_COPY_EXPORT]
    for api in _COPY_EXPORT_APIS:
        assert f"#   {api}: ABSENT" in text, (
            f"the recorded inventory does not state the disposition of {api}")


def test_the_error_class_drives_at_least_six_failure_modes_on_each_surface(
        clean_capture):
    """Message, log line and rendered state -- all three, both surfaces."""
    panel_modes = tp.forced_error_paths()
    findings_modes = tf._error_modes(None)
    assert len(panel_modes) >= 6, f"only {len(panel_modes)} panel failure modes"
    assert len(findings_modes) >= 6, f"only {len(findings_modes)} findings failure modes"
    logs = _forced_log_lines()
    assert any(name.startswith("log/") for name, _ in logs), "no log line was forced"
    assert any("page_id=" in message for _n, message in logs), (
        "the forced log lines carry no interpolated identifier, so the log half "
        "of this class is vacuous -- a scan that only ever sees fixed strings "
        "proves that it ran, not that it saw anything")
    text = clean_capture["texts"][CLASS_ERROR_PATHS]
    for prefix in ("panel/", "findings/", "log/", "rendered-outage/",
                   "rendered-malformed-row/"):
        assert prefix in text, f"the error-path capture has no {prefix} entries"


def test_the_panel_message_half_of_the_error_class_carries_no_artifact_VALUE():
    """Why the error-path control seeds a LOG LINE and a RENDERED state rather
    than a panel exception message: the panel model refuses a malformed claim by
    CODE and FIELD NAME and never interpolates a value. That is a real property,
    asserted here rather than assumed -- "the needle could not be routed here"
    and "nobody tried" look identical in a passing suite."""
    marker = "ZZQQ-NOT-A-RESTRICTED-NAME-ZZQQ"
    for _mode, message in tp.forced_error_paths():
        assert marker not in message
    from shared.discovery_panel_model import PanelContractError
    from shared.discovery_surface_projection import surface_safe_claim
    try:
        tp.build_panel_rows(tp.PanelServiceBundle(
            claims=make_envelope(STATUS_OK, [surface_safe_claim(tp._claim_source(
                routing_status=marker, neutral_title=marker))], 1,
                meta={"page_id": "p", "include_review": False}),
            page_ids=make_envelope(STATUS_OK, ["p"], 1, meta={
                "sys_id": "s", "resolved": True, "truncated": False,
                "volume_ie": None}),
            manuscript_works=make_envelope(STATUS_OK, [], 0, meta={
                "page_scope_resolved": True, "lang": "en"}),
            related_count=make_envelope(STATUS_OK, [], 0, meta={
                "unit": "distinct_opposite_pages"})))
    except PanelContractError as exc:
        assert marker not in str(exc), (
            "a panel refusal interpolated a row VALUE -- artifact text can now "
            "reach a log line through an exception message, and the error-path "
            "class needs a message-level control of its own")
    else:                                                     # pragma: no cover
        raise AssertionError("the malformed row was accepted")


# ===========================================================================
# B. THE REAL SCAN.
# ===========================================================================

def test_the_whole_capture_and_the_repository_pass_the_real_masking_scan(clean_capture):
    """The D-25 scan itself, `--strict` over all four class files AND the
    repository, with the REAL pattern set.

    WHEN THE PATTERN FILE IS ABSENT THIS FAILS. It does not skip. Without the
    real patterns the assertion this test's name makes is unverified, and a
    required check that reports "unverified" as a pass is the silent green the
    standing rule forbids -- the same rule that makes the scanner itself exit 1
    on an unset `MASKING_SCAN_PATTERNS_FILE`. The MECHANISM is proved
    separately by the four positive controls, so a red run here means exactly
    one thing: this environment has no pattern set.
    """
    patterns = _configured_pattern_file()
    result = _run_scan(["--strict", "--scan-repo",
                        "--scan-asset", str(clean_capture["dir"])], patterns)
    assert result.returncode == 0, (
        "the masking scan reported a restricted string on a discovery surface, "
        "in a payload, on a copy path, on an error path, or in the repository. "
        "The report names a path, an offset and a pattern INDEX and never the "
        f"pattern text:\n{result.stdout[-4000:]}\n{result.stderr[-2000:]}")


def test_scanning_the_repository_after_the_capture_is_still_clean(clean_capture):
    """The capture must not become the leak. Written outside the tree (asserted
    at fixture time), so `--scan-repo` on its own stays clean afterwards."""
    del clean_capture
    result = _run_scan(["--scan-repo"], _configured_pattern_file())
    assert result.returncode == 0, (
        "`--scan-repo` is dirty after the sweep ran -- most likely a capture was "
        f"written inside the working tree:\n{result.stdout[-2000:]}")


def test_this_module_hardcodes_no_restricted_name():
    """The needle is generated from the pattern file at test time; a restricted
    name typed into this file would be the leak the file exists to prevent."""
    result = _run_scan(["--scan-asset", SWEEP_PATH], _configured_pattern_file())
    assert result.returncode == 0, (
        "this test module itself carries a restricted pattern:\n"
        f"{result.stdout[-2000:]}")


# ===========================================================================
# C. THE FOUR POSITIVE CONTROLS -- one per path class.
#
# Each seeds a marker read from the pattern file, assembles a directory in which
# ONLY that class is seeded, and asserts the SPECIFIC expected failure: the
# nonzero exit, the identified file, and the three files that must NOT be
# reported. "The suite went red" is not evidence that a class is covered.
# ===========================================================================

def _control_directory(tmp_path: pathlib.Path, clean_capture, seeded_capture,
                       path_class: str) -> pathlib.Path:
    directory = tmp_path / f"control-{path_class}"
    directory.mkdir()
    _assert_outside_the_repo(directory)
    texts = {other: (seeded_capture["texts"][other] if other == path_class
                     else clean_capture["texts"][other])
             for other in PATH_CLASSES}
    _write_class_files(directory, texts)
    return directory


def _contains(haystack: str, needle: str) -> bool:
    """Membership as a plain bool, NEVER inline in an `assert`.

    FOUND BY THIS SWEEP'S OWN MUTATION RUN. `assert needle in text` reads
    perfectly and leaks: pytest rewrites the expression and prints BOTH operands
    on failure, so the first control that ever went red put the restricted
    pattern -- and 2 MB of captured surface -- into the test log. A masking test
    that leaks the thing it tests for on its way to reporting a leak is the
    worst possible shape for this file, and `test_no_assertion_in_this_module_can_
    ECHO_the_needle` pins it structurally rather than by care.
    """
    return needle in haystack


def _assert_control(result: subprocess.CompletedProcess, path_class: str,
                    needle: str) -> None:
    output = result.stdout + result.stderr
    assert result.returncode != 0, (
        f"the scanner PASSED a {path_class} surface with a restricted pattern on "
        f"it -- it cannot fail on this class, so its clean runs over "
        f"{path_class} prove nothing:\n{result.stdout[-2000:]}")
    assert CLASS_FILES[path_class] in output, (
        f"the scan failed, but it did not name the {path_class} file "
        f"({CLASS_FILES[path_class]}). A control that only shows the suite went "
        f"red does not show WHICH class is covered:\n{result.stdout[-2000:]}")
    for other in PATH_CLASSES:
        if other == path_class:
            continue
        assert CLASS_FILES[other] not in output, (
            f"the {path_class} control also reported {CLASS_FILES[other]}, so the "
            "unseeded classes are not clean and the control is not specific")
    echoed = _contains(output, needle)
    assert not echoed, (
        "the scanner ECHOED the matched pattern; a real one would land in a CI log")


@pytest.mark.parametrize("path_class", PATH_CLASSES)
def test_CONTROL_the_sweep_fails_on_a_seeded(path_class, clean_capture,
                                             seeded_capture, tmp_path):
    """One control per path class. The needle enters through DATA and leaves
    through the class's own egress -- a rendered row, a serialised envelope, a
    link target, a log line and a rendered error state -- never by appending a
    string to a finished file, which would prove only that the scanner can read
    one."""
    needle = seeded_capture["needle"]
    survived = _contains(seeded_capture["texts"][path_class], needle)
    assert survived, (
        f"the planted marker did not survive into the {path_class} capture, so "
        "scanning it would prove nothing about that class")
    contaminated = [other for other in PATH_CLASSES
                    if _contains(clean_capture["texts"][other], needle)]
    assert not contaminated, (
        f"the UNSEEDED {contaminated} capture(s) already carry the marker -- the "
        "control is inert")
    directory = _control_directory(tmp_path, clean_capture, seeded_capture, path_class)
    result = _run_scan(["--scan-asset", str(directory)],
                       _needle_pattern_file(tmp_path))
    _assert_control(result, path_class, needle)


def test_CONTROL_the_error_path_marker_really_reaches_a_LOG_LINE(seeded_capture):
    """The error-path control, made specific about WHICH error egress it proved.

    An error class demonstrated only on a rendered state would leave the two
    egresses that never pass a renderer -- the exception message and the log
    line -- unproven. `web/discovery.py` interpolates the `page_id` into its
    degraded-path log lines, so a value arriving as a page id lands in a log
    with no renderer between it and the file.
    """
    needle = seeded_capture["needle"]
    logs = _forced_log_lines(needle)
    carrying = [name for name, message in logs
                if name.startswith("log/") and _contains(message, needle)]
    assert carrying, (
        "the planted marker reached no log line, so the log half of the error "
        "class is unproven. Forced records were: "
        + repr([name for name, _m in logs]))


# ===========================================================================
# D. THE FAIL-CLOSED CONTRACT.
# ===========================================================================

def test_an_unset_pattern_file_FAILS_the_scan_rather_than_reporting_a_clean_surface(
        clean_capture):
    """The scanner's own fail-closed exit, exercised rather than quoted."""
    env = dict(os.environ)
    env.pop("MASKING_SCAN_PATTERNS_FILE", None)
    result = subprocess.run(
        [sys.executable, MASKING_SCRIPT, "--scan-asset", str(clean_capture["dir"])],
        capture_output=True, text=True, env=env, cwd=str(REPO_ROOT))
    assert result.returncode != 0, (
        "a zero-pattern scan exited 0 -- it reported four clean surfaces it "
        "never inspected")
    assert "no masking patterns loaded" in result.stderr, result.stderr


def test_an_unset_pattern_file_makes_the_sweeps_own_needle_a_hard_ERROR(monkeypatch):
    """And the sweep itself refuses to proceed, rather than reducing coverage
    quietly. This is the first of the six false-confidence defects this phase
    produced -- a masking test that SKIPPED when its pattern file was absent --
    pinned so it cannot come back."""
    monkeypatch.delenv("MASKING_SCAN_PATTERNS_FILE", raising=False)
    with pytest.raises(PatternSetUnavailable) as excinfo:
        _needle()
    assert "not a skip" in str(excinfo.value)


def test_readiness_is_NOT_MET_unless_the_patterns_loaded_and_both_modes_ran():
    """`masking_readiness` is the ONE place the attestation's masking row is
    decided, and it refuses every partial outcome."""
    both = frozenset({"--scan-asset", "--scan-sqlite"})
    assert masking_readiness(pattern_set_loaded=False, modes_run=both)[0] == "NOT MET"
    assert masking_readiness(pattern_set_loaded=True,
                             modes_run=frozenset({"--scan-asset"}))[0] == "NOT MET"
    assert masking_readiness(pattern_set_loaded=True,
                             modes_run=frozenset({"--scan-sqlite"}))[0] == "NOT MET"
    assert masking_readiness(pattern_set_loaded=True, modes_run=both)[0] == "MET"
    state, reason = masking_readiness(pattern_set_loaded=False, modes_run=both)
    assert "never inspected" in reason, reason
    del state


def _skip_constructs(rel_path: str) -> List[str]:
    """Every skip/xfail construct a module actually USES, found by parsing it.

    An AST walk rather than a substring search, and not for elegance: a string
    check over this file's own source matches the list of forbidden names it
    carries, so the check would fail on itself and would then be "fixed" by
    weakening it. Structure is what distinguishes a call from a mention.
    """
    found: List[str] = []
    tree = ast.parse(_read(rel_path))

    def _dotted(node) -> str:
        parts: List[str] = []
        while isinstance(node, ast.Attribute):
            parts.append(node.attr)
            node = node.value
        if isinstance(node, ast.Name):
            parts.append(node.id)
        return ".".join(reversed(parts))

    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            name = _dotted(node.func)
            if name in ("pytest.skip", "pytest.xfail", "pytest.importorskip"):
                found.append(f"{name}() at line {node.lineno}")
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for decorator in node.decorator_list:
                target = decorator.func if isinstance(decorator, ast.Call) else decorator
                name = _dotted(target)
                if name in ("pytest.mark.skip", "pytest.mark.skipif",
                            "pytest.mark.xfail"):
                    found.append(f"@{name} on {node.name} at line {node.lineno}")
    return found


#: Names that hold a REAL restricted pattern at runtime. An `assert` whose test
#: expression references one of these is rewritten by pytest into something that
#: prints BOTH operands on failure -- i.e. it publishes the pattern, and usually
#: a megabyte of captured surface with it, into the test log.
_NEEDLE_BEARING_NAMES = frozenset({"needle", "_needle", "seeded_text", "planted"})


def test_no_assertion_in_this_module_can_ECHO_the_needle():
    """The leak this sweep introduced and then closed, pinned structurally.

    It was found by running the sweep's OWN mutation battery: mutation M4
    (stop seeding the error-path class) made a control fail, and the failure
    output carried the restricted pattern in clear text because the assertion
    read `assert needle in seeded_text`. Nothing about that line looks wrong.

    So the rule is structural rather than careful: membership goes through
    `_contains`, which returns a bool, and the assert tests the bool. `assert
    survived` prints `False`.

    The one admissible exception is a `not in` test against a FABRICATED marker
    (`_SQLITE_NEEDLE`), which is a literal in this file and restricted-free by
    construction.
    """
    tree = ast.parse(_read(SWEEP_PATH))
    offenders: List[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assert):
            continue
        for child in ast.walk(node.test):
            if isinstance(child, ast.Name) and child.id in _NEEDLE_BEARING_NAMES:
                offenders.append(f"line {node.lineno}: assert ... {child.id} ...")
            if (isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
                    and child.func.id == "_needle"):
                offenders.append(f"line {node.lineno}: assert ... _needle() ...")
    assert not offenders, (
        "these assertions can print a restricted pattern into the test log when "
        "they fail -- route the comparison through `_contains` and assert the "
        "bool:\n" + "\n".join(offenders))


def test_this_sweep_contains_no_skip_no_skipif_and_no_xfail():
    """The defect this whole file is shaped against: a skip reads as a pass.

    An unavailable pattern file, an unreachable surface, an empty capture and a
    path class that produced nothing must each FAIL BY NAME here. The first of
    this phase's six false-confidence defects was a masking test that skipped
    when its pattern file was absent; this pins the shape so it cannot return.
    """
    used = _skip_constructs(SWEEP_PATH)
    assert not used, (
        "this sweep uses a skip/xfail construct: " + "; ".join(used)
        + ". A skip reads as a pass and is how a masking gate becomes decorative.")


# ===========================================================================
# E. THE SQLITE SIDECAR: --scan-asset ALONE IS NOT DATABASE COVERAGE.
# ===========================================================================

_SQLITE_NEEDLE = "ZZQQ-OVERFLOW-STRADDLE-NEEDLE-ZZQQ"
_SQLITE_PAGE_SIZE = 512


def _sqlite_with_value(directory: pathlib.Path, value: str, *,
                       table: str = "t", column: str = "v") -> pathlib.Path:
    import sqlite3
    path = directory / "sidecar_probe.db"
    conn = sqlite3.connect(str(path))
    try:
        conn.execute(f"PRAGMA page_size={_SQLITE_PAGE_SIZE}")
        conn.execute("VACUUM")
        conn.execute(f'CREATE TABLE "{table}" ("{column}" TEXT)')
        conn.execute(f'INSERT INTO "{table}" VALUES (?)', (value,))
        conn.commit()
    finally:
        conn.close()
    return path


def test_the_cell_scan_catches_a_value_the_BYTE_scan_structurally_cannot(tmp_path):
    """Constructed, not quoted.

    SQLite spills a long value into an overflow chain and interleaves a 4-byte
    next-page pointer every page. A value straddling that boundary is not
    contiguous in the FILE, so `--scan-asset` cannot see it; `--scan-sqlite`
    reads the reassembled cell and does. This is why a `--scan-asset`-only run
    is not accepted as database coverage, and it is the reason `masking_readiness`
    refuses that combination.
    """
    def _hits(offset: int) -> Tuple[int, int]:
        directory = tmp_path / f"offset-{offset}"
        directory.mkdir()
        path = _sqlite_with_value(
            directory, ("a" * offset) + _SQLITE_NEEDLE + ("b" * 4000))
        return (len(cam.scan_asset(str(path), [_SQLITE_NEEDLE], strict=False)),
                len(cam.scan_sqlite(str(path), [_SQLITE_NEEDLE])))

    # A NON-straddling offset first: the needle sits wholly inside one overflow
    # page, both scans see it, and the byte scan is therefore working. Without
    # this half a broken byte scan would "prove" the gap on every offset.
    for offset in (100, 610, 1120):
        byte_hits, cell_hits = _hits(offset)
        assert byte_hits and cell_hits, (
            f"at offset {offset} the needle is contiguous in the file and both "
            f"scans should see it, but got byte={byte_hits} cell={cell_hits}. The "
            "byte scan is broken, so any 'gap' below would be an artefact.")
    # ...and then the straddling offsets, where only the cell scan can see it.
    proved = [offset for offset in (515, 522, 529, 536, 543)
              if _hits(offset) == (0, 1)]
    assert proved, (
        "no offset was found at which the raw byte scan misses a value the cell "
        "scan catches. If that is genuinely true of this SQLite build, the "
        "both-modes requirement needs re-deriving -- do not weaken it on the "
        "strength of a passing byte scan.")


def test_the_cell_scan_sees_a_TABLE_name_and_a_COLUMN_name_the_schema_carries(tmp_path):
    """A restricted string can hide in an identifier, which no row walk reaches.
    Both identifier surfaces are proved able to fail."""
    for kind, kwargs in (("table", {"table": f"tbl_{_SQLITE_NEEDLE}"}),
                         ("column", {"column": f"col_{_SQLITE_NEEDLE}"})):
        directory = tmp_path / f"identifier-{kind}"
        directory.mkdir()
        path = _sqlite_with_value(directory, "nothing restricted here", **kwargs)
        hits = cam.scan_sqlite(str(path), [_SQLITE_NEEDLE])
        assert hits, f"a restricted {kind} NAME was not reported by the cell scan"
        assert any(issue.surface in ("raw", "filename") for issue in hits)


def test_a_clean_synthetic_sidecar_is_reported_clean(tmp_path):
    """The other half of every control: the scan must also be capable of passing,
    or a failing run says nothing."""
    directory = tmp_path / "clean-sidecar"
    directory.mkdir()
    path = _sqlite_with_value(directory, "nothing restricted here")
    assert not cam.scan_sqlite(str(path), [_SQLITE_NEEDLE])
    assert not cam.scan_asset(str(path), [_SQLITE_NEEDLE], strict=False)


def test_the_strict_invocation_requires_BOTH_repo_and_asset(tmp_path):
    """`--strict` without both surfaces is a usage error, not a narrower scan."""
    patterns = tmp_path / "p"
    patterns.write_text(_SQLITE_NEEDLE + "\n", encoding="utf-8")
    result = _run_scan(["--strict", "--scan-repo"], patterns)
    assert result.returncode == 2, result.stderr
    assert "--strict requires BOTH" in result.stderr


def test_the_recorded_production_database_scans_name_both_modes_and_a_result():
    """The executor-run evidence, pinned in shape.

    THIS IS NOT A CI GATE and must not be read as one: the three sidecars are
    gitignored and ~390-470 MB each, so no runner holds them. What is asserted
    here is that the record is complete and internally honest; the attestation
    quotes it and says the same thing.
    """
    assert len(PRODUCTION_DATABASE_SCANS) == 6, PRODUCTION_DATABASE_SCANS
    for name, record in PRODUCTION_DATABASE_SCANS.items():
        assert name.endswith(".db"), name
        for mode in ("--strict", "--scan-repo", "--scan-asset", "--scan-sqlite"):
            assert mode in record["modes"], f"{name} was not scanned with {mode}"
        assert record["result"] == "clean", f"{name}: {record['result']}"
        assert record["seconds"] > 0 and record["date"]
        assert len(record["what"]) > 20, f"{name} has no stated purpose"
    assert masking_readiness(
        pattern_set_loaded=True,
        modes_run=frozenset(PRODUCTION_DATABASE_SCANS[
            "discovery-public-136rebuild.db"]["modes"]))[0] == "MET"


def test_the_recorded_production_scans_are_shown_NON_VACUOUS():
    """A clean run over a database proves nothing unless the same walk has been
    seen reporting. Each entry is a value read OUT of the deployed artifact and
    fed back in as the whole pattern set."""
    assert len(PRODUCTION_SQLITE_NON_VACUITY) >= 3
    for label, record in PRODUCTION_SQLITE_NON_VACUITY.items():
        assert record["exit"] != 0, f"{label} did not make the scan fail"
        assert record["hits"] >= 1, f"{label} reported no hits"
    assert any("TABLE NAME" in label for label in PRODUCTION_SQLITE_NON_VACUITY), (
        "the schema/identifier pass over the real artifact was never shown to "
        "report, so only the row walk is evidenced")


def _manifest_named_databases() -> Tuple[Set[str], Set[str]]:
    """`(named by a manifest, present on disk)`.

    Derived from the manifests themselves, so a rebuild that repoints a manifest
    puts its new artifact in scope without this file being edited. It found one
    on its first run: three databases had been scanned and the artifact the
    repo's own `manifest.json` resolves -- the one a LOCAL run of the app serves
    -- had not.
    """
    directory = REPO_ROOT / "discovery_data"
    named: Set[str] = set()
    for manifest in sorted(directory.glob("manifest*.json")):
        try:
            basename = json.loads(
                io.open(manifest, encoding="utf-8").read()).get("asset_basename")
        except (OSError, ValueError):                         # pragma: no cover
            continue
        if basename:
            named.add(f"{basename}.db")
    present = {name for name in named if (directory / name).is_file()}
    return named, present


def test_every_discovery_database_present_on_this_machine_is_in_the_record():
    """Where the artifacts ARE present, the record must cover every one a
    manifest names.

    Where they are not -- every CI runner, because `discovery_data/` is
    gitignored -- this covers nothing, and the run REPORTS that (see
    `test_zz_report_what_this_sweep_scanned`) instead of letting an empty set
    read as a clean result. The database coverage claim belongs to the
    executor-run record above and to the attestation, both of which say so.
    """
    _named, present = _manifest_named_databases()
    unscanned = sorted(present - set(PRODUCTION_DATABASE_SCANS))
    assert not unscanned, (
        "these discovery databases are present and named by a manifest but "
        "appear in no recorded scan: " + repr(unscanned))


ATTESTATION_PATH = (".planning/phases/136-read-surfaces-connections-panel-"
                    "work-witnesses/136-FLAG-ON-READINESS.md")

#: The figure ruling U retired. It was wrong -- built by adding main-pool
#: `fills_gap` to UNFILTERED `refines_granularity` and `container_predicts` --
#: and it is scanned for because a retired number is the one most likely to be
#: copied forward from an old draft.
_RETIRED_LAUNCH_FIGURE = "13,285"


def _recorded_launch_figures() -> FrozenSet[str]:
    """Every launch figure the attestation records, read OUT of the attestation.

    Derived rather than listed here so a rebake that moves the numbers moves
    this check with it: the attestation is where they are recorded WITH their
    artifact and audience, so it is the right authority.
    """
    import re
    text = _read(ATTESTATION_PATH)
    heading = "### The launch figures"
    assert heading in text, (
        f"{ATTESTATION_PATH} no longer carries a {heading!r} section, so the "
        "forbidden set cannot be derived. Do NOT fall back to a list written "
        "here: the attestation is the authority precisely because it records "
        "each figure WITH its artifact and audience.")
    start = text.index(heading)
    end = text.index("## 5.", start)
    figures = set(re.findall(r"\*\*([\d,]{4,})\*\*", text[start:end]))
    return frozenset(figures | {_RETIRED_LAUNCH_FIGURE})


def test_no_launch_figure_reaches_the_CHANGELOG():
    """A number written into a changelog outlives the artifact that produced it,
    exactly as one written into a translation would -- and 136-22's no-literals
    guard scans source and translations, not `CHANGELOG.md`.

    The forbidden set is DERIVED from the attestation's own recorded table, and
    the derivation is asserted non-empty: a parse that silently found nothing
    would report a clean changelog it never inspected, which is this phase's
    signature defect.
    """
    figures = _recorded_launch_figures()
    assert len(figures) >= 5, (
        "the launch-figure table could not be parsed out of the attestation "
        f"(found {sorted(figures)}) -- this check would pass vacuously")
    changelog = _read("CHANGELOG.md")
    found = sorted(figure for figure in figures if figure in changelog)
    assert not found, (
        "these launch figures appear in CHANGELOG.md: " + repr(found)
        + ". They are properties of one artifact and will move on the next "
          "rebuild; record them in the attestation, with their sidecar version "
          "and audience, and nowhere else.")


def test_the_changelog_says_flag_gated_and_claims_no_launch():
    """The release note must not read as a user-facing launch. Deployed and
    gated are different claims, and the difference is the posture of the whole
    milestone."""
    changelog = _read("CHANGELOG.md")
    section_start = changelog.index("### Built and deployed, NOT public")
    section = changelog[section_start:changelog.index("### New Features", section_start)]
    for phrase in ("DISCOVERY_ENABLED", "hide", "not live"):
        assert phrase.lower() in section.lower(), (
            f"the changelog entry does not say {phrase!r}")
    ASSERTED_PROHIBITED = ("now available to", "is now live", "launched", "public beta")
    for phrase in ASSERTED_PROHIBITED:
        assert phrase not in section.lower(), (
            f"the changelog entry claims a launch: {phrase!r}")


def test_zz_report_what_this_sweep_scanned(clean_capture, capsys):
    """Last by name. Prints what each class covered, because a sweep that cannot
    say what it looked at is a sweep nobody can check."""
    with capsys.disabled():
        print("\n[136-19] cross-surface masking sweep:")
        for path_class in PATH_CLASSES:
            print(f"  {path_class:<14} {len(clean_capture['texts'][path_class]):>9,} chars"
                  f"  -> {CLASS_FILES[path_class]}")
        print(f"  enveloped reads scanned: {len(payload_map())}")
        print(f"  link targets scanned:    {len(set(clean_capture['hrefs']))}")
        _named, present = _manifest_named_databases()
        print(f"  discovery databases present here: {len(present)} of "
              f"{len(PRODUCTION_DATABASE_SCANS)} in the executor-run record"
              + ("" if present else
                 "  -- NONE present, so THIS RUN covers no database at all; "
                 "the database scans are executor-run evidence, not a CI gate"))
        state, reason = masking_readiness(
            pattern_set_loaded=bool(cam.load_patterns()),
            modes_run=frozenset({"--scan-asset", "--scan-sqlite"}))
        print(f"  D-25 masking item: {state} -- {reason}")
    assert clean_capture["texts"]
