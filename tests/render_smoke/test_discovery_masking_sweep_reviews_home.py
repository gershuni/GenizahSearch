"""The REL-01 masking sweep over the two surfaces the D-25 sweep never reached.

WHY THIS FILE EXISTS, and why it is not a copy of the sweep beside it.

`test_discovery_masking_sweep.py` (136-19, attested 2026-08-05) covers four
egress classes over the connections panel and the findings page. The roadmap
recorded three surfaces as having shipped after that attestation and never been
swept. Checking each against the sweep rather than against the roadmap found
that claim is stale by one:

  * the EXCERPT view is already swept -- `_excerpt_loader` drives
    `render_excerpt_disclosure` through six states (direct / reprojected /
    nowork / empty / raise / busy), added with the surface on 2026-08-13, and
    the line-coverage gate over `findings_rows.py` keeps it driven;
  * the BETA IDENTIFICATION REVIEWS are not swept -- no test in that module
    references `identification_review.py`, and its own suite asserts wording
    and storage, never restricted-pattern egress;
  * the HOMEPAGE DISCOVERY PROMOTION is not swept -- `test_home_teaser_render_
    smoke.py` checks honesty wording (`_FORBIDDEN_SUBSTRINGS_EN`), which is a
    different gate: it asks whether the copy overclaims, not whether an
    artifact value reached the page.

So this module sweeps those two, at the same standard: a REAL render, the REAL
pattern set, and a positive control per surface that has been watched failing.

WHAT THE REVIEWS SURFACE ADDS THAT NO EXISTING SWEEP HAS.

Two egress shapes the four-class sweep does not model, because until the
reviews shipped no discovery surface had them:

  1. A MAILTO LINK TARGET carrying artifact values (`report_mailto` embeds
     `identification_id` and `sidecar_version` in a URL a reader can copy).
     The existing sweep's copy/export inventory asserts an ABSENCE over four
     modules and `identification_review.py` is not one of them, so on a clean
     checkout that absence was true of the scanned set and false of the
     product.
  2. An OUTBOUND WRITE. Every other discovery surface is read-only; this one
     posts `ReviewSubmission` to Supabase carrying `sys_id`, `page_id`,
     `work_id` and `displayed_relation`. That leaves the building, so it is
     swept as its own class rather than folded into the rendered text.

Helpers are IMPORTED from the sweep beside this one, never re-implemented: one
needle derivation, one scan invocation, one pattern-file resolution. In
particular the needle is generated from the pattern file at run time and is
never typed into this file -- `test_this_module_hardcodes_no_restricted_name`
proves that about this file itself.
"""
from __future__ import annotations

import ast
import asyncio
import io
import json
import pathlib
import sys
from typing import Any, Dict, List, Optional, Tuple

import pytest

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import web.components.identification_review as ir  # noqa: E402
import web.identification_reviews as irs  # noqa: E402
from tests.render_smoke import test_discovery_masking_sweep as sweep  # noqa: E402
from tests.render_smoke import test_panel_render_smoke as tp  # noqa: E402

# Reused, not re-derived. Each of these is the sweep's single definition.
PatternSetUnavailable = sweep.PatternSetUnavailable
_configured_pattern_file = sweep._configured_pattern_file
_needle = sweep._needle
_needle_pattern_file = sweep._needle_pattern_file
_run_scan = sweep._run_scan
_client_hrefs = sweep._client_hrefs
_client_texts = sweep._client_texts
_Patch = sweep._Patch

THIS_PATH = "tests/render_smoke/test_discovery_masking_sweep_reviews_home.py"
REVIEW_MODULE = "web/components/identification_review.py"
HOME_MODULE = "web/pages/home.py"

LANGS = ("en", "he")

#: One file per class, so a control names WHICH class leaked rather than only
#: that something did.
CLASS_REVIEWS = "reviews-rendered"
CLASS_REVIEW_LINKS = "reviews-link-targets"
CLASS_REVIEW_WRITE = "reviews-outbound-write"
CLASS_HOME = "home-promotion"
SURFACE_CLASSES = (CLASS_REVIEWS, CLASS_REVIEW_LINKS, CLASS_REVIEW_WRITE,
                   CLASS_HOME)

CLASS_FILES = {
    CLASS_REVIEWS: "r1-reviews-rendered.txt",
    CLASS_REVIEW_LINKS: "r2-reviews-link-targets.txt",
    CLASS_REVIEW_WRITE: "r3-reviews-outbound-write.txt",
    CLASS_HOME: "r4-home-promotion.txt",
}

#: Copy/export egress APIs, same inventory the sweep uses. Kept as a name here
#: so the reviews module is measured against the SAME list rather than a
#: shorter one written to pass.
_COPY_EXPORT_APIS = sweep._COPY_EXPORT_APIS


# ===========================================================================
# DERIVATIONS -- properties of the CODE, so they cannot drift.
# ===========================================================================

def _read(rel: str) -> str:
    return io.open(REPO_ROOT / rel, encoding="utf-8").read()


def _ui_emitting_functions(rel_path: str) -> Dict[str, ast.AST]:
    """Delegate to the sweep's derivation so both modules agree on what
    'paints something' means."""
    return sweep._ui_emitting_functions(rel_path)


async def _settle() -> None:
    """Let handler-scheduled work run WHILE the client is still alive.

    This is load-bearing, not hygiene. Both surfaces build their real content
    off the click rather than in it: the review dialog is constructed on first
    open (`_open` -> `_build_dialog`) and the homepage's count arrives from a
    deferred launch read. NiceGUI dispatches those through
    `background_tasks`, so control has to return to the loop before they run.
    Measured while writing this file: without the yield the capture held 5
    elements and no dialog at all -- no Radio, no Textarea, no email fallback --
    and every scan over it was clean because there was nothing in it. With the
    yield: 22 elements including the whole form. A capture that exits its
    client context first gets the deferred build raising
    `parent element ... has been deleted` into a log nobody reads, and reports
    a clean surface it never rendered.

    BOUNDED, and that is the second thing measured here: the homepage installs
    `ui.timer` loops whose tasks never finish, so awaiting the pending set to
    completion hangs the suite forever rather than failing. It is waited on with
    a timeout and whatever is still running is left alone -- a deferred build
    that has not finished in this long is not going to paint.
    """
    for _ in range(8):
        await asyncio.sleep(0)
    pending = [task for task in asyncio.all_tasks()
               if task is not asyncio.current_task() and not task.done()]
    if pending:
        await asyncio.wait(pending, timeout=2.0)


# ===========================================================================
# THE REVIEWS SURFACE. Rendered for real, in every state a reader can reach.
# ===========================================================================

def _review_item(seed: Optional[str]) -> Dict[str, Any]:
    """One identification leaf as the panel hands it to the review action.

    Every value here is artifact-derived on the live site, so every value is a
    place a restricted string could arrive. When `seed` is set it replaces them
    all -- that is the positive control's payload.
    """
    return {
        "identification_id": seed or "id-000123",
        "sys_id": seed or "990000000000000944",
        "page_id": seed or "990000000000000944_IE1_P000002_FL3",
        "page_number": 2,
        "first_match_page": 2,
        "work_id": seed or "REF2:w000948",
        "display_work_id": seed or "w000948",
        "rendered_relation": "direct_witness",
    }


def _published_reviews(seed: Optional[str]) -> List[Dict[str, Any]]:
    """Approved community reviews, including the free-text comment -- the one
    value on this surface a HUMAN wrote, and the only one the artifact cannot
    vouch for."""
    return [
        {
            "relation_verdict": irs.RELATION_DIRECT_WITNESS,
            "direct_novelty": irs.DIRECT_NOVELTY_POTENTIALLY_NEW,
            "comment": seed or "מתאים לגמרי לנוסח שלפנינו",
        },
        {
            "relation_verdict": irs.RELATION_MANUSCRIPT_QUOTES_WORK,
            "direct_novelty": None,
            "comment": "",
        },
        {
            "relation_verdict": "not_in_the_frozen_vocabulary",
            "direct_novelty": seed or irs.DIRECT_NOVELTY_ALREADY_KNOWN,
            "comment": seed or "citation only",
        },
    ]


def _render_reviews_surface(seed: Optional[str] = None) -> Dict[str, Any]:
    """Render every reviews state in a real client and hand back what a reader
    could read OR copy.

    The states, and why each is here rather than assumed equivalent:
      * reviews ON  -> the action button, then the DIALOG opened (the dialog is
        built lazily on first open, so an un-opened action paints none of the
        radio labels, the textarea, the moderation note or the email fallback);
      * reviews OFF -> the mailto fallback link, which is the ONLY thing that
        paints in that state and carries two artifact values;
      * published reviews -> the public card, whose values live in `aria-label`
        and tooltip text rather than in element text;
      * both languages, because the Hebrew vocabulary is a separate dict.
    """
    tp._ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    holder: Dict[str, Any] = {}
    patch = _Patch()

    async def _run() -> None:
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_reviews_masking_capture")) as client:
            with client:
                item = _review_item(seed)
                for lang in LANGS:
                    # a) reviews ENABLED: the action, and the dialog opened.
                    patch.setattr(ir, "reviews_enabled", lambda: True)
                    ir.render_identification_review_action(
                        item, lang, sidecar_version=seed or "v42lit-1",
                        shown_relation="direct_witness")
                    ir.render_published_identification_reviews(
                        _published_reviews(seed), lang)

                    # b) reviews DISABLED: only the mailto fallback paints.
                    patch.setattr(ir, "reviews_enabled", lambda: False)
                    ir.render_identification_review_action(
                        item, lang, sidecar_version=seed or "v42lit-1")

                    # c) the two early returns, so their absence is captured
                    #    rather than presumed: no identification, no version.
                    patch.setattr(ir, "reviews_enabled", lambda: True)
                    ir.render_identification_review_action(
                        {k: v for k, v in item.items()
                         if k != "identification_id"},
                        lang, sidecar_version=seed or "v42lit-1")
                    ir.render_identification_review_action(
                        item, lang, sidecar_version=None)

                    # d) no approved reviews -> paints nothing.
                    ir.render_published_identification_reviews([], lang)

                # Open every action the render produced. The dialog is built on
                # first open, so without this the whole form is uncaptured.
                await tp._drive_click_handlers(client)
                await _settle()
                await sweep._drive_value_change_handlers(client)
                await _settle()
        holder["client"] = client

    try:
        asyncio.run(_run())
    finally:
        patch.undo()

    client = holder["client"]
    try:
        texts = _client_texts(client)
        hrefs = _client_hrefs(client)
        # aria-label and tooltip text carry the published-review provenance and
        # each verdict; neither is element text, so a text-only capture would
        # scan a surface a reader can still read.
        aria: List[str] = []
        for element in client.elements.values():
            props = getattr(element, "_props", None) or {}
            for key in ("aria-label", "title", "label", "placeholder"):
                value = props.get(key)
                if isinstance(value, str) and value.strip():
                    aria.append(f"{key}={value}")
    finally:
        client.delete()
    return {"texts": texts, "hrefs": hrefs, "aria": aria}


def _review_link_targets(seed: Optional[str] = None) -> List[str]:
    """`report_mailto` through the SHIPPED builder, so the capture cannot
    disagree with the product about what a review link is."""
    out: List[str] = []
    for lang in LANGS:
        url = ir.report_mailto(_review_item(seed), lang, seed or "v42lit-1")
        if url:
            out.append(url)
    # The two None-returning branches, recorded so a later change that starts
    # emitting a URL there shows up as a new captured line.
    for item, version in (({}, "v42lit-1"), (_review_item(seed), None)):
        out.append(f"withheld={ir.report_mailto(item, 'en', version)!r}")
    return out


def _review_outbound_write(seed: Optional[str] = None) -> List[str]:
    """The payload that LEAVES THE BUILDING, captured by intercepting the
    storage boundary instead of trusting the dialog's own reading of it.

    `submit_review` is patched to record its `ReviewSubmission`; the dialog is
    then driven to submit. What is scanned is therefore the object the boundary
    actually received.
    """
    tp._ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    captured: List[str] = []
    patch = _Patch()

    def _record(submission: Any, client: Any = None) -> None:
        del client
        fields = {
            name: getattr(submission, name, None)
            for name in ("identification_id", "sidecar_version",
                         "relation_verdict", "direct_novelty", "comment",
                         "sys_id", "page_id", "page_number", "work_id",
                         "displayed_relation", "anonymous_key")
        }
        captured.append(json.dumps(fields, ensure_ascii=False, default=str))

    async def _run() -> None:
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_reviews_write_capture")) as client:
            # Everything stays INSIDE `with client:`. The dialog is built on
            # first open and `ui.dialog()` reaches for `context.client.layout`,
            # so driving the action from outside the context raises instead of
            # building the form -- and the capture would then be empty for a
            # reason that has nothing to do with masking.
            with client:
                ir.render_identification_review_action(
                    _review_item(seed), "en",
                    sidecar_version=seed or "v42lit-1",
                    shown_relation=seed or "direct_witness")
                await tp._drive_click_handlers(client)  # opens -> builds dialog
                await _settle()
                # Choose a verdict, then submit. Without a verdict `_submit`
                # notifies and returns, so an undriven form would capture
                # nothing and this class would report a clean surface it never
                # exercised.
                for element in list(client.elements.values()):
                    if type(element).__name__ == "Radio":
                        options = getattr(element, "options", None) or {}
                        for key in options:
                            if key == irs.RELATION_DIRECT_WITNESS:
                                element.value = key
                                break
                    elif type(element).__name__ == "Textarea":
                        element.value = seed or "a reviewer's note"
                await sweep._drive_value_change_handlers(client)
                await _settle()
                await tp._drive_click_handlers(client)  # submit
                await _settle()
        client.delete()

    patch.setattr(ir, "reviews_enabled", lambda: True)
    patch.setattr(ir, "submit_review", _record)
    patch.setattr(ir, "get_user_client", lambda: None)
    patch.setattr(ir, "get_session_uuid", lambda: "session-uuid-probe")
    patch.setattr(ir, "anonymous_reviewer_key", lambda uuid: f"anon:{uuid}")

    async def _direct(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    patch.setattr(ir.run, "io_bound", _direct)
    try:
        asyncio.run(_run())
    finally:
        patch.undo()
    return captured


# ===========================================================================
# THE HOMEPAGE PROMOTION.
# ===========================================================================

def _render_home_surface(seed: Optional[str] = None,
                         count: Any = 694) -> Dict[str, Any]:
    """The real homepage with discovery READY.

    NO ARTIFACT VALUE REACHES THIS SURFACE ANY MORE, and that is the claim these
    tests now hold up.

    Until 2026-09-04 the homepage read the discovery artifact in exactly one
    place -- `_fill_discovery_count`, the only `get_launch_stats_enveloped` call
    in `web/pages/home.py` -- and rendered one value from it, `meta.work_total`,
    behind `isinstance(total, int) and total > 0`. The honest claim then was "one
    artifact value reaches this surface and is guarded".

    The owner removed the two big promotion cards that day ("we are past the
    promotional phase") and `_fill_discovery_count` went with them. So the claim
    is now stronger and simpler: the homepage does not read the artifact, so
    nothing from it can reach the page whatever the artifact contains. That is
    asserted, not assumed -- `invoked` below records whether the read was called
    at all, and `test_the_home_reads_no_artifact_value_at_all` fails if it ever
    is.

    `seed` and `count` are KEPT rather than deleted: the patched read still
    returns them, so if a future edit reintroduces a read, these tests
    immediately have a needle to catch it with. A capture helper that stopped
    offering a poisoned value would have to be rebuilt at exactly the moment it
    was needed.
    """
    tp._ensure_sim()
    from nicegui import core, ui
    from nicegui.client import Client

    import web.discovery_assets as da
    import web.pages.home as home

    holder: Dict[str, Any] = {}
    patch = _Patch()

    invoked: List[str] = []

    async def _launch(*_a, **_k):
        # Records the call. The homepage is expected NEVER to make it since the
        # promotion cards were removed; `test_the_home_reads_no_artifact_value_
        # at_all` is what turns that expectation into a failure if it changes.
        invoked.append("get_launch_stats_enveloped")
        return {
            "status": "ok",
            "meta": {"work_total": seed if seed is not None else count},
            "items": [],
        }

    async def _run() -> None:
        core.loop = asyncio.get_running_loop()
        with Client(ui.page("/_home_masking_capture")) as client:
            with client:
                home.create_page()
                # The promotion's count is filled by a background task; drive
                # the click handlers too, so the carousel and the tool card
                # paint. Inside the context for the same reason as the reviews
                # capture: a handler that opens anything needs a live client.
                await tp._drive_click_handlers(client)
                await _settle()
        holder["client"] = client

    patch.setattr(da, "discovery_available", lambda: True)
    patch.setattr(home, "discovery_available", lambda: True)
    # The launch read is imported INSIDE `_fill_discovery_count`, so the module
    # that has to be patched is `web.discovery`, not this one's namespace. A
    # silent `except` here would leave the real read in place and the capture
    # would then depend on whatever sidecar the machine happens to have.
    import web.discovery as wd
    patch.setattr(wd, "get_launch_stats_enveloped", _launch)
    try:
        asyncio.run(_run())
    finally:
        patch.undo()

    client = holder["client"]
    try:
        texts = _client_texts(client)
        hrefs = _client_hrefs(client)
        # The promotion navigates with `ui.navigate.to(...)` inside click
        # handlers rather than through an `href`, and its title goes through
        # `tr()`, so neither the route nor an English title is a reliable
        # witness that it rendered. Its MARKERS are.
        markers: List[str] = []
        for element in client.elements.values():
            for mark in (getattr(element, "_markers", None) or ()):
                markers.append(f"mark={mark}")
    finally:
        client.delete()
    return {"texts": texts, "hrefs": hrefs, "markers": markers,
            "artifact_reads": list(invoked)}


# ===========================================================================
# THE CAPTURE, written OUTSIDE the working tree.
# ===========================================================================

def _build_capture(directory: pathlib.Path, seed: Optional[str]) -> Dict[str, Any]:
    reviews = _render_reviews_surface(seed)
    home = _render_home_surface(seed)
    payload = {
        CLASS_REVIEWS: reviews["texts"] + reviews["aria"],
        CLASS_REVIEW_LINKS: reviews["hrefs"] + _review_link_targets(seed),
        CLASS_REVIEW_WRITE: _review_outbound_write(seed),
        CLASS_HOME: home["texts"] + home["hrefs"] + home["markers"],
    }
    for name, lines in payload.items():
        path = directory / CLASS_FILES[name]
        path.write_text("\n".join(str(line) for line in lines) + "\n",
                        encoding="utf-8")
    return {"dir": directory, "lines": payload}


@pytest.fixture(scope="module")
def clean_capture(tmp_path_factory):
    """`tmp_path_factory` roots under pytest's basetemp, never the repo -- a
    capture written inside the tree would make this gate report its own
    artifact as the leak."""
    directory = tmp_path_factory.mktemp("rel01-reviews-home")
    capture = _build_capture(directory, seed=None)
    assert REPO_ROOT not in directory.parents and directory != REPO_ROOT, (
        "the capture must not be written inside the working tree")
    return capture


@pytest.fixture(scope="module")
def seeded_capture(tmp_path_factory):
    directory = tmp_path_factory.mktemp("rel01-reviews-home-seeded")
    return _build_capture(directory, seed=_needle())


# ===========================================================================
# A. THE CAPTURE IS REAL AND COVERS WHAT IT CLAIMS.
# ===========================================================================

def test_every_class_is_captured_and_none_is_a_stub(clean_capture):
    for name in SURFACE_CLASSES:
        path = clean_capture["dir"] / CLASS_FILES[name]
        assert path.is_file(), f"{name} produced no capture file"
        body = path.read_text(encoding="utf-8").strip()
        assert len(body) > 80, (
            f"the {name} capture is {len(body)} characters -- too short to be a "
            "real render, which means this class is a stub and its clean scan "
            "says nothing")


def test_the_reviews_capture_reaches_every_ui_emitting_function_of_the_module(
        clean_capture):
    """Derived from the CODE: every top-level function of the reviews module
    that can put something on a screen must appear in the rendered capture's
    provenance, not merely be importable."""
    del clean_capture
    expected = set(_ui_emitting_functions(REVIEW_MODULE))
    exercised = set()
    patch = _Patch()
    originals = {name: getattr(ir, name) for name in expected
                 if hasattr(ir, name)}

    def _wrap(name, fn):
        def _recorder(*args, **kwargs):
            exercised.add(name)
            return fn(*args, **kwargs)
        return _recorder

    for name, fn in originals.items():
        patch.setattr(ir, name, _wrap(name, fn))
    try:
        _render_reviews_surface(None)
    finally:
        patch.undo()
    missing = sorted(expected - exercised)
    assert not missing, (
        "these reviews renderers were never reached by the capture, so their "
        f"output was never scanned: {missing!r}")


def test_the_reviews_capture_holds_the_dialog_and_not_just_the_action(
        clean_capture):
    """The dialog is built lazily on first open. A capture that only rendered
    the action button would scan four words and call the surface clean."""
    body = (clean_capture["dir"] / CLASS_FILES[CLASS_REVIEWS]).read_text(
        encoding="utf-8")
    for key in ("title", "question", "comment", "moderation_note", "submit"):
        expected = ir.review_text(key, "en")
        assert expected in body, (
            f"the dialog's {key!r} text is absent from the capture -- the "
            "dialog was never opened, so the form was never scanned")
    hebrew = ir.review_text("title", "he")
    assert hebrew in body, "the Hebrew vocabulary was never rendered"


def test_the_link_class_holds_a_real_mailto_carrying_artifact_values(
        clean_capture):
    body = (clean_capture["dir"] / CLASS_FILES[CLASS_REVIEW_LINKS]).read_text(
        encoding="utf-8")
    assert "mailto:" in body, (
        "no mailto target was captured, yet `report_mailto` is the reviews "
        "surface's copy/export egress")
    assert "id-000123" in body and "v42lit-1" in body, (
        "the captured mailto carries neither the identification id nor the "
        "sidecar version, so it is not the real link a reader copies")


def test_the_outbound_write_class_holds_the_payload_the_boundary_received(
        clean_capture):
    """The dialog must have actually SUBMITTED. An unsubmitted form captures
    nothing and would report a clean outbound surface that never ran."""
    lines = [line for line in clean_capture["lines"][CLASS_REVIEW_WRITE]
             if line.strip()]
    assert lines, (
        "no ReviewSubmission reached the storage boundary -- the dialog was "
        "never driven to submit, so this class scanned nothing")
    payload = json.loads(lines[0])
    for field in ("identification_id", "sidecar_version", "relation_verdict",
                  "sys_id", "page_id", "work_id"):
        assert payload.get(field), (
            f"the captured submission has no {field!r}; this is the payload "
            "that leaves the building, so every artifact-derived field must be "
            "present in what gets scanned")


#: Every homepage entry point gated on the discovery predicate. Derived from
#: the markers the page sets, so a further entry point added later shows up here
#: as a capture that no longer contains it rather than as silence.
#:
#: `discovery-announcement` was the fourth until 2026-09-04, when the owner
#: removed the two big promotion cards ("we are past the promotional phase").
#: The surface is NOT withdrawn -- these three remain, each still gated on the
#: same availability predicate: the capability chip, the carousel slide and the
#: Research Tools card.
HOME_DISCOVERY_MARKERS = ("home-chip-computed", "home-carousel-computed",
                          "computed-tool-card")


def test_the_home_capture_contains_every_discovery_entry_point(clean_capture):
    """All four gated entry points, by MARKER.

    Not by route and not by title: the promotion navigates from a click handler
    (`ui.navigate.to`) rather than an `href`, and its title is rendered through
    `tr()`, so on a Hebrew-default client an English-title assertion fails while
    the surface is rendering perfectly. Both of those were tried here first and
    were wrong about the capture rather than about the page.
    """
    body = (clean_capture["dir"] / CLASS_FILES[CLASS_HOME]).read_text(
        encoding="utf-8")
    missing = [mark for mark in HOME_DISCOVERY_MARKERS
               if f"mark={mark}" not in body]
    assert not missing, (
        f"these discovery entry points never rendered: {missing!r} -- the "
        "homepage capture does not reach the promotion, so its clean scan says "
        "nothing about it")


def test_the_home_reads_no_artifact_value_at_all(clean_capture):
    """THE SAFETY PROPERTY for this surface, and it is now an ABSENCE.

    Replaces `test_the_home_capture_holds_the_DEFERRED_count_not_the_first_paint`
    and `test_the_home_promotion_refuses_a_non_integer_total`, both of which
    asserted the promotion card rendered and its int guard held. The card was
    removed on 2026-09-04 and `_fill_discovery_count` went with it, so those
    tests were asserting a surface that no longer exists.

    What replaces them is stronger. The old claim was "one artifact value
    reaches this page and is guarded by `isinstance(total, int)`". The claim now
    is "no artifact value reaches this page", which needs no guard to hold and
    cannot be defeated by a guard being wrong.

    Proven three ways, because an absence is exactly the kind of claim that
    passes vacuously:
      1. the patched read is never CALLED;
      2. a restricted needle handed to that read appears nowhere in the capture;
      3. an INTEGER handed to it appears nowhere either -- the control. Without
         (3) this test would still pass if the capture rendered nothing at all,
         which is the trap the previous version of this suite fell into.
    """
    del clean_capture  # this test builds its own poisoned surfaces

    poisoned = _render_home_surface(seed=_needle())
    assert poisoned["artifact_reads"] == [], (
        "the homepage read the discovery artifact: %r. It stopped doing so when "
        "the promotion cards were removed; if a read is back, this surface needs "
        "a guard again and a leak control to go with it."
        % (poisoned["artifact_reads"],))

    joined = "\n".join(str(line) for line in
                        poisoned["texts"] + poisoned["markers"])
    # Reduced to a bool BEFORE the assert: pytest prints the operands of a
    # failing assertion, so `assert _needle() not in joined` would put the
    # restricted value into the CI log on the one run where it matters.
    leaked = _needle() in joined
    assert not leaked, (
        "a restricted value from the discovery artifact reached the homepage "
        "even though the page makes no artifact read -- so it arrived by some "
        "route this sweep does not model.")

    # (3) THE CONTROL. An integer total must not appear either, and the surface
    # must actually have rendered -- otherwise the two assertions above are
    # statements about an empty page.
    reachable = _render_home_surface(count=8675309)
    assert reachable["artifact_reads"] == []
    rendered = "\n".join(str(line) for line in
                         reachable["texts"] + reachable["markers"])
    assert "mark=home-chip-computed" in rendered, (
        "the homepage's discovery entry points did not render, so this test "
        "proves nothing about what does or does not reach them")
    assert "8,675,309" not in rendered and "8675309" not in rendered, (
        "an artifact integer rendered on the homepage; the page is reading the "
        "artifact again")


def test_the_copy_export_inventory_now_includes_the_reviews_module():
    """The existing sweep asserts an ABSENCE of clipboard/download APIs over
    four modules. The reviews module is a fifth surface; its egress is a link
    target, which is captured above. This test pins the rest of the inventory
    at zero so a future clipboard or download path cannot arrive unswept."""
    source = _read(REVIEW_MODULE)
    found = sorted(api for api in _COPY_EXPORT_APIS if api in source)
    assert not found, (
        f"{REVIEW_MODULE} gained a copy/export API this sweep does not "
        f"capture: {found!r}. Add a capture class for it before shipping.")


# ===========================================================================
# B. THE REAL SCAN.
# ===========================================================================

def test_the_capture_and_the_repository_pass_the_real_masking_scan(
        clean_capture):
    """`--strict` over both surfaces AND the repository, with the REAL pattern
    set. WHEN THE PATTERN FILE IS ABSENT THIS FAILS rather than skipping: a
    sweep that searches for no patterns reports clean surfaces it never
    inspected."""
    patterns = _configured_pattern_file()
    result = _run_scan(["--strict", "--scan-repo",
                        "--scan-asset", str(clean_capture["dir"])], patterns)
    assert result.returncode == 0, (
        "the masking scan reported a restricted string on the reviews surface, "
        "on its link targets, in its outbound write, or on the homepage "
        "promotion. The report names a path, an offset and a pattern INDEX and "
        f"never the pattern text:\n{result.stdout[-4000:]}\n"
        f"{result.stderr[-2000:]}")


def test_scanning_the_repository_after_the_capture_is_still_clean(clean_capture):
    del clean_capture
    result = _run_scan(["--scan-repo"], _configured_pattern_file())
    assert result.returncode == 0, (
        "`--scan-repo` is dirty after this sweep ran -- most likely a capture "
        f"was written inside the working tree:\n{result.stdout[-2000:]}")


def test_this_module_hardcodes_no_restricted_name():
    result = _run_scan(["--scan-asset", THIS_PATH], _configured_pattern_file())
    assert result.returncode == 0, (
        "this test module itself carries a restricted pattern:\n"
        f"{result.stdout[-2000:]}")


# ===========================================================================
# C. THE POSITIVE CONTROLS -- one per class, each watched failing.
# ===========================================================================

#: The classes a seeded needle can actually reach. `CLASS_HOME` is deliberately
#: absent: its only artifact value is int-guarded, so a string needle CANNOT
#: land there, and a control demanding that it does would be a control written
#: to fail. That surface's safety is asserted by
#: `test_the_home_promotion_refuses_a_non_integer_total` instead, which is the
#: stronger claim.
SEEDABLE_CLASSES = (CLASS_REVIEWS, CLASS_REVIEW_LINKS, CLASS_REVIEW_WRITE)


@pytest.mark.parametrize("surface_class", SEEDABLE_CLASSES)
def test_CONTROL_the_sweep_fails_on_a_seeded_needle(surface_class,
                                                    seeded_capture, tmp_path):
    """The mechanism, per class. A clean run above means nothing unless this
    same scan, over this same capture path, CATCHES a restricted value placed
    where the artifact's own values go.

    The needle is read from the pattern file at run time and never echoed: the
    assertion reports the class and the scanner's own index-only report.
    """
    patterns = _needle_pattern_file(tmp_path)
    path = seeded_capture["dir"] / CLASS_FILES[surface_class]
    result = _run_scan(["--scan-asset", str(path)], patterns)
    assert result.returncode != 0, (
        f"the {surface_class} capture was seeded with a restricted value in "
        "the place an artifact value goes, and the scan reported it CLEAN. "
        "This class's clean run is therefore worthless -- the capture does not "
        "reach the surface it claims to, or the scan cannot see this shape of "
        f"egress:\n{result.stdout[-2000:]}")
    assert CLASS_FILES[surface_class] in result.stdout, (
        "the scan fired but did not name this class's file, so the control "
        f"proves nothing about {surface_class}:\n{result.stdout[-2000:]}")


def test_CONTROL_the_GATING_invocation_itself_fails_on_a_seeded_capture(
        seeded_capture, tmp_path):
    """The controls above scan ONE file. This one runs the exact invocation the
    gate runs -- `--strict --scan-repo --scan-asset <dir>` -- over the seeded
    directory, because that is the command whose exit code decides whether this
    surface ships, and a per-file control does not prove the directory walk
    reaches these files under `--strict`.

    The mutation is at the DATA level, not the assertion level: the needle
    enters through `_review_item`, the same dict shape the panel hands the
    component, and the SHIPPED renderer is what puts it on screen. Nothing in
    the product source is edited, and nothing in this test's expectations is
    relaxed.
    """
    patterns = _needle_pattern_file(tmp_path)
    result = _run_scan(["--strict", "--scan-repo",
                        "--scan-asset", str(seeded_capture["dir"])], patterns)
    assert result.returncode != 0, (
        "the GATING invocation reported a seeded capture clean. Every green run "
        "of `test_the_capture_and_the_repository_pass_the_real_masking_scan` is "
        "therefore worthless -- under `--strict` the directory walk is not "
        f"reaching these capture files:\n{result.stdout[-2000:]}")


def test_CONTROL_a_clean_capture_is_reported_clean_against_the_needle_only(
        clean_capture, tmp_path):
    """The controls' false-positive half: the same one-pattern file over the
    UNSEEDED capture must exit 0. Without this, a control that fires proves
    only that the scanner fires on everything."""
    patterns = _needle_pattern_file(tmp_path)
    result = _run_scan(["--scan-asset", str(clean_capture["dir"])], patterns)
    assert result.returncode == 0, (
        "the unseeded capture matched the needle -- the positive controls "
        f"above cannot distinguish a leak from noise:\n{result.stdout[-2000:]}")


def test_an_unset_pattern_file_FAILS_rather_than_reporting_clean(monkeypatch,
                                                                 tmp_path):
    """With no env var AND no local pattern file, resolution RAISES.

    `tmp_path` is the empty root, not `os.devnull`'s parent: on Windows
    `Path('nul').parent` is `.`, which resolves to the repository and finds the
    real `.masking_patterns` -- so that version of this test passed for the
    wrong reason and proved nothing. Measured while writing this file.
    """
    monkeypatch.delenv("MASKING_SCAN_PATTERNS_FILE", raising=False)
    monkeypatch.setattr(sweep, "REPO_ROOT", tmp_path)
    assert not (tmp_path / ".masking_patterns").exists()
    with pytest.raises(PatternSetUnavailable):
        _configured_pattern_file()


def test_this_sweep_contains_no_skip_no_skipif_and_no_xfail():
    """A masking gate that can skip is a masking gate that reports green on the
    one machine where it matters least.

    Uses the sweep's AST walk rather than a substring search, for the reason
    its own docstring gives: a string check over this file matches the list of
    forbidden names the check itself carries, fails on itself, and then gets
    "fixed" by weakening it. That is exactly what the first draft of this test
    did.
    """
    used = sweep._skip_constructs(THIS_PATH)
    assert not used, (
        "this sweep uses a skip/xfail construct: " + "; ".join(used)
        + ". A skip reads as a pass and is how a masking gate becomes "
          "decorative.")


def test_no_assertion_in_this_module_can_ECHO_the_needle():
    """Assertion messages here report class names, paths and the scanner's
    index-only output. An f-string interpolating the needle would put a
    restricted value into CI logs."""
    tree = ast.parse(_read(THIS_PATH))
    offenders: List[Tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assert) or node.msg is None:
            continue
        for sub in ast.walk(node.msg):
            if isinstance(sub, ast.Name) and sub.id in {"needle", "_needle"}:
                offenders.append((node.lineno, sub.id))
            if isinstance(sub, ast.Call):
                func = sub.func
                if isinstance(func, ast.Name) and func.id == "_needle":
                    offenders.append((node.lineno, "_needle()"))
    assert not offenders, (
        f"these assertion messages could echo the needle: {offenders!r}")


def test_zz_report_what_this_sweep_scanned(clean_capture, capsys):
    """A build record of what ran, with sizes -- never contents."""
    with capsys.disabled():
        print("\n  REL-01 reviews+home masking sweep")
        for name in SURFACE_CLASSES:
            path = clean_capture["dir"] / CLASS_FILES[name]
            lines = len(clean_capture["lines"][name])
            print(f"    {name:24} {lines:>5} captured values  "
                  f"{path.stat().st_size:>7,} bytes")
        print(f"    surfaces: {REVIEW_MODULE}, {HOME_MODULE}")
