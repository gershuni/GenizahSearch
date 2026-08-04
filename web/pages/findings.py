# -*- coding: utf-8 -*-
"""The corpus-wide "Computed Identifications" findings page (Phase 136, plan 136-16).

The surface the owner described as the reason the milestone is worth shipping --
"a big new amazing feature... maximum ability to see new findings" -- and the one
place the caveat is given a designed home rather than being buried as fine print.

WHAT THIS MODULE OWNS
---------------------
The page SHELL: the header with its reserved launch-headline slot and its
permanent caveat slot, the mode strip, the filter bar (including the "more
matches" control ruling T made load-bearing), the result bar, a minimal result
row, the pager, and the four service states. The full row anatomy -- relation
chip, novelty badge, coverage clause, side actions -- belongs to the row track
(plans 136-17 / 136-18) and is deliberately NOT built here.

OFF-LOOP DISCIPLINE (T-136-16-05)
---------------------------------
Every read on this page is a direct ``await`` on an async wrapper imported from
``web.discovery``. Those wrappers ALREADY dispatch to an executor internally
(``run_in_executor`` + ``asyncio.wait``, never ``wait_for``), so this module adds
NO second offload wrapper around them, makes no synchronous service call, and
never reaches for the composition module's private singleton. Wrapping an
already-async, already-offloading wrapper is a NESTED offload that burns two
threadpool slots per request on a server that runs ONE uvicorn worker.
``tests/test_no_await_sync_function.py`` cannot see any of that -- it detects
only an ``await`` on a LOCALLY defined synchronous ``def`` -- which is why
``tests/test_findings_page.py`` carries its own AST guard and an executor
dispatch spy.

WHERE EVERY STRING COMES FROM
-----------------------------
``tr()`` owns page chrome; ``shared.discovery_display_strings`` owns the claim
vocabulary (plan 136-10 fixed that split and a test enforces it). A small number
of strings this page needs had no home in either -- the caveat, the reserved
headline region's label, the approximate-count note, the second-bucket result-bar
line and the disabled-filter tag. They live in ``_FINDINGS_COPY`` below, in both
languages, and every one of them is swept through the SHARED honesty gate
(``tests/render_smoke/discovery_honesty_gate.py``) by this plan's suite. See the
plan summary for why they are not in ``genizah_translations.py``.

NO GRADE FILTER
---------------
D-16 was ratified on 2026-08-02: this page ships WITHOUT a relation or
quality-grade filter control. Within the main pool one relation kind dominates
heavily, so such a filter would restate the bucket rather than narrow it.
Quality is the bucket; kind is the panel's own filter. (The measured split is
recorded in `136-GATE1-DECISIONS.md` D-16 and is deliberately not quoted here:
no figure of any kind belongs in a discovery surface module, not even in a
docstring, because a figure in a docstring is one careless copy away from a
surface.)
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any, Dict, List, Optional, Tuple

from nicegui import ui

from shared.discovery_display_strings import (
    bucket_name,
    coverage_label,
    display_work_title,
    missing_title,
    novelty_strings,
    recall_disclaimer,
    retry_label,
    rule_sentence,
    service_state_message,
)
from shared.discovery_novelty import CANDIDATE_STATUS
from web.discovery import (
    BUCKET_MAIN,
    FACET_LEVELS,
    FINDINGS_BUCKETS,
    FINDINGS_SORT_BAND_RANK,
    FINDINGS_SORTS,
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNITS,
    get_findings_enveloped,
    get_findings_facets_enveloped,
)
from web.safe_storage import safe_user_get, safe_user_set
from web.translations import get_language, tr

logger = logging.getLogger(__name__)

#: The second bucket's stored value.
#:
#: `web.discovery` re-exports `BUCKET_MAIN` and the closed `FINDINGS_BUCKETS`
#: set, but not this member; and this page's off-loop guard forbids naming the
#: service module here at all (a page that can reach the service module can
#: reach its private singleton). So the value is written ONCE and immediately
#: CHECKED against the exported vocabulary at module load: a rename in the
#: service breaks this import loudly rather than silently sending an
#: out-of-vocabulary bucket that would raise at request time. A test pins it
#: byte-for-byte against the service's own constant.
BUCKET_MORE = "more"
if BUCKET_MORE not in FINDINGS_BUCKETS or BUCKET_MORE == BUCKET_MAIN:
    raise RuntimeError(
        "web/pages/findings.py: BUCKET_MORE is no longer a member of the exported "
        "FINDINGS_BUCKETS vocabulary (or has collided with BUCKET_MAIN) -- the "
        "second bucket's stored value moved and this module was not updated"
    )


# ---------------------------------------------------------------------------
# Route + stable selectors. Every marker class below is part of this module's
# contract with its test suite and with plan 136-18 (which fills the reserved
# headline slot); renaming one is a breaking change, not a refactor.
# ---------------------------------------------------------------------------

FINDINGS_ROUTE = "/computed-identifications"

#: The discovery CSS block (plan 136-10) is scoped under `.gs-discovery`; the
#: page root must carry it or none of the block applies.
ROOT_CLASS = "gs-discovery"
PAGE_CLASS = "gs-findings"

HEAD_CLASS = "gs-findings-head"
#: Reserved by THIS plan, filled by plan 136-18 from plan 136-22's
#: artifact-backed reader. This module writes no number into it -- see
#: `_render_headline_slot`.
HEADLINE_SLOT_CLASS = "gs-findings-headline"
CAVEAT_CLASS = "gs-findings-caveat"
MODES_CLASS = "gs-findings-modes"
FILTER_BAR_CLASS = "gs-findings-fbar"
BUCKET_CONTROL_CLASS = "gs-findings-bucket"
RESULT_BAR_CLASS = "gs-findings-rbar"
RESULTS_CLASS = "gs-findings-results"
ROW_CLASS = "gs-findings-row"
PAGER_CLASS = "gs-findings-pager"
STATE_CLASS = "gs-findings-state"


# ---------------------------------------------------------------------------
# Per-user state. Everything goes through the storage chokepoint
# (`web/safe_storage.py`) -- never the raw per-user store (T-136-16-07).
# ---------------------------------------------------------------------------

_STORAGE_PREFIX = "discovery_findings_"

_KEY_UNIT = _STORAGE_PREFIX + "unit"
_KEY_BUCKET = _STORAGE_PREFIX + "bucket"
_KEY_SORT = _STORAGE_PREFIX + "sort"
_KEY_NOVELTY = _STORAGE_PREFIX + "novelty_only"
_KEY_DOMAIN = _STORAGE_PREFIX + "domain"
_KEY_AUTHOR = _STORAGE_PREFIX + "author"
_KEY_WORK = _STORAGE_PREFIX + "work_id"
_KEY_PAGE = _STORAGE_PREFIX + "page"


# ---------------------------------------------------------------------------
# The five strings that had no home in `tr()` or in the shared claim
# vocabulary. Bilingual, digit-free where the criteria require it, and swept
# through the shared honesty gate by this plan's suite.
# ---------------------------------------------------------------------------

_FINDINGS_COPY: Dict[str, Dict[str, str]] = {
    # The permanent caveat. Hand-written prose is exactly where these rules get
    # broken -- the findings sketch's own first draft failed the suite by using
    # a prohibited relation phrase inside a NEGATION, which a grep-based guard
    # cannot see. This wording states what a match IS and what it is not,
    # without reaching for any of the three prohibited words.
    "caveat": {
        "en": (
            "Every row here is a text match found by software, not a reviewed "
            "identification. A text match is not by itself proof of identity — "
            "read each row as a lead to check, never as a conclusion."
        ),
        "he": (
            "כל שורה כאן היא התאמת טקסט שנמצאה על ידי תוכנה, ולא זיהוי שנבדק. "
            "התאמת טקסט אינה כשלעצמה הוכחה לזהות — יש לקרוא כל שורה ככיוון "
            "לבדיקה, ולעולם לא כמסקנה."
        ),
    },
    # The reserved launch-headline region's accessible label. Ruling U's framing
    # ("what the release adds to the existing finding aids") with NO number:
    # the figures are artifact-backed and version-dependent, they are supplied
    # by plan 136-22 and rendered by plan 136-18, and a placeholder digit here
    # would survive as a hardcoded launch number -- precisely the failure
    # ruling U was issued to prevent.
    "headline_slot_label": {
        "en": "What this release adds to the finding aids",
        "he": "מה מוסיפה מהדורה זו לכלי העזר",
    },
    # Ruling U constraint 1: a silently approximate number presented as exact is
    # worse than no number.
    "approximate_note": {
        "en": "This total is approximate.",
        "he": "המספר הזה מקורב.",
    },
    # The second-bucket counterpart of tr('Showing the {bucket} by default.').
    # The bar must name its bucket in BOTH bucket states.
    "showing_bucket": {
        "en": "Showing {bucket}.",
        "he": "מוצגות {bucket}.",
    },
    # The amber tag on a filter whose backing data is missing. A filter that
    # silently vanishes is indistinguishable from a filter that never existed.
    "needs_tag": {
        "en": "not available yet",
        "he": "עדיין לא זמין",
    },
}


def _lang_key(lang: str) -> str:
    return "he" if lang == "he" else "en"


def copy_text(key: str, lang: str = "en") -> str:
    """One of the five page-local bilingual strings. Raises on an unknown key
    rather than rendering an empty element for a string nobody designed."""
    entry = _FINDINGS_COPY.get(key)
    if entry is None:
        raise ValueError(
            "copy_text: unknown key {!r} (expected one of {})".format(
                key, sorted(_FINDINGS_COPY)
            )
        )
    return entry[_lang_key(lang)]


def copy_keys() -> Tuple[str, ...]:
    """Every page-local copy key, for the suite's honesty sweep."""
    return tuple(sorted(_FINDINGS_COPY))


# ---------------------------------------------------------------------------
# Closed-vocabulary maps. An out-of-vocabulary `unit` / `sort` / `bucket`
# RAISES `ValueError` in the service rather than becoming an envelope, so the
# request is validated against the exported sets BEFORE any call.
# ---------------------------------------------------------------------------

_UNIT_LABEL_KEYS: Dict[str, str] = {
    "identification": "One row per identification",
    "manuscript": "One row per manuscript",
    "work": "One row per work",
}

_SORT_LABEL_KEYS: Dict[str, str] = {
    "band_rank": "Strongest first",
    "page_count": "Pages matched",
    "matched_text": "Matched text",
}

#: The two buckets this page OFFERS, in display order. A subset of the exported
#: closed vocabulary: the all-bucket sentinel is deliberately not a reader
#: choice, because ruling U constraint 1 requires ONE stated basis and a control
#: that silently unions the two pools would produce a figure the page could not
#: name.
_OFFERED_BUCKETS: Tuple[str, ...] = (BUCKET_MAIN, BUCKET_MORE)

#: The three outage statuses, each rendered distinctly and each with a retry.
#: `ok` is not here: an `ok` envelope with zero rows is an honest empty state,
#: which must never be confused with any of these.
_OUTAGE_STATUSES: Tuple[str, ...] = ("unavailable", "timeout", "busy")

#: The mode strip. "All findings" is live; the other two ship visible, inert and
#: phase-tagged, so plans 137/138 add a tab rather than a page.
_MODES: Tuple[Tuple[str, Optional[str]], ...] = (
    ("All findings", None),
    ("Screening leads", "Phase 138"),
    ("My saved", "Phase 137"),
)


def _default_page_size() -> int:
    """The BUDGETED default page size (`docs/specs/discovery-budgets.md` §5).

    Read here for pager arithmetic only. The CEILING is deliberately not
    restated in this module: the page passes this value to the service, which
    clamps it server-side against the shared `DISCOVERY_PAGE_SIZE_MAX`, so a
    control can never widen the page beyond the budget.
    """
    try:
        value = int(os.environ.get("DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT", "50"))
    except (TypeError, ValueError):
        return 50
    return value if value > 0 else 50


def read_state() -> Dict[str, Any]:
    """The reader's persisted selections, read through the storage chokepoint
    and VALIDATED against the exported closed vocabularies.

    An unrecognised stored value (a hand-edited cookie, a vocabulary that moved
    between releases) resolves to the default rather than reaching the service,
    where it would raise.
    """
    unit = safe_user_get(_KEY_UNIT, FINDINGS_UNIT_IDENTIFICATION)
    if unit not in FINDINGS_UNITS:
        unit = FINDINGS_UNIT_IDENTIFICATION

    bucket = safe_user_get(_KEY_BUCKET, BUCKET_MAIN)
    # Validated against the EXPORTED closed set first (an out-of-vocabulary
    # value raises in the service rather than becoming an envelope), then
    # narrowed to the two buckets this page actually offers.
    if bucket not in FINDINGS_BUCKETS or bucket not in _OFFERED_BUCKETS:
        bucket = BUCKET_MAIN

    sort = safe_user_get(_KEY_SORT, FINDINGS_SORT_BAND_RANK)
    if sort not in FINDINGS_SORTS:
        sort = FINDINGS_SORT_BAND_RANK

    try:
        page = int(safe_user_get(_KEY_PAGE, 1) or 1)
    except (TypeError, ValueError):
        page = 1

    def _opt(key: str) -> Optional[str]:
        value = safe_user_get(key, None)
        return value if isinstance(value, str) and value else None

    return {
        "unit": unit,
        "bucket": bucket,
        "sort": sort,
        "novelty_only": bool(safe_user_get(_KEY_NOVELTY, False)),
        "domain": _opt(_KEY_DOMAIN),
        "author": _opt(_KEY_AUTHOR),
        "work_id": _opt(_KEY_WORK),
        "page": page if page >= 1 else 1,
    }


def write_state(state: Dict[str, Any]) -> None:
    """Persist the reader's selections through the storage chokepoint."""
    safe_user_set(_KEY_UNIT, state["unit"])
    safe_user_set(_KEY_BUCKET, state["bucket"])
    safe_user_set(_KEY_SORT, state["sort"])
    safe_user_set(_KEY_NOVELTY, bool(state["novelty_only"]))
    safe_user_set(_KEY_DOMAIN, state["domain"])
    safe_user_set(_KEY_AUTHOR, state["author"])
    safe_user_set(_KEY_WORK, state["work_id"])
    safe_user_set(_KEY_PAGE, state["page"])


def _novelty_selection(state: Dict[str, Any]) -> Optional[Tuple[str, ...]]:
    """The candidacy filter, as the service's novelty argument.

    `None` (the empty selection) means ALL -- the phase-wide convention that
    filters compose as AND and an empty set is not a filter.
    """
    return (CANDIDATE_STATUS,) if state.get("novelty_only") else None


async def fetch_findings(state: Dict[str, Any]) -> Dict[str, Any]:
    """One enveloped findings read. A DIRECT await on the async wrapper."""
    return await get_findings_enveloped(
        state["unit"],
        bucket=state["bucket"],
        novelty=_novelty_selection(state),
        domain=state.get("domain"),
        author=state.get("author"),
        work_id=state.get("work_id"),
        sort=state["sort"],
        page=state["page"],
        page_size=_default_page_size(),
    )


async def fetch_facets(level: str, state: Dict[str, Any]) -> Dict[str, Any]:
    """One enveloped facet-cascade read. A DIRECT await on the async wrapper."""
    if level not in FACET_LEVELS:
        raise ValueError(
            "fetch_facets: unknown facet level {!r} (expected one of {})".format(
                level, sorted(FACET_LEVELS)
            )
        )
    return await get_findings_facets_enveloped(
        level,
        bucket=state["bucket"],
        novelty=_novelty_selection(state),
        domain=state.get("domain") if level != "domain" else None,
        author=state.get("author") if level == "work" else None,
    )


# ---------------------------------------------------------------------------
# Client-liveness guard. `page_client` is bound at RENDER time (never lazily
# inside a handler): a late binding is a latent failure rather than an error,
# because a background context has no UI context to read it from.
# ---------------------------------------------------------------------------

def _page_is_gone(page_client: Any) -> bool:
    if page_client is None:
        return False
    try:
        return bool(getattr(page_client, "_deleted", False))
    except (RuntimeError, AttributeError):  # pragma: no cover -- defensive
        return True


# ---------------------------------------------------------------------------
# The page.
# ---------------------------------------------------------------------------

async def create_findings_page() -> None:
    """Render the corpus-wide findings page.

    The route has already proved availability and rendered the layout; this
    builder is never even imported while discovery is unavailable.
    """
    lang = get_language()
    # Bound at RENDER time, inside the UI context, before any await. A late
    # binding is a latent failure rather than an error: a background context has
    # no UI context to read it from.
    try:
        page_client = ui.context.client
    except Exception:  # pragma: no cover -- no client in a bare probe context
        page_client = None

    state = read_state()

    with ui.column().classes(f"{ROOT_CLASS} {PAGE_CLASS} w-full max-w-5xl mx-auto p-4 gap-4"):
        _render_head(lang)
        _render_mode_strip(lang)
        body = ui.column().classes("w-full gap-3")

    if _page_is_gone(page_client):
        return
    with body:
        await _render_body(state, lang, page_client)


def _render_head(lang: str) -> None:
    """Title, sub-line, the RESERVED launch-headline region, and the permanent
    caveat slot -- in that order, with the caveat between header and body."""
    with ui.column().classes(f"phead {HEAD_CLASS} w-full gap-2"):
        ui.label(tr("Computed Identifications")).classes("text-2xl font-bold")
        ui.label(recall_disclaimer(lang)).classes("sub text-sm").style(
            "color: var(--text-secondary);"
        )
        _render_headline_slot(lang)
        _render_caveat(lang)


def _render_headline_slot(lang: str) -> None:
    """The launch-headline region: RESERVED, never populated here.

    Plan 136-22 (wave 8) supplies the artifact-backed, version-aware reader and
    plan 136-18 (wave 9) fills this slot. This plan is wave 7 and cannot consume
    either, so its whole share is a named, structurally-present container with
    bilingual label scaffolding and NO DIGIT of any kind. A placeholder digit
    here would survive as a hardcoded launch number, which is precisely the
    failure ruling U was issued to prevent.
    """
    region = ui.column().classes(f"{HEADLINE_SLOT_CLASS} w-full gap-1")
    region.props(f'role=region aria-label="{copy_text("headline_slot_label", lang)}"')
    with region:
        # An empty, stable child for 136-18 to fill. No text, hence no digit.
        ui.element("div").classes(f"{HEADLINE_SLOT_CLASS}-value")


def _render_caveat(lang: str) -> None:
    """The permanent caveat slot -- a designed element with the gold
    inline-start rule, never fine print and never a dismissible warning."""
    with ui.element("div").classes(f"caveat {CAVEAT_CLASS} w-full p-3 text-sm"):
        ui.label(copy_text("caveat", lang))


def _render_mode_strip(lang: str) -> None:
    """Three modes: one live, two visible-inert-and-phase-tagged, so plans
    137/138 add a tab rather than a page."""
    with ui.row().classes(f"modes {MODES_CLASS} w-full gap-2 items-center flex-wrap"):
        for label_key, phase_key in _MODES:
            future = phase_key is not None
            button = ui.button(tr(label_key)).props("flat dense no-caps")
            button.classes(("mode future" if future else "mode") + f" {MODES_CLASS}-item")
            if future:
                button.disable()
                ui.label(tr(phase_key)).classes(f"needs {MODES_CLASS}-phase")


async def _render_body(state: Dict[str, Any], lang: str, page_client: Any) -> None:
    """Filter bar and results, with ONE refresh path shared by every control --
    so a filter change and a bucket change take exactly the same route."""
    filter_bar = ui.row().classes(
        f"fbar {FILTER_BAR_CLASS} w-full gap-4 items-start flex-wrap"
    )
    results_region = ui.column().classes(f"{RESULTS_CLASS} w-full gap-2")

    async def refresh() -> None:
        write_state(state)
        if _page_is_gone(page_client):
            return
        envelope = await fetch_findings(state)
        if _page_is_gone(page_client):
            return
        results_region.clear()
        with results_region:
            _render_results(envelope, state, lang, refresh)

    with filter_bar:
        _render_filter_bar(state, lang, refresh)

    await refresh()
    await _populate_facets(filter_bar, state, lang, refresh)


# ---------------------------------------------------------------------------
# Filter bar. The novelty switch is FIRST by CSS order (`.fg.novgrp {order:-1}`)
# regardless of DOM order, which is what keeps it first in BOTH directions.
# ---------------------------------------------------------------------------

def _render_filter_bar(state: Dict[str, Any], lang: str, refresh) -> None:
    _render_novelty_switch(state, lang, refresh)
    _render_bucket_control(state, lang, refresh)
    _render_coverage_filter(lang)
    _render_facet_groups(lang)


def _render_novelty_switch(state: Dict[str, Any], lang: str, refresh) -> None:
    """The candidacy switch, first in the filter bar by CSS order."""
    words = novelty_strings(lang)
    with ui.column().classes(f"fg novgrp {FILTER_BAR_CLASS}-novelty gap-1"):
        async def _toggle(_event=None) -> None:
            state["novelty_only"] = not state["novelty_only"]
            state["page"] = 1
            switch.props(f'aria-pressed={"true" if state["novelty_only"] else "false"}')
            await refresh()

        switch = ui.button(words["toggle"], on_click=_toggle).props("flat dense no-caps")
        switch.classes("fchip")
        switch.props(f'aria-pressed={"true" if state["novelty_only"] else "false"}')
        switch.tooltip(words["help"])
        ui.label(words["subline"]).classes("dnote text-xs")


def _render_bucket_control(state: Dict[str, Any], lang: str, refresh) -> None:
    """THE "more matches" control (ruling T).

    A first-class, always-rendered control in the filter bar -- never inside an
    overflow menu, a `<details>`, an "advanced" disclosure or a footer link, and
    never below the results. ONE interaction switches the result set between the
    two buckets.

    It carries NO count. The owner's assessment of that bucket is an impression
    over a rendered sample with no draw protocol and no blind grading; it must
    never become a percentage, a quality score or a number here or anywhere
    else. The bucket names come from the shared vocabulary, in match framing:
    the second bucket means there was not enough evidence for the main-pool
    rule, never that those identifications are probably wrong.
    """
    with ui.column().classes(f"fg {BUCKET_CONTROL_CLASS}-group gap-1"):
        with ui.row().classes(f"{BUCKET_CONTROL_CLASS} gap-2 items-center"):
            for in_main in (True, False):
                target = BUCKET_MAIN if in_main else BUCKET_MORE
                label = bucket_name(in_main, lang)
                selected = state["bucket"] == target

                async def _select(_event=None, target=target) -> None:
                    state["bucket"] = target
                    state["page"] = 1
                    await refresh()

                chip = ui.button(label, on_click=_select).props("flat dense no-caps")
                chip.classes("fchip here" if selected else "fchip")
                chip.props(f'aria-pressed={"true" if selected else "false"}')
        # The rule, in the one place it is worded. Deliberately a SIBLING of the
        # control row, never a child: the control's own subtree must stay
        # digit-free and count-free.
        ui.label(rule_sentence(lang)).classes("dnote text-xs")


def _render_coverage_filter(lang: str) -> None:
    """Rendered, visibly disabled, and tagged -- never silently absent.

    The service exposes no coverage predicate, so this filter has no backing
    data to act on. A filter that silently vanishes is indistinguishable from a
    filter that never existed, so the treatment stays even though the state
    should not occur once the axis is wired.
    """
    with ui.column().classes(f"fg blocked {FILTER_BAR_CLASS}-coverage gap-1"):
        with ui.row().classes("gap-2 items-center"):
            ui.label(coverage_label(lang)).classes("text-xs font-bold")
            ui.label(copy_text("needs_tag", lang)).classes("needs")
        chip = ui.button(coverage_label(lang)).props("flat dense no-caps")
        chip.classes("fchip")
        chip.disable()


def _render_facet_groups(lang: str) -> None:
    """The domain / author / work cascade's containers.

    Populated after the first paint by `_populate_facets`, so the filter bar's
    structure exists before any facet read returns.
    """
    for level, label_key in (("domain", "Domain"), ("author", "Author"), ("work", "Work")):
        with ui.column().classes(f"fg {FILTER_BAR_CLASS}-{level} gap-1"):
            ui.label(tr(label_key)).classes("text-xs font-bold")
            ui.column().classes(f"{FILTER_BAR_CLASS}-{level}-items gap-1")


def _facet_containers(filter_bar: Any) -> Dict[str, Any]:
    containers: Dict[str, Any] = {}
    for element in filter_bar.descendants(include_self=True):
        classes = getattr(element, "_classes", None) or []
        for level in ("domain", "author", "work"):
            if f"{FILTER_BAR_CLASS}-{level}-items" in classes:
                containers[level] = element
    return containers


async def _populate_facets(
    filter_bar: Any, state: Dict[str, Any], lang: str, refresh
) -> None:
    """Fill the three facet lists from the cascade.

    Every work-level label routes through `display_work_title` (ruling R): the
    cascade selects the RAW recorded title at the work level, and a facet list
    that prints it directly opts out of the curation in the very control a
    reader uses to find that work.
    """
    containers = _facet_containers(filter_bar)
    for level in ("domain", "author", "work"):
        container = containers.get(level)
        if container is None:  # pragma: no cover -- structural
            continue
        envelope = await fetch_facets(level, state)
        container.clear()
        with container:
            _render_facet_items(level, envelope, state, lang, refresh)


def _render_facet_items(
    level: str, envelope: Dict[str, Any], state: Dict[str, Any], lang: str, refresh
) -> None:
    if (envelope or {}).get("status") != "ok":
        # Backing data absent: visibly disabled and tagged, never absent.
        with ui.column().classes(f"fg blocked {FILTER_BAR_CLASS}-{level}-blocked gap-1"):
            ui.label(copy_text("needs_tag", lang)).classes("needs")
        return

    state_key = "work_id" if level == "work" else level
    for item in envelope.get("items") or []:
        value = item.get("value")
        raw_label = item.get("label") or value or ""
        if level == "work":
            # Ruling R -- the curated display title, never the raw recorded one.
            label = display_work_title(value, raw_label, lang) or missing_title(lang)
        else:
            label = raw_label
        selected = state.get(state_key) == value

        async def _pick(_event=None, value=value, state_key=state_key) -> None:
            state[state_key] = None if state.get(state_key) == value else value
            state["page"] = 1
            await refresh()

        node = ui.button(label, on_click=_pick).props("flat dense no-caps align=left")
        node.classes(
            " ".join(
                part for part in (
                    "dnode",
                    "leaf" if item.get("is_leaf") else "",
                    "here" if selected else "",
                ) if part
            )
        )


# ---------------------------------------------------------------------------
# Result bar, rows, pager, and the four service states.
# ---------------------------------------------------------------------------

def _render_results(
    envelope: Dict[str, Any], state: Dict[str, Any], lang: str, refresh
) -> None:
    status = (envelope or {}).get("status")
    if status != "ok":
        _render_outage_state(status, lang, refresh)
        return

    items: List[Dict[str, Any]] = list(envelope.get("items") or [])
    total = int(envelope.get("total") or 0)
    meta = dict(envelope.get("meta") or {})

    _render_result_bar(items, total, meta, state, lang, refresh)

    with ui.column().classes(f"rows {RESULTS_CLASS}-rows w-full gap-2"):
        if not items:
            # An HONEST empty state: `ok` with zero rows. Visually and
            # structurally distinct from the three outage states below, which is
            # the whole point -- an outage that reads as "no findings" silently
            # under-reports the corpus.
            ui.label(tr("No results found")).classes(f"{RESULTS_CLASS}-empty")
        for item in items:
            _render_row(item, lang)

    _render_pager(total, state, lang, refresh)


def _render_outage_state(status: Optional[str], lang: str, refresh) -> None:
    """`unavailable` / `timeout` / `busy` -- each a VISIBLE temporary condition
    with a retry affordance, never an empty result (T-136-16-04).

    `busy` is genuinely reachable here rather than theoretical: the corpus-wide
    query is heavy and takes a bounded-concurrency slot, so a burst degrades to
    an explicit busy rather than queueing behind itself.
    """
    key = status if status in _OUTAGE_STATUSES else _OUTAGE_STATUSES[0]
    with ui.column().classes(
        f"{STATE_CLASS} {STATE_CLASS}-{key} w-full gap-2 p-3"
    ):
        ui.label(service_state_message(key, lang)).classes(f"{STATE_CLASS}-message")

        async def _retry(_event=None) -> None:
            await refresh()

        ui.button(retry_label(lang), on_click=_retry).props(
            "flat dense no-caps"
        ).classes(f"{STATE_CLASS}-retry")


def _render_result_bar(
    items: List[Dict[str, Any]],
    total: int,
    meta: Dict[str, Any],
    state: Dict[str, Any],
    lang: str,
    refresh,
) -> None:
    """The count, WHICH BUCKET it covers, the "Show as" row unit, and the sort.

    The count is the envelope's real pre-`LIMIT` total, never `len(items)`; the
    bar names its bucket in words in BOTH bucket states (ruling U constraint 1 --
    one basis, stated, never a main-pool figure and an all-bucket figure summed
    into one number); and an approximate total says so, because a silently
    approximate number presented as exact is worse than no number.
    """
    with ui.column().classes(f"rbar {RESULT_BAR_CLASS} w-full gap-2"):
        with ui.row().classes("w-full gap-3 items-center flex-wrap"):
            ui.label(
                tr("Showing {shown} of {total} findings").format(
                    shown=len(items), total=total
                )
            ).classes(f"{RESULT_BAR_CLASS}-count")

            if state["bucket"] == BUCKET_MAIN:
                bucket_line = tr("Showing the {bucket} by default.").format(
                    bucket=bucket_name(True, lang)
                )
            else:
                bucket_line = copy_text("showing_bucket", lang).format(
                    bucket=bucket_name(False, lang)
                )
            ui.label(bucket_line).classes(f"{RESULT_BAR_CLASS}-bucket dnote text-xs")

            if meta.get("approximate_total"):
                ui.label(copy_text("approximate_note", lang)).classes(
                    f"{RESULT_BAR_CLASS}-approx dnote text-xs"
                )

        with ui.row().classes("w-full gap-3 items-center flex-wrap"):
            _render_unit_select(state, refresh)
            _render_sort_select(state, refresh)


def _render_unit_select(state: Dict[str, Any], refresh) -> None:
    """The row unit is a READER choice, not a design pick. The option set is the
    exported closed vocabulary itself, so a unit the service gains cannot be
    silently withheld and a unit it loses cannot be silently offered."""
    options = {unit: tr(_UNIT_LABEL_KEYS[unit]) for unit in sorted(FINDINGS_UNITS)}

    async def _change(event) -> None:
        value = getattr(event, "value", None)
        if value in FINDINGS_UNITS:
            state["unit"] = value
            state["page"] = 1
            await refresh()

    ui.select(
        options, value=state["unit"], label=tr("Show as"), on_change=_change
    ).props("dense outlined").classes(f"{RESULT_BAR_CLASS}-unit")


def _render_sort_select(state: Dict[str, Any], refresh) -> None:
    """Sort offers exactly the exported orderings.

    Novelty is deliberately NOT among them: absence from a finding aid is not
    evidence a match is correct, and offering it as an ordering would imply
    otherwise (D-15a / D-24).
    """
    options = {sort: tr(_SORT_LABEL_KEYS[sort]) for sort in sorted(FINDINGS_SORTS)}

    async def _change(event) -> None:
        value = getattr(event, "value", None)
        if value in FINDINGS_SORTS:
            state["sort"] = value
            state["page"] = 1
            await refresh()

    ui.select(
        options, value=state["sort"], label=tr("Sort by"), on_change=_change
    ).props("dense outlined").classes(f"{RESULT_BAR_CLASS}-sort")


def _render_row(item: Dict[str, Any], lang: str) -> None:
    """A MINIMAL identity row -- enough for the shell to be verifiable.

    The full row anatomy (relation chip with the band label on hover, novelty
    badge, matched-letter coverage, side actions) belongs to the row track.
    """
    work_id = item.get("display_work_id") or item.get("canonical_work_id") or ""
    raw_title = item.get("neutral_title") or ""
    # Ruling R -- every work title a reader sees routes through this.
    title = display_work_title(work_id, raw_title, lang) if raw_title else missing_title(lang)

    with ui.column().classes(f"row {ROW_CLASS} w-full gap-1 p-2"):
        ui.label(title).classes(f"{ROW_CLASS}-title font-bold")
        shelf = " ".join(
            part for part in (item.get("library_code"), item.get("shelfmark_display")) if part
        )
        if shelf:
            ui.label(shelf).classes(f"{ROW_CLASS}-shelfmark r-sub text-xs")


def _render_pager(total: int, state: Dict[str, Any], lang: str, refresh) -> None:
    """Pagination over the FULL filtered set.

    The service supplies a real pre-`LIMIT` total, so the page count is derived
    from that and never from the length of the current page. The page-size
    CEILING is enforced server-side (the service clamps whatever it is handed
    against the shared maximum); this module names only the budgeted default.
    """
    size = _default_page_size()
    pages = max(1, math.ceil(total / size)) if total > 0 else 1
    page = min(max(1, state["page"]), pages)

    with ui.row().classes(f"pager {PAGER_CLASS} w-full gap-2 items-center"):
        async def _go(delta: int) -> None:
            state["page"] = max(1, min(pages, state["page"] + delta))
            await refresh()

        previous = ui.button(tr("Previous"), on_click=lambda _e=None: _go(-1))
        previous.props("flat dense no-caps").classes(f"{PAGER_CLASS}-prev")
        if page <= 1:
            previous.disable()

        ui.label(f"{tr('Page')} {page} / {pages}").classes(
            f"{PAGER_CLASS}-position text-xs"
        )

        following = ui.button(tr("Next"), on_click=lambda _e=None: _go(1))
        following.props("flat dense no-caps").classes(f"{PAGER_CLASS}-next")
        if page >= pages:
            following.disable()
