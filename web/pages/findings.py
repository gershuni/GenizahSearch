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
quality-grade filter control. Within the main pool the relation split is 94% one
kind, so such a filter would restate the bucket rather than narrow it. Quality is
the bucket; kind is the panel's own filter.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Dict, Optional, Tuple

from nicegui import ui

from shared.discovery_display_strings import (
    recall_disclaimer,
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
# (`web/safe_storage.py`) -- never `app.storage.user` directly (T-136-16-07).
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
    if bucket not in FINDINGS_BUCKETS:
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

    Task 1 ships the SHELL ROOT only -- the header, mode strip, filter bar,
    result bar and pager are added by the remaining tasks of plan 136-16.
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
    write_state(state)

    with ui.column().classes(f"{ROOT_CLASS} {PAGE_CLASS} w-full max-w-5xl mx-auto p-4 gap-4"):
        with ui.column().classes(f"phead {HEAD_CLASS} w-full gap-2"):
            ui.label(tr("Computed Identifications")).classes("text-2xl font-bold")
            ui.label(recall_disclaimer(lang)).classes("sub text-sm").style(
                "color: var(--text-secondary);"
            )
        body = ui.column().classes("w-full gap-3")

    if _page_is_gone(page_client):
        return
    with body:
        ui.element("div").classes(f"{RESULTS_CLASS}")
