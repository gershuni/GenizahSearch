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

LAYOUT (2026-08-04, owner verdict)
----------------------------------
The page was one long column: a wall of prose, then a flat unstyled stack of
domain links, then the rest. The owner read it as a draft and pointed at
``/catalog-browse`` as the shape it should have. It now uses that page's own
structure -- centred title, ONE visible line, then a LEFT COLUMN OF WHITE CARDS
(each with the same uppercase small-caps header) beside the results region --
and the explanatory prose that used to sit above the first control lives in a
collapsed "how to read this page" panel. Not one word of that prose was deleted
or reworded: every line of it is honesty-critical text under D-06a and the
match-framing rule, so MOVING it was the task and REWRITING it was not.

No stylesheet rule was added for any of it (``web/static/common.css`` is
untouched, and two tests assert so): the cards are ``ui.card``, the columns are
flex-basis on a wrapping row, and every existing ``.gs-discovery`` class the
page already relied on is still on the element it was written for.

OFF-LOOP DISCIPLINE (T-136-16-05)
---------------------------------
Every read on this page is a direct ``await`` on an async wrapper that offloads
EXACTLY ONCE internally. Two modules provide those wrappers:

* ``web.discovery`` -- every discovery-sidecar read (``run_in_executor`` +
  ``asyncio.wait``, never ``wait_for``);
* ``web.discovery_genre_labels`` -- the one FJMS read this page needs, the
  bilingual domain vocabulary, primed at most once per process through
  ``web.bounded_io.bounded_io_bound``.

This module therefore adds NO second offload wrapper around either, makes no
synchronous service call, and never reaches for the composition module's
private singleton. Wrapping an already-async, already-offloading wrapper is a
NESTED offload that burns two threadpool slots per request on a server that runs
ONE uvicorn worker. ``tests/test_no_await_sync_function.py`` cannot see any of
that -- it detects only an ``await`` on a LOCALLY defined synchronous ``def`` --
which is why ``tests/test_findings_page.py`` carries its own AST guard (whose
allowlist is those two modules, named there and nowhere else) and an executor
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

import asyncio
import logging
import math
import os
from typing import Any, Dict, List, Optional, Tuple

from nicegui import ui

from shared.discovery_display_strings import (
    TOGGLE_DIVERGENCE,
    TOGGLE_MORE_MATCHES,
    bucket_name,
    coverage_label,
    disclosure_toggle,
    display_work_title,
    divergence_warning,
    missing_title,
    novelty_strings,
    recall_disclaimer,
    retry_label,
    rule_sentence,
    service_state_message,
)
from shared.discovery_novelty import CANDIDATE_STATUS
from web.components import findings_rows as rows
from web.components.typography import h1
from web.discovery import (
    BUCKET_MAIN,
    FACET_LEVELS,
    FINDINGS_BUCKETS,
    FINDINGS_SORT_BAND_RANK,
    FINDINGS_SORTS,
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNITS,
    findings_novelty_offered,
    get_findings_enveloped,
    get_findings_facets_enveloped,
    get_launch_stats_enveloped,
)
from web.discovery_assets import (
    discovery_db_path,
    discovery_meta,
    discovery_sidecar_version,
)
from web.discovery_genre_labels import (
    GENRE_PART_SEPARATOR,
    genre_display_label,
    prime_domain_translations,
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
#: The collapsed panel the demoted prose lives in. NOTHING is deleted or
#: reworded on its way in there -- see `_render_howto`.
HOWTO_CLASS = "gs-findings-howto"
MODES_CLASS = "gs-findings-modes"
#: The two-column body: the filter bar is the LEFT COLUMN OF CARDS and the
#: results sit beside it, the shape `/catalog-browse` already ships.
BODY_CLASS = "gs-findings-body"
MAIN_CLASS = "gs-findings-main"
#: One filter card. `CARD_HEADER_CLASS` carries the same small-caps header
#: treatment `web/pages/catalog_browse.py` uses on DOMAIN / FILTER BY
#: AVAILABILITY / FILTER BY LIBRARY, so the two pages read as one product.
CARD_CLASS = "gs-findings-card"
CARD_HEADER_CLASS = "gs-findings-card-header"
FILTER_BAR_CLASS = "gs-findings-fbar"
BUCKET_CONTROL_CLASS = "gs-findings-bucket"
FACET_HEADER_CLASS = "gs-findings-facet-header"
RESULT_BAR_CLASS = "gs-findings-rbar"
#: The active-filter chip bar: one removable chip per selection plus a clear
#: all, between the result bar and the rows it describes. NEVER a chip for the
#: pool -- see `_active_filter_chips`.
ACTIVE_FILTERS_CLASS = "gs-findings-active"
#: The second-pool invitation strip -- a SECOND live entry point beside the
#: results, never a replacement for the ruling-T control in the filter bar.
POOL_INVITE_CLASS = "gs-findings-pool-invite"
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
#: Ruling F's opt-in. Persisted like every other axis, and DEFAULT FALSE at
#: every read: a stored `True` from a previous visit is the reader's own
#: deliberate choice and is honoured, but nothing else -- an absent key, a
#: hand-edited cookie, a non-boolean -- can turn the axis on.
_KEY_DIVERGENCE = _STORAGE_PREFIX + "divergence"
_KEY_DOMAIN = _STORAGE_PREFIX + "domain"
_KEY_AUTHOR = _STORAGE_PREFIX + "author"
_KEY_WORK = _STORAGE_PREFIX + "work_id"
#: The work facet is the ONE level whose stored value is not also its own name:
#: `work_id` is a `w`-prefixed key. The active-filter chip has to name the work
#: a reader chose, and the chip is rendered BEFORE the facet cascade returns on
#: a cold load, so the label cannot be looked up from the list at render time.
#: What is persisted is therefore the RAW RECORDED TITLE the facet item carried
#: -- data, not a rendered string -- and it is routed through
#: `facet_display_label` (i.e. through ruling R's curation, in the CURRENT
#: language) every time it is shown. Persisting the rendered label instead
#: would show a reader who switches language the title they picked in the
#: other one.
_KEY_WORK_LABEL = _STORAGE_PREFIX + "work_label"
_KEY_PAGE = _STORAGE_PREFIX + "page"


# ---------------------------------------------------------------------------
# The page-local strings that had no home in `tr()` or in the shared claim
# vocabulary. Bilingual, digit-free where the criteria require it, and swept
# through the shared honesty gate by this plan's suite (which iterates
# `copy_keys()`, so an entry added here is swept without editing the test).
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
    # §3.5. `DISCOVERY_FINDINGS_COUNT_MAX` stops the counting query at a cap, so
    # the last page the pager's arithmetic can NAME is not the last page that
    # exists. Left alone, the pager disables `Next` there and reads as "that is
    # all there is" -- a claim a capped count cannot support, and one that hides
    # rows behind a tuning knob nobody on the page can see.
    #
    # Deliberately says MAY: the count stopped, so the page genuinely does not
    # know how many more there are, and a figure here would be invented.
    "pager_capped_note": {
        "en": "There may be more pages than this count shows.",
        "he": "ייתכן שיש עוד עמודים מכפי שהמספר הזה מראה.",
    },
    # The second-bucket counterpart of tr('Showing the {bucket} by default.').
    # The bar must name its bucket in BOTH bucket states.
    "showing_bucket": {
        "en": "Showing {bucket}.",
        "he": "מוצגות {bucket}.",
    },
    # THE RECONCILIATION LINE (ruling F, 2026-08-05). The headline reports what
    # the release CONTAINS -- every identification in the artifact, on one
    # stated basis -- and the default view shows ~23.6% fewer rows than that,
    # because ruling F hides the catalogue-divergent ones. A reader who counts
    # would find the gap and have nothing to attribute it to.
    #
    # The headline was deliberately NOT made to track the reader's filters: a
    # corpus figure that moves when a toggle moves stops being a corpus figure,
    # and ruling U's whole discipline is that a figure names its own basis and
    # keeps it. So the OTHER number explains itself, here, beside the rows it
    # describes -- and it says so in BOTH directions, because a line that
    # appeared only while something was excluded would leave the wider view
    # unexplained and the two figures disagreeing again.
    #
    # NO FIGURE of its own. A count of what is excluded would be a third number
    # on a fourth basis, which is the mixed-basis defect ruling U was issued
    # over; and the exclusion is a category, which words state exactly.
    "divergence_excluded": {
        "en": "Not counted here: findings that disagree with the catalogue.",
        "he": "לא נכללים כאן: ממצאים החולקים על הקטלוג.",
    },
    "divergence_included": {
        "en": "Counted here: findings that disagree with the catalogue.",
        "he": "נכללים כאן: ממצאים החולקים על הקטלוג.",
    },
    # The amber tag on a filter whose backing data is missing. A filter that
    # silently vanishes is indistinguishable from a filter that never existed.
    "needs_tag": {
        "en": "not available yet",
        "he": "עדיין לא זמין",
    },
    # The pool card's header -- it NAMES THE AXIS, not the selection on it.
    # "main pool" and "more matches" are the two values; this is the question
    # they answer, and it is a question rather than a claim about either. It
    # deliberately does not retype either bucket name: those have exactly one
    # definition (`shared.discovery_main_pool.bucket_label`) and a test fails
    # on a literal of either anywhere in this module.
    "pool_card_header": {
        "en": "Which pool",
        "he": "באיזה מאגר",
    },
    # The divergence card's header -- it NAMES THE AXIS, exactly as the pool
    # card's does, and leaves the ACTION to the ratified toggle wording on the
    # control below it. It names a relationship between two records ("the
    # catalogue says something else here") and takes no side: ruling F's whole
    # posture is that the SYSTEM never treats the catalogue's disagreement as a
    # verdict, so a header reading "catalogue conflicts" or anything with a
    # direction in it would concede the point the toggle exists to refuse.
    "divergence_card_header": {
        "en": "Catalogue disagreements",
        "he": "מחלוקות עם הקטלוג",
    },
    # THE SECOND-POOL INVITATION, rendered beside the results in BOTH bucket
    # states. The sidebar control works, is one click, and switches the whole
    # result set; what it never did was say that a second corpus exists, what
    # is in it, or why a reader would look -- so a comparable body of
    # identifications was reachable only by noticing that one of two pills was
    # not selected.
    #
    # The wording carries NO count and NO quality language, which is what makes
    # it publishable at all. "The same works and the same kinds of match" is a
    # statement about WHAT IS THERE, and it is the measured shape of that pool
    # (its largest single group is same-work claims); "the evidence did not
    # meet the main-pool rule" restates `bucket_label`'s own ratified gloss and
    # is a statement about EVIDENCE, never a verdict that those identifications
    # are wrong. `{bucket}` / `{main_bucket}` are filled from `bucket_name`,
    # the single definition, so neither name is retyped here.
    "pool_invite_heading": {
        "en": "There is a second pool",
        "he": "יש מאגר שני",
    },
    "pool_invite_body": {
        "en": (
            "The same works and the same kinds of match also appear under "
            "'{bucket}', where the evidence did not meet the main-pool rule."
        ),
        "he": (
            "אותם חיבורים ואותם סוגי התאמה מופיעים גם תחת '{bucket}', "
            "שם הראיות לא עמדו בכלל המאגר העיקרי."
        ),
    },
    # THE SIZED VARIANT (owner ruling, 2026-08-05). The pool carried no number
    # anywhere, deliberately: this design did not need one, and a figure was an
    # owner ruling rather than a designer's choice. The owner has now ruled that
    # it may be shown, so it is shown HERE -- on the invitation, where a reader
    # decides whether to go and look -- and NOT on the bucket control, which
    # ruling T keeps digit-free.
    #
    # `{count}` IS A SIZE. What it may never become is a quality figure: the
    # owner's assessment of that pool must not turn into a percentage, a rate,
    # an interval or a score anywhere, and a count of what is in a pool is a
    # different kind of fact from a judgement about it. The sentence is
    # otherwise the digit-free one, verbatim, so nothing about the framing
    # moved: the second pool is where the EVIDENCE did not meet the rule, never
    # "findings you are missing" and never a lure.
    #
    # The figure is read from the artifact at request time
    # (`meta.more_pool_total`) and is a LITERAL nowhere -- ruling U, enforced by
    # a repository-wide scan that fails on any committed launch figure.
    "pool_invite_body_counted": {
        "en": (
            "{count} more identifications appear under '{bucket}', where the "
            "evidence did not meet the main-pool rule. They are the same works "
            "and the same kinds of match."
        ),
        # The Hebrew deliberately does NOT open with the figure. A Latin-digit
        # run at the start of an RTL sentence reorders unpredictably at the
        # boundary with the word after it; leading with `עוד` puts the number
        # inside the sentence, where the paragraph direction settles it.
        "he": (
            "עוד {count} זיהויים מופיעים תחת '{bucket}', שם הראיות לא עמדו "
            "בכלל המאגר העיקרי. אלה אותם חיבורים ואותם סוגי התאמה."
        ),
    },
    "pool_here_heading": {
        "en": "You are in '{bucket}'",
        "he": "אתם ב'{bucket}'",
    },
    "pool_here_body": {
        "en": (
            "These are the same kinds of match as the {main_bucket}, at less "
            "evidence than the main-pool rule asks for. Read each one as a lead "
            "to check."
        ),
        "he": (
            "אלה אותם סוגי התאמה כמו ב{main_bucket}, בראיות מועטות מכפי שכלל "
            "המאגר העיקרי דורש. קראו כל אחת ככיוון לבדיקה."
        ),
    },
    # The AUTHOR and WORK selects' own placeholder (2026-08-05). `ui.select`
    # takes its label as a plain argument and an AST check forbids a literal
    # there, so the two strings live here beside the page's other copy rather
    # than inline. They name the ACTION ("search"), because the card header
    # above already names the axis -- a placeholder that repeated the axis name
    # would be the axis stated twice and the affordance stated nowhere.
    "facet_search_author": {
        "en": "Search authors...",
        "he": "חיפוש מחברים...",
    },
    "facet_search_work": {
        "en": "Search works...",
        "he": "חיפוש חיבורים...",
    },
    # An `ok` facet envelope carrying NO items. Deliberately a different
    # statement from `needs_tag` above, because it is a different fact: that
    # one says the data to filter on is absent, this one says the data is
    # there and the current filters select none of it. A card that said
    # nothing at all -- which is what an empty loop produces -- reads as a
    # broken card under a loud header, and was reported as exactly that.
    "facet_empty": {
        "en": "No matches under the current filters.",
        "he": "אין התאמות בסינון הנוכחי.",
    },
    # The collapsed panel that now carries the page's explanatory prose. The
    # prose itself is UNCHANGED and comes from the same shared vocabulary it
    # always did; this title is the only new string, and it names an
    # affordance rather than making any claim about the data.
    "howto_title": {
        "en": "How to read this page",
        "he": "איך לקרוא את הדף הזה",
    },
    # Why the candidacy switch is inert while the row unit is a work. The axis
    # asks about ONE work on ONE fragment; a work row covers many, so the
    # service does not offer the combination at all -- and a control that
    # silently stopped filtering would be worse than one that says it cannot.
    "novelty_unit_note": {
        "en": (
            "Not offered while each row is a work: this asks about one work on "
            "one fragment, and a work row covers many fragments."
        ),
        "he": (
            "לא מוצע כאשר כל שורה היא חיבור: השאלה נוגעת לחיבור אחד בקטע אחד, "
            "ושורת חיבור מאגדת קטעים רבים."
        ),
    },
}


def _lang_key(lang: str) -> str:
    return "he" if lang == "he" else "en"


def copy_text(key: str, lang: str = "en") -> str:
    """One of the page-local bilingual strings. Raises on an unknown key
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

#: The mode strip. "All findings" is live; the other two ship visible and inert
#: so plans 137/138 add a tab rather than a page.
#:
#: The badge says "Coming soon", NOT the plan number. It previously read
#: "Phase 138" / "Phase 137" — internal planning vocabulary rendered to readers,
#: who have no way to know what a phase is or when one lands. The tab stays
#: inert either way; only the word a reader sees changes.
_MODES: Tuple[Tuple[str, Optional[str]], ...] = (
    ("All findings", None),
    ("Screening leads", "Coming soon"),
    ("My saved", "Coming soon"),
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

    state = {
        "unit": unit,
        "bucket": bucket,
        "sort": sort,
        "novelty_only": bool(safe_user_get(_KEY_NOVELTY, False)),
        "divergence": bool(safe_user_get(_KEY_DIVERGENCE, False)),
        "domain": _opt(_KEY_DOMAIN),
        "author": _opt(_KEY_AUTHOR),
        "work_id": _opt(_KEY_WORK),
        "work_label": _opt(_KEY_WORK_LABEL),
        "page": page if page >= 1 else 1,
    }
    return normalise_state(state)


def normalise_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """Settle the axes that are not independent, IN PLACE, and return `state`.

    Exactly one such pair ships: the candidacy switch and the row unit. Novelty
    is a verdict about one work on one fragment, so the service does not offer
    it on the per-work unit and its builder RAISES on the combination -- which
    means a page that lets a reader hold both is a page that can be driven into
    an unhandled failure. It could, and it did: the switch stayed live and the
    "Show as" control changed the unit underneath it (code review round 15,
    finding 1).

    Called from `read_state` (so a persisted or hand-edited pair cannot arrive
    already inconsistent) and from the ONE refresh path (so no control can leave
    it inconsistent). The predicate is the SERVICE's own
    `findings_novelty_offered`, never a comparison restated here.
    """
    if not findings_novelty_offered(state.get("unit")):
        state["novelty_only"] = False
    return state


def write_state(state: Dict[str, Any]) -> None:
    """Persist the reader's selections through the storage chokepoint."""
    safe_user_set(_KEY_UNIT, state["unit"])
    safe_user_set(_KEY_BUCKET, state["bucket"])
    safe_user_set(_KEY_SORT, state["sort"])
    safe_user_set(_KEY_NOVELTY, bool(state["novelty_only"]))
    safe_user_set(_KEY_DIVERGENCE, bool(state["divergence"]))
    safe_user_set(_KEY_DOMAIN, state["domain"])
    safe_user_set(_KEY_AUTHOR, state["author"])
    safe_user_set(_KEY_WORK, state["work_id"])
    safe_user_set(_KEY_WORK_LABEL, state.get("work_label"))
    safe_user_set(_KEY_PAGE, state["page"])


def _novelty_selection(state: Dict[str, Any]) -> Optional[Tuple[str, ...]]:
    """The candidacy filter, as the service's novelty argument.

    `None` (the empty selection) means ALL -- the phase-wide convention that
    filters compose as AND and an empty set is not a filter.

    The unit test is the LAST line of defence, not the first: `normalise_state`
    has already cleared the flag on a unit that does not offer the axis, and the
    switch is disabled there. Repeating it here costs one comparison and makes
    the illegal argument unreachable from this module even if a future control
    forgets to normalise -- the failure it prevents is an unhandled `ValueError`
    on a live page, not a wrong number.
    """
    if not state.get("novelty_only"):
        return None
    if not findings_novelty_offered(state.get("unit")):
        return None
    return (CANDIDATE_STATUS,)


async def fetch_findings(state: Dict[str, Any]) -> Dict[str, Any]:
    """One enveloped findings read. A DIRECT await on the async wrapper."""
    return await get_findings_enveloped(
        state["unit"],
        bucket=state["bucket"],
        novelty=_novelty_selection(state),
        include_divergent=bool(state.get("divergence")),
        domain=state.get("domain"),
        author=state.get("author"),
        work_id=state.get("work_id"),
        sort=state["sort"],
        page=state["page"],
        page_size=_default_page_size(),
    )


async def fetch_launch_stats() -> Dict[str, Any]:
    """One enveloped launch-statistics read. A DIRECT await on the async
    wrapper.

    `web.discovery.get_launch_stats_enveloped` is THE only supported way to
    obtain any launch figure: every number is computed from the artifact being
    served at request time, on the single basis ruling U fixed, and carries the
    sidecar version and audience that produced it. Not one of those numbers may
    appear as a literal anywhere in code or in a translation, and plan 136-22
    ships a repo-level guard that fails naming the file, the line and this
    accessor.
    """
    return await get_launch_stats_enveloped()


def _facet_request(level: str, state: Dict[str, Any]) -> Dict[str, Any]:
    """The EXACT arguments ONE facet-cascade read takes at `level`.

    Written ONCE and used by two callers -- the read itself and the re-fetch
    skip key below. That is the whole point: the skip is only sound if the key
    varies on everything the query varies on, and a key derived from a second,
    hand-written list of inputs is one edit away from omitting one. Omitting one
    means a facet list kept beside a result set it no longer describes, which is
    the precise defect the re-fetch exists to fix.

    The cascade narrows DOWNWARDS only: `domain` is offered unfiltered, `author`
    within the chosen domain, `work` within the chosen domain and author. A
    level never filters on itself, or picking a value would empty its own list.
    """
    if level not in FACET_LEVELS:
        raise ValueError(
            "_facet_request: unknown facet level {!r} (expected one of {})".format(
                level, sorted(FACET_LEVELS)
            )
        )
    return {
        "bucket": state["bucket"],
        "novelty": _novelty_selection(state),
        "include_divergent": bool(state.get("divergence")),
        # THE ROW UNIT (§3.6). The cascade's counts are counts of ROWS, and the
        # unit decides what a row is -- so a cascade that did not carry it put a
        # number beside an option describing a population the result bar beside
        # it did not report. It is part of the request, so it is part of the
        # re-fetch key: changing the unit re-reads all three levels.
        "unit": state["unit"],
        "domain": state.get("domain") if level != "domain" else None,
        "author": state.get("author") if level == "work" else None,
    }


def _artifact_identity() -> Tuple[Any, Any]:
    """WHICH artifact an answer came from -- its path and its version.

    BOTH, not either. The version alone is not an identity: every local artifact
    in this project reports the same `sidecar_version` string while holding
    different data (the service's own launch-stats cache documents exactly that
    trap and keys on the pair for the same reason). The path alone would miss a
    rebuild written in place.

    Two pure in-memory reads of state loaded at startup -- no I/O, so this is
    safe to call from a synchronous key builder.
    """
    return (discovery_db_path(), discovery_sidecar_version())


def _facet_cache_key(level: str, state: Dict[str, Any]) -> Tuple[Any, ...]:
    """A hashable rendering of `_facet_request`, plus WHICH ARTIFACT answered.

    Two reads with the same arguments against the same artifact return the same
    envelope, so an unchanged key is a provably unchanged answer rather than an
    assumption about which control the reader touched.

    THE ARTIFACT IS PART OF THE KEY (§3.1). Without it, a rebuild swapped in
    under a page that stays open left the cached facet COUNTS from the old
    artifact sitting beside ROWS re-read from the new one -- a number beside an
    option describing a population the option no longer produces, which is the
    one promise `_node_text` makes. The window was bounded (the cache is a local
    of ONE `_render_body` call, never a module singleton), and a bounded window
    in which a count is wrong is still a count that is wrong.
    """
    return ((level,) + tuple(sorted(_facet_request(level, state).items()))
            + _artifact_identity())


async def fetch_facets(level: str, state: Dict[str, Any]) -> Dict[str, Any]:
    """One enveloped facet-cascade read. A DIRECT await on the async wrapper."""
    return await get_findings_facets_enveloped(level, **_facet_request(level, state))


# ---------------------------------------------------------------------------
# Client-liveness guard. `page_client` is bound at RENDER time (never lazily
# inside a handler): a late binding is a latent failure rather than an error,
# because a background context has no UI context to read it from.
# ---------------------------------------------------------------------------

def _page_count(total: Any, size: int) -> int:
    """How many pages a set of `total` rows has, at `size` per page.

    ONE arithmetic, used by the pager and by the out-of-range clamp alike. A
    second copy is how the two come to disagree about which page is the last
    one -- and disagreeing about that is exactly the defect the clamp exists to
    fix.
    """
    try:
        rows = int(total or 0)
    except (TypeError, ValueError):  # pragma: no cover -- defensive
        rows = 0
    if rows <= 0 or size <= 0:
        return 1
    return max(1, math.ceil(rows / size))


def effective_page_size(envelope: Dict[str, Any]) -> int:
    """The page size the SERVICE actually used, not the one this page asked for.

    `_clamp_findings_page_size` applies the shared `DISCOVERY_PAGE_SIZE_MAX`
    ceiling server-side, so with `DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT` set
    above it the two numbers differ -- and a pager that divides a real total by
    the number the service REFUSED reports too few pages and leaves the tail of
    the set unreachable, with nothing on the page saying so. The envelope
    reports what it was built with; that is the only number the arithmetic here
    may use.

    Falls back to the budgeted default when the envelope does not say (an
    outage, or an older reader): a fallback is unavoidable, and the budgeted
    default is the same value the service would have clamped from.
    """
    size = (envelope or {}).get("meta")
    size = (size or {}).get("page_size")
    if isinstance(size, int) and not isinstance(size, bool) and size > 0:
        return size
    return _default_page_size()


def clamp_page_to_total(state: Dict[str, Any], envelope: Dict[str, Any]) -> bool:
    """Pull a persisted page back inside the real set. Returns whether it moved.

    THE STATE, not a local for display. `_render_pager` already clamped a local
    copy, which fixed what the pager PRINTED and nothing else: the next
    `fetch_findings` still sent the out-of-range page, so the reader stayed on
    an empty result with both pager buttons disabled and no way out. `page` is
    persisted, so that state survived a reload -- a reader whose filters had
    narrowed under them was told the corpus was empty, permanently, and the
    caller has to REFETCH once this returns True.

    Only ever moves DOWNWARDS, and only on an `ok` envelope: an outage carries
    no trustworthy total, and clamping against one would turn a temporary
    failure into a persisted page-1 reset.

    ONLY WHEN THE PAGE CAME BACK EMPTY, and that condition is what makes this
    safe under `DISCOVERY_FINDINGS_COUNT_MAX`. A capped total is a LOWER BOUND,
    so a page above `total / size` can still be full of rows -- and clamping a
    reader off a page they can see would be this fix creating the very defect
    §3.5 is about. An empty page is the only evidence that a page is really past
    the end; under a cap it clamps to the last page the count can vouch for,
    which is a page that certainly has rows.
    """
    if (envelope or {}).get("status") != "ok":
        return False
    if envelope.get("items"):
        return False
    pages = _page_count(envelope.get("total"), effective_page_size(envelope))
    try:
        current = int(state.get("page") or 1)
    except (TypeError, ValueError):  # pragma: no cover -- read_state normalises
        current = 1
    if current <= pages:
        return False
    state["page"] = pages
    return True


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

    with ui.column().classes(f"{ROOT_CLASS} {PAGE_CLASS} w-full max-w-7xl mx-auto p-4 gap-4"):
        headline_region = _render_head(lang)
        _render_mode_strip(lang)
        body = ui.column().classes("w-full gap-3")

    if _page_is_gone(page_client):
        return
    # ONE launch read, TWO consumers: the headline paints from the envelope and
    # the pool invitation takes the second pool's size out of its `meta`. Read
    # once rather than twice, because a second read is a second executor
    # crossing on the heavy budget for a figure the page already has -- and not
    # inside `refresh`, which would pay that crossing on every filter change for
    # a number that cannot move while the page is open.
    #
    # STARTED, NOT AWAITED (§3.7). It used to be awaited HERE, before the body
    # was rendered at all, so the rows waited for a corpus-scale count in SERIES
    # -- while `_paint_headline`'s own docstring claimed a slow headline read
    # never delays the rest of the page. Dispatching it as a task lets it run
    # CONCURRENTLY with the body's own read: the page now waits for the slower of
    # the two rather than for their sum.
    #
    # A task, not a bare coroutine, because two places await it and a coroutine
    # can only be awaited once. `fetch_launch_stats` touches no UI, so starting
    # it outside the slot stack is safe (a UI-touching background task would not
    # be -- it would have no slot to render into).
    launch = asyncio.ensure_future(fetch_launch_stats())
    with body:
        await _render_body(state, lang, page_client, launch=launch)
    if _page_is_gone(page_client):
        return
    # LAST, deliberately. The headline region was reserved above the body and is
    # painted into afterwards, which is what makes this order a layout-neutral
    # choice rather than a visible one.
    await _paint_headline(headline_region, lang, page_client,
                         envelope=await launch)


def _render_head(lang: str) -> Any:
    """Centred title, the ONE permanent caveat, the RESERVED launch-headline
    region, and the collapsed "how to read this page" panel -- in that order.

    THE HEAD IS DELIBERATELY SHORT. It previously opened with a wall of prose:
    a caveat line, a four-line headline, a bordered disclaimer, a heading with
    three explanatory lines under it, a sources-checked line and a bucket
    explanation, all before the first control. The owner read that as a draft
    rather than a page. Everything except the caveat and the headline now lives
    one click away in `_render_howto`; NOTHING is deleted and NOTHING is
    reworded, because every line of it is honesty-critical text under D-06a and
    the match-framing rule. Moving it was the task. Rewriting it was not.

    Returns the headline region's stable empty child, which `_paint_headline`
    fills once the artifact-backed read returns."""
    with ui.column().classes(f"phead {HEAD_CLASS} w-full gap-2"):
        h1(
            tr("Computed Identifications"),
            classes="text-3xl font-bold text-center w-full",
            style="color: var(--primary-700);",
        )
        # The caveat IS this page's one line of description: it says what a row
        # is and what it is not, which is exactly what a reader needs before
        # anything else on the page.
        _render_caveat(lang)
        region = _render_headline_slot(lang)
        _render_howto(lang)
    return region


def _render_howto(lang: str) -> None:
    """The demoted prose, collapsed by default and complete.

    Four pieces of copy live here, every one of them VERBATIM from the shared
    vocabulary that owns it:

    * `recall_disclaimer` -- the "not exhaustive" note;
    * the candidacy sub-line, which says what the novelty switch selects;
    * `rows.render_novelty_help` -- the checked-source list, the "candidate,
      not a confirmed find" sentence and the date the sources were checked as
      of (read from the artifact's own meta, omitted rather than guessed);
    * `rule_sentence` -- the two-bucket rule, in the one place it is worded.

    Collapsed is not hidden: the panel is always rendered, always in the page's
    own head, and its title names what it holds. The alternative the owner
    rejected was leaving all of it stacked above the first control, where -- by
    his own account -- it read as a draft and therefore got read by nobody.
    """
    panel = ui.expansion(copy_text("howto_title", lang), value=False)
    panel.classes(f"{HOWTO_CLASS} w-full").props("dense expand-separator")
    with panel:
        with ui.column().classes("w-full gap-2 p-1"):
            ui.label(recall_disclaimer(lang)).classes(
                f"{HOWTO_CLASS}-recall dnote text-xs"
            )
            ui.label(novelty_strings(lang)["subline"]).classes(
                f"{HOWTO_CLASS}-novelty dnote text-xs"
            )
            # The help affordance, VERBATIM: the checked-source list, the
            # "candidate, not a confirmed find" sentence, and the as-of date
            # read from the artifact's own meta -- omitted entirely when the
            # artifact records none rather than dated by guess.
            rows.render_novelty_help(lang, as_of=discovery_meta("data_as_of"))
            ui.label(rule_sentence(lang)).classes(f"{HOWTO_CLASS}-rule dnote text-xs")


def _render_headline_slot(lang: str):
    """The launch-headline region: reserved by plan 136-16, PAINTED by
    `_paint_headline` from plan 136-22's artifact-backed reader.

    This function still writes NO NUMBER of any kind -- it builds the named,
    structurally-present container and its bilingual accessible label and
    returns the empty child. Every figure arrives later, from the envelope. A
    placeholder digit here would survive as a hardcoded launch number, which is
    precisely the failure ruling U was issued to prevent.
    """
    region = ui.column().classes(f"{HEADLINE_SLOT_CLASS} w-full gap-1")
    region.props(f'role=region aria-label="{copy_text("headline_slot_label", lang)}"')
    with region:
        # The stable child the launch statistics are painted into.
        value = ui.element("div").classes(f"{HEADLINE_SLOT_CLASS}-value")
    return value


async def _paint_headline(region: Any, lang: str, page_client: Any,
                          envelope: Optional[Dict[str, Any]] = None) -> None:
    """Render the launch statistics into the reserved slot.

    Painted into a region RESERVED during the shell and filled afterwards, so a
    failing headline read never breaks the rest of the page; and re-entrant, so
    the outage state's retry re-runs exactly this path rather than a second copy
    of it.

    WHAT THIS DOES NOT CLAIM, corrected 2026-08-05 (§3.7). It used to promise
    that a slow read here holds nothing else up, while the caller awaited that
    read before rendering the body at all -- the claim was the opposite of the
    code. The read is now DISPATCHED before the body and awaited after it, so
    the two corpus-scale reads overlap and the page waits for the slower of them
    rather than for their sum. That is an improvement and not isolation: the
    pool invitation shows the second pool's size from the SAME envelope (an
    owner ruling, 2026-08-05), so the body has a real data dependency on this
    read, and a first paint that ignored it would show a figure-less invitation
    that never gained its figure.

    (The retired wording is deliberately not quoted here. A test greps this
    docstring for it, and a docstring that quotes the phrase it retired makes
    that test fire on the explanation rather than on a regression.)

    `envelope` is the read the CALLER already made -- the page issues one launch
    read and two surfaces consume it. The RETRY passes none and re-reads, which
    is the point of a retry: a retry that re-rendered the same failed envelope
    would be a button that cannot work.
    """
    if region is None:  # pragma: no cover -- structural
        return
    if envelope is None:
        envelope = await fetch_launch_stats()
    if _page_is_gone(page_client):
        return

    async def _retry(_event=None) -> None:
        await _paint_headline(region, lang, page_client)

    region.clear()
    with region:
        rows.render_launch_headline(envelope, lang, on_retry=_retry)


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


async def _render_body(state: Dict[str, Any], lang: str, page_client: Any,
                       launch: Any = None) -> None:
    """The two-column body -- a sidebar of filter CARDS and the results beside
    it -- with ONE refresh path shared by every control, so a filter change and
    a bucket change take exactly the same route.

    THE COLUMNS ARE FLEX RATIOS, NOT A MEDIA QUERY -- because this work adds no
    CSS at all, and a media query would need a stylesheet rule. The wrapper is a
    plain flex `div` rather than `ui.row()` ON PURPOSE: `ui.row()` carries
    Quasar's `row` class, and the shared block's phone rule
    (`.gs-discovery .row {flex-direction: column}`) would then stack these two
    columns while leaving the sidebar at its 280px basis -- a narrow strip on a
    phone. Wrapping does the same job better: the sidebar's grow factor is tiny
    beside the results column's, so while they share a line the sidebar keeps
    ~280px, and once the results wrap below it the sidebar is alone on its line
    and grows to the full width. Phone-first, with no breakpoint to maintain.

    Direction is not set here either. The app puts `dir` on the document
    (Quasar's RTL activation in `web/main.py`), so a plain flex row mirrors
    itself in Hebrew: the sidebar sits on the right and the results on the
    left, with no `flex-row-reverse` and no physical margin anywhere.
    """
    with ui.element("div").classes(
        f"{BODY_CLASS} w-full flex flex-wrap gap-4 items-start"
    ):
        filter_bar = ui.column().classes(
            f"fbar {FILTER_BAR_CLASS} gap-3 items-stretch"
        ).style("flex: 1 1 280px; min-width: 240px;")
        main_column = ui.column().classes(f"{MAIN_CLASS} gap-3").style(
            "flex: 999 1 420px; min-width: 0;"
        )
    with main_column:
        results_region = ui.column().classes(f"{RESULTS_CLASS} w-full gap-2")

    # Controls whose PRESENTATION depends on state another control owns. Only
    # the filter bar has them, and it is built once and never re-rendered (the
    # facet lists are filled after the first paint and re-rendering the bar
    # would drop them), so each such control hands back a callable that
    # re-reads the state it depends on. The ONE refresh path runs them all
    # BEFORE the state is persisted or queried, so what a control shows and
    # what the query does can never disagree.
    control_sync: List[Any] = []

    #: THE FACET RE-FETCH CACHE, per page render. `level -> (request key,
    #: envelope)`, so a refresh re-reads exactly the levels whose inputs moved
    #: and re-renders all three regardless. See `refresh` below.
    facet_cache: Dict[str, Tuple[Any, Dict[str, Any]]] = {}

    #: THE REVISION TOKEN. Every control's handler is a separate task on the one
    #: event loop and every one of them mutates the SHARED `state` dict and then
    #: awaits -- so two overlapping handlers interleave, and without a token the
    #: read that RETURNS LAST paints last regardless of which was issued last.
    #: A reader who switches pool twice quickly, or picks a domain while a slow
    #: bucket read is in flight, could be shown one pool's rows under the other
    #: pool's labels and count. Every other async path on this surface already
    #: carries a guard of this shape (the browse panel's staleness check, the
    #: headline's client check); this one had none.
    generation = {"n": 0}

    async def _more_pool_total() -> Any:
        """The second pool's size, from the launch read the CALLER dispatched.

        The read is started before this function is entered and awaited HERE,
        after the body's own read has been issued -- so the two corpus-scale
        reads overlap instead of running in series (§3.7).

        NO MEMO, and its absence is deliberate. `launch` is a TASK: the first
        await runs it and every later await returns the same result without a
        second read, so a memo would buy nothing and no test could tell it from
        its absence. An unfalsifiable optimisation reads as coverage nobody has,
        which is the defect class this whole package is about. What IS
        load-bearing is that the caller passes a task rather than letting this
        re-read -- and a test drives three refreshes and counts the reads.
        """
        envelope = await launch if launch is not None else None
        return ((envelope or {}).get("meta") or {}).get("more_pool_total")

    async def refresh() -> None:
        """THE one refresh path -- results first, then the facet lists.

        THE FACETS ARE PART OF THIS PATH NOW, and they were not. They were
        filled ONCE, after the first paint, and never again; `refresh` re-rendered
        only the results region. Two things followed, and both were correctness
        failures rather than polish:

        * a count beside a domain described whichever bucket happened to be
          active at first paint. Switch bucket and `_node_text`'s promise -- "a
          number beside a domain always agrees with the result set that domain
          produces" -- stopped being true, silently.
        * THE CASCADE NEVER RAN. `_facet_request` narrows author by domain and
          work by domain+author, but it was only ever evaluated against the state
          as it stood at page load, so picking a domain shortened nothing.

        A third, quieter one: a facet node's `.here` treatment is decided when
        the node is BUILT, so a reader's own selection was never marked either.

        ORDER IS DELIBERATE. The rows paint first and the facet reads follow, so
        a slow (or busy, or timed-out) cascade delays no result the reader came
        for; and `_page_is_gone` is re-checked between the two, because the reads
        below are the ones most likely to still be in flight when a reader
        navigates away.

        BUDGET. Every facet read takes a slot in the HEAVY bounded-concurrency
        budget (`DISCOVERY_MAX_CONCURRENT_QUERIES`, default 4 -- see
        `docs/specs/discovery-budgets.md`), so re-reading all three on every
        interaction would triple this page's draw on the smaller of the two
        budgets. `facet_cache` keeps that at the levels whose OWN request
        arguments changed: a sort change or a page turn re-reads nothing, a
        domain pick re-reads author and work only, and only a bucket or
        candidacy change re-reads all three.

        REVISION SAFETY. The token is taken BEFORE the first await and re-checked
        after every one of them, including inside the facet cascade. A superseded
        pass abandons its own paint entirely rather than trying to merge -- there
        is nothing to merge, because a later handler has already changed the
        state its rows would be labelled with.
        """
        generation["n"] += 1
        mine = generation["n"]

        def _stale() -> bool:
            """Superseded by a newer refresh, or the reader has left."""
            return generation["n"] != mine or _page_is_gone(page_client)

        normalise_state(state)
        for sync in control_sync:
            sync()
        write_state(state)
        if _stale():
            return
        envelope = await fetch_findings(state)
        if _stale():
            return
        # A PERSISTED PAGE PAST THE END. The clamp moves the STATE (never a
        # display-only local) and the refetch is what makes the move real --
        # without it the reader is looking at the empty page they were clamped
        # off. AT MOST ONE extra read: the clamped page is inside the set by
        # construction, so the second envelope cannot be out of range again.
        if clamp_page_to_total(state, envelope):
            write_state(state)
            envelope = await fetch_findings(state)
            if _stale():
                return
        more_pool_total = await _more_pool_total()
        if _stale():
            return
        results_region.clear()
        with results_region:
            _render_results(envelope, state, lang, refresh,
                            more_pool_total=more_pool_total)
        if _stale():
            return
        await _populate_facets(
            filter_bar, state, lang, refresh,
            cache=facet_cache, page_client=page_client, is_stale=_stale,
        )

    with filter_bar:
        control_sync.extend(_render_filter_bar(state, lang, refresh))

    await refresh()


# ---------------------------------------------------------------------------
# Filter bar. The novelty switch is FIRST by CSS order (`.fg.novgrp {order:-1}`)
# regardless of DOM order, which is what keeps it first in BOTH directions.
# ---------------------------------------------------------------------------

def _render_filter_bar(state: Dict[str, Any], lang: str, refresh) -> List[Any]:
    """Build the sidebar cards; return the state-dependent re-sync callables."""
    syncs = [_render_novelty_switch(state, lang, refresh)]
    # The bucket control is state-dependent too, and was not collected -- see
    # `_render_bucket_control`'s "IT WENT STALE".
    syncs.append(_render_bucket_control(state, lang, refresh))
    syncs.append(_render_divergence_switch(state, lang, refresh))
    _render_facet_groups(lang)
    _render_coverage_filter(lang)
    return syncs


def _filter_card(*extra_classes: str) -> Any:
    """One filter card, in `/catalog-browse`'s shape.

    `fg` is what the shared CSS block styles (`.fg.novgrp {order:-1}`,
    `.fg.blocked {opacity:.55}`), so it stays on the CARD rather than on some
    inner wrapper -- otherwise the novelty card would lose its first position
    and a blocked card its dimming.
    """
    return ui.card().classes(
        " ".join(("fg", CARD_CLASS, "w-full p-4 gap-2") + tuple(extra_classes))
    )


def _card_header(text: str, *extra_classes: str) -> Any:
    """The uppercase small-caps card header `/catalog-browse` uses verbatim
    (`text-sm font-bold uppercase tracking-wide` on `--text-secondary`)."""
    label = ui.label(text).classes(
        " ".join((CARD_HEADER_CLASS, "text-sm font-bold uppercase tracking-wide")
                 + tuple(extra_classes))
    )
    return label.style("color: var(--text-secondary);")


def _render_novelty_switch(state: Dict[str, Any], lang: str, refresh):
    """The candidacy switch, first in the filter bar by CSS order.

    Its three explanatory lines moved to `_render_howto`; the switch keeps the
    full help text on its own tooltip, so the wording is still one hover away
    from the control it describes as well as one click away in the panel.

    NO CARD HEADER, deliberately. Every other card is titled from vocabulary
    that already exists; this axis has exactly one ratified name, and it is
    already on the control. A header would either repeat that name twice in one
    card or invent a second name for the same axis -- and a second name for an
    axis is how a vocabulary starts drifting.

    THE SWITCH IS NOT ALWAYS ACTIONABLE, and it says so (code review round 15,
    finding 1). The row unit is a SEPARATE control in the result bar, and the
    service does not offer this axis on the per-work unit -- its builder raises
    rather than returning an envelope. A reader who turned the switch on and
    then chose "one row per work" therefore drove the page into an unhandled
    `ValueError`, and the results simply stopped updating. The card now takes
    the SAME treatment `_render_coverage_filter` already uses for a filter with
    nothing to act on -- dimmed, disabled and tagged with the reason -- and the
    stored selection is dropped rather than kept and silently ignored, so what
    the control shows is what the query does.

    Returns a re-sync callable the one refresh path runs, because the filter
    bar is built once while the unit can change at any time.
    """
    words = novelty_strings(lang)
    with _filter_card("novgrp", f"{FILTER_BAR_CLASS}-novelty") as card:

        async def _toggle(_event=None) -> None:
            # A disabled button is not clickable, and a stale client cannot make
            # it one: the state it would produce is refused here as well.
            if not findings_novelty_offered(state.get("unit")):
                return
            state["novelty_only"] = not state["novelty_only"]
            state["page"] = 1
            await refresh()

        switch = ui.button(words["toggle"], on_click=_toggle).props("flat dense no-caps")
        switch.classes("fchip")
        switch.tooltip(words["help"])
        note = ui.label(copy_text("novelty_unit_note", lang)).classes(
            f"needs {FILTER_BAR_CLASS}-novelty-note"
        )

    def _sync() -> None:
        offered = findings_novelty_offered(state.get("unit"))
        if not offered:
            state["novelty_only"] = False
        switch.props(f'aria-pressed={"true" if state["novelty_only"] else "false"}')
        if offered:
            switch.enable()
        else:
            switch.disable()
        note.set_visibility(not offered)
        if offered:
            card.classes(remove="blocked")
        else:
            card.classes(add="blocked")

    _sync()
    return _sync


#: The two halves of the pool segment. `.fchip` has NO base rule in the shared
#: CSS block and `.fchip.here` has none at all -- only `.chip.here` does, which
#: is a different class on a different element -- so before this the active
#: bucket was announced through `aria-pressed` and through nothing a sighted
#: reader could see. These are the ratified `.chip.here` values, applied
#: INLINE: this work adds no stylesheet rule, and every property here is
#: side-neutral, so neither needs to flip for RTL.
_SEGMENT_OFF = "border-radius: 4px; margin: 0;"
_SEGMENT_ON = (
    "border-radius: 4px; margin: 0; background: var(--bg-active); "
    "color: var(--text-primary); font-weight: 700;"
)


def _render_bucket_control(state: Dict[str, Any], lang: str, refresh):
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

    The card is a plain `q-card` and stays one: putting this control inside an
    expansion, a menu or any other disclosure container would break ruling T,
    and `tests/test_findings_page.py` fails on the ancestor.

    NAMED AND EXPLAINED (2026-08-05). Of the five sidebar cards, the two
    carrying the page's AXES were the only two with no header, while the three
    that merely NARROW all carried the loud uppercase one -- so the page shouted
    its narrowing tools and whispered its axes, and this axis in particular was
    two unlabelled grey pills in a header-less box. Three things changed, all of
    them things ruling T explicitly permits: the card takes a HEADER naming the
    axis, the two chips are joined into ONE VISUAL SEGMENT so they read as one
    control with two states rather than two unrelated buttons, and
    `rule_sentence` -- the one worded statement of what decides between the two
    buckets -- sits under them as card prose.

    THE RULE SENTENCE IS COPIED, NOT MOVED. It stays in `_render_howto` as well;
    a test pins it in both places. Nothing was demoted to make room here.

    STILL NO COUNT, anywhere in this subtree, and no quality language: the
    owner's assessment of the second bucket is an impression over a rendered
    sample with no draw protocol and no blind grading. The bucket names come
    from the shared vocabulary, in match framing -- the second bucket means
    there was not enough evidence for the main-pool rule, never that those
    identifications are probably wrong.

    IT WENT STALE (fixed 2026-08-05, found by two external reviewers). This
    function returned None and the filter bar collected a re-sync callable only
    from the novelty switch -- and the bar is BUILT ONCE, so `selected` was
    evaluated at build time and never again. Clicking "more matches" switched
    the result set while the chip kept `here` on the main pool and
    `aria-pressed="true"` stayed on the wrong control: ruling T's own control
    visibly and accessibly contradicting the result set it had just produced,
    for sighted and screen-reader readers alike.

    It now returns a sync callable the ONE refresh path runs before the state is
    persisted or queried, exactly as `_render_novelty_switch` does, so what the
    control shows and what the query did cannot disagree.
    """
    chips: List[Tuple[str, Any]] = []
    with _filter_card(f"{BUCKET_CONTROL_CLASS}-group"):
        _card_header(copy_text("pool_card_header", lang))
        # ONE SEGMENT. The enclosing rule and the zero gap are what turn two
        # loose pills into one control; `border` and `border-radius` are
        # side-neutral, so nothing here needs to flip for RTL. Inline, because
        # this work adds no stylesheet rule (a test diffs `common.css`).
        with ui.row().classes(
            f"{BUCKET_CONTROL_CLASS} items-center flex-wrap"
        ).style(
            "gap: 0; border: 1px solid var(--border-light); border-radius: 6px; "
            "padding: 2px; width: fit-content; max-width: 100%;"
        ):
            for in_main in (True, False):
                target = BUCKET_MAIN if in_main else BUCKET_MORE
                label = bucket_name(in_main, lang)

                async def _select(_event=None, target=target) -> None:
                    state["bucket"] = target
                    state["page"] = 1
                    await refresh()

                chip = ui.button(label, on_click=_select).props("flat dense no-caps")
                chips.append((target, chip))
        ui.label(rule_sentence(lang)).classes(
            f"{BUCKET_CONTROL_CLASS}-rule dnote text-xs"
        )

    def _sync() -> None:
        """Re-read `state['bucket']` and re-apply BOTH announcements.

        All three of them, together: the `here` class a sighted reader sees, the
        `aria-pressed` a screen reader hears, and the inline segment styling
        that is the only visible difference between the two halves. Applying one
        without the others is how the control announced its state to one reader
        and not the other in the first place.
        """
        for target, chip in chips:
            selected = state["bucket"] == target
            # CLASSES: add/remove, never `replace` -- `replace` drops every
            # other class on the element, the framework's included.
            chip.classes(add="fchip here" if selected else "fchip",
                         remove="" if selected else "here")
            chip.props(f'aria-pressed={"true" if selected else "false"}')
            # STYLE: `replace`, and it has to be. The two segment styles are not
            # symmetric -- the selected one sets a background, a colour and a
            # weight that the unselected one does not mention -- so ADDING the
            # unselected style over the selected one would leave a deselected
            # chip still painted as selected.
            chip.style(replace=_SEGMENT_ON if selected else _SEGMENT_OFF)

    _sync()
    return _sync


def _render_divergence_switch(state: Dict[str, Any], lang: str, refresh):
    """RULING F'S AXIS -- the third one, and the only one that is off by
    default because of what the rows ARE rather than what a reader picked.

    12,664 of 53,581 identifications (23.6%) contradict a catalogue
    identification. Until this card existed they rendered exactly like the ones
    that confirm it: no toggle, no label, no warning. The policy had been in the
    code since plan 136-04 as `HIDDEN_BY_DEFAULT_SHADES` and
    `is_hidden_by_default`, and nothing outside `tests/` had ever called either.

    A CARD IN THE SIDEBAR, in the pool card's own shape -- header, control,
    prose -- rather than a footnote or a disclosure. The owner's framing is that
    this pool contains many false identifications AND real findings, so the
    toggle is not only a safety measure: it is the ACCESS PATH to the 3,229
    main-pool divergent rows, which is where a scholar would go looking. It is
    designed as a place worth visiting.

    WHAT THE COPY MAY NOT DO (ruling F, and each has a test): it must state THAT
    the catalogue and the computed identification disagree, say explicitly that
    neither side has been adjudicated, and never assert which side is right.
    `divergence_correctness` is HUMAN-ONLY (ruling L) and is NULL on every
    shipped row, so there is no adjudication anywhere to imply. The toggle's
    label is the RATIFIED `TOGGLE_DIVERGENCE` wording and the prose is the
    ratified `divergence_warning`; neither is composed here.

    Returns a re-sync callable for the same reason the two cards above it do:
    the bar is built once, and a control that announces a state it no longer has
    is the defect two external reviewers found on the bucket chip.
    """
    with _filter_card(f"{FILTER_BAR_CLASS}-divergence"):
        _card_header(copy_text("divergence_card_header", lang))

        async def _toggle(_event=None) -> None:
            state["divergence"] = not state.get("divergence")
            state["page"] = 1
            await refresh()

        switch = ui.button(
            disclosure_toggle(TOGGLE_DIVERGENCE, lang), on_click=_toggle
        ).props("flat dense no-caps")
        switch.classes("fchip")
        # THE WARNING IS CARD PROSE, not a tooltip. Ruling F's toggle is an
        # "explicitly warned" one, and a warning a reader has to hover to find
        # is not one they were given before they chose.
        ui.label(divergence_warning(lang)).classes(
            f"{FILTER_BAR_CLASS}-divergence-warning dnote text-xs")

    def _sync() -> None:
        on = bool(state.get("divergence"))
        switch.classes(add="fchip here" if on else "fchip",
                       remove="" if on else "here")
        switch.props(f'aria-pressed={"true" if on else "false"}')
        # `.fchip.here` has no rule in the shared CSS block -- only `.chip.here`
        # does, on a different element -- so without this the state would be
        # announced to a screen reader and to nobody else. The same two ratified
        # `.chip.here` values the bucket segment uses, applied inline (this work
        # adds no stylesheet rule) and side-neutral in both directions.
        switch.style(replace=_SEGMENT_ON if on else _SEGMENT_OFF)

    _sync()
    return _sync


def _render_coverage_filter(lang: str) -> None:
    """Rendered, visibly disabled, and tagged -- never silently absent.

    The service exposes no coverage predicate, so this filter has no backing
    data to act on. A filter that silently vanishes is indistinguishable from a
    filter that never existed, so the treatment stays even though the state
    should not occur once the axis is wired.
    """
    with _filter_card("blocked", f"{FILTER_BAR_CLASS}-coverage"):
        with ui.row().classes("gap-2 items-center flex-wrap"):
            _card_header(coverage_label(lang))
            ui.label(copy_text("needs_tag", lang)).classes("needs")
        chip = ui.button(coverage_label(lang)).props("flat dense no-caps")
        chip.classes("fchip")
        chip.disable()


def _render_facet_groups(lang: str) -> None:
    """One titled CARD per facet, in the sidebar column.

    Populated after the first paint by `_populate_facets`, so the filter bar's
    structure exists before any facet read returns.
    """
    for level, label_key in (("domain", "Domain"), ("author", "Author"), ("work", "Work")):
        with _filter_card(f"{FILTER_BAR_CLASS}-{level}"):
            # The DOMAIN header names its axis explicitly. Filtering on the
            # MANUSCRIPT's catalogue domain would hide exactly the findings that
            # disagree with the catalogue -- a manuscript catalogued as court
            # records carries a verifiably correct commentary identification --
            # so a header that leaves the axis to inference is a header a reader
            # can read the wrong way.
            header = (
                rows.copy_text("facet_domain_header", lang)
                if level == "domain" else tr(label_key)
            )
            _card_header(header, FACET_HEADER_CLASS, f"{FACET_HEADER_CLASS}-{level}")
            # A long list scrolls INSIDE its card rather than pushing every
            # card below it off the screen. The cap is generous enough that
            # the domain tree's parents are all reachable without scrolling on
            # a desktop viewport.
            ui.column().classes(
                f"{FILTER_BAR_CLASS}-{level}-items w-full gap-1"
            ).style("max-height: 340px; overflow-y: auto;")


def _facet_containers(filter_bar: Any) -> Dict[str, Any]:
    containers: Dict[str, Any] = {}
    for element in filter_bar.descendants(include_self=True):
        classes = getattr(element, "_classes", None) or []
        for level in ("domain", "author", "work"):
            if f"{FILTER_BAR_CLASS}-{level}-items" in classes:
                containers[level] = element
    return containers


async def _prime_domain_labels(lang: str) -> None:
    """Load the bilingual domain vocabulary OFF the event loop, once.

    `works.genre` is stored entirely in ENGLISH, and it is this page's main
    facet -- so without this a Hebrew reader gets a Hebrew page with an English
    filter list. FJMS already holds the authority (`DomainHeb` /
    `ParentDomainHeb`), and `web/discovery_genre_labels.py` maps one to the
    other at DISPLAY time: the stored value the service filters on never
    changes, so a domain chosen in Hebrew narrows exactly what the same domain
    chosen in English narrows.

    Only in Hebrew, and only until the map is built: `prime_domain_translations`
    is a no-op (and issues NO executor dispatch) once the process has it.
    """
    if lang != "he":
        return
    await prime_domain_translations()


async def _populate_facets(
    filter_bar: Any, state: Dict[str, Any], lang: str, refresh,
    *,
    cache: Optional[Dict[str, Tuple[Any, Dict[str, Any]]]] = None,
    page_client: Any = None,
    is_stale: Optional[Any] = None,
) -> None:
    """Fill the three facet lists from the cascade -- on EVERY refresh.

    Every work-level label routes through `display_work_title` (ruling R): the
    cascade selects the RAW recorded title at the work level, and a facet list
    that prints it directly opts out of the curation in the very control a
    reader uses to find that work.

    RE-READ AND RE-RENDER ARE SEPARATE DECISIONS, and keeping them separate is
    what makes this both correct and affordable. Every level is RE-RENDERED
    every time, because what a node LOOKS like depends on the reader's current
    selection (`.here`, `aria-pressed`, and the domain branch that must open
    around a selected leaf) and that is not part of the query at all. Only the
    READ is skipped, and only when `_facet_cache_key` -- the request tuple
    itself -- is unchanged.

    The cards themselves are never rebuilt: this clears and refills the
    `-items` containers only, so the filter bar's structure (and the ruling-T
    bucket control living beside it) is built exactly once per page.

    `is_stale()` is the caller's REVISION TOKEN, checked after every await here
    as well. Three reads happen in this loop and each one is a chance for a
    newer refresh to have taken over; a superseded pass that kept filling
    containers would paint one request's facet counts beside another request's
    rows, which is the same defect the results region has and in the control a
    reader uses to trust the number.
    """
    def _stale() -> bool:
        # ONE expression, deliberately. Written as two branches it grew a
        # `return True` line that no capture can drive -- reaching it needs a
        # client deleted mid-cascade -- and a line that reads as coverage
        # nobody has is the defect this suite's line gate exists to find. The
        # short circuit is the same behaviour: a departed reader needs no
        # revision comparison.
        return _page_is_gone(page_client) or (
            bool(is_stale()) if is_stale is not None else False)

    await _prime_domain_labels(lang)
    if _stale():
        return
    containers = _facet_containers(filter_bar)
    for level in ("domain", "author", "work"):
        container = containers.get(level)
        if container is None:  # pragma: no cover -- structural
            continue
        key = _facet_cache_key(level, state)
        cached = cache.get(level) if cache is not None else None
        if cached is not None and cached[0] == key:
            envelope = cached[1]
        else:
            envelope = await fetch_facets(level, state)
            # ONLY AN `ok` ENVELOPE IS CACHED. A `timeout` or a `busy` is a
            # statement about the SERVICE at one instant, not an answer to the
            # question this key asks -- and the key is derived from the request,
            # so a cached failure is served for every later refresh whose
            # request is unchanged. The reader then sees "not available yet"
            # beside a working result set until some OTHER control happens to
            # move an input, and no retry they can reach clears it.
            #
            # Not caching a failure costs one re-read on the next refresh, on a
            # cascade that was already re-read whenever an input moved.
            #
            # CACHED BEFORE the staleness check, deliberately: `key` was
            # computed from the state this read was ISSUED against, so the entry
            # is a correct answer to a question that was really asked, and a
            # newer refresh taking over does not make it wrong. Discarding it
            # would cost the newer pass a re-read of a level it may not have
            # changed.
            if cache is not None and (envelope or {}).get("status") == "ok":
                cache[level] = (key, envelope)
            if _stale():
                return
        container.clear()
        with container:
            _render_facet_items(level, envelope, state, lang, refresh)


def facet_display_label(level: str, item: Dict[str, Any], lang: str) -> str:
    """The reader-facing label for ONE facet node -- and nothing else.

    The node's VALUE is never touched: it is the key the service filters and
    persists on, so it stays the stored English string in both languages. Only
    what a reader sees is language-dependent.

    * `work` -- ruling R's curated display title, never the raw recorded one.
    * `domain` -- the FJMS Hebrew name for a Hebrew reader, the stored English
      string otherwise, and the stored English string for any part the
      vocabulary cannot place (never a blank).
    * `author` -- the recorded name, which is not ours to translate.
    """
    value = item.get("value")
    raw_label = item.get("label") or value or ""
    if level == "work":
        # NEVER the stored key. A work's value is a `w`-prefixed id, so the
        # `or value` fallback above would print `w000404` as a title -- which
        # is what happened when a SELECTED work was not in the cascade's own
        # answer and nothing had persisted its label. A reader gets the shared
        # "title unavailable" wording instead, in the select and on the chip
        # alike, because both route through this one function.
        if not item.get("label") or item.get("label") == value:
            return missing_title(lang)
        return display_work_title(value, raw_label, lang) or missing_title(lang)
    if level == "domain":
        return genre_display_label(raw_label, lang)
    return raw_label


def _leaf_display_label(full_label: str) -> str:
    """A leaf's own name, without its parent's -- the parent is already the
    heading it sits under, and repeating it makes every child row long enough
    to truncate. The full path stays on the node's tooltip."""
    parts = full_label.split(GENRE_PART_SEPARATOR)
    tail = parts[-1].strip() if parts else ""
    return tail or full_label


def _node_text(label: str, count: Any) -> str:
    """`Label (1,234)` -- `/catalog-browse`'s own facet-row shape.

    The count is the facet envelope's own, so a number beside a domain always
    agrees with the result set that domain produces. (This is a FACET count,
    not a claim about anything: the ruling-T prohibition is on attaching a
    number to the BUCKET control, and that control still carries none.)
    """
    try:
        return "{} ({:,})".format(label, int(count))
    except (TypeError, ValueError):
        return label


#: The state key each facet level selects on -- ONE mapping, read by the domain
#: tree's node and by the author/work select alike, so the two controls cannot
#: come to disagree about which key an axis persists on. `work` selects on
#: `work_id` because its VALUE is a `w`-prefixed key rather than a title.
_FACET_STATE_KEY: Dict[str, str] = {"domain": "domain", "author": "author",
                                    "work": "work_id"}


def _facet_node(
    level: str, item: Dict[str, Any], state: Dict[str, Any], refresh,
    *, text: str, tooltip: Optional[str] = None, leaf: bool = False,
) -> Any:
    """One selectable facet node, with the classes the shared CSS block
    styles (`.dnode`, `.dnode.leaf`, `.here`).

    THE DOMAIN LEVEL ONLY, since 2026-08-05: author and work are searchable
    selects (`_render_facet_select`), and their `work_label` bookkeeping moved
    there with them. The `level` parameter stays because the state key it
    resolves is the shared mapping, not a domain-specific constant -- but a
    branch here for a level that no longer arrives would be dead code that
    reads as coverage.
    """
    value = item.get("value")
    state_key = _FACET_STATE_KEY[level]
    selected = state.get(state_key) == value

    async def _pick(_event=None, value=value, state_key=state_key) -> None:
        picked = None if state.get(state_key) == value else value
        state[state_key] = picked
        state["page"] = 1
        await refresh()

    node = ui.button(text, on_click=_pick).props("flat dense no-caps align=left")
    node.classes(
        " ".join(
            part for part in (
                "dnode", "w-full",
                "leaf" if leaf else "",
                "here" if selected else "",
            ) if part
        )
    )
    node.props(f'aria-pressed={"true" if selected else "false"}')
    if tooltip:
        node.tooltip(tooltip)
    return node


def _render_domain_tree(
    items: List[Dict[str, Any]], state: Dict[str, Any], lang: str, refresh
) -> None:
    """The domain facet as a two-level TREE with counts, not a flat stack.

    The service already emits both levels -- each parent as its own selectable
    node carrying the sum of its leaves, each leaf carrying its parent's key --
    so the tree is a grouping of what the envelope says, never a hierarchy this
    page invents.

    Collapse is a plain visibility toggle rather than a framework expansion
    container, for one reason worth stating: a `q-expansion-item` swallows the
    clicks of everything inside it, so a leaf click would bubble to the parent
    and select the parent instead. `/catalog-browse` hangs its domain-select
    handler on the expansion itself and has exactly that hazard. The chevron
    here is a SEPARATE control from the selectable parent node, and it is a
    VERTICAL one (`expand_more` / `expand_less`), so nothing about it needs to
    flip for RTL.
    """
    # A leaf is only nested when its parent node is ACTUALLY in the envelope.
    # The service builds a parent node for every leaf that names one, so this
    # cannot normally differ -- but an orphaned leaf silently disappearing
    # would delete a domain a reader can otherwise select, which is the one
    # failure a facet list must never have.
    present = {item.get("value") for item in items if not item.get("is_leaf")}
    children: Dict[str, List[Dict[str, Any]]] = {}
    for item in items:
        parent = item.get("parent")
        if item.get("is_leaf") and parent and parent in present:
            children.setdefault(parent, []).append(item)

    for item in items:
        if item.get("is_leaf") and item.get("parent") in present:
            continue  # rendered under its parent, below
        label = facet_display_label("domain", item, lang)
        kids = children.get(item.get("value")) or []
        if not kids:
            _facet_node("domain", item, state, refresh,
                        text=_node_text(label, item.get("count")),
                        leaf=bool(item.get("is_leaf")))
            continue

        # A parent WITH leaves: its own selectable node, a chevron beside it,
        # and its leaves in a container that starts collapsed -- unless
        # something inside it (or the parent itself) is the current selection,
        # in which case a reader must be able to see what is selected.
        selected_inside = state.get("domain") in (
            {item.get("value")} | {kid.get("value") for kid in kids}
        )
        with ui.column().classes(f"{FILTER_BAR_CLASS}-domain-branch w-full gap-1"):
            with ui.row().classes("w-full gap-1 items-center flex-nowrap"):
                _facet_node("domain", item, state, refresh,
                            text=_node_text(label, item.get("count")))
                toggle = ui.button().props("flat dense round size=sm")
                toggle.classes(f"{FILTER_BAR_CLASS}-domain-toggle shrink-0")
            kids_box = ui.column().classes(
                f"{FILTER_BAR_CLASS}-domain-children w-full gap-1"
            )
            with kids_box:
                for kid in kids:
                    kid_label = facet_display_label("domain", kid, lang)
                    _facet_node("domain", kid, state, refresh,
                                text=_node_text(_leaf_display_label(kid_label),
                                                kid.get("count")),
                                tooltip=kid_label, leaf=True)

        kids_box.set_visibility(selected_inside)
        toggle.props(f'icon={"expand_less" if selected_inside else "expand_more"}')

        def _toggle_kids(_event=None, box=kids_box, button=toggle) -> None:
            opened = not box.visible
            box.set_visibility(opened)
            button.props(f'icon={"expand_less" if opened else "expand_more"}')

        toggle.on("click", _toggle_kids)


def _render_facet_select(
    level: str, items: List[Dict[str, Any]], state: Dict[str, Any], lang: str, refresh
) -> Any:
    """The AUTHOR and WORK facets as `/catalog-browse`'s searchable select.

    47 authors and 478 works were 525 flat `ui.button` nodes in a 340px scroll
    box, with no search field: finding a specific work meant scrolling 478
    buttons. `/catalog-browse` -- the page this one was deliberately matched to
    -- already uses the right control for exactly these two facets, and this
    page took that page's CARD pattern without its CONTROL. The props here are
    that page's verbatim (`dense outlined clearable use-input
    input-debounce=300`); its physical Tailwind classes (`ml-1`, `text-left`,
    `pl-4`) deliberately are NOT copied, because they put the label and the
    caret on the wrong side in Hebrew.

    THE DOMAIN FACET KEEPS ITS TREE. It is two levels with counts and a
    collapse, and it reads as navigation rather than as a lookup.

    LABELS route through `facet_display_label` (ruling R): the work level's raw
    recorded title must never be printed, and this is the very control a reader
    uses to find a work. VALUES stay the stored key the service filters and
    persists on, so a work chosen in Hebrew narrows exactly what the same work
    chosen in English narrows.

    COUNTS ride on the option, from the facet envelope's own count -- the same
    `_node_text` shape the domain tree uses, so the number beside an option
    always agrees with the result set that option produces. They fit here: the
    old buttons wrapped in a 340px column, and a count after a work title was
    the first thing to truncate; an option line is the full card width.

    A SELECTION THE CASCADE NO LONGER OFFERS IS STILL SHOWN. The facets
    re-fetch on every refresh, so narrowing another axis can drop the selected
    option out of the returned set -- and a select whose value is absent from
    its own options renders BLANK while the query is still filtering on it.
    The current selection is therefore added back to the option map (labelled
    from the persisted raw label where there is one), so what the control shows
    and what the query does cannot disagree, and the reader can still clear it.
    """
    state_key = _FACET_STATE_KEY[level]
    selected = state.get(state_key)

    options: Dict[Any, str] = {}
    raw_labels: Dict[Any, Any] = {}
    for item in items:
        value = item.get("value")
        options[value] = _node_text(facet_display_label(level, item, lang),
                                    item.get("count"))
        raw_labels[value] = item.get("label")
    if selected is not None and selected not in options:
        # Labelled from what we persisted about it, never from the value: the
        # work level's value is a `w`-prefixed key and printing it would put a
        # stored identifier in a reader's control.
        options[selected] = facet_display_label(
            level, {"value": selected,
                    "label": state.get("work_label") if level == "work" else selected},
            lang)

    async def _pick(event) -> None:
        value = getattr(event, "value", None)
        # The `clearable` X emits None, and that is the axis being cleared --
        # the SAME state change the chip bar's own remove control makes, so the
        # two round-trip against each other.
        picked = value if value in options else None
        # GUARDED, and the guard is what stops a loop rather than a nicety:
        # every refresh REBUILDS this control with its current value, and a
        # rebuild that re-entered `refresh` would recurse. Written as a
        # condition around the body rather than as an early `return`, so the
        # no-op path adds no line of its own -- a line that paints nothing and
        # that no capture can drive reads as coverage nobody has.
        if picked != state.get(state_key):
            state[state_key] = picked
            if level == "work":
                state["work_label"] = (
                    raw_labels.get(picked) if picked is not None else None)
            state["page"] = 1
            await refresh()

    control = ui.select(
        options=options,
        value=selected if selected in options else None,
        with_input=True,
        on_change=_pick,
        label=copy_text(f"facet_search_{level}", lang),
    ).props("dense outlined clearable use-input input-debounce=300")
    control.classes(f"{FILTER_BAR_CLASS}-{level}-select w-full")
    return control


def _render_facet_items(
    level: str, envelope: Dict[str, Any], state: Dict[str, Any], lang: str, refresh
) -> None:
    if (envelope or {}).get("status") != "ok":
        # Backing data absent: visibly disabled and tagged, never absent.
        with ui.column().classes(f"fg blocked {FILTER_BAR_CLASS}-{level}-blocked gap-1"):
            ui.label(copy_text("needs_tag", lang)).classes("needs")
        return

    items = list(envelope.get("items") or [])
    if not items:
        # AN EMPTY LIST IS A FACT, and it has to look like one. The loops below
        # emit nothing at all for an empty `items`, which leaves a blank box
        # under a loud uppercase header -- indistinguishable from a card that
        # failed to load. QUIET, not amber: the `.needs` treatment beside it
        # means the backing data is missing and dims its whole card as
        # unusable, and this control is neither.
        ui.label(copy_text("facet_empty", lang)).classes(
            f"{FILTER_BAR_CLASS}-{level}-empty dnote text-xs"
        )
        return

    if level == "domain":
        _render_domain_tree(items, state, lang, refresh)
        return

    _render_facet_select(level, items, state, lang, refresh)


# ---------------------------------------------------------------------------
# Result bar, rows, pager, and the four service states.
# ---------------------------------------------------------------------------

def _render_results(
    envelope: Dict[str, Any], state: Dict[str, Any], lang: str, refresh,
    more_pool_total: Any = None,
) -> None:
    status = (envelope or {}).get("status")
    if status != "ok":
        _render_outage_state(status, lang, refresh)
        return

    items: List[Dict[str, Any]] = list(envelope.get("items") or [])
    total = int(envelope.get("total") or 0)
    meta = dict(envelope.get("meta") or {})

    _render_result_bar(items, total, meta, state, lang, refresh)
    _render_active_filters(state, lang, refresh)
    # ONE invitation on the page, in the place that serves the reader who is
    # actually there. With rows on screen it sits above them; with none, it
    # belongs inside the empty state, which is the moment a reader is most
    # likely to want the other pool. Rendering it in both places would put two
    # identical buttons a few pixels apart and make the control ambiguous to
    # locate, for a reader and for a test.
    if items:
        _render_pool_invite(state, lang, refresh, more_pool_total)

    with ui.column().classes(f"rows {RESULTS_CLASS}-rows w-full gap-2"):
        if not items:
            _render_empty_state(state, lang, refresh, more_pool_total)
        for item in items:
            _render_row(item, lang)

    _render_pager(total, state, lang, refresh,
                  page_size=effective_page_size(envelope),
                  approximate=bool(meta.get("approximate_total")))


def _render_empty_state(state: Dict[str, Any], lang: str, refresh,
                        more_pool_total: Any = None) -> None:
    """`ok` with ZERO rows -- the fourth state, and the only honest zero.

    It used to be four grey words in an otherwise empty column. An empty result
    after three filters is the highest-intent moment on this page for meeting
    the second pool -- the reader has just told you exactly what they were
    looking for and been told there is none of it -- so the invitation is
    rendered here rather than above a list that does not exist.

    DISTINCT FROM THE THREE OUTAGE STATES, structurally and in what it offers.
    `_render_outage_state` names a temporary condition and offers a RETRY;
    this names a real (zero) result and offers the OTHER POOL, because retrying
    a query that answered correctly is not a thing to suggest. An outage that
    reads as "this corpus has no findings" silently under-reports the corpus
    (T-136-16-04), and the converse misleads just as badly.

    Note that no invitation is rendered on an outage at all: the other pool is
    served by the same sidecar and is not answering either, so offering it there
    would present an outage as a fact about this bucket.
    """
    with ui.column().classes(
        f"{RESULTS_CLASS}-empty w-full items-center gap-2 p-4"
    ):
        ui.icon("search_off").classes(
            f"{RESULTS_CLASS}-empty-icon text-4xl"
        ).style("color: var(--text-secondary);")
        ui.label(tr("No results found")).classes(
            f"{RESULTS_CLASS}-empty-message text-lg font-bold"
        )
        _render_pool_invite(state, lang, refresh, more_pool_total)


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


def _render_divergence_basis(meta: Dict[str, Any], lang: str) -> None:
    """Say whether THIS count includes the catalogue-divergent rows.

    THE RECONCILIATION between the headline and the result bar, and the reason
    it lives here rather than in the headline: the headline reports what the
    RELEASE contains, on the single basis ruling U fixed, and a corpus figure
    that silently tracked the reader's filters would stop being a corpus figure
    at all. So the figure that moves is the one that explains itself.

    It reconciles the pool INVITATION on the same line and for the same reason:
    that strip names the second pool's full size from the artifact, and the
    default view of that pool is smaller for exactly this reason. One statement,
    rendered in both bucket states, covers both figures.

    READ FROM THE ENVELOPE, never from `state`. `meta['include_divergent']` is
    the SERVICE's own report of the query it actually ran; the page's state is
    only what the page intended. If those two ever disagree -- a control that
    failed to persist, a request that lost an argument in a wrapper -- this line
    follows the ROWS, which is the half a reader is counting.

    A `meta` with no such key states NOTHING. An envelope that does not say what
    it did is not evidence of either answer, and asserting the default here
    would be this module claiming to know something it was not told. The shipped
    reader always supplies it, and a test pins that.
    """
    if "include_divergent" not in meta:
        return
    key = "divergence_included" if meta.get("include_divergent") else "divergence_excluded"
    ui.label(copy_text(key, lang)).classes(
        f"{RESULT_BAR_CLASS}-divergence dnote text-xs")


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

            _render_divergence_basis(meta, lang)

            if meta.get("approximate_total"):
                ui.label(copy_text("approximate_note", lang)).classes(
                    f"{RESULT_BAR_CLASS}-approx dnote text-xs"
                )

        with ui.row().classes("w-full gap-3 items-center flex-wrap"):
            _render_unit_select(state, refresh)
            _render_sort_select(state, refresh)


#: The axes an active-filter chip may represent, in display order, each named
#: by the state key it clears.
#:
#: THE POOL IS NOT HERE, and adding it would be a design error rather than a
#: feature: a removable chip promises a neutral "no pool" state to return to,
#: and `_OFFERED_BUCKETS` offers exactly two buckets with no union between them
#: (ruling U constraint 1 -- a control that silently unioned the two would
#: produce a figure the page could not name). The pool is switched on its own
#: always-present control, never dismissed.
#:
#: `unit` and `sort` are absent for a different reason: both already have a live
#: control in the result bar showing their current value, so a chip would be a
#: second display of the same state.
#:
#: `divergence` IS here, and it is here for the reason the pool is not: this
#: axis HAS a neutral state to return to, and it is the ratified default. Off
#: means "the corpus as ruling F says it renders by default"; on is a
#: deliberate opt-in the reader made and can therefore undo the same way they
#: undo a domain. Leaving it off the bar would make it the one selection that
#: changes ~23.6% of the corpus without appearing anywhere in "why am I looking
#: at these rows".
_CHIP_AXES: Tuple[str, ...] = (
    "novelty_only", "divergence", "domain", "author", "work_id")

#: The chip axes whose neutral value is `False` rather than `None`.
_BOOLEAN_CHIP_AXES: Tuple[str, ...] = ("novelty_only", "divergence")


def _clear_filter_axis(state: Dict[str, Any], axis: Optional[str]) -> None:
    """Clear ONE chip axis, or -- with `axis=None` -- every one of them.

    PURE: no UI, no read, no refresh, so the rule is assertable without a
    browser. Any filter change returns to page 1, because page 4 of the old set
    is not page 4 of the new one and a reader landing past the end of a shorter
    set reads it as "no results".

    `bucket`, `unit` and `sort` survive a clear-all: none of the three has a
    neutral value to return to, and inventing one for the bucket is the exact
    thing the chip bar refuses to imply.
    """
    for key in _CHIP_AXES:
        if axis is not None and key != axis:
            continue
        state[key] = False if key in _BOOLEAN_CHIP_AXES else None
        if key == "work_id":
            state["work_label"] = None
    state["page"] = 1


def _active_filter_chips(state: Dict[str, Any], lang: str) -> List[Tuple[str, str]]:
    """`(axis, reader label)` for every active selection, in display order.

    Every label comes from vocabulary that already exists -- the candidacy
    switch's own ratified name, and `facet_display_label`, which is the same
    function the facet lists route through -- so this bar introduces no second
    name for anything and no claim vocabulary of its own. In particular the work
    chip goes through ruling R's curation exactly as the work facet does.

    The candidacy chip is gated on the SERVICE's own axis predicate as well as
    on the flag: on a unit that does not offer the axis the flag is already
    cleared by `normalise_state`, and a chip for a filter the query is not
    applying would be the same lie in a smaller box.
    """
    chips: List[Tuple[str, str]] = []
    if state.get("novelty_only") and findings_novelty_offered(state.get("unit")):
        chips.append(("novelty_only", novelty_strings(lang)["toggle"]))
    if state.get("divergence"):
        # The RATIFIED toggle wording, the same string the sidebar control
        # carries. A shorter chip label would be a second name for the axis,
        # and this is the axis where a second name is most expensive: every
        # abbreviation of "findings that disagree with the catalogue" that fits
        # a chip loses either the disagreement or the fact that it is the
        # catalogue that disagrees.
        chips.append(("divergence", disclosure_toggle(TOGGLE_DIVERGENCE, lang)))
    for level, axis_key in (("domain", "Domain"), ("author", "Author")):
        value = state.get(level)
        if value:
            chips.append((level, "{}: {}".format(
                tr(axis_key),
                facet_display_label(level, {"value": value, "label": value}, lang))))
    work_id = state.get("work_id")
    if work_id:
        chips.append(("work_id", "{}: {}".format(
            tr("Work"),
            facet_display_label(
                "work", {"value": work_id, "label": state.get("work_label")}, lang))))
    return chips


def _render_active_filters(state: Dict[str, Any], lang: str, refresh) -> None:
    """The reader's own selections, beside the rows they produced.

    There was no "you are here" on this page at all. Picking a domain changed a
    row count and set one `.here` class in a 340px scroll box up to 800px away,
    possibly scrolled out of view -- so a reader could not tell what was applied,
    and had to hunt for the same node to undo it. `/catalog-browse` solves this
    with a chip bar and a clear-all; this page took that page's CARD pattern and
    not its STATE pattern, and state is the half that lets a reader trust what
    they are looking at.

    Rendered between the result bar and the rows, so the answer to "why am I
    looking at these rows" sits with the rows.

    THE SHAPE is `/catalog-browse`'s; the CLASSES deliberately are not. That
    page's `_make_chip` uses `ml-1` and `text-left`, which put the close button
    and the label on the wrong side in Hebrew. Everything here is either a
    logical property or side-neutral, and all of it is inline -- this work adds
    no stylesheet rule.
    """
    chips = _active_filter_chips(state, lang)
    if not chips:
        return

    async def _clear(axis: Optional[str]) -> None:
        _clear_filter_axis(state, axis)
        await refresh()

    with ui.row().classes(
        f"{ACTIVE_FILTERS_CLASS} w-full items-center gap-2 flex-wrap"
    ):
        for axis, label in chips:
            with ui.row().classes(
                f"{ACTIVE_FILTERS_CLASS}-chip items-center flex-nowrap"
            ).style(
                "gap: 4px; border: 1px solid var(--border-light); "
                "border-radius: 999px; background: var(--bg-secondary); "
                "padding-block: 2px; padding-inline: 10px;"
            ):
                ui.label(label).classes("text-xs")
                remove = ui.button(
                    icon="close", on_click=lambda _event=None, axis=axis: _clear(axis)
                ).props("flat round dense size=xs")
                remove.classes(f"{ACTIVE_FILTERS_CLASS}-remove")
                # An icon-only control needs a name a screen reader can read.
                remove.props(f'aria-label="{tr("Remove")}"')

        clear = ui.button(tr("Clear All"), on_click=lambda _event=None: _clear(None))
        clear.props("flat dense no-caps size=sm color=red")
        clear.classes(f"{ACTIVE_FILTERS_CLASS}-clear")


def _pool_invite_body(more_pool_total: Any, lang: str) -> str:
    """The invitation's main-pool sentence, WITH the second pool's size when the
    artifact supplied one and without it when it did not.

    The degradation is the whole reason this is a function. `int(None)` raises,
    `str(None)` prints "None", and `int(x) or 0` prints a zero -- three ways for
    a missing figure to become a visible wrong one. A value that is not a
    POSITIVE INTEGER is treated as absent and the digit-free sentence is
    returned unchanged: a size of zero is not a thing to advertise either, and
    on this artifact it would mean the read is wrong rather than that the pool
    is empty.
    """
    try:
        count = int(more_pool_total)
    except (TypeError, ValueError):
        count = 0
    if count <= 0:
        return copy_text("pool_invite_body", lang).format(
            bucket=bucket_name(False, lang))
    return copy_text("pool_invite_body_counted", lang).format(
        count=f"{count:,}", bucket=bucket_name(False, lang))


def _render_pool_invite(state: Dict[str, Any], lang: str, refresh,
                        more_pool_total: Any = None) -> None:
    """The second pool, introduced WHERE THE READER IS LOOKING.

    A body of identifications comparable in size to the one on display was
    reachable only by noticing that one of two unlabelled pills in a header-less
    box was not selected. Every part of that is a COMMUNICATION failure and none
    of it is a control failure: the sidebar control works, is one interaction,
    and switches the whole result set. What it never did was say that a second
    pool exists, what is in it, or why anyone would look.

    So this is a SECOND live entry point, not a move and not a demotion. Ruling
    T's control stays exactly where it is, first-class in the filter bar, and
    every ancestry and no-count assertion over it still runs. The one placement
    ruling T names -- never below the results -- is honoured here too: this
    strip is rendered between the result bar and the rows column, never after
    the rows.

    CREATION ORDER IS LOAD-BEARING. `_find_bucket_control` in the suite resolves
    the FIRST element whose accessible name is the second bucket's, and the
    ruling-T ancestry assertions are made about whatever it returns. The sidebar
    card is built in `_render_filter_bar` BEFORE the first `refresh()` paints
    this strip, so it wins. Reordering those two would silently re-point those
    assertions at an element in the results region -- where `RESULTS_CLASS` in
    the ancestor set is exactly what one of them fails on.

    THE SECOND POOL'S SIZE (owner ruling, 2026-08-05). It carried no number
    anywhere and this design deliberately did not need one; the owner has now
    ruled that the size may be shown, and this strip -- where a reader decides
    whether to go and look -- is where it goes. THREE things that ruling does
    NOT move, each with a test:

    * the BUCKET CONTROL stays digit-free (ruling T). The figure lives here and
      never there;
    * it is a SIZE, not a quality figure. The prohibition on the owner's
      assessment of that pool becoming a percentage, a rate, an interval or a
      score is untouched and absolute; a count of what is IN a pool is a
      different kind of fact from a judgement about it;
    * the FRAMING is unchanged. The second pool means the evidence did not meet
      the main-pool rule -- never "probably wrong", never "leftovers", and never
      "findings you are missing".

    `more_pool_total` arrives from the artifact through `meta.more_pool_total`
    (ruling U: read at request time, never a literal). When it is ABSENT -- an
    older sidecar, a launch read that failed, the render-smoke sentinel envelope
    -- this renders exactly the digit-free sentence it always did. A missing
    figure must never print as `0`, as `None`, or as a gap in a sentence.

    The SECOND-BUCKET state carries no figure at all: a reader already inside
    that pool is looking at its own result bar, which states the total on the
    same basis as the rows beside it.
    """
    in_main = state["bucket"] == BUCKET_MAIN
    if in_main:
        heading = copy_text("pool_invite_heading", lang)
        body = _pool_invite_body(more_pool_total, lang)
        action = disclosure_toggle(TOGGLE_MORE_MATCHES, lang)
        target = BUCKET_MORE
    else:
        heading = copy_text("pool_here_heading", lang).format(
            bucket=bucket_name(False, lang))
        body = copy_text("pool_here_body", lang).format(
            main_bucket=bucket_name(True, lang))
        action = bucket_name(True, lang)
        target = BUCKET_MAIN

    async def _switch(_event=None) -> None:
        state["bucket"] = target
        state["page"] = 1
        await refresh()

    # The same bounded, tinted treatment as the launch headline, so the two read
    # as one family. Inline and LOGICAL (`border-inline-start`): the rule sits
    # on the leading edge in both directions, and this work adds no stylesheet.
    with ui.column().classes(f"{POOL_INVITE_CLASS} w-full gap-1 p-3").style(
        "border-inline-start: 3px solid var(--primary-600); "
        "background: var(--bg-secondary); border-radius: 6px;"
    ):
        ui.label(heading).classes(f"{POOL_INVITE_CLASS}-heading text-sm font-bold")
        ui.label(body).classes(f"{POOL_INVITE_CLASS}-body dnote text-xs")
        button = ui.button(action, on_click=_switch).props("flat dense no-caps")
        button.classes(f"{POOL_INVITE_CLASS}-action fchip self-start")


def _render_unit_select(state: Dict[str, Any], refresh) -> None:
    """The row unit is a READER choice, not a design pick. The option set is the
    exported closed vocabulary itself, so a unit the service gains cannot be
    silently withheld and a unit it loses cannot be silently offered.

    EVERY unit stays offered, including while the candidacy switch is on. The
    two axes are not independent -- the service does not offer novelty on the
    per-work unit -- and the settlement is that the UNIT wins and the switch
    turns visibly off, never that a row unit disappears from this list. A reader
    reaching for a different row shape must not have to guess which other
    control is withholding it; `_render_novelty_switch` says what happened, in
    words, on the control that changed.
    """
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
    """One result row, in whichever of the three shipped units the service
    produced it.

    Delegated to `web/components/findings_rows.py` (plan 136-18), which owns the
    anatomy, the novelty badge, the coverage clause and the bucket name. The
    same component renders BOTH buckets: a second-bucket row is a first-class
    result, not a footnote, and giving it its own renderer here is how a
    demotion creeps in.
    """
    rows.render_finding_row(item, lang)


def _render_pager(total: int, state: Dict[str, Any], lang: str, refresh,
                  *, page_size: int, approximate: bool = False) -> None:
    """Pagination over the FULL filtered set.

    The service supplies a real pre-`LIMIT` total, so the page count is derived
    from that and never from the length of the current page.

    THE PAGE SIZE IS THE SERVICE'S, not this module's request (§3.5). The
    ceiling is enforced server-side, so with
    `DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT` above `DISCOVERY_PAGE_SIZE_MAX` the
    two differ -- and dividing a real total by the size the service refused
    reports too few pages and leaves the tail of the set unreachable.

    A CAPPED TOTAL IS A LOWER BOUND, and the pager says so rather than acting as
    though the reader has seen everything (§3.5). With
    `DISCOVERY_FINDINGS_COUNT_MAX` set, `total` stops at the cap, so the last
    page the arithmetic can name is not the last page that exists: `Next` stays
    ENABLED there and a note states that more pages may follow. A silently
    terminal pager reads as "that is all there is", which is the one thing a
    capped count cannot support.
    """
    # REQUIRED, and taken as given. `effective_page_size` already applied the
    # fallback for an envelope that does not report its size, so a second one
    # here would be a branch nothing can drive -- and an unreachable defensive
    # branch reads as coverage nobody has, which is what this plan's own line
    # gate exists to find.
    pages = _page_count(total, page_size)
    page = min(max(1, state["page"]), pages) if not approximate \
        else max(1, state["page"])

    with ui.row().classes(f"pager {PAGER_CLASS} w-full gap-2 items-center"):
        async def _go(delta: int) -> None:
            ceiling = None if approximate else pages
            target = state["page"] + delta
            state["page"] = max(1, target if ceiling is None
                                else min(ceiling, target))
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
        if page >= pages and not approximate:
            following.disable()

        if approximate:
            ui.label(copy_text("pager_capped_note", lang)).classes(
                f"{PAGER_CLASS}-capped dnote text-xs")
