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
from typing import Any, Dict, List, Mapping, Optional, Tuple

from nicegui import ui

from shared.discovery_display_strings import (
    TOGGLE_MORE_MATCHES,
    bucket_name,
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
from web.components import discovery_links
from web.components import findings_rows as rows
from web.components.typography import h1
from web.discovery import (
    BUCKET_MAIN,
    DIVERGENCE_HIDDEN,
    DIVERGENCE_MODES,
    DIVERGENCE_ONLY,
    DIVERGENCE_SHOWN,
    FACET_LEVELS,
    FINDINGS_BUCKETS,
    FINDINGS_SORT_BAND_RANK,
    FINDINGS_SORTS,
    FINDINGS_UNIT_IDENTIFICATION,
    FINDINGS_UNITS,
    NOVELTY_VIEW_ALL,
    NOVELTY_VIEW_CANDIDATES,
    NOVELTY_VIEW_DIVERGENT,
    NOVELTY_VIEW_EITHER,
    NOVELTY_VIEWS,
    cached_suppressed_identification_ids,
    excerpts_available,
    get_excerpt_enveloped,
    get_findings_enveloped,
    get_findings_facets_enveloped,
    get_launch_stats_enveloped,
    novelty_view_shades,
    suppress_identification,
    suppressed_identification_ids,
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

#: "Not in the cache", distinct from a cached `None` ("looked up; there is no
#: title"). A sentinel rather than `None` because ~14% of manuscripts have no title
#: in `libraries.csv`, and conflating the two would re-look-up every one of them on
#: every render pass. See `_render_results`'s `_catalogue_title`.
_MISSING = object()

_STORAGE_PREFIX = "discovery_findings_"

_KEY_UNIT = _STORAGE_PREFIX + "unit"
_KEY_BUCKET = _STORAGE_PREFIX + "bucket"
_KEY_SORT = _STORAGE_PREFIX + "sort"
#: THE FOUR-STATE SELECTOR's persisted value (owner ruling, 2026-08-06). A NEW
#: key, deliberately, rather than a reinterpretation of either old one: a reader
#: returning with `novelty_only=True` and `divergence=hidden` in storage would
#: otherwise have those read as some third thing, and a silent reinterpretation
#: of a persisted choice is a choice made for them. An absent key means the
#: default (`all`), which is what a first visit gets too -- so the migration is
#: that a returning reader's next visit starts unfiltered, once, visibly.
_KEY_NOVELTY_VIEW = _STORAGE_PREFIX + "novelty_view"
#: The two keys the selector REPLACES. Read no longer; named here because
#: `web/safe_storage.py` is the chokepoint for per-user state and a key that
#: stops being read should be recorded as retired rather than simply vanishing
#: from the file -- a future reader finding stale values in a storage dump needs
#: to know what wrote them.
_RETIRED_KEY_NOVELTY = _STORAGE_PREFIX + "novelty_only"
_RETIRED_KEY_DIVERGENCE = _STORAGE_PREFIX + "divergence"
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
    # TWO SENTENCES ADDED 2026-08-07, at the owner's request, rephrased from
    # their draft ("Matches may point to a work with shared text. Candidates for
    # new finds and catalog non-correspondences were LLM-generated so there might
    # be inaccuracies").
    #
    # WHY THEY BELONG HERE and not in `_render_howto`: both name a limit of the
    # DATA ITSELF rather than explaining how to read the page, and the caveat is
    # the one block every reader meets before any row. The first sentence bounds
    # what a match means; the second attributes the two candidacy axes to their
    # actual producer. Together they close the gap the original caveat left -- it
    # said the rows are unreviewed, but not that the SHADES are model-generated.
    #
    # THE WORDING IS CONSTRAINED IN THREE WAYS, none of them stylistic:
    #
    # 1. "shares wording", not "a copy of" / "quotes" / "a witness of" -- all
    #    three are in the honesty gate's prohibited-relation list (D-21), and
    #    "shared text" is the relation kind's OWN neutral label.
    # 2. "does not correspond", never "disagree" (ruling F). The catalogue is not
    #    adjudicated here and neither is our match; they either correspond or
    #    they do not.
    # 3. NO RATE WORD AND NO QUANTITY. The owner's "inaccuracies" sits one
    #    morpheme from `accuracy`, which is a rate word in the gate's lexicon --
    #    and the honest thing to say is not how OFTEN a shade is wrong (we have
    #    no reader-facing number and are prohibited from publishing one) but THAT
    #    a shade can be. "some will be mistaken" says exactly that and reaches
    #    for no figure.
    #
    # NO INTERNAL VOCABULARY IN THE PROSE. An earlier revision said "the two
    # candidacy readings" / "שני ממדי המועמדות" -- owner, 2026-08-07: it "sounds
    # not good", and they were right for a reason worth recording. "Candidacy" and
    # "axis" are this project's OWN words for the model: a reader meeting them in
    # the caveat, before any row, has nothing to attach them to. The sentence now
    # names the two markings by WHAT THEY SAY ("whether a row looks new, and
    # whether it does not correspond to the catalogues") instead of by the internal
    # term for the pair. The Hebrew drops the count as well, since "הסימון" covers
    # both without asking the reader to hold a number.
    "caveat": {
        "en": (
            "Every row here is a text match found by software, not a reviewed "
            "identification. A text match is not by itself proof of identity — "
            "read each row as a lead to check, never as a conclusion. A match may "
            "point to a work that shares wording rather than to the work itself. "
            "The marking of whether an identification looks new, or does not "
            "correspond to the catalogues, comes from a language model, so some of "
            "it will be mistaken."
        ),
        "he": (
            "כל שורה כאן היא התאמת טקסט שנמצאה על ידי תוכנה, ולא זיהוי שנבדק. "
            "התאמת טקסט אינה כשלעצמה הוכחה לזהות — יש לקרוא כל שורה ככיוון "
            "לבדיקה, ולעולם לא כמסקנה. התאמה עשויה להצביע על חיבור שיש בו נוסח "
            "משותף, ולא על החיבור עצמו. כמו כן, הסימון אם זיהוי נראה חדש או אינו "
            "מתאים לקטלוגים נעשה על ידי מודל שפה, ולכן ייתכנו בו טעויות."
        ),
    },
    # H1 -- THE BETA NOTE (owner-approved wording, applied verbatim).
    #
    # Three constraints, all owner rulings, and each one is why this reads the
    # way it does rather than the obvious way:
    #
    # 1. NO ENUMERATED ROADMAP. The general form was asked for explicitly; an
    #    "evidence view / work pages / catalogue integration" list is not to be
    #    added here later either.
    # 2. NEVER "better identifications". On this surface that reads as MORE
    #    ACCURATE, which is a precision claim -- prohibited even as a
    #    forward-looking promise. Improvement attaches to the METHOD, never to
    #    the results. "More identifications, and more ways to work with them"
    #    is the approved form.
    # 3. THE PERMANENT CAVEAT IS UNTOUCHED. This does not replace it, and the
    #    head must not grow back into the wall of prose `_render_howto` exists
    #    to hold -- so the head line is SHORT and the fuller one lives there.
    "beta_head": {
        "en": (
            "This is a beta and it will grow — more identifications, and more "
            "ways to work with them."
        ),
        "he": (
            "זוהי גרסת בטא והיא תתרחב בהמשך — זיהויים נוספים ודרכים נוספות "
            "לעבוד איתם."
        ),
    },
    "beta_howto": {
        "en": (
            "This is a beta. Every row is a text match found by software — a "
            "lead to check, not a settled identification. It will grow: more "
            "identifications, and more ways to explore and judge them."
        ),
        "he": (
            "זוהי גרסת בטא. כל שורה היא התאמת טקסט שנמצאה על ידי תוכנה — כיוון "
            "לבדיקה, לא זיהוי מוכרע. המאגר יתרחב בהמשך: זיהויים נוספים ודרכים "
            "נוספות לעיין בהם ולשפוט אותם."
        ),
    },
    # The reserved launch-headline region's accessible label. Ruling U's framing
    # ("what the release adds to the existing finding aids") with NO number:
    # the figures are artifact-backed and version-dependent, they are supplied
    # by plan 136-22 and rendered by plan 136-18, and a placeholder digit here
    # would survive as a hardcoded launch number -- precisely the failure
    # ruling U was issued to prevent.
    #
    # "THE CATALOGUES WE CHECKED", not "the finding aids" (owner ruling,
    # 2026-08-06). This is the region's SCREEN-READER label, so it was the one
    # place the retired jargon would have survived the visible-copy sweep -- and
    # a blind reader meeting "finding aids" while every visible card says
    # "catalogues" gets a different vocabulary for the same fact. Same scope
    # correction as `launch_total`: what is checked is the set we check, not every
    # catalogue that exists.
    "headline_slot_label": {
        "en": "What this release adds to the catalogues we checked",
        "he": "מה מוסיפה מהדורה זו לקטלוגים שבדקנו",
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
    #
    # "DO NOT CORRESPOND", never "disagree" (owner ruling, 2026-08-06 -- the
    # same vocabulary as the selector and as `shared.discovery_display_strings`'
    # warning and row chip). These three lines had been left on the older
    # wording after the selector was renamed, so a reader filtered on "do not
    # correspond" and read a bar that said "disagree" about the same set: one
    # screen, one fact, two vocabularies (found by external review).
    "divergence_excluded": {
        "en": "Not counted here: findings that do not correspond to the catalogue.",
        "he": "לא נכללים כאן: ממצאים שאינם מתאימים לקטלוג.",
    },
    "divergence_included": {
        "en": "Counted here: findings that do not correspond to the catalogue.",
        "he": "נכללים כאן: ממצאים שאינם מתאימים לקטלוג.",
    },
    "divergence_alone": {
        "en": "Counted here: only findings that do not correspond to the catalogue.",
        "he": "נכללים כאן: רק ממצאים שאינם מתאימים לקטלוג.",
    },
    # THE THREE STATE LABELS (owner, 2026-08-05). Each one names WHAT THIS STATE
    # DOES, which is the whole correction: the control shipped with the panel's
    # ratified `TOGGLE_DIVERGENCE` wording -- "Show findings that disagree with
    # the catalogue" -- on a mechanic that is purely ADDITIVE here. On the panel
    # that string labels a disclosure that opens onto a divergent-ONLY section
    # and is exactly accurate; on this page it widened 24,480 main-pool rows to
    # 27,709 and kept every non-divergent one, so the reader who asked to see
    # the disagreements got a mixed list in which they were 12% of the rows and
    # the top of the page did not change at all.
    #
    # The panel keeps the ratified string. This page names its states.
    #
    # None of the three asserts which side is right: they say only whether the
    # disagreements are out, in, or the whole of what is counted. The ratified
    # warning still sits beside them as card prose.
    # NO `divergence_state_*` STRINGS. They labelled the three positions of the
    # cycling divergence CONTROL, which the four-state selector replaced; their
    # table (`_DIVERGENCE_STATE_KEY`) was validated but never indexed again, so
    # the strings were unreachable. Deleted rather than retranslated: keeping
    # them meant maintaining a THIRD vocabulary ("Catalogue disagreements" /
    # `מחלוקות`) for a fact the selector and the row chip already state, purely
    # to keep a dead table populated. The result bar's basis line uses
    # `divergence_excluded` / `_included` / `_alone` above, which are live.
    # -- THE FOUR-STATE SELECTOR (owner ruling + owner-authored Hebrew,
    #    2026-08-06). ONE control replacing two, because `novelty_status` is ONE
    #    column and the two chips were fighting over it.
    #
    # WHY "CORRESPOND" AND NOT "DISAGREE". The owner's Hebrew is
    # `חוסר התאמה עם הקטלוג` -- literally "lack of correspondence with the
    # catalogue" -- and that is a better description than `מחלוקות`
    # ("disputes"), in a way that matters here rather than being a matter of
    # taste: a dispute has parties and implies one of them is wrong, and ruling
    # F's whole position is that NEITHER side is adjudicated. So the English
    # follows the Hebrew rather than the reverse.
    #
    # THIS IS NOW THE WHOLE PAGE'S VOCABULARY, not just this control's (owner
    # ruling, 2026-08-06). It was applied to the selector first and the older
    # "disagree"/`חולק`/`מחלוקות` wording was left standing on the result bar's
    # basis lines, the shared warning and the row chip -- so one screen carried
    # two vocabularies for one fact, and a reader who filtered on "do not
    # correspond" read rows labelled "Disagrees". External review caught it.
    # `shared/discovery_display_strings.py` was changed in the same pass, so the
    # connections panel says the same thing.
    #
    # THE SELECTOR'S OWN LABEL. "Which findings", never a bare "Show":
    # the result bar carries a "Show as" control (the row unit) a short scroll
    # away, and in ENGLISH "Show" is a strict prefix of "Show as" -- two
    # adjacent filter labels where one contains the other. A reader scanning
    # for the control that changes what is listed has to disambiguate them by
    # position, and a simulated reader could not disambiguate them at all: the
    # test driving this sequence matched BOTH selects and clicked the wrong
    # one, which is how the collision was found. It reproduced in English only,
    # because the Hebrew pair (`הצגה` / `הצגה כ־`) does not collide the same way
    # -- so the fix names the AXIS in both languages rather than patching the
    # English string alone and leaving the two labels differently shaped.
    "novelty_view_label": {
        "en": "Which findings",
        "he": "אילו ממצאים",
    },
    "novelty_view_all": {
        "en": "All findings",
        "he": "כל הממצאים",
    },
    # State 2 reuses the RATIFIED candidacy name (`novelty_strings()['toggle']`)
    # rather than a second name for the same axis -- substituted at render time,
    # never retyped, so the two cannot drift.
    "novelty_view_divergent": {
        "en": "Do not correspond to the catalogue",
        "he": "חוסר התאמה עם הקטלוג",
    },
    "novelty_view_either": {
        "en": "Candidates or non-correspondence",
        "he": "מועמדים או חוסר התאמה",
    },
    # G4. Both controls select on ONE column, and `fills_gap` and the two
    # divergence shades are mutually exclusive values of it -- so with the
    # candidacy filter on, this axis cannot change anything. Saying so is the
    # same treatment `novelty_unit_note` already gives the other inert pair.
    # `{candidates}` is the candidacy control's own ratified name, substituted
    # rather than retyped.
    "divergence_candidacy_note": {
        "en": (
            "Not offered while '{candidates}' is on: a candidate is a finding "
            "no aid records, so it cannot also disagree with one."
        ),
        "he": (
            "לא מוצע כאשר '{candidates}' מסומן: מועמד הוא ממצא ששום כלי עזר "
            "אינו מתעד, ולכן אינו יכול גם לחלוק על כלי עזר."
        ),
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
    # Why the candidacy switch is inert while the row unit is a work -- and,
    # crucially, WHAT STILL WORKS THERE.
    #
    # Owner, 2026-08-05: "in each work we can show only those mss we want, i.e.
    # only divergent". He was right, and the previous wording was over-broad in a
    # way that mattered. It said only "Not offered while each row is a work",
    # sitting on a card that holds TWO controls -- so it read as if the whole
    # card were dead, when in fact only ONE axis is withdrawn.
    #
    # Measured against the served artifact: `divergence=only` on the per-work
    # unit builds a real predicate (`novelty_status IN (...)` before the GROUP
    # BY), 302 of 478 main-pool works carry at least one divergent
    # identification, and 275 of them carry BOTH kinds -- so on the majority of
    # works, narrowing to the disagreements genuinely changes what the reader
    # sees. That is not a hypothetical the note was entitled to write off.
    #
    # Only CANDIDACY is inert, and for a reason specific to it: it is a verdict
    # about one work on one fragment, and `novelty_status` is NULL on a mixed
    # group by construction, so there is nothing for it to test. The divergence
    # flag is `MAX(...)` over the group, which is exactly why it survives
    # grouping. The note now says both halves.
    "novelty_unit_note": {
        "en": (
            "Candidacy asks about one work on one fragment, so it is not "
            "offered while each row is a work. The catalogue-disagreement "
            "filter beside it still applies."
        ),
        "he": (
            "מועמדות נוגעת לחיבור אחד בקטע אחד, ולכן היא אינה מוצעת כאשר כל "
            "שורה היא חיבור. הסינון של מחלוקות עם הקטלוג שלצידה עדיין פעיל."
        ),
    },
}


#: Divergence mode -> the result bar's basis line.
#:
#: THERE IS NO LONGER A `_DIVERGENCE_STATE_KEY` (deleted 2026-08-06). It mapped
#: each mode to a chip label for the cycling divergence CONTROL, and that control
#: no longer exists -- the four-state selector replaced it. The table was never
#: indexed again, only validated, so it and its three
#: `divergence_state_*` strings were dead weight that a copy-sweep test still
#: dutifully swept. Keeping them would have meant maintaining a third
#: vocabulary for the same fact (they said "Catalogue disagreements" /
#: `מחלוקות`) purely so an unreachable table stayed populated.
_DIVERGENCE_BASIS_KEY: Dict[str, str] = {
    DIVERGENCE_HIDDEN: "divergence_excluded",
    DIVERGENCE_SHOWN: "divergence_included",
    DIVERGENCE_ONLY: "divergence_alone",
}

if set(_DIVERGENCE_BASIS_KEY) != set(DIVERGENCE_MODES):
    raise RuntimeError(
        "web/pages/findings.py: the divergence mode vocabulary moved and this "
        "module's basis table did not -- a mode with no basis line leaves the "
        "count unexplained"
    )


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


def _stored_novelty_view() -> str:
    """The reader's selector view, validated against the closed vocabulary.

    FAILS OPEN to `all`. Every other axis on this page defaults to its narrowest
    honest state, but this one defaults to the WIDEST: an unrecognised stored
    value must never resolve to a filtered view, because a reader would then be
    shown a subset they did not choose, with the control displaying a state they
    cannot connect to what they are seeing. Showing everything is always
    explicable.
    """
    stored = safe_user_get(_KEY_NOVELTY_VIEW, NOVELTY_VIEW_ALL)
    if isinstance(stored, str) and stored in NOVELTY_VIEWS:
        return stored
    return NOVELTY_VIEW_ALL


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
        "novelty_view": _stored_novelty_view(),
        "domain": _opt(_KEY_DOMAIN),
        "author": _opt(_KEY_AUTHOR),
        "work_id": _opt(_KEY_WORK),
        "work_label": _opt(_KEY_WORK_LABEL),
        "page": page if page >= 1 else 1,
    }
    return normalise_state(state)


def normalise_state(state: Dict[str, Any]) -> Dict[str, Any]:
    """Settle the axes that are not independent, IN PLACE, and return `state`.

    THERE ARE NONE LEFT (owner ruling, 2026-08-06), and this function is kept as
    a no-op with its history rather than deleted, because the history is the
    argument for why nothing here should grow back.

    It settled TWO pairs, and both were the same defect: a control that stayed
    live while something else made it meaningless.

    * The candidacy switch and the ROW UNIT. `_build_findings_filter` refused
      novelty on the per-work unit, so a reader who turned the switch on and then
      chose "one row per work" drove the shipped builder into an unhandled
      `ValueError` and the results simply stopped updating (round 15, finding 1).
      GONE because the builder no longer refuses: the filter asks "does this row
      have such an identification under it", which every unit can answer.
    * The candidacy switch and the DIVERGENCE control. Both selected on
      `novelty_status`, and `fills_gap` and the two divergence shades are
      mutually exclusive values of one column -- so with candidacy on, "included"
      added rows candidacy had already excluded and "only" intersected two
      disjoint sets and returned nothing. GONE because the two controls are now
      ONE four-state selector, and a single-select cannot express an incoherent
      combination at all.

    That is the general lesson worth keeping: BOTH pairs existed because two
    controls shared one column. Normalisation papered over it; collapsing the
    controls removed it. If a future axis needs settling here, check first
    whether it is really a second control over an existing column -- the fix is
    probably to merge the controls, not to add a rule.

    Still CALLED from `read_state` and from the one refresh path, so a future
    coupling has a home that is already wired rather than needing to be
    reintroduced at both sites under time pressure.
    """
    return state


def write_state(state: Dict[str, Any]) -> None:
    """Persist the reader's selections through the storage chokepoint."""
    safe_user_set(_KEY_UNIT, state["unit"])
    safe_user_set(_KEY_BUCKET, state["bucket"])
    safe_user_set(_KEY_SORT, state["sort"])
    safe_user_set(_KEY_NOVELTY_VIEW, state["novelty_view"])
    safe_user_set(_KEY_DOMAIN, state["domain"])
    safe_user_set(_KEY_AUTHOR, state["author"])
    safe_user_set(_KEY_WORK, state["work_id"])
    safe_user_set(_KEY_WORK_LABEL, state.get("work_label"))
    safe_user_set(_KEY_PAGE, state["page"])


def _novelty_selection(state: Dict[str, Any]) -> Optional[Tuple[str, ...]]:
    """The selector's chosen view, as the service's `novelty` argument.

    `None` (the empty selection) means ALL -- the phase-wide convention that
    filters compose as AND and an empty set is not a filter.

    The mapping is the SERVICE's `novelty_view_shades`, never a set of shades
    assembled here: which statuses count as a candidate and which as
    non-correspondence is policy (`shared/discovery_novelty.py`), and a page that
    built its own tuple would keep matching the old policy after the policy
    moved. Ruling F's shades reached this page as a hardcoded pair once already
    and the mistake is cheap to repeat.

    NO UNIT CHECK ANY MORE. It used to return `None` on the per-work unit because
    the builder refused the combination there; it now answers it, so silently
    dropping the reader's selection would be showing them a wider set than the
    control says. See `normalise_state`.
    """
    return novelty_view_shades(state.get("novelty_view"))


async def fetch_findings(state: Dict[str, Any],
                         suppressed: Tuple[str, ...] = ()) -> Dict[str, Any]:
    """One enveloped findings read. A DIRECT await on the async wrapper.

    `suppressed` is the admin hide list, read ONCE per refresh by the caller and
    threaded in -- never fetched here, because this is called twice on the
    page-clamp path and a second Supabase read for the same list would be a
    second network round trip for an answer the caller already has.
    
    It reaches the SQL predicate, so the COUNTS follow the hide list (owner
    ruling, 2026-08-06): `total`, the pager and the facet counts all drop by
    whatever was hidden, because all three are built from that one predicate.
    """
    return await get_findings_enveloped(
        state["unit"],
        bucket=state["bucket"],
        novelty=_novelty_selection(state),
        suppressed=suppressed,
        # ALWAYS `SHOWN` -- i.e. the divergence axis adds NO predicate. The
        # selector now expresses ruling F's rows through `novelty` (they are
        # shades of the same column), so leaving the old axis at its
        # hidden-by-default value would silently subtract them again and the
        # "do not correspond" view would return nothing at all.
        divergence=DIVERGENCE_SHOWN,
        domain=state.get("domain"),
        author=state.get("author"),
        work_id=state.get("work_id"),
        # THE MANUSCRIPT AXIS (2026-08-07), read from the state like every other
        # filter. NORMALLY ABSENT here: no control on this page sets `sys_id`, so
        # `state.get` returns `None` and the predicate adds nothing. It is passed
        # anyway, and that is not defensive padding --
        # `tests/test_discovery_build.py::
        # test_the_probes_filter_axes_are_PINNED_to_the_shipped_predicate_builder`
        # requires every axis the builder accepts to be reachable from this call,
        # precisely because an axis the page cannot pass is one the benchmark claims
        # to measure and never does. The expansion path (`_fetch_children`) is what
        # actually sets it today.
        sys_id=state.get("sys_id"),
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


def _facet_request(level: str, state: Dict[str, Any],
                   suppressed: Tuple[str, ...] = ()) -> Dict[str, Any]:
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
        # `SHOWN` for the same reason `fetch_findings` passes it: ruling F's rows
        # are now selected through `novelty`, so the old axis must add no
        # predicate or the facet counts would describe a narrower population than
        # the rows beside them.
        "divergence": DIVERGENCE_SHOWN,
        # THE ROW UNIT (§3.6). The cascade's counts are counts of ROWS, and the
        # unit decides what a row is -- so a cascade that did not carry it put a
        # number beside an option describing a population the result bar beside
        # it did not report. It is part of the request, so it is part of the
        # re-fetch key: changing the unit re-reads all three levels.
        "unit": state["unit"],
        "domain": state.get("domain") if level != "domain" else None,
        "author": state.get("author") if level == "work" else None,
        # THE HIDE LIST IS PART OF THE REQUEST, so it is part of the re-fetch key
        # below: suppressing a row must re-read the cascade, or a facet count
        # would keep counting a row the result set no longer shows -- which is
        # exactly the promise `_node_text` makes.
        "suppressed": suppressed,
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


def _facet_cache_key(level: str, state: Dict[str, Any],
                     suppressed: Tuple[str, ...] = ()) -> Tuple[Any, ...]:
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
    return ((level,) + tuple(sorted(_facet_request(level, state, suppressed).items()))
            + _artifact_identity())


async def fetch_facets(level: str, state: Dict[str, Any],
                       suppressed: Tuple[str, ...] = ()) -> Dict[str, Any]:
    """One enveloped facet-cascade read. A DIRECT await on the async wrapper."""
    return await get_findings_facets_enveloped(
        level, **_facet_request(level, state, suppressed))


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
    # THE ADMIN HIDE LIST, awaited BEFORE the launch read is dispatched.
    #
    # THE ORDER IS THE WHOLE POINT, and it is the opposite of what it looks like
    # it should be. The findings query needs this list as an ARGUMENT, so there is
    # nothing for it to overlap with -- it cannot be a task the row read races.
    # And awaiting it AFTER dispatching the launch task would hand the loop a
    # suspension point in which the launch read completes, so the row read would
    # then be issued after the launch read finished: §3.7's concurrency, undone.
    # `test_the_launch_read_and_the_row_read_OVERLAP` caught exactly that.
    #
    # So it is resolved FIRST, while nothing else is in flight, and the launch
    # task is dispatched afterwards to overlap the row read as before. The cost is
    # bounded and small: the list is process-cached for 30s, the read is skipped
    # entirely when Supabase is not configured, and a failure is cached for 5s --
    # so in the steady state this line costs nothing at all.
    #
    # A MUTABLE HOLDER, because the ✕ handler replaces the value: after hiding a
    # row it re-reads and writes back here, so the next `refresh` filters on the
    # new list. Without that the row the owner just hid would stay on screen.
    #
    # NO `try` AROUND THIS. The fail-open is `suppressed_identification_ids`'s own
    # (it catches every exception and returns `()`), so a second handler here would
    # be defending against something that cannot arrive -- and the masking sweep's
    # line-granular gate proved it: the `except` and its log line were never
    # executed by any capture, correctly, because nothing reaches them. A branch no
    # capture can paint and no test can drive reads as coverage nobody has, which
    # is the defect class that gate exists to find. The fail-open behaviour is
    # unchanged and is asserted where it lives, on the wrapper.
    hidden: Dict[str, Any] = {"ids": tuple(await suppressed_identification_ids())}

    launch = asyncio.ensure_future(fetch_launch_stats())
    with body:
        await _render_body(state, lang, page_client, launch=launch,
                           hidden=hidden)
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
        # AFTER the caveat, never instead of it: the caveat says what a row IS
        # and is the one thing a reader needs first. This says what the PAGE is.
        ui.label(copy_text("beta_head", lang)).classes(
            f"{HEAD_CLASS}-beta dnote text-xs")
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
            ui.label(copy_text("beta_howto", lang)).classes(
                f"{HOWTO_CLASS}-beta dnote text-xs"
            )
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
    inline-start rule, never fine print and never a dismissible warning.

    IT NOW CARRIES AN ICON (owner report, 2026-08-06: the disclaimer "is easily
    missed"). The `caveat` class already gives it a tinted plate and a gold
    inline-start rule, and it was still being read past -- so the missing signal
    was the one thing that marks a block as an ADVISORY rather than as more prose.
    `info` rather than `warning`: nothing here is going wrong, and an alarm glyph
    on a permanent element trains a reader to dismiss it.

    The element stays a plain `div` carrying `caveat` (a test pins the class, and
    the CSS rule that draws the plate is keyed on it), and stays UNDISMISSIBLE.
    The row is `items-start` so the glyph aligns to the first line of a wrapping
    sentence rather than floating in the vertical middle of three lines, and the
    text keeps `flex: 1` so it wraps beside the icon instead of pushing it out.
    """
    with ui.element("div").classes(f"caveat {CAVEAT_CLASS} w-full p-3 text-sm"):
        with ui.row().classes("items-start gap-2 flex-nowrap w-full"):
            # `shrink-0` so the glyph keeps its box when the sentence wraps.
            ui.icon("info").classes(
                f"{CAVEAT_CLASS}-icon shrink-0"
            ).style("color: var(--accent-gold); font-size: 20px;")
            ui.label(copy_text("caveat", lang)).classes(
                f"{CAVEAT_CLASS}-text").style("flex: 1 1 auto; min-width: 0;")


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
                       launch: Any = None,
                       hidden: Optional[Dict[str, Any]] = None) -> None:
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

    #: THE ADMIN HIDE LIST for this page render, in a MUTABLE holder.
    #:
    #: A dict rather than a local, because the ✕ handler has to be able to replace
    #: it: suppressing a row calls `invalidate()` and then re-reads, and `refresh`
    #: must see the NEW list or the row the owner just hid would stay on screen
    #: until a reload -- which reads as the button not working.
    #:
    #: NOT DEFAULTED HERE. An earlier revision carried `if hidden is None: hidden
    #: = {"ids": ()}`, and the masking sweep's line-granular gate showed that line
    #: never executes -- correctly, because `create_findings_page` is the only
    #: caller and it always builds the holder. A defensive default against a caller
    #: that does not exist is a line no capture can paint and no test can reach,
    #: which reads as coverage nobody has. `hidden or {"ids": ()}` at the two use
    #: sites keeps a bare `None` from raising without adding an unreachable
    #: statement.
    hidden = hidden if hidden is not None else {"ids": ()}

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

    async def _launch_meta() -> Dict[str, Any]:
        """The launch envelope's `meta`, from the read the CALLER dispatched.

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
        return dict((envelope or {}).get("meta") or {})

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
        # THE ADMIN HIDE LIST, read ONCE per refresh and threaded into all three
        # reads below (owner ruling, 2026-08-06). One read, not three: it is a
        # Supabase round trip, it is process-cached, and the rows, the count and
        # the facet cascade must all be built from the SAME list -- a second read
        # between them could return a different one and put a count beside rows it
        # does not describe.
        #
        # RESOLVED ONCE PER PAGE, NOT PER REFRESH, and read from the caller's
        # mutable holder here. Awaiting it on this line is what an earlier draft
        # did, and it undid §3.7: the launch read is dispatched as a task
        # specifically so it overlaps the row read, and an `await` in front of the
        # row read serialised the whole chain again. Caught by
        # `test_the_launch_read_and_the_row_read_OVERLAP`, not by reading the diff.
        #
        # RE-CHECKED ON EVERY REFRESH, THROUGH A CACHE PEEK THAT NEVER AWAITS
        # (Codex review, 2026-08-07, HIGH). "Once per page" is the wrong lifetime
        # for a fact other people can change: a row hidden by another admin -- or by
        # this admin in a second tab -- stayed visible here for as long as the page
        # was open, through every filter change and page turn.
        #
        # AND THE OBVIOUS FIX IS WRONG. Awaiting `suppressed_identification_ids`
        # here puts a Supabase round trip on the critical path of every control the
        # reader touches, and two guards caught it within a minute of my trying:
        # the one-dispatch-per-read probe, and
        # `test_clicking_cycles_hidden_then_shown_then_only_and_the_query_follows`,
        # whose rows stopped arriving inside its yield budget. Dispatching it
        # concurrently and refetching on a change is no better -- it is still an
        # extra await before the rows can render.
        #
        # So this reads the PROCESS CACHE and nothing else: a lock, a clock read and
        # a dict lookup, no I/O and no offload.
        #
        # `None` (nothing fresh cached) means KEEP WHAT WE HAD -- never `()`. The
        # async reader fails open to an empty tuple, so treating "could not tell" as
        # "nothing hidden" would un-hide every row; the peek returns `None` for that
        # case specifically. The union is belt-and-braces on the same point: this
        # holder only ever grows within a page's life.
        #
        # AND A PEEK ALONE IS NOT ENOUGH -- the previous revision of this block
        # claimed coherence it did not deliver, and Codex's re-review was right to
        # reject it. The cache is warmed ONLY by a page load and by a local write,
        # so on a long-open page the entry expires, the peek returns `None` forever
        # after, and the page keeps a list that can no longer change. "Within the
        # TTL the peek IS the current list" was true; "so a peek is enough" did not
        # follow.
        #
        # THE RE-WARM IS DISPATCHED AND NOT AWAITED, and it APPLIES ITSELF when it
        # lands (re-rendering only if the list actually changed). An earlier revision
        # left the result for "the next refresh", which Codex's third pass rightly
        # rejected: with no further interaction there is no next refresh, so
        # staleness stayed unbounded rather than becoming bounded by one click. This
        # refresh still pays nothing, and convergence needs no help from the reader.
        cached = cached_suppressed_identification_ids()
        if cached is not None:
            merged = tuple(sorted(set(cached) | set(hidden["ids"] or ())))
            if merged != tuple(hidden["ids"]):
                hidden["ids"] = merged
        else:
            _rewarm_hide_list(hidden, refresh, page_client)
        suppressed = tuple(hidden["ids"])
        envelope = await fetch_findings(state, suppressed)
        if _stale():
            return
        # A PERSISTED PAGE PAST THE END. The clamp moves the STATE (never a
        # display-only local) and the refetch is what makes the move real --
        # without it the reader is looking at the empty page they were clamped
        # off. AT MOST ONE extra read: the clamped page is inside the set by
        # construction, so the second envelope cannot be out of range again.
        if clamp_page_to_total(state, envelope):
            write_state(state)
            envelope = await fetch_findings(state, suppressed)
            if _stale():
                return
        launch_meta = await _launch_meta()
        if _stale():
            return
        # ONE availability probe per refresh, not one per row: the service
        # caches it per (path, version), and the rows only receive a loader
        # when the served asset actually carries excerpts -- an older asset
        # renders exactly the pre-excerpt page (no dead toggle).
        excerpts_on = await excerpts_available()
        if _stale():
            return
        results_region.clear()
        with results_region:
            _render_results(envelope, state, lang, refresh,
                            more_pool_total=launch_meta.get("more_pool_total"),
                            sidecar_version=launch_meta.get("sidecar_version"),
                            suppressed=suppressed, hidden=hidden,
                            excerpts_on=excerpts_on)
        if _stale():
            return
        await _populate_facets(
            filter_bar, state, lang, refresh,
            cache=facet_cache, page_client=page_client, is_stale=_stale,
            suppressed=suppressed,
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
    # ONE sync, not two: the selector replaced a pair of chips that each needed
    # re-syncing against the other's state.
    syncs = [_render_novelty_switch(state, lang, refresh)]
    # The bucket control is state-dependent too, and was not collected -- see
    # `_render_bucket_control`'s "IT WENT STALE".
    syncs.append(_render_bucket_control(state, lang, refresh))
    _render_facet_groups(lang)
    # NO COVERAGE FILTER, and its absence is a decision rather than an omission
    # (owner ruling, 2026-08-06). This bar carried a permanently-disabled
    # coverage card on the "never silently absent" principle -- a filter that
    # vanishes is indistinguishable from one that never existed. That principle
    # is right when the axis is COMING; measurement says this one is not,
    # because the POOL CONTROL ABOVE ALREADY IS IT. `COVERAGE_FLOOR = 0.8` lives
    # inside `shared.discovery_main_pool`, and `low_coverage` is one of the five
    # reasons a row is in the second pool (5,508 of them). A coverage filter
    # would re-ask, in a second control with a second vocabulary, the question
    # the always-present pool control has already answered -- and there is
    # almost nothing left for it to separate: 98.4% of main-pool rows are at
    # >=50% coverage and 88.8% are past the floor itself.
    #
    # It also made a PROMISE. An amber "not available yet" tag on a beta surface
    # is an enumerated roadmap item, which is exactly what the owner's beta note
    # ruling excludes. The figure itself still renders on every row that has one
    # (`coverage_clause`), so a reader loses no fact -- only a control they
    # could never use.
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
    """THE FOUR-STATE SELECTOR -- one control where this card had two chips.

    Owner ruling, 2026-08-06: "we have three wanted states. Default: Show all.
    State one: Show only candidates for new find. State two: Show only
    divergent" -- then "we may want also the state of divergent OR novel".

    WHY ONE CONTROL AND NOT TWO. `novelty_status` holds ONE value per
    identification, and `fills_gap` (a candidate) and the two ruling-F shades are
    three mutually exclusive values of it. Two independent controls over one
    column can therefore be driven into combinations where one of them silently
    does nothing -- which is exactly what happened: a cycling divergence chip
    beside a candidacy switch, each able to make the other inert, propped up by
    `findings_divergence_offered`, a disabled state, an explanatory note and a
    `normalise_state` rule. A single-select cannot express an incoherent
    combination at all, so all of that machinery is deleted rather than fixed.

    THE FOURTH STATE IS A UNION, not an intersection, and the measurement is why:
    "divergent AND novel" is EMPTY at the leaf grain (0 of 53,581 rows -- one
    column cannot hold two values), while "divergent OR novel" is 7,381
    main-pool rows. The owner asked for the second.

    WORDING. The Hebrew is the owner's own (`חוסר התאמה עם הקטלוג` -- "lack of
    correspondence with the catalogue") and the English follows it rather than
    the reverse. `מחלוקות` / "disagreements", used by the strings this replaces,
    implies parties and implies one of them is wrong; ruling F's entire position
    is that NEITHER side is adjudicated. The candidacy state reuses the RATIFIED
    name from `novelty_strings()` rather than a second name for the same axis.

    NO COUNTS IN THE OPTIONS (owner ruling). Ruling T already forbids a count on
    the bucket control, for the reason that applies here too: a number inside a
    filter reads as a finding. Four counting queries per render would also have
    to track every other active filter or contradict the list below them.

    A DROPDOWN, not a segmented row: four states with prose labels do not fit a
    chip row at phone width, and the reader sees all four at once instead of
    discovering them by clicking. The warning stays CARD PROSE -- ruling F's
    control is an "explicitly warned" one, and a warning a reader must hover to
    find is not one they were given before choosing.

    Returns a re-sync callable for symmetry with the other cards; the selector
    has nothing left to re-sync (no state can make it inert), so it only keeps
    the label honest if a future state is added.
    """
    with _filter_card("novgrp", f"{FILTER_BAR_CLASS}-novelty"):

        async def _change(event) -> None:
            chosen = getattr(event, "value", None)
            # VALIDATED, not trusted: the value arrives from the client, and an
            # unknown one must widen to `all` rather than reach the service.
            state["novelty_view"] = (
                chosen if chosen in NOVELTY_VIEWS else NOVELTY_VIEW_ALL)
            state["page"] = 1
            await refresh()

        # Shaped exactly like `_render_sort_select` -- options positional, no
        # `emit-value map-options` -- so this select behaves like the one that
        # already ships rather than like a second idiom.
        #
        # A NOTE FOR ANYONE INSPECTING THE RENDERED PROPS: `_props['options']`
        # shows the four entries with INTEGER values 0..3 and `_props['value']`
        # is `None`. That is NiceGUI's wire encoding for a dict-valued select and
        # is not a defect -- the shipped sort select reads identically. What
        # matters is `element.value` (`'all'`) and `element._values` (the four
        # view keys), which is what the change handler receives. I mistook the
        # encoding for a bug once; the way to tell is to compare against the sort
        # select in the same render, which is known-good.
        select = ui.select(
            _novelty_view_options(lang),
            value=_current_novelty_view(state),
            label=copy_text("novelty_view_label", lang),
            on_change=_change,
        ).props("dense outlined").classes(
            f"{FILTER_BAR_CLASS}-novelty-view w-full")
        select.tooltip(novelty_strings(lang)["help"])

        # THE WARNING, verbatim from the shared vocabulary, as card prose.
        ui.label(divergence_warning(lang)).classes(
            f"{FILTER_BAR_CLASS}-divergence-warning dnote text-xs")

    def _sync() -> None:
        # The selector is actionable in EVERY state -- the service answers the
        # novelty filter on all three row units as of 2026-08-06, so there is no
        # unit that can withdraw it and nothing to disable. Kept as a no-op so
        # the filter bar's build contract (every card returns a re-sync) holds.
        select.value = _current_novelty_view(state)

    _sync()
    return _sync


def _novelty_view_options(lang: str) -> Dict[str, str]:
    """VIEW -> its reader-facing label, for the selector.

    Built from the SERVICE's closed `NOVELTY_VIEWS` vocabulary rather than
    written out, so a view added there without a label here fails loudly at
    render (a `KeyError` naming the view) instead of rendering a blank option.
    """
    labels = {
        NOVELTY_VIEW_ALL: copy_text("novelty_view_all", lang),
        # The RATIFIED candidacy name, substituted rather than retyped.
        NOVELTY_VIEW_CANDIDATES: novelty_strings(lang)["toggle"],
        NOVELTY_VIEW_DIVERGENT: copy_text("novelty_view_divergent", lang),
        NOVELTY_VIEW_EITHER: copy_text("novelty_view_either", lang),
    }
    missing = [view for view in NOVELTY_VIEWS if view not in labels]
    if missing:  # pragma: no cover -- a vocabulary change with no label
        raise KeyError(
            "web/pages/findings.py: no reader-facing label for novelty view(s) "
            f"{missing} -- every member of NOVELTY_VIEWS needs one, or the "
            "selector renders a blank option a reader cannot interpret")
    return {view: labels[view] for view in NOVELTY_VIEWS}


def _current_novelty_view(state: Dict[str, Any]) -> str:
    """The state's view, resolved to a member of the vocabulary."""
    view = state.get("novelty_view")
    return view if view in NOVELTY_VIEWS else NOVELTY_VIEW_ALL

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
    suppressed: Tuple[str, ...] = (),
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
        key = _facet_cache_key(level, state, suppressed)
        cached = cache.get(level) if cache is not None else None
        if cached is not None and cached[0] == key:
            envelope = cached[1]
        else:
            envelope = await fetch_facets(level, state, suppressed)
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

    STILL ONE STRING, and that is a decision rather than an omission (owner
    report, 2026-08-06: "count numbers in the domain list are misaligned").

    The report is right about the symptom -- a ragged column of counts is harder
    to scan than a flush one -- and the shared CSS block already ships the rule
    that fixes it: `.gs-discovery .dnode .c { margin-inline-start: auto;
    font-variant-numeric: tabular-nums }`, written for a `<span class="c">`
    around the count and currently matching NOTHING, because every count arrives
    here inside the label text.

    Splitting them is deliberately NOT done in this pass. `_node_text` returns a
    STRING and its single-definition property is pinned by
    `test_the_count_promise_is_made_on_all_three_controls_or_on_none`: the domain
    tree passes the result to `ui.button(text)` while the author and work facets
    pass it as a `ui.select` OPTION LABEL, and a select option cannot hold
    markup. So a span-based fix aligns the tree and silently does nothing for the
    other two controls -- which is worse than the ragged column it replaces,
    because the three lists would then disagree about the shape of the same fact
    while a test asserts they share one formatter. Making the tree flush needs
    that formatter split in two (an element builder for the tree, a string for
    the selects), and that is a change with its own gate, not a cosmetic tweak.
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

    # `align=left` WOULD BE A PHYSICAL DIRECTION and is deliberately absent
    # (external review, 2026-08-06). Quasar's `align` prop maps to a physical
    # side, so a domain label in Hebrew stayed pinned to the LEFT edge of its
    # own button while the rest of the tree read right-to-left. The shared block
    # already carries the logical equivalent -- `.gs-discovery .dnode {
    # text-align: start }` -- so the correct treatment is to let that rule apply
    # rather than to override it per element. `justify-start` is Quasar's own
    # flex utility and is direction-AWARE (it resolves to `flex-start`, which
    # follows the writing mode), which is what keeps the label against the
    # reading edge in both languages.
    node = ui.button(text, on_click=_pick).props("flat dense no-caps")
    node.classes(
        " ".join(
            part for part in (
                "dnode", "w-full", "justify-start",
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
    more_pool_total: Any = None, sidecar_version: Any = None,
    suppressed: Tuple[str, ...] = (),
    hidden: Optional[Dict[str, Any]] = None,
    excerpts_on: bool = False,
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

    # THE CATALOGUE TITLE (2026-08-05, coordinator-authorized addition, so a
    # reader can recognise the physical object beside its shelfmark). Resolved
    # ONCE for the whole page, never per row: `libraries.csv` has no home in
    # the discovery sidecar, and `state.meta_mgr.csv_bank` is a PLAIN DICT
    # populated in the BACKGROUND at startup (`MetadataManager
    # .start_background_loading`, `shared/metadata_manager.py`; not atomically --
    # see the memo-lifetime note below) -- reading it here is synchronous and
    # zero-I/O, the same unguarded pattern `web/main.py` and every call site in
    # `web/pages/browse.py` already use on `state.meta_mgr.csv_bank`. It needs
    # no offload wrapper and adds none. `app_state`, not `state`: this
    # function's own parameter is already named `state` for the page's filter
    # state and would shadow the module-level singleton.
    #
    # Missing on ~14% of rows (`libraries.csv` has no title for them); those
    # sys_ids are simply absent from the dict below, and `catalogue_title`
    # returns `None` for them -- `_render_shelfmark` renders nothing at all in
    # that case, not an empty element or a placeholder.
    from web.state import state as app_state
    # MEMOISED ON DEMAND, and there is no separate pre-pass. There WAS one, added
    # when this only had to serve the rows on the page; when expansion children
    # needed titles too (2026-08-07) I added a lazy miss branch beside it and left
    # the pre-pass in place -- and the masking sweep's line gate immediately reported
    # the miss branch as never executed, because the pre-pass had already resolved
    # every key any test could ask for.
    #
    # The gate was right, and the right conclusion was NOT to contrive a case that
    # reaches the miss branch: two pieces of code doing the same work, one of which
    # only ever runs when the other has not, is one piece of code too many. The
    # accessor below now owns the whole job -- including its own key normalisation,
    # which the two used to have to agree on by hand (they briefly did not: the batch
    # keyed by the RAW sys_id while the lookup read `str(...)`, so a non-string
    # sys_id was resolved and then missed, a silent bypass that showed up only as a
    # slower render).
    #
    # NOTHING IS LOST BY DROPPING THE PRE-PASS. `csv_bank` is a plain in-memory dict,
    # so resolution is a dict lookup with no I/O,
    # and the memo means each distinct sys_id is looked up exactly once per render
    # however many rows and children carry it. What the pre-pass bought was ordering,
    # not fewer lookups.
    catalogue_titles: Dict[str, Optional[str]] = {}

    def _catalogue_title(item: Mapping[str, Any]) -> Optional[str]:
        """The catalogue's title for `item`'s manuscript, batch first.

        MEMOISED ON MISS, and that is what makes this work for EXPANSION CHILDREN
        (owner report, 2026-08-07: the "Catalogued as:" line was absent from a work
        row's children). The batch above can only see the sys_ids on the CURRENT
        page; a child is fetched lazily, long after this closure was built, and its
        manuscript may not appear at the top level at all. A pure `dict.get` returned
        `None` for every one of them -- and `None` means "render nothing", so the
        line silently vanished exactly where a reader comparing a work against its
        witnesses most needs it.

        STILL NOT A PER-ROW READ, which is the property
        `test_the_page_batches_catalogue_titles_off_the_event_loop_never_per_row`
        pins -- and it is pinned as a CEILING on `csv_bank.get` calls (one per
        distinct sys_id), which memoisation satisfies exactly as a pre-pass did.
        `csv_bank` is a plain in-memory dict, so a miss costs one dict lookup and
        no I/O.

        `_MISSING` rather than `None` as the negative cache value: ~14% of rows have
        no title in `libraries.csv`, and storing `None` would make every one of them
        re-look-up on each render pass -- harmless but pointless, and it would blur
        "not yet resolved" into "resolved, and there is no title".

        CACHING A MISS IS SAFE HERE, AND ONLY BECAUSE THE MEMO IS PER-RENDER.
        `csv_bank` is NOT populated atomically at startup, whatever the sentence
        below once claimed: `MetadataManager.start_background_loading` spawns a
        daemon thread that inserts the ~255k rows ONE AT A TIME, so early in a
        process every lookup misses (the owner's startup log, 2026-08-07, shows
        four `/computed-identifications` requests served before "Loaded 255723
        records into csv_bank"). A memo that OUTLIVED the render would therefore
        pin "no catalogue title" for a reader who arrived during warm-up -- which
        is exactly the defect the browse panel's equivalent resolver had, and was
        fixed for (`web/pages/browse_enrichment.py::_csv_row`).

        This dict is created inside `_render_results` and contains no `await`, so
        it cannot survive the pass that built it: the next `refresh()` builds a
        fresh one and re-reads the bank. That lifetime is the whole safety
        argument, and it is pinned by
        `test_a_warmup_miss_cannot_outlive_the_render_that_cached_it`. If this memo
        is ever hoisted to page scope -- to share it across refreshes, say -- the
        warm-up defect arrives with it and the miss must then be gated on the bank
        being non-empty.

        WRITTEN WITH NO EARLY `return` for the missing-sys_id case. Every row on this
        surface has a sys_id (the work unit is the one grain without one, and it never
        calls here), so a guard clause there is a line no capture can paint -- which
        the masking sweep's line gate reports, correctly. As a positive branch the
        behaviour is identical and every line is reachable.
        """
        key = str(item.get("sys_id") or "")
        cached = catalogue_titles.get(key, _MISSING) if key else None
        if cached is not _MISSING:
            return cached or None
        title = None
        if app_state.meta_mgr is not None:
            csv_row = app_state.meta_mgr.csv_bank.get(key)
            title = (csv_row or {}).get("title") or None
        catalogue_titles[key] = title
        return title

    with ui.column().classes(f"rows {RESULTS_CLASS}-rows w-full gap-2"):
        if not items:
            _render_empty_state(state, lang, refresh, more_pool_total)
        for item in items:
            _render_row(item, lang, sidecar_version=sidecar_version,
                        state=state, catalogue_title=_catalogue_title,
                        refresh=refresh, suppressed=suppressed,
                        hidden=hidden, excerpts_on=excerpts_on)

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


def _render_divergence_basis(meta: Dict[str, Any], lang: str,
                             view: str = NOVELTY_VIEW_ALL) -> None:
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

    READ FROM THE ENVELOPE, never from `state`. `meta['divergence']` is the
    SERVICE's own report of the query it actually ran; the page's state is only
    what the page intended. If those two ever disagree -- a control that failed
    to persist, a request that lost an argument in a wrapper -- this line follows
    the ROWS, which is the half a reader is counting.

    A `meta` with no such key, or one outside the closed vocabulary, states
    NOTHING. An envelope that does not say what it did is not evidence of any
    answer, and asserting a default here would be this module claiming to know
    something it was not told. The shipped reader always supplies it, and a test
    pins that.

    READ FROM `meta['divergent_included']`, NOT `meta['divergence']`
    (2026-08-06). The old key would now report "included" on EVERY render and be
    a lie on three of the four views: the selector expresses ruling F's rows as
    novelty shades and pins `divergence=SHOWN` unconditionally, so that key no
    longer varies with what the reader chose. The new one is derived by the
    service from the predicate it actually applied.

    THE "ALONE" WORDING comes from the reader's own selected VIEW and not from
    the envelope, and the split is deliberate: whether the divergent rows are in
    the count is a fact about the QUERY (the envelope owns it), while whether
    they are the ONLY thing asked for is a fact about the SELECTION (the control
    owns it). Reading the second from the envelope would need `meta` to echo the
    shade list, which is stored machine vocabulary a reader-facing envelope must
    not carry.
    """
    included = meta.get("divergent_included")
    if included is None:
        # An envelope that does not say what it did is not evidence of any
        # answer. State nothing rather than assert a default.
        return
    if not included:
        key = "divergence_excluded"
    elif view == NOVELTY_VIEW_DIVERGENT:
        key = "divergence_alone"
    else:
        key = "divergence_included"
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

            _render_divergence_basis(meta, lang, _current_novelty_view(state))

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
    "novelty_view", "domain", "author", "work_id")

#: Chip axis -> the value clearing it returns to, for the axes whose neutral
#: value is not `None`. The selector clears to `all` -- a member of its closed
#: vocabulary, never `None` or `False`: `_stored_novelty_view` and
#: `novelty_view_shades` both treat an unrecognised value as "show everything",
#: so a non-member would happen to work today and stop working the moment either
#: gained a stricter check.
_CHIP_AXIS_NEUTRAL: Dict[str, Any] = {
    "novelty_view": NOVELTY_VIEW_ALL,
}


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
        state[key] = _CHIP_AXIS_NEUTRAL.get(key)
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
    # ONE chip for the selector, carrying the SELECTOR'S OWN label so the two
    # say the same thing about the same axis.
    #
    # COMPARED TO THE DEFAULT, never tested for truthiness: every view is a
    # non-empty string, so `if state.get("novelty_view")` would put a chip on the
    # page in the default state -- a chip bar announcing a filter nobody applied,
    # which is the defect the retired divergence chip shipped with.
    view = _current_novelty_view(state)
    if view != NOVELTY_VIEW_ALL:
        chips.append(("novelty_view", _novelty_view_options(lang)[view]))
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


#: How many children ONE expanded row shows at a time. Bounded for the reason
#: the page itself is: the heaviest work in the public artifact carries hundreds
#: of identifications, and a row that renders all of them turns one click into
#: the largest render on the page. The extent line says what was withheld, so a
#: bounded page is never mistaken for the whole group.
_EXPANSION_PAGE_SIZE = 25

#: The filter axes an expansion may pin. DERIVED from the shipped predicate
#: builder's own signature -- never a list written here -- and derived in the
#: COMPONENT, beside the unit->axis table it validates, so the table and the
#: question "is this axis real" cannot drift apart across two files.
#:
#: Derived THERE rather than here because this page may not name the query
#: layer at all -- `tests/test_findings_page.py` forbids it by substring, and
#: rightly: every read on this page goes through `web/discovery.py`, which does
#: the offloading exactly once, and a page that could reach past it could nest a
#: second dispatch. The row component already imports that layer's closed
#: vocabularies, so the derivation belongs beside the table it validates.
_EXPANSION_SUPPORTED_AXES = rows.EXPANSION_SUPPORTED_AXES


def _child_state(state: Dict[str, Any], axis: str, value: str) -> Dict[str, Any]:
    """The reader's OWN filter state, at the LEAF grain, pinned to one group.

    Every axis is carried over unchanged -- bucket, the novelty view, domain,
    author, sort -- and only `unit` and the group key differ. That is what makes
    an expansion honest rather than merely convenient: the parent row was produced
    under this filter set, so its count and the rows underneath it come from ONE
    predicate and cannot contradict each other.

    The alternative was `get_manuscript_works_enveloped` / the panel's
    `get_work_expansion_enveloped`, and both were rejected on measurement, not
    taste: neither takes a bucket, candidacy or divergence filter at all, so a
    parent reading "3 works" under the main pool would open onto every work in
    the corpus for that manuscript. A reader cannot see that kind of wrongness,
    which is what makes it worse than an error they can.
    """
    child = dict(state)
    child["unit"] = FINDINGS_UNIT_IDENTIFICATION
    child["page"] = 1
    # Pinning the group key REPLACES any same-axis filter the reader had set: a
    # work row inside a work-filtered page is that same work, and a manuscript
    # row's children are that manuscript's regardless.
    child[axis] = value
    return child


async def _fetch_children(state: Dict[str, Any], item: Mapping[str, Any],
                          page: int = 1,
                          suppressed: Tuple[str, ...] = ()) -> Dict[str, Any]:
    """One grouped row's children, through the SHIPPED findings read.

    No new query and no new service entry point -- `_build_findings_filter`
    already accepts `work_id`, and the leaf grain is one of the three units the
    page offers. The envelope comes back in the same closed four-key shape, so
    the renderer's failure branch works on it unchanged.
    """
    target = rows.expansion_target(item)
    if target is None:
        return unavailable_envelope_shape()
    axis, value = target
    # REFUSED, LOUDLY, rather than passed and dropped. `get_findings_enveloped`
    # accepts **kwargs-shaped filters, so an axis the predicate does not
    # implement does not raise -- it is IGNORED, and the expansion then returns
    # every row matching the reader's filters instead of the ones under this
    # parent. A reader cannot see that; they see a plausible list that is the
    # wrong list. So the supported set is named here, and anything else fails
    # closed to the renderer's named failure state.
    if axis not in _EXPANSION_SUPPORTED_AXES:
        logger.error(
            "findings expansion: unit %r asks for the %r filter axis, which "
            "_build_findings_filter does not implement -- refusing rather than "
            "returning an unpinned page",
            item.get("unit"), axis)
        return unavailable_envelope_shape()
    child = _child_state(state, axis, value)
    # The CHILD list's own page, not the reader's page through the parent list.
    # Clamped at 1 so a caller cannot ask for page 0 or a negative offset.
    child["page"] = max(1, int(page or 1))
    return await get_findings_enveloped(
        child["unit"],
        bucket=child["bucket"],
        novelty=_novelty_selection(child),
        # THE HIDE LIST APPLIES INSIDE AN EXPANSION TOO. A suppressed row that
        # survived one click into its parent work would be the hide silently not
        # working, in the one place a reader looks hardest -- and the parent's
        # count already excludes it, so the child list would contradict the number
        # above it.
        suppressed=suppressed,
        # `SHOWN`, matching `fetch_findings` exactly. The children must come from
        # the SAME predicate as the parent -- that is the whole honesty property
        # of the expansion -- so any divergence here other than the parent's
        # would let a parent's count and its children disagree.
        divergence=DIVERGENCE_SHOWN,
        domain=child.get("domain"),
        author=child.get("author"),
        work_id=child.get("work_id"),
        # THE MANUSCRIPT AXIS (2026-08-07). `_child_state` pins whichever axis the
        # parent's unit names, so this reads the same `child` dict `work_id` does --
        # only one of the two is ever set for a given parent.
        sys_id=child.get("sys_id"),
        sort=child["sort"],
        page=child["page"],
        page_size=_EXPANSION_PAGE_SIZE,
    )


def unavailable_envelope_shape() -> Dict[str, Any]:
    """The four-key envelope for "this row cannot name what it would expand".

    A real envelope shape rather than `None`, so the renderer has exactly one
    failure branch to handle and a caller cannot forget which of two shapes it
    got.
    """
    return {"status": "unavailable", "items": [], "total": 0,
            "meta": {"reason": "expansion_key_missing"}}


def preview_url(item: Mapping[str, Any]) -> Optional[str]:
    """The leaf row's MATCHED FOLIO, in the EXISTING bare browse viewer.

    `?embed=1` is the route built for the discovery-review iframe and reused by
    `/atlas`: it renders the viewer with no nav shell AND disables snapshot
    restore/persist (`web/pages/browse.py`, `embedded=True`), which is the
    property that matters here -- previewing a manuscript from this page must not
    overwrite wherever the reader had left `/browse`.

    `page` AND `volume_ie` ARE ADDED TOGETHER OR NOT AT ALL (owner report,
    2026-08-08). They are the two components of ONE address the service parsed
    out of ONE page id; a folio number without its volume addresses more than one
    page in the 988 identifications that span volumes, and `/browse` would
    resolve it against whichever volume it happened to open. Requiring both is
    therefore not defensive padding -- it is the difference between a link that
    lands on a matched folio and one that lands a volume away.

    THE BUILDER IS `web/components/discovery_links.py`, shared with BOTH of the
    connections panel's link sites, and `rows.preview_targets_a_folio` -- the
    call the row's NOTE makes -- is the same module's predicate. Sharing it is
    the point rather than tidiness: this link and that sentence are two
    statements about one fact, and two derivations of one fact is how they come
    to disagree.

    The bare `sys_id` form REMAINS the fallback, deliberately: a row whose folio
    did not resolve still has a manuscript worth reaching, and the row's note
    says which of the two it is about to do rather than promising the folio.

    `None` when the row carries no `sys_id`, so a preview is withheld rather
    than pointing at a page that cannot resolve.
    """
    return discovery_links.browse_url(
        item.get("sys_id"), page=item.get("first_match_page"),
        volume_ie=item.get("first_match_volume_ie"), embed=True)


def _viewer_is_admin() -> bool:
    """Whether the CURRENT viewer is an admin, for the ✕'s visibility only.

    NOT THE SECURITY BOUNDARY, and that distinction is the whole reason this is
    allowed to be a cheap in-memory check. The boundary is the RLS policy on
    `discovery_suppressed`: its INSERT and DELETE policies test
    `auth.uid() IN (SELECT id FROM profiles WHERE role = 'admin')`, so a forged
    request from a non-admin is refused by Postgres whatever this returns. This
    only decides whether a button is drawn -- the same posture
    `web/supabase_client.py` documents for hidden discoveries.

    Fails CLOSED (no control) on any error: an unreadable auth state must not
    render an admin affordance, and the cost of a false negative is that the
    owner reloads the page.
    """
    try:
        from web.auth_state import GlobalAuthState

        return bool(GlobalAuthState.is_admin())
    except Exception:  # noqa: BLE001 -- an auth hiccup must not break the page
        return False


def _rewarm_hide_list(hidden: Optional[Dict[str, Any]] = None,
                      refresh=None, page_client: Any = None) -> None:
    """Dispatch a background re-read of the admin hide list. Awaits nothing.

    THE OTHER HALF OF THE PEEK (Codex re-review, 2026-08-07). `cached_ids` never
    fetches, and the cache is warmed only by a page load and by a local write -- so
    without this, a long-open page's entry expires and its peek returns `None`
    forever after, leaving the page on a list that can no longer change. The peek
    gave "no added latency"; this gives "and it still converges".

    NOT AWAITED BY THE REFRESH -- awaiting it would restore exactly the
    round-trip-per-interaction this design exists to avoid -- but IT APPLIES ITSELF
    WHEN IT LANDS. An earlier revision only warmed the cache and left the result for
    "the next refresh", which Codex's third pass correctly rejected: with no
    subsequent interaction there is no next refresh, so staleness was still
    unbounded rather than bounded by one click. The task now folds what it read into
    the page's holder and re-renders if that changed anything, so convergence needs
    no help from the reader.

    GUARDED BY `cache_needs_refresh()` rather than by the peek's `None`, and the
    difference is load-bearing. A FRESH FAILURE also peeks as `None`, and
    re-dispatching on it would turn a Supabase outage into a fetch per interaction
    -- which is what `FAILURE_TTL_SECONDS` exists to prevent. It is also what keeps
    `test_..._issues_one_dispatch_per_read` green: in a process with no Supabase
    credentials (CI, and every test that renders this page) the first read caches a
    failure, so nothing dispatches.

    SINGLE-FLIGHT, via the holder itself. Without a marker, every concurrent refresh
    across every open page passes the expired-cache test and dispatches its own task
    (Codex, MEDIUM): the shared semaphore then serialises the I/O, so they queue
    rather than storm Supabase, but the task queue itself is unbounded and every one
    of them is redundant. The flag lives in `hidden` rather than at module scope
    because it is per-page state -- a global would let one page's in-flight read
    suppress another page's, and the two hold different lists.

    `background_tasks.create` rather than a bare `ensure_future`: NiceGUI keeps a
    reference, so the task cannot be garbage-collected mid-flight and its exception
    is retrieved rather than surfacing as "never retrieved" at interpreter exit.
    """
    if hidden is None:
        return
    try:
        from nicegui import background_tasks

        from web.discovery_suppression import cache_needs_refresh

        if hidden.get("rewarming") or not cache_needs_refresh():
            return
        hidden["rewarming"] = True
        background_tasks.create(
            _reread_hide_list(hidden, refresh, page_client),
            name="discovery-hide-list")
    except Exception as exc:  # noqa: BLE001 -- a re-warm must never break a refresh
        hidden["rewarming"] = False
        logger.warning("findings: could not re-warm the hide list (%s)",
                       type(exc).__name__)


async def _reread_hide_list(hidden: Dict[str, Any], refresh,
                            page_client: Any) -> None:
    """The re-warm body: read the list, APPLY it, and re-render if it changed.

    Applying is the part that makes this converge. Warming the cache alone leaves
    the new list sitting in a dict that only a future interaction would consult, so
    a page nobody touches again never picks it up -- the defect Codex's third pass
    named.

    THE UNION, never an assignment, for the reason the ✕ handler documents: the
    reader fails open to `()`, so assigning would un-hide every row on a failed
    read. One consequence is worth stating plainly rather than leaving implicit: a
    suppression REMOVED elsewhere is not picked up within this page's life. Nothing
    in the UI can remove one -- `unsuppress` has no caller, so un-hiding is a manual
    SQL operation -- and a reader who sees a row reappear on their next page load is
    a far better failure than one whose hidden rows silently return because a read
    failed.

    RE-RENDERS ONLY ON A CHANGE, and only if the reader is still here. `refresh`
    re-reads rows and facets; running it when nothing changed would be a free
    repaint of the page under a reader who did not ask for one.
    """
    try:
        latest = await suppressed_identification_ids()
        merged = tuple(sorted(set(latest) | set(hidden.get("ids") or ())))
        if merged == tuple(hidden.get("ids") or ()):
            return
        hidden["ids"] = merged
        if refresh is not None and not _page_is_gone(page_client):
            await refresh()
    except Exception as exc:  # noqa: BLE001 -- a background re-warm must stay quiet
        logger.warning("findings: the hide-list re-warm failed (%s)",
                       type(exc).__name__)
    finally:
        hidden["rewarming"] = False


def _notify_suppress_failed() -> None:
    """Tell the admin the hide did NOT take effect.

    A silent failure here is the worst outcome of the whole mechanism: the owner
    clicks ✕ to take an embarrassing row off a live beta, sees the page repaint,
    and reasonably concludes it is gone. It would not be. So a failed write says
    so, in the one place the person who clicked is looking.

    `ui.notify` rather than a rendered element: this is a transient report about
    an ACTION, not a fact about the data, and nothing on the page should change
    because a write failed.

    WRAPPED IN `try`, and the wrapping was REMOVED and then RESTORED -- the round
    trip is the useful part of this comment.

    The masking sweep's line gate reported the `except` as never executed, so I
    deleted it, having convinced myself `ui.notify` cannot raise without a client
    context. It can. Codex review, 2026-08-07, checked the installed NiceGUI (3.8.0)
    and was right: `notify()` reads `context.client`, which reads `context.slot`,
    which RAISES `RuntimeError` on an empty slot stack. My probe missed it because I
    called `notify` at module scope, where a default slot exists -- not from a
    background task, where the stack is empty and it raises. That is the exact trap
    `reference_io_bound_safe_storage_trap` already records: `ensure_future` empties
    the slot stack, so `ui.*` RAISES there.

    So the failure mode is real: a hide that fails after the reader's client has
    gone away would turn a reported failure into an unhandled callback exception --
    losing the report AND adding a traceback.

    THE LINE GATE WAS THE RIGHT SIGNAL AND I DREW THE WRONG CONCLUSION FROM IT. "No
    capture paints this line" has two possible causes: the branch is dead, or the
    capture cannot produce the condition. `NON_PAINTING_EXEMPT` exists for the
    second, and its stated admissible reason is "a `return` taken when the reader's
    page is already gone, or a defensive `except` that assigns a local" -- which is
    this, precisely. Three sibling entries already cover the page-gone guards in
    `_populate_facets` and `_render_body`. Registered there rather than deleted.
    """
    try:
        ui.notify(tr("Could not hide this finding. Please try again."),
                  type="negative")
    except Exception:  # noqa: BLE001 -- no live client to notify; the log has it
        logger.warning("findings: the hide failed and could not be reported")


def _render_row(item: Dict[str, Any], lang: str,
                sidecar_version: Any = None,
                state: Optional[Dict[str, Any]] = None,
                catalogue_title=None,
                refresh=None,
                suppressed: Tuple[str, ...] = (),
                hidden: Optional[Dict[str, Any]] = None,
                excerpts_on: bool = False) -> None:
    """One result row, in whichever of the three shipped units the service
    produced it.

    Delegated to `web/components/findings_rows.py` (plan 136-18), which owns the
    anatomy, the novelty badge, the coverage clause and the bucket name. The
    same component renders BOTH buckets: a second-bucket row is a first-class
    result, not a footnote, and giving it its own renderer here is how a
    demotion creeps in.

    The three reader affordances are INJECTED from here rather than reached for
    there: the component renders and does not read, and "where does a manuscript
    live" (and, now, "what did the catalogue call it") is this page's decision.
    The loader closes over the reader's CURRENT state, which is what keeps a
    parent's count and its children in agreement. `catalogue_title` closes over
    the page-wide lookup `_render_results` built in one batched, off-loop-free
    read (see there) -- never a per-row read here.
    """
    loader = None
    if state is not None:
        async def loader(row, page=1, _state=dict(state), _hidden=suppressed):  # noqa: F811
            return await _fetch_children(_state, row, page, _hidden)

    # THE ADMIN ✕, on the identification leaf only and only for an admin. The
    # handler is built HERE rather than in the component for the same reason
    # `load_children` and `preview_url` are injected: the component renders and
    # does not read, so it cannot reach Supabase and cannot know what a refresh
    # is. `None` when there is no refresh to run afterwards (a bare probe render),
    # which withholds the control rather than offering a button that cannot work.
    on_suppress = None
    if refresh is not None and _viewer_is_admin():
        async def on_suppress(identification_id: str) -> None:
            # IF / ELSE rather than an early `return`, and the reason is small but
            # real: a trailing `return` as the last statement of a branch is a line
            # nothing can execute, and the masking sweep's line-granular gate
            # reports it as never painted -- correctly. Written as two branches,
            # every line is reachable and the control flow reads the same.
            if await suppress_identification(identification_id):
                # RE-READ BEFORE REFRESHING, and this order is the whole reason
                # the hide list lives in a mutable holder. `refresh` filters on
                # `hidden['ids']`; the write invalidated the process cache but
                # this page's holder still has the OLD tuple, so refreshing first
                # would repaint the row that was just hidden -- indistinguishable
                # from a broken button.
                #
                # THE UNION, NOT THE RE-READ ALONE (Codex review, 2026-08-07,
                # HIGH). `suppressed_identification_ids` FAILS OPEN to `()`, so on
                # a Supabase blip between the write and the re-read the holder was
                # being overwritten with an empty tuple -- un-hiding every row
                # hidden earlier in the session, right after a click whose whole
                # purpose was to hide one more. A confirmed write is a fact this
                # page knows and must not lose to a failed read, so the id is
                # folded in explicitly and the holder only ever GROWS within a
                # page's life.
                #
                # `sorted` for the same reason the wrapper sorts: the tuple lands
                # in the service's cache key, so its ORDER has to be stable between
                # identical requests or the cache silently never hits.
                if hidden is not None:
                    refreshed = await suppressed_identification_ids()
                    hidden["ids"] = tuple(sorted(
                        set(refreshed) | set(hidden.get("ids") or ())
                        | {str(identification_id)}))
                await refresh()
            else:
                # The write failed (no admin session, an RLS refusal, Supabase
                # down). Refreshing would repaint the same row and read as the
                # click doing nothing at all, so SAY so instead.
                _notify_suppress_failed()

    # The text-vs-text loader, injected only when the served asset carries
    # excerpts (excerpt-v1 marker) -- an older asset gets no toggle at all,
    # which is the rollback-safe direction: new code + old asset renders the
    # pre-excerpt page rather than a control that cannot work.
    load_excerpt = None
    if excerpts_on:
        async def load_excerpt(row):
            return await get_excerpt_enveloped(
                str(row.get("identification_id") or ""))

    rows.render_finding_row(item, lang, sidecar_version=sidecar_version,
                            load_children=loader, preview_url=preview_url,
                            catalogue_title=catalogue_title,
                            on_suppress=on_suppress,
                            load_excerpt=load_excerpt)


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
