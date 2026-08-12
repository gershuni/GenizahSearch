# -*- coding: utf-8 -*-
"""The discovery panel's WHOLE display model, as pure functions over the LIVE
service envelopes (Phase 136, plan 136-15, PANEL-01/PANEL-02).

Every display rule the panel obeys lives here, in one pure function, and none
of them lives inside a render call. That split is not tidiness: the rules are
where this surface is most likely to be quietly wrong, and every one of them
was found by looking at real data rather than by reasoning.

* The SAME work appeared twice under two titles on a real page (D-13a).
* One 66-letter span produced four separate liturgical "identifications"
  (D-13c).
* A verse-chain pulled three unrelated works onto byte-identical offsets
  (D-13d).
* Two rows a human had confirmed were treated differently on one manuscript,
  because the query dropped one of them before the predicate meant to protect
  it ever ran (D-13g).

THE INPUT IS THE ENVELOPE SET, NOT BARE ROWS
--------------------------------------------
`PanelServiceBundle` carries the FIVE live envelopes verbatim. Bare rows cannot
carry a status, so a caller holding only rows cannot tell an `ok`-with-zero from
an outage, cannot tell an unresolved page scope from an empty one, and cannot
tell a section nobody has asked for yet from a section with nothing in it. Those
are four different facts and the panel renders them four different ways.

The FIFTH field -- the related-page ROWS -- is optional, and `None` means NOT
REQUESTED. Those rows load only when the reader opens the toggle, so on a normal
page load there genuinely is no envelope to carry. Synthesising an
`ok`/`items=[]`/`total=0` to fill the slot would tell a reader "this page has no
related pages" on the strength of a query nobody ran -- the false-zero class
plan 136-14 fixed for claims, re-entering through a constructor. `None` is
rejected on the four EAGER fields for the mirror-image reason: a real outage
must never be laundered as "nobody asked".

WHAT THIS MODULE NEVER DOES
---------------------------
It never queries anything, never imports a UI toolkit, and never re-implements a
rule that already exists:

* bucket membership and both bucket names come from `shared.discovery_main_pool`
  (`bucket_label`), never from a second "is this good enough" rule here;
* collapse, lead attribution and granularity separation are
  `shared.discovery_grouping`'s ratified predicates, called and not restated;
* every reader-facing string is `shared.discovery_display_strings`;
* EVERY work title goes through `display_work_title` (owner ruling R). The
  service rows carry the RAW recorded title by design, so a model that formats
  `neutral_title` itself is not skipping a nicety -- on `w000176` it prints a
  halakhic work's name over pages the owner ruled are mostly liturgy;
* outage detection is `is_outage(envelope)` and zero detection is an explicit
  `status == ok and total == 0`; a truthiness test on an item list is a defect.

The ONE rule it does restate is D-13g's eligibility predicate (`routing_status
== 'shipped' OR adjudication_status == 'human_confirmed'`), and the reason is
the bug in the fourth bullet above: that row was dropped one layer down from
the predicate meant to protect it, because each layer assumed the other had
decided. This model is a pure function over envelopes a CALLER supplies, so
"the query already filtered" is an assumption here, not a fact. It checks the
predicate itself and refuses a row that satisfies neither limb -- unless the
claims envelope's own `meta.include_review` says the reader opted into the
review population, in which case such a row is admitted and can never reach
the default disclosure level.

There is deliberately NO human-review-marker field on any object this module
emits (D-13f): the badge is dropped until the provenance of those rows is
established, and the safest implementation of "no marker" is that the field does
not exist.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple

import scripts.discovery_ids as ids
import shared.discovery_display_strings as ds
import shared.discovery_grouping as grouping
from shared.discovery_main_pool import (
    REASON_MAIN_HUMAN_CONFIRMED,
    REASON_MAIN_MULTIFOLIO,
    SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS,
    bucket_label,
)
from shared.discovery_novelty import NOVELTY_STATUSES, is_hidden_by_default
from shared.discovery_relation_matrix import NEVER_RENDERED_IN_V1
from shared.discovery_surface_projection import (
    STATUS_OK,
    SURFACE_STATUSES,
    is_outage,
)

# ---------------------------------------------------------------------------
# The envelope contract this model consumes.
# ---------------------------------------------------------------------------

#: The FOUR keys every discovery read returns, always.
ENVELOPE_KEYS: frozenset = frozenset({"status", "items", "total", "meta"})

#: The four envelopes the panel fetches EAGERLY on a page load.
EAGER_ENVELOPE_FIELDS: Tuple[str, ...] = (
    "claims", "page_ids", "manuscript_works", "related_count",
)

#: The fifth envelope, fetched only when the reader opens the related-pages
#: toggle. `None` here means NOT REQUESTED -- see the module docstring.
LAZY_ENVELOPE_FIELD: str = "related_rows"

ENVELOPE_FIELDS: Tuple[str, ...] = EAGER_ENVELOPE_FIELDS + (LAZY_ENVELOPE_FIELD,)

#: The `meta` key sets each LIVE read emits on `ok` -- one entry per PRODUCING
#: BRANCH, never a union of them. A drifted service shape must fail against
#: this, in the model's own contract test, rather than in a renderer.
#: `tests/test_discovery_panel_model.py` parses the producing functions and
#: asserts this table equals the set of shapes they actually build.
#:
#: Per branch, because a union proves nothing (code review 2B, finding 5): a
#: suite that collects keys from every successful return and compares once lets
#: each branch violate the declared shape while their union matches. That was
#: already happening -- `get_manuscript_works_enveloped` has TWO `ok` returns
#: and the unresolved-page-scope one omits `lang`, because it returns before it
#: has anything to report a language ABOUT. That is a real state distinction, so
#: it is declared here rather than normalized away in a producer this model does
#: not own; the model's own `_scope_state` reads exactly that branch's key.
LIVE_OK_META_SHAPES: Mapping[str, Tuple[frozenset, ...]] = {
    "claims": (
        frozenset({"page_id", "include_review"}),
    ),
    "page_ids": (
        frozenset({"sys_id", "resolved", "truncated", "volume_ie"}),
    ),
    "manuscript_works": (
        frozenset({"page_scope_resolved", "lang"}),   # the ordinary read
        frozenset({"page_scope_resolved"}),           # the page scope did not resolve
    ),
    "related_count": (
        frozenset({"unit"}),
    ),
    "related_rows": (
        frozenset({"unit"}),
    ),
}

# ---------------------------------------------------------------------------
# The emitted state vocabularies. Each is CLOSED and pairwise distinct.
# ---------------------------------------------------------------------------

#: The related-pages ROW state -- four values, and the first one is why the
#: fifth envelope field is optional at all.
ROWS_NOT_REQUESTED = "not_requested"
ROWS_POPULATED = "populated"
ROWS_EMPTY = "empty"
ROWS_OUTAGE = "outage"

SECTION_ROW_STATES: frozenset = frozenset({
    ROWS_NOT_REQUESTED, ROWS_POPULATED, ROWS_EMPTY, ROWS_OUTAGE,
})

#: The manuscript pane's own state. `PANE_UNRESOLVED` is deliberately NOT
#: `PANE_EMPTY`: an unresolved page scope is a statement about OUR plumbing,
#: and rendering it as "this manuscript has nothing elsewhere" attributes our
#: failure to the manuscript.
PANE_POPULATED = ROWS_POPULATED
PANE_EMPTY = ROWS_EMPTY
PANE_OUTAGE = ROWS_OUTAGE
PANE_UNRESOLVED = "unresolved_scope"

#: The page scope's own state -- four values, and `resolved` / `truncated` are
#: facts SEPARATE from an empty result (`ManuscriptPageIds`' own contract).
SCOPE_RESOLVED = "resolved"
SCOPE_TRUNCATED = "truncated"
SCOPE_UNRESOLVED = "unresolved"
SCOPE_OUTAGE = "scope_outage"

SCOPE_STATES: Tuple[str, ...] = (
    SCOPE_RESOLVED, SCOPE_TRUNCATED, SCOPE_UNRESOLVED, SCOPE_OUTAGE,
)

#: The closed status vocabulary as an ORDERED tuple, so the arbitration table
#: below and the suite that reads it enumerate it identically.
SURFACE_STATUSES_ORDERED: Tuple[str, ...] = tuple(sorted(SURFACE_STATUSES))

#: The three disclosure levels D-13e ratified, plus ruling F's FOURTH. Level 2
#: holds the generic identical-span groups and the related-pages section and is
#: explicitly NOT identifications.
#:
#: The fourth is ORTHOGONAL to the first three rather than weaker than them
#: (`136-GATE1-DECISIONS.md` section F): it holds the claims that contradict a
#: catalogue identification, at whatever strength, and it is hidden by default
#: behind an explicitly warned toggle. Ruling F names the panel plans
#: (136-15/136-17) that must build it, and the same ruling's own rationale is
#: why it is a LEVEL and not a badge -- the system never treats the catalogue's
#: disagreement as a verdict, it surfaces the disagreement and lets the reader
#: decide, which is a decision a reader can only make before opening it.
LEVEL_IDENTIFICATIONS = "identifications"
LEVEL_ALSO_SHARES_TEXT = ds.TOGGLE_ALSO_SHARES_TEXT
LEVEL_MORE_MATCHES = ds.TOGGLE_MORE_MATCHES
LEVEL_DIVERGENCE = ds.TOGGLE_DIVERGENCE

DISCLOSURE_LEVEL_KEYS: Tuple[str, ...] = (
    LEVEL_IDENTIFICATIONS, LEVEL_ALSO_SHARES_TEXT, LEVEL_MORE_MATCHES,
    LEVEL_DIVERGENCE,
)

#: The pipeline's named steps. ORDER IS LOAD-BEARING and the names exist so a
#: test can assert the order in the source: pulling out generic groups before
#: collapsing duplicates is exactly the mistake that cost one manuscript a
#: correct identification.
STEP_COLLAPSE_DUPLICATES = "STEP 1 -- collapse duplicate canonical works"
STEP_SEPARATE_GENERIC_GROUPS = "STEP 2 -- separate identical-span generic groups"
STEP_LEAD_ATTRIBUTION = "STEP 3 -- lead attribution over what remains"
STEP_GATE_SHORT_EVIDENCE = "STEP 4 -- gate short-evidence rows"

PIPELINE_STEPS: Tuple[str, ...] = (
    STEP_COLLAPSE_DUPLICATES,
    STEP_SEPARATE_GENERIC_GROUPS,
    STEP_LEAD_ATTRIBUTION,
    STEP_GATE_SHORT_EVIDENCE,
)

#: Fields whose VALUES are machine vocabulary by design -- they feed a later
#: query or a renderer branch and are never rendered as text. Every OTHER
#: emitted field is swept for raw stored vocabulary keys by
#: `tests/test_discovery_panel_model.py`. Declaring the exception explicitly is
#: what keeps that sweep able to fail.
MACHINE_VOCABULARY_FIELDS: frozenset = frozenset({
    "main_pool_reason",
    #: C-track step 3d: was `anchor_claim_type`. The descriptor now carries the
    #: anchor's CAPPED rendered relation, because that is what the pane's anchor
    #: chip shows and what `relations_differ` compares. Still machine vocabulary
    #: -- it feeds a later query and a renderer branch, and reaches a reader only
    #: through `relation_chip`.
    "anchor_rendered_relation",
    "anchor_evidence_source",
    "anchor_confidence_band",
    "status",
    "reason",
})

#: A (source, band) pair outside the frozen lattice sorts last, never first.
_UNRANKED_BAND_RANK = 10 ** 6

#: Paging appears above six named works: one sampled manuscript carries 61
#: works elsewhere, and a chip list that long stops being a reader aid.
MANUSCRIPT_PANE_PAGE_THRESHOLD = 6

#: The page size the renderer asks the work-expansion wrapper for. The
#: expansion is a DESCRIPTOR, never loaded rows: the heaviest work has
#: thousands of claim rows while the median manuscript carries one work, so
#: eager loading pays the worst case to serve the common one.
EXPANSION_PAGE_SIZE = 20


# ---------------------------------------------------------------------------
# Status arbitration -- explicit and TOTAL.
#
# One row per (claims status, page-scope state) combination the panel can
# receive. The failure this prevents is the one plan 136-14 found on the real
# pre-rebuild asset: a failing query reporting `ok` with a total of zero, on a
# surface whose rule is to hide itself on a zero. The envelope now NAMES the
# failure; this table is where naming it turns into behaviour, and the suite
# reads the table rather than restating it.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ArbitrationOutcome:
    """What one (claims status, page-scope state) combination decides.

    `panel_status` is the claims envelope's own status -- the PANEL's status is
    never invented from a section's. `panel_hidden_on_zero` says whether the
    entry-control's hide rule may apply at all (it may only when NOTHING the
    panel would show came back as an outage, and then only when the claims
    total is genuinely zero). `pane_reports_manuscript_facts` is False whenever
    the page scope did not resolve: the pane may then report nothing ABOUT the
    manuscript, because anything it said would be our own plumbing failure
    wearing the manuscript's name.
    """

    panel_status: str
    panel_hidden_on_zero: bool
    scope_state: str
    pane_reports_manuscript_facts: bool


ARBITRATION_TABLE: Mapping[Tuple[str, str], ArbitrationOutcome] = {
    # claims `ok` -- the panel renders; the entry control may hide on a TRUE zero
    ("ok", SCOPE_RESOLVED): ArbitrationOutcome("ok", True, SCOPE_RESOLVED, True),
    ("ok", SCOPE_TRUNCATED): ArbitrationOutcome("ok", True, SCOPE_TRUNCATED, True),
    # SCOPE_UNRESOLVED is a FACT about the manuscript (no resolvable page
    # scope), not a failure, so a claims-level true zero may still hide.
    ("ok", SCOPE_UNRESOLVED): ArbitrationOutcome("ok", True, SCOPE_UNRESOLVED, False),
    # SCOPE_OUTAGE is a FAILURE. The claims zero is true of THIS PAGE, but the
    # panel's other pane is about the whole MANUSCRIPT and its read did not come
    # back -- so hiding here says "this manuscript has nothing" on the strength
    # of a query that failed. That is F-14's false zero relocated to the page-ID
    # read, and it also puts the only retry out of the reader's reach: with the
    # control hidden there is no way to open the panel that carries it
    # (code review round 12, finding 2).
    ("ok", SCOPE_OUTAGE): ArbitrationOutcome("ok", False, SCOPE_OUTAGE, False),
    # claims `unavailable` -- an outage is NEVER a zero; the control stays visible
    ("unavailable", SCOPE_RESOLVED): ArbitrationOutcome("unavailable", False, SCOPE_RESOLVED, True),
    ("unavailable", SCOPE_TRUNCATED): ArbitrationOutcome("unavailable", False, SCOPE_TRUNCATED, True),
    ("unavailable", SCOPE_UNRESOLVED): ArbitrationOutcome("unavailable", False, SCOPE_UNRESOLVED, False),
    ("unavailable", SCOPE_OUTAGE): ArbitrationOutcome("unavailable", False, SCOPE_OUTAGE, False),
    # claims `timeout`
    ("timeout", SCOPE_RESOLVED): ArbitrationOutcome("timeout", False, SCOPE_RESOLVED, True),
    ("timeout", SCOPE_TRUNCATED): ArbitrationOutcome("timeout", False, SCOPE_TRUNCATED, True),
    ("timeout", SCOPE_UNRESOLVED): ArbitrationOutcome("timeout", False, SCOPE_UNRESOLVED, False),
    ("timeout", SCOPE_OUTAGE): ArbitrationOutcome("timeout", False, SCOPE_OUTAGE, False),
    # claims `busy`
    ("busy", SCOPE_RESOLVED): ArbitrationOutcome("busy", False, SCOPE_RESOLVED, True),
    ("busy", SCOPE_TRUNCATED): ArbitrationOutcome("busy", False, SCOPE_TRUNCATED, True),
    ("busy", SCOPE_UNRESOLVED): ArbitrationOutcome("busy", False, SCOPE_UNRESOLVED, False),
    ("busy", SCOPE_OUTAGE): ArbitrationOutcome("busy", False, SCOPE_OUTAGE, False),
}

# Import-time totality guard: a combination missing from the table is a state
# nobody decided on, and those are exactly the ones that ship wrong.
_EXPECTED_ARBITRATION_KEYS = {
    (status, scope) for status in SURFACE_STATUSES_ORDERED for scope in SCOPE_STATES
}
if set(ARBITRATION_TABLE) != _EXPECTED_ARBITRATION_KEYS:  # pragma: no cover -- structural
    raise RuntimeError(
        "ARBITRATION_TABLE is not total over (status x page-scope state); missing "
        "%s" % sorted(_EXPECTED_ARBITRATION_KEYS - set(ARBITRATION_TABLE))
    )


# ---------------------------------------------------------------------------
# Error paths. A TYPE distinction, not a list of audited message sites.
# ---------------------------------------------------------------------------


class PanelContractError(ValueError):
    """A refusal this module composed ITSELF, and whose text is therefore safe
    to log.

    Everything this model reads -- an envelope, a claim row, a work summary, a
    title -- is artifact content, and the artifact may carry restricted
    (M-source / R-source) text. The standing rule is that such content never
    reaches a log; a caller that does `logger.exception(...)` around
    `build_panel_rows` must not thereby publish a row it was handed.

    Patching the interpolations Codex cited is NOT enough, and the loader next
    door (`web/discovery_assets.py`, code review 2A finding 1) is why we know:
    two paths there carried artifact text into a log without a single f-string
    appearing in the file -- `int(x)` raising ``invalid literal for int() with
    base 10: '<raw value>'``, and `OSError` naming a path built from the
    manifest's own basename. An audit that greps for f-strings cannot find
    either.

    So the rule here is a type, not a habit. Every deliberate refusal below
    raises THIS class with a message composed from fixed error CODES, this
    module's own constants, FIELD NAMES and COUNTS -- never a value. Anything
    else that escapes -- a `KeyError` naming its key, a library `ValueError`
    naming its input, a future raise site nobody audits -- is caught at the two
    public boundaries (`PanelServiceBundle.__post_init__` and
    `build_panel_rows`) and re-raised as THIS class carrying only the offending
    exception's TYPE NAME, with the chain severed (`from None`) so the original
    message cannot reappear in a formatted traceback.

    It subclasses `ValueError` because that is the contract callers already
    have, and because a refusal IS a statement about a bad value -- just never
    a quotation of one.
    """


#: The residue a reduced exception is allowed to carry. A Python type's name is
#: CODE, never artifact content -- the same residue `web/discovery_assets.py`
#: settled on for its fail-closed handler.
_CODE_UNAUDITED_RAISE = "model_internal_refusal"


def _reduce_to_type(exc: BaseException, where: str) -> "PanelContractError":
    """Re-raise material for an exception this module did not compose."""
    return PanelContractError(
        "%s: %s raised %s while %s -- detail withheld, because the message may "
        "quote artifact content this model was handed"
        % (_CODE_UNAUDITED_RAISE, type(exc).__module__, type(exc).__name__, where)
    )


def _int_or_refuse(value: Any, field_name: str) -> int:
    """`int(value)`, with the conversion's own error message suppressed.

    `int('<raw value>')` names its input in the exception it raises. That is
    the exact path that survived the first fix on the sidecar loader, and there
    is no f-string here for an auditor to find.
    """
    try:
        return int(value)
    except (TypeError, ValueError):
        raise PanelContractError(
            "field_not_an_integer: field %r is not convertible to an integer "
            "(value withheld)" % (field_name,)
        ) from None


# ---------------------------------------------------------------------------
# The input bundle.
# ---------------------------------------------------------------------------


def _validate_envelope(field_name: str, value: Any, *, optional: bool) -> None:
    """Reject anything that is not a four-key envelope.

    `optional` is True for exactly ONE field (the lazy related-page rows), where
    `None` is a meaningful value. Everywhere else `None` raises: a bundle that
    accepted `None` on an eager field would let a real outage be dropped on the
    floor as "nobody asked for this".

    The KEYS and the STATUS a caller supplies are untrusted content and are
    never named -- only counted, and only against this module's own expected
    set (see `PanelContractError`).
    """
    if value is None:
        if optional:
            return
        raise PanelContractError(
            "envelope_none_on_eager_field: PanelServiceBundle.%s is fetched "
            "eagerly, so accepting None would hide a real outage behind 'not "
            "requested' (only %s may be None)" % (field_name, LAZY_ENVELOPE_FIELD)
        )
    if not isinstance(value, Mapping):
        raise PanelContractError(
            "envelope_not_a_mapping: PanelServiceBundle.%s expected the four-key "
            "envelope %s, got a %s -- a bare list cannot say whether it is empty "
            "because the manuscript is empty or because the service failed"
            % (field_name, sorted(ENVELOPE_KEYS), type(value).__name__)
        )
    supplied = set(value)
    if supplied != ENVELOPE_KEYS:
        raise PanelContractError(
            "envelope_key_set_mismatch: PanelServiceBundle.%s does not carry "
            "exactly the live shape %s -- missing %s, plus %d unexpected key(s) "
            "(their names are caller content and are withheld)"
            % (field_name, sorted(ENVELOPE_KEYS),
               sorted(ENVELOPE_KEYS - supplied), len(supplied - ENVELOPE_KEYS))
        )
    if value["status"] not in SURFACE_STATUSES:
        raise PanelContractError(
            "envelope_status_outside_vocabulary: PanelServiceBundle.%s carries a "
            "status outside the closed vocabulary %s (found value withheld)"
            % (field_name, sorted(SURFACE_STATUSES))
        )


@dataclass(frozen=True)
class PanelServiceBundle:
    """The model's ONLY input: the five live envelopes, verbatim.

    `claims`, `page_ids`, `manuscript_works` and `related_count` are REQUIRED.
    `related_rows` defaults to `None`, which means NOT REQUESTED -- the reader
    has not opened the related-pages toggle, so the query was never issued.
    """

    claims: Mapping[str, Any]
    page_ids: Mapping[str, Any]
    manuscript_works: Mapping[str, Any]
    related_count: Mapping[str, Any]
    related_rows: Optional[Mapping[str, Any]] = None
    lang: str = "en"
    show_more: bool = False
    #: Ruling F's fourth level, opened. DEFAULT FALSE, and the default is the
    #: ruling: a caller that has never heard of divergence gets the hidden
    #: posture rather than its opposite.
    show_divergence: bool = False

    def __post_init__(self) -> None:
        # One of the two public boundaries. Our own refusals pass through
        # verbatim; anything else -- a Mapping whose __iter__ raises, a future
        # check nobody audits -- is reduced to its type name so it cannot carry
        # a caller-supplied value into a log. See `PanelContractError`.
        try:
            for field_name in EAGER_ENVELOPE_FIELDS:
                _validate_envelope(field_name, getattr(self, field_name), optional=False)
            _validate_envelope(
                LAZY_ENVELOPE_FIELD, getattr(self, LAZY_ENVELOPE_FIELD), optional=True)
        except PanelContractError:
            raise
        except Exception as exc:
            raise _reduce_to_type(exc, "validating the envelope set") from None

    def with_related_rows(self, envelope: Mapping[str, Any]) -> "PanelServiceBundle":
        """A copy carrying the lazily-fetched related-page rows -- what a
        surface builds when the reader opens the toggle."""
        return dataclasses.replace(self, related_rows=envelope)


# ---------------------------------------------------------------------------
# Envelope readers. `is_outage` and an EXPLICIT zero test, never `not items`.
# ---------------------------------------------------------------------------


def _is_ok(envelope: Mapping[str, Any]) -> bool:
    return envelope.get("status") == STATUS_OK


def _envelope_total(envelope: Mapping[str, Any]) -> int:
    """The envelope's own total. `total` is artifact content, so the
    conversion goes through `_int_or_refuse` rather than letting `int()` name
    the value it choked on."""
    return _int_or_refuse(envelope.get("total") or 0, "total")


def _is_ok_zero(envelope: Mapping[str, Any]) -> bool:
    """A SUCCESSFUL zero -- the only state the entry control hides on."""
    return _is_ok(envelope) and _envelope_total(envelope) == 0


def _items(envelope: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    return list(envelope.get("items") or [])


def _service_state(envelope: Mapping[str, Any], lang: str) -> Dict[str, Any]:
    """The temporary-unavailable copy plus its retry, for a section whose own
    envelope failed. An outage is a visible temporary state -- never an empty
    section, which reads as an authoritative zero."""
    status = envelope.get("status")
    state: Dict[str, Any] = {
        "status": status,
        "message": ds.service_state_message(status, lang),
        "retry": ds.retry_label(lang),
    }
    reason = (envelope.get("meta") or {}).get("reason")
    if reason is not None:
        state["reason"] = reason
    return state


def _scope_state(page_ids: Mapping[str, Any], works: Mapping[str, Any]) -> str:
    """Which of the four page-scope states the bundle describes.

    `resolved` and `truncated` are facts SEPARATE from an empty page list, and
    an `ok` manuscript-works envelope carrying `page_scope_resolved: False` is
    an `ok` envelope that is NOT a fact about the manuscript. A missing
    `resolved` key fails CLOSED to unresolved -- the broken implementation this
    guards against is one that finds the key absent on an outage envelope,
    treats it as present-but-falsy or defaults it to True, and then reports
    "nothing elsewhere in this manuscript" during an outage.
    """
    if is_outage(page_ids):
        return SCOPE_OUTAGE
    if (page_ids.get("meta") or {}).get("resolved") is not True:
        return SCOPE_UNRESOLVED
    if _is_ok(works) and (works.get("meta") or {}).get("page_scope_resolved") is False:
        return SCOPE_UNRESOLVED
    if (page_ids.get("meta") or {}).get("truncated") is True:
        return SCOPE_TRUNCATED
    return SCOPE_RESOLVED


# ---------------------------------------------------------------------------
# Ruling R: THE one site that reads a raw recorded title.
# ---------------------------------------------------------------------------


def _routed_title(row: Mapping[str, Any], lang: str) -> Tuple[str, bool]:
    """`(display title, title_missing)` for one row.

    This is the ONLY place in the module that reads the raw recorded title, and
    it hands it straight to `display_work_title` with the DISPLAY work id --
    `work_id` is the claim's own work and is not the id the curated table is
    keyed on. A grep in the suite pins the single call site.
    """
    display_id = row.get("display_work_id") or row.get("work_id")
    routed = ds.display_work_title(display_id, row.get("neutral_title"), lang)
    if not routed:
        return ds.missing_title(lang), True
    return routed, False


def _display_work_id(row: Mapping[str, Any]) -> Any:
    return row.get("display_work_id") or row.get("work_id")


def _band_rank(row: Mapping[str, Any], key: str = "band_rank") -> int:
    """The row's band rank, with an out-of-lattice/absent rank sorting LAST."""
    rank = row.get(key)
    return _UNRANKED_BAND_RANK if rank is None else _int_or_refuse(rank, key)


# ---------------------------------------------------------------------------
# STEP 1 -- collapse duplicate canonical works (D-13a).
# ---------------------------------------------------------------------------


def _split_confirmed(
    members: Sequence[Mapping[str, Any]]
) -> Tuple[List[Mapping[str, Any]], List[Mapping[str, Any]]]:
    """`(confirmed, rest)`, order preserved.

    Every lossy step below asks this question BEFORE it discards anything, so
    that "a human-confirmed row is emitted in the DEFAULT set" is a property of
    the pipeline rather than a repair applied to whatever survived it.
    """
    confirmed = [m for m in members if _is_human_confirmed(m)]
    rest = [m for m in members if not _is_human_confirmed(m)]
    return confirmed, rest


def _in_ratified_order(
    members: Sequence[Mapping[str, Any]]
) -> List[Mapping[str, Any]]:
    """`members` in `lead_attribution`'s total order -- the ratified order, over
    whatever subset the caller is entitled to choose from. Never a fresh
    tie-break."""
    if len(members) == 0:
        return []
    lead, remainder = grouping.lead_attribution(members)
    return [lead] + list(remainder)


def _collapse_duplicates(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """`grouping.collapse_canonical`, applied only to rows that HAVE a canonical
    work id.

    Rows whose `canonical_work_id` is absent pass through untouched: grouping
    them would key every one of them on the same missing value and collapse
    unrelated works into one row, which is the opposite of what D-13a asks for.

    CONFIRMATION PRECEDENCE (D-13g x D-13a). The collapse is lossy by design --
    the losing member is dropped from view entirely, "never merged, never
    averaged" -- so applied blind it can drop the one member a human confirmed
    in favour of a member nobody looked at, taking that row's claim identity and
    its coverage note with it. When a canonical group carries confirmed members,
    only those are offered to the collapse. The RATIFIED rule still picks the
    winner; this decides only what it picks from, so no tie-break is restated
    here. Between two confirmed members D-13a's own deterministic rule decides,
    and the survivor is confirmed either way.
    """
    with_canon = [row for row in rows if row.get("canonical_work_id")]
    without_canon = [dict(row) for row in rows if not row.get("canonical_work_id")]

    by_canon: Dict[Any, List[Mapping[str, Any]]] = {}
    for row in with_canon:
        by_canon.setdefault(row["canonical_work_id"], []).append(row)
    candidates: List[Mapping[str, Any]] = []
    for members in by_canon.values():
        confirmed, _rest = _split_confirmed(members)
        candidates.extend(confirmed if len(confirmed) > 0 else members)

    collapsed = grouping.collapse_canonical(candidates) if len(candidates) > 0 else []
    return list(collapsed) + without_canon


# ---------------------------------------------------------------------------
# STEP 2/3 -- identical-span groups: which leave, which collapse, who leads.
# ---------------------------------------------------------------------------


def _span_key(row: Mapping[str, Any]) -> Optional[Tuple[int, int]]:
    start, end = row.get("span_start"), row.get("span_end")
    if start is None or end is None:
        return None
    return (_int_or_refuse(start, "span_start"), _int_or_refuse(end, "span_end"))


def _group_by_span(
    rows: Sequence[Mapping[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[Tuple[int, int], List[Dict[str, Any]]]]:
    """Split `rows` into standalone rows and identical-span groups (>=2 rows on
    byte-identical offsets)."""
    standalone: List[Dict[str, Any]] = []
    groups: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}
    for row in rows:
        key = _span_key(row)
        if key is None:
            standalone.append(dict(row))
            continue
        groups.setdefault(key, []).append(dict(row))
    for key in [k for k, members in groups.items() if len(members) == 1]:
        standalone.append(groups.pop(key)[0])
    return standalone, groups


def _generic_group(
    key: Tuple[int, int], members: Sequence[Mapping[str, Any]], lang: str
) -> Dict[str, Any]:
    """One identical-span group of genuinely different works, as it leaves the
    identifications bucket entirely (D-13d).

    Several works claiming byte-identical text with identical matched length is
    the signature of generic shared text -- a verse-chain, a liturgical formula
    -- not of a witness.
    """
    letters = [m.get("matched_letters") for m in members if m.get("matched_letters") is not None]
    works = []
    for member in members:
        title, missing = _routed_title(member, lang)
        works.append({
            "work_id": _display_work_id(member),
            "work_title": title,
            "title_missing": missing,
            # C-track step 3b: the matrix output, capped at member grain -- not
            # the stored claim type. A group member is a claim row like any
            # other; the only thing this group changes is that it leaves the
            # identifications bucket, not what each row may assert.
            "relation_chip": ds.relation_chip(member.get("rendered_relation"), lang),
        })
    return {
        "kind": "generic_passage_group",
        "span_start": key[0],
        "span_end": key[1],
        "matched_letters": max(letters) if len(letters) > 0 else None,
        "work_count": len(members),
        "works": tuple(works),
        "note": ds.not_an_identification_note(lang),
    }


# ---------------------------------------------------------------------------
# STEP 4 -- the disclosure gate.
# ---------------------------------------------------------------------------


def _is_shipped(row: Mapping[str, Any]) -> bool:
    """D-13g limb 1, read from the row's OWN routing status."""
    return row.get("routing_status") == ids.ROUTING_STATUS_SHIPPED


def _is_human_confirmed(row: Mapping[str, Any]) -> bool:
    """D-13g limb 2, and ONLY the explicit adjudication limb.

    The earlier version of this predicate also accepted
    `restored_by_human_confirmation` and a `main_pool_reason` of
    `REASON_MAIN_HUMAN_CONFIRMED` as substitutes. Both are DERIVED from this
    limb -- the query computes `restored_by_human_confirmation` as
    `routing_status <> 'shipped' AND adjudication_status = 'human_confirmed'`,
    and the reason is materialized by the build from the same fact -- so
    accepting them let a row assert human confirmation that no human record
    backs, and promoted it into the DEFAULT level on that assertion. A derived
    field is evidence that the fact was recorded, never the record itself;
    `_validate_claim_row` fails loudly when the two disagree rather than
    quietly picking one.
    """
    return row.get("adjudication_status") == ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED


def _is_default_surface_eligible(row: Mapping[str, Any]) -> bool:
    """D-13g's eligibility predicate, EXACTLY: `routing_status == 'shipped'`
    OR `adjudication_status == 'human_confirmed'`.

    This is the same two-limb rule the page query materializes
    (`shared/discovery_service.py::_CLAIMS_DEFAULT_ROUTING_CLAUSE`) and the
    same one the build writes into `discovery_identification`. Restating it
    here is not duplication for its own sake: the model is a PURE function over
    envelopes a caller supplies, so "the query already filtered" is an
    assumption, and an assumption is precisely what let one manuscript's
    human-confirmed row be dropped one layer down from the predicate meant to
    protect it.
    """
    return _is_shipped(row) or _is_human_confirmed(row)


#: Closed stored vocabularies every input claim is checked against BEFORE any
#: predicate reads them, and before any of them is handed to a display-string
#: lookup. TWO reasons, and the second is the one Codex found:
#:
#: 1. Without it the two-limb eligibility test is not total -- a row carrying a
#:    routing status outside the frozen enum satisfies neither limb and neither
#:    its negation, and the four-value truth table has five values.
#: 2. `relation_chip`, `filter_code`, `row_headline` and `band_label` all raise
#:    with the offending value formatted into the message (`{!r}`). Those
#:    modules are right to do that -- their input domain is OUR vocabulary, not
#:    artifact content -- which makes it THIS module's job not to hand them a
#:    raw row value it has not checked. Checking here means the lookup cannot
#:    raise, and the refusal a caller sees is one we composed.
#:
#: The two groups differ in how they treat ABSENCE. A missing routing or
#: adjudication status is itself a refusal (reason 1). A missing anchor field is
#: the ALL-OR-NONE contract's business (`_anchor_identity`), which must speak
#: first so a short row is reported as a short row rather than as a vocabulary
#: failure -- so those are checked only for a value that is PRESENT and outside
#: the frozen set, which is exactly the case a display lookup would otherwise
#: raise on while quoting it.
_REQUIRED_CLAIM_VOCABULARIES: Tuple[Tuple[str, frozenset], ...] = (
    ("routing_status", ids.ROUTING_STATUSES),
    ("adjudication_status", ids.ADJUDICATION_STATUSES),
    #: ⟨ADDED 2026-08-12 -- C-track step 3b⟩ Contract 1's matrix output, capped
    #: at member grain. REQUIRED, not anchor-optional, for reason 2 above: every
    #: row hands this value to `relation_chip` and `row_headline`
    #: unconditionally, so an absent or foreign value would reach a lookup that
    #: raises while quoting it. Note this is the RENDERED vocabulary (five
    #: states, `uncertain` among them), not `CLAIM_TYPES` -- the fail-closed
    #: state is a legitimate value here and refusing it would refuse exactly the
    #: rows the matrix exists to soften.
    ("rendered_relation", ids.RENDERED_RELATIONS),
)

#: ⟨CHANGED 2026-08-12 -- C-track step 3d⟩ `relation_kind` is GONE from this
#: list, because it is gone from the claim surface: step 3d retired its last
#: consumer (the expansion query's anchor), so `SURFACE_CLAIM_FIELDS` no longer
#: carries it and a vocabulary check on an absent field would be a check on
#: nothing. The anchor's relation is validated by `rendered_relation` in
#: `_REQUIRED_CLAIM_VOCABULARIES` above -- REQUIRED there rather than optional
#: here, which is strictly stronger.
_ANCHOR_CLAIM_VOCABULARIES: Tuple[Tuple[str, frozenset], ...] = (
    ("evidence_source", ids.EVIDENCE_SOURCES),
)


def _validate_claim_row(row: Mapping[str, Any], *, include_review: bool) -> None:
    """The D-13g contract on ONE input claim, checked at the model boundary.

    `include_review` comes from the claims envelope's own `meta` -- the
    producer's record of WHICH population it returned. On the default surface
    (`include_review` false) a row failing both eligibility limbs is a producer
    -contract violation and is REFUSED, not silently dropped: dropping it would
    reproduce, in the display layer, exactly the class of bug D-13g exists to
    fix -- a row disappearing between two layers that each assumed the other
    had decided. Under `include_review` such a row is the requested population;
    it is admitted and can never reach the default level.

    Every message below names a fixed CODE and FIELD NAMES only. No value read
    out of a row is ever interpolated: a claim row is artifact content, and a
    refusal that quotes it puts restricted text one `logger.exception` away
    from a log file.
    """
    # The anchor's ALL-OR-NONE contract speaks first, and it speaks about EVERY
    # input claim (code review 2B, finding 2). Called only from
    # `_identification_row`, it ran for the eventual LEAD identification and for
    # nothing else -- so a partial anchor on a generic-group member reached a
    # display-string lookup before any contract saw it, a partial anchor on a
    # nested granularity row was never validated at all, and a partial anchor on
    # a row the canonical collapse discards was never even reached. Validating
    # only what survives grouping is not a contract; it is a sample.
    _anchor_identity(row)

    for field_name, vocabulary in _REQUIRED_CLAIM_VOCABULARIES:
        if row.get(field_name) not in vocabulary:
            raise PanelContractError(
                "claim_vocabulary_outside_closed_set: field %r is missing or "
                "outside its frozen vocabulary (value withheld); the closed set "
                "has %d members" % (field_name, len(vocabulary))
            )
    for field_name, vocabulary in _ANCHOR_CLAIM_VOCABULARIES:
        value = row.get(field_name)
        if value is not None and value not in vocabulary:
            raise PanelContractError(
                "claim_vocabulary_outside_closed_set: field %r carries a value "
                "outside its frozen vocabulary (value withheld); the closed set "
                "has %d members" % (field_name, len(vocabulary))
            )
    # The band is only meaningful WITH its source (`band_label` is keyed on the
    # pair), so the pair is what gets checked -- a band alone cannot produce a
    # rank and would silently compare against a default. Absence stays the
    # all-or-none contract's business, as above.
    if row.get("evidence_source") is not None and row.get("confidence_band") is not None \
            and row.get("confidence_band") not in ids.CONFIDENCE_BANDS_BY_SOURCE.get(
                row.get("evidence_source"), frozenset()):
        raise PanelContractError(
            "claim_vocabulary_outside_closed_set: fields 'evidence_source' and "
            "'confidence_band' are not a pair in the frozen band lattice "
            "(values withheld)"
        )

    if not include_review and not _is_default_surface_eligible(row):
        raise PanelContractError(
            "claim_ineligible_for_default_surface: this claim satisfies neither "
            "limb of D-13g (routing_status == 'shipped' OR adjudication_status "
            "== 'human_confirmed') and the claims envelope's meta.include_review "
            "is false, so the producer should never have returned it (values "
            "withheld)"
        )

    # The derived confirmation markers must AGREE with the explicit limb.
    # `restored_by_human_confirmation` is exactly derivable, so both directions
    # are checked; `main_pool_reason` is an identification-level fact that may
    # legitimately be something else for a confirmed row, so only the direction
    # that would let it stand IN for the limb is.
    if bool(row.get("restored_by_human_confirmation")) != (
        _is_human_confirmed(row) and not _is_shipped(row)
    ):
        raise PanelContractError(
            "claim_derived_confirmation_inconsistent: field "
            "'restored_by_human_confirmation' disagrees with its own definition "
            "over 'adjudication_status' and 'routing_status' (values withheld)"
        )
    if row.get("main_pool_reason") == REASON_MAIN_HUMAN_CONFIRMED \
            and not _is_human_confirmed(row):
        raise PanelContractError(
            "claim_derived_confirmation_inconsistent: field 'main_pool_reason' "
            "asserts human confirmation that field 'adjudication_status' does "
            "not record (values withheld)"
        )


def _is_catalogue_divergent(row: Mapping[str, Any]) -> bool:
    """Whether this claim contradicts a catalogue identification (ruling F).

    DERIVED from `shared.discovery_novelty.is_hidden_by_default`, never from a
    restated shade list -- that predicate is the policy, and a second membership
    test here would be a second policy the first one could not move.

    Refuses an out-of-vocabulary shade rather than treating it as undivergent.
    The vocabulary is frozen by the schema's own CHECK constraint and by the
    release verifier's frozen-enum check, so an unrecognized value here means
    the artifact is not the one this code was written against -- and the
    fail-quiet reading of that would silently put an unclassifiable row in the
    DEFAULT view, which is the one place ruling F says it must not be.
    """
    status = row.get("novelty_status")
    try:
        return is_hidden_by_default(status)
    except ValueError:
        raise PanelContractError(
            "claim_vocabulary_outside_closed_set: field 'novelty_status' carries "
            "a value outside the frozen novelty vocabulary (value withheld); the "
            "closed set has %d members" % len(NOVELTY_STATUSES)
        ) from None


def _disclosure_level_for(row: Mapping[str, Any]) -> str:
    """Which disclosure level one identification row belongs to.

    Nothing is deleted here: a row that fails every test below is GATED behind
    a toggle and stays reachable. Bucket membership itself is the materialized
    shared-rule decision on the row (`main_pool`), never recomputed.

    DIVERGENCE IS TESTED FIRST, and the order is the ruling. Ruling F's axis is
    ORTHOGONAL to the other three -- a catalogue-divergent claim can be
    main-pool, default-eligible and long-evidence, i.e. a level-1 row by every
    other test -- so a divergence test placed anywhere but first would leave
    exactly the strongest divergent rows in the default view, which is the one
    place the ruling says they must not be.

    It is tested BEFORE the human-confirmation carve-out too. Human
    confirmation adjudicates the CLAIM; it does not adjudicate the
    DISAGREEMENT, and `divergence_correctness` -- the only field that could --
    is human-only (ruling L) and NULL on every shipped row. Reading a confirmed
    claim as a settled divergence would be this module supplying the verdict
    ruling F says nobody has reached.
    """
    if _is_catalogue_divergent(row):
        return LEVEL_DIVERGENCE
    if not _is_default_surface_eligible(row):
        # Only reachable under `include_review`. Stated explicitly rather than
        # left to fall through the `main_pool` test below, which would be the
        # same substitution of a derived field for the eligibility rule.
        return LEVEL_MORE_MATCHES
    if _is_human_confirmed(row):
        return LEVEL_IDENTIFICATIONS
    if row.get("main_pool") is not True:
        return LEVEL_MORE_MATCHES
    if row.get("default_eligible") is not True:
        return LEVEL_MORE_MATCHES
    matched = row.get("matched_letters")
    if (
        matched is not None
        and _int_or_refuse(matched, "matched_letters") < SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS
        and row.get("main_pool_reason") != REASON_MAIN_MULTIFOLIO
    ):
        # D-13c, with its ratified carve-out: the short-evidence floor never
        # applies to an identification already qualified via multi-folio
        # agreement. A short liturgical passage may be exactly the correct
        # identification for a prayer book, so the row is demoted, never
        # deleted.
        return LEVEL_MORE_MATCHES
    return LEVEL_IDENTIFICATIONS


# ---------------------------------------------------------------------------
# Row composition.
# ---------------------------------------------------------------------------


def _anchor_identity(row: Mapping[str, Any]) -> Dict[str, Any]:
    """The four anchor-identity fields the work expansion needs, ALL-OR-NONE.

    Called TWICE by design, and the first call is the load-bearing one:
    `_validate_claim_row` runs it over every input claim in STEP 0, and
    `_identification_row` runs it again on the lead to build the descriptor. It
    is a pure read, so the second call costs a dict and buys the descriptor its
    values from the same contract that admitted the row.

    Checked before any display string is composed, so this contract is the one
    that speaks when a row is short of it -- otherwise a missing band or source
    surfaces as a label-lookup error and the all-or-none rule never runs. The
    ranking the expansion wrapper does needs the anchor's source AND its band
    (a band alone cannot produce a rank and would silently compare against a
    default), so a partial set raises rather than reaching a query that would
    quietly answer the wrong question.
    """
    anchor = {
        "anchor_sys_id": row.get("sys_id"),
        # C-track step 3d: the anchor travels as its CAPPED rendered relation,
        # not as its stored `relation_kind`. The expansion pane renders this
        # value as the anchor's chip and compares it against the carrier's own
        # capped relation, so sending the stored type would put one surface's
        # two chips in two different vocabularies. It is already capped -- step
        # 3b capped it against this claim's identification -- so nothing here
        # re-applies §3.2.
        "anchor_rendered_relation": row.get("rendered_relation"),
        "anchor_evidence_source": row.get("evidence_source"),
        "anchor_confidence_band": row.get("confidence_band"),
    }
    missing = sorted(name for name, value in anchor.items() if value is None)
    if len(missing) > 0:
        present = sorted(name for name, value in anchor.items() if value is not None)
        # The FIELD NAMES are ours; the claim id is artifact content and is not
        # named, so this refusal identifies the contract that failed without
        # quoting the row that failed it.
        raise PanelContractError(
            "claim_anchor_identity_partial: the anchor identity is all-or-none "
            "-- present %s, missing %s (the claim's own id is withheld)"
            % (present, missing)
        )
    return anchor


def _expansion_descriptor(
    row: Mapping[str, Any], work_title: str, lang: str, anchor: Mapping[str, Any]
) -> Dict[str, Any]:
    """The "other manuscripts matching this work" expansion, as a DESCRIPTOR the
    renderer can request lazily -- never as loaded rows."""
    descriptor: Dict[str, Any] = {
        "work_id": _display_work_id(row),
        "heading": ds.section_header(ds.SECTION_OTHER_MANUSCRIPTS, lang, work_title),
        "page_size": EXPANSION_PAGE_SIZE,
        "loaded": False,
    }
    descriptor.update(anchor)
    return descriptor


def _nested_entry(row: Mapping[str, Any], lang: str) -> Dict[str, Any]:
    title, missing = _routed_title(row, lang)
    return {
        "work_id": _display_work_id(row),
        "work_title": title,
        "title_missing": missing,
        "subline": ds.granularity_subline(title, lang),
    }


def _identification_row(
    row: Mapping[str, Any], nested: Sequence[Mapping[str, Any]], lang: str
) -> Dict[str, Any]:
    """One emitted identification row.

    An optional field is ABSENT rather than None when it does not apply: a
    propagated row emits no coverage field at all, because coverage is a
    DIRECT-family measurement and a null there is one careless renderer away
    from reading as zero coverage.
    """
    anchor = _anchor_identity(row)
    title, missing = _routed_title(row, lang)
    # C-track step 3b, narrowed by 3d: ONE relation feeds every display decision
    # below -- the chip, the headline, the filter code and D-08a's percentage
    # gate -- and it is the matrix output capped at member grain. The stored
    # claim type is no longer on this surface at all: step 3d retired its last
    # consumer, `_anchor_identity` above, which now carries this same capped
    # value into the expansion query.
    rendered_relation = row.get("rendered_relation")
    evidence_source = row.get("evidence_source")
    in_main_pool = row.get("main_pool") is True
    level = _disclosure_level_for(row)

    emitted: Dict[str, Any] = {
        "kind": "identification",
        "claim_id": row.get("claim_id"),
        "page_id": row.get("page_id"),
        "sys_id": row.get("sys_id"),
        "work_id": _display_work_id(row),
        "work_title": title,
        "title_missing": missing,
        "headline": ds.row_headline(
            title, row.get("coverage_ppm"), rendered_relation, lang,
            evidence_source=evidence_source),
        "relation_chip": ds.relation_chip(rendered_relation, lang),
        "band_tooltip": ds.relation_tooltip(
            evidence_source, row.get("confidence_band"), lang),
        "in_main_pool": in_main_pool,
        "bucket": bucket_label(in_main_pool, lang),
        "main_pool_reason": row.get("main_pool_reason"),
        "disclosure_level": level,
        "gated": level != LEVEL_IDENTIFICATIONS,
        "span_start": row.get("span_start"),
        "span_end": row.get("span_end"),
        "matched_letters": row.get("matched_letters"),
        "nested": tuple(_nested_entry(other, lang) for other in nested),
        "expansion": _expansion_descriptor(row, title, lang, anchor),
    }

    # The relation FILTER's short code. Emitted only where one exists: the three
    # stored claim types have codes, the fail-closed state does not, and
    # inventing a fourth code would put "Needs review" into a reader-facing
    # filter set nobody has ruled on. An ABSENT key rather than a null, like
    # every other conditional field here -- and `filter_code` still raises on
    # anything it does not know, so the branch is a decision, not a swallow.
    if rendered_relation in ids.CLAIM_TYPES:
        emitted["relation_code"] = ds.filter_code(rendered_relation)

    # D-08a: the ONE permitted percentage, direct family only, always with its
    # matched-letter qualifier (which `row_headline` composes). Gated on the
    # RENDERED relation for the same reason the findings page is: a row the
    # matrix demoted must not go on advertising "68% of page" beside a chip that
    # says it shares text.
    if (
        rendered_relation == ids.RENDERED_RELATION_DIRECT_WITNESS
        and evidence_source != ids.EVIDENCE_SOURCE_PROPAGATED
        and row.get("coverage_ppm") is not None
    ):
        emitted["coverage_ppm"] = row.get("coverage_ppm")
        emitted["coverage_label"] = ds.coverage_label(lang)

    if bool(row.get("low_coverage_marker")) or bool(row.get("restored_by_human_confirmation")):
        # D-13g/D-13f: the note is about COVERAGE, not review. No field on this
        # object asserts human review of any kind.
        emitted["low_coverage_note"] = ds.low_coverage_note(lang)

    if len(emitted["nested"]) > 0:
        emitted["granularity_subline"] = emitted["nested"][0]["subline"]

    return emitted


def _compose_rows(
    service_rows: Sequence[Mapping[str, Any]], lang: str, *, include_review: bool
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """The pipeline, in the ONE order that is correct.

    Applying these in a different order changes what a reader sees.

    CONFIRMATION PRECEDENCE runs INSIDE the lossy steps, not after them (code
    review 2B, finding 4). D-13g's contract -- "a human-confirmed row is
    emitted in the default set" -- is unconditional, but preservation used to be
    a property of `_disclosure_level_for`, which sees only the rows that already
    survived the collapse and the generic separation. Both of those discard
    rows. So a confirmed row could lose the collapse to a member nobody looked
    at, or vanish into a generic group, and the predicate meant to protect it
    never ran -- the same shape as the query bug D-13g was written for, one
    layer further in. Three places ask the question first:

    * STEP 1 offers the collapse only the confirmed members of a canonical
      group (`_collapse_duplicates`);
    * a GENERIC group's confirmed members are lifted out and emitted as
      identification rows, and the group keeps the rest;
    * a GRANULARITY group's lead is chosen from among its confirmed members
      when there are any, so the confirmed row keeps its claim identity, its
      bucket and its coverage note while the ratified nesting survives -- and
      any FURTHER confirmed member is lifted out rather than nested, because a
      nested row is emitted as a title and a subline, which is not "emitted in
      the default set".

    The GENERIC verdict itself is taken over the group's FULL membership,
    before any lift: whether a passage is generic shared text is a fact about
    the passage, and letting one confirmation change the verdict for the others
    would make a display rule depend on who happened to have been reviewed. A
    group left with a single member after the lift stops being a group, exactly
    as a single-member span already does in `_group_by_span`.
    """
    # STEP 0 -- the input contract, on EVERY claim, before any step below reads
    # one. A validation that runs only on the rows that survive grouping is not
    # a contract: it is a sample of one.
    for row in service_rows:
        _validate_claim_row(row, include_review=include_review)

    # STEP 1 -- collapse duplicate canonical works
    collapsed = _collapse_duplicates(service_rows)
    standalone, span_groups = _group_by_span(collapsed)

    # STEP 2 -- separate identical-span generic groups. A group the ratified
    # predicate cannot decide is classified conservatively as generic and is
    # never silently promoted.
    generic_keys = []
    granularity_keys = []
    for key, members in span_groups.items():
        verdict = grouping.separate_granularity(members)
        if verdict == grouping.SAME_WORK_GRANULARITY:
            granularity_keys.append(key)
        else:
            generic_keys.append(key)

    # STEP 3 -- lead attribution over what remains, with confirmation
    # precedence. The same deterministic total order fixes the nesting order and
    # the order in which a generic group's own works are listed.
    leads: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = [
        (row, []) for row in standalone
    ]
    for key in granularity_keys:
        confirmed, rest = _split_confirmed(span_groups[key])
        if len(confirmed) > 0:
            # The lead is chosen from the confirmed members, by the SAME
            # ratified order applied to that subset -- no new tie-break.
            lead, *further_confirmed = _in_ratified_order(confirmed)
            leads.append((lead, _in_ratified_order(rest)))
            leads.extend((row, []) for row in further_confirmed)
        else:
            lead, *nested = _in_ratified_order(rest)
            leads.append((lead, nested))

    generic_groups = []
    for key in generic_keys:
        confirmed, rest = _split_confirmed(span_groups[key])
        leads.extend((row, []) for row in confirmed)
        if len(rest) >= 2:
            generic_groups.append(_generic_group(key, _in_ratified_order(rest), lang))
        else:
            # Not a group any more. D-13d's own precondition is >=2 works on
            # one span; a single row falls back to the standalone path, where
            # STEP 4's gate still applies to it.
            leads.extend((row, []) for row in rest)

    # STEP 4 -- gate short-evidence rows (inside `_identification_row`, whose
    # level assignment IS the gate).
    leads.sort(key=lambda pair: _lead_sort_key(pair[0]))
    rows = [_identification_row(row, nested, lang) for row, nested in leads]
    generic_groups.sort(key=lambda group: (group["span_start"], group["span_end"]))
    return rows, generic_groups


def _sort_offset(value: Any, field_name: str) -> Tuple[int, int]:
    """One passage offset as a sort key: `(present, magnitude)`.

    `str(offset)` sorted lexicographically, which puts offset 100 before offset
    20 -- so a page's rows came out in an order no reader could explain, and
    the more offsets a page carried the more scrambled it looked. The leading
    flag is an EXPLICIT sentinel for a missing offset, so an absent value sorts
    last by decision rather than by whatever `None` happens to compare like.
    """
    if value is None:
        return (1, 0)
    return (0, _int_or_refuse(value, field_name))


def _lead_sort_key(row: Mapping[str, Any]):
    """Deterministic emission order: strongest band first, then the passage's
    offsets NUMERICALLY, then the display work id, then the claim id -- never
    input order, which a caller could vary.

    The ids stay lexicographic on purpose: they are opaque strings, and the
    only property asked of them here is that the order be total.
    """
    return (
        _band_rank(row),
        _sort_offset(row.get("span_start"), "span_start"),
        _sort_offset(row.get("span_end"), "span_end"),
        str(_display_work_id(row)),
        str(row.get("claim_id")),
    )


# ---------------------------------------------------------------------------
# The related-pages section (D-11/D-11a). The header count comes from the
# EAGER count envelope; the rows come only behind the toggle.
# ---------------------------------------------------------------------------


def related_page_row(item: Mapping[str, Any]) -> Dict[str, Any]:
    """ONE related-page row as the panel may render it -- and the composite
    `related_page_id` is deliberately NOT in it.

    The row used to carry that id and the panel printed it, so a scholarly
    surface showed `990051620920205171_IE167198813_P000003_FL167198817` where a
    shelfmark belongs. Leaving the id out of the emitted row is what makes the
    fix structural rather than a habit: a renderer cannot print a field it was
    never given, on the happy path or on the degraded one.

    PUBLIC because the renderer needs it on the LAZY path too -- the toggle
    fetches an envelope and paints it without rebuilding the whole model, so
    both paths have to project through this one function or they will drift.
    """
    return {
        "sys_id": item.get("sys_id"),
        "library_code": item.get("library_code"),
        "shelfmark_display": item.get("shelfmark_display"),
        "page_number": item.get("page_number"),
        # The volume that folio number belongs to (2026-08-08). Carried for the
        # row's LINK, which needs a COMPLETE address: folio numbering is per
        # volume, so `page=3` alone is a different page in each volume of a
        # multi-volume manuscript -- and this row emitted exactly that until the
        # volume was threaded through here.
        #
        # This is the composite id's ONLY legitimate descendant on the row. The
        # id itself is still deliberately absent (see the docstring); what
        # reaches the renderer is the parsed volume, which is not printable as
        # an identifier and is consumed by `browse_url` alone.
        "volume_ie": item.get("volume_ie"),
        # Trust the service's own flag when it sent one; fall back to the
        # fields themselves, so a row from an older envelope still resolves to
        # a named state rather than to a half-blank line.
        "display_missing": bool(item.get("display_missing")) or not (
            item.get("library_code") and item.get("shelfmark_display")),
        "evidence_row_count": item.get("evidence_row_count"),
    }


def _related_pages(bundle: PanelServiceBundle) -> Dict[str, Any]:
    lang = bundle.lang
    count_envelope = bundle.related_count
    section: Dict[str, Any] = {
        "header": ds.section_header(ds.SECTION_PAGES_MATCHING_THIS_PAGE, lang),
        "label": ds.related_pages_label(lang),
    }

    # The header count is fetched EAGERLY and is what the default view renders,
    # so the section still has a real number without the rows. D-11a: the unit
    # is DISTINCT OPPOSITE PAGES -- never evidence rows and never directed
    # pairs, three genuinely different populations an earlier figure conflated.
    if is_outage(count_envelope):
        section["count"] = None
        section["count_state"] = ROWS_OUTAGE
        section["count_service_state"] = _service_state(count_envelope, lang)
    else:
        count = _envelope_total(count_envelope)
        section["count"] = count
        section["count_state"] = ROWS_EMPTY if count == 0 else ROWS_POPULATED
        section["count_unit"] = (count_envelope.get("meta") or {}).get("unit")
        section["count_line"] = ds.related_pages_count_line(count, lang)

    rows_envelope = getattr(bundle, LAZY_ENVELOPE_FIELD)

    if rows_envelope is None:
        # NOT REQUESTED. Not an empty section, not an outage -- a query nobody
        # ran. The header still carries a real number, from the count envelope.
        section["rows_state"] = ROWS_NOT_REQUESTED
    elif is_outage(rows_envelope):
        section["rows_state"] = ROWS_OUTAGE
        section["service_state"] = _service_state(rows_envelope, lang)
    elif _is_ok_zero(rows_envelope):
        section["rows_state"] = ROWS_EMPTY
        section["rows"] = ()
    else:
        section["rows_state"] = ROWS_POPULATED
        section["rows"] = tuple(
            related_page_row(item) for item in _items(rows_envelope))
    return section


# ---------------------------------------------------------------------------
# The manuscript pane (D-13h). NAMED works, never a bare count: manuscript-level
# coherence is the context that makes a single claim judgeable -- a page-23
# Esther identification looks arbitrary alone and obviously right once the
# reader sees the neighbouring folios carry the same commentator on adjacent
# books.
#
# READER AID ONLY. It must NEVER feed band assignment or routing, which would
# be circular: the pane is built FROM the claims it would then be scoring.
# ---------------------------------------------------------------------------


#: The rendered states that HAVE reader strings, which is the frozen vocabulary
#: minus the one the owner has not assigned strings for. Derived from
#: `NEVER_RENDERED_IN_V1` rather than listed, so the day a direction signal ships
#: and `work_quotes_page` becomes reachable, this set grows with it instead of
#: silently dropping a chip.
_CHIPPABLE_RELATIONS: frozenset = ids.RENDERED_RELATIONS - NEVER_RENDERED_IN_V1


def _work_chip(row: Mapping[str, Any], lang: str) -> Dict[str, Any]:
    title, missing = _routed_title(row, lang)
    in_main_pool = row.get("main_pool") is True
    chip: Dict[str, Any] = {
        "work_id": _display_work_id(row),
        "work_title": title,
        "title_missing": missing,
        "page_count": row.get("page_count"),
        "gated": bool(row.get("gated")),
        "in_main_pool": in_main_pool,
        "bucket": bucket_label(in_main_pool, lang),
    }
    # C-track step 3c: the strongest matrix output over this work's
    # identifications in this manuscript, NOT the strongest stored claim type.
    # The membership test widens with it -- `uncertain` is a renderable state
    # here, and a pane that dropped its chip would be silent about exactly the
    # rows the matrix exists to qualify. `work_quotes_page` stays out, because
    # `relation_chip` raises on it (§1: no owner-assigned strings), and this
    # caller is the reason the test is a membership check rather than a
    # try/except: a swallowed ValueError drops the element instead of failing.
    rendered_relation = row.get("rendered_relation")
    if rendered_relation in _CHIPPABLE_RELATIONS:
        chip["relation_chip"] = ds.relation_chip(rendered_relation, lang)
    return chip


def _work_chip_sort_key(chip: Mapping[str, Any], row: Mapping[str, Any]):
    """Strongest band first, then the widest page span, then the work id -- a
    total order, so the chip list never depends on the query's row order."""
    return (
        _band_rank(row, "best_band_rank"),
        -_int_or_refuse(row.get("page_count") or 0, "page_count"),
        str(chip["work_id"]),
    )


def _manuscript_pane(
    bundle: PanelServiceBundle, outcome: ArbitrationOutcome
) -> Dict[str, Any]:
    lang = bundle.lang
    works_envelope = bundle.manuscript_works
    pane: Dict[str, Any] = {
        "header": ds.section_header(ds.SECTION_ELSEWHERE_IN_MANUSCRIPT, lang),
        "scope_state": outcome.scope_state,
        "partial_scope": outcome.scope_state == SCOPE_TRUNCATED,
        "reader_aid_only": True,
    }

    # The scope decides FIRST. When it did not resolve, the pane reports nothing
    # about the manuscript at all -- not a total, not an empty marker, not a
    # zero. 136-17 does not even issue the works query in that case, so
    # whatever envelope reaches here says nothing either.
    if not outcome.pane_reports_manuscript_facts:
        pane["state"] = PANE_UNRESOLVED
        if outcome.scope_state == SCOPE_OUTAGE:
            # A FAILED page-scope read, distinguished from an unresolvable one:
            # the first is temporary and re-running the reads can fix it, the
            # second is a fact about the manuscript that a retry cannot change.
            # Only the first gets a retry, because a retry button on a permanent
            # condition is a control that cannot work (round 12, finding 2).
            pane["service_state"] = _service_state(bundle.page_ids, lang)
        return pane

    if is_outage(works_envelope):
        pane["state"] = PANE_OUTAGE
        pane["service_state"] = _service_state(works_envelope, lang)
        return pane

    total = _envelope_total(works_envelope)
    pane["total"] = total
    # A truncated scope's total covers the RESOLVED pages only; labelling it as
    # the manuscript's total would state a number we did not measure.
    pane["total_covers_resolved_pages_only"] = pane["partial_scope"]
    pane["page_threshold"] = MANUSCRIPT_PANE_PAGE_THRESHOLD
    pane["paginated"] = total > MANUSCRIPT_PANE_PAGE_THRESHOLD

    if _is_ok_zero(works_envelope):
        pane["state"] = PANE_EMPTY
        pane["works"] = ()
        return pane

    decorated = [(_work_chip(row, lang), row) for row in _items(works_envelope)]
    decorated.sort(key=lambda pair: _work_chip_sort_key(pair[0], pair[1]))
    pane["state"] = PANE_POPULATED
    pane["works"] = tuple(chip for chip, _row in decorated)
    return pane


# ---------------------------------------------------------------------------
# The entry control (D-13).
# ---------------------------------------------------------------------------


def _manuscript_reports_identifications(
    works: Mapping[str, Any], outcome: ArbitrationOutcome
) -> bool:
    """Whether the WHOLE-MANUSCRIPT read is evidence of anything at all.

    Three conjuncts, and each one is refusing to invent a fact:

    * `pane_reports_manuscript_facts` -- the SAME predicate the pane itself
      obeys, reused rather than restated. Where the page scope did not resolve,
      anything said about the manuscript is our own plumbing failure wearing
      the manuscript's name, so this must read False there;
    * an `ok` works envelope -- an outage, a timeout or a `busy` is not
      evidence of a manuscript with content any more than it is evidence of a
      manuscript without it;
    * a total ABOVE zero -- an `ok` zero is a true "nothing elsewhere either".
    """
    return (outcome.pane_reports_manuscript_facts
            and _is_ok(works)
            and _envelope_total(works) > 0)


def _entry_control(
    claims: Mapping[str, Any], page_ids: Mapping[str, Any],
    works: Mapping[str, Any], outcome: ArbitrationOutcome,
) -> Dict[str, Any]:
    """Visibility is a FIELD on the model, not a render-time expression.

    Hidden ONLY on a status of `ok` with a total of zero -- AND only when the
    whole-manuscript read has nothing to add. An outage must never look like a
    manuscript with nothing on it, and neither must a claim-less FOLIO of a
    claim-rich manuscript: RNL Ms. Evr. Antonin A 1 carries 483 claims across
    396 of its 492 pages and none at all on page 1, where the hide rule made
    the control byte-identical to a manuscript the corpus knows nothing about.
    `manuscript_elsewhere_only` names that state so the renderer can SAY it
    rather than infer it; the hide rule itself is untouched everywhere else.

    `degraded_status` is a SECOND field, and it exists because of what unhiding
    the control on a page-scope outage exposed: `status` is the CLAIMS status,
    so on `("ok", SCOPE_OUTAGE)` it is `ok` and `count` is a true zero for this
    page -- while the pane that would have spoken for the whole manuscript is an
    outage the reader cannot see until the panel is opened. A bare "(0)" beside
    an unknown reads as "this manuscript has nothing".

    It carries the FAILED READ'S OWN status rather than a boolean, so the
    control names what actually happened: a page-scope `timeout` says "this took
    longer than expected", not "temporarily unavailable". Deciding that here
    rather than in the renderer is what keeps the renderer from recombining the
    claims status with the scope state on its own.
    """
    true_page_zero = outcome.panel_hidden_on_zero and _is_ok_zero(claims)
    elsewhere_only = true_page_zero and _manuscript_reports_identifications(works, outcome)
    control: Dict[str, Any] = {
        "hidden": true_page_zero and not elsewhere_only,
        "status": claims.get("status"),
        "manuscript_elsewhere_only": elsewhere_only,
    }
    if elsewhere_only:
        # THIS PAGE's total is zero, and it is the only count the control could
        # carry. A bare "(0)" beside a manuscript with identifications on 396 of
        # its pages states the page's fact where the reader reads the
        # manuscript's, so the control reports the SCOPE in words and no number
        # at all. Declining to count is not the same as `None` for an outage --
        # `manuscript_elsewhere_only` is what tells the two apart.
        control["count"] = None
    else:
        control["count"] = _envelope_total(claims) if _is_ok(claims) else None
    if not _is_ok(claims):
        control["degraded_status"] = claims.get("status")
    elif outcome.scope_state == SCOPE_OUTAGE:
        control["degraded_status"] = page_ids.get("status")
    else:
        control["degraded_status"] = None
    return control


# ---------------------------------------------------------------------------
# The model.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PanelModel:
    """Everything the renderer needs, and no judgement left for it to make."""

    lang: str
    show_more: bool
    show_divergence: bool
    panel_status: str
    entry_control: Dict[str, Any]
    service_state: Dict[str, Any]
    caveat: str
    bucket_rule_sentence: str
    manuscript_pane: Dict[str, Any]
    disclosure_levels: Tuple[Dict[str, Any], ...]
    #: Pane ORDER is a display decision, so the model takes it. True only in the
    #: `manuscript_elsewhere_only` state, where the page pane is an empty list
    #: and the manuscript pane holds everything the reader opened the panel for;
    #: leading with the empty one is how a reader who was told "there are
    #: identifications elsewhere" arrives at nothing.
    lead_with_manuscript_pane: bool = False

    # -- convenience accessors; deliberately NOT part of `as_dict`, so nothing
    # -- below is walked twice by the honesty sweep.
    @property
    def related_pages(self) -> Dict[str, Any]:
        return self._level(LEVEL_ALSO_SHARES_TEXT)["related_pages"]

    @property
    def generic_groups(self) -> Tuple[Dict[str, Any], ...]:
        return self._level(LEVEL_ALSO_SHARES_TEXT)["generic_groups"]

    def _level(self, key: str) -> Dict[str, Any]:
        for level in self.disclosure_levels:
            if level["key"] == key:
                return level
        raise KeyError(key)

    def as_dict(self) -> Dict[str, Any]:
        """The whole emitted model as plain data -- what the honesty sweep
        walks, and what a renderer may treat as read-only."""
        return dataclasses.asdict(self)


def iter_rows(model: PanelModel) -> Iterator[Dict[str, Any]]:
    """Every emitted identification row, in disclosure-level order."""
    for level in model.disclosure_levels:
        for row in level.get("rows", ()):
            yield row


def build_panel_rows(bundle: PanelServiceBundle) -> PanelModel:
    """The panel's whole display model, as a pure function of the envelope set.

    Makes no query, touches no UI object, and takes no decision that is already
    taken by `shared.discovery_grouping`, `shared.discovery_main_pool` or
    `shared.discovery_display_strings`.

    The SECOND of the two public boundaries, and the reason log-safety here is
    a property of the module rather than of whoever last edited it: our own
    refusals leave verbatim, and anything else -- an exception from a shared
    module, from the standard library, or from a raise site added next year --
    leaves carrying only its type name. See `PanelContractError`.
    """
    try:
        return _build_panel_rows(bundle)
    except PanelContractError:
        raise
    except Exception as exc:
        raise _reduce_to_type(exc, "building the panel model") from None


def _build_panel_rows(bundle: PanelServiceBundle) -> PanelModel:
    lang = bundle.lang
    outcome = ARBITRATION_TABLE[(bundle.claims.get("status"),
                                 _scope_state(bundle.page_ids, bundle.manuscript_works))]
    # WHICH population the producer says it returned (D-13g). Read from the
    # envelope's own meta, never assumed: the default surface and the
    # review-opt-in surface admit different rows, and a model that guessed
    # would either refuse a legitimate opt-in read or admit an ineligible row.
    include_review = bool((bundle.claims.get("meta") or {}).get("include_review"))
    rows, generic_groups = _compose_rows(
        _items(bundle.claims), lang, include_review=include_review)

    default_rows = tuple(r for r in rows if r["disclosure_level"] == LEVEL_IDENTIFICATIONS)
    gated_rows = tuple(r for r in rows if r["disclosure_level"] == LEVEL_MORE_MATCHES)
    divergent_rows = tuple(r for r in rows if r["disclosure_level"] == LEVEL_DIVERGENCE)

    levels: Tuple[Dict[str, Any], ...] = (
        {
            "key": LEVEL_IDENTIFICATIONS,
            "label": ds.section_header(ds.SECTION_ON_THIS_PAGE, lang),
            "is_identifications": True,
            "default_visible": True,
            "visible": True,
            "rows": default_rows,
        },
        {
            "key": LEVEL_ALSO_SHARES_TEXT,
            "label": ds.disclosure_toggle(ds.TOGGLE_ALSO_SHARES_TEXT, lang),
            "is_identifications": False,
            "default_visible": False,
            "visible": False,
            "note": ds.not_an_identification_note(lang),
            "generic_groups": tuple(generic_groups),
            "related_pages": _related_pages(bundle),
        },
        {
            "key": LEVEL_MORE_MATCHES,
            "label": ds.disclosure_toggle(ds.TOGGLE_MORE_MATCHES, lang),
            "is_identifications": True,
            "default_visible": False,
            "visible": bool(bundle.show_more),
            "rows": gated_rows,
        },
        {
            "key": LEVEL_DIVERGENCE,
            "label": ds.disclosure_toggle(ds.TOGGLE_DIVERGENCE, lang),
            # These ARE identifications -- the catalogue names a different one,
            # and ruling F is explicit that the system takes no side on which
            # is right. Marking them `is_identifications: False` would be the
            # renderer's `notid` treatment saying they are not identifications
            # at all, which is a side.
            "is_identifications": True,
            "default_visible": False,
            "visible": bool(bundle.show_divergence),
            # OUTSIDE the collapsed body, unlike the middle level's `note`.
            # Ruling F's toggle is an "explicitly warned" one, and a warning
            # that lives inside a `<details>` is one a reader only meets AFTER
            # opening -- i.e. after the decision it exists to inform.
            #
            # EMPTY WHEN THERE IS NOTHING TO WARN ABOUT. The warning states a
            # fact about THIS folio ("these findings conflict with an existing
            # catalogue identification"), so on a folio with no divergent claim
            # it would be a standing assertion of a conflict that does not
            # exist -- and ~76% of the corpus has none. The LEVEL still renders
            # (like the empty "show more" level beside it); only the claim
            # about the folio is withheld.
            "warning": ds.divergence_warning(lang) if divergent_rows else "",
            "rows": divergent_rows,
        },
    )

    entry_control = _entry_control(
        bundle.claims, bundle.page_ids, bundle.manuscript_works, outcome)

    return PanelModel(
        lang=lang,
        show_more=bool(bundle.show_more),
        show_divergence=bool(bundle.show_divergence),
        panel_status=outcome.panel_status,
        entry_control=entry_control,
        service_state=_service_state(bundle.claims, lang),
        caveat=ds.recall_disclaimer(lang),
        bucket_rule_sentence=ds.rule_sentence(lang),
        manuscript_pane=_manuscript_pane(bundle, outcome),
        disclosure_levels=levels,
        # ONE predicate, read back from the control that already decided it --
        # a second evaluation here is how the label and the pane order drift
        # apart and the reader is told one thing and shown another.
        lead_with_manuscript_pane=bool(entry_control["manuscript_elsewhere_only"]),
    )
