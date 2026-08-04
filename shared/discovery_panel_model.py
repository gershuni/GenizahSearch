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

#: The exact `meta` key set each LIVE read emits on `ok`. A drifted service
#: shape must fail against this, in the model's own contract test, rather than
#: in a renderer. `tests/test_discovery_panel_model.py` parses the producing
#: functions and asserts this table equals what they actually build.
LIVE_OK_META_KEYS: Mapping[str, frozenset] = {
    "claims": frozenset({"page_id", "include_review"}),
    "page_ids": frozenset({"sys_id", "resolved", "truncated", "volume_ie"}),
    "manuscript_works": frozenset({"page_scope_resolved", "lang"}),
    "related_count": frozenset({"unit"}),
    "related_rows": frozenset({"unit"}),
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

#: The three disclosure levels D-13e ratified -- no more, no fewer. Level 2
#: holds the generic identical-span groups and the related-pages section and is
#: explicitly NOT identifications.
LEVEL_IDENTIFICATIONS = "identifications"
LEVEL_ALSO_SHARES_TEXT = ds.TOGGLE_ALSO_SHARES_TEXT
LEVEL_MORE_MATCHES = ds.TOGGLE_MORE_MATCHES

DISCLOSURE_LEVEL_KEYS: Tuple[str, ...] = (
    LEVEL_IDENTIFICATIONS, LEVEL_ALSO_SHARES_TEXT, LEVEL_MORE_MATCHES,
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
    "anchor_claim_type",
    "anchor_evidence_source",
    "anchor_confidence_band",
    "status",
    "reason",
})

#: A (source, band) pair outside the frozen lattice sorts last, never first.
_UNRANKED_BAND_RANK = 10 ** 6


# ---------------------------------------------------------------------------
# The input bundle.
# ---------------------------------------------------------------------------


def _validate_envelope(field_name: str, value: Any, *, optional: bool) -> None:
    """Reject anything that is not a four-key envelope.

    `optional` is True for exactly ONE field (the lazy related-page rows), where
    `None` is a meaningful value. Everywhere else `None` raises: a bundle that
    accepted `None` on an eager field would let a real outage be dropped on the
    floor as "nobody asked for this".
    """
    if value is None:
        if optional:
            return
        raise ValueError(
            "PanelServiceBundle.%s: None is not accepted -- this envelope is "
            "fetched eagerly, so its absence would hide a real outage behind "
            "'not requested' (only %s may be None)" % (field_name, LAZY_ENVELOPE_FIELD)
        )
    if not isinstance(value, Mapping):
        raise ValueError(
            "PanelServiceBundle.%s: expected a four-key envelope %s, got %s -- a "
            "bare list cannot say whether it is empty because the manuscript is "
            "empty or because the service failed"
            % (field_name, sorted(ENVELOPE_KEYS), type(value).__name__)
        )
    if set(value) != ENVELOPE_KEYS:
        raise ValueError(
            "PanelServiceBundle.%s: envelope keys %s != the live shape %s"
            % (field_name, sorted(value), sorted(ENVELOPE_KEYS))
        )
    if value["status"] not in SURFACE_STATUSES:
        raise ValueError(
            "PanelServiceBundle.%s: status %r is outside the closed vocabulary %s"
            % (field_name, value["status"], sorted(SURFACE_STATUSES))
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

    def __post_init__(self) -> None:
        for field_name in EAGER_ENVELOPE_FIELDS:
            _validate_envelope(field_name, getattr(self, field_name), optional=False)
        _validate_envelope(
            LAZY_ENVELOPE_FIELD, getattr(self, LAZY_ENVELOPE_FIELD), optional=True)

    def with_related_rows(self, envelope: Mapping[str, Any]) -> "PanelServiceBundle":
        """A copy carrying the lazily-fetched related-page rows -- what a
        surface builds when the reader opens the toggle."""
        return dataclasses.replace(self, related_rows=envelope)


# ---------------------------------------------------------------------------
# Envelope readers. `is_outage` and an EXPLICIT zero test, never `not items`.
# ---------------------------------------------------------------------------


def _is_ok(envelope: Mapping[str, Any]) -> bool:
    return envelope.get("status") == STATUS_OK


def _is_ok_zero(envelope: Mapping[str, Any]) -> bool:
    """A SUCCESSFUL zero -- the only state the entry control hides on."""
    return _is_ok(envelope) and int(envelope.get("total") or 0) == 0


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


def _band_rank(row: Mapping[str, Any]) -> int:
    rank = row.get("band_rank")
    return _UNRANKED_BAND_RANK if rank is None else int(rank)


# ---------------------------------------------------------------------------
# STEP 1 -- collapse duplicate canonical works (D-13a).
# ---------------------------------------------------------------------------


def _collapse_duplicates(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """`grouping.collapse_canonical`, applied only to rows that HAVE a canonical
    work id.

    Rows whose `canonical_work_id` is absent pass through untouched: grouping
    them would key every one of them on the same missing value and collapse
    unrelated works into one row, which is the opposite of what D-13a asks for.
    """
    with_canon = [row for row in rows if row.get("canonical_work_id")]
    without_canon = [dict(row) for row in rows if not row.get("canonical_work_id")]
    collapsed = grouping.collapse_canonical(with_canon) if len(with_canon) > 0 else []
    return list(collapsed) + without_canon


# ---------------------------------------------------------------------------
# STEP 2/3 -- identical-span groups: which leave, which collapse, who leads.
# ---------------------------------------------------------------------------


def _span_key(row: Mapping[str, Any]) -> Optional[Tuple[int, int]]:
    start, end = row.get("span_start"), row.get("span_end")
    if start is None or end is None:
        return None
    return (int(start), int(end))


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
            "relation_chip": ds.relation_chip(member.get("relation_kind"), lang),
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


def _is_human_confirmed(row: Mapping[str, Any]) -> bool:
    """D-13g: a human-confirmed row is present in the DEFAULT set even when
    routing demoted it. The identification grain materializes it under
    `shipped OR human_confirmed`, and the row itself records which."""
    return bool(row.get("restored_by_human_confirmation")) or (
        row.get("main_pool_reason") == REASON_MAIN_HUMAN_CONFIRMED
    ) or (
        row.get("adjudication_status") == ids.ADJUDICATION_STATUS_HUMAN_CONFIRMED
    )


def _disclosure_level_for(row: Mapping[str, Any]) -> str:
    """Which disclosure level one identification row belongs to.

    Nothing is deleted here: a row that fails every test below is GATED behind
    the "show more" toggle and stays reachable. Bucket membership itself is the
    materialized shared-rule decision on the row (`main_pool`), never
    recomputed.
    """
    if _is_human_confirmed(row):
        return LEVEL_IDENTIFICATIONS
    if row.get("main_pool") is not True:
        return LEVEL_MORE_MATCHES
    if row.get("default_eligible") is not True:
        return LEVEL_MORE_MATCHES
    matched = row.get("matched_letters")
    if (
        matched is not None
        and int(matched) < SHORT_EVIDENCE_THRESHOLD_MATCHED_LETTERS
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
    title, missing = _routed_title(row, lang)
    relation_kind = row.get("relation_kind")
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
            title, row.get("coverage_ppm"), relation_kind, lang,
            evidence_source=evidence_source),
        "relation_chip": ds.relation_chip(relation_kind, lang),
        "relation_code": ds.filter_code(relation_kind),
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
    }

    # D-08a: the ONE permitted percentage, direct family only, always with its
    # matched-letter qualifier (which `row_headline` composes).
    if (
        relation_kind == ids.CLAIM_TYPE_DIRECT_WITNESS
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
    service_rows: Sequence[Mapping[str, Any]], lang: str
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """The pipeline, in the ONE order that is correct.

    Applying these in a different order changes what a reader sees.
    """
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

    # STEP 3 -- lead attribution over what remains. The generic groups have
    # already left; the same deterministic total order also fixes the order in
    # which a generic group's own works are listed.
    leads: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = [
        (row, []) for row in standalone
    ]
    for key in granularity_keys:
        lead, remainder = grouping.lead_attribution(span_groups[key])
        leads.append((lead, remainder))

    generic_groups = []
    for key in generic_keys:
        lead, remainder = grouping.lead_attribution(span_groups[key])
        generic_groups.append(_generic_group(key, [lead] + remainder, lang))

    # STEP 4 -- gate short-evidence rows (inside `_identification_row`, whose
    # level assignment is the gate).
    rows = [_identification_row(row, nested, lang) for row, nested in leads]
    rows.sort(key=_row_sort_key)
    generic_groups.sort(key=lambda group: (group["span_start"], group["span_end"]))
    return rows, generic_groups


def _row_sort_key(row: Mapping[str, Any]):
    """Deterministic emission order: strongest band first, then the display work
    id, then the claim id -- never input order, which a caller could vary."""
    return (
        0 if row["disclosure_level"] == LEVEL_IDENTIFICATIONS else 1,
        str(row.get("span_start")),
        str(row.get("work_id")),
        str(row.get("claim_id")),
    )


# ---------------------------------------------------------------------------
# The related-pages section (D-11/D-11a). The header count comes from the
# EAGER count envelope; the rows come only behind the toggle.
# ---------------------------------------------------------------------------


def _related_pages(bundle: PanelServiceBundle) -> Dict[str, Any]:
    lang = bundle.lang
    section: Dict[str, Any] = {}
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
            {
                "related_page_id": item.get("related_page_id"),
                "evidence_row_count": item.get("evidence_row_count"),
            }
            for item in _items(rows_envelope)
        )
    return section


# ---------------------------------------------------------------------------
# The model.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PanelModel:
    """Everything the renderer needs, and no judgement left for it to make."""

    lang: str
    show_more: bool
    panel_status: str
    disclosure_levels: Tuple[Dict[str, Any], ...]

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
    """
    lang = bundle.lang
    rows, generic_groups = _compose_rows(_items(bundle.claims), lang)

    default_rows = tuple(r for r in rows if r["disclosure_level"] == LEVEL_IDENTIFICATIONS)
    gated_rows = tuple(r for r in rows if r["disclosure_level"] == LEVEL_MORE_MATCHES)

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
    )

    return PanelModel(
        lang=lang,
        show_more=bool(bundle.show_more),
        panel_status=bundle.claims.get("status"),
        disclosure_levels=levels,
    )
