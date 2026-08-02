# -*- coding: utf-8 -*-
"""VIS-01 two-axis public/private visibility derivation (Phase 136, plan
136-08, owner decision D-22 -- `.planning/phases/136-read-surfaces-connections-panel-work-witnesses/136-CONTEXT.md`
"Public/private projection (VIS-01)").

**Why two axes, not one.** The shipped schema *requires*
`discovery_claim.source_corpus` to equal the identified work's
`source_corpus` (`docs/specs/discovery-sidecar-schema-v1.md` SS1.2, enforced
at `scripts/verify_discovery_sidecar.py::check_source_corpus_consistency`,
SS319-332). That equality constraint means `works.source_corpus` is the ONLY
corpus signal the shipped tables carry today -- and D-22 measured it
insufficient as a general-purpose "is this safe to publish" proxy: the
restricted-corpus id prefix maps to **656 restricted-identity works AND 235
open (Sefaria) ones** in the live asset, so a corpus-keyed shortcut mislabels
in BOTH directions. The build must therefore derive TWO separate axes at
build time, before the raw provenance id is discarded:

- **`assertion_visibility`** -- the visibility of the origin of the
  DISPLAYED ASSERTION (the specific evidence occurrence), never the
  identified work's corpus.
- **`identity_visibility`** -- the visibility of the displayed WORK's own
  identity source.

**Public eligibility requires BOTH axes public** (`is_public`, the ONE
eligibility rule in this tree -- `scripts/project_discovery_public.py` calls
it and never restates the conjunction). Neither axis is a proxy for the
other: a row whose work is open but whose displayed assertion originates in
a restricted corpus is NOT public (open work / restricted assertion); a work
whose neutral title originates in a restricted corpus is NOT public even
when the specific assertion is open (restricted work / open assertion).

**Fail-closed, always.** An unknown, missing, `None`, or malformed origin
value yields `private` -- never `public` by default. This mirrors the
"never return a restricted name on any path" discipline already established
for the novelty-provenance masking shape (NOVEL-02): no function in this
module ever returns, logs, or interpolates a raw origin id -- only the
closed `{public, private}` enum defined by
`docs/specs/discovery-sidecar-schema-v1.md`'s 2026-08-02 Amendment (A):

    assertion_visibility TEXT NOT NULL CHECK (assertion_visibility IN ('public','private')),
    identity_visibility  TEXT NOT NULL CHECK (identity_visibility  IN ('public','private')),

**Reconciling the launch-scope statement.** VIS-01's own prose describes a
corpus/family shortcut ("launch scope: Sefaria-direct matches [union] all
MS-relationship/propagated claims") that is EXACTLY the proxy D-22 proves
insufficient. `reconcile_launch_scope` computes both the VIS-01 shortcut's
count and the two-axis conjunction's count over the same row set and reports
the symmetric difference broken down by (source_corpus, evidence_source) --
it never resolves a disagreement in code. A real number belongs in front of
plan 136-13's gate battery and the owner, not a silent narrowing or
widening of the public asset.

Deliberately stdlib-only (no web-framework import), mirroring
`scripts/discovery_ids.py`'s own discipline, so this module stays importable
from the offline build/projection scripts AND from `shared/discovery_service.py`
without violating the `shared/` -> `web/` back-edge convention.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional, Tuple

import scripts.discovery_ids as ids

# ---------------------------------------------------------------------------
# The closed {public, private} enum -- mirrors the schema's Amendment (A) CHECK
# constraints verbatim. ONE source of truth for both axes' return values.
# ---------------------------------------------------------------------------

VISIBILITY_PUBLIC = "public"
VISIBILITY_PRIVATE = "private"
VISIBILITY_VALUES = frozenset({VISIBILITY_PUBLIC, VISIBILITY_PRIVATE})

# The masked `source_corpus` codes that count as an OPEN origin vs a
# RESTRICTED one. `msource` is the sole restricted corpus in the frozen
# vocabulary (`scripts.discovery_ids.SOURCE_CORPUS_CODES`); anything outside
# the frozen three-code set at all (unknown/malformed) fails closed to
# `private` -- it is never treated as "open by elimination".
_OPEN_SOURCE_CORPORA = frozenset({ids.SOURCE_CORPUS_SEFARIA, ids.SOURCE_CORPUS_JA})
_RESTRICTED_SOURCE_CORPORA = frozenset({ids.SOURCE_CORPUS_MSOURCE})

# The public-facing (source_corpus) tie-break order used by the schema's own
# `display_work_id` selection rule (SS(B1)) -- public-before-private, lowest
# code wins. Exposed here (not just in the projection script) so any future
# caller needing the SAME order never re-derives a second copy.
SOURCE_CORPUS_RANK: Dict[str, int] = {
    ids.SOURCE_CORPUS_SEFARIA: 0,
    ids.SOURCE_CORPUS_JA: 1,
    ids.SOURCE_CORPUS_MSOURCE: 2,
}


def _corpus_code_to_visibility(raw_corpus: Any) -> str:
    """Fail-closed mapping from a raw MASKED `source_corpus` CODE (never a
    raw provenance id, a filename, or an `M:`/`J:`/`REF`-shaped token -- this
    function only ever consumes the ALREADY-MASKED three-value corpus code)
    to a `{public, private}` visibility value.

    Anything that is not exactly one of the two OPEN codes
    (`sefaria`, `ja`) yields `private` -- including the restricted code
    (`msource`), `None`, an empty/whitespace string, a non-string value, and
    any code outside the frozen three-value vocabulary entirely. There is no
    "unknown counts as open" branch anywhere in this function.
    """
    if not isinstance(raw_corpus, str):
        return VISIBILITY_PRIVATE
    code = raw_corpus.strip()
    if code in _OPEN_SOURCE_CORPORA:
        return VISIBILITY_PUBLIC
    return VISIBILITY_PRIVATE


def assertion_visibility(evidence_row: Optional[Mapping[str, Any]]) -> str:
    """D-22's FIRST axis: the visibility of the DISPLAYED ASSERTION -- derived
    from the raw origin of the SPECIFIC evidence occurrence, never from the
    identified work's corpus (`identity_visibility` below is the other,
    independent axis; `works.source_corpus` is proven an insufficient proxy
    for THIS axis by D-22's own 656/235 measurement).

    `evidence_row` is expected to be a BUILD-TIME (pre-asset) row carrying
    the evidence occurrence's own raw masked-corpus origin under the key
    `assertion_source_corpus` (one of `scripts.discovery_ids.SOURCE_CORPUS_CODES`).
    This key exists ONLY at build time, before the raw provenance id is
    discarded -- the shipped `discovery_evidence` table itself never stores
    a general assertion-origin column (the schema amendment's own text:
    "`discovery_evidence` carries only a family code ... `works.source_corpus`
    IS exactly the proxy D-22 says is insufficient"). Once the PRIVATE asset
    is built, the build process writes the DERIVED `public`/`private` value
    onto `discovery_evidence.assertion_visibility` directly -- a later reader
    (e.g. `scripts/project_discovery_public.py`) reads that STORED column
    rather than re-deriving it from a raw origin that no longer exists.

    Fail-closed: a missing key, a `None` value, an empty/malformed string, a
    non-mapping `evidence_row`, or any code outside the closed
    `source_corpus` vocabulary all yield `private` -- never `public` by
    default.
    """
    if not isinstance(evidence_row, Mapping):
        return VISIBILITY_PRIVATE
    return _corpus_code_to_visibility(evidence_row.get("assertion_source_corpus"))


def identity_visibility(work_row: Optional[Mapping[str, Any]]) -> str:
    """D-22's SECOND axis: the visibility of the DISPLAYED WORK's own
    identity -- derived from the work's `source_corpus` (the existing
    `works.source_corpus` masked-corpus column, SS1.1). This is the SAME
    proxy D-22 proves insufficient for the assertion axis above, but it is
    exactly right for THIS axis: it answers "does this work's own neutral
    title/identity originate in an open or a restricted corpus", which is
    precisely what `works.source_corpus` records.

    Fail-closed: a missing key, a `None` value, an empty/malformed string, a
    non-mapping `work_row`, or any code outside the closed `source_corpus`
    vocabulary all yield `private` -- never `public` by default.
    """
    if not isinstance(work_row, Mapping):
        return VISIBILITY_PRIVATE
    return _corpus_code_to_visibility(work_row.get("source_corpus"))


def is_public(assertion_vis: Optional[str], identity_vis: Optional[str]) -> bool:
    """The ONE public-eligibility rule (D-22): the conjunction of BOTH axes.

    A row is public if and only if BOTH `assertion_vis` and `identity_vis`
    are EXACTLY the string `'public'`. Any other value on either axis
    (`'private'`, `None`, an unrecognized string, a non-string) yields
    `False` -- fail-closed by construction, since the comparison is an exact
    equality test rather than a `!= 'private'` negative test.

    This is the ONLY place the conjunction is ever written. Every caller
    (`scripts/project_discovery_public.py` in particular) MUST call this
    function rather than re-deriving `assertion_vis == 'public' and
    identity_vis == 'public'` inline -- a second, textually-identical
    conjunction elsewhere would still be a duplicated eligibility rule this
    module's own docstring and the plan's acceptance criteria prohibit.
    """
    return assertion_vis == VISIBILITY_PUBLIC and identity_vis == VISIBILITY_PUBLIC


# ---------------------------------------------------------------------------
# Launch-scope reconciliation (VIS-01's own prose vs. the D-22 conjunction).
# ---------------------------------------------------------------------------


def _vis01_launch_scope_predicate(row: Mapping[str, Any]) -> bool:
    """VIS-01's OWN launch-scope shortcut, reproduced LITERALLY as a
    predicate for reconciliation purposes ONLY -- this is NOT an eligibility
    rule (`is_public` is the only one), it is the legacy corpus/family
    shortcut D-22 measures as insufficient, kept here so
    `reconcile_launch_scope` can compare against it honestly rather than
    against a paraphrase.

    VIS-01's requirement text: "launch scope: Sefaria-direct matches union
    all MS-relationship/propagated claims" -- i.e. a row counts under this
    shortcut when EITHER (a) it is a `track1_direct` row whose
    `source_corpus` is `sefaria`, OR (b) it is ANY `propagated` row,
    regardless of corpus (propagated evidence is the MS-relationship family
    per `docs/specs/discovery-sidecar-schema-v1.md` SS4.2).

    `row` must carry `evidence_source` and `source_corpus` (the CLAIM's/
    WORK's corpus -- SS1.2's `discovery_claim.source_corpus == works.source_corpus`
    equality constraint means these are interchangeable for this shortcut).
    """
    evidence_source = row.get("evidence_source")
    if evidence_source == ids.EVIDENCE_SOURCE_PROPAGATED:
        return True
    if evidence_source == ids.EVIDENCE_SOURCE_TRACK1_DIRECT:
        return row.get("source_corpus") == ids.SOURCE_CORPUS_SEFARIA
    return False


def reconcile_launch_scope(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    """Reconcile VIS-01's launch-scope shortcut against the D-22 two-axis
    conjunction over the SAME row set, WITHOUT resolving any disagreement.

    Each `row` must carry:
    - `evidence_source` (`track1_direct` | `propagated`) and `source_corpus`
      (the claim's/work's masked corpus code) -- consumed by the VIS-01
      shortcut predicate.
    - `assertion_source_corpus` -- the raw build-time evidence-occurrence
      origin consumed by `assertion_visibility`.

    The identity axis is read from the SAME `source_corpus` field the
    shortcut predicate reads, because SS1.2's cross-table equality
    constraint (`discovery_claim.source_corpus == works.source_corpus`)
    means a joined claim/evidence/work row carries only ONE corpus value for
    the work side -- there is no second, distinct "work_row" to pass
    separately at this reconciliation grain.

    Returns a dict with:
    - `total_rows`
    - `vis01_launch_scope_count` -- how many rows VIS-01's shortcut would
      include
    - `conjunction_count` -- how many rows the D-22 conjunction includes
    - `symmetric_difference_count` -- how many rows the two rules disagree on
    - `symmetric_difference_by_corpus_family` -- a
      `{(source_corpus, evidence_source): count}` breakdown of exactly the
      disagreeing rows, per D-22's own "corpus x family" reference framing

    This function NEVER resolves a disagreement (it does not drop, keep, or
    relabel a single row) -- it only counts and reports, so plan 136-13's
    gate battery can put a real, measured number in front of the owner
    instead of a projection silently narrowing or widening the public asset.
    """
    total_rows = 0
    vis01_count = 0
    conjunction_count = 0
    symmetric_difference_count = 0
    by_corpus_family: Dict[Tuple[Any, Any], int] = {}

    for row in rows:
        total_rows += 1
        vis01_included = _vis01_launch_scope_predicate(row)
        assertion_vis = assertion_visibility(row)
        identity_vis = _corpus_code_to_visibility(row.get("source_corpus"))
        conjunction_included = is_public(assertion_vis, identity_vis)

        if vis01_included:
            vis01_count += 1
        if conjunction_included:
            conjunction_count += 1
        if vis01_included != conjunction_included:
            symmetric_difference_count += 1
            key = (row.get("source_corpus"), row.get("evidence_source"))
            by_corpus_family[key] = by_corpus_family.get(key, 0) + 1

    return {
        "total_rows": total_rows,
        "vis01_launch_scope_count": vis01_count,
        "conjunction_count": conjunction_count,
        "symmetric_difference_count": symmetric_difference_count,
        "symmetric_difference_by_corpus_family": dict(by_corpus_family),
    }
