# -*- coding: utf-8 -*-
"""matrix-v1 — Contract 1's precedence matrix, the ONE implementation.

The semantics are frozen in `docs/specs/discovery-relation-matrix-v1.md` §2
(A0a-2, 2026-08-12). This module renders them and nothing else: six rules, in
order, first match wins. It is deliberately shared by the builder (which stores
the column), the projector (which RE-stores it per asset), the release verifier
(which recomputes and asserts equality row-for-row), and the read path (which
renders strings from it) — the same never-duplicated posture as
``compute_frame_content_hash`` and ``shared.discovery_family``. A second
implementation anywhere would make the verifier's equality gate a tautology.

**What the output is.** A *display* verdict about one identification (one
manuscript × one work). It changes no stored judgement: ``routing_status``,
``claim_type``, ``relation_kind``, ``novelty_status`` all keep their values.
Step 6 is the identity case — most rows render exactly what they always did.

**Asset-relativity is not an implementation detail.** Step 4's work-divergence
ratio is an aggregate over the asset's OWN identification rows, so pruning
changes its denominators: a work that is 3-of-10 divergent privately may be
3-of-6 publicly. The rendered relation is therefore recomputed after public
pruning rather than copied, and each asset's meta records the parameterization
its stored values were produced under
(``relation_matrix_parameterization`` keys, via
:func:`parameterization_meta_rows`). The verifier reconstructs the
parameterization FROM meta — never from a default — because a gate that
recomputes under its own assumptions would pass a row stored under different
ones.

**Deploy-1 parameterization** (owner ruling 2026-08-11, spec §2): steps 1, 2, 5
and 6 active; step 3 (region) NOT activated; step 4's threshold arm unset. Step
4's *curated* arm is data, not a parameter — it fires for whatever the asset's
``discovery_curated_quoter`` table holds. That table shipped EMPTY when the spec
was frozen and is non-empty as of the owner's 2026-08-12 ruling (both ילקוט
שמעוני works), so deploy 1 does move rows; B's ``--compare --expect-delta`` run
is what measures it.
"""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Dict, Iterable, Iterator, List, Mapping, Optional, Tuple

import scripts.discovery_ids as ids
from shared.discovery_novelty import DEFAULT_STATUS as NOVELTY_NOT_CHECKED
from shared.discovery_novelty import DIVERGENCE_SHADES

MATRIX_VERSION = "matrix-v1"

# Step 2's routing reasons. Both mean "the router already decided this row's
# text-sharing is not a same-work claim", so the display must not out-claim it.
SHARED_TEXT_ROUTING_REASONS = frozenset({
    ids.ROUTING_REASON_LATER_SHARED_TEXT,
    ids.ROUTING_REASON_CO_CITATION,
})

# Step 4's work-divergence floor, from the A0a-2 census recipe
# (`region_matrix_sweep.py`: `d / n if n >= 5 else 0.0`). A work with fewer than
# five novelty-checked identifications has no usable divergence rate — one
# divergent row out of two is noise, not a quoter signature.
WORK_DIVERGENCE_MIN_DENOMINATOR = 5

# `work_quotes_page` is in the frozen vocabulary but is UNREACHABLE in v1: it
# renders only where a validated direction signal supports it, and no such
# signal exists (spec §1). Anything that would have pointed at it falls to
# `shared_text`. This is asserted, not assumed — see
# `tests/test_discovery_relation_matrix.py`.
NEVER_RENDERED_IN_V1 = frozenset({ids.RENDERED_RELATION_WORK_QUOTES_PAGE})

# Step 6 may only pass through a value that is actually a stored relation kind.
# Anything else — a NULL, a typo, a future token — fails closed rather than
# reaching a surface unvalidated (spec §2, missing-input rule, last row).
_PASSTHROUGH_RELATIONS = frozenset(ids.CLAIM_TYPES)

_META_KEY_VERSION = "relation_matrix_version"
_META_KEY_REGION_ACTIVE = "relation_matrix_region_active"
_META_KEY_QUOTER_THRESHOLD = "relation_matrix_quoter_threshold"

PARAMETERIZATION_META_KEYS: Tuple[str, ...] = (
    _META_KEY_VERSION,
    _META_KEY_REGION_ACTIVE,
    _META_KEY_QUOTER_THRESHOLD,
)


class RelationMatrixError(ValueError):
    """A parameterization or stored-value problem that must stop a build."""


@dataclass(frozen=True)
class MatrixParameterization:
    """The two open parameters of §2, and only those.

    Steps 1, 2, 5 and 6 carry no parameter — they are frozen semantics. Step 3
    has an activation flag (its census is priced but unruled: findings 9.1%
    reduction at 0.9% loss, expansion 9.1% at 14.3% — an A0b decision). Step 4
    has a threshold whose ``None`` means "this arm cannot fire", per the
    missing-input rule.
    """

    region_active: bool = False
    quoter_threshold: Optional[float] = None

    def __post_init__(self) -> None:
        t = self.quoter_threshold
        if t is None:
            return
        if not isinstance(t, (int, float)) or isinstance(t, bool):
            raise RelationMatrixError(
                f"quoter_threshold must be a number or None, got {t!r}"
            )
        # T=0 would flag every novelty-checked work and is never a ruling; the
        # domain is the half-open interval that makes `>= T` meaningful.
        if not (0.0 < float(t) <= 1.0):
            raise RelationMatrixError(
                f"quoter_threshold must be a ratio in (0, 1], got {t!r}"
            )


# The owner's 2026-08-11 ruling, as a named constant so no caller has to
# remember which way each flag points.
DEPLOY_1_PARAMETERIZATION = MatrixParameterization(
    region_active=False,
    quoter_threshold=None,
)


@dataclass(frozen=True)
class RelationInputs:
    """One identification's matrix inputs, all six of them.

    Every ``Optional`` here means "absent", and absence is load-bearing: the
    spec's missing-input table says which way each absence fails. None of them
    default — a caller that forgets one should get a TypeError at the call site,
    not a silently weaker claim on a reader's screen.
    """

    # Step 1. Absence IS the signal here, not a missing input.
    has_shipped_evidence: bool
    # Step 2. The DISPLAY row's reason — the identification's own best evidence
    # row (Codex finding 2), which is the reading the §2 census counted.
    routing_reason: Optional[str]
    # Step 3. Tri-state: True = whole footprint known non-discriminative;
    # False = at least one discriminative unit; None = not knowable (no region
    # entry, or no footprint at all) and the step cannot fire.
    footprint_all_non_discriminative: Optional[bool]
    # Step 4a. The work-level divergence ratio; None = unmeasured.
    work_divergence: Optional[float]
    # Step 4b. Membership in the asset's curated quoter table.
    on_curated_quoter_list: bool
    # Step 5. Contributes known/unknown, not a band (the public asset has zero
    # rows in the 0–10% bands, so a band threshold would gate nothing).
    coverage_known: bool
    # Step 6. The stored relation kind this row has always rendered.
    stored_relation_kind: Optional[str]


def render_relation(
    inputs: RelationInputs,
    parameterization: MatrixParameterization = DEPLOY_1_PARAMETERIZATION,
) -> str:
    """Return the rendered relation for one identification.

    Six rules, evaluated in the frozen order, first match renders. The result is
    always a member of ``ids.RENDERED_RELATIONS``.
    """
    # Step 1 — no shipped evidence at all. The honest router predicate at this
    # grain; the tempting "any evidence is review_only" reading flags 4,070 rows
    # and is a display-selection artifact (spec §2, step-1 note).
    if not inputs.has_shipped_evidence:
        return ids.RENDERED_RELATION_UNCERTAIN

    # Step 2 — the router already called this text-sharing, not same-work.
    if inputs.routing_reason in SHARED_TEXT_ROUTING_REASONS:
        return ids.RENDERED_RELATION_SHARED_TEXT

    # Step 3 — region: demote only when the ENTIRE matched footprint is known
    # non-discriminative. A demotion is also an assertion, so `None` (nobody
    # ruled on some unit) blocks it. Inactive in deploy 1.
    if parameterization.region_active and inputs.footprint_all_non_discriminative is True:
        return ids.RENDERED_RELATION_SHARED_TEXT

    # Step 4 — the quoter step, two independent arms. The curated arm is an
    # owner ruling and needs no threshold; the divergence arm cannot fire while
    # T is unset (missing-input rule).
    if inputs.on_curated_quoter_list:
        return ids.RENDERED_RELATION_QUOTES_THIS_WORK
    t = parameterization.quoter_threshold
    if t is not None and inputs.work_divergence is not None and inputs.work_divergence >= t:
        return ids.RENDERED_RELATION_QUOTES_THIS_WORK

    # Step 5 — coverage unknown.
    if not inputs.coverage_known:
        return ids.RENDERED_RELATION_UNCERTAIN

    # Step 6 — the stored relation stands. A value outside the stored
    # vocabulary (including NULL) fails closed rather than passing through.
    if inputs.stored_relation_kind in _PASSTHROUGH_RELATIONS:
        return inputs.stored_relation_kind
    return ids.RENDERED_RELATION_FAIL_CLOSED


# ---------------------------------------------------------------------------
# §3.2 — the member-grain cap.
#
# `render_relation` above answers at the IDENTIFICATION grain (one manuscript ×
# one work). Two surfaces render an assertion one grain BELOW that — the panel's
# page-level claim rows, and the expansion pane's representative chip — and §3.2
# fixes what those may say: the member's own relation, except it never
# out-asserts the identification it belongs to.
# ---------------------------------------------------------------------------

#: §3.1's frozen strength order, as comparable ranks. `direct_witness` >
#: `quotes_this_work` > `shared_text` > `uncertain` is the SAME order the
#: identification-grain SQL aggregate uses, written once here so the two cannot
#: drift.
#:
#: `work_quotes_page` is deliberately ABSENT rather than ranked. It has no
#: owner-assigned reader strings (spec §1), so a member rendering it would raise
#: in `relation_chip`; and inventing a rank for it would be inventing the very
#: semantics §1 defers. Absent means the cap fails closed on it — see below.
_RELATION_STRENGTH: Dict[str, int] = {
    ids.RENDERED_RELATION_DIRECT_WITNESS: 3,
    ids.RENDERED_RELATION_QUOTES_THIS_WORK: 2,
    ids.RENDERED_RELATION_SHARED_TEXT: 1,
    ids.RENDERED_RELATION_UNCERTAIN: 0,
}


def cap_member_relation(
    member_relation_kind: Optional[str],
    identification_relation: Optional[str],
) -> str:
    """§3.2: what ONE member-grain row may assert, given its identification's
    matrix output.

    ``member_relation_kind`` is the member's OWN stored ``claim_type``;
    ``identification_relation`` is the stored ``rendered_relation`` of the
    identification it belongs to, or ``None`` when there is none.

    The rule is the MINIMUM of the two over the frozen strength order — "never
    out-assert your identification", which is §3.2's own lead-in sentence.

    ⟨AMENDMENT 2026-08-12 — C-track, step 3b⟩ §3.2 states that principle and then
    enumerates it in two bullets: an `uncertain` identification forces
    `uncertain`, and a demoted identification demotes a `direct_witness` member
    while "members already asserting a non-direct relation keep their own". Those
    bullets and the principle DISAGREE in exactly one cell — identification
    `shared_text`, member `quotes_this_work` — because §3.1's frozen order puts
    `quotes_this_work` ABOVE `shared_text`, so the enumeration's literal reading
    would let that member out-assert its identification. The principle wins here,
    and the enumeration is read as what it is: a walk-through of the common
    cases, written with the direct-witness member in mind.

    Priced before it was decided, on the served asset
    (`discovery-v3-PUBLIC`, 2026-08-12): the disputed cell is **at most 53
    claim rows** in the panel's whole default population of 150,604 — a superset
    bound, since it counts every default-population `quotes_this_work` row under
    an identification that MIGHT reach step 2. A0b ratifies §3.2; this note is
    what it ratifies against.

    Absence, both kinds, renders ``uncertain``:

    * **No identification to cap against** (`None`). §3.2 assumes one exists;
      §5a.1 rules the case where none does — "stored relation absent →
      `uncertain`", §2's missing-input rule — for the expansion pane, and the
      same reading applies here for the same reason: there is no verdict to cap
      against, and a member asserting on its own would be asserting more than
      anything published about it. Measured: this never happens in the panel's
      DEFAULT population (0 of 150,604 rows), and happens for 52,510 of 231,322
      rows behind the review toggle.
    * **Anything outside the two vocabularies** — a NULL member, a typo, a
      future token, or `work_quotes_page` on either side. Fails closed, exactly
      as step 6 does.
    """
    member_rank = _RELATION_STRENGTH.get(member_relation_kind)
    identification_rank = _RELATION_STRENGTH.get(identification_relation)
    if member_rank is None or identification_rank is None:
        return ids.RENDERED_RELATION_FAIL_CLOSED
    if identification_rank < member_rank:
        return identification_relation
    return member_relation_kind


def work_divergence_ratios(
    rows: Iterable[Tuple[str, Optional[str]]],
) -> Dict[str, float]:
    """Step 4a's per-work divergence ratios, from ``(canonical_work_id,
    novelty_status)`` pairs — one pair per identification IN THIS ASSET.

    The recipe is the A0a-2 census recipe verbatim: ``not_checked`` rows leave
    the denominator entirely (they are unmeasured, not negative), the numerator
    is the two divergence shades, and a work with fewer than
    ``WORK_DIVERGENCE_MIN_DENOMINATOR`` checked rows is OMITTED from the result.

    Omission rather than ``0.0`` is a deliberate, provably equivalent
    tightening: the sweep stored ``0.0`` for sub-floor works, and since
    ``quoter_threshold`` is validated into ``(0, 1]``, ``0.0 >= T`` is false for
    every admissible T — the two spellings render identically, while "absent"
    says the true thing about a work nobody could measure.
    """
    tallies: Dict[str, List[int]] = {}
    for canonical_work_id, novelty_status in rows:
        if novelty_status is None or novelty_status == NOVELTY_NOT_CHECKED:
            continue
        tally = tallies.setdefault(canonical_work_id, [0, 0])
        tally[1] += 1
        if novelty_status in DIVERGENCE_SHADES:
            tally[0] += 1
    return {
        canonical_work_id: divergent / checked
        for canonical_work_id, (divergent, checked) in tallies.items()
        if checked >= WORK_DIVERGENCE_MIN_DENOMINATOR
    }


# ---------------------------------------------------------------------------
# Reading the inputs off a materialized asset.
#
# EVERY matrix input is recoverable from stored columns, which is what makes the
# verifier's equality gate meaningful: the builder, the projector and the
# verifier all reconstruct the inputs through the one function below, so "the
# stored value equals the recomputed value" compares an asset against the
# contract rather than a file against itself.
# ---------------------------------------------------------------------------

# Step 1's stored encoding. `eligibility_basis` records WHICH rule admitted the
# identification: `shipped` means it had shipped evidence, `human_confirmed`
# means routing alone would NOT have admitted it -- i.e. exactly "no shipped
# evidence at all", the 9 rows of step 1. Reading the flag rather than
# re-joining discovery_evidence keeps the recompute a function of the
# identification grain, which is the grain the column lives at.
_SHIPPED_BASIS = ids.ROUTING_STATUS_SHIPPED


class RegionInputUnavailable(RelationMatrixError):
    """Raised when step 3 is ACTIVE but no footprint recipe is wired.

    Fails the build rather than recomputing with `None`: a verifier that quietly
    treated "I cannot compute the region input" as "the region does not fire"
    would pass every row the builder demoted, which is the exact shape of a
    vacuous gate. The footprint recipe arrives with the D-track locus import.
    """


def iter_relation_inputs(
    conn: sqlite3.Connection,
    parameterization: MatrixParameterization = DEPLOY_1_PARAMETERIZATION,
) -> Iterator[Tuple[str, RelationInputs]]:
    """Yield ``(identification_id, RelationInputs)`` for every row of a
    materialized ``discovery_identification`` table, in a stable id order.

    Two passes over the table: the first tallies step 4a's work-level
    divergence, the second yields the per-row inputs. Both read only stored
    columns plus ``discovery_curated_quoter``.
    """
    if parameterization.region_active:
        # Guard, not a TODO: see RegionInputUnavailable.
        raise RegionInputUnavailable(
            "step 3 (region) is active in this parameterization, but no matched-"
            "footprint recipe is wired yet (D-track locus import). Refusing to "
            "recompute rendered relations with an absent region input."
        )

    divergence = work_divergence_ratios(
        conn.execute(
            "SELECT canonical_work_id, novelty_status FROM discovery_identification"
        )
    )
    curated = curated_quoter_work_ids(conn)

    for row in conn.execute(
        """
        SELECT identification_id, canonical_work_id, eligibility_basis,
               routing_reason, max_coverage_ppm, relation_kind
        FROM discovery_identification
        ORDER BY identification_id
        """
    ):
        identification_id, canonical_work_id, eligibility_basis = row[0], row[1], row[2]
        routing_reason, max_coverage_ppm, relation_kind = row[3], row[4], row[5]
        yield identification_id, RelationInputs(
            has_shipped_evidence=(eligibility_basis == _SHIPPED_BASIS),
            routing_reason=routing_reason,
            footprint_all_non_discriminative=None,
            work_divergence=divergence.get(canonical_work_id),
            on_curated_quoter_list=canonical_work_id in curated,
            coverage_known=max_coverage_ppm is not None,
            stored_relation_kind=relation_kind,
        )


def curated_quoter_work_ids(conn: sqlite3.Connection) -> frozenset:
    """The canonical work ids on this asset's curated quoter list.

    An ABSENT table yields the empty set — a pre-batch asset has no curated
    list, and step 4b simply cannot fire on it. An absent table is not the same
    as an unreadable one: any other SQL error propagates.
    """
    present = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' "
        "AND name='discovery_curated_quoter'"
    ).fetchone()
    if not present:
        return frozenset()
    return frozenset(
        r[0] for r in conn.execute(
            "SELECT canonical_work_id FROM discovery_curated_quoter"
        )
    )


def recompute_and_store(
    conn: sqlite3.Connection,
    parameterization: MatrixParameterization = DEPLOY_1_PARAMETERIZATION,
) -> Dict[str, int]:
    """Recompute ``rendered_relation`` for every identification and store it.

    Returns the census by rendered state. Called by the builder (after the grain
    is materialized) and by the projector (after public pruning, whose different
    row population legitimately produces different step-4a denominators).
    """
    updates = []
    census: Dict[str, int] = {state: 0 for state in sorted(ids.RENDERED_RELATIONS)}
    for identification_id, inputs in iter_relation_inputs(conn, parameterization):
        rendered = render_relation(inputs, parameterization)
        census[rendered] += 1
        updates.append((rendered, identification_id))
    conn.executemany(
        "UPDATE discovery_identification SET rendered_relation = ? "
        "WHERE identification_id = ?",
        updates,
    )
    return census


def stored_relation_mismatches(
    conn: sqlite3.Connection,
    parameterization: MatrixParameterization,
    *,
    limit: Optional[int] = None,
) -> List[Tuple[str, str, str]]:
    """Row-for-row recompute-equality: return
    ``(identification_id, stored, recomputed)`` for every disagreement.

    The verifier's gate. An empty list is the only passing result.
    """
    stored = dict(conn.execute(
        "SELECT identification_id, rendered_relation FROM discovery_identification"
    ))
    out: List[Tuple[str, str, str]] = []
    for identification_id, inputs in iter_relation_inputs(conn, parameterization):
        recomputed = render_relation(inputs, parameterization)
        if stored.get(identification_id) != recomputed:
            out.append((identification_id, stored.get(identification_id), recomputed))
            if limit is not None and len(out) >= limit:
                break
    return out


def parameterization_meta_rows(
    parameterization: MatrixParameterization,
) -> List[Tuple[str, str]]:
    """The ``(key, value)`` meta rows recording which parameterization an
    asset's stored relations were produced under.

    Written by every builder/projector that stores the column, and read back by
    the verifier. The threshold is stored as an empty string when unset so the
    key is always present — a missing key and an unset value must not be the
    same observation.
    """
    t = parameterization.quoter_threshold
    return [
        (_META_KEY_VERSION, MATRIX_VERSION),
        (_META_KEY_REGION_ACTIVE, "1" if parameterization.region_active else "0"),
        (_META_KEY_QUOTER_THRESHOLD, "" if t is None else repr(float(t))),
    ]


def parameterization_from_meta(meta: Mapping[str, str]) -> MatrixParameterization:
    """Reconstruct the parameterization an asset was built under.

    Raises rather than defaulting on anything malformed or missing: the
    verifier's equality gate is only meaningful if it recomputes under the
    asset's OWN parameterization, so "assume deploy 1" would silently pass rows
    stored under a different one.
    """
    missing = [k for k in PARAMETERIZATION_META_KEYS if k not in meta]
    if missing:
        raise RelationMatrixError(
            "meta is missing relation-matrix parameterization keys: "
            + ", ".join(sorted(missing))
        )
    version = meta[_META_KEY_VERSION]
    if version != MATRIX_VERSION:
        raise RelationMatrixError(
            f"asset was built under relation matrix {version!r}, "
            f"this code implements {MATRIX_VERSION!r} — refusing to recompute "
            "across matrix versions"
        )
    region_raw = meta[_META_KEY_REGION_ACTIVE]
    if region_raw not in ("0", "1"):
        raise RelationMatrixError(
            f"{_META_KEY_REGION_ACTIVE} must be '0' or '1', got {region_raw!r}"
        )
    threshold_raw = meta[_META_KEY_QUOTER_THRESHOLD]
    threshold: Optional[float]
    if threshold_raw == "":
        threshold = None
    else:
        try:
            threshold = float(threshold_raw)
        except (TypeError, ValueError):
            raise RelationMatrixError(
                f"{_META_KEY_QUOTER_THRESHOLD} must be a float or empty, "
                f"got {threshold_raw!r}"
            ) from None
    return MatrixParameterization(
        region_active=(region_raw == "1"),
        quoter_threshold=threshold,
    )
