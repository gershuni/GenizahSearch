# -*- coding: utf-8 -*-
"""The single async, read-only ``DiscoveryService`` chokepoint (Phase 134,
DATA-06) through which all web access to the ``discovery.db`` sidecar flows.

Modeled on the established ``shared/*_service.py`` sidecar-service shape
(``shared/fjms_service.py``: module-level sidecar open + graceful-absent
reads + a ``meta`` version accessor) composed with the off-event-loop async
pattern already proven in ``web/search_api.py`` (``run_in_executor`` +
``asyncio.wait`` -- NEVER ``asyncio.wait_for`` over ``run_in_executor``,
since executor threads are not cancellable -- plus non-blocking bounded
concurrency semaphores: one budget for heavy corpus-wide queries and a
separate, larger one for the per-page browse path. EVERY executor crossing
takes one of the two; there is no unbounded dispatch.

Key invariants (134-06 must_haves):
  - ``__init__`` takes INJECTED LAZY providers (``path_provider``,
    ``availability_callable``, ``sidecar_version_provider``) and stores them
    WITHOUT touching the DB (F15) -- importing a module that constructs a
    ``DiscoveryService`` at import time (``web/discovery.py``) before the
    sidecar loader has run must never bind a stale/empty path.
  - The ``ThreadLocalConnection`` is built LAZILY on the first AVAILABLE
    call, and RECREATED whenever the resolved path or sidecar version
    changes -- the PRIOR ``ThreadLocalConnection`` pool is ``.close()``d
    first so no per-thread handle is ever leaked (R8).
  - Every sync read method follows the fjms_service graceful-absent shape:
    unavailable or any exception -> return ``[]``/``None``, NEVER raise.
  - Every async wrapper dispatches its sync method via
    ``loop.run_in_executor(...)`` wrapped in ``asyncio.wait({fut},
    timeout=...)`` (never ``wait_for``); a timeout raises
    ``DiscoveryUnavailable`` WITHOUT awaiting the abandoned future.
  - EVERY read acquires a non-blocking bounded semaphore before its executor
    crossing -- the HEAVY budget for corpus-wide reads
    (``get_work_witnesses``, findings, facets, launch stats, the per-work
    expansion), the BROWSE budget for everything else. The slot is released
    from the future's ``add_done_callback`` (never a bare ``finally``) so a
    timed-out thread cannot re-admit new work until it truly finishes (DC6).
    A cache HIT takes no slot, because it runs no query.
  - Each budget class dispatches into its OWN ``ThreadPoolExecutor`` whose
    ``max_workers`` EQUALS that class's capacity, never the shared default
    ``run_in_executor`` pool. Over one shared pool the two budgets are two
    names for one budget: browse work can occupy or queue ahead of every
    worker in a pool this repository does not configure, and a heavy read
    then times out while its own semaphore still has capacity. The executors
    are built lazily (a flag-OFF process pays nothing) and shut down by a
    ``weakref.finalize`` that closes over the dict, never over ``self``.
  - The browse-enrichment reads (``get_claims_for_page`` /
    ``get_pages_related_to_page``) are wrapped in a small bounded LRU keyed
    INCLUDING the sidecar version, so a version swap never serves stale
    cached rows (F15).
  - ``get_work_witnesses`` implements the DATA-10 unit x work projection: a
    physical-MS witness_unit is shown ONCE at its highest member band; the
    enabled-band filter acts on that DISPLAYED band BEFORE pagination; the
    anchor's own unit is excluded; same-unit members are suppressed from the
    list (the grouping/filtering/sorting/pagination logic itself is a pure,
    DB-free helper -- ``_project_work_witnesses`` -- directly unit-testable).

All timeout/concurrency/LRU/page-size defaults are read from
``docs/specs/discovery-budgets.md``'s ``DISCOVERY_*`` env-var convention,
re-read PER CALL (never baked in at import time) so they can be tuned in
production without a restart.

THE LAUNCH STATISTICS (owner ruling U, plan 136-22) -- EVERY BASIS, IN SQL
------------------------------------------------------------------------
``get_launch_stats_enveloped`` returns the release's headline contribution
figure and its three shades. "Fragments" and "pages" are ambiguous across at
least four plausible populations in this schema, and that ambiguity is exactly
how an earlier draft produced a total by adding a main-pool count to two
unfiltered ones. So every key is named after its basis, and every basis is
stated here in SQL terms, so a later reader can VERIFY a number rather than
trust it. ``<shades>`` is ``LAUNCH_CONTRIBUTION_SHADES``.

===============================  =========================================
envelope key                     the query it is
===============================  =========================================
``total``                        the SUM of the three ``items`` rows below
                                 (never a separately-counted number)
``items[i].identification_count``  ``SELECT COUNT(*) FROM
                                 discovery_identification WHERE main_pool = 1
                                 AND novelty_status = <shade>``
``items[i].manuscript_count``    ``SELECT COUNT(DISTINCT sys_id)`` over the
                                 same rows
``meta.main_pool_manuscript_count``  ``SELECT COUNT(DISTINCT sys_id) FROM
                                 discovery_identification WHERE main_pool = 1
                                 AND novelty_status IN <shades>``. NOT the sum
                                 of the per-shade manuscript counts: a
                                 manuscript contributing under two shades is
                                 counted once here and twice there.
``meta.main_pool_total``         ``SELECT COUNT(*) FROM
                                 discovery_identification WHERE main_pool = 1``
                                 -- **NO ``novelty_status`` predicate at all.**
``meta.main_pool_total_manuscript_count``  ``SELECT COUNT(DISTINCT sys_id)``
                                 over the same rows, again with no shade
                                 predicate.
``meta.identification_total``    ``SELECT COUNT(*) FROM
                                 discovery_identification`` -- **no
                                 ``main_pool`` predicate and no
                                 ``novelty_status`` predicate.** The all-in-all
                                 figure the headline ledes with (owner ruling,
                                 2026-08-05). It is NOT ``all_bucket_total``,
                                 which is shade filtered and a different
                                 population; substituting one for the other is
                                 the mixed-basis defect ruling U was issued
                                 over. It is its own ``COUNT(*)`` rather than a
                                 sum of the two pool keys below: the identity
                                 holds today because ``main_pool`` partitions
                                 the table, and a UI that added two separately
                                 derived figures would go wrong the first time
                                 either one's basis moved, silently.
``meta.work_total``              ``SELECT COUNT(DISTINCT display_work_id)
                                 FROM discovery_identification`` -- how many
                                 distinct WORKS the identifications name, again
                                 with no predicate of any kind.
                                 ``display_work_id`` is not a choice: it is the
                                 column ``_FINDINGS_UNIT_GROUP_BY`` groups the
                                 per-work row unit by, so this figure and the
                                 row count that unit returns are one population
                                 by construction. NOTE that it is the UNION
                                 over both buckets and is therefore larger than
                                 either bucket's own work count; no surface may
                                 present it as reachable through the bucket
                                 control, which offers the two buckets and no
                                 union between them.
``meta.more_pool_total``         ``SELECT COUNT(*) FROM
                                 discovery_identification WHERE main_pool = 0``
                                 -- the SECOND pool's SIZE, again with no
                                 ``novelty_status`` predicate. It is the exact
                                 complement of ``main_pool_total`` on one
                                 stated basis, and the two are never summed:
                                 their sum is ``COUNT(*)`` over the table,
                                 which is a third population nothing here
                                 reports.
``meta.all_bucket_total``        the same as ``total`` with the ``main_pool``
                                 predicate DROPPED. Ruling U constraint 1
                                 permits a page to show this only if it says
                                 so in words -- so it lives under its own
                                 named key and is NEVER merged into ``total``.
``meta.all_bucket_manuscript_count``  ditto for the manuscript count
``meta.corpus_manuscript_count``  ``SELECT COUNT(DISTINCT sys_id) FROM
                                 discovery_identification`` -- every bucket,
                                 every shade. **It counts the fragments THIS
                                 RELEASE IDENTIFIED SOMETHING ON, not the
                                 corpus**: the project's corpus is ~255,615
                                 manuscript records (``libraries.csv``), and a
                                 surface that labelled this figure "the whole
                                 corpus" would present the denominator of what
                                 we already matched as the denominator of
                                 everything -- a coverage overclaim of roughly
                                 6.6x. The key keeps its NAME (renaming a
                                 shipped key is a separate, breaking change)
                                 and every reader-facing string that uses it
                                 now says what it counts.
``meta.corpus_page_count``       ``SELECT COUNT(DISTINCT page_id) FROM
                                 discovery_claim`` -- every page carrying at
                                 least one claim. Again NOT the corpus's page
                                 count, and the rendered wording says so.
===============================  =========================================

**``main_pool_total`` AND ``total`` ARE TWO DIFFERENT POPULATIONS.** ``total``
is and stays the SHADE-FILTERED contribution figure -- what the release adds to
the finding aids -- while ``main_pool_total`` counts every main-pool
identification whatever its novelty shade, including the shades that are not a
contribution at all. Substituting one for the other, or summing them, or
pairing ``main_pool_total`` with ``main_pool_manuscript_count`` (which IS shade
filtered), reproduces exactly the mixed-basis defect ruling U was issued over:
a figure built by adding counts taken on different bases. The same applies to
the pair ``main_pool_total`` / ``main_pool_total_manuscript_count``, which is
internally consistent and must be used together.

``meta.more_pool_total`` exists because the OWNER RULED (2026-08-05) that the
second pool's size may be shown. It was deliberately withheld until then: a
number advertising that pool is an owner ruling rather than a reader's
convenience. What the ruling does NOT touch is the prohibition it sits beside
-- the owner's QUALITY assessment of that pool must never become a percentage,
a score, an interval or any number at all, on this surface or any other. A
count of what is IN a pool is a different kind of fact from a judgement about
it, and this key is only ever the first. Ruling T is likewise untouched: the
BUCKET CONTROL still carries no count, and a test asserts no digit appears in
its subtree.

``meta.basis`` states the single basis (``main_pool``) explicitly, so no
consumer has to infer it and no consumer can mix bases silently.
``meta.sidecar_version`` and ``meta.audience`` come from the ARTIFACT's own
``meta`` table, not from an injected provider: the same query against the
public projection and the private rebuild returns two different, both-correct
answers, so a number without the provenance of the artifact that produced it
is not interpretable.

NOTHING this reader emits is a precision, an accuracy rate, a confidence
interval, a review badge or a percentage. "The finding aids did not already
have it" is a claim about the AIDS, not about the match, and that distinction
is the whole basis on which these numbers may be published at all.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sqlite3
import threading
import weakref
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

from shared.discovery_band_labels import serialize_banded_claim
from shared.discovery_errors import DiscoveryOverload, DiscoveryUnavailable
from shared.discovery_novelty import (
    NOVELTY_STATUS_ORDER,
    NOVELTY_STATUSES,
    is_hidden_by_default,
)
from shared.discovery_surface_projection import (
    STATUS_OK,
    busy_envelope,
    make_envelope,
    surface_safe_claim,
    surface_safe_expansion,
    surface_safe_facet,
    surface_safe_finding,
    surface_safe_launch_shade,
    surface_safe_related_page,
    surface_safe_work_summary,
    timeout_envelope,
    unavailable_envelope,
)
from shared.thread_local_db import ThreadLocalConnection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Env-var defaults (docs/specs/discovery-budgets.md SS2/SS3) -- re-read per
# call, never cached at import time.
# ---------------------------------------------------------------------------

_DEFAULT_QUERY_TIMEOUT_BROWSE = 2.0
_DEFAULT_QUERY_TIMEOUT_WORK = 5.0
_DEFAULT_MAX_CONCURRENT_QUERIES = 4
#: The BROWSE-path bound, separate from the heavy one and deliberately larger.
#:
#: Both are the same non-blocking fast-fail shape; only the number differs, and
#: the number has to differ. The heavy cap of 4 is sized for corpus-wide
#: queries that a caller issues ONE of. A cold connections-panel load issues
#: THREE reads concurrently and then a fourth, so a cap of 4 would put the
#: SECOND simultaneous browse visitor into `busy` -- a self-inflicted outage on
#: an already-shipped page, which is not what "bounded" is for. 24 admits eight
#: concurrent cold panel loads. Warm loads cost NO slot at all: the
#: version-keyed LRU returns before any dispatch.
#:
#: The two budgets are only really separate because each has its OWN executor
#: (see `_executor_for`). Sized against the shared default `run_in_executor`
#: pool the split was nominal: 24 browse jobs could occupy or queue ahead of
#: every worker in a pool this repository never configures and whose width is
#: not guaranteed, so a heavy read could time out while its own semaphore still
#: had capacity -- reproducible with a two-worker default executor. A budget is
#: only a budget when a slot guarantees a worker, so each class's semaphore
#: capacity IS its executor's `max_workers` (code review round 13, finding 2).
_DEFAULT_MAX_CONCURRENT_BROWSE_QUERIES = 24
_DEFAULT_BROWSE_LRU_MAX_ENTRIES = 5000
_DEFAULT_PAGE_SIZE_DEFAULT = 50
_DEFAULT_PAGE_SIZE_MAX = 200

# M3: the frozen ABSOLUTE row-per-page ceiling (docs/specs/discovery-budgets.md
# SS3: "hard ceiling; never overridable above this"). DISCOVERY_PAGE_SIZE_MAX
# may only ever TIGHTEN this value, never raise it -- see _clamp_page_size.
_ABSOLUTE_PAGE_SIZE_CEILING = 200

# H1: there is NO pre-grouping raw-claim cap here (a previous
# _MAX_RAW_CLAIMS_PER_WORK=5000 / _MAX_UNIT_MEMBER_ROWS=200_000 pair silently
# dropped units on any work with more claims than that -- get_work_witnesses
# now performs the whole DATA-10 unit x work projection IN SQL, so
# LIMIT/OFFSET paginates over UNITS post-grouping, never over a truncated
# pre-grouping raw-claim scan; see get_work_witnesses below).


def _get_float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw:
        try:
            return float(raw)
        except (TypeError, ValueError):
            pass
    return default


def _get_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw:
        try:
            return int(raw)
        except (TypeError, ValueError):
            pass
    return default


def _get_positive_int_env(name: str, default: int) -> int:
    """Like ``_get_int_env``, but coerces to >= 1 (M3) -- a non-positive or
    unparsable env value falls back to ``default`` rather than propagating
    into ``asyncio.Semaphore(n)`` (which raises ValueError for n < 0 and
    means "always locked" for n == 0, either of which would silently break
    or permanently overload the service from a single bad env var)."""
    value = _get_int_env(name, default)
    return value if value >= 1 else default


def _get_positive_float_env(name: str, default: float) -> float:
    """Like ``_get_float_env``, but coerces to > 0 (M3) -- a non-positive
    timeout would make ``asyncio.wait(..., timeout=X)`` return immediately
    on every call, permanently overloading the service from a single bad
    env var; falls back to ``default`` instead."""
    value = _get_float_env(name, default)
    return value if value > 0 else default


def _shutdown_executors(executors: Dict[str, "ThreadPoolExecutor"]) -> None:
    """Retire a collected service's per-budget threadpools.

    Registered through ``weakref.finalize`` over the DICT and never over the
    service, because a finalizer holding ``self`` would keep the very object it
    is meant to clean up alive. ``wait=False``: an in-flight query is left to
    finish on its own thread, exactly as a timed-out read already is.
    """
    for executor in list(executors.values()):
        try:
            executor.shutdown(wait=False)
        except Exception:                                    # pragma: no cover
            pass
    executors.clear()


def _parse_json_field(value: Optional[str]) -> Any:
    """Best-effort JSON parse for the seed_spans/seed_ms_ids TEXT columns.
    Returns None for a NULL cell, the parsed object on success, or the raw
    string unchanged if it somehow isn't valid JSON (defensive; never
    raises)."""
    if value is None:
        return None
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return value


# ---------------------------------------------------------------------------
# Frozen global band-rank (docs/specs/discovery-sidecar-schema-v1.md SS6 item
# 2), strongest first. Inlined here (rather than importing
# scripts/discovery_ids.py) to keep this runtime module decoupled from the
# offline-build script tree -- mirrors the same decision already made in
# web/discovery_assets.py (see its own enum-vocab spot-check comment).
# ---------------------------------------------------------------------------

# v1-read-compat window (Codex #8): the strongest track1_direct band is listed
# under BOTH the v2 key `high_confidence_algorithmic` AND the v1 key
# `expert_verified` at the same (top) rank position, so the band-rank lattice
# ranks whichever key the currently-loaded asset carries. A built v2 asset
# uses only the v2 key (the offline verifier's no-mixed-enum-state check
# enforces that); do NOT drop `expert_verified` until the v2 manifest is live
# (135-08) -- the v1 asset + the v1 fixture tests still read it.
_BAND_RANK_ORDER: List[Tuple[str, str]] = [
    ("track1_direct", "high_confidence_algorithmic"),
    ("track1_direct", "expert_verified"),
    ("track1_direct", "tier_a"),
    ("propagated", "corroborated"),
    ("track1_direct", "screening_rb"),
    ("track1_direct", "screening_canon"),
    ("propagated", "weak"),
    ("propagated", "not_evaluated"),
]
_BAND_RANK_INDEX: Dict[Tuple[str, str], int] = {
    pair: i for i, pair in enumerate(_BAND_RANK_ORDER)
}
_UNRANKED_BAND = len(_BAND_RANK_ORDER)


def _band_rank(evidence_source: Optional[str], confidence_band: Optional[str]) -> int:
    """Lower is "stronger" (rank 0 = expert_verified, the strongest band)."""
    return _BAND_RANK_INDEX.get((evidence_source, confidence_band), _UNRANKED_BAND)


#: The page component of a corpus page id, `{sys_id}_{ie_id}_P{n:06d}_{fl_id}`.
_PAGE_ID_PAGE_NUMBER_RE = re.compile(r"_P(\d+)_")


def _page_number_from_page_id(page_id: Any) -> Optional[int]:
    """The folio number carried INSIDE a page id, or None.

    Parsed in the SERVICE so no surface has to know the id's shape to show a
    page number -- and so that no surface has a reason to hold the composite id
    at all. Returns None rather than raising on any id that does not carry one:
    a missing page number is a row that says less, while an exception here
    would take down a section that has already been fetched.
    """
    if not isinstance(page_id, str):
        return None
    match = _PAGE_ID_PAGE_NUMBER_RE.search(page_id)
    if not match:
        return None
    try:
        return int(match.group(1))
    except ValueError:                                       # pragma: no cover
        return None


def _build_band_rank_case_sql() -> str:
    """Build the frozen band-rank lattice as a SQL CASE expression (H1) --
    ``_BAND_RANK_ORDER`` is a frozen module-level constant (never user
    input), so building SQL via string formatting here is safe. Used to
    perform the DATA-10 unit x work projection's "highest member band"
    selection IN SQL (a ``ROW_NUMBER() OVER (PARTITION BY unit_key ...)``
    window-function query) instead of in Python, so ``LIMIT``/``OFFSET``
    can paginate over UNITS post-grouping rather than a pre-grouping
    raw-claim cap."""
    lines = ["CASE"]
    for i, (source, band) in enumerate(_BAND_RANK_ORDER):
        lines.append(
            f"    WHEN de.evidence_source = '{source}' AND de.confidence_band = '{band}' THEN {i}"
        )
    lines.append(f"    ELSE {_UNRANKED_BAND}")
    lines.append("  END")
    return "\n".join(lines)


_BAND_RANK_CASE_SQL = _build_band_rank_case_sql()


# ---------------------------------------------------------------------------
# D-13g: the default-surface routing predicate.
#
# The bug this replaces: the page query filtered `routing_status = 'shipped'`
# in SQL, while `shared/discovery_band_labels.py::is_default_eligible` returns
# True for `human_confirmed` UNCONDITIONALLY, before it inspects routing --
# and `discovery-band-labels-v1.md` SS4 says the same. So a human-confirmed row
# that routing had demoted was dropped by the query BEFORE the predicate meant
# to protect it ever ran: 19 of the 121 human-confirmed rows across all
# human-confirmed evidence, and 14 of 116 on the DISPLAY evidence this query
# actually reads. Live symptom: on one manuscript, one page's human-confirmed
# row was hidden while another page's human-confirmed row showed -- two rows a
# human confirmed, treated differently.
#
# The fix is in SQL, not in a display-layer patch, and it MIRRORS the
# eligibility rule the build already materializes into
# `discovery_identification` (`scripts/build_discovery_sidecar.py::
# populate_discovery_identification`) -- so the restore cannot be undone one
# layer down by the identification join.
# ---------------------------------------------------------------------------

_CLAIMS_DEFAULT_ROUTING_CLAUSE = (
    "AND (de.routing_status = 'shipped' "
    "OR de.adjudication_status = 'human_confirmed')"
)

# The manuscript scope is served by `page_id IN (...)` over the browse page's
# own page list, NOT by `discovery_evidence.sys_id` -- that column has no
# index, while `ix_discovery_claim_page_id` exists and is confirmed in use by
# EXPLAIN QUERY PLAN (asserted by a test, not assumed). Bounded so an
# unbounded caller cannot build a giant IN list: the largest manuscript in the
# corpus carries 427 claims, and 429 manuscripts carry over 50.
_MAX_MANUSCRIPT_PAGE_IDS = 500


# ---------------------------------------------------------------------------
# 136-14 Task 3: the corpus-wide findings query ("Computed Identifications").
#
# THREE row units, ONE implementation. Serving them from one parameterised
# builder is not tidiness -- it is the only way filter, sort and count
# semantics cannot diverge between the unit a reader selects and the unit the
# counts were computed over.
#
# The base grain is `discovery_identification`, NEVER a claim-row scan: that
# shape measured 3.41-3.55 s against a 1.5 s cap, and the deduped count alone
# measured 16 s. Over the materialized grain the same shapes measure 124 ms p95
# in production, with the visible count at 0.46 ms against its own 500 ms cap.
#
# The domain axis is the IDENTIFIED WORK's (`works.genre`, reached through
# `display_work_id`), never the manuscript's catalogue domain -- see the
# wrong-axis guard in tests/test_discovery_findings_query.py.
# ---------------------------------------------------------------------------

FINDINGS_UNIT_IDENTIFICATION = "identification"
FINDINGS_UNIT_MANUSCRIPT = "manuscript"
FINDINGS_UNIT_WORK = "work"

#: The three OFFERED units. `claim` is deliberately absent: the same
#: identification repeats once per folio, which inflates same-work matches
#: ~2.3x relative to citations (a fragment that copies a work matches on every
#: folio; a citation matches once).
FINDINGS_UNITS: frozenset = frozenset({
    FINDINGS_UNIT_IDENTIFICATION, FINDINGS_UNIT_MANUSCRIPT, FINDINGS_UNIT_WORK,
})

FINDINGS_SORT_BAND_RANK = "band_rank"
FINDINGS_SORT_PAGE_COUNT = "page_count"
FINDINGS_SORT_MATCHED_TEXT = "matched_text"

#: The three offered sorts. `novelty` is deliberately absent: absence from a
#: finding aid is not evidence a match is correct, and ordering by it would
#: imply otherwise (D-15a / D-24). Novelty filters and may group; it never
#: orders.
FINDINGS_SORTS: frozenset = frozenset({
    FINDINGS_SORT_BAND_RANK, FINDINGS_SORT_PAGE_COUNT, FINDINGS_SORT_MATCHED_TEXT,
})


def findings_novelty_offered(unit: str) -> bool:
    """Whether the candidacy axis applies to `unit`. THE authority on that rule.

    Novelty is a verdict about ONE work on ONE fragment: no finding aid we
    checked records this work there. The per-work row unit collapses many
    manuscripts into a single line, and those lines carry no single verdict --
    so the filter is not offered there, `_build_findings_filter` REFUSES the
    combination, and `get_findings_enveloped` reports it as
    `meta['novelty_offered']`.

    It is a function and not a comparison spelled out at each site because a
    SURFACE has to know the same rule BEFORE it calls: `web/pages/findings.py`
    disables the switch and drops the selection on this predicate. When the rule
    lived only inside the raiser, the page left the switch live while the "Show
    as" control changed the unit underneath it, and a reader who turned novelty
    on and then chose one row per work drove the shipped builder into its own
    `ValueError` -- an unhandled failure on a live page (code review round 15,
    finding 1). One predicate, three callers, no restatement.
    """
    return unit != FINDINGS_UNIT_WORK

#: Ruling F's hidden-by-default shades, as a DETERMINISTICALLY ORDERED tuple.
#:
#: DERIVED, never restated. `shared.discovery_novelty.is_hidden_by_default` is
#: the policy; this filters the canonical shade ORDER through it, so the SQL
#: predicate below and the predicate the rest of the project tests against
#: cannot come to disagree -- a shade joining or leaving the policy moves this
#: tuple with it, and a second literal list here would not move at all. The
#: ordering comes from `NOVELTY_STATUS_ORDER` rather than from iterating the
#: frozenset, because a set's iteration order is not stable across processes
#: and a query's bound parameters must be reproducible.
#:
#: Ruling F's own rationale (`136-GATE1-DECISIONS.md` section F): these rows
#: are neither auto-hidden by policy nor silently trusted. The SYSTEM never
#: treats the catalogue's disagreement as a verdict; it surfaces the
#: disagreement behind an explicit, warned opt-in and lets the reader decide.
DIVERGENCE_SHADE_ORDER: Tuple[str, ...] = tuple(
    shade for shade in NOVELTY_STATUS_ORDER if is_hidden_by_default(shade)
)

BUCKET_MAIN = "main"
BUCKET_MORE = "more"
BUCKET_ALL = "all"
FINDINGS_BUCKETS: frozenset = frozenset({BUCKET_MAIN, BUCKET_MORE, BUCKET_ALL})

#: The visible bucket for a work the domain vocabulary cannot place. It is a
#: SELECTABLE value with a real count, never a silent disappearance.
DOMAIN_UNASSIGNED = "Unassigned"

FACET_LEVELS: frozenset = frozenset({"domain", "author", "work"})

_DEFAULT_QUERY_TIMEOUT_FINDINGS = 5.0
_DEFAULT_FINDINGS_PAGE_SIZE_DEFAULT = 50

# The per-unit SELECT lists. Every unit exposes the SAME output aliases
# (`main_pool`, `best_band_rank`, `page_count`, `max_coverage_ppm`), so ONE
# ORDER BY serves all three and a sort cannot mean different things on
# different units.
#
# `{divergent}` is the ONE substitution, filled by `_divergence_flag_sql` --
# a per-row CASE on the ungrouped unit and a MAX over the group on the two
# grouped ones. It is written as a template rather than three hand-written
# expressions for the same reason the rest of this table is data: the grouped
# units would otherwise be one careless edit away from reporting a mixed group
# as undivergent.
_FINDINGS_UNIT_SELECT: Dict[str, str] = {
    FINDINGS_UNIT_IDENTIFICATION: """
        di.identification_id           AS identification_id,
        di.sys_id                      AS sys_id,
        di.canonical_work_id           AS canonical_work_id,
        di.display_work_id             AS display_work_id,
        w.neutral_title                AS neutral_title,
        w.author                       AS author,
        w.genre                        AS genre,
        di.main_pool                   AS main_pool,
        di.main_pool_reason            AS main_pool_reason,
        di.best_band_rank              AS best_band_rank,
        di.page_count                  AS page_count,
        di.max_coverage_ppm            AS max_coverage_ppm,
        di.relation_kind               AS relation_kind,
        di.novelty_status              AS novelty_status,
        {divergent}                    AS divergent,
        1                              AS work_count,
        1                              AS manuscript_count,
        md.library_code                AS library_code,
        md.shelfmark_display           AS shelfmark_display
    """,
    FINDINGS_UNIT_MANUSCRIPT: """
        NULL                           AS identification_id,
        di.sys_id                      AS sys_id,
        NULL                           AS canonical_work_id,
        NULL                           AS display_work_id,
        NULL                           AS neutral_title,
        NULL                           AS author,
        NULL                           AS genre,
        MAX(di.main_pool)              AS main_pool,
        NULL                           AS main_pool_reason,
        MIN(di.best_band_rank)         AS best_band_rank,
        SUM(di.page_count)             AS page_count,
        MAX(di.max_coverage_ppm)       AS max_coverage_ppm,
        NULL                           AS relation_kind,
        CASE WHEN COUNT(DISTINCT di.novelty_status) = 1
             THEN MIN(di.novelty_status) ELSE NULL END AS novelty_status,
        {divergent}                    AS divergent,
        COUNT(DISTINCT di.display_work_id) AS work_count,
        1                              AS manuscript_count,
        MIN(md.library_code)           AS library_code,
        MIN(md.shelfmark_display)      AS shelfmark_display
    """,
    FINDINGS_UNIT_WORK: """
        NULL                           AS identification_id,
        NULL                           AS sys_id,
        MIN(di.canonical_work_id)      AS canonical_work_id,
        di.display_work_id             AS display_work_id,
        MIN(w.neutral_title)           AS neutral_title,
        MIN(w.author)                  AS author,
        MIN(w.genre)                   AS genre,
        MAX(di.main_pool)              AS main_pool,
        NULL                           AS main_pool_reason,
        MIN(di.best_band_rank)         AS best_band_rank,
        SUM(di.page_count)             AS page_count,
        MAX(di.max_coverage_ppm)       AS max_coverage_ppm,
        NULL                           AS relation_kind,
        NULL                           AS novelty_status,
        {divergent}                    AS divergent,
        1                              AS work_count,
        COUNT(DISTINCT di.sys_id)      AS manuscript_count,
        NULL                           AS library_code,
        NULL                           AS shelfmark_display
    """,
}

_FINDINGS_UNIT_GROUP_BY: Dict[str, Optional[str]] = {
    FINDINGS_UNIT_IDENTIFICATION: None,
    FINDINGS_UNIT_MANUSCRIPT: "di.sys_id",
    FINDINGS_UNIT_WORK: "di.display_work_id",
}

# The deterministic final tie-break per unit -- a page boundary must never
# depend on unspecified scan order.
_FINDINGS_UNIT_TIEBREAK: Dict[str, str] = {
    FINDINGS_UNIT_IDENTIFICATION: "di.identification_id ASC",
    FINDINGS_UNIT_MANUSCRIPT: "di.sys_id ASC",
    FINDINGS_UNIT_WORK: "di.display_work_id ASC",
}

# Sorts as FIXED SQL fragments over the shared output aliases -- a closed enum
# mapped to a constant, never user text interpolated into SQL (T-136-14-04).
# `max_coverage_ppm IS NULL` first puts unmeasurable rows LAST rather than
# treating a missing measurement as a zero.
_FINDINGS_QUALITY_ORDER = (
    "main_pool DESC, best_band_rank ASC, "
    "max_coverage_ppm IS NULL, max_coverage_ppm DESC"
)
_FINDINGS_SORT_SQL: Dict[str, str] = {
    FINDINGS_SORT_BAND_RANK: _FINDINGS_QUALITY_ORDER,
    FINDINGS_SORT_PAGE_COUNT: f"page_count DESC, {_FINDINGS_QUALITY_ORDER}",
    # The identification grain materializes COVERAGE, not matched letters
    # (matched_letters lives per evidence row, and all shipped propagated rows
    # have none at all). Coverage is therefore the grain's own measure of "how
    # much text matched"; the envelope's `sort_basis` names it, so the surface
    # never implies a letter count it does not have.
    FINDINGS_SORT_MATCHED_TEXT: (
        f"max_coverage_ppm IS NULL, max_coverage_ppm DESC, {_FINDINGS_QUALITY_ORDER}"
    ),
}
_FINDINGS_SORT_BASIS: Dict[str, str] = {
    FINDINGS_SORT_BAND_RANK: "best_band_rank",
    FINDINGS_SORT_PAGE_COUNT: "page_count",
    FINDINGS_SORT_MATCHED_TEXT: "max_coverage_ppm",
}

_FINDINGS_FROM = """
    FROM discovery_identification di
    JOIN works w ON w.work_id = di.display_work_id
    LEFT JOIN manuscript_display md ON md.sys_id = di.sys_id
"""

# ---------------------------------------------------------------------------
# Ruling U (plan 136-22): the launch statistics.
# ---------------------------------------------------------------------------

#: The THREE contribution shades, in the frozen order ruling U lists them.
#:
#: Defined ONCE, here, and never retyped at a call site: a shade added to the
#: novelty vocabulary later must not silently join the contribution total, and a
#: shade retired from it must not silently leave a smaller one behind.
LAUNCH_CONTRIBUTION_SHADES: Tuple[str, ...] = (
    "fills_gap",
    "refines_granularity",
    "container_predicts",
)

#: The single basis every launch figure is computed on (ruling U constraint 1).
LAUNCH_BASIS: str = "main_pool"


def _validate_contribution_shades(shades: Tuple[str, ...]) -> None:
    """Raise unless every contribution shade is in the novelty vocabulary.

    Called at IMPORT time below. A typo or a retired shade must fail loudly at
    import rather than producing a quietly smaller headline at request time --
    the shade simply matches no row, the total shrinks, and nothing raises.
    """
    unknown = sorted(set(shades) - NOVELTY_STATUSES)
    if unknown:
        raise RuntimeError(
            f"LAUNCH_CONTRIBUTION_SHADES names {unknown}, which is not in "
            "shared.discovery_novelty.NOVELTY_STATUSES -- a contribution shade "
            "outside the vocabulary matches no row and silently shrinks the "
            "launch total (ruling U constraint 2)"
        )
    if len(set(shades)) != len(shades):
        raise RuntimeError(
            "LAUNCH_CONTRIBUTION_SHADES repeats a shade -- a repeated shade "
            "would be double-counted into the total"
        )


_validate_contribution_shades(LAUNCH_CONTRIBUTION_SHADES)


def _build_launch_contribution_sql(*, main_pool_only: bool) -> Tuple[str, List[Any]]:
    """The ONE grouped statement both contribution figures are computed with.

    `main_pool_only=False` is the SAME statement with the `main_pool` predicate
    dropped -- one shape, not two separately-written queries, so the main-pool
    and all-bucket figures cannot drift apart through an edit to one of them.

    Returns per shade the identification count AND the distinct-manuscript
    count; the TOTAL is then the sum of the returned rows in Python, which is
    what makes the decomposition identity structural rather than asserted.
    """
    placeholders = ",".join("?" * len(LAUNCH_CONTRIBUTION_SHADES))
    where = [f"novelty_status IN ({placeholders})"]
    if main_pool_only:
        where.insert(0, "main_pool = 1")
    return (
        "SELECT novelty_status, COUNT(*) AS identification_count, "
        "COUNT(DISTINCT sys_id) AS manuscript_count "
        "FROM discovery_identification "
        "WHERE " + " AND ".join(where) + " "
        "GROUP BY novelty_status",
        list(LAUNCH_CONTRIBUTION_SHADES),
    )


def _build_launch_manuscript_sql(*, main_pool_only: bool) -> Tuple[str, List[Any]]:
    """Distinct CONTRIBUTING manuscripts, on the same basis as the total above.

    Not derivable by summing the per-shade manuscript counts: one manuscript can
    contribute under two shades, so the sum over-counts. Ruling U's "over 6,755
    manuscripts" is this number, never that sum.
    """
    placeholders = ",".join("?" * len(LAUNCH_CONTRIBUTION_SHADES))
    where = [f"novelty_status IN ({placeholders})"]
    if main_pool_only:
        where.insert(0, "main_pool = 1")
    return (
        "SELECT COUNT(DISTINCT sys_id) AS n FROM discovery_identification "
        "WHERE " + " AND ".join(where),
        list(LAUNCH_CONTRIBUTION_SHADES),
    )


_LIKE_ESCAPE_RE = re.compile(r"([\\%_])")


def _like_prefix(value: str) -> str:
    r"""`value` as a LIKE pattern matching its own children (`value / ...`),
    with `\`, `%` and `_` escaped so a domain containing one cannot behave as a
    wildcard. Bound as a PARAMETER; never interpolated."""
    return _LIKE_ESCAPE_RE.sub(r"\\\1", value) + " / %"


def _build_findings_filter(
    *, unit: str = FINDINGS_UNIT_IDENTIFICATION,
    bucket: str = BUCKET_MAIN,
    novelty: Optional[Iterable[str]] = None,
    include_divergent: bool = False,
    domain: Optional[str] = None,
    author: Optional[str] = None,
    work_id: Optional[str] = None,
) -> Tuple[str, List[Any]]:
    """The ONE findings predicate, shared by the row query and the facet
    counts.

    Building the predicate twice is exactly how a facet count and the result
    set it sits beside drift apart, so both callers come through here.

    Filters compose as AND; an empty filter set returns the whole current
    bucket. Every VALUE is bound -- the only interpolation is placeholder
    punctuation (T-136-14-04).

    `include_divergent` is ruling F's opt-in, and it is applied HERE -- in SQL,
    on the same predicate the count and the facet cascade are built from --
    rather than by dropping rows out of a fetched page. Post-filtering a page
    would leave `total`, the pager and every facet count describing a
    population the reader is not being shown, which is a lie the reader cannot
    see. The DEFAULT is `False`: divergent rows are ABSENT from the default
    render (ruling F's "hidden by default", stronger than decision E's earlier
    "excluded from the candidate toggle but shown normally").
    """
    if bucket not in FINDINGS_BUCKETS:
        raise ValueError(
            f"unknown findings bucket {bucket!r} -- offered buckets are "
            f"{sorted(FINDINGS_BUCKETS)}"
        )
    where: List[str] = []
    params: List[Any] = []

    if bucket == BUCKET_MAIN:
        where.append("di.main_pool = 1")
    elif bucket == BUCKET_MORE:
        where.append("di.main_pool = 0")

    if not include_divergent and DIVERGENCE_SHADE_ORDER:
        # A bare `NOT IN` is sound HERE and nowhere else in this file's reach:
        # SQL's three-valued logic makes `NULL NOT IN (...)` evaluate to NULL
        # (i.e. false in a WHERE clause), so on a nullable column this shape
        # would silently drop every row whose novelty was never recorded --
        # rows the absence of a verdict must never be conflated with a
        # disagreement. `discovery_identification.novelty_status` is declared
        # NOT NULL with the fail-closed `not_checked` default carrying the
        # "never checked" case as a REAL value, which is what makes the null
        # branch unreachable rather than merely unlikely. That schema
        # invariant is pinned by
        # `tests/test_discovery_findings_query.py::
        # test_the_divergence_filter_relies_on_a_pinned_not_null_column`, so a
        # future migration relaxing it fails a named test instead of quietly
        # shrinking this page.
        where.append(
            "di.novelty_status NOT IN (%s)"
            % ",".join("?" for _ in DIVERGENCE_SHADE_ORDER)
        )
        params.extend(DIVERGENCE_SHADE_ORDER)

    novelty_list = list(novelty) if novelty else []
    if novelty_list:
        if not findings_novelty_offered(unit):
            raise ValueError(
                "novelty is not offered on the per-work unit -- a work spanning "
                "many manuscripts has no single novelty verdict"
            )
        where.append("di.novelty_status IN (%s)" % ",".join("?" for _ in novelty_list))
        params.extend(novelty_list)

    if domain:
        if domain == DOMAIN_UNASSIGNED:
            where.append("(w.genre IS NULL OR w.genre = '' OR w.genre = ?)")
            params.append(DOMAIN_UNASSIGNED)
        else:
            # Matches the domain itself OR any leaf beneath it, so a parent
            # selection is a strict superset of each of its leaves.
            where.append(r"(w.genre = ? OR w.genre LIKE ? ESCAPE '\')")
            params.extend([domain, _like_prefix(domain)])

    if author:
        if author == DOMAIN_UNASSIGNED:
            where.append("(w.author IS NULL OR w.author = '')")
        else:
            where.append("w.author = ?")
            params.append(author)

    if work_id:
        where.append("di.display_work_id = ?")
        params.append(work_id)

    return ("WHERE " + " AND ".join(where)) if where else "", params


def _divergence_flag_sql(*, aggregate: bool) -> Tuple[str, List[Any]]:
    """`(expression, params)` for a findings row's own divergence flag.

    `aggregate=True` wraps the per-identification CASE in `MAX(...)`, which is
    what the two GROUPED units need: a manuscript or work row is divergent when
    ANY identification under it is, and `novelty_status` cannot answer that (it
    is NULL on a mixed group by construction).

    Returns the constant `0` when the hidden-by-default policy selects no shade
    at all, so the flag stays a real boolean rather than becoming invalid SQL.
    """
    if not DIVERGENCE_SHADE_ORDER:
        return "0", []
    case = (
        "CASE WHEN di.novelty_status IN (%s) THEN 1 ELSE 0 END"
        % ",".join("?" for _ in DIVERGENCE_SHADE_ORDER)
    )
    return (f"MAX({case})" if aggregate else case), list(DIVERGENCE_SHADE_ORDER)


def _build_findings_query(
    *, unit: str = FINDINGS_UNIT_IDENTIFICATION,
    sort: str = FINDINGS_SORT_BAND_RANK,
    bucket: str = BUCKET_MAIN,
    novelty: Optional[Iterable[str]] = None,
    include_divergent: bool = False,
    domain: Optional[str] = None,
    author: Optional[str] = None,
    work_id: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
    count_only: bool = False,
    count_cap: Optional[int] = None,
) -> Tuple[str, List[Any]]:
    """Build the findings query for ONE unit. The single query builder -- all
    three offered units come through here.

    Returns `(sql, params)`. Every user-supplied VALUE is bound; the only
    interpolated fragments come from closed enums mapped to fixed constants
    above, so the large filter/sort/unit combination space cannot reach SQL as
    text (T-136-14-04).

    `count_only` builds the bounded-count form used when an exact count is
    capped; otherwise the row query carries `COUNT(*) OVER ()`, which SQLite
    evaluates over the whole (post-GROUP BY) result set BEFORE `LIMIT`, so the
    real total costs no second query.
    """
    if unit not in FINDINGS_UNITS:
        raise ValueError(
            f"unknown findings unit {unit!r} -- offered units are "
            f"{sorted(FINDINGS_UNITS)} (the per-claim unit is deliberately not offered)"
        )
    if sort not in FINDINGS_SORTS:
        raise ValueError(
            f"unknown findings sort {sort!r} -- offered sorts are "
            f"{sorted(FINDINGS_SORTS)} (novelty is deliberately not a sort key)"
        )

    where_sql, params = _build_findings_filter(
        unit=unit, bucket=bucket, novelty=novelty,
        include_divergent=include_divergent, domain=domain,
        author=author, work_id=work_id)
    group_by = _FINDINGS_UNIT_GROUP_BY[unit]
    group_sql = f"GROUP BY {group_by}" if group_by else ""

    if count_only:
        inner = f"SELECT 1 {_FINDINGS_FROM} {where_sql} {group_sql}"
        if count_cap:
            inner += f" LIMIT {int(count_cap) + 1}"
        return f"SELECT COUNT(*) AS n FROM ({inner})", params

    order_sql = f"{_FINDINGS_SORT_SQL[sort]}, {_FINDINGS_UNIT_TIEBREAK[unit]}"
    # The flag's placeholders sit in the SELECT list, so its parameters bind
    # BEFORE the WHERE clause's. Ordering is the whole contract of a positional
    # parameter list; getting it wrong here would silently filter on a shade
    # name and flag on a domain.
    flag_sql, flag_params = _divergence_flag_sql(aggregate=bool(group_by))
    sql = f"""
        SELECT {_FINDINGS_UNIT_SELECT[unit].format(divergent=flag_sql)},
               COUNT(*) OVER () AS _total_rows
        {_FINDINGS_FROM}
        {where_sql}
        {group_sql}
        ORDER BY {order_sql}
        LIMIT ? OFFSET ?
    """
    return sql, [*flag_params, *params, page_size, max(0, (page - 1) * page_size)]


def _build_manuscript_works_sql(n_page_ids: int) -> str:
    """The D-13h manuscript-scope query, as SQL, for `n_page_ids` bound
    parameters.

    A module-level builder rather than an inline f-string so a test can run
    `EXPLAIN QUERY PLAN` over the EXACT statement the service executes -- an
    index assertion against a hand-retyped near-copy proves nothing.

    Only the placeholder COUNT is interpolated; every value is bound.
    """
    placeholders = ",".join("?" for _ in range(n_page_ids))
    return f"""
        SELECT w.canonical_work_id AS canonical_work_id,
               MIN(di.display_work_id) AS display_work_id,
               MIN(dw.neutral_title) AS display_neutral_title,
               MIN(w.neutral_title) AS neutral_title,
               MIN(COALESCE(dw.author, w.author)) AS author,
               MIN(COALESCE(dw.genre, w.genre)) AS genre,
               COUNT(DISTINCT dc.page_id) AS page_count,
               MIN(COALESCE(de.band_rank, {_UNRANKED_BAND})) AS best_band_rank,
               MAX(COALESCE(di.main_pool, 0)) AS any_main_pool,
               MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                                      WHEN 'quotes_this_work' THEN 1
                                      ELSE 2 END) AS _relation_strength,
               CASE MIN(CASE dc.claim_type WHEN 'direct_witness' THEN 0
                                           WHEN 'quotes_this_work' THEN 1
                                           ELSE 2 END)
                    WHEN 0 THEN 'direct_witness'
                    WHEN 1 THEN 'quotes_this_work'
                    ELSE 'shared_text' END AS relation_kind,
               COUNT(*) OVER () AS _total_rows
        FROM discovery_claim dc
        JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
        JOIN works w ON w.work_id = dc.work_id
        LEFT JOIN discovery_identification di
               ON di.sys_id = de.sys_id
              AND di.canonical_work_id = w.canonical_work_id
        LEFT JOIN works dw ON dw.work_id = di.display_work_id
        WHERE dc.page_id IN ({placeholders})
        GROUP BY w.canonical_work_id
        ORDER BY best_band_rank ASC, canonical_work_id ASC
        LIMIT ? OFFSET ?
    """

# The per-work "ranked" CTE body (H1): every witness claim's display
# evidence for `work_id`, its physical-MS unit_key (a real unit_id, or a
# `sys:<sys_id>` singleton discriminator -- unit_id values are always
# 64-hex-char sha256 digests so they can never collide with the "sys:"
# prefix form), and its frozen band_rank. Reused (with the SAME `work_id`
# bind parameter) by both the paginated projection query and the
# member-sys_ids follow-up query below, so the two queries can never drift
# out of sync with each other.
#
# 136-21: `manuscript_display` is LEFT-joined here (mirroring `_FINDINGS_FROM`)
# so every expansion row can NAME the manuscript it points at. LEFT, not inner:
# a carrier absent from `manuscript_display` must still appear -- flagged --
# rather than vanishing from a list whose whole purpose is completeness. Joining
# it once inside the SHARED CTE is what keeps the row query, the member query
# and the count query consistent. `adjudication_status` comes through for the
# same reason `_present_claim_row` needs it: `serialize_banded_claim` REFUSES to
# emit a bandless presentation without it (SC#1), and the expansion's
# `band_label` is produced by that serializer rather than formatted locally.
def _build_work_witnesses_ranked_cte_sql(*, restrict_work_id: bool = True) -> str:
    """The ranked CTE body. `restrict_work_id=False` builds the CORPUS-WIDE
    form (no `dc.work_id = ?` bind), used ONLY by the cardinality probe that
    has to rank every work at this exact grain through this exact fragment --
    the service itself always builds the per-work form."""
    work_clause = "dc.work_id = ?\n    AND " if restrict_work_id else ""
    return f"""
  SELECT
    COALESCE(wum.unit_id, 'sys:' || de.sys_id) AS unit_key,
    wum.unit_id AS unit_id,
    dc.page_id AS page_id,
    dc.work_id AS work_id,
    dc.claim_id AS claim_id,
    dc.claim_type AS claim_type,
    de.sys_id AS sys_id,
    de.evidence_source AS evidence_source,
    de.confidence_band AS confidence_band,
    de.adjudication_status AS adjudication_status,
    md.library_code AS library_code,
    md.shelfmark_display AS shelfmark_display,
    {_BAND_RANK_CASE_SQL} AS band_rank
  FROM discovery_claim dc
  JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
  LEFT JOIN witness_unit_members wum ON wum.sys_id = de.sys_id
  LEFT JOIN manuscript_display md ON md.sys_id = de.sys_id
  WHERE {work_clause}dc.claim_type IN ('direct_witness', 'quotes_this_work')
"""


_WORK_WITNESSES_RANKED_CTE_SQL = _build_work_witnesses_ranked_cte_sql()


# ---------------------------------------------------------------------------
# 136-21 / PANEL-02: the anchor side of the expansion.
#
# The anchor's identity is ALL THREE OR NONE. With three independent optionals,
# "anchor arguments were supplied" is ambiguous for the six partial
# combinations, and a partial call could rank against a defaulted side or emit
# `relations_differ` computed from a missing relation -- both silently wrong
# rather than loudly broken. Ranking needs BOTH `(evidence_source,
# confidence_band)` because that is what `_band_rank` takes; a band alone cannot
# produce a rank.
# ---------------------------------------------------------------------------

_ANCHOR_IDENTITY_FIELDS: Tuple[str, ...] = (
    "anchor_claim_type", "anchor_evidence_source", "anchor_confidence_band",
)


def _validate_anchor_identity(
    anchor_claim_type: Optional[str],
    anchor_evidence_source: Optional[str],
    anchor_confidence_band: Optional[str],
) -> bool:
    """Enforce the all-or-none invariant; return True when the anchor is
    supplied. Raises ValueError naming which fields were PRESENT and which were
    MISSING on any of the six partial combinations."""
    supplied = {
        "anchor_claim_type": anchor_claim_type,
        "anchor_evidence_source": anchor_evidence_source,
        "anchor_confidence_band": anchor_confidence_band,
    }
    present = [f for f in _ANCHOR_IDENTITY_FIELDS if supplied[f] is not None]
    missing = [f for f in _ANCHOR_IDENTITY_FIELDS if supplied[f] is None]
    if present and missing:
        raise ValueError(
            "the anchor identity is all-three-or-none: present "
            f"{present}, missing {missing} -- ranking the anchor side needs BOTH "
            "its evidence_source and its confidence_band, and `relations_differ` "
            "needs its claim_type (PANEL-02 / DATA-01)"
        )
    return bool(present)


def _resolve_displayed_band(
    evidence_source: Optional[str],
    confidence_band: Optional[str],
    anchor_evidence_source: Optional[str],
    anchor_confidence_band: Optional[str],
) -> Tuple[Optional[str], Optional[str], int]:
    """DATA-01: the WEAKER of the two claims' bands -- `(evidence_source,
    confidence_band, band_rank)` of whichever side ranks weaker.

    `_band_rank` is the ONLY comparator (lower is stronger, so the weaker side
    is the higher rank); a second local ordering over band strings would drift
    from the frozen lattice. On a tie the carrier's own pair wins, which mirrors
    the SQL's strict `anchor_rank > band_rank` test exactly.
    """
    carrier_rank = _band_rank(evidence_source, confidence_band)
    if anchor_evidence_source is None or anchor_confidence_band is None:
        return evidence_source, confidence_band, carrier_rank
    anchor_rank = _band_rank(anchor_evidence_source, anchor_confidence_band)
    if anchor_rank > carrier_rank:
        return anchor_evidence_source, anchor_confidence_band, anchor_rank
    return evidence_source, confidence_band, carrier_rank


# ---------------------------------------------------------------------------
# 136-21: the ONE `ranked -> unit_best -> filtered` pipeline. BOTH the row
# query and the count query are built from this single fragment.
#
# Sharing only the raw CTE would not be enough: unit-best selection and band
# filtering are separate SQL BELOW it, and under this plan the FILTERING stage
# itself changes shape depending on whether an anchor was supplied. A
# separately-written count is exactly how a total drifts from the list it
# labels, and that drift is invisible until someone counts by hand.
# ---------------------------------------------------------------------------

#: The deterministic ordering, unchanged from the pre-136-21 query: band_rank
#: (the CARRIER's own, never the resolved one -- ordering is not part of this
#: plan's change) plus stable secondary tie-breakers.
_WORK_EXPANSION_ORDER_BY = "band_rank ASC, sys_id ASC, page_id ASC, claim_id ASC"

#: Every column the row query returns. Named explicitly so a new CTE column
#: cannot silently reach a caller.
_WORK_EXPANSION_ROW_COLUMNS = """unit_key, unit_id, page_id, work_id, claim_id, claim_type,
               sys_id, evidence_source, confidence_band, adjudication_status,
               library_code, shelfmark_display,
               displayed_evidence_source, displayed_confidence_band, displayed_band_rank"""


def _build_work_expansion_pipeline(
    *,
    work_id: Optional[str],
    anchor_unit_key: Optional[str] = None,
    anchor_evidence_source: Optional[str] = None,
    anchor_confidence_band: Optional[str] = None,
    enabled_bands: Optional[Iterable[str]] = None,
) -> Tuple[str, List[Any]]:
    """Build the shared `WITH ranked ... unit_best ... filtered` prefix and its
    bind parameters. Callers append their own terminal SELECT.

    THE FILTER CONTRACT (deliberately split, and stated because the two obvious
    readings contradict each other):

      * anchor supplied -> the enabled-band filter acts on the RESOLVED,
        DISPLAYED band, because that is the band the reader actually sees.
        Filtering on the other carrier's band while displaying the weaker one
        would put a screening-band row above the disclosure line whenever the
        anchor happened to be the weak side.
      * anchor absent -> the pre-136-21 behaviour, unchanged: the filter acts
        on the other carrier's own band.

    The `PARTITION BY work_id, unit_key` is behaviour-identical to the
    pre-136-21 `PARTITION BY unit_key` on the per-work form (work_id is a
    constant there); naming it makes the SAME fragment correct for the
    corpus-wide probe form, where a unit_key can legitimately recur across
    works.
    """
    params: List[Any] = []
    if work_id is not None:
        params.append(work_id)

    anchored = anchor_evidence_source is not None and anchor_confidence_band is not None
    if anchored:
        anchor_rank = _band_rank(anchor_evidence_source, anchor_confidence_band)
        displayed_sql = (
            "CASE WHEN ? > band_rank THEN ? ELSE evidence_source END "
            "AS displayed_evidence_source,\n"
            "                   CASE WHEN ? > band_rank THEN ? ELSE confidence_band END "
            "AS displayed_confidence_band,\n"
            "                   CASE WHEN ? > band_rank THEN ? ELSE band_rank END "
            "AS displayed_band_rank,"
        )
        params.extend([
            anchor_rank, anchor_evidence_source,
            anchor_rank, anchor_confidence_band,
            anchor_rank, anchor_rank,
        ])
        band_filter_column = "displayed_confidence_band"
    else:
        displayed_sql = (
            "evidence_source AS displayed_evidence_source,\n"
            "                   confidence_band AS displayed_confidence_band,\n"
            "                   band_rank AS displayed_band_rank,"
        )
        band_filter_column = "confidence_band"

    extra_clauses: List[str] = []
    if anchor_unit_key is not None:
        extra_clauses.append("unit_key != ?")
        params.append(anchor_unit_key)
    bands = list(enabled_bands) if enabled_bands else []
    if bands:
        placeholders = ",".join("?" for _ in bands)
        extra_clauses.append(f"{band_filter_column} IN ({placeholders})")
        params.extend(bands)
    where_extra = (" AND " + " AND ".join(extra_clauses)) if extra_clauses else ""

    cte = _build_work_witnesses_ranked_cte_sql(restrict_work_id=work_id is not None)
    pipeline = f"""
                WITH ranked AS ({cte}),
                unit_best AS (
                    SELECT *,
                           {displayed_sql}
                           ROW_NUMBER() OVER (
                               PARTITION BY work_id, unit_key
                               ORDER BY {_WORK_EXPANSION_ORDER_BY}
                           ) AS rn
                    FROM ranked
                ),
                filtered AS (
                    SELECT * FROM unit_best WHERE rn = 1{where_extra}
                )
    """
    return pipeline, params


def build_work_expansion_rows_sql(
    *, page_size: int, offset: int, **pipeline_kwargs: Any
) -> Tuple[str, List[Any]]:
    """The paginated expansion row query. Built from the shared pipeline, so it
    cannot diverge from the count below."""
    pipeline, params = _build_work_expansion_pipeline(**pipeline_kwargs)
    sql = f"""{pipeline}
                SELECT {_WORK_EXPANSION_ROW_COLUMNS}
                FROM filtered
                ORDER BY {_WORK_EXPANSION_ORDER_BY}
                LIMIT ? OFFSET ?
    """
    return sql, [*params, page_size, offset]


def build_work_expansion_count_sql(**pipeline_kwargs: Any) -> Tuple[str, List[Any]]:
    """The EXACT count of distinct witness UNITS the row query would return
    across all pages -- the same `ranked -> unit-best -> filtered` fragment,
    counted instead of paginated. Never a claim-row count, never bounded by a
    LIMIT, never approximated."""
    pipeline, params = _build_work_expansion_pipeline(**pipeline_kwargs)
    return f"{pipeline}\n                SELECT COUNT(*) AS n FROM filtered\n", params


# ---------------------------------------------------------------------------
# Pure, DB-free DATA-10 unit x work projection helper. Kept as the reference
# implementation of the SAME grouping/highest-band/anchor-exclusion/
# same-unit-suppression/pagination rules the SQL projection in
# get_work_witnesses() below implements directly in the database -- used
# for member-claim expansion callers that already hold an in-memory row set
# with no DB handle, and directly unit-tested with fabricated data (no
# fixture DB required) so the rules stay pinned independent of the SQL.
# ---------------------------------------------------------------------------

def _project_work_witnesses(
    claim_rows: Iterable[Dict[str, Any]],
    unit_by_sys: Dict[str, str],
    *,
    enabled_bands: Optional[Iterable[str]] = None,
    anchor_sys_id: Optional[str] = None,
    anchor_claim_type: Optional[str] = None,
    anchor_evidence_source: Optional[str] = None,
    anchor_confidence_band: Optional[str] = None,
    page: int = 1,
    page_size: int = 50,
) -> List[Dict[str, Any]]:
    """Group ``claim_rows`` (one row per real (page_id, work_id) witness
    claim, each carrying the claim's OWN display-evidence band) into
    physical-MS ``witness_units`` and project ONE row per unit at its
    HIGHEST member band, filtering on that displayed band BEFORE
    pagination, excluding the anchor's own unit, and suppressing same-unit
    members from the returned list.

    Args:
        claim_rows: dicts with keys ``page_id``, ``work_id``, ``claim_id``,
            ``claim_type``, ``sys_id``, ``evidence_source``,
            ``confidence_band`` -- one row per witness claim for a single
            work_id (already restricted to claim_type IN
            ('direct_witness', 'quotes_this_work') by the caller).
        unit_by_sys: sys_id -> unit_id for every sys_id that belongs to a
            merged witness_unit; a sys_id absent from this dict is its own
            singleton (unmerged) unit.
        enabled_bands: optional iterable of confidence_band strings; when
            given (non-empty), only units whose DISPLAYED band is in this
            set are kept. None/empty = no filtering (every band shown).
        anchor_sys_id: when given, the unit containing this sys_id
            (merged or singleton) is excluded entirely from the result.
        anchor_claim_type, anchor_evidence_source, anchor_confidence_band:
            the ANCHOR side's own identity (136-21) -- ALL THREE OR NONE
            (see ``_validate_anchor_identity``). When supplied, the displayed
            band becomes the WEAKER of the pair (DATA-01) and the
            ``enabled_bands`` filter acts on THAT resolved band rather than
            on the other carrier's; when absent, the pre-136-21 behaviour is
            unchanged.
        page, page_size: 1-indexed pagination applied AFTER filtering.

    Returns:
        A list of dicts, one per surviving unit, each carrying:
        ``work_id``, ``unit_id`` (None for an unmerged singleton),
        ``representative_sys_id``, ``representative_page_id``,
        ``representative_claim_id``, ``claim_type``, ``evidence_source``,
        ``confidence_band`` (the unit's displayed/highest band), and
        ``member_sys_ids`` (sorted list of every claim-bearing sys_id in
        the unit -- the surface a caller uses to retrieve suppressed
        member claims on expansion, via ``get_claims_for_page`` +
        ``witness_unit_members``; there is no ``supporting_page_ids``
        column, G4/R5), plus the 136-21 fields: both sides' relation kinds
        (``anchor_claim_type`` / ``relations_differ``), the resolved displayed
        pair (``displayed_evidence_source`` / ``displayed_confidence_band`` /
        ``band_rank``) and the carrier's name (``library_code`` /
        ``shelfmark_display`` / ``display_missing``, READ OFF the input rows --
        this helper has no DB to join them from and never invents them).

        ``band_label`` is the ONE field the SQL path produces that this helper
        does not: it needs the sidecar's cached band-measurement read and a UI
        language, neither of which exists here. Every other field is computed
        identically, and a test asserts the two agree over all of them.
    """
    _validate_anchor_identity(
        anchor_claim_type, anchor_evidence_source, anchor_confidence_band)
    rows = list(claim_rows)
    if not rows:
        return []

    enabled_bands_set = set(enabled_bands) if enabled_bands else None

    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    for row in rows:
        sys_id = row["sys_id"]
        unit_id = unit_by_sys.get(sys_id)
        unit_key = ("unit", unit_id) if unit_id is not None else ("sys", sys_id)
        groups.setdefault(unit_key, []).append(row)

    anchor_unit_key = None
    if anchor_sys_id:
        anchor_unit_id = unit_by_sys.get(anchor_sys_id)
        anchor_unit_key = (
            ("unit", anchor_unit_id) if anchor_unit_id is not None else ("sys", anchor_sys_id)
        )

    items: List[Dict[str, Any]] = []
    for unit_key, members in groups.items():
        if anchor_unit_key is not None and unit_key == anchor_unit_key:
            continue  # exclude the anchor's own unit entirely

        # MED (Codex R2): band_rank + sys_id alone is NOT a total order --
        # a unit/sys_id can carry >=2 same-band page claims (2,829 tied
        # units observed in the cited real-corpus large work), leaving the
        # representative dependent on unspecified scan/insertion order.
        # page_id/claim_id are stable, deterministic secondary tie-breakers
        # -- MIRRORED exactly (same key order) in the SQL projection below
        # (_WORK_WITNESSES_RANKED_CTE_SQL's ROW_NUMBER() ORDER BY) so the
        # pure-Python reference implementation and the SQL projection can
        # never disagree on which row wins a tie.
        best_row = min(
            members,
            key=lambda r: (
                _band_rank(r["evidence_source"], r["confidence_band"]),
                r["sys_id"],
                r["page_id"],
                r["claim_id"],
            ),
        )
        displayed_band = best_row["confidence_band"]
        resolved_source, resolved_band, resolved_rank = _resolve_displayed_band(
            best_row["evidence_source"], displayed_band,
            anchor_evidence_source, anchor_confidence_band)
        # The band the FILTER acts on is the band the reader SEES -- the
        # resolved one when an anchor was supplied, the carrier's own when it
        # was not (see _build_work_expansion_pipeline's filter contract).
        if enabled_bands_set is not None and resolved_band not in enabled_bands_set:
            continue

        member_sys_ids = sorted({m["sys_id"] for m in members})
        library_code = best_row.get("library_code") or None
        shelfmark_display = best_row.get("shelfmark_display") or None
        claim_type = best_row["claim_type"]
        items.append({
            "work_id": best_row["work_id"],
            "unit_id": unit_key[1] if unit_key[0] == "unit" else None,
            "representative_sys_id": best_row["sys_id"],
            "representative_page_id": best_row["page_id"],
            "representative_claim_id": best_row["claim_id"],
            "claim_type": claim_type,
            "evidence_source": best_row["evidence_source"],
            "confidence_band": displayed_band,
            "member_sys_ids": member_sys_ids,
            "anchor_claim_type": anchor_claim_type,
            "anchor_evidence_source": anchor_evidence_source,
            "anchor_confidence_band": anchor_confidence_band,
            "relations_differ": bool(
                anchor_claim_type is not None and claim_type != anchor_claim_type),
            "displayed_evidence_source": resolved_source,
            "displayed_confidence_band": resolved_band,
            "band_rank": resolved_rank,
            "library_code": library_code,
            "shelfmark_display": shelfmark_display,
            "display_missing": library_code is None or shelfmark_display is None,
        })

    items.sort(
        key=lambda it: (
            _band_rank(it["evidence_source"], it["confidence_band"]),
            it["representative_sys_id"],
            it["representative_page_id"],
            it["representative_claim_id"],
        )
    )

    page = page if isinstance(page, int) and page >= 1 else 1
    page_size = page_size if isinstance(page_size, int) and page_size > 0 else 1
    offset = (page - 1) * page_size
    return items[offset: offset + page_size]


def _present_expansion_row(
    row: Mapping[str, Any],
    members_by_key: Mapping[str, Any],
    measurements: Mapping[Tuple[str, str], Tuple[Optional[str], Optional[float]]],
    lang: str,
    *,
    anchor_claim_type: Optional[str],
    anchor_evidence_source: Optional[str],
    anchor_confidence_band: Optional[str],
) -> Dict[str, Any]:
    """One raw expansion query row -> the INTERNAL expansion row.

    `band_label` is produced exactly the way the page query produces its own:
    `_band_measurements()` supplies `measurement_status`/`ci_low`,
    `serialize_banded_claim` composes the presentation over the RESOLVED
    displayed pair, and only `band_label` is taken off the result --
    `review_overlay` and the measurement inputs are dropped here and dropped
    again by `surface_safe_expansion`. `band_precision` is deliberately NOT
    joined into the query: its `precision`/`ci_low`/`ci_high` columns would then
    sit on every returned row, one careless caller away from a D-06 violation.

    `adjudication_status` is the CARRIER's own, and is passed only because the
    serializer REFUSES to emit a bandless presentation without it (SC#1) -- that
    refusal is the property this call is here for. Nothing derived from it
    (`review_overlay`, `default_eligible`) leaves this function.
    """
    displayed_source = row["displayed_evidence_source"]
    displayed_band = row["displayed_confidence_band"]
    measurement_status, ci_low = measurements.get(
        (displayed_source, displayed_band), (None, None))
    banded = serialize_banded_claim({
        "evidence_source": displayed_source,
        "confidence_band": displayed_band,
        "adjudication_status": row["adjudication_status"],
        "measurement_status": measurement_status,
        "ci_low": ci_low,
    }, lang)

    # An absent `manuscript_display` row surfaces as explicit NULLs plus a
    # marker -- never an empty string, and never a silently dropped row.
    library_code = row["library_code"] or None
    shelfmark_display = row["shelfmark_display"] or None
    claim_type = row["claim_type"]
    return {
        "work_id": row["work_id"],
        "unit_id": row["unit_id"],
        "representative_sys_id": row["sys_id"],
        "representative_page_id": row["page_id"],
        "representative_claim_id": row["claim_id"],
        "claim_type": claim_type,
        "evidence_source": row["evidence_source"],
        "confidence_band": row["confidence_band"],
        "member_sys_ids": sorted(members_by_key.get(row["unit_key"], {row["sys_id"]})),
        "anchor_claim_type": anchor_claim_type,
        "anchor_evidence_source": anchor_evidence_source,
        "anchor_confidence_band": anchor_confidence_band,
        "relations_differ": bool(
            anchor_claim_type is not None and claim_type != anchor_claim_type),
        "displayed_evidence_source": displayed_source,
        "displayed_confidence_band": displayed_band,
        "band_rank": row["displayed_band_rank"],
        "band_label": banded["band_label"],
        "library_code": library_code,
        "shelfmark_display": shelfmark_display,
        "display_missing": library_code is None or shelfmark_display is None,
    }


class DiscoveryService:
    """The one async read-only chokepoint over ``discovery.db``.

    Constructed with LAZY injected providers -- NOTHING is read from disk
    until the first available call (F15). Safe to construct at MODULE
    import time (as ``web/discovery.py`` does) even before the sidecar
    loader has run.
    """

    def __init__(
        self,
        path_provider: Callable[[], Optional[str]],
        availability_callable: Optional[Callable[[], bool]] = None,
        sidecar_version_provider: Optional[Callable[[], Optional[str]]] = None,
    ) -> None:
        self._path_provider = path_provider
        self._availability_callable = availability_callable
        self._sidecar_version_provider = sidecar_version_provider

        # Lazy connection state (F15) -- deliberately NOT built here.
        self._conn: Optional[ThreadLocalConnection] = None
        self._last_path: Optional[str] = None
        self._last_version: Optional[str] = None
        self._conn_lock = threading.Lock()

        # Browse-enrichment LRU (version-keyed, F15).
        self._browse_lru: "OrderedDict[tuple, Any]" = OrderedDict()
        self._browse_lru_lock = threading.Lock()

        # 136-14: the `scope='band'` measurement lookup, cached per
        # (path, version) -- see _band_measurements().
        self._band_measurement_cache: Optional[Tuple[tuple, Dict]] = None

        # 136-22 / ruling U: the launch statistics, cached per (path, version).
        # The PATH is in the key on purpose and this is not hypothetical -- the
        # pre-rebuild asset, the private rebuild and the public projection ALL
        # THREE report `sidecar_version = 'discovery-v1-real'` while the two
        # rebuilds answer this query differently, so a version-only key would
        # serve one artifact's headline for the other.
        self._launch_stats_cache: Optional[Tuple[tuple, Dict[str, Any]]] = None

        # Heavy-query bounded concurrency (mirrors web/search_api.py's
        # _HeavySemaphoreState, kept per-instance since this class -- unlike
        # the module-function shape of search_api.py -- IS the natural
        # single-owner scope for its own concurrency budget).
        capacity = _get_positive_int_env(
            "DISCOVERY_MAX_CONCURRENT_QUERIES", _DEFAULT_MAX_CONCURRENT_QUERIES
        )
        self._heavy_sem = asyncio.Semaphore(capacity)
        self._heavy_capacity = capacity

        # The BROWSE-path bound. EVERY executor crossing takes one of these two
        # slots -- there is no unbounded dispatch left (see `_run_off_loop`).
        browse_capacity = _get_positive_int_env(
            "DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES",
            _DEFAULT_MAX_CONCURRENT_BROWSE_QUERIES,
        )
        self._browse_sem = asyncio.Semaphore(browse_capacity)
        self._browse_capacity = browse_capacity

        # One executor PER BUDGET CLASS, sized to that class's capacity.
        #
        # Without them the split was nominal: both budgets dispatched into the
        # SAME default `run_in_executor` pool, which this repository never
        # configures and whose width is not guaranteed, so 24 browse jobs could
        # occupy or queue ahead of every worker and a heavy read could time out
        # while its own semaphore still had capacity (round 13, finding 2).
        # Because `max_workers` EQUALS the semaphore capacity, holding a slot
        # now guarantees a worker for that class -- and the timed-out-thread
        # case stays consistent, since the slot is held until the future
        # completes, which is exactly as long as the worker is busy.
        #
        # Built lazily and per class: `ThreadPoolExecutor` spawns a thread only
        # when it has work, so a process with the discovery flag OFF pays
        # nothing. Shut down when the service is collected -- the finalizer
        # closes over the DICT, never over `self`, or it would keep the service
        # alive forever.
        self._executors: Dict[str, ThreadPoolExecutor] = {}
        self._executor_finalizer = weakref.finalize(
            self, _shutdown_executors, self._executors)

    # ------------------------------------------------------------------
    # Lazy connection management (F15 / R8)
    # ------------------------------------------------------------------

    def _get_conn(self) -> Optional[ThreadLocalConnection]:
        try:
            path = self._path_provider() if self._path_provider is not None else None
        except Exception as e:
            logger.error("DiscoveryService: path_provider raised: %s", e)
            return None
        if not path:
            return None

        version = None
        if self._sidecar_version_provider is not None:
            try:
                version = self._sidecar_version_provider()
            except Exception as e:
                logger.error("DiscoveryService: sidecar_version_provider raised: %s", e)
                version = None

        with self._conn_lock:
            if self._conn is None or path != self._last_path or version != self._last_version:
                old_conn = self._conn
                try:
                    new_conn = ThreadLocalConnection(f"file:{path}?mode=ro", row_factory=sqlite3.Row)
                except Exception as e:
                    logger.error("DiscoveryService: failed to open %s: %s", path, e)
                    return None
                self._conn = new_conn
                self._last_path = path
                self._last_version = version
                if old_conn is not None:
                    try:
                        old_conn.close()
                    except Exception:
                        logger.debug(
                            "DiscoveryService: error closing prior connection pool", exc_info=True
                        )
            return self._conn

    def is_available(self) -> bool:
        try:
            if self._availability_callable is not None and not self._availability_callable():
                return False
        except Exception as e:
            logger.error("DiscoveryService.is_available: availability_callable raised: %s", e)
            return False
        return self._get_conn() is not None

    # ------------------------------------------------------------------
    # Pagination helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _clamp_page(page: Any) -> int:
        try:
            page = int(page)
        except (TypeError, ValueError):
            page = 1
        return page if page >= 1 else 1

    @staticmethod
    def _clamp_page_size(page_size: Any) -> int:
        # M3: DISCOVERY_PAGE_SIZE_MAX can only TIGHTEN the frozen absolute
        # ceiling, never raise it -- a misconfigured env var (0, negative,
        # or > _ABSOLUTE_PAGE_SIZE_CEILING) falls back to the ceiling itself
        # rather than being trusted as-is.
        raw_maximum = _get_int_env("DISCOVERY_PAGE_SIZE_MAX", _DEFAULT_PAGE_SIZE_MAX)
        if raw_maximum <= 0 or raw_maximum > _ABSOLUTE_PAGE_SIZE_CEILING:
            maximum = _ABSOLUTE_PAGE_SIZE_CEILING
        else:
            maximum = raw_maximum

        default = _get_int_env("DISCOVERY_PAGE_SIZE_DEFAULT", _DEFAULT_PAGE_SIZE_DEFAULT)
        if default <= 0 or default > maximum:
            default = min(_DEFAULT_PAGE_SIZE_DEFAULT, maximum)

        if page_size is None:
            page_size = default
        try:
            page_size = int(page_size)
        except (TypeError, ValueError):
            page_size = default
        if page_size <= 0:
            page_size = default
        return min(page_size, maximum)

    # ------------------------------------------------------------------
    # Sync read core (fjms_service graceful-absent shape -- never raises)
    # ------------------------------------------------------------------

    def get_version(self) -> Optional[str]:
        if not self.is_available():
            return None
        conn = self._get_conn()
        if conn is None:
            return None
        try:
            cur = conn.execute("SELECT value FROM meta WHERE key = 'sidecar_version'")
            row = cur.fetchone()
            return row["value"] if row else None
        except Exception as e:
            logger.error("DiscoveryService.get_version error: %s", e)
            return None

    def get_band_precision(
        self, evidence_source: Optional[str], confidence_band: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        """BAND-02: the matching ``scope='band'`` ``band_precision`` row, or
        None when absent/unavailable (never raises -- mirrors ``get_version``
        exactly).

        Uses ``SELECT *`` (never a fixed column list) so the 135-05-added
        columns (``measurement_status``/``measurement_date``/``grader``/
        ``audit_status``/``report_id``) surface when present and are simply
        absent (via ``dict.get`` downstream) against a v1-shaped fixture."""
        if not self.is_available():
            return None
        conn = self._get_conn()
        if conn is None:
            return None
        try:
            cur = conn.execute(
                "SELECT * FROM band_precision WHERE scope = 'band' "
                "AND evidence_source = ? AND confidence_band = ?",
                (evidence_source, confidence_band),
            )
            row = cur.fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.error(
                "DiscoveryService.get_band_precision error for (%s, %s): %s",
                evidence_source, confidence_band, e,
            )
            return None

    def get_band_precision_collection(
        self, collection_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """The ``scope='collection'`` ``band_precision`` row -- the
        propagated-witness COLLECTION-level number (e.g. 0.926) that never
        lives on any band row. With ``collection_id=None``, returns the sole
        collection row when exactly one exists; None when zero or more than
        one (ambiguous -- never raises, mirrors ``get_version``)."""
        if not self.is_available():
            return None
        conn = self._get_conn()
        if conn is None:
            return None
        try:
            if collection_id is not None:
                cur = conn.execute(
                    "SELECT * FROM band_precision WHERE scope = 'collection' AND collection_id = ?",
                    (collection_id,),
                )
                row = cur.fetchone()
                return dict(row) if row else None
            cur = conn.execute("SELECT * FROM band_precision WHERE scope = 'collection'")
            rows = cur.fetchall()
            if len(rows) != 1:
                return None
            return dict(rows[0])
        except Exception as e:
            logger.error("DiscoveryService.get_band_precision_collection error: %s", e)
            return None

    def get_band_claim_counts(self) -> Dict[Tuple[str, str], int]:
        """Codex #9/#B1: the version-aware, SHIPPED, DISPLAY-DEDUPLICATED
        per-(evidence_source, confidence_band) CLAIM-count population --
        each ``discovery_claim`` counted ONCE via its single
        ``display_evidence_id`` (per docs/specs/discovery-frames.md §4), NOT
        raw ``discovery_evidence`` rows (a claim owning >1 evidence row, e.g.
        the witness + shared_text collision, must not inflate the count).
        ``{}`` on unavailable/error (never raises). This is the BAND-05
        "population" source -- reflects whatever sidecar asset is currently
        loaded (v1 pre-bake, v2 post-deploy), never ``band_precision.
        denominator`` (the graded-sample size, a different number)."""
        if not self.is_available():
            return {}
        conn = self._get_conn()
        if conn is None:
            return {}
        try:
            cur = conn.execute(
                """
                SELECT e.evidence_source, e.confidence_band, COUNT(*) AS n
                FROM discovery_claim c
                JOIN discovery_evidence e ON e.evidence_id = c.display_evidence_id
                WHERE e.routing_status = 'shipped'
                GROUP BY e.evidence_source, e.confidence_band
                """
            )
            return {
                (row["evidence_source"], row["confidence_band"]): row["n"]
                for row in cur.fetchall()
            }
        except Exception as e:
            logger.error("DiscoveryService.get_band_claim_counts error: %s", e)
            return {}

    def get_claims_for_page(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> List[Dict[str, Any]]:
        """PANEL-01/02: the manuscript's banded claims on this page, each at
        its display_evidence_id-selected band.

        Defaults to the DEFAULT-SURFACE population, which is NOT "shipped
        only" -- see ``_CLAIMS_DEFAULT_ROUTING_CLAUSE`` and D-13g. Review-only
        rows still PERSIST in the sidecar (queryable) -- they are only hidden
        from this default read surface, never deleted
        (docs/specs/discovery-sidecar-schema-v1.md SS7); ``include_review=True``
        opts into all of them.

        Signature and return type are UNCHANGED from 134-06 (a list of dicts,
        ``[]`` on every failure path, never an exception). The row now carries
        the panel's display fields as well; the total is available only through
        the enveloped shape below, since adding it here would change the return
        type."""
        try:
            rows, _total = self._query_claims_for_page(
                page_id, page=page, page_size=page_size, include_review=include_review)
        except Exception as e:
            logger.error("DiscoveryService.get_claims_for_page error for %s: %s", page_id, e)
            return []
        return rows

    # ------------------------------------------------------------------
    # 136-14 Task 1: the panel's ONE query.
    # ------------------------------------------------------------------

    def _query_claims_for_page(
        self, page_id: str, *, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> Tuple[List[Dict[str, Any]], int]:
        """The single page query: claim + display evidence + the materialized
        identification + the DISPLAY work, plus the real total, in ONE
        statement.

        ``COUNT(*) OVER ()`` is what makes the total free: SQLite evaluates a
        window function over the whole result set BEFORE ``LIMIT``, so the
        panel gets its "N identifications" header without a second query and
        without a per-row follow-up (T-136-14-03).

        The identification join is a LEFT join deliberately. Its eligibility
        rule (`shipped` OR `human_confirmed`) matches this query's own default
        predicate, so it is total for the default population -- but under
        ``include_review=True`` a review-only/unreviewed claim legitimately has
        NO identification row, and an inner join would silently drop exactly
        the rows the opt-in flag exists to reveal. A missing identification
        surfaces as a NULL bucket, never as a vanished row.

        ``eligibility_basis`` is derived PER ROW here rather than read from
        ``discovery_identification.eligibility_basis``, for two reasons. It is
        more precise: the stored column is an AGGREGATE over an identification's
        evidence rows, so an identification carrying one shipped row and one
        restored review-only row reports ``shipped`` for both, mislabelling the
        row the surface must annotate. And it is more portable: that column is
        not yet in the schema contract's authorized column list (a dated
        amendment is owed), so a contract-shaped artifact may not carry it at
        all. Three closed values: ``shipped``, ``human_confirmed`` (D-13g's
        restore) and ``review_opt_in`` (present only because the caller passed
        ``include_review``).

        RAISES on a query failure rather than swallowing it into an empty
        result. That distinction is load-bearing: the legacy list method turns
        it back into ``[]`` (its contract), while the enveloped method turns it
        into `unavailable`. Swallowing it here produced a real false zero --
        against a PRE-REBUILD asset (no ``discovery_identification`` table) the
        enveloped call reported `ok` with a total of 0, i.e. exactly the
        "this manuscript has nothing" reading D-13 exists to prevent, on a
        surface whose rule is to hide itself on a zero.

        The identity join is on ``display_work_id``, NEVER ``canonical_work_id``:
        the latter is not unique on ``works`` (15 duplicated groups on the live
        asset, three with different titles and mixed source corpora), so joining
        on it FANS OUT the identification grain (64,509 -> 65,587 rows) and
        makes both the displayed title and ``identity_visibility`` a matter of
        which row the join happened to return.
        """
        if not self.is_available():
            return [], 0
        conn = self._get_conn()
        if conn is None:
            return [], 0
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        routing_clause = "" if include_review else _CLAIMS_DEFAULT_ROUTING_CLAUSE
        try:
            cur = conn.execute(
                f"""
                SELECT dc.page_id, dc.work_id, dc.claim_id, dc.claim_type, dc.source_corpus,
                       de.evidence_id, de.evidence_kind, de.evidence_source, de.confidence_band,
                       de.adjudication_status, de.audit_status, de.routing_status, de.routing_reason,
                       de.is_new, de.a_page_id, de.sys_id, de.span_start, de.span_end,
                       de.text_layer, de.snapshot_hash,
                       de.matched_letters, de.n_spans, de.band_rank,
                       de.coverage_ppm, de.coverage_status,
                       de.novelty_status, de.novelty_source_label,
                       w.neutral_title, w.author, w.genre, w.canonical_work_id,
                       di.identification_id, di.display_work_id, di.main_pool,
                       di.main_pool_reason, di.page_count AS identification_page_count,
                       CASE WHEN de.routing_status = 'shipped' THEN 'shipped'
                            WHEN de.adjudication_status = 'human_confirmed'
                                 THEN 'human_confirmed'
                            ELSE 'review_opt_in' END AS eligibility_basis,
                       dw.neutral_title AS display_neutral_title,
                       dw.author AS display_author,
                       dw.genre AS display_genre,
                       CASE WHEN de.routing_status <> 'shipped'
                             AND de.adjudication_status = 'human_confirmed'
                            THEN 1 ELSE 0 END AS restored_by_human_confirmation,
                       CASE WHEN de.routing_status <> 'shipped'
                             AND de.routing_reason = 'low_coverage'
                            THEN 1 ELSE 0 END AS low_coverage_marker,
                       COUNT(*) OVER () AS _total_rows
                FROM discovery_claim dc
                JOIN discovery_evidence de ON de.evidence_id = dc.display_evidence_id
                JOIN works w ON w.work_id = dc.work_id
                LEFT JOIN discovery_identification di
                       ON di.sys_id = de.sys_id
                      AND di.canonical_work_id = w.canonical_work_id
                LEFT JOIN works dw ON dw.work_id = di.display_work_id
                WHERE dc.page_id = ?
                {routing_clause}
                ORDER BY COALESCE(de.band_rank, {_UNRANKED_BAND}) ASC, dc.work_id ASC
                LIMIT ? OFFSET ?
                """,
                (page_id, page_size, offset),
            )
            rows = [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error("DiscoveryService.get_claims_for_page error for %s: %s", page_id, e)
            raise
        total = rows[0].pop("_total_rows", len(rows)) if rows else 0
        for row in rows[1:]:
            row.pop("_total_rows", None)
        return rows, int(total)

    def _band_measurements(self) -> Dict[Tuple[str, str], Tuple[Optional[str], Optional[float]]]:
        """`(evidence_source, confidence_band) -> (measurement_status, ci_low)`
        for every stored `scope='band'` row, cached per sidecar version.

        Read ONCE per version rather than joined into the page query on
        purpose: `band_precision` carries `precision`/`ci_low`/`ci_high`, and a
        join would put those columns on every raw row the service returns --
        one careless caller away from a D-06 violation. Here they exist only
        inside this module, are consumed by `serialize_banded_claim`, and are
        dropped by `surface_safe_claim` before anything leaves.
        """
        version = None
        if self._sidecar_version_provider is not None:
            try:
                version = self._sidecar_version_provider()
            except Exception:
                version = None
        key = (self._last_path, version)
        cached = self._band_measurement_cache
        if cached is not None and cached[0] == key:
            return cached[1]

        measurements: Dict[Tuple[str, str], Tuple[Optional[str], Optional[float]]] = {}
        conn = self._get_conn()
        if conn is not None:
            try:
                cur = conn.execute(
                    "SELECT evidence_source, confidence_band, measurement_status, ci_low "
                    "FROM band_precision WHERE scope = 'band'"
                )
                for row in cur.fetchall():
                    measurements[(row["evidence_source"], row["confidence_band"])] = (
                        row["measurement_status"], row["ci_low"],
                    )
            except Exception as e:
                logger.error("DiscoveryService._band_measurements error: %s", e)
                return {}
        self._band_measurement_cache = (key, measurements)
        return measurements

    def _present_claim_row(
        self, row: Mapping[str, Any], measurements, lang: str,
    ) -> Dict[str, Any]:
        """One raw query row -> the surface-safe panel row.

        Band presentation goes through `serialize_banded_claim`, which RAISES
        rather than emitting a bandless presentation (SC#1); the result is then
        projected through the allowlist, which drops `review_overlay` and the
        band measurement inputs. Both steps are mandatory: the serializer
        guarantees the band is present, the projection guarantees the badge is
        not.
        """
        measurement_status, ci_low = measurements.get(
            (row.get("evidence_source"), row.get("confidence_band")), (None, None))
        banded = serialize_banded_claim(
            {**row, "measurement_status": measurement_status, "ci_low": ci_low}, lang)

        display_title = row.get("display_neutral_title")
        claim_title = row.get("neutral_title")
        title = display_title if display_title else claim_title
        presented = {
            **row,
            "band_label": banded["band_label"],
            "measurement_status": banded["measurement_status"],
            "default_eligible": banded["default_eligible"],
            "display_work_id": row.get("display_work_id") or row.get("work_id"),
            "neutral_title": title or None,
            "title_missing": not bool(title),
            "author": row.get("display_author") if display_title else row.get("author"),
            "genre": row.get("display_genre") if display_title else row.get("genre"),
            "relation_kind": row.get("claim_type"),
            "main_pool": None if row.get("main_pool") is None else bool(row.get("main_pool")),
            "restored_by_human_confirmation": bool(row.get("restored_by_human_confirmation")),
            "low_coverage_marker": bool(row.get("low_coverage_marker")),
        }
        return surface_safe_claim(presented)

    def get_claims_for_page_enveloped(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False, lang: str = "en",
    ) -> Dict[str, Any]:
        """The SYNC enveloped shape of `get_claims_for_page` (D-13).

        Two shapes, ONE implementation. This sync callable exists because the
        panel crosses into `run.io_bound` with an explicit client, and an async
        function handed to a sync worker silently returns a coroutine nobody
        awaits; the async wrapper below exists because every other caller is on
        the loop. Both go through this method -- never two query paths.

        A failed query returns `unavailable`, never `ok` with an empty list: an
        outage that renders as a genuine zero is the exact defect D-13 exists to
        prevent (the panel hides itself on a SUCCESSFUL zero).
        """
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        try:
            rows, total = self._query_claims_for_page(
                page_id, page=page, page_size=page_size, include_review=include_review)
            measurements = self._band_measurements()
            items = [self._present_claim_row(row, measurements, lang) for row in rows]
        except Exception as e:
            logger.error(
                "DiscoveryService.get_claims_for_page_enveloped error for %s: %s", page_id, e)
            return unavailable_envelope(meta={"reason": "query_failed"})
        return make_envelope(
            STATUS_OK, items, total,
            meta={"page_id": page_id, "include_review": bool(include_review)},
        )

    # ------------------------------------------------------------------
    # 136-14 Task 2: manuscript scope that NAMES the works (D-13h).
    # ------------------------------------------------------------------

    def get_manuscript_works_enveloped(
        self, page_ids: Iterable[str], page: int = 1, page_size: Optional[int] = None,
        lang: str = "en",
    ) -> Dict[str, Any]:
        """"Elsewhere in this manuscript", as NAMED works (D-13h).

        One row per DISTINCT canonical work identified anywhere in the given
        page set, each carrying its page count, its strongest band rank, its
        gating and its title. A bare count was rejected for a measured reason:
        manuscript-level coherence is the context that makes a single claim
        judgeable -- a page-23 Esther identification looks arbitrary alone and
        obviously right once the reader sees that P2-P8 carry Song of Songs and
        P22 Lamentations, i.e. a Megillot codex in the standard order. (Reader
        aid ONLY -- it must never feed band assignment or routing, which would
        be circular.)

        NO routing filter. A work reachable only behind the screening toggle is
        returned with `gated=True`, never omitted: on the mockup's teaching
        case the five folios that made the anchor judgeable were ALL
        `review_only/low_coverage`, so filtering them out removes exactly the
        context this pane exists to supply.

        `page_ids` empty is reported as `page_scope_resolved=False` rather than
        as an ordinary zero, so a manuscript whose pages could not be resolved
        never renders as "no identifications" (T-136-14-11).
        """
        page_id_list = [p for p in (page_ids or []) if p]
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        if not page_id_list:
            return make_envelope(STATUS_OK, [], 0, meta={"page_scope_resolved": False})
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})

        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        # A page list is bounded by the accessor that produced it; clamp again
        # here so an unbounded caller cannot build a giant IN (...) list.
        page_id_list = page_id_list[:_MAX_MANUSCRIPT_PAGE_IDS]
        try:
            cur = conn.execute(
                _build_manuscript_works_sql(len(page_id_list)),
                [*page_id_list, page_size, offset],
            )
            rows = [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error("DiscoveryService.get_manuscript_works error: %s", e)
            return unavailable_envelope(meta={"reason": "query_failed"})

        total = int(rows[0]["_total_rows"]) if rows else 0
        items = []
        for row in rows:
            title = row.get("display_neutral_title") or row.get("neutral_title")
            items.append(surface_safe_work_summary({
                **row,
                "neutral_title": title or None,
                "title_missing": not bool(title),
                "gated": not bool(row.get("any_main_pool")),
                "main_pool": bool(row.get("any_main_pool")),
            }))
        return make_envelope(STATUS_OK, items, total,
                             meta={"page_scope_resolved": True, "lang": lang})

    # ------------------------------------------------------------------
    # 136-14 Task 2: the related-page count (D-11a) and its rows (D-11).
    # ------------------------------------------------------------------

    def get_related_page_count_enveloped(
        self, page_id: str, include_review: bool = False,
    ) -> Dict[str, Any]:
        """The header figure: DISTINCT opposite pages for this anchor,
        deduplicated (D-11a).

        NOT evidence rows and NOT directed pairs -- the three populations are
        genuinely different (40,968 shipped shared-text evidence rows / 37,397
        directed page pairs / 30,539 unordered), and an earlier published
        figure conflated them. Returns the count with NO rows: the panel shows
        the header and count by default and fetches the rows only behind the
        toggle (D-11).
        """
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        routing_clause = "" if include_review else "AND routing_status = 'shipped'"
        try:
            cur = conn.execute(
                f"""
                SELECT COUNT(DISTINCT CASE WHEN a_page_id = ? THEN other_page_id
                                           ELSE a_page_id END) AS n
                FROM discovery_evidence
                WHERE evidence_kind = 'shared_text'
                  AND (a_page_id = ? OR other_page_id = ?)
                  {routing_clause}
                """,
                (page_id, page_id, page_id),
            )
            row = cur.fetchone()
            total = int(row["n"] or 0) if row else 0
        except Exception as e:
            logger.error(
                "DiscoveryService.get_related_page_count error for %s: %s", page_id, e)
            return unavailable_envelope(meta={"reason": "query_failed"})
        return make_envelope(STATUS_OK, [], total, meta={"unit": "distinct_opposite_pages"})

    def get_related_pages_enveloped(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> Dict[str, Any]:
        """The rows behind the toggle: ONE row per DISTINCT opposite page,
        carrying how many evidence rows collapsed into it. The total agrees
        with `get_related_page_count_enveloped` by construction (same
        grouping).

        EACH ROW NAMES ITS MANUSCRIPT. The rows used to carry the composite
        `related_page_id` and nothing else, and the panel rendered it -- so a
        scholarly surface showed readers
        `990051620920205171_IE167198813_P000003_FL167198817` where a shelfmark
        belongs. The name is resolved HERE, in ONE joined query against
        `manuscript_display`, and never per row in the UI layer: this app runs a
        single uvicorn worker, so a per-row lookup on the event loop stalls
        every concurrent request.

        The FOLIO number comes out of the id's own shape
        (`{sys_id}_{ie_id}_P{n:06d}_{fl_id}`), parsed here rather than in a
        renderer, so no surface has to know that shape to show a page number.
        `display_missing` is True when the join found no display row -- the
        surface then says so rather than falling back to the raw id, which is
        how the defect would come back.
        """
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        routing_clause = "" if include_review else "AND routing_status = 'shipped'"
        try:
            cur = conn.execute(
                f"""
                SELECT r.related_page_id AS related_page_id,
                       r.sys_id AS sys_id,
                       MIN(md.library_code) AS library_code,
                       MIN(md.shelfmark_display) AS shelfmark_display,
                       MIN(r.evidence_id) AS evidence_id,
                       MIN(r.evidence_source) AS evidence_source,
                       MIN(r.confidence_band) AS confidence_band,
                       MIN(COALESCE(r.band_rank, {_UNRANKED_BAND})) AS band_rank,
                       COUNT(*) AS evidence_row_count,
                       COUNT(*) OVER () AS _total_rows
                FROM (
                    SELECT related_page_id,
                           -- The sys_id is the id's own first component; the
                           -- shape is `{{sys_id}}_{{ie_id}}_P{{n}}_{{fl_id}}`.
                           substr(related_page_id, 1,
                                  instr(related_page_id, '_') - 1) AS sys_id,
                           evidence_id, evidence_source, confidence_band, band_rank
                    FROM (
                        SELECT CASE WHEN a_page_id = ? THEN other_page_id
                                    ELSE a_page_id END AS related_page_id,
                               evidence_id, evidence_source, confidence_band, band_rank
                        FROM discovery_evidence
                        WHERE evidence_kind = 'shared_text'
                          AND (a_page_id = ? OR other_page_id = ?)
                          {routing_clause}
                    )
                    WHERE related_page_id IS NOT NULL
                ) AS r
                LEFT JOIN manuscript_display md ON md.sys_id = r.sys_id
                GROUP BY r.related_page_id, r.sys_id
                ORDER BY band_rank ASC, r.related_page_id ASC
                LIMIT ? OFFSET ?
                """,
                (page_id, page_id, page_id, page_size, offset),
            )
            rows = [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(
                "DiscoveryService.get_related_pages error for %s: %s", page_id, e)
            return unavailable_envelope(meta={"reason": "query_failed"})
        total = int(rows[0]["_total_rows"]) if rows else 0
        items = [
            surface_safe_related_page({
                **row,
                "page_number": _page_number_from_page_id(row.get("related_page_id")),
                # A LEFT JOIN that found nothing is a NAMED state, never a
                # blank: the surface says the manuscript is not in the display
                # index rather than printing the composite id it does have.
                "display_missing": (row.get("library_code") is None
                                    or row.get("shelfmark_display") is None),
            })
            for row in rows
        ]
        return make_envelope(STATUS_OK, items, total,
                             meta={"unit": "distinct_opposite_pages"})

    # ------------------------------------------------------------------
    # 136-14 Task 3: the corpus-wide findings query and its facet cascade.
    # ------------------------------------------------------------------

    @staticmethod
    def _clamp_findings_page_size(page_size: Any) -> int:
        if page_size is None:
            page_size = _get_int_env(
                "DISCOVERY_FINDINGS_PAGE_SIZE_DEFAULT",
                _DEFAULT_FINDINGS_PAGE_SIZE_DEFAULT,
            )
        # The shared DISCOVERY_PAGE_SIZE_MAX ceiling is unchanged and applies.
        return DiscoveryService._clamp_page_size(page_size)

    def get_findings_enveloped(
        self, unit: str = FINDINGS_UNIT_IDENTIFICATION,
        bucket: str = BUCKET_MAIN,
        novelty: Optional[Iterable[str]] = None,
        include_divergent: bool = False,
        domain: Optional[str] = None,
        author: Optional[str] = None,
        work_id: Optional[str] = None,
        sort: str = FINDINGS_SORT_BAND_RANK,
        page: int = 1,
        page_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """The corpus-wide findings query, in whichever of the three offered
        units the reader selected.

        The default result set is the MAIN POOL with ruling F's catalogue-
        divergent rows EXCLUDED, and the envelope's meta says so on both counts
        (`bucket`, `include_divergent`) -- the surface narrows the corpus view
        visibly, never silently.

        An out-of-vocabulary `unit`, `sort` or `bucket` raises `ValueError`
        rather than returning an outage envelope: those values come from closed
        enums the surface maps, so an unknown one is a programming error, not a
        service state. The four envelope statuses stay reserved for things that
        actually happened to the service.
        """
        page = self._clamp_page(page)
        page_size = self._clamp_findings_page_size(page_size)
        # Validate the vocabulary BEFORE the availability check, so a bad unit
        # is a loud error even while the sidecar is off.
        sql, params = _build_findings_query(
            unit=unit, sort=sort, bucket=bucket, novelty=novelty,
            include_divergent=include_divergent, domain=domain,
            author=author, work_id=work_id, page=page, page_size=page_size)

        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})

        novelty_offered = findings_novelty_offered(unit)
        count_cap = _get_int_env("DISCOVERY_FINDINGS_COUNT_MAX", 0)
        try:
            rows = [dict(row) for row in conn.execute(sql, params).fetchall()]
            approximate = False
            if count_cap > 0:
                count_sql, count_params = _build_findings_query(
                    unit=unit, sort=sort, bucket=bucket, novelty=novelty,
                    include_divergent=include_divergent,
                    domain=domain, author=author, work_id=work_id,
                    count_only=True, count_cap=count_cap)
                counted = int(conn.execute(count_sql, count_params).fetchone()["n"])
                if counted > count_cap:
                    total, approximate = count_cap, True
                else:
                    total = counted
            else:
                total = int(rows[0]["_total_rows"]) if rows else 0
                if not rows and page > 1:
                    # AN OFFSET PAST THE END RETURNS NO ROWS, so `COUNT(*)
                    # OVER ()` has nothing to count and the window function's
                    # total is 0 -- for a filtered set that may hold thousands.
                    # Reported as-is, that zero is indistinguishable from a
                    # genuine empty result, and every surface downstream renders
                    # "no results found" over a corpus that has them, with a
                    # pager that says page 1 of 1 and disables both directions.
                    # A persisted page (this one is) makes that state STICKY.
                    #
                    # So the REAL total is resolved with the count form, on the
                    # same predicate. It costs one extra aggregate on a path
                    # nobody reaches in normal paging, and it turns an outage-
                    # shaped lie into a number the surface can clamp against.
                    count_sql, count_params = _build_findings_query(
                        unit=unit, sort=sort, bucket=bucket, novelty=novelty,
                        include_divergent=include_divergent,
                        domain=domain, author=author, work_id=work_id,
                        count_only=True)
                    total = int(conn.execute(count_sql, count_params).fetchone()["n"])
        except Exception as e:
            logger.error("DiscoveryService.get_findings error (unit=%s): %s", unit, e)
            return unavailable_envelope(meta={"reason": "query_failed"})

        items = []
        for row in rows:
            work_count = int(row.get("work_count") or 1)
            items.append(surface_safe_finding({
                **row,
                "unit": unit,
                "domain": row.get("genre") or DOMAIN_UNASSIGNED,
                "main_pool": bool(row.get("main_pool")),
                "novelty_offered": novelty_offered,
                "novelty_status": row.get("novelty_status") if novelty_offered else None,
                # Ruling F's marker, on EVERY unit -- deliberately NOT gated on
                # `novelty_offered`. That flag answers "does a single novelty
                # verdict mean anything on this row", which is false for a work
                # row; whether the row disagrees with a catalogue is a
                # different question and has an answer on all three units.
                "divergent": bool(row.get("divergent")),
                "multi_work_annotation": work_count > 1,
            }))
        return make_envelope(STATUS_OK, items, total, meta={
            "unit": unit,
            "bucket": bucket,
            "sort": sort,
            "sort_basis": _FINDINGS_SORT_BASIS[sort],
            "novelty_offered": novelty_offered,
            "include_divergent": bool(include_divergent),
            # The page this envelope actually SERVED, after the service's own
            # clamp. A surface that persists its page needs to compare the page
            # it asked for against the set that came back, and reading its own
            # request back for that comparison proves nothing.
            "page": page,
            "approximate_total": approximate,
        })

    def get_findings_facets_enveloped(
        self, level: str, bucket: str = BUCKET_MAIN,
        novelty: Optional[Iterable[str]] = None,
        include_divergent: bool = False,
        domain: Optional[str] = None,
        author: Optional[str] = None,
    ) -> Dict[str, Any]:
        """The domain / author / work cascade, mirroring the catalogue page's
        accessor SHAPE (`get_browse_authors(domain)` ->
        `get_browse_works(domain, author)`) but sourced from `works.genre` and
        the work's own author -- i.e. from the IDENTIFIED WORK.

        Counts come from the materialized grain, so opening the facet tree
        costs no scan. Every level is cross-filtered by the levels above it,
        and by the same bucket/novelty/divergence filters the result set
        carries, so a facet count and the result set it sits beside always
        agree. `include_divergent` in particular is NOT optional to thread
        through: a cascade built without it would count the ~23.6% of the grain
        the default result set excludes, and every number beside every option
        would overstate what selecting it returns.
        """
        if level not in FACET_LEVELS:
            raise ValueError(
                f"unknown facet level {level!r} -- offered levels are {sorted(FACET_LEVELS)}")
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})

        # The ONE filter builder, cross-filtered by the levels ABOVE this one:
        # the author list narrows by domain, the work list by domain and
        # author, and a level never filters by itself (that would collapse the
        # facet to the single value already selected).
        where_sql, params = _build_findings_filter(
            unit=FINDINGS_UNIT_IDENTIFICATION, bucket=bucket, novelty=novelty,
            include_divergent=include_divergent,
            domain=None if level == "domain" else domain,
            author=author if level == "work" else None,
        )

        if level == "domain":
            key_sql = f"COALESCE(NULLIF(w.genre, ''), '{DOMAIN_UNASSIGNED}')"
            label_sql = key_sql
        elif level == "author":
            key_sql = f"COALESCE(NULLIF(w.author, ''), '{DOMAIN_UNASSIGNED}')"
            label_sql = key_sql
        else:
            key_sql = "di.display_work_id"
            label_sql = "MIN(w.neutral_title)"

        try:
            rows = conn.execute(
                f"""
                SELECT {key_sql} AS value, {label_sql} AS label, COUNT(*) AS count
                {_FINDINGS_FROM}
                {where_sql}
                GROUP BY {key_sql}
                ORDER BY count DESC, value ASC
                """,
                params,
            ).fetchall()
        except Exception as e:
            logger.error("DiscoveryService.get_findings_facets error (%s): %s", level, e)
            return unavailable_envelope(meta={"reason": "query_failed"})

        items = self._project_facets(level, rows)
        return make_envelope(STATUS_OK, items, len(items), meta={
            "level": level, "bucket": bucket, "domain": domain, "author": author,
            "include_divergent": bool(include_divergent),
        })

    @staticmethod
    def _project_facets(level: str, rows) -> List[Dict[str, Any]]:
        """Shape the grouped rows into facet rows.

        For `domain` this rebuilds the two-level TREE: the stored genre is a
        `Parent / Leaf` string, so each leaf's parent is emitted as its own
        selectable node carrying the SUM of its leaves. Done in Python over at
        most a few hundred groups rather than in a second SQL pass.
        """
        if level != "domain":
            return [
                surface_safe_facet({
                    "level": level, "value": row["value"],
                    "label": row["label"] or row["value"],
                    "parent": None, "is_leaf": True, "count": int(row["count"]),
                })
                for row in rows
            ]

        leaves: List[Dict[str, Any]] = []
        parents: "OrderedDict[str, int]" = OrderedDict()
        for row in rows:
            value = row["value"]
            count = int(row["count"])
            parent = value.split(" / ", 1)[0] if " / " in value else None
            leaves.append({
                "level": level, "value": value, "label": value,
                "parent": parent, "is_leaf": True, "count": count,
            })
            if parent is not None:
                parents[parent] = parents.get(parent, 0) + count

        items = [
            surface_safe_facet({
                "level": level, "value": parent, "label": parent,
                "parent": None, "is_leaf": False, "count": count,
            })
            for parent, count in parents.items()
        ]
        items.extend(surface_safe_facet(leaf) for leaf in leaves)
        items.sort(key=lambda it: (it["parent"] or it["value"], it["is_leaf"], it["value"]))
        return items

    def _findings_timeout(self) -> float:
        return _get_positive_float_env(
            "DISCOVERY_QUERY_TIMEOUT_FINDINGS", _DEFAULT_QUERY_TIMEOUT_FINDINGS)

    # ------------------------------------------------------------------
    # Ruling U (plan 136-22): the launch statistics, read from the ARTIFACT.
    # ------------------------------------------------------------------

    def _query_launch_contribution(self, conn, *, main_pool_only: bool):
        """`(rows_by_shade, total, manuscript_count)` on ONE basis.

        The total is the SUM of the returned shade rows -- deliberately not a
        separately-counted number, so the surface can never be handed a total
        its shades do not reproduce.
        """
        sql, params = _build_launch_contribution_sql(main_pool_only=main_pool_only)
        by_shade = {
            row["novelty_status"]: (
                int(row["identification_count"]), int(row["manuscript_count"]))
            for row in conn.execute(sql, params).fetchall()
        }
        total = sum(counts[0] for counts in by_shade.values())
        ms_sql, ms_params = _build_launch_manuscript_sql(main_pool_only=main_pool_only)
        manuscripts = int(conn.execute(ms_sql, ms_params).fetchone()["n"])
        return by_shade, total, manuscripts

    @staticmethod
    def _copy_launch_envelope(envelope: Dict[str, Any]) -> Dict[str, Any]:
        """A DEFENSIVE COPY of a cached launch envelope.

        Every other cached enveloped read gets this for free from
        `_enveloped_off_loop`'s `cache_name` branch. This read deliberately does
        NOT go through that branch -- its LRU key carries no path and would
        cancel the path-aware cache below outright -- so the protection has to
        live here instead. A cache that hands the same mutable list to every
        caller produces a WRONG HEADLINE for the next reader and raises nothing.
        """
        return {
            **envelope,
            "items": [dict(row) for row in envelope.get("items") or ()],
            "meta": dict(envelope.get("meta") or {}),
        }

    def get_launch_stats_enveloped(self) -> Dict[str, Any]:
        """Ruling U's launch statistics, computed from the LOADED artifact.

        `items` is one row per contribution shade in the frozen ruling order,
        each carrying its identification count and its distinct-manuscript
        count; `total` is their sum; `meta` carries the basis, the provenance
        and the context figures. See the module docstring for every basis in SQL
        terms.

        A failed or unavailable read is an OUTAGE, never `ok` with a zero
        contribution: a release headline reading "0" during a sidecar failure is
        the exact defect the envelope exists to prevent.

        THE TRAP, stated where a caller reading this will meet it:
        `meta['main_pool_total']` and `total` are TWO DIFFERENT POPULATIONS.
        `total` is the SHADE-FILTERED contribution figure (main pool AND
        `novelty_status IN LAUNCH_CONTRIBUTION_SHADES`); `main_pool_total`
        carries NO shade predicate at all and counts every main-pool
        identification. Likewise `main_pool_total_manuscript_count` is the
        unfiltered partner of `main_pool_total`, and is NOT
        `main_pool_manuscript_count`, which is shade filtered. Substituting one
        for the other, summing them, or pairing a figure from one basis with a
        figure from the other reproduces exactly the mixed-basis defect ruling U
        was issued over.

        `meta['more_pool_total']` is the SECOND pool's size -- `COUNT(*) WHERE
        main_pool = 0`, no shade predicate -- added on the owner's 2026-08-05
        ruling that the pool's size may be shown. It is a SIZE and never a
        quality figure: the prohibition on the second pool's assessment ever
        becoming a percentage, a score or an interval is untouched, and so is
        ruling T's rule that the bucket CONTROL carries no count.

        `meta['identification_total']`, `meta['work_total']` and
        `meta['corpus_manuscript_count']` are the headline's three
        UNCONDITIONAL figures (owner ruling, 2026-08-05): `COUNT(*)`,
        `COUNT(DISTINCT display_work_id)` and `COUNT(DISTINCT sys_id)` over the
        identification table, no `main_pool` predicate and no `novelty_status`
        predicate on any of them. Because they share ONE basis they may be read
        together; `total` (shade filtered) and `all_bucket_total` (also shade
        filtered) may NOT be mixed in with them, which is the same mixed-basis
        substitution this docstring warns about one paragraph up.

        `corpus_manuscript_count` and `corpus_page_count` count the fragments
        and pages THIS RELEASE TOUCHED, not the corpus. Both names are older
        than that distinction; the table above states what each one actually
        counts, and no reader-facing string built from them may say "the whole
        corpus".
        """
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})

        # Resolve the connection FIRST, then key on the path IT resolved.
        # `_band_measurements` has the right key SHAPE and the wrong ORDER: it
        # reads `self._last_path` before `_get_conn()` refreshes it, so on the
        # first call after the artifact moves it keys on the PREVIOUS artifact
        # and returns the previous artifact's answer. All three local artifacts
        # report the identical `sidecar_version`, so a version-only key -- and a
        # stale path read -- both serve one artifact's headline for another.
        conn = self._get_conn()
        if conn is None:
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        key = (self._last_path, self._last_version)

        cached = self._launch_stats_cache
        if cached is not None and cached[0] == key:
            return self._copy_launch_envelope(cached[1])

        try:
            main_by_shade, total, main_manuscripts = self._query_launch_contribution(
                conn, main_pool_only=True)
            _all_by_shade, all_total, all_manuscripts = self._query_launch_contribution(
                conn, main_pool_only=False)
            # The UNCONDITIONAL main-pool figures: no `novelty_status`
            # predicate, deliberately a different population from `total` and
            # `main_pool_manuscript_count` above, which are both shade
            # filtered. Written as one row so the two can never be read from
            # two differently-filtered statements.
            unconditional = conn.execute(
                "SELECT COUNT(*) AS identifications, "
                "COUNT(DISTINCT sys_id) AS manuscripts "
                "FROM discovery_identification WHERE main_pool = 1"
            ).fetchone()
            main_pool_total = int(unconditional["identifications"])
            main_pool_total_manuscripts = int(unconditional["manuscripts"])
            # The SECOND pool's size (owner ruling, 2026-08-05). Its own
            # statement on its own stated basis -- `main_pool = 0`, no shade
            # predicate -- rather than a subtraction from a corpus total: a
            # figure derived by arithmetic over two other figures is exactly the
            # mixed-basis shape ruling U was issued over, and it would be wrong
            # the moment either operand's basis moved.
            more_pool_total = int(conn.execute(
                "SELECT COUNT(*) AS n FROM discovery_identification "
                "WHERE main_pool = 0"
            ).fetchone()["n"])
            # THE ALL-IN-ALL figure the headline ledes with (owner ruling,
            # 2026-08-05). Its own COUNT(*), not `main_pool_total +
            # more_pool_total`: the identity holds only while `main_pool`
            # partitions the table, and a figure assembled by adding two
            # separately-derived numbers goes wrong the first time either
            # basis moves -- with nothing to notice it.
            identification_total = int(conn.execute(
                "SELECT COUNT(*) AS n FROM discovery_identification"
            ).fetchone()["n"])
            # The headline's other unconditional figure: how many DISTINCT
            # WORKS those identifications name. `display_work_id` is not a
            # choice made here -- it is the column `_FINDINGS_UNIT_GROUP_BY`
            # groups the per-work row unit by, so this count and the number of
            # rows that unit would return are the same population by
            # construction rather than by coincidence.
            work_total = int(conn.execute(
                "SELECT COUNT(DISTINCT display_work_id) AS n "
                "FROM discovery_identification"
            ).fetchone()["n"])
            corpus_manuscripts = int(conn.execute(
                "SELECT COUNT(DISTINCT sys_id) AS n FROM discovery_identification"
            ).fetchone()["n"])
            corpus_pages = int(conn.execute(
                "SELECT COUNT(DISTINCT page_id) AS n FROM discovery_claim"
            ).fetchone()["n"])
            provenance = {
                row["key"]: row["value"] for row in conn.execute(
                    "SELECT key, value FROM meta WHERE key IN "
                    "('sidecar_version', 'audience')").fetchall()
            }
        except Exception as e:
            # Log the exception TYPE only: an artifact-derived value must never
            # be interpolated into a log line or an exception message.
            logger.error(
                "DiscoveryService.get_launch_stats_enveloped failed: %s",
                type(e).__name__)
            return unavailable_envelope(meta={"reason": "query_failed"})

        items = [
            surface_safe_launch_shade({
                "shade": shade,
                # A shade the artifact has none of is emitted as a ZERO row, not
                # omitted: a missing row and a zero row read identically to a
                # renderer, and only one of them is a fact.
                "identification_count": main_by_shade.get(shade, (0, 0))[0],
                "manuscript_count": main_by_shade.get(shade, (0, 0))[1],
            })
            for shade in LAUNCH_CONTRIBUTION_SHADES
        ]
        envelope = make_envelope(STATUS_OK, items, total, meta={
            "basis": LAUNCH_BASIS,
            "sidecar_version": provenance.get("sidecar_version") or self._last_version,
            "audience": provenance.get("audience"),
            "main_pool_manuscript_count": main_manuscripts,
            "identification_total": identification_total,
            "work_total": work_total,
            "main_pool_total": main_pool_total,
            "main_pool_total_manuscript_count": main_pool_total_manuscripts,
            "more_pool_total": more_pool_total,
            "all_bucket_total": all_total,
            "all_bucket_manuscript_count": all_manuscripts,
            "corpus_manuscript_count": corpus_manuscripts,
            "corpus_page_count": corpus_pages,
        })
        self._launch_stats_cache = (key, envelope)
        return self._copy_launch_envelope(envelope)

    def get_pages_related_to_page(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> List[Dict[str, Any]]:
        """PANEL-02: shared_text alignments touching this page, from EITHER
        side (a_page_id or other_page_id) -- both columns are indexed.

        L1 fix: defaults to SHIPPED-only -- review_only rows (e.g. the
        family-router tafsir_targum/with_arabic co-citation collections)
        are excluded unless ``include_review=True``."""
        if not self.is_available():
            return []
        conn = self._get_conn()
        if conn is None:
            return []
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        routing_clause = "" if include_review else "AND routing_status = 'shipped'"
        try:
            cur = conn.execute(
                f"""
                SELECT * FROM discovery_evidence
                WHERE evidence_kind = 'shared_text'
                  AND (a_page_id = ? OR other_page_id = ?)
                  {routing_clause}
                ORDER BY evidence_id
                LIMIT ? OFFSET ?
                """,
                (page_id, page_id, page_size, offset),
            )
            results = []
            for row in cur.fetchall():
                d = dict(row)
                d["related_page_id"] = d["other_page_id"] if d["a_page_id"] == page_id else d["a_page_id"]
                d["seed_spans"] = _parse_json_field(d.get("seed_spans"))
                d["seed_ms_ids"] = _parse_json_field(d.get("seed_ms_ids"))
                results.append(d)
            return results
        except Exception as e:
            logger.error("DiscoveryService.get_pages_related_to_page error for %s: %s", page_id, e)
            return []

    def get_evidence(
        self, claim_id: str, page: int = 1, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """PANEL-03: every evidence row for a claim (offsets + text_layer +
        per-side snapshot hash), for on-demand expansion."""
        if not self.is_available():
            return []
        conn = self._get_conn()
        if conn is None:
            return []
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size
        try:
            cur = conn.execute(
                """
                SELECT * FROM discovery_evidence
                WHERE claim_id = ?
                ORDER BY evidence_id
                LIMIT ? OFFSET ?
                """,
                (claim_id, page_size, offset),
            )
            results = []
            for row in cur.fetchall():
                d = dict(row)
                d["seed_spans"] = _parse_json_field(d.get("seed_spans"))
                d["seed_ms_ids"] = _parse_json_field(d.get("seed_ms_ids"))
                results.append(d)
            return results
        except Exception as e:
            logger.error("DiscoveryService.get_evidence error for %s: %s", claim_id, e)
            return []

    def get_work_witnesses(
        self,
        work_id: str,
        enabled_bands: Optional[Iterable[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None,
        anchor_sys_id: Optional[str] = None,
        *,
        anchor_claim_type: Optional[str] = None,
        anchor_evidence_source: Optional[str] = None,
        anchor_confidence_band: Optional[str] = None,
        lang: str = "en",
    ) -> List[Dict[str, Any]]:
        """DATA-10 unit x work projection: witnesses of ``work_id``, one row
        per physical-MS witness_unit at its highest member band, the
        enabled-band filter applied on the DISPLAYED band BEFORE pagination,
        the anchor's own unit excluded, same-unit members suppressed.

        LEGACY LIST CONTRACT, UNCHANGED: a list of dicts, ``[]`` on EVERY
        failure path, never an exception (except the all-or-none anchor
        ValueError below, which is a programming error rather than a service
        state). Published, and callers depend on it -- which is exactly why the
        queries now live in ``_query_work_expansion``, which RAISES, and why the
        ENVELOPED shape maps that raise to a named outage instead of an
        ok-with-zero.

        136-21 additions, all opt-in through the anchor triple:
          * ``anchor_claim_type`` / ``anchor_evidence_source`` /
            ``anchor_confidence_band`` -- ALL THREE OR NONE. Every existing call
            site keeps working unchanged (all three default to None).
          * every row carries both sides' relation kind, ``relations_differ``,
            the RESOLVED weaker ``displayed_*`` pair with its ``band_label``,
            and the carrier's ``library_code`` / ``shelfmark_display`` /
            ``display_missing``.

        THE ONE DELIBERATE BEHAVIOUR CHANGE, stated because it is silent under
        the old call shape: when the anchor triple IS supplied, ``enabled_bands``
        filters on the RESOLVED displayed band -- the band the reader actually
        sees -- rather than on the other carrier's. When it is absent, filtering
        is exactly as before. See ``_build_work_expansion_pipeline``.

        H1 (unchanged): the grouping / highest-member-band selection / anchor
        exclusion / band filtering / deterministic ordering ALL run IN SQL, so
        ``LIMIT``/``OFFSET`` paginate over UNITS post-grouping, never over a
        pre-grouping raw-claim cap. ``_project_work_witnesses`` remains the
        pure-Python reference implementation of these SAME rules.
        """
        # Validated OUTSIDE the try: a partial anchor set is a caller bug, and
        # swallowing it into `[]` is precisely the silence this plan removes.
        _validate_anchor_identity(
            anchor_claim_type, anchor_evidence_source, anchor_confidence_band)
        try:
            rows, _total = self._query_work_expansion(
                work_id, enabled_bands=enabled_bands, page=page, page_size=page_size,
                anchor_sys_id=anchor_sys_id, anchor_claim_type=anchor_claim_type,
                anchor_evidence_source=anchor_evidence_source,
                anchor_confidence_band=anchor_confidence_band, lang=lang,
            )
        except Exception as e:
            # Log the exception TYPE only. This module reads an artifact that
            # may carry restricted content, and a driver message can quote its
            # input; the established pattern (web/discovery_assets.py) is to log
            # anything not written from our own constants by type name alone.
            logger.error(
                "DiscoveryService.get_work_witnesses query failed (%s)",
                type(e).__name__)
            return []
        return rows

    def _query_work_expansion(
        self,
        work_id: str,
        *,
        enabled_bands: Optional[Iterable[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None,
        anchor_sys_id: Optional[str] = None,
        anchor_claim_type: Optional[str] = None,
        anchor_evidence_source: Optional[str] = None,
        anchor_confidence_band: Optional[str] = None,
        lang: str = "en",
    ) -> Tuple[List[Dict[str, Any]], int]:
        """The expansion's rows AND its EXACT total -- ``(rows, total)``.

        RAISES on a query failure rather than swallowing it into an empty
        result. That distinction is load-bearing and is the whole reason this
        helper was factored out: the legacy list method above turns a raise back
        into ``[]`` (its published contract), while the enveloped method turns
        it into `unavailable` with a named reason. Swallowing it produced a real
        false zero on the page query against a real pre-rebuild asset -- `ok`
        with a total of 0, i.e. "this work has no other carriers" -- and EVERY
        fixture in the suite carries the tables these queries read, so no
        ordinary unit test can reach that class of bug. The three forced-failure
        tests are the only thing that can.

        ``total`` is the count query's EXACT result. There is no approximate,
        estimated, sampled or capped alternative anywhere on this path: a count
        that cannot be produced inside its budget degrades to the `timeout`
        status, never to a softened number. A bounded page cannot supply a
        total, and a number a reader cannot reproduce is worse than an honest
        temporary failure.
        """
        _validate_anchor_identity(
            anchor_claim_type, anchor_evidence_source, anchor_confidence_band)
        if not self.is_available():
            return [], 0
        conn = self._get_conn()
        if conn is None:
            return [], 0
        page = self._clamp_page(page)
        page_size = self._clamp_page_size(page_size)
        offset = (page - 1) * page_size

        # Mirrors _project_work_witnesses: a given-but-empty enabled_bands
        # iterable means "filter to nothing" (every unit excluded), not
        # "no filter" -- short-circuit before building an invalid `IN ()`.
        enabled_bands_list = list(enabled_bands) if enabled_bands else None
        if enabled_bands_list is not None and len(enabled_bands_list) == 0:
            return [], 0

        anchor_unit_key = None
        if anchor_sys_id:
            arow = conn.execute(
                "SELECT unit_id FROM witness_unit_members WHERE sys_id = ?",
                (anchor_sys_id,),
            ).fetchone()
            anchor_unit_id = arow["unit_id"] if arow else None
            anchor_unit_key = (
                anchor_unit_id if anchor_unit_id is not None else f"sys:{anchor_sys_id}")

        pipeline_kwargs = {
            "work_id": work_id,
            "anchor_unit_key": anchor_unit_key,
            "anchor_evidence_source": anchor_evidence_source,
            "anchor_confidence_band": anchor_confidence_band,
            "enabled_bands": enabled_bands_list,
        }
        count_sql, count_params = build_work_expansion_count_sql(**pipeline_kwargs)
        count_row = conn.execute(count_sql, count_params).fetchone()
        total = count_row[0]

        rows_sql, rows_params = build_work_expansion_rows_sql(
            page_size=page_size, offset=offset, **pipeline_kwargs)
        page_rows = [dict(row) for row in conn.execute(rows_sql, rows_params).fetchall()]

        items: List[Dict[str, Any]] = []
        if page_rows:
            unit_keys = [r["unit_key"] for r in page_rows]
            member_placeholders = ",".join("?" for _ in unit_keys)
            member_sql = f"""
                WITH ranked AS ({_WORK_WITNESSES_RANKED_CTE_SQL})
                SELECT unit_key, sys_id FROM ranked WHERE unit_key IN ({member_placeholders})
            """
            member_cur = conn.execute(member_sql, [work_id, *unit_keys])
            members_by_key: Dict[str, set] = {}
            for row in member_cur.fetchall():
                members_by_key.setdefault(row["unit_key"], set()).add(row["sys_id"])
            measurements = self._band_measurements()
            items = [
                _present_expansion_row(
                    r, members_by_key, measurements, lang,
                    anchor_claim_type=anchor_claim_type,
                    anchor_evidence_source=anchor_evidence_source,
                    anchor_confidence_band=anchor_confidence_band,
                )
                for r in page_rows
            ]
        return items, total

    def get_work_expansion_enveloped(
        self,
        work_id: str,
        enabled_bands: Optional[Iterable[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None,
        anchor_sys_id: Optional[str] = None,
        anchor_claim_type: Optional[str] = None,
        anchor_evidence_source: Optional[str] = None,
        anchor_confidence_band: Optional[str] = None,
        lang: str = "en",
    ) -> Dict[str, Any]:
        """PANEL-02's expansion in the CLOSED four-key envelope (D-13):
        `{status, items, total, meta}`.

        A failed query returns `unavailable` with a named reason, NEVER `ok`
        with zero items -- an outage that renders as "no other manuscript
        carries this work" is the exact false-zero class 136-14 found on a real
        asset.

        `total` is the COUNT QUERY's exact result, never `len(items)`: a bounded
        page cannot supply a total, and a page length rendered as a corpus fact
        is a number the reader will believe. There is no approximate, estimated,
        sampled or capped alternative anywhere on this path -- a count that
        cannot be produced inside its budget degrades to the `timeout` status,
        which is a temporary failure a reader can retry rather than a figure
        they cannot check.

        `meta` names WHICH of the two documented filter contracts produced the
        result, so a reader of the envelope never has to guess:
          * `anchor_mode` -- whether the anchor IDENTITY triple was supplied.
            That is the axis that decides what the reader sees; `anchor_sys_id`
            (unit EXCLUSION) is an independent axis and is reported separately
            as `anchor_excluded`.
          * `filter_basis` -- `displayed_band` (the resolved weaker band) when
            anchored, `other_carrier_band` when not.
        """
        anchored = _validate_anchor_identity(
            anchor_claim_type, anchor_evidence_source, anchor_confidence_band)
        if not self.is_available():
            return unavailable_envelope(meta={"reason": "sidecar_not_serving"})
        try:
            rows, total = self._query_work_expansion(
                work_id, enabled_bands=enabled_bands, page=page, page_size=page_size,
                anchor_sys_id=anchor_sys_id, anchor_claim_type=anchor_claim_type,
                anchor_evidence_source=anchor_evidence_source,
                anchor_confidence_band=anchor_confidence_band, lang=lang,
            )
            items = [surface_safe_expansion(row) for row in rows]
        except Exception as e:
            # Type name only -- see get_work_witnesses.
            logger.error(
                "DiscoveryService.get_work_expansion_enveloped query failed (%s)",
                type(e).__name__)
            return unavailable_envelope(meta={"reason": "query_failed"})
        return make_envelope(
            STATUS_OK, items, total=total,
            meta={
                "work_id": work_id,
                "anchor_mode": "anchored" if anchored else "unanchored",
                "filter_basis": "displayed_band" if anchored else "other_carrier_band",
                "anchor_excluded": anchor_sys_id is not None,
            },
        )

    # ------------------------------------------------------------------
    # Bounded concurrency (mirrors web/search_api.py's _acquire_heavy_slot
    # exactly -- non-blocking; raises DiscoveryOverload immediately when full;
    # release is the caller's responsibility, always wired to a future's
    # add_done_callback, never a bare finally).
    #
    # TWO budgets, and EVERY executor crossing takes one of them:
    #
    #   heavy  -- corpus-wide queries (findings, facets, launch stats, work
    #             witnesses, the per-work expansion). `DISCOVERY_MAX_CONCURRENT_QUERIES`,
    #             default 4, the figure docs/specs/discovery-budgets.md SS2 fixes.
    #   browse -- everything else, which is the per-page connections-panel path.
    #             `DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES`, default 24.
    #
    # Until code review round 12 the browse path took NO slot: `heavy` defaults
    # to False and no browse caller passed it, so "bounded concurrency" was a
    # documented property the code did not have. The consequence was not merely
    # cosmetic -- `run_in_executor` threads are NOT cancellable, so a timed-out
    # read keeps its threadpool worker until the query itself finishes, and an
    # unbounded retry burst could sustain a backlog on a single-uvicorn-worker
    # server instead of failing fast as `busy`.
    # ------------------------------------------------------------------

    _SLOT_HEAVY = "heavy"
    _SLOT_BROWSE = "browse"

    _SLOT_SPECS = {
        _SLOT_HEAVY: ("_heavy_sem", "_heavy_capacity",
                      "DISCOVERY_MAX_CONCURRENT_QUERIES",
                      _DEFAULT_MAX_CONCURRENT_QUERIES),
        _SLOT_BROWSE: ("_browse_sem", "_browse_capacity",
                       "DISCOVERY_MAX_CONCURRENT_BROWSE_QUERIES",
                       _DEFAULT_MAX_CONCURRENT_BROWSE_QUERIES),
    }

    async def _acquire_slot(self, kind: str) -> Callable[[], None]:
        sem_attr, cap_attr, env_name, default = self._SLOT_SPECS[kind]
        desired = _get_positive_int_env(env_name, default)
        if desired != getattr(self, cap_attr):
            # Only safe to rebuild when fully idle (no held slots) -- mirrors
            # web/search_api.py's _HeavySemaphoreState rebuild guard exactly,
            # so a live rebuild never strands a held slot.
            current_value = getattr(getattr(self, sem_attr), "_value", None)
            if current_value == getattr(self, cap_attr):
                setattr(self, sem_attr, asyncio.Semaphore(desired))
                setattr(self, cap_attr, desired)
                # The executor is rebuilt WITH the semaphore or the two sizes
                # drift apart and the slot stops guaranteeing a worker. Safe
                # here precisely because the guard above fires only when no
                # slot is held, and a slot is held for the whole life of the
                # future -- so this class has nothing in flight.
                retired = self._executors.pop(kind, None)
                if retired is not None:
                    retired.shutdown(wait=False)

        sem = getattr(self, sem_attr)
        if sem.locked():
            raise DiscoveryOverload("temporarily unavailable")
        await sem.acquire()

        def _release() -> None:
            sem.release()

        return _release

    def _executor_for(self, kind: str) -> ThreadPoolExecutor:
        """This budget class's OWN threadpool, `max_workers` == its capacity.

        Called between `_acquire_slot` and `run_in_executor`, with no `await`
        between the three, so a rebuild cannot interleave.
        """
        executor = self._executors.get(kind)
        if executor is None:
            _sem_attr, cap_attr, _env, _default = self._SLOT_SPECS[kind]
            executor = ThreadPoolExecutor(
                max_workers=max(1, int(getattr(self, cap_attr))),
                thread_name_prefix=f"discovery-{kind}",
            )
            self._executors[kind] = executor
        return executor

    async def _acquire_heavy_slot(self) -> Callable[[], None]:
        """The heavy budget, by its original name (kept: it is what the
        DC6 slot-recycling tests and the review brief both refer to)."""
        return await self._acquire_slot(self._SLOT_HEAVY)

    # ------------------------------------------------------------------
    # Off-event-loop async dispatch (asyncio.wait, NEVER wait_for, over
    # run_in_executor -- run_in_executor threads are not cancellable; a
    # timeout must never await the abandoned future. Mirrors
    # web/search_api.py lines 1104-1166 exactly, incl. the load-bearing
    # rationale at lines 1129-1140.)
    # ------------------------------------------------------------------

    async def _run_off_loop(self, sync_fn: Callable, *args, timeout: float, heavy: bool = False):
        # get_RUNNING_loop, never get_event_loop.
        #
        # `get_event_loop()` returns whatever loop is SET for the thread, which
        # inside a coroutine is usually -- but not necessarily -- the running
        # one; the call is deprecated in this context for exactly that reason.
        # `tests/conftest.py`'s autouse fixture does set a thread loop when it
        # finds the current one closed, so the divergence is reachable here.
        #
        # HONEST SCOPE: this was changed while chasing an off-loop dispatch
        # assertion that measures ZERO dispatches under a multi-file run and
        # passes alone, and it did NOT fix that. It is kept because it is
        # correct on its own merits -- in production, anything calling
        # `asyncio.set_event_loop` in the serving thread would otherwise send
        # discovery reads to a loop that is not serving the request. It is not
        # the explanation for that failure, which is still open.
        loop = asyncio.get_running_loop()
        # EVERY crossing takes a slot; only WHICH budget depends on `heavy`.
        # There is no third branch and no `bounded=False` escape, deliberately:
        # an opt-in bound is a bound whose next caller forgets it, which is
        # exactly how the browse path came to have none (round 12, finding 4).
        kind = self._SLOT_HEAVY if heavy else self._SLOT_BROWSE
        _release: Optional[Callable[[], None]] = await self._acquire_slot(kind)
        try:
            # This class's OWN executor, never the shared default one. Two
            # budgets over one pool are two names for one budget: browse work
            # could occupy or queue ahead of every worker and starve a heavy
            # read that still held capacity (round 13, finding 2).
            fut = loop.run_in_executor(self._executor_for(kind), sync_fn, *args)
            if _release is not None:
                # Ownership of the release callable transfers to the
                # done-callback -- a timed-out thread keeps occupying the
                # threadpool worker (run_in_executor cannot cancel a running
                # thread), so the concurrency slot must stay held until the
                # thread ACTUALLY finishes, never merely until the awaiter
                # gives up. Releasing in a plain finally around the await
                # would recycle the slot while the timed-out call still
                # runs, re-admitting new heavy work past the budget.
                fut.add_done_callback(lambda _f, _r=_release: _r())
                _release = None
            done, pending = await asyncio.wait({fut}, timeout=timeout)
            if fut in pending:
                logger.warning(
                    "DiscoveryService query timed out after %ss (fn=%s, heavy=%s)",
                    timeout, getattr(sync_fn, "__name__", sync_fn), heavy,
                )
                raise DiscoveryUnavailable("temporarily unavailable")
            return fut.result()
        finally:
            # Safety net: only fires if a slot was acquired but the executor
            # dispatch itself failed before ownership transferred to the
            # done-callback above.
            if _release is not None:
                _release()

    def _browse_timeout(self) -> float:
        return _get_positive_float_env("DISCOVERY_QUERY_TIMEOUT_BROWSE", _DEFAULT_QUERY_TIMEOUT_BROWSE)

    def _work_timeout(self) -> float:
        return _get_positive_float_env("DISCOVERY_QUERY_TIMEOUT_WORK", _DEFAULT_QUERY_TIMEOUT_WORK)

    # ------------------------------------------------------------------
    # Browse-enrichment version-keyed LRU (F15) -- wraps the cheap,
    # non-heavy per-page reads (get_claims_for_page / get_pages_related_to_page).
    # ------------------------------------------------------------------

    async def _browse_cached_call(self, cache_name: str, sync_fn: Callable, args: tuple):
        max_entries = _get_int_env("DISCOVERY_BROWSE_LRU_MAX_ENTRIES", _DEFAULT_BROWSE_LRU_MAX_ENTRIES)
        if max_entries <= 0:
            # M3: a non-positive size means "disable caching" (bounded to
            # zero), NEVER "unbounded" -- also proactively clears any
            # entries cached under a previously-valid size so a live env
            # flip to <=0 frees memory immediately rather than merely
            # freezing further growth.
            with self._browse_lru_lock:
                if self._browse_lru:
                    self._browse_lru.clear()
            return await self._run_off_loop(sync_fn, *args, timeout=self._browse_timeout())

        version = None
        if self._sidecar_version_provider is not None:
            try:
                version = self._sidecar_version_provider()
            except Exception:
                version = None
        key = (cache_name,) + tuple(args) + (version,)

        with self._browse_lru_lock:
            if key in self._browse_lru:
                self._browse_lru.move_to_end(key)
                return self._browse_lru[key]

        result = await self._run_off_loop(sync_fn, *args, timeout=self._browse_timeout())

        with self._browse_lru_lock:
            self._browse_lru[key] = result
            self._browse_lru.move_to_end(key)
            while len(self._browse_lru) > max_entries:
                self._browse_lru.popitem(last=False)
        return result

    # ------------------------------------------------------------------
    # Async public API
    # ------------------------------------------------------------------

    async def get_version_async(self) -> Optional[str]:
        return await self._run_off_loop(self.get_version, timeout=self._browse_timeout())

    async def get_band_precision_async(
        self, evidence_source: Optional[str], confidence_band: Optional[str]
    ) -> Optional[Dict[str, Any]]:
        return await self._run_off_loop(
            self.get_band_precision, evidence_source, confidence_band,
            timeout=self._browse_timeout(),
        )

    async def get_band_precision_collection_async(
        self, collection_id: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        return await self._run_off_loop(
            self.get_band_precision_collection, collection_id, timeout=self._browse_timeout()
        )

    async def get_band_claim_counts_async(self) -> Dict[Tuple[str, str], int]:
        return await self._run_off_loop(self.get_band_claim_counts, timeout=self._browse_timeout())

    async def get_claims_for_page_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> List[Dict[str, Any]]:
        if include_review:
            # A distinct cache_name (rather than appending include_review to
            # the sync call's positional args) keeps the DEFAULT call shape
            # unchanged -- callers/tests that monkeypatch get_claims_for_page
            # with a 3-positional-arg fake keep working (L1).
            return await self._browse_cached_call(
                "claims_for_page_include_review",
                lambda p, pg, ps: self.get_claims_for_page(p, pg, ps, include_review=True),
                (page_id, page, page_size),
            )
        return await self._browse_cached_call(
            "claims_for_page", self.get_claims_for_page, (page_id, page, page_size)
        )

    async def get_pages_related_to_page_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> List[Dict[str, Any]]:
        if include_review:
            return await self._browse_cached_call(
                "pages_related_to_page_include_review",
                lambda p, pg, ps: self.get_pages_related_to_page(p, pg, ps, include_review=True),
                (page_id, page, page_size),
            )
        return await self._browse_cached_call(
            "pages_related_to_page", self.get_pages_related_to_page, (page_id, page, page_size)
        )

    # ------------------------------------------------------------------
    # 136-14: the enveloped async shapes. ONE exception->status mapping,
    # shared by every enveloped read, so the four states cannot be
    # classified differently on two surfaces.
    # ------------------------------------------------------------------

    async def _enveloped_off_loop(
        self, sync_fn: Callable, args: tuple, *, timeout: float,
        heavy: bool = False, cache_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Run an enveloped SYNC callable off the loop and classify its failure
        modes into the closed status vocabulary.

        The three outage statuses come from three genuinely different places
        and must stay distinguishable (T-136-01): the availability predicate
        returning False is `unavailable` (handled inside the sync callable),
        `DiscoveryUnavailable` from the timeout path is `timeout`, and the
        bounded-concurrency rejection is `busy`.

        `busy` is reachable on EVERY read, `cache_name` ones included: the
        cached path still dispatches through `_run_off_loop` on a MISS, and
        `_run_off_loop` now takes a slot unconditionally. Before round 12 the
        `heavy` flag was the only thing that took one, so `busy` was a status
        this branch could classify but no browse read could ever produce.

        Blocking work goes through `_run_off_loop` (`run_in_executor` +
        `asyncio.wait`, NEVER `asyncio.wait_for`, since executor threads are
        not cancellable). The web app runs a SINGLE uvicorn worker: a
        synchronous database call on the loop stalls every concurrent request
        while burning no CPU, which is why it is invisible in load average.
        """
        try:
            if cache_name is not None:
                cached = await self._browse_cached_call(cache_name, sync_fn, args)
                # The LRU hands the SAME envelope object to every caller; a
                # surface that appended to `items` would poison the cache for
                # the next request. Copy the mutable containers (rows
                # themselves are treated as immutable by contract).
                return {
                    **cached,
                    "items": list(cached.get("items") or []),
                    "meta": dict(cached.get("meta") or {}),
                }
            return await self._run_off_loop(sync_fn, *args, timeout=timeout, heavy=heavy)
        except DiscoveryOverload:
            return busy_envelope(meta={"reason": "bounded_concurrency"})
        except DiscoveryUnavailable:
            return timeout_envelope(meta={"reason": "query_timeout"})

    async def get_claims_for_page_enveloped_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False, lang: str = "en",
    ) -> Dict[str, Any]:
        """The async shape of `get_claims_for_page_enveloped` -- a thin wrapper
        over the SAME sync implementation, never a second query path."""
        return await self._enveloped_off_loop(
            self.get_claims_for_page_enveloped,
            (page_id, page, page_size, include_review, lang),
            timeout=self._browse_timeout(),
            cache_name="claims_for_page_enveloped",
        )

    async def get_manuscript_works_enveloped_async(
        self, page_ids: Iterable[str], page: int = 1, page_size: Optional[int] = None,
        lang: str = "en",
    ) -> Dict[str, Any]:
        # The page list is part of the cache key, so it must be hashable.
        return await self._enveloped_off_loop(
            self.get_manuscript_works_enveloped,
            (tuple(page_ids or ()), page, page_size, lang),
            timeout=self._browse_timeout(),
            cache_name="manuscript_works_enveloped",
        )

    async def get_related_page_count_enveloped_async(
        self, page_id: str, include_review: bool = False,
    ) -> Dict[str, Any]:
        return await self._enveloped_off_loop(
            self.get_related_page_count_enveloped,
            (page_id, include_review),
            timeout=self._browse_timeout(),
            cache_name="related_page_count_enveloped",
        )

    async def get_related_pages_enveloped_async(
        self, page_id: str, page: int = 1, page_size: Optional[int] = None,
        include_review: bool = False,
    ) -> Dict[str, Any]:
        return await self._enveloped_off_loop(
            self.get_related_pages_enveloped,
            (page_id, page, page_size, include_review),
            timeout=self._browse_timeout(),
            cache_name="related_pages_enveloped",
        )

    async def get_findings_enveloped_async(
        self, unit: str = FINDINGS_UNIT_IDENTIFICATION,
        bucket: str = BUCKET_MAIN,
        novelty: Optional[Iterable[str]] = None,
        include_divergent: bool = False,
        domain: Optional[str] = None,
        author: Optional[str] = None,
        work_id: Optional[str] = None,
        sort: str = FINDINGS_SORT_BAND_RANK,
        page: int = 1,
        page_size: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Heavy by design: the corpus-wide query gets a bounded-concurrency
        slot, so a burst of findings requests degrades to an explicit `busy`
        rather than queueing behind each other and starving the browse path."""
        return await self._enveloped_off_loop(
            self.get_findings_enveloped,
            (unit, bucket, tuple(novelty or ()) or None, bool(include_divergent),
             domain, author, work_id, sort, page, page_size),
            timeout=self._findings_timeout(), heavy=True,
        )

    async def get_launch_stats_enveloped_async(self) -> Dict[str, Any]:
        """The async shape of `get_launch_stats_enveloped` (ruling U).

        Deliberately passes NO `cache_name`, and passes `_findings_timeout()`
        EXPLICITLY. Both matter, and both read like free optimisations foregone:

        * `_enveloped_off_loop`'s `cache_name` branch delegates to
          `_browse_cached_call`, whose key is `(cache_name,) + args + (version,)`
          -- with NO path component -- and which returns the cached envelope
          BEFORE the wrapped sync callable runs. Layering it above the reader's
          own `(path, version)` cache would CANCEL that cache outright: after a
          path switch at a constant `sidecar_version` (the live situation) the
          outer LRU hits and serves the previous artifact's headline, while the
          path-aware reader never gets the chance to miss. Every reader-level
          test still passes, because they exercise the reader directly.
        * `_browse_cached_call` also ignores the timeout it is handed and
          applies `self._browse_timeout()` unconditionally, so routing a
          corpus-scale count through it would silently put it on the browse
          budget.

        The reader's own cache already makes the repeat call cheap; the outer
        layer would add only the defect.
        """
        return await self._enveloped_off_loop(
            self.get_launch_stats_enveloped, (),
            timeout=self._findings_timeout(), heavy=True,
        )

    async def get_findings_facets_enveloped_async(
        self, level: str, bucket: str = BUCKET_MAIN,
        novelty: Optional[Iterable[str]] = None,
        include_divergent: bool = False,
        domain: Optional[str] = None,
        author: Optional[str] = None,
    ) -> Dict[str, Any]:
        return await self._enveloped_off_loop(
            self.get_findings_facets_enveloped,
            (level, bucket, tuple(novelty or ()) or None,
             bool(include_divergent), domain, author),
            timeout=self._findings_timeout(), heavy=True,
        )

    async def run_off_loop(
        self, sync_fn: Callable, *args, timeout: Optional[float] = None,
        heavy: bool = False,
    ):
        """PUBLIC alias of the off-loop dispatch discipline, for the web
        composition module.

        `web/discovery.py` has one blocking read that is NOT a sidecar query --
        the browse-map page-ID accessor the manuscript scope is served by
        (D-09) -- and it must run under the SAME rules as every sidecar read:
        `run_in_executor` + `asyncio.wait` (never `wait_for`, because executor
        threads are not cancellable), under the browse budget. Exposed here
        rather than duplicated there, so there is one implementation of the
        discipline and not two.
        """
        return await self._run_off_loop(
            sync_fn, *args,
            timeout=self._browse_timeout() if timeout is None else timeout,
            heavy=heavy,
        )

    async def get_evidence_async(
        self, claim_id: str, page: int = 1, page_size: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        return await self._run_off_loop(
            self.get_evidence, claim_id, page, page_size, timeout=self._browse_timeout()
        )

    async def get_work_witnesses_async(
        self,
        work_id: str,
        enabled_bands: Optional[Iterable[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None,
        anchor_sys_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        return await self._run_off_loop(
            self.get_work_witnesses,
            work_id, enabled_bands, page, page_size, anchor_sys_id,
            timeout=self._work_timeout(), heavy=True,
        )

    async def get_work_expansion_enveloped_async(
        self,
        work_id: str,
        enabled_bands: Optional[Iterable[str]] = None,
        page: int = 1,
        page_size: Optional[int] = None,
        anchor_sys_id: Optional[str] = None,
        anchor_claim_type: Optional[str] = None,
        anchor_evidence_source: Optional[str] = None,
        anchor_confidence_band: Optional[str] = None,
        lang: str = "en",
    ) -> Dict[str, Any]:
        """The async shape of `get_work_expansion_enveloped` -- a thin wrapper
        over the SAME sync implementation, never a second query path.

        Heavy: the expansion issues a count, a page and a member lookup over a
        window query, so a burst degrades to an explicit `busy` rather than
        queueing behind the browse path. Deliberately NOT LRU-cached: the cache
        hands one envelope object to every caller, and the forced-failure and
        exhaustive-pagination contracts here must each see a real query.

        `enabled_bands` is passed through UNCHANGED (an empty iterable keeps its
        pre-existing meaning); it is only made hashable for the executor call.
        """
        return await self._enveloped_off_loop(
            self.get_work_expansion_enveloped,
            (work_id,
             None if enabled_bands is None else tuple(enabled_bands),
             page, page_size, anchor_sys_id, anchor_claim_type,
             anchor_evidence_source, anchor_confidence_band, lang),
            timeout=self._browse_timeout(), heavy=True,
        )
