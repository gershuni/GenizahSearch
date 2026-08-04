# -*- coding: utf-8 -*-
"""Ruling U's launch statistics, and the guard that none of them is ever a
literal (Phase 136, plan 136-22).

WHAT RULING U DECIDED, and why a reader had to exist at all. The public launch
leads with what the release ADDS to the existing finding aids -- a contribution
total and its three shades -- rather than with the larger coverage figure. That
choice only survives if the number is true of the artifact actually being
served, and it demonstrably is not stable: the deployed public projection and
the private rebuild answer this question differently while reporting the
IDENTICAL `sidecar_version` string, and an earlier draft quoted a figure built
by adding a main-pool count to two unfiltered ones. A hardcoded launch number is
not a shortcut; it is that same defect deferred to the next bake.

So this module holds two things, and neither substitutes for the other:

1. **The reader's contract** (Task 1) -- one basis, an exact decomposition,
   provenance, a path-aware cache, defensive copies, and outages that are never
   a zero contribution.
2. **The no-literals guard** (Task 2) -- a scan over a GLOB-DERIVED source set
   plus the translation table, forbidding a COMMITTED union of figures.

WHICH SPECIFIC FAILURE EACH HALF OF THE FORBIDDEN LIST FORECLOSES, in the terms
the plan requires:

  * **Without the COMMITTED file** the guard derives its expectations at test
    time and discards them. The repository manifest currently selects a
    pre-rebuild asset the public loader refuses, so "derive them where an
    artifact is available" resolves to "forbid nothing, report green" on the
    very machine this runs on.
  * **Without the COMPLETENESS gate** (an exact key-set equality against every
    numeric value the envelope exposes) a file holding one figure passes, and a
    recomputation that is equally partial agrees with it -- so the guard covers
    one number out of twelve while every other launch figure stays free to be
    hardcoded.
  * **Without the FRESHNESS check** the committed file goes stale in silence: a
    rebuild moves the numbers, the file still holds the old ones, and a literal
    of the NEW figure is forbidden by nothing.

THE SCANNER'S LIMIT, STATED RATHER THAN IMPLIED, AND THE MECHANISM THAT COVERS
IT. `scan_launch_literals` reaches four literal forms: string literals and
f-string parts, numeric AST constants, formatted expressions, and
CONSTANT-FOLDED numeric arithmetic. Folding is what closes
`ui.label(f"{9_000 + 523:,}")`, which renders the live headline while NEITHER
operand is a launch figure.

It does NOT close -- and no static scanner over this repository can close -- a
figure assembled ACROSS STATEMENTS (`_A = 9_000` ... `str(_A + 523)`), imported
from another module, or read from a data file at runtime. An enumeration of
literal forms can never be complete.

That is not a reason to weaken the scan. It is the reason plan 136-18
ADDITIONALLY drives the rendered headline from a stubbed envelope whose four
numbers are SENTINELS appearing in no artifact, and asserts the rendered figures
equal those sentinels. That test proves the DATA PATH instead of enumerating
literal forms, so it is complete for the rendering surface in exactly the way
this scanner cannot be.

THE PAIRING RUNS BOTH WAYS, and neither test substitutes for the other:

  * this scanner reaches `web/discovery.py`, `shared/discovery_*.py` and the
    translation table -- none of which any render test exercises;
  * 136-18's sentinel test catches a hardcode in WHATEVER FORM IT TOOK,
    including the assembled-across-statements case above.

Each has a mutation control the other misses. Do not delete either as redundant
with the other, and do not narrow this scan because "the render test covers it".

Masking discipline: every fixture here is fabricated in-test through
`scripts.build_discovery_sidecar.create_schema`; nothing names a corpus.
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import sqlite3
import sys
from pathlib import Path

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from shared.discovery_novelty import NOVELTY_STATUSES
from shared.discovery_service import (
    LAUNCH_CONTRIBUTION_SHADES,
    DiscoveryService,
    _validate_contribution_shades,
)
from shared.discovery_surface_projection import (
    _ALL_ALLOWLISTS,
    OUTAGE_STATUSES,
    SURFACE_LAUNCH_SHADE_FIELDS,
    is_forbidden_surface_field,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_ENVELOPE_KEYS = {"status", "items", "total", "meta"}


# ===========================================================================
# Fixture builder -- a synthetic sidecar carrying identification rows in both
# buckets and any shade the caller asks for.
# ===========================================================================

def _build_launch_db(db_path, rows, *, sidecar_version="test-launch",
                     audience="public", pages=()):
    """`rows`: iterable of (sys_id, work_suffix, main_pool, novelty_status).

    `pages`: page_ids to seed into `discovery_claim`, which is where the
    corpus PAGE figure is counted from.
    """
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        sidecar_build.create_schema(conn)
        cur = conn.cursor()
        work_ids = sorted({f"w{suffix}" for _s, suffix, _m, _n in rows})
        cur.executemany(
            "INSERT INTO works (work_id, canonical_work_id, neutral_title, author, "
            "genre, source_corpus) VALUES (?, ?, ?, ?, ?, ?)",
            [(w, w, "Synthetic Launch Work", None, None, "sefaria") for w in work_ids],
        )
        cur.executemany(
            "INSERT INTO discovery_identification ("
            "identification_id, sys_id, canonical_work_id, display_work_id, "
            "main_pool, main_pool_reason, best_band_rank, page_count, "
            "max_coverage_ppm, relation_kind, novelty_status, "
            "assertion_visibility, identity_visibility) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (f"id{i:06d}", sys_id, f"w{suffix}", f"w{suffix}", int(main_pool),
                 "main_full_coverage" if main_pool else "low_coverage",
                 0, 1, 900, "direct_witness", novelty, "public", "public")
                for i, (sys_id, suffix, main_pool, novelty) in enumerate(rows)
            ],
        )
        for i, page_id in enumerate(pages):
            # Cycle the work so a REPEATED page_id is a distinct
            # (page_id, work_id) pair -- the repeat is deliberate, and it is
            # what proves the corpus page figure counts DISTINCT pages rather
            # than claim rows.
            cur.execute(
                "INSERT INTO discovery_claim (page_id, work_id, claim_id, claim_type, "
                "display_evidence_id, source_corpus, sidecar_version) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (page_id, work_ids[i % len(work_ids)], f"c{i:06d}", "direct_witness",
                 f"e{i:06d}", "sefaria", sidecar_version),
            )
        meta_rows = [("schema_version", "discovery-v1"),
                     ("sidecar_version", sidecar_version)]
        if audience is not None:
            meta_rows.append(("audience", audience))
        cur.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta_rows)
        conn.commit()
    finally:
        conn.close()
    return str(db_path)


def _service_for(db_path, version="test-launch", available=True):
    return DiscoveryService(
        path_provider=lambda: str(db_path),
        availability_callable=lambda: available,
        sidecar_version_provider=lambda: version,
    )


#: The populated fixture every structural assertion runs against: all three
#: contribution shades present in the main pool, plus `main_pool = 0` rows the
#: main-pool total must EXCLUDE and the all-bucket total must INCLUDE, plus a
#: non-contribution shade that must join neither.
_POPULATED_ROWS = (
    # main pool, fills_gap: 3 rows over 2 manuscripts
    ("s001", "A", 1, "fills_gap"),
    ("s001", "B", 1, "fills_gap"),
    ("s002", "A", 1, "fills_gap"),
    # main pool, refines_granularity: 2 rows over 2 manuscripts
    ("s003", "A", 1, "refines_granularity"),
    ("s004", "A", 1, "refines_granularity"),
    # main pool, container_predicts: 1 row over 1 manuscript
    ("s005", "A", 1, "container_predicts"),
    # NOT main pool -- excluded from the headline, included in all-bucket
    ("s006", "A", 0, "fills_gap"),
    ("s007", "A", 0, "container_predicts"),
    # a shade OUTSIDE the contribution set, in the main pool: joins neither
    ("s008", "A", 1, "confirms"),
)
_EXPECTED_SHADE_COUNTS = {"fills_gap": 3, "refines_granularity": 2, "container_predicts": 1}
_EXPECTED_SHADE_MANUSCRIPTS = {"fills_gap": 2, "refines_granularity": 2, "container_predicts": 1}
_EXPECTED_TOTAL = 6
_EXPECTED_ALL_BUCKET_TOTAL = 8
_EXPECTED_PAGES = ("p1", "p2", "p2", "p3")


@pytest.fixture
def populated_db(tmp_path):
    return _build_launch_db(
        tmp_path / "launch-populated.db", _POPULATED_ROWS, pages=_EXPECTED_PAGES)


@pytest.fixture
def populated_service(populated_db):
    return _service_for(populated_db)


# ===========================================================================
# Task 1, behaviour 1: the four-key envelope, one row per shade, frozen order
# ===========================================================================

def test_envelope_has_one_row_per_shade_in_the_frozen_ruling_order(populated_service):
    env = populated_service.get_launch_stats_enveloped()
    assert set(env) == _ENVELOPE_KEYS
    assert env["status"] == "ok"
    assert len(env["items"]) == 3
    assert [row["shade"] for row in env["items"]] == list(LAUNCH_CONTRIBUTION_SHADES)
    for row in env["items"]:
        assert set(row) == set(SURFACE_LAUNCH_SHADE_FIELDS)
        assert row["identification_count"] == _EXPECTED_SHADE_COUNTS[row["shade"]]
        assert row["manuscript_count"] == _EXPECTED_SHADE_MANUSCRIPTS[row["shade"]]


# ===========================================================================
# Task 1, behaviour 2: the decomposition identity -- structural, not asserted
# ===========================================================================

def test_total_is_exactly_the_sum_of_its_three_shades_on_the_fixture(populated_service):
    env = populated_service.get_launch_stats_enveloped()
    assert env["total"] == _EXPECTED_TOTAL
    assert env["total"] == sum(row["identification_count"] for row in env["items"])


def test_total_is_exactly_the_sum_of_its_three_shades_on_the_real_artifact():
    """The identity must hold on the artifact actually being served, not only on
    a fixture whose numbers this test chose."""
    db = _resolve_real_artifact()
    if db is None:
        pytest.skip(
            "no resolvable discovery artifact: set DISCOVERY_LAUNCH_GUARD_DB to a "
            "public-audience sidecar to run the real-artifact identity check")
    env = _service_for(db, version="real").get_launch_stats_enveloped()
    assert env["status"] == "ok"
    assert len(env["items"]) == 3
    assert env["total"] == sum(row["identification_count"] for row in env["items"])
    assert env["total"] > 0


# ===========================================================================
# Task 1, behaviour 3: ONE basis -- main_pool = 1 -- and meta says so
# ===========================================================================

def test_every_count_is_on_the_main_pool_basis_and_meta_names_it(populated_service):
    env = populated_service.get_launch_stats_enveloped()
    assert env["meta"]["basis"] == "main_pool"
    # The main-pool total EXCLUDES the two `main_pool = 0` rows...
    assert env["total"] == _EXPECTED_TOTAL
    # ...which the all-bucket figure INCLUDES.
    assert env["meta"]["all_bucket_total"] == _EXPECTED_ALL_BUCKET_TOTAL
    assert env["meta"]["all_bucket_total"] != env["total"]


# ===========================================================================
# Task 1, behaviour 4: the all-bucket figures live on their OWN named keys
# ===========================================================================

def test_all_bucket_figures_are_never_merged_into_the_main_pool_total(populated_service):
    env = populated_service.get_launch_stats_enveloped()
    assert env["meta"]["all_bucket_total"] == _EXPECTED_ALL_BUCKET_TOTAL
    assert env["meta"]["all_bucket_manuscript_count"] == 7
    # The retired 13,285 defect in miniature: a total that is the sum of one
    # filtered and one unfiltered count.
    assert env["total"] != env["meta"]["all_bucket_total"]
    assert env["total"] == sum(row["identification_count"] for row in env["items"])


# ===========================================================================
# Task 1, behaviour 5: the shade set is a closed, validated constant
# ===========================================================================

def test_the_shade_tuple_is_validated_against_the_novelty_vocabulary():
    assert set(LAUNCH_CONTRIBUTION_SHADES) <= NOVELTY_STATUSES
    with pytest.raises(RuntimeError) as excinfo:
        _validate_contribution_shades(LAUNCH_CONTRIBUTION_SHADES + ("not_a_shade",))
    assert "not_a_shade" in str(excinfo.value)


def test_the_module_validates_the_shade_tuple_at_import_time():
    """The validation is load-bearing only if it actually RUNS at import."""
    import ast
    import inspect

    import shared.discovery_service as svc

    tree = ast.parse(inspect.getsource(svc))
    called = [
        node for node in tree.body
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Name)
        and node.value.func.id == "_validate_contribution_shades"
    ]
    assert called, (
        "_validate_contribution_shades is never called at module level -- a typo "
        "or a retired shade would then produce a quietly smaller total instead of "
        "an import failure")


def test_a_fourth_novelty_shade_in_the_artifact_does_not_join_the_total(tmp_path):
    db = _build_launch_db(
        tmp_path / "fourth-shade.db",
        _POPULATED_ROWS + (
            ("s009", "A", 1, "extends"),
            ("s010", "A", 1, "aid_more_specific"),
        ),
        pages=_EXPECTED_PAGES,
    )
    env = _service_for(db).get_launch_stats_enveloped()
    assert env["total"] == _EXPECTED_TOTAL
    assert [row["shade"] for row in env["items"]] == list(LAUNCH_CONTRIBUTION_SHADES)


def test_a_shade_with_no_rows_still_appears_with_a_zero_count(tmp_path):
    """A missing row and a zero row read identically to a renderer, and only one
    of them is a fact."""
    db = _build_launch_db(
        tmp_path / "missing-shade.db",
        (("s001", "A", 1, "fills_gap"),),
        pages=("p1",),
    )
    env = _service_for(db).get_launch_stats_enveloped()
    assert [row["shade"] for row in env["items"]] == list(LAUNCH_CONTRIBUTION_SHADES)
    by_shade = {row["shade"]: row for row in env["items"]}
    assert by_shade["refines_granularity"]["identification_count"] == 0
    assert by_shade["refines_granularity"]["manuscript_count"] == 0
    assert by_shade["container_predicts"]["identification_count"] == 0
    assert env["total"] == 1


# ===========================================================================
# Task 1, behaviour 6: the CONTEXT figures, each key named after its basis
# ===========================================================================

def test_meta_carries_the_context_figures_on_basis_named_keys(populated_service):
    meta = populated_service.get_launch_stats_enveloped()["meta"]
    # distinct manuscripts contributing to the MAIN-POOL contribution total
    assert meta["main_pool_manuscript_count"] == 5
    # corpus scale: every identification row's manuscript, and every page a
    # claim touches
    assert meta["corpus_manuscript_count"] == 8
    assert meta["corpus_page_count"] == 3


# ===========================================================================
# Task 1, behaviour 7: provenance -- version and audience
# ===========================================================================

def test_meta_carries_the_sidecar_version_and_the_audience(tmp_path):
    db = _build_launch_db(
        tmp_path / "provenance.db", _POPULATED_ROWS, pages=_EXPECTED_PAGES,
        sidecar_version="test-provenance", audience="private")
    meta = _service_for(db, version="test-provenance").get_launch_stats_enveloped()["meta"]
    assert meta["sidecar_version"] == "test-provenance"
    assert meta["audience"] == "private"
    assert meta["basis"] == "main_pool"


# ===========================================================================
# Task 1, behaviour 8: the (path, version) cache, resolved in the right ORDER
# ===========================================================================

def test_switching_the_artifact_path_at_a_constant_version_changes_the_answer(tmp_path):
    """The live situation: the pre-rebuild asset, the private rebuild and the
    public projection ALL report the identical `sidecar_version`, while the
    rebuilds answer this query differently. A version-only key, or a key read
    from a stale `self._last_path`, passes a version-flip test and fails here."""
    db_a = _build_launch_db(
        tmp_path / "artifact-a.db", _POPULATED_ROWS, pages=_EXPECTED_PAGES,
        sidecar_version="same-version")
    db_b = _build_launch_db(
        tmp_path / "artifact-b.db",
        _POPULATED_ROWS + (("s100", "A", 1, "fills_gap"), ("s101", "A", 1, "container_predicts")),
        pages=_EXPECTED_PAGES + ("p9",),
        sidecar_version="same-version", audience="private")
    current = {"path": db_a}
    service = DiscoveryService(
        path_provider=lambda: current["path"],
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: "same-version",
    )
    service.get_launch_stats_enveloped()
    # WARM THE CACHE UNDER ARTIFACT A'S OWN KEY BEFORE SWITCHING, and do not
    # remove this second call -- it is what gives this test its power, and that
    # was established by running the mutation rather than by reasoning about it.
    #
    # Under the `_band_measurements` ordering (the cached path attribute read
    # for BOTH the lookup and the store, before anything refreshes it), the
    # very first call keys on `(None, None)`, because `self._last_path` has not
    # been resolved yet. So a single pre-switch call leaves A's answer stored
    # under a key the post-switch call never asks for, the lookup misses, the
    # connection refreshes on the way past, and the stale implementation
    # returns the RIGHT answer. Measured: with one pre-switch call that
    # mutation PASSES; with two it FAILS on the first post-switch call.
    first = service.get_launch_stats_enveloped()
    assert first["total"] == _EXPECTED_TOTAL

    current["path"] = db_b
    second = service.get_launch_stats_enveloped()      # the FIRST post-switch call
    assert second["total"] == _EXPECTED_TOTAL + 2
    assert second["items"] != first["items"]
    assert second["meta"]["audience"] == "private"
    assert second["meta"] != first["meta"]


def test_flipping_the_version_at_a_constant_path_recomputes_the_answer(tmp_path):
    """The other half of the key: neither component may be dropped."""
    db = _build_launch_db(tmp_path / "version-flip.db", _POPULATED_ROWS,
                          pages=_EXPECTED_PAGES)
    version = {"v": "v-one"}
    service = DiscoveryService(
        path_provider=lambda: str(db),
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: version["v"],
    )
    first = service.get_launch_stats_enveloped()
    calls = {"n": 0}
    original = service._query_launch_contribution

    def counting(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    service._query_launch_contribution = counting
    service.get_launch_stats_enveloped()
    assert calls["n"] == 0, "a repeat call at the same (path, version) must hit the cache"
    version["v"] = "v-two"
    service.get_launch_stats_enveloped()
    # TWO, not one: a recompute issues the main-pool query and the all-bucket
    # query, which is the one grouped statement in both of its shapes.
    assert calls["n"] == 2, "a version flip must MISS the cache"
    assert first["total"] == _EXPECTED_TOTAL


# ===========================================================================
# Task 1, behaviour 9: defensive copies -- this reader owns the copy protection
# ===========================================================================

def test_a_caller_that_mutates_what_it_was_given_cannot_poison_the_next_reader(
        populated_service):
    first = populated_service.get_launch_stats_enveloped()
    baseline = copy.deepcopy(first)
    first["items"].append({"shade": "injected", "identification_count": 10 ** 6,
                           "manuscript_count": 0})
    first["meta"]["injected"] = True
    first["items"][0]["identification_count"] = -1
    second = populated_service.get_launch_stats_enveloped()
    assert second["items"] == baseline["items"]
    assert second["meta"] == baseline["meta"]
    assert second["total"] == baseline["total"]
    assert second["items"] is not first["items"]


# ===========================================================================
# Task 1, behaviour 10: outages -- never `ok` with a zero contribution
# ===========================================================================

def test_an_unavailable_service_is_an_outage_not_a_zero(populated_db):
    env = _service_for(populated_db, available=False).get_launch_stats_enveloped()
    assert env["status"] in OUTAGE_STATUSES
    assert env["status"] == "unavailable"
    assert env["meta"]["reason"] == "sidecar_not_serving"
    assert env["items"] == [] and env["total"] == 0


def test_a_dropped_identification_table_is_an_outage_not_a_zero(tmp_path):
    db = _build_launch_db(tmp_path / "dropped.db", _POPULATED_ROWS, pages=_EXPECTED_PAGES)
    conn = sqlite3.connect(db)
    conn.execute("DROP TABLE discovery_identification")
    conn.commit()
    conn.close()
    env = _service_for(db).get_launch_stats_enveloped()
    assert env["status"] == "unavailable"
    assert env["meta"]["reason"] == "query_failed"
    assert env["total"] == 0 and env["items"] == []


def test_a_query_timeout_is_a_timeout_envelope_not_a_zero(populated_db, monkeypatch):
    from shared.discovery_errors import DiscoveryUnavailable

    service = _service_for(populated_db)

    async def _boom(*args, **kwargs):
        raise DiscoveryUnavailable("temporarily unavailable")

    monkeypatch.setattr(service, "_run_off_loop", _boom)
    env = asyncio.run(service.get_launch_stats_enveloped_async())
    assert env["status"] == "timeout"
    assert env["meta"]["reason"] == "query_timeout"
    assert env["total"] == 0 and env["items"] == []


# ===========================================================================
# Task 1, behaviour 11: nothing here is a precision, a rate or a badge
# ===========================================================================

def test_the_launch_shade_allowlist_is_registered_and_names_no_quality_field():
    registered = dict(_ALL_ALLOWLISTS)
    assert "SURFACE_LAUNCH_SHADE_FIELDS" in registered, (
        "an allowlist absent from _ALL_ALLOWLISTS is NOT covered by the "
        "import-time forbidden-field guard")
    assert registered["SURFACE_LAUNCH_SHADE_FIELDS"] == SURFACE_LAUNCH_SHADE_FIELDS
    assert set(SURFACE_LAUNCH_SHADE_FIELDS) == {
        "shade", "identification_count", "manuscript_count"}
    for field in SURFACE_LAUNCH_SHADE_FIELDS:
        assert not is_forbidden_surface_field(field)
    for banned in ("precision", "share", "ratio", "rank", "pct", "percent"):
        assert not any(banned in field for field in SURFACE_LAUNCH_SHADE_FIELDS)


def test_a_forbidden_name_in_a_copy_of_the_allowlist_is_rejected_by_the_guard():
    """Proves the REGISTRATION is load-bearing rather than decorative.

    Runs the very function the module body runs over `_ALL_ALLOWLISTS` at import
    -- not a re-implementation of it, and not the `is_forbidden_surface_field`
    predicate underneath it, either of which would pass while the registration
    loop itself was skipping this allowlist.
    """
    from shared.discovery_surface_projection import _assert_allowlist_safe

    _assert_allowlist_safe("SURFACE_LAUNCH_SHADE_FIELDS", SURFACE_LAUNCH_SHADE_FIELDS)
    with pytest.raises(RuntimeError) as excinfo:
        _assert_allowlist_safe(
            "SURFACE_LAUNCH_SHADE_FIELDS", SURFACE_LAUNCH_SHADE_FIELDS + ("band_precision",))
    assert "band_precision" in str(excinfo.value)


def test_the_import_time_guard_runs_over_every_registered_allowlist():
    """`_ALL_ALLOWLISTS` is only a guard if the module body actually walks it."""
    import ast
    import inspect

    import shared.discovery_surface_projection as proj

    tree = ast.parse(inspect.getsource(proj))
    loops = [
        node for node in tree.body
        if isinstance(node, ast.For)
        and isinstance(node.iter, ast.Name) and node.iter.id == "_ALL_ALLOWLISTS"
    ]
    assert loops, "no module-level loop walks _ALL_ALLOWLISTS"
    calls = [
        n.func.id for loop in loops for n in ast.walk(loop)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
    ]
    assert "_assert_allowlist_safe" in calls, (
        "the import-time loop does not call the same checker the test above "
        "exercises -- two implementations of one rule is how the rule drifts")


def test_no_value_in_the_envelope_is_a_percentage_ratio_interval_or_badge(populated_service):
    import re

    env = populated_service.get_launch_stats_enveloped()
    for path, key, value in _walk(env):
        if isinstance(key, str):
            assert not is_forbidden_surface_field(key), path
            for token in ("precision", "accuracy", "ratio", "share", "pct", "percent",
                          "confidence", "badge", "review"):
                assert token not in key.lower(), f"{path} names a quality measure"
        if isinstance(value, str):
            assert "%" not in value, path
            assert "Expert-reviewed" not in value, path
            assert not re.search(r"\d*\.\d+", value), path
        if isinstance(value, float):
            pytest.fail(f"{path} is a float -- every launch figure is a COUNT")


def _walk(node, path=""):
    if isinstance(node, dict):
        for k, v in node.items():
            child = f"{path}.{k}" if path else str(k)
            yield child, k, v
            yield from _walk(v, child)
    elif isinstance(node, (list, tuple)):
        for i, v in enumerate(node):
            child = f"{path}[{i}]"
            yield child, None, v
            yield from _walk(v, child)


# ===========================================================================
# Real-artifact resolution (shared with the freshness half in Task 2)
# ===========================================================================

def _resolve_real_artifact():
    """The artifact the guard measures against, or None.

    `DISCOVERY_LAUNCH_GUARD_DB` when set; otherwise the repository manifest's
    selection -- and only when it passes the SAME audience and required-table
    checks the public loader applies, imported from the loader itself so the two
    cannot drift apart.
    """
    return resolve_guard_artifact()[0]


def resolve_guard_artifact():
    """`(path, reason)` -- exactly one of the two is None.

    A resolution failure is NEVER a silent green: when `DISCOVERY_LAUNCH_GUARD_DB`
    is set, an unusable value is a NAMED reason the caller must fail on.
    """
    from web.discovery_assets import _PUBLIC_LOADER_AUDIENCE, _REQUIRED_TABLES

    explicit = os.environ.get("DISCOVERY_LAUNCH_GUARD_DB")
    if explicit:
        return _check_artifact(explicit, _PUBLIC_LOADER_AUDIENCE, _REQUIRED_TABLES,
                               explicit=True)
    manifest = _REPO_ROOT / "discovery_data" / "manifest.json"
    if not manifest.is_file():
        return None, None
    try:
        basename = json.loads(manifest.read_text(encoding="utf-8"))["asset_basename"]
    except Exception:
        return None, None
    return _check_artifact(str(_REPO_ROOT / "discovery_data" / f"{basename}.db"),
                           _PUBLIC_LOADER_AUDIENCE, _REQUIRED_TABLES, explicit=False)


def _check_artifact(path, required_audience, required_tables, *, explicit):
    if not Path(path).is_file():
        return None, (f"DISCOVERY_LAUNCH_GUARD_DB names {path!r}, which is not a file"
                      if explicit else None)
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except Exception:
        return None, (f"DISCOVERY_LAUNCH_GUARD_DB names {path!r}, which could not be "
                      "opened read-only" if explicit else None)
    try:
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        missing = required_tables - tables
        if missing:
            return None, (f"{path!r} is missing required table(s) "
                          f"{sorted(missing)}" if explicit else None)
        audience = None
        for row in conn.execute("SELECT value FROM meta WHERE key = 'audience'"):
            audience = row[0]
        if audience != required_audience:
            return None, (f"{path!r} carries meta.audience={audience!r}; the public "
                          f"loader serves only {required_audience!r}" if explicit else None)
    except Exception:
        return None, (f"{path!r} could not be inspected" if explicit else None)
    finally:
        conn.close()
    return path, None


# ###########################################################################
# TASK 2 -- the no-literals guard.
#
# The scanner's LIMIT, and the mechanism that covers it (136-18's SENTINEL
# provenance test), are stated in this module's DOCSTRING rather than here,
# so there is one statement of the pairing and not two that can drift.
# ###########################################################################

import ast          # noqa: E402 -- the guard's own imports, kept beside it
import hashlib      # noqa: E402
import re           # noqa: E402

#: THE SCANNER'S scope rule. Four globs, so a module added later is in scope by
#: DEFAULT rather than by someone remembering to add it to a list.
_SCAN_GLOBS = (
    "web/pages/*.py",
    "web/components/*.py",
    "web/discovery*.py",
    "shared/discovery_*.py",
)

#: The translation table, scanned alongside the source tree. A translated
#: headline is a string, and a number baked into one is invisible to any code
#: review that greps for digits in `.py` files.
_TRANSLATIONS_REL = "genizah_translations.py"

#: The modules that can render or carry these numbers TODAY. A FLOOR on the
#: derivation above -- asserted to be inside it -- never a substitute for it.
_FLOOR_MODULES = (
    "web/pages/findings.py",
    "web/components/findings_rows.py",
    "web/components/discovery_panel.py",
    "web/pages/browse.py",
    "web/pages/browse_enrichment.py",
    "web/discovery.py",
    "shared/discovery_service.py",
    "shared/discovery_display_strings.py",
)

#: The committed artifact-derived half of the forbidden list.
_FIGURE_FILE_REL = "tests/fixtures/discovery/launch_figures.json"

#: The committed HISTORICAL half: every superseded figure, each naming what it
#: retires. Held in the TEST MODULE and loaded by its OWN loader, so that
#: mis-wiring either half is detectable independently of the other.
_HISTORICAL_FIGURES = (
    {"value": 13285, "reason":
        "the MIXED-BASIS contribution total ruling U corrected: a main-pool "
        "fills_gap count added to UNFILTERED refines_granularity and "
        "container_predicts counts. It must never reappear on any surface."},
    {"value": 10432, "reason":
        "the private rebuild's main-pool contribution total -- a correct answer "
        "about a DIFFERENT artifact than the one the public loader serves"},
    {"value": 7563, "reason":
        "the private rebuild's contributing-manuscript count"},
    {"value": 19902, "reason":
        "the private rebuild's all-bucket contribution total"},
    {"value": 4434, "reason": "the private rebuild's fills_gap count"},
    {"value": 4503, "reason": "the private rebuild's refines_granularity count"},
    {"value": 1495, "reason": "the private rebuild's container_predicts count"},
    {"value": 3894, "reason": "the private rebuild's fills_gap manuscript count"},
    {"value": 2687, "reason":
        "the private rebuild's refines_granularity manuscript count"},
    {"value": 44384, "reason": "the private rebuild's corpus manuscript count"},
    {"value": 195274, "reason": "the private rebuild's corpus page count"},
)

#: THE SHIPPED EXEMPTION LIST. Its expected state is EMPTY, and a test asserts
#: that separately. The mechanism exists so that the first collision between a
#: launch figure and a legitimate constant is not resolved by deleting the
#: guard -- never as a way to reintroduce the defect.
LAUNCH_LITERAL_EXEMPTIONS = ()

#: Characters that must not abut a rendered figure for it to count as a hit --
#: so `1177402` and `177402.5` are not read as the corpus page count.
_FIGURE_BOUNDARY = set("0123456789.,_") | {"٬", " ", " ", "׳"}

#: Hebrew-locale digit grouping. Hebrew renders thousands with the same comma
#: the Latin locale does -- so the comma form already covers both -- and these
#: are the ADDITIONAL separators a localized string might carry.
_HEBREW_GROUPERS = ("٬", " ", " ", "׳")

#: The folder's bound. `**` is excluded OUTRIGHT rather than bounded (it is the
#: one operator whose result grows faster than its source text), and any operand
#: beyond this magnitude is refused, so a seeded `2 ** 9999999` can never make
#: the scan hang. A folder that evaluates arbitrary expressions is a scanner
#: nobody runs in CI.
_FOLD_MAX_OPERAND = 10 ** 12


class LaunchLiteralViolation:
    """One flagged constant: WHERE it is, WHICH figure it renders, and -- when
    it was folded -- the SOURCE EXPRESSION it came from.

    The source expression is not decoration: "9523 is forbidden" is useless
    guidance when the source reads `9_000 + 523`.
    """

    __slots__ = ("path", "line", "figure", "form", "source", "positions")

    def __init__(self, path, line, figure, form, source, positions=()):
        self.path = path
        self.line = line
        self.figure = figure
        self.form = form
        self.source = source
        self.positions = tuple(positions)

    def message(self):
        folded = ("" if self.source is None
                  else f" (folded from the expression `{self.source}`)")
        return (
            f"{self.path}:{self.line} contains the launch figure {self.figure:,} "
            f"as {self.form}{folded}. Launch figures change on every rebuild and "
            f"may NEVER be literals: read this number through "
            f"web.discovery.get_launch_stats_enveloped() instead."
        )

    def __repr__(self):  # pragma: no cover -- diagnostic only
        return f"<LaunchLiteralViolation {self.path}:{self.line} {self.figure}>"


# ---------------------------------------------------------------------------
# Scope derivation
# ---------------------------------------------------------------------------

def _norm(path, root):
    return Path(path).resolve().relative_to(Path(root).resolve()).as_posix()


def scanner_scanned_paths(root, globs=None):
    """The SCANNER's own derivation of what it scans, from `_SCAN_GLOBS`."""
    patterns = _SCAN_GLOBS if globs is None else globs
    found = set()
    for pattern in patterns:
        for path in Path(root).glob(pattern):
            if path.is_file():
                found.add(_norm(path, root))
    return found


# ---------------------------------------------------------------------------
# Literal forms
# ---------------------------------------------------------------------------

def _digit_groups(digits):
    out = []
    while len(digits) > 3:
        out.insert(0, digits[-3:])
        digits = digits[:-3]
    out.insert(0, digits)
    return out


def _string_forms(value):
    """Every way `value` can be WRITTEN inside a string."""
    bare = str(value)
    grouped = f"{value:,}"
    forms = {bare, grouped, "_".join(_digit_groups(bare))}
    for separator in _HEBREW_GROUPERS:
        forms.add(grouped.replace(",", separator))
    return forms


def _string_hits(text, figures):
    for figure in figures:
        for form in _string_forms(figure):
            start = 0
            while True:
                index = text.find(form, start)
                if index < 0:
                    break
                start = index + 1
                before = text[index - 1] if index else ""
                after_index = index + len(form)
                after = text[after_index] if after_index < len(text) else ""
                if before in _FIGURE_BOUNDARY or after in _FIGURE_BOUNDARY:
                    continue
                yield figure, f"the string {form!r}"


# ---------------------------------------------------------------------------
# The bounded constant folder
# ---------------------------------------------------------------------------

_FOLD_BINOPS = {
    ast.Add: lambda a, b: a + b,
    ast.Sub: lambda a, b: a - b,
    ast.Mult: lambda a, b: a * b,
    ast.Div: lambda a, b: a / b,
    ast.FloorDiv: lambda a, b: a // b,
    ast.Mod: lambda a, b: a % b,
}


def fold_numeric(node):
    """`node`'s value if it folds to a number, else None.

    Bounded on purpose. `**` is refused OUTRIGHT, operands beyond
    `_FOLD_MAX_OPERAND` are refused, and ANY operation that raises -- a division
    by zero, an overflow -- is skipped rather than propagated. The scan must
    complete over hostile input; a folder that hangs or raises is a gate that
    gets removed.
    """
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
            return None
        return node.value if abs(node.value) <= _FOLD_MAX_OPERAND else None
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
        inner = fold_numeric(node.operand)
        if inner is None:
            return None
        return inner if isinstance(node.op, ast.UAdd) else -inner
    if isinstance(node, ast.BinOp):
        operation = _FOLD_BINOPS.get(type(node.op))
        if operation is None:               # `**` and everything else: refused
            return None
        left, right = fold_numeric(node.left), fold_numeric(node.right)
        if left is None or right is None:
            return None
        try:
            result = operation(left, right)
        except Exception:                   # ZeroDivisionError, OverflowError...
            return None
        return result
    return None


def _as_figure(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float):
        if not value.is_integer():
            return None
        value = int(value)
    return value


# ---------------------------------------------------------------------------
# Position classification -- reused by BOTH the scan and the exemption rules,
# so an exemption rule cannot drift away from what the scanner looks at.
# ---------------------------------------------------------------------------

def _parent_map(tree):
    parents = {}
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            parents[child] = node
    return parents


def _attribute_root(node):
    while isinstance(node, ast.Attribute):
        node = node.value
    return node.id if isinstance(node, ast.Name) else None


def position_labels(node, parents, key_names):
    """Every DISPLAY-REACHABLE position `node` sits in.

    Empty means no reader can see it through any path this scanner understands.
    """
    labels = []
    child = node
    current = parents.get(node)
    while current is not None:
        if isinstance(current, (ast.JoinedStr, ast.FormattedValue)):
            labels.append("inside an f-string")
        elif isinstance(current, ast.Call):
            func = current.func
            if isinstance(func, ast.Name) and func.id == "str":
                labels.append("an operand of str()")
            elif isinstance(func, ast.Attribute) and func.attr == "format":
                labels.append("an operand of .format()")
            if isinstance(func, ast.Attribute) and _attribute_root(func) == "ui":
                labels.append("an argument of a ui.* call")
            for keyword in current.keywords:
                if keyword.value is child and keyword.arg in key_names:
                    labels.append(f"bound to the envelope key {keyword.arg!r}")
        elif isinstance(current, ast.BinOp) and isinstance(current.op, ast.Mod):
            if isinstance(current.left, ast.Constant) and isinstance(
                    current.left.value, str):
                labels.append("an operand of %-formatting")
        elif isinstance(current, ast.Assign) and current.value is child:
            for target in current.targets:
                name = (target.id if isinstance(target, ast.Name)
                        else target.attr if isinstance(target, ast.Attribute)
                        else None)
                if name in key_names:
                    labels.append(f"bound to the envelope key {name!r}")
        elif isinstance(current, ast.AnnAssign) and current.value is child:
            name = getattr(current.target, "id",
                           getattr(current.target, "attr", None))
            if name in key_names:
                labels.append(f"bound to the envelope key {name!r}")
        elif isinstance(current, ast.Dict):
            for key, value in zip(current.keys, current.values):
                if value is child and isinstance(key, ast.Constant) \
                        and key.value in key_names:
                    labels.append(f"under the envelope key {key.value!r}")
        child = current
        current = parents.get(current)
    return tuple(dict.fromkeys(labels))


# ---------------------------------------------------------------------------
# The scan
# ---------------------------------------------------------------------------

def _docstring_nodes(tree):
    found = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Module, ast.ClassDef, ast.FunctionDef,
                             ast.AsyncFunctionDef)):
            body = getattr(node, "body", None)
            if body and isinstance(body[0], ast.Expr) \
                    and isinstance(body[0].value, ast.Constant) \
                    and isinstance(body[0].value.value, str):
                found.add(body[0].value)
    return found


def scan_python_source(source, rel_path, figures, key_names, *,
                       exclude_docstrings=True):
    """Every launch-figure literal a reader could see in `source`.

    COMMENTS are structurally absent from the AST and DOCSTRINGS are excluded:
    a figure in either is not rendered, and the modules this guard covers carry
    comments explaining the guard itself, so an unfiltered scan would invalidate
    its own gate.
    """
    violations = []
    try:
        tree = ast.parse(source)
    except SyntaxError:                     # pragma: no cover -- defensive
        return violations
    parents = _parent_map(tree)
    skip = _docstring_nodes(tree) if exclude_docstrings else set()
    for node in ast.walk(tree):
        if node in skip:
            continue
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            positions = None
            for figure, form in _string_hits(node.value, figures):
                if positions is None:
                    positions = position_labels(node, parents, key_names)
                violations.append(LaunchLiteralViolation(
                    rel_path, node.lineno, figure, form, None, positions))
        elif isinstance(node, ast.Constant):
            figure = _as_figure(node.value)
            if figure in figures:
                violations.append(LaunchLiteralViolation(
                    rel_path, node.lineno, figure, "a numeric constant", None,
                    position_labels(node, parents, key_names)))
        elif isinstance(node, (ast.BinOp, ast.UnaryOp)):
            figure = _as_figure(fold_numeric(node))
            if figure in figures:
                violations.append(LaunchLiteralViolation(
                    rel_path, node.lineno, figure, "a computed constant",
                    ast.unparse(node),
                    position_labels(node, parents, key_names)))
    return violations


def scan_launch_literals(root, figures, key_names, *, globs=None,
                         include_translations=True):
    """Scan the glob-derived source set PLUS the translation table.

    `root` is a PARAMETER: every positive control points it at a temporary tree,
    so no control ever edits a committed file.
    """
    figures = frozenset(figures)
    violations = []
    for rel in sorted(scanner_scanned_paths(root, globs)):
        text = (Path(root) / rel).read_text(encoding="utf-8", errors="replace")
        violations.extend(scan_python_source(text, rel, figures, key_names))
    if include_translations:
        translations = Path(root) / _TRANSLATIONS_REL
        if translations.is_file():
            violations.extend(scan_python_source(
                translations.read_text(encoding="utf-8", errors="replace"),
                _TRANSLATIONS_REL, figures, key_names,
                # NO exclusion here: every string in a translation table is
                # rendered by definition.
                exclude_docstrings=False))
    return violations


# ---------------------------------------------------------------------------
# The forbidden union -- two committed halves, each with its OWN loader
# ---------------------------------------------------------------------------

def load_committed_figures(path=None):
    """The ARTIFACT-DERIVED half, from its committed file."""
    path = Path(path) if path else _REPO_ROOT / _FIGURE_FILE_REL
    return json.loads(path.read_text(encoding="utf-8"))["figures"]


def load_historical_figures(entries=None):
    """The HISTORICAL half, from the committed in-module list."""
    return tuple(_HISTORICAL_FIGURES if entries is None else entries)


def forbidden_figures(figure_file=None, historical=None):
    """The UNION the scan forbids.

    Non-empty in EVERY environment, artifact present or not, because BOTH halves
    ship in the repository. The round-5 draft derived its expectations at test
    time and fell back to history when no artifact loaded -- and since the
    repository manifest selects a pre-rebuild asset the public loader refuses,
    that resolved to "forbid nothing, report green" on the very machine this
    runs on.
    """
    committed = load_committed_figures(figure_file)
    entries = load_historical_figures(historical)
    values = {int(entry["value"]) for entry in committed.values()}
    values |= {int(entry["value"]) for entry in entries}
    return values


# ---------------------------------------------------------------------------
# The envelope's own numeric key paths -- what COMPLETENESS is measured against
# ---------------------------------------------------------------------------

def numeric_key_paths(envelope):
    """`{key path: value}` for every NUMERIC value the envelope exposes.

    Derived by walking the envelope itself, never hand-listed: the committed
    figure file's key set must EQUAL this, so a file holding one figure FAILS
    rather than passing a non-emptiness check while every other launch number
    stays free to be hardcoded.
    """
    found = {}

    def walk(node, path):
        if isinstance(node, bool):
            return
        if isinstance(node, (int, float)):
            found[path] = node
        elif isinstance(node, dict):
            for key, value in node.items():
                walk(value, f"{path}.{key}" if path else str(key))
        elif isinstance(node, (list, tuple)):
            for index, value in enumerate(node):
                walk(value, f"{path}[{index}]")

    for key, value in envelope.items():
        if key == "status":
            continue
        walk(value, key)
    return found


def envelope_key_names(envelope):
    """The LEAF names of those key paths -- what the position classifier treats
    as a display-reachable binding."""
    names = set()
    for path in numeric_key_paths(envelope):
        leaf = path.split(".")[-1]
        names.add(re.sub(r"\[\d+\]$", "", leaf))
    return names


def compute_launch_figures(db_path):
    """`{key path: value}` recomputed from `db_path` -- the freshness half's
    right-hand side, and the generator for the committed file."""
    service = DiscoveryService(
        path_provider=lambda: str(db_path),
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: None,
    )
    envelope = service.get_launch_stats_enveloped()
    if envelope["status"] != "ok":
        raise RuntimeError(
            f"the launch reader returned {envelope['status']!r} for the guard "
            "artifact; the figure file cannot be generated from it")
    return numeric_key_paths(envelope), envelope


def regenerate_committed_figures():
    """Rewrite `tests/fixtures/discovery/launch_figures.json` from the artifact
    `DISCOVERY_LAUNCH_GUARD_DB` names.

    Named in the freshness failure message, because a guard that fails with
    "assertion failed" gets deleted by the next person who trips it.
    """
    path, reason = resolve_guard_artifact()
    if path is None:
        raise SystemExit(reason or _NO_ARTIFACT_HINT)
    figures, envelope = compute_launch_figures(path)
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    provenance = {
        "sidecar_version": envelope["meta"]["sidecar_version"],
        "audience": envelope["meta"]["audience"],
        "content_hash": digest.hexdigest(),
    }
    payload = {
        "_comment": (
            "GENERATED -- do not hand-edit. The artifact-derived half of the "
            "no-literals forbidden list (plan 136-22, ruling U). Committed so "
            "the guard forbids something in EVERY environment, artifact present "
            "or not. Regenerate with: DISCOVERY_LAUNCH_GUARD_DB=<sidecar.db> "
            "python -c \"from tests.test_discovery_launch_stats import "
            "regenerate_committed_figures as r; r()\""
        ),
        "figures": {
            key: {"value": value, **provenance} for key, value in figures.items()
        },
    }
    target = _REPO_ROOT / _FIGURE_FILE_REL
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return target


def _fixture_envelope(tmp_dir):
    """The launch envelope over Task 1's populated fixture.

    Every completeness and classification assertion is derived from THIS, so it
    holds in every environment -- artifact present or not.
    """
    db = _build_launch_db(Path(tmp_dir) / "keyset.db", _POPULATED_ROWS,
                          pages=_EXPECTED_PAGES)
    return _service_for(db).get_launch_stats_enveloped()


_REGENERATE_COMMAND = (
    'DISCOVERY_LAUNCH_GUARD_DB=<sidecar.db> python -c "from '
    'tests.test_discovery_launch_stats import regenerate_committed_figures as r; r()"'
)
_NO_ARTIFACT_HINT = (
    "no discovery artifact resolves: set DISCOVERY_LAUNCH_GUARD_DB to a "
    "public-audience sidecar carrying every required table"
)


# ---------------------------------------------------------------------------
# Exemption admissibility -- decided by the scanner's OWN classification of the
# figure and its position, NEVER by a directory prefix.
# ---------------------------------------------------------------------------

RULE_1 = "rule 1 (a CURRENT figure in a display-reachable position)"
RULE_2 = "rule 2 (a display-reachable POSITION)"
RULE_3 = "rule 3 (a STALE entry: no flagged constant at that file, line and figure)"
RULE_4 = "rule 4 (the directory FLOOR: never under web/pages/ or web/components/)"


def validate_exemptions(entries, root, figures, key_names, current_values):
    """`{index: [rules broken]}` for every INADMISSIBLE entry.

    Rules 1 and 2 are decided by re-running the SCANNER'S OWN position
    classifier over the node at the exempted `(file, line)`. Reusing the
    classifier is the point: the rule cannot drift away from what the scanner
    actually looks at, and "not under `web/pages/`" -- the rule this replaces --
    would have accepted a hardcoded `total = <figure>` fallback in
    `web/discovery.py`, which is a file, not a page, and can hand a figure
    straight to one.
    """
    rejected = {}
    for index, entry in enumerate(entries):
        broken = []
        missing = [field for field in ("file", "line", "figure", "reason")
                   if not entry.get(field)]
        if missing:
            broken.append(f"incomplete entry (missing {sorted(missing)})")
            rejected[index] = broken
            continue

        rel = entry["file"].replace("\\", "/")
        if rel.startswith("web/pages/") or rel.startswith("web/components/"):
            broken.append(RULE_4)

        source_path = Path(root) / rel
        found = []
        if source_path.is_file():
            found = [
                violation for violation in scan_python_source(
                    source_path.read_text(encoding="utf-8", errors="replace"),
                    rel, frozenset(figures), key_names)
                if violation.line == entry["line"]
                and violation.figure == entry["figure"]
            ]
        if not found:
            broken.append(RULE_3)
        else:
            display_reachable = [v for v in found if v.positions]
            if display_reachable:
                broken.append(RULE_2)
                if entry["figure"] in current_values:
                    broken.append(RULE_1)
        if broken:
            rejected[index] = broken
    return rejected


# ---------------------------------------------------------------------------
# Non-destructive seeding: every positive control points the scanner's ROOT
# parameter at a temporary tree. No control edits a committed file.
# ---------------------------------------------------------------------------

def _seed_tree(tmp_path, files):
    root = Path(tmp_path) / "seeded-root"
    for rel, text in files.items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text, encoding="utf-8")
    return root


def _copy_with_line(rel, extra_line):
    """The REAL module's source plus one appended line -- and that line's
    number, so a control can assert the violation is reported against it."""
    original = (_REPO_ROOT / rel).read_text(encoding="utf-8")
    if not original.endswith("\n"):
        original += "\n"
    seeded = original + extra_line + "\n"
    return seeded, len(seeded.splitlines())


def _tracked_fingerprint():
    """A byte-level fingerprint of every file a control could plausibly touch.

    Scoped to the scanned set, the translation table and the committed figure
    file rather than the whole working tree: other agents may hold unrelated
    work in this checkout, and a fingerprint that goes red for THEIR edit is a
    fingerprint the next person deletes.
    """
    # Deliberately built from the TEST'S OWN expansion, never from
    # `scanner_scanned_paths`: one control monkeypatches the scanner's glob
    # list, and a fingerprint derived from the thing under test would change
    # shape under that patch and report a phantom edit.
    interesting = sorted(_independent_expansion()) + [
        _TRANSLATIONS_REL, _FIGURE_FILE_REL]
    fingerprint = {}
    for rel in interesting:
        path = _REPO_ROOT / rel
        fingerprint[rel] = (hashlib.sha256(path.read_bytes()).hexdigest()
                            if path.is_file() else None)
    return fingerprint


@pytest.fixture
def guard_context(tmp_path):
    """`(figures, key_names)` -- the forbidden union and the envelope's own
    numeric key leaf names, both derived rather than hand-listed."""
    envelope = _fixture_envelope(tmp_path)
    return forbidden_figures(), envelope_key_names(envelope)


# ===========================================================================
# Task 2: the web wrapper
# ===========================================================================

def _web(monkeypatch, service, available=True):
    import web.discovery as web_discovery
    monkeypatch.setattr(web_discovery, "discovery_available", lambda: available)
    monkeypatch.setattr(web_discovery, "_service", service)
    return web_discovery


def test_the_wrapper_returns_the_four_key_envelope_on_ok_and_on_outage(
        populated_db, monkeypatch):
    web_discovery = _web(monkeypatch, _service_for(populated_db))
    ok = asyncio.run(web_discovery.get_launch_stats_enveloped())
    assert set(ok) == _ENVELOPE_KEYS and ok["status"] == "ok"
    assert ok["total"] == _EXPECTED_TOTAL

    web_discovery = _web(monkeypatch, _service_for(populated_db), available=False)
    outage = asyncio.run(web_discovery.get_launch_stats_enveloped())
    assert set(outage) == _ENVELOPE_KEYS
    assert outage["status"] == "unavailable"


def test_the_wrapper_returns_unavailable_rather_than_none_or_empty(
        populated_db, monkeypatch):
    web_discovery = _web(monkeypatch, _service_for(populated_db), available=False)
    env = asyncio.run(web_discovery.get_launch_stats_enveloped())
    assert env is not None and env != [] and env != {}
    assert env["status"] == "unavailable"
    assert env["meta"]["reason"] == "sidecar_not_serving"
    assert env["items"] == [] and env["total"] == 0


@pytest.mark.parametrize("error, expected_status, expected_reason", [
    ("DiscoveryOverload", "busy", "bounded_concurrency"),
    ("DiscoveryUnavailable", "timeout", "query_timeout"),
])
def test_the_wrapper_maps_each_service_error_to_its_own_status(
        populated_db, monkeypatch, error, expected_status, expected_reason):
    import shared.discovery_errors as errors

    service = _service_for(populated_db)

    async def _raise():
        raise getattr(errors, error)("boom")

    monkeypatch.setattr(service, "get_launch_stats_enveloped_async", _raise)
    web_discovery = _web(monkeypatch, service)
    env = asyncio.run(web_discovery.get_launch_stats_enveloped())
    assert env["status"] == expected_status
    assert env["meta"]["reason"] == expected_reason
    assert env["total"] == 0


def _two_artifact_service(tmp_path):
    db_a = _build_launch_db(tmp_path / "wrapper-a.db", _POPULATED_ROWS,
                            pages=_EXPECTED_PAGES, sidecar_version="same-version")
    db_b = _build_launch_db(
        tmp_path / "wrapper-b.db",
        _POPULATED_ROWS + (("s100", "A", 1, "fills_gap"),),
        pages=_EXPECTED_PAGES, sidecar_version="same-version")
    current = {"path": db_a}
    service = DiscoveryService(
        path_provider=lambda: current["path"],
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: "same-version",
    )
    return service, current, db_b


def test_the_path_switch_holds_THROUGH_THE_PUBLIC_ASYNC_WRAPPER(tmp_path, monkeypatch):
    """Task 1's criteria exercise the reader DIRECTLY, so they all pass against
    a wrapper that re-caches above it. This is the assertion that does not."""
    service, current, db_b = _two_artifact_service(tmp_path)
    web_discovery = _web(monkeypatch, service)

    asyncio.run(web_discovery.get_launch_stats_enveloped())
    first = asyncio.run(web_discovery.get_launch_stats_enveloped())
    assert first["total"] == _EXPECTED_TOTAL

    current["path"] = db_b
    second = asyncio.run(web_discovery.get_launch_stats_enveloped())
    assert second["total"] == _EXPECTED_TOTAL + 1, (
        "the FIRST post-switch call through the public wrapper served the "
        "previous artifact's headline")


def test_routing_the_read_through_cache_name_would_serve_the_stale_artifact(
        tmp_path, monkeypatch):
    """The mutation control, run as a test rather than only by hand.

    `_enveloped_off_loop(..., cache_name=...)` delegates to `_browse_cached_call`,
    whose key is `(cache_name,) + args + (version,)` -- with NO path component --
    and which returns BEFORE the wrapped sync callable runs. This drives exactly
    that call and asserts it serves the PREVIOUS artifact, which is what makes
    the shipped wrapper's omission of `cache_name` load-bearing rather than
    stylistic.
    """
    service, current, db_b = _two_artifact_service(tmp_path)

    async def through_cache_name():
        return await service._enveloped_off_loop(
            service.get_launch_stats_enveloped, (),
            timeout=service._findings_timeout(), heavy=True,
            cache_name="launch_stats")

    assert asyncio.run(through_cache_name())["total"] == _EXPECTED_TOTAL
    current["path"] = db_b
    assert asyncio.run(through_cache_name())["total"] == _EXPECTED_TOTAL, (
        "the path-blind outer LRU did NOT serve a stale answer -- if this ever "
        "becomes true, _browse_cached_call has gained a path component and this "
        "plan's reason for bypassing it needs re-reading")

    # ...while the shipped wrapper, on the same switch, does not.
    web_discovery = _web(monkeypatch, service)
    assert asyncio.run(
        web_discovery.get_launch_stats_enveloped())["total"] == _EXPECTED_TOTAL + 1


def test_the_shipped_async_wrapper_passes_no_cache_name_and_the_findings_timeout():
    import inspect
    import textwrap

    source = textwrap.dedent(
        inspect.getsource(DiscoveryService.get_launch_stats_enveloped_async))
    calls = [
        node for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "_enveloped_off_loop"
    ]
    assert len(calls) == 1
    keywords = {kw.arg: kw.value for kw in calls[0].keywords}
    assert "cache_name" not in keywords, (
        "the launch read must NOT go through _browse_cached_call: its key "
        "carries no path, so it would cancel the reader's own (path, version) "
        "cache and serve the previous artifact's headline")
    timeout = keywords.get("timeout")
    assert isinstance(timeout, ast.Call) and timeout.func.attr == "_findings_timeout"


def test_the_read_runs_under_the_findings_timeout_not_the_browse_timeout(
        populated_db, monkeypatch):
    """A second, INDEPENDENT detector of the same routing defect: it does not
    depend on the cache-key question at all. `_browse_cached_call` hardcodes
    `self._browse_timeout()`, so this fails if the call is ever routed back
    through it."""
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_BROWSE", "2.0")
    monkeypatch.setenv("DISCOVERY_QUERY_TIMEOUT_FINDINGS", "17.5")
    service = _service_for(populated_db)
    seen = []
    original = service._run_off_loop

    async def spy(sync_fn, *args, timeout, heavy=False):
        seen.append(timeout)
        return await original(sync_fn, *args, timeout=timeout, heavy=heavy)

    monkeypatch.setattr(service, "_run_off_loop", spy)
    asyncio.run(service.get_launch_stats_enveloped_async())
    assert seen == [17.5], (
        f"the read ran under {seen} -- the findings budget is 17.5 and the "
        "browse budget is 2.0")


# ===========================================================================
# Task 2: the guard's SCOPE -- proved by EXACT SET EQUALITY against an
# expansion the test computes for ITSELF.
# ===========================================================================

#: The test's OWN glob list. A LITERAL list written here, deliberately NOT
#: imported from the scanner: a single list read twice is not two computations,
#: and a test that imported `_SCAN_GLOBS` would shrink in lockstep with any
#: mutation to it -- which is exactly how a circular check looks from inside.
_INDEPENDENT_GLOBS = (
    "web/pages/*.py",
    "web/components/*.py",
    "web/discovery*.py",
    "shared/discovery_*.py",
)


def _independent_expansion(root=None, globs=None):
    """The test's own expansion, normalised the same way the scanner's is:
    repository-relative, POSIX separators, duplicates collapsed.

    The normalisation is not incidental. An equality that goes red because one
    side produced backslashes is an assertion the next person deletes, and it
    would take the real check with it.
    """
    root = Path(root or _REPO_ROOT)
    found = set()
    for pattern in (globs or _INDEPENDENT_GLOBS):
        for path in root.glob(pattern):
            if path.is_file():
                found.add(path.resolve().relative_to(root.resolve()).as_posix())
    return found


def test_the_scanned_set_EQUALS_an_independent_expansion_of_the_four_globs():
    derived = scanner_scanned_paths(_REPO_ROOT)
    independent = _independent_expansion()
    assert independent, "the test's own glob expansion found nothing"
    missing = sorted(independent - derived)
    extra = sorted(derived - independent)
    assert not missing, f"the scanner does not scan {missing}"
    assert not extra, f"the scanner scans {extra}, which no glob expands to"
    assert derived == independent


def test_dropping_one_glob_from_THE_SCANNER_breaks_the_equality(monkeypatch):
    """What proves the two lists are INDEPENDENT computations rather than one
    list read twice.

    Monkeypatches the SCANNER'S pattern constant -- never its source file -- and
    watches the equality fail. Had the test imported `_SCAN_GLOBS`, both sides
    would shrink together and this control would pass, which is precisely the
    circularity round 10 found: a scanner hardcoding `floor + {one arbitrary
    module}` satisfies "strictly larger", satisfies dynamic selection and
    satisfies a seeded violation, while never consulting a glob.
    """
    module = sys.modules[__name__]

    before = _tracked_fingerprint()
    # Capture BEFORE patching: a bare `_SCAN_GLOBS` inside this function is a
    # module-global lookup resolved at execution time, so after the patch it
    # would name the reduced tuple and the assertion below would compare the
    # mutation against itself.
    dropped = _SCAN_GLOBS[0]
    monkeypatch.setattr(module, "_SCAN_GLOBS", _SCAN_GLOBS[1:])
    reduced = scanner_scanned_paths(_REPO_ROOT)
    independent = _independent_expansion()
    assert reduced != independent, (
        "dropping a glob from the scanner changed nothing -- the two lists are "
        "not independent computations")
    # The modules that vanished are EXACTLY the dropped glob's own expansion --
    # asserted against the test's independent expansion of that one pattern, so
    # the control names what it lost rather than merely noticing a difference.
    disappeared = sorted(independent - reduced)
    assert disappeared == sorted(_independent_expansion(globs=(dropped,)))
    assert _tracked_fingerprint() == before


def test_the_named_floor_is_a_SUBSET_of_the_derived_set_never_its_definition():
    derived = scanner_scanned_paths(_REPO_ROOT)
    # Every floor module is covered by the RULE, whether or not it exists yet:
    # two of them are created by later plans in this phase, and a module that
    # does not exist cannot be in a filesystem expansion. The rule-level check
    # is the one that must hold for all eight.
    import fnmatch
    for rel in _FLOOR_MODULES:
        assert any(fnmatch.fnmatch(rel, pattern) for pattern in _INDEPENDENT_GLOBS), (
            f"{rel} matches none of the four globs -- it is named as a floor "
            "module but the scope rule would not pick it up")
    present = {rel for rel in _FLOOR_MODULES if (_REPO_ROOT / rel).is_file()}
    assert present <= derived, f"floor modules missing from the scan: {present - derived}"
    assert len(derived) > len(present), (
        "the derived set is no larger than the floor -- the globs would then be "
        "indistinguishable from a hand-written list")


# ===========================================================================
# Task 2: COMPLETENESS -- an exact key set, not non-emptiness
# ===========================================================================

def test_the_committed_figure_file_key_set_EQUALS_the_envelopes_numeric_keys(tmp_path):
    """Runs off the FIXTURE, so it holds in every environment, artifact or none.

    The broken implementation this catches: a figure file holding only `total`,
    whose freshness check passes because the recomputation is equally partial,
    leaving every other launch number free to be hardcoded.
    """
    expected = set(numeric_key_paths(_fixture_envelope(tmp_path)))
    committed = set(load_committed_figures())
    assert expected, "the envelope exposes no numeric value at all"
    assert committed - expected == set(), (
        f"the figure file names keys the envelope does not expose: "
        f"{sorted(committed - expected)}")
    assert expected - committed == set(), (
        f"the figure file is MISSING launch figures the envelope exposes: "
        f"{sorted(expected - committed)} -- regenerate with {_REGENERATE_COMMAND}")
    assert committed == expected
    # The four ruling-U headline numbers are among them, by construction.
    assert "total" in committed
    assert sum(1 for key in committed if key.endswith(".identification_count")) == 3


def test_the_historical_half_keeps_its_non_emptiness_floor():
    historical = load_historical_figures()
    assert len(historical) >= 2
    for entry in historical:
        assert isinstance(entry["value"], int)
        assert entry["reason"].strip(), "every retired figure must name what it retires"


def test_the_committed_file_records_provenance_for_every_value():
    for key, entry in load_committed_figures().items():
        for field in ("sidecar_version", "audience", "content_hash"):
            assert entry.get(field), f"{key} carries no {field}"
        assert isinstance(entry["value"], int)


def test_the_corrected_mixed_basis_figure_is_forbidden_and_named_as_such():
    """The guard's own coverage is ASSERTED, not assumed."""
    assert 13285 in forbidden_figures()
    entry = next(e for e in load_historical_figures() if e["value"] == 13285)
    assert "MIXED-BASIS" in entry["reason"]
    assert "never reappear" in entry["reason"]


# ===========================================================================
# Task 2: the forbidden union is non-empty EVERYWHERE, and BOTH halves are
# proved CONSUMED -- not merely valid.
# ===========================================================================

def test_the_forbidden_union_is_non_empty_with_no_artifact_resolvable(monkeypatch):
    monkeypatch.setenv("DISCOVERY_LAUNCH_GUARD_DB", str(_REPO_ROOT / "no-such.db"))
    assert resolve_guard_artifact()[0] is None
    assert len(forbidden_figures()) >= 10


def test_each_half_is_non_empty_on_its_own_and_read_from_its_own_source():
    assert load_committed_figures(), "the artifact-derived half is empty"
    assert load_historical_figures(), "the historical half is empty"


#: Two TAGGED sentinels, one per half. Nine digits, comfortably outside the
#: four-to-six-digit range the real figures occupy, and asserted absent from
#: both real halves before use so a control cannot pass for the wrong reason.
_SENTINEL_ARTIFACT_HALF = 907315462
_SENTINEL_HISTORICAL_HALF = 907315463


def test_the_sentinels_appear_in_neither_real_half_nor_the_repository(guard_context):
    figures, key_names = guard_context
    assert _SENTINEL_ARTIFACT_HALF not in figures
    assert _SENTINEL_HISTORICAL_HALF not in figures
    seeded = {_SENTINEL_ARTIFACT_HALF, _SENTINEL_HISTORICAL_HALF}
    assert not scan_launch_literals(_REPO_ROOT, seeded, key_names)


def test_the_ARTIFACT_DERIVED_half_is_CONSUMED_by_the_scan(tmp_path, guard_context):
    """Emptying a half proves the half is VALID. It does NOT prove it is
    CONSUMED: a union that silently ignores this half still passes an empty-half
    mutation, because emptying it trips its own standalone non-empty assertion.

    So the sentinel goes in through THIS half's own loader, and is required both
    to reach the union AND to raise a real, named scanner violation. That stays
    valid when the two halves' values OVERLAP -- which a correct rebuild
    reproducing a historical count will cause.
    """
    _, key_names = guard_context
    before = _tracked_fingerprint()

    payload = json.loads((_REPO_ROOT / _FIGURE_FILE_REL).read_text(encoding="utf-8"))
    payload["figures"]["sentinel"] = {
        "value": _SENTINEL_ARTIFACT_HALF, "sidecar_version": "sentinel",
        "audience": "public", "content_hash": "sentinel"}
    seeded_file = tmp_path / "launch_figures.sentinel.json"
    seeded_file.write_text(json.dumps(payload), encoding="utf-8")

    union = forbidden_figures(figure_file=seeded_file)
    assert _SENTINEL_ARTIFACT_HALF in union, (
        "the artifact-derived half's loader was not read at all")

    line = f"ui.label(str({_SENTINEL_ARTIFACT_HALF}))"
    root = _seed_tree(tmp_path, {"web/pages/findings.py": line + "\n"})
    violations = scan_launch_literals(root, union, key_names)
    hits = [v for v in violations if v.figure == _SENTINEL_ARTIFACT_HALF]
    assert hits, "the union reached the loader but never reached the scan"
    assert hits[0].path == "web/pages/findings.py" and hits[0].line == 1
    assert _tracked_fingerprint() == before


def test_the_HISTORICAL_half_is_CONSUMED_by_the_scan(tmp_path, guard_context, monkeypatch):
    """The same proof, in the other direction, through the historical half's own
    loader -- monkeypatching the committed in-module list, never editing it."""
    module = sys.modules[__name__]

    _, key_names = guard_context
    before = _tracked_fingerprint()
    monkeypatch.setattr(module, "_HISTORICAL_FIGURES", _HISTORICAL_FIGURES + (
        {"value": _SENTINEL_HISTORICAL_HALF, "reason": "tagged consumption sentinel"},
    ))
    union = module.forbidden_figures()
    assert _SENTINEL_HISTORICAL_HALF in union, (
        "the historical half's loader was not read at all")

    line = f"ui.label(str({_SENTINEL_HISTORICAL_HALF}))"
    root = _seed_tree(tmp_path, {"shared/discovery_display_strings.py": line + "\n"})
    violations = scan_launch_literals(root, union, key_names)
    hits = [v for v in violations if v.figure == _SENTINEL_HISTORICAL_HALF]
    assert hits, "the union reached the loader but never reached the scan"
    assert hits[0].path == "shared/discovery_display_strings.py"
    assert _tracked_fingerprint() == before


def test_emptying_a_half_trips_that_halfs_own_standalone_assertion(tmp_path, monkeypatch):
    """KEPT, but only as a control on the standalone non-empty assertions.

    It proves each half is VALID; it can never prove either is CONSUMED, which
    is why the two sentinel tests above exist and why neither may be deleted in
    favour of this one.
    """
    module = sys.modules[__name__]

    emptied = tmp_path / "empty.json"
    emptied.write_text(json.dumps({"figures": {}}), encoding="utf-8")
    assert not load_committed_figures(emptied)

    monkeypatch.setattr(module, "_HISTORICAL_FIGURES", ())
    assert not module.load_historical_figures()


# ===========================================================================
# Task 2: the POSITIVE controls -- every figure x every literal form
# ===========================================================================

def _split_operands(figure, figures):
    """Two operands that fold to `figure` while NEITHER is itself forbidden --
    so a folding control can never pass for the wrong reason."""
    for offset in (523, 517, 409, 307, 211, 101):
        left = figure - offset
        if left > 0 and left not in figures and offset not in figures:
            return left, offset
    raise AssertionError(f"no clean operand split for {figure}")  # pragma: no cover


def _literal_forms_for(figure, figures):
    left, right = _split_operands(figure, figures)
    grouped = f"{figure:,}"
    return {
        "bare integer constant": f"ui.label({figure})",
        "underscore-grouped integer": (
            "ui.label(" + "_".join(_digit_groups(str(figure))) + ")"),
        "thousands-separated string": f'ui.label("{grouped}")',
        "f-string": 'ui.label(f"{' + str(figure) + ':,}")',
        "str() argument": f"ui.label(str({figure}))",
        "Hebrew-locale grouped string": (
            'ui.label("' + grouped.replace(",", "٬") + '")'),
        "computed constant": (
            'ui.label(f"{' + f"{left} + {right}" + ':,}")'),
    }


_ALL_FIGURES = sorted(forbidden_figures())
_ALL_FORMS = tuple(_literal_forms_for(_ALL_FIGURES[0], set(_ALL_FIGURES)))


@pytest.mark.parametrize("figure", _ALL_FIGURES)
@pytest.mark.parametrize("form", _ALL_FORMS)
def test_every_figure_in_every_literal_form_is_caught(figure, form, tmp_path,
                                                      guard_context):
    """The broken implementation this catches is a string-literal-only scanner,
    which passes every integer-constant form while appearing fully tested."""
    figures, key_names = guard_context
    snippet = _literal_forms_for(figure, figures)[form]
    root = _seed_tree(tmp_path, {"web/pages/findings.py": "\n" + snippet + "\n"})
    hits = [v for v in scan_launch_literals(root, figures, key_names)
            if v.figure == figure]
    assert hits, f"{form} of {figure} walked straight through the scan"
    assert hits[0].path == "web/pages/findings.py"
    assert hits[0].line == 2
    assert str(figure) in hits[0].message() or f"{figure:,}" in hits[0].message()


def test_the_str_call_form_reports_the_line_of_the_str_call(tmp_path, guard_context):
    """`ui.label(str(<figure>))` contains NO string literal anywhere, so a
    string-only scanner passes it. Required as an explicit case."""
    figures, key_names = guard_context
    figure = 9523 if 9523 in figures else sorted(figures)[0]
    source = "\n".join(["import x", "", f"ui.label(str({figure}))", ""])
    root = _seed_tree(tmp_path, {"web/components/discovery_panel.py": source})
    hits = [v for v in scan_launch_literals(root, figures, key_names)
            if v.figure == figure]
    assert hits and hits[0].line == 3
    assert hits[0].path == "web/components/discovery_panel.py"


@pytest.mark.parametrize("shape", ["sum", "difference", "product"])
def test_a_computed_constant_folding_to_a_figure_is_caught_in_two_shapes(
        shape, tmp_path, guard_context):
    """Round 8's case: `ui.label(f"{9_000 + 523:,}")` renders the live headline
    while NEITHER operand is a launch figure, so it walks through a scanner that
    reads every numeric constant but compares only the constants it SEES."""
    figures, key_names = guard_context
    figure = 9523 if 9523 in figures else sorted(figures)[0]
    left, right = _split_operands(figure, figures)
    if shape == "sum":
        expression = f"{left} + {right}"
        operands = (left, right)
    elif shape == "difference":
        expression = f"{figure + right} - {right}"
        operands = (figure + right, right)
    else:
        expression = f"{figure} * 3 - {figure} * 2"
        operands = (3, 2)
        assert figure in figures  # the literal IS present in this shape

    if shape != "product":
        for operand in operands:
            assert operand not in figures, (
                f"operand {operand} is itself a forbidden figure -- this control "
                "would pass for the wrong reason")

    snippet = 'ui.label(f"{' + expression + ':,}")'
    root = _seed_tree(tmp_path, {"web/pages/findings.py": snippet + "\n"})
    folded = [v for v in scan_launch_literals(root, figures, key_names)
              if v.figure == figure and v.form == "a computed constant"]
    assert folded, f"the {shape} shape folding to {figure} was not caught"
    message = folded[0].message()
    assert f"{figure:,}" in message, "the FOLDED value is not named"
    assert expression in message, "the SOURCE EXPRESSION is not named"


def test_the_folder_is_bounded_and_cannot_become_a_test_time_hazard(
        tmp_path, guard_context):
    """A folder that evaluates arbitrary expressions is a scanner nobody runs in
    CI. `**` is refused outright and a raising operation is skipped."""
    figures, key_names = guard_context
    source = "\n".join([
        "HUGE = 2 ** 9999999",
        "BOOM = 1 / 0",
        "ALSO = 7 // 0",
        "",
    ])
    root = _seed_tree(tmp_path, {"web/pages/findings.py": source})
    violations = scan_launch_literals(root, figures, key_names)
    assert violations == [], (
        "the bounded folder reported a violation for an unevaluable expression")
    assert fold_numeric(ast.parse("2 ** 9999999").body[0].value) is None
    assert fold_numeric(ast.parse("1 / 0").body[0].value) is None
    assert fold_numeric(ast.parse("9000 + 523").body[0].value) == 9523


# ===========================================================================
# Task 2: the THREE placement controls
# ===========================================================================

def test_placement_control_a_figure_in_a_FLOOR_module_is_reported(
        tmp_path, guard_context):
    figures, key_names = guard_context
    before = _tracked_fingerprint()
    figure = sorted(figures)[0]
    seeded, line = _copy_with_line("web/pages/findings.py", f"ui.label({figure})")
    root = _seed_tree(tmp_path, {"web/pages/findings.py": seeded})
    hits = [v for v in scan_launch_literals(root, figures, key_names)
            if v.figure == figure]
    assert hits, "a figure in a floor module was not reported"
    assert hits[0].path == "web/pages/findings.py" and hits[0].line == line
    assert _tracked_fingerprint() == before


def test_placement_control_a_module_SELECTED_from_the_independent_difference(
        tmp_path, guard_context):
    """The module is selected AT TEST TIME from `independent_set - floor` -- the
    difference taken over the TEST'S OWN glob expansion, which the equality
    assertion has already proved equal to the scanner's derived set.

    Two prior revisions of this control were unsound and are recorded so neither
    returns: naming a module exercised the FLOOR and proved nothing about the
    glob; selecting from the SCANNER's own difference proved the selected module
    was scanned but not that the set came from the globs, which a
    `floor + {one arbitrary module}` hardcode satisfies.
    """
    figures, key_names = guard_context
    before = _tracked_fingerprint()
    assert scanner_scanned_paths(_REPO_ROOT) == _independent_expansion()

    difference = sorted(_independent_expansion() - set(_FLOOR_MODULES))
    assert difference, (
        "every scanned module is in the floor -- the globs are then "
        "indistinguishable from a hand-written list")
    selected = difference[0]

    figure = sorted(figures)[0]
    seeded, line = _copy_with_line(selected, f"ui.label({figure})")
    root = _seed_tree(tmp_path, {selected: seeded})
    hits = [v for v in scan_launch_literals(root, figures, key_names)
            if v.figure == figure]
    assert hits, f"a figure in the glob-only module {selected} was not reported"
    assert hits[0].path == selected and hits[0].line == line
    assert selected not in _FLOOR_MODULES
    assert _tracked_fingerprint() == before


def test_placement_control_a_figure_in_a_TRANSLATION_string_is_reported(
        tmp_path, guard_context):
    """A Python-source-only scan passes this: a translated headline is a string,
    and a number baked into one is invisible to any review that greps `.py`
    files for digits."""
    figures, key_names = guard_context
    before = _tracked_fingerprint()
    figure = sorted(figures)[0]
    original = (_REPO_ROOT / _TRANSLATIONS_REL).read_text(encoding="utf-8")
    seeded = original + (
        '\nTRANSLATIONS.update({\n'
        f'    "{figure:,} identifications the finding aids did not have":\n'
        f'        "{figure:,} zihuyim",\n'
        '})\n'
    )
    root = _seed_tree(tmp_path, {_TRANSLATIONS_REL: seeded})
    hits = [v for v in scan_launch_literals(root, figures, key_names)
            if v.figure == figure]
    assert hits, "a figure inside a translation string was not reported"
    assert hits[0].path == _TRANSLATIONS_REL
    assert hits[0].line >= len(original.splitlines())
    assert _tracked_fingerprint() == before


# ===========================================================================
# Task 2: the failure MESSAGE, and the scanner's stated limit
# ===========================================================================

def test_the_failure_message_names_file_line_figure_and_the_accessor(
        tmp_path, guard_context):
    """A guard that fails with "assertion failed" gets deleted by the next
    person who trips it."""
    figures, key_names = guard_context
    figure = sorted(figures)[0]
    root = _seed_tree(tmp_path, {"web/pages/findings.py": f"\n\nui.label({figure})\n"})
    message = scan_launch_literals(root, figures, key_names)[0].message()
    assert "web/pages/findings.py" in message
    assert ":3" in message
    assert f"{figure:,}" in message
    assert "get_launch_stats_enveloped()" in message


def test_the_module_docstring_states_the_scanners_limit_and_names_its_pairing():
    """The broken implementation this catches is a future reader deleting
    136-18's sentinel test as redundant with this scanner, or narrowing this
    scanner because "the render test covers it"."""
    doc = sys.modules[__name__].__doc__
    assert "ACROSS STATEMENTS" in doc, "the assembled-across-statements case is not named"
    assert "136-18" in doc, "the mechanism that covers the limit is not named"
    assert "SENTINEL" in doc
    # BOTH directions of the pairing, so neither test can be deleted as
    # redundant with the other.
    assert "none of which any render test exercises" in doc
    assert "WHATEVER FORM IT TOOK" in doc
    assert "Do not delete either as redundant" in doc


# ===========================================================================
# Task 2: the EXEMPTION mechanism -- one rejection control per derived rule,
# plus an admissibility control, because a rule that refuses correct code is
# not a bound but a defect.
# ===========================================================================

def _current_values():
    return {int(entry["value"]) for entry in load_committed_figures().values()}


def _exemption_case(tmp_path, rel, snippet, figure, reason):
    seeded, line = _copy_with_line(rel, snippet)
    root = _seed_tree(tmp_path, {rel: seeded})
    return root, {"file": rel, "line": line, "figure": figure, "reason": reason}


def test_the_shipped_exemption_list_is_empty():
    assert LAUNCH_LITERAL_EXEMPTIONS == (), (
        "the expected state of the shipped exemption list is EMPTY -- an entry "
        "here needs its own reviewed justification")


def test_rule_1_refuses_a_CURRENT_figure_in_a_display_reachable_position(
        tmp_path, guard_context):
    """Round 8's case exactly: a hardcoded `total = <figure>` fallback in
    `web/discovery.py` -- a file, not a page -- silenced by an exemption because
    the old directory rule did not name it."""
    figures, key_names = guard_context
    figure = sorted(_current_values())[0]
    root, entry = _exemption_case(
        tmp_path, "web/discovery.py", f"total = {figure}", figure,
        "a legitimate-looking fallback that is in fact the defect")
    rejected = validate_exemptions([entry], root, figures, key_names, _current_values())
    assert 0 in rejected
    assert RULE_1 in rejected[0]
    assert RULE_2 in rejected[0]
    assert RULE_4 not in rejected[0], "this entry is outside web/pages and web/components"


def test_rule_2_refuses_any_display_reachable_position_even_for_a_retired_figure(
        tmp_path, guard_context):
    figures, key_names = guard_context
    retired = 13285
    assert retired not in _current_values()
    root, entry = _exemption_case(
        tmp_path, "shared/discovery_display_strings.py",
        'NOTE = f"{' + str(retired) + '} items"', retired,
        "a retired figure, but rendered")
    rejected = validate_exemptions([entry], root, figures, key_names, _current_values())
    assert 0 in rejected
    assert RULE_2 in rejected[0]
    assert RULE_1 not in rejected[0], "a retired figure is not a current one"


def test_rule_3_refuses_a_STALE_entry_whose_line_no_longer_carries_the_constant(
        tmp_path, guard_context):
    figures, key_names = guard_context
    figure = sorted(figures)[0]
    root, entry = _exemption_case(
        tmp_path, "web/discovery.py", f"_LIMIT = {figure}", figure,
        "a page size that happens to equal a launch figure")
    entry["line"] = entry["line"] - 1          # the constant has MOVED
    rejected = validate_exemptions([entry], root, figures, key_names, _current_values())
    assert 0 in rejected and RULE_3 in rejected[0]


def test_rule_4_keeps_the_directory_rule_as_a_FLOOR(tmp_path, guard_context):
    """A floor rather than the whole rule: this entry is rejected by rules 1-3
    NOWHERE, and only the directory rule refuses it."""
    figures, key_names = guard_context
    retired = 13285
    root, entry = _exemption_case(
        tmp_path, "web/pages/findings.py", f"_ROWS = {retired}", retired,
        "a retired figure at a non-display-reachable position, but on a PAGE")
    rejected = validate_exemptions([entry], root, figures, key_names, _current_values())
    assert 0 in rejected
    assert RULE_4 in rejected[0]
    assert RULE_2 not in rejected[0] and RULE_3 not in rejected[0]


def test_a_CURRENT_figure_at_a_NON_display_reachable_position_is_ADMISSIBLE(
        tmp_path, guard_context):
    """A rule that refuses correct code is not a bound but a defect.

    The figures move with every bake, and nothing stops one landing on an
    ordinary page-size or timeout constant already living in these modules. A
    blanket ban on current figures would then block CORRECT code with no
    available response -- which is a guard people delete rather than obey.
    POSITION is the property that actually decides whether a reader can see the
    number, and it is DERIVED by the scanner rather than asserted by whoever
    writes the entry.
    """
    figures, key_names = guard_context
    figure = sorted(_current_values())[0]
    root, entry = _exemption_case(
        tmp_path, "web/discovery.py", f"_SOME_INTERNAL_LIMIT = {figure}", figure,
        "the executor's queue bound; coincides with a launch figure only by "
        "accident of this bake, and reaches no reader")
    rejected = validate_exemptions([entry], root, figures, key_names, _current_values())
    assert rejected == {}, (
        f"a current figure at a non-display-reachable position was refused: "
        f"{rejected}")


def test_an_exemption_missing_a_written_reason_is_refused(tmp_path, guard_context):
    figures, key_names = guard_context
    figure = sorted(figures)[0]
    root, entry = _exemption_case(
        tmp_path, "web/discovery.py", f"_X = {figure}", figure, "placeholder")
    entry["reason"] = ""
    rejected = validate_exemptions([entry], root, figures, key_names, _current_values())
    assert 0 in rejected


# ===========================================================================
# Task 2: the FRESHNESS half -- proved to fail three ways
# ===========================================================================

def _resolvable_artifact(tmp_path, rows=_POPULATED_ROWS, audience="public"):
    """A synthetic artifact that passes the SAME audience and required-table
    checks the public loader applies -- so the freshness controls below run in
    every environment, artifact present or not."""
    return _build_launch_db(tmp_path / "resolvable.db", rows,
                            pages=_EXPECTED_PAGES, audience=audience)


def check_freshness(figure_file=None):
    """Compare the committed figures against a recomputation from the resolved
    artifact. Returns None when nothing resolves; raises AssertionError with the
    regeneration command named otherwise."""
    path, reason = resolve_guard_artifact()
    if path is None:
        if reason:
            raise AssertionError(
                f"{reason}. A resolution failure is never a silent green.")
        return None
    recomputed, _ = compute_launch_figures(path)
    committed = {key: entry["value"]
                 for key, entry in load_committed_figures(figure_file).items()}
    if committed != recomputed:
        differing = sorted(
            set(committed) ^ set(recomputed)
            | {k for k in set(committed) & set(recomputed)
               if committed[k] != recomputed[k]})
        raise AssertionError(
            f"the committed launch figures no longer match the artifact being "
            f"served (differing keys: {differing}). Regenerate with: "
            f"{_REGENERATE_COMMAND}")
    return path


def test_freshness_a_mutated_value_fails_naming_the_regeneration_command(
        tmp_path, monkeypatch):
    monkeypatch.setenv("DISCOVERY_LAUNCH_GUARD_DB", _resolvable_artifact(tmp_path))
    figures, _ = compute_launch_figures(os.environ["DISCOVERY_LAUNCH_GUARD_DB"])
    good = tmp_path / "good.json"
    good.write_text(json.dumps({"figures": {
        key: {"value": value, "sidecar_version": "x", "audience": "public",
              "content_hash": "x"} for key, value in figures.items()}}),
        encoding="utf-8")
    assert check_freshness(good) is not None

    payload = json.loads(good.read_text(encoding="utf-8"))
    payload["figures"]["total"]["value"] += 1
    mutated = tmp_path / "mutated.json"
    mutated.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(AssertionError) as excinfo:
        check_freshness(mutated)
    assert "regenerate" in str(excinfo.value).lower()
    assert "DISCOVERY_LAUNCH_GUARD_DB" in str(excinfo.value)
    assert "total" in str(excinfo.value)


def test_freshness_a_nonexistent_guard_db_is_a_NAMED_failure_not_a_pass(monkeypatch):
    monkeypatch.setenv("DISCOVERY_LAUNCH_GUARD_DB", "/no/such/artifact.db")
    with pytest.raises(AssertionError) as excinfo:
        check_freshness()
    assert "not a file" in str(excinfo.value)
    assert "never a silent green" in str(excinfo.value)


@pytest.mark.parametrize("defect", ["audience", "tables"])
def test_freshness_an_artifact_failing_the_loader_gates_is_a_NAMED_failure(
        defect, tmp_path, monkeypatch):
    if defect == "audience":
        db = _resolvable_artifact(tmp_path, audience="private")
        expected = "audience"
    else:
        db = _resolvable_artifact(tmp_path)
        conn = sqlite3.connect(db)
        conn.execute("DROP TABLE manuscript_display")
        conn.commit()
        conn.close()
        expected = "missing required table"
    monkeypatch.setenv("DISCOVERY_LAUNCH_GUARD_DB", db)
    with pytest.raises(AssertionError) as excinfo:
        check_freshness()
    assert expected in str(excinfo.value)


def test_freshness_against_the_artifact_actually_being_served():
    """The runtime half. On a machine where nothing resolves this cannot compare
    anything -- the three controls above are the ones that hold everywhere -- so
    it says so loudly rather than passing."""
    path, reason = resolve_guard_artifact()
    if path is None and reason is None:
        pytest.skip(_NO_ARTIFACT_HINT)
    assert check_freshness() is not None


# ===========================================================================
# Task 2: THE GUARD ITSELF
# ===========================================================================

def test_no_launch_figure_is_a_literal_anywhere_in_the_repository(guard_context):
    """The gate. Every launch number must be read through
    `web.discovery.get_launch_stats_enveloped()`."""
    figures, key_names = guard_context
    violations = scan_launch_literals(_REPO_ROOT, figures, key_names)
    exempted = {(e["file"], e["line"], e["figure"])
                for e in LAUNCH_LITERAL_EXEMPTIONS}
    unexplained = [v for v in violations
                   if (v.path, v.line, v.figure) not in exempted]
    assert not unexplained, "\n".join(v.message() for v in unexplained)


def test_every_shipped_exemption_would_be_admissible():
    figures = forbidden_figures()
    envelope_names = {"total", "identification_count", "manuscript_count"}
    rejected = validate_exemptions(
        LAUNCH_LITERAL_EXEMPTIONS, _REPO_ROOT, figures, envelope_names,
        _current_values())
    assert rejected == {}, f"inadmissible shipped exemption(s): {rejected}"
