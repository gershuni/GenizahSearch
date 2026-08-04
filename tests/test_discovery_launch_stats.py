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

Masking discipline: every fixture here is fabricated in-test through
`scripts.build_discovery_sidecar.create_schema`; nothing names a corpus.
"""
from __future__ import annotations

import asyncio
import copy
import json
import os
import sqlite3
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
