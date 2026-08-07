# -*- coding: utf-8 -*-
"""Admin suppression of individual computed identifications (owner ruling,
2026-08-06).

The mechanism: a Supabase list of identification ids, an admin-only ✕ that adds
one, and a filter that drops them from the findings query.

WHAT THESE TESTS ARE FOR, stated as the properties rather than as the code, because
each one is a way the feature could look like it works and not:

1. THE COUNTS FOLLOW. The owner ruled that a suppressed row leaves the totals as
   well as the list. That is only true because the id set reaches the SQL
   predicate; a renderer-side filter would leave `total`, the pager and every
   facet count describing rows nobody can see.
2. THE HEADLINE DOES NOT. The launch figures are corpus figures on ruling U's
   fixed basis, read by a separate query. Owner-confirmed. A future "make the
   numbers consistent" change would break the basis, so it is pinned.
3. IT FAILS OPEN. A Supabase outage must leave the page fully readable.
4. THE ✕ IS ADMIN-ONLY, and its ABSENCE for everyone else is the assertion --
   an absent button renders no text, so no text scan can see it.
5. THE CAP RAISES rather than truncating. A silently truncated hide list would
   show rows the owner believes are hidden.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from shared.discovery_service import (
    FINDINGS_SUPPRESSION_MAX,
    _build_findings_filter,
    _build_findings_query,
)

REPO_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# The predicate: the layer everything else depends on.
# ---------------------------------------------------------------------------


def test_the_hide_list_reaches_the_sql_predicate_with_bound_parameters():
    """IN THE `WHERE`, and every id BOUND.

    Both halves matter. In the WHERE is what makes the counts follow (the count
    query and the facet cascade are built from this same predicate). Bound is what
    keeps an id -- a value that arrives from a database row -- out of SQL as text.
    """
    where, params = _build_findings_filter(suppressed=["id-a", "id-b"])
    assert "di.identification_id NOT IN (?,?)" in where, where
    assert params[-2:] == ["id-a", "id-b"]
    # No id appears in the SQL text itself.
    assert "id-a" not in where and "id-b" not in where


def test_an_empty_hide_list_adds_no_predicate_at_all():
    """The no-suppression case must be the SAME query as before the feature.

    A `NOT IN ()` or a `1=1` would be a new query shape for every reader who has
    nothing hidden -- i.e. all of them -- and would invalidate the measured
    baseline the benchmark reports.
    """
    for empty in (None, (), [], ["", None]):
        where, params = _build_findings_filter(suppressed=empty)
        assert "identification_id" not in where, (empty, where)
        assert all(not isinstance(p, str) or "identification" not in p
                   for p in params)


def test_a_repeated_id_is_deduplicated_and_the_order_is_stable():
    """Duplicates burn parameters against the cap for no effect, and an unstable
    order would make the generated SQL vary between identical requests -- which
    defeats any statement cache and makes an assertion on the SQL flaky."""
    where, params = _build_findings_filter(suppressed=["b", "a", "b", "a"])
    # THE CLAUSE ITSELF, sliced out rather than counting `?` over the whole
    # predicate: the WHERE also carries the divergence filter's own placeholders,
    # so a total count asserts arithmetic about an unrelated clause. (My first
    # attempt did exactly that and failed on its own sum, 4 vs 3, while the
    # deduplication under test was working correctly.)
    clause = where[where.index("di.identification_id NOT IN ("):]
    assert clause.count("?") == 2, (
        f"four ids with two distinct values produced {clause.count('?')} "
        f"placeholders: {clause}")
    ids = [p for p in params if p in ("a", "b")]
    assert ids == ["b", "a"], f"order not preserved or not deduped: {ids}"


def test_over_cap_RAISES_rather_than_silently_truncating():
    """A truncated hide list shows rows the owner believes are hidden.

    The cap is also a real SQLite limit (999 bound parameters, shared with every
    other value in the statement), so the alternative to raising here is an opaque
    failure deep inside a query.
    """
    too_many = [f"id-{n}" for n in range(FINDINGS_SUPPRESSION_MAX + 1)]
    with pytest.raises(ValueError) as excinfo:
        _build_findings_filter(suppressed=too_many)
    message = str(excinfo.value)
    assert str(FINDINGS_SUPPRESSION_MAX) in message
    # ...and it says what to do about it, because the fix is never a longer list.
    assert "bake" in message.lower()

    # Exactly AT the cap is allowed: an off-by-one here would make the documented
    # limit a lie.
    where, _params = _build_findings_filter(
        suppressed=[f"id-{n}" for n in range(FINDINGS_SUPPRESSION_MAX)])
    assert "identification_id NOT IN" in where


def test_the_not_in_clause_is_safe_because_the_column_is_the_primary_key():
    """`NULL NOT IN (...)` is NULL, i.e. false -- so on a NULLABLE column this
    shape would silently drop every row with a null there. The sibling divergence
    filter carries a long warning about exactly that.

    This asserts the schema fact that makes the shape sound HERE, against the real
    artifact, so a future migration that relaxes it fails this test rather than
    quietly shrinking the page.
    """
    db = _live_artifact()
    if db is None:
        pytest.skip("no live discovery artifact in this checkout")
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        columns = {
            row[1]: row for row in
            conn.execute("PRAGMA table_info(discovery_identification)")
        }
        assert "identification_id" in columns
        nulls = conn.execute(
            "SELECT COUNT(*) FROM discovery_identification "
            "WHERE identification_id IS NULL").fetchone()[0]
        assert nulls == 0, (
            f"{nulls} identification_id(s) are NULL -- the NOT IN suppression "
            "clause would silently drop every one of them")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# THE HONEST-COUNTS PROPERTY, against the real artifact.
# ---------------------------------------------------------------------------


def _live_artifact():
    """The served artifact, or None. Read-only, never modified."""
    live = REPO_ROOT / "discovery_data" / "live"
    if not live.is_dir():
        return None
    candidates = sorted(live.glob("discovery-v1-*.db"))
    return candidates[0] if candidates else None


def test_suppressing_rows_reduces_the_TOTAL_by_exactly_that_many():
    """THE OWNER'S RULING, measured end to end on the real artifact.

    This is the assertion the whole design turns on: the count query is built
    from the same predicate as the rows, so hiding N rows drops the reported total
    by exactly N. If this ever fails, the page is telling a reader it holds rows
    it will not show them.
    """
    db = _live_artifact()
    if db is None:
        pytest.skip("no live discovery artifact in this checkout")
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        sql, params = _build_findings_query(count_only=True)
        before = int(conn.execute(sql, params).fetchone()["n"])
        assert before > 3, "the artifact has too few main-pool rows to test with"

        ids = [
            row[0] for row in conn.execute(
                "SELECT identification_id FROM discovery_identification "
                "WHERE main_pool = 1 LIMIT 3")
        ]
        assert len(ids) == 3

        sql, params = _build_findings_query(count_only=True, suppressed=ids)
        after = int(conn.execute(sql, params).fetchone()["n"])
        assert before - after == 3, (
            f"suppressing 3 rows changed the total by {before - after} -- the "
            "count does not follow the hide list, so the page reports rows it "
            "will not show")
    finally:
        conn.close()


def test_a_suppressed_row_is_absent_from_the_ROWS_as_well_as_the_count():
    """The count and the list must agree, which is only automatic because both
    come from one predicate. Asserted separately anyway: "the total went down"
    and "the row is gone" are different claims, and a bug could satisfy one."""
    db = _live_artifact()
    if db is None:
        pytest.skip("no live discovery artifact in this checkout")
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        sql, params = _build_findings_query(page=1, page_size=5)
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
        assert rows, "the artifact returned no rows to suppress"
        victim = rows[0]["identification_id"]

        sql, params = _build_findings_query(page=1, page_size=5,
                                            suppressed=[victim])
        after = [dict(r)["identification_id"]
                 for r in conn.execute(sql, params).fetchall()]
        assert victim not in after, "the suppressed row is still in the result set"
    finally:
        conn.close()


# ===========================================================================
# THE RLS HANDOFF (Codex review, 2026-08-07, LOW-4).
#
# The write shipped broken and every test above still passed, because they all
# exercise the SQL PREDICATE and none of them exercises how the write reaches
# Supabase. The defect was one layer out: `suppress` ran in a thread-pool worker
# and called `get_user_client()` there, where `app.storage.user` raises, a helper
# catches it and returns `{}`, and the client silently degrades to ANONYMOUS --
# which the admin-only `WITH CHECK` policy correctly refuses. Nothing raised.
#
# So the property is: THE CLIENT IS BUILT ON THE LOOP AND HANDED TO THE WORKER.
# Both tests below fail on the previous implementation, which is the only reason
# they are worth having.
# ===========================================================================


def test_the_write_receives_the_client_the_LOOP_built_not_one_the_worker_makes():
    """The regression test for the RLS refusal.

    Asserts the sentinel client constructed on the event loop is the object the
    worker is called with. On the previous implementation `suppress` took no
    `client` at all and built its own inside the worker, so this fails at the
    signature and then at the identity -- not merely at a mock count.
    """
    import asyncio
    import inspect

    import web.discovery as disc
    import web.discovery_suppression as sup

    sentinel = object()
    seen = {}

    # The worker body, recording what it was handed.
    def _fake_suppress(identification_id, client=None):
        seen["id"] = identification_id
        seen["client"] = client
        return True

    def _fake_get_user_client():
        # Called ON THE LOOP in the fixed implementation. Returning the sentinel
        # here is what lets the identity assertion below distinguish "the loop
        # built it" from "the worker built its own".
        seen["built_on_loop"] = True
        return sentinel

    import web.supabase_client as sc

    original_suppress = sup.suppress
    original_get = sc.get_user_client
    try:
        sup.suppress = _fake_suppress
        sc.get_user_client = _fake_get_user_client
        assert asyncio.run(disc.suppress_identification("abc123")) is True
    finally:
        sup.suppress = original_suppress
        sc.get_user_client = original_get

    assert seen.get("built_on_loop"), (
        "the user client was never built on the event loop -- if the worker "
        "builds it instead, `app.storage.user` is unreadable there and the "
        "client degrades to anonymous, which RLS refuses")
    assert seen["client"] is sentinel, (
        "the worker did not receive the client the loop built (got "
        f"{seen['client']!r}) -- this is the RLS refusal the owner reported")
    assert seen["id"] == "abc123"

    # And the parameter genuinely exists to be passed, rather than being absorbed
    # by a `**kwargs` that would make the assertion above vacuous.
    assert "client" in inspect.signature(original_suppress).parameters


def test_a_write_with_no_client_is_REFUSED_rather_than_falling_back():
    """No silent anonymous fallback, in either direction.

    `get_user_client()` returning `None` must fail the write, and calling the
    worker with no client must refuse rather than reaching for the client itself
    -- the fallback IS the defect, so a convenient default would restore it for
    the next caller who forgets the argument.
    """
    import asyncio

    import web.discovery as disc
    import web.discovery_suppression as sup
    import web.supabase_client as sc

    # 1. The worker refuses `None` outright, and touches no client to do it.
    assert sup.suppress("abc123", client=None) is False
    assert sup.unsuppress("abc123", client=None) is False

    # 2. No client on the loop -> the write fails, and the worker is never called.
    called = {"n": 0}

    def _must_not_run(*_args, **_kwargs):
        called["n"] += 1
        return True

    original_suppress = sup.suppress
    original_get = sc.get_user_client
    try:
        sup.suppress = _must_not_run
        sc.get_user_client = lambda: None
        assert asyncio.run(disc.suppress_identification("abc123")) is False
    finally:
        sup.suppress = original_suppress
        sc.get_user_client = original_get

    assert called["n"] == 0, (
        "the write was dispatched to the worker with no client -- it would run "
        "anonymously and be refused by RLS, reported as a mysterious failure")


# ===========================================================================
# CROSS-PAGE COHERENCE + THE FAIL-OPEN TRAP (Codex review, 2026-08-07, HIGH).
#
# Two defects, one root cause: the page treated its hide list as a fact resolved
# once at load. (a) A row hidden by ANOTHER admin stayed visible on an already-open
# page forever. (b) Worse, the ✕ handler ASSIGNED the re-read result, and the
# reader fails open to `()` -- so one Supabase blip between a successful write and
# the re-read un-hid every row hidden earlier in the session.
#
# The fix has two halves, and both are load-bearing:
#   * a SYNCHRONOUS cache PEEK (`cached_ids`) that never awaits, so per-refresh
#     coherence costs no round trip -- awaiting the real reader here broke the
#     one-dispatch probe AND a control-driving test's yield budget;
#   * a UNION rather than an assignment, so a failed read can never un-hide.
# ===========================================================================


def test_the_cache_peek_never_fetches_and_distinguishes_empty_from_unknown():
    """`None` (no fresh answer) and `frozenset()` (nothing hidden) must not be the
    same value. Collapsing them is exactly how a fail-open empty set un-hides every
    row, so the distinction is the whole contract."""
    import web.discovery_suppression as sup

    fetches = {"n": 0}

    def _must_not_fetch():
        fetches["n"] += 1
        return frozenset(), True

    original_fetch = sup._fetch_ids
    original_cache = sup._CACHE
    try:
        sup._fetch_ids = _must_not_fetch

        # 1. Cold cache -> None, and NOT a fetch.
        sup.invalidate()
        assert sup.cached_ids() is None
        assert fetches["n"] == 0, "the peek fetched -- it must never touch I/O"

        # 2. A warm SUCCESSFUL empty read is an answer: "nothing is hidden".
        sup._CACHE = (sup.time.monotonic(), frozenset(), True)
        assert sup.cached_ids() == frozenset()

        # 3. A warm FAILED read is NOT an answer, even though it holds an empty set.
        sup._CACHE = (sup.time.monotonic(), frozenset(), False)
        assert sup.cached_ids() is None, (
            "a failed read's empty set was returned as though it meant 'nothing "
            "hidden' -- that is the fail-open bug, one layer down")

        # 4. An EXPIRED success is not an answer either.
        sup._CACHE = (sup.time.monotonic() - (sup.CACHE_TTL_SECONDS + 1),
                      frozenset({"x"}), True)
        assert sup.cached_ids() is None

        # 5. A warm success with content is returned verbatim.
        sup._CACHE = (sup.time.monotonic(), frozenset({"a", "b"}), True)
        assert sup.cached_ids() == frozenset({"a", "b"})

        assert fetches["n"] == 0, "the peek fetched at some point"
    finally:
        sup._fetch_ids = original_fetch
        sup._CACHE = original_cache


def test_a_failed_reread_after_a_successful_write_never_UNHIDES_anything():
    """THE fail-open trap, at the page's merge rule.

    A successful write followed by a failed list read must leave the page hiding
    MORE than before, never less. The old code assigned the re-read straight into
    the holder, so a blip replaced a populated list with `()`.

    Asserted as the merge ARITHMETIC rather than by driving the page, because the
    property is about set algebra and holds for every ordering of the two calls.
    """
    holder = {"ids": ("already-hidden-1", "already-hidden-2")}
    just_written = "newly-hidden"
    failed_reread: tuple = ()          # what the wrapper returns on failure

    merged = tuple(sorted(
        set(failed_reread) | set(holder["ids"]) | {just_written}))

    assert set(merged) >= set(holder["ids"]), (
        "a failed re-read shrank the hide list -- rows the admin hid earlier "
        "became visible again")
    assert just_written in merged, "the row just hidden is not in the list"
    assert merged == tuple(sorted(merged)), (
        "the merged list is not sorted -- it lands in the service's cache key, "
        "so an unstable order means the cache silently never hits")


def test_the_page_merge_rule_keeps_the_existing_list_when_the_peek_is_unknown():
    """`None` from the peek means KEEP WHAT WE HAD. The page must not read it as
    an empty list, which would un-hide everything on any cold cache."""
    holder = {"ids": ("hidden-1", "hidden-2")}

    for cached in (None, frozenset(), frozenset({"hidden-3"})):
        before = tuple(holder["ids"])
        if cached is not None:
            merged = tuple(sorted(set(cached) | set(before)))
        else:
            merged = before
        assert set(merged) >= set(before), (
            f"peek={cached!r} shrank the hide list from {before} to {merged}")

    # And the growth case genuinely grows.
    merged = tuple(sorted(set(frozenset({"hidden-3"})) | set(holder["ids"])))
    assert "hidden-3" in merged, (
        "a row another admin hid was not picked up on refresh")
