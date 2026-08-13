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
        pytest.skip(_LIVE_ARTIFACT_SKIP)
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


#: Why the live-artifact tests did not run, when they did not. Set by
#: `_live_artifact`; read by the skip messages so "did not run" always says
#: WHICH reason, and a stale artifact never reads as an absent one.
_LIVE_ARTIFACT_SKIP = "no live discovery artifact in this checkout"

#: Columns the CURRENT read paths select, which a pre-CD-batch artifact lacks.
#: Checked because these tests run the real `_build_findings_query` against
#: whatever happens to be staged locally.
_REQUIRED_LIVE_COLUMNS = {"rendered_relation"}


def _live_artifact():
    """The served artifact, or None. Read-only, never modified.

    Also refuses an artifact whose SCHEMA predates the columns the queries under
    test select. Existence was the only check until 2026-08-12, and once the
    C-track read paths began selecting `di.rendered_relation` unconditionally, a
    checkout serving a pre-batch artifact got `sqlite3.OperationalError: no such
    column` raised from inside a test body -- a red suite that says nothing about
    the code and everything about which file happens to sit in
    `discovery_data/live`.

    The same treatment the real-artifact expansion probe got in step 3d: check
    for the COLUMN, not merely the tables, and skip with a reason that NAMES the
    staleness instead of raising. A pre-batch artifact is refused by the runtime
    loader too (`web/discovery_assets.py::_REQUIRED_COLUMNS`), so skipping here
    agrees with what production would do rather than papering over it.
    """
    global _LIVE_ARTIFACT_SKIP
    live = REPO_ROOT / "discovery_data" / "live"
    if not live.is_dir():
        _LIVE_ARTIFACT_SKIP = "no live discovery artifact in this checkout"
        return None
    candidates = sorted(live.glob("discovery-v1-*.db"))
    if not candidates:
        _LIVE_ARTIFACT_SKIP = "no live discovery artifact in this checkout"
        return None
    db = candidates[0]
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    try:
        present = {r[1] for r in conn.execute(
            "PRAGMA table_info(discovery_identification)")}
    finally:
        conn.close()
    missing = _REQUIRED_LIVE_COLUMNS - present
    if missing:
        _LIVE_ARTIFACT_SKIP = (
            f"the staged live artifact ({db.name}) predates the 2026-08-12 CD "
            f"batch -- missing discovery_identification.{', '.join(sorted(missing))}. "
            "Re-stage a current asset to exercise these tests.")
        return None
    _LIVE_ARTIFACT_SKIP = "no live discovery artifact in this checkout"
    return db


def test_suppressing_rows_reduces_the_TOTAL_by_exactly_that_many():
    """THE OWNER'S RULING, measured end to end on the real artifact.

    This is the assertion the whole design turns on: the count query is built
    from the same predicate as the rows, so hiding N rows drops the reported total
    by exactly N. If this ever fails, the page is telling a reader it holds rows
    it will not show them.
    """
    db = _live_artifact()
    if db is None:
        pytest.skip(_LIVE_ARTIFACT_SKIP)
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
        pytest.skip(_LIVE_ARTIFACT_SKIP)
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


def test_the_cache_CONVERGES_after_expiry_via_the_rewarm_predicate():
    """THE finding Codex's re-review caught: a peek alone never converges.

    `cached_ids` never fetches, and the cache is warmed only by a page load and by a
    local write -- so once an entry expires, a long-open page's peek returns `None`
    forever and the page keeps a list that can no longer change. The peek bought "no
    added latency"; it did NOT buy coherence.

    `cache_needs_refresh()` is the other half, and this drives the real state machine
    through the actual sequence rather than restating an expression:
    fresh -> expired -> re-warmed, with the re-warm seeing a CHANGED list.
    """
    import web.discovery_suppression as sup

    remote = {"ids": frozenset({"hidden-1"})}
    fetches = {"n": 0}

    def _fetch():
        fetches["n"] += 1
        return remote["ids"], True

    original_fetch = sup._fetch_ids
    original_cache = sup._CACHE
    try:
        sup._fetch_ids = _fetch
        sup.invalidate()

        # 1. Cold: no answer, and a re-warm IS wanted.
        assert sup.cached_ids() is None
        assert sup.cache_needs_refresh() is True
        assert fetches["n"] == 0, "the peek/predicate fetched -- neither may"

        # 2. A real read warms it. Now the peek answers and no re-warm is wanted.
        assert sup.suppressed_ids() == frozenset({"hidden-1"})
        assert fetches["n"] == 1
        assert sup.cached_ids() == frozenset({"hidden-1"})
        assert sup.cache_needs_refresh() is False, (
            "a fresh cache still asked to be re-warmed -- that would fetch on "
            "every interaction")

        # 3. ANOTHER admin (another process) hides a row. This process cannot know:
        #    its own cache is untouched, so the peek keeps the OLD answer.
        remote["ids"] = frozenset({"hidden-1", "hidden-by-someone-else"})
        assert sup.cached_ids() == frozenset({"hidden-1"}), (
            "the peek fetched -- it must answer from cache only")

        # 4. TIME PASSES past the TTL. THIS is the state the old design got stuck
        #    in: no answer, and (before the fix) nothing that would ever re-fetch.
        cached = sup._CACHE
        sup._CACHE = (cached[0] - (sup.CACHE_TTL_SECONDS + 1), cached[1], cached[2])
        assert sup.cached_ids() is None
        assert sup.cache_needs_refresh() is True, (
            "an EXPIRED cache did not ask to be re-warmed -- the page would keep "
            "its stale list indefinitely, which is exactly the reported defect")

        # 5. The re-warm runs and the new id is now visible to this process.
        sup.suppressed_ids()
        assert sup.cached_ids() == frozenset({"hidden-1", "hidden-by-someone-else"}), (
            "the re-warm did not pick up the row another admin hid")
        assert fetches["n"] == 2
    finally:
        sup._fetch_ids = original_fetch
        sup._CACHE = original_cache


def test_a_FRESH_FAILURE_does_not_ask_to_be_rewarmed():
    """The bound on the re-warm, and the reason it is a separate predicate from the
    peek rather than `cached_ids() is None`.

    A cached failure means "we tried moments ago and could not tell". Re-dispatching
    on it would turn a Supabase outage into a fetch on every interaction -- what
    `FAILURE_TTL_SECONDS` exists to prevent -- and would break the findings page's
    one-dispatch-per-read guard in CI, where no credentials are configured and the
    first read always caches a failure.
    """
    import web.discovery_suppression as sup

    original_cache = sup._CACHE
    try:
        # A fresh FAILURE: no usable answer, but no re-warm either.
        sup._CACHE = (sup.time.monotonic(), frozenset(), False)
        assert sup.cached_ids() is None, (
            "a failed read's empty set was returned as though it meant 'nothing "
            "hidden' -- that is the fail-open bug one layer down")
        assert sup.cache_needs_refresh() is False, (
            "a FRESH failure asked to be re-warmed -- during an outage that is a "
            "fetch per interaction")

        # Once the SHORTER failure TTL lapses, retrying is right again.
        sup._CACHE = (sup.time.monotonic() - (sup.FAILURE_TTL_SECONDS + 1),
                      frozenset(), False)
        assert sup.cache_needs_refresh() is True, (
            "an expired failure never retries -- the hide list would stay "
            "unapplied for the life of the process")
    finally:
        sup._CACHE = original_cache


def test_a_failed_reread_after_a_successful_write_never_UNHIDES_anything():
    """THE fail-open trap, through the page's REAL ✕ handler.

    A successful write followed by a failed list read must leave the page hiding
    MORE than before, never less. The old code assigned the re-read straight into the
    holder, and the reader fails open to `()`, so a blip replaced a populated list
    with nothing -- immediately after a click whose purpose was to hide one more row.

    DRIVEN THROUGH `_render_row`'s ACTUAL HANDLER. Two earlier revisions of this test
    recomputed the page's union expression in test code, which Codex's third pass
    correctly called tautological: it would pass against the direct-assignment bug,
    because the assertion never touched the production rule. This one extracts the
    real `on_suppress` closure the page builds and calls it.
    """
    import asyncio

    import web.pages.findings as fp

    holder = {"ids": ("already-hidden-1", "already-hidden-2")}
    captured = {}
    refreshed = {"n": 0}

    async def _refresh():
        refreshed["n"] += 1

    class _Rows:
        """Captures the `on_suppress` the page wires onto the row."""

        @staticmethod
        def render_finding_row(_item, _lang, **kwargs):
            captured["on_suppress"] = kwargs.get("on_suppress")

        def __getattr__(self, name):
            return getattr(fp.rows, name)

    original_rows = fp.rows
    original_admin = fp._viewer_is_admin
    original_write = fp.suppress_identification
    original_read = fp.suppressed_identification_ids
    try:
        fp.rows = _Rows()
        fp._viewer_is_admin = lambda: True

        # The write SUCCEEDS...
        async def _write_ok(_id):
            return True

        # ...and THEN the list read FAILS, returning the fail-open empty tuple.
        async def _read_fails_open():
            return ()

        fp.suppress_identification = _write_ok
        fp.suppressed_identification_ids = _read_fails_open

        fp._render_row({"unit": "identification",
                        "identification_id": "newly-hidden",
                        "canonical_work_id": "w000001",
                        "display_work_id": "w000001",
                        "neutral_title": "T", "sys_id": "1", "main_pool": 1},
                       "en", refresh=_refresh, hidden=holder)

        handler = captured.get("on_suppress")
        assert handler is not None, (
            "the page wired no ✕ handler for an admin -- fixture error")
        asyncio.run(handler("newly-hidden"))
    finally:
        fp.rows = original_rows
        fp._viewer_is_admin = original_admin
        fp.suppress_identification = original_write
        fp.suppressed_identification_ids = original_read

    assert refreshed["n"] == 1, "a successful hide did not re-render the page"
    assert set(holder["ids"]) >= {"already-hidden-1", "already-hidden-2"}, (
        "a failed re-read after a SUCCESSFUL write shrank the hide list -- rows "
        f"the admin hid earlier became visible again (holder is {holder['ids']})")
    assert "newly-hidden" in holder["ids"], (
        "the row just hidden is not in the page's list, so the next query will "
        "show it again")
    assert holder["ids"] == tuple(sorted(holder["ids"])), (
        "the merged list is not sorted -- it lands in the service's cache key, so "
        "an unstable order means the cache silently never hits")


# ===========================================================================
# THE MANUSCRIPT FILTER AXIS (owner report, 2026-08-07: "In One Row Per Manuscript
# I don't see the computed identifications at all").
#
# The per-manuscript unit had no expansion because the shared predicate had no
# `sys_id` axis, and offering one would have passed a keyword the read silently
# drops -- opening the row onto a page of the CORPUS rather than the manuscript.
# These tests are about the axis itself, against the real artifact.
# ===========================================================================


def test_the_manuscript_axis_pins_children_to_exactly_one_manuscript():
    """The property the expansion's honesty rests on: a parent's child list is the
    identifications IN that manuscript, and its count agrees with the parent's own
    `work_count`.

    Run against the LIVE artifact, because "the SQL looks right" is what the
    silently-dropped keyword also looked like.
    """
    from shared.discovery_service import (
        FINDINGS_UNIT_IDENTIFICATION,
        FINDINGS_UNIT_MANUSCRIPT,
    )

    db = _live_artifact()
    if db is None:
        pytest.skip(_LIVE_ARTIFACT_SKIP)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        sql, params = _build_findings_query(
            unit=FINDINGS_UNIT_MANUSCRIPT, page=1, page_size=5)
        parents = [dict(r) for r in conn.execute(sql, params).fetchall()]
        assert parents, "the artifact returned no manuscript rows"

        for parent in parents:
            sys_id = parent["sys_id"]
            sql, params = _build_findings_query(
                unit=FINDINGS_UNIT_IDENTIFICATION, sys_id=sys_id,
                page=1, page_size=200)
            children = [dict(r) for r in conn.execute(sql, params).fetchall()]
            assert children, f"manuscript {sys_id} opened onto nothing"
            # EVERY child is in THIS manuscript. A dropped keyword would return a
            # page of the corpus, which is the defect this axis was withheld over.
            assert {c["sys_id"] for c in children} == {sys_id}, (
                "the manuscript expansion returned identifications from other "
                "manuscripts -- the axis is not being applied")
            # ...and the parent's own count is the number underneath it, because
            # both come from ONE predicate.
            assert int(children[0]["_total_rows"]) == int(parent["work_count"]), (
                f"manuscript {sys_id} says {parent['work_count']} works but opens "
                f"onto {children[0]['_total_rows']} identifications")
    finally:
        conn.close()


def test_the_manuscript_axis_ACTUALLY_NARROWS_and_is_not_silently_dropped():
    """The control for the test above, and the reason it is worth having: an
    IGNORED `sys_id` produces a passing-looking child list too -- it is just the
    whole corpus. So the pinned total must be strictly smaller than the unpinned
    one, which no dropped keyword can satisfy."""
    from shared.discovery_service import FINDINGS_UNIT_IDENTIFICATION

    db = _live_artifact()
    if db is None:
        pytest.skip(_LIVE_ARTIFACT_SKIP)
    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    try:
        sql, params = _build_findings_query(
            unit=FINDINGS_UNIT_IDENTIFICATION, count_only=True)
        unpinned = int(conn.execute(sql, params).fetchone()["n"])

        sql, params = _build_findings_query(
            unit=FINDINGS_UNIT_IDENTIFICATION, page=1, page_size=1)
        row = [dict(r) for r in conn.execute(sql, params).fetchall()][0]

        sql, params = _build_findings_query(
            unit=FINDINGS_UNIT_IDENTIFICATION, sys_id=row["sys_id"],
            count_only=True)
        pinned = int(conn.execute(sql, params).fetchone()["n"])

        assert 0 < pinned < unpinned, (
            f"pinning one manuscript gave {pinned} of {unpinned} rows -- a "
            "predicate that narrows nothing is a keyword being dropped")
        # And the SQL really binds it, rather than interpolating.
        where, bound = _build_findings_filter(sys_id=row["sys_id"])
        assert "di.sys_id = ?" in where, "the manuscript axis is not a bound predicate"
        assert row["sys_id"] in bound
    finally:
        conn.close()
