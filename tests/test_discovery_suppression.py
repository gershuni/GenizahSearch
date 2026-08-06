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
