# -*- coding: utf-8 -*-
"""The ONE eligibility clause (Codex pre-flight finding 3).

Four read paths used to decide "which rows may a reader see" independently, and
disagreed three ways: D-13g in the claims query, pre-D-13g shipped-only in the
three shared_text queries, and NO predicate at all in the work-expansion CTE.
These tests pin the unified builder, pin that the three shared_text sites now
carry the D-13g rule, and pin that the expansion CTE's divergence is EXPLICIT
and defaults to unchanged behaviour — because the change there is an unmade
owner decision, and a test that quietly encoded either answer would decide it.

Masking discipline: everything here is either generated SQL text or rows
fabricated in-test through `create_schema`. No real research data.
"""
from __future__ import annotations

import sqlite3

import pytest

from scripts import build_discovery_sidecar as sidecar_build
from shared import discovery_service as svc


# ---------------------------------------------------------------------------
# The builder itself.
# ---------------------------------------------------------------------------

def test_default_mode_is_the_d13g_rule():
    """shipped OR human-confirmed — the rule the BUILD materializes into
    `discovery_identification.eligibility_basis`. A read path that used
    shipped-only would drop the restored rows a second time, one layer down."""
    clause = svc.eligibility_clause(svc.ELIGIBILITY_DEFAULT)
    assert clause == (
        "AND (routing_status = 'shipped' "
        "OR adjudication_status = 'human_confirmed')"
    )


def test_prefix_is_applied_to_every_column_not_just_the_first():
    """The bug this forecloses: prefixing only `routing_status` yields SQL that
    parses, runs, and silently resolves `adjudication_status` against whatever
    other table is in scope."""
    clause = svc.eligibility_clause(svc.ELIGIBILITY_DEFAULT, "de.")
    assert clause == (
        "AND (de.routing_status = 'shipped' "
        "OR de.adjudication_status = 'human_confirmed')"
    )
    assert clause.count("de.") == 2


def test_shipped_only_is_nameable_but_is_not_the_default():
    assert svc.eligibility_clause(svc.ELIGIBILITY_SHIPPED_ONLY) == (
        "AND routing_status = 'shipped'"
    )
    assert svc.ELIGIBILITY_DEFAULT != svc.ELIGIBILITY_SHIPPED_ONLY


def test_all_mode_emits_no_predicate():
    assert svc.eligibility_clause(svc.ELIGIBILITY_ALL) == ""
    assert svc.eligibility_clause(svc.ELIGIBILITY_ALL, "de.") == ""


@pytest.mark.parametrize("bad", ["", "shipped", "SHIPPED_ONLY", "review", None, True])
def test_an_unknown_mode_raises_instead_of_defaulting(bad):
    """Falling back to a weaker predicate on a typo is how review_only rows
    reach a reader. Loud beats lenient here."""
    with pytest.raises(ValueError, match="unknown mode"):
        svc.eligibility_clause(bad)


def test_the_review_toggle_has_exactly_one_definition():
    assert svc._review_toggle_mode(False) == svc.ELIGIBILITY_DEFAULT
    assert svc._review_toggle_mode(True) == svc.ELIGIBILITY_ALL


def test_the_claims_clause_is_the_builders_output_not_a_second_literal():
    """`_CLAIMS_DEFAULT_ROUTING_CLAUSE` predates the builder. It must now BE the
    builder's output — otherwise there are two spellings of D-13g again, which
    is the whole defect."""
    assert svc._CLAIMS_DEFAULT_ROUTING_CLAUSE == svc.eligibility_clause(
        svc.ELIGIBILITY_DEFAULT, "de."
    )


def test_no_shipped_only_literal_survives_in_the_read_paths():
    """Drift guard over the source: the three sites Codex found each built
    `"AND routing_status = 'shipped'"` inline. Any reappearance means a fourth
    independent decision about eligibility."""
    import inspect

    src = inspect.getsource(svc)
    assert '"AND routing_status = \'shipped\'"' not in src
    # The only place the literal may be assembled is the builder.
    assert src.count("AND {prefix}routing_status = 'shipped'") == 1


# ---------------------------------------------------------------------------
# Behaviour: the shared_text sites now restore human-confirmed rows.
# ---------------------------------------------------------------------------

def _asset_with_one_human_confirmed_shared_text(tmp_path):
    """A minimal asset holding exactly two shared_text rows on one anchor: one
    shipped, one human-confirmed that routing demoted.

    The served asset has ZERO human-confirmed shared_text rows (measured), which
    is precisely why this case has to be fabricated: the fix is a no-op on
    today's data, so only a constructed row can show it works at all.
    """
    db = tmp_path / "eligibility.db"
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA foreign_keys = ON")
    sidecar_build.create_schema(conn)
    sidecar_build.populate_synthetic(conn, source_db_hash="eligibility-test")
    row = conn.execute(
        "SELECT * FROM discovery_evidence WHERE evidence_kind = 'shared_text' "
        "AND other_page_id IS NOT NULL LIMIT 1"
    ).fetchone()
    if row is None:
        conn.close()
        pytest.skip("synthetic fixture carries no two-sided shared_text row")
    cols = [d[0] for d in conn.execute(
        "SELECT * FROM discovery_evidence LIMIT 1").description]
    values = dict(zip(cols, row))
    anchor = values["a_page_id"]
    # Row 1: make the existing row unambiguously shipped.
    conn.execute(
        "UPDATE discovery_evidence SET routing_status = 'shipped', "
        "adjudication_status = 'unreviewed' WHERE evidence_id = ?",
        (values["evidence_id"],),
    )
    # Row 2: a clone that routing demoted but a human confirmed.
    clone = dict(values)
    clone["evidence_id"] = values["evidence_id"] + "__hc"
    clone["other_page_id"] = (values["other_page_id"] or "") + "__hc"
    clone["routing_status"] = "review_only"
    clone["routing_reason"] = "later_shared_text"
    clone["adjudication_status"] = "human_confirmed"
    conn.execute(
        "INSERT INTO discovery_evidence ({}) VALUES ({})".format(
            ", ".join(clone), ", ".join("?" * len(clone))
        ),
        tuple(clone.values()),
    )
    conn.commit()
    conn.close()
    return db, anchor, clone["other_page_id"]


def _service_for(db):
    return svc.DiscoveryService(
        path_provider=lambda: str(db),
        availability_callable=lambda: True,
        sidecar_version_provider=lambda: "eligibility-test",
    )


def test_a_human_confirmed_shared_text_row_is_visible_by_default(tmp_path):
    """The D-13g defect, in the related-pages query: before this change a row a
    human confirmed was hidden because routing had demoted it — the predicate
    meant to protect it never ran."""
    db, anchor, restored_page = _asset_with_one_human_confirmed_shared_text(tmp_path)
    service = _service_for(db)
    rows = service.get_pages_related_to_page(anchor)
    related = {r["related_page_id"] for r in rows}
    assert restored_page in related, (
        "a human-confirmed shared_text row must be visible by default")


def test_CONTROL_the_restore_test_fails_with_the_old_shipped_only_predicate(
    tmp_path, monkeypatch
):
    """Mutation control. Reading the diff cannot tell you whether the test above
    exercises the fix; putting the defect back can. With the pre-Codex-finding-3
    predicate restored, the human-confirmed row must disappear again — otherwise
    that test is passing for some other reason and proves nothing."""
    db, anchor, restored_page = _asset_with_one_human_confirmed_shared_text(tmp_path)
    monkeypatch.setattr(
        svc, "_review_toggle_mode",
        lambda include_review: (svc.ELIGIBILITY_ALL if include_review
                                else svc.ELIGIBILITY_SHIPPED_ONLY),
    )
    service = _service_for(db)
    related = {r["related_page_id"] for r in service.get_pages_related_to_page(anchor)}
    assert restored_page not in related, (
        "the shipped-only predicate no longer hides the human-confirmed row, so "
        "the restore test above is not testing the restore"
    )


def test_the_related_page_count_agrees_with_its_rows(tmp_path):
    """The header figure and the rows behind the toggle must apply the SAME
    predicate — a count that includes a row the list omits is worse than either
    being wrong alone."""
    db, anchor, _ = _asset_with_one_human_confirmed_shared_text(tmp_path)
    service = _service_for(db)
    count_env = service.get_related_page_count_enveloped(anchor)
    rows_env = service.get_related_pages_enveloped(anchor, page_size=100)
    assert count_env["total"] == rows_env["total"]
    assert count_env["total"] == len(rows_env["items"])


def test_a_review_only_row_nobody_confirmed_stays_hidden_by_default(tmp_path):
    """The other half: widening to D-13g must not widen to review_only. If this
    ever passes vacuously the fix has become "show everything"."""
    db = tmp_path / "review_only.db"
    conn = sqlite3.connect(str(db))
    conn.execute("PRAGMA foreign_keys = ON")
    sidecar_build.create_schema(conn)
    sidecar_build.populate_synthetic(conn, source_db_hash="eligibility-test")
    row = conn.execute(
        "SELECT evidence_id, a_page_id FROM discovery_evidence "
        "WHERE evidence_kind = 'shared_text' LIMIT 1").fetchone()
    if row is None:
        conn.close()
        pytest.skip("synthetic fixture carries no shared_text row")
    conn.execute(
        "UPDATE discovery_evidence SET routing_status = 'review_only', "
        "routing_reason = 'later_shared_text', adjudication_status = 'unreviewed' "
        "WHERE evidence_id = ?", (row[0],))
    conn.commit()
    conn.close()
    service = _service_for(db)
    default_rows = service.get_pages_related_to_page(row[1])
    assert row[0] not in {r["evidence_id"] for r in default_rows}
    review_rows = service.get_pages_related_to_page(row[1], include_review=True)
    assert row[0] in {r["evidence_id"] for r in review_rows}


# ---------------------------------------------------------------------------
# The expansion CTE: the divergence is explicit and DELIBERATELY unchanged.
# ---------------------------------------------------------------------------

def test_the_expansion_cte_still_applies_no_routing_predicate_by_default():
    """Pinned as an UNMADE DECISION, not as correct behaviour. Applying D-13g
    here would remove 80,718 of 222,972 rows (88,337 -> 48,701 at the pane's
    real grain, 58 panes emptying), which is a change to what the pane claims.
    If this test ever needs updating, that is the owner's ruling landing — and
    the number belongs in the same commit."""
    sql = svc._build_work_witnesses_ranked_cte_sql()
    assert "routing_status" not in sql
    assert "adjudication_status = 'human_confirmed'" not in sql
    # The column still comes THROUGH (the serializer needs it, SC#1).
    assert "de.adjudication_status AS adjudication_status" in sql


def test_the_shipped_module_level_cte_is_the_default_form():
    assert svc._WORK_WITNESSES_RANKED_CTE_SQL == (
        svc._build_work_witnesses_ranked_cte_sql())


@pytest.mark.parametrize("mode,expect", [
    (svc.ELIGIBILITY_DEFAULT, "OR de.adjudication_status = 'human_confirmed'"),
    (svc.ELIGIBILITY_SHIPPED_ONLY, "AND de.routing_status = 'shipped'"),
])
def test_the_expansion_cte_can_be_built_with_a_predicate_when_ruled(mode, expect):
    """The parameter is real, prefixed for the CTE's alias, and lands inside the
    WHERE rather than after it — so the ruling is a one-argument change."""
    sql = svc._build_work_witnesses_ranked_cte_sql(eligibility=mode)
    assert expect in sql
    where = sql.index("WHERE")
    assert sql.index("de.routing_status", where) > where


def test_the_corpus_wide_form_takes_the_predicate_too():
    """The cardinality probe ranks every work through this same fragment; if it
    could not carry the predicate, the probe would measure a population the
    service does not serve."""
    sql = svc._build_work_witnesses_ranked_cte_sql(
        restrict_work_id=False, eligibility=svc.ELIGIBILITY_DEFAULT)
    assert "dc.work_id = ?" not in sql
    assert "de.routing_status = 'shipped'" in sql


def test_an_unknown_eligibility_mode_fails_the_cte_build():
    with pytest.raises(ValueError, match="unknown mode"):
        svc._build_work_witnesses_ranked_cte_sql(eligibility="everything")
