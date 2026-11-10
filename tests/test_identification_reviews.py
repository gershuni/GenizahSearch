from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from web.identification_reviews import (
    DIRECT_NOVELTY_POTENTIALLY_NEW,
    IdentificationReviewError,
    RELATION_DIRECT_WITNESS,
    RELATION_SHARED_SOURCE,
    RELATION_VERDICTS,
    ReviewSubmission,
    anonymous_reviewer_key,
    moderate_review,
    normalize_submission,
    published_reviews,
    published_reviews_by_identification,
    submit_review,
)
from web.components.identification_review import (
    direct_novelty_icon,
    relation_verdict_icon,
    relation_verdict_text,
    review_text,
)


SESSION_UUID = "0123456789abcdef0123456789abcdef"


def submission(**overrides):
    values = {
        "identification_id": "a" * 64,
        "sidecar_version": "discovery-v1-test",
        "relation_verdict": RELATION_DIRECT_WITNESS,
        "direct_novelty": DIRECT_NOVELTY_POTENTIALLY_NEW,
        "comment": "A useful source.",
        "anonymous_key": anonymous_reviewer_key(SESSION_UUID),
        "sys_id": "990000000000001234",
        "page_id": "990000000000001234_IE1_P000007_FL9",
        "page_number": 7,
        "work_id": "w000001",
        "displayed_relation": "Direct witness",
    }
    values.update(overrides)
    return ReviewSubmission(**values)


def test_anonymous_key_is_stable_one_way_and_never_contains_the_session_uuid():
    first = anonymous_reviewer_key(SESSION_UUID)
    assert first == anonymous_reviewer_key(SESSION_UUID)
    assert len(first) == 64
    assert SESSION_UUID not in first


@pytest.mark.parametrize("bad", ["", "A" * 32, "xyz", "0" * 31])
def test_anonymous_key_refuses_malformed_session_values(bad):
    with pytest.raises(IdentificationReviewError):
        anonymous_reviewer_key(bad)


def test_non_direct_relationship_discards_the_direct_only_answer():
    params = normalize_submission(submission(
        relation_verdict=RELATION_SHARED_SOURCE,
        direct_novelty=DIRECT_NOVELTY_POTENTIALLY_NEW,
    ))
    assert params["p_direct_novelty"] is None


def test_closed_relation_vocabulary_and_comment_limit_are_enforced_client_side():
    with pytest.raises(IdentificationReviewError):
        normalize_submission(submission(relation_verdict="invented"))
    with pytest.raises(IdentificationReviewError):
        normalize_submission(submission(comment="x" * 1501))


def test_every_relation_has_bilingual_reader_copy_without_assertive_public_terms():
    for verdict in RELATION_VERDICTS:
        english = relation_verdict_text(verdict, "en")
        hebrew = relation_verdict_text(verdict, "he")
        assert english and hebrew and english != hebrew
        for prohibited in ("copy of", "quotes", "witness of"):
            assert prohibited not in english.lower()


def test_every_vote_value_has_a_compact_public_icon():
    assert all(relation_verdict_icon(value) for value in RELATION_VERDICTS)
    assert direct_novelty_icon(DIRECT_NOVELTY_POTENTIALLY_NEW) == "new_releases"
    assert direct_novelty_icon(None) == "remove_circle_outline"


class _RpcClient:
    def __init__(self, data):
        self.data = data
        self.calls = []

    def rpc(self, name, params):
        self.calls.append((name, params))
        return self

    def execute(self):
        return SimpleNamespace(data=self.data)


def test_submit_uses_only_the_constrained_rpc_and_returns_pending_status():
    client = _RpcClient([{"review_id": "id-1", "review_status": "pending"}])
    result = submit_review(submission(), client=client)
    assert result["review_status"] == "pending"
    assert client.calls[0][0] == "submit_identification_review_beta"
    assert client.calls[0][1]["p_identification_id"] == "a" * 64


def test_public_read_uses_the_identity_free_rpc():
    rows = [{
        "relation_verdict": RELATION_SHARED_SOURCE,
        "direct_novelty": None,
        "comment": "Shared source.",
        "published_at": "2026-08-13T12:00:00Z",
    }]
    client = _RpcClient(rows)
    assert published_reviews("a" * 64, client=client) == tuple(rows)
    assert client.calls == [(
        "get_published_identification_reviews_beta",
        {"p_identification_id": "a" * 64},
    )]
    assert "reviewer" not in rows[0]


def test_public_batch_groups_rows_and_strips_the_grouping_id_from_each_review():
    rows = [{
        "identification_id": "a" * 64,
        "relation_verdict": RELATION_SHARED_SOURCE,
        "direct_novelty": None,
        "comment": None,
        "published_at": "2026-08-13T12:00:00Z",
    }]
    client = _RpcClient(rows)
    grouped = published_reviews_by_identification(
        ["a" * 64, "a" * 64], client=client)
    assert grouped == {"a" * 64: ({
        "relation_verdict": RELATION_SHARED_SOURCE,
        "direct_novelty": None,
        "comment": None,
        "published_at": "2026-08-13T12:00:00Z",
    },)}
    assert client.calls == [(
        "get_published_identification_reviews_batch_beta",
        {"p_identification_ids": ["a" * 64]},
    )]


def test_moderator_can_edit_both_votes_and_keep_the_comment_private():
    client = _RpcClient(True)
    assert moderate_review(
        "review-id", "approved", "checked",
        relation_verdict=RELATION_DIRECT_WITNESS,
        direct_novelty=DIRECT_NOVELTY_POTENTIALLY_NEW,
        comment="Useful, but private.",
        publish_comment=False,
        client=client,
    )
    name, params = client.calls[0]
    assert name == "moderate_identification_review_beta_v2"
    assert params["p_relation_verdict"] == RELATION_DIRECT_WITNESS
    assert params["p_direct_novelty"] == DIRECT_NOVELTY_POTENTIALLY_NEW
    assert params["p_comment"] == "Useful, but private."
    assert params["p_publish_comment"] is False


def test_requested_hebrew_question_is_exact():
    assert review_text("action", "he") == "האם ההתאמה נכונה?"


def test_migration_keeps_table_writes_private_and_public_output_identity_free():
    sql = Path("scripts/create_identification_reviews_beta.sql").read_text(
        encoding="utf-8")
    assert "ENABLE ROW LEVEL SECURITY" in sql
    assert "REVOKE ALL ON TABLE public.identification_reviews FROM anon" in sql
    assert "GRANT INSERT" not in sql
    assert "TO anon, authenticated" in sql  # constrained public RPC grants
    assert "ADD COLUMN IF NOT EXISTS publish_comment" in sql
    assert "CASE WHEN r.publish_comment THEN r.comment ELSE NULL END" in sql
    assert "get_published_identification_reviews_batch_beta" in sql
    assert "moderate_identification_review_beta_v2" in sql

    public_function = sql.split(
        "CREATE OR REPLACE FUNCTION public.get_published_identification_reviews_beta",
        1,
    )[1].split(
        "CREATE OR REPLACE FUNCTION public.moderate_identification_review_beta",
        1,
    )[0]
    returns_clause = public_function.split("LANGUAGE sql", 1)[0]
    for private_field in ("reviewer_key", "reviewer_user_id", "reviewed_by"):
        assert private_field not in returns_clause
