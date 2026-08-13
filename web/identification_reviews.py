# -*- coding: utf-8 -*-
"""Storage boundary for the computed-identification review beta.

The browser-facing component never writes the review table directly.  It calls
the ``submit_identification_review_beta`` RPC, whose SQL implementation owns
validation, throttling, reviewer identity and the pending-only publication
workflow.  Keeping that boundary here also gives the UI one small failure mode:
if the migration is not installed yet, the review dialog remains usable and
offers the existing email channel instead.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence

logger = logging.getLogger(__name__)


RELATION_DIRECT_WITNESS = "direct_witness"
RELATION_MANUSCRIPT_QUOTES_WORK = "manuscript_quotes_work"
RELATION_SHARED_SOURCE = "shared_source"
RELATION_WORK_QUOTES_MANUSCRIPT = "work_quotes_manuscript"
RELATION_NOT_MEANINGFUL = "not_meaningful"
RELATION_OTHER_UNSURE = "other_unsure"

RELATION_VERDICTS = frozenset({
    RELATION_DIRECT_WITNESS,
    RELATION_MANUSCRIPT_QUOTES_WORK,
    RELATION_SHARED_SOURCE,
    RELATION_WORK_QUOTES_MANUSCRIPT,
    RELATION_NOT_MEANINGFUL,
    RELATION_OTHER_UNSURE,
})

DIRECT_NOVELTY_POTENTIALLY_NEW = "potentially_new"
DIRECT_NOVELTY_ALREADY_KNOWN = "already_known"
DIRECT_NOVELTY_OTHER_UNSURE = "other_unsure"
DIRECT_NOVELTY_VERDICTS = frozenset({
    DIRECT_NOVELTY_POTENTIALLY_NEW,
    DIRECT_NOVELTY_ALREADY_KNOWN,
    DIRECT_NOVELTY_OTHER_UNSURE,
})

REVIEW_STATUSES = frozenset({"pending", "approved", "rejected"})
MAX_COMMENT_LENGTH = 1500
_PUBLIC_READ_SEMAPHORE = asyncio.Semaphore(4)


class IdentificationReviewError(ValueError):
    """A safe, reader-facing validation or storage failure."""


@dataclass(frozen=True)
class ReviewSubmission:
    identification_id: str
    sidecar_version: str
    relation_verdict: str
    direct_novelty: Optional[str]
    comment: Optional[str]
    anonymous_key: str
    sys_id: Optional[str] = None
    page_id: Optional[str] = None
    page_number: Optional[int] = None
    work_id: Optional[str] = None
    displayed_relation: Optional[str] = None


def reviews_enabled() -> bool:
    """Return the lazy rollout flag; enabled by default for the beta."""
    return os.getenv("IDENTIFICATION_REVIEWS_ENABLED", "true").strip().lower() \
        not in {"0", "false", "no", "off"}


def anonymous_reviewer_key(session_uuid: str) -> str:
    """One-way, stable key for an anonymous NiceGUI session.

    The raw session UUID is a server-side session secret and must not leave the
    web process.  UUID4 has enough entropy that a domain-separated SHA-256
    digest is non-reversible in practice; the database receives only the
    digest.  Authenticated submissions are keyed to ``auth.uid()`` inside the
    SQL function and ignore this value for identity purposes.
    """
    value = str(session_uuid or "")
    if len(value) != 32 or any(ch not in "0123456789abcdef" for ch in value):
        raise IdentificationReviewError("Review session is unavailable.")
    return hashlib.sha256(
        ("genizah-identification-review-v1:" + value).encode("ascii")
    ).hexdigest()


def _clean_required(value: Any, *, field: str, maximum: int) -> str:
    text = str(value or "").strip()
    if not text or len(text) > maximum:
        raise IdentificationReviewError(f"Invalid {field}.")
    return text


def _clean_optional(value: Any, *, maximum: int) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) > maximum:
        raise IdentificationReviewError("Comment or context is too long.")
    return text


def normalize_submission(submission: ReviewSubmission) -> Dict[str, Any]:
    """Validate and convert a submission to the exact RPC parameter shape."""
    identification_id = _clean_required(
        submission.identification_id, field="identification", maximum=128)
    sidecar_version = _clean_required(
        submission.sidecar_version, field="data version", maximum=200)
    relation = str(submission.relation_verdict or "").strip()
    if relation not in RELATION_VERDICTS:
        raise IdentificationReviewError("Choose what kind of relationship this is.")

    novelty = submission.direct_novelty
    if relation != RELATION_DIRECT_WITNESS:
        novelty = None
    elif novelty is not None and novelty not in DIRECT_NOVELTY_VERDICTS:
        raise IdentificationReviewError("Choose a valid direct-identification status.")

    page_number = submission.page_number
    if page_number is not None:
        if isinstance(page_number, bool):
            raise IdentificationReviewError("Invalid page number.")
        try:
            page_number = int(page_number)
        except (TypeError, ValueError) as exc:
            raise IdentificationReviewError("Invalid page number.") from exc
        if page_number < 1 or page_number > 2_000_000_000:
            raise IdentificationReviewError("Invalid page number.")

    anonymous_key = _clean_required(
        submission.anonymous_key, field="review session", maximum=64)
    if len(anonymous_key) != 64 or any(
        char not in "0123456789abcdef" for char in anonymous_key
    ):
        raise IdentificationReviewError("Invalid review session.")

    return {
        "p_identification_id": identification_id,
        "p_sidecar_version": sidecar_version,
        "p_relation_verdict": relation,
        "p_direct_novelty": novelty,
        "p_comment": _clean_optional(
            submission.comment, maximum=MAX_COMMENT_LENGTH),
        "p_anonymous_key": anonymous_key,
        "p_sys_id": _clean_optional(submission.sys_id, maximum=128),
        "p_page_id": _clean_optional(submission.page_id, maximum=300),
        "p_page_number": page_number,
        "p_work_id": _clean_optional(submission.work_id, maximum=128),
        "p_displayed_relation": _clean_optional(
            submission.displayed_relation, maximum=200),
    }


def submit_review(submission: ReviewSubmission, *, client) -> Dict[str, Any]:
    """Submit through the constrained RPC using a caller-provided client."""
    if client is None:
        raise IdentificationReviewError("Review service is unavailable.")
    params = normalize_submission(submission)
    try:
        response = client.rpc(
            "submit_identification_review_beta", params).execute()
    except Exception as exc:
        # Artifact ids, user comments and Supabase exception strings do not
        # belong in logs.  The type is enough to distinguish setup/network bugs.
        logger.warning("identification review submission failed (%s)",
                       type(exc).__name__)
        raise IdentificationReviewError("Review service is temporarily unavailable.") \
            from None
    rows = response.data or []
    return dict(rows[0]) if isinstance(rows, list) and rows else {"status": "pending"}


def published_reviews(identification_id: str, *, client) -> Sequence[Dict[str, Any]]:
    """Return only the identity-free rows exposed by the public RPC."""
    finding_id = _clean_required(
        identification_id, field="identification", maximum=128)
    if client is None:
        return ()
    try:
        response = client.rpc(
            "get_published_identification_reviews_beta",
            {"p_identification_id": finding_id},
        ).execute()
    except Exception as exc:
        logger.info("published identification reviews unavailable (%s)",
                    type(exc).__name__)
        return ()
    return tuple(dict(row) for row in (response.data or ()))


def published_reviews_by_identification(
    identification_ids: Iterable[Any], *, client,
) -> Dict[str, Sequence[Dict[str, Any]]]:
    """Batch the public review overlay for one rendered page.

    Public findings pages can contain dozens of identification leaves.  One RPC
    per leaf would turn the small human-review overlay into the most expensive
    part of the page, so the SQL boundary accepts the visible ids as one bounded
    request and still returns only identity-free approved fields.
    """
    ids = []
    for value in identification_ids:
        cleaned = str(value or "").strip()
        if cleaned and len(cleaned) <= 128 and cleaned not in ids:
            ids.append(cleaned)
        if len(ids) >= 100:
            break
    if not ids or client is None:
        return {}
    try:
        response = client.rpc(
            "get_published_identification_reviews_batch_beta",
            {"p_identification_ids": ids},
        ).execute()
    except Exception as exc:
        logger.info("published identification review batch unavailable (%s)",
                    type(exc).__name__)
        return {}
    grouped: Dict[str, list[Dict[str, Any]]] = {}
    for raw in response.data or ():
        row = dict(raw)
        identification_id = str(row.pop("identification_id", "") or "")
        if identification_id in ids:
            grouped.setdefault(identification_id, []).append(row)
    return {key: tuple(rows) for key, rows in grouped.items()}


async def published_reviews_by_identification_async(
    identification_ids: Iterable[Any], *, client,
) -> Dict[str, Sequence[Dict[str, Any]]]:
    """Off-loop form for async page builders; exactly one bounded dispatch."""
    from web.bounded_io import bounded_io_bound

    result = await bounded_io_bound(
        _PUBLIC_READ_SEMAPHORE,
        published_reviews_by_identification,
        identification_ids,
        client=client,
    )
    return dict(result or {})


_ADMIN_SELECT = (
    "id,identification_id,sidecar_version,sys_id,page_id,page_number,work_id,"
    "displayed_relation,relation_verdict,direct_novelty,comment,status,"
    "publish_comment,reviewer_user_id,submitted_at,updated_at"
)


def pending_reviews(*, client) -> Sequence[Dict[str, Any]]:
    """Admin-only pending queue. RLS is the authority, not the admin page."""
    if client is None:
        return ()
    try:
        response = client.table("identification_reviews").select(
            _ADMIN_SELECT
        ).eq("status", "pending").order("updated_at", desc=True).execute()
    except Exception as exc:
        logger.warning("pending identification reviews unavailable (%s)",
                       type(exc).__name__)
        return ()
    return tuple(dict(row) for row in (response.data or ()))


def moderate_review(
    review_id: str,
    status: str,
    note: Optional[str],
    *,
    relation_verdict: Optional[str] = None,
    direct_novelty: Optional[str] = None,
    comment: Optional[str] = None,
    publish_comment: bool = False,
    client,
) -> bool:
    """Save the moderator's edited assessment and approve or reject it."""
    if status not in {"approved", "rejected"}:
        raise IdentificationReviewError("Invalid moderation decision.")
    rid = _clean_required(review_id, field="review", maximum=64)
    relation = _clean_optional(relation_verdict, maximum=64)
    if relation is not None and relation not in RELATION_VERDICTS:
        raise IdentificationReviewError("Invalid moderated relationship.")
    novelty = _clean_optional(direct_novelty, maximum=64)
    if relation is not None and relation != RELATION_DIRECT_WITNESS:
        novelty = None
    elif novelty is not None and novelty not in DIRECT_NOVELTY_VERDICTS:
        raise IdentificationReviewError("Invalid moderated novelty assessment.")
    cleaned_comment = _clean_optional(comment, maximum=MAX_COMMENT_LENGTH)
    params = {
        "p_review_id": rid,
        "p_status": status,
        "p_moderation_note": _clean_optional(note, maximum=1000),
        "p_relation_verdict": relation,
        "p_direct_novelty": novelty,
        "p_comment": cleaned_comment,
        "p_publish_comment": bool(publish_comment and cleaned_comment),
    }
    if client is None:
        return False
    try:
        response = client.rpc(
            "moderate_identification_review_beta_v2", params).execute()
    except Exception as exc:
        logger.warning("identification review moderation failed (%s)",
                       type(exc).__name__)
        return False
    rows = response.data or []
    if isinstance(rows, bool):
        return rows
    if isinstance(rows, list) and rows:
        value = rows[0]
        if isinstance(value, Mapping):
            return bool(value.get("moderate_identification_review_beta_v2", True))
    return bool(rows)


__all__ = [
    "DIRECT_NOVELTY_ALREADY_KNOWN",
    "DIRECT_NOVELTY_OTHER_UNSURE",
    "DIRECT_NOVELTY_POTENTIALLY_NEW",
    "DIRECT_NOVELTY_VERDICTS",
    "IdentificationReviewError",
    "MAX_COMMENT_LENGTH",
    "RELATION_DIRECT_WITNESS",
    "RELATION_MANUSCRIPT_QUOTES_WORK",
    "RELATION_NOT_MEANINGFUL",
    "RELATION_OTHER_UNSURE",
    "RELATION_SHARED_SOURCE",
    "RELATION_VERDICTS",
    "RELATION_WORK_QUOTES_MANUSCRIPT",
    "ReviewSubmission",
    "anonymous_reviewer_key",
    "moderate_review",
    "normalize_submission",
    "pending_reviews",
    "published_reviews",
    "published_reviews_by_identification",
    "published_reviews_by_identification_async",
    "reviews_enabled",
    "submit_review",
]
