"""Neutral exception module shared by web and shared layers.

Concern #3 fix (78-REVIEWS.md): previously APIError lived in
web/api_hardening.py, which forced shared/fjms_service.validate_filter_values
to do a `shared -> web` lazy import. Moving APIError to shared/ eliminates
that dependency inversion: shared/ depends only on stdlib, web/ depends on
shared/, never the reverse.

Both `web/api_hardening.py` AND `shared/fjms_service.validate_filter_values`
import APIError from here. This module deliberately imports NOTHING from
the web layer or any web framework (nicegui, fastapi, starlette).
"""

import logging
from typing import Optional


logger = logging.getLogger(__name__)


# D-07 error code taxonomy (lowercase snake_case, stable, contractual).
# These codes are part of the public API surface — Phase 81's skill consumer
# branches on `code` strings, so renaming any of them is a breaking change.
ERROR_CODES = frozenset({
    'invalid_request',
    'invalid_mode',
    'query_required',
    'query_too_long',
    'limit_too_high',
    'unknown_filter_key',
    'unresolvable_filter_value',
    'filter_vocabulary_unavailable',  # R2-#3: fail-closed when vocabulary loader fails
    'rate_limited',
    'disabled',
    'localhost_only',
    'internal_error',
    # Phase 79 (/api/browse) additions:
    'locator_conflict',
    'manuscript_page_not_found',
    'core_timeout',
    # Phase 80 (/api/parallels) additions:
    'composition_required',     # D-06: text.strip() empty
    'composition_too_long',     # D-06: len(text.strip()) > COMPOSITION_LENGTH_CAP (20000)
})

# Surfaced in top-level `warnings: []` arrays (D-07), NOT as errors.
WARNING_CODES = frozenset({
    'query_downgraded',
    # Phase 80 (/api/parallels) additions:
    'truncated_to_200',         # D-07: parallels group count exceeds 200; top 200 returned
})


class APIError(Exception):
    """Semantic API error. Raised inside handlers and validators.

    Caught by the per-endpoint `wrap_endpoint` / `_build_envelope_response`
    helper from web.api_hardening (Plan 03; per-endpoint, NOT global per
    Concern #2).

    Args:
        code: One of ERROR_CODES (lowercase snake_case). Unknown codes log
            a warning but are not rejected — the taxonomy can evolve, the
            wrapper renders whatever code is supplied.
        message: Human-readable message. Logs use this; the skill consumer
            branches on `code`, not message text.
        http_status: HTTP status to return in the envelope. Defaults to 400.
        headers: Optional HTTP headers to propagate (notably Retry-After
            for rate_limited responses).
    """

    def __init__(
        self,
        code: str,
        message: str,
        http_status: int = 400,
        headers: Optional[dict] = None,
    ):
        if code not in ERROR_CODES:
            logger.warning(
                "APIError raised with unknown code %r; allowed: %s",
                code, sorted(ERROR_CODES),
            )
        self.code = code
        self.message = message
        self.http_status = http_status
        self.headers = headers or {}
        super().__init__(f"{code}: {message}")
