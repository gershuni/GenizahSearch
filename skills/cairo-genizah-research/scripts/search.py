"""POST /api/search transport. Emits JSON envelope to stdout.

Usage: python search.py --query "ויאמר" --search-mode exact [--limit N]
                        [--gap N] [--filters-json '{"library":["CUL"]}']
                        [--responsa-options-json '{"variants":true}']
                        [--base-url URL]

Per SKILL-01 / D-09: GENIZAH_API_BASE env var overrides --base-url.
This INVERTS the typical CLI convention — env wins.

Valid search_mode values: exact | variants | responsa | title | shelfmark | fuzzy

Runtime notes:
- fuzzy mode is the slowest (variants_maximum tier); the server allows up to
  ~300s (SEARCH_API_FUZZY_TIMEOUT). Client timeout default is 320s so the
  server's 504 core_timeout envelope wins instead of a socket-level timeout.
- fuzzy limit ceiling is SEARCH_API_FUZZY_MAX_LIMIT (server default 500); pass
  a higher --limit for recall-critical queries (max 2000).
- If a heavy-mode (variants/fuzzy) request hits the server's concurrency cap,
  the server returns 503 heavy_search_busy + Retry-After; handle by retrying.
"""
from __future__ import annotations
import argparse
import json
import sys

import requests

# Support both `python -m skills.cairo_genizah_research.scripts.search` and
# `python skills/cairo-genizah-research/scripts/search.py` direct invocations.
try:
    from . import _config, throttle
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import _config, throttle  # type: ignore

SEARCH_MODES = {"exact", "variants", "responsa", "title", "shelfmark", "fuzzy"}


def call_search(
    *,
    query: str,
    search_mode: str = "exact",
    limit: int | None = None,
    gap: int = 0,
    filters: dict | None = None,
    responsa_options: dict | None = None,
    base_url: str | None = None,
    timeout: float = 320.0,
) -> dict:
    """POST /api/search and return the parsed JSON response dict.

    Always returns a dict — either the server's success envelope or an
    error envelope matching shared/api_errors.py shape. Never raises.

    GENIZAH_API_BASE env var overrides base_url per D-09.

    limit defaults to None: when omitted, the request carries no `limit` key
    so the SERVER applies its per-mode default (50 for non-fuzzy; a wider
    recall default ~250 for fuzzy via SEARCH_API_FUZZY_MAX_LIMIT). Passing an
    explicit limit overrides that — required to exceed 100 on fuzzy.

    timeout default is 320s (slightly above the server's heaviest ceiling of
    300s for fuzzy/parallels) so the server's 504 core_timeout envelope is
    returned rather than a client-side socket timeout. For interactive modes
    (exact/title/shelfmark/responsa) the server responds within 30s; the
    higher default never causes a problem in practice.
    """
    if search_mode not in SEARCH_MODES:
        return {
            "error": {
                "code": "invalid_request",
                "message": f"unknown search_mode '{search_mode}'; valid: {sorted(SEARCH_MODES)}",
            }
        }
    url = _config.resolve_base_url(base_url) + "/api/search"
    body: dict = {
        "search_mode": search_mode,
        "query": query,
        "gap": gap,
    }
    # Omit `limit` entirely when unset so the server's per-mode default applies
    # (esp. fuzzy's wider recall default — sending a value here would suppress it).
    if limit is not None:
        body["limit"] = limit
    if filters:
        body["filters"] = filters
    if responsa_options:
        body["responsa_options"] = responsa_options

    throttle.acquire("search")

    try:
        resp = requests.post(url, json=body, timeout=timeout)
    except requests.exceptions.Timeout:
        return {
            "error": {
                "code": "core_timeout",
                "message": f"search request timed out after {timeout}s",
            }
        }
    except requests.exceptions.ConnectionError as e:
        return {"error": {"code": "connection_error", "message": str(e)}}

    # Pass through server response — error envelope OR success envelope.
    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return {
            "error": {
                "code": "invalid_response",
                "message": f"non-JSON response (HTTP {resp.status_code})",
            }
        }

    # Surface Retry-After when present so callers can honor the server's
    # backoff hint — applies to BOTH 429 rate_limited AND 503 heavy_search_busy.
    if "Retry-After" in resp.headers and isinstance(data, dict) and "error" in data:
        data["error"]["retry_after"] = resp.headers["Retry-After"]

    return data


def _main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="POST /api/search — emit JSON envelope to stdout"
    )
    p.add_argument("--query", required=True, help="Search query string")
    p.add_argument(
        "--search-mode",
        default="exact",
        choices=sorted(SEARCH_MODES),
        dest="search_mode",
        help="Search mode (default: exact)",
    )
    p.add_argument(
        "--limit", type=int, default=None,
        help=(
            "Max results. When omitted, the server's per-mode default applies "
            "(50 for non-fuzzy; ~250 recall default for fuzzy). "
            "Non-fuzzy modes accept 1-100; fuzzy accepts up to 2000 "
            "(SEARCH_API_FUZZY_MAX_LIMIT, server default 500)."
        ),
    )
    p.add_argument("--gap", type=int, default=0, help="Token gap for phrase search")
    p.add_argument(
        "--filters-json",
        default=None,
        dest="filters_json",
        help='JSON filters dict e.g. \'{"library":["CUL"]}\'',
    )
    p.add_argument(
        "--responsa-options-json",
        default=None,
        dest="responsa_options_json",
        help='JSON responsa_options dict (only with --search-mode responsa)',
    )
    p.add_argument(
        "--base-url",
        default=None,
        dest="base_url",
        help=f"Base URL (default: {_config.DEFAULT_BASE_URL}; overridden by GENIZAH_API_BASE env)",
    )
    args = p.parse_args(argv)

    filters = json.loads(args.filters_json) if args.filters_json else None
    ropts = (
        json.loads(args.responsa_options_json) if args.responsa_options_json else None
    )

    result = call_search(
        query=args.query,
        search_mode=args.search_mode,
        limit=args.limit,
        gap=args.gap,
        filters=filters,
        responsa_options=ropts,
        base_url=args.base_url,
    )
    json.dump(result, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
