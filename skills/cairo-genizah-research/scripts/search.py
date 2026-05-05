"""POST /api/search transport. Emits JSON envelope to stdout.

Usage: python search.py --query "ויאמר" --search-mode exact --limit 10
                        [--gap N] [--filters-json '{"library":["CUL"]}']
                        [--responsa-options-json '{"variants":true}']
                        [--base-url URL]

Per SKILL-01 / D-09: GENIZAH_API_BASE env var overrides --base-url.
This INVERTS the typical CLI convention — env wins.

Valid search_mode values: exact | variants | responsa | title | shelfmark
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

SEARCH_MODES = {"exact", "variants", "responsa", "title", "shelfmark"}


def call_search(
    *,
    query: str,
    search_mode: str = "exact",
    limit: int = 10,
    gap: int = 0,
    filters: dict | None = None,
    responsa_options: dict | None = None,
    base_url: str | None = None,
    timeout: float = 30.0,
) -> dict:
    """POST /api/search and return the parsed JSON response dict.

    Always returns a dict — either the server's success envelope or an
    error envelope matching shared/api_errors.py shape. Never raises.

    GENIZAH_API_BASE env var overrides base_url per D-09.
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
        "limit": limit,
    }
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

    # On 429, surface Retry-After if present so callers can observe it.
    if resp.status_code == 429 and "Retry-After" in resp.headers:
        if isinstance(data, dict) and "error" in data:
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
    p.add_argument("--limit", type=int, default=10, help="Max results (1-100)")
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
