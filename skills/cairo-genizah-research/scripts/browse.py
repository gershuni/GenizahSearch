"""GET /api/browse transport. Emits JSON envelope to stdout.

Usage (preferred — single uid locator):
    python browse.py --uid "CUL/T-S_12.123/001r"

Usage (fallback A — sys_id + page):
    python browse.py --sys-id 990001234560205171 --p-num 1 [--volume-ie "IE001"]

Usage (fallback B — fl_id):
    python browse.py --fl-id "T-S 12.123.1r"

Optional: --text-cap N (100..10000; default server-side 4000)
          --base-url URL

Per SKILL-01 / D-09: GENIZAH_API_BASE env var overrides --base-url.
This INVERTS the typical CLI convention — env wins.
"""
from __future__ import annotations
import argparse
import json
import sys
from urllib.parse import urlencode

import requests

# Support both package import and direct script invocation.
try:
    from . import _config, throttle
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import _config, throttle  # type: ignore


def call_browse(
    *,
    uid: str | None = None,
    sys_id: str | None = None,
    p_num: int | None = None,
    volume_ie: str | None = None,
    fl_id: str | None = None,
    text_cap: int | None = None,
    base_url: str | None = None,
    timeout: float = 30.0,
) -> dict:
    """GET /api/browse and return the parsed JSON response dict.

    At least one of uid, sys_id, or fl_id must be provided. Locator preference:
    1. uid (preferred — single param, phase 79 D-13)
    2. sys_id (+ optional p_num, volume_ie)
    3. fl_id

    Always returns a dict — never raises.
    GENIZAH_API_BASE env var overrides base_url per D-09.
    """
    if not any([uid, sys_id, fl_id]):
        return {
            "error": {
                "code": "invalid_request",
                "message": "must provide uid, sys_id, or fl_id",
            }
        }

    # Build query params by preferred locator
    params: dict[str, str | int] = {}
    if uid:
        params["uid"] = uid
    elif sys_id:
        params["sys_id"] = sys_id
        if p_num is not None:
            params["p_num"] = p_num
        if volume_ie:
            params["volume_ie"] = volume_ie
    else:
        params["fl_id"] = fl_id  # type: ignore[assignment]

    if text_cap is not None:
        params["text_cap"] = text_cap

    url = _config.resolve_base_url(base_url) + "/api/browse"
    throttle.acquire("browse")

    try:
        resp = requests.get(url, params=params, timeout=timeout)
    except requests.exceptions.Timeout:
        return {
            "error": {
                "code": "core_timeout",
                "message": f"browse request timed out after {timeout}s",
            }
        }
    except requests.exceptions.ConnectionError as e:
        return {"error": {"code": "connection_error", "message": str(e)}}

    try:
        data = resp.json()
    except (json.JSONDecodeError, ValueError):
        return {
            "error": {
                "code": "invalid_response",
                "message": f"non-JSON response (HTTP {resp.status_code})",
            }
        }

    # Surface Retry-After on 429
    if resp.status_code == 429 and "Retry-After" in resp.headers:
        if isinstance(data, dict) and "error" in data:
            data["error"]["retry_after"] = resp.headers["Retry-After"]

    return data


def _main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="GET /api/browse — emit JSON envelope to stdout"
    )
    locator = p.add_mutually_exclusive_group()
    locator.add_argument("--uid", default=None, help="Preferred single-param locator")
    locator.add_argument(
        "--fl-id",
        default=None,
        dest="fl_id",
        help="Fragment/leaf locator (e.g. 'T-S 12.123.1r')",
    )
    p.add_argument(
        "--sys-id",
        default=None,
        dest="sys_id",
        help="System ID (used with --fl-id fallback B or standalone)",
    )
    p.add_argument(
        "--p-num",
        type=int,
        default=None,
        dest="p_num",
        help="Page number within sys_id",
    )
    p.add_argument(
        "--volume-ie",
        default=None,
        dest="volume_ie",
        help="Volume IE identifier (optional with --sys-id)",
    )
    p.add_argument(
        "--text-cap",
        type=int,
        default=None,
        dest="text_cap",
        help="Max transcription chars (100..10000)",
    )
    p.add_argument(
        "--base-url",
        default=None,
        dest="base_url",
        help=f"Base URL (default: {_config.DEFAULT_BASE_URL}; overridden by GENIZAH_API_BASE env)",
    )
    args = p.parse_args(argv)

    # Allow --sys-id alone (without --uid or --fl-id) for fallback A
    result = call_browse(
        uid=args.uid,
        sys_id=args.sys_id,
        p_num=args.p_num,
        volume_ie=args.volume_ie,
        fl_id=args.fl_id,
        text_cap=args.text_cap,
        base_url=args.base_url,
    )
    json.dump(result, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
