"""POST /api/parallels transport. Emits JSON envelope to stdout.

Usage:
    python parallels.py --text "ויאמר אליו" [--chunk-size 5] [--mode exact]
                        [--max-freq N] [--boundary-mode full]
                        [--filters-json '{"library":["CUL"]}']
                        [--base-url URL]

Or read text from a file:
    python parallels.py --text-file path/to/text.txt

Per SKILL-01 / D-09: GENIZAH_API_BASE env var overrides --base-url.
This INVERTS the typical CLI convention — env wins.

NOTE: /api/parallels uses the `mode` field (NOT `search_mode`). This is
intentional per Phase 81A D-07 — the parallels endpoint keeps its original
field name distinct from search_mode used by /api/search.
"""
from __future__ import annotations
import argparse
import json
import sys

import requests

# Support both package import and direct script invocation.
try:
    from . import _config, throttle
except ImportError:
    import os
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import _config, throttle  # type: ignore

# Parallels endpoint uses `mode` (not `search_mode` — Phase 81A D-07)
PARALLELS_MODES = {"exact", "variants", "fuzzy"}


def call_parallels(
    *,
    text: str,
    chunk_size: int = 5,
    mode: str = "exact",
    max_freq: int | None = None,
    boundary_mode: str | None = None,
    filters: dict | None = None,
    base_url: str | None = None,
    timeout: float = 60.0,
) -> dict:
    """POST /api/parallels and return the parsed JSON response dict.

    Higher default timeout (60s) — composition searches can be slow (Phase 80).
    Always returns a dict — never raises.
    GENIZAH_API_BASE env var overrides base_url per D-09.

    Note: uses `mode` field (not `search_mode`) per Phase 81A D-07.
    """
    if mode not in PARALLELS_MODES:
        return {
            "error": {
                "code": "invalid_request",
                "message": f"unknown mode '{mode}'; valid: {sorted(PARALLELS_MODES)}",
            }
        }

    url = _config.resolve_base_url(base_url) + "/api/parallels"
    # NOTE: parallels uses `mode` (not `search_mode`) per 81A D-07
    body: dict = {
        "text": text,
        "chunk_size": chunk_size,
        "mode": mode,
    }
    if max_freq is not None:
        body["max_freq"] = max_freq
    if boundary_mode is not None:
        body["boundary_mode"] = boundary_mode
    if filters:
        body["filters"] = filters

    throttle.acquire("parallels")

    try:
        resp = requests.post(url, json=body, timeout=timeout)
    except requests.exceptions.Timeout:
        return {
            "error": {
                "code": "core_timeout",
                "message": f"parallels request timed out after {timeout}s",
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
        description="POST /api/parallels — emit JSON envelope to stdout"
    )
    text_src = p.add_mutually_exclusive_group(required=True)
    text_src.add_argument("--text", default=None, help="Composition text (inline)")
    text_src.add_argument(
        "--text-file",
        default=None,
        dest="text_file",
        help="Path to file containing composition text (use '-' for stdin)",
    )
    p.add_argument(
        "--chunk-size",
        type=int,
        default=5,
        dest="chunk_size",
        help="Chunk size in words (default: 5)",
    )
    p.add_argument(
        "--mode",
        default="exact",
        choices=sorted(PARALLELS_MODES),
        help="Match mode: exact | variants | fuzzy (default: exact)",
    )
    p.add_argument(
        "--max-freq",
        type=int,
        default=None,
        dest="max_freq",
        help="Max chunk frequency filter",
    )
    p.add_argument(
        "--boundary-mode",
        default=None,
        dest="boundary_mode",
        choices=["full", "boundary", "combined"],
        help="Boundary mode for combined search",
    )
    p.add_argument(
        "--filters-json",
        default=None,
        dest="filters_json",
        help='JSON filters dict e.g. \'{"library":["CUL"]}\'',
    )
    p.add_argument(
        "--base-url",
        default=None,
        dest="base_url",
        help=f"Base URL (default: {_config.DEFAULT_BASE_URL}; overridden by GENIZAH_API_BASE env)",
    )
    args = p.parse_args(argv)

    # Resolve text source
    if args.text is not None:
        text = args.text
    elif args.text_file == "-":
        text = sys.stdin.read()
    else:
        with open(args.text_file, encoding="utf-8") as fh:
            text = fh.read()

    filters = json.loads(args.filters_json) if args.filters_json else None

    result = call_parallels(
        text=text,
        chunk_size=args.chunk_size,
        mode=args.mode,
        max_freq=args.max_freq,
        boundary_mode=args.boundary_mode,
        filters=filters,
        base_url=args.base_url,
    )
    json.dump(result, sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(_main())
