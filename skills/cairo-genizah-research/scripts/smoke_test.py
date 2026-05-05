"""Bundled smoke harness for cairo-genizah-research skill.

Runs one /api/search, one /api/browse (drilling the first search result), and
one /api/parallels against the configured base URL. Prints a per-endpoint
pass/fail report to stdout and exits 0 (all pass) or non-zero (any fail).

Usage:
    python scripts/smoke_test.py [--base-url URL] [--query Q]

Per SKILL-01: works against any base URL (env var or CLI flag); D-09 env wins.
"""
from __future__ import annotations
import argparse
import os
import sys
from typing import Any

try:
    from . import _config
    from .search import call_search
    from .browse import call_browse
    from .parallels import call_parallels
except ImportError:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) + "/..")
    from scripts import _config  # type: ignore
    from scripts.search import call_search  # type: ignore
    from scripts.browse import call_browse  # type: ignore
    from scripts.parallels import call_parallels  # type: ignore


def _check(label: str, response: dict, want_keys: list[str]) -> tuple[bool, str]:
    if "error" in response:
        err = response["error"]
        return False, f"{label}: ERROR code={err.get('code')} message={err.get('message')}"
    for key in want_keys:
        if key not in response:
            return False, f"{label}: missing required key '{key}' in response"
    return True, f"{label}: OK (keys: {sorted(response.keys())[:5]}...)"


def run_smoke(base_url: str | None = None, query: str = "ויאמר") -> int:
    report: list[str] = []
    all_pass = True

    effective_base = _config.resolve_base_url(base_url)
    report.append(f"Base URL: {effective_base}")
    report.append("")

    s = call_search(query=query, search_mode="exact", limit=3, base_url=base_url)
    ok, msg = _check("search", s, ["schema_version", "source", "results"])
    report.append(msg)
    all_pass = all_pass and ok

    if ok and s.get("results"):
        first = s["results"][0]
        loc = first.get("locator") or {}
        b = call_browse(
            sys_id=loc.get("sys_id"),
            p_num=loc.get("p_num"),
            volume_ie=loc.get("volume_ie"),
            base_url=base_url,
        )
        if "error" in b and first.get("uid"):
            b = call_browse(uid=first["uid"], base_url=base_url)
        ok2, msg2 = _check("browse", b, ["schema_version", "source", "locator", "text_source"])
        report.append(msg2)
        all_pass = all_pass and ok2
    else:
        report.append("browse: SKIPPED (search returned 0 results or failed)")
        all_pass = False

    p = call_parallels(
        text="ויאמר משה אל בני ישראל ראו קרא בשם בצלאל",
        chunk_size=5,
        mode="exact",
        base_url=base_url,
    )
    ok3, msg3 = _check("parallels", p, ["schema_version", "source", "results"])
    report.append(msg3)
    all_pass = all_pass and ok3

    report.append("")
    report.append("=" * 50)
    report.append("OVERALL: " + ("PASS" if all_pass else "FAIL"))
    print("\n".join(report))
    return 0 if all_pass else 1


def _main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke test cairo-genizah-research skill")
    parser.add_argument("--base-url", default=None,
                        help="Override base URL (env GENIZAH_API_BASE wins per D-09)")
    parser.add_argument("--query", default="ויאמר",
                        help="Test query for /api/search smoke")
    args = parser.parse_args(argv)
    return run_smoke(base_url=args.base_url, query=args.query)


if __name__ == "__main__":
    sys.exit(_main())
