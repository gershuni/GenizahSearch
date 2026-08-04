# -*- coding: utf-8 -*-
"""Poll an HTTP origin until it answers, or exit non-zero with a clear reason.

Phase 136. Used by the ``findings-browser-check`` job in
``.github/workflows/ci.yml`` to replace ``sleep N && hope``.

A fixed sleep produces the two worst CI outcomes: a flake when the app is slow,
and a slow job when it is fast. Worse, when the app fails to boot a sleep hands
the browser check a dead port, which then fails with a browser timeout that says
nothing about the real cause. This polls, and when it gives up it says exactly
what it was waiting for, for how long, and what the last failure was.

Usage::

    python scripts/wait_for_http.py http://127.0.0.1:8099/ --timeout 120
"""
from __future__ import annotations

import argparse
import sys
import time
import urllib.error
import urllib.request


def wait(url: str, timeout: float, interval: float) -> str:
    """Return the last error string, or "" once the origin answers."""
    deadline = time.monotonic() + timeout
    last_error = "no attempt completed"
    attempts = 0
    while time.monotonic() < deadline:
        attempts += 1
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                status = getattr(response, "status", None) or response.getcode()
                if 200 <= int(status) < 500:
                    # 4xx still proves a server is listening and routing; the
                    # check that follows is what judges the page itself.
                    print(f"{url} answered {status} after {attempts} attempt(s)")
                    return ""
                last_error = f"HTTP {status}"
        except urllib.error.HTTPError as exc:
            print(f"{url} answered {exc.code} after {attempts} attempt(s)")
            return ""
        except Exception as exc:  # noqa: BLE001 -- every failure is just "not up yet"
            last_error = f"{type(exc).__name__}: {exc}"
        time.sleep(interval)
    return last_error


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("url")
    parser.add_argument("--timeout", type=float, default=120.0, help="seconds (default 120)")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between polls")
    args = parser.parse_args(argv)

    last_error = wait(args.url, args.timeout, args.interval)
    if last_error:
        print(
            f"TIMED OUT: {args.url} did not answer within {args.timeout:g}s. "
            f"Last failure: {last_error}. The server never came up -- read its log "
            "before looking at anything downstream.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
