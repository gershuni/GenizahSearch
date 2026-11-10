# -*- coding: utf-8 -*-
"""docs/guides/ENV_VARS.md must not contradict the code it documents.

That file declares itself the canonical environment-variable reference, and
CLAUDE.md tells agents to read it before changing any of them. It also
duplicates the code's default values as literal `NAME=value` lines -- which
means it can disagree with the code, and when it does the disagreement is worse
than a stale doc: an operator configuring a deployment from it SETS those
values, and an explicit environment value wins over the code default.

That is not hypothetical. When the fuzzy/parallels ceilings dropped 300 -> 110
(so our JSON `core_timeout` 504 would beat the edge proxy's opaque error), this
file still said 300. Anyone following it would have restored the exact bug the
change removed -- and, because the bundled skill client now stops at 130 s,
would have got a client-side socket timeout rather than the server envelope.
Found by the Codex review of PR #336.

So the numbers are asserted against the code, one by one. A new SEARCH_API_*
default worth documenting should be added to both places and listed here.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
ENV_VARS_MD = REPO_ROOT / "docs" / "guides" / "ENV_VARS.md"


def _documented_defaults() -> dict:
    """Parse the `NAME=value` lines out of the guide's shell-style blocks."""
    text = ENV_VARS_MD.read_text(encoding="utf-8")
    found = {}
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or "=" not in stripped:
            continue
        m = re.match(r"^([A-Z][A-Z0-9_]*)=([^\s#]*)", stripped)
        if m:
            # First occurrence wins; later mentions are prose examples.
            found.setdefault(m.group(1), m.group(2))
    return found


@pytest.fixture(scope="module")
def documented():
    d = _documented_defaults()
    assert d, f"parsed no NAME=value lines from {ENV_VARS_MD}"
    return d


# (env var in the guide, code object holding the default)
def _code_defaults():
    from web.search_api import (
        DEFAULT_SEARCH_CORE_TIMEOUT,
        DEFAULT_VARIANTS_TIMEOUT,
        DEFAULT_FUZZY_TIMEOUT,
        DEFAULT_PARALLELS_TIMEOUT,
        DEFAULT_PASSAGE_TIMEOUT,
        DEFAULT_HEAVY_CONCURRENCY,
        DEFAULT_PASSAGE_CONCURRENCY,
    )
    from shared.browse_service import (
        DEFAULT_BROWSE_TIMEOUT,
        DEFAULT_BROWSE_CORE_TIMEOUT,
        DEFAULT_BROWSE_CORE_WARMUP_TIMEOUT,
    )
    return {
        "SEARCH_API_CORE_TIMEOUT": DEFAULT_SEARCH_CORE_TIMEOUT,
        "SEARCH_API_VARIANTS_TIMEOUT": DEFAULT_VARIANTS_TIMEOUT,
        "SEARCH_API_FUZZY_TIMEOUT": DEFAULT_FUZZY_TIMEOUT,
        "SEARCH_API_PARALLELS_TIMEOUT": DEFAULT_PARALLELS_TIMEOUT,
        "SEARCH_API_PASSAGE_TIMEOUT": DEFAULT_PASSAGE_TIMEOUT,
        "SEARCH_API_HEAVY_CONCURRENCY": DEFAULT_HEAVY_CONCURRENCY,
        "SEARCH_API_PASSAGE_CONCURRENCY": DEFAULT_PASSAGE_CONCURRENCY,
        "SEARCH_API_BROWSE_TIMEOUT": DEFAULT_BROWSE_TIMEOUT,
        "SEARCH_API_BROWSE_CORE_TIMEOUT": DEFAULT_BROWSE_CORE_TIMEOUT,
        "SEARCH_API_BROWSE_CORE_WARMUP_TIMEOUT": DEFAULT_BROWSE_CORE_WARMUP_TIMEOUT,
    }


@pytest.mark.parametrize("name", sorted(_code_defaults()))
def test_documented_default_matches_the_code(name, documented):
    """Compared numerically: the guide writes `60` where the code has `60.0`,
    and that is fine -- `_read_timeout` parses either. A different NUMBER is not.
    """
    expected = _code_defaults()[name]
    assert name in documented, (
        f"{name} is not documented in docs/guides/ENV_VARS.md, which claims to "
        f"be the canonical environment-variable reference"
    )
    raw = documented[name]
    try:
        actual = float(raw)
    except ValueError:
        pytest.fail(f"{name}={raw!r} in ENV_VARS.md is not a number")
    assert actual == float(expected), (
        f"docs/guides/ENV_VARS.md says {name}={raw}, but the code default is "
        f"{expected}. An operator configuring from that guide SETS this value, "
        f"and an explicit env value wins over the code default -- so a wrong "
        f"number here is not a stale doc, it is a misconfigured deployment."
    )


def test_the_two_edge_bound_ceilings_are_110_in_both_places():
    """Named explicitly rather than left to the parametrized sweep.

    These two are the reason this file exists, and 300 is not merely a wrong
    number here -- it is the specific value that makes the endpoint
    undeliverable through the edge proxy.
    """
    from web.search_api import DEFAULT_FUZZY_TIMEOUT, DEFAULT_PARALLELS_TIMEOUT
    assert DEFAULT_FUZZY_TIMEOUT == DEFAULT_PARALLELS_TIMEOUT == 110.0

    text = ENV_VARS_MD.read_text(encoding="utf-8")
    assert "SEARCH_API_FUZZY_TIMEOUT=300" not in text
    assert "SEARCH_API_PARALLELS_TIMEOUT=300" not in text
    assert "SEARCH_API_FUZZY_TIMEOUT=110" in text
    assert "SEARCH_API_PARALLELS_TIMEOUT=110" in text


def test_the_guide_explains_why_not_300():
    """A bare number invites someone to "fix" it back. The reasoning has to be
    next to it, including the coupling to the client-side timeout."""
    text = ENV_VARS_MD.read_text(encoding="utf-8")
    assert "WHY 110 AND NOT 300" in text
    assert "125.2" in text and "97.0" in text, (
        "the measured success/giveup pair is what makes 110 a reasoned choice "
        "rather than a magic number"
    )
    assert "130" in text, (
        "the guide must mention that the bundled client stops at 130 s, so "
        "raising the server ceiling alone accomplishes nothing"
    )


def test_client_timeout_stays_above_the_server_ceiling():
    """The skill client's socket timeout is an INVARIANT relative to the
    server's heaviest ceiling: it must be the larger, so the server's 504
    envelope arrives instead of a client-side socket timeout. If someone raises
    the server ceiling without raising the client, this catches it."""
    from web.search_api import DEFAULT_FUZZY_TIMEOUT, DEFAULT_PARALLELS_TIMEOUT
    heaviest = max(DEFAULT_FUZZY_TIMEOUT, DEFAULT_PARALLELS_TIMEOUT)

    for rel in ("skills/cairo-genizah-research/scripts/search.py",
                "skills/cairo-genizah-research/scripts/parallels.py"):
        src = (REPO_ROOT / rel).read_text(encoding="utf-8")
        m = re.search(r"timeout:\s*float\s*=\s*([0-9.]+)", src)
        assert m, f"no `timeout: float = N` default found in {rel}"
        client_timeout = float(m.group(1))
        assert client_timeout > heaviest, (
            f"{rel} waits {client_timeout}s but the server's heaviest ceiling "
            f"is {heaviest}s. The client must wait LONGER, or it aborts with a "
            f"socket timeout instead of receiving the server's documented "
            f"core_timeout envelope."
        )
