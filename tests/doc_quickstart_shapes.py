# -*- coding: utf-8 -*-
"""Helper: read the response shapes docs/SEARCH_API.md's Quick Start advertises.

Not a test module (no `test_` prefix, so pytest does not collect it). The
assertions live in tests/test_search_api.py and tests/test_browse_api.py, where
fixtures that build a REAL response body already exist -- comparing the doc
against a real body is the only comparison worth making.

Why this exists: on 2026-09-08 every one of the three Quick Start examples was
found to be materially wrong against production. The browse example invented a
`manuscript{shelfmark,library_code}` / `page{text,text_source,image_url}`
nesting that the endpoint has never returned; the search example advertised a
`rank` field that does not exist; the parallels example showed top-level
`sys_id` and `matched_chunks` instead of `locator` and `matches`. An integrator
copying any of them writes a client that breaks on first contact -- which is
exactly how this project lost a third-party MCP author's trust in the same
document a few weeks earlier.

Nothing catches that class of error except comparing the document to a real
response, so that is what the tests do.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SEARCH_API_MD = REPO_ROOT / "docs" / "SEARCH_API.md"

# The Quick Start subsection heading that precedes each endpoint's example.
_SECTIONS = {
    "search": "### Search for manuscripts",
    "browse": "### Drill down to a manuscript page",
    "parallels": "### Find composition parallels",
}


def _quick_start_region() -> str:
    text = SEARCH_API_MD.read_text(encoding="utf-8")
    start = text.index("## Quick Start")
    end = text.index("### Error responses", start)
    return text[start:end]


def documented_example(endpoint: str) -> dict:
    """The parsed JSON response example the Quick Start shows for `endpoint`."""
    if endpoint not in _SECTIONS:
        raise KeyError(f"unknown endpoint {endpoint!r}; expected {sorted(_SECTIONS)}")
    region = _quick_start_region()
    heading = _SECTIONS[endpoint]
    if heading not in region:
        raise AssertionError(
            f"Quick Start no longer contains {heading!r}. If the section was "
            f"renamed, update tests/doc_quickstart_shapes.py::_SECTIONS -- do "
            f"not delete the assertion, it is the only thing comparing this "
            f"document's examples to a real response."
        )
        # (unreachable; kept explicit so the failure message is the guidance)
    chunk = region[region.index(heading):]
    for other in _SECTIONS.values():
        if other != heading and other in chunk:
            chunk = chunk[:chunk.index(other)]
    blocks = re.findall(r"```json\s*\n(.*?)\n```", chunk, re.DOTALL)
    if not blocks:
        raise AssertionError(f"no ```json example under {heading!r}")
    # The response example is the last JSON block in the subsection (a request
    # body block, if one is ever added, would come first).
    return json.loads(blocks[-1])


def key_paths(obj, prefix: str = "") -> set:
    """Every dotted key path in a parsed example.

    A list is descended through its FIRST element only and the index is
    dropped, so `results[0].uid` reads as `results.uid` -- the example shows one
    representative row and that is what the real response must match.
    """
    paths = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            paths.add(path)
            paths |= key_paths(v, path)
    elif isinstance(obj, list) and obj:
        paths |= key_paths(obj[0], prefix)
    return paths


def missing_from(body: dict, endpoint: str) -> set:
    """Documented key paths that a real response body does NOT provide.

    Empty set == the document does not promise anything the endpoint withholds.
    The reverse direction is deliberately NOT checked: an example is allowed to
    be a truncated view of a larger response, and every one of them says so.
    """
    documented = key_paths(documented_example(endpoint))
    actual = key_paths(body)
    return {p for p in documented if p not in actual}
