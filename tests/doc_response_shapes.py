# -*- coding: utf-8 -*-
"""Helper: read the response shapes docs/SEARCH_API.md advertises.

Not a test module (no `test_` prefix, so pytest does not collect it). The
assertions live in tests/test_search_api.py, tests/test_browse_api.py,
tests/test_parallels_api.py and tests/test_capabilities_api.py, where fixtures
that build a REAL response body already exist -- comparing the doc against a
real body is the only comparison worth making.

WHY THIS EXISTS, and why it now covers more than the Quick Start
----------------------------------------------------------------
On 2026-09-08 all three Quick Start examples were found to be materially wrong
against production: browse invented a `manuscript{...}`/`page{...}` nesting,
search advertised a `rank` field, parallels showed `sys_id`/`matched_chunks`
instead of `locator`/`matches`. This helper was written that day to stop it
recurring -- but it only ever read the Quick Start.

A Codex review immediately found the obvious hole: the endpoint-level
REFERENCE sections were still wrong, in the same file, a few hundred lines
down. Between them they promised a nonexistent `metadata` object on search
items, `aggregate_score` on parallels items, flat `library_code`/`library_name`
on browse, an `fl_id` key in the search/parallels locator, an integer `p_num`
where the serializer emits a string, `metadata.pgp/fjms/nli` sub-objects whose
every key was wrong, and `image.sources` as a list of strings when it is a list
of objects. A gate that checks 3 of 8 examples reads as "the examples are
checked" and is worse than no gate at all.

So the unit of coverage is now EVERY response example in the document, located
by an explicit anchor. `assert_all_examples_are_covered()` fails if a new
example appears that nothing checks, which is the only way this file stays
honest as the document grows.

Direction of the check, deliberately asymmetric
-----------------------------------------------
Only DOCUMENTED-BUT-ABSENT fails. An example is allowed to be a truncated view
of a larger response, and every one of them says so -- adding the reverse
direction would redden the build every time a serializer gains a field, which
trains people to delete the assertion. The failure this gate exists to prevent
is an integrator writing `item['metadata']['library']` and getting a KeyError.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
SEARCH_API_MD = REPO_ROOT / "docs" / "SEARCH_API.md"

# Anchors, never positions. `blocks[-1]` was the original selector and it was
# only ever correct by accident: it does not establish that the block it
# returns IS the response, so a second example (or a trailing request body)
# would silently redirect the check or defeat it entirely.
_QUICK_START_HEADING = "## Quick Start"
_QUICK_START_END = "### Error responses"
_QS_ANCHOR = "Response shape (truncated):"
_REF_ANCHOR = "### Response example"

# Quick Start subsection heading per endpoint. Absent from this map == that
# endpoint has no Quick Start entry (capabilities does not).
_QS_SECTIONS = {
    "search": "### Search for manuscripts",
    "browse": "### Drill down to a manuscript page",
    "parallels": "### Find composition parallels",
}

# The `## Endpoint: ...` heading that opens each endpoint's reference section.
_REF_SECTIONS = {
    "search": "## Endpoint: POST /api/search",
    "browse": "## Endpoint: GET /api/browse",
    "parallels": "## Endpoint: POST /api/parallels",
    "capabilities": "## Endpoint: GET /api/capabilities",
}

ENDPOINTS = tuple(sorted(_REF_SECTIONS))

# Every (endpoint, which) pair a test must cover. Pinned so that adding an
# example to the document cannot silently go unchecked -- see
# assert_all_examples_are_covered().
EXPECTED_EXAMPLES = tuple(
    [(e, "quick_start") for e in sorted(_QS_SECTIONS)]
    + [(e, "reference") for e in sorted(_REF_SECTIONS)]
)

# JSON blocks in the file that are deliberately NOT full response-shape
# examples. Each needle is matched against ONE block's own body (never the
# whole file), so two blocks cannot be accounted for by the same entry -- and
# `assert_all_examples_are_covered` names any block matching nothing, by line
# number, rather than reporting a bare count.
#
# Each entry says why it is exempt. A REQUEST body cannot be compared with a
# response; an error envelope is not a success shape; the multi-witness and
# locator blocks are partial fragments (a handful of keys shown in isolation)
# with no fixture that produces them today. Naming them here is the honest
# alternative to a bare `continue`: the exemption is on the record, and a NEW
# block matching none of them reddens the build.
_NON_RESPONSE_BLOCKS = (
    ('"code": "rate_limited"', "Quick Start error-response example"),
    ('"query": "rambam"', "search request example (non-Responsa)"),
    ('"responsa_options": {', "search request example (Responsa)"),
    ('"request": {\n    "search_mode": "responsa"', "search worked-echo fragment"),
    ('"text": "<the composition text', "parallels request example"),
    ('"method": "passage",\n  "witnesses": [', "multi-witness request example"),
    ('"witness_fusion": {', "multi-witness response fragment (no fixture yet)"),
    ('"sys_id": "990001234560205171",\n  "volume_ie"', "drill-down locator fragment"),
    ('"code": "invalid_combination"', "error envelope example"),
    ('"warnings": ["query_downgraded', "warnings-array worked example"),
)


def _text() -> str:
    # newline='' is NOT used: the file is CRLF on disk and Path.read_text
    # normalizes to \n, which is what the anchors below are written against.
    # Never write this file back through read_text/write_text -- that
    # round-trip destroys the CRLF line endings (it did, once).
    return SEARCH_API_MD.read_text(encoding="utf-8")


def _quick_start_region(text: str) -> str:
    start = text.index(_QUICK_START_HEADING)
    return text[start:text.index(_QUICK_START_END, start)]


def _section(endpoint: str, which: str) -> str:
    """The slice of the document that owns `endpoint`'s `which` example."""
    text = _text()
    if which == "quick_start":
        if endpoint not in _QS_SECTIONS:
            raise KeyError(
                "%r has no Quick Start example; expected one of %s"
                % (endpoint, sorted(_QS_SECTIONS))
            )
        region = _quick_start_region(text)
        heading = _QS_SECTIONS[endpoint]
        if heading not in region:
            raise AssertionError(
                "Quick Start no longer contains %r. If the section was renamed, "
                "update tests/doc_response_shapes.py::_QS_SECTIONS -- do not "
                "delete the assertion, it is the only thing comparing this "
                "document's examples to a real response." % heading
            )
        chunk = region[region.index(heading):]
        for other in _QS_SECTIONS.values():
            if other != heading and other in chunk:
                chunk = chunk[:chunk.index(other)]
        return chunk

    if which != "reference":
        raise KeyError("unknown example kind %r; expected quick_start|reference" % which)

    heading = _REF_SECTIONS.get(endpoint)
    if heading is None:
        raise KeyError("unknown endpoint %r; expected %s" % (endpoint, list(ENDPOINTS)))
    if heading not in text:
        raise AssertionError(
            "docs/SEARCH_API.md no longer contains %r. If the heading changed, "
            "update tests/doc_response_shapes.py::_REF_SECTIONS." % heading
        )
    chunk = text[text.index(heading):]
    # Stop at the next top-level section so a later endpoint's example cannot
    # be mistaken for this one.
    nxt = chunk.find("\n## ", 1)
    return chunk if nxt == -1 else chunk[:nxt]


def documented_example(endpoint: str, which: str = "quick_start") -> dict:
    """The parsed JSON response example the document shows for `endpoint`.

    Selection is by ANCHOR, and fails loud in both directions: no anchor means
    the document was reorganized without carrying the label along, and two
    anchors mean the choice is ambiguous. Neither silently checks nothing --
    that is the failure mode this replaced.
    """
    chunk = _section(endpoint, which)
    anchor = _QS_ANCHOR if which == "quick_start" else _REF_ANCHOR
    found = chunk.count(anchor)
    if found == 0:
        raise AssertionError(
            "%s/%s: no %r label found. The response example is located by that "
            "label, not by position. Restore it (or update the anchor in "
            "tests/doc_response_shapes.py) -- do not delete this check."
            % (endpoint, which, anchor)
        )
    if found > 1:
        raise AssertionError(
            "%s/%s: %d %r labels in one section, so which block is 'the' "
            "response example is ambiguous. Give the extra example a distinct "
            "label and add it to EXPECTED_EXAMPLES."
            % (endpoint, which, found, anchor)
        )
    after = chunk[chunk.index(anchor) + len(anchor):]
    blocks = re.findall(r"```json\s*\n(.*?)\n```", after, re.DOTALL)
    if not blocks:
        raise AssertionError(
            "%s/%s: the %r label is present but no ```json block follows it."
            % (endpoint, which, anchor)
        )
    # The FIRST block after the anchor, not the last: the anchor names it.
    return json.loads(blocks[0])


def key_paths(obj, prefix: str = "") -> set:
    """Every dotted key path in a parsed example.

    A list is descended through its FIRST element and the index dropped, so
    `results[0].uid` reads as `results.uid` -- the example shows one
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


def _unexpanded(obj, prefix: str = "") -> set:
    """Paths the real body carries but that hold nothing to compare against.

    `null`, `[]` and `{}` are all "present, with no children". A documented
    sub-shape underneath one of these is NOT a broken promise: `metadata.pgp`
    is legitimately null when PGP has nothing for a page, and the document
    still needs to say what it looks like when it is populated. Without this,
    documenting any nullable sub-object would redden the build.
    """
    out = set()
    if isinstance(obj, dict):
        for k, v in obj.items():
            path = f"{prefix}.{k}" if prefix else k
            if v is None or (isinstance(v, (list, dict, str)) and not v):
                out.add(path)
            else:
                out |= _unexpanded(v, path)
    elif isinstance(obj, list) and obj:
        out |= _unexpanded(obj[0], prefix)
    return out


def missing_from(body: dict, endpoint: str, which: str = "quick_start") -> set:
    """Documented key paths that a real response body does NOT provide.

    Empty set == the document does not promise anything the endpoint withholds.
    Paths under an empty/null parent in the real body are exempt (see
    _unexpanded); the reverse direction is deliberately not checked (see the
    module docstring).
    """
    documented = key_paths(documented_example(endpoint, which))
    actual = key_paths(body)
    unexpanded = _unexpanded(body)

    def exempt(path: str) -> bool:
        parts = path.split(".")
        return any(".".join(parts[:i]) in unexpanded for i in range(1, len(parts)))

    return {p for p in documented if p not in actual and not exempt(p)}


def assert_all_examples_are_covered() -> None:
    """Every ```json block in the document is either checked or named exempt.

    This is the part that keeps the gate honest as the file grows. Without it,
    someone adds a fifth endpoint's response example, nothing checks it, and
    the build still passes -- which is exactly how the endpoint reference
    sections stayed wrong while three Quick Start examples were green.
    """
    text = _text()

    # Every fenced json block in the file, with the line it starts on.
    blocks = [
        (text[:m.start()].count("\n") + 1, m.group(1))
        for m in re.finditer(r"```json\s*\n(.*?)\n```", text, re.DOTALL)
    ]

    # The bodies the anchored selector actually returns. Compared by parsed
    # content, so a block is "checked" only if a test can really reach it.
    checked_bodies = []
    for endpoint, which in EXPECTED_EXAMPLES:
        checked_bodies.append(documented_example(endpoint, which))  # raises if not locatable

    unaccounted = []
    for line_no, raw in blocks:
        try:
            parsed = json.loads(raw)
        except ValueError:
            parsed = None
        if parsed is not None and any(parsed == b for b in checked_bodies):
            continue
        if any(needle in raw for needle, _why in _NON_RESPONSE_BLOCKS):
            continue
        unaccounted.append(line_no)

    if unaccounted:
        raise AssertionError(
            "docs/SEARCH_API.md has %d ```json blocks; %d are checked against a "
            "real response body and the rest are named exempt, but the block(s) "
            "starting at line(s) %s match neither. If a new RESPONSE example was "
            "added, add it to EXPECTED_EXAMPLES so it is checked against a real "
            "body; if it is a request/fragment/error example, add a needle to "
            "_NON_RESPONSE_BLOCKS with the reason. Do not delete this check -- "
            "an unchecked example is how the endpoint reference sections stayed "
            "wrong for months while three Quick Start examples were green."
            % (len(blocks), len(checked_bodies),
               ", ".join(str(n) for n in unaccounted))
        )
