# -*- coding: utf-8 -*-
"""Tests for scripts/check_docs.py's relaxed 'Last updated' staleness regex
(2026-09-08, part of the external MCP-server incident doc fix, P1).

Imports the REAL regex from scripts/check_docs.py rather than copying it into
the test — a copy would let the two drift apart silently and prove nothing
about the actual script's behavior.
"""
import scripts.check_docs as check_docs


def test_matches_bold_last_updated_form():
    """The exact form this repo's docs actually use:
    '**Last Updated:** 2026-09-08' (docs/SEARCH_API.md's header)."""
    m = check_docs.LAST_UPDATED_RE.search("**Last Updated:** 2026-09-08")
    assert m is not None, "relaxed regex failed to match the bold '**Last Updated:**' form"
    assert m.group(1) == "2026-09-08"


def test_matches_blockquote_bold_form():
    m = check_docs.LAST_UPDATED_RE.search("> **Last Updated:** 2026-09-08")
    assert m is not None
    assert m.group(1) == "2026-09-08"


def test_matches_plain_colon_form():
    m = check_docs.LAST_UPDATED_RE.search("Last updated: 2026-09-08")
    assert m is not None
    assert m.group(1) == "2026-09-08"


def test_negative_gap_too_wide_does_not_match():
    """'Last updated' followed by unrelated prose before an unrelated date must
    NOT match — the bounded {0,6} gap exists precisely to stop the regex from
    skipping past prose to grab a later, unrelated date."""
    content = "Last updated sometime around 2026-09-08 during the migration."
    m = check_docs.LAST_UPDATED_RE.search(content)
    assert m is None, (
        f"regex matched across a >6-char gap of unrelated prose: {m.group(0)!r}"
    )


def test_negative_date_on_a_different_line_does_not_match():
    """The label and its date must be on the SAME line — the character class
    is [ \\t:*_-] (explicitly NOT \\s), so a newline between them must not
    bridge the match."""
    content = "Last updated:\n2026-09-08"
    m = check_docs.LAST_UPDATED_RE.search(content)
    assert m is None, (
        f"regex matched across a newline between the label and the date: {m.group(0)!r}"
    )


def test_label_probe_still_finds_label_in_negative_cases():
    """Sanity check that the negative cases above are genuinely 'label present,
    date not adjacent' rather than 'no label at all' — otherwise the negative
    result would be meaningless (nothing to fail to parse)."""
    for content in (
        "Last updated sometime around 2026-09-08 during the migration.",
        "Last updated:\n2026-09-08",
    ):
        assert check_docs.LAST_UPDATED_LABEL_RE.search(content) is not None, (
            f"LAST_UPDATED_LABEL_RE unexpectedly found no label in {content!r} — "
            f"the negative case above proves nothing if there is no label to begin with"
        )
