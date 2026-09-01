# -*- coding: utf-8 -*-
"""`serve_v3_review.clean_display_markers` — the review viewer's display-only
marker cleaning.

The viewer ships to a reviewer as ONE stdlib-only script, so it cannot import
the repo: its JA-marker rule is a copy of the live pipeline's
`scripts/bake_discovery_excerpts.py::clean_ja_markers` (owner ruling
2026-08-13). This test imports BOTH and asserts they agree, so the copy cannot
drift silently — and pins what must be kept, which matters more than what is
stripped: brackets and question marks are philology, not noise.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "scripts"))
from serve_v3_review import clean_display_markers  # noqa: E402

JA_CASES = [
    "+פסוק~ +כב~ נץ אלכלאם",
    "סוק~ +כב~ נץ אלכלאם",
    "אכר אלקול +פסו",
    "+פתח~ +דבר~ {בשמ' רחמ'}\nמקאלה",
    "רמב\"ע, העיונים\n+השירה~ +בימי~ +המלכות~ ואמא אן כאן",
    "no markers here at all",
]


def test_agrees_with_the_live_pipeline_on_ja_markers():
    """The two implementations of the SAME owner ruling must not diverge."""
    bake = pytest.importorskip("bake_discovery_excerpts",
                               reason="the live baker is not importable here")
    for text in JA_CASES:
        live = bake.clean_ja_markers(text)
        here = clean_display_markers(text)
        assert here == live, f"{text!r}: viewer {here!r} != live {live!r}"


def test_strips_the_verse_start_marker():
    """`>>` marks a verse start in the M-source text (197,982 rows); `>` is
    never a content character."""
    assert clean_display_markers("בָּךְ.\n\n>> וּלְאָדָם אָמַר") == (
        "בָּךְ.\n\nוּלְאָדָם אָמַר")
    assert clean_display_markers(">> פסוק") == "פסוק"
    assert clean_display_markers("<< סוף") == "סוף"


def test_keeps_the_philology():
    """Editorial brackets, restorations and uncertain readings are EVIDENCE.
    Stripping them would quietly hide what a scholar is here to weigh."""
    keep = "וַיְהִי אַךְ <יָצֹא יָצָא> בֹ?קר? {בשמ' רחמ'} עַל"
    assert clean_display_markers(keep) == keep
    assert clean_display_markers("נַעֲשֶׂה אָדָם בְּצַלְמֵ?נ?<וּ>") == (
        "נַעֲשֶׂה אָדָם בְּצַלְמֵ?נ?<וּ>")


def test_a_lone_angle_pair_is_not_a_section_mark():
    """`>>` only counts standing alone: `<וּ>` and `-->` must survive."""
    assert clean_display_markers("א<וּ>ב") == "א<וּ>ב"
    assert clean_display_markers("אב>>גד") == "אב>>גד"


def test_empty_and_none():
    assert clean_display_markers(None) is None
    assert clean_display_markers("") == ""
    assert clean_display_markers(7) == 7          # never crashes on a non-str


def test_offsets_are_not_the_cleaner_s_business():
    """A regression guard in words: this function runs on an ALREADY-CUT piece,
    so it may change the piece's length. Nothing may derive an offset from its
    output -- the stored offsets index the original text."""
    src = "+פסוק~ +כב~ נץ אלכלאם"
    out = clean_display_markers(src)
    assert len(out) < len(src)      # the point: display text is not a coordinate
