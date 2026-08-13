# -*- coding: utf-8 -*-
"""`scripts/bake_discovery_excerpts.py::clean_ja_markers` (2026-08-13).

The J-corpus per_doc files carry `+פסוק~ +כב~`-style structural markers
(label/value tokens: `+` prefix, `~` suffix). They leaked into the edition
pane raw (owner report). The cleaner strips them from DISPLAY pieces only --
the marker letters live inside the matcher's coordinate stream, so the bake
applies this AFTER slicing and BEFORE the word-highlight pass, and the
stream itself is never touched (these tests pin the function, not that
ordering; the ordering is pinned by the bake's own structure).

Grammar licence, measured over all 92 per_doc files: 1,743 distinct tokens,
and outside the grammar the corpus contains only three stray `+~` pairs --
`+` and `~` are never content characters.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from bake_discovery_excerpts import clean_ja_markers  # noqa: E402


def test_label_and_value_tokens_are_removed_inline_and_on_their_own_line():
    assert clean_ja_markers(
        "פקאל.\n+פרק~ +א~\n+פסוק~ +א~ אלחק פי קולה"
    ) == "פקאל.\nאלחק פי קולה"


def test_a_token_the_piece_slice_cut_at_the_start_is_removed():
    # The slice landed inside `+פסוק~`, leaving no opening `+`.
    assert clean_ja_markers("סוק~ +כב~ נץ אלכלאם") == "נץ אלכלאם"


def test_a_token_the_piece_slice_cut_at_the_end_is_removed():
    # The slice landed inside the token, leaving no closing `~`.
    assert clean_ja_markers("אכר אלקול +פסו") == "אכר אלקול"


def test_the_stray_empty_marker_is_removed_and_lines_collapse():
    # The one real out-of-grammar shape in the corpus (3 occurrences).
    assert clean_ja_markers("א[…]}\n         +~  \n\nעמא אתפק") == (
        "א[…]}\nעמא אתפק")


def test_text_without_markers_passes_through_unchanged():
    # Braces are the RENDERER's transform (ja_braces), never this one's.
    text = "נץ בלא סימנים {מלים עבריות} ושורה\nשניה"
    assert clean_ja_markers(text) == text


def test_none_and_empty_are_passed_through():
    assert clean_ja_markers(None) is None
    assert clean_ja_markers("") == ""
