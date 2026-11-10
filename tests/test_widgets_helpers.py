# -*- coding: utf-8 -*-
"""
Unit tests for desktop/widgets.py module-level helpers.

Focus: ``_get_initial_image_index`` semantic-disambiguation refactor
(260421-aln follow-up). The old single-parameter form conflated a
1-indexed transcription page number with a folio number (extracted
from shelfmark), producing wrong canvases on manuscripts where
transcription page N != folio N — e.g. T-S NS 158.112, whose 14
transcription pages map to folio labels ``1r, 1v, 2r, 2v, ..., 8v``
while CUDL exposes 12 canvases labeled ``1r..6v``.
"""

from desktop.widgets import _get_folio_image_index, _get_initial_image_index


def _cudl_158_112_images():
    """CUDL canvases for T-S NS 158.112 (folios 1r..6v as 12 entries)."""
    folios = [(i, 'r' if s == 0 else 'v') for i in range(1, 7) for s in (0, 1)]
    return [
        {'label': f'{f}{s}', 'url': f'https://x/{f}{s}', 'folio_num': f, 'folio_side': s}
        for f, s in folios
    ]


class TestFolioNumSearch:
    """When the caller has extracted a folio from a shelfmark (e.g.
    ``T-S 12.34.2`` → 2), ``_get_initial_image_index`` should locate the
    canvas whose ``folio_num`` matches.
    """

    def test_exact_folio_match(self):
        idx = _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()}, folio_num=3,
        )
        # folio 3 starts at index 4 (1r,1v,2r,2v,3r,3v — 3r at index 4)
        assert idx == 4

    def test_folio_match_beyond_list_returns_nearest_prior(self):
        idx = _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()}, folio_num=99,
        )
        # No canvas with folio 99 → largest prior folio is 6 → last 6*
        # entry (index 10 = 6r since iteration stops at the first match
        # with max folio, recto comes first in our list).
        assert idx == 10

    def test_folio_match_missing_falls_to_first(self):
        """Folio 0 is below all entries → returns min folio entry (index 0)."""
        idx = _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()}, folio_num=0,
        )
        assert idx == 0

    def test_positional_arg_still_works_as_folio(self):
        """Backward-compat: second positional arg is ``folio_num``."""
        idx = _get_initial_image_index({'images_ext': _cudl_158_112_images()}, 3)
        assert idx == 4


class TestPageNumPositional:
    """When the caller only has a 1-indexed transcription page number
    (no folio extracted from shelfmark), ``page_num=N`` should return
    ``max(N - 1, 0)`` — positional, not a folio search.

    Regression for the T-S NS 158.112 bug where the caller fell back to
    ``current_browse_p`` and the function searched ``images_ext`` for
    ``folio_num == current_browse_p``, returning the wrong canvas.
    """

    def test_page_3_returns_index_2(self):
        """User on transcription page 3 (label '2r' on T-S NS 158.112).
        Positional index 2 → CUDL canvas 2r. Previously returned 4 (3r)."""
        idx = _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()}, page_num=3,
        )
        assert idx == 2

    def test_page_1_returns_index_0(self):
        idx = _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()}, page_num=1,
        )
        assert idx == 0

    def test_page_0_clamped_to_0(self):
        idx = _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()}, page_num=0,
        )
        assert idx == 0

    def test_page_num_ignores_folio_metadata(self):
        """Even with folio_num fields populated on the image list,
        page_num should NOT trigger folio search."""
        idx = _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()}, page_num=3,
        )
        # Positional: page 3 → index 2 (2r). NOT index 4 (3r).
        assert idx == 2


class TestNoneInputs:
    def test_both_none_returns_zero(self):
        assert _get_initial_image_index({'images_ext': _cudl_158_112_images()}) == 0

    def test_folio_num_none_page_num_none_returns_zero(self):
        assert _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()},
            folio_num=None, page_num=None,
        ) == 0

    def test_folio_num_takes_priority_when_both_given(self):
        """When both are given (caller ambiguity), folio_num wins."""
        idx = _get_initial_image_index(
            {'images_ext': _cudl_158_112_images()},
            folio_num=3, page_num=1,
        )
        assert idx == 4  # folio 3 → index 4, NOT page 1 → index 0


class TestFolioNumFallbackToPositional:
    """When ``folio_num`` is given but the image list has no folio metadata,
    the function falls back to ``max(folio_num - 1, 0)`` — legacy behavior
    preserved for manuscripts with unparseable or absent folio labels."""

    def test_empty_image_list(self):
        assert _get_initial_image_index({'images_ext': []}, folio_num=3) == 2

    def test_images_without_folio_num(self):
        images = [{'label': 'Img 1', 'url': 'x'}, {'label': 'Img 2', 'url': 'y'}]
        assert _get_initial_image_index({'images_ext': images}, folio_num=2) == 1

    def test_no_meta(self):
        assert _get_initial_image_index(None, folio_num=5) == 4


class TestGetFolioImageIndexPageNum:
    """``_get_folio_image_index(..., page_num=N)`` — navigation path.

    Regression for T-S NS 158.112 CUDL-switch bug: when the shelfmark has
    no folio and the caller falls back to ``current_browse_p``, the helper
    must use positional indexing (page_num - 1) rather than searching
    images_ext by folio. side_offset is ignored in the positional branch
    because the page number already encodes recto/verso.
    """

    def test_page_num_positional_ignores_side_offset(self):
        """User on transcription page 2 (label '1v'): page_num=2 should
        return idx=1 (CUDL 1v), NOT idx=3 (CUDL 2v) which would come from
        folio search + side_offset advancement."""
        idx = _get_folio_image_index(
            {'images_ext': _cudl_158_112_images()},
            folio_num=None, side_offset=1, page_num=2,
        )
        assert idx == 1  # CUDL 1v

    def test_page_num_clamps_past_list_end(self):
        """Page 20 on a 12-canvas list → clamp to last index (11)."""
        idx = _get_folio_image_index(
            {'images_ext': _cudl_158_112_images()},
            folio_num=None, side_offset=0, page_num=20,
        )
        assert idx == 11

    def test_folio_num_still_drives_side_offset(self):
        """When folio_num IS given, the page_num kwarg is ignored and the
        existing side_offset logic runs."""
        idx = _get_folio_image_index(
            {'images_ext': _cudl_158_112_images()},
            folio_num=2, side_offset=1, page_num=99,
        )
        # folio 2 at idx 2; side_offset=1 advances to idx 3 (2v)
        assert idx == 3

    def test_matches_t_s_ns_158_112_all_cudl_pages(self):
        """End-to-end: for each transcription page 1..12 on T-S NS 158.112,
        switching to CUDL should show the corresponding canvas 1r..6v.
        Previously page 2+ showed the NEXT folio's canvas due to the bug."""
        cudl = _cudl_158_112_images()
        labels = ['1r', '1v', '2r', '2v', '3r', '3v', '4r', '4v', '5r', '5v', '6r', '6v']
        for page_num, expected_label in enumerate(labels, start=1):
            side_offset = 1 if page_num % 2 == 0 else 0
            idx = _get_folio_image_index(
                {'images_ext': cudl},
                folio_num=None, side_offset=side_offset, page_num=page_num,
            )
            assert cudl[idx]['label'] == expected_label, (
                f"page {page_num} expected {expected_label}, got {cudl[idx]['label']} at idx {idx}"
            )
