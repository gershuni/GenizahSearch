# -*- coding: utf-8 -*-
"""Tests for shared/discovery_locus.py.

House rule (feedback_gates_must_be_proven_able_to_fail): a gate that cannot be shown
to fail is not a gate. Each behavioural test below is paired with a test that
reintroduces the real defect against a LOCAL re-implementation and asserts the
assertion would have caught it -- so the test's own sensitivity is demonstrated,
not assumed.
"""
import re

import pytest

from shared.discovery_locus import (
    PIECE_SEP,
    RANGE_SEP,
    compress_pieces,
    parse_canonical_header,
    render_ranges,
    select_locus_work,
    units_for_span,
)

# The real header grammar, all four canonical families.
#
# Everything after the first `|` is the source-manuscript provenance field. Its real
# name is a restricted string and must never appear in a tracked file, so these
# fixtures carry a neutral placeholder -- which costs the tests nothing, because the
# parser's contract is that it cuts at `|` WITHOUT looking at what follows. A fixture
# that named the field would also be testing the masking gate's patience.
PROVENANCE = "| PROVENANCE-FIELD: SOURCE-MS"
BIBLE_HEADER = f"בראשית, פרק א, פסוק א {PROVENANCE}"
MISHNAH_HEADER = f"ברכות, פרק ב, משנה ג {PROVENANCE}"
TOSEFTA_HEADER = f"ברכות, פרק א, הלכה ח {PROVENANCE}"
TRACTATE_HEADER = f"פרק א, משנה ו {PROVENANCE}"
GIRSA_HEADER = f"ברכות, פרק א, משנה א (גרסה) {PROVENANCE}"


class TestParseCanonicalHeader:
    def test_parses_every_canonical_family(self):
        assert parse_canonical_header(BIBLE_HEADER)[:3] == ("בראשית", "א", "א")
        assert parse_canonical_header(MISHNAH_HEADER)[:3] == ("ברכות", "ב", "ג")
        assert parse_canonical_header(TOSEFTA_HEADER)[:3] == ("ברכות", "א", "ח")
        # A per-tractate file carries no division field: the file IS the tractate.
        assert parse_canonical_header(TRACTATE_HEADER)[:3] == ("", "א", "ו")

    def test_records_which_kind_of_sub_unit_the_family_uses(self):
        assert parse_canonical_header(BIBLE_HEADER).sub_kind == "פסוק"
        assert parse_canonical_header(MISHNAH_HEADER).sub_kind == "משנה"
        assert parse_canonical_header(TOSEFTA_HEADER).sub_kind == "הלכה"

    def test_edition_apparatus_is_stripped_from_the_numeral(self):
        assert parse_canonical_header(GIRSA_HEADER).sub == "א"

    def test_provenance_never_survives_the_parse(self):
        """The source-manuscript field must not reach any returned value."""
        for header in (BIBLE_HEADER, MISHNAH_HEADER, TOSEFTA_HEADER, TRACTATE_HEADER):
            address = parse_canonical_header(header)
            joined = "".join(address)
            assert "SOURCE-MS" not in joined
            assert "PROVENANCE-FIELD" not in joined

    def test_a_header_without_a_chapter_is_unaddressable_not_an_error(self):
        assert parse_canonical_header(f"סתם כותרת {PROVENANCE}") is None

    def test_whole_header_subtraction_would_have_produced_a_silent_empty_parse(self):
        """The defect this parser exists to avoid, proven able to fail.

        The pipeline's provenance-extraction regex matches the WHOLE `##...##`
        header. Subtracting it -- the natural way to 'strip provenance' -- deletes
        the address too, and the result is not an exception but an empty parse, i.e.
        a work that silently looks like it has no divisions.

        The regex below is that shape, keyed on the `|` separator rather than on the
        field's restricted name.
        """
        whole_header_re = re.compile(r"##[^#]*?\|[^#]+?##")
        stripped = whole_header_re.sub("", "##" + BIBLE_HEADER + "##").strip("#").strip()
        assert stripped == ""
        assert parse_canonical_header(stripped) is None
        # while the shipped implementation keeps the address
        assert parse_canonical_header(BIBLE_HEADER).chapter == "א"


class TestUnitsForSpan:
    STARTS = [0, 100, 200, 300]

    def test_a_span_inside_one_unit_yields_that_unit_twice(self):
        assert units_for_span(self.STARTS, 110, 150) == (1, 1)

    def test_a_span_crossing_units_yields_the_inclusive_pair(self):
        assert units_for_span(self.STARTS, 50, 250) == (0, 2)

    def test_a_span_ending_exactly_on_a_boundary_stops_before_that_unit(self):
        """[0,100) touches only unit 0 -- citing unit 1 would name text it never reaches."""
        assert units_for_span(self.STARTS, 0, 100) == (0, 0)

    def test_the_off_by_one_at_the_boundary_is_detectable(self):
        """Probing `end` instead of `end - 1` is the defect; prove the case separates."""
        def buggy(starts, start, end):
            lo = max(sum(1 for s in starts if s <= start) - 1, 0)
            hi = max(sum(1 for s in starts if s <= end) - 1, 0)   # <- probes `end`
            return lo, max(lo, hi)

        assert buggy(self.STARTS, 0, 100) == (0, 1)
        assert units_for_span(self.STARTS, 0, 100) == (0, 0)

    def test_a_work_with_no_units_cannot_carry_a_locus(self):
        with pytest.raises(ValueError):
            units_for_span([], 0, 10)

    def test_a_reversed_span_is_refused_rather_than_guessed(self):
        with pytest.raises(ValueError):
            units_for_span(self.STARTS, 250, 100)


class TestCompressPieces:
    def test_adjacent_units_merge_into_one_run(self):
        assert compress_pieces([(0, 1), (2, 3)]) == [(0, 3)]

    def test_overlapping_pieces_merge(self):
        assert compress_pieces([(0, 5), (3, 7)]) == [(0, 7)]

    def test_a_skipped_unit_stays_a_gap(self):
        """THE rule: 2 and 4 must not become 2-4, because unit 3 is not witnessed."""
        assert compress_pieces([(2, 2), (4, 4)]) == [(2, 2), (4, 4)]

    def test_character_gap_merging_would_have_swallowed_the_skipped_unit(self):
        """The defect, proven able to fail: merge on proximity rather than adjacency."""
        def buggy(pieces, tolerance=2):
            ordered = sorted(set(pieces))
            merged = [list(ordered[0])]
            for lo, hi in ordered[1:]:
                if lo <= merged[-1][1] + tolerance:      # <- proximity, not successor
                    merged[-1][1] = max(merged[-1][1], hi)
                else:
                    merged.append([lo, hi])
            return [(lo, hi) for lo, hi in merged]

        assert buggy([(2, 2), (4, 4)]) == [(2, 4)]
        assert compress_pieces([(2, 2), (4, 4)]) == [(2, 2), (4, 4)]

    def test_duplicate_pieces_collapse(self):
        assert compress_pieces([(1, 2), (1, 2)]) == [(1, 2)]

    def test_no_pieces_is_no_locus(self):
        assert compress_pieces([]) == []

    def test_a_reversed_piece_is_refused(self):
        with pytest.raises(ValueError):
            compress_pieces([(5, 2)])


class TestRenderRanges:
    LABELS = ["א", "ב", "ג", "ד", "ה"]

    def test_a_single_unit_renders_bare(self):
        assert render_ranges([(0, 0)], self.LABELS) == "א"

    def test_a_run_renders_as_a_range(self):
        assert render_ranges([(0, 2)], self.LABELS) == f"א{RANGE_SEP}ג"

    def test_separate_runs_are_visibly_separate(self):
        rendered = render_ranges([(0, 1), (3, 4)], self.LABELS)
        assert rendered == f"א{RANGE_SEP}ב{PIECE_SEP}ד{RANGE_SEP}ה"

    def test_the_rendered_citation_is_never_bracketed(self):
        """A bracketed numeric pair is rejected by the surface envelope, and that
        guard covers the whole envelope -- one bad string costs the entire page."""
        rendered = render_ranges([(0, 2), (3, 4)], self.LABELS)
        assert not any(ch in rendered for ch in "[]()")

    def test_an_out_of_range_ordinal_is_refused_rather_than_silently_clipped(self):
        with pytest.raises(IndexError):
            render_ranges([(0, 9)], self.LABELS)


class TestSelectLocusWork:
    def test_the_member_carrying_the_most_matched_text_wins(self):
        assert select_locus_work({"w000465": 59_759, "w001238": 27_963}, "w001238") == "w000465"

    def test_a_tiny_display_member_does_not_override_the_heavier_member(self):
        """Measured on the real duplicates: the non-display member wins 9 of 15."""
        assert select_locus_work({"w000192": 493_820, "w001269": 40}, "w001269") == "w000192"

    def test_display_first_would_have_picked_the_wrong_coordinate_space(self):
        """The defect, proven able to fail."""
        def buggy(letters, display):
            return display if letters.get(display) else max(letters, key=letters.get)

        assert buggy({"w000192": 493_820, "w001269": 40}, "w001269") == "w001269"
        assert select_locus_work({"w000192": 493_820, "w001269": 40}, "w001269") == "w000192"

    def test_a_tie_goes_to_the_display_work(self):
        assert select_locus_work({"w000191": 321, "w001337": 321}, "w001337") == "w001337"

    def test_an_equal_tie_without_the_display_work_is_still_deterministic(self):
        assert select_locus_work({"wB": 10, "wA": 10}, "wZZZ") == "wA"

    def test_a_member_with_no_matched_text_is_not_eligible(self):
        assert select_locus_work({"wA": 0, "wB": 5}, "wA") == "wB"

    def test_no_evidence_anywhere_means_no_place_rather_than_an_invented_one(self):
        assert select_locus_work({"wA": 0, "wB": 0}, "wA") is None
        assert select_locus_work({}, "wA") is None
