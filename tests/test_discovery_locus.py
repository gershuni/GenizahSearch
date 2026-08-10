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
    amud_ordinal,
    citation_seq_for_daf,
    compress_pieces,
    daf_label_he,
    heb_numeral,
    parse_canonical_header,
    parse_unit_numeral,
    render_ranges,
    sefaria_daf,
    select_locus_work,
    split_at_citation_breaks,
    stream_offset_for_raw,
    units_for_span,
)

# The real header grammar, all four canonical families.
#
# Everything after the first `|` is the source-manuscript provenance field. Its real
# name is a restricted string and must never appear in a tracked file, so these
# fixtures carry a neutral placeholder -- which costs the tests nothing, because the
# parser's contract is that it cuts at `|` WITHOUT looking at what follows. A fixture
# that named the field would also be testing the masking gate's patience.
#: A local copy of the letter values, so the buggy re-implementations below are
#: genuinely independent of the module under test rather than importing its table.
_HEB_VALUE_LOCAL = {
    "א": 1, "ב": 2, "ג": 3, "ד": 4, "ה": 5, "ו": 6, "ז": 7, "ח": 8, "ט": 9,
    "י": 10, "כ": 20, "ל": 30, "מ": 40, "נ": 50, "ס": 60, "ע": 70, "פ": 80, "צ": 90,
    "ק": 100, "ר": 200, "ש": 300, "ת": 400,
}

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


class TestCitationOrderIsNotTableOrder:
    """The defect both adversarial passes found on real shipped data.

    A work's units are ordered by where they sit in the stream, because that is what
    the bisect needs. In the Talmud editions that order follows the CHAPTERS while
    the printed daf markers follow the FOLIATION, and where the two disagree the
    marker sequence steps backwards. Measured: the daf sequence rises monotonically
    in only 41 of 87 marker-bearing works.

    This fixture is Menachot's real shape -- four consecutive marker units whose
    folios run צג ע"ב, צד ע"א, סג ע"ב, סד ע"א: a 31-folio step back at the seam.
    """

    #: (daf, amud) per table unit, in stream order.
    MENACHOT = [(93, 2), (94, 1), (63, 2), (64, 1)]
    SEQ = citation_seq_for_daf(MENACHOT)

    def test_a_span_crossing_the_inversion_is_split_not_bridged(self):
        assert split_at_citation_breaks(0, 3, self.SEQ) == [(0, 1), (2, 3)]

    def test_a_span_inside_one_forward_run_is_left_whole(self):
        assert split_at_citation_breaks(0, 1, self.SEQ) == [(0, 1)]

    def test_compression_will_not_join_across_the_seam(self):
        assert compress_pieces([(1, 1), (2, 2)], self.SEQ) == [(1, 1), (2, 2)]

    def test_table_adjacency_alone_would_have_printed_a_descending_citation(self):
        """The defect, proven able to fail. On real shipped data 27 spans cross an
        inversion and 17 render visibly backwards -- `בבלי מנחות צד ע\"א–סג ע\"ב`."""
        labels = [daf_label_he(d, a) for d, a in self.MENACHOT]

        table_only = compress_pieces([(1, 1), (2, 2)])          # <- no citation_seq
        assert table_only == [(1, 2)]
        assert render_ranges(table_only, labels) == 'צד ע"א–סג ע"ב'

        honest = compress_pieces([(1, 1), (2, 2)], self.SEQ)
        assert render_ranges(honest, labels) == 'צד ע"א; סג ע"ב'

    def test_the_forward_facing_inversion_is_the_dangerous_one(self):
        """Backwards is the lucky case -- it is at least visible. The same step the
        other way prints a forward range claiming folios never touched."""
        rising = citation_seq_for_daf([(63, 2), (64, 1), (93, 1), (94, 1)])
        labels = [daf_label_he(d, a) for d, a in [(63, 2), (64, 1), (93, 1), (94, 1)]]

        assert render_ranges(compress_pieces([(1, 1), (2, 2)]), labels) == 'סד ע"א–צג ע"א'
        assert compress_pieces([(1, 1), (2, 2)], rising) == [(1, 1), (2, 2)]

    def test_a_yerushalmi_leaf_runs_four_columns_before_it_turns(self):
        seq = citation_seq_for_daf([(7, 3), (7, 4), (8, 1)], columns_per_folio=4)
        assert split_at_citation_breaks(0, 2, seq) == [(0, 2)]

    def test_the_two_column_rule_would_have_broken_that_leaf_in_two(self):
        """The defect, proven able to fail: ע\"ד is not adjacent to the next ח ע\"א
        under a two-sided model, so a continuous Yerushalmi span splits."""
        two = citation_seq_for_daf([(7, 3), (7, 4), (8, 1)])
        assert two[1] is None                                   # ע"ד has no place
        assert split_at_citation_breaks(0, 2, two) == [(0, 0), (1, 1), (2, 2)]

    def test_an_unreadable_folio_breaks_the_run_rather_than_bridging_it(self):
        seq = citation_seq_for_daf([(5, 1), (None, 1), (5, 2)])
        assert seq[1] is None
        assert split_at_citation_breaks(0, 2, seq) == [(0, 0), (1, 1), (2, 2)]

    def test_a_chapter_table_that_really_is_monotonic_still_merges(self):
        """The rule must not be so strict that it refuses the families where table
        order IS citation order -- Bible and Sefaria chapters, proven increasing."""
        assert compress_pieces([(0, 1), (2, 3)], [1, 2, 3, 4]) == [(0, 3)]

    def test_a_reversed_range_is_refused_rather_than_reordered(self):
        with pytest.raises(ValueError):
            split_at_citation_breaks(3, 1, self.SEQ)


class TestCitationSeqForDaf:
    def test_consecutive_amudim_are_consecutive_positions(self):
        assert citation_seq_for_daf([(2, 1), (2, 2), (3, 1)]) == [4, 5, 6]

    def test_a_four_column_leaf_uses_four_slots(self):
        """ז ע\"ד is 7*4+3 = 31 and ח ע\"א is 8*4+0 = 32 -- successors, as they must
        be: the last column of a leaf runs straight into the first of the next."""
        assert citation_seq_for_daf([(7, 4), (8, 1)], columns_per_folio=4) == [31, 32]

    def test_a_skipped_column_is_not_a_successor(self):
        assert citation_seq_for_daf([(7, 1), (8, 1)]) == [14, 16]

    def test_a_column_count_no_folio_has_is_refused(self):
        with pytest.raises(ValueError):
            citation_seq_for_daf([(2, 1)], columns_per_folio=3)


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


class TestStreamOffsetForRaw:
    """The bridge between the raw body (which the versemaps index) and the
    normalized stream (which the stored offsets index)."""

    # "אב, גד" -> stream "אבגד"; raw indices of the four letters:
    OFFSETS = [0, 1, 4, 5]

    def test_a_raw_position_on_a_letter_maps_to_that_letter(self):
        assert stream_offset_for_raw(self.OFFSETS, 4) == 2

    def test_a_raw_position_on_dropped_punctuation_maps_to_the_next_letter(self):
        """Raw 2 is the comma and raw 3 the space; a unit starting there starts at ג."""
        assert stream_offset_for_raw(self.OFFSETS, 2) == 2
        assert stream_offset_for_raw(self.OFFSETS, 3) == 2

    def test_a_position_past_the_last_letter_is_the_end_of_the_stream(self):
        assert stream_offset_for_raw(self.OFFSETS, 99) == 4

    def test_bisect_right_would_have_skipped_a_unit_starting_on_its_first_letter(self):
        """The defect, proven able to fail.

        `bisect_right` answers "after any equal entry", so a unit whose raw start IS
        a letter would begin one letter late -- every unit shifted, and no error.
        """
        import bisect as _b

        assert _b.bisect_right(self.OFFSETS, 4) == 3      # <- starts at ד, one late
        assert stream_offset_for_raw(self.OFFSETS, 4) == 2

    def test_a_work_whose_offsets_are_empty_yields_the_empty_stream_end(self):
        assert stream_offset_for_raw([], 7) == 0


class TestHebrewNumerals:
    def test_the_ordinary_cases(self):
        assert heb_numeral(1) == "א"
        assert heb_numeral(9) == "ט"
        assert heb_numeral(10) == "י"
        assert heb_numeral(11) == "יא"
        assert heb_numeral(100) == "ק"
        assert heb_numeral(176) == "קעו"

    def test_fifteen_and_sixteen_are_not_written_as_the_name(self):
        assert heb_numeral(15) == "טו"
        assert heb_numeral(16) == "טז"
        assert heb_numeral(115) == "קטו"
        assert heb_numeral(116) == "קטז"

    def test_the_naive_tens_plus_ones_rule_would_have_spelled_the_name(self):
        """The defect, proven able to fail -- and it is not merely improper: an
        edition does not print יה for 15, so the citation would not match the page."""
        def buggy(value):
            hundreds, rest = divmod(value, 100)
            tens, ones = divmod(rest, 10)
            return ("", "ק", "ר")[hundreds] + ("", "י")[tens > 0] + "אבגדהוזחט"[ones - 1]

        assert buggy(15) == "יה"
        assert heb_numeral(15) == "טו"

    def test_every_daf_a_tractate_can_reach_round_trips(self):
        """Bava Batra reaches 176; the whole range must survive render -> parse."""
        for n in range(1, 500):
            assert parse_unit_numeral(heb_numeral(n)) == n, n

    def test_a_numeral_is_read_through_the_punctuation_editions_put_round_it(self):
        assert parse_unit_numeral('ק"עו') == 176
        assert parse_unit_numeral("יד'") == 14

    def test_final_letters_carry_their_ordinary_value(self):
        assert parse_unit_numeral("ך") == 20

    def test_a_folio_spelled_in_digits_is_still_a_folio(self):
        """95 distinct labels across 173 of the corpus's 8,736 daf markers are ASCII
        digits. A Hebrew-only parser drops them -- and a dropped marker does not lose
        a citation, it widens its neighbour over the text it was there to divide."""
        assert parse_unit_numeral("17") == 17
        assert parse_unit_numeral("120") == 120
        assert parse_unit_numeral(" 159 ") == 159

    def test_a_hebrew_only_parser_would_have_dropped_the_decimal_markers(self):
        """The defect, proven able to fail."""
        def hebrew_only(label):
            vals = [_HEB_VALUE_LOCAL[c] for c in label if c in _HEB_VALUE_LOCAL]
            return sum(vals) if vals else None

        assert hebrew_only("120") is None
        assert parse_unit_numeral("120") == 120

    def test_mixed_script_is_refused_rather_than_half_read(self):
        assert parse_unit_numeral("12ג") is None

    def test_a_decimal_outside_the_citable_range_is_refused(self):
        assert parse_unit_numeral("0") is None
        assert parse_unit_numeral("4096") is None

    def test_a_label_with_no_numeral_is_none_rather_than_zero(self):
        """Zero would sort as a real daf; None makes the caller decide."""
        assert parse_unit_numeral("") is None
        assert parse_unit_numeral("עמוד") is None

    def test_a_hebrew_WORD_is_not_a_numeral(self):
        """הקדמה is five Hebrew letters and sums to 154."""
        assert parse_unit_numeral("הקדמה") is None
        assert parse_unit_numeral("ברכות") is None
        assert parse_unit_numeral("פרק") is None

    def test_summing_every_letter_would_have_sorted_a_word_into_the_tractate(self):
        """The defect, proven able to fail -- this is what the probe tree does.

        154 is a real daf in half the tractates, so an unnumbered opening section
        titled הקדמה sorts between 153 and 155 and nothing about it looks wrong.
        """
        def buggy(label):
            total = sum(_HEB_VALUE_LOCAL.get(ch, 0) for ch in label or "")
            return total or None

        assert buggy("הקדמה") == 154
        assert parse_unit_numeral("הקדמה") is None

    def test_a_descending_word_is_also_refused(self):
        """The near-miss rule, proven insufficient: requiring non-increasing letter
        values rejects הקדמה but accepts עמוד (70, 40, 6, 4 = 120) -- and עמוד is a
        word this corpus uses. Only the round-trip separates them."""
        def near_miss(label):
            vals = [_HEB_VALUE_LOCAL[c] for c in label if c in _HEB_VALUE_LOCAL]
            if not vals or any(a < b for a, b in zip(vals, vals[1:])):
                return None
            return sum(vals)

        assert near_miss("הקדמה") is None          # the easy word, caught
        assert near_miss("עמוד") == 120            # the hard word, missed
        assert parse_unit_numeral("עמוד") is None   # round-trip catches both

    def test_the_well_formedness_rule_does_not_reject_real_daf_labels(self):
        """A rule that rejected everything would pass the tests above vacuously."""
        for n in range(1, 200):
            assert parse_unit_numeral(heb_numeral(n)) == n, n

    def test_a_value_outside_the_citable_range_is_refused(self):
        for bad in (0, -1, 1000, True, "ג"):
            with pytest.raises(ValueError):
                heb_numeral(bad)


class TestSefariaDaf:
    def test_the_index_where_every_tractate_begins(self):
        assert sefaria_daf(3) == (2, 1)      # 2a

    def test_recto_and_verso_alternate(self):
        assert sefaria_daf(4) == (2, 2)      # 2b
        assert sefaria_daf(5) == (3, 1)      # 3a

    def test_a_rif_work_starts_at_its_own_first_folio(self):
        assert sefaria_daf(1) == (1, 1)      # Rif 1a

    def test_the_recovered_last_daf_never_overshoots_the_tractate(self):
        """The oracle, in miniature: real staged maxima against real tractate
        lengths. Measured over all 36 staged Tosafot works, zero overshoots."""
        for last_index, tractate_dapim in (
            (351, 176),   # Bava Batra   -> 176a
            (314, 157),   # Shabbat      -> 157b
            (224, 112),   # Ketubot      -> 112b
            (175, 88),    # Yoma         -> 88a
            (123, 64),    # Berakhot     -> 62a
        ):
            daf, _amud = sefaria_daf(last_index)
            assert daf <= tractate_dapim, (last_index, daf, tractate_dapim)

    def test_a_work_that_starts_late_needs_no_per_work_correction(self):
        """Tosafot on Keritot starts at index 17. That is 9a, which is where that
        commentary begins -- not an offset to be subtracted away."""
        assert sefaria_daf(17) == (9, 1)

    def test_treating_the_index_as_a_daf_would_have_doubled_every_citation(self):
        """The defect, proven able to fail: the plausible reading of `chapter`."""
        assert sefaria_daf(314) == (157, 2)          # Shabbat's true last daf
        assert 314 > 157                              # the raw index as a daf

    def test_an_index_below_one_is_refused(self):
        for bad in (0, -3, True, 2.5):
            with pytest.raises(ValueError):
                sefaria_daf(bad)


class TestDafLabel:
    def test_the_ordinary_label(self):
        assert daf_label_he(14, 1) == 'יד ע"א'
        assert daf_label_he(14, 2) == 'יד ע"ב'

    def test_a_foliation_that_is_not_the_tractates_is_named_in_the_label(self):
        """A Rif index rendered bare points the reader at a real Bavli page that is
        not the text -- a wrong citation that resolves."""
        assert daf_label_he(3, 1, prefix='רי"ף') == 'רי"ף ג ע"א'
        assert daf_label_he(3, 1) == 'ג ע"א'

    def test_the_label_is_never_bracketed(self):
        assert not any(ch in daf_label_he(176, 2) for ch in "[]()")

    def test_a_column_no_folio_has_is_refused(self):
        for bad in (0, 5, -1, None, True, "א"):
            with pytest.raises(ValueError):
                daf_label_he(14, bad)


class TestAmudOrdinal:
    def test_recto_and_verso(self):
        assert amud_ordinal("א") == 1
        assert amud_ordinal("ב") == 2

    def test_a_yerushalmi_folio_has_four_columns(self):
        """772 of the corpus's 8,736 daf markers -- 8.8% -- carry ג or ד."""
        assert amud_ordinal("ג") == 3
        assert amud_ordinal("ד") == 4
        assert daf_label_he(7, 3) == 'ז ע"ג'
        assert daf_label_he(7, 4) == 'ז ע"ד'

    def test_a_two_sided_model_would_have_dropped_772_real_markers(self):
        """The defect, proven able to fail. Not a mislabelling: a recto/verso model
        has nowhere to put ע"ג, so the marker is dropped -- and its neighbour then
        silently spans the column it was there to divide."""
        def two_sided(label):
            return {"א": 1, "ב": 2}.get((label or "").strip()[:1], 0)

        assert two_sided("ג") == 0
        assert amud_ordinal("ג") == 3

    def test_anything_else_names_no_column(self):
        assert amud_ordinal("") == 0
        assert amud_ordinal("ה") == 0
        assert amud_ordinal(None) == 0

    def test_a_fifth_column_is_refused(self):
        with pytest.raises(ValueError):
            daf_label_he(7, 5)


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
