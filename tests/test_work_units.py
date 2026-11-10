# -*- coding: utf-8 -*-
"""Tests for scripts/work_units.py, the format-neutral half of the locus unit tables.

Every input here is synthetic. The builders that read the reference corpora's own
files are tested where they are kept, in the private repository.
"""
from __future__ import annotations

from scripts.work_units import (
    SUB_KIND_LABEL,
    Unit,
    build_staged_ja_chapters,
    chapter_units,
    dedupe_ascending,
)
from shared.discovery_locus import (
    LocusAddress,
    citation_runs,
    compress_pieces,
    heb_numeral,
    norm_stream,
    render_ranges,
)

_GUIDE = "ja2_rambam_moreh"
_GUIDE_REF = "REF2:ja2_rambam_moreh"


def _address(division: str, chapter: str) -> LocusAddress:
    return LocusAddress(division=division, chapter=chapter, chapter_kind="פרק",
                        sub="", sub_kind="")


def test_staged_guide_uses_its_three_exact_chapter_resets(tmp_path):
    parts = (76, 48, 54)
    lines = ["הקדמת המחבר"]
    for count in parts:
        for chapter in range(1, count + 1):
            # The text's two non-canonical sixteens are accepted too.
            numeral = "יו" if chapter == 16 and count != 54 else heb_numeral(chapter)
            lines.extend((f"פצל # {numeral}", f"טקסט {chapter}"))
    raw = "\n".join(lines)
    (tmp_path / "guide.txt").write_text(raw, encoding="utf-8")
    stream = norm_stream(raw)[0]

    built = build_staged_ja_chapters(
        _GUIDE, str(tmp_path), "guide.txt", _GUIDE_REF, {_GUIDE_REF: stream})

    assert built is not None
    assert (built.family, built.grain, len(built.units)) == ("ja", "chapter", 179)
    assert built.units[0].label_he == "הקדמה"
    assert built.units[0].citation_pos == 1
    assert built.units[1].label_he == "חלק א, פרק א"
    assert built.units[1].citation_pos == 1001
    assert built.units[76].label_he == "חלק א, פרק עו"
    assert built.units[77].label_he == "חלק ב, פרק א"
    assert built.units[77].citation_pos == 2001
    assert built.units[-1].label_he == "חלק ג, פרק נד"
    assert all(a.start < b.start for a, b in zip(built.units, built.units[1:]))


def test_staged_guide_fails_closed_on_changed_chapter_sequence(tmp_path):
    lines = ["פתיחה"]
    for count in (76, 48, 54):
        for chapter in range(1, count + 1):
            numeral = heb_numeral(2 if chapter == 1 and count == 48 else chapter)
            lines.extend((f"פצל # {numeral}", "טקסט"))
    raw = "\n".join(lines)
    (tmp_path / "guide.txt").write_text(raw, encoding="utf-8")

    assert build_staged_ja_chapters(
        _GUIDE, str(tmp_path), "guide.txt", _GUIDE_REF, {_GUIDE_REF: norm_stream(raw)[0]},
    ) is None


def test_staged_guide_ignores_any_other_key_and_a_drifted_stream(tmp_path):
    (tmp_path / "guide.txt").write_text("פצל # א\nטקסט", encoding="utf-8")
    assert build_staged_ja_chapters(
        "some_other_key", str(tmp_path), "guide.txt", _GUIDE_REF, {}) is None
    # A stream that does not re-derive exactly from the body gets no units.
    assert build_staged_ja_chapters(
        _GUIDE, str(tmp_path), "guide.txt", _GUIDE_REF, {_GUIDE_REF: "אחר"}) is None


def test_chapter_units_carries_the_division_only_when_there_are_several():
    one = chapter_units([(_address("", "א"), 0), (_address("", "ב"), 10)])
    assert [u.label_he for u in one] == ["פרק א", "פרק ב"]
    assert [u.citation_pos for u in one] == [1, 2]

    # Numbering that restarts under each division never names two places.
    two = chapter_units([
        (_address("ספר א", "א"), 0), (_address("ספר א", "ב"), 10),
        (_address("ספר ב", "א"), 20),
    ])
    assert [u.label_he for u in two] == ["ספר א, פרק א", "ספר א, פרק ב", "ספר ב, פרק א"]
    assert len({u.citation_pos for u in two}) == 3
    # A division boundary is never a citation successor.
    assert two[2].citation_pos - two[1].citation_pos > 1


def test_chapter_units_collapses_a_run_of_one_address():
    units = chapter_units([
        (_address("", "א"), 0), (_address("", "א"), 5), (_address("", "ב"), 9),
    ])
    assert [(u.unit_ord, u.start, u.label_he) for u in units] == [
        (0, 0, "פרק א"), (1, 9, "פרק ב")]


def test_dedupe_ascending_keeps_the_first_of_a_tie_and_renumbers():
    units = [Unit(7, 0, "a", "א", 1), Unit(8, 0, "b", "ב", 2), Unit(9, 4, "c", "ג", 3)]
    out = dedupe_ascending(units)
    assert [(u.unit_ord, u.part_key) for u in out] == [(0, "a"), (1, "c")]


# ---------------------------------------------------------------------------
# chapter_units
# ---------------------------------------------------------------------------

def _addr(chapter: str, division: str = "", sub: str = "", kind: str = "",
          word: str = "פרק") -> LocusAddress:
    """One parsed address, as a builder hands it to `chapter_units`."""
    return LocusAddress(division=division, chapter=chapter, sub=sub, sub_kind=kind,
                        chapter_kind=word)


class TestChapterUnits:
    def test_one_unit_per_chapter_at_the_offset_the_chapter_opens(self):
        units = chapter_units([(_addr("א"), 0), (_addr("א"), 20), (_addr("ב"), 55)])
        assert [(u.label_he, u.start) for u in units] == [("פרק א", 0), ("פרק ב", 55)]

    def test_a_run_of_one_label_collapses_to_one_unit(self):
        """A source can state one address many times in a row; each repeat is the
        same place, not a new unit."""
        marks = [(_addr("א"), i * 10) for i in range(600)] + [(_addr("ב"), 6000)]
        assert len(chapter_units(marks)) == 2

    def test_not_collapsing_would_have_produced_hundreds_of_duplicate_labels(self):
        """The defect, proven able to fail."""
        def buggy(marks):
            return [(a.chapter, offset) for a, offset in marks if a is not None]

        marks = [(_addr("א"), i * 10) for i in range(600)] + [(_addr("ב"), 6000)]
        assert len(buggy(marks)) == 601
        assert len(chapter_units(marks)) == 2

    def test_the_citation_position_is_the_numeral_the_label_denotes(self):
        units = chapter_units([(_addr("א"), 0), (_addr("ב"), 10), (_addr("טו"), 20)])
        assert [u.citation_pos for u in units] == [1, 2, 15]

    def test_an_address_that_did_not_parse_contributes_no_unit(self):
        assert chapter_units([(None, 0), (_addr("א"), 10)]) == [
            Unit(0, 10, "ch:0.1", "פרק א", 1, ("", "1", "", "", "פרק"))
        ]

    def test_ordinals_are_a_dense_run_from_zero(self):
        units = chapter_units([(_addr("א"), 0), (_addr("ב"), 10), (_addr("ג"), 20)])
        assert [u.unit_ord for u in units] == [0, 1, 2]

    def test_a_chapter_that_is_not_a_numeral_gets_no_citation_position(self):
        """It has no place in the citation ORDER, so it may never merge with a
        neighbour -- `compress_pieces` refuses to bridge a `None`."""
        units = chapter_units([(_addr("א"), 0), (_addr("הקדמה"), 10)])
        assert [u.citation_pos for u in units] == [1, None]


class TestANonNumeralChapter:
    """A chapter label that is a word, not a numeral, is still a unit: it keeps its
    own text in the label, the part key and the stated address, and it stays out of
    the citation order."""

    MARKS = [(_addr("א"), 0), (_addr("הקדמה"), 10), (_addr("ב"), 20)]

    def test_it_is_a_unit_carrying_its_own_text(self):
        unit = chapter_units(self.MARKS)[1]
        assert unit == Unit(1, 10, "ch:0.הקדמה", "פרק הקדמה", None,
                            ("", "הקדמה", "", "", "פרק"))

    def test_its_neighbours_keep_their_numeral_positions(self):
        assert [u.citation_pos for u in chapter_units(self.MARKS)] == [1, None, 2]

    def test_compress_pieces_does_not_merge_across_it_and_citation_runs_reports_it(self):
        sequence = [u.citation_pos for u in chapter_units(self.MARKS)]
        pieces = [(0, 0), (1, 1), (2, 2)]
        assert compress_pieces(pieces, sequence) == pieces
        # Without the citation order the three ordinals would read as one range.
        assert compress_pieces(pieces) == [(0, 2)]
        # Folded to citation positions, it is reported rather than dropped.
        assert citation_runs([(0, 2)], sequence) == ([(1, 2)], [1])


class TestChapterUnitsWithSub:
    """`with_sub` renders the sub-unit too, its word mapped through
    `SUB_KIND_LABEL`; the ordinal, offset, part key and position are the
    source's own."""

    def test_the_sub_unit_is_rendered_and_positioned_inside_its_chapter(self):
        units = chapter_units([
            (_addr("ג", sub="ה", kind="משנה"), 0),
            (_addr("ג", sub="ו", kind="משנה"), 10),
            (_addr("ד", sub="א", kind="משנה"), 20),
        ], with_sub=True)
        assert units == [
            Unit(0, 0, "ch:0.3.5", "פרק ג, הלכה ה", 305, ("", "3", "5", "משנה", "פרק")),
            Unit(1, 10, "ch:0.3.6", "פרק ג, הלכה ו", 306, ("", "3", "6", "משנה", "פרק")),
            Unit(2, 20, "ch:0.4.1", "פרק ד, הלכה א", 401, ("", "4", "1", "משנה", "פרק")),
        ]
        # Inside a chapter the sub-units are successors; across chapters they are not.
        assert units[1].citation_pos == units[0].citation_pos + 1
        assert units[2].citation_pos != units[1].citation_pos + 1

    def test_only_the_word_is_mapped(self):
        units = chapter_units([(_addr("ג"), 0), (_addr("ג", sub="א", kind="פסוק"), 10)],
                              with_sub=True)
        assert [(u.label_he, u.part_key, u.citation_pos) for u in units] == [
            ("פרק ג", "ch:0.3.0", 300), ("פרק ג, פסוק א", "ch:0.3.1", 301)]
        assert SUB_KIND_LABEL["פסוק"] == "פסוק"

    def test_a_range_of_sub_units_reads_as_one_shortened_citation(self):
        units = chapter_units([
            (_addr("ג", sub="ה", kind="משנה"), 0),
            (_addr("ג", sub="ו", kind="משנה"), 10),
        ], with_sub=True)
        runs, unplaced = citation_runs([(0, 1)], [u.citation_pos for u in units])
        assert unplaced == []
        assert render_ranges(runs, {u.citation_pos: u.label_he for u in units}) == (
            "פרק ג, הלכה ה–ו")

    def test_two_sub_kinds_that_render_alike_stay_two_units(self):
        """`משנה` and `הלכה` render as one word, so the stated address keeps the
        sub-unit's kind: two places that read alike are still two units, and a
        check comparing the label with the stated address can see them."""
        units = chapter_units([
            (_addr("ג", sub="ה", kind="משנה"), 0),
            (_addr("ג", sub="ה", kind="הלכה"), 10),
        ], with_sub=True)
        assert [u.label_he for u in units] == ["פרק ג, הלכה ה", "פרק ג, הלכה ה"]
        assert len({u.source_address for u in units}) == 2

    def test_without_it_the_sub_unit_is_neither_rendered_nor_stated(self):
        marks = [(_addr("ג", sub="ה", kind="משנה"), 0),
                 (_addr("ג", sub="ו", kind="משנה"), 10)]
        assert chapter_units(marks) == [
            Unit(0, 0, "ch:0.3", "פרק ג", 3, ("", "3", "", "", "פרק"))]


# ---------------------------------------------------------------------------
# dedupe_ascending
# ---------------------------------------------------------------------------

class TestDedupeAscending:
    UNITS = [Unit(0, 0, "a", "א", 1), Unit(1, 0, "b", "ב", 2), Unit(2, 9, "c", "ג", 3)]

    def test_two_units_at_one_offset_keep_the_first(self):
        assert [u.part_key for u in dedupe_ascending(self.UNITS)] == ["a", "c"]

    def test_ordinals_are_renumbered_so_no_gap_survives(self):
        assert [u.unit_ord for u in dedupe_ascending(self.UNITS)] == [0, 1]

    def test_leaving_the_ordinals_alone_would_have_broken_the_dense_run(self):
        """The defect, proven able to fail: a unit table's ordinals must run 0..n-1."""
        units = self.UNITS
        naive = [u for i, u in enumerate(units) if i == 0 or u.start != units[i - 1].start]
        assert [u.unit_ord for u in naive] == [0, 2]
        assert [u.unit_ord for u in naive] != list(range(len(naive)))
        out = dedupe_ascending(units)
        assert [u.unit_ord for u in out] == list(range(len(out)))
