# -*- coding: utf-8 -*-
"""Tests for scripts/build_work_divisions.py.

Every fixture here is FABRICATED in the shape of the real sources -- never real
reference content, and never the source-manuscript provenance field's real name,
which is itself a restricted string.

House rule (feedback_gates_must_be_proven_able_to_fail): each rule is paired with
a test that reintroduces the real defect against a local re-implementation and
shows it produces the wrong answer, so the assertion's sensitivity is demonstrated
rather than asserted.
"""
from __future__ import annotations

import collections
import json
import os
import sqlite3

import pytest

from scripts.build_work_divisions import (
    JA_LEAF_KINDS,
    Edition,
    Unit,
    WorkUnits,
    _arukh_headwords,
    _chapter_units,
    _clean_marker_text,
    _daf_units,
    _is_foreign_label,
    _dedupe_ascending,
    _ja_flattens,
    _ja_keep,
    _ja_resurfaces,
    _msource_files,
    _source_title_key,
    _split_divisions,
    _standalone_header_units,
    _tree_reading_order,
    bind_tree_chains,
    build_ja,
    build_ja_pages,
    build_sefaria,
    check_invariants,
    ja_edition,
    ja_range_regressions,
    ja_reconstruct,
    ja_shed_depth,
    ja_source_index,
    ja_tree_chain,
    resolve_tree,
    tree_alignment,
    sefaria_render_kind,
    write_artifact,
)
from shared.discovery_locus import (
    PIECE_SEP,
    LocusAddress,
    citation_runs,
    norm_stream,
    parse_canonical_header,
    render_ranges,
)

PROVENANCE = "| PROVENANCE-FIELD: SOURCE-MS"


# ---------------------------------------------------------------------------
# The monolith split
# ---------------------------------------------------------------------------

MONOLITH = "\n".join([
    ">>",                                            # the empty lead-in line
    f"##בראשית, פרק א, פסוק א {PROVENANCE}##",
    ">> בראשית ברא אלהים",
    f"##בראשית, פרק א, פסוק ב {PROVENANCE}##",
    ">> והארץ היתה תהו",
    f"##בראשית, פרק ב, פסוק א {PROVENANCE}##",
    ">> ויכלו השמים",
    f"##שמות, פרק א, פסוק א {PROVENANCE}##",
    ">> ואלה שמות",
])


class TestSplitDivisions:
    def test_each_division_collects_only_its_own_payload(self):
        divisions = _split_divisions(MONOLITH)
        assert [d for d, _, _ in divisions] == ["בראשית", "שמות"]
        assert divisions[0][1] == ["בראשית ברא אלהים", "והארץ היתה תהו", "ויכלו השמים"]
        assert divisions[1][1] == ["ואלה שמות"]

    def test_each_payload_line_carries_the_address_in_force_at_that_point(self):
        divisions = _split_divisions(MONOLITH)
        assert [a.chapter for a in divisions[0][2]] == ["א", "א", "ב"]

    def test_the_whole_parsed_address_travels_not_just_the_chapter(self):
        """The division is what the ambiguity gate compares against, and a level
        dropped at the parse cannot be recovered further down."""
        first = _split_divisions(MONOLITH)[0][2][0]
        assert (first.division, first.chapter, first.sub_kind, first.sub) == (
            "בראשית", "א", "פסוק", "א")

    def test_header_words_never_enter_the_payload(self):
        """The stream is payload-only; a header word in it shifts every offset."""
        divisions = _split_divisions(MONOLITH)
        joined = " ".join(divisions[0][1])
        assert "פרק" not in joined
        assert "PROVENANCE-FIELD" not in joined and "SOURCE-MS" not in joined

    def test_a_payload_line_before_any_header_is_dropped(self):
        assert _split_divisions(">> orphan\n##בראשית, פרק א##\n>> real")[0][1] == ["real"]


# ---------------------------------------------------------------------------
# Chapter units
# ---------------------------------------------------------------------------

def addr(chapter, division="", sub="", kind=""):
    """One parsed header, in the shape `parse_canonical_header` returns."""
    return LocusAddress(division=division, chapter=chapter, sub=sub, sub_kind=kind)


class TestChapterUnits:
    def test_one_unit_per_chapter_at_the_offset_the_chapter_opens(self):
        units = _chapter_units([(addr("א"), 0), (addr("א"), 20), (addr("ב"), 55)])
        assert [(u.label_he, u.start) for u in units] == [("א", 0), ("ב", 55)]

    def test_a_run_of_one_label_collapses_to_one_unit(self):
        """The Yerushalmi interleaves a main and a variant segment under one
        chapter -- y.Berakhot carries 1,151 headers for 9 chapters."""
        marks = [(addr("א"), i * 10) for i in range(600)] + [(addr("ב"), 6000)]
        assert len(_chapter_units(marks)) == 2

    def test_not_collapsing_would_have_produced_hundreds_of_duplicate_labels(self):
        """The defect, proven able to fail."""
        def buggy(marks):
            return [(a.chapter, offset) for a, offset in marks if a is not None]

        marks = [(addr("א"), i * 10) for i in range(600)] + [(addr("ב"), 6000)]
        assert len(buggy(marks)) == 601
        assert len(_chapter_units(marks)) == 2

    def test_the_citation_position_is_the_numeral_the_label_denotes(self):
        units = _chapter_units([(addr("א"), 0), (addr("ב"), 10), (addr("טו"), 20)])
        assert [u.citation_pos for u in units] == [1, 2, 15]

    def test_an_unparsed_header_contributes_no_unit(self):
        assert _chapter_units([(None, 0), (addr("א"), 10)]) == [
            Unit(0, 10, "ch:0.1", "א", 1, ("", "1", "", ""))
        ]

    def test_ordinals_are_a_dense_run_from_zero(self):
        units = _chapter_units([(addr("א"), 0), (addr("ב"), 10), (addr("ג"), 20)])
        assert [u.unit_ord for u in units] == [0, 1, 2]

    def test_a_chapter_that_is_not_a_numeral_gets_no_citation_position(self):
        """It has no place in the citation ORDER, so it may never merge with a
        neighbour -- `compress_pieces` refuses to bridge a `None`."""
        units = _chapter_units([(addr("א"), 0), (addr("הקדמה"), 10)])
        assert [u.citation_pos for u in units] == [1, None]


# ---------------------------------------------------------------------------
# The enclosing division -- the largest defect the scholar audit found
# ---------------------------------------------------------------------------

class TestTheEnclosingDivision:
    """Mishneh Torah is Book -> Hilkhot X -> chapter, and the chapter grain emitted
    the chapter alone, so ספר המדע carried five units labelled `ה`."""

    #: Two sets of הלכות, each numbering its chapters from א. The shape is real: a
    #: book of Mishneh Torah, five הלכות deep, is why 46 of one work's 187 units
    #: named more than one place.
    RESTARTING = [
        (addr("א", division="הלכות דעות"), 0),
        (addr("ב", division="הלכות דעות"), 500),
        (addr("א", division="הלכות תלמוד תורה"), 900),
        (addr("ב", division="הלכות תלמוד תורה"), 1400),
    ]

    def test_the_address_names_the_division_the_source_states(self):
        assert [u.label_he for u in _chapter_units(self.RESTARTING)] == [
            "הלכות דעות, פרק א", "הלכות דעות, פרק ב",
            "הלכות תלמוד תורה, פרק א", "הלכות תלמוד תורה, פרק ב"]

    def test_dropping_it_named_two_places_with_one_label(self):
        """PROVEN ABLE TO FAIL: the shipped defect, re-run locally."""
        def buggy(marks):
            return [a.chapter for a, _ in marks]

        assert buggy(self.RESTARTING) == ["א", "ב", "א", "ב"]
        assert len(set(buggy(self.RESTARTING))) == 2      # four places, two labels

    def test_the_gate_now_sees_it(self):
        """PROVEN ABLE TO FAIL, and this is the one that matters: the OLD gate could
        not see this shape at all. It grouped labels by citation position, and
        `_chapter_units` set the position to the chapter's VALUE -- so the two `א`s
        collided there too and the table was indistinguishable from an edition
        legitimately revisiting one folio."""
        units = _chapter_units(self.RESTARTING)
        assert check_invariants([WorkUnits("w", "msource_header", "chapter",
                                           units, 2_000)]) == []

        # the same table with the division dropped from the LABEL only -- the stated
        # address still records it, which is exactly what makes the gate able to fire
        dropped = [u._replace(label_he=u.label_he.split(", ")[-1].replace("פרק ", ""))
                   for u in units]
        problems = check_invariants([WorkUnits("w", "msource_header", "chapter",
                                               dropped, 2_000)])
        assert any("name more than one place" in p for p in problems)

        def old_gate(units):
            """The gate as it was written, reviewed and believed."""
            by_label = collections.defaultdict(set)
            for unit in units:
                if unit.label_he:
                    by_label[unit.label_he].add(unit.citation_pos)
            return sorted(lab for lab, places in by_label.items() if len(places) > 1)

        # and it reported nothing, because the citation position collided as well
        collapsed = [u._replace(label_he=lab, citation_pos=pos)
                     for u, lab, pos in zip(units, ["א", "ב", "א", "ב"], [1, 2, 1, 2])]
        assert old_gate(collapsed) == []

    def test_a_division_boundary_is_never_a_citation_successor(self):
        """A fragment witnessing the end of one הלכות and the start of the next must
        render as two pieces. If the boundary were a successor, `compress_pieces`
        would merge them into one range spanning a boundary it never crosses."""
        positions = [u.citation_pos for u in _chapter_units(self.RESTARTING)]
        assert positions[1] == positions[0] + 1           # inside a division: adjacent
        assert positions[2] != positions[1] + 1           # across one: never

    def test_a_work_stating_ONE_division_keeps_the_bare_numeral(self):
        """A monolith child IS its division and a per-tractate file states none, so
        nothing about the Bible family moves. `בראשית · יב` is what the audit judged."""
        one = [(addr("יא", division="בראשית"), 0), (addr("יב", division="בראשית"), 90)]
        assert [u.label_he for u in _chapter_units(one)] == ["יא", "יב"]
        none = [(addr("יא"), 0), (addr("יב"), 90)]
        assert [u.label_he for u in _chapter_units(none)] == ["יא", "יב"]

    def test_the_dropped_division_is_still_recorded_for_the_gate(self):
        """So the gate can tell a work whose chapters really are unique from one
        whose builder merely forgot to say which book they are in."""
        one = [(addr("יא", division="בראשית"), 0), (addr("יב", division="בראשית"), 90)]
        assert [u.source_address for u in _chapter_units(one)] == [
            ("בראשית", "11", "", ""), ("בראשית", "12", "", "")]

    def test_the_boundary_gate_fires_when_the_packing_is_reverted(self):
        """PROVEN ABLE TO FAIL. On the shipped build 22 real spans already render as
        one continuous range whose ends sit in different divisions -- `ד–ה` running
        from הלכות קריית שמע into הלכות תפילה וברכת כוהנים. The two units need not be
        neighbours in the table: `citation_runs` merges what is consecutive in
        CITATION space, so the gate looks there. The packing makes it arithmetically
        impossible, which is exactly the kind of claim this file has had wrong before,
        so the gate checks it rather than the comment asserting it."""
        units = _chapter_units(self.RESTARTING)
        work = WorkUnits("w", "msource_header", "chapter", units, 2_000)
        assert check_invariants([work]) == []

        # the old packing: the chapter's own value, with no division term
        reverted = [u._replace(citation_pos=v) for u, v in zip(units, [1, 2, 1, 2])]
        problems = check_invariants([work._replace(units=reverted)])
        assert any("ACROSS a division boundary" in p for p in problems)

    def test_a_span_across_a_boundary_renders_as_two_pieces_not_one_range(self):
        """The consequence the boundary gate exists to prevent, shown end to end
        through the real renderer rather than asserted about the integers."""
        units = _chapter_units(self.RESTARTING)
        sequence = [u.citation_pos for u in units]
        runs, unplaced = citation_runs([(1, 2)], sequence)      # דעות ב .. ת"ת א
        by_position = {u.citation_pos: u.label_he for u in units}
        assert render_ranges(runs, by_position) == (
            "הלכות דעות, פרק ב" + PIECE_SEP + "הלכות תלמוד תורה, פרק א")
        assert unplaced == []

        # and inside one division it is still a single range, correctly shortened
        runs, _ = citation_runs([(0, 1)], sequence)
        assert render_ranges(runs, by_position) == "הלכות דעות, פרק א–ב"

    def test_two_divisions_whose_adjacent_chapters_share_a_numeral_stay_apart(self):
        """The run-collapse compares the STATED ADDRESS, not the rendered label. On
        the label it would have folded the last chapter of one division into the
        first of the next wherever the label omits the division."""
        touching = [(addr("א", division="הלכות דעות"), 0),
                    (addr("א", division="הלכות תלמוד תורה"), 700)]
        assert len(_chapter_units(touching)) == 2


class TestDedupeAscending:
    def test_two_units_at_one_offset_keep_the_first(self):
        units = [Unit(0, 0, "a", "א", 1), Unit(1, 0, "b", "ב", 2), Unit(2, 9, "c", "ג", 3)]
        assert [u.part_key for u in _dedupe_ascending(units)] == ["a", "c"]

    def test_ordinals_are_renumbered_so_no_gap_survives(self):
        units = [Unit(0, 0, "a", "א", 1), Unit(1, 0, "b", "ב", 2), Unit(2, 9, "c", "ג", 3)]
        assert [u.unit_ord for u in _dedupe_ascending(units)] == [0, 1]

    def test_leaving_the_ordinals_alone_would_have_broken_the_dense_run(self):
        """The defect, proven able to fail: `check_invariants` requires 0..n-1."""
        units = [Unit(0, 0, "a", "א", 1), Unit(1, 0, "b", "ב", 2), Unit(2, 9, "c", "ג", 3)]
        naive = [u for i, u in enumerate(units) if i == 0 or u.start != units[i - 1].start]
        assert [u.unit_ord for u in naive] == [0, 2]
        assert check_invariants([WorkUnits("w", "f", "chapter", naive, 100)])


# ---------------------------------------------------------------------------
# Which coordinate a staged versemap's integer actually is
# ---------------------------------------------------------------------------

class TestSefariaRenderKind:
    def test_a_bavli_commentary_is_an_amud_index(self):
        assert sefaria_render_kind("sef_tosafot_shabbat", "Tosafot on Shabbat") == "daf_bavli"

    def test_a_rif_work_is_its_own_foliation(self):
        assert sefaria_render_kind("sef_rif_berakhot", "Rif Berakhot") == "daf_rif"
        assert sefaria_render_kind("b2_rif_hilchot_shabbat", "Rif Shabbat") == "daf_rif"

    def test_a_commentary_on_a_BOOK_is_a_real_chapter(self):
        assert sefaria_render_kind("sef_rashi_genesis", "Rashi on Genesis") == "chapter"
        assert sefaria_render_kind("sef_radak_isaiah", "Radak on Isaiah") == "chapter"

    def test_a_torah_commentary_by_a_daf_author_is_still_a_chapter(self):
        """רבנו חננאל writes on tractates AND on the Torah; the key disambiguates."""
        assert sefaria_render_kind("sef_rabbeinu_chananel_berakhot", "") == "daf_bavli"
        assert sefaria_render_kind("sef_rabbeinu_chananel_genesis", "") == "chapter"

    def test_a_midrash_numbered_by_paragraph_is_not_read_as_a_chapter_index(self):
        assert sefaria_render_kind("sef_bereshit_rabbati_parashat_bereshit", "") == "chapter"

    def test_classifying_by_the_book_the_title_names_would_have_overshot(self):
        """The defect, proven able to fail. A suffix classifier reads a midrash's
        paragraph 93 as a chapter of a book that has 50 -- the single overshoot an
        adversarial pass found in an otherwise clean 162-work oracle."""
        def by_suffix(source_ref):
            return "chapter" if source_ref.endswith(("Genesis", "Exodus")) else "other"

        assert by_suffix("Bereshit Rabbati, Parashat Bereshit") == "other"
        assert by_suffix("Rashi on Genesis") == "chapter"
        # the shipped rule keys on the stable id, so the midrash cannot be mistaken
        # for a verse-indexed commentary on the same book
        assert sefaria_render_kind("sef_bereshit_rabbati_parashat_bereshit", "") == "chapter"


# ---------------------------------------------------------------------------
# The Judeo-Arabic leaf tier
# ---------------------------------------------------------------------------

class TestCleanMarkerText:
    def test_editorial_delimiters_are_not_part_of_the_heading(self):
        assert _clean_marker_text("{הקדמה}") == "הקדמה"
        assert _clean_marker_text("<שופטים>") == "שופטים"

    def test_an_html_escaped_heading_does_not_reach_the_reader_escaped(self):
        """Some headings arrive as entities; `&lt;שופטים&gt;` in a citation exposes
        the transport encoding to a reader."""
        assert _clean_marker_text("&lt;שופטים&gt;") == "שופטים"

    def test_stripping_without_unescaping_first_would_have_left_the_entity(self):
        """The defect, proven able to fail."""
        markup = str.maketrans({c: None for c in "{}<>[]"})
        assert "&lt;שופטים&gt;".translate(markup) == "&lt;שופטים&gt;"
        assert _clean_marker_text("&lt;שופטים&gt;") == "שופטים"

    def test_a_chain_keeps_its_separators(self):
        assert _clean_marker_text("{הקדמה}, פרק א") == "הקדמה, פרק א"

    def test_a_trailing_separator_left_by_an_empty_level_is_dropped(self):
        assert _clean_marker_text("פרק א, {}") == "פרק א"

    def test_a_headings_own_trailing_punctuation_is_not_part_of_its_name(self):
        """The source writes `+מאמר~ +א:~`, and `מאמר א:, פרק ג` reads as a typo."""
        assert _clean_marker_text("מאמר א:") == "מאמר א"


class TestBracketedDecimalPair:
    """The one shape that costs a reader the whole page rather than one row."""

    def test_a_bracketed_numeric_cross_reference_loses_its_brackets(self):
        assert _clean_marker_text("319א. כז א- יט [28- 27]") == "319א. כז א- יט 28- 27"

    def test_a_witness_list_of_hebrew_sigla_keeps_its_brackets(self):
        assert _clean_marker_text("1. [פ, מ, לא, לו, ליא]") == "1. [פ, מ, לא, לו, ליא]"

    def test_a_bracketed_single_number_is_left_alone(self):
        assert _clean_marker_text("סימן [א]") == "סימן [א]"
        assert _clean_marker_text("[הקדמה]") == "[הקדמה]"

    def test_marker_syntax_in_the_publishers_own_heading_is_stripped(self):
        """The partition tree embeds the transport form of a marker inside a heading:
        `[ספר השנים ...] +3~ [החלק הראשון]`. It reached 20 citations. Same class of leak
        as `&lt;שופטים&gt;` -- a reader should never see how the text was encoded."""
        assert _clean_marker_text("[ספר השנים] +3~ [החלק הראשון]") == \
            "[ספר השנים] [החלק הראשון]"

    def test_a_stray_tilde_never_survives_into_a_citation(self):
        assert "~" not in _clean_marker_text("פרק א~")
        assert _clean_marker_text("שער~ראשון") == "שער ראשון"

    def test_the_publishers_typesetting_slack_does_not_reach_the_citation(self):
        """Real headings arrive as `פרק  א.`; quoted inside an address that becomes a
        double space in the middle of a citation."""
        assert _clean_marker_text("פרק  א.") == "פרק א"
        assert _clean_marker_text("שער\tראשון") == "שער ראשון"

    def test_an_english_incipit_is_reported(self):
        """PROVEN ABLE TO FAIL. One anthology's `section_he` values are English
        incipits, and 5 of them reached a generated audit deck as addresses."""
        clean = WorkUnits("w1", "sefaria", "section", [
            Unit(0, 0, "sec:0", "מגיד", 0)], 100)
        assert check_invariants([clean]) == []

        english = WorkUnits("w1", "sefaria", "section", [
            Unit(0, 0, "sec:0", "In the Beginning Our Fathers Were Idol Worshipers", 0)],
            100)
        problems = check_invariants([english])
        assert len(problems) == 1
        assert "non-Hebrew language" in problems[0]

    def test_a_bare_NUMBER_is_not_a_foreign_label(self):
        """The first draft of this gate demanded a Hebrew letter, and so condemned 192
        real labels that are simply numbers -- four M-source works numbering their
        chapters in digits, and the chapters above 999 of two works too long for a
        Hebrew numeral. A number is inconsistent with the Hebrew numerals elsewhere; it
        is not the wrong language."""
        for label in ("1", "1000", "22"):
            assert not _is_foreign_label(label)
        numbered = WorkUnits("w1", "msource_header", "chapter",
                             [Unit(0, 0, "ch:1", "1", 0)], 100)
        assert check_invariants([numbered]) == []

    def test_a_hebrew_label_quoting_a_LATIN_shelfmark_is_not_foreign(self):
        """The obvious correction -- reject Latin letters -- would have condemned 49
        real labels, all of them Hebrew labels naming a Cambridge manuscript."""
        label = "מגילת סתרים, קטעים, כ\"י קמברידג' T- S Ar. 29/187c"
        assert not _is_foreign_label(label)
        assert check_invariants([WorkUnits(
            "J:x", "ja", "page", [Unit(0, 0, "page:1", label, 0)], 100)]) == []

    def test_the_two_shapes_are_told_apart(self):
        assert _is_foreign_label("In the Beginning")
        assert not _is_foreign_label("פרק א")
        assert not _is_foreign_label("7")
        assert not _is_foreign_label("סימן [א] T- S Ar. Box 5")

    def test_a_section_table_with_an_english_label_builds_nothing(self, tmp_path):
        """Refused for the WHOLE work: a table mixing `פרק ג` with an English incipit
        reads as a bug wherever the second one lands."""
        body = "אאאא\nבבבב\nגגגג\n"
        (tmp_path / "k.txt").write_text(body, encoding="utf-8")
        shipped = {"REF2:k": norm_stream(body)[0]}

        def build(labels):
            (tmp_path / "k.versemap.json").write_text(json.dumps(
                {"sections": [{"section_he": lab, "start": i * 5}
                               for i, lab in enumerate(labels)]},
                ensure_ascii=False), encoding="utf-8")
            return build_sefaria("k", str(tmp_path), "k.txt", "k.versemap.json",
                                 "REF2:k", shipped)

        assert build(["מגיד", "הלל", "נרצה"]) is not None
        assert build(["מגיד", "In the Beginning", "נרצה"]) is None

    def test_the_invariant_catches_the_shape_if_it_ever_survives(self):
        """PROVEN ABLE TO FAIL. The builder strips it; this is the backstop, and it
        exists because the previous defence was a comment asserting the shape could
        not occur -- while one real heading already carried it."""
        clean = WorkUnits("w1", "ja", "page", [
            Unit(0, 0, "page:1", "פרק א, עמ' 1", 0)], 100)
        assert check_invariants([clean]) == []

        carrying = WorkUnits("w1", "ja", "page", [
            Unit(0, 0, "page:1", "כז א- יט [28- 27], עמ' 345", 0)], 100)
        problems = check_invariants([carrying])
        assert len(problems) == 1
        assert "bracketed decimal pair" in problems[0]


class TestJaLeafKinds:
    def test_the_verse_tier_is_excluded_from_the_citable_grain(self):
        """76.3% of all JA markers, median 72 letters -- finer than a stored span."""
        assert JA_LEAF_KINDS == {"פסוק", "פס'", "משנה"}

    def test_the_coarse_kinds_are_not_in_it(self):
        for kind in ("פרק", "סימן", "שאלה", "מסכת", "שער", "פיסקא"):
            assert kind not in JA_LEAF_KINDS


# ---------------------------------------------------------------------------
# Judeo-Arabic printed pages
# ---------------------------------------------------------------------------

def _ja_source_doc(pages, author="פלוני", title="חיבור לדוגמה"):
    """A fabricated source record in the real shape: pages, each holding rows."""
    return {
        "AuthorName": author,
        "TitleName": title,
        "Publisher": "מוציא לאור",
        "PublisherCity": "ירושלים",
        "PublisherYear": 'התש"ף',
        "Editor": "עורך",
        "Content": [{"PageNumber": number,
                     "rows": [{"LineNumber": ln, "Text": text}
                              for ln, text in rows]}
                    for number, rows in pages],
    }


class TestJaReconstruct:
    def test_a_page_start_is_recorded_before_its_own_rows(self):
        doc = _ja_source_doc([("7", [(1, "אאא"), (2, "בבב")]),
                              ("8", [(1, "גגג")])])
        text, starts = ja_reconstruct(doc, "כותרת")
        assert text == "כותרת\nאאא\nבבב\nגגג\n"
        assert [n for n, _ in starts] == ["7", "8"]
        assert text[starts[0][1]:].startswith("אאא")
        assert text[starts[1][1]:].startswith("גגג")

    def test_rows_are_emitted_in_line_number_order_not_array_order(self):
        doc = _ja_source_doc([("7", [(2, "שני"), (1, "ראשון"), (3, "שלישי")])])
        text, _ = ja_reconstruct(doc, "כותרת")
        assert text == "כותרת\nראשון\nשני\nשלישי\n"

    def test_array_order_would_have_transposed_the_text(self):
        """THE DEFECT, PROVEN ABLE TO FAIL. Three of the eighty-nine real documents
        arrive with rows out of sequence -- the same length to the character, so a
        length check sees nothing. Under array order the rebuild does not reproduce
        the indexed stream and the work fails closed, losing its address entirely."""
        doc = _ja_source_doc([("7", [(2, "שני"), (1, "ראשון"), (3, "שלישי")])])
        in_array_order = "כותרת\n" + "".join(
            (row["Text"] + "\n") for row in doc["Content"][0]["rows"])
        assert in_array_order == "כותרת\nשני\nראשון\nשלישי\n"
        assert ja_reconstruct(doc, "כותרת")[0] != in_array_order

    def test_pages_are_left_in_array_order(self):
        """Sorting pages fixes nothing real and would reorder any edition whose
        printed numbering is not monotonic -- front matter, plates, appendices."""
        doc = _ja_source_doc([("12", [(1, "אחד")]), ("3", [(1, "שני")])])
        assert [n for n, _ in ja_reconstruct(doc, "כ")[1]] == ["12", "3"]

    def test_the_title_line_is_part_of_the_text(self):
        """The ingest indexed it, so its letters are in the stream every stored
        offset was measured against. Dropping it shifts every page by its length."""
        doc = _ja_source_doc([("1", [(1, "גוף")])])
        assert ja_reconstruct(doc, "שם החיבור")[0].startswith("שם החיבור\n")
        assert ja_reconstruct(doc, "שם החיבור")[1][0][1] == len("שם החיבור\n")


class TestJaSourceIndex:
    def test_a_document_is_found_by_its_author_and_title(self, tmp_path):
        doc = _ja_source_doc([("1", [(1, "גוף")])], author="פלוני", title="ספר")
        (tmp_path / "1.json").write_text(json.dumps(doc, ensure_ascii=False),
                                        encoding="utf-8")
        index = ja_source_index(str(tmp_path))
        assert _source_title_key("פלוני, ספר") in index

    def test_a_near_miss_does_not_bind(self, tmp_path):
        """Exact only. A substring fallback once bound a commentary on one biblical
        book to the same author's commentary on another, and a mis-binding does not
        fail -- it addresses one work with a different work's pages."""
        doc = _ja_source_doc([("1", [(1, "גוף")])], author="פלוני", title="פירוש שמות")
        (tmp_path / "1.json").write_text(json.dumps(doc, ensure_ascii=False),
                                         encoding="utf-8")
        index = ja_source_index(str(tmp_path))
        assert _source_title_key("פלוני, פירוש") not in index

    def test_punctuation_and_spacing_do_not_defeat_the_match(self):
        assert (_source_title_key("פלוני, ספר-הדוגמה")
                == _source_title_key("פלוני,ספר הדוגמה"))


def _node(path, text, value=None):
    return {"path": path, "depth": path.count(":"), "text": text, "value": value}


class TestTreeReadingOrder:
    def test_paths_sort_component_wise_as_integers(self):
        nodes = [_node("10", "י"), _node("9", "ט"), _node("9:2", "ב"), _node("9:10", "י")]
        assert [n["text"] for n in _tree_reading_order(nodes)] == ["ט", "ב", "י", "י"]

    def test_string_sorting_would_interleave_the_tenth_section_into_the_ninth(self):
        """THE DEFECT, PROVEN ABLE TO FAIL. The alignment below is positional, so an
        order that puts section 10 before section 9 does not merely look untidy -- it
        hands each marker the wrong node's parent."""
        nodes = [_node("10", "י"), _node("9", "ט")]
        assert [n["text"] for n in _tree_reading_order(nodes)] == ["ט", "י"]
        as_strings = sorted(nodes, key=lambda n: n["path"])
        assert [n["text"] for n in as_strings] == ["י", "ט"]


class TestJaTreeChain:
    NODES = [_node("0", "שער ראשון"), _node("0:0", "פרק א"), _node("0:1", "פרק ב"),
             _node("1", "שער שני"), _node("1:0", "פרק א")]

    def _by_path(self):
        return {n["path"]: n for n in self.NODES}

    def test_a_child_carries_the_parent_the_publisher_states(self):
        assert ja_tree_chain(self.NODES[1], self._by_path()) == ["שער ראשון", "פרק א"]

    def test_a_top_level_node_has_no_parent_to_state(self):
        assert ja_tree_chain(self.NODES[0], self._by_path()) == ["שער ראשון"]

    def test_two_sections_with_the_same_child_label_stay_distinguishable(self):
        by_path = self._by_path()
        assert ja_tree_chain(self.NODES[1], by_path) != ja_tree_chain(self.NODES[4], by_path)

    def test_a_link_repeating_its_parent_is_dropped(self):
        """The tree nests a book under its own name -- `בראשית` the book, `בראשית` the
        reading, then the chapter -- and quoted into a citation that reads
        `בראשית, בראשית, א`, which looks like a mistake rather than a hierarchy."""
        nodes = [_node("0", "בראשית"), _node("0:0", "בראשית"), _node("0:0:0", "א")]
        chain = ja_tree_chain(nodes[-1], {n["path"]: n for n in nodes})
        assert chain == ["בראשית", "א"]

    def test_a_label_repeating_a_NON_adjacent_ancestor_is_kept(self):
        """Only a link repeating its immediate parent is redundant. `הקדמה` inside
        discourse 1 whose own book also opens with a `הקדמה` is a different place."""
        nodes = [_node("0", "הקדמה"), _node("0:0", "מאמר א"), _node("0:0:0", "הקדמה")]
        chain = ja_tree_chain(nodes[-1], {n["path"]: n for n in nodes})
        assert chain == ["הקדמה", "מאמר א", "הקדמה"]

    def test_a_chain_is_capped_so_a_citation_stays_readable(self):
        deep = [_node("0", "א"), _node("0:0", "ב"), _node("0:0:0", "ג"),
                _node("0:0:0:0", "ד"), _node("0:0:0:0:0", "ה")]
        chain = ja_tree_chain(deep[-1], {n["path"]: n for n in deep})
        assert len(chain) == 3
        assert chain[-1] == "ה"


class TestBindTreeChains:
    """Markers know WHERE, the tree knows WHAT CONTAINS IT. Both in reading order."""

    MARKERS = [(0, "פרק", "א"), (100, "פסוק", "א"), (200, "פסוק", "ב"),
               (300, "פרק", "ב"), (400, "פסוק", "א")]
    NODES = [_node("0", "פרק א"), _node("0:0", "פסוק א"), _node("0:1", "פסוק ב"),
             _node("1", "פרק ב"), _node("1:0", "פסוק א")]

    def test_a_bare_verse_gains_the_chapter_the_publisher_puts_it_under(self):
        chains = bind_tree_chains(self.MARKERS, self.NODES)
        assert chains[100] == ["פרק א", "פסוק א"]
        assert chains[400] == ["פרק ב", "פסוק א"]

    def test_the_two_identical_verse_labels_get_DIFFERENT_parents(self):
        """The reason positional alignment is used rather than label lookup: `פסוק א`
        occurs under both chapters, and a dictionary keyed on the label would give
        both the same parent and silently misplace one."""
        chains = bind_tree_chains(self.MARKERS, self.NODES)
        assert chains[100] != chains[400]

        by_label = {n["text"]: n for n in self.NODES}
        assert by_label["פסוק א"]["path"] == "1:0"      # the LAST one wins, wrongly

    def test_typographic_differences_do_not_defeat_the_match(self):
        nodes = [_node("0", "פרק  א."), _node("0:0", " פסוק א ")]
        chains = bind_tree_chains(self.MARKERS[:2], nodes)
        assert chains[100] == ["פרק א", "פסוק א"]

    def test_a_tree_that_does_not_align_contributes_NOTHING(self):
        """Fail closed. Borrowing a parent from a misalignment asserts a containment
        that is not in the source, which is the exact defect the tree was fetched to
        remove -- worse than having no parent at all."""
        unrelated = [_node(str(i), f"מדור {i}") for i in range(12)]
        assert bind_tree_chains(self.MARKERS, unrelated) == {}

    def test_no_tree_means_no_chains_rather_than_an_error(self):
        assert bind_tree_chains(self.MARKERS, []) == {}
        assert bind_tree_chains([], self.NODES) == {}

    def test_a_partially_aligned_tree_binds_only_what_it_matched(self):
        nodes = self.NODES + [_node("2", "פרק ג"), _node("2:0", "פסוק א")]
        chains = bind_tree_chains(self.MARKERS, nodes)
        assert set(chains) <= {p for p, _, _ in self.MARKERS}
        assert chains[100] == ["פרק א", "פסוק א"]


class TestJaEdition:
    """What a reader needs to check a page address against a book on a shelf."""

    def test_the_imprint_and_both_names_are_carried(self):
        doc = _ja_source_doc([("1", [(1, "גוף")])])
        doc["OriginalName"] = "כתאב אלמואזנה"
        ed = ja_edition(doc)
        assert ed.title_he == "חיבור לדוגמה"
        assert ed.title_original == "כתאב אלמואזנה"
        assert (ed.publisher, ed.publisher_city, ed.editor) == \
            ("מוציא לאור", "ירושלים", "עורך")

    def test_an_original_name_that_merely_repeats_the_title_is_dropped(self):
        """5 of 92 real documents record no distinct original name, and a consumer
        must not render the same name twice."""
        doc = _ja_source_doc([("1", [(1, "גוף")])])
        doc["OriginalName"] = doc["TitleName"]
        assert ja_edition(doc).title_original == ""

    def test_a_missing_field_becomes_an_empty_string_not_None(self):
        doc = _ja_source_doc([("1", [(1, "גוף")])])
        doc.pop("Editor")
        doc["Edition"] = None
        ed = ja_edition(doc)
        assert ed.editor == "" and ed.edition == ""

    def test_the_original_name_is_kept_in_hebrew_letters_verbatim(self):
        """Judeo-Arabic is the Arabic language in Hebrew script, and all 92 real
        records are written that way. This is the name the source carries, not a
        translation of anything, so nothing is transliterated or rendered."""
        doc = _ja_source_doc([("1", [(1, "גוף")])])
        doc["OriginalName"] = "אלמכ'תאר פי אלאמאנאת ואלאעתקאדאת"
        assert ja_edition(doc).title_original == "אלמכ'תאר פי אלאמאנאת ואלאעתקאדאת"

    def test_a_page_built_from_the_source_carries_its_edition(self, tmp_path):
        helper = TestBuildJaPages()
        path, ref_id, shipped, source = helper._write(tmp_path, [
            ("1", [(1, "+פרק~ +א~"), (2, "גוף")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source)
        assert built.edition is not None
        assert built.edition.publisher == "מוציא לאור"

    def test_the_edition_reaches_the_artifact(self, tmp_path):
        edition = Edition("ספר", "כתאב", 'רס"ג', "רב סעדיה", "מוציא", "ירושלים",
                          'התש"ף', "עורך", "")
        work = WorkUnits("J:x", "ja", "page",
                         [Unit(0, 0, "page:1", "פרק א, עמ' 1", 0)], 100, edition)
        out = str(tmp_path / "a.db")
        write_artifact(out, [work])
        conn = sqlite3.connect(out)
        row = conn.execute(
            "SELECT title_he, title_original, author_short, publisher, editor "
            "FROM locus_edition WHERE locus_ref_id='J:x'").fetchone()
        conn.close()
        assert row == ("ספר", "כתאב", 'רס"ג', "מוציא", "עורך")

    def test_a_work_with_no_edition_writes_no_row(self, tmp_path):
        work = WorkUnits("M:y", "msource_header", "chapter",
                         [Unit(0, 0, "ch:1", "פרק א", 0)], 100)
        out = str(tmp_path / "b.db")
        write_artifact(out, [work])
        conn = sqlite3.connect(out)
        assert conn.execute("SELECT COUNT(*) FROM locus_edition").fetchone()[0] == 0
        conn.close()


class TestResolveTree:
    """The filename binding is a claim, and it is measurably wrong on real data."""

    MARKERS = [(0, "פרק", "א"), (100, "פרק", "ב"), (200, "פרק", "ג"),
               (300, "פרק", "ד"), (400, "פרק", "ה")]
    RIGHT = [_node("0", "פרק א"), _node("1", "פרק ב"), _node("2", "פרק ג"),
             _node("3", "פרק ד"), _node("4", "פרק ה")]
    WRONG = [_node(str(i), f"האות {n}") for i, n in enumerate("אבגדהוזחט")]

    def test_a_tree_that_fits_is_used_without_a_search(self):
        nodes, how = resolve_tree(self.MARKERS, self.RIGHT, {"x": self.WRONG})
        assert nodes is self.RIGHT
        assert how == "filename"

    def test_a_tree_that_does_not_fit_is_replaced_by_one_that_does(self):
        """Two real documents were handed each other's trees -- each aligning at 0.000
        with what it was given and 1.000 with the other's."""
        nodes, how = resolve_tree(self.MARKERS, self.WRONG,
                                  {"right": self.RIGHT, "wrong": self.WRONG})
        assert nodes is self.RIGHT
        assert how == "searched->right"

    def test_a_tiny_tree_does_not_win_on_the_FRACTION_it_matched(self):
        """THE DEFECT, PROVEN ABLE TO FAIL. Ranking by fraction-of-tree-matched lets a
        two-node tree whose labels occur anywhere score a perfect 1.000 -- one real
        tree scores 1.000 against three unrelated works -- and so replace a large tree
        that genuinely fits. Ranking is by labels MATCHED; the fraction is only a floor.
        """
        tiny = [_node("0", "פרק א")]
        nodes, how = resolve_tree(self.MARKERS, self.WRONG,
                                  {"tiny": tiny, "right": self.RIGHT})
        assert nodes is self.RIGHT

        by_fraction = max(
            (tree_alignment(self.MARKERS, cand)[1], key)
            for key, cand in {"tiny": tiny, "right": self.RIGHT}.items())
        assert by_fraction[1] == "tiny"
        assert tree_alignment(self.MARKERS, tiny)[1] == 1.0

    def test_two_equally_plausible_trees_bind_NEITHER(self):
        """Two volumes of one commentary have near-identical structures. Binding either
        on a coin-toss would publish one volume's section names over the other's text,
        and no gate downstream could see it -- every offset would still be right."""
        twin = [_node(str(i), n["text"]) for i, n in enumerate(self.RIGHT)]
        nodes, how = resolve_tree(self.MARKERS, self.WRONG,
                                  {"a": self.RIGHT, "b": twin})
        assert nodes is None
        assert how == "unbound"

    def test_nothing_fits_means_nothing_is_bound(self):
        nodes, how = resolve_tree(self.MARKERS, self.WRONG, {"wrong": self.WRONG})
        assert nodes is None
        assert how == "unbound"

    def test_no_candidates_at_all_is_not_an_error(self):
        assert resolve_tree(self.MARKERS, None, None) == (None, "unbound")


class TestBuildJaPages:
    """The page grain end to end, through the real fail-closed gate."""

    TITLE = "פלוני, ספר הדוגמה"

    def _write(self, tmp_path, pages, title=None):
        title = title or self.TITLE
        doc = _ja_source_doc(pages, author="פלוני", title="ספר הדוגמה")
        text, _ = ja_reconstruct(doc, title)
        # The per-document file the ingest read: a lead-in, the title, a rule, body.
        body = text.split("\n", 1)[1]
        path = tmp_path / "07-דוגמה.txt"
        path.write_text("***\n" + title + "\n----------\n" + body, encoding="utf-8")
        ref_id = "J:07-דוגמה"
        shipped = {ref_id: norm_stream(path.read_text(encoding="utf-8"))[0]}
        return str(path), ref_id, shipped, {_source_title_key(title): doc}

    def test_one_unit_per_page_labelled_with_its_section(self, tmp_path):
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("9", [(1, "+פרק~ +א~"), (2, "טקסט ראשון")]),
            ("10", [(1, "עוד טקסט")]),
            ("11", [(1, "+פרק~ +ב~"), (2, "טקסט אחר")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source)
        assert built is not None
        assert built.grain == "page"
        assert [u.label_he for u in built.units] == [
            "פרק א, עמ' 9", "פרק א, עמ' 10", "פרק ב, עמ' 11"]
        assert [u.part_key for u in built.units] == ["page:9", "page:10", "page:11"]

    def test_the_printed_number_is_used_not_a_running_index(self, tmp_path):
        """Documents commonly open at page 23. Numbering from zero would give every
        one of them an address that is not in the book."""
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("23", [(1, "+פרק~ +א~"), (2, "פתיחה")]),
            ("24", [(1, "המשך")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source)
        assert [u.part_key for u in built.units] == ["page:23", "page:24"]

    def test_a_page_with_no_rows_does_not_become_an_address(self, tmp_path):
        """3,717 real pages carry no rows at all. They are not places."""
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("9", [(1, "+פרק~ +א~"), (2, "טקסט")]),
            ("10", []),
            ("11", [(1, "עוד")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source)
        assert [u.part_key for u in built.units] == ["page:9", "page:11"]

    def test_deduping_a_tie_by_keeping_the_first_would_misnumber_the_next_page(
            self, tmp_path):
        """THE DEFECT, PROVEN ABLE TO FAIL, and it is an off-by-a-page not a gap.

        An empty page and the page after it share a start offset, because the offset
        is recorded before the rows are read. Resolving that tie by keeping the FIRST
        -- which is what the generic deduper does -- publishes the following page's
        text under the empty page's number. Nothing about the output looks wrong: the
        offsets still ascend, the invariants still pass, and the reader is sent one
        page early.
        """
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("9", [(1, "+פרק~ +א~"), (2, "טקסט")]),
            ("10", []),
            ("11", [(1, "עוד")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source)
        text_offset = {u.part_key: u.start for u in built.units}["page:11"]

        keep_first = _dedupe_ascending([
            Unit(0, 0, "page:9", "פרק א, עמ' 9", None),
            Unit(1, text_offset, "page:10", "פרק א, עמ' 10", None),
            Unit(2, text_offset, "page:11", "פרק א, עמ' 11", None),
        ])
        assert [u.part_key for u in keep_first] == ["page:9", "page:10"]
        assert check_invariants([WorkUnits(ref_id, "ja", "page", keep_first,
                                           built.stream_len)]) == []

    def test_offsets_are_ascending_and_land_inside_the_stream(self, tmp_path):
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("1", [(1, "+פרק~ +א~"), (2, "אאאא")]),
            ("2", [(1, "בבבב")]),
            ("3", [(1, "גגגג")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source)
        starts = [u.start for u in built.units]
        assert starts == sorted(starts)
        assert starts[-1] < built.stream_len
        assert check_invariants([built]) == []

    def test_the_label_carries_the_sections_own_name_not_the_inferred_chain(self, tmp_path):
        """The enclosing chain is the inferred part. A verified page address does not
        borrow it -- the owner's objection to invented containment was that a chain
        asserting one section sits inside another reads as information."""
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("1", [(1, "+שער~ +א~"), (2, "פתיחה")]),
            ("2", [(1, "+פרק~ +א~"), (2, "גוף")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source)
        assert [u.label_he for u in built.units] == ["שער א, עמ' 1", "פרק א, עמ' 2"]
        assert not any(", " in u.label_he.rsplit(", ", 1)[0] for u in built.units)

    def test_a_stream_that_does_not_match_gets_no_units(self, tmp_path):
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("1", [(1, "+פרק~ +א~"), (2, "טקסט")]),
        ])
        assert build_ja_pages(path, ref_id, {ref_id: "אחר"}, source) is None

    def test_a_document_with_no_source_record_gets_no_units(self, tmp_path):
        path, ref_id, shipped, _ = self._write(tmp_path, [
            ("1", [(1, "+פרק~ +א~"), (2, "טקסט")]),
        ])
        assert build_ja_pages(path, ref_id, shipped, {}) is None

    def test_pages_that_do_not_ascend_fall_back_rather_than_ship(self, tmp_path):
        """A numbering that runs backwards makes `citation_pos = unit_ord` false, and
        a two-page span then renders as a range with its ends reversed. The daf family
        already emitted `מנחות צד ע"א–סג ע"ב` from exactly this assumption unchecked."""
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("30", [(1, "+פרק~ +א~"), (2, "אאאא")]),
            ("12", [(1, "בבבב")]),
        ])
        assert build_ja_pages(path, ref_id, shipped, source) is None

    def test_a_page_before_any_section_marker_is_still_addressable(self, tmp_path):
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("1", [(1, "טקסט בלי סימון")]),
            ("2", [(1, "+פרק~ +א~"), (2, "גוף")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source)
        assert built.units[0].label_he == "עמ' 1"
        assert built.units[1].label_he == "פרק א, עמ' 2"

    def test_the_publishers_tree_supplies_the_stated_parent(self, tmp_path):
        """The point of harvesting the tree: `פרק א` becomes `שער ראשון, פרק א` because
        the publisher SAYS so, not because a restart pattern was read as containment."""
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("1", [(1, "+שער~ +ראשון~"), (2, "פתיחה")]),
            ("2", [(1, "+פרק~ +א~"), (2, "גוף")]),
            ("3", [(1, "+פרק~ +ב~"), (2, "עוד")]),
        ])
        tree = _tree_reading_order([
            _node("0", "שער ראשון"), _node("0:0", "פרק א"), _node("0:1", "פרק ב")])
        built = build_ja_pages(path, ref_id, shipped, source, tree)
        assert [u.label_he for u in built.units] == [
            "שער ראשון, עמ' 1", "שער ראשון, פרק א, עמ' 2", "שער ראשון, פרק ב, עמ' 3"]

    def test_without_a_tree_the_section_carries_its_own_label_alone(self, tmp_path):
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("1", [(1, "+שער~ +ראשון~"), (2, "פתיחה")]),
            ("2", [(1, "+פרק~ +א~"), (2, "גוף")]),
        ])
        built = build_ja_pages(path, ref_id, shipped, source, None)
        assert [u.label_he for u in built.units] == ["שער ראשון, עמ' 1", "פרק א, עמ' 2"]

    def test_a_page_is_never_labelled_with_a_VERSE(self, tmp_path):
        """The tree is aligned against every marker so its leaves have anchors, but a
        page holds many verses -- naming the page after one of them would claim the
        page IS that verse. Only the coarse tier may name a page.

        Three chapters, because a document with fewer than three coarse markers is
        treated as flat and then its verses ARE the only structure it has.
        """
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("1", [(1, "+פרק~ +א~"), (2, "+פסוק~ +א~"), (3, "טקסט"),
                   (4, "+פסוק~ +ב~"), (5, "עוד")]),
            ("2", [(1, "+פסוק~ +ג~"), (2, "המשך")]),
            ("3", [(1, "+פרק~ +ב~"), (2, "+פסוק~ +א~"), (3, "שוב")]),
            ("4", [(1, "+פרק~ +ג~"), (2, "+פסוק~ +א~"), (3, "סוף")]),
        ])
        tree = _tree_reading_order([
            _node("0", "פרק א"), _node("0:0", "פסוק א"),
            _node("0:1", "פסוק ב"), _node("0:2", "פסוק ג"),
            _node("1", "פרק ב"), _node("1:0", "פסוק א"),
            _node("2", "פרק ג"), _node("2:0", "פסוק א")])
        built = build_ja_pages(path, ref_id, shipped, source, tree)
        assert [u.label_he for u in built.units] == [
            "פרק א, עמ' 1", "פרק א, עמ' 2", "פרק ב, עמ' 3", "פרק ג, עמ' 4"]
        assert not any("פסוק" in u.label_he for u in built.units)

    def test_a_tree_belonging_to_another_work_contributes_nothing(self, tmp_path):
        path, ref_id, shipped, source = self._write(tmp_path, [
            ("1", [(1, "+פרק~ +א~"), (2, "גוף")]),
            ("2", [(1, "+פרק~ +ב~"), (2, "עוד")]),
            ("3", [(1, "+פרק~ +ג~"), (2, "שוב")]),
        ])
        foreign = _tree_reading_order([_node(str(i), f"מדור {i}") for i in range(14)])
        built = build_ja_pages(path, ref_id, shipped, source, foreign)
        assert [u.label_he for u in built.units] == [
            "פרק א, עמ' 1", "פרק ב, עמ' 2", "פרק ג, עמ' 3"]


# ---------------------------------------------------------------------------
# Structural gates
# ---------------------------------------------------------------------------

class TestTheJudeoArabicShed:
    """One shed depth per WORK, and a guard that is the whole design.

    Owner: *"The JA title is just weird… Should perhaps be shortened."* Labels run to
    144 characters. Two shortening families were designed and measured on RENDERED
    RANGES rather than on unit labels, and both were refused: deciding per SECTION
    makes 4,020 ordered pairs longer and 1,853 re-state an ancestor the head was
    stripped of, and a uniform DEPTH CAP fails the same way on a depth count while
    barely reaching the length -- deleting every ancestor in the corpus still leaves
    318 labels over 40 characters, because the length is in the innermost element.

    TWO guards, and the second was found only after the first shipped. Range-safety
    (nothing longer, nothing re-stated, never a child running into its parent) is not
    the whole of correctness: a shed can also make two DIFFERENT sections render
    alike, and every range test passes that by construction. See
    `test_two_sections_that_would_render_alike_block_the_shed`.
    """

    #: Two deep, and every innermost element distinct: shedding is safe.
    SAFE = [(["שער א", "פרק א"], ", עמ' 1"), (["שער א", "פרק ב"], ", עמ' 2"),
            (["שער ב", "פרק ג"], ", עמ' 3")]
    #: Two deep, and TWO SECTIONS THAT WOULD RENDER ALIKE. This was written as the
    #: safe case and it is the defect: shedding gives `פרק א, עמ' 1–3`, one section
    #: spanning three pages, across two different gates. Every range test passes it
    #: -- shorter, nothing re-stated, sibling-to-sibling -- which is why it needed a
    #: guard of its own rather than another clause in the ones that were there.
    FLATTENS = [(["שער א", "פרק א"], ", עמ' 1"), (["שער א", "פרק ב"], ", עמ' 2"),
                (["שער ב", "פרק א"], ", עמ' 3")]
    #: A work where a name is an ancestor early and a whole label LATER. Shedding it
    #: would make the later page render as the earlier page's own parent.
    UNEVEN = [(["הקדמה", "פרק א"], ", עמ' 1"), (["הקדמה", "פרק ב"], ", עמ' 2"),
              (["הקדמה"], ", עמ' 3")]

    def test_a_work_within_budget_sheds_nothing(self):
        assert ja_shed_depth(self.SAFE, budget=60) == 0

    def test_a_work_over_budget_sheds_one_level_for_every_unit(self):
        assert ja_shed_depth(self.SAFE, budget=12) == 1
        assert [_ja_keep(parts, 1) for parts, _ in self.SAFE] == [
            ["פרק א"], ["פרק ב"], ["פרק ג"]]

    def test_the_innermost_element_is_never_shed(self):
        """Criterion E, and it is what protects the work whose organising principle is
        a manuscript siglum -- there the siglum is the ancestor, but elsewhere the
        innermost element IS the address, and 46 of the 48 labels still over budget
        after the shed have no ancestor left at all."""
        assert ja_shed_depth(self.SAFE, budget=1) == 1        # not 2
        assert _ja_keep(["שער א", "פרק א"], 5) == ["פרק א"]
        # and the search stops there rather than reporting a depth that changes
        # nothing: past `deepest - 1` every chain is already down to one element

    def test_a_name_that_resurfaces_later_blocks_the_shed(self):
        """PROVEN ABLE TO FAIL. Uniform shedding keeps head and tail structurally
        alike -- but the chains are not equally deep, so a page directly under a
        division keeps that division as its WHOLE label while a page under a
        sub-section has it removed. If the shallow one comes later, the range reads
        as running from a chapter into the introduction that contains it."""
        assert ja_shed_depth(self.UNEVEN, budget=8) == 0
        assert _ja_resurfaces(self.UNEVEN, 1)
        assert not _ja_resurfaces(self.SAFE, 1)

    def test_two_sections_that_would_render_alike_block_the_shed(self):
        """PROVEN ABLE TO FAIL, and it is the case the acceptance tests could not see.
        A range whose ends sit in different sections that now render alike is SHORTER,
        hides the ancestor at BOTH ends so nothing is re-stated, and is
        sibling-to-sibling so it never reads as a child running into its parent. On
        the corpus this flattened 258 ordered pairs: `4, עמ' 89–95` for a span
        crossing two different מקאלות."""
        assert _ja_flattens(self.FLATTENS, 1)
        assert not _ja_flattens(self.SAFE, 1)
        assert ja_shed_depth(self.FLATTENS, budget=12) == 0
        assert ja_shed_depth(self.SAFE, budget=12) == 1

    def test_every_range_test_passes_the_flattened_case(self):
        """Which is the whole argument for a separate guard: the shed is refused by
        `_ja_flattens` and by nothing else."""
        kept = [_ja_keep(parts, 1) for parts, _ in self.FLATTENS]
        assert ja_range_regressions(self.FLATTENS, kept) == 0
        assert not _ja_resurfaces(self.FLATTENS, 1)

    def _shed(self, rows, depth):
        return [_ja_keep(parts, depth) for parts, _ in rows]

    def test_without_the_guard_that_work_would_render_a_child_into_its_parent(self):
        """The defect the guard exists to prevent, rendered rather than asserted."""
        head = ", ".join(_ja_keep(self.UNEVEN[1][0], 1)) + self.UNEVEN[1][1]
        tail = ", ".join(_ja_keep(self.UNEVEN[2][0], 1)) + self.UNEVEN[2][1]
        assert head == "פרק ב, עמ' 2"
        assert tail == "הקדמה, עמ' 3"        # the parent of the head's own chapter
        assert ja_range_regressions(self.UNEVEN, self._shed(self.UNEVEN, 1)) > 0
        assert ja_range_regressions(self.UNEVEN, self._shed(self.UNEVEN, 0)) == 0

    def test_the_gate_stays_green_on_a_safe_shed(self):
        assert ja_range_regressions(self.SAFE, self._shed(self.SAFE, 1)) == 0

    def test_the_gate_catches_a_range_that_gets_LONGER(self):
        """The other half of the gate, and the reason it takes the emitted chains
        rather than a shed depth. A gate parameterised by one depth can only describe
        a UNIFORM rule, so it could not express the per-SECTION rule that made 4,020
        ordered pairs longer -- head and tail stop sharing a prefix, so
        `shorten_range_tail` stops eliding and the tail re-prints the ancestor."""
        rows = [(["שער ארוך מאוד", "פרק א"], ", עמ' 1"),
                (["שער ארוך מאוד", "פרק ב"], ", עמ' 2")]
        assert ja_range_regressions(rows, self._shed(rows, 0)) == 0
        assert ja_range_regressions(rows, self._shed(rows, 1)) == 0

        # the refused shape: ONE section sheds and its sibling does not
        per_section = [rows[0][0][1:], rows[1][0]]
        assert ja_range_regressions(rows, per_section) > 0

    def test_a_work_end_to_end_sheds_and_keeps_its_pages(self, tmp_path):
        long_section = "מאמר ראשון על עניין ארוך במיוחד שאין לו סוף"
        doc = _ja_source_doc([
            ("1", [(1, f"+{long_section}~"), (2, "טקסט")]),
            ("2", [(1, "+פרק~ +א~"), (2, "עוד טקסט")]),
            ("3", [(1, "+פרק~ +ב~"), (2, "טקסט אחרון")]),
        ], author="פלוני", title="ספר")
        text, _ = ja_reconstruct(doc, "פלוני, ספר")
        path = tmp_path / "07-דוגמה.txt"
        path.write_text("***\nפלוני, ספר\n----------\n" + text.split("\n", 1)[1],
                        encoding="utf-8")
        shipped = {"J:x": norm_stream(path.read_text(encoding="utf-8"))[0]}
        built = build_ja_pages(str(path), "J:x", shipped,
                               {_source_title_key("פלוני, ספר"): doc})
        assert [u.part_key for u in built.units] == ["page:1", "page:2", "page:3"]
        assert all(u.label_he.endswith(f"עמ' {n}") for u, n in
                   zip(built.units, (1, 2, 3)))
        assert check_invariants([built]) == []


class TestTheSourcesOwnDefectsSurviveVerbatim:
    """Two things in the Judeo-Arabic labels look like our bugs and are not.

    The Latin classmarks read as though a bidi renderer had reversed them --
    `7 8G S _T` where a reader expects `T-S G8 7`. Measured, each such string is
    byte-identical in three independent layers: the document as stored, the
    publisher's separately harvested partition tree, and our label. And `T- S` is not
    a lost space: this source writes EVERY hyphen with a following space -- 679
    occurrences in these labels, 521 of them between two Hebrew letters (`ו- ח`), 111
    between digits (`35- 38`), and not one unspaced hyphen anywhere. Nor is there an
    inverse to apply: of 17 distinct Latin runs, 9 already read canonically, so any
    reordering rule would break the ones that are right to fix the ones that are not.

    `תרגום הפסוקום` is the same kind of thing: the source's own typo, present in the
    marker stream, in the publisher's tree and in the source record, against 39
    correctly spelled siblings in the same work.

    ONE DIVISION AUTHORITY, NAMED settles both. A label quietly corrected here would
    disagree with the publisher's own tree, and a reader holding one of the two would
    have no way to tell which. These tests exist so that a later reader who notices
    the mangling does not "fix" it.
    """

    def test_a_latin_classmark_is_emitted_exactly_as_the_source_wrote_it(self):
        for marker in ('כ"י קמברידג\' T- S Ar. Box 43', 'כ"י קמברידג\' 7 8G S _T'):
            assert _clean_marker_text("{" + marker + "}") == marker

    def test_the_hyphen_convention_is_the_sources_and_is_not_normalised(self):
        """It is applied to Hebrew and digits identically, so a rule that closed the
        gap in a Latin run would be treating one script's typography as an error."""
        for marker in ("ו- ח", "35- 38", "א- ק"):
            assert _clean_marker_text(marker) == marker

    def test_a_typo_in_the_source_is_not_corrected_on_the_way_out(self):
        assert _clean_marker_text("{תרגום הפסוקום מג, כט- לד}") == \
            "תרגום הפסוקום מג, כט- לד"

    def test_reordering_the_latin_would_break_the_runs_that_are_already_right(self):
        """PROVEN ABLE TO FAIL: the obvious repair, applied to the 9 canonical runs."""
        def buggy(text):
            return " ".join(reversed(text.split()))

        assert buggy("T- S Ar. Box 5") != "T- S Ar. Box 5"
        assert buggy("Ms. heb. e100") != "Ms. heb. e100"


class TestABareOrdinalIsNotACitation:
    """`שע–שעט` in ספר הערוך and `מא` in הלכות גדולות: the scholar audit's two
    complaints about the staged family, which fail for different reasons and so need
    different repairs. Owner: *"Where is שע-שעט? what is the source?"* and *"What's
    מא in הלכות גדולות?"*
    """

    BODY = "אאאא\nבבבב\nגגגג\nדדדד\n"

    def _build(self, tmp_path, key, records):
        (tmp_path / "k.txt").write_text(self.BODY, encoding="utf-8")
        (tmp_path / "k.versemap.json").write_text(
            json.dumps({"units": records}, ensure_ascii=False), encoding="utf-8")
        return build_sefaria(key, str(tmp_path), "k.txt", "k.versemap.json",
                             "REF2:k", {"REF2:k": norm_stream(self.BODY)[0]})

    def test_a_work_numbered_by_siman_says_so(self, tmp_path):
        built = self._build(tmp_path, "sef_halakhot_gedolot",
                            [{"chapter": 1, "start": 0}, {"chapter": 2, "start": 5}])
        assert [u.label_he for u in built.units] == ["סימן א", "סימן ב"]

    def test_a_work_the_table_does_not_name_keeps_the_bare_ordinal(self, tmp_path):
        """NEGATIVE CONTROL. A counting word applied everywhere would be a
        confidently wrong citation with every offset still right, and five works
        whose counting unit could not be established are deliberately absent from the
        table -- they wait on a ruling rather than on a guess."""
        built = self._build(tmp_path, "sef_rashi_isaiah",
                            [{"chapter": 11, "start": 0}, {"chapter": 12, "start": 5}])
        assert [u.label_he for u in built.units] == ["יא", "יב"]

    def test_a_dictionary_is_cited_by_its_headword(self, tmp_path):
        """Naming what the number counts cannot help here: the Arukh's staged number
        is a segment index no printed edition prints, so `ערך שע` is exactly as
        unlookupable as `שע`."""
        body = "שלום ראשון\nשמים שני\nתורה זרה\n"
        (tmp_path / "k.txt").write_text(body, encoding="utf-8")
        (tmp_path / "k.versemap.json").write_text(json.dumps({"units": [
            {"chapter": 1, "start": 0, "end": 11},
            {"chapter": 2, "start": 11, "end": 22},
            {"chapter": 3, "start": 22, "end": 33}]}, ensure_ascii=False),
            encoding="utf-8")
        built = build_sefaria("sef_arukh_letter_shin", str(tmp_path), "k.txt",
                              "k.versemap.json", "REF2:k",
                              {"REF2:k": norm_stream(body)[0]})
        # the third entry does not open with the filed letter, so it keeps its ordinal
        assert [u.label_he for u in built.units] == ["ערך שלום", "ערך שמים", "ג"]

    def test_the_filed_letter_is_read_off_the_file_not_transliterated(self, tmp_path):
        """No 22-row key-to-letter table to go stale: the modal first letter of the
        file's own entries IS the letter it is filed under."""
        body = "מלך גוף\nמנחה גוף\nמסכת גוף\nזרה גוף\n"
        starts = [0]
        for line in body.split("\n")[:-1]:
            starts.append(starts[-1] + len(line) + 1)
        records = [{"chapter": i + 1, "start": starts[i], "end": starts[i + 1]}
                   for i in range(4)]
        assert _arukh_headwords(records, body) == ["מלך", "מנחה", "מסכת", None]

    def test_a_repeated_headword_is_numbered_rather_than_left_ambiguous(self, tmp_path):
        """48.4% of Arukh entries share a headword with another entry. A citation
        that silently named five places would be worse than an ugly one."""
        body = "שפר אחד\nשפר שני\nשפר שלישי\n"
        (tmp_path / "k.txt").write_text(body, encoding="utf-8")
        (tmp_path / "k.versemap.json").write_text(json.dumps({"units": [
            {"chapter": 1, "start": 0, "end": 8},
            {"chapter": 2, "start": 8, "end": 16},
            {"chapter": 3, "start": 16, "end": 26}]}, ensure_ascii=False),
            encoding="utf-8")
        built = build_sefaria("sef_arukh_letter_shin", str(tmp_path), "k.txt",
                              "k.versemap.json", "REF2:k",
                              {"REF2:k": norm_stream(body)[0]})
        assert [u.label_he for u in built.units] == [
            "ערך שפר (1)", "ערך שפר (2)", "ערך שפר (3)"]
        assert check_invariants([built]) == []

    def test_without_the_occurrence_number_the_gate_reports_it(self, tmp_path):
        """PROVEN ABLE TO FAIL: drop `_disambiguate_labels` and three distinct stated
        addresses render one string."""
        collided = [Unit(i, i * 10, f"chapter:{i + 1}", "ערך שפר", i + 1,
                         (f"chapter:{i + 1}",)) for i in range(3)]
        problems = check_invariants([WorkUnits("REF2:k", "sefaria", "chapter",
                                               collided, 100)])
        assert any("name more than one place" in p for p in problems)

    def test_the_daf_kinds_are_untouched_by_either_repair(self, tmp_path):
        built = self._build(tmp_path, "sef_rif_berakhot",
                            [{"chapter": 1, "start": 0}, {"chapter": 2, "start": 5}])
        assert [u.label_he for u in built.units] == ['רי"ף א ע"א', 'רי"ף א ע"ב']


class TestTheYerushalmiSubUnitReadsHalakhah:
    """Owner ruling, reversing an argument the module itself used to make.

    The point of the assertions below is that this is a RENAME and not a renumbering:
    the ordinal, the part key and the citation position must all be the same
    afterwards, and the parser must still report what the source actually says.
    """

    def test_the_source_states_mishnah_and_the_reader_sees_halakhah(self):
        address = parse_canonical_header("פרק ח, משנה א")
        assert address.sub_kind == "משנה"          # RED if the PARSER is changed
        unit = _chapter_units([(address, 0)], with_sub=True)[0]
        assert unit.label_he == "פרק ח, הלכה א"     # RED if the map is reverted

    def test_the_ordinal_is_untouched(self):
        """A rename that renumbered would satisfy the label assertion and be wrong."""
        unit = _chapter_units([(parse_canonical_header("פרק ח, משנה א"), 0)],
                              with_sub=True)[0]
        assert unit.part_key == "ch:0.8.1"
        assert unit.citation_pos == 801
        # the sub KIND is a level too: the map renders `משנה` and `הלכה` alike, so
        # without it two headers stating different sub-units at one number would
        # record one address as well as one label, and the gate compares the two
        assert unit.source_address == ("", "8", "1", "משנה")

    def test_the_artifact_gate_reports_the_old_word(self):
        """PROVEN ABLE TO FAIL: the shipped labels, run through the gate."""
        stale = [Unit(0, 0, "ch:0.8.1", "פרק ח, משנה א", 801, ("", "8", "1"))]
        problems = check_invariants([WorkUnits(
            "M:y", "msource_header", "chapter_halakhah", stale, 500)])
        assert any("still render the sub-unit as משנה" in p for p in problems)

        fresh = [u._replace(label_he="פרק ח, הלכה א") for u in stale]
        assert check_invariants([WorkUnits(
            "M:y", "msource_header", "chapter_halakhah", fresh, 500)]) == []

    def test_two_different_sub_units_at_one_number_are_two_stated_places(self):
        """PROVEN ABLE TO FAIL, and this blind spot belonged to neither change that
        made it. Renaming `משנה` to `הלכה` made the label map non-injective; carrying
        the stated address as division/chapter/sub dropped the kind. Either alone is
        fine. Together, two headers stating DIFFERENT sub-units at the same number
        render one string AND record one address -- and the gate compares exactly
        those two things, so it would have gone quietly green."""
        marks = [(parse_canonical_header("פרק ג, משנה ה"), 0),
                 (parse_canonical_header("פרק ג, פסוק ז"), 400),
                 (parse_canonical_header("פרק ג, הלכה ה"), 800)]
        units = _chapter_units(marks, with_sub=True)
        assert [u.label_he for u in units] == [
            "פרק ג, הלכה ה", "פרק ג, פסוק ז", "פרק ג, הלכה ה"]
        problems = check_invariants([WorkUnits(
            "M:y", "msource_header", "chapter_halakhah", units, 2_000)])
        assert any("name more than one place" in p for p in problems)

        # with the kind dropped from the stated address -- the shape before this fix
        blind = [u._replace(source_address=u.source_address[:3]) for u in units]
        assert len({u.source_address for u in blind}) == 2   # three places, two addresses
        assert not any("name more than one place" in p for p in check_invariants(
            [WorkUnits("M:y", "msource_header", "chapter_halakhah", blind, 2_000)]))

    def test_a_range_still_shortens_on_the_new_word(self):
        units = _chapter_units(
            [(parse_canonical_header("פרק ג, משנה ד"), 0),
             (parse_canonical_header("פרק ג, משנה ה"), 90)], with_sub=True)
        runs, _ = citation_runs([(0, 1)], [u.citation_pos for u in units])
        assert render_ranges(runs, {u.citation_pos: u.label_he for u in units}) == (
            "פרק ג, הלכה ד–ה")


class TestTheDivisionSurvivesTheWalk:
    """The gate compares the label against the STATED address, so it is blind to a
    level dropped BEFORE both are built. That is the one hole it cannot close from
    inside, and it is the hole the original defect went through -- the division was
    parsed correctly and discarded one line later. So the plumbing is tested
    separately, at the point where the header text becomes a mark.
    """

    STANDALONE = "\n".join([
        f"##הלכות אלף, פרק א, הלכה א {PROVENANCE}##",
        "טקסט של הפרק הראשון",
        f"##הלכות אלף, פרק ב, הלכה א {PROVENANCE}##",
        "טקסט של הפרק השני",
        f"##הלכות בית, פרק א, הלכה א {PROVENANCE}##",
        "טקסט אחר לגמרי",
    ])

    def test_the_division_reaches_the_unit_from_the_header_text(self):
        units = _standalone_header_units(self.STANDALONE)
        assert [u.source_address[0] for u in units] == [
            "הלכות אלף", "הלכות אלף", "הלכות בית"]

    def test_reading_only_the_chapter_would_have_lost_it_silently(self):
        """PROVEN ABLE TO FAIL: the shipped shape of the defect. It is not an error
        -- the units still build, the offsets are still right, and the only symptom
        is a citation naming a place the fragment is not in."""
        addresses = [parse_canonical_header(line.strip("#").split("|")[0])
                     for line in self.STANDALONE.split("\n") if line.startswith("##")]
        assert [a.division for a in addresses] == [
            "הלכות אלף", "הלכות אלף", "הלכות בית"]
        assert [a.chapter for a in addresses] == ["א", "ב", "א"]   # what was kept
        assert len({a.chapter for a in addresses}) == 2            # three places, two


class TestEveryGrainRecordsTheStatedAddress:
    """The hole in the ambiguity gate, closed from the other side.

    `_ambiguity_problems` SKIPS a unit that records no stated address rather than
    assuming it clean, because a hand-built fixture legitimately has none. That makes
    a family which forgot to record one silently ungated -- so every real builder path
    is checked here to produce one. This is the artifact, not the payload: each grain
    is driven through its own entry point rather than asserted about in the abstract.
    """

    def _all_recorded(self, units):
        return units and all(u.source_address for u in units)

    def test_the_chapter_grain(self):
        assert self._all_recorded(_chapter_units([(addr("א"), 0), (addr("ב"), 50)]))

    def test_the_daf_grain_including_a_folio_nobody_can_read(self):
        offsets = list(range(400))
        units = _daf_units([(0, "יד", "א"), (100, "יד", "ב"), (200, "עמוד", "א")],
                           offsets, 2)
        assert self._all_recorded(units)
        assert units[2].label_he == ""          # unreadable, and still gated

    def test_the_ja_division_grain(self, tmp_path):
        raw = "***\nפלוני, ספר\n----------\n" + "\n".join(
            f"+פרק~ +{n}~\nגוף הפרק" for n in ("א", "ב", "ג", "ד"))
        path = tmp_path / "07-דוגמה.txt"
        path.write_text(raw, encoding="utf-8")
        built = build_ja(str(path), "J:x", {"J:x": norm_stream(raw)[0]})
        assert built.grain == "division"
        assert self._all_recorded(built.units)

    def test_the_ja_page_grain(self, tmp_path):
        doc = _ja_source_doc([("9", [(1, "+פרק~ +א~"), (2, "טקסט")]),
                              ("10", [(1, "עוד")])], author="פלוני", title="ספר")
        text, _ = ja_reconstruct(doc, "פלוני, ספר")
        path = tmp_path / "07-דוגמה.txt"
        path.write_text("***\nפלוני, ספר\n----------\n" + text.split("\n", 1)[1],
                        encoding="utf-8")
        shipped = {"J:x": norm_stream(path.read_text(encoding="utf-8"))[0]}
        built = build_ja_pages(str(path), "J:x", shipped,
                               {_source_title_key("פלוני, ספר"): doc})
        assert built.grain == "page"
        assert self._all_recorded(built.units)

    def test_the_staged_verse_grain(self, tmp_path):
        assert self._all_recorded(
            self._staged(tmp_path, {"units": [{"chapter": 1, "start": 0},
                                              {"chapter": 2, "start": 5}]}).units)

    def test_the_staged_section_grain(self, tmp_path):
        assert self._all_recorded(
            self._staged(tmp_path, {"sections": [{"section_he": "מגיד", "start": 0},
                                                 {"section_he": "הלל", "start": 5}]}).units)

    def _staged(self, tmp_path, sidecar):
        body = "אאאא\nבבבב\nגגגג\n"
        (tmp_path / "k.txt").write_text(body, encoding="utf-8")
        (tmp_path / "k.versemap.json").write_text(
            json.dumps(sidecar, ensure_ascii=False), encoding="utf-8")
        return build_sefaria("k", str(tmp_path), "k.txt", "k.versemap.json",
                             "REF2:k", {"REF2:k": norm_stream(body)[0]})


class TestCheckInvariants:
    def _work(self, units):
        return WorkUnits("w000001", "msource_header", "chapter", units, 1_000)

    def test_a_clean_table_reports_nothing(self):
        assert check_invariants([self._work([
            Unit(0, 0, "ch:1", "א", 1), Unit(1, 50, "ch:2", "ב", 2)])]) == []

    def test_a_table_whose_starts_descend_is_refused(self):
        problems = check_invariants([self._work([
            Unit(0, 90, "ch:1", "א", 1), Unit(1, 10, "ch:2", "ב", 2)])])
        assert any("ascending" in p for p in problems)

    def test_duplicate_start_offsets_are_refused(self):
        problems = check_invariants([self._work([
            Unit(0, 7, "ch:1", "א", 1), Unit(1, 7, "ch:2", "ב", 2)])])
        assert any("duplicate" in p for p in problems)

    def test_a_unit_past_the_end_of_the_stream_is_refused(self):
        problems = check_invariants([self._work([
            Unit(0, 0, "ch:1", "א", 1), Unit(1, 5_000, "ch:2", "ב", 2)])])
        assert any("past the end" in p for p in problems)

    def test_a_gap_in_the_ordinals_is_refused(self):
        problems = check_invariants([self._work([
            Unit(0, 0, "ch:1", "א", 1), Unit(2, 50, "ch:2", "ב", 2)])])
        assert any("ordinals" in p for p in problems)

    def test_the_clean_case_really_does_pass(self):
        """Without this the failure controls above prove only that SOMETHING is
        rejected -- possibly everything, which would be a gate that blocks the
        build rather than one that discriminates."""
        assert check_invariants([self._work([
            Unit(0, 0, "ch:1", "א", 1),
            Unit(1, 50, "ch:2", "ב", 2),
            Unit(2, 999, "ch:3", "ג", 3)])]) == []


# ---------------------------------------------------------------------------
# The artifact
# ---------------------------------------------------------------------------

class TestWriteArtifact:
    def test_the_units_round_trip(self, tmp_path):
        path = str(tmp_path / "work_divisions.db")
        write_artifact(path, [WorkUnits("w1", "ja", "division", [
            Unit(0, 0, "ja:0", "פרק א", 0), Unit(1, 40, "ja:1", "פרק ב", 1)], 500)])
        conn = sqlite3.connect(path)
        assert conn.execute("SELECT unit_count FROM locus_work").fetchone()[0] == 2
        rows = conn.execute(
            "SELECT unit_ord, start_offset, label_he FROM locus_unit ORDER BY unit_ord"
        ).fetchall()
        assert rows == [(0, 0, "פרק א"), (1, 40, "פרק ב")]
        conn.close()

    def test_two_units_may_share_a_part_key(self, tmp_path):
        """46 of 87 marker-bearing works visit the same folio twice, so a UNIQUE
        constraint on (work, part_key) would reject real tables."""
        path = str(tmp_path / "dup.db")
        write_artifact(path, [WorkUnits("w1", "msource_daf", "daf2", [
            Unit(0, 0, "daf:57.1", 'נז ע"א', 115),
            Unit(1, 90, "daf:56.2", 'נו ע"ב', 113),
            Unit(2, 180, "daf:57.1", 'נז ע"א', 115)], 500)])
        conn = sqlite3.connect(path)
        assert conn.execute(
            "SELECT COUNT(*) FROM locus_unit WHERE part_key='daf:57.1'").fetchone()[0] == 2
        conn.close()

    def test_rebuilding_over_an_existing_artifact_replaces_it(self, tmp_path):
        path = str(tmp_path / "twice.db")
        write_artifact(path, [WorkUnits("w1", "ja", "division",
                                        [Unit(0, 0, "a", "א", 0)], 10)])
        write_artifact(path, [WorkUnits("w2", "ja", "division",
                                        [Unit(0, 0, "b", "ב", 0)], 10)])
        conn = sqlite3.connect(path)
        assert [r[0] for r in conn.execute("SELECT locus_ref_id FROM locus_work")] == ["w2"]
        conn.close()


# ---------------------------------------------------------------------------
# The staged-versemap path, end to end on a fabricated pair
# ---------------------------------------------------------------------------

class TestBuildSefaria:
    def _stage(self, tmp_path, body, units):
        (tmp_path / "b.txt").write_text(body, encoding="utf-8")
        (tmp_path / "b.versemap.json").write_text(
            json.dumps({"key": "b", "structure": "verse", "units": units},
                       ensure_ascii=False), encoding="utf-8")
        return norm_stream(body)[0]

    def test_a_chapter_work_resolves_its_units_into_stream_space(self, tmp_path):
        body = "אאא, בבב. גגג"
        stream = self._stage(tmp_path, body, [
            {"ref": "X 1:1", "chapter": 1, "verse": 1, "start": 0, "end": 3},
            {"ref": "X 2:1", "chapter": 2, "verse": 1, "start": 5, "end": 8},
            {"ref": "X 3:1", "chapter": 3, "verse": 1, "start": 10, "end": 13},
        ])
        built = build_sefaria("sef_rashi_genesis", str(tmp_path), "b.txt",
                              "b.versemap.json", "REF2:b", {"REF2:b": stream})
        assert built is not None
        assert built.grain == "chapter"
        assert [u.label_he for u in built.units] == ["א", "ב", "ג"]
        assert [u.start for u in built.units] == [0, 3, 6]

    def test_a_work_whose_stream_does_not_rebuild_gets_no_units_at_all(self, tmp_path):
        """Fail closed. Not close enough: one wrong character shifts every offset."""
        stream = self._stage(tmp_path, "אאא בבב", [
            {"ref": "X 1:1", "chapter": 1, "verse": 1, "start": 0, "end": 3}])
        assert build_sefaria("sef_rashi_genesis", str(tmp_path), "b.txt",
                             "b.versemap.json", "REF2:b",
                             {"REF2:b": stream + "ד"}) is None

    def test_a_bavli_commentary_renders_its_index_as_a_folio(self, tmp_path):
        stream = self._stage(tmp_path, "אאא בבב", [
            {"ref": "T on X 3:1", "chapter": 3, "verse": 1, "start": 0, "end": 3},
            {"ref": "T on X 4:1", "chapter": 4, "verse": 1, "start": 4, "end": 7},
        ])
        built = build_sefaria("sef_tosafot_shabbat", str(tmp_path), "b.txt",
                              "b.versemap.json", "REF2:b", {"REF2:b": stream})
        assert [u.label_he for u in built.units] == ['ב ע"א', 'ב ע"ב']

    def test_a_rif_folio_says_whose_foliation_it_is(self, tmp_path):
        stream = self._stage(tmp_path, "אאא בבב", [
            {"ref": "Rif X 1:1", "chapter": 1, "verse": 1, "start": 0, "end": 3}])
        built = build_sefaria("sef_rif_berakhot", str(tmp_path), "b.txt",
                              "b.versemap.json", "REF2:b", {"REF2:b": stream})
        assert built.units[0].label_he == 'רי"ף א ע"א'

    def test_the_sub_index_is_not_shown(self, tmp_path):
        """Only 25.8% of daf-family spans sit inside a single numbered comment, so
        a `2a §1` would be wrong or misleading for roughly three rows in four."""
        stream = self._stage(tmp_path, "אאא בבב", [
            {"ref": "T on X 3:1", "chapter": 3, "verse": 1, "start": 0, "end": 3},
            {"ref": "T on X 3:2", "chapter": 3, "verse": 2, "start": 4, "end": 7},
        ])
        built = build_sefaria("sef_tosafot_shabbat", str(tmp_path), "b.txt",
                              "b.versemap.json", "REF2:b", {"REF2:b": stream})
        assert len(built.units) == 1                      # one amud, not two comments
        assert built.units[0].label_he == 'ב ע"א'
        assert "§" not in built.units[0].label_he

    def test_a_work_with_no_versemap_is_not_addressable(self, tmp_path):
        (tmp_path / "b.txt").write_text("אאא", encoding="utf-8")
        assert build_sefaria("k", str(tmp_path), "b.txt", None, "REF2:b", {}) is None


class TestMsourceFileResolution:
    def test_an_edition_is_located_by_its_exact_number(self, tmp_path):
        for name in ("a--Ytext1000.txt", "b--Ytext10001.txt", "c--Ytext28000.txt"):
            (tmp_path / name).write_text("", encoding="utf-8")
        found = _msource_files(str(tmp_path))
        assert found["1000"] == "a--Ytext1000.txt"
        assert found["10001"] == "b--Ytext10001.txt"

    def test_a_substring_rule_would_have_resolved_the_wrong_edition(self, tmp_path):
        """The defect, proven able to fail: 60 of the 8,233 numbers are a strict
        prefix of another, and the research tree's rule demands exactly one hit."""
        for name in ("a--Ytext1000.txt", "b--Ytext10001.txt"):
            (tmp_path / name).write_text("", encoding="utf-8")
        by_substring = [f for f in os.listdir(str(tmp_path)) if "Ytext1000" in f]
        assert len(by_substring) == 2                     # ambiguous, so it aborts
        assert _msource_files(str(tmp_path))["1000"] == "a--Ytext1000.txt"

    def test_a_split_child_id_would_have_matched_no_file_at_all(self):
        """`'M:Ytext1000_00' in filename` is 0 hits: the prefix is in no filename."""
        assert "M:Ytext1000_00" not in "a--Ytext1000.txt"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
