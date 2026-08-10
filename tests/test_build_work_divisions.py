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

import json
import os
import sqlite3

import pytest

from scripts.build_work_divisions import (
    JA_LEAF_KINDS,
    Edition,
    Unit,
    WorkUnits,
    _chapter_units,
    _clean_marker_text,
    _dedupe_ascending,
    _msource_files,
    _source_title_key,
    _split_divisions,
    _tree_reading_order,
    bind_tree_chains,
    build_ja_pages,
    build_sefaria,
    check_invariants,
    ja_edition,
    ja_reconstruct,
    ja_source_index,
    ja_tree_chain,
    resolve_tree,
    tree_alignment,
    sefaria_render_kind,
    write_artifact,
)
from shared.discovery_locus import norm_stream

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

    def test_each_payload_line_carries_the_chapter_in_force_at_that_point(self):
        divisions = _split_divisions(MONOLITH)
        assert divisions[0][2] == ["א", "א", "ב"]

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

class TestChapterUnits:
    def test_one_unit_per_chapter_at_the_offset_the_chapter_opens(self):
        units = _chapter_units([("א", 0), ("א", 20), ("ב", 55)])
        assert [(u.label_he, u.start) for u in units] == [("א", 0), ("ב", 55)]

    def test_a_run_of_one_label_collapses_to_one_unit(self):
        """The Yerushalmi interleaves a main and a variant segment under one
        chapter -- y.Berakhot carries 1,151 headers for 9 chapters."""
        marks = [("א", i * 10) for i in range(600)] + [("ב", 6000)]
        assert len(_chapter_units(marks)) == 2

    def test_not_collapsing_would_have_produced_hundreds_of_duplicate_labels(self):
        """The defect, proven able to fail."""
        def buggy(marks):
            return [(label, offset) for label, offset in marks if label is not None]

        marks = [("א", i * 10) for i in range(600)] + [("ב", 6000)]
        assert len(buggy(marks)) == 601
        assert len(_chapter_units(marks)) == 2

    def test_the_citation_position_is_the_numeral_the_label_denotes(self):
        units = _chapter_units([("א", 0), ("ב", 10), ("טו", 20)])
        assert [u.citation_pos for u in units] == [1, 2, 15]

    def test_an_unparsed_header_contributes_no_unit(self):
        assert _chapter_units([(None, 0), ("א", 10)]) == [
            Unit(0, 10, "ch:1", "א", 1)
        ]

    def test_ordinals_are_a_dense_run_from_zero(self):
        units = _chapter_units([("א", 0), ("ב", 10), ("ג", 20)])
        assert [u.unit_ord for u in units] == [0, 1, 2]


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
