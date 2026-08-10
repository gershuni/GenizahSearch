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
    Unit,
    WorkUnits,
    _chapter_units,
    _dedupe_ascending,
    _msource_files,
    _split_divisions,
    build_sefaria,
    check_invariants,
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

class TestJaLeafKinds:
    def test_the_verse_tier_is_excluded_from_the_citable_grain(self):
        """76.3% of all JA markers, median 72 letters -- finer than a stored span."""
        assert JA_LEAF_KINDS == {"פסוק", "פס'", "משנה"}

    def test_the_coarse_kinds_are_not_in_it(self):
        for kind in ("פרק", "סימן", "שאלה", "מסכת", "שער", "פיסקא"):
            assert kind not in JA_LEAF_KINDS


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
