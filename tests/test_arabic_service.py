# -*- coding: utf-8 -*-
"""shared/arabic_service.py on a synthetic sidecar written by the real builder.

The lookup is the plan's method v4 over the sidecar: the readings ranked by the stored count, the
first reading's root through the closed-class table, Lane's headwords, then stripping, and the
root's entries with the matched ones first. Fail-closed: a missing file, another schema version, a
sidecar keyed by another normalizer, one without the Perseus credit line, a missing lookup index,
or the kill switch off each leave the service unavailable and every lookup empty.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import scripts.build_arabic_sidecar as build
from shared import arabic_service
from shared.arabic_normalize import root_key
from shared.arabic_service import ArabicService, cut_text, entry_url, lane_heading, root_url
from tests.test_build_arabic_sidecar import METADATA_SHA256, SHARDS, make_closed_class, make_source


@pytest.fixture
def sidecar(tmp_path, monkeypatch) -> Path:
    monkeypatch.setattr(build, "EMEIRI_SHARDS", SHARDS)
    monkeypatch.setattr(build, "OPENITI_METADATA_SHA256", METADATA_SHA256)
    monkeypatch.delenv("LANE_DICTIONARY_ENABLED", raising=False)
    source = make_source(tmp_path / "src")
    closed = make_closed_class(tmp_path / "closed.json")
    out = tmp_path / "arabic.db"
    assert build.main(["build", "--source-dir", str(source), "--out", str(out), "--closed-class", str(closed),
                       "--workers", "1"]) == build.EXIT_OK
    return out


def _service(path: Path) -> ArabicService:
    service = ArabicService(str(path))
    assert service.is_ready(), service.unavailable_reason
    return service


def test_the_reader_understands_the_builder_s_schema():
    assert arabic_service.SCHEMA_VERSION == build.SCHEMA_VERSION
    assert arabic_service.PERSEUS_CREDIT == build.PERSEUS_CREDIT
    assert set(arabic_service.COUNTED_TABLES) == set(build.COUNTED_TABLES)


def test_explicit_plural_lists_skip_qualifications_and_do_not_index_examples():
    extract = arabic_service.explicit_plural_forms
    assert extract('the pl. (of the former, S) is بُلْدَانٌ (S) and (or) بِلَادٌ: text') == ('بلدان', 'بلاد')
    assert extract('pl. كُتُبٌ; a sentence كتاب آخر') == ('كتب',)
    assert extract('pl. of كِتَابٌ; and an example كتب') == ()
    assert extract('The text happens to contain بلاد.') == ()
    assert extract('the pl. of مَعْنًى is مَعَانٍ.', 'عنW') == ('معان', 'معاني')
    assert extract('pl. كِتَابٍ.', 'كتب') == ('كتاب',), 'kasratan alone does not license a final yod'


def test_verbal_noun_lists_are_explicit_and_stop_before_examples():
    extract = arabic_service.explicit_verbal_nouns
    assert extract('inf. n. سُلُوكٌ (X) and سَلْكٌ; example طريق') == ('سلوك', 'سلك')
    assert extract('inf. n. [of this verb]; an example سلوك') == ()
    assert extract('An unrelated quotation سلوك الطريق.') == ()


def test_printed_verbal_noun_keeps_a_final_root_consonant(sidecar):
    with sqlite3.connect(sidecar) as con:
        con.execute("UPDATE lane_entries SET text='synthetic; inf. n. كُتُوبٌ; example كلام' WHERE id='lane:t:1'")
    svc = _service(sidecar)
    result = svc.lookup('والكتوب')
    first = result.roots[0].entries[0]
    assert first.entry_id == 'lane:t:1' and first.match_kind == 'verbal_noun'
    assert first.matched_form == 'كتوب' and first.matched
    svc.close()


def test_closed_class_matches_survive_conjunctions_and_spelling_aliases(sidecar):
    svc = _service(sidecar)
    for word in ('דאלך', '[דא]לך', 'ודאלך', 'ודלך'):
        result = svc.lookup(word)
        assert result.roots[0].entries[0].entry_id == 'lane:t:5', word
    svc.close()


def test_missing_personal_pronoun_does_not_fall_back_to_an_unrelated_verb(sidecar):
    svc = _service(sidecar)
    for query in ('نحن', 'ونحن'):
        result = svc.lookup(query)
        assert result.readings and not result.roots
    svc.close()


def test_numbered_finite_verb_precedes_an_inferred_feminine_noun(sidecar):
    with sqlite3.connect(sidecar) as con:
        con.execute("UPDATE lane_entries SET text='1. A synthetic verb.' WHERE id='lane:t:1'")
        con.execute("UPDATE lane_entries SET headword='كُتْبَةٌ' WHERE id='lane:t:2'")
        con.execute("UPDATE lane_forms SET skeleton='كتبه' WHERE entry_id='lane:t:2'")
    svc = _service(sidecar)
    result = svc.lookup('كتبت')
    first = result.roots[0].entries[0]
    assert (first.entry_id, first.match_kind) == ('lane:t:1', 'verb_form')
    assert any(e.entry_id == 'lane:t:2' for e in result.roots[0].entries)
    assert svc.lookup('كتبه').roots[0].entries[0].entry_id == 'lane:t:2'
    svc.close()


def test_article_prefers_a_noun_to_a_homographic_numbered_verb(sidecar):
    with sqlite3.connect(sidecar) as con:
        con.execute("UPDATE lane_entries SET text='1. A synthetic verb.' WHERE id='lane:t:1'")
        con.execute("UPDATE lane_forms SET skeleton='كتب' WHERE entry_id='lane:t:2'")
    svc = _service(sidecar)
    assert svc.lookup('الكتب').roots[0].entries[0].entry_id == 'lane:t:2'
    assert svc.lookup('كتب').roots[0].entries[0].entry_id == 'lane:t:1'
    svc.close()


def test_attested_conjoined_noun_keeps_priority_over_a_possible_imperfect(sidecar):
    with sqlite3.connect(sidecar) as con:
        con.execute("UPDATE lane_entries SET text='1. A synthetic verb.' WHERE id='lane:t:1'")
        con.execute("UPDATE lane_entries SET headword='يَكْتُبٌ' WHERE id='lane:t:2'")
        con.execute("UPDATE lane_forms SET skeleton='يكتب' WHERE entry_id='lane:t:2'")
    svc = _service(sidecar)
    assert svc.lookup('ويكتب').roots[0].entries[0].entry_id == 'lane:t:2'
    svc.close()


def test_a_direct_headword_precedes_a_printed_plural_homograph(sidecar):
    with sqlite3.connect(sidecar) as con:
        con.execute("UPDATE lane_entries SET text='sample; pl. كِتَابٌ: sample' WHERE id='lane:t:1'")
    service = _service(sidecar)
    result = service.lookup('كِتَاب')
    assert result.roots[0].entries[0].entry_id == 'lane:t:2'
    assert result.roots[0].entries[0].matched
    assert next(e for e in result.roots[0].entries if e.entry_id == 'lane:t:1').match_kind == 'plural'


def test_prefixed_construct_noun_uses_its_lexical_form_without_a_frequency_hit(sidecar):
    with sqlite3.connect(sidecar) as con:
        con.execute("UPDATE lane_entries SET headword='كِتَابَةٌ' WHERE id='lane:t:2'")
        con.execute("UPDATE lane_forms SET skeleton='كتابه' WHERE entry_id='lane:t:2'")
    service = _service(sidecar)
    for query in ('בכתאבת', 'وبكتابتك'):
        result = service.lookup(query)
        assert result.roots[0].entries[0].entry_id == 'lane:t:2'
        assert result.roots[0].entries[0].matched


def test_the_commonest_reading_first_and_its_closed_class_root(sidecar):
    """דלך: ذلك is counted 4 times, دلك once (not stored); the closed-class table files ذلك under
    Lane's ذا and names that headword."""
    service = _service(sidecar)
    for word in ("דלך", "ד'לך"):
        result = service.lookup(word)
        assert result.script == "ja" and [r.skeleton for r in result.readings] == ["ذلك"]
        assert result.readings[0].count == 4
        [root] = result.roots
        assert root.root_key == root_key("ذا") and root.name == "ذا"
        assert [(e.entry_id, e.matched) for e in root.entries] == [("lane:t:5", True)]
        assert root.entries[0].url == entry_url("lane:t:5") and root.url == root_url("ذا")
    assert service.credit == build.PERSEUS_CREDIT


def test_a_preposition_with_a_pronoun_reaches_the_letter_s_article(sidecar):
    [root] = _service(sidecar).lookup("לנא").roots
    assert root.root_key == "ل" and root.entries[0].entry_id == "lane:t:4" and root.entries[0].matched


def test_the_headword_table_finds_the_root_and_its_entry_comes_first(sidecar):
    result = _service(sidecar).lookup("כתאב")
    assert [r.skeleton for r in result.readings] == []  # كتاب is counted once: not offered
    result = _service(sidecar).lookup("כתב")
    assert [r.skeleton for r in result.readings] == ["كتب"]
    [root] = result.roots
    assert root.root_key == "كتب"
    assert [(e.entry_id, e.matched) for e in root.entries] == [("lane:t:1", True), ("lane:t:2", False)]


def test_typed_arabic_is_its_own_reading(sidecar):
    """No readings step: كِتَاب is looked up as it is, even uncounted, and reaches كتاب's entry."""
    result = _service(sidecar).lookup("كِتَاب")
    assert result.script == "arabic" and [(r.skeleton, r.spelling) for r in result.readings] == [("كتاب", "كتاب")]
    [root] = result.roots
    assert root.root_key == "كتب" and root.entries[0].entry_id == "lane:t:2" and root.entries[0].matched
    assert _service(sidecar).lookup("ذلك الكتاب").readings == (), "one word only"


def test_nothing_for_an_abbreviation_a_word_with_no_reading_or_no_letters(sidecar):
    service = _service(sidecar)
    # "ד לך" joined would be דלך, which is counted: two words are never read as one.
    for word in ("ר'", "קקק", "abc", "", "   ", "ד לך", "ד־לך", "ד-לך", "כל מה"):
        result = service.lookup(word)
        assert not result.found and not result.readings, word


def test_one_word_survives_its_dot_marks_and_niqqud():
    from shared.arabic_service import one_ja_word
    for word in ("דלך", "ד'לך", "ד׳לך", "ד̇לך", "ד‘לך", "ד’לך", "ד[ל]ך", "דָלִךְ",
                 "מוּסא", "מוּסא", "ר'"):
        assert one_ja_word(word), word
    for word in ("ד לך", "ד־לך", "ד-לך", "דלך كتاب", "דלך-كتاب", "דלךabc", "abc", ""):
        assert not one_ja_word(word), word


def test_the_entries_are_bounded_and_the_rest_counted(sidecar, monkeypatch):
    monkeypatch.setattr(arabic_service, "MAX_ENTRIES", 1)
    [root] = _service(sidecar).lookup("כתב").roots
    assert [e.entry_id for e in root.entries] == ["lane:t:1"] and root.more_entries == 1


def test_a_root_carries_every_entry_and_a_cut_entry_its_whole_text(sidecar, monkeypatch):
    """Nothing only on the site. MAX_ENTRIES is a bound above Lane's largest root (69
    entries); a cut entry carries its whole text for the panel's "Show the full entry"."""
    assert arabic_service.MAX_ENTRIES >= 69
    monkeypatch.setattr(arabic_service, "MAX_TEXT", 5)
    [root] = _service(sidecar).lookup("כתב").roots
    assert root.more_entries == 0 and len(root.entries) == 2
    for entry in root.entries:
        assert entry.cut and len(entry.full) > len(entry.text) - 1 and entry.full.startswith(entry.text[:3])
    monkeypatch.setattr(arabic_service, "MAX_TEXT", 10_000)
    [root] = _service(sidecar).lookup("כתב").roots
    assert all(not e.cut and e.full == "" for e in root.entries), "an uncut entry carries no second copy"


@pytest.mark.parametrize("root, reading, shapes", [
    # An adverbial accusative's alif: أيضا is cited as أيض.
    ("ءWض", "ايضا", ["ايضا", "ايض"]),
    # A perfect's subject ending goes, an object pronoun stays: طالبته is cited as طالبه.
    ("طلب", "طالبته", ["طالبته", "طالبه"]),
    ("طلب", "طالبت", ["طالبت", "طالب"]),
    ("كتب", "كتبنا", ["كتبنا", "كتب"]),
    # A doubled root's contracted last letter, written twice: استقر is cited as استقرر.
    ("قرر", "استقر", ["استقر", "استقرر"]),
    # Nothing to undo; a weak root's key is not a doubled one.
    ("كWن", "يكون", ["يكون"]),
    ("ذWW", "ذا", ["ذا"]),
    ("ءWض", "اضا", ["اضا"]),   # three letters: no alif taken off
])
def test_the_shapes_a_lane_headword_cites_a_reading_by(root, reading, shapes):
    from shared.arabic_service import _head_shapes
    assert _head_shapes(root, reading) == shapes


def test_the_other_shapes_count_only_when_the_reading_itself_names_no_entry(sidecar, monkeypatch):
    """بنت is cited as it is: its shape بن (a perfect's ending taken off) must not mark another
    headword. طالبته names none of its root's headwords as it is, so طالبه is tried."""
    service = _service(sidecar)
    asked: list[list[str]] = []

    def forms(found):
        def matching(root_key, shapes):
            asked.append(list(shapes))
            return {"lane:t:own"} if found(shapes) else set()
        return matching

    monkeypatch.setattr(service, "_forms_matching", forms(lambda shapes: "بنت" in shapes))
    assert service._matched_entries("بنو", "بنت") == {"lane:t:own"} and asked == [["بنت"]], asked
    asked.clear()
    monkeypatch.setattr(service, "_forms_matching", forms(lambda shapes: "طالبه" in shapes))
    assert service._matched_entries("طلب", "طالبته") == {"lane:t:own"}
    assert asked == [["طالبته"], ["طالبه"]], asked


def test_a_long_text_is_cut_at_a_space():
    text, cut = cut_text("word " * 200, limit=100)
    assert cut and text.endswith("…") and len(text) <= 101 and not text[:-1].endswith(" ")
    assert cut_text("  a short\n entry ", limit=100) == ("a short entry", False)


def _mutated(sidecar: Path, sql: str) -> ArabicService:
    con = sqlite3.connect(sidecar)
    con.executescript(sql)
    con.commit()
    con.close()
    return ArabicService(str(sidecar))


@pytest.mark.parametrize("sql, reason", [
    ("UPDATE meta SET value = '9.9.9' WHERE key = 'version'", "schema version"),
    ("UPDATE meta SET value = '99' WHERE key = 'normalizer_version'", "normalizer"),
    ("UPDATE meta SET value = '' WHERE key = 'lane_credit'", "credit"),
    ("UPDATE meta SET value = 'Text provided by Perseus.' WHERE key = 'lane_credit'", "Perseus credit line"),
    ("UPDATE meta SET value = '3' WHERE key = 'freq_min_stored'", "freq_min_stored"),
    ("DELETE FROM closed_class", "rows_closed_class"),
    ("DELETE FROM lane_forms WHERE entry_id = 'lane:t:1'", "rows_lane_forms"),
    ("DROP INDEX idx_lane_forms_skeleton", "index"),
    ("DROP TABLE closed_class", "missing table"),
    ("DELETE FROM freq WHERE skeleton = 'كتب'", "rows_freq"),
])
def test_a_sidecar_it_cannot_vouch_for_is_not_served(sidecar, sql, reason):
    service = _mutated(sidecar, sql)
    assert not service.is_ready() and reason in service.unavailable_reason, service.unavailable_reason
    assert not service.lookup("דלך").found


def test_a_missing_file_and_the_kill_switch(sidecar, tmp_path, monkeypatch):
    missing = ArabicService(str(tmp_path / "nope.db"))
    assert not missing.is_ready() and "no sidecar" in missing.unavailable_reason
    assert not missing.lookup("דלך").found
    service = _service(sidecar)
    monkeypatch.setenv("LANE_DICTIONARY_ENABLED", "0")
    assert not service.is_available() and not service.lookup("דלך").found
    monkeypatch.setenv("LANE_DICTIONARY_ENABLED", "1")
    assert service.lookup("דלך").found


def test_an_editors_switch_serves_editors_only_and_the_lookup_answers(sidecar, monkeypatch):
    service = _service(sidecar)
    monkeypatch.setenv("LANE_DICTIONARY_ENABLED", "editors")
    assert not arabic_service.lane_enabled() and not service.is_available()
    assert service.served_to(True) and not service.served_to(False) and service.lookup("דלך").found


def test_a_card_on_a_merged_root_is_titled_with_the_heading_of_its_entry():
    """Lane files بوع and بيع (سود and سيد, بقو and بقى) apart, under one root key; the two AI judges
    of plan §5.4 marked يبيعه under بوع wrong. The card names the heading of the first matched
    entry; else the heading with the most entries (the commoner sense); else the key's name. Not the
    word's letters: سيّد and جيّد carry a ي but are of سود and جود."""
    entries = [("e1", "سود"), ("e2", "سود"), ("e3", "سيد")]
    assert lane_heading(["سود", "سيد"], entries, {"e3"}) == "سيد", "the matched entry, before the sizes"
    assert lane_heading(["سود", "سيد"], entries, {"e2", "e3"}) == "سود", "the first matched, in Lane's order"
    sell = [("b1", "بوع"), ("b2", "بيع"), ("b3", "بيع")]
    assert lane_heading(["بوع", "بيع"], sell, set()) == "بيع", "nothing matched: the larger heading"
    assert lane_heading(["بوع", "بيع"], sell[:2], set()) == "", "a tie: the key's name"
    assert lane_heading(["بيع"], sell, set()) == "", "one heading: nothing to choose"
    assert lane_heading(["بوع", "بيع"], sell + [("x", "عز")], {"x"}) == "بيع",         "a matched entry under no heading of the key is ignored"


def test_the_service_titles_a_merged_root_card_with_its_entry_s_heading(sidecar):
    """The call site: a key whose entries Lane files under a second heading is titled, linked and
    paged by that heading, not by the key's first name."""
    con = sqlite3.connect(sidecar)
    with con:
        con.execute("UPDATE lane_roots SET names_json = ? WHERE root_key = ?", ('["كتب", "كتبى"]', "كتب"))
        con.execute("UPDATE lane_entries SET root = ? WHERE root_key = ?", ("كتبى", "كتب"))
    con.close()
    [root] = _service(sidecar).lookup("כתב").roots
    assert root.root_key == "كتب" and root.name == "كتبى" and root.url == root_url("كتبى")


@pytest.mark.parametrize('closed_homograph', [False, True])
def test_unfrequent_feminine_construct_reaches_the_exact_noun_first(sidecar, closed_homograph):
    with sqlite3.connect(sidecar) as conn:
        conn.execute("INSERT INTO lane_roots VALUES ('مدن', 'مدن', '[]', 99, 2, 1)")
        conn.execute("INSERT INTO lane_entries VALUES ('synthetic:verb', 'مدن', 'مدن', 99, 'مَدَنَ', 1, '1', 'synthetic verb')")
        conn.execute("INSERT INTO lane_entries VALUES ('synthetic:city', 'مدن', 'مدن', 100, 'مَدِينَةٌ', 1, '1', 'synthetic city noun')")
        conn.execute("INSERT INTO lane_forms VALUES ('مدينه', 'مدن', 'synthetic:city')")
        if closed_homograph:
            conn.execute("INSERT INTO closed_class VALUES ('مدينه', 'مدينه', 'مدن', 'synthetic:verb', 'synthetic homograph', 99)")
        for table in ('lane_roots', 'lane_entries', 'lane_forms', 'closed_class'):
            conn.execute('UPDATE meta SET value=? WHERE key=?',
                         (str(conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]), f'rows_{table}'))
    service = _service(sidecar)
    for query in ('מדינת', 'מדינתך'):
        result = service.lookup(query)
        assert result.roots[0].entries[0].entry_id == 'synthetic:city'
        assert result.roots[0].entries[0].matched
        assert result.roots[0].inferred
    service.close()


def test_an_arabic_word_reaches_a_numbered_stem_as_the_judeo_arabic_one_does(sidecar):
    """يكتتبون reached no entry while יכתתבון reached stem 8 of كتب: the verb forms are generated in
    Hebrew letters, and an Arabic word is now read into them letter for letter."""
    with sqlite3.connect(sidecar) as con:
        con.execute("UPDATE lane_entries SET text='8. A synthetic verb.' WHERE id='lane:t:1'")
    svc = _service(sidecar)
    for word in ("יכתתבון", "يكتتبون"):
        roots = svc.lookup(word).roots
        assert roots and roots[0].root_key == "كتب", word
        assert any(e.entry_id == "lane:t:1" and e.matched for e in roots[0].entries), word
    svc.close()

