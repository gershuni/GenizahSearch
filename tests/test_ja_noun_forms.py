# -*- coding: utf-8 -*-
"""The noun forms a clicked word may be (plurals, dual, the accusative's alif, later pronoun
spellings, a doubled glide), and the verb forms added with them (four-letter roots, the verbal
nouns of stems V and VI). Synthetic dictionary only."""
from __future__ import annotations

from shared.blau_normalize import (
    ACCUSATIVE, BROKEN_PLURAL, DUAL, FEMININE_PLURAL, PLURAL, analysis_paths, broken_plural_singulars, evidence_cost,
    plural_analyses, query_analyses,
)
from shared.blau_service import BlauService
from shared.ja_verb_forms import finite_form, nominal_forms, stem_forms
from tests.test_blau_service import _build, _form, _root, _seg, dictionary


def test_the_singulars_a_broken_plural_s_pattern_gives():
    def gives(plural, singular):
        return singular in broken_plural_singulars(plural)

    assert gives("אבראג", "ברג") and gives("אפמאמ", "פמ"), "אC1C2אC3, a doubled root named with two letters"
    assert gives("תמאר", "תמרה") and gives("תכות", "תכת") and gives("כטוט", "כט") and gives("עביד", "עבד")
    assert gives("קואדס", "קאדוס") and gives("כצאיל", "כצלה")
    assert gives("דראדר", "דרדור") and gives("מחאזיר", "מחזור") and gives("מראזיב", "מרזבה") and gives("בראהינ", "ברהאנ")
    assert broken_plural_singulars("כתב") == () and broken_plural_singulars("כתאב אלמלכ") == ()
    assert "תמאר" not in broken_plural_singulars("תמאר"), "never the word itself"


def test_each_noun_form_names_its_kind():
    def kinds(word):
        return {a.key: a.steps[-1] for a in plural_analyses(word)}

    assert kinds("כאתבינ")["כאתב"] == PLURAL
    assert kinds("אלדיאנאת")["דיאנה"] == FEMININE_PLURAL and kinds("אלדיאנאת")["דיאנ"] == FEMININE_PLURAL
    assert kinds("אפלתאנ")["אפלה"] == DUAL and kinds("כתאבאנ")["כתאב"] == DUAL
    assert kinds("גלילא")["גליל"] == ACCUSATIVE
    assert kinds("אלתמאר")["תמרה"] == BROKEN_PLURAL
    assert all(a.nominal for a in plural_analyses("אלתמאר"))


def test_later_pronoun_spellings_and_a_doubled_glide_on_the_reader_s_side_only():
    keys = {a.key: a for a in query_analyses("בתאעתהו")}
    assert keys["בתאעת"].steps == ("suffix",) and keys["בתאעה"].steps == ("suffix", "feminine")
    assert "טהור" in {a.key for a in query_analyses("טהורהי")}
    assert {a.key for a in query_analyses("בהו")} == {a.key for a in query_analyses("בה")} - {"בה"} | {"בהו", "הו"}, \
        "too short a word to peel a pronoun from"
    assert "מתצור" in {a.key for a in query_analyses("מתצוור")} and "יטיב" in {a.key for a in query_analyses("יטייב")}


def test_a_noun_form_explains_a_match_only_when_no_plainer_analysis_does():
    assert analysis_paths("אלתמאר", "תמרה")[0] == ("prefix", BROKEN_PLURAL)
    assert analysis_paths("כתאבה", "כתאב") == (("suffix",),), "not the costlier noun form"
    assert evidence_cost((("suffix",),)) < evidence_cost(((ACCUSATIVE,),)) < evidence_cost(((BROKEN_PLURAL,),))


def test_a_four_letter_root_and_the_verbal_nouns_of_v_and_vi():
    assert {"בהדל", "יבהדל", "מבהדל", "בהדלה", "בהדלת"} <= stem_forms("בהדל", 2)
    assert {"תכלכל", "יתכלכל", "מתכלכלה"} <= stem_forms("כלכל", 5)
    assert stem_forms("בהדל", 7) == frozenset() and not finite_form("בהדל", 2, "בהדלת")
    assert "תצור" in nominal_forms("צור", 5) and "תראסל" in nominal_forms("רסל", 6) and "תסלי" in nominal_forms("סלו", 5)
    assert "תלתה" not in nominal_forms("להו", 5), "three, not a verbal noun with its weak letter dropped"
    assert "תשרי" not in nominal_forms("שרר", 5), "the month, not a contracted doubled root"


def _dictionary():
    data = dictionary()

    def verb(name, rid, stem="I"):
        return _root(name, rid, [name], [], stems=[{
            "id": rid.replace("-e-", "-st-"), "stem_head": {"binyan": stem},
            "forms": [_form(rid.replace("-e-", "-f-"), [_seg("synthetic verb", "definition_hebrew")])]}])

    data["ברג"] = verb("ברג", "blau-e-000910")
    data["תמר"] = verb("תמר", "blau-e-000911")
    data["תמר"]["sub_entries"] = [{
        "id": "blau-se-000911", "name_ja": ["תמרה"], "arabic_spelling": [],
        "stems": [{"id": "blau-st-000912", "stem_head": {}, "forms": [
            _form("blau-f-000912", [_seg("synthetic fruit", "definition_hebrew")])]}]}]
    data["בהדל"] = verb("בהדל", "blau-e-000913", "II")
    data["נעאל"] = _root("נעאל", "blau-e-000914", ["נעאל"], [], stems=[{
        "id": "blau-st-000914", "stem_head": {},
        "forms": [_form("blau-f-000914", [_seg("synthetic noun", "definition_hebrew")])]}])
    data["נעל"] = verb("נעל", "blau-e-000915")
    data["ברכ"] = verb("ברכ", "blau-e-000916")
    data["ברכ"]["sub_entries"] = [{
        "id": f"blau-se-00091{n}", "name_ja": ["ברכה̈"], "arabic_spelling": [],
        "stems": [{"id": f"blau-st-00092{n}", "stem_head": {}, "forms": [
            _form(f"blau-f-00092{n}", [_seg(f"synthetic sense {n}", "definition_hebrew")])]}]} for n in (6, 7)]
    data["ברא"] = verb("ברא", "blau-e-000918", "II")
    data["ברג"]["sub_entries"] = [{
        "id": "blau-se-000930", "name_ja": ["בברג"], "arabic_spelling": [],
        "stems": [{"id": "blau-st-000930", "stem_head": {}, "forms": [
            _form("blau-f-000930", [_seg("synthetic prefixed name", "definition_hebrew")])]}]}]

    def noun(name, rid):
        return _root(name, rid, [name], [], stems=[{
            "id": rid.replace("-e-", "-st-"), "stem_head": {},
            "forms": [_form(rid.replace("-e-", "-f-"), [_seg("synthetic noun", "definition_hebrew")])]}])

    data["אבר"] = noun("אבר", "blau-e-000919")
    data["מלה̈"] = noun("מלה̈", "blau-e-000920")
    return data


def test_the_dictionary_opens_the_noun_a_form_belongs_to(tmp_path):
    svc = BlauService(str(_build(tmp_path, _dictionary())))
    assert svc.is_ready(), svc.unavailable_reason

    def reached(word):
        return [(c.root_id, c.sub_entry) for c in svc.lookup(word).cards]

    assert reached("אבראג") == [("blau-e-000910", "")], "a noun's pattern reaches a verb's root"
    for word in ("אלתמאר", "תמראת", "תמרתהו", "תמרתאן"):
        assert ("blau-e-000911", "תמרה") in reached(word), word
    assert ("blau-e-000913", "") in reached("יבהדלון"), "a four-letter root's imperfect"
    assert reached("נעאל") == [("blau-e-000914", "")], "a word that reaches an entry as written is no plural of another"
    assert len([c for c in svc.lookup("ברכאת").cards if c.root_id == "blau-e-000916" and c.sub_entry]) == 2, (
        "every noun of that name under one entry, not the first alone")
    assert "blau-e-000919" not in [c.root_id for c in svc.lookup("אבראת").cards], "ברא IV with its subject ending"
    assert "blau-e-000919" in [c.root_id for c in svc.lookup("אלאבראת").cards], "with the article it is a noun's plural"
    assert "blau-e-000920" not in [c.root_id for c in svc.lookup("מלהא").cards], "a noun in ה̈ takes no accusative alif"
    assert "blau-e-000910" in [c.root_id for c in svc.lookup("וברג").cards], "a conjunction on the word is peeled"
    assert "blau-e-000910" not in [c.root_id for c in svc.lookup("רג").cards], (
        "a root's first letter is no preposition: רג is not ברג without its ב")
    assert ("blau-e-000910", "בברג") in reached("ברג"), (
        "a name printed with a preposition that is also its root's first letter is still reached")
    svc.close()


def test_a_perfect_of_a_root_ending_in_hamza_is_no_feminine_plural():
    from types import SimpleNamespace
    from shared.ja_verb_forms import final_alef_perfect, listed_alef_perfect
    assert final_alef_perfect("אבראת") == ("בר",) and final_alef_perfect("תבראתו") == ("בר",)
    assert final_alef_perfect("אסתבלאת") == (), "a longer stem's shape is left to the nouns: stables"
    assert final_alef_perfect("בנאת") == () and final_alef_perfect("גואבאת") == () and final_alef_perfect("אברא") == ()
    pairs = [SimpleNamespace(root_key="ברא", stem=2), SimpleNamespace(root_key="כתב", stem=1)]
    assert listed_alef_perfect(("אבראתה", "אבראת"), pairs)
    assert not listed_alef_perfect(("אכתאת",), pairs), "a root the dictionary lists no verb for"


def test_a_noun_form_is_not_read_with_the_article_inside_the_noun():
    from shared.blau_normalize import DUAL, plural_analyses
    from shared.blau_normalize import feminine_heading
    duals = {a.key for a in plural_analyses("אלזמאן") if a.steps[-1] == DUAL}
    assert duals == {"אלזמ"}, "a candidate still: אלפאן is the dual of אלף"
    assert feminine_heading("מִנّה̈: מِנّהٌ̈ מِןْ") and feminine_heading("גِ̇הَהٍ̈") and not feminine_heading("וَגْ̇ה")
    assert "כתאב" in {a.key for a in plural_analyses("אלכתאבאן") if a.steps[-1] == DUAL}
    assert "אלואנ" in {a.key for a in plural_analyses("אלואנא")}, "a noun that begins with these letters"


def test_friedman_reads_a_plural_s_pattern_only_for_a_word_that_reached_nothing_else(tmp_path, monkeypatch):
    """A pattern often names another word (אטפאל: the fuller's earth טפל), and as often the only entry
    the reader can be given (אולאד: ולד): tried where the word reached nothing as written, never
    beside what it did reach."""
    from scripts.build_friedman_sidecar import write_sidecar
    from shared.friedman_service import FriedmanService
    from tests.test_friedman_service import unit
    units = [unit("f000001", "entry", "טפל", ["טפל"]),
             unit("f000002", "entry", "ולד", ["ולד"], dotted=["ולד"]),
             unit("f000003", "entry", "נעאל", ["נעאל"]),
             unit("f000004", "entry", "נעל", ["נעל"])]
    path = tmp_path / "plurals.db"
    write_sidecar({"recovery_version": 5, "units": units}, {"index_version": 1, "rows": []}, {}, path)
    monkeypatch.setenv("FRIEDMAN_DICTIONARY_ENABLED", "1")
    service = FriedmanService(str(path))
    assert service.is_ready(), service.unavailable_reason
    ids = lambda word: [m.unit.id for m in service.lookup(word).matches]   # noqa: E731
    assert ids("אלאטפאל") == ["f000001"] and ids("אולאד") == ["f000002"], "the only entries there are"
    assert all(BROKEN_PLURAL in path for m in service.lookup("אולאד").matches for path in m.analyses), (
        "said to be a possible analysis (expression_panel.render_match_details)")
    assert ids("נעאל") == ["f000003"], "a word that reaches a unit as written is no plural of another"
    assert "f000002" in ids("וולד"), "a conjunction on the word is peeled"
    assert "f000002" not in ids("לד"), "a root's first letter is no conjunction: לד is not ולד without its ו"
    service.close()


def test_a_function_word_s_own_entries_come_first():
    """עליה is עלי with a pronoun before it is the noun עליה̈, and וכל is כל before the verb וכל; the
    pronoun ני is no pronoun under a preposition."""
    from types import SimpleNamespace
    from shared.blau_service import function_word_first
    from shared.ja_expression import function_reading

    def card(candidate, name, paths=((),), **more):
        return SimpleNamespace(candidate=candidate, matched_key=name, analyses=paths, verb_form="", borrowed=(),
                               dot_mismatch=False, **more)

    noun, preposition = card("עליה", "עליה̈"), card("עלי", "עלי", (("suffix",),))
    assert function_word_first((noun, preposition), "עליה") == (preposition, noun)
    longer = card("כל", "בכ̇ל", (("prefix", "source_prefix"),))
    assert function_word_first((noun, longer), "וכל") == (noun, longer), "an entry of another name is not the word's"
    assert function_word_first((noun, card("מע", "מע", (("suffix",),))), "מעני")[0] is noun
    assert function_reading("אנני", "אנ") and function_reading("ומא", "מא") and not function_reading("אלדיני", "אלדי")
    assert not function_reading("כתאבה", "כתאב")


def test_what_follows_a_preposition_and_what_a_conjunction_leaves():
    """לשגל is ל + the noun שגל, so the noun's entry before the verb's; ונצף spells נצף, so the entry
    of that name before a verb form of another root."""
    from shared.blau_normalize import bare_keys, preposition_candidates
    assert preposition_candidates("לשגל") == {"שגל"}
    assert preposition_candidates("ובחכמה") >= {"חכמה", "חכמ"}, "after a conjunction, with a pronoun or without"
    assert not preposition_candidates("שגל") and not preposition_candidates("ונצף")
    assert "כתאב" not in preposition_candidates("כתאב"), "never the word's own key"
    assert not preposition_candidates("לבס"), "two letters are no noun's key"
    assert bare_keys("ונצפ") == {"ונצפ", "נצפ"} and bare_keys("נצפ") == {"נצפ"}
    assert bare_keys("ולד") == {"ולד"}, "a short word keeps its first letter"


def test_the_nouns_with_a_long_vowel_before_a_pronoun():
    """אכ̇יה and אבוך are אך̇ and אב with a pronoun; the vowel between is the case."""
    from shared.blau_normalize import query_stems
    assert "אכ" in query_stems("אכיה") and "אב" in query_stems("ואבוך") and "חמ" in query_stems("חמאהא")
    assert "אב" not in query_stems("אבואב"), "only before a pronoun"


def test_a_verb_form_under_the_conjunction_is_read_as_the_word_s_own():
    from shared.ja_verb_forms import under_conjunction
    assert under_conjunction("אשהדו", "ואשהדו")
    assert not under_conjunction("צאיל", "פצאיל"), "פ is mostly a root's letter"
    assert not under_conjunction("צלת", "וצלת"), "three letters are too few"
    assert not under_conjunction("שהדו", "ואשהדו"), "only the conjunction is peeled"


def test_of_two_sub_entries_the_one_the_next_words_support_comes_first():
    """"עליה" and "עליה בהא" under one verb: the second first when a word after the click carries its
    ב, the first first when none does; a placeholder (פעל) is no word to look for; hits of other
    entries keep their places."""
    from types import SimpleNamespace
    from shared.ja_expression import fuller_first

    def hit(name, words, family="verb"):
        return SimpleNamespace(name=name, family=family,
                               places=(SimpleNamespace(words=tuple(words), positions=(0, 1)),))

    short, full = hit("short", ("ט̇הר", "עליה")), hit("full", ("ט̇הר", "עליה", "בהא"))
    other = hit("other", ("ט̇הר", "עליה", "כתיר"), family=None)
    order = lambda after: [h.name for h in fuller_first((short, other, full), after, lambda h: h.family)]   # noqa: E731
    assert order(("בכתב", "אביה")) == ["full", "other", "short"]
    assert order(("אלעדו",)) == ["short", "other", "full"]
    assert order(()) == ["short", "other", "full"]
    slot = hit("slot", ("וקת", "מן", "פעל"))
    assert [h.name for h in fuller_first((slot, full), ("כרוגה",), lambda h: h.family)] == ["slot", "full"]

