"""Recall controls for spelling, noun forms and script parity; synthetic source units."""
import sqlite3

import pytest

from scripts.build_friedman_sidecar import write_sidecar
from shared.blau_normalize import lexical_key, spelling_key, word_analyses
from shared.friedman_service import FriedmanService
from shared.ja_expression import KEYING_VERSION, query_keys, scope_keys
from test_friedman_service import unit


@pytest.fixture
def expanded(tmp_path, monkeypatch):
    units = [
        unit('f1', 'entry', 'בכיר', ['בכיר'], ['בכיר']),
        unit('f2', 'entry', 'בכייר', ['בכייר'], ['בכיר'], examples=[('אנא בכייר', '', '')]),
        unit('f3', 'entry', 'כוואה', ['כוואה'], ['כוה']),
        unit('f4', 'entry', 'מדינה', ['מדינה'], ['דין']),
        unit('f5', 'entry', 'מולא', ['מולא', 'אלמולי'], ['מולא']),
        unit('f6', 'entry', 'אך', ['אכ'], ['אכ']),
        unit('f7', 'subentry', 'אלאכואן / אכואננא', ['אלאכואנ', 'אכואננא'], parent='f6'),
        unit('f8', 'entry', 'כתאב', ['כתאב'], ['כתב']),
        unit('f9', 'entry', 'יאקוט', ['יאקוט'], ['יאקוט']),
        unit('f10', 'entry', 'יאסמין', ['יאסמינ'], ['יאסמינ']),
        unit('f11', 'entry', 'בחן', ['בחנ'], ['בחנ'], examples=[('בכיר כתאב', '', '')]),
    ]
    path = tmp_path / 'forms.db'
    write_sidecar({'recovery_version': 5, 'units': units}, {'index_version': 1, 'rows': []}, {}, path)
    monkeypatch.setenv('FRIEDMAN_DICTIONARY_ENABLED', '1')
    service = FriedmanService(str(path))
    assert service.is_ready(), service.unavailable_reason
    yield service
    service.close()


def ids(answer):
    return [match.unit.id for match in answer.matches]


def test_short_verb_fallback_requires_a_quote_in_its_own_family(tmp_path, monkeypatch):
    units = [
        unit('want', 'entry', 'חבב', ['חבב'], ['חבב'], binyan='I',
             examples=[('פנחב א', '', '')]),
        unit('stay', 'entry', 'קרר', ['קרר'], ['קרר'], binyan='I'),
        unit('other', 'entry', 'כתב', ['כתב'], ['כתב'], examples=[('נקר א', '', '')]),
    ]
    path = tmp_path / 'short.db'
    write_sidecar({'recovery_version': 5, 'units': units}, {'index_version': 1, 'rows': []}, {}, path)
    monkeypatch.setenv('FRIEDMAN_DICTIONARY_ENABLED', '1')
    service = FriedmanService(str(path))
    try:
        assert ids(service.lookup('פנחב')) == ['want']
        assert ids(service.lookup('נחב')) == ['want']
        assert 'stay' not in ids(service.lookup('נקר'))
        monkeypatch.setenv('FRIEDMAN_VERB_FORM_KINDS', '')
        assert not service.lookup('פנחב').matches
    finally:
        service.close()


def test_object_suffix_retains_an_inflected_perfect_and_its_stem(tmp_path, monkeypatch):
    units = [unit('root', 'entry', 'ברי', ['ברי'], ['ברי'], binyan='I'),
             unit('four', 'binyan', None, [], parent='root', binyan='IV')]
    path = tmp_path / 'object.db'
    write_sidecar({'recovery_version': 5, 'units': units}, {'index_version': 1, 'rows': []}, {}, path)
    monkeypatch.setenv('FRIEDMAN_DICTIONARY_ENABLED', '1')
    service = FriedmanService(str(path))
    try:
        result = service.lookup('אבראנאה')
        assert result.matches[0].unit.id == 'four'
        assert result.matches[0].stem == 4
        assert not service.lookup('אברכ').matches
    finally:
        service.close()


def test_clicked_verb_finds_a_named_stem_construction_across_its_argument_slot(tmp_path, monkeypatch):
    units = [unit('root', 'entry', 'סהל', ['סהל'], ['סהל'], binyan='I'),
             unit('three', 'binyan', '-הא אללה', ['אללה'], parent='root', binyan='III')]
    path = tmp_path / 'context.db'
    write_sidecar({'recovery_version': 5, 'units': units}, {'index_version': 1, 'rows': []}, {}, path)
    monkeypatch.setenv('FRIEDMAN_DICTIONARY_ENABLED', '1')
    service = FriedmanService(str(path))
    try:
        selected = service.expression('סאהל אללה')
        clicked = service.answer('סאהל', before=('ואן',), after=('אללה', 'ועאפאה'))
        assert selected.hits[0].unit.id == 'three'
        assert clicked.context[0].hits[0].unit.id == 'three'
        assert clicked.context[0].hits[0].unit.binyan == 'III'
        assert not clicked.context_quoted
    finally:
        service.close()


def test_source_placeholders_variants_and_index_relationships(tmp_path, monkeypatch):
    units = [
        unit('argument', 'entry', 'חסר פלאנא', ['חסר', 'פלאנא'], ['חסר']),
        unit('pronoun', 'entry', 'אנא', ['אנא'], ['אנא']),
        unit('variant', 'entry', 'מאגור (מَגُוראً)', ['מאגור'], ['מאגור']),
        unit('colon', 'entry', 'לחד: ברד אללה לחדה', ['לחד ברד'], ['לחדברד']),
        unit('hand', 'entry', 'יَד', ['יד'], ['יד']),
        unit('hands', 'subentry', 'אידי אלחאדתאת', ['אידי אלחאדתאת'], parent='hand'),
        unit('geminate', 'entry', 'חתّ̇ -ה', ['חת'], ['חת̇']),
        unit('feminine', 'entry', 'אלסאמיה', ['אלסאמיה'], ['סמי']),
        unit('masculine', 'entry', 'סאמי', ['סאמי'], ['סמי']),
    ]
    index = {'index_version': 1, 'rows': []}
    for form, root in [('אידי', 'ידי'), ('אחת', 'חתת')]:
        index['rows'].append(dict(form=form, form_key=form, line=form, pdf_page=1,
                                  printed_page='1', url='', roots=[dict(
                                      text=root, dotted=root, key=root, alternatives=1)]))
    path = tmp_path / 'source.db'
    write_sidecar({'recovery_version': 5, 'units': units}, index, {}, path)
    monkeypatch.setenv('FRIEDMAN_DICTIONARY_ENABLED', '1')
    service = FriedmanService(str(path))
    try:
        assert ids(service.lookup('ואנא')) == ['pronoun']
        assert 'argument' in ids(service.lookup('חסר'))
        assert 'argument' in [h.unit.id for h in service.expression('חסר פלאנא').hits]
        for query, target in [('מגור', 'variant'), ('מגורא', 'variant'), ('לחד', 'colon'),
                              ('אידי', 'hand'), ('אחת', 'geminate')]:
            assert target in ids(service.lookup(query)), query
        assert ids(service.lookup('אלסאמי'))[0] == 'masculine'
        with sqlite3.connect(path) as conn:
            from scripts.dictionary_search_indexes import verify_search_indexes
            verify_search_indexes(conn, 'friedman')
    finally:
        service.close()


def test_both_sides_of_lexical_evidence_affect_rank():
    from shared.blau_normalize import evidence_cost, matching_paths
    direct = matching_paths('אלסאמי', 'סאמי', 'סאמי')
    derived = matching_paths('אלסאמי', 'אלסאמיה', 'אלסאמי')
    assert evidence_cost(direct) < evidence_cost(derived)
    assert any('source_prefix' in path for path in matching_paths('ואנא', 'לאנא', 'אנא'))


def test_partial_components_and_longer_click_context(expanded):
    from shared.ja_expression import component_runs, context_component_runs
    assert 'בכיר כתאב' in component_runs('תוספת בכיר כתאב')
    result = expanded.answer('תוספת בכיר כתאב')
    assert not result.hits and any(r.text == 'בכיר כתאב' and r.found for r in result.related)
    assert 'והב אלעאפיה' in context_component_runs(
        'אלעאפיה', ('והב', 'אללה', 'לה', 'פי', 'כל', 'מא'), ())
    assert expanded.expression('אנא בכיר').found
    assert not expanded.expression('אנא בכיר', head_only=True).found


def test_spelling_runs_are_symmetric_and_do_not_collapse_other_consonants(expanded):
    assert ids(expanded.lookup('בכיר'))[:2] == ['f1', 'f2']
    assert ids(expanded.lookup('בכייר'))[:2] == ['f2', 'f1']
    assert 'f3' in ids(expanded.lookup('כואה'))
    assert spelling_key('ניייוווטט') == 'ניוטט'
    assert 'f2' in [h.unit.id for h in expanded.expression('אנא בכיר').hits]
    assert 'f2' in [h.unit.id for h in expanded.expression('אנא בכייר').hits]
    assert all(pos == 0 for pos, _, _ in scope_keys('בכייר'))


@pytest.mark.parametrize('query,target', [
    ('מדינת', 'f4'), ('מדינתך', 'f4'), ('יאמולאי', 'f5'), ('ויאמולאי', 'f5'),
    ('מולי', 'f5'), ('אכואן', 'f7'), ('אכואני', 'f7'), ('كتاب', 'f8'), ('مولاي', 'f5'),
    ('יאקוט', 'f9'), ('יאסמין', 'f10'),
])
def test_whole_word_and_source_form_candidates(expanded, query, target):
    assert target in ids(expanded.lookup(query))


def test_analysis_preserves_paths_and_source_query_agreement():
    assert any(a.key == 'מדינה' and a.nominal and 'feminine' in a.steps for a in word_analyses('מדינתך'))
    assert any(a.key == 'מולא' and 'vocative' in a.steps for a in word_analyses('יאמולאי'))
    assert set(query_keys('סלאמה')) & set(query_keys('סלאמתה'))
    assert set(query_keys('בלסלאם')) & set(query_keys('באלסלאם'))
    assert lexical_key('مَدِينَةٌ') == 'מדינה'
    assert query_keys('اخذ') == query_keys('אכד')


def test_reader_refuses_stale_expression_keys(expanded):
    with sqlite3.connect(expanded.db_path) as conn:
        conn.execute("UPDATE meta SET value=? WHERE key='keying_version'", (str(KEYING_VERSION - 1),))
    other = FriedmanService(expanded.db_path)
    assert not other.is_ready() and 'keying_version' in other.unavailable_reason


def test_any_order_matches_distinct_positions_and_respects_gaps():
    from shared.ja_expression import unordered_match
    assert unordered_match(["a", "x", "b"], ["b", "a"], 1) == (2, 0)
    assert unordered_match(["a", "x", "b"], ["b", "a"], 0) is None
    assert unordered_match([(0, "a"), (0, "b")], ["a", "b"], 20) is None
    assert unordered_match(["a", "b", "a"], ["a", "a", "b"], 0) == (0, 2, 1)


def test_any_order_against_exhaustive_small_scopes():
    import itertools
    import random
    from shared.ja_expression import unordered_match
    rng = random.Random(12)
    for _ in range(200):
        tokens = [rng.choice("abc") for _ in range(rng.randint(2, 8))]
        query = [rng.choice("abc") for _ in range(rng.randint(2, 4))]
        gap = rng.randrange(3)
        possible = []
        for places in itertools.permutations(range(len(tokens)), len(query)):
            ordered = sorted(places)
            if all(tokens[p] == q for p, q in zip(places, query)) and all(
                    b - a <= gap + 1 for a, b in zip(ordered, ordered[1:])):
                possible.append(places)
        expected = min(possible, key=lambda p: (max(p) - min(p), p)) if possible else None
        assert unordered_match(tokens, query, gap) == expected


def test_order_option_limits_and_errors_are_not_absence(expanded):
    assert not expanded.answer("בכיר אנא").found
    found = expanded.answer("בכיר אנא", ordered=False)
    assert found.found and not found.ordered
    long = " ".join(["אנא"] * 13)
    answer = expanded.answer(long)
    assert answer.text == long and answer.problem == "limit"
    assert expanded.answer("hello world").problem == "unsupported"
    assert expanded.lookup("hello").problem == "unsupported"
    expanded._conn.close()
    assert expanded.lookup("בכיר").problem == "error"
    assert expanded.expression("אנא בכיר").problem == "error"


def test_vocative_components_can_share_only_a_segmented_written_token():
    from shared.ja_expression import expression_match, components_for
    items = [(p, k) for p, k, _ in scope_keys("יאמולאי")]
    candidates = [query_keys("יא"), query_keys("מולאי")]
    match = expression_match(items, candidates, 0)
    assert match is not None
    positions, _components = components_for(items, match, ("יא", "מולאי"), candidates, ("יאמולאי",))
    assert positions == (0, 0)
    assert expression_match(items, [query_keys("מולאי")] * 2, 20) is None


def test_arabic_quotation_recovery_keeps_font_fragments_in_source_order():
    from scripts.build_friedman_sidecar import quotation_foreign
    source = {"examples": [{"foreign": "يوجب"}], "segments": [
        ["bullet", "•"], ["other", "فهذا "], ["foreign", "يوجب "],
        ["other", "ان الولد "], ["ja", "ממזר"], ["he", " translation"],
        ["loc", "(reference)"]]}
    assert quotation_foreign(source, 1) == "فهذا يوجب ان الولد ממזר"


def test_short_hollow_ranking_keeps_explicit_dotted_alternatives():
    from shared.ja_roots import supported_hollow_perfect
    assert supported_hollow_perfect("כנת", "כון", (1, 2))
    assert supported_hollow_perfect("וקלנא", "קול", (1,))
    assert not supported_hollow_perfect("כ̇נת̇", "כון", (1,))
    assert not supported_hollow_perfect("כנת", "כון", (2,))


def test_quotation_pages_are_bounded_and_do_not_repeat(tmp_path, monkeypatch):
    units = [unit(f"f{i}", "entry", f"אבג{i}", ["אבג"], ["אבג"],
                  examples=[("אבו תלתה", "", "")]) for i in range(12)]
    path = tmp_path / "quotes.db"
    write_sidecar({"recovery_version": 5, "units": units}, {"index_version": 1, "rows": []}, {}, path)
    monkeypatch.setenv("FRIEDMAN_DICTIONARY_ENABLED", "1")
    service = FriedmanService(str(path))
    result = service.lookup("אבו")
    assert len(result.quotes) == 5 and result.more_quotes == 7
    page, more = service.quotation_page(result, result.quote_offset)
    assert len(page) == 5 and more == 2
    assert not {h.unit.id for h in page} & {h.unit.id for h in result.quotes}
    service.close()


def test_context_crosses_function_words_and_prefers_the_tighter_source_phrase(tmp_path, monkeypatch):
    units = [unit('tight', 'entry', 'אבג', ['אבג'], ['אבג'],
                  examples=[('ווהב אלעאפיה', '', '')]),
             unit('loose', 'entry', 'דהז', ['דהז'], ['דהז'],
                  examples=[('בעאפיתה הג כרג', '', '')])]
    path = tmp_path / 'context.db'
    write_sidecar({'recovery_version': 5, 'units': units}, {'index_version': 1, 'rows': []}, {}, path)
    monkeypatch.setenv('FRIEDMAN_DICTIONARY_ENABLED', '1')
    service = FriedmanService(str(path))
    result = service.answer('אלעאפיה', before=('ואנה', 'אדא', 'והב', 'אללה', 'לה'), after=('וכרג',))
    assert result.context[0].text == 'והב אלעאפיה'
    assert result.context[0].hits[0].unit.id == 'tight'
    assert any(h.unit.id == 'loose' for r in result.context for h in r.hits)
    full = 'ואנה אדא והב אללה לה אלעאפיה'
    selected = service.answer(full, stems=False)
    assert selected.text == full and not selected.hits
    assert selected.related[0].text == 'והב אלעאפיה'
    assert selected.related[0].hits[0].unit.id == 'tight'
    assert not service.answer('אללה לה').related
    service.close()


def test_arabic_quote_has_the_same_context_rank_as_a_hebrew_quote():
    from types import SimpleNamespace
    from shared.ja_expression import context_quality
    def result(scope):
        place = SimpleNamespace(scope=scope, positions=(0, 1),
                                components=(SimpleNamespace(via='word'),) * 2)
        return SimpleNamespace(hits=[SimpleNamespace(places=[place])])
    assert context_quality(result('arabic_quotation')) == context_quality(result('quotation'))
    assert context_quality(result('construction')) < context_quality(result('arabic_quotation'))


def test_friedman_additional_lexical_entries_keep_the_first_page(tmp_path, monkeypatch):
    units = [unit(f'f{i:02}', 'entry', 'אבג', ['אבג'], ['אבג']) for i in range(25)]
    path = tmp_path / 'pages.db'
    write_sidecar({'recovery_version': 5, 'units': units}, {'index_version': 1, 'rows': []}, {}, path)
    monkeypatch.setenv('FRIEDMAN_DICTIONARY_ENABLED', '1')
    service = FriedmanService(str(path))
    try:
        first, second, last = (service.lookup('אבג', limit=n) for n in (12, 22, 32))
        assert [len(a.matches) for a in (first, second, last)] == [12, 22, 25]
        assert ids(first) == ids(second)[:12] == ids(last)[:12]
        assert first.truncated and second.truncated and not last.truncated
    finally:
        service.close()


def test_added_plural_path_reaches_a_noun_not_a_verbal_index_entry(tmp_path, monkeypatch):
    units = [unit('noun', 'entry', 'בכיר', ['בכיר'], ['בכיר']),
             unit('verb', 'entry', 'בלג', ['בלג'], ['בלג'], binyan='I')]
    index = {'index_version': 1, 'rows': [dict(
        form='אבלג', form_key='אבלג', line='אבלג (בלג)', pdf_page=1,
        printed_page='1', url='', roots=[dict(text='בלג', key='בלג', dotted='בלג', alternatives=1)])]}
    path = tmp_path / 'nouns.db'
    write_sidecar({'recovery_version': 5, 'units': units}, index, {}, path)
    monkeypatch.setenv('FRIEDMAN_DICTIONARY_ENABLED', '1')
    service = FriedmanService(str(path))
    try:
        assert ids(service.lookup('בכירין')) == ['noun']
        assert 'verb' in ids(service.lookup('אבלג'))
        assert 'verb' not in ids(service.lookup('אבלגין', verb_forms=frozenset()))
    finally:
        service.close()
