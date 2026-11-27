# -*- coding: utf-8 -*-
"""GS-9: the filters and exclusions decide over the whole result list (desktop/result_view.py)."""
from desktop.result_view import (DOMAIN, SHELF, SNIPPET, TITLE, ResultRules, ResultView, TextCache,
                                 passes, text_matches)


def _res(i, sid=None, snippet='', uid=None):
    return {'uid': uid or f'u{i}', 'display': {'id': sid or f'99{i:04d}'}, 'snippet': snippet}


def _sid(res):
    return res['display']['id']


def _view(results, rules, texts=None):
    texts = texts or {}
    return ResultView(results, rules, _sid, lambda r, c: texts.get((_sid(r), c), r.get('snippet') if c == SNIPPET else ''))


def test_a_match_far_past_the_first_batch_is_found():
    results = [_res(i, snippet='שלום') for i in range(1200)]
    results[900]['snippet'] = 'ברוך'
    view = _view(results, ResultRules(column_filters={SNIPPET: {'text': 'ברוך'}}))
    assert view.count == 1 and view.shown_after(0) == [results[900]]
    assert view.remaining_after(500) == 1 and view.remaining_after(901) == 0


def test_exclusions_count_first_whatever_the_filters_say():
    results = [_res(i) for i in range(5)]
    rules = ResultRules(excluded={_sid(results[1]), _sid(results[3])},
                        column_filters={SNIPPET: {'text': 'nothing'}})
    view = _view(results, rules)
    assert view.excluded == 2 and view.count == 0


def test_text_not_known_yet_passes_a_column_filter():
    rule = {'text': 'T-S'}
    assert text_matches(None, rule) and text_matches(None, dict(rule, exclude=True))
    assert not text_matches('ENA 1', rule) and text_matches('ena t-s 1', rule)
    assert text_matches('ENA 1', dict(rule, exclude=True))


def test_each_rule():
    r = _res(1, sid='991')
    ok = lambda **kw: passes(r, '991', ResultRules(**kw), lambda res, c: 'T-S 1' if c == SHELF else '')  # noqa: E731
    assert ok()
    assert not ok(column_filters={SHELF: {'text': 'ENA'}}) and ok(column_filters={SHELF: {'text': 't-s'}})
    assert ok(list_mode='in', list_ids={'991'}) and not ok(list_mode='not_in', list_ids={'991'})
    assert not ok(list_mode='in', list_ids=set())
    assert not ok(domain_exclusions={'Uncategorized'})
    assert ok(domain_exclusions={'Bible'}, domain_map={'991': ['Bible', 'Liturgy']})
    assert not ok(domain_exclusions={'Bible'}, domain_map={'991': ['Bible']})
    assert not ok(printed_state='hide_printed', printed_ids={'991'})
    assert not ok(printed_state='only_printed') and ok(printed_state='only_printed', printed_ids={'991'})
    assert not ok(post_ids=set()) and ok(post_ids={'991'})
    assert not ok(local_ids={'992'}) and ok(local_ids={'991'})
    assert not ok(all_terms_uids={'other'}) and ok(all_terms_uids={'u1'})


def test_title_and_domain_texts_come_from_text_of():
    results = [_res(1), _res(2)]
    texts = {(_sid(results[0]), TITLE): 'פיוט', (_sid(results[1]), DOMAIN): 'Bible'}
    assert _view(results, ResultRules(column_filters={TITLE: {'text': 'פיוט'}}), texts).shown == [True, False]
    assert _view(results, ResultRules(column_filters={DOMAIN: {'text': 'bible'}}), texts).shown == [False, True]


def test_the_text_cache_computes_once_and_forgets_a_manuscript():
    cache, calls = TextCache(), []
    results = [_res(1), _res(2)]
    cache.for_list(results)

    def text(res):
        return cache.get(res, SHELF, _sid(res), lambda: calls.append(_sid(res)) or 'x')
    text(results[0]); text(results[0]); text(results[1])
    assert calls == ['990001', '990002']
    cache.forget('990001')
    text(results[0]); text(results[1])
    assert calls == ['990001', '990002', '990001']
    cache.for_list(list(results))           # a new list: everything again
    text(results[1])
    assert calls[-1] == '990002'
    cache.get(results[1], TITLE, '990002', lambda: 't')
    cache.forget_column(TITLE)
    assert cache.get(results[1], TITLE, '990002', lambda: 'u') == 'u'


def test_a_result_kept_only_for_want_of_its_text_is_drawn_but_not_counted():
    """Codex on #150: a result whose text is not fetched yet is drawn (so its fetch starts)
    but neither counted nor exported until the fetched text decides."""
    results = [_res(1), _res(2), _res(3)]
    texts = {(_sid(results[0]), SHELF): 'T-S 1', (_sid(results[1]), SHELF): None, (_sid(results[2]), SHELF): 'ENA 3'}
    view = ResultView(results, ResultRules(column_filters={SHELF: {'text': 'T-S'}}), _sid,
                      lambda r, c: texts[(_sid(r), c)])
    assert view.shown == [True, True, False] and view.pending == {1}
    assert view.count == 1 and view.remaining_after(0) == 2
    assert view.shown_after(0) == [results[0]] and view.is_pending(1)


def test_filters_nothing():
    assert ResultRules(excluded={'1'}).filters_nothing()
    assert not ResultRules(printed_state='hide_printed').filters_nothing()
