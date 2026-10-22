"""Responsa reads pages first, and its matches across pages come from the seams on format 2
(a star query's always, finite words' when S <= K: decision 10) and from the
whole-manuscript documents on format 1, a star query's bounded
(docs/plans/RESPONSA_TRUE_WILDCARDS_PLAN.md §5.4, §6.1 point 2, §7 Phase 3 points 7-11).

One corpus, built by the real Indexer as format 2 (seams, K = 30) and as format 1
(tests/responsa_index_fixture.py), and a second format-2 build with K = 2 for the queries
whose side is longer than K. Every expectation is written by hand from the pages below.
"""
import pytest

import shared.search_engine as se
from shared.config import Config
from shared.search_regex import isolated_matching
from tests.responsa_index_fixture import build, engine, formats, uid

R = {"responsa_mode": True}


def sid(n):
    return f"99000000{n:03d}0205171"


# (manuscript number, [page texts]): manuscript n is sys_id sid(n), volume IE 2000+n,
# pages numbered from 1 in V0.8.
MANUSCRIPTS = {
    101: ["פתיחה מאמתי", "קורין סוף", "אחרון כאן"],           # מאמ* קור*: across p1 | p2
    102: ["אחד כהנים גדולים", "גדולים שני"],                   # כהנ* גדול*: inside p1
    103: ["כהנים גדולים ועוד כהנים", "גדולים סוף"],            # inside p1, and across p1 | p2
    104: ["עמוד תעודדו", "עמוד", "תענגו עמוד"],               # over two breaks (three pages)
    105: ["תחילה מאמרו"],                                      # V0.8's last page; V0.7 restarts below
    106: ["שורה אחת\nסוף מאמתי", "קורין בתחילה\nעוד"],         # a line query across p1 | p2
    107: ["א ב ג כהנים", "ד ה גדולים סוף"],                   # [3]: three words after the break
    108: ["ראשון כהנים", "גדולים אחרון"],                      # one word each side
    109: ["ראשית כהנים", "גדולים סופית"],
    110: ["כהנים גדולים ושם כהנים גדולים"],                    # two matches on one page
    111: ["כהנים גדולים", "שם כהנים", "גדולים סוף"],            # a match inside p1, a crossing p2 | p3
}
V08 = [(sid(n), 2000 + n, p, text) for n, pages in MANUSCRIPTS.items() for p, text in enumerate(pages, 1)]
V07 = [(sid(105), 2105, 1, "קוראים כאן")]       # the whole-manuscript doc joins it to V0.8's p1


def page(n, p=1):
    return uid(2000 + n, p)


@pytest.fixture(scope='module')
def engines(tmp_path_factory):
    with formats(tmp_path_factory, 'star_crossings', V08, V07) as e:
        yield e


@pytest.fixture(scope='module')
def short_seams(tmp_path_factory):
    """Format 2 with seams of K = 2 words a side."""
    root = build(tmp_path_factory.mktemp('short_seams') / 'k2', V08, V07, seam_words=2)
    e = engine(root)
    yield e
    e.close_index()


def search(eng, query, *, runtime='regex', **kwargs):
    opts = dict(R, **kwargs.pop('options', {}))
    se.consume_last_search_cutoff()
    se._consume_last_responsa_downgrade()

    def run():
        return eng.execute_search(query, 'literal', 0, responsa_options=opts, corpus_scope='genizah', **kwargs)
    if runtime == 're':
        with isolated_matching(native=True):
            rows = run()
    else:
        rows = run()
    return rows, se.consume_last_search_cutoff(), se._consume_last_responsa_downgrade()


def by_uid(rows):
    out = {}
    for r in rows:
        assert r['uid'] not in out, f"two rows for {r['uid']}"
        out[r['uid']] = r
    return out


# --- Where the crossings come from ----------------------------------------------------------

@pytest.mark.parametrize('runtime', ['re', 'regex'])
@pytest.mark.parametrize('fmt, scope', [('2', 'seam'), ('1', 'system')])
def test_a_star_crossing_is_found_on_both_formats(engines, fmt, scope, runtime):
    rows, cutoff, note = search(engines[fmt], 'מאמ* קור*', runtime=runtime)
    row = by_uid(rows)[page(101)]
    assert row['scope'] == scope and row['cross_page']
    # The line break that joins the pages belongs to the page before it (as for every
    # whole-manuscript row, tests/test_responsa_crossings.py).
    assert [(h['uid'], tuple(h['span'])) for h in row['page_highlights']] == [(page(101, 1), (6, 12)),
                                                                             (page(101, 2), (0, 5))]
    assert '*מאמתי\nקורין*' in row['raw_file_hl']
    assert not cutoff['capped'] and note is None


def test_a_seam_row_carries_the_pages_it_covers_whole(engines):
    row = by_uid(search(engines['2'], 'מאמ* קור*')[0])[page(101)]
    assert row['full_text'] == "פתיחה מאמתי\nקורין סוף"          # never the seam's slices
    assert row['raw_header'] == f"{sid(101)}_{page(101)}"


def test_a_page_the_main_index_does_not_give_keeps_the_seams_slice_of_it(engines, monkeypatch):
    eng = engines['2']
    monkeypatch.setattr(eng, '_page_texts', lambda pages, cache: ['', 'קורין סוף'])
    row = by_uid(search(eng, 'מאמ* קור*')[0])[page(101)]
    assert row['full_text'] == "פתיחה מאמתי\nקורין סוף"


def test_seams_join_only_consecutive_pages_the_whole_manuscript_also_joins_the_v07_restart(engines):
    # Manuscript 105: its V0.8 page then its V0.7 copy, one whole-manuscript document.
    # Format 1 reads that junction (today's false junction, §3.6.5); the seams do not hold it.
    assert page(105) in by_uid(search(engines['1'], 'מאמ* קור*')[0])
    assert page(105) not in by_uid(search(engines['2'], 'מאמ* קור*')[0])


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_every_crossing_of_a_manuscript_is_found_not_only_its_first_match(engines, fmt):
    # 111's whole text matches first inside p1; its crossing p2 | p3 comes after. Read as
    # a whole (today's route for finite words) the manuscript gives only the first.
    found = by_uid(search(engines[fmt], 'כהנ* גדול*')[0])
    assert found[page(111, 1)]['scope'] == 'page'
    assert found[page(111, 2)]['cross_page']


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_a_match_over_two_breaks_is_one_row(engines, fmt):
    # tests/test_cross_page_spans.py's three-page case, with stars: the match crosses
    # p1 | p2 and p2 | p3, so two seams (or two break windows) find it; one row stays.
    rows = [r for r in search(engines[fmt], 'תעוד* עמו* תענ*')[0] if r['uid'] == page(104)]
    assert len(rows) == 1
    assert [h['uid'] for h in rows[0]['page_highlights']] == [page(104, 1), page(104, 2), page(104, 3)]


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_pages_first_and_the_first_row_wins(engines, fmt):
    # Manuscript 103's first page holds the phrase and also starts a crossing: its row is
    # the page's own (§5.4), with the page's own text.
    rows, _cut, _note = search(engines[fmt], 'כהנ* גדול*')
    row = by_uid(rows)[page(103)]
    assert row['scope'] == 'page' and row['full_text'] == "כהנים גדולים ועוד כהנים"
    assert not row.get('cross_page')


def test_finite_words_read_the_seams_on_format_2_and_whole_manuscripts_on_format_1(engines):
    # Decision 10 (plan §7, Phase 3 point 7): a finite sequence with S <= K reads the seams;
    # format 1 keeps today's whole-manuscript documents.
    assert by_uid(search(engines['2'], 'כהנים גדולים')[0])[page(109)]['scope'] == 'seam'
    row = by_uid(search(engines['1'], 'כהנים גדולים')[0])[page(109)]
    assert row['scope'] == 'system' and row['cross_page']
    # Manuscript 111: a match inside p1, then a crossing p2 | p3. A whole manuscript gives
    # its first match only, which p1's own row already shows; the seams give the crossing.
    assert page(111, 2) in by_uid(search(engines['2'], 'כהנים גדולים')[0])
    assert page(111, 2) not in by_uid(search(engines['1'], 'כהנים גדולים')[0])


def test_finite_words_past_k_keep_the_whole_manuscripts_unmarked(short_seams):
    # S = 1 + 1 + 3 - 1 = 4 > K = 2: without a star, today's whole-manuscript documents, so
    # nothing is left out and nothing is marked (a star word would read the seams, marked).
    rows, cutoff, note = search(short_seams, 'כהנים [3] גדולים')
    row = by_uid(rows)[page(107)]
    assert row['scope'] == 'system' and row['cross_page']
    assert not cutoff.get('partial') and not cutoff['capped'] and not note


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_the_crossing_rows_honour_the_restriction(engines, fmt):
    found = by_uid(search(engines[fmt], 'מאמ* קור*', restrict_sys_ids={sid(101)})[0])
    assert page(101) in found
    assert page(101) not in by_uid(search(engines[fmt], 'מאמ* קור*', restrict_sys_ids={sid(102)})[0])


@pytest.mark.parametrize('fmt', ['2', '1'])
@pytest.mark.parametrize('word, excluded', [('סוף', True), ('פתיחה', True), ('אחרון', False)])
def test_an_excluded_word_is_read_on_the_pages_the_crossing_covers(engines, fmt, word, excluded):
    # Decision 7: p1 and p2 are the crossing's pages; p3 (אחרון) is the manuscript's, not its.
    found = by_uid(search(engines[fmt], f'מאמ* קור* -{word}')[0])
    assert (page(101) not in found) == excluded


@pytest.mark.parametrize('fmt, scope', [('2', 'seam'), ('1', 'system')])
def test_ids_only_gives_the_crossing_and_its_exclusion(engines, fmt, scope):
    rows = search(engines[fmt], 'מאמ* קור*', ids_only=True)[0]
    assert {(r['uid'], r['scope']) for r in rows if r['uid'] == page(101)} == {(page(101), scope)}
    rows = search(engines[fmt], 'מאמ* קור* -סוף', ids_only=True)[0]
    assert page(101) not in {r['uid'] for r in rows}


# --- Past K on format 2, and the bounded read on format 1 ------------------------------------

def test_past_k_the_seams_still_give_the_short_crossings_and_the_search_says_n_plus(short_seams):
    # כהנ* [3] גדול*: S = 1 + 1 + 3 - 1 = 4 > K = 2. Manuscript 108's crossing has one word
    # a side and is in its seam; 107's has three after the break and is not.
    rows, cutoff, note = search(short_seams, 'כהנ* [3] גדול*')
    found = by_uid(rows)
    assert page(108) in found and page(107) not in found
    assert cutoff['capped']
    assert note and 'more than 2 words on one side' in note


def test_crossings_left_out_by_rule_are_partial_not_a_limit(short_seams, monkeypatch):
    # Codex on PR #442, round 1: "N+" either way, but reading every candidate (ids_only)
    # completes only a limit's cut, so the signal tells them apart.
    _rows, cutoff, _note = search(short_seams, 'כהנ* [3] גדול*')
    assert cutoff == {'capped': True, 'interrupted': False, 'partial': True, 'limited': False}
    monkeypatch.setattr(Config, 'RESPONSA_PAGE_LIMIT', 1)
    _rows, cutoff, _note = search(short_seams, 'כהנ* [3] גדול*')
    assert cutoff == {'capped': True, 'interrupted': False, 'partial': True, 'limited': True}


def test_a_step_that_left_crossings_out_by_rule_is_not_completed(short_seams):
    from shared.refinement import RefinementStep, complete_chain, replay_chain, steps_that_cannot_complete
    step = RefinementStep(query='כהנ* [3] גדול*', mode='literal', responsa_options=dict(R), corpus_scope='genizah')
    replay_chain([step], short_seams, None)
    assert step.result_count_capped and step.result_partial
    assert steps_that_cannot_complete([step]) == [step]
    calls = []

    class Counting:
        def execute_search(self, *args, **kwargs):
            calls.append(kwargs)
            return []
    assert complete_chain([step], Counting(), None)['interrupted'] is False
    assert calls == []                      # not run again: it would leave them out again
    assert step.result_count_capped


@pytest.mark.parametrize('child_cut, runs_again', [({}, []), ({'capped': True}, [('child', True)])])
def test_a_step_searched_within_a_partial_step_is_not_completed_either(child_cut, runs_again):
    # Codex round 2 on PR #442: the child of a step that left crossings out by rule is "+"
    # (its restriction is incomplete) and, unless its own candidate limit cut it, running
    # it again with every candidate gives the same rows: it is not completed either. Cut by
    # its own limit, it is run again once; then it, too, has nothing left to complete.
    from shared.refinement import RefinementStep, complete_chain, replay_chain
    cuts = {'parent': {'partial': True}, 'child': child_cut}
    calls = []

    class Fake:
        def execute_search(self, query, *args, **kwargs):
            calls.append((query, bool(kwargs.get('ids_only'))))
            if cuts[query] and not kwargs.get('ids_only'):
                se._note_search_cutoff(**cuts[query])
            return [{'display': {'id': sid(101)}, 'uid': page(101)}]
    parent, child = (RefinementStep(query=q, mode='literal') for q in ('parent', 'child'))
    se.consume_last_search_cutoff()
    replay_chain([parent, child], Fake(), None)
    assert parent.result_partial and child.result_count_capped
    assert child.result_partial == (not child_cut)
    calls.clear()
    complete_chain([parent, child], Fake(), None)
    assert calls == runs_again
    assert child.result_partial and child.result_count_capped
    calls.clear()
    complete_chain([parent, child], Fake(), None)
    assert calls == []


def test_format_1_reads_the_long_crossing_through_the_whole_manuscript(engines):
    assert {page(107), page(108)} <= set(by_uid(search(engines['1'], 'כהנ* [3] גדול*')[0]))


def test_too_many_manuscripts_for_a_star_are_not_read_and_the_search_says_so(engines, monkeypatch):
    monkeypatch.setattr(Config, 'RESPONSA_STAR_AGGREGATE_LIMIT', 0)
    rows, cutoff, note = search(engines['1'], 'מאמ* קור*')
    assert page(101) not in by_uid(rows)
    assert cutoff['capped'] and note and 'more than the 0 that can be read in time' in note
    # Format 2 reads the seams, not the whole manuscripts: the limit does not apply.
    rows, cutoff, note = search(engines['2'], 'מאמ* קור*')
    assert page(101) in by_uid(rows) and not cutoff['capped'] and note is None


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_and_and_line_queries_with_a_star_read_whole_manuscripts_when_bounded(engines, fmt, monkeypatch):
    assert page(101) in by_uid(search(engines[fmt], 'מאמ* AND קורין')[0])
    assert page(106, 1) in by_uid(search(engines[fmt], 'מאמ* | קור*')[0])
    monkeypatch.setattr(Config, 'RESPONSA_STAR_AGGREGATE_LIMIT', 0)
    for query in ('מאמ* AND קורין', 'מאמ* | קור*'):
        rows, cutoff, note = search(engines[fmt], query)
        assert cutoff['capped'] and note and 'run across pages were not searched' in note


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_a_failed_crossing_step_marks_the_search_incomplete(engines, fmt, monkeypatch):
    # The pages' query runs; the second one -- the seams', or the whole manuscripts' --
    # fails.
    real, calls = se._top_hits, []

    def failing_second(searcher, query, limit):
        calls.append(limit)
        if len(calls) > 1:
            raise RuntimeError('the crossing index is gone')
        return real(searcher, query, limit)
    monkeypatch.setattr(se, '_top_hits', failing_second)
    rows, cutoff, note = search(engines[fmt], 'כהנ* גדול*')
    assert page(102) in by_uid(rows)                       # the page rows are kept
    assert page(109) not in by_uid(rows)                   # the crossing step failed
    assert cutoff['capped'] and note and 'could not be searched' in note


# --- Limits (decision 3) ----------------------------------------------------------------------

def _limits(monkeypatch):
    seen = []
    real = se._top_hits

    def recording(searcher, query, limit):
        seen.append(limit)
        return real(searcher, query, limit)
    monkeypatch.setattr(se, '_top_hits', recording)
    return seen


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_responsa_pages_read_up_to_their_own_limit_and_crossings_up_to_theirs(engines, fmt, monkeypatch):
    monkeypatch.setattr(Config, 'RESPONSA_PAGE_LIMIT', 7)
    monkeypatch.setattr(Config, 'SEARCH_LIMIT', 5)
    seen = _limits(monkeypatch)
    search(engines[fmt], 'כהנ* גדול*')
    assert seen == [7, 5]                    # the pages, then the seams or the manuscripts
    seen.clear()
    engines[fmt].execute_search('כהנים גדולים', 'literal', 0, corpus_scope='genizah')
    assert seen == [5, 5]                    # Exact keeps 50,000 (here 5) for both


def test_the_page_limit_cuts_and_says_n_plus(engines, monkeypatch):
    monkeypatch.setattr(Config, 'RESPONSA_PAGE_LIMIT', 1)
    rows, cutoff, _note = search(engines['2'], 'כהנ* גדול*')
    assert cutoff['capped']
    assert sum(1 for r in rows if r['scope'] == 'page') == 1


# --- Order (decision 2) -----------------------------------------------------------------------

@pytest.mark.parametrize('fmt', ['2', '1'])
def test_an_all_star_query_goes_by_matches_on_the_page_then_shelfmark(engines, fmt):
    # 110's page holds two matches; the others one each (a crossing counts one), then the
    # shelfmark (the fixture's shelfmark is the header, so the sys_id orders them).
    rows = search(engines[fmt], 'כהנ* גדול*')[0]
    assert [r['uid'] for r in rows] == [page(110), page(102), page(103), page(108), page(109),
                                        page(111, 1), page(111, 2)]


def test_a_query_with_a_finite_word_keeps_the_score_order_pages_first(engines):
    rows = search(engines['2'], 'כהנ* גדולים')[0]
    scopes = [r['scope'] for r in rows]
    assert scopes == sorted(scopes, key=lambda s: s != 'page') and 'seam' in scopes


# --- Exact, Variants and Fuzzy: their crossings from the seams (decision 10, point 12) -----

def evf(eng, query, mode, gap=0, **kwargs):
    se.consume_last_search_cutoff()
    rows = eng.execute_search(query, mode, gap, corpus_scope='genizah', **kwargs)
    return rows, se.consume_last_search_cutoff()


@pytest.mark.parametrize('mode', ['literal', 'variants', 'fuzzy'])
@pytest.mark.parametrize('fmt, scope', [('2', 'seam'), ('1', 'system')])
def test_exact_variants_and_fuzzy_read_their_crossings_from_the_seams_on_format_2(engines, mode, fmt, scope):
    rows, cutoff = evf(engines[fmt], 'כהנים גדולים', mode)
    found = by_uid(rows)
    row = found[page(109)]                       # ראשית כהנים | גדולים סופית
    assert row['scope'] == scope and row['cross_page']
    assert found[page(108)]['scope'] == scope    # ראשון כהנים | גדולים אחרון
    assert found[page(102)]['scope'] == 'page'   # inside its page: the page row
    assert not cutoff['capped'] and not cutoff.get('partial')


def test_the_seams_do_not_hold_the_v07_restart_the_whole_manuscript_joins(engines):
    # Manuscript 105: its V0.8 page, then its V0.7 copy -- one whole-manuscript document on
    # either format, a junction no seam holds (§6.1 point 3).
    assert page(105) in by_uid(evf(engines['1'], 'מאמרו קוראים', 'literal')[0])
    assert page(105) not in by_uid(evf(engines['2'], 'מאמרו קוראים', 'literal')[0])


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_exact_seam_crossings_honour_the_restriction(engines, fmt):
    assert page(109) in by_uid(evf(engines[fmt], 'כהנים גדולים', 'literal', restrict_sys_ids={sid(109)})[0])
    assert page(109) not in by_uid(evf(engines[fmt], 'כהנים גדולים', 'literal', restrict_sys_ids={sid(108)})[0])


@pytest.mark.parametrize('fmt', ['2', '1'])
@pytest.mark.parametrize('word, excluded', [('סוף', True), ('פתיחה', True), ('אחרון', False)])
def test_an_exact_crossing_reads_its_excluded_words_on_the_pages_it_covers(engines, fmt, word, excluded):
    rows, _ = evf(engines[fmt], 'מאמתי קורין', 'literal', exclude_words=[word])
    assert (page(101) not in by_uid(rows)) == excluded


@pytest.mark.parametrize('fmt, scope', [('2', 'seam'), ('1', 'system')])
def test_exact_ids_only_gives_the_seam_crossing(engines, fmt, scope):
    rows, _ = evf(engines[fmt], 'מאמתי קורין', 'literal', ids_only=True)
    assert by_uid(rows)[page(101)]['scope'] == scope


@pytest.mark.parametrize('gap, scope', [(1, 'seam'), (3, 'system')])
def test_exact_past_k_keeps_the_whole_manuscripts(short_seams, gap, scope):
    # S = (2 - 1) x (gap + 1): 2 <= K = 2 reads the seams, 4 > K the whole manuscripts.
    rows, cutoff = evf(short_seams, 'כהנים גדולים', 'literal', gap)
    assert by_uid(rows)[page(109)]['scope'] == scope
    assert not cutoff['capped'] and not cutoff.get('partial')


# --- A line query with a star: every crossing of a manuscript (Codex round 2 on PR #442) ---

LINE_MANUSCRIPTS = {
    301: ["פתח מאמר\nקורא שם",          # p1: a match inside the page, before the crossings
          "סוף מאמתי",                  # p2 | p3: מאמתי at a line's end, קורין the next line
          "קורין כאן\nעוד מאמרי",       # p3 | p4: מאמרי, then קוראים
          "קוראים סוף"],
}
LINE_V08 = [(sid(n), 3000 + n, p, text) for n, pages in LINE_MANUSCRIPTS.items()
            for p, text in enumerate(pages, 1)]


@pytest.fixture(scope='module')
def line_engines(tmp_path_factory):
    with formats(tmp_path_factory, 'line_crossings', LINE_V08) as e:
        yield e


@pytest.mark.parametrize('fmt', ['2', '1'])
def test_a_star_line_query_gives_every_crossing_of_a_manuscript(line_engines, fmt):
    # The manuscript's first match is inside p1 (its page row); its whole-manuscript
    # document still gives both crossings, p2 | p3 and p3 | p4.
    rows = by_uid(search(line_engines[fmt], 'מאמ* | קור*')[0])
    assert set(rows) == {uid(3301, 1), uid(3301, 2), uid(3301, 3)}
    assert rows[uid(3301, 1)]['scope'] == 'page'
    assert rows[uid(3301, 2)]['cross_page'] and rows[uid(3301, 3)]['cross_page']
