# -*- coding: utf-8 -*-
"""The scholarly transcription filter: which result pages have a scholarly transcription
(shared.transcription_service.rows_with_scholarly_transcription), and the search page's
button driven through the real /search page (NiceGUI User simulation, a job queue in place
of the workers; no index, no network)."""
from __future__ import annotations

import asyncio
import logging
import os

import pytest

os.environ.setdefault('GENIZAH_STORAGE_SECRET', 'scholarly-filter-test-secret-0123456789abcdef')

from tests.test_web_variant_settings_per_visitor import (  # noqa: E402,F401  (server is a fixture)
    _element, _fire, run, server, store, stored, submit, wait_for_payloads,
)

ED = '990000000000000001'      # a PGP edition of its recto only
FGP = '990000000000000002'     # an FGP edition of image 62553, its page 2
TRANS = '990000000000000003'   # a PGP translation only
NONE = '990000000000000004'    # nothing scholarly
MIXED = '990000000000000005'   # a PGP edition of its recto, a translation of its verso


def _row(uid, sys_id, page, **extra):
    row = {'uid': uid, 'raw_header': '', 'full_text': 'נוסח הדף',
           'display': {'id': sys_id, 'img': str(page), 'source': 'V0.8'}}
    row.update(extra)
    return row


@pytest.fixture
def sidecars(monkeypatch):
    """Stand-in PGP and FGP sidecars: what the chooser's sources and the edition sets hold."""
    import shared.document_service as document_service
    import shared.fgp_service as fgp_service
    sources = {
        ED: [{'doc_relation': 'Edition', 'content': 'מהדורה', 'page_info': 'recto'}],
        FGP: [{'source': 'fgp', 'is_fgp': True, 'doc_relation': 'Digital Edition',
               'content': 'מהדורת FGP', 'c_number': 'C62553'}],
        TRANS: [{'doc_relation': 'Translation', 'content': 'a translation', 'page_info': 'recto'}],
        MIXED: [{'doc_relation': 'Edition', 'content': 'מהדורה', 'page_info': 'recto'},
                {'doc_relation': 'Translation', 'content': 'a translation', 'page_info': 'verso'}],
    }
    reads = []

    def pgp_sources(sys_id, strict=False):
        reads.append(sys_id)
        return [dict(s) for s in sources.get(sys_id, []) if not s.get('is_fgp')]

    monkeypatch.setattr(document_service, 'get_all_sources_for_fragment', pgp_sources)
    monkeypatch.setattr(document_service, 'get_sys_ids_with_editions',
                        lambda ids=None, strict=False: {s for s in (ids or []) if s in (ED, TRANS, MIXED)})
    monkeypatch.setattr(fgp_service, 'get_fgp_sources_for_fragment',
                        lambda sys_id, strict=False: [dict(s) for s in sources.get(sys_id, []) if s.get('is_fgp')])
    monkeypatch.setattr(fgp_service, 'get_sys_ids_with_fgp_editions',
                        lambda ids=None, strict=False: {s for s in (ids or []) if s == FGP})
    monkeypatch.setattr(fgp_service, 'displayed_folio_label', lambda sys_id, p, total=0: '')
    monkeypatch.setattr(fgp_service, 'displayed_fgp_image_number',
                        lambda sys_id, p, total=0: {1: '62552', 2: '62553'}.get(p, ''))
    return reads


def _found(rows, *, include_fgp=True, read_page=None):
    from shared.transcription_service import rows_with_scholarly_transcription
    pages = read_page or (lambda row: (int(row['display']['img']), row['full_text'], 2))
    check = rows_with_scholarly_transcription(rows, read_page=pages, include_fgp=include_fgp)
    _found.complete = check.complete
    return check.found


def test_a_page_has_one_where_the_version_chooser_offers_an_edition(sidecars):
    rows = [_row('ed-1', ED, 1), _row('ed-2', ED, 2), _row('fgp-1', FGP, 1), _row('fgp-2', FGP, 2),
            _row('trans-1', TRANS, 1), _row('none-1', NONE, 1),
            _row('mixed-1', MIXED, 1), _row('mixed-2', MIXED, 2)]
    assert _found(rows) == {'ed-1', 'fgp-2', 'mixed-1'}


def test_only_manuscripts_with_an_edition_are_read(sidecars):
    """The batch edition sets decide which manuscripts are opened at all."""
    asked = []
    _found([_row('none-1', NONE, 1), _row('ed-1', ED, 1)],
           read_page=lambda row: asked.append(row['uid']) or (1, 'נוסח', 2))
    assert sidecars == [ED] and asked == ['ed-1']


def test_the_web_fgp_switch_leaves_the_fgp_editions_out(sidecars):
    assert _found([_row('fgp-2', FGP, 2), _row('ed-1', ED, 1)], include_fgp=False) == {'ed-1'}


def _edition(kind):
    return {'kind': kind, 'id': '7', 'version': f'{kind}:7'}


def test_a_row_found_in_a_scholarly_transcription_has_one_unplaced_or_not(sidecars):
    """A page's group (the automatic hit leads, an edition is a member) and an unplaced hit."""
    rows = [_row('hit', NONE, 1, edition_match=True,
                 members=[{'uid': 'a'}, {'uid': 'b', 'edition': _edition('fgp')}]),
            _row('unplaced', NONE, '', edition=_edition('pgp'))]
    assert _found(rows, read_page=lambda row: pytest.fail('no page read for an edition hit')) == {
        'hit', 'unplaced'}


def test_an_fgp_edition_hit_counts_only_where_fgp_is_shown(sidecars):
    """The web FGP switch off: a row found only in an FGP edition is checked by its page like
    any other, and a row also found in a PGP edition still counts (Codex review of PR #22)."""
    rows = [_row('fgp-only', NONE, 1, edition_match=True, edition=_edition('fgp'),
                 members=[{'uid': 'f', 'edition': _edition('fgp')}]),
            _row('both', NONE, 1, edition_match=True,
                 members=[{'uid': 'f', 'edition': _edition('fgp')}, {'uid': 'p', 'edition': _edition('pgp')}])]
    assert _found(rows, include_fgp=False) == {'both'}
    assert _found(rows, include_fgp=True) == {'fgp-only', 'both'}


def test_a_page_the_index_lacks_is_read_as_quick_view_shows_it(sidecars):
    """No page in the index: the result's own page number (Quick View's), not a miss."""
    assert _found([_row('ed-1', ED, 1), _row('ed-2', ED, 2)], read_page=lambda row: None) == {'ed-1'}


def test_one_manuscript_failing_leaves_the_others_checked_and_is_logged(sidecars, monkeypatch, caplog):
    import shared.fgp_service as fgp_service

    def broken(sys_id, strict=False):
        raise RuntimeError('sidecar gone')
    monkeypatch.setattr(fgp_service, 'get_fgp_sources_for_fragment', broken)
    with caplog.at_level(logging.WARNING, logger='shared.transcription_service'):
        assert _found([_row('fgp-2', FGP, 2), _row('ed-1', ED, 1)], include_fgp=True) == set()
    assert 'not checked' in caplog.text
    # Not answered, not "none": the caller must not apply the filter on it (Codex, PR #22).
    assert _found.complete is False
    # FGP off: the PGP edition is found, the broken FGP read never made.
    assert _found([_row('fgp-2', FGP, 2), _row('ed-1', ED, 1)], include_fgp=False) == {'ed-1'}
    assert _found.complete is True


def test_the_edition_sets_are_read_strictly_and_a_failed_read_is_not_none(sidecars, monkeypatch):
    """The batch edition sets are asked for strictly; one that cannot be read leaves the check
    incomplete, not "no editions", and an edition hit still counts (Codex review of PR #22)."""
    import shared.document_service as document_service
    asked = []

    def editions(ids=None, strict=False):
        asked.append(strict)
        raise RuntimeError('database is locked')
    monkeypatch.setattr(document_service, 'get_sys_ids_with_editions', editions)
    rows = [_row('ed-1', ED, 1), _row('hit', NONE, 1, edition=_edition('pgp'))]
    assert _found(rows) == {'hit'} and _found.complete is False
    assert asked == [True]


def _bare(cls, **attrs):
    svc = cls.__new__(cls)
    for name, value in attrs.items():
        setattr(svc, name, value)
    return svc


class _FailingConn:
    def execute(self, *args, **kwargs):
        raise RuntimeError('database is locked')


def test_the_pgp_reads_raise_on_a_failed_query_when_strict_and_stay_quiet_otherwise():
    """Strict: a failed query raises; a sidecar that is not there answers empty, as the
    chooser shows nothing from it (Codex reviews of PR #22)."""
    from shared.document_service import PgpService
    svc = _bare(PgpService, _conn=_FailingConn())
    assert svc.get_sys_ids_with_editions([ED]) == set()
    assert svc.get_all_sources_for_fragment(ED) == []
    with pytest.raises(RuntimeError):
        svc.get_sys_ids_with_editions([ED], strict=True)
    with pytest.raises(RuntimeError):
        svc.get_all_sources_for_fragment(ED, strict=True)
    absent = _bare(PgpService, _conn=None)
    assert absent.get_sys_ids_with_editions([ED], strict=True) == set()
    assert absent.get_all_sources_for_fragment(ED, strict=True) == []


def _absent_fgp():
    from shared.fgp_service import FgpService
    return _bare(FgpService, _conn=None, _table=None, _columns=set())


def test_the_fgp_reads_raise_on_a_failed_query_when_strict(monkeypatch):
    """Strict: a failed query raises; a sidecar that is not there, or FGP switched off,
    answers empty, as the chooser shows nothing from it (Codex reviews of PR #22)."""
    from shared.fgp_service import FgpService
    monkeypatch.setenv('FGP_TRANSCRIPTIONS_ENABLED', '1')
    svc = _bare(FgpService, _conn=_FailingConn(), _table='fgp', _columns={'sys_id', 'id'})
    assert svc.get_sys_ids_with_fgp_editions([FGP]) == set()
    assert svc.get_fgp_sources_for_fragment(FGP) == []
    with pytest.raises(RuntimeError):
        svc.get_sys_ids_with_fgp_editions([FGP], strict=True)
    with pytest.raises(RuntimeError):
        svc.get_fgp_sources_for_fragment(FGP, strict=True)
    for flag in ('1', '0'):
        monkeypatch.setenv('FGP_TRANSCRIPTIONS_ENABLED', flag)
        assert _absent_fgp().get_sys_ids_with_fgp_editions([FGP], strict=True) == set()
        assert _absent_fgp().get_fgp_sources_for_fragment(FGP, strict=True) == []


def test_a_missing_fgp_sidecar_leaves_the_check_complete(sidecars, monkeypatch):
    """No FGP sidecar installed: the PGP pages are still answered and the check completes,
    so the filter works (Codex review of PR #22)."""
    import shared.fgp_service as fgp_service
    monkeypatch.setenv('FGP_TRANSCRIPTIONS_ENABLED', '1')
    absent = _absent_fgp()
    monkeypatch.setattr(fgp_service, 'get_sys_ids_with_fgp_editions',
                        lambda ids=None, strict=False: absent.get_sys_ids_with_fgp_editions(ids, strict=strict))
    monkeypatch.setattr(fgp_service, 'get_fgp_sources_for_fragment',
                        lambda sid, strict=False: absent.get_fgp_sources_for_fragment(sid, strict=strict))
    assert _found([_row('ed-1', ED, 1), _row('fgp-2', FGP, 2)]) == {'ed-1'}
    assert _found.complete is True


def test_the_filter_state_travels_with_the_run_and_is_checked_on_restore():
    from web.pages.search_state import (
        SearchUIState, _post_filters, preview_shows_final_rows, restore_post_filters,
    )
    state = SearchUIState()
    assert preview_shows_final_rows(state)
    state.scholarly_filter = 'hide_scholarly'
    assert _post_filters(state)['scholarly_filter'] == 'hide_scholarly'
    assert not preview_shows_final_rows(state)
    fresh = SearchUIState()
    restore_post_filters(fresh, {'scholarly_filter': 'only_scholarly'})
    assert fresh.scholarly_filter == 'only_scholarly'
    restore_post_filters(fresh, {'scholarly_filter': 'bogus'})
    assert fresh.scholarly_filter == 'only_scholarly'


def test_new_search_resets_the_stored_filter():
    import inspect

    from web.pages import search_state
    assert "'search_scholarly_filter': 'all'" in inspect.getsource(search_state.clear_search_snapshot)


def _count_line(user):
    from nicegui import ui
    return _element(user, ui.label, lambda e: str(e.text).rstrip().endswith('Results')
                    or ' Results (' in str(e.text)).text


WORD = 'שלום'


def _answer_with_rows(srv, monkeypatch, scholarly_uids, rows=None, check=None):
    """The search's rows (three by default) from the queue; the pages with one are
    *scholarly_uids*, or what *check* answers for each batch of rows the page asks about.
    Returns the list of the uids the check was asked about."""
    from concurrent.futures import Future

    import web.pages.search as search_page
    rows = rows or [_row('r1', ED, 1, snippet='*שלום*'), _row('r2', NONE, 1, snippet='*שלום*'),
                    _row('r3', TRANS, 1, snippet='*שלום*')]
    checked = []

    from shared.transcription_service import ScholarlyCheck

    def scholarly(rows_):
        checked.extend(r['uid'] for r in rows_)
        answer = check(rows_) if check is not None else {
            r['uid'] for r in rows_ if r['uid'] in scholarly_uids}
        return answer if isinstance(answer, ScholarlyCheck) else ScholarlyCheck(answer, True)
    monkeypatch.setattr(search_page, '_web_scholarly_page_uids', scholarly)
    answer = srv.queue.submit

    def submit_answering(payload):
        job = answer(payload)
        if srv.queue.query_of(payload) == WORD:
            job.future = Future()
            job.future.set_result({'value': [dict(r) for r in rows]})
        return job
    srv.queue.submit = submit_answering
    return checked


def _filter_button(user):
    from nicegui import ui
    return _element(user, ui.button, lambda e: str(e.text) in (
        'Scholarly transcriptions', 'Has scholarly transcription',
        'No scholarly transcription'))


def _shown(button):
    """The page hides a filter button with the `tb-hidden` class (and visibility: hidden)."""
    return 'tb-hidden' not in button.classes


_MODE_LABELS = {'only': 'Has scholarly transcription', 'hide': 'No scholarly transcription',
                'all': 'All'}


def _choose(user, mode):
    """Pick *mode* from the scholarly transcription chip's menu, as a reader does."""
    from nicegui import ui
    btn = _filter_button(user)
    with user._client:
        item = next(e for e in btn.descendants() if isinstance(e, ui.menu_item) and any(
            getattr(c, 'text', None) == _MODE_LABELS[mode] for c in e.descendants()))
    _fire(user, item, 'click')


async def _until_button_shows(user):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 15
    while not _shown(_filter_button(user)):
        assert loop.time() < deadline, 'the filter button never showed'
        await asyncio.sleep(0.05)


def _count_line_now(user):
    """The count line, or None while the page shows none (a search starting)."""
    try:
        return _count_line(user)
    except AssertionError:
        return None


def test_the_button_filters_the_results_by_their_pages(server, monkeypatch):  # noqa: F811
    """A search's rows, the pages with a scholarly transcription known from the enrichment:
    the button cycles all -> only those -> only the others -> all, the count line says
    which, and the choice is stored for the next visit."""
    checked = _answer_with_rows(server, monkeypatch, {'r1'})
    seen = {}

    async def driver(a):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        for name in ('only', 'hide', 'all'):
            _choose(a, name)
            await asyncio.sleep(0.2)
            seen[name] = (_count_line(a), _filter_button(a).text, stored(a, 'search_scholarly_filter'))

    run(driver, count=1)
    assert sorted(set(checked)) == ['r1', 'r2', 'r3']
    assert seen['only'] == ('1 of 3 Results (Has scholarly transcription)',
                            'Has scholarly transcription', 'only_scholarly'), seen
    assert seen['hide'] == ('2 of 3 Results (No scholarly transcription)',
                            'No scholarly transcription', 'hide_scholarly'), seen
    assert seen['all'][1:] == ('Scholarly transcriptions', 'all'), seen
    assert seen['all'][0].startswith('3 Results'), seen


def test_a_kept_mode_applies_when_no_page_has_one_and_can_be_turned_off(server, monkeypatch):  # noqa: F811
    """'Only with one', kept from an earlier visit, and a search none of whose pages has one:
    no rows, the count line says why, and the button shows so the mode can be turned off --
    not every row under a count line that claims the filter (Codex review of PR #22)."""
    _answer_with_rows(server, monkeypatch, set())
    seen = {}

    async def driver(a):
        await a.open('/search')
        store(a, search_scholarly_filter='only_scholarly')
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        await asyncio.sleep(0.2)
        seen['kept'] = (_count_line(a), _filter_button(a).text)
        for name in ('hide', 'all'):
            _choose(a, name)
            await asyncio.sleep(0.2)
        seen['off'] = (_count_line(a), _filter_button(a).text, stored(a, 'search_scholarly_filter'))

    run(driver, count=1)
    assert seen['kept'] == ('0 of 3 Results (Has scholarly transcription)',
                            'Has scholarly transcription'), seen
    assert seen['off'][1:] == ('Scholarly transcriptions', 'all'), seen
    assert seen['off'][0].startswith('3 Results'), seen


@pytest.mark.parametrize('background', ['answers', 'fails', 'first unanswered',
                                        'background unanswered'])
def test_a_kept_mode_waits_for_every_row_to_be_checked(server, monkeypatch, background):  # noqa: F811
    """60 rows: the visible 50 are checked first, the rest in the background. A kept mode
    applies only once every row is checked; if the background pass fails, the rows are shown
    unfiltered and the count line does not claim the filter (Codex review of PR #22)."""
    rows = [_row(f'r{i}', f'99{i:016d}', 1, snippet='*שלום*') for i in range(60)]
    batches = []

    from shared.transcription_service import ScholarlyCheck

    def check(batch):
        batches.append(len(batch))
        if len(batches) > 1 and background == 'fails':
            raise RuntimeError('sidecar gone')
        found = {r['uid'] for r in batch if r['uid'] in ('r1', 'r55')}
        # A pass that could not read some manuscript answers incomplete (Codex, PR #22).
        unanswered = {'first unanswered': len(batches) == 1,
                      'background unanswered': len(batches) > 1}.get(background, False)
        return ScholarlyCheck(found, not unanswered)
    _answer_with_rows(server, monkeypatch, set(), rows=rows, check=check)
    seen = {}

    async def driver(a):
        await a.open('/search')
        store(a, search_scholarly_filter='only_scholarly')
        await a.open('/search')
        # Past the page's session-restore pass (0.2 s after it opens), which checks every
        # row of results it finds: here the search's own passes must do it.
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        while len(batches) < 2:
            assert loop.time() < deadline, batches
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.5)
        seen['end'] = (_count_line(a), _shown(_filter_button(a)))

    run(driver, count=1)
    assert batches == [50, 10], batches
    if background == 'answers':
        assert seen['end'] == ('2 of 60 Results (Has scholarly transcription)', True), seen
    else:
        assert seen['end'][1] is False and seen['end'][0].startswith('60 Results'), seen
        assert 'scholarly' not in seen['end'][0], seen


def test_a_single_pass_that_could_not_answer_leaves_the_rows_unfiltered(server, monkeypatch):  # noqa: F811
    """Three rows, one pass, a manuscript it could not read: a kept mode is not applied, the
    count line does not claim it and the button stays hidden (Codex review of PR #22)."""
    from shared.transcription_service import ScholarlyCheck
    _answer_with_rows(server, monkeypatch, set(), check=lambda rows_: ScholarlyCheck({'r1'}, False))
    seen = {}

    async def driver(a):
        await a.open('/search')
        store(a, search_scholarly_filter='only_scholarly')
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await asyncio.sleep(1.0)
        seen['end'] = (_count_line(a), _shown(_filter_button(a)))

    run(driver, count=1)
    assert seen['end'][1] is False and seen['end'][0].startswith('3 Results'), seen
    assert 'scholarly' not in seen['end'][0], seen


def test_the_filters_panel_keeps_the_scholarly_filter(server, monkeypatch):  # noqa: F811
    """Toggle Filters' Apply and Clear start from every row: the scholarly button's choice
    stays in force under both (Codex review of PR #22)."""
    from nicegui import ui
    _answer_with_rows(server, monkeypatch, {'r1'})
    seen = {}

    async def driver(a):
        await a.open('/search')
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        _choose(a, 'only')                # only those with one
        await asyncio.sleep(0.2)
        for name in ('Apply Filters', 'Clear Filters'):
            _fire(a, _element(a, ui.button, lambda e, n=name: str(e.text) == n), 'click')
            await asyncio.sleep(0.3)
            seen[name] = _count_line(a)

    run(driver, count=1)
    assert seen == {'Apply Filters': '1 / 3 Results (Has scholarly transcription)',
                    'Clear Filters': '1 / 3 Results (Has scholarly transcription)'}, seen


def test_the_button_keeps_the_rows_excluded_from_the_word_search_out(server, monkeypatch):  # noqa: F811
    """Rows excluded from the word search stay out when the filter changes: the button
    passes through the word-search exclusions first, as the search's own render does
    (Codex review of PR #22)."""
    _answer_with_rows(server, monkeypatch, {'r1'})
    seen = {}

    async def driver(a):
        await a.open('/search')
        store(a, word_search_excluded_ids=[NONE])      # r2's manuscript
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        for name in ('only', 'hide'):
            _choose(a, name)
            await asyncio.sleep(0.2)
            seen[name] = _count_line(a)

    run(driver, count=1)
    assert seen == {'only': '1 of 3 Results (1 excluded, Has scholarly transcription)',
                    'hide': '1 of 3 Results (1 excluded, No scholarly transcription)'}, seen


def test_a_restored_history_entry_is_checked_before_the_filter_applies(server, monkeypatch):  # noqa: F811
    """A history entry that kept its rows replaces the shown search's: the filter forgets that
    search's answers and checks the entry's rows first, then applies to them (Codex review of
    PR #22)."""
    from tests.test_web_variant_settings_per_visitor import _restore_history
    earlier = 'ישן'
    entry = {'query': earlier, 'mode': 'exact', 'result_count': 2, 'params': {'mode': 'exact'},
             'state': {'results': [_row('b1', ED, 1, snippet=''), _row('b2', NONE, 1, snippet='')]}}
    _answer_with_rows(server, monkeypatch, set(),
                      check=lambda rows_: {r['uid'] for r in rows_ if r['uid'] in ('r1', 'b1')})
    seen = {}

    async def driver(a):
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        _choose(a, 'only')                # only those with one
        await asyncio.sleep(0.2)
        seen['shown'] = _count_line(a)
        store(a, search_history=[entry])
        _restore_history(a, earlier)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        while 'of 2 Results' not in _count_line(a):
            assert loop.time() < deadline, _count_line(a)
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.2)
        seen['restored'] = (_count_line(a), _shown(_filter_button(a)))

    run(driver, count=1)
    assert seen['shown'] == '1 of 3 Results (Has scholarly transcription)', seen
    assert seen['restored'] == ('1 of 2 Results (Has scholarly transcription)', True), seen


def test_a_session_restore_that_ends_after_a_new_search_changes_nothing(server, monkeypatch):  # noqa: F811
    """The page opens on saved rows and their check is slow; a new search meanwhile gets its
    own answers. The saved rows' answer, arriving last, must not replace them (Codex review
    of PR #22)."""
    import threading
    saved = [_row('o1', ED, 1, snippet=''), _row('o2', NONE, 1, snippet='')]
    release = threading.Event()
    restore_started = threading.Event()

    def check(rows_):
        if any(r['uid'] == 'o1' for r in rows_):      # the saved rows: slow
            restore_started.set()
            release.wait(10)
            return {'o1'}
        return {r['uid'] for r in rows_ if r['uid'] == 'r1'}
    _answer_with_rows(server, monkeypatch, set(), check=check)
    seen = {}

    async def driver(a):
        await a.open('/search')
        store(a, search_results=[dict(r) for r in saved], search_scholarly_filter='only_scholarly')
        await a.open('/search')
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        while not restore_started.is_set():
            assert loop.time() < deadline, 'the saved rows were never checked'
            await asyncio.sleep(0.05)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        while _count_line_now(a) != '1 of 3 Results (Has scholarly transcription)':
            assert loop.time() < deadline, _count_line_now(a)
            await asyncio.sleep(0.05)
        release.set()
        await asyncio.sleep(1.0)
        seen['after'] = _count_line(a)
        _choose(a, 'hide')       # the next mode reads the stored answers
        await asyncio.sleep(0.3)
        seen['hide'] = _count_line(a)

    run(driver, count=1)
    assert seen['after'] == '1 of 3 Results (Has scholarly transcription)', seen
    assert seen['hide'] == '2 of 3 Results (No scholarly transcription)', seen


def test_each_row_is_checked_once_the_first_page_first(server, monkeypatch):  # noqa: F811
    """60 hits on one edited manuscript: the first pass checks the first 50 rows, the
    background pass the other 10 -- not every row of the manuscript twice (Codex review of
    PR #22)."""
    rows = [_row(f'r{i}', ED, i + 1, snippet='*שלום*') for i in range(60)]
    batches = []

    def check(batch):
        batches.append(sorted(int(r['uid'][1:]) for r in batch))
        return set()
    _answer_with_rows(server, monkeypatch, set(), rows=rows, check=check)

    async def driver(a):
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 15
        while len(batches) < 2:
            assert loop.time() < deadline, [len(b) for b in batches]
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.3)

    run(driver, count=1)
    assert batches == [list(range(50)), list(range(50, 60))], [len(b) for b in batches]


def test_changing_the_mode_keeps_the_filters_panel_fields(server, monkeypatch):  # noqa: F811
    """With the Toggle Filters panel's text field applied, changing the scholarly mode keeps
    the rows it took out out (Codex review of PR #22)."""
    from nicegui import ui
    rows = [_row('r1', ED, 1, snippet='aaa'), _row('r2', NONE, 1, snippet='aaa'),
            _row('r3', TRANS, 1, snippet='bbb')]
    _answer_with_rows(server, monkeypatch, {'r1'}, rows=rows)
    seen = {}

    async def driver(a):
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        text = _element(a, ui.input, lambda e: e.props.get('placeholder') == 'Filter by text')
        with a._client:
            text.value = 'aaa'
        _fire(a, _element(a, ui.button, lambda e: str(e.text) == 'Apply Filters'), 'click')
        await asyncio.sleep(0.3)
        seen['panel'] = _count_line(a)
        for name in ('only', 'hide'):
            _choose(a, name)
            await asyncio.sleep(0.3)
            seen[name] = _count_line(a)

    run(driver, count=1)
    assert seen == {'panel': '2 / 3 Results',
                    'only': '1 / 3 Results (Has scholarly transcription)',
                    'hide': '1 / 3 Results (No scholarly transcription)'}, seen


def _reset_button(user):
    from nicegui import ui
    return _element(user, ui.button, lambda e: str(e.text) == 'Reset filters')


def test_reset_filters_shows_while_a_filter_is_on_and_clears_them(server, monkeypatch):  # noqa: F811
    """Filters kept from an earlier visit: the row offers Reset filters, which clears them
    (stored too) and shows every row; with none on, it is hidden (owner, 2026-11-16)."""
    _answer_with_rows(server, monkeypatch, {'r1'})
    seen = {}

    async def driver(a):
        await a.open('/search')
        store(a, search_scholarly_filter='only_scholarly', search_pgp_filter='hide_pgp')
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        await asyncio.sleep(0.2)
        seen['before'] = (_count_line(a), _shown(_reset_button(a)))
        _fire(a, _reset_button(a), 'click')
        await asyncio.sleep(0.3)
        seen['after'] = (_count_line(a), _shown(_reset_button(a)), _filter_button(a).text,
                         stored(a, 'search_scholarly_filter'), stored(a, 'search_pgp_filter'))

    run(driver, count=1)
    assert seen['before'] == ('1 of 3 Results (Hiding PGP, Has scholarly transcription)', True), seen
    assert seen['after'][0].startswith('3 Results'), seen
    assert seen['after'][1:] == (False, 'Scholarly transcriptions', 'all', 'all'), seen


def test_the_pgp_chip_menu_filters_and_the_chip_fills(server, monkeypatch):  # noqa: F811
    """The PGP filter is a chip with a menu, not a cycle: choosing "No PGP" hides the rows with
    PGP, names itself on the chip and fills it; "All" empties it again (owner's chip design,
    2026-11-16)."""
    from nicegui import ui

    import web.pages.search as search_page
    _answer_with_rows(server, monkeypatch, {'r1'})
    monkeypatch.setattr(search_page, 'get_sys_ids_with_transcriptions',
                        lambda ids: {i for i in ids if i == ED})
    seen = {}

    def chip():
        return _element(a_user['a'], ui.button, lambda e: str(e.text) in ('PGP', 'Has PGP', 'No PGP'))

    def pick(label):
        with a_user['a']._client:
            item = next(e for e in chip().descendants() if isinstance(e, ui.menu_item) and any(
                getattr(c, 'text', None) == label for c in e.descendants()))
        _fire(a_user['a'], item, 'click')

    a_user = {}

    async def driver(a):
        a_user['a'] = a
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        seen['off'] = (chip().text, 'unelevated' in chip().props, 'filter-chip-off' in chip().classes)
        pick('No PGP')
        await asyncio.sleep(0.3)
        seen['on'] = (_count_line(a), chip().text, 'unelevated' in chip().props,
                      stored(a, 'search_pgp_filter'), 'filter-chip-off' in chip().classes)
        pick('All')
        await asyncio.sleep(0.3)
        seen['all'] = (_count_line(a), chip().text, 'unelevated' in chip().props,
                       'filter-chip-off' in chip().classes)

    run(driver, count=1)
    # Off: outline in the theme's colour (filter-chip-off); on: filled.
    assert seen['off'] == ('PGP', False, True), seen
    assert seen['on'] == ('2 of 3 Results (Hiding PGP)', 'No PGP', True, 'hide_pgp', False), seen
    assert seen['all'][0].startswith('3 Results') and seen['all'][1:] == ('PGP', False, True), seen


def test_a_chip_choice_applies_under_the_filters_panel_fields(server, monkeypatch):  # noqa: F811
    """With the Toggle Filters panel's text field applied, a chip's choice re-renders through
    the panel's Apply, which must stack the chip's filter on the panel's fields: "No PGP" takes
    the PGP row out, "All" brings it back, the panel's field stays (Codex review of PR #26)."""
    from nicegui import ui

    import web.pages.search as search_page
    rows = [_row('r1', ED, 1, snippet='aaa'), _row('r2', NONE, 1, snippet='aaa'),
            _row('r3', TRANS, 1, snippet='bbb')]
    _answer_with_rows(server, monkeypatch, {'r1'}, rows=rows)
    monkeypatch.setattr(search_page, 'get_sys_ids_with_transcriptions',
                        lambda ids: {i for i in ids if i == ED})
    seen = {}
    a_user = {}

    def pick(label):
        a = a_user['a']
        chip = _element(a, ui.button, lambda e: str(e.text) in ('PGP', 'Has PGP', 'No PGP'))
        with a._client:
            item = next(e for e in chip.descendants() if isinstance(e, ui.menu_item) and any(
                getattr(c, 'text', None) == label for c in e.descendants()))
        _fire(a, item, 'click')

    async def driver(a):
        a_user['a'] = a
        await a.open('/search')
        await asyncio.sleep(0.5)
        submit(a, WORD)
        await wait_for_payloads(server.queue, 1)
        await _until_button_shows(a)
        text = _element(a, ui.input, lambda e: e.props.get('placeholder') == 'Filter by text')
        with a._client:
            text.value = 'aaa'
        _fire(a, _element(a, ui.button, lambda e: str(e.text) == 'Apply Filters'), 'click')
        await asyncio.sleep(0.3)
        seen['panel'] = _count_line(a)
        for label in ('No PGP', 'All'):
            pick(label)
            await asyncio.sleep(0.3)
            seen[label] = _count_line(a)

    run(driver, count=1)
    assert seen == {'panel': '2 / 3 Results', 'No PGP': '1 / 3 Results',
                    'All': '2 / 3 Results'}, seen
