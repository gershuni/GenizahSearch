# -*- coding: utf-8 -*-
"""/browse's Submit and Save Draft write the correction off the event loop.

``handle_submit_correction`` and ``handle_save_draft`` called
``create_correction`` / ``update_correction`` synchronously on the loop: an
Oregon->Frankfurt round trip (~150 ms) during which no visitor was served.
Both now build the visitor's client on the loop (``get_user_client``; a worker
cannot read the visitor's storage and would write anonymously, refused by RLS)
and run the write through ``run.io_bound(..., client=client)``. No answer from
the worker (NiceGUI returns None when the task is cancelled or the app is
stopping) is a failure toast, and the editor stays in edit mode with the text
intact.

Harness: tests/test_browse_recents_off_loop.py -- a real /browse page on a fake
service inside a NiceGUI Client, put into edit mode through its state and
re-rendered through the page's own ``load_page``.

Every test here holds ``with client:`` across awaits, and NiceGUI keeps one
slot stack per asyncio task, keyed by the task's id. An earlier test can leave
a page task pending inside its own ``with client:`` when its loop closes (the
CI log names the joins button's ``_runner``); when the garbage collector later
destroys it, its ``with`` exit pops the slot stack of
whichever task is running, and the test's own ``with client:`` then fails with
``IndexError: pop from empty list`` (CI on 0fb81585). The ``editor`` fixture
collects that garbage before the test opens any slot.
"""
from __future__ import annotations

import asyncio
import gc
import itertools
import threading
import time

import pytest
from nicegui import core, ui
from nicegui.client import Client
from nicegui.testing.general import prepare_simulation

from tests.test_browse_recents_off_loop import _load_page_of, _next_buttons, _wait
from tests.test_browse_recents_off_loop import supabase_offline  # noqa: F401  (a fixture)
from web.translations import tr

_paths = itertools.count()
SENTINEL = object()
FAILURE = 'The change could not be saved. Check your connection and try again.'


def _handler(client, name):
    """The page's own handler ``name``, through the live button it is wired to.

    ``ui.button(on_click=fn)`` stores ``lambda _: handle_event(fn, ...)``, which
    schedules an async ``fn`` as a background task instead of returning its
    coroutine; the test awaits ``fn`` itself, taken from that lambda's closure.
    Only a DIRECT closure cell counts, so a button whose handler merely reaches
    the function through ``update_content`` (every navigation button) does not.
    """
    for element in client.elements.values():
        if not isinstance(element, ui.button):
            continue
        for listener in element._event_listeners.values():
            if listener.type != 'click':
                continue
            for cell in getattr(listener.handler, '__closure__', None) or ():
                try:
                    fn = cell.cell_contents
                except ValueError:
                    continue
                if callable(fn) and getattr(fn, '__name__', '') == name:
                    return fn
    return None


@pytest.fixture
def editor(monkeypatch, supabase_offline):  # noqa: F811  (the imported fixture, requested by name)
    """A signed-in /browse page in edit mode; every correction write is recorded.

    ``answer`` is what the write returns (a dict, or None for no answer);
    ``calls`` the recorded writes; ``notes`` every ui.notify (message, type).
    ``writing`` / ``hold`` (threading.Events): the write sets ``writing`` once
    it runs in its worker, then waits for ``hold`` -- a step the test takes
    while the write is provably in flight, whatever the loop's timing.
    """
    # Destroy the pending page tasks earlier tests left on their closed loops
    # now, before this test opens a slot they could pop (module docstring).
    gc.collect()

    from web.services import BrowsePage
    import web.auth_state as auth_state
    import web.pages.browse as browse
    import web.supabase_client as supabase_client
    from web.state import state as app_state

    def page(p):
        return BrowsePage(uid=f'u{p}', p_num=p, text='שלום', full_header='hdr',
                          total_pages=3, current_idx=p, sys_id='990000000000017',
                          shelfmark='T-S 1.1', fl_id=f'FL{p}')

    class _Service:
        is_ready = True

        def get_browse_page(self, sys_id, p_num=1, direction=0, volume_ie=None, **_kwargs):
            return page(max(1, min(3, (p_num or 1) + (direction or 0))))

        def __getattr__(self, _name):
            return lambda *a, **k: None

    class _ListsMgr:
        meta_mgr = None

        def add_to_recent_sync(self, *_a, **_k):
            raise AssertionError('the recents write ran on the event loop')

    h = {'calls': [], 'client_threads': [], 'answer': {'success': True, 'correction': {'id': 41}},
         'notes': [], 'states': [], 'loop_thread': None, 'pages': [],
         'writing': None, 'hold': None}

    def recorder(name):
        def record(*args, client=None, **kwargs):
            h['calls'].append({'helper': name, 'thread': threading.get_ident(), 'client': client,
                               'args': args, 'kwargs': kwargs})
            if h['writing'] is not None:
                h['writing'].set()
            if h['hold'] is not None and not h['hold'].wait(5):  # the round trip, in the worker
                h['calls'][-1]['hold_timed_out'] = True
            return h['answer']
        record.__name__ = name
        return record

    def build_client():
        h['client_threads'].append(threading.get_ident())
        return SENTINEL

    class _State(browse.BrowseState):
        def __init__(self, *a, **k):
            super().__init__(*a, **k)
            h['states'].append(self)

    monkeypatch.setattr(browse, 'get_service', lambda: _Service())
    monkeypatch.setattr(browse, 'BrowseState', _State)
    monkeypatch.setattr(browse, 'create_correction', recorder('create_correction'))
    monkeypatch.setattr(browse, 'update_correction', recorder('update_correction'))
    monkeypatch.setattr(browse, 'get_user_client', build_client)
    monkeypatch.setattr(browse.ui, 'notify',
                        lambda message, *a, **k: h['notes'].append((str(message), k.get('type'))))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'is_logged_in', staticmethod(lambda: True))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'get_user_id', staticmethod(lambda: 'user-1'))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'is_admin', staticmethod(lambda: False))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'is_editor', staticmethod(lambda: False))
    monkeypatch.setattr(supabase_client, 'add_recent_item', lambda *a, **k: {'success': True})
    monkeypatch.setattr(supabase_client, 'get_user_client', lambda: SENTINEL)
    monkeypatch.setattr(app_state, 'lists_mgr', _ListsMgr(), raising=False)
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()

    async def open_in_edit_mode():
        core.loop = asyncio.get_running_loop()
        h['loop_thread'] = threading.get_ident()
        with Client(ui.page(f'/_correction_probe_{next(_paths)}')) as client:
            h['pages'].append(client)
            browse.create_browse_page(initial_sys_id='990000000000017')
        assert await _wait(lambda: _next_buttons(client)), 'the page rendered no Next button'
        state = h['states'][-1]
        state.edit_mode = True
        state.original_edit_text = 'שלום'
        state.edit_text = 'שלום עולם'
        state.edit_notes = ''
        load_page = _load_page_of(client)
        with client:
            await load_page(direction=0)  # re-render, now in edit mode
        assert await _wait(lambda: _handler(client, 'handle_submit_correction')), 'no Submit button in edit mode'
        return client, state

    h['open_in_edit_mode'] = open_in_edit_mode
    return h


def _drive(editor, handler_name, *, draft_id=None):
    async def _run():
        client, state = await editor['open_in_edit_mode']()
        state.draft_id = draft_id
        handler = _handler(client, handler_name)
        with client:
            await handler()
        return state

    return asyncio.run(_run())


def _types(editor):
    return [kind for _message, kind in editor['notes']]


# ---------------------------------------------------------------------------
# Submit
# ---------------------------------------------------------------------------

def test_submit_writes_off_the_loop_with_the_visitors_client_then_leaves_edit_mode(editor):
    state = _drive(editor, 'handle_submit_correction')

    calls = editor['calls']
    assert [c['helper'] for c in calls] == ['create_correction'], calls
    assert calls[0]['thread'] != editor['loop_thread'], 'the correction was written on the event loop'
    assert calls[0]['client'] is SENTINEL, "the write did not get the visitor's own client"
    assert calls[0]['kwargs']['corrected_text'] == 'שלום עולם'
    assert calls[0]['kwargs']['original_text'] == 'שלום'
    assert calls[0]['kwargs']['status'] == 'pending'
    assert calls[0]['kwargs']['author_id'] == 'user-1'
    assert editor['client_threads'] == [editor['loop_thread']], 'get_user_client must run once, on the loop'
    # The success path ran: out of edit mode, the draft cleared, a success toast.
    assert state.edit_mode is False and state.draft_id is None and state.draft_saved is False
    assert 'positive' in _types(editor), editor['notes']


def test_submit_of_a_saved_draft_updates_it_off_the_loop(editor):
    _drive(editor, 'handle_submit_correction', draft_id=41)

    calls = editor['calls']
    assert [c['helper'] for c in calls] == ['update_correction'], calls
    assert calls[0]['thread'] != editor['loop_thread'] and calls[0]['client'] is SENTINEL
    assert calls[0]['args'] == (41, {'corrected_text': 'שלום עולם', 'notes': '', 'status': 'pending'})


def test_submit_with_no_answer_toasts_a_failure_and_stays_in_edit_mode(editor):
    editor['answer'] = None
    state = _drive(editor, 'handle_submit_correction')

    assert len(editor['calls']) == 1
    assert (tr(FAILURE), 'negative') in editor['notes'], editor['notes']
    assert 'positive' not in _types(editor)
    assert state.edit_mode is True, 'the editor left edit mode although nothing was saved'
    assert state.edit_text == 'שלום עולם', 'the unsaved text was lost'


# ---------------------------------------------------------------------------
# Save Draft
# ---------------------------------------------------------------------------

def test_save_draft_writes_off_the_loop_then_marks_the_draft_saved(editor):
    state = _drive(editor, 'handle_save_draft')

    calls = editor['calls']
    assert [c['helper'] for c in calls] == ['create_correction'], calls
    assert calls[0]['thread'] != editor['loop_thread'], 'the draft was written on the event loop'
    assert calls[0]['client'] is SENTINEL
    assert calls[0]['kwargs']['status'] == 'draft'
    assert editor['client_threads'] == [editor['loop_thread']]
    assert state.draft_saved is True and state.draft_id == 41 and state.edit_mode is True
    assert 'positive' in _types(editor), editor['notes']
    # update_content ran after the write: the edit bar now says Saved.
    labels = [e.text for e in editor['pages'][-1].elements.values() if isinstance(e, ui.label)]
    assert tr('Saved') in labels, labels


def test_save_draft_of_an_existing_draft_updates_it_off_the_loop(editor):
    state = _drive(editor, 'handle_save_draft', draft_id=41)

    calls = editor['calls']
    assert [c['helper'] for c in calls] == ['update_correction'], calls
    assert calls[0]['thread'] != editor['loop_thread'] and calls[0]['client'] is SENTINEL
    assert calls[0]['args'] == (41, {'corrected_text': 'שלום עולם', 'notes': '', 'status': 'draft'})
    assert state.draft_saved is True


def test_save_draft_with_no_answer_toasts_a_failure_and_keeps_the_draft_unsaved(editor):
    editor['answer'] = None
    state = _drive(editor, 'handle_save_draft')

    assert len(editor['calls']) == 1
    assert (tr(FAILURE), 'negative') in editor['notes'], editor['notes']
    assert 'positive' not in _types(editor)
    assert state.edit_mode is True and state.draft_saved is False and state.draft_id is None
    assert state.edit_text == 'שלום עולם'


# ---------------------------------------------------------------------------
# A second click while the first write runs. The write is off the loop now, so
# the loop serves the second click at once: without a lock, two drafts. It now
# waits for the first and then runs, as it did when the write blocked.
# ---------------------------------------------------------------------------

async def _until_writing(editor, timeout=5.0):
    """Wait, on the loop, until the fixture's write is running in its worker."""
    deadline = time.monotonic() + timeout
    while not editor['writing'].is_set():
        assert time.monotonic() < deadline, 'the write never started'
        await asyncio.sleep(0.01)


def _drive_twice(editor, first, second):
    """Click ``first``; once its write runs, click ``second``; then let the write answer.

    The second click is tied to the first write, not to a clock: with sleeps,
    a loop busy at the start of the save could serve it after the write, where
    it passes without the lock it is here to test.
    """
    editor['writing'], editor['hold'] = threading.Event(), threading.Event()

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        a, b = _handler(client, first), _handler(client, second)

        async def later():
            await _until_writing(editor)
            second_click = asyncio.ensure_future(b())
            # One turn of the loop takes the second click to its first await:
            # the lock, or (without one) its own write.
            await asyncio.sleep(0.05)
            editor['hold'].set()
            await second_click

        with client:
            await asyncio.gather(a(), later())
        return client, state

    result = asyncio.run(_run())
    assert not any(c.get('hold_timed_out') for c in editor['calls']), editor['calls']
    return result


def _kinds(editor):
    return [(c['helper'], (c['kwargs'].get('status') if c['kwargs'] else c['args'][1]['status']))
            for c in editor['calls']]


def test_two_quick_save_draft_clicks_write_the_draft_then_update_it(editor):
    _client, state = _drive_twice(editor, 'handle_save_draft', 'handle_save_draft')

    assert _kinds(editor) == [('create_correction', 'draft'), ('update_correction', 'draft')], editor['calls']
    assert editor['calls'][1]['args'][0] == 41, 'the second save must update the draft the first created'
    assert state.draft_id == 41 and not state.correction_lock.locked()


def test_submit_during_a_draft_save_waits_and_submits_the_draft(editor):
    """Dropping the Submit (an in-flight flag) left the correction unsubmitted
    with no word to the reader; it waits for the draft and submits it."""
    _client, state = _drive_twice(editor, 'handle_save_draft', 'handle_submit_correction')

    assert _kinds(editor) == [('create_correction', 'draft'), ('update_correction', 'pending')], editor['calls']
    assert state.edit_mode is False, 'the Submit did not run'


def test_two_quick_submit_clicks_submit_once(editor):
    _client, state = _drive_twice(editor, 'handle_submit_correction', 'handle_submit_correction')

    assert [c['helper'] for c in editor['calls']] == ['create_correction'], editor['calls']
    assert not state.correction_lock.locked()


def test_text_typed_during_a_draft_save_is_not_marked_saved(editor):
    # Typing is tied to the write itself, not to a clock: a sleep(0.1) inside a
    # 0.3 s write typed AFTER the write when the loop was busy for 0.35 s at the
    # start of the save (a lone run of this test on Windows, NiceGUI 3.18).
    editor['writing'], editor['hold'] = threading.Event(), threading.Event()

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        save = _handler(client, 'handle_save_draft')

        async def type_more():
            await _until_writing(editor)
            state.edit_text = 'שלום עולם ומלואו'  # bind_value keeps writing while the save runs
            editor['hold'].set()  # only now may the write answer

        with client:
            await asyncio.gather(save(), type_more())
        return state

    state = asyncio.run(_run())
    assert 'hold_timed_out' not in editor['calls'][0], 'the write answered before the typing'
    assert editor['calls'][0]['kwargs']['corrected_text'] == 'שלום עולם'
    assert state.draft_id == 41
    assert state.draft_saved is False, 'text that is not in the draft was shown as Saved'


def test_a_draft_that_lands_after_a_page_turn_does_not_become_the_new_pages_draft(editor):
    """Save Draft, then a page turn while the write runs (Codex on #430): the
    old draft's id must not land in the state of the page now shown, or the
    next save would overwrite that draft with the other page's text."""
    editor['writing'], editor['hold'] = threading.Event(), threading.Event()

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        save = _handler(client, 'handle_save_draft')
        load_page = _load_page_of(client)

        async def turn_page():
            await _until_writing(editor)  # the page turns while the write is in flight
            await load_page(direction=1)
            editor['hold'].set()  # only now may the write answer

        with client:
            await asyncio.gather(save(), turn_page())
        return state

    state = asyncio.run(_run())
    assert 'hold_timed_out' not in editor['calls'][0], 'the write answered before the page turn'
    assert [c['helper'] for c in editor['calls']] == ['create_correction'], editor['calls']
    assert state.current_page.p_num == 2
    assert state.draft_id is None, "the previous page's draft id became the new page's draft"
    assert state.draft_saved is False
    assert 'positive' in [kind for _m, kind in editor['notes']], 'the save itself succeeded and says so'


def test_a_failed_save_leaves_the_next_click_free_to_write(editor):
    editor['answer'] = None

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        handler = _handler(client, 'handle_save_draft')
        with client:
            await handler()
            assert not state.correction_lock.locked(), 'the lock outlived a failed write'
            editor['answer'] = {'success': True, 'correction': {'id': 41}}
            await handler()
        return state

    state = asyncio.run(_run())
    assert [c['helper'] for c in editor['calls']] == ['create_correction', 'create_correction']
    assert state.draft_id == 41


def test_a_stale_button_after_a_submit_writes_nothing(editor):
    """After a successful Submit the page reloads with the old form kept,
    dimmed, under the spinner: a second activation of its (focused) Submit or
    Save Draft must not write again. The handlers act only in edit mode and
    never during a load."""
    async def _run():
        client, state = await editor['open_in_edit_mode']()
        submit = _handler(client, 'handle_submit_correction')
        draft = _handler(client, 'handle_save_draft')
        with client:
            await submit()  # success: out of edit mode, the page reloads
            assert state.edit_mode is False
            await submit()
            await draft()
            state.edit_mode = True  # and while a page loads, even in edit mode
            state.is_loading = True
            await submit()
            await draft()
            state.is_loading = False
        return state

    asyncio.run(_run())
    assert [c['helper'] for c in editor['calls']] == ['create_correction'], editor['calls']


# ---------------------------------------------------------------------------
# A page turn while editing (GS-5). The editor used to survive it: the
# textarea kept page 1's text over page 2, and Save Draft or Submit then filed
# that text under page 2. The edit now belongs to its page
# (web/pages/browse_correction_edit.py): the turn closes the editor, and an
# unsaved edit is kept as a draft of the page it was typed on.
# ---------------------------------------------------------------------------

def _turn_page(editor, *, text=None, then=None):
    """Open page 1 in edit mode, set the editor's text, turn to page 2, run ``then``."""
    async def _run():
        client, state = await editor['open_in_edit_mode']()
        if text is not None:
            state.edit_text = text
        load_page = _load_page_of(client)
        with client:
            await load_page(direction=1)
            if then is not None:
                await then(client, state)
        return state

    return asyncio.run(_run())


def test_a_page_turn_keeps_an_unsaved_edit_as_a_draft_of_its_own_page(editor):
    async def until_written(_client, _state):
        assert await _wait(lambda: editor['calls'], timeout=10), 'the unsaved edit was not kept'

    state = _turn_page(editor, then=until_written)

    assert state.current_page.p_num == 2
    assert state.edit_mode is False and state.edit_page is None and state.draft_id is None
    calls = editor['calls']
    assert [c['helper'] for c in calls] == ['create_correction'], calls
    kw = calls[0]['kwargs']
    assert (kw['page_number'], kw['corrected_text'], kw['status']) == (1, 'שלום עולם', 'draft'), kw
    assert calls[0]['thread'] != editor['loop_thread'] and calls[0]['client'] is SENTINEL
    assert (tr('Draft saved'), 'positive') in editor['notes'], editor['notes']


def test_save_draft_or_submit_after_a_page_turn_never_files_under_the_new_page(editor):
    """The reported fault: a write after the turn landed on page 2 with page 1's text."""
    async def click_both(client, _state):
        await _wait(lambda: editor['calls'], timeout=10)       # the kept draft of page 1
        for name in ('handle_save_draft', 'handle_submit_correction'):
            handler = _handler(client, name)
            if handler is not None:
                await handler()

    _turn_page(editor, then=click_both)

    pages = [c['kwargs'].get('page_number') for c in editor['calls'] if c['helper'] == 'create_correction']
    assert 2 not in pages, editor['calls']


def test_a_page_turn_with_nothing_typed_closes_the_editor_without_a_write(editor):
    async def settle(_client, _state):
        await asyncio.sleep(0.3)

    state = _turn_page(editor, text='שלום', then=settle)         # the page's own text: nothing typed

    assert state.edit_mode is False and editor['calls'] == [], editor['calls']


def test_an_edit_entered_through_the_edit_button_belongs_to_its_page(editor, monkeypatch):
    """toggle_edit_mode binds the edit when it opens, with its resumed draft as already saved."""
    import web.pages.browse as browse
    monkeypatch.setattr(browse, 'get_corrections', lambda **_k: [
        {'id': 7, 'page_number': 1, 'corrected_text': 'טיוטה', 'notes': '', 'status': 'draft', 'created_at': '1'}])

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        state.edit_mode, state.edit_page = False, None
        with client:
            await _load_page_of(client)(direction=0)              # re-render in view mode: the Edit button
        assert await _wait(lambda: _handler(client, 'toggle_edit_mode')), 'no Edit button'
        toggle = _handler(client, 'toggle_edit_mode')
        with client:
            toggle()
        return state

    state = asyncio.run(_run())
    assert state.edit_mode is True and state.draft_id == 7 and state.edit_text == 'טיוטה'
    page = state.edit_page
    assert (page.sys_id, page.p_num, page.start, page.saved, page.saved_draft_id) == \
        ('990000000000017', 1, ('טיוטה', ''), ('טיוטה', ''), 7)


def test_an_edit_that_could_not_be_kept_reopens_when_the_reader_returns(editor):
    """The write of a left page's edit fails: the text is not lost, and the editor reopens with it
    when that page is shown again (Codex on #118)."""
    editor['answer'] = None

    async def go_back(client, state):
        assert await _wait(lambda: editor['calls'], timeout=10)
        assert await _wait(lambda: state.unkept, timeout=10), 'the failed edit was not kept'
        await _load_page_of(client)(direction=-1)

    state = _turn_page(editor, then=go_back)

    assert state.current_page.p_num == 1
    assert state.edit_mode is True and state.edit_text == 'שלום עולם' and state.unkept == {}
    assert (tr(FAILURE), 'negative') in editor['notes'], editor['notes']


def test_a_submission_in_flight_during_a_page_turn_is_not_turned_back_into_a_draft(editor):
    """Submit, then a page turn while it runs: the kept draft queued behind it must not write the
    submitted text back as a draft (Codex on #118)."""
    editor['writing'], editor['hold'] = threading.Event(), threading.Event()

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        submit = _handler(client, 'handle_submit_correction')
        load_page = _load_page_of(client)

        async def turn_page():
            await _until_writing(editor)
            await load_page(direction=1)
            editor['hold'].set()

        with client:
            await asyncio.gather(submit(), turn_page())
            await asyncio.sleep(0.5)                              # room for a queued kept draft to run
        return state

    asyncio.run(_run())
    assert not any(c.get('hold_timed_out') for c in editor['calls']), editor['calls']
    assert _kinds(editor) == [('create_correction', 'pending')], editor['calls']


def test_a_submission_that_lands_after_the_reader_opened_another_page_leaves_that_editor_open(editor, monkeypatch):
    """Submit page 1, turn to page 2 and open its editor while the submission runs: when it lands,
    the editor on page 2 and its text are the reader's (Codex on #118, round 2)."""
    import web.pages.browse as browse
    monkeypatch.setattr(browse, 'get_corrections', lambda **_k: [])
    editor['writing'], editor['hold'] = threading.Event(), threading.Event()

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        submit = _handler(client, 'handle_submit_correction')

        async def edit_page_two():
            await _until_writing(editor)
            await _load_page_of(client)(direction=1)
            assert await _wait(lambda: _handler(client, 'toggle_edit_mode')), 'no Edit button on page 2'
            _handler(client, 'toggle_edit_mode')()
            state.edit_text = 'עמוד שני'
            editor['hold'].set()

        with client:
            await asyncio.gather(submit(), edit_page_two())
            await asyncio.sleep(0.5)
        return state

    state = asyncio.run(_run())
    assert _kinds(editor) == [('create_correction', 'pending')], editor['calls']
    assert editor['calls'][0]['kwargs']['page_number'] == 1
    assert state.current_page.p_num == 2 and state.edit_mode is True and state.edit_text == 'עמוד שני'
    assert state.edit_page is not None and state.edit_page.p_num == 2


def test_edit_on_a_page_whose_kept_draft_is_still_being_written_does_not_make_a_second_draft(editor):
    """Turn away with an unsaved edit, come back and press Edit while its draft is still being
    written: the editor reopens with that edit, and a Save then updates the kept draft instead of
    creating another (Codex on #118, round 2)."""
    editor['writing'], editor['hold'] = threading.Event(), threading.Event()

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        load_page = _load_page_of(client)
        with client:
            await load_page(direction=1)                          # the keep starts, and is held
            await _until_writing(editor)
            await load_page(direction=-1)                         # back to page 1
            assert await _wait(lambda: _handler(client, 'toggle_edit_mode')), 'no Edit button on page 1'
            _handler(client, 'toggle_edit_mode')()
            assert state.edit_mode is True and state.edit_text == 'שלום עולם', 'the kept edit did not reopen'
            editor['hold'].set()                                  # the kept draft lands: id 41
            assert await _wait(lambda: not state.keeping, timeout=10)
            state.edit_text = 'שלום עולם ומלואו'
            await _handler(client, 'handle_save_draft')()
        return state

    asyncio.run(_run())
    kinds = [c['helper'] for c in editor['calls']]
    assert kinds == ['create_correction', 'update_correction'], editor['calls']
    assert editor['calls'][1]['args'][0] == 41, 'the save did not update the kept draft'


@pytest.mark.parametrize('open_page_two', [False, True], ids=['page-turn', 'page-two-editor-opened'])
def test_a_submit_waiting_behind_a_save_goes_out_for_its_own_page_after_a_page_turn(editor, monkeypatch, open_page_two):
    """Save Draft, Submit while the save runs (it waits for the lock), then a page turn before the save
    answers: the Submit still goes out, for page 1 with page 1's text. It used to read the editor after
    the wait: closed, it dropped the Submit; open on page 2, it submitted page 2 (Codex on #118, round 3)."""
    import web.pages.browse as browse
    monkeypatch.setattr(browse, 'get_corrections', lambda **_k: [])
    editor['writing'], editor['hold'] = threading.Event(), threading.Event()

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        save, submit = _handler(client, 'handle_save_draft'), _handler(client, 'handle_submit_correction')

        async def submit_then_leave():
            await _until_writing(editor)
            queued = asyncio.ensure_future(submit())
            await asyncio.sleep(0.05)                             # the Submit reaches the lock and waits
            await _load_page_of(client)(direction=1)
            if open_page_two:
                assert await _wait(lambda: _handler(client, 'toggle_edit_mode')), 'no Edit button on page 2'
                _handler(client, 'toggle_edit_mode')()
                state.edit_text = 'עמוד שני'
            editor['hold'].set()
            await queued

        with client:
            await asyncio.gather(save(), submit_then_leave())
            await asyncio.sleep(0.5)                              # room for page 1's kept draft to run
        return state

    state = asyncio.run(_run())
    assert not any(c.get('hold_timed_out') for c in editor['calls']), editor['calls']
    assert _kinds(editor) == [('create_correction', 'draft'), ('update_correction', 'pending')], editor['calls']
    assert editor['calls'][1]['args'][0] == 41 and editor['calls'][1]['args'][1]['corrected_text'] == 'שלום עולם'
    assert (tr('Correction submitted successfully'), 'positive') in editor['notes'], editor['notes']
    assert state.current_page.p_num == 2 and state.edit_mode is open_page_two
    if open_page_two:
        assert state.edit_text == 'עמוד שני' and state.edit_page.p_num == 2


def test_save_draft_waiting_behind_a_submit_of_the_same_text_writes_no_draft(editor):
    """The Save Draft is fixed when clicked, so it runs after the submission lands: it must not file
    the submitted text again as a new draft."""
    _client, state = _drive_twice(editor, 'handle_submit_correction', 'handle_save_draft')

    assert _kinds(editor) == [('create_correction', 'pending')], editor['calls']
    assert state.edit_mode is False and not state.correction_lock.locked()


@pytest.mark.parametrize('first_fails', [False, True], ids=['first-kept', 'first-failed'])
def test_a_newer_edit_waiting_to_be_kept_survives_the_older_keep_finishing(editor, monkeypatch, first_fails):
    """Turn away with an edit; come back while its draft is written, reopen it, change it, turn away
    again: a second keep waits behind the first. When the first is done (or fails) the second is still
    being kept, so Edit on the page opens the newest text, not the server's older one, and a failure
    of the older text is not reopened over it (Codex on #118, round 4)."""
    import web.pages.browse as browse
    monkeypatch.setattr(browse, 'get_corrections', lambda **_k: [
        {'id': 41, 'page_number': 1, 'corrected_text': 'שלום עולם', 'notes': '', 'status': 'draft', 'created_at': '1'}])
    first_hold = threading.Event()
    editor['writing'], editor['hold'] = threading.Event(), first_hold
    newest = 'שלום עולם ומלואו'

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        load_page = _load_page_of(client)
        with client:
            await load_page(direction=1)                          # keep 1 starts, and is held
            await _until_writing(editor)
            await load_page(direction=-1)
            assert await _wait(lambda: _handler(client, 'toggle_edit_mode')), 'no Edit button on page 1'
            _handler(client, 'toggle_edit_mode')()
            state.edit_text = newest
            await load_page(direction=1)                          # keep 2 waits behind keep 1
            editor['writing'], editor['hold'] = threading.Event(), threading.Event()
            if first_fails:
                editor['answer'] = None
            first_hold.set()                                      # keep 1 answers; keep 2's write starts, held
            await _until_writing(editor)
            editor['answer'] = {'success': True, 'correction': {'id': 41}}
            being_kept, unkept = [k.text for k in state.keeping.values()], dict(state.unkept)
            await load_page(direction=-1)                         # back to page 1 while keep 2 is written
            assert await _wait(lambda: _handler(client, 'toggle_edit_mode')), 'no Edit button on page 1'
            _handler(client, 'toggle_edit_mode')()
            reopened = state.edit_text
            editor['hold'].set()
            assert await _wait(lambda: not state.keeping, timeout=10)
        return being_kept, unkept, reopened

    being_kept, unkept, reopened = asyncio.run(_run())
    assert not any(c.get('hold_timed_out') for c in editor['calls']), editor['calls']
    assert being_kept == [newest], 'the older keep, done, dropped the newer one still being kept'
    assert unkept == {}, "the older keep's failure was kept to reopen over the newer edit"
    assert reopened == newest, 'Edit opened the older text'
    assert [c['helper'] for c in editor['calls']] == ['create_correction', 'create_correction' if first_fails
                                                       else 'update_correction'], editor['calls']


@pytest.mark.parametrize('typed', [False, True], ids=['as-submitted', 'newer-text'])
def test_a_submission_landing_in_its_reopened_editor_keeps_text_typed_since(editor, typed):
    """Submit page 1, turn away and come back while it runs, reopen the edit (still being kept) and
    type: when the submission lands, the editor keeps the newer text, and its Save is a new draft, never
    an update of the submitted row. Nothing typed, the editor closes as after any submission (Codex on
    #118, round 5)."""
    editor['writing'], editor['hold'] = threading.Event(), threading.Event()
    newest = 'שלום עולם ומלואו'

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        submit = _handler(client, 'handle_submit_correction')
        load_page = _load_page_of(client)

        async def come_back_and_type():
            await _until_writing(editor)
            await load_page(direction=1)                          # page 1's keep waits behind the submission
            await load_page(direction=-1)
            assert await _wait(lambda: _handler(client, 'toggle_edit_mode')), 'no Edit button on page 1'
            _handler(client, 'toggle_edit_mode')()
            assert state.edit_mode is True and state.edit_text == 'שלום עולם', 'the kept edit did not reopen'
            if typed:
                state.edit_text = newest
            editor['hold'].set()

        with client:
            await asyncio.gather(submit(), come_back_and_type())
            assert await _wait(lambda: not state.keeping, timeout=10)
            after = (state.edit_mode, state.edit_text, state.draft_id)
            if typed:
                await _handler(client, 'handle_save_draft')()
        return after

    edit_mode, text, draft_id = asyncio.run(_run())
    assert not any(c.get('hold_timed_out') for c in editor['calls']), editor['calls']
    if not typed:
        assert edit_mode is False and _kinds(editor) == [('create_correction', 'pending')], editor['calls']
        return
    assert (edit_mode, text, draft_id) == (True, newest, None), 'the newer text was discarded'
    assert _kinds(editor) == [('create_correction', 'pending'), ('create_correction', 'draft')], editor['calls']
    assert editor['calls'][1]['kwargs']['corrected_text'] == newest
