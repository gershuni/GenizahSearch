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
"""
from __future__ import annotations

import asyncio
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
    """
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
         'notes': [], 'states': [], 'loop_thread': None, 'pages': [], 'delay': 0.0}

    def recorder(name):
        def record(*args, client=None, **kwargs):
            h['calls'].append({'helper': name, 'thread': threading.get_ident(), 'client': client,
                               'args': args, 'kwargs': kwargs})
            if h['delay']:
                time.sleep(h['delay'])  # the round trip, in the worker
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

def _drive_twice(editor, first, second, *, between=0.0, during=None):
    async def _run():
        client, state = await editor['open_in_edit_mode']()
        a, b = _handler(client, first), _handler(client, second)

        async def later():
            await asyncio.sleep(between)
            if during:
                during(state)
            await b()

        with client:
            await asyncio.gather(a(), later())
        return client, state

    return asyncio.run(_run())


def _kinds(editor):
    return [(c['helper'], (c['kwargs'].get('status') if c['kwargs'] else c['args'][1]['status']))
            for c in editor['calls']]


def test_two_quick_save_draft_clicks_write_the_draft_then_update_it(editor):
    editor['delay'] = 0.2
    _client, state = _drive_twice(editor, 'handle_save_draft', 'handle_save_draft')

    assert _kinds(editor) == [('create_correction', 'draft'), ('update_correction', 'draft')], editor['calls']
    assert editor['calls'][1]['args'][0] == 41, 'the second save must update the draft the first created'
    assert state.draft_id == 41 and not state.correction_lock.locked()


def test_submit_during_a_draft_save_waits_and_submits_the_draft(editor):
    """Dropping the Submit (an in-flight flag) left the correction unsubmitted
    with no word to the reader; it waits for the draft and submits it."""
    editor['delay'] = 0.2
    _client, state = _drive_twice(editor, 'handle_save_draft', 'handle_submit_correction', between=0.05)

    assert _kinds(editor) == [('create_correction', 'draft'), ('update_correction', 'pending')], editor['calls']
    assert state.edit_mode is False, 'the Submit did not run'


def test_two_quick_submit_clicks_submit_once(editor):
    editor['delay'] = 0.2
    _client, state = _drive_twice(editor, 'handle_submit_correction', 'handle_submit_correction')

    assert [c['helper'] for c in editor['calls']] == ['create_correction'], editor['calls']
    assert not state.correction_lock.locked()


def test_text_typed_during_a_draft_save_is_not_marked_saved(editor):
    editor['delay'] = 0.3

    async def _run():
        client, state = await editor['open_in_edit_mode']()
        save = _handler(client, 'handle_save_draft')

        async def type_more():
            await asyncio.sleep(0.1)
            state.edit_text = 'שלום עולם ומלואו'  # bind_value keeps writing while the save runs

        with client:
            await asyncio.gather(save(), type_more())
        return state

    state = asyncio.run(_run())
    assert editor['calls'][0]['kwargs']['corrected_text'] == 'שלום עולם'
    assert state.draft_id == 41
    assert state.draft_saved is False, 'text that is not in the draft was shown as Saved'


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
