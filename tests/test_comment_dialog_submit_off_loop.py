# -*- coding: utf-8 -*-
"""The web comment dialog saves the comment off the event loop with the visitor's client.

``submit_comment`` in web/components/comment_dialog.py called ``create_comment``
synchronously on the loop: one Oregon->Frankfurt round trip (~150 ms) during
which uvicorn's single worker served nobody. It now builds the visitor's
client on the loop (``get_user_client``; a worker cannot read the visitor's
storage and would write anonymously) and runs the write through
``run.io_bound(..., client=client)``. The element values are read before the
await, as the call's arguments. No answer from the worker (NiceGUI returns
None when the task is cancelled or the app is stopping) shows the dialog's
error label; nothing says "submitted".

(tests/test_comment_dialog_stored_choices.py is the DESKTOP dialog; this is
the first test of the web dialog's submit.) Drives the real dialog inside a
NiceGUI Client; no network.
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

from web.translations import tr

_paths = itertools.count()
SENTINEL = object()
FAILURE = 'The change could not be saved. Check your connection and try again.'


def _submit_handler_of(button):
    """The dialog's own ``submit_comment``, from the Submit button's click listener.

    ``ui.button(on_click=fn)`` stores ``lambda _: handle_event(fn, ...)``, which
    schedules an async ``fn`` as a background task instead of returning its
    coroutine; the test awaits ``fn`` itself, taken from that lambda's closure.
    """
    for listener in button._event_listeners.values():
        if listener.type != 'click':
            continue
        for cell in getattr(listener.handler, '__closure__', None) or ():
            try:
                fn = cell.cell_contents
            except ValueError:
                continue
            if callable(fn) and getattr(fn, '__name__', '') == 'submit_comment':
                return fn
    raise AssertionError('submit_comment is not wired to the Submit button')


@pytest.fixture
def dialog(monkeypatch):
    """The real comment dialog; ``submit(text)`` types the text and clicks Submit.

    ``answer`` is what create_comment returns (a dict, or None for no answer);
    ``calls`` the recorded writes; ``notes`` every ui.notify (message, type).
    """
    import web.auth_state as auth_state
    import web.components.comment_dialog as cd

    h = {'calls': [], 'client_threads': [], 'answer': {'success': True, 'comment': {'id': 9}},
         'notes': [], 'loop_thread': None, 'submitted': [], 'delay': 0.0}

    def record(*args, client=None, **kwargs):
        h['calls'].append({'thread': threading.get_ident(), 'client': client, 'args': args, 'kwargs': kwargs})
        if h['delay']:
            time.sleep(h['delay'])  # the round trip, in the worker
        return h['answer']

    def build_client():
        h['client_threads'].append(threading.get_ident())
        return SENTINEL

    monkeypatch.setattr(cd, 'create_comment', record)
    monkeypatch.setattr(cd, 'get_user_client', build_client)
    monkeypatch.setattr(cd.ui, 'notify', lambda message, *a, **k: h['notes'].append((str(message), k.get('type'))))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'is_logged_in', staticmethod(lambda: True))
    monkeypatch.setattr(auth_state.GlobalAuthState, 'get_user_id', staticmethod(lambda: 'user-1'))
    monkeypatch.setenv('GENIZAH_STORAGE_SECRET', 'x' * 40)
    prepare_simulation()

    async def submit(text, clicks=1):
        core.loop = asyncio.get_running_loop()
        h['loop_thread'] = threading.get_ident()
        with Client(ui.page(f'/_comment_probe_{next(_paths)}')) as client:
            cd.create_comment_dialog('990000000000017', page_number=1, shelfmark='T-S 1.1',
                                     on_submit=lambda: h['submitted'].append(True))
            elements = list(client.elements.values())
            textarea = next(e for e in elements if isinstance(e, ui.textarea))
            textarea.value = text
            button = next(e for e in elements if isinstance(e, ui.button)
                          and str(e._props.get('data-ux', '')).strip('"') == 'save_comment')
            handler = _submit_handler_of(button)
            if clicks == 1:
                await handler()
            else:
                async def click():  # a task of its own, as NiceGUI runs each click: enter the page
                    with client:
                        await handler()
                await asyncio.gather(*(click() for _ in range(clicks)))
            return next(e for e in elements if isinstance(e, ui.label) and 'text-red-500' in e._classes)

    h['submit'] = submit
    return h


def test_submit_comment_runs_off_the_loop_with_the_visitors_client(dialog):
    error_label = asyncio.run(dialog['submit']('  שלום  '))

    calls = dialog['calls']
    assert len(calls) == 1, calls
    assert calls[0]['thread'] != dialog['loop_thread'], 'the comment was written on the event loop'
    assert calls[0]['client'] is SENTINEL, "the write did not get the visitor's own client"
    kwargs = calls[0]['kwargs']
    assert kwargs['content'] == 'שלום', 'the textarea value must be read (and stripped) before the await'
    assert kwargs['author_id'] == 'user-1' and kwargs['sys_id'] == '990000000000017'
    assert kwargs['page_number'] == 1 and kwargs['scope'] == 'page' and kwargs['is_public'] is True
    assert dialog['client_threads'] == [dialog['loop_thread']], 'get_user_client must run once, on the loop'
    # The success path ran: a success toast, the callback, no error shown.
    assert [kind for _m, kind in dialog['notes']] == ['positive'], dialog['notes']
    assert dialog['submitted'] == [True]
    assert 'hidden' in error_label._classes


def test_no_answer_shows_the_error_label_and_claims_no_success(dialog):
    dialog['answer'] = None
    error_label = asyncio.run(dialog['submit']('שלום'))

    assert len(dialog['calls']) == 1
    assert error_label.text == tr(FAILURE)
    assert 'hidden' not in error_label._classes and 'visible' in error_label._classes
    assert dialog['notes'] == [] and dialog['submitted'] == []


def test_a_refused_write_shows_its_error(dialog):
    dialog['answer'] = {'error': 'new row violates row-level security policy'}
    error_label = asyncio.run(dialog['submit']('שלום'))

    assert error_label.text == 'new row violates row-level security policy'
    assert 'hidden' not in error_label._classes
    assert dialog['notes'] == [] and dialog['submitted'] == []


def test_two_quick_submit_clicks_post_the_comment_once(dialog):
    dialog['delay'] = 0.2
    asyncio.run(dialog['submit']('שלום', clicks=2))

    assert len(dialog['calls']) == 1, 'a second click while the write ran posted the comment twice'
    assert [kind for _m, kind in dialog['notes']] == ['positive'], dialog['notes']
    assert dialog['submitted'] == [True]
