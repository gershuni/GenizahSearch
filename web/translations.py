# -*- coding: utf-8 -*-
"""
Web application translations.

Uses a simple key-value approach where English is the key
and translations are the values. Supports RTL languages.
"""

import functools
import weakref
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Callable, Iterator, TypeVar

from nicegui.slot import Slot, get_task_id
from nicegui.storage import request_contextvar

from shared.genizah_translations import TRANSLATIONS

_T = TypeVar('_T')

# The language is each visitor's own. One process serves every visitor on one
# event loop, so a single module-level value was whatever the LAST page render
# set: a page that drew after an await, or a download built by an API route,
# could come out in another visitor's language. get_language() now answers,
# most specific first:
#   1. inside ``using_language(lang)``: lang;
#   2. in a page build, or a task it started (asyncio tasks copy the context of
#      the code that created them): the language create_layout() gave the page
#      -- likewise for a task that called set_language() itself;
#   3. inside a page otherwise (the current NiceGUI client: its event handlers,
#      timers, ``with page_client:`` blocks): the language that page was built
#      in, recorded on the client by set_language();
#   4. inside an HTTP request (API routes such as the exports): the visitor's
#      saved choice, else their browser's language -- the rule create_layout
#      applies (web/main.py::_resolve_ui_language);
#   5. otherwise (a worker thread, startup, a test): the process default below.
# A worker thread (``run.io_bound``) gets no context at all, so code there must
# be handed the language (``build_author_options(lang)`` and the like), or run
# through ``in_visitor_language(fn)``.
_current_lang = 'he'  # the process default (5); set_language() changes it only outside any task
_override_lang: ContextVar[str | None] = ContextVar('genizah_override_lang', default=None)
_page_lang: ContextVar[str | None] = ContextVar('genizah_page_lang', default=None)
_client_lang: 'weakref.WeakKeyDictionary[object, str]' = weakref.WeakKeyDictionary()
_REQUEST_CACHE_ATTR = 'genizah_ui_language'


def _current_client():
    """The NiceGUI client whose slot this asyncio task is in, or None.

    Reads NiceGUI's per-task slot stack directly: ``context.client`` raises
    when there is none, and ``context.slot_stack`` creates a script-mode
    client when the app has not started. Task id 0 (no running task: a worker
    thread, or sync code) is never treated as a page.
    """
    task_id = get_task_id()
    if task_id == 0:
        return None
    stack = Slot.stacks.get(task_id)
    if not stack:
        return None
    try:
        return stack[-1].parent.client
    except RuntimeError:  # the slot's element was deleted
        return None


def set_language(lang: str) -> None:
    """Set the language of the page being built ('he' or 'en').

    Inside a page this is that page's language: its later event handlers and
    the tasks it starts use it too, and no other visitor's page is affected.
    Elsewhere in an asyncio task it is that task's language (and the tasks it
    starts). Outside any task (startup, a script, a synchronous test) it sets
    the process default.
    """
    if get_task_id() == 0:
        global _current_lang
        _current_lang = lang
        return
    client = _current_client()
    if client is not None:
        _client_lang[client] = lang
    _page_lang.set(lang)


def get_language() -> str:
    """The current visitor's language code (see the precedence above)."""
    lang = _override_lang.get()
    if lang is not None:
        return lang
    lang = _page_lang.get()
    if lang is not None:
        return lang
    client = _current_client()
    if client is not None:
        lang = _client_lang.get(client)
        if lang is not None:
            return lang
    lang = _request_language()
    if lang is not None:
        return lang
    return _current_lang


def language_from_accept_language(header: str | None) -> str:
    """'he' when the browser's first-choice language is Hebrew, otherwise 'en'.

    ``header`` is an HTTP ``Accept-Language`` value, e.g.
    ``"he-IL,he;q=0.9,en;q=0.8"``. The first choice is the entry with the
    highest q (the earliest on a tie); q=0 means "not acceptable" and is
    skipped. ``iw`` is the legacy code for Hebrew. No header (most crawlers
    send none) or an unparsable one gives 'en'.
    """
    best_tag, best_q = '', 0.0
    for part in (header or '').split(','):
        tag, _, params = part.strip().partition(';')
        q = 1.0
        for param in params.split(';'):
            key, _, value = param.strip().partition('=')
            if key.strip().lower() == 'q':
                try:
                    q = float(value)
                except ValueError:
                    q = 0.0
        if tag and q > best_q:
            best_tag, best_q = tag, q
    primary = best_tag.split('-')[0].strip().lower()
    return 'he' if primary in ('he', 'iw') else 'en'


def browser_language() -> str:
    """The default UI language for the current request's browser.

    Reads NiceGUI's per-request context, which page builds, their event
    handlers and FastAPI routes all carry. Outside a request: 'en'.
    """
    request = request_contextvar.get()
    if request is None:
        return 'en'
    return language_from_accept_language(request.headers.get('accept-language'))


def visitor_language(saved: str | None) -> str:
    """A visitor's UI language: their saved choice, else their browser's.

    ``saved`` is the ``ui_language`` value from their storage (anything other
    than 'he'/'en' counts as no choice).
    """
    if saved in ('he', 'en'):
        return saved
    return browser_language()


def _request_language() -> str | None:
    """The language of the visitor making the current HTTP request, or None.

    Computed once per request and kept on ``request.state``.
    """
    request = request_contextvar.get()
    if request is None:
        return None
    request_state = getattr(request, 'state', None)
    cached = getattr(request_state, _REQUEST_CACHE_ATTR, None)
    if cached is not None:
        return cached
    from web.safe_storage import safe_user_get

    try:
        lang = visitor_language(safe_user_get('ui_language'))
    except Exception:
        return None  # tr() must never fail: an unreadable request gives the default
    if request_state is not None:
        try:
            setattr(request_state, _REQUEST_CACHE_ATTR, lang)
        except Exception:
            pass  # a request without a writable state: compute it again next time
    return lang


@contextmanager
def using_language(lang: str) -> Iterator[None]:
    """Run a block in ``lang``, then restore what was there before.

    Overrides every other source (see get_language). The setting is local to
    this asyncio task's context, so the block may await: other visitors'
    tasks never see it.
    """
    token = _override_lang.set(lang)
    try:
        yield
    finally:
        _override_lang.reset(token)


def in_visitor_language(fn: Callable[..., _T]) -> Callable[..., _T]:
    """``fn``, to run in a worker thread in the current visitor's language.

    The language is read now, on the event loop; the returned callable runs
    ``fn`` inside ``using_language`` of it. For ``run.io_bound(...)``, whose
    threads get no context (``run.io_bound(in_visitor_language(fn), *args)``).
    Only the language is carried: copying the whole context would also hand
    the thread the request, and with it the visitor's storage.
    """
    lang = get_language()

    @functools.wraps(fn)
    def _in_language(*args, **kwargs):
        with using_language(lang):
            return fn(*args, **kwargs)

    return _in_language


def is_rtl() -> bool:
    """Check if current language is RTL."""
    return get_language() == 'he'


def tr(text: str) -> str:
    """
    Translate text to current language.

    Args:
        text: English text to translate

    Returns:
        Translated text if available, otherwise original text
    """
    if get_language() == 'en':
        return text

    return TRANSLATIONS.get(text, text)


def get_dir() -> str:
    """Get text direction for current language."""
    return 'rtl' if is_rtl() else 'ltr'
