# -*- coding: utf-8 -*-
"""
Web application translations.

Uses a simple key-value approach where English is the key
and translations are the values. Supports RTL languages.
"""

from contextlib import contextmanager
from typing import Iterator

from shared.genizah_translations import TRANSLATIONS

# Current language state
_current_lang = 'he'  # Default to Hebrew


def set_language(lang: str) -> None:
    """Set the current language ('he' for Hebrew, 'en' for English)."""
    global _current_lang
    _current_lang = lang


def get_language() -> str:
    """Get the current language code."""
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
    from nicegui.storage import request_contextvar

    request = request_contextvar.get()
    if request is None:
        return 'en'
    return language_from_accept_language(request.headers.get('accept-language'))


@contextmanager
def using_language(lang: str) -> Iterator[None]:
    """Render a synchronous block in ``lang``, then restore the previous language.

    For UI built after an await (a deferred fill), when another visitor's page
    render may have set the process-global language in the meantime. The block
    must not await: the restore is only safe because nothing else runs on the
    loop between the set and the restore.
    """
    global _current_lang
    previous = _current_lang
    _current_lang = lang
    try:
        yield
    finally:
        _current_lang = previous


def is_rtl() -> bool:
    """Check if current language is RTL."""
    return _current_lang == 'he'


def tr(text: str) -> str:
    """
    Translate text to current language.

    Args:
        text: English text to translate

    Returns:
        Translated text if available, otherwise original text
    """
    if _current_lang == 'en':
        return text

    return TRANSLATIONS.get(text, text)


def get_dir() -> str:
    """Get text direction for current language."""
    return 'rtl' if is_rtl() else 'ltr'
