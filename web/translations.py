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
