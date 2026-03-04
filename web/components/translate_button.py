# -*- coding: utf-8 -*-
"""
Translation Button Component

Provides a translate button that can be added to any text content.
Uses MyMemory free translation API (no API key required).
Supports Hebrew <-> English translation.
"""

import logging
import requests
from nicegui import ui
from web.translations import tr, get_language
from typing import Optional, Callable
import re

logger = logging.getLogger(__name__)


def detect_language(text: str) -> str:
    """
    Simple language detection based on character presence.
    Returns 'he' for Hebrew, 'en' for English.
    """
    if not text:
        return 'en'

    # Count Hebrew characters
    hebrew_chars = len(re.findall(r'[\u0590-\u05FF]', text))
    total_letters = len(re.findall(r'[a-zA-Z\u0590-\u05FF]', text))

    if total_letters == 0:
        return 'en'

    # If more than 30% Hebrew characters, consider it Hebrew
    if hebrew_chars / total_letters > 0.3:
        return 'he'
    return 'en'


def translate_text(text: str, source_lang: str, target_lang: str) -> Optional[str]:
    """
    Translate text using MyMemory free API.

    Args:
        text: Text to translate
        source_lang: Source language code ('he' or 'en')
        target_lang: Target language code ('he' or 'en')

    Returns:
        Translated text or None if translation failed
    """
    if not text or not text.strip():
        return None

    # MyMemory uses ISO language codes
    lang_map = {'he': 'he', 'en': 'en'}
    src = lang_map.get(source_lang, 'en')
    tgt = lang_map.get(target_lang, 'he')

    try:
        response = requests.get(
            'https://api.mymemory.translated.net/get',
            params={
                'q': text[:500],  # API limit
                'langpair': f'{src}|{tgt}'
            },
            timeout=10
        )

        if response.status_code == 200:
            data = response.json()
            if data.get('responseStatus') == 200:
                translated = data.get('responseData', {}).get('translatedText')
                if translated:
                    return translated
        return None
    except Exception as e:
        logger.error("Translation error: %s", e)
        return None


def create_translate_button(
    text_content: str,
    container_element,
    text_style: str = '',
    on_translate: Optional[Callable[[str], None]] = None
):
    """
    Create a translate button for a piece of text.

    Args:
        text_content: The original text content
        container_element: The UI element that contains/displays the text
        text_style: CSS style for the translated text display
        on_translate: Optional callback when translation is complete

    Returns:
        The button element
    """
    # Track state
    state = {
        'is_translated': False,
        'original_text': text_content,
        'translated_text': None,
        'is_loading': False
    }

    def toggle_translation():
        if state['is_loading']:
            return

        if state['is_translated']:
            # Show original
            state['is_translated'] = False
            btn.props('icon=translate')
            btn.tooltip(tr('Translate'))
            if on_translate:
                on_translate(state['original_text'])
        else:
            # Translate
            if state['translated_text']:
                # Use cached translation
                state['is_translated'] = True
                btn.props('icon=undo')
                btn.tooltip(tr('Show original'))
                if on_translate:
                    on_translate(state['translated_text'])
            else:
                # Fetch translation
                state['is_loading'] = True
                btn.props('loading')

                # Detect source language and determine target
                src_lang = detect_language(state['original_text'])
                tgt_lang = 'en' if src_lang == 'he' else 'he'

                translated = translate_text(state['original_text'], src_lang, tgt_lang)

                state['is_loading'] = False
                btn.props(remove='loading')

                if translated:
                    state['translated_text'] = translated
                    state['is_translated'] = True
                    btn.props('icon=undo')
                    btn.tooltip(tr('Show original'))
                    if on_translate:
                        on_translate(translated)
                else:
                    ui.notify(tr('Translation failed'), type='warning')

    btn = ui.button(
        icon='translate',
        on_click=toggle_translation
    ).props('flat round dense size=xs').tooltip(tr('Translate'))

    return btn


def create_translatable_text(
    text_content: str,
    container_classes: str = '',
    container_style: str = ''
):
    """
    Create a text element with an inline translate button.

    Args:
        text_content: The text to display
        container_classes: CSS classes for the container
        container_style: Inline CSS style for the container

    Returns:
        Tuple of (container element, text label, translate button)
    """
    if not text_content:
        return None, None, None

    # State for this instance
    state = {
        'is_translated': False,
        'original_text': text_content,
        'translated_text': None,
        'is_loading': False
    }

    container = ui.row().classes(f'w-full items-start gap-1 {container_classes}')

    with container:
        # Text content
        text_label = ui.label(text_content).classes('flex-1 text-sm whitespace-pre-wrap').style(container_style)

        # Translate button
        def toggle_translation():
            if state['is_loading']:
                return

            if state['is_translated']:
                # Show original
                state['is_translated'] = False
                text_label.text = state['original_text']
                btn.props('icon=translate')
                btn.tooltip(tr('Translate'))
            else:
                if state['translated_text']:
                    # Use cached translation
                    state['is_translated'] = True
                    text_label.text = state['translated_text']
                    btn.props('icon=undo')
                    btn.tooltip(tr('Show original'))
                else:
                    # Fetch translation
                    state['is_loading'] = True
                    btn.props('loading')

                    src_lang = detect_language(state['original_text'])
                    tgt_lang = 'en' if src_lang == 'he' else 'he'

                    translated = translate_text(state['original_text'], src_lang, tgt_lang)

                    state['is_loading'] = False
                    btn.props(remove='loading')

                    if translated:
                        state['translated_text'] = translated
                        state['is_translated'] = True
                        text_label.text = translated
                        btn.props('icon=undo')
                        btn.tooltip(tr('Show original'))
                    else:
                        ui.notify(tr('Translation failed'), type='warning')

        btn = ui.button(
            icon='translate',
            on_click=toggle_translation
        ).props('flat round dense size=xs').tooltip(tr('Translate')).classes('self-start mt-1')

    return container, text_label, btn
