# -*- coding: utf-8 -*-
"""
Translation Button Component

Provides a translate button that can be added to any text content.
Uses the Dicta Translation API (via shared.dicta_client) for scholarly-quality
Hebrew <-> English translation. Replaces the previous MyMemory API.

For community content (corrections, comments, notes) -- on-demand translation
powered by Dicta LM 2.0 with scholarly few-shot prompts.
"""

import logging
import os
from nicegui import ui
from web.translations import tr, get_language
from typing import Optional, Callable
import re

logger = logging.getLogger(__name__)

# =============================================================================
# Lazy-loaded few-shot prompts (singleton, loaded once on first use)
# =============================================================================

_few_shot_cache = {}


def _get_few_shot_prompt(direction: str) -> str:
    """Get the cached few-shot prompt for the given direction.

    Args:
        direction: 'en2he' or 'he2en'.

    Returns:
        Pre-built few-shot prompt string. Empty string if templates not found.
    """
    if direction in _few_shot_cache:
        return _few_shot_cache[direction]

    try:
        from shared.dicta_client import load_few_shot_template, build_few_shot_prompt

        # Find template files relative to project root
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        if direction == 'en2he':
            template_path = os.path.join(project_root, 'data', 'few_shot_en2he_scholarly.json')
        else:
            template_path = os.path.join(project_root, 'data', 'few_shot_he2en_scholarly.json')

        if os.path.isfile(template_path):
            template = load_few_shot_template(template_path)
            prompt = build_few_shot_prompt(template, direction=direction)
            _few_shot_cache[direction] = prompt
            return prompt
        else:
            logger.warning("Few-shot template not found: %s", template_path)
            _few_shot_cache[direction] = ''
            return ''
    except Exception as e:
        logger.warning("Failed to load few-shot template for %s: %s", direction, e)
        _few_shot_cache[direction] = ''
        return ''


# =============================================================================
# Language Detection
# =============================================================================


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


# =============================================================================
# Translation Function (Dicta API)
# =============================================================================


def translate_text(text: str, source_lang: str, target_lang: str) -> Optional[str]:
    """
    Translate text using the Dicta Translation API.

    Uses scholarly few-shot prompts for domain-appropriate translation
    of Genizah-related content. Replaces the previous MyMemory API.

    Args:
        text: Text to translate
        source_lang: Source language code ('he' or 'en')
        target_lang: Target language code ('he' or 'en')

    Returns:
        Translated text or None if translation failed

    Note:
        Text longer than 2000 characters is truncated before translation
        to keep on-demand UX responsive.
    """
    if not text or not text.strip():
        return None

    # Determine direction
    if source_lang == 'he':
        direction = 'he2en'
    else:
        direction = 'en2he'

    # Truncate very long text for on-demand UX (Dicta handles longer, but keep responsive)
    translate_text_input = text.strip()
    if len(translate_text_input) > 2000:
        translate_text_input = translate_text_input[:2000] + '...'

    try:
        from shared.dicta_client import translate_text as dicta_translate

        few_shot_prompt = _get_few_shot_prompt(direction)
        result = dicta_translate(translate_text_input, few_shot_prompt, direction=direction)
        return result
    except Exception as e:
        logger.error("Dicta translation error: %s", e)
        return None


# =============================================================================
# UI Components
# =============================================================================


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

    async def toggle_translation():
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
                # Fetch translation via Dicta API (async to avoid blocking)
                state['is_loading'] = True
                btn.props('loading')

                # Detect source language and determine target
                src_lang = detect_language(state['original_text'])
                tgt_lang = 'en' if src_lang == 'he' else 'he'

                from nicegui import run
                translated = await run.io_bound(
                    translate_text, state['original_text'], src_lang, tgt_lang
                )

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
        async def toggle_translation():
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
                    # Fetch translation via Dicta API (async to avoid blocking)
                    state['is_loading'] = True
                    btn.props('loading')

                    src_lang = detect_language(state['original_text'])
                    tgt_lang = 'en' if src_lang == 'he' else 'he'

                    from nicegui import run
                    translated = await run.io_bound(
                        translate_text, state['original_text'], src_lang, tgt_lang
                    )

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
