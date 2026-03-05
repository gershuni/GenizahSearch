# -*- coding: utf-8 -*-
"""
Dicta Translation API client for GenizahSearch.

Wraps the Dicta LM 2.0 REST Completions API with few-shot prompt construction
for scholarly Genizah manuscript metadata translation (EN<->HE).

All translation is batch/offline -- this module is used by batch scripts,
never at runtime during user searches.

API Details:
- Endpoint: POST /whatcanthisbe/completions
- Model: dicta-il/dictalm2.0 (Mistral-7B derivative, 190B+ Hebrew/English tokens)
- Auth: Bearer x-no-api-key (no authentication required)
- Stop: double newline for multi-sentence, temperature 0 for deterministic output
"""

import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

logger = logging.getLogger(__name__)

# =============================================================================
# API Constants
# =============================================================================

DICTA_BASE = "https://dicta-translation.loadbalancer3.dicta.org.il"
DICTA_ENDPOINT = f"{DICTA_BASE}/whatcanthisbe/completions"
DICTA_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": "Bearer x-no-api-key",
}
DICTA_MODEL = "dicta-il/dictalm2.0"

# Retry settings for 429 rate limiting
MAX_RETRIES_429 = 3
RETRY_BASE_DELAY = 3.0  # seconds
RETRY_MAX_DELAY = 30.0  # seconds

# =============================================================================
# PGP Document Type Manual Translations
# =============================================================================

# Only 9 distinct values -- manual translation is more reliable than API for
# a small fixed taxonomy. Scholarly precision required.
PGP_DOCUMENT_TYPE_HE: Dict[str, str] = {
    "Letter": "\u05de\u05db\u05ea\u05d1",
    "Legal document": "\u05de\u05e1\u05de\u05da \u05de\u05e9\u05e4\u05d8\u05d9",
    "List or table": "\u05e8\u05e9\u05d9\u05de\u05d4 \u05d0\u05d5 \u05d8\u05d1\u05dc\u05d4",
    "Literary text": "\u05d8\u05e7\u05e1\u05d8 \u05e1\u05e4\u05e8\u05d5\u05ea\u05d9",
    "State document": "\u05de\u05e1\u05de\u05da \u05de\u05d3\u05d9\u05e0\u05d4",
    "Paraliterary text": "\u05d8\u05e7\u05e1\u05d8 \u05e4\u05e8\u05d4-\u05e1\u05e4\u05e8\u05d5\u05ea\u05d9",
    "Credit instrument or private receipt": "\u05e9\u05d8\u05e8 \u05d0\u05e9\u05e8\u05d0\u05d9 \u05d0\u05d5 \u05e7\u05d1\u05dc\u05d4 \u05e4\u05e8\u05d8\u05d9\u05ea",
    "Legal query or responsum": "\u05e9\u05d0\u05dc\u05d4 \u05de\u05e9\u05e4\u05d8\u05d9\u05ea \u05d0\u05d5 \u05ea\u05e9\u05d5\u05d1\u05d4",
    "Inscription": "\u05db\u05ea\u05d5\u05d1\u05ea",
}


# =============================================================================
# Few-Shot Template Functions
# =============================================================================


def load_few_shot_template(path: str) -> dict:
    """Load a few-shot template from a JSON file.

    Args:
        path: Path to the JSON template file.

    Returns:
        Dict with keys: prompts (list of example pairs),
        en_category (str), he_category (str).
    """
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_few_shot_prompt(template: dict, direction: str = "en2he") -> str:
    """Build the few-shot prefix from a template.

    Constructs alternating category:text pairs from the template examples.
    For en2he: "English: {en}\\nHebrew: {he}" pairs separated by "\\n\\n".
    For he2en: "Hebrew: {he}\\nEnglish: {en}" pairs separated by "\\n\\n".

    Args:
        template: Dict loaded from a few-shot JSON file.
        direction: "en2he" or "he2en".

    Returns:
        Multi-line string of category:text pairs separated by double newlines.
    """
    pairs = []
    en_cat = template["en_category"]
    he_cat = template["he_category"]

    for p in template["prompts"]:
        en_text = p["English"].strip()
        he_text = p["Hebrew"].strip()

        if direction == "en2he":
            pairs.append(f"{en_cat}: {en_text}\n{he_cat}: {he_text}")
        else:
            pairs.append(f"{he_cat}: {he_text}\n{en_cat}: {en_text}")

    return "\n\n".join(pairs)


# =============================================================================
# Translation Functions
# =============================================================================


def translate_text(
    text: str,
    few_shot_prompt: str,
    direction: str = "en2he",
    timeout: int = 60,
) -> Optional[str]:
    """Translate text using Dicta LM 2.0 Completions API.

    Builds full prompt from few-shot prefix + source text + target category,
    then POSTs to the Dicta endpoint. Retries with exponential backoff on
    429 rate limit responses.

    Args:
        text: Text to translate.
        few_shot_prompt: Pre-built few-shot prefix (from build_few_shot_prompt).
        direction: "en2he" or "he2en".
        timeout: HTTP request timeout in seconds.

    Returns:
        Translated text string, stripped of whitespace. None on error.
    """
    if direction == "en2he":
        src_cat, tgt_cat = "English", "Hebrew"
    else:
        src_cat, tgt_cat = "Hebrew", "English"

    prompt = f"{few_shot_prompt}\n\n{src_cat}: {text.strip()}\n{tgt_cat}:"

    payload = {
        "model": DICTA_MODEL,
        "prompt": prompt,
        "temperature": 0,
        "stop": ["\n\n"],
        "max_tokens": 1024,
    }

    for attempt in range(MAX_RETRIES_429):
        try:
            resp = requests.post(
                DICTA_ENDPOINT, json=payload, headers=DICTA_HEADERS, timeout=timeout
            )

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    # Cap server's Retry-After — often wildly inflated
                    delay = min(float(retry_after), RETRY_MAX_DELAY)
                else:
                    delay = min(
                        RETRY_BASE_DELAY * (2 ** attempt) + random.uniform(0, 1),
                        RETRY_MAX_DELAY,
                    )
                logger.warning(
                    "429 rate limit (attempt %d/%d), retrying in %.1fs for: %.50s...",
                    attempt + 1, MAX_RETRIES_429, delay, text[:50],
                )
                time.sleep(delay)
                continue

            resp.raise_for_status()
            return resp.json()["choices"][0]["text"].strip()
        except requests.exceptions.HTTPError:
            # Non-429 HTTP errors — don't retry
            logger.warning("Translation failed for text (%.50s...): %s", text[:50], resp.status_code)
            return None
        except Exception as e:
            logger.warning("Translation failed for text (%.50s...): %s", text[:50], e)
            return None

    logger.warning("Translation failed after %d retries for: %.50s...", MAX_RETRIES_429, text[:50])
    return None


def batch_translate(
    texts: List[Tuple[Any, str]],
    few_shot_prompt: str,
    direction: str = "en2he",
    max_workers: int = 5,
    on_progress: Optional[Callable] = None,
) -> List[Tuple[Any, str]]:
    """Translate multiple texts in parallel using ThreadPoolExecutor.

    Args:
        texts: List of (id, text) tuples to translate.
        few_shot_prompt: Pre-built few-shot prefix.
        direction: "en2he" or "he2en".
        max_workers: Maximum concurrent API requests.
        on_progress: Optional callback(completed, total) for progress reporting.

    Returns:
        List of (id, translated_text) tuples for successful translations.
        Failed translations are logged and skipped.
    """
    results = []
    total = len(texts)

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {}
        for item_id, text in texts:
            f = pool.submit(translate_text, text, few_shot_prompt, direction)
            futures[f] = item_id

        completed = 0
        for f in as_completed(futures):
            item_id = futures[f]
            completed += 1
            try:
                translation = f.result()
                if translation is not None:
                    results.append((item_id, translation))
                else:
                    logger.warning("Translation returned None for id=%s", item_id)
            except Exception as e:
                logger.error("Translation failed for id=%s: %s", item_id, e)

            if on_progress:
                on_progress(completed, total)

    return results
