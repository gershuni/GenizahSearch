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
import os
import random
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Dict, List, Optional, Tuple

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)

# =============================================================================
# API Constants
# =============================================================================

DICTA_BASE = "https://dicta-translation.loadbalancer3.dicta.org.il"
DICTA_ENDPOINT = f"{DICTA_BASE}/whatcanthisbe/completions"
DICTA_MODEL = "dicta-il/dictalm2.0"

# God mode: bypasses rate limits. Set DICTA_GOD_MODE in .env
_GOD_MODE_KEY = os.environ.get("DICTA_GOD_MODE", "")
GOD_MODE = bool(_GOD_MODE_KEY)

DICTA_HEADERS: Dict[str, str] = {
    "Content-Type": "application/json",
    "Authorization": "Bearer x-no-api-key",
}
if GOD_MODE:
    DICTA_HEADERS["x-god-mode"] = _GOD_MODE_KEY

# Retry settings for 429 rate limiting
MAX_RETRIES_429 = 5
RETRY_BASE_DELAY = 10.0 if not GOD_MODE else 2.0  # seconds
RETRY_MAX_DELAY = 120.0 if not GOD_MODE else 15.0

# Word limit per API request (god mode constraint: max 100 words)
MAX_WORDS_PER_REQUEST = 100

# Max concurrent workers (god mode constraint: max 5)
MAX_WORKERS = 5

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


def _sanitize_text(text: str) -> str:
    """Remove line breaks and collapse whitespace for API submission."""
    return re.sub(r"\s+", " ", text.replace("\n", " ").replace("\r", " ")).strip()


def _split_by_words(text: str, max_words: int = MAX_WORDS_PER_REQUEST) -> List[str]:
    """Split text into chunks of at most max_words words.

    Splits on sentence boundaries (. ! ? ;) when possible, falling back to
    word-boundary splits.

    Returns:
        List of text chunks, each with at most max_words words.
    """
    words = text.split()
    if len(words) <= max_words:
        return [text]

    # Split on sentence boundaries first
    sentences = re.split(r"(?<=[.!?;])\s+", text)
    chunks: List[str] = []
    current: List[str] = []
    current_wc = 0

    for sentence in sentences:
        s_words = sentence.split()
        s_wc = len(s_words)

        if s_wc > max_words:
            # Sentence itself is too long — flush current, then split sentence by word count
            if current:
                chunks.append(" ".join(current))
                current = []
                current_wc = 0
            for i in range(0, s_wc, max_words):
                chunks.append(" ".join(s_words[i : i + max_words]))
        elif current_wc + s_wc > max_words:
            # Adding this sentence would exceed limit — flush
            chunks.append(" ".join(current))
            current = [sentence]
            current_wc = s_wc
        else:
            current.append(sentence)
            current_wc += s_wc

    if current:
        chunks.append(" ".join(current))

    return chunks


def _translate_single(
    text: str,
    few_shot_prompt: str,
    src_cat: str,
    tgt_cat: str,
    timeout: int = 60,
) -> Optional[str]:
    """Send a single translation request to the Dicta API.

    Retries with exponential backoff on 429 rate limit responses.

    Returns:
        Translated text string, or None on error.
    """
    prompt = f"{few_shot_prompt}\n\n{src_cat}: {text}\n{tgt_cat}:"

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
                reset = resp.headers.get("RateLimit-Reset")
                retry_after = resp.headers.get("Retry-After")
                if reset:
                    delay = min(float(reset) + 2, RETRY_MAX_DELAY)
                elif retry_after:
                    delay = min(float(retry_after) + 2, RETRY_MAX_DELAY)
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
            logger.warning("Translation failed for text (%.50s...): %s", text[:50], resp.status_code)
            return None
        except Exception as e:
            logger.warning("Translation failed for text (%.50s...): %s", text[:50], e)
            return None

    logger.warning("Translation failed after %d retries for: %.50s...", MAX_RETRIES_429, text[:50])
    return None


def translate_text(
    text: str,
    few_shot_prompt: str,
    direction: str = "en2he",
    timeout: int = 60,
) -> Optional[str]:
    """Translate text using Dicta LM 2.0 Completions API.

    Sanitizes input (strips line breaks), splits long texts (>100 words) into
    chunks, translates each, and joins results.

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

    clean = _sanitize_text(text)
    if not clean:
        return None

    chunks = _split_by_words(clean)
    if len(chunks) == 1:
        return _translate_single(clean, few_shot_prompt, src_cat, tgt_cat, timeout)

    # Translate each chunk and join
    translated_parts = []
    for chunk in chunks:
        result = _translate_single(chunk, few_shot_prompt, src_cat, tgt_cat, timeout)
        if result is None:
            return None  # Fail entire text if any chunk fails
        translated_parts.append(result)

    return " ".join(translated_parts)


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
    max_workers = min(max_workers, MAX_WORKERS)

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
