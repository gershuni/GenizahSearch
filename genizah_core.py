"""Core search, indexing, metadata, and AI helpers for the Genizah project."""

# -*- coding: utf-8 -*-
# genizah_core.py
import logging
import os
import sys
import re
import difflib
import shutil
import pickle
import requests
import threading
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from logging.handlers import RotatingFileHandler
from typing import Mapping
import itertools
import json
import bisect
import math

from genizah_translations import TRANSLATIONS

try:
    import google.generativeai as genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False
    
try:
    import tantivy
    from tantivy import Filter, Query, TextAnalyzerBuilder, Tokenizer, Occur
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")

# ==============================================================================
#  LAB SETTINGS
# ==============================================================================
class LabSettings:
    """Manages configuration for the Lab Mode."""
    def __init__(self):
        self.custom_variants = {} # dict mapping char/string -> set of replacements
        self.candidate_limit = 2000
        self.ngram_size = 3
        self.min_should_match = 60
        self.gap_penalty = 0.15
        self.ignore_matres = False
        self.phonetic_expansion = False

        self.load()

    def load(self):
        if os.path.exists(Config.LAB_CONFIG_FILE):
            try:
                with open(Config.LAB_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.custom_variants = data.get('custom_variants', {})
                    self.candidate_limit = data.get('candidate_limit', 2000)
                    self.ngram_size = 3  # enforced trigram for robustness
                    legacy_min = data.get('ngram_min_match')
                    self.min_should_match = data.get('min_should_match', legacy_min if legacy_min is not None else 60)
                    self.gap_penalty = float(data.get('gap_penalty', 0.15))
                    self.ignore_matres = data.get('ignore_matres', False)
                    self.phonetic_expansion = data.get('phonetic_expansion', False)
                    self.candidate_limit = max(500, min(self.candidate_limit, 50000))
                    self.ngram_size = 3
                    self.min_should_match = max(1, min(int(self.min_should_match), 100))
                    self.gap_penalty = max(0.0, min(self.gap_penalty, 1.0))
            except Exception as e:
                LOGGER.warning("Failed to load Lab config: %s", e)

    def save(self):
        os.makedirs(Config.LAB_DIR, exist_ok=True)
        try:
            with open(Config.LAB_CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'custom_variants': self.custom_variants,
                    'candidate_limit': max(500, min(self.candidate_limit, 50000)),
                    'ngram_size': self.ngram_size,
                    'min_should_match': self.min_should_match,
                    'gap_penalty': self.gap_penalty,
                    'ignore_matres': self.ignore_matres,
                    'phonetic_expansion': self.phonetic_expansion
                }, f, indent=4)
        except Exception as e:
            LOGGER.error("Failed to save Lab config: %s", e)

    def parse_variants_text(self, text):
        """Parse text input (e.g. 'a=b, c=d') into custom_variants dict."""
        new_vars = {}
        # Handle newlines or commas
        lines = text.replace(',', '\n').splitlines()
        for line in lines:
            if '=' in line:
                parts = line.split('=')
                if len(parts) == 2:
                    k, v = parts[0].strip(), parts[1].strip()
                    if k and v:
                        if k not in new_vars: new_vars[k] = []
                        new_vars[k].append(v)
                        # Also add reverse? Usually variants are symmetric but maybe not always.
                        # For now, let's assume symmetric for char swaps, maybe not for multi-char.
                        # User request: "also for two to one words such as נו=מ".
                        if v not in new_vars: new_vars[v] = []
                        new_vars[v].append(k)
        self.custom_variants = new_vars

    def get_variants_text(self):
        """Convert custom_variants dict back to text format."""
        pairs = set()
        for k, vals in self.custom_variants.items():
            for v in vals:
                # Store sorted tuple to avoid duplicates like a=b and b=a
                if k < v:
                    pairs.add(f"{k}={v}")
                elif v < k:
                    pairs.add(f"{v}={k}")
        return "\n".join(sorted(list(pairs)))

# ==============================================================================
#  CONFIG CLASS (EXE Compatible)
# ==============================================================================
class Config:
    """Static paths and limits used by the application and by bundled binaries."""

    @staticmethod
    def _pick_writable_dir(primary: str, fallback: str) -> str:
        """
        Prefer primary; if we cannot create/write there, use fallback.
        Returns a directory path that is guaranteed (best-effort) to exist and be writable.
        """
        # Try primary
        try:
            os.makedirs(primary, exist_ok=True)
            test_path = os.path.join(primary, ".__write_test__")
            with open(test_path, "w", encoding="utf-8") as f:
                f.write("ok")
            os.remove(test_path)
            return primary
        except Exception:
            pass

        # Fallback
        os.makedirs(fallback, exist_ok=True)
        return fallback

    # 1. Determine Base Paths
    if getattr(sys, "frozen", False):
        BASE_DIR = os.path.dirname(sys.executable)
        _cand = os.path.join(BASE_DIR, "_internal")
        INTERNAL_DIR = _cand if os.path.isdir(_cand) else getattr(sys, "_MEIPASS", BASE_DIR)
    else:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        INTERNAL_DIR = BASE_DIR

    # 2. External Files (Must be placed NEXT to the EXE by the user)
    FILE_V8 = os.path.join(BASE_DIR, "Transcriptions.txt")
    FILE_V7 = os.path.join(BASE_DIR, "AllGenizah_OLD.txt")
    INPUT_FILE = os.path.join(BASE_DIR, "input.txt")

    # 3. User Data Directory (Index, Caches) - Smart Logic
    _PORTABLE_INDEX_PATH = os.path.join(BASE_DIR, "Genizah_Index")
    _APPDATA_PATH = os.path.join(
        os.getenv("LOCALAPPDATA", os.path.expanduser("~")),
        "GenizahSearchPro",
        "Index",
    )
    _LEGACY_PATH = os.path.join(os.path.expanduser("~"), "Genizah_Tantivy_Index")

    if os.path.exists(_PORTABLE_INDEX_PATH):
        INDEX_DIR = _PORTABLE_INDEX_PATH
    elif os.path.exists(_LEGACY_PATH) and not os.path.exists(_APPDATA_PATH):
        INDEX_DIR = _LEGACY_PATH
    else:
        INDEX_DIR = _APPDATA_PATH

    # Ensure the directory is created
    try:
        os.makedirs(INDEX_DIR, exist_ok=True)
    except Exception:
        INDEX_DIR = _PORTABLE_INDEX_PATH
        os.makedirs(INDEX_DIR, exist_ok=True)

    # 4. Output folders: try BASE_DIR first; fallback to INDEX_DIR
    RESULTS_DIR = _pick_writable_dir(
        os.path.join(BASE_DIR, "Results"),
        os.path.join(INDEX_DIR, "Results"),
    )
    REPORTS_DIR = _pick_writable_dir(
        os.path.join(BASE_DIR, "Reports"),
        os.path.join(INDEX_DIR, "Reports"),
    )

    IMAGE_CACHE_DIR = os.path.join(INDEX_DIR, "images_cache")

    # 5. Generated Files (Logs, Configs, Caches - inside Index Dir)
    CACHE_META = os.path.join(INDEX_DIR, "metadata_cache.pkl")
    CACHE_NLI = os.path.join(INDEX_DIR, "nli_cache.pkl")
    CONFIG_FILE = os.path.join(INDEX_DIR, "config.pkl")
    LANGUAGE_FILE = os.path.join(INDEX_DIR, "lang.pkl")
    BROWSE_MAP = os.path.join(INDEX_DIR, "browse_map.pkl")
    FL_MAP = os.path.join(INDEX_DIR, "fl_lookup.pkl")
    LOG_FILE = os.path.join(INDEX_DIR, "genizah.log")

    # Lab Mode Paths
    LAB_DIR = os.path.join(INDEX_DIR, "lab")
    LAB_INDEX_DIR = os.path.join(INDEX_DIR, "lab_index")
    LAB_CONFIG_FILE = os.path.join(LAB_DIR, "lab_config.json")
    LAB_LOG_FILE = os.path.join(LAB_DIR, "lab_genizah.log")

    # 6. Bundled Internal Resources (Packaged inside the EXE/_internal)
    LIBRARIES_CSV = os.path.join(INTERNAL_DIR, "libraries.csv")
    HELP_FILE = os.path.join(INTERNAL_DIR, "Help.html")

    # Settings
    TANTIVY_CLAUSE_LIMIT = 5000
    SEARCH_LIMIT = 5000
    VARIANT_GEN_LIMIT = 5000
    REGEX_VARIANTS_LIMIT = 3000
    WORD_TOKEN_PATTERN = r"[\w\u0590-\u05FF\']+"
    
    @staticmethod
    def resource_path(relative_path: str) -> str:
        """Return absolute path to bundled resources."""
        return os.path.join(Config.INTERNAL_DIR, relative_path)

# ==============================================================================
#  LOGGING
# ==============================================================================


def configure_logger():
    """Configure a rotating file logger for the app (quiet for users, verbose for devs)."""
    logger = logging.getLogger("genizah")
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    os.makedirs(Config.INDEX_DIR, exist_ok=True)

    file_handler = RotatingFileHandler(Config.LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s - %(message)s"))
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
    console.setLevel(logging.INFO)
    logger.addHandler(console)

    logger.propagate = False
    return logger


def get_logger(name=None):
    base_logger = configure_logger()
    return base_logger.getChild(name) if name else base_logger


LOGGER = get_logger(__name__)


def configure_lab_logger():
    """Configure a separate logger for Lab Mode operations."""
    lab_logger = logging.getLogger("genizah_lab")
    if lab_logger.handlers:
        return lab_logger

    lab_logger.setLevel(logging.DEBUG)

    # Ensure lab directory exists
    os.makedirs(Config.LAB_DIR, exist_ok=True)

    file_handler = RotatingFileHandler(Config.LAB_LOG_FILE, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] - %(message)s"))
    file_handler.setLevel(logging.DEBUG)
    lab_logger.addHandler(file_handler)

    # Optional: Log to console as well if debugging
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("[LAB] %(levelname)s: %(message)s"))
    console.setLevel(logging.INFO)
    lab_logger.addHandler(console)

    lab_logger.propagate = False
    return lab_logger

LAB_LOGGER = configure_lab_logger()

SERVICE_ENDPOINTS = {
    'network': 'https://www.google.com/generate_204',
    'nli': 'https://iiif.nli.org.il/IIIFv21/',
}

AI_PROVIDER_ENDPOINTS = {
    "Google Gemini": "https://generativelanguage.googleapis.com",
    "OpenAI": "https://api.openai.com/v1/models",
    "Anthropic Claude": "https://api.anthropic.com/v1/models",
}

# Paths resolved through PyInstaller-friendly helper
Config.HELP_FILE = Config.resource_path("Help.html")

def load_language():
    """Load language preference. Returns 'en' or 'he'."""
    try:
        if os.path.exists(Config.LANGUAGE_FILE):
            with open(Config.LANGUAGE_FILE, 'rb') as f:
                return pickle.load(f)
    except Exception as e:
        LOGGER.warning("Failed to load language preference from %s: %s", Config.LANGUAGE_FILE, e)
    return 'en'

def save_language(lang):
    """Save language preference."""
    try:
        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.LANGUAGE_FILE, 'wb') as f:
            pickle.dump(lang, f)
    except Exception as e:
        LOGGER.error("Failed to save language preference to %s: %s", Config.LANGUAGE_FILE, e)

# Global language state
CURRENT_LANG = load_language()

def tr(text):
    """Translate text if current language is Hebrew."""
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text

try:
    import tantivy
except ImportError:
    raise ImportError(tr("Tantivy library missing. Please install it."))

def check_external_services(extra_endpoints=None, timeout=3):
    """Check whether core external services respond within a short timeout."""
    endpoints = dict(SERVICE_ENDPOINTS)
    if extra_endpoints:
        endpoints.update(extra_endpoints)

    results = {}
    for name, url in endpoints.items():
        detail = {"reachable": False, "status_code": None, "note": None}
        try:
            if name == "network":
                resp = requests.get(url, timeout=timeout, allow_redirects=True, stream=True)
            else:
                resp = requests.head(url, timeout=timeout, allow_redirects=True)
            detail["status_code"] = resp.status_code
            detail["reachable"] = resp.status_code < 500
            if resp.status_code in (401, 403):
                detail["note"] = "reachable but unauthorized"
            if name == "network":
                resp.close()
        except Exception as e:
            LOGGER.warning("Health check failed for %s at %s: %s", name, url, e)
            detail["note"] = str(e)
            detail["reachable"] = False
        results[name] = detail
    return results

# ==============================================================================
#  AI MANAGER
# ==============================================================================
class AIManager:
    """Manage AI configuration (Provider, Model, Key) and prompt sessions."""
    def __init__(self):
        self.provider = "Google Gemini"
        self.model_name = "gemini-1.5-flash"
        self.api_key = ""
        self.chat = None

        # Ensure dir exists
        if not os.path.exists(Config.INDEX_DIR):
            try:
                os.makedirs(Config.INDEX_DIR)
            except Exception as e:
                LOGGER.error("Failed to create index directory for AI config at %s: %s", Config.INDEX_DIR, e)

        if os.path.exists(Config.CONFIG_FILE):
            try:
                with open(Config.CONFIG_FILE, 'rb') as f:
                    cfg = pickle.load(f)
                    # Support legacy key
                    if 'gemini_key' in cfg and 'api_key' not in cfg:
                        self.api_key = cfg.get('gemini_key', '')
                    else:
                        self.api_key = cfg.get('api_key', '')
                        self.provider = cfg.get('provider', 'Google Gemini')
                        self.model_name = cfg.get('model_name', 'gemini-1.5-flash')
            except Exception as e:
                LOGGER.warning("Failed to load AI configuration from %s: %s", Config.CONFIG_FILE, e)

    def get_healthcheck_endpoint(self):
        """Return the connectivity probe endpoint for the configured provider."""
        return AI_PROVIDER_ENDPOINTS.get(self.provider)

    def save_config(self, provider, model_name, key):
        self.provider = provider
        self.model_name = model_name
        self.api_key = key.strip()

        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.CONFIG_FILE, 'wb') as f:
            pickle.dump({
                'provider': self.provider,
                'model_name': self.model_name,
                'api_key': self.api_key
            }, f)
        # Reset session
        self.chat = None

    def _get_sys_inst(self):
        base_inst = """You are an expert in Regex for Hebrew manuscripts (Cairo Genizah).
            Your goal is to help the user construct Python Regex patterns.
            
            IMPORTANT RULES:
            1. Do NOT use \\w. Instead, use [\\u0590-\\u05FF"] to match Hebrew letters and Geresh.
            2. For "word starting with X", use \\bX...
            3. For spaces, use \\s+.
            4. Output format MUST be strictly JSON: {"regex": "THE_PATTERN", "explanation": "Brief explanation"}.
            5. Do not include markdown formatting like ```json.
            """

        if CURRENT_LANG == 'he':
            base_inst += "\n\nIMPORTANT: Provide the 'explanation' field in Hebrew."

        return base_inst

    def init_session(self):
        if not self.api_key: return "Error: Missing API Key."

        if self.provider == "Google Gemini":
            if not HAS_GENAI: return "Error: 'google-generativeai' library missing."
            try:
                genai.configure(api_key=self.api_key)
                model = genai.GenerativeModel(self.model_name)

                self.chat = model.start_chat(history=[
                    {"role": "user", "parts": [self._get_sys_inst()]},
                    {"role": "model", "parts": ["Understood. I will provide JSON output with robust Hebrew regex."]}
                ])
                return None
            except Exception as e:
                return str(e)

        return None # Other providers are stateless or handled in send_prompt

    def send_prompt(self, user_text):
        if self.provider == "Google Gemini" and not self.chat:
            err = self.init_session()
            if err: return None, err
            
        try:
            response_text = ""

            if self.provider == "Google Gemini":
                response = self.chat.send_message(user_text)
                response_text = response.text

            elif self.provider == "OpenAI":
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {self.api_key}"
                }
                payload = {
                    "model": self.model_name,
                    "messages": [
                        {"role": "system", "content": self._get_sys_inst()},
                        {"role": "user", "content": user_text}
                    ],
                    "response_format": { "type": "json_object" }
                }
                r = requests.post("https://api.openai.com/v1/chat/completions", headers=headers, json=payload, timeout=20)
                if r.status_code != 200:
                    return None, f"OpenAI Error {r.status_code}: {r.text}"
                res_json = r.json()
                response_text = res_json['choices'][0]['message']['content']

            elif self.provider == "Anthropic Claude":
                headers = {
                    "x-api-key": self.api_key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                }
                payload = {
                    "model": self.model_name,
                    "max_tokens": 1024,
                    "messages": [
                        {"role": "user", "content": self._get_sys_inst() + "\n\n" + user_text}
                    ]
                }
                r = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=payload, timeout=20)
                if r.status_code != 200:
                    return None, f"Claude Error {r.status_code}: {r.text}"
                res_json = r.json()
                response_text = res_json['content'][0]['text']

            clean = response_text.strip().replace('```json', '').replace('```', '').strip()
            data = json.loads(clean)
            return data, None
        except Exception as e:
            return None, str(e)

# ==============================================================================
#  VARIANTS LOGIC
# ==============================================================================
class VariantManager:
    """Generate spelling variants for Hebrew search terms using multiple maps."""

    _BASIC_LIST = [
        ('ד', 'ר'), ('כ', 'ב'), ('ה', 'ח'),
        ('ו', 'ז'), ('ו', 'י'), ('ו', 'ן'),
        ('ט', 'ת'), ('ס', 'ש')
    ]

    @staticmethod
    def make_multimap(pairs):
        m = defaultdict(set)
        for a, b in pairs:
            m[a].add(b)
            m[b].add(a)
        return m

    def __init__(self):
        self.basic_map = self.make_multimap(self._BASIC_LIST)
        self.extended_map = self.make_multimap(self._BASIC_LIST + [
            ('ה', 'ח'), ('ת', 'ה'), ('י', 'ל'), ('א', 'ו'), ('ה', 'ר'), ('ל', 'י'), ('א', 'י'), ('ה', 'ת'),
            ('ר', 'י'), ('א', 'ח'), ('י', 'א'), ('י', 'ר'), ('ק', 'ה'), ('נ', 'ו'), ('ל', 'ו'), ('ה', 'ו'),
            ('ו', 'א'), ('ו', 'נ'), ('ו', 'ל'), ('ה', 'י'), ('א', 'ה'), ('ר', 'ו'), ('ו', 'ר'), ('ל', 'ר'),
            ('מ', 'י'), ('ה', 'א'), ('מ', 'א'), ('נ', 'י'), ('מ', 'ו'), ('י', 'ה'), ('ו', 'ה'), ('ו', 'ן'),
            ('ן', 'ו'), ('ח', 'ה'), ('א', 'ל'), ('ל', 'נ'), ('י', 'נ'), ('ת', 'י'), ('י', 'מ'), ('ת', 'ח'),
            ('ב', 'י'), ('ל', 'א'), ('ה', 'ם'), ('י', 'ב'), ('ר', 'ה'), ('ו', 'ש'), ('ל', 'כ'), ('י', 'ת'),
            ('א', 'מ'), ('ת', 'ר'), ('ב', 'ו'), ('ר', 'ל'), ('י', 'ש'), ('ב', 'ר'), ('ו', 'ז'), ('א', 'ש'),
            ('ש', 'י'), ('ס', 'ם'), ('ז', 'ו'), ('ש', 'ו'), ('ב', 'נ'), ('ח', 'א'), ('ו', 'מ'), ('מ', 'ש'),
            ('מ', 'ע'), ('ת', 'ו'), ('ר', 'א'), ('ו', 'ב'), ('מ', 'ל'), ('מ', 'ב'), ('ד', 'י'), ('נ', 'ג'),
            ('ה', 'ד')
        ])

        self.maximum_map = self.make_multimap([
            ("'", 'י'), ("'", 'ר'), ('א', 'ב'), ('א', 'ד'), ('א', 'ה'), ('א', 'ו'), ('א', 'ח'), ('א', 'י'),
            ('א', 'ל'), ('א', 'מ'), ('א', 'ם'), ('א', 'נ'), ('א', 'ע'), ('א', 'ר'), ('א', 'ש'), ('א', 'ת'),
            ('ב', 'ד'), ('ב', 'ה'), ('ב', 'ו'), ('ב', 'י'), ('ב', 'כ'), ('ב', 'ל'), ('ב', 'מ'), ('ב', 'נ'),
            ('ב', 'פ'), ('ב', 'ר'), ('ב', 'ש'), ('ב', 'ת'), ('ג', 'ו'), ('ג', 'נ'), ('ד', 'ה'), ('ד', 'ו'),
            ('ד', 'י'), ('ד', 'כ'), ('ד', 'ל'), ('ד', 'ר'), ('ה', 'ב'), ('ה', 'ד'), ('ה', 'ו'), ('ה', 'ח'),
            ('ה', 'י'), ('ה', 'ך'), ('ה', 'כ'), ('ה', 'ל'), ('ה', 'מ'), ('ה', 'ם'), ('ה', 'ק'), ('ה', 'ר'),
            ('ה', 'ש'), ('ה', 'ת'), ('ו', 'ג'), ('ו', 'ד'), ('ו', 'ה'), ('ו', 'ז'), ('ו', 'ח'), ('ו', 'י'),
            ('ו', 'כ'), ('ו', 'ל'), ('ו', 'מ'), ('ו', 'ם'), ('ו', 'נ'), ('ו', 'ע'), ('ו', 'ר'), ('ו', 'ש'),
            ('ו', 'ת'), ('ז', 'י'), ('ח', 'א'), ('ח', 'ה'), ('ח', 'י'), ('ח', 'מ'), ('ח', 'ר'), ('ח', 'ת'),
            ('ט', 'ע'), ('ט', 'ש'), ('ט', 'ת'), ('י', 'ב'), ('י', 'ד'), ('י', 'ה'), ('י', 'ו'), ('י', 'ך'),
            ('י', 'כ'), ('י', 'ל'), ('י', 'מ'), ('י', 'ם'), ('י', 'נ'), ('י', 'ן'), ('י', 'ע'), ('י', 'ר'),
            ('י', 'ש'), ('י', 'ת'), ('כ', 'ה'), ('כ', 'ו'), ('כ', 'ל'), ('כ', 'מ'), ('כ', 'נ'), ('כ', 'פ'),
            ('כ', 'ר'), ('כ', 'ת'), ('ל', 'ד'), ('ל', 'ה'), ('ל', 'ו'), ('ל', 'מ'), ('ל', 'ם'), ('ל', 'נ'),
            ('ל', 'ע'), ('ל', 'ר'), ('ל', 'ש'), ('ל', 'ת'), ('מ', 'ב'), ('מ', 'ה'), ('מ', 'ח'), ('מ', 'נ'),
            ('מ', 'ס'), ('מ', 'ע'), ('מ', 'ר'), ('מ', 'ש'), ('מ', 'ת'), ('נ', 'ג'), ('נ', 'ו'), ('נ', 'ל'),
            ('נ', 'פ'), ('נ', 'ר'), ('נ', 'ת'), ('ס', 'ם'), ('ס', 'מ'), ('ס', 'ש'), ('ע', 'ל'), ('ע', 'מ'),
            ('ע', 'נ'), ('ע', 'ש'), ('פ', 'ב'), ('פ', 'כ'), ('פ', 'נ'), ('ק', 'ה'), ('ק', 'ר'), ('ר', 'ב'),
            ('ר', 'ה'), ('ר', 'ך'), ('ר', 'ח'), ('ר', 'כ'), ('ר', 'ל'), ('ר', 'מ'), ('ר', 'נ'), ('ר', 'ק'),
            ('ר', 'ש'), ('ר', 'ת'), ('ש', 'ב'), ('ש', 'ה'), ('ש', 'ו'), ('ש', 'ט'), ('ש', 'י'), ('ש', 'ל'),
            ('ש', 'מ'), ('ש', 'ע'), ('ש', 'ר'), ('ת', 'ה'), ('ת', 'ו'), ('ת', 'ח'), ('ת', 'ט'), ('ת', 'י'),
            ('ת', 'כ'), ('ת', 'ל'), ('ת', 'מ'), ('ת', 'ם'), ('ת', 'נ')
        ])

    def hamming_distance(self, term: str, variant: str) -> int:
        if len(term) != len(variant):
            # quite arbitrary, but ensures variants of different lengths are sorted last
            return len(term) + len(variant)
        diff = sum(1 for a, b in zip(term, variant) if a != b)
        return diff

    def generate_variants(self, term: str, mapping: Mapping[str, set[str]], max_changes: int, limit: int) -> set[str]:
        indices = range(len(term))
        limit = min(limit, Config.VARIANT_GEN_LIMIT)
        result = set()
        for number_of_changes in range(max_changes):
            for positions_to_change in itertools.combinations(indices, number_of_changes + 1):
                char_options_list = []
                for i, char in enumerate(term):
                    if i in positions_to_change:
                        repls = mapping[char] - {char}
                        if not repls:
                            break
                        char_options_list.append(repls)
                    else:
                        char_options_list.append({char})
                else:
                    for p in itertools.product(*char_options_list):
                        result.add("".join(p))
                        if len(result) >= limit:
                            return result
        return result

    def get_variants(self, term: str, mode: str, limit: int = Config.VARIANT_GEN_LIMIT) -> list[str]:
        """Generate spelling variants for Hebrew search terms using multiple maps."""
        if len(term) < 2:
            return [term]

        # Priority Queues logic
        # Rank 0: Original Term
        # Rank 1: Basic Variants
        # Rank 2: Extended Variants
        # Rank 3: Maximum Variants

        candidates = {term: 0}

        layers = []
        if mode == 'variants':
            layers.append((self.basic_map, 1, 1))
        elif mode == 'variants_extended':
            layers.append((self.basic_map, 1, 1))
            layers.append((self.extended_map, 2, 2))
        elif mode == 'variants_maximum':
            layers.append((self.basic_map, 1, 1))
            layers.append((self.extended_map, 2, 2))
            layers.append((self.maximum_map, 2, 3))
        else:
            return [term]

        # Process layers
        for mapping, max_changes, rank in layers:
            layer_vars = self.generate_variants(term, mapping, max_changes, limit)
            for v in layer_vars:
                if v not in candidates:
                    candidates[v] = rank

        # Sort by Rank then Hamming Distance
        def sort_key(v):
            return (candidates[v], self.hamming_distance(term, v))

        final_list = sorted(list(candidates.keys()), key=sort_key)

        # Clamp to limit
        return final_list[:limit]

# ==============================================================================
#  METADATA MANAGER
# ==============================================================================
class MetadataManager:
    def _make_session(self):
        return requests.Session()
        
    """Handle metadata parsing, remote retrieval, and persistent caching."""
    def __init__(self):
        self.meta_map = {}
        self.nli_cache = {}
        self.csv_bank = {}
        self.nli_executor = ThreadPoolExecutor(max_workers=2)
        self.ns = {'marc': 'http://www.loc.gov/MARC21/slim'}

        # Ensure index dir exists for caches
        if not os.path.exists(Config.INDEX_DIR):
            try:
                os.makedirs(Config.INDEX_DIR)
            except Exception as e:
                LOGGER.error("Failed to create index directory for metadata at %s: %s", Config.INDEX_DIR, e)

        # Load small caches immediately
        self._load_small_caches()

    def start_background_loading(self):
        """Start loading heavy metadata resources (CSV, Maps) in background."""
        threading.Thread(target=self._load_heavy_caches_bg, daemon=True).start()
        threading.Thread(target=self._build_file_map_background, daemon=True).start()

    def _load_small_caches(self):
        if os.path.exists(Config.CACHE_NLI):
            try:
                with open(Config.CACHE_NLI, 'rb') as f: self.nli_cache = pickle.load(f)
            except Exception as e:
                LOGGER.warning("Failed to load NLI cache from %s: %s", Config.CACHE_NLI, e)
        if os.path.exists(Config.CACHE_META):
            try:
                with open(Config.CACHE_META, 'rb') as f: self.meta_map = pickle.load(f)
            except Exception as e:
                LOGGER.warning("Failed to load metadata cache from %s: %s", Config.CACHE_META, e)

    def _load_heavy_caches_bg(self):
        self._load_csv_bank()

    def _load_csv_bank(self):
        """Load the massive CSV file into memory for instant lookup."""
        if not os.path.exists(Config.LIBRARIES_CSV):
            LOGGER.warning("libraries.csv not found at %s; csv_bank will remain empty", Config.LIBRARIES_CSV)
            return

        LOGGER.info("Loading libraries.csv from %s", Config.LIBRARIES_CSV)
        
        import csv
        try:
            with open(Config.LIBRARIES_CSV, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f, delimiter=',')
                next(reader, None) # Skip header

                for row in reader:

                    if not row or len(row) < 2:
                        continue
                    # Format: system_number | call_numbers | ... | titles
                    raw_sys_id = row[0]
                    sys_id = "".join(ch for ch in str(raw_sys_id) if ch.isdigit())

                    # Call numbers can be multiple separated by '|'
                    # We take the shortest one that looks like a shelfmark, or just the first
                    raw_shelves = row[1].split('|')
                    shelf = raw_shelves[0].strip()
                    # Try to find a nice short shelfmark
                    for s in raw_shelves:
                        s = s.strip()
                        if s and len(s) < len(shelf):
                            shelf = s

                    # Title is column index 5 (0-based)
                    title = ""
                    if len(row) > 5:
                        title = row[5].strip()

                    self.csv_bank[sys_id] = {'shelfmark': shelf, 'title': title}
            LOGGER.info("Loaded %d records into csv_bank from libraries.csv", len(self.csv_bank))
        except Exception as e:
            LOGGER.error("Failed to load CSV library bank from %s: %s", Config.LIBRARIES_CSV, e)

    def get_meta_for_id(self, sys_id):
        # Normalize sys_id to digits only (handles BOM/RTL marks/stray chars)
        if sys_id is None:
            return "Unknown", ""
        sys_id = "".join(ch for ch in str(sys_id) if ch.isdigit())
        # Log only if normalization changed the identifier
        raw = str(sys_id)
        norm = "".join(ch for ch in raw if ch.isdigit())
        if raw != norm:
            LOGGER.debug("Normalized sys_id: raw=%r -> %r", raw, norm)
        sys_id = norm

        """Get shelfmark and title from ANY source (CSV > Cache > Bank)."""
        shelf = "Unknown"
        title = ""

        # 1. Check CSV (Fastest & Most reliable for basic info)
        if sys_id in self.csv_bank:
            shelf = self.csv_bank[sys_id]['shelfmark']
            title = self.csv_bank[sys_id]['title']

        # 2. Check NLI Cache (Fallback/Enrichment)
        if sys_id in self.nli_cache:
            m = self.nli_cache[sys_id]
            cached_shelf = m.get('shelfmark')
            cached_title = m.get('title')

            # If CSV missed shelfmark, try cache
            if shelf == "Unknown" or not shelf:
                if cached_shelf and cached_shelf != "Unknown":
                    shelf = cached_shelf

            # If CSV missed title, try cache (crucial fix for missing titles)
            if not title and cached_title:
                title = cached_title

        return shelf, title

    def get_shelfmark_from_header(self, full_header):
        parsed = self.parse_full_id_components(full_header)

        sys_id = parsed.get('sys_id')
        if sys_id:
            shelf, _ = self.get_meta_for_id(sys_id)
            if shelf and shelf != "Unknown":
                return shelf

        if sys_id and sys_id in self.nli_cache:
            return self.nli_cache[sys_id].get('shelfmark', '')
        return ''

    def save_caches(self):
        try:
            with open(Config.CACHE_NLI, 'wb') as f: pickle.dump(self.nli_cache, f)
        except Exception as e:
            LOGGER.error("Failed to persist NLI cache to %s: %s", Config.CACHE_NLI, e)

    def _build_file_map_background(self):
        if self.meta_map: return
        if not os.path.exists(Config.FILE_V7): return
        temp_map = {}
        try:
            with open(Config.FILE_V7, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.startswith("###"):
                        uid = self.extract_unique_id(line)
                        if "xml -" in line:
                            parts = line.split("xml -")
                            if len(parts) > 1: temp_map[uid] = parts[1].strip()
            self.meta_map = temp_map
            with open(Config.CACHE_META, 'wb') as f: pickle.dump(self.meta_map, f)
        except Exception as e:
            LOGGER.warning("Failed to build or save file map cache from %s: %s", Config.FILE_V7, e)

    def extract_unique_id(self, text):
        match = re.search(r'(IE\d+_P\d+_FL\d+)', text)
        if not match:
            sys = re.search(r'(99\d+)', text)
            return sys.group(1) if sys else "UNKNOWN"
        return match.group(1)

    def parse_header_smart(self, full_header):
        sys_match = re.search(r'(99\d{8,})', full_header)
        sys_id = sys_match.group(1) if sys_match else None
        p_num = "Unknown"
        p_match = re.search(r'_P(\d+)_', full_header)
        if p_match:
            p_num = str(int(p_match.group(1)))
        else:
            tif_match = re.search(r'[ -_](\d{3,4})\.tif', full_header, re.IGNORECASE)
            if tif_match: p_num = str(int(tif_match.group(1)))
        return sys_id, p_num
        
    def parse_full_id_components(self, full_header):
        match = re.search(r'(99\d+)_?(IE\d+)?_?(P\d+)?_?(FL\d+)?', full_header)
        result = {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}
        if match:
            result['sys_id'] = match.group(1)
            if match.group(2): result['ie_id'] = match.group(2)
            if match.group(3): result['p_num'] = str(int(match.group(3)[1:])) 
            if match.group(4): result['fl_id'] = match.group(4).replace("FL", "") 
        return result

    def fetch_nli_data(self, system_id):
        if system_id in self.nli_cache: return self.nli_cache[system_id]
        _, meta = self._fetch_single_worker(system_id)
        self.nli_cache[system_id] = meta
        return meta

    def _fetch_single_worker(self, system_id):
        url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{system_id}"
        # Initialize default meta structure
        meta = {'shelfmark': 'Unknown', 'title': '', 'desc': '', 'fl_ids': [], 'thumb_url': None, 'thumb_checked': False}
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        import time 

        for attempt in range(2):
            try:
                time.sleep(0.3)
                session = self._make_session()
                resp = session.get(url, headers=headers, timeout=10)
                
                if resp.status_code == 200:
                    try:
                        root = ET.fromstring(resp.content)
                        
                        # --- 1. Extract Representative FL (907 $d) ---
                        # This is the "Cover Image" or main representative FL
                        rep_fl = None
                        for df in root.findall("marc:datafield[@tag='907']", self.ns):
                            sf = df.find("marc:subfield[@code='d']", self.ns)
                            if sf is not None and sf.text:
                                clean_fl = sf.text.strip()
                                if clean_fl.startswith("FL"):
                                    rep_fl = clean_fl
                                    break 
                        
                        # --- 2. Extract Standard Metadata ---
                        c_942 = None; c_907 = None; c_090 = None; c_avd = None
                        fl_ids = self._extract_fl_ids(root) # Backup list

                        for df in root.findall('marc:datafield', self.ns):
                            tag = df.get('tag')
                            def get_val(code):
                                sf = df.find(f"marc:subfield[@code='{code}']", self.ns)
                                return sf.text if sf is not None else None

                            if tag == '942':
                                val = get_val('z')
                                if val: 
                                    if not c_942: c_942 = val
                                    elif val.isdigit(): pass
                                    else: c_942 = val
                            elif tag == '907':
                                val = get_val('e')
                                if val: c_907 = val
                            elif tag == '090':
                                val = get_val('a')
                                if val and "MSS" not in val: c_090 = val
                            elif tag == 'AVD':
                                val = get_val('e')
                                if val: c_avd = val
                            elif tag == '245':
                                val = get_val('a')
                                if val: meta['title'] = val.rstrip('./,:;')

                        final = c_942 or c_907 or c_090 or c_avd
                        if final: meta['shelfmark'] = final

                        meta['fl_ids'] = fl_ids
                        
                        # --- 3. Set Thumbnail URL ---
                        # PRIORITIZE the Representative FL found in 907 $d
                        if rep_fl:
                             meta['thumb_url'] = self._resolve_thumbnail([rep_fl])
                        else:
                             # Only if missing, fallback to the list
                             meta['thumb_url'] = self._resolve_thumbnail(fl_ids)
                             
                        meta['thumb_checked'] = True
                        return system_id, meta

                    except ET.ParseError:
                        break
                elif resp.status_code >= 500:
                    time.sleep(1)
                else:
                    break
            except Exception:
                time.sleep(1)
        
        return system_id, meta

    def _extract_fl_ids(self, root):
        fl_ids = []
        for df in root.findall("marc:datafield[@tag='907']", self.ns):
            for sf in df.findall("marc:subfield[@code='d']", self.ns):
                val = (sf.text or "").strip()
                if val.startswith("FL"):
                    fl_ids.append(val)
        return fl_ids

    def _resolve_thumbnail(self, fl_ids, size=320, session=None):
        if not fl_ids: return None
        
        # Ensure it's iterable but treat string as single item list
        if isinstance(fl_ids, str): fl_ids = [fl_ids]
            
        for fl_id in fl_ids:
            if not fl_id: continue
            
            # Robust extraction of digits
            raw_str = str(fl_id)
            digits = re.sub(r"\D", "", raw_str)
            
            # Basic validation: FL IDs are usually long (e.g. 7+ digits)
            if not digits or len(digits) < 4: continue
            
            # Return the URL that worked in debug
            return f"https://iiif.nli.org.il/IIIFv21/FL{digits}/full/400,/0/default.jpg"
                
        return None

    @staticmethod
    def get_rosetta_fallback_url(fl_id):
        """Construct a fallback URL for Rosetta if IIIF fails."""
        if not fl_id: return None
        raw_str = str(fl_id)
        digits = re.sub(r"\D", "", raw_str)
        if not digits: return None
        return f"https://rosetta.nli.org.il/delivery/DeliveryManagerServlet?dps_func=thumbnail&dps_pid=FL{digits}"

    def _fetch_fl_ids(self, system_id):
        url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{system_id}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        try:
            session = self._make_session()
            resp = session.get(url, headers=headers, timeout=5, allow_redirects=True)
            if resp.status_code == 200:
                root = ET.fromstring(resp.content)
                return self._extract_fl_ids(root)
        except Exception:
            return []
        return []

    def get_thumbnail(self, system_id, size=320):
        meta = self.nli_cache.get(system_id)
        if meta and meta.get('thumb_checked') and meta.get('thumb_url'):
            return meta.get('thumb_url')

        fl_ids = []
        if meta:
            fl_ids = meta.get('fl_ids', [])
        if not fl_ids:
            fl_ids = self._fetch_fl_ids(system_id)

        thumb_url = self._resolve_thumbnail(fl_ids, size=size)

        if meta is None:
            meta = {'shelfmark': 'Unknown', 'title': '', 'desc': '', 'fl_ids': fl_ids}
        meta['fl_ids'] = fl_ids
        meta['thumb_url'] = thumb_url
        meta['thumb_checked'] = True
        self.nli_cache[system_id] = meta
        return thumb_url
        
    def batch_fetch_shelfmarks(self, system_ids, progress_callback=None):
        to_fetch = [sid for sid in system_ids if sid not in self.nli_cache]
        if not to_fetch: return

        futures = {self.nli_executor.submit(self._fetch_single_worker, sid): sid for sid in to_fetch}
        count = 0
        for future in as_completed(futures):
            sid, meta = future.result()
            self.nli_cache[sid] = meta
            count += 1
            if progress_callback:
                progress_callback(count, len(to_fetch), sid)
        self.save_caches()

    def search_by_meta(self, query, field):
        """Search for system IDs where the specified field matches the query."""
        results = set()
        q_norm = query.lower()

        # 1. Search in CSV Bank (Fastest)
        for sys_id, data in self.csv_bank.items():
            val = data.get(field, '')
            if val and q_norm in val.lower():
                results.add(sys_id)

        # 2. Search in NLI Cache (for items not in CSV or updated)
        for sys_id, data in self.nli_cache.items():
            val = data.get(field, '')
            if val and q_norm in val.lower():
                results.add(sys_id)

        return list(results)

    def get_display_data(self, full_header, src_label):
        sys_id, p_num = self.parse_header_smart(full_header)

        meta = self.nli_cache.get(sys_id, {'shelfmark': '', 'title': ''})
        shelfmark = meta.get('shelfmark')

        # Fallback to CSV bank if not in cache (get_meta_for_id handles this priority)
        if not shelfmark or shelfmark == "Unknown":
             shelfmark, _ = self.get_meta_for_id(sys_id)

        return {
            'shelfmark': shelfmark or f"ID: {sys_id}",
            'title': meta.get('title', ''),
            'img': p_num,
            'source': src_label,
            'id': sys_id
        }

# ==============================================================================
#  LAB ENGINE
# ==============================================================================
class LabEngine:
    """Handles advanced logic for Lab Mode: Two-Stage Retrieval, Normalization, Lab Indexing."""
    RARE_THRESHOLD = 0.001
    LAB_TRIGRAM_TOKENIZER = "lab_trigram_3"
    MAX_RERANK_BATCH = 1000
    MAX_DOC_TOKENS = 1500
    SNIPPET_WINDOW = 160

    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        self.settings = LabSettings()
        self.lab_index = None
        self.lab_searcher = None
        self.lab_index_needs_rebuild = False
        self._ngram_field_validated = False
        self._reload_lab_index()

    def _build_trigram_analyzer(self):
        builder = TextAnalyzerBuilder(Tokenizer.ngram(min_gram=3, max_gram=3, prefix_only=False))
        builder = builder.filter(Filter.lowercase())
        builder = builder.filter(Filter.remove_long(64))
        return builder.build()

    def _register_trigram_tokenizer(self, index):
        try:
            analyzer = self._build_trigram_analyzer()
            index.register_tokenizer(self.LAB_TRIGRAM_TOKENIZER, analyzer)
        except Exception as e:
            LAB_LOGGER.error("Failed to register trigram tokenizer: %s", e)

    def _get_ngram_field(self):
        if not self.lab_index:
            return None
        if self._ngram_field_validated:
            return "text_ngram"
        try:
            Query.term_query(self.lab_index.schema, "text_ngram", "__probe__")
            self._ngram_field_validated = True
            return "text_ngram"
        except Exception:
            LAB_LOGGER.error("Field 'text_ngram' not found in schema.")
            self._ngram_field_validated = True
            return None

    def _build_ngram_query(self, grams):
        if not grams:
            return None
        if self._get_ngram_field() is None:
            return None
        schema = self.lab_index.schema
        subqueries = []
        for gram in grams:
            if not gram:
                continue
            try:
                tq = Query.term_query(schema, "text_ngram", gram)
                subqueries.append((Occur.Should, tq))
            except Exception as e:
                LAB_LOGGER.debug("Failed to build term query for %s: %s", gram, e)
        if not subqueries:
            return None
        return Query.boolean_query(subqueries)

    def _build_query_windows(self, tokens, window_size=15, overlap=5):
        if not tokens:
            return []
        if len(tokens) <= window_size:
            return [" ".join(tokens)]
        step = max(1, window_size - overlap)
        windows = []
        for start in range(0, len(tokens), step):
            slice_tokens = tokens[start:start + window_size]
            if slice_tokens:
                windows.append(" ".join(slice_tokens))
            if start + window_size >= len(tokens):
                break
        return windows

    def _expand_ngrams_from_text(self, text):
        normalized = self.lab_index_normalize(text)
        grams = set(self.generate_ngrams(normalized, 3).split())

        if self.settings.ignore_matres:
            skeleton = normalized.replace("ו", "").replace("י", "")
            grams.update(self.generate_ngrams(skeleton, 3).split())

        if self.settings.phonetic_expansion:
            for word in normalized.split():
                for idx, char in enumerate(word):
                    for repl in self.var_mgr.basic_map.get(char, set()):
                        variant = word[:idx] + repl + word[idx + 1:]
                        grams.update(self.generate_ngrams(variant, 3).split())

        return sorted(g for g in grams if g)

    def _tokenize_with_spans(self, text):
        tokens = []
        for m in re.finditer(r"[\w\u0590-\u05FF]+", text):
            tokens.append((m.group().lower(), m.start(), m.end()))
            if len(tokens) >= self.MAX_DOC_TOKENS:
                break
        return tokens

    def _score_alignment(self, query_tokens, doc_tokens, gap_penalty, gap_tolerance=0):
        if not query_tokens or not doc_tokens:
            return 0.0, []
        matcher = difflib.SequenceMatcher(None, query_tokens, [t[0] for t in doc_tokens], autojunk=False)
        blocks = [b for b in matcher.get_matching_blocks() if b.size]
        if not blocks:
            return 0.0, []
        coverage = sum(b.size for b in blocks) / max(1, len(query_tokens))
        gap_cost = 0
        for prev, curr in zip(blocks, blocks[1:]):
            q_gap = max(0, curr.a - (prev.a + prev.size))
            d_gap = max(0, curr.b - (prev.b + prev.size))
            gap_amount = max(q_gap, d_gap)
            gap_cost += max(0, gap_amount - gap_tolerance)
        normalized_gap = gap_cost / max(1, len(query_tokens))
        score = (coverage * 0.6 + matcher.ratio() * 0.4) - gap_penalty * normalized_gap
        return score, blocks

    def _build_alignment_snippet(self, content, doc_tokens, blocks):
        spans = []
        for block in blocks:
            for offset in range(block.size):
                idx = block.b + offset
                if idx < len(doc_tokens):
                    span = doc_tokens[idx][1], doc_tokens[idx][2]
                    spans.append(span)
        merged = self._merge_spans(spans)
        if not merged:
            snippet_text = content[:300]
            return snippet_text, snippet_text
        start = max(0, merged[0][0] - self.SNIPPET_WINDOW)
        end = min(len(content), merged[-1][1] + self.SNIPPET_WINDOW)
        rel_spans = [
            (max(0, s - start), min(end - start, e - start))
            for s, e in merged if s < end and e > start
        ]
        snippet_text = content[start:end]
        snippet_hl = self._apply_highlights(snippet_text, rel_spans)
        full_hl = self._apply_highlights(content, merged)
        return f"...{snippet_hl}...", full_hl

    def _trigram_overlap(self, query_trigrams, content):
        if not query_trigrams:
            return 0.0
        normalized = self.lab_index_normalize(content)
        doc_trigrams = set(self.generate_ngrams(normalized, 3).split())
        if not doc_trigrams:
            return 0.0
        return (len(query_trigrams & doc_trigrams) / len(query_trigrams)) * 100.0

    def _gather_candidate_addresses(self, windows, progress_callback=None):
        candidates = {}
        stage_limit = self._stage1_limit()
        total_windows = len(windows)

        for idx, win in enumerate(windows):
            grams = self._expand_ngrams_from_text(win)
            query_obj = self._build_ngram_query(grams)
            if not query_obj:
                continue
            try:
                res_obj = self.lab_searcher.search(query_obj, stage_limit)
            except Exception as e:
                LAB_LOGGER.error("N-Gram search failed for window %s: %s", idx, e)
                continue

            for score, doc_addr in res_obj.hits:
                prev = candidates.get(doc_addr)
                if prev is None or score > prev:
                    candidates[doc_addr] = score

            if progress_callback:
                progress_callback(idx + 1, max(1, total_windows))

        ranked = sorted(candidates.items(), key=lambda kv: kv[1], reverse=True)
        return ranked[:self._candidate_limit()]

    @staticmethod
    def _chunk(seq, size):
        for idx in range(0, len(seq), size):
            yield seq[idx: idx + size]

    def _close_index(self):
        """Force release of Tantivy file handles."""
        self.lab_searcher = None
        self.lab_index = None
        self._ngram_field_validated = False
        import gc
        gc.collect() 

    def _reload_lab_index(self):
        if os.path.exists(Config.LAB_INDEX_DIR):
            try:
                self.lab_index = tantivy.Index.open(Config.LAB_INDEX_DIR)
                self._register_trigram_tokenizer(self.lab_index)

                # Robust check: Try to parse a query on the field
                try:
                    schema = self.lab_index.schema
                    self._ngram_field_validated = False
                    Query.term_query(schema, "text_ngram", "tst")
                except Exception:
                    LAB_LOGGER.warning("Lab index schema outdated (missing text_ngram).")
                    self.lab_index_needs_rebuild = True
                    self._close_index()
                    return False
                
                self.lab_searcher = self.lab_index.searcher()
                self.lab_index_needs_rebuild = False
                return True
            except Exception as e:
                LAB_LOGGER.error("Failed to load Lab Index: %s", e)
                self._close_index()
        
        self.lab_index_needs_rebuild = True
        return False

    @staticmethod
    def lab_index_normalize(text):
        return re.sub(r"[^\w\u0590-\u05FF\s\*\~]", "", text).replace('_', ' ').lower()

    @staticmethod
    def generate_ngrams(text, n=3):
        if not text: return ""
        cleaned = "".join(ch for ch in text if "\u0590" <= ch <= "\u05FF")
        if n <= 1 or len(cleaned) <= n: return cleaned
        grams = [cleaned[i:i + n] for i in range(len(cleaned) - n + 1)]
        return " ".join(grams)

    def rebuild_lab_index(self, progress_callback=None):
        """Build the isolated Lab Index. Uses Safe Mode (Trash Strategy)."""
        LAB_LOGGER.info(f"Starting Lab Index rebuild at: {Config.LAB_INDEX_DIR}")

        # 1. Close handles
        self._close_index()
        time.sleep(0.5)

        # 2. Validation
        if not os.path.exists(Config.FILE_V8):
            raise FileNotFoundError(tr("Input file not found: {}").format(Config.FILE_V8))

        # 3. Cleanup Strategy: Rename to trash instead of delete
        if os.path.exists(Config.LAB_INDEX_DIR):
            try:
                trash_name = f"lab_index_trash_{int(time.time())}"
                trash_path = os.path.join(os.path.dirname(Config.LAB_INDEX_DIR), trash_name)
                os.rename(Config.LAB_INDEX_DIR, trash_path)
            except Exception as e:
                LAB_LOGGER.error("Rename failed: %s. Trying delete...", e)
                try:
                    shutil.rmtree(Config.LAB_INDEX_DIR)
                except Exception as e2:
                    raise PermissionError(f"Cannot clear index. Restart App. ({e2})")

        os.makedirs(Config.LAB_INDEX_DIR, exist_ok=True)

        # 4. Define Schema
        builder = tantivy.SchemaBuilder()
        builder.add_text_field("unique_id", stored=True)
        builder.add_text_field("text_normalized", stored=True, tokenizer_name="whitespace")
        # CRITICAL FIX: Ensure this field is added
        builder.add_text_field("text_ngram", stored=False, tokenizer_name=self.LAB_TRIGRAM_TOKENIZER)
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        builder.add_text_field("source", stored=True)
        builder.add_text_field("content", stored=True)

        schema = builder.build()
        index = tantivy.Index(schema, path=Config.LAB_INDEX_DIR)
        self._register_trigram_tokenizer(index)
        writer = index.writer(heap_size=50_000_000)

        # 5. Indexing Loop
        total_docs = 0
        def count_lines(fname):
            if not os.path.exists(fname): return 0
            with open(fname, 'r', encoding='utf-8') as f: return sum(1 for line in f)

        total_lines = count_lines(Config.FILE_V8) + count_lines(Config.FILE_V7)
        processed_lines = 0

        for fpath, label in [(Config.FILE_V8, "V0.8"), (Config.FILE_V7, "V0.7")]:
            if not os.path.exists(fpath): continue
            LAB_LOGGER.info("Indexing %s...", label)

            with open(fpath, 'r', encoding='utf-8-sig') as f:
                cid, chead, ctext = None, None, []
                for line in f:
                    processed_lines += 1
                    line = line.strip()
                    is_sep = (label == "V0.8" and line.startswith("==>")) or (label == "V0.7" and line.startswith("###"))

                    if is_sep:
                        if cid and ctext:
                            original_content = "\n".join(ctext)
                            norm_content = self.lab_index_normalize(original_content)
                            ngram_content = self.generate_ngrams(original_content, self.settings.ngram_size)
                            shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or "Unknown"

                            writer.add_document(tantivy.Document(
                                unique_id=str(cid),
                                text_normalized=norm_content,
                                text_ngram=ngram_content, # CRITICAL FIX: Populate field
                                content=original_content,
                                full_header=str(chead),
                                shelfmark=str(shelfmark),
                                source=str(label)
                            ))
                            total_docs += 1
                        chead = line.replace("==>", "").replace("<==", "").strip() if label == "V0.8" else line
                        cid = self.meta_mgr.extract_unique_id(line)
                        ctext = [] 
                    else:
                        ctext.append(line)

                    if progress_callback and processed_lines % 1000 == 0:
                        progress_callback(processed_lines, total_lines)

                # Last doc
                if cid and ctext:
                    original_content = "\n".join(ctext)
                    norm_content = self.lab_index_normalize(original_content)
                    ngram_content = self.generate_ngrams(original_content, self.settings.ngram_size)
                    shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or "Unknown"
                    writer.add_document(tantivy.Document(
                        unique_id=str(cid),
                        text_normalized=norm_content,
                        text_ngram=ngram_content,
                        content=original_content,
                        full_header=str(chead),
                        shelfmark=str(shelfmark),
                        source=str(label)
                    ))
                    total_docs += 1

        writer.commit()
        LAB_LOGGER.info("Lab Index rebuild complete. %d docs.", total_docs)
        
        self._reload_lab_index()
        return total_docs

    # --- Search Methods (Fixing Syntax Error) ---

    def _parse_lab_ngram_query(self, query_str):
        ngram_field = self._get_ngram_field()
        if not ngram_field:
            return None

        fields = [ngram_field]
        try:
            return self.lab_index.parse_query(query_str, fields)
        except Exception:
            try:
                scoped_query = f"text_ngram:({query_str})"
                return self.lab_index.parse_query(scoped_query, fields)
            except Exception as e2:
                LAB_LOGGER.error(f"Query parsing failed: {e2}")
                return None

    def _candidate_limit(self):
        return max(500, min(self.settings.candidate_limit, 50000))

    def _stage1_limit(self):
        return self._candidate_limit()

    def lab_search(self, query_str, mode='variants', progress_callback=None, gap=0):
        if not self.lab_searcher or self.lab_index_needs_rebuild:
            LAB_LOGGER.warning("Lab Index not loaded or needs rebuild.")
            return iter(())

        LAB_LOGGER.info("Lab Mode Broad Net: %s", query_str)

        cleaned_query = self.lab_index_normalize(query_str)
        tokens = [t for t in cleaned_query.split() if t]
        if not tokens:
            return iter(())

        windows = self._build_query_windows(tokens, window_size=15, overlap=5)
        if not windows:
            return iter(())

        candidates = self._gather_candidate_addresses(windows, progress_callback=progress_callback)
        total_candidates = len(candidates)
        query_trigrams = set(self.generate_ngrams(cleaned_query, 3).split())
        gap_tolerance = max(0, gap)
        gap_penalty = max(0.0, self.settings.gap_penalty)

        def _generator():
            processed = 0
            for batch in self._chunk(candidates, self.MAX_RERANK_BATCH):
                docs = []
                for doc_addr, bm25_score in batch:
                    try:
                        docs.append((bm25_score, self.lab_searcher.doc(doc_addr)))
                    except Exception as e:
                        LAB_LOGGER.debug("Failed to load doc %s: %s", doc_addr, e)

                for bm25_score, doc in docs:
                    try:
                        source = doc['source'][0] if 'source' in doc else "Unknown"
                        header = doc['full_header'][0]
                        uid = doc['unique_id'][0]
                        content = doc['content'][0]
                    except Exception as e:
                        LAB_LOGGER.debug("Malformed document skipped: %s", e)
                        continue

                    overlap_pct = self._trigram_overlap(query_trigrams, content)
                    if overlap_pct < self.settings.min_should_match:
                        continue

                    doc_tokens = self._tokenize_with_spans(content)
                    score, blocks = self._score_alignment(tokens, doc_tokens, gap_penalty, gap_tolerance)
                    if score <= 0:
                        continue

                    snippet, hl_full = self._build_alignment_snippet(content, doc_tokens, blocks)
                    display = self.meta_mgr.get_display_data(header, source)

                    yield {
                        'display': display,
                        'snippet': self._snippet_html(snippet),
                        'full_text': content,
                        'uid': uid,
                        'raw_header': header,
                        'raw_file_hl': hl_full,
                        'highlight_pattern': None,
                        'score': score,
                        'bm25': bm25_score,
                        'overlap': overlap_pct
                    }

                processed += len(batch)
                if progress_callback:
                    progress_callback(processed, max(1, total_candidates))

        return _generator()

    def deduplicate_lab_results(self, results):
        best = {}
        for r in results:
            sid = None
            if r.get('display'):
                sid = r['display'].get('id')
            if not sid:
                sid = self.meta_mgr.extract_unique_id(r.get('raw_header', '')) or r.get('uid')

            existing = best.get(sid)
            if existing is None or r.get('score', 0) > existing.get('score', 0):
                best[sid] = r

        final = list(best.values())
        final.sort(key=lambda x: (x.get('score', 0), x.get('bm25', 0)), reverse=True)
        return final

    def lab_composition_search(self, full_text, mode='variants', progress_callback=None, chunk_size=None):
        if not self.lab_searcher or self.lab_index_needs_rebuild:
            LAB_LOGGER.warning("Lab Index not ready.")
            return {'main': [], 'filtered': []}

        LAB_LOGGER.info("Starting N-Gram Composition Search...")
        start_time = time.time()

        norm_text = self.lab_index_normalize(full_text)
        tokens = norm_text.split()
        
        c_size = int(chunk_size) if chunk_size else 7
        step = max(1, c_size // 2)

        if len(tokens) < c_size:
            chunks = [" ".join(tokens)]
        else:
            chunks = [" ".join(tokens[i : i + c_size]) for i in range(0, len(tokens) - c_size + 1, step)]

        LAB_LOGGER.info("Generated %d chunks (Size: %d, Step: %d)", len(chunks), c_size, step)

        candidates = {}

        for i, chunk_str in enumerate(chunks):
            if progress_callback and i % 5 == 0:
                progress_callback(i, len(chunks))

            base_ngrams = self.generate_ngrams(chunk_str, self.settings.ngram_size)
            base_gram_list = base_ngrams.split()
            grams_set = set(base_gram_list)
            
            if self.settings.ignore_matres:
                skeleton = chunk_str.replace("ו", "").replace("י", "")
                grams_set.update(self.generate_ngrams(skeleton, self.settings.ngram_size).split())
            
            if self.settings.phonetic_expansion:
                for word in chunk_str.split():
                    for idx, char in enumerate(word):
                        for repl in self.var_mgr.basic_map.get(char, set()):
                             variant = word[:idx] + repl + word[idx+1:]
                             grams_set.update(self.generate_ngrams(variant, self.settings.ngram_size).split())

            if not grams_set: continue

            try:
                t_query = self._build_ngram_query(sorted(grams_set))
                if t_query is None:
                    continue
                res = self.lab_searcher.search(t_query, 50)
            except Exception:
                continue

            for score, doc_addr in res.hits:
                doc = self.lab_searcher.doc(doc_addr)
                uid = doc['unique_id'][0]
                
                if uid not in candidates:
                    candidates[uid] = {
                        'uid': uid,
                        'score': 0,
                        'hits': 0,
                        'content': doc['content'][0],
                        'header': doc['full_header'][0],
                        'src': doc['source'][0] if 'source' in doc else "Unknown"
                    }
                
                candidates[uid]['score'] += score
                candidates[uid]['hits'] += 1

        results = list(candidates.values())
        results.sort(key=lambda x: x['score'], reverse=True)
        results = results[:self.settings.candidate_limit]

        formatted_results = []
        full_source_grams = self.generate_ngrams(full_text[:1000], self.settings.ngram_size).split()
        
        for cand in results:
            hl_text = self._highlight_ngram_in_text(cand['content'], full_source_grams)
            snippet_html = self._generate_ngram_snippet(cand['content'], full_source_grams)

            formatted_results.append({
                'uid': cand['uid'],
                'score': cand['score'],
                'raw_header': cand['header'],
                'src_lbl': cand['src'],
                'source_ctx': "Composition Match",
                'text': self._snippet_html(snippet_html),
                'raw_file_hl': hl_text
            })

        elapsed = time.time() - start_time
        LAB_LOGGER.info("Composition search done. %d results in %.2fs", len(formatted_results), elapsed)
        
        return {'main': formatted_results, 'filtered': []}

    # --- Highlighting Helpers (Required) ---
    def _snippet_html(self, snippet):
        if not snippet: return snippet
        return re.sub(r'\*(.*?)\*', r"<b style='color:red;'>\1</b>", snippet)

    def _highlight_ngram_in_text(self, text, grams):
        if not text or not grams: return text
        spans = self._find_ngram_spans(text, grams)
        return self._apply_highlights(text, spans)

    def _generate_ngram_snippet(self, text, grams, window=120):
        if not text: return text
        if not grams: return text[:300]
        spans = self._find_ngram_spans(text, grams)
        if not spans: return text[:300]
        
        first_start, first_end = spans[0]
        start = max(0, first_start - window)
        end = min(len(text), first_end + window)
        
        snippet_text = text[start:end]
        rel_spans = [
            (max(0, s - start), min(end - start, e - start))
            for s, e in spans if s < end and e > start
        ]
        merged = self._merge_spans(rel_spans)
        
        hl = self._apply_highlights(snippet_text, merged)
        return f"...{hl}..."

    @staticmethod
    def _find_ngram_spans(text, grams):
        spans = []
        for g in grams:
            if not g: continue
            start = 0
            while True:
                idx = text.find(g, start)
                if idx == -1: break
                spans.append((idx, idx + len(g)))
                start = idx + 1
        return LabEngine._merge_spans(spans)

    @staticmethod
    def _merge_spans(spans):
        if not spans: return []
        spans.sort()
        merged = [list(spans[0])]
        for s, e in spans[1:]:
            last = merged[-1]
            if s <= last[1]:
                last[1] = max(last[1], e)
            else:
                merged.append([s, e])
        return [(s, e) for s, e in merged]

    @staticmethod
    def _apply_highlights(text, spans):
        if not spans: return text
        out = []
        last_idx = 0
        for s, e in spans:
            out.append(text[last_idx:s])
            out.append(f"*{text[s:e]}*")
            last_idx = e
        out.append(text[last_idx:])
        return "".join(out)
    
    # Minimal helpers for legacy support (if called externally)
    def _get_doc_freq(self, term): return 0
    def _is_rare(self, term, total): return False


# ==============================================================================
#  INDEXER
# ==============================================================================
class Indexer:
    """Create or update the Tantivy index and keep browse maps in sync."""
    def __init__(self, meta_mgr):
        self.meta_mgr = meta_mgr

    def create_index(self, progress_callback=None):
        # Validation
        if not os.path.exists(Config.FILE_V8):
            raise FileNotFoundError(tr("Input file not found: {}\nPlease place 'Transcriptions.txt' next to the executable.").format(Config.FILE_V8))

        # Ensure main index dir exists
        if not os.path.exists(Config.INDEX_DIR):
            os.makedirs(Config.INDEX_DIR)
            
        # Specific Tantivy Subfolder (to avoid deleting user data)
        db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
        if os.path.exists(db_path):
            shutil.rmtree(db_path)
        os.makedirs(db_path)

        builder = tantivy.SchemaBuilder()
        builder.add_text_field("unique_id", stored=True)
        builder.add_text_field("content", stored=True, tokenizer_name="whitespace")
        builder.add_text_field("source", stored=True)
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        schema = builder.build()
        
        index = tantivy.Index(schema, path=db_path)
        writer = index.writer(heap_size=30_000_000)
        
        total_docs = 0
        browse_map = defaultdict(list)
        
        def count_lines(fname):
            if not os.path.exists(fname): return 0
            with open(fname, 'r', encoding='utf-8') as f: return sum(1 for line in f)

        total_lines = count_lines(Config.FILE_V8) + count_lines(Config.FILE_V7)
        processed_lines = 0

        for fpath, label in [(Config.FILE_V8, "V0.8"), (Config.FILE_V7, "V0.7")]:
            if not os.path.exists(fpath): continue
            with open(fpath, 'r', encoding='utf-8') as f:
                cid, chead, ctext = None, None, []
                for line in f:
                    processed_lines += 1
                    line = line.strip()
                    is_sep = (label == "V0.8" and line.startswith("==>")) or (label == "V0.7" and line.startswith("###"))
                    
                    if is_sep:
                        if cid and ctext:
                            shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or self.meta_mgr.meta_map.get(cid, "")
                            writer.add_document(tantivy.Document(
                                unique_id=str(cid), content="\n".join(ctext), source=str(label),
                                full_header=str(chead), shelfmark=str(shelfmark)
                            ))
                            parsed = self.meta_mgr.parse_full_id_components(chead)
                            if parsed['sys_id'] and parsed['p_num']:
                                browse_map[parsed['sys_id']].append({'p_num': int(parsed['p_num']), 'uid': cid, 'full_header': chead})
                            total_docs += 1
                        chead = line.replace("==>", "").replace("<==", "").strip() if label == "V0.8" else line
                        cid = self.meta_mgr.extract_unique_id(line)
                        ctext = []
                    else: ctext.append(line)
                    if progress_callback and processed_lines % 1000 == 0:
                        progress_callback(processed_lines, total_lines)
                
                if cid and ctext:
                    shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or self.meta_mgr.meta_map.get(cid, "")
                    writer.add_document(tantivy.Document(
                        unique_id=str(cid), content=" ".join(ctext), source=str(label),
                        full_header=str(chead), shelfmark=str(shelfmark)
                    ))
                    parsed = self.meta_mgr.parse_full_id_components(chead)
                    if parsed['sys_id'] and parsed['p_num']:
                        browse_map[parsed['sys_id']].append({'p_num': int(parsed['p_num']), 'uid': cid, 'full_header': chead})
                    total_docs += 1

        writer.commit()
        for sid in browse_map: browse_map[sid].sort(key=lambda x: x['p_num'])
        with open(Config.BROWSE_MAP, 'wb') as f: pickle.dump(browse_map, f)
        return total_docs

# ==============================================================================
#  SEARCH ENGINE
# ==============================================================================
class SearchEngine:
    """Run searches, build queries, and provide browsing utilities."""
    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        self.index = None
        self.searcher = None
        self.reload_index()

    def reload_index(self):
        db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
        if os.path.exists(db_path):
            try:
                self.index = tantivy.Index.open(db_path)
                self.searcher = self.index.searcher()
                return True
            except Exception as e:
                LOGGER.error("Failed to reload Tantivy index from %s: %s", db_path, e)
        return False

    def build_tantivy_query(self, terms, mode):
        if mode == 'Regex':
            regex_str = terms[0]
            candidates = re.findall(r'[\u0590-\u05FF]{2,}', regex_str)
            if candidates: return " AND ".join(candidates)
            else: return "*" 

        parts = []
        for term in terms:
            if term.upper() in ['AND', 'OR', 'NOT', '(', ')']:
                parts.append(term)
                continue
                
            if mode == 'fuzzy':
                if len(term) < 3: parts.append(f'"{term}"') 
                elif len(term) < 5: parts.append(f'"{term}"~1')
                else: parts.append(f'"{term}"~2')
            else:
                # 1. Get variants (limit 200 is usually enough if quality is good)
                all_vars = self.var_mgr.get_variants(term, mode, limit=200)
                
                # 2. Prepare list
                clean_vars = []
                
                # Add EXACT term with BOOST (^5)
                # This tells Tantivy: "If you find the exact word, it's 5x more important"
                clean_vars.append(f'"{term}"^5')
                
                # Add variants
                for v in all_vars:
                    if v == term: continue # Skip exact (already added)
                    
                    # CRITICAL FIX: Filter out 1-letter noise variants
                    # If original was >1 char, variant must be >1 char.
                    # Prevents single-letter fallbacks that over-match
                    if len(term) > 1 and len(v) < 2:
                        continue
                        
                    # Clean quotes
                    v_clean = v.replace('"', '')
                    if v_clean:
                        clean_vars.append(f'"{v_clean}"')
                
                parts.append(f'({" OR ".join(clean_vars)})')
                
        return " AND ".join(parts)

    def build_regex_pattern(self, terms, mode, max_gap):
        if mode == 'Regex':
            try: return re.compile(" ".join(terms), re.IGNORECASE)
            except: return None

        parts = []
        for term in terms:
            regex_mode = 'variants_maximum' if mode == 'fuzzy' else mode
            
            # 1. Get variants
            vars_list = self.var_mgr.get_variants(term, regex_mode, limit=Config.REGEX_VARIANTS_LIMIT)
            
            # 2. Ensure exact term
            if term not in vars_list:
                vars_list.append(term)
            
            # 3. Sort by LENGTH (Descending)
            # This is the correct fix for the visual glitch. 
            # Favor longer matches before short variants
            unique_vars = sorted(list(set(vars_list)), key=len, reverse=True)
            
            # 4. Escape special chars
            escaped = [re.escape(v) for v in unique_vars]
            
            # 5. Simple Group (Removed strict Lookbehind/Lookahead)
            # Allow prefix matches when search term appears inside a word
            parts.append(f"({'|'.join(escaped)})")

        if max_gap == 0:
            # Flexible separator (any non-word char)
            sep = r'[^\w\u0590-\u05FF\']+'
        else:
            # Gap logic
            sep = rf'(?:[^\w\u0590-\u05FF\']+{Config.WORD_TOKEN_PATTERN}){{0,{max_gap}}}[^\w\u0590-\u05FF\']+'

        try: 
            return re.compile(sep.join(parts), re.IGNORECASE)
        except: 
            return None

    def highlight(self, text, regex, for_file=False):
        m = regex.search(text)
        if not m: return None
        s, e = m.span()
        start = max(0, s - 60)
        end = min(len(text), e + 60)
        
        # Calculate indices relative to snippet
        rel_s = s - start
        rel_e = e - start

        # Grab raw snippet
        snippet = text[start:end]
        
        # If showing in table (HTML), verify valid HTML and remove newlines for compactness
        if not for_file:
            # Clean newlines for table display so rows don't explode
            snippet_clean = snippet.replace('\n', ' ')

            # Since we removed newlines, indices might shift if newlines were before the match.
            # However, simpler approach: Split snippet into pre-match, match, post-match based on INDICES.
            # But 'replace' logic assumes we are working on the string WITH newlines removed.
            # If we remove newlines first, we lose index fidelity if newlines were inside the snippet range.
            # A safer way: Highlight FIRST, then clean newlines.

            # 1. Highlight in raw text snippet
            hl_snippet = snippet[:rel_s] + f"<b style='color:red;'>{snippet[rel_s:rel_e]}</b>" + snippet[rel_e:]

            # 2. Now clean newlines
            return hl_snippet.replace('\n', ' ')
        
        # If for export file, keep newlines or mark them
        return snippet[:rel_s] + f"*{snippet[rel_s:rel_e]}*" + snippet[rel_e:]

    def _get_best_text_for_id(self, sys_id):
        """Find the first page with meaningful text for a given System ID."""
        if not self.searcher: return "", "", "", ""

        # Query index for all pages of this manuscript
        try:
            q = self.index.parse_query(f'full_header:"{sys_id}"', ["full_header"])
            # Fetch enough docs to cover a manuscript
            res = self.searcher.search(q, 2000)
        except:
            return "", "", "", ""

        pages = []
        for score, doc_addr in res.hits:
            doc = self.searcher.doc(doc_addr)
            full_header = doc['full_header'][0]

            # Verify this doc really belongs to the sys_id (strict check)
            parsed = self.meta_mgr.parse_header_smart(full_header)
            if parsed[0] != sys_id:
                continue

            p_num_str = parsed[1]
            try: p_num = int(p_num_str)
            except: p_num = 999999

            content = doc['content'][0]
            uid = doc['unique_id'][0]
            src = doc['source'][0]
            pages.append({'p': p_num, 'text': content, 'head': full_header, 'uid': uid, 'src': src})

        if not pages:
            return "", "", "", ""

        # Sort by page number
        pages.sort(key=lambda x: x['p'])

        # Heuristic: Find first page with sequence of 3 words, each > 3 chars
        best_page = pages[0] # Default to first page

        pattern = re.compile(r'[\w\u0590-\u05FF]{4,}\s+[\w\u0590-\u05FF]{4,}\s+[\w\u0590-\u05FF]{4,}')

        for p in pages:
            if pattern.search(p['text']):
                best_page = p
                break

        return best_page['text'], best_page['head'], best_page['src'], best_page['uid']

    def execute_search(self, query_str, mode, gap, progress_callback=None):
        if not self.searcher: return []

        # --- Metadata Search Modes ---
        if mode in ['Title', 'Shelfmark']:
            field_map = {'Title': 'title', 'Shelfmark': 'shelfmark'}
            target_field = field_map.get(mode)

            sys_ids = self.meta_mgr.search_by_meta(query_str, target_field)
            results = []
            total_ids = len(sys_ids)

            for i, sid in enumerate(sys_ids):
                if progress_callback and i % 10 == 0: progress_callback(i, total_ids)

                text, head, src, uid = self._get_best_text_for_id(sid)
                if not text: continue

                meta = self.meta_mgr.get_display_data(head, src or "V0.8")

                # Limit snippet length for display
                snippet = text[:300] + "..." if len(text) > 300 else text

                results.append({
                    'display': meta,
                    'snippet': snippet,
                    'full_text': text,
                    'uid': uid,
                    'raw_header': head,
                    'raw_file_hl': text,
                    'highlight_pattern': None
                })

            return results
        
        if mode == 'Regex': terms = [query_str] 
        else: terms = query_str.split()
            
        t_query_str = self.build_tantivy_query(terms, mode)
        regex = self.build_regex_pattern(terms, mode, gap)
        if not regex: return []
        
        # Save pattern string for passing to results
        pattern_str = regex.pattern

        try:
            query = self.index.parse_query(t_query_str, ["content"])
            res_obj = self.searcher.search(query, Config.SEARCH_LIMIT)
        except Exception as e:
            LOGGER.warning("Search query failed to parse/execute for pattern %s: %s", t_query_str, e)
            return []

        hits = res_obj.hits if hasattr(res_obj, 'hits') else res_obj
        total_hits = len(hits)
        results = []

        for i, (score, doc_addr) in enumerate(hits):
            if progress_callback and i % 50 == 0:
                progress_callback(i, total_hits)
            try:
                doc = self.searcher.doc(doc_addr)
                content = doc['content'][0]
                hl_c = self.highlight(content, regex, False)
                hl_f = self.highlight(content, regex, True)
                if hl_c:
                    meta = self.meta_mgr.get_display_data(doc['full_header'][0], doc['source'][0])
                    results.append({
                        'display': meta, 'snippet': hl_c, 'full_text': content,
                        'uid': doc['unique_id'][0], 'raw_header': doc['full_header'][0],
                        'raw_file_hl': hl_f, 'highlight_pattern': pattern_str
                    })
            except Exception as e:
                LOGGER.warning("Failed to materialize search hit at position %s: %s", i, e)
        return self._deduplicate(results)

    def _deduplicate(self, results):
        v8 = {r['uid']: r for r in results if r['display']['source'] == "V0.8"}
        final = list(v8.values())
        for r in results:
            if r['display']['source'] == "V0.7" and r['uid'] not in v8: final.append(r)
        return final

    def search_composition_logic(self, full_text, chunk_size, max_freq, mode, filter_text=None, progress_callback=None):
        tokens = re.findall(Config.WORD_TOKEN_PATTERN, full_text)
        if len(tokens) < chunk_size: return None
        chunks = [tokens[i:i + chunk_size] for i in range(len(tokens) - chunk_size + 1)]

        # We need two accumulators now
        doc_hits_main = defaultdict(lambda: {'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(), 'patterns': set()})
        doc_hits_filtered = defaultdict(lambda: {'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(), 'patterns': set()})

        total_chunks = len(chunks)
        
        for i, chunk in enumerate(chunks):
            if progress_callback and i % 10 == 0: progress_callback(i, total_chunks)
            t_query = self.build_tantivy_query(chunk, mode)
            regex = self.build_regex_pattern(chunk, mode, 0)
            if not regex: continue

            # Check filter text (sampling)
            is_filtered = False
            if filter_text:
                if regex.search(filter_text):
                    is_filtered = True

            try:
                query = self.index.parse_query(t_query, ["content"])
                hits = self.searcher.search(query, 50).hits
                if len(hits) > max_freq: continue 
                for score, doc_addr in hits:
                    doc = self.searcher.doc(doc_addr)
                    content = doc['content'][0]
                    if regex.search(content):
                        uid = doc['unique_id'][0]

                        rec = doc_hits_filtered[uid] if is_filtered else doc_hits_main[uid]

                        rec['head'] = doc['full_header'][0]
                        rec['src'] = doc['source'][0]
                        rec['content'] = content
                        rec['matches'].append(regex.search(content).span())
                        rec['src_indices'].update(range(i, i + chunk_size))
                        rec['patterns'].add(regex.pattern)
            except Exception as e:
                LOGGER.warning("Failed composition chunk processing at token %s: %s", i, e)

        def build_items(hits_dict):
            final_items = []
            for uid, data in hits_dict.items():
                src_indices = sorted(list(data['src_indices']))
                src_snippets = []
                if src_indices:
                    for k, g in itertools.groupby(enumerate(src_indices), lambda ix: ix[0] - ix[1]):
                        group = list(map(lambda ix: ix[1], g))
                        s, e = group[0], group[-1]
                        ctx_s = max(0, s - 15); ctx_e = min(len(tokens), e + 1 + 15)
                        seq = " ".join(tokens[s:e+1])
                        src_snippets.append(f"... {' '.join(tokens[ctx_s:s])} *{seq}* {' '.join(tokens[e+1:ctx_e])} ...")

                spans = sorted(data['matches'], key=lambda x: x[0])
                merged = []
                if spans:
                    curr_s, curr_e = spans[0]
                    for s, e in spans[1:]:
                        if s <= curr_e + 20: curr_e = max(curr_e, e)
                        else: merged.append((curr_s, curr_e)); curr_s, curr_e = s, e
                    merged.append((curr_s, curr_e))

                score = sum(e-s for s,e in merged)
                ms_snips = []
                for s, e in merged:
                    start = max(0, s - 60); end = min(len(data['content']), e + 60)
                    ms_snips.append(data['content'][start:s] + "*" + data['content'][s:e] + "*" + data['content'][e:end])

                combined_pattern = "|".join(list(data['patterns'])) if data.get('patterns') else ""

                final_items.append({
                    'score': score, 'uid': uid,
                    'raw_header': data['head'], 'src_lbl': data['src'],
                    'source_ctx': "\n".join(src_snippets),
                    'text': "\n...\n".join(ms_snips),
                    'highlight_pattern': combined_pattern
                })
            final_items.sort(key=lambda x: x['score'], reverse=True)
            return final_items

        # Build both lists
        main_list = build_items(doc_hits_main)
        filtered_list = build_items(doc_hits_filtered)

        return {'main': main_list, 'filtered': filtered_list}

    def group_pages_by_manuscript(self, pages_list):
        """Aggregate individual page results into manuscript-level items."""
        grouped = defaultdict(list)

        # 1. Bucket pages by System ID
        for p in pages_list:
            sid, _ = self.meta_mgr.parse_header_smart(p['raw_header'])
            if sid:
                grouped[sid].append(p)
            else:
                # Fallback for pages without valid ID (should be rare)
                grouped["UNKNOWN"].append(p)

        manuscripts = []

        for sid, pages in grouped.items():
            if not pages: continue

            # Aggregate Score
            total_score = sum(p['score'] for p in pages)

            # Use the first page's header as the representative one for metadata parsing
            # (Ideally find the best page or just use the first)
            pages.sort(key=lambda x: x['score'], reverse=True)
            rep_page = pages[0]

            manuscript_item = {
                'type': 'manuscript',
                'sys_id': sid,
                'score': total_score,
                'pages': pages, # Keep all pages as children
                'raw_header': rep_page['raw_header'], # For metadata compatibility
                'text': rep_page['text'], # Representative text
                'source_ctx': rep_page.get('source_ctx', ''),
                'highlight_pattern': rep_page.get('highlight_pattern', '')
            }
            manuscripts.append(manuscript_item)

        # Sort manuscripts by aggregated score
        manuscripts.sort(key=lambda x: x['score'], reverse=True)
        return manuscripts

    def group_composition_results(self, items, threshold=5, progress_callback=None, status_callback=None, check_cancel=None):
        ids = []
        for i in items:
            if check_cancel and check_cancel(): return None, None, None
            # Check if it's a manuscript object with pre-parsed ID
            if i.get('type') == 'manuscript' and i.get('sys_id'):
                ids.append(i['sys_id'])
            else:
                ids.append(self.meta_mgr.parse_header_smart(i['raw_header'])[0])

        if status_callback:
            status_callback(tr("Fetching metadata..."))

        def fetch_cb(c, t, s):
            if progress_callback:
                progress_callback(c, t)

        self.meta_mgr.batch_fetch_shelfmarks([x for x in ids if x], progress_callback=fetch_cb)

        if status_callback:
            status_callback(tr("Grouping results..."))
            # Reset progress for the grouping phase
            if progress_callback:
                progress_callback(0, len(items))

        IGNORE_PREFIXES = {'קטע', 'קטעי', 'גניזה', 'לא', 'מזוהה', 'חיבור', 'פילוסופיה', 'הלכה', 'שירה', 'פיוט', 'מסמך', 'מכתב', 'ספרות', 'סיפורת', 'יפה', 'דרשות', 'פרשנות', 'מקרא', 'בפילוסופיה', 'קטעים', 'וספרות', 'מוסר', 'הגות', 'וחכמת', 'הלשון', 'פירוש', 'תפסיר', 'שרח', 'על', 'ספר', 'כתאב', 'משנה', 'תלמוד'}

        def _get_clean_words(t):
            if not t: return []
            clean = re.sub(r'[^\w]', ' ', t)
            return [w for w in clean.split() if len(w) > 1]

        def _get_signature(title_str):
            words = _get_clean_words(title_str)
            while words and words[0] in IGNORE_PREFIXES: words.pop(0)
            if not words: return None
            return f"{words[0]} {words[1]}" if len(words) >= 2 else words[0]

        wrapped = []
        for item in items:
            if item.get('type') == 'manuscript' and item.get('sys_id'):
                sid = item['sys_id']
            else:
                sid, _ = self.meta_mgr.parse_header_smart(item['raw_header'])

            meta = self.meta_mgr.nli_cache.get(sid, {})
            t = meta.get('title', '').strip()
            shelfmark = self.meta_mgr.get_shelfmark_from_header(item['raw_header']) or meta.get('shelfmark', 'Unknown')
            wrapped.append({
                'item': item, 'title': t, 'clean': " ".join(_get_clean_words(t)),
                'grouped': False, 'shelfmark': shelfmark
            })

        wrapped.sort(key=lambda x: len(x['title']))
        appendix = defaultdict(list)
        summary = defaultdict(list)
        total = len(wrapped)

        for i, root in enumerate(wrapped):
            if check_cancel and check_cancel(): return None, None, None
            if progress_callback and total:
                progress_callback(i, total)
            if root['grouped']: continue
            sig = _get_signature(root['title'])
            if not sig: continue
            matches = [root]
            for j, cand in enumerate(wrapped):
                if i == j or cand['grouped']: continue
                if sig in cand['clean']: matches.append(cand)
            if len(matches) > threshold:
                for m in matches:
                    m['grouped'] = True
                    appendix[sig].append(m['item'])
                    summary[sig].append(m['shelfmark'])
        
        if progress_callback and total:
            progress_callback(total, total)

        main_list = [w['item'] for w in wrapped if not w['grouped']]
        main_list.sort(key=lambda x: x['score'], reverse=True)
        return main_list, appendix, summary

    def get_full_text_by_id(self, uid):
        try:
            q = self.index.parse_query(f'unique_id:"{uid}"', ["unique_id"])
            res = self.searcher.search(q, 1)
            if res.hits: return self.searcher.doc(res.hits[0][1])['content'][0]
        except Exception as e:
            LOGGER.warning("Failed to retrieve full text for uid %s: %s", uid, e)
        return None

    def get_full_manuscript(self, sys_id):
        """Fetch ALL pages for a system ID, sorted by page number."""
        if not os.path.exists(Config.BROWSE_MAP): return []
        with open(Config.BROWSE_MAP, 'rb') as f: browse_map = pickle.load(f)
        
        pages_meta = browse_map.get(sys_id, [])
        if not pages_meta: return []

        full_content = []
        for p in pages_meta:
            text = self.get_full_text_by_id(p['uid'])
            if text:
                parsed = self.meta_mgr.parse_full_id_components(p.get('full_header', ''))
                full_content.append({
                    'p_num': p['p_num'],
                    'text': text,
                    'uid': p['uid'],
                    'full_header': p.get('full_header', ''),
                    'fl_id': parsed.get('fl_id')
                })
        return full_content
        
    def get_browse_page(self, sys_id, p_num=None, next_prev=0):
        if not os.path.exists(Config.BROWSE_MAP): return None
        with open(Config.BROWSE_MAP, 'rb') as f: browse_map = pickle.load(f)
        if sys_id not in browse_map: return None
        pages = browse_map[sys_id]
        if not pages: return None
        
        target_idx = 0
        if p_num is not None:
            for i, p in enumerate(pages):
                if p['p_num'] == p_num: target_idx = i; break
        
        new_idx = target_idx + next_prev
        if new_idx < 0 or new_idx >= len(pages): return None
        
        target_page = pages[new_idx]
        text = self.get_full_text_by_id(target_page['uid'])
        return {
            'uid': target_page['uid'], 'p_num': target_page['p_num'],
            'full_header': target_page['full_header'], 'text': text,
            'total_pages': len(pages), 'current_idx': new_idx + 1
        }

    def get_browse_page_by_fl(self, fl_id, sys_id=None):
        if not os.path.exists(Config.BROWSE_MAP): return None
        with open(Config.BROWSE_MAP, 'rb') as f: browse_map = pickle.load(f)

        if not fl_id:
            return None

        fl_digits = re.sub(r"\D", "", str(fl_id))
        if not fl_digits:
            return None

        sys_candidates = [sys_id] if sys_id else list(browse_map.keys())

        for sid in sys_candidates:
            if sid not in browse_map:
                continue
            pages = browse_map[sid]
            for idx, page in enumerate(pages):
                parsed = self.meta_mgr.parse_full_id_components(page.get('full_header', ''))
                page_fl = re.sub(r"\D", "", str(parsed.get('fl_id') or ""))
                if page_fl and page_fl == fl_digits:
                    text = self.get_full_text_by_id(page['uid'])
                    return {
                        'uid': page['uid'],
                        'p_num': page['p_num'],
                        'full_header': page['full_header'],
                        'text': text,
                        'total_pages': len(pages),
                        'current_idx': idx + 1,
                        'sys_id': sid,
                        'fl_id': fl_digits
                    }
        return None
