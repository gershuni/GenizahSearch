"""Core search, indexing, metadata, and AI helpers for the Genizah project."""

# -*- coding: utf-8 -*-
# genizah_core.py
import logging
import os
import sys
import re
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
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")

# ==============================================================================
#  LAB SETTINGS
# ==============================================================================
class LabSettings:
    """Manages configuration for the Lab Mode."""
    def __init__(self):
        self.custom_variants = {} # dict mapping char/string -> set of replacements
        self.expansion_budget = 5000
        self.slop_window = 15
        self.rare_word_bonus = 0.5
        self.normalize_abbreviations = True
        self.rare_threshold = 0.001 # Top 0.1% frequency considered rare
        self.candidate_limit = 2000
        self.max_char_changes = 1
        self.prefix_chars = 1

        # New Settings
        self.use_slop_window = True
        self.use_rare_words = True
        self.prefix_mode = False
        self.use_order_tolerance = False
        self.order_n = 4
        self.order_m = 4 # "m" as in "n+m" -> window size = n + m

        self.load()

    def load(self):
        if os.path.exists(Config.LAB_CONFIG_FILE):
            try:
                with open(Config.LAB_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.custom_variants = data.get('custom_variants', {})
                    self.expansion_budget = data.get('expansion_budget', 5000)
                    self.slop_window = data.get('slop_window', 15)
                    self.rare_word_bonus = data.get('rare_word_bonus', 0.5)
                    self.normalize_abbreviations = data.get('normalize_abbreviations', True)
                    self.rare_threshold = data.get('rare_threshold', 0.001)
                    self.candidate_limit = data.get('candidate_limit', 2000)
                    self.max_char_changes = data.get('max_char_changes', 1)
                    self.prefix_chars = data.get('prefix_chars', 1)

                    self.use_slop_window = data.get('use_slop_window', True)
                    self.use_rare_words = data.get('use_rare_words', True)
                    self.prefix_mode = data.get('prefix_mode', False)
                    self.use_order_tolerance = data.get('use_order_tolerance', False)
                    self.order_n = data.get('order_n', 4)
                    self.order_m = data.get('order_m', 4)
                    self.candidate_limit = max(1, min(self.candidate_limit, 10000))
                    self.max_char_changes = max(1, min(self.max_char_changes, 3))
                    self.prefix_chars = max(1, min(self.prefix_chars, 10))
            except Exception as e:
                LOGGER.warning("Failed to load Lab config: %s", e)

    def save(self):
        os.makedirs(Config.LAB_DIR, exist_ok=True)
        try:
            with open(Config.LAB_CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'custom_variants': self.custom_variants,
                    'expansion_budget': self.expansion_budget,
                    'slop_window': self.slop_window,
                    'rare_word_bonus': self.rare_word_bonus,
                    'normalize_abbreviations': self.normalize_abbreviations,
                    'rare_threshold': self.rare_threshold,
                    'candidate_limit': max(1, min(self.candidate_limit, 10000)),
                    'max_char_changes': max(1, min(self.max_char_changes, 3)),
                    'prefix_chars': max(1, min(self.prefix_chars, 10)),
                    'use_slop_window': self.use_slop_window,
                    'use_rare_words': self.use_rare_words,
                    'prefix_mode': self.prefix_mode,
                    'use_order_tolerance': self.use_order_tolerance,
                    'order_n': self.order_n,
                    'order_m': self.order_m
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

    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        self.settings = LabSettings()
        self.lab_index = None
        self.lab_searcher = None
        self._reload_lab_index()

    def _reload_lab_index(self):
        if os.path.exists(Config.LAB_INDEX_DIR):
            try:
                self.lab_index = tantivy.Index.open(Config.LAB_INDEX_DIR)
                self.lab_searcher = self.lab_index.searcher()
                return True
            except Exception as e:
                LAB_LOGGER.error("Failed to load Lab Index: %s", e)
        return False

    @staticmethod
    def lab_index_normalize(text):
        """Normalize text for Lab Index: Strip non-alphanumeric chars (keep Hebrew/English letters)."""
        # Remove specific punctuation chars that are common in these texts
        # Quotes (single/double), dashes, dots, etc.
        # We keep whitespace to separate words.
        return re.sub(r"[^\w\u0590-\u05FF\s\*\~]", "", text).replace('_', ' ').lower()

    def rebuild_lab_index(self, progress_callback=None):
        """Build the isolated Lab Index with normalized text."""
        LAB_LOGGER.info("Starting Lab Index rebuild...")

        # Validation
        if not os.path.exists(Config.FILE_V8):
            raise FileNotFoundError(tr("Input file not found: {}").format(Config.FILE_V8))

        # Ensure lab index dir exists and is empty
        if os.path.exists(Config.LAB_INDEX_DIR):
            try:
                shutil.rmtree(Config.LAB_INDEX_DIR)
            except Exception as e:
                LAB_LOGGER.error("Failed to clear old Lab Index: %s", e)
        os.makedirs(Config.LAB_INDEX_DIR, exist_ok=True)

        builder = tantivy.SchemaBuilder()
        builder.add_text_field("unique_id", stored=True)
        # Primary search field for Lab Mode Stage 1
        builder.add_text_field("text_normalized", stored=True, tokenizer_name="whitespace")
        # Store original header/shelfmark for result reconstruction
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        # Store full content just in case, though Stage 2 fetches from disk usually?
        # Plan says "Stage 2: Fetch full text". Usually we fetch from disk or main index.
        # But to be self-contained, let's store it or rely on unique_id.
        # We'll store it to avoid main index dependency if possible, but keeping it light is better.
        # Let's NOT store full original content here to save space, assuming we can get it via unique_id from main index
        # or via file read.
        # Actually, `SearchEngine._get_best_text_for_id` uses `content` field from main index.
        # `LabEngine` might need to access original text.
        # For simplicity/performance of Stage 2 (Python re-ranking), fetching from Tantivy doc store is fast.
        builder.add_text_field("content", stored=True)

        schema = builder.build()
        index = tantivy.Index(schema, path=Config.LAB_INDEX_DIR)
        writer = index.writer(heap_size=50_000_000)

        total_docs = 0

        def count_lines(fname):
            if not os.path.exists(fname): return 0
            with open(fname, 'r', encoding='utf-8') as f: return sum(1 for line in f)

        total_lines = count_lines(Config.FILE_V8) + count_lines(Config.FILE_V7)
        processed_lines = 0

        for fpath, label in [(Config.FILE_V8, "V0.8"), (Config.FILE_V7, "V0.7")]:
            if not os.path.exists(fpath):
                LAB_LOGGER.warning(f"File not found: {fpath}")
                continue

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

                            shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or "Unknown"

                            writer.add_document(tantivy.Document(
                                unique_id=str(cid),
                                text_normalized=norm_content,
                                content=original_content,
                                full_header=str(chead),
                                shelfmark=str(shelfmark)
                            ))
                            total_docs += 1

                        # Reset for new document
                        chead = line.replace("==>", "").replace("<==", "").strip() if label == "V0.8" else line
                        cid = self.meta_mgr.extract_unique_id(line)
                        ctext = [] # Explicit reset for new document
                    else:
                        ctext.append(line)

                    if progress_callback and processed_lines % 1000 == 0:
                        progress_callback(processed_lines, total_lines)

                # Last doc
                if cid and ctext:
                    original_content = "\n".join(ctext)
                    norm_content = self.lab_index_normalize(original_content)
                    shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or "Unknown"
                    writer.add_document(tantivy.Document(
                        unique_id=str(cid),
                        text_normalized=norm_content,
                        content=original_content,
                        full_header=str(chead),
                        shelfmark=str(shelfmark)
                    ))
                    total_docs += 1
                cid, chead, ctext = None, None, []

        writer.commit()
        LAB_LOGGER.info("Lab Index rebuild complete. %d docs.", total_docs)
        self._reload_lab_index()
        return total_docs

    def _get_doc_freq(self, term):
        if not self.lab_searcher: return 0
        try:
            # Tantivy-py 0.20+ signature uses index_reader usually, but check Searcher
            if hasattr(self.lab_searcher, 'doc_freq'):
                return self.lab_searcher.doc_freq("text_normalized", term)
            # Try index reader if available (not exposed directly in simple binding?)
            # Fallback: Perform a search (slower but safe)
            q = self.lab_index.parse_query(f'text_normalized:"{term}"', ["text_normalized"])
            return self.lab_searcher.search(q, 0).count
        except:
            return 0

    def _is_rare(self, term, total_docs):
        df = self._get_doc_freq(term)
        if total_docs == 0: return False
        return (df / total_docs) < self.settings.rare_threshold

    def _edit_distance_limit(self, s1, s2, max_dist):
        if s1 == s2:
            return 0
        if max_dist <= 0:
            return max_dist + 1
        len1 = len(s1)
        len2 = len(s2)
        if abs(len1 - len2) > max_dist:
            return max_dist + 1
        prev = list(range(len2 + 1))
        for i in range(1, len1 + 1):
            curr = [i] + [0] * len2
            min_row = curr[0]
            c1 = s1[i - 1]
            for j in range(1, len2 + 1):
                cost = 0 if c1 == s2[j - 1] else 1
                curr[j] = min(
                    prev[j] + 1,
                    curr[j - 1] + 1,
                    prev[j - 1] + cost
                )
                if curr[j] < min_row:
                    min_row = curr[j]
            if min_row > max_dist:
                return max_dist + 1
            prev = curr
        return prev[-1]

    def _variant_within_change_limit(self, term, variant):
        if term == variant:
            return True
        max_changes = max(1, min(self.settings.max_char_changes, 3))
        dist = self._edit_distance_limit(term, variant, max_changes)
        if max_changes == 1:
            return dist == 1
        return 0 < dist <= max_changes

    def budgeted_expansion(self, term):
        budget = self.settings.expansion_budget
        variants = []
        seen = set()

        def add_variant(v):
            if v in seen:
                return False
            if not self._variant_within_change_limit(term, v):
                return False
            seen.add(v)
            variants.append(v)
            return len(variants) >= budget

        add_variant(term)
        if len(variants) >= budget:
            return variants

        # Rank 1: Basic
        basic = self.var_mgr.get_variants(term, 'variants', limit=budget)
        for v in basic:
            if add_variant(v):
                return variants

        # Rank 2: Custom
        if term in self.settings.custom_variants:
            for v in self.settings.custom_variants[term]:
                if add_variant(v):
                    return variants

        # Rank 3: Extended
        extended = self.var_mgr.get_variants(term, 'variants_extended', limit=budget)
        for v in extended:
            if add_variant(v):
                return variants

        # Rank 4: Maximum
        maximum = self.var_mgr.get_variants(term, 'variants_maximum', limit=budget)
        for v in maximum:
            if add_variant(v):
                return variants

        return variants

    @staticmethod
    def lab_normalize(text):
        # Strips non-Hebrew characters but keeps spaces
        return re.sub(r"[^\u0590-\u05FF\s\*\~]", "", text)

    def _candidate_limit(self):
        return max(1, min(self.settings.candidate_limit, 10000))

    def _stage1_limit(self):
        return min(self._candidate_limit() * 2, 10000)

    def _prefix_term(self, term):
        prefix_len = max(1, min(self.settings.prefix_chars, 10))
        return term[:prefix_len] if len(term) > prefix_len else term

    def _term_matches(self, query_term, token):
        token_norm = self.lab_index_normalize(token)
        if isinstance(query_term, set):
            return token_norm in query_term
        if self.settings.prefix_mode:
            q_prefix = self._prefix_term(query_term)
            return token_norm.startswith(q_prefix)
        return self.soft_match(query_term, token)

    def _find_term_sequence(self, doc_tokens, query_terms, gap):
        if not query_terms:
            return None
        if len(query_terms) == 1:
            for idx, token in enumerate(doc_tokens):
                if self._term_matches(query_terms[0], token):
                    return [idx]
            return None

        max_distance = max(0, gap) + 1
        positions = []
        for term in query_terms:
            hits = [i for i, token in enumerate(doc_tokens) if self._term_matches(term, token)]
            if not hits:
                return None
            positions.append(hits)

        for start in positions[0]:
            seq = [start]
            prev = start
            valid = True
            for hits in positions[1:]:
                next_hits = [p for p in hits if prev < p <= prev + max_distance]
                if not next_hits:
                    valid = False
                    break
                prev = next_hits[0]
                seq.append(prev)
            if valid:
                return seq
        return None

    def _parse_lab_query(self, query_str):
        parser_cls = getattr(tantivy, "QueryParser", None)
        if parser_cls:
            parser = parser_cls.for_index(self.lab_index, ["text_normalized"])
            return parser.parse_query(query_str)
        return self.lab_index.parse_query(query_str, ["text_normalized"])

    def _get_term_boost(self, term, total_docs):
        df = self._get_doc_freq(term)
        if total_docs == 0:
            return 1
        ratio = df / total_docs
        if ratio <= self.settings.rare_threshold:
            return 10
        if ratio <= self.settings.rare_threshold * 5:
            return 6
        if ratio <= self.settings.rare_threshold * 20:
            return 3
        return 1

    def _min_should_match(self, term_count):
        if term_count <= 1:
            return 1
        if term_count <= 3:
            ratio = 0.5
        elif term_count <= 6:
            ratio = 0.4
        else:
            ratio = 0.3
        return max(1, math.ceil(term_count * ratio))

    def _matches_min_terms(self, doc_text, terms):
        if not terms:
            return 0
        doc_tokens = set(doc_text.split())
        matched = 0
        for term in terms:
            if isinstance(doc_tokens, set):
                if any(self._term_matches(term, token) for token in doc_tokens):
                    matched += 1
            else:
                if any(self._term_matches(term, token) for token in doc_tokens):
                    matched += 1
        return matched

    def soft_match(self, t1, t2):
        if t1.lower() == t2.lower(): return True
        # Normalize
        n1 = self.lab_normalize(t1).lower()
        n2 = self.lab_normalize(t2).lower()
        if n1 and n1 == n2: return True

        # Custom Variants (check normalized and raw)
        if t1 in self.settings.custom_variants:
            if t2 in self.settings.custom_variants[t1]: return True
        if n1 in self.settings.custom_variants:
            if n2 in self.settings.custom_variants[n1]: return True

        return False

    def lab_search(self, query_str, progress_callback=None, gap=0):
        """Execute Lab Mode Search (Two-Stage)."""
        if not self.lab_searcher:
            LAB_LOGGER.warning("Lab Index not loaded.")
            return []

        LAB_LOGGER.info(f"Stage 1 Search: {query_str}")
        start_time = time.time()

        # 1. Prepare Query
        # Normalize query string to list of terms using index normalization rules
        norm_query_str = self.lab_index_normalize(query_str)
        terms = norm_query_str.split()
        if not terms: return []
        unique_terms = list(dict.fromkeys(terms))
        min_should_match = self._min_should_match(len(unique_terms))

        # Budgeted Expansion
        expanded_terms = []
        match_terms = []
        total_docs = self.lab_searcher.num_docs
        rare_query_terms = set()

        for term in terms:
            if self._is_rare(term, total_docs):
                rare_query_terms.add(term)

            boost = self._get_term_boost(term, total_docs)
            if self.settings.prefix_mode:
                variants = [self._prefix_term(term)]
            else:
                variants = self.budgeted_expansion(term)
                # Normalize variants
                variants = [self.lab_index_normalize(v) for v in variants if v.strip()]

            if self.settings.prefix_mode:
                # Append * for prefix matching (Tantivy syntax)
                variants = [v + "*" for v in variants]

            expanded_terms.append((variants, boost, False))
            if self.settings.prefix_mode:
                match_terms.append(self._prefix_term(term))
            else:
                match_terms.append(set(variants))

        if len(rare_query_terms) >= 2:
            rare_clause = " AND ".join([f"\"{t}\"^12" for t in rare_query_terms])
            expanded_terms.append(([rare_clause], 1, True))

        # Build Boolean Query
        query_parts = []
        for group, boost, raw in expanded_terms:
            if not group:
                continue
            clean_group = []
            for t in group:
                if raw:
                    term = t
                elif '*' in t:
                    term = t
                else:
                    term = f'"{t}"'
                if boost > 1:
                    term = f"{term}^{boost}"
                clean_group.append(term)
            query_parts.append(f"({' OR '.join(clean_group)})")

        final_query = " OR ".join(query_parts)
        LAB_LOGGER.info("Stage 1 Raw Query: %s", final_query)
        LAB_LOGGER.debug(f"Stage 1 Query: {final_query}")

        try:
            t_query = self._parse_lab_query(final_query)
            res_obj = self.lab_searcher.search(t_query, self._stage1_limit())
        except Exception as e:
            LAB_LOGGER.error(f"Stage 1 failed: {e}")
            return []

        stage1_time = time.time() - start_time
        candidate_count = len(res_obj.hits)
        LAB_LOGGER.info(f"Stage 1 found {candidate_count} candidates in {stage1_time:.2f}s")

        # Stage 2: Re-ranking
        candidates = []
        for score, doc_addr in res_obj.hits:
            doc = self.lab_searcher.doc(doc_addr)
            doc_text_norm = doc['text_normalized'][0]
            if len(unique_terms) > 1:
                matched_terms = self._matches_min_terms(doc_text_norm, match_terms)
                if matched_terms < min_should_match:
                    continue
            candidates.append({
                'bm25': score,
                'content': doc['content'][0],
                'header': doc['full_header'][0],
                'shelfmark': doc['shelfmark'][0],
                'uid': doc['unique_id'][0]
            })

        results = []
        raw_terms = [t for t in query_str.split() if t.strip()]
        norm_terms = [self.lab_index_normalize(t) for t in raw_terms if t.strip()]

        # Pre-calculate rare terms for Stage 2 boosting
        # We use the normalized 'terms' set for rarity check, but raw_terms for matching logic order?
        # Actually, let's use the normalized terms for rarity.

        for i, cand in enumerate(candidates):
            if time.time() - start_time > 30:
                LAB_LOGGER.warning("Stage 2 Timeout Reached")
                break

            text = cand['content']
            tokens = text.split() # Simple whitespace tokenization

            if len(match_terms) >= 2:
                sequence = self._find_term_sequence(tokens, match_terms, gap)
                if not sequence:
                    continue
            else:
                sequence = None

            # Density & Order Analysis
            max_density = 0
            best_order_score = 0

            # Find all matches
            # Map token_index -> matching_query_term_index
            matches = [] # list of (doc_idx, query_idx)

            for doc_idx, token in enumerate(tokens):
                for q_idx, q_term in enumerate(match_terms):
                    if self._term_matches(q_term, token):
                        matches.append((doc_idx, q_idx))

            # Sliding Window for Density
            if matches and self.settings.use_slop_window:
                # matches is sorted by doc_idx
                # We want max matches in a window of size 'slop_window' tokens
                # Using 2 pointers
                left = 0
                window_matches = []
                for right in range(len(matches)):
                    curr_match = matches[right]
                    doc_idx_right = curr_match[0]

                    # Shrink left
                    while doc_idx_right - matches[left][0] > self.settings.slop_window:
                        left += 1

                    # Current window is matches[left : right+1]
                    window = matches[left : right+1]
                    density = len(set(m[1] for m in window))

                    if density > max_density:
                        max_density = density

                        # Calculate Order Score for this best window
                        q_indices = [m[1] for m in window]
                        order_score = 0
                        for idx1 in range(len(q_indices)):
                            for idx2 in range(idx1 + 1, len(q_indices)):
                                if q_indices[idx1] < q_indices[idx2]:
                                    order_score += 1
                        best_order_score = order_score

            # Rare Word Boost
            rare_score = 0
            if self.settings.use_rare_words:
                found_rare_count = 0
                for rare_term in rare_query_terms:
                    # Check if this rare term matches any token in doc
                    for token in tokens:
                        if self._term_matches(rare_term, token):
                            found_rare_count += 1
                            break # Count once per term
                rare_score = found_rare_count * self.settings.rare_word_bonus

            # Final Score
            final_score = (cand['bm25'] * 0.1) + (max_density * 0.6) + (best_order_score * 0.2) + (rare_score * 0.1)

            cand['final_score'] = final_score
            if sequence:
                match_indices = set(sequence)
                cand['snippet_data'] = self._snippet_html(
                    self._generate_snippet(cand['content'], matches, norm_terms, match_indices=match_indices)
                )
                cand['raw_file_hl'] = self._highlight_tokens(cand['content'], match_indices)
            else:
                match_indices = set(m[0] for m in matches)
                cand['snippet_data'] = self._snippet_html(
                    self._generate_snippet(cand['content'], matches, norm_terms, match_indices=match_indices)
                )
                cand['raw_file_hl'] = self._highlight_tokens(cand['content'], match_indices)
            results.append(cand)

        # Sort
        results.sort(key=lambda x: x['final_score'], reverse=True)

        stage2_time = time.time() - start_time - stage1_time
        LAB_LOGGER.info(
            "Stage 2 completed in %.2fs. Candidates: %d, Results: %d",
            stage2_time,
            candidate_count,
            len(results),
        )

        # Format for GUI (Pure Local Metadata Lookup)
        gui_results = []
        for r in results:
            # Use local lookup only to prevent lag
            sys_id, p_num = self.meta_mgr.parse_header_smart(r['header'])
            shelf, title = self.meta_mgr.get_meta_for_id(sys_id)

            meta = {
                'shelfmark': shelf or f"ID: {sys_id}",
                'title': title or "",
                'img': p_num,
                'source': "Lab",
                'id': sys_id
            }

            gui_results.append({
                'display': meta,
                'snippet': r['snippet_data'],
                'full_text': r['content'],
                'uid': r['uid'],
                'raw_header': r['header'],
                'raw_file_hl': r.get('raw_file_hl') or r['content'],
                'highlight_pattern': None
            })

        return gui_results

    def _highlight_tokens(self, text, match_indices):
        if not match_indices:
            return text
        tokens = text.split()
        out = []
        for idx, token in enumerate(tokens):
            if idx in match_indices:
                out.append(f"*{token}*")
            else:
                out.append(token)
        return " ".join(out)

    def _snippet_html(self, snippet):
        if not snippet:
            return snippet
        return re.sub(r'\*(.*?)\*', r"<b style='color:red;'>\1</b>", snippet)

    def _generate_snippet(self, text, matches, query_terms, match_indices=None):
        """Simple snippet generator for Lab Search."""
        if not matches: return text[:300]
        # Find window with most matches
        # We already computed windows? Re-use logic or simple center on first best match
        # Let's just take the first match area
        match_idx = matches[0][0] # doc_idx of first match
        tokens = text.split()
        start = max(0, match_idx - 10)
        end = min(len(tokens), match_idx + 20)

        # Highlight
        out = []
        if match_indices is None:
            match_indices = set(m[0] for m in matches)
        for i in range(start, end):
            t = tokens[i]
            if i in match_indices:
                out.append(f"*{t}*")
            else:
                out.append(t)

        return "..." + " ".join(out) + "..."

    def _extract_rare_terms(self, text, total_docs):
        """Identify rare terms in the input text to use as search anchors."""
        norm_text = self.lab_index_normalize(text)
        tokens = list(set(norm_text.split())) # Unique tokens

        rare_terms = []

        # Rule 1: Strict (Threshold from settings, len >= 3)
        threshold_strict = max(1, total_docs * self.settings.rare_threshold)
        for t in tokens:
            if len(t) >= 3 and self._get_doc_freq(t) < threshold_strict:
                rare_terms.append(t)

        # Fallback: Relaxed (5x Threshold, len >= 2)
        if len(rare_terms) < 5:
            rare_terms = [] # Reset to avoid duplicates or mixing strictly
            threshold_relaxed = max(1, total_docs * (self.settings.rare_threshold * 5))
            for t in tokens:
                if len(t) >= 2 and self._get_doc_freq(t) < threshold_relaxed:
                    rare_terms.append(t)

        return rare_terms

    def lab_composition_search(self, full_text, progress_callback=None, chunk_size=None):
        """Execute Broad-to-Narrow Composition Search."""
        if not self.lab_searcher: return {'main': [], 'filtered': []}

        LAB_LOGGER.info("Starting Lab Composition Search...")
        start_time = time.time()

        # 1. Broad Filter (Candidate Generation)
        total_docs = self.lab_searcher.num_docs
        rare_terms = self._extract_rare_terms(full_text, total_docs)

        if not rare_terms:
            LAB_LOGGER.warning("No rare terms found for anchoring.")
            # Fallback? Or just return nothing?
            # User instruction was to rely on rare terms.
            return {'main': [], 'filtered': []}

        LAB_LOGGER.info(f"Using {len(rare_terms)} rare terms for candidate generation.")

        # Build Query
        # Using a huge OR might be slow if too many terms. Cap it?
        # Let's cap at 150 terms
        query_terms = rare_terms[:150]
        if self.settings.prefix_mode:
            query_str = " OR ".join([f"{self._prefix_term(t)}*" for t in query_terms])
        else:
            query_str = " OR ".join([f'"{t}"' for t in query_terms])

        try:
            LAB_LOGGER.info("Stage 1 Raw Query: %s", query_str)
            q = self._parse_lab_query(query_str)
            res = self.lab_searcher.search(q, self._stage1_limit())
        except Exception as e:
            LAB_LOGGER.error(f"Candidate generation failed: {e}")
            return {'main': [], 'filtered': []}

        # Retry with Fuzzy if 0 results
        if res.count == 0:
            LAB_LOGGER.info("Stage 1 yielded 0 results. Retrying with Fuzzy (~1)...")
            try:
                # Add fuzzy ~1 to each term
                fuzzy_terms = []
                for t in query_terms:
                    # Don't add fuzzy to wildcard terms
                    if '*' in t: fuzzy_terms.append(t)
                    else: fuzzy_terms.append(f'"{t}"~1')

                fuzzy_query = " OR ".join(fuzzy_terms)
                LAB_LOGGER.info("Stage 1 Raw Query (Fuzzy): %s", fuzzy_query)
                q = self._parse_lab_query(fuzzy_query)
                res = self.lab_searcher.search(q, self._stage1_limit())
                LAB_LOGGER.info(f"Fuzzy retry found {res.count} candidates.")
            except Exception as e:
                LAB_LOGGER.error(f"Fuzzy retry failed: {e}")

        candidates = []
        for score, doc_addr in res.hits:
            doc = self.lab_searcher.doc(doc_addr)
            candidates.append({
                'content': doc['content'][0],
                'header': doc['full_header'][0],
                'shelfmark': doc['shelfmark'][0],
                'uid': doc['unique_id'][0],
                'src': 'Lab' # Source label
            })

        stage1_time = time.time() - start_time
        LAB_LOGGER.info(f"Found {len(candidates)} candidates in {stage1_time:.2f}s.")

        # 2. Narrow Scan (Python)
        # Prepare Input Chunks
        norm_input = self.lab_index_normalize(full_text)
        input_tokens = norm_input.split()

        # Determine chunk size based on settings
        if chunk_size is not None and chunk_size > 0:
            chunk_size = int(chunk_size)
        elif self.settings.use_order_tolerance:
            chunk_size = self.settings.order_n + self.settings.order_m
        else:
            chunk_size = self.settings.slop_window

        # "Slide Input Chunks over Candidate Doc"
        step = max(1, chunk_size // 2)

        input_chunks = []
        for i in range(0, len(input_tokens), step):
            chunk = input_tokens[i : i + chunk_size]
            if len(chunk) < 2: continue # Skip tiny chunks
            input_chunks.append({
                'tokens': chunk,
                'start_idx': i,
                'end_idx': i + len(chunk)
            })

        final_items = []

        for idx, cand in enumerate(candidates):
            if progress_callback and idx % 10 == 0:
                progress_callback(idx, len(candidates))

            doc_text = cand['content']
            doc_norm = self.lab_index_normalize(doc_text)
            doc_tokens = doc_norm.split()

            # Map doc tokens for fast lookup?
            # Or scan doc for each chunk?
            # Since we have "soft_match", exact map is hard.
            # But we can assume soft_match is mostly equality or known variants.
            # Optimization:
            #   Scan doc once, find positions of all input tokens (roughly).
            #   Then check density.

            # Simplified Narrow Scan:
            # For each input chunk, verify if a "soft match" cluster exists in doc.

            doc_matches = [] # (start, end, score, snippet)
            total_score = 0

            # Pre-filter: Check if chunk terms exist in doc at all to skip expensive scan
            doc_token_set = set(doc_tokens)

            for chunk in input_chunks:
                # Quick check: do at least 50% of chunk tokens exist in doc_set? (normalized)
                if self.settings.prefix_mode:
                    present = sum(
                        1 for t in chunk['tokens']
                        if any(token.startswith(self._prefix_term(t)) for token in doc_token_set)
                    )
                else:
                    present = sum(1 for t in chunk['tokens'] if t in doc_token_set)
                if present < len(chunk['tokens']) * 0.4: continue

                # Detailed Window Scan on Doc
                # Slide window over doc tokens
                # Window size = len(chunk) + padding?
                # User said: "Sliding Window... soft match... density"

                best_chunk_score = 0
                best_window_idx = -1

                # We scan the doc for this chunk
                # Optimized: Find all positions of the first few rare tokens of the chunk?
                # Brute force sliding window over doc is O(N*M) where N=doc_len, M=chunk_len.
                # doc_len ~ 500, chunk_len ~ 15. 500*15 = 7500 ops.
                # If 100 chunks * 2000 docs * 7500 = 1.5 Billion ops. Too slow.

                # Better approach:
                # Find all token matches first.
                # match_positions = list of (doc_token_index, input_token_index_in_chunk)

                match_positions = []
                for dt_i, dt in enumerate(doc_tokens):
                    for ct_i, ct in enumerate(chunk['tokens']):
                        if self._term_matches(ct, dt):
                            match_positions.append((dt_i, ct_i))

                if not match_positions: continue

                # Find clusters in match_positions
                # A cluster is a set of matches where doc_indices are close and chunk_indices are consistent
                # Use a sliding window over match_positions?

                # Sort by doc_index
                match_positions.sort(key=lambda x: x[0])

                # Sliding window of size 'chunk_size' (in doc terms)
                left = 0
                for right in range(len(match_positions)):
                    curr = match_positions[right]

                    # Shrink: Window size is chunk length (N+M)
                    # Use a slightly relaxed window for Slop (1.5x) but strict for Order Tol?
                    # Let's keep 1.5x as "Slop" factor generally, unless strict mode requested.
                    max_window_len = len(chunk['tokens']) if self.settings.use_order_tolerance else (len(chunk['tokens']) * 1.5)

                    while curr[0] - match_positions[left][0] > max_window_len:
                        left += 1

                    window = match_positions[left : right+1]

                    # Score this window
                    covered_chunk_indices = set(m[1] for m in window)
                    count_match = len(covered_chunk_indices)

                    valid_hit = False
                    score = 0

                    if self.settings.use_order_tolerance:
                        # Logic: N out of N+M matches (where N+M is chunk size)
                        # We need count_match >= order_n
                        if count_match >= self.settings.order_n:
                            # And check Order (Longest Increasing Subsequence?)
                            # User said "finding n words in the same order"
                            # We check LIS of chunk indices
                            q_indices = [m[1] for m in window]
                            if not q_indices: continue

                            # Calculate LIS length
                            tails = []
                            for x in q_indices:
                                idx = bisect.bisect_left(tails, x)
                                if idx < len(tails): tails[idx] = x
                                else: tails.append(x)

                            if len(tails) >= self.settings.order_n:
                                valid_hit = True
                                score = len(tails) * 2 # Boost ordered matches

                    else:
                        # Standard Density Logic (Default or Slop Window mode)
                        coverage = count_match / len(chunk['tokens'])
                        if coverage > 0.5: # Threshold 50%
                            valid_hit = True
                            score = count_match

                    if valid_hit:
                        if score > best_chunk_score:
                            best_chunk_score = score
                            best_window_idx = (match_positions[left][0], curr[0])

                if best_chunk_score > 0 and best_window_idx != -1:
                    total_score += best_chunk_score
                    s, e = best_window_idx
                    # Store match data including the source text chunk
                    chunk_text = " ".join(chunk['tokens'])
                    chunk_highlight = " ".join([f"*{t}*" for t in chunk['tokens']])
                    doc_matches.append((s, e, best_chunk_score, chunk_text, chunk_highlight))

            if total_score > 0:
                # Format snippet
                hl_snippet = "..."
                source_contexts = []

                if doc_matches:
                    doc_matches.sort(key=lambda x: x[2], reverse=True) # best score first

                    # Collect source contexts (up to 3 best)
                    for m in doc_matches[:3]:
                        if m[4] not in source_contexts:
                            source_contexts.append(m[4])

                    # Generate highlighted snippet for the BEST match
                    s, e, _, _ = doc_matches[0]

                    orig_tokens = doc_text.split()

                    # Expand context
                    start = max(0, s - 10)
                    end = min(len(orig_tokens), e + 10)

                    # Build snippet with highlights
                    out = []
                    # We highlight tokens in the match window
                    for i in range(start, end):
                        t = orig_tokens[i]
                        # If inside the match window, wrap in *...*
                        if s <= i <= e:
                            out.append(f"*{t}*")
                        else:
                            out.append(t)

                    hl_snippet = "... " + " ".join(out) + " ..."

                final_items.append({
                    'score': total_score,
                    'uid': cand['uid'],
                    'raw_header': cand['header'],
                    'src_lbl': 'Lab',
                    'source_ctx': " ... ".join(source_contexts),
                    'text': hl_snippet,
                    'highlight_pattern': ""
                })

        final_items.sort(key=lambda x: x['score'], reverse=True)
        stage2_time = time.time() - start_time - stage1_time
        LAB_LOGGER.info(
            "Stage 2 completed in %.2fs. Candidates: %d, Results: %d",
            stage2_time,
            len(candidates),
            len(final_items),
        )
        return {'main': final_items, 'filtered': []}


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
