"""Core search, indexing, metadata, and AI helpers for the Genizah project."""

# -*- coding: utf-8 -*-
# genizah_core.py
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
from typing import Mapping
import itertools
import json

from genizah_translations import TRANSLATIONS

try:
    import tantivy
except ImportError:
    raise ImportError(tr("Tantivy library missing. Please install it."))

try:
    import google.generativeai as genai
    HAS_GENAI = True
except ImportError:
    HAS_GENAI = False

# ==============================================================================
#  CONFIG CLASS (EXE Compatible)
# ==============================================================================
class Config:
    """Static paths and limits used by the application and by bundled binaries."""
    # 1. Base Directory (Where the EXE/Script is)
    if getattr(sys, 'frozen', False):
        BASE_DIR = os.path.dirname(sys.executable)
    else:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))

    # 2. Input Files (Must be next to EXE)
    FILE_V8 = os.path.join(BASE_DIR, "Transcriptions.txt")
    FILE_V7 = os.path.join(BASE_DIR, "AllGenizah_OLD.txt")
    INPUT_FILE = os.path.join(BASE_DIR, "input.txt")
    RESULTS_DIR = os.path.join(BASE_DIR, "Results")
    REPORTS_DIR = os.path.join(BASE_DIR, "Reports")
    LIBRARIES_CSV = os.path.join(BASE_DIR, "libraries.csv")

    # 3. User Data Directory (Index, Caches)
    INDEX_DIR = os.path.join(os.path.expanduser("~"), "Genizah_Tantivy_Index")
    IMAGE_CACHE_DIR = os.path.join(INDEX_DIR, "images_cache")
    
    # Generated Files locations
    CACHE_META = os.path.join(INDEX_DIR, "metadata_cache.pkl")
    CACHE_NLI = os.path.join(INDEX_DIR, "nli_cache.pkl")
    CONFIG_FILE = os.path.join(INDEX_DIR, "config.pkl")
    LANGUAGE_FILE = os.path.join(INDEX_DIR, "lang.pkl")
    BROWSE_MAP = os.path.join(INDEX_DIR, "browse_map.pkl")
    
    # Settings
    TANTIVY_CLAUSE_LIMIT = 5000
    SEARCH_LIMIT = 5000
    VARIANT_GEN_LIMIT = 5000
    REGEX_VARIANTS_LIMIT = 3000
    WORD_TOKEN_PATTERN = r'[\w\u0590-\u05FF\']+'

def load_language():
    """Load language preference. Returns 'en' or 'he'."""
    try:
        if os.path.exists(Config.LANGUAGE_FILE):
            with open(Config.LANGUAGE_FILE, 'rb') as f:
                return pickle.load(f)
    except: pass
    return 'en'

def save_language(lang):
    """Save language preference."""
    try:
        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.LANGUAGE_FILE, 'wb') as f:
            pickle.dump(lang, f)
    except: pass

# Global language state
CURRENT_LANG = load_language()

def tr(text):
    """Translate text if current language is Hebrew."""
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text

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
            try: os.makedirs(Config.INDEX_DIR)
            except: pass
            
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
            except: pass

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
            try: os.makedirs(Config.INDEX_DIR)
            except: pass

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
            except: pass
        if os.path.exists(Config.CACHE_META):
            try:
                with open(Config.CACHE_META, 'rb') as f: self.meta_map = pickle.load(f)
            except: pass

    def _load_heavy_caches_bg(self):
        self._load_csv_bank()

    def _load_csv_bank(self):
        """Load the massive CSV file into memory for instant lookup."""
        if not os.path.exists(Config.LIBRARIES_CSV):
            return

        import csv
        try:
            with open(Config.LIBRARIES_CSV, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f, delimiter=',')
                next(reader, None) # Skip header

                for row in reader:
                    if not row or len(row) < 2: continue
                    # Format: system_number | call_numbers | ... | titles
                    sys_id = row[0].strip()

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
        except Exception as e:
            print(f"Error loading CSV bank: {e}")

    def get_meta_for_id(self, sys_id):
        """Get shelfmark and title from ANY source (CSV > Cache > Bank)."""
        shelf = "Unknown"
        title = ""

        # 1. Check CSV (Fastest & Most reliable for basic info)
        # Note: Accessing self.csv_bank is generally thread-safe for reading in Python
        # (GIL handles atomic dict reads), even if being populated.
        if sys_id in self.csv_bank:
            return self.csv_bank[sys_id]['shelfmark'], self.csv_bank[sys_id]['title']

        # 2. Check NLI Cache (If we fetched it before)
        if sys_id in self.nli_cache:
            m = self.nli_cache[sys_id]
            return m.get('shelfmark', 'Unknown'), m.get('title', '')

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
        except: pass

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
        except: pass

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
        
    def _pick_working_thumbnail(self, base, size, session):
        candidates = [
            f"{base}/full/!{size},{size}/0/default.jpg",
            f"{base}/full/!{max(size, 600)},{max(size, 600)}/0/default.jpg",
            f"{base}/full/full/0/default.jpg",
            f"{base}/full/max/0/default.jpg",
        ]

        for url in candidates:
            if self._url_returns_image(session, url):
                return url
        return None

    def _url_returns_image(self, session, url):
        def _is_image(resp):
            ctype = (resp.headers.get("content-type") or "").lower()
            return resp.status_code == 200 and "image" in ctype

        try:
            head = session.head(url, timeout=5, allow_redirects=True)
            if _is_image(head):
                head.close()
                return True
            head.close()

            # If HEAD is not supported or inconclusive, try GET
            if head.status_code not in (200, 405):
                return False
        except Exception:
            pass

        try:
            resp = session.get(url, timeout=5, allow_redirects=True, stream=True)
            ok = _is_image(resp)
            resp.close()
            return ok
        except Exception:
            return False

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
        builder.add_text_field("nospaces", stored=True) # SWIFT Support
        builder.add_text_field("source", stored=True)
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        schema = builder.build()
        
        index = tantivy.Index(schema, path=db_path)
        writer = index.writer(heap_size=100_000_000)
        
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
                            full_c = "\n".join(ctext)
                            # Strip all whitespace/punctuation for SWIFT
                            clean_c = re.sub(r'[^\w\u0590-\u05FF]', '', full_c)

                            shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or self.meta_mgr.meta_map.get(cid, "")
                            writer.add_document(tantivy.Document(
                                unique_id=str(cid), content=full_c, nospaces=clean_c, source=str(label),
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
                    full_c = " ".join(ctext)
                    clean_c = re.sub(r'[^\w\u0590-\u05FF]', '', full_c)

                    shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or self.meta_mgr.meta_map.get(cid, "")
                    writer.add_document(tantivy.Document(
                        unique_id=str(cid), content=full_c, nospaces=clean_c, source=str(label),
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
            except: pass
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
                    # This prevents "דא" becoming "ר" and matching everything in the universe.
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
            # It ensures "וידא" matches before "דא".
            unique_vars = sorted(list(set(vars_list)), key=len, reverse=True)
            
            # 4. Escape special chars
            escaped = [re.escape(v) for v in unique_vars]
            
            # 5. Simple Group (Removed strict Lookbehind/Lookahead)
            # This allows finding "והמילה" even if searching "מילה"
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
        
        # Grab raw snippet
        snippet = text[start:end]
        matched_text = m.group(0)
        
        # If showing in table (HTML), verify valid HTML and remove newlines for compactness
        if not for_file:
            # Clean newlines for table display so rows don't explode
            snippet_clean = snippet.replace('\n', ' ') 
            match_clean = matched_text.replace('\n', ' ')
            return snippet_clean.replace(match_clean, f"<b style='color:red;'>{match_clean}</b>")
        
        # If for export file, keep newlines or mark them
        return snippet.replace(matched_text, f"*{matched_text}*")

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
        except Exception: return []

        hits = res_obj.hits if hasattr(res_obj, 'hits') else res_obj
        total_hits = len(hits)
        results = []
        
        for i, (score, doc_addr) in enumerate(hits):
            if progress_callback and i % 50 == 0: progress_callback(i, total_hits)
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
            except: pass
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

                        # Decide which dict to use
                        # If the chunk is filtered, we add it to the filtered results.
                        # Note: A document might match some filtered chunks and some valid chunks.
                        # For now, if ANY matched chunk is filtered, does it taint the whole doc?
                        # Or do we separate by match?
                        # The user requirement: "All text where these words are found will be filtered".
                        # So if the *chunk* matches the filter text, this specific hit is filtered.
                        # If a doc has ONLY filtered hits, it goes to filtered list.
                        # If a doc has mixed hits... probably safer to split the *matches* or just classify the doc?
                        # Let's say: we accumulate hits. At the end, if a doc has significant filtered content, maybe move it?
                        # Simplest approach: Segregate by chunk.

                        rec = doc_hits_filtered[uid] if is_filtered else doc_hits_main[uid]

                        rec['head'] = doc['full_header'][0]
                        rec['src'] = doc['source'][0]
                        rec['content'] = content
                        rec['matches'].append(regex.search(content).span())
                        rec['src_indices'].update(range(i, i + chunk_size))
                        rec['patterns'].add(regex.pattern)
            except: pass

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

        # Post-processing: If a UID appears in both, usually it means different chunks matched.
        # We can present it in both, or prioritize Main?
        # If the user wants to filter out "known texts", appearing in Main implies there's *also* unknown content?
        # Or should we be strict? "If found in this text... filtered".
        # If I found a match that IS in the filter text, that match is filtered.
        # If I found another match in the same doc that is NOT in filter text, that match stays in Main.
        # So separated by matches is correct.

        return {'main': main_list, 'filtered': filtered_list}

    def search_composition_swift(self, source_text, threshold=5, progress_callback=None):
        """
        Hybrid Tantivy/Python SWIFT Implementation.
        1. Clean source text and generate q-grams (seeds).
        2. Fetch candidate docs from Tantivy containing these seeds.
        3. Python: 'Parallelogram Filter' - find diagonals in (source_pos, doc_pos) grid.
        """
        if not self.searcher: return {'main': [], 'filtered': []}

        # 0. Check Index Compatibility
        try:
            # Try to access 'nospaces' field in a dummy query to see if schema matches
            # or just check if the field exists in the searcher/index wrapper
            # Since tantivy-py doesn't expose schema easily on searcher, we try a query
            q = self.index.parse_query('nospaces:"test"', ["nospaces"])
        except Exception:
             raise Exception(tr("Index outdated. Please rebuild index to use SWIFT mode."))

        # 1. Preprocessing (Seeds)
        # Clean text
        clean_source = re.sub(r'[^\w\u0590-\u05FF]', '', source_text)
        if len(clean_source) < 10: return {'main': [], 'filtered': []}

        seed_len = 5
        step = 3 # Sample every 3rd seed
        seeds = []
        # specific_seeds map: seed_str -> list of [positions_in_clean_source]
        seed_map = defaultdict(list)

        for i in range(0, len(clean_source) - seed_len + 1, 1): # Index all positions for map
            s = clean_source[i : i + seed_len]
            seed_map[s].append(i)

        # Select query seeds (sampling)
        query_seeds = list(seed_map.keys())[::step]
        if not query_seeds: return {'main': [], 'filtered': []}

        # 2. Querying (Batching to avoid clause limits)
        candidate_docs = set()

        # Max boolean clauses is usually 1024 or 4096. Let's be safe with batches of 500.
        batch_size = 500
        total_batches = (len(query_seeds) + batch_size - 1) // batch_size

        for i in range(0, len(query_seeds), batch_size):
            if progress_callback:
                 progress_callback(i, len(query_seeds) * 2) # Phase 1 is querying

            batch = query_seeds[i : i + batch_size]
            # Construct OR query: nospaces:"SEED1" OR nospaces:"SEED2"...
            # Use raw query string for simplicity
            parts = [f'"{s}"' for s in batch]
            q_str = " OR ".join(parts)

            try:
                # We query 'nospaces' field
                q = self.index.parse_query(q_str, ["nospaces"])
                # We need enough top docs. SWIFT is high recall.
                # If we have many seeds, we might match MANY docs.
                # But typical composition search expects specific manuscripts.
                res = self.searcher.search(q, 5000)
                for score, doc_addr in res.hits:
                    candidate_docs.add(doc_addr)
            except: pass

        # 3. Parallelogram Filter (Python)
        # We need to process each candidate doc
        results = []
        total_candidates = len(candidate_docs)

        # Cache regex for finding seeds in doc content
        # Actually, brute force find in string is fast enough in Python for limited docs

        for idx, doc_addr in enumerate(candidate_docs):
            if progress_callback and idx % 10 == 0:
                 # Phase 2 is filtering (mapped to second half of progress)
                 progress_callback(len(query_seeds) + int((idx / total_candidates) * len(query_seeds)), len(query_seeds) * 2)

            doc = self.searcher.doc(doc_addr)
            doc_uid = doc['unique_id'][0]

            # Get nospaces content (Must exist if schema check passed)
            try:
                doc_clean = doc['nospaces'][0]
            except IndexError:
                # Should not happen if index is rebuilt
                continue

            if not doc_clean: continue

            # Find hits: (source_pos, doc_pos)
            # We iterate over the SEEDS found in this document.
            # Optimization: Instead of searching all source seeds in doc,
            # we can iterate the doc seeds (if doc is small) or source seeds (if source is small).
            # Usually Source is smaller than the whole Genizah but larger than a single fragment?
            # No, Source is the input text (e.g. a chapter of Mishnah). Fragment is small.
            # So iterate fragment n-grams.

            hits = [] # List of (src_pos, doc_pos)

            # Sliding window over DOC to find matching seeds
            # Only check seeds that exist in our source map
            doc_len = len(doc_clean)
            if doc_len < seed_len: continue

            for d_pos in range(0, doc_len - seed_len + 1):
                gram = doc_clean[d_pos : d_pos + seed_len]
                if gram in seed_map:
                    # Retrieve all source positions for this gram
                    for s_pos in seed_map[gram]:
                        hits.append((s_pos, d_pos))

            if not hits: continue

            # Diagonal Clustering
            # We look for hits where (d_pos - s_pos) is roughly constant.
            # We bucket them by diff.
            # Allow variance of a few chars? The paper suggests strict diagonals or fuzzy.
            # Simple approach: Bucket by (d_pos - s_pos) // tolerance
            # But Hebrew SWIFT Guide implies strict Parallelogram or "approximately constant".
            # Let's use a tolerance of +/- 20 chars drift?
            # Or just strict binning with merging.

            # Let's simple bin by exact difference first.
            diagonals = defaultdict(list)
            for s, d in hits:
                diff = d - s
                diagonals[diff].append((s, d))

            # Filter clusters
            valid_clusters = []

            # Tolerance merging:
            # If we have diff=500 with 3 hits and diff=502 with 3 hits, they are likely same cluster.
            # Sort keys (diffs)
            sorted_diffs = sorted(diagonals.keys())
            if not sorted_diffs: continue

            current_cluster_hits = []
            last_diff = sorted_diffs[0]

            # We group diffs that are close (e.g. within 10 chars)
            diff_tolerance = 15

            # We need to flatten the map to iterate easily
            # List of (diff, hit_list)

            # Grouping Logic:
            # Iterate through sorted diffs. If diff is close to last, merge.

            merged_groups = []
            if sorted_diffs:
                curr_group = diagonals[sorted_diffs[0]][:]
                curr_diff_ref = sorted_diffs[0]

                for diff in sorted_diffs[1:]:
                    if diff - curr_diff_ref <= diff_tolerance:
                        curr_group.extend(diagonals[diff])
                    else:
                        merged_groups.append(curr_group)
                        curr_group = diagonals[diff][:]
                        curr_diff_ref = diff
                merged_groups.append(curr_group)

            # Check Threshold
            final_matches = []
            for group in merged_groups:
                # Group is list of (s, d).
                # We need count of UNIQUE seeds involved? Or just count?
                # Using unique source positions prevents counting the same word multiple times if it appears twice in doc closeby?
                # Actually, we want to know if we covered enough length.
                # Threshold is "Chunk" (e.g. 5).
                if len(group) >= threshold:
                    # Sort by source pos
                    group.sort(key=lambda x: x[0])

                    # Determine range in clean strings
                    s_start = group[0][0]
                    s_end = group[-1][0] + seed_len
                    d_start = group[0][1]
                    d_end = group[-1][1] + seed_len

                    final_matches.append({
                        'score': len(group),
                        'clean_range': (d_start, d_end),
                        'src_range': (s_start, s_end)
                    })

            if not final_matches: continue

            # Construct Result Item
            # We need to map clean_range back to original text content
            full_content = doc['content'][0]

            # Helper to map clean index to original index
            # This is expensive if we do it char by char.
            # Heuristic: The ratio of length is roughly constant? No.
            # Robust way: Re-scan original text to find non-whitespace chars.

            # Function to find start/end in original text given clean indices
            # We iterate original text, counting valid chars.

            def get_orig_indices(text, start_clean, end_clean):
                orig_len = len(text)
                c_idx = 0
                o_start = 0
                o_end = 0
                found_start = False

                for i, char in enumerate(text):
                    # Check if char is valid (was kept in clean)
                    if re.match(r'[\w\u0590-\u05FF]', char):
                        if c_idx == start_clean:
                            o_start = i
                            found_start = True
                        c_idx += 1
                        if c_idx == end_clean:
                            o_end = i + 1 # Include this char
                            break
                if not found_start: return 0, 0
                if o_end == 0: o_end = len(text)
                return o_start, o_end

            # Collect matches in this doc
            combined_snips = []
            best_score = 0

            # Extract distinct words for highlighting
            found_words = set()

            for m in final_matches:
                best_score = max(best_score, m['score'])
                d_s, d_e = m['clean_range']

                # Get snippet from clean to find words?
                # Better: Get snippet from original.
                o_s, o_e = get_orig_indices(full_content, d_s, d_e)

                # Context
                ctx_s = max(0, o_s - 50)
                ctx_e = min(len(full_content), o_e + 50)

                match_text = full_content[o_s:o_e]
                # Extract words for highlighter (simple split)
                for w in re.split(r'[^\w\u0590-\u05FF]+', match_text):
                    if len(w) > 2: found_words.add(w)

                snippet = full_content[ctx_s:o_s] + "*" + match_text + "*" + full_content[o_e:ctx_e]
                combined_snips.append(snippet)

            # Generate Regex Pattern for highlighting
            # (word1|word2|...)
            hl_pattern = ""
            if found_words:
                 # Escape words
                 esc_words = [re.escape(w) for w in found_words]
                 hl_pattern = "|".join(esc_words)

            results.append({
                'score': best_score,
                'uid': doc_uid,
                'raw_header': doc['full_header'][0],
                'src_lbl': doc['source'][0],
                'source_ctx': "", # We could extract source context similarly if needed
                'text': "\n...\n".join(combined_snips),
                'highlight_pattern': hl_pattern
            })

        results.sort(key=lambda x: x['score'], reverse=True)
        return {'main': results, 'filtered': []}

    def group_composition_results(self, items, threshold=5, progress_callback=None):
        ids = [self.meta_mgr.parse_header_smart(i['raw_header'])[0] for i in items]
        self.meta_mgr.batch_fetch_shelfmarks([x for x in ids if x])

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
        except: pass
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
                full_content.append({
                    'p_num': p['p_num'],
                    'text': text,
                    'uid': p['uid']
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