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
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import itertools
import json

try:
    import tantivy
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")

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
    METADATA_BANK = os.path.join(BASE_DIR, "metadata_bank.pkl")

    # 3. User Data Directory (Index, Caches)
    INDEX_DIR = os.path.join(os.path.expanduser("~"), "Genizah_Tantivy_Index")
    
    # Generated Files locations
    CACHE_META = os.path.join(INDEX_DIR, "metadata_cache.pkl")
    CACHE_NLI = os.path.join(INDEX_DIR, "nli_cache.pkl")
    CONFIG_FILE = os.path.join(INDEX_DIR, "config.pkl")
    BROWSE_MAP = os.path.join(INDEX_DIR, "browse_map.pkl")
    
    # Settings
    TANTIVY_CLAUSE_LIMIT = 100
    SEARCH_LIMIT = 5000
    VARIANT_GEN_LIMIT = 5000
    REGEX_VARIANTS_LIMIT = 3000
    WORD_TOKEN_PATTERN = r'[\w\u0590-\u05FF\']+'

# ==============================================================================
#  AI MANAGER
# ==============================================================================
class AIManager:
    """Manage Gemini configuration, key persistence, and prompt sessions."""
    def __init__(self):
        self.api_key = ""
        self.chat_history = []
        self.model = None
        self.chat = None
        
        # Ensure dir exists
        if not os.path.exists(Config.INDEX_DIR):
            try: os.makedirs(Config.INDEX_DIR)
            except: pass
            
        if os.path.exists(Config.CONFIG_FILE):
            try:
                with open(Config.CONFIG_FILE, 'rb') as f:
                    cfg = pickle.load(f)
                    self.api_key = cfg.get('gemini_key', '')
            except: pass

    def save_key(self, key):
        self.api_key = key.strip()
        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.CONFIG_FILE, 'wb') as f:
            pickle.dump({'gemini_key': self.api_key}, f)

    def init_session(self):
        if not HAS_GENAI: return "Error: 'google-generativeai' library missing."
        if not self.api_key: return "Error: Missing API Key."
        
        try:
            genai.configure(api_key=self.api_key)
            self.model = genai.GenerativeModel('gemini-2.0-flash')
            
            sys_inst = """You are an expert in Regex for Hebrew manuscripts (Cairo Genizah). 
            Your goal is to help the user construct Python Regex patterns.
            
            IMPORTANT RULES:
            1. Do NOT use \\w. Instead, use [\\u0590-\\u05FF"] to match Hebrew letters and Geresh.
            2. For "word starting with X", use \\bX...
            3. For spaces, use \\s+.
            4. Output format MUST be strictly JSON: {"regex": "THE_PATTERN", "explanation": "Brief explanation"}.
            5. Do not include markdown formatting like ```json.
            """
            
            self.chat = self.model.start_chat(history=[
                {"role": "user", "parts": [sys_inst]},
                {"role": "model", "parts": ["Understood. I will provide JSON output with robust Hebrew regex."]}
            ])
            return None
        except Exception as e:
            return str(e)

    def send_prompt(self, user_text):
        if not self.chat:
            err = self.init_session()
            if err: return None, err
            
        try:
            response = self.chat.send_message(user_text)
            clean = response.text.strip().replace('```json', '').replace('```', '').strip()
            data = json.loads(clean)
            return data, None
        except Exception as e:
            return None, str(e)

# ==============================================================================
#  VARIANTS LOGIC
# ==============================================================================
class VariantManager:
    """Generate spelling variants for Hebrew search terms using multiple maps."""
    def __init__(self):
        self.basic_map = self._build_basic()
        self.extended_map = self._build_extended()
        self.maximum_map = self._build_maximum()

    def _build_basic(self):
        return {'ד': 'ר', 'ר': 'ד', 'כ': 'ב', 'ב': 'כ', 'ה': 'ח', 'ח': 'ה', 
                'ו': 'ז', 'ז': 'ו', 'ו': 'י', 'י': 'ו', 'ו': 'ן', 'ן': 'ו', 
                'ט': 'ת', 'ת': 'ט', 'ס': 'ש', 'ש': 'ס'}

    def _build_extended(self):
        pairs = [('ה', 'ח'), ('ת', 'ה'), ('י', 'ל'), ('א', 'ו'), ('ה', 'ר'), ('ל', 'י'), ('א', 'י'), ('ה', 'ת'), ('ר', 'י'), ('א', 'ח'), ('י', 'א'), ('י', 'ר'), ('ק', 'ה'), ('נ', 'ו'), ('ל', 'ו'), ('ה', 'ו'), ('ו', 'א'), ('ו', 'נ'), ('ו', 'ל'), ('ה', 'י'), ('א', 'ה'), ('ר', 'ו'), ('ו', 'ר'), ('ל', 'ר'), ('מ', 'י'), ('ה', 'א'), ('מ', 'א'), ('נ', 'י'), ('מ', 'ו'), ('י', 'ה'), ('ו', 'ה'), ('ו', 'ן'), ('ן', 'ו'), ('ח', 'ה'), ('א', 'ל'), ('ל', 'נ'), ('י', 'נ'), ('ת', 'י'), ('י', 'מ'), ('ת', 'ח'), ('ב', 'י'), ('ל', 'א'), ('ה', 'ם'), ('י', 'ב'), ('ר', 'ה'), ('ו', 'ש'), ('ל', 'כ'), ('י', 'ת'), ('א', 'מ'), ('ת', 'ר'), ('ב', 'ו'), ('ר', 'ל'), ('י', 'ש'), ('ב', 'ר'), ('ו', 'ז'), ('א', 'ש'), ('ש', 'י'), ('ס', 'ם'), ('ז', 'ו'), ('ש', 'ו'), ('ב', 'נ'), ('ח', 'א'), ('ו', 'מ'), ('מ', 'ש'), ('מ', 'ע'), ('ת', 'ו'), ('ר', 'א'), ('ו', 'ב'), ('מ', 'ל'), ('מ', 'ב'), ('ד', 'י'), ('נ', 'ג'), ('ה', 'ד')]
        m = defaultdict(set)
        for k, v in self._build_basic().items(): m[k].add(v)
        for a, b in pairs: m[a].add(b); m[b].add(a)
        return m

    def _build_maximum(self):
        all_pairs = [("'", 'י'), ("'", 'ר'), ('א', 'ב'), ('א', 'ד'), ('א', 'ה'), ('א', 'ו'), ('א', 'ח'), ('א', 'י'), ('א', 'ל'), ('א', 'מ'), ('א', 'ם'), ('א', 'נ'), ('א', 'ע'), ('א', 'ר'), ('א', 'ש'), ('א', 'ת'), ('ב', 'ד'), ('ב', 'ה'), ('ב', 'ו'), ('ב', 'י'), ('ב', 'כ'), ('ב', 'ל'), ('ב', 'מ'), ('ב', 'נ'), ('ב', 'פ'), ('ב', 'ר'), ('ב', 'ש'), ('ב', 'ת'), ('ג', 'ו'), ('ג', 'נ'), ('ד', 'ה'), ('ד', 'ו'), ('ד', 'י'), ('ד', 'כ'), ('ד', 'ל'), ('ד', 'ר'), ('ה', 'ב'), ('ה', 'ד'), ('ה', 'ו'), ('ה', 'ח'), ('ה', 'י'), ('ה', 'ך'), ('ה', 'כ'), ('ה', 'ל'), ('ה', 'מ'), ('ה', 'ם'), ('ה', 'ק'), ('ה', 'ר'), ('ה', 'ש'), ('ה', 'ת'), ('ו', 'ג'), ('ו', 'ד'), ('ו', 'ה'), ('ו', 'ז'), ('ו', 'ח'), ('ו', 'י'), ('ו', 'כ'), ('ו', 'ל'), ('ו', 'מ'), ('ו', 'ם'), ('ו', 'נ'), ('ו', 'ע'), ('ו', 'ר'), ('ו', 'ש'), ('ו', 'ת'), ('ז', 'י'), ('ח', 'א'), ('ח', 'ה'), ('ח', 'י'), ('ח', 'מ'), ('ח', 'ר'), ('ח', 'ת'), ('ט', 'ע'), ('ט', 'ש'), ('ט', 'ת'), ('י', 'ב'), ('י', 'ד'), ('י', 'ה'), ('י', 'ו'), ('י', 'ך'), ('י', 'כ'), ('י', 'ל'), ('י', 'מ'), ('י', 'ם'), ('י', 'נ'), ('י', 'ן'), ('י', 'ע'), ('י', 'ר'), ('י', 'ש'), ('י', 'ת'), ('כ', 'ה'), ('כ', 'ו'), ('כ', 'ל'), ('כ', 'מ'), ('כ', 'נ'), ('כ', 'פ'), ('כ', 'ר'), ('כ', 'ת'), ('ל', 'ד'), ('ל', 'ה'), ('ל', 'ו'), ('ל', 'מ'), ('ל', 'ם'), ('ל', 'נ'), ('ל', 'ע'), ('ל', 'ר'), ('ל', 'ש'), ('ל', 'ת'), ('מ', 'ב'), ('מ', 'ה'), ('מ', 'ח'), ('מ', 'נ'), ('מ', 'ס'), ('מ', 'ע'), ('מ', 'ר'), ('מ', 'ש'), ('מ', 'ת'), ('נ', 'ג'), ('נ', 'ו'), ('נ', 'ל'), ('נ', 'פ'), ('נ', 'ר'), ('נ', 'ת'), ('ס', 'ם'), ('ס', 'מ'), ('ס', 'ש'), ('ע', 'ל'), ('ע', 'מ'), ('ע', 'נ'), ('ע', 'ש'), ('פ', 'ב'), ('פ', 'כ'), ('פ', 'נ'), ('ק', 'ה'), ('ק', 'ר'), ('ר', 'ב'), ('ר', 'ה'), ('ר', 'ך'), ('ר', 'ח'), ('ר', 'כ'), ('ר', 'ל'), ('ר', 'מ'), ('ר', 'נ'), ('ר', 'ק'), ('ר', 'ש'), ('ר', 'ת'), ('ש', 'ב'), ('ש', 'ה'), ('ש', 'ו'), ('ש', 'ט'), ('ש', 'י'), ('ש', 'ל'), ('ש', 'מ'), ('ש', 'ע'), ('ש', 'ר'), ('ת', 'ה'), ('ת', 'ו'), ('ת', 'ח'), ('ת', 'ט'), ('ת', 'י'), ('ת', 'כ'), ('ת', 'ל'), ('ת', 'מ'), ('ת', 'ם'), ('ת', 'נ')]
        m = defaultdict(set)
        for a, b in all_pairs: m[a].add(b); m[b].add(a)
        return m

    def _calc_similarity(self, term, variant):
        if len(term) != len(variant): return 99 
        diff = sum(1 for a, b in zip(term, variant) if a != b)
        return diff

    def generate_variants_recursive(self, term, mapping, max_depth, limit):
        results = {term}
        indices = range(len(term))
        for depth in range(1, max_depth + 1):
            if len(results) >= limit: break
            for positions_to_change in itertools.combinations(indices, depth):
                if len(results) >= limit: break
                char_options_list = []
                valid_combination = True
                for i, char in enumerate(term):
                    if i in positions_to_change:
                        repls = set()
                        if char in mapping:
                            val = mapping[char]
                            if isinstance(val, (set, list)): repls.update(val)
                            else: repls.add(val)
                        if char in repls: repls.remove(char)
                        if not repls:
                            valid_combination = False
                            break
                        char_options_list.append(list(repls))
                    else:
                        char_options_list.append([char])
                if valid_combination:
                    for p in itertools.product(*char_options_list):
                        results.add("".join(p))
                        if len(results) >= limit: break
        return list(results)

    def get_variants(self, term, mode):
        if len(term) < 2: return [term]
        
        mapping = None
        limit = Config.VARIANT_GEN_LIMIT
        max_depth = 1 
        
        if mode == 'variants':
            mapping = self.basic_map
            max_depth = 1
        elif mode == 'variants_extended':
            mapping = self.extended_map
            max_depth = 2
        elif mode == 'variants_maximum':
            mapping = self.maximum_map
            max_depth = 2 
        else:
            return [term]

        variants = self.generate_variants_recursive(term, mapping, max_depth, limit)
        if term not in variants: variants.append(term)
        variants.sort(key=lambda x: (x != term, self._calc_similarity(term, x)))
        return variants

# ==============================================================================
#  METADATA MANAGER
# ==============================================================================
class MetadataManager:
    """Handle metadata parsing, remote retrieval, and persistent caching."""
    def __init__(self):
        self.meta_map = {}
        self.nli_cache = {}
        self.shelf_bank = {}
        self.nli_executor = ThreadPoolExecutor(max_workers=2)
        self.ns = {'marc': 'http://www.loc.gov/MARC21/slim'}
        
        # Ensure index dir exists for caches
        if not os.path.exists(Config.INDEX_DIR):
            try: os.makedirs(Config.INDEX_DIR)
            except: pass
            
        self._load_caches()
        threading.Thread(target=self._build_file_map_background, daemon=True).start()

    def _make_session(self):
        session = requests.Session()
        if hasattr(session, "trust_env"):
            session.trust_env = False
        session.proxies = {}
        return session

    def _load_caches(self):
        self._load_metadata_bank()
        if os.path.exists(Config.CACHE_NLI):
            try:
                with open(Config.CACHE_NLI, 'rb') as f: self.nli_cache = pickle.load(f)
            except: pass
        if os.path.exists(Config.CACHE_META):
            try:
                with open(Config.CACHE_META, 'rb') as f: self.meta_map = pickle.load(f)
            except: pass

    def _normalize_shelfmark(self, raw):
        if not raw: return None
        cleaned = str(raw).strip().strip('-').strip()
        cleaned = cleaned if cleaned else None
        if not cleaned: return None

        # Ignore optional "MS"/"Ms" prefix (with optional punctuation/spacing like "M.S." or "Ms.")
        cleaned = re.sub(r"^\s*m[\.\s]*s[\.\s]*\.?\s*", "", cleaned, flags=re.IGNORECASE)

        no_spaces = re.sub(r"[^\w]", "", cleaned).lower()
        if no_spaces.startswith("ms"):
            cleaned = cleaned[2:].lstrip()
        return cleaned

    def _load_metadata_bank(self):
        if not os.path.exists(Config.METADATA_BANK): return
        try:
            with open(Config.METADATA_BANK, 'rb') as f:
                data = pickle.load(f)
        except Exception:
            return

        parsed = {}

        def add_entry(ie_id, shelf):
            shelfmark = self._normalize_shelfmark(shelf)
            if ie_id and shelfmark:
                parsed[ie_id] = shelfmark

        if isinstance(data, dict):
            for key, val in data.items():
                ie_match = re.search(r'(IE\d+)', str(key)) or re.search(r'(IE\d+)', str(val))
                add_entry(ie_match.group(1) if ie_match else None, val)
        elif isinstance(data, (list, tuple)):
            for item in data:
                ie_match = re.search(r'(IE\d+)', str(item))
                shelf_match = re.split(r'"', str(item).replace('”', '"').replace('“', '"'))
                shelf = shelf_match[-1] if shelf_match else None
                add_entry(ie_match.group(1) if ie_match else None, shelf)
        elif isinstance(data, str):
            chunks = data.replace('\n', ' ').replace('”', '"').replace('“', '"').split(' - ')
            for chunk in chunks:
                parts = [p for p in chunk.split('"') if p]
                ie_match = None
                shelf = None
                for p in parts:
                    if not ie_match:
                        m = re.search(r'(IE\d+)', p)
                        if m: ie_match = m.group(1)
                    if not shelf and re.search(r'[A-Za-z]{1,3}\.?[A-Za-z0-9\.\- ]+', p):
                        shelf = p
                add_entry(ie_match, shelf)

        self.shelf_bank = parsed

    def get_shelfmark_from_header(self, full_header):
        parsed = self.parse_full_id_components(full_header)
        if parsed.get('ie_id') and parsed['ie_id'] in self.shelf_bank:
            return self.shelf_bank.get(parsed['ie_id'], '')
        sys_id = parsed.get('sys_id')
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
        if system_id in self.nli_cache:
            meta = self.nli_cache[system_id]
            if not meta.get('thumb_checked') or not meta.get('thumb_url'):
                meta['thumb_url'] = meta.get('thumb_url') or self.get_thumbnail(system_id)
                meta['thumb_checked'] = True
            return meta

        _, meta = self._fetch_single_worker(system_id)
        self.nli_cache[system_id] = meta
        return meta

    def _fetch_single_worker(self, system_id):
        url = f"https://iiif.nli.org.il/IIIFv21/marc/bib/{system_id}"
        meta = {'shelfmark': 'Unknown', 'title': '', 'desc': '', 'fl_ids': [], 'thumb_url': None, 'thumb_checked': False}
        
        # כותרות כדי להיראות כמו דפדפן
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        
        import time 

        # נסה פעמיים
        for attempt in range(2):
            try:
                # המתנה קלה כדי לא להעמיס
                time.sleep(0.3)
                
                session = self._make_session()
                resp = session.get(url, headers=headers, timeout=5, allow_redirects=True)
                
                if resp.status_code == 200:
                    try:
                        root = ET.fromstring(resp.content)
                        # ... הלוגיקה הרגילה של ה-XML ...
                        c_942 = None; c_907 = None; c_090 = None; c_avd = None

                        fl_ids = self._extract_fl_ids(root)

                        for df in root.findall('marc:datafield', self.ns):
                            tag = df.get('tag')
                            def get_val(code):
                                sf = df.find(f"marc:subfield[@code='{code}']", self.ns)
                                return sf.text if sf is not None else None

                            if tag == '942':
                                val = get_val('z')
                                if val and not c_942: c_942 = val
                                elif val and c_942 and c_942.isdigit() and not val.isdigit(): c_942 = val
                                    
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
                        meta['thumb_url'] = self._resolve_thumbnail(fl_ids, session=session)
                        meta['thumb_checked'] = True
                        
                        # הצלחה - צא מהלולאה והחזר תוצאה
                        return system_id, meta
                    except ET.ParseError:
                        pass # XML שבור, אין טעם לנסות שוב
                        break
                
                elif resp.status_code >= 500:
                    print(f"[DEBUG] Server Error {resp.status_code} for {system_id}. Retry {attempt+1}...")
                    time.sleep(1) # חכה שניה לפני ניסיון הבא
                else:
                    break # שגיאה אחרת (404 וכו'), לא לנסות שוב

            except Exception as e:
                print(f"[DEBUG] Network Error: {e}")
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
        session = session or self._make_session()
        for fl_id in fl_ids:
            base = f"https://iiif.nli.org.il/IIIFv21/{fl_id}"
            try:
                info = session.get(f"{base}/info.json", timeout=5, allow_redirects=True)
                if info.status_code != 200:
                    continue

                thumb = self._pick_working_thumbnail(base, size, session)
                if thumb:
                    return thumb
            except Exception:
                continue
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

    def get_display_data(self, full_header, src_label):
        sys_id, p_num = self.parse_header_smart(full_header)
        parsed = self.parse_full_id_components(full_header)

        shelfmark = None
        if parsed.get('ie_id') and parsed['ie_id'] in self.shelf_bank:
            shelfmark = self.shelf_bank.get(parsed['ie_id'])
        meta = self.nli_cache.get(sys_id, {'shelfmark': '', 'title': ''})
        shelfmark = shelfmark or meta.get('shelfmark')
        return {
            'shelfmark': shelfmark or f"ID: {sys_id}",
            'title': meta.get('title', ''),
            'img': p_num,
            'source': src_label,
            'id': sys_id,
            'thumb_url': meta.get('thumb_url')
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
            raise FileNotFoundError(f"Input file not found: {Config.FILE_V8}\nPlease place 'Transcriptions.txt' next to the executable.")

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
                            shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or self.meta_mgr.meta_map.get(cid, "")
                            writer.add_document(tantivy.Document(
                                unique_id=str(cid), content=" ".join(ctext), source=str(label),
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
                all_vars = self.var_mgr.get_variants(term, mode)
                subset = all_vars[:Config.TANTIVY_CLAUSE_LIMIT]
                if term not in subset: subset.insert(0, term)
                quoted_vars = [f'"{v}"' for v in subset]
                parts.append(f'({" OR ".join(quoted_vars)})')
        return " AND ".join(parts)

    def build_regex_pattern(self, terms, mode, max_gap):
        if mode == 'Regex':
            try: return re.compile(" ".join(terms), re.IGNORECASE)
            except: return None

        parts = []
        for term in terms:
            regex_mode = 'variants_maximum' if mode == 'fuzzy' else mode
            vars_list = self.var_mgr.get_variants(term, regex_mode)
            vars_list = vars_list[:Config.REGEX_VARIANTS_LIMIT]
            escaped = [re.escape(v) for v in vars_list]
            parts.append(f"({'|'.join(escaped)})")

        if max_gap == 0:
            sep = r'[^\w\u0590-\u05FF\']+'
        else:
            sep = rf'(?:[^\w\u0590-\u05FF\']+{Config.WORD_TOKEN_PATTERN}){{0,{max_gap}}}[^\w\u0590-\u05FF\']+'

        try: return re.compile(sep.join(parts), re.IGNORECASE)
        except: return None

    def highlight(self, text, regex, for_file=False):
        m = regex.search(text)
        if not m: return None
        s, e = m.span()
        start = max(0, s - 60)
        end = min(len(text), e + 60)
        snippet = text[start:end]
        matched_text = m.group(0)
        if for_file: return snippet.replace(matched_text, f"*{matched_text}*")
        else: return snippet.replace(matched_text, f"<b style='color:red;'>{matched_text}</b>")

    def execute_search(self, query_str, mode, gap, progress_callback=None):
        if not self.searcher: return []
        
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

    def search_composition_logic(self, full_text, chunk_size, max_freq, mode, progress_callback=None):
        tokens = re.findall(Config.WORD_TOKEN_PATTERN, full_text)
        if len(tokens) < chunk_size: return None
        chunks = [tokens[i:i + chunk_size] for i in range(len(tokens) - chunk_size + 1)]
        doc_hits = defaultdict(lambda: {'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(), 'patterns': set()})
        total_chunks = len(chunks)
        
        for i, chunk in enumerate(chunks):
            if progress_callback and i % 10 == 0: progress_callback(i, total_chunks)
            t_query = self.build_tantivy_query(chunk, mode)
            regex = self.build_regex_pattern(chunk, mode, 0)
            if not regex: continue
            try:
                query = self.index.parse_query(t_query, ["content"])
                hits = self.searcher.search(query, 50).hits
                if len(hits) > max_freq: continue 
                for score, doc_addr in hits:
                    doc = self.searcher.doc(doc_addr)
                    content = doc['content'][0]
                    if regex.search(content):
                        uid = doc['unique_id'][0]
                        rec = doc_hits[uid]
                        rec['head'] = doc['full_header'][0]
                        rec['src'] = doc['source'][0]
                        rec['content'] = content
                        rec['matches'].append(regex.search(content).span())
                        rec['src_indices'].update(range(i, i + chunk_size))
                        rec['patterns'].add(regex.pattern)
            except: pass

        final_items = []
        for uid, data in doc_hits.items():
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

            # Combined regex for highlighting
            combined_pattern = "|".join(list(data['patterns'])) if data.get('patterns') else ""

            final_items.append({
                'score': score, 'uid': uid, 
                'raw_header': data['head'], 'src_lbl': data['src'],
                'source_ctx': "\n".join(src_snippets), 
                'text': "\n...\n".join(ms_snips),
                'highlight_pattern': combined_pattern # <--- קריטי להדגשה
            })

        final_items.sort(key=lambda x: x['score'], reverse=True)
        return final_items

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