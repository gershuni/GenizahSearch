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
from functools import lru_cache
import itertools
import json

from genizah_translations import TRANSLATIONS

# --- Shmidman Rare-Letter Helpers ---
HEBREW_FREQ = {
    'י': 1, 'ו': 2, 'ה': 3, 'ל': 4, 'א': 5, 'ר': 6, 'מ': 7, 'ת': 8, 
    'ב': 9, 'ש': 10, 'נ': 11, 'ד': 12, 'כ': 13, 'ע': 14, 'ח': 15, 
    'ק': 16, 'פ': 17, 'ס': 18, 'ג': 19, 'ט': 20, 'ז': 21, 'צ': 22,
    # Final letters
    'ך': 13, 'ם': 7, 'ן': 11, 'ף': 17, 'ץ': 22
}

def encode_word_shmidman(word: str) -> str:
    """Encode a single word by selecting its two rarest Hebrew characters."""
    letters = []
    for idx, ch in enumerate(word):
        if ch in HEBREW_FREQ:
            letters.append((idx, ch, HEBREW_FREQ[ch]))

    if not letters:
        return ""

    rarest = sorted(letters, key=lambda item: (-item[2], item[0]))[:3]
    rarest_sorted = sorted(rarest, key=lambda item: item[0])
    return "".join(ch for _, ch, _ in rarest_sorted)


def text_to_fingerprint(text: str) -> str:
    """Convert free text into a fingerprint representation."""
    tokens = re.findall(Config.WORD_TOKEN_PATTERN, text or "")
    encoded_tokens = []
    for tok in tokens:
        encoded = encode_word_shmidman(tok)
        if encoded:
            encoded_tokens.append(encoded)
    return " ".join(encoded_tokens)


def natural_sort_key(text):
    """Sort strings containing numbers naturally (e.g. 'Item 2' < 'Item 10')."""
    normalized = re.sub(r'^\s*ms\.?\s*', '', text or "", flags=re.IGNORECASE)
    return [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', normalized)]


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
    """Manages configuration for the Lab Mode, including scoring weights."""
    def __init__(self):
        self.custom_variants = {} 
        self.candidate_limit = 2000
        self.min_should_match = 60
        self.gap_penalty = 2
        
        # Scoring Weights
        self.length_bonus_factor = 1.5
        self.common_penalty_factor = 0.1
        self.unique_bonus_base = 100
        self.density_penalty = 0.2
        self.coverage_power = 2.0
        self.order_bonus = 10.0
        
        # --- הגדרות חדשות: דיכוי רעשים (Stop Words) ---
        self.stop_word_score = 1.0       # ניקוד למילים קצרות (<3 אותיות)
        self.common_3char_score = 2.0    # ניקוד למילים נפוצות בנות 3 אותיות
        
        # Composition Settings
        self.comp_chunk_limit = 200
        self.comp_min_score = 70
        self.comp_max_final_results = 100
        
        self.load()

    def load(self):
        if os.path.exists(Config.LAB_CONFIG_FILE):
            try:
                with open(Config.LAB_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.custom_variants = data.get('custom_variants', {})
                    self.candidate_limit = data.get('candidate_limit', 2000)
                    self.min_should_match = data.get('min_should_match', 60)
                    self.gap_penalty = data.get('gap_penalty', 2)
                    
                    self.length_bonus_factor = data.get('length_bonus_factor', 1.5)
                    self.common_penalty_factor = data.get('common_penalty_factor', 0.1)
                    self.unique_bonus_base = data.get('unique_bonus_base', 100)
                    self.density_penalty = data.get('density_penalty', 0.2)
                    self.coverage_power = data.get('coverage_power', 2.0)
                    self.order_bonus = data.get('order_bonus', 10.0)

                    # טעינת הגדרות רעש
                    self.stop_word_score = data.get('stop_word_score', 1.0)
                    self.common_3char_score = data.get('common_3char_score', 2.0)

                    self.comp_chunk_limit = data.get('comp_chunk_limit', 200)
                    self.comp_min_score = data.get('comp_min_score', 70)
                    self.comp_max_final_results = data.get('comp_max_final_results', 100)
            except Exception: pass

    def save(self):
        try:
            os.makedirs(Config.LAB_DIR, exist_ok=True)
            with open(Config.LAB_CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'custom_variants': self.custom_variants,
                    'candidate_limit': self.candidate_limit,
                    'min_should_match': self.min_should_match,
                    'gap_penalty': self.gap_penalty,
                    
                    'length_bonus_factor': self.length_bonus_factor,
                    'common_penalty_factor': self.common_penalty_factor,
                    'unique_bonus_base': self.unique_bonus_base,
                    'density_penalty': self.density_penalty,
                    'coverage_power': self.coverage_power,
                    'order_bonus': self.order_bonus,
                    
                    # שמירת הגדרות רעש
                    'stop_word_score': self.stop_word_score,
                    'common_3char_score': self.common_3char_score,

                    'comp_chunk_limit': self.comp_chunk_limit,
                    'comp_min_score': self.comp_min_score,
                    'comp_max_final_results': self.comp_max_final_results
                }, f, indent=4)
        except Exception: pass

# ==============================================================================
#  LAB ENGINE 
# ==============================================================================
class LabEngine:
    LAB_FINGERPRINT_FIELD = "fingerprint"
    # NGRAM_SIZE kept for compatibility if other parts of code ref it
    NGRAM_SIZE = 3 

    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        self.settings = LabSettings()
        self.lab_index = None
        self.lab_searcher = None
        self.lab_index_needs_rebuild = False
        self._reload_lab_index()

    def _close_index(self):
        self.lab_searcher = None
        self.lab_index = None
        import gc
        gc.collect() 

    def _ensure_lab_tokenizers(self, index):
        """Register analyzers safely."""
        try:
            index.register_tokenizer("whitespace", tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.whitespace()).build())
        except Exception:
            pass
        try:
            index.register_tokenizer("simple", tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple()).build())
        except Exception:
            pass

    def _reload_lab_index(self):
        """Loads index with heavy debug logging."""
        if os.path.exists(Config.LAB_INDEX_DIR):
            try:
                LAB_LOGGER.info("Reloading Lab Index...")
                self.lab_index = tantivy.Index.open(Config.LAB_INDEX_DIR)
                self._ensure_lab_tokenizers(self.lab_index)
                self.lab_searcher = self.lab_index.searcher()
                
                # Simplified robust check
                self.lab_index_needs_rebuild = False
                return True
            except Exception as e:
                LAB_LOGGER.error(f"Failed to load Lab Index: {e}")
                self._close_index()
        
        self.lab_index_needs_rebuild = True
        return False

    @staticmethod
    def lab_index_normalize(text):
        return re.sub(r"[^\w\u0590-\u05FF\s\*\~]", "", text).replace('_', ' ').lower()

    def rebuild_lab_index(self, progress_callback=None):
        LAB_LOGGER.info(f"Starting REBUILD at: {Config.LAB_INDEX_DIR}")
        self._close_index()
        time.sleep(0.5)

        if not os.path.exists(Config.FILE_V8):
            raise FileNotFoundError("Input file not found")

        if os.path.exists(Config.LAB_INDEX_DIR):
            try:
                shutil.rmtree(Config.LAB_INDEX_DIR, ignore_errors=True)
            except Exception as e:
                LAB_LOGGER.error(f"Delete failed: {e}")

        os.makedirs(Config.LAB_INDEX_DIR, exist_ok=True)

        builder = tantivy.SchemaBuilder()
        builder.add_text_field("unique_id", stored=True)
        builder.add_text_field("text_normalized", stored=True, tokenizer_name="simple")
        builder.add_text_field("text_ngram", stored=False, tokenizer_name="whitespace") # Legacy
        
        # The critical field
        builder.add_text_field(self.LAB_FINGERPRINT_FIELD, stored=False, tokenizer_name="simple")
        
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        builder.add_text_field("source", stored=True)
        builder.add_text_field("content", stored=True, tokenizer_name="simple")

        schema = builder.build()
        index = tantivy.Index(schema, path=Config.LAB_INDEX_DIR)
        self._ensure_lab_tokenizers(index)
        writer = index.writer(heap_size=50_000_000)

        total_docs = 0
        
        def process_file(fpath, label):
            nonlocal total_docs
            if not os.path.exists(fpath): return
            LAB_LOGGER.info(f"Indexing {label}...")
            
            with open(fpath, 'r', encoding='utf-8-sig') as f:
                cid, chead, ctext = None, None, []
                for line in f:
                    line = line.strip()
                    is_sep = (label == "V0.8" and line.startswith("==>")) or (label == "V0.7" and line.startswith("###"))

                    if is_sep:
                        if cid and ctext:
                            original = "\n".join(ctext)
                            norm = self.lab_index_normalize(original)
                            fp = text_to_fingerprint(original)
                            sm = self.meta_mgr.get_shelfmark_from_header(chead) or "Unknown"

                            writer.add_document(tantivy.Document(
                                unique_id=str(cid),
                                text_normalized=norm,
                                fingerprint=fp,
                                content=original,
                                full_header=str(chead),
                                shelfmark=str(sm),
                                source=str(label)
                            ))
                            total_docs += 1
                            if progress_callback and total_docs % 1000 == 0:
                                progress_callback(total_docs, 0)
                        
                        chead = line.replace("==>", "").replace("<==", "").strip() if label == "V0.8" else line
                        cid = self.meta_mgr.extract_unique_id(line)
                        ctext = [] 
                    else:
                        ctext.append(line)
                
                # Last doc
                if cid and ctext:
                    original = "\n".join(ctext)
                    fp = text_to_fingerprint(original)
                    writer.add_document(tantivy.Document(
                        unique_id=str(cid),
                        text_normalized=self.lab_index_normalize(original),
                        fingerprint=fp,
                        content=original,
                        full_header=str(chead),
                        shelfmark=str("Unknown"),
                        source=str(label)
                    ))
                    total_docs += 1

        process_file(Config.FILE_V8, "V0.8")
        process_file(Config.FILE_V7, "V0.7")

        writer.commit()
        LAB_LOGGER.info(f"Rebuild done. {total_docs} docs committed.")
        self._reload_lab_index()
        return total_docs

    def _execute_safe_search(self, query_str, slop=0):
        """
        Modified to support fuzzy matching based on sensitivity settings.
        If min_should_match is 100%, uses a strict Phrase Query.
        Otherwise, constructs a Boolean OR query to fetch candidates for filtering.
        """
        tokens = query_str.split()
        if not tokens:
            return None

        # אם המשתמש דורש 100% התאמה, נשתמש בחיפוש המקורי (Phrase Query) שהוא המחמיר ביותר
        if self.settings.min_should_match >= 100:
            final_query_str = f'{self.LAB_FINGERPRINT_FIELD}:"{query_str}"~{slop}'
        else:
            # אחרת, נבנה שאילתת OR כדי לקבל כל מסמך שמכיל לפחות חלק מהמילים
            # הסינון המדויק לפי אחוזים יתבצע בפונקציה lab_search
            # מבנה: fingerprint:token1 OR fingerprint:token2 ...
            clauses = [f'{self.LAB_FINGERPRINT_FIELD}:{t}' for t in tokens]
            final_query_str = " OR ".join(clauses)

        # Strategy A: Call parse_query with ONE argument
        try:
            q = self.lab_index.parse_query(final_query_str)
            return self.lab_searcher.search(q, self.settings.candidate_limit)
        except Exception as e1:
            pass

        # Strategy B: Call parse_query with explicit field list (STRINGS)
        try:
            # בחיפוש בוליאני מורכב, לפעמים צריך לציין את השדות ברירת המחדל
            q = self.lab_index.parse_query(final_query_str, [self.LAB_FINGERPRINT_FIELD])
            return self.lab_searcher.search(q, self.settings.candidate_limit)
        except Exception as e2:
            pass

        # Strategy C: Call parse_query with explicit field list (INTS - Schema objects)
        try:
            field_handle = self.lab_index.schema.get_field(self.LAB_FINGERPRINT_FIELD)
            q = self.lab_index.parse_query(final_query_str, [field_handle])
            return self.lab_searcher.search(q, self.settings.candidate_limit)
        except Exception as e3:
            LAB_LOGGER.error(f"All query strategies failed. Last: {e3}")
            return None

    def _get_term_weight(self, fp):
        """
        Calculates importance using User Configurable Stop-Word scores.
        """
        raw_weight = 0
        for char in fp:
            raw_weight += HEBREW_FREQ.get(char, 0)
        
        # 1. מילים קצרות מדי (<3 תווים)
        if len(fp) < 3:
            return self.settings.stop_word_score 
        
        # 2. מילים נפוצות בנות 3 תווים (משקל נמוך)
        if len(fp) == 3 and raw_weight < 18:
            return self.settings.common_3char_score

        # 3. מילים רגילות/נדירות
        final_weight = raw_weight
        
        # בונוס אורך רק למילים משמעותיות
        if len(fp) > 3:
            final_weight *= self.settings.length_bonus_factor
            
        return final_weight

    def _calculate_match_metrics(self, text, query_fingerprints_list, original_query_str):
        """
        Calculates score with STRICT FREQUENCY CAP & SEQUENTIAL ORDER.
        1. Words appearing more times in text than in query yield ZERO score.
        2. Sequence matches get huge bonuses.
        """
        if not text:
            return 0, [], (0, 0)

        # 1. Exact Match Check
        def safe_norm(s): return re.sub(r"[^\w\u0590-\u05FF]", "", s).lower()
        norm_text = safe_norm(text)
        norm_query = safe_norm(original_query_str)
        exact_bonus = 0
        if norm_query and norm_query in norm_text:
            exact_bonus = 1000000

        # 2. Weights & Mapping
        fp_to_query_indices = defaultdict(list)
        term_weights = {}
        
        for idx, fp in enumerate(query_fingerprints_list):
            fp_to_query_indices[fp].append(idx)
            term_weights[fp] = self._get_term_weight(fp)

        max_possible_unique_weight = sum(term_weights.values()) 
        
        # 3. Collect Matches
        matches = []
        q_fp_set = set(query_fingerprints_list)
        
        for m in re.finditer(r"[\w\u0590-\u05FF\']+", text):
            word = m.group()
            fp = encode_word_shmidman(word)
            if fp in q_fp_set:
                matches.append({
                    'start': m.start(),
                    'end': m.end(),
                    'word': word,
                    'fp': fp,
                    'weight': term_weights[fp],
                    'q_indices': fp_to_query_indices[fp]
                })

        if not matches:
            return 0, [], (0, 0)

        # 4. Find Best Cluster
        max_score = 0
        best_window = (0, 0)
        total_matches = len(matches)
        
        unique_bonus = self.settings.unique_bonus_base
        common_factor = self.settings.common_penalty_factor
        density_pen = self.settings.density_penalty
        order_bonus_factor = self.settings.order_bonus
        
        lookahead_limit = len(query_fingerprints_list) * 5
        
        for i in range(total_matches):
            current_window_score = 0
            
            # מעקב אחרי כמויות: כמה פעמים ראינו כל מילה בחלון הנוכחי?
            seen_counts = defaultdict(int)
            
            # מעקב אחרי סדר
            last_valid_query_idx = -1
            sequential_chain_length = 0
            
            # אתחול לפי מילת ההתחלה
            if matches[i]['q_indices']:
                last_valid_query_idx = matches[i]['q_indices'][0]

            for j in range(i, min(total_matches, i + lookahead_limit)):
                m = matches[j]
                
                # בדיקת מרחק פיזי
                dist = m['end'] - matches[i]['start']
                if dist > 450: break 
                
                fp = m['fp']
                w = m['weight']
                
                # כמה פעמים המילה הזו מופיעה בשאילתה המקורית?
                allowed_count = len(fp_to_query_indices[fp])
                
                # כמה פעמים ראינו אותה בחלון הזה עד כה?
                seen_counts[fp] += 1
                
                # חישוב הניקוד למילה הספציפית הזו
                word_score = 0
                
                if seen_counts[fp] <= allowed_count:
                    # זוהי הופעה "חוקית" (ראשונה או שניה אם יש כפילות בשאילתה)
                    # ניתן ניקוד מלא
                    word_score = (w * unique_bonus)
                else:
                    # זוהי הופעה מיותרת (זבל). המילה כבר נמצאה מספיק פעמים.
                    # ניתן ניקוד מופחת דרסטית (או אפס אם המשתמש קבע 0)
                    word_score = (w * common_factor) 
                
                current_window_score += word_score

                # --- Order Bonus Logic ---
                found_sequence = False
                best_q_idx_for_match = -1
                
                for q_idx in m['q_indices']:
                    if q_idx > last_valid_query_idx:
                        best_q_idx_for_match = q_idx
                        found_sequence = True
                        break 
                
                if found_sequence:
                    sequential_chain_length += 1
                    current_window_score += (w * order_bonus_factor * sequential_chain_length)
                    last_valid_query_idx = best_q_idx_for_match
                
                # --- Density Penalty ---
                penalty = dist * density_pen
                final_window_score = current_window_score - penalty
                
                if final_window_score > max_score:
                    max_score = final_window_score
                    best_window = (i, j)

        # 5. Coverage Calculation
        start_idx, end_idx = best_window
        window_matches = matches[start_idx : end_idx + 1]
        
        found_unique_fps = set(m['fp'] for m in window_matches)
        found_unique_weight = sum(term_weights[fp] for fp in found_unique_fps)
        
        coverage_ratio = 0
        if max_possible_unique_weight > 0:
            coverage_ratio = found_unique_weight / max_possible_unique_weight
        
        final_score = (max_score * (coverage_ratio ** self.settings.coverage_power)) + exact_bonus

        return final_score, matches, best_window    

    def _generate_highlighted_snippet(self, text, matches, best_window):
        """
        Generates an HTML snippet with RED highlighting and surrounding context.
        """
        if not text: return ""
        if not matches: return text[:300]

        start_m_idx, end_m_idx = best_window
        
        # הגנה על אינדקסים
        start_m_idx = max(0, start_m_idx)
        end_m_idx = min(len(matches) - 1, end_m_idx)

        # 1. קביעת גבולות הטקסט להצגה (100 תווים להקשר רחב)
        padding = 100
        snippet_start_char = max(0, matches[start_m_idx]['start'] - padding)
        snippet_end_char = min(len(text), matches[end_m_idx]['end'] + padding)
        
        # קוסמטיקה: לא לחתוך באמצע מילה
        if snippet_start_char > 0:
            next_space = text.find(' ', snippet_start_char)
            if next_space != -1 and next_space < matches[start_m_idx]['start']:
                snippet_start_char = next_space + 1

        # 2. איסוף המילים הרלוונטיות
        relevant_matches = matches[start_m_idx : end_m_idx + 1]
        
        # 3. בניית ה-HTML
        out_parts = []
        out_parts.append("<div dir='rtl' style='white-space: pre-wrap;'>") 
        
        if snippet_start_char > 0: out_parts.append("... ")
        
        current_idx = snippet_start_char
        
        for m in relevant_matches:
            if m['start'] < snippet_start_char: continue
            if m['end'] > snippet_end_char: break
            
            # טקסט רגיל
            if m['start'] > current_idx:
                plain = text[current_idx : m['start']]
                out_parts.append(plain)
            
            # מילה מודגשת באדום
            word = text[m['start'] : m['end']]
            out_parts.append(f"<span style='color:#ff0000; font-weight:bold;'>{word}</span>")
            
            current_idx = m['end']
        
        # שארית
        if current_idx < snippet_end_char:
            out_parts.append(text[current_idx : snippet_end_char])
            
        if snippet_end_char < len(text): out_parts.append(" ...")
        
        out_parts.append("</div>")

        final_html = "".join(out_parts)
        # המרת ירידות שורה לרווחים כדי לא לשבור את הטבלה
        return final_html.replace("\n", " ").replace("\r", "")

    def lab_search(self, query_str, mode='variants', progress_callback=None, gap=0):
        if not self.lab_searcher: return []

        # 1. Prepare Fingerprints
        fp_str = text_to_fingerprint(query_str)
        if not fp_str: return []
        
        query_fp_list = fp_str.split()
        
        # 2. Fetch Candidates
        slop = max(50, int(self.settings.gap_penalty) * 10) 
        res_obj = self._execute_safe_search(fp_str, slop)
        if not res_obj: return []

        results = []
        min_match_pct = self.settings.min_should_match

        # 3. Process
        for score, doc_addr in res_obj.hits:
            try:
                doc = self.lab_searcher.doc(doc_addr)
                content = doc['content'][0]
                uid = doc['unique_id'][0]

                # --- Core: Calculate Score & Find Matches ---
                custom_score, matches, best_window = self._calculate_match_metrics(content, query_fp_list, query_str)
                
                if custom_score < 15: 
                    continue
                
                # Filter by Percentage (Approximate)
                if min_match_pct < 100:
                    found_unique = set(m['fp'] for m in matches)
                    needed_unique = set(query_fp_list)
                    common = found_unique.intersection(needed_unique)
                    if len(needed_unique) > 0 and (len(common) / len(needed_unique) * 100 < min_match_pct):
                        continue

                # --- Highlight Snippet ---
                smart_snippet = self._generate_highlighted_snippet(content, matches, best_window)
                html_snippet = self._html_snippet(smart_snippet)

                # --- FIX FOR VIEWER HIGHLIGHTING ---
                # We extract the ACTUAL corrupted words found in the match window
                # and create a Regex pattern from them. The GUI uses this pattern to highlight.
                start_idx, end_idx = best_window
                relevant_matches = matches[start_idx : end_idx + 1]
                
                # Collect unique words found (e.g., "מאמתי", "קורין", "את", "שמע")
                found_words = list(set(m['word'] for m in relevant_matches))
                
                # Sort by length descending (so "wordLong" matches before "word")
                found_words.sort(key=len, reverse=True)
                
                # Create a regex OR pattern: (word1|word2|...)
                # We use re.escape to handle any special chars in the text
                highlight_regex_str = "|".join(re.escape(w) for w in found_words) if found_words else ""
                
                results.append({
                    'sort_score': custom_score,
                    'display': {
                        'id': self.meta_mgr.extract_unique_id(doc['full_header'][0]) or uid, 
                        'shelfmark': doc['shelfmark'][0], 
                        'title': '', 
                        'source': doc['source'][0], 
                        'img': ''
                    },
                    'snippet': html_snippet,
                    'full_text': content,
                    'uid': uid,
                    'raw_header': doc['full_header'][0],
                    'raw_file_hl': smart_snippet,
                    # This is the magic key for the Viewer:
                    'highlight_pattern': highlight_regex_str 
                })
            except Exception as e:
                LAB_LOGGER.error(f"Error processing doc: {e}")

        # 4. Sort & Dedup (Logic Fixed: Prioritize V0.8 over V0.7)
        v8_map = {r['uid']: r for r in results if r['display']['source'] == "V0.8"}
        
        final_list = []
        
        # מוסיפים את כל תוצאות V0.8
        final_list.extend(v8_map.values())
        
        # מוסיפים תוצאות V0.7 *רק* אם ה-UID שלהן לא קיים ב-V0.8
        for r in results:
            if r['display']['source'] != "V0.8": # V0.7 or others
                if r['uid'] not in v8_map:
                    final_list.append(r)

        # לבסוף, ממיינים את הרשימה המאוחדת לפי הציון הגבוה ביותר
        final_list.sort(key=lambda x: x['sort_score'], reverse=True)

        return final_list

    def _generate_snippet(self, text, terms, window=100):
        # Basic highlighter finding first term
        low = text.lower()
        idx = -1
        for t in terms:
            idx = low.find(t.lower())
            if idx != -1: break
        
        if idx == -1: return text[:300]
        
        start = max(0, idx - window)
        end = min(len(text), idx + window)
        chunk = text[start:end]
        
        # Highlight in chunk
        for t in terms:
            chunk = re.sub(f"({re.escape(t)})", r"*\1*", chunk, flags=re.IGNORECASE)
        return chunk

    def _html_snippet(self, text):
        if not text: return ""
        return re.sub(r'\*(.*?)\*', r"<b style='color:red'>\1</b>", text)

    def lab_composition_search(self, full_text, mode='variants', progress_callback=None, chunk_size=None, excluded_ids=None):
        """
        Scans a composition using Lab Mode.
        UPGRADES:
        1. Filters common phrases.
        2. Boosts V0.8.
        3. FIX: Separates excluded/known manuscripts.
        """
        if not full_text:
            return {'main': [], 'filtered': [], 'known': []} # הוספנו known

        # נרמול רשימת ההחרגה לחיפוש מהיר
        excluded_set = set(str(x) for x in (excluded_ids or []))

        # הגדרות משתמש
        PER_CHUNK_LIMIT = self.settings.comp_chunk_limit
        MIN_SCORE_THRESHOLD = self.settings.comp_min_score
        MAX_FINAL = self.settings.comp_max_final_results
        min_pct_ratio = self.settings.min_should_match / 100.0

        # ... (חלק 1: פירוק לטוקנים - נשאר זהה) ...
        tokens = re.findall(r"[\w\u0590-\u05FF\']+", full_text)
        c_size = chunk_size if chunk_size else 15
        step = max(1, int(c_size * 0.5)) 
        
        chunks_data = []
        for i in range(0, max(1, len(tokens) - c_size + 1), step):
            chunks_data.append((i, tokens[i : i + c_size]))
        if len(tokens) < c_size: chunks_data = [(0, tokens)]

        total_chunks = len(chunks_data)
        results_map = {} 

        # ... (חלק 2: סריקה - נשאר זהה לחלוטין לגרסה הקודמת והטובה) ...
        for i, (token_start_idx, chunk_tokens) in enumerate(chunks_data):
            if progress_callback and i % 5 == 0: progress_callback(i, total_chunks)
            chunk_text = " ".join(chunk_tokens)
            
            if self._is_phrase_statistically_weak(chunk_text): continue

            fp_str = text_to_fingerprint(chunk_text)
            if not fp_str or len(chunk_tokens) < 4: continue
            
            fp_list = fp_str.split()
            needed_unique_fps = set(fp_list) 

            # שאילתה עם Boost
            query_tokens = fp_str.split()
            clauses = [f'{self.LAB_FINGERPRINT_FIELD}:{t}' for t in query_tokens]
            core_query = " OR ".join(clauses)
            final_query_str = f'({core_query}) AND (source:"V0.8"^10 OR source:"V0.7")'
            
            res_obj = None
            try:
                q = self.lab_index.parse_query(final_query_str)
                res_obj = self.lab_searcher.search(q, PER_CHUNK_LIMIT)
            except:
                try:
                    q = self.lab_index.parse_query(core_query)
                    res_obj = self.lab_searcher.search(q, PER_CHUNK_LIMIT)
                except: continue

            if not res_obj: continue

            for score, doc_addr in res_obj.hits:
                try:
                    doc = self.lab_searcher.doc(doc_addr)
                    content = doc['content'][0]
                    match_score, matches, best_window = self._calculate_match_metrics(content, fp_list, chunk_text)
                    
                    found_unique_fps = set(m['fp'] for m in matches[best_window[0]:best_window[1]+1])
                    common_fps = found_unique_fps.intersection(needed_unique_fps)
                    if len(needed_unique_fps) > 0:
                        if (len(common_fps) / len(needed_unique_fps)) < min_pct_ratio: continue
                    
                    if match_score < MIN_SCORE_THRESHOLD: continue

                    uid = doc['unique_id'][0] 
                    if uid not in results_map:
                        results_map[uid] = {
                            'uid': uid, 'total_score': 0, 'hits_count': 0,
                            'raw_header': doc['full_header'][0], 'source': doc['source'][0],
                            'content': content, 'best_chunk_score': -1,
                            'all_found_words': set(), 'src_indices': set(), 'ms_matches': [] 
                        }
                    rec = results_map[uid]
                    rec['total_score'] += match_score
                    rec['hits_count'] += 1
                    token_end_idx = token_start_idx + len(chunk_tokens)
                    rec['src_indices'].update(range(token_start_idx, token_end_idx))
                    start_m, end_m = best_window
                    if matches:
                        rec['ms_matches'].append((matches[start_m]['start'], matches[end_m]['end']))
                        for m in matches[start_m : end_m + 1]: rec['all_found_words'].add(m['word'])
                except: pass

        # ... (חלק 3: עיבוד תוצאות - כאן השינוי המהותי בסוף) ...
        raw_final_items = []
        is_short_search = (total_chunks <= 3)

        for uid, data in results_map.items():
            if not is_short_search:
                if data['hits_count'] < 2 and data['total_score'] < 1000: continue 
            else:
                if data['total_score'] < 250: continue

            # יצירת סניפטים (אותו קוד בדיוק כמו קודם)
            src_snippets = []
            src_indices = sorted(list(data['src_indices']))
            if src_indices:
                clusters = []
                curr_cluster = [src_indices[0]]
                for idx in src_indices[1:]:
                    if idx - curr_cluster[-1] < 60: curr_cluster.append(idx)
                    else: clusters.append(curr_cluster); curr_cluster = [idx]
                clusters.append(curr_cluster)
                for cl in clusters:
                    start_ctx = max(0, cl[0] - 50); end_ctx = min(len(tokens), cl[-1] + 51)
                    cl_set = set(cl)
                    words_out = [f"*{tokens[k]}*" if k in cl_set else tokens[k] for k in range(start_ctx, end_ctx)]
                    src_snippets.append(f"... {' '.join(words_out)} ...")

            ms_snips = []
            spans = sorted(data['ms_matches'], key=lambda x: x[0])
            merged = []
            if spans:
                curr_s, curr_e = spans[0]
                for s, e in spans[1:]:
                    if s <= curr_e + 20: curr_e = max(curr_e, e)
                    else: merged.append((curr_s, curr_e)); curr_s, curr_e = s, e
                merged.append((curr_s, curr_e))
            
            content = data['content']
            for s, e in merged:
                start = max(0, s - 60); end = min(len(content), e + 60)
                snip = content[start:end]
                rs = max(0, s - start); re_ = min(len(snip), e - start)
                if re_ > rs:
                    ms_snips.append(snip[:rs] + f"<span style='color:#ff0000; font-weight:bold;'>{snip[rs:re_]}</span>" + snip[re_:])

            found_words = sorted(list(data['all_found_words']), key=len, reverse=True)[:50]
            hl_pattern = "|".join(re.escape(w) for w in found_words) if found_words else ""

            item = {
                'score': data['total_score'],
                'uid': uid,
                'raw_header': data['raw_header'],
                'src_lbl': data['source'],
                'source_ctx': "\n\n".join(src_snippets),
                'text': "\n...\n".join(ms_snips),        
                'highlight_pattern': hl_pattern,
                'full_text': data['content']
            }
            raw_final_items.append(item)

        # --- מיון והפרדה (Sorting & Splitting Logic) ---
        raw_final_items.sort(key=lambda x: x['score'], reverse=True)
        
        main_list = []
        known_list = []
        
        for item in raw_final_items:
            # בדיקה האם כתב היד מוחרג
            is_excluded = False
            
            # 1. בדיקה לפי UID (למשל IE...)
            if str(item['uid']) in excluded_set:
                is_excluded = True
            
            # 2. בדיקה לפי System ID (המספר 99...) שנמצא בכותרת
            # זה חשוב כי ברשימת ההחרגה יש בד"כ מספרי מערכת, ובמעבדה ה-UID הוא IE
            if not is_excluded:
                # מנסים לחלץ 99... מהכותרת
                m = re.search(r'(99\d+)', str(item['raw_header']))
                if m and m.group(1) in excluded_set:
                    is_excluded = True
            
            if is_excluded:
                known_list.append(item)
            else:
                main_list.append(item)

        # חיתוך המגבלה רק על הרשימה הראשית
        if len(main_list) > MAX_FINAL:
            main_list = main_list[:MAX_FINAL]

        # החזרה מפוצלת כדי שה-GUI ידע לבנות את העץ נכון
        return {'main': main_list, 'known': known_list, 'filtered': []}    
    
    @lru_cache(maxsize=10000)
    def _is_word_too_common(self, word, threshold=5000):
        """
        Check existing index stats to see if a word is essentially a stop-word.
        Uses LRU Cache to avoid hitting the index repeatedly for 'אמר' or 'על'.
        """
        try:
            # Tantivy allows checking document frequency for a term
            # Note: Create a Term object for the specific field
            # בגרסאות מסוימות של tantivy-py הפקודה היא doc_freq
            # אנו בודקים כמה מסמכים מכילים את המילה
            count = self.lab_searcher.doc_freq(self.lab_index.schema.get_field(self.LAB_FINGERPRINT_FIELD), word)
            return count > threshold
        except Exception:
            # במקרה של שגיאה או אם הפונקציה לא נתמכת, נניח שהמילה לא נפוצה מדי כדי לא לפספס
            return False

    def _is_phrase_statistically_weak(self, phrase_text):
        """
        Returns True if the phrase consists ONLY of extremely common words.
        If it has at least one 'rare' anchor word, it returns False (keep it).
        """
        # מנקים סימני פיסוק ומפרקים למילים
        words = re.findall(r"[\w\u0590-\u05FF]+", phrase_text)
        if not words:
            return True # Empty phrase is weak
            
        rare_anchors = 0
        
        for w in words:
            # אנו משתמשים בקידוד של שמידמן כי זה מה ששמור באינדקס,
            # אבל לבדיקת תדירות אפשר לבדוק גם את המילה הגולמית אם האינדקס שומר אותה,
            # או את ה-Fingerprint שלה.
            # נניח שאנו בודקים את ה-Fingerprint כי זה השדה המאונדקס שלנו:
            fp_word = encode_word_shmidman(w)
            if not fp_word: continue
            
            # אם המילה *אינה* נפוצה מדי, מצאנו עוגן!
            if not self._is_word_too_common(fp_word):
                rare_anchors += 1
        
        # אם לא מצאנו אפילו מילה נדירה אחת, המשפט חלש
        return rare_anchors == 0
    
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
    lab_logger = logging.getLogger("GenizahLab")
    if lab_logger.handlers:
        # Check if it only has NullHandler (length 1 and is NullHandler)
        # If so, we still want to add the real handlers.
        # But for simplicity in this specific task context:
        # The user instruction says: "If using a global logger, use NullHandler as default".
        # When this runs, we want to ADD file/stream handlers.
        # However, `logging.getLogger` returns the same instance.
        # So we should just check if we have "real" handlers or just clear and re-add.
        # Let's follow the standard pattern:
        # If it has handlers other than NullHandler, return.
        has_real = any(not isinstance(h, logging.NullHandler) for h in lab_logger.handlers)
        if has_real:
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
        raw_input = str(sys_id) if sys_id is not None else ""
        sys_id = "".join(ch for ch in raw_input if ch.isdigit())
        
        if raw_input != sys_id and raw_input:
             LOGGER.debug("Normalized sys_id: raw=%r -> %r", raw_input, sys_id)

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
        # 1. בדיקה במטמון הקיים
        if system_id in self.nli_cache: 
            return self.nli_cache[system_id]
        
        # 2. בדיקה ב-CSV Bank (התיקון המרכזי: שליפה מקומית במקום רשת)
        if system_id in self.csv_bank:
            row = self.csv_bank[system_id]
            meta = {
                'shelfmark': row['shelfmark'], 
                'title': row['title'],
                'desc': '', 
                'fl_ids': [], 
                'thumb_url': None, 
                'thumb_checked': True # מסמנים כבדוק כדי למנוע ניסיונות חוזרים להורדת תמונה
            }
            self.nli_cache[system_id] = meta
            return meta

        # 3. רק אם אין ברירה (לא במטמון ולא ב-CSV) - פנייה לרשת
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
        
    def batch_fetch_shelfmarks(self, system_ids, progress_callback=None, use_network=True):
        """
        Populate metadata cache. 
        use_network=False -> Only loads from local CSV/Cache (Instant).
        use_network=True  -> Fetches missing items from NLI.
        """
        # שלב א': שליפה מהירה מה-CSV (ללא רשת)
        for sid in system_ids:
            if sid not in self.nli_cache and sid in self.csv_bank:
                self.fetch_nli_data(sid) # זה שולף מה-CSV אוטומטית כעת
        
        # אם ביקשנו רק עבודה מקומית, עוצרים כאן
        if not use_network:
            return

        # שלב ב': זיהוי מה *באמת* חסר 
        to_fetch = [sid for sid in system_ids if sid not in self.nli_cache]
        
        if not to_fetch:
            if progress_callback:
                for i, sid in enumerate(system_ids):
                     progress_callback(i + 1, len(system_ids), sid)
            return

        # שלב ג': הורדה מהרשת (רק אם use_network=True)
        futures = {self.nli_executor.submit(self._fetch_single_worker, sid): sid for sid in to_fetch}
        current_progress = len(system_ids) - len(to_fetch)
        
        for future in as_completed(futures):
            sid, meta = future.result()
            self.nli_cache[sid] = meta
            current_progress += 1
            if progress_callback:
                progress_callback(current_progress, len(system_ids), sid)
        
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
        """
        Scans composition chunks against the index.
        Returns aggregated results with WIDE source context.
        FIX: Common phrases (> max_freq) are now moved to 'filtered' instead of being discarded.
        """
        # 1. פירוק הטקסט המקורי לטוקנים
        tokens = re.findall(Config.WORD_TOKEN_PATTERN, full_text)
        if len(tokens) < chunk_size: return None
        chunks = [tokens[i:i + chunk_size] for i in range(len(tokens) - chunk_size + 1)]

        doc_hits_main = defaultdict(lambda: {'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(), 'patterns': set()})
        doc_hits_filtered = defaultdict(lambda: {'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(), 'patterns': set()})

        total_chunks = len(chunks)
        
        # 2. סריקת הצ'אנקים
        for i, chunk in enumerate(chunks):
            if progress_callback and i % 10 == 0: progress_callback(i, total_chunks)
            
            # בניית שאילתה
            t_query = self.build_tantivy_query(chunk, mode)
            regex = self.build_regex_pattern(chunk, mode, 0)
            if not regex: continue

            # בדיקה: האם הביטוי נמצא ב"טקסט לסינון" (Filter Text)?
            is_text_filtered = False
            if filter_text:
                if regex.search(filter_text):
                    is_text_filtered = True

            try:
                # חיפוש באינדקס
                query = self.index.parse_query(t_query, ["content"])
                hits = self.searcher.search(query, 50).hits
                
                is_freq_filtered = len(hits) > max_freq 
                
                for score, doc_addr in hits:
                    doc = self.searcher.doc(doc_addr)
                    content = doc['content'][0]
                    
                    # וידוא התאמה מדויקת עם Regex
                    if regex.search(content):
                        uid = doc['unique_id'][0]
                        
                        # ניתוב למפה המתאימה
                        if is_text_filtered or is_freq_filtered:
                            rec = doc_hits_filtered[uid]
                        else:
                            rec = doc_hits_main[uid]

                        rec['head'] = doc['full_header'][0]
                        rec['src'] = doc['source'][0]
                        rec['content'] = content
                        rec['matches'].append(regex.search(content).span())
                        # שמירת האינדקסים של המילים בטקסט *המקור* שנמצאו
                        rec['src_indices'].update(range(i, i + chunk_size))
                        rec['patterns'].add(regex.pattern)
            except Exception as e:
                LAB_LOGGER.warning(f"Failed composition chunk processing at token {i}: {e}")

        # 3. בניית התוצאות עם הקשר רחב (Wide Context Logic)
        def build_items(hits_dict):
            final_items = []
            
            for uid, data in hits_dict.items():
                src_indices = sorted(list(data['src_indices']))
                src_snippets = []
                
                if src_indices:
                    # א. קיבוץ אינדקסים קרובים
                    clusters = []
                    if src_indices:
                        curr_cluster = [src_indices[0]]
                        for idx in src_indices[1:]:
                            if idx - curr_cluster[-1] < 60: 
                                curr_cluster.append(idx)
                            else:
                                clusters.append(curr_cluster)
                                curr_cluster = [idx]
                        clusters.append(curr_cluster)
                    
                    # ב. בניית הטקסט לכל קלאסטר
                    for cl in clusters:
                        start_ctx = max(0, cl[0] - 200)
                        end_ctx = min(len(tokens), cl[-1] + 201)
                        
                        cl_set = set(cl)
                        words_out = []
                        for k in range(start_ctx, end_ctx):
                            word = tokens[k]
                            if k in cl_set:
                                words_out.append(f"*{word}*") 
                            else:
                                words_out.append(word)
                        
                        src_snippets.append(f"... {' '.join(words_out)} ...")

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
                    fragment = data['content'][start:s] + \
                               f"<span style='color:#ff0000; font-weight:bold;'>{data['content'][s:e]}</span>" + \
                               data['content'][e:end]
                    ms_snips.append(fragment)

                combined_pattern = "|".join(list(data['patterns'])) if data.get('patterns') else ""

                final_items.append({
                    'score': score, 
                    'uid': uid,
                    'raw_header': data['head'], 
                    'src_lbl': data['src'],
                    'source_ctx': "\n\n".join(src_snippets),
                    'text': "\n...\n".join(ms_snips),
                    'highlight_pattern': combined_pattern
                })
                
            final_items.sort(key=lambda x: x['score'], reverse=True)
            return final_items

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
        # 1. איסוף IDs לטובת מטא-דאטה
        ids = []
        for i in items:
            if check_cancel and check_cancel(): return None, None, None
            if i.get('type') == 'manuscript' and i.get('sys_id'):
                ids.append(i['sys_id'])
            else:
                parsed = self.meta_mgr.parse_header_smart(i['raw_header'])
                if parsed and parsed[0]: ids.append(parsed[0])

        if status_callback:
            status_callback(tr("Fetching metadata..."))

        # טעינת מטא-דאטה (כעת מהירה בזכות התיקון הקודם)
        self.meta_mgr.batch_fetch_shelfmarks([x for x in ids if x], progress_callback=progress_callback)

        if status_callback:
            status_callback(tr("Grouping results..."))

        # 2. הכנת נתונים למיון
        IGNORE_PREFIXES = {'קטע', 'קטעי', 'גניזה', 'לא', 'מזוהה', 'חיבור', 'פילוסופיה', 'הלכה', 'שירה', 'פיוט', 'מסמך', 'מכתב', 'ספרות', 'סיפורת', 'יפה', 'דרשות', 'פרשנות', 'מקרא', 'בפילוסופיה', 'קטעים', 'וספרות', 'מוסר', 'הגות', 'וחכמת', 'הלשון', 'פירוש', 'תפסיר', 'שרח', 'על', 'ספר', 'כתאב', 'משנה', 'תלמוד'}

        def _get_clean_words(t):
            if not t: return []
            clean = re.sub(r'[^\w]', ' ', t)
            return [w for w in clean.split() if len(w) > 1]

        def _get_signature(title_str):
            words = _get_clean_words(title_str)
            while words and words[0] in IGNORE_PREFIXES: words.pop(0)
            if not words: return None
            # חתימה: שתי המילים המשמעותיות הראשונות
            return f"{words[0]} {words[1]}" if len(words) >= 2 else words[0]

        # 3. אלגוריתם ה-Grouping החדש (Dictionary Based - O(N))
        # במקום לולאה כפולה, אנו ממפים את כל הפריטים לפי החתימה שלהם
        
        groups_map = defaultdict(list)
        wrapped_items = []
        total_items = len(items)

        for idx, item in enumerate(items):
            # עדכון GUI בתדירות נמוכה למניעת קיפאון
            if progress_callback and idx % 100 == 0:
                progress_callback(idx, total_items)
            
            if check_cancel and check_cancel(): return None, None, None

            # חילוץ כותרת
            if item.get('type') == 'manuscript' and item.get('sys_id'):
                sid = item['sys_id']
            else:
                sid, _ = self.meta_mgr.parse_header_smart(item['raw_header'])

            meta = self.meta_mgr.nli_cache.get(sid, {})
            t = meta.get('title', '').strip()
            shelfmark = self.meta_mgr.get_shelfmark_from_header(item['raw_header']) or meta.get('shelfmark', 'Unknown')
            
            sig = _get_signature(t)
            
            w_item = {
                'item': item, 
                'title': t, 
                'signature': sig,
                'shelfmark': shelfmark,
                'grouped': False
            }
            wrapped_items.append(w_item)
            
            if sig:
                groups_map[sig].append(w_item)

        # 4. סינון קבוצות לפי הסף (Threshold)
        appendix = defaultdict(list)
        summary = defaultdict(list)

        for sig, group_items in groups_map.items():
            if len(group_items) > threshold:
                # הקבוצה גדולה מספיק - מעבירים לנספח
                for w in group_items:
                    w['grouped'] = True
                    appendix[sig].append(w['item'])
                    summary[sig].append(w['shelfmark'])

        # 5. יצירת הרשימה הראשית (כל מה שלא קובץ)
        main_list = [w['item'] for w in wrapped_items if not w['grouped']]
        
        # מיון לפי ציון יורד
        main_list.sort(key=lambda x: x['score'], reverse=True)
        
        # עדכון סופי ל-GUI
        if progress_callback:
            progress_callback(total_items, total_items)

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
