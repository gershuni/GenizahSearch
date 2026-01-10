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
import csv
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

STANDARD_HEBREW_DIST = {
    'י': 11.5, 'ו': 10.2, 'ה': 8.5, 'א': 8.2, 'ל': 7.2, 'מ': 6.5, 'ת': 5.5,
    'ב': 5.2, 'ר': 5.1, 'ש': 4.3, 'נ': 4.0, 'ד': 2.8, 'כ': 2.5, 'ע': 2.4,
    'ח': 2.3, 'ק': 2.0, 'פ': 1.8, 'ס': 1.5, 'ט': 1.1, 'ז': 0.9, 'ג': 0.8,
    'צ': 0.8, 'ץ': 0.4, 'ף': 0.3, 'ך': 0.3, 'ם': 2.5, 'ן': 1.0
}

def encode_word_shmidman(word: str, freq_map=None) -> str:
    """Encode a single word by selecting its two rarest Hebrew characters."""
    if freq_map is None:
        freq_map = HEBREW_FREQ
    letters = []
    for idx, ch in enumerate(word):
        if ch in freq_map:
            letters.append((idx, ch, freq_map[ch]))

    if not letters:
        return ""

    rarest = sorted(letters, key=lambda item: (-item[2], item[0]))[:3]
    rarest_sorted = sorted(rarest, key=lambda item: item[0])
    return "".join(ch for _, ch, _ in rarest_sorted)


def text_to_fingerprint(text: str, freq_map=None) -> str:
    """Convert free text into a fingerprint representation."""
    tokens = re.findall(Config.WORD_TOKEN_PATTERN, text or "")
    encoded_tokens = []
    for tok in tokens:
        encoded = encode_word_shmidman(tok, freq_map=freq_map)
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
        self.candidate_limit = 5000
        self.min_should_match = 75
        self.gap_penalty = 2
        
        # Scoring Weights
        self.length_bonus_factor = 1.5
        self.common_penalty_factor = 0.1
        self.unique_bonus_base = 100
        self.density_penalty = 0.2
        self.coverage_power = 2.0
        self.order_bonus = 10.0
        
        # --- New Settings: Noise Suppression (Stop Words) ---
        self.stop_word_score = 1.0       # Score for short words (<3 chars)
        self.common_3char_score = 2.0    # Score for common 3-letter words
        
        # Composition Settings
        self.comp_chunk_limit = 500
        self.comp_min_score = 70
        self.comp_max_final_results = 200
        
        # Deep Scan Settings
        self.lab_scan_limit = 50000

        # Display Limit
        self.lab_display_limit = 500

        self.use_dynamic_weights = False

        # Variant Search Settings (affects standard search when using variants mode)
        self.variant_min_word_len = 2      # Words <= this length get only 1 change
        self.variant_max_changes = 2       # Max character changes per word
        self.variant_aggressive = False    # If True, ignore length limits (like old behavior)

        self.load()

    def load(self):
        if os.path.exists(Config.LAB_CONFIG_FILE):
            try:
                with open(Config.LAB_CONFIG_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.custom_variants = data.get('custom_variants', {})
                    self.use_dynamic_weights = data.get('use_dynamic_weights', False)
                    self.candidate_limit = data.get('candidate_limit', 2000)
                    self.min_should_match = data.get('min_should_match', 60)
                    self.gap_penalty = data.get('gap_penalty', 2)
                    
                    self.length_bonus_factor = data.get('length_bonus_factor', 1.5)
                    self.common_penalty_factor = data.get('common_penalty_factor', 0.1)
                    self.unique_bonus_base = data.get('unique_bonus_base', 100)
                    self.density_penalty = data.get('density_penalty', 0.2)
                    self.coverage_power = data.get('coverage_power', 2.0)
                    self.order_bonus = data.get('order_bonus', 10.0)

                    # Load noise settings
                    self.stop_word_score = data.get('stop_word_score', 1.0)
                    self.common_3char_score = data.get('common_3char_score', 2.0)

                    self.comp_chunk_limit = data.get('comp_chunk_limit', 200)
                    self.comp_min_score = data.get('comp_min_score', 70)
                    self.comp_max_final_results = data.get('comp_max_final_results', 100)

                    self.lab_scan_limit = data.get('lab_scan_limit', 50000)
                    self.lab_display_limit = data.get('lab_display_limit', 500)

                    # Load variant settings
                    self.variant_min_word_len = data.get('variant_min_word_len', 2)
                    self.variant_max_changes = data.get('variant_max_changes', 2)
                    self.variant_aggressive = data.get('variant_aggressive', False)
            except Exception: pass

    def save(self):
        try:
            os.makedirs(Config.LAB_DIR, exist_ok=True)
            with open(Config.LAB_CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump({
                    'custom_variants': self.custom_variants,
                    'use_dynamic_weights': self.use_dynamic_weights,
                    'candidate_limit': self.candidate_limit,
                    'min_should_match': self.min_should_match,
                    'gap_penalty': self.gap_penalty,
                    
                    'length_bonus_factor': self.length_bonus_factor,
                    'common_penalty_factor': self.common_penalty_factor,
                    'unique_bonus_base': self.unique_bonus_base,
                    'density_penalty': self.density_penalty,
                    'coverage_power': self.coverage_power,
                    'order_bonus': self.order_bonus,
                    
                    # Save noise settings
                    'stop_word_score': self.stop_word_score,
                    'common_3char_score': self.common_3char_score,

                    'comp_chunk_limit': self.comp_chunk_limit,
                    'comp_min_score': self.comp_min_score,
                    'comp_max_final_results': self.comp_max_final_results,

                    'lab_scan_limit': self.lab_scan_limit,
                    'lab_display_limit': self.lab_display_limit,

                    # Variant settings
                    'variant_min_word_len': self.variant_min_word_len,
                    'variant_max_changes': self.variant_max_changes,
                    'variant_aggressive': self.variant_aggressive
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
        self.dynamic_rank_map = None

        # Try load dynamic weights
        if os.path.exists(Config.LAB_WEIGHTS_FILE):
            try:
                with open(Config.LAB_WEIGHTS_FILE, 'r', encoding='utf-8') as f:
                    self.dynamic_rank_map = json.load(f)
            except Exception:
                pass

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

        # 1. Always Calculate Dynamic Weights First
        LAB_LOGGER.info("Calculating dynamic corpus statistics...")
        self.dynamic_rank_map = calculate_smart_weights(Config.FILE_V8, sample_size=None)

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
        
        # The critical fields
        builder.add_text_field(self.LAB_FINGERPRINT_FIELD, stored=False, tokenizer_name="simple") # Static
        builder.add_text_field("fingerprint_dyn", stored=False, tokenizer_name="simple")          # Dynamic
        
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        builder.add_text_field("source", stored=True)
        builder.add_text_field("content", stored=True, tokenizer_name="simple")

        schema = builder.build()
        index = tantivy.Index(schema, path=Config.LAB_INDEX_DIR)
        self._ensure_lab_tokenizers(index)
        writer = index.writer(heap_size=50_000_000)

        # --- Pre-calculation for progress percentage ---
        def count_documents(fname, label):
            if not os.path.exists(fname): return 0
            count = 0
            try:
                with open(fname, 'r', encoding='utf-8-sig') as f:
                    for line in f:
                        if label == "V0.8" and line.startswith("==>"): count += 1
                        elif label == "V0.7" and line.startswith("###"): count += 1
            except Exception: pass
            return count

        estimated_total = count_documents(Config.FILE_V8, "V0.8") + count_documents(Config.FILE_V7, "V0.7")
        LAB_LOGGER.info(f"Estimated total docs: {estimated_total}")

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

                            fp_static = text_to_fingerprint(original, freq_map=HEBREW_FREQ)
                            fp_dyn = text_to_fingerprint(original, freq_map=self.dynamic_rank_map)

                            sm = self.meta_mgr.get_shelfmark_from_header(chead) or "Unknown"

                            writer.add_document(tantivy.Document(
                                unique_id=str(cid),
                                text_normalized=norm,
                                fingerprint=fp_static,
                                fingerprint_dyn=fp_dyn,
                                content=original,
                                full_header=str(chead),
                                shelfmark=str(sm),
                                source=str(label)
                            ))
                            total_docs += 1
                            if progress_callback and total_docs % 1000 == 0:
                                progress_callback(total_docs, estimated_total)
                        
                        chead = line.replace("==>", "").replace("<==", "").strip() if label == "V0.8" else line
                        cid = self.meta_mgr.extract_unique_id(line)
                        ctext = [] 
                    else:
                        ctext.append(line)
                
                # Last doc
                if cid and ctext:
                    original = "\n".join(ctext)
                    fp_static = text_to_fingerprint(original, freq_map=HEBREW_FREQ)
                    fp_dyn = text_to_fingerprint(original, freq_map=self.dynamic_rank_map)

                    writer.add_document(tantivy.Document(
                        unique_id=str(cid),
                        text_normalized=self.lab_index_normalize(original),
                        fingerprint=fp_static,
                        fingerprint_dyn=fp_dyn,
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

    def _create_lab_query(self, query_str, slop=0, field_name=None):
        """
        Helper to construct the Tantivy query object based on settings.
        """
        if field_name is None:
            field_name = self.LAB_FINGERPRINT_FIELD

        tokens = query_str.split()
        if not tokens:
            return None

        # If 100% match required, use Phrase Query
        if self.settings.min_should_match >= 100:
            final_query_str = f'{field_name}:"{query_str}"~{slop}'
        else:
            # OR query
            clauses = [f'{field_name}:{t}' for t in tokens]
            final_query_str = " OR ".join(clauses)

        # Try parsing strategies
        strategies = [
            lambda: self.lab_index.parse_query(final_query_str),
            lambda: self.lab_index.parse_query(final_query_str, [field_name]),
            lambda: self.lab_index.parse_query(final_query_str, [self.lab_index.schema.get_field(field_name)])
        ]

        for strategy in strategies:
            try:
                return strategy()
            except Exception:
                continue

        LAB_LOGGER.error("All query strategies failed.")
        return None

    def _execute_batched_search(self, query_obj, progress_callback=None, limit_override=None):
        """
        Executes a Tantivy search in memory-safe batches.
        Yields (score, doc_address) tuples.
        """
        if not query_obj or not self.lab_searcher:
            return

        BATCH_SIZE = 5000
        MAX_SCAN_LIMIT = limit_override if limit_override else 50000

        # Determine strict limit
        limit = MAX_SCAN_LIMIT

        # 1. Fetch all candidate pointers (lightweight tuples)
        # Note: tantivy-py search() returns all hits at once, but they are just (score, addr).
        # This is memory-safe even for 50k items. The heavy lifting (doc loading) happens in the loop.
        try:
            res = self.lab_searcher.search(query_obj, limit)
        except Exception as e:
            LAB_LOGGER.warning(f"Search execution failed: {e}")
            return

        hits = res.hits
        total_hits = len(hits)

        # 2. Iterate in batches to allow for progress updates / UI breathing
        for i in range(0, total_hits, BATCH_SIZE):
            batch = hits[i : i + BATCH_SIZE]

            if progress_callback:
                # Send numeric progress for ProgressBar (i, total)
                try:
                    progress_callback(i, total_hits)
                except Exception:
                    pass
                # Send text status for Label
                progress_callback(f"Scanning items {i}-{min(i+BATCH_SIZE, total_hits)} / {total_hits}...")

            for hit in batch:
                yield hit

    def _get_term_weight(self, fp):
        """
        Calculates importance using User Configurable Stop-Word scores.
        """
        raw_weight = 0
        for char in fp:
            raw_weight += HEBREW_FREQ.get(char, 0)
        
        # 1. Words too short (<3 chars)
        if len(fp) < 3:
            return self.settings.stop_word_score 
        
        # 2. Common 3-letter words (low weight)
        if len(fp) == 3 and raw_weight < 18:
            return self.settings.common_3char_score

        # 3. Regular/Rare words
        final_weight = raw_weight
        
        # Length bonus only for significant words
        if len(fp) > 3:
            final_weight *= self.settings.length_bonus_factor
            
        return final_weight

    def _calculate_match_metrics(self, text, query_fingerprints_list, original_query_str, freq_map=None):
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
            fp = encode_word_shmidman(word, freq_map=freq_map)
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
            
            # Track quantities: how many times have we seen each word in the current window?
            seen_counts = defaultdict(int)
            
            # Track order
            last_valid_query_idx = -1
            sequential_chain_length = 0
            
            # Initialize by start word
            if matches[i]['q_indices']:
                last_valid_query_idx = matches[i]['q_indices'][0]

            for j in range(i, min(total_matches, i + lookahead_limit)):
                m = matches[j]
                
                # Check physical distance
                dist = m['end'] - matches[i]['start']
                if dist > 450: break 
                
                fp = m['fp']
                w = m['weight']
                
                # How many times does this word appear in the original query?
                allowed_count = len(fp_to_query_indices[fp])
                
                # How many times have we seen it in this window so far?
                seen_counts[fp] += 1
                
                # Calculate score for this specific word
                word_score = 0
                
                if seen_counts[fp] <= allowed_count:
                    # Valid occurrence (first or second allowed)
                    # Full score
                    word_score = (w * unique_bonus)
                else:
                    # Redundant occurrence (garbage). Word found enough times.
                    # Drastically reduced score (or 0 if user set 0)
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
        Generates a snippet with asterisk markers (*text*) for highlighting.
        """
        if not text: return ""
        if not matches: return text[:300]

        start_m_idx, end_m_idx = best_window
        
        # Guard indices
        start_m_idx = max(0, start_m_idx)
        end_m_idx = min(len(matches) - 1, end_m_idx)

        # 1. Determine snippet bounds (100 chars context)
        padding = 100
        snippet_start_char = max(0, matches[start_m_idx]['start'] - padding)
        snippet_end_char = min(len(text), matches[end_m_idx]['end'] + padding)
        
        # Cosmetic: Don't cut in middle of word
        if snippet_start_char > 0:
            next_space = text.find(' ', snippet_start_char)
            if next_space != -1 and next_space < matches[start_m_idx]['start']:
                snippet_start_char = next_space + 1

        # 2. Collect relevant matches
        relevant_matches = matches[start_m_idx : end_m_idx + 1]
        
        # 3. Build text
        out_parts = []
        
        if snippet_start_char > 0: out_parts.append("... ")
        
        current_idx = snippet_start_char
        
        for m in relevant_matches:
            if m['start'] < snippet_start_char: continue
            if m['end'] > snippet_end_char: break
            
            # Plain text
            if m['start'] > current_idx:
                plain = text[current_idx : m['start']]
                out_parts.append(plain.replace('*', ''))
            
            # Highlighted word (Asterisks)
            word = text[m['start'] : m['end']]
            out_parts.append(f"*{word.replace('*', '')}*")
            
            current_idx = m['end']
        
        # Remainder
        if current_idx < snippet_end_char:
            out_parts.append(text[current_idx : snippet_end_char].replace('*', ''))
            
        if snippet_end_char < len(text): out_parts.append(" ...")
        
        final_text = "".join(out_parts)
        # Flatten for table display
        return final_text.replace("\n", " ").replace("\r", " ")

    def lab_search(self, query_str, mode='variants', progress_callback=None, gap=0, deep_scan=False, scan_limit=50000):
        if not self.lab_searcher: return []

        # Determine strategy: Static or Dynamic
        use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None

        target_field = "fingerprint_dyn" if use_dyn else self.LAB_FINGERPRINT_FIELD
        target_map = self.dynamic_rank_map if use_dyn else HEBREW_FREQ

        # 1. Prepare Fingerprints
        fp_str = text_to_fingerprint(query_str, freq_map=target_map)
        if not fp_str: return []
        
        query_fp_list = fp_str.split()
        
        # 2. Fetch Candidates
        slop = max(50, int(self.settings.gap_penalty) * 10) 

        query_obj = self._create_lab_query(fp_str, slop, field_name=target_field)
        if not query_obj: return []

        results = []
        min_match_pct = self.settings.min_should_match

        # 3. Process
        if deep_scan:
            # Use Deep Scan batched iterator
            def batch_cb(*args):
                if progress_callback:
                    try:
                        progress_callback(*args)
                    except Exception:
                        pass

            iterator = self._execute_batched_search(query_obj, progress_callback=batch_cb, limit_override=scan_limit)
        else:
            # Standard Fast Method
            try:
                # Limit 5000 for standard scan
                res = self.lab_searcher.search(query_obj, 5000)
                iterator = res.hits
            except Exception:
                iterator = []

        for score, doc_addr in iterator:
            try:
                doc = self.lab_searcher.doc(doc_addr)
                content = doc['content'][0]
                uid = doc['unique_id'][0]

                # --- Core: Calculate Score & Find Matches ---
                custom_score, matches, best_window = self._calculate_match_metrics(content, query_fp_list, query_str, freq_map=target_map)
                
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
                html_snippet = smart_snippet # No HTML conversion needed, pure markers

                start_idx, end_idx = best_window
                relevant_matches = matches[start_idx : end_idx + 1]
                
                # Collect unique words found (e.g., "מאמתי", "קורין", "את", "שמע")
                found_words = list(set(m['word'] for m in relevant_matches))
                
                # Sort by length descending (so "wordLong" matches before "word")
                found_words.sort(key=len, reverse=True)
                
                # Create a regex OR pattern: (word1|word2|...)
                # We use re.escape to handle any special chars in the text
                highlight_regex_str = "|".join(re.escape(w) for w in found_words) if found_words else ""
                
                # Populate display metadata correctly
                display_meta = self.meta_mgr.get_display_data(doc['full_header'][0], doc['source'][0])

                results.append({
                    'sort_score': custom_score,
                    'display': display_meta,
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
        
        # Add all V0.8 results
        final_list.extend(v8_map.values())
        
        # Add V0.7 results *only* if UID not in V0.8
        for r in results:
            if r['display']['source'] != "V0.8": # V0.7 or others
                if r['uid'] not in v8_map:
                    final_list.append(r)

        # Finally, sort unified list by highest score
        final_list.sort(key=lambda x: x['sort_score'], reverse=True)

        return final_list

    def lab_composition_search(self, full_text, mode='variants', progress_callback=None, chunk_size=None, excluded_ids=None, filter_text=None, deep_scan=False, scan_limit=50000):
        """
        Scans a composition using Lab Mode.
        UPGRADES:
        1. Filters common phrases.
        2. Boosts V0.8.
        3. FIX: Separates excluded/known manuscripts.
        4. Supports Filter Text and Batching.
        """
        if not full_text:
            return {'main': [], 'filtered': [], 'known': []} # Added known

        # Determine strategy: Static or Dynamic
        use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None
        target_field = "fingerprint_dyn" if use_dyn else self.LAB_FINGERPRINT_FIELD
        target_map = self.dynamic_rank_map if use_dyn else HEBREW_FREQ

        # Normalize exclusion list for fast lookup
        excluded_set = set(str(x) for x in (excluded_ids or []))

        # User settings
        PER_CHUNK_LIMIT = self.settings.comp_chunk_limit
        MIN_SCORE_THRESHOLD = self.settings.comp_min_score
        MAX_FINAL = self.settings.comp_max_final_results
        min_pct_ratio = self.settings.min_should_match / 100.0

        # (Part 1: Tokenization)
        tokens = re.findall(r"[\w\u0590-\u05FF\']+", full_text)
        c_size = chunk_size if chunk_size else 15
        step = max(1, int(c_size * 0.5)) 
        
        chunks_data = []
        for i in range(0, max(1, len(tokens) - c_size + 1), step):
            chunks_data.append((i, tokens[i : i + c_size]))
        if len(tokens) < c_size: chunks_data = [(0, tokens)]

        total_chunks = len(chunks_data)
        results_map = {} 

        # (Part 2: Scanning)
        for i, (token_start_idx, chunk_tokens) in enumerate(chunks_data):
            if progress_callback and i % 5 == 0: progress_callback(i, total_chunks)
            chunk_text = " ".join(chunk_tokens)
            
            if self._is_phrase_statistically_weak(chunk_text): continue

            fp_str = text_to_fingerprint(chunk_text, freq_map=target_map)
            if not fp_str or len(chunk_tokens) < 4: continue
            
            fp_list = fp_str.split()
            needed_unique_fps = set(fp_list) 

            # Query with Boost
            query_tokens = fp_str.split()
            clauses = [f'{target_field}:{t}' for t in query_tokens]
            core_query = " OR ".join(clauses)
            final_query_str = f'({core_query}) AND (source:"V0.8"^10 OR source:"V0.7")'
            
            q_obj = None
            try:
                q_obj = self.lab_index.parse_query(final_query_str)
            except:
                try:
                    q_obj = self.lab_index.parse_query(core_query)
                except: continue

            if not q_obj: continue

            iterator = []
            if deep_scan:
                batch_cb = None
                if progress_callback:
                    batch_cb = lambda *args: progress_callback(*args) if callable(progress_callback) else None
                iterator = self._execute_batched_search(q_obj, progress_callback=batch_cb, limit_override=scan_limit)
            else:
                try:
                    res = self.lab_searcher.search(q_obj, 5000)
                    iterator = res.hits
                except Exception:
                    iterator = []

            for score, doc_addr in iterator:
                try:
                    doc = self.lab_searcher.doc(doc_addr)
                    content = doc['content'][0]
                    uid = doc['unique_id'][0]

                    # --- Filter Text Logic ---
                    is_filtered_match = False
                    if filter_text and filter_text in content:
                        is_filtered_match = True

                    match_score, matches, best_window = self._calculate_match_metrics(content, fp_list, chunk_text, freq_map=target_map)
                    
                    found_unique_fps = set(m['fp'] for m in matches[best_window[0]:best_window[1]+1])
                    common_fps = found_unique_fps.intersection(needed_unique_fps)
                    if len(needed_unique_fps) > 0:
                        if (len(common_fps) / len(needed_unique_fps)) < min_pct_ratio: continue
                    
                    if match_score < MIN_SCORE_THRESHOLD: continue

                    if uid not in results_map:
                        results_map[uid] = {
                            'uid': uid, 'total_score': 0, 'hits_count': 0,
                            'raw_header': doc['full_header'][0], 'source': doc['source'][0],
                            'content': content, 'best_chunk_score': -1,
                            'all_found_words': set(), 'src_indices': set(), 'ms_matches': [],
                            'is_text_filtered': False
                        }
                    rec = results_map[uid]

                    if is_filtered_match:
                        rec['is_text_filtered'] = True

                    rec['total_score'] += match_score
                    rec['hits_count'] += 1
                    token_end_idx = token_start_idx + len(chunk_tokens)
                    rec['src_indices'].update(range(token_start_idx, token_end_idx))
                    start_m, end_m = best_window
                    if matches:
                        rec['ms_matches'].append((matches[start_m]['start'], matches[end_m]['end']))
                        for m in matches[start_m : end_m + 1]: rec['all_found_words'].add(m['word'])
                except: pass

        # (Part 3: Result Processing)
        raw_final_items = []
        is_short_search = (total_chunks <= 3)

        for uid, data in results_map.items():
            if not is_short_search:
                if data['hits_count'] < 2 and data['total_score'] < 1000: continue 
            else:
                if data['total_score'] < 250: continue

            # Generate snippets
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
                    ms_snips.append(snip[:rs] + f"*{snip[rs:re_]}*" + snip[re_:])

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
                'full_text': data['content'],
                'is_text_filtered': data.get('is_text_filtered', False)
            }
            raw_final_items.append(item)

        # --- Sorting & Splitting Logic ---
        raw_final_items.sort(key=lambda x: x['score'], reverse=True)
        
        main_list = []
        known_list = []
        filtered_list = []
        
        for item in raw_final_items:
            # Check if manuscript is excluded
            is_excluded = False
            
            # 1. Check by UID (e.g. IE...)
            if str(item['uid']) in excluded_set:
                is_excluded = True
            
            # 2. Check by System ID (99...) found in header
            if not is_excluded:
                m = re.search(r'(99\d+)', str(item['raw_header']))
                if m and m.group(1) in excluded_set:
                    is_excluded = True
            
            if is_excluded:
                known_list.append(item)
            elif item.get('is_text_filtered'):
                filtered_list.append(item)
            else:
                main_list.append(item)

        # Truncate limit only on main list
        if len(main_list) > MAX_FINAL:
            main_list = main_list[:MAX_FINAL]

        # Split return so GUI builds tree correctly
        return {'main': main_list, 'known': known_list, 'filtered': filtered_list}
    
    @lru_cache(maxsize=10000)
    def _is_word_too_common(self, word, threshold=5000):
        """
        Check existing index stats to see if a word is essentially a stop-word.
        Uses LRU Cache to avoid hitting the index repeatedly for 'אמר' or 'על'.
        """
        try:
            # Tantivy allows checking document frequency for a term
            # Note: Create a Term object for the specific field
            # In some tantivy-py versions command is doc_freq
            # We check how many documents contain the word

            # Determine field based on setting
            use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None
            target_field = "fingerprint_dyn" if use_dyn else self.LAB_FINGERPRINT_FIELD

            count = self.lab_searcher.doc_freq(self.lab_index.schema.get_field(target_field), word)
            return count > threshold
        except Exception:
            # If error/unsupported, assume word is not too common to avoid missing it
            return False

    def _is_phrase_statistically_weak(self, phrase_text):
        """
        Returns True if the phrase consists ONLY of extremely common words.
        If it has at least one 'rare' anchor word, it returns False (keep it).
        """
        # Clean punctuation and split to words
        words = re.findall(r"[\w\u0590-\u05FF]+", phrase_text)
        if not words:
            return True # Empty phrase is weak
            
        rare_anchors = 0
        
        for w in words:
            # We use Shmidman encoding as stored in index,
            # but could check raw word if stored,
            # or its Fingerprint.
            # Assuming Fingerprint check as it's our indexed field:

            use_dyn = self.settings.use_dynamic_weights and self.dynamic_rank_map is not None
            target_map = self.dynamic_rank_map if use_dyn else HEBREW_FREQ

            fp_word = encode_word_shmidman(w, freq_map=target_map)
            if not fp_word: continue
            
            # If word is *not* too common, anchor found!
            if not self._is_word_too_common(fp_word):
                rare_anchors += 1
        
        # If no rare word found, phrase is weak
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

    @staticmethod
    def _get_documents_dir() -> str:
        """Best-effort Documents directory (Windows-aware), falling back to home."""
        documents_dir = None
        try:
            import ctypes.wintypes

            CSIDL_PERSONAL = 5  # My Documents
            buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
            ctypes.windll.shell32.SHGetFolderPathW(None, CSIDL_PERSONAL, None, 0, buf)
            if buf.value:
                documents_dir = buf.value
        except Exception:
            pass

        if not documents_dir or not os.path.isdir(documents_dir):
            for folder_name in ["Documents", "My Documents"]:
                candidate = os.path.join(os.path.expanduser("~"), folder_name)
                if os.path.isdir(candidate):
                    documents_dir = candidate
                    break

        return documents_dir if documents_dir and os.path.isdir(documents_dir) else os.path.expanduser("~")

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

    # 4. Output folders: always use Documents\GenizahSearchPro\Reports
    REPORTS_DIR = _pick_writable_dir(
        os.path.join(_get_documents_dir(), "GenizahSearchPro", "Reports"),
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
    LAB_WEIGHTS_FILE = os.path.join(LAB_DIR, "lab_weights.json")
    LAB_LOG_FILE = os.path.join(LAB_DIR, "lab_genizah.log")

    # 6. Bundled Internal Resources (Packaged inside the EXE/_internal)
    LIBRARIES_CSV = os.path.join(INTERNAL_DIR, "libraries.csv")
    OXFORD_DB = os.path.join(INTERNAL_DIR, "oxford_full_db.json")
    HELP_FILE = os.path.join(INTERNAL_DIR, "Help.html")

    # Settings
    SEARCH_LIMIT = 50000
    VARIANT_GEN_LIMIT = 8000
    REGEX_VARIANTS_LIMIT = 8000
    WORD_TOKEN_PATTERN = r"[\w\u0590-\u05FF\']+"
    NLI_IIIF_BASE = "https://iiif.nli.org.il/IIIFv21"
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    HTTP_HEADERS = {"User-Agent": USER_AGENT}
    
    @staticmethod
    def resource_path(relative_path: str) -> str:
        """Return absolute path to bundled resources."""
        return os.path.join(Config.INTERNAL_DIR, relative_path)


def dedupe_browse_map(browse_map):
    """
    Remove duplicate page entries per system ID, keeping the first occurrence
    of each page number.
    """
    cleaned = {}
    changed = False

    for sid, pages in browse_map.items():
        seen_p_nums = set()
        deduped_pages = []
        for page in pages:
            p_num = page.get('p_num')
            if p_num is None:
                deduped_pages.append(page)
                continue

            if p_num in seen_p_nums:
                changed = True
                continue

            seen_p_nums.add(p_num)
            deduped_pages.append(page)

        cleaned[sid] = deduped_pages

    return cleaned, changed

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

def load_app_config():
    """Load general app configuration (non-AI)."""
    cfg = {}
    if os.path.exists(Config.CONFIG_FILE):
        try:
            with open(Config.CONFIG_FILE, 'rb') as f:
                cfg = pickle.load(f)
        except Exception:
            pass
    return cfg

def save_app_config(new_data):
    """Update general app configuration with new keys."""
    try:
        cfg = load_app_config()
        cfg.update(new_data)
        if not os.path.exists(Config.INDEX_DIR): os.makedirs(Config.INDEX_DIR)
        with open(Config.CONFIG_FILE, 'wb') as f:
            pickle.dump(cfg, f)
    except Exception as e:
        LOGGER.error("Failed to save config: %s", e)

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
    """
    Generate spelling variants for Hebrew search terms using hierarchical maps.

    Improvements over original:
    1. Hierarchical maps: extended builds on basic, maximum builds on extended
    2. Single-pass generation instead of redundant multi-layer processing
    3. Dynamic max_changes based on term length to prevent combinatorial explosion
    4. LRU caching for frequently searched terms
    5. Early termination with smarter limit handling
    """

    # === HIERARCHICAL PAIR DEFINITIONS ===
    # Basic: High-confidence visual confusions in HTR
    _BASIC_PAIRS = [
        ('ד', 'ר'), ('כ', 'ב'), ('ה', 'ח'),
        ('ו', 'ז'), ('ו', 'י'), ('ו', 'ן'),
        ('ט', 'ת'), ('ס', 'ש')
    ]

    # Extended additions: Medium-confidence confusions
    _EXTENDED_ADDITIONS = [
        ('ת', 'ה'), ('י', 'ל'), ('א', 'ו'), ('ה', 'ר'), ('א', 'י'), ('ה', 'ת'),
        ('ר', 'י'), ('א', 'ח'), ('י', 'ר'), ('ק', 'ה'), ('נ', 'ו'), ('ל', 'ו'),
        ('ה', 'ו'), ('ו', 'א'), ('ה', 'י'), ('א', 'ה'), ('ר', 'ו'), ('ל', 'ר'),
        ('מ', 'י'), ('מ', 'א'), ('נ', 'י'), ('מ', 'ו'), ('י', 'ה'), ('א', 'ל'),
        ('ל', 'נ'), ('י', 'נ'), ('ת', 'י'), ('י', 'מ'), ('ת', 'ח'), ('ב', 'י'),
        ('ל', 'א'), ('ה', 'ם'), ('ר', 'ה'), ('ו', 'ש'), ('ל', 'כ'), ('י', 'ת'),
        ('א', 'מ'), ('ת', 'ר'), ('ב', 'ו'), ('ר', 'ל'), ('י', 'ש'), ('ב', 'ר'),
        ('א', 'ש'), ('ש', 'י'), ('ס', 'ם'), ('ש', 'ו'), ('ב', 'נ'), ('ו', 'מ'),
        ('מ', 'ש'), ('מ', 'ע'), ('ת', 'ו'), ('ר', 'א'), ('מ', 'ל'), ('מ', 'ב'),
        ('ד', 'י'), ('נ', 'ג'), ('ה', 'ד')
    ]

    # Maximum additions: Lower-confidence / aggressive confusions
    _MAXIMUM_ADDITIONS = [
        ("'", 'י'), ("'", 'ר'), ('א', 'ב'), ('א', 'ד'), ('א', 'ם'), ('א', 'נ'),
        ('א', 'ע'), ('א', 'ת'), ('ב', 'ד'), ('ב', 'ה'), ('ב', 'ל'), ('ב', 'מ'),
        ('ב', 'פ'), ('ב', 'ש'), ('ב', 'ת'), ('ג', 'ו'), ('ג', 'נ'), ('ד', 'ה'),
        ('ד', 'ו'), ('ד', 'כ'), ('ד', 'ל'), ('ה', 'ב'), ('ה', 'ך'), ('ה', 'כ'),
        ('ה', 'ל'), ('ה', 'מ'), ('ה', 'ק'), ('ה', 'ש'), ('ו', 'ג'), ('ו', 'ד'),
        ('ו', 'ח'), ('ו', 'כ'), ('ו', 'ם'), ('ו', 'ע'), ('ו', 'ת'), ('ז', 'י'),
        ('ח', 'י'), ('ח', 'מ'), ('ח', 'ר'), ('ח', 'ת'), ('ט', 'ע'), ('ט', 'ש'),
        ('י', 'ד'), ('י', 'ך'), ('י', 'כ'), ('י', 'ם'), ('י', 'ן'), ('י', 'ע'),
        ('כ', 'ה'), ('כ', 'ו'), ('כ', 'ל'), ('כ', 'מ'), ('כ', 'נ'), ('כ', 'פ'),
        ('כ', 'ר'), ('כ', 'ת'), ('ל', 'ד'), ('ל', 'ה'), ('ל', 'מ'), ('ל', 'ם'),
        ('ל', 'ע'), ('ל', 'ש'), ('ל', 'ת'), ('מ', 'ה'), ('מ', 'ח'), ('מ', 'נ'),
        ('מ', 'ס'), ('מ', 'ר'), ('מ', 'ת'), ('נ', 'ל'), ('נ', 'פ'), ('נ', 'ר'),
        ('נ', 'ת'), ('ס', 'מ'), ('ע', 'ל'), ('ע', 'מ'), ('ע', 'נ'), ('ע', 'ש'),
        ('פ', 'ב'), ('פ', 'כ'), ('פ', 'נ'), ('ק', 'ר'), ('ר', 'ב'), ('ר', 'ך'),
        ('ר', 'כ'), ('ר', 'מ'), ('ר', 'נ'), ('ר', 'ק'), ('ר', 'ש'), ('ר', 'ת'),
        ('ש', 'ב'), ('ש', 'ה'), ('ש', 'ט'), ('ש', 'ל'), ('ש', 'מ'), ('ש', 'ע'),
        ('ש', 'ר'), ('ת', 'ט'), ('ת', 'כ'), ('ת', 'ל'), ('ת', 'מ'), ('ת', 'ם'),
        ('ת', 'נ')
    ]

    # Tier configuration for balanced flexibility vs explosion prevention
    _TIER_CONFIG = {
        'variants': {'max_changes': 1, 'per_term_limit': 50},
        'variants_extended': {'max_changes': 2, 'per_term_limit': 150},
        'variants_maximum': {'max_changes': 2, 'per_term_limit': 300},
    }

    @staticmethod
    def make_multimap(pairs):
        """Create bidirectional mapping from character pairs."""
        m = defaultdict(set)
        for a, b in pairs:
            m[a].add(b)
            m[b].add(a)
        return m

    def __init__(self, settings=None):
        # Settings reference (can be updated later via set_settings)
        self._settings = settings

        # Cache for frequently searched terms
        self._cache = {}
        self._cache_max_size = 5000

        # Build maps (will include custom variants if settings has them)
        self._rebuild_maps()

    def _get_custom_pairs(self) -> tuple:
        """
        Parse custom variants from settings.
        Format: dict of 'a=b' style strings, e.g. {'ק=א': True, 'כו=מ': True}
        Returns (single_char_pairs, multi_char_pairs) tuple.
        Single-char pairs: both sides are 1 character (for regular variant maps)
        Multi-char pairs: at least one side has >1 character (for string substitution)
        """
        if not self._settings:
            return [], []

        custom = getattr(self._settings, 'custom_variants', {})
        if not custom:
            return [], []

        single_pairs = []
        multi_pairs = []
        for key in custom:
            if '=' in key:
                parts = key.split('=', 1)
                if len(parts) == 2:
                    a, b = parts[0].strip(), parts[1].strip()
                    if a and b:
                        if len(a) == 1 and len(b) == 1:
                            single_pairs.append((a, b))
                        else:
                            multi_pairs.append((a, b))
        return single_pairs, multi_pairs

    def _generate_multichar_variants(self, term: str) -> set:
        """
        Generate variants using multi-character substitution pairs.
        Each pair is applied as simple string replacement (bidirectional).
        Returns set of variant terms (may have different lengths than original).
        """
        _, multi_pairs = self._get_custom_pairs()
        if not multi_pairs:
            return set()

        variants = set()
        for a, b in multi_pairs:
            # a -> b substitution
            if a in term:
                variants.add(term.replace(a, b))
            # b -> a substitution
            if b in term:
                variants.add(term.replace(b, a))

        # Remove original term if present
        variants.discard(term)
        return variants

    def _rebuild_maps(self):
        """Build hierarchical maps including single-char custom variants from settings."""
        single_char_pairs, _ = self._get_custom_pairs()

        # Build hierarchical maps (each level includes all previous)
        self.basic_map = self.make_multimap(self._BASIC_PAIRS + single_char_pairs)

        self.extended_map = self.make_multimap(
            self._BASIC_PAIRS + self._EXTENDED_ADDITIONS + single_char_pairs
        )

        self.maximum_map = self.make_multimap(
            self._BASIC_PAIRS + self._EXTENDED_ADDITIONS + self._MAXIMUM_ADDITIONS + single_char_pairs
        )

    def set_settings(self, settings):
        """Update settings reference, rebuild maps, and clear cache."""
        self._settings = settings
        self._rebuild_maps()
        self._cache.clear()

    def _get_max_changes_for_length(self, term_len: int, base_max: int) -> int:
        """
        Dynamic max_changes based on term length to prevent combinatorial explosion.
        Respects settings if available (variant_min_word_len, variant_aggressive).
        """
        # Check for aggressive mode (old behavior - no limits based on length)
        if self._settings and getattr(self._settings, 'variant_aggressive', False):
            return min(base_max, getattr(self._settings, 'variant_max_changes', 2))

        # Get threshold from settings or use default
        min_len = 2
        if self._settings:
            min_len = getattr(self._settings, 'variant_min_word_len', 2)

        if term_len <= min_len:
            # Short words: only 1 change
            return 1
        else:
            # Longer words: allow full base_max (capped by settings or 2)
            max_cap = 2
            if self._settings:
                max_cap = getattr(self._settings, 'variant_max_changes', 2)
            return min(base_max, max_cap)

    def hamming_distance(self, term: str, variant: str) -> int:
        """Calculate character difference count between term and variant."""
        if len(term) != len(variant):
            return len(term) + len(variant)
        return sum(1 for a, b in zip(term, variant) if a != b)

    def generate_variants(self, term: str, mapping: Mapping[str, set[str]],
                          max_changes: int, limit: int) -> set[str]:
        """
        Generate variants with early termination and smart position filtering.
        Only considers positions that actually have replacements in the mapping.
        """
        term_len = len(term)
        limit = min(limit, Config.VARIANT_GEN_LIMIT)
        result = set()

        # Pre-filter: find positions that have possible replacements
        replaceable_positions = []
        for i, char in enumerate(term):
            if char in mapping and mapping[char] - {char}:
                replaceable_positions.append(i)

        if not replaceable_positions:
            return result

        # Generate variants by number of changes (1 change first, then 2, etc.)
        for num_changes in range(1, max_changes + 1):
            if num_changes > len(replaceable_positions):
                break

            for positions in itertools.combinations(replaceable_positions, num_changes):
                # Build character options for each position
                char_options = []
                valid = True

                for i in range(term_len):
                    if i in positions:
                        repls = mapping[term[i]] - {term[i]}
                        if not repls:
                            valid = False
                            break
                        char_options.append(repls)
                    else:
                        char_options.append((term[i],))

                if not valid:
                    continue

                # Generate all combinations for these positions
                for combo in itertools.product(*char_options):
                    result.add("".join(combo))
                    if len(result) >= limit:
                        return result

        return result

    def get_variants(self, term: str, mode: str, limit: int = None) -> list[str]:
        """
        Generate spelling variants for Hebrew search terms.

        Uses single-pass generation with the appropriate hierarchical map,
        instead of redundant multi-layer processing.

        Also applies multi-character substitutions from custom variant pairs,
        generating single-char variants for each multi-char substitution result.
        """
        if len(term) < 2:
            return [term]

        # Get tier configuration
        tier = self._TIER_CONFIG.get(mode)
        if not tier:
            return [term]

        # Apply limit from tier config if not specified
        if limit is None:
            limit = tier['per_term_limit']
        else:
            limit = min(limit, Config.VARIANT_GEN_LIMIT)

        # Check cache
        cache_key = (term, mode, limit)
        if cache_key in self._cache:
            return self._cache[cache_key]

        # Select the appropriate map (hierarchical - includes all lower tiers)
        if mode == 'variants':
            mapping = self.basic_map
        elif mode == 'variants_extended':
            mapping = self.extended_map
        elif mode == 'variants_maximum':
            mapping = self.maximum_map
        else:
            return [term]

        # Dynamic max_changes based on term length
        base_max = tier['max_changes']
        max_changes = self._get_max_changes_for_length(len(term), base_max)

        # Step 1: Generate multi-char substitution variants (e.g., כו=מ)
        multichar_variants = self._generate_multichar_variants(term)

        # Step 2: Generate single-char variants for original term
        variants = self.generate_variants(term, mapping, max_changes, limit)
        variants.add(term)  # Always include original

        # Step 3: Generate single-char variants for each multi-char variant
        for mc_variant in multichar_variants:
            variants.add(mc_variant)
            if len(variants) < limit and len(mc_variant) >= 2:
                mc_max_changes = self._get_max_changes_for_length(len(mc_variant), base_max)
                mc_single_variants = self.generate_variants(
                    mc_variant, mapping, mc_max_changes,
                    limit - len(variants)  # Remaining budget
                )
                variants.update(mc_single_variants)

        # Sort: original term first, then by similarity
        def sort_key(v):
            if v == term:
                return (0, 0, v)
            elif v in multichar_variants:
                return (1, 0, v)  # Multi-char variants second
            else:
                return (2, self.hamming_distance(term, v) if len(v) == len(term) else 100, v)

        sorted_variants = sorted(variants, key=sort_key)[:limit]

        # Cache result (with size limit)
        if len(self._cache) >= self._cache_max_size:
            # Simple eviction: clear half the cache
            keys_to_remove = list(self._cache.keys())[:self._cache_max_size // 2]
            for k in keys_to_remove:
                del self._cache[k]

        self._cache[cache_key] = sorted_variants
        return sorted_variants

    def clear_cache(self):
        """Clear the variant cache."""
        self._cache.clear()

# ==============================================================================
#  CODICOLOGICAL MANAGER (Oxford Parts / Neubauer)
# ==============================================================================
class CodicologicalManager:
    """
    Manages codicological units (Parts) for Oxford manuscripts.
    Maps between our folio-based system IDs and Oxford's Neubauer catalog Parts.

    A "Part" is a codicological unit in the Neubauer catalog that may contain
    multiple folios (and thus multiple system IDs in our system).
    """

    def __init__(self):
        # Core mappings
        self.folio_to_part = {}       # sys_id → part_id (e.g., "MS. Heb. d. 29/2")
        self.part_to_folios = {}      # part_id → [sys_ids] (ordered by folio number)
        self.part_metadata = {}       # part_id → {title, contents, provenance, images, ...}
        self.part_to_volume = {}      # part_id → volume_number (for grouping)

        # For autocomplete: [(display_text, normalized, part_id)]
        self.part_autocomplete = []

        # Raw JSON data (loaded once)
        self._oxford_db = {}
        self._loaded = False

        # Volume structure: volume_num → {part_id → folio_range}
        self._volume_parts = {}

    def load(self, csv_bank=None):
        """
        Load Oxford database and build all mappings.

        Args:
            csv_bank: Optional dict of sys_id → {shelfmark, title, oxford_part_id}
                      from MetadataManager for resolving missing parts.
        """
        if self._loaded:
            return True

        if not os.path.exists(Config.OXFORD_DB):
            LOGGER.warning("Oxford database not found at %s", Config.OXFORD_DB)
            return False

        try:
            LOGGER.info("Loading Oxford codicological database from %s", Config.OXFORD_DB)
            with open(Config.OXFORD_DB, 'r', encoding='utf-8') as f:
                self._oxford_db = json.load(f)

            self._build_part_mappings()
            self._build_folio_mappings(csv_bank)
            self._build_autocomplete_list()

            self._loaded = True
            LOGGER.info("Loaded %d codicological parts from Oxford database", len(self.part_metadata))
            return True

        except Exception as e:
            LOGGER.error("Failed to load Oxford database: %s", e)
            return False

    def _build_part_mappings(self):
        """Build part_metadata and volume structure from JSON."""
        for volume_num, parts in self._oxford_db.items():
            self._volume_parts[volume_num] = {}

            for part_id, part_data in parts.items():
                # Store metadata
                self.part_metadata[part_id] = {
                    'title': part_data.get('metadata', {}).get('title', ''),
                    'contents': part_data.get('metadata', {}).get('contents', ''),
                    'provenance': part_data.get('metadata', {}).get('provenance', ''),
                    'languages': part_data.get('metadata', {}).get('languages', ''),
                    'volume_url': part_data.get('volume_url', ''),
                    'direct_link': part_data.get('direct_link', ''),
                    'folio_range': part_data.get('folio_range', []),
                    'images': part_data.get('images', []),
                }

                self.part_to_volume[part_id] = volume_num

                # Store folio range for later resolution
                folio_range = part_data.get('folio_range', [])
                if folio_range and len(folio_range) == 2:
                    self._volume_parts[volume_num][part_id] = folio_range

                # Initialize empty folio list (will be populated from CSV)
                self.part_to_folios[part_id] = []

    def _build_folio_mappings(self, csv_bank=None):
        """
        Build folio→part mappings using CSV data.
        For entries with oxford_part_id, use it directly.
        For entries without, try to resolve using folio_range from JSON.
        """
        if csv_bank is None:
            return

        # First pass: direct mappings from CSV oxford_part_id
        for sys_id, data in csv_bank.items():
            oxford_part_id = data.get('oxford_part_id', '')
            if oxford_part_id and oxford_part_id in self.part_metadata:
                self.folio_to_part[sys_id] = oxford_part_id
                if sys_id not in self.part_to_folios[oxford_part_id]:
                    self.part_to_folios[oxford_part_id].append(sys_id)

        # Second pass: resolve missing parts using folio_range
        for sys_id, data in csv_bank.items():
            if sys_id in self.folio_to_part:
                continue  # Already mapped

            shelfmark = data.get('shelfmark', '')
            resolved_part = self._resolve_part_by_folio_range(shelfmark)
            if resolved_part:
                self.folio_to_part[sys_id] = resolved_part
                if sys_id not in self.part_to_folios[resolved_part]:
                    self.part_to_folios[resolved_part].append(sys_id)

        # Sort folio lists by natural order of shelfmark
        for part_id in self.part_to_folios:
            self.part_to_folios[part_id].sort(
                key=lambda sid: self._get_folio_number(csv_bank.get(sid, {}).get('shelfmark', ''))
            )

    def _resolve_part_by_folio_range(self, shelfmark):
        """
        Resolve a shelfmark to its Part using folio_range from JSON.

        Example: "MS heb. d.29/8" → folio 8 in volume d.29
        We look for a Part in that volume whose folio_range contains 8.
        """
        if not shelfmark:
            return None

        # Parse shelfmark to extract volume and folio
        # Pattern: MS heb. X.Y/Z or MS heb. X. Y/Z
        match = re.match(
            r'(?:MS\.?\s*)?heb\.?\s*([a-z])\.?\s*(\d+)[/.](\d+)',
            shelfmark,
            re.IGNORECASE
        )
        if not match:
            return None

        letter, volume_num, folio_num = match.groups()
        folio_num = int(folio_num)

        # Construct volume identifier pattern to find in our volume_parts
        # The JSON uses volume numbers like "56" which correspond to specific volumes
        # We need to find parts that match "MS. Heb. {letter}. {volume_num}"
        volume_pattern = f"heb. {letter}. {volume_num}"

        for part_id, metadata in self.part_metadata.items():
            # Check if part_id matches the volume pattern
            if volume_pattern.lower() not in part_id.lower():
                continue

            folio_range = metadata.get('folio_range', [])
            if len(folio_range) == 2:
                start, end = folio_range
                if start <= folio_num <= end:
                    return part_id

        return None

    def _get_folio_number(self, shelfmark):
        """Extract folio number from shelfmark for sorting."""
        if not shelfmark:
            return 0
        match = re.search(r'[/.](\d+)\s*$', shelfmark)
        return int(match.group(1)) if match else 0

    def _build_autocomplete_list(self):
        """Build autocomplete entries for Parts with 'part X' suffix."""
        self.part_autocomplete = []

        for part_id, metadata in self.part_metadata.items():
            # Part ID format: "MS. Heb. d. 29/2" -> "heb. d. 29 part 2"
            # Parse the Part ID
            match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)/(\d+)$', part_id, re.IGNORECASE)
            if match:
                letter, volume, part_num = match.groups()
                # Display format: "heb. d. 29 part 2"
                display = f"heb. {letter}. {volume} part {part_num}"
            else:
                # Fallback for non-standard Part IDs
                display = f"{part_id} part"

            # Normalized for matching (lowercase, no dots/spaces)
            normalized = re.sub(r'[\s.]', '', display.lower())

            title = metadata.get('title', '')

            self.part_autocomplete.append({
                'display': display,
                'normalized': normalized,
                'part_id': part_id,
                'title': title,
            })

        # Sort by natural order
        self.part_autocomplete.sort(key=lambda x: natural_sort_key(x['display']))

    # --- Public API ---

    def get_part_for_folio(self, sys_id):
        """Get the Part ID for a given system ID (folio)."""
        return self.folio_to_part.get(sys_id)

    def get_folios_for_part(self, part_id):
        """Get all system IDs (folios) belonging to a Part, in order."""
        return self.part_to_folios.get(part_id, [])

    def get_part_metadata(self, part_id):
        """Get full metadata for a Part."""
        return self.part_metadata.get(part_id, {})

    def get_part_images(self, part_id):
        """Get all images for a Part."""
        metadata = self.part_metadata.get(part_id, {})
        return metadata.get('images', [])

    def get_part_display_name(self, part_id):
        """Get display name for a Part (with 'part X' suffix)."""
        if part_id in self.part_metadata:
            # Convert "MS. Heb. d. 29/2" to "heb. d. 29 part 2"
            # Also handles letter parts like "MS. Heb. d. 25/C" -> "heb. d. 25 part C"
            match = re.match(r'^MS\.?\s*Heb\.?\s*([a-z])\.?\s*(\d+)/([A-Za-z0-9]+)$', part_id, re.IGNORECASE)
            if match:
                letter, volume, part_num = match.groups()
                return f"heb. {letter}. {volume} part {part_num}"
            return f"{part_id} part"
        return part_id

    def get_part_label(self, part_id):
        """Return a short Part label suitable for shelfmark suffixes (e.g., "part 23")."""
        if not part_id:
            return ""

        match = re.search(r'/([A-Za-z0-9]+)$', part_id)
        if match:
            return f"part {match.group(1)}"

        return "part"

    def is_part_id(self, identifier):
        """Check if an identifier is a Part ID (vs a regular shelfmark)."""
        # Check for "part" suffix (new format)
        if re.search(r'\bpart\s*\d*\s*$', identifier, re.IGNORECASE):
            return True
        # Legacy: check for "(neubauer)" suffix
        if "(neubauer)" in identifier.lower():
            return True
        # Also check if it's directly in our metadata
        clean = identifier.strip()
        return clean in self.part_metadata

    def parse_part_identifier(self, identifier):
        """
        Parse an identifier that might be a Part.
        Returns (part_id, is_part) tuple.

        Handles formats:
        - "heb. d. 29 part 2" -> "MS. Heb. d. 29/2"
        - "MS. Heb. d. 29/2 (neubauer)" -> "MS. Heb. d. 29/2"
        - "MS. Heb. d. 29/2" -> "MS. Heb. d. 29/2"
        """
        if not identifier:
            return None, False

        # Check for new format: "heb. d. 29 part 2"
        match = re.match(r'^(?:ms\.?\s*)?heb\.?\s*([a-z])\.?\s*(\d+)\s+part\s*(\d+)\s*$',
                         identifier, re.IGNORECASE)
        if match:
            letter, volume, part_num = match.groups()
            # Convert to canonical Part ID format
            part_id = f"MS. Heb. {letter}. {volume}/{part_num}"
            if part_id in self.part_metadata:
                return part_id, True
            # Try with uppercase letter
            part_id_upper = f"MS. Heb. {letter.upper()}. {volume}/{part_num}"
            if part_id_upper in self.part_metadata:
                return part_id_upper, True

        # Legacy: remove "(neubauer)" suffix if present
        clean = re.sub(r'\s*\(neubauer\)\s*$', '', identifier, flags=re.IGNORECASE).strip()
        # Also remove " part" suffix without number
        clean = re.sub(r'\s+part\s*$', '', clean, flags=re.IGNORECASE).strip()

        if clean in self.part_metadata:
            return clean, True

        # Try to normalize and find
        normalized = re.sub(r'\s+', ' ', clean).strip()
        if normalized in self.part_metadata:
            return normalized, True

        return identifier, False

    def get_image_for_folio(self, sys_id, side='a'):
        """
        Get the specific image URL for a folio within its Part.

        Args:
            sys_id: System ID of the folio
            side: 'a' or 'b' for recto/verso

        Returns:
            dict with 'label', 'full_url', 'thumb_url' or None
        """
        part_id = self.folio_to_part.get(sys_id)
        if not part_id:
            return None

        images = self.get_part_images(part_id)
        if not images:
            return None

        # Find the folio number in the part's folio list
        folios = self.part_to_folios.get(part_id, [])
        if sys_id not in folios:
            return None

        folio_index = folios.index(sys_id)

        # Each folio has 2 images (a and b)
        # folio_index 0 → images[0] (a), images[1] (b)
        # folio_index 1 → images[2] (a), images[3] (b)
        image_offset = 0 if side == 'a' else 1
        image_index = (folio_index * 2) + image_offset

        if image_index < len(images):
            return images[image_index]

        return None

    def get_all_images_for_part(self, part_id):
        """Get all images for a Part with their labels."""
        return self.get_part_images(part_id)

    def get_adjacent_part(self, part_id, direction=1):
        """
        Get the next or previous Part in the same volume.

        Args:
            part_id: Current Part ID
            direction: 1 for next, -1 for previous

        Returns:
            Adjacent Part ID or None
        """
        volume = self.part_to_volume.get(part_id)
        if not volume or volume not in self._oxford_db:
            return None

        # Get parts in this volume, sorted
        volume_parts = list(self._oxford_db[volume].keys())
        volume_parts.sort(key=natural_sort_key)

        try:
            idx = volume_parts.index(part_id)
            new_idx = idx + direction
            if 0 <= new_idx < len(volume_parts):
                return volume_parts[new_idx]
        except ValueError:
            pass

        return None


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

        # Codicological Parts manager (Oxford Neubauer)
        self.codico_mgr = CodicologicalManager()

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
        # Load codicological parts after CSV is ready
        self.codico_mgr.load(csv_bank=self.csv_bank)

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

                    if not row or len(row) < 3:
                        continue
                    # Format: system_number, oxford_part_id, call_numbers, ..., titles_non_placeholder
                    raw_sys_id = row[0]
                    sys_id = "".join(ch for ch in str(raw_sys_id) if ch.isdigit())

                    # Oxford Part ID (column 1) - for Neubauer catalog
                    oxford_part_id = row[1].strip() if len(row) > 1 else ""

                    # Call numbers (column 2) can be multiple separated by '|'
                    # We take the shortest one that looks like a shelfmark, or just the first
                    raw_shelves = row[2].split('|') if len(row) > 2 else []
                    shelf = raw_shelves[0].strip() if raw_shelves else ""
                    # Try to find a nice short shelfmark
                    for s in raw_shelves:
                        s = s.strip()
                        if s and len(s) < len(shelf):
                            shelf = s

                    # Title is column index 6 (0-based) - titles_non_placeholder
                    title = ""
                    if len(row) > 6:
                        title = row[6].strip()

                    self.csv_bank[sys_id] = {
                        'shelfmark': shelf,
                        'title': title,
                        'oxford_part_id': oxford_part_id,
                    }
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

    # --- Codicological Parts API (delegates to codico_mgr) ---

    def get_part_for_folio(self, sys_id):
        """Get the Part ID for a given system ID."""
        return self.codico_mgr.get_part_for_folio(sys_id)

    def get_folios_for_part(self, part_id):
        """Get all system IDs (folios) belonging to a Part."""
        return self.codico_mgr.get_folios_for_part(part_id)

    def get_part_metadata(self, part_id):
        """Get full metadata for a Part (Oxford Neubauer)."""
        return self.codico_mgr.get_part_metadata(part_id)

    def get_part_images(self, part_id):
        """Get all images for a Part."""
        return self.codico_mgr.get_part_images(part_id)

    def is_part_id(self, identifier):
        """Check if an identifier is a Part ID."""
        return self.codico_mgr.is_part_id(identifier)

    def parse_part_identifier(self, identifier):
        """Parse an identifier that might be a Part. Returns (part_id, is_part)."""
        return self.codico_mgr.parse_part_identifier(identifier)

    def get_part_autocomplete_list(self):
        """Get list of Parts for autocomplete."""
        return self.codico_mgr.part_autocomplete

    def get_meta_with_part(self, sys_id):
        """
        Get shelfmark, title, and Part info for a system ID.
        Returns dict with: shelfmark, title, oxford_part_id, part_metadata
        """
        shelf, title = self.get_meta_for_id(sys_id)

        result = {
            'shelfmark': shelf,
            'title': title,
            'oxford_part_id': None,
            'part_metadata': None,
        }

        # Get Part info if available
        part_id = self.get_part_for_folio(sys_id)
        if part_id:
            result['oxford_part_id'] = part_id
            result['part_metadata'] = self.get_part_metadata(part_id)

            # If our title is empty but Part has a title, use it
            if not title and result['part_metadata']:
                result['title'] = result['part_metadata'].get('title', '')

        return result

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
        """
        Robust extraction of Unique ID.
        Instead of expecting a fixed string 'IE_P_FL', we scan for components anywhere.
        This fixes issues with file paths containing backslashes (e.g. Russia Library).
        """
        # First attempt: classic continuous structure
        match = re.search(r'(IE\d+_P\d+_FL\d+)', text)
        if match:
            return match.group(1)

        # Second attempt: components assembly (robust to path breaks)
        ie = re.search(r'(IE\d+)', text)
        p = re.search(r'(P\d+)', text)
        fl = re.search(r'(FL\d+)', text)

        if ie and p and fl:
            return f"{ie.group(1)}_{p.group(1)}_{fl.group(1)}"

        # Default: System ID (only if all else fails)
        sys = re.search(r'(99\d+)', text)
        return sys.group(1) if sys else "UNKNOWN"

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
        """
        Parse header into components regardless of order or separators.
        Fixes display issues for V0.7 paths.
        """
        result = {'sys_id': None, 'ie_id': None, 'p_num': None, 'fl_id': None}

        # 1. System ID (99...)
        sys_match = re.search(r'(99\d{8,})', full_header)
        if sys_match:
            result['sys_id'] = sys_match.group(1)

        # 2. IE ID
        ie_match = re.search(r'(IE\d+)', full_header)
        if ie_match:
            result['ie_id'] = ie_match.group(1)

        # 3. Page Number (P...)
        p_match = re.search(r'_?(P\d+)', full_header)
        if p_match:
            # Remove P to get clean number
            raw_p = p_match.group(1) # P0001
            result['p_num'] = str(int(raw_p[1:]))

        # 4. FL ID
        fl_match = re.search(r'(FL\d+)', full_header)
        if fl_match:
            result['fl_id'] = fl_match.group(1).replace("FL", "")

        return result

    def fetch_nli_data(self, system_id):
        # 1. Check existing cache
        if system_id in self.nli_cache: 
            return self.nli_cache[system_id]
        
        # 2. Check CSV Bank (local fetch)
        if system_id in self.csv_bank:
            row = self.csv_bank[system_id]
            meta = {
                'shelfmark': row['shelfmark'], 
                'title': row['title'],
                'desc': '', 
                'fl_ids': [], 
                'thumb_url': None, 
                'thumb_checked': True # Mark as checked to prevent repeated image download attempts
            }
            self.nli_cache[system_id] = meta
            return meta

        # 3. Only if necessary (not in cache/CSV) - Network request
        _, meta = self._fetch_single_worker(system_id)
        self.nli_cache[system_id] = meta
        return meta

    def fetch_iiif_manifest(self, system_id):
        """Fetch and parse IIIF manifest for physical description, attribution, and image labels."""
        url = f"{Config.NLI_IIIF_BASE}/DOCID/PNX_MANUSCRIPTS{system_id}-1/manifest"
        headers = Config.HTTP_HEADERS

        result = {'physical_desc': '', 'canvas_map': {}, 'attribution': ''}
        try:
            session = self._make_session()
            resp = session.get(url, headers=headers, timeout=10, verify=False)
            if resp.status_code == 200:
                data = resp.json()

                # 1. Physical Description
                result['physical_desc'] = data.get('attribution', '')
                attr_val = data.get('attribution')
                if isinstance(attr_val, str):
                    result['attribution'] = attr_val
                elif isinstance(attr_val, list) and attr_val:
                    result['attribution'] = str(attr_val[0])
                elif data.get('label'):
                    result['attribution'] = str(data.get('label'))

                # 2. Canvas Map (FL -> Label)
                if 'sequences' in data and data['sequences']:
                    for canvas in data['sequences'][0].get('canvases', []):
                        label = canvas.get('label', '')
                        # Extract FL ID from image service ID
                        images = canvas.get('images', [])
                        if images:
                            resource = images[0].get('resource', {})
                            service = resource.get('service', {})
                            service_id = service.get('@id', '')
                            # Extract FL number (e.g. .../FL7734473/...)
                            fl_match = re.search(r'FL(\d+)', service_id)
                            if fl_match:
                                fl_digits = fl_match.group(1)
                                result['canvas_map'][fl_digits] = label

            return result
        except Exception as e:
            LOGGER.warning(f"IIIF fetch failed for {system_id}: {e}")
            return result

    def fetch_marc_data(self, system_id):
        """Fetch and parse MARC XML for bibliography, notes, and extended metadata."""
        # Use the specific IIIF/MARC endpoint which is more reliable
        url = f"{Config.NLI_IIIF_BASE}/marc/bib/{system_id}"
        headers = Config.HTTP_HEADERS

        result = {
            'bibliography': [],
            'notes': [],
            'english_title': '',
            'dimensions': '',
            'people': [],
            'current_owner': '',
            'shelfmark_alt': '',
            'date': '',
            'subjects': [],
            'physical_medium': '',
            'attribution': '',
            'online_link': None,
            'external_iiif_link': None
        }

        try:
            session = self._make_session()
            resp = session.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                # Remove namespaces to simplify parsing
                xml_content = re.sub(r'\sxmlns="[^"]+"', '', resp.text, count=1)
                xml_content = re.sub(r'\sxmlns:marc="[^"]+"', '', xml_content, count=1)
                xml_content = xml_content.replace('marc:', '') # Bruteforce namespace removal

                root = ET.fromstring(xml_content)

                for df in root.findall(".//datafield"):
                    tag = df.get('tag')

                    def get_sub(code):
                        sf = df.find(f"subfield[@code='{code}']")
                        return sf.text.strip() if sf is not None and sf.text else ""

                    if tag == '581': # Bibliography
                        val = get_sub('a')
                        if val: result['bibliography'].append(val)

                    elif tag == '500': # Notes
                        val = get_sub('a')
                        if val: result['notes'].append(val)

                    elif tag == '246': # English Title
                        val_a = get_sub('a')
                        val_i = get_sub('i')
                        if "English" in val_i:
                            result['english_title'] = val_a

                    elif tag == '260' or tag == '264': # Date
                        val = get_sub('c')
                        if val: result['date'] = val

                    elif tag == '300': # Dimensions
                        val_a = get_sub('a') # Extent (pages)
                        val_c = get_sub('c') # Dimensions
                        parts = [p for p in [val_a, val_c] if p]
                        result['dimensions'] = " | ".join(parts)

                    elif tag == '340': # Physical Medium / Condition
                        val = get_sub('a')
                        if val: result['physical_medium'] = val

                    elif tag == '650': # Subjects
                        val = get_sub('a')
                        if val: result['subjects'].append(val)

                    elif tag == '700': # People / Owners
                        name = get_sub('a')
                        role = get_sub('e')
                        if name:
                            full = f"{name} ({role})" if role else name
                            result['people'].append(full)

                    elif tag == '710': # Current Owner (Library Name)
                        val = get_sub('a')
                        if val: result['current_owner'] = val

                    elif tag == '856': # Online Link
                        url = get_sub('u')
                        label = get_sub('z') or "Online Version"
                        if url:
                            result['online_link'] = {'url': url, 'label': label}
                            # Detect CUDL for External Viewer
                            if "cudl.lib.cam.ac.uk" in url:
                                result['external_iiif_link'] = url

                    elif tag == '942': # Alt Shelfmark
                        val = get_sub('z')
                        if val: result['shelfmark_alt'] = val

                    elif tag == '597': # Image credit / attribution
                        val = get_sub('a')
                        if val: result['attribution'] = val

            return result
        except Exception as e:
            LOGGER.warning(f"MARC fetch failed for {system_id}: {e}")
            return result

    def enrich_metadata(self, system_id):
        """Fetch extended metadata (IIIF/MARC), build Image List, and merge into cache."""
        if not system_id: return {}

        # Ensure basic meta exists
        if system_id not in self.nli_cache:
            self.fetch_nli_data(system_id)

        current_meta = self.nli_cache.get(system_id, {})

        # 1. Fetch MARC (Bibliographic Data)
        marc_data = self.fetch_marc_data(system_id)
        current_meta['marc'] = marc_data
        marc_attribution = marc_data.get('attribution')

        # 2. Determine Image Source (External CUDL vs Fallback NLI)
        image_list = []
        external_meta = {}

        # Check for External Link from MARC (e.g. CUDL)
        ext_link = marc_data.get('external_iiif_link')
        if ext_link:
            current_meta['external_url'] = ext_link

        # Lists for multiple sources
        images_nli = []
        images_ext = []

        # 2a. Fetch External IIIF (Cambridge)
        if ext_link:
            ext_data = self.fetch_external_iiif_data(ext_link)
            if ext_data.get('canvases'):
                images_ext = ext_data['canvases'] # Format: [{'label': '...', 'url': '...'}]
                external_meta = ext_data.get('metadata', {})
                if not marc_attribution:
                    current_meta['attribution'] = ext_data.get('attribution')

        # 2a2. Check for Oxford Part images (if no Cambridge images)
        if not images_ext:
            part_id = self.get_part_for_folio(system_id)
            if part_id:
                current_meta['oxford_part_id'] = part_id
                part_meta = self.get_part_metadata(part_id)
                if part_meta:
                    current_meta['oxford_part_metadata'] = part_meta
                    if not current_meta.get('title') and part_meta.get('title'):
                        current_meta['title'] = part_meta['title']
                    if part_meta.get('direct_link'):
                        current_meta['external_url'] = part_meta['direct_link']

                part_images = self.get_part_images(part_id)
                if part_images:
                    # Convert Oxford Part images to the expected format (include thumb_url)
                    images_ext = [{
                        'label': img.get('label', ''),
                        'url': img.get('full_url', ''),
                        'thumb_url': img.get('thumb_url', ''),
                        'folio_num': img.get('folio_num')
                    } for img in part_images]
                    current_meta['attribution'] = "From the collections of the Bodleian Libraries, Oxford"
                    current_meta['thumb_url'] = part_images[0].get('thumb_url') or current_meta.get('thumb_url')

        # 2b. Always Fetch NLI IIIF (for fallback or toggle)
        nli_iiif_data = self.fetch_iiif_manifest(system_id)
        if nli_iiif_data.get('canvas_map'):
            sorted_map = sorted(nli_iiif_data['canvas_map'].items(), key=lambda x: x[0])
            for fl_id, label in sorted_map:
                url = f"{Config.NLI_IIIF_BASE}/FL{fl_id}"
                images_nli.append({'label': label, 'url': url, 'fl_id': fl_id})

        if not current_meta.get('physical_desc'):
            current_meta['physical_desc'] = nli_iiif_data.get('physical_desc', '')

        if marc_attribution:
            current_meta['attribution'] = marc_attribution
        elif not current_meta.get('attribution'):
            current_meta['attribution'] = nli_iiif_data.get('attribution', '')

        if nli_iiif_data.get('canvas_map'):
            current_meta['canvas_map'] = nli_iiif_data['canvas_map']

        # Prioritize External if available, but keep both sets
        current_meta['images'] = images_ext if images_ext else images_nli
        current_meta['images_nli'] = images_nli
        current_meta['images_ext'] = images_ext
        current_meta['external_meta'] = external_meta

        # Update cache precedence (Enrichment overrides basic placeholders)
        if marc_data.get('english_title') and not current_meta.get('title'):
            current_meta['title'] = marc_data['english_title']

        if marc_data.get('shelfmark_alt') and (not current_meta.get('shelfmark') or current_meta.get('shelfmark') == 'Unknown'):
            current_meta['shelfmark'] = marc_data['shelfmark_alt']

        self.nli_cache[system_id] = current_meta
        return current_meta

    def fetch_external_iiif_data(self, view_url):
        """
        Generic handler to fetch external IIIF data.
        Currently supports CUDL logic: /view/ -> /iiif/ manifest.
        Returns: {'attribution': str, 'metadata': dict, 'canvases': [{'label': str, 'url': str}]}
        """
        if not view_url: return {}

        # CUDL Conversion Logic
        manifest_url = view_url
        if "cudl.lib.cam.ac.uk/view/" in view_url:
            base = view_url.replace("/view/", "/iiif/")
            manifest_url = re.sub(r'/\d+$', '', base)

        if manifest_url.startswith("http://"):
            manifest_url = manifest_url.replace("http://", "https://")

        result = {
            'attribution': 'External Library',
            'metadata': {},
            'canvases': []
        }

        try:
            session = self._make_session()
            resp = session.get(manifest_url, timeout=10)
            if resp.status_code == 200:
                data = resp.json()

                # Attribution
                attr = data.get('attribution')
                if isinstance(attr, str):
                    result['attribution'] = attr
                elif isinstance(attr, list) and attr:
                    result['attribution'] = str(attr[0])
                elif data.get('label'):
                    result['attribution'] = str(data.get('label'))

                # Metadata
                if 'metadata' in data:
                    for item in data['metadata']:
                        label = str(item.get('label', '')).lower()
                        val = str(item.get('value', ''))
                        if label in ['abstract', 'condition', 'provenance', 'physical description']:
                            result['metadata'][label.title()] = val

                # Canvases (Images with Labels)
                if 'sequences' in data and data['sequences']:
                    for idx, canvas in enumerate(data['sequences'][0].get('canvases', [])):
                        lbl = canvas.get('label', f"Img {idx + 1}")
                        images = canvas.get('images', [])
                        if images:
                            resource = images[0].get('resource', {})
                            service = resource.get('service', {})
                            # Try to get the service ID (base URL) for flexible resizing
                            img_id = service.get('@id') if service else resource.get('@id')

                            if img_id:
                                # Extract folio_num from label for proper page indexing
                                # Labels like "1", "1r", "1v", "2" etc. - extract leading number
                                # Labels like "Binding", "Cover" etc. - no folio_num
                                folio_num = None
                                lbl_match = re.match(r'^(\d+)', str(lbl).strip())
                                if lbl_match:
                                    try:
                                        folio_num = int(lbl_match.group(1))
                                    except (TypeError, ValueError):
                                        pass
                                result['canvases'].append({'label': lbl, 'url': img_id, 'folio_num': folio_num})

            return result
        except Exception as e:
            LOGGER.warning(f"External IIIF fetch failed for {view_url}: {e}")
            return result

    def _fetch_single_worker(self, system_id):
        url = f"{Config.NLI_IIIF_BASE}/marc/bib/{system_id}"
        # Initialize default meta structure
        meta = {'shelfmark': 'Unknown', 'title': '', 'desc': '', 'fl_ids': [], 'thumb_url': None, 'thumb_checked': False}
        
        headers = Config.HTTP_HEADERS
        
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
            return f"{Config.NLI_IIIF_BASE}/FL{digits}/full/400,/0/default.jpg"
                
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
        url = f"{Config.NLI_IIIF_BASE}/marc/bib/{system_id}"
        headers = Config.HTTP_HEADERS
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
        
    def batch_fetch_shelfmarks(self, system_ids, progress_callback=None, use_network=True, check_cancel=None):
        """
        Populate metadata cache. 
        use_network=False -> Only loads from local CSV/Cache (Instant).
        use_network=True  -> Fetches missing items from NLI.
        """
        # Step A: Fast fetch from CSV (no network)
        for sid in system_ids:
            if check_cancel and check_cancel(): return
            if sid not in self.nli_cache and sid in self.csv_bank:
                self.fetch_nli_data(sid) # This fetches from CSV automatically now
        
        # If only local work requested, stop here
        if not use_network:
            return

        # Step B: Identify what is *really* missing
        to_fetch = [sid for sid in system_ids if sid not in self.nli_cache]
        
        if not to_fetch:
            if progress_callback:
                for i, sid in enumerate(system_ids):
                     progress_callback(i + 1, len(system_ids), sid)
            return

        # Step C: Network download (only if use_network=True)
        futures = {self.nli_executor.submit(self._fetch_single_worker, sid): sid for sid in to_fetch}
        current_progress = len(system_ids) - len(to_fetch)
        
        for future in as_completed(futures):
            if check_cancel and check_cancel(): break
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

    # ---------------- Shelfmark Resolution Helpers ----------------
    def _normalize_shelfmark(self, shelfmark: str) -> str:
        """Normalize shelfmarks: remove ALL non-alphanumeric chars (spaces, dots, etc)."""
        if not shelfmark:
            return ""
        
        cleaned = re.sub(r'\W+', '', shelfmark).casefold()
        
        if cleaned.startswith("ms"):
            cleaned = cleaned[2:]
            
        return cleaned

    def _iter_shelfmark_sources(self):
        """Yield shelfmark candidates from CSV bank and cached metadata."""
        # CSV bank
        for sys_id, data in self.csv_bank.items():
            shelf = data.get('shelfmark', '')
            title = data.get('title', '')
            if shelf:
                yield sys_id, shelf, title
        # NLI cache (may contain enriched shelfmarks)
        for sys_id, data in self.nli_cache.items():
            shelf = data.get('shelfmark', '')
            alt = data.get('shelfmark_alt', '')
            title = data.get('title', '')
            for candidate in [shelf, alt]:
                if candidate:
                    yield sys_id, candidate, title

    def resolve_system_by_shelfmark(self, query, limit=100):
        """
        Resolve a system ID by shelfmark, ignoring dots/slashes/spaces.
        Returns a dict: {'sys_id': ..., 'options': [...], 'selected_shelfmark': ...}
        """
        result = {'sys_id': None, 'options': [], 'selected_shelfmark': None}

        norm_query = self._normalize_shelfmark(query)
        if not norm_query:
            return result

        exact_matches = []
        partial_matches = []
        seen = set()

        def shelf_sort_key(entry):
            shelf = entry.get('shelfmark', '')
            title = entry.get('title', '')
            sid_val = entry.get('sys_id', '')
            return (natural_sort_key(shelf), natural_sort_key(title), natural_sort_key(sid_val))

        for sys_id, shelf, title in self._iter_shelfmark_sources():
            norm_shelf = self._normalize_shelfmark(shelf)
            if not norm_shelf or (sys_id, norm_shelf) in seen:
                continue
            seen.add((sys_id, norm_shelf))

            entry = {'sys_id': sys_id, 'shelfmark': shelf, 'title': title}
            if norm_shelf == norm_query:
                exact_matches.append(entry)
            elif norm_query in norm_shelf:
                partial_matches.append(entry)

        if len(exact_matches) == 1:
            result['sys_id'] = exact_matches[0]['sys_id']
            result['selected_shelfmark'] = exact_matches[0]['shelfmark']
            return result

        exact_matches.sort(key=shelf_sort_key)
        partial_matches.sort(key=shelf_sort_key)

        # Aggregate suggestions (exact first, then partial), capped at limit
        suggestions = exact_matches + partial_matches
        result['options'] = suggestions[:limit]
        return result

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
        builder.add_text_field("scope", stored=True)
        builder.add_text_field("boundaries", stored=True)
        schema = builder.build()
        
        index = tantivy.Index(schema, path=db_path)
        writer = index.writer(heap_size=150_000_000)
        
        total_docs = 0
        browse_map = defaultdict(list)
        system_pages = defaultdict(list)
        # Preserve file-order sequencing for continuous documents
        global_seq_index = 0 
        word_pattern = re.compile(Config.WORD_TOKEN_PATTERN)

        # Ensure metadata (including Oxford parts) is loaded for continuous scopes
        try:
            self.meta_mgr._load_csv_bank()
        except Exception as e:
            LOGGER.warning("Failed to load CSV bank before indexing: %s", e)
        try:
            self.meta_mgr.codico_mgr.load(csv_bank=self.meta_mgr.csv_bank)
        except Exception as e:
            LOGGER.warning("Failed to load codicological manager before indexing: %s", e)
        
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
                                full_header=str(chead), shelfmark=str(shelfmark),
                                scope="page", boundaries=""
                            ))
                            parsed = self.meta_mgr.parse_full_id_components(chead)
                            if parsed['sys_id']:
                                if parsed['p_num']:
                                    browse_map[parsed['sys_id']].append({'p_num': int(parsed['p_num']), 'uid': cid, 'full_header': chead})
                                system_pages[parsed['sys_id']].append({
                                    'p_num': int(parsed['p_num']) if parsed['p_num'] else 0,
                                    'uid': cid,
                                    'full_header': chead,
                                    'source': label,
                                    'content': "\n".join(ctext),
                                    'sys_id': parsed['sys_id'],
                                    'seq_index': global_seq_index
                                })
                                global_seq_index += 1
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
                        full_header=str(chead), shelfmark=str(shelfmark),
                        scope="page", boundaries=""
                    ))
                    parsed = self.meta_mgr.parse_full_id_components(chead)
                    if parsed['sys_id']:
                        if parsed['p_num']:
                            browse_map[parsed['sys_id']].append({'p_num': int(parsed['p_num']), 'uid': cid, 'full_header': chead})
                        system_pages[parsed['sys_id']].append({
                            'p_num': int(parsed['p_num']) if parsed['p_num'] else 0,
                            'uid': cid,
                            'full_header': chead,
                            'source': label,
                            'content': " ".join(ctext),
                            'sys_id': parsed['sys_id'],
                            'seq_index': global_seq_index
                        })
                        global_seq_index += 1
                    total_docs += 1

        # Build continuous documents per System ID
        for sid, pages in system_pages.items():
            if not pages:
                continue
            pages.sort(key=lambda p: p['seq_index'])
            self._add_continuous_document(writer, pages, scope="system", unique_id=f"sys:{sid}")

        # Build continuous documents per Codicological Part (Oxford)
        if self.meta_mgr.codico_mgr.part_to_folios:
            total_parts = len(self.meta_mgr.codico_mgr.part_to_folios)
            LOGGER.info("Indexing %d Codicological Parts...", total_parts)

            for idx, (part_id, folios) in enumerate(self.meta_mgr.codico_mgr.part_to_folios.items()):
                if idx % 500 == 0:
                    LOGGER.info("Processing Part %d/%d...", idx, total_parts)

                part_pages = []
                for folio_sid in folios:
                    sys_p = system_pages.get(folio_sid, [])
                    sys_p.sort(key=lambda p: p['seq_index'])
                    part_pages.extend(sys_p)

                if not part_pages:
                    continue

                if len(part_pages) > 1000:
                    LOGGER.warning("Skipping massive part '%s' with %d pages (likely data error).", part_id, len(part_pages))
                    continue

                total_words = sum(len((p.get('content', '') or "").split()) for p in part_pages)
                
                WORD_LIMIT = 150_000

                if total_words > WORD_LIMIT:
                    num_chunks = self._add_chunked_continuous_documents(
                        writer, part_pages, scope="part", unique_id=f"part:{part_id}",
                        word_limit=WORD_LIMIT, word_pattern=word_pattern
                    )
                    LOGGER.warning(
                        "Part '%s' split into %d chunk(s) due to %d words (limit=%d).",
                        part_id, num_chunks, total_words, WORD_LIMIT
                    )
                else:
                    self._add_continuous_document(writer, part_pages, scope="part", unique_id=f"part:{part_id}")

        LOGGER.info("Committing index (this may take a moment)...")
        writer.commit()
        for sid in browse_map: browse_map[sid].sort(key=lambda x: x['p_num'])

        total_before = sum(len(v) for v in browse_map.values())
        browse_map, deduped = dedupe_browse_map(browse_map)
        total_after = sum(len(v) for v in browse_map.values())

        if deduped:
            LOGGER.info("Removed %d duplicate browse-map entries during indexing", total_before - total_after)

        with open(Config.BROWSE_MAP, 'wb') as f: pickle.dump(browse_map, f)
        return total_docs

    def _add_continuous_document(self, writer, pages, scope, unique_id):
        """Add an aggregated document (system/part) with boundary metadata."""
        if not pages:
            return
        assembled = []
        boundaries = []
        cursor = 0
        for idx, page in enumerate(pages):
            text = page.get('content', '') or ''
            start = cursor
            assembled.append(text)
            cursor += len(text)
            boundaries.append({
                'uid': page.get('uid'),
                'p_num': page.get('p_num'),
                'full_header': page.get('full_header', ''),
                'source': page.get('source', ''),
                'sys_id': page.get('sys_id', '')
            })
            if idx != len(pages) - 1:
                assembled.append("\n")
                cursor += 1
            boundaries[-1]['start'] = start
            boundaries[-1]['end'] = cursor

        content = "".join(assembled)
        first_header = pages[0].get('full_header', '')
        first_source = pages[0].get('source', '')
        shelfmark = self.meta_mgr.get_shelfmark_from_header(first_header) or ""

        writer.add_document(tantivy.Document(
            unique_id=str(unique_id),
            content=str(content),
            source=str(first_source),
            full_header=str(first_header),
            shelfmark=str(shelfmark),
            scope=str(scope),
            boundaries=json.dumps(boundaries, ensure_ascii=False)
        ))

    def _add_chunked_continuous_documents(self, writer, pages, scope, unique_id, word_limit, word_pattern):
        """
        Split a large aggregated document into multiple chunks to avoid massive allocations.
        Returns the number of chunks created.
        """
        chunks = []
        current = []
        current_words = 0

        for page in pages:
            page_words = len(word_pattern.findall(page.get('content', '') or ""))
            if current and current_words + page_words > word_limit:
                chunks.append(current)
                current = []
                current_words = 0
            current.append(page)
            current_words += page_words

        if current:
            chunks.append(current)

        for idx, chunk_pages in enumerate(chunks, start=1):
            uid = unique_id if idx == 1 else f"{unique_id}#chunk{idx}"
            self._add_continuous_document(writer, chunk_pages, scope=scope, unique_id=uid)

        return len(chunks)

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

    def _load_browse_map(self):
        """Load the browse map, deduplicate it, and persist corrections if needed."""
        if not os.path.exists(Config.BROWSE_MAP):
            return {}

        with open(Config.BROWSE_MAP, 'rb') as f:
            raw_map = pickle.load(f)

        cleaned_map, changed = dedupe_browse_map(raw_map)
        if changed:
            try:
                with open(Config.BROWSE_MAP, 'wb') as f:
                    pickle.dump(cleaned_map, f)
            except Exception as e:
                LOGGER.warning("Failed to write deduplicated browse map to %s: %s", Config.BROWSE_MAP, e)

        return cleaned_map

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

                    if len(term) > 1 and len(v) < 2:
                        continue

                    # Clean quotes
                    v_clean = v.replace('"', '')
                    if v_clean:
                        # Multi-char variants (different length) get medium boost
                        # This ensures they rank higher and don't get cut off at search limit
                        if len(v_clean) != len(term):
                            clean_vars.append(f'"{v_clean}"^3')
                        else:
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
        
        # Sanitize snippet to prevent interference with markers (replace with space to keep indices)
        snippet_safe = snippet.replace('*', ' ')

        # Insert Asterisks for Unified Highlighting
        hl_snippet = snippet_safe[:rel_s] + f"*{snippet_safe[rel_s:rel_e]}*" + snippet_safe[rel_e:]

        if not for_file:
            # For UI Table: Flatten newlines
            return hl_snippet.replace('\n', ' ')
        
        # For File/Export: Keep newlines
        return hl_snippet

    def _highlight_by_span(self, text, span, for_file=False):
        """Return a highlighted snippet around a specific span."""
        if not span:
            return None
        s, e = span
        start = max(0, s - 60)
        end = min(len(text), e + 60)

        rel_s = s - start
        rel_e = e - start

        snippet = text[start:end]
        snippet_safe = snippet.replace('*', ' ')
        hl_snippet = snippet_safe[:rel_s] + f"*{snippet_safe[rel_s:rel_e]}*" + snippet_safe[rel_e:]

        if not for_file:
            return hl_snippet.replace('\n', ' ')
        return hl_snippet

    def _parse_boundaries(self, doc):
        raw = self._get_field(doc, 'boundaries', [""])
        if not raw or not raw[0]:
            return []
        try:
            return json.loads(raw[0])
        except Exception as e:
            uid_val = None
            try:
                uid_val = doc['unique_id'][0]
            except Exception:
                uid_val = '?'
            LOGGER.warning("Failed to parse boundaries for doc %s: %s", uid_val, e)
            return []

    def _map_span_to_pages(self, span, boundaries):
        """Return page overlaps and primary page for a match span."""
        overlaps = []
        primary = None
        if not span:
            return {'primary': primary, 'overlaps': overlaps, 'cross_page': False}

        s, e = span
        for b in boundaries:
            b_start = b.get('start', 0)
            b_end = b.get('end', 0)
            if e <= b_start or s >= b_end:
                continue
            overlap_start = max(s, b_start)
            overlap_end = min(e, b_end)
            if overlap_start >= overlap_end:
                continue
            rel_start = overlap_start - b_start
            rel_end = overlap_end - b_start
            overlaps.append({
                'uid': b.get('uid'),
                'p_num': b.get('p_num'),
                'full_header': b.get('full_header', ''),
                'source': b.get('source', ''),
                'sys_id': b.get('sys_id', ''),
                'span': (rel_start, rel_end)
            })
            if not primary:
                primary = b
        cross_page = len({o.get('uid') for o in overlaps if o.get('uid')}) > 1
        return {'primary': primary or (boundaries[0] if boundaries else None), 'overlaps': overlaps, 'cross_page': cross_page}

    def _get_field(self, doc, field, default=None):
        try:
            return doc[field]
        except Exception:
            return default

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

        # DEBUG: Log query and regex
        LOGGER.info(f"[DEBUG] Mode: {mode}, Terms: {terms}")
        LOGGER.info(f"[DEBUG] Tantivy query: {t_query_str[:500]}")
        LOGGER.info(f"[DEBUG] Regex pattern: {regex.pattern[:500]}")

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
        LOGGER.info(f"[DEBUG] Tantivy returned {total_hits} hits")
        results = []
        regex_filtered_count = 0

        for i, (score, doc_addr) in enumerate(hits):
            if progress_callback and i % 50 == 0:
                progress_callback(i, total_hits)
            try:
                doc = self.searcher.doc(doc_addr)
                content = self._get_field(doc, 'content', [""])[0]
                scope_list = self._get_field(doc, 'scope', ['page']) or ['page']
                scope = scope_list[0]

                # Check for match before any heavy parsing
                match_obj = regex.search(content)
                if not match_obj:
                    regex_filtered_count += 1
                    continue

                boundaries = self._parse_boundaries(doc) if scope != 'page' else []
                span = match_obj.span()
                if boundaries:
                    span_map = self._map_span_to_pages(span, boundaries)
                    primary = span_map.get('primary') or {}
                    display_header = primary.get('full_header', doc['full_header'][0])
                    source_label = primary.get('source', doc['source'][0])
                    hl_c = self._highlight_by_span(content, span, False)
                    hl_f = self._highlight_by_span(content, span, True)
                    meta = self.meta_mgr.get_display_data(display_header, source_label)
                    page_highlights = []
                    for ov in span_map.get('overlaps', []):
                        if 'span' in ov and ov.get('uid'):
                            page_highlights.append({
                                'uid': ov.get('uid'),
                                'p_num': ov.get('p_num'),
                                'span': ov.get('span'),
                                'full_header': ov.get('full_header', ''),
                                'source': ov.get('source', '')
                            })
                    results.append({
                        'display': meta,
                        'snippet': hl_c or "",
                        'full_text': content,
                        'uid': primary.get('uid') or doc['unique_id'][0],
                        'raw_header': display_header,
                        'raw_file_hl': hl_f or "",
                        'highlight_pattern': pattern_str,
                        'page_highlights': page_highlights,
                        'cross_page': span_map.get('cross_page', False),
                        'scope': scope
                    })
                else:
                    hl_c = self.highlight(content, regex, False)
                    hl_f = self.highlight(content, regex, True)
                    if hl_c:
                        meta = self.meta_mgr.get_display_data(doc['full_header'][0], doc['source'][0])
                        results.append({
                            'display': meta, 'snippet': hl_c, 'full_text': content,
                            'uid': doc['unique_id'][0], 'raw_header': doc['full_header'][0],
                            'raw_file_hl': hl_f, 'highlight_pattern': pattern_str,
                            'scope': scope
                        })
            except Exception as e:
                LOGGER.warning("Failed to materialize search hit at position %s: %s", i, e)
        LOGGER.info(f"[DEBUG] Regex filtered out: {regex_filtered_count}, Results before dedup: {len(results)}")
        deduped = self._deduplicate(results)
        LOGGER.info(f"[DEBUG] Results after dedup: {len(deduped)}")
        return deduped

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
        """
        # 1. Tokenize original text
        tokens = re.findall(Config.WORD_TOKEN_PATTERN, full_text)
        if len(tokens) < chunk_size: return None
        chunks = [tokens[i:i + chunk_size] for i in range(len(tokens) - chunk_size + 1)]

        doc_hits_main = defaultdict(lambda: {'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(), 'patterns': set()})
        doc_hits_filtered = defaultdict(lambda: {'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(), 'patterns': set()})

        total_chunks = len(chunks)
        
        # 2. Scan chunks
        for i, chunk in enumerate(chunks):
            if progress_callback and i % 10 == 0: progress_callback(i, total_chunks)
            
            # Build query
            t_query = self.build_tantivy_query(chunk, mode)
            regex = self.build_regex_pattern(chunk, mode, 0)
            if not regex: continue

            # Check: Is phrase in "Filter Text"?
            is_text_filtered = False
            if filter_text:
                if regex.search(filter_text):
                    is_text_filtered = True

            try:
                # Search index
                query = self.index.parse_query(t_query, ["content"])
                hits = self.searcher.search(query, 50).hits
                
                is_freq_filtered = len(hits) > max_freq 
                
                for score, doc_addr in hits:
                    doc = self.searcher.doc(doc_addr)
                    content = doc['content'][0]
                    
                    # Verify exact Regex match
                    if regex.search(content):
                        uid = doc['unique_id'][0]
                        
                        # Route to appropriate map
                        if is_text_filtered or is_freq_filtered:
                            rec = doc_hits_filtered[uid]
                        else:
                            rec = doc_hits_main[uid]

                        rec['head'] = doc['full_header'][0]
                        rec['src'] = doc['source'][0]
                        rec['content'] = content
                        rec['matches'].append(regex.search(content).span())
                        # Save indices of found words in *source* text
                        rec['src_indices'].update(range(i, i + chunk_size))
                        rec['patterns'].add(regex.pattern)
            except Exception as e:
                LAB_LOGGER.warning(f"Failed composition chunk processing at token {i}: {e}")

        # 3. Build results with Wide Context
        def build_items(hits_dict):
            final_items = []
            
            for uid, data in hits_dict.items():
                src_indices = sorted(list(data['src_indices']))
                src_snippets = []
                
                if src_indices:
                    # A. Group nearby indices
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
                    
                    # B. Build text for each cluster
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
                        
                        src_snippets.append(" ".join(words_out))

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
                               f"*{data['content'][s:e]}*" + \
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
        """Aggregate individual page results into manuscript-level items.

        Groups by Codicological Part (Neubauer) when available, otherwise by System ID.
        """
        grouped = defaultdict(list)
        part_info = {}  # Track Part metadata for grouped items

        # 1. Bucket pages by Part ID (if available) or System ID
        for p in pages_list:
            sid, _ = self.meta_mgr.parse_header_smart(p['raw_header'])
            if sid:
                # Check if this folio belongs to a Part
                part_id = self.meta_mgr.get_part_for_folio(sid)
                if part_id:
                    # Group by Part ID
                    grouped[f"PART:{part_id}"].append(p)
                    if part_id not in part_info:
                        part_info[part_id] = {
                            'part_id': part_id,
                            'folios': set()
                        }
                    part_info[part_id]['folios'].add(sid)
                else:
                    grouped[sid].append(p)
            else:
                # Fallback for pages without valid ID (should be rare)
                grouped["UNKNOWN"].append(p)

        manuscripts = []

        for group_key, pages in grouped.items():
            if not pages: continue

            # Filter out continuous document results (sys:/part:) - they use wrong
            # raw_header (first page's header instead of actual match location).
            # Individual page results are more accurate.
            page_results = [p for p in pages if not str(p.get('uid', '')).startswith(('sys:', 'part:'))]
            continuous_results = [p for p in pages if str(p.get('uid', '')).startswith(('sys:', 'part:'))]

            # Use page results if available, otherwise fall back to continuous
            pages = page_results if page_results else continuous_results

            # Deduplicate pages by p_num within this group.
            # Same page can appear multiple times from V0.7 and V0.8.
            p_num_best = {}
            for p in pages:
                _, p_num = self.meta_mgr.parse_header_smart(p['raw_header'])
                if p_num and p_num != "Unknown":
                    if p_num not in p_num_best or p['score'] > p_num_best[p_num]['score']:
                        p_num_best[p_num] = p
                else:
                    # Pages without valid p_num: use uid as fallback key
                    uid_key = f"_uid_{p.get('uid', id(p))}"
                    if uid_key not in p_num_best or p['score'] > p_num_best[uid_key]['score']:
                        p_num_best[uid_key] = p
            pages = list(p_num_best.values())

            # Aggregate Score
            total_score = sum(p['score'] for p in pages)

            # Use the highest-scoring page as representative
            pages.sort(key=lambda x: x['score'], reverse=True)
            rep_page = pages[0]

            # Check if this is a Part grouping
            is_part = group_key.startswith("PART:")
            if is_part:
                part_id = group_key[5:]  # Remove "PART:" prefix
                part_meta = self.meta_mgr.get_part_metadata(part_id)
                folios = self.meta_mgr.get_folios_for_part(part_id) or []

                # Get Part display name
                part_display = self.meta_mgr.codico_mgr.get_part_display_name(part_id)

                manuscript_item = {
                    'type': 'part',
                    'part_id': part_id,
                    'part_display': part_display,
                    'sys_id': folios[0] if folios else None,  # First folio as representative
                    'folios': folios,
                    'score': total_score,
                    'pages': pages,
                    'raw_header': rep_page['raw_header'],
                    'text': rep_page['text'],
                    'source_ctx': rep_page.get('source_ctx', ''),
                    'highlight_pattern': rep_page.get('highlight_pattern', ''),
                    'oxford_title': part_meta.get('title', '') if part_meta else '',
                    'oxford_contents': part_meta.get('contents', '') if part_meta else '',
                }
            else:
                manuscript_item = {
                    'type': 'manuscript',
                    'sys_id': group_key,
                    'score': total_score,
                    'pages': pages,
                    'raw_header': rep_page['raw_header'],
                    'text': rep_page['text'],
                    'source_ctx': rep_page.get('source_ctx', ''),
                    'highlight_pattern': rep_page.get('highlight_pattern', '')
                }
            manuscripts.append(manuscript_item)

        # Sort manuscripts by aggregated score
        manuscripts.sort(key=lambda x: x['score'], reverse=True)
        return manuscripts

    def group_composition_results(self, items, threshold=5, progress_callback=None, status_callback=None, check_cancel=None):
        # 1. Collect IDs for metadata
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

        # Load metadata (fast due to previous fix)
        self.meta_mgr.batch_fetch_shelfmarks([x for x in ids if x], progress_callback=progress_callback)

        if status_callback:
            status_callback(tr("Grouping results..."))

        # 2. Prepare data for sorting
        IGNORE_PREFIXES = {'קטע', 'קטעי', 'גניזה', 'לא', 'מזוהה', 'חיבור', 'פילוסופיה', 'הלכה', 'שירה', 'פיוט', 'מסמך', 'מכתב', 'ספרות', 'סיפורת', 'יפה', 'דרשות', 'פרשנות', 'מקרא', 'בפילוסופיה', 'קטעים', 'וספרות', 'מוסר', 'הגות', 'וחכמת', 'הלשון', 'פירוש', 'תפסיר', 'שרח', 'על', 'ספר', 'כתאב', 'משנה', 'תלמוד'}

        def _get_clean_words(t):
            if not t: return []
            clean = re.sub(r'[^\w]', ' ', t)
            return [w for w in clean.split() if len(w) > 1]

        def _get_signature(title_str):
            words = _get_clean_words(title_str)
            while words and words[0] in IGNORE_PREFIXES: words.pop(0)
            if not words: return None
            # Signature: First two significant words
            return f"{words[0]} {words[1]}" if len(words) >= 2 else words[0]

        # 3. New Grouping Algorithm (Dictionary Based - O(N))
        # Instead of double loop, map all items by signature
        
        groups_map = defaultdict(list)
        wrapped_items = []
        total_items = len(items)

        for idx, item in enumerate(items):
            # Update GUI infrequently to prevent freezing
            if progress_callback and idx % 100 == 0:
                progress_callback(idx, total_items)
            
            if check_cancel and check_cancel(): return None, None, None

            # Extract title
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

        # 4. Filter groups by Threshold
        appendix = defaultdict(list)
        summary = defaultdict(list)

        for sig, group_items in groups_map.items():
            if len(group_items) > threshold:
                # Group large enough - move to appendix
                for w in group_items:
                    w['grouped'] = True
                    appendix[sig].append(w['item'])
                    summary[sig].append(w['shelfmark'])

        # 5. Create Main List (ungrouped)
        main_list = [w['item'] for w in wrapped_items if not w['grouped']]
        
        # Sort by score descending
        main_list.sort(key=lambda x: x['score'], reverse=True)
        
        # Final GUI update
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
        browse_map = self._load_browse_map()
        if not browse_map: return []
        
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
        
    def get_browse_page(self, sys_id, p_num=None, next_prev=0, absolute_index=None, allow_cross=False):
        browse_map = self._load_browse_map()
        if not browse_map: return None

        # Prepare ordered list for cross-manuscript navigation
        if allow_cross and (not hasattr(self, '_ordered_sys_ids') or not self._ordered_sys_ids):
            self._ordered_sys_ids = list(browse_map.keys())

        if sys_id not in browse_map: return None
        pages = browse_map[sys_id]
        if not pages: return None
        
        target_idx = -1

        # PRIORITY 1: Use Absolute Index if provided (Fixes duplicate page loop)
        if absolute_index is not None:
            if 0 <= absolute_index < len(pages):
                target_idx = absolute_index
            else:
                # If index is invalid, fallback to p_num logic? No, just fail or reset.
                pass 
        
        # PRIORITY 2: Search by p_num (Fallback / Initial Load)
        if target_idx == -1 and p_num is not None:
            # Robust casting
            try: p_val = int(p_num)
            except: p_val = -999
            
            for i, p in enumerate(pages):
                if p['p_num'] == p_val: 
                    target_idx = i; break
            
            # Smart Fallback: Find closest insertion point
            if target_idx == -1:
                for i, p in enumerate(pages):
                    if p['p_num'] > p_val:
                        target_idx = max(0, i - 1)
                        break
                if target_idx == -1: target_idx = len(pages) - 1

        # PRIORITY 3: Default to start
        if target_idx == -1: target_idx = 0
        
        # Calculate New Index
        new_idx = target_idx + next_prev
        
        # Handle crossing to adjacent manuscripts when requested
        if (new_idx < 0 or new_idx >= len(pages)) and allow_cross and next_prev != 0:
            direction = 1 if next_prev > 0 else -1
            adjacent_id = self.get_adjacent_sys_id_by_file_order(sys_id, direction)
            while adjacent_id:
                if adjacent_id in browse_map and browse_map[adjacent_id]:
                    pages = browse_map[adjacent_id]
                    sys_id = adjacent_id
                    new_idx = 0 if direction > 0 else len(pages) - 1
                    break
                adjacent_id = self.get_adjacent_sys_id_by_file_order(adjacent_id, direction)
            else:
                return None

        if new_idx < 0 or new_idx >= len(pages): return None
        
        target_page = pages[new_idx]
        text = self.get_full_text_by_id(target_page['uid'])
        
        return {
            'uid': target_page['uid'], 
            'p_num': target_page['p_num'],
            'full_header': target_page['full_header'], 
            'text': text,
            'total_pages': len(pages), 
            'current_idx': new_idx + 1, # Display is 1-based
            'internal_index': new_idx,  # 0-based for logic (NEW)
            'sys_id': sys_id
        }

    def get_browse_page_by_fl(self, fl_id, sys_id=None):
        browse_map = self._load_browse_map()
        if not browse_map: return None

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

    def get_adjacent_sys_id_by_file_order(self, current_sys_id, offset):
        """
        Returns the next/prev system ID based on the order in Transcriptions.txt.
        This relies on browse_map preserving insertion order.
        """
        # Load map if not already cached in memory for navigation
        if not hasattr(self, '_ordered_sys_ids') or not self._ordered_sys_ids:
            if not os.path.exists(Config.BROWSE_MAP):
                return None
            with open(Config.BROWSE_MAP, 'rb') as f:
                b_map = pickle.load(f)
                # list(dict.keys()) returns items in insertion order (File Order)
                self._ordered_sys_ids = list(b_map.keys())

        if not current_sys_id:
            return self._ordered_sys_ids[0] if self._ordered_sys_ids else None

        try:
            # Find current index
            curr_idx = self._ordered_sys_ids.index(current_sys_id)
            new_idx = curr_idx + offset
            
            # Check bounds
            if 0 <= new_idx < len(self._ordered_sys_ids):
                return self._ordered_sys_ids[new_idx]
        except ValueError:
            pass # Current ID not found in list
            
        return None

def calculate_smart_weights(file_path, sample_size=None):
    """
    Analyzes corpus to generate HTR-aware letter frequency weights.
    Robust version: Tries multiple encodings to ensure file reading.
    """
    size_desc = "ALL lines" if sample_size is None else f"{sample_size} lines"
    LAB_LOGGER.info(f"Calculating smart weights from {file_path} (Sample: {sample_size})...")
    
    total_letters = 0
    counts = defaultdict(int)
    
    encodings_to_try = ['utf-8-sig', 'utf-8', 'windows-1255', 'iso-8859-8', 'latin-1']
    
    file_read_success = False
    
    for enc in encodings_to_try:
        try:
            temp_counts = defaultdict(int)
            temp_total = 0
            
            with open(file_path, 'r', encoding=enc) as f:
                for i, line in enumerate(f):
                    if sample_size is not None and i >= sample_size: 
                        break
                    if line.startswith("==>") or line.startswith("###"): continue
                    
                    text = re.sub(r"[^\u0590-\u05FF]", "", line)
                    if not text: continue
                    
                    for char in text:
                        temp_counts[char] += 1
                        temp_total += 1
            
            if temp_total > 0:
                counts = temp_counts
                total_letters = temp_total
                file_read_success = True
                LAB_LOGGER.info(f"Successfully read corpus using encoding: {enc}")
                break
                
        except (UnicodeDecodeError, UnicodeError):
            continue
        except Exception as e:
            LAB_LOGGER.error(f"Error reading file with {enc}: {e}")
            break

    if not file_read_success or total_letters == 0:
        LAB_LOGGER.error("Failed to read corpus with any encoding or file is empty. Using static weights.")
        return HEBREW_FREQ
    
    # 2. Analyze & Score
    analysis_rows = []
    final_scores = {}

    for char, count in counts.items():
        if char not in STANDARD_HEBREW_DIST: continue
        
        corpus_pct = (count / total_letters) * 100
        standard_pct = STANDARD_HEBREW_DIST.get(char, 0.1)
        
        ratio = corpus_pct / standard_pct
        score = (1 / corpus_pct) if corpus_pct > 0 else 100.0
        
        original_score = score
        if ratio > 1.5:
            score = score / (ratio ** 2)
            
        final_scores[char] = score
        
        analysis_rows.append({
            'Letter': char,
            'Standard_Pct': round(standard_pct, 4),
            'Corpus_Pct': round(corpus_pct, 4),
            'Ratio_Suspicion': round(ratio, 2),
            'Original_Score': round(original_score, 4),
            'Penalized_Score': round(score, 4)
        })

    # 3. Save Report
    LAB_LOGGER.info(f"DEBUG: Preparing to save HTR report with {len(analysis_rows)} rows...") # <--- שורה חדשה
    try:
        os.makedirs(Config.REPORTS_DIR, exist_ok=True)
        report_path = os.path.join(Config.REPORTS_DIR, "HTR_Frequency_Analysis.csv")
        
        with open(report_path, 'w', encoding='utf-8-sig', newline='') as f:
            fieldnames = ['Letter', 'Standard_Pct', 'Corpus_Pct', 'Ratio_Suspicion', 'Original_Score', 'Penalized_Score']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            sorted_rows = sorted(analysis_rows, key=lambda x: x['Ratio_Suspicion'], reverse=True)
            writer.writerows(sorted_rows)
            
        LAB_LOGGER.info(f"SUCCESS: HTR Report saved to: {report_path}") # <--- שורה חדשה לאישור הצלחה
        
    except Exception as e:
        LAB_LOGGER.warning(f"Failed to save HTR report: {e}")

    # 4. Normalize to Ranks
    LAB_LOGGER.info("DEBUG: Normalizing scores to integer ranks...") # <--- שורה חדשה
    sorted_chars = sorted(final_scores.keys(), key=lambda x: final_scores[x], reverse=True)
    rank_map = {}
    max_rank = len(sorted_chars)
    for i, char in enumerate(sorted_chars):
        rank_map[char] = max_rank - i
        
    # 5. Save JSON
    try:
        os.makedirs(Config.LAB_DIR, exist_ok=True)
        LAB_LOGGER.info(f"DEBUG: Saving JSON weights to {Config.LAB_WEIGHTS_FILE}...") # <--- שורה חדשה
        
        with open(Config.LAB_WEIGHTS_FILE, 'w', encoding='utf-8') as f:
            json.dump(rank_map, f, ensure_ascii=False, indent=2)
            
        LAB_LOGGER.info(f"SUCCESS: Lab weights JSON saved successfully.") # <--- שורה חדשה לאישור הצלחה
        
    except Exception as e:
        LAB_LOGGER.error(f"Failed to save lab weights JSON: {e}")

    return rank_map


# ==============================================================================
#  PERSONAL LISTS MANAGER
# ==============================================================================

class ListsManager:
    """
    Manages personal lists (starred/saved manuscripts) with tags and notes.

    Features:
    - Multiple named lists with colors
    - Built-in "כללי" (General) default list
    - Built-in "נצפו לאחרונה" (Recently Viewed) auto-populated list
    - Tags and notes per item
    - Export/import functionality
    """

    LISTS_FILE = os.path.join(Config.INDEX_DIR, "lists.pkl")
    MAX_RECENT_ITEMS = 50

    # Default colors for lists
    DEFAULT_COLORS = [
        '#FFD700',  # Gold (default)
        '#4CAF50',  # Green
        '#2196F3',  # Blue
        '#9C27B0',  # Purple
        '#FF5722',  # Deep Orange
        '#00BCD4',  # Cyan
        '#E91E63',  # Pink
        '#795548',  # Brown
        '#607D8B',  # Blue Grey
        '#F44336',  # Red
    ]

    def __init__(self, meta_mgr=None):
        """Initialize the lists manager."""
        self.meta_mgr = meta_mgr
        self.data = self._get_default_data()
        self.load()

    def _get_default_data(self):
        """Return the default data structure."""
        import time
        return {
            'lists': {
                'default': {
                    'name': 'כללי',
                    'name_en': 'General',
                    'color': '#FFD700',
                    'created': time.time(),
                    'is_default': True,
                    'is_system': False
                },
                'recent': {
                    'name': 'נצפו לאחרונה',
                    'name_en': 'Recently Viewed',
                    'color': '#9E9E9E',
                    'is_system': True,
                    'max_items': self.MAX_RECENT_ITEMS
                }
            },
            'items': {},  # sys_id -> item data
            'recent_items': [],  # ordered list of sys_ids (most recent first)
            'all_tags': []  # for autocomplete
        }

    def load(self):
        """Load lists from file."""
        if os.path.exists(self.LISTS_FILE):
            try:
                with open(self.LISTS_FILE, 'rb') as f:
                    loaded = pickle.load(f)
                    # Merge with defaults to handle new fields
                    defaults = self._get_default_data()
                    for key in defaults:
                        if key not in loaded:
                            loaded[key] = defaults[key]
                    # Ensure system lists exist
                    if 'default' not in loaded['lists']:
                        loaded['lists']['default'] = defaults['lists']['default']
                    if 'recent' not in loaded['lists']:
                        loaded['lists']['recent'] = defaults['lists']['recent']
                    self.data = loaded
            except Exception as e:
                LOGGER.warning(f"Failed to load lists: {e}")
                self.data = self._get_default_data()
        else:
            self.data = self._get_default_data()

    def save(self):
        """Save lists to file."""
        try:
            os.makedirs(Config.INDEX_DIR, exist_ok=True)
            with open(self.LISTS_FILE, 'wb') as f:
                pickle.dump(self.data, f)
        except Exception as e:
            LOGGER.error(f"Failed to save lists: {e}")

    # --- List Management ---

    def get_all_lists(self, include_recent=True):
        """Get all lists sorted alphabetically (system lists have special handling)."""
        lists = []
        for list_id, list_data in self.data['lists'].items():
            if list_id == 'recent' and not include_recent:
                continue
            lists.append({
                'id': list_id,
                **list_data,
                'count': self._get_list_item_count(list_id)
            })

        # Sort: default first, then recent, then alphabetically by name
        def sort_key(lst):
            if lst['id'] == 'default':
                return (0, '')
            elif lst['id'] == 'recent':
                return (1, '')
            else:
                return (2, lst.get('name', ''))

        return sorted(lists, key=sort_key)

    def _get_list_item_count(self, list_id):
        """Get the number of items in a list."""
        if list_id == 'recent':
            return len(self.data.get('recent_items', []))

        count = 0
        for item in self.data['items'].values():
            if list_id in item.get('lists', []):
                count += 1
        return count

    def create_list(self, name, color=None):
        """Create a new list. Returns the list ID."""
        import time
        import uuid

        list_id = f"list_{uuid.uuid4().hex[:8]}"

        if color is None:
            # Pick next available color
            used_colors = {lst.get('color') for lst in self.data['lists'].values()}
            for c in self.DEFAULT_COLORS:
                if c not in used_colors:
                    color = c
                    break
            if color is None:
                color = self.DEFAULT_COLORS[0]

        self.data['lists'][list_id] = {
            'name': name,
            'color': color,
            'created': time.time()
        }
        self.save()
        return list_id

    def update_list(self, list_id, name=None, color=None):
        """Update list properties."""
        if list_id not in self.data['lists']:
            return False

        lst = self.data['lists'][list_id]
        if lst.get('is_system'):
            return False  # Cannot edit system lists

        if name is not None:
            lst['name'] = name
        if color is not None:
            lst['color'] = color

        self.save()
        return True

    def delete_list(self, list_id):
        """Delete a list and all its items."""
        if list_id not in self.data['lists']:
            return False

        lst = self.data['lists'][list_id]
        if lst.get('is_default') or lst.get('is_system'):
            return False  # Cannot delete system lists

        # Remove list reference from all items
        items_to_remove = []
        for sys_id, item in self.data['items'].items():
            if list_id in item.get('lists', []):
                item['lists'].remove(list_id)
                # If item has no more lists, mark for removal
                if not item['lists']:
                    items_to_remove.append(sys_id)

        # Remove orphaned items
        for sys_id in items_to_remove:
            del self.data['items'][sys_id]

        del self.data['lists'][list_id]
        self.save()
        return True

    def duplicate_list(self, list_id, new_name=None):
        """Duplicate a list with all its items."""
        if list_id not in self.data['lists']:
            return None

        original = self.data['lists'][list_id]
        if new_name is None:
            new_name = f"{original.get('name', 'רשימה')} (עותק)"

        new_list_id = self.create_list(new_name, original.get('color'))

        # Copy items
        for sys_id, item in self.data['items'].items():
            if list_id in item.get('lists', []):
                if new_list_id not in item['lists']:
                    item['lists'].append(new_list_id)

        self.save()
        return new_list_id

    def merge_lists(self, source_list_id, target_list_id, delete_source=True):
        """Merge source list into target list."""
        if source_list_id not in self.data['lists'] or target_list_id not in self.data['lists']:
            return False

        source = self.data['lists'][source_list_id]
        if source.get('is_system'):
            return False  # Cannot merge system lists

        # Move items from source to target
        for sys_id, item in self.data['items'].items():
            if source_list_id in item.get('lists', []):
                if target_list_id not in item['lists']:
                    item['lists'].append(target_list_id)

        if delete_source:
            self.delete_list(source_list_id)
        else:
            self.save()

        return True

    # --- Item Management ---

    def add_item(self, sys_id, list_id='default', note='', tags=None, source=''):
        """Add an item to a list. Returns True if added, False if already exists."""
        import time

        if list_id not in self.data['lists']:
            return False

        if sys_id in self.data['items']:
            # Item exists, add to list if not already
            item = self.data['items'][sys_id]
            if list_id in item.get('lists', []):
                return False  # Already in this list
            item['lists'].append(list_id)
            item['modified'] = time.time()
        else:
            # New item
            self.data['items'][sys_id] = {
                'lists': [list_id],
                'tags': tags or [],
                'note': note,
                'source': source,
                'added': time.time(),
                'modified': time.time(),
                'shelfmark_override': None  # For unidentified items
            }

        # Update all_tags
        if tags:
            for tag in tags:
                if tag not in self.data['all_tags']:
                    self.data['all_tags'].append(tag)

        self.save()
        return True

    def add_items_bulk(self, sys_ids, list_id='default', source=''):
        """Add multiple items to a list at once."""
        import time

        if list_id not in self.data['lists']:
            return 0

        added = 0
        for sys_id in sys_ids:
            if sys_id in self.data['items']:
                item = self.data['items'][sys_id]
                if list_id not in item.get('lists', []):
                    item['lists'].append(list_id)
                    item['modified'] = time.time()
                    added += 1
            else:
                self.data['items'][sys_id] = {
                    'lists': [list_id],
                    'tags': [],
                    'note': '',
                    'source': source,
                    'added': time.time(),
                    'modified': time.time(),
                    'shelfmark_override': None
                }
                added += 1

        self.save()
        return added

    def update_item(self, sys_id, note=None, tags=None, shelfmark_override=None):
        """Update an item's properties."""
        import time

        if sys_id not in self.data['items']:
            return False

        item = self.data['items'][sys_id]

        if note is not None:
            item['note'] = note
        if tags is not None:
            item['tags'] = tags
            # Update all_tags
            for tag in tags:
                if tag not in self.data['all_tags']:
                    self.data['all_tags'].append(tag)
        if shelfmark_override is not None:
            item['shelfmark_override'] = shelfmark_override

        item['modified'] = time.time()
        self.save()
        return True

    def remove_item_from_list(self, sys_id, list_id):
        """Remove an item from a specific list."""
        if sys_id not in self.data['items']:
            return False

        item = self.data['items'][sys_id]
        if list_id not in item.get('lists', []):
            return False

        item['lists'].remove(list_id)

        # If item has no more lists, remove it entirely
        if not item['lists']:
            del self.data['items'][sys_id]

        self.save()
        return True

    def move_items_to_list(self, sys_ids, from_list_id, to_list_id):
        """Move items from one list to another."""
        import time

        for sys_id in sys_ids:
            if sys_id in self.data['items']:
                item = self.data['items'][sys_id]
                if from_list_id in item.get('lists', []):
                    item['lists'].remove(from_list_id)
                if to_list_id not in item.get('lists', []):
                    item['lists'].append(to_list_id)
                item['modified'] = time.time()

        self.save()

    def get_items_in_list(self, list_id):
        """Get all items in a list with their metadata."""
        if list_id == 'recent':
            items = []
            for sys_id in self.data.get('recent_items', []):
                item_data = self.data['items'].get(sys_id, {})
                items.append({
                    'sys_id': sys_id,
                    **item_data
                })
            return items

        items = []
        for sys_id, item_data in self.data['items'].items():
            if list_id in item_data.get('lists', []):
                items.append({
                    'sys_id': sys_id,
                    **item_data
                })
        return items

    def get_item(self, sys_id):
        """Get a single item's data."""
        if sys_id in self.data['items']:
            return {'sys_id': sys_id, **self.data['items'][sys_id]}
        return None

    def is_item_in_any_list(self, sys_id):
        """Check if an item is in any list (excluding recent)."""
        return sys_id in self.data['items']

    def get_item_lists(self, sys_id):
        """Get list of lists an item belongs to."""
        if sys_id not in self.data['items']:
            return []
        return self.data['items'][sys_id].get('lists', [])

    # --- Recently Viewed ---

    def add_to_recent(self, sys_id):
        """Add an item to the recently viewed list."""
        import time

        recent = self.data.get('recent_items', [])

        # Remove if already present (we'll add to front)
        if sys_id in recent:
            recent.remove(sys_id)

        # Add to front
        recent.insert(0, sys_id)

        # Trim to max size
        if len(recent) > self.MAX_RECENT_ITEMS:
            recent = recent[:self.MAX_RECENT_ITEMS]

        self.data['recent_items'] = recent

        # Also ensure item exists in items dict for metadata
        if sys_id not in self.data['items']:
            self.data['items'][sys_id] = {
                'lists': [],  # Not in any regular list, just recent
                'tags': [],
                'note': '',
                'source': '',
                'added': time.time(),
                'modified': time.time(),
                'shelfmark_override': None
            }

        self.save()

    # --- Tags ---

    def get_all_tags(self):
        """Get all tags for autocomplete."""
        return sorted(self.data.get('all_tags', []))

    def add_tag_to_items(self, sys_ids, tag):
        """Add a tag to multiple items."""
        import time

        for sys_id in sys_ids:
            if sys_id in self.data['items']:
                item = self.data['items'][sys_id]
                if tag not in item.get('tags', []):
                    if 'tags' not in item:
                        item['tags'] = []
                    item['tags'].append(tag)
                    item['modified'] = time.time()

        if tag not in self.data['all_tags']:
            self.data['all_tags'].append(tag)

        self.save()

    # --- Export/Import ---

    def export_list(self, list_id, include_metadata=True, include_snippets=False):
        """Export a list to a dictionary suitable for JSON serialization."""
        if list_id not in self.data['lists']:
            return None

        list_info = self.data['lists'][list_id]
        items = self.get_items_in_list(list_id)

        export_data = {
            'version': 1,
            'list_name': list_info.get('name', ''),
            'list_color': list_info.get('color', ''),
            'exported_at': time.time(),
            'items': []
        }

        for item in items:
            item_export = {
                'sys_id': item['sys_id'],
                'tags': item.get('tags', []),
                'note': item.get('note', ''),
                'source': item.get('source', ''),
                'shelfmark_override': item.get('shelfmark_override')
            }

            if include_metadata and self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(item['sys_id'])
                item_export['shelfmark'] = shelfmark
                item_export['title'] = title

            # Snippets would require access to the search engine - skip for now

            export_data['items'].append(item_export)

        return export_data

    def import_list(self, import_data, list_name_override=None):
        """Import a list from exported data. Returns (list_id, imported_count, unidentified_count)."""
        if not import_data or 'items' not in import_data:
            return None, 0, 0

        list_name = list_name_override or import_data.get('list_name', 'רשימה מיובאת')
        list_color = import_data.get('list_color')

        list_id = self.create_list(list_name, list_color)

        imported = 0
        unidentified = 0

        for item in import_data['items']:
            sys_id = item.get('sys_id')
            if not sys_id:
                continue

            # Check if this sys_id exists in our database
            is_identified = True
            if self.meta_mgr:
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)
                if not shelfmark or shelfmark == 'Unknown':
                    is_identified = False
                    unidentified += 1

            self.add_item(
                sys_id=sys_id,
                list_id=list_id,
                note=item.get('note', ''),
                tags=item.get('tags', []),
                source=item.get('source', '')
            )

            # Set shelfmark override for unidentified items
            if not is_identified and item.get('shelfmark'):
                self.update_item(sys_id, shelfmark_override=item.get('shelfmark'))

            imported += 1

        return list_id, imported, unidentified

    # --- Sorting ---

    @staticmethod
    def shelfmark_sort_key(shelfmark):
        """
        Sort key for shelfmarks that handles dots correctly.
        E.g., T-S K1.2 < T-S K1.10 (not lexicographic)
        """
        if not shelfmark:
            return ('', [])

        # Split into parts
        parts = re.split(r'(\d+)', shelfmark)
        result = []
        for part in parts:
            if part.isdigit():
                result.append((0, int(part)))  # Numbers sort first by value
            else:
                result.append((1, part.lower()))  # Strings sort lexicographically
        return result

    def get_items_sorted(self, list_id, sort_by='shelfmark', reverse=False):
        """Get items in a list, sorted by the specified field."""
        items = self.get_items_in_list(list_id)

        # Enrich with metadata
        if self.meta_mgr:
            for item in items:
                sys_id = item['sys_id']
                shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)
                item['shelfmark'] = item.get('shelfmark_override') or shelfmark or 'Unknown'
                item['title'] = title or ''

        if sort_by == 'shelfmark':
            items.sort(key=lambda x: self.shelfmark_sort_key(x.get('shelfmark', '')), reverse=reverse)
        elif sort_by == 'title':
            items.sort(key=lambda x: x.get('title', '').lower(), reverse=reverse)
        elif sort_by == 'added':
            items.sort(key=lambda x: x.get('added', 0), reverse=not reverse)  # Default newest first
        elif sort_by == 'modified':
            items.sort(key=lambda x: x.get('modified', 0), reverse=not reverse)

        return items

    # --- Copy Info ---

    def get_item_copy_text(self, sys_id, format_type='compact'):
        """
        Generate text for copying item info.
        format_type: 'compact', 'detailed', 'with_link'
        """
        if sys_id not in self.data['items']:
            return ''

        item = self.data['items'][sys_id]

        shelfmark = 'Unknown'
        title = ''

        if item.get('shelfmark_override'):
            shelfmark = item['shelfmark_override']
        elif self.meta_mgr:
            shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)

        if format_type == 'compact':
            if title:
                return f"{shelfmark} - {title}"
            return shelfmark

        elif format_type == 'detailed':
            lines = [f"מספר מדף: {shelfmark}"]
            if title:
                lines.append(f"כותרת: {title}")
            lines.append(f"מספר מערכת: {sys_id}")
            if item.get('note'):
                lines.append(f"הערה: {item['note']}")
            return '\n'.join(lines)

        elif format_type == 'with_link':
            lines = [f"מספר מדף: {shelfmark}"]
            if title:
                lines.append(f"כותרת: {title}")
            lines.append(f"מספר מערכת: {sys_id}")
            # Add Ktiv link
            ktiv_url = f"https://web.nli.org.il/sites/NLIS/he/ManuScript/Pages/Item.aspx?ItemID={sys_id}"
            lines.append(f"קישור: {ktiv_url}")
            return '\n'.join(lines)

        return shelfmark
