# -*- coding: utf-8 -*-
"""LabEngine: Lab-mode fingerprint composition search + LOCAL-LAB side-index.

Phase 125: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import LabEngine`` callers continue working.
"""

import logging
import os
import re
import shutil
import json
import time
from collections import defaultdict
from functools import lru_cache

try:
    import tantivy
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")

from shared.config import Config
from shared.lab_settings import LabSettings
from shared.text_normalize import strip_nikud, strip_search_diacritics

LOGGER = logging.getLogger("genizah." + __name__)

# LAB_LOGGER: this is the SAME named logger that configure_lab_logger() in
# genizah_core.py configured with file+console handlers and propagate=False.
# logging.getLogger returns the same instance by name — NOT a new logger.
# Keep ALL LAB_LOGGER references in this file as LAB_LOGGER (never substitute LOGGER).
LAB_LOGGER = logging.getLogger("GenizahLab")


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

        # CR-02 FIX: LOCAL LAB side-index attributes — mirror SearchEngine so
        # LabEngine.lab_composition_search can query LOCAL LAB hits in LAB mode.
        # Previously these attributes only existed on SearchEngine, so the
        # `getattr(self, "_check_local_lab_freshness", None)` guard in
        # lab_composition_search returned None and the entire LOCAL LAB hook
        # was silently skipped — REQ-6 (three-surface coverage) was broken.
        self.local_lab_searcher = None
        self._local_lab_index = None
        self.local_lab_searcher_stale = False
        self._lab_local_meta = None

        # Try load dynamic weights
        if os.path.exists(Config.LAB_WEIGHTS_FILE):
            try:
                with open(Config.LAB_WEIGHTS_FILE, 'r', encoding='utf-8') as f:
                    self.dynamic_rank_map = json.load(f)
            except Exception:
                # Dynamic weights file corrupt or unreadable; keep defaults.
                logging.getLogger(__name__).warning(
                    'Failed to load dynamic weights from %s; using defaults',
                    Config.LAB_WEIGHTS_FILE, exc_info=True,
                )

        self._reload_lab_index()
        # CR-02 FIX: open LOCAL LAB side-index at startup so LAB-mode
        # Composition Search sees LOCAL hits without waiting for a refresh.
        self.reload_local_lab_index()

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
            pass  # Tokenizer registration may fail on reopen; non-fatal, search still works
        try:
            index.register_tokenizer("simple", tantivy.TextAnalyzerBuilder(tantivy.Tokenizer.simple()).build())
        except Exception:
            pass  # Tokenizer registration may fail on reopen; non-fatal, search still works

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

    # ------------------------------------------------------------------
    # CR-02 FIX: LOCAL LAB side-index handling on LabEngine
    # ------------------------------------------------------------------
    # These mirror the SearchEngine.reload_local_lab_index /
    # _check_local_lab_freshness methods so LabEngine.lab_composition_search
    # actually surfaces LOCAL hits in LAB mode (REQ-6).  Wired by
    # MyLibraryTab on startup + after every Refresh / Add / Remove.
    def reload_local_lab_index(self) -> None:
        """Reopen the LOCAL LAB side-index against the current Config.LOCAL_LAB_INDEX_DIR.

        Idempotent + defensive: D-37 semantics — on any open failure the
        searcher falls back to None and the LAB-mode composition path
        cleanly skips LOCAL.
        """
        self.local_lab_searcher = None
        self._local_lab_index = None
        self._lab_local_meta = None
        try:
            if os.path.isdir(Config.LOCAL_LAB_INDEX_DIR):
                from shared.local_indexer import build_local_lab_schema, LocalIndexer
                schema = build_local_lab_schema()
                local_lab_index = tantivy.Index(schema, path=Config.LOCAL_LAB_INDEX_DIR)
                # Phase 110 UAT BLOCKER: the LOCAL LAB schema declares the
                # fingerprint / fingerprint_dyn / content fields with
                # tokenizer_name="simple" (and text_ngram with "whitespace").
                # A freshly-opened tantivy.Index does NOT know those custom
                # tokenizers, so EVERY parse_query against the fingerprint field
                # raised ValueError('The tokenizer "simple" ... is unknown'),
                # which lab_composition_search's `except (ValueError, RuntimeError):
                # continue` swallowed — silently skipping every chunk and returning
                # ZERO LOCAL LAB hits. The Genizah lab index never hit this because
                # _reload_lab_index() calls _ensure_lab_tokenizers(); the LOCAL
                # reload simply forgot to. Register them here, before any searcher
                # query, exactly as _reload_lab_index does.
                self._ensure_lab_tokenizers(local_lab_index)
                self._local_lab_index = local_lab_index
                self.local_lab_searcher = local_lab_index.searcher()
                self._lab_local_meta = LocalIndexer.read_lab_meta(Config.LOCAL_LAB_INDEX_DIR)
                LAB_LOGGER.info(
                    "CR-02: LabEngine LOCAL LAB side-index reopened: %s",
                    Config.LOCAL_LAB_INDEX_DIR,
                )
            else:
                LAB_LOGGER.info(
                    "CR-02: LabEngine LOCAL LAB side-index dir absent; searcher=None"
                )
        except Exception as e:
            LAB_LOGGER.warning(
                "CR-02: LabEngine LOCAL LAB side-index unavailable: %r", e
            )
            self.local_lab_searcher = None
            self._local_lab_index = None
            self._lab_local_meta = None

    def _current_lab_weights_hash(self) -> str:
        """Compute hash of current LAB weights for D-38 staleness check.

        Mirrors SearchEngine._current_lab_weights_hash; uses the real
        dynamic_rank_map / settings that live on LabEngine.
        """
        import hashlib as _hashlib
        import json as _json
        weights_dict = {
            "dynamic_rank_map": self.dynamic_rank_map if self.dynamic_rank_map else None,
            "use_dynamic_weights": getattr(self.settings, "use_dynamic_weights", False),
        }
        return _hashlib.sha256(
            _json.dumps(weights_dict, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    def _check_local_lab_freshness(self) -> bool:
        """Return True if LOCAL LAB index is fresh; False if stale or missing.

        D-38 mirror on LabEngine: compares current LAB weights_hash to value
        stored in .meta.json by build_lab_side_index. Side effect: sets
        self.local_lab_searcher_stale.
        """
        if getattr(self, "local_lab_searcher", None) is None:
            return False
        meta = getattr(self, "_lab_local_meta", None)
        if not meta:
            self.local_lab_searcher_stale = True
            LAB_LOGGER.info("CR-02: LabEngine LOCAL LAB has no .meta.json — stale")
            return False
        current_hash = self._current_lab_weights_hash()
        if meta.get("weights_hash") != current_hash:
            self.local_lab_searcher_stale = True
            LAB_LOGGER.info(
                "CR-02: LabEngine LOCAL LAB index stale (weights changed)"
            )
            return False
        self.local_lab_searcher_stale = False
        return True

    @staticmethod
    def lab_index_normalize(text):
        return re.sub(r"[^\w֐-׿\s\*\~]", "", text).replace('_', ' ').lower()

    def rebuild_lab_index(self, progress_callback=None):
        from genizah_core import calculate_smart_weights, text_to_fingerprint, HEBREW_FREQ  # noqa: PLC0415 — lazy; GUARD-01 safe
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
            except Exception as e:
                logging.getLogger(__name__).debug('Could not count documents in %s: %s', fname, e)
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
                continue  # Try next query strategy; all-fail logged after loop by LAB_LOGGER.error

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
                except (InterruptedError, KeyboardInterrupt):
                    raise
                except Exception:
                    pass  # Score extraction optional — result still usable without score
                # Send text status for Label.
                # Same guard as the numeric call above: cancellation must propagate,
                # but a callback that can't handle the single-string protocol must
                # degrade to "no status text" — never abort a long deep-scan search
                # (prod 2026-06-12: web two-arg callback raised TypeError here).
                try:
                    progress_callback(f"Scanning items {i}-{min(i+BATCH_SIZE, total_hits)} / {total_hits}...")
                except (InterruptedError, KeyboardInterrupt):
                    raise
                except Exception:
                    pass  # Status text optional — search proceeds without it

            for hit in batch:
                yield hit

    def _get_term_weight(self, fp):
        """
        Calculates importance using User Configurable Stop-Word scores.
        """
        from genizah_core import HEBREW_FREQ  # noqa: PLC0415 — lazy; GUARD-01 safe
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
        from genizah_core import encode_word_shmidman  # noqa: PLC0415 — lazy; GUARD-01 safe
        if not text:
            return 0, [], (0, 0)

        # 1. Exact Match Check
        def safe_norm(s): return re.sub(r"[^\w֐-׿]", "", s).lower()
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

        for m in re.finditer(r"[\w֐-׿\']+", text):
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

    def lab_search(self, query_str, mode='variants', progress_callback=None, gap=0, deep_scan=False, scan_limit=50000,
                   corpus_scope: str = 'genizah'):
        """Lab Mode (fingerprint) word search.

        Phase 110 (UAT bug #2): honor the corpus selector — ``corpus_scope`` is
        'genizah' (Genizah LAB index only, legacy default), 'local' (LOCAL LAB
        side-index only), or 'all' (both, merged). Previously lab_search ignored
        the selector entirely and always queried the Genizah LAB index, so a
        regular Search-tab "Lab Mode + Local" run returned Genizah hits. Corpus
        is orthogonal to mode (Lab Mode is NOT hardwired to LOCAL), mirroring the
        composition path (lab_composition_search).
        """
        from genizah_core import text_to_fingerprint, HEBREW_FREQ  # noqa: PLC0415 — lazy; GUARD-01 safe
        from shared.search_engine import make_mark_tolerant_pattern  # noqa: PLC0415 — lazy; GUARD-01 safe
        # Phase 110 C4: fail CLOSED — never expose LOCAL on a bad value.
        if corpus_scope not in ('genizah', 'local', 'all'):
            corpus_scope = 'genizah'

        # Strip combining diacritical marks and geresh/gershayim from query
        query_str = strip_search_diacritics(query_str)

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

        results = []
        min_match_pct = self.settings.min_should_match

        # --- Shared per-doc processing (Genizah + LOCAL) ---
        def _process_lab_doc(doc, is_local):
            content = doc['content'][0]
            uid = doc['unique_id'][0]

            # --- Core: Calculate Score & Find Matches ---
            custom_score, matches, best_window = self._calculate_match_metrics(content, query_fp_list, query_str, freq_map=target_map)

            if custom_score < 15:
                return

            # Filter by Percentage (Approximate)
            if min_match_pct < 100:
                found_unique = set(m['fp'] for m in matches)
                needed_unique = set(query_fp_list)
                common = found_unique.intersection(needed_unique)
                if len(needed_unique) > 0 and (len(common) / len(needed_unique) * 100 < min_match_pct):
                    return

            # --- Highlight Snippet ---
            smart_snippet = self._generate_highlighted_snippet(content, matches, best_window)
            html_snippet = smart_snippet  # No HTML conversion needed, pure markers

            start_idx, end_idx = best_window
            relevant_matches = matches[start_idx : end_idx + 1]
            found_words = list(set(m['word'] for m in relevant_matches))
            found_words.sort(key=len, reverse=True)
            highlight_regex_str = "|".join(make_mark_tolerant_pattern(re.escape(w)) for w in found_words) if found_words else ""

            full_header = doc['full_header'][0]
            if is_local:
                # Phase 110 bug #2: build the LOCAL hit shape the search-results
                # renderer expects (load_next_batch reads display.source=='LOCAL'
                # and resolves the filename/parent-folder from the canonical
                # filepath). Parse sys_id + page from the LOCAL full_header
                # ({sys_id}_LOCAL_P{page}_F{file_id}) — same as _build_local_result_dict.
                sys_id = ""
                p_num = "1"
                _parts = full_header.split("_LOCAL_P")
                if len(_parts) == 2:
                    sys_id = _parts[0]
                    p_num = _parts[1].split("_F")[0]
                try:
                    _shelf = doc['shelfmark'][0] if doc['shelfmark'] else sys_id
                except Exception:
                    _shelf = sys_id
                display_meta = {
                    "id": sys_id,
                    "source": "LOCAL",
                    "library_code": "LOCAL",
                    "shelfmark": _shelf,
                    "img": p_num,
                }
                results.append({
                    'sort_score': custom_score,
                    'display': display_meta,
                    'snippet': html_snippet,
                    'full_text': content,
                    'uid': uid,
                    'raw_header': full_header,
                    'raw_file_hl': smart_snippet,
                    'highlight_pattern': highlight_regex_str,
                    # LOCAL extras for ResultDialog / Browse / file-open actions
                    'sys_id': sys_id,
                    'p_num': p_num,
                    'img': p_num,
                    'full_header': full_header,
                    'score': float(custom_score),
                })
            else:
                # Populate display metadata correctly
                display_meta = self.meta_mgr.get_display_data(full_header, doc['source'][0])
                results.append({
                    'sort_score': custom_score,
                    'display': display_meta,
                    'snippet': html_snippet,
                    'full_text': content,
                    'uid': uid,
                    'raw_header': full_header,
                    'raw_file_hl': smart_snippet,
                    # This is the magic key for the Viewer:
                    'highlight_pattern': highlight_regex_str
                })

        # 3. Process — Genizah LAB loop (skipped for corpus_scope='local', and
        # gracefully skipped if the Genizah LAB index was never built).
        if corpus_scope != 'local' and self.lab_searcher is not None and self.lab_index is not None:
            query_obj = self._create_lab_query(fp_str, slop, field_name=target_field)
            if query_obj:
                if deep_scan:
                    # Use Deep Scan batched iterator
                    def batch_cb(*args):
                        if progress_callback:
                            try:
                                progress_callback(*args)
                            except Exception:
                                pass  # Progress callback optional — search proceeds without progress updates

                    iterator = self._execute_batched_search(query_obj, progress_callback=batch_cb, limit_override=scan_limit)
                else:
                    # Standard Fast Method
                    try:
                        # Limit 5000 for standard scan
                        res = self.lab_searcher.search(query_obj, 5000)
                        iterator = res.hits
                    except Exception as e:
                        LOGGER.debug('Batched search query failed, falling back to empty: %s', e)
                        iterator = []

                for score, doc_addr in iterator:
                    try:
                        _process_lab_doc(self.lab_searcher.doc(doc_addr), is_local=False)
                    except Exception as e:
                        LAB_LOGGER.error(f"Error processing doc: {e}")

        # 3b. LOCAL LAB loop (corpus_scope 'local' or 'all'). Mirrors the LOCAL
        # extension in lab_composition_search: query the LOCAL LAB side-index with
        # the same fingerprint field, build LOCAL-shaped hits. The simple/whitespace
        # tokenizers are registered by reload_local_lab_index (Phase 110 UAT fix) so
        # parse_query on the fingerprint field no longer raises.
        if (corpus_scope != 'genizah'
                and getattr(self, 'local_lab_searcher', None) is not None
                and getattr(self, '_local_lab_index', None) is not None):
            _tab = self._my_library_tab_ref() if getattr(self, "_my_library_tab_ref", None) is not None else None
            _searchable = getattr(_tab, "is_searchable", True) if _tab is not None else True
            if _searchable:
                try:
                    _clauses = [f'{target_field}:{t}' for t in fp_str.split()]
                    _core_query = " OR ".join(_clauses)
                    _q_obj = self._local_lab_index.parse_query(_core_query)
                    _res = self.local_lab_searcher.search(_q_obj, 5000)
                    for _score, _doc_addr in _res.hits:
                        try:
                            _process_lab_doc(self.local_lab_searcher.doc(_doc_addr), is_local=True)
                        except Exception as _e:
                            LAB_LOGGER.error(f"Error processing LOCAL LAB doc: {_e}")
                except (ValueError, RuntimeError):
                    pass  # tokenizer/parse issue — skip LOCAL contribution gracefully
                except Exception as _local_exc:
                    LAB_LOGGER.warning("lab_search LOCAL LAB scan failed: %r", _local_exc)

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

    def lab_composition_search(self, full_text, mode='variants', progress_callback=None, chunk_size=None,
                                excluded_ids=None, filter_text=None, deep_scan=False, scan_limit=50000,
                                boundary_mode='full', boundary_delimiter='\n', boundary_boost=1.5,
                                min_boundary_matches=0, min_delimiter_distance=3,
                                corpus_scope: str = 'genizah'):
        """
        Scans a composition using Lab Mode.
        UPGRADES:
        1. Filters common phrases.
        2. Boosts V0.8.
        3. FIX: Separates excluded/known manuscripts.
        4. Supports Filter Text and Batching.
        5. Returns partial results if interrupted/cancelled.
        6. Supports boundary-crossing search modes.

        Boundary Search Modes:
        - 'full': Regular search, track boundary matches for display
        - 'boundary': Only return results with boundary-crossing matches
        - 'combined': Full search with score boost for boundary matches

        Phase 110 (COMP-LOC-01/02): corpus_scope selects which index loop runs —
        'genizah' (Genizah lab loop only), 'local' (LOCAL LAB loop only), or 'all'
        (both, merged into results_map). Corpus is orthogonal to mode (Lab Mode is
        NOT hardwired to LOCAL).
        """
        from genizah_core import text_to_fingerprint, HEBREW_FREQ  # noqa: PLC0415 — lazy; GUARD-01 safe
        from genizah_core import get_boundary_stats, get_crossed_boundaries  # noqa: PLC0415 — lazy; GUARD-01 safe
        from genizah_core import calculate_boundary_quality, calculate_final_score_with_boost  # noqa: PLC0415 — lazy; GUARD-01 safe
        from shared.search_engine import _count_unique_chunks  # noqa: PLC0415 — lazy; GUARD-01 safe
        from shared.search_engine import _LabChunkPlan  # noqa: PLC0415 — lazy; GUARD-01 safe
        # Phase 110 C4: fail CLOSED — never expose LOCAL on a bad value.
        if corpus_scope not in ('genizah', 'local', 'all'):
            corpus_scope = 'genizah'
        _local_lab_stale = False  # Phase 110 Round-2 #4: A2 default so EVERY return path carries it

        if not full_text:
            return {'main': [], 'filtered': [], 'known': [], 'partial': False, 'boundary_stats': None,
                    'corpus_scope': corpus_scope, 'local_lab_stale': _local_lab_stale}

        # Strip combining diacritical marks and geresh/gershayim from queries
        full_text = strip_search_diacritics(full_text)
        if filter_text:
            filter_text = strip_search_diacritics(filter_text)

        # Reset debug counter for this search (prevents state leak between searches)
        self._filter_match_count = 0

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

        # (Part 1: Tokenization) - track positions for preserving formatting
        token_matches = list(re.finditer(r"[\w֐-׿\']+", full_text))
        tokens = [strip_nikud(m.group()) for m in token_matches]  # Strip nikud from tokens
        token_positions = [(m.start(), m.end()) for m in token_matches]  # Store positions
        c_size = chunk_size if chunk_size else 15
        step = max(1, int(c_size * 0.5))

        # Strip nikud from filter text for consistent matching
        if filter_text:
            filter_text = strip_nikud(filter_text)

        # Get boundary stats (includes parsed boundaries to avoid double parsing)
        boundary_stats = get_boundary_stats(full_text, boundary_delimiter, c_size, min_delimiter_distance)
        boundaries = boundary_stats.get('boundaries', [])

        # Build chunks - handle short texts first to avoid wasteful iteration
        chunks_data = []
        if len(tokens) < c_size:
            # Short text: single chunk with all tokens
            crossed_bounds = get_crossed_boundaries(0, len(tokens), boundaries)
            chunks_data = [(0, tokens, crossed_bounds)]
        else:
            # Normal text: create overlapping chunks
            for i in range(0, max(1, len(tokens) - c_size + 1), step):
                chunk_end = i + c_size
                crossed_bounds = get_crossed_boundaries(i, chunk_end, boundaries)
                chunks_data.append((i, tokens[i : i + c_size], crossed_bounds))

        total_chunks = len(chunks_data)
        results_map = {}
        was_interrupted = False
        chunks_processed = 0

        # SEED-011 (125a): Build per-chunk lab plans ONCE before both LAB loops.
        # fp_str / fp_list / needed_unique_fps / core_query are genuinely index-
        # independent — identical for the Genizah-LAB and LOCAL-LAB passes.
        # text_to_fingerprint is called ONCE per qualifying chunk (not 2x).
        # Chunks that fail the weak-phrase test or the len<4 test become None
        # sentinels that both loops skip.  final_query_str is NOT stored on the
        # plan because the Genizah-LAB loop adds a source boost while the
        # LOCAL-LAB loop uses core_query directly.
        #
        # Codex Gate-2 fix: gate the prep on whether EITHER lab loop will actually
        # run.  In base, text_to_fingerprint / _is_phrase_statistically_weak ran
        # INSIDE each loop, so an unbuilt or scoped-out LAB index never paid for
        # the (costly) fingerprinting.  Without this guard a no-LAB-index run (the
        # common case — Lab Mode is opt-in) would fingerprint every chunk for
        # nothing.  (`is_searchable`/freshness are advisory and computed later, so
        # they are intentionally not part of this gate — the dominant waste is the
        # absent-index case, which the index-presence checks here fully cover.)
        _do_genizah_lab_pp = (corpus_scope != 'local'
                              and self.lab_index is not None
                              and self.lab_searcher is not None)
        _do_local_lab_pp = (corpus_scope != 'genizah'
                            and getattr(self, 'local_lab_searcher', None) is not None
                            and getattr(self, '_local_lab_index', None) is not None)
        lab_chunk_plans = []
        if _do_genizah_lab_pp or _do_local_lab_pp:
            for (lcp_token_start, lcp_chunk_tokens, lcp_chunk_crossed) in chunks_data:
                lcp_chunk_text = " ".join(lcp_chunk_tokens)
                if self._is_phrase_statistically_weak(lcp_chunk_text):
                    lab_chunk_plans.append(None)
                    continue
                lcp_fp_str = text_to_fingerprint(lcp_chunk_text, freq_map=target_map)
                if not lcp_fp_str or len(lcp_chunk_tokens) < 4:
                    lab_chunk_plans.append(None)
                    continue
                lcp_fp_list = lcp_fp_str.split()
                lcp_needed_unique_fps = set(lcp_fp_list)
                lcp_clauses = [f'{target_field}:{t}' for t in lcp_fp_str.split()]
                lcp_core_query = " OR ".join(lcp_clauses)
                lab_chunk_plans.append(_LabChunkPlan(
                    token_start_idx=lcp_token_start,
                    chunk_tokens=lcp_chunk_tokens,
                    chunk_text=lcp_chunk_text,
                    chunk_crossed_bounds=lcp_chunk_crossed,
                    fp_str=lcp_fp_str,
                    fp_list=lcp_fp_list,
                    needed_unique_fps=lcp_needed_unique_fps,
                    core_query=lcp_core_query,
                ))

        # (Part 2: Scanning) - wrapped in try/except to support partial results on cancel
        try:
          # Phase 110: gate the Genizah lab loop — skipped on a LOCAL-only run.
          # Phase 110 UAT (Issue 3): guard against an UNBUILT Genizah fingerprint
          # LAB index. When Config.LAB_INDEX_DIR has never been built,
          # self.lab_index / self.lab_searcher are None and a Lab-Mode + Genizah
          # run (chunk_size>3) crashed with
          # "'NoneType' object has no attribute 'parse_query'". Mirror the LOCAL
          # LAB loop's existing None-guard below: skip the Genizah lab contribution
          # gracefully (no Genizah-lab hits) — do NOT crash, do NOT build anything.
          if corpus_scope != 'local' and (self.lab_index is None or self.lab_searcher is None):
            LAB_LOGGER.info(
                "lab_composition_search: Genizah LAB index not built — skipping Genizah lab loop"
            )
          if corpus_scope != 'local' and self.lab_index is not None and self.lab_searcher is not None:
            for i, plan in enumerate(lab_chunk_plans):
                chunks_processed = i
                if progress_callback: progress_callback(i, total_chunks)
                if plan is None: continue  # statistically-weak or too short — pre-pass skipped it

                # Consume pre-built plan fields (SEED-011)
                token_start_idx = plan.token_start_idx
                chunk_tokens = plan.chunk_tokens
                chunk_text = plan.chunk_text
                chunk_crossed_bounds = plan.chunk_crossed_bounds
                fp_str = plan.fp_str
                fp_list = plan.fp_list
                needed_unique_fps = plan.needed_unique_fps
                core_query = plan.core_query

                # Query with Boost (Genizah-LAB only — index-local source boost)
                final_query_str = f'({core_query}) AND (source:"V0.8"^10 OR source:"V0.7")'

                q_obj = None
                try:
                    q_obj = self.lab_index.parse_query(final_query_str)
                except (ValueError, RuntimeError):
                    try:
                        q_obj = self.lab_index.parse_query(core_query)
                    except (ValueError, RuntimeError): continue

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
                    except Exception as e:
                        LOGGER.debug('Batched search query failed, falling back to empty: %s', e)
                        iterator = []

                for score, doc_addr in iterator:
                    try:
                        doc = self.lab_searcher.doc(doc_addr)
                        content = doc['content'][0]
                        uid = doc['unique_id'][0]

                        # --- Filter Text Logic ---
                        # Check if the search chunk's words appear in sequence in the filter text
                        is_filtered_match = False
                        if filter_text and len(chunk_tokens) >= 3:
                            # Normalize: keep only Hebrew letters, join with single space
                            clean_chunk = ' '.join(re.findall(r'[א-ת]+', chunk_text))
                            if clean_chunk and clean_chunk in filter_text:
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
                                'is_text_filtered': False,
                                # Boundary tracking - use set to count each boundary only once
                                'boundary_chunk_scores': [],
                                'crossed_boundaries': set(),
                                # Phase 77 D-13: per-chunk attribution for parallels JSON
                                # matches[]. Tuples are (chunk_index_0_based, source_chunk_text,
                                # match_score, manuscript_snippet). Used by
                                # shared/search_serializer.serialize_parallels_payload.
                                'chunk_hits': [],
                            }
                        rec = results_map[uid]

                        if is_filtered_match:
                            rec['is_text_filtered'] = True

                        # Track boundary-crossing matches - each boundary counted once
                        if chunk_crossed_bounds:
                            rec['boundary_chunk_scores'].append(match_score)
                            rec['crossed_boundaries'].update(chunk_crossed_bounds)

                        rec['total_score'] += match_score
                        rec['hits_count'] += 1
                        token_end_idx = token_start_idx + len(chunk_tokens)
                        rec['src_indices'].update(range(token_start_idx, token_end_idx))
                        start_m, end_m = best_window
                        if matches:
                            rec['ms_matches'].append((matches[start_m]['start'], matches[end_m]['end']))
                            for m in matches[start_m : end_m + 1]: rec['all_found_words'].add(m['word'])
                            # Phase 77 D-13: per-chunk attribution for parallels matches[].
                            # i is the 0-based chunk index from the outer enumerate(chunks_data) loop.
                            # The manuscript snippet is the same substring used for ms_matches.
                            ms_snip = content[matches[start_m]['start']:matches[end_m]['end']]
                            # Dedup: same (chunk_index, ms_snip) can arise from
                            # multiple Tantivy segments returning the same uid.
                            # Keep the highest-scoring entry per key.
                            _seen = rec.setdefault('_chunk_hit_keys', {})
                            _key = (i, ms_snip)
                            _existing_idx = _seen.get(_key)
                            if _existing_idx is None:
                                _seen[_key] = len(rec['chunk_hits'])
                                rec['chunk_hits'].append((i, chunk_text, match_score, ms_snip))
                            elif match_score > rec['chunk_hits'][_existing_idx][2]:
                                rec['chunk_hits'][_existing_idx] = (i, chunk_text, match_score, ms_snip)
                    except (KeyError, IndexError, TypeError) as _dedup_exc:
                        logging.getLogger(__name__).debug(
                            "lab_composition_search: skipped chunk-hit dedup entry: %r", _dedup_exc
                        )
        except InterruptedError:
            was_interrupted = True

        # Phase 95 D-09: LOCAL LAB extension — query LOCAL LAB side-index with same
        # fingerprint scoring path (NOT RRF, NOT BM25 — custom scoring per D-09).
        # Results merged into results_map so Part 3 handles them uniformly.
        # Guard: _check_local_lab_freshness is defined on SearchEngine (not LabEngine).
        _freshness_fn = getattr(self, "_check_local_lab_freshness", None)
        # Phase 97 R-01: skip LOCAL LAB search if is_searchable gate is closed.
        _lab_tab = self._my_library_tab_ref() if getattr(self, "_my_library_tab_ref", None) is not None else None
        _lab_is_searchable = getattr(_lab_tab, "is_searchable", True) if _lab_tab is not None else True
        # Phase 110: compute freshness ONCE (preserve D-37 try/except — never raise on
        # the worker thread; never trigger a rebuild here).
        _lab_fresh_lab = False
        if callable(_freshness_fn):
            try:
                _lab_fresh_lab = bool(_freshness_fn())
            except Exception as _lab_fresh_exc:
                LOGGER.warning(
                    "lab_composition_search: _check_local_lab_freshness raised %r — "
                    "skipping LOCAL LAB extension (D-37 fallback).",
                    _lab_fresh_exc,
                )
                _lab_fresh_lab = False
        # Phase 110 A2 + M2: per-run stale verdict — stale ONLY when an index is present
        # but not fresh (stale != no-index). Set the back-compat engine flag only then.
        _local_index_present_lab = getattr(self, 'local_lab_searcher', None) is not None
        _local_lab_stale = bool(
            corpus_scope in ('local', 'all') and _local_index_present_lab and not _lab_fresh_lab
        )
        if _local_lab_stale:
            self.local_lab_searcher_stale = True
            LAB_LOGGER.info(
                "lab_composition_search: LOCAL LAB weights-hash reports stale, but "
                "searching the present index anyway (freshness is advisory, not a hard "
                "gate — the static fingerprint field is weights-independent; an empty "
                "result is worse than a slightly-stale one)."
            )
        # Phase 110 (UAT fix): gate the LOCAL LAB loop on corpus_scope + the index being
        # PRESENT — NOT on `_lab_fresh_lab`. The weights-hash freshness check is perpetually
        # false-stale when the LabEngine's dynamic_rank_map differs build-vs-search (see
        # rebuild_local_lab_index docstring), which silently suppressed ALL Lab+LOCAL results.
        # Freshness stays advisory (per-run `local_lab_stale` payload + the log above); the
        # inner None-guard below keeps it crash-safe.
        if (not was_interrupted and corpus_scope != 'genizah' and _lab_is_searchable
                and getattr(self, 'local_lab_searcher', None) is not None
                and getattr(self, '_local_lab_index', None) is not None):
            try:
                local_lab_index = getattr(self, "_local_lab_index", None)
                local_lab_searcher = self.local_lab_searcher
                if local_lab_index is not None and local_lab_searcher is not None:
                    # SEED-011 (125a): consume pre-built lab_chunk_plans — fingerprint
                    # prep is index-independent and was already computed above.
                    for _i, _plan in enumerate(lab_chunk_plans):
                        if _plan is None:
                            continue  # pre-pass marked this chunk as weak/too-short
                        _token_start_idx = _plan.token_start_idx
                        _chunk_tokens = _plan.chunk_tokens
                        _chunk_text = _plan.chunk_text
                        _chunk_crossed_bounds = _plan.chunk_crossed_bounds
                        _fp_str = _plan.fp_str
                        _fp_list = _plan.fp_list
                        _needed_unique_fps = _plan.needed_unique_fps
                        _core_query = _plan.core_query
                        try:
                            _q_obj = local_lab_index.parse_query(_core_query)
                        except (ValueError, RuntimeError):
                            continue
                        if not _q_obj:
                            continue
                        try:
                            _res = local_lab_searcher.search(_q_obj, 5000)
                            _local_iter = _res.hits
                        except Exception:
                            continue
                        for _score, _doc_addr in _local_iter:
                            try:
                                _doc = local_lab_searcher.doc(_doc_addr)
                                _content = _doc['content'][0]
                                _uid = _doc['unique_id'][0]
                                if filter_text and len(_chunk_tokens) >= 3:
                                    _clean_chunk = ' '.join(re.findall(r'[א-ת]+', _chunk_text))
                                    if _clean_chunk and _clean_chunk in filter_text:
                                        pass  # LOCAL LAB hits not filter-excluded by source_text
                                _match_score, _matches, _best_window = self._calculate_match_metrics(
                                    _content, _fp_list, _chunk_text, freq_map=target_map
                                )
                                _found_unique_fps = set(
                                    m['fp'] for m in _matches[_best_window[0]:_best_window[1] + 1]
                                )
                                _common_fps = _found_unique_fps.intersection(_needed_unique_fps)
                                if len(_needed_unique_fps) > 0:
                                    if (len(_common_fps) / len(_needed_unique_fps)) < min_pct_ratio:
                                        continue
                                if _match_score < MIN_SCORE_THRESHOLD:
                                    continue
                                if _uid not in results_map:
                                    results_map[_uid] = {
                                        'uid': _uid, 'total_score': 0, 'hits_count': 0,
                                        'raw_header': _doc['full_header'][0],
                                        'source': _doc['source'][0],
                                        'content': _content, 'best_chunk_score': -1,
                                        'all_found_words': set(), 'src_indices': set(),
                                        'ms_matches': [], 'is_text_filtered': False,
                                        'boundary_chunk_scores': [],
                                        'crossed_boundaries': set(),
                                        'chunk_hits': [],
                                    }
                                _rec = results_map[_uid]
                                if _chunk_crossed_bounds:
                                    _rec['boundary_chunk_scores'].append(_match_score)
                                    _rec['crossed_boundaries'].update(_chunk_crossed_bounds)
                                _rec['total_score'] += _match_score
                                _rec['hits_count'] += 1
                                _token_end_idx = _token_start_idx + len(_chunk_tokens)
                                _rec['src_indices'].update(range(_token_start_idx, _token_end_idx))
                                _start_m, _end_m = _best_window
                                if _matches:
                                    _rec['ms_matches'].append(
                                        (_matches[_start_m]['start'], _matches[_end_m]['end'])
                                    )
                                    for _m in _matches[_start_m:_end_m + 1]:
                                        _rec['all_found_words'].add(_m['word'])
                                    _ms_snip = _content[
                                        _matches[_start_m]['start']:_matches[_end_m]['end']
                                    ]
                                    _seen_llb = _rec.setdefault('_chunk_hit_keys', {})
                                    _key_llb = (_i, _ms_snip)
                                    _existing_llb = _seen_llb.get(_key_llb)
                                    if _existing_llb is None:
                                        _seen_llb[_key_llb] = len(_rec['chunk_hits'])
                                        _rec['chunk_hits'].append(
                                            (_i, _chunk_text, _match_score, _ms_snip)
                                        )
                                    elif _match_score > _rec['chunk_hits'][_existing_llb][2]:
                                        _rec['chunk_hits'][_existing_llb] = (
                                            _i, _chunk_text, _match_score, _ms_snip
                                        )
                            except (KeyError, IndexError, TypeError) as _dedup_llb_exc:
                                logging.getLogger(__name__).debug(
                                    "lab_composition_search: skipped LOCAL-LAB chunk-hit dedup entry: %r",
                                    _dedup_llb_exc,
                                )
            except Exception as _local_lab_exc:
                logging.getLogger(__name__).warning(
                    "lab_composition_search: LOCAL LAB scan failed: %r", _local_lab_exc,
                    exc_info=True,
                )

        # (Part 3: Result Processing) - runs even if interrupted to return partial results
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

                    # Get character positions from token_positions - preserve original formatting
                    char_start = token_positions[start_ctx][0]
                    char_end = token_positions[end_ctx - 1][1]
                    original_snippet = full_text[char_start:char_end]

                    # Build highlights for matched words
                    highlights = []
                    for k in range(start_ctx, end_ctx):
                        if k in cl_set:
                            word_char_start = token_positions[k][0] - char_start
                            word_char_end = token_positions[k][1] - char_start
                            highlights.append((word_char_start, word_char_end))

                    # Apply highlights in reverse order to preserve positions
                    result = original_snippet
                    for word_start, word_end in reversed(highlights):
                        result = result[:word_start] + '*' + result[word_start:word_end] + '*' + result[word_end:]

                    src_snippets.append(f"... {result} ...")

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

            # Calculate boundary match quality and final score with boost
            base_score = data['total_score']
            boundary_chunk_scores = data.get('boundary_chunk_scores', [])
            has_boundary_matches = len(boundary_chunk_scores) > 0
            boundary_quality = calculate_boundary_quality(boundary_chunk_scores)

            # Apply score boost in combined mode
            if boundary_mode == 'combined' and has_boundary_matches:
                final_score = calculate_final_score_with_boost(
                    base_score, boundary_quality, has_boundary_matches, boundary_boost
                )
            else:
                final_score = base_score

            # Calculate normalized boundary quality (0-1 range)
            boundary_quality_normalized = 0.0
            if has_boundary_matches and base_score > 0:
                boundary_quality_normalized = min(boundary_quality / base_score, 1.0)

            item = {
                'score': base_score,
                'final_score': final_score,
                'uid': uid,
                'raw_header': data['raw_header'],
                'src_lbl': data['source'],
                'source_ctx': "\n\n".join(src_snippets),
                'text': "\n...\n".join(ms_snips),
                'highlight_pattern': hl_pattern,
                'full_text': data['content'],
                'is_text_filtered': data.get('is_text_filtered', False),
                'filter_reason': 'source_text' if data.get('is_text_filtered', False) else '',
                # Boundary metadata
                'has_boundary_matches': has_boundary_matches,
                'boundary_match_count': len(data.get('crossed_boundaries', set())),
                'boundary_quality': boundary_quality_normalized,
                # Phase 77 D-13: surface per-chunk attribution to consumers
                # (serialize_parallels_payload, /api/parallels). Each tuple is
                # (chunk_index_0_based, source_chunk_text, match_score,
                # manuscript_snippet). May be empty if no chunks matched
                # (defensive default for forward compatibility).
                'chunk_hits': data.get('chunk_hits', []),
                # User-facing chunk_count: unique source-chunk contents. The
                # internal `hits_count` counter (line 1448) drives the noise
                # gate at line 1480 and is left alone. The full-mode
                # min_boundary_matches filter below reads this derived field
                # so repeated source phrases don't inflate the result.
                'chunk_count': _count_unique_chunks(data.get('chunk_hits', [])),
            }
            raw_final_items.append(item)

        # --- Sorting & Splitting Logic ---
        # In combined mode, sort by final_score; otherwise by base score
        if boundary_mode == 'combined':
            raw_final_items.sort(key=lambda x: x.get('final_score', x['score']), reverse=True)
        else:
            raw_final_items.sort(key=lambda x: x['score'], reverse=True)

        main_list = []
        known_list = []
        filtered_list = []

        for item in raw_final_items:
            # In boundary-only mode, skip items without boundary matches
            if boundary_mode == 'boundary' and not item.get('has_boundary_matches', False):
                continue

            # Apply min_boundary_matches filter
            if min_boundary_matches > 0:
                if boundary_mode == 'full':
                    # Use derived chunk_count (unique source chunks), not
                    # the internal hits_count which was never surfaced on
                    # the item dict (latent always-zero bug pre-fix).
                    if item.get('chunk_count', 0) < min_boundary_matches:
                        continue
                else:
                    if item.get('boundary_match_count', 0) < min_boundary_matches:
                        continue

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
        return {
            'main': main_list,
            'known': known_list,
            'filtered': filtered_list,
            'partial': was_interrupted,
            'boundary_stats': boundary_stats,
            # Phase 110 A2 + Round-2 #4: per-run scope + staleness verdict.
            'corpus_scope': corpus_scope,
            'local_lab_stale': _local_lab_stale,
        }

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
        from genizah_core import encode_word_shmidman, HEBREW_FREQ  # noqa: PLC0415 — lazy; GUARD-01 safe
        # Clean punctuation and split to words
        words = re.findall(r"[\w֐-׿]+", phrase_text)
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
