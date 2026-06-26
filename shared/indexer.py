# -*- coding: utf-8 -*-
"""Tantivy index construction and browse-map assembly.

Phase 124: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import Indexer`` callers continue working.
"""

import logging
import os
import re
import shutil
import json
import pickle
from collections import defaultdict

try:
    import tantivy
except ImportError:
    # GUARD-02 (zero behavior change): at base, genizah_core guarded its
    # `import tantivy` and raised this friendly message. The Phase 124 facade
    # shim `from shared.indexer import Indexer` now executes BEFORE genizah_core's
    # own guard, so this module must raise the identical ImportError — otherwise a
    # missing-tantivy install surfaces a raw ModuleNotFoundError. Plain (untranslated)
    # to match the first guard that fired at base genizah_core.py.
    raise ImportError("Tantivy library missing. Please install it.")

from genizah_translations import TRANSLATIONS

from shared.config import Config
from shared.search_tokenizer import register_search_tokenizers
from shared.text_normalize import strip_search_diacritics
from shared.browse_map_utils import dedupe_browse_map

LOGGER = logging.getLogger("genizah." + __name__)


def _tr(text: str) -> str:
    """Translate text if current language is Hebrew.

    Mirrors genizah_core.tr() — lazy import of CURRENT_LANG inside the
    function body so we always see the live value (Pitfall 2 of Phase 123).
    GUARD-01-safe: the import is function-body-only, not module-level.
    """
    from genizah_core import CURRENT_LANG  # noqa: PLC0415 — intentional lazy; GUARD-01 safe
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text


def _strip_brackets(text: str) -> str:
    """Remove all square brackets from *text*. Mirrors genizah_core._strip_brackets."""
    return text.replace('[', '').replace(']', '')


class Indexer:
    """Create or update the Tantivy index and keep browse maps in sync."""
    def __init__(self, meta_mgr):
        self.meta_mgr = meta_mgr

    @staticmethod
    def _extract_position_fields(content, head_words=10):
        """Extract position-search fields from content text.

        Returns dict with content_head, content_tail, line_starts, line_ends.
        line_starts/line_ends include both plain tokens (for "any line" search)
        and L{n}:word positional tokens (for per-line search like L3:שלום).
        """
        words = content.split()
        head = " ".join(words[:head_words]) if words else ""
        tail = " ".join(words[-head_words:]) if words else ""
        lines = [ln.strip() for ln in content.split("\n") if ln.strip()]
        ls_parts = []
        le_parts = []
        for i, ln in enumerate(lines, 1):
            ln_words = ln.split()
            if ln_words:
                ls_parts.append(ln_words[0])
                ls_parts.append(f"L{i}:{ln_words[0]}")
                le_parts.append(ln_words[-1])
                le_parts.append(f"L{i}:{ln_words[-1]}")
        return {
            'content_head': head,
            'content_tail': tail,
            'line_starts': " ".join(ls_parts),
            'line_ends': " ".join(le_parts),
        }

    @staticmethod
    def _validate_position_match(content, match_obj, text_position, line_constraints=None, strip_brackets=True):
        """Post-filter: validate that a regex match occurs at the expected text position.

        Tantivy uses broad fields (e.g. first 10 words) for speed; this validates
        the exact position (e.g. match is literally the first/last words).

        line_constraints: {line_num: word} for per-line validation (L3:שלום syntax).
        strip_brackets: when True (bracket-free query), ignore brackets in
            prefix/suffix.  When False (bracket-containing query), enforce
            exact position against un-stripped content.
        """
        if not text_position:
            return True

        def _clean(s):
            return _strip_brackets(s).strip() if strip_brackets else s.strip()

        # Per-line constraints: validate each L{n}:word against specific lines
        if line_constraints:
            lines = [ln.strip() for ln in content.split('\n') if ln.strip()]
            for line_num, word in line_constraints.items():
                if line_num < 1 or line_num > len(lines):
                    return False
                ln_words = lines[line_num - 1].split()
                if not ln_words:
                    return False
                if text_position == 'line_start':
                    if ln_words[0] != word:
                        return False
                elif text_position == 'line_end':
                    if ln_words[-1] != word:
                        return False
            return True

        start, end = match_obj.start(), match_obj.end()
        if text_position == 'start':
            return not _clean(content[:start])
        elif text_position == 'end':
            return not _clean(content[end:])
        elif text_position == 'line_start':
            before = content[:start]
            last_nl = before.rfind('\n')
            line_prefix = before[last_nl + 1:] if last_nl >= 0 else before
            return not _clean(line_prefix)
        elif text_position == 'line_end':
            after = content[end:]
            next_nl = after.find('\n')
            line_suffix = after[:next_nl] if next_nl >= 0 else after
            return not _clean(line_suffix)
        return True

    @staticmethod
    def _validate_line_break_match(content, line_groups, line_gaps, expanded_groups):
        """Post-filter: validate that a document satisfies line-break constraints.

        Args:
            content: Full document text
            line_groups: List[LineGroup] with positional constraints
            line_gaps: List[Optional[int]] — None=consecutive, int=skip N lines
            expanded_groups: List[List[set]] — for each group, for each component,
                            the set of expanded words (lowercase) that match

        Returns:
            list of (line_index, line_text) tuples for matched lines, or None if no match.
        """
        lines = [ln.strip() for ln in content.split('\n') if ln.strip()]
        if not lines:
            return None

        # For each group, find all line indices where ALL components match
        group_candidates = []
        for gi, group in enumerate(line_groups):
            exp_words = expanded_groups[gi]  # list of sets, one per component
            matching_lines = []
            for li, line in enumerate(lines):
                line_words = line.split()
                if not line_words:
                    continue

                # Check positional constraints
                all_match = True
                for ci, word_set in enumerate(exp_words):
                    if not word_set:
                        all_match = False
                        break
                    if ci == 0 and group.line_start:
                        # First component must match first word of line
                        if line_words[0].lower() not in word_set:
                            all_match = False
                            break
                    elif ci == len(exp_words) - 1 and group.line_end:
                        # Last component must match last word of line
                        if line_words[-1].lower() not in word_set:
                            all_match = False
                            break

                    # Word must appear somewhere on the line (for unconstrained position)
                    if ci == 0 and group.line_start:
                        continue  # Already checked above
                    if ci == len(exp_words) - 1 and group.line_end:
                        continue  # Already checked above

                    # Check if any expanded word appears on this line
                    line_lower = [w.lower() for w in line_words]
                    if not any(w in word_set for w in line_lower):
                        all_match = False
                        break

                if all_match:
                    matching_lines.append(li)

            if not matching_lines:
                return None  # This group has no matching line → fail
            group_candidates.append(matching_lines)

        # Find valid assignment: ordered line indices with gap constraints
        # Use recursive backtracking (groups are typically 2-4, so this is fast)
        def _find_assignment(gi, prev_line):
            if gi >= len(group_candidates):
                return []  # All groups assigned
            gap = line_gaps[gi - 1] if gi > 0 and gi - 1 < len(line_gaps) else None
            for li in group_candidates[gi]:
                if gi == 0:
                    # First group can be any line
                    result = _find_assignment(gi + 1, li)
                    if result is not None:
                        return [(li, lines[li])] + result
                else:
                    # Check ordering and gap constraint
                    expected_distance = (gap if gap is not None else 0) + 1
                    actual_distance = li - prev_line
                    if actual_distance == expected_distance:
                        result = _find_assignment(gi + 1, li)
                        if result is not None:
                            return [(li, lines[li])] + result
            return None

        return _find_assignment(0, -1)

    def create_index(self, progress_callback=None):
        # Validation
        if not os.path.exists(Config.FILE_V8):
            raise FileNotFoundError(_tr("Input file not found: {}\nPlease place 'Transcriptions.txt' next to the executable.").format(Config.FILE_V8))

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
        # SEED-006 Stage 1: hebword tokenizer makes punctuation-attached words
        # (בסגן, -> בסגן) retrievable; stored value stays original (display intact).
        # content_head/tail + line_starts/line_ends KEEP whitespace — their
        # L{n}:word colon markers would be shattered by hebword.
        builder.add_text_field("content", stored=True, tokenizer_name="hebword")
        builder.add_text_field("content_head", stored=False, tokenizer_name="whitespace")
        builder.add_text_field("content_tail", stored=False, tokenizer_name="whitespace")
        builder.add_text_field("line_starts", stored=False, tokenizer_name="whitespace")
        builder.add_text_field("line_ends", stored=False, tokenizer_name="whitespace")
        # SEED-006 Stage 2: additive, non-stored, diacritic-folded retrieval field
        # (= strip_search_diacritics(content)) so צמאן / צ'מאן find the corpus
        # form צ̇מאן (U+0307). Lower-weighted OR fallback only; display reads `content`.
        builder.add_text_field("content_search", stored=False, tokenizer_name="hebword")
        builder.add_text_field("source", stored=True)
        builder.add_text_field("full_header", stored=True)
        builder.add_text_field("shelfmark", stored=True)
        builder.add_text_field("scope", stored=True)
        builder.add_text_field("boundaries", stored=True)
        schema = builder.build()

        index = tantivy.Index(schema, path=db_path)
        # SEED-006: register hebword (+ builtins) before the writer tokenizes content.
        register_search_tokenizers(index)
        writer = index.writer(heap_size=500_000_000)

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
                            page_content = "\n".join(ctext)
                            pos = self._extract_position_fields(page_content)
                            shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or self.meta_mgr.meta_map.get(cid, "")
                            writer.add_document(tantivy.Document(
                                unique_id=str(cid), content=page_content, source=str(label),
                                content_search=strip_search_diacritics(page_content),  # SEED-006 Stage 2
                                full_header=str(chead), shelfmark=str(shelfmark),
                                scope="page", boundaries="",
                                content_head=pos['content_head'], content_tail=pos['content_tail'],
                                line_starts=pos['line_starts'], line_ends=pos['line_ends'],
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
                    page_content = " ".join(ctext)
                    pos = self._extract_position_fields(page_content)
                    shelfmark = self.meta_mgr.get_shelfmark_from_header(chead) or self.meta_mgr.meta_map.get(cid, "")
                    writer.add_document(tantivy.Document(
                        unique_id=str(cid), content=page_content, source=str(label),
                        content_search=strip_search_diacritics(page_content),  # SEED-006 Stage 2
                        full_header=str(chead), shelfmark=str(shelfmark),
                        scope="page", boundaries="",
                        content_head=pos['content_head'], content_tail=pos['content_tail'],
                        line_starts=pos['line_starts'], line_ends=pos['line_ends'],
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

        pos = self._extract_position_fields(content)
        writer.add_document(tantivy.Document(
            unique_id=str(unique_id),
            content=str(content),
            content_search=strip_search_diacritics(str(content)),  # SEED-006 Stage 2
            source=str(first_source),
            full_header=str(first_header),
            shelfmark=str(shelfmark),
            scope=str(scope),
            boundaries=json.dumps(boundaries, ensure_ascii=False),
            content_head=pos['content_head'], content_tail=pos['content_tail'],
            line_starts=pos['line_starts'], line_ends=pos['line_ends'],
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
