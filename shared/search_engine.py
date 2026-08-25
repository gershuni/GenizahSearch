# -*- coding: utf-8 -*-
"""Full-text search execution, browse utilities, and LOCAL index management.

Phase 125: Extracted from genizah_core.py (v8.3.0 God-File Decomposition).
genizah_core.py retains a permanent same-object re-export shim so all
existing ``from genizah_core import SearchEngine`` callers continue working.
"""

import logging
import os
import re
import threading
import time
import weakref
import html
import json
import pickle
from collections import defaultdict
from dataclasses import dataclass
from typing import Optional

try:
    import tantivy
except ImportError:
    raise ImportError("Tantivy library missing. Please install it.")

from shared.config import Config
from shared.text_normalize import strip_nikud, strip_search_diacritics
from shared.browse_map_utils import natural_sort_key, dedupe_browse_map, _extract_ie_from_header
from shared.search_tokenizer import register_search_tokenizers
from shared.indexer import Indexer
from shared.responsa import (
    expand_grammatical_prefixes, expand_grammatical_suffixes,
    expand_judeo_arabic, expand_plene_defective,
    parse_responsa_query, _parse_line_break_query,
    ResponsaComponent, _apply_explosion_guard,  # noqa: F401
    extract_per_pair_gaps, _expand_inline_alternation,
)
from shared.responsa import _SOFIT_TO_NORMAL

LOGGER = logging.getLogger("genizah." + __name__)

# LAB_LOGGER: this is the SAME named logger that configure_lab_logger() in
# genizah_core.py configured with file+console handlers and propagate=False.
# logging.getLogger returns the same instance by name -- NOT a new logger.
# Keep ALL LAB_LOGGER references in this file as LAB_LOGGER (never substitute LOGGER).
LAB_LOGGER = logging.getLogger("GenizahLab")


def _tr(text: str) -> str:
    """Translate text if current language is Hebrew.

    Mirrors genizah_core.tr() -- lazy import of CURRENT_LANG inside the
    function body so we always see the live value (Pitfall 2 of Phase 123).
    GUARD-01-safe: the import is function-body-only, not module-level.
    """
    from genizah_core import CURRENT_LANG  # noqa: PLC0415 -- intentional lazy; GUARD-01 safe
    from genizah_translations import TRANSLATIONS  # noqa: PLC0415 -- intentional lazy; GUARD-01 safe
    if CURRENT_LANG == 'he':
        return TRANSLATIONS.get(text, text)
    return text


# --- Phase 78 (Concern #6): thread-local cascade-downgrade signal ---
# When a Responsa query triggers MAX_EXPANDED_TERMS downgrade, the cascade
# code sets this AND ALSO attaches responsa_warning to deduped[0] (legacy
# path, preserved for callers that read result rows). The /api/search handler
# reads this thread-local instead so warnings survive empty result sets.
#
# Naming: prefixed with _LAST_ to make obvious it's a one-shot signal that the
# next consumer should consume (read-and-clear). Per-thread because the search
# engine may be invoked concurrently from FastAPI threads.
_LAST_RESPONSA_DOWNGRADE = threading.local()

# Phase 81A — structured per-flag cascade outcome carried alongside the
# legacy string message. Skill consumer (81B) reads this to populate
# responsa_options_effective in the /api/search envelope echo.
_LAST_RESPONSA_DOWNGRADE_META = threading.local()


def _set_last_responsa_downgrade(message: str) -> None:
    """Record a Responsa downgrade signal on the current thread.

    Phase 78 Concern #6: called from the cascade decision site so the
    /api/search handler can surface the warning even when results == [].
    """
    _LAST_RESPONSA_DOWNGRADE.value = message


def _consume_last_responsa_downgrade() -> Optional[str]:
    """Read-and-clear the per-thread downgrade signal. Returns None if unset.

    Phase 78 Concern #6: called once per /api/search invocation in the finally
    branch of the response builder. Read-and-clear semantics ensure the signal
    is attributed to exactly one request.
    """
    msg = getattr(_LAST_RESPONSA_DOWNGRADE, 'value', None)
    if msg is not None:
        # Clear by deleting the attribute so subsequent calls return None.
        try:
            del _LAST_RESPONSA_DOWNGRADE.value
        except AttributeError:
            pass
    return msg


def _set_last_responsa_downgrade_meta(meta: dict) -> None:
    """Phase 81A — record a structured per-flag cascade outcome.

    `meta` is a dict with the four ResponsaOptions field names as keys and
    booleans indicating whether each was applied (True) or cascade-disabled
    (False). The skill consumer compares this to the request's
    responsa_options to detect server-side downgrades.
    """
    _LAST_RESPONSA_DOWNGRADE_META.value = meta


def _consume_last_responsa_downgrade_meta() -> Optional[dict]:
    """Phase 81A — read-and-clear the structured cascade outcome.

    Returns None when no downgrade occurred OR when already consumed
    on the current thread.
    """
    meta = getattr(_LAST_RESPONSA_DOWNGRADE_META, 'value', None)
    if meta is not None:
        try:
            del _LAST_RESPONSA_DOWNGRADE_META.value
        except AttributeError:
            pass
    return meta



def _count_unique_chunks(chunk_hits):
    """Count distinct source-chunk contents from a chunk_hits list.

    Used by both search_composition_logic and lab_composition_search to derive
    the user-facing `chunk_count` (the basis for the `min chunks` filter).
    Counting by chunk_text dedupes (a) sliding-window indices with identical
    content from repeated source phrases and (b) cross-Tantivy-segment hits
    that already share (i, ms_snip) after the per-rec dedup step in each
    caller. Defensive against malformed inputs.
    """
    return len({
        hit[1]
        for hit in (chunk_hits or ())
        if isinstance(hit, (tuple, list)) and len(hit) > 1 and hit[1]
    })



@dataclass
class _ChunkPlan:
    """Per-chunk precomputed plan for search_composition_logic (SEED-011 dedup).

    Computed ONCE in a pre-pass over chunks_data before the index loops begin,
    then shared by both the Genizah loop and the LOCAL loop.

    The two query strings genuinely differ: the LOCAL pass applies diacritic
    folding (SEED-006 M1) so local_query_str / compiled_regex_local are built
    from the folded chunk while genizah_query_str / compiled_regex_genizah are
    built from the raw chunk.  build_tantivy_query / build_regex_pattern are
    each still called once per (chunk x flavor) — the dedup does NOT reduce that
    count; it prevents each loop from independently re-iterating chunks_data.
    """
    token_idx: int                      # start token index in the source
    chunk: list                         # raw (unfolded) chunk token list
    chunk_crossed_bounds: object        # boundary info (set or None)
    genizah_query_str: str              # build_tantivy_query(chunk, mode, content_search_field=_cs_field)
    compiled_regex_genizah: object      # build_regex_pattern(chunk, mode, 0) — re.Pattern or None
    local_query_str: str                # build_tantivy_query(folded_chunk, mode) — diacritic-folded
    compiled_regex_local: object        # build_regex_pattern(folded_chunk, mode, 0) — re.Pattern or None
    local_chunk_q: list                 # the folded chunk (for LOCAL _query_has_brackets check)


@dataclass
class _LabChunkPlan:
    """Per-chunk fingerprint plan for lab_composition_search (SEED-011 dedup).

    Computed ONCE per qualifying chunk before the two LAB loops (Genizah-LAB and
    LOCAL-LAB).  The fingerprint prep is genuinely index-independent: fp_str,
    fp_list, needed_unique_fps, and core_query are identical for both passes.

    final_query_str is NOT stored here because the Genizah-LAB loop adds a
    source boost (``AND (source:"V0.8"^10 OR source:"V0.7")``) while the
    LOCAL-LAB loop uses core_query directly.  Each loop builds final_query_str
    from plan.core_query.
    """
    token_start_idx: int                # start token index in the source
    chunk_tokens: list                  # raw chunk token list
    chunk_text: str                     # " ".join(chunk_tokens)
    chunk_crossed_bounds: object        # boundary info (set or None)
    fp_str: str                         # text_to_fingerprint(chunk_text)
    fp_list: list                       # fp_str.split()
    needed_unique_fps: set              # set(fp_list)
    core_query: str                     # " OR ".join([f'{target_field}:{t}' for t in fp_str.split()])



# ==============================================================================
#  RESPONSA REGEX HELPERS
# ==============================================================================

def _make_flex_spacing_pattern(term: str) -> str:
    """Create a flex-spacing regex pattern for a term.

    Inserts \\s* between each character of the term, allowing flexible
    whitespace between characters (handles OCR/HTR word boundary errors).

    Example: "abc" -> "a\\s*b\\s*c"
    """
    if not term:
        return term
    chars = [re.escape(ch) for ch in term]
    return r'\s*'.join(chars)


def _build_wildcard_regex(component: dict) -> str:
    """Build a regex pattern for a component with wildcard type.

    Returns the regex string (NOT compiled) for the wildcard pattern.
    """
    wildcard = component.get('wildcard')
    wildcard_pattern = component.get('wildcard_pattern')

    if wildcard == 'pattern' and wildcard_pattern:
        # Character pattern: *a*b*c* -> \S*a\S*b\S*c\S*
        # Split on '*', escape each part, join with \S*
        parts = wildcard_pattern.split('*')
        escaped_parts = [re.escape(p) for p in parts]
        return r'\S*'.join(escaped_parts)

    if wildcard == 'suffix':
        # Suffix wildcard: term\S*
        # For Hebrew suffix wildcards, the trailing letter may be a sofit (final-form)
        # letter. When followed by \S*, the sofit form will never match text where
        # the stem continues with more letters (Hebrew uses normal form mid-word).
        # Replace trailing sofit with a character class matching BOTH forms.
        # Example: שלום -> שלו[םמ]\S* (matches both standalone שלום and continuation שלומ-)
        regex_terms = component.get('regex_terms', [])
        if regex_terms:
            # Sort by length descending, escape
            sorted_terms = sorted(set(regex_terms), key=len, reverse=True)
            escaped = []
            for t in sorted_terms:
                if t and t[-1] in _SOFIT_TO_NORMAL:
                    # Replace trailing sofit with char class matching both forms
                    normal = _SOFIT_TO_NORMAL[t[-1]]
                    sofit = t[-1]
                    base = re.escape(t[:-1])
                    escaped.append(f'{base}[{sofit}{normal}]' + r'\S*')
                else:
                    escaped.append(re.escape(t) + r'\S*')
            return f"({'|'.join(escaped)})"
        return ''

    if wildcard == 'prefix':
        # Prefix wildcard: \S*term
        regex_terms = component.get('regex_terms', [])
        if regex_terms:
            sorted_terms = sorted(set(regex_terms), key=len, reverse=True)
            escaped = [r'\S*' + re.escape(t) for t in sorted_terms]
            return f"({'|'.join(escaped)})"
        return ''

    return ''


def _add_bracket_variants(term: str) -> list:
    """Return bracket-adorned variants of *term* for Tantivy OR expansion.

    The hebword tokenizer keeps bracketed words as single tokens
    (e.g., ``]הנתשנ``), so a bare query needs the plausible bracket positions
    so Tantivy returns them as candidates.

    SEED-006 (invariants 1 & 2): only expand a term that has NO bracket of its
    own. A query that already contains ``[`` / ``]`` is an *exact* bracket
    search (``[סגן`` must return only ``[סגן``), so it is returned unchanged
    — never broadened to the bare/other-bracket forms. A bare ``סגן`` still
    expands to ``[סגן`` etc. so it reaches bracketed tokens on pages that
    contain it.
    """
    variants = [term]
    if not term:
        return variants
    if '[' in term or ']' in term:
        return variants  # exact bracket query — do not expand
    for v in (f'[{term}', f'{term}]', f'[{term}]', f']{term}', f'{term}['):
        if v not in variants:
            variants.append(v)
    return variants


def _query_has_brackets(query_str: str) -> bool:
    """Return True if *query_str* contains literal square brackets.

    Responsa gap operators like ``[3]`` and ``[|2]`` are NOT literal
    bracket searches — strip them before checking.
    """
    stripped = re.sub(r'\[\|?\d+\]', '', query_str)
    return '[' in stripped or ']' in stripped


def _strip_brackets(text: str) -> str:
    """Remove all square brackets from *text*."""
    return text.replace('[', '').replace(']', '')


def _index_has_field(index, field_name: str) -> bool:
    """SEED-006 compat gate: True if *index*'s schema defines *field_name*.

    tantivy-py 0.25 ``Schema`` exposes no field introspection, so we probe via
    ``parse_query`` — querying a missing field raises ``ValueError`` mentioning
    'does not exist' / 'not defined in the schema'. Any other outcome (parse OK,
    or an unrelated error such as an unregistered tokenizer) is treated as
    "field present" so we never disable a working field by accident. Register
    tokenizers BEFORE calling this so a hebword field does not look absent.
    """
    if index is None:
        return False
    try:
        index.parse_query(f'{field_name}:probe', [field_name])
        return True
    except ValueError as exc:
        msg = str(exc).lower()
        if 'does not exist' in msg or 'not defined' in msg:
            return False
        return True
    except Exception:
        return True


def content_search_staleness_messages(genizah_present: bool,
                                      local_present):
    """SEED-019 #28: human-readable staleness diagnostics for the SEED-006
    ``content_search`` compat gate.

    The compat gate (:func:`_index_has_field`) lets search keep working against an
    index built before the ``content_search`` field existed — but it does so by
    *silently* degrading Hebrew punctuation/diacritic retrieval to the old
    whitespace-tokenized ``content`` field. This helper turns that silent state
    into one actionable remediation string per degraded index so the condition is
    visible (logged at open + queryable via
    :meth:`SearchEngine.index_staleness_report`).

    Args:
        genizah_present: the main GENIZAH index defines ``content_search``.
        local_present: the LOCAL side-index defines it, or ``None`` when no LOCAL
            index is open (so it contributes no message).

    Returns:
        list[str]: one message per degraded index; empty when nothing is stale.
    """
    messages = []
    if not genizah_present:
        messages.append(
            "GENIZAH index predates the content_search field (SEED-006): Hebrew "
            "punctuation/diacritic retrieval is degraded to whitespace-tokenized "
            "content-only. Rebuild the index via create_index (web: rebuild + "
            "redeploy; desktop: Settings -> Build / Rebuild Index)."
        )
    if local_present is False:
        messages.append(
            "LOCAL (My Library) index predates the content_search field "
            "(SEED-006): Hebrew punctuation/diacritic retrieval is degraded for "
            "local documents. Re-index via My Library -> Re-index All."
        )
    return messages


# Inserted between regex tokens to allow optional combining marks and apostrophe/quote variants.
# SEED-006: U+0022 (ASCII double quote) is included so the second-phase regex filter stays
# symmetric with strip_search_diacritics / COMBINING_DIACRITICALS_PATTERN (which fold U+0022).
# Without it, a clean query retrieves the stored quoted abbreviation via content_search but the
# filter then drops it (no tolerated quote between letters).
MARK_TOLERANT_INSERTER = '[\u0300-\u036F\u0022\u0027\u05F3\u05F4\u2018\u2019]*'


def make_mark_tolerant_pattern(escaped_term: str) -> str:
    """Insert optional combining mark matchers between characters of an escaped regex term.

    Takes an already-escaped regex term (from re.escape) and inserts optional
    combining mark matchers between each token. Escape sequences like \\. are
    treated as single tokens.

    Example: re.escape("abc") -> "a[\\u0300-\\u036F]*b[\\u0300-\\u036F]*c"
    """
    if not escaped_term:
        return escaped_term
    # Split escaped string into tokens: \\X (escape sequences) or single chars
    tokens = re.findall(r'\\.|.', escaped_term)
    return MARK_TOLERANT_INSERTER.join(tokens)



# ==============================================================================
#  SEARCH ENGINE
# ==============================================================================
# Reciprocal Rank Fusion constant (D-08 Codex P0): BM25 scores from two
# independent indexes are not comparable, so LOCAL + Genizah hits are fused by
# rank. k=60 is the Cormack/Clarke (2009) default.
RRF_K = 60

# Progress PHASES, reported through the optional `phase_callback` that
# execute_search / _query_local_index accept.  Deliberately a small closed
# vocabulary of stable codes, NOT free text: the UI maps the code to a
# translated label and switches the progress bar to indeterminate, so it must
# never have to pattern-match on prose.  A phase is not a status message.
PHASE_LOCAL_SEARCH = 'local_search'


#: Padding (characters) kept either side of a matched span in composition
#: snippets.
COMPOSITION_SNIPPET_PAD = 60


def build_marked_composition_fragment(content, span_start, span_end,
                                      pad=COMPOSITION_SNIPPET_PAD):
    """Return one composition snippet with the matched span in ``*`` markers.

    Extracted from ``search_composition_logic`` so the marker contract can be
    tested without a Tantivy index (PR #325 workflow review: the fix below
    shipped unproven because nothing could reach these two lines cheaply).

    A literal ``*`` already present in the manuscript text is replaced with a
    space FIRST: every consumer treats ``*`` as a highlight delimiter -- the
    parallels xlsx export splits on it to build red+bold runs, and one stray
    marker restyles the remainder of the cell. ``highlight`` and
    ``_highlight_by_span`` in this module already apply the same rule; the
    composition builder did not, and the xlsx highlighting work made the gap
    visible.
    """
    if not content:
        return ''
    start = max(0, span_start - pad)
    end = min(len(content), span_end + pad)
    return (content[start:span_start].replace('*', ' ')
            + '*' + content[span_start:span_end].replace('*', ' ')
            + '*' + content[span_end:end].replace('*', ' '))


def mark_word_highlights(snippet, highlights):
    """Wrap each (start, end) character span of ``snippet`` in ``*`` markers.

    Extracted for the same reason as ``build_marked_composition_fragment``
    above: the marker contract must be testable without a Tantivy index.

    Codex round 6 (PR #325): the source-context builder inserted markers into
    the user's PASTED text verbatim, so a literal ``*`` already in it (a
    copied footnote marker, say) toggled the xlsx rich-text state and styled
    the rest of the cell as matched. Neutralize literal asterisks FIRST --
    ``.replace`` is length-preserving, which is what keeps the span offsets
    (measured on the raw snippet) valid -- then insert in reverse order so
    earlier offsets survive the splices. Same rule as ``highlight``,
    ``_highlight_by_span`` and the composition-fragment builder above.
    """
    result = snippet.replace('*', ' ')
    for word_start, word_end in reversed(highlights):
        result = (result[:word_start] + '*' + result[word_start:word_end]
                  + '*' + result[word_end:])
    return result


class SearchEngine:
    """Run searches, build queries, and provide browsing utilities."""
    def __init__(self, meta_mgr, variants_mgr):
        self.meta_mgr = meta_mgr
        self.var_mgr = variants_mgr
        self.index = None
        self.searcher = None
        # SEED-006 compat gate: whether the on-disk main / LOCAL indexes carry
        # the additive content_search field. Set at open time; gates the Stage 2
        # OR-fallback so the NEW query code never crashes against an OLD index
        # built before content_search existed (graceful degradation to content).
        self._has_content_search = False
        # FL ID index for O(1) browse lookup (built in background)
        self._fl_id_index = None  # dict: fl_digits_str -> list of (sys_id, page_idx)
        self._fl_id_index_building = False
        self._fl_id_index_lock = threading.Lock()
        self.reload_index()
        self.start_fl_id_index_build()
        # Phase 95 — open LOCAL side-index alongside main (D-14 + D-37 fallback).
        self.local_index = None            # tantivy.Index for LOCAL side-index
        self.local_searcher = None         # tantivy.Searcher snapshot
        self._local_has_content_search = False  # SEED-006 compat gate (LOCAL)
        self.local_lab_searcher = None     # tantivy.Searcher for LOCAL LAB side-index
        self._local_lab_index = None       # tantivy.Index for LOCAL LAB side-index (parse_query)
        self.local_lab_searcher_stale = False  # D-38: True when weights_hash mismatch
        self._lab_local_meta = None        # dict from .meta.json, or None
        # Phase 96 D-F5 (Codex HIGH #3): instrumentation for test spies.
        self._last_local_query_regex = None
        # Phase 96 NEW-2: per-sys_id page-list cache for get_local_browse_page.
        self._local_pages_cache = {}
        # Phase 97 R-01: weakref to MyLibraryTab for is_searchable gate.
        # Default True when no tab attached (e.g. standalone CLI / tests).
        self._my_library_tab_ref: weakref.ref | None = None
        # Phase 97 R-02: error from last atomic rebuild attempt; surfaced in UI banner.
        self._local_open_error: str | None = None
        self._open_local_searcher()

    def attach_my_library_tab(self, tab) -> None:
        """Phase 97 R-01: attach a weakref to the MyLibraryTab for is_searchable gate.

        Called from MyLibraryTab.__init__ after engine reference is available.
        The gate at _query_local_index checks is_searchable via this weakref.
        Default-True when weakref is dead (engine running standalone / tests).
        """
        self._my_library_tab_ref = weakref.ref(tab)

    def close_local_searcher(self) -> None:
        """Phase 97 R-02 LD-5: close BOTH main + LAB searcher + index handles.

        Called BEFORE os.rename in rebuild_main_index_atomic to prevent
        Windows os error 5 (Access denied) from an open reader handle.
        Closes the FULL handle graph:
          local_searcher, local_index, local_lab_searcher, _local_lab_index
        (note underscore prefix on _local_lab_index — verified at genizah_core.py:6745).
        """
        self.local_searcher = None
        self.local_index = None
        self.local_lab_searcher = None
        self._local_lab_index = None  # exact attribute name (line 6745)

    def close_local_lab_searcher(self) -> None:
        """Phase 97 R-02 LD-5 (LAB-only variant used by rebuild_lab_index_atomic)."""
        self.local_lab_searcher = None
        self._local_lab_index = None

    def _open_local_searcher(self) -> None:
        """Phase 97 R-02: open LOCAL side-index with atomic-rebuild recovery.

        Replaces D-37 silent None fallback per Codex HIGH #3 and ADVICE LD-4.
        On Index.open exception OR schema-mismatch, attempts atomic rebuild from
        cached_text via LocalIndexer. On rebuild failure, logs + sets both to None
        AND records error in self._local_open_error for UI banner consumption.
        Does NOT silently create a fresh empty index on corruption.
        """
        self.local_index = None
        self.local_searcher = None
        self._local_open_error = None

        if not os.path.isdir(Config.LOCAL_INDEX_DIR):
            LOGGER.info(
                "LOCAL side-index dir not present (no scan yet?): %s",
                Config.LOCAL_INDEX_DIR,
            )
            return

        from shared.local_indexer import (
            build_local_schema,
            LocalIndexer,
            migrate_legacy_local_db,
            _compute_schema_marker,
            _read_schema_marker,
        )

        # Check schema marker
        expected_marker = _compute_schema_marker(build_local_schema)
        actual_marker = _read_schema_marker(Config.LOCAL_INDEX_DIR)
        schema_mismatch = (actual_marker != expected_marker)

        try:
            schema = build_local_schema()
            local_index = tantivy.Index(schema, path=Config.LOCAL_INDEX_DIR)
            register_search_tokenizers(local_index)  # SEED-006: hebword + builtins
            if schema_mismatch:
                raise RuntimeError(
                    f"Schema marker mismatch (actual={actual_marker!r}, "
                    f"expected={expected_marker!r})"
                )
            self.local_index = local_index
            self.local_searcher = local_index.searcher()
            self._local_has_content_search = _index_has_field(local_index, "content_search")
            self._warn_if_local_index_stale()
            LOGGER.info("LOCAL side-index opened: %s", Config.LOCAL_INDEX_DIR)
        except Exception as open_exc:
            LOGGER.warning(
                "LOCAL index open/schema-check failed: %r — attempting atomic rebuild",
                open_exc,
            )
            try:
                import gc as _gc
                # Codex MED #3: drop the probe Tantivy handle opened above BEFORE
                # constructing the indexer. The LocalIndexer constructor performs the
                # atomic rebuild from cached_text on the mismatch/open-failure it
                # re-detects (which renames LOCAL_INDEX_DIR); a live read handle here
                # would block os.rename on Windows.
                local_index = None
                _gc.collect()
                # SEED-006 P1: DB must live OUTSIDE the atomically-swapped LocalIndex
                # dir; migrate any legacy in-dir DB out before constructing.
                db_path = migrate_legacy_local_db(Config.LOCAL_INDEX_DIR)
                # Codex MED #3: the constructor already does the SINGLE atomic rebuild
                # on the same mismatch — do NOT call rebuild_main_index_atomic() again
                # (the prior code rebuilt twice: once in __init__, once explicitly).
                indexer = LocalIndexer(
                    index_dir=Config.LOCAL_INDEX_DIR,
                    lab_index_dir=Config.LOCAL_LAB_INDEX_DIR,
                    db_path=db_path,
                )
                # D-01: close the temp indexer's writer + index handles before
                # opening the live searcher below, so its writer lock is released
                # (a still-live writer would block writer acquisition on the dir).
                try:
                    indexer._close_internal_writer_index()
                except Exception:
                    LOGGER.exception("temp indexer close failed (continuing)")
                schema2 = build_local_schema()
                local_index2 = tantivy.Index(schema2, path=Config.LOCAL_INDEX_DIR)
                register_search_tokenizers(local_index2)  # SEED-006
                self.local_index = local_index2
                self.local_searcher = local_index2.searcher()
                self._local_has_content_search = _index_has_field(local_index2, "content_search")
                self._warn_if_local_index_stale()
                LOGGER.info(
                    "LOCAL side-index: atomic rebuild succeeded, reopened: %s",
                    Config.LOCAL_INDEX_DIR,
                )
            except Exception as rebuild_exc:
                LOGGER.error("LOCAL atomic rebuild failed: %r", rebuild_exc)
                self._local_open_error = str(rebuild_exc)
                self.local_index = None
                self.local_searcher = None

    def reload_local_indexes(self) -> None:
        """HIGH-1 review fix: reopen LOCAL Tantivy searchers (main + LAB) so newly
        committed docs become visible in the live session.

        Called by MyLibraryTab (Plan 07) AFTER every refresh / delete / rebuild /
        recovery commit. Idempotent + defensive: on any open failure, the searcher
        falls back to None (D-37 semantics).
        """
        # Phase 96 NEW-2: invalidate page-list cache so get_local_browse_page
        # sees fresh docs after a rebuild/refresh.
        self._local_pages_cache = {}
        self._open_local_searcher()
        self.reload_local_lab_index()

    def reload_local_lab_index(self) -> None:
        """HIGH-1 review fix (LAB-only narrow reload). Reopens self.local_lab_searcher
        and re-reads the .meta.json staleness sentinel.

        Plan 06: also reads .meta.json for D-38 weights_hash freshness check.
        """
        self.local_lab_searcher = None
        self._lab_local_meta = None
        try:
            if os.path.isdir(Config.LOCAL_LAB_INDEX_DIR):
                from shared.local_indexer import build_local_lab_schema, LocalIndexer
                schema = build_local_lab_schema()
                local_lab_index = tantivy.Index(schema, path=Config.LOCAL_LAB_INDEX_DIR)
                # Phase 110 UAT BLOCKER (mirror of LabEngine.reload_local_lab_index):
                # the LOCAL LAB schema's fingerprint / fingerprint_dyn / content
                # fields use tokenizer_name="simple" (text_ngram uses "whitespace").
                # Without registering them on this freshly-opened Index, any
                # parse_query against those fields raises
                # ValueError('The tokenizer "simple" ... is unknown'). Register
                # before creating the searcher / running any query.
                for _tk_name, _tk in (
                    ("simple", tantivy.Tokenizer.simple()),
                    ("whitespace", tantivy.Tokenizer.whitespace()),
                ):
                    try:
                        local_lab_index.register_tokenizer(
                            _tk_name, tantivy.TextAnalyzerBuilder(_tk).build()
                        )
                    except Exception:
                        pass  # May fail on reopen — non-fatal, like _ensure_lab_tokenizers
                self.local_lab_searcher = local_lab_index.searcher()
                # Phase 95 D-38: read .meta.json for weights_hash freshness check
                self._lab_local_meta = LocalIndexer.read_lab_meta(Config.LOCAL_LAB_INDEX_DIR)
                self._local_lab_index = local_lab_index  # kept for parse_query
                LOGGER.info(
                    "HIGH-1 reload: LOCAL LAB side-index reopened: %s",
                    Config.LOCAL_LAB_INDEX_DIR,
                )
            else:
                LOGGER.info(
                    "HIGH-1 reload: LOCAL LAB side-index dir absent; searcher=None"
                )
        except Exception as e:
            LOGGER.warning(
                "HIGH-1 reload: LOCAL LAB side-index unavailable: %r", e
            )
            self.local_lab_searcher = None
            self._lab_local_meta = None

    def _current_lab_weights_hash(self) -> str:
        """Compute hash of current LAB weights for D-38 staleness check.

        CR-01 FIX: ``dynamic_rank_map`` and ``settings`` are NOT defined on
        :class:`SearchEngine` — they live on :class:`LabEngine`.  Until the
        two classes share a registry, ``getattr`` with safe defaults is the
        smallest correctness fix:

          * On a real :class:`SearchEngine` (no LAB attrs) the function returns
            a deterministic hash of "no weights" → never crashes.
          * On a :class:`LabEngine` instance (or a SearchEngine that has been
            wired to a LabEngine and forwards attribute access), the real LAB
            weights are picked up via ``getattr`` and the hash matches what
            ``build_lab_side_index`` writes into ``.meta.json``.
        """
        # Phase 110 RF-4: standard composition uses the LabEngine's hash;
        # SearchEngine.dynamic_rank_map is always None → would never match
        # .meta.json otherwise. The app injects
        # searcher._lab_weights_hash_override = lab_engine._current_lab_weights_hash()
        # (Plan 03 — at GenizahGUI init + after every LOCAL LAB rebuild) so the
        # standard-composition freshness check compares against the SAME hash the
        # index was built with, letting 'all'-scope LOCAL LAB hits through.
        _override = getattr(self, '_lab_weights_hash_override', None)
        if _override:
            return _override
        import hashlib as _hashlib
        import json as _json
        # CR-01: use getattr defaults so missing attrs don't raise.
        dyn_map = getattr(self, "dynamic_rank_map", None)
        settings = getattr(self, "settings", None)
        weights_dict = {
            "dynamic_rank_map": dyn_map if dyn_map else None,
            "use_dynamic_weights": (
                getattr(settings, "use_dynamic_weights", False)
                if settings is not None
                else False
            ),
        }
        return _hashlib.sha256(
            _json.dumps(weights_dict, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    def _check_local_lab_freshness(self) -> bool:
        """Return True if LOCAL LAB index is fresh; False if stale or missing.

        Side effect: sets self.local_lab_searcher_stale.
        D-38: compares current LAB weights_hash to value stored in .meta.json.
        """
        if self.local_lab_searcher is None:
            return False
        meta = self._lab_local_meta
        if not meta:
            self.local_lab_searcher_stale = True
            LOGGER.info("LOCAL LAB index has no .meta.json — treating as stale")
            return False
        current_hash = self._current_lab_weights_hash()
        if meta.get("weights_hash") != current_hash:
            self.local_lab_searcher_stale = True
            LOGGER.info(
                "LOCAL LAB index stale (weights changed); banner surface required"
            )
            return False
        self.local_lab_searcher_stale = False
        return True

    def rebuild_local_lab_index(self, local_indexer, lab_engine=None) -> None:
        """Trigger LOCAL LAB rebuild via LocalIndexer, passing fingerprint helpers
        as callbacks (W5 — Option C LOCKED). Called from MyLibraryTab Refresh
        (Plan 07) and Tools→Rebuild LAB (per D-38).

        The three bound-method callbacks (_compute_fingerprint_dyn,
        _compute_fingerprint_static, _normalize_text) are thin wrappers around
        the existing text_to_fingerprint / lab_index_normalize helpers in this class.

        The LAB weights (``dynamic_rank_map`` + ``settings``) live on
        :class:`LabEngine`, NOT on :class:`SearchEngine` (same split CR-01 fixed
        for ``_current_lab_weights_hash``). The caller passes the live LabEngine
        as ``lab_engine`` so the weights — and therefore the build-time
        fingerprints and the ``weights_hash`` written into ``.meta.json`` —
        match what the LabEngine freshness check recomputes. Falling back to
        ``self`` with ``getattr`` defaults keeps a plain SearchEngine from
        raising ``AttributeError``; sourcing weights from ``self`` (None map)
        would make the freshness check see a different hash and judge the index
        perpetually stale.
        """
        weights_source = lab_engine if lab_engine is not None else self
        dyn_map = getattr(weights_source, "dynamic_rank_map", None)
        settings = getattr(weights_source, "settings", None)
        # Normalize identically to LabEngine._current_lab_weights_hash so the
        # weights_hash written here matches the freshness check exactly.
        lab_weights = {
            "dynamic_rank_map": dyn_map if dyn_map else None,
            "use_dynamic_weights": (
                getattr(settings, "use_dynamic_weights", False)
                if settings is not None else False
            ),
        }
        local_indexer.build_lab_side_index(
            lab_weights=lab_weights,
            fingerprint_dyn_fn=self._compute_fingerprint_dyn,
            fingerprint_static_fn=self._compute_fingerprint_static,
            normalize_text_fn=self._normalize_text,
            lab_schema_version=1,
            dynamic_rank_map=dyn_map,
        )
        # Reload so newly built index is visible in live session
        self.reload_local_lab_index()

    def _compute_fingerprint_dyn(self, content: str, dynamic_rank_map) -> str:
        """Compute fingerprint_dyn for a content string using the given rank map.
        W5 Option C callback — wraps text_to_fingerprint with dynamic weights."""
        from genizah_core import text_to_fingerprint  # noqa: PLC0415 -- lazy; GUARD-01 safe
        return text_to_fingerprint(content, freq_map=dynamic_rank_map)

    def _compute_fingerprint_static(self, content: str) -> str:
        """Compute static fingerprint for a content string using HEBREW_FREQ.
        W5 Option C callback — wraps text_to_fingerprint with static weights."""
        from genizah_core import text_to_fingerprint, HEBREW_FREQ  # noqa: PLC0415 -- lazy; GUARD-01 safe
        return text_to_fingerprint(content, freq_map=HEBREW_FREQ)

    def _normalize_text(self, content: str) -> str:
        """Normalize content for the text_normalized field (W5 Option C callback).

        Wraps ``LabEngine.lab_index_normalize`` — a pure ``@staticmethod`` that
        lives on :class:`LabEngine`, NOT on :class:`SearchEngine`. The previous
        ``self.lab_index_normalize`` raised ``AttributeError`` whenever this
        callback ran on a SearchEngine, which aborted every LOCAL LAB rebuild at
        ``build_lab_side_index``'s pre-flight probe — so ``.meta.json`` was never
        written, the index stayed perpetually "stale", and a doomed rebuild was
        re-attempted on every startup/refresh (console churn + wasted reloads)."""
        from shared.lab_engine import LabEngine  # noqa: PLC0415 -- lazy; avoids module cycle
        return LabEngine.lab_index_normalize(content)

    def _query_local_index(self, query_str: str, mode: str, gap: int,
                           limit=None, regex=None, tantivy_query_str=None,
                           progress_callback=None, phase_callback=None):
        """Query the LOCAL side-index. Returns [] if local_searcher is None (D-37).

        MEDIUM-1 note: this uses a simplified parse_query (not the full Responsa
        expansion pipeline used by the main searcher). The divergence is documented
        as a deferred follow-up — see plan 95-05 <deferred> block.

        Phase 96 D-F5: accept an optional `regex` parameter so each hit dict
        can carry `highlight_pattern` + asterisk-marker `snippet`, matching
        the Genizah hit shape. When `regex` is None (legacy callers), hits
        are returned with raw snippets (back-compat).

        REVISION 2026-05-24 — D-04.1 LOAD-BEARING (Codex HIGH #2): when
        `regex` is provided AND `_build_local_result_dict` returns None (regex
        didn't match), the candidate is SKIPPED. Visible LOCAL hits all satisfy
        the regex — same algebra as Genizah hits (genizah_core.py:8335-8345
        `if hl_c:` filter-out pattern).

        REVISION 2026-05-24 — Codex HIGH #3: records the last-passed regex
        object on `self._last_local_query_regex` so test spies can assert the
        merge call site genuinely threaded regex (rather than passing None
        silently). Cleared on each call entry to avoid stale state.
        """
        # Codex HIGH #3 instrumentation — record what we got passed.
        self._last_local_query_regex = regex

        # Phase 97 R-01 is_searchable gate — FIRST executable check.
        # Returns [] while MyLibraryTab recovery modal is unresolved.
        # Default-True if weakref is dead (engine running standalone / CLI / tests).
        tab = self._my_library_tab_ref() if self._my_library_tab_ref is not None else None
        if tab is not None and not getattr(tab, "is_searchable", True):
            return []

        if self.local_searcher is None or self.local_index is None:
            return []
        # Declared before the try so every handler below can hand back whatever
        # was materialised — a cancel must never cost the user their partial hits.
        results = []
        try:
            # Use self.local_index (kept alongside local_searcher) for parse_query.
            # tantivy.Searcher has no .index attribute — Index must be stored separately.
            # MEDIUM-1 deferred: full query builder (variants/Responsa) not extracted yet.
            # SEED-006 Stage 2: fan field-less terms across content_search too (the
            # diacritic-folded field) so צמאן / צ'מאן reach צ̇מאן. query_str is
            # already diacritic-stripped by both callers. Gated on the compat flag
            # so an OLD LOCAL index (no content_search) keeps working.
            _fields = ["content", "content_head", "content_tail"]
            if getattr(self, "_local_has_content_search", False):
                _fields.append("content_search")
            # Phase 95-05 follow-up (Responsa-over-LOCAL): when the caller supplies
            # a pre-expanded candidate query (the SAME operator-expanded query the
            # main index uses for Responsa #/*/%/(a/b)), parse THAT — the simplified
            # parse_query below strips operator metacharacters and returns nothing,
            # which is why LOCAL Responsa came back empty. Defensive fall-back to the
            # simplified path if the pre-built query somehow fails to parse.
            tantivy_q = None
            if tantivy_query_str:
                try:
                    tantivy_q = self.local_index.parse_query(tantivy_query_str, _fields)
                except (ValueError, Exception):
                    LOGGER.warning(
                        "LOCAL pre-built query failed to parse; falling back to simplified: %.200s",
                        tantivy_query_str,
                    )
                    tantivy_q = None
            if tantivy_q is None:
                try:
                    tantivy_q = self.local_index.parse_query(query_str, _fields)
                except (ValueError, Exception):
                    # v7.16: Tantivy's query parser chokes on syntax metacharacters,
                    # which appear naturally in Hebrew abbreviations — the geresh in
                    # אמ' and the gershayim in רמב"ם both raise "Syntax Error" and the
                    # whole LOCAL search returned 0 results. Strip the metacharacters
                    # (`'"` + structural chars) so the term query parses; the precise
                    # match is still enforced by the regex filter below.
                    _safe = re.sub(r"[+\-&|!(){}\[\]^\"~*?:\\/']", " ", query_str).strip()
                    if not _safe:
                        return []
                    tantivy_q = self.local_index.parse_query(_safe, _fields)
            search_limit = limit or Config.SEARCH_LIMIT
            res_obj = self.local_searcher.search(tantivy_q, search_limit)
            hits = res_obj.hits if hasattr(res_obj, "hits") else res_obj
            pattern_str = regex.pattern if regex is not None else ""
            # The LOCAL pass is a distinct phase, not more of the Genizah one: its
            # hit counts are unrelated, so reporting them on the same numeric
            # channel would rewind the bar. Announce the phase and let the UI go
            # indeterminate; the ticks below are then only a cancel/pause carrier.
            if phase_callback:
                try:
                    phase_callback(PHASE_LOCAL_SEARCH)
                except (InterruptedError, KeyboardInterrupt):
                    raise
                except Exception:
                    pass
            _local_total = len(hits) if hasattr(hits, '__len__') else 0
            # Scoped tightly to the loop: a cancel here means "stop scanning
            # and keep what you found", matching _execute_metadata_search,
            # search_composition_logic and lab_search. Before this the raise
            # travelled all the way out to SearchThread, which emitted [] — so
            # a stopped My Library search showed nothing under a UI label that
            # explicitly reads "(Partial results)".
            try:
                for _i_loc, (score, doc_address) in enumerate(hits):
                    # Until now this loop had no callback at all, so a LOCAL or ALL
                    # scope search ignored Stop entirely.
                    if progress_callback and _i_loc % 5 == 0:
                        try:
                            progress_callback(_i_loc, _local_total)
                        except (InterruptedError, KeyboardInterrupt):
                            raise
                        except Exception:
                            pass  # progress is advisory; cancellation is not
                    doc = self.local_searcher.doc(doc_address)
                    hit = self._build_local_result_dict(
                        doc, score, regex=regex, pattern_str=pattern_str
                    )
                    # D-04.1 filter-out: skip candidates whose regex didn't match.
                    # _build_local_result_dict returns None for those.
                    if hit is None:
                        continue
                    results.append(hit)
            except InterruptedError:
                return results  # cancelled mid-scan — keep the hits we built
            return results
        except InterruptedError:
            # Still not an index failure, so still ahead of the broad handler —
            # but hand back what was gathered instead of re-raising. Telemetry
            # correctness does NOT depend on the exception escaping: perf_signal
            # is suppressed by the worker's cancel_flag, and the "(Partial
            # results)" suffix comes from the UI's own _search_was_cancelled.
            return results
        except Exception as e:
            LOGGER.warning("LOCAL index query failed: %r", e)
            return []

    def _build_local_result_dict(self, doc, score, regex=None, pattern_str=None):
        """Construct a result row from a LOCAL Tantivy doc per D-34 shape.

        Phase 96 D-F5: when `regex` is provided, populate snippet + raw_file_hl
        + highlight_pattern so the LOCAL hit shape matches Genizah hits and
        downstream UI (format_snippet, ResultDialog highlight branch) works
        without per-source branching. Mirrors genizah_core.py:8335-8345.

        REVISION 2026-05-24 — D-04.1 LOAD-BEARING (per user CONTEXT update +
        Codex HIGH #2): when `regex` is provided AND the regex does NOT match
        `content`, this function returns **None** to signal the caller to FILTER
        OUT this candidate. This matches the Genizah two-phase model — Tantivy
        candidates that fail the regex are silently dropped from the result list.
        No fallback display of unhighlighted content.

        Back-compat: when `regex` is None (legacy callers), the function ALWAYS
        returns a dict (old shape — snippet = content[:200]).

        Returns:
            - dict (the hit) when regex matches OR regex is None
            - None when regex is provided AND regex does NOT match content
              → caller MUST skip this candidate (D-04.1)
        """
        unique_id = doc.get_first("unique_id") or ""
        full_header = doc.get_first("full_header") or ""
        content = doc.get_first("content") or ""
        shelfmark = doc.get_first("shelfmark") or ""
        # Phase 97 D-NEW-5: chunk_locator — location string set by extractor
        # (e.g. "p. 3" for PDF, "paragraphs 1-20" for DOCX, "§ Introduction" for HTML).
        chunk_locator = doc.get_first("chunk_locator") or ""
        # Parse sys_id + p_num from full_header (format: {sys_id}_LOCAL_P{page}_F{file_id})
        sys_id = ""
        p_num = "1"
        if full_header:
            parts = full_header.split("_LOCAL_P")
            if len(parts) == 2:
                sys_id = parts[0]
                # p_num is before the _F suffix
                p_part = parts[1].split("_F")[0]
                p_num = p_part

        # Phase 96 D-F5: compute snippet via self.highlight when regex provided.
        # D-04.1: filter-out signal when regex doesn't match (return None).
        if regex is not None:
            hl_c = self.highlight(content, regex, for_file=False)
            if not hl_c:
                # D-04.1: Tantivy matched but regex didn't.
                # SILENTLY DROP this candidate by returning None.
                # _query_local_index will skip it.
                return None
            hl_f = self.highlight(content, regex, for_file=True)
            snippet = hl_c
            raw_file_hl = hl_f or ""
            effective_pattern = pattern_str or regex.pattern
        else:
            # Back-compat path (no regex passed by caller — old behaviour).
            # Always returns a dict; no filter-out.
            snippet = content[:200] if content else ""
            raw_file_hl = ""
            effective_pattern = ""

        return {
            "uid": unique_id,
            "full_text": content,
            "snippet": snippet,
            "raw_file_hl": raw_file_hl,
            "highlight_pattern": effective_pattern,
            "sys_id": sys_id,
            "p_num": p_num,
            # Phase 96 fix-4: populate 'img' with p_num (page/chunk number).
            # Genizah hits use 'img' for the folio page number; LOCAL hits must
            # mirror this so ResultDialog.load_result and _open_local_browse can
            # both open at the correct page (not always page 1).
            "img": p_num,
            "score": float(score),
            # Phase 97 D-NEW-5: chunk_locator for human-readable position in source file.
            "chunk_locator": chunk_locator,
            "display": {
                "id": sys_id,
                "source": "LOCAL",
                "library_code": "LOCAL",
                "shelfmark": shelfmark,
                # Phase 96 fix-3 (Img column): search results render the Img
                # column from meta.get('img') where meta = res['display'].
                # Genizah hits populate 'img' inside the display dict via
                # get_display_data().  LOCAL hits must mirror this so the Img
                # column shows the page/chunk number instead of a blank cell.
                "img": p_num,
            },
            "full_header": full_header,
        }

    def _rrf_merge(self, genizah_hits, local_hits, k: int = RRF_K, limit=None):
        """Reciprocal Rank Fusion merger (D-08 Codex P0). BM25 scores from two
        independent indexes are NOT comparable; RRF fuses by rank (Cormack/Clarke 2009).

        Tie-break: Genizah first when LOCAL and Genizah hits have identical scores.
        The tie-break is content-driven (display.source != 'LOCAL' → Genizah) so it
        is ORDER-INDEPENDENT — passing (local, genizah) vs (genizah, local) gives the
        same result (W7 requirement).
        """
        rrf: dict = {}
        for rank, hit in enumerate(genizah_hits, start=1):
            uid = hit["uid"]
            rrf.setdefault(uid, {"hit": hit, "score": 0.0})
            rrf[uid]["score"] += 1.0 / (k + rank)
        for rank, hit in enumerate(local_hits, start=1):
            uid = hit["uid"]
            rrf.setdefault(uid, {"hit": hit, "score": 0.0})
            rrf[uid]["score"] += 1.0 / (k + rank)
        # Tie-break: Genizah (non-LOCAL) first at equal score.
        # display.source == 'LOCAL' → local (lower priority on tie).
        # Any other source (V0.8, V0.7) → Genizah (higher priority on tie).
        # True > False → non-LOCAL sorts higher at equal score (reverse=True).
        fused = sorted(
            rrf.values(),
            key=lambda r: (r["score"], r["hit"].get("display", {}).get("source") != "LOCAL"),
            reverse=True,
        )
        out = [r["hit"] for r in fused]
        return out[:limit] if limit else out

    # ------------------------------------------------------------------
    #  FL ID Index (background build for O(1) browse-by-FL lookup)
    # ------------------------------------------------------------------
    def _build_fl_id_index(self):
        """Build FL ID -> (sys_id, page_idx) index from browse_map. Called in background thread."""
        browse_map = self._load_browse_map()
        if not browse_map:
            return
        index = {}
        for sys_id, pages in browse_map.items():
            for idx, page in enumerate(pages):
                parsed = self.meta_mgr.parse_full_id_components(page.get('full_header', ''))
                fl_id = parsed.get('fl_id')
                if fl_id:
                    fl_digits = re.sub(r"\D", "", str(fl_id))
                    if fl_digits:
                        if fl_digits not in index:
                            index[fl_digits] = []
                        index[fl_digits].append((sys_id, idx))
        with self._fl_id_index_lock:
            self._fl_id_index = index
        LOGGER.info("FL ID index built: %d entries", len(index))

    def start_fl_id_index_build(self):
        """Start building FL ID index in background. Non-blocking."""
        if self._fl_id_index is not None or self._fl_id_index_building:
            return
        self._fl_id_index_building = True
        t = threading.Thread(target=self._build_fl_id_index_thread, daemon=True)
        t.start()

    def _build_fl_id_index_thread(self):
        try:
            self._build_fl_id_index()
        except Exception as e:
            LOGGER.warning("Failed to build FL ID index: %s", e)
        finally:
            self._fl_id_index_building = False

    @staticmethod
    def format_snippet(text, style='html_class'):
        """
        Format snippet with highlighted matches, safely escaping HTML.
        style: 'html_class' (Web: span class="highlight-match") or 'html_inline' (Desktop: span style="color:...")
        """
        if not text:
            return ""

        # First escape HTML to prevent XSS
        escaped = html.escape(text)

        # Style ‖ line-break indicators
        escaped = escaped.replace('\u2016', '<span class="line-break-sep">\u2016</span>' if style == 'html_class'
                                  else '<span style="color:#888; font-weight:bold;">\u2016</span>')

        # Convert *word* to highlighted span (after escaping, markers are safe)
        if style == 'html_class':
            return re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', escaped)
        else:
            # Desktop style (inline) - Red/Bold
            return re.sub(r'\*(.*?)\*', r'<span style="color:#ff0000; font-weight:bold;">\1</span>', escaped)

    def close_index(self):
        """Release Tantivy index and searcher to unlock files (required before rebuild on Windows)."""
        self.searcher = None
        self.index = None
        import gc
        gc.collect()

    def reload_index(self):
        db_path = os.path.join(Config.INDEX_DIR, "tantivy_db")
        if os.path.exists(db_path):
            try:
                self.index = tantivy.Index.open(db_path)
                # SEED-006: register hebword (content field) BEFORE searcher use,
                # then detect content_search for the Stage 2 compat gate.
                register_search_tokenizers(self.index)
                self._has_content_search = _index_has_field(self.index, "content_search")
                if not self._has_content_search:
                    # SEED-006 M3 + SEED-019 #28: the GENIZAH main index has no schema
                    # marker, so nothing else detects that it predates this fix.
                    # Surface it loudly — the punctuation/diacritic retrieval fix is
                    # INERT (degrades to whitespace-tokenized content-only) until the
                    # index is rebuilt. Message centralized in
                    # content_search_staleness_messages so this log and the queryable
                    # index_staleness_report() never drift apart.
                    for _msg in content_search_staleness_messages(False, None):
                        LOGGER.warning("Stale index [%s]: %s", db_path, _msg)
                self.searcher = self.index.searcher()
                return True
            except Exception as e:
                LOGGER.error("Failed to reload Tantivy index from %s: %s", db_path, e)
        return False

    def index_staleness_report(self) -> dict:
        """SEED-019 #28: queryable verdict on the SEED-006 ``content_search`` compat
        gate, so a degraded (stale) index is visible beyond the one-shot WARNING
        emitted at index open.

        The gate keeps search working against an index that predates the
        ``content_search`` field, but silently degrades Hebrew
        punctuation/diacritic retrieval. This report exposes that state for ops /
        health surfaces (and the desktop rebuild prompt) instead of leaving it to a
        single startup log line.

        Returns:
            dict: ``{'genizah_content_search': bool,
                     'local_content_search': bool | None,   # None = no LOCAL index
                     'stale': bool,                          # any index degraded
                     'messages': list[str]}``               # remediation per index
        """
        genizah_present = bool(getattr(self, "_has_content_search", False))
        local_present = (
            bool(getattr(self, "_local_has_content_search", False))
            if getattr(self, "local_index", None) is not None
            else None
        )
        messages = content_search_staleness_messages(genizah_present, local_present)
        return {
            "genizah_content_search": genizah_present,
            "local_content_search": local_present,
            "stale": bool(messages),
            "messages": messages,
        }

    def _warn_if_local_index_stale(self) -> None:
        """SEED-019 #28: log a remediation warning when a freshly-opened LOCAL
        side-index predates the SEED-006 ``content_search`` field. LOCAL normally
        self-heals via its schema marker (a mismatch triggers an atomic rebuild),
        so this is a defensive catch for the rare case where it opened degraded —
        parity with the GENIZAH warning in :meth:`reload_index`.
        """
        if getattr(self, "_local_has_content_search", True) is False:
            for _msg in content_search_staleness_messages(True, False):
                LOGGER.warning("Stale LOCAL index: %s", _msg)

    # Class-level browse_map shared across all instances (loaded once)
    _shared_browse_map = None
    _browse_map_lock = threading.Lock()

    def _load_browse_map(self):
        """Load the browse map, deduplicate it, and persist corrections if needed.

        Uses class-level cache shared across all SearchEngine instances to avoid
        redundant file I/O, repair scans, and pickle write race conditions.
        """
        if SearchEngine._shared_browse_map is not None:
            return SearchEngine._shared_browse_map

        if hasattr(self, '_browse_map_cache') and self._browse_map_cache is not None:
            return self._browse_map_cache

        if not os.path.exists(Config.BROWSE_MAP):
            return {}

        with SearchEngine._browse_map_lock:
            # Double-check after acquiring lock (another thread may have loaded)
            if SearchEngine._shared_browse_map is not None:
                return SearchEngine._shared_browse_map

            with open(Config.BROWSE_MAP, 'rb') as f:
                raw_map = pickle.load(f)

            cleaned_map, changed = dedupe_browse_map(raw_map)
            if changed:
                # Atomic write: this runs off the FL-ID background thread, which
                # no app-close drain waits on, so a process killed mid-write must
                # never leave a truncated/corrupt browse_map.pkl behind -- only
                # ever the old file or the new one, via os.replace.
                tmp_path = Config.BROWSE_MAP + '.tmp'
                try:
                    with open(tmp_path, 'wb') as f:
                        pickle.dump(cleaned_map, f)
                    os.replace(tmp_path, Config.BROWSE_MAP)
                except Exception as e:
                    LOGGER.warning("Failed to write deduplicated browse map to %s: %s", Config.BROWSE_MAP, e)
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass

            SearchEngine._shared_browse_map = cleaned_map
            self._browse_map_cache = cleaned_map
            return cleaned_map

    def _get_or_compute_variants(self, terms, mode):
        """Pre-compute variants at the larger limit for each search term.

        This ensures that when build_tantivy_query requests variants with
        limit=200 and build_regex_pattern later requests limit=8000 for the
        same term+mode, the second call is served from the superset cache
        (via slicing) instead of recomputing from scratch.
        """
        if not self.var_mgr or not terms:
            return
        max_limit = Config.REGEX_VARIANTS_LIMIT  # 8000
        regex_mode = 'variants_maximum' if mode == 'fuzzy' else mode
        for term in terms:
            if term.upper() in ['AND', 'OR', 'NOT', '(', ')']:
                continue
            # Pre-compute at the larger limit; Tantivy phase will slice from cache
            self.var_mgr.get_variants(term, mode, limit=max_limit)
            if regex_mode != mode:
                self.var_mgr.get_variants(term, regex_mode, limit=max_limit)

    def _build_local_responsa_query_and_regex(self, query_str, mode, gap, responsa_options):
        """Build a Tantivy query string + filter regex for a Responsa query, for
        use against the LOCAL My-Library index.

        Mirrors the component expansion the main (genizah) path runs inline in
        execute_search — grammatical ``#prefix``/``suffix#``, ``*`` wildcards,
        ``%`` plene/defective, ``(a/b)`` alternation, Judeo-Arabic + spelling-
        variant expansion — but is self-contained so it never disturbs the
        load-bearing main search path. It reuses the SAME index-agnostic
        ``build_tantivy_query`` / ``build_regex_pattern`` the main path uses, so
        the returned query string drops straight onto ``local_index.parse_query``.

        Closes the Phase-95 MEDIUM-1 deferral (95-05): LOCAL search used a
        simplified ``parse_query`` that stripped operator metacharacters, so
        Responsa queries returned nothing. The line-break (``|``) operator is NOT
        handled here — it runs through the separate ``_execute_line_break_search``
        (main index only); callers get ``(None, None)`` and fall back.

        Parity with the main path is pinned by
        ``tests/test_local_reload_after_refresh.py::test_query_semantics_*``.

        Returns ``(t_query_str, regex)``, or ``(None, None)`` when the query can't
        be expressed for LOCAL (no components, line-break query, or no positive
        components) so the caller falls back to the simplified path.
        """
        components = parse_responsa_query(query_str)
        if not components:
            return None, None
        # Line-break ('|') is a separate, main-index-only path. Bail to fallback.
        line_groups, _ = _parse_line_break_query(query_str)
        if line_groups is not None:
            return None, None
        per_pair_gaps = extract_per_pair_gaps(query_str)

        variants_on = responsa_options.get('variants', False)
        ja_on = responsa_options.get('ja', False)
        flex_spacing = responsa_options.get('flex_spacing', False)
        variant_mode = responsa_options.get('variant_mode', 'exact')

        # Rewrite *word* -> #word# (both-side wildcard = prefix+suffix expansion;
        # true substring search isn't expressible in Tantivy). Mirrors main path.
        for comp in components:
            if (comp.wildcard == 'pattern' and comp.wildcard_pattern
                    and comp.wildcard_pattern.startswith('*')
                    and comp.wildcard_pattern.endswith('*')
                    and comp.wildcard_pattern.count('*') == 2):
                stem = comp.wildcard_pattern.strip('*')
                if stem:
                    comp.words = [stem]
                    comp.wildcard = None
                    comp.wildcard_pattern = None
                    comp.grammatical_prefixes = True
                    comp.grammatical_suffixes = True

        # Explosion guard (same as main path) so a huge LOCAL expansion can't hang.
        _components, _guard_warning, actual_opts = _apply_explosion_guard(
            components, variants_on=variants_on, ja_on=ja_on,
            var_mgr=self.var_mgr, variant_mode=variant_mode,
        )
        if _guard_warning:
            variants_on = actual_opts['variants_on']
            ja_on = actual_opts['ja_on']
            variant_mode = actual_opts['variant_mode']

        positive_components = [c for c in components if not c.negated]
        if not positive_components:
            return None, None

        component_dicts = []
        for comp in positive_components:
            expanded_words = list(comp.words)
            if comp.plene_defective:
                pe = []
                for w in expanded_words:
                    pe.extend(expand_plene_defective(w))
                expanded_words = list(dict.fromkeys(pe))
            if comp.grammatical_prefixes:
                pe = []
                for w in expanded_words:
                    pe.extend(expand_grammatical_prefixes(w))
                expanded_words = list(dict.fromkeys(pe))
            if comp.grammatical_suffixes:
                se = []
                for w in expanded_words:
                    se.extend(expand_grammatical_suffixes(w))
                expanded_words = list(dict.fromkeys(se))
            if ja_on:
                je = []
                for w in expanded_words:
                    je.extend(expand_judeo_arabic(w))
                expanded_words = list(dict.fromkeys(je))
            if variants_on and self.var_mgr:
                ve = []
                for w in expanded_words:
                    try:
                        ve.extend(self.var_mgr.get_variants(w, variant_mode, limit=200))
                    except Exception:
                        ve.append(w)
                expanded_words = list(dict.fromkeys(ve))
            flex_patterns = []
            if flex_spacing:
                for w in comp.words:
                    flex_patterns.append(_make_flex_spacing_pattern(w))
            component_dicts.append({
                'tantivy_terms': expanded_words,
                'regex_terms': expanded_words,
                'original_words': comp.words,
                'wildcard': comp.wildcard,
                'wildcard_pattern': comp.wildcard_pattern,
                'flex_patterns': flex_patterns,
                'inline_pattern': comp.inline_pattern,
            })

        if not component_dicts:
            return None, None
        t_query_str = self.build_tantivy_query(
            terms=None, mode=mode,
            responsa_components=component_dicts, responsa_options=responsa_options,
        )
        regex = self.build_regex_pattern(
            terms=None, mode=mode, max_gap=gap,
            responsa_components=component_dicts, responsa_options=responsa_options,
            per_pair_gaps=per_pair_gaps,
        )
        return t_query_str, regex

    def build_tantivy_query(self, terms, mode, responsa_components=None, responsa_options=None,
                            content_search_field=None):
        # SEED-006 Stage 2: when *content_search_field* is supplied (only by the
        # plain word-search + composition retrieval sites, NOT position /
        # line-break / responsa), each term gets an extra lower-weighted
        # ``content_search:"<diacritic-folded>"`` OR-clause so צמאן / צ'מאן reach
        # the corpus form צ̇מאן. The exact-content ^5 boost is preserved, so a
        # doc carrying the original form still ranks above a fold-only match.
        # --- Responsa branch ---
        if responsa_components is not None:
            flex_spacing = responsa_options.get('flex_spacing', False) if responsa_options else False
            parts = []
            for comp in responsa_components:
                tantivy_terms = comp.get('tantivy_terms', [])
                original_words = set(comp.get('original_words', []))
                if not tantivy_terms:
                    continue

                clean_vars = []
                seen = set()
                for t in tantivy_terms:
                    t_clean = t.replace('"', '')
                    if not t_clean or t_clean in seen:
                        continue
                    seen.add(t_clean)

                    if t_clean in original_words:
                        # Original / exact term gets highest boost
                        clean_vars.append(f'"{t_clean}"^5')
                    elif any(len(t_clean) != len(ow) for ow in original_words):
                        # Different length from any original -> medium boost
                        clean_vars.append(f'"{t_clean}"^3')
                    else:
                        clean_vars.append(f'"{t_clean}"')

                # For suffix wildcards, expand with grammatical suffixes so Tantivy
                # finds documents containing derived forms (e.g., שלומו for שלום*).
                # Also add sofit-converted stem as fallback.
                wildcard = comp.get('wildcard')
                if wildcard == 'suffix':
                    for w in comp.get('original_words', []):
                        for sfx in expand_grammatical_suffixes(w):
                            if sfx not in seen:
                                seen.add(sfx)
                                clean_vars.append(f'"{sfx}"')
                        if w and w[-1] in _SOFIT_TO_NORMAL:
                            converted = w[:-1] + _SOFIT_TO_NORMAL[w[-1]]
                            if converted not in seen:
                                seen.add(converted)
                                clean_vars.append(f'"{converted}"')
                elif wildcard == 'prefix':
                    for w in comp.get('original_words', []):
                        for pfx in expand_grammatical_prefixes(w):
                            if pfx not in seen:
                                seen.add(pfx)
                                clean_vars.append(f'"{pfx}"')

                # Bracket variants: add bracket-adorned forms of each
                # original word so Tantivy returns bracketed tokens.
                for w in comp.get('original_words', []):
                    for bv in _add_bracket_variants(w):
                        if bv != w and bv not in seen:
                            seen.add(bv)
                            clean_vars.append(f'"{bv}"')

                # Flex spacing: add split alternatives so Tantivy finds
                # documents where a word appears with spaces (e.g., "בן דוד"
                # for query "בןדוד"). Each split produces an AND pair;
                # regex phase then verifies adjacency.
                flex_split_clauses = []
                if flex_spacing:
                    for w in comp.get('original_words', []):
                        if len(w) >= 3:  # only split words with 3+ chars
                            for i in range(1, len(w)):
                                left, right = w[:i], w[i:]
                                if len(left) >= 1 and len(right) >= 1:
                                    flex_split_clauses.append(f'("{left}" AND "{right}")')

                if clean_vars or flex_split_clauses:
                    all_alternatives = clean_vars + flex_split_clauses
                    parts.append(f'({" OR ".join(all_alternatives)})')

            return " AND ".join(parts)

        # --- Existing path (unchanged) ---
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

            # SEED-006 P2: a raw ASCII double-quote inside a term (the typed
            # gershayim substitute, e.g. רמב"ם) would break the quoted Tantivy
            # clauses below (f'"{term}"' -> "רמב"ם" = a parse error). The normal
            # entry points already fold it via strip_search_diacritics, but
            # sanitize defensively here too so direct callers (tests/API) are
            # safe and the exact clause matches the diacritic-folded variants.
            term = term.replace('"', '')
            if not term:
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
                seen_vars = set()

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

                # Bracket variants: add bracket-adorned forms so Tantivy
                # returns documents where the term appears with scholarly
                # brackets (e.g., ]הנתשנ for query הנתשנ).
                for bv in _add_bracket_variants(term):
                    if bv != term and bv not in seen_vars:
                        clean_vars.append(f'"{bv}"')
                        seen_vars.add(bv)

                # SEED-006 Stage 2: lower-weighted diacritic-folded fallback so a
                # query like צמאן / צ'מאן reaches the corpus form צ̇מאן (U+0307).
                # Explicit field clause (works without content_search being in the
                # parse_query default list — verified on tantivy 0.25); strip
                # quotes/diacritics defensively. ^0.5 keeps it below the ^5 exact.
                if content_search_field:
                    cs_term = strip_search_diacritics(term).replace('"', '')
                    if cs_term:
                        clean_vars.append(f'{content_search_field}:"{cs_term}"^0.5')

                parts.append(f'({" OR ".join(clean_vars)})')

        return " AND ".join(parts)

    def build_regex_pattern(self, terms, mode, max_gap, responsa_components=None, responsa_options=None, per_pair_gaps=None):
        # --- Responsa branch ---
        if responsa_components is not None:
            opts = responsa_options or {}
            flex_spacing = opts.get('flex_spacing', False)
            bidirectional = opts.get('bidirectional', False)

            parts = []
            for comp in responsa_components:
                wildcard = comp.get('wildcard')
                inline_pattern = comp.get('inline_pattern')

                if inline_pattern:
                    # Inline alternation: word(a/b)end
                    part = _expand_inline_alternation(inline_pattern)
                    parts.append(f"({part})")
                    continue

                if wildcard == 'pattern':
                    # Character pattern: *a*b*c*
                    part = _build_wildcard_regex(comp)
                    parts.append(f"({part})")
                    continue

                if wildcard in ('suffix', 'prefix'):
                    # Wildcard suffix/prefix
                    part = _build_wildcard_regex(comp)
                    if part:
                        parts.append(part)
                    continue

                # Regular terms: build alternation group
                regex_terms = comp.get('regex_terms', [])
                flex_patterns = comp.get('flex_patterns', [])

                all_alternatives = []

                # Add regex terms (sorted by length descending, escaped)
                unique_terms = sorted(set(regex_terms), key=len, reverse=True)
                for t in unique_terms:
                    all_alternatives.append(make_mark_tolerant_pattern(re.escape(t)))

                # Add flex spacing patterns (already regex, NOT escaped)
                if flex_spacing and flex_patterns:
                    for fp in flex_patterns:
                        if fp not in all_alternatives:
                            all_alternatives.append(fp)

                if all_alternatives:
                    parts.append(f"({'|'.join(all_alternatives)})")

            if not parts:
                return None

            def _make_sep_for_gap(gap_val, flex):
                """Build regex separator for a specific gap value."""
                if gap_val == 0:
                    if flex:
                        return r'[^\w\u0590-\u05FF\']*'
                    else:
                        return r'[^\w\u0590-\u05FF\']+'
                else:
                    return rf'(?:[^\w\u0590-\u05FF\']+{Config.WORD_TOKEN_PATTERN}){{0,{gap_val}}}[^\w\u0590-\u05FF\']+'

            def _join_parts_with_gaps(parts_list, gaps_list, default_gap, flex):
                """Join regex parts with per-pair or uniform gap separators."""
                if len(parts_list) == 1:
                    return parts_list[0]
                result = parts_list[0]
                for i in range(1, len(parts_list)):
                    gap = gaps_list[i-1] if gaps_list and i-1 < len(gaps_list) and gaps_list[i-1] is not None else default_gap
                    result += _make_sep_for_gap(gap, flex) + parts_list[i]
                return result

            forward = _join_parts_with_gaps(parts, per_pair_gaps, max_gap, flex_spacing)

            if bidirectional and len(parts) >= 2:
                reversed_gaps = list(reversed(per_pair_gaps)) if per_pair_gaps else per_pair_gaps
                backward = _join_parts_with_gaps(list(reversed(parts)), reversed_gaps, max_gap, flex_spacing)
                pattern_str = f"({forward})|({backward})"
            else:
                pattern_str = forward

            try:
                return re.compile(pattern_str, re.IGNORECASE)
            except Exception:
                return None  # Boundary data unavailable for this document

        # --- Existing path (unchanged) ---
        if mode == 'Regex':
            try: return re.compile(" ".join(terms), re.IGNORECASE)
            except re.error: return None

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
            escaped = [make_mark_tolerant_pattern(re.escape(v)) for v in unique_vars]

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
        except re.error:
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
            # For UI Table: Replace newlines with ‖ line-break indicator
            return hl_snippet.replace('\n', ' \u2016 ')

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
            return hl_snippet.replace('\n', ' \u2016 ')
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
                uid_val = '?'  # UID extraction failed; use placeholder for warning message
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
            return default  # Key missing or type mismatch; caller gets default value

    def _get_best_text_for_id(self, sys_id):
        """Find the first page with meaningful text for a given System ID."""
        if not self.searcher: return "", "", "", ""

        # Query index for all pages of this manuscript
        try:
            q = self.index.parse_query(f'full_header:"{sys_id}"', ["full_header"])
            # Fetch enough docs to cover a manuscript
            res = self.searcher.search(q, 2000)
        except (ValueError, RuntimeError):
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
            except (ValueError, TypeError): p_num = 999999

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

    def parse_query_syntax(self, query, responsa_mode=False):
        """
        Parses search syntax prefix from query string.
        Returns (mode, clean_query). If no prefix, returns (None, query).

        When responsa_mode=True, all prefix shortcuts are bypassed and the raw
        query is returned unchanged. This prevents '#' from being interpreted as
        Shelfmark when Responsa mode is active.

        Prefixes:
        - ??? = variants_maximum (top 150 pairs)
        - ?? = variants_extended (top 70 pairs)
        - ? = variants (top 30 pairs, basic)
        - = = exact
        - ~ = fuzzy
        - / = Regex
        - $ = Title
        - # = Shelfmark
        - R = responsa
        """
        if responsa_mode:
            return None, query
        if not query: return None, ""

        # Check multi-character prefixes first (order matters: longer prefixes first)
        prefix_map = [
            ('???', 'variants_maximum'),
            ('??', 'variants_extended'),
            ('?', 'variants'),
            ('=', 'exact'),
            ('~', 'fuzzy'),
            ('/', 'Regex'),
            ('$', 'Title'),
            ('#', 'Shelfmark'),
            ('R', 'responsa'),
        ]

        for prefix, mode in prefix_map:
            if query.startswith(prefix):
                clean = query[len(prefix):].lstrip()
                if clean:
                    return mode, clean

        return None, query

    def _expand_responsa_component(self, comp, responsa_options):
        """Expand a single ResponsaComponent through the full Responsa expansion pipeline.

        Returns list of expanded words (lowercase strings).
        """
        variants_on = responsa_options.get('variants', False)
        ja_on = responsa_options.get('ja', False)
        variant_mode = responsa_options.get('variant_mode', 'exact')

        expanded = list(comp.words)

        if comp.plene_defective:
            plene = []
            for w in expanded:
                plene.extend(expand_plene_defective(w))
            expanded = list(dict.fromkeys(plene))

        if comp.grammatical_prefixes:
            pfx = []
            for w in expanded:
                pfx.extend(expand_grammatical_prefixes(w))
            expanded = list(dict.fromkeys(pfx))

        if comp.grammatical_suffixes:
            sfx = []
            for w in expanded:
                sfx.extend(expand_grammatical_suffixes(w))
            expanded = list(dict.fromkeys(sfx))

        if ja_on:
            ja = []
            for w in expanded:
                ja.extend(expand_judeo_arabic(w))
            expanded = list(dict.fromkeys(ja))

        if variants_on and self.var_mgr:
            var = []
            for w in expanded:
                try:
                    var.extend(self.var_mgr.get_variants(w, variant_mode, limit=200))
                except Exception:
                    var.append(w)  # Variant expansion failed for this word; use original
            expanded = list(dict.fromkeys(var))

        return expanded

    @staticmethod
    def _build_line_break_regex(line_groups, line_gaps, expanded_groups):
        """Build a regex pattern for line-break search.

        Each group becomes a line pattern, joined by newline separators with gap support.
        Returns compiled regex or None.
        """
        def _line_word_sep(gap):
            """Separator between two words on the SAME line (never crosses \\n).

            gap None/0 -> adjacent words (whitespace only). gap N>0 -> up to N
            intervening words on the line (CR HIGH-6 — [N] word gaps in line mode).
            """
            if not gap:
                return r'[^\S\n]+'
            return r'(?:[^\S\n]+\S+){0,' + str(int(gap)) + r'}[^\S\n]+'

        line_patterns = []
        for gi, group in enumerate(line_groups):
            # Build per-component alternatives, tracking the [N] word gap that
            # precedes each part so the separators below can honor it.
            comp_parts = []  # list of (pattern, gap_before)
            grp_word_gaps = getattr(group, 'word_gaps', None) or []
            for ci, word_set in enumerate(expanded_groups[gi]):
                gap_before = (
                    grp_word_gaps[ci - 1]
                    if ci > 0 and (ci - 1) < len(grp_word_gaps)
                    else None
                )
                # Check if component has a wildcard — dispatch to wildcard regex builder
                comp = group.components[ci] if ci < len(group.components) else None
                pat = None
                if comp and comp.wildcard:
                    comp_dict = {
                        'wildcard': comp.wildcard,
                        'wildcard_pattern': comp.wildcard_pattern,
                        'regex_terms': sorted(word_set, key=len, reverse=True),
                        'original_words': comp.words,
                    }
                    pat = _build_wildcard_regex(comp_dict)
                # Plain word path (non-wildcard or wildcard fallback)
                if pat is None:
                    sorted_words = sorted(word_set, key=len, reverse=True)
                    escaped = [make_mark_tolerant_pattern(re.escape(w)) for w in sorted_words]
                    if not escaped:
                        continue
                    pat = f"({'|'.join(escaped)})"
                comp_parts.append((pat, gap_before))

            if not comp_parts:
                continue

            # Build line pattern based on position constraints
            is_first_start = group.line_start
            is_last_end = group.line_end

            if len(comp_parts) == 1:
                word_pat = comp_parts[0][0]
            else:
                # Multiple words on same line: join with per-pair gap separators
                word_pat = comp_parts[0][0]
                for pat, gap_before in comp_parts[1:]:
                    word_pat += _line_word_sep(gap_before) + pat

            if is_first_start and is_last_end:
                # Entire line must be just these words
                line_pat = r'^\s*' + word_pat + r'\s*$'
            elif is_first_start:
                line_pat = r'^\s*' + word_pat + r'.*$'
            elif is_last_end:
                line_pat = r'^.*' + word_pat + r'\s*$'
            else:
                # Word anywhere on line
                line_pat = r'^.*' + word_pat + r'.*$'

            line_patterns.append(line_pat)

        if not line_patterns:
            return None

        # Join line patterns with newline separators respecting gaps
        parts = [line_patterns[0]]
        for i in range(1, len(line_patterns)):
            gap = line_gaps[i - 1] if i - 1 < len(line_gaps) and line_gaps[i - 1] is not None else 0
            if gap == 0:
                parts.append(r'\n')
            else:
                # Skip exactly `gap` lines
                parts.append(r'\n(?:.*\n){' + str(gap) + r'}')
            parts.append(line_patterns[i])

        pattern_str = ''.join(parts)
        try:
            return re.compile(pattern_str, re.IGNORECASE | re.MULTILINE)
        except re.error as e:
            LOGGER.warning("Line-break regex failed to compile: %s", e)
            return None

    def _execute_line_break_search(self, line_groups, line_gaps, query_str,
                                    responsa_options=None, progress_callback=None,
                                    exclude_words=None, restrict_sys_ids=None,
                                    text_position=None):
        """Execute a line-break search using | syntax.

        Uses regex for both matching and highlighting (fast + correct highlights).
        """
        if not self.searcher:
            return []
        opts = responsa_options or {}

        # CR HIGH-5: the whole-query Text Position 'line_start'/'line_end' applies
        # to the FIRST / LAST line group in a line-break query. The line-break path
        # previously ignored text_position entirely, so the dropdown silently did
        # nothing once a query became multi-line.
        if text_position == 'line_start' and line_groups:
            line_groups[0].line_start = True
        elif text_position == 'line_end' and line_groups:
            line_groups[-1].line_end = True

        # Expand each component in each group
        expanded_groups = []
        tantivy_parts = []
        for group in line_groups:
            group_expanded = []
            for comp in group.components:
                expanded = self._expand_responsa_component(comp, opts)
                word_set = {w.lower() for w in expanded}
                group_expanded.append(word_set)

                is_first = (comp == group.components[0])
                is_last = (comp == group.components[-1])

                if is_first and group.line_start:
                    field = 'line_starts'
                elif is_last and group.line_end:
                    field = 'line_ends'
                else:
                    field = 'content'

                # Wildcard components: use content field (not positional)
                # since the matched word may differ from the base stem.
                # Expand with grammatical suffixes/prefixes for Tantivy recall.
                if comp.wildcard in ('suffix', 'prefix', 'pattern'):
                    wc_terms = []
                    for w in comp.words:
                        wc_terms.append(f'content:"{w}"')
                        if comp.wildcard == 'suffix':
                            for sfx in expand_grammatical_suffixes(w):
                                wc_terms.append(f'content:"{sfx}"')
                            if w and w[-1] in _SOFIT_TO_NORMAL:
                                converted = w[:-1] + _SOFIT_TO_NORMAL[w[-1]]
                                wc_terms.append(f'content:"{converted}"')
                        elif comp.wildcard == 'prefix':
                            for pfx in expand_grammatical_prefixes(w):
                                wc_terms.append(f'content:"{pfx}"')
                    tantivy_parts.append(f'({" OR ".join(wc_terms)})')
                else:
                    # Use expanded terms (plene, grammatical, JA, variants)
                    # for better Tantivy recall, matching the normal path strategy.
                    # Boost original words higher for scoring.
                    original_set = set(comp.words)
                    tantivy_clause_terms = []
                    seen = set()
                    for w in expanded:
                        w_clean = w.replace('"', '')
                        if not w_clean or w_clean in seen:
                            continue
                        seen.add(w_clean)
                        if w_clean in original_set:
                            tantivy_clause_terms.append(f'{field}:"{w_clean}"^5')
                        else:
                            tantivy_clause_terms.append(f'{field}:"{w_clean}"')
                    if tantivy_clause_terms:
                        tantivy_parts.append(f'({" OR ".join(tantivy_clause_terms)})')

            expanded_groups.append(group_expanded)

        if not tantivy_parts:
            return []

        # Build regex for matching + highlighting
        regex = SearchEngine._build_line_break_regex(line_groups, line_gaps, expanded_groups)
        if not regex:
            return []

        t_query_str = " AND ".join(tantivy_parts)
        pattern_str = regex.pattern
        LOGGER.debug(f"Line-break search, Tantivy: {t_query_str[:500]}")
        LOGGER.debug(f"Line-break regex: {pattern_str[:500]}")

        if restrict_sys_ids is not None and len(restrict_sys_ids) <= 500:
            sid_clauses = ' OR '.join(f'full_header:"{sid}"' for sid in restrict_sys_ids)
            t_query_str = f'({t_query_str}) AND ({sid_clauses})'

        try:
            query = self.index.parse_query(t_query_str, ['content'])
            res_obj = self.searcher.search(query, Config.SEARCH_LIMIT)
        except Exception as e:
            LOGGER.warning("Line-break search query failed: %s", e)
            return []

        hits = res_obj.hits if hasattr(res_obj, 'hits') else res_obj
        total_hits = len(hits)
        LOGGER.debug(f"Line-break Tantivy returned {total_hits} hits")

        restrict_uids = None
        if restrict_sys_ids is not None:
            browse_map = self._load_browse_map()
            restrict_uids = set()
            for sid in restrict_sys_ids:
                for page in browse_map.get(sid, []):
                    restrict_uids.add(page['uid'])

        results = []
        regex_filtered = 0
        was_interrupted = False

        try:
            for i, (score, doc_addr) in enumerate(hits):
                if progress_callback and i % 5 == 0:
                    progress_callback(i, total_hits)
                try:
                    doc = self.searcher.doc(doc_addr)

                    if restrict_uids is not None:
                        if doc['unique_id'][0] not in restrict_uids:
                            continue

                    content = self._get_field(doc, 'content', [""])[0]

                    # Bracket handling: strip brackets for bracket-free queries
                    match_content = content if _query_has_brackets(query_str) else _strip_brackets(content)

                    # Regex match — fast filter + provides highlight span
                    match_obj = regex.search(match_content)
                    if not match_obj:
                        regex_filtered += 1
                        continue

                    # Re-search on original content for highlighting
                    if match_content is not content:
                        orig_match = regex.search(content)
                        if orig_match:
                            match_obj = orig_match

                    # Text position filter — strip brackets from
                    # prefix/suffix only for bracket-free queries
                    _brackets_in_query = _query_has_brackets(query_str)
                    if text_position == 'start' and match_obj.start() > 0:
                        prefix = content[:match_obj.start()]
                        cleaned = prefix.strip() if _brackets_in_query else _strip_brackets(prefix).strip()
                        if cleaned:
                            regex_filtered += 1
                            continue
                    elif text_position == 'end' and match_obj.end() < len(content):
                        suffix = content[match_obj.end():]
                        cleaned = suffix.strip() if _brackets_in_query else _strip_brackets(suffix).strip()
                        if cleaned:
                            regex_filtered += 1
                            continue

                    # Use standard highlight helpers with the match span
                    span = match_obj.span()
                    scope_list = self._get_field(doc, 'scope', ['page']) or ['page']
                    scope = scope_list[0]
                    boundaries = self._parse_boundaries(doc) if scope != 'page' else []

                    hl_c = self.highlight(content, regex, False)
                    hl_f = self.highlight(content, regex, True)

                    if boundaries:
                        span_map = self._map_span_to_pages(span, boundaries)
                        primary = span_map.get('primary') or {}
                        display_header = primary.get('full_header', doc['full_header'][0])
                        source_label = primary.get('source', doc['source'][0])
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
                        if hl_c:
                            meta = self.meta_mgr.get_display_data(doc['full_header'][0], doc['source'][0])
                            results.append({
                                'display': meta, 'snippet': hl_c, 'full_text': content,
                                'uid': doc['unique_id'][0], 'raw_header': doc['full_header'][0],
                                'raw_file_hl': hl_f, 'highlight_pattern': pattern_str,
                                'scope': scope
                            })

                except Exception as e:
                    LOGGER.warning("Line-break search: failed to process hit %s: %s", i, e)
        except InterruptedError:
            was_interrupted = True

        LOGGER.debug(f"Line-break search: {len(results)} results, filtered: {regex_filtered}, interrupted: {was_interrupted}")
        deduped = self._deduplicate(results)

        if exclude_words and deduped:
            filtered = []
            for r in deduped:
                text_content = (r.get('snippet', '') + ' ' + r.get('full_text', '')).lower()
                should_exclude = any(w.lower() in text_content for w in exclude_words)
                if not should_exclude:
                    filtered.append(r)
            deduped = filtered

        return deduped

    def _execute_metadata_search(self, query_str, mode, progress_callback=None, restrict_sys_ids=None):
        """Search by title or shelfmark via csv_bank. Returns results even for metadata-only records."""
        if mode != 'Regex':
            query_str = strip_search_diacritics(query_str)

        field_map = {'Title': 'title', 'Shelfmark': 'shelfmark'}
        target_field = field_map.get(mode)

        # Wildcard or empty query with restrict set: return all restricted IDs
        stripped = query_str.strip()
        if (stripped == '*' or stripped == '') and restrict_sys_ids:
            sys_ids = sorted(restrict_sys_ids)
        else:
            sys_ids = self.meta_mgr.search_by_meta(query_str, target_field)
            if restrict_sys_ids is not None:
                sys_ids = [s for s in sys_ids if s in restrict_sys_ids]
        results = []
        total_ids = len(sys_ids)

        # Wrapped so a cancel returns what was gathered instead of nothing.
        # This was the ONLY search loop without it: the raise escaped all the way
        # to SearchThread's `except InterruptedError` -> results_signal.emit([]),
        # so a stopped Title/Shelfmark search threw away every row it had, while
        # every other mode returned partial results.
        try:
            for i, sid in enumerate(sys_ids):
                if progress_callback and i % 5 == 0:
                    progress_callback(i, total_ids)

                # Try to get transcription text from Tantivy
                text, head, src, uid = '', '', '', ''
                if self.searcher:
                    text, head, src, uid = self._get_best_text_for_id(sid)

                metadata_only = not text

                if metadata_only:
                    # Build display from csv_bank metadata (no Tantivy needed)
                    meta_info = self.meta_mgr.get_meta_for_id(sid)
                    if isinstance(meta_info, tuple):
                        # get_meta_for_id returns (shelfmark, title) tuple
                        shelfmark, title = meta_info
                        meta_info = {'shelfmark': shelfmark, 'title': title}
                    display = {
                        'shelfmark': meta_info.get('shelfmark', f'ID: {sid}'),
                        'title': meta_info.get('title', ''),
                        'img': '',
                        'source': '',
                        'id': sid,
                        'library_code': meta_info.get('library_code', '') if isinstance(meta_info, dict) else '',
                    }
                    # For tuple returns, get library_code from csv_bank directly
                    if isinstance(meta_info, dict) and not display['library_code']:
                        display['library_code'] = self.meta_mgr.get_library_for_id(sid)
                    elif not display['library_code']:
                        display['library_code'] = self.meta_mgr.get_library_for_id(sid)
                    results.append({
                        'display': display,
                        'snippet': '',
                        'full_text': '',
                        'uid': '',
                        'raw_header': '',
                        'raw_file_hl': '',
                        'highlight_pattern': None,
                        'metadata_only': True,
                    })
                else:
                    meta = self.meta_mgr.get_display_data(head, src or "V0.8")
                    snippet = text[:300] + "..." if len(text) > 300 else text
                    results.append({
                        'display': meta,
                        'snippet': snippet,
                        'full_text': text,
                        'uid': uid,
                        'raw_header': head,
                        'raw_file_hl': text,
                        'highlight_pattern': None,
                        'metadata_only': False,
                    })

        except InterruptedError:
            pass  # cancelled mid-scan — fall through to the sort and return partials

        results.sort(key=lambda r: natural_sort_key(r.get('display', {}).get('shelfmark', '')))
        return results

    def execute_search(self, query_str, mode, gap, progress_callback=None, exclude_words=None, responsa_options=None, restrict_sys_ids: set = None, text_position: str = None, corpus_scope: str = "all", phase_callback=None):
        search_started = time.perf_counter()
        # R2-#1: discard any stale per-thread downgrade signal from a prior
        # invocation (e.g., a prior request that crashed before consuming).
        # Keeps the signal one-shot per execute_search call, so it cannot
        # leak across requests on the same worker thread.
        _consume_last_responsa_downgrade()
        # Phase 81A (Codex MEDIUM-2) — drain the structured-meta channel
        # symmetrically so a direct-core caller cannot leave a stale meta
        # dict that a later web request would read as "the cascade fired."
        _consume_last_responsa_downgrade_meta()
        # --- Metadata Search Modes (csv_bank-backed, no Tantivy needed) ---
        if mode in ['Title', 'Shelfmark']:
            return self._execute_metadata_search(query_str, mode, progress_callback, restrict_sys_ids)

        # Phase 95 smoke-fix (item 2): corpus_scope gates which index is queried.
        # 'local' → query LOCAL side-index only (skip Genizah Tantivy).
        # 'genizah' → query Genizah only (LOCAL merge block below is skipped).
        # 'all' → current behavior: Genizah + LOCAL merged via RRF.
        if corpus_scope == "local":
            if getattr(self, "local_searcher", None) is None:
                return []
            try:
                # SEED-006: strip diacritics here (the 'all' path strips at the
                # shared site below) so the LOCAL regex is built mark-tolerant
                # AND content_search retrieval folds צ'מאן -> צמאן. Mirrors the
                # main path; Regex mode is left untouched (user owns the pattern).
                if mode != 'Regex':
                    query_str = strip_search_diacritics(query_str)
                # Phase 95-05 follow-up: Responsa operators (#/*/%/(a/b)) need the
                # full component expansion; the simplified path below strips them and
                # returns nothing. Build the same candidate query + components-aware
                # regex the main index uses, then run it against LOCAL. Line-break (|)
                # is main-index only -> helper returns None -> simplified fallback.
                if responsa_options and responsa_options.get('responsa_mode'):
                    _resp_q, _resp_regex = self._build_local_responsa_query_and_regex(
                        query_str, mode, gap, responsa_options
                    )
                    if _resp_q is not None and _resp_regex is not None:
                        return self._query_local_index(
                            query_str, mode, gap,
                            regex=_resp_regex, tantivy_query_str=_resp_q,
                            progress_callback=progress_callback,
                            phase_callback=phase_callback,
                        )
                # Phase 96 D-F5: build regex here so LOCAL-only path also gets
                # D-04.1 filter-out + highlight_pattern, same as the RRF merge path.
                if mode == 'Regex':
                    _local_terms = [query_str]
                else:
                    _local_terms = query_str.split()
                _local_regex = self.build_regex_pattern(_local_terms, mode, gap)
                return self._query_local_index(
                    query_str, mode, gap, regex=_local_regex or None,
                    progress_callback=progress_callback,
                    phase_callback=phase_callback,
                )
            except Exception as _le:
                LOGGER.warning("LOCAL-only search failed: %r", _le)
                return []

        if not self.searcher: return []
        _line_constraints = {}  # Per-line position constraints (L3:word syntax)
        _has_wildcard_component = False  # Set True when any Responsa component has a wildcard

        # Strip combining diacritical marks and geresh/gershayim from query
        # Skip for Regex mode -- user controls the pattern directly
        if mode != 'Regex':
            query_str = strip_search_diacritics(query_str)

        # --- Responsa Pipeline ---
        responsa_warning = None
        # Phase 95-05: the clean (pre-restrict) Responsa candidate query, captured
        # so the LOCAL RRF merge can reuse it without re-parsing/expanding (keeps the
        # single parse_responsa_query/build_regex_pattern contract the tests pin).
        # Stays None for non-Responsa searches.
        _local_responsa_query = None
        if responsa_options and responsa_options.get('responsa_mode'):
            # a. Bypass prefix shortcuts
            self.parse_query_syntax(query_str, responsa_mode=True)

            # b. Parse Responsa query into components
            components = parse_responsa_query(query_str)
            if not components:
                return []

            # b2. Extract per-pair gap values from [N] tokens
            per_pair_gaps = extract_per_pair_gaps(query_str)

            # b3. Check for line-break syntax (| separators)
            line_groups, line_gaps = _parse_line_break_query(query_str)
            if line_groups is not None:
                return self._execute_line_break_search(
                    line_groups, line_gaps, query_str,
                    responsa_options=responsa_options,
                    progress_callback=progress_callback,
                    exclude_words=exclude_words,
                    restrict_sys_ids=restrict_sys_ids,
                    text_position=text_position,
                )

            # c. Expand each component
            variants_on = responsa_options.get('variants', False)
            ja_on = responsa_options.get('ja', False)
            flex_spacing = responsa_options.get('flex_spacing', False)
            bidirectional = responsa_options.get('bidirectional', False)
            variant_mode = responsa_options.get('variant_mode', 'exact')

            # Rewrite simple *word* patterns to #word# (prefix+suffix expansion).
            # Both-side wildcards can't be executed as true substring searches in
            # Tantivy, so we convert to grammatical expansion which is more useful.
            rewritten_patterns = []
            for comp in components:
                if (comp.wildcard == 'pattern' and comp.wildcard_pattern
                        and comp.wildcard_pattern.startswith('*')
                        and comp.wildcard_pattern.endswith('*')
                        and comp.wildcard_pattern.count('*') == 2):
                    stem = comp.wildcard_pattern.strip('*')
                    if stem:
                        rewritten_patterns.append(f'*{stem}*')
                        comp.words = [stem]
                        comp.wildcard = None
                        comp.wildcard_pattern = None
                        comp.grammatical_prefixes = True
                        comp.grammatical_suffixes = True
            if rewritten_patterns:
                rewrite_msg = _tr("*word* rewritten as #word# (prefix + suffix expansion)")
                responsa_warning = rewrite_msg

            # Apply explosion guard (estimates before materializing)
            _components, guard_warning, actual_opts = _apply_explosion_guard(
                components,
                variants_on=variants_on,
                ja_on=ja_on,
                var_mgr=self.var_mgr,
                variant_mode=variant_mode,
            )
            if guard_warning:
                responsa_warning = f"{responsa_warning}; {guard_warning}" if responsa_warning else guard_warning
                variants_on = actual_opts['variants_on']
                ja_on = actual_opts['ja_on']
                variant_mode = actual_opts['variant_mode']

            # d. Separate negated components from positive ones
            positive_components = [c for c in components if not c.negated]
            negated_components = [c for c in components if c.negated]

            # Extract negated words and add to exclude_words
            if negated_components:
                negated_word_list = []
                for nc in negated_components:
                    for w in nc.words:
                        negated_word_list.append(w)
                        # Also expand plene/defective for negated words
                        if nc.plene_defective:
                            negated_word_list.extend(expand_plene_defective(w))
                if negated_word_list:
                    if exclude_words is None:
                        exclude_words = []
                    exclude_words = list(exclude_words) + negated_word_list

            if not positive_components:
                return []

            # Build per-component dicts with expanded terms
            component_dicts = []
            for comp in positive_components:
                # Start with component.words
                expanded_words = list(comp.words)

                # 1. Plene/defective expansion (before prefix/suffix to generate base variants)
                if comp.plene_defective:
                    plene_expanded = []
                    for w in expanded_words:
                        plene_expanded.extend(expand_plene_defective(w))
                    expanded_words = list(dict.fromkeys(plene_expanded))  # dedupe preserving order

                # 2. Grammatical prefix expansion
                if comp.grammatical_prefixes:
                    prefix_expanded = []
                    for w in expanded_words:
                        prefix_expanded.extend(expand_grammatical_prefixes(w))
                    expanded_words = list(dict.fromkeys(prefix_expanded))

                # 3. Grammatical suffix expansion
                if comp.grammatical_suffixes:
                    suffix_expanded = []
                    for w in expanded_words:
                        suffix_expanded.extend(expand_grammatical_suffixes(w))
                    expanded_words = list(dict.fromkeys(suffix_expanded))

                # 4. Judeo-Arabic expansion
                if ja_on:
                    ja_expanded = []
                    for w in expanded_words:
                        ja_expanded.extend(expand_judeo_arabic(w))
                    expanded_words = list(dict.fromkeys(ja_expanded))

                # 5. Spelling variants expansion
                if variants_on and self.var_mgr:
                    var_expanded = []
                    for w in expanded_words:
                        try:
                            variants = self.var_mgr.get_variants(w, variant_mode, limit=200)
                            var_expanded.extend(variants)
                        except Exception:
                            var_expanded.append(w)  # Variant expansion failed for this word; use original
                    expanded_words = list(dict.fromkeys(var_expanded))

                # Build flex spacing patterns for original words only
                flex_patterns = []
                if flex_spacing:
                    for w in comp.words:
                        flex_patterns.append(_make_flex_spacing_pattern(w))

                component_dicts.append({
                    'tantivy_terms': expanded_words,
                    'regex_terms': expanded_words,
                    'original_words': comp.words,
                    'wildcard': comp.wildcard,
                    'wildcard_pattern': comp.wildcard_pattern,
                    'flex_patterns': flex_patterns,
                    'inline_pattern': comp.inline_pattern,
                })
                if comp.wildcard:
                    _has_wildcard_component = True

            # Calculate total expanded terms for UI display
            total_expanded = sum(len(cd['tantivy_terms']) for cd in component_dicts)

            # e. Build Tantivy query
            t_query_str = self.build_tantivy_query(
                terms=None, mode=mode,
                responsa_components=component_dicts,
                responsa_options=responsa_options,
            )
            # f. Build regex pattern
            regex = self.build_regex_pattern(
                terms=None, mode=mode, max_gap=gap,
                responsa_components=component_dicts,
                responsa_options=responsa_options,
                per_pair_gaps=per_pair_gaps,
            )
            # Phase 95-05: capture the clean Responsa candidate query for the LOCAL
            # RRF merge below — BEFORE any restrict_sys_ids augmentation (which adds
            # genizah-only full_header clauses that don't apply to LOCAL docs).
            _local_responsa_query = t_query_str
        else:
            # --- Existing path (unchanged) ---
            if mode == 'Regex': terms = [query_str]
            else: terms = query_str.split()

            # Per-line position search: parse L{n}:word constraints
            # e.g. "L1:שלום L3:עליכם" → Tantivy searches positional tokens,
            # regex matches the plain words, post-filter validates line positions
            _line_constraints = {}  # {line_num: word} for post-filter
            if text_position in ('line_start', 'line_end') and mode != 'Regex':
                _lc_pattern = re.compile(r'^L(\d+):(.+)$')
                has_line_prefixes = any(_lc_pattern.match(t) for t in terms)
                if has_line_prefixes:
                    tantivy_parts = []
                    regex_terms = []
                    for t in terms:
                        m = _lc_pattern.match(t)
                        if m:
                            line_num, word = int(m.group(1)), m.group(2)
                            _line_constraints[line_num] = word
                            tantivy_parts.append(f'"{t}"')  # L{n}:word token
                            regex_terms.append(word)         # plain word for regex
                        else:
                            tantivy_parts.append(f'"{t}"')
                            regex_terms.append(t)
                    t_query_str = " AND ".join(tantivy_parts)
                    regex = self.build_regex_pattern(regex_terms, mode, gap)
                    if not regex: return []
                    # Skip the normal build path below
                    terms = None

            if terms is not None:
                # Pre-compute variants at max limit so Tantivy (limit=200) can
                # slice from cache instead of recomputing when regex (limit=8000) runs
                if mode != 'Regex':
                    self._get_or_compute_variants(terms, mode)

                # SEED-006 Stage 2: only fold-fallback for a plain content search.
                # When text_position is set the query is reused against
                # content_head/tail/line_starts/ends, where a full-content
                # content_search clause would defeat the position filter.
                _cs_field = ('content_search'
                             if (not text_position and getattr(self, '_has_content_search', False))
                             else None)
                t_query_str = self.build_tantivy_query(terms, mode, content_search_field=_cs_field)
                regex = self.build_regex_pattern(terms, mode, gap)
        if not regex: return []

        LOGGER.debug(f"Mode: {mode}, Query: {query_str[:200]}")
        LOGGER.debug(f"Tantivy query: {t_query_str[:500]}")
        LOGGER.debug(f"Regex pattern: {regex.pattern[:500]}")

        # Save pattern string for passing to results
        pattern_str = regex.pattern

        # Augment Tantivy query with sys_id filter so the index only returns
        # hits from the restricted manuscripts (avoids iterating 50K hits).
        if restrict_sys_ids is not None and len(restrict_sys_ids) <= 500:
            sid_clauses = ' OR '.join(f'full_header:"{sid}"' for sid in restrict_sys_ids)
            t_query_str = f'({t_query_str}) AND ({sid_clauses})'

        # Choose search field based on text_position filter
        position_field_map = {
            'start': 'content_head',
            'end': 'content_tail',
            'line_start': 'line_starts',
            'line_end': 'line_ends',
        }
        search_field = position_field_map.get(text_position, 'content')

        # Wildcard components with positional fields: fall back to content field.
        # Positional fields only contain exact tokens (first/last words or head/tail),
        # so suffix/prefix wildcards won't match extended forms. The regex +
        # _validate_position_match post-filter handles exact position validation.
        if search_field != 'content' and _has_wildcard_component:
            search_field = 'content'

        tantivy_started = time.perf_counter()
        try:
            query = self.index.parse_query(t_query_str, [search_field])
            res_obj = self.searcher.search(query, Config.SEARCH_LIMIT)
        except Exception as e:
            if text_position and search_field != 'content':
                raise RuntimeError(
                    _tr("Line/position search requires a rebuilt index. Please rebuild the index from Settings to use this feature.")
                ) from e
            LOGGER.warning("Search query failed to parse/execute for pattern %s: %s", t_query_str, e)
            return []

        tantivy_elapsed_ms = (time.perf_counter() - tantivy_started) * 1000.0
        hits = res_obj.hits if hasattr(res_obj, 'hits') else res_obj
        total_hits = len(hits)
        LOGGER.debug(f"Tantivy returned {total_hits} hits")
        results = []
        regex_filtered_count = 0
        was_interrupted = False

        # Pre-compute allowed unique_ids for fast O(1) filtering
        # instead of running parse_header_smart regex on every hit
        restrict_uids = None
        if restrict_sys_ids is not None:
            browse_map = self._load_browse_map()
            restrict_uids = set()
            for sid in restrict_sys_ids:
                for page in browse_map.get(sid, []):
                    restrict_uids.add(page['uid'])

        materialize_started = time.perf_counter()
        try:
            for i, (score, doc_addr) in enumerate(hits):
                if progress_callback and i % 5 == 0:
                    progress_callback(i, total_hits)
                try:
                    doc = self.searcher.doc(doc_addr)

                    # Pre-search filter: skip manuscripts outside the restrict set
                    if restrict_uids is not None:
                        if doc['unique_id'][0] not in restrict_uids:
                            continue

                    content = self._get_field(doc, 'content', [""])[0]
                    scope_list = self._get_field(doc, 'scope', ['page']) or ['page']
                    scope = scope_list[0]

                    # Bracket handling: strip brackets from content for
                    # bracket-free queries so e.g. הנתשנ matches ]הנתשנ
                    match_content = content if _query_has_brackets(query_str) else _strip_brackets(content)

                    # Check for match before any heavy parsing
                    match_obj = regex.search(match_content)
                    if not match_obj:
                        regex_filtered_count += 1
                        continue

                    # Position post-filter: Tantivy uses broad fields (10-word head/tail),
                    # validate exact position (first word, last word, line boundary)
                    if text_position and not Indexer._validate_position_match(match_content, match_obj, text_position, _line_constraints or None, strip_brackets=not _query_has_brackets(query_str)):
                        regex_filtered_count += 1
                        continue

                    # For highlighting, re-search on original content to
                    # preserve scholarly bracket notation in snippets.
                    if match_content is not content:
                        orig_match = regex.search(content)
                        if orig_match:
                            match_obj = orig_match
                        # else: keep match_obj from stripped content; highlight
                        # may be slightly offset but still useful

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
                            'scope': scope,
                            # Phase 77 D-01: surface Tantivy relevance score so
                            # serialize_search_payload emits non-zero scores.
                            'score': float(score),
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
                                'scope': scope,
                                # Phase 77 D-01: Tantivy relevance score for JSON.
                                'score': float(score),
                            })
                except Exception as e:
                    LOGGER.warning("Failed to materialize search hit at position %s: %s", i, e)
        except InterruptedError:
            was_interrupted = True
            LOGGER.debug(f"Search interrupted at hit {i}/{total_hits}, found {len(results)} results so far")

        materialize_elapsed_ms = (time.perf_counter() - materialize_started) * 1000.0
        LOGGER.debug(f"Regex filtered out: {regex_filtered_count}, Results before dedup: {len(results)}, interrupted: {was_interrupted}")
        deduped = self._deduplicate(results)

        # Phase 95 D-08 (Codex P0): LOCAL hits merge AFTER _deduplicate.
        # The dedup body at _deduplicate() whitelists V0.8/V0.7 only and would
        # otherwise DROP LOCAL hits. RRF k=60 used (BM25 IDF from two independent
        # indexes is not comparable; raw score sort would mis-rank — Codex revision).
        # Phase 95 smoke-fix (item 2): skip LOCAL merge when corpus_scope='genizah'.
        local_merge_elapsed_ms = 0.0
        if corpus_scope != "genizah" and getattr(self, "local_searcher", None) is not None:
            local_merge_started = time.perf_counter()
            try:
                # Phase 95-05 follow-up: reuse the main path's already-built,
                # operator-expanded Responsa candidate query (captured above, pre-
                # restrict) so merged LOCAL hits aren't dropped by the simplified
                # path — WITHOUT re-running parse/expand (preserves the single
                # parse_responsa_query/build_regex_pattern call the tests pin). The
                # main `regex` is already components-aware. Non-Responsa: stays None
                # -> simplified path, unchanged. Line-break (|) returned early above.
                if _local_responsa_query is not None:
                    local_hits = self._query_local_index(
                        query_str, mode, gap, regex=regex,
                        tantivy_query_str=_local_responsa_query,
                        progress_callback=progress_callback,
                        phase_callback=phase_callback,
                    )
                else:
                    local_hits = self._query_local_index(
                        query_str, mode, gap, regex=regex,
                        progress_callback=progress_callback,
                        phase_callback=phase_callback,
                    )
            except InterruptedError:
                # Defensive only: _query_local_index now returns its partial hits
                # rather than raising, so a cancel arrives here as a short list and
                # gets merged. Kept so that if it ever raises again, a user cancel
                # still does not get logged below as an index failure — and NOT a
                # re-raise, because the Genizah results already in `deduped` are
                # real and partial is the right semantics at this point.
                local_hits = []
            except Exception as _e:
                LOGGER.warning(
                    "LOCAL side-index query failed; main results unaffected: %r", _e
                )
                local_hits = []
            if local_hits:
                deduped = self._rrf_merge(deduped, local_hits, k=RRF_K)
            local_merge_elapsed_ms = (time.perf_counter() - local_merge_started) * 1000.0
        # End Phase 95 D-08 LOCAL merge.

        # --- Apply Exclusion Filter (NOT Filter) ---
        if exclude_words and deduped:
            filtered = []
            for r in deduped:
                # Combine text fields for checking
                # We check snippet and full_text to be safe
                text_content = (r.get('snippet', '') + ' ' + r.get('full_text', '')).lower()

                # Check if ANY excluded word is present
                should_exclude = False
                for w in exclude_words:
                    if w.lower() in text_content:
                        should_exclude = True
                        break

                if not should_exclude:
                    filtered.append(r)
            deduped = filtered

        LOGGER.debug(f"Results after dedup & filtering: {len(deduped)}")
        LOGGER.info(
            "search_perf mode=%s scope=%s candidates=%d regex_kept=%d final=%d "
            "tantivy_ms=%.0f materialize_ms=%.0f local_merge_ms=%.0f total_ms=%.0f",
            mode, corpus_scope, total_hits, len(results), len(deduped),
            tantivy_elapsed_ms, materialize_elapsed_ms, local_merge_elapsed_ms,
            (time.perf_counter() - search_started) * 1000.0,
        )

        # --- Attach Responsa explosion guard warning to first result ---
        # Phase 78 Concern #6: ALSO record on the thread-local so the
        # /api/search handler can surface the warning even when deduped is
        # empty (the legacy results[0] attachment is preserved as a fallback).
        if responsa_warning:
            _set_last_responsa_downgrade(responsa_warning)
            # Phase 81A — structured per-flag effective state alongside the
            # legacy string channel. The local variables variants_on / ja_on /
            # flex_spacing / bidirectional are bound at lines ~7295-7298 from
            # the input responsa_options dict and are mutated by the cascade
            # at lines ~7332-7334 (variants_on, ja_on; variant_mode is
            # internal-only). flex_spacing and bidirectional are pass-through
            # in 81A scope (cascade tiers 4-6 are deferred).
            _set_last_responsa_downgrade_meta({
                'variants':      bool(variants_on),
                'ja':            bool(ja_on),
                'flex_spacing':  bool(flex_spacing),
                'bidirectional': bool(bidirectional),
            })
        if responsa_warning and deduped:
            deduped[0]['responsa_warning'] = responsa_warning

        # --- Attach Responsa expanded term count to first result ---
        if responsa_options and responsa_options.get('responsa_mode') and deduped:
            deduped[0]['responsa_expanded_count'] = total_expanded

        return deduped

    def _deduplicate(self, results):
        # V0.8 wins outright on a uid collision. V0.7 rows must ALSO dedupe against
        # each other: the same uid arrives twice for one page -- once from the
        # aggregated scope='system' continuous doc and once from its scope='page'
        # doc -- and the old `uid not in v8` test let both through, rendering the
        # identical folio as two separate results.
        v8 = {r['uid']: r for r in results if r['display']['source'] == "V0.8"}
        final = list(v8.values())
        seen_v7 = set()
        for r in results:
            if r['display']['source'] != "V0.7":
                continue
            uid = r['uid']
            if uid in v8 or uid in seen_v7:
                continue
            seen_v7.add(uid)
            final.append(r)
        return final

    def search_composition_logic(self, full_text, chunk_size, max_freq, mode, filter_text=None, progress_callback=None,
                                   boundary_mode='full', boundary_delimiter='\n', boundary_boost=1.5,
                                   min_boundary_matches=0, min_delimiter_distance=3,
                                   restrict_sys_ids: set = None,
                                   corpus_scope: str = 'genizah'):
        """
        Scans composition chunks against the index.
        Returns aggregated results with WIDE source context.

        Boundary Search Modes:
        - 'full': Regular search, track boundary matches for display
        - 'boundary': Only return results with boundary-crossing matches
        - 'combined': Full search with score boost for boundary matches

        Phase 110 (COMP-LOC-01/02): corpus_scope selects which index loop runs —
        'genizah' (Genizah Tantivy loop only), 'local' (regular My-Library index
        only), or 'all' (both, merged into the same doc_hits accumulator — NOT RRF).

        Phase 110 DESIGN CORRECTION (2026-06-08): standard composition queries the
        REGULAR My-Library index (self.local_searcher), the same index regular
        search scope=Local uses — NOT the LAB side-index. The LAB side-index is
        opt-in via Lab Mode (LabEngine.lab_composition_search). The default path
        has no weights-hash / no staleness; an empty LOCAL result is just "no
        results" (no staleness banner).
        """
        # Phase 110 C4: fail CLOSED — never expose LOCAL on a bad value.
        if corpus_scope not in ('genizah', 'local', 'all'):
            corpus_scope = 'genizah'
        _local_lab_stale = False  # Phase 110 Round-2 #4: A2 default so EVERY return path carries it

        # 1. Tokenize original text - track positions for preserving formatting
        token_matches = list(re.finditer(Config.WORD_TOKEN_PATTERN, full_text))
        tokens = [strip_nikud(m.group()) for m in token_matches]  # Strip nikud from tokens
        token_positions = [(m.start(), m.end()) for m in token_matches]  # Store positions

        # Strip nikud from filter text for consistent matching
        if filter_text:
            filter_text = strip_nikud(filter_text)

        if len(tokens) < chunk_size:
            return {'main': [], 'filtered': [], 'boundary_stats': None,
                    'corpus_scope': corpus_scope, 'local_lab_stale': _local_lab_stale}

        # Get boundary stats (includes parsed boundaries to avoid double parsing)
        from genizah_core import get_boundary_stats, get_crossed_boundaries  # noqa: PLC0415 -- lazy; GUARD-01 safe
        boundary_stats = get_boundary_stats(full_text, boundary_delimiter, chunk_size, min_delimiter_distance)
        boundaries = boundary_stats.get('boundaries', [])

        # Build chunks with boundary tracking
        chunks_data = []
        for i in range(len(tokens) - chunk_size + 1):
            crossed_bounds = get_crossed_boundaries(i, i + chunk_size, boundaries)
            chunks_data.append((i, tokens[i:i + chunk_size], crossed_bounds))

        # Single map for all results - track filtering status per uid
        doc_hits = defaultdict(lambda: {
            'head': '', 'src': '', 'content': '', 'matches': [], 'src_indices': set(),
            # Phase 110 UAT (Issue 1): carry the LOCAL doc's shelfmark (filename)
            # so build_items can emit it for the comp display. Empty for Genizah hits.
            'shelfmark': '',
            'patterns': set(), 'boundary_chunk_scores': [], 'crossed_boundaries': set(),
            # Phase 77 D-13: per-chunk attribution mirrors lab_composition_search
            # at line 1366. Same tuple shape (chunk_index, chunk_text, score, snippet)
            # so shared.search_serializer consumes both producers uniformly.
            # chunk_count is derived from chunk_hits at build_items time via
            # _count_unique_chunks() so it reflects unique source-chunk contents,
            # not raw Tantivy hits (matters when source repeats a phrase).
            'chunk_hits': [],
            'is_filtered': False  # True if ANY match came from filtered chunk
        })

        total_chunks = len(chunks_data)
        was_cancelled = False

        # Pre-compute allowed unique_ids for fast O(1) filtering
        restrict_uids = None
        _sid_filter_clause = None
        if restrict_sys_ids is not None:
            browse_map = self._load_browse_map()
            restrict_uids = set()
            for sid in restrict_sys_ids:
                for page in browse_map.get(sid, []):
                    restrict_uids.add(page['uid'])
            # Pre-build Tantivy filter clause for small restrict sets
            if len(restrict_sys_ids) <= 500:
                _sid_filter_clause = '(' + ' OR '.join(
                    f'full_header:"{sid}"' for sid in restrict_sys_ids
                ) + ')'

        # SEED-011 (125a): Build per-chunk plans ONCE before the index loops.
        # Each _ChunkPlan carries both flavor query strings (Genizah raw +
        # LOCAL diacritic-folded) and both compiled regexes.  Both loops then
        # consume the pre-built plans rather than re-deriving them independently.
        # build_tantivy_query / build_regex_pattern are called once per
        # (chunk x flavor) — for corpus_scope='all' that is the same 2*N as base
        # (the two flavors genuinely differ: LOCAL folds diacritics for SEED-006
        # M1).  Codex Gate-2 fix: each flavor's build is GATED on the same
        # predicate as its consuming loop, so a scoped run (incl. the default
        # corpus_scope='genizah') no longer builds the unused opposite flavor —
        # restoring base behavior, where each build lived inside its own loop.
        _cs_field = 'content_search' if getattr(self, '_has_content_search', False) else None
        _local_has_cs_prepass = getattr(self, "_local_has_content_search", False)
        # Predicates mirror the Genizah loop gate (corpus_scope != 'local') and the
        # LOCAL block gate (corpus_scope != 'genizah' + local index present +
        # is_searchable) below.  (`not was_cancelled` is intentionally omitted: it
        # can only flip mid-Genizah-loop, AFTER this pre-pass; in that rare cancel
        # case the LOCAL loop is skipped anyway, so the pre-built LOCAL plans are
        # simply unused — wasted work on an already-cancelled run, identical result.)
        _do_genizah_pp = (corpus_scope != 'local')
        _scl_tab_pp = self._my_library_tab_ref() if getattr(self, "_my_library_tab_ref", None) is not None else None
        _scl_is_searchable_pp = getattr(_scl_tab_pp, "is_searchable", True) if _scl_tab_pp is not None else True
        _do_local_pp = (corpus_scope != 'genizah'
                        and getattr(self, 'local_searcher', None) is not None
                        and getattr(self, 'local_index', None) is not None
                        and _scl_is_searchable_pp)
        chunk_plans = []
        for (token_idx_pp, chunk_pp, chunk_crossed_pp) in chunks_data:
            # Genizah flavor: raw chunk, with content_search_field if available
            if _do_genizah_pp:
                _genizah_q_str = self.build_tantivy_query(chunk_pp, mode, content_search_field=_cs_field)
                _genizah_regex = self.build_regex_pattern(chunk_pp, mode, 0)
            else:
                _genizah_q_str = None
                _genizah_regex = None
            # LOCAL flavor: diacritic-folded chunk for SEED-006 M1 compat
            if _do_local_pp:
                if _local_has_cs_prepass and mode != 'Regex':
                    _local_chunk_q = [strip_search_diacritics(_w) for _w in chunk_pp]
                else:
                    _local_chunk_q = chunk_pp
                _local_q_str = self.build_tantivy_query(_local_chunk_q, mode)
                _local_regex = self.build_regex_pattern(_local_chunk_q, mode, 0)
            else:
                _local_chunk_q = chunk_pp
                _local_q_str = None
                _local_regex = None
            chunk_plans.append(_ChunkPlan(
                token_idx=token_idx_pp,
                chunk=chunk_pp,
                chunk_crossed_bounds=chunk_crossed_pp,
                genizah_query_str=_genizah_q_str,
                compiled_regex_genizah=_genizah_regex,
                local_query_str=_local_q_str,
                compiled_regex_local=_local_regex,
                local_chunk_q=_local_chunk_q,
            ))

        # 2. Scan chunks (wrapped in try/except to support partial results on cancel)
        try:
          # Phase 110: gate the Genizah Tantivy loop — skipped on a LOCAL-only run.
          # doc_hits/was_cancelled/total_chunks are initialized ABOVE this branch (M1),
          # so a corpus_scope='local' run never NameErrors on a loop-local variable.
          if corpus_scope != 'local':
            for i, plan in enumerate(chunk_plans):
                token_idx = plan.token_idx
                chunk = plan.chunk
                chunk_crossed_bounds = plan.chunk_crossed_bounds
                if progress_callback: progress_callback(i, total_chunks)

                # Consume pre-built Genizah-flavor query + regex (SEED-011)
                t_query = plan.genizah_query_str
                regex = plan.compiled_regex_genizah
                if not regex: continue

                # Augment chunk query with sys_id filter
                if _sid_filter_clause:
                    t_query = f'({t_query}) AND {_sid_filter_clause}'

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

                        # Pre-search filter: skip manuscripts outside the restrict set
                        if restrict_uids is not None:
                            if doc['unique_id'][0] not in restrict_uids:
                                continue

                        content = doc['content'][0]

                        # Bracket handling: strip brackets for bracket-free
                        # queries; preserve for bracket-containing queries
                        # (user pasted text with literal brackets).
                        match_content = content if _query_has_brackets(' '.join(chunk)) else _strip_brackets(content)

                        # Verify exact Regex match
                        if regex.search(match_content):
                            uid = doc['unique_id'][0]

                            # Always use single map - accumulate all matches for same uid
                            rec = doc_hits[uid]

                            # Mark as filtered if ANY chunk match is filtered
                            if is_text_filtered or is_freq_filtered:
                                rec['is_filtered'] = True
                                # Annotate filter reason for UI display
                                if is_text_filtered:
                                    rec['filter_reason'] = 'source_text'
                                elif is_freq_filtered:
                                    rec['filter_reason'] = 'high_frequency'

                            rec['head'] = doc['full_header'][0]
                            rec['src'] = doc['source'][0]
                            rec['content'] = content
                            # Use original content span if possible, fall back to stripped
                            _orig_m = regex.search(content)
                            _ms_match = _orig_m or regex.search(match_content)
                            rec['matches'].append(_ms_match.span())
                            # Save indices of found words in *source* text
                            rec['src_indices'].update(range(token_idx, token_idx + chunk_size))
                            rec['patterns'].add(regex.pattern)
                            # chunk_count is derived post-hoc from unique
                            # chunk_hits contents in build_items below;
                            # incrementing here would inflate the count
                            # whenever the source repeats a phrase or Tantivy
                            # returns the same uid from multiple segments.

                            # Phase 77 D-13: per-chunk attribution mirrors
                            # lab_composition_search at line 1390. ms_snip is a
                            # 60-char window around the match for parallels JSON
                            # matches[*].manuscript_snippet (chunk_index is the
                            # 0-based outer-loop variable i).
                            try:
                                _ms_s, _ms_e = _ms_match.span()
                                _snip_s = max(0, _ms_s - 60)
                                _snip_e = min(len(content), _ms_e + 60)
                                ms_snip = content[_snip_s:_ms_s] + f"*{content[_ms_s:_ms_e]}*" + content[_ms_e:_snip_e]
                            except Exception:
                                ms_snip = ''
                            # Dedup: Tantivy can return the same uid twice from
                            # different segments — keep the highest-scoring entry
                            # per (chunk_index, ms_snip) instead of appending duplicates.
                            _seen = rec.setdefault('_chunk_hit_keys', {})
                            _key = (i, ms_snip)
                            _existing_idx = _seen.get(_key)
                            _new_score = float(score)
                            if _existing_idx is None:
                                _seen[_key] = len(rec['chunk_hits'])
                                rec['chunk_hits'].append(
                                    (i, ' '.join(chunk), _new_score, ms_snip)
                                )
                            elif _new_score > rec['chunk_hits'][_existing_idx][2]:
                                rec['chunk_hits'][_existing_idx] = (
                                    i, ' '.join(chunk), _new_score, ms_snip
                                )

                            # Track boundary-crossing matches - each boundary counted once
                            if chunk_crossed_bounds:
                                rec['boundary_chunk_scores'].append(score)
                                rec['crossed_boundaries'].update(chunk_crossed_bounds)
                except Exception as e:
                    LAB_LOGGER.warning(f"Failed composition chunk processing at token {token_idx}: {e}")
        except InterruptedError:
            was_cancelled = True

        # Phase 110 DESIGN CORRECTION (2026-06-08, Plan 110-03 UAT checkpoint):
        # Standard (Lab-Mode-OFF) composition now queries the REGULAR My-Library
        # index (self.local_searcher / self.local_index) — the SAME index that
        # regular search scope=Local uses — NOT the LOCAL LAB side-index. The LAB
        # side-index is opt-in ("Lab Mode") only; routing the default path through
        # it returned nothing when the user had never built a LAB index. The regular
        # LOCAL schema (shared/local_indexer.build_local_schema) carries
        # content/unique_id/full_header/source/content_head/content_tail — every
        # field this hook reads — so the doc-field reads are unchanged.
        #
        # Lab Mode composition (LabEngine.lab_composition_search) is UNCHANGED and
        # keeps using the LAB side-index + its own freshness/staleness. The default
        # path has NO weights-hash and NO staleness concept: an empty LOCAL result
        # is treated exactly like an empty Genizah result ("no results"), with no
        # staleness banner.
        #
        # CR-01 fallback semantics preserved: the outer try/except below guarantees
        # LOCAL never breaks standard Composition Search.
        # Phase 110 correction: the regular index has no staleness — the standard
        # path never reports stale. (Lab-Mode staleness is handled in
        # lab_composition_search only.) Keep the per-run key for the result payload.
        _local_lab_stale = False
        # Phase 97 R-01 + Codex Gate-2 (round 2): enter the LOCAL block IFF the
        # pre-pass actually built the LOCAL-flavor query/regex (`_do_local_pp` —
        # the SAME snapshot of corpus_scope != 'genizah' + local index present +
        # is_searchable used above to decide the build), so build and consume can
        # never disagree.  Re-reading mutable availability HERE (as before) could
        # diverge from the pre-pass: if it flipped absent->present mid-search (a
        # MyLibraryTab rebuild completing on another thread) this loop would enter
        # and consume None plans, silently dropping every LOCAL hit.  (A
        # present->absent flip is still handled by the inner None-guard below.)
        if not was_cancelled and _do_local_pp:
            try:
                _local_index_scl = self.local_index
                _local_searcher_scl = self.local_searcher
                # SEED-006 M1: parity with the regular LOCAL path (_query_local_index)
                # — when the diacritic-folded content_search field exists, fan
                # field-less chunk terms across it too so צמאן/צ'מאן reach צ̇מאן in
                # LOCAL composition/parallels, not just regular LOCAL search.
                _local_has_cs_scl = getattr(self, "_local_has_content_search", False)
                _local_fields_scl = ["content", "content_head", "content_tail"]
                if _local_has_cs_scl:
                    _local_fields_scl.append("content_search")
                if _local_index_scl is not None and _local_searcher_scl is not None:
                    # SEED-011 (125a): consume pre-built LOCAL-flavor plans instead of
                    # re-deriving query/regex per chunk.  The diacritic-folded
                    # local_query_str / compiled_regex_local were computed in the
                    # pre-pass above (SEED-006 M1 fold already applied).
                    _total_scl = len(chunk_plans)
                    for _i_scl, _plan_scl in enumerate(chunk_plans):
                        # Every chunk, no modulo: one chunk is a parse_query + a
                        # 50-hit search + up to 50 materializations, far too coarse
                        # to skip. Outside the per-chunk try below, whose broad
                        # handler would swallow the cancel.
                        if progress_callback:
                            try:
                                progress_callback(_i_scl, _total_scl)
                            except (InterruptedError, KeyboardInterrupt):
                                raise
                            except Exception:
                                pass  # progress is advisory; cancellation is not
                        _token_idx_scl = _plan_scl.token_idx
                        _chunk_scl = _plan_scl.chunk
                        _chunk_crossed_scl = _plan_scl.chunk_crossed_bounds
                        _chunk_q_scl = _plan_scl.local_chunk_q
                        _t_query_scl = _plan_scl.local_query_str
                        _regex_scl = _plan_scl.compiled_regex_local
                        if not _regex_scl:
                            continue
                        _is_freq_filtered_scl = False
                        _is_text_filtered_scl = False
                        if filter_text and _regex_scl.search(filter_text):
                            _is_text_filtered_scl = True
                        try:
                            # Mirror _query_local_index's metacharacter-strip
                            # fallback (v7.16 load-bearing fix): Tantivy's parser
                            # chokes on the geresh in אמ' / gershayim in רמב"ם and
                            # the whole LOCAL search returned 0. Strip the syntax
                            # metacharacters and re-parse; the regex filter below
                            # still enforces the precise match.
                            try:
                                _query_scl = _local_index_scl.parse_query(
                                    _t_query_scl, _local_fields_scl
                                )
                            except (ValueError, Exception):
                                _safe_scl = re.sub(
                                    r"[+\-&|!(){}\[\]^\"~*?:\\/']", " ", _t_query_scl
                                ).strip()
                                if not _safe_scl:
                                    continue
                                _query_scl = _local_index_scl.parse_query(
                                    _safe_scl, _local_fields_scl
                                )
                            _hits_scl = _local_searcher_scl.search(_query_scl, 50).hits
                            _is_freq_filtered_scl = len(_hits_scl) > max_freq
                            for _score_scl, _doc_addr_scl in _hits_scl:
                                _doc_scl = _local_searcher_scl.doc(_doc_addr_scl)
                                _content_scl = _doc_scl['content'][0]
                                _match_content_scl = (
                                    _content_scl
                                    if _query_has_brackets(' '.join(_chunk_scl))
                                    else _strip_brackets(_content_scl)
                                )
                                if _regex_scl.search(_match_content_scl):
                                    _uid_scl = _doc_scl['unique_id'][0]
                                    _rec_scl = doc_hits[_uid_scl]
                                    if _is_text_filtered_scl or _is_freq_filtered_scl:
                                        _rec_scl['is_filtered'] = True
                                        if _is_text_filtered_scl:
                                            _rec_scl['filter_reason'] = 'source_text'
                                        elif _is_freq_filtered_scl:
                                            _rec_scl['filter_reason'] = 'high_frequency'
                                    _rec_scl['head'] = _doc_scl['full_header'][0]
                                    _rec_scl['src'] = _doc_scl['source'][0]
                                    # Phase 110 UAT (Issue 1): carry the LOCAL
                                    # doc's shelfmark (filename) through to the
                                    # comp display so the manuscript row shows the
                                    # filename, not "unknown". Tolerate a missing
                                    # field (only the regular LOCAL schema has it).
                                    try:
                                        _shelf_scl = _doc_scl['shelfmark']
                                        _rec_scl['shelfmark'] = _shelf_scl[0] if _shelf_scl else ''
                                    except (KeyError, IndexError, TypeError):
                                        _rec_scl['shelfmark'] = ''
                                    _rec_scl['content'] = _content_scl
                                    _orig_m_scl = _regex_scl.search(_content_scl)
                                    _ms_match_scl = _orig_m_scl or _regex_scl.search(_match_content_scl)
                                    _rec_scl['matches'].append(_ms_match_scl.span())
                                    _rec_scl['src_indices'].update(
                                        range(_token_idx_scl, _token_idx_scl + chunk_size)
                                    )
                                    _rec_scl['patterns'].add(_regex_scl.pattern)
                                    if _chunk_crossed_scl:
                                        _rec_scl['boundary_chunk_scores'].append(_score_scl)
                                        _rec_scl['crossed_boundaries'].update(_chunk_crossed_scl)
                                    try:
                                        _ms_s_scl, _ms_e_scl = _ms_match_scl.span()
                                        _snip_s_scl = max(0, _ms_s_scl - 60)
                                        _snip_e_scl = min(len(_content_scl), _ms_e_scl + 60)
                                        _ms_snip_scl = (
                                            _content_scl[_snip_s_scl:_ms_s_scl]
                                            + f"*{_content_scl[_ms_s_scl:_ms_e_scl]}*"
                                            + _content_scl[_ms_e_scl:_snip_e_scl]
                                        )
                                    except Exception:
                                        _ms_snip_scl = ''
                                    _seen_scl = _rec_scl.setdefault('_chunk_hit_keys', {})
                                    _key_scl = (_i_scl, _ms_snip_scl)
                                    _existing_scl = _seen_scl.get(_key_scl)
                                    _new_score_scl = float(_score_scl)
                                    if _existing_scl is None:
                                        _seen_scl[_key_scl] = len(_rec_scl['chunk_hits'])
                                        _rec_scl['chunk_hits'].append(
                                            (_i_scl, ' '.join(_chunk_scl), _new_score_scl, _ms_snip_scl)
                                        )
                                    elif _new_score_scl > _rec_scl['chunk_hits'][_existing_scl][2]:
                                        _rec_scl['chunk_hits'][_existing_scl] = (
                                            _i_scl, ' '.join(_chunk_scl), _new_score_scl, _ms_snip_scl
                                        )
                        except Exception:
                            pass
            except InterruptedError:
                # MUST precede the broad handler AND must set the flag: otherwise
                # the returned payload's 'partial' key reports False for a run the
                # user cancelled — a lie, not just a missing detail.
                was_cancelled = True
            except Exception as _scl_exc:
                LAB_LOGGER.warning(
                    "search_composition_logic: LOCAL (regular-index) scan failed: %r", _scl_exc
                )

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

                    # B. Build text for each cluster - preserve original formatting
                    for cl in clusters:
                        start_ctx = max(0, cl[0] - 200)
                        end_ctx = min(len(tokens), cl[-1] + 201)

                        cl_set = set(cl)

                        # Get character positions from token_positions
                        char_start = token_positions[start_ctx][0]
                        char_end = token_positions[end_ctx - 1][1]

                        # Extract original text with formatting preserved
                        original_snippet = full_text[char_start:char_end]

                        # Insert highlight markers for matched words (work backwards to preserve positions)
                        # Build list of (offset_in_snippet, word_start, word_end) for matched words
                        highlights = []
                        for k in range(start_ctx, end_ctx):
                            if k in cl_set:
                                word_char_start = token_positions[k][0] - char_start
                                word_char_end = token_positions[k][1] - char_start
                                highlights.append((word_char_start, word_char_end))

                        src_snippets.append(
                            mark_word_highlights(original_snippet, highlights))

                spans = sorted(data['matches'], key=lambda x: x[0])
                merged = []
                if spans:
                    curr_s, curr_e = spans[0]
                    for s, e in spans[1:]:
                        if s <= curr_e + 20: curr_e = max(curr_e, e)
                        else: merged.append((curr_s, curr_e)); curr_s, curr_e = s, e
                    merged.append((curr_s, curr_e))

                base_score = sum(e-s for s,e in merged)

                # Calculate boundary match quality and final score
                from genizah_core import calculate_boundary_quality, calculate_final_score_with_boost  # noqa: PLC0415 -- lazy; GUARD-01 safe
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

                # Calculate normalized boundary quality
                boundary_quality_normalized = 0.0
                if has_boundary_matches and base_score > 0:
                    boundary_quality_normalized = min(boundary_quality / base_score, 1.0)

                ms_snips = []
                for s, e in merged:
                    ms_snips.append(build_marked_composition_fragment(
                        data['content'], s, e))

                combined_pattern = "|".join(list(data['patterns'])) if data.get('patterns') else ""

                final_items.append({
                    'score': base_score,
                    'final_score': final_score,
                    'uid': uid,
                    'raw_header': data['head'],
                    'src_lbl': data['src'],
                    # Phase 110 UAT (Issue 1): per-item shelfmark (filename) for
                    # LOCAL comp hits; empty for Genizah (UI computes Genizah
                    # shelfmark from raw_header via the Genizah meta path).
                    'shelfmark': data.get('shelfmark', ''),
                    'source_ctx': "\n\n".join(src_snippets),
                    'text': "\n...\n".join(ms_snips),
                    'highlight_pattern': combined_pattern,
                    # Boundary metadata
                    'has_boundary_matches': has_boundary_matches,
                    'boundary_match_count': len(data.get('crossed_boundaries', set())),
                    'boundary_quality': boundary_quality_normalized,
                    # chunk_count is derived from unique chunk_hits contents,
                    # not raw Tantivy hits, so repeated source phrases and
                    # cross-segment duplicates don't inflate the user-facing
                    # `min chunks` filter. chunk_hits remains the list of
                    # (chunk_index, chunk_text, score, ms_snip) tuples for
                    # serialize_parallels_payload matches[].
                    'chunk_count': _count_unique_chunks(data.get('chunk_hits', [])),
                    'chunk_hits': data.get('chunk_hits', []),
                    # Filtering flag and reason
                    'is_filtered': data.get('is_filtered', False),
                    'filter_reason': data.get('filter_reason', '')
                })

            # Sort by final_score in combined mode, otherwise by base score
            if boundary_mode == 'combined':
                final_items.sort(key=lambda x: x.get('final_score', x['score']), reverse=True)
            else:
                final_items.sort(key=lambda x: x['score'], reverse=True)

            return final_items

        # Build all items from single map, then separate by filter status
        all_items = build_items(doc_hits)

        # Apply boundary mode filtering first
        if boundary_mode == 'boundary':
            all_items = [item for item in all_items if item.get('has_boundary_matches', False)]

        # Apply min_boundary_matches filter
        # In 'full' mode, this acts as min chunk hits (total matching chunks per document)
        # In 'boundary'/'combined' modes, this filters on actual boundary crossings
        if min_boundary_matches > 0:
            if boundary_mode == 'full':
                all_items = [item for item in all_items if item.get('chunk_count', 0) >= min_boundary_matches]
            else:
                all_items = [item for item in all_items if item.get('boundary_match_count', 0) >= min_boundary_matches]

        # Separate into main and filtered lists
        main_list = [item for item in all_items if not item.get('is_filtered', False)]
        filtered_list = [item for item in all_items if item.get('is_filtered', False)]

        return {'main': main_list, 'filtered': filtered_list, 'partial': was_cancelled,
                'boundary_stats': boundary_stats,
                # Phase 110 A2 + Round-2 #4: per-run scope + staleness verdict.
                'corpus_scope': corpus_scope, 'local_lab_stale': _local_lab_stale}

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
                    'highlight_pattern': rep_page.get('highlight_pattern', ''),
                    # Phase 110 UAT (Issue 1): carry the representative page's source
                    # label + shelfmark (filename) so the comp UI can render LOCAL
                    # manuscript rows as filename / parent-folder (mirroring regular
                    # search) instead of the Genizah-meta "unknown".
                    'source': rep_page.get('src_lbl', ''),
                    'shelfmark': rep_page.get('shelfmark', ''),
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
            status_callback(_tr("Fetching metadata..."))

        # Phase 110 (D-12 / Round-2 #1): filter LOCAL `97…` sys_ids OUT of the
        # NLI/FJMS metadata fetch. A private LOCAL id is not in csv_bank, so a
        # grouped LOCAL composition run would otherwise reach the NLI network
        # path. LOCAL display data comes only from the primed filepath cache.
        from shared.local_sys_id import is_local_sys_id
        genizah_ids = [sid for sid in ids if sid and not is_local_sys_id(sid)]

        # Load metadata (fast due to previous fix)
        self.meta_mgr.batch_fetch_shelfmarks(genizah_ids, progress_callback=progress_callback)

        if status_callback:
            status_callback(_tr("Grouping results..."))

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

    def get_full_text_by_header(self, full_header):
        """Fetch a page's stored `content` by its exact `full_header` value.

        Phase 145 (passage-matching parallels search): the passage index
        identifies a record by the same corpus-record id the Tantivy
        `full_header` field stores verbatim (`shared/passage_corpus.py`'s
        ``==> {sys_id}_{IE..}_{P######}_{FL..} <==`` header, stripped of its
        arrows -- byte-identical to `full_header` since both are produced
        from the same source line by the same strip). This mirrors
        `get_full_text_by_id` exactly, just keyed on `full_header` instead of
        `unique_id` (a phrase query on the header field, verified elsewhere
        in this module at lines querying `full_header:"{sid}"`); a defensive
        exact-match check guards against a same-prefix collision within the
        phrase-query hit set.

        `full_header` is validated against a closed character set BEFORE it
        is interpolated into the Tantivy query string (adversarial review
        finding #6): every real header is `[A-Za-z0-9_]+` by construction
        (sys_id/IE/P/FL digits and letters joined by underscores -- see
        shared/passage_corpus.py's HEADER_RE), so a value outside that set
        cannot be a real header, and rejecting it before it reaches
        `parse_query` closes the query-injection surface a directly-crafted
        `record_id` (e.g. from a malformed or adversarial artifact) would
        otherwise open. The rejected value itself is never logged --
        untrusted input in a log line is its own hazard.
        """
        if not full_header or not re.match(r'^[A-Za-z0-9_]+$', full_header):
            LOGGER.warning(
                "get_full_text_by_header: rejecting a header outside the "
                "[A-Za-z0-9_]+ character set (value withheld)"
            )
            return None
        try:
            q = self.index.parse_query(f'full_header:"{full_header}"', ["full_header"])
            res = self.searcher.search(q, 5)
            for _score, doc_addr in res.hits:
                doc = self.searcher.doc(doc_addr)
                if doc['full_header'][0] == full_header:
                    return doc['content'][0]
        except Exception as e:
            LOGGER.warning("Failed to retrieve full text for header %s: %s", full_header, e)
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

    def _get_metadata_only_browse_page(self, sys_id, p_num=None, absolute_index=None, next_prev=0):
        """Build a minimal browse result from csv_bank for records with no Tantivy text.

        Phase 86: synthetic (and other metadata-only) records have no Tantivy
        pages, but enriched images can drive pagination at the UI layer. Thread
        the caller's p_num / absolute_index / direction through so Next/Prev
        and combo selection produce a moving target page that the renderer can
        clamp against the image count.
        """
        if not hasattr(self, 'meta_mgr') or not self.meta_mgr:
            return None
        shelfmark, title = self.meta_mgr.get_meta_for_id(sys_id)
        if shelfmark == 'Unknown':
            return None

        if p_num is not None:
            try:
                base_p = max(1, int(p_num))
            except (ValueError, TypeError):
                base_p = 1
        elif absolute_index is not None:
            try:
                base_p = max(1, int(absolute_index) + 1)
            except (ValueError, TypeError):
                base_p = 1
        else:
            base_p = 1

        try:
            bump = int(next_prev or 0)
        except (ValueError, TypeError):
            bump = 0

        target_p = max(1, base_p + bump)

        return {
            'uid': '',
            'p_num': target_p,
            'full_header': '',
            'text': '',
            'total_pages': 0,
            'current_idx': target_p,
            'internal_index': target_p - 1,
            'sys_id': sys_id,
            'metadata_only': True,
        }

    def get_browse_page(self, sys_id, p_num=None, next_prev=0, absolute_index=None, allow_cross=False, volume_ie=None):
        browse_map = self._load_browse_map()
        if not browse_map:
            return self._get_metadata_only_browse_page(sys_id, p_num=p_num, absolute_index=absolute_index, next_prev=next_prev)

        # Prepare ordered list for cross-manuscript navigation
        if allow_cross and (not hasattr(self, '_ordered_sys_ids') or not self._ordered_sys_ids):
            self._ordered_sys_ids = list(browse_map.keys())

        if sys_id not in browse_map:
            return self._get_metadata_only_browse_page(sys_id, p_num=p_num, absolute_index=absolute_index, next_prev=next_prev)
        all_pages = browse_map[sys_id]
        if not all_pages:
            return self._get_metadata_only_browse_page(sys_id, p_num=p_num, absolute_index=absolute_index, next_prev=next_prev)

        # Filter to specific IE for multi-IE manuscripts
        active_ie = volume_ie
        if volume_ie:
            from genizah_core import get_volume_pages  # noqa: PLC0415 -- lazy; GUARD-01 safe
            pages = get_volume_pages(all_pages, volume_ie)
            if not pages:
                # Requested IE not found — fall back to all pages
                LOGGER.warning("get_browse_page: volume_ie=%s not found in %d pages for sys_id=%s. ie_ids in pages: %s",
                               volume_ie, len(all_pages), sys_id,
                               list(set(p.get('ie_id') for p in all_pages[:20])))
                pages = all_pages
                active_ie = None
            else:
                LOGGER.info("get_browse_page: volume_ie=%s filtered to %d/%d pages for sys_id=%s",
                            volume_ie, len(pages), len(all_pages), sys_id)
        else:
            pages = all_pages

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
            except (ValueError, TypeError): p_val = -999

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
            'sys_id': sys_id,
            'volume_ie': active_ie or target_page.get('ie_id'),
        }

    def get_local_browse_page(self, sys_id, p_num=None, next_prev=0,
                              absolute_index=None, allow_cross=False, volume_ie=None):
        """Phase 96 NEW-2: LOCAL analog to `get_browse_page` (folio nav for LOCAL files).

        Page identity contract (Codex Item 4 / tech-debt D):
          p_num       — physical page number in the source file.  For PDFs this is
                        the real PDF page number; blank/empty pages are skipped by
                        the indexer so the set of p_nums is SPARSE (not contiguous).
                        Example: a 1,600-page PDF with 71 blank pages has indexed
                        p_nums like {1, 2, …, 1529, 1552, …} — NOT 1..1529.
          current_idx — 0-based ordinal in the SORTED indexed page list (dense).
                        Used for prev/next boundary detection only.  The UI spinbox
                        must display p_num, not current_idx.

        Returns `{uid, p_num, full_header, text, total_pages, current_idx,
        internal_index, max_p_num, sys_id}` — same shape as `get_browse_page`
        plus `max_p_num` so the caller can set a meaningful spinbox upper bound
        for sparse files.

        Canonical img read order (tech-debt D): prefer hit['display']['img']
        over hit['img'] for Genizah hits; for LOCAL hits use p_num directly
        (see _build_local_result_dict which writes p_num into display['img']).

        Parameters mirror get_browse_page for drop-in dispatch, but LOCAL
        semantics differ:
          - allow_cross is IGNORED (D-12: no wrap, no cross-file nav)
          - volume_ie is IGNORED (LOCAL files have no volume concept)
          - absolute_index is IGNORED (p_num + current position in sorted page
            list is the authoritative navigation state for LOCAL files)

        Returns None when:
          - sys_id has no LOCAL pages in the index
          - target p_num is not found in the sorted page list (Item 5: no
            silent fallback to page 1 — let the caller preserve its current state)
          - next_prev would land outside [0, total_pages) (D-12: no wrap)

        Note: this method does NOT use Task 1's D-04.1 filter-out semantics.
        Browse navigation must return ALL pages of the file (user navigated
        into a specific file and wants to see ALL its pages, regardless of
        whether the search regex matches each page). Filter-out is search-only.
        """
        if self.local_searcher is None or self.local_index is None:
            return None

        # Cache the sorted page list per sys_id to avoid repeat Tantivy queries
        # on every nav click. Invalidated by reload_local_indexes().
        cache = getattr(self, "_local_pages_cache", None)
        if cache is None:
            cache = {}
            self._local_pages_cache = cache

        pages = cache.get(sys_id)
        if pages is None:
            try:
                q = self.local_index.parse_query(sys_id, ["full_header"])
                res = self.local_searcher.search(q, 5000)
            except Exception as e:
                LOGGER.warning(
                    "get_local_browse_page: parse_query failed for %s: %s", sys_id, e
                )
                return None
            collected = []
            hits = res.hits if hasattr(res, "hits") else res
            for _score, doc_addr in hits:
                try:
                    doc = self.local_searcher.doc(doc_addr)
                    full_header = doc.get_first("full_header") or ""
                    if not full_header.startswith(f"{sys_id}_LOCAL_P"):
                        continue
                    content = doc.get_first("content") or ""
                    uid = doc.get_first("unique_id") or ""
                    try:
                        p_str = full_header.split("_LOCAL_P")[1].split("_F")[0]
                        pn = int(p_str)
                    except (ValueError, IndexError):
                        continue
                    collected.append({
                        "p_num": pn,
                        "full_header": full_header,
                        "text": content,
                        "uid": uid,
                    })
                except (KeyError, IndexError, TypeError):
                    continue
            collected.sort(key=lambda x: x["p_num"])
            pages = collected
            cache[sys_id] = pages

        if not pages:
            return None

        # max_p_num: the highest physical page number in the sorted list.
        # For sparse PDFs this is the real last-page number (e.g. 1600), NOT
        # len(pages) (e.g. 1529). Used by the caller to set spinbox maximum.
        max_p_num = pages[-1]["p_num"]

        # Determine target index.
        if p_num is None and next_prev == 0:
            target_idx = 0
        elif p_num is not None:
            # Find the current p_num in the sorted page list, then apply offset.
            # Item 5: if p_num is not in the list, return None — do NOT silently
            # fall back to page 1.  The UI caller keeps its existing spinner value.
            found_idx = next(
                (i for i, pg in enumerate(pages) if pg["p_num"] == p_num), None
            )
            if found_idx is None:
                LOGGER.debug(
                    "get_local_browse_page: p_num=%s not in index for %s — returning None",
                    p_num, sys_id,
                )
                return None
            target_idx = found_idx + next_prev
        else:
            target_idx = 0 + next_prev

        if target_idx < 0 or target_idx >= len(pages):
            return None  # D-12: no wrap at boundaries

        target = pages[target_idx]
        return {
            "uid": target["uid"],
            "p_num": target["p_num"],
            "full_header": target["full_header"],
            "text": target["text"],
            "total_pages": len(pages),
            "current_idx": target_idx + 1,    # 1-based ordinal (UI display)
            "internal_index": target_idx,      # 0-based ordinal (boundary logic)
            "max_p_num": max_p_num,            # highest physical page number
            "sys_id": sys_id,
        }

    def get_browse_page_by_fl(self, fl_id, sys_id=None):
        browse_map = self._load_browse_map()
        if not browse_map: return None

        if not fl_id:
            return None

        fl_digits = re.sub(r"\D", "", str(fl_id))
        if not fl_digits:
            return None

        def _build_fl_result(sid, all_pages, page, idx):
            """Build result dict with volume-aware page count and IE info."""
            text = self.get_full_text_by_id(page['uid'])
            ie_id = page.get('ie_id') or _extract_ie_from_header(page.get('full_header', ''))
            # For multi-IE manuscripts, filter to the page's IE for correct page count
            from genizah_core import get_volume_pages  # noqa: PLC0415 -- lazy; GUARD-01 safe
            if ie_id:
                volume_pages = get_volume_pages(all_pages, ie_id)
                if volume_pages:
                    # Recompute index within the volume's pages
                    vol_idx = next((i for i, p in enumerate(volume_pages) if p['uid'] == page['uid']), 0)
                    return {
                        'uid': page['uid'],
                        'p_num': page['p_num'],
                        'full_header': page['full_header'],
                        'text': text,
                        'total_pages': len(volume_pages),
                        'current_idx': vol_idx + 1,
                        'sys_id': sid,
                        'fl_id': fl_digits,
                        'volume_ie': ie_id,
                    }
            return {
                'uid': page['uid'],
                'p_num': page['p_num'],
                'full_header': page['full_header'],
                'text': text,
                'total_pages': len(all_pages),
                'current_idx': idx + 1,
                'sys_id': sid,
                'fl_id': fl_digits,
                'volume_ie': ie_id,
            }

        # Try O(1) index lookup first
        if self._fl_id_index is not None:
            candidates = self._fl_id_index.get(fl_digits, [])
            # If sys_id is known, filter to that sys_id
            if sys_id and candidates:
                candidates = [(sid, idx) for sid, idx in candidates if sid == sys_id]
            if candidates:
                sid, idx = candidates[0]
                if sid in browse_map and idx < len(browse_map[sid]):
                    page = browse_map[sid][idx]
                    return _build_fl_result(sid, browse_map[sid], page, idx)

        # Fallback: linear scan (index not yet ready or FL ID not found in index)
        sys_candidates = [sys_id] if sys_id else list(browse_map.keys())

        for sid in sys_candidates:
            if sid not in browse_map:
                continue
            pages = browse_map[sid]
            for idx, page in enumerate(pages):
                parsed = self.meta_mgr.parse_full_id_components(page.get('full_header', ''))
                page_fl = re.sub(r"\D", "", str(parsed.get('fl_id') or ""))
                if page_fl and page_fl == fl_digits:
                    return _build_fl_result(sid, pages, page, idx)
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
