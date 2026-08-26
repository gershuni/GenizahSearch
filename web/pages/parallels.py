# -*- coding: utf-8 -*-
"""
Parallels Search Page - Dicta Genizah Search

Find parallel texts in the Genizah corpus using:
- Shmidman-Koppel-Porat fingerprinting algorithm
- Configurable chunk size
- Advanced filtering options
"""

import logging

from nicegui import ui, run, app
from web.state import state
from web.safe_storage import safe_user_get, safe_user_set
from web.translations import tr, get_language
from urllib.parse import unquote
import asyncio
import re
import html
import os
import time
import requests
from datetime import datetime
from web.components.typography import h1, h2, h3, h4
from web.components.filter_panel import (
    build_domain_options, build_author_options, build_work_options,
    build_filter_summary, has_active_filters, persist_value,
    load_filter_state, consume_incoming_filters, recompute_filter_count,
    create_filter_handlers,
)

logger = logging.getLogger(__name__)

# Import Sefaria sources and text cleaning from the shared sefaria_utils module (no PyQt6 dependency)
from sefaria_utils import SEFARIA_SOURCES, clean_hebrew_text, get_cache_dir, get_sefaria_library

# Import shared sanitization utility
from shared_export_utils import sanitize_cache_filename as _sanitize_cache_filename

# DMF-09/DMF-10/DMF-13: library filter imports
from shared.browse_map_utils import (
    LIBRARY_CODES, get_library_display, sanitize_library_codes,
    library_codes_with_manuscripts,
)

# --- Multi-witness letter-level search ------------------------------------
# Module level, not closure level: the tab-snapshot restore runs during page
# build, BEFORE the witness helpers are defined, and a constant it cannot see
# is a NameError at build time -- the exact failure that once took the whole
# page down (owner-reported 2026-08-23).
WITNESS_SEED_ID = 'seed'
WITNESS_CAP = 25          # mirrors SEARCH_API_PASSAGE_MAX_WITNESSES

# Per-depth ceiling on how many witnesses ONE click may search. Deep and
# deepest cost ~6s and ~19s per witness against normal's ~0.7s, so a flat cap
# would mean a click that looks hung. Refused with a specific message instead
# -- and auto-expand refuses to START a round that would breach the cap
# rather than silently shrinking top-K, which would make the control a lie.
WITNESS_DEPTH_CAP = {'normal': WITNESS_CAP, 'deep': 8, 'deepest': 4}

WITNESS_SYS_ID_RE = re.compile(r'((?:99|97)\d{8,})')


def witness_depth_cap(ctx, widget_depth: str = None) -> int:
    """How many witnesses one click may search, at the depth they will
    actually RUN at.

    `ctx` wins over the dropdown, and that is the whole subtlety: a witness
    search takes its depth from `last_passage_ctx`, the settings of the last
    seed search (`_run_one_witness_search`), not from whatever the control
    shows now. Reading the widget first would cap a batch against a depth
    nothing was going to use -- refusing work that is cheap, and admitting
    work that is not.

    The dropdown is the fallback for the one case where there is no ctx yet:
    adding witnesses before the first search.
    """
    depth = (ctx or {}).get('depth') or widget_depth or 'normal'
    return WITNESS_DEPTH_CAP.get(depth, WITNESS_CAP)


def witnesses_over_dispatch_cap(pending, ctx, widget_depth: str = None):
    """`(pending_count, cap)` when a batch must be REFUSED, else `None`.

    The cap was enforced only while adding and promoting witnesses, which
    bounded the wrong quantity: `Find Parallels` resets every non-stale
    witness to `pending` and dispatches the batch, so twenty-five witnesses
    added at normal depth all re-run the moment the seed is re-run at
    `deepest` -- roughly eight minutes of work from one click, taking a slot
    of the shared passage budget twenty-five times over, against a documented
    deepest cap of four.

    Returns the pair rather than a bool so the refusal can name both numbers;
    a message reading only "too many witnesses" leaves the user guessing how
    many to remove.
    """
    count = len(pending or [])
    cap = witness_depth_cap(ctx, widget_depth)
    return (count, cap) if count > cap else None


def witness_sys_id(row) -> str:
    r"""The sys_id a result row belongs to.

    Mirrors `shared/passage_parallels.py::_SYS_ID_RE` and the authoritative
    `shared/metadata_manager.py`, both of which accept a 97 prefix as well as
    99. THE one copy on this page.

    That is WIDER than the nine `r'(99\d{8,})'` patterns elsewhere in this
    file, and wider than the serializer, the export writers and
    `web/export_state.py`. The divergence is real but currently unreachable:
    the live index holds 759,224 records and not one of them is 97-prefixed,
    so no manuscript is skipped by the narrow patterns today. Following the
    authoritative parser here is the safe direction -- a witness resolved by
    the engine cannot fail to resolve on the page. Reconciling all of them is
    a corpus-wide change, not a witness-feature one.
    """
    m = WITNESS_SYS_ID_RE.search((row or {}).get('raw_header') or '')
    return m.group(1) if m else None


def restore_witness_entries(raw, default_label: str, cap: int = None) -> list:
    """Normalise a tab snapshot's witness list back into witness dicts.

    Pure, module level and dependency-injected (`default_label` is the only
    thing it would otherwise need `tr()` for) so the RULES are testable.
    A mutation sweep proved they were not: reverting the drop rule below to
    the obvious `if not text.strip()` -- which deletes every restored
    manuscript witness -- left the entire page suite green.

    Three rules:

    * **A manuscript witness with a `sys_id` survives without text.** The
      snapshot drops its text deliberately (the corpus still has it) and
      `witnesses_needing_text` / the rehydrator put it back before dispatch.
      Dropping it here would quietly shrink a restored 17-witness search.
    * **Anything else with no text is dropped**, because nothing in the world
      can recover it and a witness that cannot be searched must not sit in
      the list pretending otherwise.
    * **Ids are renumbered `w1..wN` over the SURVIVORS.** Reusing the stored
      ids would leave gaps that `_witness_new_id` could then re-issue, and
      two witnesses sharing an id corrupt the per-witness row cache.

    Every witness comes back `pending`: the snapshot holds the FUSED rows, so
    per-witness ranks cannot be recovered, and a fusion rebuilt from partial
    inputs would be quietly wrong rather than visibly absent.

    Returns the list; the caller assigns it and resets the row caches.
    """
    if not isinstance(raw, list) or not raw:
        return []
    out = []
    for entry in raw[:(cap if cap is not None else WITNESS_CAP)]:
        if not isinstance(entry, dict):
            continue
        kind = 'manuscript' if entry.get('kind') == 'manuscript' else 'pasted'
        text = str(entry.get('text') or '')
        sys_id = entry.get('sys_id')
        if not text.strip() and not (kind == 'manuscript' and sys_id):
            continue
        out.append({
            'id': f'w{len(out) + 1}',
            'label': str(entry.get('label') or '') or (sys_id or default_label),
            'kind': kind,
            'sys_id': sys_id,
            'seed_digest': str(entry.get('seed_digest') or ''),
            'headers': [str(h) for h in (entry.get('headers') or []) if h],
            'text': text,
            'status': 'pending',
            'hits': 0,
            'error': '',
        })
    return out


def witnesses_needing_text(pending) -> list:
    """Which pending witnesses have no text to search and can get one back.

    `_persist_active_snapshot` stores a MANUSCRIPT witness without its text on
    purpose -- the corpus still has it, and copying up to 25 x 20,000 chars of
    corpus text into a tab snapshot buys nothing. Nothing re-fetched it on
    restore, so after a reload those witnesses searched the empty string and
    reported `searched, 0 matches`: a false negative indistinguishable from a
    real one. (Found by review, not by any test here.)

    Module level and pure so the RULE is tested rather than its plumbing. A
    pasted witness is never included -- its text existed nowhere but the
    snapshot, so there is nothing to re-fetch -- and neither is one with no
    `sys_id` to fetch by. Both are refused at dispatch instead.
    """
    return [w for w in (pending or [])
            if w.get('kind') == 'manuscript'
            and w.get('sys_id')
            and not (w.get('text') or '').strip()]


def witness_headers_for(sys_ids, rows) -> dict:
    """Which page headers make up each promoted manuscript's witness text.

    Extracted from `collect_witness_texts` so the promotion can RECORD its
    choice. A promoted witness is not a deterministic function of its
    `sys_id`: it is the concatenation of the pages that MATCHED, which is a
    property of the result set on screen at that moment. Re-deriving it later
    from a different result set yields a different witness under the same
    label.
    """
    wanted = set(sys_ids)
    headers_by_sid: dict = {}
    for row in rows or []:
        sid = witness_sys_id(row)
        if sid in wanted and row.get('raw_header'):
            headers_by_sid.setdefault(sid, []).append(row['raw_header'])
    return headers_by_sid


def collect_witness_texts(sys_ids, rows, fetch_header,
                          fetch_manuscript=None, headers_by_sid=None):
    """Gather the text to search a promoted manuscript WITH.

    Module level and dependency-injected so it can be tested without building
    a page: the AST tests that covered this logic in the closure were proven
    vacuous against the exact bug it was written to fix -- a source-text
    assertion cannot tell `for header in headers` from `for header in []`.

    Two rules, both learned the hard way:

    * **The matched pages' own `raw_header`s are the PRIMARY source.** The
      first version used `get_full_manuscript(sys_id)`, which resolves through
      `Config.BROWSE_MAP` -- an auxiliary pickle with no guarantee of holding
      an arbitrary manuscript. Owner-reported: every promotion failed, because
      that map held two entries. `fetch_header` is the same fetcher the engine
      just used to render those rows, so it cannot fail for a row on screen.
    * **Every matched page, not the best one.** A result GROUP spans several
      page-level hits; one page is usually a fraction of the witness.

    `fetch_manuscript` (optional) is the whole-manuscript fallback, tried only
    when no header resolves.

    Returns `(texts_by_sys_id, failed_sys_ids)` -- failures are RETURNED, not
    logged and dropped, so the caller can name them once instead of emitting
    one anonymous toast per manuscript.
    """
    # Caller-supplied headers win: on a REHYDRATE those are the headers the
    # promotion actually used, and re-deriving them from whatever rows are on
    # screen now would rebuild a different witness under the same label.
    if headers_by_sid is None:
        headers_by_sid = witness_headers_for(sys_ids, rows)

    out, failed = {}, []
    for sid in sys_ids:
        parts = []
        for header in (headers_by_sid.get(sid) or []):
            try:
                page = fetch_header(header)
            except Exception:
                page = None
            if page:
                parts.append(page)
        text = "\n".join(parts).strip()
        if not text and fetch_manuscript is not None:
            try:
                pages = fetch_manuscript(sid) or []
            except Exception:
                pages = []
            text = "\n".join(p.get('text') or '' for p in pages).strip()
        if text:
            out[sid] = text
        else:
            failed.append(sid)
    return out, failed


# Phase 145: passage-matching parallels search (fail-closed -- flag AND
# a loaded index; see web/passage_assets.py).
from web.passage_assets import (passage_available, get_passage_searcher,
                                passage_multi_witness_available)
# Codex review finding #15: route the page's passage search through the
# SAME bounded execution budget POST /api/parallels uses -- one semaphore,
# one dedicated ThreadPoolExecutor, one timeout ceiling, for BOTH surfaces.
from web.search_api import run_passage_search
from shared.api_errors import APIError


def get_source_display_name(ref: str) -> str:
    """Get a display name for a source reference."""
    # Handle custom sources
    if ref.startswith('custom:'):
        parts = ref.split(':', 2)
        if len(parts) >= 3:
            return f"📝 {parts[2]}"  # Return the custom name
        return "📝 Custom Text"

    # Look up in predefined sources
    for source_type, source_data in SEFARIA_SOURCES.items():
        for book_key, book_data in source_data.get("books", {}).items():
            if ref in book_data.get("refs", []):
                idx = book_data["refs"].index(ref)
                return f"{source_data['name']} - {book_data['he_names'][idx]}"
    return ref


def flatten_sefaria_text(text_data):
    """Recursively flatten nested text arrays from Sefaria."""
    if isinstance(text_data, str):
        return re.sub(r'<[^>]+>', '', text_data)
    elif isinstance(text_data, list):
        parts = []
        for item in text_data:
            flattened = flatten_sefaria_text(item)
            if flattened:
                parts.append(flattened)
        return " ".join(parts)
    return ""


def fetch_sefaria_text(ref: str, use_cache: bool = True) -> str:
    """Fetch a single text from Sefaria API (cleaned, no nikud/taamim)."""
    cache_dir = get_cache_dir()
    # Use sanitized filename to prevent path traversal attacks
    safe_filename = _sanitize_cache_filename(ref)
    cache_file = os.path.join(cache_dir, f"{safe_filename}_v2.txt")

    if use_cache and os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                text = f.read()
                if text:
                    return text
        except Exception:
            pass  # Cache cleanup failed; stale cache is acceptable

    try:
        encoded_ref = ref.replace(' ', '%20')
        raw_text = ""

        # Determine if this is a Tanakh ref (for v3 "Text Only" version)
        is_tanakh = any(ref.startswith(book) for book in [
            'Genesis', 'Exodus', 'Leviticus', 'Numbers', 'Deuteronomy',
            'Joshua', 'Judges', 'I Samuel', 'II Samuel', 'I Kings', 'II Kings',
            'Isaiah', 'Jeremiah', 'Ezekiel', 'Hosea', 'Joel', 'Amos', 'Obadiah',
            'Jonah', 'Micah', 'Nahum', 'Habakkuk', 'Zephaniah', 'Haggai',
            'Zechariah', 'Malachi', 'Psalms', 'Proverbs', 'Job', 'Song of Songs',
            'Ruth', 'Lamentations', 'Ecclesiastes', 'Esther', 'Daniel',
            'Ezra', 'Nehemiah', 'I Chronicles', 'II Chronicles'
        ])

        if is_tanakh:
            # Try v3 API with "Text Only" version (no nikud/taamim) for Tanakh
            url = f"https://www.sefaria.org/api/v3/texts/{encoded_ref}?version=hebrew|Tanach%20with%20Text%20Only"
            resp = requests.get(url, timeout=15)

            if resp.status_code == 200:
                data = resp.json()
                versions = data.get('versions', [])
                for ver in versions:
                    if ver.get('language') == 'he':
                        ver_text = ver.get('text', [])
                        if isinstance(ver_text, str):
                            raw_text = ver_text
                        else:
                            raw_text = flatten_sefaria_text(ver_text)
                        break

        # Use v2 API for non-Tanakh or as fallback
        if not raw_text:
            url = f"https://www.sefaria.org/api/texts/{encoded_ref}?context=0&pad=0"
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                he_text = data.get('he', [])
                if isinstance(he_text, str):
                    raw_text = he_text
                else:
                    raw_text = flatten_sefaria_text(he_text)

        if raw_text:
            # Clean the text (remove any remaining nikud, taamim, non-Hebrew)
            cleaned = clean_hebrew_text(raw_text)
            if cleaned:
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        f.write(cleaned)
                except Exception:
                    pass  # Cache cleanup failed; stale cache is acceptable
                return cleaned
    except requests.Timeout:
        logger.error(f"Timeout fetching {ref}")
    except Exception as e:
        logger.error(f"Error fetching {ref}: {e}")

    return ""


def create_parallels_page(initial_text: str = None):
    """Create the parallels (composition) search page."""

    # === State ===
    class ParallelsState:
        def __init__(self):
            self.is_running = False
            self.is_cancelled = False
            self.progress = 0
            self.status = ""
            self.results = []
            self.filtered_results = []
            self.finished_animation_shown = False
            self.update_timer = None  # asyncio.Task for progress update loop
            # Search timing state
            self.search_start_time: float = 0.0
            self.chunks_processed: int = 0
            self.chunks_total: int = 0
            self.last_eta_update: float = 0.0
            self.last_eta_text: str = ""
            # Domain filter state
            self.all_result_domains: dict = {}  # sys_id -> list of domain names
            self.domain_exclusions: set = set()
            self.has_domain_data: bool = False
            self.domain_name_map: dict = {}  # English domain name -> Hebrew name
            self.domain_hierarchy: dict = {}  # cached hierarchy from get_domain_hierarchy()
            self.printed_ids: set = set()  # sys_ids with FragmentMaterial=Printed
            # Pre-search filter state (Advanced Filters panel)
            self.filter_domains: list = []
            self.filter_authors: list = []
            self.filter_works: list = []
            self.filter_include_mode: bool = True
            self.filter_date_from: int = None
            self.filter_date_to: int = None
            self.filter_material_exclude: list = []
            self.filter_text_all: list = []
            self.filter_text_any: list = []
            self.filter_text_not: list = []
            self.filter_manuscript_count: int = None
            self.restrict_sys_ids: set = None
            self.excluded_manuscript_ids: set = set()   # Per-manuscript exclusions (NEW for web)
            self.auto_excluded_source_id: str = None    # Auto-excluded source manuscript
            # Translation enrichment (Phase 46-07)
            self.title_translations: dict = {}  # sys_id -> {hebrew_title, english_title, ...}
            self.translation_data: dict = {}  # sys_id -> {description_he, document_type_he}
            # DMF-09: library filter for parallels page (Phase 131-05)
            self.library_filter: list = []   # active library codes (for filter)
            self.library_mode: str = 'hide'  # 'show_only' | 'hide' (D-05 default)
            # --- Multi-witness letter-level search --------------------------
            # One work survives in many manuscripts and no single witness of
            # it retrieves every other (17 Birkat Hamazon witnesses searched
            # SEPARATELY and merged reach 85% of the reachable census, against
            # 50-69% for any one of them).
            #
            # `witnesses` are the user's own entries; the SEED (the pasted
            # source text) is modelled as a virtual witness with
            # id=WITNESS_SEED_ID -- not listed in the panel, since it is
            # already on screen, but tagged identically, so "found by 3 of 5"
            # needs no +1 special case anywhere.
            #
            # `witness_rows` maps witness id -> that witness's OWN result
            # rows, in engine order. Keeping them separate is what makes the
            # page's incremental model work: adding a witness searches ONLY
            # that witness and re-fuses, so an R-round auto-expansion costs
            # 1 + rounds x K searches rather than re-running everything on
            # every addition.
            self.witnesses: list = []
            self.witness_rows: dict = {}
            self.witness_filtered: dict = {}
            self.witness_seq: int = 0          # for unique ids across removals
            self.checked_for_promotion: set = set()
            self.auto_expanding: bool = False
            self.witness_progress: str = ''
            # The dispatch-time passage configuration of the LAST search, so
            # a witness added afterwards is searched with the same settings
            # the rows beside it were found with. A witness searched at a
            # different width/depth than its neighbours would be fused into
            # one list with them and be invisible as an anomaly.
            self.last_passage_ctx: dict = {}
            # True while _promote_checked is fetching texts off the loop.
            self.promoting: bool = False

    p_state = ParallelsState()

    # DMF-09: LOCAL helper for Show-only all-selected -> [] normalization (Codex N2).
    # Mirrors search.py's nested _library_apply_selection but defined here (NOT imported
    # from search.py — that function is a nested closure and is NOT module-level importable;
    # referencing it would NameError at runtime).
    def _parallels_apply_selection(checked_codes, all_codes):
        """Return [] when all codes checked (= show all / clear Show-only), else return the subset.

        Mirrors web/pages/search.py::_library_apply_selection (1670-1683).
        Codex N2 contract: defined locally in parallels.py, NOT imported from search.py.
        """
        if set(checked_codes) == set(all_codes):
            return []
        return list(checked_codes)

    # DMF-09: dual-mode post-fetch filter for parallels results (Phase 131-05).
    # Used for Hide mode (Show-only is scoped pre-query via restrict_sys_ids).
    # Apply BEFORE the export+storage writes so exports + stored payloads are scoped (Codex MED #6).
    def _apply_parallels_library_filter(results_list, mode, codes):
        """Dual-mode filter parallels results by selected library codes.

        Mirrors web/pages/search.py::_apply_library_filter (3830-3853).
        Show-only: keep rows where library_code IN codes.
        Hide: keep rows where library_code NOT IN codes.
        Empty codes in either mode = show all (D-05/D-08).

        Library code resolution: tries row['library_code'] first, then
        row['display']['library_code'], then meta_mgr lookup via raw_header sys_id.
        (Show-only is scoped pre-query so this is mainly used for Hide.)

        Round 6 (Codex P2): `mode` and `codes` are explicit parameters, not
        p_state reads. The post-search caller passes its dispatch-time
        captures so rows are filtered by the same values they are
        fingerprinted with; the re-render caller passes the live selection
        the user just applied. An internal p_state read let a selection
        change DURING a search filter rows with values the fingerprint never
        described. The pure mirror in tests/test_parallels_library_filter.py
        has carried this exact signature all along.
        """
        codes = set(codes or ())
        if not codes:
            return results_list  # empty codes = show all in either mode (D-05/D-08)

        def _get_lib_code(item):
            # Try direct fields first (fast path)
            lc = item.get('library_code', '')
            if lc:
                return lc
            lc = item.get('display', {}).get('library_code', '')
            if lc:
                return lc
            # Fallback: resolve via meta_mgr from raw_header sys_id
            if state.meta_mgr:
                try:
                    raw_header = item.get('raw_header', '')
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        return state.meta_mgr.get_library_for_id(sys_match.group(1)) or ''
                except Exception:
                    pass
            return ''

        if mode == 'show_only':
            # DMF-10: LOCAL rows are not user-selectable; exclude them symmetrically so
            # Show-only and Hide both treat LOCAL the same way (never in the filter set).
            return [r for r in results_list
                    if _get_lib_code(r) in codes and _get_lib_code(r) != 'LOCAL']
        else:  # hide
            return [r for r in results_list if _get_lib_code(r) not in codes]

    _PARALLELS_ACTIVE_TAB_KEY = 'parallels_active_snapshot'
    _PARALLELS_ACTIVE_TAB_VERSION = 1
    _PARALLELS_ACTIVE_USER_FALLBACK_LIMIT = 250

    def _get_tab_storage():
        try:
            return app.storage.tab
        except Exception:
            return None

    def _compact_result_rows(rows: list) -> list:
        compacted = []
        for row in rows or []:
            compacted.append(dict(row) if isinstance(row, dict) else row)
        return compacted

    def _get_active_snapshot() -> dict:
        tab = _get_tab_storage()
        if not tab:
            return {}
        raw = tab.get(_PARALLELS_ACTIVE_TAB_KEY)
        if not isinstance(raw, dict):
            return {}
        if raw.get('version') != _PARALLELS_ACTIVE_TAB_VERSION:
            return {}
        return raw

    def _persist_active_snapshot() -> None:
        tab = _get_tab_storage()
        if not tab:
            return
        try:
            tab[_PARALLELS_ACTIVE_TAB_KEY] = {
                'version': _PARALLELS_ACTIVE_TAB_VERSION,
                'results': _compact_result_rows(p_state.results[:500]),
                'filtered_results': _compact_result_rows((p_state.filtered_results or [])[:500]),
                # True pre-truncation sizes (PR #325 review): the [:500] cap
                # above is deliberate (the 778 MB search_history.json lesson),
                # so a restore that trims the tail must be able to SAY so
                # instead of silently presenting 500 as everything.
                'results_total': len(p_state.results),
                'filtered_total': len(p_state.filtered_results or []),
                'search_fingerprint': getattr(p_state, 'search_fingerprint', '') or '',
                'domain_exclusions': sorted(p_state.domain_exclusions),
                'excluded_manuscript_ids': sorted(p_state.excluded_manuscript_ids),
                'source_text': (getattr(p_state, 'searched_source_text', '')
                                or (text_input.value
                                    if 'text_input' in locals()
                                    else decoded_text)),
                'search_config': dict(
                    getattr(p_state, 'searched_config', None) or {}),
                # The witness LIST, so a reload does not lose seventeen
                # pasted texts. Deliberately NOT part of `search_config`:
                # that dict is re-applied by _apply_restored_search_config,
                # which validates every value against a widget's `.options`,
                # and a witness is not a select.
                #
                # A PROMOTED witness is stored WITHOUT its text (re-fetchable
                # from its sys_id, like title_translations today); a PASTED
                # one keeps its text, because nothing else in the world has
                # it. Worst case is bounded by the two caps at 25 x 20,000
                # ~= 500 KB -- small beside the incidents that motivated
                # _EXPORT_RESULTS_CAP, which came from thousands of result
                # rows carrying content, not a bounded list of typed queries.
                'witnesses': [
                    {'id': w.get('id'), 'label': w.get('label'),
                     'kind': w.get('kind'), 'sys_id': w.get('sys_id'),
                     'seed_digest': w.get('seed_digest') or '',
                     # The pages a PROMOTED witness was built from. Kept
                     # instead of its text, which is the cheaper half of the
                     # same guarantee -- a header is ~45 characters against a
                     # page of manuscript -- and the only thing that makes the
                     # rebuilt witness the same witness.
                     'headers': (list(w.get('headers') or [])
                                 if w.get('kind') == 'manuscript' else []),
                     'text': ('' if w.get('kind') == 'manuscript'
                              else (w.get('text') or ''))}
                    for w in (p_state.witnesses or [])
                ],
            }
        except Exception:
            pass

    def _clear_active_snapshot() -> None:
        tab = _get_tab_storage()
        if not tab:
            return
        try:
            tab.pop(_PARALLELS_ACTIVE_TAB_KEY, None)
        except Exception:
            pass

    # --- Incoming filters from catalog browse (Path B: browse -> parallels) ---
    _filters_from_browse = consume_incoming_filters(p_state, 'parallels', require_from_browse=False)

    # Restore filter state from session (only if NOT from browse, browse takes priority)
    if not _filters_from_browse:
        load_filter_state(p_state, 'parallels')

    # Restore per-manuscript exclusions from session.
    # 2026-05-12: pruned-session AssertionError fix — safe_user_get returns
    # default on session-prune races so the page renders empty instead of 500.
    from web.safe_storage import safe_user_get as _safe_get
    _emi = _safe_get('parallels_excluded_manuscript_ids')
    p_state.excluded_manuscript_ids = set(_emi) if _emi is not None else set()

    def _has_active_filters() -> bool:
        """Check if any pre-search filters are active."""
        return has_active_filters(p_state)

    # Restore domain exclusions for parallels
    _pde = _safe_get('parallels_domain_exclusions')
    p_state.domain_exclusions = set(_pde) if _pde is not None else set()

    # DMF-09: restore library filter mode + codes (key 'parallels_library_filter')
    # Mirrors search.py:189-216 D-06 migration pattern.
    _plib_raw = _safe_get('parallels_library_filter', None)
    if isinstance(_plib_raw, list):
        # D-06 legacy migration: plain list -> Show-only (v8.3.0 values were inclusion-only lists).
        _plib_codes = sanitize_library_codes(_plib_raw)
        if _plib_codes:
            p_state.library_mode = 'show_only'
            p_state.library_filter = _plib_codes
        else:
            p_state.library_mode = 'hide'
            p_state.library_filter = []
    elif isinstance(_plib_raw, dict):
        _pm = _plib_raw.get('mode', 'hide')
        _plib_codes = sanitize_library_codes(_plib_raw.get('codes'))
        _pm = _pm if _pm in ('show_only', 'hide') else 'hide'
        # Normalize invalid show_only+empty to neutral (Codex HIGH fix, mirrors search.py:206-210)
        if _pm == 'show_only' and not _plib_codes:
            _pm = 'hide'
        p_state.library_mode = _pm
        p_state.library_filter = _plib_codes
    else:
        p_state.library_mode = 'hide'
        p_state.library_filter = []

    # Restore previous results
    _restored_search_config = {}

    def _apply_restored_identity_state():
        """Re-apply the restored search's p_state-level identity inputs.

        The library scope and advanced filters are persisted independently
        the moment the user edits them -- so after run-search-A, edit a
        filter, reload, the page showed A's rows beside the NEWER filter
        state, and a re-run was a different search (Codex P2 on PR #326).
        The snapshot already wins for exclusions (just below); this makes
        it win for the remaining identity inputs. Browse-handoff filters
        keep priority: they are fresher intent than any snapshot.
        """
        cfg = _restored_search_config
        if not isinstance(cfg, dict) or not cfg:
            return
        try:
            _cfg_mode = cfg.get('library_mode')
            if _cfg_mode in ('show_only', 'hide'):
                _cfg_codes = sanitize_library_codes(cfg.get('library_filter'))
                if _cfg_mode == 'show_only' and not _cfg_codes:
                    _cfg_mode = 'hide'
                p_state.library_mode = _cfg_mode
                p_state.library_filter = _cfg_codes
            if not _filters_from_browse and 'filters' in cfg:
                # None is meaningful: search A ran WITHOUT filters, so any
                # later-edited filter state is cleared back to defaults.
                _f = cfg.get('filters')
                _f = _f if isinstance(_f, dict) else {}
                p_state.filter_domains = list(_f.get('domains') or [])
                p_state.filter_authors = list(_f.get('authors') or [])
                p_state.filter_works = list(_f.get('works') or [])
                p_state.filter_include_mode = bool(_f.get('include_mode', True))
                p_state.filter_date_from = _f.get('date_from')
                p_state.filter_date_to = _f.get('date_to')
                p_state.filter_material_exclude = list(_f.get('material_exclude') or [])
                p_state.filter_text_all = list(_f.get('text_all') or [])
                p_state.filter_text_any = list(_f.get('text_any') or [])
                p_state.filter_text_not = list(_f.get('text_not') or [])
        except Exception:
            pass  # A broken snapshot costs the restore, never the page

    def _restore_witnesses_from_snapshot(snapshot: dict) -> None:
        """Rebuild the witness list after a reload.

        Storing witnesses in the snapshot does nothing on its own -- the
        restore path applies known primitive controls only, and search
        history records the seed and config without witness inputs. Without
        this, a restored multi-witness search would silently re-run as
        seed-only while LOOKING identical, which is the worst failure this
        feature could have.

        Every restored witness comes back `pending`, and the per-witness row
        cache is NOT reconstructed. That is deliberate: the snapshot holds
        the FUSED rows, from which per-witness ranks cannot be recovered, and
        a fusion rebuilt from incomplete inputs would be quietly wrong rather
        than visibly absent. The rows already on screen are still shown; the
        panel says the witnesses need re-running, and pressing Find Parallels
        searches all of them again.
        """
        # The rules live in `restore_witness_entries` (module level, pure,
        # directly tested) -- a mutation sweep proved that a copy of them
        # inside this closure was covered by nothing.
        restored = restore_witness_entries(
            snapshot.get('witnesses'), tr('Pasted text'))
        p_state.witnesses = restored
        p_state.witness_seq = len(restored)
        p_state.witness_rows = {}
        p_state.witness_filtered = {}

    _active_snapshot = _get_active_snapshot()
    if _active_snapshot:
        try:
            p_state.results = _active_snapshot.get('results', []) or []
            p_state.filtered_results = _active_snapshot.get('filtered_results', []) or []
            # isinstance, not bare dict(): dict('chunk') raises and the
            # single try/except would then skip EVERY later restore step --
            # exclusions, fingerprint, richer-row recovery, the notice --
            # over one corrupt key (workflow review W3).
            _raw_search_config = _active_snapshot.get('search_config')
            _restored_search_config = (dict(_raw_search_config)
                                       if isinstance(_raw_search_config, dict)
                                       else {})
            _apply_restored_identity_state()
            _restore_witnesses_from_snapshot(_active_snapshot)
            p_state.domain_exclusions = set(_active_snapshot.get('domain_exclusions', []))
            p_state.excluded_manuscript_ids = set(_active_snapshot.get('excluded_manuscript_ids', []))
            # Phase 88: per-session export payload is the sole writer path (singleton mirror removed).
            # Per D-13 (Refinement 3 audit): thread snapshot's source_text into meta so export
            # handlers can echo it in the envelope without falling back to a legacy storage key.
            _snapshot_source_text = _active_snapshot.get('source_text', '') or ''
            p_state.searched_source_text = _snapshot_source_text
            _snapshot_meta = {'source_text': _snapshot_source_text}
            _snapshot_fp = _active_snapshot.get('search_fingerprint') or ''
            if _snapshot_fp:
                _snapshot_meta['search_fingerprint'] = _snapshot_fp
                p_state.search_fingerprint = _snapshot_fp
            # Display recovery (PR #325 round 2, adopting the reviewer's
            # suggestion on its merits): the export payload persists up to
            # 5,000 rows of the SAME search and its rows ARE the display
            # shape (the page stores exactly these compacted rows as its
            # own state after every fresh search) -- so a reload recovers
            # the pager's tail from it instead of apologising for the
            # snapshot's deliberate 500-row cap.
            from web.export_state import recover_richer_parallels_rows
            p_state.results, p_state.filtered_results, _recovered = (
                recover_richer_parallels_rows(
                    p_state.results, p_state.filtered_results,
                    meta=_snapshot_meta))
            # PR #325 review (Codex P2): the display snapshot is 500 rows by
            # design, but the FULL export payload (up to 5,000 rows) survives
            # the reload in app.storage.user. Overwriting it with the display
            # fallback silently capped every post-refresh export at 500 --
            # preserve_or_set keeps the richer same-search payload and writes
            # only when nothing would be lost.
            from web.export_state import preserve_or_set_parallels_export
            preserve_or_set_parallels_export(
                results=p_state.results,
                filtered=p_state.filtered_results,
                meta=_snapshot_meta,
            )
            # And the DISPLAY truncation stops being silent: the snapshot
            # records the true totals, so a trimmed restore says what it
            # trimmed and how to get it back (a passage re-run is <1s).
            # Filtered-aware (PR #325 round 2, Codex P2): a search whose
            # FILTERED bucket lost its tail was announced as complete when
            # only results_total was compared. Both buckets count, and the
            # arithmetic lives in a unit-tested helper -- the first version
            # sat inline in this closure, its "mutation proof" matched zero
            # tests, and pytest's exit-5 masqueraded as red in the harness.
            from web.export_state import parallels_restore_shortfall
            _restored_shown, _restored_total = parallels_restore_shortfall(
                _active_snapshot, p_state.results, p_state.filtered_results)
            if _restored_total > _restored_shown:
                ui.notify(
                    # Round 7 (Codex P2): the controls sit at build-time
                    # defaults after a reload, so an UNCONDITIONAL "run
                    # the search again" would run a DIFFERENT search.
                    # State the condition instead of promising. Whether a
                    # reload should restore the configuration itself is
                    # the owner decision filed in docs/OPEN_ISSUES.md.
                    tr('Restored {shown} of {total} results from the last '
                       'search — to see the full list, run the search '
                       'again with its original settings.'
                       ).format(shown=_restored_shown,
                                total=_restored_total),
                    type='info',
                )
        except Exception:
            pass  # Snapshot restore failed; page falls back to empty
    else:
        # Phase 88: safe_user_get guards bootstrap reads (Codex HIGH, 2026-05-12).
        _legacy_results = _safe_get('parallels_results')
        if _legacy_results is not None:
            try:
                p_state.results = _legacy_results or []
                _legacy_filtered = _safe_get('parallels_filtered', []) or []
                # Phase 88 D-13: fold the legacy parallels_source_text storage key into the
                # per-session export meta so the API export handlers read source_text from
                # the canonical export payload (legacy fallback in api.py is deleted in 88-02).
                # If the legacy session lost its source_text (storage cleared, partial state),
                # we always populate meta with an explicit empty string so the export envelope
                # carries a known shape — bucket (b) positive export with empty source_text.
                _legacy_source_text = _safe_get('parallels_source_text', '') or ''
                _bootstrap_meta = {'source_text': _legacy_source_text}
                # Round 6: fallback rows written by stamped code carry their
                # search identity in a sibling key -- fold it in so
                # _same_parallels_search can VERIFY same-search. Rows from
                # before the stamp leave meta bare, and the mixed-pair rule
                # fails closed rather than trusting source_text.
                _legacy_fingerprint = _safe_get(
                    'parallels_results_fingerprint', '') or ''
                if _legacy_fingerprint:
                    _bootstrap_meta['search_fingerprint'] = _legacy_fingerprint
                # Workflow review W4: the config lived only in the TAB
                # snapshot, so this branch -- a second tab, or a tab whose
                # snapshot was lost -- restored the rows with build-default
                # controls, recreating the exact defect the snapshot path
                # fixed. The per-user mirror is written beside the rows it
                # describes, at the same dispatch moment.
                _raw_user_config = _safe_get('parallels_search_config', None)
                if isinstance(_raw_user_config, dict) and _raw_user_config:
                    _restored_search_config = dict(_raw_user_config)
                _apply_restored_identity_state()
                p_state.searched_source_text = _legacy_source_text
                # Workflow review (P1): this branch runs whenever the TAB has
                # no snapshot -- opening the page in a second tab is enough.
                # It used to call set_parallels_export() directly, overwriting
                # the per-USER export payload (up to 5,000 rows) with the
                # 250-row `parallels_results` fallback it just read. Harmless
                # while results were capped near 200; with the uncapped fetch
                # this PR introduces, a 3,000-row search silently exported 250
                # rows after opening a second tab. Use the same preserve-and-
                # recover pair the snapshot branch above uses -- one restore
                # semantics, not two.
                from web.export_state import (
                    preserve_or_set_parallels_export,
                    recover_richer_parallels_rows,
                )
                p_state.results, _legacy_filtered, _bootstrap_recovered = (
                    recover_richer_parallels_rows(
                        p_state.results, _legacy_filtered,
                        meta=_bootstrap_meta))
                preserve_or_set_parallels_export(
                    results=p_state.results,
                    filtered=_legacy_filtered,
                    meta=_bootstrap_meta,
                )
                p_state.filtered_results = _legacy_filtered
            except Exception:
                pass  # Browser storage operation failed; preference not persisted

    # Decode initial text from URL or restore from storage
    decoded_text = ""
    source_sys_id = None  # For auto-exclude source manuscript
    if initial_text:
        try:
            decoded_text = unquote(initial_text)
            # Try to determine source sys_id from initial_text URL params
            # The initial_text might encode sys_id info, or we check query params
            # NiceGUI query params are available via app.storage.user or client
        except Exception:
            decoded_text = initial_text  # Operation failed; use fallback value
    else:
        # Try to restore from storage
        decoded_text = (_active_snapshot or {}).get('source_text') or _safe_get('parallels_source_text', '')

    # Auto-exclude source manuscript when launched from another module
    if initial_text and state.meta_mgr:
        try:
            # Try to find a sys_id reference in the URL text (e.g., "99NNN...")
            sys_match = re.search(r'(99\d{8,})', initial_text)
            if sys_match:
                source_sys_id = sys_match.group(1)
                p_state.auto_excluded_source_id = source_sys_id
                p_state.excluded_manuscript_ids.add(source_sys_id)
                persist_value('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
        except Exception:
            pass  # Browser storage operation failed; preference not persisted

    # --- Composition History Management ---
    def _get_comp_history() -> list:
        """Get composition search history from storage."""
        return safe_user_get('composition_history', [])

    def _add_to_comp_history(title: str, result_count: int, params: dict, state_snapshot: dict):
        """Add or update a composition history entry. Deduplicates by title."""
        if not safe_user_get('session_persistence_enabled', True):
            return
        limit = safe_user_get('search_history_limit', 20)
        history = _get_comp_history()

        # Dedup by title
        existing_idx = None
        for i, entry in enumerate(history):
            if entry.get('title') == title:
                existing_idx = i
                break

        compact_state = dict(state_snapshot or {})
        compact_state.pop('results', None)
        compact_state.pop('filtered_results', None)

        entry = {
            'title': title,
            'result_count': result_count,
            'timestamp': datetime.now().isoformat(),
            'params': params,
            'state': compact_state,
        }

        if existing_idx is not None:
            history.pop(existing_idx)  # Remove old position
            history.insert(0, entry)   # Move to front with updated data
        else:
            history.insert(0, entry)   # Add at front (newest first)

        # Enforce limit
        history = history[:limit]
        safe_user_set('composition_history', history)

    def _delete_comp_history_entry(index: int):
        """Delete a specific composition history entry by index."""
        history = _get_comp_history()
        if 0 <= index < len(history):
            history.pop(index)
            safe_user_set('composition_history', history)

    def _clear_comp_history():
        """Clear all composition search history."""
        safe_user_set('composition_history', [])

    # === UI Layout ===

    # Library filter dialog JS helpers (separate parLibFilter* namespace from domainFilter/libFilter)
    ui.add_head_html('''<script>
    // DMF (Phase 131-05) parallels library filter JS — parLibFilter* namespace.
    // Mode-aware Apply enable: Show-only with zero checked -> disable; Hide allows empty.
    function parLibFilterUpdateApply(cid) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        var cbs = cont.querySelectorAll('.par-lib-cb');
        var n = 0;
        cbs.forEach(function(cb) { if (cb.checked) n++; });
        var btn = document.getElementById('parLibApplyBtn_' + cid);
        if (!btn) return;
        var mode = cont.getAttribute('data-libmode') || 'hide';
        btn.disabled = (mode === 'show_only' && n === 0);
    }
    function parLibFilterGetChecked(cid) {
        var cont = document.getElementById(cid);
        if (!cont) return [];
        var result = [];
        cont.querySelectorAll('.par-lib-cb:checked').forEach(function(cb) { result.push(cb.dataset.code); });
        return result;
    }
    function parLibFilterSelectAll(cid, val) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        cont.querySelectorAll('.par-lib-cb').forEach(function(cb) { cb.checked = val; });
        parLibFilterUpdateApply(cid);
    }
    // Set mode attribute and reset checkboxes (D-04: mode flip clears selection).
    function parLibFilterSetMode(cid, mode) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        cont.setAttribute('data-libmode', mode);
        cont.querySelectorAll('.par-lib-cb').forEach(function(cb) { cb.checked = false; });
        parLibFilterUpdateApply(cid);
    }
    // Text-search row filter: hide rows whose label text does not contain the typed substring.
    function parLibFilterSearch(cid, query) {
        var cont = document.getElementById(cid);
        if (!cont) return;
        var q = query.toLowerCase().trim();
        cont.querySelectorAll('.par-lib-cb-row').forEach(function(row) {
            if (!q) { row.style.display = ''; return; }
            var label = (row.getAttribute('data-label') || '').toLowerCase();
            row.style.display = (label.indexOf(q) >= 0) ? '' : 'none';
        });
    }
    </script>''')

    # Domain filter dialog JS helpers (must be at page level for inline onchange handlers)
    # Functions accept containerId parameter for unique dialog instances
    ui.add_head_html('''<script>
    function domainFilterParentChanged(parentCb) {
        try {
            var children = JSON.parse(parentCb.getAttribute('data-children') || '[]');
            var container = parentCb.closest('[id^="domain-filter-"]');
            if (!container) return;
            for (var i = 0; i < children.length; i++) {
                var childCb = container.querySelector(
                    'input[data-domain="' + CSS.escape(children[i]) + '"]'
                );
                if (childCb) childCb.checked = parentCb.checked;
            }
        } catch(e) { console.error('domainFilterParentChanged:', e); }
    }
    function domainFilterSelectAll(containerId, checked) {
        var container = document.getElementById(containerId);
        if (!container) return;
        var cbs = container.querySelectorAll('input[type="checkbox"]');
        for (var i = 0; i < cbs.length; i++) cbs[i].checked = checked;
    }
    function domainFilterGetExcluded(containerId) {
        var container = document.getElementById(containerId);
        if (!container) return [];
        var excluded = [];
        var cbs = container.querySelectorAll('input[type="checkbox"]');
        for (var i = 0; i < cbs.length; i++) {
            if (!cbs[i].checked) excluded.push(cbs[i].getAttribute('data-domain'));
        }
        return excluded;
    }
    </script>''')

    with ui.column().classes('w-full max-w-7xl mx-auto gap-6 fade-in'):

        # === Page Header ===
        with ui.row().classes('w-full items-center justify-between'):
            with ui.column().classes('gap-1'):
                # Changed to H1
                h1(tr('Find Parallels'), classes='text-3xl font-bold', style='color: var(--text-primary);')
                ui.label(tr('Discover parallel texts in the Genizah corpus')).style('color: var(--text-secondary);')

        # === Input Section ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('w-full gap-6'):

                # Left: Text Input.
                #
                # `min-w-0` is load-bearing, not decoration: a flex item's
                # default `min-width: auto` lets a wide child set the column's
                # minimum width, which pushed the `w-80` options pane below
                # the text instead of beside it (owner-reported 2026-08-25).
                with ui.column().classes('flex-grow min-w-0 gap-4'):
                    # Changed to H2
                    h2(tr('Source text'), classes='text-xl font-bold', style='color: var(--text-primary);')

                    text_input = ui.textarea(
                        placeholder=tr('Paste your Hebrew text here...'),
                        value=decoded_text
                    ).classes('w-full').props('outlined rows=8').style('direction: rtl;')

                    # Word count
                    word_count_label = ui.label('0 ' + tr('Words')).classes('text-sm').style('color: var(--text-muted);')

                    def update_word_count():
                        text = text_input.value or ""
                        words = len([w for w in text.split() if w])
                        word_count_label.text = f"{words} {tr('Words')}"
                        # Save text to storage for persistence
                        safe_user_set('parallels_source_text', text)

                    text_input.on('update:model-value', update_word_count)
                    # Also update on blur to catch paste events
                    text_input.on('blur', update_word_count)
                    # Update after a short delay to ensure textarea has initial value from storage
                    async def _deferred_word_count():
                        await asyncio.sleep(0.3)
                        try:
                            update_word_count()
                        except (RuntimeError, Exception):
                            pass
                    asyncio.ensure_future(_deferred_word_count())

                    # === Witnesses (letter-level multi-witness search) ===
                    # One work survives in many manuscripts, and no single
                    # witness of it retrieves every other -- 17 Birkat
                    # Hamazon witnesses searched SEPARATELY and merged reach
                    # 85% of the reachable census against 50-69% for any one
                    # of them. Shown only in letter-level mode: the chunk
                    # engine has no per-query budget to starve, and
                    # multi-witness there measured +2 positives of 74 with
                    # zero frontier gain at 4-6x the time.
                    with ui.column().classes('w-full gap-2') as witness_panel:
                        # ONE collapsed line by default (owner, 2026-08-24:
                        # "the witnesses option should be less prominent").
                        # Most searches use a single text; an always-open
                        # block pushed the primary controls down the page for
                        # a feature they never touch.
                        #
                        # It must not be invisible when it has something to
                        # say, so the caption carries the live counts and
                        # `_refresh_witness_panel` OPENS it whenever a witness
                        # needs attention -- stale, pending or failed.
                        with ui.expansion(
                                tr('Witnesses'), icon='groups',
                        ).classes('w-full').props('dense') as witness_expansion:
                            with ui.column().classes('w-full gap-2 p-1'):
                                witness_empty_label = ui.label(tr(
                                    'Add other copies of this work to search '
                                    'with. Each is searched on its own and '
                                    'the results are merged.'
                                )).classes('text-xs').style(
                                    'color: var(--text-muted);')
                                witness_list = ui.column().classes('w-full gap-1')
                                # Appears when the source text changed under a
                                # witness list gathered for a different work.
                                witness_stale_row = ui.column().classes('w-full')
                                witness_stale_row.set_visibility(False)
                                with ui.row().classes('w-full items-center gap-2'):
                                    ui.button(
                                        tr('Add witness text'), icon='add',
                                        on_click=lambda: _open_add_witness_dialog(),
                                    ).props('flat dense no-caps size=sm')
                                    ui.space()
                                with ui.row().classes(
                                        'w-full items-center gap-2') as witness_run_row:
                                    witness_run_btn = ui.button(
                                        tr('Search now'), icon='play_arrow',
                                        on_click=lambda: (
                                            _clear_stop(),
                                            _search_pending_witnesses())[-1],
                                    ).props('outline dense no-caps size=sm')
                                    witness_progress_label = ui.label('').classes(
                                        'text-xs').style('color: var(--text-muted);')
                                witness_run_row.set_visibility(False)

                                # Auto-expand: promote the best results and
                                # search with them too. An EXPLICIT button,
                                # never folded into "Find Parallels" -- a user
                                # who wanted one search must not get twenty.
                                #
                                # A plain section, not a nested expansion: an
                                # expansion inside an expansion is two clicks
                                # to reach a control and reads as a
                                # sub-feature of a sub-feature.
                                ui.separator().classes('my-1')
                                with ui.column().classes('w-full gap-2'):
                                    ui.label(tr('Auto-expand (optional)')).classes(
                                        'text-xs font-bold').style(
                                        'color: var(--text-secondary);')
                                    ui.label(tr(
                                        'Repeatedly search with the best results '
                                        'as new witnesses. Reach goes up and '
                                        'top-of-list precision goes down.'
                                    )).classes('text-xs').style(
                                        'color: var(--text-muted);')
                                    with ui.row().classes('items-center gap-3'):
                                        auto_rounds = ui.number(
                                            label=tr('Rounds'), value=3, min=1,
                                            max=5, step=1,
                                        ).props('outlined dense').classes('w-28')
                                        auto_top_k = ui.number(
                                            label=tr('Top-K per round'), value=5,
                                            min=1, max=10, step=1,
                                        ).props('outlined dense').classes('w-32')
                                    auto_expand_btn = ui.button(
                                        tr('Run auto-expand now'),
                                        icon='auto_awesome',
                                        on_click=lambda: _run_auto_expand(),
                                    ).props('outline dense no-caps size=sm')
                                    auto_expand_btn.disable()
                    witness_panel.set_visibility(False)

                    # === Lab Mode and Boundary Search Settings (below text input) ===
                    ui.separator().classes('my-3')

                    # Search-method row. Lab Mode used to sit here, beside
                    # the two radio options, which read as though it were a
                    # third one; it is a different BACKEND, mutually exclusive
                    # with both, and it now lives in the options pane with the
                    # chunk settings it belongs to (owner, 2026-08-25).
                    with ui.row().classes('w-full items-center gap-4'):
                        # Method selector (owner ruling 2026-08-23): letter-level
                        # search is the DEFAULT when available, chunk is the explicit
                        # alternative -- a radio, no longer an opt-in checkbox. The flip
                        # rests on the owner's own row-by-row grading of live GUI runs
                        # (docs/specs/parallels-method-comparison.md, 2026-08-23
                        # sections): precision 94%, recall parity with chunk-4 plus 7
                        # novel verified witnesses, ~0.6s vs minutes. Visible ONLY when
                        # passage_available() (flag AND a loaded index) -- a clean hide,
                        # value pinned to 'chunk', when the index is not deployed.
                        # Mutually exclusive with Lab Mode (both are "pick a different
                        # backend" toggles; a request can only use one at a time).
                        method_radio = ui.radio(
                            options={
                                'passage': tr('New! Letter-level search'),
                                'chunk': tr('Chunk search (slower)'),
                            },
                            value='passage',
                        ).props('inline dense')
                        def _letter_level_selected() -> bool:
                            return method_radio.value == 'passage'

                        if not passage_available():
                            method_radio.value = 'chunk'
                            method_radio.style('display: none;')

                    # A live help line, NOT a tooltip: `ui.radio` renders one
                    # QOptionGroup, so a tooltip attached to it fires for BOTH
                    # options -- hovering "Chunk search" described letter-level
                    # search (owner-reported 2026-08-25).
                    #
                    # OUTSIDE the row above, mirroring `boundary_mode_help`:
                    # inside it, this sentence's min-content width propagated
                    # out through the flex column and pushed the options pane
                    # off its side of the card.
                    method_help = ui.label('').classes(
                        'text-xs mt-1').style('color: var(--text-muted);')

                    _METHOD_HELP = {
                        # The passage claim names speed and precision and
                        # stops. Everything trimmed from it described the two
                        # engines as differing where they do not: nikkud and
                        # line breaks (2026-08-25 -- both strip nikkud, both
                        # treat a newline as an ordinary separator), then
                        # tolerance of spelling and transcription differences
                        # (2026-08-26, owner review -- chunk search's Variants
                        # and Fuzzy modes do that job).
                        'passage': tr(
                            'Faster, with fewer irrelevant results.'),
                        'chunk': tr(
                            'The older method. Slower, but offers Exact / '
                            'Variants / Fuzzy modes and cross-paragraph '
                            'filtering.'),
                    }

                    def _update_method_help() -> None:
                        method_help.text = _METHOD_HELP.get(
                            method_radio.value, '')

                    method_radio.on_value_change(
                        lambda _e: _update_method_help())
                    _update_method_help()

                    # === Boundary Search Settings (CHUNK ONLY) ===
                    with ui.row().classes(
                            'w-full items-center gap-4 flex-wrap mt-2'
                    ).mark('boundary-settings') as boundary_row:
                        # Paragraph delimiter (always editable - affects display even in full mode)
                        with ui.column().classes('gap-1') as delimiter_col:
                            delimiter_label = ui.label(tr('Paragraph separator')).classes('text-xs font-medium').style('color: var(--text-muted);')
                            boundary_delimiter = ui.select(
                                options={
                                    '\n': tr('Line break'),
                                    '\n\n': tr('Blank line (paragraph)'),
                                    '.': tr('Period (.)'),
                                    ':': tr('Colon (:)')
                                },
                                value='\n'
                            ).classes('w-40').props('outlined dense')
                            boundary_delimiter.tooltip(tr('Character or pattern that separates paragraphs in your text'))

                        # Boundary mode radio buttons (visible)
                        boundary_mode = ui.radio(
                            options={
                                'full': tr('Full search'),
                                'boundary': tr('Cross-paragraph only'),
                                'combined': tr('Full + Cross-paragraph boost')
                            },
                            value='full'
                        ).props('inline dense')

                        # Advanced settings button (initially hidden)
                        advanced_btn = ui.button(icon='tune', on_click=lambda: advanced_dialog.open()).props('flat dense').style('display: none;')
                        advanced_btn.tooltip(tr('Advanced cross-paragraph settings'))

                    # Help text for current selection
                    boundary_mode_help = ui.label('').classes('text-xs').style('color: var(--text-muted); display: none;')

                    # Tooltip descriptions for each mode
                    mode_tooltips = {
                        'full': tr('Search all text chunks regardless of paragraph breaks'),
                        'boundary': tr('Show only matches where the matching text spans a paragraph break in your source'),
                        'combined': tr('Search everything, but rank cross-paragraph matches higher')
                    }

                    def update_boundary_help():
                        mode = boundary_mode.value
                        if mode in mode_tooltips:
                            boundary_mode_help.text = mode_tooltips[mode]
                            boundary_mode_help.style('display: block;')
                        else:
                            boundary_mode_help.style('display: none;')

                    # Pre-search stats display
                    boundary_stats_label = ui.label('').classes('text-xs mt-1').style('color: var(--primary-600); display: none;')
                    boundary_warning_label = ui.label('').classes('text-xs mt-1').style('color: var(--error); display: none;')

                    # Advanced settings dialog
                    with ui.dialog() as advanced_dialog:
                        with ui.card().classes('p-4 w-96'):
                            h3(tr('Advanced cross-paragraph settings'), classes='text-lg font-bold mb-4', style='color: var(--text-primary);')

                            # Cross-paragraph boost slider
                            with ui.column().classes('gap-1 mb-4'):
                                ui.label(tr('Cross-paragraph boost')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                boundary_boost = ui.slider(min=1.0, max=3.0, value=1.5, step=0.1).props('label-always')
                                ui.label(tr('Score multiplier for cross-paragraph matches')).classes('text-xs').style('color: var(--text-muted);')

                            # Min boundary matches filter
                            with ui.column().classes('gap-1 mb-4'):
                                ui.label(tr('Min. cross-paragraph matches')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                min_boundary_matches = ui.select(
                                    options={i: str(i) for i in range(11)},
                                    value=0
                                ).classes('w-32').props('outlined dense')
                                ui.label(tr('Minimum number of cross-paragraph matches required')).classes('text-xs').style('color: var(--text-muted);')

                            # Min delimiter distance
                            with ui.column().classes('gap-1 mb-4'):
                                ui.label(tr('Min. words between separators')).classes('text-sm font-medium').style('color: var(--text-secondary);')
                                min_delimiter_distance = ui.select(
                                    options={i: str(i) for i in range(1, 11)},
                                    value=3
                                ).classes('w-32').props('outlined dense')
                                ui.label(tr('Ignore separators that are too close together')).classes('text-xs').style('color: var(--text-muted);')

                            ui.button(tr('Close'), on_click=advanced_dialog.close).props('flat')

                    def update_boundary_ui():
                        """Update boundary UI based on selected mode."""
                        is_boundary_mode = boundary_mode.value in ('boundary', 'combined')
                        # Show/hide advanced settings button based on mode
                        if is_boundary_mode:
                            advanced_btn.style('display: inline-flex;')
                        else:
                            advanced_btn.style('display: none;')
                        update_boundary_help()
                        update_boundary_stats()

                    def update_boundary_stats():
                        """Update pre-search boundary statistics.

                        Guarded HERE rather than in the method handler because
                        `text_input.on('blur', ...)` calls this directly: a
                        letter-level user who clicked out of the textarea would
                        otherwise see "N boundaries detected" reappear under a
                        method that has no paragraph boundaries at all.
                        """
                        if _letter_level_selected():
                            boundary_stats_label.style('display: none;')
                            boundary_warning_label.style('display: none;')
                            return
                        try:
                            from genizah_core import get_boundary_stats
                            text = text_input.value or ""
                            if not text.strip():
                                boundary_stats_label.style('display: none;')
                                boundary_warning_label.style('display: none;')
                                return

                            c_size = int(chunk_size.value) if chunk_size.value else 5
                            delimiter = boundary_delimiter.value or '\n'
                            min_dist = int(min_delimiter_distance.value) if min_delimiter_distance.value else 3

                            stats = get_boundary_stats(text, delimiter, c_size, min_dist)

                            if stats['boundary_count'] > 0:
                                boundary_stats_label.text = tr('{} boundaries detected, {} chunks will cross them').format(
                                    stats['boundary_count'], stats['crossing_chunk_count']
                                )
                                boundary_stats_label.style('display: block;')
                                boundary_warning_label.style('display: none;')
                            else:
                                boundary_stats_label.style('display: none;')
                                if boundary_mode.value in ('boundary', 'combined'):
                                    boundary_warning_label.text = tr('No paragraph breaks detected in text!')
                                    boundary_warning_label.style('display: block;')
                                else:
                                    boundary_warning_label.style('display: none;')
                        except Exception:
                            boundary_stats_label.style('display: none;')  # Operation failed; use fallback value
                            boundary_warning_label.style('display: none;')

                    # Update stats when relevant controls change
                    boundary_mode.on('update:model-value', update_boundary_ui)
                    boundary_delimiter.on('update:model-value', update_boundary_stats)
                    min_delimiter_distance.on('update:model-value', update_boundary_stats)
                    text_input.on('blur', update_boundary_stats)

                    # Lab Mode toggle handler
                    def on_lab_mode_change():
                        """Show/hide lab mode options based on toggle."""
                        if lab_mode.value:
                            deep_scan.style('display: inline-flex;')
                            freq_threshold_row.style('display: none;')
                            # Higher default for composition/lab mode
                            min_chunks_input.value = 3
                            # Phase 145: Lab Mode and passage matching are two
                            # different backends -- mutually exclusive, like the
                            # incumbent-vs-Lab choice already is.
                            if _letter_level_selected():
                                method_radio.value = 'chunk'
                                # NiceGUI does not fire 'update:model-value' for
                                # a programmatic .value assignment (only for a
                                # real user click), so on_passage_mode_change's
                                # boundary_mode re-enable would otherwise never
                                # run -- call it explicitly.
                                on_passage_mode_change()
                        else:
                            deep_scan.style('display: none;')
                            freq_threshold_row.style('display: block;')
                            # Lower default for regular mode
                            min_chunks_input.value = 1

                    # Phase 145: passage-matching toggle handler -- reciprocal
                    # mutual exclusivity with Lab Mode.
                    def on_passage_mode_change():
                        if _letter_level_selected() and lab_mode.value:
                            lab_mode.value = False
                            on_lab_mode_change()
                        if _letter_level_selected():
                            # Swap the pane's contents, do not merely grey it.
                            letter_options_col.set_visibility(True)
                            # Multi-witness is letter-level ONLY, and that is
                            # a measured finding rather than an assumption:
                            # on the chunk engine multi-witness bought +2
                            # positives of 74 with zero frontier gain at 4-6x
                            # the time, because concatenation and union there
                            # return the identical manuscript set.
                            # ANDs the multi-witness flag, not just
                            # letter-level: `PASSAGE_MULTI_WITNESS_ENABLED` is
                            # default-off and separate from
                            # `PASSAGE_PARALLELS_ENABLED` precisely so
                            # single-witness passage can stay broadly on while
                            # the costly fan-out is validated. Gated on
                            # letter-level alone, the page offered the whole
                            # capability while the API refused it.
                            witness_panel.set_visibility(
                                passage_multi_witness_available())
                            for _row in (mode_select, chunk_size_row,
                                         freq_threshold_row, min_chunks_row,
                                         lab_mode_row):
                                _row.set_visibility(False)
                            # Finding #2 (adversarial review): passage-matching
                            # has no cross-paragraph/token-boundary concept --
                            # PassageSearcher raises ValueError for anything but
                            # 'full', and web/search_api.py rejects it with 400
                            # 'passage_option_unsupported'. Forcing + disabling
                            # here means the UI can never even SEND the
                            # unsupported value, rather than relying only on
                            # that rejection.
                            # HIDDEN as well as disabled. The force-set and
                            # disable below stay exactly as they were -- they
                            # are what guarantees the UI can never SEND an
                            # unsupported value -- but a greyed-out control
                            # still reads as an option you might have, and
                            # letter-level search has no paragraph boundaries
                            # to offer (owner, 2026-08-25).
                            boundary_row.set_visibility(False)
                            boundary_mode_help.style('display: none;')
                            boundary_mode.value = 'full'
                            # NiceGUI fires no event for a programmatic
                            # .value write, so boundary_mode's own handler
                            # (registered below as update_boundary_ui) never
                            # ran -- the help text, stats line and Advanced
                            # button kept describing the boundary mode the
                            # user had selected before switching to
                            # letter-level. Same explicit-call rule already
                            # applied to mode_select just below.
                            update_boundary_ui()
                            boundary_mode.disable()
                            # Codex review finding #13(c): chunk_size/mode/
                            # max_freq are likewise inert for passage (no
                            # sliding-window chunk, no morphological-variant
                            # matching, no per-chunk frequency signal) --
                            # web/search_api.py rejects a non-default value of
                            # any of them with the same 400
                            # 'passage_option_unsupported', so these must be
                            # forced to their defaults and disabled too,
                            # exactly like boundary_mode above.
                            chunk_size.value = 5
                            chunk_size.disable()
                            mode_select.value = 'exact'
                            on_mode_change()  # hide variant_controls_col if it was showing
                            mode_select.disable()
                            freq_threshold.value = 50
                            freq_threshold.disable()
                            # Owner ruling 2026-08-23: "Min. chunk matches"
                            # counts CHUNKS, which letter-level search does
                            # not have. Its nearest analogue (n_spans) counts
                            # merged match spans, where one long continuous
                            # match is a SINGLE span -- so any min>1 would
                            # silently drop exactly the strongest witnesses.
                            # Force the no-op value and disable, like the
                            # knobs above.
                            min_chunks_input.value = 1
                            min_chunks_input.disable()
                        else:
                            letter_options_col.set_visibility(False)
                            # HIDDEN, never cleared: a user who switches to
                            # chunk to check something and back must not
                            # find their seventeen pasted witnesses gone.
                            witness_panel.set_visibility(False)
                            for _row in (mode_select, chunk_size_row,
                                         freq_threshold_row, min_chunks_row,
                                         lab_mode_row):
                                _row.set_visibility(True)
                            boundary_row.set_visibility(True)
                            boundary_mode.enable()
                            chunk_size.enable()
                            mode_select.enable()
                            freq_threshold.enable()
                            min_chunks_input.enable()
                            # Recompute the three inline-styled labels rather
                            # than un-hiding them: which of them belongs on
                            # screen depends on the boundary mode and on
                            # whether the text has any breaks.
                            update_boundary_ui()

                    method_radio.on('update:model-value', on_passage_mode_change)

                    # Initialize help text
                    update_boundary_help()

                # Right: Options Panel
                with ui.column().classes('w-80 gap-4'):
                    # Changed to H2
                    h2(tr('Options'), classes='text-xl font-bold', style='color: var(--text-primary);')

                    # Letter-level options live HERE, in the same pane as the
                    # chunk options they replace -- owner feedback 2026-08-24.
                    # The earlier shape put them in the method row and left
                    # this whole pane visible-but-disabled, so the options a
                    # letter-level search actually uses sat far from the four
                    # greyed-out ones it does not. One pane, contents swapped
                    # by method. The chunk controls stay force-set and
                    # disabled underneath (web/search_api.py rejects a
                    # non-default value of any of them for method='passage',
                    # so disabling remains the guarantee); hiding is
                    # presentation on top of that, never instead of it.
                    with ui.column().classes('gap-4') as letter_options_col:
                        passage_width = ui.select(
                            options={
                                'standard-40': tr('Narrow (near-exact)'),
                                'wide-40': tr('Medium width'),
                                'wider-40': tr('Wide width'),
                                'widest-40': tr('Very wide (default)'),
                                'max-40': tr('Maximal (may add noise)'),
                            },
                            value='widest-40',
                            label=tr('Match width'),
                        ).classes('w-44').props('outlined dense')
                        passage_width.tooltip(tr(
                            'How far a manuscript may drift from your text and '
                            'still match. Wider finds more noisy witnesses; the '
                            'strongest matches always rank first.'
                        ))
                        # The SECOND axis (2026-08-24). Width and passage
                        # length are different questions -- "how corrupt a
                        # copy may be" vs "how short a shared passage counts"
                        # -- and the short profile is emphatically not a
                        # wider width, so folding it into the width list
                        # would mislabel it. Two knobs, each a small list of
                        # named policies; never raw sliders, because
                        # min_span and verify_margin are ONE coupled decision
                        # (spec section 8.1) that a slider would present as
                        # two independent ones.
                        passage_length = ui.select(
                            options={
                                'normal': tr('Normal passages (default)'),
                                'short': tr('Also short passages'),
                            },
                            value='normal',
                            label=tr('Passage length'),
                        ).classes('w-44').props('outlined dense')
                        passage_length.tooltip(tr(
                            'Whether a short shared passage counts as a match. '
                            '"Also short" finds piyyutim, quotations and badly '
                            'damaged copies that share only a phrase — measured '
                            'at roughly double the results for a third fewer '
                            'correct ones, so expect to skim more.'
                        ))
                        # The THIRD axis (2026-08-24): search depth. The
                        # engine's default budgets were tuned on short
                        # queries; a full composition carries ~10M postings
                        # of which the default budget admits under 5%, so
                        # true witnesses never even reach verification --
                        # starvation, not the match boundary, is the main
                        # recall loss on long queries. Depth raises the
                        # posting/verify/candidate budgets together (they
                        # are ONE coupled decision -- more budget without
                        # more verification changes almost nothing), at a
                        # measured latency cost: ~8s for deep, ~19s for
                        # deepest on the Antiochus benchmark vs 0.6s normal.
                        # Named profiles, never sliders, like the two axes
                        # above (DEPTH_PROFILES in shared/passage_policy.py
                        # carries the full measurements).
                        passage_depth = ui.select(
                            options={
                                'normal': tr('Normal (fast, default)'),
                                'deep': tr('Deep (slower, more witnesses)'),
                                'deepest': tr('Deepest (slowest, most)'),
                            },
                            value='normal',
                            label=tr('Search depth'),
                        ).classes('w-44').props('outlined dense')
                        passage_depth.tooltip(tr(
                            'How much of the corpus the search may examine. '
                            'Deeper searches take seconds longer and return '
                            'more manuscripts — including badly damaged and '
                            'reworked copies a fast pass misses. Long texts '
                            'benefit the most.'
                        ))

                    # Mode
                    mode_select = ui.select(
                        {
                            'exact': tr('Exact'),
                            'variants': tr('Variants'),
                            'fuzzy': tr('Fuzzy'),
                        },
                        value='exact',
                        label=tr('Search Mode')
                    ).classes('w-full').props('outlined dense')

                    # Check if user prefers slider or presets (default: presets)
                    use_slider = False
                    if state.lab_engine and hasattr(state.lab_engine, 'settings') and state.lab_engine.settings:
                        use_slider = getattr(state.lab_engine.settings, 'variant_use_slider', False)

                    # Track current preset level (default: Basic=30)
                    current_preset = {'value': 30}

                    # Variables for elements
                    variant_level_select = None
                    variant_slider = variant_slider_label = None

                    # Variant Level Controls (visible only in Variants mode)
                    with ui.row().classes('items-center gap-4') as variant_controls_col:
                        if not use_slider:
                            # Dropdown selector (compact mode)
                            with ui.column().classes('gap-1'):
                                h3(tr('Level'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                variant_level_select = ui.select(
                                    {
                                        30: '○ ' + tr('Basic'),
                                        70: '◐ ' + tr('Extended'),
                                        150: '● ' + tr('Maximum'),
                                    },
                                    value=current_preset['value']
                                ).classes('w-36').props('outlined dense')

                            with ui.column().classes('gap-1'):
                                h3(tr('Num Changes'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                max_changes_select = ui.select({1: '×1', 2: '×2', 3: '×3'}, value=2).classes('w-16').props('outlined dense')
                        else:
                            # Slider mode
                            with ui.column().classes('gap-1 w-full'):
                                h3(tr('Variant Level'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                with ui.row().classes('items-center gap-2 w-full'):
                                    variant_slider = ui.slider(min=10, max=300, value=30, step=10).classes('flex-grow').props('label-always')
                                    variant_slider_label = ui.label('30').classes('text-sm font-medium w-10').style('color: var(--primary-600);')
                            with ui.column().classes('gap-1'):
                                h3(tr('Num Changes'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                                max_changes_select = ui.select({1: '×1', 2: '×2', 3: '×3'}, value=2).classes('w-16').props('outlined dense')

                    def set_level(level_value):
                        """Set variant level."""
                        current_preset['value'] = level_value
                        if state.var_mgr:
                            state.var_mgr.set_variant_level(level_value)

                    if variant_level_select:
                        def on_level_change():
                            set_level(int(variant_level_select.value))
                        variant_level_select.on('update:model-value', on_level_change)

                    if variant_slider:
                        def on_slider_change():
                            val = int(variant_slider.value)
                            current_preset['value'] = val
                            variant_slider_label.set_text(str(val))
                            if state.var_mgr:
                                state.var_mgr.set_variant_level(val)
                        variant_slider.on('update:model-value', on_slider_change)

                    def on_mode_change():
                        is_variants = mode_select.value == 'variants'
                        variant_controls_col.set_visibility(is_variants)

                    mode_select.on('update:model-value', on_mode_change)
                    # Set initial visibility (exact mode = hide variant controls)
                    variant_controls_col.set_visibility(False)

                    # Chunk Size
                    with ui.column().classes('gap-1') as chunk_size_row:
                        # Changed to H3
                        h3(tr('Chunk size'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        chunk_size = ui.slider(min=2, max=12, value=5).props('label-always')
                        ui.label(tr('Words per search chunk (recommended: 4-7)')).classes('text-xs').style('color: var(--text-muted);')

                    # Frequency threshold (chunk search only -- hidden for
                    # letter-level, which has no per-chunk frequency signal).
                    #
                    # It counts PAGE HITS, not manuscripts: the engine tests
                    # `len(hits) > max_freq` against `searcher.search(query,
                    # 50).hits`, a truncated top-50 of Tantivy documents, and a
                    # document is a page. Eleven pages of one manuscript trip a
                    # threshold of ten. The label said "manuscripts" until
                    # 2026-08-25; 5cd2bb7e had already retired that wording in
                    # docs/SEARCH_API.md and even touched this string's Hebrew,
                    # but left the English behind.
                    #
                    # The 50-hit retrieval cap also means no value at or above
                    # 50 can ever fire -- and 50 is the DEFAULT, so the control
                    # does nothing until it is dragged left. Measured in
                    # 5cd2bb7e (identical results at 50/100/1000/100000). The
                    # range and default are deliberately unchanged here:
                    # narrowing to [10, 49] forces a new default, and that
                    # changes the results of every chunk search -- a product
                    # decision, not a labelling fix.
                    with ui.column().classes('gap-1') as freq_threshold_row:
                        h3(tr('Max frequency'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        freq_threshold = ui.slider(min=10, max=100, value=50).props('label-always')
                        ui.label(tr('Skip phrases matching more than this many pages (lower = stricter; 50 or above turns it off)')).classes('text-xs').style('color: var(--text-muted);')

                    # Min chunk matches (for regular full-text chunk search)
                    with ui.column().classes('gap-1') as min_chunks_row:
                        h3(tr('Min. chunk matches'), classes='text-sm font-medium', style='color: var(--text-secondary);')
                        min_chunks_input = ui.number(
                            min=1, max=20, value=1, step=1,
                            format='%d'
                        ).classes('w-24').props('outlined dense')
                        ui.label(tr('Minimum matching chunks per manuscript')).classes('text-xs').style('color: var(--text-muted);')

                    # Lab Mode: a third BACKEND, not a third search method.
                    # It lives with the chunk settings because it is only
                    # reachable from chunk search -- selecting letter-level
                    # turns it off (`on_passage_mode_change`), so it can never
                    # be left ON behind a hidden control.
                    with ui.column().classes('gap-1').mark(
                            'lab-mode-row') as lab_mode_row:
                        lab_mode = ui.checkbox(tr('Lab Mode (experimental)'))
                        lab_mode.tooltip(tr('Advanced search using fingerprint algorithm. Slower but more features.'))

                        # Deep Scan (Lab Mode only - initially hidden)
                        deep_scan = ui.checkbox(tr('Deep Scan')).style('display: none;')
                        deep_scan.tooltip(tr('Exhaustive search - slower but finds more results'))

                    # Registered HERE, not beside `on_lab_mode_change`: this is
                    # the only BUILD-TIME reference to the widget, so it has to
                    # follow the definition above or the page 500s.
                    lab_mode.on('update:model-value', on_lab_mode_change)

                    def _apply_restored_search_config():
                        """Re-apply the snapshot's search configuration to
                        the controls, then run the method's enable/disable
                        pass.

                        Rules this encodes (each has bitten this page):
                        * NiceGUI fires no event for a programmatic .value
                          write, so every handler is called EXPLICITLY after
                          its widget is set.
                        * Every select/radio value is validated against the
                          widget's own .options before it is applied -- a
                          stale snapshot from an older build must degrade to
                          the default, never crash the page or smuggle in a
                          value the UI cannot express.
                        * Passage-inert knobs (chunk/mode/freq/min-chunks/
                          boundary) are skipped for engine='passage': the
                          final on_passage_mode_change() forces and disables
                          them, exactly as a fresh selection would.
                        * Any failure falls back to the plain build-time
                          init -- a broken snapshot costs the restore, not
                          the page.
                        """
                        cfg = _restored_search_config
                        engine = cfg.get('engine') if isinstance(cfg, dict) else None
                        if engine not in ('chunk', 'passage', 'lab'):
                            on_passage_mode_change()
                            return
                        try:
                            if engine == 'lab':
                                # Point the method radio at 'chunk' FIRST:
                                # its build default is 'passage' when the
                                # index is available, and the final
                                # on_passage_mode_change() below treats
                                # letter-level + lab as the mutual-
                                # exclusion conflict and would switch lab
                                # back OFF -- undoing this very restore.
                                method_radio.value = 'chunk'
                                if not lab_mode.value:
                                    lab_mode.value = True
                                    on_lab_mode_change()
                            elif engine == 'passage' and passage_available():
                                method_radio.value = 'passage'
                                if cfg.get('width') in passage_width.options:
                                    passage_width.value = cfg['width']
                                if cfg.get('length') in passage_length.options:
                                    passage_length.value = cfg['length']
                                if cfg.get('depth') in passage_depth.options:
                                    passage_depth.value = cfg['depth']
                            else:
                                # 'chunk', or 'passage' degrading because the
                                # index is unavailable (same rule as dispatch).
                                method_radio.value = 'chunk'
                            # The paragraph separator and min-distance
                            # are LIVE in every mode: on_passage_mode_change
                            # never forces or disables them, and
                            # update_boundary_stats reads them
                            # unconditionally for the stats line -- so they
                            # restore for the passage engine too (workflow
                            # review W1; the final on_passage_mode_change()
                            # below refreshes the stats from these values).
                            if cfg.get('boundary_delimiter') in boundary_delimiter.options:
                                boundary_delimiter.value = cfg['boundary_delimiter']
                            if cfg.get('min_delimiter_distance') in min_delimiter_distance.options:
                                min_delimiter_distance.value = cfg['min_delimiter_distance']
                            if engine != 'passage':
                                cs = cfg.get('chunk_size')
                                if isinstance(cs, (int, float)) and 2 <= cs <= 12:
                                    chunk_size.value = int(cs)
                                if cfg.get('mode') in ('exact', 'variants', 'fuzzy'):
                                    mode_select.value = cfg['mode']
                                    on_mode_change()
                                mf = cfg.get('max_freq')
                                if isinstance(mf, (int, float)) and 10 <= mf <= 100:
                                    freq_threshold.value = int(mf)
                                if cfg.get('boundary_mode') == 'full':
                                    # In 'full' mode the identity's
                                    # min_boundary_matches IS the "Min.
                                    # chunk matches" widget (the dispatch
                                    # derives one from the other); in
                                    # boundary modes the widget is inert,
                                    # so nothing is lost by not storing
                                    # it separately -- the config carries
                                    # EXACTLY the identity inputs.
                                    mc = cfg.get('min_boundary_matches')
                                    if isinstance(mc, (int, float)) and 1 <= mc <= 20:
                                        min_chunks_input.value = int(mc)
                                if engine == 'lab':
                                    deep_scan.value = bool(cfg.get('deep_scan', False))
                                if cfg.get('boundary_mode') in boundary_mode.options:
                                    boundary_mode.value = cfg['boundary_mode']
                                bb = cfg.get('boundary_boost')
                                if isinstance(bb, (int, float)) and 1.0 <= bb <= 3.0:
                                    boundary_boost.value = float(bb)
                                if (cfg.get('boundary_mode') != 'full'
                                        and cfg.get('min_boundary_matches')
                                        in min_boundary_matches.options):
                                    # In 'full' mode this cfg key holds the
                                    # min-CHUNKS value (see above); writing
                                    # it into the Advanced cross-paragraph
                                    # select would invent a filter the user
                                    # never chose (workflow review W2).
                                    min_boundary_matches.value = cfg['min_boundary_matches']
                                update_boundary_ui()
                                vl = cfg.get('variant_level')
                                if isinstance(vl, (int, float)):
                                    if (variant_level_select is not None
                                            and int(vl) in variant_level_select.options):
                                        variant_level_select.value = int(vl)
                                        on_level_change()
                                    elif variant_slider is not None and 10 <= vl <= 300:
                                        variant_slider.value = int(vl)
                                        on_slider_change()
                                vmc = cfg.get('variant_max_changes')
                                if (isinstance(vmc, (int, float))
                                        and int(vmc) in max_changes_select.options):
                                    max_changes_select.value = int(vmc)
                        except Exception:
                            pass  # Stale/foreign snapshot: keep defaults, page must build
                        on_passage_mode_change()

                    # Apply the restored (or default) method's control state
                    # on load. This call sits HERE -- after mode_select,
                    # chunk_size, freq_threshold and min_chunks_input -- an
                    # earlier call site crashed the whole page with NameError
                    # at build time (owner-reported, 2026-08-23): the widgets
                    # are created BELOW the selector block, and only a real
                    # render executes this path -- a source-text pin cannot.
                    _apply_restored_search_config()

                    ui.separator().classes('my-2')

                    # Run Button + New Search Reset
                    with ui.row().classes('w-full items-center gap-2'):
                        run_btn = ui.button(
                            tr('Find Parallels'),
                            icon='compare_arrows',
                            on_click=lambda: execute_parallels()
                        ).classes('btn-primary flex-grow')

                        ui.button(icon='restart_alt', on_click=lambda: _reset_parallels()).props(
                            'flat dense round'
                        ).tooltip(tr('New Composition Search'))

                    # Stop Button (hidden by default) - shows partial results
                    with ui.column().classes('w-full items-center gap-0').style('display: none;') as cancel_btn:
                        ui.button(
                            tr('Stop'),
                            icon='stop',
                            on_click=lambda: cancel_search()
                        ).classes('w-full').props('outline color=red')
                        ui.label(tr('and show partial results')).classes('text-xs').style('color: var(--text-muted);')

                    # Progress - visible spinner + status in the control panel
                    with ui.linear_progress(0, show_value=False).classes('w-full my-2').style('height: 12px; opacity: 0;') as progress_bar:
                        ui.label().classes('absolute-center text-xs text-white').bind_text_from(
                            progress_bar, 'value', backward=lambda v: f'{round(v * 100)}%' if v > 0 else ''
                        )
                    with ui.row().classes('w-full items-center justify-center gap-2').style('display: none;') as search_indicator:
                        ui.spinner('dots', size='sm', color='primary')
                        status_label = ui.label('').classes('text-sm font-medium').style('color: var(--primary-600);')
                    # Summary label (stays visible after search completes, hidden during search)
                    summary_label = ui.label('').classes('text-sm font-medium text-center w-full').style('color: var(--primary-600);')

                    # Composition History Button + Menu
                    ui.separator().classes('my-1')
                    with ui.row().classes('w-full items-center justify-center'):
                        comp_history_btn = ui.button(
                            tr('Composition History'), icon='history',
                            on_click=lambda: (_refresh_comp_history_menu(), comp_history_menu.open())
                        ).props('flat dense no-caps').classes('text-sm')

                        comp_history_menu = ui.menu()

        # === Advanced Filters Panel (collapsible, below source input) ===
        _adv_filters_expanded = _has_active_filters() or _filters_from_browse
        adv_filters_panel = ui.expansion(
            text=tr('Search only in...'),
            icon='filter_alt',
            value=_adv_filters_expanded,
        ).classes('w-full').style(
            'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light);'
        ).props('dense header-class="text-subtitle2 text-weight-medium"')

        # References to filter UI elements
        _filter_refs = {}

        with adv_filters_panel:
            with ui.column().classes('w-full px-4 py-3 gap-4'):

                # Include/Exclude toggle
                with ui.row().classes('w-full items-center gap-2'):
                    p_filter_mode_toggle = ui.toggle(
                        {True: tr('Include'), False: tr('Exclude')},
                        value=p_state.filter_include_mode,
                    ).props('dense no-caps size=sm')
                    _filter_refs['mode'] = p_filter_mode_toggle

                with ui.row().classes('w-full gap-4 flex-wrap items-end'):
                    # Domain filter (multi-select)
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Domain')).classes('text-xs font-medium').style('color: var(--text-secondary);')

                        p_domain_select = ui.select(
                            options={},
                            value=p_state.filter_domains,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips loading')
                        _filter_refs['domain'] = p_domain_select

                    # Author filter (multi-select) — options loaded asynchronously after page renders
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Author')).classes('text-xs font-medium').style('color: var(--text-secondary);')

                        p_author_select = ui.select(
                            options={},
                            value=p_state.filter_authors,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips loading')
                        _filter_refs['author'] = p_author_select

                    # Work filter (multi-select) — options loaded asynchronously after page renders
                    with ui.column().classes('gap-1 min-w-48 flex-grow'):
                        ui.label(tr('Work')).classes('text-xs font-medium').style('color: var(--text-secondary);')

                        p_work_select = ui.select(
                            options={},
                            value=p_state.filter_works,
                            multiple=True,
                            with_input=True,
                            clearable=True,
                        ).classes('w-full').props('outlined dense use-chips loading')
                        _filter_refs['work'] = p_work_select

                with ui.row().classes('w-full gap-4 flex-wrap items-end'):
                    # Date range
                    with ui.column().classes('gap-1 min-w-32'):
                        ui.label(tr('Date Range')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        with ui.row().classes('items-center gap-2'):
                            p_date_from_input = ui.number(
                                label=tr('From Year'),
                                value=p_state.filter_date_from,
                            ).classes('w-28').props('outlined dense')
                            ui.label('\u2013').style('color: var(--text-muted);')
                            p_date_to_input = ui.number(
                                label=tr('To Year'),
                                value=p_state.filter_date_to,
                            ).classes('w-28').props('outlined dense')
                        _filter_refs['date_from'] = p_date_from_input
                        _filter_refs['date_to'] = p_date_to_input

                    # Material exclude (Printed)
                    with ui.column().classes('gap-1 min-w-48'):
                        ui.label(tr('Material')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        p_exclude_printed_cb = ui.checkbox(
                            tr('Exclude Printed'),
                            value='Printed' in p_state.filter_material_exclude,
                        ).props('dense')
                        _filter_refs['exclude_printed'] = p_exclude_printed_cb

                    # Import exclusions button
                    with ui.column().classes('gap-1 justify-end'):
                        with ui.row().classes('gap-2'):
                            def _import_exclusions_from_word_search():
                                """Import per-manuscript exclusions from word search."""
                                ws_excluded = safe_user_get('word_search_excluded_ids', [])
                                if not ws_excluded:
                                    ui.notify(tr('No word search exclusions to import'), type='info', timeout=2000)
                                    return
                                imported_count = 0
                                for sid in ws_excluded:
                                    if sid not in p_state.excluded_manuscript_ids:
                                        p_state.excluded_manuscript_ids.add(sid)
                                        imported_count += 1
                                persist_value('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
                                ui.notify(
                                    f"{tr('Imported')} {imported_count} {tr('exclusions from word search')}",
                                    type='positive', timeout=3000
                                )
                                _update_p_chip_bar()

                            ui.button(tr('Import exclusions'), icon='download',
                                      on_click=_import_exclusions_from_word_search).props('flat dense no-caps size=sm')

                    # Clear all filters button
                    with ui.column().classes('gap-1 justify-end'):
                        def _clear_all_p_adv_filters():
                            """Clear all advanced filter selections."""
                            p_state.filter_domains = []
                            p_state.filter_authors = []
                            p_state.filter_works = []
                            p_state.filter_include_mode = True
                            p_state.filter_date_from = None
                            p_state.filter_date_to = None
                            p_state.filter_material_exclude = []
                            p_state.filter_text_all = []
                            p_state.filter_text_any = []
                            p_state.filter_text_not = []
                            p_state.filter_manuscript_count = None
                            p_state.restrict_sys_ids = None
                            # Update UI elements
                            p_domain_select.value = []
                            p_author_select.value = []
                            p_work_select.value = []
                            p_filter_mode_toggle.value = True
                            p_date_from_input.value = None
                            p_date_to_input.value = None
                            p_exclude_printed_cb.value = False
                            if _filter_refs.get('text_input'):
                                _filter_refs['text_input'].value = ''
                            # Reset filter storage to clean defaults
                            safe_user_set('parallels_filter_domains', [])
                            safe_user_set('parallels_filter_authors', [])
                            safe_user_set('parallels_filter_works', [])
                            safe_user_set('parallels_filter_include_mode', True)
                            safe_user_set('parallels_filter_date_from', None)
                            safe_user_set('parallels_filter_date_to', None)
                            safe_user_set('parallels_filter_material_exclude', [])
                            safe_user_set('parallels_filter_text_all', [])
                            safe_user_set('parallels_filter_text_any', [])
                            safe_user_set('parallels_filter_text_not', [])
                            _update_p_chip_bar()

                        ui.button(tr('Clear All'), icon='clear_all',
                                  on_click=_clear_all_p_adv_filters).props('flat dense no-caps')

                # Text filter row
                with ui.row().classes('w-full gap-2 items-end'):
                    with ui.column().classes('gap-1 flex-grow'):
                        ui.label(tr('Text Filter')).classes('text-xs font-medium').style('color: var(--text-secondary);')
                        with ui.row().classes('items-center gap-2 w-full'):
                            p_text_mode_select = ui.select(
                                options={
                                    'all': tr('All words'),
                                    'any': tr('Any word'),
                                    'not': tr('Not these words'),
                                },
                                value='all',
                            ).classes('w-36').props('outlined dense')
                            _filter_refs['text_mode'] = p_text_mode_select

                            p_text_filter_input = ui.input(
                                placeholder=tr('Add term'),
                            ).classes('flex-grow').props('outlined dense').on(
                                'keydown.enter', lambda e: _add_p_text_term()
                            )
                            _filter_refs['text_input'] = p_text_filter_input

                            ui.button(icon='add', on_click=lambda: _add_p_text_term()).props('flat dense round')

                    # Display current text filter chips
                    with ui.row().classes('w-full gap-1 flex-wrap') as p_text_chip_row:
                        _filter_refs['text_chips'] = p_text_chip_row

                def _add_p_text_term():
                    """Add a text filter term from the input."""
                    term = p_text_filter_input.value.strip() if p_text_filter_input.value else ''
                    if not term:
                        return
                    mode = p_text_mode_select.value
                    if mode == 'all':
                        if term not in p_state.filter_text_all:
                            p_state.filter_text_all.append(term)
                    elif mode == 'any':
                        if term not in p_state.filter_text_any:
                            p_state.filter_text_any.append(term)
                    elif mode == 'not':
                        if term not in p_state.filter_text_not:
                            p_state.filter_text_not.append(term)
                    p_text_filter_input.value = ''
                    persist_value('parallels_filter_text_all', p_state.filter_text_all)
                    persist_value('parallels_filter_text_any', p_state.filter_text_any)
                    persist_value('parallels_filter_text_not', p_state.filter_text_not)
                    asyncio.ensure_future(_recompute_p_filter_count())
                    _update_p_chip_bar()
                    _rebuild_p_text_chips()

                def _remove_p_text_term(mode, term):
                    """Remove a text filter term."""
                    target = getattr(p_state, f'filter_text_{mode}')
                    if term in target:
                        target.remove(term)
                    persist_value(f'parallels_filter_text_{mode}', target)
                    asyncio.ensure_future(_recompute_p_filter_count())
                    _update_p_chip_bar()
                    _rebuild_p_text_chips()

                def _rebuild_p_text_chips():
                    """Rebuild text filter chip display."""
                    text_chip_row = _filter_refs.get('text_chips')
                    if not text_chip_row:
                        return
                    text_chip_row.clear()
                    with text_chip_row:
                        for t in p_state.filter_text_all:
                            ui.chip(f"+ {t}", icon='check_circle', removable=True,
                                    color='green-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_p_text_term('all', _t))
                        for t in p_state.filter_text_any:
                            ui.chip(f"~ {t}", icon='help_outline', removable=True,
                                    color='blue-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_p_text_term('any', _t))
                        for t in p_state.filter_text_not:
                            ui.chip(f"- {t}", icon='block', removable=True,
                                    color='red-2', on_click=lambda: None,
                            ).on('remove', lambda _t=t: _remove_p_text_term('not', _t))

        # --- Filter chip bar (always visible, even when panel is collapsed) ---
        p_chip_bar_container = ui.row().classes('w-full px-4 py-1 gap-2 items-center flex-wrap').style(
            'background: var(--bg-tertiary); border-bottom: 1px solid var(--border-light); min-height: 0; margin-bottom: 16px; position: relative; z-index: 1;'
        )
        p_chip_bar_container.set_visibility(False)

        def _get_p_display_name(key, opts_dict):
            """Extract display name from options dict (strip trailing count suffix only)."""
            if isinstance(opts_dict, dict) and key in opts_dict:
                import re
                # Strip only the trailing " (N,NNN)" count, preserving qualified names like "Other (Bible)"
                raw = opts_dict[key].lstrip(' \u2514').strip()
                return re.sub(r'\s*\([\d,]+\)\s*$', '', raw).strip()
            return key

        def _update_p_chip_bar():
            """Rebuild chip bar from current filter state."""
            p_chip_bar_container.clear()
            has_any = _has_active_filters()
            has_excl = bool(p_state.excluded_manuscript_ids)
            p_chip_bar_container.set_visibility(has_any or has_excl)
            if not has_any and not has_excl:
                return

            opts_d = p_domain_select.options if hasattr(p_domain_select, 'options') else {}
            opts_a = p_author_select.options if hasattr(p_author_select, 'options') else {}
            opts_w = p_work_select.options if hasattr(p_work_select, 'options') else {}

            with p_chip_bar_container:
                # Mode indicator
                if not p_state.filter_include_mode and (
                    p_state.filter_domains or p_state.filter_authors or p_state.filter_works
                ):
                    ui.chip(tr('Exclude selected'), icon='block', color='red-2')

                # Domain chips
                for d in p_state.filter_domains:
                    dname = _get_p_display_name(d, opts_d)
                    ui.chip(
                        dname, icon='category', removable=True,
                        on_click=lambda: None, color='deep-purple-2',
                    ).on('remove', lambda _d=d: _remove_p_filter('domain', _d))

                # Author chips
                for a in p_state.filter_authors:
                    aname = _get_p_display_name(a, opts_a)
                    ui.chip(
                        aname, icon='person', removable=True,
                        on_click=lambda: None, color='blue-2',
                    ).on('remove', lambda _a=a: _remove_p_filter('author', _a))

                # Work chips
                for w in p_state.filter_works:
                    wname = _get_p_display_name(w, opts_w)
                    ui.chip(
                        wname, icon='menu_book', removable=True,
                        on_click=lambda: None, color='teal-2',
                    ).on('remove', lambda _w=w: _remove_p_filter('work', _w))

                # Date range chip
                if p_state.filter_date_from is not None or p_state.filter_date_to is not None:
                    df = p_state.filter_date_from or '...'
                    dt = p_state.filter_date_to or '...'
                    ui.chip(
                        f"{df}\u2013{dt}", icon='date_range', removable=True,
                        on_click=lambda: None,
                        color='orange-2',
                    ).on('remove', lambda: _remove_p_filter('date'))

                # Material exclude chip
                if p_state.filter_material_exclude:
                    for mat in p_state.filter_material_exclude:
                        ui.chip(
                            f"{tr('Exclude')} {mat}", icon='block', removable=True,
                            on_click=lambda: None,
                            color='red-2',
                        ).on('remove', lambda m=mat: _remove_p_filter('material', m))

                # Text filter chips
                for t in p_state.filter_text_all:
                    ui.chip(f"+ {t}", icon='check_circle', removable=True,
                            color='green-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_p_text_term('all', _t))
                for t in p_state.filter_text_any:
                    ui.chip(f"~ {t}", icon='help_outline', removable=True,
                            color='blue-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_p_text_term('any', _t))
                for t in p_state.filter_text_not:
                    ui.chip(f"- {t}", icon='block', removable=True,
                            color='red-2', on_click=lambda: None,
                    ).on('remove', lambda _t=t: _remove_p_text_term('not', _t))

                # Per-manuscript exclusion count chip
                if p_state.excluded_manuscript_ids:
                    ui.chip(
                        f"{len(p_state.excluded_manuscript_ids)} {tr('excluded')}",
                        icon='remove_circle_outline',
                        color='grey-4',
                    )

                # Manuscript count badge
                if p_state.filter_manuscript_count is not None:
                    ui.label(
                        f"{p_state.filter_manuscript_count:,} {tr('manuscripts')}"
                    ).classes('text-xs px-2 py-0.5 rounded ml-2').style(
                        'background: var(--bg-tertiary); color: var(--text-secondary); border: 1px solid var(--border-light);'
                    )

        def _remove_p_filter(filter_type, value=None):
            """Remove a specific filter and update state."""
            if filter_type == 'domain':
                if value and value in p_state.filter_domains:
                    p_state.filter_domains.remove(value)
                else:
                    p_state.filter_domains = []
                p_domain_select.value = p_state.filter_domains
                persist_value('parallels_filter_domains', p_state.filter_domains)
                asyncio.ensure_future(_refresh_p_author_options())
                asyncio.ensure_future(_refresh_p_work_options())
            elif filter_type == 'author':
                if value and value in p_state.filter_authors:
                    p_state.filter_authors.remove(value)
                else:
                    p_state.filter_authors = []
                p_author_select.value = p_state.filter_authors
                persist_value('parallels_filter_authors', p_state.filter_authors)
                asyncio.ensure_future(_refresh_p_work_options())
            elif filter_type == 'work':
                if value and value in p_state.filter_works:
                    p_state.filter_works.remove(value)
                else:
                    p_state.filter_works = []
                p_work_select.value = p_state.filter_works
                persist_value('parallels_filter_works', p_state.filter_works)
            elif filter_type == 'date':
                p_state.filter_date_from = None
                p_state.filter_date_to = None
                p_date_from_input.value = None
                p_date_to_input.value = None
                persist_value('parallels_filter_date_from', None)
                persist_value('parallels_filter_date_to', None)
            elif filter_type == 'material':
                if value and value in p_state.filter_material_exclude:
                    p_state.filter_material_exclude.remove(value)
                    persist_value('parallels_filter_material_exclude', p_state.filter_material_exclude)
                    p_exclude_printed_cb.value = 'Printed' in p_state.filter_material_exclude
            asyncio.ensure_future(_recompute_p_filter_count())
            _update_p_chip_bar()

        _p_filter_refresh_seq = {'author': 0, 'work': 0}

        async def _refresh_p_author_options():
            """Refresh author select options based on current domain filter (async)."""
            _p_filter_refresh_seq['author'] += 1
            seq = _p_filter_refresh_seq['author']
            p_author_select.props('loading')
            lang = get_language()
            new_opts = await run.io_bound(build_author_options, lang, p_state.filter_domains)
            if _p_filter_refresh_seq['author'] != seq:
                return  # Stale -- newer request in flight
            p_author_select.props(remove='loading')
            p_author_select.options = new_opts
            p_author_select.update()

        async def _refresh_p_work_options():
            """Refresh work select options based on current domain and author filters (async)."""
            _p_filter_refresh_seq['work'] += 1
            seq = _p_filter_refresh_seq['work']
            p_work_select.props('loading')
            lang = get_language()
            new_opts = await run.io_bound(
                build_work_options, lang, p_state.filter_domains, p_state.filter_authors
            )
            if _p_filter_refresh_seq['work'] != seq:
                return  # Stale -- newer request in flight
            p_work_select.props(remove='loading')
            p_work_select.options = new_opts
            p_work_select.update()

        async def _recompute_p_filter_count():
            """Recompute manuscript count for current filters (background)."""
            await recompute_filter_count(p_state, _update_p_chip_bar)

        # --- Filter change handlers (via shared factory) ---
        _p_handlers = create_filter_handlers(
            p_state, 'parallels', _filter_refs,
            _refresh_p_author_options, _refresh_p_work_options,
            _recompute_p_filter_count, _update_p_chip_bar,
        )

        # Wire up change handlers
        p_domain_select.on('update:model-value', _p_handlers['on_domain_change'])
        p_author_select.on('update:model-value', _p_handlers['on_author_change'])
        p_work_select.on('update:model-value', _p_handlers['on_work_change'])
        p_filter_mode_toggle.on('update:model-value', _p_handlers['on_mode_change'])
        p_date_from_input.on('blur', _p_handlers['on_date_from_change'])
        p_date_to_input.on('blur', _p_handlers['on_date_to_change'])
        p_exclude_printed_cb.on('update:model-value', _p_handlers['on_exclude_printed_change'])

        # Initialize chip bar on page load
        _update_p_chip_bar()

        # === Filter Text (Collapsible) ===
        # State for loaded sources: {ref: cleaned_text}
        # Only store refs in persistent storage (not the full text - too large for WebSocket)
        # Full text is reloaded from cache files on page load (async)
        filter_sources = {'loaded': {}, 'enabled': set(), 'pending_restore': True, 'custom_count': 0}

        # Filter expansion with dynamic badge
        with ui.row().classes('w-full items-center'):
            filter_expansion = ui.expansion(tr('Filter text (exclude known sources)'), icon='filter_alt').classes('flex-1')
            filter_expansion.tooltip(tr('Choose known sources to exclude from results (e.g., Bible verses, Mishnah). Matches found in these sources will be moved to a separate list.'))
            filter_badge = ui.badge('0').props('color=grey transparent').classes('ml-2').style('display: none;')

        def update_filter_badge():
            """Update badge with number of loaded sources."""
            count = len(filter_sources['enabled'])
            if count > 0:
                filter_badge.set_text(f"{count}")
                filter_badge.props('color=primary')
                filter_badge.style('display: inline-flex;')
            else:
                filter_badge.style('display: none;')

        with filter_expansion:
            with ui.column().classes('w-full p-4 gap-4'):
                ui.label(tr('Select sources to filter results (matches found in checked sources will be moved to a separate list):')).classes('text-sm').style('color: var(--text-muted);')

                # Sefaria source buttons
                with ui.row().classes('w-full items-center gap-2 flex-wrap'):
                    ui.label(tr('Load from Sefaria') + ':').classes('text-sm font-medium').style('color: var(--text-secondary);')
                    btn_tanakh = ui.button(tr('Tanakh'), icon='menu_book').props('outline dense size=sm')
                    btn_mishnah = ui.button(tr('Mishnah'), icon='menu_book').props('outline dense size=sm')
                    btn_talmud = ui.button(tr('Talmud'), icon='menu_book').props('outline dense size=sm')
                    btn_more = ui.button(tr('More Sources...'), icon='library_books').props('outline dense size=sm')
                    btn_sefaria_search = ui.button(tr('Search Sefaria'), icon='search').props('outline dense size=sm')

                # Custom text button
                with ui.row().classes('w-full items-center gap-2'):
                    ui.label(tr('Custom source') + ':').classes('text-sm font-medium').style('color: var(--text-secondary);')
                    btn_add_custom = ui.button(tr('Add Custom Text'), icon='add').props('outline dense size=sm')

                # Progress for Sefaria loading
                sefaria_progress = ui.linear_progress(0).classes('w-full').style('display: none;')
                sefaria_status = ui.label('').classes('text-xs').style('color: var(--text-muted); display: none;')

                # Loaded sources list (checkboxes)
                with ui.column().classes('w-full gap-2'):
                    h4(tr('Loaded Sources'), classes='text-sm font-medium', style='color: var(--text-secondary);')

                    with ui.row().classes('gap-2 mb-2'):
                        btn_select_all = ui.button(tr('Select All'), icon='check_box').props('flat dense size=sm')
                        btn_deselect_all = ui.button(tr('Deselect All'), icon='check_box_outline_blank').props('flat dense size=sm')
                        btn_remove_unchecked = ui.button(tr('Remove Unchecked'), icon='delete').props('flat dense size=sm color=red')

                    loaded_sources_container = ui.column().classes('w-full max-h-48 overflow-y-auto gap-1 p-2 rounded').style('background: var(--bg-secondary);')

                    filter_info_label = ui.label(tr('Active: {} / {}').format(0, 0)).classes('text-xs').style('color: var(--text-muted);')

        # === Results Section ===
        with ui.card().classes('w-full p-6'):
            with ui.row().classes('w-full items-center justify-between mb-4'):
                # Changed to H2 (stored in variable but it's a UI element)
                results_header = h2(tr('Results'), classes='text-xl font-bold', style='color: var(--text-primary);')

                with ui.row().classes('gap-2'):
                    # Domain filter button (hidden until search with domain data)
                    p_domain_filter_btn = ui.button(
                        tr('Filter by domains'), icon='category',
                        on_click=lambda: _open_parallels_domain_filter_dialog()
                    ).classes('text-sm').props('outline dense no-caps')
                    p_domain_filter_btn.set_visibility(False)

                    # Restore visibility if stored exclusions exist
                    if p_state.domain_exclusions:
                        p_domain_filter_btn.set_visibility(True)
                        n_excl = len(p_state.domain_exclusions)
                        p_domain_filter_btn.text = f"{tr('Filter by domains')} ({n_excl} {tr('excluded')})"
                        p_domain_filter_btn.props('outline dense no-caps color=red')

                    # DMF-09: library filter button (Phase 131-05)
                    # 3-state: neutral / Show-only (showing N/total) / Hide (hiding N)
                    # Hidden until results exist (like domain filter), then shown on results arrival.
                    parallels_library_filter_btn = ui.button(
                        tr('Filter by library'),
                    ).classes('text-sm').props('outline dense no-caps').style('min-height: 2.286em;')
                    parallels_library_filter_btn.tooltip(tr('Filter results by library'))
                    parallels_library_filter_btn.set_visibility(False)
                    parallels_library_filter_btn.on('click', lambda: _open_parallels_library_filter_dialog())

                    # Sort options
                    sort_select = ui.select(
                        {
                            'score': tr('Sort by score'),
                            'shelfmark': tr('Sort by shelfmark'),
                            'matches': tr('Sort by matches'),
                        },
                        value='score'
                    ).props('outlined dense').classes('w-40')

                    export_word_btn = ui.button(icon='description', on_click=lambda: ui.download('/api/export/parallels/word')).props(
                        'flat round dense disable'
                    ).tooltip(tr('Export Word'))
                    export_excel_btn = ui.button(icon='table_view', on_click=lambda: ui.download('/api/export/parallels/excel')).props(
                        'flat round dense disable'
                    ).tooltip(tr('Export Excel'))
                    export_json_btn = ui.button(icon='data_object', on_click=lambda: ui.download('/api/export/parallels/json')).props(
                        'flat round dense disable'
                    ).tooltip(tr('Export JSON'))

            # Appears when at least one result is checked for promotion.
            promotion_bar = ui.column().classes('w-full')
            promotion_bar.set_visibility(False)
            results_container = ui.column().classes('w-full gap-4').style('min-height: 300px;')

    # === Logic ===

    # =====================================================================
    # Multi-witness letter-level search
    # =====================================================================
    # The engine can fan out over a witness list inside ONE call, and the
    # public API uses that. This page deliberately does not: it is a session,
    # not a request. A user who adds a witness expects only THAT witness to be
    # searched and its rows merged into what is already on screen. Re-running
    # every witness on every addition would make an R-round auto-expansion
    # quadratic instead of linear -- which would falsify the "cost is linear"
    # premise the whole feature rests on.
    #
    # So both surfaces share ONE fusion module (shared/passage_fusion.py) and
    # nothing else: the API fuses N results from one call, the page fuses N
    # results accumulated across N calls. Same maths, one definition.

    def _witness_depth_cap() -> int:
        # Delegates: the rule is module level so it can be called by a test.
        return witness_depth_cap(p_state.last_passage_ctx,
                                 passage_depth.value)

    def _witness_new_id() -> str:
        # Monotonic, never reused after a removal: a recycled id would let a
        # removed witness's stale rows be attributed to its replacement.
        p_state.witness_seq += 1
        return f'w{p_state.witness_seq}'

    def _witness_default_label(text: str) -> str:
        words = [w for w in (text or '').split() if w][:5]
        return ' '.join(words) or tr('Pasted text')

    def _witness_labels() -> dict:
        labels = {WITNESS_SEED_ID: tr('Your text')}
        for w in p_state.witnesses:
            labels[w['id']] = w.get('label') or ''
        return labels

    def _witness_order() -> list:
        return [WITNESS_SEED_ID] + [w['id'] for w in p_state.witnesses]

    def _searched_witness_count() -> int:
        return sum(1 for wid in _witness_order()
                   if p_state.witness_rows.get(wid) is not None)

    def _fuse_and_store() -> None:
        """Rebuild p_state.results/filtered_results from the per-witness rows.

        With a single searched witness the rows pass through UNTOUCHED and
        carry no fusion fields -- RRF over one list is a 1/(k+rank) rescale
        that carries no information, and `score` must keep meaning matched
        letters for the common case. This mirrors the engine's own
        short-circuit exactly, so a one-witness page search and a one-witness
        API search produce the same row shape.
        """
        from shared.passage_fusion import fuse_routed, tag_rows

        order = [wid for wid in _witness_order()
                 if p_state.witness_rows.get(wid) is not None]
        if not order:
            # Nothing to fuse FROM, which is not the same as "the result set
            # is empty" -- and conflating the two destroyed data.
            #
            # In a live session this is unreachable: a search always stores
            # the seed under WITNESS_SEED_ID, so `order` holds at least one
            # entry. It IS reachable in the one state this feature creates on
            # purpose -- after a reload `_restore_witnesses_from_snapshot`
            # leaves `witness_rows` empty (per-witness ranks cannot be
            # recovered from fused rows) while `p_state.results` holds the
            # restored rows. Removing a witness then wiped every one of them,
            # and the republish and snapshot persist in `_remove_witness`
            # wrote that loss to storage.
            #
            # Rows this function did not produce are not its to discard. They
            # are left exactly as they are; the panel already says the
            # witnesses need re-running, which is what actually rebuilds them.
            return
        if len(order) == 1:
            wid = order[0]
            p_state.results = list(p_state.witness_rows.get(wid) or [])
            p_state.filtered_results = list(
                p_state.witness_filtered.get(wid) or [])
            return

        labels = _witness_labels()
        main_pairs, filt_pairs = [], []
        for wid in order:
            label = labels.get(wid, '')
            main = list(p_state.witness_rows.get(wid) or [])
            filt = list(p_state.witness_filtered.get(wid) or [])
            # Rank over BOTH buckets together, in score order -- the same
            # basis the engine's own fan-out uses, which tags the full result
            # list BEFORE splitting it. Ranking each bucket from 1
            # independently gave a filtered row the rank of a top hit, and
            # left every main row's rank short by however many rows the
            # engine had demoted ahead of it. Either way the RRF sums came
            # out different from the API's for the same witnesses.
            combined = sorted(main + filt,
                              key=lambda r: float(r.get('score') or 0.0),
                              reverse=True)
            tag_rows(combined, wid, label)
            main_pairs.append((wid, main))
            filt_pairs.append((wid, filt))
        # Routing and the contributor arithmetic both live in `fuse_routed`,
        # so the page and the API cannot disagree about either. (They did
        # disagree about neither, but the rule was written out twice, which
        # is how they start to.)
        p_state.results, p_state.filtered_results = fuse_routed(
            main_pairs, filt_pairs)

    def _text_digest_of(value: str) -> str:
        # Delegates: the page and the API must agree about when two witnesses
        # are the same witness, and the rule was written out twice.
        from shared.passage_fusion import witness_text_key
        return witness_text_key(value)

    def _witness_text_keys() -> set:
        """Every witness text already represented, THE SEED INCLUDED.

        The seed is a witness -- it is fused under `WITNESS_SEED_ID` like any
        other -- so a paste identical to the box above counts the same text
        twice, and `witness_count` then reports two witnesses where there is
        one. Read live, because that is the text the search is about to run.
        """
        keys = {_seed_digest()}
        keys.update(_text_digest_of(w.get('text'))
                    for w in p_state.witnesses if (w.get('text') or '').strip())
        return keys

    def _seed_digest() -> str:
        """The digest of what is in the box RIGHT NOW.

        Staleness is measured against this -- "was this witness gathered for
        the text I am about to search?" -- so it is deliberately live. What
        must NOT be read live is the stamp on a promoted witness; that one
        comes from `last_passage_ctx`.
        """
        return _text_digest_of(text_input.value)

    def _add_witness(text: str, label: str = '', kind: str = 'pasted',
                     sys_id: str = None, seed_digest: str = None,
                     headers: list = None) -> dict:
        entry = {
            'id': _witness_new_id(),
            # The seed this witness was gathered FOR. A witness of one work is
            # noise in another, so a later search against a different source
            # text must not quietly include it.
            #
            # Defaults to the LIVE box, which is right for a pasted witness --
            # the user typed it for the query in front of them. A PROMOTED one
            # passes the digest of the search that produced the row instead
            # (`_promote_checked`): if the box was edited since, those two are
            # different texts and the row belongs to the older one.
            'seed_digest': seed_digest or _seed_digest(),
            'label': (label or '').strip() or _witness_default_label(text),
            'kind': kind,
            'sys_id': sys_id,
            # The pages this witness was BUILT FROM. A promoted witness is not
            # a function of its sys_id -- it is the concatenation of the pages
            # that matched -- so without this a reload rebuilds a different
            # witness under the same label. See `_rehydrate_manuscript_witnesses`.
            'headers': list(headers or []),
            'text': text or '',
            'status': 'pending',
            'hits': 0,
            'error': '',
        }
        p_state.witnesses.append(entry)
        return entry

    def _remove_witness(wid: str) -> None:
        """Drop a witness AND every row it contributed.

        Stripping the rows is not optional: leaving them would have the panel
        say the witness is gone while its results -- up to a few thousand of
        them, for a witness that found nothing useful -- stayed on screen with
        no way to attribute or remove them.
        """
        p_state.witnesses = [w for w in p_state.witnesses if w['id'] != wid]
        p_state.witness_rows.pop(wid, None)
        p_state.witness_filtered.pop(wid, None)
        # Can this removal actually take its rows with it?
        #
        # Only if there is something to re-fuse FROM. After a reload the
        # per-witness caches are deliberately empty -- per-witness ranks
        # cannot be recovered from fused rows -- so the rows on screen keep
        # the removed witness's contributions whatever happens here. In a
        # live session the seed is always cached under WITNESS_SEED_ID, so
        # this is true whenever a search has run in this tab.
        #
        # Discarding the rows instead would honour the docstring's promise by
        # destroying a restored result set that exists nowhere else -- the
        # data loss the guard in `_fuse_and_store` was added to stop. The
        # answer is to say so, not to delete.
        _can_restrip = bool(p_state.witness_rows)
        _fuse_and_store()
        _refresh_witness_panel()
        if p_state.results or p_state.filtered_results:
            render_results(p_state.results, p_state.filtered_results)
        else:
            results_header.text = tr('No results')
            results_container.clear()
            with results_container:
                show_empty_state()
        # A removal changes the ROW SET, and everything that describes the row
        # set has to change with it. This is the same debt a witness SEARCH
        # owes and pays; here it went unpaid, with three consequences, the
        # third serious:
        #
        #   * the downloadable Word/XLSX/JSON still held the removed
        #     witness's rows and named it in the manifest;
        #   * the summary line and library-filter button described the old
        #     set;
        #   * `p_state.search_fingerprint` stayed the identity of the OLD
        #     witness set, and `_persist_witness_state()` below then wrote the
        #     snapshot under it -- so on reload `recover_richer_parallels_rows`
        #     matched that fingerprint, judged the stored payload to be the
        #     same search, and restored the removed witness's contributions.
        #     The removal silently undid itself.
        #
        # `_refresh_export_payload` recomputes the identity and republishes,
        # so it MUST run before the snapshot is persisted.
        summary_label.text = ''
        parallels_library_filter_btn.set_visibility(
            bool(p_state.results or p_state.filtered_results))
        _update_parallels_library_filter_btn()
        if not _can_restrip and (p_state.results or p_state.filtered_results):
            # The panel now shows the witness gone while its rows are still on
            # screen. Presenting that as a completed removal is the kind of
            # quiet disagreement between what a surface says and what it holds
            # that this feature has already been bitten by twice.
            ui.notify(
                tr('Witness removed. The results on screen were found with '
                   'the previous witness list — run the search again to '
                   'update them.'),
                type='warning')
        _refresh_export_payload()
        # The snapshot used to be written only at the end of a witness
        # SEARCH, so a removal survived until the next one and then came back
        # -- rows and all -- on reload.
        _persist_witness_state()

    def _witness_status_chip(entry: dict):
        status = entry.get('status')
        if status == 'stale':
            ui.badge(tr('Other source text'), color='orange').classes(
                'text-xs').tooltip(tr(
                'This witness was added for a different source text, so it '
                'was not searched.'))
        elif status == 'searched':
            ui.badge(tr('{n} matches found').format(n=entry.get('hits', 0)),
                     color='green').classes('text-xs')
        elif status == 'failed':
            ui.badge(tr('Failed'), color='red').classes('text-xs')
        elif status == 'running':
            ui.spinner(size='sm')
        else:
            ui.badge(tr('Pending'), color='grey').classes('text-xs')

    def _refresh_witness_panel() -> None:
        witness_list.clear()
        pending = [w for w in p_state.witnesses if w['status'] == 'pending']
        witness_empty_label.set_visibility(not p_state.witnesses)
        with witness_list:
            for entry in p_state.witnesses:
                with ui.row().classes(
                        'w-full items-center gap-2 p-1 rounded').style(
                        'background: var(--bg-subtle, rgba(0,0,0,0.03));'):
                    ui.icon('menu_book' if entry['kind'] == 'manuscript'
                            else 'notes').classes('text-sm').style(
                        'color: var(--text-muted);')
                    if entry['kind'] == 'manuscript' and entry.get('sys_id'):
                        ui.link(entry['label'],
                                f"/browse?sys_id={entry['sys_id']}",
                                new_tab=True).classes(
                            'text-sm no-underline hover:underline')
                    else:
                        ui.label(entry['label']).classes('text-sm')
                    ui.label(f"{len(entry['text'])}").classes(
                        'text-xs').style('color: var(--text-muted);')
                    _witness_status_chip(entry)
                    ui.space()
                    if entry['status'] == 'failed':
                        ui.button(
                            tr('Retry'), icon='refresh',
                            on_click=lambda _e=None, _w=entry['id']:
                                _retry_witness(_w),
                        ).props('flat dense no-caps size=sm')
                    ui.button(
                        icon='close',
                        on_click=lambda _e=None, _w=entry['id']:
                            _remove_witness(_w),
                    ).props('flat round dense size=sm').tooltip(tr('Remove'))
        _sync_sort_options()
        stale = [w for w in p_state.witnesses if w['status'] == 'stale']
        witness_stale_row.clear()
        witness_stale_row.set_visibility(bool(stale))
        if stale:
            with witness_stale_row:
                with ui.row().classes('w-full items-center gap-2 p-2 rounded').style(
                        'background: var(--bg-card); '
                        'border: 1px solid var(--accent-amber);'):
                    ui.icon('info').classes('text-sm').style(
                        'color: var(--accent-amber);')
                    ui.label(tr(
                        '{n} witnesses were added for a different source text.'
                    ).format(n=len(stale))).classes('text-xs')
                    ui.space()
                    ui.button(
                        tr('Use them anyway'),
                        on_click=lambda: _revive_stale_witnesses(),
                    ).props('flat dense no-caps size=sm')
                    ui.button(
                        tr('Remove them'),
                        on_click=lambda: _remove_stale_witnesses(),
                    ).props('flat dense no-caps size=sm')
        # The collapsed header is the only thing most users see, so it
        # carries the counts -- and the panel opens ITSELF when a witness
        # needs attention, since a warning inside a closed drawer is not a
        # warning. Never auto-CLOSES: a user who opened it keeps it open.
        failed_now = [w for w in p_state.witnesses if w['status'] == 'failed']
        if p_state.witnesses:
            bits = [tr('{n} witnesses').format(n=len(p_state.witnesses))]
            if stale:
                bits.append(tr('{n} from another text').format(n=len(stale)))
            elif pending:
                bits.append(tr('{n} pending').format(n=len(pending)))
            if failed_now:
                bits.append(tr('{n} failed').format(n=len(failed_now)))
            witness_expansion.props('caption="' + ' · '.join(bits) + '"')
        else:
            witness_expansion.props(remove='caption')
        if stale or pending or failed_now:
            witness_expansion.value = True
        witness_run_row.set_visibility(bool(pending))
        witness_run_btn.text = tr('Search now ({n} pending)').format(
            n=len(pending))
        # Auto-expand needs results to promote FROM.
        if p_state.results:
            auto_expand_btn.enable()
        else:
            auto_expand_btn.disable()

    def _revive_stale_witnesses() -> None:
        """Adopt the stale witnesses into the CURRENT source text.

        Re-stamping the digest is the point: without it they would go stale
        again on the next search and the user would have to answer twice.
        """
        digest = _seed_digest()
        for w in p_state.witnesses:
            if w['status'] == 'stale':
                w['status'] = 'pending'
                w['seed_digest'] = digest
        _refresh_witness_panel()
        _persist_witness_state()

    def _remove_stale_witnesses() -> None:
        for w in [w for w in p_state.witnesses if w['status'] == 'stale']:
            _remove_witness(w['id'])
        _refresh_witness_panel()

    def _open_add_witness_dialog() -> None:
        with ui.dialog() as dialog, ui.card().classes('w-[36rem] max-w-full'):
            ui.label(tr('Add witness text')).classes('text-lg font-bold')
            label_input = ui.input(label=tr('Label (optional)')).classes(
                'w-full').props('outlined dense')
            body_input = ui.textarea(
                placeholder=tr('Paste your Hebrew text here...'),
            ).classes('w-full').props('outlined rows=10').style(
                'direction: rtl;')
            bulk_checkbox = ui.checkbox(tr(
                'This paste contains several witnesses separated by blank '
                'lines'))
            preview_label = ui.label('').classes('text-sm').style(
                'color: var(--text-muted);')

            def _split_preview():
                from shared.passage_fusion import split_pasted
                texts, skipped = split_pasted(body_input.value or '')
                parts = [tr('{n} witnesses detected').format(n=len(texts))]
                if skipped:
                    # Never a silent drop: a paste that quietly loses a third
                    # of itself is the failure this repo treats as a defect.
                    parts.append(tr('({n} skipped: too short)').format(
                        n=skipped))
                preview_label.text = '  '.join(parts)

            preview_btn = ui.button(
                tr('Preview split'), on_click=_split_preview,
            ).props('flat dense no-caps size=sm')
            preview_btn.bind_visibility_from(bulk_checkbox, 'value')

            def _commit():
                body = body_input.value or ''
                label = (label_input.value or '').strip()
                if bulk_checkbox.value:
                    from shared.passage_fusion import split_pasted
                    texts, skipped = split_pasted(body)
                else:
                    from shared.passage_fusion import MIN_WITNESS_WORDS
                    body = body.strip()
                    words = len([w for w in body.split() if w])
                    texts = [body] if words >= MIN_WITNESS_WORDS else []
                    skipped = 0 if texts else 1
                if not texts:
                    ui.notify(tr('Enter at least 3 words'), type='warning')
                    return
                # Never a silent drop -- the Preview button said so, but only
                # if the user pressed it, and a paste that quietly loses part
                # of a file is the failure this repo treats as a defect.
                if skipped:
                    ui.notify(
                        tr('({n} skipped: too short)').format(n=skipped),
                        type='warning')
                from shared.passage_fusion import (
                    MAX_WITNESS_CHARS, split_by_length)
                texts, _long = split_by_length(texts)
                if _long:
                    # Refuse now, in one message. Accepted, each of these
                    # becomes a witness that spends its whole 30s ceiling and
                    # fails, and the user reads the same generic timeout up to
                    # 25 times.
                    ui.notify(
                        tr('Witness text is too long (max {cap} characters)'
                           ).format(cap=MAX_WITNESS_CHARS), type='warning')
                    if not texts:
                        return
                # Same rule the API applies to a `witnesses` array: one
                # witness supplied twice is one witness. `fuse()` counts
                # contributors positionally, so a repeat inflates
                # `witness_count` and `fusion_score` and reorders the results.
                _keys = _witness_text_keys()
                _fresh, _dupes = [], 0
                for _chunk in texts:
                    _k = _text_digest_of(_chunk)
                    if _k in _keys:
                        _dupes += 1
                        continue
                    _keys.add(_k)
                    _fresh.append(_chunk)
                texts = _fresh
                if _dupes:
                    # Counted and reported, never dropped in silence -- a
                    # paste that quietly loses part of a file is the failure
                    # this repo treats as a defect.
                    ui.notify(
                        tr('({n} skipped: already added)').format(n=_dupes),
                        type='warning')
                    if not texts:
                        return
                room = _witness_depth_cap() - len(p_state.witnesses)
                if len(texts) > room:
                    ui.notify(
                        tr('Witness list is full (max {n})').format(
                            n=_witness_depth_cap()),
                        type='warning')
                    texts = texts[:max(0, room)]
                    if not texts:
                        return
                for i, chunk in enumerate(texts):
                    _add_witness(
                        chunk,
                        label=(label if len(texts) == 1 else
                               (f'{label} {i + 1}' if label else '')))
                dialog.close()
                _refresh_witness_panel()
                # Once, after the loop -- a bulk paste adds up to 25 in one
                # go and each would otherwise re-serialise the whole result
                # set.
                _persist_witness_state()
                # Adding never auto-searches: witnesses land `pending` and
                # the user chooses when to spend the time.
                ui.notify(tr('{n} witnesses detected').format(n=len(texts)),
                          type='positive')

            with ui.row().classes('w-full justify-end gap-2'):
                ui.button(tr('Cancel'), on_click=dialog.close).props(
                    'flat no-caps')
                ui.button(tr('Add'), on_click=_commit).props('no-caps')
        dialog.open()

    async def _run_one_witness_search(entry: dict):
        """Search ONE witness through the SAME bounded passage budget the
        seed search uses.

        Each witness gets its own acquire/release, so the 30s ceiling bounds
        ONE witness rather than a whole batch, and the shared pool of 4
        interleaves with other users between witnesses. A witness that hits
        `passage_search_busy` or `core_timeout` is marked failed, skipped, and
        offered a Retry -- the run continues.
        """
        ctx = p_state.last_passage_ctx or {}

        def _sync():
            searcher = get_passage_searcher(
                state.searcher,
                preset=ctx.get('width') or 'widest-40',
                length=ctx.get('length') or 'normal',
                depth=ctx.get('depth') or 'normal',
                render_cap=0,
            )
            if searcher is None:
                return None
            return searcher.search_composition_logic(
                entry['text'],
                filter_text=ctx.get('filter_text') or None,
                boundary_mode='full',
                min_boundary_matches=ctx.get('min_boundary_matches') or 0,
                restrict_sys_ids=ctx.get('restrict_sys_ids'),
            )

        try:
            return await run_passage_search(_sync)
        except APIError as exc:
            if exc.code == 'passage_search_busy':
                entry['error'] = tr(
                    'Letter-level search is busy right now — please try again '
                    'in a moment.')
            elif exc.code == 'core_timeout':
                entry['error'] = tr(
                    'Letter-level search timed out — try a shorter text.')
            else:
                entry['error'] = tr('Letter-level search failed.')
            return None
        except Exception as e:
            logger.exception(f"Parallels witness search failed: {e}")
            entry['error'] = tr('Letter-level search failed.')
            return None

    def _clear_stop() -> None:
        """Fresh user intent clears a standing Stop.

        Deliberately NOT inside `_search_pending_witnesses`: that runs from
        the seed search and from each auto-expand round too, where clearing
        would undo a Stop the user already pressed.
        """
        p_state.is_cancelled = False

    async def _rehydrate_manuscript_witnesses(pending: list) -> None:
        """Re-fetch the text of restored manuscript witnesses.

        `_persist_active_snapshot` stores manuscript witnesses WITHOUT their
        text on purpose -- the corpus still has it, and duplicating up to
        25 x 20,000 characters of it into a tab snapshot buys nothing. But
        nothing re-fetched it on restore, so after a reload those witnesses
        searched the empty string and came back `searched, 0 matches`: a
        false negative that looks exactly like a real one.

        Fetched off the event loop, in ONE batch, through the same
        `collect_witness_texts` the promotion path uses. Anything still
        unresolved keeps its empty text and is failed at dispatch with a
        reason.
        """
        stale = witnesses_needing_text(pending)
        if not stale:
            return
        sys_ids = [w['sys_id'] for w in stale]
        # Captured HERE, on the event loop: the fetch runs off-loop and must
        # not read page state.
        rows_snapshot = list(p_state.results or [])
        # The headers the PROMOTION used, where the snapshot preserved them.
        # Without this the rebuild derives headers from whatever rows are on
        # screen NOW -- and after `execute_parallels` has reset the fused rows
        # that is the SEED-ONLY set, so a promoted witness came back built
        # from fewer pages, or from none and hence the whole manuscript. The
        # re-run then searched a different witness under the same label, and
        # nothing on screen said so.
        _derived = witness_headers_for(sys_ids, rows_snapshot)
        _headers = {w['sys_id']: (list(w.get('headers') or [])
                                  or _derived.get(w['sys_id']) or [])
                    for w in stale if w.get('sys_id')}

        def _fetch():
            return collect_witness_texts(
                sys_ids, rows_snapshot,
                fetch_header=state.searcher.get_full_text_by_header,
                fetch_manuscript=state.searcher.get_full_manuscript,
                headers_by_sid=_headers,
            )

        try:
            texts, _failed = await run.io_bound(_fetch)
        except Exception as exc:
            logger.exception(f"Witness rehydrate failed: {exc}")
            return
        # The length cap has to be re-applied HERE, not only where a witness
        # is added. What comes back is not what was promoted: the refetch
        # reads whatever headers the RESTORED row set holds, and falls back
        # to `get_full_manuscript`, which returns the whole manuscript. A
        # reload could therefore turn a capped witness into an uncapped one --
        # and the cap exists because an over-long witness spends its entire
        # 30s ceiling and fails, once per witness.
        from shared.passage_fusion import MAX_WITNESS_CHARS, split_by_length
        _over_cap = 0
        for w in stale:
            _text = texts.get(w['sys_id']) or ''
            _fits, _too_long = split_by_length([_text]) if _text else ([], [])
            if _too_long:
                # Emptied, not truncated -- half a manuscript searched as if
                # it were the whole one is a worse answer than none, and an
                # invisible one. The dispatch loop fails it; the error is set
                # here because only here is the REASON known.
                w['text'] = ''
                w['error'] = tr(
                    'Witness text is too long (max {cap} characters)'
                ).format(cap=MAX_WITNESS_CHARS)
                _over_cap += 1
                continue
            w['text'] = _text
        if _over_cap:
            ui.notify(
                tr('Witness text is too long (max {cap} characters)').format(
                    cap=MAX_WITNESS_CHARS), type='warning')

    async def _search_pending_witnesses(_e=None) -> int:
        """Search every `pending` witness and merge its rows into what is
        already on screen. Additive by construction: a witness is searched at
        most once, which is what makes an R-round expansion cost
        `1 + rounds x K` searches rather than re-running everything.

        Returns the number of witnesses that produced results.
        """
        if not passage_multi_witness_available():
            # The rollout flag enforced where the WORK happens, not only where
            # the button is drawn. Hiding the panel stops a witness being
            # added; it does not stop one that is already there. A tab
            # snapshot taken while the flag was ON restores its witness list
            # `pending`, and the seed search dispatches whatever is pending --
            # so the fan-out could run from browser storage alone after the
            # flag was turned off. All four call sites funnel through here.
            return 0
        if p_state.is_running:
            return 0
        if p_state.is_cancelled:
            # A standing Stop stops the witnesses too. This function used to
            # clear the flag on entry -- but it is reached from the SEED search
            # and from each auto-expand round, not only from a button, so a
            # Stop the user had already pressed was silently undone and the
            # run continued. Only the explicit entry points clear it now
            # (`_clear_stop`).
            return 0
        pending = [w for w in p_state.witnesses if w['status'] == 'pending']
        if not pending:
            return 0
        if not p_state.last_passage_ctx:
            ui.notify(tr('Run a letter-level search first.'), type='warning')
            return 0
        # The depth cap is a DISPATCH cap. Enforced only while adding and
        # promoting, it bounded the wrong quantity: `Find Parallels` resets
        # every non-stale witness to `pending` and dispatches the batch, so
        # twenty-five witnesses added at normal depth all re-run the moment
        # the seed is re-run at `deepest` -- roughly eight minutes of work
        # from one click, taking a slot of the shared passage budget
        # twenty-five times over, against a documented deepest cap of four.
        #
        # Checked AFTER the ctx guard because the cap READS that ctx. A
        # witness search runs at the depth of the LAST SEED SEARCH
        # (`_run_one_witness_search` takes its depth from the same dict), not
        # at whatever the dropdown shows now -- so moving the dropdown alone
        # changes nothing here, and re-running the seed changes everything.
        _over = witnesses_over_dispatch_cap(
            pending, p_state.last_passage_ctx, passage_depth.value)
        if _over:
            # Refuse the whole batch rather than searching a cap's worth:
            # a run that quietly does less than the panel lists is the same
            # lie the auto-expand round already refuses to tell. The
            # witnesses stay `pending`, so removing some or dropping the
            # depth and pressing again is all that is needed.
            ui.notify(
                tr('{n} witnesses are waiting, but only {cap} can be searched '
                   'at this depth — remove some, or choose a shallower '
                   'depth.').format(n=_over[0], cap=_over[1]),
                type='warning')
            return 0
        await _rehydrate_manuscript_witnesses(pending)

        p_state.is_running = True
        found = 0
        try:
            for i, entry in enumerate(pending, start=1):
                # Cancellation is checked at the witness boundary, and only
                # there: the passage engine emits no progress and cannot be
                # interrupted mid-search (PassageSearcher accepts a
                # progress_callback and never calls it), so an in-flight
                # witness always runs to completion.
                if p_state.is_cancelled:
                    break
                if not (entry.get('text') or '').strip():
                    # Never dispatch an empty query. The engine answers it
                    # with nothing, and the panel would then report a
                    # perfectly honest-looking "0 matches" for a search that
                    # never ran -- a false negative no user could detect.
                    entry['status'] = 'failed'
                    # Only when nothing more specific is known. Rehydration
                    # empties an over-long witness and records WHY; saying
                    # "could not load" over that replaces the true reason
                    # with a false one, and the user retries forever.
                    if not entry.get('error'):
                        entry['error'] = tr(
                            'Could not load text for this manuscript.')
                    continue
                entry['status'] = 'running'
                p_state.witness_progress = tr(
                    'Witness {i}/{k}: {label}').format(
                    i=i, k=len(pending), label=entry['label'])
                witness_progress_label.text = p_state.witness_progress
                _refresh_witness_panel()
                result = await _run_one_witness_search(entry)
                if result is None:
                    entry['status'] = 'failed'
                    continue
                rows, filt = _apply_post_search_filters(
                    result.get('main') or [], result.get('filtered') or [])
                p_state.witness_rows[entry['id']] = rows
                p_state.witness_filtered[entry['id']] = filt
                entry['status'] = 'searched'
                entry['hits'] = len(rows)
                entry['error'] = ''
                found += 1
        finally:
            p_state.is_running = False
            p_state.witness_progress = ''
            witness_progress_label.text = ''

        _fuse_and_store()
        _refresh_witness_panel()
        render_results(p_state.results, p_state.filtered_results)
        # `render_results` rewrites the results header from the rows it is
        # given; the summary line and the library-filter button are set once
        # by the seed search and never again. After a witness run the summary
        # described the SEED's count beneath a list showing the fused one --
        # and on the empty-seed path it read "no results yet" above a full
        # page of results. Cleared rather than rewritten: the header already
        # carries the count, and a second count is a second thing to drift.
        summary_label.text = ''
        parallels_library_filter_btn.set_visibility(
            bool(p_state.results or p_state.filtered_results))
        _update_parallels_library_filter_btn()
        _refresh_export_payload()
        _persist_witness_state()
        failed = [w for w in p_state.witnesses if w['status'] == 'failed']
        if failed:
            ui.notify(
                tr('{n} witnesses could not be searched — use Retry.').format(
                    n=len(failed)), type='warning')
        return found

    async def _retry_witness(wid: str) -> None:
        _clear_stop()   # pressing Retry is fresh intent
        for w in p_state.witnesses:
            if w['id'] == wid:
                w['status'] = 'pending'
                w['error'] = ''
        _refresh_witness_panel()
        await _search_pending_witnesses()

    async def _promote_checked(_e=None) -> None:
        """Add the checked manuscripts as witnesses and search them.

        Text comes from the manuscript's MATCHED pages, resolved by their own
        `raw_header`s -- all of them, because a result GROUP spans several
        page-level hits and promoting only the best-scoring page would throw
        away most of the witness. Fetched off the event loop: Tantivy lookups
        on the single uvicorn loop stall every other request.
        """
        # Skip manuscripts already in the list. Two witnesses with identical
        # text would BOTH contribute to witness_count and to the RRF sum, so
        # a manuscript found by one witness would report two -- a wrong
        # number, not merely a redundant search. (Auto-expand already
        # filtered these; the checkbox path did not.)
        if p_state.promoting:
            # Re-entrancy is not a wasted search, it is a WRONG NUMBER: two
            # overlapping runs both read the pre-fetch witness list and both
            # add the same manuscripts, and the duplicate then contributes
            # twice to witness_count and to the RRF sum.
            return
        _already = {w.get('sys_id') for w in p_state.witnesses if w.get('sys_id')}
        sys_ids = [s for s in p_state.checked_for_promotion
                   if s and s not in _already]
        if not sys_ids:
            p_state.checked_for_promotion.clear()
            _refresh_promotion_bar()
            return
        room = _witness_depth_cap() - len(p_state.witnesses)
        if room <= 0:
            ui.notify(tr('Witness list is full (max {n})').format(
                n=_witness_depth_cap()), type='warning')
            return
        if len(sys_ids) > room:
            # The add-witness dialog already reports its own truncation; this
            # path dropped the excess in silence, so ten checked manuscripts
            # with room for three became three with no sign of the seven.
            # Same message, so the two doors say the same thing.
            ui.notify(tr('Witness list is full (max {n})').format(
                n=_witness_depth_cap()), type='warning')
        sys_ids = sys_ids[:room]

        # Captured HERE, on the event loop: the fetch below runs off-loop
        # and must not read page state.
        rows_snapshot = list(p_state.results or [])
        # WHICH pages this promotion is built from, decided once and recorded
        # on the witness. Pure and cheap (a scan of rows already in memory),
        # so it stays on the loop with the rest of the capture.
        promoted_headers = witness_headers_for(sys_ids, rows_snapshot)

        # The seed the promoted rows belong to -- the search that produced
        # them, not whatever the box holds now. Captured before the await with
        # everything else.
        promoted_digest = (p_state.last_passage_ctx or {}).get(
            'seed_digest') or _seed_digest()

        def _fetch():
            # The decision lives in collect_witness_texts (module level, pure,
            # directly tested); this only injects the two real fetchers.
            return collect_witness_texts(
                sys_ids, rows_snapshot,
                fetch_header=state.searcher.get_full_text_by_header,
                fetch_manuscript=state.searcher.get_full_manuscript,
                headers_by_sid=promoted_headers,
            )

        p_state.promoting = True
        try:
            texts, failed = await run.io_bound(_fetch)
        finally:
            p_state.promoting = False
        # Re-read the list AFTER the await: the guard above stops a second
        # promotion, but a witness can also arrive from the add dialog while
        # the fetch is in flight.
        _already = {w.get('sys_id') for w in p_state.witnesses if w.get('sys_id')}
        from shared.passage_fusion import (
            MAX_WITNESS_CHARS, split_by_length)
        # By TEXT as well as by sys_id: a pasted witness has no sys_id, so
        # the guard above cannot see that a promoted manuscript repeats it.
        _keys = _witness_text_keys()
        added, too_long, dupes = 0, [], 0
        for sid in sys_ids:
            text = texts.get(sid)
            if not text or sid in _already:
                continue
            _key = _text_digest_of(text)
            if _key in _keys:
                dupes += 1
                continue
            _keys.add(_key)
            # The same rule the paste path applies, so a manuscript fetched
            # from the corpus and a manuscript pasted by hand cannot disagree
            # about what is searchable.
            _ok, _over = split_by_length([text])
            if _over:
                too_long.append(sid)
                continue
            label = sid
            try:
                shelf, _title = state.meta_mgr.get_meta_for_id(sid)
                label = shelf or sid
            except Exception:
                pass
            _add_witness(text, label=label, kind='manuscript', sys_id=sid,
                         seed_digest=promoted_digest,
                         headers=promoted_headers.get(sid) or [])
            added += 1
        if dupes:
            ui.notify(tr('({n} skipped: already added)').format(n=dupes),
                      type='warning')
        if too_long:
            ui.notify(
                tr('Witness text is too long (max {cap} characters)').format(
                    cap=MAX_WITNESS_CHARS), type='warning')
        if failed:
            # ONE line naming what failed, not N identical toasts saying
            # nothing (fifteen of them was the owner's first experience of
            # this feature).
            names = []
            for sid in failed[:5]:
                try:
                    shelf, _t = state.meta_mgr.get_meta_for_id(sid)
                except Exception:
                    shelf = None
                names.append(shelf or sid)
            more = f' (+{len(failed) - 5})' if len(failed) > 5 else ''
            ui.notify(
                tr('Could not load text for {n} manuscripts: {names}').format(
                    n=len(failed), names=', '.join(names) + more),
                type='warning')
        p_state.checked_for_promotion.clear()
        _refresh_promotion_bar()
        if not added:
            _refresh_witness_panel()
            return
        _refresh_witness_panel()
        found = await _search_pending_witnesses()
        ui.notify(
            tr('Added {n} witnesses — found {m} new matches.').format(
                n=added, m=found), type='positive')

    async def _run_auto_expand(_e=None) -> None:
        """Seed -> top-K -> repeat. Measured on Megillat Antiochus: frontier
        coverage 2 -> 4 -> 7 -> 9 of 20 over three rounds, monotone, with all
        15 promoted witnesses graded positive.

        The cost is the first page: rows go from 191 to 2,795 and positives in
        the top 100 fall from 48 to 32. Reach up, precision down -- which is
        why this is an explicit button with that trade-off written next to it,
        never folded into "Find Parallels".
        """
        if p_state.auto_expanding or p_state.is_running:
            return
        if not p_state.results:
            ui.notify(tr('Run a letter-level search first.'), type='warning')
            return
        rounds = int(auto_rounds.value or 3)
        top_k = int(auto_top_k.value or 5)
        p_state.auto_expanding = True
        _clear_stop()   # pressing Run auto-expand is fresh intent
        try:
            for rnd in range(1, rounds + 1):
                if p_state.is_cancelled:
                    break
                cap = _witness_depth_cap()
                if len(p_state.witnesses) + top_k > cap:
                    # Refuse the ROUND rather than silently shrinking top-K --
                    # a control that quietly does less than it says is a lie.
                    ui.notify(
                        tr('Auto-expand stopped: witness cap reached.'),
                        type='info')
                    break
                already = {w.get('sys_id') for w in p_state.witnesses
                           if w.get('sys_id')}
                candidates = []
                for sid in _ranked_sys_ids(p_state.results):
                    if sid in already or sid in p_state.excluded_manuscript_ids:
                        continue
                    candidates.append(sid)
                    if len(candidates) >= top_k:
                        break
                if not candidates:
                    break
                p_state.checked_for_promotion = set(candidates)
                p_state.witness_progress = tr('Round {r}/{n}').format(
                    r=rnd, n=rounds)
                witness_progress_label.text = p_state.witness_progress
                await _promote_checked()
        finally:
            p_state.auto_expanding = False
            p_state.witness_progress = ''
            witness_progress_label.text = ''

    def _ranked_sys_ids(rows: list) -> list:
        """sys_ids in the order the results present them, de-duplicated."""
        seen, out = set(), []
        for row in rows or []:
            sid = witness_sys_id(row)
            if not sid or sid in seen:
                continue
            seen.add(sid)
            out.append(sid)
        return out

    def _group_witness_stats(group_data: dict) -> dict:
        from shared.passage_fusion import group_stats
        return group_stats(group_data.get('items') or [])

    def _sort_groups(grouped_items, sort_by: str):
        """Order manuscript GROUPS.

        Group order used to be hard-coded to `max_score` regardless of the
        sort control, so two of its three existing options -- 'shelfmark' and
        'matches' -- had no visible effect at all. Routing every option
        through one key function repairs those and adds the two
        multi-witness orders in the same stroke.

        `witnesses` counts DISTINCT witnesses pointing at the manuscript (a
        union across its rows, not a sum -- two pages found by one witness
        are one witness), and `fused` is the combined rank-fusion score, the
        order the API returns.
        """
        if sort_by == 'shelfmark':
            return sorted(grouped_items,
                          key=lambda kv: ((kv[1].get('shelfmark') or '').lower(),
                                          -(kv[1].get('max_score') or 0)))

        def _key(kv):
            data = kv[1]
            top = float(data.get('max_score') or 0)
            if sort_by == 'matches':
                return (len(data.get('items') or []), top)
            if sort_by in ('fused', 'witnesses'):
                stats = _group_witness_stats(data)
                if sort_by == 'witnesses':
                    return (stats['witness_count'], stats['fusion_score'], top)
                return (stats['fusion_score'], top)
            return (top, 0.0)

        return sorted(grouped_items, key=_key, reverse=True)

    def _sync_sort_options() -> None:
        """Offer the fusion orders only once there is fusion to order by.

        An option reading "Sort by number of witnesses" on a single-text
        search would sort by a column that is 1 everywhere.
        """
        base = {
            'score': tr('Sort by score'),
            'shelfmark': tr('Sort by shelfmark'),
            'matches': tr('Sort by matches'),
        }
        if _searched_witness_count() > 1:
            base['fused'] = tr('Sort by combined score')
            base['witnesses'] = tr('Sort by number of witnesses')
            value = sort_select.value if sort_select.value in base else 'fused'
            # Default to the fused order the moment fusion exists: it is the
            # order the ranking actually produced, and leaving 'score'
            # selected would show a rank-fused result set sorted by raw
            # matched letters.
            if sort_select.value == 'score':
                value = 'fused'
        else:
            value = sort_select.value if sort_select.value in base else 'score'
        sort_select.set_options(base, value=value)

    def _source_heading_for(item: dict) -> str:
        """Heading over the query-side excerpt: the WITNESS it came from.

        Falls back to 'Your text' for a single-witness search, where the rows
        carry no witness tag at all (the fusion short-circuits) and the seed
        is the only possible source.
        """
        wid = (item or {}).get('witness_id')
        if not wid or wid == WITNESS_SEED_ID:
            return tr('Your text')
        return (item.get('witness_label')
                or _witness_labels().get(wid) or tr('Pasted text'))

    def _refresh_promotion_bar() -> None:
        promotion_bar.clear()
        chosen = p_state.checked_for_promotion
        promotion_bar.set_visibility(bool(chosen))
        if not chosen:
            return
        with promotion_bar:
            with ui.row().classes('w-full items-center gap-3 p-2 rounded').style(
                    'background: var(--bg-card); '
                    'border: 1px solid var(--primary-600);'):
                ui.icon('groups').classes('text-lg').style(
                    'color: var(--primary-600);')
                ui.label(tr('{n} manuscripts selected').format(
                    n=len(chosen))).classes('text-sm font-medium')
                ui.space()
                ui.button(
                    tr('Search with these too'), icon='playlist_add',
                    on_click=lambda: (_clear_stop(), _promote_checked())[-1],
                ).props('dense no-caps size=sm')
                ui.button(
                    tr('Clear selection'),
                    on_click=lambda: (p_state.checked_for_promotion.clear(),
                                      _refresh_promotion_bar(),
                                      render_results(p_state.results,
                                                     p_state.filtered_results)),
                ).props('flat dense no-caps size=sm')

    def _apply_post_search_filters(rows: list, filtered: list):
        """Put a witness's rows through the same post-search passes the
        seed's rows already went through.

        Applied AT INGEST, before the rows enter `witness_rows`, so every
        later re-fusion operates on already-filtered rows -- filtering after
        the fusion would be undone by the next one.

        The library pass is code-based and works on any row. The domain pass
        can only exclude sys_ids whose domains were loaded for the seed's
        results, so a newly-reached manuscript is never wrongly dropped; it
        may simply not be excluded yet, which is the safe direction.
        """
        ctx = p_state.last_passage_ctx or {}
        mode, codes = ctx.get('library_mode'), ctx.get('library_filter')
        if mode == 'hide' and codes:
            rows = _apply_parallels_library_filter(rows, mode, codes)
            filtered = _apply_parallels_library_filter(filtered, mode, codes)
        if p_state.domain_exclusions and p_state.has_domain_data:
            rows = _filter_parallels_by_domain(rows)
            filtered = _filter_parallels_by_domain(filtered) if filtered else filtered
        if p_state.excluded_manuscript_ids:
            rows = [r for r in rows
                    if _row_sys_id(r) not in p_state.excluded_manuscript_ids]
        return rows, filtered

    def _row_sys_id(row: dict):
        return witness_sys_id(row)

    def _recompute_search_identity() -> str:
        """The identity of the result set AS IT NOW STANDS.

        The same seed searched with three witnesses and with seventeen
        produces different results, so the fingerprint has to move when the
        witness set does -- otherwise recover_richer_parallels_rows would
        hand back rows belonging to a different set.

        Built by REUSING the dispatch-time keyword capture rather than
        re-listing the arguments: an identity rule written out twice is an
        identity rule that drifts (web/export_state.py's own docstring says
        so). Only witnesses that actually PRODUCED rows count -- a pending
        or failed one did not shape these results.
        """
        from web.export_state import compute_parallels_search_fingerprint
        kwargs = dict(getattr(p_state, 'last_fingerprint_kwargs', None) or {})
        if not kwargs:
            return getattr(p_state, 'search_fingerprint', '') or ''
        kwargs['witnesses'] = [
            {'kind': w.get('kind'), 'sys_id': w.get('sys_id'),
             'text': w.get('text') or '', 'label': w.get('label') or ''}
            for w in p_state.witnesses
            if p_state.witness_rows.get(w['id']) is not None
        ]
        return compute_parallels_search_fingerprint(**kwargs)

    def _refresh_export_payload() -> None:
        """Re-publish the export payload after the row set changes.

        Without this the downloadable workbook would still be the seed-only
        result while the screen showed the fused one -- and it would carry
        the seed-only identity, so a reload would 'recover' the wrong rows.
        """
        meta = dict(getattr(p_state, 'last_export_meta', None) or {})
        if not meta:
            return
        try:
            from web.export_state import set_parallels_export
            fingerprint = _recompute_search_identity()
            p_state.search_fingerprint = fingerprint
            meta['search_fingerprint'] = fingerprint
            meta['witnesses'] = [
                {'label': w.get('label') or '', 'kind': w.get('kind'),
                 'sys_id': w.get('sys_id')}
                for w in p_state.witnesses
                if p_state.witness_rows.get(w['id']) is not None
            ] or None
            # The rows are FUSED, so the exported file must be ordered and
            # described the way the screen is. Without this flag the JSON
            # export re-ranked the groups by summed matched letters -- a
            # different order than the user saw -- and dropped the witness
            # facts entirely, though every row still carried them.
            meta['multi_witness'] = bool(meta['witnesses'])
            p_state.last_export_meta = meta
            set_parallels_export(results=p_state.results,
                                 filtered=p_state.filtered_results,
                                 meta=meta)
        except Exception:
            pass  # Export refresh is best-effort; the screen is still right

    def _persist_witness_state() -> None:
        """Mirror the witness list into the tab snapshot.

        Witnesses are deliberately NOT part of `searched_config`: that dict is
        re-applied by `_apply_restored_search_config`, which validates each
        value against a widget's `.options`, and a witness is not a select.
        """
        try:
            _persist_active_snapshot()
        except Exception:
            pass  # Browser storage operation failed; nothing else depends on it

    # The panel's INITIAL state comes from the same refresh every later update
    # uses -- never from the widget constructors, which would be a second
    # definition of "empty" that nothing keeps in step. It has to sit HERE,
    # below the helpers: called from the widget block above (the intuitive
    # place) it raises UnboundLocalError and takes the whole page down with a
    # 500. That is not hypothetical -- it is what happened on the first
    # attempt, and only the render-smoke test saw it.
    _refresh_witness_panel()


    # === DMF-09: Parallels Library Filter Button + Dialog (Phase 131-05) ===

    def _update_parallels_library_filter_btn():
        """Sync the parallels library filter button: 3-state label and color (DMF D-07).

        Three states keyed on mode + active-ness:
          Neutral:      tr('Filter by library') — outline primary.
          Show-only:    tr('Showing {shown}/{total} library'/'libraries') — filled red/negative.
          Hide active:  tr('Hiding {n} library'/'libraries') — filled deep-orange.

        total = selectable-universe count (library_codes_with_manuscripts minus LOCAL),
        NOT a result-facet count — Codex R5 mandate.
        """
        flt = p_state.library_filter
        mode = getattr(p_state, 'library_mode', 'hide')
        if not flt:
            # Neutral: no active restriction
            parallels_library_filter_btn.text = tr('Filter by library')
            parallels_library_filter_btn.props(remove='color')
            parallels_library_filter_btn.props('outline dense no-caps color=primary')
        elif mode == 'show_only':
            # Show-only active: N of total selected
            # total is the selectable-universe (NOT a result facet — Codex R5)
            total = len([c for c in library_codes_with_manuscripts() if c != 'LOCAL'])
            shown = len(flt)
            # REAL Phase-130 pluralized keys (genizah_translations.py:2918-2921)
            _lib_btn_key = ('Showing {shown}/{total} library' if total == 1
                            else 'Showing {shown}/{total} libraries')
            parallels_library_filter_btn.text = tr(_lib_btn_key).format(shown=shown, total=total)
            parallels_library_filter_btn.props(remove='color outline')
            parallels_library_filter_btn.props('dense no-caps color=negative')
        else:  # hide mode, non-empty set
            _n = len(flt)
            _lib_btn_key = ('Hiding {n} library' if _n == 1 else 'Hiding {n} libraries')
            parallels_library_filter_btn.text = tr(_lib_btn_key).format(n=_n)
            parallels_library_filter_btn.props(remove='color outline')
            parallels_library_filter_btn.props('dense no-caps color=deep-orange')

    def _open_parallels_library_filter_dialog():
        """Open dual-mode library filter dialog for /parallels (Phase 131-05 / DMF-09).

        Dialog layout:
          1. Mode toggle (Show-only | Hide) at TOP — D-03.
          2. Text-search input filtering the combined list client-side.
          3. Count shortlist — libraries present in current results, sorted by count desc.
          4. Expandable section — ALL canonical libraries not in shortlist, sorted A-Z. LOCAL excluded.
          5. Select All / Select None / Apply / Cancel buttons.

        Persistence: safe_user_set('parallels_library_filter', {'mode': ..., 'codes': [...]}).
        Show-only normalizes all-selected -> [] via the LOCAL _parallels_apply_selection helper
        (Codex N2: NOT search.py's nested _library_apply_selection).
        JS namespace: parLibFilter* (separate from search page's libFilter*).
        """
        import html as _html
        import uuid as _uuid
        import json as _json_plibfilter
        from collections import Counter

        lang = get_language()
        container_id = f'par-lib-filter-{_uuid.uuid4().hex[:8]}'
        current_filter = set(p_state.library_filter)
        current_mode = [getattr(p_state, 'library_mode', 'hide')]  # mutable cell

        # Build shortlist: libraries present in current results, LOCAL excluded (DMF-10).
        # Derive from p_state.results via get_library_for_id (or display.library_code).
        def _get_result_lib(item):
            lc = item.get('display', {}).get('library_code', '')
            if lc:
                return lc
            if state.meta_mgr:
                try:
                    raw_header = item.get('raw_header', '')
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        return state.meta_mgr.get_library_for_id(sys_match.group(1)) or ''
                except Exception:
                    pass
            return ''

        facets = Counter(
            _get_result_lib(r) for r in (p_state.results or [])
            if _get_result_lib(r) and _get_result_lib(r) != 'LOCAL'
        )

        # Shortlist codes: libraries in results, LOCAL excluded (HIGH-2 / DMF-10)
        shortlist_codes = sorted(
            [c for c in facets if c in LIBRARY_CODES and c != 'LOCAL'],
            key=lambda c: -facets[c],
        )
        shortlist_set = set(shortlist_codes)

        # Expand section: all canonical libraries not in shortlist, LOCAL excluded (DMF-10/DMF-13)
        # Keep literal `c != 'LOCAL'` so the AST LOCAL guard passes (DMF-10).
        _codes_with_mss = library_codes_with_manuscripts()
        expand_codes = sorted(
            [c for c in LIBRARY_CODES if c != 'LOCAL' and c not in shortlist_set
             and c in _codes_with_mss],
            key=lambda c: get_library_display(c, short=False, lang=lang),
        )

        def _make_cb_row(code, label_text, checked):
            """Single checkbox row HTML — parLibFilter* classes."""
            code_attr = _html.escape(code, quote=True)
            label_esc = _html.escape(label_text, quote=True)
            checked_attr = 'checked' if checked else ''
            return (
                f'<label class="par-lib-cb-row" data-label="{label_esc.lower()}" '
                f'style="display:flex;align-items:center;gap:8px;'
                f'padding:4px 0;cursor:pointer;font-size:0.9rem">'
                f'<input type="checkbox" class="par-lib-cb" data-code="{code_attr}" '
                f'{checked_attr} '
                f'style="width:16px;height:16px;accent-color:#1976d2;cursor:pointer" '
                f'onchange="parLibFilterUpdateApply(\'{container_id}\')">'
                f'<span>{label_esc}</span></label>'
            )

        def _is_checked_init(code):
            if current_mode[0] == 'show_only':
                return (not current_filter) or (code in current_filter)
            else:
                return code in current_filter

        # Shortlist rows (with count)
        shortlist_rows = []
        for code in shortlist_codes:
            count = facets[code]
            label = get_library_display(code, short=False, lang=lang)
            shortlist_rows.append(_make_cb_row(code, f"{label} ({count})", _is_checked_init(code)))

        # Expand section rows (no count)
        expand_rows = []
        for code in expand_codes:
            label = get_library_display(code, short=False, lang=lang)
            expand_rows.append(_make_cb_row(code, label, _is_checked_init(code)))

        init_mode = _html.escape(current_mode[0], quote=True)
        full_html = f'<div id="{container_id}" data-libmode="{init_mode}">'
        full_html += '\n'.join(shortlist_rows)
        if expand_rows:
            expand_label = _html.escape(tr('All libraries'))
            full_html += (
                f'<details style="margin-top:8px">'
                f'<summary style="cursor:pointer;font-size:0.85rem;color:#666;'
                f'padding:4px 0">{expand_label}</summary>'
                + '\n'.join(expand_rows)
                + '</details>'
            )
        full_html += '</div>'

        with ui.dialog() as dialog, ui.card().classes('w-[520px] max-h-[80vh]'):
            with ui.column().classes('w-full gap-2'):
                ui.label(tr('Filter by Library')).classes('text-lg font-bold')

                # Mode toggle (D-03): Show-only | Hide
                mode_options = {
                    'show_only': tr('Show only selected'),
                    'hide': tr('Hide selected'),
                }
                mode_toggle = ui.toggle(
                    options=mode_options,
                    value=current_mode[0],
                ).props('dense no-caps')

                def _on_mode_change(new_mode):
                    current_mode[0] = new_mode
                    # D-04: flipping mode resets checked set + re-syncs Apply-enable.
                    ui.run_javascript(f'parLibFilterSetMode("{container_id}", "{new_mode}")')

                mode_toggle.on_value_change(lambda e: _on_mode_change(e.value))

                # Text-search input — client-side row filter
                ui.input(
                    placeholder=tr('Search libraries...'),
                    on_change=lambda e: ui.run_javascript(
                        f'parLibFilterSearch("{container_id}", {_json_plibfilter.dumps(e.value or "")})'
                    ),
                ).props('dense clearable').classes('w-full')

                with ui.scroll_area().classes('w-full').style('max-height: 45vh;'):
                    ui.html(full_html, sanitize=False)

                # Buttons row
                with ui.row().classes('w-full justify-between'):
                    _cid = container_id
                    # WR-01: full selectable universe for show-all normalization
                    _all_for_norm = shortlist_codes + expand_codes

                    with ui.row().classes('gap-1'):
                        ui.button(
                            tr('Select All'),
                            on_click=lambda: ui.run_javascript(
                                f'parLibFilterSelectAll("{_cid}", true)')
                        ).props('flat dense no-caps')
                        ui.button(
                            tr('Select None'),
                            on_click=lambda: ui.run_javascript(
                                f'parLibFilterSelectAll("{_cid}", false)')
                        ).props('flat dense no-caps')

                    with ui.row().classes('gap-2'):
                        async def apply_parallels_library_filter():
                            checked_list = await ui.run_javascript(
                                f'parLibFilterGetChecked("{_cid}")', timeout=5.0
                            )
                            checked = list(checked_list) if checked_list else []
                            # Sanitize JS-returned codes (drops non-str, unknown, LOCAL).
                            checked = sanitize_library_codes(checked)
                            committed_mode = current_mode[0]

                            if committed_mode == 'show_only':
                                if not checked:
                                    ui.notify(
                                        tr('Select at least one library, or check all to clear the filter'),
                                        type='warning',
                                    )
                                    return
                                # All-in-universe checked -> [] mapping (show-all for Show-only).
                                # Uses LOCAL _parallels_apply_selection (Codex N2 — NOT search.py's nested fn).
                                new_filter = _parallels_apply_selection(checked, _all_for_norm)
                                # Show-all normalization: Show-only + empty codes = neutral Hide/[].
                                if not new_filter:
                                    p_state.library_mode = 'hide'
                                    p_state.library_filter = []
                                else:
                                    p_state.library_mode = 'show_only'
                                    p_state.library_filter = new_filter
                            else:
                                # Hide mode: empty set allowed (hide nothing = show all, D-08).
                                p_state.library_mode = 'hide'
                                p_state.library_filter = checked

                            # Persist dict shape (never a bare list).
                            safe_user_set('parallels_library_filter', {
                                'mode': p_state.library_mode,
                                'codes': p_state.library_filter,
                            })
                            _update_parallels_library_filter_btn()
                            parallels_library_filter_btn.set_visibility(True)
                            dialog.close()

                            # Show-only re-runs the search pre-query scoped (restrict_sys_ids).
                            # Hide re-renders with post-fetch filter applied.
                            if p_state.library_mode == 'show_only' and p_state.library_filter:
                                await execute_parallels()
                            elif p_state.results:
                                # Re-render applying hide filter
                                _rerender_with_library_filter()

                        apply_btn = ui.button(
                            tr('Apply'), on_click=apply_parallels_library_filter
                        ).props('dense no-caps color=primary')
                        apply_btn.props(f'id="parLibApplyBtn_{container_id}"')
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat dense no-caps')

        dialog.open()
        # Initialize Apply disabled-state from current checked count + mode.
        ui.run_javascript(f'parLibFilterUpdateApply("{container_id}")')

    def _rerender_with_library_filter():
        """Re-render results applying all post-fetch filters including library filter."""
        main_results = p_state.results
        filtered_results = p_state.filtered_results
        if p_state.domain_exclusions and p_state.has_domain_data:
            main_results = _filter_parallels_by_domain(main_results)
            filtered_results = _filter_parallels_by_domain(filtered_results) if filtered_results else filtered_results
        # Apply library Hide filter (Show-only is pre-query so not needed here on re-render)
        if p_state.library_mode == 'hide' and p_state.library_filter:
            main_results = _apply_parallels_library_filter(
                main_results, p_state.library_mode, p_state.library_filter)
            if filtered_results:
                filtered_results = _apply_parallels_library_filter(
                    filtered_results, p_state.library_mode,
                    p_state.library_filter)
        render_results(main_results, filtered_results)

    # If session restored a library filter, sync button state now (after functions are defined)
    if p_state.library_filter:
        parallels_library_filter_btn.set_visibility(True)
        _update_parallels_library_filter_btn()

    # === Sefaria Loading Functions ===
    def show_sefaria_selection_dialog(source_type: str):
        """Show dialog to select books from a Sefaria source."""
        source_data = SEFARIA_SOURCES.get(source_type)
        if not source_data:
            return

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[400px] max-w-[500px]'):
            h3(tr('Select Books'), classes='text-xl font-bold mb-4').style('color: var(--text-primary);')

            # Category selector
            with ui.row().classes('w-full items-center gap-2 mb-4'):
                ui.label(tr('Category:')).classes('text-sm').style('color: var(--text-secondary);')
                cat_options = {'all': tr('All')}
                for key, book_data in source_data['books'].items():
                    cat_options[key] = book_data['name']
                cat_select = ui.select(cat_options, value='all').props('outlined dense').classes('flex-grow')

            # Books list container
            books_container = ui.column().classes('w-full max-h-64 overflow-y-auto gap-1 p-2 rounded').style('background: var(--bg-secondary);')

            # Track selected books
            selected_refs = {'refs': []}

            def populate_books():
                books_container.clear()
                cat_key = cat_select.value

                with books_container:
                    if cat_key == 'all':
                        for book_key, book_data in source_data['books'].items():
                            with ui.expansion(book_data['name'], icon='folder').classes('w-full'):
                                for ref, he_name in zip(book_data['refs'], book_data['he_names']):
                                    cb = ui.checkbox(he_name).classes('text-sm')
                                    cb.on('update:model-value', lambda checked, r=ref: toggle_ref(r, checked))
                    else:
                        book_data = source_data['books'].get(cat_key, {})
                        for ref, he_name in zip(book_data.get('refs', []), book_data.get('he_names', [])):
                            cb = ui.checkbox(he_name).classes('text-sm')
                            cb.on('update:model-value', lambda checked, r=ref: toggle_ref(r, checked))

            def toggle_ref(ref, checked):
                if checked and ref not in selected_refs['refs']:
                    selected_refs['refs'].append(ref)
                elif not checked and ref in selected_refs['refs']:
                    selected_refs['refs'].remove(ref)

            cat_select.on('update:model-value', lambda: populate_books())

            # Select all checkbox
            def select_all(checked):
                selected_refs['refs'] = []
                if checked:
                    for book_data in source_data['books'].values():
                        selected_refs['refs'].extend(book_data['refs'])
                populate_books()

            ui.checkbox(tr('Select All'), on_change=lambda e: select_all(e.value)).classes('my-2')

            populate_books()

            # Buttons
            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Load Selected'), on_click=lambda: load_selected_refs(selected_refs['refs'], dialog)).classes('btn-primary')

        dialog.open()

    def refresh_loaded_sources_ui():
        """Refresh the list of loaded sources with checkboxes."""
        loaded_sources_container.clear()

        with loaded_sources_container:
            if not filter_sources['loaded']:
                ui.label(tr('No sources loaded yet')).classes('text-sm text-gray-500')
            else:
                for ref in sorted(filter_sources['loaded'].keys()):
                    cb = ui.checkbox(get_source_display_name(ref), value=ref in filter_sources['enabled']).classes('text-sm')
                    cb.on('update:model-value', lambda checked, r=ref: on_source_toggled(r, checked))

        update_filter_info()

    def save_filter_sources():
        """Save filter source refs to persistent storage (not the full text - too large)."""
        try:
            # Separate custom texts from Sefaria refs
            sefaria_refs = [ref for ref in filter_sources['loaded'].keys() if not ref.startswith('custom:')]
            custom_texts = {ref: filter_sources['loaded'][ref] for ref in filter_sources['loaded'].keys() if ref.startswith('custom:')}

            # Save Sefaria refs (text reloaded from cache)
            safe_user_set('filter_sources_refs', sefaria_refs)
            safe_user_set('filter_sources_enabled', list(filter_sources['enabled']))

            # Save custom texts (small enough to store directly)
            safe_user_set('filter_sources_custom', custom_texts)
            safe_user_set('filter_sources_custom_count', filter_sources.get('custom_count', 0))
        except Exception:
            pass  # Cache operation failed; continue without cached data

    def on_source_toggled(ref, checked):
        """Handle source checkbox toggle."""
        if checked:
            filter_sources['enabled'].add(ref)
        else:
            filter_sources['enabled'].discard(ref)
        update_filter_info()  # Also updates badge
        save_filter_sources()

    def update_filter_info():
        """Update the info label and badge."""
        enabled = len(filter_sources['enabled'])
        total = len(filter_sources['loaded'])
        filter_info_label.text = tr('Active: {} / {}').format(enabled, total)
        update_filter_badge()

    def select_all_sources():
        filter_sources['enabled'] = set(filter_sources['loaded'].keys())
        refresh_loaded_sources_ui()
        save_filter_sources()

    def deselect_all_sources():
        filter_sources['enabled'].clear()
        refresh_loaded_sources_ui()
        save_filter_sources()

    def remove_unchecked_sources():
        to_remove = [ref for ref in filter_sources['loaded'].keys() if ref not in filter_sources['enabled']]
        for ref in to_remove:
            del filter_sources['loaded'][ref]
        refresh_loaded_sources_ui()
        save_filter_sources()

    def get_filter_text():
        """Get combined text from all enabled sources."""
        texts = [filter_sources['loaded'][ref] for ref in filter_sources['enabled'] if ref in filter_sources['loaded']]
        return " ".join(texts)

    # Connect filter management buttons
    btn_select_all.on('click', select_all_sources)
    btn_deselect_all.on('click', deselect_all_sources)
    btn_remove_unchecked.on('click', remove_unchecked_sources)

    async def load_selected_refs(refs, dialog):
        """Load selected refs from Sefaria with incremental progress."""
        if not refs:
            ui.notify(tr('Please select at least one book.'), type='warning')
            return

        if dialog:
            try:
                dialog.close()
            except Exception:
                pass  # UI element update optional; continue rendering

        # Filter out already loaded refs
        new_refs = [r for r in refs if r not in filter_sources['loaded']]
        if not new_refs:
            ui.notify(tr('All selected sources are already loaded.'), type='info')
            return

        # Show progress
        try:
            sefaria_progress.style('display: block;')
            sefaria_status.style('display: block;')
            sefaria_progress.value = 0
        except (RuntimeError, Exception):
            return  # Client deleted

        total = len(new_refs)
        loaded_count = 0
        failed_count = 0

        try:
            sefaria_status.text = tr('Loading: {}').format(f"0/{total}")
        except (RuntimeError, Exception):
            return

        # Fetch one at a time with progress updates
        for i, ref in enumerate(new_refs):
            # Check if client is still valid
            try:
                _ = sefaria_progress.client
            except (RuntimeError, Exception):
                return

            # Update progress before fetching
            try:
                sefaria_status.text = tr('Loading: {}').format(f"{i}/{total} - {get_source_display_name(ref)[:30]}...")
                sefaria_progress.value = i / total
            except (RuntimeError, Exception):
                return

            # Fetch in background thread to avoid blocking UI
            text = await run.io_bound(fetch_sefaria_text, ref)

            if text:
                filter_sources['loaded'][ref] = text
                filter_sources['enabled'].add(ref)
                loaded_count += 1
            else:
                failed_count += 1

            # Update UI periodically (every item)
            try:
                sefaria_progress.value = (i + 1) / total
            except (RuntimeError, Exception):
                return

        # Save to storage
        save_filter_sources()

        # Update UI
        try:
            refresh_loaded_sources_ui()

            # Notify only on failure
            if failed_count > 0:
                ui.notify(f'{tr("Failed to load")} {failed_count} {tr("sources")}', type='negative')

            # Hide progress
            sefaria_progress.style('display: none;')
            sefaria_status.style('display: none;')
        except (RuntimeError, Exception):
            pass  # Client deleted

    # Connect Sefaria buttons
    btn_tanakh.on('click', lambda: show_sefaria_selection_dialog('tanakh'))
    btn_mishnah.on('click', lambda: show_sefaria_selection_dialog('mishnah'))
    btn_talmud.on('click', lambda: show_sefaria_selection_dialog('talmud'))
    btn_more.on('click', lambda: show_all_sources_dialog())
    btn_sefaria_search.on('click', lambda: show_sefaria_search_dialog())
    btn_add_custom.on('click', lambda: show_add_custom_dialog())

    def show_add_custom_dialog():
        """Show dialog to add custom text source."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[500px] max-w-[600px]'):
            h3(tr('Add Custom Text'), classes='text-xl font-bold mb-4').style('color: var(--text-primary);')

            ui.label(tr('Enter a name for this source:')).classes('text-sm').style('color: var(--text-secondary);')
            name_input = ui.input(placeholder=tr('e.g., My Commentary')).classes('w-full mb-4').props('outlined')

            ui.label(tr('Paste your text (will be cleaned automatically):')).classes('text-sm').style('color: var(--text-secondary);')
            text_area = ui.textarea(placeholder=tr('Paste Hebrew text here...')).classes('w-full').props('outlined rows=10').style('direction: rtl;')

            def add_custom_text():
                name = name_input.value.strip() if name_input.value else ''
                text = text_area.value.strip() if text_area.value else ''

                if not name:
                    ui.notify(tr('Please enter a name for the source'), type='warning')
                    return
                if not text or len(text) < 10:
                    ui.notify(tr('Please enter at least 10 characters of text'), type='warning')
                    return

                # Clean the text
                cleaned = clean_hebrew_text(text)
                if not cleaned or len(cleaned) < 10:
                    ui.notify(tr('No valid Hebrew text found'), type='warning')
                    return

                # Generate a unique ref for custom text
                filter_sources['custom_count'] = filter_sources.get('custom_count', 0) + 1
                custom_ref = f"custom:{filter_sources['custom_count']}:{name}"

                # Add to sources
                filter_sources['loaded'][custom_ref] = cleaned
                filter_sources['enabled'].add(custom_ref)
                save_filter_sources()
                refresh_loaded_sources_ui()

                dialog.close()
                ui.notify(f'{tr("Added")} "{name}" ({len(cleaned)} {tr("characters")})', type='positive')

            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Add'), on_click=add_custom_text).classes('btn-primary')

        dialog.open()

    async def show_sefaria_search_dialog():
        """Show dialog to search and load any Sefaria text by reference."""
        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-[500px] max-w-[600px]'):
            h3(tr('Search Sefaria'), classes='text-xl font-bold mb-4').style('color: var(--text-primary);')

            ui.label(tr('Enter a Sefaria reference (e.g., "Genesis 1", "Berakhot 2a", "Rashi on Genesis 1"):')).classes('text-sm').style('color: var(--text-muted);')
            ref_input = ui.input(placeholder='Genesis 1').classes('w-full mb-2').props('outlined')

            # Quick examples
            ui.label(tr('Examples:')).classes('text-xs mt-2').style('color: var(--text-muted);')
            with ui.row().classes('gap-1 flex-wrap'):
                for example in ['Genesis 1', 'Exodus', 'Psalms', 'Berakhot', 'Shabbat', 'Rashi on Genesis', 'Mishneh Torah']:
                    ui.button(example, on_click=lambda e=example: ref_input.set_value(e)).props('flat dense size=xs')

            # Status
            search_status = ui.label('').classes('text-sm mt-4').style('color: var(--text-secondary);')

            async def search_and_load():
                ref = ref_input.value.strip() if ref_input.value else ''
                if not ref:
                    ui.notify(tr('Please enter a Sefaria reference'), type='warning')
                    return

                search_status.text = tr('Searching...')

                # Try to fetch
                text = await run.io_bound(fetch_sefaria_text, ref, True)

                if text:
                    filter_sources['loaded'][ref] = text
                    filter_sources['enabled'].add(ref)
                    save_filter_sources()
                    refresh_loaded_sources_ui()
                    dialog.close()
                    ui.notify(f'{tr("Loaded")} "{ref}" ({len(text)} {tr("characters")})', type='positive')
                else:
                    search_status.text = tr('Not found. Try a different reference.')
                    ui.notify(tr('Text not found'), type='negative')

            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                ui.button(tr('Load'), on_click=search_and_load).classes('btn-primary')

        dialog.open()

    async def show_all_sources_dialog():
        """Show dialog to browse all Sefaria sources in hierarchical tree."""
        library = get_sefaria_library()

        # Track selected refs
        selected_refs_state = {'refs': set()}

        with ui.dialog().classes('max-w-4xl') as dialog, ui.card().classes('p-6 w-full').style('min-width: 700px; max-height: 80vh;'):
            h3(tr('Sefaria Library'), classes='text-xl font-bold mb-4').style('color: var(--text-primary);')

            # Search box
            with ui.row().classes('w-full items-center gap-2 mb-4'):
                ui.label(tr('Search:')).classes('text-sm').style('color: var(--text-secondary);')
                search_input = ui.input(placeholder=tr('Search texts...')).classes('flex-grow').props('outlined dense')

            # Status label
            status_label = ui.label(tr('Loading library...')).classes('text-sm').style('color: var(--text-muted);')

            # Main content area with two columns
            with ui.splitter(value=35).classes('w-full').style('height: 400px;') as splitter:
                with splitter.before:
                    # Category tree (left side)
                    with ui.scroll_area().classes('w-full h-full'):
                        categories_container = ui.column().classes('w-full gap-1 p-2')

                with splitter.after:
                    # Texts list (right side)
                    with ui.column().classes('w-full h-full'):
                        texts_container = ui.scroll_area().classes('w-full flex-grow')
                        with ui.row().classes('w-full items-center gap-2 mt-2'):
                            select_all_cb = ui.checkbox(tr('Select All in Category'))
                            info_label = ui.label(tr('Selected: 0')).classes('text-xs').style('color: var(--text-muted);')

            # Buttons
            with ui.row().classes('w-full justify-end gap-2 mt-4'):
                ui.button(tr('Cancel'), on_click=dialog.close).props('flat')
                load_btn = ui.button(tr('Load Selected'), on_click=lambda: finish_selection()).classes('btn-primary')

            def update_info():
                info_label.text = tr('Selected: {}').format(len(selected_refs_state['refs']))

            def toggle_ref(ref, checked):
                if checked:
                    selected_refs_state['refs'].add(ref)
                else:
                    selected_refs_state['refs'].discard(ref)
                update_info()

            def show_category_texts(category_data):
                """Show texts from a category in the right panel."""
                texts_container.clear()
                select_all_cb.value = False

                texts = library.get_texts_recursive(category_data)

                with texts_container:
                    with ui.column().classes('w-full gap-1 p-2'):
                        for text in texts:
                            title = text.get('title', '')
                            he_title = text.get('heTitle', title)
                            cb = ui.checkbox(he_title, value=title in selected_refs_state['refs']).classes('text-sm')
                            cb.on('update:model-value', lambda checked, r=title: toggle_ref(r, checked))

                # Connect select all
                def on_select_all(checked):
                    for text in texts:
                        title = text.get('title', '')
                        if checked:
                            selected_refs_state['refs'].add(title)
                        else:
                            selected_refs_state['refs'].discard(title)
                    show_category_texts(category_data)  # Refresh to update checkboxes
                    update_info()

                select_all_cb.on('update:model-value', on_select_all)
                update_info()

            def build_category_tree(parent_container, contents, depth=0):
                """Recursively build the category tree."""
                for item in contents:
                    if isinstance(item, dict):
                        if 'category' in item:
                            # It's a category
                            cat_name = item.get('heCategory', item.get('category', ''))
                            sub_contents = item.get('contents', [])

                            if sub_contents:
                                # Has children - make it expandable
                                with parent_container:
                                    with ui.expansion(cat_name, icon='folder').classes('w-full'):
                                        inner_container = ui.column().classes('w-full gap-1 pl-4')
                                        build_category_tree(inner_container, sub_contents, depth + 1)

                                    # Add click handler to show texts
                                    # The expansion header can be clicked to show texts
                            else:
                                # Leaf category
                                with parent_container:
                                    btn = ui.button(cat_name, on_click=lambda i=item: show_category_texts(i)).props('flat dense align=left').classes('w-full justify-start')

            async def load_library():
                """Load the Sefaria library TOC."""
                toc = await run.io_bound(library.get_toc)
                if not toc:
                    status_label.text = tr('Failed to load library. Check internet connection.')
                    return

                status_label.text = ''
                categories_container.clear()

                with categories_container:
                    for category in toc:
                        if isinstance(category, dict) and 'category' in category:
                            cat_name = category.get('heCategory', category.get('category', ''))
                            sub_contents = category.get('contents', [])

                            with ui.expansion(cat_name, icon='folder').classes('w-full') as exp:
                                inner_container = ui.column().classes('w-full gap-1')

                                # Add click handler for the expansion to show its texts
                                exp.on('click', lambda c=category: show_category_texts(c))

                                if sub_contents:
                                    build_category_tree(inner_container, sub_contents, 1)

            async def finish_selection():
                """Complete the selection and load texts."""
                if not selected_refs_state['refs']:
                    ui.notify(tr('Please select at least one book.'), type='warning')
                    return
                dialog.close()
                await load_selected_refs(list(selected_refs_state['refs']), None)

            # Start loading
            await load_library()

        dialog.open()

    async def load_all_sources_refs(refs, dialog):
        """Load selected refs from the all sources dialog."""
        if not refs:
            ui.notify(tr('Please select at least one book.'), type='warning')
            return

        dialog.close()
        await load_selected_refs(refs, None)

    def update_ui():
        """Update progress UI. Returns False if the client is gone and the loop should stop."""
        try:
            # Check if client still exists
            _ = progress_bar.client
        except (RuntimeError, Exception):
            return False

        try:
            if p_state.is_running:
                run_btn.disable()
                cancel_btn.style('display: flex;')
                search_indicator.style('display: flex;')
                summary_label.text = ''  # Clear summary during active search
                progress_bar.style('opacity: 1;')
                progress_bar.set_value(p_state.progress)
                # Compute elapsed time + ETA
                elapsed = time.time() - p_state.search_start_time if p_state.search_start_time else 0
                if elapsed >= 3600:
                    elapsed_str = f"{int(elapsed // 3600)}:{int((elapsed % 3600) // 60):02d}:{int(elapsed % 60):02d}"
                else:
                    elapsed_str = f"{int(elapsed // 60)}:{int(elapsed % 60):02d}"
                # Build status with elapsed, chunk count, and ETA
                chunks_str = ""
                eta_str = ""
                if p_state.chunks_total > 0:
                    chunks_str = f"{p_state.chunks_processed}/{p_state.chunks_total} {tr('chunks')}"
                    # Compute ETA with 2-second smoothing
                    now = time.time()
                    if p_state.chunks_processed > 0 and elapsed > 0:
                        if (now - p_state.last_eta_update) >= 2.0:
                            rate = p_state.chunks_processed / elapsed
                            remaining = (p_state.chunks_total - p_state.chunks_processed) / rate
                            if remaining >= 3600:
                                eta_str = f"\u223c{int(remaining // 3600)}:{int((remaining % 3600) // 60):02d}:{int(remaining % 60):02d} {tr('remaining')}"
                            else:
                                eta_str = f"\u223c{int(remaining // 60)}:{int(remaining % 60):02d} {tr('remaining')}"
                            p_state.last_eta_text = eta_str
                            p_state.last_eta_update = now
                        else:
                            eta_str = p_state.last_eta_text
                # Assemble status line
                parts = [elapsed_str]
                if chunks_str:
                    parts.append(chunks_str)
                if eta_str:
                    parts.append(eta_str)
                status_label.text = " \u2014 ".join(parts)
            else:
                run_btn.enable()
                cancel_btn.style('display: none;')
                search_indicator.style('display: none;')
                if p_state.progress >= 1.0 and not p_state.finished_animation_shown:
                    progress_bar.set_value(1.0)
                    p_state.finished_animation_shown = True
                    # Don't auto-hide progress bar -- summary stays visible until next search
        except (RuntimeError, Exception):
            return False  # Client deleted — stop the loop
        return True

    # Use asyncio loop for progress updates instead of ui.timer to avoid
    # "parent slot of the element has been deleted" RuntimeError on navigation
    if p_state.update_timer:
        p_state.update_timer.cancel()
    async def _update_loop():
        while True:
            if not update_ui():
                break
            await asyncio.sleep(0.05)
    p_state.update_timer = asyncio.ensure_future(_update_loop())

    def cancel_search():
        p_state.is_cancelled = True
        p_state.status = tr('Cancelling...')
        # Hide top page loading bar on cancel
        ui.run_javascript('if (window.__hideLoadingBar) window.__hideLoadingBar();')

    # --- Composition History UI Helpers ---
    def _refresh_comp_history_menu():
        """Refresh the composition history dropdown menu contents."""
        comp_history_menu.clear()
        history = _get_comp_history()
        if not history:
            with comp_history_menu:
                ui.menu_item(tr('No composition history')).props('disable')
            return

        def _build_web_filter_summary(filters: dict, max_len: int = 50) -> str:
            return build_filter_summary(filters, tr, get_language, max_len)

        with comp_history_menu:
            for i, entry in enumerate(history):
                title_text = entry.get('title', '')
                title_display = (title_text[:40] + '...') if len(title_text) > 40 else title_text
                count = entry.get('result_count', 0)
                # Build filter summary text from params
                filters = entry.get('params', {}).get('filters')
                filter_text = _build_web_filter_summary(filters) if filters else ''
                label = f"{title_display}  ({count})"

                idx = i  # Capture for closure
                with ui.menu_item(label, on_click=lambda e, idx=idx: _on_comp_history_clicked(idx)).style('direction: rtl;'):
                    if filter_text:
                        ui.label(filter_text).style('font-size: 0.7rem; color: var(--primary-600); direction: ltr;')
                    # Delete button on each item
                    ui.button(icon='close', on_click=lambda e, idx=idx: (
                        _delete_comp_history_entry(idx), _refresh_comp_history_menu()
                    )).props('flat dense size=xs round').classes('ml-auto')

            ui.separator()
            ui.menu_item(tr('Clear all'), on_click=lambda: (
                _clear_comp_history(), _refresh_comp_history_menu()
            ))

    async def _on_comp_history_clicked(index: int):
        """Restore state from a composition history entry."""
        history = _get_comp_history()
        if index >= len(history):
            return
        entry = history[index]
        state_snapshot = entry.get('state', {})
        params = entry.get('params', {})

        # Restore source text
        if state_snapshot.get('source_text'):
            text_input.value = state_snapshot['source_text']

        # Restore filter state from history entry
        filters = params.get('filters')
        if filters and isinstance(filters, dict):
            # Migrate from legacy single-value to lists
            _d = filters.get('domains') or ([filters['domain']] if filters.get('domain') else [])
            _a = filters.get('authors') or ([filters['author']] if filters.get('author') else [])
            _w = filters.get('works') or ([filters['work']] if filters.get('work') else [])
            p_state.filter_domains = _d
            p_state.filter_authors = _a
            p_state.filter_works = _w
            p_state.filter_include_mode = filters.get('include_mode', True)
            p_state.filter_date_from = filters.get('date_from')
            p_state.filter_date_to = filters.get('date_to')
            p_state.filter_material_exclude = filters.get('material_exclude', [])
            p_state.filter_text_all = filters.get('text_all', [])
            p_state.filter_text_any = filters.get('text_any', [])
            p_state.filter_text_not = filters.get('text_not', [])
            # Update filter UI elements
            p_domain_select.value = p_state.filter_domains
            p_author_select.value = p_state.filter_authors
            p_work_select.value = p_state.filter_works
            p_filter_mode_toggle.value = p_state.filter_include_mode
            p_date_from_input.value = p_state.filter_date_from
            p_date_to_input.value = p_state.filter_date_to
            p_exclude_printed_cb.value = 'Printed' in p_state.filter_material_exclude
            # Persist restored filters
            persist_value('parallels_filter_domains', p_state.filter_domains)
            persist_value('parallels_filter_authors', p_state.filter_authors)
            persist_value('parallels_filter_works', p_state.filter_works)
            persist_value('parallels_filter_include_mode', p_state.filter_include_mode)
            persist_value('parallels_filter_date_from', p_state.filter_date_from)
            persist_value('parallels_filter_date_to', p_state.filter_date_to)
            persist_value('parallels_filter_material_exclude', p_state.filter_material_exclude)
            persist_value('parallels_filter_text_all', p_state.filter_text_all)
            persist_value('parallels_filter_text_any', p_state.filter_text_any)
            persist_value('parallels_filter_text_not', p_state.filter_text_not)
            _update_p_chip_bar()
            _rebuild_p_text_chips()
        else:
            # Clear filters if history entry had none
            _clear_all_p_adv_filters()

        # Restore per-manuscript exclusions
        if state_snapshot.get('excluded_manuscript_ids'):
            p_state.excluded_manuscript_ids = set(state_snapshot['excluded_manuscript_ids'])
            persist_value('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
            _update_p_chip_bar()

        # Restore results and state from snapshot
        if state_snapshot.get('results'):
            p_state.results = state_snapshot['results']
            p_state.filtered_results = state_snapshot.get('filtered_results', [])
            p_state.domain_exclusions = set(state_snapshot.get('domain_exclusions', []))

            # Phase 88: build per-session export payload from snapshot canonical fields
            # (state_snapshot['source_text'] at line 2216, params dict at line 1834). Do NOT
            # reconstruct from result rows — that loses fidelity (params.chunk_size/mode/filters
            # are not necessarily present on result rows). Historical context: 77-REVIEWS.md
            # HIGH-03; singleton mirror removed.
            _parallels_search_meta = {
                'source_text': state_snapshot.get('source_text', '') or '',
                'chunk_size': params.get('chunk_size'),
                'mode': params.get('mode'),
                'max_freq': None,  # Not stored on snapshot; explicit null for replay
                'filters': params.get('filters'),  # 10-key shape from parallels.py:2202-2213
                'boundary_options': None,
                'warnings': ['restored-from-history'],
            }
            # Workflow review (P1): this handler wrote the payload but never
            # touched p_state.search_fingerprint, so the NEXT snapshot persist
            # stamped these history-restored rows with the identity of
            # whatever search ran before them -- and a later reload could
            # recover that unrelated search's payload into this view. Stamp
            # the restored search's own identity, through the one helper that
            # defines identity (history knows only a subset of the inputs;
            # the rest default to None, which is honest -- a restored entry
            # is not the same identity as a fresh run with those knobs set).
            from web.export_state import (
                compute_parallels_search_fingerprint,
                set_parallels_export,
            )
            _restored_fingerprint = compute_parallels_search_fingerprint(
                text=_parallels_search_meta['source_text'],
                engine='history',
                chunk_size=params.get('chunk_size'),
                mode=params.get('mode'),
                filters=params.get('filters'),
                excluded=p_state.excluded_manuscript_ids,
            )
            _parallels_search_meta['search_fingerprint'] = _restored_fingerprint
            p_state.search_fingerprint = _restored_fingerprint
            p_state.searched_source_text = _parallels_search_meta['source_text']
            set_parallels_export(
                results=p_state.results,
                filtered=p_state.filtered_results,
                meta=_parallels_search_meta,
            )

            # Update header and render
            results_header.text = f"{len(p_state.results)} {tr('parallels found')}"
            render_results(p_state.results, p_state.filtered_results)

        comp_history_menu.close()
        if state_snapshot.get('results'):
            ui.notify(tr('Composition restored from history'), type='info', timeout=2000)
            return

        ui.notify(tr('Re-running composition from history'), type='info', timeout=2000)
        await execute_parallels()

    def _reset_parallels():
        """Reset all composition search state, clear results, filters, exclusions, and persistent storage."""
        # Clear source text
        text_input.value = ''
        # Clear results
        p_state.results = []
        p_state.filtered_results = []
        p_state.domain_exclusions = set()
        p_state.excluded_manuscript_ids = set()
        p_state.printed_ids = set()
        p_state.is_running = False
        p_state.is_cancelled = False
        p_state.progress = 0
        p_state.chunks_processed = 0
        p_state.chunks_total = 0
        # Clear pre-search filters
        _clear_all_p_adv_filters()
        # Disable export buttons (no results)
        export_word_btn.props('disable')
        export_excel_btn.props('disable')
        export_json_btn.props('disable')
        # Clear results container
        results_container.clear()
        with results_container:
            with ui.column().classes('w-full h-64 items-center justify-center'):
                ui.icon('compare_arrows').classes('text-6xl').style('color: var(--text-muted);')
                ui.label(tr('Enter text to search for parallels')).classes('mt-4').style('color: var(--text-muted);')
        # Reset results header
        results_header.text = tr('Results')
        # Reset summary label
        summary_label.text = ''
        # Reset library filter state (DMF-09)
        p_state.library_mode = 'hide'
        p_state.library_filter = []
        safe_user_set('parallels_library_filter', {'mode': 'hide', 'codes': []})
        # Reset persistent storage to clean defaults
        safe_user_set('parallels_results', [])
        safe_user_set('parallels_results_fingerprint', '')
        safe_user_set('parallels_search_config', {})
        safe_user_set('parallels_filtered', [])
        safe_user_set('parallels_source_text', '')
        safe_user_set('parallels_domain_exclusions', [])
        safe_user_set('parallels_excluded_manuscript_ids', [])
        # The witness list is part of the composition, so Reset must take it
        # too. It did not: the panel kept up to 25 texts and their row caches
        # while `_clear_active_snapshot()` below deleted the snapshot that
        # held them, so the screen and storage disagreed until the next
        # search. (Review finding.)
        p_state.witnesses = []
        p_state.witness_rows = {}
        p_state.witness_filtered = {}
        p_state.witness_seq = 0
        p_state.checked_for_promotion = set()
        p_state.witness_progress = ''
        p_state.last_passage_ctx = {}
        _refresh_witness_panel()
        _refresh_promotion_bar()
        _clear_active_snapshot()
        # Phase 88: Clear per-session export payload — singleton mirror removed.
        from web.export_state import clear_parallels_export
        clear_parallels_export()
        ui.notify(tr('Composition reset'), type='info', timeout=2000)

    async def execute_parallels():
        # Prevent duplicate executions
        if p_state.is_running:
            return

        # Disable button immediately to prevent rageclicks (re-enabled in finally block of _run_search_wrapper)
        run_btn.disable()

        text = text_input.value or ""
        words = len([w for w in text.split() if w])

        # Allow shorter texts (minimum 3 words instead of 10)
        if words < 3:
            ui.notify(tr('Enter at least 3 words'), type='warning')
            run_btn.enable()
            return

        if not state.lab_engine:
            ui.notify(tr('Lab Engine not initialized'), type='negative')
            run_btn.enable()
            return

        # Update variant level and max changes from UI before search.
        # Round 5 (Codex P2): capture BOTH values here, at dispatch -- the
        # variant controls stay enabled during the await, and a fingerprint
        # reading the live widgets afterwards could describe an edited
        # configuration the engine never used, colliding with a tab that
        # really searched it. The captures below are, by construction, the
        # exact values that initialized the engine.
        captured_variant_level = None
        captured_variant_max_changes = None
        if mode_select.value == 'variants' and state.var_mgr:
            # Get pairs count from preset or slider
            captured_variant_level = (int(variant_slider.value)
                                      if variant_slider
                                      else current_preset['value'])
            state.var_mgr.set_variant_level(captured_variant_level)
            if state.lab_engine and state.lab_engine.settings:
                captured_variant_max_changes = int(max_changes_select.value)
                state.lab_engine.settings.variant_max_changes = (
                    captured_variant_max_changes)

        # Reset state
        p_state.is_running = True
        p_state.is_cancelled = False
        p_state.progress = 0
        p_state.finished_animation_shown = False
        p_state.status = tr('Initializing search...')
        p_state.search_start_time = time.time()
        p_state.chunks_processed = 0
        p_state.chunks_total = 0
        p_state.last_eta_update = 0.0
        p_state.last_eta_text = ""
        p_state.results = []
        p_state.filtered_results = []
        results_container.clear()

        # Show loading spinner in results area - make it prominent so user knows it's working
        with results_container:
            with ui.column().classes('w-full items-center py-12'):
                ui.spinner('bars', size='xl', color='primary').classes('mb-4')
                ui.label(tr('Searching for parallels...')).classes('text-xl font-bold animate-pulse').style('color: var(--primary-600);')
                ui.label(tr('This may take a while...')).classes('text-sm mt-2').style('color: var(--text-muted);')

        # Show immediate feedback in control panel
        ui.notify(tr('Starting search...'), type='info', timeout=2000)
        search_indicator.style('display: flex;')
        progress_bar.style('opacity: 1;')
        progress_bar.set_value(0)
        # Collapse filter panel — chips summarize active filters
        adv_filters_panel.value = False
        # Scroll results area into view
        ui.run_javascript(f'document.getElementById("c{results_container.id}").scrollIntoView({{behavior: "smooth", block: "start"}})')
        status_label.text = tr('Initializing search...')

        # Show top page loading bar during search
        ui.run_javascript('if (window.__showLoadingBar) window.__showLoadingBar();')

        # Clear previous results header and container when starting new search
        results_header.text = tr('Searching...')
        results_container.clear()

        # "Find Parallels" is a FULL FRESH RUN: the seed plus every witness
        # OF THIS TEXT, all reset to pending. The witness LIST survives (a
        # user who pasted seventeen texts must not lose them by pressing the
        # button), but no rows do -- rows found under the previous settings
        # must never be fused with rows found under the new ones.
        #
        # A witness gathered under a DIFFERENT source text is marked stale
        # instead: witnesses belong to the work they were gathered for, and
        # searching Antiochus witnesses against Birkat Hamazon (owner-
        # reported) fuses one work's witnesses into another work's results.
        # Not deleted -- that would throw away seventeen hand-pasted texts on
        # a typo edit; the panel offers both answers explicitly.
        p_state.witness_rows = {}
        p_state.witness_filtered = {}
        p_state.checked_for_promotion = set()
        _digest_now = _seed_digest()
        for _w in p_state.witnesses:
            _stale = _w.get('seed_digest') not in (None, '', _digest_now)
            _w['status'] = 'stale' if _stale else 'pending'
            _w['hits'] = 0
            _w['error'] = ''

        # Capture filter text in main thread to avoid closure issues in background thread
        captured_filter_text = get_filter_text()
        # The restorable PROXY for filter_text: the identity hashes the
        # combined TEXT, but what a reload can re-select is the enabled
        # source refs -- captured at the same instant the text is.
        captured_sefaria_enabled = sorted(filter_sources['enabled'])

        def progress_cb(arg1, arg2=None):
            # Dual-protocol callback, mirroring desktop gui_threads.LabCompositionThread.cb:
            # the core emits BOTH (current:int, total:int) numeric progress AND a single
            # (message:str) text status (genizah_core._execute_batched_search, deep-scan
            # path). A two-required-args signature here crashed lab deep-scan searches
            # with "missing 1 required positional argument: 'total'" (prod 2026-06-12).
            if p_state.is_cancelled:
                raise InterruptedError("Search cancelled")
            if isinstance(arg1, str):
                # Text status — ignore content; the numeric (i, total) call immediately
                # precedes each string call and already drives the web progress UI.
                return
            current, total = arg1, arg2
            if total is not None and total > 0:
                p_state.progress = current / total
                p_state.chunks_processed = current
                p_state.chunks_total = total
                p_state.status = f"{current} / {total}"

        # Capture search mode settings in main thread
        captured_lab_mode = lab_mode.value
        # Phase 145: re-check passage_available() here too (not just at widget
        # build time) -- a request in flight when the passage index becomes
        # unavailable must degrade to "not selected", never crash run_search().
        captured_passage_mode = _letter_level_selected() and passage_available() and not captured_lab_mode
        captured_passage_width = passage_width.value or 'widest-40'
        captured_passage_length = passage_length.value or 'normal'
        captured_passage_depth = passage_depth.value or 'normal'
        # Phase 145 finding #10 (adversarial review): computed ONCE here
        # rather than the 'lab'/'passage'/'chunk' ternary being written twice
        # (PostHog capture + composition-history params) below.
        captured_engine = 'lab' if captured_lab_mode else (
            'passage' if captured_passage_mode else 'chunk'
        )
        # A control the engine never READ is not part of that search's
        # identity (Codex review, 2026-08-24). Both letter-level selects keep
        # their value while hidden in chunk/Lab mode, so a chunk search run
        # after a letter-level one fingerprinted DIFFERENTLY from the same
        # chunk search run before it -- and _apply_restored_search_config
        # re-applies these selects only for engine == 'passage', so the
        # reloaded page could not reproduce that identity and same-search row
        # recovery silently failed.
        #
        # Pinned to their defaults rather than dropped: every chunk
        # fingerprint recorded before this rule already hashed exactly these
        # values (the selects sat at their build defaults), so the fix costs
        # no stored identity. `length` is excluded from the payload at its
        # default, so 'normal' and "absent" are the same hash.
        if captured_engine != 'passage':
            captured_passage_width = 'widest-40'
            captured_passage_length = 'normal'
            captured_passage_depth = 'normal'
        captured_freq_threshold = int(freq_threshold.value) if freq_threshold.value else 50
        captured_deep_scan = deep_scan.value if captured_lab_mode else False
        captured_chunk_size = int(chunk_size.value) if chunk_size.value else 5
        captured_mode = mode_select.value

        # Capture boundary settings (only used in lab mode)
        captured_boundary_mode = boundary_mode.value or 'full'
        captured_boundary_delimiter = boundary_delimiter.value or '\n'
        captured_boundary_boost = float(boundary_boost.value) if boundary_boost.value else 1.5
        captured_min_delimiter_distance = int(min_delimiter_distance.value) if min_delimiter_distance.value else 3
        # For regular (full) mode, use min_chunks_input as the min_boundary_matches value
        # For boundary/combined modes, use the advanced dialog's min_boundary_matches
        captured_min_chunks = int(min_chunks_input.value) if min_chunks_input.value else 1
        if captured_boundary_mode == 'full':
            captured_min_boundary_matches = captured_min_chunks
        else:
            captured_min_boundary_matches = int(min_boundary_matches.value) if min_boundary_matches.value else 0

        # Compute pre-search filter set from active filters
        restrict_sys_ids = None
        if _has_active_filters():
            from shared.fjms_service import get_fjms_service

            include_mode = p_state.filter_include_mode
            _domains = p_state.filter_domains or None
            _authors = p_state.filter_authors or None
            _works = p_state.filter_works or None

            def _compute_restrict():
                fjms = get_fjms_service(thread_safe=True)
                if not fjms.is_available():
                    return None
                kwargs = dict(
                    date_from=p_state.filter_date_from,
                    date_to=p_state.filter_date_to,
                    material_exclude=p_state.filter_material_exclude or None,
                    text_all=p_state.filter_text_all or None,
                    text_any=p_state.filter_text_any or None,
                    text_not=p_state.filter_text_not or None,
                )
                if include_mode:
                    kwargs['domains'] = _domains
                    kwargs['authors'] = _authors
                    kwargs['works'] = _works
                else:
                    kwargs['domains_exclude'] = _domains
                    kwargs['authors_exclude'] = _authors
                    kwargs['works_exclude'] = _works
                return fjms.get_filter_sys_ids(**kwargs)

            restrict_sys_ids = await run.io_bound(_compute_restrict)
            p_state.restrict_sys_ids = restrict_sys_ids
            # If filters are active but match nothing, show message and return
            if restrict_sys_ids is not None and len(restrict_sys_ids) == 0:
                ui.notify(tr("No manuscripts match the current filters."), type='warning')
                p_state.is_running = False
                search_indicator.style('display: none;')
                progress_bar.style('opacity: 0;')
                ui.run_javascript('if (window.__hideLoadingBar) window.__hideLoadingBar();')
                return

        # DMF-09 HYBRID Show-only library pre-query intersect (Phase 131-05 / Codex R3 F4).
        # MUST be OUTSIDE / AFTER the `_has_active_filters()` block above — that gate is False
        # when ONLY a library filter is set, so gating the resolve inside it would make a
        # library-only Show-only never scope.  Must also run BEFORE the per-manuscript
        # exclusion subtraction below so library-only AND advanced+library cases both compose.
        if p_state.library_mode == 'show_only' and p_state.library_filter:
            from shared.fjms_service import resolve_library_sys_ids as _resolve_lib_ids
            lib_ids = await run.io_bound(
                _resolve_lib_ids, list(p_state.library_filter), state.meta_mgr
            )
            if lib_ids:  # fail-open: skip intersect if resolution returned empty
                restrict_sys_ids = lib_ids if restrict_sys_ids is None else (restrict_sys_ids & lib_ids)

        # Merge per-manuscript exclusions into restrict_sys_ids if both are present
        if p_state.excluded_manuscript_ids and restrict_sys_ids is not None:
            restrict_sys_ids = restrict_sys_ids - p_state.excluded_manuscript_ids

        # Capture restrict_sys_ids for the background thread
        captured_restrict_sys_ids = restrict_sys_ids
        # Workflow review: the library scope and the per-manuscript
        # exclusions were the last inputs still read LIVE from p_state after
        # the await. Capture them here with everything else -- the search
        # that ran used these values, and the post-search 'hide' pass must
        # filter by the same ones it is fingerprinted with.
        captured_library_mode = p_state.library_mode
        captured_library_filter = list(p_state.library_filter or [])
        captured_excluded_ids = set(p_state.excluded_manuscript_ids or ())

        # The settings a witness added AFTER this search must be searched
        # with. A witness run at a different width or depth than the rows
        # beside it would be fused into one list with them and be invisible
        # as an anomaly -- the numbers would simply be wrong, quietly.
        # (There is deliberately no dispatch-time witness capture. The export
        # manifest is built AFTER the run from `witness_rows`, by
        # `_refresh_export_payload`, so it can only name witnesses that
        # actually produced rows; and the search identity re-derives its own
        # via `_recompute_search_identity` for the same reason.)

        p_state.last_passage_ctx = {
            # The seed digest of THIS search, captured at dispatch (`text` is
            # read before the awaits above). A witness promoted from these
            # rows belongs to this text, not to whatever the box holds by the
            # time the user clicks.
            'seed_digest': _text_digest_of(text),
            'width': captured_passage_width,
            'length': captured_passage_length,
            'depth': captured_passage_depth,
            'filter_text': captured_filter_text,
            'min_boundary_matches': captured_min_boundary_matches,
            'restrict_sys_ids': captured_restrict_sys_ids,
            # The POST-search passes the seed's rows go through. A witness's
            # rows must go through the same ones, or "hide this library"
            # would silently stop applying to everything found after it was
            # set -- and the user would have no way to tell.
            'library_mode': captured_library_mode,
            'library_filter': captured_library_filter,
        } if captured_passage_mode else {}

        def run_search():
            try:
                if captured_lab_mode:
                    # LAB MODE: Use fingerprint-based search with advanced features
                    result = state.lab_engine.lab_composition_search(
                        text,
                        mode=captured_mode,
                        progress_callback=progress_cb,
                        chunk_size=captured_chunk_size,
                        filter_text=captured_filter_text or None,
                        deep_scan=captured_deep_scan,
                        boundary_mode=captured_boundary_mode,
                        boundary_delimiter=captured_boundary_delimiter,
                        boundary_boost=captured_boundary_boost,
                        min_boundary_matches=captured_min_boundary_matches,
                        min_delimiter_distance=captured_min_delimiter_distance
                    )
                else:
                    # STANDARD MODE: Use direct Tantivy search (faster, simpler)
                    result = state.searcher.search_composition_logic(
                        text,
                        chunk_size=captured_chunk_size,
                        max_freq=captured_freq_threshold,
                        mode=captured_mode,
                        filter_text=captured_filter_text or None,
                        progress_callback=progress_cb,
                        boundary_mode=captured_boundary_mode,
                        boundary_delimiter=captured_boundary_delimiter,
                        boundary_boost=captured_boundary_boost,
                        min_boundary_matches=captured_min_boundary_matches,
                        min_delimiter_distance=captured_min_delimiter_distance,
                        restrict_sys_ids=captured_restrict_sys_ids,
                    )
                return result
            except InterruptedError:
                # Search was cancelled -- return None; partial results handled by core functions
                # that catch InterruptedError internally and return accumulated results
                return None
            except Exception as e:
                logger.exception(f"Parallels Error: {e}")
                return None

        def _run_passage_search_sync():
            """PASSAGE MATCHING (Phase 145, beta): character-level engine,
            tolerant of OCR noise / reflowed line breaks. Constructed fresh
            here (cheap -- no I/O, the index is already open) and never when
            the index is unavailable, per
            web/passage_assets.py::get_passage_searcher's contract.

            Deliberately NOT wrapped in its own try/except -- run_search()
            above swallows InterruptedError/Exception itself because it
            dispatches via run.io_bound with no other error channel; this
            function instead lets exceptions propagate through
            run_passage_search so the await site below (still on THIS
            coroutine, not a background thread) can distinguish a budget
            APIError (busy/timeout -> a translated notification) from any
            other failure.
            """
            # render_cap=0 -> UNCAPPED (owner ruling 2026-08-23): the page's
            # own "Load more" batching (50 groups per click, strongest first)
            # is the pager, and set_parallels_export's 5,000-row bound is the
            # export ceiling -- so the engine's 200-group cap was hiding found
            # manuscripts from both surfaces (measured: 198 shown of 497
            # found on Birkat Hamazon).
            passage_searcher = get_passage_searcher(
                state.searcher, preset=captured_passage_width,
                length=captured_passage_length,
                depth=captured_passage_depth, render_cap=0)
            if passage_searcher is None:
                return None
            return passage_searcher.search_composition_logic(
                text,
                chunk_size=captured_chunk_size,
                max_freq=captured_freq_threshold,
                mode=captured_mode,
                filter_text=captured_filter_text or None,
                progress_callback=progress_cb,
                boundary_mode=captured_boundary_mode,
                boundary_delimiter=captured_boundary_delimiter,
                boundary_boost=captured_boundary_boost,
                min_boundary_matches=captured_min_boundary_matches,
                min_delimiter_distance=captured_min_delimiter_distance,
                restrict_sys_ids=captured_restrict_sys_ids,
            )

        if captured_passage_mode:
            # Codex review finding #15: route through the SAME bounded
            # execution budget POST /api/parallels uses for method='passage'
            # (semaphore capacity 4 + its own dedicated ThreadPoolExecutor +
            # SEARCH_API_PASSAGE_TIMEOUT) -- never run.io_bound's generic,
            # unbounded pool. run_passage_search is awaited directly from
            # THIS coroutine (it manages its own off-loop dispatch via
            # run_in_executor internally); all ui.*/safe_user_* interaction
            # stays on THIS side of the await (repo memory: NiceGUI
            # background execution loses context -- ui.* calls from a raw
            # executor thread RAISE, and safe_user_* reads silently degrade
            # to {} -- _run_passage_search_sync itself does neither).
            try:
                result_data = await run_passage_search(_run_passage_search_sync)
            except APIError as exc:
                if exc.code == 'passage_search_busy':
                    ui.notify(
                        tr('Letter-level search is busy right now — please try again in a moment.'),
                        type='warning',
                    )
                elif exc.code == 'core_timeout':
                    ui.notify(
                        tr('Letter-level search timed out — try a shorter text.'),
                        type='negative',
                    )
                else:
                    ui.notify(tr('Letter-level search failed.'), type='negative')
                result_data = None
            except Exception as e:
                logger.exception(f"Parallels Error (passage): {e}")
                result_data = None
        else:
            result_data = await run.io_bound(run_search)

        p_state.is_running = False
        p_state.progress = 1.0

        # Compute total elapsed time for summary
        total_elapsed = time.time() - p_state.search_start_time if p_state.search_start_time else 0
        if total_elapsed >= 3600:
            total_elapsed_str = f"{int(total_elapsed // 3600)}:{int((total_elapsed % 3600) // 60):02d}:{int(total_elapsed % 60):02d}"
        else:
            total_elapsed_str = f"{int(total_elapsed // 60)}:{int(total_elapsed % 60):02d}"

        # Hide top page loading bar
        ui.run_javascript('if (window.__hideLoadingBar) window.__hideLoadingBar();')

        # Hide the search indicator animation (but keep status label visible for summary)
        search_indicator.style('display: none;')

        if result_data:
            main_results = result_data.get('main', [])
            filtered_results = result_data.get('filtered', [])
            is_partial = result_data.get('partial', False)

            # PR #324 round 4: a capped passage search must say so HERE too.
            # The API path warns (`passage_results_truncated`), and this
            # direct page path was the one product caller still discarding
            # `query_report` -- so a GUI user could mistake capped results
            # for exhaustive ones. Only the two states that truncate the
            # RESULT SET notify; postings exclusion is routine budget
            # behaviour on long queries and would make the notice fire on
            # nearly every request. (duplicate_photography_demoted rows need
            # no notify: they are visible in the filtered section itself.)
            _qrep = result_data.get('query_report') or {}
            if _qrep.get('candidates_truncated') or _qrep.get('verify_truncated'):
                # Info, not warning, and it says what actually happened
                # (owner ruling 2026-08-23). The first wording -- "results
                # may be incomplete" -- fired on virtually every common-
                # phrase query, and the Dror Yikra measurement showed a
                # firing where uncapped verification of all 26,164
                # candidates changed NOTHING: candidates are verified
                # strongest-evidence-first, so what the cap skips is the
                # weakest tail. An alarm that cries wolf on famous piyyutim
                # teaches users to distrust the engine (the first real user
                # hit exactly this and reported it as a bug).
                ui.notify(
                    tr('Letter-level search checked the {n} best-evidenced '
                       'candidates of {m}.').format(
                        n=f"{_qrep.get('verified', 0):,}",
                        m=f"{_qrep.get('candidates', 0):,}"),
                    type='info',
                )
            # PR #324 round 5: the searcher's group-cap flag used to be
            # discarded entirely, so a >200-manuscript passage query looked
            # complete. Same info-not-alarm register as above.
            if result_data.get('truncated_to_200'):
                from shared.parallels_service import PARALLELS_GROUP_CAP
                ui.notify(
                    tr('Letter-level search matched more than {cap} manuscripts '
                       '— showing the strongest {cap}.').format(
                        cap=PARALLELS_GROUP_CAP),
                    type='info',
                )

            # Built from DISPATCH-TIME captures, so it describes the search
            # that RAN -- which is why it sits outside the result guard below
            # rather than inside it. Nested there, a seed that matched nothing
            # left `last_export_meta` and `last_fingerprint_kwargs` unset (or,
            # worse, holding the PREVIOUS search's), and a witness run that
            # then found rows published them under the wrong identity or not
            # at all. Every statement in here is empty-list-safe.
            try:
                # Phase 88: build per-session export payload for /api/export/parallels/*
                # (singleton mirror removed). Variable provenance (verified live in
                # web/pages/parallels.py):
                #   captured_chunk_size      — int(chunk_size.value) or 5
                #   captured_freq_threshold  — int(freq_threshold.value) or 50
                #   captured_mode            — mode_select.value
                #   text_input.value         — NiceGUI textarea, source text
                # HIGH-02 fix (historical Phase 77): capture the active filter dict (same
                # 10-key shape as the live snapshot at parallels.py:2202-2213) so envelope
                # replay matches what history-restore reconstructs.
                _parallels_filters = {
                    'domains': list(getattr(p_state, 'filter_domains', None) or []),
                    'authors': list(getattr(p_state, 'filter_authors', None) or []),
                    'works': list(getattr(p_state, 'filter_works', None) or []),
                    'include_mode': getattr(p_state, 'filter_include_mode', True),
                    'date_from': getattr(p_state, 'filter_date_from', None),
                    'date_to': getattr(p_state, 'filter_date_to', None),
                    'material_exclude': list(getattr(p_state, 'filter_material_exclude', None) or []),
                    'text_all': list(getattr(p_state, 'filter_text_all', None) or []),
                    'text_any': list(getattr(p_state, 'filter_text_any', None) or []),
                    'text_not': list(getattr(p_state, 'filter_text_not', None) or []),
                } if _has_active_filters() else None
                # Search identity: ONE definition, in export_state --
                # executable by tests, canonicalizing its own set-like
                # inputs (PR #325 rounds 2-5, plus the workflow review
                # that found the same rule being rebuilt, differently, by
                # the history-restore path). Every value passed here is a
                # DISPATCH-TIME capture: a live read describes a
                # configuration the search may not have used.
                from web.export_state import (
                    compute_parallels_search_fingerprint,
                )
                _fingerprint_kwargs = dict(
                    text=text,
                    engine=captured_engine,
                    width=captured_passage_width,
                    length=captured_passage_length,
                    depth=captured_passage_depth,
                    chunk_size=captured_chunk_size,
                    mode=captured_mode,
                    max_freq=captured_freq_threshold,
                    filter_text=captured_filter_text or '',
                    deep_scan=captured_deep_scan,
                    boundary_mode=captured_boundary_mode,
                    boundary_delimiter=captured_boundary_delimiter,
                    boundary_boost=captured_boundary_boost,
                    min_boundary_matches=captured_min_boundary_matches,
                    min_delimiter_distance=captured_min_delimiter_distance,
                    variant_level=captured_variant_level,
                    variant_max_changes=captured_variant_max_changes,
                    # The library 'hide' pass below reads these same
                    # captures, so the identity and the filtering that
                    # shaped the rows cannot disagree.
                    library_mode=captured_library_mode,
                    library_filter=captured_library_filter,
                    # Pre-query scope: advanced filters + show_only
                    # libraries + per-manuscript exclusions, merged into
                    # one set at dispatch.
                    restrict=captured_restrict_sys_ids,
                    excluded=captured_excluded_ids,
                    filters=_parallels_filters,
                )
                # The witness set is part of the identity, but at THIS
                # instant none has been searched -- these rows really are
                # seed-only. `_recompute_search_identity` re-runs this
                # exact keyword capture with the witnesses that produced
                # rows, as soon as any has. One construction, reused, so
                # the two cannot describe different searches.
                p_state.last_fingerprint_kwargs = _fingerprint_kwargs
                _search_fingerprint = compute_parallels_search_fingerprint(
                    **_fingerprint_kwargs)
                p_state.search_fingerprint = _search_fingerprint
                p_state.searched_source_text = text
                # The CONFIGURATION that produced these rows, from the
                # SAME dispatch-time captures the fingerprint hashes --
                # one list of "what defines a search", so the restored
                # controls and the stored identity cannot disagree.
                # Persisted by _persist_active_snapshot and re-applied to
                # the widgets by _apply_restored_search_config on reload
                # (docs/OPEN_ISSUES.md: reload restored the rows but left
                # the controls at build-time defaults, so the restore
                # notice pointed at a DIFFERENT search).
                p_state.searched_config = {
                    'engine': captured_engine,
                    'width': captured_passage_width,
                    'length': captured_passage_length,
                    'depth': captured_passage_depth,
                    'chunk_size': captured_chunk_size,
                    'mode': captured_mode,
                    'max_freq': captured_freq_threshold,
                    'deep_scan': captured_deep_scan,
                    'boundary_mode': captured_boundary_mode,
                    'boundary_delimiter': captured_boundary_delimiter,
                    'boundary_boost': captured_boundary_boost,
                    'min_boundary_matches': captured_min_boundary_matches,
                    'min_delimiter_distance': captured_min_delimiter_distance,
                    'variant_level': captured_variant_level,
                    'variant_max_changes': captured_variant_max_changes,
                    'filters': _parallels_filters,
                    'library_mode': captured_library_mode,
                    'library_filter': captured_library_filter,
                    'sefaria_enabled': captured_sefaria_enabled,
                }
                _parallels_search_meta = {
                    'source_text': text,
                    'search_fingerprint': _search_fingerprint,
                    # What the workbook was searched WITH. Labels, kinds
                    # and shelfmarks only -- never the texts: a
                    # downloaded file that carried twenty-five 20,000-
                    # character witnesses would be mostly query.
                    # None at dispatch, ALWAYS: at this instant no witness
                    # has contributed a row, so these results really are
                    # seed-only -- the same reasoning the fingerprint capture
                    # above states for itself. Naming them here made the
                    # Word/XLSX exports claim "Searched with" witnesses that
                    # never ran, and add multi-witness columns to seed-only
                    # rows, whenever dispatch returned early: a new seed marks
                    # the existing witnesses `stale` so nothing is pending, and
                    # the depth-cap refusal does the same. Neither path reaches
                    # `_refresh_export_payload`, which is what publishes the
                    # real manifest -- derived from `witness_rows`, so it names
                    # only witnesses that actually produced rows.
                    'witnesses': None,
                    'chunk_size': captured_chunk_size,
                    'mode': captured_mode,
                    'max_freq': float(captured_freq_threshold) if captured_freq_threshold is not None else None,
                    'filters': _parallels_filters,
                    'boundary_options': None,  # Phase 77: not yet exposed as user-settable; placeholder for parity with /api/parallels API-02
                    'warnings': [],  # Phase 78 will populate
                }
                # DMF-09 HYBRID Hide post-fetch filter (Phase 131-05 / Codex MED #6).
                # Applied BEFORE set_parallels_export / safe_user_set so exports + stored
                # payloads are scoped.  Show-only is already scoped pre-query (restrict_sys_ids)
                # so no post-fetch pass needed for Show-only.
                if captured_library_mode == 'hide' and captured_library_filter:
                    main_results = _apply_parallels_library_filter(
                        main_results, captured_library_mode,
                        captured_library_filter)
                    if filtered_results:
                        filtered_results = _apply_parallels_library_filter(
                            filtered_results, captured_library_mode,
                            captured_library_filter)
                from web.export_state import (
                    compact_parallels_result_rows,
                    set_parallels_export,
                )
                p_state.last_export_meta = dict(_parallels_search_meta)
                set_parallels_export(
                    results=main_results,
                    filtered=filtered_results,
                    meta=_parallels_search_meta,
                )
                main_results = compact_parallels_result_rows(main_results)
                filtered_results = compact_parallels_result_rows(filtered_results)
                p_state.results = main_results
                p_state.filtered_results = filtered_results
                # Also store in user storage (for UI persistence across page reloads)
                safe_user_set('parallels_results', _compact_result_rows(
                    main_results[:_PARALLELS_ACTIVE_USER_FALLBACK_LIMIT]
                ))
                # Round 6: stamp the fallback's identity beside it. The
                # legacy bootstrap folds it into its meta, so the
                # mixed-pair rule in _same_parallels_search can VERIFY
                # same-search instead of trusting source_text.
                safe_user_set('parallels_results_fingerprint',
                              _search_fingerprint)
                # Workflow review W4: mirror the config beside the rows
                # and identity, so the legacy bootstrap restores controls
                # too, not only in the tab that searched.
                safe_user_set('parallels_search_config',
                              dict(p_state.searched_config))
                safe_user_set('parallels_filtered', _compact_result_rows(
                    (filtered_results or [])[:_PARALLELS_ACTIVE_USER_FALLBACK_LIMIT]
                ))
                _persist_active_snapshot()
            except Exception:
                pass  # Browser storage operation failed; preference not persisted

            if main_results or filtered_results:
                p_state.results = main_results
                p_state.filtered_results = filtered_results

                # PostHog: track parallels search
                from web.analytics import posthog_capture
                posthog_capture('parallels_search', {
                    'text_length': len(text),
                    'word_count': words,
                    'result_count': len(main_results),
                    'filtered_count': len(filtered_results),
                    'duration_seconds': round(total_elapsed, 1),
                    'is_partial': is_partial,
                    'mode': captured_mode,
                    # Phase 145: which backend actually served this search --
                    # 'lab' / 'passage' / 'chunk' (the pre-existing default).
                    'engine': captured_engine,
                })


                # Add to composition history
                try:
                    source_text = text_input.value or ''
                    comp_title = source_text[:50].replace('\n', ' ').strip()
                    if len(source_text) > 50:
                        comp_title += '...'
                    _add_to_comp_history(
                        title=comp_title,
                        result_count=len(main_results),
                        params={
                            'chunk_size': int(chunk_size.value) if chunk_size.value else 5,
                            'mode': mode_select.value or 'exact',
                            # Phase 145: record-only (history restore does not
                            # re-select any engine toggle today -- lab_mode
                            # included -- so this is observability, not a
                            # restore contract).
                            'engine': captured_engine,
                            'filters': {
                                'domains': p_state.filter_domains,
                                'authors': p_state.filter_authors,
                                'works': p_state.filter_works,
                                'include_mode': p_state.filter_include_mode,
                                'date_from': p_state.filter_date_from,
                                'date_to': p_state.filter_date_to,
                                'material_exclude': p_state.filter_material_exclude,
                                'text_all': p_state.filter_text_all,
                                'text_any': p_state.filter_text_any,
                                'text_not': p_state.filter_text_not,
                            } if _has_active_filters() else None,
                        },
                        state_snapshot={
                            'source_text': source_text,
                            'domain_exclusions': sorted(p_state.domain_exclusions),
                            'excluded_manuscript_ids': sorted(p_state.excluded_manuscript_ids),
                        },
                    )
                except Exception:
                    pass  # Filter operation failed; continue with defaults

                # Collect domain data for parallels results
                all_sys_ids = []
                for item in main_results:
                    raw_header = item.get('raw_header', '')
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        all_sys_ids.append(sys_match.group(1))

                if all_sys_ids:
                    def collect_parallels_domains(sys_ids):
                        from shared.fjms_service import get_fjms_service
                        fjms = get_fjms_service(thread_safe=True)
                        return fjms.get_domains_for_sys_ids(sys_ids) if fjms.is_available() else {}

                    def collect_parallels_printed(sys_ids):
                        from shared.fjms_service import get_fjms_service
                        fjms = get_fjms_service(thread_safe=True)
                        return fjms.get_printed_sys_ids(sys_ids) if fjms.is_available() else set()

                    # Read show_translations in main thread before entering thread pool
                    _par_show_trans = safe_user_get('show_translations', False)

                    def collect_parallels_translations(sys_ids, show_trans=False):
                        """Batch-fetch title and PGP translations for parallels results (Phase 46-07)."""
                        try:
                            from shared.translation_service import TranslationService
                            svc = TranslationService(thread_safe=True)
                            # Title translations always fetched (language-aware)
                            title_trans = svc.get_title_translations_batch(sys_ids) if svc.titles_available() else {}
                            # PGP translations only when toggle is ON
                            pgp_trans = svc.get_pgp_translations_by_sys_ids(sys_ids) if show_trans and svc.pgp_available() else {}
                            svc.close()
                            return title_trans, pgp_trans
                        except Exception as e:
                            logger.warning("Parallels translation batch lookup failed: %s", e)
                            return {}, {}

                    import asyncio as _asyncio
                    raw_domains, printed_result, trans_tuple = await _asyncio.gather(
                        run.io_bound(collect_parallels_domains, all_sys_ids),
                        run.io_bound(collect_parallels_printed, all_sys_ids),
                        run.io_bound(collect_parallels_translations, all_sys_ids, _par_show_trans),
                    )
                    p_state.printed_ids = printed_result
                    p_state.title_translations, p_state.translation_data = trans_tuple
                    p_state.all_result_domains = {}
                    p_state.domain_name_map = {}
                    from shared.fjms_service import qualify_domain_name
                    for sys_id, doms in raw_domains.items():
                        child_names = {d['domain'] for d in doms}
                        filtered_doms = [qualify_domain_name(d['domain'], d.get('parent_domain')) for d in doms if not (d.get('parent_domain') and d['parent_domain'] in child_names and d['parent_domain'] != d['domain'])]
                        if filtered_doms:
                            p_state.all_result_domains[sys_id] = filtered_doms
                        for d in doms:
                            qname = qualify_domain_name(d['domain'], d.get('parent_domain'))
                            if qname != d['domain'] and d.get('domain_heb') and d.get('parent_domain_heb'):
                                p_state.domain_name_map[qname] = f"{d['domain_heb']} ({d['parent_domain_heb']})"
                            if d.get('domain_heb') and d['domain'] not in p_state.domain_name_map:
                                p_state.domain_name_map[d['domain']] = d['domain_heb']
                            if d.get('parent_domain_heb') and d.get('parent_domain') and d['parent_domain'] not in p_state.domain_name_map:
                                p_state.domain_name_map[d['parent_domain']] = d['parent_domain_heb']
                    p_state.has_domain_data = bool(p_state.all_result_domains)

                    # Pre-cache domain hierarchy for filter dialog
                    if p_state.has_domain_data:
                        def fetch_parallels_hierarchy():
                            from shared.fjms_service import get_fjms_service
                            fjms_h = get_fjms_service(thread_safe=True)
                            return fjms_h.get_domain_hierarchy() if fjms_h.is_available() else {}
                        p_state.domain_hierarchy = await run.io_bound(fetch_parallels_hierarchy)
                    else:
                        p_state.domain_hierarchy = {}
                else:
                    p_state.all_result_domains = {}
                    p_state.has_domain_data = False
                    p_state.domain_hierarchy = {}
                    p_state.printed_ids = set()
                    p_state.title_translations = {}
                    p_state.translation_data = {}

                # Show/hide domain filter button
                p_domain_filter_btn.set_visibility(p_state.has_domain_data)
                _update_parallels_domain_filter_btn()

                # DMF-09: show library filter button whenever there are results (Phase 131-05)
                parallels_library_filter_btn.set_visibility(bool(main_results or filtered_results))
                _update_parallels_library_filter_btn()

                # Build filter summary suffix for status line
                _filter_suffix = ''
                if _has_active_filters() and p_state.filter_manuscript_count is not None:
                    filter_parts = []
                    opts_d = p_domain_select.options if hasattr(p_domain_select, 'options') else {}
                    opts_a = p_author_select.options if hasattr(p_author_select, 'options') else {}
                    opts_w = p_work_select.options if hasattr(p_work_select, 'options') else {}
                    for d in p_state.filter_domains:
                        filter_parts.append(_get_p_display_name(d, opts_d))
                    for a in p_state.filter_authors:
                        filter_parts.append(_get_p_display_name(a, opts_a))
                    for w in p_state.filter_works:
                        filter_parts.append(_get_p_display_name(w, opts_w))
                    if filter_parts:
                        _filter_suffix = f" ({tr('filtered')}: {', '.join(filter_parts)}, {p_state.filter_manuscript_count:,} {tr('manuscripts')})"
                    else:
                        _filter_suffix = f" ({tr('filtered')}: {p_state.filter_manuscript_count:,} {tr('manuscripts')})"

                # Show message if results are partial (search was cancelled)
                if is_partial:
                    chunks_done = p_state.chunks_processed
                    chunks_all = p_state.chunks_total
                    result_count = len(main_results) + len(filtered_results)
                    summary_label.text = f"{tr('Partial results')} \u2014 {total_elapsed_str} \u2014 {chunks_done}/{chunks_all} {tr('chunks')}, {result_count} {tr('Results')}{_filter_suffix}"
                    ui.notify(tr('Showing partial results'), type='warning', timeout=3000)
                else:
                    # Set summary line that stays visible until next search
                    chunks_all = p_state.chunks_total
                    result_count = len(main_results) + len(filtered_results)
                    summary_label.text = f"{tr('Search completed in')} {total_elapsed_str} \u2014 {chunks_all} {tr('chunks')}, {result_count} {tr('Results')}{_filter_suffix}"

                # Apply domain exclusions if any
                if p_state.domain_exclusions and p_state.has_domain_data:
                    main_results = _filter_parallels_by_domain(main_results)
                    filtered_results = _filter_parallels_by_domain(filtered_results) if filtered_results else filtered_results

                render_results(main_results, filtered_results, is_partial=is_partial)
            else:
                if is_partial:
                    summary_label.text = f"{tr('Search cancelled')} \u2014 {total_elapsed_str} \u2014 {tr('no results yet')}"
                results_header.text = tr('No results')
                with results_container:
                    show_empty_state()

            # The seed is a witness too -- modelled with its own id so
            # "found by 3 of 5" needs no +1 special case anywhere. Only
            # letter-level: the panel is hidden for chunk and Lab, and
            # fusing rows from an engine whose score means something else
            # would be meaningless.
            #
            # OUTSIDE the result guard above, and that is the whole point.
            # Nested inside it, a seed that matched nothing left every
            # witness unsearched and the page said "No results" -- in a
            # feature that exists precisely because no single witness of a
            # work retrieves what the others do. The measured case is not
            # exotic: one BH witness reaches 56.7% of the census where the
            # fused seventeen reach 74.1%, so a seed landing on the empty
            # side of that gap is ordinary, not pathological.
            #
            # An empty seed is stored as an empty SEARCHED witness, never
            # skipped: a seed missing from `witness_rows` also drops out of
            # `_searched_witness_count()`, which would hide the fusion sort
            # options and, with exactly one other witness, turn the fusion
            # into a single-witness passthrough.
            if captured_passage_mode:
                p_state.witness_rows[WITNESS_SEED_ID] = list(main_results)
                p_state.witness_filtered[WITNESS_SEED_ID] = list(
                    filtered_results or [])
                _refresh_witness_panel()
                if any(_w['status'] == 'pending'
                       for _w in p_state.witnesses):
                    await _search_pending_witnesses()
        else:
            results_header.text = tr('No results')
            with results_container:
                show_empty_state()

    def _update_parallels_domain_filter_btn():
        """Update parallels domain filter button text and styling."""
        if p_state.domain_exclusions:
            n = len(p_state.domain_exclusions)
            p_domain_filter_btn.text = f"{tr('Filter by domains')} ({n} {tr('excluded')})"
            p_domain_filter_btn.props('outline dense no-caps color=red')
        else:
            p_domain_filter_btn.text = tr('Filter by domains')
            p_domain_filter_btn.props('outline dense no-caps color=primary')

    def _get_sys_id_from_parallels_item(item):
        """Extract sys_id from a parallels result item."""
        raw_header = item.get('raw_header', '')
        sys_match = re.search(r'(99\d{8,})', raw_header)
        return sys_match.group(1) if sys_match else None

    def _filter_parallels_by_domain(results):
        """Filter parallels results based on domain exclusions."""
        if not p_state.domain_exclusions:
            return results
        hide_uncategorized = 'Uncategorized' in p_state.domain_exclusions
        filtered = []
        for item in results:
            sys_id = _get_sys_id_from_parallels_item(item)
            result_domains = p_state.all_result_domains.get(sys_id, []) if sys_id else []
            if not result_domains:
                if not hide_uncategorized:
                    filtered.append(item)
                continue
            elif all(d in p_state.domain_exclusions for d in result_domains):
                continue
            else:
                filtered.append(item)
        return filtered

    def _parallels_domain_display(en_name: str) -> str:
        """Get display name for a domain (Hebrew if UI is Hebrew, else English)."""
        from web.translations import get_language
        if get_language() == 'he':
            if en_name in p_state.domain_name_map:
                return p_state.domain_name_map[en_name]
            translated = tr(en_name)
            if translated != en_name:
                return translated
        return en_name

    def _open_parallels_domain_filter_dialog():
        """Open modal dialog with domain filter checkboxes for parallels results.

        Uses a single HTML container with client-side JavaScript for checkbox
        interactions to avoid the overhead of creating ~200 individual NiceGUI
        ui.checkbox elements.
        """
        if not p_state.has_domain_data:
            if p_state.domain_exclusions:
                ui.notify(tr('Run a search first to see domain options.'), type='info', timeout=3000)
            return

        # Use pre-cached hierarchy -- no DB call
        hierarchy = p_state.domain_hierarchy
        if not hierarchy:
            from shared.fjms_service import get_fjms_service
            fjms = get_fjms_service(thread_safe=True)
            hierarchy = fjms.get_domain_hierarchy() if fjms.is_available() else {}
            p_state.domain_hierarchy = hierarchy

        # Count results per domain
        domain_counts = {}
        for sys_id, domain_names in p_state.all_result_domains.items():
            for d in domain_names:
                domain_counts[d] = domain_counts.get(d, 0) + 1

        # Build filtered hierarchy
        from shared.fjms_service import qualify_domain_name, AMBIGUOUS_CHILD_DOMAINS
        result_hierarchy = {}
        for parent_name, info in hierarchy.items():
            parent_in_results = parent_name in domain_counts
            children_in_results = []
            for child in info.get('children', []):
                qname = qualify_domain_name(child['domain'], parent_name)
                if qname in domain_counts:
                    children_in_results.append({
                        'domain': qname,
                        'domain_heb': child.get('domain_heb', child['domain']),
                        'count': domain_counts[qname],
                    })
                elif child['domain'] in domain_counts and child['domain'] not in AMBIGUOUS_CHILD_DOMAINS:
                    children_in_results.append({
                        'domain': child['domain'],
                        'domain_heb': child.get('domain_heb', child['domain']),
                        'count': domain_counts[child['domain']],
                    })
            if parent_in_results or children_in_results:
                parent_count = domain_counts.get(parent_name, 0)
                if children_in_results and parent_count == 0:
                    parent_count = sum(c['count'] for c in children_in_results)
                result_hierarchy[parent_name] = {
                    'parent_domain_heb': info.get('parent_domain_heb', parent_name),
                    'count': parent_count,
                    'children': children_in_results,
                }

        # Orphans
        known_domains = set()
        for parent_name, info in result_hierarchy.items():
            known_domains.add(parent_name)
            for c in info['children']:
                known_domains.add(c['domain'])
        for domain_name, count in domain_counts.items():
            if domain_name not in known_domains:
                result_hierarchy[domain_name] = {
                    'parent_domain_heb': domain_name,
                    'count': count,
                    'children': [],
                }

        # Uncategorized
        all_sys_ids_in_results = set()
        for item in p_state.results:
            sid = _get_sys_id_from_parallels_item(item)
            if sid:
                all_sys_ids_in_results.add(sid)
        uncategorized_count = sum(1 for sid in all_sys_ids_in_results if sid not in p_state.all_result_domains)
        if uncategorized_count > 0:
            result_hierarchy['Uncategorized'] = {
                'parent_domain_heb': tr('Uncategorized'),
                'count': uncategorized_count,
                'children': [],
            }

        total_results = len(p_state.results)
        current_exclusions = p_state.domain_exclusions.copy()

        # Build checkbox HTML -- all checkboxes as a single HTML string
        # Use unique container ID to avoid conflicts with stale dialog DOM nodes
        import json as _json
        import uuid as _uuid
        container_id = f'domain-filter-{_uuid.uuid4().hex[:8]}'
        checkbox_html_parts = []
        for parent_name, info in sorted(result_hierarchy.items(), key=lambda x: -x[1]['count']):
            children = info.get('children', [])
            parent_checked = 'checked' if parent_name not in current_exclusions else ''
            parent_label = f"{_parallels_domain_display(parent_name)} ({info['count']})"
            child_domain_names = [c['domain'] for c in children]
            parent_domain_attr = html.escape(parent_name, quote=True)
            children_json_attr = html.escape(_json.dumps(child_domain_names), quote=True)
            parent_label_html = html.escape(parent_label)
            checkbox_html_parts.append(
                f'<label class="domain-parent" style="display:flex;align-items:center;gap:6px;'
                f'font-weight:bold;padding:4px 0;cursor:pointer">'
                f'<input type="checkbox" data-domain="{parent_domain_attr}" '
                f'data-children="{children_json_attr}" '
                f'{parent_checked} onchange="domainFilterParentChanged(this)" '
                f'style="width:18px;height:18px;accent-color:#1976d2">'
                f'<span>{parent_label_html}</span></label>'
            )
            for child in sorted(children, key=lambda c: -c['count']):
                child_checked = 'checked' if child['domain'] not in current_exclusions else ''
                child_label = f"{_parallels_domain_display(child['domain'])} ({child['count']})"
                child_domain_attr = html.escape(child['domain'], quote=True)
                child_label_html = html.escape(child_label)
                checkbox_html_parts.append(
                    f'<label class="domain-child" style="display:flex;align-items:center;gap:6px;'
                    f'padding:2px 0;padding-inline-start:2rem;cursor:pointer">'
                    f'<input type="checkbox" data-domain="{child_domain_attr}" '
                    f'{child_checked} '
                    f'style="width:16px;height:16px;accent-color:#1976d2">'
                    f'<span>{child_label_html}</span></label>'
                )

        checkbox_html = '\n'.join(checkbox_html_parts)

        # Build the dialog with minimal NiceGUI elements
        with ui.dialog() as dialog, ui.card().classes('w-[600px] max-h-[80vh]'):
            with ui.column().classes('w-full gap-2'):
                ui.label(tr('Filter by Domain')).classes('text-lg font-bold')
                ui.label(
                    f"{tr('Showing')} {total_results} {tr('of')} {total_results} {tr('results')}"
                ).classes('text-sm text-gray-500')

                # Single HTML container with all checkboxes (JS helpers loaded at page level)
                with ui.scroll_area().classes('w-full').style('max-height: 50vh;'):
                    ui.html(f'<div id="{container_id}">{checkbox_html}</div>', sanitize=False)

                with ui.row().classes('w-full justify-between'):
                    _cid = container_id  # capture for closures

                    with ui.row().classes('gap-2'):
                        ui.button(
                            tr('Select All'),
                            on_click=lambda: ui.run_javascript(
                                f'domainFilterSelectAll("{_cid}", true)')
                        ).props('flat dense no-caps')
                        ui.button(
                            tr('Select None'),
                            on_click=lambda: ui.run_javascript(
                                f'domainFilterSelectAll("{_cid}", false)')
                        ).props('flat dense no-caps')

                    with ui.row().classes('gap-2'):
                        async def apply_filter():
                            excluded_list = await ui.run_javascript(
                                f'domainFilterGetExcluded("{_cid}")', timeout=5.0
                            )
                            excluded = set(excluded_list) if excluded_list else set()
                            p_state.domain_exclusions = excluded
                            safe_user_set('parallels_domain_exclusions', list(excluded))
                            _update_parallels_domain_filter_btn()
                            main_filtered = _filter_parallels_by_domain(p_state.results)
                            filt_filtered = _filter_parallels_by_domain(p_state.filtered_results) if p_state.filtered_results else p_state.filtered_results
                            render_results(main_filtered, filt_filtered)
                            dialog.close()

                        ui.button(tr('Apply'), on_click=apply_filter).props('dense no-caps color=primary')
                        ui.button(tr('Cancel'), on_click=dialog.close).props('flat dense no-caps')

        dialog.open()

    def show_empty_state():
        with ui.column().classes('w-full items-center py-12'):
            ui.icon('search_off').classes('text-5xl').style('color: var(--text-muted);')
            # Changed to H3
            h3(tr('No parallels found'), classes='text-lg mt-4', style='color: var(--text-secondary);')
            ui.label(tr('Try adjusting your search parameters')).classes('text-sm').style('color: var(--text-muted);')

    def _rerender_with_exclusions():
        """Re-render results applying per-manuscript exclusions and domain filters."""
        main_results = p_state.results
        filtered_results = p_state.filtered_results
        if p_state.domain_exclusions and p_state.has_domain_data:
            main_results = _filter_parallels_by_domain(main_results)
            filtered_results = _filter_parallels_by_domain(filtered_results) if filtered_results else filtered_results
        render_results(main_results, filtered_results)

    def render_results(results, filtered_results=None, is_partial=False):
        try:
            _ = results_container.client
        except (RuntimeError, Exception):
            return  # Client deleted

        try:
            results_container.clear()
        except (RuntimeError, Exception):
            return

        if not results and not filtered_results:
            export_word_btn.props('disable')
            export_excel_btn.props('disable')
            export_json_btn.props('disable')
            with results_container:
                show_empty_state()
            return

        # Enable export buttons now that we have results
        export_word_btn.props(remove='disable')
        export_excel_btn.props(remove='disable')
        export_json_btn.props(remove='disable')

        # Show partial results warning banner at top
        if is_partial:
            with results_container:
                with ui.element('div').classes('w-full px-4 py-3 rounded-lg mb-4').style(
                    'background: #fff3cd; border: 1px solid #ffc107; color: #856404;'
                ):
                    with ui.row().classes('items-center gap-2'):
                        ui.icon('warning').classes('text-xl')
                        chunks_info = f"{p_state.chunks_processed} / {p_state.chunks_total}" if p_state.chunks_total > 0 else ""
                        ui.label(f"{tr('Partial results')} — {chunks_info} {tr('chunks searched')}").classes('font-medium')

        # Sort if needed
        sort_by = sort_select.value
        if sort_by == 'score':
            sorted_results = sorted(results, key=lambda x: x.get('score', 0), reverse=True)
        elif sort_by == 'shelfmark':
            sorted_results = sorted(results, key=lambda x: extract_shelfmark(x))
        else:
            sorted_results = results

        # Group results by manuscript
        grouped = {}
        for item in sorted_results:
            raw_header = item.get('raw_header', '')
            sys_id = None
            shelfmark = 'Unknown'

            if raw_header and state.meta_mgr:
                try:
                    sys_match = re.search(r'(99\d{8,})', raw_header)
                    if sys_match:
                        sys_id = sys_match.group(1)
                        shelf_temp, _ = state.meta_mgr.get_meta_for_id(sys_id)
                        shelfmark = shelf_temp or shelfmark
                except Exception:
                    pass  # Shelfmark lookup failed; use fallback identifier

            # Use sys_id as key, fallback to shelfmark
            key = sys_id if sys_id else shelfmark

            if key not in grouped:
                grouped[key] = {
                    'sys_id': sys_id,
                    'shelfmark': shelfmark,
                    'items': [],
                    'max_score': 0,
                    'avg_score': 0
                }

            grouped[key]['items'].append(item)
            grouped[key]['max_score'] = max(grouped[key]['max_score'], item.get('score', 0))

        # Calculate average scores
        for key in grouped:
            scores = [item.get('score', 0) for item in grouped[key]['items']]
            grouped[key]['avg_score'] = sum(scores) / len(scores) if scores else 0

        # Group order follows the SORT CONTROL (it used to be hard-coded to
        # max_score, which is why 'shelfmark' and 'matches' never appeared to
        # do anything).
        sorted_groups = _sort_groups(grouped.items(), sort_by)

        # Group filtered results similarly
        filtered_grouped = {}
        if filtered_results:
            for item in filtered_results:
                raw_header = item.get('raw_header', '')
                sys_id = None
                shelfmark = 'Unknown'

                if raw_header and state.meta_mgr:
                    try:
                        sys_match = re.search(r'(99\d{8,})', raw_header)
                        if sys_match:
                            sys_id = sys_match.group(1)
                            shelf_temp, _ = state.meta_mgr.get_meta_for_id(sys_id)
                            shelfmark = shelf_temp or shelfmark
                    except Exception:
                        pass  # Shelfmark lookup failed; use fallback identifier

                key = sys_id if sys_id else shelfmark
                if key not in filtered_grouped:
                    filtered_grouped[key] = {
                        'sys_id': sys_id,
                        'shelfmark': shelfmark,
                        'items': [],
                        'max_score': 0,
                        'avg_score': 0
                    }
                filtered_grouped[key]['items'].append(item)
                filtered_grouped[key]['max_score'] = max(filtered_grouped[key]['max_score'], item.get('score', 0))

            for key in filtered_grouped:
                scores = [item.get('score', 0) for item in filtered_grouped[key]['items']]
                filtered_grouped[key]['avg_score'] = sum(scores) / len(scores) if scores else 0

        sorted_filtered_groups = _sort_groups(filtered_grouped.items(), sort_by)

        # Separate per-manuscript excluded groups from main results
        excluded_ms_groups = []
        visible_groups = []
        for group_key, group_data in sorted_groups:
            sid = group_data.get('sys_id')
            if sid and sid in p_state.excluded_manuscript_ids:
                excluded_ms_groups.append((group_key, group_data))
            else:
                visible_groups.append((group_key, group_data))
        sorted_groups = visible_groups

        # Lazy loading configuration
        BATCH_SIZE = 50
        main_displayed = [0]  # Use list to allow modification in nested function
        filtered_displayed = [0]

        # Update header with manuscript count
        total_results = len(results)
        total_manuscripts = len(sorted_groups)
        filtered_count = len(filtered_results) if filtered_results else 0
        partial_suffix = f" - {tr('partial results')}" if is_partial else ""

        if total_results == 0 and filtered_count > 0:
            # All results were filtered - explain this to user
            results_header.text = f"{tr('All results filtered')} ({filtered_count} {tr('in filtered sources')}){partial_suffix}"
        elif filtered_count > 0:
            results_header.text = f"{total_results} {tr('matches in')} {total_manuscripts} {tr('manuscripts')} ({filtered_count} {tr('filtered')}){partial_suffix}"
        else:
            results_header.text = f"{total_results} {tr('matches in')} {total_manuscripts} {tr('manuscripts')}{partial_suffix}"

        with results_container:
            # Container for main results
            main_results_container = ui.column().classes('w-full gap-4')
            main_load_more_container = ui.row().classes('w-full justify-center py-4')

        def load_more_main():
            """Load next batch of main results."""
            start = main_displayed[0]
            end = min(start + BATCH_SIZE, len(sorted_groups))
            with main_results_container:
                for group_key, group_data in sorted_groups[start:end]:
                    create_manuscript_group(group_data)
            main_displayed[0] = end

            # Update load more button
            main_load_more_container.clear()
            remaining = len(sorted_groups) - main_displayed[0]
            if remaining > 0:
                with main_load_more_container:
                    ui.button(
                        f"{tr('Load more')} ({remaining} {tr('remaining')})",
                        icon='expand_more',
                        on_click=load_more_main
                    ).props('flat color=primary')

        # Initial load of main results
        if sorted_groups:
            load_more_main()

        # Filtered/excluded results in collapsible section (collapsed by default)
        if sorted_filtered_groups:
            with results_container:
                with ui.expansion(
                    text=f"{tr('Excluded Results')} ({filtered_count})",
                    icon='filter_alt',
                    value=False  # collapsed by default
                ).classes('w-full').style(
                    'border: 1px solid var(--accent-amber); border-radius: 8px; margin-top: 16px;'
                ).props('dense header-class="text-amber-8 text-subtitle1 text-weight-medium"') as filtered_expansion:
                    filtered_section = ui.column().classes('w-full gap-4')
                    filtered_load_more_container = ui.row().classes('w-full justify-center py-4')

            def load_more_filtered_inner():
                """Load next batch of filtered results."""
                start = filtered_displayed[0]
                end = min(start + BATCH_SIZE, len(sorted_filtered_groups))
                with filtered_section:
                    for group_key, group_data in sorted_filtered_groups[start:end]:
                        create_manuscript_group(group_data, is_filtered=True)
                filtered_displayed[0] = end

                # Update load more button
                filtered_load_more_container.clear()
                remaining = len(sorted_filtered_groups) - filtered_displayed[0]
                if remaining > 0:
                    with filtered_load_more_container:
                        ui.button(
                            f"{tr('Load more')} ({remaining} {tr('remaining')})",
                            icon='expand_more',
                            on_click=load_more_filtered_inner
                        ).props('flat color=amber')

            load_more_filtered_inner()

        # Per-manuscript excluded results in collapsible section (separate from filtered)
        if excluded_ms_groups:
            with results_container:
                with ui.expansion(
                    text=f"{tr('Excluded Manuscripts')} ({len(excluded_ms_groups)})",
                    icon='remove_circle_outline',
                    value=False  # collapsed by default
                ).classes('w-full').style(
                    'border: 1px solid var(--border-light); border-radius: 8px; margin-top: 16px;'
                ).props('dense header-class="text-grey-7 text-subtitle1 text-weight-medium"'):
                    for group_key, group_data in excluded_ms_groups:
                        sid = group_data.get('sys_id')
                        with ui.row().classes('w-full items-center justify-between py-2 px-4').style(
                            'border-bottom: 1px solid var(--border-light);'
                        ):
                            label_parts = [group_data['shelfmark']]
                            if sid == p_state.auto_excluded_source_id:
                                label_parts.append(f"({tr('Source manuscript')})")
                            ui.label(' '.join(label_parts)).classes('text-sm').style(
                                'color: var(--text-secondary);'
                            )
                            # Restore button
                            def _restore_manuscript(restore_sid=sid):
                                p_state.excluded_manuscript_ids.discard(restore_sid)
                                if restore_sid == p_state.auto_excluded_source_id:
                                    p_state.auto_excluded_source_id = None
                                persist_value('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
                                _update_p_chip_bar()
                                _rerender_with_exclusions()

                            ui.button(
                                icon='undo', on_click=_restore_manuscript
                            ).props('flat round dense size=sm color=primary').tooltip(tr('Restore'))

    def create_manuscript_group(group_data, is_filtered=False):
        """Create an expandable manuscript group with its parallels."""
        shelfmark = group_data['shelfmark']
        sys_id = group_data['sys_id']
        items = group_data['items']
        max_score = group_data['max_score']
        avg_score = group_data['avg_score']

        # Get title and library
        title = ''
        library_name = ''
        if sys_id and state.meta_mgr:
            try:
                _, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                title = title_temp or ''
                # Get library name
                library_code = state.meta_mgr.get_library_for_id(sys_id)
                if library_code:
                    from genizah_core import get_library_display
                    library_name = get_library_display(library_code, short=False, lang=get_language())
            except Exception:
                pass  # Library enrichment failed; continue with available data

        # Build display shelfmark with library name
        display_shelfmark = shelfmark
        if library_name:
            display_shelfmark = f"{library_name}, {shelfmark}"

        border_style = 'border: 2px solid var(--accent-amber);' if is_filtered else 'border: 2px solid var(--border-light);'
        with ui.card().classes('w-full p-0 overflow-hidden').style(border_style):
            # Header (always visible)
            with ui.row().classes('w-full items-center justify-between p-4').style('background: var(--bg-card);'):
                with ui.column().classes('gap-1 flex-grow'):
                    with ui.row().classes('items-center gap-3'):
                        icon_color = 'color: var(--accent-amber);' if is_filtered else 'color: var(--primary-600);'
                        ui.icon('menu_book').classes('text-xl').style(icon_color)
                        # Clickable shelfmark — opens browse page
                        shelfmark_color = 'color: var(--accent-amber);' if is_filtered else 'color: var(--primary-700);'
                        if sys_id:
                            ui.link(display_shelfmark, f'/browse?sys_id={sys_id}', new_tab=True).classes(
                                'text-lg font-bold no-underline hover:underline'
                            ).style(f'{shelfmark_color} cursor: pointer;')
                        else:
                            h3(display_shelfmark, classes='text-lg font-bold', style=shelfmark_color)
                        badge_color = 'amber' if is_filtered else 'blue'
                        ui.badge(f"{len(items)} {tr('matches')}", color=badge_color).classes('text-xs')

                        # How many DISTINCT witnesses point at this
                        # manuscript -- a union across its rows, not a sum:
                        # two of its pages found by one witness are one
                        # witness. Shown only when there is more than one
                        # witness to distinguish.
                        _wtotal = _searched_witness_count()
                        if _wtotal > 1:
                            _wstats = _group_witness_stats(group_data)
                            if _wstats['witness_count']:
                                _labels = _witness_labels()
                                ui.badge(
                                    tr('{n} of {m} witnesses').format(
                                        n=_wstats['witness_count'], m=_wtotal),
                                    color='purple',
                                ).classes('text-xs').tooltip(', '.join(
                                    _labels.get(wid, wid)
                                    for wid in _wstats['witness_ids']))

                        # Printed material indicator
                        if sys_id and sys_id in p_state.printed_ids:
                            from shared.fjms_service import PRINTED_BADGE_COLORS, PRINTED_LABEL_EN, PRINTED_LABEL_HE
                            from web.translations import get_language as _get_lang
                            _bg, _fg = PRINTED_BADGE_COLORS
                            _plabel = PRINTED_LABEL_HE if _get_lang() == 'he' else PRINTED_LABEL_EN
                            ui.label(_plabel).classes('text-xs px-2 py-0.5 rounded shrink-0 font-medium').style(
                                f'background: {_bg}; color: {_fg};'
                            )

                        # Exclusion reason chip for filtered results
                        if is_filtered:
                            # Determine dominant filter reason from items
                            reasons = set()
                            for it in items:
                                fr = it.get('filter_reason', '') or ''
                                if fr == 'source_text':
                                    reasons.add(tr('Found in source text'))
                                elif fr == 'high_frequency':
                                    reasons.add(tr('High frequency'))
                                elif it.get('is_text_filtered'):
                                    reasons.add(tr('Found in source text'))
                                elif it.get('is_filtered'):
                                    reasons.add(tr('Filtered'))
                            reason_text = ', '.join(reasons) if reasons else tr('Filtered')
                            ui.label(reason_text).classes('text-xs px-2 py-0.5 rounded').style(
                                'background: #fff3cd; color: #856404; white-space: nowrap;'
                            )

                    # Resolve translated title — always language-aware (not gated behind toggle)
                    _p_title = title
                    if sys_id and p_state.title_translations:
                        _p_tt = p_state.title_translations.get(sys_id)
                        if _p_tt:
                            _lang = get_language()
                            if _lang == 'he':
                                _p_title = _p_tt.get('hebrew_title') or _p_tt.get('english_title') or title
                            else:
                                _p_title = _p_tt.get('english_title') or _p_tt.get('hebrew_title') or title
                    if _p_title:
                        _p_title_short = (_p_title[:100] + '...') if len(_p_title) > 100 else _p_title
                        _p_dir = 'ltr' if (p_state.title_translations.get(sys_id, {}).get('english_title') and get_language() != 'he') else 'rtl'
                        ui.label(_p_title_short).classes('text-xs').style(f'color: var(--text-secondary); direction: {_p_dir};')

                with ui.row().classes('items-center gap-3'):
                    # Score badges
                    max_color = 'green' if max_score > 70 else 'amber' if max_score > 40 else 'gray'
                    ui.badge(f"{tr('Max')}: {int(max_score)}", color=max_color).classes('text-xs')
                    avg_color = 'green' if avg_score > 60 else 'amber' if avg_score > 35 else 'gray'
                    ui.badge(f"{tr('Avg')}: {int(avg_score)}", color=avg_color).classes('text-xs')

                    # Promote this manuscript to a witness. State lives in
                    # p_state, NOT on the widget: the checkbox is destroyed
                    # and rebuilt on every re-render, so a selection held on
                    # the widget would vanish the moment anything re-rendered.
                    if (sys_id and not is_filtered
                            and _letter_level_selected()
                            and passage_multi_witness_available()):
                        def _toggle_promotion(e, sid=sys_id):
                            if e.value:
                                p_state.checked_for_promotion.add(sid)
                            else:
                                p_state.checked_for_promotion.discard(sid)
                            _refresh_promotion_bar()

                        ui.checkbox(
                            value=sys_id in p_state.checked_for_promotion,
                            on_change=_toggle_promotion,
                        ).props('dense size=sm').tooltip(
                            tr('Search with this manuscript too'))

                    # Per-manuscript exclude button
                    if sys_id and not is_filtered:
                        def _exclude_manuscript(sid=sys_id):
                            p_state.excluded_manuscript_ids.add(sid)
                            persist_value('parallels_excluded_manuscript_ids', list(p_state.excluded_manuscript_ids))
                            _update_p_chip_bar()
                            # Re-render results with exclusion applied
                            _rerender_with_exclusions()

                        is_auto_excluded = sys_id == p_state.auto_excluded_source_id
                        excl_tooltip = tr('Source manuscript') if is_auto_excluded else tr('Exclude this manuscript')
                        ui.button(
                            icon='remove_circle_outline',
                            on_click=_exclude_manuscript
                        ).props('flat round dense size=sm color=grey').tooltip(excl_tooltip)

            # All matches (initially visible in compact form)
            with ui.column().classes('w-full').style('background: var(--bg-secondary);'):
                for idx, item in enumerate(items):
                    create_parallel_item(idx, item, sys_id, shelfmark)

    def create_parallel_item(idx, item, sys_id, shelfmark):
        """Create a single parallel match item within a manuscript group."""
        # Use round() instead of int() to avoid hiding small boosts
        score = round(item.get('score', 0))
        final_score = round(item.get('final_score', score))
        has_boundary_matches = item.get('has_boundary_matches', False)
        boundary_quality = item.get('boundary_quality', 0)
        boundary_match_count = item.get('boundary_match_count', 0)

        # Format text snippets
        ms_text = html.escape(item.get('text', '').replace('\n', ' '))
        ms_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', ms_text)

        # For source text, show paragraph breaks with red | marker
        src_raw = item.get('source_ctx', '')
        delim = boundary_delimiter.value or '\n'
        # Replace delimiter with placeholder (no special HTML chars), then other newlines with space
        BOUNDARY_MARKER = '~PARA_BREAK~'
        if delim in src_raw:
            src_raw = src_raw.replace(delim, BOUNDARY_MARKER)
        src_raw = src_raw.replace('\n', ' ')
        src_text = html.escape(src_raw)
        # Replace placeholder with red pipe HTML (marker survives html.escape since it has no special chars)
        src_text = src_text.replace(BOUNDARY_MARKER, ' <span style="color: #ef4444; font-weight: bold;">|</span> ')
        src_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', src_text)

        # Create short preview (first 80 chars)
        ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
        preview = (ms_text_clean[:80] + '...') if len(ms_text_clean) > 80 else ms_text_clean

        # Determine item styling based on boundary matches
        expansion_style = 'border-bottom: 1px solid var(--border-light);'
        if has_boundary_matches:
            expansion_style += ' background: rgba(255, 193, 7, 0.05);'  # Subtle amber highlight

        with ui.expansion().classes('w-full').style(expansion_style) as expansion:
            # Compact header (always visible)
            with expansion.add_slot('header'):
                with ui.row().classes('w-full items-center gap-3 py-2 px-4'):
                    ui.label(f"#{idx + 1}").classes('text-xs px-2 py-0.5 rounded').style(
                        'background: var(--bg-tertiary); color: var(--text-muted);'
                    )

                    # Score badge - show boost if applied
                    # Note: Raw scores are typically 100-10000+, not percentages
                    if final_score > score:
                        score_color = 'green' if final_score > 2000 else 'amber' if final_score > 500 else 'gray'
                        ui.badge(f"{score} → {final_score}", color=score_color).classes('text-xs')
                    else:
                        score_color = 'green' if score > 2000 else 'amber' if score > 500 else 'gray'
                        ui.badge(f"{score}", color=score_color).classes('text-xs')

                    # Boundary match indicator
                    if has_boundary_matches:
                        quality_pct = int(boundary_quality * 100)
                        ui.icon('link').classes('text-sm').style('color: var(--accent-amber);')
                        ui.badge(
                            f"{tr('Cross-paragraph')} ({quality_pct}%)",
                            color='amber'
                        ).classes('text-xs').tooltip(
                            f"{boundary_match_count} {tr('cross-paragraph matches')}"
                        )

                    # Preview snippet
                    ui.label(preview).classes('text-sm flex-grow').style(
                        'color: var(--text-secondary); direction: rtl; text-align: right;'
                    )

                    # NO expand indicator here. QExpansionItem draws its own
                    # chevron at the end of the header and rotates it on open;
                    # this one was a SECOND arrow that never rotated, because
                    # nothing ever changed its transform. Owner-reported
                    # 2026-08-25: "why do the results have two down arrow
                    # 'open' symbols?"

            # Expanded content (shown on click)
            with ui.column().classes('w-full p-4 gap-4').style('background: var(--bg-card);'):
                # Content comparison
                with ui.row().classes('w-full gap-3'):
                    # Source context
                    with ui.column().classes('flex-1 gap-2'):
                        # Which witness this highlight came from. A span
                        # offset is a position in ONE witness's text, so the
                        # context below belongs to that witness and to no
                        # other -- labelling it "Your text" when it came from
                        # a promoted manuscript would misattribute the
                        # evidence.
                        ui.label(_source_heading_for(item)).classes(
                            'text-xs font-bold uppercase').style(
                            'color: var(--success);')
                        with ui.element('div').classes('p-3 rounded-lg text-sm').style(
                            'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--success); color: var(--text-primary);'
                        ):
                            ui.html(src_text_html, sanitize=False)

                    # Manuscript match
                    with ui.column().classes('flex-1 gap-2'):
                        ui.label(tr('Manuscript Text')).classes('text-xs font-bold uppercase').style('color: var(--accent-amber);')
                        with ui.element('div').classes('p-3 rounded-lg text-sm').style(
                            'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--accent-amber); color: var(--text-primary);'
                        ):
                            ui.html(ms_text_html, sanitize=False)

                # Action buttons
                with ui.row().classes('w-full gap-2 mt-2'):
                    if sys_id:
                        # Browse button
                        ui.button(
                            tr('Browse'),
                            icon='menu_book',
                            on_click=lambda: ui.navigate.to(f'/browse?sys_id={sys_id}')
                        ).props('flat dense size=sm color=primary')

                        # Metadata button
                        def show_metadata_dialog(sid=sys_id, shelf=shelfmark):
                            show_parallel_metadata(sid, shelf, item)

                        ui.button(
                            tr('Metadata'),
                            icon='info',
                            on_click=show_metadata_dialog
                        ).props('flat dense size=sm')

                        # Add to list button
                        def show_add_dialog(sid=sys_id, shelf=shelfmark):
                            from web.components import show_add_to_list_dialog
                            show_add_to_list_dialog(
                                sys_id=sid,
                                shelfmark=shelf,
                                lists_mgr=state.lists_mgr,
                                note_default='',
                                fl_id=None
                            )

                        ui.button(
                            tr('Add to List'),
                            icon='star',
                            on_click=show_add_dialog
                        ).props('flat dense size=sm').style('color: var(--accent-amber);')

                        # Edit and Comment buttons
                        ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
                        if ms_text_clean:
                            from web.components import create_edit_button, create_comment_button
                            create_edit_button(
                                document_id=sys_id,
                                page_number=1,  # Page unknown in parallels
                                original_text=ms_text_clean,
                                shelfmark=shelfmark,
                                size='sm'
                            )
                            create_comment_button(
                                document_id=sys_id,
                                page_number=1,
                                shelfmark=shelfmark,
                                size='sm'
                            )

    def show_parallel_metadata(sys_id, shelfmark, item):
        """Show metadata dialog for a parallel result."""
        # Get full metadata
        title = ''
        library_name = ''
        if sys_id and state.meta_mgr:
            try:
                _, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                title = title_temp or ''
                # Get library name
                library_code = state.meta_mgr.get_library_for_id(sys_id)
                if library_code:
                    from genizah_core import get_library_display
                    library_name = get_library_display(library_code, short=False, lang=get_language())
            except Exception:
                pass  # Library enrichment failed; continue with available data

        with ui.dialog() as dialog, ui.card().classes('p-6 min-w-96 max-w-2xl'):
            # Changed to H3
            h3(tr('Metadata'), classes='text-xl font-bold mb-4')

            with ui.column().classes('w-full gap-3'):
                # Resolve translated title for metadata dialog — always language-aware
                _md_title = title
                if sys_id and p_state.title_translations:
                    _md_tt = p_state.title_translations.get(sys_id)
                    if _md_tt:
                        _lang = get_language()
                        if _lang == 'he':
                            _md_title = _md_tt.get('hebrew_title') or _md_tt.get('english_title') or title
                        else:
                            _md_title = _md_tt.get('english_title') or _md_tt.get('hebrew_title') or title

                # Show PGP description translation if available (only when UI is Hebrew)
                _md_desc = ''
                if p_state.translation_data and get_language() == 'he' and sys_id:
                    _md_trans = p_state.translation_data.get(sys_id)
                    if _md_trans:
                        _md_desc = _md_trans.get('description_he') or ''

                metadata_items = [
                    (tr('Library'), library_name or tr('Not available')),
                    (tr('Shelfmark'), shelfmark),
                    (tr('System ID'), sys_id or tr('Not available')),
                    (tr('Score'), str(int(item.get('score', 0)))),
                ]
                if _md_desc:
                    metadata_items.append((tr('Description'), _md_desc))

                # Title row with toggle to original
                with ui.row().classes('w-full items-start gap-4'):
                    ui.label(tr('Title') + ':').classes('font-bold w-32').style('color: var(--text-secondary);')
                    _md_display = _md_title or tr('Not available')
                    _md_orig = title or ''
                    _md_dir = 'ltr' if get_language() != 'he' else 'rtl'
                    if _md_orig and _md_orig != _md_display:
                        _md_st = {'showing_original': False}
                        with ui.row().classes('flex-grow items-center gap-0'):
                            _md_lbl = ui.label(_md_display).classes('flex-grow').style(f'color: var(--text-primary); direction: {_md_dir};')
                            def _make_md_toggle(lbl, orig, resolved, flag):
                                def handler():
                                    flag['showing_original'] = not flag['showing_original']
                                    lbl.text = orig if flag['showing_original'] else resolved
                                    lbl.style(f'color: var(--text-primary); direction: rtl;' if flag['showing_original'] else f'color: var(--text-primary); direction: {_md_dir};')
                                return handler
                            ui.button(icon='swap_horiz').props('flat dense round size=xs').style(
                                'min-width: 18px; min-height: 18px; padding: 0; opacity: 0.4;'
                            ).tooltip(tr('Show original title')).on('click.stop', _make_md_toggle(_md_lbl, _md_orig, _md_display, _md_st))
                    else:
                        ui.label(_md_display).classes('flex-grow').style(f'color: var(--text-primary); direction: {_md_dir};')

                for label, value in metadata_items:
                    with ui.row().classes('w-full items-start gap-4'):
                        ui.label(label + ':').classes('font-bold w-32').style('color: var(--text-secondary);')
                        ui.label(value).classes('flex-grow').style('color: var(--text-primary); direction: rtl;')

            with ui.row().classes('w-full justify-end gap-2 mt-6'):
                ui.button(tr('Close'), on_click=dialog.close).classes('btn-primary')

        dialog.open()

    def show_add_to_list_dialog_parallel(sys_id, shelfmark):
        """Show add to list dialog for a parallel result."""
        from web.components import show_add_to_list_dialog
        show_add_to_list_dialog(
            sys_id=sys_id,
            shelfmark=shelfmark,
            lists_mgr=state.lists_mgr,
            note_default='',
            fl_id=None
        )

    def extract_shelfmark(item):
        raw_header = item.get('raw_header', '')
        if raw_header and state.meta_mgr:
            try:
                sys_match = re.search(r'(99\d{8,})', raw_header)
                if sys_match:
                    sys_id = sys_match.group(1)
                    shelf, _ = state.meta_mgr.get_meta_for_id(sys_id)
                    return shelf or 'Unknown'
            except Exception:
                pass  # Shelfmark lookup failed; use fallback identifier
        return 'Unknown'

    def create_result_card(idx, item):
        score = int(item.get('score', 0))
        raw_header = item.get('raw_header', '')

        # Extract metadata
        sys_id = None
        shelfmark = 'Unknown'
        title = ''
        library_name = ''

        if raw_header and state.meta_mgr:
            try:
                sys_match = re.search(r'(99\d{8,})', raw_header)
                if sys_match:
                    sys_id = sys_match.group(1)
                    shelf_temp, title_temp = state.meta_mgr.get_meta_for_id(sys_id)
                    shelfmark = shelf_temp or shelfmark
                    title = title_temp or ''
                    # Get library name
                    library_code = state.meta_mgr.get_library_for_id(sys_id)
                    if library_code:
                        from genizah_core import get_library_display
                        library_name = get_library_display(library_code, short=False, lang=get_language())
            except Exception:
                pass  # Library enrichment failed; continue with available data

        # Build display shelfmark with library name
        display_shelfmark = shelfmark
        if library_name:
            display_shelfmark = f"{library_name}, {shelfmark}"

        # Format text snippets (escape HTML first to prevent XSS)
        ms_text = html.escape(item.get('text', '').replace('\n', ' '))
        ms_text_html = re.sub(r'\*(.*?)\*', r'<span class="highlight-match">\1</span>', ms_text)

        # For source text, show paragraph breaks with red | marker
        src_raw = item.get('source_ctx', '')
        delim = boundary_delimiter.value or '\n'
        BOUNDARY_MARKER = '~PARA_BREAK~'
        if delim in src_raw:
            src_raw = src_raw.replace(delim, BOUNDARY_MARKER)
        src_raw = src_raw.replace('\n', ' ')
        src_text = html.escape(src_raw)
        src_text = src_text.replace(BOUNDARY_MARKER, ' <span style="color: #ef4444; font-weight: bold;">|</span> ')
        src_text_html = re.sub(r'\*(.*?)\*', r'<span style="background: #bbf7d0; padding: 2px 4px; border-radius: 3px;">\1</span>', src_text)

        with ui.card().classes('w-full p-5 hover:shadow-lg transition-all'):
            # Header row
            with ui.row().classes('w-full items-start justify-between mb-4'):
                with ui.column().classes('gap-1'):
                    with ui.row().classes('items-center gap-3'):
                        ui.label(f"#{idx + 1}").classes('text-xs px-2 py-1 rounded').style(
                            'background: var(--bg-tertiary); color: var(--text-muted);'
                        )
                        ui.label(display_shelfmark).classes('text-lg font-bold').style('color: var(--primary-700);')
                    if title:
                        # Resolve title by language
                        _exp_title = title
                        if sys_id and p_state.title_translations:
                            _exp_tt = p_state.title_translations.get(sys_id)
                            if _exp_tt:
                                _exp_lang = get_language()
                                _exp_title = (_exp_tt.get('english_title') or _exp_tt.get('hebrew_title') or title) if _exp_lang != 'he' else (_exp_tt.get('hebrew_title') or _exp_tt.get('english_title') or title)
                        _exp_short = (_exp_title[:80] + '...') if len(_exp_title) > 80 else _exp_title
                        _exp_dir = 'ltr' if get_language() != 'he' else 'rtl'
                        ui.label(_exp_short).classes('text-sm').style(f'color: var(--text-secondary); direction: {_exp_dir};')

                # Score badge
                score_color = 'green' if score > 70 else 'amber' if score > 40 else 'gray'
                ui.badge(f"{tr('Score')}: {score}", color=score_color).classes('text-sm')

            # Content comparison
            with ui.row().classes('w-full gap-4'):
                # Source context
                with ui.column().classes('flex-1 gap-2'):
                    ui.label(_source_heading_for(item)).classes(
                        'text-xs font-bold uppercase').style(
                        'color: var(--success);')
                    with ui.element('div').classes('p-4 rounded-lg text-sm').style(
                        'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--success); color: var(--text-primary);'
                    ):
                        ui.html(src_text_html, sanitize=False)

                # Manuscript match
                with ui.column().classes('flex-1 gap-2'):
                    ui.label(tr('Manuscript Match')).classes('text-xs font-bold uppercase').style('color: var(--accent-amber);')
                    with ui.element('div').classes('p-4 rounded-lg text-sm').style(
                        'background: var(--bg-tertiary); direction: rtl; text-align: right; line-height: 1.8; border: 1px solid var(--accent-amber); color: var(--text-primary);'
                    ):
                        ui.html(ms_text_html, sanitize=False)

            # Actions
            with ui.row().classes('w-full gap-2 mt-4 pt-4').style('border-top: 1px solid var(--border-light);'):
                if sys_id:
                    ui.button(
                        tr('View manuscript'),
                        icon='menu_book',
                        on_click=lambda sid=sys_id: ui.navigate.to(f'/browse?sys_id={sid}')
                    ).props('flat dense').style('color: var(--primary-700);')

                # Check if item is in any list
                parallels_in_list = state.lists_mgr and sys_id and state.lists_mgr.is_item_in_any_list(sys_id)
                ui.button(
                    icon='star' if parallels_in_list else 'star_border',
                    on_click=lambda i=item, s=shelfmark, t=title, sid=sys_id: add_to_list(i, s, t, sid)
                ).props('flat round dense').style('color: var(--accent-amber);').tooltip(tr('In List') if parallels_in_list else tr('Add to List'))

                # Edit and Comment buttons
                ms_text_clean = item.get('text', '').replace('*', '').replace('\n', ' ').strip()
                if ms_text_clean and sys_id:
                    from web.components import create_edit_button, create_comment_button
                    create_edit_button(
                        document_id=sys_id,
                        page_number=1,  # Page unknown in parallels
                        original_text=ms_text_clean,
                        shelfmark=shelfmark,
                        size='sm'
                    )
                    create_comment_button(
                        document_id=sys_id,
                        page_number=1,
                        shelfmark=shelfmark,
                        size='sm'
                    )

    def add_to_list(item, shelfmark, title, sys_id):
        from web.components import show_add_to_list_dialog
        show_add_to_list_dialog(
            sys_id=sys_id,
            shelfmark=shelfmark,
            lists_mgr=state.lists_mgr,
            note_default='',
            fl_id=None
        )

    # Sort change handler
    sort_select.on('update:model-value', lambda: render_results(p_state.results) if p_state.results else None)

    # Initialize with restored results
    if p_state.results:
        results_header.text = f"{len(p_state.results)} {tr('parallels found')}"
        render_results(p_state.results)
        ui.notify(tr('Session restored'), type='info', timeout=3000, position='top')

    # Async function to restore filter sources from persistent storage
    async def restore_filter_sources():
        """Restore filter sources from cache files (async to avoid blocking)."""
        # Phase 87 migration (87-REVIEWS.md MEDIUM-2 from Codex round 4): deferred
        # callbacks may silently lose state on session prune (safe_storage helpers
        # absorb AssertionError). This is intentional — the alternative would crash
        # the asyncio event loop.
        stored_refs = safe_user_get('filter_sources_refs', [])
        stored_enabled = set(safe_user_get('filter_sources_enabled', []))
        # Codex P2 (PR #326): the enabled set is an identity input (it
        # produces filter_text). When the restored search carries its own
        # selection, it wins over the per-user latest-edit -- the loaded
        # TEXTS still come from user storage; only the checkmarks move.
        _cfg_sefaria = (_restored_search_config.get('sefaria_enabled')
                        if isinstance(_restored_search_config, dict) else None)
        if isinstance(_cfg_sefaria, list):
            stored_enabled = set(_cfg_sefaria)
        stored_custom = safe_user_get('filter_sources_custom', {})
        filter_sources['custom_count'] = safe_user_get('filter_sources_custom_count', 0)

        # Restore custom texts immediately (they're already in storage)
        for ref, text in stored_custom.items():
            filter_sources['loaded'][ref] = text
            if ref in stored_enabled:
                filter_sources['enabled'].add(ref)

        if not stored_refs:
            filter_sources['pending_restore'] = False
            try:
                refresh_loaded_sources_ui()  # Show current state (may include custom texts)
            except Exception:
                pass  # Client may have been deleted
            return

        # Show loading indicator
        try:
            sefaria_progress.style('display: block;')
            sefaria_status.style('display: block;')
            sefaria_progress.value = 0
            sefaria_status.text = tr('Loading: {}').format(f"0/{len(stored_refs)}")
        except Exception:
            return  # Client deleted, abort

        # Load Sefaria refs from cache (in background thread)
        loaded_count = len(stored_custom)  # Count custom texts already loaded
        for i, ref in enumerate(stored_refs):
            # Check if client is still valid before each iteration
            try:
                _ = sefaria_progress.client
            except (RuntimeError, Exception):
                return

            text = await run.io_bound(fetch_sefaria_text, ref, True)
            if text:
                filter_sources['loaded'][ref] = text
                if ref in stored_enabled:
                    filter_sources['enabled'].add(ref)
                loaded_count += 1

            # Update UI with error handling
            try:
                sefaria_progress.value = (i + 1) / len(stored_refs)
                sefaria_status.text = tr('Loading: {}').format(f"{i+1}/{len(stored_refs)}")
            except (RuntimeError, Exception):
                return

        # Update UI
        filter_sources['pending_restore'] = False
        try:
            sefaria_progress.style('display: none;')
            sefaria_status.style('display: none;')
            refresh_loaded_sources_ui()
        except (RuntimeError, Exception):
            pass  # Client deleted

    # Schedule async restore on page load
    async def _deferred_restore():
        await asyncio.sleep(0.1)
        try:
            await restore_filter_sources()
        except (RuntimeError, Exception):
            pass
    asyncio.ensure_future(_deferred_restore())

    # --- Deferred filter option loading (runs after UI renders) ---

    async def _deferred_p_filter_init():
        """Load filter select options asynchronously after page renders."""
        lang = get_language()  # Capture in client context before io_bound
        d = await run.io_bound(build_domain_options, lang)
        p_domain_select.options = d
        p_domain_select.props(remove='loading')
        p_domain_select.update()
        a = await run.io_bound(build_author_options, lang, p_state.filter_domains)
        p_author_select.options = a
        p_author_select.props(remove='loading')
        p_author_select.update()
        w = await run.io_bound(build_work_options, lang, p_state.filter_domains, p_state.filter_authors)
        p_work_select.options = w
        p_work_select.props(remove='loading')
        p_work_select.update()
        _update_p_chip_bar()

    async def _deferred_p_filter_init_wrapper():
        await asyncio.sleep(0.1)
        try:
            await _deferred_p_filter_init()
        except (RuntimeError, Exception):
            pass
    asyncio.ensure_future(_deferred_p_filter_init_wrapper())
