"""Which Search results the filters and exclusions keep, over the whole result list (GS-9).

The results table shows the list in batches, so a filter that read the table saw only the
loaded rows: export, the counts and "Load more" disagreed with the filters. This module
decides for every result, loaded or not. It holds no Qt: the window gathers its state into a
``ResultRules`` and supplies the cell texts the column filters read (``text_of``), the same
texts the table shows. Plan: the private repository's DESKTOP_SEARCH_RUN_CONTROLLER_PLAN.md §9.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field

# The columns a column filter can read (the window maps its table columns onto these).
SHELF = 'shelf'
LIBRARY = 'library'
TITLE = 'title'
SNIPPET = 'snippet'
DOMAIN = 'domain'

UNCATEGORIZED = 'Uncategorized'


@dataclass
class ResultRules:
    """Everything that decides whether a result shows, read from the window at one moment."""
    excluded: set = field(default_factory=set)          # sys_ids: exclusions hold until New
    column_filters: dict = field(default_factory=dict)  # SHELF/LIBRARY/TITLE/SNIPPET/DOMAIN -> {'text', 'exclude'}
    list_mode: str | None = None                        # 'in' / 'not_in', or None
    list_ids: set = field(default_factory=set)
    domain_exclusions: set = field(default_factory=set)  # empty: no domain filter
    domain_map: dict = field(default_factory=dict)       # sys_id -> [domain, ...]
    printed_state: str = 'all'                           # 'all' / 'hide_printed' / 'only_printed'
    printed_ids: set = field(default_factory=set)
    post_ids: set | None = None                          # the post-search criteria's ids, once known
    local_ids: set | None = None                         # the LOCAL filters' kept ids, when active
    all_terms_uids: set | None = None                    # "Only results with all terms", when applied

    def filters_nothing(self):
        """No rule but the exclusions applies."""
        return (not self.column_filters and self.list_mode is None and not self.domain_exclusions
                and self.printed_state == 'all' and self.post_ids is None and self.local_ids is None
                and self.all_terms_uids is None)


def rules_from_window(w, column_kinds, local_ids):
    """The Search window's filters and exclusions as one ResultRules. *column_kinds* maps its
    table columns to SHELF, ...; *local_ids* is what the LOCAL filters keep, or None
    (GenizahGUI._result_rules computes it)."""
    lfs = w.list_filter_state
    post_ids = getattr(w, '_post_criteria_ids', None)
    post_active = (bool(getattr(w, '_post_criteria', None)) and post_ids is not None
                   and getattr(w, '_measurement_fetch_complete', False))
    applied = getattr(w, '_all_terms_applied', None)
    return ResultRules(
        excluded=set(w.word_excluded_sys_ids) | set(w.excluded_sys_ids),
        column_filters=column_filters_of(w.results_filters, column_kinds),
        list_mode=lfs.get('mode', 'in') if lfs.get('active', False) else None,
        list_ids=lfs.get('cached_ids', set()),
        domain_exclusions=(set(w._domain_exclusions) if w._domain_exclusions and w._has_result_domains
                           else set()),
        domain_map=w._result_domain_map,
        printed_state=w._printed_filter_state,
        printed_ids=w._printed_sys_ids or set(),
        post_ids=post_ids if post_active else None,
        local_ids=local_ids,
        all_terms_uids=applied[1] if applied and applied[0] is w.last_results else None)


def text_matches(text, rule):
    """A column filter's verdict on one cell's text. ``None`` is a text not known yet (its
    metadata is still to be fetched): it passes, so the row is drawn, its fetch starts, and
    the next pass decides on the fetched text (Codex F3 on the plan)."""
    needle = (rule.get('text') or '').strip()
    if not needle or text is None:
        return True
    contains = needle.lower() in text.lower()
    return not contains if rule.get('exclude') else contains


def result_cells(res, meta_mgr, local_filepath):
    """(sid, shelfmark, title, library code, LOCAL library, is LOCAL, needs a metadata fetch)
    of a result, as its table row shows them; the column filters read the same values.
    *local_filepath* gives a LOCAL file's path from its sys_id."""
    meta = res['display']
    parsed = meta_mgr.parse_full_id_components(res.get('raw_header', ''))
    sid = parsed['sys_id'] or meta.get('id')
    if meta.get('source') != 'LOCAL':
        shelf, title = meta_mgr.get_meta_for_id(sid)
        library_code = meta_mgr.get_library_for_id(sid)
        return sid, shelf, title, library_code, '', False, (shelf == "Unknown" and not title)
    # Phase 95 smoke-fix (A): a LOCAL hit's shelfmark is its file name and its library
    # parent_folder/folder_name (get_meta_for_id strips a LOCAL sys_id's non-digits: always
    # "Unknown"). Phase 110 bug #2: the name comes from the canonical file path, since the
    # LOCAL LAB side-index stores the sys_id as the shelfmark.
    shelf = meta.get('shelfmark', '') or sid
    local_library = ''
    try:
        path = local_filepath(sid)
        if path:
            shelf = os.path.basename(path) or shelf
            folder_dir = os.path.dirname(path)
            folder, parent = os.path.basename(folder_dir), os.path.basename(os.path.dirname(folder_dir))
            local_library = f"{parent}/{folder}" if parent else folder
    except Exception:
        local_library = ''
    return sid, shelf, meta.get('title', ''), '', local_library, True, False


def column_text(cells, kind, fetched, unknown, resolve_title):
    """The text of a SHELF, LIBRARY or TITLE cell from ``result_cells``, or None while the
    manuscript's metadata is still to be fetched (``fetched(sid)`` false). *unknown* is the
    shelfmark shown when there is none; ``resolve_title(sid, title)`` the title shown."""
    sid, shelf, title, library_code, local_library, is_local, needs_fetch = cells
    if kind == LIBRARY:
        return local_library if is_local else (library_code or '')
    if needs_fetch and not fetched(sid):
        return None
    if kind == SHELF:
        return shelf or unknown
    return resolve_title(sid, title) if sid else (title or '')


def column_filters_of(filters, kinds):
    """The window's column filters ({table column: rule}, the column an int or, from a saved
    session, its text) keyed by what they read (*kinds*: table column -> SHELF, ...)."""
    out = {}
    for column, rule in (filters or {}).items():
        try:
            kind = kinds.get(int(column))
        except (TypeError, ValueError):
            kind = None
        if kind:
            out[kind] = rule
    return out


def result_key(res):
    """The key "Only results with all terms" compares (its uid, or the manuscript's id)."""
    return res.get('uid') or (res.get('display') or {}).get('id')


def passes(res, sid, rules, text_of):
    """Whether *res* (manuscript *sid*) shows under *rules*, the exclusions aside."""
    for column, rule in rules.column_filters.items():
        if not text_matches(text_of(res, column), rule):
            return False
    if rules.list_mode is not None:
        in_list = sid in rules.list_ids
        if (rules.list_mode == 'in') != in_list:
            return False
    if rules.domain_exclusions:
        domains = rules.domain_map.get(sid, []) if sid else []
        if not domains:
            if UNCATEGORIZED in rules.domain_exclusions:
                return False
        elif all(d in rules.domain_exclusions for d in domains):
            return False
    if rules.printed_state != 'all':
        printed = bool(sid) and sid in rules.printed_ids
        if printed if rules.printed_state == 'hide_printed' else not printed:
            return False
    if rules.post_ids is not None and sid not in rules.post_ids:
        return False
    if rules.local_ids is not None and sid not in rules.local_ids:
        return False
    if rules.all_terms_uids is not None and result_key(res) not in rules.all_terms_uids:
        return False
    return True


class ResultView:
    """The verdict on every result of one list under one set of rules.

    ``shown[i]`` says whether ``results[i]`` is drawn; ``excluded`` how many the exclusions
    hide (they are counted first, whatever the filters say). ``pending`` holds the shown
    results a column filter let through only because their text is not fetched yet: they
    are drawn (so their fetch starts) but not counted or exported until it lands (Codex on
    #150). ``count`` is the shown results less those."""

    def __init__(self, results, rules, sid_of, text_of):
        self.results = results
        self.rules = rules
        shown, excluded, pending = [], 0, set()
        reads = [c for c, rule in rules.column_filters.items() if (rule.get('text') or '').strip()]
        for i, res in enumerate(results):
            sid = sid_of(res)
            if sid and sid in rules.excluded:
                excluded += 1
                shown.append(False)
                continue
            kept = passes(res, sid, rules, text_of)
            shown.append(kept)
            if kept and any(text_of(res, c) is None for c in reads):
                pending.add(i)
        self.shown = shown
        self.excluded = excluded
        self.pending = pending
        self.count = sum(shown) - len(pending)

    def is_pending(self, position):
        return position in self.pending

    def shows_position(self, position):
        """Whether the result at *position* shows (False for a position not in the list)."""
        return isinstance(position, int) and 0 <= position < len(self.shown) and self.shown[position]

    def remaining_after(self, cursor):
        """How many shown results lie at or after position *cursor*."""
        return sum(self.shown[cursor:])

    def shown_after(self, cursor):
        """The kept results at or after position *cursor*, in list order, those still
        pending left out (what Export writes of the results not drawn yet)."""
        return [r for i, (r, s) in enumerate(zip(self.results[cursor:], self.shown[cursor:]), cursor)
                if s and i not in self.pending]


class PendingFetches:
    """The manuscripts of one list whose details a filter asked to fetch (``asked``), and
    those whose fetch ended (``done``: found or not, the filter then decides on what the row
    shows, so nothing waits for ever). Codex on #150."""

    def __init__(self):
        self.owner, self.asked, self.done, self.waiting = None, set(), set(), set()

    def for_list(self, results):
        """Start over for a new list. ``waiting``: asked while another load ran, started
        when it ends, never by cancelling it (Codex round 3 on #150)."""
        if results is not self.owner:
            self.owner, self.asked, self.done, self.waiting = results, set(), set(), set()
        return self

    def to_ask(self, sids):
        """The ids among *sids* not asked or fetched yet, now marked asked."""
        new = set(sids) - self.asked - self.done
        self.asked |= new
        return sorted(new)


class TextCache:
    """The column texts of one result list, computed once each. A manuscript's shelfmark and
    title change when its metadata arrives (``forget``); every title changes when the
    translations setting does (``forget_column``). Keyed by the result object itself, so it is
    cleared whenever the list is replaced (``owner``)."""

    def __init__(self):
        self.owner = None
        self._texts = {}      # (id(res), column) -> text or None
        self._keys_by_sid = {}

    def for_list(self, results):
        """Start over when *results* is not the list these texts belong to."""
        if results is not self.owner:
            self.owner = results
            self._texts.clear()
            self._keys_by_sid.clear()
        return self

    def get(self, res, column, sid, compute):
        key = (id(res), column)
        try:
            return self._texts[key]
        except KeyError:
            text = compute()
            self._texts[key] = text
            self._keys_by_sid.setdefault(sid, set()).add(key)
            return text

    def forget(self, *sids):
        for sid in sids:
            for key in self._keys_by_sid.pop(sid, ()):
                self._texts.pop(key, None)

    def forget_column(self, column):
        for key in [k for k in self._texts if k[1] == column]:
            del self._texts[key]
